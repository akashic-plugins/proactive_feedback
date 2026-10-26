from __future__ import annotations

import asyncio
import logging
import sqlite3
from importlib import import_module
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar, cast

from agent.plugin_composition import Context, RUNTIME_STARTED, RUNTIME_STOPPING
from plugins.models.contract import EMBEDDINGS, Embeddings
from plugins.ledger.contract import MESSAGE_CATALOG
from plugins.ui.contract import UI
from plugins.ledger.contract import Input, Message, Output
from core.common.file_io import run_file_io

from plugins.ledger.contract import MessageCatalog, MessageReader
from plugins.turn_projection.contract import TURN_PROJECTION, TurnProjection
from .db import (
    DiscoveryCursor, FeedbackEvent, FeedbackInputRecord, insert_feedback, insert_feedback_input,
    feedback_identity_exists, load_discovery_cursors, mark_feedback_input_processed, open_db,
    pending_feedback_inputs, save_discovery_cursor,
)
from .history import PROACTIVE_FEEDBACK_HISTORY, SqliteFeedbackHistory
from .scorer import (
    CandidateIndex, EmbedBatch, MessageRow, message_rows_from_messages,
    parse_quote_parts, score_followup,
)

logger = logging.getLogger("plugin.proactive_feedback")
_FEEDBACK_DB_NAME = "proactive_feedback.db"
_PREVIEW_MAX_CHARS = 2400
_RETRY_SECONDS = 1.0
# 候选只看最近 64×4 条非空助手消息与上一个用户输入之后的主动输出；回看读到两者都覆盖为止。
_LOOKBACK_ASSISTANTS = 256
_LOOKBACK_PAGE = 200
T = TypeVar("T")

api_version = 3
name = "proactive_feedback"
version = "4.1.0"
desc = "从 Message 日志记录主动消息被继续的反馈，并提供只读历史与面板。"
author = "Akashic"
inject = (MESSAGE_CATALOG, TURN_PROJECTION, UI, EMBEDDINGS)


async def apply(ctx: Context) -> None:
    """注册只读历史和生命周期；正式 Root 启动后才扫描消息与打开反馈库。"""
    await ctx.require(UI).register(
        ctx, dashboard=lambda: import_module(".dashboard", __package__),
    )
    db_path = ctx.data_root / _FEEDBACK_DB_NAME
    runtime = ProactiveFeedbackRuntime(
        catalog=ctx.require(MESSAGE_CATALOG), projection=ctx.require(TURN_PROJECTION),
        embed_batch=_bind_embeddings(ctx.require(EMBEDDINGS), ctx), db_path=db_path,
    )
    _ = await ctx.provide(PROACTIVE_FEEDBACK_HISTORY, SqliteFeedbackHistory(db_path))
    watcher: asyncio.Task[None] | None = None

    async def start(_event: object) -> None:
        nonlocal watcher
        watcher = await ctx.spawn(runtime.follow(), name="proactive-feedback")

    async def stop(_event: object) -> None:
        nonlocal watcher
        if watcher is not None:
            watcher.cancel()
            _ = await asyncio.gather(watcher, return_exceptions=True)
        watcher = None

    _ = await ctx.on(RUNTIME_STARTED, start)
    _ = await ctx.on(RUNTIME_STOPPING, stop)


@dataclass(frozen=True)
class _PendingTurn:
    """一个新闭合、待判定候选的 Turn：来源、聚合用户行、首条输入 seq 与 inbox 身份。"""
    source: str
    user: MessageRow
    first_seq: int
    user_ids: tuple[str, ...]
    ending_id: str


class ProactiveFeedbackRuntime:
    """按发现续读点增量发现已完成回复，并拥有评分 inbox 与不可变历史。"""
    def __init__(self, *, catalog: MessageCatalog, projection: TurnProjection,
                 embed_batch: EmbedBatch, db_path: Path) -> None:
        self._catalog = catalog
        self._projection = projection
        self._embed_batch = embed_batch
        self._db_path = db_path
        self._heads: dict[str, int] = {}
        self._cursors: dict[str, DiscoveryCursor] | None = None
        self._store_lock = asyncio.Lock()

    async def _store_io(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        """独占本代的一笔数据库工作，实际连接关闭后才释放准入锁。"""
        def work() -> T:
            with closing(open_db(self._db_path)) as sink:
                return operation(sink)

        # 原本由同步 loop 串行的数据库工作仍串行，初始化不会被评分读取抢过。
        async with self._store_lock:
            return await run_file_io(work)

    async def follow(self) -> None:
        """独立追赶 Message head 与重试耐久 inbox，避免评分失败重扫历史。"""
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._follow_heads(), name="feedback-discovery")
            tasks.create_task(self._follow_inputs(), name="feedback-scoring")

    async def _follow_heads(self) -> None:
        """发现成功后推进 head；评分结果不改变发现进度。"""
        async for heads in self._catalog.follow():
            while True:
                try:
                    await self._discover_changed(dict(heads))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("proactive_feedback Message 发现失败")
                    await asyncio.sleep(_RETRY_SECONDS)
                    continue
                self._heads = dict(heads)
                break

    async def _follow_inputs(self) -> None:
        """独立重试未完成评分；错误保持可见，不拖住新消息发现。"""
        while True:
            try:
                _ = await self._process_pending_inputs()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("proactive_feedback Message 消费失败")
            await asyncio.sleep(_RETRY_SECONDS)

    async def _discover_changed(self, heads: Mapping[str, int]) -> None:
        """只按 Core 目录选择列出的 Session；从保存的闭合边界续读，不整段解码历史。"""
        # 1. 保留空目录初始化；连接的完整寿命只在物理 worker 内。
        await self._store_io(lambda _sink: None)
        if self._cursors is None:
            self._cursors = await self._store_io(load_discovery_cursors)
        attributes = await run_file_io(self._catalog.snapshot_attributes)
        revision = await run_file_io(self._catalog.prefix_revision)
        for session_id, head in heads.items():
            if self._heads.get(session_id) == head:
                continue
            session_attributes = attributes.get(session_id)
            if session_attributes is None or session_attributes.visibility != "listed":
                continue
            # 2. 前缀被改写、Core 无改写标记或 head 回退时从头发现；inbox 写入幂等。
            saved = self._cursors.get(session_id)
            if saved is None or revision is None or saved.prefix_revision != revision or saved.head > head:
                saved = DiscoveryCursor(-1 if revision is None else revision, -1, {})
            elif saved.head == head:
                continue
            cursor = await self._discover_session(session_id, head, saved)
            # 3. 本轮 Turn 身份都已提交才保存续读点；中途取消从旧续读点重做。
            if revision is not None:
                await self._store_io(lambda sink: save_discovery_cursor(sink, session_id, cursor))
                self._cursors[session_id] = cursor

    async def _discover_session(self, session_id: str, head: int,
                                cursor: DiscoveryCursor) -> DiscoveryCursor:
        """把新闭合、含用户输入与最终输出的 Turn 身份写入插件 inbox，返回新的续读点。"""
        # 1. 只有出现新消息的来源可能闭合新 Turn；各来源只读自己尚未处理的尾段。
        reader = self._catalog.reader(session_id)
        fresh = await run_file_io(lambda: reader.snapshot(after_seq=cursor.head, through_seq=head))
        progress = dict(cursor.sources)
        pending: list[_PendingTurn] = []
        for source in dict.fromkeys(message.source for message in fresh):
            boundary, read_from = cursor.sources.get(source, (-1, cursor.head))
            own = tuple(message for message in fresh if message.source == source)
            if read_from < cursor.head:
                own = (*await run_file_io(lambda: reader.scan(
                    tuple, after_seq=read_from, through_seq=cursor.head, source=source,
                )), *own)
            progress[source] = await self._closed_turns(own, source, boundary, head, pending)
        # 2. 全量重扫按各来源首条 Input 的次序写 inbox，同一主动消息先到先得，次序必须保持。
        order = _input_order(cursor.input_order, fresh)
        rank = {source: index for index, source in enumerate(order)}
        pending.sort(key=lambda turn: rank.get(turn.source, len(rank)))
        if not pending:
            return DiscoveryCursor(cursor.prefix_revision, head, progress, order)
        # 3. 候选只看各 Turn 首条输入之前；只有引用回复需要最近 256 条助手消息的窗口。
        first = min(turn.first_seq for turn in pending)
        depth = _LOOKBACK_ASSISTANTS if any(_needs_recent(turn.user) for turn in pending) else 0
        rows = await run_file_io(lambda: [
            *_lookback_rows(reader, before_seq=first, through_seq=head, assistants=depth),
            *message_rows_from_messages(reader.snapshot(
                after_seq=first - 1, through_seq=max(turn.first_seq for turn in pending) - 1,
            )),
        ])
        candidates = CandidateIndex(rows)
        for turn in pending:
            # 4. 历史追赶必须让聊天、心跳和取消任务继续运行；每次只接纳一个 Turn。
            await asyncio.sleep(0)
            if self._candidates(candidates, turn.user, turn.first_seq):
                await self._store_io(lambda sink: insert_feedback_input(
                    sink, session_key=session_id, turn_id=turn.ending_id,
                    client_message_id=turn.user_ids[0], user_message_id=turn.user_ids[-1],
                    user_message_ids=turn.user_ids, assistant_message_id=turn.ending_id,
                ))
        return DiscoveryCursor(cursor.prefix_revision, head, progress, order)

    async def _closed_turns(
        self, own: tuple[Message, ...], source: str, boundary: int, head: int,
        pending: list[_PendingTurn],
    ) -> tuple[int, int]:
        """从闭合边界投影一个来源，收集待判定 Turn，返回它的新续读点。"""
        turns = await run_file_io(
            lambda: self._projection.project(own, source, after_seq=boundary)
        )
        by_id = {message.message_id: message for message in own}
        for turn in turns:
            if turn.status != "complete" or turn.ending_message_id is None:
                continue
            users = [by_id[message_id] for message_id in turn.message_ids
                     if isinstance(by_id[message_id].body, Input)]
            if not users or not isinstance(by_id[turn.ending_message_id].body, Output):
                continue
            pending.append(_PendingTurn(
                source, _aggregate_user_rows(message_rows_from_messages(users)), users[0].seq,
                tuple(message.message_id for message in users), turn.ending_message_id,
            ))
        closed = [turn.through_seq for turn in turns if turn.status != "open"]
        ending = closed[-1] if closed else boundary
        # 长期不闭合的尾段从其首个成员续读，不把读取起点拖回闭合边界。
        if turns and turns[-1].status == "open":
            return ending, by_id[turns[-1].message_ids[0]].seq - 1
        return ending, head

    @staticmethod
    def _candidates(index: CandidateIndex, user: MessageRow,
                    before_seq: int) -> list[MessageRow]:
        if _needs_recent(user):
            return index.recent(before_seq=before_seq, limit=64)
        return index.since_previous_user(before_seq=before_seq, limit=8)

    async def _process_pending_inputs(self) -> bool:
        """按 inbox 顺序处理；缺失的权威 Message 保留到后续日志变化。"""
        if not await run_file_io(self._db_path.exists):
            return False
        pending = await self._store_io(lambda sink: pending_feedback_inputs(sink, limit=100))
        results = [await self._process_record(record) for record in pending]
        return bool(results) and not all(results)

    async def _process_record(self, record: FeedbackInputRecord) -> bool:
        """恢复已接受反馈的 inbox 回执；只有新身份才读取历史并评分。"""
        # 1. accepted payload 已经是权威结果，重启不能重新评分或覆盖它。
        def recover_accepted(sink: sqlite3.Connection) -> bool:
            if feedback_identity_exists(
                sink, session_key=record.session_key, user_message_id=record.user_message_id,
            ):
                mark_feedback_input_processed(sink, row_id=record.row_id)
                return True
            return False

        if await self._store_io(recover_accepted):
            return True

        # 2. 按身份读取本 Turn 成员，候选只需有界回看；读取在工作线程完成，不拖住聊天与心跳。
        reader = self._catalog.reader(record.session_key)
        loaded = await run_file_io(lambda: _record_rows(reader, record))
        if loaded is None:
            logger.warning(
                "proactive_feedback inbox 引用的 Message 暂不可读 session=%s user=%s",
                record.session_key, record.user_message_id,
            )
            return False
        user_rows, assistant, history = loaded
        if any(row.role != "user" for row in user_rows) or assistant.role != "assistant":
            raise ValueError("proactive_feedback inbox Message 类型与原身份不一致")
        user = _aggregate_user_rows(user_rows)
        candidates = self._candidates(CandidateIndex(history), user, user_rows[0].seq)
        if not candidates:
            await self._complete(record.row_id)
            return True
        quote = parse_quote_parts(user.content)
        explicit = bool(user.reply_to_id or quote.quoted_text)
        try:
            scored = await score_followup(
                embed_batch=_no_embed if explicit else self._embed_batch,
                user=user, assistant=assistant, candidates=candidates,
                allow_pua=not explicit,
            )
        except Exception:
            logger.exception("proactive_feedback scoring failed")
            await self._persist_feedback(
                record, user, assistant, candidates[0], feedback_type="unscored",
                confidence="low", pa_score=None, pua_score=None, lag_seconds=None,
                candidate_count=len(candidates), matched_by="recent_pua",
                reason="scoring_failed",
            )
        else:
            if scored is not None:
                await self._persist_feedback(
                    record, user, assistant, scored.proactive,
                    feedback_type=scored.feedback_type, confidence=scored.confidence,
                    pa_score=scored.pa_score, pua_score=scored.pua_score,
                    lag_seconds=scored.lag_seconds,
                    candidate_count=scored.candidate_count,
                    matched_by=scored.matched_by, reason=scored.reason,
                )
        await self._complete(record.row_id)
        return True

    async def _persist_feedback(self, record: FeedbackInputRecord, user: MessageRow,
                                assistant: MessageRow, proactive: MessageRow,
                                **score: object) -> None:
        def save(sink: sqlite3.Connection) -> None:
            _ = insert_feedback(sink, FeedbackEvent(
                session_key=record.session_key, user_message_id=user.id,
                assistant_message_id=assistant.id, proactive_message_id=proactive.id,
                feedback_type=cast(str, score["feedback_type"]),
                confidence=cast(str, score["confidence"]),
                pa_score=cast(float | None, score["pa_score"]),
                pua_score=cast(float | None, score["pua_score"]),
                lag_seconds=cast(int | None, score["lag_seconds"]),
                candidate_count=cast(int, score["candidate_count"]),
                matched_by=cast(str, score["matched_by"]), reason=cast(str, score["reason"]),
                user_content_preview=_bounded_preview(user.content),
                assistant_content_preview=_bounded_preview(assistant.content),
                proactive_content_preview=_bounded_preview(proactive.content),
            ))

        await self._store_io(save)

    async def _complete(self, row_id: int) -> None:
        """确认单条处理回执，取消后也等待真实提交与连接关闭。"""
        await self._store_io(lambda sink: mark_feedback_input_processed(sink, row_id=row_id))


def _input_order(known: tuple[str, ...], fresh: tuple[Message, ...]) -> tuple[str, ...]:
    """已登记来源之后按首条 Input 追加本批新来源；新来源的首条 Input 必晚于已登记来源。"""
    first: dict[str, int] = {}
    for message in fresh:
        if isinstance(message.body, Input) and message.source not in known:
            _ = first.setdefault(message.source, message.seq)
    return known + tuple(sorted(first, key=first.__getitem__))


def _needs_recent(user: MessageRow) -> bool:
    """引用回复从最近的主动输出里找被引用者；普通追问只看上一个用户输入之后。"""
    return user.reply_to_id is not None or bool(parse_quote_parts(user.content).quoted_text)


def _lookback_rows(reader: MessageReader, *, before_seq: int,
                   through_seq: int | None, assistants: int) -> list[MessageRow]:
    """读 before_seq 之前的最短历史：含一条用户输入与 assistants 条非空助手消息，或已到会话开头。"""
    pages: list[list[MessageRow]] = []
    seen = 0
    user = False
    cursor = before_seq
    while cursor > 0:
        page = reader.read_tail(before_seq=cursor, through_seq=through_seq, limit=_LOOKBACK_PAGE)
        if not page.messages:
            break
        rows = message_rows_from_messages(page.messages)
        pages.append(rows)
        seen += sum(1 for row in rows if row.role == "assistant" and row.content)
        user = user or any(row.role == "user" for row in rows)
        if not page.has_more or (seen >= assistants and user):
            break
        cursor = page.messages[0].seq
    return [row for rows in reversed(pages) for row in rows]


def _record_rows(reader: MessageReader, record: FeedbackInputRecord,
                 ) -> tuple[list[MessageRow], MessageRow, list[MessageRow]] | None:
    """inbox 身份对应的用户行、助手行与候选回看；任一成员暂不可读返回 None。"""
    if record.assistant_message_id is None:
        return None
    found = [reader.get(message_id)
             for message_id in (*record.user_message_ids, record.assistant_message_id)]
    members = [message for message in found if message is not None]
    if len(members) != len(found):
        return None
    rows = message_rows_from_messages(members)
    try:
        recent = _needs_recent(_aggregate_user_rows(rows[:-1]))
    except ValueError:
        # 聚合失败由调用方按原身份报错；回看按最宽窗口读取。
        recent = True
    history = _lookback_rows(reader, before_seq=rows[0].seq, through_seq=None,
                             assistants=_LOOKBACK_ASSISTANTS if recent else 0)
    return rows[:-1], rows[-1], history


def _aggregate_user_rows(rows: list[MessageRow]) -> MessageRow:
    """保留同 Turn 多条输入的次序，并让最后输入拥有稳定反馈 identity。"""
    if not rows:
        raise ValueError("反馈 Turn 缺少 Input")
    last = rows[-1]
    reply_ids = tuple(dict.fromkeys(row.reply_to_id for row in rows if row.reply_to_id))
    if len(reply_ids) > 1:
        raise ValueError("同一 Turn 引用了多条不同消息")
    return MessageRow(
        id=last.id, seq=last.seq, role="user",
        content="\n\n".join(row.content for row in rows), source=last.source,
        proactive=False, reply_to_id=reply_ids[0] if reply_ids else None, ts=last.ts,
    )


def _bounded_preview(value: str, limit: int = _PREVIEW_MAX_CHARS) -> str:
    return value[:limit]


def _bind_embeddings(embeddings: Embeddings, ctx: Context) -> EmbedBatch:
    async def embed_batch(texts: list[str]) -> list[list[float]]:
        async with ctx.runtime_scope():
            async with embeddings.bind() as bound:
                result = await bound.embed(texts)
        return [list(vector) for vector in result.vectors]
    return embed_batch


async def _no_embed(texts: list[str]) -> list[list[float]]:
    _ = texts
    raise RuntimeError("显式引用反馈不得调用 embedding")
