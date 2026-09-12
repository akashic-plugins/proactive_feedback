from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol, cast

from agent.plugin_composition import (
    Context, EMBEDDINGS, Embeddings, MobileUiDefinition, MobileUiNavigation,
    MobileUiRpcInvalidRequest, RUNTIME_STARTED, RUNTIME_STOPPING, ServiceKey,
    UI_SLOTS,
)
from agent.plugin_composition.messages import MESSAGE_CATALOG, MessageCatalog
from agent.plugin_contracts import Input, Message, Output

from .dashboard import ProactiveFeedbackDashboardReader
from .db import (
    FeedbackEvent, FeedbackInputRecord, insert_feedback, insert_feedback_input,
    mark_feedback_input_processed, open_db, pending_feedback_inputs,
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


class ProjectedTurn(Protocol):
    """Consumer-owned view of the Turn facts needed by feedback scoring."""

    status: str
    ending_message_id: str | None
    message_ids: tuple[str, ...]


class TurnProjection(Protocol):
    """Read-only projection boundary supplied by the installed turn owner."""

    def project(
        self, messages: Sequence[Message], source: str
    ) -> tuple[ProjectedTurn, ...]: ...


TURN_PROJECTION = ServiceKey[TurnProjection]("turn.projection.v1")

api_version = 3
name = "proactive_feedback"
version = "4.0.0"
desc = "从 Message 日志记录主动消息被继续的反馈，并提供只读历史与面板。"
author = "Akashic"
inject = (MESSAGE_CATALOG, TURN_PROJECTION, UI_SLOTS, EMBEDDINGS)
skill_roots: tuple[str, ...] = ()
drift_skill_roots: tuple[str, ...] = ()
workspace_roots: tuple[str, ...] = ()
dashboard_module = "dashboard.py"
web_module = "web_module.js"
web_requires = ("workbench.panels.v2",)
web_provides = ()
web_contract_digests = {
    "workbench.panels.v2": "fb6417c9bf532c1fdb344767d06065d5d3293da85deb64eff1e8088889a33bcb",
}


async def apply(ctx: Context, config: object) -> None:
    """注册只读历史和生命周期；正式 Root 启动后才扫描消息与打开反馈库。"""
    _ = config
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
    await ctx.require(UI_SLOTS).register_mobile(
        ctx,
        MobileUiDefinition(
            module="mobile_panel.js", stylesheet="mobile_panel.css",
            navigation=MobileUiNavigation(
                label="主动反馈", description="主动消息是否被继续，以及对应的回应链路",
            ),
        ),
        query=runtime.query_mobile,
    )


class ProactiveFeedbackRuntime:
    """从完整 Message 前缀发现已完成回复，并拥有评分 inbox 与不可变历史。"""
    def __init__(self, *, catalog: MessageCatalog, projection: TurnProjection,
                 embed_batch: EmbedBatch, db_path: Path) -> None:
        self._catalog = catalog
        self._projection = projection
        self._embed_batch = embed_batch
        self._db_path = db_path
        self._heads: dict[str, int] = {}

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
        """只按 Core 目录选择列出的 Session；每个完整前缀可安全重复投影。"""
        attributes = self._catalog.snapshot_attributes()
        sink = open_db(self._db_path)
        try:
            for session_id, head in heads.items():
                if self._heads.get(session_id) == head:
                    continue
                session_attributes = attributes.get(session_id)
                if session_attributes is None or session_attributes.visibility != "listed":
                    continue
                messages = self._catalog.reader(session_id).snapshot(through_seq=head)
                await self._discover_session(sink, session_id, messages)
        finally:
            sink.close()

    async def _discover_session(self, sink: sqlite3.Connection, session_id: str,
                                messages: tuple[Message, ...]) -> None:
        """把含用户输入与最终输出的完整 Turn 身份写入插件 inbox。"""
        # 1. 每个前缀只建一次候选索引，避免每个 Turn 重新扫描历史。
        rows = message_rows_from_messages(messages)
        candidates = CandidateIndex(rows)
        by_id = {message.message_id: message for message in messages}
        row_by_id = {row.id: row for row in rows}
        sources = tuple(dict.fromkeys(
            message.source for message in messages if isinstance(message.body, Input)
        ))
        for source in sources:
            for turn in self._projection.project(messages, source):
                # 2. 历史追赶必须让聊天、心跳和取消任务继续运行。
                await asyncio.sleep(0)
                if turn.status != "complete" or turn.ending_message_id is None:
                    continue
                members = [by_id[message_id] for message_id in turn.message_ids]
                users = [message for message in members if isinstance(message.body, Input)]
                assistant = by_id[turn.ending_message_id]
                if not users or not isinstance(assistant.body, Output):
                    continue
                aggregate = _aggregate_user_rows(
                    [row_by_id[message.message_id] for message in users]
                )
                if not self._candidates(candidates, aggregate, users[0].seq):
                    continue
                insert_feedback_input(
                    sink, session_key=session_id,
                    turn_id=turn.ending_message_id,
                    client_message_id=users[0].message_id,
                    user_message_id=users[-1].message_id,
                    user_message_ids=tuple(message.message_id for message in users),
                    assistant_message_id=assistant.message_id,
                )

    @staticmethod
    def _candidates(index: CandidateIndex, user: MessageRow,
                    before_seq: int) -> list[MessageRow]:
        quote = parse_quote_parts(user.content)
        if user.reply_to_id is not None or quote.quoted_text:
            return index.recent(before_seq=before_seq, limit=64)
        return index.since_previous_user(before_seq=before_seq, limit=8)

    async def _process_pending_inputs(self) -> bool:
        """按 inbox 顺序处理；缺失的权威 Message 保留到后续日志变化。"""
        if not self._db_path.exists():
            return False
        sink = open_db(self._db_path)
        try:
            pending = pending_feedback_inputs(sink, limit=100)
        finally:
            sink.close()
        results = [await self._process_record(record) for record in pending]
        return bool(results) and not all(results)

    async def _process_record(self, record: FeedbackInputRecord) -> bool:
        messages = self._catalog.reader(record.session_key).snapshot()
        rows = message_rows_from_messages(messages)
        by_id = {row.id: row for row in rows}
        try:
            user_rows = [by_id[message_id] for message_id in record.user_message_ids]
            if record.assistant_message_id is None:
                raise KeyError("assistant_message_id")
            assistant = by_id[record.assistant_message_id]
        except KeyError:
            logger.warning(
                "proactive_feedback inbox 引用的 Message 暂不可读 session=%s user=%s",
                record.session_key, record.user_message_id,
            )
            return False
        if any(row.role != "user" for row in user_rows) or assistant.role != "assistant":
            raise ValueError("proactive_feedback inbox Message 类型与原身份不一致")
        user = _aggregate_user_rows(user_rows)
        candidates = self._candidates(CandidateIndex(rows), user, user_rows[0].seq)
        if not candidates:
            self._complete(record.row_id)
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
        self._complete(record.row_id)
        return True

    async def _persist_feedback(self, record: FeedbackInputRecord, user: MessageRow,
                                assistant: MessageRow, proactive: MessageRow,
                                **score: object) -> None:
        sink = open_db(self._db_path)
        try:
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
        finally:
            sink.close()

    def _complete(self, row_id: int) -> None:
        sink = open_db(self._db_path)
        try:
            mark_feedback_input_processed(sink, row_id=row_id)
        finally:
            sink.close()

    def query_mobile(self, method: str, payload: dict[str, object], *,
                     session_id: str | None, turn_id: str | None) -> dict[str, object]:
        """返回当前 generation 的只读移动投影。"""
        _ = session_id, turn_id
        if method not in {"feedback.overview", "feedback.events"}:
            raise MobileUiRpcInvalidRequest(f"未知 proactive_feedback 移动方法: {method}")
        reader = ProactiveFeedbackDashboardReader(self._db_path.parent)
        if method == "feedback.overview":
            if payload:
                raise MobileUiRpcInvalidRequest("feedback.overview 不接受参数")
            return reader.get_overview()
        if set(payload) - {"page", "page_size", "feedback_type"}:
            raise MobileUiRpcInvalidRequest("feedback.events 参数无效")
        page = _mobile_page_value(payload, "page", default=1, maximum=10_000)
        page_size = _mobile_page_value(payload, "page_size", default=30, maximum=50)
        feedback_type = _mobile_feedback_type(payload)
        items, total = reader.list_events(
            page=page, page_size=page_size, feedback_type=feedback_type,
        )
        return {"items": items, "total": total, "page": page, "page_size": page_size}


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


def _mobile_page_value(payload: dict[str, object], name: str, *,
                       default: int, maximum: int) -> int:
    value = payload.get(name, default)
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
        raise MobileUiRpcInvalidRequest(f"{name} 必须是 1 到 {maximum} 的整数")
    return value


def _mobile_feedback_type(payload: dict[str, object]) -> str:
    value = payload.get("feedback_type", "")
    if not isinstance(value, str):
        raise MobileUiRpcInvalidRequest("feedback_type 必须是字符串")
    allowed = {"", "topic_follow", "explicit_quote", "no_topic_follow", "unscored"}
    if value not in allowed:
        raise MobileUiRpcInvalidRequest("feedback_type 不受支持")
    return value
