from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any, cast

from agent.config_models import Config as CoreConfig
from agent.plugin_composition import (
    Context,
    MobileUiDefinition,
    MobileUiNavigation,
    MobileUiRpcInvalidRequest,
    SESSION_READ,
    SessionReadService,
    UI_SLOTS,
)
from agent.turn_events.after_turn import AFTER_TURN_COMMITTED
from agent.turn_events.proactive_feedback import (
    PROACTIVE_FEEDBACK_COMMITTED,
    ProactiveFeedbackCommitted,
)
from bus.events_lifecycle import TurnCommitted
from core.net.http import get_default_http_requester
from memory2.embedder import Embedder

from .dashboard import ProactiveFeedbackDashboardReader
from .db import (
    FeedbackEvent,
    FeedbackInputRecord,
    feedback_identity_exists,
    feedback_session_keys,
    insert_feedback,
    insert_feedback_input,
    mark_feedback_input_processed,
    mark_feedback_published,
    open_db,
    pending_feedback_input,
    pending_feedback_inputs,
    pending_feedback_outbox,
)
from .scorer import (
    MessageRow,
    message_rows_from_snapshot,
    parse_quote_parts,
    proactive_since_previous_user_from_rows,
    recent_proactive_messages_from_rows,
    score_followup,
)

logger = logging.getLogger("plugin.proactive_feedback")

_QUEUE_MAX = 100
_FEEDBACK_DB_NAME = "proactive_feedback.db"
_PREVIEW_MAX_CHARS = 2400
_OUTBOX_BATCH_SIZE = 100
_OUTBOX_RETRY_SECONDS = 1.0
_DISCOVERY_SESSION_LIMIT = 64
_DISCOVERY_TURN_LIMIT = 256

FeedbackPublisher = Callable[[ProactiveFeedbackCommitted], Awaitable[None]]

api_version = 3
name = "proactive_feedback"
version = "3.0.0"
desc = "记录主动消息被继续的反馈，并提供桌面与移动只读投影。"
author = "Akashic"
inject = (SESSION_READ, UI_SLOTS)
skill_roots: tuple[str, ...] = ()
drift_skill_roots: tuple[str, ...] = ()
workspace_roots: tuple[str, ...] = ()
dashboard_module = "dashboard.py"


async def apply(ctx: Context, config: object) -> None:
    """Register the committed-turn observer, worker, and exact mobile projection."""

    # 1. Resolve only Core-owned services and generation paths.
    _ = config
    session_read = ctx.require(SESSION_READ)
    ui_slots = ctx.require(UI_SLOTS)
    db_path = ctx.data_root / _FEEDBACK_DB_NAME
    runtime = ProactiveFeedbackRuntime(
        session_read=session_read,
        workspace=ctx.runtime.workspace,
        db_path=db_path,
        publish_feedback=lambda event: ctx.observe(
            PROACTIVE_FEEDBACK_COMMITTED,
            event,
        ),
    )

    # 2. Bind every executable contribution to this Fiber's lifecycle.
    await ctx.on(AFTER_TURN_COMMITTED, runtime.enqueue)
    await ui_slots.register_mobile(
        ctx,
        MobileUiDefinition(
            module="mobile_panel.js",
            stylesheet="mobile_panel.css",
            navigation=MobileUiNavigation(
                label="主动反馈",
                description="主动消息是否被继续，以及对应的回应链路",
            ),
        ),
        query=runtime.mobile_ui_query,
    )
    await ctx.spawn(runtime.run_worker(), name="proactive_feedback_worker")


class ProactiveFeedbackRuntime:
    """Own one generation's feedback queue and plugin-owned SQLite projection."""

    def __init__(
        self,
        *,
        session_read: SessionReadService,
        workspace: Path,
        db_path: Path,
        publish_feedback: FeedbackPublisher | None = None,
        session_keys: Callable[[], Iterable[str]] | None = None,
    ) -> None:
        self._session_read = session_read
        self._workspace = workspace
        self._db_path = db_path
        self._publish_feedback = publish_feedback
        self._session_keys = session_keys or (
            lambda: _formal_session_keys_from_read_service(session_read)
        )
        self._queue: asyncio.Queue[int] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._embedder: Embedder | None = None
        self._discovery_done = False

    def enqueue(self, event: TurnCommitted) -> None:
        """Durably record one committed Turn identity and wake the worker."""

        # 1. Candidate validation has no formal Session or plugin data owner.
        if getattr(self._session_read, "_lookup_existing", None) is None:
            raise RuntimeError("候选验证期禁止写入 proactive_feedback")
        if event.persisted_user_message is None or not _event_user_message_ids(event):
            return

        # 2. Persist the identity before waking the in-memory worker.
        input_row_id = self._persist_input(event)
        try:
            self._queue.put_nowait(input_row_id)
        except asyncio.QueueFull:
            logger.warning(
                "proactive_feedback queue full, durable input retained session=%s",
                event.session_key,
            )

    async def run_worker(self) -> None:
        """Process queued committed turns until the owning Fiber is disposed."""

        # 1. Recover a Core commit that ended before AFTER_TURN_COMMITTED.
        if not self._discovery_done:
            self._discover_committed_inputs()
            self._discovery_done = True
        while True:
            # 2. Replay every durable payload before waiting for new Turns.
            try:
                await self._publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("proactive_feedback outbox publish failed")
                await asyncio.sleep(_OUTBOX_RETRY_SECONDS)
                continue
            try:
                has_pending_inputs = await self._process_pending_inputs()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("proactive_feedback durable input replay failed")
                await asyncio.sleep(_OUTBOX_RETRY_SECONDS)
                continue
            if has_pending_inputs:
                await asyncio.sleep(_OUTBOX_RETRY_SECONDS)
                continue

            # 3. Process one Core-committed Turn and drain its transaction's outbox.
            input_row_id = await self._queue.get()
            try:
                await self._process_input_row(input_row_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("proactive_feedback process failed")
            finally:
                self._queue.task_done()

    def mobile_ui_query(
        self,
        method: str,
        payload: dict[str, object],
        *,
        session_id: str | None,
        turn_id: str | None,
    ) -> dict[str, object]:
        """Return the read-only mobile projection for this exact generation."""

        # 1. Validate the bounded RPC input before touching plugin data.
        _ = session_id, turn_id
        if method not in {"feedback.overview", "feedback.events"}:
            raise MobileUiRpcInvalidRequest(
                f"未知 proactive_feedback 移动方法: {method}"
            )
        reader = ProactiveFeedbackDashboardReader(self._db_path.parent)
        if method == "feedback.overview":
            if payload:
                raise MobileUiRpcInvalidRequest("feedback.overview 不接受参数")
            return reader.get_overview()

        # 2. Reuse the dashboard reader for stable pagination and filters.
        if set(payload) - {"page", "page_size", "feedback_type"}:
            raise MobileUiRpcInvalidRequest("feedback.events 参数无效")
        page = _mobile_page_value(payload, "page", default=1, maximum=10_000)
        page_size = _mobile_page_value(payload, "page_size", default=30, maximum=50)
        feedback_type = _mobile_feedback_type(payload)
        items, total = reader.list_events(
            page=page,
            page_size=page_size,
            feedback_type=feedback_type,
        )
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    async def _process(
        self,
        event: TurnCommitted,
        *,
        input_row_id: int | None = None,
    ) -> None:
        """Score one committed turn using a detached Session snapshot."""

        # 1. Resolve the committed message identity through Core's read service.
        if not event.persisted_user_message or not event.assistant_response:
            self._complete_input(input_row_id)
            return
        snapshot = self._session_read.read(event.session_key)
        if snapshot is None:
            return
        rows = message_rows_from_snapshot(snapshot.messages)
        turn = _turn_for_event(rows, event)
        if turn is None:
            logger.warning(
                "proactive_feedback committed message missing session=%s",
                event.session_key,
            )
            return
        user, assistant, candidate_before_seq = turn

        # 2. Preserve the v2 candidate window and quote matching semantics.
        quote = parse_quote_parts(user.content)
        allow_pua = not bool(quote.quoted_text)
        if quote.quoted_text:
            candidates = recent_proactive_messages_from_rows(
                rows,
                before_seq=candidate_before_seq,
                limit=64,
            )
        else:
            candidates = proactive_since_previous_user_from_rows(
                rows,
                before_seq=candidate_before_seq,
                limit=8,
            )
        if not candidates:
            self._complete_input(input_row_id)
            return

        # 3. Persist one deduplicated projection, including bounded display text.
        try:
            scored = await score_followup(
                embed_batch=self._get_embedder().embed_batch if allow_pua else _no_embed,
                user=user,
                assistant=assistant,
                candidates=candidates,
                allow_pua=allow_pua,
            )
        except Exception:
            logger.exception("proactive_feedback scoring failed")
            await self._persist_feedback(
                event=event,
                user=user,
                assistant=assistant,
                proactive=candidates[0],
                feedback_type="unscored",
                confidence="low",
                pa_score=None,
                pua_score=None,
                lag_seconds=None,
                candidate_count=len(candidates),
                matched_by="recent_pua",
                reason="scoring_failed",
            )
            self._complete_input(input_row_id)
            await self._publish_pending()
            return
        if scored is None:
            self._complete_input(input_row_id)
            return
        await self._persist_feedback(
            event=event,
            user=user,
            assistant=assistant,
            proactive=scored.proactive,
            feedback_type=scored.feedback_type,
            confidence=scored.confidence,
            pa_score=scored.pa_score,
            pua_score=scored.pua_score,
            lag_seconds=scored.lag_seconds,
            candidate_count=scored.candidate_count,
            matched_by=scored.matched_by,
            reason=scored.reason,
        )
        self._complete_input(input_row_id)
        await self._publish_pending()

    async def _persist_feedback(
        self,
        *,
        event: TurnCommitted,
        user: MessageRow,
        assistant: MessageRow,
        proactive: MessageRow,
        feedback_type: str,
        confidence: str,
        pa_score: float | None,
        pua_score: float | None,
        lag_seconds: int | None,
        candidate_count: int,
        matched_by: str,
        reason: str,
    ) -> None:
        sink = open_db(self._db_path)
        try:
            _ = insert_feedback(
                sink,
                FeedbackEvent(
                    session_key=event.session_key,
                    user_message_id=user.id,
                    assistant_message_id=assistant.id,
                    proactive_message_id=proactive.id,
                    feedback_type=feedback_type,
                    confidence=confidence,
                    pa_score=pa_score,
                    pua_score=pua_score,
                    lag_seconds=lag_seconds,
                    candidate_count=candidate_count,
                    matched_by=matched_by,
                    reason=reason,
                    user_content_preview=_bounded_preview(user.content),
                    assistant_content_preview=_bounded_preview(assistant.content),
                    proactive_content_preview=_bounded_preview(proactive.content),
                ),
            )
        finally:
            sink.close()

    def _persist_input(self, event: TurnCommitted) -> int:
        user_message_ids = _event_user_message_ids(event)
        sink = open_db(self._db_path)
        try:
            return insert_feedback_input(
                sink,
                session_key=event.session_key,
                turn_id=event.turn_id,
                client_message_id=event.client_message_id,
                user_message_id=(
                    event.persisted_user_message_id or user_message_ids[-1]
                ),
                assistant_message_id=event.assistant_message_id,
                user_message_ids=user_message_ids,
            )
        finally:
            sink.close()

    def _complete_input(self, input_row_id: int | None) -> None:
        if input_row_id is None:
            return
        sink = open_db(self._db_path)
        try:
            mark_feedback_input_processed(sink, row_id=input_row_id)
        finally:
            sink.close()

    async def _process_pending_inputs(self) -> bool:
        """Replay durable Turn identities and report unresolved rows."""

        # 1. Read only identities; canonical SessionRead reconstructs text in memory.
        if not self._db_path.exists():
            return False
        sink = open_db(self._db_path)
        try:
            pending = pending_feedback_inputs(sink, limit=_OUTBOX_BATCH_SIZE)
        finally:
            sink.close()
        for record in pending:
            await self._process_input_record(record)

        # 2. Keep retrying rows whose canonical messages are not readable yet.
        sink = open_db(self._db_path)
        try:
            return bool(pending_feedback_inputs(sink, limit=1))
        finally:
            sink.close()

    async def _process_input_row(self, input_row_id: int) -> None:
        if not self._db_path.exists():
            return
        sink = open_db(self._db_path)
        try:
            record = pending_feedback_input(sink, row_id=input_row_id)
        finally:
            sink.close()
        if record is not None:
            await self._process_input_record(record)

    async def _process_input_record(self, record: FeedbackInputRecord) -> None:
        snapshot = self._session_read.read(record.session_key)
        if snapshot is None:
            logger.warning(
                "proactive_feedback durable input session missing session=%s",
                record.session_key,
            )
            return
        rows = message_rows_from_snapshot(snapshot.messages)
        user = _aggregate_user_row(rows, record.user_message_ids)
        assistant = _assistant_for_input(rows, record)
        if user is None or assistant is None:
            logger.warning(
                "proactive_feedback durable input message missing session=%s user=%s",
                record.session_key,
                record.user_message_id,
            )
            return
        await self._process(
            TurnCommitted(
                session_key=record.session_key,
                channel="proactive_feedback_replay",
                chat_id="",
                input_message=user.content,
                persisted_user_message=user.content,
                assistant_response=assistant.content,
                tools_used=[],
                turn_id=record.turn_id,
                client_message_id=record.client_message_id,
                persisted_user_message_id=user.id,
                persisted_user_message_ids=record.user_message_ids,
                assistant_message_id=assistant.id,
            ),
            input_row_id=record.row_id,
        )

    async def _publish_pending(self) -> None:
        """Publish durable rows and advance their SQLite cursor after receipt."""

        # 1. A candidate with no database and tests without a publisher stay inert.
        if self._publish_feedback is None or not self._db_path.exists():
            return
        # 2. Publish outside SQLite; only a returned receipt advances state.
        while True:
            sink = open_db(self._db_path)
            try:
                pending = pending_feedback_outbox(sink, limit=_OUTBOX_BATCH_SIZE)
            finally:
                sink.close()
            if not pending:
                return
            for record in pending:
                payload = _decode_outbox_payload(record.payload_json)
                if payload.get("event_id") != record.event_id:
                    raise ValueError("proactive_feedback outbox event_id 不一致")
                feedback = ProactiveFeedbackCommitted(**cast(Any, payload))
                await self._publish_feedback(feedback)
                sink = open_db(self._db_path)
                try:
                    mark_feedback_published(
                        sink,
                        row_id=record.row_id,
                        event_id=record.event_id,
                    )
                finally:
                    sink.close()

    def _get_embedder(self) -> Embedder:
        if self._embedder is None:
            self._embedder = _build_embedder(self._workspace)
        return self._embedder

    def _discover_committed_inputs(self) -> None:
        """Discover bounded eligible Turns committed before the callback fanout."""

        # 1. Candidate generations never receive formal SessionRead data.
        if getattr(self._session_read, "_lookup_existing", None) is None:
            return

        # 2. Prefer Core's current key catalog; the durable catalog only fills gaps.
        ordered_keys = list(
            _bounded_session_keys(
                self._session_keys(),
                limit=_DISCOVERY_SESSION_LIMIT,
            )
        )
        if self._db_path.exists():
            sink = open_db(self._db_path)
            try:
                catalog_keys = feedback_session_keys(
                    sink,
                    limit=_DISCOVERY_SESSION_LIMIT,
                )
            finally:
                sink.close()
            seen = set(ordered_keys)
            for session_key in catalog_keys:
                if len(ordered_keys) >= _DISCOVERY_SESSION_LIMIT:
                    break
                if session_key in seen:
                    continue
                ordered_keys.append(session_key)
                seen.add(session_key)
        if not ordered_keys:
            return

        # 3. Read detached snapshots and persist identities only.
        sink = open_db(self._db_path)
        try:
            discovered = 0
            for session_key in ordered_keys:
                snapshot = self._session_read.read(session_key)
                if snapshot is None:
                    continue
                rows = message_rows_from_snapshot(snapshot.messages)
                for user_ids, assistant_id in _iter_discovered_turns(rows):
                    if discovered >= _DISCOVERY_TURN_LIMIT:
                        return
                    if feedback_identity_exists(
                        sink,
                        session_key=session_key,
                        user_message_id=user_ids[-1],
                    ):
                        continue
                    _ = insert_feedback_input(
                        sink,
                        session_key=session_key,
                        turn_id="",
                        client_message_id="",
                        user_message_id=user_ids[-1],
                        user_message_ids=user_ids,
                        assistant_message_id=assistant_id,
                    )
                    discovered += 1
        finally:
            sink.close()


def _bounded_preview(value: str, limit: int = _PREVIEW_MAX_CHARS) -> str:
    return value[:limit]


def _event_user_message_ids(event: TurnCommitted) -> tuple[str, ...]:
    if event.persisted_user_message_ids:
        return tuple(event.persisted_user_message_ids)
    if event.persisted_user_message_id is not None:
        return (event.persisted_user_message_id,)
    return ()


def _turn_for_event(
    rows: list[MessageRow],
    event: TurnCommitted,
) -> tuple[MessageRow, MessageRow, int] | None:
    user_ids = _event_user_message_ids(event)
    user = _aggregate_user_row(
        rows,
        user_ids,
        expected_content=event.persisted_user_message,
    )
    if user is None:
        return None
    assistant = _assistant_for_event(rows, event)
    if assistant is None:
        return None
    first_user = min(
        (row.seq for row in rows if row.role == "user" and row.id in user_ids),
        default=user.seq,
    )
    return user, assistant, first_user


def _aggregate_user_row(
    rows: list[MessageRow],
    user_message_ids: tuple[str, ...],
    *,
    expected_content: str | None = None,
) -> MessageRow | None:
    if not user_message_ids or len(set(user_message_ids)) != len(user_message_ids):
        return None
    by_id = {row.id: row for row in rows if row.role == "user"}
    user_rows = [by_id[message_id] for message_id in user_message_ids if message_id in by_id]
    if len(user_rows) != len(user_message_ids):
        return None
    content = "\n\n".join(row.content for row in user_rows)
    if expected_content is not None and content != expected_content:
        return None
    last = user_rows[-1]
    return MessageRow(
        id=last.id,
        seq=last.seq,
        role=last.role,
        content=content,
        extra=last.extra,
        ts=last.ts,
    )


def _assistant_for_event(
    rows: list[MessageRow],
    event: TurnCommitted,
) -> MessageRow | None:
    candidates = [row for row in rows if row.role == "assistant"]
    if event.assistant_message_id is not None:
        candidates = [
            row for row in candidates if row.id == event.assistant_message_id
        ]
    else:
        candidates = [
            row for row in candidates if row.content == event.assistant_response
        ]
    if event.assistant_response:
        candidates = [
            row for row in candidates if row.content == event.assistant_response
        ]
    return max(candidates, key=lambda row: row.seq, default=None)


def _iter_discovered_turns(
    rows: list[MessageRow],
) -> Iterable[tuple[tuple[str, ...], str]]:
    """Yield bounded eligible committed Turn identities from a detached snapshot."""

    ordered = sorted(rows, key=lambda row: row.seq)
    previous_assistant_seq = -1
    for assistant in (row for row in ordered if row.role == "assistant"):
        users = [
            row
            for row in ordered
            if row.role == "user"
            and previous_assistant_seq < row.seq < assistant.seq
        ]
        if users:
            user_ids = tuple(row.id for row in users)
            aggregate = "\n\n".join(row.content for row in users)
            quote = parse_quote_parts(aggregate)
            candidates = (
                recent_proactive_messages_from_rows(
                    ordered,
                    before_seq=users[0].seq,
                    limit=64,
                )
                if quote.quoted_text
                else proactive_since_previous_user_from_rows(
                    ordered,
                    before_seq=users[0].seq,
                    limit=8,
                )
            )
            if candidates:
                yield user_ids, assistant.id
        previous_assistant_seq = assistant.seq


def _formal_session_keys_from_read_service(
    session_read: SessionReadService,
) -> tuple[str, ...]:
    """Return only the Core-owned bounded key catalog; snapshots still use SESSION_READ."""

    public_catalog = getattr(session_read, "list_session_keys", None)
    if callable(public_catalog):
        catalog = cast(Callable[[], Iterable[object]], public_catalog)
        return _bounded_session_keys(catalog(), limit=_DISCOVERY_SESSION_LIMIT)
    lookup = getattr(session_read, "_lookup_existing", None)
    owner = getattr(lookup, "__self__", None)
    manager = getattr(owner, "_session_manager", None)
    list_sessions = getattr(manager, "list_sessions", None)
    if not callable(list_sessions):
        return ()
    catalog = cast(Callable[[], list[object]], list_sessions)
    keys: list[str] = []
    for item in catalog():
        if isinstance(item, dict) and isinstance(item.get("key"), str):
            keys.append(item["key"])
    return _bounded_session_keys(keys, limit=_DISCOVERY_SESSION_LIMIT)


def _bounded_session_keys(
    values: Iterable[object],
    *,
    limit: int,
) -> tuple[str, ...]:
    """Bound a key catalog while preserving both current-list edges."""

    unique = tuple(dict.fromkeys(str(value) for value in values if value))
    if len(unique) <= limit:
        return unique
    return (*unique[: limit - 1], unique[-1])


def _build_embedder(workspace: Path) -> Embedder:
    config_path = os.environ.get("AKASHIC_CONFIG", "").strip()
    if not config_path:
        raise RuntimeError("proactive_feedback 需要 Core 的 AKASHIC_CONFIG")
    embedding = CoreConfig.load(path=config_path, workspace=workspace).memory.embedding
    return Embedder(
        base_url=embedding.base_url,
        api_key=embedding.api_key,
        model=embedding.model,
        output_dimensionality=embedding.output_dimensionality,
        requester=get_default_http_requester("external_default"),
    )


def _decode_outbox_payload(payload_json: str) -> dict[str, object]:
    """Decode one durable payload without accepting a second fallback schema."""

    payload = json.loads(payload_json)
    if not isinstance(payload, dict):
        raise TypeError("proactive_feedback outbox payload 必须是 object")
    return payload


def _assistant_for_input(
    rows: list[MessageRow],
    record: FeedbackInputRecord,
) -> MessageRow | None:
    if record.assistant_message_id is not None:
        return next(
            (
                row
                for row in rows
                if row.role == "assistant" and row.id == record.assistant_message_id
            ),
            None,
        )
    user = _aggregate_user_row(rows, record.user_message_ids)
    if user is None:
        return None
    return min(
        (row for row in rows if row.role == "assistant" and row.seq > user.seq),
        key=lambda row: row.seq,
        default=None,
    )


async def _no_embed(texts: list[str]) -> list[list[float]]:
    _ = texts
    raise RuntimeError("quoted feedback must not call embedding")


def _mobile_page_value(
    payload: dict[str, object],
    name: str,
    *,
    default: int,
    maximum: int,
) -> int:
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
