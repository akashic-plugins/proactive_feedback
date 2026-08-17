from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
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
    insert_feedback,
    mark_feedback_published,
    open_db,
    pending_feedback_outbox,
)
from .scorer import (
    latest_turn_messages_from_rows,
    message_rows_from_snapshot,
    MessageRow,
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
    ) -> None:
        self._session_read = session_read
        self._workspace = workspace
        self._db_path = db_path
        self._publish_feedback = publish_feedback
        self._queue: asyncio.Queue[TurnCommitted] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._embedder: Embedder | None = None

    def enqueue(self, event: TurnCommitted) -> None:
        """Queue one committed turn without blocking the Core lifecycle seam."""

        if event.persisted_user_message is None:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(
                "proactive_feedback queue full, drop session=%s",
                event.session_key,
            )

    async def run_worker(self) -> None:
        """Process queued committed turns until the owning Fiber is disposed."""

        while True:
            # 1. Replay every durable payload before waiting for new Turns.
            try:
                await self._publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("proactive_feedback outbox publish failed")
                await asyncio.sleep(_OUTBOX_RETRY_SECONDS)
                continue

            # 2. Process one Core-committed Turn and drain its transaction's outbox.
            event = await self._queue.get()
            try:
                await self._process(event)
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

    async def _process(self, event: TurnCommitted) -> None:
        """Score one committed turn using a detached Session snapshot."""

        # 1. Resolve the committed message identity through Core's read service.
        user_text = event.persisted_user_message
        if not user_text or not event.assistant_response:
            return
        snapshot = self._session_read.read(event.session_key)
        if snapshot is None:
            return
        rows = message_rows_from_snapshot(snapshot.messages)
        turn = latest_turn_messages_from_rows(
            rows,
            user_message_id=event.persisted_user_message_id,
            assistant_message_id=event.assistant_message_id,
            user_content=user_text,
            assistant_content=event.assistant_response,
        )
        if turn is None:
            logger.warning(
                "proactive_feedback committed message missing session=%s",
                event.session_key,
            )
            return
        user, assistant = turn

        # 2. Preserve the v2 candidate window and quote matching semantics.
        quote = parse_quote_parts(user.content)
        allow_pua = not bool(quote.quoted_text)
        if quote.quoted_text:
            candidates = recent_proactive_messages_from_rows(
                rows,
                before_seq=user.seq,
                limit=64,
            )
        else:
            candidates = proactive_since_previous_user_from_rows(
                rows,
                before_seq=user.seq,
                limit=8,
            )
        if not candidates:
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
            await self._publish_pending()
            return
        if scored is None:
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


def _bounded_preview(value: str, limit: int = _PREVIEW_MAX_CHARS) -> str:
    return value[:limit]


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
