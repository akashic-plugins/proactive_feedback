from __future__ import annotations

import asyncio
import importlib.util
import inspect
import shutil
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agent.plugins.composable import ComposablePlugin
from plugins.content.plugin import check_text
from plugins.turn_projection.plugin import TurnProjection
from session.log import MessageCatalog, MessageLog, SessionAttributes
from session.message import ContentPart, ContentReferences, Input, Message, Output
from tests.test_standard_tools import environment


def _load_plugin():
    path = Path(__file__).parents[1] / "plugin.py"
    package_name = "proactive_feedback_message_test"
    package = type(sys)(package_name)
    package.__path__ = [str(path.parent)]
    sys.modules[package_name] = package
    spec = importlib.util.spec_from_file_location(
        package_name + ".plugin", path, submodule_search_locations=[str(path.parent)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


feedback = _load_plugin()


def _check_ref(part: ContentPart) -> ContentReferences:
    if not isinstance(part.value, str) or not part.value:
        raise ValueError("reply_ref 必须是消息 ID")
    return ContentReferences()


def _append_turn(log: MessageLog, session_id: str = "akashic:test") -> None:
    _ = log.ensure_session(session_id, SessionAttributes())
    proactive = log.writer(
        session_id, author="wake", source="wake", body_types=(Output,),
        content={"text": check_text},
    )
    proactive.append(
        "proactive-1", Output((ContentPart("text", "主动提醒某个很长很长的主题"),), "complete")
    )
    user = log.writer(
        session_id, author="user", source="conversation", body_types=(Input,),
        content={"text": check_text, "reply_ref": _check_ref},
    )
    user.append(
        "user-1",
        Input((ContentPart("text", "我继续这个主题"), ContentPart("reply_ref", "proactive-1"))),
    )
    assistant = log.writer(
        session_id, author="assistant", source="conversation", body_types=(Output,),
        content={"text": check_text},
    )
    assistant.append(
        "assistant-1", Output((ContentPart("text", "我接着回答这个主题"),), "complete")
    )


def _runtime(log: MessageLog, db_path: Path):
    async def embed(texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]

    return feedback.ProactiveFeedbackRuntime(
        catalog=MessageCatalog(log), projection=TurnProjection(),
        embed_batch=embed, db_path=db_path,
    )


def test_module_uses_message_runtime_contract() -> None:
    loaded = ComposablePlugin.from_module(feedback)
    assert loaded.name == "proactive_feedback"
    assert loaded.version == "4.0.0"
    assert inspect.signature(feedback.apply).parameters.keys() == {"ctx", "config"}
    source = (Path(__file__).parents[1] / "plugin.py").read_text(encoding="utf-8")
    for removed in (
        "AFTER_TURN_COMMITTED", "TurnCommitted", "SESSION_READ",
        "SessionReadService", "event_bus", "sessions.db",
    ):
        assert removed not in source


def test_message_rows_keep_new_source_and_explicit_reply() -> None:
    now = datetime.now(UTC)
    rows = feedback.message_rows_from_messages((
        Message("p", "s", 0, now, "wake", "wake",
                Output((ContentPart("text", "主动内容"),), "complete")),
        Message("u", "s", 1, now + timedelta(seconds=1), "user", "conversation",
                Input((ContentPart("text", "继续"), ContentPart("reply_ref", "p")))),
    ))
    assert rows[0].proactive is True
    assert rows[1].reply_to_id == "p"


def test_migrated_proactive_fact_is_read_without_rewriting_history() -> None:
    now = datetime.now(UTC)
    provenance = ContentPart("history.provenance", {
        "schema": "sessions.messages.v0", "role": "assistant",
        "content_was_null": False, "extra": '{"proactive":true}', "extra_sha256": "x",
    })
    row = feedback.message_rows_from_messages((
        Message("p", "s", 0, now, "legacy-attribution-unknown", "legacy-unattributed",
                Output((ContentPart("text", "旧主动内容"), provenance), "complete")),
    ))[0]
    assert row.proactive is True


@pytest.mark.asyncio
async def test_complete_message_turn_writes_one_immutable_feedback(tmp_path: Path) -> None:
    log = MessageLog(tmp_path / "sessions.db")
    try:
        _append_turn(log)
        runtime = _runtime(log, tmp_path / "plugin-data" / "proactive_feedback.db")
        heads = dict(runtime._catalog.snapshot_heads())
        await runtime._discover_changed(heads)
        assert await runtime._process_pending_inputs() is False
        await runtime._discover_changed(heads)
        assert await runtime._process_pending_inputs() is False

        page = feedback.SqliteFeedbackHistory(runtime._db_path).page(
            after_cursor=0, max_items=10,
        )
        assert len(page.records) == 1
        record = page.records[0]
        assert record.user_message_id == "user-1"
        assert record.assistant_message_id == "assistant-1"
        assert record.proactive_message_id == "proactive-1"
        assert record.feedback_type == "explicit_quote"
    finally:
        log.close()


@pytest.mark.asyncio
async def test_restart_replays_durable_identity_without_message_copies(tmp_path: Path) -> None:
    log = MessageLog(tmp_path / "sessions.db")
    try:
        _append_turn(log)
        db_path = tmp_path / "plugin-data" / "proactive_feedback.db"
        first = _runtime(log, db_path)
        await first._discover_changed(dict(first._catalog.snapshot_heads()))

        restarted = _runtime(log, db_path)
        assert await restarted._process_pending_inputs() is False
        assert len(feedback.SqliteFeedbackHistory(db_path).page(
            after_cursor=0, max_items=10,
        ).records) == 1
        with sqlite3.connect(db_path) as connection:
            row = connection.execute(
                "SELECT user_message_ids_json, processed_at FROM proactive_feedback_input_inbox"
            ).fetchone()
        assert row is not None and row[0] == '["user-1"]' and row[1] is not None
    finally:
        log.close()


@pytest.mark.asyncio
async def test_open_turn_is_not_recorded_until_final_output(tmp_path: Path) -> None:
    log = MessageLog(tmp_path / "sessions.db")
    try:
        _ = log.ensure_session("akashic:test", SessionAttributes())
        p = log.writer("akashic:test", author="wake", source="wake", body_types=(Output,),
                       content={"text": check_text})
        p.append("p", Output((ContentPart("text", "主动内容"),), "complete"))
        u = log.writer("akashic:test", author="user", source="conversation", body_types=(Input,),
                       content={"text": check_text})
        u.append("u", Input((ContentPart("text", "继续"),)))
        runtime = _runtime(log, tmp_path / "feedback.db")
        await runtime._discover_changed(dict(runtime._catalog.snapshot_heads()))
        with sqlite3.connect(runtime._db_path) as connection:
            assert connection.execute(
                "SELECT count(*) FROM proactive_feedback_input_inbox"
            ).fetchone() == (0,)
    finally:
        log.close()


def test_candidate_index_keeps_user_boundaries_and_recent_window() -> None:
    rows = [
        feedback.MessageRow(str(seq), seq, role, content, "conversation", proactive, None, "")
        for seq, role, content, proactive in (
            (0, "assistant", "old", True), (1, "user", "first", False),
            (2, "assistant", "one", True), (3, "assistant", "two", True),
            (4, "user", "next", False), (5, "assistant", "plain", False),
            (6, "assistant", "", True), (7, "assistant", "plain", False),
            (8, "assistant", "plain", False), (9, "assistant", "plain", False),
            (10, "assistant", "future", True),
        )
    ]
    index = feedback.CandidateIndex(list(reversed(rows)))
    assert [row.id for row in index.since_previous_user(before_seq=4, limit=8)] == ["3", "2"]
    assert index.since_previous_user(before_seq=10, limit=8) == []
    assert index.recent(before_seq=10, limit=1) == []
    assert [row.id for row in index.recent(before_seq=11, limit=1)] == ["10"]
    assert index.recent(before_seq=0, limit=1) == []


@pytest.mark.asyncio
async def test_scoring_failure_does_not_repeat_discovery_or_block_new_heads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = MessageLog(tmp_path / "sessions.db")
    _append_turn(log)
    runtime = _runtime(log, tmp_path / "feedback.db")
    discovered: list[str] = []
    complete = asyncio.Event()
    discover = runtime._discover_session
    attempts = 0

    async def record_discovery(sink, session_id, messages):
        await discover(sink, session_id, messages)
        discovered.append(session_id)
        if session_id == "akashic:second":
            complete.set()

    async def fail_scoring():
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            _append_turn(log, "akashic:second")
        raise RuntimeError("accepted feedback payload conflict")

    monkeypatch.setattr(runtime, "_discover_session", record_discovery)
    monkeypatch.setattr(runtime, "_process_pending_inputs", fail_scoring)
    monkeypatch.setattr(feedback, "_RETRY_SECONDS", 0)
    task = asyncio.create_task(runtime.follow())
    try:
        async with asyncio.timeout(3):
            await complete.wait()
        assert discovered == ["akashic:test", "akashic:second"]
        assert attempts >= 2
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        log.close()


@pytest.mark.asyncio
async def test_history_discovery_yields_between_turns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = MessageLog(tmp_path / "sessions.db")
    try:
        _append_turn(log)
        user = log.writer("akashic:test", author="user", source="conversation",
                          body_types=(Input,), content={"text": check_text, "reply_ref": _check_ref})
        user.append("user-2", Input((ContentPart("text", "继续"), ContentPart("reply_ref", "proactive-1"))))
        assistant = log.writer("akashic:test", author="assistant", source="conversation",
                               body_types=(Output,), content={"text": check_text})
        assistant.append("assistant-2", Output((ContentPart("text", "回答"),), "complete"))
        runtime = _runtime(log, tmp_path / "feedback.db")
        first_written = asyncio.Event()
        writes = 0
        insert = feedback.insert_feedback_input

        def record_insert(*args, **kwargs):
            nonlocal writes
            result = insert(*args, **kwargs)
            writes += 1
            first_written.set()
            return result

        monkeypatch.setattr(feedback, "insert_feedback_input", record_insert)
        task = asyncio.create_task(runtime._discover_changed(dict(runtime._catalog.snapshot_heads())))
        await first_written.wait()
        assert writes == 1
        await task
        assert writes == 2
    finally:
        log.close()


@pytest.mark.asyncio
async def test_real_plugin_manager_consumes_messages_and_restart_is_idempotent(
    tmp_path: Path,
) -> None:
    host, store, log, _artifacts, sources = environment(tmp_path, reply=True)
    embeddings = sources / "embeddings_probe"
    embeddings.mkdir()
    (embeddings / "plugin.py").write_text('''
from contextlib import asynccontextmanager
from types import SimpleNamespace
from agent.plugin_composition import EMBEDDINGS
api_version = 3
name = "embeddings_probe"
version = "1.0.0"
inject = ()
class Bound:
    async def embed(self, texts):
        return SimpleNamespace(vectors=tuple((1.0, 0.0) for _ in texts))
class Embeddings:
    @asynccontextmanager
    async def bind(self):
        yield Bound()
async def apply(ctx, config):
    await ctx.provide(EMBEDDINGS, Embeddings())
''', encoding="utf-8")
    shutil.copytree(
        Path(__file__).parents[1], sources / "proactive_feedback",
        ignore=shutil.ignore_patterns(
            ".git", ".pytest_cache", "__pycache__", "tests", ".akashic-core", ".plugin-contracts",
        ),
    )
    try:
        await host.load_all()
        await host.start_runtime()
        _append_turn(log)
        db_paths = list((tmp_path / "workspace" / "plugin-data").glob(
            "proactive_feedback-*/proactive_feedback.db"
        ))
        async with asyncio.timeout(3):
            while not db_paths or not feedback.SqliteFeedbackHistory(db_paths[0]).page(
                after_cursor=0, max_items=10,
            ).records:
                await asyncio.sleep(0.01)
                db_paths = list((tmp_path / "workspace" / "plugin-data").glob(
                    "proactive_feedback-*/proactive_feedback.db"
                ))
        assert len(feedback.SqliteFeedbackHistory(db_paths[0]).page(
            after_cursor=0, max_items=10,
        ).records) == 1

        await host.terminate_all()
        await host.load_all()
        await host.start_runtime()
        await asyncio.sleep(0.05)
        assert len(feedback.SqliteFeedbackHistory(db_paths[0]).page(
            after_cursor=0, max_items=10,
        ).records) == 1
    finally:
        await host.terminate_all()
        log.close()
        store.close()


def test_mobile_projection_rejects_unknown_method_without_writing(tmp_path: Path) -> None:
    log = MessageLog(tmp_path / "sessions.db")
    try:
        runtime = _runtime(log, tmp_path / "missing" / "proactive_feedback.db")
        with pytest.raises(feedback.MobileUiRpcInvalidRequest):
            runtime.query_mobile("feedback.delete", {}, session_id=None, turn_id=None)
        assert not runtime._db_path.exists()
    finally:
        log.close()
