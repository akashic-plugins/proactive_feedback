from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import inspect
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from agent.plugin_composition import SessionReadService, SessionReadSnapshot
from agent.plugins.composable import ComposablePlugin
from agent.plugins.dashboard_host import DashboardBinding, PluginDashboardHost
from agent.plugins.install import install_git_plugin
from agent.plugins.manager import PluginManager
from agent.plugins.mobile_ui import PluginMobileUiProvider
from agent.plugins.manifest import write_plugin_manifest
from agent.turn_events.after_turn import AFTER_TURN_COMMITTED
from bus.events_lifecycle import TurnCommitted
from bus.event_bus import EventBus


def _load_plugin_module():
    path = Path(__file__).parents[1] / "plugin.py"
    spec = importlib.util.spec_from_file_location(
        "proactive_feedback_v3_test.plugin",
        path,
        submodule_search_locations=[str(path.parent)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    package = type(sys)("proactive_feedback_v3_test")
    package.__path__ = [str(path.parent)]
    sys.modules[package.__name__] = package
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_plugin_module()
FeedbackEvent = module.FeedbackEvent


async def _embed_batch(texts: list[str]) -> list[list[float]]:
    return [[1.0, 0.0] for _ in texts]


def _commit_plugin(repo: Path) -> None:
    for args in (
        ("init",),
        ("config", "user.name", "test"),
        ("config", "user.email", "test@example.com"),
        ("add", "."),
        ("commit", "-m", "fixture"),
    ):
        result = subprocess.run(
            ("git", *args),
            cwd=repo,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
        assert result.returncode == 0, result.stderr


def _event(*, quoted: bool = True) -> TurnCommitted:
    user = (
        "被回复消息：主动提醒某个很长很长的主题\n\n【你当前新消息】我继续这个主题"
        if quoted
        else "我继续这个主题"
    )
    return TurnCommitted(
        session_key="mobile:test",
        channel="test",
        chat_id="chat",
        input_message=user,
        persisted_user_message=user,
        assistant_response="我接着回答这个主题",
        tools_used=[],
        persisted_user_message_id="u1",
        assistant_message_id="a1",
    )


def _snapshot(*, quoted: bool = True) -> SessionReadSnapshot:
    user_content = (
        "被回复消息：主动提醒某个很长很长的主题\n\n【你当前新消息】我继续这个主题"
        if quoted
        else "我继续这个主题"
    )
    return SessionReadSnapshot(
        session_key="mobile:test",
        messages=(
            {
                "id": "p1",
                "seq": 1,
                "role": "assistant",
                "content": "主动提醒某个很长很长的主题",
                "extra": '{"proactive": true}',
                "ts": "2026-08-17T00:00:00+00:00",
            },
            {
                "id": "u1",
                "seq": 2,
                "role": "user",
                "content": user_content,
                "extra": None,
                "ts": "2026-08-17T00:00:10+00:00",
            },
            {
                "id": "a1",
                "seq": 3,
                "role": "assistant",
                "content": "我接着回答这个主题",
                "extra": None,
                "ts": "2026-08-17T00:00:11+00:00",
            },
        ),
        compaction_generation=0,
        consolidated_through_seq=None,
    )


def _two_user_event() -> TurnCommitted:
    first = "被回复消息：主动提醒某个很长很长的主题"
    second = "【你当前新消息】我继续这个主题"
    return TurnCommitted(
        session_key="mobile:test",
        channel="test",
        chat_id="chat",
        input_message=f"{first}\n\n{second}",
        persisted_user_message=f"{first}\n\n{second}",
        assistant_response="我接着回答这个主题",
        tools_used=[],
        persisted_user_message_id="u2",
        persisted_user_message_ids=("u1", "u2"),
        assistant_message_id="a1",
    )


def _two_user_snapshot() -> SessionReadSnapshot:
    return SessionReadSnapshot(
        session_key="mobile:test",
        messages=(
            {
                "id": "p1",
                "seq": 1,
                "role": "assistant",
                "content": "主动提醒某个很长很长的主题",
                "extra": '{"proactive": true}',
                "ts": "2026-08-17T00:00:00+00:00",
            },
            {
                "id": "u1",
                "seq": 2,
                "role": "user",
                "content": "被回复消息：主动提醒某个很长很长的主题",
                "extra": None,
                "ts": "2026-08-17T00:00:10+00:00",
            },
            {
                "id": "u2",
                "seq": 3,
                "role": "user",
                "content": "【你当前新消息】我继续这个主题",
                "extra": None,
                "ts": "2026-08-17T00:00:11+00:00",
            },
            {
                "id": "a1",
                "seq": 4,
                "role": "assistant",
                "content": "我接着回答这个主题",
                "extra": None,
                "ts": "2026-08-17T00:00:12+00:00",
            },
        ),
        compaction_generation=0,
        consolidated_through_seq=None,
    )


def _multi_turn_session_state(session_key: str, *, count: int = 5) -> object:
    messages: list[dict[str, object]] = []
    seq = 1
    for index in range(count):
        messages.extend(
            [
                {
                    "id": f"{session_key}:p{index}",
                    "seq": seq,
                    "role": "assistant",
                    "content": f"主动提醒 {index}",
                    "extra": '{"proactive": true}',
                    "ts": "2026-08-17T00:00:00+00:00",
                },
                {
                    "id": f"{session_key}:u{index}",
                    "seq": seq + 1,
                    "role": "user",
                    "content": f"继续主题 {index}",
                    "extra": None,
                    "ts": "2026-08-17T00:00:01+00:00",
                },
                {
                    "id": f"{session_key}:a{index}",
                    "seq": seq + 2,
                    "role": "assistant",
                    "content": f"回答主题 {index}",
                    "extra": None,
                    "ts": "2026-08-17T00:00:02+00:00",
                },
            ]
        )
        seq += 3
    return SimpleNamespace(messages=messages, last_consolidated=0)


def test_module_exports_pure_v3_contract() -> None:
    assert module.api_version == 3
    assert module.name == "proactive_feedback"
    assert inspect.signature(module.apply).parameters.keys() == {"ctx", "config"}
    assert ComposablePlugin.from_module(module).dashboard_module == "dashboard.py"


def test_v2_runtime_symbols_are_not_used_by_module() -> None:
    module_file = module.__file__
    assert module_file is not None
    source = Path(module_file).read_text(encoding="utf-8")
    assert "class ProactiveFeedbackPlugin" not in source
    assert "from agent.plugins import" not in source
    assert "event_bus" not in source
    assert "sessions.db" not in source
    assert "ProactiveFeedbackRecorded" not in source
    assert "PROACTIVE_FEEDBACK_COMMITTED" not in source
    assert "CONTENT_SOURCE" not in source
    assert "Content" not in source
    assert "event.extra" not in source
    assert "【你当前新消息】" not in source


@pytest.mark.asyncio
async def test_embedding_service_is_bound_inside_the_generation_scope() -> None:
    events: list[str] = []

    class Scope:
        def __init__(self, value: object = None) -> None:
            self.value = value

        async def __aenter__(self) -> object:
            events.append("enter")
            return self.value

        async def __aexit__(self, *_args: object) -> None:
            events.append("exit")

    class Bound:
        async def embed(self, texts: list[str]) -> object:
            events.append("embed")
            assert texts == ["one", "two"]
            return SimpleNamespace(vectors=((1.0, 0.0), (0.0, 1.0)))

    class Embeddings:
        def bind(self) -> Scope:
            events.append("bind")
            return Scope(Bound())

    class Context:
        def runtime_scope(self) -> Scope:
            events.append("scope")
            return Scope()

    embed_batch = module._bind_embeddings(
        cast(Any, Embeddings()),
        cast(Any, Context()),
    )

    assert await embed_batch(["one", "two"]) == [[1.0, 0.0], [0.0, 1.0]]
    assert events == ["scope", "enter", "bind", "enter", "embed", "exit", "exit"]


@pytest.mark.asyncio
async def test_committed_turn_writes_plugin_owned_projection(tmp_path: Path) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    await runtime._process(_event())

    conn = sqlite3.connect(runtime._db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM proactive_feedback_events"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["feedback_type"] == "explicit_quote"
    assert row["user_content_preview"].startswith("被回复消息")
    assert row["assistant_content_preview"] == "我接着回答这个主题"
    assert row["proactive_content_preview"] == "主动提醒某个很长很长的主题"


@pytest.mark.asyncio
async def test_feedback_commit_is_readable_from_stable_history(tmp_path: Path) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    await runtime._process(_event())
    await runtime._process(_event())

    page = module.SqliteFeedbackHistory(runtime._db_path).page(
        after_cursor=0,
        max_items=10,
    )
    assert [record.event_id for record in page.records] == ["proactive_feedback:1"]
    assert page.records[0].session_key == "mobile:test"
    assert len(page.records[0].payload_hash) == 64
    with sqlite3.connect(runtime._db_path) as conn:
        assert conn.execute(
            "SELECT count(*) FROM proactive_feedback_outbox"
        ).fetchone() == (0,)


def test_committed_turn_identity_is_durable_without_message_text(tmp_path: Path) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    runtime.enqueue(_event())

    conn = sqlite3.connect(runtime._db_path)
    try:
        row = conn.execute(
            "SELECT session_key, user_message_id, assistant_message_id, "
            "turn_id, client_message_id, processed_at "
            "FROM proactive_feedback_input_inbox"
        ).fetchone()
        columns = {
            str(column[1])
            for column in conn.execute(
                "PRAGMA table_info(proactive_feedback_input_inbox)"
            )
        }
    finally:
        conn.close()
    assert row == ("mobile:test", "u1", "a1", "", "", None)
    assert "user_content" not in columns
    assert "assistant_content" not in columns


def test_candidate_enqueue_fails_before_any_write(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "proactive_feedback.db"
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService.candidate_validation(),
        embed_batch=_embed_batch,
        db_path=db_path,
    )

    with pytest.raises(RuntimeError, match="候选验证期禁止写入"):
        runtime.enqueue(_event())

    assert not db_path.exists()


@pytest.mark.asyncio
async def test_durable_input_replays_after_runtime_restart(tmp_path: Path) -> None:
    original = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    original.enqueue(_event())

    restarted = module.ProactiveFeedbackRuntime(
        session_read=original._session_read,
        embed_batch=_embed_batch,
        db_path=original._db_path,
    )
    assert await restarted._process_pending_inputs() is False
    assert len(module.SqliteFeedbackHistory(original._db_path).page(
        after_cursor=0, max_items=10
    ).records) == 1

    conn = sqlite3.connect(original._db_path)
    try:
        assert conn.execute(
            "SELECT processed_at FROM proactive_feedback_input_inbox"
        ).fetchone()[0] is not None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_durable_input_keeps_ordered_two_user_ids_and_scores_one_turn(
    tmp_path: Path,
) -> None:
    session_read = SessionReadService(
        lambda _key: (
            cast(Any, SimpleNamespace(
                messages=[dict(message) for message in _two_user_snapshot().messages],
                last_consolidated=0,
            )),
            None,
        )
    )
    original = module.ProactiveFeedbackRuntime(
        session_read=session_read,
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    original.enqueue(_two_user_event())

    restarted = module.ProactiveFeedbackRuntime(
        session_read=session_read,
        embed_batch=_embed_batch,
        db_path=original._db_path,
    )
    assert await restarted._process_pending_inputs() is False

    conn = sqlite3.connect(original._db_path)
    try:
        inbox = conn.execute(
            "SELECT user_message_id, user_message_ids_json, processed_at "
            "FROM proactive_feedback_input_inbox"
        ).fetchone()
        projection = conn.execute(
            "SELECT user_message_id, user_content_preview "
            "FROM proactive_feedback_events"
        ).fetchone()
    finally:
        conn.close()
    assert inbox == ("u2", '["u1", "u2"]', inbox[2])
    assert inbox[2] is not None
    assert projection[0] == "u2"
    assert "被回复消息" in projection[1] and "当前新消息" in projection[1]


@pytest.mark.asyncio
async def test_formal_boot_discovers_committed_turn_without_callback_once(
    tmp_path: Path,
) -> None:
    session_read = SessionReadService(
        lambda _key: (
            cast(Any, _session_state()),
            None,
        )
    )
    restarted = module.ProactiveFeedbackRuntime(
        session_read=session_read,
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
        session_keys=lambda: ("mobile:test",),
    )

    worker = asyncio.create_task(restarted.run_worker())
    for _ in range(100):
        if restarted._db_path.exists() and module.SqliteFeedbackHistory(
            restarted._db_path
        ).page(after_cursor=0, max_items=10).records:
            break
        await asyncio.sleep(0.01)
    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker
    restarted._discover_committed_inputs()

    conn = sqlite3.connect(restarted._db_path)
    try:
        assert conn.execute(
            "SELECT count(*) FROM proactive_feedback_input_inbox"
        ).fetchone() == (1,)
        assert conn.execute(
            "SELECT count(*) FROM proactive_feedback_events"
        ).fetchone() == (1,)
    finally:
        conn.close()


def test_formal_boot_prioritizes_new_session_over_old_catalog_window(
    tmp_path: Path,
) -> None:
    old_keys = tuple(f"old:{index:02d}" for index in range(64))
    new_key = "mobile:new"
    read_keys: list[str] = []

    def lookup(session_key: str) -> tuple[Any, None]:
        read_keys.append(session_key)
        return cast(Any, _session_state()), None

    db_path = tmp_path / "data" / "proactive_feedback.db"
    sink = module.open_db(db_path)
    try:
        for index, session_key in enumerate(old_keys):
            module.insert_feedback_input(
                sink,
                session_key=session_key,
                turn_id="",
                client_message_id="",
                user_message_id=f"old-u{index}",
                assistant_message_id="old-a",
            )
    finally:
        sink.close()

    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(lookup),
        embed_batch=_embed_batch,
        db_path=db_path,
        session_keys=lambda: (*old_keys, new_key),
    )
    runtime._discover_committed_inputs()

    assert new_key in read_keys
    assert len(read_keys) == 64
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT count(*) FROM proactive_feedback_input_inbox "
            "WHERE session_key = ?",
            (new_key,),
        ).fetchone() == (1,)
    finally:
        conn.close()


def test_formal_boot_rotates_turns_across_sessions_with_pending_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    old_keys = tuple(f"old:{index:02d}" for index in range(64))
    new_key = "mobile:new"
    read_keys: list[str] = []

    def lookup(session_key: str) -> tuple[Any, None]:
        read_keys.append(session_key)
        count = 1 if session_key == new_key else 5
        return cast(Any, _multi_turn_session_state(session_key, count=count)), None

    db_path = tmp_path / "data" / "proactive_feedback.db"
    sink = module.open_db(db_path)
    try:
        for session_key in old_keys:
            for index in range(5):
                module.insert_feedback_input(
                    sink,
                    session_key=session_key,
                    turn_id="",
                    client_message_id="",
                    user_message_id=f"{session_key}:u{index}",
                    assistant_message_id=f"{session_key}:a{index}",
                )
    finally:
        sink.close()

    insert_calls = 0
    real_insert = module.insert_feedback_input

    def track_insert(*args: Any, **kwargs: Any) -> int:
        nonlocal insert_calls
        insert_calls += 1
        return real_insert(*args, **kwargs)

    monkeypatch.setattr(module, "insert_feedback_input", track_insert)
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(lookup),
        embed_batch=_embed_batch,
        db_path=db_path,
        session_keys=lambda: (*old_keys, new_key),
    )
    runtime._discover_committed_inputs()

    assert new_key in read_keys
    assert len(read_keys) == 64
    assert insert_calls <= module._DISCOVERY_TURN_LIMIT
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT count(*) FROM proactive_feedback_input_inbox"
        ).fetchone() == (len(old_keys) * 5 + 1,)
        assert conn.execute(
            "SELECT count(*) FROM proactive_feedback_input_inbox "
            "WHERE session_key = ?",
            (new_key,),
        ).fetchone() == (1,)
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_durable_input_cancellation_keeps_pending_row(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started = asyncio.Event()

    async def blocked_scoring(**_kwargs: object) -> None:
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(module, "score_followup", blocked_scoring)
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    runtime.enqueue(_event())
    task = asyncio.create_task(runtime._process_pending_inputs())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    conn = sqlite3.connect(runtime._db_path)
    try:
        assert conn.execute(
            "SELECT processed_at FROM proactive_feedback_input_inbox"
        ).fetchone() == (None,)
    finally:
        conn.close()


def test_accepted_feedback_identity_is_immutable_and_drift_fails(tmp_path: Path) -> None:
    sink = module.open_db(tmp_path / "proactive_feedback.db")
    try:
        first = FeedbackEvent(
            session_key="mobile:test",
            user_message_id="u1",
            assistant_message_id="a1",
            proactive_message_id="p1",
            feedback_type="explicit_quote",
            confidence="gold",
            pa_score=1.0,
            pua_score=0.9,
            lag_seconds=8,
            candidate_count=1,
            matched_by="explicit_quote",
            reason="first_reason",
            user_content_preview="first user",
            assistant_content_preview="first answer",
            proactive_content_preview="first proactive",
        )
        row_id = module.insert_feedback(sink, first)
        assert row_id == 1
        second = FeedbackEvent(
            **{
                **first.__dict__,
                "feedback_type": "no_topic_follow",
                "confidence": "low",
                "pa_score": 0.1,
                "pua_score": 0.2,
                "reason": "second_reason",
                "user_content_preview": "second user",
                "assistant_content_preview": "second answer",
                "proactive_content_preview": "second proactive",
            }
        )
        with pytest.raises(RuntimeError, match="payload 漂移"):
            module.insert_feedback(sink, second)
        third = FeedbackEvent(
            **{**first.__dict__, "proactive_message_id": "p2"}
        )
        with pytest.raises(RuntimeError, match="payload 漂移"):
            module.insert_feedback(sink, third)
        assert module.insert_feedback(sink, first) == row_id
        assert tuple(sink.execute(
            "SELECT count(*) FROM proactive_feedback_events"
        ).fetchone()) == (1,)
        projection = tuple(sink.execute(
            "SELECT feedback_type, confidence, pa_score, pua_score, reason, "
            "user_content_preview, assistant_content_preview, proactive_content_preview "
            "FROM proactive_feedback_events WHERE id = 1"
        ).fetchone())
        assert tuple(sink.execute(
            "SELECT count(*) FROM proactive_feedback_outbox"
        ).fetchone()) == (0,)
    finally:
        sink.close()
    assert projection == (
        "explicit_quote",
        "gold",
        1.0,
        0.9,
        "first_reason",
        "first user",
        "first answer",
        "first proactive",
    )


@pytest.mark.asyncio
async def test_candidate_session_read_fails_before_any_write(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "proactive_feedback.db"
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService.candidate_validation(),
        embed_batch=_embed_batch,
        db_path=db_path,
    )
    with pytest.raises(RuntimeError, match="禁止读取正式 Session"):
        await runtime._process(_event())
    assert not db_path.exists()


@pytest.mark.asyncio
async def test_nonquoted_turn_keeps_pua_scoring_path(
    tmp_path: Path,
) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state(quoted=False)), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )

    await runtime._process(_event(quoted=False))
    conn = sqlite3.connect(runtime._db_path)
    try:
        row = conn.execute(
            "SELECT feedback_type, matched_by FROM proactive_feedback_events"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("topic_follow", "recent_pua")


@pytest.mark.asyncio
async def test_scoring_failure_records_unscored_feedback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )

    async def fail_scoring(**_kwargs: object) -> None:
        raise RuntimeError("embedding unavailable")

    monkeypatch.setattr(module, "score_followup", fail_scoring)
    await runtime._process(_event())
    conn = sqlite3.connect(runtime._db_path)
    try:
        row = conn.execute(
            "SELECT feedback_type, reason FROM proactive_feedback_events"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("unscored", "scoring_failed")


@pytest.mark.asyncio
async def test_in_process_cancellation_does_not_persist_partial_feedback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    started = asyncio.Event()

    async def blocked_scoring(**_kwargs: object) -> None:
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(module, "score_followup", blocked_scoring)
    task = asyncio.create_task(runtime._process(_event()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not runtime._db_path.exists()


@pytest.mark.asyncio
async def test_worker_cancellation_has_no_live_task(tmp_path: Path) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService.candidate_validation(),
        embed_batch=_embed_batch,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )
    task = asyncio.create_task(runtime.run_worker())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not runtime._db_path.exists()


def test_dashboard_reads_preview_projection_without_sessions_database(tmp_path: Path) -> None:
    sink = module.open_db(tmp_path / "proactive_feedback.db")
    try:
        module.insert_feedback(
            sink,
            FeedbackEvent(
                session_key="mobile:test",
                user_message_id="u1",
                assistant_message_id="a1",
                proactive_message_id="p1",
                feedback_type="explicit_quote",
                confidence="gold",
                pa_score=1.0,
                pua_score=1.0,
                lag_seconds=8,
                candidate_count=1,
                matched_by="explicit_quote",
                reason="explicit_quote",
                user_content_preview="被回复消息：主题\n\n【你当前新消息】继续",
                assistant_content_preview="回答",
                proactive_content_preview="主题",
            ),
        )
    finally:
        sink.close()
    reader = module.ProactiveFeedbackDashboardReader(tmp_path)
    items, total = reader.list_events()
    assert total == 1
    assert items[0]["quoted_preview"] == "主题"
    assert items[0]["user_reply_preview"] == "继续"
    assert items[0]["assistant_preview"] == "回答"


def test_new_feedback_does_not_touch_frozen_legacy_outbox(tmp_path: Path) -> None:
    sink = module.open_db(tmp_path / "proactive_feedback.db")
    try:
        _ = sink.execute(
            """
            INSERT INTO proactive_feedback_outbox(
                row_id, event_id, payload_json, published_at
            ) VALUES(91, 'legacy:91', '{"legacy":true}', '2026-08-01T00:00:00Z')
            """
        )
        sink.commit()
        before = tuple(sink.execute(
            "SELECT * FROM proactive_feedback_outbox"
        ).fetchone())
        module.insert_feedback(
            sink,
            FeedbackEvent(
                session_key="mobile:test",
                user_message_id="u1",
                assistant_message_id="a1",
                proactive_message_id="p1",
                feedback_type="explicit_quote",
                confidence="gold",
                pa_score=1.0,
                pua_score=1.0,
                lag_seconds=8,
                candidate_count=1,
                matched_by="explicit_quote",
                reason="explicit_quote",
                user_content_preview="继续",
                assistant_content_preview="回答",
                proactive_content_preview="主题",
            ),
        )
        assert tuple(sink.execute(
            "SELECT count(*) FROM proactive_feedback_events"
        ).fetchone()) == (1,)
        assert tuple(sink.execute(
            "SELECT * FROM proactive_feedback_outbox"
        ).fetchone()) == before
    finally:
        sink.close()


def test_history_missing_database_is_empty_without_creation(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "proactive_feedback.db"
    page = module.SqliteFeedbackHistory(path).page(after_cursor=0, max_items=10)
    assert page.records == ()
    assert not path.parent.exists()


def test_history_corrupt_or_incompatible_database_fails_loud(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"not sqlite")
    before = corrupt.read_bytes()
    with pytest.raises(sqlite3.DatabaseError):
        module.SqliteFeedbackHistory(corrupt).page(after_cursor=0, max_items=10)
    assert corrupt.read_bytes() == before

    incompatible = tmp_path / "incompatible.db"
    with sqlite3.connect(incompatible) as conn:
        conn.execute("CREATE TABLE unrelated(value TEXT)")
    with pytest.raises(RuntimeError, match="缺少 events 表"):
        module.SqliteFeedbackHistory(incompatible).page(
            after_cursor=0, max_items=10
        )

    malformed = tmp_path / "same-columns-without-constraints.db"
    valid = tmp_path / "valid-schema.db"
    connection = module.open_db(valid)
    try:
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='proactive_feedback_events'"
        ).fetchone()[0]
    finally:
        connection.close()
    malformed_sql = table_sql.replace(
        "id INTEGER PRIMARY KEY AUTOINCREMENT", "id INTEGER"
    ).replace(
        ",\n            UNIQUE(user_message_id, proactive_message_id)", ""
    )
    with sqlite3.connect(malformed) as connection:
        connection.execute(malformed_sql)
    with pytest.raises(RuntimeError, match="events table schema 不匹配"):
        module.SqliteFeedbackHistory(malformed).page(
            after_cursor=0, max_items=10
        )

    invalid_payload = tmp_path / "invalid-payload.db"
    connection = module.open_db(invalid_payload)
    try:
        module.insert_feedback(
            connection,
            FeedbackEvent(
                session_key="mobile:test",
                user_message_id="u1",
                assistant_message_id="a1",
                proactive_message_id="p1",
                feedback_type="topic_follow",
                confidence="high",
                pa_score=0.8,
                pua_score=0.7,
                lag_seconds=1,
                candidate_count=1,
                matched_by="pua",
                reason="fixture",
            ),
        )
        connection.execute(
            "UPDATE proactive_feedback_events SET confidence='unknown' WHERE id=1"
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(ValueError, match="confidence 不支持"):
        module.SqliteFeedbackHistory(invalid_payload).page(
            after_cursor=0, max_items=10
        )

    invalid_score = tmp_path / "invalid-score.db"
    connection = module.open_db(invalid_score)
    try:
        module.insert_feedback(
            connection,
            FeedbackEvent(
                session_key="mobile:test",
                user_message_id="u1",
                assistant_message_id="a1",
                proactive_message_id="p1",
                feedback_type="topic_follow",
                confidence="high",
                pa_score=0.8,
                pua_score=0.7,
                lag_seconds=1,
                candidate_count=1,
                matched_by="pua",
                reason="fixture",
            ),
        )
        connection.execute(
            "UPDATE proactive_feedback_events SET pa_score=? WHERE id=1",
            (float("inf"),),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(ValueError, match="pa_score 必须在"):
        module.SqliteFeedbackHistory(invalid_score).page(
            after_cursor=0, max_items=10
        )

    history_module = sys.modules["proactive_feedback_v3_test.plugin.history"]
    with pytest.raises(ValueError):
        history_module.accepted_payload_hash({"score": float("nan")})


def test_history_reads_exact_legacy_and_altered_table_lineage(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE proactive_feedback_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                session_key TEXT NOT NULL,
                user_message_id TEXT NOT NULL,
                assistant_message_id TEXT NOT NULL,
                proactive_message_id TEXT,
                feedback_type TEXT NOT NULL,
                confidence TEXT NOT NULL,
                pa_score REAL,
                pua_score REAL,
                lag_seconds INTEGER,
                candidate_count INTEGER NOT NULL,
                matched_by TEXT NOT NULL,
                reason TEXT NOT NULL,
                UNIQUE(user_message_id, proactive_message_id)
            );
            INSERT INTO proactive_feedback_events(
                session_key, user_message_id, assistant_message_id,
                proactive_message_id, feedback_type, confidence, pa_score,
                pua_score, lag_seconds, candidate_count, matched_by, reason
            ) VALUES (
                'mobile:test', 'u1', 'a1', 'p1', 'topic_follow', 'high',
                0.8, 0.7, 1, 1, 'pua', 'legacy fixture'
            );
            """
        )

    legacy = module.SqliteFeedbackHistory(path).page(after_cursor=0, max_items=10)
    assert legacy.records[0].user_content_preview is None

    migrated = module.open_db(path)
    migrated.close()
    with sqlite3.connect(path) as connection:
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='proactive_feedback_events'"
        ).fetchone()[0]
    assert table_sql.index("user_content_preview") < table_sql.index("UNIQUE(")
    page = module.SqliteFeedbackHistory(path).page(after_cursor=0, max_items=10)
    assert page.records == legacy.records


def test_history_pages_are_stable_and_new_rows_wait_for_next_page(tmp_path: Path) -> None:
    path = tmp_path / "proactive_feedback.db"
    sink = module.open_db(path)
    try:
        for index in range(3):
            event = FeedbackEvent(
                session_key="mobile:test",
                user_message_id=f"u{index}",
                assistant_message_id=f"a{index}",
                proactive_message_id=f"p{index}",
                feedback_type="topic_follow",
                confidence="high",
                pa_score=0.8,
                pua_score=0.7,
                lag_seconds=index,
                candidate_count=1,
                matched_by="pua",
                reason="fixture",
            )
            module.insert_feedback(sink, event)
    finally:
        sink.close()
    history = module.SqliteFeedbackHistory(path)
    first = history.page(after_cursor=0, max_items=2)
    assert [row.cursor for row in first.records] == [1, 2]
    repeated = history.page(after_cursor=0, max_items=2)
    assert repeated == first
    second = history.page(after_cursor=2, max_items=2)
    assert [row.cursor for row in second.records] == [3]


def test_plugin_runtime_does_not_move_legacy_database(tmp_path: Path) -> None:
    legacy = tmp_path / "workspace" / "proactive_feedback" / "proactive_feedback.db"
    legacy.parent.mkdir(parents=True)
    old = module.open_db(legacy)
    old.close()
    target_root = tmp_path / "plugin-data"
    runtime = module.ProactiveFeedbackRuntime(
        session_read=cast(Any, object()),
        embed_batch=_embed_batch,
        db_path=target_root / "proactive_feedback.db",
    )

    assert runtime._db_path == target_root / "proactive_feedback.db"
    assert legacy.exists()
    assert not target_root.exists()


@pytest.mark.asyncio
async def test_plugin_installs_and_loads_from_ordinary_cache(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for path in Path(__file__).parents[1].iterdir():
        if path.is_file() and path.name != ".git":
            shutil.copy2(path, source / path.name)
    _commit_plugin(source)
    workspace = tmp_path / "workspace"
    plugin_home = tmp_path / "home"
    installed = install_git_plugin(
        workspace=workspace,
        source=str(source),
        marketplace="ordinary-test",
        plugins_home=plugin_home,
    )
    core_plugins = Path(inspect.getfile(PluginManager)).parents[2] / "plugins"
    manager = PluginManager(
        plugin_dirs=[
            core_plugins / "shell_ui",
            core_plugins / "models",
            core_plugins / "openai_compatible",
            core_plugins / "workbench_ui",
        ],
        event_bus=EventBus(),
        tool_registry=None,
        session_manager=_empty_session_manager(),
        workspace=workspace,
        installed_cache_root=plugin_home / "cache",
    )
    try:
        await manager.load_all()
        generation = manager.generation("proactive_feedback@ordinary-test")
        assert generation is not None
        assert generation.source_type == "installed"
        assert generation.plugin_dir == installed.installed_path
        instance = cast(ComposablePlugin, generation.instance)
        assert instance.module.__file__ is not None
        assert Path(instance.module.__file__).resolve().is_relative_to(
            installed.installed_path
        )
        assert manager.current_snapshot is not None
        assert manager.current_snapshot.composition_root is not None
        assert manager.current_snapshot.composition_root.receipt().ready
    finally:
        await manager.terminate_all()


@pytest.mark.asyncio
async def test_manager_stable_candidate_ui_dashboard_and_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "proactive_feedback"
    core_plugins = Path(inspect.getfile(PluginManager)).parents[2] / "plugins"
    plugin_dir.mkdir(parents=True)
    for filename in (
        "plugin.py",
        "dashboard.py",
        "db.py",
        "history.py",
        "scorer.py",
        "mobile_panel.js",
        "mobile_panel.css",
        "web_module.js",
        "web_module.css",
        "akashic.plugin.toml",
    ):
        shutil.copy2(Path(__file__).parents[1] / filename, plugin_dir / filename)
    write_plugin_manifest(
        {"proactive_feedback": True},
        plugins_home=tmp_path / "home",
    )
    manager = PluginManager(
        plugin_dirs=[
            tmp_path / "plugins",
            core_plugins / "shell_ui",
            core_plugins / "models",
            core_plugins / "openai_compatible",
            core_plugins / "workbench_ui",
        ],
        event_bus=EventBus(),
        tool_registry=None,
        session_manager=_empty_session_manager(),
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "home" / "cache",
    )
    dashboard_host = PluginDashboardHost(core_routes=())
    try:
        await manager.load_all()
        stable = manager.current_snapshot
        assert stable is not None and stable.composition_root is not None
        assert stable.composition_root.receipt().ready
        assert stable.mobile_ui_registry is not None
        dashboard_host.prepare_initial_snapshot(stable)
        manager.bind_dashboard_preparer(
            dashboard_host.prepare_snapshot,
            validation_releaser=dashboard_host.release_validation,
        )
        mobile_provider = PluginMobileUiProvider(manager)
        assert mobile_provider.catalog()["items"]
        formal_data = stable.composition_root.plugin_runtime(
            "proactive_feedback"
        ).data_dir
        assert not (formal_data / "proactive_feedback.db").exists()
        formal_history = stable.composition_root.context.require(
            module.PROACTIVE_FEEDBACK_HISTORY
        )
        assert formal_history.page(after_cursor=0, max_items=10).records == ()
        formal_database = formal_data / "proactive_feedback.db"
        formal_database.parent.mkdir(parents=True, exist_ok=True)
        formal_connection = module.open_db(formal_database)
        formal_connection.close()
        formal_digest = hashlib.sha256(formal_database.read_bytes()).hexdigest()

        plugin_source = plugin_dir / "plugin.py"
        plugin_source.write_text(
            plugin_source.read_text(encoding="utf-8").replace(
                'version = "3.0.0"',
                'version = "3.0.1"',
            ),
            encoding="utf-8",
        )
        manifest = plugin_dir / "akashic.plugin.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "3.0.0"',
                'version = "3.0.1"',
            ),
            encoding="utf-8",
        )
        candidate = await manager.prepare_candidate("proactive_feedback")
        assert candidate is not None and candidate.runtime_snapshot is not None
        assert manager.current_snapshot is stable
        candidate_snapshot = candidate.runtime_snapshot
        assert candidate_snapshot.mobile_ui_registry is not None
        dashboard_host.prepare_snapshot(candidate_snapshot)
        binding = next(
            item
            for item in candidate_snapshot.dashboard_bindings
            if isinstance(item, DashboardBinding)
            and item.plugin_id == "proactive_feedback"
        )
        assert binding.validation is True
        assert binding.runtime_data_root is not None
        assert binding.runtime_data_root != formal_data.resolve()
        assert not (binding.runtime_data_root / "proactive_feedback.db").exists()
        candidate_root = candidate_snapshot.composition_root
        assert candidate_root is not None
        candidate_history = candidate_root.context.require(
            module.PROACTIVE_FEEDBACK_HISTORY
        )
        assert candidate_history.page(after_cursor=0, max_items=10).records == ()
        assert candidate_snapshot.composition_topology is not None
        assert stable.composition_topology is not None
        assert (
            candidate_snapshot.composition_topology.identity
            == stable.composition_topology.identity
        )
        candidate_root.context.emit(AFTER_TURN_COMMITTED, _event())
        assert not (binding.runtime_data_root / "proactive_feedback.db").exists()
        assert hashlib.sha256(formal_database.read_bytes()).hexdigest() == formal_digest
        result = await manager.publish_prepared("proactive_feedback")
        assert result["publication_state"] == "committed"
        assert manager.current_snapshot is not stable
    finally:
        await manager.terminate_all()
    receipt = stable.composition_root.receipt()
    assert receipt.effects == ()
    assert cast(Any, stable.composition_root)._events.registrations() == ()


def _session_state(*, quoted: bool = True) -> object:
    return SimpleNamespace(
        messages=[dict(message) for message in _snapshot(quoted=quoted).messages],
        last_consolidated=0,
    )


def _empty_session_manager() -> object:
    class ControlStore:
        def get_active_compaction(self, session_key: str) -> None:
            _ = session_key
            return None

    class SessionManager:
        control_store = ControlStore()

        def get_existing(self, session_key: str) -> None:
            raise KeyError(session_key)

    return SessionManager()
