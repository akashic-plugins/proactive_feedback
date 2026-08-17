from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import inspect
import shutil
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from agent.plugin_composition import SessionReadService, SessionReadSnapshot
from agent.plugins.composable import ComposablePlugin
from agent.plugins.dashboard_host import DashboardBinding, PluginDashboardHost
from agent.plugins.manager import PluginManager
from agent.plugins.mobile_ui import PluginMobileUiProvider
from agent.plugins.manifest import write_plugin_manifest
from agent.turn_events.proactive_feedback import ProactiveFeedbackCommitted
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
    assert "event.extra" not in source
    assert "【你当前新消息】" not in source


def test_embedder_uses_core_config_and_shared_http_requester(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "runtime.toml"
    requester = object()
    embedding = SimpleNamespace(
        base_url="https://embedding.example/v1",
        api_key="test-key",
        model="text-embedding-v3",
        output_dimensionality=1024,
    )
    seen: list[tuple[Path, Path]] = []

    def fake_load(path: str | Path, *, workspace: str | Path) -> object:
        seen.append((Path(path), Path(workspace)))
        return SimpleNamespace(memory=SimpleNamespace(embedding=embedding))

    class FakeEmbedder:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    monkeypatch.setenv("AKASHIC_CONFIG", str(config_path))
    monkeypatch.setattr(module.CoreConfig, "load", fake_load)
    monkeypatch.setattr(module, "get_default_http_requester", lambda _profile: requester)
    monkeypatch.setattr(module, "Embedder", FakeEmbedder)

    embedder = module._build_embedder(tmp_path)

    assert seen == [(config_path, tmp_path)]
    assert embedder.kwargs == {
        "base_url": embedding.base_url,
        "api_key": embedding.api_key,
        "model": embedding.model,
        "output_dimensionality": embedding.output_dimensionality,
        "requester": requester,
    }


@pytest.mark.asyncio
async def test_committed_turn_writes_plugin_owned_projection(tmp_path: Path) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        workspace=tmp_path,
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
async def test_feedback_commit_publishes_typed_event_and_cursor(tmp_path: Path) -> None:
    published: list[ProactiveFeedbackCommitted] = []

    async def publish(event: ProactiveFeedbackCommitted) -> None:
        published.append(event)

    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        workspace=tmp_path,
        db_path=tmp_path / "data" / "proactive_feedback.db",
        publish_feedback=publish,
    )
    await runtime._process(_event())
    await runtime._process(_event())

    assert [event.event_id for event in published] == ["proactive_feedback:1"]
    assert published[0].event_id == "proactive_feedback:1"
    assert published[0].session_key == "mobile:test"
    assert published[0].user_content_preview is not None
    conn = sqlite3.connect(runtime._db_path)
    try:
        outbox = conn.execute(
            "SELECT published_at FROM proactive_feedback_outbox"
        ).fetchone()
        cursor = conn.execute(
            "SELECT row_id FROM proactive_feedback_published_cursor "
            "WHERE name = 'proactive_feedback'"
        ).fetchone()
    finally:
        conn.close()
    assert outbox is not None and outbox[0] is not None
    assert cursor == (1,)


@pytest.mark.asyncio
async def test_failed_publication_is_replayed_after_restart(tmp_path: Path) -> None:
    async def fail(_event: ProactiveFeedbackCommitted) -> None:
        raise RuntimeError("observer unavailable")

    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        workspace=tmp_path,
        db_path=tmp_path / "data" / "proactive_feedback.db",
        publish_feedback=fail,
    )
    with pytest.raises(RuntimeError, match="observer unavailable"):
        await runtime._process(_event())

    conn = sqlite3.connect(runtime._db_path)
    try:
        assert conn.execute(
            "SELECT published_at FROM proactive_feedback_outbox"
        ).fetchone() == (None,)
    finally:
        conn.close()

    replayed: list[ProactiveFeedbackCommitted] = []

    async def publish(event: ProactiveFeedbackCommitted) -> None:
        replayed.append(event)

    restarted = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService.candidate_validation(),
        workspace=tmp_path,
        db_path=runtime._db_path,
        publish_feedback=publish,
    )
    await restarted._publish_pending()
    assert [event.event_id for event in replayed] == ["proactive_feedback:1"]
    conn = sqlite3.connect(runtime._db_path)
    try:
        assert conn.execute(
            "SELECT published_at FROM proactive_feedback_outbox"
        ).fetchone()[0] is not None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_publication_cancellation_keeps_pending_receipt(tmp_path: Path) -> None:
    started = asyncio.Event()

    async def blocked(_event: ProactiveFeedbackCommitted) -> None:
        started.set()
        await asyncio.Future()

    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state()), None)
        ),
        workspace=tmp_path,
        db_path=tmp_path / "data" / "proactive_feedback.db",
        publish_feedback=blocked,
    )
    task = asyncio.create_task(runtime._process(_event()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    conn = sqlite3.connect(runtime._db_path)
    try:
        assert conn.execute(
            "SELECT published_at FROM proactive_feedback_outbox"
        ).fetchone() == (None,)
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_candidate_session_read_fails_before_any_write(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "proactive_feedback.db"
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService.candidate_validation(),
        workspace=tmp_path,
        db_path=db_path,
    )
    with pytest.raises(RuntimeError, match="禁止读取正式 Session"):
        await runtime._process(_event())
    assert not db_path.exists()


@pytest.mark.asyncio
async def test_nonquoted_turn_keeps_pua_scoring_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime = module.ProactiveFeedbackRuntime(
        session_read=SessionReadService(
            lambda _key: (cast(Any, _session_state(quoted=False)), None)
        ),
        workspace=tmp_path,
        db_path=tmp_path / "data" / "proactive_feedback.db",
    )

    class EmbedderStub:
        async def embed_batch(self, texts: list[str]) -> list[list[float]]:
            assert len(texts) == 3
            return [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]]

    monkeypatch.setattr(runtime, "_get_embedder", lambda: EmbedderStub())
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
        workspace=tmp_path,
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
        workspace=tmp_path,
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
        workspace=tmp_path,
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


def test_feedback_projection_and_outbox_commit_atomically(tmp_path: Path) -> None:
    sink = module.open_db(tmp_path / "proactive_feedback.db")
    try:
        _ = sink.execute(
            """
            CREATE TRIGGER reject_feedback_outbox
            BEFORE INSERT ON proactive_feedback_outbox
            BEGIN
                SELECT RAISE(ABORT, 'outbox unavailable');
            END;
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="outbox unavailable"):
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
        ).fetchone()) == (0,)
        assert tuple(sink.execute(
            "SELECT count(*) FROM proactive_feedback_outbox"
        ).fetchone()) == (0,)
    finally:
        sink.close()


def test_plugin_runtime_does_not_move_legacy_database(tmp_path: Path) -> None:
    legacy = tmp_path / "workspace" / "proactive_feedback" / "proactive_feedback.db"
    legacy.parent.mkdir(parents=True)
    old = module.open_db(legacy)
    old.close()
    target_root = tmp_path / "plugin-data"
    runtime = module.ProactiveFeedbackRuntime(
        session_read=cast(Any, object()),
        workspace=tmp_path / "workspace",
        db_path=target_root / "proactive_feedback.db",
    )

    assert runtime._db_path == target_root / "proactive_feedback.db"
    assert legacy.exists()
    assert not target_root.exists()


@pytest.mark.asyncio
async def test_manager_stable_candidate_ui_dashboard_and_cleanup(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins" / "proactive_feedback"
    plugin_dir.mkdir(parents=True)
    for filename in (
        "plugin.py",
        "dashboard.py",
        "db.py",
        "scorer.py",
        "mobile_panel.js",
        "mobile_panel.css",
        "akashic.plugin.toml",
    ):
        shutil.copy2(Path(__file__).parents[1] / filename, plugin_dir / filename)
    write_plugin_manifest(
        {"proactive_feedback": True},
        plugins_home=tmp_path / "home",
    )
    manager = PluginManager(
        plugin_dirs=[tmp_path / "plugins"],
        event_bus=EventBus(),
        tool_registry=None,
        session_manager=_empty_session_manager(),
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "home" / "cache",
    )
    dashboard_host = PluginDashboardHost(
        workspace=tmp_path / "workspace",
        memory_admin=object(),
        memory_store=object(),
        core_routes=(),
    )
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
        assert len(candidate_snapshot.dashboard_bindings) == 1
        binding = candidate_snapshot.dashboard_bindings[0]
        assert isinstance(binding, DashboardBinding)
        assert binding.validation is True
        assert binding.runtime_data_root is not None
        assert binding.runtime_data_root != formal_data.resolve()
        assert not (binding.runtime_data_root / "proactive_feedback.db").exists()
        assert hashlib.sha256(formal_database.read_bytes()).hexdigest() == formal_digest
        await manager.discard_prepared("proactive_feedback")
        assert manager.current_snapshot is stable
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
