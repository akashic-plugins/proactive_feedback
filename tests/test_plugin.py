from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from agent.plugins.context import PluginContext, PluginKVStore
from agent.plugins.scope import PluginScope, ScopedEventBus
from bus.event_bus import EventBus


def _load_plugin_module():
    path = Path(__file__).parents[1] / "plugin.py"
    spec = importlib.util.spec_from_file_location(
        "test_proactive_feedback_plugin",
        path,
        submodule_search_locations=[str(path.parent)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _plugin_context(tmp_path: Path) -> PluginContext:
    scope = PluginScope("proactive_feedback")
    return PluginContext(
        event_bus=ScopedEventBus(EventBus(), scope),
        tool_registry=None,
        plugin_id="proactive_feedback",
        plugin_dir=tmp_path,
        data_dir=tmp_path,
        kv_store=PluginKVStore(tmp_path / ".kv.json"),
        workspace=tmp_path,
        scope=scope,
    )


module = _load_plugin_module()
ProactiveFeedbackPlugin = module.ProactiveFeedbackPlugin
FeedbackEvent = module.FeedbackEvent


@pytest.mark.asyncio
async def test_proactive_feedback_summary_empty(tmp_path: Path) -> None:
    plugin = ProactiveFeedbackPlugin()
    scope = PluginScope("proactive_feedback")
    plugin.context = PluginContext(
        event_bus=ScopedEventBus(EventBus(), scope),
        tool_registry=None,
        plugin_id="proactive_feedback",
        plugin_dir=tmp_path,
        data_dir=tmp_path,
        kv_store=PluginKVStore(tmp_path / ".kv.json"),
        workspace=tmp_path,
        scope=scope,
    )
    await plugin.initialize()
    try:
        summary = await plugin.get_summary(None)
    finally:
        await plugin.terminate()
        assert await scope.aclose() == []
    assert summary["total"] == 0


def test_recorded_event_matches_runtime_shape() -> None:
    feedback = FeedbackEvent(
        session_key="telegram:1",
        user_message_id="u1",
        assistant_message_id="a1",
        proactive_message_id="p1",
        feedback_type="topic_follow",
        confidence="high",
        pa_score=0.9,
        pua_score=0.8,
        lag_seconds=12,
        candidate_count=2,
        matched_by="recent_pua",
        reason="matched",
    )

    event = module._recorded_event(7, feedback)

    assert event.event_id == 7
    assert event.session_key == "telegram:1"
    assert event.user_message_id == "u1"
    assert event.assistant_message_id == "a1"
    assert event.proactive_message_id == "p1"
    assert event.feedback_type == "topic_follow"
    assert event.confidence == "high"
    assert event.pua_score == 0.8
    assert event.lag_seconds == 12
    assert event.matched_by == "recent_pua"


def test_get_embedder_uses_workspace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: list[Path] = []

    def fake_build_embedder(root: Path) -> object:
        seen.append(root)
        return object()

    plugin = ProactiveFeedbackPlugin()
    plugin.context = _plugin_context(tmp_path)
    plugin._workspace = tmp_path
    plugin._embedder = None
    monkeypatch.setattr(module, "_build_embedder", fake_build_embedder)

    embedder = plugin._get_embedder()

    assert embedder is plugin._embedder
    assert seen == [tmp_path]


def test_mobile_contribution_declares_dashboard() -> None:
    contribution = ProactiveFeedbackPlugin.mobile_ui()

    assert contribution.module == "mobile_panel.js"
    assert contribution.stylesheet == "mobile_panel.css"
    assert contribution.navigation is not None
    assert contribution.navigation.label == "主动反馈"


def test_mobile_feedback_projection_reuses_dashboard_reader(tmp_path: Path) -> None:
    plugin = ProactiveFeedbackPlugin()
    plugin.context = _plugin_context(tmp_path)
    sink = module.open_db(tmp_path / "proactive_feedback" / "proactive_feedback.db")
    try:
        module.insert_feedback(
            sink,
            FeedbackEvent(
                session_key="mobile:test",
                user_message_id="u-mobile",
                assistant_message_id="a-mobile",
                proactive_message_id="p-mobile",
                feedback_type="explicit_quote",
                confidence="gold",
                pa_score=1.0,
                pua_score=None,
                lag_seconds=8,
                candidate_count=1,
                matched_by="quote",
                reason="explicit_quote",
            ),
        )
    finally:
        sink.close()

    overview = plugin.mobile_ui_query(
        "feedback.overview",
        {},
        session_id=None,
        turn_id=None,
    )
    page = plugin.mobile_ui_query(
        "feedback.events",
        {"page": 1, "page_size": 30, "feedback_type": "explicit_quote"},
        session_id=None,
        turn_id=None,
    )

    assert overview["total"] == 1
    assert overview["follow_rate"] == 1.0
    assert page["total"] == 1
    assert page["items"][0]["feedback_type"] == "explicit_quote"


def test_mobile_feedback_projection_rejects_unknown_filter(tmp_path: Path) -> None:
    plugin = ProactiveFeedbackPlugin()
    plugin.context = _plugin_context(tmp_path)

    with pytest.raises(ValueError, match="feedback_type 不受支持"):
        plugin.mobile_ui_query(
            "feedback.events",
            {"feedback_type": "invented"},
            session_id=None,
            turn_id=None,
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"page": True}, "page 必须"),
        ({"page_size": 51}, "page_size 必须"),
    ],
)
def test_mobile_feedback_projection_rejects_invalid_page(
    tmp_path: Path,
    payload: dict[str, object],
    message: str,
) -> None:
    plugin = ProactiveFeedbackPlugin()
    plugin.context = _plugin_context(tmp_path)

    with pytest.raises(ValueError, match=message):
        plugin.mobile_ui_query(
            "feedback.events",
            payload,
            session_id=None,
            turn_id=None,
        )


def test_mobile_feedback_projection_rejects_unknown_method(tmp_path: Path) -> None:
    plugin = ProactiveFeedbackPlugin()
    plugin.context = _plugin_context(tmp_path)

    with pytest.raises(ValueError, match="未知 proactive_feedback 移动方法"):
        plugin.mobile_ui_query(
            "feedback.delete",
            {},
            session_id=None,
            turn_id=None,
        )
