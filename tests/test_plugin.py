from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from agent.plugins.context import PluginContext, PluginKVStore
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


module = _load_plugin_module()
ProactiveFeedbackPlugin = module.ProactiveFeedbackPlugin
FeedbackEvent = module.FeedbackEvent


@pytest.mark.asyncio
async def test_proactive_feedback_summary_empty(tmp_path: Path) -> None:
    plugin = ProactiveFeedbackPlugin()
    plugin.context = PluginContext(
        event_bus=EventBus(),
        tool_registry=None,
        plugin_id="proactive_feedback",
        plugin_dir=tmp_path,
        data_dir=tmp_path,
        kv_store=PluginKVStore(tmp_path / ".kv.json"),
        workspace=tmp_path,
    )
    await plugin.initialize()
    try:
        summary = await plugin.get_summary(None)
    finally:
        await plugin.terminate()
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


def test_get_embedder_uses_plugin_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: list[Path] = []

    def fake_build_embedder(root: Path) -> object:
        seen.append(root)
        return object()

    plugin = ProactiveFeedbackPlugin()
    plugin.context = PluginContext(
        event_bus=EventBus(),
        tool_registry=None,
        plugin_id="proactive_feedback",
        plugin_dir=tmp_path,
        data_dir=tmp_path,
        kv_store=PluginKVStore(tmp_path / ".kv.json"),
        workspace=tmp_path,
    )
    plugin._embedder = None
    monkeypatch.setattr(module, "_build_embedder", fake_build_embedder)

    embedder = plugin._get_embedder()

    assert embedder is plugin._embedder
    assert seen == [tmp_path]
