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
