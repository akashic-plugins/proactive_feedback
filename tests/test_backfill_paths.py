from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_backfill_module():
    path = Path(__file__).parents[1] / "scripts" / "backfill_proactive_feedback.py"
    spec = importlib.util.spec_from_file_location("test_backfill_paths_module", path)
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_backfill_module()


def test_explicit_workspace_has_priority(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("AKASHIC_WORKSPACE", raising=False)
    assert module._resolve_workspace(tmp_path) == tmp_path


def test_workspace_comes_from_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AKASHIC_WORKSPACE", str(tmp_path))
    assert module._resolve_workspace(None) == tmp_path


def test_missing_workspace_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AKASHIC_WORKSPACE", "   ")
    with pytest.raises(RuntimeError, match="AKASHIC_WORKSPACE"):
        module._resolve_workspace(None)


def test_blank_explicit_workspace_fails_loudly() -> None:
    with pytest.raises(RuntimeError, match="不能为空"):
        module._resolve_workspace(Path("   "))
