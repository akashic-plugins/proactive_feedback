from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from scripts import migrate_v2_data as migration


def _source(workspace: Path) -> Path:
    path = workspace / "proactive_feedback" / "proactive_feedback.db"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT)")
        database.execute("INSERT INTO evidence(value) VALUES ('retained')")
        database.commit()
    return path


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_in_process_failure_rolls_back_new_target_and_retains_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    source = _source(workspace)
    source_digest = _digest(source)
    original_replace = migration.os.replace
    calls = 0

    def fail_receipt(source_path: Path, target_path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected receipt failure")
        original_replace(source_path, target_path)

    monkeypatch.setattr(migration.os, "replace", fail_receipt)
    with pytest.raises(OSError, match="injected receipt failure"):
        _ = migration.migrate_v2_data(workspace, "github")

    assert source.is_file() and _digest(source) == source_digest
    assert not (workspace / "plugin-data" / "proactive_feedback-github").exists()


def test_core_process_crash_resumes_partial_publication(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    source = _source(workspace)
    source_digest = _digest(source)
    repo = Path(__file__).resolve().parents[1]
    core = Path(os.environ["AKASHIC_AGENT_ROOT"])
    code = """
import os
from pathlib import Path
from scripts import migrate_v2_data as migration

real_replace = migration.os.replace
calls = 0
def crash_receipt(source, target):
    global calls
    calls += 1
    if calls == 2:
        os._exit(137)
    real_replace(source, target)
migration.os.replace = crash_receipt
migration.migrate_v2_data(Path(os.environ['FEEDBACK_TEST_WORKSPACE']), 'github')
"""
    environment = {
        **os.environ,
        "AKASHIC_AGENT_ROOT": str(core),
        "FEEDBACK_TEST_WORKSPACE": str(workspace),
        "PYTHONPATH": os.pathsep.join((str(repo), str(core))),
    }
    crashed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=repo,
        env=environment,
        check=False,
    )
    assert crashed.returncode == 137

    target = workspace / "plugin-data" / "proactive_feedback-github"
    assert (target / "proactive_feedback.db").is_file()
    assert not (target / ".proactive-feedback-v2-migration.json").exists()
    receipt = migration.migrate_v2_data(workspace, "github")

    assert receipt["source_retained"] is True
    assert source.is_file() and _digest(source) == source_digest
    assert (target / ".proactive-feedback-v2-migration.json").is_file()
    assert not list((workspace / "plugin-data").glob(".proactive-feedback-v2-migrate-*"))


def test_completed_receipt_requires_retained_source(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    source = _source(workspace)
    _ = migration.migrate_v2_data(workspace, "github")
    source.unlink()

    with pytest.raises(FileNotFoundError, match="不存在或不安全"):
        _ = migration.migrate_v2_data(workspace, "github")


def test_legacy_parent_symlink_is_rejected(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    _ = _source(outside)
    workspace.mkdir()
    (workspace / "proactive_feedback").symlink_to(
        outside / "proactive_feedback",
        target_is_directory=True,
    )

    with pytest.raises(FileNotFoundError, match="不存在或不安全"):
        _ = migration.migrate_v2_data(workspace, "github")
