#!/usr/bin/env python3
"""把 Proactive Feedback v2 SQLite 非破坏迁移到 v3 plugin-data。"""

from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import uuid

from agent.plugins.manifest import (
    ensure_workspace_plugin_data_dir,
    validate_workspace_plugin_data_path,
)
from bootstrap.workspace_lock import WorkspaceInstanceLock


_DATABASE = "proactive_feedback.db"
_RECEIPT = ".proactive-feedback-v2-migration.json"


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _integrity(path: Path) -> None:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as database:
        result = database.execute("PRAGMA integrity_check").fetchone()
    if result != ("ok",):
        raise sqlite3.DatabaseError(f"Proactive Feedback SQLite 损坏: {path}")


def _backup(source: Path, destination: Path) -> None:
    """Create and verify one transactionally consistent SQLite copy."""

    _integrity(source)
    source_uri = f"{source.resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source_db:
        with closing(sqlite3.connect(destination)) as destination_db:
            source_db.backup(destination_db, pages=256, sleep=0.1)
            destination_db.commit()
    _integrity(destination)


def _remove_crash_staging(workspace: Path) -> None:
    parent = workspace / "plugin-data"
    if parent.is_symlink():
        raise ValueError(f"plugin-data 根不得是符号链接: {parent}")
    if not parent.is_dir():
        return
    for path in parent.glob(".proactive-feedback-v2-migrate-*"):
        if path.is_symlink() or not path.is_dir():
            raise ValueError(f"Proactive Feedback staging 无效: {path}")
        shutil.rmtree(path)


def _verify_receipt(
    target: Path,
    source: Path,
    receipt: dict[str, object],
) -> dict[str, object]:
    """Verify the durable receipt and its exact published database."""

    database = receipt.get("database")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("source") != "proactive_feedback/proactive_feedback.db"
        or receipt.get("target") != f"plugin-data/{target.name}/{_DATABASE}"
        or receipt.get("source_retained") is not True
        or not isinstance(database, dict)
    ):
        raise ValueError("Proactive Feedback migration receipt 身份无效")
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"Proactive Feedback migration 旧源已丢失: {source}")
    _integrity(source)
    expected = database.get("sha256")
    size = database.get("size")
    path = target / _DATABASE
    if (
        database.get("name") != _DATABASE
        or not isinstance(expected, str)
        or len(expected) != 64
        or not isinstance(size, int)
        or isinstance(size, bool)
        or path.is_symlink()
        or not path.is_file()
        or path.stat().st_size != size
        or _digest(path) != expected
    ):
        raise ValueError(f"Proactive Feedback migration 目标漂移: {path}")
    _integrity(path)
    return receipt


def _read_receipt(path: Path) -> dict[str, object] | None:
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Proactive Feedback migration receipt 无效: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Proactive Feedback migration receipt 无效: {path}")
    return value


def _migrate_locked(workspace: Path, marketplace: str) -> dict[str, object]:
    """Stage, publish, and verify one idempotent v2 database migration."""

    # 1. Validate all durable paths before opening SQLite.
    if not marketplace or not marketplace.replace("-", "").replace("_", "").isalnum():
        raise ValueError(f"Proactive Feedback marketplace 无效: {marketplace}")
    legacy_root = workspace / "proactive_feedback"
    source = legacy_root / _DATABASE
    if (
        legacy_root.is_symlink()
        or not legacy_root.is_dir()
        or source.is_symlink()
        or not source.is_file()
        or not source.resolve().is_relative_to(workspace)
    ):
        raise FileNotFoundError(f"Proactive Feedback v2 数据库不存在或不安全: {source}")
    target = workspace / "plugin-data" / f"proactive_feedback-{marketplace}"
    validate_workspace_plugin_data_path(target, workspace)
    _remove_crash_staging(workspace)
    existing = _read_receipt(target / _RECEIPT)
    if existing is not None:
        return _verify_receipt(target, source, existing)

    # 2. Freeze a consistent source snapshot outside the published target.
    parent = workspace / "plugin-data"
    parent.mkdir(parents=True, exist_ok=True)
    staging = parent / f".proactive-feedback-v2-migrate-{uuid.uuid4().hex}"
    staging.mkdir()
    staged_database = staging / _DATABASE
    target_created = not target.exists()
    try:
        _backup(source, staged_database)
        digest = _digest(staged_database)
        size = staged_database.stat().st_size
        ensure_workspace_plugin_data_dir(target, workspace)
        destination = target / _DATABASE
        published = False
        if destination.exists() or destination.is_symlink():
            if (
                destination.is_symlink()
                or not destination.is_file()
                or destination.stat().st_size != size
                or _digest(destination) != digest
            ):
                raise FileExistsError(
                    f"Proactive Feedback v3 目标已存在且内容不同: {destination}"
                )
            _integrity(destination)
        else:
            os.replace(staged_database, destination)
            published = True

        # 3. Publish the receipt last; in-process failure rolls back this run.
        receipt: dict[str, object] = {
            "schema_version": 1,
            "source": "proactive_feedback/proactive_feedback.db",
            "target": f"plugin-data/{target.name}/{_DATABASE}",
            "source_retained": True,
            "database": {"name": _DATABASE, "sha256": digest, "size": size},
        }
        staged_receipt = staging / _RECEIPT
        staged_receipt.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        try:
            os.replace(staged_receipt, target / _RECEIPT)
        except BaseException:
            if published:
                destination.unlink(missing_ok=True)
            raise
        return _verify_receipt(target, source, receipt)
    except BaseException:
        if target_created and target.is_dir() and not any(target.iterdir()):
            target.rmdir()
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def migrate_v2_data(workspace: Path, marketplace: str) -> dict[str, object]:
    """Hold the workspace owner lock while migrating the v2 database."""

    resolved = workspace.expanduser().resolve()
    lock = WorkspaceInstanceLock(resolved)
    lock.acquire()
    try:
        return _migrate_locked(resolved, marketplace)
    finally:
        lock.release()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--marketplace", default="github")
    args = parser.parse_args()
    print(
        json.dumps(
            migrate_v2_data(args.workspace, args.marketplace),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
