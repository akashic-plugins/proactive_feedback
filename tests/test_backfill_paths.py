from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path


def _load_preview_migration_module():
    path = Path(__file__).parents[1] / "scripts" / "migrate_feedback_previews.py"
    spec = importlib.util.spec_from_file_location(
        "test_feedback_preview_migration_module",
        path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


preview_module = _load_preview_migration_module()


def test_preview_migration_keeps_feedback_identity_and_fills_text(tmp_path: Path) -> None:
    sessions = tmp_path / "sessions.db"
    source = sqlite3.connect(sessions)
    try:
        _ = source.execute(
            "CREATE TABLE messages (id TEXT PRIMARY KEY, content TEXT)"
        )
        _ = source.executemany(
            "INSERT INTO messages (id, content) VALUES (?, ?)",
            [("u1", "user text"), ("a1", "assistant text"), ("p1", "proactive text")],
        )
        source.commit()
    finally:
        source.close()

    feedback = tmp_path / "feedback.db"
    sink = sqlite3.connect(feedback)
    try:
        _ = sink.execute(
            """
            CREATE TABLE proactive_feedback_events (
                id INTEGER PRIMARY KEY,
                user_message_id TEXT NOT NULL,
                assistant_message_id TEXT NOT NULL,
                proactive_message_id TEXT
            )
            """
        )
        _ = sink.execute(
            "INSERT INTO proactive_feedback_events VALUES (7, 'u1', 'a1', 'p1')"
        )
        sink.commit()
    finally:
        sink.close()

    stats = preview_module.migrate_feedback_previews(
        sessions_db=sessions,
        feedback_db=feedback,
    )
    assert stats == preview_module.MigrationStats(scanned=1, updated=1)
    check = sqlite3.connect(feedback)
    try:
        row = check.execute(
            "SELECT id, user_content_preview, assistant_content_preview, proactive_content_preview "
            "FROM proactive_feedback_events"
        ).fetchone()
    finally:
        check.close()
    assert row == (7, "user text", "assistant text", "proactive text")
