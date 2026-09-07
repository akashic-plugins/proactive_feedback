from __future__ import annotations

import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest


def _load_db():
    root = Path(__file__).parents[1]
    package_name = "proactive_feedback_inbox_migration_test"
    package = type(sys)(package_name)
    package.__path__ = [str(root)]
    sys.modules[package_name] = package
    spec = importlib.util.spec_from_file_location(package_name + ".db", root / "db.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


db = _load_db()


def test_old_input_inbox_is_backfilled_once_with_database_backup(tmp_path: Path) -> None:
    path = tmp_path / "proactive_feedback.db"
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE proactive_feedback_input_inbox (
                id INTEGER PRIMARY KEY, session_key TEXT NOT NULL,
                turn_id TEXT NOT NULL DEFAULT '',
                client_message_id TEXT NOT NULL DEFAULT '',
                user_message_id TEXT NOT NULL,
                assistant_message_id TEXT, processed_at TEXT
            );
            INSERT INTO proactive_feedback_input_inbox(
                id, session_key, user_message_id, processed_at
            ) VALUES (1, 's', 'u1', NULL);
            """)

    connection = db.open_db(path)
    try:
        assert db.pending_feedback_inputs(connection)[0].user_message_ids == ("u1",)
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        connection.execute(
            "UPDATE proactive_feedback_input_inbox SET user_message_ids_json = '[]'"
        )
        connection.commit()
    finally:
        connection.close()
    with sqlite3.connect(path.with_name(path.name + ".before-message-ids")) as saved:
        assert "user_message_ids_json" not in {
            row[1] for row in saved.execute(
                "PRAGMA table_info(proactive_feedback_input_inbox)"
            )
        }
    reopened = db.open_db(path)
    try:
        with pytest.raises(ValueError, match="不能为空"):
            db.pending_feedback_inputs(reopened)
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "encoded", ('not-json', '"not-an-array"', '["u1", "u1"]', '["wrong-tail"]'),
)
def test_normal_read_rejects_corrupt_ordered_message_ids(
    tmp_path: Path, encoded: str,
) -> None:
    connection = db.open_db(tmp_path / "proactive_feedback.db")
    try:
        connection.execute(
            """
            INSERT INTO proactive_feedback_input_inbox(
                session_key, user_message_id, user_message_ids_json
            ) VALUES ('s', 'u1', ?)
            """,
            (encoded,),
        )
        connection.commit()
        with pytest.raises((TypeError, ValueError)):
            db.pending_feedback_inputs(connection)
    finally:
        connection.close()
