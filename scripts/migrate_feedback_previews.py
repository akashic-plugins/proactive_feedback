from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path


_PREVIEW_MAX_CHARS = 2400
_PREVIEW_COLUMNS = (
    "user_content_preview",
    "assistant_content_preview",
    "proactive_content_preview",
)


@dataclass(frozen=True)
class MigrationStats:
    scanned: int
    updated: int


def migrate_feedback_previews(
    *,
    sessions_db: Path,
    feedback_db: Path,
) -> MigrationStats:
    """Fill plugin-owned display projections from immutable Session messages."""

    if not sessions_db.exists():
        raise FileNotFoundError(sessions_db)
    if not feedback_db.exists():
        raise FileNotFoundError(feedback_db)
    if sessions_db.resolve() == feedback_db.resolve():
        raise ValueError("sessions.db 与 feedback.db 必须是两个不同文件")
    source = sqlite3.connect(sessions_db)
    source.row_factory = sqlite3.Row
    sink = sqlite3.connect(feedback_db)
    sink.row_factory = sqlite3.Row
    try:
        # 1. Add only nullable projection columns; existing identities remain intact.
        _ensure_preview_columns(sink)
        rows = sink.execute(
            """
            SELECT id, user_message_id, assistant_message_id, proactive_message_id
            FROM proactive_feedback_events
            ORDER BY id ASC
            """
        ).fetchall()
        updated = 0

        # 2. Resolve all message text in one bounded read batch per event row.
        for row in rows:
            ids = tuple(
                str(value)
                for value in (
                    row["user_message_id"],
                    row["assistant_message_id"],
                    row["proactive_message_id"],
                )
                if value
            )
            if not ids:
                continue
            placeholders = ",".join("?" for _ in ids)
            messages = source.execute(
                f"SELECT id, content FROM messages WHERE id IN ({placeholders})",
                ids,
            ).fetchall()
            content = {
                str(message["id"]): _bounded_preview(message["content"])
                for message in messages
            }
            if not content:
                continue
            cursor = sink.execute(
                """
                UPDATE proactive_feedback_events
                SET user_content_preview = COALESCE(user_content_preview, ?),
                    assistant_content_preview = COALESCE(assistant_content_preview, ?),
                    proactive_content_preview = COALESCE(proactive_content_preview, ?)
                WHERE id = ?
                """,
                (
                    content.get(str(row["user_message_id"])),
                    content.get(str(row["assistant_message_id"])),
                    content.get(str(row["proactive_message_id"])),
                    row["id"],
                ),
            )
            updated += cursor.rowcount
        sink.commit()
        return MigrationStats(scanned=len(rows), updated=updated)
    finally:
        sink.close()
        source.close()


def _ensure_preview_columns(conn: sqlite3.Connection) -> None:
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(proactive_feedback_events)")
    }
    for name in _PREVIEW_COLUMNS:
        if name not in columns:
            _ = conn.execute(
                f"ALTER TABLE proactive_feedback_events ADD COLUMN {name} TEXT"
            )


def _bounded_preview(value: object) -> str:
    return str(value or "")[:_PREVIEW_MAX_CHARS]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="从 sessions.db 补齐 proactive feedback Dashboard 文本投影"
    )
    _ = parser.add_argument("--sessions-db", type=Path, required=True)
    _ = parser.add_argument("--feedback-db", type=Path, required=True)
    args = parser.parse_args()
    stats = migrate_feedback_previews(
        sessions_db=args.sessions_db,
        feedback_db=args.feedback_db,
    )
    print(f"scanned={stats.scanned} updated={stats.updated}")


if __name__ == "__main__":
    main()
