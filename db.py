from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FeedbackEvent:
    session_key: str
    user_message_id: str
    assistant_message_id: str
    proactive_message_id: str | None
    feedback_type: str
    confidence: str
    pa_score: float | None
    pua_score: float | None
    lag_seconds: int | None
    candidate_count: int
    matched_by: str
    reason: str
    user_content_preview: str | None = None
    assistant_content_preview: str | None = None
    proactive_content_preview: str | None = None


@dataclass(frozen=True)
class FeedbackOutboxRecord:
    """Describe one durable typed-event payload waiting for publication."""

    row_id: int
    event_id: str
    payload_json: str


@dataclass(frozen=True)
class FeedbackInputRecord:
    """Describe one committed Turn identity waiting for durable processing."""

    row_id: int
    session_key: str
    turn_id: str
    client_message_id: str
    user_message_id: str
    user_message_ids: tuple[str, ...]
    assistant_message_id: str | None


def open_db(path: Path) -> sqlite3.Connection:
    """Open the plugin-owned SQLite projection and its durable event ledger."""

    # 1. Open with WAL and full synchronous durability.
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    _ = conn.execute("PRAGMA journal_mode = WAL")
    _ = conn.execute("PRAGMA synchronous = FULL")
    _ = conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS proactive_feedback_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            session_key TEXT NOT NULL,
            user_message_id TEXT NOT NULL,
            assistant_message_id TEXT NOT NULL,
            proactive_message_id TEXT,
            feedback_type TEXT NOT NULL,
            confidence TEXT NOT NULL,
            pa_score REAL,
            pua_score REAL,
            lag_seconds INTEGER,
            candidate_count INTEGER NOT NULL,
            matched_by TEXT NOT NULL,
            reason TEXT NOT NULL,
            user_content_preview TEXT,
            assistant_content_preview TEXT,
            proactive_content_preview TEXT,
            UNIQUE(user_message_id, proactive_message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pfe_session_created
        ON proactive_feedback_events(session_key, created_at);

        CREATE INDEX IF NOT EXISTS idx_pfe_proactive
        ON proactive_feedback_events(proactive_message_id);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pfe_one_user_per_proactive
        ON proactive_feedback_events(proactive_message_id)
        WHERE proactive_message_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS proactive_feedback_input_inbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            session_key TEXT NOT NULL,
            turn_id TEXT NOT NULL DEFAULT '',
            client_message_id TEXT NOT NULL DEFAULT '',
            user_message_id TEXT NOT NULL,
            user_message_ids_json TEXT NOT NULL DEFAULT '[]',
            assistant_message_id TEXT,
            processed_at TEXT,
            UNIQUE(session_key, user_message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pfe_input_pending
        ON proactive_feedback_input_inbox(processed_at, id);

        CREATE TABLE IF NOT EXISTS proactive_feedback_session_catalog (
            session_key TEXT PRIMARY KEY,
            discovered_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS proactive_feedback_outbox (
            row_id INTEGER PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            published_at TEXT
        );

        CREATE TABLE IF NOT EXISTS proactive_feedback_published_cursor (
            name TEXT PRIMARY KEY,
            row_id INTEGER NOT NULL DEFAULT 0
        );

        INSERT OR IGNORE INTO proactive_feedback_published_cursor(name, row_id)
        VALUES ('proactive_feedback', 0);
        """
    )
    # 2. Preserve the v2 projection columns while adding the v3 ledger.
    _ensure_column(conn, "user_content_preview")
    _ensure_column(conn, "assistant_content_preview")
    _ensure_column(conn, "proactive_content_preview")
    _ensure_input_column(conn, "user_message_ids_json")
    conn.commit()
    return conn


def insert_feedback(conn: sqlite3.Connection, event: FeedbackEvent) -> int | None:
    """Atomically replace one feedback row and enqueue its typed event."""

    # 1. Reject a proactive message already owned by another user reply.
    if _feedback_owned_by_other(conn, event):
        return None

    # 2. Keep one row identity for duplicate committed Turns.
    existing_id = _existing_feedback_id(conn, event)
    if existing_id is not None:
        try:
            if not _feedback_is_published(conn, existing_id):
                _update_feedback_row(conn, event, existing_id)
                _upsert_feedback_outbox(conn, event, existing_id)
            conn.commit()
        except (sqlite3.Error, RuntimeError, TypeError, ValueError):
            conn.rollback()
            raise
        return existing_id

    # 3. Replace this user's previous projection and pending outbox row together.
    try:
        _remove_previous_feedback(conn, event.user_message_id)
        row_id = _insert_feedback_row(conn, event)
        _upsert_feedback_outbox(conn, event, row_id)
        conn.commit()
    except (sqlite3.Error, RuntimeError, TypeError, ValueError):
        conn.rollback()
        raise
    return row_id


def insert_feedback_input(
    conn: sqlite3.Connection,
    *,
    session_key: str,
    turn_id: str,
    client_message_id: str,
    user_message_id: str,
    assistant_message_id: str | None,
    user_message_ids: tuple[str, ...] | None = None,
) -> int:
    """Durably record one committed Turn identity without storing message text."""

    # 1. Validate the identity that the recovery reader will use.
    _required_input_text(session_key, "session_key")
    _required_input_text(user_message_id, "user_message_id")
    ordered_user_ids = (
        (user_message_id,) if user_message_ids is None else user_message_ids
    )
    _validate_user_message_ids(ordered_user_ids)
    if ordered_user_ids[-1] != user_message_id:
        raise ValueError("input inbox user_message_id 必须是 ordered IDs 的最后一项")
    _optional_input_text(turn_id, "turn_id")
    _optional_input_text(client_message_id, "client_message_id")
    if assistant_message_id is not None:
        _required_input_text(assistant_message_id, "assistant_message_id")

    # 2. Preserve one durable row for duplicate committed events.
    existing = conn.execute(
        """
        SELECT id, processed_at, user_message_ids_json
        FROM proactive_feedback_input_inbox
        WHERE session_key = ? AND user_message_id = ?
        LIMIT 1
        """,
        (session_key, user_message_id),
    ).fetchone()
    if existing is not None:
        if existing["processed_at"] is None and _decode_user_message_ids(
            existing["user_message_ids_json"], user_message_id
        ) != ordered_user_ids:
            _ = conn.execute(
                """
                UPDATE proactive_feedback_input_inbox
                SET user_message_ids_json = ?, assistant_message_id = ?
                WHERE id = ? AND processed_at IS NULL
                """,
                (
                    json.dumps(ordered_user_ids, ensure_ascii=False),
                    assistant_message_id,
                    int(existing["id"]),
                ),
            )
            conn.commit()
        return int(existing["id"])
    try:
        cursor = conn.execute(
            """
            INSERT INTO proactive_feedback_input_inbox(
                session_key, turn_id, client_message_id,
                user_message_id, user_message_ids_json, assistant_message_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                session_key,
                turn_id,
                client_message_id,
                user_message_id,
                json.dumps(ordered_user_ids, ensure_ascii=False),
                assistant_message_id,
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("feedback input insert failed")
        row_id = int(cursor.lastrowid)
        _ = conn.execute(
            """
            INSERT OR IGNORE INTO proactive_feedback_session_catalog(session_key)
            VALUES (?)
            """,
            (session_key,),
        )
        conn.commit()
        return row_id
    except (sqlite3.Error, RuntimeError, ValueError):
        conn.rollback()
        raise


def pending_feedback_inputs(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[FeedbackInputRecord]:
    """Read unprocessed committed Turn identities in durable row order."""

    # 1. Bound the recovery batch before reading the durable inbox.
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("input inbox limit 必须是正整数")
    rows = conn.execute(
        """
        SELECT id, session_key, turn_id, client_message_id,
               user_message_id, user_message_ids_json, assistant_message_id
        FROM proactive_feedback_input_inbox
        WHERE processed_at IS NULL
        ORDER BY id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_feedback_input_record(row) for row in rows]


def pending_feedback_input(
    conn: sqlite3.Connection,
    *,
    row_id: int,
) -> FeedbackInputRecord | None:
    """Read one pending committed Turn identity for the in-memory wake path."""

    if isinstance(row_id, bool) or not isinstance(row_id, int) or row_id < 1:
        raise ValueError("input inbox row_id 必须是正整数")
    row = conn.execute(
        """
        SELECT id, session_key, turn_id, client_message_id,
               user_message_id, user_message_ids_json, assistant_message_id
        FROM proactive_feedback_input_inbox
        WHERE id = ? AND processed_at IS NULL
        """,
        (row_id,),
    ).fetchone()
    return None if row is None else _feedback_input_record(row)


def mark_feedback_input_processed(
    conn: sqlite3.Connection,
    *,
    row_id: int,
) -> None:
    """Record successful handling of one durable committed Turn identity."""

    # 1. Validate the receipt identity before changing the inbox state.
    if isinstance(row_id, bool) or not isinstance(row_id, int) or row_id < 1:
        raise ValueError("input inbox row_id 必须是正整数")
    update = conn.execute(
        """
        UPDATE proactive_feedback_input_inbox
        SET processed_at = datetime('now')
        WHERE id = ? AND processed_at IS NULL
        """,
        (row_id,),
    )
    if update.rowcount == 0:
        existing = conn.execute(
            "SELECT id FROM proactive_feedback_input_inbox WHERE id = ?",
            (row_id,),
        ).fetchone()
        if existing is None:
            conn.rollback()
            raise RuntimeError("input inbox receipt 不匹配 pending row")
    conn.commit()


def feedback_session_keys(
    conn: sqlite3.Connection,
    *,
    limit: int = 64,
) -> tuple[str, ...]:
    """Read the bounded durable session-key catalog used by formal recovery."""

    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("session catalog limit 必须是正整数")
    rows = conn.execute(
        """
        SELECT session_key
        FROM proactive_feedback_session_catalog
        ORDER BY discovered_at ASC, session_key ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return tuple(str(row["session_key"]) for row in rows)


def feedback_identity_exists(
    conn: sqlite3.Connection,
    *,
    session_key: str,
    user_message_id: str,
) -> bool:
    """Check whether a feedback projection already owns one user identity."""

    row = conn.execute(
        """
        SELECT 1
        FROM proactive_feedback_events
        WHERE session_key = ? AND user_message_id = ?
        LIMIT 1
        """,
        (session_key, user_message_id),
    ).fetchone()
    return row is not None


def _feedback_owned_by_other(
    conn: sqlite3.Connection,
    event: FeedbackEvent,
) -> bool:
    if event.proactive_message_id is None:
        return False
    row = conn.execute(
        """
        SELECT id
        FROM proactive_feedback_events
        WHERE proactive_message_id = ? AND user_message_id <> ?
        LIMIT 1
        """,
        (event.proactive_message_id, event.user_message_id),
    ).fetchone()
    return row is not None


def _existing_feedback_id(
    conn: sqlite3.Connection,
    event: FeedbackEvent,
) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM proactive_feedback_events
        WHERE user_message_id = ? AND proactive_message_id IS ?
        LIMIT 1
        """,
        (event.user_message_id, event.proactive_message_id),
    ).fetchone()
    return None if row is None else int(row["id"])


def _feedback_is_published(conn: sqlite3.Connection, row_id: int) -> bool:
    row = conn.execute(
        """
        SELECT published_at
        FROM proactive_feedback_outbox
        WHERE row_id = ?
        """,
        (row_id,),
    ).fetchone()
    return row is not None and row["published_at"] is not None


def _update_feedback_row(
    conn: sqlite3.Connection,
    event: FeedbackEvent,
    row_id: int,
) -> None:
    _ = conn.execute(
        """
        UPDATE proactive_feedback_events
        SET session_key = ?, assistant_message_id = ?, feedback_type = ?,
            confidence = ?, pa_score = ?, pua_score = ?, lag_seconds = ?,
            candidate_count = ?, matched_by = ?, reason = ?,
            user_content_preview = ?, assistant_content_preview = ?,
            proactive_content_preview = ?
        WHERE id = ?
        """,
        (
            event.session_key,
            event.assistant_message_id,
            event.feedback_type,
            event.confidence,
            event.pa_score,
            event.pua_score,
            event.lag_seconds,
            event.candidate_count,
            event.matched_by,
            event.reason,
            event.user_content_preview,
            event.assistant_content_preview,
            event.proactive_content_preview,
            row_id,
        ),
    )


def _remove_previous_feedback(conn: sqlite3.Connection, user_message_id: str) -> None:
    pending = conn.execute(
        """
        SELECT events.id, outbox.row_id
        FROM proactive_feedback_events AS events
        LEFT JOIN proactive_feedback_outbox AS outbox
          ON outbox.row_id = events.id
        WHERE events.user_message_id = ?
          AND (outbox.row_id IS NULL OR outbox.published_at IS NULL)
        """,
        (user_message_id,),
    ).fetchall()
    for row in pending:
        _ = conn.execute(
            "DELETE FROM proactive_feedback_events WHERE id = ?",
            (int(row["id"]),),
        )
        if row["row_id"] is None:
            continue
        _ = conn.execute(
            "DELETE FROM proactive_feedback_outbox WHERE row_id = ?",
            (int(row["row_id"]),),
        )


def _insert_feedback_row(conn: sqlite3.Connection, event: FeedbackEvent) -> int:
    cursor = conn.execute(
        """
        INSERT INTO proactive_feedback_events (
            session_key, user_message_id, assistant_message_id,
            proactive_message_id, feedback_type, confidence, pa_score, pua_score,
            lag_seconds, candidate_count, matched_by, reason,
            user_content_preview, assistant_content_preview, proactive_content_preview
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.session_key,
            event.user_message_id,
            event.assistant_message_id,
            event.proactive_message_id,
            event.feedback_type,
            event.confidence,
            event.pa_score,
            event.pua_score,
            event.lag_seconds,
            event.candidate_count,
            event.matched_by,
            event.reason,
            event.user_content_preview,
            event.assistant_content_preview,
            event.proactive_content_preview,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("feedback insert failed")
    return int(cursor.lastrowid)


def _upsert_feedback_outbox(
    conn: sqlite3.Connection,
    event: FeedbackEvent,
    row_id: int,
) -> None:
    event_id = f"proactive_feedback:{row_id}"
    payload_json = json.dumps(
        _feedback_payload(event_id, event),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    outbox = conn.execute(
        """
        SELECT published_at
        FROM proactive_feedback_outbox
        WHERE row_id = ? AND event_id = ?
        """,
        (row_id, event_id),
    ).fetchone()
    if outbox is None:
        _ = conn.execute(
            """
            INSERT INTO proactive_feedback_outbox(row_id, event_id, payload_json)
            VALUES (?, ?, ?)
            """,
            (row_id, event_id, payload_json),
        )
    elif outbox["published_at"] is None:
        _ = conn.execute(
            """
            UPDATE proactive_feedback_outbox
            SET payload_json = ?
            WHERE row_id = ? AND event_id = ?
            """,
            (payload_json, row_id, event_id),
        )


def _feedback_payload(event_id: str, event: FeedbackEvent) -> dict[str, object]:
    return {
        "event_id": event_id,
        "session_key": event.session_key,
        "user_message_id": event.user_message_id,
        "assistant_message_id": event.assistant_message_id,
        "proactive_message_id": event.proactive_message_id,
        "feedback_type": event.feedback_type,
        "confidence": event.confidence,
        "pa_score": event.pa_score,
        "pua_score": event.pua_score,
        "lag_seconds": event.lag_seconds,
        "candidate_count": event.candidate_count,
        "matched_by": event.matched_by,
        "reason": event.reason,
        "user_content_preview": event.user_content_preview,
        "assistant_content_preview": event.assistant_content_preview,
        "proactive_content_preview": event.proactive_content_preview,
    }


def _feedback_input_record(row: sqlite3.Row) -> FeedbackInputRecord:
    user_message_id = str(row["user_message_id"])
    return FeedbackInputRecord(
        row_id=int(row["id"]),
        session_key=str(row["session_key"]),
        turn_id=str(row["turn_id"]),
        client_message_id=str(row["client_message_id"]),
        user_message_id=user_message_id,
        user_message_ids=_decode_user_message_ids(
            row["user_message_ids_json"], user_message_id
        ),
        assistant_message_id=(
            None
            if row["assistant_message_id"] is None
            else str(row["assistant_message_id"])
        ),
    )


def _required_input_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"input inbox {field} 必须是非空字符串")
    if value != value.strip():
        raise ValueError(f"input inbox {field} 不能有首尾空白")


def _optional_input_text(value: str, field: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"input inbox {field} 必须是字符串")
    if value != value.strip():
        raise ValueError(f"input inbox {field} 不能有首尾空白")


def _validate_user_message_ids(user_message_ids: tuple[str, ...]) -> None:
    if not user_message_ids:
        raise ValueError("input inbox user_message_ids 不能为空")
    if len(set(user_message_ids)) != len(user_message_ids):
        raise ValueError("input inbox user_message_ids 不能重复")
    for message_id in user_message_ids:
        _required_input_text(message_id, "user_message_id")


def _decode_user_message_ids(value: object, fallback: str) -> tuple[str, ...]:
    if isinstance(value, str) and value:
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            decoded = None
        if decoded and isinstance(decoded, list) and all(
            isinstance(item, str) and item for item in decoded
        ):
            ids = tuple(decoded)
            if len(set(ids)) == len(ids) and ids[-1] == fallback:
                return ids
    return (fallback,)


def _ensure_input_column(conn: sqlite3.Connection, name: str) -> None:
    columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA table_info(proactive_feedback_input_inbox)"
        )
    }
    if name not in columns:
        _ = conn.execute(
            "ALTER TABLE proactive_feedback_input_inbox "
            "ADD COLUMN user_message_ids_json TEXT NOT NULL DEFAULT '[]'"
        )


def pending_feedback_outbox(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[FeedbackOutboxRecord]:
    """Read unpublished payloads in durable row order."""

    # 1. Bound the recovery batch before reading the durable queue.
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("outbox limit 必须是正整数")
    rows = conn.execute(
        """
        SELECT row_id, event_id, payload_json
        FROM proactive_feedback_outbox
        WHERE published_at IS NULL
        ORDER BY row_id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        FeedbackOutboxRecord(
            row_id=int(row["row_id"]),
            event_id=str(row["event_id"]),
            payload_json=str(row["payload_json"]),
        )
        for row in rows
    ]


def mark_feedback_published(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    event_id: str,
) -> None:
    """Record one successful publication and advance the same-DB cursor."""

    # 1. Validate the receipt identity before changing the cursor.
    if isinstance(row_id, bool) or not isinstance(row_id, int) or row_id < 1:
        raise ValueError("outbox row_id 必须是正整数")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("outbox event_id 必须是非空字符串")
    # 2. Mark the exact outbox row and advance only its owner cursor.
    update = conn.execute(
        """
        UPDATE proactive_feedback_outbox
        SET published_at = datetime('now')
        WHERE row_id = ? AND event_id = ? AND published_at IS NULL
        """,
        (row_id, event_id),
    )
    if update.rowcount == 0:
        existing = conn.execute(
            """
            SELECT published_at
            FROM proactive_feedback_outbox
            WHERE row_id = ? AND event_id = ?
            """,
            (row_id, event_id),
        ).fetchone()
        if existing is None or existing["published_at"] is None:
            conn.rollback()
            raise RuntimeError("outbox receipt 不匹配 pending row")
    _ = conn.execute(
        """
        UPDATE proactive_feedback_published_cursor
        SET row_id = MAX(row_id, ?)
        WHERE name = 'proactive_feedback'
        """,
        (row_id,),
    )
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, name: str) -> None:
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(proactive_feedback_events)")
    }
    if name not in columns:
        _ = conn.execute(
            f"ALTER TABLE proactive_feedback_events ADD COLUMN {name} TEXT"
        )
