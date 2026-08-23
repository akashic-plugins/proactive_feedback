from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from agent.plugin_composition import ServiceKey


_MAX_PAGE_SIZE = 100
_FEEDBACK_TYPES = frozenset(
    {"explicit_quote", "topic_follow", "no_topic_follow", "unscored"}
)
_CONFIDENCE = frozenset({"gold", "high", "medium", "low"})


def _normalize_sql(sql: str) -> str:
    return "".join(sql.lower().split())


_CURRENT_EVENTS_SQL = """
CREATE TABLE proactive_feedback_events (
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
)
"""
_LEGACY_EVENTS_SQL = """
CREATE TABLE proactive_feedback_events (
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
    UNIQUE(user_message_id, proactive_message_id)
)
"""
_ALLOWED_EVENTS_SQL = frozenset(
    {
        _normalize_sql(_CURRENT_EVENTS_SQL),
        _normalize_sql(_LEGACY_EVENTS_SQL),
    }
)
_REQUIRED_COLUMNS = frozenset(
    {
        "id",
        "session_key",
        "user_message_id",
        "assistant_message_id",
        "proactive_message_id",
        "feedback_type",
        "confidence",
        "pa_score",
        "pua_score",
        "lag_seconds",
        "candidate_count",
        "matched_by",
        "reason",
        "user_content_preview",
        "assistant_content_preview",
        "proactive_content_preview",
    }
)


@dataclass(frozen=True, slots=True)
class FeedbackHistoryRecord:
    cursor: int
    event_id: str
    payload_hash: str
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
    user_content_preview: str | None
    assistant_content_preview: str | None
    proactive_content_preview: str | None


@dataclass(frozen=True, slots=True)
class FeedbackHistoryPage:
    after_cursor: int
    records: tuple[FeedbackHistoryRecord, ...]


class FeedbackHistory(Protocol):
    def page(self, *, after_cursor: int, max_items: int) -> FeedbackHistoryPage: ...


PROACTIVE_FEEDBACK_HISTORY = ServiceKey[FeedbackHistory](
    "proactive-feedback.history.v1"
)


class SqliteFeedbackHistory:
    """Read immutable accepted feedback from the plugin-owned SQLite history."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def page(self, *, after_cursor: int, max_items: int) -> FeedbackHistoryPage:
        """Return one stable cursor-ordered page without creating or migrating data."""

        # 1. Validate the cross-plugin request before opening plugin data.
        if isinstance(after_cursor, bool) or not isinstance(after_cursor, int):
            raise TypeError("after_cursor 必须是整数")
        if after_cursor < 0:
            raise ValueError("after_cursor 不得小于零")
        if isinstance(max_items, bool) or not isinstance(max_items, int):
            raise TypeError("max_items 必须是整数")
        if max_items < 1 or max_items > _MAX_PAGE_SIZE:
            raise ValueError(f"max_items 必须在 1..{_MAX_PAGE_SIZE} 之间")
        if not self._db_path.exists():
            return FeedbackHistoryPage(after_cursor=after_cursor, records=())

        # 2. Existing bytes must satisfy the exact read schema; no DDL is allowed.
        connection = sqlite3.connect(
            self._db_path.resolve().as_uri() + "?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            columns = _validate_schema(connection)
            previews = ", ".join(
                column if column in columns else f"NULL AS {column}"
                for column in (
                    "user_content_preview",
                    "assistant_content_preview",
                    "proactive_content_preview",
                )
            )
            rows = connection.execute(
                f"""
                SELECT id, session_key, user_message_id, assistant_message_id,
                       proactive_message_id, feedback_type, confidence,
                       pa_score, pua_score, lag_seconds, candidate_count,
                       matched_by, reason, {previews}
                FROM proactive_feedback_events
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (after_cursor, max_items),
            ).fetchall()
        finally:
            connection.close()

        # 3. Decode every row at the trust boundary and freeze its content hash.
        records = tuple(_record_from_row(row) for row in rows)
        return FeedbackHistoryPage(after_cursor=after_cursor, records=records)


def accepted_payload_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_schema(connection: sqlite3.Connection) -> frozenset[str]:
    tables = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table'"
        )
    }
    if "proactive_feedback_events" not in tables:
        raise RuntimeError("proactive_feedback history 缺少 events 表")
    if _normalize_sql(tables["proactive_feedback_events"]) not in _ALLOWED_EVENTS_SQL:
        raise RuntimeError("proactive_feedback history events table schema 不匹配")
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(proactive_feedback_events)")
    }
    required = _REQUIRED_COLUMNS.difference(
        {
            "user_content_preview",
            "assistant_content_preview",
            "proactive_content_preview",
        }
    )
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(
            "proactive_feedback history schema 缺少列: " + ", ".join(missing)
        )
    checks = tuple(tuple(row) for row in connection.execute("PRAGMA quick_check"))
    if checks != (("ok",),):
        raise RuntimeError("proactive_feedback history SQLite quick_check failed")
    return frozenset(columns)


def _record_from_row(row: sqlite3.Row) -> FeedbackHistoryRecord:
    cursor = _positive_int(row["id"], "id")
    payload = {
        "session_key": _required_text(row["session_key"], "session_key"),
        "user_message_id": _required_text(
            row["user_message_id"], "user_message_id"
        ),
        "assistant_message_id": _required_text(
            row["assistant_message_id"], "assistant_message_id"
        ),
        "proactive_message_id": _optional_text(
            row["proactive_message_id"], "proactive_message_id"
        ),
        "feedback_type": _enum_text(
            row["feedback_type"], "feedback_type", _FEEDBACK_TYPES
        ),
        "confidence": _enum_text(row["confidence"], "confidence", _CONFIDENCE),
        "pa_score": _optional_score(row["pa_score"], "pa_score"),
        "pua_score": _optional_score(row["pua_score"], "pua_score"),
        "lag_seconds": _optional_nonnegative_int(
            row["lag_seconds"], "lag_seconds"
        ),
        "candidate_count": _nonnegative_int(
            row["candidate_count"], "candidate_count"
        ),
        "matched_by": _required_text(row["matched_by"], "matched_by"),
        "reason": _required_text(row["reason"], "reason"),
        "user_content_preview": _optional_text(
            row["user_content_preview"], "user_content_preview"
        ),
        "assistant_content_preview": _optional_text(
            row["assistant_content_preview"], "assistant_content_preview"
        ),
        "proactive_content_preview": _optional_text(
            row["proactive_content_preview"], "proactive_content_preview"
        ),
    }
    return FeedbackHistoryRecord(
        cursor=cursor,
        event_id=f"proactive_feedback:{cursor}",
        payload_hash=accepted_payload_hash(payload),
        **payload,
    )


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"history {field} 必须是非空字符串")
    return value


def _optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"history {field} 必须是字符串或 null")
    return value


def _enum_text(value: object, field: str, choices: frozenset[str]) -> str:
    text = _required_text(value, field)
    if text not in choices:
        raise ValueError(f"history {field} 不支持: {text}")
    return text


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"history {field} 必须是正整数")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"history {field} 必须是非负整数")
    return value


def _optional_nonnegative_int(value: object, field: str) -> int | None:
    return None if value is None else _nonnegative_int(value, field)


def _optional_score(value: object, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"history {field} 必须是数字或 null")
    score = float(value)
    if not math.isfinite(score) or score < -1.0 or score > 1.0:
        raise ValueError(f"history {field} 必须在 -1..1 之间")
    return score
