from __future__ import annotations

import math
import re
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime
from collections.abc import Mapping, Sequence
from typing import Protocol

from session.message import ContentPart, Input, Message, Output


@dataclass(frozen=True)
class MessageRow:
    id: str
    seq: int
    role: str
    content: str
    source: str
    proactive: bool
    reply_to_id: str | None
    ts: str


@dataclass(frozen=True)
class QuoteParts:
    quoted_text: str | None
    current_text: str


@dataclass(frozen=True)
class FeedbackScore:
    proactive: MessageRow
    pa_score: float
    pua_score: float
    matched_by: str
    feedback_type: str
    confidence: str
    reason: str
    candidate_count: int
    lag_seconds: int | None


class EmbedBatch(Protocol):
    async def __call__(self, texts: list[str]) -> list[list[float]]: ...


def message_rows_from_messages(messages: Sequence[Message]) -> list[MessageRow]:
    """将不可变 Message 前缀转换成反馈评分所需的窄行。"""

    return [_message_row(message) for message in messages]


def clean_text(text: str, max_chars: int = 1200) -> str:
    return re.sub(r"\s+", " ", text).strip()[:max_chars]


def normalize_quote_text(text: str, max_chars: int = 1200) -> str:
    cleaned = clean_text(text, max_chars=max_chars).lower()
    cleaned = re.sub(r"[*_`#>\[\]()]", "", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def parse_quote_parts(content: str) -> QuoteParts:
    marker = "【你当前新消息】"
    if marker not in content:
        return QuoteParts(quoted_text=None, current_text=content.strip())

    before, after = content.split(marker, 1)
    quoted = before
    quote_prefix = "被回复消息"
    if quote_prefix in quoted:
        quoted = quoted.split(quote_prefix, 1)[1]
    if "：" in quoted:
        quoted = quoted.split("：", 1)[1]
    return QuoteParts(
        quoted_text=clean_text(quoted, max_chars=300) or None,
        current_text=after.strip(),
    )


def _message_row(message: Message) -> MessageRow:
    body = message.body
    role = "user" if isinstance(body, Input) else "assistant" if isinstance(body, Output) else "other"
    parts = body.parts if isinstance(body, (Input, Output)) else ()
    content = "\n\n".join(
        part.value for part in parts
        if isinstance(part, ContentPart) and part.kind == "text" and isinstance(part.value, str)
    )
    reply_ids = [
        part.value for part in parts
        if isinstance(part, ContentPart) and part.kind == "reply_ref" and isinstance(part.value, str)
    ]
    if len(reply_ids) > 1:
        raise ValueError("一个 Input 只能引用一条被回复消息")
    return MessageRow(
        id=message.message_id,
        seq=message.seq,
        role=role,
        content=content,
        source=message.source,
        proactive=_is_proactive_message(message, parts),
        reply_to_id=reply_ids[0] if reply_ids else None,
        ts=message.recorded_at.isoformat(),
    )


def _is_proactive_message(message: Message, parts: Sequence[object]) -> bool:
    """识别新来源输出，并保留迁移消息中明确记录的旧 proactive 事实。"""

    if not isinstance(message.body, Output) or message.body.finish != "complete":
        return False
    for part in parts:
        if not isinstance(part, ContentPart) or part.kind != "history.provenance":
            continue
        value = part.value
        if not isinstance(value, Mapping) or value.get("schema") != "sessions.messages.v0":
            continue
        raw = value.get("extra")
        if isinstance(raw, str):
            import json

            decoded = json.loads(raw)
            if isinstance(decoded, dict) and decoded.get("proactive") is True:
                return True
    return message.source not in {"conversation", "legacy-unattributed"}


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def classify_pua(score: float) -> tuple[str, str, str]:
    if score >= 0.62:
        return "topic_follow", "high", "pua_high"
    if score >= 0.54:
        return "topic_follow", "medium", "pua_medium"
    return "no_topic_follow", "low", "pua_low"


class CandidateIndex:
    """为一个只读 Message 前缀索引用户边界与主动输出。"""

    def __init__(self, rows: Sequence[MessageRow]) -> None:
        ordered = sorted(rows, key=lambda row: row.seq)
        self._users = [row.seq for row in ordered if row.role == "user"]
        self._assistants = [row for row in ordered if row.role == "assistant" and row.content]
        self._assistant_seqs = [row.seq for row in self._assistants]
        self._proactive = [row for row in self._assistants if row.proactive]
        self._proactive_seqs = [row.seq for row in self._proactive]

    def recent(self, *, before_seq: int, limit: int) -> list[MessageRow]:
        """保留原有最近 limit × 4 条非空助手消息的候选窗口。"""
        end = bisect_left(self._assistant_seqs, before_seq)
        recent = reversed(self._assistants[max(0, end - limit * 4):end])
        return [row for row in recent if row.proactive][:limit]

    def since_previous_user(self, *, before_seq: int, limit: int) -> list[MessageRow]:
        """只选上一个用户输入之后、当前输入之前的主动输出。"""
        previous = bisect_left(self._users, before_seq)
        after_seq = self._users[previous - 1] if previous else -1
        start = bisect_right(self._proactive_seqs, after_seq)
        end = bisect_left(self._proactive_seqs, before_seq)
        return list(reversed(self._proactive[max(start, end - limit):end]))


async def score_followup(
    *,
    embed_batch: EmbedBatch,
    user: MessageRow,
    assistant: MessageRow,
    candidates: list[MessageRow],
    allow_pua: bool = True,
) -> FeedbackScore | None:
    if not candidates:
        return None

    quote = parse_quote_parts(user.content)
    quoted_match = next(
        (candidate for candidate in candidates if candidate.id == user.reply_to_id),
        None,
    )
    if quoted_match is None:
        quoted_match = _match_quoted(candidates, quote.quoted_text)
    if quoted_match is not None:
        return FeedbackScore(
            proactive=quoted_match,
            pa_score=1.0,
            pua_score=1.0,
            matched_by="explicit_quote",
            feedback_type="explicit_quote",
            confidence="gold",
            reason="explicit_quote",
            candidate_count=len(candidates),
            lag_seconds=_lag_seconds(quoted_match.ts, user.ts),
        )
    if not allow_pua:
        return None

    target = candidates[0]
    p_text = clean_text(target.content)
    a_text = clean_text(assistant.content)
    ua_text = clean_text(f"{quote.current_text}\n\n{assistant.content}")
    vectors = await embed_batch([p_text, a_text, ua_text])
    pa_score = cosine(vectors[0], vectors[1])
    pua_score = cosine(vectors[0], vectors[2])
    feedback_type, confidence, reason = classify_pua(pua_score)
    return FeedbackScore(
        proactive=target,
        pa_score=pa_score,
        pua_score=pua_score,
        matched_by="recent_pua",
        feedback_type=feedback_type,
        confidence=confidence,
        reason=reason,
        candidate_count=len(candidates),
        lag_seconds=_lag_seconds(target.ts, user.ts),
    )

def _match_quoted(candidates: list[MessageRow], quoted_text: str | None) -> MessageRow | None:
    if not quoted_text:
        return None
    needle = normalize_quote_text(quoted_text, max_chars=220)
    if len(needle) < 12:
        return None
    for candidate in candidates:
        haystack = normalize_quote_text(candidate.content, max_chars=3000)
        if needle in haystack:
            return candidate
    return None


def _lag_seconds(start: str, end: str) -> int | None:
    try:
        return int(datetime.fromisoformat(end).timestamp() - datetime.fromisoformat(start).timestamp())
    except ValueError:
        return None
