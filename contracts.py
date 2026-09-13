"""Proactive Feedback 使用的公开能力结构合同。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, Protocol

from agent.plugin_composition import ServiceKey
from agent.plugin_composition.messages import MessageCatalog
from agent.plugin_contracts import Message


class Turn(Protocol):
    status: Literal["open", "complete", "quiet", "abandoned"]
    ending_message_id: str | None
    message_ids: tuple[str, ...]


class TurnProjection(Protocol):
    def project(self, messages: Sequence[Message], source: str) -> tuple[Turn, ...]: ...


TURN_PROJECTION = ServiceKey[TurnProjection]("turn.projection.v1")


__all__ = ["MessageCatalog", "TURN_PROJECTION", "Turn", "TurnProjection"]
