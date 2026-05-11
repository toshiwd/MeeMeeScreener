from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict


HoldingReviewAction = Literal[
    "buy_more",
    "hold",
    "reduce",
    "exit",
    "increase_hedge",
    "maintain_hedge",
]


class HoldingReviewBundle(TypedDict):
    code: str
    as_of: dict[str, Any]
    position: dict[str, Any]
    entry_reason_snapshot: dict[str, Any]
    current_hold_reason: dict[str, Any]
    confirmed_bar: dict[str, Any] | None
    provisional_bar: dict[str, Any] | None
    event_gate: dict[str, Any]
    decision: dict[str, Any]
    data_quality: dict[str, Any]


class HoldingReviewResponse(TypedDict):
    schema_version: str
    items: list[HoldingReviewBundle]
    warnings: NotRequired[list[str]]
