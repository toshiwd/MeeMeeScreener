from __future__ import annotations

from typing import Final, NotRequired, TypedDict

NORMALIZED_TRADE_HISTORY_FIELDS: Final[tuple[str, ...]] = (
    "broker",
    "trade_datetime",
    "code",
    "side",
    "quantity",
    "price",
    "fees",
    "taxes",
    "raw_ref",
)
NORMALIZED_TRADE_HISTORY_BUNDLE_FIELDS: Final[tuple[str, ...]] = ("source", "rows")


class NormalizedTradeHistoryRow(TypedDict):
    broker: str
    trade_datetime: str
    code: str
    side: str
    quantity: int
    price: float
    fees: NotRequired[float]
    taxes: NotRequired[float]
    raw_ref: NotRequired[str]


class NormalizedTradeHistoryBundle(TypedDict):
    source: str
    rows: list[NormalizedTradeHistoryRow]
