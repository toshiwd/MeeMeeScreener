from __future__ import annotations

from typing import Final
from typing import NotRequired, TypedDict

CONFIRMED_MARKET_BAR_FIELDS: Final[tuple[str, ...]] = (
    "code",
    "market_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "confirmation_state",
)
PROVISIONAL_INTRADAY_OVERLAY_FIELDS: Final[tuple[str, ...]] = (
    "code",
    "overlay_at",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "display_only",
    "freshness_state",
    "fetched_at",
)


class ConfirmedMarketBar(TypedDict):
    code: str
    market_date: int | str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    confirmation_state: str


class ProvisionalIntradayOverlay(TypedDict):
    code: str
    overlay_at: int | str
    open: NotRequired[float | None]
    high: NotRequired[float | None]
    low: NotRequired[float | None]
    close: NotRequired[float | None]
    volume: NotRequired[float | None]
    source: str
    display_only: bool
    freshness_state: NotRequired[str]
    fetched_at: NotRequired[int | str | None]
