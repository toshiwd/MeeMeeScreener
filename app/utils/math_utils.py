from __future__ import annotations

import math
from typing import Any


def to_float_or_none(value: Any) -> float | None:
    """Convert value to float, returning None for non-numeric or non-finite values."""
    if not isinstance(value, (int, float)):
        return None
    fv = float(value)
    return fv if math.isfinite(fv) else None


def _pct_change(latest: float | None, prev: float | None) -> float | None:
    if latest is None or prev is None:
        return None
    if prev == 0:
        return None
    return (latest - prev) / prev * 100


def _build_ma_series(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    result: list[float | None] = []
    total = 0.0
    for index, value in enumerate(values):
        total += value
        if index >= period:
            total -= values[index - period]
        if index >= period - 1:
            result.append(total / period)
        else:
            result.append(None)
    return result


def _compute_atr(highs: list[float], lows: list[float], closes: list[float], period: int) -> float | None:
    if len(closes) < 2 or len(closes) != len(highs) or len(closes) != len(lows):
        return None
    trs: list[float] = []
    prev_close = closes[0]
    for high, low, close in zip(highs, lows, closes):
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < period:
        return None
    window = trs[-period:]
    return sum(window) / period


def _calc_slope(values: list[float | None], lookback: int) -> float | None:
    if lookback <= 0 or len(values) <= lookback:
        return None
    current = values[-1]
    past = values[-1 - lookback]
    if current is None or past is None:
        return None
    return float(current) - float(past)
