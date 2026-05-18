from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class Bar:
    time: int
    open: float
    high: float
    low: float
    close: float


def _to_bars(rows: Iterable[tuple]) -> list[Bar]:
    bars: list[Bar] = []
    for row in rows:
        if len(row) < 5:
            continue
        time, open_, high, low, close = row[:5]
        if time is None or high is None or low is None or close is None:
            continue
        bars.append(
            Bar(
                time=int(time),
                open=float(open_),
                high=float(high),
                low=float(low),
                close=float(close)
            )
        )
    bars.sort(key=lambda item: item.time)
    return bars


def detect_boxes(
    rows: Iterable[tuple],
    *,
    min_bars: int = 3,
    max_bars: int = 24,
    max_range_pct: float = 0.18,
    min_range_pct: float = 0.12,
    edge_touch_pct: float = 0.18,
    min_edge_touches_per_side: int = 2,
    max_mid_slope_ratio: float = 0.6,
    rescue_bars: int = 2,
    rescue_hits_per_side: int = 1,
    breakout_buffer: float = 0.005,
    range_basis: str = "high_low",
) -> list[dict]:
    bars = _to_bars(rows)
    if len(bars) < min_bars:
        return []

    basis = "body" if range_basis == "body" else "high_low"

    def bar_upper(bar: Bar) -> float:
        return max(bar.open, bar.close) if basis == "body" else bar.high

    def bar_lower(bar: Bar) -> float:
        return min(bar.open, bar.close) if basis == "body" else bar.low

    def range_pct_for(upper: float, lower: float) -> float:
        return (upper - lower) / max(abs(lower), 1e-9)

    def mid_slope_ratio(values: list[float], height: float) -> float:
        count = len(values)
        if count < 2 or height <= 0:
            return 0.0
        x_mean = (count - 1) / 2
        y_mean = sum(values) / count
        denominator = sum((index - x_mean) ** 2 for index in range(count))
        if denominator <= 0:
            return 0.0
        slope = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(values)) / denominator
        return abs(slope) * (count - 1) / height

    def is_box_quality(candidate: list[Bar], upper: float, lower: float) -> bool:
        height = upper - lower
        if height <= 0:
            return False
        range_pct = range_pct_for(upper, lower)
        if range_pct < min_range_pct or range_pct > max_range_pct:
            return False

        edge_margin = height * edge_touch_pct
        upper_touches = 0
        lower_touches = 0
        clean_upper_touches = 0
        clean_lower_touches = 0
        mids: list[float] = []
        for bar in candidate:
            high = bar_upper(bar)
            low = bar_lower(bar)
            touches_upper = high >= upper - edge_margin
            touches_lower = low <= lower + edge_margin
            if touches_upper:
                upper_touches += 1
            if touches_lower:
                lower_touches += 1
            if touches_upper and not touches_lower:
                clean_upper_touches += 1
            if touches_lower and not touches_upper:
                clean_lower_touches += 1
            mids.append((high + low) / 2)

        if upper_touches < min_edge_touches_per_side or lower_touches < min_edge_touches_per_side:
            return False
        if clean_upper_touches < 1 or clean_lower_touches < 1:
            return False
        if range_pct >= max_range_pct * 0.95:
            if clean_upper_touches < min_edge_touches_per_side or clean_lower_touches < min_edge_touches_per_side:
                return False
        return mid_slope_ratio(mids, height) <= max_mid_slope_ratio

    boxes: list[dict] = []
    index = 0
    last = len(bars)

    while index <= last - min_bars:
        start = index
        best_end = None
        best_upper = None
        best_lower = None
        upper = float("-inf")
        lower = float("inf")

        for cursor in range(start, min(last, start + max_bars)):
            bar = bars[cursor]
            upper = max(upper, bar_upper(bar))
            lower = min(lower, bar_lower(bar))
            if cursor - start + 1 < min_bars:
                continue
            if range_pct_for(upper, lower) > max_range_pct:
                break
            candidate = bars[start : cursor + 1]
            if is_box_quality(candidate, upper, lower):
                best_end = cursor
                best_upper = upper
                best_lower = lower

        if best_end is None or best_upper is None or best_lower is None:
            index += 1
            continue

        breakout = None
        for cursor in range(best_end + 1, last):
            close = bars[cursor].close
            if close >= best_upper * (1 + breakout_buffer):
                breakout = "up"
                break
            if close <= best_lower * (1 - breakout_buffer):
                breakout = "down"
                break

        boxes.append(
            {
                "startIndex": start,
                "endIndex": best_end,
                "startTime": bars[start].time,
                "endTime": bars[best_end].time,
                "lower": float(best_lower),
                "upper": float(best_upper),
                "breakout": breakout
            }
        )
        index = best_end + 1

    return boxes
