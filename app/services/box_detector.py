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
                close=float(close),
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

    boxes: list[dict] = []
    index = 0
    last = len(bars)

    while index <= last - min_bars:
        start = index
        end = start + min_bars - 1
        upper = max(bar_upper(bar) for bar in bars[start : end + 1])
        lower = min(bar_lower(bar) for bar in bars[start : end + 1])
        base = max(abs(lower), 1e-9)
        if (upper - lower) / base > max_range_pct:
            index += 1
            continue

        rescue_up = 0
        rescue_down = 0

        for cursor in range(end + 1, min(last, start + max_bars)):
            bar = bars[cursor]
            bar_hi = bar_upper(bar)
            bar_lo = bar_lower(bar)
            out_up = bar_hi > upper
            out_down = bar_lo < lower
            if out_up or out_down:
                if cursor - start <= rescue_bars:
                    if out_up and rescue_up < rescue_hits_per_side:
                        rescue_up += 1
                        upper = max(upper, bar_hi)
                    elif out_down and rescue_down < rescue_hits_per_side:
                        rescue_down += 1
                        lower = min(lower, bar_lo)
                    else:
                        break
                else:
                    break
            else:
                upper = max(upper, bar_hi)
                lower = min(lower, bar_lo)

            base = max(abs(lower), 1e-9)
            if (upper - lower) / base > max_range_pct:
                break
            end = cursor

        if end - start + 1 < min_bars:
            index += 1
            continue

        breakout = None
        for cursor in range(end + 1, last):
            close = bars[cursor].close
            if close >= upper * (1 + breakout_buffer):
                breakout = "up"
                break
            if close <= lower * (1 - breakout_buffer):
                breakout = "down"
                break

        boxes.append(
            {
                "startIndex": start,
                "endIndex": end,
                "startTime": bars[start].time,
                "endTime": bars[end].time,
                "lower": float(lower),
                "upper": float(upper),
                "breakout": breakout,
            }
        )
        index = end + 1

    return boxes
