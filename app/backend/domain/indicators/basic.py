from typing import List, Optional

def pct_change(latest: Optional[float], prev: Optional[float]) -> Optional[float]:
    if latest is None or prev is None:
        return None
    if prev == 0:
        return None
    return (latest - prev) / prev * 100

def build_ma_series(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 0:
        return [None for _ in values]
    result: List[Optional[float]] = []
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

def compute_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
    if len(closes) < 2 or len(closes) != len(highs) or len(closes) != len(lows):
        return None
    trs: List[float] = []
    prev_close = closes[0]
    for high, low, close in zip(highs, lows, closes):
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < period:
        return None
    window = trs[-period:]
    return sum(window) / period

def calc_slope(values: List[Optional[float]], lookback: int) -> Optional[float]:
    if lookback <= 0 or len(values) <= lookback:
        return None
    current = values[-1]
    past = values[-1 - lookback]
    if current is None or past is None:
        return None
    return float(current) - float(past)
