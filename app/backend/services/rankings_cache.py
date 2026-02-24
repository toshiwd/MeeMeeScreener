from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import logging
from threading import Lock
from typing import Any, Literal

import duckdb

from app.core.config import config as core_config
from app.backend.domain.screening.metrics import _calc_liquidity_20d
from app.backend.services.ml_config import load_ml_config
from app.backend.services.ml_service import select_top_n_ml

RankTimeframe = Literal["D", "W", "M"]
RankWhich = Literal["latest", "prev"]
RankDir = Literal["up", "down"]
RankMode = Literal["rule", "ml", "hybrid", "turn"]

_CACHE: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
_LAST_UPDATED: datetime | None = None
_LOCK = Lock()
logger = logging.getLogger(__name__)

_DAILY_LIMIT = 1260
_MONTHLY_LIMIT = 60
_ENTRY_MIN_EV_NET_UP = 0.003
_ENTRY_MAX_EV_NET_DOWN = 0.005
_ENTRY_MAX_DIST_MA20 = 0.12
_ENTRY_MIN_PROB_DOWN_STRICT = 0.56
_ENTRY_MIN_RULE_SIGNAL_DOWN = 0.002
_ENTRY_MAX_COUNTER_MOVE_DOWN = 0.01
# Current model outputs are concentrated around 0.47-0.60 for p_up_5.
# A hard 0.70 gate eliminates all symbols.
_ENTRY_MIN_PROB_UP_5D = 0.58
_ENTRY_PROB_CURVE_EPS = 0.02
_ENTRY_BONUS_CANDLE_PATTERN = 0.01
_ENTRY_BONUS_BOX_BOTTOM = 0.03
_ENTRY_BONUS_MTF_SYNERGY = 0.02
_ENTRY_BONUS_STRICT_STACK = 0.02
_ENTRY_PENALTY_60V_STRONG = 0.01
_MONTHLY_ABS_GATE_DEFAULT = 0.30
_MONTHLY_SIDE_GATE_DEFAULT = 0.30
_MONTHLY_ABS_GATE_MIN = 0.15
_MONTHLY_SIDE_GATE_MIN = 0.10
_MONTHLY_GATE_MIN_CANDIDATES = 5
_MONTHLY_ABS_RELAX_STEPS: tuple[float, ...] = (0.35, 0.32, 0.30, 0.28, 0.25, 0.22, 0.20, 0.18, 0.15)
_MONTHLY_SIDE_RELAX_STEPS: tuple[float, ...] = (0.30, 0.25, 0.22, 0.20, 0.18, 0.16, 0.14, 0.12, 0.10)
_MONTHLY_REGIME_BONUS = 0.04
_MONTHLY_RANGE_PENALTY = 0.03
_MONTHLY_TARGET20_GATE_MIN_UP = 0.11
_MONTHLY_TARGET20_GATE_MIN_DOWN = 0.08


def _parse_date_value(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw >= 1_000_000_000:
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    raw_str = str(raw).zfill(8)
    if len(raw_str) == 8:
        try:
            year = int(raw_str[:4])
            month = int(raw_str[4:6])
            day = int(raw_str[6:8])
            return datetime(year, month, day)
        except ValueError:
            return None
    if len(raw_str) == 6:
        try:
            year = int(raw_str[:4])
            month = int(raw_str[4:6])
            return datetime(year, month, 1)
        except ValueError:
            return None
    return None


def _format_date(value: datetime | None) -> str | None:
    if not value:
        return None
    return value.date().isoformat()


def _iso_date_to_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        raw = value.replace("-", "")
        if len(raw) != 8 or not raw.isdigit():
            return None
        return int(raw)
    except Exception:
        return None


def _coerce_as_of_int(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        if 19_000_101 <= value <= 21_001_231:
            return int(value)
        if value >= 1_000_000_000:
            try:
                return int(datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return None
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return _coerce_as_of_int(int(text))
    return _iso_date_to_int(text)


def _as_of_int_to_utc_epoch(value: int) -> int:
    year = value // 10_000
    month = (value // 100) % 100
    day = value % 100
    dt = datetime(year, month, day, tzinfo=timezone.utc)
    return int(dt.timestamp())


def _as_of_month_int_to_utc_epoch(value: int) -> int:
    year = value // 10_000
    month = (value // 100) % 100
    day = value % 100
    dt = datetime(year, month, day, tzinfo=timezone.utc)
    return int(dt.timestamp())


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _to_month_start_int(value: int | None) -> int | None:
    dt = _parse_date_value(value)
    if dt is None:
        return None
    return int(dt.year * 10_000 + dt.month * 100 + 1)


def _build_weekly_bars(daily_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_week = None
    for row in daily_rows:
        if len(row) < 5:
            continue
        date_value, open_, high, low, close = row[:5]
        if open_ is None or high is None or low is None or close is None:
            continue
        dt = _parse_date_value(date_value)
        if not dt:
            continue
        week_start = dt.date() - timedelta(days=dt.weekday())
        if current_week != week_start:
            items.append(
                {
                    "week_start": week_start,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "last_date": dt,
                }
            )
            current_week = week_start
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
            current["last_date"] = dt
    return items


def _drop_incomplete_weekly(weekly: list[dict], last_daily: datetime | None) -> list[dict]:
    if not weekly or not last_daily:
        return weekly
    last_week_start = last_daily.date() - timedelta(days=last_daily.weekday())
    if weekly[-1]["week_start"] == last_week_start and last_daily.weekday() < 4:
        return weekly[:-1]
    return weekly


def _compute_change(
    closes: list[float], dates: list[datetime], which: RankWhich
) -> tuple[float | None, float | None, str | None, float | None, float | None]:
    if which == "latest":
        target_idx = -1
        prev_idx = -2
    else:
        target_idx = -2
        prev_idx = -3
    if len(closes) < abs(prev_idx):
        return None, None, None, None, None
    close = closes[target_idx]
    prev_close = closes[prev_idx]
    if prev_close is None or prev_close == 0:
        return None, None, _format_date(dates[target_idx]), close, prev_close
    change_abs = close - prev_close
    change_pct = change_abs / prev_close
    return change_pct, change_abs, _format_date(dates[target_idx]), close, prev_close


def _clip01(value: float | None) -> float | None:
    if value is None:
        return None
    if not math.isfinite(value):
        return None
    return max(0.0, min(1.0, float(value)))


def _safe_div(num: float, den: float) -> float | None:
    if not math.isfinite(num) or not math.isfinite(den):
        return None
    if abs(den) <= 1e-12:
        return None
    return float(num / den)


def _rolling_sma(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    out: list[float | None] = [None for _ in values]
    running = 0.0
    for idx, value in enumerate(values):
        running += float(value)
        if idx >= period:
            running -= float(values[idx - period])
        if idx >= period - 1:
            out[idx] = float(running / period)
    return out


def _calc_60v_signals(daily_rows: list[tuple]) -> dict[str, float]:
    closes: list[float] = []
    for row in daily_rows:
        if len(row) < 5:
            continue
        close = _finite_float(row[4])
        if close is None:
            continue
        closes.append(float(close))

    default = {
        "reclaim60": 0.0,
        "v60Core": 0.0,
        "v60Strong": 0.0,
    }
    if len(closes) < 62:
        return default

    ma20 = _rolling_sma(closes, 20)
    ma60 = _rolling_sma(closes, 60)
    last_idx = len(closes) - 1
    close_now = closes[last_idx]
    ma20_now = ma20[last_idx]
    ma60_now = ma60[last_idx]
    if ma20_now is None or ma60_now is None:
        return default

    ma20_prev = ma20[last_idx - 1] if last_idx - 1 >= 0 else None
    ma60_prev = ma60[last_idx - 1] if last_idx - 1 >= 0 else None
    ma20_slope = (
        float(ma20_now - ma20_prev)
        if ma20_prev is not None and math.isfinite(ma20_prev) and math.isfinite(ma20_now)
        else None
    )
    ma60_slope = (
        float(ma60_now - ma60_prev)
        if ma60_prev is not None and math.isfinite(ma60_prev) and math.isfinite(ma60_now)
        else None
    )
    dist20 = _safe_div(close_now - ma20_now, ma20_now)
    dist60 = _safe_div(close_now - ma60_now, ma60_now)

    recent_below60 = False
    for idx in range(max(0, last_idx - 15), last_idx):
        ma60_i = ma60[idx]
        if ma60_i is None:
            continue
        if closes[idx] < ma60_i:
            recent_below60 = True
            break

    reclaim60 = bool(ma20_now > ma60_now and close_now >= ma60_now and recent_below60)
    v60_core = bool(reclaim60 and dist20 is not None and float(dist20) >= -0.01)
    v60_strong = bool(
        v60_core
        and dist20 is not None
        and dist60 is not None
        and float(dist20) >= 0.02
        and float(dist60) >= 0.04
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) > 0
        and float(ma60_slope) > 0
    )
    return {
        "reclaim60": 1.0 if reclaim60 else 0.0,
        "v60Core": 1.0 if v60_core else 0.0,
        "v60Strong": 1.0 if v60_strong else 0.0,
    }


def _calc_triplet_candle_signals(daily_rows: list[tuple]) -> dict[str, float | None]:
    bars: list[dict[str, float]] = []
    for row in daily_rows:
        if len(row) < 5:
            continue
        o = _finite_float(row[1])
        h = _finite_float(row[2])
        l = _finite_float(row[3])
        c = _finite_float(row[4])
        if o is None or h is None or l is None or c is None:
            continue
        if h < l:
            continue
        span = h - l
        body = c - o
        body_ratio = abs(body) / span if span > 1e-12 else 0.0
        upper_ratio = (h - max(o, c)) / span if span > 1e-12 else 0.0
        lower_ratio = (min(o, c) - l) / span if span > 1e-12 else 0.0
        bars.append(
            {
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "body": body,
                "body_ratio": max(0.0, min(1.0, body_ratio)),
                "upper_ratio": max(0.0, min(1.0, upper_ratio)),
                "lower_ratio": max(0.0, min(1.0, lower_ratio)),
            }
        )

    if not bars:
        return {
            "candleBodyRatio": None,
            "candleUpperWickRatio": None,
            "candleLowerWickRatio": None,
            "candleTripletUp": None,
            "candleTripletDown": None,
            "shootingStarLike": None,
            "threeWhiteSoldiers": None,
            "bullEngulfing": None,
        }

    latest = bars[-1]
    shooting_star_like = 1.0 if (
        float(latest["upper_ratio"]) >= 0.48
        and float(latest["body_ratio"]) <= 0.38
        and float(latest["lower_ratio"]) <= 0.24
    ) else 0.0
    bull_engulfing: float | None = None
    if len(bars) >= 2:
        prev = bars[-2]
        bull_engulfing = 1.0 if (
            float(prev["body"]) < 0
            and float(latest["body"]) > 0
            and float(latest["o"]) <= float(prev["c"])
            and float(latest["c"]) >= float(prev["o"])
        ) else 0.0
    if len(bars) < 3:
        return {
            "candleBodyRatio": latest["body_ratio"],
            "candleUpperWickRatio": latest["upper_ratio"],
            "candleLowerWickRatio": latest["lower_ratio"],
            "candleTripletUp": None,
            "candleTripletDown": None,
            "shootingStarLike": shooting_star_like,
            "threeWhiteSoldiers": None,
            "bullEngulfing": bull_engulfing,
        }

    b0, b1, b2 = bars[-3], bars[-2], bars[-1]
    trio = [b0, b1, b2]
    bull_count = sum(1 for b in trio if b["body"] > 0)
    bear_count = sum(1 for b in trio if b["body"] < 0)
    higher_close = 1.0 if (b0["c"] < b1["c"] < b2["c"]) else 0.0
    lower_close = 1.0 if (b0["c"] > b1["c"] > b2["c"]) else 0.0
    move_3 = _safe_div(b2["c"] - b0["c"], b0["c"]) or 0.0

    prev_anchor = bars[-10]["c"] if len(bars) >= 10 else b0["o"]
    prev_anchor = prev_anchor if abs(prev_anchor) > 1e-12 else b0["o"]
    prior_move = _safe_div(b0["c"] - prev_anchor, prev_anchor) or 0.0

    latest_upper = float(b2["upper_ratio"])
    latest_lower = float(b2["lower_ratio"])
    three_white_soldiers = 1.0 if (
        all(float(bar["body"]) > 0 for bar in trio)
        and float(b0["c"]) < float(b1["c"]) < float(b2["c"])
        and min(float(bar["body_ratio"]) for bar in trio) >= 0.45
    ) else 0.0

    # Reversal-style 3-candle block:
    # - Up side: short pullback (bearish trio) after prior uptrend and lower-wick support.
    # - Down side: short squeeze (bullish trio) after prior downtrend and upper-wick pressure.
    up_prob = _clip01(
        0.10
        + 0.26 * (bear_count / 3.0)
        + 0.18 * lower_close
        + 0.16 * (_clip01(((-move_3) - 0.003) / 0.05) or 0.0)
        + 0.12 * (_clip01((prior_move + 0.06) / 0.18) or 0.0)
        + 0.11 * (_clip01((latest_lower + 0.02) / 0.30) or 0.0)
        + 0.07 * (_clip01((0.55 - latest_upper) / 0.55) or 0.0)
    )
    down_prob = _clip01(
        0.10
        + 0.26 * (bull_count / 3.0)
        + 0.18 * higher_close
        + 0.16 * (_clip01((move_3 - 0.003) / 0.05) or 0.0)
        + 0.12 * (_clip01(((-prior_move) + 0.06) / 0.18) or 0.0)
        + 0.11 * (_clip01((latest_upper + 0.02) / 0.30) or 0.0)
        + 0.07 * (_clip01((0.55 - latest_lower) / 0.55) or 0.0)
    )
    return {
        "candleBodyRatio": latest["body_ratio"],
        "candleUpperWickRatio": latest["upper_ratio"],
        "candleLowerWickRatio": latest["lower_ratio"],
        "candleTripletUp": up_prob,
        "candleTripletDown": down_prob,
        "shootingStarLike": shooting_star_like,
        "threeWhiteSoldiers": three_white_soldiers,
        "bullEngulfing": bull_engulfing,
    }


def _calc_regime_probs(closes: list[float], *, lookback: int) -> dict[str, float | None]:
    need = max(lookback + 1, 4)
    if len(closes) < need:
        return {
            "breakoutUpProb": None,
            "breakoutDownProb": None,
            "rangeProb": None,
            "rangeWidth": None,
            "rangePos": None,
        }
    last = float(closes[-1])
    hist = [float(v) for v in closes[-(lookback + 1) : -1]]
    if not hist:
        return {
            "breakoutUpProb": None,
            "breakoutDownProb": None,
            "rangeProb": None,
            "rangeWidth": None,
            "rangePos": None,
        }
    hi = max(hist)
    lo = min(hist)
    scale = max(abs((hi + lo) / 2.0), abs(hi), abs(lo), 1e-9)
    width = max(0.0, (hi - lo) / scale)
    if hi > lo:
        range_pos = max(0.0, min(1.0, (last - lo) / (hi - lo)))
    else:
        range_pos = 0.5
    compression = _clip01((0.45 - width) / 0.45) or 0.0
    up_break_dist = max(0.0, (last - hi) / max(abs(hi), 1e-9))
    down_break_dist = max(0.0, (lo - last) / max(abs(lo), 1e-9))
    up_break = _clip01(up_break_dist / 0.08) or 0.0
    down_break = _clip01(down_break_dist / 0.08) or 0.0
    midness = max(0.0, 1.0 - abs(range_pos - 0.5) * 2.0)

    up_prob = _clip01(0.55 * compression + 0.35 * range_pos + 0.10 * up_break)
    down_prob = _clip01(0.55 * compression + 0.35 * (1.0 - range_pos) + 0.10 * down_break)
    range_prob = _clip01(0.65 * compression + 0.35 * midness - 0.40 * max(up_break, down_break))
    if up_break > 0.0:
        up_prob = max(up_prob or 0.0, (_clip01(0.72 + 0.28 * up_break) or 0.0))
    if down_break > 0.0:
        down_prob = max(down_prob or 0.0, (_clip01(0.72 + 0.28 * down_break) or 0.0))
    return {
        "breakoutUpProb": up_prob,
        "breakoutDownProb": down_prob,
        "rangeProb": range_prob,
        "rangeWidth": width,
        "rangePos": range_pos,
    }


def _sort_items(items: list[dict], direction: RankDir) -> list[dict]:
    def _liquidity(value: float | None) -> float:
        return float(value) if value is not None else -1.0

    def _sort_key(item: dict) -> tuple:
        change = item.get("changePct")
        missing = change is None or not isinstance(change, (int, float)) or not math.isfinite(change)
        liq = _liquidity(item.get("liquidity20d"))
        if direction == "up":
            return (missing, -(change or 0.0), -liq, item.get("code", ""))
        return (missing, (change or 0.0), -liq, item.get("code", ""))

    return sorted(items, key=_sort_key)


def _finite_float(value: object) -> float | None:
    if not isinstance(value, (int, float)):
        return None
    casted = float(value)
    if not math.isfinite(casted):
        return None
    return casted


def _first_finite(*values: object) -> float | None:
    for value in values:
        resolved = _finite_float(value)
        if resolved is not None:
            return resolved
    return None


def _is_non_increasing_curve(
    value_5d: float | None,
    value_10d: float | None,
    value_20d: float | None,
    *,
    eps: float = _ENTRY_PROB_CURVE_EPS,
) -> bool:
    points = [
        value
        for value in (value_5d, value_10d, value_20d)
        if isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    if len(points) < 2:
        return True
    for left, right in zip(points, points[1:]):
        if float(left) + float(eps) < float(right):
            return False
    return True


def _sanitize_rank_item_for_json(item: dict) -> dict:
    sanitized: dict = {}
    for key, value in item.items():
        if isinstance(value, float) and not math.isfinite(value):
            sanitized[key] = None
            continue
        sanitized[key] = value
    return sanitized


def _fetch_daily_rows(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    return conn.execute(
        """
        SELECT code, date, o, h, l, c, v
        FROM (
            SELECT
                code,
                date,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
        )
        WHERE rn <= ?
        ORDER BY code, date
        """,
        [_DAILY_LIMIT],
    ).fetchall()


def _fetch_daily_rows_asof(conn: duckdb.DuckDBPyConnection, as_of_int: int) -> list[tuple]:
    as_of_epoch = _as_of_int_to_utc_epoch(as_of_int)
    return conn.execute(
        """
        SELECT code, date, o, h, l, c, v
        FROM (
            SELECT
                code,
                date,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
            WHERE date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END
        )
        WHERE rn <= ?
        ORDER BY code, date
        """,
        [as_of_epoch, as_of_int, _DAILY_LIMIT],
    ).fetchall()


def _fetch_monthly_rows(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    return conn.execute(
        """
        SELECT code, month, o, h, l, c, v
        FROM (
            SELECT
                code,
                month,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
            FROM monthly_bars
        )
        WHERE rn <= ?
        ORDER BY code, month
        """,
        [_MONTHLY_LIMIT],
    ).fetchall()


def _fetch_monthly_rows_asof(conn: duckdb.DuckDBPyConnection, as_of_int: int) -> list[tuple]:
    as_of_month = int((as_of_int // 100) * 100 + 1)
    as_of_month_epoch = _as_of_month_int_to_utc_epoch(as_of_month)
    return conn.execute(
        """
        SELECT code, month, o, h, l, c, v
        FROM (
            SELECT
                code,
                month,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
            FROM monthly_bars
            WHERE month <= CASE WHEN month >= 1000000000 THEN ? ELSE ? END
        )
        WHERE rn <= ?
        ORDER BY code, month
        """,
        [as_of_month_epoch, as_of_month, _MONTHLY_LIMIT],
    ).fetchall()


def _fetch_names(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute("SELECT code, name FROM tickers").fetchall()
    return {row[0]: row[1] for row in rows}


def _build_cache() -> dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]]:
    with duckdb.connect(str(core_config.DB_PATH)) as conn:
        codes = [row[0] for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
        names = _fetch_names(conn)
        daily_rows = _fetch_daily_rows(conn)
        monthly_rows = _fetch_monthly_rows(conn)

    daily_map: dict[str, list[tuple]] = {}
    for row in daily_rows:
        daily_map.setdefault(row[0], []).append(row[1:])

    monthly_map: dict[str, list[tuple]] = {}
    for row in monthly_rows:
        monthly_map.setdefault(row[0], []).append(row[1:])

    items_by_tf: dict[tuple[RankTimeframe, RankWhich], list[dict]] = {
        ("D", "latest"): [],
        ("D", "prev"): [],
        ("W", "latest"): [],
        ("W", "prev"): [],
        ("M", "latest"): [],
        ("M", "prev"): [],
    }

    for code in codes:
        daily = daily_map.get(code, [])
        if not daily:
            continue
        last_daily_dt = _parse_date_value(daily[-1][0])
        liquidity = _calc_liquidity_20d(daily)

        daily_closes: list[float] = []
        daily_dates: list[datetime] = []
        for row in daily:
            if row[4] is None:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            daily_closes.append(float(row[4]))
            daily_dates.append(dt)

        weekly = _build_weekly_bars(daily)
        weekly = _drop_incomplete_weekly(weekly, last_daily_dt)
        weekly_closes = [float(item["c"]) for item in weekly]
        weekly_dates = [item["last_date"] for item in weekly]

        monthly = monthly_map.get(code, [])
        monthly_closes: list[float] = []
        monthly_dates: list[datetime] = []
        for row in monthly:
            if row[4] is None:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            monthly_closes.append(float(row[4]))
            monthly_dates.append(dt)

        candle_signals = _calc_triplet_candle_signals(daily)
        v60_signals = _calc_60v_signals(daily)
        weekly_regime = _calc_regime_probs(weekly_closes, lookback=20)
        monthly_regime = _calc_regime_probs(monthly_closes, lookback=12)

        common_fields = {
            "candleBodyRatio": candle_signals.get("candleBodyRatio"),
            "candleUpperWickRatio": candle_signals.get("candleUpperWickRatio"),
            "candleLowerWickRatio": candle_signals.get("candleLowerWickRatio"),
            "candleTripletUp": candle_signals.get("candleTripletUp"),
            "candleTripletDown": candle_signals.get("candleTripletDown"),
            "shootingStarLike": candle_signals.get("shootingStarLike"),
            "threeWhiteSoldiers": candle_signals.get("threeWhiteSoldiers"),
            "bullEngulfing": candle_signals.get("bullEngulfing"),
            "weeklyBreakoutUpProb": weekly_regime.get("breakoutUpProb"),
            "weeklyBreakoutDownProb": weekly_regime.get("breakoutDownProb"),
            "weeklyRangeProb": weekly_regime.get("rangeProb"),
            "monthlyBreakoutUpProb": monthly_regime.get("breakoutUpProb"),
            "monthlyBreakoutDownProb": monthly_regime.get("breakoutDownProb"),
            "monthlyRangeProb": monthly_regime.get("rangeProb"),
            "monthlyRangeWidth": monthly_regime.get("rangeWidth"),
            "monthlyRangePos": monthly_regime.get("rangePos"),
            "reclaim60": v60_signals.get("reclaim60"),
            "v60Core": v60_signals.get("v60Core"),
            "v60Strong": v60_signals.get("v60Strong"),
        }

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                daily_closes, daily_dates, which
            )
            items_by_tf[("D", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                weekly_closes, weekly_dates, which
            )
            items_by_tf[("W", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                monthly_closes, monthly_dates, which
            )
            items_by_tf[("M", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

    cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
    for tf in ("D", "W", "M"):
        for which in ("latest", "prev"):
            items = items_by_tf[(tf, which)]
            for direction in ("up", "down"):
                cache[(tf, which, direction)] = _sort_items(items, direction)
    return cache


def _build_cache_asof(conn: duckdb.DuckDBPyConnection, as_of_int: int) -> dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]]:
    codes = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT code
            FROM daily_bars
            WHERE date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END
            ORDER BY code
            """,
            [_as_of_int_to_utc_epoch(as_of_int), as_of_int],
        ).fetchall()
    ]
    names = _fetch_names(conn)
    daily_rows = _fetch_daily_rows_asof(conn, as_of_int)
    monthly_rows = _fetch_monthly_rows_asof(conn, as_of_int)

    daily_map: dict[str, list[tuple]] = {}
    for row in daily_rows:
        daily_map.setdefault(row[0], []).append(row[1:])
    monthly_map: dict[str, list[tuple]] = {}
    for row in monthly_rows:
        monthly_map.setdefault(row[0], []).append(row[1:])

    items_by_tf: dict[tuple[RankTimeframe, RankWhich], list[dict]] = {
        ("D", "latest"): [],
        ("D", "prev"): [],
        ("W", "latest"): [],
        ("W", "prev"): [],
        ("M", "latest"): [],
        ("M", "prev"): [],
    }

    for code in codes:
        daily = daily_map.get(code, [])
        if not daily:
            continue
        last_daily_dt = _parse_date_value(daily[-1][0])
        liquidity = _calc_liquidity_20d(daily)
        daily_closes: list[float] = []
        daily_dates: list[datetime] = []
        for row in daily:
            if len(row) < 5:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            daily_closes.append(float(row[4]))
            daily_dates.append(dt)

        weekly = _build_weekly_bars(daily)
        weekly = _drop_incomplete_weekly(weekly, last_daily_dt)
        weekly_closes: list[float] = []
        weekly_dates: list[datetime] = []
        for item in weekly:
            weekly_closes.append(float(item["c"]))
            weekly_dates.append(item["last_date"])

        monthly = monthly_map.get(code, [])
        monthly_closes: list[float] = []
        monthly_dates: list[datetime] = []
        for row in monthly:
            if len(row) < 5:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            monthly_closes.append(float(row[4]))
            monthly_dates.append(dt)

        candle_signals = _calc_triplet_candle_signals(daily)
        v60_signals = _calc_60v_signals(daily)
        weekly_regime = _calc_regime_probs(weekly_closes, lookback=20)
        monthly_regime = _calc_regime_probs(monthly_closes, lookback=12)

        common_fields = {
            "candleBodyRatio": candle_signals.get("candleBodyRatio"),
            "candleUpperWickRatio": candle_signals.get("candleUpperWickRatio"),
            "candleLowerWickRatio": candle_signals.get("candleLowerWickRatio"),
            "candleTripletUp": candle_signals.get("candleTripletUp"),
            "candleTripletDown": candle_signals.get("candleTripletDown"),
            "shootingStarLike": candle_signals.get("shootingStarLike"),
            "threeWhiteSoldiers": candle_signals.get("threeWhiteSoldiers"),
            "bullEngulfing": candle_signals.get("bullEngulfing"),
            "weeklyBreakoutUpProb": weekly_regime.get("breakoutUpProb"),
            "weeklyBreakoutDownProb": weekly_regime.get("breakoutDownProb"),
            "weeklyRangeProb": weekly_regime.get("rangeProb"),
            "monthlyBreakoutUpProb": monthly_regime.get("breakoutUpProb"),
            "monthlyBreakoutDownProb": monthly_regime.get("breakoutDownProb"),
            "monthlyRangeProb": monthly_regime.get("rangeProb"),
            "monthlyRangeWidth": monthly_regime.get("rangeWidth"),
            "monthlyRangePos": monthly_regime.get("rangePos"),
            "reclaim60": v60_signals.get("reclaim60"),
            "v60Core": v60_signals.get("v60Core"),
            "v60Strong": v60_signals.get("v60Strong"),
        }

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                daily_closes, daily_dates, which
            )
            items_by_tf[("D", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                weekly_closes, weekly_dates, which
            )
            items_by_tf[("W", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                monthly_closes, monthly_dates, which
            )
            items_by_tf[("M", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

    cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
    for tf in ("D", "W", "M"):
        for which in ("latest", "prev"):
            items = items_by_tf[(tf, which)]
            for direction in ("up", "down"):
                cache[(tf, which, direction)] = _sort_items(items, direction)
    return cache


def _resolve_prediction_dt(conn: duckdb.DuckDBPyConnection, items: list[dict]) -> int | None:
    as_of_values = sorted(
        {v for v in (_iso_date_to_int(item.get("asOf")) for item in items) if v is not None}
    )
    if as_of_values:
        row = conn.execute("SELECT MAX(dt) FROM ml_pred_20d WHERE dt <= ?", [as_of_values[-1]]).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    row = conn.execute("SELECT MAX(dt) FROM ml_pred_20d").fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _load_ml_pred_map(
    conn: duckdb.DuckDBPyConnection,
    pred_dt: int,
) -> tuple[dict[str, dict], str | None]:
    cols = conn.execute("PRAGMA table_info('ml_pred_20d')").fetchall()
    names = {str(row[1]).lower() for row in cols}
    p_up_5_expr = "p_up_5" if "p_up_5" in names else "NULL AS p_up_5"
    p_up_10_expr = "p_up_10" if "p_up_10" in names else "NULL AS p_up_10"
    turn_up_expr = "p_turn_up" if "p_turn_up" in names else "NULL AS p_turn_up"
    turn_down_expr = "p_turn_down" if "p_turn_down" in names else "NULL AS p_turn_down"
    turn_down_5_expr = "p_turn_down_5" if "p_turn_down_5" in names else "NULL AS p_turn_down_5"
    turn_down_10_expr = "p_turn_down_10" if "p_turn_down_10" in names else "NULL AS p_turn_down_10"
    turn_down_20_expr = "p_turn_down_20" if "p_turn_down_20" in names else "NULL AS p_turn_down_20"
    p_down_expr = "p_down" if "p_down" in names else "NULL AS p_down"
    rank_up_expr = "rank_up_20" if "rank_up_20" in names else "NULL AS rank_up_20"
    rank_down_expr = "rank_down_20" if "rank_down_20" in names else "NULL AS rank_down_20"
    ev5_net_expr = "ev5_net" if "ev5_net" in names else "NULL AS ev5_net"
    ev10_net_expr = "ev10_net" if "ev10_net" in names else "NULL AS ev10_net"
    rows = conn.execute(
        f"""
        SELECT
            code,
            p_up,
            {p_up_5_expr},
            {p_up_10_expr},
            {turn_up_expr},
            {turn_down_expr},
            {turn_down_5_expr},
            {turn_down_10_expr},
            {turn_down_20_expr},
            {p_down_expr},
            {rank_up_expr},
            {rank_down_expr},
            ret_pred20,
            ev20,
            ev20_net,
            {ev5_net_expr},
            {ev10_net_expr},
            model_version
        FROM ml_pred_20d
        WHERE dt = ?
        """,
        [pred_dt],
    ).fetchall()
    pred_map = {
        str(row[0]): {
            "p_up": float(row[1]) if row[1] is not None else None,
            "p_up_5": float(row[2]) if row[2] is not None else None,
            "p_up_10": float(row[3]) if row[3] is not None else None,
            "p_turn_up": float(row[4]) if row[4] is not None else None,
            "p_turn_down": float(row[5]) if row[5] is not None else None,
            "p_turn_down_5": float(row[6]) if row[6] is not None else None,
            "p_turn_down_10": float(row[7]) if row[7] is not None else None,
            "p_turn_down_20": float(row[8]) if row[8] is not None else None,
            "p_down": float(row[9]) if row[9] is not None else None,
            "rank_up_20": float(row[10]) if row[10] is not None else None,
            "rank_down_20": float(row[11]) if row[11] is not None else None,
            "ret_pred20": float(row[12]) if row[12] is not None else None,
            "ev20": float(row[13]) if row[13] is not None else None,
            "ev20_net": float(row[14]) if row[14] is not None else None,
            "ev5_net": float(row[15]) if row[15] is not None else None,
            "ev10_net": float(row[16]) if row[16] is not None else None,
            "model_version": row[17],
        }
        for row in rows
    }
    model_version = None
    for item in pred_map.values():
        model_version = item.get("model_version")
        if model_version:
            break
    return pred_map, model_version


def _resolve_monthly_prediction_dt(conn: duckdb.DuckDBPyConnection, items: list[dict]) -> int | None:
    if not _table_exists(conn, "ml_monthly_pred"):
        return None
    as_of_month_values = sorted(
        {
            month
            for month in (
                _to_month_start_int(_iso_date_to_int(item.get("asOf")))
                for item in items
            )
            if month is not None
        }
    )
    if as_of_month_values:
        row = conn.execute(
            "SELECT MAX(dt) FROM ml_monthly_pred WHERE dt <= ?",
            [as_of_month_values[-1]],
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    row = conn.execute("SELECT MAX(dt) FROM ml_monthly_pred").fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _load_monthly_pred_map(
    conn: duckdb.DuckDBPyConnection,
    pred_dt: int,
) -> tuple[dict[str, dict], str | None]:
    if not _table_exists(conn, "ml_monthly_pred"):
        return {}, None
    rows = conn.execute(
        """
        SELECT
            code,
            p_abs_big,
            p_up_given_big,
            p_up_big,
            p_down_big,
            score_up,
            score_down,
            model_version,
            n_train_abs,
            n_train_dir
        FROM ml_monthly_pred
        WHERE dt = ?
        """,
        [pred_dt],
    ).fetchall()
    pred_map = {
        str(row[0]): {
            "p_abs_big": float(row[1]) if row[1] is not None else None,
            "p_up_given_big": float(row[2]) if row[2] is not None else None,
            "p_up_big": float(row[3]) if row[3] is not None else None,
            "p_down_big": float(row[4]) if row[4] is not None else None,
            "score_up": float(row[5]) if row[5] is not None else None,
            "score_down": float(row[6]) if row[6] is not None else None,
            "model_version": row[7],
            "n_train_abs": int(row[8]) if row[8] is not None else None,
            "n_train_dir": int(row[9]) if row[9] is not None else None,
        }
        for row in rows
    }
    model_version = None
    for item in pred_map.values():
        model_version = item.get("model_version")
        if model_version:
            break
    return pred_map, model_version


def _clamp_monthly_gate(value: float | None, *, low: float, high: float) -> float:
    if value is None or not math.isfinite(float(value)):
        return float(low)
    return float(max(low, min(high, float(value))))


def _default_monthly_ret20_lookup() -> dict[str, dict[str, Any]]:
    return {
        "up": {"baseline_rate": 0.03, "bins": []},
        "down": {"baseline_rate": 0.02, "bins": []},
    }


def _sanitize_monthly_ret20_lookup_dir(raw: object, fallback_baseline: float) -> dict[str, Any]:
    baseline = fallback_baseline
    bins: list[dict[str, float]] = []
    if isinstance(raw, dict):
        baseline = _clamp_monthly_gate(_first_finite(raw.get("baseline_rate")), low=0.0, high=1.0)
        raw_bins = raw.get("bins")
        if isinstance(raw_bins, list):
            for item in raw_bins:
                if not isinstance(item, dict):
                    continue
                low = _first_finite(item.get("min_prob"))
                high = _first_finite(item.get("max_prob"))
                rate = _first_finite(item.get("event_rate"))
                samples = _first_finite(item.get("samples"))
                if (
                    low is None
                    or high is None
                    or rate is None
                    or high < low
                ):
                    continue
                bins.append(
                    {
                        "min_prob": float(max(0.0, min(1.0, low))),
                        "max_prob": float(max(0.0, min(1.0, high))),
                        "event_rate": float(max(0.0, min(1.0, rate))),
                        "samples": float(samples) if samples is not None else 0.0,
                    }
                )
    bins = sorted(bins, key=lambda row: (row.get("min_prob", 0.0), row.get("max_prob", 0.0)))
    running = 0.0
    for row in bins:
        running = max(running, float(row.get("event_rate") or 0.0))
        row["event_rate"] = float(running)
    return {
        "baseline_rate": float(max(0.0, min(1.0, baseline))),
        "bins": bins,
    }


def _load_monthly_gate_recommendation(
    conn: duckdb.DuckDBPyConnection,
    model_version: str | None,
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, Any]]]:
    default = {
        "up": {
            "abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT),
            "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT),
        },
        "down": {
            "abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT),
            "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT),
        },
    }
    default_ret20_lookup = _default_monthly_ret20_lookup()
    if not model_version or not _table_exists(conn, "ml_monthly_model_registry"):
        return default, default_ret20_lookup
    row = conn.execute(
        """
        SELECT metrics_json
        FROM ml_monthly_model_registry
        WHERE model_version = ?
        LIMIT 1
        """,
        [model_version],
    ).fetchone()
    if not row or row[0] is None:
        return default, default_ret20_lookup
    try:
        metrics_json = json.loads(str(row[0]))
    except Exception:
        return default, default_ret20_lookup
    if not isinstance(metrics_json, dict):
        return default, default_ret20_lookup
    rec = metrics_json.get("gate_recommendation")
    ret20_raw = metrics_json.get("ret20_lookup")
    if not isinstance(rec, dict):
        rec = {}
    out = dict(default)
    for direction in ("up", "down"):
        raw = rec.get(direction)
        if not isinstance(raw, dict):
            continue
        out[direction] = {
            "abs_gate": _clamp_monthly_gate(
                _first_finite(raw.get("abs_gate")),
                low=_MONTHLY_ABS_GATE_MIN,
                high=0.60,
            ),
            "side_gate": _clamp_monthly_gate(
                _first_finite(raw.get("side_gate")),
                low=_MONTHLY_SIDE_GATE_MIN,
                high=0.60,
            ),
        }
        target20_gate = _first_finite(raw.get("target20_gate"))
        if target20_gate is not None:
            out[direction]["target20_gate"] = _clamp_monthly_gate(
                target20_gate,
                low=0.02,
                high=0.60,
            )
    ret20_lookup = dict(default_ret20_lookup)
    if isinstance(ret20_raw, dict):
        ret20_lookup["up"] = _sanitize_monthly_ret20_lookup_dir(ret20_raw.get("up"), 0.03)
        ret20_lookup["down"] = _sanitize_monthly_ret20_lookup_dir(ret20_raw.get("down"), 0.02)
    return out, ret20_lookup


def _estimate_monthly_side20_probability(
    prob_side: float | None,
    lookup_dir: dict[str, Any],
) -> float | None:
    if prob_side is None or not math.isfinite(float(prob_side)):
        return None
    p = float(max(0.0, min(1.0, prob_side)))
    baseline = _first_finite(lookup_dir.get("baseline_rate")) or 0.0
    raw_bins = lookup_dir.get("bins")
    bins = raw_bins if isinstance(raw_bins, list) else []
    fallback = baseline * 0.5 + 0.20 * p
    for idx, row in enumerate(bins):
        if not isinstance(row, dict):
            continue
        low = _first_finite(row.get("min_prob"))
        high = _first_finite(row.get("max_prob"))
        rate = _first_finite(row.get("event_rate"))
        if low is None or high is None or rate is None:
            continue
        in_bin = (p >= low and p < high) if idx < len(bins) - 1 else (p >= low and p <= high)
        if in_bin:
            mixed = 0.70 * float(rate) + 0.30 * float(fallback)
            return float(max(0.0, min(1.0, mixed)))
    # Conservative fallback when lookup bins are unavailable.
    return float(max(0.0, min(1.0, fallback)))


def _calc_monthly_accumulation_score(item: dict, *, direction: RankDir) -> float:
    range_prob = _first_finite(item.get("monthlyRangeProb"))
    range_width = _first_finite(item.get("monthlyRangeWidth"))
    range_pos = _first_finite(item.get("monthlyRangePos"))
    body_ratio = _first_finite(item.get("candleBodyRatio"))
    change_pct = _first_finite(item.get("changePct"))
    score = 0.0
    if range_prob is not None:
        score += 0.35 * max(0.0, min(1.0, (range_prob - 0.45) / 0.35))
    if range_width is not None:
        score += 0.25 * max(0.0, min(1.0, (0.35 - range_width) / 0.25))
    if range_pos is not None:
        if direction == "up":
            pos_term = (0.55 - range_pos) / 0.35
        else:
            pos_term = (range_pos - 0.45) / 0.35
        score += 0.20 * max(0.0, min(1.0, pos_term))
    if body_ratio is not None:
        score += 0.10 * max(0.0, min(1.0, (0.55 - body_ratio) / 0.25))
    if change_pct is not None:
        score += 0.10 * max(0.0, min(1.0, (0.14 - abs(change_pct)) / 0.14))
    return float(max(0.0, min(1.0, score)))


def _calc_monthly_breakout_readiness_score(item: dict, *, direction: RankDir) -> float:
    monthly_breakout_prob = _first_finite(
        item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
    )
    weekly_breakout_prob = _first_finite(
        item.get("weeklyBreakoutUpProb") if direction == "up" else item.get("weeklyBreakoutDownProb")
    )
    candle_triplet = _first_finite(item.get("candleTripletUp") if direction == "up" else item.get("candleTripletDown"))
    monthly_range_prob = _first_finite(item.get("monthlyRangeProb"))
    score = 0.0
    if monthly_breakout_prob is not None:
        score += 0.55 * monthly_breakout_prob
    if weekly_breakout_prob is not None:
        score += 0.25 * weekly_breakout_prob
    if candle_triplet is not None:
        score += 0.20 * candle_triplet
    if (
        monthly_range_prob is not None
        and monthly_range_prob >= 0.70
        and (monthly_breakout_prob is None or monthly_breakout_prob < 0.55)
    ):
        score -= 0.08
    return float(max(0.0, min(1.0, score)))


def _count_monthly_gate_candidates(
    items: list[dict],
    *,
    direction: RankDir,
    abs_gate: float,
    side_gate: float,
) -> int:
    side_key = "mlPUpBig" if direction == "up" else "mlPDownBig"
    count = 0
    for item in items:
        p_abs_big = _first_finite(item.get("mlPAbsBig"))
        prob_side = _first_finite(item.get(side_key))
        liquidity = _first_finite(item.get("liquidity20d"))
        if (
            p_abs_big is not None
            and p_abs_big >= float(abs_gate)
            and prob_side is not None
            and prob_side >= float(side_gate)
            and liquidity is not None
        ):
            count += 1
    return int(count)


def _relax_monthly_gates_for_coverage(
    items: list[dict],
    *,
    direction: RankDir,
    abs_gate: float,
    side_gate: float,
    limit: int,
) -> tuple[float, float]:
    base_abs = _clamp_monthly_gate(abs_gate, low=_MONTHLY_ABS_GATE_MIN, high=0.60)
    base_side = _clamp_monthly_gate(side_gate, low=_MONTHLY_SIDE_GATE_MIN, high=0.60)
    required = int(max(1, min(limit, _MONTHLY_GATE_MIN_CANDIDATES)))
    if _count_monthly_gate_candidates(
        items,
        direction=direction,
        abs_gate=base_abs,
        side_gate=base_side,
    ) >= required:
        return base_abs, base_side
    abs_steps = sorted(
        {float(base_abs), *[float(v) for v in _MONTHLY_ABS_RELAX_STEPS if float(v) <= float(base_abs) + 1e-12]},
        reverse=True,
    )
    side_steps = sorted(
        {float(base_side), *[float(v) for v in _MONTHLY_SIDE_RELAX_STEPS if float(v) <= float(base_side) + 1e-12]},
        reverse=True,
    )
    for abs_step in abs_steps:
        abs_step = _clamp_monthly_gate(abs_step, low=_MONTHLY_ABS_GATE_MIN, high=0.60)
        for side_step in side_steps:
            side_step = _clamp_monthly_gate(side_step, low=_MONTHLY_SIDE_GATE_MIN, high=0.60)
            count = _count_monthly_gate_candidates(
                items,
                direction=direction,
                abs_gate=abs_step,
                side_gate=side_step,
            )
            if count >= required:
                return abs_step, side_step
    return float(_MONTHLY_ABS_GATE_MIN), float(_MONTHLY_SIDE_GATE_MIN)


def _decorate_items_with_monthly_ml(items: list[dict], pred_map: dict[str, dict]) -> list[dict]:
    enriched: list[dict] = []
    for item in items:
        code = str(item.get("code") or "")
        pred = pred_map.get(code) or {}
        p_up_big = _first_finite(pred.get("p_up_big"))
        p_down_big = _first_finite(pred.get("p_down_big"))
        enriched.append(
            {
                **item,
                "mlPAbsBig": _first_finite(pred.get("p_abs_big")),
                "mlPUpBig": p_up_big,
                "mlPDownBig": p_down_big,
                "mlScoreUp1M": _first_finite(pred.get("score_up")),
                "mlScoreDown1M": _first_finite(pred.get("score_down")),
                # Backward compatible fields.
                "mlPUp": p_up_big,
                "mlPDown": p_down_big,
                "mlRankUp": _first_finite(pred.get("score_up")),
                "mlRankDown": _first_finite(pred.get("score_down")),
                "mlEv20Net": (
                    float(p_up_big - p_down_big)
                    if p_up_big is not None and p_down_big is not None
                    else None
                ),
                "modelVersion": pred.get("model_version"),
                "prob5d": None,
                "prob10d": None,
                "prob20d": None,
                "prob5dAligned": None,
                "probCurveAligned": None,
                "horizonAligned": None,
            }
        )
    return enriched


def _apply_monthly_ml_mode(
    items: list[dict],
    *,
    direction: RankDir,
    limit: int,
) -> tuple[list[dict], int | None, str | None]:
    gate_recommendation = {
        "up": {"abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT), "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT)},
        "down": {"abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT), "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT)},
    }
    ret20_lookup = _default_monthly_ret20_lookup()
    try:
        with duckdb.connect(str(core_config.DB_PATH)) as conn:
            pred_dt = _resolve_monthly_prediction_dt(conn, items)
            if pred_dt is None:
                return items[:limit], None, None
            pred_map, model_version = _load_monthly_pred_map(conn, pred_dt)
            gate_recommendation, ret20_lookup = _load_monthly_gate_recommendation(conn, model_version)
    except Exception:
        return items[:limit], None, None
    if not pred_map:
        return items[:limit], pred_dt, model_version

    enriched = _decorate_items_with_monthly_ml(items, pred_map)
    dir_gate = gate_recommendation.get(direction, {})
    abs_gate, side_gate = _relax_monthly_gates_for_coverage(
        enriched,
        direction=direction,
        abs_gate=_first_finite(dir_gate.get("abs_gate")) or _MONTHLY_ABS_GATE_DEFAULT,
        side_gate=_first_finite(dir_gate.get("side_gate")) or _MONTHLY_SIDE_GATE_DEFAULT,
        limit=limit,
    )
    ret20_dir_lookup = ret20_lookup.get(direction) if isinstance(ret20_lookup, dict) else {}
    if not isinstance(ret20_dir_lookup, dict):
        ret20_dir_lookup = {}
    ret20_baseline = _first_finite(ret20_dir_lookup.get("baseline_rate")) or (0.03 if direction == "up" else 0.02)
    target20_floor = _MONTHLY_TARGET20_GATE_MIN_UP if direction == "up" else _MONTHLY_TARGET20_GATE_MIN_DOWN
    rec_target20_gate = _first_finite(dir_gate.get("target20_gate"))
    if rec_target20_gate is not None:
        target20_gate = float(max(target20_floor, min(0.50, rec_target20_gate)))
        target20_gate_source = "model_backtest"
    else:
        target20_gate = float(max(target20_floor, min(0.35, ret20_baseline * 2.8)))
        target20_gate_source = "baseline"
    qualified: list[dict] = []
    by_code: dict[str, dict] = {}
    for item in enriched:
        code = str(item.get("code") or "")
        by_code[code] = item
        p_abs_big = _first_finite(item.get("mlPAbsBig"))
        prob_side = _first_finite(item.get("mlPUpBig") if direction == "up" else item.get("mlPDownBig"))
        score_side = _first_finite(item.get("mlScoreUp1M") if direction == "up" else item.get("mlScoreDown1M"))
        monthly_breakout_prob = _first_finite(
            item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
        )
        monthly_range_prob = _first_finite(item.get("monthlyRangeProb"))
        p_side20 = _estimate_monthly_side20_probability(prob_side, ret20_dir_lookup)
        accumulation_score = _calc_monthly_accumulation_score(item, direction=direction)
        breakout_readiness = _calc_monthly_breakout_readiness_score(item, direction=direction)
        monthly_range_pos = _first_finite(item.get("monthlyRangePos"))
        shooting_star_like = _first_finite(item.get("shootingStarLike"))
        three_white_soldiers = _first_finite(item.get("threeWhiteSoldiers"))
        bull_engulfing = _first_finite(item.get("bullEngulfing"))
        v60_strong = _first_finite(item.get("v60Strong"))
        range_trap_penalty = 0.0
        if (
            monthly_range_prob is not None
            and monthly_range_prob >= 0.75
            and breakout_readiness < 0.55
        ):
            range_trap_penalty = 0.03
        p_side20_adj = (
            float(
                max(
                    0.0,
                    min(
                        1.0,
                        (p_side20 if p_side20 is not None else ret20_baseline)
                        + 0.07 * breakout_readiness
                        + 0.05 * accumulation_score
                        - range_trap_penalty,
                    ),
                )
            )
            if (p_side20 is not None or math.isfinite(float(ret20_baseline)))
            else None
        )
        regime_bonus = 0.0
        if monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60:
            regime_bonus += _MONTHLY_REGIME_BONUS
        if (
            monthly_range_prob is not None
            and monthly_range_prob >= 0.70
            and (monthly_breakout_prob is None or monthly_breakout_prob < 0.55)
        ):
            regime_bonus -= _MONTHLY_RANGE_PENALTY
        pattern_bonus = 0.0
        box_bottom_ok = bool(
            monthly_range_prob is not None
            and monthly_range_pos is not None
            and monthly_range_prob >= 0.62
            and (
                (direction == "up" and monthly_range_pos <= 0.38)
                or (direction == "down" and monthly_range_pos >= 0.62)
            )
        )
        if direction == "up":
            if shooting_star_like is not None and shooting_star_like >= 0.5:
                pattern_bonus += _ENTRY_BONUS_CANDLE_PATTERN
            if three_white_soldiers is not None and three_white_soldiers >= 0.5:
                pattern_bonus += _ENTRY_BONUS_CANDLE_PATTERN
            if bull_engulfing is not None and bull_engulfing >= 0.5:
                pattern_bonus += _ENTRY_BONUS_CANDLE_PATTERN
            if v60_strong is not None and v60_strong >= 0.5:
                pattern_bonus -= _ENTRY_PENALTY_60V_STRONG
        if box_bottom_ok:
            pattern_bonus += 0.02

        item["hybridScore"] = float(score_side) if score_side is not None else None
        item["probSide"] = float(prob_side) if prob_side is not None else None
        item["mlP20Side1MRaw"] = float(p_side20) if p_side20 is not None else None
        item["mlP20Side1M"] = float(p_side20_adj) if p_side20_adj is not None else None
        item["accumulationScore"] = float(accumulation_score)
        item["breakoutReadiness"] = float(breakout_readiness)
        item["boxBottomAligned"] = bool(box_bottom_ok)
        item["candlestickPatternBonus"] = float(pattern_bonus)
        item["v60StrongPenalty"] = bool(direction == "up" and v60_strong is not None and v60_strong >= 0.5)
        item["target20Gate"] = float(target20_gate)
        item["target20GateSource"] = target20_gate_source
        item["target20Qualified"] = bool(
            p_side20_adj is not None
            and p_side20_adj >= target20_gate
        )
        item["entryGateAbs"] = float(abs_gate)
        item["entryGateSide"] = float(side_gate)
        item["entryScore"] = (
            float(
                0.48 * (score_side if score_side is not None else 0.0)
                + 0.32 * (p_side20_adj if p_side20_adj is not None else ret20_baseline)
                + 0.14 * breakout_readiness
                + 0.06 * accumulation_score
                + regime_bonus
                + pattern_bonus
            )
            if score_side is not None
            else None
        )
        item["monthlyRegimeAligned"] = bool(monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60)
        trend_breakout_ok = bool(
            breakout_readiness >= 0.70
            and prob_side is not None
            and prob_side >= max(float(side_gate), 0.25)
        )
        accumulation_ok = bool(
            accumulation_score >= 0.70
            and breakout_readiness >= 0.45
            and prob_side is not None
            and prob_side >= max(0.20, float(side_gate) * 0.85)
        )
        target20_ok = bool(p_side20_adj is not None and p_side20_adj >= target20_gate)
        if target20_ok and trend_breakout_ok:
            setup_type = "breakout20"
        elif accumulation_ok:
            setup_type = "accumulation"
        elif trend_breakout_ok:
            setup_type = "breakout"
        else:
            setup_type = "watch"
        item["setupType"] = setup_type
        item["entryQualified"] = bool(
            p_abs_big is not None
            and p_abs_big >= float(abs_gate)
            and prob_side is not None
            and prob_side >= float(side_gate)
            and _first_finite(item.get("liquidity20d")) is not None
            and (target20_ok or trend_breakout_ok or accumulation_ok)
        )
        if item["entryQualified"]:
            qualified.append(item)

    qualified.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("probSide") or 0.0),
            item.get("code", ""),
        )
    )
    if len(qualified) >= limit:
        return qualified[:limit], pred_dt, model_version

    selected: list[dict] = []
    seen: set[str] = set()
    for item in qualified:
        code = str(item.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        selected.append(item)
    for base in items:
        code = str(base.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        candidate = by_code.get(code) or {
            **base,
            "mlPAbsBig": None,
            "mlPUpBig": None,
            "mlPDownBig": None,
            "mlScoreUp1M": None,
            "mlScoreDown1M": None,
            "entryQualified": False,
        }
        selected.append(candidate)
        if len(selected) >= limit:
            break
    return selected[:limit], pred_dt, model_version


def _load_daily_snapshot_map(
    conn: duckdb.DuckDBPyConnection,
    anchor_dt: int,
) -> dict[str, dict]:
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT
                b.code,
                b.date,
                b.c,
                m.ma20,
                m.ma60,
                ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.date DESC) AS rn
            FROM daily_bars b
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            WHERE b.date <= ?
        )
        SELECT
            code,
            MAX(CASE WHEN rn = 1 THEN date END) AS snap_dt,
            MAX(CASE WHEN rn = 1 THEN c END) AS snap_close,
            MAX(CASE WHEN rn = 1 THEN ma20 END) AS snap_ma20,
            MAX(CASE WHEN rn = 1 THEN ma60 END) AS snap_ma60,
            MAX(CASE WHEN rn = 2 THEN c END) AS prev_close,
            MAX(CASE WHEN rn = 2 THEN ma20 END) AS prev_ma20,
            MAX(CASE WHEN rn = 2 THEN ma60 END) AS prev_ma60
        FROM latest
        WHERE rn <= 2
        GROUP BY code
        """,
        [anchor_dt],
    ).fetchall()
    snapshot_map: dict[str, dict] = {}
    for row in rows:
        code = str(row[0])
        close = float(row[2]) if row[2] is not None else None
        ma20 = float(row[3]) if row[3] is not None else None
        ma60 = float(row[4]) if row[4] is not None else None
        prev_close = float(row[5]) if row[5] is not None else None
        prev_ma20 = float(row[6]) if row[6] is not None else None
        prev_ma60 = float(row[7]) if row[7] is not None else None
        dist_ma20 = None
        dist_ma20_signed = None
        dist_ma60_signed = None
        if close is not None and ma20 is not None and ma20 > 0:
            dist_ma20 = abs(close - ma20) / ma20
            dist_ma20_signed = (close - ma20) / ma20
        if close is not None and ma60 is not None and ma60 > 0:
            dist_ma60_signed = (close - ma60) / ma60
        trend_up = (
            close is not None
            and ma20 is not None
            and ma60 is not None
            and close > ma20 > ma60
        )
        trend_down = (
            close is not None
            and ma20 is not None
            and ma60 is not None
            and close < ma20 < ma60
        )
        ma20_slope = (
            (ma20 - prev_ma20)
            if ma20 is not None and prev_ma20 is not None and math.isfinite(ma20) and math.isfinite(prev_ma20)
            else None
        )
        ma60_slope = (
            (ma60 - prev_ma60)
            if ma60 is not None and prev_ma60 is not None and math.isfinite(ma60) and math.isfinite(prev_ma60)
            else None
        )
        trend_up_strict = bool(
            trend_up
            and isinstance(ma20_slope, (int, float))
            and isinstance(ma60_slope, (int, float))
            and ma20_slope > 0
            and ma60_slope > 0
            and isinstance(dist_ma20_signed, (int, float))
            and dist_ma20_signed >= 0.005
        )
        trend_down_strict = bool(
            trend_down
            and isinstance(ma20_slope, (int, float))
            and isinstance(ma60_slope, (int, float))
            and ma20_slope < 0
            and ma60_slope < 0
            and isinstance(dist_ma20_signed, (int, float))
            and dist_ma20_signed <= -0.005
            and isinstance(dist_ma60_signed, (int, float))
            and dist_ma60_signed <= -0.01
        )
        snapshot_map[code] = {
            "snap_dt": int(row[1]) if row[1] is not None else None,
            "snap_close": close,
            "snap_ma20": ma20,
            "snap_ma60": ma60,
            "prev_close": prev_close,
            "prev_ma20": prev_ma20,
            "prev_ma60": prev_ma60,
            "dist_ma20": dist_ma20,
            "dist_ma20_signed": dist_ma20_signed,
            "dist_ma60_signed": dist_ma60_signed,
            "ma20_slope": ma20_slope,
            "ma60_slope": ma60_slope,
            "trend_up": bool(trend_up),
            "trend_down": bool(trend_down),
            "trend_up_strict": trend_up_strict,
            "trend_down_strict": trend_down_strict,
        }
    return snapshot_map


def _decorate_items_with_ml(
    items: list[dict],
    pred_map: dict[str, dict],
    snapshot_map: dict[str, dict],
) -> list[dict]:
    enriched: list[dict] = []
    for item in items:
        code = str(item.get("code") or "")
        pred = pred_map.get(code) or {}
        snap = snapshot_map.get(code) or {}
        p_up_short = _first_finite(pred.get("p_up_5"), pred.get("p_up_10"), pred.get("p_up"))
        p_down_short = _first_finite(
            pred.get("p_down"),
            (1.0 - p_up_short) if p_up_short is not None else None,
        )
        p_turn_down_short = _first_finite(
            pred.get("p_turn_down_5"),
            pred.get("p_turn_down_10"),
            pred.get("p_turn_down_20"),
            pred.get("p_turn_down"),
        )
        ev_short_net = _first_finite(pred.get("ev5_net"), pred.get("ev10_net"), pred.get("ev20_net"))
        enriched.append(
            {
                **item,
                "mlPUp": pred.get("p_up"),
                "mlPUp5": pred.get("p_up_5"),
                "mlPUp10": pred.get("p_up_10"),
                "mlPUpShort": p_up_short,
                "mlPDownShort": p_down_short,
                "mlPDown": pred.get("p_down"),
                "mlPTurnUp": pred.get("p_turn_up"),
                "mlPTurnDown": pred.get("p_turn_down"),
                "mlPTurnDown5": pred.get("p_turn_down_5"),
                "mlPTurnDown10": pred.get("p_turn_down_10"),
                "mlPTurnDown20": pred.get("p_turn_down_20"),
                "mlRankUp": pred.get("rank_up_20"),
                "mlRankDown": pred.get("rank_down_20"),
                "mlPTurnDownShort": p_turn_down_short,
                "mlRetPred20": pred.get("ret_pred20"),
                "mlEv20": pred.get("ev20"),
                "mlEv20Net": pred.get("ev20_net"),
                "mlEv5Net": pred.get("ev5_net"),
                "mlEv10Net": pred.get("ev10_net"),
                "mlEvShortNet": ev_short_net,
                "modelVersion": pred.get("model_version"),
                "hybridScore": None,
                "entryScore": None,
                "trendUp": snap.get("trend_up"),
                "trendDown": snap.get("trend_down"),
                "trendUpStrict": snap.get("trend_up_strict"),
                "trendDownStrict": snap.get("trend_down_strict"),
                "distMa20": snap.get("dist_ma20"),
                "distMa20Signed": snap.get("dist_ma20_signed"),
                "ma20Slope": snap.get("ma20_slope"),
                "ma60Slope": snap.get("ma60_slope"),
            }
        )
    return enriched


def _percent_rank_desc(values: dict[str, float | None]) -> dict[str, float]:
    pairs = [
        (code, float(value))
        for code, value in values.items()
        if value is not None and isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    if not pairs:
        return {}
    pairs.sort(key=lambda item: (-item[1], item[0]))
    n = len(pairs)
    if n == 1:
        return {pairs[0][0]: 1.0}
    result: dict[str, float] = {}
    idx = 0
    while idx < n:
        value = pairs[idx][1]
        start = idx
        idx += 1
        while idx < n and pairs[idx][1] == value:
            idx += 1
        end = idx - 1
        avg_rank = ((start + 1) + (end + 1)) / 2.0
        pr = 1.0 - ((avg_rank - 1.0) / (n - 1.0))
        for j in range(start, idx):
            result[pairs[j][0]] = pr
    return result


def _apply_ml_mode(
    items: list[dict],
    *,
    direction: RankDir,
    mode: RankMode,
    limit: int,
) -> tuple[list[dict], int | None, str | None]:
    try:
        with duckdb.connect(str(core_config.DB_PATH)) as conn:
            pred_dt = _resolve_prediction_dt(conn, items)
            if pred_dt is None:
                return items[:limit], None, None
            pred_map, model_version = _load_ml_pred_map(conn, pred_dt)
            snapshot_map = _load_daily_snapshot_map(conn, pred_dt)
    except Exception:
        return items[:limit], None, None

    cfg = load_ml_config()
    enriched = _decorate_items_with_ml(items, pred_map, snapshot_map)

    def _prob_up_short(item: dict) -> float | None:
        return _first_finite(item.get("mlPUpShort"), item.get("mlPUp"))

    def _prob_down_short(item: dict) -> float | None:
        if isinstance(item.get("mlPDown"), (int, float)):
            return _first_finite(item.get("mlPDownShort"), item.get("mlPDown"))
        return _first_finite(item.get("mlPDownShort"), 1.0 - float(item.get("mlPUp"))) if isinstance(item.get("mlPUp"), (int, float)) else _first_finite(item.get("mlPDownShort"))

    def _turn_down_short(item: dict) -> float | None:
        return _first_finite(item.get("mlPTurnDownShort"), item.get("mlPTurnDown"))

    def _turn_up_short(item: dict) -> float | None:
        down = _turn_down_short(item)
        if down is not None:
            return max(0.0, min(1.0, 1.0 - down))
        return _first_finite(item.get("mlPTurnUp"))

    def _rank_up(item: dict) -> float | None:
        return _first_finite(item.get("mlRankUp"))

    def _rank_down(item: dict) -> float | None:
        return _first_finite(item.get("mlRankDown"))

    if mode == "ml":
        selected = select_top_n_ml(
            enriched,
            top_n=int(cfg.top_n),
            p_up_threshold=float(cfg.p_up_threshold if direction == "up" else cfg.min_prob_down),
            direction=direction,
        )
        if direction == "up":
            selected.sort(
                key=lambda item: (
                    _rank_up(item) is None,
                    -(_rank_up(item) or 0.0),
                    item.get("mlEvShortNet") is None,
                    -(item.get("mlEvShortNet") or 0.0),
                    -(_prob_up_short(item) or 0.0),
                    item.get("code", ""),
                )
            )
        else:
            selected.sort(
                key=lambda item: (
                    _rank_down(item) is None,
                    -(_rank_down(item) or 0.0),
                    item.get("mlEvShortNet") is None,
                    (item.get("mlEvShortNet") or 0.0),
                    -(_prob_down_short(item) or 0.0),
                    item.get("code", ""),
                )
            )
        return selected[: min(limit, int(cfg.top_n))], pred_dt, model_version

    sign = 1.0 if direction == "up" else -1.0
    prob_min = float(cfg.min_prob_up if direction == "up" else cfg.min_prob_down)
    prob_gate = prob_min if direction == "up" else max(prob_min, _ENTRY_MIN_PROB_DOWN_STRICT)
    fallback_prob_gate = prob_gate if direction == "up" else max(prob_min, 0.52)
    rule_values = {
        str(item.get("code") or ""): (
            float(item["changePct"]) * sign
            if isinstance(item.get("changePct"), (int, float)) and math.isfinite(float(item["changePct"]))
            else None
        )
        for item in enriched
    }
    ev_values = {
        str(item.get("code") or ""): (
            float(item["mlEvShortNet"]) * sign
            if isinstance(item.get("mlEvShortNet"), (int, float)) and math.isfinite(float(item["mlEvShortNet"]))
            else None
        )
        for item in enriched
    }
    prob_values = {
        str(item.get("code") or ""): (
            _prob_up_short(item)
            if direction == "up"
            else _prob_down_short(item)
        )
        for item in enriched
    }
    rank_values = {
        str(item.get("code") or ""): (
            _rank_up(item)
            if direction == "up"
            else _rank_down(item)
        )
        for item in enriched
    }
    turn_values = {
        str(item.get("code") or ""): (
            _turn_up_short(item)
            if direction == "up"
            else _turn_down_short(item)
        )
        for item in enriched
    }
    turn_opp_values = {
        str(item.get("code") or ""): (
            _turn_down_short(item)
            if direction == "up"
            else _turn_up_short(item)
        )
        for item in enriched
    }
    turn_margin_values = {
        code: (
            (turn_values.get(code) - turn_opp_values.get(code))
            if isinstance(turn_values.get(code), (int, float)) and isinstance(turn_opp_values.get(code), (int, float))
            else None
        )
        for code in {str(item.get("code") or "") for item in enriched}
    }
    rule_rank = _percent_rank_desc(rule_values)
    ev_rank = _percent_rank_desc(ev_values)
    prob_rank = _percent_rank_desc(prob_values)
    rank_rank = _percent_rank_desc(rank_values)
    turn_rank = _percent_rank_desc(turn_values)
    turn_margin_rank = _percent_rank_desc(turn_margin_values)
    qualified: list[dict] = []
    fallback: list[dict] = []
    for item in enriched:
        code = str(item.get("code") or "")
        rr = rule_rank.get(code)
        er = ev_rank.get(code)
        pr = prob_rank.get(code)
        rkr = rank_rank.get(code)
        tr = turn_rank.get(code)
        tmr = turn_margin_rank.get(code)
        prob = prob_values.get(code)
        p_up_5d = _first_finite(item.get("mlPUp5"), item.get("mlPUpShort"), item.get("mlPUp"))
        p_up_10d = _first_finite(item.get("mlPUp10"), item.get("mlPUp"))
        p_up_20d = _first_finite(item.get("mlPUp"))
        p_down_5d = _first_finite(
            item.get("mlPDownShort"),
            (1.0 - p_up_5d) if p_up_5d is not None else None,
            item.get("mlPDown"),
        )
        p_down_10d = _first_finite(
            (1.0 - p_up_10d) if p_up_10d is not None else None,
            item.get("mlPDown"),
            item.get("mlPDownShort"),
        )
        p_down_20d = _first_finite(
            item.get("mlPDown"),
            (1.0 - p_up_20d) if p_up_20d is not None else None,
        )
        prob_5d = p_up_5d if direction == "up" else p_down_5d
        prob_10d = p_up_10d if direction == "up" else p_down_10d
        prob_20d = p_up_20d if direction == "up" else p_down_20d
        if direction == "up":
            prob_5d_gate = _ENTRY_MIN_PROB_UP_5D
            prob_5d_ok = bool(
                isinstance(prob_5d, (int, float))
                and math.isfinite(float(prob_5d))
                and float(prob_5d) >= prob_5d_gate
            )
            prob_curve_ok = _is_non_increasing_curve(prob_5d, prob_10d, prob_20d)
        else:
            prob_5d_ok = True
            prob_curve_ok = True
        horizon_ok = bool(prob_5d_ok and prob_curve_ok)
        if rr is None or er is None or pr is None:
            item["hybridScore"] = None
            item["entryScore"] = None
            continue
        base_score_raw = float(cfg.rule_weight * rr + cfg.ev_weight * er + cfg.prob_weight * pr)
        rank_weight = float(min(0.8, max(0.0, getattr(cfg, "rank_weight", 0.0))))
        base_score = (
            float((1.0 - rank_weight) * base_score_raw + rank_weight * rkr)
            if rkr is not None
            else base_score_raw
        )
        if mode == "turn":
            if tr is None or tmr is None:
                item["hybridScore"] = None
                item["entryScore"] = None
                continue
            if rkr is not None:
                item["hybridScore"] = float(0.55 * tr + 0.25 * tmr + 0.20 * rkr)
            else:
                item["hybridScore"] = float(0.65 * tr + 0.35 * tmr)
        else:
            turn_weight = float(min(0.7, max(0.0, getattr(cfg, "turn_weight", 0.0))))
            if tr is not None and mode == "hybrid":
                item["hybridScore"] = float((1.0 - turn_weight) * base_score + turn_weight * tr)
            else:
                item["hybridScore"] = base_score
        ev_net = item.get("mlEv20Net")
        ev_ok = (
            isinstance(ev_net, (int, float))
            and math.isfinite(float(ev_net))
            and (
                float(ev_net) >= _ENTRY_MIN_EV_NET_UP
                if direction == "up"
                else float(ev_net) <= _ENTRY_MAX_EV_NET_DOWN
            )
        )
        trend_ok = bool(item.get("trendUp")) if direction == "up" else bool(item.get("trendDownStrict"))
        dist_ma20 = item.get("distMa20")
        dist_ok = (
            isinstance(dist_ma20, (int, float))
            and math.isfinite(float(dist_ma20))
            and float(dist_ma20) <= _ENTRY_MAX_DIST_MA20
        )
        rule_signal = rule_values.get(code)
        rule_ok = bool(
            isinstance(rule_signal, (int, float))
            and math.isfinite(float(rule_signal))
            and (
                float(rule_signal) >= 0.0
                if direction == "up"
                else float(rule_signal) >= _ENTRY_MIN_RULE_SIGNAL_DOWN
            )
        )
        counter_move_ok = bool(
            isinstance(rule_signal, (int, float))
            and math.isfinite(float(rule_signal))
            and (
                True
                if direction == "up"
                else float(rule_signal) >= -_ENTRY_MAX_COUNTER_MOVE_DOWN
            )
        )
        turn_prob = turn_values.get(code)
        turn_opp = turn_opp_values.get(code)
        turn_gate = float(cfg.min_turn_prob_up if direction == "up" else cfg.min_turn_prob_down)
        turn_margin_gate = float(cfg.min_turn_margin)
        turn_ok = bool(
            isinstance(turn_prob, (int, float))
            and math.isfinite(float(turn_prob))
            and float(turn_prob) >= turn_gate
            and (
                not isinstance(turn_opp, (int, float))
                or not math.isfinite(float(turn_opp))
                or (float(turn_prob) - float(turn_opp)) >= turn_margin_gate
            )
        )
        candle_prob = _first_finite(
            item.get("candleTripletUp") if direction == "up" else item.get("candleTripletDown")
        )
        weekly_breakout_prob = _first_finite(
            item.get("weeklyBreakoutUpProb") if direction == "up" else item.get("weeklyBreakoutDownProb")
        )
        monthly_breakout_prob = _first_finite(
            item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
        )
        monthly_range_prob = _first_finite(item.get("monthlyRangeProb"))
        monthly_range_pos = _first_finite(item.get("monthlyRangePos"))
        shooting_star_like = _first_finite(item.get("shootingStarLike"))
        three_white_soldiers = _first_finite(item.get("threeWhiteSoldiers"))
        bull_engulfing = _first_finite(item.get("bullEngulfing"))
        v60_strong = _first_finite(item.get("v60Strong"))
        trend_strict_ok = bool(item.get("trendUpStrict")) if direction == "up" else bool(item.get("trendDownStrict"))
        mtf_strong_alignment = bool(
            trend_strict_ok
            and weekly_breakout_prob is not None
            and weekly_breakout_prob >= 0.56
            and monthly_breakout_prob is not None
            and monthly_breakout_prob >= 0.60
        )
        box_bottom_ok = bool(
            monthly_range_prob is not None
            and monthly_range_pos is not None
            and monthly_range_prob >= 0.62
            and (
                (direction == "up" and monthly_range_pos <= 0.38)
                or (direction == "down" and monthly_range_pos >= 0.62)
            )
        )
        candle_shape_bonus = 0.0
        if direction == "up":
            if shooting_star_like is not None and shooting_star_like >= 0.5:
                candle_shape_bonus += _ENTRY_BONUS_CANDLE_PATTERN
            if three_white_soldiers is not None and three_white_soldiers >= 0.5:
                candle_shape_bonus += _ENTRY_BONUS_CANDLE_PATTERN
            if bull_engulfing is not None and bull_engulfing >= 0.5:
                candle_shape_bonus += _ENTRY_BONUS_CANDLE_PATTERN
        bonus = 0.0
        if trend_ok:
            bonus += 0.08
        if trend_strict_ok:
            bonus += _ENTRY_BONUS_STRICT_STACK
        if ev_ok:
            bonus += 0.05
        if prob is not None and prob >= (prob_gate + 0.03):
            bonus += 0.04
        if dist_ok:
            bonus += 0.03
        if rule_ok:
            bonus += 0.03
        if turn_ok:
            bonus += 0.07
        if prob is not None and prob >= (prob_gate + 0.08):
            bonus += 0.03
        if candle_prob is not None and candle_prob >= 0.58:
            bonus += 0.03
        if weekly_breakout_prob is not None and weekly_breakout_prob >= 0.56:
            bonus += 0.03
        if monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60:
            bonus += 0.05
        if mtf_strong_alignment:
            bonus += _ENTRY_BONUS_MTF_SYNERGY
        if box_bottom_ok:
            bonus += _ENTRY_BONUS_BOX_BOTTOM
        bonus += candle_shape_bonus
        if direction == "up" and v60_strong is not None and v60_strong >= 0.5:
            bonus -= _ENTRY_PENALTY_60V_STRONG
        if (
            monthly_range_prob is not None
            and monthly_range_prob >= 0.68
            and (monthly_breakout_prob is None or monthly_breakout_prob < 0.55)
        ):
            bonus -= 0.02
        item["entryScore"] = float((item.get("hybridScore") or 0.0) + bonus)
        item["evAligned"] = bool(ev_ok)
        item["trendAligned"] = bool(trend_ok)
        item["distOk"] = bool(dist_ok)
        item["ruleAligned"] = bool(rule_ok)
        item["counterMoveOk"] = bool(counter_move_ok)
        item["turnAligned"] = bool(turn_ok)
        item["candleAligned"] = bool(candle_prob is not None and candle_prob >= 0.58)
        item["trendStrictAligned"] = bool(trend_strict_ok)
        item["mtfStrongAligned"] = bool(mtf_strong_alignment)
        item["boxBottomAligned"] = bool(box_bottom_ok)
        item["candlestickPatternBonus"] = float(candle_shape_bonus)
        item["v60StrongPenalty"] = bool(direction == "up" and v60_strong is not None and v60_strong >= 0.5)
        item["weeklyRegimeAligned"] = bool(
            weekly_breakout_prob is not None and weekly_breakout_prob >= 0.56
        )
        item["monthlyRegimeAligned"] = bool(
            monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60
        )
        item["probSide"] = float(prob) if prob is not None else None
        item["prob5d"] = float(prob_5d) if isinstance(prob_5d, (int, float)) and math.isfinite(float(prob_5d)) else None
        item["prob10d"] = float(prob_10d) if isinstance(prob_10d, (int, float)) and math.isfinite(float(prob_10d)) else None
        item["prob20d"] = float(prob_20d) if isinstance(prob_20d, (int, float)) and math.isfinite(float(prob_20d)) else None
        item["prob5dAligned"] = bool(prob_5d_ok)
        item["probCurveAligned"] = bool(prob_curve_ok)
        item["horizonAligned"] = bool(horizon_ok)
        if mode == "turn":
            item["entryQualified"] = bool(turn_ok and dist_ok and counter_move_ok and horizon_ok)
        else:
            trend_path_ok = bool(
                prob is not None
                and prob >= prob_gate
                and ev_ok
                and trend_ok
            )
            item["entryQualified"] = bool(
                (trend_path_ok or turn_ok)
                and dist_ok
                and (rule_ok if direction == "up" else counter_move_ok)
                and horizon_ok
            )
        if item["entryQualified"]:
            qualified.append(item)
        else:
            fallback.append(item)

    qualified.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("probSide") or 0.0),
            item.get("code", ""),
        )
    )
    if len(qualified) >= limit:
        return qualified[:limit], pred_dt, model_version
    strict_fallback = [
        item
        for item in fallback
        if bool(item.get("evAligned"))
        and (bool(item.get("trendAligned")) or bool(item.get("turnAligned")))
        and bool(item.get("distOk"))
        and bool(item.get("counterMoveOk"))
        and bool(item.get("horizonAligned"))
        and (
            mode == "turn"
            or (
                isinstance(item.get("probSide"), (int, float))
                and float(item.get("probSide") or 0.0) >= fallback_prob_gate
            )
        )
    ]
    strict_fallback.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("probSide") or 0.0),
            item.get("code", ""),
        )
    )
    min_return = min(limit, 12)
    if len(qualified) < min_return:
        selected = qualified + strict_fallback[: max(0, min_return - len(qualified))]
        return selected[:limit], pred_dt, model_version
    fallback.sort(
        key=lambda item: (
            not bool(item.get("trendUp")) if direction == "up" else not bool(item.get("trendDown")),
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("probSide") or 0.0),
            item.get("code", ""),
        )
    )
    selected = qualified + strict_fallback
    return selected[:limit], pred_dt, model_version


def refresh_cache() -> None:
    global _CACHE, _LAST_UPDATED
    with _LOCK:
        _CACHE = _build_cache()
        _LAST_UPDATED = datetime.now(timezone.utc)


def get_rankings(
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    limit: int,
    *,
    mode: RankMode = "hybrid",
) -> dict:
    with _LOCK:
        items = _CACHE.get((tf, which, direction))
        last_updated = _LAST_UPDATED
    if items is None:
        refresh_cache()
        with _LOCK:
            items = _CACHE.get((tf, which, direction), [])
            last_updated = _LAST_UPDATED

    limit = max(1, min(int(limit or 50), 200))
    pred_dt = None
    model_version = None
    if mode == "rule":
        out_items = items[:limit]
    elif tf == "M" and mode == "hybrid":
        out_items, pred_dt, model_version = _apply_monthly_ml_mode(
            items,
            direction=direction,
            limit=limit,
        )
    else:
        out_items, pred_dt, model_version = _apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
        )
    out_items = [_sanitize_rank_item_for_json(item) for item in out_items]

    try:
        top_items = out_items[:10] if out_items else []
        log_payload = {
            "tag": "rank_request",
            "tf": tf,
            "which": which,
            "direction": direction,
            "mode": mode,
            "limit": limit,
            "pred_dt": pred_dt,
            "model_version": model_version,
            "anchor_date_list": [item.get("asOf") for item in top_items],
            "top": [
                {
                    "code": item.get("code"),
                    "target_dt": item.get("asOf"),
                    "changePct": item.get("changePct"),
                    "mlEv20Net": item.get("mlEv20Net"),
                    "hybridScore": item.get("hybridScore"),
                    "candleTripletUp": item.get("candleTripletUp"),
                    "candleTripletDown": item.get("candleTripletDown"),
                    "monthlyBreakoutUpProb": item.get("monthlyBreakoutUpProb"),
                    "monthlyBreakoutDownProb": item.get("monthlyBreakoutDownProb"),
                    "monthlyRangeProb": item.get("monthlyRangeProb"),
                }
                for item in top_items
            ],
        }
        print(json.dumps(log_payload, ensure_ascii=False))
    except Exception as exc:
        logger.debug("rank_request debug logging failed: %s", exc)
    return {
        "tf": tf,
        "which": which,
        "dir": direction,
        "mode": mode,
        "pred_dt": pred_dt,
        "model_version": model_version,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "items": out_items,
    }


def get_rankings_asof(
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    limit: int,
    *,
    as_of: str | int,
    mode: RankMode = "hybrid",
) -> dict:
    as_of_int = _coerce_as_of_int(as_of)
    if as_of_int is None:
        raise ValueError("as_of must be YYYY-MM-DD or YYYYMMDD")

    with duckdb.connect(str(core_config.DB_PATH)) as conn:
        cache = _build_cache_asof(conn, as_of_int)

    items = cache.get((tf, which, direction), [])
    limit = max(1, min(int(limit or 50), 200))

    pred_dt = None
    model_version = None
    if mode == "rule" or not items:
        out_items = items[:limit]
    elif tf == "M" and mode == "hybrid":
        out_items, pred_dt, model_version = _apply_monthly_ml_mode(
            items,
            direction=direction,
            limit=limit,
        )
    else:
        out_items, pred_dt, model_version = _apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
        )

    if pred_dt is not None:
        pred_key = pred_dt
        if pred_key >= 1_000_000_000:
            try:
                pred_key = int(datetime.fromtimestamp(pred_key, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                pred_key = as_of_int
        if pred_key > as_of_int:
            pred_dt = None
            model_version = None
            out_items = items[:limit]

    filtered: list[dict] = []
    for item in out_items:
        key = _iso_date_to_int(item.get("asOf"))
        if key is not None and key > as_of_int:
            continue
        filtered.append(_sanitize_rank_item_for_json(item))

    return {
        "tf": tf,
        "which": which,
        "dir": direction,
        "mode": mode,
        "requested_as_of": f"{as_of_int:08d}",
        "pred_dt": pred_dt,
        "model_version": model_version,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "items": filtered[:limit],
    }
