from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from threading import Lock
from typing import Literal

import duckdb

from app.core.config import config as core_config
from app.backend.domain.screening.metrics import _calc_liquidity_20d

RankTimeframe = Literal["D", "W", "M"]
RankWhich = Literal["latest", "prev"]
RankDir = Literal["up", "down"]

_CACHE: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
_LAST_UPDATED: datetime | None = None
_LOCK = Lock()

_DAILY_LIMIT = 120
_MONTHLY_LIMIT = 6


def _parse_date_value(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw >= 1_000_000_000:
        # Unix seconds
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
        week_start = (dt.date() - timedelta(days=dt.weekday()))
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
    last_week_start = (last_daily.date() - timedelta(days=last_daily.weekday()))
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

    import math

    return sorted(items, key=_sort_key)


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
        code = row[0]
        daily_map.setdefault(code, []).append(row[1:])

    monthly_map: dict[str, list[tuple]] = {}
    for row in monthly_rows:
        code = row[0]
        monthly_map.setdefault(code, []).append(row[1:])

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

        # Daily bars (close only)
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
                }
            )

        # Weekly bars
        weekly = _build_weekly_bars(daily)
        weekly = _drop_incomplete_weekly(weekly, last_daily_dt)
        weekly_closes = [float(item["c"]) for item in weekly]
        weekly_dates = [item["last_date"] for item in weekly]
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
                }
            )

        # Monthly bars
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
                }
            )

    cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
    for tf in ("D", "W", "M"):
        for which in ("latest", "prev"):
            items = items_by_tf[(tf, which)]
            for direction in ("up", "down"):
                cache[(tf, which, direction)] = _sort_items(items, direction)
    return cache


def refresh_cache() -> None:
    global _CACHE, _LAST_UPDATED
    with _LOCK:
        _CACHE = _build_cache()
        _LAST_UPDATED = datetime.now(timezone.utc)


def get_rankings(tf: RankTimeframe, which: RankWhich, direction: RankDir, limit: int) -> dict:
    with _LOCK:
        items = _CACHE.get((tf, which, direction))
        last_updated = _LAST_UPDATED
    if items is None:
        refresh_cache()
        with _LOCK:
            items = _CACHE.get((tf, which, direction), [])
            last_updated = _LAST_UPDATED
    try:
        tf_label_map = {"D": "日", "W": "週", "M": "月"}
        which_label_map = {
            "D": {"latest": "当日", "prev": "前日"},
            "W": {"latest": "今週", "prev": "前週"},
            "M": {"latest": "今月", "prev": "前月"},
        }
        direction_label = "上昇" if direction == "up" else "下落"
        top_items = items[:10] if items else []
        log_payload = {
            "tag": "rank_request",
            "tf": tf,
            "timeframe_kind": tf_label_map.get(tf, tf),
            "timeframe": which_label_map.get(tf, {}).get(which, which),
            "which": which,
            "direction": direction_label,
            "limit": limit,
            "bars": None,
            "bars_note": "not_provided_to_/rankings",
            "anchor_date_list": [item.get("asOf") for item in top_items],
            "top": [
                {
                    "code": item.get("code"),
                    "target_dt": item.get("asOf"),
                    "close": item.get("close"),
                    "changePct": item.get("changePct"),
                }
                for item in top_items
            ],
        }
        print(json.dumps(log_payload, ensure_ascii=False))
    except Exception:
        pass
    return {
        "tf": tf,
        "which": which,
        "dir": direction,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "items": items[: max(1, min(int(limit or 50), 200))],
    }
