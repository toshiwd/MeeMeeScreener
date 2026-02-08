from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import logging
from threading import Lock
from typing import Literal

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

_DAILY_LIMIT = 120
_MONTHLY_LIMIT = 6
_ENTRY_MIN_EV_NET_UP = 0.003
_ENTRY_MAX_EV_NET_DOWN = 0.005
_ENTRY_MAX_DIST_MA20 = 0.12
_ENTRY_MIN_PROB_DOWN_STRICT = 0.56
_ENTRY_MIN_RULE_SIGNAL_DOWN = 0.002
_ENTRY_MAX_COUNTER_MOVE_DOWN = 0.01


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
    turn_up_expr = "p_turn_up" if "p_turn_up" in names else "NULL AS p_turn_up"
    turn_down_expr = "p_turn_down" if "p_turn_down" in names else "NULL AS p_turn_down"
    rows = conn.execute(
        f"""
        SELECT code, p_up, {turn_up_expr}, {turn_down_expr}, ret_pred20, ev20, ev20_net, model_version
        FROM ml_pred_20d
        WHERE dt = ?
        """,
        [pred_dt],
    ).fetchall()
    pred_map = {
        str(row[0]): {
            "p_up": float(row[1]) if row[1] is not None else None,
            "p_turn_up": float(row[2]) if row[2] is not None else None,
            "p_turn_down": float(row[3]) if row[3] is not None else None,
            "ret_pred20": float(row[4]) if row[4] is not None else None,
            "ev20": float(row[5]) if row[5] is not None else None,
            "ev20_net": float(row[6]) if row[6] is not None else None,
            "model_version": row[7],
        }
        for row in rows
    }
    model_version = None
    for item in pred_map.values():
        model_version = item.get("model_version")
        if model_version:
            break
    return pred_map, model_version


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
        enriched.append(
            {
                **item,
                "mlPUp": pred.get("p_up"),
                "mlPTurnUp": pred.get("p_turn_up"),
                "mlPTurnDown": pred.get("p_turn_down"),
                "mlRetPred20": pred.get("ret_pred20"),
                "mlEv20": pred.get("ev20"),
                "mlEv20Net": pred.get("ev20_net"),
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

    if mode == "ml":
        selected = select_top_n_ml(
            enriched,
            top_n=int(cfg.top_n),
            p_up_threshold=float(cfg.p_up_threshold),
            direction=direction,
        )
        if direction == "up":
            selected.sort(
                key=lambda item: (
                    item.get("mlEv20Net") is None,
                    -(item.get("mlEv20Net") or 0.0),
                    item.get("code", ""),
                )
            )
        else:
            selected.sort(
                key=lambda item: (
                    item.get("mlEv20Net") is None,
                    (item.get("mlEv20Net") or 0.0),
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
            float(item["mlEv20Net"]) * sign
            if isinstance(item.get("mlEv20Net"), (int, float)) and math.isfinite(float(item["mlEv20Net"]))
            else None
        )
        for item in enriched
    }
    prob_values = {
        str(item.get("code") or ""): (
            float(item["mlPUp"])
            if direction == "up"
            else 1.0 - float(item["mlPUp"])
        )
        if isinstance(item.get("mlPUp"), (int, float)) and math.isfinite(float(item["mlPUp"]))
        else None
        for item in enriched
    }
    turn_values = {
        str(item.get("code") or ""): (
            float(item["mlPTurnUp"])
            if direction == "up"
            else float(item["mlPTurnDown"])
        )
        if (
            isinstance(item.get("mlPTurnUp"), (int, float))
            and isinstance(item.get("mlPTurnDown"), (int, float))
            and math.isfinite(float(item["mlPTurnUp"]))
            and math.isfinite(float(item["mlPTurnDown"]))
        )
        else None
        for item in enriched
    }
    turn_opp_values = {
        str(item.get("code") or ""): (
            float(item["mlPTurnDown"])
            if direction == "up"
            else float(item["mlPTurnUp"])
        )
        if (
            isinstance(item.get("mlPTurnUp"), (int, float))
            and isinstance(item.get("mlPTurnDown"), (int, float))
            and math.isfinite(float(item["mlPTurnUp"]))
            and math.isfinite(float(item["mlPTurnDown"]))
        )
        else None
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
    turn_rank = _percent_rank_desc(turn_values)
    turn_margin_rank = _percent_rank_desc(turn_margin_values)
    qualified: list[dict] = []
    fallback: list[dict] = []
    for item in enriched:
        code = str(item.get("code") or "")
        rr = rule_rank.get(code)
        er = ev_rank.get(code)
        pr = prob_rank.get(code)
        tr = turn_rank.get(code)
        tmr = turn_margin_rank.get(code)
        prob = prob_values.get(code)
        if rr is None or er is None or pr is None:
            item["hybridScore"] = None
            item["entryScore"] = None
            continue
        base_score = float(cfg.rule_weight * rr + cfg.ev_weight * er + cfg.prob_weight * pr)
        if mode == "turn":
            if tr is None or tmr is None:
                item["hybridScore"] = None
                item["entryScore"] = None
                continue
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
        bonus = 0.0
        if trend_ok:
            bonus += 0.08
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
        item["entryScore"] = float((item.get("hybridScore") or 0.0) + bonus)
        item["evAligned"] = bool(ev_ok)
        item["trendAligned"] = bool(trend_ok)
        item["distOk"] = bool(dist_ok)
        item["ruleAligned"] = bool(rule_ok)
        item["counterMoveOk"] = bool(counter_move_ok)
        item["turnAligned"] = bool(turn_ok)
        item["probSide"] = float(prob) if prob is not None else None
        if mode == "turn":
            item["entryQualified"] = bool(turn_ok and dist_ok and counter_move_ok)
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
            )
        if item["entryQualified"]:
            qualified.append(item)
        else:
            fallback.append(item)

    qualified.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
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
    else:
        out_items, pred_dt, model_version = _apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
        )

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
