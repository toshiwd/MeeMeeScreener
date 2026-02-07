from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
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
RankMode = Literal["rule", "ml", "hybrid"]

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
    rows = conn.execute(
        """
        SELECT code, p_up, ret_pred20, ev20, ev20_net, model_version
        FROM ml_pred_20d
        WHERE dt = ?
        """,
        [pred_dt],
    ).fetchall()
    pred_map = {
        str(row[0]): {
            "p_up": float(row[1]) if row[1] is not None else None,
            "ret_pred20": float(row[2]) if row[2] is not None else None,
            "ev20": float(row[3]) if row[3] is not None else None,
            "ev20_net": float(row[4]) if row[4] is not None else None,
            "model_version": row[5],
        }
        for row in rows
    }
    model_version = None
    for item in pred_map.values():
        model_version = item.get("model_version")
        if model_version:
            break
    return pred_map, model_version


def _decorate_items_with_ml(items: list[dict], pred_map: dict[str, dict]) -> list[dict]:
    enriched: list[dict] = []
    for item in items:
        code = str(item.get("code") or "")
        pred = pred_map.get(code) or {}
        enriched.append(
            {
                **item,
                "mlPUp": pred.get("p_up"),
                "mlRetPred20": pred.get("ret_pred20"),
                "mlEv20": pred.get("ev20"),
                "mlEv20Net": pred.get("ev20_net"),
                "modelVersion": pred.get("model_version"),
                "hybridScore": None,
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
    except Exception:
        return items[:limit], None, None

    cfg = load_ml_config()
    enriched = _decorate_items_with_ml(items, pred_map)

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
    rule_rank = _percent_rank_desc(rule_values)
    ev_rank = _percent_rank_desc(ev_values)
    prob_rank = _percent_rank_desc(prob_values)
    qualified: list[dict] = []
    fallback: list[dict] = []
    for item in enriched:
        code = str(item.get("code") or "")
        rr = rule_rank.get(code)
        er = ev_rank.get(code)
        pr = prob_rank.get(code)
        prob = prob_values.get(code)
        if rr is None or er is None or pr is None:
            item["hybridScore"] = None
            continue
        item["hybridScore"] = float(
            cfg.rule_weight * rr + cfg.ev_weight * er + cfg.prob_weight * pr
        )
        if prob is not None and prob >= prob_min:
            qualified.append(item)
        else:
            fallback.append(item)

    qualified.sort(
        key=lambda item: (
            item.get("hybridScore") is None,
            -(item.get("hybridScore") or 0.0),
            item.get("code", ""),
        )
    )
    if len(qualified) >= limit:
        return qualified[:limit], pred_dt, model_version
    fallback.sort(
        key=lambda item: (
            item.get("hybridScore") is None,
            -(item.get("hybridScore") or 0.0),
            item.get("code", ""),
        )
    )
    selected = qualified + fallback[: max(0, limit - len(qualified))]
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
    except Exception:
        pass
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
