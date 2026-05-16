from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

import duckdb

from app.backend.services import yahoo_provisional
from app.services import position_calc, trade_events
from shared.contracts.holding_review import HoldingReviewBundle, HoldingReviewResponse

SCHEMA_VERSION = "meemee_holding_review_bundle_v1"
TOP_RANK_LIMIT = 50
MA_WINDOWS = (7, 20, 60, 100, 200)


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
    except Exception:
        return set()


def _has_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _first_existing(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    return next((column for column in candidates if column in columns), None)


def _ymd_to_iso(value: Any) -> str | None:
    key = yahoo_provisional.normalize_date_key(value)
    if key is None:
        return None
    text = str(key)
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _timestamp_to_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    parsed = _ymd_to_iso(text)
    if parsed:
        return parsed
    return text[:10]


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "buy", "qualified", "accept"}


def _safe_div(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _holding_codes(conn: duckdb.DuckDBPyConnection, requested_code: str | None) -> list[str]:
    if requested_code:
        return [str(requested_code)]
    if not _has_table(conn, "positions_live"):
        return []
    columns = _table_columns(conn, "positions_live")
    symbol_col = _first_existing(columns, ("symbol", "code"))
    buy_col = _first_existing(columns, ("buy_qty", "long_qty", "spot_qty", "margin_long_qty"))
    sell_col = _first_existing(columns, ("sell_qty", "short_qty", "margin_short_qty"))
    if not symbol_col:
        return []
    predicates: list[str] = []
    if buy_col:
        predicates.append(f"COALESCE({buy_col}, 0) > 0")
    if sell_col:
        predicates.append(f"COALESCE({sell_col}, 0) > 0")
    where_sql = " WHERE " + " OR ".join(predicates) if predicates else ""
    rows = conn.execute(f"SELECT DISTINCT {symbol_col} FROM positions_live{where_sql} ORDER BY {symbol_col}").fetchall()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _latest_trade(events: list[dict[str, Any]], *, side: str, action: str) -> dict[str, Any] | None:
    candidates = [event for event in events if event.get("side") == side and event.get("action") == action]
    if not candidates:
        return None
    latest = sorted(candidates, key=lambda event: str(event.get("exec_dt") or event.get("date") or ""))[-1]
    return {
        "date": latest.get("date"),
        "qty": latest.get("qtyShares"),
        "price": latest.get("price"),
    }


def _position_payload(
    conn: duckdb.DuckDBPyConnection,
    code: str,
    provisional_last: float | None,
) -> tuple[dict[str, Any], list[str]]:
    missing: list[str] = []
    db_events = trade_events.get_events(conn, [code]) if _has_table(conn, "trade_events") else []
    events = [position_calc._map_trade_event(event) for event in db_events]
    metrics = position_calc._calc_position_metrics(events)
    long_lots = float(metrics["longLots"] or 0)
    short_lots = float(metrics["shortLots"] or 0)
    long_qty = long_lots * 100
    short_qty = short_lots * 100
    avg_long = _to_float(metrics["avgLongPrice"])
    avg_short = _to_float(metrics["avgShortPrice"])
    unrealized: float | None = None
    if provisional_last is not None:
        unrealized = 0.0
        if avg_long is not None:
            unrealized += (provisional_last - avg_long) * long_qty
        if avg_short is not None:
            unrealized += (avg_short - provisional_last) * short_qty
    else:
        missing.append("provisional_last")
    if not events:
        missing.append("trade_events")
    return (
        {
            "long_qty": long_qty,
            "short_qty": short_qty,
            "avg_long_price": avg_long,
            "avg_short_price": avg_short,
            "hedge_ratio": _safe_div(short_qty, long_qty) if long_qty else None,
            "latest_long_trade": _latest_trade(events, side="buy", action="open"),
            "latest_short_trade": _latest_trade(events, side="sell", action="open"),
            "unrealized_pnl_using_provisional": unrealized,
            "realized_pnl_current_round": metrics.get("realizedPnL"),
        },
        missing,
    )


def _confirmed_bar(conn: duckdb.DuckDBPyConnection, code: str) -> tuple[dict[str, Any] | None, list[str]]:
    if not _has_table(conn, "daily_bars"):
        return None, ["daily_bars"]
    columns = _table_columns(conn, "daily_bars")
    date_col = _first_existing(columns, ("date", "dt", "ymd", "trade_date"))
    if not date_col:
        return None, ["daily_bars.date"]
    select = [date_col]
    for name in ("o", "open", "h", "high", "l", "low", "c", "close", "v", "volume", "source"):
        if name in columns and name not in select:
            select.append(name)
    row = conn.execute(
        f"SELECT {', '.join(select)} FROM daily_bars WHERE code = ? ORDER BY {date_col} DESC LIMIT 1",
        [code],
    ).fetchone()
    if not row:
        return None, ["confirmed_bar"]
    values = dict(zip(select, row))
    return (
        {
            "date": _ymd_to_iso(values.get(date_col)),
            "open": _to_float(values.get("o", values.get("open"))),
            "high": _to_float(values.get("h", values.get("high"))),
            "low": _to_float(values.get("l", values.get("low"))),
            "close": _to_float(values.get("c", values.get("close"))),
            "volume": _to_float(values.get("v", values.get("volume"))),
            "ma7": None,
            "ma20": None,
            "ma60": None,
        },
        [],
    )


def _merge_feature_snapshot(conn: duckdb.DuckDBPyConnection, code: str, bar: dict[str, Any] | None) -> list[str]:
    if bar is None or not _has_table(conn, "feature_snapshot_daily"):
        return ["feature_snapshot_daily"]
    columns = _table_columns(conn, "feature_snapshot_daily")
    date_col = _first_existing(columns, ("date", "dt", "ymd", "trade_date"))
    if not date_col:
        return ["feature_snapshot_daily.date"]
    select = [date_col] + [column for column in ("ma7", "ma20", "ma60", "ma100", "ma200", "cnt_20_above", "cnt_7_above", "candle_flags") if column in columns]
    if len(select) == 1:
        return ["feature_snapshot_daily.ma"]
    row = conn.execute(
        f"SELECT {', '.join(select)} FROM feature_snapshot_daily WHERE code = ? ORDER BY {date_col} DESC LIMIT 1",
        [code],
    ).fetchone()
    if not row:
        return ["feature_snapshot_daily"]
    values = dict(zip(select, row))
    for key in ("ma7", "ma20", "ma60", "ma100", "ma200", "cnt_20_above", "cnt_7_above"):
        if key in values:
            bar[key] = _to_float(values[key])
    if "candle_flags" in values:
        bar["candle_flags"] = values["candle_flags"]
    return []


def _moving_average(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return sum(values[-window:]) / float(window)


def _zone(close: float | None, low: float | None, high: float | None) -> str:
    if close is None or low is None or high is None or high <= low:
        return "unknown"
    pct = (close - low) / (high - low)
    if pct >= 0.75:
        return "near_high_zone"
    if pct <= 0.25:
        return "support_zone"
    return "middle_zone"


def _candle_label(row: dict[str, Any], prev_close: float | None) -> str:
    open_price = _to_float(row.get("open"))
    high = _to_float(row.get("high"))
    low = _to_float(row.get("low"))
    close = _to_float(row.get("close"))
    if open_price is None or high is None or low is None or close is None:
        return "unknown"
    span = max(0.0001, high - low)
    body = abs(close - open_price)
    labels: list[str] = []
    if prev_close and open_price > prev_close * 1.01:
        labels.append("gap_up")
    if prev_close and open_price < prev_close * 0.99:
        labels.append("gap_down")
    if (high - max(open_price, close)) / span >= 0.45:
        labels.append("upper_wick")
    if (min(open_price, close) - low) / span >= 0.45:
        labels.append("lower_wick")
    if body / span >= 0.65:
        labels.append("large_body_up" if close >= open_price else "large_body_down")
    return "+".join(labels) if labels else ("up" if close >= open_price else "down")


def _daily_rows(conn: duckdb.DuckDBPyConnection, code: str, limit: int = 260) -> tuple[list[dict[str, Any]], list[str]]:
    if not _has_table(conn, "daily_bars"):
        return [], ["daily_bars"]
    columns = _table_columns(conn, "daily_bars")
    date_col = _first_existing(columns, ("date", "dt", "ymd", "trade_date"))
    open_col = _first_existing(columns, ("o", "open"))
    high_col = _first_existing(columns, ("h", "high"))
    low_col = _first_existing(columns, ("l", "low"))
    close_col = _first_existing(columns, ("c", "close"))
    volume_col = _first_existing(columns, ("v", "volume"))
    if not date_col:
        return [], ["daily_bars.date"]
    if not open_col or not high_col or not low_col or not close_col:
        return [], ["daily_bars.ohlc"]
    select = [date_col, open_col, high_col, low_col, close_col] + ([volume_col] if volume_col else [])
    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM daily_bars WHERE code = ? ORDER BY {date_col} DESC LIMIT ?",
        [code, limit],
    ).fetchall()
    parsed: list[dict[str, Any]] = []
    for row in reversed(rows):
        values = dict(zip(select, row))
        parsed.append(
            {
                "date": _ymd_to_iso(values.get(date_col)),
                "open": _to_float(values.get(open_col)),
                "high": _to_float(values.get(high_col)),
                "low": _to_float(values.get(low_col)),
                "close": _to_float(values.get(close_col)),
                "volume": _to_float(values.get(volume_col)) if volume_col else None,
            }
        )
    return parsed, []


def _resample_period(rows: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        try:
            day = date.fromisoformat(str(row.get("date") or ""))
        except ValueError:
            continue
        key = f"{day.isocalendar().year}-W{day.isocalendar().week:02d}" if period == "week" else f"{day.year}-{day.month:02d}"
        buckets.setdefault(key, []).append(row)
    result: list[dict[str, Any]] = []
    for key in sorted(buckets):
        bucket = buckets[key]
        result.append(
            {
                "period": key,
                "open": bucket[0].get("open"),
                "high": max(_to_float(row.get("high")) or 0 for row in bucket),
                "low": min(_to_float(row.get("low")) or 0 for row in bucket),
                "close": bucket[-1].get("close"),
                "volume": sum(_to_float(row.get("volume")) or 0 for row in bucket),
            }
        )
    return result


def _chart_context(conn: duckdb.DuckDBPyConnection, code: str, confirmed: dict[str, Any] | None, provisional: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    rows, missing = _daily_rows(conn, code)
    if not rows:
        return {"short_term_price_state": "unknown", "chart_structure_state": "unknown", "position_risk_state": "unknown", "daily_6m_context": {}, "weekly_context": {}, "monthly_context": {}}, missing
    closes = [_to_float(row.get("close")) for row in rows if _to_float(row.get("close")) is not None]
    close = _to_float(rows[-1].get("close"))
    ma = {f"ma{window}": _moving_average(closes, window) for window in MA_WINDOWS}
    for key in ma:
        if confirmed and confirmed.get(key) is not None:
            ma[key] = _to_float(confirmed.get(key))
    high20 = max((_to_float(row.get("high")) or 0) for row in rows[-20:])
    low20 = min((_to_float(row.get("low")) or 0) for row in rows[-20:])
    high60 = max((_to_float(row.get("high")) or 0) for row in rows[-60:])
    low60 = min((_to_float(row.get("low")) or 0) for row in rows[-60:])
    above_ma = {key: bool(close is not None and value is not None and close > value) for key, value in ma.items()}
    above_count = sum(1 for value in above_ma.values() if value)
    price_zone = _zone(close, low60, high60)
    latest_open = _to_float(rows[-1].get("open"))
    if above_count >= 4 and price_zone == "near_high_zone" and latest_open is not None and close is not None and close < latest_open:
        move_state = "high_zone_failure"
    elif above_count >= 4:
        move_state = "uptrend_continuation"
    elif above_count >= 3:
        move_state = "pullback_within_uptrend"
    elif above_count <= 1 and ma.get("ma60") is not None and close is not None and close < ma["ma60"]:
        move_state = "breakdown"
    else:
        move_state = "range_middle"
    candle_sequence = []
    prev_close: float | None = None
    for row in rows[-10:]:
        candle_sequence.append({"date": row.get("date"), "label": _candle_label(row, prev_close), "close": row.get("close")})
        prev_close = _to_float(row.get("close"))
    weekly = _resample_period(rows, "week")
    monthly = _resample_period(rows, "month")
    weekly_closes = [_to_float(row.get("close")) for row in weekly if _to_float(row.get("close")) is not None]
    monthly_closes = [_to_float(row.get("close")) for row in monthly if _to_float(row.get("close")) is not None]
    weekly_ma4 = _moving_average(weekly_closes, 4)
    weekly_ma13 = _moving_average(weekly_closes, 13)
    monthly_ma3 = _moving_average(monthly_closes, 3)
    monthly_ma6 = _moving_average(monthly_closes, 6)
    weekly_close = weekly_closes[-1] if weekly_closes else None
    monthly_close = monthly_closes[-1] if monthly_closes else None
    weekly_supports = bool(weekly_close is not None and (weekly_ma4 is None or weekly_close >= weekly_ma4) and (weekly_ma13 is None or weekly_close >= weekly_ma13))
    monthly_above = bool(monthly_close is not None and (monthly_ma3 is None or monthly_close >= monthly_ma3) and (monthly_ma6 is None or monthly_close >= monthly_ma6))
    structurally_alive = above_count >= 3 and move_state != "breakdown" and weekly_supports
    return (
        {
            "short_term_price_state": str((provisional or {}).get("candle_label") or "unknown"),
            "chart_structure_state": "structurally_alive" if structurally_alive else "structure_warning",
            "position_risk_state": "requires_position_overlay",
            "daily_6m_context": {
                "latest_close": close,
                "latest_close_source": "confirmed",
                "provisional_last": _to_float((provisional or {}).get("last")),
                "ma": ma,
                "above_ma": above_ma,
                "above_ma_count": above_count,
                "cnt_20_above": (confirmed or {}).get("cnt_20_above"),
                "cnt_7_above": (confirmed or {}).get("cnt_7_above"),
                "recent_20d_high": high20,
                "recent_20d_low": low20,
                "recent_60d_high": high60,
                "recent_60d_low": low60,
                "price_zone": price_zone,
                "move_state": move_state,
                "recent_candle_sequence": candle_sequence,
            },
            "weekly_context": {"trend_direction": "up" if weekly_supports else "warning", "latest_close": weekly_close, "ma4": weekly_ma4, "ma13": weekly_ma13, "supports_holding": weekly_supports},
            "monthly_context": {"monthly_regime": "above_major_averages" if monthly_above else "mixed_or_below_major_averages", "latest_close": monthly_close, "ma3": monthly_ma3, "ma6": monthly_ma6, "near_monthly_resistance": _zone(monthly_close, min(monthly_closes[-6:]) if monthly_closes else None, max(monthly_closes[-6:]) if monthly_closes else None) == "near_high_zone"},
        },
        [],
    )


def _provisional_bar(code: str, confirmed: dict[str, Any] | None) -> tuple[dict[str, Any] | None, list[str]]:
    row = yahoo_provisional.get_provisional_daily_rows_from_spark(
        [code],
        allow_chart_fallback=False,
    ).get(code)
    if row is None:
        row = yahoo_provisional.get_provisional_daily_row_from_chart(code)
    if row is None:
        return None, ["yahoo_provisional"]
    date_iso = _ymd_to_iso(row[0])
    last = _to_float(row[4])
    confirmed_close = _to_float((confirmed or {}).get("close"))
    vs_close = ((last - confirmed_close) / confirmed_close * 100.0) if last is not None and confirmed_close else None
    ma7 = _to_float((confirmed or {}).get("ma7"))
    ma20 = _to_float((confirmed or {}).get("ma20"))
    ma60 = _to_float((confirmed or {}).get("ma60"))
    open_price = _to_float(row[1])
    low = _to_float(row[3])
    candle_label = "unknown"
    if open_price is not None and low is not None and last is not None and confirmed_close:
        if open_price < confirmed_close and last > open_price and low <= open_price:
            candle_label = "gap_down_rejection"
        elif last < open_price:
            candle_label = "weak_intraday"
        else:
            candle_label = "firm_intraday"
    return (
        {
            "date": date_iso,
            "open": open_price,
            "high": _to_float(row[2]),
            "low": low,
            "last": last,
            "volume": _to_float(row[5]) if len(row) > 5 else None,
            "vs_confirmed_close_pct": vs_close,
            "above_ma7": bool(last is not None and ma7 is not None and last > ma7),
            "above_ma20": bool(last is not None and ma20 is not None and last > ma20),
            "above_ma60": bool(last is not None and ma60 is not None and last > ma60),
            "candle_label": candle_label,
        },
        [],
    )


def _latest_ranking(conn: duckdb.DuckDBPyConnection, code: str) -> tuple[dict[str, Any], list[str]]:
    table = next((name for name in ("ranking_appearance_daily", "ranking_appearances", "rankings_cache", "ranking_snapshot") if _has_table(conn, name)), None)
    if not table:
        return {"latest_alive_ranking_date": None, "latest_alive_rank": None, "latest_alive_tone": None, "entry_reason_available": False}, ["ranking_appearances"]
    columns = _table_columns(conn, table)
    date_col = _first_existing(columns, ("date", "dt", "ymd", "trade_date", "as_of_date"))
    rank_col = _first_existing(columns, ("rank", "rank_position", "position"))
    tone_col = _first_existing(columns, ("tone", "side", "direction", "dir", "bucket"))
    if not date_col or not rank_col:
        return {"latest_alive_ranking_date": None, "latest_alive_rank": None, "latest_alive_tone": None, "entry_reason_available": False}, [f"{table}.rank"]
    global_latest_row = conn.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()
    global_latest_date = global_latest_row[0] if global_latest_row else None
    select = [date_col, rank_col] + ([tone_col] if tone_col else [])
    row = conn.execute(
        f"SELECT {', '.join(select)} FROM {table} WHERE code = ? ORDER BY {date_col} DESC, {rank_col} ASC LIMIT 1",
        [code],
    ).fetchone()
    if not row:
        return {"latest_alive_ranking_date": None, "latest_alive_rank": None, "latest_alive_tone": None, "entry_reason_available": False}, []
    values = dict(zip(select, row))
    return (
        {
            "latest_alive_ranking_date": _ymd_to_iso(values.get(date_col)) or _timestamp_to_iso_date(values.get(date_col)),
            "latest_alive_rank": int(values[rank_col]) if values.get(rank_col) is not None else None,
            "latest_alive_tone": str(values[tone_col]) if tone_col and values.get(tone_col) is not None else None,
            "latest_available_ranking_date": _ymd_to_iso(global_latest_date) or _timestamp_to_iso_date(global_latest_date),
            "current_top_ranking_present": bool(values.get(date_col) == global_latest_date and values.get(rank_col) is not None and int(values[rank_col]) <= TOP_RANK_LIMIT),
            "entry_reason_available": True,
        },
        [],
    )


def _latest_signal(conn: duckdb.DuckDBPyConnection, code: str) -> tuple[dict[str, Any], list[str]]:
    table = next((name for name in ("signal_decision_daily", "signal_decisions", "signal_tracking_events", "ranking_signal_decisions") if _has_table(conn, name)), None)
    if not table:
        return {"latest_signal_date": None, "buy_entry_qualified": False, "sell_entry_qualified": False}, ["signal_decisions"]
    columns = _table_columns(conn, table)
    date_col = _first_existing(columns, ("date", "dt", "ymd", "trade_date", "as_of_date", "event_date"))
    side_col = _first_existing(columns, ("side", "dir", "direction"))
    buy_col = _first_existing(columns, ("buy_entry_qualified", "is_buy_signal", "buy_signal", "buy_qualified"))
    sell_col = _first_existing(columns, ("sell_entry_qualified", "is_sell_signal", "sell_signal", "sell_qualified"))
    entry_col = _first_existing(columns, ("entry_qualified", "qualified"))
    if not date_col:
        return {"latest_signal_date": None, "buy_entry_qualified": False, "sell_entry_qualified": False}, [f"{table}.date"]
    if side_col and entry_col:
        rows = conn.execute(
            f"SELECT {date_col}, {side_col}, {entry_col} FROM {table} WHERE code = ? ORDER BY {date_col} DESC LIMIT 10",
            [code],
        ).fetchall()
        if not rows:
            return {"latest_signal_date": None, "buy_entry_qualified": False, "sell_entry_qualified": False}, []
        latest_date = rows[0][0]
        latest_rows = [row for row in rows if row[0] == latest_date]
        buy_qualified = any(str(row[1]).lower() in {"buy", "up", "long"} and _to_bool(row[2]) for row in latest_rows)
        sell_qualified = any(str(row[1]).lower() in {"sell", "down", "short"} and _to_bool(row[2]) for row in latest_rows)
        return (
            {
                "latest_signal_date": _ymd_to_iso(latest_date) or _timestamp_to_iso_date(latest_date),
                "buy_entry_qualified": buy_qualified,
                "sell_entry_qualified": sell_qualified,
            },
            [],
        )
    select = [date_col] + [column for column in (buy_col, sell_col) if column]
    row = conn.execute(
        f"SELECT {', '.join(select)} FROM {table} WHERE code = ? ORDER BY {date_col} DESC LIMIT 1",
        [code],
    ).fetchone()
    if not row:
        return {"latest_signal_date": None, "buy_entry_qualified": False, "sell_entry_qualified": False}, []
    values = dict(zip(select, row))
    return (
        {
            "latest_signal_date": _ymd_to_iso(values.get(date_col)) or _timestamp_to_iso_date(values.get(date_col)),
            "buy_entry_qualified": _to_bool(values.get(buy_col)) if buy_col else False,
            "sell_entry_qualified": _to_bool(values.get(sell_col)) if sell_col else False,
        },
        [],
    )


def _event_gate(conn: duckdb.DuckDBPyConnection, code: str, today_iso: str | None) -> tuple[dict[str, Any], list[str]]:
    table = next((name for name in ("earnings_planned", "events_meta", "event_facts", "financial_events") if _has_table(conn, name)), None)
    if not table:
        return {
            "earnings_date": None,
            "earnings_relative_day": None,
            "ex_rights_nearby": False,
            "event_risk_level": "unknown",
            "crossing_allowed_without_fundamental_check": False,
        }, ["events_meta"]
    columns = _table_columns(conn, table)
    date_col = _first_existing(columns, ("earnings_date", "planned_date", "event_date", "date", "scheduled_date"))
    kind_col = _first_existing(columns, ("event_type", "kind", "type"))
    if not date_col:
        return {
            "earnings_date": None,
            "earnings_relative_day": None,
            "ex_rights_nearby": False,
            "event_risk_level": "unknown",
            "crossing_allowed_without_fundamental_check": False,
        }, [f"{table}.date"]
    where = "code = ?"
    params: list[Any] = [code]
    if kind_col and table != "earnings_planned":
        where += f" AND lower(COALESCE({kind_col}, '')) LIKE '%earning%'"
    row = conn.execute(
        f"SELECT {date_col} FROM {table} WHERE {where} ORDER BY {date_col} ASC LIMIT 1",
        params,
    ).fetchone()
    earnings_date = _ymd_to_iso(row[0]) or _timestamp_to_iso_date(row[0]) if row else None
    relative = None
    risk = "unknown"
    if earnings_date and today_iso:
        try:
            days = (date.fromisoformat(earnings_date) - date.fromisoformat(today_iso)).days
            relative = "T" if days == 0 else f"T-{days}" if days > 0 else f"T+{abs(days)}"
            if 0 <= days <= 2:
                risk = "high"
            elif 3 <= days <= 5:
                risk = "medium"
            else:
                risk = "low"
        except ValueError:
            risk = "unknown"
    ex_rights_nearby = False
    if _has_table(conn, "ex_rights"):
        ex_columns = _table_columns(conn, "ex_rights")
        ex_date_col = _first_existing(ex_columns, ("ex_date", "record_date", "last_rights_date"))
        if ex_date_col and today_iso:
            rows = conn.execute(
                f"SELECT {ex_date_col} FROM ex_rights WHERE code = ? ORDER BY {ex_date_col} ASC LIMIT 3",
                [code],
            ).fetchall()
            for ex_row in rows:
                ex_iso = _ymd_to_iso(ex_row[0]) or _timestamp_to_iso_date(ex_row[0])
                if not ex_iso:
                    continue
                try:
                    if abs((date.fromisoformat(ex_iso) - date.fromisoformat(today_iso)).days) <= 5:
                        ex_rights_nearby = True
                        break
                except ValueError:
                    continue
    return (
        {
            "earnings_date": earnings_date,
            "earnings_relative_day": relative,
            "ex_rights_nearby": ex_rights_nearby,
            "event_risk_level": risk,
            "crossing_allowed_without_fundamental_check": False,
        },
        [],
    )


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_payload_value(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round((current - previous) / abs(previous) * 100.0, 2)


def _progress_rate(actual: float | None, forecast: float | None) -> float | None:
    if actual is None or forecast in (None, 0):
        return None
    return round(actual / forecast * 100.0, 2)


def _forecast_revision_direction(current: float | None, previous: float | None) -> str:
    if current is None or previous is None:
        return "unknown"
    if current > previous:
        return "upward"
    if current < previous:
        return "downward"
    return "unchanged"


def _empty_fundamentals(reason: str) -> dict[str, Any]:
    return {
        "available": False,
        "source": "local",
        "latest_result_date": None,
        "fiscal_year_end": None,
        "quarter": None,
        "sales_yoy_pct": None,
        "operating_profit_yoy_pct": None,
        "ordinary_profit_yoy_pct": None,
        "net_profit_yoy_pct": None,
        "company_forecast_operating_profit": None,
        "company_forecast_ordinary_profit": None,
        "progress_rate_operating_profit_pct": None,
        "progress_rate_ordinary_profit_pct": None,
        "forecast_revision_direction": "unknown",
        "consensus_gap_pct": None,
        "earnings_crossing_support": "unavailable",
        "reasons": [reason],
    }


def _fundamentals_supplement(conn: duckdb.DuckDBPyConnection, code: str) -> tuple[dict[str, Any], list[str]]:
    missing: list[str] = []
    if not _has_table(conn, "edinetdb_company_map"):
        return _empty_fundamentals("edinet_mapping_table_missing"), ["edinetdb_company_map"]
    row = conn.execute(
        """
        SELECT edinet_code
        FROM edinetdb_company_map
        WHERE sec_code = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        [code],
    ).fetchone()
    if not row or not row[0]:
        return _empty_fundamentals("edinet_mapping_missing"), [f"edinetdb_company_map:{code}"]
    edinet_code = str(row[0])
    if not _has_table(conn, "edinetdb_financials"):
        return _empty_fundamentals("financials_table_missing"), ["edinetdb_financials"]
    rows = conn.execute(
        """
        SELECT fiscal_year, accounting_standard, payload_json, fetched_at
        FROM edinetdb_financials
        WHERE edinet_code = ?
        ORDER BY fiscal_year DESC
        LIMIT 2
        """,
        [edinet_code],
    ).fetchall()
    if not rows:
        return _empty_fundamentals("financials_missing"), [f"edinetdb_financials:{code}"]

    latest = rows[0]
    previous = rows[1] if len(rows) > 1 else None
    latest_payload = _json_object(latest[2])
    previous_payload = _json_object(previous[2]) if previous else {}
    if not latest_payload:
        return _empty_fundamentals("financials_payload_unreadable"), [f"edinetdb_financials.payload_json:{code}"]

    revenue = _to_float(_first_payload_value(latest_payload, ("revenue", "net_sales", "sales")))
    operating_income = _to_float(
        _first_payload_value(latest_payload, ("operating_income", "operating_profit", "operating_profit_loss"))
    )
    ordinary_income = _to_float(
        _first_payload_value(latest_payload, ("ordinary_income", "ordinary_profit", "ordinary_profit_loss"))
    )
    net_income = _to_float(_first_payload_value(latest_payload, ("net_income", "profit", "profit_attributable_to_owners")))
    previous_revenue = _to_float(_first_payload_value(previous_payload, ("revenue", "net_sales", "sales")))
    previous_operating = _to_float(
        _first_payload_value(previous_payload, ("operating_income", "operating_profit", "operating_profit_loss"))
    )
    previous_ordinary = _to_float(
        _first_payload_value(previous_payload, ("ordinary_income", "ordinary_profit", "ordinary_profit_loss"))
    )
    previous_net = _to_float(
        _first_payload_value(previous_payload, ("net_income", "profit", "profit_attributable_to_owners"))
    )

    forecast_operating_keys = (
        "company_forecast_operating_profit",
        "forecast_operating_income",
        "operating_income_forecast",
        "full_year_operating_income_forecast",
    )
    forecast_ordinary_keys = (
        "company_forecast_ordinary_profit",
        "forecast_ordinary_income",
        "ordinary_income_forecast",
        "full_year_ordinary_income_forecast",
    )
    forecast_operating = _to_float(_first_payload_value(latest_payload, forecast_operating_keys))
    forecast_ordinary = _to_float(_first_payload_value(latest_payload, forecast_ordinary_keys))
    previous_forecast_operating = _to_float(_first_payload_value(previous_payload, forecast_operating_keys))
    previous_forecast_ordinary = _to_float(_first_payload_value(previous_payload, forecast_ordinary_keys))
    explicit_revision = str(
        _first_payload_value(latest_payload, ("forecast_revision_direction", "revision_direction")) or ""
    ).strip().lower()
    if explicit_revision in {"upward", "downward", "unchanged", "unknown"}:
        revision_direction = explicit_revision
    else:
        revision_direction = _forecast_revision_direction(
            forecast_operating or forecast_ordinary,
            previous_forecast_operating or previous_forecast_ordinary,
        )

    consensus = _to_float(
        _first_payload_value(
            latest_payload,
            ("consensus_operating_profit", "consensus_ordinary_profit", "analyst_consensus_profit"),
        )
    )
    consensus_base = forecast_operating or forecast_ordinary or operating_income or ordinary_income
    consensus_gap = _pct_change(consensus_base, consensus)
    operating_yoy = _pct_change(operating_income, previous_operating)
    ordinary_yoy = _pct_change(ordinary_income, previous_ordinary)
    net_yoy = _pct_change(net_income, previous_net)
    operating_progress = _progress_rate(operating_income, forecast_operating)
    ordinary_progress = _progress_rate(ordinary_income, forecast_ordinary)

    reasons = ["local_edinet_financials_available"]
    if previous is None:
        reasons.append("previous_year_financials_missing")
    if forecast_operating is None and forecast_ordinary is None:
        reasons.append("forecast_data_missing")
        missing.append(f"fundamentals_supplement.company_forecast:{code}")
    if consensus is None:
        reasons.append("consensus_data_missing")
        missing.append(f"fundamentals_supplement.consensus_gap_pct:{code}")
    if str(_first_payload_value(latest_payload, ("quarter", "period_type")) or "").strip() == "":
        reasons.append("annual_financials_only")

    avoid_signals = [value is not None and value <= -10.0 for value in (operating_yoy, ordinary_yoy, net_yoy)]
    support_signals = [value is not None and value >= 5.0 for value in (operating_yoy, ordinary_yoy, net_yoy)]
    progress_signals = [value is not None and value >= 75.0 for value in (operating_progress, ordinary_progress)]
    if revision_direction == "downward" or any(avoid_signals):
        crossing_support = "avoid_crossing"
    elif revision_direction == "upward" or any(support_signals) or any(progress_signals):
        crossing_support = "support_crossing"
    else:
        crossing_support = "neutral"

    fiscal_year = latest[0]
    fiscal_year_end = _first_payload_value(latest_payload, ("fiscal_year_end", "period_end", "fiscal_period_end"))
    return (
        {
            "available": True,
            "source": "local",
            "latest_result_date": _timestamp_to_iso_date(latest[3]),
            "fiscal_year_end": str(fiscal_year_end or fiscal_year) if fiscal_year_end or fiscal_year is not None else None,
            "quarter": str(_first_payload_value(latest_payload, ("quarter", "period_type")) or "FY"),
            "sales_yoy_pct": _pct_change(revenue, previous_revenue),
            "operating_profit_yoy_pct": operating_yoy,
            "ordinary_profit_yoy_pct": ordinary_yoy,
            "net_profit_yoy_pct": net_yoy,
            "company_forecast_operating_profit": forecast_operating,
            "company_forecast_ordinary_profit": forecast_ordinary,
            "progress_rate_operating_profit_pct": operating_progress,
            "progress_rate_ordinary_profit_pct": ordinary_progress,
            "forecast_revision_direction": revision_direction,
            "consensus_gap_pct": consensus_gap,
            "earnings_crossing_support": crossing_support,
            "reasons": reasons,
        },
        missing,
    )


def _current_hold_reason(
    signal: dict[str, Any],
    ranking: dict[str, Any],
    position: dict[str, Any],
    event_gate: dict[str, Any],
    chart_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latest_top_present = bool(ranking.get("current_top_ranking_present"))
    signal_alive = bool(signal.get("buy_entry_qualified") or signal.get("sell_entry_qualified"))
    reasons: list[str] = []
    if not signal_alive:
        reasons.append("latest_signal_reject")
    if not latest_top_present:
        reasons.append("not_in_latest_top_ranking")
    if str(event_gate.get("event_risk_level")) == "high":
        reasons.append("earnings_nearby")
    pnl = _to_float(position.get("unrealized_pnl_using_provisional"))
    if pnl is not None and pnl < 0:
        reasons.append("unrealized_loss")
    chart_alive = (chart_context or {}).get("chart_structure_state") == "structurally_alive"
    return {
        **signal,
        "latest_top_ranking_present": latest_top_present,
        "reason_alive": (signal_alive and latest_top_present) or chart_alive,
        "chart_structure_alive": chart_alive,
        "deterioration_reasons": reasons,
    }


def _gross_position_value(position: dict[str, Any]) -> float | None:
    long_qty = _to_float(position.get("long_qty")) or 0.0
    short_qty = _to_float(position.get("short_qty")) or 0.0
    avg_long = _to_float(position.get("avg_long_price")) or 0.0
    avg_short = _to_float(position.get("avg_short_price")) or 0.0
    value = abs(long_qty * avg_long) + abs(short_qty * avg_short)
    return value if value > 0 else None


def _structural_hold_guard(
    current_hold: dict[str, Any],
    position: dict[str, Any],
    event_gate: dict[str, Any],
    chart_context: dict[str, Any] | None,
) -> tuple[bool, float | None]:
    event_risk = str(event_gate.get("event_risk_level") or "unknown").lower()
    provisional = (chart_context or {}).get("daily_6m_context") or {}
    candle_label = str((chart_context or {}).get("short_term_price_state") or "unknown")
    gross_value = _gross_position_value(position)
    unrealized = _to_float(position.get("unrealized_pnl_using_provisional"))
    loss_pct = (unrealized / gross_value * 100.0) if unrealized is not None and gross_value else None
    bad_candles = {"breakdown", "large_bearish", "gap_up_failure_below_ma20", "weak_intraday"}
    above_ma = provisional.get("provisional_above_ma") or {}
    if not above_ma:
        above_ma = {
            "ma20": ((chart_context or {}).get("provisional_bar") or {}).get("above_ma20"),
            "ma60": ((chart_context or {}).get("provisional_bar") or {}).get("above_ma60"),
        }
    guard = bool(
        current_hold.get("chart_structure_alive")
        and event_risk in {"low", "none", "unknown"}
        and above_ma.get("ma20") is True
        and above_ma.get("ma60") is True
        and loss_pct is not None
        and loss_pct > -1.0
        and candle_label not in bad_candles
    )
    return guard, loss_pct


def _decision(
    current_hold: dict[str, Any],
    position: dict[str, Any],
    event_gate: dict[str, Any],
    chart_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reasons = list(current_hold.get("deterioration_reasons") or [])
    long_qty = _to_float(position.get("long_qty")) or 0.0
    short_qty = _to_float(position.get("short_qty")) or 0.0
    from_text = f"short{short_qty:g}-long{long_qty:g}"
    action = "hold"
    confidence = "low"
    move_state = ((chart_context or {}).get("daily_6m_context") or {}).get("move_state")
    structurally_alive = (chart_context or {}).get("chart_structure_state") == "structurally_alive"
    structural_hold_guard, unrealized_loss_pct = _structural_hold_guard(current_hold, position, event_gate, chart_context)
    if (not current_hold.get("reason_alive") or event_gate.get("event_risk_level") == "high" or "unrealized_loss" in reasons) and (
        "unrealized_loss" in reasons or event_gate.get("event_risk_level") == "high" or move_state == "high_zone_failure"
    ):
        if structural_hold_guard and event_gate.get("event_risk_level") != "high" and move_state != "high_zone_failure":
            action = "hold"
            confidence = "medium"
            reasons = [reason for reason in reasons if reason != "unrealized_loss"]
            reasons.append("structural_hold_guard_small_loss")
        else:
            action = "reduce"
            confidence = "medium"
    elif current_hold.get("reason_alive") and event_gate.get("event_risk_level") not in {"high", "unknown"}:
        action = "hold"
        confidence = "medium"
    if long_qty <= 0 and short_qty > 0:
        action = "maintain_hedge"
    to_text = from_text
    if action == "reduce" and long_qty > 0:
        if structurally_alive:
            reasons = [*reasons, "chart_structure_still_alive_reduce_not_exit"]
        to_text = f"short{short_qty:g}-long{long_qty * 0.5:g}~{long_qty * 0.7:g}"
    return {
        "action": action,
        "confidence": confidence,
        "position_proposal": {"from": from_text, "to": to_text},
        "decision_reasons": reasons,
        "structural_hold_guard": structural_hold_guard,
        "unrealized_loss_pct": unrealized_loss_pct,
    }


def build_holding_review_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str | None = None,
) -> HoldingReviewResponse:
    items: list[HoldingReviewBundle] = []
    warnings: list[str] = []
    now_iso = datetime.now(timezone.utc).date().isoformat()
    for holding_code in _holding_codes(conn, code):
        missing: list[str] = []
        confirmed, missing_confirmed = _confirmed_bar(conn, holding_code)
        missing.extend(missing_confirmed)
        missing.extend(_merge_feature_snapshot(conn, holding_code, confirmed))
        provisional, missing_provisional = _provisional_bar(holding_code, confirmed)
        missing.extend(missing_provisional)
        provisional_last = _to_float((provisional or {}).get("last"))
        position, missing_position = _position_payload(conn, holding_code, provisional_last)
        missing.extend(missing_position)
        ranking, missing_ranking = _latest_ranking(conn, holding_code)
        missing.extend(missing_ranking)
        signal, missing_signal = _latest_signal(conn, holding_code)
        missing.extend(missing_signal)
        gate, missing_gate = _event_gate(conn, holding_code, (provisional or confirmed or {}).get("date") or now_iso)
        missing.extend(missing_gate)
        fundamentals, missing_fundamentals = _fundamentals_supplement(conn, holding_code)
        missing.extend(missing_fundamentals)
        chart_context, missing_chart = _chart_context(conn, holding_code, confirmed, provisional)
        missing.extend(missing_chart)
        if provisional:
            chart_context["daily_6m_context"]["provisional_above_ma"] = {
                "ma20": provisional.get("above_ma20"),
                "ma60": provisional.get("above_ma60"),
            }
            chart_context["provisional_bar"] = provisional
        position["position_risk_state"] = (
            "heavy_loss_or_event_risk"
            if (gate.get("event_risk_level") == "high" or (_to_float(position.get("unrealized_pnl_using_provisional")) or 0) < 0)
            else "normal"
        )
        chart_context["position_risk_state"] = position["position_risk_state"]
        hold_reason = _current_hold_reason(signal, ranking, position, gate, chart_context)
        decision = _decision(hold_reason, position, gate, chart_context)
        items.append(
            {
                "code": holding_code,
                "as_of": {
                    "confirmed_date": (confirmed or {}).get("date"),
                    "provisional_date": (provisional or {}).get("date"),
                    "provisional_source": "yahoo" if provisional else None,
                    "confirmed_freshness_status": "fresh" if confirmed else "missing",
                },
                "position": position,
                "entry_reason_snapshot": ranking,
                "current_hold_reason": hold_reason,
                "confirmed_bar": confirmed,
                "provisional_bar": provisional,
                "chart_context": chart_context,
                "fundamentals_supplement": fundamentals,
                "event_gate": gate,
                "decision": decision,
                "data_quality": {
                    "position_avg_price_source": "computed_from_trade_events",
                    "provisional_is_confirmed": False,
                    "missing_fields": sorted(set(missing)),
                },
            }
        )
    if code and not items:
        warnings.append(f"no_holding_review_item:{code}")
    return {"schema_version": SCHEMA_VERSION, "items": items, "warnings": warnings}
