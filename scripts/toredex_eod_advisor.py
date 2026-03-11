from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config


DEFAULT_SEASON_ID = "toredex_live_short_hybrid_prod_20260304"
JST = timezone(timedelta(hours=9))


PROFILE_CONFIG: dict[str, dict[str, float]] = {
    "defensive": {
        "exit_score": 4.0,
        "reduce_score": 3.0,
        "reduce_ratio": 0.40,
    },
    "balanced": {
        "exit_score": 5.0,
        "reduce_score": 3.0,
        "reduce_ratio": 0.30,
    },
    "aggressive": {
        "exit_score": 6.0,
        "reduce_score": 4.0,
        "reduce_ratio": 0.20,
    },
}


def _resolve_db_path(explicit: str | None) -> str:
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    env = os.getenv("STOCKS_DB_PATH")
    if env:
        return str(Path(env).expanduser().resolve())
    return str(config.DB_PATH)


def _parse_symbols(raw: str) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    for part in raw.split(","):
        sym = str(part or "").strip()
        if not sym:
            continue
        out.append(sym)
    return out


def _to_date_epoch_bounds(as_of_text: str | None) -> tuple[int | None, int | None, str | None]:
    text = str(as_of_text or "").strip()
    if not text:
        return None, None, None
    as_of_date = datetime.strptime(text, "%Y-%m-%d").date()
    start = int(datetime(as_of_date.year, as_of_date.month, as_of_date.day, tzinfo=timezone.utc).timestamp())
    return start, start + 86399, as_of_date.isoformat()


def _epoch_to_date_text(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(JST).date().isoformat()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _round_qty(raw_qty: float, lot_size: int, max_qty: float) -> int:
    max_int = max(0, int(max_qty))
    if raw_qty <= 0 or max_int <= 0:
        return 0
    qty = min(int(round(raw_qty)), max_int)
    if lot_size <= 1:
        return qty
    rounded = (qty // lot_size) * lot_size
    if rounded > 0:
        return min(rounded, max_int)
    # Keep odd-lot support when holdings are smaller than lot.
    return max(0, min(qty, max_int))


def _build_in_clause(symbols: list[str]) -> tuple[str, list[Any]]:
    placeholders = ", ".join(["?"] * len(symbols))
    return f"({placeholders})", list(symbols)


def _load_positions(conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> list[dict[str, Any]]:
    params: list[Any] = []
    where_sql = "WHERE (COALESCE(buy_qty, 0) > 0 OR COALESCE(sell_qty, 0) > 0)"
    if symbols:
        in_clause, in_params = _build_in_clause(symbols)
        where_sql += f" AND symbol IN {in_clause}"
        params.extend(in_params)

    rows = conn.execute(
        f"""
        SELECT
          symbol,
          COALESCE(spot_qty, 0),
          COALESCE(margin_long_qty, 0),
          COALESCE(margin_short_qty, 0),
          COALESCE(buy_qty, 0),
          COALESCE(sell_qty, 0),
          opened_at,
          updated_at
        FROM positions_live
        {where_sql}
        ORDER BY symbol
        """,
        params,
    ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "symbol": str(row[0]),
                "spot_qty": float(row[1] or 0.0),
                "margin_long_qty": float(row[2] or 0.0),
                "margin_short_qty": float(row[3] or 0.0),
                "long_qty": float(row[4] or 0.0),
                "short_qty": float(row[5] or 0.0),
                "opened_at": str(row[6]) if row[6] is not None else None,
                "updated_at": str(row[7]) if row[7] is not None else None,
            }
        )
    return out


def _load_latest_ml(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    start_epoch: int | None,
    end_epoch: int | None,
) -> tuple[dict[str, Any] | None, bool]:
    if start_epoch is not None and end_epoch is not None:
        row = conn.execute(
            """
            SELECT dt, p_up, p_down, p_turn_down, ev20_net, rank_up_20, rank_down_20
            FROM ml_pred_20d
            WHERE code = ? AND dt >= ? AND dt <= ?
            ORDER BY dt DESC
            LIMIT 1
            """,
            [symbol, int(start_epoch), int(end_epoch)],
        ).fetchone()
        if row is not None:
            return (
                {
                    "dt": int(row[0]),
                    "date": _epoch_to_date_text(int(row[0])),
                    "p_up": _to_float(row[1]),
                    "p_down": _to_float(row[2]),
                    "p_turn_down": _to_float(row[3]),
                    "ev20_net": _to_float(row[4]),
                    "rank_up_20": _to_float(row[5]),
                    "rank_down_20": _to_float(row[6]),
                },
                True,
            )

    row = conn.execute(
        """
        SELECT dt, p_up, p_down, p_turn_down, ev20_net, rank_up_20, rank_down_20
        FROM ml_pred_20d
        WHERE code = ?
        ORDER BY dt DESC
        LIMIT 1
        """,
        [symbol],
    ).fetchone()
    if row is None:
        return None, False
    return (
        {
            "dt": int(row[0]),
            "date": _epoch_to_date_text(int(row[0])),
            "p_up": _to_float(row[1]),
            "p_down": _to_float(row[2]),
            "p_turn_down": _to_float(row[3]),
            "ev20_net": _to_float(row[4]),
            "rank_up_20": _to_float(row[5]),
            "rank_down_20": _to_float(row[6]),
        },
        False,
    )


def _load_latest_sell(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    start_epoch: int | None,
    end_epoch: int | None,
) -> tuple[dict[str, Any] | None, bool]:
    if start_epoch is not None and end_epoch is not None:
        row = conn.execute(
            """
            SELECT
              dt, close, day_change_pct, p_down, p_turn_down, ev20_net,
              short_score, trend_down, trend_down_strict
            FROM sell_analysis_daily
            WHERE code = ? AND dt >= ? AND dt <= ?
            ORDER BY dt DESC
            LIMIT 1
            """,
            [symbol, int(start_epoch), int(end_epoch)],
        ).fetchone()
        if row is not None:
            return (
                {
                    "dt": int(row[0]),
                    "date": _epoch_to_date_text(int(row[0])),
                    "close": _to_float(row[1]),
                    "day_change_pct": _to_float(row[2]),
                    "p_down": _to_float(row[3]),
                    "p_turn_down": _to_float(row[4]),
                    "ev20_net": _to_float(row[5]),
                    "short_score": _to_float(row[6]),
                    "trend_down": _to_bool(row[7]),
                    "trend_down_strict": _to_bool(row[8]),
                },
                True,
            )

    row = conn.execute(
        """
        SELECT
          dt, close, day_change_pct, p_down, p_turn_down, ev20_net,
          short_score, trend_down, trend_down_strict
        FROM sell_analysis_daily
        WHERE code = ?
        ORDER BY dt DESC
        LIMIT 1
        """,
        [symbol],
    ).fetchone()
    if row is None:
        return None, False
    return (
        {
            "dt": int(row[0]),
            "date": _epoch_to_date_text(int(row[0])),
            "close": _to_float(row[1]),
            "day_change_pct": _to_float(row[2]),
            "p_down": _to_float(row[3]),
            "p_turn_down": _to_float(row[4]),
            "ev20_net": _to_float(row[5]),
            "short_score": _to_float(row[6]),
            "trend_down": _to_bool(row[7]),
            "trend_down_strict": _to_bool(row[8]),
        },
        False,
    )


def _load_latest_bar(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    start_epoch: int | None,
    end_epoch: int | None,
) -> tuple[dict[str, Any] | None, bool]:
    if start_epoch is not None and end_epoch is not None:
        row = conn.execute(
            """
            SELECT date, o, h, l, c, v
            FROM daily_bars
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            [symbol, int(start_epoch), int(end_epoch)],
        ).fetchone()
        if row is not None:
            return (
                {
                    "date_epoch": int(row[0]),
                    "date": _epoch_to_date_text(int(row[0])),
                    "open": _to_float(row[1]),
                    "high": _to_float(row[2]),
                    "low": _to_float(row[3]),
                    "close": _to_float(row[4]),
                    "volume": int(row[5] or 0),
                },
                True,
            )

    row = conn.execute(
        """
        SELECT date, o, h, l, c, v
        FROM daily_bars
        WHERE code = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        [symbol],
    ).fetchone()
    if row is None:
        return None, False
    return (
        {
            "date_epoch": int(row[0]),
            "date": _epoch_to_date_text(int(row[0])),
            "open": _to_float(row[1]),
            "high": _to_float(row[2]),
            "low": _to_float(row[3]),
            "close": _to_float(row[4]),
            "volume": int(row[5] or 0),
        },
        False,
    )


def _load_latest_toredex_actions(
    conn: duckdb.DuckDBPyConnection,
    season_id: str,
) -> dict[str, dict[str, Any]]:
    row = conn.execute(
        """
        SELECT "asOf", payload_json
        FROM toredex_decisions
        WHERE season_id = ?
        ORDER BY "asOf" DESC
        LIMIT 1
        """,
        [season_id],
    ).fetchone()
    if row is None:
        return {}
    payload = str(row[1] or "")
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except Exception:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for action in parsed.get("actions") or []:
        if not isinstance(action, dict):
            continue
        ticker = str(action.get("ticker") or "")
        if not ticker:
            continue
        out[ticker] = {
            "as_of": str(row[0]),
            "side": str(action.get("side") or "").upper(),
            "delta_units": int(action.get("deltaUnits") or 0),
            "reason_id": str(action.get("reasonId") or ""),
        }
    return out


def _fetch_yahoo_chart(symbol: str) -> dict[str, Any] | None:
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    result = (((payload or {}).get("chart") or {}).get("result") or [None])[0]
    if not isinstance(result, dict):
        return None

    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
    closes = quote.get("close") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    volumes = quote.get("volume") or []
    rows: list[dict[str, Any]] = []
    for ts, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, volumes):
        if c is None:
            continue
        rows.append(
            {
                "date": datetime.fromtimestamp(int(ts), tz=JST).date().isoformat(),
                "open": _to_float(o),
                "high": _to_float(h),
                "low": _to_float(l),
                "close": _to_float(c),
                "volume": int(v or 0),
            }
        )
    if not rows:
        return None
    latest = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else None
    day_change_pct = None
    if prev and prev.get("close") and latest.get("close"):
        prev_close = float(prev["close"])
        if prev_close != 0.0:
            day_change_pct = float(latest["close"] / prev_close - 1.0)
    return {
        "symbol": symbol,
        "latest": latest,
        "previous": prev,
        "day_change_pct": day_change_pct,
    }


def _market_regime(n225_day_change_pct: float | None) -> str:
    if n225_day_change_pct is None:
        return "unknown"
    if n225_day_change_pct <= -0.02:
        return "risk_off"
    if n225_day_change_pct <= -0.01:
        return "weak"
    if n225_day_change_pct >= 0.01:
        return "risk_on"
    return "neutral"


def _build_orders_for_long_close(
    *,
    symbol: str,
    close_qty: int,
    spot_qty: float,
    margin_long_qty: float,
    reason: str,
) -> list[dict[str, Any]]:
    remain = int(close_qty)
    out: list[dict[str, Any]] = []
    if remain <= 0:
        return out
    spot_close = min(remain, int(spot_qty))
    if spot_close > 0:
        out.append(
            {
                "symbol": symbol,
                "action": "SPOT_SELL",
                "qty": int(spot_close),
                "reason": reason,
            }
        )
        remain -= int(spot_close)
    margin_close = min(remain, int(margin_long_qty))
    if margin_close > 0:
        out.append(
            {
                "symbol": symbol,
                "action": "MARGIN_CLOSE_LONG",
                "qty": int(margin_close),
                "reason": reason,
            }
        )
    return out


def _decide_for_symbol(
    *,
    position: dict[str, Any],
    ml: dict[str, Any] | None,
    sell: dict[str, Any] | None,
    bar: dict[str, Any] | None,
    toredex_action: dict[str, Any] | None,
    market_regime_value: str,
    profile: str,
    lot_size: int,
) -> dict[str, Any]:
    cfg = PROFILE_CONFIG[profile]
    long_qty = float(position.get("long_qty") or 0.0)
    short_qty = float(position.get("short_qty") or 0.0)
    net_qty = long_qty - short_qty
    spot_qty = float(position.get("spot_qty") or 0.0)
    margin_long_qty = float(position.get("margin_long_qty") or 0.0)
    margin_short_qty = float(position.get("margin_short_qty") or 0.0)

    p_up = _to_float((ml or {}).get("p_up"))
    p_down = _to_float((sell or {}).get("p_down"))
    if p_down is None:
        p_down = _to_float((ml or {}).get("p_down"))
    p_turn_down = _to_float((sell or {}).get("p_turn_down"))
    if p_turn_down is None:
        p_turn_down = _to_float((ml or {}).get("p_turn_down"))
    ev20_net = _to_float((sell or {}).get("ev20_net"))
    if ev20_net is None:
        ev20_net = _to_float((ml or {}).get("ev20_net"))
    day_change_pct = _to_float((sell or {}).get("day_change_pct"))
    trend_down_strict = _to_bool((sell or {}).get("trend_down_strict"))

    bearish_score = 0.0
    bullish_score = 0.0
    reasons: list[str] = []

    if p_down is not None and p_down >= 0.52:
        bearish_score += 2.0
        reasons.append(f"p_down={p_down:.3f}>=0.52")
    if p_turn_down is not None and p_turn_down >= 0.52:
        bearish_score += 1.0
        reasons.append(f"p_turn_down={p_turn_down:.3f}>=0.52")
    if ev20_net is not None and ev20_net < 0.0:
        bearish_score += 1.0
        reasons.append(f"ev20_net={ev20_net:.4f}<0")
    if trend_down_strict:
        bearish_score += 1.0
        reasons.append("trend_down_strict=true")
    if day_change_pct is not None and day_change_pct <= -0.02:
        bearish_score += 1.0
        reasons.append(f"day_change_pct={day_change_pct:.2%}<=-2%")
    if market_regime_value == "risk_off":
        bearish_score += 1.0
        reasons.append("market_regime=risk_off")

    if p_up is not None and p_up >= 0.52:
        bullish_score += 2.0
    if ev20_net is not None and ev20_net > 0.0:
        bullish_score += 1.0
    if p_down is not None and p_down <= 0.45:
        bullish_score += 1.0
    if day_change_pct is not None and day_change_pct >= 0.02:
        bullish_score += 1.0
    if not trend_down_strict:
        bullish_score += 1.0
    if market_regime_value == "risk_on":
        bullish_score += 1.0

    action = "HOLD"
    confidence = "LOW"
    orders: list[dict[str, Any]] = []
    reduce_ratio = float(cfg["reduce_ratio"])
    exit_score = float(cfg["exit_score"])
    reduce_score = float(cfg["reduce_score"])

    if net_qty > 0:
        if bearish_score >= exit_score:
            close_qty = _round_qty(raw_qty=net_qty, lot_size=lot_size, max_qty=long_qty)
            if close_qty > 0:
                action = "EXIT_NET_LONG"
                confidence = "HIGH"
                orders = _build_orders_for_long_close(
                    symbol=str(position["symbol"]),
                    close_qty=close_qty,
                    spot_qty=spot_qty,
                    margin_long_qty=margin_long_qty,
                    reason="bearish_exit",
                )
        elif bearish_score >= reduce_score:
            target = max(net_qty * 0.5, long_qty * reduce_ratio)
            close_qty = _round_qty(raw_qty=target, lot_size=lot_size, max_qty=long_qty)
            if close_qty > 0:
                action = "REDUCE_LONG"
                confidence = "MEDIUM"
                orders = _build_orders_for_long_close(
                    symbol=str(position["symbol"]),
                    close_qty=close_qty,
                    spot_qty=spot_qty,
                    margin_long_qty=margin_long_qty,
                    reason="bearish_reduce",
                )
        else:
            action = "HOLD_LONG_BIAS"
            confidence = "LOW"
    elif net_qty < 0:
        net_short = abs(net_qty)
        if bullish_score >= exit_score:
            close_qty = _round_qty(raw_qty=net_short, lot_size=lot_size, max_qty=margin_short_qty)
            if close_qty > 0:
                action = "EXIT_NET_SHORT"
                confidence = "HIGH"
                orders = [
                    {
                        "symbol": str(position["symbol"]),
                        "action": "MARGIN_CLOSE_SHORT",
                        "qty": int(close_qty),
                        "reason": "bullish_exit",
                    }
                ]
        elif bullish_score >= reduce_score:
            target = max(net_short * 0.5, margin_short_qty * reduce_ratio)
            close_qty = _round_qty(raw_qty=target, lot_size=lot_size, max_qty=margin_short_qty)
            if close_qty > 0:
                action = "REDUCE_SHORT"
                confidence = "MEDIUM"
                orders = [
                    {
                        "symbol": str(position["symbol"]),
                        "action": "MARGIN_CLOSE_SHORT",
                        "qty": int(close_qty),
                        "reason": "bullish_reduce",
                    }
                ]
        else:
            action = "HOLD_SHORT_BIAS"
            confidence = "LOW"
    else:
        action = "HOLD_HEDGED"
        confidence = "LOW"

    post_long = long_qty
    post_short = short_qty
    for order in orders:
        oq = int(order.get("qty") or 0)
        if str(order.get("action")) in {"SPOT_SELL", "MARGIN_CLOSE_LONG"}:
            post_long = max(0.0, post_long - oq)
        elif str(order.get("action")) == "MARGIN_CLOSE_SHORT":
            post_short = max(0.0, post_short - oq)

    return {
        "symbol": str(position["symbol"]),
        "position": {
            "spot_qty": spot_qty,
            "margin_long_qty": margin_long_qty,
            "margin_short_qty": margin_short_qty,
            "long_qty": long_qty,
            "short_qty": short_qty,
            "net_qty": net_qty,
            "updated_at": position.get("updated_at"),
        },
        "signals": {
            "ml": ml,
            "sell": sell,
            "bar": bar,
            "toredex_latest_action": toredex_action,
        },
        "scoring": {
            "bearish_score": float(bearish_score),
            "bullish_score": float(bullish_score),
            "reasons": reasons[:8],
            "market_regime": market_regime_value,
        },
        "recommendation": {
            "action": action,
            "confidence": confidence,
            "orders": orders,
            "post_position": {
                "long_qty": post_long,
                "short_qty": post_short,
                "net_qty": post_long - post_short,
            },
        },
    }


def _default_output_path(as_of_date_text: str | None) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = as_of_date_text or "latest"
    return Path("tmp") / f"toredex_eod_advice_{suffix}_{stamp}.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate end-of-day proposal from positions + ML/sell signals.")
    parser.add_argument("--symbols", default="", help="Comma separated symbols (default: all current holdings).")
    parser.add_argument("--as-of", default="", help="Target date in YYYY-MM-DD (default: latest rows).")
    parser.add_argument(
        "--risk-profile",
        default="balanced",
        choices=["defensive", "balanced", "aggressive"],
        help="Controls reduce/exit thresholds.",
    )
    parser.add_argument("--season-id", default=DEFAULT_SEASON_ID, help="TOREDEX season for latest action context.")
    parser.add_argument("--lot-size", type=int, default=100, help="Quantity rounding lot size.")
    parser.add_argument("--db-path", default="", help="Optional DuckDB path.")
    parser.add_argument("--output", default="", help="Output JSON path.")
    parser.add_argument(
        "--yahoo-verify",
        action="store_true",
        help="Fetch Yahoo closes for Nikkei and symbol sanity checks.",
    )
    parser.add_argument(
        "--max-yahoo-symbols",
        type=int,
        default=8,
        help="Cap Yahoo symbol checks to avoid rate limiting.",
    )
    args = parser.parse_args()

    db_path = _resolve_db_path(str(args.db_path).strip() or None)
    symbols = _parse_symbols(str(args.symbols).strip())
    start_epoch, end_epoch, as_of_date_text = _to_date_epoch_bounds(str(args.as_of).strip() or None)
    lot_size = max(1, int(args.lot_size))

    with duckdb.connect(db_path, read_only=True) as conn:
        positions = _load_positions(conn, symbols)
        toredex_actions = _load_latest_toredex_actions(conn, str(args.season_id).strip())

        if not positions:
            payload = {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "db_path": db_path,
                "as_of": as_of_date_text,
                "risk_profile": str(args.risk_profile),
                "season_id": str(args.season_id),
                "market_review": None,
                "items": [],
                "order_plan": [],
                "note": "No active positions.",
            }
            out_path = Path(str(args.output).strip()) if str(args.output).strip() else _default_output_path(as_of_date_text)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[toredex_eod_advisor] wrote {out_path}")
            print("No active positions.")
            return 0

        market_review: dict[str, Any] | None = None
        market_regime_value = "unknown"
        yahoo_symbol_checks: dict[str, Any] = {}
        if bool(args.yahoo_verify):
            nikkei = _fetch_yahoo_chart("^N225")
            if nikkei:
                market_review = {
                    "nikkei225": nikkei,
                    "regime": _market_regime(_to_float(nikkei.get("day_change_pct"))),
                }
                market_regime_value = str(market_review["regime"])
            capped_symbols = [p["symbol"] for p in positions][: max(0, int(args.max_yahoo_symbols))]
            for sym in capped_symbols:
                if len(sym) == 4 and sym.isdigit():
                    chk = _fetch_yahoo_chart(f"{sym}.T")
                    if chk:
                        yahoo_symbol_checks[sym] = chk

        items: list[dict[str, Any]] = []
        order_plan: list[dict[str, Any]] = []
        for position in positions:
            symbol = str(position["symbol"])
            ml, ml_exact = _load_latest_ml(conn, symbol, start_epoch, end_epoch)
            sell, sell_exact = _load_latest_sell(conn, symbol, start_epoch, end_epoch)
            bar, bar_exact = _load_latest_bar(conn, symbol, start_epoch, end_epoch)

            row = _decide_for_symbol(
                position=position,
                ml=ml,
                sell=sell,
                bar=bar,
                toredex_action=toredex_actions.get(symbol),
                market_regime_value=market_regime_value,
                profile=str(args.risk_profile),
                lot_size=lot_size,
            )

            warnings: list[str] = []
            if as_of_date_text:
                if not ml_exact:
                    warnings.append("ml_pred: fallback_to_latest")
                if not sell_exact:
                    warnings.append("sell_analysis: fallback_to_latest")
                if not bar_exact:
                    warnings.append("daily_bars: fallback_to_latest")

            if symbol in yahoo_symbol_checks:
                yahoo_latest_close = _to_float((yahoo_symbol_checks[symbol].get("latest") or {}).get("close"))
                db_close = _to_float((bar or {}).get("close")) or _to_float((sell or {}).get("close"))
                if yahoo_latest_close is not None and db_close is not None and db_close != 0:
                    gap = (yahoo_latest_close / db_close) - 1.0
                    row["signals"]["yahoo_close_gap_pct"] = float(gap)
                    if abs(gap) >= 0.01:
                        warnings.append(f"close_gap_ge_1pct: db={db_close:.2f}, yahoo={yahoo_latest_close:.2f}")

            if warnings:
                row["warnings"] = warnings

            items.append(row)
            for order in row["recommendation"]["orders"]:
                order_plan.append(order)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": db_path,
        "as_of": as_of_date_text,
        "risk_profile": str(args.risk_profile),
        "season_id": str(args.season_id),
        "market_review": market_review,
        "items": items,
        "order_plan": order_plan,
        "yahoo_symbol_checks": yahoo_symbol_checks if bool(args.yahoo_verify) else None,
    }
    out_path = Path(str(args.output).strip()) if str(args.output).strip() else _default_output_path(as_of_date_text)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[toredex_eod_advisor] wrote {out_path}")
    print(f"positions={len(items)} orders={len(order_plan)} risk_profile={args.risk_profile}")
    for item in items:
        rec = item["recommendation"]
        pos = item["position"]
        print(
            f"{item['symbol']}: action={rec['action']} net={pos['net_qty']:.0f} "
            f"-> {rec['post_position']['net_qty']:.0f} orders={len(rec['orders'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
