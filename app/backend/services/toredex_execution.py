from __future__ import annotations

from datetime import date
from typing import Any

from app.backend.services.toredex_config import ToredexConfig
from app.backend.services.toredex_hash import hash_payload
from app.backend.services.toredex_repository import ToredexRepository


def _build_price_map(snapshot: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    rankings = snapshot.get("rankings") if isinstance(snapshot.get("rankings"), dict) else {}
    for key in ("buy", "sell"):
        items = rankings.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "")
            if not ticker:
                continue
            try:
                close = float(item.get("close"))
            except Exception:
                continue
            if close > 0:
                out[ticker] = close
    return out


def _calc_stage(units: int) -> str:
    if units <= 2:
        return "PROBE"
    if units <= 5:
        return "ADD"
    return "MAIN"


def _position_pnl_pct(side: str, avg_price: float, close: float) -> float:
    if avg_price <= 0:
        return 0.0
    if side == "SHORT":
        return (avg_price - close) / avg_price * 100.0
    return (close - avg_price) / avg_price * 100.0


def _to_position_map(positions: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for pos in positions:
        ticker = str(pos.get("ticker") or "")
        side = str(pos.get("side") or "LONG").upper()
        if not ticker:
            continue
        out[(ticker, side)] = {
            "ticker": ticker,
            "side": side,
            "units": int(pos.get("units") or 0),
            "avgPrice": float(pos.get("avgPrice") or 0.0),
            "stage": str(pos.get("stage") or "PROBE"),
            "openedAt": str(pos.get("openedAt")),
            "holdingDays": int(pos.get("holdingDays") or 0),
            "pnlPct": float(pos.get("pnlPct") or 0.0),
        }
    return out


def execute_live_decision(
    *,
    repo: ToredexRepository,
    season_id: str,
    as_of: date,
    snapshot: dict[str, Any],
    decision: dict[str, Any],
    config: ToredexConfig,
) -> dict[str, Any]:
    positions_before = repo.get_positions(season_id)
    pos_map = _to_position_map(positions_before)

    season = repo.get_season(season_id)
    if not season:
        raise RuntimeError(f"season not found: {season_id}")

    initial_cash = float(season.get("initial_cash") or config.initial_cash)
    previous_metric = repo.get_latest_metrics(season_id, before_or_equal=as_of)
    prev_cash = float(previous_metric.get("cash")) if previous_metric else initial_cash
    prev_equity = float(previous_metric.get("equity")) if previous_metric else initial_cash
    prev_max_dd = float(previous_metric.get("max_drawdown_pct")) if previous_metric else 0.0

    unit_notional = float(config.max_per_ticker) / 10.0
    price_map = _build_price_map(snapshot)
    actions = decision.get("actions") if isinstance(decision.get("actions"), list) else []

    trades: list[dict[str, Any]] = []
    cash = prev_cash
    as_of_iso = as_of.isoformat()

    for idx, action in enumerate(actions):
        if not isinstance(action, dict):
            continue
        ticker = str(action.get("ticker") or "")
        side = str(action.get("side") or "LONG").upper()
        delta = int(action.get("deltaUnits") or 0)
        reason_id = str(action.get("reasonId") or "")
        if not ticker or side not in {"LONG", "SHORT"}:
            continue
        if delta not in {-5, -3, -2, 2, 3, 5}:
            raise RuntimeError("K_POLICY_INCONSISTENT: invalid deltaUnits")

        key = (ticker, side)
        pos = pos_map.get(key)
        old_units = int(pos["units"]) if pos else 0
        new_units = old_units + delta
        if new_units < 0:
            raise RuntimeError("K_POLICY_INCONSISTENT: negative units")

        price = price_map.get(ticker)
        if price is None:
            if pos and float(pos.get("avgPrice") or 0.0) > 0:
                price = float(pos["avgPrice"])
            else:
                raise RuntimeError(f"K_POLICY_INCONSISTENT: missing close price for {ticker}")

        notion = abs(delta) * unit_notional
        if side == "LONG":
            if delta > 0:
                cash -= notion
            else:
                cash += notion
        else:
            if delta > 0:
                cash += notion
            else:
                cash -= notion

        if pos is None and new_units > 0:
            pos = {
                "ticker": ticker,
                "side": side,
                "units": 0,
                "avgPrice": float(price),
                "stage": "PROBE",
                "openedAt": as_of_iso,
                "holdingDays": 0,
                "pnlPct": 0.0,
            }

        if pos is not None:
            if delta > 0:
                old_qty = int(pos.get("units") or 0)
                old_avg = float(pos.get("avgPrice") or price)
                total_qty = old_qty + delta
                if total_qty > 0:
                    pos["avgPrice"] = (old_avg * old_qty + float(price) * delta) / total_qty
            pos["units"] = new_units
            if new_units <= 0:
                pos_map.pop(key, None)
            else:
                pos["stage"] = _calc_stage(new_units)
                pos["holdingDays"] = int(pos.get("holdingDays") or 0) + 1
                pos_map[key] = pos

        trade_id = hash_payload(
            {
                "seasonId": season_id,
                "asOf": as_of_iso,
                "index": idx,
                "ticker": ticker,
                "side": side,
                "deltaUnits": delta,
                "reasonId": reason_id,
                "price": round(float(price), 6),
            }
        )
        trades.append(
            {
                "season_id": season_id,
                "asOf": as_of,
                "trade_id": trade_id,
                "ticker": ticker,
                "side": side,
                "delta_units": delta,
                "price": float(price),
                "reason_id": reason_id,
                "fees_bps": 0.0,
            }
        )

    positions_after: list[dict[str, Any]] = []
    invested_base = 0.0
    unrealized = 0.0
    for (_ticker, _side), pos in sorted(pos_map.items(), key=lambda item: (item[0][0], item[0][1])):
        units = int(pos.get("units") or 0)
        if units <= 0:
            continue
        ticker = str(pos["ticker"])
        side = str(pos["side"])
        close = price_map.get(ticker, float(pos.get("avgPrice") or 0.0))
        avg_price = float(pos.get("avgPrice") or close)
        pnl_pct = _position_pnl_pct(side, avg_price, close)
        pos["pnlPct"] = pnl_pct
        pos["openedAt"] = str(pos.get("openedAt") or as_of_iso)

        invested_base += abs(units) * unit_notional
        unrealized += abs(units) * unit_notional * (pnl_pct / 100.0)

        positions_after.append(
            {
                "ticker": ticker,
                "side": side,
                "units": units,
                "avgPrice": avg_price,
                "stage": str(pos.get("stage") or _calc_stage(units)),
                "openedAt": str(pos.get("openedAt") or as_of_iso),
                "holdingDays": int(pos.get("holdingDays") or 0),
                "pnlPct": float(pnl_pct),
            }
        )

    equity = float(cash + invested_base + unrealized)
    daily_pnl = float(equity - prev_equity)
    cum_pnl = float(equity - initial_cash)
    cum_return_pct = float((cum_pnl / initial_cash) * 100.0) if initial_cash else 0.0

    peak = max(float(initial_cash), float(prev_equity))
    dd_now_pct = ((equity - peak) / peak * 100.0) if peak > 0 else 0.0
    max_drawdown_pct = min(float(prev_max_dd), float(dd_now_pct)) if previous_metric else float(dd_now_pct)

    goal20 = float(config.stage_rules.get("goal20Pct", 20.0))
    goal30 = float(config.stage_rules.get("goal30Pct", 30.0))
    game_over_threshold = float(config.thresholds.get("gameOverPct", -20.0))

    metric = {
        "season_id": season_id,
        "asOf": as_of,
        "cash": float(round(cash, 6)),
        "equity": float(round(equity, 6)),
        "daily_pnl": float(round(daily_pnl, 6)),
        "cum_pnl": float(round(cum_pnl, 6)),
        "cum_return_pct": float(round(cum_return_pct, 6)),
        "max_drawdown_pct": float(round(max_drawdown_pct, 6)),
        "holdings_count": int(len({p["ticker"] for p in positions_after})),
        "goal20_reached": bool(cum_return_pct >= goal20),
        "goal30_reached": bool(cum_return_pct >= goal30),
        "game_over": bool(cum_return_pct <= game_over_threshold),
    }

    repo.save_trades(trades)
    repo.replace_positions(season_id, positions_after)
    repo.save_daily_metrics(metric)

    return {
        "trades": trades,
        "positions": positions_after,
        "metrics": metric,
    }
