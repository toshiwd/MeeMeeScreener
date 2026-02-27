from __future__ import annotations

from datetime import date
import math
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


def _build_ranking_index(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rankings = snapshot.get("rankings") if isinstance(snapshot.get("rankings"), dict) else {}
    for key in ("buy", "sell"):
        items = rankings.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "")
            if ticker and ticker not in out:
                out[ticker] = item
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


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
        if math.isfinite(f):
            return float(f)
    except Exception:
        return None
    return None


def _estimate_slippage_bps(
    *,
    notion: float,
    liquidity20d: float | None,
    base_bps: float,
    liq_factor_bps: float,
) -> float:
    out = max(0.0, float(base_bps))
    if liq_factor_bps <= 0:
        return out
    if liquidity20d is None or liquidity20d <= 0:
        return out + float(liq_factor_bps)
    size_ratio = min(3.0, max(0.0, float(notion) / float(liquidity20d)))
    return out + float(liq_factor_bps) * size_ratio


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

    prev_fees_cum = float(previous_metric.get("fees_cost_cum")) if previous_metric and previous_metric.get("fees_cost_cum") is not None else 0.0
    prev_slippage_cum = float(previous_metric.get("slippage_cost_cum")) if previous_metric and previous_metric.get("slippage_cost_cum") is not None else 0.0
    prev_borrow_cum = float(previous_metric.get("borrow_cost_cum")) if previous_metric and previous_metric.get("borrow_cost_cum") is not None else 0.0
    prev_turnover_cum = float(previous_metric.get("turnover_notional_cum")) if previous_metric and previous_metric.get("turnover_notional_cum") is not None else 0.0

    unit_notional = float(config.max_per_ticker) / 10.0
    price_map = _build_price_map(snapshot)
    ranking_index = _build_ranking_index(snapshot)
    actions = decision.get("actions") if isinstance(decision.get("actions"), list) else []
    tracked_tickers = [
        str(action.get("ticker") or "")
        for action in actions
        if isinstance(action, dict) and str(action.get("ticker") or "").strip()
    ]
    tracked_tickers.extend([ticker for (ticker, _side) in pos_map.keys()])
    db_prices = repo.get_close_map(as_of=as_of, tickers=tracked_tickers)
    if db_prices:
        price_map.update(db_prices)

    cost_model = config.cost_model
    fees_bps = max(0.0, float(cost_model.get("feesBps") or 0.0))
    slippage_bps_base = max(0.0, float(cost_model.get("slippageBps") or 0.0))
    slippage_liq_factor_bps = max(0.0, float(cost_model.get("slippageLiquidityFactorBps") or 0.0))
    borrow_short_bps_annual = max(0.0, float(cost_model.get("borrowShortBpsAnnual") or 0.0))

    trades: list[dict[str, Any]] = []
    cash = prev_cash
    as_of_iso = as_of.isoformat()
    fees_cost_daily = 0.0
    slippage_cost_daily = 0.0
    turnover_notional_daily = 0.0

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
        turnover_notional_daily += notion
        liquidity20d = _float_or_none((ranking_index.get(ticker) or {}).get("liquidity20d"))
        slippage_bps = _estimate_slippage_bps(
            notion=notion,
            liquidity20d=liquidity20d,
            base_bps=slippage_bps_base,
            liq_factor_bps=slippage_liq_factor_bps,
        )
        fees_cost = notion * (fees_bps / 10_000.0)
        slippage_cost = notion * (slippage_bps / 10_000.0)
        fees_cost_daily += fees_cost
        slippage_cost_daily += slippage_cost

        close_ratio = 1.0
        if delta < 0 and pos is not None and old_units > 0:
            avg_for_close = float(pos.get("avgPrice") or 0.0)
            if avg_for_close > 0 and float(price) > 0:
                close_ratio = float(price) / avg_for_close

        if side == "LONG":
            if delta > 0:
                cash -= notion
            else:
                cash += notion * close_ratio
        else:
            if delta > 0:
                cash += notion
            else:
                cash -= notion * close_ratio

        cash -= (fees_cost + slippage_cost)

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
                "fees_bps": float(fees_bps),
                "slippage_bps": float(slippage_bps),
                "borrow_bps_annual": float(borrow_short_bps_annual),
                "notional": float(notion),
                "fees_cost": float(round(fees_cost, 6)),
                "slippage_cost": float(round(slippage_cost, 6)),
                "borrow_cost": 0.0,
            }
        )

    positions_after: list[dict[str, Any]] = []
    invested_base = 0.0
    unrealized = 0.0
    long_units = 0
    short_units = 0
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
        holding_days = int(pos.get("holdingDays") or 0)
        try:
            opened_at_date = date.fromisoformat(str(pos["openedAt"]))
        except Exception:
            opened_at_date = as_of
        if opened_at_date < as_of:
            holding_days += 1
        else:
            holding_days = max(holding_days, 1)
        pos["holdingDays"] = int(holding_days)

        invested_base += abs(units) * unit_notional
        unrealized += abs(units) * unit_notional * (pnl_pct / 100.0)

        if side == "SHORT":
            short_units += abs(units)
        else:
            long_units += abs(units)

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

    borrow_cost_daily = 0.0
    if borrow_short_bps_annual > 0 and short_units > 0:
        borrow_cost_daily = (short_units * unit_notional) * (borrow_short_bps_annual / 10_000.0) / 252.0
        cash -= borrow_cost_daily

    equity = float(cash + invested_base + unrealized)
    net_daily_pnl = float(equity - prev_equity)
    net_cum_pnl = float(equity - initial_cash)
    net_cum_return_pct = float((net_cum_pnl / initial_cash) * 100.0) if initial_cash else 0.0

    fees_cost_cum = prev_fees_cum + fees_cost_daily
    slippage_cost_cum = prev_slippage_cum + slippage_cost_daily
    borrow_cost_cum = prev_borrow_cum + borrow_cost_daily
    total_cost_cum = fees_cost_cum + slippage_cost_cum + borrow_cost_cum
    prev_total_cost_cum = prev_fees_cum + prev_slippage_cum + prev_borrow_cum

    gross_equity = float(equity + total_cost_cum)
    prev_gross_equity = float(prev_equity + prev_total_cost_cum)
    gross_daily_pnl = float(gross_equity - prev_gross_equity)
    gross_cum_pnl = float(gross_equity - initial_cash)
    gross_cum_return_pct = float((gross_cum_pnl / initial_cash) * 100.0) if initial_cash else 0.0

    peak = max(float(initial_cash), float(prev_equity))
    dd_now_pct = ((equity - peak) / peak * 100.0) if peak > 0 else 0.0
    max_drawdown_pct = min(float(prev_max_dd), float(dd_now_pct)) if previous_metric else float(dd_now_pct)

    goal20 = float(config.stage_rules.get("goal20Pct", 20.0))
    goal30 = float(config.stage_rules.get("goal30Pct", 30.0))
    game_over_threshold = float(config.thresholds.get("gameOverPct", -20.0))

    turnover_notional_cum = prev_turnover_cum + turnover_notional_daily
    turnover_pct_daily = (turnover_notional_daily / prev_equity * 100.0) if prev_equity > 0 else 0.0

    gross_units = int(long_units + short_units)
    net_units = int(long_units - short_units)
    gross_cap = max(1, int(config.portfolio_constraints.get("grossUnitsCap", 10)))
    net_exposure_pct = float(net_units / gross_cap * 100.0)

    sensitivities: list[dict[str, float]] = []
    raw_sensitivity = cost_model.get("sensitivityBps")
    if isinstance(raw_sensitivity, list):
        for raw_bps in raw_sensitivity:
            try:
                bps = max(0.0, float(raw_bps))
            except Exception:
                continue
            fee_cum_at_bps = turnover_notional_cum * (bps / 10_000.0)
            extra_vs_actual = max(0.0, fee_cum_at_bps - fees_cost_cum)
            net_cum_at_bps = net_cum_pnl - extra_vs_actual
            sensitivities.append(
                {
                    "fees_bps": round(bps, 4),
                    "net_cum_pnl": round(net_cum_at_bps, 6),
                    "net_cum_return_pct": round((net_cum_at_bps / initial_cash) * 100.0 if initial_cash else 0.0, 6),
                }
            )

    metric = {
        "season_id": season_id,
        "asOf": as_of,
        "cash": float(round(cash, 6)),
        "equity": float(round(equity, 6)),
        "daily_pnl": float(round(net_daily_pnl, 6)),
        "cum_pnl": float(round(net_cum_pnl, 6)),
        "cum_return_pct": float(round(net_cum_return_pct, 6)),
        "max_drawdown_pct": float(round(max_drawdown_pct, 6)),
        "holdings_count": int(len({p["ticker"] for p in positions_after})),
        "goal20_reached": bool(net_cum_return_pct >= goal20),
        "goal30_reached": bool(net_cum_return_pct >= goal30),
        "game_over": bool(net_cum_return_pct <= game_over_threshold),
        "gross_daily_pnl": float(round(gross_daily_pnl, 6)),
        "gross_cum_pnl": float(round(gross_cum_pnl, 6)),
        "gross_cum_return_pct": float(round(gross_cum_return_pct, 6)),
        "net_daily_pnl": float(round(net_daily_pnl, 6)),
        "net_cum_pnl": float(round(net_cum_pnl, 6)),
        "net_cum_return_pct": float(round(net_cum_return_pct, 6)),
        "fees_cost_daily": float(round(fees_cost_daily, 6)),
        "slippage_cost_daily": float(round(slippage_cost_daily, 6)),
        "borrow_cost_daily": float(round(borrow_cost_daily, 6)),
        "fees_cost_cum": float(round(fees_cost_cum, 6)),
        "slippage_cost_cum": float(round(slippage_cost_cum, 6)),
        "borrow_cost_cum": float(round(borrow_cost_cum, 6)),
        "turnover_notional_daily": float(round(turnover_notional_daily, 6)),
        "turnover_notional_cum": float(round(turnover_notional_cum, 6)),
        "turnover_pct_daily": float(round(turnover_pct_daily, 6)),
        "long_units": int(long_units),
        "short_units": int(short_units),
        "gross_units": int(gross_units),
        "net_units": int(net_units),
        "net_exposure_pct": float(round(net_exposure_pct, 6)),
        "risk_gate_pass": True,
        "risk_gate_reason": "",
        "cost_sensitivity": sensitivities,
    }

    repo.save_trades(trades)
    repo.replace_positions(season_id, positions_after)
    repo.save_daily_metrics(metric)

    return {
        "trades": trades,
        "positions": positions_after,
        "metrics": metric,
        "costs": {
            "gross_daily_pnl": float(round(gross_daily_pnl, 6)),
            "net_daily_pnl": float(round(net_daily_pnl, 6)),
            "fees_cost_daily": float(round(fees_cost_daily, 6)),
            "slippage_cost_daily": float(round(slippage_cost_daily, 6)),
            "borrow_cost_daily": float(round(borrow_cost_daily, 6)),
            "sensitivity": sensitivities,
        },
    }
