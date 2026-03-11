from __future__ import annotations

from functools import lru_cache
import math
from typing import Any

from .toredex_config import ToredexConfig
from .toredex_models import ALLOWED_UNIT_SET


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


def _round6(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


@lru_cache(maxsize=256)
def _decompose_reduce_units(units: int) -> tuple[int, ...]:
    if units == 0:
        return ()
    if units < 0:
        return tuple()
    for step in (5, 3, 2):
        if units - step < 0:
            continue
        tail = _decompose_reduce_units(units - step)
        if units - step == 0 or tail:
            return (step, *tail)
    return tuple()


def _next_add_units(current_units: int) -> int:
    if current_units <= 0:
        return 2
    if current_units <= 2:
        return 3
    if current_units <= 5:
        return 5
    if current_units == 7:
        return 3
    if current_units == 8:
        return 2
    return 0


def _build_candidate_key(item: dict[str, Any]) -> tuple[Any, ...]:
    ev = _float_or_none(item.get("ev"))
    up_prob = _float_or_none(item.get("upProb"))
    rev_risk = _float_or_none(item.get("revRisk"))
    score = _float_or_none(item.get("entryScore"))
    ticker = str(item.get("ticker") or "")
    return (
        -(score if score is not None else -1e9),
        -(ev if ev is not None else -1e9),
        -(up_prob if up_prob is not None else -1e9),
        (rev_risk if rev_risk is not None else 1e9),
        ticker,
    )


def _is_crash_dip_short_candidate(
    item: dict[str, Any],
    *,
    dip_pct: float,
    min_down_prob: float,
    min_turn_down: float,
    max_ev: float,
) -> bool:
    change_pct = _float_or_none(item.get("changePct"))
    if change_pct is None or change_pct > dip_pct:
        return False

    down_prob = _float_or_none(item.get("pDown"))
    if down_prob is None:
        down_prob = _float_or_none(item.get("upProb"))
    if down_prob is None or down_prob < min_down_prob:
        return False

    turn_down = _float_or_none(item.get("pTurnDown"))
    if turn_down is None:
        turn_down = _float_or_none(item.get("revRisk"))
    if turn_down is None or turn_down < min_turn_down:
        return False

    ev20_net = _float_or_none(item.get("ev20Net"))
    if ev20_net is None:
        ev20_net = _float_or_none(item.get("ev"))
    if ev20_net is None or ev20_net > max_ev:
        return False

    return True


def _position_key(pos: dict[str, Any]) -> tuple[str, str]:
    return (str(pos.get("ticker") or ""), str(pos.get("side") or "LONG").upper())


def _state_after_actions(
    positions: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> dict[tuple[str, str], int]:
    state: dict[tuple[str, str], int] = {}
    for pos in positions:
        key = _position_key(pos)
        state[key] = int(pos.get("units") or 0)
    for action in actions:
        key = (str(action.get("ticker") or ""), str(action.get("side") or "LONG").upper())
        state[key] = int(state.get(key, 0)) + int(action.get("deltaUnits") or 0)
        if state[key] <= 0:
            state[key] = 0
    return state


def _distinct_tickers(state: dict[tuple[str, str], int]) -> set[str]:
    out: set[str] = set()
    for (ticker, _side), units in state.items():
        if units > 0 and ticker:
            out.add(ticker)
    return out


def _gross_units(state: dict[tuple[str, str], int]) -> int:
    total = 0
    for units in state.values():
        ui = int(units or 0)
        if ui > 0:
            total += ui
    return total


def _long_short_units(state: dict[tuple[str, str], int]) -> tuple[int, int]:
    long_units = 0
    short_units = 0
    for (_ticker, side), units in state.items():
        ui = int(units or 0)
        if ui <= 0:
            continue
        if str(side).upper() == "SHORT":
            short_units += ui
        else:
            long_units += ui
    return long_units, short_units


def _units_for_ticker(state: dict[tuple[str, str], int], ticker: str) -> int:
    total = 0
    for (tk, _side), units in state.items():
        if tk == ticker and int(units or 0) > 0:
            total += int(units or 0)
    return total


def _sector_counts(state: dict[tuple[str, str], int], sector_map: dict[str, str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    tickers = _distinct_tickers(state)
    for ticker in tickers:
        sector = str(sector_map.get(ticker) or "").strip()
        if not sector:
            continue
        counts[sector] = int(counts.get(sector, 0)) + 1
    return counts


def _append_close_all_actions(
    actions: list[dict[str, Any]],
    *,
    ticker: str,
    side: str,
    units: int,
    reason_id: str,
) -> None:
    parts = _decompose_reduce_units(int(units))
    if not parts:
        return
    for part in parts:
        actions.append(
            {
                "ticker": ticker,
                "side": side,
                "deltaUnits": -int(part),
                "reasonId": reason_id,
            }
        )


def _add_action(actions: list[dict[str, Any]], ticker: str, side: str, delta: int, reason: str, notes: str | None = None) -> None:
    payload: dict[str, Any] = {
        "ticker": ticker,
        "side": side,
        "deltaUnits": int(delta),
        "reasonId": reason,
    }
    if notes:
        payload["notes"] = notes
    actions.append(payload)


def _has_reduce_action(actions: list[dict[str, Any]], *, ticker: str, side: str) -> bool:
    for action in actions:
        if not isinstance(action, dict):
            continue
        if str(action.get("ticker") or "") != ticker:
            continue
        if str(action.get("side") or "LONG").upper() != side:
            continue
        if int(action.get("deltaUnits") or 0) < 0:
            return True
    return False


def _should_exit_on_gate_ng(
    *,
    pnl_pct: float,
    holding_days: int,
    rev_risk: float | None,
    rev_risk_high: float,
    gate_ng_min_holding_days: int,
    gate_ng_min_pnl_pct: float,
) -> bool:
    # If reversal risk is already high, allow immediate exit on gate miss.
    if rev_risk is not None and rev_risk >= rev_risk_high:
        return True
    # Allow immediate exit when position is not in profit enough.
    if pnl_pct <= gate_ng_min_pnl_pct:
        return True
    # Otherwise require a minimum holding period before exiting on gate miss.
    return int(holding_days) >= int(gate_ng_min_holding_days)


def _can_apply_exposure_constraints(
    *,
    state_now: dict[tuple[str, str], int],
    ticker: str,
    side: str,
    delta: int,
    max_gross_units: int,
    max_net_units: int,
    max_units_per_ticker: int,
    max_per_sector: int,
    sector_map: dict[str, str],
) -> bool:
    next_state = dict(state_now)
    key = (ticker, side)
    next_state[key] = int(next_state.get(key, 0)) + int(delta)
    if next_state[key] <= 0:
        next_state[key] = 0

    if _gross_units(next_state) > max_gross_units:
        return False
    long_units, short_units = _long_short_units(next_state)
    if abs(long_units - short_units) > max_net_units:
        return False
    if _units_for_ticker(next_state, ticker) > max_units_per_ticker:
        return False
    if max_per_sector > 0:
        sector = str(sector_map.get(ticker) or "").strip()
        if sector:
            counts = _sector_counts(next_state, sector_map)
            if int(counts.get(sector, 0)) > max_per_sector:
                return False
    return True


def build_decision(
    *,
    snapshot: dict[str, Any],
    config: ToredexConfig,
    prev_metrics: dict[str, Any] | None,
    mode: str = "LIVE",
) -> dict[str, Any]:
    as_of = str(snapshot.get("asOf") or "")
    season_id = str(snapshot.get("seasonId") or "")

    positions = snapshot.get("positions") if isinstance(snapshot.get("positions"), list) else []
    rankings = snapshot.get("rankings") if isinstance(snapshot.get("rankings"), dict) else {}
    buy_rankings = rankings.get("buy") if isinstance(rankings.get("buy"), list) else []
    sell_rankings = rankings.get("sell") if isinstance(rankings.get("sell"), list) else []

    th = config.thresholds
    cut_warn = float(th.get("cutLossWarnPct", -8.0))
    cut_hard = float(th.get("cutLossHardPct", -10.0))
    entry_min_up = float(th.get("entryMinUpProb", 0.55))
    entry_min_ev = float(th.get("entryMinEv", 0.0))
    exit_min_up = float(th.get("exitMinUpProb", 0.45))
    exit_min_ev = float(th.get("exitMinEv", -0.01))
    rev_risk_warn = float(th.get("revRiskWarn", 0.55))
    rev_risk_high = float(th.get("revRiskHigh", 0.65))
    entry_max_rev_risk = float(th.get("entryMaxRevRisk", rev_risk_warn))
    crash_dip_bonus_enabled = float(th.get("crashDipBonusEnabled", 1.0)) >= 0.5
    crash_dip_bonus = float(th.get("crashDipBonus", 0.08))
    crash_dip_pct = float(th.get("crashDipPct", -0.07))
    crash_dip_min_down_prob = float(th.get("crashDipMinDownProb", 0.57))
    crash_dip_min_turn_down = float(th.get("crashDipMinTurnDown", 0.52))
    crash_dip_max_ev = float(th.get("crashDipMaxEv", 0.0))
    crash_dip_max_per_day = max(0, int(float(th.get("crashDipMaxPerDay", 1.0))))
    topk_entry_up_boost = float(th.get("topKEntryUpProbBoost", 0.0))
    topk_entry_ev_boost = float(th.get("topKEntryEvBoost", 0.0))
    add_min_up = float(th.get("addMinUpProb", entry_min_up))
    add_min_ev = float(th.get("addMinEv", entry_min_ev))
    add_max_rev_risk = float(th.get("addMaxRevRisk", rev_risk_warn))
    add_min_pnl = float(th.get("addMinPnlPct", -1.0))
    add_max_pnl = float(th.get("addMaxPnlPct", 15.0))
    switch_gap = float(th.get("switchMinEvGap", 0.03))
    take_profit_hint = float(th.get("takeProfitHintPct", 10.0))
    exit_if_unranked = float(th.get("exitIfUnranked", 1.0)) >= 0.5
    exit_gate_ng_min_holding_days = max(0, int(float(th.get("exitGateNgMinHoldingDays", 10.0))))
    exit_gate_ng_min_pnl_pct = float(th.get("exitGateNgMinPnlPct", 0.0))
    max_new_entries_per_day = max(0, int(float(th.get("maxNewEntriesPerDay", 1.0))))
    new_entry_max_rank = max(1, int(float(th.get("newEntryMaxRank", 3.0))))
    unit_notional = float(config.max_per_ticker) / 10.0 if float(config.max_per_ticker) > 0 else 0.0
    equity_for_limit = _float_or_none((prev_metrics or {}).get("equity"))
    if equity_for_limit is None or equity_for_limit <= 0:
        equity_for_limit = float(config.initial_cash)
    if unit_notional > 0:
        max_gross_units = max(1, int(float(equity_for_limit) // unit_notional))
    else:
        max_gross_units = 10
    constraints = config.portfolio_constraints if hasattr(config, "portfolio_constraints") else {}
    configured_gross_units = max(1, int(constraints.get("grossUnitsCap", 10)))
    max_gross_units = min(max_gross_units, configured_gross_units)
    max_net_units = max(0, int(constraints.get("maxNetUnits", configured_gross_units)))
    max_units_per_ticker = max(1, int(constraints.get("maxUnitsPerTicker", 10)))
    max_per_sector = max(0, int(constraints.get("maxPerSector", 0)))
    min_liquidity20d = max(0.0, float(constraints.get("minLiquidity20d", 0.0)))
    short_blacklist = {str(v).strip() for v in (constraints.get("shortBlacklist") or []) if str(v).strip()}

    prev_cum_return = _float_or_none((prev_metrics or {}).get("cum_return_pct")) or 0.0
    goal20 = float(config.stage_rules.get("goal20Pct", 20.0))
    goal30 = float(config.stage_rules.get("goal30Pct", 30.0))
    goal20_reached = prev_cum_return >= goal20
    goal30_reached = prev_cum_return >= goal30
    stage2_active = goal20_reached

    buy_map = {str(item.get("ticker") or ""): item for item in buy_rankings if isinstance(item, dict)}
    sell_map = {str(item.get("ticker") or ""): item for item in sell_rankings if isinstance(item, dict)}
    sector_map: dict[str, str] = {}
    for item in [*buy_rankings, *sell_rankings]:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "")
        if not ticker:
            continue
        sector_map[ticker] = str(item.get("sector") or "")

    actions: list[dict[str, Any]] = []
    new_entries_today = 0
    crash_new_entries_today = 0

    # 1) Risk hard rules
    sorted_positions = sorted(
        [p for p in positions if isinstance(p, dict)],
        key=lambda p: (str(p.get("ticker") or ""), str(p.get("side") or "LONG")),
    )
    position_map = {_position_key(pos): pos for pos in sorted_positions}
    for pos in sorted_positions:
        ticker = str(pos.get("ticker") or "")
        side = str(pos.get("side") or "LONG").upper()
        units = int(pos.get("units") or 0)
        if not ticker or units <= 0:
            continue
        pnl_pct = _float_or_none(pos.get("pnlPct")) or 0.0
        if pnl_pct <= cut_hard:
            _append_close_all_actions(actions, ticker=ticker, side=side, units=units, reason_id="R_CUT_LOSS_HARD")
            continue
        if pnl_pct <= cut_warn:
            reduce_units = 5 if units >= 7 else (3 if units >= 5 else (2 if units >= 2 else 0))
            if reduce_units > 0:
                _add_action(actions, ticker, side, -reduce_units, "R_CUT_LOSS_WARN")

    # 2) Exit / take-profit
    for pos in sorted_positions:
        ticker = str(pos.get("ticker") or "")
        side = str(pos.get("side") or "LONG").upper()
        if not ticker:
            continue
        current_state = _state_after_actions(positions, actions)
        units = int(current_state.get((ticker, side), 0))
        if units <= 0:
            continue

        pnl_pct = _float_or_none(pos.get("pnlPct")) or 0.0
        holding_days = max(0, int(pos.get("holdingDays") or 0))
        ref = buy_map.get(ticker) if side == "LONG" else sell_map.get(ticker)
        if not isinstance(ref, dict):
            if exit_if_unranked and _should_exit_on_gate_ng(
                pnl_pct=pnl_pct,
                holding_days=holding_days,
                rev_risk=None,
                rev_risk_high=rev_risk_high,
                gate_ng_min_holding_days=exit_gate_ng_min_holding_days,
                gate_ng_min_pnl_pct=exit_gate_ng_min_pnl_pct,
            ):
                _append_close_all_actions(actions, ticker=ticker, side=side, units=units, reason_id="X_EXIT_GATE_NG")
            continue
        gate = ref.get("gate") if isinstance(ref.get("gate"), dict) else {}
        gate_ok = bool(gate.get("ok"))
        ev = _float_or_none(ref.get("ev"))
        up_prob = _float_or_none(ref.get("upProb"))
        rev_risk = _float_or_none(ref.get("revRisk"))

        if not gate_ok:
            signal_weak = (
                (ev is not None and ev < exit_min_ev)
                or (up_prob is not None and up_prob < exit_min_up)
            )
            if _should_exit_on_gate_ng(
                pnl_pct=pnl_pct,
                holding_days=holding_days,
                rev_risk=rev_risk,
                rev_risk_high=rev_risk_high,
                gate_ng_min_holding_days=exit_gate_ng_min_holding_days,
                gate_ng_min_pnl_pct=exit_gate_ng_min_pnl_pct,
            ) and (
                bool(signal_weak) or (rev_risk is not None and rev_risk >= rev_risk_high)
            ):
                _append_close_all_actions(actions, ticker=ticker, side=side, units=units, reason_id="X_EXIT_GATE_NG")
            continue
        if ev is not None and ev < exit_min_ev:
            _append_close_all_actions(actions, ticker=ticker, side=side, units=units, reason_id="X_EXIT_EV_DROP")
            continue
        if up_prob is not None and up_prob < exit_min_up:
            _append_close_all_actions(actions, ticker=ticker, side=side, units=units, reason_id="X_EXIT_UPPROB_DROP")
            continue
        if rev_risk is not None and rev_risk >= rev_risk_high:
            _append_close_all_actions(actions, ticker=ticker, side=side, units=units, reason_id="X_EXIT_REV_RISK_HIGH")
            continue

        if pnl_pct >= take_profit_hint and units >= 5:
            reduce_units = 3 if units >= 8 else 2
            reason = "T_TP_PARTIAL_5_TO_3" if reduce_units == 3 else "T_TP_PARTIAL_3_TO_2"
            _add_action(actions, ticker, side, -reduce_units, reason)
            continue
        if rev_risk is not None and rev_risk >= rev_risk_warn and units >= 5:
            reduce_units = 3 if units >= 8 else 2
            _add_action(actions, ticker, side, -reduce_units, "T_TP_REV_RISK_RISING")
            continue

    # 3) Entry / add / switch
    candidates: list[dict[str, Any]] = []
    if bool(config.sides.get("longEnabled", True)):
        for item in buy_rankings:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "")
            if not ticker:
                continue
            regime = str(item.get("regime") or "").upper()
            if regime == "DOWN_WEAK":
                continue
            gate = item.get("gate") if isinstance(item.get("gate"), dict) else {}
            if not bool(gate.get("ok")):
                continue
            up_prob = _float_or_none(item.get("upProb"))
            ev = _float_or_none(item.get("ev"))
            rev_risk = _float_or_none(item.get("revRisk"))
            liquidity20d = _float_or_none(item.get("liquidity20d"))
            if up_prob is None or ev is None:
                continue
            if up_prob < entry_min_up or ev < entry_min_ev:
                continue
            if rev_risk is not None and rev_risk > entry_max_rev_risk:
                continue
            if min_liquidity20d > 0 and (liquidity20d is None or liquidity20d < min_liquidity20d):
                continue
            candidates.append({"side": "LONG", **item})

    if bool(config.sides.get("shortEnabled", False)):
        for item in sell_rankings:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "")
            if not ticker:
                continue
            gate = item.get("gate") if isinstance(item.get("gate"), dict) else {}
            if not bool(gate.get("ok")):
                continue
            up_prob = _float_or_none(item.get("upProb"))
            ev = _float_or_none(item.get("ev"))
            rev_risk = _float_or_none(item.get("revRisk"))
            liquidity20d = _float_or_none(item.get("liquidity20d"))
            shortable = bool(item.get("shortable", True))
            if up_prob is None or ev is None:
                continue
            if up_prob < entry_min_up or ev < entry_min_ev:
                continue
            if rev_risk is not None and rev_risk > entry_max_rev_risk:
                continue
            if ticker in short_blacklist or not shortable:
                continue
            if min_liquidity20d > 0 and (liquidity20d is None or liquidity20d < min_liquidity20d):
                continue
            cand = {"side": "SHORT", **item}
            crash_dip_match = False
            if crash_dip_bonus_enabled:
                crash_dip_match = _is_crash_dip_short_candidate(
                    cand,
                    dip_pct=crash_dip_pct,
                    min_down_prob=crash_dip_min_down_prob,
                    min_turn_down=crash_dip_min_turn_down,
                    max_ev=crash_dip_max_ev,
                )
                if crash_dip_match and crash_dip_bonus != 0.0:
                    base_score = _float_or_none(cand.get("entryScore")) or 0.0
                    cand["entryScore"] = max(0.0, min(1.0, float(base_score + crash_dip_bonus)))
            cand["crashDipBoostApplied"] = bool(crash_dip_match)
            candidates.append(cand)

    candidates.sort(key=_build_candidate_key)

    switched = False
    for rank_index, cand in enumerate(candidates):
        ticker = str(cand.get("ticker") or "")
        side = str(cand.get("side") or "LONG").upper()
        if not ticker:
            continue

        state_now = _state_after_actions(positions, actions)
        units_now = int(state_now.get((ticker, side), 0))
        if _has_reduce_action(actions, ticker=ticker, side=side):
            continue

        if units_now > 0:
            pos_ref = position_map.get((ticker, side), {})
            current_pnl = _float_or_none(pos_ref.get("pnlPct")) or 0.0
            cand_up_prob = _float_or_none(cand.get("upProb"))
            cand_ev = _float_or_none(cand.get("ev"))
            cand_rev_risk = _float_or_none(cand.get("revRisk"))
            if current_pnl < add_min_pnl or current_pnl > add_max_pnl:
                continue
            if cand_up_prob is not None and cand_up_prob < add_min_up:
                continue
            if cand_ev is not None and cand_ev < add_min_ev:
                continue
            if cand_rev_risk is not None and cand_rev_risk > add_max_rev_risk:
                continue
            add_qty = _next_add_units(units_now)
            if (
                add_qty in {2, 3, 5}
                and units_now + add_qty <= max_units_per_ticker
                and _can_apply_exposure_constraints(
                    state_now=state_now,
                    ticker=ticker,
                    side=side,
                    delta=add_qty,
                    max_gross_units=max_gross_units,
                    max_net_units=max_net_units,
                    max_units_per_ticker=max_units_per_ticker,
                    max_per_sector=max_per_sector,
                    sector_map=sector_map,
                )
            ):
                if stage2_active:
                    reason_id = "A_ADD_STAGE2_STRICT_OK"
                elif units_now <= 2:
                    reason_id = "A_ADD_PROBE_TO_ADD_OK"
                else:
                    reason_id = "A_ADD_ADD_TO_MAIN_OK"
                _add_action(actions, ticker, side, add_qty, reason_id)
            continue

        held_tickers = _distinct_tickers(state_now)
        if len(held_tickers) < int(config.max_holdings):
            if max_new_entries_per_day and new_entries_today >= max_new_entries_per_day:
                continue
            is_crash_candidate = bool(cand.get("crashDipBoostApplied")) and side == "SHORT"
            if is_crash_candidate and crash_dip_max_per_day and crash_new_entries_today >= crash_dip_max_per_day:
                continue
            if rank_index >= new_entry_max_rank:
                continue
            cand_up_prob = _float_or_none(cand.get("upProb"))
            cand_ev = _float_or_none(cand.get("ev"))
            rank_entry_min_up = entry_min_up
            rank_entry_min_ev = entry_min_ev
            if rank_index > 0:
                rank_entry_min_up += max(0.0, topk_entry_up_boost)
                rank_entry_min_ev += max(0.0, topk_entry_ev_boost)
            if cand_up_prob is not None and cand_up_prob < rank_entry_min_up:
                continue
            if cand_ev is not None and cand_ev < rank_entry_min_ev:
                continue
            if not _can_apply_exposure_constraints(
                state_now=state_now,
                ticker=ticker,
                side=side,
                delta=2,
                max_gross_units=max_gross_units,
                max_net_units=max_net_units,
                max_units_per_ticker=max_units_per_ticker,
                max_per_sector=max_per_sector,
                sector_map=sector_map,
            ):
                continue
            reason_id = "E_NEW_TOP1_GATE_OK" if rank_index == 0 else "E_NEW_TOPK_GATE_OK"
            notes = "CRASH_DIP_BOOST" if is_crash_candidate else None
            _add_action(actions, ticker, side, 2, reason_id, notes=notes)
            new_entries_today += 1
            if is_crash_candidate:
                crash_new_entries_today += 1
            continue

        if switched:
            continue
        candidate_ev = _float_or_none(cand.get("ev"))
        if candidate_ev is None:
            continue

        held_long = sorted(t for t in held_tickers)
        worst_ticker = None
        worst_ev = None
        for held in held_long:
            ref = buy_map.get(held) if side == "LONG" else sell_map.get(held)
            ev = _float_or_none((ref or {}).get("ev"))
            if worst_ticker is None or (ev is not None and (worst_ev is None or ev < worst_ev)):
                worst_ticker = held
                worst_ev = ev
        if not worst_ticker:
            continue
        if worst_ev is None or candidate_ev - worst_ev < switch_gap:
            continue

        worst_side = side
        worst_units = int(state_now.get((worst_ticker, worst_side), 0))
        if worst_units <= 0:
            continue
        _append_close_all_actions(actions, ticker=worst_ticker, side=worst_side, units=worst_units, reason_id="S_SWITCH_EV_GAP")
        state_after_switch_out = _state_after_actions(positions, actions)
        if not _can_apply_exposure_constraints(
            state_now=state_after_switch_out,
            ticker=ticker,
            side=side,
            delta=2,
            max_gross_units=max_gross_units,
            max_net_units=max_net_units,
            max_units_per_ticker=max_units_per_ticker,
            max_per_sector=max_per_sector,
            sector_map=sector_map,
        ):
            continue
        _add_action(actions, ticker, side, 2, "E_NEW_SWITCH_IN")
        new_entries_today += 1
        switched = True

    final_state = _state_after_actions(positions, actions)
    max_holdings_ok = len(_distinct_tickers(final_state)) <= int(config.max_holdings)
    unit_rule_ok = all(int(action.get("deltaUnits") or 0) in ALLOWED_UNIT_SET for action in actions)
    final_long_units, final_short_units = _long_short_units(final_state)
    exposure_ok = True
    if _gross_units(final_state) > max_gross_units:
        exposure_ok = False
    if abs(final_long_units - final_short_units) > max_net_units:
        exposure_ok = False
    for held_ticker in _distinct_tickers(final_state):
        if _units_for_ticker(final_state, held_ticker) > max_units_per_ticker:
            exposure_ok = False
            break
    if exposure_ok and max_per_sector > 0:
        for count in _sector_counts(final_state, sector_map).values():
            if int(count) > max_per_sector:
                exposure_ok = False
                break

    loss_limit_ok = True
    for pos in sorted_positions:
        ticker = str(pos.get("ticker") or "")
        side = str(pos.get("side") or "LONG").upper()
        pnl_pct = _float_or_none(pos.get("pnlPct")) or 0.0
        if pnl_pct <= cut_hard and int(final_state.get((ticker, side), 0)) > 0:
            loss_limit_ok = False
            break

    meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
    no_future = bool(meta.get("noFutureLeakOk", True))

    decision = {
        "asOf": as_of,
        "seasonId": season_id,
        "mode": mode,
        "policyVersion": config.policy_version,
        "actions": actions,
        "checks": {
            "maxHoldingsOk": bool(max_holdings_ok),
            "unitRuleOk": bool(unit_rule_ok),
            "lossLimitOk": bool(loss_limit_ok),
            "exposureOk": bool(exposure_ok),
            "noFutureLeakOk": bool(no_future),
        },
        "stage": {
            "goal20Reached": bool(goal20_reached),
            "goal30Reached": bool(goal30_reached),
            "stage2Active": bool(stage2_active),
        },
    }
    return decision
