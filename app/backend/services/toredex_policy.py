from __future__ import annotations

from functools import lru_cache
import math
from typing import Any

from app.backend.services.toredex_config import ToredexConfig
from app.backend.services.toredex_models import ALLOWED_UNIT_SET


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
    switch_gap = float(th.get("switchMinEvGap", 0.03))
    take_profit_hint = float(th.get("takeProfitHintPct", 10.0))

    prev_cum_return = _float_or_none((prev_metrics or {}).get("cum_return_pct")) or 0.0
    goal20 = float(config.stage_rules.get("goal20Pct", 20.0))
    goal30 = float(config.stage_rules.get("goal30Pct", 30.0))
    goal20_reached = prev_cum_return >= goal20
    goal30_reached = prev_cum_return >= goal30
    stage2_active = goal20_reached

    buy_map = {str(item.get("ticker") or ""): item for item in buy_rankings if isinstance(item, dict)}
    sell_map = {str(item.get("ticker") or ""): item for item in sell_rankings if isinstance(item, dict)}

    actions: list[dict[str, Any]] = []

    # 1) Risk hard rules
    sorted_positions = sorted(
        [p for p in positions if isinstance(p, dict)],
        key=lambda p: (str(p.get("ticker") or ""), str(p.get("side") or "LONG")),
    )
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

        ref = buy_map.get(ticker) if side == "LONG" else sell_map.get(ticker)
        if not isinstance(ref, dict):
            continue
        gate = ref.get("gate") if isinstance(ref.get("gate"), dict) else {}
        gate_ok = bool(gate.get("ok"))
        ev = _float_or_none(ref.get("ev"))
        up_prob = _float_or_none(ref.get("upProb"))
        rev_risk = _float_or_none(ref.get("revRisk"))
        pnl_pct = _float_or_none(pos.get("pnlPct")) or 0.0

        if not gate_ok:
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
            gate = item.get("gate") if isinstance(item.get("gate"), dict) else {}
            if not bool(gate.get("ok")):
                continue
            up_prob = _float_or_none(item.get("upProb"))
            ev = _float_or_none(item.get("ev"))
            if up_prob is None or ev is None:
                continue
            if up_prob < entry_min_up or ev < entry_min_ev:
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
            if up_prob is None or ev is None:
                continue
            if up_prob < entry_min_up or ev < entry_min_ev:
                continue
            candidates.append({"side": "SHORT", **item})

    candidates.sort(key=_build_candidate_key)

    switched = False
    for rank_index, cand in enumerate(candidates):
        ticker = str(cand.get("ticker") or "")
        side = str(cand.get("side") or "LONG").upper()
        if not ticker:
            continue

        state_now = _state_after_actions(positions, actions)
        units_now = int(state_now.get((ticker, side), 0))

        if units_now > 0:
            add_qty = _next_add_units(units_now)
            if add_qty in {2, 3, 5} and units_now + add_qty <= 10:
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
            reason_id = "E_NEW_TOP1_GATE_OK" if rank_index == 0 else "E_NEW_TOPK_GATE_OK"
            _add_action(actions, ticker, side, 2, reason_id)
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
        _add_action(actions, ticker, side, 2, "E_NEW_SWITCH_IN")
        switched = True

    final_state = _state_after_actions(positions, actions)
    max_holdings_ok = len(_distinct_tickers(final_state)) <= int(config.max_holdings)
    unit_rule_ok = all(int(action.get("deltaUnits") or 0) in ALLOWED_UNIT_SET for action in actions)

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
            "noFutureLeakOk": bool(no_future),
        },
        "stage": {
            "goal20Reached": bool(goal20_reached),
            "goal30Reached": bool(goal30_reached),
            "stage2Active": bool(stage2_active),
        },
    }
    return decision
