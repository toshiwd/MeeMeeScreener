from __future__ import annotations

import math
import os
from typing import Any

from . import swing_expectancy_service

_MIN_LIQUIDITY_20D = 50_000_000.0
_MIN_SETUP_SAMPLES = max(0, int(os.getenv("MEEMEE_SWING_MIN_SETUP_SAMPLES", "80")))
_MIN_SETUP_WIN_RATE = float(os.getenv("MEEMEE_SWING_MIN_SETUP_WIN_RATE", "0.54"))
_MIN_SETUP_SHRUNK_MEAN = float(os.getenv("MEEMEE_SWING_MIN_SETUP_SHRUNK_MEAN", "0.003"))
_MIN_SETUP_P10_RET = float(os.getenv("MEEMEE_SWING_MIN_SETUP_P10_RET", "-0.13"))
_MIN_SETUP_MAX_ADVERSE = float(os.getenv("MEEMEE_SWING_MIN_SETUP_MAX_ADVERSE", "-0.45"))
_MIN_SWING_SCORE = float(os.getenv("MEEMEE_SWING_MIN_SCORE", "0.62"))
_MIN_LONG_PROB = float(os.getenv("MEEMEE_SWING_MIN_LONG_PROB", "0.53"))
_MIN_SHORT_PROB = float(os.getenv("MEEMEE_SWING_MIN_SHORT_PROB", "0.53"))
_MIN_LONG_EV = float(os.getenv("MEEMEE_SWING_MIN_LONG_EV", "-0.005"))
_MAX_SHORT_EV = float(os.getenv("MEEMEE_SWING_MAX_SHORT_EV", "0.005"))
_MIN_SHORT_SCORE_GATE = float(os.getenv("MEEMEE_SWING_MIN_SHORT_SCORE", "78"))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _clip(value: float, low: float, high: float) -> float:
    return float(max(low, min(high, value)))


def _prob(value: float | None) -> float:
    if value is None:
        return 0.5
    return _clip(float(value), 0.0, 1.0)


def _norm(value: float | None, *, low: float, high: float, default: float = 0.5) -> float:
    if value is None:
        return float(default)
    if high <= low:
        return float(default)
    return _clip((float(value) - float(low)) / float(high - low), 0.0, 1.0)


def _regime_fit(side: str, tone: str | None) -> float:
    normalized_side = "long" if str(side).lower() == "long" else "short"
    normalized_tone = str(tone or "").strip().lower()
    if normalized_tone == "up":
        return 1.0 if normalized_side == "long" else 0.20
    if normalized_tone == "down":
        return 1.0 if normalized_side == "short" else 0.20
    if normalized_tone == "neutral":
        return 0.60
    return 0.50


def _build_side_reasons(
    *,
    side: str,
    score: float,
    edge: float,
    risk: float,
    setup: dict[str, Any],
    gate_passed: bool,
) -> list[str]:
    label = "LONG" if side == "long" else "SHORT"
    reasons = [
        f"{label} score={score:.3f} edge={edge:.3f} risk={risk:.3f}",
        f"setup={setup.get('setupType')} n={int(setup.get('samples') or 0)} mean={float(setup.get('shrunkMeanRet') or 0.0):+.4f}",
    ]
    reasons.append("gate=PASS" if gate_passed else "gate=FAIL")
    return reasons


def _evaluate_setup_quality(setup: dict[str, Any]) -> tuple[bool, list[str]]:
    samples = int(setup.get("samples") or 0)
    win_rate = _to_float(setup.get("winRate"))
    shrunk_mean = _to_float(setup.get("shrunkMeanRet"))
    p10_ret = _to_float(setup.get("p10Ret"))
    max_adverse = _to_float(setup.get("maxAdverse"))
    # Old rows can include split/noise outliers (e.g. < -1.0). Ignore those for gating.
    effective_max_adverse = (
        max_adverse if (max_adverse is not None and -1.0 <= max_adverse <= 0.0) else None
    )

    checks: list[tuple[str, bool]] = [
        ("samples", samples >= _MIN_SETUP_SAMPLES),
        ("win_rate", win_rate is not None and win_rate >= _MIN_SETUP_WIN_RATE),
        ("shrunk_mean", shrunk_mean is not None and shrunk_mean >= _MIN_SETUP_SHRUNK_MEAN),
        ("p10_ret", p10_ret is not None and p10_ret >= _MIN_SETUP_P10_RET),
        (
            "max_adverse",
            True
            if effective_max_adverse is None
            else effective_max_adverse >= _MIN_SETUP_MAX_ADVERSE,
        ),
    ]
    passed = all(flag for _, flag in checks)
    details = [
        "setup_quality="
        + ("PASS" if passed else "FAIL")
        + f"(samples={samples},win={0.0 if win_rate is None else win_rate:.3f},"
        + f"shrunk={0.0 if shrunk_mean is None else shrunk_mean:+.4f},"
        + f"p10={0.0 if p10_ret is None else p10_ret:+.4f},"
        + f"max_adv={0.0 if max_adverse is None else max_adverse:+.4f},"
        + ("max_adv_ignored=1" if effective_max_adverse is None else "max_adv_ignored=0")
        + ")",
        "setup_checks=" + ",".join(f"{name}:{'ok' if ok else 'ng'}" for name, ok in checks),
    ]
    return passed, details


def evaluate_swing_candidates(
    *,
    as_of_ymd: int | None,
    p_up: float | None,
    p_down: float | None,
    p_turn_up: float | None,
    p_turn_down: float | None,
    ev20_net: float | None,
    long_setup_type: str | None,
    short_setup_type: str | None,
    playbook_bonus_long: float | None,
    playbook_bonus_short: float | None,
    short_score: float | None,
    atr_pct: float | None,
    liquidity20d: float | None,
) -> dict[str, Any]:
    up_prob = _prob(p_up if p_up is not None else (1.0 - p_down if p_down is not None else None))
    down_prob = _prob(p_down if p_down is not None else (1.0 - up_prob))
    turn_up = _prob(p_turn_up if p_turn_up is not None else (1.0 - p_turn_down if p_turn_down is not None else None))
    turn_down = _prob(p_turn_down if p_turn_down is not None else (1.0 - turn_up))
    ev_net = _to_float(ev20_net)
    short_score_val = _to_float(short_score)
    atr_value = _to_float(atr_pct)
    liquidity = _to_float(liquidity20d)

    exp_long = swing_expectancy_service.resolve_setup_expectancy(
        side="long",
        setup_type=long_setup_type,
        horizon_days=20,
        as_of_ymd=as_of_ymd,
    )
    exp_short = swing_expectancy_service.resolve_setup_expectancy(
        side="short",
        setup_type=short_setup_type,
        horizon_days=20,
        as_of_ymd=as_of_ymd,
    )

    norm_ev_long = _norm(ev_net, low=-0.06, high=0.06)
    norm_ev_short = _norm((-ev_net) if ev_net is not None else None, low=-0.06, high=0.06)
    norm_setup_long = _norm(_to_float(exp_long.get("shrunkMeanRet")), low=-0.08, high=0.08)
    norm_setup_short = _norm(_to_float(exp_short.get("shrunkMeanRet")), low=-0.08, high=0.08)
    norm_playbook_long = _norm(_to_float(playbook_bonus_long), low=-0.03, high=0.03)
    norm_playbook_short = _norm(_to_float(playbook_bonus_short), low=-0.03, high=0.03)

    long_edge = _clip(
        0.30 * up_prob
        + 0.20 * turn_up
        + 0.20 * norm_ev_long
        + 0.20 * norm_setup_long
        + 0.10 * norm_playbook_long,
        0.0,
        1.0,
    )
    short_edge = _clip(
        0.30 * down_prob
        + 0.20 * turn_down
        + 0.20 * norm_ev_short
        + 0.20 * norm_setup_short
        + 0.10 * norm_playbook_short,
        0.0,
        1.0,
    )
    long_setup_ok, long_setup_notes = _evaluate_setup_quality(exp_long)
    short_setup_ok, short_setup_notes = _evaluate_setup_quality(exp_short)

    volatility_penalty = _norm(atr_value, low=0.01, high=0.09, default=0.35)
    if liquidity is None:
        liquidity_penalty = 1.0
    else:
        liquidity_penalty = _clip((_MIN_LIQUIDITY_20D - liquidity) / _MIN_LIQUIDITY_20D, 0.0, 1.0)
    down_pressure = down_prob
    short_score_penalty = _norm((78.0 - short_score_val) if short_score_val is not None else None, low=0.0, high=30.0)
    short_squeeze_risk = _clip(0.65 * turn_up + 0.35 * short_score_penalty, 0.0, 1.0)

    long_risk = _clip(0.45 * down_pressure + 0.25 * volatility_penalty + 0.30 * liquidity_penalty, 0.0, 1.0)
    short_risk = _clip(0.45 * short_squeeze_risk + 0.25 * volatility_penalty + 0.30 * liquidity_penalty, 0.0, 1.0)

    long_score = _clip(0.68 * long_edge + 0.32 * (1.0 - long_risk), 0.0, 1.0)
    short_score_total = _clip(0.68 * short_edge + 0.32 * (1.0 - short_risk), 0.0, 1.0)

    long_gate_checks = {
        "score": long_score >= _MIN_SWING_SCORE,
        "prob": up_prob >= _MIN_LONG_PROB,
        "ev": (ev_net is not None and ev_net >= _MIN_LONG_EV),
        "setup": long_setup_ok,
    }
    short_gate_checks = {
        "score": short_score_total >= _MIN_SWING_SCORE,
        "prob": down_prob >= _MIN_SHORT_PROB,
        "ev": (ev_net is not None and ev_net <= _MAX_SHORT_EV),
        "short_score": (short_score_val is not None and short_score_val >= _MIN_SHORT_SCORE_GATE),
        "setup": short_setup_ok,
    }
    long_gate = all(bool(flag) for flag in long_gate_checks.values())
    short_gate = all(bool(flag) for flag in short_gate_checks.values())

    long_reasons = _build_side_reasons(
        side="long",
        score=long_score,
        edge=long_edge,
        risk=long_risk,
        setup=exp_long,
        gate_passed=long_gate,
    )
    long_reasons.extend(long_setup_notes)
    long_reasons.append(
        "gate_checks=" + ",".join(f"{name}:{'ok' if ok else 'ng'}" for name, ok in long_gate_checks.items())
    )
    short_reasons = _build_side_reasons(
        side="short",
        score=short_score_total,
        edge=short_edge,
        risk=short_risk,
        setup=exp_short,
        gate_passed=short_gate,
    )
    short_reasons.extend(short_setup_notes)
    short_reasons.append(
        "gate_checks=" + ",".join(f"{name}:{'ok' if ok else 'ng'}" for name, ok in short_gate_checks.items())
    )

    selected_side = "none"
    if long_gate and short_gate:
        selected_side = "long" if long_score >= short_score_total else "short"
    elif long_gate:
        selected_side = "long"
    elif short_gate:
        selected_side = "short"

    return {
        "selectedSide": selected_side,
        "long": {
            "score": float(long_score),
            "edge": float(long_edge),
            "risk": float(long_risk),
            "qualified": bool(long_gate),
            "reasons": long_reasons,
            "setupExpectancy": exp_long,
            "setupQualityPassed": bool(long_setup_ok),
        },
        "short": {
            "score": float(short_score_total),
            "edge": float(short_edge),
            "risk": float(short_risk),
            "qualified": bool(short_gate),
            "reasons": short_reasons,
            "setupExpectancy": exp_short,
            "setupQualityPassed": bool(short_setup_ok),
        },
    }


def build_swing_plan(
    *,
    code: str | None,
    as_of_ymd: int | None,
    close: float | None,
    p_up: float | None,
    p_down: float | None,
    p_turn_up: float | None,
    p_turn_down: float | None,
    ev20_net: float | None,
    long_setup_type: str | None,
    short_setup_type: str | None,
    playbook_bonus_long: float | None,
    playbook_bonus_short: float | None,
    short_score: float | None,
    atr_pct: float | None,
    liquidity20d: float | None,
    decision_tone: str | None,
    hold_days_long: int | None,
    hold_days_short: int | None,
) -> dict[str, Any]:
    candidates = evaluate_swing_candidates(
        as_of_ymd=as_of_ymd,
        p_up=p_up,
        p_down=p_down,
        p_turn_up=p_turn_up,
        p_turn_down=p_turn_down,
        ev20_net=ev20_net,
        long_setup_type=long_setup_type,
        short_setup_type=short_setup_type,
        playbook_bonus_long=playbook_bonus_long,
        playbook_bonus_short=playbook_bonus_short,
        short_score=short_score,
        atr_pct=atr_pct,
        liquidity20d=liquidity20d,
    )
    selected_side = str(candidates.get("selectedSide") or "none")
    selected_payload = (
        candidates.get("long")
        if selected_side == "long"
        else candidates.get("short")
        if selected_side == "short"
        else None
    )
    entry = _to_float(close)
    atr_value = _to_float(atr_pct)
    risk_pct = _clip((atr_value if atr_value is not None else 0.03) * 1.6, 0.02, 0.08)
    hold_days = int(hold_days_long or 20) if selected_side == "long" else int(hold_days_short or 20)
    hold_days = max(10, min(25, hold_days))

    plan: dict[str, Any] | None = None
    if (
        selected_payload
        and entry is not None
        and entry > 0
        and bool(selected_payload.get("qualified"))
        and selected_side in {"long", "short"}
    ):
        if selected_side == "long":
            stop = entry * (1.0 - risk_pct)
            tp1 = entry * (1.0 + risk_pct * 1.2)
            tp2 = entry * (1.0 + risk_pct * 2.0)
        else:
            stop = entry * (1.0 + risk_pct)
            tp1 = entry * (1.0 - risk_pct * 1.2)
            tp2 = entry * (1.0 - risk_pct * 2.0)
        plan = {
            "code": str(code or "").strip() or None,
            "side": selected_side,
            "score": float(selected_payload.get("score") or 0.0),
            "horizonDays": 20,
            "entry": float(entry),
            "stop": float(stop),
            "tp1": float(tp1),
            "tp2": float(tp2),
            "timeStopDays": int(hold_days),
            "reasons": list(selected_payload.get("reasons") or []),
        }

    selected_diagnostics = selected_payload if isinstance(selected_payload, dict) else {}
    return {
        "plan": plan,
        "diagnostics": {
            "edge": _to_float(selected_diagnostics.get("edge")),
            "risk": _to_float(selected_diagnostics.get("risk")),
            "setupExpectancy": selected_diagnostics.get("setupExpectancy"),
            "regimeFit": _regime_fit(selected_side if selected_side in {"long", "short"} else "long", decision_tone),
            "atrPct": _to_float(atr_pct),
            "liquidity20d": _to_float(liquidity20d),
            "long": candidates.get("long"),
            "short": candidates.get("short"),
        },
    }
