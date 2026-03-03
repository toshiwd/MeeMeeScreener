from __future__ import annotations

import math
import logging
import os
from typing import Dict, Iterable, List, Sequence, Any
from threading import Lock

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.domain.screening import ranking
from app.backend.services import rankings_cache
from app.backend.services.analysis_decision import build_analysis_decision
from app.backend.services.yahoo_provisional import (
    get_provisional_daily_row_from_chart,
    merge_daily_rows_with_provisional,
)
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api/ticker", tags=["ticker"])
logger = logging.getLogger(__name__)
SYNC_BACKFILL_MAX_AGE_DAYS = max(0, int(os.getenv("MEEMEE_SYNC_BACKFILL_MAX_AGE_DAYS", "7")))
_VALID_RISK_MODES = {"defensive", "balanced", "aggressive"}
_BACKFILL_ATTEMPTS: set[tuple[str, int, bool, bool]] = set()
_BACKFILL_ATTEMPTS_LOCK = Lock()


def _normalize_rows(rows: Iterable[Sequence], *, fill_volume: bool) -> List[List[float]]:
    normalized: List[List[float]] = []
    for row in rows:
        if len(row) < 5:
            continue
        time_value, open_, high, low, close = row[:5]
        if time_value is None or open_ is None or high is None or low is None or close is None:
            continue
        volume = 0.0
        if len(row) >= 6 and row[5] is not None and fill_volume:
            try:
                volume = float(row[5])
            except (TypeError, ValueError):
                volume = 0.0
        normalized.append(
            [
                float(time_value),
                float(open_),
                float(high),
                float(low),
                float(close),
                volume,
            ]
        )
    return normalized


@router.get("/daily", response_model=None)
def get_daily_bars(
    code: str,
    limit: int = 400,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, List[List[float]]]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_daily_bars(code, limit, asof_dt)
    try:
        provisional_row = get_provisional_daily_row_from_chart(code)
        rows = merge_daily_rows_with_provisional(rows, provisional_row, asof_dt=asof_dt)
    except Exception as exc:
        logger.debug("Yahoo provisional merge skipped for code=%s: %s", code, exc)
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": []}


@router.get("/monthly", response_model=None)
def get_monthly_bars(
    code: str,
    limit: int = 120,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, List[List[float]]]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_monthly_bars(code, limit, asof_dt)
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": []}


@router.get("/boxes", response_model=None)
def get_boxes(
    code: str,
    limit: int = 120,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> List[Dict]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_monthly_bars(code, limit, asof_dt)
    return detect_boxes(rows, range_basis="body", max_range_pct=0.2)


def _parse_dt(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        raw = str(value)
    else:
        raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit() and len(raw) == 8:
        parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    if raw.isdigit():
        value_int = int(raw)
        if value_int > 1_000_000_000_000:
            return int(value_int / 1000)
        return value_int
    return None


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _build_sell_context_from_row(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "pDown": _to_float_or_none(row[3]) if len(row) > 3 else None,
        "pTurnDown": _to_float_or_none(row[4]) if len(row) > 4 else None,
        "shortScore": _to_float_or_none(row[11]) if len(row) > 11 else None,
        "distMa20Signed": _to_float_or_none(row[18]) if len(row) > 18 else None,
        "ma20Slope": _to_float_or_none(row[16]) if len(row) > 16 else None,
        "ma60Slope": _to_float_or_none(row[17]) if len(row) > 17 else None,
        "trendDown": bool(row[20]) if len(row) > 20 and row[20] is not None else None,
        "trendDownStrict": bool(row[21]) if len(row) > 21 and row[21] is not None else None,
    }


def _build_research_prior_summary(code: str) -> Dict[str, Any] | None:
    code_key = str(code or "").strip()
    if not code_key:
        return None
    try:
        snapshot = rankings_cache._load_research_prior_snapshot()
    except Exception:
        return None
    if not isinstance(snapshot, dict):
        return None

    run_id = str(snapshot.get("run_id") or "").strip() or None
    if run_id is None:
        return None

    summary: Dict[str, Any] = {"runId": run_id}
    for side in ("up", "down"):
        probe: Dict[str, Any] = {}
        rankings_cache._calc_research_prior_bonus(
            item=probe,
            direction=side,  # type: ignore[arg-type]
            code=code_key,
            prior_snapshot=snapshot,
        )
        summary[side] = {
            "aligned": bool(probe.get("researchPriorAligned")),
            "rank": _to_int_or_none(probe.get("researchPriorRank")),
            "universe": _to_int_or_none(probe.get("researchPriorUniverse")),
            "bonus": _to_float_or_none(probe.get("researchPriorBonus")),
            "asOf": str(probe.get("researchPriorAsOf") or "").strip() or None,
        }
    return summary


def _normalize_risk_mode(value: str | None) -> str:
    resolved = str(value or "balanced").strip().lower()
    if resolved not in _VALID_RISK_MODES:
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return resolved


def _infer_playbook_setup_type(
    *,
    direction: str,
    shape_patterns: dict[str, bool],
    trend_up_strict: bool,
    trend_down_strict: bool,
    monthly_box_state: str | None,
) -> str:
    box_state = str(monthly_box_state or "")
    if direction == "up":
        if bool(shape_patterns.get("a3CapitulationRebound")):
            return "rebound"
        if bool(shape_patterns.get("a1MaturedBreakout")):
            return "breakout"
        if bool(shape_patterns.get("a2BoxTrend")):
            return "accumulation"
        if trend_up_strict and box_state in {"box_mid", "box_upper", "breakout_up"}:
            return "continuation"
        return "watch"

    if (
        bool(shape_patterns.get("d1ShortBreakdown"))
        or bool(shape_patterns.get("d2ShortMixedFar"))
        or bool(shape_patterns.get("d3ShortNaBelow"))
    ):
        return "breakdown"
    if trend_down_strict and box_state in {"below_box", "box_lower"}:
        return "continuation"
    return "watch"


def _build_playbook_policy_side(
    *,
    direction: str,
    risk_mode: str,
    trend_up_strict: bool,
    trend_down_strict: bool,
    monthly_box_state: str | None,
    monthly_box_months: float | None,
    dist_ma20_signed: float | None,
    cnt60_up: float | None,
    cnt100_up: float | None,
) -> Dict[str, Any]:
    shape_patterns = rankings_cache._calc_shape_pattern_flags(
        direction=direction,  # type: ignore[arg-type]
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
        monthly_box_months=monthly_box_months,
        dist_ma20_signed=dist_ma20_signed,
        cnt60_up=cnt60_up,
        cnt100_up=cnt100_up,
    )
    setup_type = _infer_playbook_setup_type(
        direction=direction,
        shape_patterns=shape_patterns,
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
    )
    side: Dict[str, Any] = {}
    rankings_cache._apply_entry_playbook_fields(
        side,
        direction=direction,  # type: ignore[arg-type]
        setup_type=setup_type,
        shape_patterns=shape_patterns,
        risk_mode=risk_mode,  # type: ignore[arg-type]
    )
    side["setupType"] = setup_type
    side["shapePatterns"] = shape_patterns
    side["playbookScoreBonus"] = float(
        rankings_cache._calc_playbook_entry_bonus(
            direction=direction,  # type: ignore[arg-type]
            shape_patterns=shape_patterns,
        )
    )
    return side


def _build_entry_policy_summary(
    *,
    daily_rows: list[tuple],
    monthly_rows: list[tuple],
    risk_mode: str,
) -> Dict[str, Any] | None:
    if not daily_rows:
        return None

    daily_closes: list[float] = []
    for row in daily_rows:
        if len(row) < 5 or row[4] is None:
            continue
        close_val = _to_float_or_none(row[4])
        if close_val is None:
            continue
        daily_closes.append(float(close_val))
    if not daily_closes:
        return None

    ma20 = _rolling_sma(daily_closes, 20)
    ma60 = _rolling_sma(daily_closes, 60)
    last_idx = len(daily_closes) - 1
    close_now = daily_closes[last_idx]
    ma20_now = ma20[last_idx] if last_idx >= 0 else None
    ma60_now = ma60[last_idx] if last_idx >= 0 else None
    ma20_prev = ma20[last_idx - 1] if last_idx - 1 >= 0 else None
    ma60_prev = ma60[last_idx - 1] if last_idx - 1 >= 0 else None

    trend_up = bool(
        ma20_now is not None
        and ma60_now is not None
        and close_now > ma20_now > ma60_now
    )
    trend_down = bool(
        ma20_now is not None
        and ma60_now is not None
        and close_now < ma20_now < ma60_now
    )
    ma20_slope = (
        float(ma20_now - ma20_prev)
        if ma20_now is not None and ma20_prev is not None and math.isfinite(ma20_now) and math.isfinite(ma20_prev)
        else None
    )
    ma60_slope = (
        float(ma60_now - ma60_prev)
        if ma60_now is not None and ma60_prev is not None and math.isfinite(ma60_now) and math.isfinite(ma60_prev)
        else None
    )
    dist_ma20_signed = (
        float((close_now - ma20_now) / ma20_now)
        if ma20_now is not None and ma20_now != 0 and math.isfinite(ma20_now)
        else None
    )
    trend_up_strict = bool(
        trend_up
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) > 0
        and float(ma60_slope) > 0
        and isinstance(dist_ma20_signed, (int, float))
        and float(dist_ma20_signed) >= 0.005
    )
    trend_down_strict = bool(
        trend_down
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) < 0
        and float(ma60_slope) < 0
        and isinstance(dist_ma20_signed, (int, float))
        and float(dist_ma20_signed) <= -0.005
    )

    v60_signals = rankings_cache._calc_60v_signals(daily_rows)
    cnt60_up = _to_float_or_none(v60_signals.get("cnt60Up"))
    cnt100_up = _to_float_or_none(v60_signals.get("cnt100Up"))

    monthly_box = rankings_cache._detect_monthly_body_box(monthly_rows)
    monthly_box_state, _ = rankings_cache._calc_monthly_box_state(
        entry_close=close_now,
        box=monthly_box,
    )
    monthly_box_months = (
        _to_float_or_none(monthly_box.get("months"))
        if isinstance(monthly_box, dict)
        else None
    )

    up_side = _build_playbook_policy_side(
        direction="up",
        risk_mode=risk_mode,
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
        monthly_box_months=monthly_box_months,
        dist_ma20_signed=dist_ma20_signed,
        cnt60_up=cnt60_up,
        cnt100_up=cnt100_up,
    )
    down_side = _build_playbook_policy_side(
        direction="down",
        risk_mode=risk_mode,
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
        monthly_box_months=monthly_box_months,
        dist_ma20_signed=dist_ma20_signed,
        cnt60_up=cnt60_up,
        cnt100_up=cnt100_up,
    )
    return {
        "riskMode": risk_mode,
        "up": up_side,
        "down": down_side,
    }


def _resolve_effective_trade_dt(
    repo: StockRepository,
    code: str,
    asof_dt: int | None,
) -> int | None:
    if asof_dt is None:
        return None
    try:
        rows = repo.get_daily_bars(code, limit=1, asof_dt=asof_dt)
    except Exception:
        rows = []
    if not rows:
        return asof_dt
    raw_dt = rows[-1][0] if rows[-1] else asof_dt
    if isinstance(raw_dt, float) and math.isfinite(raw_dt):
        raw_dt = int(raw_dt)
    normalized = _parse_dt(raw_dt)
    return normalized if normalized is not None else asof_dt


def _latest_trade_dt(repo: StockRepository, code: str) -> int | None:
    try:
        rows = repo.get_daily_bars(code, limit=1, asof_dt=None)
    except Exception:
        rows = []
    if not rows:
        return None
    raw_dt = rows[-1][0] if rows[-1] else None
    normalized = _parse_dt(raw_dt)
    return normalized


def _maybe_backfill_for_analysis(
    *,
    repo: StockRepository,
    code: str,
    asof_dt: int | None,
    ensure_ml: bool,
    ensure_sell: bool,
) -> None:
    effective_dt = _resolve_effective_trade_dt(repo, code, asof_dt)
    if effective_dt is None:
        return
    latest_dt = _latest_trade_dt(repo, code)
    if latest_dt is not None:
        try:
            latest_date = datetime.fromtimestamp(int(latest_dt), tz=timezone.utc).date()
            target_date = datetime.fromtimestamp(int(effective_dt), tz=timezone.utc).date()
            if (latest_date - target_date).days > SYNC_BACKFILL_MAX_AGE_DAYS:
                return
        except Exception:
            return
    attempt_key = (str(code), int(effective_dt), bool(ensure_ml), bool(ensure_sell))
    with _BACKFILL_ATTEMPTS_LOCK:
        if attempt_key in _BACKFILL_ATTEMPTS:
            return
        _BACKFILL_ATTEMPTS.add(attempt_key)
    if ensure_ml:
        try:
            from app.backend.services import ml_service
            ml_service.predict_for_dt(dt=int(effective_dt))
        except Exception as exc:
            logger.warning("ml backfill skipped code=%s dt=%s reason=%s", code, effective_dt, exc)
    if ensure_sell:
        try:
            from app.backend.services.sell_analysis_accumulator import accumulate_sell_analysis
            accumulate_sell_analysis(lookback_days=1, anchor_dt=int(effective_dt))
        except Exception as exc:
            logger.warning("sell backfill skipped code=%s dt=%s reason=%s", code, effective_dt, exc)


def _clip_probability(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return min(1.0, max(0.0, float(value)))


def _scale_probability_by_horizon(
    base_prob: float | None,
    source_horizon: int,
    target_horizon: int,
) -> float | None:
    clipped = _clip_probability(base_prob)
    if clipped is None:
        return None
    if source_horizon <= 0 or target_horizon <= 0:
        return clipped
    eps = 1.0e-6
    p = min(1.0 - eps, max(eps, clipped))
    ratio = float(target_horizon) / float(source_horizon)
    if ratio <= 0:
        return clipped
    scale = math.sqrt(ratio)
    logit = math.log(p / (1.0 - p))
    scaled = logit * scale
    prob = 1.0 / (1.0 + math.exp(-scaled))
    return _clip_probability(prob)


def _scale_ev_by_horizon(
    base_ev: float | None,
    source_horizon: int,
    target_horizon: int,
) -> float | None:
    if base_ev is None or not math.isfinite(base_ev):
        return None
    if source_horizon <= 0 or target_horizon <= 0:
        return float(base_ev)
    return float(base_ev) * (float(target_horizon) / float(source_horizon))


def _build_horizon_analysis(
    p_up_20d: float | None,
    ev_net_20d: float | None,
    p_turn_down_10d: float | None,
    *,
    p_up_5d: float | None = None,
    p_up_10d: float | None = None,
    ev_net_5d: float | None = None,
    ev_net_10d: float | None = None,
    p_turn_down_5d: float | None = None,
    p_turn_down_20d: float | None = None,
) -> Dict[str, Any]:
    horizon_values: Dict[str, Dict[str, Any]] = {}
    base_turn = p_turn_down_10d
    if base_turn is None:
        base_turn = 1.0 - p_up_20d if p_up_20d is not None else None
    for horizon in (5, 10, 20):
        direct_p_up = (
            _clip_probability(p_up_5d)
            if horizon == 5
            else _clip_probability(p_up_10d)
            if horizon == 10
            else _clip_probability(p_up_20d)
        )
        if direct_p_up is not None:
            p_up = direct_p_up
            p_up_projected = False
        elif horizon == 20:
            p_up = _clip_probability(p_up_20d)
            p_up_projected = False
        else:
            p_up = _scale_probability_by_horizon(p_up_20d, source_horizon=20, target_horizon=horizon)
            p_up_projected = True
        p_down = (1.0 - p_up) if p_up is not None else None
        direct_ev = (
            _to_float_or_none(ev_net_5d)
            if horizon == 5
            else _to_float_or_none(ev_net_10d)
            if horizon == 10
            else _to_float_or_none(ev_net_20d)
        )
        if direct_ev is not None:
            ev_net = direct_ev
            ev_projected = False
        else:
            ev_net = _scale_ev_by_horizon(ev_net_20d, source_horizon=20, target_horizon=horizon)
            ev_projected = horizon != 20
        direct_turn = (
            _clip_probability(p_turn_down_5d)
            if horizon == 5
            else _clip_probability(p_turn_down_10d)
            if horizon == 10
            else _clip_probability(p_turn_down_20d)
        )
        if direct_turn is not None:
            p_turn_down = direct_turn
            turn_projected = False
        elif horizon == 10:
            p_turn_down = _clip_probability(1.0 - p_up) if p_up is not None else None
            turn_projected = True
        else:
            p_turn_down = _scale_probability_by_horizon(
                base_turn,
                source_horizon=10,
                target_horizon=horizon,
            )
            turn_projected = True
        horizon_values[str(horizon)] = {
            "horizon": horizon,
            "pUp": p_up,
            "pDown": p_down,
            "evNet": ev_net,
            "pTurnDown": p_turn_down,
            "pTurnUp": (1.0 - p_turn_down) if p_turn_down is not None else None,
            "pUpProjected": p_up_projected,
            "evProjected": ev_projected,
            "turnProjected": turn_projected,
        }
    return {
        "defaultHorizon": 20,
        "turnBaseHorizon": 10,
        "projectionMethod": "logit_sqrt_time",
        "items": horizon_values,
    }


def _rolling_sma(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    out: list[float | None] = [None for _ in values]
    running = 0.0
    for idx, value in enumerate(values):
        running += float(value)
        if idx >= period:
            running -= float(values[idx - period])
        if idx >= period - 1:
            out[idx] = float(running / period)
    return out


def _build_additive_signal_summary(
    daily_rows: list[tuple],
    monthly_rows: list[tuple],
) -> Dict[str, Any] | None:
    if not daily_rows:
        return None

    daily_closes: list[float] = []
    for row in daily_rows:
        if len(row) < 5 or row[4] is None:
            continue
        try:
            daily_closes.append(float(row[4]))
        except (TypeError, ValueError):
            continue
    if not daily_closes:
        return None

    ma20 = _rolling_sma(daily_closes, 20)
    ma60 = _rolling_sma(daily_closes, 60)
    last_idx = len(daily_closes) - 1
    close_now = daily_closes[last_idx]
    ma20_now = ma20[last_idx] if last_idx >= 0 else None
    ma60_now = ma60[last_idx] if last_idx >= 0 else None
    ma20_prev = ma20[last_idx - 1] if last_idx - 1 >= 0 else None
    ma60_prev = ma60[last_idx - 1] if last_idx - 1 >= 0 else None
    trend_up = bool(
        ma20_now is not None
        and ma60_now is not None
        and close_now > ma20_now > ma60_now
    )
    ma20_slope = (
        float(ma20_now - ma20_prev)
        if ma20_now is not None and ma20_prev is not None and math.isfinite(ma20_now) and math.isfinite(ma20_prev)
        else None
    )
    ma60_slope = (
        float(ma60_now - ma60_prev)
        if ma60_now is not None and ma60_prev is not None and math.isfinite(ma60_now) and math.isfinite(ma60_prev)
        else None
    )
    dist_ma20_signed = (
        float((close_now - ma20_now) / ma20_now)
        if ma20_now is not None and ma20_now != 0 and math.isfinite(ma20_now)
        else None
    )
    trend_up_strict = bool(
        trend_up
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) > 0
        and float(ma60_slope) > 0
        and isinstance(dist_ma20_signed, (int, float))
        and float(dist_ma20_signed) >= 0.005
    )

    weekly = rankings_cache._build_weekly_bars(daily_rows)
    last_daily_dt = rankings_cache._parse_date_value(daily_rows[-1][0]) if daily_rows else None
    weekly = rankings_cache._drop_incomplete_weekly(weekly, last_daily_dt)
    weekly_closes = [float(item["c"]) for item in weekly if isinstance(item.get("c"), (int, float))]
    monthly_closes = [
        float(row[4])
        for row in monthly_rows
        if len(row) >= 5 and isinstance(row[4], (int, float))
    ]
    weekly_regime = rankings_cache._calc_regime_probs(weekly_closes, lookback=20)
    monthly_regime = rankings_cache._calc_regime_probs(monthly_closes, lookback=12)
    weekly_breakout_up_prob = _to_float_or_none(weekly_regime.get("breakoutUpProb"))
    monthly_breakout_up_prob = _to_float_or_none(monthly_regime.get("breakoutUpProb"))
    monthly_range_prob = _to_float_or_none(monthly_regime.get("rangeProb"))
    monthly_range_pos = _to_float_or_none(monthly_regime.get("rangePos"))

    candle_signals = rankings_cache._calc_triplet_candle_signals(daily_rows)
    shooting_star_like = bool((_to_float_or_none(candle_signals.get("shootingStarLike")) or 0.0) >= 0.5)
    bear_marubozu = bool((_to_float_or_none(candle_signals.get("bearMarubozu")) or 0.0) >= 0.5)
    three_white_soldiers = bool((_to_float_or_none(candle_signals.get("threeWhiteSoldiers")) or 0.0) >= 0.5)
    three_black_crows = bool((_to_float_or_none(candle_signals.get("threeBlackCrows")) or 0.0) >= 0.5)
    morning_star = bool((_to_float_or_none(candle_signals.get("morningStar")) or 0.0) >= 0.5)
    bull_engulfing = bool((_to_float_or_none(candle_signals.get("bullEngulfing")) or 0.0) >= 0.5)

    v60_signals = rankings_cache._calc_60v_signals(daily_rows)
    reclaim60 = bool((_to_float_or_none(v60_signals.get("reclaim60")) or 0.0) >= 0.5)
    v60_core = bool((_to_float_or_none(v60_signals.get("v60Core")) or 0.0) >= 0.5)
    v60_strong = bool((_to_float_or_none(v60_signals.get("v60Strong")) or 0.0) >= 0.5)

    mtf_strong_aligned = bool(
        trend_up_strict
        and weekly_breakout_up_prob is not None
        and weekly_breakout_up_prob >= 0.56
        and monthly_breakout_up_prob is not None
        and monthly_breakout_up_prob >= 0.60
    )
    box_bottom_aligned = bool(
        monthly_range_prob is not None
        and monthly_range_pos is not None
        and monthly_range_prob >= 0.62
        and monthly_range_pos <= 0.38
    )

    candlestick_pattern_bonus, candlestick_pattern_bonus_details = rankings_cache._calc_candlestick_pattern_bonus(
        candle_signals,
        direction="up",
    )
    v60_strong_penalty = bool(v60_strong)
    bonus_estimate = (
        (0.02 if trend_up_strict else 0.0)
        + (0.02 if mtf_strong_aligned else 0.0)
        + (0.03 if box_bottom_aligned else 0.0)
        + candlestick_pattern_bonus
        - (0.01 if v60_strong_penalty else 0.0)
    )

    return {
        "trendUpStrict": trend_up_strict,
        "mtfStrongAligned": mtf_strong_aligned,
        "boxBottomAligned": box_bottom_aligned,
        "shootingStarLike": shooting_star_like,
        "bearMarubozu": bear_marubozu,
        "threeWhiteSoldiers": three_white_soldiers,
        "threeBlackCrows": three_black_crows,
        "morningStar": morning_star,
        "bullEngulfing": bull_engulfing,
        "reclaim60": reclaim60,
        "v60Core": v60_core,
        "v60Strong": v60_strong,
        "v60StrongPenalty": v60_strong_penalty,
        "candlestickPatternBonus": candlestick_pattern_bonus,
        "candlestickPatternBonusDetails": candlestick_pattern_bonus_details,
        "bonusEstimate": bonus_estimate,
        "weeklyBreakoutUpProb": weekly_breakout_up_prob,
        "monthlyBreakoutUpProb": monthly_breakout_up_prob,
        "monthlyRangeProb": monthly_range_prob,
        "monthlyRangePos": monthly_range_pos,
    }


@router.get("/phase", response_model=None)
def get_phase_pred(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    row = repo.get_phase_pred(code, asof_dt)
    if not row:
        return {"item": None}
    return {
        "item": {
            "dt": row[0],
            "earlyScore": row[1],
            "lateScore": row[2],
            "bodyScore": row[3],
            "n": row[4],
            "reasonsTop3": row[5],
        }
    }


@router.get("/analysis", response_model=None)
def get_analysis_pred(
    code: str,
    asof: str | int | None = None,
    risk_mode: str = Query("balanced"),
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    resolved_risk_mode = _normalize_risk_mode(risk_mode)
    asof_dt = _parse_dt(asof)
    row = repo.get_ml_analysis_pred(code, asof_dt)
    if not row and asof_dt is not None:
        _maybe_backfill_for_analysis(
            repo=repo,
            code=code,
            asof_dt=asof_dt,
            ensure_ml=True,
            ensure_sell=False,
        )
        row = repo.get_ml_analysis_pred(code, asof_dt)
    if not row:
        return {"item": None}
    p_up = _to_float_or_none(row[1])
    p_down = _to_float_or_none(row[2]) if len(row) > 2 else None
    if p_down is None and p_up is not None:
        p_down = 1.0 - p_up
    p_up_5 = _to_float_or_none(row[3]) if len(row) > 3 else None
    p_up_10 = _to_float_or_none(row[4]) if len(row) > 4 else None
    p_turn_up = _to_float_or_none(row[5]) if len(row) > 5 else None
    p_turn_down = _to_float_or_none(row[6]) if len(row) > 6 else None
    p_turn_down_5 = _to_float_or_none(row[7]) if len(row) > 7 else None
    p_turn_down_10 = _to_float_or_none(row[8]) if len(row) > 8 else None
    p_turn_down_20 = _to_float_or_none(row[9]) if len(row) > 9 else None
    ret_pred20 = _to_float_or_none(row[12]) if len(row) > 12 else None
    ev20 = _to_float_or_none(row[13]) if len(row) > 13 else None
    ev20_net_raw = _to_float_or_none(row[14]) if len(row) > 14 else None
    ev5_net = _to_float_or_none(row[15]) if len(row) > 15 else None
    ev10_net = _to_float_or_none(row[16]) if len(row) > 16 else None
    ev20_net = ev20_net_raw if ev20_net_raw is not None else (ev20 - 0.002 if ev20 is not None else None)
    horizon_analysis = _build_horizon_analysis(
        p_up,
        ev20_net,
        p_turn_down_10 if p_turn_down_10 is not None else p_turn_down,
        p_up_5d=p_up_5,
        p_up_10d=p_up_10,
        ev_net_5d=ev5_net,
        ev_net_10d=ev10_net,
        p_turn_down_5d=p_turn_down_5,
        p_turn_down_20d=p_turn_down_20,
    )
    model_version = row[17] if len(row) > 17 else None
    additive_signals = None
    buy_stage_precision = None
    entry_policy = None
    try:
        daily_rows = repo.get_daily_bars(code, limit=1260, asof_dt=asof_dt)
        monthly_rows = repo.get_monthly_bars(code, limit=60, asof_dt=asof_dt)
        additive_signals = _build_additive_signal_summary(daily_rows, monthly_rows)
        entry_policy = _build_entry_policy_summary(
            daily_rows=daily_rows,
            monthly_rows=monthly_rows,
            risk_mode=resolved_risk_mode,
        )
    except Exception:
        additive_signals = None
        entry_policy = None
    try:
        buy_stage_precision = repo.get_buy_stage_precision(code, asof_dt, lookback_bars=360, horizon=20)
    except Exception:
        buy_stage_precision = None
    research_prior = _build_research_prior_summary(code)
    sell_context = None
    try:
        sell_context = _build_sell_context_from_row(repo.get_sell_analysis_snapshot(code, asof_dt))
    except Exception:
        sell_context = None
    decision = build_analysis_decision(
        analysis_p_up=p_up,
        analysis_p_down=p_down,
        analysis_p_turn_up=p_turn_up,
        analysis_p_turn_down=p_turn_down,
        analysis_ev_net=ev20_net,
        playbook_up_score_bonus=_to_float_or_none((entry_policy or {}).get("up", {}).get("playbookScoreBonus"))
        if isinstance(entry_policy, dict)
        else None,
        playbook_down_score_bonus=_to_float_or_none((entry_policy or {}).get("down", {}).get("playbookScoreBonus"))
        if isinstance(entry_policy, dict)
        else None,
        additive_signals=additive_signals if isinstance(additive_signals, dict) else None,
        sell_analysis=sell_context if isinstance(sell_context, dict) else None,
    )
    return {
        "item": {
            "dt": row[0],
            "pUp": p_up,
            "pDown": p_down,
            "pTurnUp": p_turn_up,
            "pTurnDown": p_turn_down,
            "pTurnDownHorizon": 10,
            "retPred20": ret_pred20,
            "ev20": ev20,
            "ev20Net": ev20_net,
            "horizonAnalysis": horizon_analysis,
            "additiveSignals": additive_signals,
            "entryPolicy": entry_policy,
            "riskMode": resolved_risk_mode,
            "buyStagePrecision": buy_stage_precision,
            "researchPrior": research_prior,
            "modelVersion": str(model_version) if model_version is not None else None,
            "decision": decision,
        }
    }


@router.get("/analysis/timeline", response_model=None)
def get_analysis_timeline(
    code: str,
    limit: int = Query(400, ge=1, le=2000),
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    items = repo.get_analysis_timeline(code, asof_dt, limit=limit)
    if not items and asof_dt is not None:
        _maybe_backfill_for_analysis(
            repo=repo,
            code=code,
            asof_dt=asof_dt,
            ensure_ml=True,
            ensure_sell=True,
        )
        items = repo.get_analysis_timeline(code, asof_dt, limit=limit)
        
    if items:
        try:
            # Compute ranking score once for the latest date only (O(1) instead of O(N))
            daily_rows = repo.get_daily_bars(code, limit=500, asof_dt=asof_dt)
            if daily_rows:
                daily_rows_asc = list(reversed(daily_rows))
                config = {
                    "common": {"min_daily_bars": 80},
                    "weekly": {
                        "weights": {"ma_alignment": 10},
                        "thresholds": {"volume_ratio": 1.5}
                    }
                }
                up, _, _ = ranking.score_weekly_candidate(code, "", daily_rows_asc, config, None)
                if up:
                    latest_score = up.get("total_score")
                    if latest_score is not None:
                        for item in items:
                            item["rankingScore"] = latest_score
        except Exception as exc:
            logger.warning("timeline ranking score attach failed code=%s reason=%s", code, exc)

    return {"items": items}


@router.get("/analysis/sell", response_model=None)
def get_sell_analysis_snapshot(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    row = repo.get_sell_analysis_snapshot(code, asof_dt)
    if not row and asof_dt is not None:
        _maybe_backfill_for_analysis(
            repo=repo,
            code=code,
            asof_dt=asof_dt,
            ensure_ml=True,
            ensure_sell=True,
        )
        row = repo.get_sell_analysis_snapshot(code, asof_dt)
    if not row:
        return {"item": None}
    return {
        "item": {
            "dt": row[0],
            "close": _to_float_or_none(row[1]),
            "dayChangePct": _to_float_or_none(row[2]),
            "pDown": _to_float_or_none(row[3]),
            "pTurnDown": _to_float_or_none(row[4]),
            "ev20Net": _to_float_or_none(row[5]),
            "rankDown20": _to_float_or_none(row[6]),
            "predDt": row[7],
            "pUp5": _to_float_or_none(row[8]),
            "pUp10": _to_float_or_none(row[9]),
            "pUp20": _to_float_or_none(row[10]),
            "shortScore": _to_float_or_none(row[11]),
            "aScore": _to_float_or_none(row[12]),
            "bScore": _to_float_or_none(row[13]),
            "ma20": _to_float_or_none(row[14]),
            "ma60": _to_float_or_none(row[15]),
            "ma20Slope": _to_float_or_none(row[16]),
            "ma60Slope": _to_float_or_none(row[17]),
            "distMa20Signed": _to_float_or_none(row[18]),
            "distMa60Signed": _to_float_or_none(row[19]),
            "trendDown": bool(row[20]) if row[20] is not None else None,
            "trendDownStrict": bool(row[21]) if row[21] is not None else None,
            "fwdClose5": _to_float_or_none(row[22]),
            "fwdClose10": _to_float_or_none(row[23]),
            "fwdClose20": _to_float_or_none(row[24]),
            "shortRet5": _to_float_or_none(row[25]),
            "shortRet10": _to_float_or_none(row[26]),
            "shortRet20": _to_float_or_none(row[27]),
            "shortWin5": bool(row[28]) if row[28] is not None else None,
            "shortWin10": bool(row[29]) if row[29] is not None else None,
            "shortWin20": bool(row[30]) if row[30] is not None else None,
        }
    }
