import logging
import os
import time
import copy
from threading import Lock
from fastapi import APIRouter, Depends
from typing import List, Any, Dict
from datetime import datetime, timedelta

from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.api.dependencies import get_screener_repo, get_stock_repo
from app.backend.domain.screening import metrics, ranking
from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.services.yahoo_provisional import (
    get_provisional_daily_rows_from_spark,
    merge_daily_rows_with_provisional,
)
from app.backend.services.watchlist import load_watchlist_codes, resolve_watchlist_path, watchlist_lock
from app.core.config import config as core_config
from app.utils.date_utils import jst_now

router = APIRouter(prefix="/api/grid", tags=["grid"])
logger = logging.getLogger(__name__)
_AUTO_REPAIR_MIN_MISSING = max(1, int(os.getenv("MEEMEE_AUTO_REPAIR_MIN_MISSING", "30")))
_AUTO_REPAIR_MIN_RATIO = max(0.0, float(os.getenv("MEEMEE_AUTO_REPAIR_MIN_RATIO", "0.2")))
_AUTO_REPAIR_COOLDOWN_SEC = max(60, int(os.getenv("MEEMEE_AUTO_REPAIR_COOLDOWN_SEC", "900")))
_ENTRY_MIN_LIQUIDITY = max(0.0, float(os.getenv("MEEMEE_ENTRY_MIN_LIQUIDITY", "50000000")))
_ENTRY_TIER_A_THRESHOLD = float(os.getenv("MEEMEE_ENTRY_TIER_A", "37"))
_ENTRY_TIER_B_THRESHOLD = float(os.getenv("MEEMEE_ENTRY_TIER_B", "33"))
_SHORT_ML_MIN_PDOWN = max(0.0, min(1.0, float(os.getenv("MEEMEE_SHORT_ML_MIN_PDOWN", "0.52"))))
_SHORT_ML_MIN_PTURN = max(0.0, min(1.0, float(os.getenv("MEEMEE_SHORT_ML_MIN_PTURN", "0.52"))))
_SHORT_ML_STRICT_PDOWN = max(0.0, min(1.0, float(os.getenv("MEEMEE_SHORT_ML_STRICT_PDOWN", "0.62"))))
_SHORT_ML_STRICT_PTURN = max(0.0, min(1.0, float(os.getenv("MEEMEE_SHORT_ML_STRICT_PTURN", "0.62"))))
_SHORT_EXT_MA20_MIN = float(os.getenv("MEEMEE_SHORT_EXT_MA20_MIN", "0.03"))
_SHORT_CNT60_SOFT_MIN = max(0, int(os.getenv("MEEMEE_SHORT_CNT60_SOFT_MIN", "40")))
_SHORT_CNT60_MEDIUM_MIN = max(0, int(os.getenv("MEEMEE_SHORT_CNT60_MEDIUM_MIN", "50")))
_SHORT_CNT60_STRICT_MIN = max(0, int(os.getenv("MEEMEE_SHORT_CNT60_STRICT_MIN", "60")))
_SHORT_TIER_A_THRESHOLD = float(os.getenv("MEEMEE_SHORT_TIER_A", "66"))
_SHORT_TIER_A_STRICT_FLOOR = float(os.getenv("MEEMEE_SHORT_TIER_A_STRICT_FLOOR", "57"))
_SHORT_TIER_B_THRESHOLD = float(os.getenv("MEEMEE_SHORT_TIER_B", "54"))
_auto_repair_lock = Lock()
_last_auto_repair_ts = 0.0

def _group_rows_by_code(rows: list[tuple]) -> dict[str, list[tuple]]:
    grouped: dict[str, list[tuple]] = {}
    for row in rows:
        if not row:
            continue
        code = row[0]
        grouped.setdefault(code, []).append(row)
    return grouped


def _apply_short_scores(items: list[dict[str, Any]], score_map: dict[str, dict[str, Any]]) -> None:
    for item in items:
        code = item.get("code")
        if not isinstance(code, str):
            continue
        short_info = score_map.get(code) or {}
        short_a = short_info.get("score_a")
        short_b = short_info.get("score_b")
        short_reasons = short_info.get("reasons") if isinstance(short_info.get("reasons"), list) else []
        short_badges = short_info.get("badges") if isinstance(short_info.get("badges"), list) else []
        short_total = None
        if isinstance(short_a, (int, float)) or isinstance(short_b, (int, float)):
            short_total = float(short_a or 0.0) + float(short_b or 0.0)

        item["shortScore"] = short_total
        item["aScore"] = float(short_a) if isinstance(short_a, (int, float)) else None
        item["bScore"] = float(short_b) if isinstance(short_b, (int, float)) else None
        item["shortBadges"] = short_badges
        item["shortReasons"] = short_reasons


def _apply_ml_metrics(items: list[dict[str, Any]], ml_map: dict[str, dict[str, Any]]) -> None:
    for item in items:
        code = item.get("code")
        if not isinstance(code, str):
            continue
        ml = ml_map.get(code) or {}
        item["mlPUp"] = ml.get("p_up")
        item["mlPUp5"] = ml.get("p_up_5")
        item["mlPUp10"] = ml.get("p_up_10")
        item["mlPUpShort"] = ml.get("p_up_short")
        item["mlPDown"] = ml.get("p_down")
        item["mlPDownShort"] = ml.get("p_down_short")
        item["mlPTurnDown"] = ml.get("p_turn_down")
        item["mlPTurnDown5"] = ml.get("p_turn_down_5")
        item["mlPTurnDown10"] = ml.get("p_turn_down_10")
        item["mlPTurnDown20"] = ml.get("p_turn_down_20")
        item["mlPTurnDownShort"] = ml.get("p_turn_down_short")
        item["mlEv20Net"] = ml.get("ev20_net")
        item["mlEv5Net"] = ml.get("ev5_net")
        item["mlEv10Net"] = ml.get("ev10_net")
        item["mlEvShortNet"] = ml.get("ev_short_net")
        item["mlModelVersion"] = ml.get("model_version")


def _to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        value = float(value)
        return value if value == value else None
    return None


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _first_finite(*values: Any) -> float | None:
    for value in values:
        finite = _to_float(value)
        if finite is not None:
            return finite
    return None


def _apply_short_priority_metrics(items: list[dict[str, Any]]) -> None:
    cnt60_scale = max(1, int(_SHORT_CNT60_STRICT_MIN))
    for item in items:
        short_score = _first_finite(item.get("shortCandidateScore"), item.get("shortScore"))
        if short_score is None:
            short_score = (_to_float(item.get("aScore")) or 0.0) + (_to_float(item.get("bScore")) or 0.0)
        ml_pdown = _first_finite(item.get("mlPDownShort"), item.get("mlPDown"))
        ml_pturn = _first_finite(item.get("mlPTurnDownShort"), item.get("mlPTurnDown"), ml_pdown)
        short_eligible = item.get("shortEligible")
        sell_risk_atr = _to_float(item.get("sellRiskAtr"))
        last_close = _to_float(item.get("lastClose"))
        ma20 = _to_float(item.get("ma20"))
        counts = item.get("counts") if isinstance(item.get("counts"), dict) else {}
        cnt60_above = _first_finite(
            counts.get("up60"),
            counts.get("cnt60"),
            counts.get("cnt_60_above"),
            item.get("cnt60"),
            item.get("cnt_60_above"),
            item.get("up60"),
        )
        ext_ma20 = (last_close / ma20 - 1.0) if (last_close is not None and ma20 is not None and ma20 != 0) else None

        hard_exclude_reasons: list[str] = []
        if (
            ml_pdown is not None
            and ml_pturn is not None
            and ml_pdown < _SHORT_ML_MIN_PDOWN
            and ml_pturn < _SHORT_ML_MIN_PTURN
        ):
            hard_exclude_reasons.append("ml_weak")
        if sell_risk_atr is not None and sell_risk_atr >= 3.0:
            hard_exclude_reasons.append("risk_atr>=3.0")
        hard_excluded = len(hard_exclude_reasons) > 0

        short_component = _clamp(short_score, 0.0, 100.0)
        down_component = _clamp((ml_pdown * 100.0) if ml_pdown is not None else 50.0, 0.0, 100.0)
        turn_component = _clamp((ml_pturn * 100.0) if ml_pturn is not None else down_component, 0.0, 100.0)
        cnt60_component = (
            _clamp((min(cnt60_above, float(cnt60_scale)) / float(cnt60_scale)) * 100.0, 0.0, 100.0)
            if cnt60_above is not None
            else 50.0
        )
        risk_component = (
            _clamp(((2.5 - sell_risk_atr) / 2.0) * 100.0, 0.0, 100.0)
            if sell_risk_atr is not None
            else 50.0
        )

        short_priority_score = (
            0.33 * short_component
            + 0.23 * down_component
            + 0.23 * turn_component
            + 0.13 * cnt60_component
            + 0.08 * risk_component
        )

        cnt60_strict_ok = cnt60_above is None or cnt60_above >= float(_SHORT_CNT60_STRICT_MIN)
        cnt60_medium_ok = cnt60_above is None or cnt60_above >= float(_SHORT_CNT60_MEDIUM_MIN)
        strict_consensus = (
            ml_pdown is not None
            and ml_pturn is not None
            and ml_pdown >= _SHORT_ML_STRICT_PDOWN
            and ml_pturn >= _SHORT_ML_STRICT_PTURN
            and cnt60_strict_ok
        )
        medium_consensus = (
            ml_pdown is not None
            and ml_pturn is not None
            and ml_pdown >= 0.58
            and ml_pturn >= 0.58
            and cnt60_medium_ok
        )

        if strict_consensus:
            short_priority_score += 5.0
        elif medium_consensus:
            short_priority_score += 2.0
        if ext_ma20 is not None and ext_ma20 >= _SHORT_EXT_MA20_MIN:
            short_priority_score += 2.0
        if cnt60_above is not None and cnt60_above < float(_SHORT_CNT60_SOFT_MIN):
            short_priority_score *= 0.88
        if short_eligible is False:
            short_priority_score *= 0.86
        if hard_excluded:
            short_priority_score = min(short_priority_score, 39.9)
        short_priority_score = _clamp(short_priority_score, 0.0, 100.0)

        tier_a_threshold = max(_SHORT_TIER_A_THRESHOLD, _SHORT_TIER_B_THRESHOLD)
        tier_b_threshold = min(_SHORT_TIER_A_THRESHOLD, _SHORT_TIER_B_THRESHOLD)
        strict_tier_a_threshold = min(tier_a_threshold, float(_SHORT_TIER_A_STRICT_FLOOR))
        if not hard_excluded and strict_consensus and short_priority_score >= strict_tier_a_threshold:
            tier = "A"
            tier_label = "A: high precision short"
        elif not hard_excluded and medium_consensus and short_priority_score >= tier_b_threshold:
            tier = "B"
            tier_label = "B: monitored short"
        else:
            tier = "C"
            tier_label = "C: observe"

        reason_candidates: list[tuple[float, str]] = []
        reason_candidates.append((0.33 * short_component, f"short_score {short_component:.0f}"))
        if ml_pdown is not None:
            reason_candidates.append((0.23 * down_component, f"p_down {ml_pdown * 100.0:.1f}%"))
        if ml_pturn is not None:
            reason_candidates.append((0.23 * turn_component, f"p_turn_down {ml_pturn * 100.0:.1f}%"))
        if cnt60_above is not None:
            reason_candidates.append((0.13 * cnt60_component, f"cnt60 {cnt60_above:.0f}"))
        if sell_risk_atr is not None:
            reason_candidates.append((0.08 * risk_component, f"risk {sell_risk_atr:.2f}ATR"))
        if ext_ma20 is not None and ext_ma20 >= _SHORT_EXT_MA20_MIN:
            reason_candidates.append((3.0, f"ext_ma20 {ext_ma20 * 100.0:+.1f}%"))
        if strict_consensus:
            reason_candidates.append((5.0, "consensus: ml+cnt60"))

        reason_candidates.sort(key=lambda pair: pair[0], reverse=True)
        short_reasons = [text for _, text in reason_candidates[:3]]
        if hard_excluded:
            short_reasons = hard_exclude_reasons[:3]
        elif len(short_reasons) < 2:
            short_reasons.append(f"total {short_priority_score:.1f}")
        if not hard_excluded and short_eligible is False and len(short_reasons) < 3:
            short_reasons.append("pre-entry")

        item["shortPriorityScore"] = round(short_priority_score, 2)
        item["shortPriorityTier"] = tier
        item["shortPriorityLabel"] = tier_label
        item["shortPriorityReasons"] = short_reasons
        item["shortHardExcluded"] = hard_excluded
        item["shortHardExcludeReasons"] = hard_exclude_reasons
        item["shortMlConsensus"] = strict_consensus
        item["shortCnt60Above"] = round(cnt60_above, 2) if cnt60_above is not None else None
        item["shortPriorityComponents"] = {
            "short": round(short_component, 2),
            "down": round(down_component, 2),
            "turn": round(turn_component, 2),
            "cnt60": round(cnt60_component, 2),
            "risk": round(risk_component, 2),
        }


def _apply_entry_priority_metrics(items: list[dict[str, Any]]) -> None:
    for item in items:
        buy_score = _to_float(item.get("buyCandidateScore"))
        ml_prob = _first_finite(item.get("mlPUpShort"), item.get("mlPUp"))
        ev_short_net = _first_finite(item.get("mlEvShortNet"), item.get("mlEv20Net"))
        buy_risk_atr = _to_float(item.get("buyRiskAtr"))
        liquidity_20d = _to_float(item.get("liquidity20d"))
        buy_eligible = item.get("buyEligible")
        buy_overextended = bool(item.get("buyOverextended"))

        hard_exclude_reasons: list[str] = []
        if buy_overextended:
            hard_exclude_reasons.append("上昇伸び切り")
        if liquidity_20d is not None and liquidity_20d < _ENTRY_MIN_LIQUIDITY:
            hard_exclude_reasons.append("流動性不足")
        hard_excluded = len(hard_exclude_reasons) > 0

        buy_component = _clamp(buy_score if buy_score is not None else 0.0, 0.0, 100.0)
        prob_component = _clamp((ml_prob * 100.0) if ml_prob is not None else 50.0, 0.0, 100.0)
        ev_component = (
            _clamp(((ev_short_net + 0.02) / 0.12) * 100.0, 0.0, 100.0)
            if ev_short_net is not None
            else 50.0
        )
        risk_component = (
            _clamp(((2.5 - buy_risk_atr) / 2.0) * 100.0, 0.0, 100.0)
            if buy_risk_atr is not None
            else 50.0
        )

        entry_priority_score = (
            0.40 * buy_component
            + 0.35 * prob_component
            + 0.15 * ev_component
            + 0.10 * risk_component
        )
        if buy_eligible is False:
            # Allow pre-entry names to remain visible, but rank lower than active signals.
            entry_priority_score *= 0.88
        if hard_excluded:
            entry_priority_score = min(entry_priority_score, 39.9)

        tier_a_threshold = max(_ENTRY_TIER_A_THRESHOLD, _ENTRY_TIER_B_THRESHOLD)
        tier_b_threshold = min(_ENTRY_TIER_A_THRESHOLD, _ENTRY_TIER_B_THRESHOLD)

        if not hard_excluded and entry_priority_score >= tier_a_threshold:
            tier = "A"
            tier_label = "A: 今週仕込み候補"
        elif not hard_excluded and entry_priority_score >= tier_b_threshold:
            tier = "B"
            tier_label = "B: 押し目待ち"
        else:
            tier = "C"
            tier_label = "C: 監視"

        reason_candidates: list[tuple[float, str]] = []
        reason_candidates.append((0.40 * buy_component, f"買い候補スコア {buy_component:.0f}"))
        if ml_prob is not None:
            reason_candidates.append((0.35 * prob_component, f"ML上昇確率 {ml_prob * 100.0:.1f}%"))
        if ev_short_net is not None:
            reason_candidates.append((0.15 * ev_component, f"期待値(20D) {ev_short_net * 100.0:+.2f}%"))
        if buy_risk_atr is not None:
            reason_candidates.append((0.10 * risk_component, f"下値リスク {buy_risk_atr:.2f}ATR"))

        reason_candidates.sort(key=lambda pair: pair[0], reverse=True)
        entry_reasons = [text for _, text in reason_candidates[:3]]
        if hard_excluded:
            entry_reasons = hard_exclude_reasons[:3]
        elif len(entry_reasons) < 2:
            entry_reasons.append(f"統合スコア {entry_priority_score:.1f}")
        if not hard_excluded and buy_eligible is False and len(entry_reasons) < 3:
            entry_reasons.append("初動未成立（押し目待ち）")

        item["entryPriorityScore"] = round(entry_priority_score, 2)
        item["entryPriorityTier"] = tier
        item["entryPriorityLabel"] = tier_label
        item["entryPriorityReasons"] = entry_reasons
        item["buyHardExcluded"] = hard_excluded
        item["buyHardExcludeReasons"] = hard_exclude_reasons
        item["entryPriorityComponents"] = {
            "buy": round(buy_component, 2),
            "prob": round(prob_component, 2),
            "ev": round(ev_component, 2),
            "risk": round(risk_component, 2),
        }


def _maybe_trigger_missing_data_repair(covered_codes: list[str]) -> None:
    global _last_auto_repair_ts

    if not covered_codes:
        return

    try:
        watchlist_path = resolve_watchlist_path()
        if not watchlist_path or not os.path.isfile(watchlist_path):
            return
        with watchlist_lock:
            watchlist_codes = load_watchlist_codes(watchlist_path)
    except Exception as exc:
        logger.debug("auto-repair watchlist load skipped: %s", exc)
        return

    if not watchlist_codes:
        return

    covered_set = {str(code) for code in covered_codes if code}
    missing_count = sum(1 for code in watchlist_codes if code not in covered_set)
    missing_ratio = missing_count / max(1, len(watchlist_codes))
    if missing_count < _AUTO_REPAIR_MIN_MISSING or missing_ratio < _AUTO_REPAIR_MIN_RATIO:
        return

    now_ts = time.time()
    with _auto_repair_lock:
        if now_ts - _last_auto_repair_ts < _AUTO_REPAIR_COOLDOWN_SEC:
            return
        _last_auto_repair_ts = now_ts

    try:
        cleanup_stale_jobs()
        if job_manager.is_active("force_sync") or job_manager.is_active("txt_update"):
            logger.info(
                "auto-repair skipped: active job exists (missing=%s ratio=%.3f)",
                missing_count,
                missing_ratio,
            )
            return
        job_id = job_manager.submit(
            "force_sync",
            {"ingest_retry": 3, "ingest_retry_sleep": 1.5},
            unique=True,
            message=f"Auto repair: missing coverage {missing_count}/{len(watchlist_codes)}",
            progress=0,
        )
        if job_id:
            logger.warning(
                "auto-repair queued force_sync job_id=%s missing=%s/%s ratio=%.3f",
                job_id,
                missing_count,
                len(watchlist_codes),
                missing_ratio,
            )
    except Exception as exc:
        logger.exception("auto-repair submission failed: %s", exc)


# Simple in-memory cache for screener results (to match legacy behavior of caching)
# Cache key is tied to DB mtime and request window so stale data is naturally invalidated.
_screener_cache = {
    "data": [],
    "cache_key": None,
}
_screener_cache_lock = Lock()


def _resolve_db_mtime() -> float | None:
    try:
        return os.path.getmtime(str(core_config.DB_PATH))
    except OSError:
        return None

@router.get("/screener", response_model=List[Dict[str, Any]])
def get_screener_rows(
    limit: int = 260,
    force_update: bool = False,
    screener_repo: ScreenerRepository = Depends(get_screener_repo),
    stock_repo: StockRepository = Depends(get_stock_repo),
):
    global _screener_cache

    # 1. Fetch Data
    today = jst_now().date()
    window_end = today + timedelta(days=30)
    cache_key = (
        _resolve_db_mtime(),
        int(limit),
        today.isoformat(),
        window_end.isoformat(),
    )

    if not force_update:
        with _screener_cache_lock:
            cached_key = _screener_cache.get("cache_key")
            cached_data = _screener_cache.get("data") if cached_key == cache_key else None
        if cached_data:
            cached_results = copy.deepcopy(cached_data)
            score_map = stock_repo.get_scores()
            cache_codes = [
                str(item.get("code"))
                for item in cached_results
                if isinstance(item.get("code"), str)
            ]
            _maybe_trigger_missing_data_repair(cache_codes)
            ml_map = stock_repo.get_latest_ml_pred_map(cache_codes)
            _apply_short_scores(cached_results, score_map)
            _apply_ml_metrics(cached_results, ml_map)
            _apply_short_priority_metrics(cached_results)
            _apply_entry_priority_metrics(cached_results)
            return cached_results
    
    (
        codes,
        meta_rows,
        daily_rows,
        monthly_rows,
        earnings_rows,
        rights_rows
    ) = screener_repo.fetch_screener_batch(
        daily_limit=limit,
        earnings_start=today,
        earnings_end=window_end,
        rights_min_date=today,
        monthly_limit=120,
    )
    
    # 2. Process Data
    meta_map = {row[0]: row for row in meta_rows}
    sector_map = screener_repo.fetch_sector_map(codes)
    daily_map = _group_rows_by_code(daily_rows)
    monthly_map = _group_rows_by_code(monthly_rows)
    earnings_map = {row[0]: row[1] for row in earnings_rows}
    rights_map = {row[0]: row[1] for row in rights_rows}
    short_score_map = stock_repo.get_scores()
    ml_map = stock_repo.get_latest_ml_pred_map(codes)
    
    asof_map: dict[str, int | None] = {}
    results = []
    for code in codes:
        # Extract specific rows for this code
        d_rows = daily_map.get(code, [])
        m_rows = monthly_map.get(code, [])
        asof_map[code] = d_rows[-1][1] if d_rows else None
        
        # We need to strip the code from the rows for metrics computation if it expects (date, o, h, l, c, v)
        # generic _group_rows_by_code preserves the full tuple including code at index 0.
        # metrics.py expects: date at index 0?
        # Let's check logic in metrics.py.
        # It uses row[0] as date.
        # ScreenerRepository returns (code, date, o, h, l, c, v).
        # So we need to pass `row[1:]` to metrics if metrics expects (date, ...).
        # Let's double check metrics.py logic.
        # metrics.py: `date_value = row[0]` inside `_build_weekly_bars`.
        # So it expects `(date, o, h, l, c, v)`.
        # So we must slice `row[1:]`.
        
        d_rows_sliced = [r[1:] for r in d_rows]
        m_rows_sliced = [r[1:] for r in m_rows]
        
        meta = meta_map.get(code)
        
        computed = metrics.compute_screener_metrics(d_rows_sliced, m_rows_sliced)
        
        # Merge Meta
        # Meta row: code, name, stage, score, reason, score_status, missing_reasons, score_breakdown
        sector_info = sector_map.get(code)
        industry_name = sector_info[0] if sector_info else None
        name = meta[1] if meta else (industry_name or code)
        stage = meta[2] if meta else None
        score = meta[3] if meta else None
        reason = meta[4] if meta else None
        score_status = meta[5] if meta else None
        
        # Fallback/Default logic (simplified from screener_engine)
        if not stage or stage == "UNKNOWN":
             stage = computed.get("statusLabel", "UNKNOWN")

        # Construct Result Item
        item = {
            "code": code,
            "name": name,
            "stage": stage,
            "score": score,
            "reason": reason,
            "scoreStatus": score_status,
            "eventEarningsDate": earnings_map.get(code),
            "eventRightsDate": rights_map.get(code),
            "sector33_code": sector_info[1] if sector_info else None,
            "sector33_name": sector_info[2] if sector_info else None,
            **computed
        }
        results.append(item)

    phase_map = screener_repo.fetch_phase_pred_map(asof_map)
    for item in results:
        phase_info = phase_map.get(item["code"])
        if not phase_info:
            continue
        item["earlyScore"] = phase_info["early_score"]
        item["lateScore"] = phase_info["late_score"]
        item["bodyScore"] = phase_info["body_score"]
        item["phaseN"] = phase_info["n"]
        item["phaseDt"] = phase_info["dt"]

    response_results = copy.deepcopy(results)
    _apply_short_scores(response_results, short_score_map)
    _apply_ml_metrics(response_results, ml_map)
    _apply_short_priority_metrics(response_results)
    _apply_entry_priority_metrics(response_results)

    with _screener_cache_lock:
        _screener_cache["data"] = copy.deepcopy(results)
        _screener_cache["cache_key"] = cache_key
    _maybe_trigger_missing_data_repair(codes)

    return response_results

@router.get("/ranking", response_model=Dict[str, Any])
def get_ranking(
    limit: int = 50,
    screener_repo: ScreenerRepository = Depends(get_screener_repo)
):
    # This roughly maps to `build_weekly_ranking`
    # We need a way to load rank config. 
    # For now, use an empty config or default.
    # Ideally inject ConfigRepository and load it.
    
    # 1. Fetch Data (reuses fetch_screener_batch for efficiency?)
    # ranking needs daily bars.
    # fetch_screener_batch gets 260 days.
    
    today = jst_now().date()
    (
        codes,
        meta_rows,
        daily_rows,
        monthly_rows,
        _, _
    ) = screener_repo.fetch_screener_batch(
        daily_limit=260, # Ranking needs ~260 for MA200
        earnings_start=today, # Not used for ranking but required by signature
        earnings_end=today,
        rights_min_date=today
    )
    
    daily_map = _group_rows_by_code(daily_rows)
    provisional_map: dict[str, tuple] = {}
    try:
        provisional_map = get_provisional_daily_rows_from_spark(codes)
    except Exception as exc:
        logger.debug("grid ranking provisional fetch skipped: %s", exc)
    meta_map = {row[0]: row[1] for row in meta_rows} # code -> name
    
    up_items = []
    down_items = []
    
    # Config is required.
    # We should define a minimal default config if file not found, or use ConfigRepository.
    # Assuming default for now.
    config = {
        "common": {"min_daily_bars": 80},
        "weekly": {
             "weights": {"ma_alignment": 10}, 
             "thresholds": {"volume_ratio": 1.5}
        }
    }
    
    # Process
    for code in codes:
        d_rows = daily_map.get(code, [])
        d_rows_sliced = [r[1:] for r in d_rows]
        d_rows_sliced = merge_daily_rows_with_provisional(d_rows_sliced, provisional_map.get(code))
        
        name = meta_map.get(code, code)
        
        up, down, err = ranking.score_weekly_candidate(
            code, name, d_rows_sliced, config, None
        )
        
        if up: up_items.append(up)
        if down: down_items.append(down)
        
    up_items.sort(key=lambda x: x["total_score"], reverse=True)
    down_items.sort(key=lambda x: x["total_score"], reverse=True)
    
    return {
        "up": up_items[:limit],
        "down": down_items[:limit],
        "meta": {"count": len(codes)}
    }
