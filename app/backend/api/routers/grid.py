import logging
import os
import time
from threading import Lock
from fastapi import APIRouter, Depends
from typing import List, Any, Dict
from datetime import datetime, timedelta

from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.api.dependencies import get_screener_repo, get_stock_repo
from app.backend.domain.screening import metrics
from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.services import rankings_cache
from app.backend.services import screener_snapshot_service
from app.backend.services import swing_plan_service
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
_SWING_SIDE_TOPN = max(1, int(os.getenv("MEEMEE_SWING_SIDE_TOPN", "5")))
_SWING_TOTAL_TOPN = max(_SWING_SIDE_TOPN * 2, int(os.getenv("MEEMEE_SWING_TOTAL_TOPN", "10")))
_SWING_FILL_MIN_SCORE = float(os.getenv("MEEMEE_SWING_FILL_MIN_SCORE", "0.66"))
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


def _clone_screener_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Only top-level keys are modified per request, so shallow copy is sufficient.
    return [dict(row) for row in rows]


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


def _as_of_to_ymd(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        as_int = int(text)
        if 19000101 <= as_int <= 21001231:
            return as_int
        return None
    try:
        parsed = datetime.strptime(text[:10], "%Y-%m-%d")
        return int(parsed.strftime("%Y%m%d"))
    except ValueError:
        return None


def _guess_long_setup_type(item: dict[str, Any]) -> str:
    code = str(item.get("buyPatternCode") or "").strip().lower()
    state = str(item.get("buyState") or "").strip()
    if "p2" in code:
        return "breakout"
    if "p1" in code:
        return "rebound"
    if "p3" in code:
        return "accumulation"
    if "初動" in state:
        return "breakout"
    if "底" in state:
        return "accumulation"
    return "watch"


def _guess_short_setup_type(item: dict[str, Any]) -> str:
    short_type = str(item.get("shortType") or "").strip().upper()
    if short_type == "A":
        return "breakdown"
    if short_type == "B":
        return "pressure"
    return "watch"


def _resolve_short_score(item: dict[str, Any]) -> float | None:
    direct = _first_finite(
        item.get("shortScore"),
        item.get("shortCandidateScore"),
        item.get("shortPriorityScore"),
    )
    if direct is not None:
        return float(direct)
    a_score = _first_finite(item.get("aScore"), item.get("aCandidateScore"))
    b_score = _first_finite(item.get("bScore"), item.get("bCandidateScore"))
    if a_score is None and b_score is None:
        return None
    return float((a_score or 0.0) + (b_score or 0.0))


def _apply_swing_metrics(items: list[dict[str, Any]]) -> None:
    long_candidates: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    short_candidates: list[tuple[float, dict[str, Any], dict[str, Any]]] = []

    for item in items:
        close = _first_finite(item.get("lastClose"), item.get("close"))
        atr14 = _first_finite(item.get("atr14"))
        atr_pct = (float(atr14) / float(close)) if atr14 is not None and close not in (None, 0) else None
        as_of_ymd = _as_of_to_ymd(item.get("asOf"))
        eval_payload = swing_plan_service.evaluate_swing_candidates(
            as_of_ymd=as_of_ymd,
            p_up=_first_finite(item.get("mlPUp"), item.get("mlPUpShort")),
            p_down=_first_finite(item.get("mlPDown"), item.get("mlPDownShort")),
            p_turn_up=_first_finite(item.get("mlPTurnUp")),
            p_turn_down=_first_finite(item.get("mlPTurnDown"), item.get("mlPTurnDownShort")),
            ev20_net=_first_finite(item.get("mlEv20Net"), item.get("mlEvShortNet")),
            long_setup_type=_guess_long_setup_type(item),
            short_setup_type=_guess_short_setup_type(item),
            playbook_bonus_long=_first_finite(item.get("buyStateScore")),
            playbook_bonus_short=_first_finite(item.get("shortPriorityScore")),
            short_score=_resolve_short_score(item),
            atr_pct=atr_pct,
            liquidity20d=_first_finite(item.get("liquidity20d")),
        )
        long_eval = eval_payload.get("long") if isinstance(eval_payload, dict) else {}
        short_eval = eval_payload.get("short") if isinstance(eval_payload, dict) else {}
        long_score = _first_finite((long_eval or {}).get("score"))
        short_score = _first_finite((short_eval or {}).get("score"))
        long_qualified = bool((long_eval or {}).get("qualified"))
        short_qualified = bool((short_eval or {}).get("qualified"))

        best_side = "long"
        best_score = long_score
        best_eval = long_eval
        if (short_score is not None and best_score is None) or (
            short_score is not None and best_score is not None and short_score > best_score
        ):
            best_side = "short"
            best_score = short_score
            best_eval = short_eval

        item["swingScore"] = float(best_score) if best_score is not None else None
        item["swingSide"] = best_side if bool((best_eval or {}).get("qualified")) else "none"
        item["swingQualified"] = False
        base_reasons = (best_eval or {}).get("reasons")
        item["swingReasons"] = [str(v) for v in base_reasons] if isinstance(base_reasons, list) else []
        item["swingLongScore"] = float(long_score) if long_score is not None else None
        item["swingShortScore"] = float(short_score) if short_score is not None else None

        if long_score is not None and long_qualified:
            long_candidates.append((float(long_score), item, long_eval if isinstance(long_eval, dict) else {}))
        if short_score is not None and short_qualified:
            short_candidates.append((float(short_score), item, short_eval if isinstance(short_eval, dict) else {}))

    long_candidates.sort(key=lambda row: (-row[0], str(row[1].get("code") or "")))
    short_candidates.sort(key=lambda row: (-row[0], str(row[1].get("code") or "")))

    selected_by_code: dict[str, tuple[str, float, dict[str, Any]]] = {}
    long_count = 0
    for score, item, side_eval in long_candidates:
        code = str(item.get("code") or "")
        if not code or code in selected_by_code:
            continue
        if long_count >= _SWING_SIDE_TOPN:
            break
        selected_by_code[code] = ("long", float(score), side_eval)
        long_count += 1

    short_count = 0
    for score, item, side_eval in short_candidates:
        code = str(item.get("code") or "")
        if not code or code in selected_by_code:
            continue
        if short_count >= _SWING_SIDE_TOPN:
            break
        selected_by_code[code] = ("short", float(score), side_eval)
        short_count += 1

    if len(selected_by_code) < _SWING_TOTAL_TOPN:
        fill_pool: list[tuple[float, str, str, dict[str, Any]]] = []
        for score, item, side_eval in long_candidates:
            code = str(item.get("code") or "")
            if not code or code in selected_by_code:
                continue
            if score < _SWING_FILL_MIN_SCORE:
                continue
            fill_pool.append((float(score), code, "long", side_eval))
        for score, item, side_eval in short_candidates:
            code = str(item.get("code") or "")
            if not code or code in selected_by_code:
                continue
            if score < _SWING_FILL_MIN_SCORE:
                continue
            fill_pool.append((float(score), code, "short", side_eval))
        fill_pool.sort(key=lambda row: (-row[0], row[1]))
        for score, code, inferred_side, side_eval in fill_pool:
            if len(selected_by_code) >= _SWING_TOTAL_TOPN:
                break
            selected_by_code[code] = (inferred_side, float(score), side_eval if isinstance(side_eval, dict) else {})

    for item in items:
        code = str(item.get("code") or "")
        selected = selected_by_code.get(code)
        if not selected:
            continue
        side, score, side_eval = selected
        reasons = side_eval.get("reasons") if isinstance(side_eval, dict) else []
        resolved_reasons = [str(v) for v in reasons] if isinstance(reasons, list) else []
        resolved_reasons.append("selected_top_candidates")
        item["swingSide"] = side
        item["swingScore"] = float(score)
        item["swingQualified"] = True
        item["swingReasons"] = resolved_reasons


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

    now_ts = time.time()
    with _auto_repair_lock:
        if now_ts - _last_auto_repair_ts < _AUTO_REPAIR_COOLDOWN_SEC:
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


# Cache only the normalized base response to avoid duplicated row caches.
_grid_api_cache = {
    "data": [],
    "codes": [],
    "cache_key": None,
}
_grid_api_lock = Lock()


def _resolve_db_mtime() -> float | None:
    try:
        return os.path.getmtime(str(core_config.DB_PATH))
    except OSError:
        return None


def _build_grid_rankings_fallback(limit: int) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 260), 260))
    fallback_rows: list[dict[str, Any]] = []
    seen_codes: set[str] = set()

    for direction, stage_label in (("up", "RANKING_UP"), ("down", "RANKING_DOWN")):
        payload = rankings_cache.get_rankings(
            "W",
            "latest",
            direction,
            safe_limit,
            mode="rule",
            risk_mode="balanced",
        )
        items = payload.get("items", []) if isinstance(payload, dict) else []
        for rank, src in enumerate(items, start=1):
            code = str(src.get("code") or "").strip()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            name = str(src.get("name") or code).strip() or code
            score = _first_finite(
                src.get("entryScore"),
                src.get("hybridScore"),
                src.get("changePct"),
            )
            reason_parts = ["RANKING_CACHE_FALLBACK", stage_label, f"rank={rank}"]
            as_of = str(src.get("asOf") or "").strip()
            if as_of:
                reason_parts.append(f"asOf={as_of}")
            fallback_rows.append(
                {
                    "code": code,
                    "name": name,
                    "stage": stage_label,
                    "score": float(score) if score is not None else None,
                    "reason": " / ".join(reason_parts),
                    "scoreStatus": "STALE_FALLBACK",
                    "missingReasons": ["GRID_DB_LOCKED_FALLBACK"],
                    "scoreBreakdown": None,
                    "chg1W": _first_finite(src.get("changePct")),
                    "entryPriorityScore": _first_finite(src.get("entryScore")),
                    "mlEv20Net": _first_finite(src.get("hybridScore")),
                }
            )
            if len(fallback_rows) >= safe_limit:
                return fallback_rows
    return fallback_rows

def _compute_live_screener_rows(
    limit: int = 260,
    screener_repo: ScreenerRepository = Depends(get_screener_repo),
    stock_repo: StockRepository = Depends(get_stock_repo),
):
    today = jst_now().date()
    window_end = today + timedelta(days=30)
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
        d_rows = daily_map.get(code, [])
        m_rows = monthly_map.get(code, [])
        asof_map[code] = d_rows[-1][1] if d_rows else None

        d_rows_sliced = [r[1:] for r in d_rows]
        m_rows_sliced = [r[1:] for r in m_rows]

        meta = meta_map.get(code)
        computed = metrics.compute_screener_metrics(d_rows_sliced, m_rows_sliced)

        sector_info = sector_map.get(code)
        industry_name = sector_info[0] if sector_info else None
        name = meta[1] if meta else (industry_name or code)
        stage = meta[2] if meta else None
        score = meta[3] if meta else None
        reason = meta[4] if meta else None
        score_status = meta[5] if meta else None

        if not stage or stage == "UNKNOWN":
            stage = computed.get("statusLabel", "UNKNOWN")

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

    response_results = results
    _apply_short_scores(response_results, short_score_map)
    _apply_ml_metrics(response_results, ml_map)
    _apply_short_priority_metrics(response_results)
    _apply_entry_priority_metrics(response_results)
    _apply_swing_metrics(response_results)
    _maybe_trigger_missing_data_repair(codes)
    return response_results


@router.get("/screener", response_model=Dict[str, Any])
def get_screener_rows(
    limit: int = 260,
    force_update: bool = False,
    screener_repo: ScreenerRepository = Depends(get_screener_repo),
    stock_repo: StockRepository = Depends(get_stock_repo),
):
    response = screener_snapshot_service.get_screener_snapshot_response(
        limit=limit,
        force_refresh=force_update,
        screener_repo=screener_repo,
        stock_repo=stock_repo,
    )
    return response

@router.get("/ranking", response_model=Dict[str, Any])
def get_ranking(limit: int = 50):
    safe_limit = max(1, min(int(limit or 50), 200))

    def _as_legacy_rows(items: list[dict]) -> list[dict]:
        rows: list[dict] = []
        for src in items:
            row = dict(src)
            score = _first_finite(
                row.get("entryScore"),
                row.get("hybridScore"),
                row.get("changePct"),
            )
            row.setdefault("total_score", float(score) if score is not None else 0.0)
            row.setdefault("as_of", row.get("asOf"))
            row.setdefault("reasons", [])
            row.setdefault("badges", [])
            rows.append(row)
        rows.sort(key=lambda item: float(item.get("total_score") or 0.0), reverse=True)
        return rows

    try:
        up_payload = rankings_cache.get_rankings(
            "W",
            "latest",
            "up",
            safe_limit,
            mode="rule",
            risk_mode="balanced",
        )
        down_payload = rankings_cache.get_rankings(
            "W",
            "latest",
            "down",
            safe_limit,
            mode="rule",
            risk_mode="balanced",
        )
    except Exception as exc:
        logger.warning("grid ranking fallback failed: %s", exc)
        return {
            "up": [],
            "down": [],
            "meta": {"count": 0, "source": "rankings_cache", "error": str(exc)},
        }

    up_items = _as_legacy_rows(up_payload.get("items", []) if isinstance(up_payload, dict) else [])
    down_items = _as_legacy_rows(down_payload.get("items", []) if isinstance(down_payload, dict) else [])
    return {
        "up": up_items[:safe_limit],
        "down": down_items[:safe_limit],
        "meta": {
            "count": max(len(up_items), len(down_items)),
            "source": "rankings_cache",
        },
    }
