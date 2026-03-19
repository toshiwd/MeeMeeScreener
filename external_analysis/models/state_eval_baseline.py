from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db
from external_analysis.ops.ops_schema import connect_ops_db, ensure_ops_schema
from external_analysis.ops.store import _apply_ops_retention
from external_analysis.similarity.baseline import (
    _load_case_vectors,
    _load_query_vectors,
    _vector_distance,
    build_case_library,
)

BASELINE_VERSION = "state_eval_baseline_v2"
CHALLENGER_VERSION = "state_eval_challenger_v2"
DECISION_ENTER = "enter"
DECISION_WAIT = "wait"
DECISION_SKIP = "skip"
LONG_ADVERSE_MOVE_THRESHOLD = 0.08
SHORT_SQUEEZE_MOVE_THRESHOLD = 0.08
PROMOTION_MIN_SAMPLE_COUNT = 50
SIMILARITY_SUPPORT_TOP_K = 3


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _as_of_date_text(value: int) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    return float(value)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _normalize_signal(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.5
    return _clamp01((float(value) - low) / (high - low))


def _build_reason_codes(*codes: str) -> str:
    return json.dumps([code for code in codes if code], ensure_ascii=False)


def _build_reason_text_top3(*texts: str) -> str:
    return json.dumps([text for text in texts if text][:3], ensure_ascii=False)


def _entry_side_from_event_type(event_type: str) -> str | None:
    normalized = str(event_type or "").strip().upper()
    if normalized in {"SPOT_BUY", "MARGIN_OPEN_LONG", "BUY_OPEN", "OPEN_LONG"}:
        return "long"
    if normalized in {"MARGIN_OPEN_SHORT", "SELL_OPEN", "OPEN_SHORT"}:
        return "short"
    return None


def _assign_holding_band(side: str, row: dict[str, Any]) -> str:
    ret_20 = _safe_float(row.get("ret_20_past"))
    atr_ratio = _safe_float(row.get("atr_ratio"))
    if side == "long":
        if ret_20 >= 0.12 and atr_ratio <= 0.05:
            return "buy_21_60"
        return "buy_5_20"
    if ret_20 <= -0.06 and atr_ratio <= 0.06:
        return "sell_11_20"
    return "sell_5_10"


def _derive_strategy_tags(side: str, row: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    close_vs_ma20 = _safe_float(row.get("close_vs_ma20"))
    ret_5 = _safe_float(row.get("ret_5_past"))
    ret_20 = _safe_float(row.get("ret_20_past"))
    volume_ratio = _safe_float(row.get("volume_ratio"), 1.0)
    box_state = str(row.get("box_state") or "").lower()
    candle_flags = _parse_candle_flags(row.get("candle_flags"))
    prev_candle_flags = _parse_candle_flags(row.get("prev_candle_flags"))
    prev2_candle_flags = _parse_candle_flags(row.get("prev2_candle_flags"))
    if side == "long":
        if "break" in box_state or ret_20 >= 0.10:
            tags.append("box_breakout")
        if close_vs_ma20 >= 0.0:
            tags.append("ma20_reclaim")
        if ret_20 >= 0.15:
            tags.append("higher_high_break")
        if 0.0 <= ret_5 <= 0.06 and ret_20 >= 0.05:
            tags.append("pullback_rebound")
        if volume_ratio >= 1.5:
            tags.append("volume_surge")
        if ret_5 >= 0.04 and close_vs_ma20 >= 0.02:
            tags.append("big_bear_full_reclaim")
        if any("bullish_engulf" in token or "bull_engulf" in token for token in candle_flags):
            tags.append("bullish_engulfing")
        if any("hammer" in token for token in candle_flags):
            tags.append("hammer_reversal")
        if any("inside" in token for token in prev_candle_flags) and ret_5 >= 0.0:
            tags.append("inside_break_bull")
        if any("inside" in token for token in prev_candle_flags) and any("bullish_engulf" in token or "bull_engulf" in token for token in candle_flags):
            tags.append("bullish_engulfing_after_inside")
        if any("hammer" in token for token in candle_flags) and (
            any("bear" in token for token in prev_candle_flags) or ret_5 <= 0.02
        ):
            tags.append("hammer_after_bear")
        if (
            any("bear" in token or "doji" in token for token in (prev_candle_flags | prev2_candle_flags))
            and ret_5 >= 0.02
            and close_vs_ma20 >= 0.0
        ):
            tags.append("bullish_follow_through")
        if (
            any("inside" in token for token in prev_candle_flags)
            and any("bear" in token or "doji" in token for token in prev2_candle_flags)
            and (
                any("bullish_engulf" in token or "bull_engulf" in token for token in candle_flags)
                or any("hammer" in token for token in candle_flags)
            )
        ):
            tags.append("three_bar_bull_reversal")
        return sorted(set(tags or ["ma20_reclaim"]))
    if close_vs_ma20 >= 0.04:
        tags.append("extension_fade")
    if ret_5 > 0.02 and ret_20 <= 0.02:
        tags.append("rebound_failure")
    if ret_5 >= 0.05:
        tags.append("big_bear_retrace_short")
    if ret_20 <= -0.08:
        tags.append("prev_low_break")
    if volume_ratio >= 1.5:
        tags.append("volume_exhaustion_fade")
    if any("bearish_engulf" in token or "bear_engulf" in token for token in candle_flags):
        tags.append("bearish_engulfing")
    if any("shooting_star" in token or "upper_shadow" in token for token in candle_flags):
        tags.append("shooting_star_reversal")
    if any("inside" in token for token in prev_candle_flags) and ret_5 <= 0.02:
        tags.append("inside_break_bear")
    if any("inside" in token for token in prev_candle_flags) and any("bearish_engulf" in token or "bear_engulf" in token for token in candle_flags):
        tags.append("bearish_engulfing_after_inside")
    if any("shooting_star" in token or "upper_shadow" in token for token in candle_flags) and (
        any("bull" in token for token in prev_candle_flags) or ret_5 >= -0.01
    ):
        tags.append("shooting_star_after_bull")
    if (
        any("bull" in token or "doji" in token for token in (prev_candle_flags | prev2_candle_flags))
        and close_vs_ma20 >= 0.02
    ):
        tags.append("bearish_follow_through")
    if (
        any("inside" in token for token in prev_candle_flags)
        and any("bull" in token or "doji" in token for token in prev2_candle_flags)
        and (
            any("bearish_engulf" in token or "bear_engulf" in token for token in candle_flags)
            or any("shooting_star" in token or "upper_shadow" in token for token in candle_flags)
        )
    ):
        tags.append("three_bar_bear_reversal")
    return sorted(set(tags or ["extension_fade"]))


def _reason_texts(
    *,
    side: str,
    row: dict[str, Any],
    tags: list[str],
    similarity_evidence: dict[str, float] | None = None,
    tag_prior_summary: dict[str, float | str | None] | None = None,
) -> list[str]:
    label_by_tag = {
        "box_breakout": "Box breakout",
        "ma20_reclaim": "20MA reclaim",
        "higher_high_break": "Higher high break",
        "pullback_rebound": "Pullback rebound",
        "volume_surge": "Volume surge",
        "big_bear_full_reclaim": "Bear candle reclaim",
        "bullish_engulfing": "Bullish engulfing",
        "hammer_reversal": "Hammer reversal",
        "inside_break_bull": "Inside break bull",
        "bullish_follow_through": "Bullish follow-through",
        "bullish_engulfing_after_inside": "Bull engulf after inside",
        "hammer_after_bear": "Hammer after bear",
        "three_bar_bull_reversal": "3-bar bull reversal",
        "extension_fade": "Extension fade",
        "rebound_failure": "Rebound failure",
        "big_bear_retrace_short": "Bear retrace short",
        "prev_low_break": "Prev low break",
        "volume_exhaustion_fade": "Volume fade",
        "bearish_engulfing": "Bearish engulfing",
        "shooting_star_reversal": "Shooting star",
        "inside_break_bear": "Inside break bear",
        "bearish_follow_through": "Bearish follow-through",
        "bearish_engulfing_after_inside": "Bear engulf after inside",
        "shooting_star_after_bull": "Shooting star after bull",
        "three_bar_bear_reversal": "3-bar bear reversal",
    }
    primary = label_by_tag.get(tags[0], tags[0].replace("_", " ")) if tags else "Setup"
    if side == "long":
        similar = _build_similarity_reason(side=side, evidence=similarity_evidence)
        risk = "Adverse risk capped" if _safe_float(row.get("atr_ratio")) < 0.06 else "Adverse risk elevated"
    else:
        similar = _build_similarity_reason(side=side, evidence=similarity_evidence)
        risk = "Squeeze risk capped" if _safe_float(row.get("close_vs_ma20")) < 0.06 else "Squeeze risk elevated"
    prior_signal = _safe_float((tag_prior_summary or {}).get("tag_prior_signal"), 0.5)
    combo_signal = _safe_float((tag_prior_summary or {}).get("combo_prior_signal"), 0.5)
    prior_tag = str((tag_prior_summary or {}).get("best_prior_tag") or "")
    if combo_signal >= 0.62 and prior_tag:
        prior_text = f"Combo strength: {label_by_tag.get(prior_tag, prior_tag.replace('_', ' '))}"
    elif prior_signal >= 0.60 and prior_tag:
        prior_text = f"Historically strong: {label_by_tag.get(prior_tag, prior_tag.replace('_', ' '))}"
    elif prior_signal <= 0.40 and prior_tag:
        prior_text = f"Historical caution: {label_by_tag.get(prior_tag, prior_tag.replace('_', ' '))}"
    else:
        prior_text = similar
    texts = [primary, prior_text]
    if prior_text != similar:
        texts.append(similar)
    texts.append(risk)
    return texts[:3]


def _parse_json_array(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _parse_candle_flags(value: Any) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    normalized = (
        text.replace("[", " ")
        .replace("]", " ")
        .replace('"', " ")
        .replace("'", " ")
        .replace("|", ",")
        .replace(";", ",")
    )
    tokens = {part.strip().replace(" ", "_") for part in normalized.split(",") if part.strip()}
    return {token for token in tokens if token}


def _tag_loss_threshold(side: str) -> float:
    return LONG_ADVERSE_MOVE_THRESHOLD if str(side) == "long" else SHORT_SQUEEZE_MOVE_THRESHOLD


def _teacher_confidence_weight(*counts: float) -> float:
    total = max(0.0, sum(max(0.0, float(value)) for value in counts))
    return _clamp01(total / 12.0)


def _blend_teacher_signal(raw_signal: float, confidence_weight: float) -> float:
    return 0.5 + ((float(raw_signal) - 0.5) * (0.35 + (0.65 * _clamp01(confidence_weight))))


def _teacher_effective_signal(values: dict[str, float]) -> float:
    alignment = _safe_float(values.get("alignment_score"), 0.5)
    position_bias = _safe_float(values.get("position_bias"), 0.0)
    band_alignment = _safe_float(values.get("band_alignment"), 0.5)
    tag_alignment = _safe_float(values.get("tag_alignment"), 0.5)
    confidence_weight = _safe_float(values.get("confidence_weight"), 0.0)
    raw_signal = (
        (0.30 * alignment)
        + (0.15 * position_bias)
        + (0.25 * band_alignment)
        + (0.30 * tag_alignment)
    )
    return _blend_teacher_signal(raw_signal, confidence_weight)


def _summarize_teacher_scores(teacher_scores: list[dict[str, float]]) -> dict[str, float]:
    if not teacher_scores:
        return {
            "alignment_score": 0.5,
            "position_bias": 0.0,
            "band_alignment": 0.5,
            "tag_alignment": 0.5,
            "confidence_weight": 0.0,
            "effective_signal": 0.5,
            "trade_count": 0.0,
            "band_trade_count": 0.0,
            "tag_trade_count": 0.0,
        }
    summary = {
        "alignment_score": sum(_safe_float(item.get("alignment_score"), 0.5) for item in teacher_scores) / len(teacher_scores),
        "position_bias": sum(_safe_float(item.get("position_bias"), 0.0) for item in teacher_scores) / len(teacher_scores),
        "band_alignment": sum(_safe_float(item.get("band_alignment"), 0.5) for item in teacher_scores) / len(teacher_scores),
        "tag_alignment": sum(_safe_float(item.get("tag_alignment"), 0.5) for item in teacher_scores) / len(teacher_scores),
        "confidence_weight": sum(_safe_float(item.get("confidence_weight"), 0.0) for item in teacher_scores) / len(teacher_scores),
        "trade_count": sum(_safe_float(item.get("trade_count"), 0.0) for item in teacher_scores) / len(teacher_scores),
        "band_trade_count": sum(_safe_float(item.get("band_trade_count"), 0.0) for item in teacher_scores) / len(teacher_scores),
        "tag_trade_count": sum(_safe_float(item.get("tag_trade_count"), 0.0) for item in teacher_scores) / len(teacher_scores),
    }
    summary["effective_signal"] = _teacher_effective_signal(summary)
    return summary


def _build_similarity_reason(*, side: str, evidence: dict[str, float] | None) -> str:
    if not evidence or int(_safe_float(evidence.get("neighbor_count"), 0.0)) <= 0:
        return "Similar setup data thin"
    avg_path_20 = _safe_float(evidence.get("avg_path_20"), 0.0)
    big_drop_rate = _safe_float(evidence.get("big_drop_rate"), 0.0)
    big_up_rate = _safe_float(evidence.get("big_up_rate"), 0.0)
    if side == "long":
        if avg_path_20 >= 0.08 and big_drop_rate <= 0.34:
            return "Similar charts rose after setup"
        if big_drop_rate >= 0.45 or avg_path_20 <= -0.05:
            return "Similar charts often failed lower"
        return "Similar chart path mixed"
    if avg_path_20 <= -0.08 and big_up_rate <= 0.34:
        return "Similar charts faded after setup"
    if big_up_rate >= 0.45 or avg_path_20 >= 0.05:
        return "Similar charts often squeezed up"
    return "Similar chart path mixed"


def _similarity_signal_for_side(*, side: str, evidence: dict[str, float] | None) -> float:
    if not evidence or int(_safe_float(evidence.get("neighbor_count"), 0.0)) <= 0:
        return 0.5
    avg_path_20 = _safe_float(evidence.get("avg_path_20"), 0.0)
    success_rate = _safe_float(evidence.get("success_rate"), 0.0)
    big_drop_rate = _safe_float(evidence.get("big_drop_rate"), 0.0)
    big_up_rate = _safe_float(evidence.get("big_up_rate"), 0.0)
    if side == "long":
        path_signal = _normalize_signal(avg_path_20, -0.12, 0.20)
        safety_signal = 1.0 - _clamp01(big_drop_rate)
        return _clamp01((0.35 * path_signal) + (0.35 * success_rate) + (0.30 * safety_signal))
    path_signal = _normalize_signal(-avg_path_20, -0.12, 0.20)
    squeeze_safety = 1.0 - _clamp01(big_up_rate)
    downside_signal = _clamp01(big_drop_rate)
    return _clamp01((0.35 * path_signal) + (0.35 * squeeze_safety) + (0.30 * downside_signal))


def _load_similarity_support(
    *,
    export_db_path: str | None,
    label_db_path: str | None,
    similarity_db_path: str | None,
    as_of_date: int,
    codes: list[str],
    top_k: int = SIMILARITY_SUPPORT_TOP_K,
) -> dict[str, dict[str, float]]:
    if not codes or not label_db_path:
        return {}
    try:
        build_case_library(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            similarity_db_path=similarity_db_path,
            as_of_date=as_of_date,
            codes=codes,
        )
        case_vectors, library, case_paths = _load_case_vectors(similarity_db_path)
        query_rows = _load_query_vectors(export_db_path, as_of_date, codes=codes)
    except Exception:
        return {}
    evidence_by_code: dict[str, dict[str, float]] = {}
    effective_top_k = max(1, int(top_k))
    for query in query_rows:
        query_code = str(query["code"])
        scored: list[tuple[str, float]] = []
        for case_id, vector in case_vectors.items():
            case_meta = library.get(case_id)
            if not case_meta:
                continue
            if str(case_meta.get("code") or "") == query_code:
                continue
            if int(case_meta.get("anchor_date") or 0) >= int(as_of_date):
                continue
            scored.append((case_id, _vector_distance(query["vector"], vector)))
        scored.sort(key=lambda item: (float(item[1]), str(item[0])))
        top_rows = scored[:effective_top_k]
        if not top_rows:
            continue
        avg_similarity_score = sum(1.0 / (1.0 + float(distance)) for _, distance in top_rows) / len(top_rows)
        future_20_values: list[float] = []
        success_hits = 0
        big_drop_hits = 0
        big_up_hits = 0
        for case_id, distance in top_rows:
            case_meta = library.get(case_id, {})
            if bool(case_meta.get("success_flag")):
                success_hits += 1
            if str(case_meta.get("outcome_class") or "") == "big_drop":
                big_drop_hits += 1
            if str(case_meta.get("outcome_class") or "") == "big_up":
                big_up_hits += 1
            path_rows = case_paths.get(case_id, [])
            if path_rows:
                future_20_values.append(float(path_rows[-1].get("path_return_norm") or 0.0))
        evidence_by_code[query_code] = {
            "neighbor_count": float(len(top_rows)),
            "avg_similarity_score": float(avg_similarity_score),
            "avg_path_20": (sum(future_20_values) / len(future_20_values)) if future_20_values else 0.0,
            "success_rate": float(success_hits / len(top_rows)),
            "big_drop_rate": float(big_drop_hits / len(top_rows)),
            "big_up_rate": float(big_up_hits / len(top_rows)),
        }
    return evidence_by_code


def _tag_rollup_hint(*, labeled_count: int, expectancy_mean: float | None, large_loss_rate: float | None) -> str:
    if labeled_count < PROMOTION_MIN_SAMPLE_COUNT:
        return "needs_samples"
    if expectancy_mean is None:
        return "needs_labels"
    if expectancy_mean <= 0.0:
        return "negative_expectancy"
    if large_loss_rate is not None and large_loss_rate > 0.35:
        return "risk_heavy"
    return "promotable"


def _is_candle_research_tag(strategy_tag: str) -> bool:
    return str(strategy_tag) in {
        "bullish_engulfing",
        "hammer_reversal",
        "inside_break_bull",
        "bullish_follow_through",
        "bullish_engulfing_after_inside",
        "hammer_after_bear",
        "three_bar_bull_reversal",
        "bearish_engulfing",
        "shooting_star_reversal",
        "inside_break_bear",
        "bearish_follow_through",
        "bearish_engulfing_after_inside",
        "shooting_star_after_bull",
        "three_bar_bear_reversal",
    }


def _is_candle_combo_tag(strategy_tag: str) -> bool:
    return str(strategy_tag) in {
        "bullish_engulfing_after_inside",
        "hammer_after_bear",
        "three_bar_bull_reversal",
        "bearish_engulfing_after_inside",
        "shooting_star_after_bull",
        "three_bar_bear_reversal",
    }


def _tag_prior_effective_signal(
    *,
    expectancy_mean: float | None,
    large_loss_rate: float | None,
    win_rate: float | None,
    readiness_hint: str,
    labeled_count: float,
) -> float:
    expectancy_signal = (
        0.5 if expectancy_mean is None else _normalize_signal(float(expectancy_mean), -0.08, 0.12)
    )
    loss_signal = (
        0.5
        if large_loss_rate is None
        else 1.0 - _normalize_signal(float(large_loss_rate), 0.05, 0.45)
    )
    win_signal = 0.5 if win_rate is None else _normalize_signal(float(win_rate), 0.35, 0.70)
    readiness_signal = {
        "promotable": 0.72,
        "needs_samples": 0.48,
        "needs_labels": 0.45,
        "negative_expectancy": 0.24,
        "risk_heavy": 0.18,
    }.get(str(readiness_hint or ""), 0.5)
    raw_signal = (
        (0.38 * expectancy_signal)
        + (0.28 * loss_signal)
        + (0.20 * win_signal)
        + (0.14 * readiness_signal)
    )
    confidence_weight = _clamp01(float(labeled_count) / 150.0)
    return 0.5 + ((raw_signal - 0.5) * (0.35 + (0.65 * confidence_weight)))


def _load_historical_tag_priors(
    *,
    ops_db_path: str | None,
    as_of_date: int,
    candidate_keys: list[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, float | str]]:
    if not ops_db_path or not candidate_keys:
        return {}
    side_values = sorted({str(item["side"]) for item in candidate_keys})
    band_values = sorted({str(item["holding_band"]) for item in candidate_keys})
    tag_values = sorted({str(item["strategy_tag"]) for item in candidate_keys})
    if not side_values or not band_values or not tag_values:
        return {}
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        side_placeholders = ", ".join(["?"] * len(side_values))
        band_placeholders = ", ".join(["?"] * len(band_values))
        tag_placeholders = ", ".join(["?"] * len(tag_values))
        rows = conn.execute(
            f"""
            SELECT
                as_of_date,
                side,
                holding_band,
                strategy_tag,
                labeled_count,
                expectancy_mean,
                large_loss_rate,
                win_rate,
                readiness_hint
            FROM external_state_eval_tag_rollups
            WHERE as_of_date < CAST(? AS DATE)
              AND side IN ({side_placeholders})
              AND holding_band IN ({band_placeholders})
              AND strategy_tag IN ({tag_placeholders})
            ORDER BY as_of_date DESC
            """,
            [_as_of_date_text(as_of_date), *side_values, *band_values, *tag_values],
        ).fetchall()
    finally:
        conn.close()
    by_key: dict[tuple[str, str, str], list[tuple[Any, ...]]] = {}
    valid_keys = {
        (str(item["side"]), str(item["holding_band"]), str(item["strategy_tag"]))
        for item in candidate_keys
    }
    for row in rows:
        key = (str(row[1]), str(row[2]), str(row[3]))
        if key not in valid_keys:
            continue
        bucket = by_key.setdefault(key, [])
        if len(bucket) < 5:
            bucket.append(row)
    priors: dict[tuple[str, str, str], dict[str, float | str]] = {}
    for key, history in by_key.items():
        weighted_expectancy = 0.0
        weighted_loss = 0.0
        weighted_win = 0.0
        total_weight = 0.0
        readiness_counts: dict[str, float] = {}
        labeled_total = 0.0
        for index, row in enumerate(history):
            labeled_count = max(0.0, _safe_float(row[4], 0.0))
            recency_weight = float(max(1, len(history) - index))
            sample_weight = min(labeled_count, 100.0) / 100.0
            weight = recency_weight * max(0.25, sample_weight)
            weighted_expectancy += _safe_float(row[5], 0.0) * weight
            weighted_loss += _safe_float(row[6], 0.0) * weight
            weighted_win += _safe_float(row[7], 0.5) * weight
            total_weight += weight
            labeled_total += labeled_count
            hint = str(row[8] or "")
            readiness_counts[hint] = readiness_counts.get(hint, 0.0) + weight
        if total_weight <= 0.0:
            continue
        dominant_hint = max(readiness_counts.items(), key=lambda item: (item[1], item[0]))[0] if readiness_counts else "needs_samples"
        expectancy_mean = weighted_expectancy / total_weight
        large_loss_rate = weighted_loss / total_weight
        win_rate = weighted_win / total_weight
        priors[key] = {
            "effective_signal": float(
                _tag_prior_effective_signal(
                    expectancy_mean=expectancy_mean,
                    large_loss_rate=large_loss_rate,
                    win_rate=win_rate,
                    readiness_hint=dominant_hint,
                    labeled_count=labeled_total,
                )
            ),
            "expectancy_mean": float(expectancy_mean),
            "large_loss_rate": float(large_loss_rate),
            "win_rate": float(win_rate),
            "labeled_count": float(labeled_total),
            "readiness_hint": dominant_hint,
        }
    return priors


def _summarize_tag_prior_scores(
    *,
    strategy_tags: list[str],
    side: str,
    holding_band: str,
    tag_priors: dict[tuple[str, str, str], dict[str, float | str]],
) -> dict[str, float | str | None]:
    if not strategy_tags:
        return {
            "tag_prior_signal": 0.5,
            "combo_prior_signal": 0.5,
            "prior_labeled_count": 0.0,
            "best_prior_tag": None,
            "best_prior_hint": None,
        }
    matched = [
        (tag, tag_priors.get((side, holding_band, tag), {}))
        for tag in strategy_tags
        if tag_priors.get((side, holding_band, tag), {})
    ]
    if not matched:
        return {
            "tag_prior_signal": 0.5,
            "combo_prior_signal": 0.5,
            "prior_labeled_count": 0.0,
            "best_prior_tag": None,
            "best_prior_hint": None,
        }
    total_signal = 0.0
    total_weight = 0.0
    combo_signal = 0.0
    combo_weight = 0.0
    best_tag = None
    best_signal = -1.0
    best_hint = None
    labeled_total = 0.0
    for tag, prior in matched:
        labeled_count = max(0.0, _safe_float(prior.get("labeled_count"), 0.0))
        weight = max(0.35, min(labeled_count, 100.0) / 100.0)
        signal = _safe_float(prior.get("effective_signal"), 0.5)
        total_signal += signal * weight
        total_weight += weight
        labeled_total += labeled_count
        if _is_candle_combo_tag(tag):
            combo_signal += signal * weight
            combo_weight += weight
        if signal > best_signal:
            best_signal = signal
            best_tag = tag
            best_hint = str(prior.get("readiness_hint") or "")
    return {
        "tag_prior_signal": float(total_signal / total_weight) if total_weight > 0 else 0.5,
        "combo_prior_signal": float(combo_signal / combo_weight) if combo_weight > 0 else 0.5,
        "prior_labeled_count": float(labeled_total),
        "best_prior_tag": best_tag,
        "best_prior_hint": best_hint,
    }


def _compact_failure_example(
    *,
    code: str,
    side: str,
    holding_band: str,
    strategy_tag: str,
    as_of_date: int,
    expected_return: float,
    adverse_move: float,
    decision: str,
    teacher_alignment: float,
) -> dict[str, Any]:
    return {
        "code": str(code),
        "side": str(side),
        "holding_band": str(holding_band),
        "strategy_tag": str(strategy_tag),
        "as_of_date": _as_of_date_text(as_of_date),
        "decision": str(decision),
        "expected_return": float(round(expected_return, 6)),
        "adverse_move": float(round(adverse_move, 6)),
        "teacher_alignment": float(round(teacher_alignment, 6)),
    }


def _persist_tag_validation_rollups(
    *,
    conn,
    publish_id: str,
    as_of_date: int,
    champion_rows: list[dict[str, Any]],
    labels: dict[tuple[str, str], dict[str, float | None]],
    teacher_profile: dict[tuple[str, str, str, str], dict[str, float]],
    similarity_support: dict[str, dict[str, float]],
    created_at: datetime,
) -> list[dict[str, Any]]:
    accumulators: dict[tuple[str, str, str], dict[str, Any]] = {}
    for champion in champion_rows:
        code = str(champion.get("code") or "")
        side = str(champion.get("side") or "")
        holding_band = str(champion.get("holding_band") or "")
        decision = str(champion.get("decision_3way") or "")
        strategy_tags = [str(tag) for tag in _parse_json_array(champion.get("strategy_tags"))]
        if not code or not side or not holding_band or not strategy_tags:
            continue
        label = labels.get((code, side), {})
        expected_return = label.get("expected_return")
        adverse_move = label.get("adverse_move")
        labeled = expected_return is not None and adverse_move is not None
        threshold = _tag_loss_threshold(side)
        for strategy_tag in strategy_tags:
            key = (side, holding_band, strategy_tag)
            bucket = accumulators.setdefault(
                key,
                {
                    "observation_count": 0,
                    "labeled_count": 0,
                    "enter_count": 0,
                    "wait_count": 0,
                    "skip_count": 0,
                    "expected_returns": [],
                    "adverse_moves": [],
                    "teacher_alignments": [],
                    "teacher_signals": [],
                    "similarity_signals": [],
                    "large_loss_count": 0,
                    "win_count": 0,
                    "failure_count": 0,
                    "latest_failures": [],
                    "worst_failures": [],
                },
            )
            bucket["observation_count"] += 1
            if decision == DECISION_ENTER:
                bucket["enter_count"] += 1
            elif decision == DECISION_WAIT:
                bucket["wait_count"] += 1
            else:
                bucket["skip_count"] += 1
            teacher_alignment = _safe_float(
                teacher_profile.get((code, side, holding_band, strategy_tag), {}).get("alignment_score"),
                0.5,
            )
            teacher_signal = _safe_float(
                teacher_profile.get((code, side, holding_band, strategy_tag), {}).get("effective_signal"),
                0.5,
            )
            similarity_signal = _similarity_signal_for_side(side=side, evidence=similarity_support.get(code))
            if not labeled:
                bucket["teacher_signals"].append(teacher_signal)
                bucket["similarity_signals"].append(similarity_signal)
                continue
            expected_return_f = float(expected_return)
            adverse_move_f = float(adverse_move)
            bucket["labeled_count"] += 1
            bucket["expected_returns"].append(expected_return_f)
            bucket["adverse_moves"].append(adverse_move_f)
            bucket["teacher_alignments"].append(teacher_alignment)
            bucket["teacher_signals"].append(teacher_signal)
            bucket["similarity_signals"].append(similarity_signal)
            if expected_return_f > 0:
                bucket["win_count"] += 1
            if adverse_move_f >= threshold:
                bucket["large_loss_count"] += 1
            is_failure = expected_return_f <= 0.0 or adverse_move_f >= threshold
            if is_failure:
                bucket["failure_count"] += 1
                example = _compact_failure_example(
                    code=code,
                    side=side,
                    holding_band=holding_band,
                    strategy_tag=strategy_tag,
                    as_of_date=as_of_date,
                    expected_return=expected_return_f,
                    adverse_move=adverse_move_f,
                    decision=decision,
                    teacher_alignment=teacher_alignment,
                )
                bucket["latest_failures"].append(example)
                bucket["worst_failures"].append(example)
    conn.execute("DELETE FROM external_state_eval_tag_rollups WHERE publish_id = ?", [publish_id])
    rollup_rows: list[list[Any]] = []
    rollup_payload_rows: list[dict[str, Any]] = []
    for (side, holding_band, strategy_tag), bucket in sorted(accumulators.items()):
        labeled_count = int(bucket["labeled_count"])
        expectancy_mean = (
            sum(bucket["expected_returns"]) / labeled_count
            if labeled_count > 0
            else None
        )
        adverse_mean = (
            sum(bucket["adverse_moves"]) / labeled_count
            if labeled_count > 0
            else None
        )
        large_loss_rate = (
            float(bucket["large_loss_count"]) / labeled_count
            if labeled_count > 0
            else None
        )
        win_rate = (
            float(bucket["win_count"]) / labeled_count
            if labeled_count > 0
            else None
        )
        teacher_alignment_mean = (
            sum(bucket["teacher_alignments"]) / labeled_count
            if labeled_count > 0
            else None
        )
        teacher_signal_mean = (
            sum(bucket["teacher_signals"]) / len(bucket["teacher_signals"])
            if bucket["teacher_signals"]
            else None
        )
        similarity_signal_mean = (
            sum(bucket["similarity_signals"]) / len(bucket["similarity_signals"])
            if bucket["similarity_signals"]
            else None
        )
        latest_failures = sorted(
            bucket["latest_failures"],
            key=lambda item: (str(item["as_of_date"]), str(item["code"])),
            reverse=True,
        )[:10]
        worst_failures = sorted(
            bucket["worst_failures"],
            key=lambda item: (float(item["adverse_move"]), -float(item["expected_return"]), str(item["code"])),
            reverse=True,
        )[:10]
        readiness_hint = _tag_rollup_hint(
            labeled_count=labeled_count,
            expectancy_mean=expectancy_mean,
            large_loss_rate=large_loss_rate,
        )
        summary = {
            "observation_count": int(bucket["observation_count"]),
            "labeled_count": labeled_count,
            "enter_count": int(bucket["enter_count"]),
            "wait_count": int(bucket["wait_count"]),
            "skip_count": int(bucket["skip_count"]),
            "failure_count": int(bucket["failure_count"]),
            "readiness_hint": readiness_hint,
            "teacher_signal_mean": teacher_signal_mean,
            "similarity_signal_mean": similarity_signal_mean,
        }
        rollup_payload_rows.append(
            {
                "side": side,
                "holding_band": holding_band,
                "strategy_tag": strategy_tag,
                "observation_count": int(bucket["observation_count"]),
                "labeled_count": labeled_count,
                "enter_count": int(bucket["enter_count"]),
                "wait_count": int(bucket["wait_count"]),
                "skip_count": int(bucket["skip_count"]),
                "expectancy_mean": expectancy_mean,
                "adverse_mean": adverse_mean,
                "large_loss_rate": large_loss_rate,
                "win_rate": win_rate,
                "teacher_alignment_mean": teacher_alignment_mean,
                "teacher_signal_mean": teacher_signal_mean,
                "similarity_signal_mean": similarity_signal_mean,
                "failure_count": int(bucket["failure_count"]),
                "readiness_hint": readiness_hint,
            }
        )
        rollup_rows.append(
            [
                f"{publish_id}:{side}:{holding_band}:{strategy_tag}",
                publish_id,
                _as_of_date_text(as_of_date),
                side,
                holding_band,
                strategy_tag,
                int(bucket["observation_count"]),
                labeled_count,
                int(bucket["enter_count"]),
                int(bucket["wait_count"]),
                int(bucket["skip_count"]),
                expectancy_mean,
                adverse_mean,
                large_loss_rate,
                win_rate,
                teacher_alignment_mean,
                int(bucket["failure_count"]),
                readiness_hint,
                json.dumps(latest_failures, ensure_ascii=False),
                json.dumps(worst_failures, ensure_ascii=False),
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
                created_at,
            ]
        )
    if rollup_rows:
        conn.executemany(
            """
            INSERT INTO external_state_eval_tag_rollups (
                rollup_id, publish_id, as_of_date, side, holding_band, strategy_tag,
                observation_count, labeled_count, enter_count, wait_count, skip_count,
                expectancy_mean, adverse_mean, large_loss_rate, win_rate, teacher_alignment_mean,
                failure_count, readiness_hint, latest_failure_examples, worst_failure_examples, summary_json, created_at
            ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rollup_rows,
        )
    return rollup_payload_rows


def _persist_daily_summary_snapshots(
    *,
    conn,
    publish_id: str,
    as_of_date: int,
    created_at: datetime,
    summary: dict[str, Any],
) -> None:
    rows = summary.get("rows") or []
    readiness = summary.get("readiness") or {}

    def _pick_top_expectancy(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        valid = [row for row in candidates if row.get("expectancy_mean") is not None and str(row.get("readiness_hint") or "") != "needs_samples"]
        if not valid:
            return None
        return max(
            valid,
            key=lambda row: (
                _safe_float(row.get("expectancy_mean"), -999.0),
                _safe_float(row.get("labeled_count"), 0.0),
                -_safe_float(row.get("large_loss_rate"), 0.0),
            ),
        )

    def _pick_risk_watch(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        risky = [
            row
            for row in candidates
            if str(row.get("readiness_hint") or "") in {"risk_heavy", "negative_expectancy"}
            or _safe_float(row.get("large_loss_rate"), 0.0) >= 0.35
        ]
        if not risky:
            return None
        return max(
            risky,
            key=lambda row: (
                _safe_float(row.get("large_loss_rate"), 0.0),
                -_safe_float(row.get("expectancy_mean"), 0.0),
                _safe_float(row.get("labeled_count"), 0.0),
            ),
        )

    def _pick_sample_watch(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        lacking = [row for row in candidates if str(row.get("readiness_hint") or "") == "needs_samples"]
        if not lacking:
            return None
        return min(
            lacking,
            key=lambda row: (
                _safe_float(row.get("labeled_count"), 0.0),
                _safe_float(row.get("observation_count"), 0.0),
                str(row.get("strategy_tag") or ""),
            ),
        )

    def _serialize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        return {
            "side": str(row.get("side") or ""),
            "holding_band": str(row.get("holding_band") or ""),
            "strategy_tag": str(row.get("strategy_tag") or ""),
            "expectancy_mean": row.get("expectancy_mean"),
            "large_loss_rate": row.get("large_loss_rate"),
            "labeled_count": int(_safe_float(row.get("labeled_count"), 0.0)),
            "readiness_hint": str(row.get("readiness_hint") or ""),
            "teacher_signal_mean": row.get("teacher_signal_mean"),
            "similarity_signal_mean": row.get("similarity_signal_mean"),
        }

    def _build_reason(row: dict[str, Any] | None, *, kind: str) -> str | None:
        if not row:
            return None
        teacher_signal = _safe_float(row.get("teacher_signal_mean"), 0.5)
        similarity_signal = _safe_float(row.get("similarity_signal_mean"), 0.5)
        labeled_count = int(_safe_float(row.get("labeled_count"), 0.0))
        if kind == "top":
            if similarity_signal >= 0.60 and teacher_signal >= 0.60:
                return "teacher整合と類似チャートが両方強い"
            if similarity_signal >= 0.60:
                return "類似チャートの再現性が強い"
            if teacher_signal >= 0.60:
                return "実売買プロファイルと整合が強い"
            return f"期待値優位。ラベル件数 {labeled_count}"
        if kind == "risk":
            loss_rate = _safe_float(row.get("large_loss_rate"), 0.0)
            if loss_rate >= 0.45:
                return "大きな逆行率が高い"
            if similarity_signal <= 0.45:
                return "類似チャートが崩れやすい"
            return "risk heavy 判定が継続"
        if kind == "sample":
            return f"ラベル件数が不足。現在 {labeled_count}"
        return None

    conn.execute("DELETE FROM external_state_eval_daily_summaries WHERE publish_id = ?", [publish_id])
    scope_rows: list[list[Any]] = []
    for side_scope in ("all", "long", "short"):
        scoped_rows = rows if side_scope == "all" else [row for row in rows if str(row.get("side") or "") == side_scope]
        top_strategy = _pick_top_expectancy(scoped_rows)
        top_candle = _pick_top_expectancy([row for row in scoped_rows if _is_candle_research_tag(str(row.get("strategy_tag") or ""))])
        risk_watch = _pick_risk_watch(scoped_rows)
        sample_watch = _pick_sample_watch(scoped_rows)
        payload = {
            "top_strategy": _serialize_row(top_strategy),
            "top_strategy_reason": _build_reason(top_strategy, kind="top"),
            "top_candle": _serialize_row(top_candle),
            "top_candle_reason": _build_reason(top_candle, kind="top"),
            "risk_watch": _serialize_row(risk_watch),
            "risk_watch_reason": _build_reason(risk_watch, kind="risk"),
            "sample_watch": _serialize_row(sample_watch),
            "sample_watch_reason": _build_reason(sample_watch, kind="sample"),
            "promotion": {
                "readiness_pass": bool(readiness.get("readiness_pass")),
                "sample_count": int(_safe_float(readiness.get("sample_count"), 0.0)),
                "expectancy_delta": readiness.get("expectancy_delta"),
            },
        }
        scope_rows.append(
            [
                f"{publish_id}:{side_scope}",
                publish_id,
                _as_of_date_text(as_of_date),
                side_scope,
                None if top_strategy is None else str(top_strategy.get("strategy_tag") or ""),
                None if top_strategy is None else top_strategy.get("expectancy_mean"),
                None if top_candle is None else str(top_candle.get("strategy_tag") or ""),
                None if top_candle is None else top_candle.get("expectancy_mean"),
                None if risk_watch is None else str(risk_watch.get("strategy_tag") or ""),
                None if risk_watch is None else risk_watch.get("large_loss_rate"),
                None if sample_watch is None else str(sample_watch.get("strategy_tag") or ""),
                None if sample_watch is None else int(_safe_float(sample_watch.get("labeled_count"), 0.0)),
                bool(readiness.get("readiness_pass")),
                int(_safe_float(readiness.get("sample_count"), 0.0)),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                created_at,
            ]
        )
    conn.executemany(
        """
        INSERT INTO external_state_eval_daily_summaries (
            summary_id, publish_id, as_of_date, side_scope,
            top_strategy_tag, top_strategy_expectancy,
            top_candle_tag, top_candle_expectancy,
            risk_watch_tag, risk_watch_loss_rate,
            sample_watch_tag, sample_watch_labeled_count,
            promotion_ready, promotion_sample_count,
            summary_json, created_at
        ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        scope_rows,
    )


def _load_trade_teacher_profile(
    *,
    export_db_path: str | None,
    as_of_date: int,
    candidate_keys: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], dict[str, float]]:
    if not candidate_keys:
        return {}
    codes = sorted({str(item["code"]) for item in candidate_keys})
    conn = connect_export_db(export_db_path)
    try:
        placeholders = ", ".join(["?"] * len(codes))
        trade_rows = conn.execute(
            f"""
            WITH enriched AS (
                SELECT
                    b.code,
                    b.trade_date,
                    CAST(b.c AS DOUBLE) AS close_price,
                    CAST(b.v AS DOUBLE) AS volume_value,
                    CAST(i.ma20 AS DOUBLE) AS ma20,
                    CAST(i.atr14 AS DOUBLE) AS atr14,
                    CAST(LAG(b.c, 5) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS DOUBLE) AS close_5d_ago,
                    CAST(LAG(b.c, 20) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS DOUBLE) AS close_20d_ago,
                    CAST(AVG(b.v) OVER (
                        PARTITION BY b.code
                        ORDER BY b.trade_date
                        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                    ) AS DOUBLE) AS avg_volume_prev_5,
                    COALESCE(p.box_state, '') AS box_state
                FROM bars_daily_export b
                LEFT JOIN indicator_daily_export i
                  ON i.code = b.code AND i.trade_date = b.trade_date
                LEFT JOIN pattern_state_export p
                  ON p.code = b.code AND p.trade_date = b.trade_date
                WHERE b.code IN ({placeholders})
                  AND b.trade_date <= CAST(strftime(CAST(? AS DATE), '%Y%m%d') AS INTEGER)
            ),
            entry_events AS (
                SELECT
                    code,
                    CAST(strftime(CAST(event_ts AS TIMESTAMP), '%Y%m%d') AS INTEGER) AS trade_date,
                    event_type,
                    COUNT(*) AS event_count
                FROM trade_event_export
                WHERE CAST(event_ts AS DATE) <= CAST(? AS DATE)
                  AND code IN ({placeholders})
                GROUP BY code, trade_date, event_type
            )
            SELECT
                e.code,
                e.trade_date,
                e.event_type,
                e.event_count,
                x.close_price,
                x.volume_value,
                x.ma20,
                x.atr14,
                x.close_5d_ago,
                x.close_20d_ago,
                x.avg_volume_prev_5,
                x.box_state
            FROM entry_events e
            LEFT JOIN enriched x
              ON x.code = e.code AND x.trade_date = e.trade_date
            """,
            [*codes, _as_of_date_text(as_of_date), _as_of_date_text(as_of_date), *codes],
        ).fetchall()
        position_rows = conn.execute(
            f"""
            WITH latest_positions AS (
                SELECT
                    code,
                    buy_qty,
                    sell_qty,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY snapshot_at DESC) AS rn
                FROM position_snapshot_export
                WHERE CAST(snapshot_at AS DATE) <= CAST(? AS DATE)
                  AND code IN ({placeholders})
            )
            SELECT code, buy_qty, sell_qty
            FROM latest_positions
            WHERE rn = 1
            """,
            [_as_of_date_text(as_of_date), *codes],
        ).fetchall()
    finally:
        conn.close()
    code_profile: dict[str, dict[str, float]] = {
        str(code): {
            "long_entry_count": 0.0,
            "short_entry_count": 0.0,
            "long_alignment": 0.5,
            "short_alignment": 0.5,
            "position_long_bias": 0.0,
            "position_short_bias": 0.0,
        }
        for code in codes
    }
    side_totals: dict[str, float] = {"long": 0.0, "short": 0.0}
    band_totals: dict[tuple[str, str], float] = {}
    tag_totals: dict[tuple[str, str, str], float] = {}
    for (
        code,
        trade_date,
        event_type,
        event_count,
        close_price,
        volume_value,
        ma20,
        atr14,
        close_5d_ago,
        close_20d_ago,
        avg_volume_prev_5,
        box_state,
    ) in trade_rows:
        side = _entry_side_from_event_type(str(event_type or ""))
        if side is None:
            continue
        event_count_f = float(event_count or 0.0)
        code_profile[str(code)][f"{side}_entry_count"] += event_count_f
        side_totals[side] = side_totals.get(side, 0.0) + event_count_f
        if close_price in (None, 0) or close_5d_ago in (None, 0) or close_20d_ago in (None, 0):
            continue
        close_price_f = float(close_price)
        ma20_f = float(ma20) if ma20 not in (None, 0) else close_price_f
        volume_ratio = 1.0 if avg_volume_prev_5 in (None, 0) else (float(volume_value or 0.0) / float(avg_volume_prev_5))
        row = {
            "ret_5_past": (close_price_f / float(close_5d_ago)) - 1.0,
            "ret_20_past": (close_price_f / float(close_20d_ago)) - 1.0,
            "close_vs_ma20": (close_price_f / ma20_f) - 1.0 if ma20_f > 0 else 0.0,
            "volume_ratio": volume_ratio,
            "atr_ratio": (float(atr14) / close_price_f) if atr14 not in (None, 0) and close_price_f > 0 else 0.0,
            "box_state": str(box_state or ""),
        }
        holding_band = _assign_holding_band(side, row)
        band_totals[(side, holding_band)] = band_totals.get((side, holding_band), 0.0) + event_count_f
        for strategy_tag in _derive_strategy_tags(side, row):
            tag_key = (side, holding_band, strategy_tag)
            tag_totals[tag_key] = tag_totals.get(tag_key, 0.0) + event_count_f
    for code, values in code_profile.items():
        total_entries = values["long_entry_count"] + values["short_entry_count"]
        if total_entries > 0:
            values["long_alignment"] = values["long_entry_count"] / total_entries
            values["short_alignment"] = values["short_entry_count"] / total_entries
    for code, buy_qty, sell_qty in position_rows:
        buy_qty_f = max(0.0, _safe_float(buy_qty))
        sell_qty_f = max(0.0, _safe_float(sell_qty))
        total = buy_qty_f + sell_qty_f
        if total <= 0:
            continue
        code_profile[str(code)]["position_long_bias"] = buy_qty_f / total
        code_profile[str(code)]["position_short_bias"] = sell_qty_f / total
    profile: dict[tuple[str, str, str, str], dict[str, float]] = {}
    for item in candidate_keys:
        code = str(item["code"])
        side = str(item["side"])
        holding_band = str(item["holding_band"])
        strategy_tag = str(item["strategy_tag"])
        base = code_profile.get(code, {})
        if side == "long":
            alignment_score = _safe_float(base.get("long_alignment"), 0.5)
            position_bias = _safe_float(base.get("position_long_bias"), 0.0)
            trade_count = int(_safe_float(base.get("long_entry_count"), 0.0))
        else:
            alignment_score = _safe_float(base.get("short_alignment"), 0.5)
            position_bias = _safe_float(base.get("position_short_bias"), 0.0)
            trade_count = int(_safe_float(base.get("short_entry_count"), 0.0))
        side_total = max(0.0, side_totals.get(side, 0.0))
        band_trade_count = max(0.0, band_totals.get((side, holding_band), 0.0))
        tag_trade_count = max(0.0, tag_totals.get((side, holding_band, strategy_tag), 0.0))
        band_alignment = 0.5 if side_total <= 0 else (band_trade_count / side_total)
        tag_alignment = 0.5 if band_trade_count <= 0 else (tag_trade_count / band_trade_count)
        confidence_weight = _teacher_confidence_weight(trade_count, band_trade_count, tag_trade_count)
        profile[(code, side, holding_band, strategy_tag)] = {
            "trade_count": float(trade_count),
            "alignment_score": alignment_score,
            "position_bias": position_bias,
            "band_trade_count": float(band_trade_count),
            "tag_trade_count": float(tag_trade_count),
            "band_alignment": float(_clamp01(band_alignment)),
            "tag_alignment": float(_clamp01(tag_alignment)),
            "confidence_weight": float(confidence_weight),
            "effective_signal": float(
                _teacher_effective_signal(
                    {
                        "alignment_score": alignment_score,
                        "position_bias": position_bias,
                        "band_alignment": _clamp01(band_alignment),
                        "tag_alignment": _clamp01(tag_alignment),
                        "confidence_weight": confidence_weight,
                    }
                )
            ),
        }
    return profile


def _persist_trade_teacher_profile(
    *,
    ops_db_path: str | None,
    as_of_date: int,
    profile: dict[tuple[str, str, str, str], dict[str, float]],
) -> None:
    if not ops_db_path:
        return
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute("DELETE FROM external_trade_teacher_profiles WHERE as_of_date = CAST(? AS DATE)", [_as_of_date_text(as_of_date)])
        created_at = _utcnow()
        rows: list[list[Any]] = []
        for (code, side, holding_band, strategy_tag), values in sorted(profile.items()):
            rows.append(
                [
                    f"{_as_of_date_text(as_of_date)}:{code}:{side}:{holding_band}:{strategy_tag}",
                    _as_of_date_text(as_of_date),
                    code,
                    side,
                    holding_band,
                    json.dumps([strategy_tag], ensure_ascii=False),
                    int(values["trade_count"]),
                    float(values["alignment_score"]),
                    float(values["position_bias"]),
                    json.dumps(values, ensure_ascii=False, sort_keys=True),
                    created_at,
                ]
            )
        if rows:
            conn.executemany(
                """
                INSERT INTO external_trade_teacher_profiles (
                    profile_id, as_of_date, code, side, holding_band, strategy_tags, trade_count, alignment_score, position_bias, summary_json, created_at
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def _champion_long_score(row: dict[str, Any], teacher: dict[str, float]) -> tuple[float, list[str]]:
    trend_signal = _normalize_signal(_safe_float(row["ranking_score_long"]), -5.0, 12.0)
    momentum_signal = _normalize_signal(_safe_float(row["ret_20_past"]), -0.10, 0.25)
    ma_signal = _normalize_signal(_safe_float(row["close_vs_ma20"]), -0.08, 0.12)
    teacher_signal = _safe_float(teacher.get("effective_signal"), 0.5)
    similarity_signal = _safe_float(teacher.get("similarity_signal"), 0.5)
    tag_prior_signal = _safe_float(teacher.get("tag_prior_signal"), 0.5)
    combo_prior_signal = _safe_float(teacher.get("combo_prior_signal"), 0.5)
    risk_penalty = _normalize_signal(_safe_float(row["atr_ratio"]), 0.02, 0.08)
    score = (
        (0.23 * trend_signal)
        + (0.20 * momentum_signal)
        + (0.13 * ma_signal)
        + (0.18 * teacher_signal)
        + (0.10 * similarity_signal)
        + (0.10 * tag_prior_signal)
        + (0.08 * combo_prior_signal)
        - (0.18 * risk_penalty)
    )
    return score, ["BUY_TREND", "MA20_SUPPORT", "SIMILARITY_SUPPORT", "TAG_PRIOR_SUPPORT", "COMBO_PRIOR_SUPPORT"]


def _challenger_long_score(row: dict[str, Any], teacher: dict[str, float]) -> tuple[float, list[str]]:
    trend_signal = _normalize_signal(_safe_float(row["ranking_score_long"]), -3.0, 10.0)
    momentum_signal = _normalize_signal(_safe_float(row["ret_20_past"]), -0.06, 0.20)
    ma_signal = _normalize_signal(_safe_float(row["close_vs_ma20"]), -0.05, 0.10)
    risk_penalty = _normalize_signal(_safe_float(row["atr_ratio"]), 0.02, 0.08)
    teacher_signal = _safe_float(teacher.get("effective_signal"), 0.5)
    similarity_signal = _safe_float(teacher.get("similarity_signal"), 0.5)
    tag_prior_signal = _safe_float(teacher.get("tag_prior_signal"), 0.5)
    combo_prior_signal = _safe_float(teacher.get("combo_prior_signal"), 0.5)
    score = (
        (0.20 * trend_signal)
        + (0.18 * momentum_signal)
        + (0.12 * ma_signal)
        + (0.22 * teacher_signal)
        + (0.14 * similarity_signal)
        + (0.10 * tag_prior_signal)
        + (0.10 * combo_prior_signal)
        - (0.24 * risk_penalty)
    )
    return score, ["BUY_TREND_STRICT", "FALSE_BREAKOUT_FILTER", "SIMILARITY_STRICT", "TAG_PRIOR_STRICT", "COMBO_PRIOR_STRICT"]


def _champion_short_score(row: dict[str, Any], teacher: dict[str, float]) -> tuple[float, list[str]]:
    rebound_signal = _normalize_signal(_safe_float(row["ret_5_past"]), -0.03, 0.12)
    extension_signal = _normalize_signal(_safe_float(row["close_vs_ma20"]), -0.02, 0.10)
    volume_signal = _normalize_signal(_safe_float(row["volume_ratio"]), 0.8, 2.2)
    teacher_signal = _safe_float(teacher.get("effective_signal"), 0.5)
    similarity_signal = _safe_float(teacher.get("similarity_signal"), 0.5)
    tag_prior_signal = _safe_float(teacher.get("tag_prior_signal"), 0.5)
    combo_prior_signal = _safe_float(teacher.get("combo_prior_signal"), 0.5)
    squeeze_penalty = _normalize_signal(_safe_float(row["close_vs_ma20"]), 0.02, 0.10)
    score = (
        (0.20 * rebound_signal)
        + (0.20 * extension_signal)
        + (0.08 * volume_signal)
        + (0.20 * teacher_signal)
        + (0.12 * similarity_signal)
        + (0.12 * tag_prior_signal)
        + (0.10 * combo_prior_signal)
        - (0.18 * squeeze_penalty)
    )
    return score, ["SELL_COUNTERTREND", "EXTENDED_ABOVE_MA20", "SIMILARITY_SUPPORT", "TAG_PRIOR_SUPPORT", "COMBO_PRIOR_SUPPORT"]


def _challenger_short_score(row: dict[str, Any], teacher: dict[str, float]) -> tuple[float, list[str]]:
    rebound_signal = _normalize_signal(_safe_float(row["ret_5_past"]), -0.01, 0.10)
    extension_signal = _normalize_signal(_safe_float(row["close_vs_ma20"]), 0.0, 0.08)
    volume_signal = _normalize_signal(_safe_float(row["volume_ratio"]), 1.0, 2.0)
    risk_penalty = _normalize_signal(_safe_float(row["atr_ratio"]), 0.02, 0.08)
    teacher_signal = _safe_float(teacher.get("effective_signal"), 0.5)
    similarity_signal = _safe_float(teacher.get("similarity_signal"), 0.5)
    tag_prior_signal = _safe_float(teacher.get("tag_prior_signal"), 0.5)
    combo_prior_signal = _safe_float(teacher.get("combo_prior_signal"), 0.5)
    squeeze_penalty = _normalize_signal(_safe_float(row["close_vs_ma20"]), 0.02, 0.10)
    score = (
        (0.17 * rebound_signal)
        + (0.18 * extension_signal)
        + (0.06 * volume_signal)
        + (0.22 * teacher_signal)
        + (0.14 * similarity_signal)
        + (0.11 * tag_prior_signal)
        + (0.10 * combo_prior_signal)
        - (0.18 * risk_penalty)
        - (0.18 * squeeze_penalty)
    )
    return score, ["SELL_COUNTERTREND_STRICT", "SQUEEZE_FILTER", "SIMILARITY_STRICT", "TAG_PRIOR_STRICT", "COMBO_PRIOR_STRICT"]


def _decision_from_score(score: float, *, strict: bool) -> str:
    if strict:
        if score >= 0.70:
            return DECISION_ENTER
        if score >= 0.52:
            return DECISION_WAIT
        return DECISION_SKIP
    if score >= 0.64:
        return DECISION_ENTER
    if score >= 0.45:
        return DECISION_WAIT
    return DECISION_SKIP


def build_state_eval_rows(
    *,
    scored: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    publish_id: str,
    as_of_date: int,
    freshness_state: str,
    export_db_path: str | None,
    label_db_path: str | None = None,
    similarity_db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    scored_by_code = {str(row["code"]): row for row in scored}
    candidate_keys: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        scored_row = scored_by_code.get(str(candidate["code"]))
        if scored_row is None:
            continue
        side = str(candidate["side"])
        holding_band = _assign_holding_band(side, scored_row)
        for strategy_tag in _derive_strategy_tags(side, scored_row):
            candidate_keys.append(
                {
                    "code": str(candidate["code"]),
                    "side": side,
                    "holding_band": holding_band,
                    "strategy_tag": strategy_tag,
                }
            )
    teacher_profile = _load_trade_teacher_profile(export_db_path=export_db_path, as_of_date=as_of_date, candidate_keys=candidate_keys)
    _persist_trade_teacher_profile(ops_db_path=ops_db_path, as_of_date=as_of_date, profile=teacher_profile)
    tag_prior_support = _load_historical_tag_priors(
        ops_db_path=ops_db_path,
        as_of_date=as_of_date,
        candidate_keys=candidate_keys,
    )
    similarity_support = _load_similarity_support(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        similarity_db_path=similarity_db_path,
        as_of_date=as_of_date,
        codes=sorted({str(candidate["code"]) for candidate in candidate_rows}),
    )
    champion_rows: list[dict[str, Any]] = []
    challenger_rows: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        code = str(candidate["code"])
        side = str(candidate["side"])
        scored_row = scored_by_code.get(code)
        if scored_row is None:
            continue
        holding_band = _assign_holding_band(side, scored_row)
        strategy_tags = _derive_strategy_tags(side, scored_row)
        teacher_scores = [teacher_profile.get((code, side, holding_band, tag), {}) for tag in strategy_tags]
        teacher = _summarize_teacher_scores(teacher_scores)
        teacher["similarity_signal"] = _similarity_signal_for_side(side=side, evidence=similarity_support.get(code))
        teacher.update(
            _summarize_tag_prior_scores(
                strategy_tags=strategy_tags,
                side=side,
                holding_band=holding_band,
                tag_priors=tag_prior_support,
            )
        )
        if side == "long":
            champion_score, champion_reasons = _champion_long_score(scored_row, teacher)
            challenger_score, challenger_reasons = _challenger_long_score(scored_row, teacher)
        else:
            champion_score, champion_reasons = _champion_short_score(scored_row, teacher)
            challenger_score, challenger_reasons = _challenger_short_score(scored_row, teacher)
        reason_text_top3 = _reason_texts(
            side=side,
            row=scored_row,
            tags=strategy_tags,
            similarity_evidence=similarity_support.get(code),
            tag_prior_summary=teacher,
        )
        champion_decision = _decision_from_score(champion_score, strict=False)
        challenger_decision = _decision_from_score(challenger_score, strict=True)
        champion_rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": _as_of_date_text(as_of_date),
                "code": code,
                "state_action": champion_decision,
                "side": side,
                "holding_band": holding_band,
                "strategy_tags": json.dumps(strategy_tags, ensure_ascii=False),
                "decision_3way": champion_decision,
                "confidence": float(round(champion_score, 6)),
                "reason_codes": _build_reason_codes(*champion_reasons),
                "reason_text_top3": _build_reason_text_top3(*reason_text_top3),
                "freshness_state": freshness_state,
            }
        )
        challenger_rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": _as_of_date_text(as_of_date),
                "code": code,
                "side": side,
                "holding_band": holding_band,
                "strategy_tags": json.dumps(strategy_tags, ensure_ascii=False),
                "decision_3way": challenger_decision,
                "confidence": float(round(challenger_score, 6)),
                "reason_codes": _build_reason_codes(*challenger_reasons),
                "reason_text_top3": _build_reason_text_top3(*reason_text_top3),
            }
        )
    return {
        "rows": champion_rows,
        "challenger_rows": challenger_rows,
        "teacher_profile": teacher_profile,
        "similarity_support": similarity_support,
        "tag_prior_support": tag_prior_support,
        "baseline_version": BASELINE_VERSION,
        "challenger_version": CHALLENGER_VERSION,
    }


def _load_state_eval_labels(*, label_db_path: str | None, as_of_date: int, codes: list[str]) -> dict[tuple[str, str], dict[str, float | None]]:
    if not codes:
        return {}
    conn = connect_label_db(label_db_path)
    try:
        placeholders = ", ".join(["?"] * len(codes))
        rows = conn.execute(
            f"""
            SELECT code, ret_h, mfe_h, mae_h
            FROM label_daily_h20
            WHERE as_of_date = ? AND code IN ({placeholders})
            """,
            [as_of_date, *codes],
        ).fetchall()
    finally:
        conn.close()
    labels: dict[tuple[str, str], dict[str, float | None]] = {}
    for code, ret_h, mfe_h, mae_h in rows:
        labels[(str(code), "long")] = {
            "expected_return": None if ret_h is None else float(ret_h),
            "adverse_move": None if mae_h is None else abs(min(float(mae_h), 0.0)),
        }
        labels[(str(code), "short")] = {
            "expected_return": None if ret_h is None else -float(ret_h),
            "adverse_move": None if mfe_h is None else max(float(mfe_h), 0.0),
        }
    return labels


def persist_state_eval_shadow(
    *,
    ops_db_path: str | None,
    publish_id: str,
    as_of_date: int,
    champion_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
    teacher_profile: dict[tuple[str, str, str, str], dict[str, float]],
    similarity_support: dict[str, dict[str, float]],
    tag_prior_support: dict[tuple[str, str, str], dict[str, float | str]],
    label_db_path: str | None,
) -> dict[str, Any]:
    if not ops_db_path:
        return {"saved": False, "reason": "ops_db_path_missing"}
    labels = _load_state_eval_labels(
        label_db_path=label_db_path,
        as_of_date=as_of_date,
        codes=sorted({str(row["code"]) for row in champion_rows}),
    )
    champion_index = {(str(row["code"]), str(row["side"])): row for row in champion_rows}
    challenger_index = {(str(row["code"]), str(row["side"])): row for row in challenger_rows}
    created_at = _utcnow()
    shadow_rows: list[list[Any]] = []
    champion_selected: list[dict[str, float]] = []
    challenger_selected: list[dict[str, float]] = []
    for key, champion in sorted(champion_index.items()):
        challenger = challenger_index.get(key)
        if challenger is None:
            continue
        code, side = key
        label = labels.get(key, {})
        expected_return = label.get("expected_return")
        adverse_move = label.get("adverse_move")
        label_available = expected_return is not None and adverse_move is not None
        holding_band = str(champion.get("holding_band") or "")
        strategy_tags = json.loads(str(champion.get("strategy_tags") or "[]"))
        teacher_rows = [teacher_profile.get((code, side, holding_band, str(tag)), {}) for tag in strategy_tags]
        teacher_alignment = _safe_float(_summarize_teacher_scores(teacher_rows).get("effective_signal"), 0.5)
        similarity_signal = _similarity_signal_for_side(side=side, evidence=similarity_support.get(code))
        tag_prior_summary = _summarize_tag_prior_scores(
            strategy_tags=[str(tag) for tag in strategy_tags],
            side=side,
            holding_band=holding_band,
            tag_priors=tag_prior_support,
        )
        tag_prior_signal = _safe_float(tag_prior_summary.get("tag_prior_signal"), 0.5)
        combo_prior_signal = _safe_float(tag_prior_summary.get("combo_prior_signal"), 0.5)
        shadow_rows.append(
            [
                f"{publish_id}:{code}:{side}:{holding_band}",
                publish_id,
                _as_of_date_text(as_of_date),
                code,
                side,
                holding_band,
                json.dumps(strategy_tags, ensure_ascii=False),
                champion["decision_3way"],
                challenger["decision_3way"],
                float(champion["confidence"]),
                float(challenger["confidence"]),
                expected_return,
                adverse_move,
                teacher_alignment,
                bool(label_available),
                json.dumps(
                    {
                        "champion_reason_codes": champion.get("reason_codes"),
                        "challenger_reason_codes": challenger.get("reason_codes"),
                        "reason_text_top3": champion.get("reason_text_top3"),
                        "similarity_support": similarity_support.get(code) or {},
                        "tag_prior_summary": tag_prior_summary,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                created_at,
            ]
        )
        if label_available and champion["decision_3way"] == DECISION_ENTER:
            champion_selected.append(
                {
                    "expected_return": float(expected_return),
                    "adverse_move": float(adverse_move),
                    "alignment": teacher_alignment,
                    "similarity": similarity_signal,
                    "tag_prior": tag_prior_signal,
                    "combo_prior": combo_prior_signal,
                }
            )
        if label_available and challenger["decision_3way"] == DECISION_ENTER:
            challenger_selected.append(
                {
                    "expected_return": float(expected_return),
                    "adverse_move": float(adverse_move),
                    "alignment": teacher_alignment,
                    "similarity": similarity_signal,
                    "tag_prior": tag_prior_signal,
                    "combo_prior": combo_prior_signal,
                }
            )
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute("DELETE FROM external_state_eval_shadow_runs WHERE publish_id = ?", [publish_id])
        if shadow_rows:
            conn.executemany(
                """
                INSERT INTO external_state_eval_shadow_runs (
                    shadow_id, publish_id, as_of_date, code, side, holding_band, strategy_tags, champion_decision, challenger_decision,
                    champion_confidence, challenger_confidence, expected_return, adverse_move, teacher_alignment,
                    label_available, summary_json, created_at
                ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                shadow_rows,
            )
        champion_expectancy = sum(row["expected_return"] for row in champion_selected) / len(champion_selected) if champion_selected else None
        challenger_expectancy = sum(row["expected_return"] for row in challenger_selected) / len(challenger_selected) if challenger_selected else None
        champion_adverse = sum(row["adverse_move"] for row in champion_selected) / len(champion_selected) if champion_selected else None
        challenger_adverse = sum(row["adverse_move"] for row in challenger_selected) / len(challenger_selected) if challenger_selected else None
        champion_adverse_rate = (
            sum(1.0 for row in champion_selected if row["adverse_move"] >= LONG_ADVERSE_MOVE_THRESHOLD) / len(champion_selected)
            if champion_selected
            else None
        )
        challenger_adverse_rate = (
            sum(1.0 for row in challenger_selected if row["adverse_move"] >= LONG_ADVERSE_MOVE_THRESHOLD) / len(challenger_selected)
            if challenger_selected
            else None
        )
        champion_alignment = sum(row["alignment"] for row in champion_selected) / len(champion_selected) if champion_selected else None
        challenger_alignment = sum(row["alignment"] for row in challenger_selected) / len(challenger_selected) if challenger_selected else None
        champion_similarity = sum(row["similarity"] for row in champion_selected) / len(champion_selected) if champion_selected else None
        challenger_similarity = sum(row["similarity"] for row in challenger_selected) / len(challenger_selected) if challenger_selected else None
        champion_tag_prior = sum(row["tag_prior"] for row in champion_selected) / len(champion_selected) if champion_selected else None
        challenger_tag_prior = sum(row["tag_prior"] for row in challenger_selected) / len(challenger_selected) if challenger_selected else None
        champion_combo_prior = sum(row["combo_prior"] for row in champion_selected) / len(champion_selected) if champion_selected else None
        challenger_combo_prior = sum(row["combo_prior"] for row in challenger_selected) / len(challenger_selected) if challenger_selected else None
        improved_expectancy = champion_expectancy is not None and challenger_expectancy is not None and challenger_expectancy >= champion_expectancy
        mae_non_worse = champion_adverse is not None and challenger_adverse is not None and challenger_adverse <= champion_adverse
        adverse_move_non_worse = champion_adverse_rate is not None and challenger_adverse_rate is not None and challenger_adverse_rate <= champion_adverse_rate
        sample_count = min(len(champion_selected), len(challenger_selected))
        stable_window = sample_count >= PROMOTION_MIN_SAMPLE_COUNT
        alignment_ok = champion_alignment is not None and challenger_alignment is not None and challenger_alignment >= champion_alignment
        similarity_ok = (
            True
            if champion_similarity is None or challenger_similarity is None
            else challenger_similarity >= champion_similarity
        )
        tag_prior_ok = (
            True
            if champion_tag_prior is None or challenger_tag_prior is None
            else challenger_tag_prior >= champion_tag_prior
        )
        combo_prior_ok = (
            True
            if champion_combo_prior is None or challenger_combo_prior is None
            else challenger_combo_prior >= champion_combo_prior
        )
        expectancy_delta = (
            None
            if champion_expectancy is None or challenger_expectancy is None
            else float(challenger_expectancy - champion_expectancy)
        )
        reason_codes: list[str] = []
        if not improved_expectancy:
            reason_codes.append("expectancy_not_improved")
        if not mae_non_worse:
            reason_codes.append("adverse_move_regressed")
        if not adverse_move_non_worse:
            reason_codes.append("large_loss_rate_regressed")
        if not stable_window:
            reason_codes.append("sample_count_below_50")
        if not alignment_ok:
            reason_codes.append("teacher_alignment_not_improved")
        if not similarity_ok:
            reason_codes.append("similarity_support_not_improved")
        if not tag_prior_ok:
            reason_codes.append("tag_prior_not_improved")
        if not combo_prior_ok:
            reason_codes.append("combo_prior_not_improved")
        readiness_pass = bool(
            improved_expectancy
            and mae_non_worse
            and adverse_move_non_worse
            and stable_window
            and alignment_ok
            and similarity_ok
            and tag_prior_ok
            and combo_prior_ok
        )
        summary = {
            "champion_version": BASELINE_VERSION,
            "challenger_version": CHALLENGER_VERSION,
            "champion_selected": len(champion_selected),
            "challenger_selected": len(challenger_selected),
            "champion_expectancy": champion_expectancy,
            "challenger_expectancy": challenger_expectancy,
            "champion_adverse": champion_adverse,
            "challenger_adverse": challenger_adverse,
            "champion_adverse_rate": champion_adverse_rate,
            "challenger_adverse_rate": challenger_adverse_rate,
            "champion_alignment": champion_alignment,
            "challenger_alignment": challenger_alignment,
            "champion_similarity": champion_similarity,
            "challenger_similarity": challenger_similarity,
            "champion_tag_prior": champion_tag_prior,
            "challenger_tag_prior": challenger_tag_prior,
            "champion_combo_prior": champion_combo_prior,
            "challenger_combo_prior": challenger_combo_prior,
            "sample_count": sample_count,
            "expectancy_delta": expectancy_delta,
        }
        conn.execute("DELETE FROM external_state_eval_readiness WHERE publish_id = ?", [publish_id])
        conn.execute(
            """
            INSERT INTO external_state_eval_readiness (
                readiness_id, publish_id, as_of_date, champion_version, challenger_version,
                sample_count, expectancy_delta, improved_expectancy, mae_non_worse, adverse_move_non_worse, stable_window, alignment_ok,
                readiness_pass, reason_codes, summary_json, created_at
            ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"{publish_id}:readiness",
                publish_id,
                _as_of_date_text(as_of_date),
                BASELINE_VERSION,
                CHALLENGER_VERSION,
                int(sample_count),
                expectancy_delta,
                improved_expectancy,
                mae_non_worse,
                adverse_move_non_worse,
                stable_window,
                alignment_ok,
                readiness_pass,
                json.dumps(reason_codes, ensure_ascii=False),
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
                created_at,
            ],
        )
        conn.execute("DELETE FROM external_state_eval_failure_samples WHERE publish_id = ?", [publish_id])
        latest_failures = sorted(shadow_rows, key=lambda item: (str(item[2]), str(item[3])), reverse=True)[:10]
        worst_failures = sorted(
            [item for item in shadow_rows if item[12] is not None],
            key=lambda item: (float(item[12]), -float(item[11] or 0.0), str(item[3])),
            reverse=True,
        )[:10]
        failure_rows: list[list[Any]] = []
        for bucket_type, rows in (("latest_bucket", latest_failures), ("worst_bucket", worst_failures)):
            for index, item in enumerate(rows, start=1):
                failure_rows.append(
                    [
                        f"{publish_id}:{bucket_type}:{index}",
                        publish_id,
                        str(item[2]),
                        str(item[3]),
                        str(item[4]),
                        str(item[5]),
                        str(item[6]),
                        bucket_type,
                        item[11],
                        item[12],
                        json.dumps(reason_codes or ["failure_sample"], ensure_ascii=False),
                        json.dumps(
                            {
                                "champion_decision": item[7],
                                "challenger_decision": item[8],
                                "teacher_alignment": item[13],
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        created_at,
                    ]
                )
        if failure_rows:
            conn.executemany(
                """
                INSERT INTO external_state_eval_failure_samples (
                    sample_id, publish_id, as_of_date, code, side, holding_band, strategy_tags,
                    bucket_type, expected_return, adverse_move, reason_codes, summary_json, created_at
                ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                failure_rows,
            )
        tag_rollup_rows = _persist_tag_validation_rollups(
            conn=conn,
            publish_id=publish_id,
            as_of_date=as_of_date,
            champion_rows=champion_rows,
            labels=labels,
            teacher_profile=teacher_profile,
            similarity_support=similarity_support,
            created_at=created_at,
        )
        _persist_daily_summary_snapshots(
            conn=conn,
            publish_id=publish_id,
            as_of_date=as_of_date,
            created_at=created_at,
            summary={
                "rows": tag_rollup_rows,
                "readiness": {
                    "readiness_pass": readiness_pass,
                    "sample_count": sample_count,
                    "expectancy_delta": expectancy_delta,
                },
            },
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return {
        "saved": True,
        "source": "external_analysis_shadow",
        "publish_id": publish_id,
        "as_of_date": _as_of_date_text(as_of_date),
        "champion_version": BASELINE_VERSION,
        "challenger_version": CHALLENGER_VERSION,
        "sample_count": sample_count,
        "expectancy_delta": expectancy_delta,
        "improved_expectancy": improved_expectancy,
        "mae_non_worse": mae_non_worse,
        "adverse_move_non_worse": adverse_move_non_worse,
        "stable_window": stable_window,
        "alignment_ok": alignment_ok,
        "readiness_pass": readiness_pass,
        "reason_codes": reason_codes,
        "created_at": created_at,
        "summary": summary,
        "champion_selected": len(champion_selected),
        "challenger_selected": len(challenger_selected),
    }
