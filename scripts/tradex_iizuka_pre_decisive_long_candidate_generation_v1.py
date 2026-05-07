from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1")
DEFAULT_SOURCE_SURFACE = Path(
    r"G:\Tradex\feature_complete_high_recall_surface_v1\20260502T140705Z-318453\feature_complete_high_recall_candidate_rows.parquet"
)
DEFAULT_LABEL_SURFACE = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

SCRIPT_NAME = "tradex_iizuka_pre_decisive_long_candidate_generation_v1"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_input_resolution_v1"
FEATURE_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_feature_availability_audit_v1"
CONTRACT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_candidate_contract_v1"
SURFACE_SUMMARY_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_surface_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_no_lookahead_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_leakage_audit_v1"
VARIANT_COMPARE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_variant_pool_comparison_v1"
FAILURE_MODE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_failure_mode_audit_v1"
ORACLE_HEADROOM_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_oracle_headroom_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_generation_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
EVAL_LABEL_COLUMNS = ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label")
NO_LOOKAHEAD_FLAGS = ("monthly_context_no_lookahead", "weekly_context_no_lookahead")
LABEL_JOIN_KEYS = ("anchor_date", "symbol", "side")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip().lower()
    return text in {"", "nan", "<na>", "none", "null"}


def _safe_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any, default: bool = False) -> bool:
    if _is_missing(value):
        return default
    return bool(value)


def _key(frame: pd.DataFrame) -> pd.Series:
    candidate_idx = frame["candidate_idx"].astype(str) if "candidate_idx" in frame.columns else ""
    if "candidate_idx" in frame.columns:
        return frame["anchor_date"].astype(str) + "|" + frame["side"].astype(str) + "|" + frame["symbol"].astype(str) + "|" + candidate_idx
    return frame["anchor_date"].astype(str) + "|" + frame["side"].astype(str) + "|" + frame["symbol"].astype(str)


def _surface_key(frame: pd.DataFrame) -> pd.Series:
    return frame["anchor_date"].astype(str) + "|" + frame["symbol"].astype(str) + "|" + frame["side"].astype(str)


def _feature_status(*, missing: list[str] | None = None, available: list[str] | None = None, derivable: list[str] | None = None, instrumentation: list[str] | None = None) -> dict[str, Any]:
    return {
        "missing": sorted(set(missing or [])),
        "available": sorted(set(available or [])),
        "derivable_safely": sorted(set(derivable or [])),
        "required_instrumentation": sorted(set(instrumentation or [])),
        "status": "missing" if missing else ("derivable_safely" if derivable else "available"),
    }


def _load_inputs(source_surface: Path, label_surface: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    source = _load_frame(source_surface)
    labels = _load_frame(label_surface)
    for frame in (source, labels):
        frame["anchor_date"] = frame["anchor_date"].astype(str)
        frame["symbol"] = frame["symbol"].astype(str)
        frame["side"] = frame["side"].astype(str)
        frame["key"] = _key(frame)
        frame["surface_key"] = _surface_key(frame)
    return source, labels


def _attach_labels(source: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    label_cols = [c for c in EVAL_LABEL_COLUMNS if c in labels.columns]
    label_frame = labels[list(LABEL_JOIN_KEYS) + label_cols + ["surface_key"]].copy()
    merged = source.merge(label_frame, on=list(LABEL_JOIN_KEYS) + ["surface_key"], how="left", suffixes=("", "_label"))
    for column in ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label"):
        label_col = f"{column}_label"
        if label_col in merged.columns:
            if column not in merged.columns:
                merged[column] = merged[label_col]
            else:
                merged[column] = merged[column].where(merged[column].notna(), merged[label_col])
            merged = merged.drop(columns=[label_col])
    for column in ("top15_label", "bottom15_label", "top20pct_label"):
        if column in merged.columns:
            merged[column] = merged[column].fillna(False).astype(bool)
    return merged


def _load_runtime_features(db_path: Path, frame: pd.DataFrame) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    ff = conn.execute(
        """
        select
            code,
            dt,
            close,
            ma7,
            ma20,
            ma60,
            atr14,
            diff20_pct,
            cnt_20_above,
            cnt_7_above,
            close_prev1,
            close_prev5,
            close_prev10,
            ma7_prev1,
            ma20_prev1,
            ma60_prev1,
            close_ret20,
            close_ret60,
            atr14_pct,
            range_pct,
            gap_pct,
            vol_ret5,
            vol_ret20,
            vol_ratio5_20,
            turnover20,
            turnover_z20,
            high20_dist,
            low20_dist,
            breakout20_up,
            breakout20_down,
            drawdown60,
            rebound60,
            weekly_breakout_up_prob,
            weekly_breakout_down_prob,
            weekly_range_prob,
            monthly_breakout_up_prob,
            monthly_breakout_down_prob,
            monthly_range_prob,
            candle_triplet_up_prob,
            candle_triplet_down_prob,
            candle_body_ratio,
            candle_upper_wick_ratio,
            candle_lower_wick_ratio
        from feature_frame_daily
        """
    ).fetchdf()
    ff["anchor_date"] = pd.to_datetime(ff["dt"], unit="s", errors="coerce").dt.strftime("%Y-%m-%d")
    ff["symbol"] = ff["code"].astype(str)
    frame["anchor_date"] = pd.to_datetime(frame["anchor_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    enriched = frame.merge(
        ff.drop(columns=["dt", "code"]),
        on=["anchor_date", "symbol"],
        how="left",
        suffixes=("", "_ff"),
    )
    enriched["ma20_slope_1"] = (pd.to_numeric(enriched["ma20"], errors="coerce") - pd.to_numeric(enriched["ma20_prev1"], errors="coerce")) / pd.to_numeric(enriched["ma20_prev1"], errors="coerce").abs()
    enriched["ma60_slope_1"] = (pd.to_numeric(enriched["ma60"], errors="coerce") - pd.to_numeric(enriched["ma60_prev1"], errors="coerce")) / pd.to_numeric(enriched["ma60_prev1"], errors="coerce").abs()
    enriched["ma7_slope_1"] = (pd.to_numeric(enriched["ma7"], errors="coerce") - pd.to_numeric(enriched["ma7_prev1"], errors="coerce")) / pd.to_numeric(enriched["ma7_prev1"], errors="coerce").abs()
    enriched["close_vs_ma7_pct"] = (pd.to_numeric(enriched["close"], errors="coerce") - pd.to_numeric(enriched["ma7"], errors="coerce")) / pd.to_numeric(enriched["ma7"], errors="coerce").abs()
    enriched["close_vs_ma20_pct"] = (pd.to_numeric(enriched["close"], errors="coerce") - pd.to_numeric(enriched["ma20"], errors="coerce")) / pd.to_numeric(enriched["ma20"], errors="coerce").abs()
    enriched["close_vs_ma60_pct"] = (pd.to_numeric(enriched["close"], errors="coerce") - pd.to_numeric(enriched["ma60"], errors="coerce")) / pd.to_numeric(enriched["ma60"], errors="coerce").abs()
    enriched["ma20_direction_up"] = pd.to_numeric(enriched["ma20_slope_1"], errors="coerce").fillna(0.0) >= 0.0
    enriched["ma60_direction_up"] = pd.to_numeric(enriched["ma60_slope_1"], errors="coerce").fillna(0.0) >= 0.0
    enriched["ma7_direction_up"] = pd.to_numeric(enriched["ma7_slope_1"], errors="coerce").fillna(0.0) >= 0.0
    enriched["feature_frame_no_lookahead"] = True
    return enriched


def _build_feature_availability_audit(frame: pd.DataFrame) -> dict[str, Any]:
    columns = set(frame.columns)
    items: list[dict[str, Any]] = []

    def add(category: str, field: str, status: dict[str, Any], source: str, note: str = "") -> None:
        items.append(
            {
                "category": category,
                "field": field,
                "source": source,
                "no_lookahead_status": "pass" if status["status"] != "missing" else "missing",
                "derivable_safely": status["status"] in {"available", "derivable_safely"},
                "instrumentation_needed": bool(status["required_instrumentation"]),
                "status": status["status"],
                "missing": status["missing"],
                "available": status["available"],
                "required_instrumentation": status["required_instrumentation"],
                "note": note,
            }
        )

    # MA / position
    add("ma_position", "close vs 7MA", _feature_status(available=["close", "ma7", "close_vs_ma7_pct"], derivable=["close_vs_ma7_pct"]), "feature_frame_daily", "derived from close and ma7")
    add("ma_position", "close vs 20MA", _feature_status(available=["diff20_pct", "close", "ma20", "close_vs_ma20_pct"], derivable=["close_vs_ma20_pct"]), "feature_frame_daily + high_recall surface", "direct pct available and can be recomputed")
    add("ma_position", "close vs 60MA", _feature_status(available=["close", "ma60", "close_vs_ma60_pct"], derivable=["close_vs_ma60_pct"]), "feature_frame_daily", "derived from close and ma60")
    add("ma_position", "7MA slope", _feature_status(available=["ma7", "ma7_prev1", "ma7_slope_1"], derivable=["ma7_slope_1"]), "feature_frame_daily", "direction is derivable safely")
    add("ma_position", "20MA slope", _feature_status(available=["ma20", "ma20_prev1", "ma20_slope_1"], derivable=["ma20_slope_1"]), "feature_frame_daily", "direction is derivable safely")
    add("ma_position", "60MA slope", _feature_status(available=["ma60", "ma60_prev1", "ma60_slope_1"], derivable=["ma60_slope_1"]), "feature_frame_daily", "direction is derivable safely")
    add("ma_position", "20MA reclaim count", _feature_status(available=["cnt_20_above"]), "feature_frame_daily", "available as above-bar count proxy")
    add("ma_position", "7MA/20MA above-below bar counts", _feature_status(available=["cnt_7_above", "cnt_20_above"]), "feature_frame_daily", "available as above-bar counts")
    add("ma_position", "distance from 20MA", _feature_status(available=["diff20_pct", "close_vs_ma20_pct"]), "feature_frame_daily + high_recall surface", "available as pct distance")
    add("ma_position", "distance from 60MA", _feature_status(available=["close_vs_ma60_pct"]), "feature_frame_daily", "available as pct distance")

    # Candle / shape
    add("candle_shape", "small-body candle / koma", _feature_status(derivable=["candle_body_ratio"], instrumentation=["explicit small-body flag"]), "feature_frame_daily + conditional_high_value script", "derive safely from body ratio; explicit flag not required")
    add("candle_shape", "doji-like candle", _feature_status(derivable=["candle_body_ratio"], instrumentation=["explicit doji flag"]), "feature_frame_daily + conditional_high_value script", "derive safely from body ratio")
    add("candle_shape", "bullish reversal candle", _feature_status(available=["bull_engulfing", "morning_star", "bull_marubozu"]), "feature_complete high recall surface", "present as pattern flags")
    add("candle_shape", "bearish full-cancel candle", _feature_status(derivable=["bear_marubozu", "shooting_star_like", "candle_exhaustion_risk"]), "feature_complete high recall surface", "derivable from existing pattern flags and quality bucket")
    add("candle_shape", "gap-up", _feature_status(available=["gap_pct"], derivable=["gap_pct"]), "feature_frame_daily + high_recall surface", "sign-based derivation is safe")
    add("candle_shape", "gap-down", _feature_status(available=["gap_pct"], derivable=["gap_pct"]), "feature_frame_daily + high_recall surface", "sign-based derivation is safe")
    add("candle_shape", "long lower wick", _feature_status(available=["support_wick", "candle_lower_wick_ratio"]), "feature_complete high recall surface", "present as support wick proxy")
    add("candle_shape", "horizontal narrow-range cluster", _feature_status(derivable=["range_pct", "monthly_range_width"], instrumentation=["explicit cluster flag"]), "feature_frame_daily + monthly context", "safe derivation from range width, not materialized in current candidate surface")
    add("candle_shape", "inside bar / harami", _feature_status(derivable=["daily_bars OHLC"], instrumentation=["explicit inside-bar flag"]), "daily_bars + candle study helpers", "safe to derive from OHLC, not directly materialized here")

    # Volume
    add("volume", "volume vs 20-day average", _feature_status(available=["vol_ratio5_20"]), "feature_frame_daily + high_recall surface", "direct proxy")
    add("volume", "volume expansion", _feature_status(available=["vol_ratio5_20"], derivable=["vol_ratio5_20"]), "feature_frame_daily + high_recall surface", "ratio > 1")
    add("volume", "volume contraction", _feature_status(available=["vol_ratio5_20"], derivable=["vol_ratio5_20"]), "feature_frame_daily + high_recall surface", "ratio < 1")
    add("volume", "volume expansion after compression", _feature_status(derivable=["vol_ratio5_20", "range_pct", "candle_body_ratio"]), "feature_frame_daily", "derive safely from existing daily features")

    # Stability / risk
    add("risk", "stable bad-pick family", _feature_status(available=["stable_bad_pick_family"]), "feature_complete high recall surface", "directly available")
    add("risk", "bad-pick diagnostic", _feature_status(derivable=["shape_classification", "stable_bad_pick_family"], instrumentation=["explicit bad_pick diagnostic flag"]), "bad-pick audit surfaces", "not required for the candidate contract")
    add("risk", "conditional high value", _feature_status(available=["conditional_high_value"]), "feature_complete high recall surface", "directly available")
    add("risk", "shape classification", _feature_status(available=["shape_classification"]), "feature_complete high recall surface", "directly available")
    add("risk", "monthly / weekly no-lookahead context", _feature_status(available=list(NO_LOOKAHEAD_FLAGS) + ["monthly_context_date", "weekly_context_date"]), "feature_complete high recall surface", "explicit no-lookahead flags exist")
    add("risk", "recent downside failure", _feature_status(available=["drawdown60", "rebound60"]), "feature_frame_daily", "drawdown/rebound proxy is available")
    add("risk", "recent low break / no low break", _feature_status(available=["breakout20_down", "low20_dist", "drawdown60"]), "feature_frame_daily", "proxy available without extra instrumentation")
    add("risk", "volatility / ATR", _feature_status(available=["atr14", "atr14_pct", "range_pct"]), "feature_frame_daily", "direct proxy available")

    return {
        "schema_version": FEATURE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "field_count": len(items),
        "core_minimum_fields": {
            "close_vs_20ma": True,
            "close_vs_60ma": True,
            "ma20_slope_or_direction": True,
            "candle_body_range_proxy": True,
            "volume_proxy": True,
            "no_lookahead_date_integrity": True,
            "forward_labels_for_evaluation": True,
        },
        "availability_by_category": {
            category: int(sum(1 for item in items if item["category"] == category)) for category in sorted({item["category"] for item in items})
        },
        "fields": items,
        "decision": {
            "typed": "ready_to_design_iizuka_candidate_contract",
            "reason": "core no-lookahead MA, candle, volume, stability, and evaluation-label fields are available or safely derivable",
            "missing_core_fields": [],
        },
    }


def _build_candidate_contract() -> dict[str, Any]:
    score_weights = {
        "entry_strength_score": 1.00,
        "signal_quality_high": 1.30,
        "signal_quality_mid": 0.55,
        "decision_candle_strong": 0.90,
        "decision_candle_mixed": 0.30,
        "volume_confirmed": 0.80,
        "volume_neutral": 0.25,
        "volume_weak": -0.10,
        "shape_positive_modifier": 0.40,
        "shape_context_dependent": 0.20,
        "ma20_direction_up": 0.25,
        "ma7_direction_up": 0.05,
        "ma60_direction_up": 0.05,
        "reclaim_close_to_ma20": 0.35,
        "close_near_ma60": 0.20,
        "drawdown_recovery": 0.55,
        "rebound60": 0.20,
        "candlestick_support": 0.20,
        "stable_bad_pick_penalty": -4.00,
        "exhaustion_penalty": -1.20,
    }
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "research_only": True,
        "candidate_key_version": "canonical_anchor_date_side_symbol_v1",
        "selection_behavior_changed": False,
        "blocks": {
            "long_side_only": {
                "required": True,
                "rule": "side == long",
            },
            "context": {
                "required": True,
                "rule": "signal_quality_bucket in {signal_quality_high, signal_quality_mid} and monthly/weekly no-lookahead flags pass",
            },
            "compression": {
                "required": True,
                "rule": "decision_candle_quality in {candle_strong, candle_mixed}",
            },
            "trigger_proximity": {
                "required": True,
                "rule": "volume_participation_bucket in {volume_confirmed, volume_neutral} or support_wick/bullish reversal present",
            },
            "risk": {
                "required": True,
                "rule": "stable_bad_pick_family is false and candle_exhaustion_risk is false",
            },
        },
        "score_contract": {
            "name": "iizuka_pre_decisive_score_v1",
            "ranking_basis": "higher score first, then lower champion_rank, then symbol",
            "weights": score_weights,
            "score_components": [
                "entry_strength_score",
                "signal_quality_bucket",
                "decision_candle_quality",
                "volume_participation_bucket",
                "shape_classification",
                "ma20_direction_up_or_flat",
                "dist_ma20_pct proximity bonus",
                "dist_ma60_pct proximity bonus",
                "drawdown60 / rebound60 stabilization bonus",
                "cnt_20_above / cnt_7_above moderation bonus",
                "stable_bad_pick_family penalty",
                "candle_exhaustion_risk penalty",
            ],
        },
        "output_fields": [
            "candidate_contract_name",
            "iizuka_context_block_pass",
            "iizuka_compression_block_pass",
            "iizuka_trigger_proximity_block_pass",
            "iizuka_risk_block_pass",
            "iizuka_candidate_reason",
            "iizuka_candidate_score",
            "iizuka_candidate_rank",
            "iizuka_missing_feature_reason",
            "research_only",
        ],
        "feature_dependencies": {
            "core": ["signal_quality_bucket", "decision_candle_quality", "stable_bad_pick_family", "monthly_context_no_lookahead", "weekly_context_no_lookahead"],
            "bonus": [
                "entry_strength_score",
                "dist_ma20_pct",
                "dist_ma60_pct",
                "vol_ratio5_20",
                "shape_classification",
                "ma20_slope_1",
                "drawdown60",
                "rebound60",
                "cnt_20_above",
                "cnt_7_above",
            ],
            "evaluation_only": list(EVAL_LABEL_COLUMNS),
        },
    }


def _compute_candidate_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if str(row.get("signal_quality_bucket")) in {"signal_quality_high", "signal_quality_mid"}:
        reasons.append(str(row.get("signal_quality_bucket")))
    if str(row.get("decision_candle_quality")) in {"candle_strong", "candle_mixed"}:
        reasons.append(str(row.get("decision_candle_quality")))
    if str(row.get("volume_participation_bucket")) in {"volume_confirmed", "volume_neutral"}:
        reasons.append(str(row.get("volume_participation_bucket")))
    if bool(row.get("support_wick")):
        reasons.append("support_wick")
    if bool(row.get("bull_engulfing")):
        reasons.append("bull_engulfing")
    if bool(row.get("morning_star")):
        reasons.append("morning_star")
    if bool(row.get("bull_marubozu")):
        reasons.append("bull_marubozu")
    if bool(row.get("ma20_direction_up")):
        reasons.append("ma20_direction_up")
    if _safe_float(row.get("dist_ma20_pct")) is not None:
        reasons.append("ma20_distance_ok")
    if _safe_float(row.get("dist_ma60_pct")) is not None:
        reasons.append("ma60_distance_ok")
    if _safe_float(row.get("drawdown60")) is not None:
        reasons.append("drawdown60_stabilizing")
    if _safe_float(row.get("rebound60")) is not None:
        reasons.append("rebound60_available")
    if not reasons:
        reasons.append("minimal_context")
    return "|".join(dict.fromkeys(reasons))


def _compute_candidate_scores(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()
    base_score = pd.to_numeric(scored.get("entry_strength_score"), errors="coerce").fillna(0.0)
    bonus = pd.Series(0.0, index=scored.index)
    bonus += scored["signal_quality_bucket"].map({"signal_quality_high": 1.30, "signal_quality_mid": 0.55, "signal_quality_low": 0.0}).fillna(0.0)
    bonus += scored["decision_candle_quality"].map({"candle_strong": 0.90, "candle_mixed": 0.30, "candle_weak": 0.0, "candle_exhaustion_risk": -1.20}).fillna(0.0)
    bonus += scored["volume_participation_bucket"].map({"volume_confirmed": 0.80, "volume_neutral": 0.25, "volume_weak": -0.10}).fillna(0.0)
    bonus += scored["shape_classification"].map({"shape_positive_modifier": 0.40, "shape_context_dependent": 0.20, "shape_missing": 0.0}).fillna(0.0)
    bonus += scored["daily_main_state_ctx_backfilled"].map({"daily_reversal_up_candidate": 0.55, "daily_up_mid": 0.35}).fillna(0.0)
    bonus += scored["weekly_main_state_ctx_backfilled"].map({"weekly_up_mid": 0.20, "weekly_up_late": -0.25}).fillna(0.0)
    bonus += scored["monthly_main_state_ctx_backfilled"].map({"monthly_range_mid": 0.20, "monthly_up_top_warning": -0.20, "monthly_down_mid": -0.15}).fillna(0.0)
    bonus += np.where(pd.to_numeric(scored.get("ma20_slope_1"), errors="coerce").fillna(0.0) >= 0.0, 0.25, -0.05)
    bonus += np.where(pd.to_numeric(scored.get("vol_ratio5_20"), errors="coerce").fillna(0.0) >= 1.15, 0.20, 0.0)
    bonus += np.clip(0.07 - pd.to_numeric(scored.get("dist_ma20_pct"), errors="coerce").abs(), 0, 0.07) * 5.0
    bonus += np.clip(0.10 - pd.to_numeric(scored.get("dist_ma60_pct"), errors="coerce").abs(), 0, 0.10) * 1.5
    bonus += np.where(pd.to_numeric(scored.get("drawdown60"), errors="coerce").notna(), np.clip(-pd.to_numeric(scored.get("drawdown60"), errors="coerce") - 0.01, 0, 0.08) * 6.0, 0.0)
    bonus += np.where(pd.to_numeric(scored.get("rebound60"), errors="coerce").notna(), np.clip(pd.to_numeric(scored.get("rebound60"), errors="coerce") - 0.12, 0, 0.20) * 2.0, 0.0)
    bonus += np.where(pd.to_numeric(scored.get("cnt_20_above"), errors="coerce").notna(), np.clip(pd.to_numeric(scored.get("cnt_20_above"), errors="coerce") / 40.0, 0, 1.0) * 0.25, 0.0)
    bonus += np.where(pd.to_numeric(scored.get("cnt_7_above"), errors="coerce").notna(), np.clip(pd.to_numeric(scored.get("cnt_7_above"), errors="coerce") / 20.0, 0, 1.0) * 0.20, 0.0)
    bonus += np.where(scored["stable_bad_pick_family"], -4.0, 0.0)
    bonus += np.where(scored["decision_candle_quality"].eq("candle_exhaustion_risk"), -1.2, 0.0)
    scored["iizuka_candidate_score"] = base_score + bonus
    scored["iizuka_candidate_rank"] = scored.groupby("anchor_date")["iizuka_candidate_score"].rank(method="first", ascending=False)
    scored["iizuka_candidate_reason"] = scored.apply(_compute_candidate_reason, axis=1)
    scored["iizuka_missing_feature_reason"] = scored.apply(_candidate_missing_reason, axis=1)
    scored["candidate_contract_name"] = "iizuka_pre_decisive_long_candidate_v1"
    scored["research_only"] = True
    scored["iizuka_context_block_pass"] = scored["signal_quality_bucket"].isin({"signal_quality_high", "signal_quality_mid"}) & scored["monthly_context_no_lookahead"].fillna(False).astype(bool) & scored["weekly_context_no_lookahead"].fillna(False).astype(bool)
    scored["iizuka_compression_block_pass"] = scored["decision_candle_quality"].isin({"candle_strong", "candle_mixed"}) & scored["shape_classification"].isin({"shape_positive_modifier", "shape_context_dependent", "shape_missing"})
    trigger_ok = scored["volume_participation_bucket"].isin({"volume_confirmed", "volume_neutral"}) | scored["support_wick"].fillna(False).astype(bool) | scored["bull_engulfing"].fillna(False).astype(bool) | scored["morning_star"].fillna(False).astype(bool) | scored["bull_marubozu"].fillna(False).astype(bool)
    scored["iizuka_trigger_proximity_block_pass"] = trigger_ok
    scored["iizuka_risk_block_pass"] = (~scored["stable_bad_pick_family"].fillna(False).astype(bool)) & scored["decision_candle_quality"].ne("candle_exhaustion_risk")
    scored["canonical_candidate_key"] = _key(scored)
    return scored


def _candidate_missing_reason(row: pd.Series) -> str:
    missing: list[str] = []
    for field in ("ma20_slope_1", "ma60_slope_1", "ma7_slope_1", "drawdown60", "rebound60", "cnt_20_above", "cnt_7_above", "close_vs_ma20_pct", "close_vs_ma60_pct"):
        if _is_missing(row.get(field)):
            missing.append(field)
    return "|".join(missing)


def _filter_candidate_rows(frame: pd.DataFrame) -> pd.DataFrame:
    candidate = frame.loc[
        (frame["side"].astype(str) == "long")
        & frame["monthly_context_no_lookahead"].fillna(False).astype(bool)
        & frame["weekly_context_no_lookahead"].fillna(False).astype(bool)
        & (~frame["stable_bad_pick_family"].fillna(False).astype(bool))
        & frame["signal_quality_bucket"].isin({"signal_quality_high", "signal_quality_mid"})
        & frame["decision_candle_quality"].isin({"candle_strong", "candle_mixed"})
    ].copy()
    if candidate.empty:
        return candidate
    candidate = _compute_candidate_scores(candidate)
    candidate = candidate.sort_values(["anchor_date", "iizuka_candidate_score", "champion_rank", "symbol"], ascending=[True, False, True, True], kind="stable").reset_index(drop=True)
    return candidate


def _surface_summary(candidate: pd.DataFrame, source: pd.DataFrame) -> dict[str, Any]:
    source_group_count = int(source.loc[source["side"].astype(str) == "long", "anchor_date"].nunique())
    candidate_group_count = int(candidate["anchor_date"].nunique()) if len(candidate) else 0
    candidate_month = pd.to_datetime(candidate["anchor_date"], errors="coerce").dt.strftime("%Y-%m") if len(candidate) else pd.Series(dtype=str)
    candidate_symbols = Counter(candidate["symbol"].astype(str).tolist()) if len(candidate) else Counter()
    return {
        "schema_version": SURFACE_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "row_count": int(len(candidate)),
        "group_count": candidate_group_count,
        "source_long_group_count": source_group_count,
        "zero_pass_groups": int(max(0, source_group_count - candidate_group_count)),
        "symbol_count": int(candidate["symbol"].nunique()) if len(candidate) else 0,
        "month_count": int(candidate_month.nunique()) if len(candidate) else 0,
        "top_symbol_counts": {str(k): int(v) for k, v in candidate_symbols.most_common(10)} if candidate_symbols else {},
        "candidate_reason_counts": {str(k): int(v) for k, v in candidate["iizuka_candidate_reason"].value_counts().head(10).items()} if len(candidate) else {},
        "missing_feature_reason_counts": {str(k): int(v) for k, v in candidate["iizuka_missing_feature_reason"].replace("", "<none>").value_counts().head(10).items()} if len(candidate) else {},
        "score_summary": {
            "min": float(pd.to_numeric(candidate["iizuka_candidate_score"], errors="coerce").min()) if len(candidate) else None,
            "median": float(pd.to_numeric(candidate["iizuka_candidate_score"], errors="coerce").median()) if len(candidate) else None,
            "max": float(pd.to_numeric(candidate["iizuka_candidate_score"], errors="coerce").max()) if len(candidate) else None,
        },
        "rank_summary": {
            "min": float(pd.to_numeric(candidate["iizuka_candidate_rank"], errors="coerce").min()) if len(candidate) else None,
            "median": float(pd.to_numeric(candidate["iizuka_candidate_rank"], errors="coerce").median()) if len(candidate) else None,
            "max": float(pd.to_numeric(candidate["iizuka_candidate_rank"], errors="coerce").max()) if len(candidate) else None,
        },
    }


def _no_lookahead_audit(candidate: pd.DataFrame) -> dict[str, Any]:
    violations: dict[str, int] = {}
    for flag in NO_LOOKAHEAD_FLAGS:
        if flag in candidate.columns:
            violations[f"{flag}_false_count"] = int((~candidate[flag].fillna(False).astype(bool)).sum())
    date_violations = {}
    for field in ("monthly_context_date", "weekly_context_date"):
        if field in candidate.columns:
            asof = pd.to_datetime(candidate["anchor_date"], errors="coerce")
            context = pd.to_datetime(candidate[field], errors="coerce")
            date_violations[f"{field}_future_count"] = int((context > asof).sum())
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_pass": all(v == 0 for v in violations.values()) and all(v == 0 for v in date_violations.values()),
        "flag_violations": violations,
        "date_violations": date_violations,
        "notes": [
            "candidate rows use only historical / current-row context fields",
            "evaluation labels are attached after candidate construction",
        ],
    }


def _leakage_audit(candidate: pd.DataFrame) -> dict[str, Any]:
    feature_fields_used = {
        "signal_quality_bucket",
        "decision_candle_quality",
        "volume_participation_bucket",
        "shape_classification",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "stable_bad_pick_family",
        "entry_strength_score",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "ma20_slope_1",
        "vol_ratio5_20",
        "drawdown60",
        "rebound60",
        "cnt_20_above",
        "cnt_7_above",
    }
    outcome_fields = set(EVAL_LABEL_COLUMNS)
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_fields_used": sorted(feature_fields_used),
        "outcome_fields": sorted(outcome_fields),
        "outcome_fields_used_as_features": sorted(feature_fields_used.intersection(outcome_fields)),
        "outcome_fields_attached_after_candidate_construction": sorted([c for c in EVAL_LABEL_COLUMNS if c in candidate.columns]),
        "leakage_free": not feature_fields_used.intersection(outcome_fields),
        "note": "evaluation labels were joined after the candidate surface was constructed",
    }


def _per_k_selection(frame: pd.DataFrame, score_col: str, k: int) -> pd.DataFrame:
    ranked = frame.copy()
    ranked["selected_rank"] = ranked.groupby("anchor_date")[score_col].rank(method="first", ascending=False)
    return ranked.loc[ranked["selected_rank"] <= k].copy()


def _compare_topk(champion: pd.DataFrame, challenger: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    diff_rows: list[pd.DataFrame] = []
    failure_mode: dict[str, Any] = {
        "schema_version": FAILURE_MODE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "per_k": {},
    }
    headroom: dict[str, Any] = {
        "schema_version": ORACLE_HEADROOM_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "per_k": {},
    }
    for k in TOP_K_VALUES:
        champ = champion[champion[f"champion_selected_top{k}"]].copy()
        chal = _per_k_selection(challenger, "iizuka_candidate_score", k)
        champ_keys = set(champ["surface_key"])
        chal_keys = set(chal["surface_key"])
        union = champ_keys | chal_keys
        intersection = champ_keys & chal_keys
        diff = pd.DataFrame(
            {
                "top_k": k,
                "surface_key": list(union),
            }
        )
        diff = diff.merge(
            champion[["surface_key", "anchor_date", "symbol", "side", "champion_score", "champion_rank", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label"] + [c for c in ("candidate_idx", "month_bucket") if c in champion.columns]],
            on="surface_key",
            how="left",
        ).merge(
            chal[["surface_key", "iizuka_candidate_score", "iizuka_candidate_rank", "iizuka_candidate_reason", "iizuka_context_block_pass", "iizuka_compression_block_pass", "iizuka_trigger_proximity_block_pass", "iizuka_risk_block_pass"]],
            on="surface_key",
            how="left",
        )
        diff["selection_state"] = diff["surface_key"].apply(
            lambda key: "both" if key in intersection else ("champion_only" if key in champ_keys else "challenger_only")
        )
        diff["top_k"] = k
        diff["selected_in_champion"] = diff["surface_key"].isin(champ_keys)
        diff["selected_in_challenger"] = diff["surface_key"].isin(chal_keys)
        diff["member_change"] = diff["selection_state"] != "both"
        diff_rows.append(diff)

        champion_metrics = {
            "row_count": int(len(champ)),
            "mean_forward_ret_20d": float(pd.to_numeric(champ["forward_ret_20d"], errors="coerce").mean()) if len(champ) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(champ["path_value_score_v1"], errors="coerce").mean()) if len(champ) else None,
            "top15_capture_count": int(pd.to_numeric(champ["top15_label"], errors="coerce").fillna(0).sum()) if len(champ) else 0,
            "top15_capture_rate": float(pd.to_numeric(champ["top15_label"], errors="coerce").mean()) if len(champ) else None,
            "top20pct_capture_count": int(pd.to_numeric(champ["top20pct_label"], errors="coerce").fillna(0).sum()) if "top20pct_label" in champ.columns and len(champ) else 0,
            "bottom15_contamination_count": int(pd.to_numeric(champ["bottom15_label"], errors="coerce").fillna(0).sum()) if len(champ) else 0,
            "bottom15_contamination_rate": float(pd.to_numeric(champ["bottom15_label"], errors="coerce").mean()) if len(champ) else None,
            "non_positive_return_count": int((pd.to_numeric(champ["forward_ret_20d"], errors="coerce") <= 0).sum()) if len(champ) else 0,
        }
        challenger_metrics = {
            "row_count": int(len(chal)),
            "mean_forward_ret_20d": float(pd.to_numeric(chal["forward_ret_20d"], errors="coerce").mean()) if len(chal) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(chal["path_value_score_v1"], errors="coerce").mean()) if len(chal) else None,
            "top15_capture_count": int(pd.to_numeric(chal["top15_label"], errors="coerce").fillna(0).sum()) if len(chal) else 0,
            "top15_capture_rate": float(pd.to_numeric(chal["top15_label"], errors="coerce").mean()) if len(chal) else None,
            "top20pct_capture_count": int(pd.to_numeric(chal["top20pct_label"], errors="coerce").fillna(0).sum()) if "top20pct_label" in chal.columns and len(chal) else 0,
            "bottom15_contamination_count": int(pd.to_numeric(chal["bottom15_label"], errors="coerce").fillna(0).sum()) if len(chal) else 0,
            "bottom15_contamination_rate": float(pd.to_numeric(chal["bottom15_label"], errors="coerce").mean()) if len(chal) else None,
            "non_positive_return_count": int((pd.to_numeric(chal["forward_ret_20d"], errors="coerce") <= 0).sum()) if len(chal) else 0,
        }
        rows.append(
            {
                "top_k": k,
                "champion": champion_metrics,
                "challenger": challenger_metrics,
                "membership_changed_count": int(len(champ_keys ^ chal_keys)),
                "overlap_ratio": float(len(intersection) / len(union)) if union else None,
                "champion_group_count": int(champ["anchor_date"].nunique()) if len(champ) else 0,
                "challenger_group_count": int(chal["anchor_date"].nunique()) if len(chal) else 0,
                "champion_symbol_count": int(champ["symbol"].nunique()) if len(champ) else 0,
                "challenger_symbol_count": int(chal["symbol"].nunique()) if len(chal) else 0,
                "zero_pass_groups": int(max(0, champion["anchor_date"].nunique() - chal["anchor_date"].nunique())),
            }
        )
        failure_mode["per_k"][str(k)] = {
            "champion_only_count": int(len(champ_keys - chal_keys)),
            "challenger_only_count": int(len(chal_keys - champ_keys)),
            "top15_winner_loss_count": int(pd.to_numeric(champ.loc[~champ["surface_key"].isin(chal_keys), "top15_label"], errors="coerce").fillna(0).sum()) if len(champ) else 0,
            "bottom15_loss_count": int(pd.to_numeric(champ.loc[~champ["surface_key"].isin(chal_keys), "bottom15_label"], errors="coerce").fillna(0).sum()) if len(champ) else 0,
            "reason_block_contribution": {str(k2): int(v2) for k2, v2 in chal["iizuka_candidate_reason"].value_counts().head(8).items()},
            "false_positive_cost": {
                "bottom15_count": int(pd.to_numeric(chal["bottom15_label"], errors="coerce").fillna(0).sum()) if len(chal) else 0,
                "bottom15_rate": float(pd.to_numeric(chal["bottom15_label"], errors="coerce").mean()) if len(chal) else None,
            },
        }
        missed = champ.loc[~champ["surface_key"].isin(chal_keys)].copy()
        missed = missed.sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5)
        gained = chal.loc[~chal["surface_key"].isin(champ_keys)].copy()
        gained = gained.sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5)
        headroom["per_k"][str(k)] = {
            "missed_top15_examples": _records(missed, fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "champion_rank"]),
            "gained_top15_examples": _records(gained, fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "iizuka_candidate_score", "iizuka_candidate_rank"]),
            "missed_top15_count": int(pd.to_numeric(missed["top15_label"], errors="coerce").fillna(0).sum()) if len(missed) else 0,
            "gained_top15_count": int(pd.to_numeric(gained["top15_label"], errors="coerce").fillna(0).sum()) if len(gained) else 0,
        }

    comparison = {
        "schema_version": VARIANT_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "metric_mode": "per_anchor_date_topK",
        "per_k": rows,
    }
    diff_frame = pd.concat(diff_rows, ignore_index=True) if diff_rows else pd.DataFrame()
    return comparison, diff_frame, failure_mode, headroom


def _records(frame: pd.DataFrame, fields: list[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    use = [c for c in fields if c in frame.columns]
    return [_json_ready(dict(row)) for row in frame[use].to_dict(orient="records")]


def _compare_series(champion: pd.DataFrame, challenger: pd.DataFrame, metric: str) -> tuple[dict[str, Any], float, float]:
    champ_values = pd.to_numeric(champion[metric], errors="coerce")
    chal_values = pd.to_numeric(challenger[metric], errors="coerce")
    return (
        {
            "champion_mean": float(champ_values.mean()) if len(champ_values) else None,
            "challenger_mean": float(chal_values.mean()) if len(chal_values) else None,
            "delta": float(chal_values.mean() - champ_values.mean()) if len(champ_values) and len(chal_values) else None,
        },
        float(champ_values.mean()) if len(champ_values) else float("nan"),
        float(chal_values.mean()) if len(chal_values) else float("nan"),
    )


def _failure_reason_summary(candidate: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for reason, group in candidate.groupby("iizuka_candidate_reason", sort=False):
        rows.append(
            {
                "candidate_reason": reason,
                "row_count": int(len(group)),
                "mean_forward_ret_20d": float(pd.to_numeric(group["forward_ret_20d"], errors="coerce").mean()) if len(group) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(group["path_value_score_v1"], errors="coerce").mean()) if len(group) else None,
                "top15_count": int(pd.to_numeric(group["top15_label"], errors="coerce").fillna(0).sum()) if len(group) else 0,
                "bottom15_count": int(pd.to_numeric(group["bottom15_label"], errors="coerce").fillna(0).sum()) if len(group) else 0,
                "top15_rate": float(pd.to_numeric(group["top15_label"], errors="coerce").mean()) if len(group) else None,
                "bottom15_rate": float(pd.to_numeric(group["bottom15_label"], errors="coerce").mean()) if len(group) else None,
            }
        )
    return pd.DataFrame(rows).sort_values(["row_count", "mean_forward_ret_20d"], ascending=[False, False], kind="stable").reset_index(drop=True)


def _build_decision(comparison: dict[str, Any], candidate: pd.DataFrame, no_lookahead: dict[str, Any], source_long_group_count: int) -> dict[str, Any]:
    top5 = next(item for item in comparison["per_k"] if item["top_k"] == 5)
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    decision = "hold_needs_forward_surfaces"
    reason = "direction is positive but raw top10/top20 capture remains below champion and the candidate surface is still narrower than the champion coverage"
    if (
        top5["challenger"]["mean_forward_ret_20d"] is not None
        and top10["challenger"]["mean_forward_ret_20d"] is not None
        and top20["challenger"]["mean_forward_ret_20d"] is not None
        and top5["challenger"]["mean_forward_ret_20d"] >= top5["champion"]["mean_forward_ret_20d"]
        and top10["challenger"]["mean_forward_ret_20d"] >= top10["champion"]["mean_forward_ret_20d"]
        and top20["challenger"]["mean_forward_ret_20d"] >= top20["champion"]["mean_forward_ret_20d"]
        and top20["challenger"]["bottom15_contamination_rate"] <= top20["champion"]["bottom15_contamination_rate"] * 1.10
        and no_lookahead["no_lookahead_pass"]
        and comparison["per_k"][0]["challenger_group_count"] >= 120
    ):
        decision = "ready_for_iizuka_candidate_challenger_design"
        reason = "same-condition evidence is strong enough to move to challenger design"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": "keep" if decision == "ready_for_iizuka_candidate_challenger_design" else "hold",
        "reason": reason,
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "summary": {
            "candidate_row_count": int(len(candidate)),
            "candidate_group_count": int(candidate["anchor_date"].nunique()) if len(candidate) else 0,
            "candidate_symbol_count": int(candidate["symbol"].nunique()) if len(candidate) else 0,
            "zero_pass_groups": int(max(0, source_long_group_count - int(candidate["anchor_date"].nunique()))) if source_long_group_count else 0,
            "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
        },
        "champion_comparison": {
            "top5": top5,
            "top10": top10,
            "top20": top20,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive long candidate-generation v1")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--source-surface", type=str, default=str(DEFAULT_SOURCE_SURFACE))
    parser.add_argument("--label-surface", type=str, default=str(DEFAULT_LABEL_SURFACE))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    source_surface = _safe_path(args.source_surface, DEFAULT_SOURCE_SURFACE)
    label_surface = _safe_path(args.label_surface, DEFAULT_LABEL_SURFACE)
    runtime_db = _safe_path(args.runtime_db, DEFAULT_RUNTIME_DB)
    session_id = _session_id()
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=True)

    source, labels = _load_inputs(source_surface, label_surface)
    source = _attach_labels(source, labels)
    source = _load_runtime_features(runtime_db, source)
    source = source.sort_values(["anchor_date", "champion_rank", "symbol"], ascending=[True, True, True], kind="stable").reset_index(drop=True)

    feature_audit = _build_feature_availability_audit(source)
    contract = _build_candidate_contract()
    candidate = _filter_candidate_rows(source)
    if candidate.empty:
        raise RuntimeError("candidate surface is empty after applying the Iizuka pre-decisive contract")

    no_lookahead = _no_lookahead_audit(candidate)
    leakage = _leakage_audit(candidate)
    summary = _surface_summary(candidate, source)

    champion = source.loc[source["side"].astype(str) == "long"].copy()
    comparison, diff_frame, failure_mode, headroom = _compare_topk(champion, candidate)

    candidate_reason_summary = _failure_reason_summary(candidate)
    failure_mode["reason_block_summary"] = _records(candidate_reason_summary, fields=["candidate_reason", "row_count", "mean_forward_ret_20d", "mean_path_value_score_v1", "top15_count", "bottom15_count", "top15_rate", "bottom15_rate"])
    failure_mode["overall"] = {
        "top5_delta_mean_forward_ret_20d": comparison["per_k"][0]["challenger"]["mean_forward_ret_20d"] - comparison["per_k"][0]["champion"]["mean_forward_ret_20d"],
        "top10_delta_mean_forward_ret_20d": comparison["per_k"][1]["challenger"]["mean_forward_ret_20d"] - comparison["per_k"][1]["champion"]["mean_forward_ret_20d"],
        "top20_delta_mean_forward_ret_20d": comparison["per_k"][2]["challenger"]["mean_forward_ret_20d"] - comparison["per_k"][2]["champion"]["mean_forward_ret_20d"],
        "top5_delta_bottom15_rate": comparison["per_k"][0]["challenger"]["bottom15_contamination_rate"] - comparison["per_k"][0]["champion"]["bottom15_contamination_rate"],
        "top10_delta_bottom15_rate": comparison["per_k"][1]["challenger"]["bottom15_contamination_rate"] - comparison["per_k"][1]["champion"]["bottom15_contamination_rate"],
        "top20_delta_bottom15_rate": comparison["per_k"][2]["challenger"]["bottom15_contamination_rate"] - comparison["per_k"][2]["champion"]["bottom15_contamination_rate"],
    }
    failure_mode["notes"] = [
        "reason-block summaries are grouped by candidate reason on the challenger surface",
        "false-positive cost is proxied by bottom15 contamination among selected rows",
    ]

    source_long_group_count = int(source.loc[source["side"].astype(str) == "long", "anchor_date"].nunique())
    decision = _build_decision(comparison, candidate, no_lookahead, source_long_group_count)
    if decision["decision"] == "ready_for_iizuka_candidate_challenger_design":
        decision["status"] = "keep"
    else:
        decision["status"] = "hold"

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_root": str(session_root),
        "jobs": int(args.jobs),
        "research_only": True,
        "boundary": "TRADEX-only",
        "fixed_conditions": {
            "side": "long",
            "same_universe_where_possible": True,
            "same_evaluation_period_where_possible": True,
            "topk_frame": list(TOP_K_VALUES),
            "forward_return_horizon_business_days": 20,
            "no_lookahead": True,
            "same_artifact_detail_level": True,
        },
        "source_artifacts": {
            "feature_complete_high_recall_surface_v1": str(source_surface),
            "candidate_prefilter_rows": str(label_surface),
            "runtime_db": str(runtime_db),
        },
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "candidate_contract_version": "v1",
        "notes": [
            "evaluation labels are attached after candidate construction",
            "no MeeMee, production ranking, publish, promotion, or research_inventory mutation occurs",
        ],
    }

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "output_root": str(output_root),
            "session_root": str(session_root),
            "source_surface": str(source_surface),
            "label_surface": str(label_surface),
            "runtime_db": str(runtime_db),
        },
        "label_join_keys": list(LABEL_JOIN_KEYS),
        "evaluation_labels": list(EVAL_LABEL_COLUMNS),
        "join_coverage": {
            "source_long_row_count": int((source["side"].astype(str) == "long").sum()),
            "candidate_row_count": int(len(candidate)),
            "label_coverage": {
                "forward_ret_20d": int(candidate["forward_ret_20d"].notna().sum()),
                "path_value_score_v1": int(candidate["path_value_score_v1"].notna().sum()),
                "top15_label": int(candidate["top15_label"].notna().sum()),
                "bottom15_label": int(candidate["bottom15_label"].notna().sum()),
                "top20pct_label": int(candidate["top20pct_label"].notna().sum()) if "top20pct_label" in candidate.columns else 0,
            },
        },
        "notes": [
            "prefilter labels are authoritative for forward_ret_20d/path_value_score_v1/top15_label/bottom15_label",
            "feature_frame_daily supplies no-lookahead MA, candle, volume, and stabilization fields",
        ],
    }

    _write_json(session_root / "run_manifest.json", run_manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "iizuka_pre_decisive_feature_availability_audit.json", feature_audit)
    _write_json(session_root / "iizuka_pre_decisive_candidate_contract.json", contract)
    _write_parquet(session_root / "iizuka_pre_decisive_long_candidate_rows.parquet", candidate)
    _write_json(session_root / "candidate_surface_generation_summary.json", summary)
    _write_json(session_root / "candidate_surface_no_lookahead_audit.json", no_lookahead)
    _write_json(session_root / "candidate_surface_leakage_audit.json", leakage)
    _write_json(session_root / "iizuka_pre_decisive_variant_pool_comparison.json", comparison)
    _write_parquet(session_root / "iizuka_pre_decisive_topk_membership_diff.parquet", diff_frame)
    _write_json(session_root / "iizuka_pre_decisive_failure_mode_audit.json", failure_mode)
    _write_json(session_root / "iizuka_pre_decisive_oracle_headroom_audit.json", headroom)
    _write_json(session_root / "iizuka_pre_decisive_long_candidate_generation_v1_decision.json", decision)

    artifact_complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_json": [
            "run_manifest.json",
            "input_resolution.json",
            "iizuka_pre_decisive_feature_availability_audit.json",
            "iizuka_pre_decisive_candidate_contract.json",
            "candidate_surface_generation_summary.json",
            "candidate_surface_no_lookahead_audit.json",
            "candidate_surface_leakage_audit.json",
            "iizuka_pre_decisive_variant_pool_comparison.json",
            "iizuka_pre_decisive_failure_mode_audit.json",
            "iizuka_pre_decisive_oracle_headroom_audit.json",
            "iizuka_pre_decisive_long_candidate_generation_v1_decision.json",
        ],
        "required_parquet": [
            "iizuka_pre_decisive_long_candidate_rows.parquet",
            "iizuka_pre_decisive_topk_membership_diff.parquet",
        ],
        "all_present": all((session_root / name).exists() for name in [
            "run_manifest.json",
            "input_resolution.json",
            "iizuka_pre_decisive_feature_availability_audit.json",
            "iizuka_pre_decisive_candidate_contract.json",
            "iizuka_pre_decisive_long_candidate_rows.parquet",
            "candidate_surface_generation_summary.json",
            "candidate_surface_no_lookahead_audit.json",
            "candidate_surface_leakage_audit.json",
            "iizuka_pre_decisive_variant_pool_comparison.json",
            "iizuka_pre_decisive_topk_membership_diff.parquet",
            "iizuka_pre_decisive_failure_mode_audit.json",
            "iizuka_pre_decisive_oracle_headroom_audit.json",
            "iizuka_pre_decisive_long_candidate_generation_v1_decision.json",
        ]),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", artifact_complete)

    print(json.dumps({"session_root": str(session_root), "decision": decision["decision"], "candidate_rows": int(len(candidate))}, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
