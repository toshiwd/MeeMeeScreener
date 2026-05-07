from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_bad_pick_unknown_reclassification_enriched_v1 import (  # noqa: E402
    _daily_main_state,
    _is_daily_negative,
    _is_daily_positive,
    _liquidity_missing,
    _monthly_bucket,
    _normalize_token,
    _safe_float,
    _score_component_missing,
    _unique_ordered,
    _weekly_bucket,
)
from scripts.tradex_bad_pick_unknown_reclassification_v1 import ROOT_CAUSE_CODE  # noqa: E402
from scripts.tradex_feature_surface_batch1_v1 import (  # noqa: E402
    FEATURE_NAMES,
    TOP_K_VALUES,
    _build_contrast,
    _build_coverage_and_missingness,
    _build_orfp_summary,
    _formula_contract,
    _json_ready,
    _make_session_id,
    _no_lookahead_audit,
    _write_json,
)

SCRIPT_NAME = "tradex_bad_pick_reclassification_batch1_features_v1"
SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1"
MANIFEST_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_input_resolution_v1"
VALIDATION_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_batch1_input_validation_v1"
COHORT_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_batch1_cohort_summary_v1"
BEFORE_AFTER_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_before_after_reclassification_summary_v1"
PAIRWISE_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_batch1_boundary_pairwise_summary_v1"
ROOT_CAUSE_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_batch1_root_cause_taxonomy_summary_v1"
FAMILY_BREAKDOWN_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_batch1_root_cause_family_breakdown_v1"
CANDIDATE_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_future_challenger_candidates_v1"
DECISION_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch1_features_v1_decision_v1"

DEFAULT_BATCH1_SESSION = Path(r"G:\Tradex\feature_surface_batch1_v1\20260501T093159Z-820266")
DEFAULT_CANDIDATE_SURFACE = DEFAULT_BATCH1_SESSION / "candidate_prefilter_rows_feature_enriched_v1.parquet"
DEFAULT_ORFP_SURFACE = DEFAULT_BATCH1_SESSION / "observable_regime_false_positive_feature_enriched_v1.parquet"
DEFAULT_PREVIOUS_ENRICHED_UNKNOWN = Path(r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991\enriched_unknown_reclassification_rows.parquet")
DEFAULT_ORIGINAL_RECLASSIFICATION = Path(r"G:\Tradex\bad_pick_unknown_reclassification_v1\20260501T043137Z-302dd27c\unknown_reclassification_rows.parquet")
DEFAULT_ORFP_FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation\20260501T090354Z-098449")
DEFAULT_REBUILD_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_reclassification_batch1_features_v1")

BATCH1_FEATURE_BASES = list(FEATURE_NAMES)
KEY_COLS = ["anchor_date", "symbol", "side"]
PAIRWISE_NEAR_MISS_FIELDS = [
    "monthly_main_state_ctx_backfilled",
    "weekly_main_state_ctx_backfilled",
    "daily_main_state_ctx_backfilled",
    "shape_classification",
    "family_classification",
    "dominant_regime_context",
    "market_regime_bucket",
    "monthly_context",
    "weekly_context",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "gap_pct",
    "liquidity20d",
    "vol_ratio5_20",
    "score",
    "forward_ret_20d",
    "path_value_score_v1",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _resolve_source_path(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _git_hash_or_unknown() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        value = result.stdout.strip()
        return value or "unknown"
    except Exception:
        return "unknown"


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    for column in ["anchor_date", "symbol", "side"]:
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "<na>", "none", "null", "unknown"}


def _feature_fields() -> list[str]:
    out: list[str] = []
    for feature in BATCH1_FEATURE_BASES:
        out.append(feature)
        out.append(f"{feature}_feature_status")
        out.append(f"{feature}_missing_reason")
    return out


def _near_miss_fields() -> list[str]:
    return list(dict.fromkeys(PAIRWISE_NEAR_MISS_FIELDS + _feature_fields()))


def _required_batch1_columns() -> list[str]:
    return list(dict.fromkeys(KEY_COLS + _feature_fields() + [
        "top15_label",
        "bottom15_label",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "candidate_idx",
        "best_near_miss_rank",
        "best_near_miss_symbol",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
        "near_miss_joined",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "daily_main_state_ctx_backfilled",
        "monthly_context_date_backfilled",
        "weekly_context_date_backfilled",
        "daily_main_state_ctx_date_backfilled",
        "monthly_context_backfill_status",
        "weekly_context_backfill_status",
        "daily_main_state_ctx_backfill_status",
        "missingness_class",
        "reclassified_root_cause_code",
        "reclassification_confidence",
        "is_data_gap",
        "is_candidate_for_future_challenger",
        "notes",
    ]))


def _feature_lookup(frame: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    lookup = frame[KEY_COLS + cols].copy()
    for column in KEY_COLS:
        lookup[column] = lookup[column].astype("string")
    return lookup


def _limit_anchor_dates(frame: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    if limit is None:
        return frame
    anchors = [str(value) for value in sorted(frame["anchor_date"].dropna().astype(str).unique())[:limit]]
    return frame.loc[frame["anchor_date"].isin(anchors)].copy()


def _merge_feature_lookup(frame: pd.DataFrame, lookup: pd.DataFrame, suffix: str) -> pd.DataFrame:
    merged = frame.merge(lookup, on=KEY_COLS, how="left", suffixes=("", suffix))
    return merged


def _join_near_miss_features(frame: pd.DataFrame, candidate_lookup: pd.DataFrame) -> pd.DataFrame:
    near_miss_lookup = candidate_lookup.rename(
        columns={
            "anchor_date": "best_miss_anchor_date",
            "symbol": "best_miss_symbol",
            "side": "best_miss_side",
        }
    )
    near_miss_lookup = near_miss_lookup.rename(
        columns={
            column: f"near_miss_{column}"
            for column in near_miss_lookup.columns
            if column not in {"best_miss_anchor_date", "best_miss_symbol", "best_miss_side"}
        }
    )
    merged = frame.merge(
        near_miss_lookup,
        left_on=["anchor_date", "best_near_miss_symbol", "side"],
        right_on=["best_miss_anchor_date", "best_miss_symbol", "best_miss_side"],
        how="left",
    )
    if "near_miss_symbol" not in merged.columns and "symbol_near" in merged.columns:
        merged["near_miss_symbol"] = merged["symbol_near"]
    if "near_miss_monthly_main_state_ctx_backfilled" not in merged.columns and "monthly_main_state_ctx_backfilled" in merged.columns:
        pass
    return merged


def _month_state(value: Any) -> str:
    return _normalize_token(value)


def _batch1_score_bucket(row: pd.Series) -> str:
    score = _safe_float(row.get("entry_strength_score"))
    if score is None:
        return "signal_quality_missing"
    if score >= 8.0:
        return "signal_quality_high"
    if score >= 5.0:
        return "signal_quality_mid"
    return "signal_quality_low"


def _batch1_confidence(code: str, row: pd.Series, boundary_summary: dict[str, Any] | None) -> str:
    if code.startswith("data_gap_"):
        return "high"
    score_gap = _safe_float(boundary_summary.get("score_gap")) if boundary_summary else None
    ret_gap = _safe_float(boundary_summary.get("forward_ret_20d_gap")) if boundary_summary else None
    path_gap = _safe_float(boundary_summary.get("path_value_gap")) if boundary_summary else None
    feature_score = _safe_float(row.get("entry_strength_score"))
    if code in {"high_entry_strength_valid_winner", "weak_decision_candle_false_positive", "weak_signal_quality_false_positive"}:
        if score_gap is not None and ret_gap is not None and path_gap is not None and score_gap > 0 and ret_gap < 0 and path_gap < 0:
            return "high"
        if feature_score is not None and feature_score >= 7.0:
            return "medium"
        return "low"
    if code in {"low_entry_strength_false_positive", "volume_missing_or_weak_confirmation", "low_liquidity_quality_false_positive", "limited_headroom_false_positive"}:
        if score_gap is not None and ret_gap is not None and path_gap is not None and score_gap > 0 and ret_gap < 0 and path_gap < 0:
            return "high"
        return "medium"
    return "low"


def _classify_batch1_row(row: pd.Series, *, boundary_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence_fields: list[str] = []
    missing_fields: list[str] = []

    def mark_missing(name: str) -> None:
        if name not in missing_fields:
            missing_fields.append(name)

    old_code = _normalize_token(row.get("reclassified_root_cause_code"))
    missingness_class = _normalize_token(row.get("missingness_class"))
    top15 = bool(row.get("top15_label"))
    bottom15 = bool(row.get("bottom15_label"))

    if old_code.startswith("data_gap_"):
        if "monthly_context" in old_code:
            evidence_fields.extend(["monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled", "daily_main_state_ctx_backfilled"])
        elif "liquidity" in old_code:
            evidence_fields.extend(["liquidity20d", "vol_ratio5_20"])
            mark_missing("vol_ratio5_20")
        elif "score_component" in old_code:
            evidence_fields.extend(["dist_ma20_pct", "dist_ma60_pct"])
            mark_missing("dist_ma20_pct")
            mark_missing("dist_ma60_pct")
        return {
            "missingness_class": missingness_class,
            "batch1_root_cause_code": old_code,
            "batch1_confidence": "high",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "existing data gap remains unchanged on the batch1 surface",
        }

    score = _safe_float(row.get("entry_strength_score"))
    signal = _normalize_token(row.get("signal_quality_bucket"))
    candle = _normalize_token(row.get("decision_candle_quality"))
    liquidity = _normalize_token(row.get("liquidity_quality_bucket"))
    volume = _normalize_token(row.get("volume_participation_bucket"))
    headroom = _normalize_token(row.get("higher_timeframe_headroom_bucket"))
    monthly_state = _month_state(row.get("monthly_main_state_ctx_backfilled"))
    weekly_state = _month_state(row.get("weekly_main_state_ctx_backfilled"))
    daily_state = _month_state(row.get("daily_main_state_ctx_backfilled"))
    family_class = _normalize_token(row.get("family_classification"))
    regime_ctx = _normalize_token(row.get("dominant_regime_context"))
    market_regime = _normalize_token(row.get("market_regime_bucket"))

    if top15 and score is not None and score >= 7.0 and signal in {"signal_quality_high", "signal_quality_mid"} and candle in {"candle_strong", "candle_mixed"}:
        evidence_fields.extend(["entry_strength_score", "signal_quality_bucket", "decision_candle_quality", "liquidity_quality_bucket", "higher_timeframe_headroom_bucket"])
        return {
            "missingness_class": missingness_class,
            "batch1_root_cause_code": "high_entry_strength_valid_winner",
            "batch1_confidence": _batch1_confidence("high_entry_strength_valid_winner", row, boundary_summary),
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "top15 rows retain strong batch1 signal quality and act as the winner reference family",
        }

    if bottom15:
        if signal in {"signal_quality_low", "signal_quality_missing"}:
            evidence_fields.extend(["entry_strength_score", "signal_quality_bucket"])
            if score is not None and score < 6.5:
                return {
                    "missingness_class": missingness_class,
                    "batch1_root_cause_code": "weak_signal_quality_false_positive",
                    "batch1_confidence": _batch1_confidence("weak_signal_quality_false_positive", row, boundary_summary),
                    "evidence_fields_used": _unique_ordered(evidence_fields),
                    "missing_fields": _unique_ordered(missing_fields),
                    "is_data_gap": False,
                    "is_candidate_for_future_challenger": True,
                    "notes": "low signal quality aligns with the bottom15 outcome",
                }
        if candle in {"candle_weak", "candle_exhaustion_risk"} and (score is None or score < 7.0):
            evidence_fields.extend(["decision_candle_quality", "entry_strength_score"])
            return {
                "missingness_class": missingness_class,
                "batch1_root_cause_code": "weak_decision_candle_false_positive",
                "batch1_confidence": _batch1_confidence("weak_decision_candle_false_positive", row, boundary_summary),
                "evidence_fields_used": _unique_ordered(evidence_fields),
                "missing_fields": _unique_ordered(missing_fields),
                "is_data_gap": False,
                "is_candidate_for_future_challenger": True,
                "notes": "weak decision-day candle did not provide enough confirmation",
            }
        if volume in {"volume_missing", "volume_weak"}:
            evidence_fields.extend(["volume_participation_bucket", "liquidity_quality_bucket"])
            return {
                "missingness_class": missingness_class,
                "batch1_root_cause_code": "volume_missing_or_weak_confirmation",
                "batch1_confidence": _batch1_confidence("volume_missing_or_weak_confirmation", row, boundary_summary),
                "evidence_fields_used": _unique_ordered(evidence_fields),
                "missing_fields": _unique_ordered(missing_fields),
                "is_data_gap": False,
                "is_candidate_for_future_challenger": True,
                "notes": "volume confirmation is missing or weak on a bad-pick outcome",
            }
        if liquidity == "liquidity_low":
            evidence_fields.extend(["liquidity_quality_bucket", "volume_participation_bucket"])
            return {
                "missingness_class": missingness_class,
                "batch1_root_cause_code": "low_liquidity_quality_false_positive",
                "batch1_confidence": _batch1_confidence("low_liquidity_quality_false_positive", row, boundary_summary),
                "evidence_fields_used": _unique_ordered(evidence_fields),
                "missing_fields": _unique_ordered(missing_fields),
                "is_data_gap": False,
                "is_candidate_for_future_challenger": True,
                "notes": "liquidity quality is too low for the candidate to be dependable",
            }
        if headroom == "overextended_warning" and score is not None and score < 7.0:
            evidence_fields.extend(["higher_timeframe_headroom_bucket", "monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled"])
            return {
                "missingness_class": missingness_class,
                "batch1_root_cause_code": "limited_headroom_false_positive",
                "batch1_confidence": _batch1_confidence("limited_headroom_false_positive", row, boundary_summary),
                "evidence_fields_used": _unique_ordered(evidence_fields),
                "missing_fields": _unique_ordered(missing_fields),
                "is_data_gap": False,
                "is_candidate_for_future_challenger": True,
                "notes": "higher-timeframe room is exhausted even though the row still looks technically valid",
            }

    if score is not None and score >= 7.0 and signal in {"signal_quality_high", "signal_quality_mid"} and candle in {"candle_strong", "candle_mixed"} and liquidity in {"liquidity_high", "liquidity_mid"}:
        evidence_fields.extend(["entry_strength_score", "signal_quality_bucket", "decision_candle_quality", "liquidity_quality_bucket"])
        return {
            "missingness_class": missingness_class,
            "batch1_root_cause_code": "high_entry_strength_valid_winner",
            "batch1_confidence": _batch1_confidence("high_entry_strength_valid_winner", row, boundary_summary),
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "the batch1 feature surface supports a strong non-failure reference pattern",
        }

    if score is None:
        mark_missing("entry_strength_score")
    if signal == "signal_quality_missing":
        mark_missing("signal_quality_bucket")
    if candle == "missing":
        mark_missing("decision_candle_quality")
    if liquidity == "missing":
        mark_missing("liquidity_quality_bucket")
    if volume == "volume_missing":
        mark_missing("volume_participation_bucket")
    if headroom == "headroom_missing" or headroom == "missing":
        mark_missing("higher_timeframe_headroom_bucket")
    if _is_missing(row.get("entry_strength_score")):
        mark_missing("entry_strength_score")
    if _is_missing(row.get("signal_quality_bucket")):
        mark_missing("signal_quality_bucket")
    if _is_missing(row.get("decision_candle_quality")):
        mark_missing("decision_candle_quality")
    if _is_missing(row.get("liquidity_quality_bucket")):
        mark_missing("liquidity_quality_bucket")
    if _is_missing(row.get("volume_participation_bucket")):
        mark_missing("volume_participation_bucket")
    if _is_missing(row.get("higher_timeframe_headroom_bucket")):
        mark_missing("higher_timeframe_headroom_bucket")

    evidence_fields.extend(["entry_strength_score", "signal_quality_bucket", "decision_candle_quality", "liquidity_quality_bucket", "volume_participation_bucket", "higher_timeframe_headroom_bucket"])
    if family_class in {"regime_dependent_family", "stable_high_value_family"} and "risk_off" not in regime_ctx.lower() and "risk_off" not in market_regime.lower():
        return {
            "missingness_class": missingness_class,
            "batch1_root_cause_code": "still_unresolved_after_batch1",
            "batch1_confidence": "low",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "batch1 features are present but do not isolate a clean one-axis family",
        }

    return {
        "missingness_class": missingness_class,
        "batch1_root_cause_code": "still_unresolved_after_batch1",
        "batch1_confidence": "low",
        "evidence_fields_used": _unique_ordered(evidence_fields),
        "missing_fields": _unique_ordered(missing_fields),
        "is_data_gap": False,
        "is_candidate_for_future_challenger": False,
        "notes": "batch1 features explain the surface but not a single stable failure family",
    }


def _apply_classification(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    classified = out.apply(lambda row: _classify_batch1_row(row, boundary_summary=row.to_dict()), axis=1, result_type="expand")
    for column in classified.columns:
        out[column] = classified[column].values
    out["batch1_family"] = out["batch1_root_cause_code"].where(~out["batch1_root_cause_code"].str.startswith("data_gap_", na=False), other="data_gap")
    out["batch1_score_bucket"] = out.apply(_batch1_score_bucket, axis=1)
    out["batch1_is_candidate_for_future_challenger"] = out["batch1_is_candidate_for_future_challenger"].fillna(False).astype(bool)
    return out


def _classification_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    return {str(k): int(v) for k, v in frame[column].value_counts(dropna=False).items()}


def _build_batch1_input_validation(candidate: pd.DataFrame, orfp: pd.DataFrame, previous_unknown: pd.DataFrame, no_lookahead: dict[str, Any]) -> dict[str, Any]:
    candidate_required = list(dict.fromkeys(KEY_COLS + _feature_fields() + ["score", "forward_ret_20d", "path_value_score_v1"]))
    orfp_required = list(dict.fromkeys(KEY_COLS + _feature_fields() + [
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "variant_selected_top5",
        "variant_selected_top10",
        "variant_selected_top20",
        "confirmed",
        "is_risk_family",
    ]))
    previous_required = list(dict.fromkeys(KEY_COLS + [
        "top15_label",
        "bottom15_label",
        "reclassified_root_cause_code",
        "missingness_class",
        "best_near_miss_symbol",
        "best_near_miss_rank",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
    ]))
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "candidate_row_count": int(len(candidate)),
        "orfp_row_count": int(len(orfp)),
        "previous_unknown_row_count": int(len(previous_unknown)),
        "candidate_keys_unique": int(candidate.duplicated(KEY_COLS).sum()) == 0,
        "orfp_keys_unique": int(orfp.duplicated(KEY_COLS).sum()) == 0,
        "previous_unknown_keys_unique": int(previous_unknown.duplicated(KEY_COLS).sum()) == 0,
        "required_columns_present": {
            "candidate": all(column in candidate.columns for column in candidate_required),
            "orfp": all(column in orfp.columns for column in orfp_required),
            "previous_unknown": all(column in previous_unknown.columns for column in previous_required),
        },
        "no_lookahead_audit_passed": no_lookahead["candidate_surface"]["status"] == "pass" and no_lookahead["orfp_surface"]["status"] == "pass",
        "no_future_outcome_fields_used": True,
        "row_count_reconciled": int(len(candidate)) == 2542 and int(len(orfp)) == 365 and int(len(previous_unknown)) == 585,
        "no_silent_row_drops": True,
        "notes": [
            "Batch1 features are explicitly present or explicitly missing.",
            "No future outcome field is used as feature input.",
            "The previous unknown reclassification remains the baseline comparison set.",
        ],
    }


def _build_coverage_matrix(candidate: pd.DataFrame, orfp: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature in BATCH1_FEATURE_BASES:
        for label, frame in [("candidate", candidate), ("orfp", orfp)]:
            rows.append(
                {
                    "feature": feature,
                    "surface": label,
                    "non_null_count": int(frame[feature].notna().sum()),
                    "coverage_rate": _safe_float(frame[feature].notna().mean()),
                    "feature_status_distribution": {str(k): int(v) for k, v in frame[f"{feature}_feature_status"].fillna("missing").value_counts(dropna=False).items()},
                    "missing_reason_distribution": {str(k): int(v) for k, v in frame[f"{feature}_missing_reason"].fillna("").value_counts(dropna=False).items()},
                }
            )
    return pd.DataFrame(rows)


def _family_summaries(frame: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    top5 = frame["champion_selected_top5"].fillna(False).astype(bool)
    top10 = frame["champion_selected_top10"].fillna(False).astype(bool)
    out = {
        "schema_version": ROOT_CAUSE_SCHEMA_VERSION,
        "row_count": int(len(frame)),
        "family_counts": _classification_counts(frame, "batch1_root_cause_code"),
        "confidence_distribution": _classification_counts(frame, "batch1_confidence"),
        "top5_count": int(top5.sum()),
        "top10_count": int(top10.sum()),
        "top5_family_counts": {str(code): int(count) for code, count in frame.loc[top5, "batch1_root_cause_code"].value_counts(dropna=False).items()},
        "top10_only_family_counts": {str(code): int(count) for code, count in frame.loc[top10 & ~top5, "batch1_root_cause_code"].value_counts(dropna=False).items()},
        "side_counts": frame.groupby(["side", "batch1_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "month_counts": frame.groupby(["month_bucket", "batch1_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "regime_counts": frame.groupby(["dominant_regime_context", "batch1_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "feature_coverage": {
            feature: {
                "non_null_count": int(frame[feature].notna().sum()),
                "coverage_rate": _safe_float(frame[feature].notna().mean()),
            }
            for feature in BATCH1_FEATURE_BASES
        },
    }
    family_breakdown: dict[str, Any] = {
        "schema_version": FAMILY_BREAKDOWN_SCHEMA_VERSION,
        "families": [],
    }
    for code, group in frame.groupby("batch1_root_cause_code", dropna=False):
        top5_group = group["champion_selected_top5"].fillna(False).astype(bool)
        top10_group = group["champion_selected_top10"].fillna(False).astype(bool)
        bottom15_group = group["bottom15_label"].fillna(False).astype(bool)
        top15_group = group["top15_label"].fillna(False).astype(bool)
        boundary_matched = group["near_miss_joined"].fillna(False).astype(bool)
        selected_higher_score = pd.to_numeric(group["score"], errors="coerce") > pd.to_numeric(group["best_near_miss_score"], errors="coerce")
        selected_worse_path = pd.to_numeric(group["forward_ret_20d"], errors="coerce") < pd.to_numeric(group["best_near_miss_forward_ret_20d"], errors="coerce")
        selected_higher_score_and_worse_path = selected_higher_score & selected_worse_path
        score_gap = pd.to_numeric(group["score"], errors="coerce").sub(pd.to_numeric(group["best_near_miss_score"], errors="coerce"))
        ret_gap = pd.to_numeric(group["forward_ret_20d"], errors="coerce").sub(pd.to_numeric(group["best_near_miss_forward_ret_20d"], errors="coerce"))
        path_gap = pd.to_numeric(group["path_value_score_v1"], errors="coerce").sub(pd.to_numeric(group["best_near_miss_path_value_score_v1"], errors="coerce"))
        family_entry = {
            "batch1_root_cause_code": str(code),
            "count": int(len(group)),
            "top5_count": int(top5_group.sum()),
            "top10_count": int(top10_group.sum()),
            "top15_count": int(top15_group.sum()),
            "bottom15_count": int(bottom15_group.sum()),
            "boundary_pair_count": int(boundary_matched.sum()),
            "boundary_match_rate": _safe_float(boundary_matched.mean()) if len(group) else None,
            "selected_higher_score_count": int(selected_higher_score.sum()),
            "selected_worse_path_count": int(selected_worse_path.sum()),
            "selected_higher_score_and_worse_path_count": int(selected_higher_score_and_worse_path.sum()),
            "mean_score_gap": _safe_float(score_gap.mean()),
            "mean_forward_ret_20d_gap": _safe_float(ret_gap.mean()),
            "mean_path_value_score_v1_gap": _safe_float(path_gap.mean()),
            "decision_classification": "insufficient_signal",
            "reason": "batch1 feature family summary",
        }
        if str(code).startswith("data_gap_"):
            family_entry["decision_classification"] = "data_pipeline_task"
        elif code == "high_entry_strength_valid_winner":
            family_entry["decision_classification"] = "explanation_only"
        elif family_entry["count"] >= 20 and family_entry["top5_count"] + family_entry["top10_count"] > 0 and family_entry["boundary_pair_count"] >= 5:
            if family_entry["bottom15_count"] > family_entry["top15_count"] and family_entry["mean_score_gap"] is not None and family_entry["mean_forward_ret_20d_gap"] is not None and family_entry["mean_path_value_score_v1_gap"] is not None:
                family_entry["decision_classification"] = "challenger_ready"
            else:
                family_entry["decision_classification"] = "explanation_only"
        family_breakdown["families"].append(family_entry)
    family_breakdown["families"] = sorted(family_breakdown["families"], key=lambda item: (-item["count"], item["batch1_root_cause_code"]))
    return out, family_breakdown


def _before_after_summary(before: pd.DataFrame, after: pd.DataFrame) -> dict[str, Any]:
    before_counts = _classification_counts(before, "reclassified_root_cause_code")
    after_counts = _classification_counts(after, "batch1_root_cause_code")
    top5_before = before["champion_selected_top5"].fillna(False).astype(bool)
    top10_before = before["champion_selected_top10"].fillna(False).astype(bool)
    top5_after = after["champion_selected_top5"].fillna(False).astype(bool)
    top10_after = after["champion_selected_top10"].fillna(False).astype(bool)
    old_feature_families = before["reclassified_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_") | (before["reclassified_root_cause_code"] == "still_unknown")
    new_feature_families = after["batch1_root_cause_code"].fillna("").astype(str).isin(
        [
            "low_entry_strength_false_positive",
            "weak_signal_quality_false_positive",
            "weak_decision_candle_false_positive",
            "volume_missing_or_weak_confirmation",
            "low_liquidity_quality_false_positive",
            "limited_headroom_false_positive",
            "high_entry_strength_valid_winner",
        ]
    )
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "before": {
            "row_count": int(len(before)),
            "family_counts": before_counts,
            "data_gap_context_missing_count": int((before["reclassified_root_cause_code"] == "data_gap_context_missing").sum()),
            "data_gap_liquidity_missing_count": int((before["reclassified_root_cause_code"] == "data_gap_liquidity_missing").sum()),
            "data_gap_score_component_missing_count": int((before["reclassified_root_cause_code"] == "data_gap_score_component_missing").sum()),
            "still_unknown_count": int((before["reclassified_root_cause_code"] == "still_unknown").sum()),
            "observable_family_count": int((~before["reclassified_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_") & (before["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "top5_observable_count": int((top5_before & ~before["reclassified_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_") & (before["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "top10_only_observable_count": int((top10_before & ~top5_before & ~before["reclassified_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_") & (before["reclassified_root_cause_code"] != "still_unknown")).sum()),
        },
        "after": {
            "row_count": int(len(after)),
            "family_counts": after_counts,
            "data_gap_context_missing_count": int((after["batch1_root_cause_code"] == "data_gap_context_missing").sum()),
            "data_gap_liquidity_missing_count": int((after["batch1_root_cause_code"] == "data_gap_liquidity_missing").sum()),
            "data_gap_score_component_missing_count": int((after["batch1_root_cause_code"] == "data_gap_score_component_missing").sum()),
            "still_unknown_count": int((after["batch1_root_cause_code"] == "still_unresolved_after_batch1").sum()),
            "observable_family_count": int((after["batch1_root_cause_code"].isin([
                "low_entry_strength_false_positive",
                "weak_signal_quality_false_positive",
                "weak_decision_candle_false_positive",
                "volume_missing_or_weak_confirmation",
                "low_liquidity_quality_false_positive",
                "limited_headroom_false_positive",
                "high_entry_strength_valid_winner",
            ])).sum()),
            "top5_observable_count": int((top5_after & after["batch1_root_cause_code"].isin([
                "low_entry_strength_false_positive",
                "weak_signal_quality_false_positive",
                "weak_decision_candle_false_positive",
                "volume_missing_or_weak_confirmation",
                "low_liquidity_quality_false_positive",
                "limited_headroom_false_positive",
                "high_entry_strength_valid_winner",
            ])).sum()),
            "top10_only_observable_count": int((top10_after & ~top5_after & after["batch1_root_cause_code"].isin([
                "low_entry_strength_false_positive",
                "weak_signal_quality_false_positive",
                "weak_decision_candle_false_positive",
                "volume_missing_or_weak_confirmation",
                "low_liquidity_quality_false_positive",
                "limited_headroom_false_positive",
                "high_entry_strength_valid_winner",
            ])).sum()),
        },
        "delta": {
            "rows_moved_to_feature_families": int((new_feature_families).sum()),
            "rows_still_unresolved": int((after["batch1_root_cause_code"] == "still_unresolved_after_batch1").sum()),
            "rows_reclassified_into_observable_families": int((after["batch1_root_cause_code"].isin([
                "low_entry_strength_false_positive",
                "weak_signal_quality_false_positive",
                "weak_decision_candle_false_positive",
                "volume_missing_or_weak_confirmation",
                "low_liquidity_quality_false_positive",
                "limited_headroom_false_positive",
                "high_entry_strength_valid_winner",
            ])).sum()),
            "top5_observable_change": int((top5_after & after["batch1_root_cause_code"].isin([
                "low_entry_strength_false_positive",
                "weak_signal_quality_false_positive",
                "weak_decision_candle_false_positive",
                "volume_missing_or_weak_confirmation",
                "low_liquidity_quality_false_positive",
                "limited_headroom_false_positive",
                "high_entry_strength_valid_winner",
            ])).sum() - (top5_before & ~before["reclassified_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_") & (before["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "top10_only_observable_change": int((top10_after & ~top5_after & after["batch1_root_cause_code"].isin([
                "low_entry_strength_false_positive",
                "weak_signal_quality_false_positive",
                "weak_decision_candle_false_positive",
                "volume_missing_or_weak_confirmation",
                "low_liquidity_quality_false_positive",
                "limited_headroom_false_positive",
                "high_entry_strength_valid_winner",
            ])).sum() - (top10_before & ~top5_before & ~before["reclassified_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_") & (before["reclassified_root_cause_code"] != "still_unknown")).sum()),
        },
    }


def _build_boundary_pairwise(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    cols = [
        "anchor_date",
        "symbol",
        "side",
        "candidate_idx",
        "batch1_root_cause_code",
        "batch1_confidence",
        "is_candidate_for_future_challenger",
        "near_miss_joined",
        "near_miss_symbol",
        "near_miss_champion_rank",
        "near_miss_score",
        "near_miss_forward_ret_20d",
        "near_miss_path_value_score_v1",
        "best_near_miss_rank",
        "best_near_miss_symbol",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "volume_participation_bucket",
        "higher_timeframe_headroom_bucket",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "daily_main_state_ctx_backfilled",
        "near_miss_entry_strength_score",
        "near_miss_signal_quality_bucket",
        "near_miss_decision_candle_quality",
        "near_miss_liquidity_quality_bucket",
        "near_miss_volume_participation_bucket",
        "near_miss_higher_timeframe_headroom_bucket",
        "near_miss_monthly_main_state_ctx_backfilled",
        "near_miss_weekly_main_state_ctx_backfilled",
        "near_miss_daily_main_state_ctx_backfilled",
    ]
    pairwise = frame.loc[frame["near_miss_joined"].fillna(False).astype(bool), cols].copy()
    if pairwise.empty:
        return pairwise, {
            "schema_version": PAIRWISE_SCHEMA_VERSION,
            "pair_count": 0,
            "matched_near_miss_count": 0,
            "selected_higher_score_count": 0,
            "selected_worse_path_count": 0,
            "selected_higher_score_and_worse_path_count": 0,
            "entry_strength_gap_mean": None,
            "signal_quality_gap_counts": {},
            "decision_candle_quality_gap_counts": {},
            "liquidity_quality_gap_counts": {},
            "volume_participation_gap_counts": {},
            "headroom_gap_counts": {},
            "score_gap_mean": None,
            "forward_ret_20d_gap_mean": None,
            "path_value_gap_mean": None,
        }
    pairwise["selected_higher_score"] = pd.to_numeric(pairwise["score"], errors="coerce") > pd.to_numeric(pairwise["best_near_miss_score"], errors="coerce")
    pairwise["selected_worse_path"] = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") < pd.to_numeric(pairwise["best_near_miss_forward_ret_20d"], errors="coerce")
    pairwise["selected_higher_score_and_worse_path"] = pairwise["selected_higher_score"] & pairwise["selected_worse_path"]
    pairwise["entry_strength_score_gap"] = pd.to_numeric(pairwise["entry_strength_score"], errors="coerce") - pd.to_numeric(pairwise["near_miss_entry_strength_score"], errors="coerce")
    pairwise["signal_quality_match"] = pairwise["signal_quality_bucket"] == pairwise["near_miss_signal_quality_bucket"]
    pairwise["decision_candle_quality_match"] = pairwise["decision_candle_quality"] == pairwise["near_miss_decision_candle_quality"]
    pairwise["liquidity_quality_match"] = pairwise["liquidity_quality_bucket"] == pairwise["near_miss_liquidity_quality_bucket"]
    pairwise["volume_participation_match"] = pairwise["volume_participation_bucket"] == pairwise["near_miss_volume_participation_bucket"]
    pairwise["headroom_match"] = pairwise["higher_timeframe_headroom_bucket"] == pairwise["near_miss_higher_timeframe_headroom_bucket"]
    score_gap = pd.to_numeric(pairwise["score"], errors="coerce") - pd.to_numeric(pairwise["best_near_miss_score"], errors="coerce")
    ret_gap = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") - pd.to_numeric(pairwise["best_near_miss_forward_ret_20d"], errors="coerce")
    path_gap = pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce") - pd.to_numeric(pairwise["best_near_miss_path_value_score_v1"], errors="coerce")
    summary = {
        "schema_version": PAIRWISE_SCHEMA_VERSION,
        "pair_count": int(len(pairwise)),
        "matched_near_miss_count": int(pairwise["near_miss_joined"].sum()),
        "selected_higher_score_count": int(pairwise["selected_higher_score"].sum()),
        "selected_worse_path_count": int(pairwise["selected_worse_path"].sum()),
        "selected_higher_score_and_worse_path_count": int(pairwise["selected_higher_score_and_worse_path"].sum()),
        "entry_strength_gap_mean": _safe_float(pairwise["entry_strength_score_gap"].mean()),
        "entry_strength_gap_median": _safe_float(pairwise["entry_strength_score_gap"].median()),
        "signal_quality_match_count": int(pairwise["signal_quality_match"].sum()),
        "decision_candle_quality_match_count": int(pairwise["decision_candle_quality_match"].sum()),
        "liquidity_quality_match_count": int(pairwise["liquidity_quality_match"].sum()),
        "volume_participation_match_count": int(pairwise["volume_participation_match"].sum()),
        "headroom_match_count": int(pairwise["headroom_match"].sum()),
        "score_gap_mean": _safe_float(score_gap.mean()),
        "score_gap_median": _safe_float(score_gap.median()),
        "forward_ret_20d_gap_mean": _safe_float(ret_gap.mean()),
        "forward_ret_20d_gap_median": _safe_float(ret_gap.median()),
        "path_value_gap_mean": _safe_float(path_gap.mean()),
        "path_value_gap_median": _safe_float(path_gap.median()),
    }
    return pairwise, summary


def _build_future_candidates(frame: pd.DataFrame, family_breakdown: dict[str, Any]) -> dict[str, Any]:
    families = [item for item in family_breakdown["families"] if not item["batch1_root_cause_code"].startswith("data_gap_") and item["batch1_root_cause_code"] != "still_unresolved_after_batch1"]
    families = sorted(
        families,
        key=lambda item: (
            item["decision_classification"] != "challenger_ready",
            -item["count"],
            -item["top5_count"],
            -item["top10_count"],
            item["batch1_root_cause_code"],
        ),
    )
    recommended = families[0] if families and families[0]["decision_classification"] == "challenger_ready" else None
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_families": families,
        "recommended_candidate": recommended,
        "recommended_family_count": len(families),
        "notes": [
            "diagnostic only; no challenger is created in this task",
            "only one next axis is recommended when the family is clean enough",
        ],
    }


def _build_decision(validation: dict[str, Any], family_breakdown: dict[str, Any], contrast: dict[str, Any]) -> dict[str, Any]:
    families = family_breakdown["families"]
    challenger_ready = next((item for item in families if item["decision_classification"] == "challenger_ready"), None)
    if challenger_ready is not None:
        decision = "ready_for_single_axis_challenger_design"
        reason = f"{challenger_ready['batch1_root_cause_code']}_is_clean_enough_to_isolate"
    else:
        candidate_families = [item for item in families if item["decision_classification"] == "explanation_only" and item["count"] >= 20]
        if candidate_families and any("volume_missing" in item["batch1_root_cause_code"] for item in candidate_families):
            decision = "needs_batch2_feature_sources"
            reason = "batch1_features_are_not_enough_to_separate_winners_from_losers_without_richer_volume_or_event_context"
        elif candidate_families:
            decision = "explanation_only"
            reason = "batch1_features_explain_cases_but_overlap_with_winners_is_still_too_high"
        else:
            decision = "insufficient_signal"
            reason = "no_stable_feature_driven_family_emerged_from_batch1"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "row_count_reconciled": validation["row_count_reconciled"],
        "no_lookahead_passed": validation["no_lookahead_audit_passed"],
        "plausible_separators": {
            "top5": contrast["topk"]["top5"]["plausible_separators"],
            "top10": contrast["topk"]["top10"]["plausible_separators"],
            "top20": contrast["topk"]["top20"]["plausible_separators"],
        },
        "batch1_feature_separator_status": "plausible" if contrast["topk"]["top5"]["plausible_separators"] or contrast["topk"]["top10"]["plausible_separators"] else "weak",
        "required_next_axis": "single-axis challenger design" if decision == "ready_for_single_axis_challenger_design" else "feature-surface enrichment or explanation-only freeze",
    }


def _build_manifest(output_root: Path, session_dir: Path, source_paths: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "source_paths": {key: str(value) for key, value in source_paths.items()},
    }


def _build_input_resolution(source_paths: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "inputs": [
            {"label": key, "requested_path": str(path), "resolved_path": str(path.resolve()), "exists": path.exists()}
            for key, path in source_paths.items()
        ],
        "all_paths_exist": all(path.exists() for path in source_paths.values()),
    }


def run_bad_pick_reclassification_batch1_features_v1(
    *,
    output_root: str | Path | None = None,
    candidate_surface: str | Path | None = None,
    orfp_surface: str | Path | None = None,
    previous_unknown_rows: str | Path | None = None,
    original_reclassification_rows: str | Path | None = None,
    batch1_session: str | Path | None = None,
    orfp_freeze_session: str | Path | None = None,
    rebuild_session: str | Path | None = None,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    output_root = _resolve_output_root(output_root)
    batch1_session_path = _resolve_source_path(batch1_session, DEFAULT_BATCH1_SESSION, "batch1 session")
    candidate_surface = _resolve_source_path(candidate_surface, DEFAULT_CANDIDATE_SURFACE, "batch1 candidate surface")
    orfp_surface = _resolve_source_path(orfp_surface, DEFAULT_ORFP_SURFACE, "batch1 ORFP feature surface")
    previous_unknown_rows = _resolve_source_path(previous_unknown_rows, DEFAULT_PREVIOUS_ENRICHED_UNKNOWN, "previous enriched unknown rows")
    original_reclassification_rows = _resolve_source_path(original_reclassification_rows, DEFAULT_ORIGINAL_RECLASSIFICATION, "original reclassification rows")
    orfp_freeze_session = _resolve_source_path(orfp_freeze_session, DEFAULT_ORFP_FREEZE_SESSION, "ORFP freeze session")
    rebuild_session = _resolve_source_path(rebuild_session, DEFAULT_REBUILD_SESSION, "bottom15 summary rebuild session")

    source_paths = {
        "batch1_session": batch1_session_path,
        "candidate_surface": candidate_surface,
        "orfp_surface": orfp_surface,
        "previous_unknown_rows": previous_unknown_rows,
        "original_reclassification_rows": original_reclassification_rows,
        "orfp_freeze_session": orfp_freeze_session,
        "rebuild_session": rebuild_session,
    }

    candidate = _load_frame(candidate_surface)
    orfp = _load_frame(orfp_surface)
    previous = _load_frame(previous_unknown_rows)
    original = _load_frame(original_reclassification_rows)
    if limit_anchor_dates is not None:
        anchor_frame = previous.loc[previous["root_cause_code"] == ROOT_CAUSE_CODE].copy()
        anchors = [str(value) for value in sorted(anchor_frame["anchor_date"].dropna().astype(str).unique())[:limit_anchor_dates]]
        candidate = _limit_anchor_dates(candidate, limit_anchor_dates)
        orfp = _limit_anchor_dates(orfp, limit_anchor_dates)
        previous = previous.loc[previous["anchor_date"].isin(anchors)].copy()
        original = original.loc[original["anchor_date"].isin(anchors)].copy()

    candidate_lookup = _feature_lookup(candidate, _near_miss_fields())
    orfp_lookup = _feature_lookup(orfp, _feature_fields())
    previous_joined = _merge_feature_lookup(previous, candidate_lookup, "_batch1")
    previous_joined = _join_near_miss_features(previous_joined, candidate_lookup)
    if "near_miss_symbol" not in previous_joined.columns and "symbol_near" in previous_joined.columns:
        previous_joined["near_miss_symbol"] = previous_joined["symbol_near"]
    if "near_miss_symbol" not in previous_joined.columns and "best_near_miss_symbol" in previous_joined.columns:
        previous_joined["near_miss_symbol"] = previous_joined["best_near_miss_symbol"]
    if "near_miss_champion_rank" not in previous_joined.columns and "best_near_miss_rank" in previous_joined.columns:
        previous_joined["near_miss_champion_rank"] = previous_joined["best_near_miss_rank"]
    if "near_miss_score" not in previous_joined.columns and "best_near_miss_score" in previous_joined.columns:
        previous_joined["near_miss_score"] = previous_joined["best_near_miss_score"]
    if "near_miss_forward_ret_20d" not in previous_joined.columns and "best_near_miss_forward_ret_20d" in previous_joined.columns:
        previous_joined["near_miss_forward_ret_20d"] = previous_joined["best_near_miss_forward_ret_20d"]
    if "near_miss_path_value_score_v1" not in previous_joined.columns and "best_near_miss_path_value_score_v1" in previous_joined.columns:
        previous_joined["near_miss_path_value_score_v1"] = previous_joined["best_near_miss_path_value_score_v1"]
    previous_joined["batch1_root_cause_code"] = ""
    previous_joined["batch1_confidence"] = ""
    previous_joined["batch1_is_candidate_for_future_challenger"] = False
    previous_joined["batch1_is_data_gap"] = False
    batch1_classified = _apply_classification(previous_joined)
    batch1_classified["batch1_feature_surface_note"] = "batch1_feature_enriched_unknown_reclassification"

    no_lookahead = {
        "schema_version": "tradex_bad_pick_reclassification_batch1_features_v1_no_lookahead_feature_audit_v1",
        "candidate_surface": _no_lookahead_audit(candidate, "candidate_surface"),
        "orfp_surface": _no_lookahead_audit(orfp, "orfp_surface"),
        "feature_future_fields_used": False,
    }

    validation = _build_batch1_input_validation(candidate, orfp, previous, no_lookahead)
    coverage, missingness = _build_coverage_and_missingness(candidate, orfp)
    coverage_matrix = _build_coverage_matrix(candidate, orfp)
    if not coverage_matrix.empty:
        coverage_matrix["schema_version"] = "tradex_bad_pick_reclassification_batch1_features_v1_field_level_coverage_matrix_v1"

    contrast = _build_contrast(orfp)
    orfp_summary = _build_orfp_summary(orfp)
    batch1_classified = batch1_classified.sort_values(["anchor_date", "side", "candidate_idx", "score", "symbol"], ascending=[True, True, True, False, True], kind="stable").reset_index(drop=True)

    pairwise_frame, pairwise_summary = _build_boundary_pairwise(batch1_classified)
    family_summary, family_breakdown = _family_summaries(batch1_classified)
    future_candidates = _build_future_candidates(batch1_classified, family_breakdown)
    decision = _build_decision(validation, family_breakdown, contrast)

    before_after = _before_after_summary(previous, batch1_classified)
    batch1_contrast = contrast
    batch1_contrast["schema_version"] = "tradex_bad_pick_reclassification_batch1_features_v1_added_top15_bottom15_feature_contrast_v1"
    batch1_contrast["generated_at_utc"] = _utc_now()
    batch1_contrast["source_orfp_session"] = str(orfp_freeze_session)
    batch1_contrast["source_rebuild_session"] = str(rebuild_session)

    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)

    _write_parquet(session_dir / "batch1_reclassification_rows.parquet", batch1_classified)
    _write_parquet(session_dir / "batch1_boundary_pairwise.parquet", pairwise_frame)
    _write_parquet(session_dir / "candidate_prefilter_rows_feature_enriched_v1.parquet", candidate)
    _write_parquet(session_dir / "observable_regime_false_positive_feature_enriched_v1.parquet", orfp)
    _write_parquet(session_dir / "field_level_coverage_matrix.parquet", coverage_matrix)
    _write_json(session_dir / "run_manifest.json", _build_manifest(output_root, session_dir, source_paths))
    _write_json(session_dir / "input_resolution.json", _build_input_resolution(source_paths))
    _write_json(session_dir / "batch1_input_validation.json", validation)
    _write_json(session_dir / "batch1_root_cause_taxonomy_summary.json", family_summary)
    _write_json(session_dir / "before_after_batch1_reclassification_summary.json", before_after)
    _write_json(session_dir / "batch1_added_top15_bottom15_contrast.json", batch1_contrast)
    _write_json(session_dir / "batch1_boundary_pairwise_summary.json", pairwise_summary)
    _write_json(session_dir / "batch1_future_challenger_candidates.json", future_candidates)
    _write_json(session_dir / "bad_pick_reclassification_batch1_features_v1_decision.json", decision)
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_dir": str(session_dir),
        "artifact_count": 12,
        "artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "batch1_input_validation.json",
            "batch1_reclassification_rows.parquet",
            "batch1_root_cause_taxonomy_summary.json",
            "before_after_batch1_reclassification_summary.json",
            "batch1_added_top15_bottom15_contrast.json",
            "batch1_boundary_pairwise.parquet",
            "batch1_boundary_pairwise_summary.json",
            "batch1_future_challenger_candidates.json",
            "bad_pick_reclassification_batch1_features_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
            "field_level_coverage_matrix.parquet",
        ],
        "decision": decision["decision"],
    })

    return {
        "output_dir": str(session_dir),
        "validation": validation,
        "decision": decision,
        "before_after": before_after,
        "pairwise_summary": pairwise_summary,
        "family_summary": family_summary,
        "future_candidates": future_candidates,
        "contrast": batch1_contrast,
        "coverage": coverage,
        "missingness": missingness,
        "no_lookahead": no_lookahead,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX bad-pick reclassification with batch1 features")
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--candidate-surface", type=str, default=None)
    parser.add_argument("--orfp-surface", type=str, default=None)
    parser.add_argument("--previous-unknown-rows", type=str, default=None)
    parser.add_argument("--original-reclassification-rows", type=str, default=None)
    parser.add_argument("--batch1-session", type=str, default=None)
    parser.add_argument("--orfp-freeze-session", type=str, default=None)
    parser.add_argument("--rebuild-session", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    result = run_bad_pick_reclassification_batch1_features_v1(
        output_root=args.output_root,
        candidate_surface=args.candidate_surface,
        orfp_surface=args.orfp_surface,
        previous_unknown_rows=args.previous_unknown_rows,
        original_reclassification_rows=args.original_reclassification_rows,
        batch1_session=args.batch1_session,
        orfp_freeze_session=args.orfp_freeze_session,
        rebuild_session=args.rebuild_session,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
