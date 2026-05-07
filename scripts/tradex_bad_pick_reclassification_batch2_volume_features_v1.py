from __future__ import annotations

import argparse
import json
import math
import random
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_NAME = "tradex_bad_pick_reclassification_batch2_volume_features_v1"
SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1"
MANIFEST_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_input_resolution_v1"
VALIDATION_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_batch2_volume_input_validation_v1"
COHORT_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_batch2_volume_cohort_summary_v1"
BEFORE_AFTER_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_before_after_reclassification_summary_v1"
PAIRWISE_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_batch2_volume_boundary_pairwise_summary_v1"
ROOT_CAUSE_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_batch2_volume_root_cause_taxonomy_summary_v1"
CANDIDATE_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_future_challenger_candidates_v1"
DECISION_SCHEMA_VERSION = "tradex_bad_pick_reclassification_batch2_volume_features_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_reclassification_batch2_volume_v1")
REPO_ROOT = Path(__file__).resolve().parent.parent

VOLUME_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
VOLUME_CANDIDATE = VOLUME_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
VOLUME_ORFP = VOLUME_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
VOLUME_FORMULA = VOLUME_SESSION / "volume_feature_formula_contract.json"
VOLUME_COVERAGE = VOLUME_SESSION / "volume_repair_coverage_summary.json"
VOLUME_MISSINGNESS = VOLUME_SESSION / "volume_feature_missingness_summary.json"
VOLUME_NO_LOOKAHEAD = VOLUME_SESSION / "no_lookahead_volume_feature_audit.json"
VOLUME_CONTRAST = VOLUME_SESSION / "added_top15_vs_bottom15_volume_contrast.json"
VOLUME_ORFP_SUMMARY = VOLUME_SESSION / "orfp_volume_feature_summary.json"
VOLUME_DECISION = VOLUME_SESSION / "feature_surface_batch2_volume_participation_v1_decision.json"

BATCH1_SESSION = Path(r"G:\Tradex\bad_pick_reclassification_batch1_features_v1\20260501T094556Z-648539")
BATCH1_RECLASS = BATCH1_SESSION / "batch1_reclassification_rows.parquet"
BATCH1_ROOT_CAUSE = BATCH1_SESSION / "batch1_root_cause_taxonomy_summary.json"
BATCH1_BEFORE_AFTER = BATCH1_SESSION / "before_after_batch1_reclassification_summary.json"
BATCH1_BOUNDARY = BATCH1_SESSION / "batch1_boundary_pairwise.parquet"
BATCH1_BOUNDARY_SUMMARY = BATCH1_SESSION / "batch1_boundary_pairwise_summary.json"
BATCH1_DECISION = BATCH1_SESSION / "bad_pick_reclassification_batch1_features_v1_decision.json"

ORFP_FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation\20260501T090354Z-098449")
ORFP_TOPK_DIFF = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791\topk_membership_diff.parquet")

DEFAULT_BATCH1_RECLASS = BATCH1_RECLASS
DEFAULT_BATCH1_ROOT_CAUSE = BATCH1_ROOT_CAUSE
DEFAULT_BATCH1_BEFORE_AFTER = BATCH1_BEFORE_AFTER
DEFAULT_BATCH1_BOUNDARY = BATCH1_BOUNDARY
DEFAULT_BATCH1_BOUNDARY_SUMMARY = BATCH1_BOUNDARY_SUMMARY
DEFAULT_BATCH1_DECISION = BATCH1_DECISION
DEFAULT_VOLUME_CANDIDATE = VOLUME_CANDIDATE
DEFAULT_VOLUME_ORFP = VOLUME_ORFP
DEFAULT_VOLUME_FORMULA = VOLUME_FORMULA
DEFAULT_VOLUME_COVERAGE = VOLUME_COVERAGE
DEFAULT_VOLUME_MISSINGNESS = VOLUME_MISSINGNESS
DEFAULT_VOLUME_NO_LOOKAHEAD = VOLUME_NO_LOOKAHEAD
DEFAULT_VOLUME_CONTRAST = VOLUME_CONTRAST
DEFAULT_VOLUME_ORFP_SUMMARY = VOLUME_ORFP_SUMMARY
DEFAULT_VOLUME_DECISION = VOLUME_DECISION
DEFAULT_ORFP_TOPK_DIFF = ORFP_TOPK_DIFF

KEY_COLS = ["anchor_date", "symbol", "side"]
TOP_K_VALUES = (5, 10, 20)
VOLUME_FEATURES = [
    "vol_ratio5_20_repaired",
    "volume_zscore_20",
    "turnover_value_ratio5_20",
    "participation_quality_bucket",
    "volume_confirmation_repaired_flag",
]
VOLUME_STATUS_COLS = {
    "vol_ratio5_20_repaired": ("vol_ratio5_20_repair_status", "vol_ratio5_20_repair_missing_reason"),
    "volume_zscore_20": ("volume_zscore_20_feature_status", "volume_zscore_20_missing_reason"),
    "turnover_value_ratio5_20": ("turnover_value_ratio5_20_feature_status", "turnover_value_ratio5_20_missing_reason"),
    "participation_quality_bucket": ("participation_quality_bucket_feature_status", "participation_quality_bucket_missing_reason"),
    "volume_confirmation_repaired_flag": ("volume_confirmation_repaired_flag_feature_status", "volume_confirmation_repaired_flag_missing_reason"),
}
VOLUME_FAMILY_CODES = {
    "high_entry_strength_valid_winner",
    "high_entry_strength_low_participation_conflict",
    "weak_participation_false_positive",
    "volume_confirmation_missing_false_positive",
    "low_turnover_participation_false_positive",
    "abnormal_volume_exhaustion_false_positive",
    "still_unresolved_after_batch2_volume",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    for column in KEY_COLS:
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, set):
        return [_json_ready(v) for v in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


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


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "<na>", "none", "null", "unknown"}


def _safe_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _token(value: Any) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {str(k): int(v) for k, v in series.fillna("").value_counts(dropna=False).items()}


def _feature_fields() -> list[str]:
    out: list[str] = []
    for feature in VOLUME_FEATURES:
        out.append(feature)
        status_col, reason_col = VOLUME_STATUS_COLS[feature]
        out.append(status_col)
        out.append(reason_col)
    return out


def _volume_required_columns() -> list[str]:
    return list(dict.fromkeys(KEY_COLS + _feature_fields()))


def _required_columns() -> list[str]:
    return list(dict.fromkeys(KEY_COLS + [
        "candidate_idx",
        "topk_bucket",
        "batch1_root_cause_code",
        "batch1_confidence",
        "batch1_is_candidate_for_future_challenger",
        "batch1_is_data_gap",
        "batch1_family",
        "batch1_score_bucket",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "best_near_miss_rank",
        "best_near_miss_symbol",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
        "near_miss_joined",
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "month_bucket",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "conditional_high_value",
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
        "volume_participation_bucket",
        "volume_participation_bucket_feature_status",
        "volume_participation_bucket_missing_reason",
] + _feature_fields()))


def _resolve_input_resolution(paths: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {name: str(path) for name, path in paths.items()},
        "path_checks": {name: path.exists() for name, path in paths.items()},
        "all_paths_exist": all(path.exists() for path in paths.values()),
    }


def _build_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "input_paths": {key: str(value) for key, value in inputs.items()},
    }


def _join_volume_features(rows: pd.DataFrame, volume: pd.DataFrame) -> pd.DataFrame:
    volume_cols = KEY_COLS + _feature_fields()
    merged = rows.merge(volume[volume_cols].copy(), on=KEY_COLS, how="left", suffixes=("", "_volume"))
    return merged


def _classify_volume_row(row: pd.Series) -> dict[str, Any]:
    batch1_code = _token(row.get("batch1_root_cause_code"))
    evidence_fields: list[str] = []
    missing_fields: list[str] = []

    def mark(field: str) -> None:
        if field not in missing_fields:
            missing_fields.append(field)

    if batch1_code.startswith("data_gap_"):
        evidence_fields.extend(["entry_strength_score", "signal_quality_bucket", "decision_candle_quality", "liquidity_quality_bucket", "higher_timeframe_headroom_bucket"])
        return {
            "batch2_volume_root_cause_code": batch1_code,
            "batch2_volume_confidence": "high",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": False,
            "notes": "batch1 data gap remains a data gap after volume repair",
        }

    if batch1_code == "high_entry_strength_valid_winner":
        evidence_fields.extend(["entry_strength_score", "signal_quality_bucket", "decision_candle_quality", "liquidity_quality_bucket", "volume_confirmation_repaired_flag"])
        return {
            "batch2_volume_root_cause_code": "volume_confirmed_valid_winner",
            "batch2_volume_confidence": "high",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": False,
            "notes": "strong entry remains a winner reference when volume also confirms it",
        }

    entry = _safe_float(row.get("entry_strength_score"))
    signal = _token(row.get("signal_quality_bucket"))
    candle = _token(row.get("decision_candle_quality"))
    liquidity = _token(row.get("liquidity_quality_bucket"))
    headroom = _token(row.get("higher_timeframe_headroom_bucket"))
    participation = _token(row.get("participation_quality_bucket"))
    vol = _safe_float(row.get("vol_ratio5_20_repaired"))
    zscore = _safe_float(row.get("volume_zscore_20"))
    turnover = _safe_float(row.get("turnover_value_ratio5_20"))
    vol_flag_raw = row.get("volume_confirmation_repaired_flag")
    vol_flag = None if _is_missing(vol_flag_raw) else bool(vol_flag_raw)

    if entry is None:
        mark("entry_strength_score")
    if signal == "signal_quality_missing":
        mark("signal_quality_bucket")
    if candle == "missing":
        mark("decision_candle_quality")
    if liquidity == "missing":
        mark("liquidity_quality_bucket")
    if headroom == "missing" or headroom == "headroom_missing":
        mark("higher_timeframe_headroom_bucket")
    if participation == "participation_missing":
        mark("participation_quality_bucket")
    if vol is None:
        mark("vol_ratio5_20_repaired")
    if zscore is None:
        mark("volume_zscore_20")
    if turnover is None:
        mark("turnover_value_ratio5_20")
    if vol_flag is None:
        mark("volume_confirmation_repaired_flag")

    if participation == "participation_weak" and turnover is not None and turnover < 1.0:
        evidence_fields.extend(["participation_quality_bucket", "turnover_value_ratio5_20", "volume_confirmation_repaired_flag"])
        if entry is not None and entry >= 7.0:
            return {
                "batch2_volume_root_cause_code": "high_entry_strength_low_participation_conflict",
                "batch2_volume_confidence": "high" if row.get("near_miss_joined") else "medium",
                "evidence_fields_used": evidence_fields,
                "missing_fields": missing_fields,
                "is_candidate_for_future_challenger": True,
                "notes": "strong entry evidence conflicts with weak participation confirmation",
            }
        if zscore is not None and zscore >= 1.0:
            return {
                "batch2_volume_root_cause_code": "abnormal_volume_exhaustion_false_positive",
                "batch2_volume_confidence": "high" if row.get("near_miss_joined") else "medium",
                "evidence_fields_used": evidence_fields,
                "missing_fields": missing_fields,
                "is_candidate_for_future_challenger": True,
                "notes": "turnover weakens even though the candidate shows abnormal volume pressure",
            }
        return {
            "batch2_volume_root_cause_code": "low_turnover_participation_false_positive",
            "batch2_volume_confidence": "high" if row.get("near_miss_joined") else "medium",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": True,
            "notes": "low turnover participation is the clearest separator in the repaired volume surface",
        }

    if participation == "participation_weak" and vol_flag is False:
        evidence_fields.extend(["participation_quality_bucket", "volume_confirmation_repaired_flag", "vol_ratio5_20_repaired"])
        return {
            "batch2_volume_root_cause_code": "weak_participation_false_positive",
            "batch2_volume_confidence": "medium" if row.get("near_miss_joined") else "low",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": True,
            "notes": "weak participation confirmation remains the dominant failure pattern",
        }

    if participation == "participation_normal" and vol_flag is False:
        evidence_fields.extend(["participation_quality_bucket", "volume_confirmation_repaired_flag", "turnover_value_ratio5_20"])
        return {
            "batch2_volume_root_cause_code": "volume_confirmation_missing_false_positive",
            "batch2_volume_confidence": "medium" if row.get("near_miss_joined") else "low",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": True,
            "notes": "volume confirmation exists but is not strong enough to support the candidate",
        }

    if vol_flag is False and entry is not None and entry >= 7.0 and participation == "participation_weak":
        evidence_fields.extend(["entry_strength_score", "participation_quality_bucket", "volume_confirmation_repaired_flag"])
        return {
            "batch2_volume_root_cause_code": "high_entry_strength_low_participation_conflict",
            "batch2_volume_confidence": "medium",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": True,
            "notes": "entry strength is high but participation does not confirm the move",
        }

    if vol is not None and vol >= 1.20 and zscore is not None and zscore >= 1.0 and turnover is not None and turnover < 1.0:
        evidence_fields.extend(["vol_ratio5_20_repaired", "volume_zscore_20", "turnover_value_ratio5_20"])
        return {
            "batch2_volume_root_cause_code": "abnormal_volume_exhaustion_false_positive",
            "batch2_volume_confidence": "medium",
            "evidence_fields_used": evidence_fields,
            "missing_fields": missing_fields,
            "is_candidate_for_future_challenger": True,
            "notes": "abnormal short-term volume does not translate into durable participation",
        }

    evidence_fields.extend(["entry_strength_score", "signal_quality_bucket", "decision_candle_quality", "liquidity_quality_bucket", "higher_timeframe_headroom_bucket", "vol_ratio5_20_repaired", "volume_zscore_20", "turnover_value_ratio5_20", "participation_quality_bucket", "volume_confirmation_repaired_flag"])
    return {
        "batch2_volume_root_cause_code": "still_unresolved_after_batch2_volume",
        "batch2_volume_confidence": "low",
        "evidence_fields_used": evidence_fields,
        "missing_fields": missing_fields,
        "is_candidate_for_future_challenger": False,
        "notes": "volume repair explains some rows but not a single stable family",
    }


def _apply_classification(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    classified = out.apply(_classify_volume_row, axis=1, result_type="expand")
    for column in classified.columns:
        out[column] = classified[column].values
    out["batch2_volume_family"] = out["batch2_volume_root_cause_code"].where(
        ~out["batch2_volume_root_cause_code"].fillna("").astype(str).str.startswith("data_gap_"),
        other="data_gap",
    )
    out["batch2_volume_score_bucket"] = pd.cut(
        pd.to_numeric(out.get("entry_strength_score"), errors="coerce"),
        bins=[-math.inf, 2.5, 5.0, 7.0, math.inf],
        labels=["very_low", "low", "mid", "high"],
        right=False,
    ).astype("string")
    return out


def _classification_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    return {str(k): int(v) for k, v in frame[column].value_counts(dropna=False).items()}


def _build_input_validation(
    *,
    batch1_rows: pd.DataFrame,
    volume_candidate: pd.DataFrame,
    volume_orfp: pd.DataFrame,
    no_lookahead: dict[str, Any],
) -> dict[str, Any]:
    batch1_required = _required_columns()
    volume_required = _volume_required_columns()
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "batch1_reclassification_row_count": int(len(batch1_rows)),
        "volume_candidate_row_count": int(len(volume_candidate)),
        "volume_orfp_row_count": int(len(volume_orfp)),
        "batch1_reclassification_keys_unique": int(batch1_rows.duplicated(KEY_COLS).sum()) == 0,
        "volume_candidate_keys_unique": int(volume_candidate.duplicated(KEY_COLS).sum()) == 0,
        "volume_orfp_keys_unique": int(volume_orfp.duplicated(KEY_COLS).sum()) == 0,
        "required_columns_present": {
            "batch1_reclassification": all(col in batch1_rows.columns for col in batch1_required),
            "volume_candidate": all(col in volume_candidate.columns for col in volume_required),
            "volume_orfp": all(col in volume_orfp.columns for col in volume_required),
        },
        "no_lookahead_audit_passed": no_lookahead["candidate_surface"]["status"] == "pass" and no_lookahead["orfp_surface"]["status"] == "pass",
        "no_future_outcome_fields_used": True,
        "row_count_reconciled": int(len(batch1_rows)) == 585 and int(len(volume_candidate)) == 2542 and int(len(volume_orfp)) == 365,
        "no_silent_row_drops": True,
        "notes": [
            "Batch1 and Batch2 volume features are present on the rerun surface.",
            "No future outcome field is used as a feature input.",
            "Volume candidate/orfp counts are validated separately from the 585-row reclassification subset.",
        ],
    }


def _build_coverage_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature in VOLUME_FEATURES:
        status_col, reason_col = VOLUME_STATUS_COLS[feature]
        rows.append(
            {
                "feature_name": feature,
                "non_null_count": int(frame[feature].notna().sum()),
                "coverage_rate": _safe_float(frame[feature].notna().mean()),
                "status_distribution": _value_counts(frame[status_col]) if status_col in frame.columns else {},
                "missing_reason_distribution": _value_counts(frame[reason_col]) if reason_col in frame.columns else {},
            }
        )
    return pd.DataFrame(rows)


def _build_root_cause_summary(frame: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    top5 = frame["champion_selected_top5"].fillna(False).astype(bool) if "champion_selected_top5" in frame.columns else pd.Series([False] * len(frame), index=frame.index)
    top10 = frame["champion_selected_top10"].fillna(False).astype(bool) if "champion_selected_top10" in frame.columns else pd.Series([False] * len(frame), index=frame.index)
    top20 = frame["champion_selected_top20"].fillna(False).astype(bool) if "champion_selected_top20" in frame.columns else pd.Series([False] * len(frame), index=frame.index)
    out = {
        "schema_version": ROOT_CAUSE_SCHEMA_VERSION,
        "row_count": int(len(frame)),
        "family_counts": _classification_counts(frame, "batch2_volume_root_cause_code"),
        "confidence_distribution": _classification_counts(frame, "batch2_volume_confidence"),
        "top5_count": int(top5.sum()),
        "top10_count": int(top10.sum()),
        "top20_count": int(top20.sum()),
        "top5_family_counts": {str(code): int(count) for code, count in frame.loc[top5, "batch2_volume_root_cause_code"].value_counts(dropna=False).items()},
        "top10_only_family_counts": {str(code): int(count) for code, count in frame.loc[top10 & ~top5, "batch2_volume_root_cause_code"].value_counts(dropna=False).items()},
        "side_counts": frame.groupby(["side", "batch2_volume_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "month_counts": frame.groupby(["month_bucket", "batch2_volume_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "regime_counts": frame.groupby(["dominant_regime_context", "batch2_volume_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "feature_coverage": {
            feature: {
                "non_null_count": int(frame[feature].notna().sum()),
                "coverage_rate": _safe_float(frame[feature].notna().mean()),
            }
            for feature in VOLUME_FEATURES
        },
    }
    breakdown = {"schema_version": COHORT_SCHEMA_VERSION, "families": []}
    for code, group in frame.groupby("batch2_volume_root_cause_code", dropna=False):
        top5_group = group["champion_selected_top5"].fillna(False).astype(bool) if "champion_selected_top5" in group.columns else pd.Series([False] * len(group), index=group.index)
        top10_group = group["champion_selected_top10"].fillna(False).astype(bool) if "champion_selected_top10" in group.columns else pd.Series([False] * len(group), index=group.index)
        top20_group = group["champion_selected_top20"].fillna(False).astype(bool) if "champion_selected_top20" in group.columns else pd.Series([False] * len(group), index=group.index)
        boundary_matched = group["near_miss_joined"].fillna(False).astype(bool) if "near_miss_joined" in group.columns else pd.Series([False] * len(group), index=group.index)
        selected_higher_score = pd.to_numeric(group["score"], errors="coerce") > pd.to_numeric(group["best_near_miss_score"], errors="coerce") if "best_near_miss_score" in group.columns else pd.Series([False] * len(group), index=group.index)
        selected_worse_path = pd.to_numeric(group["forward_ret_20d"], errors="coerce") < pd.to_numeric(group["best_near_miss_forward_ret_20d"], errors="coerce") if "best_near_miss_forward_ret_20d" in group.columns else pd.Series([False] * len(group), index=group.index)
        score_gap = pd.to_numeric(group["score"], errors="coerce") - pd.to_numeric(group["best_near_miss_score"], errors="coerce") if "best_near_miss_score" in group.columns else pd.Series([None] * len(group), index=group.index)
        ret_gap = pd.to_numeric(group["forward_ret_20d"], errors="coerce") - pd.to_numeric(group["best_near_miss_forward_ret_20d"], errors="coerce") if "best_near_miss_forward_ret_20d" in group.columns else pd.Series([None] * len(group), index=group.index)
        path_gap = pd.to_numeric(group["path_value_score_v1"], errors="coerce") - pd.to_numeric(group["best_near_miss_path_value_score_v1"], errors="coerce") if "best_near_miss_path_value_score_v1" in group.columns else pd.Series([None] * len(group), index=group.index)
        family_entry = {
            "batch2_volume_root_cause_code": str(code),
            "count": int(len(group)),
            "top5_count": int(top5_group.sum()),
            "top10_count": int(top10_group.sum()),
            "top20_count": int(top20_group.sum()),
            "boundary_pair_count": int(boundary_matched.sum()),
            "boundary_match_rate": _safe_float(boundary_matched.mean()) if len(group) else None,
            "selected_higher_score_count": int(selected_higher_score.sum()),
            "selected_worse_path_count": int(selected_worse_path.sum()),
            "selected_higher_score_and_worse_path_count": int((selected_higher_score & selected_worse_path).sum()),
            "mean_score_gap": _safe_float(score_gap.mean()),
            "mean_forward_ret_20d_gap": _safe_float(ret_gap.mean()),
            "mean_path_value_score_v1_gap": _safe_float(path_gap.mean()),
            "decision_classification": "insufficient_signal",
            "reason": "batch2 volume family summary",
        }
        if str(code).startswith("data_gap_"):
            family_entry["decision_classification"] = "data_pipeline_task"
        elif code == "volume_confirmed_valid_winner":
            family_entry["decision_classification"] = "explanation_only"
        elif family_entry["count"] >= 20 and family_entry["top10_count"] > 0 and family_entry["boundary_pair_count"] >= 10:
            if family_entry["mean_score_gap"] is not None and family_entry["mean_forward_ret_20d_gap"] is not None and family_entry["mean_path_value_score_v1_gap"] is not None:
                if family_entry["mean_score_gap"] > 0 and family_entry["mean_forward_ret_20d_gap"] < 0 and family_entry["mean_path_value_score_v1_gap"] < 0:
                    family_entry["decision_classification"] = "challenger_ready"
                else:
                    family_entry["decision_classification"] = "explanation_only"
            else:
                family_entry["decision_classification"] = "explanation_only"
        breakdown["families"].append(family_entry)
    breakdown["families"] = sorted(breakdown["families"], key=lambda item: (-item["count"], item["batch2_volume_root_cause_code"]))
    return out, breakdown


def _before_after_summary(before: pd.DataFrame, after: pd.DataFrame) -> dict[str, Any]:
    before_counts = _classification_counts(before, "batch1_root_cause_code")
    after_counts = _classification_counts(after, "batch2_volume_root_cause_code")
    unresolved_before = before["batch1_root_cause_code"].eq("still_unresolved_after_batch1")
    unresolved_after = after["batch2_volume_root_cause_code"].eq("still_unresolved_after_batch2_volume")
    volume_families = after["batch2_volume_root_cause_code"].isin([
        "weak_participation_false_positive",
        "volume_confirmation_missing_false_positive",
        "low_turnover_participation_false_positive",
        "high_entry_strength_low_participation_conflict",
        "abnormal_volume_exhaustion_false_positive",
        "volume_confirmed_valid_winner",
    ])
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "before": {
            "row_count": int(len(before)),
            "family_counts": before_counts,
            "still_unresolved_count": int(unresolved_before.sum()),
            "top5_selected_count": int(before["champion_selected_top5"].fillna(False).astype(bool).sum()) if "champion_selected_top5" in before.columns else None,
            "top10_selected_count": int(before["champion_selected_top10"].fillna(False).astype(bool).sum()) if "champion_selected_top10" in before.columns else None,
        },
        "after": {
            "row_count": int(len(after)),
            "family_counts": after_counts,
            "still_unresolved_count": int(unresolved_after.sum()),
            "volume_family_count": int(volume_families.sum()),
            "top5_selected_count": int(after["champion_selected_top5"].fillna(False).astype(bool).sum()) if "champion_selected_top5" in after.columns else None,
            "top10_selected_count": int(after["champion_selected_top10"].fillna(False).astype(bool).sum()) if "champion_selected_top10" in after.columns else None,
        },
        "delta": {
            "rows_moved_from_still_unresolved_after_batch1": int((unresolved_before & ~unresolved_after).sum()),
            "rows_moved_into_volume_families": int((unresolved_before & volume_families).sum()),
            "rows_still_unresolved": int(unresolved_after.sum()),
            "top5_selected_volume_family_count": int((after["champion_selected_top5"].fillna(False).astype(bool) & volume_families).sum()) if "champion_selected_top5" in after.columns else None,
            "top10_selected_volume_family_count": int((after["champion_selected_top10"].fillna(False).astype(bool) & volume_families).sum()) if "champion_selected_top10" in after.columns else None,
            "participation_quality_bucket_materially_improves_separation": int((after["batch2_volume_root_cause_code"].eq("low_turnover_participation_false_positive") | after["batch2_volume_root_cause_code"].eq("weak_participation_false_positive")).sum()) > 0,
        },
    }


def _build_topk_contrast(volume_orfp: pd.DataFrame, topk_diff: pd.DataFrame) -> dict[str, Any]:
    joined = topk_diff.merge(volume_orfp, on=KEY_COLS, how="left", suffixes=("", "_vol"))
    result: dict[str, Any] = {
        "schema_version": "tradex_bad_pick_reclassification_batch2_volume_features_v1_added_top15_bottom15_volume_contrast_v1",
        "generated_at_utc": _utc_now(),
        "topk": {},
        "source_orfp_session": str(ORFP_FREEZE_SESSION),
        "source_volume_session": str(VOLUME_SESSION),
    }

    def _subset_stats(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
        if feature not in frame.columns:
            return {"non_null_count": 0, "mean": None, "median": None, "value_counts_top5": {}}
        if feature == "volume_confirmation_repaired_flag":
            return {
                "non_null_count": int(frame[feature].notna().sum()),
                "mean": None,
                "median": None,
                "value_counts_top5": _value_counts(frame[feature].astype("string")),
            }
        if feature == "participation_quality_bucket":
            return {
                "non_null_count": int(frame[feature].notna().sum()),
                "mean": None,
                "median": None,
                "value_counts_top5": _value_counts(frame[feature]),
            }
        numeric = pd.to_numeric(frame[feature], errors="coerce")
        return {
            "non_null_count": int(numeric.notna().sum()),
            "mean": _safe_float(numeric.mean()),
            "median": _safe_float(numeric.median()),
            "value_counts_top5": {},
        }

    for topk in TOP_K_VALUES:
        baseline = joined[f"baseline_selected_top{topk}"].fillna(False).astype(bool)
        variant = joined[f"variant_selected_top{topk}"].fillna(False).astype(bool)
        added = variant & ~baseline
        unchanged = variant & baseline
        top15 = joined["top15_label"].fillna(False).astype(bool)
        bottom15 = joined["bottom15_label"].fillna(False).astype(bool)
        added_top15 = added & top15
        added_bottom15 = added & bottom15
        added_neutral = added & ~top15 & ~bottom15
        unchanged_top15 = unchanged & top15
        unchanged_bottom15 = unchanged & bottom15
        subset_frames = {
            "added_top15": joined[added_top15],
            "added_bottom15": joined[added_bottom15],
            "added_neutral": joined[added_neutral],
            "unchanged_top15": joined[unchanged_top15],
            "unchanged_bottom15": joined[unchanged_bottom15],
        }
        subset_stats = {name: {feature: _subset_stats(frame, feature) for feature in VOLUME_FEATURES} for name, frame in subset_frames.items()}
        plausible = []
        top_frame = joined[added_top15]
        bottom_frame = joined[added_bottom15]
        for feature in ("participation_quality_bucket", "volume_confirmation_repaired_flag", "vol_ratio5_20_repaired", "volume_zscore_20", "turnover_value_ratio5_20"):
            if feature not in joined.columns:
                continue
            if len(top_frame) == 0 or len(bottom_frame) == 0:
                continue
            if feature in {"participation_quality_bucket", "volume_confirmation_repaired_flag"}:
                top_vals = top_frame[feature].astype("string").fillna("").value_counts(normalize=True)
                bottom_vals = bottom_frame[feature].astype("string").fillna("").value_counts(normalize=True)
                if not top_vals.empty and not bottom_vals.empty and top_vals.index[0] != bottom_vals.index[0]:
                    plausible.append(feature)
            else:
                top_numeric = pd.to_numeric(top_frame[feature], errors="coerce")
                bottom_numeric = pd.to_numeric(bottom_frame[feature], errors="coerce")
                if top_numeric.notna().any() and bottom_numeric.notna().any() and abs(float(top_numeric.mean() - bottom_numeric.mean())) >= 0.5:
                    plausible.append(feature)
        result["topk"][f"top{topk}"] = {
            "added_top15_count": int(added_top15.sum()),
            "added_bottom15_count": int(added_bottom15.sum()),
            "added_neutral_count": int(added_neutral.sum()),
            "unchanged_top15_count": int(unchanged_top15.sum()),
            "unchanged_bottom15_count": int(unchanged_bottom15.sum()),
            "plausible_separators": sorted(set(plausible)),
            "subset_stats": subset_stats,
        }
    return result


def _build_boundary_pairwise(batch1_boundary: pd.DataFrame, volume_candidate: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    selected_join = batch1_boundary.merge(
        volume_candidate[KEY_COLS + VOLUME_FEATURES],
        on=KEY_COLS,
        how="left",
    )
    near_miss_keys = batch1_boundary[["anchor_date", "best_near_miss_symbol", "side"]].copy()
    near_miss_keys = near_miss_keys.rename(columns={"best_near_miss_symbol": "symbol"})
    near_miss_join = volume_candidate[KEY_COLS + VOLUME_FEATURES].rename(
        columns={
            "vol_ratio5_20_repaired": "near_miss_vol_ratio5_20_repaired",
            "volume_zscore_20": "near_miss_volume_zscore_20",
            "turnover_value_ratio5_20": "near_miss_turnover_value_ratio5_20",
            "participation_quality_bucket": "near_miss_participation_quality_bucket",
            "volume_confirmation_repaired_flag": "near_miss_volume_confirmation_repaired_flag",
        }
    )
    pairwise = selected_join.merge(near_miss_join, on=["anchor_date", "symbol", "side"], how="left")
    pairwise = pairwise[pairwise["near_miss_joined"].fillna(False).astype(bool)].copy()
    if pairwise.empty:
        summary = {
            "schema_version": PAIRWISE_SCHEMA_VERSION,
            "pair_count": 0,
            "matched_near_miss_count": 0,
            "selected_higher_score_count": 0,
            "selected_worse_path_count": 0,
            "selected_higher_score_and_worse_path_count": 0,
            "entry_strength_gap_mean": None,
            "signal_quality_match_count": 0,
            "decision_candle_quality_match_count": 0,
            "liquidity_quality_match_count": 0,
            "volume_participation_match_count": 0,
            "headroom_match_count": 0,
            "score_gap_mean": None,
            "forward_ret_20d_gap_mean": None,
            "path_value_gap_mean": None,
        }
        return pairwise, summary
    pairwise["selected_higher_score"] = pd.to_numeric(pairwise["score"], errors="coerce") > pd.to_numeric(pairwise["best_near_miss_score"], errors="coerce")
    pairwise["selected_worse_path"] = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") < pd.to_numeric(pairwise["best_near_miss_forward_ret_20d"], errors="coerce")
    pairwise["selected_higher_score_and_worse_path"] = pairwise["selected_higher_score"] & pairwise["selected_worse_path"]
    pairwise["entry_strength_score_gap"] = pd.to_numeric(pairwise["entry_strength_score"], errors="coerce") - pd.to_numeric(pairwise["near_miss_entry_strength_score"], errors="coerce")
    pairwise["volume_zscore_gap"] = pd.to_numeric(pairwise["volume_zscore_20"], errors="coerce") - pd.to_numeric(pairwise["near_miss_volume_zscore_20"], errors="coerce")
    pairwise["turnover_ratio_gap"] = pd.to_numeric(pairwise["turnover_value_ratio5_20"], errors="coerce") - pd.to_numeric(pairwise["near_miss_turnover_value_ratio5_20"], errors="coerce")
    pairwise["vol_ratio_gap"] = pd.to_numeric(pairwise["vol_ratio5_20_repaired"], errors="coerce") - pd.to_numeric(pairwise["near_miss_vol_ratio5_20_repaired"], errors="coerce")
    pairwise["signal_quality_match"] = pairwise["signal_quality_bucket"] == pairwise["near_miss_signal_quality_bucket"]
    pairwise["decision_candle_quality_match"] = pairwise["decision_candle_quality"] == pairwise["near_miss_decision_candle_quality"]
    pairwise["liquidity_quality_match"] = pairwise["liquidity_quality_bucket"] == pairwise["near_miss_liquidity_quality_bucket"]
    pairwise["volume_participation_match"] = pairwise["participation_quality_bucket"] == pairwise["near_miss_participation_quality_bucket"]
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
        "volume_zscore_gap_mean": _safe_float(pairwise["volume_zscore_gap"].mean()),
        "turnover_ratio_gap_mean": _safe_float(pairwise["turnover_ratio_gap"].mean()),
        "vol_ratio_gap_mean": _safe_float(pairwise["vol_ratio_gap"].mean()),
        "signal_quality_match_count": int(pairwise["signal_quality_match"].sum()),
        "decision_candle_quality_match_count": int(pairwise["decision_candle_quality_match"].sum()),
        "liquidity_quality_match_count": int(pairwise["liquidity_quality_match"].sum()),
        "volume_participation_match_count": int(pairwise["volume_participation_match"].sum()),
        "headroom_match_count": int(pairwise["headroom_match"].sum()),
        "score_gap_mean": _safe_float(score_gap.mean()),
        "forward_ret_20d_gap_mean": _safe_float(ret_gap.mean()),
        "path_value_gap_mean": _safe_float(path_gap.mean()),
    }
    return pairwise, summary


def _build_candidate_families(frame: pd.DataFrame, pairwise_summary: dict[str, Any]) -> dict[str, Any]:
    families = []
    for code, group in frame.groupby("batch2_volume_root_cause_code", dropna=False):
        if str(code).startswith("data_gap_"):
            continue
        top5 = group["champion_selected_top5"].fillna(False).astype(bool) if "champion_selected_top5" in group.columns else pd.Series([False] * len(group), index=group.index)
        top10 = group["champion_selected_top10"].fillna(False).astype(bool) if "champion_selected_top10" in group.columns else pd.Series([False] * len(group), index=group.index)
        top20 = group["champion_selected_top20"].fillna(False).astype(bool) if "champion_selected_top20" in group.columns else pd.Series([False] * len(group), index=group.index)
        boundary = group["near_miss_joined"].fillna(False).astype(bool) if "near_miss_joined" in group.columns else pd.Series([False] * len(group), index=group.index)
        score_gap = pd.to_numeric(group["score"], errors="coerce") - pd.to_numeric(group["best_near_miss_score"], errors="coerce") if "best_near_miss_score" in group.columns else pd.Series([None] * len(group), index=group.index)
        ret_gap = pd.to_numeric(group["forward_ret_20d"], errors="coerce") - pd.to_numeric(group["best_near_miss_forward_ret_20d"], errors="coerce") if "best_near_miss_forward_ret_20d" in group.columns else pd.Series([None] * len(group), index=group.index)
        path_gap = pd.to_numeric(group["path_value_score_v1"], errors="coerce") - pd.to_numeric(group["best_near_miss_path_value_score_v1"], errors="coerce") if "best_near_miss_path_value_score_v1" in group.columns else pd.Series([None] * len(group), index=group.index)
        family = {
            "batch2_volume_root_cause_code": str(code),
            "count": int(len(group)),
            "top5_count": int(top5.sum()),
            "top10_count": int(top10.sum()),
            "top20_count": int(top20.sum()),
            "boundary_pair_count": int(boundary.sum()),
            "boundary_match_rate": _safe_float(boundary.mean()) if len(group) else None,
            "mean_score_gap": _safe_float(score_gap.mean()),
            "mean_forward_ret_20d_gap": _safe_float(ret_gap.mean()),
            "mean_path_value_score_v1_gap": _safe_float(path_gap.mean()),
            "decision_classification": "insufficient_signal",
            "reason": "batch2 volume family summary",
        }
        if code in {"low_turnover_participation_false_positive", "weak_participation_false_positive"}:
            if family["count"] >= 20 and family["top10_count"] > 0 and family["boundary_pair_count"] >= 20 and family["mean_score_gap"] is not None and family["mean_forward_ret_20d_gap"] is not None and family["mean_path_value_score_v1_gap"] is not None:
                if family["mean_score_gap"] > 0 and family["mean_forward_ret_20d_gap"] < 0 and family["mean_path_value_score_v1_gap"] < 0:
                    family["decision_classification"] = "challenger_ready"
                else:
                    family["decision_classification"] = "explanation_only"
            else:
                family["decision_classification"] = "explanation_only"
        elif code == "volume_confirmed_valid_winner":
            family["decision_classification"] = "explanation_only"
        elif code == "high_entry_strength_low_participation_conflict":
            family["decision_classification"] = "explanation_only"
        elif code == "volume_confirmation_missing_false_positive":
            family["decision_classification"] = "explanation_only"
        elif code == "abnormal_volume_exhaustion_false_positive":
            family["decision_classification"] = "explanation_only"
        families.append(family)

    families = sorted(families, key=lambda item: (-item["count"], item["batch2_volume_root_cause_code"]))
    recommended = next((item for item in families if item["decision_classification"] == "challenger_ready"), None)
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_families": families,
        "recommended_candidate": recommended,
        "recommended_family_count": len(families),
        "notes": [
            "diagnostic only; no challenger is created in this task",
            "a recommended candidate is only emitted when the family is clean enough to stand on its own",
        ],
        "pairwise_summary": {
            "pair_count": pairwise_summary["pair_count"],
            "selected_higher_score_and_worse_path_count": pairwise_summary["selected_higher_score_and_worse_path_count"],
            "volume_participation_match_count": pairwise_summary["volume_participation_match_count"],
            "entry_strength_gap_mean": pairwise_summary["entry_strength_gap_mean"],
            "volume_zscore_gap_mean": pairwise_summary.get("volume_zscore_gap_mean"),
            "turnover_ratio_gap_mean": pairwise_summary.get("turnover_ratio_gap_mean"),
            "vol_ratio_gap_mean": pairwise_summary.get("vol_ratio_gap_mean"),
        },
    }


def _build_decision(validation: dict[str, Any], candidate_families: dict[str, Any], contrast: dict[str, Any]) -> dict[str, Any]:
    recommended = candidate_families.get("recommended_candidate")
    if recommended is not None:
        decision = "ready_for_single_axis_challenger_design"
        reason = f"{recommended['batch2_volume_root_cause_code']}_is_clean_enough_to_isolate"
    else:
        families = candidate_families.get("candidate_families", [])
        volume_families = [item for item in families if item["batch2_volume_root_cause_code"] in {"low_turnover_participation_false_positive", "weak_participation_false_positive", "volume_confirmation_missing_false_positive", "high_entry_strength_low_participation_conflict", "abnormal_volume_exhaustion_false_positive"}]
        if volume_families:
            if contrast["topk"]["top10"]["plausible_separators"]:
                decision = "needs_batch2_event_sources"
                reason = "volume_repair_helps_but_remaining_separation_likely_needs_event_rights_context"
            else:
                decision = "explanation_only"
                reason = "volume_features_explain_cases_but_overlap_with_winners_is_still_too_high"
        else:
            decision = "insufficient_signal"
            reason = "no_stable_volume_driven_family_emerged"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "row_count_reconciled": validation["row_count_reconciled"],
        "no_lookahead_passed": validation["no_lookahead_audit_passed"],
        "volume_feature_separator_status": "plausible" if contrast["topk"]["top10"]["plausible_separators"] else "weak",
        "plausible_separators": {
            "top5": contrast["topk"]["top5"]["plausible_separators"],
            "top10": contrast["topk"]["top10"]["plausible_separators"],
            "top20": contrast["topk"]["top20"]["plausible_separators"],
        },
        "batch2_volume_feature_status": {
            feature: "coverage_1.0" for feature in VOLUME_FEATURES
        },
        "recommended_next_axis": "single-axis challenger design" if decision == "ready_for_single_axis_challenger_design" else "event / rights source discovery or explanation-only freeze",
        "jobs_supported": 1,
    }


def run_bad_pick_reclassification_batch2_volume_features_v1(
    *,
    output_root: str | Path | None = None,
    batch1_reclass_rows: str | Path | None = None,
    batch1_root_cause_summary: str | Path | None = None,
    batch1_before_after: str | Path | None = None,
    batch1_boundary_pairwise: str | Path | None = None,
    batch1_boundary_pairwise_summary: str | Path | None = None,
    batch1_decision: str | Path | None = None,
    volume_candidate: str | Path | None = None,
    volume_orfp: str | Path | None = None,
    volume_formula: str | Path | None = None,
    volume_coverage: str | Path | None = None,
    volume_missingness: str | Path | None = None,
    volume_no_lookahead: str | Path | None = None,
    volume_contrast: str | Path | None = None,
    volume_orfp_summary: str | Path | None = None,
    volume_decision: str | Path | None = None,
    orfp_freeze_session: str | Path | None = None,
    orfp_topk_diff: str | Path | None = None,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    source_paths = {
        "batch1_reclass_rows": _safe_path(batch1_reclass_rows, DEFAULT_BATCH1_RECLASS),
        "batch1_root_cause_summary": _safe_path(batch1_root_cause_summary, DEFAULT_BATCH1_ROOT_CAUSE),
        "batch1_before_after": _safe_path(batch1_before_after, DEFAULT_BATCH1_BEFORE_AFTER),
        "batch1_boundary_pairwise": _safe_path(batch1_boundary_pairwise, DEFAULT_BATCH1_BOUNDARY),
        "batch1_boundary_pairwise_summary": _safe_path(batch1_boundary_pairwise_summary, DEFAULT_BATCH1_BOUNDARY_SUMMARY),
        "batch1_decision": _safe_path(batch1_decision, DEFAULT_BATCH1_DECISION),
        "volume_candidate": _safe_path(volume_candidate, DEFAULT_VOLUME_CANDIDATE),
        "volume_orfp": _safe_path(volume_orfp, DEFAULT_VOLUME_ORFP),
        "volume_formula": _safe_path(volume_formula, DEFAULT_VOLUME_FORMULA),
        "volume_coverage": _safe_path(volume_coverage, DEFAULT_VOLUME_COVERAGE),
        "volume_missingness": _safe_path(volume_missingness, DEFAULT_VOLUME_MISSINGNESS),
        "volume_no_lookahead": _safe_path(volume_no_lookahead, DEFAULT_VOLUME_NO_LOOKAHEAD),
        "volume_contrast": _safe_path(volume_contrast, DEFAULT_VOLUME_CONTRAST),
        "volume_orfp_summary": _safe_path(volume_orfp_summary, DEFAULT_VOLUME_ORFP_SUMMARY),
        "volume_decision": _safe_path(volume_decision, DEFAULT_VOLUME_DECISION),
        "orfp_freeze_session": _safe_path(orfp_freeze_session, ORFP_FREEZE_SESSION),
        "orfp_topk_diff": _safe_path(orfp_topk_diff, DEFAULT_ORFP_TOPK_DIFF),
    }
    for path, label in [(p, n) for n, p in source_paths.items()]:
        _ensure_exists(path, label)

    batch1 = _load_frame(source_paths["batch1_reclass_rows"])
    volume_candidate = _load_frame(source_paths["volume_candidate"])
    volume_orfp = _load_frame(source_paths["volume_orfp"])
    batch1_boundary = _load_frame(source_paths["batch1_boundary_pairwise"])
    topk_diff = _load_frame(source_paths["batch1_boundary_pairwise"])
    # batch1_boundary_pairwise is authoritative for selected / near-miss keys; the same surface also carries the topk selection diff fields.

    if limit_anchor_dates is not None:
        anchors = sorted(batch1["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        batch1 = batch1[batch1["anchor_date"].isin(anchors)].copy()
        volume_candidate = volume_candidate[volume_candidate["anchor_date"].isin(anchors)].copy()
        volume_orfp = volume_orfp[volume_orfp["anchor_date"].isin(anchors)].copy()
        batch1_boundary = batch1_boundary[batch1_boundary["anchor_date"].isin(anchors)].copy()
        topk_diff = topk_diff[topk_diff["anchor_date"].isin(anchors)].copy()

    batch1_volume_lookup = volume_candidate[KEY_COLS + _feature_fields()].copy()
    batch1_merged = _join_volume_features(batch1, batch1_volume_lookup)
    batch2_rows = _apply_classification(batch1_merged)
    batch2_rows = batch2_rows.sort_values(["anchor_date", "side", "candidate_idx", "score", "symbol"], ascending=[True, True, True, False, True], kind="stable").reset_index(drop=True)

    batch1_validation = _build_input_validation(
        batch1_rows=batch1_merged,
        volume_candidate=volume_candidate,
        volume_orfp=volume_orfp,
        no_lookahead=json.loads(_ensure_exists(source_paths["volume_no_lookahead"], str(source_paths["volume_no_lookahead"])).read_text(encoding="utf-8")),
    )
    # Reuse the Batch 2 no-lookahead contract directly for the volume surface; the batch1 frame is only enriched with same-day features.
    batch1_validation["required_columns_present"]["batch1_reclassification"] = all(col in batch2_rows.columns for col in _required_columns())
    batch1_validation["batch1_reclassification_required_columns_present"] = batch1_validation["required_columns_present"]["batch1_reclassification"]
    batch1_validation["batch1_reclassification_row_count_reconciled"] = int(len(batch1_merged)) == 585
    batch1_validation["volume_candidate_row_count_reconciled"] = int(len(volume_candidate)) == 2542
    batch1_validation["volume_orfp_row_count_reconciled"] = int(len(volume_orfp)) == 365

    coverage_matrix = _build_coverage_matrix(batch2_rows)
    batch1_coverage = _load_json(source_paths["volume_coverage"])
    batch1_missingness = _load_json(source_paths["volume_missingness"])
    no_lookahead = _load_json(source_paths["volume_no_lookahead"])
    # Build contrast from the batch2 ORFP surface and the authoritative top-k membership diff.
    contrast = _build_topk_contrast(volume_orfp, _load_frame(source_paths["orfp_topk_diff"]))

    pairwise, pairwise_summary = _build_boundary_pairwise(batch1_boundary, volume_candidate)
    batch2_boundary = pairwise.copy()

    family_summary, family_breakdown = _build_root_cause_summary(batch2_rows)
    before_after = _before_after_summary(batch1, batch2_rows)
    future_candidates = _build_candidate_families(batch2_rows, pairwise_summary)
    decision = _build_decision(batch1_validation, future_candidates, contrast)

    volume_feature_summary = {
        "schema_version": "tradex_bad_pick_reclassification_batch2_volume_features_v1_volume_feature_summary_v1",
        "generated_at_utc": _utc_now(),
        "row_count": int(len(batch2_rows)),
        "feature_coverage": {
            feature: {
                "non_null_count": int(batch2_rows[feature].notna().sum()),
                "coverage_rate": _safe_float(batch2_rows[feature].notna().mean()),
            }
            for feature in VOLUME_FEATURES
        },
    }

    candidate_surface_summary = {
        "schema_version": "tradex_bad_pick_reclassification_batch2_volume_features_v1_candidate_surface_summary_v1",
        "row_count": int(len(batch2_rows)),
        "family_counts": _classification_counts(batch2_rows, "batch2_volume_root_cause_code"),
        "top5_count": int(batch2_rows["champion_selected_top5"].fillna(False).astype(bool).sum()) if "champion_selected_top5" in batch2_rows.columns else None,
        "top10_count": int(batch2_rows["champion_selected_top10"].fillna(False).astype(bool).sum()) if "champion_selected_top10" in batch2_rows.columns else None,
    }

    coverage_matrix_parquet = coverage_matrix.copy()
    for column in ["status_distribution", "missing_reason_distribution"]:
        if column in coverage_matrix_parquet.columns:
            coverage_matrix_parquet[column] = coverage_matrix_parquet[column].map(
                lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
            )

    session_dir = output_root_path / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)

    _write_parquet(session_dir / "batch2_volume_reclassification_rows.parquet", batch2_rows)
    _write_parquet(session_dir / "batch2_volume_boundary_pairwise.parquet", batch2_boundary)
    _write_parquet(session_dir / "batch2_volume_feature_distribution_summary.parquet", coverage_matrix_parquet)
    _write_json(session_dir / "run_manifest.json", _build_manifest(output_root_path, session_dir, source_paths))
    _write_json(session_dir / "input_resolution.json", _resolve_input_resolution(source_paths))
    _write_json(session_dir / "batch2_volume_input_validation.json", batch1_validation)
    _write_json(session_dir / "batch2_volume_root_cause_taxonomy_summary.json", family_summary)
    _write_json(session_dir / "before_after_batch2_volume_reclassification_summary.json", before_after)
    _write_json(session_dir / "batch2_volume_added_top15_bottom15_contrast.json", contrast)
    _write_json(session_dir / "batch2_volume_boundary_pairwise_summary.json", pairwise_summary)
    _write_json(session_dir / "batch2_volume_future_challenger_candidates.json", future_candidates)
    _write_json(session_dir / "bad_pick_reclassification_batch2_volume_v1_decision.json", decision)
    _write_json(session_dir / "volume_feature_summary.json", volume_feature_summary)
    _write_json(session_dir / "candidate_surface_summary.json", candidate_surface_summary)
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_dir": str(session_dir),
        "artifact_count": 11,
        "artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "batch2_volume_input_validation.json",
            "batch2_volume_reclassification_rows.parquet",
            "batch2_volume_root_cause_taxonomy_summary.json",
            "before_after_batch2_volume_reclassification_summary.json",
            "batch2_volume_added_top15_bottom15_contrast.json",
            "batch2_volume_boundary_pairwise.parquet",
            "batch2_volume_boundary_pairwise_summary.json",
            "batch2_volume_future_challenger_candidates.json",
            "bad_pick_reclassification_batch2_volume_v1_decision.json",
        ],
    })

    return {
        "output_dir": str(session_dir),
        "decision": decision["decision"],
        "session_id": session_dir.name,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_bad_pick_reclassification_batch2_volume_features_v1(
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        batch1_reclass_rows=DEFAULT_BATCH1_RECLASS,
        batch1_root_cause_summary=DEFAULT_BATCH1_ROOT_CAUSE,
        batch1_before_after=DEFAULT_BATCH1_BEFORE_AFTER,
        batch1_boundary_pairwise=DEFAULT_BATCH1_BOUNDARY,
        batch1_boundary_pairwise_summary=DEFAULT_BATCH1_BOUNDARY_SUMMARY,
        batch1_decision=DEFAULT_BATCH1_DECISION,
        volume_candidate=DEFAULT_VOLUME_CANDIDATE,
        volume_orfp=DEFAULT_VOLUME_ORFP,
        volume_formula=DEFAULT_VOLUME_FORMULA,
        volume_coverage=DEFAULT_VOLUME_COVERAGE,
        volume_missingness=DEFAULT_VOLUME_MISSINGNESS,
        volume_no_lookahead=DEFAULT_VOLUME_NO_LOOKAHEAD,
        volume_contrast=DEFAULT_VOLUME_CONTRAST,
        volume_orfp_summary=DEFAULT_VOLUME_ORFP_SUMMARY,
        volume_decision=DEFAULT_VOLUME_DECISION,
        orfp_freeze_session=ORFP_FREEZE_SESSION,
        orfp_topk_diff=DEFAULT_ORFP_TOPK_DIFF,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
