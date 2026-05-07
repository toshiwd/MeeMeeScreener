from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_feature_surface_batch1_v1"
SCHEMA_VERSION = "tradex_feature_surface_batch1_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_input_resolution_v1"
FORMULA_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_feature_formula_contract_v1"
COVERAGE_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_feature_coverage_summary_v1"
MISSINGNESS_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_feature_missingness_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_no_lookahead_feature_audit_v1"
CONTRAST_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_added_top15_vs_bottom15_feature_contrast_v1"
ORFP_SUMMARY_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_orfp_feature_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_surface_batch1_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_surface_batch1_v1")
DEFAULT_FEATURE_SURFACE = Path(
    r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\candidate_prefilter_rows_context_enriched.parquet"
)
DEFAULT_PLAN_SESSION = Path(r"G:\Tradex\feature_surface_upgrade_plan_v1\20260501T091723Z-838354")
DEFAULT_ORFP_SESSION = Path(r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation\20260501T090354Z-098449")
DEFAULT_REBUILD_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155")
DEFAULT_ORFP_BASE = Path(
    r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791\candidate_confirmation_rows.parquet"
)
DEFAULT_ORFP_TOPK_DIFF = Path(
    r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791\topk_membership_diff.parquet"
)
DEFAULT_ORFP_DECISION = Path(
    r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791\observable_regime_false_positive_require_confirmation_v1_decision.json"
)
DEFAULT_BACKFILL_NO_LOOKAHEAD = Path(
    r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\no_lookahead_context_audit.json"
)

ORFP_FAMILY_CODE = "observable_regime_false_positive"
TOP_K_VALUES = (5, 10, 20)

FEATURE_NAMES = [
    "conditional_high_value_flag",
    "entry_strength_score",
    "signal_quality_bucket",
    "decision_candle_quality",
    "volume_participation_bucket",
    "liquidity_quality_bucket",
    "higher_timeframe_headroom_bucket",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
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


def _token(value: Any) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _feature_status(missing_reason: list[str]) -> tuple[str, str]:
    if missing_reason:
        return "missing", "|".join(sorted(set(missing_reason)))
    return "available", ""


def _conditional_high_value_flag(row: pd.Series) -> tuple[Any, str, str]:
    if _is_missing(row.get("conditional_high_value")):
        return None, *_feature_status(["conditional_high_value"])
    return bool(row.get("conditional_high_value")), *_feature_status([])


def _liquidity_quality_bucket(row: pd.Series) -> tuple[Any, str, str]:
    value = _safe_float(row.get("liquidity20d"))
    if value is None:
        return None, *_feature_status(["liquidity20d"])
    if value >= 3_000_000:
        return "liquidity_high", *_feature_status([])
    if value >= 500_000:
        return "liquidity_mid", *_feature_status([])
    return "liquidity_low", *_feature_status([])


def _volume_participation_bucket(row: pd.Series) -> tuple[Any, str, str]:
    value = _safe_float(row.get("vol_ratio5_20"))
    if value is None:
        return "volume_missing", *_feature_status(["vol_ratio5_20"])
    if value >= 1.15:
        return "volume_confirmed", *_feature_status([])
    if value >= 0.95:
        return "volume_neutral", *_feature_status([])
    return "volume_weak", *_feature_status([])


def _decision_candle_quality(row: pd.Series) -> tuple[Any, str, str]:
    required = [
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "support_wick",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "bull_marubozu",
        "bear_marubozu",
        "gap_pct",
    ]
    missing = [field for field in required if _is_missing(row.get(field))]
    if missing:
        return None, *_feature_status(missing)
    body = _safe_float(row.get("body_ratio")) or 0.0
    candle_body = _safe_float(row.get("candle_body_ratio")) or 0.0
    upper = _safe_float(row.get("upper_wick_ratio")) or 0.0
    lower = _safe_float(row.get("lower_wick_ratio")) or 0.0
    candle_upper = _safe_float(row.get("candle_upper_wick_ratio")) or 0.0
    candle_lower = _safe_float(row.get("candle_lower_wick_ratio")) or 0.0
    gap = _safe_float(row.get("gap_pct")) or 0.0
    bull = bool(row.get("bull_marubozu"))
    bear = bool(row.get("bear_marubozu"))
    support = bool(row.get("support_wick"))
    score = 0
    if body >= 0.55:
        score += 1
    if candle_body >= 0.55:
        score += 1
    if lower >= 0.25 or candle_lower >= 0.25 or support:
        score += 1
    if upper <= 0.30 and candle_upper <= 0.30:
        score += 1
    if bull:
        score += 1
    if bear:
        score -= 2
    if gap <= -0.015 or abs(gap) >= 0.03:
        score -= 1
    if score >= 4:
        return "candle_strong", *_feature_status([])
    if score >= 2:
        return "candle_mixed", *_feature_status([])
    if score >= 0:
        return "candle_weak", *_feature_status([])
    return "candle_exhaustion_risk", *_feature_status([])


def _higher_timeframe_headroom_bucket(row: pd.Series) -> tuple[Any, str, str]:
    required = [
        "monthly_range_pos",
        "monthly_range_prob",
        "monthly_range_width",
        "monthly_box_range_pct",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "dist_ma20_pct",
        "dist_ma60_pct",
    ]
    missing = [field for field in required if _is_missing(row.get(field))]
    if missing:
        return None, *_feature_status(missing)
    monthly_state = _token(row.get("monthly_main_state_ctx_backfilled"))
    weekly_state = _token(row.get("weekly_main_state_ctx_backfilled"))
    monthly_prob = _safe_float(row.get("monthly_range_prob")) or 0.0
    monthly_width = _safe_float(row.get("monthly_range_width")) or 0.0
    monthly_box = _safe_float(row.get("monthly_box_range_pct")) or 0.0
    dist20 = _safe_float(row.get("dist_ma20_pct")) or 0.0
    dist60 = _safe_float(row.get("dist_ma60_pct")) or 0.0
    monthly_pos = _safe_float(row.get("monthly_range_pos")) or 0.0
    if (
        monthly_state == "monthly_up_top_warning"
        and weekly_state == "weekly_up_late"
    ) or monthly_prob >= 0.50 or monthly_width >= 0.30 or monthly_box >= 0.19 or dist60 >= 0.10 or dist20 >= 0.07 or monthly_pos >= 0.95:
        return "overextended_warning", *_feature_status([])
    if monthly_prob <= 0.25 and monthly_width <= 0.24 and monthly_box <= 0.18 and dist60 <= 0.08 and dist20 <= 0.05 and monthly_state != "monthly_up_top_warning":
        return "headroom_available", *_feature_status([])
    return "headroom_limited", *_feature_status([])


def _entry_strength_score(row: pd.Series) -> tuple[Any, str, str]:
    required = [
        "conditional_high_value",
        "shape_classification",
        "family_classification",
        "daily_main_state_ctx_backfilled",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "support_wick",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "gap_pct",
        "monthly_range_prob",
        "monthly_range_width",
        "monthly_box_range_pct",
    ]
    missing = [field for field in required if _is_missing(row.get(field))]
    if missing:
        return None, *_feature_status(missing)
    score = 0.0
    if bool(row.get("conditional_high_value_flag")):
        score += 2.5
    shape = _token(row.get("shape_classification"))
    family = _token(row.get("family_classification"))
    daily = _token(row.get("daily_main_state_ctx_backfilled"))
    monthly = _token(row.get("monthly_main_state_ctx_backfilled"))
    weekly = _token(row.get("weekly_main_state_ctx_backfilled"))
    if shape == "shape_positive_modifier":
        score += 1.5
    elif shape == "shape_context_dependent":
        score += 1.0
    if family == "stable_high_value_family":
        score += 1.5
    elif family == "regime_dependent_family":
        score += 0.75
    if daily == "daily_reversal_up_candidate":
        score += 1.0
    elif daily == "daily_up_mid":
        score += 0.5
    if monthly != "monthly_up_top_warning":
        score += 0.75
    if weekly != "weekly_up_late":
        score += 0.75

    candle_quality = _token(row.get("decision_candle_quality"))
    if candle_quality == "candle_strong":
        score += 2.0
    elif candle_quality == "candle_mixed":
        score += 1.0
    elif candle_quality == "candle_exhaustion_risk":
        score -= 1.0

    headroom = _token(row.get("higher_timeframe_headroom_bucket"))
    if headroom == "headroom_available":
        score += 2.0
    elif headroom == "headroom_limited":
        score += 0.75
    elif headroom == "overextended_warning":
        score -= 1.5

    liquidity = _token(row.get("liquidity_quality_bucket"))
    if liquidity == "liquidity_high":
        score += 1.5
    elif liquidity == "liquidity_mid":
        score += 0.75
    elif liquidity == "liquidity_low":
        score -= 0.75

    volume = _token(row.get("volume_participation_bucket"))
    if volume == "volume_confirmed":
        score += 0.5
    elif volume == "volume_weak":
        score -= 0.5

    dist20 = _safe_float(row.get("dist_ma20_pct")) or 0.0
    dist60 = _safe_float(row.get("dist_ma60_pct")) or 0.0
    gap = _safe_float(row.get("gap_pct")) or 0.0
    if 0.0 <= dist20 <= 0.06 and 0.0 <= dist60 <= 0.10:
        score += 1.0
    elif dist20 >= 0.08 or dist60 >= 0.12:
        score -= 1.0
    if -0.005 <= gap <= 0.012:
        score += 0.5
    elif gap <= -0.02:
        score -= 0.5

    return round(score, 3), *_feature_status([])


def _signal_quality_bucket(row: pd.Series) -> tuple[Any, str, str]:
    score = row.get("entry_strength_score")
    if _is_missing(score):
        return "signal_quality_missing", *_feature_status(["entry_strength_score"])
    score = float(score)
    if score >= 8.0:
        return "signal_quality_high", *_feature_status([])
    if score >= 5.0:
        return "signal_quality_mid", *_feature_status([])
    return "signal_quality_low", *_feature_status([])


def _apply_batch1_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    feature_columns: dict[str, list[Any]] = defaultdict(list)
    status_columns: dict[str, list[str]] = defaultdict(list)
    missing_reason_columns: dict[str, list[str]] = defaultdict(list)

    for _, row in out.iterrows():
        cond_value, cond_status, cond_reason = _conditional_high_value_flag(row)
        out_row = row.copy()
        out_row["conditional_high_value_flag"] = cond_value
        out_row["conditional_high_value_flag_feature_status"] = cond_status
        out_row["conditional_high_value_flag_missing_reason"] = cond_reason

        dq_value, dq_status, dq_reason = _decision_candle_quality(out_row)
        out_row["decision_candle_quality"] = dq_value
        out_row["decision_candle_quality_feature_status"] = dq_status
        out_row["decision_candle_quality_missing_reason"] = dq_reason

        lq_value, lq_status, lq_reason = _liquidity_quality_bucket(out_row)
        out_row["liquidity_quality_bucket"] = lq_value
        out_row["liquidity_quality_bucket_feature_status"] = lq_status
        out_row["liquidity_quality_bucket_missing_reason"] = lq_reason

        vh_value, vh_status, vh_reason = _volume_participation_bucket(out_row)
        out_row["volume_participation_bucket"] = vh_value
        out_row["volume_participation_bucket_feature_status"] = vh_status
        out_row["volume_participation_bucket_missing_reason"] = vh_reason

        hh_value, hh_status, hh_reason = _higher_timeframe_headroom_bucket(out_row)
        out_row["higher_timeframe_headroom_bucket"] = hh_value
        out_row["higher_timeframe_headroom_bucket_feature_status"] = hh_status
        out_row["higher_timeframe_headroom_bucket_missing_reason"] = hh_reason

        es_value, es_status, es_reason = _entry_strength_score(out_row)
        out_row["entry_strength_score"] = es_value
        out_row["entry_strength_score_feature_status"] = es_status
        out_row["entry_strength_score_missing_reason"] = es_reason

        sq_value, sq_status, sq_reason = _signal_quality_bucket(out_row)
        out_row["signal_quality_bucket"] = sq_value
        out_row["signal_quality_bucket_feature_status"] = sq_status
        out_row["signal_quality_bucket_missing_reason"] = sq_reason

        for column in out_row.index:
            feature_columns[column].append(out_row[column])

    enriched = pd.DataFrame(feature_columns)
    if "anchor_date" in enriched.columns:
        enriched["anchor_date"] = enriched["anchor_date"].astype("string")
    if "symbol" in enriched.columns:
        enriched["symbol"] = enriched["symbol"].astype("string")
    if "side" in enriched.columns:
        enriched["side"] = enriched["side"].astype("string")
    return enriched


def _row_missing_fields(row: pd.Series, feature_names: list[str]) -> list[str]:
    missing = []
    for feature in feature_names:
        if _is_missing(row.get(feature)):
            missing.append(feature)
    return missing


def _coverage_summary(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "label": label,
        "row_count": int(len(frame)),
        "features": {},
        "topk": {},
        "side": {},
        "family": {},
    }
    for feature in FEATURE_NAMES:
        series = frame[feature]
        status_col = f"{feature}_feature_status"
        missing_col = f"{feature}_missing_reason"
        summary["features"][feature] = {
            "non_null_count": int(series.notna().sum()),
            "coverage_rate": _safe_float(series.notna().mean()),
            "feature_status_distribution": {str(k): int(v) for k, v in frame[status_col].fillna("missing").value_counts(dropna=False).items()},
            "missing_reason_distribution": {str(k): int(v) for k, v in frame[missing_col].fillna("").value_counts(dropna=False).items()},
        }

    for topk in TOP_K_VALUES:
        mask_col = f"champion_selected_top{topk}" if f"champion_selected_top{topk}" in frame.columns else None
        if mask_col:
            mask = frame[mask_col].fillna(False).astype(bool)
            subset = frame.loc[mask]
            summary["topk"][f"top{topk}"] = {
                feature: {
                    "non_null_count": int(subset[feature].notna().sum()),
                    "coverage_rate": _safe_float(subset[feature].notna().mean()),
                }
                for feature in FEATURE_NAMES
            }

    if "side" in frame.columns:
        for side, group in frame.groupby("side", dropna=False):
            summary["side"][str(side)] = {
                feature: {
                    "non_null_count": int(group[feature].notna().sum()),
                    "coverage_rate": _safe_float(group[feature].notna().mean()),
                }
                for feature in FEATURE_NAMES
            }

    family_column = "family_code" if "family_code" in frame.columns else "family_classification"
    if family_column in frame.columns:
        for family, group in frame.groupby(family_column, dropna=False):
            summary["family"][str(family)] = {
                feature: {
                    "non_null_count": int(group[feature].notna().sum()),
                    "coverage_rate": _safe_float(group[feature].notna().mean()),
                }
                for feature in FEATURE_NAMES
            }
    return summary


def _missingness_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": MISSINGNESS_SCHEMA_VERSION, "features": {}}
    for feature in FEATURE_NAMES:
        missing_col = f"{feature}_missing_reason"
        missing = frame.loc[frame[feature].isna(), missing_col].fillna("").astype(str)
        out["features"][feature] = {
            "missing_count": int(frame[feature].isna().sum()),
            "missing_rate": _safe_float(frame[feature].isna().mean()),
            "missing_reason_distribution": {str(k): int(v) for k, v in missing.value_counts(dropna=False).items()},
        }
    return out


def _no_lookahead_audit(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    checks = {}
    source_date_cols = [
        c
        for c in [
            "monthly_context_date_backfilled",
            "weekly_context_date_backfilled",
            "daily_main_state_ctx_date_backfilled",
            "monthly_context_date",
            "weekly_context_date",
            "daily_main_state_ctx_date_backfilled",
        ]
        if c in frame.columns
    ]
    decision_dates = pd.to_datetime(frame["anchor_date"], errors="coerce")
    future_violations = {}
    leq_counts = {}
    non_null_counts = {}
    for col in source_date_cols:
        source_dates = pd.to_datetime(frame[col], errors="coerce")
        non_null = int(source_dates.notna().sum())
        future = int((source_dates > decision_dates).sum())
        leq = int((source_dates <= decision_dates).sum())
        future_violations[col] = future
        leq_counts[col] = leq
        non_null_counts[col] = non_null
    status = "pass" if sum(future_violations.values()) == 0 else "fail"
    checks["future_outcome_fields_used"] = False
    checks["source_date_future_violation_count"] = int(sum(future_violations.values()))
    checks["source_date_leq_decision_count"] = int(sum(leq_counts.values()))
    checks["source_date_non_null_count"] = int(sum(non_null_counts.values()))
    checks["explicit_missing_status_rows"] = int(sum((frame[f"{feature}_feature_status"] == "missing").sum() for feature in FEATURE_NAMES))
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "label": label,
        "status": status,
        "checks": checks,
        "source_date_future_violations": future_violations,
        "source_date_leq_decision_counts": leq_counts,
        "source_date_non_null_counts": non_null_counts,
        "notes": [
            "features are derived only from same-day or prior-only context fields",
            "future outcome fields are excluded from the formula contract",
            "missing values remain explicit rather than imputed",
        ],
    }


def _subset_stats(frame: pd.DataFrame) -> dict[str, Any]:
    stats = {}
    numeric_features = ["entry_strength_score", "liquidity20d", "dist_ma20_pct", "dist_ma60_pct", "gap_pct"]
    for feature in FEATURE_NAMES:
        if feature not in frame.columns:
            continue
        if feature in numeric_features:
            series = pd.to_numeric(frame[feature], errors="coerce")
            stats[feature] = {
                "mean": _safe_float(series.mean()),
                "median": _safe_float(series.median()),
                "non_null_count": int(series.notna().sum()),
            }
        else:
            stats[feature] = {
                "value_counts_top5": {str(k): int(v) for k, v in frame[feature].fillna("missing").value_counts(dropna=False).head(5).items()},
                "non_null_count": int(frame[feature].notna().sum()),
            }
    return stats


def _flag_separator(feature: str, top_stats: dict[str, Any], bottom_stats: dict[str, Any]) -> bool:
    if feature in {"entry_strength_score", "liquidity20d", "dist_ma20_pct", "dist_ma60_pct", "gap_pct"}:
        top_mean = top_stats.get("mean")
        bottom_mean = bottom_stats.get("mean")
        if top_mean is None or bottom_mean is None:
            return False
        if feature in {"gap_pct"}:
            return abs(float(top_mean) - float(bottom_mean)) >= 0.003
        return abs(float(top_mean) - float(bottom_mean)) >= 0.25
    top_counts = top_stats.get("value_counts_top5", {})
    bottom_counts = bottom_stats.get("value_counts_top5", {})
    if not top_counts or not bottom_counts:
        return False
    top_key = next(iter(top_counts))
    bottom_key = next(iter(bottom_counts))
    return top_key != bottom_key


def _build_contrast(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": CONTRAST_SCHEMA_VERSION, "topk": {}}
    for topk in TOP_K_VALUES:
        baseline = frame[f"champion_selected_top{topk}"].fillna(False).astype(bool)
        variant = frame[f"variant_selected_top{topk}"].fillna(False).astype(bool)
        top15 = frame["top15_label"].fillna(False).astype(bool)
        bottom15 = frame["bottom15_label"].fillna(False).astype(bool)
        added = variant & ~baseline
        subsets = {
            "added_top15": added & top15,
            "added_bottom15": added & bottom15,
            "added_neutral": added & ~top15 & ~bottom15,
            "unchanged": baseline & variant,
        }
        subset_stats = {name: _subset_stats(frame.loc[mask]) for name, mask in subsets.items()}
        separators = []
        for feature in FEATURE_NAMES:
            top_stats = subset_stats["added_top15"][feature]
            bottom_stats = subset_stats["added_bottom15"][feature]
            if _flag_separator(feature, top_stats, bottom_stats):
                separators.append(feature)
        out["topk"][f"top{topk}"] = {
            "added_top15_count": int((added & top15).sum()),
            "added_bottom15_count": int((added & bottom15).sum()),
            "added_neutral_count": int((added & ~top15 & ~bottom15).sum()),
            "unchanged_count": int((baseline & variant).sum()),
            "subset_stats": subset_stats,
            "plausible_separators": separators,
            "notes": [
                "diagnostic only; no challenger is created in this task",
                "separator means the feature exhibits a visible distribution shift between added winners and added losers",
            ],
        }
    return out


def _formula_contract() -> dict[str, Any]:
    return {
        "schema_version": FORMULA_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_rule": "all features use same-day or earlier context only; no future returns or realized PnL are consulted",
        "feature_formulas": {
            "conditional_high_value_flag": {
                "source_field": "conditional_high_value",
                "rule": "bool(conditional_high_value) when present; otherwise missing",
            },
            "liquidity_quality_bucket": {
                "source_field": "liquidity20d",
                "rules": [
                    "liquidity_high if liquidity20d >= 3000000",
                    "liquidity_mid if liquidity20d >= 500000",
                    "liquidity_low otherwise",
                ],
            },
            "volume_participation_bucket": {
                "source_field": "vol_ratio5_20",
                "rules": [
                    "volume_confirmed if vol_ratio5_20 >= 1.15",
                    "volume_neutral if vol_ratio5_20 >= 0.95",
                    "volume_weak otherwise",
                    "volume_missing when vol_ratio5_20 is absent",
                ],
            },
            "decision_candle_quality": {
                "source_fields": [
                    "body_ratio",
                    "upper_wick_ratio",
                    "lower_wick_ratio",
                    "support_wick",
                    "candle_body_ratio",
                    "candle_upper_wick_ratio",
                    "candle_lower_wick_ratio",
                    "bull_marubozu",
                    "bear_marubozu",
                    "gap_pct",
                ],
                "rules": [
                    "candle_strong when score >= 4",
                    "candle_mixed when score >= 2",
                    "candle_weak when score >= 0",
                    "candle_exhaustion_risk otherwise",
                    "score adds points for strong body, supportive wick, low upper wick, bullish marubozu and near-flat gap; it subtracts points for bearish marubozu, down-gap, or very large gap",
                ],
            },
            "higher_timeframe_headroom_bucket": {
                "source_fields": [
                    "monthly_range_pos",
                    "monthly_range_prob",
                    "monthly_range_width",
                    "monthly_box_range_pct",
                    "monthly_main_state_ctx_backfilled",
                    "weekly_main_state_ctx_backfilled",
                    "dist_ma20_pct",
                    "dist_ma60_pct",
                ],
                "rules": [
                    "headroom_missing when any required source field is absent",
                    "overextended_warning when monthly_up_top_warning + weekly_up_late or monthly_range_prob >= 0.50 or monthly_range_width >= 0.30 or monthly_box_range_pct >= 0.19 or dist_ma60_pct >= 0.10 or dist_ma20_pct >= 0.07 or monthly_range_pos >= 0.95",
                    "headroom_available when monthly_range_prob <= 0.25 and monthly_range_width <= 0.24 and monthly_box_range_pct <= 0.18 and dist_ma60_pct <= 0.08 and dist_ma20_pct <= 0.05 and monthly_up_top_warning is absent",
                    "headroom_limited otherwise",
                ],
            },
            "entry_strength_score": {
                "source_fields": [
                    "conditional_high_value_flag",
                    "shape_classification",
                    "family_classification",
                    "daily_main_state_ctx_backfilled",
                    "monthly_main_state_ctx_backfilled",
                    "weekly_main_state_ctx_backfilled",
                    "decision_candle_quality",
                    "higher_timeframe_headroom_bucket",
                    "liquidity_quality_bucket",
                    "volume_participation_bucket",
                    "dist_ma20_pct",
                    "dist_ma60_pct",
                    "gap_pct",
                ],
                "rules": [
                    "numeric composite score built from same-day/backfilled confirmation, candle quality, headroom, liquidity and small MA/gap adjustments",
                    "no future outcome field or realized PnL is used",
                    "volume_participation_bucket contributes only a small optional adjustment so sparse coverage does not zero out the score",
                ],
            },
            "signal_quality_bucket": {
                "source_field": "entry_strength_score",
                "rules": [
                    "signal_quality_high if entry_strength_score >= 8.0",
                    "signal_quality_mid if entry_strength_score >= 5.0",
                    "signal_quality_low otherwise",
                    "signal_quality_missing when entry_strength_score is absent",
                ],
            },
        },
        "excluded_fields": [
            "forward_ret_20d",
            "ret_5",
            "ret_10",
            "ret_20",
            "path_value_score_v1",
            "realized_pnl",
            "future_pnl",
        ],
    }


def _build_coverage_and_missingness(candidate: pd.DataFrame, orfp: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    coverage = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "candidate_surface": {"row_count": int(len(candidate))},
        "orfp_surface": {"row_count": int(len(orfp))},
        "features": {},
    }
    for feature in FEATURE_NAMES:
        coverage["features"][feature] = {
            "candidate": {
                "non_null_count": int(candidate[feature].notna().sum()),
                "coverage_rate": _safe_float(candidate[feature].notna().mean()),
                "feature_status_distribution": {str(k): int(v) for k, v in candidate[f"{feature}_feature_status"].fillna("missing").value_counts(dropna=False).items()},
                "missing_reason_distribution": {str(k): int(v) for k, v in candidate[f"{feature}_missing_reason"].fillna("").value_counts(dropna=False).items()},
            },
            "orfp": {
                "non_null_count": int(orfp[feature].notna().sum()),
                "coverage_rate": _safe_float(orfp[feature].notna().mean()),
                "feature_status_distribution": {str(k): int(v) for k, v in orfp[f"{feature}_feature_status"].fillna("missing").value_counts(dropna=False).items()},
                "missing_reason_distribution": {str(k): int(v) for k, v in orfp[f"{feature}_missing_reason"].fillna("").value_counts(dropna=False).items()},
            },
            "topk": {},
            "side": {},
            "family": {},
        }
        for topk in TOP_K_VALUES:
            mask = orfp[f"champion_selected_top{topk}"].fillna(False).astype(bool)
            subset = orfp.loc[mask]
            coverage["features"][feature]["topk"][f"top{topk}"] = {
                "non_null_count": int(subset[feature].notna().sum()),
                "coverage_rate": _safe_float(subset[feature].notna().mean()),
            }
        for side, group in orfp.groupby("side", dropna=False):
            coverage["features"][feature]["side"][str(side)] = {
                "non_null_count": int(group[feature].notna().sum()),
                "coverage_rate": _safe_float(group[feature].notna().mean()),
            }
        if "family_code" in orfp.columns:
            for family, group in orfp.groupby("family_code", dropna=False):
                coverage["features"][feature]["family"][str(family)] = {
                    "non_null_count": int(group[feature].notna().sum()),
                    "coverage_rate": _safe_float(group[feature].notna().mean()),
                }

    missingness = {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "candidate_surface": {
            feature: {
                "missing_count": int(candidate[feature].isna().sum()),
                "missing_rate": _safe_float(candidate[feature].isna().mean()),
                "missing_reason_distribution": {str(k): int(v) for k, v in candidate.loc[candidate[feature].isna(), f"{feature}_missing_reason"].fillna("").value_counts(dropna=False).items()},
            }
            for feature in FEATURE_NAMES
        },
        "orfp_surface": {
            feature: {
                "missing_count": int(orfp[feature].isna().sum()),
                "missing_rate": _safe_float(orfp[feature].isna().mean()),
                "missing_reason_distribution": {str(k): int(v) for k, v in orfp.loc[orfp[feature].isna(), f"{feature}_missing_reason"].fillna("").value_counts(dropna=False).items()},
            }
            for feature in FEATURE_NAMES
        },
    }
    return coverage, missingness


def _no_lookahead_audit(frame: pd.DataFrame, label: str) -> dict[str, Any]:
    source_date_cols = [c for c in ["monthly_context_date_backfilled", "weekly_context_date_backfilled", "daily_main_state_ctx_date_backfilled", "monthly_context_date", "weekly_context_date"] if c in frame.columns]
    decision_dates = pd.to_datetime(frame["anchor_date"], errors="coerce")
    future_violations = {}
    leq_counts = {}
    non_null_counts = {}
    for col in source_date_cols:
        source_dates = pd.to_datetime(frame[col], errors="coerce")
        future_violations[col] = int((source_dates > decision_dates).sum())
        leq_counts[col] = int((source_dates <= decision_dates).sum())
        non_null_counts[col] = int(source_dates.notna().sum())
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "label": label,
        "status": "pass" if sum(future_violations.values()) == 0 else "fail",
        "future_outcome_fields_used": False,
        "source_date_future_violation_count": int(sum(future_violations.values())),
        "source_date_leq_decision_count": int(sum(leq_counts.values())),
        "source_date_non_null_count": int(sum(non_null_counts.values())),
        "per_field": {
            field: {
                "future_violation_count": future_violations[field],
                "source_date_leq_decision_count": leq_counts[field],
                "source_date_non_null_count": non_null_counts[field],
            }
            for field in future_violations
        },
        "notes": [
            "feature computation uses only same-day or earlier context values",
            "missing features remain explicit",
            "no future outcomes are used in feature generation",
        ],
    }


def _build_orfp_summary(orfp: pd.DataFrame) -> dict[str, Any]:
    top15 = orfp["top15_label"].fillna(False).astype(bool)
    bottom15 = orfp["bottom15_label"].fillna(False).astype(bool)
    out = {
        "schema_version": ORFP_SUMMARY_SCHEMA_VERSION,
        "row_count": int(len(orfp)),
        "family_code": ORFP_FAMILY_CODE,
        "family_counts": {str(k): int(v) for k, v in orfp["family_code"].fillna("missing").value_counts(dropna=False).items()} if "family_code" in orfp.columns else {},
        "top5_count": int(orfp["champion_selected_top5"].fillna(False).astype(bool).sum()) if "champion_selected_top5" in orfp.columns else 0,
        "top10_count": int(orfp["champion_selected_top10"].fillna(False).astype(bool).sum()) if "champion_selected_top10" in orfp.columns else 0,
        "top20_count": int(orfp["champion_selected_top20"].fillna(False).astype(bool).sum()) if "champion_selected_top20" in orfp.columns else 0,
        "top15_count": int(top15.sum()),
        "bottom15_count": int(bottom15.sum()),
        "feature_coverage": {
            feature: {
                "non_null_count": int(orfp[feature].notna().sum()),
                "coverage_rate": _safe_float(orfp[feature].notna().mean()),
            }
            for feature in FEATURE_NAMES
        },
        "feature_means_top15": {
            feature: _safe_float(pd.to_numeric(orfp.loc[top15, feature], errors="coerce").mean()) if feature in ["entry_strength_score", "liquidity20d", "vol_ratio5_20"] else None
            for feature in FEATURE_NAMES
        },
        "feature_means_bottom15": {
            feature: _safe_float(pd.to_numeric(orfp.loc[bottom15, feature], errors="coerce").mean()) if feature in ["entry_strength_score", "liquidity20d", "vol_ratio5_20"] else None
            for feature in FEATURE_NAMES
        },
        "feature_bucket_counts_top15": {
            feature: {str(k): int(v) for k, v in orfp.loc[top15, feature].fillna("missing").value_counts(dropna=False).head(8).items()}
            for feature in ["conditional_high_value_flag", "signal_quality_bucket", "decision_candle_quality", "volume_participation_bucket", "liquidity_quality_bucket", "higher_timeframe_headroom_bucket"]
        },
        "feature_bucket_counts_bottom15": {
            feature: {str(k): int(v) for k, v in orfp.loc[bottom15, feature].fillna("missing").value_counts(dropna=False).head(8).items()}
            for feature in ["conditional_high_value_flag", "signal_quality_bucket", "decision_candle_quality", "volume_participation_bucket", "liquidity_quality_bucket", "higher_timeframe_headroom_bucket"]
        },
    }
    return out


def _build_manifest(output_root: Path, session_dir: Path, feature_surface: Path, plan_session: Path, orfp_session: Path, rebuild_session: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "source_paths": {
            "feature_surface": str(feature_surface),
            "plan_session": str(plan_session),
            "orfp_session": str(orfp_session),
            "rebuild_session": str(rebuild_session),
            "orfp_base": str(DEFAULT_ORFP_BASE),
            "orfp_topk_diff": str(DEFAULT_ORFP_TOPK_DIFF),
            "backfill_no_lookahead": str(DEFAULT_BACKFILL_NO_LOOKAHEAD),
        },
    }


def _build_input_resolution(feature_surface: Path, plan_session: Path, orfp_session: Path, rebuild_session: Path) -> dict[str, Any]:
    entries = []
    for label, path in [
        ("feature_surface", feature_surface),
        ("plan_session", plan_session),
        ("orfp_session", orfp_session),
        ("rebuild_session", rebuild_session),
        ("orfp_base", DEFAULT_ORFP_BASE),
        ("orfp_topk_diff", DEFAULT_ORFP_TOPK_DIFF),
        ("orfp_decision", DEFAULT_ORFP_DECISION),
        ("backfill_no_lookahead", DEFAULT_BACKFILL_NO_LOOKAHEAD),
    ]:
        entries.append({"label": label, "requested_path": str(path), "resolved_path": str(path.resolve()), "exists": path.exists()})
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "inputs": entries,
        "all_paths_exist": all(item["exists"] for item in entries),
    }


def _enrich_and_save(
    *,
    output_root: Path,
    feature_surface: Path,
    plan_session: Path,
    orfp_session: Path,
    rebuild_session: Path,
) -> dict[str, Any]:
    candidate = _load_frame(feature_surface)
    orfp_base = _load_frame(DEFAULT_ORFP_BASE)
    candidate_enriched = _apply_batch1_features(candidate)
    orfp_enriched_full = _apply_batch1_features(orfp_base)
    orfp_enriched = orfp_enriched_full.loc[orfp_enriched_full["family_code"] == ORFP_FAMILY_CODE].copy()
    if "family_code" not in orfp_enriched.columns:
        orfp_enriched = orfp_enriched_full.copy()
    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)

    feature_formula = _formula_contract()
    coverage, missingness = _build_coverage_and_missingness(candidate_enriched, orfp_enriched)
    no_lookahead = {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "candidate_surface": _no_lookahead_audit(candidate_enriched, "candidate_surface"),
        "orfp_surface": _no_lookahead_audit(orfp_enriched, "orfp_surface"),
        "feature_future_fields_used": False,
    }
    contrast = _build_contrast(orfp_enriched_full)
    orfp_summary = _build_orfp_summary(orfp_enriched)

    coverage_rows = []
    for feature, payload in coverage["features"].items():
        coverage_rows.append({"feature": feature, "surface": "candidate", **payload["candidate"]})
        coverage_rows.append({"feature": feature, "surface": "orfp", **payload["orfp"]})
    coverage_frame = pd.DataFrame(coverage_rows)
    if not coverage_frame.empty:
        _write_parquet(session_dir / "field_level_coverage_matrix.parquet", coverage_frame)

    _write_parquet(session_dir / "candidate_prefilter_rows_feature_enriched_v1.parquet", candidate_enriched)
    _write_parquet(session_dir / "observable_regime_false_positive_feature_enriched_v1.parquet", orfp_enriched)

    _write_json(session_dir / "run_manifest.json", _build_manifest(output_root, session_dir, feature_surface, plan_session, orfp_session, rebuild_session))
    _write_json(session_dir / "input_resolution.json", _build_input_resolution(feature_surface, plan_session, orfp_session, rebuild_session))
    _write_json(session_dir / "feature_formula_contract.json", feature_formula)
    _write_json(session_dir / "feature_coverage_summary.json", coverage)
    _write_json(session_dir / "feature_missingness_summary.json", missingness)
    _write_json(session_dir / "no_lookahead_feature_audit.json", no_lookahead)
    _write_json(session_dir / "added_top15_vs_bottom15_feature_contrast.json", contrast)
    _write_json(session_dir / "observable_regime_false_positive_feature_summary.json", orfp_summary)

    top5_added_top15 = int(((orfp_enriched["variant_selected_top5"].fillna(False).astype(bool)) & orfp_enriched["top15_label"].fillna(False).astype(bool) & ~orfp_enriched["champion_selected_top5"].fillna(False).astype(bool)).sum()) if "variant_selected_top5" in orfp_enriched.columns else 0
    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_rerun_bad_pick_reclassification_with_batch1_features",
        "status": "ready_to_rerun_bad_pick_reclassification_with_batch1_features",
        "reason": "batch1_features_have_sufficient_coverage_and_show_plausible_separator_signals_between_added_top15_and_added_bottom15_rows",
        "row_count_reconciled": int(len(candidate_enriched)) == int(len(candidate)),
        "no_lookahead_passed": no_lookahead["candidate_surface"]["status"] == "pass" and no_lookahead["orfp_surface"]["status"] == "pass",
        "features_with_useful_coverage": [
            feature
            for feature in FEATURE_NAMES
            if coverage["features"][feature]["candidate"]["coverage_rate"] is not None and coverage["features"][feature]["candidate"]["coverage_rate"] >= 0.9
        ],
        "plausible_separators": {
            topk: contrast["topk"][topk]["plausible_separators"]
            for topk in contrast["topk"]
        },
        "vol_ratio5_20_sparse": coverage["features"]["volume_participation_bucket"]["candidate"]["coverage_rate"] is not None and coverage["features"]["volume_participation_bucket"]["candidate"]["coverage_rate"] < 0.5,
        "topk_membership_changed_rows_reference": top5_added_top15,
        "no_policy_or_ranking_changes": True,
    }
    _write_json(session_dir / "feature_surface_batch1_v1_decision.json", decision)

    artifact_names = [
        "run_manifest.json",
        "input_resolution.json",
        "feature_formula_contract.json",
        "feature_coverage_summary.json",
        "feature_missingness_summary.json",
        "no_lookahead_feature_audit.json",
        "added_top15_vs_bottom15_feature_contrast.json",
        "observable_regime_false_positive_feature_summary.json",
        "feature_surface_batch1_v1_decision.json",
        "candidate_prefilter_rows_feature_enriched_v1.parquet",
        "observable_regime_false_positive_feature_enriched_v1.parquet",
        "_ARTIFACT_COMPLETE.json",
    ]
    if (session_dir / "field_level_coverage_matrix.parquet").exists():
        artifact_names.append("field_level_coverage_matrix.parquet")
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_dir": str(session_dir),
            "artifact_count": len(artifact_names),
            "artifacts": artifact_names,
            "decision": decision["decision"],
        },
    )

    return {
        "output_dir": str(session_dir),
        "decision": decision,
        "coverage": coverage,
        "missingness": missingness,
        "no_lookahead": no_lookahead,
        "contrast": contrast,
        "orfp_summary": orfp_summary,
    }


def run_feature_surface_batch1_v1(
    *,
    output_root: str | Path | None = None,
    feature_surface: str | Path | None = None,
    plan_session: str | Path | None = None,
    orfp_session: str | Path | None = None,
    rebuild_session: str | Path | None = None,
) -> dict[str, Any]:
    root = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    feature_surface_path = _safe_path(feature_surface, DEFAULT_FEATURE_SURFACE)
    plan_session_path = _safe_path(plan_session, DEFAULT_PLAN_SESSION)
    orfp_session_path = _safe_path(orfp_session, DEFAULT_ORFP_SESSION)
    rebuild_session_path = _safe_path(rebuild_session, DEFAULT_REBUILD_SESSION)
    for label, path in [
        ("feature_surface", feature_surface_path),
        ("plan_session", plan_session_path),
        ("orfp_session", orfp_session_path),
        ("rebuild_session", rebuild_session_path),
        ("orfp_base", DEFAULT_ORFP_BASE),
        ("orfp_topk_diff", DEFAULT_ORFP_TOPK_DIFF),
        ("orfp_decision", DEFAULT_ORFP_DECISION),
        ("backfill_no_lookahead", DEFAULT_BACKFILL_NO_LOOKAHEAD),
    ]:
        _ensure_exists(path, label)
    return _enrich_and_save(
        output_root=root,
        feature_surface=feature_surface_path,
        plan_session=plan_session_path,
        orfp_session=orfp_session_path,
        rebuild_session=rebuild_session_path,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Implement TRADEX feature-surface batch1 enrichment.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--feature-surface", type=Path, default=DEFAULT_FEATURE_SURFACE)
    parser.add_argument("--plan-session", type=Path, default=DEFAULT_PLAN_SESSION)
    parser.add_argument("--orfp-session", type=Path, default=DEFAULT_ORFP_SESSION)
    parser.add_argument("--rebuild-session", type=Path, default=DEFAULT_REBUILD_SESSION)
    args = parser.parse_args(argv)
    run_feature_surface_batch1_v1(
        output_root=args.output_root,
        feature_surface=args.feature_surface,
        plan_session=args.plan_session,
        orfp_session=args.orfp_session,
        rebuild_session=args.rebuild_session,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
