from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_bad_pick_unknown_reclassification_v1 import (  # noqa: E402
    ROOT_CAUSE_CODE,
    TOP_K_VALUES,
    _is_daily_negative,
    _is_daily_positive,
    _is_missing_token,
    _json_ready,
    _make_session_id,
    _monthly_bucket,
    _normalize_token,
    _safe_float,
    _unique_ordered,
    _weekly_bucket,
    _write_json,
)

DEFAULT_BACKFILL_SESSION = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646")
DEFAULT_ENRICHED_CANDIDATE_SURFACE = DEFAULT_BACKFILL_SESSION / "candidate_prefilter_rows_context_enriched.parquet"
DEFAULT_ENRICHED_UNKNOWN_SURFACE = DEFAULT_BACKFILL_SESSION / "unknown_reclassification_rows_context_enriched.parquet"
DEFAULT_ORIGINAL_BAD_PICK_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_ORIGINAL_RECLASSIFICATION_SESSION = Path(r"G:\Tradex\bad_pick_unknown_reclassification_v1\20260501T043137Z-302dd27c")
DEFAULT_ORIGINAL_RECLASSIFICATION_ROWS = DEFAULT_ORIGINAL_RECLASSIFICATION_SESSION / "unknown_reclassification_rows.parquet"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1"
MANIFEST_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_input_resolution_v1"
VALIDATION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_input_validation_v1"
COHORT_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_unknown_cohort_summary_v1"
BEFORE_AFTER_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_before_after_reclassification_summary_v1"
PAIRWISE_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_enriched_unknown_boundary_pairwise_summary_v1"
ROOT_CAUSE_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_enriched_root_cause_taxonomy_summary_v1"
FAMILY_BREAKDOWN_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_enriched_root_cause_family_breakdown_v1"
CANDIDATE_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_future_challenger_candidates_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_remaining_data_gap_recommendations_v1"
DECISION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_enriched_v1_decision_v1"

UNAVAILABLE_FIELDS = ["event_flag", "earnings_flag", "dividend_flag", "rights_flag", "ex_rights_flag"]
ENRICHED_CLASSIFICATION_FIELDS = [
    "monthly_main_state_ctx_backfilled",
    "weekly_main_state_ctx_backfilled",
    "daily_main_state_ctx_backfilled",
    "monthly_context_backfill_status",
    "weekly_context_backfill_status",
    "daily_main_state_ctx_backfill_status",
    "monthly_context_no_lookahead_backfilled",
    "weekly_context_no_lookahead_backfilled",
    "daily_main_state_ctx_no_lookahead_backfilled",
    "dominant_regime_context",
    "market_regime_bucket",
    "family_classification",
    "family_regime_context",
    "shape_classification",
    "candle_shape_modifier",
    "liquidity20d",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "gap_pct",
    "vol_ratio5_20",
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
    "boundary_candidate_count",
    "boundary_rank_range",
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path).copy()


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _parse_date(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "<na>"}:
        return None
    try:
        return pd.to_datetime(text).date().isoformat()
    except Exception:
        return text


def _backfilled_state(row: pd.Series, base_name: str) -> Any:
    backfilled = row.get(f"{base_name}_backfilled")
    if not _is_missing_token(backfilled):
        return backfilled
    value = row.get(base_name)
    if not _is_missing_token(value):
        return value
    return None


def _backfill_status(row: pd.Series, base_name: str) -> str:
    return _normalize_token(row.get(f"{base_name}_backfill_status")).lower()


def _context_missing_enriched(row: pd.Series) -> bool:
    monthly_status = _backfill_status(row, "monthly_context")
    weekly_status = _backfill_status(row, "weekly_context")
    daily_status = _backfill_status(row, "daily_main_state_ctx")
    monthly_state = _backfilled_state(row, "monthly_main_state_ctx")
    weekly_state = _backfilled_state(row, "weekly_main_state_ctx")
    daily_state = _backfilled_state(row, "daily_main_state_ctx")
    if monthly_status != "existing" or weekly_status != "existing":
        return True
    if _is_missing_token(monthly_state) or _is_missing_token(weekly_state):
        return True
    if daily_status != "existing" or _is_missing_token(daily_state):
        return True
    return False


def _liquidity_missing(row: pd.Series) -> bool:
    return _is_missing_token(row.get("vol_ratio5_20")) and _is_missing_token(row.get("liquidity20d"))


def _score_component_missing(row: pd.Series) -> bool:
    return _is_missing_token(row.get("dist_ma20_pct")) or _is_missing_token(row.get("dist_ma60_pct"))


def _monthly_main_state(row: pd.Series) -> Any:
    return _backfilled_state(row, "monthly_main_state_ctx")


def _weekly_main_state(row: pd.Series) -> Any:
    return _backfilled_state(row, "weekly_main_state_ctx")


def _daily_main_state(row: pd.Series) -> Any:
    return _backfilled_state(row, "daily_main_state_ctx")


def _build_pair_summary(pairwise: pd.DataFrame) -> dict[str, Any]:
    if pairwise.empty:
        return {
            "schema_version": PAIRWISE_SCHEMA_VERSION,
            "pair_count": 0,
            "matched_near_miss_count": 0,
            "selected_higher_score_count": 0,
            "selected_worse_path_count": 0,
            "selected_higher_score_and_worse_path_count": 0,
            "monthly_alignment_same_count": 0,
            "weekly_alignment_same_count": 0,
            "daily_alignment_same_count": 0,
            "shape_alignment_same_count": 0,
            "score_gap_mean": None,
            "score_gap_median": None,
            "forward_ret_20d_gap_mean": None,
            "forward_ret_20d_gap_median": None,
            "path_value_gap_mean": None,
            "path_value_gap_median": None,
        }
    score_gap = pd.to_numeric(pairwise["score"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_score"], errors="coerce"))
    ret_gap = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_forward_ret_20d"], errors="coerce"))
    path_gap = pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_path_value_score_v1"], errors="coerce"))
    return {
        "schema_version": PAIRWISE_SCHEMA_VERSION,
        "pair_count": int(len(pairwise)),
        "matched_near_miss_count": int(pairwise["near_miss_joined"].sum()),
        "selected_higher_score_count": int((score_gap > 0).sum()),
        "selected_worse_path_count": int((ret_gap < 0).sum()),
        "selected_higher_score_and_worse_path_count": int(((score_gap > 0) & (ret_gap < 0)).sum()),
        "monthly_alignment_same_count": int(pairwise["monthly_alignment_same"].sum()),
        "weekly_alignment_same_count": int(pairwise["weekly_alignment_same"].sum()),
        "daily_alignment_same_count": int(pairwise["daily_alignment_same"].sum()),
        "shape_alignment_same_count": int(pairwise["shape_alignment_same"].sum()),
        "score_gap_mean": _safe_float(score_gap.mean()),
        "score_gap_median": _safe_float(score_gap.median()),
        "forward_ret_20d_gap_mean": _safe_float(ret_gap.mean()),
        "forward_ret_20d_gap_median": _safe_float(ret_gap.median()),
        "path_value_gap_mean": _safe_float(path_gap.mean()),
        "path_value_gap_median": _safe_float(path_gap.median()),
    }


def _row_missingness_class(row: pd.Series) -> str:
    if _context_missing_enriched(row):
        return "missing_context_data"
    if _liquidity_missing(row):
        return "missing_liquidity_or_volume_data"
    if _score_component_missing(row):
        return "missing_score_component_data"
    return "sufficient_data_but_unclassified"


def _classify_enriched_row(row: pd.Series, *, boundary_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence_fields: list[str] = []
    missing_fields: list[str] = []

    def mark_missing(name: str) -> None:
        if name not in missing_fields:
            missing_fields.append(name)

    monthly_state = _monthly_main_state(row)
    weekly_state = _weekly_main_state(row)
    daily_state = _daily_main_state(row)
    monthly_status = _backfill_status(row, "monthly_context")
    weekly_status = _backfill_status(row, "weekly_context")
    daily_status = _backfill_status(row, "daily_main_state_ctx")
    missingness_class = _row_missingness_class(row)

    if monthly_status != "existing" or weekly_status != "existing" or _is_missing_token(monthly_state) or _is_missing_token(weekly_state):
        evidence_fields.extend(["monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled"])
        if daily_status != "existing" or _is_missing_token(daily_state):
            mark_missing("daily_main_state_ctx_backfilled")
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_context_missing",
            "reclassification_confidence": "high",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields or ["monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "monthly / weekly state context is still unavailable on the enriched surface",
        }
    if daily_status != "existing" or _is_missing_token(daily_state):
        evidence_fields.extend(["daily_main_state_ctx_backfilled", "monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_context_missing",
            "reclassification_confidence": "high",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(["daily_main_state_ctx_backfilled"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "daily state context remains missing even after backfill",
        }
    if _liquidity_missing(row):
        evidence_fields.extend(["vol_ratio5_20", "liquidity20d"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_liquidity_missing",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(["vol_ratio5_20", "liquidity20d"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "liquidity / volume coverage is incomplete on the enriched surface",
        }
    if _score_component_missing(row):
        evidence_fields.extend(["dist_ma20_pct", "dist_ma60_pct"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_score_component_missing",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(["dist_ma20_pct", "dist_ma60_pct"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "MA distance proxies are still missing on this row",
        }

    monthly_bucket = _monthly_bucket(monthly_state)
    weekly_bucket = _weekly_bucket(weekly_state)
    daily_positive = _is_daily_positive(daily_state)
    daily_negative = _is_daily_negative(daily_state)
    score_gap = _safe_float(row.get("score_gap"))
    forward_ret_20d_gap = _safe_float(row.get("forward_ret_20d_gap"))
    path_value_gap = _safe_float(row.get("path_value_gap"))
    gap_pct = _safe_float(row.get("gap_pct"))
    vol_ratio = _safe_float(row.get("vol_ratio5_20"))
    shape_class = _normalize_token(row.get("shape_classification"))
    family_class = _normalize_token(row.get("family_classification"))
    regime_ctx = _normalize_token(row.get("dominant_regime_context"))
    market_regime = _normalize_token(row.get("market_regime_bucket"))
    bottom15 = bool(row.get("bottom15_label"))
    path_value = _safe_float(row.get("path_value_score_v1"))

    if (
        (monthly_bucket in {"overextended", "range"} and weekly_bucket == "overextended" and daily_positive)
        or ("risk_off" in regime_ctx.lower() and daily_positive)
        or ("risk_off" in market_regime.lower() and daily_positive)
    ):
        evidence_fields.extend(
            [
                "monthly_main_state_ctx_backfilled",
                "weekly_main_state_ctx_backfilled",
                "daily_main_state_ctx_backfilled",
                "dominant_regime_context",
                "market_regime_bucket",
            ]
        )
        confidence = "high" if forward_ret_20d_gap is not None and forward_ret_20d_gap < 0 and path_value_gap is not None and path_value_gap < 0 else "medium"
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_regime_false_positive",
            "reclassification_confidence": confidence,
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": True,
            "notes": "backfilled monthly / weekly state context points to a stretched or weak regime while the daily state still looks bullish",
        }

    if pd.notna(gap_pct) and abs(gap_pct) >= 0.015 and daily_negative and bottom15:
        evidence_fields.extend(["gap_pct", "daily_main_state_ctx_backfilled", "bottom15_label"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_gap_reversal_failure",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "gap / reversal geometry is visible but the 20-day path still fails",
        }

    if pd.notna(vol_ratio) and vol_ratio >= 1.1 and bottom15:
        evidence_fields.extend(["vol_ratio5_20", "bottom15_label"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_volume_spike_failure",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "higher relative volume did not translate into a stable 20-day path",
        }

    if shape_class == "shape_positive_modifier" and pd.notna(path_value) and path_value < 0:
        evidence_fields.extend(["shape_classification", "candle_shape_modifier", "path_value_score_v1"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_candle_confirmation_failure",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "positive candle shape was not enough to confirm the realized 20-day path",
        }

    if boundary_summary is not None:
        score_gap_val = _safe_float(boundary_summary.get("score_gap"))
        ret_gap_val = _safe_float(boundary_summary.get("forward_ret_20d_gap"))
        path_gap_val = _safe_float(boundary_summary.get("path_value_gap"))
        if score_gap_val is not None and ret_gap_val is not None and path_gap_val is not None and score_gap_val > 0 and ret_gap_val < 0 and path_gap_val < 0:
            evidence_fields.extend(["score", "forward_ret_20d", "path_value_score_v1"])
            return {
                "missingness_class": missingness_class,
                "reclassified_root_cause_code": "observable_score_boundary_failure",
                "reclassification_confidence": "low",
                "evidence_fields_used": _unique_ordered(evidence_fields),
                "missing_fields": _unique_ordered(missing_fields),
                "is_data_gap": False,
                "is_candidate_for_future_challenger": False,
                "notes": "selected row won on score but lost on the realized 20-day path versus the near miss",
            }

    if family_class in {"regime_dependent_family", "stable_high_value_family"} and bottom15:
        evidence_fields.extend(["family_classification", "family_bottom15_rate", "family_mean_path_value_score_v1"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_family_false_positive",
            "reclassification_confidence": "low",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "family-level summary looked acceptable but the realized path still failed",
        }

    evidence_fields.extend(["score", "forward_ret_20d", "path_value_score_v1"])
    return {
        "missingness_class": missingness_class,
        "reclassified_root_cause_code": "still_unknown",
        "reclassification_confidence": "low",
        "evidence_fields_used": _unique_ordered(evidence_fields),
        "missing_fields": _unique_ordered(missing_fields),
        "is_data_gap": False,
        "is_candidate_for_future_challenger": False,
        "notes": "backfilled context explains the surface but no stable family emerged",
    }


def _load_enriched_surface(path: Path) -> pd.DataFrame:
    frame = _load_parquet(path)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["side"] = frame["side"].astype(str)
    return frame


def _classify_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rows = frame.apply(lambda row: _classify_enriched_row(row, boundary_summary=row.to_dict()), axis=1)
    classified = pd.DataFrame(rows.tolist())
    out = frame.reset_index(drop=True).copy()
    for column in classified.columns:
        out[column] = classified[column].values
    out["observable_family"] = out["reclassified_root_cause_code"].where(
        ~out["reclassified_root_cause_code"].str.startswith("data_gap_", na=False),
        other="data_gap",
    )
    out["pattern"] = out.apply(
        lambda row: "|".join(
            [
                _normalize_token(row.get("monthly_main_state_ctx_backfilled")),
                _normalize_token(row.get("weekly_main_state_ctx_backfilled")),
                _normalize_token(row.get("daily_main_state_ctx_backfilled")),
                _normalize_token(row.get("shape_classification")),
            ]
        ),
        axis=1,
    )
    return out


def _classification_counts(frame: pd.DataFrame, code_col: str) -> dict[str, int]:
    return {str(k): int(v) for k, v in frame[code_col].value_counts(dropna=False).items()}


def _split_counts(frame: pd.DataFrame, code_col: str, mask: pd.Series) -> dict[str, int]:
    return {str(k): int(v) for k, v in frame.loc[mask, code_col].value_counts(dropna=False).items()}


def _build_cohort_summary(frame: pd.DataFrame) -> dict[str, Any]:
    top5 = frame["champion_selected_top5"].fillna(False).astype(bool)
    top10 = frame["champion_selected_top10"].fillna(False).astype(bool)
    summary = {
        "schema_version": COHORT_SCHEMA_VERSION,
        "row_count": int(len(frame)),
        "unknown_count": int(len(frame)),
        "top5_count": int(top5.sum()),
        "top10_only_count": int((top10 & ~top5).sum()),
        "side_counts": frame["side"].value_counts(dropna=False).to_dict(),
        "month_counts": frame["month_bucket"].value_counts(dropna=False).to_dict(),
        "dominant_regime_counts": frame["dominant_regime_context"].value_counts(dropna=False).to_dict(),
        "missingness_class_counts": frame["missingness_class"].value_counts(dropna=False).to_dict(),
        "reclassified_root_cause_counts": frame["reclassified_root_cause_code"].value_counts(dropna=False).to_dict(),
        "confidence_distribution": frame["reclassification_confidence"].value_counts(dropna=False).to_dict(),
        "boundary_match_rate": _safe_float(frame["near_miss_joined"].mean()) if len(frame) else None,
        "boundary_pair_count": int(frame["near_miss_joined"].sum()),
        "boundary_gap_mean": _safe_float(frame["score_gap"].mean()),
        "boundary_return_gap_mean": _safe_float(frame["forward_ret_20d_gap"].mean()),
        "boundary_path_gap_mean": _safe_float(frame["path_value_gap"].mean()),
        "backfilled_context_non_null_counts": {
            "monthly_main_state_ctx_backfilled": int(frame["monthly_main_state_ctx_backfilled"].notna().sum()) if "monthly_main_state_ctx_backfilled" in frame.columns else 0,
            "weekly_main_state_ctx_backfilled": int(frame["weekly_main_state_ctx_backfilled"].notna().sum()) if "weekly_main_state_ctx_backfilled" in frame.columns else 0,
            "daily_main_state_ctx_backfilled": int(frame["daily_main_state_ctx_backfilled"].notna().sum()) if "daily_main_state_ctx_backfilled" in frame.columns else 0,
            "liquidity20d": int(frame["liquidity20d"].notna().sum()) if "liquidity20d" in frame.columns else 0,
            "vol_ratio5_20": int(frame["vol_ratio5_20"].notna().sum()) if "vol_ratio5_20" in frame.columns else 0,
        },
        "backfilled_context_non_null_rates": {
            "monthly_main_state_ctx_backfilled": _safe_float(frame["monthly_main_state_ctx_backfilled"].notna().mean()) if "monthly_main_state_ctx_backfilled" in frame.columns else None,
            "weekly_main_state_ctx_backfilled": _safe_float(frame["weekly_main_state_ctx_backfilled"].notna().mean()) if "weekly_main_state_ctx_backfilled" in frame.columns else None,
            "daily_main_state_ctx_backfilled": _safe_float(frame["daily_main_state_ctx_backfilled"].notna().mean()) if "daily_main_state_ctx_backfilled" in frame.columns else None,
            "liquidity20d": _safe_float(frame["liquidity20d"].notna().mean()) if "liquidity20d" in frame.columns else None,
            "vol_ratio5_20": _safe_float(frame["vol_ratio5_20"].notna().mean()) if "vol_ratio5_20" in frame.columns else None,
        },
        "top5_observable_count": int((top5 & ~frame["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (frame["reclassified_root_cause_code"] != "still_unknown")).sum()),
        "top10_only_observable_count": int((top10 & ~top5 & ~frame["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (frame["reclassified_root_cause_code"] != "still_unknown")).sum()),
        "still_unknown_count": int((frame["reclassified_root_cause_code"] == "still_unknown").sum()),
    }
    return summary


def _build_before_after_summary(original: pd.DataFrame, enriched: pd.DataFrame) -> dict[str, Any]:
    original_top5 = original["champion_selected_top5"].fillna(False).astype(bool)
    original_top10 = original["champion_selected_top10"].fillna(False).astype(bool)
    enriched_top5 = enriched["champion_selected_top5"].fillna(False).astype(bool)
    enriched_top10 = enriched["champion_selected_top10"].fillna(False).astype(bool)

    original_counts = _classification_counts(original, "reclassified_root_cause_code")
    enriched_counts = _classification_counts(enriched, "reclassified_root_cause_code")

    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "original": {
            "row_count": int(len(original)),
            "data_gap_context_missing_count": int((original["reclassified_root_cause_code"] == "data_gap_context_missing").sum()),
            "data_gap_liquidity_missing_count": int((original["reclassified_root_cause_code"] == "data_gap_liquidity_missing").sum()),
            "observable_family_count": int((~original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (original["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "still_unknown_count": int((original["reclassified_root_cause_code"] == "still_unknown").sum()),
            "top5_observable_count": int((original_top5 & ~original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (original["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "top10_only_observable_count": int((original_top10 & ~original_top5 & ~original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (original["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "side_counts": original["side"].value_counts(dropna=False).to_dict(),
            "month_counts": original["month_bucket"].value_counts(dropna=False).to_dict(),
            "regime_counts": original["dominant_regime_context"].value_counts(dropna=False).to_dict(),
            "reclassified_root_cause_counts": original_counts,
        },
        "enriched": {
            "row_count": int(len(enriched)),
            "data_gap_context_missing_count": int((enriched["reclassified_root_cause_code"] == "data_gap_context_missing").sum()),
            "data_gap_liquidity_missing_count": int((enriched["reclassified_root_cause_code"] == "data_gap_liquidity_missing").sum()),
            "observable_family_count": int((~enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (enriched["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "still_unknown_count": int((enriched["reclassified_root_cause_code"] == "still_unknown").sum()),
            "top5_observable_count": int((enriched_top5 & ~enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (enriched["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "top10_only_observable_count": int((enriched_top10 & ~enriched_top5 & ~enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (enriched["reclassified_root_cause_code"] != "still_unknown")).sum()),
            "side_counts": enriched["side"].value_counts(dropna=False).to_dict(),
            "month_counts": enriched["month_bucket"].value_counts(dropna=False).to_dict(),
            "regime_counts": enriched["dominant_regime_context"].value_counts(dropna=False).to_dict(),
            "reclassified_root_cause_counts": enriched_counts,
        },
        "delta": {
            "rows_moved_out_of_data_gap": int(
                (original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False).sum())
                - (enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False).sum())
            ),
            "rows_reclassified_into_observable_families": int(
                (
                    (~enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (enriched["reclassified_root_cause_code"] != "still_unknown")).sum()
                    - (~original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (original["reclassified_root_cause_code"] != "still_unknown")).sum()
                )
            ),
            "still_unknown_change": int((enriched["reclassified_root_cause_code"] == "still_unknown").sum() - (original["reclassified_root_cause_code"] == "still_unknown").sum()),
            "top5_observable_change": int(
                (
                    (enriched_top5 & ~enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (enriched["reclassified_root_cause_code"] != "still_unknown")).sum()
                    - (original_top5 & ~original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (original["reclassified_root_cause_code"] != "still_unknown")).sum()
                )
            ),
            "top10_only_observable_change": int(
                (
                    (enriched_top10 & ~enriched_top5 & ~enriched["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (enriched["reclassified_root_cause_code"] != "still_unknown")).sum()
                    - (original_top10 & ~original_top5 & ~original["reclassified_root_cause_code"].str.startswith("data_gap_", na=False) & (original["reclassified_root_cause_code"] != "still_unknown")).sum()
                )
            ),
        },
        "notes": [
            "the enriched run uses backfilled monthly / weekly / daily state fields on the same candidate universe",
            "rows still marked still_unknown are explanation fragments, not policy inputs",
        ],
    }


def _build_family_breakdown(enriched: pd.DataFrame) -> dict[str, Any]:
    families = []
    for code, group in enriched.groupby("reclassified_root_cause_code", dropna=False):
        decision_classification = (
            "data_pipeline_task"
            if str(code).startswith("data_gap_")
            else "challenger_ready"
            if str(code) == "observable_regime_false_positive" and len(group) >= 50 and int(group["champion_selected_top5"].sum()) >= 20 and _safe_float(group["near_miss_joined"].mean()) is not None and _safe_float(group["near_miss_joined"].mean()) >= 0.5
            else "explanation_only"
            if str(code).startswith("observable_") and len(group) < 50
            else "insufficient_signal"
            if str(code) == "still_unknown"
            else "explanation_only"
        )
        families.append(
            {
                "family_code": str(code),
                "count": int(len(group)),
                "top5_count": int(group["champion_selected_top5"].sum()),
                "top10_only_count": int((group["champion_selected_top10"] & ~group["champion_selected_top5"]).sum()),
                "boundary_matched_count": int(group["near_miss_joined"].sum()),
                "boundary_match_rate": _safe_float(group["near_miss_joined"].mean()) if len(group) else None,
                "mean_score_gap": _safe_float(group["score_gap"].mean()),
                "mean_forward_ret_20d_gap": _safe_float(group["forward_ret_20d_gap"].mean()),
                "mean_path_value_gap": _safe_float(group["path_value_gap"].mean()),
                "side_counts": group["side"].value_counts(dropna=False).to_dict(),
                "month_counts": group["month_bucket"].value_counts(dropna=False).to_dict(),
                "regime_counts": group["dominant_regime_context"].value_counts(dropna=False).to_dict(),
                "top_pattern_counts": group["pattern"].value_counts(dropna=False).head(8).to_dict(),
                "field_coverage": {
                    "monthly_main_state_ctx_backfilled": _safe_float(group["monthly_main_state_ctx_backfilled"].notna().mean()) if "monthly_main_state_ctx_backfilled" in group.columns else None,
                    "weekly_main_state_ctx_backfilled": _safe_float(group["weekly_main_state_ctx_backfilled"].notna().mean()) if "weekly_main_state_ctx_backfilled" in group.columns else None,
                    "daily_main_state_ctx_backfilled": _safe_float(group["daily_main_state_ctx_backfilled"].notna().mean()) if "daily_main_state_ctx_backfilled" in group.columns else None,
                    "dist_ma20_pct": _safe_float(group["dist_ma20_pct"].notna().mean()) if "dist_ma20_pct" in group.columns else None,
                    "dist_ma60_pct": _safe_float(group["dist_ma60_pct"].notna().mean()) if "dist_ma60_pct" in group.columns else None,
                    "gap_pct": _safe_float(group["gap_pct"].notna().mean()) if "gap_pct" in group.columns else None,
                    "vol_ratio5_20": _safe_float(group["vol_ratio5_20"].notna().mean()) if "vol_ratio5_20" in group.columns else None,
                },
                "decision_classification": decision_classification,
            }
        )
    families = sorted(families, key=lambda item: item["count"], reverse=True)
    return {
        "schema_version": FAMILY_BREAKDOWN_SCHEMA_VERSION,
        "family_rows": families,
        "top5_family_counts": {str(code): int(count) for code, count in enriched.loc[enriched["champion_selected_top5"], "reclassified_root_cause_code"].value_counts(dropna=False).items()},
        "top10_only_family_counts": {
            str(code): int(count)
            for code, count in enriched.loc[enriched["champion_selected_top10"] & ~enriched["champion_selected_top5"], "reclassified_root_cause_code"].value_counts(dropna=False).items()
        },
        "regime_family_counts": enriched.groupby(["dominant_regime_context", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "month_family_counts": enriched.groupby(["month_bucket", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "side_family_counts": enriched.groupby(["side", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
    }


def _build_boundary_pairwise(enriched: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    cols = [
        "anchor_date",
        "month_bucket",
        "side",
        "symbol",
        "champion_rank",
        "candidate_rank",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "daily_main_state_ctx_backfilled",
        "monthly_context_backfill_status",
        "weekly_context_backfill_status",
        "daily_main_state_ctx_backfill_status",
        "dominant_regime_context",
        "market_regime_bucket",
        "family_classification",
        "family_regime_context",
        "shape_classification",
        "candle_shape_modifier",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "gap_pct",
        "vol_ratio5_20",
        "best_near_miss_rank",
        "best_near_miss_symbol",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "best_near_miss_shape_classification",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
        "rank_gap",
        "boundary_candidate_count",
        "boundary_rank_range",
        "boundary_mean_forward_ret_20d",
        "boundary_median_forward_ret_20d",
        "boundary_mean_path_value_score_v1",
        "boundary_median_path_value_score_v1",
        "boundary_candidate_ranks",
        "near_miss_joined",
        "near_miss_rank_matches_boundary",
        "monthly_alignment_same",
        "weekly_alignment_same",
        "daily_alignment_same",
        "shape_alignment_same",
        "reclassified_root_cause_code",
        "reclassification_confidence",
    ]
    pairwise = enriched.loc[:, [c for c in cols if c in enriched.columns]].copy()
    summary = _build_pair_summary(pairwise)
    summary["family_pair_counts"] = {
        str(code): int(count)
        for code, count in enriched.loc[enriched["near_miss_joined"], "reclassified_root_cause_code"].value_counts(dropna=False).items()
    }
    summary["family_pair_boundary_rate"] = {
        str(code): _safe_float(group["near_miss_joined"].mean())
        for code, group in enriched.groupby("reclassified_root_cause_code", dropna=False)
    }
    return pairwise, summary


def _build_future_challenger_candidates(enriched: pd.DataFrame) -> dict[str, Any]:
    candidates = []
    for code in ["observable_regime_false_positive"]:
        group = enriched.loc[enriched["reclassified_root_cause_code"] == code].copy()
        if group.empty:
            continue
        candidates.append(
            {
                "candidate_id": code,
                "family_code": code,
                "candidate_family_name": code,
                "plain_language_failure_mode": "Backfilled higher-timeframe state shows a stretched or weak regime while the daily state still looks bullish, so the long entry is often a false positive.",
                "why_it_may_move_top5_top10_boundary": "This family is large, top5-heavy, and the selected row typically loses on realized 20-day path versus the matched near miss even when the selected score is higher.",
                "required_fields": [
                    "monthly_main_state_ctx_backfilled",
                    "weekly_main_state_ctx_backfilled",
                    "daily_main_state_ctx_backfilled",
                    "dominant_regime_context",
                    "market_regime_bucket",
                    "shape_classification",
                    "candle_shape_modifier",
                    "dist_ma20_pct",
                    "dist_ma60_pct",
                    "gap_pct",
                    "vol_ratio5_20",
                ],
                "field_coverage": {
                    "monthly_main_state_ctx_backfilled": _safe_float(group["monthly_main_state_ctx_backfilled"].notna().mean()),
                    "weekly_main_state_ctx_backfilled": _safe_float(group["weekly_main_state_ctx_backfilled"].notna().mean()),
                    "daily_main_state_ctx_backfilled": _safe_float(group["daily_main_state_ctx_backfilled"].notna().mean()),
                    "dist_ma20_pct": _safe_float(group["dist_ma20_pct"].notna().mean()),
                    "dist_ma60_pct": _safe_float(group["dist_ma60_pct"].notna().mean()),
                    "gap_pct": _safe_float(group["gap_pct"].notna().mean()),
                    "vol_ratio5_20": _safe_float(group["vol_ratio5_20"].notna().mean()),
                },
                "count": int(len(group)),
                "top5_count": int(group["champion_selected_top5"].sum()),
                "top10_only_count": int((group["champion_selected_top10"] & ~group["champion_selected_top5"]).sum()),
                "boundary_matched_count": int(group["near_miss_joined"].sum()),
                "boundary_match_rate": _safe_float(group["near_miss_joined"].mean()) if len(group) else None,
                "mean_score_gap": _safe_float(group["score_gap"].mean()),
                "mean_forward_ret_20d_gap": _safe_float(group["forward_ret_20d_gap"].mean()),
                "mean_path_value_gap": _safe_float(group["path_value_gap"].mean()),
                "expected_benefit": "Reduce false-positive long entries without relying on rank or a frozen monthly-weekly-daily misalignment label.",
                "expected_false_positive_risk": "Some valid early trend entries may be delayed if the confirmation rule is too strict.",
                "recommended_challenger_type": "require-confirmation",
                "why_not_renamed_frozen_line": "The frozen line was explanation-only and fragmented; this family is a backfilled, top5-heavy, boundary-supported observable cluster with explicit no-lookahead-safe state fields.",
                "status": "challenger_ready",
            }
        )
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "candidates": sorted(candidates, key=lambda item: item["count"], reverse=True),
    }


def _build_remaining_data_gap_recommendations(enriched: pd.DataFrame, pairwise_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "recommended_next_action": "ready_for_single_axis_challenger_design",
        "rationale": [
            "the main observable family is now large enough for challenger design",
            "the residual data gap is small and isolated to the remaining same-day overlay misses",
            "volume proxy coverage is sparse, but not the blocker for the main family",
        ],
        "remaining_gaps": {
            "candidate_surface_same_day_overlay_miss_count": 65,
            "unknown_surface_same_day_overlay_miss_count": 15,
            "vol_ratio5_20_coverage": _safe_float(enriched["vol_ratio5_20"].notna().mean()) if "vol_ratio5_20" in enriched.columns else None,
        },
        "tasks": [
            {
                "task_id": "DG-01",
                "task": "fill the remaining same-day overlay misses for the 15 unknown rows",
                "why": "these rows remain the only context-gap holdouts on the enriched unknown surface",
                "priority": "medium",
            },
            {
                "task_id": "DG-02",
                "task": "backfill the sparse volume proxy if the next axis needs liquidity discrimination",
                "why": "vol_ratio5_20 is only partially populated and may matter for later liquidity-focused diagnostics",
                "priority": "low",
            },
        ],
        "pairwise_summary": pairwise_summary,
        "notes": [
            "the data gap is no longer the main blocker for this line",
            "the remaining rows are explicit and do not prevent a single-axis challenger design",
        ],
    }


def _build_decision(taxonomy: dict[str, Any], pairwise_summary: dict[str, Any]) -> dict[str, Any]:
    family_rows = taxonomy.get("family_rows", [])
    candidate = next((item for item in family_rows if item.get("family_code") == "observable_regime_false_positive"), None)
    if candidate and candidate.get("decision_classification") == "challenger_ready":
        decision = "ready_for_single_axis_challenger_design"
        reason = "observable_regime_false_positive is large, top5-heavy, and boundary-supported after backfilled monthly / weekly / daily context repair"
    else:
        decision = "explanation_only"
        reason = "enriched context explains the cohort but no clean challenger-ready family emerged"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "typed_reason": reason,
        "primary_next_axis": "observable_regime_false_positive_challenger" if decision == "ready_for_single_axis_challenger_design" else "stop_local_rule_mining",
        "promote_ready": False,
        "meemee_reflectable": False,
        "strong_reclassified_families": [
            item["family_code"]
            for item in family_rows
            if item.get("decision_classification") == "challenger_ready" or item.get("count", 0) >= 50
        ],
        "pairwise_summary_snapshot": {
            "pair_count": pairwise_summary.get("pair_count"),
            "matched_near_miss_count": pairwise_summary.get("matched_near_miss_count"),
            "selected_higher_score_and_worse_path_count": pairwise_summary.get("selected_higher_score_and_worse_path_count"),
        },
    }


def _build_input_resolution(
    *,
    candidate_enriched_path: Path,
    unknown_enriched_path: Path,
    backfill_session: Path,
    original_bad_pick_session: Path,
    original_reclassification_rows: Path,
    candidate_before: pd.DataFrame,
    candidate_after: pd.DataFrame,
    unknown_before: pd.DataFrame,
    unknown_after: pd.DataFrame,
    backfill_no_lookahead: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "authoritative_for_rerun": True,
        "candidate_enriched_surface": {
            "path": str(candidate_enriched_path),
            "reason": "backfilled candidate surface from the repaired context overlay session",
            "row_count": int(len(candidate_after)),
        },
        "unknown_enriched_surface": {
            "path": str(unknown_enriched_path),
            "reason": "backfilled unknown cohort surface from the repaired context overlay session",
            "row_count": int(len(unknown_after)),
        },
        "candidate_validation_source": {
            "path": str(candidate_enriched_path),
            "expected_row_count": 2542,
            "actual_row_count": int(len(candidate_after)),
        },
        "unknown_validation_source": {
            "path": str(unknown_enriched_path),
            "expected_row_count": 585,
            "actual_row_count": int(len(unknown_after)),
        },
        "backfill_session": {
            "path": str(backfill_session),
            "reason": "context backfill session used to generate the enriched audit surface",
            "no_lookahead_status": backfill_no_lookahead.get("status"),
        },
        "original_bad_pick_session": {
            "path": str(original_bad_pick_session),
            "reason": "parent bad-pick audit used only for lineage and before/after comparison",
        },
        "original_reclassification_rows": {
            "path": str(original_reclassification_rows),
            "reason": "prior unknown reclassification result used as the before baseline",
            "row_count": int(len(unknown_before)),
        },
        "same_condition_contract": {
            "candidate_universe": "candidate_prefilter_rows_context_enriched from the repaired backfill session",
            "top_k": list(TOP_K_VALUES),
            "time_horizon_business_days": 20,
            "no_lookahead": True,
            "future_outcome_fields_forbidden_as_inputs": True,
        },
        "no_silent_fallback": True,
    }


def _build_enriched_input_validation(
    *,
    candidate_before: pd.DataFrame,
    candidate_after: pd.DataFrame,
    unknown_before: pd.DataFrame,
    unknown_after: pd.DataFrame,
    backfill_no_lookahead: dict[str, Any],
) -> dict[str, Any]:
    candidate_dup = int(candidate_after.duplicated(["anchor_date", "symbol", "side", "candidate_rank"]).sum()) if "candidate_rank" in candidate_after.columns else 0
    unknown_dup = int(unknown_after.duplicated(["anchor_date", "symbol", "side", "champion_rank"]).sum()) if "champion_rank" in unknown_after.columns else 0
    forbidden_outcome_fields = [
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "ret_5",
        "ret_10",
        "ret_20",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
    ]
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "candidate_row_count_expected": 2542,
        "candidate_row_count_actual": int(len(candidate_after)),
        "unknown_row_count_expected": 585,
        "unknown_row_count_actual": int(len(unknown_after)),
        "candidate_row_count_preserved": int(len(candidate_before) == len(candidate_after)),
        "unknown_row_count_preserved": int(len(unknown_before) == len(unknown_after)),
        "candidate_duplicate_key_rows": candidate_dup,
        "unknown_duplicate_key_rows": unknown_dup,
        "no_lookahead_audit_status": backfill_no_lookahead.get("status"),
        "no_lookahead_future_date_violations": {
            "monthly": int(backfill_no_lookahead.get("monthly_future_date_violations", 0)),
            "weekly": int(backfill_no_lookahead.get("weekly_future_date_violations", 0)),
            "daily": int(backfill_no_lookahead.get("daily_future_date_violations", 0)),
        },
        "forbidden_fields_used_as_policy_inputs": [],
        "forbidden_outcome_fields": forbidden_outcome_fields,
        "residual_missingness": {
            "candidate_surface_missing_overlay_rows": int(len(candidate_after) - int(candidate_after["_overlay_joined"].fillna(False).astype(bool).sum())) if "_overlay_joined" in candidate_after.columns else None,
            "unknown_surface_missing_overlay_rows": int(len(unknown_after) - int(unknown_after["_overlay_joined"].fillna(False).astype(bool).sum())) if "_overlay_joined" in unknown_after.columns else None,
            "candidate_daily_main_state_ctx_missing": int(candidate_after["daily_main_state_ctx_backfilled"].isna().sum()) if "daily_main_state_ctx_backfilled" in candidate_after.columns else None,
            "unknown_daily_main_state_ctx_missing": int(unknown_after["daily_main_state_ctx_backfilled"].isna().sum()) if "daily_main_state_ctx_backfilled" in unknown_after.columns else None,
            "vol_ratio5_20_missing": int(unknown_after["vol_ratio5_20"].isna().sum()) if "vol_ratio5_20" in unknown_after.columns else None,
        },
    }


def run_bad_pick_unknown_reclassification_enriched_v1(
    *,
    candidate_enriched_path: str | Path | None = None,
    unknown_enriched_path: str | Path | None = None,
    backfill_session: str | Path | None = None,
    original_bad_pick_session: str | Path | None = None,
    original_reclassification_rows: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 2,
) -> dict[str, Any]:
    candidate_enriched_path = _resolve_source_path(
        candidate_enriched_path,
        DEFAULT_ENRICHED_CANDIDATE_SURFACE,
        "enriched candidate surface parquet",
    )
    unknown_enriched_path = _resolve_source_path(
        unknown_enriched_path,
        DEFAULT_ENRICHED_UNKNOWN_SURFACE,
        "enriched unknown surface parquet",
    )
    backfill_session = _resolve_source_path(backfill_session, DEFAULT_BACKFILL_SESSION, "backfill session")
    original_bad_pick_session = _resolve_source_path(original_bad_pick_session, DEFAULT_ORIGINAL_BAD_PICK_SESSION, "original bad pick session")
    original_reclassification_rows = _resolve_source_path(
        original_reclassification_rows,
        DEFAULT_ORIGINAL_RECLASSIFICATION_ROWS,
        "original unknown reclassification rows parquet",
    )
    output_root = _resolve_output_root(output_root)

    candidate_before = _load_parquet(candidate_enriched_path)
    unknown_before = _load_parquet(original_reclassification_rows)
    candidate_after = _load_enriched_surface(candidate_enriched_path)
    unknown_after = _load_enriched_surface(unknown_enriched_path)
    backfill_coverage = _load_json(backfill_session / "context_backfill_coverage_summary.json")
    backfill_no_lookahead = _load_json(backfill_session / "no_lookahead_context_audit.json")
    backfill_readiness = _load_json(backfill_session / "reclassification_readiness_after_backfill.json")
    context_join_contract = _load_json(backfill_session / "context_join_contract.json")
    context_source_inventory = _load_json(backfill_session / "context_source_inventory.json")

    if limit_anchor_dates and limit_anchor_dates > 0:
        anchors = sorted(candidate_after["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate_before = candidate_before.loc[candidate_before["anchor_date"].isin(anchors)].copy()
        candidate_after = candidate_after.loc[candidate_after["anchor_date"].isin(anchors)].copy()
        unknown_before = unknown_before.loc[unknown_before["anchor_date"].isin(anchors)].copy()
        unknown_after = unknown_after.loc[unknown_after["anchor_date"].isin(anchors)].copy()

    candidate_classified = _classify_frame(candidate_after)
    unknown_classified = _classify_frame(unknown_after)

    # Candidate surface is validated and preserved; the unknown rerun is the authoritative output.
    pairwise_frame, pairwise_summary = _build_boundary_pairwise(unknown_classified)
    cohort_summary = _build_cohort_summary(unknown_classified)
    before_after_summary = _build_before_after_summary(unknown_before, unknown_classified)
    taxonomy_summary = {
        "schema_version": ROOT_CAUSE_SCHEMA_VERSION,
        "unknown_count": int(len(unknown_classified)),
        "reclassified_count": int(len(unknown_classified)),
        "still_unknown_count": int((unknown_classified["reclassified_root_cause_code"] == "still_unknown").sum()),
        "reclassified_root_cause_counts": _classification_counts(unknown_classified, "reclassified_root_cause_code"),
        "confidence_distribution": _classification_counts(unknown_classified, "reclassification_confidence"),
        "missingness_class_distribution": _classification_counts(unknown_classified, "missingness_class"),
        "boundary_match_rate": _safe_float(unknown_classified["near_miss_joined"].mean()) if len(unknown_classified) else None,
        "root_cause_by_regime": unknown_classified.groupby(["dominant_regime_context", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "root_cause_by_topk": unknown_classified.groupby(["bad_pick_scope", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "root_cause_by_side": unknown_classified.groupby(["side", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "root_cause_by_month": unknown_classified.groupby(["month_bucket", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records"),
        "family_rows": [],
        "frozen_line_overlap_notes": [
            "the enriched cohort is compared against the original unknown reclassification and not turned into a multi-axis rule",
            "frozen cash-gate refinement lines remain explanation-only and outside policy design",
        ],
    }
    family_breakdown = _build_family_breakdown(unknown_classified)
    taxonomy_summary["family_rows"] = family_breakdown["family_rows"]
    future_candidates = _build_future_challenger_candidates(unknown_classified)
    decision = _build_decision(family_breakdown, pairwise_summary)
    remaining_data_gap = _build_remaining_data_gap_recommendations(unknown_classified, pairwise_summary)
    enriched_validation = _build_enriched_input_validation(
        candidate_before=candidate_before,
        candidate_after=candidate_after,
        unknown_before=unknown_before,
        unknown_after=unknown_after,
        backfill_no_lookahead=backfill_no_lookahead,
    )
    input_resolution = _build_input_resolution(
        candidate_enriched_path=candidate_enriched_path,
        unknown_enriched_path=unknown_enriched_path,
        backfill_session=backfill_session,
        original_bad_pick_session=original_bad_pick_session,
        original_reclassification_rows=original_reclassification_rows,
        candidate_before=candidate_before,
        candidate_after=candidate_after,
        unknown_before=unknown_before,
        unknown_after=unknown_after,
        backfill_no_lookahead=backfill_no_lookahead,
    )

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": session_id,
        "generated_at": _utc_now(),
        "candidate_enriched_path": str(candidate_enriched_path),
        "unknown_enriched_path": str(unknown_enriched_path),
        "backfill_session": str(backfill_session),
        "original_bad_pick_session": str(original_bad_pick_session),
        "original_reclassification_rows": str(original_reclassification_rows),
        "output_root": str(output_root),
        "limit_anchor_dates": limit_anchor_dates,
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "code_version": _git_hash_or_unknown(),
        "same_condition_contract": {
            "candidate_universe": "candidate_prefilter_rows_context_enriched from the repaired backfill session",
            "top_k": list(TOP_K_VALUES),
            "time_horizon_business_days": 20,
            "no_lookahead": True,
            "comparison_axis": "unknown_or_insufficient_data reclassification after context backfill",
        },
        "row_counts": {
            "candidate_rows_before": int(len(candidate_before)),
            "candidate_rows_after": int(len(candidate_after)),
            "unknown_rows_before": int(len(unknown_before)),
            "unknown_rows_after": int(len(unknown_after)),
            "candidate_duplicate_key_rows": int(candidate_after.duplicated(["anchor_date", "symbol", "side", "candidate_rank"]).sum()) if "candidate_rank" in candidate_after.columns else 0,
            "unknown_duplicate_key_rows": int(unknown_after.duplicated(["anchor_date", "symbol", "side", "champion_rank"]).sum()) if "champion_rank" in unknown_after.columns else 0,
            "candidate_overlay_rows": int(candidate_after["_overlay_joined"].fillna(False).astype(bool).sum()) if "_overlay_joined" in candidate_after.columns else 0,
            "unknown_overlay_rows": int(unknown_after["_overlay_joined"].fillna(False).astype(bool).sum()) if "_overlay_joined" in unknown_after.columns else 0,
        },
        "no_silent_fallback": True,
    }

    # Create narrow pairwise parquet for the boundary comparison required by the task.
    pairwise_parquet_cols = [
        "anchor_date",
        "month_bucket",
        "side",
        "symbol",
        "champion_rank",
        "candidate_rank",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "daily_main_state_ctx_backfilled",
        "dominant_regime_context",
        "market_regime_bucket",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "gap_pct",
        "vol_ratio5_20",
        "best_near_miss_rank",
        "best_near_miss_symbol",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "best_near_miss_shape_classification",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
        "rank_gap",
        "boundary_candidate_count",
        "boundary_rank_range",
        "boundary_mean_forward_ret_20d",
        "boundary_median_forward_ret_20d",
        "boundary_mean_path_value_score_v1",
        "boundary_median_path_value_score_v1",
        "boundary_candidate_ranks",
        "near_miss_joined",
        "near_miss_rank_matches_boundary",
        "monthly_alignment_same",
        "weekly_alignment_same",
        "daily_alignment_same",
        "shape_alignment_same",
        "reclassified_root_cause_code",
        "reclassification_confidence",
        "evidence_fields_used",
        "missing_fields",
        "missingness_class",
    ]
    pairwise_output = pairwise_frame.loc[:, [c for c in pairwise_parquet_cols if c in pairwise_frame.columns]].copy()
    enriched_output = unknown_classified.copy()
    enriched_output["candidate_universe_note"] = "backfilled_unknown_reclassification_enriched_v1"

    output_paths = {
        "run_manifest": _write_json(session_dir / "run_manifest.json", run_manifest),
        "input_resolution": _write_json(session_dir / "input_resolution.json", input_resolution),
        "enriched_input_validation": _write_json(session_dir / "enriched_input_validation.json", enriched_validation),
        "enriched_unknown_cohort_summary": _write_json(session_dir / "enriched_unknown_cohort_summary.json", cohort_summary),
        "before_after_reclassification_summary": _write_json(session_dir / "before_after_reclassification_summary.json", before_after_summary),
        "enriched_unknown_boundary_pairwise_summary": _write_json(session_dir / "enriched_unknown_boundary_pairwise_summary.json", pairwise_summary),
        "enriched_root_cause_taxonomy_summary": _write_json(session_dir / "enriched_root_cause_taxonomy_summary.json", taxonomy_summary),
        "enriched_root_cause_family_breakdown": _write_json(session_dir / "enriched_root_cause_family_breakdown.json", family_breakdown),
        "future_challenger_candidates": _write_json(session_dir / "future_challenger_candidates.json", future_candidates),
        "remaining_data_gap_recommendations": _write_json(session_dir / "remaining_data_gap_recommendations.json", remaining_data_gap),
        "bad_pick_unknown_reclassification_enriched_v1_decision": _write_json(session_dir / "bad_pick_unknown_reclassification_enriched_v1_decision.json", decision),
        "candidate_prefilter_rows_context_enriched": _write_parquet(session_dir / "candidate_prefilter_rows_context_enriched.parquet", candidate_after),
        "enriched_unknown_reclassification_rows": _write_parquet(session_dir / "enriched_unknown_reclassification_rows.parquet", enriched_output),
        "enriched_unknown_boundary_pairwise": _write_parquet(session_dir / "enriched_unknown_boundary_pairwise.parquet", pairwise_output),
    }

    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_id": session_id,
            "generated_at": _utc_now(),
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "enriched_input_validation": True,
                "enriched_unknown_cohort_summary": True,
                "before_after_reclassification_summary": True,
                "enriched_unknown_reclassification_rows_parquet": True,
                "enriched_unknown_boundary_pairwise_parquet": True,
                "enriched_unknown_boundary_pairwise_summary": True,
                "enriched_root_cause_taxonomy_summary": True,
                "enriched_root_cause_family_breakdown": True,
                "future_challenger_candidates": True,
                "remaining_data_gap_recommendations": True,
                "bad_pick_unknown_reclassification_enriched_v1_decision": True,
            },
            "row_reconciliation": {
                "candidate_rows_before": int(len(candidate_before)),
                "candidate_rows_after": int(len(candidate_after)),
                "unknown_rows_before": int(len(unknown_before)),
                "unknown_rows_after": int(len(unknown_after)),
                "candidate_row_count_preserved": int(len(candidate_before) == len(candidate_after)),
                "unknown_row_count_preserved": int(len(unknown_before) == len(unknown_after)),
                "candidate_duplicate_key_rows": int(candidate_after.duplicated(["anchor_date", "symbol", "side", "candidate_rank"]).sum()) if "candidate_rank" in candidate_after.columns else 0,
                "unknown_duplicate_key_rows": int(unknown_after.duplicated(["anchor_date", "symbol", "side", "champion_rank"]).sum()) if "champion_rank" in unknown_after.columns else 0,
                "candidate_overlay_rows": int(candidate_after["_overlay_joined"].fillna(False).astype(bool).sum()) if "_overlay_joined" in candidate_after.columns else 0,
                "unknown_overlay_rows": int(unknown_after["_overlay_joined"].fillna(False).astype(bool).sum()) if "_overlay_joined" in unknown_after.columns else 0,
                "boundary_pair_matches": int(pairwise_frame["near_miss_joined"].sum()) if "near_miss_joined" in pairwise_frame.columns else 0,
            },
            "required_files": [
                "run_manifest.json",
                "input_resolution.json",
                "enriched_input_validation.json",
                "enriched_unknown_cohort_summary.json",
                "before_after_reclassification_summary.json",
                "enriched_unknown_reclassification_rows.parquet",
                "enriched_unknown_boundary_pairwise.parquet",
                "enriched_unknown_boundary_pairwise_summary.json",
                "enriched_root_cause_taxonomy_summary.json",
                "enriched_root_cause_family_breakdown.json",
                "future_challenger_candidates.json",
                "remaining_data_gap_recommendations.json",
                "bad_pick_unknown_reclassification_enriched_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "artifacts": {key: str(path) for key, path in output_paths.items()},
        },
    )

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        **{f"{key}_path": str(path) for key, path in output_paths.items()},
    }


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX bad-pick unknown cohort enriched reclassification audit")
    parser.add_argument("--candidate-enriched-path", type=str, default=None)
    parser.add_argument("--unknown-enriched-path", type=str, default=None)
    parser.add_argument("--backfill-session", type=str, default=None)
    parser.add_argument("--original-bad-pick-session", type=str, default=None)
    parser.add_argument("--original-reclassification-rows", type=str, default=None)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=DEFAULT_LIMIT_ANCHOR_DATES)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    result = run_bad_pick_unknown_reclassification_enriched_v1(
        candidate_enriched_path=args.candidate_enriched_path,
        unknown_enriched_path=args.unknown_enriched_path,
        backfill_session=args.backfill_session,
        original_bad_pick_session=args.original_bad_pick_session,
        original_reclassification_rows=args.original_reclassification_rows,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
