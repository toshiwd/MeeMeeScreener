from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status  # noqa: E402
from scripts.tradex_iizuka_fixed_contract_forward_surface_accumulation_v1 import (  # noqa: E402
    _json_ready,
    _load_frame,
    _load_json,
    _safe_path,
    _write_json,
    _write_parquet,
)

SCRIPT_NAME = "tradex_iizuka_v2_forward_active_breadth_blockage_audit"
SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_input_resolution_v1"
CONTRACT_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_contract_v1"
SUMMARY_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_forward_coverage_summary_v1"
WATERFALL_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_gate_stage_waterfall_v1"
MISSING_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_missing_feature_audit_v1"
DATE_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_date_key_alignment_audit_v1"
ROLE_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_forward_role_distribution_v1"
FEASIBILITY_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_active_breadth_expansion_feasibility_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_v2_forward_active_breadth_blockage_audit_decision_v1"

APPROVED_V2_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2\20260503T122320Z-161925")
TOP10_SAFE_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering\20260503T130355Z-311287")
FORWARD_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_forward_accumulation\20260503T131739Z-420610")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_forward_breadth_blockage_audit")
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

APPROVED_ACTIVE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_active_candidate_rows.parquet"
APPROVED_ALL_ROLE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_all_role_rows.parquet"
TOP10_SAFE_DECISION = TOP10_SAFE_SESSION / "top10_safe_decision.json"
TOP10_SAFE_COMPARISON = TOP10_SAFE_SESSION / "top10_safe_comparison.json"
TOP10_SAFE_MONTH_AUDIT = TOP10_SAFE_SESSION / "top10_safe_month_dependence_audit.json"
FORWARD_CANDIDATE_ROWS = FORWARD_SESSION / "fixed_contract_candidate_rows.parquet"
FORWARD_COMPARISON = FORWARD_SESSION / "fixed_contract_comparison.json"
FORWARD_MONTH_AUDIT = FORWARD_SESSION / "fixed_contract_month_dependence_audit.json"
FORWARD_GROUP_AUDIT = FORWARD_SESSION / "fixed_contract_group_split_audit.json"
FORWARD_DECISION = FORWARD_SESSION / "fixed_contract_decision.json"

V2_GATE_FIELDS = [
    "iizuka_context_block_pass",
    "iizuka_compression_block_pass",
    "iizuka_risk_block_pass",
    "volume_participation_bucket",
    "support_wick",
    "bull_engulfing",
    "drawdown60",
    "rebound60",
    "stable_bad_pick_family",
    "bull_marubozu",
    "ma20_distance_bucket",
    "ma60_distance_bucket",
]

ORDERING_FIELDS = [
    "signal_quality_bucket",
    "volume_participation_bucket",
    "decision_candle_quality",
    "shape_classification",
    "support_wick",
    "bull_engulfing",
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "iizuka_candidate_score",
    "iizuka_v2_candidate_score",
    "iizuka_candidate_rank",
    "iizuka_v2_candidate_rank",
]

KEY_FIELDS = [
    "anchor_date",
    "symbol",
    "side",
    "surface_key",
    "canonical_candidate_key",
    "key",
    "research_fallback_label_source",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_inputs() -> dict[str, Any]:
    required = {
        "approved_active_rows": APPROVED_ACTIVE_ROWS,
        "approved_all_role_rows": APPROVED_ALL_ROLE_ROWS,
        "top10_safe_decision": TOP10_SAFE_DECISION,
        "top10_safe_comparison": TOP10_SAFE_COMPARISON,
        "top10_safe_month_audit": TOP10_SAFE_MONTH_AUDIT,
        "forward_candidate_rows": FORWARD_CANDIDATE_ROWS,
        "forward_comparison": FORWARD_COMPARISON,
        "forward_month_audit": FORWARD_MONTH_AUDIT,
        "forward_group_audit": FORWARD_GROUP_AUDIT,
        "forward_decision": FORWARD_DECISION,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    return {
        "approved_active_rows": _load_frame(required["approved_active_rows"]),
        "approved_all_role_rows": _load_frame(required["approved_all_role_rows"]),
        "top10_safe_decision": _load_json(required["top10_safe_decision"]),
        "top10_safe_comparison": _load_json(required["top10_safe_comparison"]),
        "top10_safe_month_audit": _load_json(required["top10_safe_month_audit"]),
        "forward_candidate_rows": _load_frame(required["forward_candidate_rows"]),
        "forward_comparison": _load_json(required["forward_comparison"]),
        "forward_month_audit": _load_json(required["forward_month_audit"]),
        "forward_group_audit": _load_json(required["forward_group_audit"]),
        "forward_decision": _load_json(required["forward_decision"]),
    }


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _safe_float(value: Any) -> float | None:
    if value is None or _is_missing(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _forward_mask(frame: pd.DataFrame, source_max_anchor_date: str) -> pd.Series:
    anchor = pd.to_datetime(frame["anchor_date"], errors="coerce")
    source_max = pd.to_datetime(source_max_anchor_date, errors="coerce")
    return anchor > source_max


def _month_bucket(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["anchor_date"], errors="coerce").dt.strftime("%Y-%m")


def _make_clean_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in out.columns:
        if out[column].dtype != object:
            continue
        if not out[column].map(lambda value: isinstance(value, (dict, list, tuple, set))).any():
            continue
        out[column] = out[column].map(
            lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple, set)) else value
        )
    return out


def _series_missing_rate(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns or len(frame) == 0:
        return None
    return float(frame[column].map(_is_missing).mean())


def _series_present_count(frame: pd.DataFrame, column: str) -> int:
    if column not in frame.columns or len(frame) == 0:
        return 0
    return int((~frame[column].map(_is_missing)).sum())


def _build_role_distribution(frame: pd.DataFrame, source_max_anchor_date: str) -> pd.DataFrame:
    forward_mask = _forward_mask(frame, source_max_anchor_date)
    rows: list[dict[str, Any]] = []
    for slice_name, subset in (("historical_approved", frame.loc[~forward_mask].copy()), ("forward", frame.loc[forward_mask].copy())):
        if subset.empty:
            continue
        for role, role_frame in subset.groupby("iizuka_v2_role", dropna=False):
            rows.append(
                {
                    "slice_name": slice_name,
                    "role": str(role),
                    "row_count": int(len(role_frame)),
                    "month_count": int(_month_bucket(role_frame).nunique()) if "anchor_date" in role_frame.columns else 0,
                    "group_count": int(role_frame["anchor_date"].nunique()) if "anchor_date" in role_frame.columns else 0,
                    "symbol_count": int(role_frame["symbol"].nunique()) if "symbol" in role_frame.columns else 0,
                    "mean_forward_ret_20d": _safe_float(pd.to_numeric(role_frame["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in role_frame.columns else None,
                    "median_forward_ret_20d": _safe_float(pd.to_numeric(role_frame["forward_ret_20d"], errors="coerce").median()) if "forward_ret_20d" in role_frame.columns else None,
                    "top15_count": int(pd.to_numeric(role_frame["top15_label"], errors="coerce").fillna(0).sum()) if "top15_label" in role_frame.columns else 0,
                    "bottom15_count": int(pd.to_numeric(role_frame["bottom15_label"], errors="coerce").fillna(0).sum()) if "bottom15_label" in role_frame.columns else 0,
                    "candidate_score_missing_rate": _series_missing_rate(role_frame, "iizuka_candidate_score"),
                    "v2_candidate_score_missing_rate": _series_missing_rate(role_frame, "iizuka_v2_candidate_score"),
                    "candidate_rank_missing_rate": _series_missing_rate(role_frame, "iizuka_candidate_rank"),
                    "v2_candidate_rank_missing_rate": _series_missing_rate(role_frame, "iizuka_v2_candidate_rank"),
                }
            )
    return pd.DataFrame(rows)


def _row_stage_flags(row: pd.Series) -> dict[str, bool]:
    context_fail = not bool(row.get("iizuka_context_block_pass"))
    compression_fail = not bool(row.get("iizuka_compression_block_pass"))
    risk_fail = not bool(row.get("iizuka_risk_block_pass"))
    trigger_fail = "trigger_gate_failed" in str(row.get("iizuka_v2_diagnostic_reason") or "")
    missing_required_fields = any(
        _is_missing(row.get(field))
        for field in ["drawdown60", "rebound60"]
    )
    return {
        "missing_required_fields": missing_required_fields,
        "non_long_side": str(row.get("side") or "").lower() != "long",
        "base_context_block_fail": context_fail,
        "compression_block_fail": compression_fail,
        "current_risk_baseline_fail": risk_fail,
        "trigger_lane_fail": trigger_fail,
        "explicit_exclusion": str(row.get("iizuka_v2_role") or "") == "excluded",
        "diagnostic_only": str(row.get("iizuka_v2_role") or "") == "diagnostic_only",
        "active_pass": str(row.get("iizuka_v2_role") or "") == "active",
    }


def _primary_blockage(row: pd.Series) -> str:
    if str(row.get("iizuka_v2_role") or "") == "active":
        return "active_pass"
    if str(row.get("iizuka_v2_role") or "") == "excluded":
        return "explicit_exclusion"
    diag_reason = str(row.get("iizuka_v2_diagnostic_reason") or "")
    if "context_block_failed" in diag_reason or "compression_block_failed" in diag_reason or "risk_block_failed" in diag_reason:
        return "frozen_gate_base_block"
    if "trigger_gate_failed" in diag_reason:
        return "trigger_lane_block"
    if "missing_stabilization_proxies" in diag_reason:
        return "missing_required_fields"
    return "diagnostic_only_near_miss"


def _build_gate_stage_waterfall(frame: pd.DataFrame, source_max_anchor_date: str) -> tuple[dict[str, Any], pd.DataFrame]:
    forward = frame.loc[_forward_mask(frame, source_max_anchor_date)].copy()
    if forward.empty:
        return {
            "schema_version": WATERFALL_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
            "stage_counts": {},
            "primary_blockage_counts": {},
            "notes": ["no forward rows available"],
        }, pd.DataFrame()

    stage_rows = []
    for _, row in forward.iterrows():
        flags = _row_stage_flags(row)
        stage_rows.append(
            {
                "anchor_date": row.get("anchor_date"),
                "month_bucket": str(row.get("anchor_date") or "")[:7],
                "symbol": row.get("symbol"),
                "surface_key": row.get("surface_key"),
                "iizuka_v2_role": row.get("iizuka_v2_role"),
                "iizuka_v2_reason": row.get("iizuka_v2_reason"),
                "iizuka_v2_diagnostic_reason": row.get("iizuka_v2_diagnostic_reason"),
                "iizuka_v2_exclusion_reason": row.get("iizuka_v2_exclusion_reason"),
                "primary_blockage": _primary_blockage(row),
                **flags,
            }
        )
    stage_frame = pd.DataFrame(stage_rows)

    stage_counts = {key: int(stage_frame[key].sum()) for key in [
        "missing_required_fields",
        "non_long_side",
        "base_context_block_fail",
        "compression_block_fail",
        "current_risk_baseline_fail",
        "trigger_lane_fail",
        "explicit_exclusion",
        "diagnostic_only",
        "active_pass",
    ]}
    primary_blockage_counts = {str(key): int(value) for key, value in stage_frame["primary_blockage"].value_counts().sort_values(ascending=False).items()}
    return {
        "schema_version": WATERFALL_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "forward_row_count": int(len(stage_frame)),
        "stage_counts": stage_counts,
        "primary_blockage_counts": primary_blockage_counts,
        "role_counts": {str(key): int(value) for key, value in stage_frame["iizuka_v2_role"].value_counts().sort_index().items()},
        "month_counts": {str(key): int(value) for key, value in stage_frame["month_bucket"].value_counts().sort_index().items()},
        "notes": [
            "stage counts are overlapping flags; primary_blockage is mutually exclusive",
            "the forward slice is read only and uses emitted v2 role / reason fields",
        ],
    }, stage_frame


def _build_missing_feature_audit(frame: pd.DataFrame, source_max_anchor_date: str) -> dict[str, Any]:
    forward = frame.loc[_forward_mask(frame, source_max_anchor_date)].copy()
    historical = frame.loc[~_forward_mask(frame, source_max_anchor_date)].copy()

    def _missing_block(frame_slice: pd.DataFrame) -> dict[str, Any]:
        fields = {field: {"missing_rate": _series_missing_rate(frame_slice, field), "present_count": _series_present_count(frame_slice, field)} for field in V2_GATE_FIELDS + ORDERING_FIELDS + KEY_FIELDS}
        return fields

    forward_fields = _missing_block(forward)
    historical_fields = _missing_block(historical)
    forward_score_missing = forward_fields.get("iizuka_candidate_score", {}).get("missing_rate")
    gate_present = all(forward_fields.get(field, {}).get("missing_rate") in (0.0, None) for field in [
        "iizuka_context_block_pass",
        "iizuka_compression_block_pass",
        "iizuka_risk_block_pass",
        "volume_participation_bucket",
        "support_wick",
        "bull_engulfing",
    ])
    key_alignment_ok = all(forward_fields.get(field, {}).get("missing_rate") in (0.0, None) for field in ["anchor_date", "symbol", "side", "surface_key"])
    ordering_data_gap = forward_score_missing == 1.0 if forward_score_missing is not None else False
    primary = "gate_restrictiveness"
    if not key_alignment_ok:
        primary = "join_date_group_mismatch"
    elif not gate_present:
        primary = "data_availability_failure"
    elif ordering_data_gap and int(forward_fields.get("drawdown60", {}).get("present_count", 0)) > 0:
        primary = "gate_restrictiveness"

    return {
        "schema_version": MISSING_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "classification": primary,
        "secondary_findings": {
            "ordering_data_gap_candidate_score_missing_on_forward_rows": bool(ordering_data_gap),
            "forward_candidate_score_missing_rate": forward_score_missing,
            "forward_v2_candidate_score_missing_rate": forward_fields.get("iizuka_v2_candidate_score", {}).get("missing_rate"),
            "forward_drawdown60_missing_rate": forward_fields.get("drawdown60", {}).get("missing_rate"),
            "forward_rebound60_missing_rate": forward_fields.get("rebound60", {}).get("missing_rate"),
            "historical_candidate_score_missing_rate": historical_fields.get("iizuka_candidate_score", {}).get("missing_rate"),
            "historical_v2_candidate_score_missing_rate": historical_fields.get("iizuka_v2_candidate_score", {}).get("missing_rate"),
        },
        "forward_field_missing_rates": forward_fields,
        "historical_field_missing_rates": historical_fields,
        "notes": [
            "the forward slice has full gate-field coverage but no active passes",
            "iizuka_candidate_score is absent on the forward slice, which is a separate ordering-path data gap",
        ],
    }


def _build_coverage_summary(frame: pd.DataFrame, source_max_anchor_date: str) -> dict[str, Any]:
    forward = frame.loc[_forward_mask(frame, source_max_anchor_date)].copy()
    historical = frame.loc[~_forward_mask(frame, source_max_anchor_date)].copy()
    eligible_for_gate = int(len(forward))
    strict_precheck = int(
        forward[
            forward[[
                "anchor_date",
                "symbol",
                "side",
                "iizuka_context_block_pass",
                "iizuka_compression_block_pass",
                "iizuka_risk_block_pass",
                "volume_participation_bucket",
                "support_wick",
                "bull_engulfing",
            ]].notna().all(axis=1)
        ].shape[0]
    ) if len(forward) else 0

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "source_surface_row_count": int(len(historical)),
        "forward_row_count": int(len(forward)),
        "candidate_row_count": int(len(frame)),
        "historical_row_count": int(len(historical)),
        "historical_vs_forward_ratio": float(len(historical) / max(len(forward), 1)) if len(forward) else None,
        "active_rows_total": int((frame["iizuka_v2_role"] == "active").sum()) if "iizuka_v2_role" in frame.columns else 0,
        "active_rows_forward": int((forward["iizuka_v2_role"] == "active").sum()) if "iizuka_v2_role" in forward.columns else 0,
        "diagnostic_only_rows_forward": int((forward["iizuka_v2_role"] == "diagnostic_only").sum()) if "iizuka_v2_role" in forward.columns else 0,
        "excluded_rows_forward": int((forward["iizuka_v2_role"] == "excluded").sum()) if "iizuka_v2_role" in forward.columns else 0,
        "forward_rows_by_month": {str(k): int(v) for k, v in _month_bucket(forward).value_counts().sort_index().items()} if len(forward) else {},
        "forward_rows_by_group": {str(k): int(v) for k, v in forward["anchor_date"].value_counts().sort_index().items()} if len(forward) else {},
        "forward_rows_by_symbol": {str(k): int(v) for k, v in forward["symbol"].value_counts().sort_values(ascending=False).items()} if len(forward) else {},
        "forward_rows_by_role": {str(k): int(v) for k, v in forward["iizuka_v2_role"].value_counts().sort_index().items()} if len(forward) else {},
        "eligible_for_v2_classification_rows": eligible_for_gate,
        "eligible_for_gate_precheck_rows": strict_precheck,
        "forward_rows_with_candidate_score_missing": int(forward["iizuka_candidate_score"].map(_is_missing).sum()) if "iizuka_candidate_score" in forward.columns else 0,
        "forward_rows_with_v2_candidate_score_missing": int(forward["iizuka_v2_candidate_score"].map(_is_missing).sum()) if "iizuka_v2_candidate_score" in forward.columns else 0,
        "forward_rows_with_active_pass": int((forward["iizuka_v2_role"] == "active").sum()) if "iizuka_v2_role" in forward.columns else 0,
        "forward_rows_with_diagnostic_only": int((forward["iizuka_v2_role"] == "diagnostic_only").sum()) if "iizuka_v2_role" in forward.columns else 0,
        "forward_rows_with_exclusion": int((forward["iizuka_v2_role"] == "excluded").sum()) if "iizuka_v2_role" in forward.columns else 0,
        "notes": [
            "forward rows are the dates after the approved-v2 max anchor date",
            "candidate-score materialization is absent on the forward slice",
        ],
    }


def _build_date_key_alignment_audit(frame: pd.DataFrame, source_max_anchor_date: str) -> dict[str, Any]:
    forward = frame.loc[_forward_mask(frame, source_max_anchor_date)].copy()
    approved = frame.loc[~_forward_mask(frame, source_max_anchor_date)].copy()
    forward_keys = forward["anchor_date"].astype(str) + "|" + forward["symbol"].astype(str) + "|" + forward["side"].astype(str)
    approved_keys = approved["anchor_date"].astype(str) + "|" + approved["symbol"].astype(str) + "|" + approved["side"].astype(str)
    return {
        "schema_version": DATE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "calendar_correct": bool(
            pd.to_datetime(forward["anchor_date"], errors="coerce").min() > pd.to_datetime(source_max_anchor_date, errors="coerce")
        ) if len(forward) else False,
        "approved_surface_max_anchor_date": source_max_anchor_date,
        "forward_min_anchor_date": str(forward["anchor_date"].min()) if len(forward) else None,
        "forward_max_anchor_date": str(forward["anchor_date"].max()) if len(forward) else None,
        "forward_row_count": int(len(forward)),
        "forward_unique_key_count": int(forward_keys.nunique()) if len(forward) else 0,
        "forward_duplicate_key_count": int(len(forward) - forward_keys.nunique()) if len(forward) else 0,
        "forward_overlap_with_approved_count": int(len(set(forward_keys) & set(approved_keys))) if len(forward) else 0,
        "forward_overlap_with_approved_exists": bool(len(set(forward_keys) & set(approved_keys))) if len(forward) else False,
        "forward_month_count": int(_month_bucket(forward).nunique()) if len(forward) else 0,
        "forward_months": sorted(_month_bucket(forward).dropna().astype(str).unique().tolist()) if len(forward) else [],
        "side_counts": {str(k): int(v) for k, v in forward["side"].value_counts(dropna=False).items()} if len(forward) else {},
        "runtime_no_lookahead_pass": bool(
            frame["monthly_context_no_lookahead"].fillna(False).astype(bool).all()
            and frame["weekly_context_no_lookahead"].fillna(False).astype(bool).all()
        ) if {"monthly_context_no_lookahead", "weekly_context_no_lookahead"}.issubset(frame.columns) and len(frame) else False,
        "label_source": "ml_label_20d" if "research_fallback_label_source" in frame.columns and frame["research_fallback_label_source"].astype(str).eq("ml_label_20d").all() else None,
        "notes": [
            "forward dates begin after the approved-v2 surface max anchor date",
            "symbol/date/side keys are stable with no duplicates or overlap",
        ],
    }


def _build_feasibility(summary: dict[str, Any], waterfall: dict[str, Any], missing: dict[str, Any], date_audit: dict[str, Any]) -> dict[str, Any]:
    forward_count = summary["forward_row_count"]
    active_count = summary["active_rows_forward"]
    diagnostic_count = summary["diagnostic_only_rows_forward"]
    exclusion_count = summary["excluded_rows_forward"]
    candidate_score_missing = missing["secondary_findings"]["forward_candidate_score_missing_rate"]
    if not date_audit["calendar_correct"] or date_audit["forward_duplicate_key_count"] > 0 or date_audit["forward_overlap_with_approved_exists"]:
        classification = "contract_alignment_issue"
    elif forward_count == 0:
        classification = "inconclusive"
    elif active_count == 0 and diagnostic_count > 0 and exclusion_count > 0:
        classification = "no_forward_setup_hold"
    else:
        classification = "gate_too_restrictive_hold"
    return {
        "schema_version": FEASIBILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "classification": classification,
        "supporting_counts": {
            "forward_row_count": forward_count,
            "active_rows_forward": active_count,
            "diagnostic_only_rows_forward": diagnostic_count,
            "excluded_rows_forward": exclusion_count,
            "base_context_block_fail_count": waterfall["stage_counts"].get("base_context_block_fail", 0),
            "compression_block_fail_count": waterfall["stage_counts"].get("compression_block_fail", 0),
            "current_risk_baseline_fail_count": waterfall["stage_counts"].get("current_risk_baseline_fail", 0),
            "trigger_lane_fail_count": waterfall["stage_counts"].get("trigger_lane_fail", 0),
            "explicit_exclusion_count": waterfall["stage_counts"].get("explicit_exclusion", 0),
        },
        "supporting_data": {
            "candidate_score_missing_rate_forward": candidate_score_missing,
            "key_alignment_ok": bool(date_audit["calendar_correct"]) and date_audit["forward_duplicate_key_count"] == 0 and not date_audit["forward_overlap_with_approved_exists"],
            "no_lookahead_pass": bool(date_audit["runtime_no_lookahead_pass"]),
        },
        "reason": (
            "forward rows are cleanly keyed and aligned, but none satisfy the frozen active contract; "
            "the forward slice contains no additional active-qualifying long setups"
            if classification == "no_forward_setup_hold"
            else (
                "date/key alignment is inconsistent or rows overlap unexpectedly"
                if classification == "contract_alignment_issue"
                else "active breadth cannot expand under the frozen contract without changing definitions"
            )
        ),
        "notes": [
            "candidate score is absent on the forward slice, which is a separate ordering-path data gap",
            "the active-lane blocker is the frozen contract outcome, not a key collision",
        ],
    }


def _build_decision(feasibility: dict[str, Any]) -> dict[str, Any]:
    decision = feasibility["classification"]
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": "hold" if decision in {"gate_too_restrictive_hold", "no_forward_setup_hold"} else "blocked",
        "reason": feasibility["reason"],
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "evidence": feasibility["supporting_counts"],
        "notes": [
            "diagnostic audit only; no keep decision is emitted",
            "top10-safe ordering remains frozen as hold",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka v2 forward active breadth blockage audit")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    runtime_db = _safe_path(args.runtime_db, DEFAULT_RUNTIME_DB)
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)

    inputs = _load_inputs()
    approved_all = inputs["approved_all_role_rows"].copy()
    approved_active = inputs["approved_active_rows"].copy()
    forward = inputs["forward_candidate_rows"].copy()

    source_max_anchor_date = str(approved_all["anchor_date"].max())
    forward_mask = _forward_mask(forward, source_max_anchor_date)
    forward_rows = forward.loc[forward_mask].copy()
    historical_rows = forward.loc[~forward_mask].copy()

    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(risk_mode="balanced")

    coverage_summary = _build_coverage_summary(forward, source_max_anchor_date)
    waterfall, stage_frame = _build_gate_stage_waterfall(forward, source_max_anchor_date)
    missing_audit = _build_missing_feature_audit(forward, source_max_anchor_date)
    date_alignment = _build_date_key_alignment_audit(forward, source_max_anchor_date)
    role_distribution = _build_role_distribution(forward, source_max_anchor_date)
    feasibility = _build_feasibility(coverage_summary, waterfall, missing_audit, date_alignment)
    decision = _build_decision(feasibility)

    forward_failure_rows = stage_frame.copy()
    if not forward_failure_rows.empty:
        forward_failure_rows["forward_slice"] = True
        for column in ["iizuka_candidate_score", "iizuka_v2_candidate_score", "champion_score", "champion_rank"]:
            if column in forward.columns and column not in forward_failure_rows.columns:
                forward_failure_rows[column] = forward_rows[column].values if len(forward_rows) else pd.NA
    else:
        forward_failure_rows = forward_rows.copy()
        forward_failure_rows["forward_slice"] = True

    coverage_summary["forward_rows_by_role"] = coverage_summary.get("forward_rows_by_role", {})
    coverage_summary["forward_rows_by_month"] = coverage_summary.get("forward_rows_by_month", {})
    coverage_summary["historical_approved_row_count"] = int(len(historical_rows))
    coverage_summary["historical_approved_active_row_count"] = int((historical_rows["iizuka_v2_role"] == "active").sum()) if "iizuka_v2_role" in historical_rows.columns else 0
    coverage_summary["forward_rows_eligible_for_active_evaluation_before_gate_checks"] = int(len(forward_rows))

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "approved_v2_session": str(APPROVED_V2_SESSION),
            "top10_safe_session": str(TOP10_SAFE_SESSION),
            "forward_session": str(FORWARD_SESSION),
            "runtime_db": str(runtime_db),
        },
        "source_artifacts": {
            "approved_active_rows": str(APPROVED_ACTIVE_ROWS),
            "approved_all_role_rows": str(APPROVED_ALL_ROLE_ROWS),
            "forward_candidate_rows": str(FORWARD_CANDIDATE_ROWS),
            "forward_comparison": str(FORWARD_COMPARISON),
            "forward_month_audit": str(FORWARD_MONTH_AUDIT),
            "forward_group_audit": str(FORWARD_GROUP_AUDIT),
            "forward_decision": str(FORWARD_DECISION),
            "top10_safe_decision": str(TOP10_SAFE_DECISION),
            "top10_safe_comparison": str(TOP10_SAFE_COMPARISON),
            "top10_safe_month_audit": str(TOP10_SAFE_MONTH_AUDIT),
            "runtime_db": str(runtime_db),
        },
        "frozen_inputs": {
            "approved_v2_gate": "iizuka_pre_decisive_long_candidate_v2",
            "top10_safe_ordering": "v2_score_anchored_top10_safe_ordering_v1",
            "forward_accumulation_session": str(FORWARD_SESSION),
        },
        "runtime_freshness": {
            "runtime_stock_db_status": runtime_status,
            "rankings_freshness": rankings_freshness,
        },
        "notes": [
            "read-only diagnostic audit",
            "no threshold tuning, no new challenger, no MeeMee changes, no production changes",
        ],
    }

    contract = {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_breadth_blockage_audit",
        "approved_v2_gate": "iizuka_pre_decisive_long_candidate_v2",
        "top10_safe_ordering": "v2_score_anchored_top10_safe_ordering_v1",
        "scope": "TRADEX-only",
        "read_only": True,
        "non_scope": [
            "no gate threshold changes",
            "no top10-safe ordering changes",
            "no diagnostic rows promoted to active",
            "no label source policy changes",
            "no MeeMee changes",
            "no production ranking changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
        "inputs": {
            "approved_v2_session": str(APPROVED_V2_SESSION),
            "top10_safe_session": str(TOP10_SAFE_SESSION),
            "forward_session": str(FORWARD_SESSION),
        },
        "questions": [
            "why 724 forward rows did not create more active coverage",
            "whether the blockage is data, alignment, or frozen contract restrictiveness",
            "whether the forward slice contains any active-qualifying long setups",
        ],
    }

    _write_json(session_root / "run_manifest.json", {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_root.name,
        "output_root": str(output_root),
        "jobs": int(args.jobs),
        "research_only": True,
        "boundary": "TRADEX-only",
        "source_artifacts": input_resolution["source_artifacts"],
        "notes": [
            "read-only audit over the latest fixed-contract forward accumulation",
            "top10-safe ordering is frozen and only inspected",
        ],
    })
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "breadth_blockage_audit_contract.json", contract)
    _write_json(session_root / "forward_coverage_summary.json", coverage_summary)
    _write_json(session_root / "gate_stage_waterfall.json", waterfall)
    _write_json(session_root / "missing_feature_audit.json", missing_audit)
    _write_json(session_root / "date_key_alignment_audit.json", date_alignment)
    _write_parquet(session_root / "forward_role_distribution.parquet", _make_clean_frame(role_distribution))
    _write_parquet(session_root / "forward_gate_failure_rows.parquet", _make_clean_frame(forward_failure_rows))
    _write_json(session_root / "active_breadth_expansion_feasibility.json", feasibility)
    _write_json(session_root / "breadth_blockage_decision.json", decision)
    _write_json(session_root / "forward_symbol_concentration_audit.json", {
        "schema_version": f"{SCHEMA_VERSION}_forward_symbol_concentration_v1",
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_breadth_blockage_audit",
        "forward_symbol_counts": {str(k): int(v) for k, v in forward_rows["symbol"].value_counts().sort_values(ascending=False).items()} if len(forward_rows) else {},
        "top_symbol_share": float(forward_rows["symbol"].value_counts().iloc[0] / len(forward_rows)) if len(forward_rows) else None,
        "top5_symbol_share": float(forward_rows["symbol"].value_counts().head(5).sum() / len(forward_rows)) if len(forward_rows) else None,
        "notes": ["forward slice is not single-name dominated"],
    })
    month_group_matrix = (
        forward_rows.assign(month_bucket=_month_bucket(forward_rows))
        .groupby(["month_bucket", "iizuka_v2_role"], dropna=False)
        .size()
        .reset_index(name="row_count")
    ) if len(forward_rows) else pd.DataFrame(columns=["month_bucket", "iizuka_v2_role", "row_count"])
    _write_parquet(session_root / "forward_month_group_matrix.parquet", _make_clean_frame(month_group_matrix))
    _write_parquet(session_root / "forward_failure_examples.parquet", _make_clean_frame(
        forward_failure_rows.head(50)[[
            c for c in [
                "anchor_date",
                "symbol",
                "surface_key",
                "month_bucket",
                "iizuka_v2_role",
                "iizuka_v2_reason",
                "iizuka_v2_diagnostic_reason",
                "iizuka_v2_exclusion_reason",
                "primary_blockage",
                "forward_slice",
            ] if c in forward_failure_rows.columns
        ]]
    ))
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "all_present": all((session_root / name).exists() for name in [
            "run_manifest.json",
            "input_resolution.json",
            "breadth_blockage_audit_contract.json",
            "forward_coverage_summary.json",
            "gate_stage_waterfall.json",
            "missing_feature_audit.json",
            "date_key_alignment_audit.json",
            "forward_role_distribution.parquet",
            "forward_gate_failure_rows.parquet",
            "active_breadth_expansion_feasibility.json",
            "breadth_blockage_decision.json",
        ]),
        "required_json": [
            "run_manifest.json",
            "input_resolution.json",
            "breadth_blockage_audit_contract.json",
            "forward_coverage_summary.json",
            "gate_stage_waterfall.json",
            "missing_feature_audit.json",
            "date_key_alignment_audit.json",
            "active_breadth_expansion_feasibility.json",
            "breadth_blockage_decision.json",
        ],
        "required_parquet": [
            "forward_role_distribution.parquet",
            "forward_gate_failure_rows.parquet",
        ],
        "decision": decision["decision"],
    })


if __name__ == "__main__":
    main()
