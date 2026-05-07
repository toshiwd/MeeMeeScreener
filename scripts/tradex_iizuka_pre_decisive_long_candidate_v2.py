from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_iizuka_pre_decisive_contract_redesign_audit_v1 import (  # noqa: E402
    _bucketize_frame,
    _json_ready,
    _load_frame,
    _load_json,
    _metric_bundle,
    _safe_bool,
    _safe_float,
    _write_json,
    _write_parquet,
)

SCRIPT_NAME = "tradex_iizuka_pre_decisive_long_candidate_v2"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_input_resolution_v1"
IMPLEMENTATION_SUMMARY_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_contract_implementation_summary_v1"
SURFACE_SUMMARY_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_surface_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_no_lookahead_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_leakage_audit_v1"
VARIANT_COMPARE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_variant_pool_comparison_v1"
FAILURE_MODE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_failure_mode_audit_v1"
ORACLE_HEADROOM_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_oracle_headroom_audit_v1"
LINEAGE_COMPARE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_lineage_comparison_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_decision_v1"

TOP_K_VALUES = (5, 10, 20)
EVAL_LABEL_COLUMNS = ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label")
NO_LOOKAHEAD_FLAGS = ("monthly_context_no_lookahead", "weekly_context_no_lookahead")

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2")
V1_ACCUMULATED_SESSION = Path(r"G:\Tradex\iizuka_fixed_contract_forward_surface_accumulation_v1\20260503T114202Z-219644")
AUDIT_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_contract_redesign_audit_v1\20260503T120124Z-359865")
ORIGINAL_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1\20260503T053753Z-395634")
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

V1_ACCUMULATED_ROWS = V1_ACCUMULATED_SESSION / "iizuka_accumulated_candidate_rows.parquet"
V1_ACCUMULATED_SUMMARY = V1_ACCUMULATED_SESSION / "iizuka_accumulated_surface_generation_summary.json"
V1_ACCUMULATED_VARIANT_COMPARE = V1_ACCUMULATED_SESSION / "iizuka_accumulated_variant_pool_comparison.json"
V1_ACCUMULATED_TOPK_DIFF = V1_ACCUMULATED_SESSION / "iizuka_accumulated_topk_membership_diff.parquet"
V1_ACCUMULATED_FAILURE_MODE = V1_ACCUMULATED_SESSION / "iizuka_accumulated_failure_mode_audit.json"
V1_ACCUMULATED_ORACLE = V1_ACCUMULATED_SESSION / "iizuka_accumulated_oracle_headroom_audit.json"
V1_ACCUMULATED_LINEAGE = V1_ACCUMULATED_SESSION / "iizuka_accumulated_lineage_comparison.json"
V1_ACCUMULATED_DECISION = V1_ACCUMULATED_SESSION / "iizuka_fixed_contract_forward_surface_accumulation_v1_decision.json"

AUDIT_PROPOSAL = AUDIT_SESSION / "iizuka_pre_decisive_v2_contract_proposal.json"
AUDIT_LEVERS = AUDIT_SESSION / "iizuka_structural_redesign_levers.json"
AUDIT_CONTRAST = AUDIT_SESSION / "iizuka_top15_bottom15_contrast_audit.json"
AUDIT_REASON_CONTRAST = AUDIT_SESSION / "iizuka_reason_block_contrast.parquet"
AUDIT_EXPECTED_IMPACT = AUDIT_SESSION / "iizuka_v2_expected_row_impact.parquet"
AUDIT_DECISION = AUDIT_SESSION / "iizuka_pre_decisive_contract_redesign_audit_v1_decision.json"

ORIGINAL_CONTRACT = ORIGINAL_SESSION / "iizuka_pre_decisive_candidate_contract.json"
ORIGINAL_FEATURE_AUDIT = ORIGINAL_SESSION / "iizuka_pre_decisive_feature_availability_audit.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _feature_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _safe_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return [str(value)]


def _load_inputs() -> dict[str, Any]:
    required = {
        "v1_accumulated_rows": V1_ACCUMULATED_ROWS,
        "v1_accumulated_summary": V1_ACCUMULATED_SUMMARY,
        "v1_accumulated_variant_compare": V1_ACCUMULATED_VARIANT_COMPARE,
        "v1_accumulated_topk_diff": V1_ACCUMULATED_TOPK_DIFF,
        "v1_accumulated_failure_mode": V1_ACCUMULATED_FAILURE_MODE,
        "v1_accumulated_oracle": V1_ACCUMULATED_ORACLE,
        "v1_accumulated_lineage": V1_ACCUMULATED_LINEAGE,
        "v1_accumulated_decision": V1_ACCUMULATED_DECISION,
        "audit_proposal": AUDIT_PROPOSAL,
        "audit_levers": AUDIT_LEVERS,
        "audit_contrast": AUDIT_CONTRAST,
        "audit_reason_contrast": AUDIT_REASON_CONTRAST,
        "audit_expected_impact": AUDIT_EXPECTED_IMPACT,
        "audit_decision": AUDIT_DECISION,
        "original_contract": ORIGINAL_CONTRACT,
        "original_feature_audit": ORIGINAL_FEATURE_AUDIT,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    return {
        "v1_accumulated_rows": _load_frame(required["v1_accumulated_rows"]),
        "v1_accumulated_summary": _load_json(required["v1_accumulated_summary"]),
        "v1_accumulated_variant_compare": _load_json(required["v1_accumulated_variant_compare"]),
        "v1_accumulated_topk_diff": _load_frame(required["v1_accumulated_topk_diff"]),
        "v1_accumulated_failure_mode": _load_json(required["v1_accumulated_failure_mode"]),
        "v1_accumulated_oracle": _load_json(required["v1_accumulated_oracle"]),
        "v1_accumulated_lineage": _load_json(required["v1_accumulated_lineage"]),
        "v1_accumulated_decision": _load_json(required["v1_accumulated_decision"]),
        "audit_proposal": _load_json(required["audit_proposal"]),
        "audit_levers": _load_json(required["audit_levers"]),
        "audit_contrast": _load_json(required["audit_contrast"]),
        "audit_reason_contrast": _load_frame(required["audit_reason_contrast"]),
        "audit_expected_impact": _load_frame(required["audit_expected_impact"]),
        "audit_decision": _load_json(required["audit_decision"]),
        "original_contract": _load_json(required["original_contract"]),
        "original_feature_audit": _load_json(required["original_feature_audit"]),
    }


def _build_manifest(output_root: Path, session_root: Path, runtime_db: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_root.name,
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "research_only": True,
        "boundary": "TRADEX-only",
        "source_artifacts": {
            "v1_accumulated_rows": str(V1_ACCUMULATED_ROWS),
            "v1_accumulated_summary": str(V1_ACCUMULATED_SUMMARY),
            "v1_accumulated_variant_compare": str(V1_ACCUMULATED_VARIANT_COMPARE),
            "v1_accumulated_topk_diff": str(V1_ACCUMULATED_TOPK_DIFF),
            "v1_accumulated_failure_mode": str(V1_ACCUMULATED_FAILURE_MODE),
            "v1_accumulated_oracle": str(V1_ACCUMULATED_ORACLE),
            "v1_accumulated_lineage": str(V1_ACCUMULATED_LINEAGE),
            "v1_accumulated_decision": str(V1_ACCUMULATED_DECISION),
            "audit_proposal": str(AUDIT_PROPOSAL),
            "audit_levers": str(AUDIT_LEVERS),
            "audit_contrast": str(AUDIT_CONTRAST),
            "audit_reason_contrast": str(AUDIT_REASON_CONTRAST),
            "audit_expected_impact": str(AUDIT_EXPECTED_IMPACT),
            "audit_decision": str(AUDIT_DECISION),
            "original_contract": str(ORIGINAL_CONTRACT),
            "original_feature_audit": str(ORIGINAL_FEATURE_AUDIT),
            "runtime_db": str(runtime_db),
        },
        "notes": [
            "no MeeMee changes",
            "no production ranking changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
    }


def _build_input_resolution(output_root: Path, session_root: Path, inputs: dict[str, Any], runtime_db: Path) -> dict[str, Any]:
    frame = inputs["v1_accumulated_rows"]
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_root": str(session_root),
        "resolved_paths": {
            "v1_accumulated_session": str(V1_ACCUMULATED_SESSION),
            "audit_session": str(AUDIT_SESSION),
            "original_session": str(ORIGINAL_SESSION),
            "runtime_db": str(runtime_db),
        },
        "row_coverage": {
            "v1_row_count": int(len(frame)),
            "v1_group_count": int(frame["anchor_date"].nunique()) if "anchor_date" in frame.columns else 0,
            "v1_symbol_count": int(frame["symbol"].nunique()) if "symbol" in frame.columns else 0,
            "top20pct_available": "top20pct_label" in frame.columns,
        },
        "artifacts_present": {
            "audit_proposal": True,
            "audit_levers": True,
            "audit_contrast": True,
            "audit_reason_contrast": True,
            "audit_expected_impact": True,
            "audit_decision": True,
            "original_contract": True,
            "original_feature_audit": True,
        },
        "notes": [
            "top20pct_label is missing from the accumulated v1 bundle and is recorded as missing rather than imputed",
            "evaluation labels remain attached after candidate construction only",
        ],
    }


def _split_bucket(value: Any, true_label: str, false_label: str = "") -> str:
    return true_label if _safe_bool(value) else false_label


def _classify_v2_row(row: pd.Series) -> tuple[str, bool, str, str, str]:
    active_reasons: list[str] = []
    diagnostic_reasons: list[str] = []
    exclusion_reasons: list[str] = []

    side = str(row.get("side") or "").lower()
    if side != "long":
        exclusion_reasons.append("non_long_side")

    context_pass = _safe_bool(row.get("iizuka_context_block_pass"))
    compression_pass = _safe_bool(row.get("iizuka_compression_block_pass"))
    risk_pass = _safe_bool(row.get("iizuka_risk_block_pass"))
    if not context_pass:
        diagnostic_reasons.append("context_block_failed")
    if not compression_pass:
        diagnostic_reasons.append("compression_block_failed")
    if not risk_pass:
        diagnostic_reasons.append("risk_block_failed")

    if _safe_bool(row.get("stable_bad_pick_family")):
        exclusion_reasons.append("stable_bad_pick_family")
    if _safe_bool(row.get("bull_marubozu")):
        exclusion_reasons.append("bull_marubozu")
    if str(row.get("ma20_distance_bucket")) == "very_extended":
        exclusion_reasons.append("very_extended_ma20")
    if str(row.get("ma60_distance_bucket")) == "very_extended":
        exclusion_reasons.append("very_extended_ma60")

    missing_stabilization = _feature_missing(row.get("drawdown60")) or _feature_missing(row.get("rebound60"))
    if missing_stabilization:
        diagnostic_reasons.append("missing_stabilization_proxies")

    volume_bucket = str(row.get("volume_participation_bucket") or "")
    support_wick = _safe_bool(row.get("support_wick"))
    bull_engulfing = _safe_bool(row.get("bull_engulfing"))
    active_trigger = False
    if volume_bucket == "volume_neutral":
        active_trigger = True
        active_reasons.append("volume_neutral_default")
    elif volume_bucket == "volume_confirmed" and (support_wick or bull_engulfing):
        active_trigger = True
        active_reasons.append("volume_confirmed_with_reversal")
    elif volume_bucket == "volume_weak" and support_wick:
        active_trigger = True
        active_reasons.append("volume_weak_with_support")
    else:
        diagnostic_reasons.append("trigger_gate_failed")

    if exclusion_reasons:
        role = "excluded"
        active_pass = False
        reason = "excluded|" + "|".join(dict.fromkeys(exclusion_reasons))
        diagnostic_reason = ""
        exclusion_reason = "|".join(dict.fromkeys(exclusion_reasons))
    elif context_pass and compression_pass and risk_pass and active_trigger and not missing_stabilization:
        role = "active"
        active_pass = True
        reason = "active|" + "|".join(dict.fromkeys(["context_pass", "compression_pass", "risk_pass"] + active_reasons))
        diagnostic_reason = ""
        exclusion_reason = ""
    else:
        role = "diagnostic_only"
        active_pass = False
        reason = "diagnostic_only|" + "|".join(dict.fromkeys(diagnostic_reasons or ["near_miss"]))
        diagnostic_reason = "|".join(dict.fromkeys(diagnostic_reasons or ["near_miss"]))
        exclusion_reason = ""

    return role, active_pass, reason, diagnostic_reason, exclusion_reason


def _enrich_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = _bucketize_frame(frame)
    if "research_fallback_label_source" not in out.columns:
        out["research_fallback_label_source"] = "ml_label_20d"
    out["candidate_contract_name"] = "iizuka_pre_decisive_long_candidate_v2"
    out["research_only"] = True
    out["iizuka_v2_candidate_score"] = pd.to_numeric(out.get("iizuka_candidate_score"), errors="coerce")
    classification = out.apply(_classify_v2_row, axis=1, result_type="expand")
    out["iizuka_v2_role"] = classification[0]
    out["iizuka_v2_active_pass"] = classification[1]
    out["iizuka_v2_reason"] = classification[2]
    out["iizuka_v2_diagnostic_reason"] = classification[3]
    out["iizuka_v2_exclusion_reason"] = classification[4]
    out["iizuka_v2_candidate_rank"] = pd.NA
    active = out.loc[out["iizuka_v2_active_pass"].fillna(False).astype(bool)].copy()
    if len(active):
        active = active.sort_values(
            ["anchor_date", "iizuka_v2_candidate_score", "champion_rank", "symbol"],
            ascending=[True, False, True, True],
            kind="stable",
        ).reset_index()
        active["iizuka_v2_candidate_rank"] = active.groupby("anchor_date")["iizuka_v2_candidate_score"].rank(method="first", ascending=False)
        out.loc[active["index"], "iizuka_v2_candidate_rank"] = active["iizuka_v2_candidate_rank"].values
    return out


def _role_counts(frame: pd.DataFrame) -> dict[str, int]:
    return {
        "active_rows": int((frame["iizuka_v2_role"] == "active").sum()),
        "diagnostic_only_rows": int((frame["iizuka_v2_role"] == "diagnostic_only").sum()),
        "excluded_rows": int((frame["iizuka_v2_role"] == "excluded").sum()),
        "active_top15_count": int(frame.loc[frame["iizuka_v2_role"] == "active", "top15_label"].fillna(False).astype(bool).sum()),
        "active_bottom15_count": int(frame.loc[frame["iizuka_v2_role"] == "active", "bottom15_label"].fillna(False).astype(bool).sum()),
        "diagnostic_top15_count": int(frame.loc[frame["iizuka_v2_role"] == "diagnostic_only", "top15_label"].fillna(False).astype(bool).sum()),
        "diagnostic_bottom15_count": int(frame.loc[frame["iizuka_v2_role"] == "diagnostic_only", "bottom15_label"].fillna(False).astype(bool).sum()),
        "excluded_top15_count": int(frame.loc[frame["iizuka_v2_role"] == "excluded", "top15_label"].fillna(False).astype(bool).sum()),
        "excluded_bottom15_count": int(frame.loc[frame["iizuka_v2_role"] == "excluded", "bottom15_label"].fillna(False).astype(bool).sum()),
    }


def _build_v2_contract_summary(frame: pd.DataFrame, proposal: dict[str, Any], levers: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": IMPLEMENTATION_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2",
        "proposal_contract_name": proposal.get("proposal_contract_name"),
        "implementation_mode": "single structural gate with preserved v1 score",
        "preserved_blocks": proposal.get("preserved_blocks", []),
        "tightened_blocks": proposal.get("tightened_blocks", []),
        "diagnostic_only_definition": proposal.get("diagnostic_only_definition", []),
        "excluded_definition": proposal.get("excluded_definition", []),
        "required_fields_present": {
            field: field in frame.columns for field in _safe_list(proposal.get("required_fields", {}).get("must_exist"))
        },
        "required_fields_missing": [field for field in _safe_list(proposal.get("required_fields", {}).get("must_exist")) if field not in frame.columns],
        "top20pct_available": "top20pct_label" in frame.columns,
        "top20pct_note": "top20pct_label is missing from the accumulated bundle and is not imputed",
        "fallback_label_source": str(frame["research_fallback_label_source"].iloc[0]) if "research_fallback_label_source" in frame.columns and len(frame) else "ml_label_20d",
        "role_counts": _role_counts(frame),
        "no_lookahead_basis": {
            "monthly_context_no_lookahead": bool(frame["monthly_context_no_lookahead"].fillna(False).astype(bool).all()) if "monthly_context_no_lookahead" in frame.columns else False,
            "weekly_context_no_lookahead": bool(frame["weekly_context_no_lookahead"].fillna(False).astype(bool).all()) if "weekly_context_no_lookahead" in frame.columns else False,
        },
        "levers_reference": levers,
        "input_summary": {
            "v1_row_count": int(len(inputs["v1_accumulated_rows"])),
            "v1_group_count": int(inputs["v1_accumulated_rows"]["anchor_date"].nunique()) if "anchor_date" in inputs["v1_accumulated_rows"].columns else 0,
            "v1_symbol_count": int(inputs["v1_accumulated_rows"]["symbol"].nunique()) if "symbol" in inputs["v1_accumulated_rows"].columns else 0,
        },
        "notes": [
            "v2 keeps the v1 score contract and only changes the structural gate",
            "evaluation labels remain evaluation-only",
        ],
    }


def _build_surface_summary(frame: pd.DataFrame) -> dict[str, Any]:
    active = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    month_series = pd.to_datetime(active["anchor_date"], errors="coerce").dt.strftime("%Y-%m") if len(active) else pd.Series(dtype=str)
    symbol_counts = Counter(active["symbol"].astype(str).tolist()) if len(active) else Counter()
    return {
        "schema_version": SURFACE_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2",
        "row_count": int(len(active)),
        "group_count": int(active["anchor_date"].nunique()) if len(active) else 0,
        "symbol_count": int(active["symbol"].nunique()) if len(active) else 0,
        "month_count": int(month_series.nunique()) if len(active) else 0,
        "zero_pass_groups": int(max(0, frame["anchor_date"].nunique() - active["anchor_date"].nunique())) if "anchor_date" in frame.columns else 0,
        "role_counts": _role_counts(frame),
        "top_symbol_counts": {str(symbol): int(count) for symbol, count in symbol_counts.most_common(10)} if symbol_counts else {},
        "candidate_reason_counts": {str(key): int(value) for key, value in active["iizuka_v2_reason"].value_counts().head(10).items()} if len(active) else {},
        "role_reason_counts": {str(key): int(value) for key, value in frame["iizuka_v2_reason"].value_counts().head(10).items()} if len(frame) else {},
        "score_summary": {
            "min": float(pd.to_numeric(active["iizuka_v2_candidate_score"], errors="coerce").min()) if len(active) else None,
            "median": float(pd.to_numeric(active["iizuka_v2_candidate_score"], errors="coerce").median()) if len(active) else None,
            "max": float(pd.to_numeric(active["iizuka_v2_candidate_score"], errors="coerce").max()) if len(active) else None,
        },
        "rank_summary": {
            "min": float(pd.to_numeric(active["iizuka_v2_candidate_rank"], errors="coerce").min()) if len(active) else None,
            "median": float(pd.to_numeric(active["iizuka_v2_candidate_rank"], errors="coerce").median()) if len(active) else None,
            "max": float(pd.to_numeric(active["iizuka_v2_candidate_rank"], errors="coerce").max()) if len(active) else None,
        },
        "top20pct_available": False,
        "top20pct_note": "top20pct_label is missing from the accumulated bundle and is recorded as missing",
        "fallback_label_source": str(frame["research_fallback_label_source"].iloc[0]) if "research_fallback_label_source" in frame.columns and len(frame) else "ml_label_20d",
    }


def _no_lookahead_audit(frame: pd.DataFrame) -> dict[str, Any]:
    violations: dict[str, int] = {}
    for flag in NO_LOOKAHEAD_FLAGS:
        if flag in frame.columns:
            violations[f"{flag}_false_count"] = int((~frame[flag].fillna(False).astype(bool)).sum())
    date_violations: dict[str, int] = {}
    for field in ("monthly_context_date", "weekly_context_date"):
        if field in frame.columns:
            asof = pd.to_datetime(frame["anchor_date"], errors="coerce")
            context = pd.to_datetime(frame[field], errors="coerce")
            date_violations[f"{field}_future_count"] = int((context > asof).sum())
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_pass": all(value == 0 for value in violations.values()) and all(value == 0 for value in date_violations.values()),
        "flag_violations": violations,
        "date_violations": date_violations,
        "notes": [
            "candidate rows use only row-local historical features",
            "evaluation labels are joined after v2 role assignment and ranking",
        ],
    }


def _leakage_audit(frame: pd.DataFrame) -> dict[str, Any]:
    feature_fields_used = {
        "signal_quality_bucket",
        "decision_candle_quality",
        "volume_participation_bucket",
        "shape_classification",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "stable_bad_pick_family",
        "iizuka_context_block_pass",
        "iizuka_compression_block_pass",
        "iizuka_trigger_proximity_block_pass",
        "iizuka_risk_block_pass",
        "iizuka_candidate_score",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "ma20_slope_1",
        "vol_ratio5_20",
        "drawdown60",
        "rebound60",
        "support_wick",
        "bull_engulfing",
        "bull_marubozu",
    }
    outcome_fields = set(EVAL_LABEL_COLUMNS)
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_fields_used": sorted(feature_fields_used),
        "outcome_fields": sorted(outcome_fields),
        "outcome_fields_used_as_features": sorted(feature_fields_used.intersection(outcome_fields)),
        "outcome_fields_attached_after_candidate_construction": sorted([column for column in EVAL_LABEL_COLUMNS if column in frame.columns]),
        "leakage_free": not feature_fields_used.intersection(outcome_fields),
        "note": "evaluation labels were joined after v2 candidate surface construction",
    }


def _select_topk(frame: pd.DataFrame, score_col: str, k: int) -> pd.DataFrame:
    selected = frame.copy()
    selected = selected.sort_values(["anchor_date", score_col, "champion_rank", "symbol"], ascending=[True, False, True, True], kind="stable").reset_index(drop=True)
    selected["selected_rank"] = selected.groupby("anchor_date")[score_col].rank(method="first", ascending=False)
    return selected.loc[selected["selected_rank"] <= k].copy()


def _select_champion_topk(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    if f"champion_selected_top{k}" in frame.columns:
        return frame.loc[frame[f"champion_selected_top{k}"].fillna(False).astype(bool)].copy()
    selected = frame.copy()
    selected = selected.sort_values(["anchor_date", "champion_score", "champion_rank", "symbol"], ascending=[True, False, True, True], kind="stable").reset_index(drop=True)
    selected["selected_rank"] = selected.groupby("anchor_date")["champion_score"].rank(method="first", ascending=False)
    return selected.loc[selected["selected_rank"] <= k].copy()


def _metric_for_selection(frame: pd.DataFrame) -> dict[str, Any]:
    metrics = _metric_bundle(frame)
    month_count = int(pd.to_datetime(frame["anchor_date"], errors="coerce").dt.strftime("%Y-%m").nunique()) if len(frame) else 0
    row_count = int(metrics["row_count"])
    top15_count = int(metrics["top15_count"])
    bottom15_count = int(metrics["bottom15_count"])
    metrics.update(
        {
            "group_count": int(frame["anchor_date"].nunique()) if "anchor_date" in frame.columns and len(frame) else 0,
            "symbol_count": int(frame["symbol"].nunique()) if "symbol" in frame.columns and len(frame) else 0,
            "month_count": month_count,
            "top15_capture_rate": float(top15_count / row_count) if row_count else None,
            "bottom15_contamination_rate": float(bottom15_count / row_count) if row_count else None,
        }
    )
    if "top20pct_label" not in frame.columns:
        metrics["top20pct_available"] = False
        metrics["top20pct_rate"] = None
    return metrics


def _selection_state(champion_key: str, v1_key: str, v2_key: str) -> str:
    states = []
    if champion_key:
        states.append("champion")
    if v1_key:
        states.append("v1")
    if v2_key:
        states.append("v2")
    return "+".join(states) if states else "none"


def _compare_topk(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any], dict[str, Any]]:
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
    champion = frame.copy()
    v1 = frame.copy()
    v2 = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    for k in TOP_K_VALUES:
        champ_sel = _select_champion_topk(champion, k)
        v1_sel = _select_topk(v1, "iizuka_candidate_score", k)
        v2_sel = _select_topk(v2, "iizuka_v2_candidate_score", k)

        champ_keys = set(champ_sel["surface_key"].astype(str)) if len(champ_sel) else set()
        v1_keys = set(v1_sel["surface_key"].astype(str)) if len(v1_sel) else set()
        v2_keys = set(v2_sel["surface_key"].astype(str)) if len(v2_sel) else set()
        union = champ_keys | v1_keys | v2_keys
        diff = pd.DataFrame({"top_k": k, "surface_key": list(union)})
        diff = diff.merge(
            champion[[
                "surface_key",
                "anchor_date",
                "symbol",
                "side",
                "champion_score",
                "champion_rank",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
                *([c for c in ("top20pct_label",) if c in champion.columns]),
            ] + [c for c in ("candidate_idx", "month_bucket") if c in champion.columns]],
            on="surface_key",
            how="left",
            suffixes=("", "_champion"),
        ).merge(
            v1[[
                "surface_key",
                "iizuka_candidate_score",
                "iizuka_candidate_rank",
                "iizuka_candidate_reason",
                "iizuka_context_block_pass",
                "iizuka_compression_block_pass",
                "iizuka_trigger_proximity_block_pass",
                "iizuka_risk_block_pass",
            ]],
            on="surface_key",
            how="left",
        ).merge(
            v2[[
                "surface_key",
                "iizuka_v2_candidate_score",
                "iizuka_v2_candidate_rank",
                "iizuka_v2_role",
                "iizuka_v2_reason",
                "iizuka_v2_active_pass",
                "iizuka_v2_diagnostic_reason",
                "iizuka_v2_exclusion_reason",
            ]],
            on="surface_key",
            how="left",
        )
        diff["selected_in_champion"] = diff["surface_key"].isin(champ_keys)
        diff["selected_in_v1"] = diff["surface_key"].isin(v1_keys)
        diff["selected_in_v2"] = diff["surface_key"].isin(v2_keys)
        diff["selection_state"] = diff.apply(
            lambda row: _selection_state(
                "champion" if row["selected_in_champion"] else "",
                "v1" if row["selected_in_v1"] else "",
                "v2" if row["selected_in_v2"] else "",
            ),
            axis=1,
        )
        diff["member_change_v1_v2"] = diff["selected_in_v1"] != diff["selected_in_v2"]
        diff["member_change_champion_v2"] = diff["selected_in_champion"] != diff["selected_in_v2"]
        diff["member_change_champion_v1"] = diff["selected_in_champion"] != diff["selected_in_v1"]
        diff["top_k"] = k
        diff_rows.append(diff)

        champion_metrics = _metric_for_selection(champ_sel)
        v1_metrics = _metric_for_selection(v1_sel)
        v2_metrics = _metric_for_selection(v2_sel)
        rows.append(
            {
                "top_k": k,
                "champion": champion_metrics,
                "v1": v1_metrics,
                "v2": v2_metrics,
                "membership_changed_count_v1_v2": int(len(v1_keys ^ v2_keys)),
                "membership_changed_count_champion_v2": int(len(champ_keys ^ v2_keys)),
                "membership_changed_count_champion_v1": int(len(champ_keys ^ v1_keys)),
                "overlap_ratio_v1_v2": float(len(v1_keys & v2_keys) / len(v1_keys | v2_keys)) if (v1_keys | v2_keys) else None,
                "overlap_ratio_champion_v2": float(len(champ_keys & v2_keys) / len(champ_keys | v2_keys)) if (champ_keys | v2_keys) else None,
                "overlap_ratio_champion_v1": float(len(champ_keys & v1_keys) / len(champ_keys | v1_keys)) if (champ_keys | v1_keys) else None,
                "champion_group_count": champion_metrics["group_count"],
                "v1_group_count": v1_metrics["group_count"],
                "v2_group_count": v2_metrics["group_count"],
                "champion_symbol_count": champion_metrics["symbol_count"],
                "v1_symbol_count": v1_metrics["symbol_count"],
                "v2_symbol_count": v2_metrics["symbol_count"],
                "zero_pass_groups_v1": int(max(0, int(frame["anchor_date"].nunique()) - v1_metrics["group_count"])) if "anchor_date" in frame.columns else 0,
                "zero_pass_groups_v2": int(max(0, int(frame["anchor_date"].nunique()) - v2_metrics["group_count"])) if "anchor_date" in frame.columns else 0,
            }
        )

        failure_mode["per_k"][str(k)] = {
            "champion_only_count_vs_v2": int(len(champ_keys - v2_keys)),
            "v1_only_count_vs_v2": int(len(v1_keys - v2_keys)),
            "v2_only_count_vs_v1": int(len(v2_keys - v1_keys)),
            "v2_top15_loss_count_vs_v1": int(pd.to_numeric(v1_sel.loc[~v1_sel["surface_key"].isin(v2_keys), "top15_label"], errors="coerce").fillna(0).sum()) if len(v1_sel) else 0,
            "v2_bottom15_loss_count_vs_v1": int(pd.to_numeric(v1_sel.loc[~v1_sel["surface_key"].isin(v2_keys), "bottom15_label"], errors="coerce").fillna(0).sum()) if len(v1_sel) else 0,
            "reason_block_contribution_v2": {str(key): int(value) for key, value in v2_sel["iizuka_v2_reason"].value_counts().head(10).items()},
            "false_positive_cost_v2": {
                "bottom15_count": int(pd.to_numeric(v2_sel["bottom15_label"], errors="coerce").fillna(0).sum()) if len(v2_sel) else 0,
                "bottom15_rate": float(pd.to_numeric(v2_sel["bottom15_label"], errors="coerce").mean()) if len(v2_sel) else None,
            },
        }

        missed = v1_sel.loc[~v1_sel["surface_key"].isin(v2_keys)].copy().sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5)
        gained = v2_sel.loc[~v2_sel["surface_key"].isin(v1_keys)].copy().sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5)
        headroom["per_k"][str(k)] = {
            "missed_v1_top15_examples": _records(missed, fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "iizuka_candidate_rank"]),
            "gained_v2_top15_examples": _records(gained, fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "iizuka_v2_candidate_score", "iizuka_v2_candidate_rank", "iizuka_v2_role"]),
            "missed_v1_top15_count": int(pd.to_numeric(missed["top15_label"], errors="coerce").fillna(0).sum()) if len(missed) else 0,
            "gained_v2_top15_count": int(pd.to_numeric(gained["top15_label"], errors="coerce").fillna(0).sum()) if len(gained) else 0,
            "missed_v2_top15_examples_vs_champion": _records(champion.loc[~champion["surface_key"].isin(v2_keys)].sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5), fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "champion_rank"]),
            "gained_v2_top15_examples_vs_champion": _records(v2_sel.loc[~v2_sel["surface_key"].isin(champ_keys)].sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5), fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "iizuka_v2_candidate_score", "iizuka_v2_candidate_rank", "iizuka_v2_role"]),
        }

    comparison = {
        "schema_version": VARIANT_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2",
        "metric_mode": "per_anchor_date_topK",
        "top20pct_available": False,
        "top20pct_note": "top20pct_label is missing from the accumulated bundle and is not imputed",
        "per_k": rows,
    }
    diff_frame = pd.concat(diff_rows, ignore_index=True) if diff_rows else pd.DataFrame()
    return comparison, diff_frame, failure_mode, headroom


def _build_lineage_comparison(frame: pd.DataFrame, comparison: dict[str, Any]) -> dict[str, Any]:
    top5 = next(item for item in comparison["per_k"] if item["top_k"] == 5)
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    v2 = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    v1_top15 = frame.loc[frame["top15_label"].fillna(False).astype(bool)].copy()
    v2_top15 = v2.loc[v2["top15_label"].fillna(False).astype(bool)].copy()
    v1_bottom15 = frame.loc[frame["bottom15_label"].fillna(False).astype(bool)].copy()
    v2_bottom15 = v2.loc[v2["bottom15_label"].fillna(False).astype(bool)].copy()
    return {
        "schema_version": LINEAGE_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "v1_lineage": {
            "row_count": int(len(frame)),
            "group_count": int(frame["anchor_date"].nunique()) if len(frame) else 0,
            "symbol_count": int(frame["symbol"].nunique()) if len(frame) else 0,
            "top15_count": int(v1_top15["top15_label"].fillna(False).astype(bool).sum()) if len(v1_top15) else 0,
            "bottom15_count": int(v1_bottom15["bottom15_label"].fillna(False).astype(bool).sum()) if len(v1_bottom15) else 0,
        },
        "v2_lineage": {
            "active_row_count": int(len(v2)),
            "active_group_count": int(v2["anchor_date"].nunique()) if len(v2) else 0,
            "active_symbol_count": int(v2["symbol"].nunique()) if len(v2) else 0,
            "diagnostic_only_row_count": int((frame["iizuka_v2_role"] == "diagnostic_only").sum()),
            "excluded_row_count": int((frame["iizuka_v2_role"] == "excluded").sum()),
            "active_top15_count": int(v2_top15["top15_label"].fillna(False).astype(bool).sum()) if len(v2_top15) else 0,
            "active_bottom15_count": int(v2_bottom15["bottom15_label"].fillna(False).astype(bool).sum()) if len(v2_bottom15) else 0,
        },
        "retention_loss": {
            "top15_retained_count": int(len(set(v1_top15["surface_key"]) & set(v2["surface_key"]))),
            "top15_lost_count": int(len(set(v1_top15["surface_key"]) - set(v2["surface_key"]))),
            "bottom15_removed_count": int(len(set(v1_bottom15["surface_key"]) - set(v2["surface_key"]))),
            "bottom15_retained_count": int(len(set(v1_bottom15["surface_key"]) & set(v2["surface_key"]))),
        },
        "comparison": {
            "top5_delta_forward_ret_20d_v2_vs_v1": _safe_float(top5["v2"]["mean_forward_ret_20d"] - top5["v1"]["mean_forward_ret_20d"]) if top5["v2"]["mean_forward_ret_20d"] is not None and top5["v1"]["mean_forward_ret_20d"] is not None else None,
            "top10_delta_forward_ret_20d_v2_vs_v1": _safe_float(top10["v2"]["mean_forward_ret_20d"] - top10["v1"]["mean_forward_ret_20d"]) if top10["v2"]["mean_forward_ret_20d"] is not None and top10["v1"]["mean_forward_ret_20d"] is not None else None,
            "top20_delta_forward_ret_20d_v2_vs_v1": _safe_float(top20["v2"]["mean_forward_ret_20d"] - top20["v1"]["mean_forward_ret_20d"]) if top20["v2"]["mean_forward_ret_20d"] is not None and top20["v1"]["mean_forward_ret_20d"] is not None else None,
            "top5_delta_bottom15_rate_v2_vs_v1": _safe_float(top5["v2"]["bottom15_count"] / max(top5["v2"]["row_count"], 1) - top5["v1"]["bottom15_count"] / max(top5["v1"]["row_count"], 1)) if top5["v2"]["row_count"] and top5["v1"]["row_count"] else None,
            "top10_delta_bottom15_rate_v2_vs_v1": _safe_float(top10["v2"]["bottom15_count"] / max(top10["v2"]["row_count"], 1) - top10["v1"]["bottom15_count"] / max(top10["v1"]["row_count"], 1)) if top10["v2"]["row_count"] and top10["v1"]["row_count"] else None,
            "top20_delta_bottom15_rate_v2_vs_v1": _safe_float(top20["v2"]["bottom15_count"] / max(top20["v2"]["row_count"], 1) - top20["v1"]["bottom15_count"] / max(top20["v1"]["row_count"], 1)) if top20["v2"]["row_count"] and top20["v1"]["row_count"] else None,
        },
        "useful_signal_preserved": bool(top5["v2"]["top15_count"] >= top5["v1"]["top15_count"] * 0.8 and top10["v2"]["top15_count"] >= top10["v1"]["top15_count"] * 0.8),
        "notes": [
            "v1 is the accumulated fixed-contract surface and v2 is the active role subset",
            "top20pct_label is unavailable in the accumulated bundle and is not used for lineage decisions",
        ],
    }


def _build_decision(comparison: dict[str, Any], lineage: dict[str, Any], no_lookahead: dict[str, Any], leakage: dict[str, Any], frame: pd.DataFrame) -> dict[str, Any]:
    top5 = next(item for item in comparison["per_k"] if item["top_k"] == 5)
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    v2 = top20["v2"]
    v1 = top20["v1"]
    champion = top20["champion"]
    decision = "needs_iizuka_v2_contract_refinement"
    reason = "v2 materially reduces bottom15 contamination versus v1 but still underperforms champion on practical top-K quality"
    if not no_lookahead["no_lookahead_pass"] or not leakage["leakage_free"]:
        decision = "drop_iizuka_v2_contract"
        reason = "no-lookahead or leakage audit failed"
    elif lineage["v2_lineage"]["active_row_count"] == 0:
        decision = "drop_iizuka_axis"
        reason = "v2 active surface is empty"
    elif (
        top5["v2"]["mean_forward_ret_20d"] is not None
        and top10["v2"]["mean_forward_ret_20d"] is not None
        and top5["v2"]["mean_forward_ret_20d"] >= top5["v1"]["mean_forward_ret_20d"]
        and top10["v2"]["mean_forward_ret_20d"] >= top10["v1"]["mean_forward_ret_20d"]
        and v2["bottom15_contamination_rate"] is not None
        and v1["bottom15_contamination_rate"] is not None
        and v2["bottom15_contamination_rate"] <= v1["bottom15_contamination_rate"] * 0.8
        and v2["top15_count"] >= v1["top15_count"] * 0.75
    ):
        decision = "ready_for_iizuka_v2_candidate_challenger_design"
        reason = "v2 improves forward return versus v1, materially reduces bottom15 contamination, and preserves enough top15 headroom for challenger design"
    elif (
        v2["bottom15_contamination_rate"] is not None
        and v1["bottom15_contamination_rate"] is not None
        and v2["bottom15_contamination_rate"] > v1["bottom15_contamination_rate"]
    ):
        decision = "drop_iizuka_v2_contract"
        reason = "v2 did not reduce bottom15 contamination versus v1"
    elif (
        top5["v2"]["mean_forward_ret_20d"] is not None
        and top10["v2"]["mean_forward_ret_20d"] is not None
        and (top5["v2"]["mean_forward_ret_20d"] < top5["champion"]["mean_forward_ret_20d"] or top10["v2"]["mean_forward_ret_20d"] < top10["champion"]["mean_forward_ret_20d"])
    ):
        decision = "needs_iizuka_v2_contract_refinement"
        reason = "v2 improves structure but still underperforms champion at top5 or top10"
    elif lineage["v2_lineage"]["active_row_count"] < 500:
        decision = "hold_needs_more_forward_surfaces"
        reason = "v2 looks structurally better but the active sample is still limited"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": "keep" if decision == "ready_for_iizuka_v2_candidate_challenger_design" else ("hold" if decision.startswith("hold_") or decision.startswith("needs_") else "blocked"),
        "reason": reason,
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2",
        "summary": {
            "v1_row_count": int(lineage["v1_lineage"]["row_count"]),
            "v2_active_row_count": int(lineage["v2_lineage"]["active_row_count"]),
            "v2_diagnostic_only_row_count": int(lineage["v2_lineage"]["diagnostic_only_row_count"]),
            "v2_excluded_row_count": int(lineage["v2_lineage"]["excluded_row_count"]),
            "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
            "leakage_free": bool(leakage["leakage_free"]),
            "top20pct_available": False,
        },
        "comparison_snapshot": {
            "top5": top5,
            "top10": top10,
            "top20": top20,
        },
        "lineage_snapshot": lineage,
        "notes": [
            "v2 preserves the v1 score contract and changes only the structural gate",
            "top20pct_label is unavailable in the accumulated bundle and is not imputed",
        ],
    }


def _records(frame: pd.DataFrame, fields: list[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    use = [field for field in fields if field in frame.columns]
    return [_json_ready(dict(row)) for row in frame[use].to_dict(orient="records")]


def _select_output_columns(frame: pd.DataFrame) -> list[str]:
    preferred = [
        "dt",
        "code",
        "side",
        "entry_qualified",
        "setup_type",
        "reason_snapshot_json",
        "score_snapshot_json",
        "rank_snapshot_json",
        "decision_hash",
        "anchor_date",
        "symbol",
        "month_bucket",
        "source_signal_side",
        "research_fallback_label_source",
        "source_max_anchor_date",
        "mature_label_max_date",
        "champion_score",
        "champion_rank",
        "candidate_idx",
        "candidate_rank",
        "candidate_score",
        "surface_key",
        "canonical_candidate_key",
        "key",
        "iizuka_candidate_score",
        "iizuka_candidate_rank",
        "iizuka_candidate_reason",
        "iizuka_missing_feature_reason",
        "candidate_contract_name",
        "research_only",
        "iizuka_context_block_pass",
        "iizuka_compression_block_pass",
        "iizuka_trigger_proximity_block_pass",
        "iizuka_risk_block_pass",
        "ret20",
        "up20_label",
        "forward_ret_20d",
        "path_value_score_v1",
        "top15_label",
        "bottom15_label",
        "iizuka_v2_role",
        "iizuka_v2_reason",
        "iizuka_v2_active_pass",
        "iizuka_v2_diagnostic_reason",
        "iizuka_v2_exclusion_reason",
        "iizuka_v2_candidate_score",
        "iizuka_v2_candidate_rank",
    ]
    preferred.extend([column for column in ("top20pct_label",) if column in frame.columns])
    preferred.extend([column for column in frame.columns if column not in preferred])
    return preferred


def _build_role_frames(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    active = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    diagnostic = frame.loc[frame["iizuka_v2_role"] == "diagnostic_only"].copy()
    excluded = frame.loc[frame["iizuka_v2_role"] == "excluded"].copy()
    all_roles = frame.copy()
    sort_cols = ["anchor_date", "iizuka_v2_candidate_score", "champion_rank", "symbol"]
    for subset in (active, diagnostic, excluded, all_roles):
        if len(subset) and all(col in subset.columns for col in sort_cols):
            subset.sort_values(sort_cols, ascending=[True, False, True, True], kind="stable", inplace=True)
    return active.reset_index(drop=True), diagnostic.reset_index(drop=True), excluded.reset_index(drop=True), all_roles.reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive long candidate v2 implementation")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = Path(args.output_root).expanduser().resolve()
    runtime_db = Path(args.runtime_db).expanduser().resolve()
    session_id = _session_id()
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=True)

    inputs = _load_inputs()
    proposal = inputs["audit_proposal"]
    levers = inputs["audit_levers"]
    frame = _enrich_frame(inputs["v1_accumulated_rows"])
    frame = frame.sort_values(["anchor_date", "champion_rank", "symbol"], ascending=[True, True, True], kind="stable").reset_index(drop=True)

    active, diagnostic, excluded, all_roles = _build_role_frames(frame)

    implementation_summary = _build_v2_contract_summary(frame, proposal, levers, inputs)
    surface_summary = _build_surface_summary(frame)
    no_lookahead = _no_lookahead_audit(frame)
    leakage = _leakage_audit(frame)
    comparison, diff_frame, failure_mode, headroom = _compare_topk(frame)
    lineage = _build_lineage_comparison(frame, comparison)
    decision = _build_decision(comparison, lineage, no_lookahead, leakage, frame)

    _write_json(session_root / "run_manifest.json", _build_manifest(output_root, session_root, runtime_db))
    _write_json(session_root / "input_resolution.json", _build_input_resolution(output_root, session_root, inputs, runtime_db))
    _write_json(session_root / "iizuka_v2_contract_implementation_summary.json", implementation_summary)
    _write_parquet(session_root / "iizuka_v2_active_candidate_rows.parquet", active[_select_output_columns(active)])
    _write_parquet(session_root / "iizuka_v2_diagnostic_candidate_rows.parquet", diagnostic[_select_output_columns(diagnostic)])
    _write_parquet(session_root / "iizuka_v2_excluded_candidate_rows.parquet", excluded[_select_output_columns(excluded)])
    _write_parquet(session_root / "iizuka_v2_all_role_rows.parquet", all_roles[_select_output_columns(all_roles)])
    _write_json(session_root / "iizuka_v2_surface_generation_summary.json", surface_summary)
    _write_json(session_root / "iizuka_v2_no_lookahead_audit.json", no_lookahead)
    _write_json(session_root / "iizuka_v2_leakage_audit.json", leakage)
    _write_json(session_root / "iizuka_v2_variant_pool_comparison.json", comparison)
    _write_parquet(session_root / "iizuka_v2_topk_membership_diff.parquet", diff_frame)
    _write_json(session_root / "iizuka_v2_failure_mode_audit.json", failure_mode)
    _write_json(session_root / "iizuka_v2_oracle_headroom_audit.json", headroom)
    _write_json(session_root / "iizuka_v1_v2_lineage_comparison.json", lineage)
    _write_json(session_root / "iizuka_pre_decisive_long_candidate_v2_decision.json", decision)
    _write_parquet(session_root / "iizuka_v2_reason_block_summary.parquet", frame[["iizuka_v2_role", "iizuka_v2_reason", "iizuka_v2_diagnostic_reason", "iizuka_v2_exclusion_reason", "top15_label", "bottom15_label", "forward_ret_20d", "path_value_score_v1"]].copy())
    _write_parquet(
        session_root / "iizuka_v2_candidate_examples.parquet",
        frame.loc[frame["iizuka_v2_role"] != "excluded"].head(50)[["anchor_date", "symbol", "iizuka_v2_role", "iizuka_v2_reason", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "iizuka_v2_candidate_score", "iizuka_v2_candidate_rank"]].copy(),
    )
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_root": str(session_root),
            "output_root": str(output_root),
            "artifact_count": 17,
            "artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "iizuka_v2_contract_implementation_summary.json",
                "iizuka_v2_active_candidate_rows.parquet",
                "iizuka_v2_diagnostic_candidate_rows.parquet",
                "iizuka_v2_excluded_candidate_rows.parquet",
                "iizuka_v2_all_role_rows.parquet",
                "iizuka_v2_surface_generation_summary.json",
                "iizuka_v2_no_lookahead_audit.json",
                "iizuka_v2_leakage_audit.json",
                "iizuka_v2_variant_pool_comparison.json",
                "iizuka_v2_topk_membership_diff.parquet",
                "iizuka_v2_failure_mode_audit.json",
                "iizuka_v2_oracle_headroom_audit.json",
                "iizuka_v1_v2_lineage_comparison.json",
                "iizuka_pre_decisive_long_candidate_v2_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "decision": decision["decision"],
        },
    )


if __name__ == "__main__":
    main()
