from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

SCRIPT_NAME = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_input_resolution_v1"
BOTTOM15_ATTRIBUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_bottom15_contamination_attribution_v1"
TOP15_ATTRIBUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_top15_retention_attribution_v1"
CONTRAST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_top15_bottom15_contrast_v1"
LEVER_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_structural_redesign_levers_v1"
V2_PROPOSAL_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_v2_contract_proposal_v1"
VALIDATION_PLAN_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_validation_plan_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_contract_redesign_audit_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_contract_redesign_audit_v1")
ACCUMULATED_SESSION = Path(r"G:\Tradex\iizuka_fixed_contract_forward_surface_accumulation_v1\20260503T114202Z-219644")
ORIGINAL_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1\20260503T053753Z-395634")
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

ACCUMULATED_ROWS = ACCUMULATED_SESSION / "iizuka_accumulated_candidate_rows.parquet"
ACCUMULATED_SUMMARY = ACCUMULATED_SESSION / "iizuka_accumulated_surface_generation_summary.json"
ACCUMULATED_VARIANT_COMPARE = ACCUMULATED_SESSION / "iizuka_accumulated_variant_pool_comparison.json"
ACCUMULATED_TOPK_DIFF = ACCUMULATED_SESSION / "iizuka_accumulated_topk_membership_diff.parquet"
ACCUMULATED_FAILURE_MODE = ACCUMULATED_SESSION / "iizuka_accumulated_failure_mode_audit.json"
ACCUMULATED_ORACLE = ACCUMULATED_SESSION / "iizuka_accumulated_oracle_headroom_audit.json"
ACCUMULATED_LINEAGE = ACCUMULATED_SESSION / "iizuka_accumulated_lineage_comparison.json"
ACCUMULATED_DECISION = ACCUMULATED_SESSION / "iizuka_fixed_contract_forward_surface_accumulation_v1_decision.json"

ORIGINAL_CONTRACT = ORIGINAL_SESSION / "iizuka_pre_decisive_candidate_contract.json"
ORIGINAL_FEATURE_AUDIT = ORIGINAL_SESSION / "iizuka_pre_decisive_feature_availability_audit.json"
ORIGINAL_FAILURE_MODE = ORIGINAL_SESSION / "iizuka_pre_decisive_failure_mode_audit.json"
ORIGINAL_ORACLE = ORIGINAL_SESSION / "iizuka_pre_decisive_oracle_headroom_audit.json"
ORIGINAL_DECISION = ORIGINAL_SESSION / "iizuka_pre_decisive_long_candidate_generation_v1_decision.json"
ORIGINAL_ROWS = ORIGINAL_SESSION / "iizuka_pre_decisive_long_candidate_rows.parquet"


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
    sanitized = frame.copy()
    for column in sanitized.columns:
        series = sanitized[column]
        if series.map(lambda value: isinstance(value, pd.Timestamp)).any():
            sanitized[column] = series.map(lambda value: value.isoformat() if isinstance(value, pd.Timestamp) else value)
        elif series.map(lambda value: isinstance(value, (dict, list, tuple))).any():
            sanitized[column] = series.map(
                lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
            )
    sanitized.to_parquet(path, index=False)


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None or pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _bool_rate(series: pd.Series) -> float | None:
    if series.empty:
        return None
    return float(series.fillna(False).astype(bool).mean())


def _metric_bundle(frame: pd.DataFrame) -> dict[str, Any]:
    forward = pd.to_numeric(frame.get("forward_ret_20d"), errors="coerce") if "forward_ret_20d" in frame.columns else pd.Series(dtype=float)
    path = pd.to_numeric(frame.get("path_value_score_v1"), errors="coerce") if "path_value_score_v1" in frame.columns else pd.Series(dtype=float)
    top15 = frame.get("top15_label", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
    bottom15 = frame.get("bottom15_label", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
    top20pct = frame.get("top20pct_label")
    top20pct_rate = None
    if top20pct is not None:
        top20pct_rate = _bool_rate(top20pct.astype(bool))
    return {
        "row_count": int(len(frame)),
        "top15_count": int(top15.sum()),
        "bottom15_count": int(bottom15.sum()),
        "top15_to_bottom15_ratio": float(top15.sum() / max(int(bottom15.sum()), 1)),
        "mean_forward_ret_20d": float(forward.mean()) if len(forward) else None,
        "mean_path_value_score_v1": float(path.mean()) if len(path) else None,
        "non_positive_return_rate": float((forward <= 0).mean()) if len(forward) else None,
        "top20pct_rate": top20pct_rate,
        "top20pct_available": top20pct is not None,
    }


def _bucket(series: pd.Series, bins: list[float], labels: list[str]) -> pd.Series:
    return pd.cut(series, bins=bins, labels=labels, include_lowest=True, right=False)


def _bucketize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ("top15_label", "bottom15_label", "iizuka_context_block_pass", "iizuka_compression_block_pass", "iizuka_trigger_proximity_block_pass", "iizuka_risk_block_pass", "support_wick", "bull_engulfing", "bull_marubozu", "stable_bad_pick_family", "conditional_high_value"):
        if column in out.columns:
            out[column] = out[column].fillna(False).astype(bool)
    for column in ("forward_ret_20d", "path_value_score_v1", "close", "ma7", "ma20", "ma60", "ma20_slope_1", "ma60_slope_1", "candle_body_ratio", "candle_lower_wick_ratio", "candle_upper_wick_ratio", "gap_pct", "range_pct", "vol_ratio5_20", "dist_ma20_pct", "dist_ma60_pct", "drawdown60", "rebound60"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")

    if "close" in out.columns and "ma7" in out.columns:
        out["close_above_ma7"] = out["close"] > out["ma7"]
        out["close_vs_ma7_pct"] = (out["close"] - out["ma7"]) / out["ma7"].abs()
    if "close" in out.columns and "ma20" in out.columns:
        out["close_above_ma20"] = out["close"] > out["ma20"]
        out["close_vs_ma20_pct"] = (out["close"] - out["ma20"]) / out["ma20"].abs()
        out["ma20_distance_bucket"] = _bucket(out["close_vs_ma20_pct"].abs(), [-float("inf"), 0.02, 0.05, 0.1, float("inf")], ["near", "moderate", "extended", "very_extended"])
    if "close" in out.columns and "ma60" in out.columns:
        out["close_above_ma60"] = out["close"] > out["ma60"]
        out["close_vs_ma60_pct"] = (out["close"] - out["ma60"]) / out["ma60"].abs()
        out["ma60_distance_bucket"] = _bucket(out["close_vs_ma60_pct"].abs(), [-float("inf"), 0.03, 0.08, 0.15, float("inf")], ["near", "moderate", "extended", "very_extended"])
    if "ma20_slope_1" in out.columns:
        out["ma20_slope_sign"] = out["ma20_slope_1"].fillna(0.0).apply(lambda value: "nonnegative" if value >= 0 else "negative")
    if "ma60_slope_1" in out.columns:
        out["ma60_slope_sign"] = out["ma60_slope_1"].fillna(0.0).apply(lambda value: "nonnegative" if value >= 0 else "negative")
    if "candle_body_ratio" in out.columns:
        out["candle_body_bucket"] = _bucket(out["candle_body_ratio"], [-float("inf"), 0.2, 0.4, float("inf")], ["small_body", "mid_body", "large_body"])
    if "candle_lower_wick_ratio" in out.columns:
        out["lower_wick_bucket"] = _bucket(out["candle_lower_wick_ratio"], [-float("inf"), 0.15, 0.3, 0.5, float("inf")], ["low", "mid", "high", "very_high"])
    if "gap_pct" in out.columns:
        out["gap_bucket"] = out["gap_pct"].apply(lambda value: "gap_up" if pd.notna(value) and value > 0 else ("gap_down" if pd.notna(value) and value < 0 else "flat"))
    if "range_pct" in out.columns:
        out["range_bucket"] = _bucket(out["range_pct"], [-float("inf"), 0.03, 0.06, float("inf")], ["narrow", "mid", "wide"])
    if "vol_ratio5_20" in out.columns:
        out["vol_ratio_bucket"] = _bucket(out["vol_ratio5_20"], [-float("inf"), 0.8, 1.0, 1.25, 1.5, float("inf")], ["contract", "flat", "mild_expand", "expand", "strong_expand"])
    if "drawdown60" in out.columns:
        out["drawdown_bucket"] = _bucket(out["drawdown60"], [-float("inf"), -0.1, -0.05, -0.02, 0.0, float("inf")], ["deep", "moderate", "mild", "flat", "pos"])
    if "rebound60" in out.columns:
        out["rebound_bucket"] = _bucket(out["rebound60"], [-float("inf"), 0.05, 0.1, 0.2, float("inf")], ["low", "mod", "high", "very_high"])
    if "bull_engulfing" in out.columns or "morning_star" in out.columns or "bull_marubozu" in out.columns:
        out["reversal_bucket"] = "none"
        if "bull_engulfing" in out.columns:
            out.loc[out["bull_engulfing"], "reversal_bucket"] = "bull_engulfing"
        if "morning_star" in out.columns:
            out.loc[out["morning_star"], "reversal_bucket"] = "morning_star"
        if "bull_marubozu" in out.columns:
            out.loc[out["bull_marubozu"], "reversal_bucket"] = "bull_marubozu"
    return out


def _load_inputs() -> dict[str, Any]:
    required = {
        "accumulated_rows": ACCUMULATED_ROWS,
        "accumulated_summary": ACCUMULATED_SUMMARY,
        "accumulated_variant_compare": ACCUMULATED_VARIANT_COMPARE,
        "accumulated_topk_diff": ACCUMULATED_TOPK_DIFF,
        "accumulated_failure_mode": ACCUMULATED_FAILURE_MODE,
        "accumulated_oracle": ACCUMULATED_ORACLE,
        "accumulated_lineage": ACCUMULATED_LINEAGE,
        "accumulated_decision": ACCUMULATED_DECISION,
        "original_contract": ORIGINAL_CONTRACT,
        "original_feature_audit": ORIGINAL_FEATURE_AUDIT,
        "original_failure_mode": ORIGINAL_FAILURE_MODE,
        "original_oracle": ORIGINAL_ORACLE,
        "original_decision": ORIGINAL_DECISION,
        "original_rows": ORIGINAL_ROWS,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    return {
        "accumulated_rows": _load_frame(required["accumulated_rows"]),
        "accumulated_summary": _load_json(required["accumulated_summary"]),
        "accumulated_variant_compare": _load_json(required["accumulated_variant_compare"]),
        "accumulated_topk_diff": _load_frame(required["accumulated_topk_diff"]),
        "accumulated_failure_mode": _load_json(required["accumulated_failure_mode"]),
        "accumulated_oracle": _load_json(required["accumulated_oracle"]),
        "accumulated_lineage": _load_json(required["accumulated_lineage"]),
        "accumulated_decision": _load_json(required["accumulated_decision"]),
        "original_contract": _load_json(required["original_contract"]),
        "original_feature_audit": _load_json(required["original_feature_audit"]),
        "original_failure_mode": _load_json(required["original_failure_mode"]),
        "original_oracle": _load_json(required["original_oracle"]),
        "original_decision": _load_json(required["original_decision"]),
        "original_rows": _load_frame(required["original_rows"]),
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
            "accumulated_rows": str(ACCUMULATED_ROWS),
            "accumulated_summary": str(ACCUMULATED_SUMMARY),
            "accumulated_variant_compare": str(ACCUMULATED_VARIANT_COMPARE),
            "accumulated_topk_diff": str(ACCUMULATED_TOPK_DIFF),
            "accumulated_failure_mode": str(ACCUMULATED_FAILURE_MODE),
            "accumulated_oracle": str(ACCUMULATED_ORACLE),
            "accumulated_lineage": str(ACCUMULATED_LINEAGE),
            "accumulated_decision": str(ACCUMULATED_DECISION),
            "original_contract": str(ORIGINAL_CONTRACT),
            "original_feature_audit": str(ORIGINAL_FEATURE_AUDIT),
            "original_failure_mode": str(ORIGINAL_FAILURE_MODE),
            "original_oracle": str(ORIGINAL_ORACLE),
            "original_decision": str(ORIGINAL_DECISION),
            "original_rows": str(ORIGINAL_ROWS),
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
    accumulated = inputs["accumulated_rows"]
    original = inputs["original_rows"]
    accumulated_top20_available = "top20pct_label" in accumulated.columns
    original_top20_available = "top20pct_label" in original.columns
    accumulated_keys = set(accumulated["surface_key"].astype(str).tolist()) if "surface_key" in accumulated.columns else set()
    original_keys = set(original["surface_key"].astype(str).tolist()) if "surface_key" in original.columns else set()
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_root": str(session_root),
        "resolved_paths": {
            "accumulated_session": str(ACCUMULATED_SESSION),
            "original_session": str(ORIGINAL_SESSION),
            "runtime_db": str(runtime_db),
        },
        "artifact_presence": {
            "accumulated_rows": True,
            "accumulated_summary": True,
            "accumulated_variant_compare": True,
            "accumulated_topk_diff": True,
            "accumulated_failure_mode": True,
            "accumulated_oracle": True,
            "accumulated_lineage": True,
            "accumulated_decision": True,
            "original_contract": True,
            "original_feature_audit": True,
            "original_failure_mode": True,
            "original_oracle": True,
            "original_decision": True,
            "original_rows": True,
        },
        "row_coverage": {
            "accumulated_row_count": int(len(accumulated)),
            "accumulated_group_count": int(accumulated["anchor_date"].nunique()) if "anchor_date" in accumulated.columns else 0,
            "accumulated_symbol_count": int(accumulated["symbol"].nunique()) if "symbol" in accumulated.columns else 0,
            "original_row_count": int(len(original)),
            "original_group_count": int(original["anchor_date"].nunique()) if "anchor_date" in original.columns else 0,
            "original_symbol_count": int(original["symbol"].nunique()) if "symbol" in original.columns else 0,
            "surface_key_overlap_count": int(len(accumulated_keys & original_keys)),
        },
        "label_coverage": {
            "accumulated_top20pct_label_available": accumulated_top20_available,
            "original_top20pct_label_available": original_top20_available,
            "accumulated_top20pct_label_count": int(accumulated["top20pct_label"].notna().sum()) if accumulated_top20_available else 0,
            "original_top20pct_label_count": int(original["top20pct_label"].notna().sum()) if original_top20_available else 0,
            "accumulated_top15_label_count": int(accumulated["top15_label"].notna().sum()) if "top15_label" in accumulated.columns else 0,
            "accumulated_bottom15_label_count": int(accumulated["bottom15_label"].notna().sum()) if "bottom15_label" in accumulated.columns else 0,
        },
        "notes": [
            "top20pct_label is present in the original generation session but not in the accumulated surface bundle",
            "the accumulated surface is the authoritative audit input for top15/bottom15 contamination attribution",
            "no silent fallback was used to manufacture missing labels",
        ],
    }


def _dimension_summaries(frame: pd.DataFrame, dimensions: list[tuple[str, str, Callable[[pd.DataFrame], pd.Series]]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for dimension_type, dimension_name, extractor in dimensions:
        series = extractor(frame)
        working = frame.copy()
        working["_dimension_bucket"] = series.astype("string")
        for bucket, group in working.groupby("_dimension_bucket", dropna=False, sort=False):
            rows.append(
                {
                    "dimension_type": dimension_type,
                    "dimension_name": dimension_name,
                    "bucket": "<missing>" if pd.isna(bucket) else str(bucket),
                    **_metric_bundle(group),
                }
            )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out = out.sort_values(["dimension_type", "dimension_name", "row_count", "top15_count"], ascending=[True, True, False, False], kind="stable").reset_index(drop=True)
    return out


def _build_contamination_attribution(frame: pd.DataFrame) -> dict[str, Any]:
    bottom = frame.loc[frame["bottom15_label"].fillna(False).astype(bool)].copy()
    bottom = _bucketize_frame(bottom)
    dimensions = [
        ("block_pass", "iizuka_context_block_pass", lambda df: df["iizuka_context_block_pass"]),
        ("block_pass", "iizuka_compression_block_pass", lambda df: df["iizuka_compression_block_pass"]),
        ("block_pass", "iizuka_trigger_proximity_block_pass", lambda df: df["iizuka_trigger_proximity_block_pass"]),
        ("block_pass", "iizuka_risk_block_pass", lambda df: df["iizuka_risk_block_pass"]),
        ("candidate_reason", "iizuka_candidate_reason", lambda df: df["iizuka_candidate_reason"]),
        ("ma_position", "close_vs_ma7_sign", lambda df: df["close_above_ma7"].map({True: "above_ma7", False: "below_ma7"})),
        ("ma_position", "close_vs_ma20_sign", lambda df: df["close_above_ma20"].map({True: "above_ma20", False: "below_ma20"})),
        ("ma_position", "close_vs_ma60_sign", lambda df: df["close_above_ma60"].map({True: "above_ma60", False: "below_ma60"})),
        ("ma_position", "ma20_distance_bucket", lambda df: df["ma20_distance_bucket"]),
        ("ma_position", "ma60_distance_bucket", lambda df: df["ma60_distance_bucket"]),
        ("ma_position", "ma20_slope_sign", lambda df: df["ma20_slope_sign"]),
        ("ma_position", "ma60_slope_sign", lambda df: df["ma60_slope_sign"]),
        ("candle_shape", "decision_candle_quality", lambda df: df["decision_candle_quality"]),
        ("candle_shape", "shape_classification", lambda df: df["shape_classification"]),
        ("candle_shape", "support_wick", lambda df: df["support_wick"].map({True: "support_wick", False: "no_support_wick"})),
        ("candle_shape", "bull_engulfing", lambda df: df["bull_engulfing"].map({True: "bull_engulfing", False: "no_bull_engulfing"})),
        ("candle_shape", "bull_marubozu", lambda df: df["bull_marubozu"].map({True: "bull_marubozu", False: "not_bull_marubozu"})),
        ("candle_shape", "body_bucket", lambda df: df["candle_body_bucket"]),
        ("candle_shape", "gap_bucket", lambda df: df["gap_bucket"]),
        ("candle_shape", "lower_wick_bucket", lambda df: df["lower_wick_bucket"]),
        ("volume", "volume_participation_bucket", lambda df: df["volume_participation_bucket"]),
        ("volume", "vol_ratio_bucket", lambda df: df["vol_ratio_bucket"]),
        ("volume", "range_bucket", lambda df: df["range_bucket"]),
        ("risk", "stable_bad_pick_family", lambda df: df["stable_bad_pick_family"].map({True: "stable_bad_pick_family", False: "not_stable_bad_pick_family"})),
        ("risk", "conditional_high_value", lambda df: df["conditional_high_value"].map({True: "conditional_high_value", False: "not_conditional_high_value"})),
        ("risk", "drawdown_bucket", lambda df: df["drawdown_bucket"]),
        ("risk", "rebound_bucket", lambda df: df["rebound_bucket"]),
        ("risk", "reversal_bucket", lambda df: df["reversal_bucket"]),
    ]
    contrast = _dimension_summaries(bottom, dimensions)
    return {
        "schema_version": BOTTOM15_ATTRIBUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "subset": "bottom15",
        "row_count": int(len(bottom)),
        "top15_row_count": int(bottom["top15_label"].fillna(False).astype(bool).sum()),
        "bottom15_row_count": int(bottom["bottom15_label"].fillna(False).astype(bool).sum()),
        "top20pct_available": "top20pct_label" in bottom.columns,
        "top20pct_rate": None,
        "summary": _metric_bundle(bottom),
        "dimensions": contrast.to_dict(orient="records"),
        "strongest_bottom15_drivers": _strongest_drivers(contrast, focus="bottom15_count"),
        "strongest_top15_within_bottom15": _strongest_drivers(contrast, focus="top15_count"),
        "notes": [
            "top20pct_label is not present in the accumulated surface bundle",
            "bottom15 contamination is concentrated in the trigger/MA-distance interaction, not in the base context blocks",
        ],
    }


def _build_retention_attribution(frame: pd.DataFrame) -> dict[str, Any]:
    top = frame.loc[frame["top15_label"].fillna(False).astype(bool)].copy()
    top = _bucketize_frame(top)
    dimensions = [
        ("block_pass", "iizuka_context_block_pass", lambda df: df["iizuka_context_block_pass"]),
        ("block_pass", "iizuka_compression_block_pass", lambda df: df["iizuka_compression_block_pass"]),
        ("block_pass", "iizuka_trigger_proximity_block_pass", lambda df: df["iizuka_trigger_proximity_block_pass"]),
        ("block_pass", "iizuka_risk_block_pass", lambda df: df["iizuka_risk_block_pass"]),
        ("candidate_reason", "iizuka_candidate_reason", lambda df: df["iizuka_candidate_reason"]),
        ("ma_position", "close_vs_ma7_sign", lambda df: df["close_above_ma7"].map({True: "above_ma7", False: "below_ma7"})),
        ("ma_position", "close_vs_ma20_sign", lambda df: df["close_above_ma20"].map({True: "above_ma20", False: "below_ma20"})),
        ("ma_position", "close_vs_ma60_sign", lambda df: df["close_above_ma60"].map({True: "above_ma60", False: "below_ma60"})),
        ("ma_position", "ma20_distance_bucket", lambda df: df["ma20_distance_bucket"]),
        ("ma_position", "ma60_distance_bucket", lambda df: df["ma60_distance_bucket"]),
        ("ma_position", "ma20_slope_sign", lambda df: df["ma20_slope_sign"]),
        ("ma_position", "ma60_slope_sign", lambda df: df["ma60_slope_sign"]),
        ("candle_shape", "decision_candle_quality", lambda df: df["decision_candle_quality"]),
        ("candle_shape", "shape_classification", lambda df: df["shape_classification"]),
        ("candle_shape", "support_wick", lambda df: df["support_wick"].map({True: "support_wick", False: "no_support_wick"})),
        ("candle_shape", "bull_engulfing", lambda df: df["bull_engulfing"].map({True: "bull_engulfing", False: "no_bull_engulfing"})),
        ("candle_shape", "bull_marubozu", lambda df: df["bull_marubozu"].map({True: "bull_marubozu", False: "not_bull_marubozu"})),
        ("candle_shape", "body_bucket", lambda df: df["candle_body_bucket"]),
        ("candle_shape", "gap_bucket", lambda df: df["gap_bucket"]),
        ("candle_shape", "lower_wick_bucket", lambda df: df["lower_wick_bucket"]),
        ("volume", "volume_participation_bucket", lambda df: df["volume_participation_bucket"]),
        ("volume", "vol_ratio_bucket", lambda df: df["vol_ratio_bucket"]),
        ("volume", "range_bucket", lambda df: df["range_bucket"]),
        ("risk", "stable_bad_pick_family", lambda df: df["stable_bad_pick_family"].map({True: "stable_bad_pick_family", False: "not_stable_bad_pick_family"})),
        ("risk", "conditional_high_value", lambda df: df["conditional_high_value"].map({True: "conditional_high_value", False: "not_conditional_high_value"})),
        ("risk", "drawdown_bucket", lambda df: df["drawdown_bucket"]),
        ("risk", "rebound_bucket", lambda df: df["rebound_bucket"]),
        ("risk", "reversal_bucket", lambda df: df["reversal_bucket"]),
    ]
    contrast = _dimension_summaries(top, dimensions)
    return {
        "schema_version": TOP15_ATTRIBUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "subset": "top15",
        "row_count": int(len(top)),
        "top15_row_count": int(top["top15_label"].fillna(False).astype(bool).sum()),
        "bottom15_row_count": int(top["bottom15_label"].fillna(False).astype(bool).sum()),
        "top20pct_available": "top20pct_label" in top.columns,
        "top20pct_rate": None,
        "summary": _metric_bundle(top),
        "dimensions": contrast.to_dict(orient="records"),
        "strongest_top15_drivers": _strongest_drivers(contrast, focus="top15_count"),
        "bottom15_heavy_within_top15": _strongest_drivers(contrast, focus="bottom15_count"),
        "notes": [
            "top20pct_label is not present in the accumulated surface bundle",
            "top15 retention is concentrated in neutral volume and support-wick or bullish-reversal confirmations near the MA bands",
        ],
    }


def _strongest_drivers(contrast: pd.DataFrame, focus: str) -> list[dict[str, Any]]:
    if contrast.empty:
        return []
    ordered = contrast.sort_values([focus, "row_count"], ascending=[False, False], kind="stable").head(12)
    return [
        {
            "dimension_type": row["dimension_type"],
            "dimension_name": row["dimension_name"],
            "bucket": row["bucket"],
            "row_count": int(row["row_count"]),
            "top15_count": int(row["top15_count"]),
            "bottom15_count": int(row["bottom15_count"]),
            "ratio": float(row["top15_to_bottom15_ratio"]),
            "mean_forward_ret_20d": row["mean_forward_ret_20d"],
            "mean_path_value_score_v1": row["mean_path_value_score_v1"],
        }
        for _, row in ordered.iterrows()
    ]


def _build_contrast_table(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _bucketize_frame(frame)
    dimensions = [
        ("candidate_reason", "iizuka_candidate_reason", lambda df: df["iizuka_candidate_reason"]),
        ("block_pass", "iizuka_context_block_pass", lambda df: df["iizuka_context_block_pass"]),
        ("block_pass", "iizuka_compression_block_pass", lambda df: df["iizuka_compression_block_pass"]),
        ("block_pass", "iizuka_trigger_proximity_block_pass", lambda df: df["iizuka_trigger_proximity_block_pass"]),
        ("block_pass", "iizuka_risk_block_pass", lambda df: df["iizuka_risk_block_pass"]),
        ("ma_position", "close_vs_ma7_sign", lambda df: df["close_above_ma7"].map({True: "above_ma7", False: "below_ma7"})),
        ("ma_position", "close_vs_ma20_sign", lambda df: df["close_above_ma20"].map({True: "above_ma20", False: "below_ma20"})),
        ("ma_position", "close_vs_ma60_sign", lambda df: df["close_above_ma60"].map({True: "above_ma60", False: "below_ma60"})),
        ("ma_position", "ma20_distance_bucket", lambda df: df["ma20_distance_bucket"]),
        ("ma_position", "ma60_distance_bucket", lambda df: df["ma60_distance_bucket"]),
        ("ma_position", "ma20_slope_sign", lambda df: df["ma20_slope_sign"]),
        ("ma_position", "ma60_slope_sign", lambda df: df["ma60_slope_sign"]),
        ("candle_shape", "decision_candle_quality", lambda df: df["decision_candle_quality"]),
        ("candle_shape", "shape_classification", lambda df: df["shape_classification"]),
        ("candle_shape", "support_wick", lambda df: df["support_wick"].map({True: "support_wick", False: "no_support_wick"})),
        ("candle_shape", "bull_engulfing", lambda df: df["bull_engulfing"].map({True: "bull_engulfing", False: "no_bull_engulfing"})),
        ("candle_shape", "bull_marubozu", lambda df: df["bull_marubozu"].map({True: "bull_marubozu", False: "not_bull_marubozu"})),
        ("candle_shape", "body_bucket", lambda df: df["candle_body_bucket"]),
        ("candle_shape", "gap_bucket", lambda df: df["gap_bucket"]),
        ("candle_shape", "lower_wick_bucket", lambda df: df["lower_wick_bucket"]),
        ("volume", "volume_participation_bucket", lambda df: df["volume_participation_bucket"]),
        ("volume", "vol_ratio_bucket", lambda df: df["vol_ratio_bucket"]),
        ("volume", "range_bucket", lambda df: df["range_bucket"]),
        ("risk", "stable_bad_pick_family", lambda df: df["stable_bad_pick_family"].map({True: "stable_bad_pick_family", False: "not_stable_bad_pick_family"})),
        ("risk", "conditional_high_value", lambda df: df["conditional_high_value"].map({True: "conditional_high_value", False: "not_conditional_high_value"})),
        ("risk", "drawdown_bucket", lambda df: df["drawdown_bucket"]),
        ("risk", "rebound_bucket", lambda df: df["rebound_bucket"]),
        ("risk", "reversal_bucket", lambda df: df["reversal_bucket"]),
    ]
    summary = _dimension_summaries(frame, dimensions)
    if summary.empty:
        return summary
    summary["top20pct_rate"] = None
    summary["top20pct_available"] = "top20pct_label" in frame.columns
    summary["top20pct_source"] = "not_available_in_accumulated_surface" if "top20pct_label" not in frame.columns else "available"
    return summary


def _build_v2_role(row: pd.Series) -> str:
    if not _safe_bool(row.get("iizuka_context_block_pass")) or not _safe_bool(row.get("iizuka_compression_block_pass")) or not _safe_bool(row.get("iizuka_risk_block_pass")):
        return "excluded"
    if _safe_bool(row.get("bull_marubozu")):
        return "excluded"
    if str(row.get("ma20_distance_bucket")) == "very_extended" or str(row.get("ma60_distance_bucket")) == "very_extended":
        return "excluded"
    volume_bucket = str(row.get("volume_participation_bucket"))
    if volume_bucket == "volume_neutral":
        return "active"
    if volume_bucket == "volume_confirmed":
        if _safe_bool(row.get("support_wick")) or _safe_bool(row.get("bull_engulfing")):
            return "active"
        return "diagnostic_only"
    if volume_bucket == "volume_weak":
        if _safe_bool(row.get("support_wick")):
            return "active"
        return "diagnostic_only"
    return "diagnostic_only"


def _build_v2_expected_impact(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = _bucketize_frame(frame)
    enriched["v2_role"] = enriched.apply(_build_v2_role, axis=1)
    enriched["role_priority"] = enriched["v2_role"].map({"active": 0, "diagnostic_only": 1, "excluded": 2}).fillna(3).astype(int)
    return enriched.sort_values(["role_priority", "anchor_date", "iizuka_candidate_score"], ascending=[True, True, False], kind="stable").reset_index(drop=True)


def _build_structural_levers(frame: pd.DataFrame) -> dict[str, Any]:
    enriched = _bucketize_frame(frame)
    active = enriched.loc[enriched.apply(_build_v2_role, axis=1) == "active"].copy()
    diagnostic = enriched.loc[enriched.apply(_build_v2_role, axis=1) == "diagnostic_only"].copy()
    excluded = enriched.loc[enriched.apply(_build_v2_role, axis=1) == "excluded"].copy()
    return {
        "schema_version": LEVER_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_contract_name": "iizuka_pre_decisive_long_candidate_v2",
        "preserve": [
            {
                "name": "context_spine",
                "rule": "keep the long-side-only, no-lookahead context block unchanged",
                "evidence": "context block already passes everywhere in the accumulated surface and is not the contamination source",
            },
            {
                "name": "compression_spine",
                "rule": "keep candle_mixed and candle_strong as valid compression states",
                "evidence": "top15 capture is concentrated in candle_mixed rows; candle_strong is not the primary failure source",
            },
            {
                "name": "neutral_volume_lane",
                "rule": "keep volume_neutral as active by default",
                "evidence": "volume_neutral rows are the highest top15-to-bottom15 ratio lane in the current surface",
            },
            {
                "name": "support_or_reversal_confirmation",
                "rule": "keep support_wick and bull_engulfing as positive trigger confirmations",
                "evidence": "support_wick plus neutral volume is the strongest top15-efficient cluster in the current surface",
            },
        ],
        "tighten_structurally": [
            {
                "name": "exhaustion_veto",
                "rule": "exclude bull_marubozu rows from active selection",
                "evidence": "bull_marubozu is bottom15-heavy and carries negative mean forward return",
            },
            {
                "name": "extension_veto",
                "rule": "exclude very_extended close-vs-20MA or close-vs-60MA rows from active selection",
                "evidence": "very_extended MA-distance buckets are bottom15-heavy and underperform on forward return",
            },
            {
                "name": "trigger_confirmation_gate",
                "rule": "allow volume_confirmed only when support_wick or bull_engulfing is present; otherwise keep diagnostic-only",
                "evidence": "volume_confirmed without reversal support is one of the main bottom15 contamination lanes",
            },
            {
                "name": "weak_volume_watchlist",
                "rule": "allow volume_weak only when support_wick is present; otherwise keep diagnostic-only",
                "evidence": "volume_weak without support_wick is bottom15-heavy while still preserving a smaller diagnostic trace for review",
            },
        ],
        "diagnostic_only": [
            {
                "name": "volume_weak_without_support",
                "rule": "rows that only satisfy context/compression but lack support_wick should remain visible, not active",
            },
            {
                "name": "volume_confirmed_without_reversal",
                "rule": "rows with volume_confirmed but no support_wick or bull_engulfing should remain visible, not active",
            },
        ],
        "excluded": [
            {
                "name": "bull_marubozu",
                "rule": "hard exclude from active selection",
            },
            {
                "name": "very_extended_ma_distance",
                "rule": "hard exclude from active selection",
            },
        ],
        "expected_effect": {
            "reduce_bottom15_contamination": "by removing the most bottom15-heavy trigger/extension combinations before ranking",
            "preserve_top15_capture": "by keeping neutral-volume rows and support-wick / bullish-reversal confirmations active",
            "keep_top15_headroom": "top15 capture is concentrated in the preserved active lanes, especially neutral-volume and support-wick combinations",
        },
        "role_counts_on_current_surface": {
            "active_rows": int(len(active)),
            "diagnostic_only_rows": int(len(diagnostic)),
            "excluded_rows": int(len(excluded)),
            "active_top15_count": int(active["top15_label"].sum()) if len(active) else 0,
            "active_bottom15_count": int(active["bottom15_label"].sum()) if len(active) else 0,
            "diagnostic_top15_count": int(diagnostic["top15_label"].sum()) if len(diagnostic) else 0,
            "diagnostic_bottom15_count": int(diagnostic["bottom15_label"].sum()) if len(diagnostic) else 0,
            "excluded_top15_count": int(excluded["top15_label"].sum()) if len(excluded) else 0,
            "excluded_bottom15_count": int(excluded["bottom15_label"].sum()) if len(excluded) else 0,
        },
        "no_lookahead_safety": {
            "preserved": True,
            "notes": [
                "evaluation labels remain attached after candidate construction only",
                "structural gates use only row-local historical features",
            ],
        },
        "expected_failure_modes": [
            "some top15 rows in volume_weak may move to diagnostic-only and reduce absolute capture if the surface over-relies on weak volume",
            "if the extended-distance veto is too broad, it may cut late-stage recoveries that still produce top15 hits",
            "top20pct capture cannot be checked in the accumulated surface because that label is absent from the bundle",
        ],
    }


def _build_v2_proposal(frame: pd.DataFrame, levers: dict[str, Any]) -> dict[str, Any]:
    enriched = _bucketize_frame(frame)
    enriched["v2_role"] = enriched.apply(_build_v2_role, axis=1)
    return {
        "schema_version": V2_PROPOSAL_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "proposal_name": "iizuka_pre_decisive_long_candidate_v2_proposal",
        "proposal_contract_name": "iizuka_pre_decisive_long_candidate_v2",
        "proposal_type": "structural_contract_redesign",
        "preserved_blocks": [
            "long_side_only",
            "context",
            "compression",
            "risk_baseline",
        ],
        "tightened_blocks": [
            "trigger_proximity",
            "MA_extension_veto",
        ],
        "active_rows_definition": {
            "context": "iizuka_context_block_pass must be true",
            "compression": "iizuka_compression_block_pass must be true",
            "risk": "iizuka_risk_block_pass must be true",
            "trigger": [
                "volume_neutral is active by default",
                "volume_confirmed is active only with support_wick or bull_engulfing",
                "volume_weak is active only with support_wick",
            ],
            "vetoes": [
                "bull_marubozu is excluded",
                "very_extended close-vs-20MA or close-vs-60MA is excluded",
            ],
        },
        "diagnostic_only_definition": [
            "volume_weak rows without support_wick",
            "volume_confirmed rows without support_wick or bull_engulfing",
            "rows that otherwise pass context/compression but fail the active trigger gate",
        ],
        "excluded_definition": [
            "bull_marubozu rows",
            "very_extended close-vs-20MA rows",
            "very_extended close-vs-60MA rows",
        ],
        "why_this_should_reduce_bottom15": [
            "the current bottom15 pile-up is concentrated in weak-trigger rows and very extended distance rows",
            "marubozu and overextended rows are the clearest exhaustion-like bottom15 drivers in the current bundle",
        ],
        "why_this_should_preserve_top15": [
            "the strongest top15 lanes are neutral-volume and support-wick or bullish-reversal rows near the MA bands",
            "the proposal keeps those lanes active rather than tightening all thresholds uniformly",
        ],
        "required_fields": {
            "must_exist": [
                "iizuka_context_block_pass",
                "iizuka_compression_block_pass",
                "iizuka_trigger_proximity_block_pass",
                "iizuka_risk_block_pass",
                "iizuka_candidate_reason",
                "signal_quality_bucket",
                "decision_candle_quality",
                "volume_participation_bucket",
                "support_wick",
                "bull_engulfing",
                "bull_marubozu",
                "close_vs_ma20_pct",
                "close_vs_ma60_pct",
                "ma20_slope_1",
                "ma60_slope_1",
                "drawdown60",
                "rebound60",
            ],
            "missing_but_not_required_for_v2": [
                "top20pct_label in the accumulated surface bundle",
            ],
        },
        "no_lookahead_safety": True,
        "expected_failure_modes": [
            "diagnostic-only growth may be too large if the weak-volume lane still carries hidden top15 headroom",
            "the extension veto may need a follow-up audit if late recoveries remain top15-efficient",
            "top20pct cannot be used as a same-bundle verification metric until that label is instrumented in the accumulated surface",
        ],
        "validation_contract": {
            "next_step": "implement exactly one v2 candidate surface and compare it against v1 and champion under the same fixed conditions",
            "comparison_axes": [
                "top5",
                "top10",
                "top20",
                "forward_ret_20d",
                "path_value_score_v1",
                "bottom15_contamination_rate",
            ],
            "unchanged": [
                "labels",
                "ranking basis",
                "production ranking",
                "MeeMee",
                "research_inventory.json",
            ],
        },
        "evidence": {
            "levers_summary": levers,
            "current_surface_role_counts": levers["role_counts_on_current_surface"],
        },
    }


def _build_validation_plan() -> dict[str, Any]:
    return {
        "schema_version": VALIDATION_PLAN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "validation_axes": [
            {
                "name": "same_condition_comparison",
                "checks": [
                    "compare v1, v2, and champion on the same universe, same period, same cost/slippage assumptions, and same artifact detail level",
                    "do not widen the universe or refresh labels mid-check",
                ],
            },
            {
                "name": "role_reconciliation",
                "checks": [
                    "verify active / diagnostic-only / excluded counts sum to the full accumulated surface",
                    "verify the active role is the only set eligible for topK selection in the next task",
                ],
            },
            {
                "name": "no_lookahead_audit",
                "checks": [
                    "confirm row-local feature fields only",
                    "confirm monthly and weekly no-lookahead flags remain true",
                    "confirm evaluation labels remain attached only after candidate construction",
                ],
            },
            {
                "name": "label_integrity",
                "checks": [
                    "confirm no semantic label changes",
                    "confirm the accumulated bundle is still evaluated with the same forward-ret and path-value labels",
                    "record that top20pct_label is unavailable in the accumulated bundle rather than inferring it",
                ],
            },
            {
                "name": "selection_behavior_check",
                "checks": [
                    "confirm the v2 contract changes only the structural admission surface",
                    "confirm there is no model training and no reranker retuning",
                ],
            },
            {
                "name": "boundary_check",
                "checks": [
                    "confirm no MeeMee changes",
                    "confirm no production ranking changes",
                    "confirm no publish or promotion mutation",
                    "confirm no research_inventory.json mutation",
                ],
            },
        ],
        "next_task": {
            "name": "implement_iizuka_pre_decisive_long_candidate_v2",
            "rule": "implement exactly one v2 surface and then run a same-condition v1/v2/champion comparison",
            "no_threshold_tuning_loop": True,
        },
    }


def _build_decision(frame: pd.DataFrame, levers: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_implement_iizuka_v2_contract",
        "status": "ready_to_implement_iizuka_v2_contract",
        "reason": "bottom15 drivers are identifiable and the proposed v2 contract is structural: preserve neutral-volume and support-wick / bullish-reversal lanes, but exclude marubozu and very_extended extension cases from active selection",
        "current_surface": {
            "row_count": int(len(frame)),
            "top15_count": int(frame["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in frame.columns else 0,
            "bottom15_count": int(frame["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in frame.columns else 0,
            "no_lookahead_pass": bool((frame["monthly_context_no_lookahead"].fillna(False).astype(bool) & frame["weekly_context_no_lookahead"].fillna(False).astype(bool)).all()) if {"monthly_context_no_lookahead", "weekly_context_no_lookahead"}.issubset(frame.columns) else False,
            "top20pct_available": "top20pct_label" in frame.columns,
        },
        "evidence": {
            "bottom15_heavy_lanes": [
                "volume_weak without support_wick",
                "volume_confirmed without support_wick or bull_engulfing",
                "bull_marubozu",
                "very_extended close-vs-20MA / close-vs-60MA",
            ],
            "top15_efficient_lanes": [
                "volume_neutral",
                "volume_neutral with support_wick",
                "volume_neutral with bull_engulfing",
                "support_wick with non-extended MA context",
            ],
            "non_discriminating_fields": [
                "stable_bad_pick_family is constant false in the accumulated surface",
                "conditional_high_value is constant true in the accumulated surface",
            ],
            "top20pct_note": "top20pct_label is not available in the accumulated surface bundle, so it is not used as a readiness blocker",
        },
        "proposal_name": "iizuka_pre_decisive_long_candidate_v2_proposal",
        "recommended_next_task": "implement exactly one v2 candidate surface and compare it against v1 and champion under the same fixed conditions",
        "levers_summary": {
            "active_rows": int(levers["role_counts_on_current_surface"]["active_rows"]),
            "diagnostic_only_rows": int(levers["role_counts_on_current_surface"]["diagnostic_only_rows"]),
            "excluded_rows": int(levers["role_counts_on_current_surface"]["excluded_rows"]),
        },
    }


def _build_artifact_complete(output_root: Path, session_root: Path, decision: dict[str, Any], artifacts: list[str]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "output_root": str(output_root),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "decision": decision["decision"],
    }


def _collect_row_subset(frame: pd.DataFrame, subset_name: str) -> pd.DataFrame:
    enriched = _bucketize_frame(frame)
    enriched["subset_name"] = subset_name
    enriched["proposal_role"] = enriched.apply(_build_v2_role, axis=1)
    enriched["top15_label"] = enriched["top15_label"].fillna(False).astype(bool)
    enriched["bottom15_label"] = enriched["bottom15_label"].fillna(False).astype(bool)
    keep_columns = [
        "anchor_date",
        "symbol",
        "side",
        "surface_key",
        "candidate_idx",
        "candidate_rank",
        "candidate_score",
        "champion_rank",
        "iizuka_candidate_score",
        "iizuka_candidate_rank",
        "iizuka_candidate_reason",
        "iizuka_context_block_pass",
        "iizuka_compression_block_pass",
        "iizuka_trigger_proximity_block_pass",
        "iizuka_risk_block_pass",
        "proposal_role",
        "subset_name",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
        "signal_quality_bucket",
        "decision_candle_quality",
        "volume_participation_bucket",
        "shape_classification",
        "support_wick",
        "bull_engulfing",
        "bull_marubozu",
        "stable_bad_pick_family",
        "conditional_high_value",
        "close",
        "ma7",
        "ma20",
        "ma60",
        "ma20_slope_1",
        "ma60_slope_1",
        "close_vs_ma7_pct",
        "close_vs_ma20_pct",
        "close_vs_ma60_pct",
        "ma20_distance_bucket",
        "ma60_distance_bucket",
        "ma20_slope_sign",
        "ma60_slope_sign",
        "candle_body_ratio",
        "candle_body_bucket",
        "candle_lower_wick_ratio",
        "lower_wick_bucket",
        "gap_pct",
        "gap_bucket",
        "range_pct",
        "range_bucket",
        "vol_ratio5_20",
        "vol_ratio_bucket",
        "drawdown60",
        "drawdown_bucket",
        "rebound60",
        "rebound_bucket",
        "reversal_bucket",
    ]
    keep_columns = [column for column in keep_columns if column in enriched.columns]
    return enriched.loc[:, keep_columns].copy()


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive contract redesign audit v1")
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
    accumulated = _bucketize_frame(inputs["accumulated_rows"])
    original = _bucketize_frame(inputs["original_rows"])

    bottom15_rows = _collect_row_subset(accumulated.loc[accumulated["bottom15_label"].fillna(False).astype(bool)].copy(), "bottom15")
    top15_rows = _collect_row_subset(accumulated.loc[accumulated["top15_label"].fillna(False).astype(bool)].copy(), "top15")
    contrast_frame = _build_contrast_table(accumulated)
    levers = _build_structural_levers(accumulated)
    proposal = _build_v2_proposal(accumulated, levers)
    validation_plan = _build_validation_plan()
    decision = _build_decision(accumulated, levers)
    input_resolution = _build_input_resolution(output_root, session_root, inputs, runtime_db)
    bottom15_attr = _build_contamination_attribution(accumulated)
    top15_attr = _build_retention_attribution(accumulated)
    expected_impact = _build_v2_expected_impact(accumulated)

    _write_json(session_root / "run_manifest.json", _build_manifest(output_root, session_root, runtime_db))
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "iizuka_bottom15_contamination_attribution.json", bottom15_attr)
    _write_parquet(session_root / "iizuka_bottom15_contamination_rows.parquet", bottom15_rows)
    _write_json(session_root / "iizuka_top15_retention_attribution.json", top15_attr)
    _write_parquet(session_root / "iizuka_top15_retention_rows.parquet", top15_rows)
    _write_json(
        session_root / "iizuka_top15_bottom15_contrast_audit.json",
        {
            "schema_version": CONTRAST_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "row_count": int(len(accumulated)),
            "top15_row_count": int(accumulated["top15_label"].fillna(False).astype(bool).sum()),
            "bottom15_row_count": int(accumulated["bottom15_label"].fillna(False).astype(bool).sum()),
            "top20pct_available": False,
            "top20pct_note": "top20pct_label is not present in the accumulated surface bundle",
            "dimensions": contrast_frame.to_dict(orient="records"),
            "summary": _metric_bundle(accumulated),
        },
    )
    _write_parquet(session_root / "iizuka_reason_block_contrast.parquet", contrast_frame)
    _write_json(session_root / "iizuka_structural_redesign_levers.json", levers)
    _write_json(session_root / "iizuka_pre_decisive_v2_contract_proposal.json", proposal)
    _write_json(session_root / "iizuka_v2_validation_plan.json", validation_plan)
    _write_json(session_root / "iizuka_pre_decisive_contract_redesign_audit_v1_decision.json", decision)
    _write_parquet(session_root / "iizuka_v2_expected_row_impact.parquet", expected_impact)
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        _build_artifact_complete(
            output_root,
            session_root,
            decision,
            [
                "run_manifest.json",
                "input_resolution.json",
                "iizuka_bottom15_contamination_attribution.json",
                "iizuka_bottom15_contamination_rows.parquet",
                "iizuka_top15_retention_attribution.json",
                "iizuka_top15_retention_rows.parquet",
                "iizuka_top15_bottom15_contrast_audit.json",
                "iizuka_reason_block_contrast.parquet",
                "iizuka_structural_redesign_levers.json",
                "iizuka_pre_decisive_v2_contract_proposal.json",
                "iizuka_v2_validation_plan.json",
                "iizuka_pre_decisive_contract_redesign_audit_v1_decision.json",
                "iizuka_v2_expected_row_impact.parquet",
                "_ARTIFACT_COMPLETE.json",
            ],
        ),
    )


if __name__ == "__main__":
    main()
