from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _aggregate_selected_rows,
    _apply_anchor_limit,
    _load_candidate_rows,
    _load_json,
    _load_source_family_session,
    _make_session_id,
    _progress_log,
    _rank_selection,
    _resolve_candidate_input_dir as _resolve_candidate_input_dir_impl,
    _safe_float,
    _safe_int,
    _selection_diff_keys,
    _write_json,
)
from scripts.tradex_multi_timeframe_conditional_state_value_v1 import (  # noqa: E402
    _classify_conditional_group,
    _count_classes,
    _build_group_summary_sql,
)

DEFAULT_CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
DEFAULT_SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
DEFAULT_SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_PRIOR_GLOBAL_BOOST_SESSION = Path(r"G:\Tradex\ma_state_family_high_value_boost_v1\20260429T084326Z-2ae0f0de")
DEFAULT_PRIOR_CONTEXT_GATED_BOOST_SESSION = Path(r"G:\Tradex\multi_timeframe_context_gated_high_value_boost_v1\20260429T094730Z-7e1acdee")
DEFAULT_CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\context_gated_candle_shape_modifier_boost_prune_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1"
COMPARE_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_manifest_v1"
MONTHLY_CONTEXT_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_monthly_context_comparison_v1"
WEEKLY_CONTEXT_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_weekly_context_comparison_v1"
SHAPE_COMPARISON_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_shape_comparison_v1"
PRIOR_BOOST_DELTA_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_prior_boost_delta_v1"
COVERAGE_SCHEMA_VERSION = "tradex_context_gated_candle_shape_modifier_boost_prune_v1_shape_adjustment_coverage_v1"

TOP_EXAMPLE_LIMIT = 50
CONDITIONAL_SHAPE_BOOST = 0.04
CONDITIONAL_SHAPE_PRUNE = -0.03

SHAPE_JOIN_COLUMNS = [
    "code",
    "trade_date",
    "state_family_id",
    "family_classification",
    "family_bad_pick_regime",
    "dominant_regime_context",
    "monthly_context",
    "monthly_context_no_lookahead",
    "weekly_context",
    "weekly_context_no_lookahead",
    "conditional_high_value",
    "candle_shape_modifier",
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "mfe_20d",
    "mae_20d",
    "path_value_score_v1",
    "family_sample_count",
    "family_unique_symbol_count",
    "family_month_count",
    "family_mean_forward_ret_5d",
    "family_mean_forward_ret_10d",
    "family_mean_forward_ret_20d",
    "family_median_forward_ret_20d",
    "family_mean_mfe_20d",
    "family_mean_mae_20d",
    "family_mean_path_value_score_v1",
    "family_median_path_value_score_v1",
    "family_plus5_before_minus5_rate",
    "family_minus5_before_plus5_rate",
    "family_top15_rate",
    "family_bottom15_rate",
    "family_positive_month_rate",
    "family_worst_month_mean_path_value",
    "family_best_month_mean_path_value",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_context_session(source_context_session: str | Path | None) -> Path:
    path = _safe_path(source_context_session, DEFAULT_CONTEXT_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"context session not found: {path}")
    return path


def _resolve_shape_session(source_shape_session: str | Path | None) -> Path:
    path = _safe_path(source_shape_session, DEFAULT_SHAPE_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"shape session not found: {path}")
    return path


def _resolve_source_family_session(source_family_session: str | Path | None) -> Path:
    path = _safe_path(source_family_session, DEFAULT_SOURCE_FAMILY_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"source family session not found: {path}")
    return path


def _resolve_prior_global_boost_session(prior_global_boost_session: str | Path | None) -> Path:
    path = _safe_path(prior_global_boost_session, DEFAULT_PRIOR_GLOBAL_BOOST_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"prior global boost session not found: {path}")
    return path


def _resolve_prior_context_gated_boost_session(prior_context_gated_boost_session: str | Path | None) -> Path:
    path = _safe_path(prior_context_gated_boost_session, DEFAULT_PRIOR_CONTEXT_GATED_BOOST_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"prior context-gated boost session not found: {path}")
    return path


def _resolve_candidate_input_dir(candidate_input_dir: str | Path | None) -> Path:
    return _resolve_candidate_input_dir_impl(candidate_input_dir)


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _load_context_session(context_session: Path) -> dict[str, Any]:
    manifest = _load_json(context_session / "run_manifest.json")
    summary = _load_json(context_session / "conditional_state_value_summary.json")
    classification = _load_json(context_session / "conditional_state_classification.json")
    comparison = _load_json(context_session / "global_vs_conditional_comparison.json")
    decision = _load_json(context_session / "multi_timeframe_conditional_state_value_v1_decision.json")
    context_definition = _load_json(context_session / "context_definition.json")
    row_parquet = context_session / "conditional_state_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "summary": summary,
        "classification": classification,
        "comparison": comparison,
        "decision": decision,
        "context_definition": context_definition,
        "row_parquet": row_parquet,
    }


def _load_shape_session(shape_session: Path) -> dict[str, Any]:
    manifest = _load_json(shape_session / "run_manifest.json")
    summary = _load_json(shape_session / "conditional_shape_value_summary.json")
    classification = _load_json(shape_session / "conditional_shape_modifier_classification.json")
    comparison = _load_json(shape_session / "shape_vs_base_slice_comparison.json")
    decision = _load_json(shape_session / "conditional_high_value_candle_shape_modifier_v1_decision.json")
    definition = _load_json(shape_session / "candle_shape_definition.json")
    row_parquet = shape_session / "conditional_shape_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "summary": summary,
        "classification": classification,
        "comparison": comparison,
        "decision": decision,
        "definition": definition,
        "row_parquet": row_parquet,
    }


def _load_prior_global_boost_session(prior_global_boost_session: Path) -> dict[str, Any]:
    compare = _load_json(prior_global_boost_session / "ma_state_family_high_value_boost_v1_compare.json")
    decision = _load_json(prior_global_boost_session / "ma_state_family_high_value_boost_v1_decision.json")
    coverage = _load_json(prior_global_boost_session / "boost_coverage_summary.json")
    return {"compare": compare, "decision": decision, "coverage": coverage}


def _load_prior_context_gated_boost_session(prior_context_gated_boost_session: Path) -> dict[str, Any]:
    compare = _load_json(prior_context_gated_boost_session / "multi_timeframe_context_gated_high_value_boost_v1_compare.json")
    decision = _load_json(prior_context_gated_boost_session / "multi_timeframe_context_gated_high_value_boost_v1_decision.json")
    coverage = _load_json(prior_context_gated_boost_session / "boost_coverage_summary.json")
    return {"compare": compare, "decision": decision, "coverage": coverage}


def _extract_source_thresholds(source_family_payloads: dict[str, Any]) -> tuple[float, float, dict[str, Any]]:
    manifest_thresholds = source_family_payloads["manifest"].get("thresholds") or {}
    summary_thresholds = source_family_payloads.get("family_summary_report", {}).get("filter_thresholds") or {}
    source_thresholds = manifest_thresholds or summary_thresholds
    top15_threshold = _safe_float(source_thresholds.get("top15_score_threshold"))
    bottom15_threshold = _safe_float(source_thresholds.get("bottom15_score_threshold"))
    if top15_threshold is None or bottom15_threshold is None:
        raise RuntimeError("missing bottom15/top15 thresholds from family filter session")
    return float(top15_threshold), float(bottom15_threshold), source_thresholds


def _build_triple_summary_and_gate(
    *,
    context_row_parquet: Path,
    thresholds: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    import duckdb

    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TEMP VIEW conditional_rows AS SELECT * FROM read_parquet('{context_row_parquet.as_posix()}')")
        triple_frame = conn.execute(_build_group_summary_sql(["monthly_context", "weekly_context", "state_family_id"])).fetchdf()
        monthly_context_frame = conn.execute(_build_group_summary_sql(["monthly_context"])).fetchdf()
        weekly_context_frame = conn.execute(_build_group_summary_sql(["weekly_context"])).fetchdf()
        monthly_weekly_frame = conn.execute(_build_group_summary_sql(["monthly_context", "weekly_context"])).fetchdf()
    finally:
        conn.close()

    for frame in (triple_frame, monthly_context_frame, weekly_context_frame, monthly_weekly_frame):
        frame["conditional_classification"] = frame.apply(_classify_conditional_group, axis=1, thresholds=thresholds)
        frame["sample_safe"] = (
            frame["sample_count"].fillna(0).astype(int) >= int(thresholds["min_sample_count"])
        ) & (
            frame["unique_symbol_count"].fillna(0).astype(int) >= int(thresholds["min_unique_symbol_count"])
        ) & (
            frame["month_count"].fillna(0).astype(int) >= int(thresholds["min_month_count"])
        )

    gate = triple_frame.loc[triple_frame["conditional_classification"] == "conditional_high_value"].copy()
    gate["conditional_high_value"] = True
    gate = gate[
        [
            "monthly_context",
            "weekly_context",
            "state_family_id",
            "sample_count",
            "unique_symbol_count",
            "month_count",
            "mean_forward_ret_5d",
            "mean_forward_ret_10d",
            "mean_forward_ret_20d",
            "median_forward_ret_20d",
            "mean_path_value_score_v1",
            "median_path_value_score_v1",
            "mean_mfe_20d",
            "mean_mae_20d",
            "plus5_before_minus5_rate",
            "minus5_before_plus5_rate",
            "top15_rate",
            "bottom15_rate",
            "positive_month_rate",
            "worst_month_mean_path_value",
            "best_month_mean_path_value",
            "regime_count",
            "regime_consistency_score",
            "score_spread",
            "dominant_regime_context",
            "conditional_high_value",
        ]
    ].copy()
    return triple_frame, monthly_context_frame, weekly_context_frame, gate


def _build_shape_lookup(shape_reference_frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, Any]], list[str], list[str]]:
    frame = shape_reference_frame.copy()
    if "candle_shape_modifier" not in frame.columns:
        raise RuntimeError("shape reference rows missing candle_shape_modifier")
    frame["candle_shape_modifier"] = frame["candle_shape_modifier"].astype(str)
    frame = frame.drop_duplicates(["candle_shape_modifier"], keep="first").reset_index(drop=True)
    frame["shape_classification"] = frame["shape_classification"].fillna("shape_sparse_or_unstable").astype(str)
    lookup = frame.set_index("candle_shape_modifier").to_dict(orient="index")
    positive_modifiers = sorted(frame.loc[frame["shape_classification"] == "shape_positive_modifier", "candle_shape_modifier"].astype(str).tolist())
    context_dependent_modifiers = sorted(frame.loc[frame["shape_classification"] == "shape_context_dependent", "candle_shape_modifier"].astype(str).tolist())
    return frame, lookup, positive_modifiers, context_dependent_modifiers


def _score_shape_adjustment(
    *,
    conditional_high_value: bool,
    candle_shape_modifier: str | None,
    shape_classification: str | None,
    delta_path_value_score_v1: float | None,
    positive_modifiers: set[str],
    context_dependent_modifiers: set[str],
    prune_enabled: bool,
) -> tuple[float, str, bool, bool]:
    shape = "" if candle_shape_modifier is None else str(candle_shape_modifier)
    cls = "" if shape_classification is None else str(shape_classification)
    if not conditional_high_value:
        return 0.0, "outside_conditional_high_value", False, False
    if shape in positive_modifiers or cls == "shape_positive_modifier":
        return float(CONDITIONAL_SHAPE_BOOST), "conditional_high_value:shape_positive_modifier", True, False
    if prune_enabled and (shape in context_dependent_modifiers or cls == "shape_context_dependent"):
        if delta_path_value_score_v1 is not None and delta_path_value_score_v1 < 0:
            return float(CONDITIONAL_SHAPE_PRUNE), "conditional_high_value:shape_context_dependent_negative_delta", False, True
    return 0.0, "conditional_high_value:no_adjustment", False, False


def _apply_shape_adjustments(
    frame: pd.DataFrame,
    *,
    shape_lookup: dict[str, dict[str, Any]],
    positive_modifiers: set[str],
    context_dependent_modifiers: set[str],
    prune_enabled: bool,
) -> pd.DataFrame:
    out = frame.copy()
    out["candle_shape_modifier"] = out["candle_shape_modifier"].fillna("unmatched").astype(str)
    shape_meta = out["candle_shape_modifier"].map(shape_lookup)

    def _pick(meta: dict[str, Any] | float | None, key: str) -> Any:
        if isinstance(meta, dict):
            return meta.get(key)
        return None

    out["shape_classification"] = shape_meta.map(lambda meta: _pick(meta, "shape_classification")).fillna("shape_sparse_or_unstable").astype(str)
    out["shape_vs_base_slice_delta_path_value_score_v1"] = shape_meta.map(lambda meta: _pick(meta, "delta_mean_path_value_score_v1"))
    out["shape_vs_base_slice_delta_forward_ret_20d"] = shape_meta.map(lambda meta: _pick(meta, "delta_mean_forward_ret_20d"))
    out["shape_vs_base_slice_delta_bottom15_rate"] = shape_meta.map(lambda meta: _pick(meta, "delta_bottom15_rate"))
    out["shape_vs_base_slice_delta_top15_rate"] = shape_meta.map(lambda meta: _pick(meta, "delta_top15_rate"))
    out["shape_sample_count"] = shape_meta.map(lambda meta: _pick(meta, "sample_count"))
    out["shape_unique_symbol_count"] = shape_meta.map(lambda meta: _pick(meta, "unique_symbol_count"))
    out["shape_month_count"] = shape_meta.map(lambda meta: _pick(meta, "month_count"))
    out["shape_mean_forward_ret_5d"] = shape_meta.map(lambda meta: _pick(meta, "mean_forward_ret_5d"))
    out["shape_mean_forward_ret_10d"] = shape_meta.map(lambda meta: _pick(meta, "mean_forward_ret_10d"))
    out["shape_mean_forward_ret_20d"] = shape_meta.map(lambda meta: _pick(meta, "mean_forward_ret_20d"))
    out["shape_median_forward_ret_20d"] = shape_meta.map(lambda meta: _pick(meta, "median_forward_ret_20d"))
    out["shape_mean_path_value_score_v1"] = shape_meta.map(lambda meta: _pick(meta, "mean_path_value_score_v1"))
    out["shape_median_path_value_score_v1"] = shape_meta.map(lambda meta: _pick(meta, "median_path_value_score_v1"))
    out["shape_mean_mfe_20d"] = shape_meta.map(lambda meta: _pick(meta, "mean_mfe_20d"))
    out["shape_mean_mae_20d"] = shape_meta.map(lambda meta: _pick(meta, "mean_mae_20d"))
    out["shape_plus5_before_minus5_rate"] = shape_meta.map(lambda meta: _pick(meta, "plus5_before_minus5_rate"))
    out["shape_minus5_before_plus5_rate"] = shape_meta.map(lambda meta: _pick(meta, "minus5_before_plus5_rate"))
    out["shape_top15_rate"] = shape_meta.map(lambda meta: _pick(meta, "top15_rate"))
    out["shape_bottom15_rate"] = shape_meta.map(lambda meta: _pick(meta, "bottom15_rate"))
    out["shape_positive_month_rate"] = shape_meta.map(lambda meta: _pick(meta, "positive_month_rate"))
    out["shape_worst_month_mean_path_value"] = shape_meta.map(lambda meta: _pick(meta, "worst_month_mean_path_value"))
    out["shape_best_month_mean_path_value"] = shape_meta.map(lambda meta: _pick(meta, "best_month_mean_path_value"))

    adjustments: list[float] = []
    reasons: list[str] = []
    applied_boost: list[bool] = []
    applied_prune: list[bool] = []
    for row in out.itertuples(index=False):
        adjustment, reason, boosted, pruned = _score_shape_adjustment(
            conditional_high_value=bool(getattr(row, "conditional_high_value", False)),
            candle_shape_modifier=getattr(row, "candle_shape_modifier", None),
            shape_classification=getattr(row, "shape_classification", None),
            delta_path_value_score_v1=_safe_float(getattr(row, "shape_vs_base_slice_delta_path_value_score_v1", None)),
            positive_modifiers=positive_modifiers,
            context_dependent_modifiers=context_dependent_modifiers,
            prune_enabled=prune_enabled,
        )
        adjustments.append(float(adjustment))
        reasons.append(str(reason))
        applied_boost.append(bool(boosted))
        applied_prune.append(bool(pruned))

    out["score_adjustment"] = pd.Series(adjustments, index=out.index, dtype="float64")
    out["shape_adjustment_reason"] = reasons
    out["shape_boost_applied"] = applied_boost
    out["shape_prune_applied"] = applied_prune
    out["challenger_score"] = pd.to_numeric(out["score"], errors="coerce").fillna(-1e9) + out["score_adjustment"]
    out["conditional_high_value"] = out["conditional_high_value"].fillna(False).astype(bool)
    return out


def _build_group_comparison(
    frame: pd.DataFrame,
    *,
    group_col: str,
    selected_suffix: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    champ_col = f"champion_selected_top{selected_suffix}"
    chal_col = f"challenger_selected_top{selected_suffix}"
    rows = []
    values = sorted(frame[group_col].fillna("unknown").astype(str).unique().tolist())
    for value in values:
        sub = frame.loc[frame[group_col].fillna("unknown").astype(str) == value].copy()
        champ = _aggregate_selected_rows(sub, selected_col=champ_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        chal = _aggregate_selected_rows(sub, selected_col=chal_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        rows.append(
            {
                group_col: value,
                "candidate_rows": int(len(sub)),
                "adjusted_rows": int(sub["score_adjustment"].ne(0).sum()),
                "champion": champ,
                "challenger": chal,
                "delta": {
                    "mean_forward_ret_20d": None
                    if champ["mean_forward_ret_20d"] is None or chal["mean_forward_ret_20d"] is None
                    else float(chal["mean_forward_ret_20d"] - champ["mean_forward_ret_20d"]),
                    "mean_path_value_score_v1": None
                    if champ["mean_path_value_score_v1"] is None or chal["mean_path_value_score_v1"] is None
                    else float(chal["mean_path_value_score_v1"] - champ["mean_path_value_score_v1"]),
                    "bottom15_contamination_rate": None
                    if champ["bottom15_contamination_rate"] is None or chal["bottom15_contamination_rate"] is None
                    else float(chal["bottom15_contamination_rate"] - champ["bottom15_contamination_rate"]),
                    "top15_capture_rate": None
                    if champ["top15_capture_rate"] is None or chal["top15_capture_rate"] is None
                    else float(chal["top15_capture_rate"] - champ["top15_capture_rate"]),
                },
            }
        )
    return {"group_col": group_col, "rows": rows}


def _build_shape_comparison(
    frame: pd.DataFrame,
    *,
    selected_suffix: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    champ_col = f"champion_selected_top{selected_suffix}"
    chal_col = f"challenger_selected_top{selected_suffix}"
    rows = []
    for shape in sorted(frame["candle_shape_modifier"].fillna("unmatched").astype(str).unique().tolist()):
        sub = frame.loc[frame["candle_shape_modifier"].fillna("unmatched").astype(str) == shape].copy()
        champ = _aggregate_selected_rows(sub, selected_col=champ_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        chal = _aggregate_selected_rows(sub, selected_col=chal_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        rows.append(
            {
                "candle_shape_modifier": shape,
                "shape_classification": str(sub["shape_classification"].dropna().iloc[0]) if sub["shape_classification"].notna().any() else "shape_sparse_or_unstable",
                "candidate_rows": int(len(sub)),
                "adjusted_rows": int(sub["score_adjustment"].ne(0).sum()),
                "boost_rows": int(sub["shape_boost_applied"].fillna(False).astype(bool).sum()),
                "prune_rows": int(sub["shape_prune_applied"].fillna(False).astype(bool).sum()),
                "changed_members_count": int((sub[champ_col].fillna(False).astype(bool) != sub[chal_col].fillna(False).astype(bool)).sum()),
                "shape_vs_base_slice_delta_mean_forward_ret_20d": _safe_float(sub["shape_vs_base_slice_delta_forward_ret_20d"].dropna().mean()),
                "shape_vs_base_slice_delta_mean_path_value_score_v1": _safe_float(sub["shape_vs_base_slice_delta_path_value_score_v1"].dropna().mean()),
                "shape_vs_base_slice_delta_bottom15_rate": _safe_float(sub["shape_vs_base_slice_delta_bottom15_rate"].dropna().mean()),
                "shape_vs_base_slice_delta_top15_rate": _safe_float(sub["shape_vs_base_slice_delta_top15_rate"].dropna().mean()),
                "champion": champ,
                "challenger": chal,
                "delta": {
                    "mean_forward_ret_20d": None
                    if champ["mean_forward_ret_20d"] is None or chal["mean_forward_ret_20d"] is None
                    else float(chal["mean_forward_ret_20d"] - champ["mean_forward_ret_20d"]),
                    "median_forward_ret_20d": None
                    if champ["median_forward_ret_20d"] is None or chal["median_forward_ret_20d"] is None
                    else float(chal["median_forward_ret_20d"] - champ["median_forward_ret_20d"]),
                    "mean_path_value_score_v1": None
                    if champ["mean_path_value_score_v1"] is None or chal["mean_path_value_score_v1"] is None
                    else float(chal["mean_path_value_score_v1"] - champ["mean_path_value_score_v1"]),
                    "median_path_value_score_v1": None
                    if champ["median_path_value_score_v1"] is None or chal["median_path_value_score_v1"] is None
                    else float(chal["median_path_value_score_v1"] - champ["median_path_value_score_v1"]),
                    "bottom15_contamination_rate": None
                    if champ["bottom15_contamination_rate"] is None or chal["bottom15_contamination_rate"] is None
                    else float(chal["bottom15_contamination_rate"] - champ["bottom15_contamination_rate"]),
                },
            }
        )
    return {"rows": rows}


def _build_prior_boost_delta(
    compare_payload: dict[str, Any],
    prior_global_compare: dict[str, Any],
    prior_context_compare: dict[str, Any],
) -> dict[str, Any]:
    def _topk_delta(current: dict[str, Any], prior: dict[str, Any], top_k: str) -> dict[str, Any]:
        current_sel = current["champion_vs_challenger"]["selection_only"][top_k]
        prior_sel = prior["champion_vs_challenger"]["selection_only"][top_k]
        current_branch = current["champion_vs_challenger"]["branching_metrics"]
        prior_branch = prior["champion_vs_challenger"]["branching_metrics"]
        delta = {
            "mean_forward_ret_20d": None,
            "median_forward_ret_20d": None,
            "mean_path_value_score_v1": None,
            "median_path_value_score_v1": None,
            "top15_capture_rate": None,
            "bottom15_contamination_rate": None,
            "bad_pick_family_contamination_rate": None,
            "changed_top5_members_count_delta": None,
            "changed_top10_members_count_delta": None,
            "changed_top20_members_count_delta": None,
            "changed_rank_count_delta": None,
            "top5_overlap_ratio_delta": None,
            "top10_overlap_ratio_delta": None,
            "top20_overlap_ratio_delta": None,
            "turnover_proxy_delta": None,
        }
        for key in ("mean_forward_ret_20d", "median_forward_ret_20d", "mean_path_value_score_v1", "median_path_value_score_v1", "top15_capture_rate", "bottom15_contamination_rate", "bad_pick_family_contamination_rate"):
            delta[key] = None if current_sel["delta"][key] is None or prior_sel["delta"][key] is None else float(current_sel["delta"][key] - prior_sel["delta"][key])
        for key in ("changed_top5_members_count", "changed_top10_members_count", "changed_top20_members_count", "changed_rank_count", "top5_overlap_ratio", "top10_overlap_ratio", "top20_overlap_ratio", "turnover_proxy"):
            delta[f"{key}_delta"] = None if current_branch.get(key) is None or prior_branch.get(key) is None else float(current_branch[key] - prior_branch[key])
        return delta

    return {
        "schema_version": PRIOR_BOOST_DELTA_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "against_prior_global_boost": {
            top_k: _topk_delta(compare_payload, prior_global_compare, top_k) for top_k in ("5", "10", "20")
        },
        "against_prior_context_gated_boost": {
            top_k: _topk_delta(compare_payload, prior_context_compare, top_k) for top_k in ("5", "10", "20")
        },
    }


def _decision_from_compare(
    compare_payload: dict[str, Any],
    *,
    positive_modifiers: list[str],
    context_dependent_prune_modifiers: list[str],
    shape_join_rate: float,
    prior_context_compare: dict[str, Any],
    shape_delta_join_available: bool,
) -> tuple[str, str]:
    top5 = compare_payload["champion_vs_challenger"]["selection_only"]["5"]
    top10 = compare_payload["champion_vs_challenger"]["selection_only"]["10"]
    top20 = compare_payload["champion_vs_challenger"]["selection_only"]["20"]

    top5_path_delta = _safe_float(top5["delta"]["mean_path_value_score_v1"])
    top10_path_delta = _safe_float(top10["delta"]["mean_path_value_score_v1"])
    top5_ret_delta = _safe_float(top5["delta"]["mean_forward_ret_20d"])
    top10_ret_delta = _safe_float(top10["delta"]["mean_forward_ret_20d"])
    top5_bottom_delta = _safe_float(top5["delta"]["bottom15_contamination_rate"])
    top10_bottom_delta = _safe_float(top10["delta"]["bottom15_contamination_rate"])

    prior_top5_path = _safe_float(prior_context_compare["champion_vs_challenger"]["selection_only"]["5"]["selection_only"]["challenger"]["mean_path_value_score_v1"])
    prior_top10_path = _safe_float(prior_context_compare["champion_vs_challenger"]["selection_only"]["10"]["selection_only"]["challenger"]["mean_path_value_score_v1"])
    prior_top5_ret = _safe_float(prior_context_compare["champion_vs_challenger"]["selection_only"]["5"]["selection_only"]["challenger"]["mean_forward_ret_20d"])
    prior_top10_ret = _safe_float(prior_context_compare["champion_vs_challenger"]["selection_only"]["10"]["selection_only"]["challenger"]["mean_forward_ret_20d"])

    top5_overlap = _safe_float(compare_payload["champion_vs_challenger"]["branching_metrics"]["top5_overlap_ratio"])
    top10_overlap = _safe_float(compare_payload["champion_vs_challenger"]["branching_metrics"]["top10_overlap_ratio"])
    changed_top5 = _safe_int(compare_payload["champion_vs_challenger"]["branching_metrics"]["changed_top5_members_count"])
    changed_top10 = _safe_int(compare_payload["champion_vs_challenger"]["branching_metrics"]["changed_top10_members_count"])

    keep_conditions = [
        top5_path_delta is not None and top5_path_delta > 0,
        top10_path_delta is not None and top10_path_delta > 0,
        top5_ret_delta is not None and top5_ret_delta >= 0,
        top10_ret_delta is not None and top10_ret_delta >= 0,
        top5_bottom_delta is not None and top5_bottom_delta <= 0.0,
        top10_bottom_delta is not None and top10_bottom_delta <= 0.0,
        prior_top5_path is not None and top5_path_delta is not None and top5_path_delta >= (prior_top5_path - _safe_float(prior_context_compare["champion_vs_challenger"]["selection_only"]["5"]["selection_only"]["champion"]["mean_path_value_score_v1"])),
        prior_top10_path is not None and top10_path_delta is not None and top10_path_delta >= (prior_top10_path - _safe_float(prior_context_compare["champion_vs_challenger"]["selection_only"]["10"]["selection_only"]["champion"]["mean_path_value_score_v1"])),
        shape_join_rate >= 0.95,
        len(positive_modifiers) >= 1,
        len(context_dependent_prune_modifiers) >= 0,
        shape_delta_join_available,
        changed_top5 is not None and changed_top5 >= 10,
        changed_top10 is not None and changed_top10 >= 20,
        top5_overlap is not None and top5_overlap < 0.99,
        top10_overlap is not None and top10_overlap < 0.995,
    ]
    if all(keep_conditions):
        return "keep", "shape_gated_contextual_selection_improves_topk_without_material_path_drag"

    hold_conditions = [
        (top10_path_delta is not None and top10_path_delta > 0),
        (top5_path_delta is not None and top5_path_delta <= 0),
        shape_join_rate >= 0.95,
        shape_delta_join_available,
    ]
    if any(hold_conditions):
        return "hold", "shape_signal_exists_but_top5_or_bottom15_remains_mixed"

    return "drop", "shape_gated_adjustment_does_not_improve_topk_quality"


def run_context_gated_candle_shape_modifier_boost_prune_v1(
    *,
    source_context_session: str | Path | None = None,
    source_shape_session: str | Path | None = None,
    source_family_session: str | Path | None = None,
    prior_global_boost_session: str | Path | None = None,
    prior_context_gated_boost_session: str | Path | None = None,
    candidate_input_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = DEFAULT_LIMIT_ANCHOR_DATES,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    context_session_path = _resolve_context_session(source_context_session)
    shape_session_path = _resolve_shape_session(source_shape_session)
    source_family_session_path = _resolve_source_family_session(source_family_session)
    prior_global_boost_session_path = _resolve_prior_global_boost_session(prior_global_boost_session)
    prior_context_gated_boost_session_path = _resolve_prior_context_gated_boost_session(prior_context_gated_boost_session)
    candidate_input_path = _resolve_candidate_input_dir(candidate_input_dir)
    output_root_path = _resolve_output_root(output_root)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(
        f"start context_session={context_session_path} shape_session={shape_session_path} family_session={source_family_session_path} "
        f"global_boost={prior_global_boost_session_path} context_gated_boost={prior_context_gated_boost_session_path} "
        f"candidate_input={candidate_input_path} out_root={output_root_path} session={session_id}"
    )

    context_payloads = _load_context_session(context_session_path)
    shape_payloads = _load_shape_session(shape_session_path)
    source_family_payloads = _load_source_family_session(source_family_session_path)
    prior_global_boost_payloads = _load_prior_global_boost_session(prior_global_boost_session_path)
    prior_context_gated_boost_payloads = _load_prior_context_gated_boost_session(prior_context_gated_boost_session_path)

    top15_threshold, bottom15_threshold, source_thresholds = _extract_source_thresholds(source_family_payloads)
    thresholds = {
        "baseline_mean_path_value_score_v1": _safe_float(context_payloads["summary"]["baseline_metrics"]["mean_path_value_score_v1"]),
        "baseline_median_path_value_score_v1": _safe_float(context_payloads["summary"]["baseline_metrics"]["median_path_value_score_v1"]),
        "baseline_plus5_before_minus5_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["plus5_before_minus5_rate"]),
        "baseline_minus5_before_plus5_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["minus5_before_plus5_rate"]),
        "baseline_bottom15_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["bottom15_rate"]),
        "baseline_top15_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["top15_rate"]),
        "min_sample_count": 100,
        "min_unique_symbol_count": 20,
        "min_month_count": 8,
    }

    triple_frame, monthly_context_frame, weekly_context_frame, high_value_gate = _build_triple_summary_and_gate(
        context_row_parquet=context_payloads["row_parquet"],
        thresholds=thresholds,
    )
    triple_class_counts = _count_classes(triple_frame)
    monthly_context_class_counts = _count_classes(monthly_context_frame)
    weekly_context_class_counts = _count_classes(weekly_context_frame)
    authoritative_triple_high_value = int(context_payloads["classification"]["triple_level"]["class_counts"]["conditional_high_value"])
    if triple_class_counts.get("conditional_high_value", 0) != authoritative_triple_high_value:
        raise RuntimeError("triple-level conditional_high_value count does not match authoritative context artifact")

    shape_classification_frame = pd.DataFrame(shape_payloads["classification"]["rows"]).copy()
    shape_reference_frame = pd.DataFrame(shape_payloads["comparison"]["shape_vs_base_slice_rows"]).copy()
    if len(shape_classification_frame) != len(shape_reference_frame):
        raise RuntimeError("shape classification and shape-vs-base comparison row counts differ")
    shape_reference_frame = shape_reference_frame.rename(columns={"shape_classification": "shape_classification_ref"})
    shape_reference_frame["candle_shape_modifier"] = shape_reference_frame["candle_shape_modifier"].astype(str)
    shape_classification_frame["candle_shape_modifier"] = shape_classification_frame["candle_shape_modifier"].astype(str)
    shape_reference_frame = shape_reference_frame.merge(
        shape_classification_frame[["candle_shape_modifier", "shape_classification"]],
        on="candle_shape_modifier",
        how="left",
        validate="1:1",
    )
    shape_reference_frame["shape_classification"] = shape_reference_frame["shape_classification"].fillna(shape_reference_frame["shape_classification_ref"]).fillna("shape_sparse_or_unstable")
    shape_reference_frame = shape_reference_frame.drop(columns=["shape_classification_ref"])
    shape_reference_frame, shape_lookup, positive_modifiers, context_dependent_modifiers = _build_shape_lookup(shape_reference_frame)
    positive_modifiers_set = set(positive_modifiers)
    context_dependent_modifiers_set = set(context_dependent_modifiers)
    shape_delta_join_available = shape_reference_frame["delta_mean_path_value_score_v1"].notna().any()

    candidate_rows = _apply_anchor_limit(_load_candidate_rows(candidate_input_path), limit_anchor_dates)
    if candidate_rows.empty:
        raise RuntimeError("no candidate rows produced")

    shape_row_frame = pd.read_parquet(shape_payloads["row_parquet"], columns=SHAPE_JOIN_COLUMNS).copy()
    shape_row_frame = shape_row_frame.rename(columns={"code": "symbol"})
    shape_row_frame["symbol"] = shape_row_frame["symbol"].astype(str)
    shape_row_frame["trade_date"] = pd.to_numeric(shape_row_frame["trade_date"], errors="coerce").astype("Int64")
    shape_row_frame = shape_row_frame.drop_duplicates(["symbol", "trade_date"], keep="first").copy()

    frame = candidate_rows.copy()
    frame["symbol"] = frame["symbol"].astype(str)
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="coerce").astype("Int64")
    frame = frame.merge(shape_row_frame, on=["symbol", "trade_date"], how="left", suffixes=("", "_shape"), validate="m:1")
    frame = frame.merge(
        shape_reference_frame,
        on="candle_shape_modifier",
        how="left",
        suffixes=("", "_shape_ref"),
        validate="m:1",
    )
    frame["conditional_high_value"] = frame["conditional_high_value"].fillna(False).astype(bool)
    frame["monthly_context_no_lookahead"] = frame["monthly_context_no_lookahead"].fillna(False).astype(bool)
    frame["weekly_context_no_lookahead"] = frame["weekly_context_no_lookahead"].fillna(False).astype(bool)
    frame["shape_joined"] = frame["state_family_id"].notna() & frame["candle_shape_modifier"].notna()
    frame = _apply_shape_adjustments(
        frame,
        shape_lookup=shape_lookup,
        positive_modifiers=positive_modifiers_set,
        context_dependent_modifiers=context_dependent_modifiers_set,
        prune_enabled=shape_delta_join_available,
    )

    row_count = int(len(frame))
    candidate_count = int(frame["candidate_idx"].nunique())
    context_match_rate = float(frame["state_family_id"].notna().mean())
    shape_join_rate = float(frame["conditional_high_value"].fillna(False).astype(bool).mean())
    shape_match_rate = float(frame["shape_joined"].fillna(False).astype(bool).mean())
    monthly_no_lookahead_rate = float(frame["monthly_context_no_lookahead"].fillna(False).astype(bool).mean())
    weekly_no_lookahead_rate = float(frame["weekly_context_no_lookahead"].fillna(False).astype(bool).mean())

    frame = _rank_selection(frame, score_col="score", prefix="champion")
    frame = _rank_selection(frame, score_col="challenger_score", prefix="challenger")

    compare_payload: dict[str, Any] = {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_shape_session_id": shape_payloads["manifest"]["session_id"],
        "source_shape_session_path": str(shape_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_global_boost_session_path": str(prior_global_boost_session_path),
        "prior_context_gated_boost_session_id": prior_context_gated_boost_session_path.name,
        "prior_context_gated_boost_session_path": str(prior_context_gated_boost_session_path),
        "same_condition_contract": {
            "universe": "integrated_guarded_v1_candidate_snapshots",
            "period": "matched candidate anchor dates from the verified stress200 compare path",
            "top_k_values": [5, 10, 20],
            "cost_slippage": "inherited from source compare path; not changed in this branch",
            "artifact_detail_level": "summary json + parquet diff rows",
            "no_silent_fallback": True,
        },
        "boost_formula": {
            "conditional_high_value_gate": True,
            "conditional_shape_boost": CONDITIONAL_SHAPE_BOOST,
            "conditional_shape_prune": CONDITIONAL_SHAPE_PRUNE,
            "shape_delta_join_available": bool(shape_delta_join_available),
            "positive_shape_modifiers": positive_modifiers,
            "context_dependent_prune_modifiers": context_dependent_modifiers if shape_delta_join_available else [],
            "note": "boost only inside conditional_high_value; prune only for context-dependent modifiers with negative base-slice path delta",
        },
        "candidate_universe": {
            "row_count": int(len(frame)),
            "anchor_count": int(frame["anchor_date"].nunique()),
            "side_count": int(frame["side"].nunique()),
            "monthly_context_count": int(frame["monthly_context"].fillna("monthly_unknown").astype(str).nunique()),
            "weekly_context_count": int(frame["weekly_context"].fillna("weekly_unknown").astype(str).nunique()),
            "shape_join_rate": shape_match_rate,
        },
        "boost_coverage": {},
        "shape_summary": {
            "conditional_high_value_gate_count": int(authoritative_triple_high_value),
            "shape_bucket_count": int(shape_payloads["summary"]["shape_bucket_count"]),
            "shape_class_counts": shape_payloads["classification"]["shape_class_counts"],
        },
        "source_thresholds": {
            "top15_score_threshold": top15_threshold,
            "bottom15_score_threshold": bottom15_threshold,
            "source_threshold_keys": sorted(map(str, source_thresholds.keys())),
        },
    }

    compare_by_topk: dict[str, Any] = {}
    for top_k in (5, 10, 20):
        champ_col = f"champion_selected_top{top_k}"
        chal_col = f"challenger_selected_top{top_k}"
        champ_metric = _aggregate_selected_rows(frame, selected_col=champ_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        chal_metric = _aggregate_selected_rows(frame, selected_col=chal_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        compare_by_topk[str(top_k)] = {
            "selection_only": {
                "champion": champ_metric,
                "challenger": chal_metric,
            },
            "delta": {
                "mean_forward_ret_20d": None
                if champ_metric["mean_forward_ret_20d"] is None or chal_metric["mean_forward_ret_20d"] is None
                else float(chal_metric["mean_forward_ret_20d"] - champ_metric["mean_forward_ret_20d"]),
                "median_forward_ret_20d": None
                if champ_metric["median_forward_ret_20d"] is None or chal_metric["median_forward_ret_20d"] is None
                else float(chal_metric["median_forward_ret_20d"] - champ_metric["median_forward_ret_20d"]),
                "mean_path_value_score_v1": None
                if champ_metric["mean_path_value_score_v1"] is None or chal_metric["mean_path_value_score_v1"] is None
                else float(chal_metric["mean_path_value_score_v1"] - champ_metric["mean_path_value_score_v1"]),
                "median_path_value_score_v1": None
                if champ_metric["median_path_value_score_v1"] is None or chal_metric["median_path_value_score_v1"] is None
                else float(chal_metric["median_path_value_score_v1"] - champ_metric["median_path_value_score_v1"]),
                "top15_capture_rate": None
                if champ_metric["top15_capture_rate"] is None or chal_metric["top15_capture_rate"] is None
                else float(chal_metric["top15_capture_rate"] - champ_metric["top15_capture_rate"]),
                "bottom15_contamination_rate": None
                if champ_metric["bottom15_contamination_rate"] is None or chal_metric["bottom15_contamination_rate"] is None
                else float(chal_metric["bottom15_contamination_rate"] - champ_metric["bottom15_contamination_rate"]),
                "bad_pick_family_contamination_rate": None
                if champ_metric["bad_pick_family_contamination_rate"] is None or chal_metric["bad_pick_family_contamination_rate"] is None
                else float(chal_metric["bad_pick_family_contamination_rate"] - champ_metric["bad_pick_family_contamination_rate"]),
                "regime_bad_pick_contamination_rate": None
                if champ_metric["regime_bad_pick_contamination_rate"] is None or chal_metric["regime_bad_pick_contamination_rate"] is None
                else float(chal_metric["regime_bad_pick_contamination_rate"] - champ_metric["regime_bad_pick_contamination_rate"]),
                "win_rate": None
                if champ_metric["win_rate"] is None or chal_metric["win_rate"] is None
                else float(chal_metric["win_rate"] - champ_metric["win_rate"]),
            },
        }

    champion_top5_keys = _selection_diff_keys(frame, selected_col="champion_selected_top5")
    challenger_top5_keys = _selection_diff_keys(frame, selected_col="challenger_selected_top5")
    champion_top10_keys = _selection_diff_keys(frame, selected_col="champion_selected_top10")
    challenger_top10_keys = _selection_diff_keys(frame, selected_col="challenger_selected_top10")
    champion_top20_keys = _selection_diff_keys(frame, selected_col="champion_selected_top20")
    challenger_top20_keys = _selection_diff_keys(frame, selected_col="challenger_selected_top20")
    intersection_top5 = len(champion_top5_keys & challenger_top5_keys)
    intersection_top10 = len(champion_top10_keys & challenger_top10_keys)
    intersection_top20 = len(champion_top20_keys & challenger_top20_keys)
    union_top5 = len(champion_top5_keys | challenger_top5_keys)
    union_top10 = len(champion_top10_keys | challenger_top10_keys)
    union_top20 = len(champion_top20_keys | challenger_top20_keys)
    changed_top5_members_count = len(champion_top5_keys ^ challenger_top5_keys)
    changed_top10_members_count = len(champion_top10_keys ^ challenger_top10_keys)
    changed_top20_members_count = len(champion_top20_keys ^ challenger_top20_keys)
    rank_changes = frame.loc[
        frame["champion_selected_top20"].fillna(False).astype(bool) & frame["challenger_selected_top20"].fillna(False).astype(bool),
        ["champion_position", "challenger_position"],
    ].copy()
    changed_rank_count = 0 if rank_changes.empty else int((pd.to_numeric(rank_changes["champion_position"], errors="coerce").astype("Int64") != pd.to_numeric(rank_changes["challenger_position"], errors="coerce").astype("Int64")).sum())

    compare_payload["champion_vs_challenger"] = {
        "selection_only": compare_by_topk,
        "branching_metrics": {
            "top5_overlap_ratio": None if union_top5 == 0 else float(intersection_top5 / union_top5),
            "top10_overlap_ratio": None if union_top10 == 0 else float(intersection_top10 / union_top10),
            "top20_overlap_ratio": None if union_top20 == 0 else float(intersection_top20 / union_top20),
            "changed_top5_members_count": changed_top5_members_count,
            "changed_top10_members_count": changed_top10_members_count,
            "changed_top20_members_count": changed_top20_members_count,
            "changed_rank_count": changed_rank_count,
            "selection_divergence_reason": "context_gated_candle_shape_modifier_boost_prune_v1_vs_champion",
            "turnover_proxy": None if union_top20 == 0 else float((changed_top10_members_count + changed_top20_members_count) / max(1, union_top10 + union_top20)),
        },
    }

    monthly_context_comparison = {
        "schema_version": MONTHLY_CONTEXT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "rows": {
            str(top_k): _build_group_comparison(
                frame,
                group_col="monthly_context",
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in (5, 10, 20)
        },
    }
    weekly_context_comparison = {
        "schema_version": WEEKLY_CONTEXT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "rows": {
            str(top_k): _build_group_comparison(
                frame,
                group_col="weekly_context",
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in (5, 10, 20)
        },
    }
    shape_comparison = {
        "schema_version": SHAPE_COMPARISON_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "rows": {
            str(top_k): _build_shape_comparison(
                frame,
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in (5, 10, 20)
        },
        "shape_reference_rows": _json_ready(shape_reference_frame.to_dict(orient="records")),
    }

    coverage_payload = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_shape_session_id": shape_payloads["manifest"]["session_id"],
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "candidate_rows": int(len(frame)),
        "matched_shape_rows": int(frame["shape_joined"].fillna(False).astype(bool).sum()),
        "unmatched_shape_rows": int(len(frame) - int(frame["shape_joined"].fillna(False).astype(bool).sum())),
        "matched_shape_rate": None if len(frame) == 0 else float(frame["shape_joined"].fillna(False).astype(bool).mean()),
        "conditional_high_value_gate_count": int(authoritative_triple_high_value),
        "source_conditional_high_value_row_count": int(shape_payloads["summary"]["conditional_high_value_row_count"]),
        "boost_applied_rows": int(frame["shape_boost_applied"].fillna(False).astype(bool).sum()),
        "prune_applied_rows": int(frame["shape_prune_applied"].fillna(False).astype(bool).sum()),
        "adjusted_rows": int(frame["score_adjustment"].fillna(0.0).ne(0).sum()),
        "adjusted_rate": None if len(frame) == 0 else float(frame["score_adjustment"].fillna(0.0).ne(0).mean()),
        "shape_delta_join_available": bool(shape_delta_join_available),
        "shape_positive_modifier_count": int((shape_reference_frame["shape_classification"] == "shape_positive_modifier").sum()),
        "shape_context_dependent_count": int((shape_reference_frame["shape_classification"] == "shape_context_dependent").sum()),
        "shape_negative_modifier_count": int((shape_reference_frame["shape_classification"] == "shape_negative_modifier").sum()),
        "shape_sparse_or_unstable_count": int((shape_reference_frame["shape_classification"] == "shape_sparse_or_unstable").sum()),
        "shape_neutral_count": int((shape_reference_frame["shape_classification"] == "shape_neutral").sum()),
        "candidate_rows_by_shape_classification": {
            str(label): int((frame["shape_classification"] == label).sum())
            for label in sorted(shape_reference_frame["shape_classification"].dropna().astype(str).unique().tolist())
        },
        "adjusted_rows_by_shape_classification": {
            str(label): int(((frame["shape_classification"] == label) & frame["score_adjustment"].fillna(0.0).ne(0)).sum())
            for label in sorted(shape_reference_frame["shape_classification"].dropna().astype(str).unique().tolist())
        },
        "candidate_rows_by_shape_modifier": {
            str(mod): int((frame["candle_shape_modifier"].fillna("unmatched").astype(str) == mod).sum())
            for mod in sorted(frame["candle_shape_modifier"].fillna("unmatched").astype(str).unique().tolist())
        },
        "adjusted_rows_by_shape_modifier": {
            str(mod): int(((frame["candle_shape_modifier"].fillna("unmatched").astype(str) == mod) & frame["score_adjustment"].fillna(0.0).ne(0)).sum())
            for mod in sorted(frame["candle_shape_modifier"].fillna("unmatched").astype(str).unique().tolist())
        },
        "monthly_context_no_lookahead_rate": monthly_no_lookahead_rate,
        "weekly_context_no_lookahead_rate": weekly_no_lookahead_rate,
        "positive_shape_modifiers": positive_modifiers,
        "context_dependent_prune_modifiers": context_dependent_modifiers if shape_delta_join_available else [],
        "shape_input_fields": {
            "confirmed": [
                "candle_body_ratio",
                "candle_upper_wick_ratio",
                "candle_lower_wick_ratio",
                "candle_triplet_up_prob",
                "candle_triplet_down_prob",
                "gap_pct",
                "vol_ratio5_20",
                "o",
                "h",
                "l",
                "c",
                "prev_o",
                "prev_h",
                "prev_l",
                "prev_c",
            ],
            "provisional": [
                "inside_bar",
                "bull_engulfing",
                "bear_engulfing",
                "gap_up_bull",
                "gap_down_bear",
                "koma_then_bull",
                "koma_then_bear",
                "lower_wick_then_bull",
                "upper_wick_then_bear",
                "bull_large",
                "bear_large",
                "doji_like",
                "neutral_small",
                "lower_wick_dominant",
                "upper_wick_dominant",
                "no_clear_shape",
            ],
            "source": "conditional_high_value_candle_shape_modifier_v1 keep-grade analysis session",
        },
    }

    prior_boost_delta_payload = _build_prior_boost_delta(
        compare_payload,
        prior_global_boost_payloads["compare"],
        prior_context_gated_boost_payloads["compare"],
    )

    decision_recommendation, decision_reason = _decision_from_compare(
        compare_payload,
        positive_modifiers=positive_modifiers,
        context_dependent_prune_modifiers=context_dependent_modifiers,
        shape_join_rate=shape_match_rate,
        prior_context_compare=prior_context_gated_boost_payloads["compare"],
        shape_delta_join_available=shape_delta_join_available,
    )
    decision_payload = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_shape_session_id": shape_payloads["manifest"]["session_id"],
        "source_shape_session_path": str(shape_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_global_boost_session_path": str(prior_global_boost_session_path),
        "prior_context_gated_boost_session_id": prior_context_gated_boost_session_path.name,
        "prior_context_gated_boost_session_path": str(prior_context_gated_boost_session_path),
        "baseline_metrics": context_payloads["summary"]["baseline_metrics"],
        "global_family_counts": context_payloads["decision"]["global_family_counts"],
        "triple_level_class_counts": triple_class_counts,
        "monthly_context_level_class_counts": monthly_context_class_counts,
        "weekly_context_level_class_counts": weekly_context_class_counts,
        "boost_coverage": coverage_payload,
        "prior_boost_delta": prior_boost_delta_payload,
        "recommendation": decision_recommendation,
        "typed_reasons": [decision_reason],
        "no_lookahead_inherited": bool(context_payloads["decision"].get("no_lookahead_inherited", True)),
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
    }

    output_files = {
        "run_manifest_json": session_tmp / "run_manifest.json",
        "context_gated_candle_shape_modifier_boost_prune_v1_compare_json": session_tmp / "context_gated_candle_shape_modifier_boost_prune_v1_compare.json",
        "context_gated_candle_shape_modifier_boost_prune_v1_decision_json": session_tmp / "context_gated_candle_shape_modifier_boost_prune_v1_decision.json",
        "shape_adjustment_coverage_summary_json": session_tmp / "shape_adjustment_coverage_summary.json",
        "prior_boost_delta_json": session_tmp / "prior_boost_delta.json",
        "shape_comparison_json": session_tmp / "shape_comparison.json",
        "monthly_context_comparison_json": session_tmp / "monthly_context_comparison.json",
        "weekly_context_comparison_json": session_tmp / "weekly_context_comparison.json",
        "topk_membership_diff_parquet": session_tmp / "topk_membership_diff.parquet",
        "_artifact_complete_json": session_tmp / "_ARTIFACT_COMPLETE.json",
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_shape_session_id": shape_payloads["manifest"]["session_id"],
        "source_shape_session_path": str(shape_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_global_boost_session_path": str(prior_global_boost_session_path),
        "prior_context_gated_boost_session_id": prior_context_gated_boost_session_path.name,
        "prior_context_gated_boost_session_path": str(prior_context_gated_boost_session_path),
        "candidate_input_dir": str(candidate_input_path),
        "output_root": str(output_root_path),
        "source_artifacts": {
            "context_run_manifest_json": str(context_session_path / "run_manifest.json"),
            "context_decision_json": str(context_session_path / "multi_timeframe_conditional_state_value_v1_decision.json"),
            "context_summary_json": str(context_session_path / "conditional_state_value_summary.json"),
            "context_classification_json": str(context_session_path / "conditional_state_classification.json"),
            "shape_run_manifest_json": str(shape_session_path / "run_manifest.json"),
            "shape_decision_json": str(shape_session_path / "conditional_high_value_candle_shape_modifier_v1_decision.json"),
            "shape_summary_json": str(shape_session_path / "conditional_shape_value_summary.json"),
            "shape_classification_json": str(shape_session_path / "conditional_shape_modifier_classification.json"),
            "shape_comparison_json": str(shape_session_path / "shape_vs_base_slice_comparison.json"),
            "shape_rows_parquet": str(shape_payloads["row_parquet"]),
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "prior_global_boost_compare_json": str(prior_global_boost_session_path / "ma_state_family_high_value_boost_v1_compare.json"),
            "prior_global_boost_decision_json": str(prior_global_boost_session_path / "ma_state_family_high_value_boost_v1_decision.json"),
            "prior_context_gated_boost_compare_json": str(prior_context_gated_boost_session_path / "multi_timeframe_context_gated_high_value_boost_v1_compare.json"),
            "prior_context_gated_boost_decision_json": str(prior_context_gated_boost_session_path / "multi_timeframe_context_gated_high_value_boost_v1_decision.json"),
        },
        "output_artifacts": {key: str(session_final / path.name) for key, path in output_files.items()},
        "no_lookahead_inherited": bool(context_payloads["decision"].get("no_lookahead_inherited", True)),
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
        "conditional_high_value_gate_count": int(authoritative_triple_high_value),
        "shape_source_row_count": int(shape_payloads["summary"]["conditional_high_value_row_count"]),
        "candidate_count": candidate_count,
        "row_count": row_count,
        "matched_shape_rate": shape_match_rate,
        "shape_join_rate": shape_join_rate,
        "context_match_rate": context_match_rate,
        "monthly_context_count": int(frame["monthly_context"].fillna("monthly_unknown").astype(str).nunique()),
        "weekly_context_count": int(frame["weekly_context"].fillna("weekly_unknown").astype(str).nunique()),
        "shape_positive_modifiers": positive_modifiers,
        "shape_context_dependent_prune_modifiers": context_dependent_modifiers,
        "shape_delta_join_available": bool(shape_delta_join_available),
    }

    _write_json(output_files["run_manifest_json"], manifest_payload)
    _write_json(output_files["context_gated_candle_shape_modifier_boost_prune_v1_compare_json"], compare_payload)
    _write_json(output_files["context_gated_candle_shape_modifier_boost_prune_v1_decision_json"], decision_payload)
    _write_json(output_files["shape_adjustment_coverage_summary_json"], coverage_payload)
    _write_json(output_files["prior_boost_delta_json"], prior_boost_delta_payload)
    _write_json(output_files["shape_comparison_json"], shape_comparison)
    _write_json(output_files["monthly_context_comparison_json"], monthly_context_comparison)
    _write_json(output_files["weekly_context_comparison_json"], weekly_context_comparison)

    diff_frame = frame.copy()
    diff_frame["selection_changed_top5"] = diff_frame["champion_selected_top5"] != diff_frame["challenger_selected_top5"]
    diff_frame["selection_changed_top10"] = diff_frame["champion_selected_top10"] != diff_frame["challenger_selected_top10"]
    diff_frame["selection_changed_top20"] = diff_frame["champion_selected_top20"] != diff_frame["challenger_selected_top20"]
    diff_frame.to_parquet(output_files["topk_membership_diff_parquet"], index=False)

    _write_json(
        output_files["_artifact_complete_json"],
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "session_id": session_id,
            "validated": True,
        },
    )

    for json_path in (
        output_files["run_manifest_json"],
        output_files["context_gated_candle_shape_modifier_boost_prune_v1_compare_json"],
        output_files["context_gated_candle_shape_modifier_boost_prune_v1_decision_json"],
        output_files["shape_adjustment_coverage_summary_json"],
        output_files["prior_boost_delta_json"],
        output_files["shape_comparison_json"],
        output_files["monthly_context_comparison_json"],
        output_files["weekly_context_comparison_json"],
        output_files["_artifact_complete_json"],
    ):
        json.loads(json_path.read_text(encoding="utf-8"))
    pd.read_parquet(output_files["topk_membership_diff_parquet"])

    session_final.mkdir(parents=True, exist_ok=False)
    for path in output_files.values():
        shutil.move(str(path), str(session_final / path.name))
    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")

    return {
        "session_id": session_id,
        "session_dir": str(session_final),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_shape_session_id": shape_payloads["manifest"]["session_id"],
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_context_gated_boost_session_id": prior_context_gated_boost_session_path.name,
        "row_count": row_count,
        "candidate_count": candidate_count,
        "matched_shape_rate": shape_match_rate,
        "decision": decision_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX candle-shape gated boost/prune analysis.")
    parser.add_argument("--source-context-session", default=str(DEFAULT_CONTEXT_SESSION))
    parser.add_argument("--source-shape-session", default=str(DEFAULT_SHAPE_SESSION))
    parser.add_argument("--source-family-session", default=str(DEFAULT_SOURCE_FAMILY_SESSION))
    parser.add_argument("--prior-global-boost-session", default=str(DEFAULT_PRIOR_GLOBAL_BOOST_SESSION))
    parser.add_argument("--prior-context-gated-boost-session", default=str(DEFAULT_PRIOR_CONTEXT_GATED_BOOST_SESSION))
    parser.add_argument("--candidate-input-dir", default=str(DEFAULT_CANDIDATE_INPUT_DIR))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    args = parser.parse_args(argv)

    run_context_gated_candle_shape_modifier_boost_prune_v1(
        source_context_session=args.source_context_session,
        source_shape_session=args.source_shape_session,
        source_family_session=args.source_family_session,
        prior_global_boost_session=args.prior_global_boost_session,
        prior_context_gated_boost_session=args.prior_context_gated_boost_session,
        candidate_input_dir=args.candidate_input_dir,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
