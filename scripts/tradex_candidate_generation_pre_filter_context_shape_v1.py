from __future__ import annotations

import argparse
import json
import math
import sys
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
    _make_session_id,
    _rank_selection,
    _safe_float,
    _safe_int,
    _write_json,
)


DEFAULT_CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
DEFAULT_SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
DEFAULT_FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\ma_context_shape_direct_adjustment_line\20260429T143302Z-8f34ef9d")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1"
COMPARE_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_manifest_v1"
POLICY_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_policy_v1"
COVERAGE_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_coverage_v1"
MONTHLY_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_VERSION = "tradex_candidate_generation_pre_filter_context_shape_v1_context_comparison_v1"

TOP_K_VALUES = (5, 10, 20)


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


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _resolve_source_session(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_family_session(session_path: Path) -> dict[str, Any]:
    manifest = _load_json(session_path / "run_manifest.json")
    decision = _load_json(session_path / "state_family_filter_v1_decision.json")
    summary = _load_json(session_path / "state_family_summary.json")
    classification = _load_json(session_path / "state_family_classification.json")
    by_regime = _load_json(session_path / "state_family_by_regime.json")
    monthly = _load_json(session_path / "state_family_monthly_stability.json")
    row_parquet = session_path / "state_family_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "decision": decision,
        "summary": summary,
        "classification": classification,
        "by_regime": by_regime,
        "monthly": monthly,
        "row_parquet": row_parquet,
    }


def _load_context_session(session_path: Path) -> dict[str, Any]:
    manifest = _load_json(session_path / "run_manifest.json")
    summary = _load_json(session_path / "conditional_state_value_summary.json")
    classification = _load_json(session_path / "conditional_state_classification.json")
    comparison = _load_json(session_path / "global_vs_conditional_comparison.json")
    decision = _load_json(session_path / "multi_timeframe_conditional_state_value_v1_decision.json")
    context_definition = _load_json(session_path / "context_definition.json")
    row_parquet = session_path / "conditional_state_rows.parquet"
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


def _load_shape_session(session_path: Path) -> dict[str, Any]:
    manifest = _load_json(session_path / "run_manifest.json")
    summary = _load_json(session_path / "conditional_shape_value_summary.json")
    classification = _load_json(session_path / "conditional_shape_modifier_classification.json")
    comparison = _load_json(session_path / "shape_vs_base_slice_comparison.json")
    decision = _load_json(session_path / "conditional_high_value_candle_shape_modifier_v1_decision.json")
    definition = _load_json(session_path / "candle_shape_definition.json")
    row_parquet = session_path / "conditional_shape_rows.parquet"
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


def _load_freeze_session(session_path: Path) -> dict[str, Any]:
    lineage = _load_json(session_path / "lineage_summary.json")
    decision = _load_json(session_path / "freeze_decision.json")
    reusable = _load_json(session_path / "remaining_reusable_signals.json")
    next_axis = _load_json(session_path / "next_axis_recommendation.json")
    return {
        "lineage_summary": lineage,
        "freeze_decision": decision,
        "remaining_reusable_signals": reusable,
        "next_axis_recommendation": next_axis,
    }


def _extract_source_thresholds(source_payloads: dict[str, Any]) -> tuple[float, float, dict[str, Any]]:
    manifest = source_payloads.get("manifest") or {}
    family_summary = source_payloads.get("family_summary") or {}
    source_thresholds = manifest.get("thresholds") or family_summary.get("filter_thresholds") or {}
    top15_threshold = _safe_float(source_thresholds.get("top15_score_threshold"))
    bottom15_threshold = _safe_float(source_thresholds.get("bottom15_score_threshold"))
    if top15_threshold is None or bottom15_threshold is None:
        raise RuntimeError("missing top15/bottom15 thresholds from source family session")
    return float(top15_threshold), float(bottom15_threshold), source_thresholds


def _build_shape_classification_map(classification_payload: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in classification_payload.get("rows") or []:
        modifier = str(row.get("candle_shape_modifier") or "")
        shape_class = str(row.get("shape_classification") or "")
        if modifier:
            mapping[modifier] = shape_class
    return mapping


def _join_candidate_signals(
    *,
    candidate_rows: pd.DataFrame,
    family_row_parquet: Path,
    shape_row_parquet: Path,
    shape_classification_map: dict[str, str],
) -> pd.DataFrame:
    import duckdb

    conn = duckdb.connect()
    try:
        conn.register("candidate_rows", candidate_rows)
        sql = f"""
        WITH shape_rows AS (
            SELECT
                CAST(code AS VARCHAR) AS symbol,
                trade_date,
                state_family_id,
                family_classification,
                stable_high_value_family,
                stable_bad_pick_family,
                regime_dependent_family,
                unstable_or_sparse_family,
                neutral_family,
                family_regime_context,
                family_bad_pick_regime,
                dominant_regime_context,
                family_sample_count,
                family_unique_symbol_count,
                family_month_count,
                family_mean_forward_ret_5d,
                family_mean_forward_ret_10d,
                family_mean_forward_ret_20d,
                family_median_forward_ret_20d,
                family_mean_mfe_20d,
                family_mean_mae_20d,
                family_mean_path_value_score_v1,
                family_median_path_value_score_v1,
                family_plus5_before_minus5_rate,
                family_minus5_before_plus5_rate,
                family_top15_rate,
                family_bottom15_rate,
                family_months_observed,
                family_positive_month_rate,
                family_worst_month_mean_path_value,
                family_best_month_mean_path_value,
                monthly_context,
                monthly_context_date,
                monthly_context_source,
                monthly_context_no_lookahead,
                weekly_context,
                weekly_context_date,
                weekly_context_source,
                weekly_context_no_lookahead,
                top15_label,
                bottom15_label,
                conditional_high_value,
                candle_shape_modifier,
                candle_body_ratio,
                candle_upper_wick_ratio,
                candle_lower_wick_ratio,
                candle_triplet_up_prob,
                candle_triplet_down_prob,
                gap_pct,
                vol_ratio5_20,
                o,
                h,
                l,
                c,
                v,
                prev_o,
                prev_h,
                prev_l,
                prev_c
            FROM read_parquet('{shape_row_parquet.as_posix()}')
        ),
        family_rows AS (
            SELECT
                CAST(code AS VARCHAR) AS symbol,
                trade_date,
                forward_ret_5d,
                forward_ret_10d,
                forward_ret_20d,
                path_value_score_v1,
                mfe_20d,
                mae_20d,
                hit_plus_5_before_minus_5,
                hit_minus_5_before_plus_5
            FROM read_parquet('{family_row_parquet.as_posix()}')
        )
        SELECT
            c.candidate_idx,
            c.anchor_date,
            c.trade_date,
            c.month_bucket,
            c.side,
            c.symbol,
            c.rank AS candidate_rank,
            c.score AS candidate_score,
            c.champion_score,
            c.champion_rank,
            c.challenger_score,
            c.challenger_rank,
            c.challenger_gate,
            c.champion_gate,
            c.selected_by,
            c.selected_by_methods,
            c.selection_reason,
            c.challenger_selected_top5,
            c.challenger_selected_top10,
            c.challenger_selected_top20,
            c.champion_selected_top5,
            c.champion_selected_top10,
            c.champion_selected_top20,
            c.changed_top5_member,
            c.changed_top10_member,
            c.changed_top20_member,
            c.market_regime_bucket,
            s.state_family_id,
            s.family_classification,
            s.stable_high_value_family,
            s.stable_bad_pick_family,
            s.regime_dependent_family,
            s.unstable_or_sparse_family,
            s.neutral_family,
            s.family_regime_context,
            s.family_bad_pick_regime,
            s.dominant_regime_context,
            s.family_sample_count,
            s.family_unique_symbol_count,
            s.family_month_count,
            s.family_mean_forward_ret_5d,
            s.family_mean_forward_ret_10d,
            s.family_mean_forward_ret_20d,
            s.family_median_forward_ret_20d,
            s.family_mean_mfe_20d,
            s.family_mean_mae_20d,
            s.family_mean_path_value_score_v1,
            s.family_median_path_value_score_v1,
            s.family_plus5_before_minus5_rate,
            s.family_minus5_before_plus5_rate,
            s.family_top15_rate,
            s.family_bottom15_rate,
            s.family_months_observed,
            s.family_positive_month_rate,
            s.family_worst_month_mean_path_value,
            s.family_best_month_mean_path_value,
            s.monthly_context,
            s.monthly_context_date,
            s.monthly_context_source,
            s.monthly_context_no_lookahead,
            s.weekly_context,
            s.weekly_context_date,
            s.weekly_context_source,
            s.weekly_context_no_lookahead,
            s.top15_label,
            s.bottom15_label,
            s.conditional_high_value,
            s.candle_shape_modifier,
            f.forward_ret_5d,
            f.forward_ret_10d,
            f.forward_ret_20d,
            f.path_value_score_v1,
            f.mfe_20d,
            f.mae_20d,
            f.hit_plus_5_before_minus_5,
            f.hit_minus_5_before_plus_5,
            s.candle_body_ratio,
            s.candle_upper_wick_ratio,
            s.candle_lower_wick_ratio,
            s.candle_triplet_up_prob,
            s.candle_triplet_down_prob,
            s.gap_pct,
            s.vol_ratio5_20,
            s.o,
            s.h,
            s.l,
            s.c,
            s.v,
            s.prev_o,
            s.prev_h,
            s.prev_l,
            s.prev_c
        FROM candidate_rows c
        LEFT JOIN shape_rows s
          ON s.trade_date = c.trade_date AND s.symbol = c.symbol
        LEFT JOIN family_rows f
          ON f.trade_date = c.trade_date AND f.symbol = c.symbol
        ORDER BY c.candidate_idx
        """
        frame = conn.execute(sql).fetchdf()
    finally:
        conn.close()

    frame["shape_joined"] = frame["candle_shape_modifier"].notna()
    frame["shape_classification"] = frame["candle_shape_modifier"].map(shape_classification_map)
    frame.loc[frame["shape_joined"] & frame["shape_classification"].isna(), "shape_classification"] = "shape_neutral"
    frame["shape_classification"] = frame["shape_classification"].fillna("shape_missing")
    frame["score"] = frame["candidate_score"]
    frame["rank"] = frame["candidate_rank"]
    frame["conditional_high_value"] = frame["conditional_high_value"].fillna(False).astype(bool)
    frame["stable_high_value_family"] = frame["stable_high_value_family"].fillna(False).astype(bool)
    frame["stable_bad_pick_family"] = frame["stable_bad_pick_family"].fillna(False).astype(bool)
    frame["family_classification"] = frame["family_classification"].fillna("unknown").astype(str)
    frame["family_bad_pick_regime"] = frame["family_bad_pick_regime"].fillna("unknown").astype(str)
    frame["dominant_regime_context"] = frame["dominant_regime_context"].fillna("unknown").astype(str)
    frame["monthly_context"] = frame["monthly_context"].fillna("unknown").astype(str)
    frame["weekly_context"] = frame["weekly_context"].fillna("unknown").astype(str)
    frame["month_bucket"] = frame["month_bucket"].fillna(frame["anchor_date"].astype(str).str.slice(0, 7)).astype(str)
    frame["prefilter_reason"] = frame.apply(_build_prefilter_reason, axis=1)
    frame["prefilter_bucket"] = frame.apply(_build_prefilter_bucket, axis=1)
    frame["include_in_broad_pool"] = frame["prefilter_bucket"].isin(["KEEP_PRIMARY", "KEEP_WATCH"])
    frame["include_in_strict_pool"] = frame["prefilter_bucket"].eq("KEEP_PRIMARY")
    frame["include_in_exclude_only_pool"] = frame["prefilter_bucket"].ne("EXCLUDE")
    return frame


def _build_prefilter_reason(row: pd.Series) -> list[str]:
    reasons: list[str] = []
    if not bool(row.get("shape_joined")):
        reasons.append("shape_missing")
    if bool(row.get("conditional_high_value")) and str(row.get("shape_classification")) == "shape_positive_modifier":
        reasons.append("conditional_high_value")
        reasons.append("positive_shape_modifier")
    elif bool(row.get("conditional_high_value")) and str(row.get("shape_classification")) == "shape_context_dependent":
        reasons.append("conditional_high_value")
        reasons.append("context_dependent_shape_modifier")
    elif bool(row.get("conditional_high_value")) and str(row.get("shape_classification")) == "shape_missing":
        reasons.append("conditional_high_value")
        reasons.append("shape_missing")

    strong_bad_pick = bool(row.get("stable_bad_pick_family"))
    bad_pick_diag = strong_bad_pick or (
        str(row.get("family_classification")) == "regime_dependent_family"
        and str(row.get("family_bad_pick_regime")) == str(row.get("dominant_regime_context"))
    )
    if strong_bad_pick:
        reasons.append("strong_bad_pick_family")
    elif bad_pick_diag:
        reasons.append("bad_pick_diagnostic")
    return reasons


def _build_prefilter_bucket(row: pd.Series) -> str:
    conditional = bool(row.get("conditional_high_value"))
    shape_class = str(row.get("shape_classification"))
    strong_bad_pick = bool(row.get("stable_bad_pick_family"))
    bad_pick_diag = strong_bad_pick or (
        str(row.get("family_classification")) == "regime_dependent_family"
        and str(row.get("family_bad_pick_regime")) == str(row.get("dominant_regime_context"))
    )
    if conditional and shape_class == "shape_positive_modifier":
        return "KEEP_PRIMARY"
    if conditional and (shape_class == "shape_context_dependent" or shape_class == "shape_missing"):
        return "KEEP_WATCH"
    if strong_bad_pick and not conditional:
        return "EXCLUDE"
    if bad_pick_diag and not conditional:
        return "DOWNGRADE"
    return "KEEP_WATCH"


def _rank_pool(frame: pd.DataFrame, *, score_col: str, prefix: str) -> pd.DataFrame:
    if frame.empty:
        out = frame.copy()
        for top_k in TOP_K_VALUES:
            out[f"{prefix}_selected_top{top_k}"] = pd.Series(dtype=bool)
        return out
    ranked = _rank_selection(frame, score_col=score_col, prefix=prefix)
    return ranked


def _topk_summary_for_frame(
    frame: pd.DataFrame,
    *,
    selected_col: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    return _aggregate_selected_rows(
        frame,
        selected_col=selected_col,
        bottom15_threshold=bottom15_threshold,
        top15_threshold=top15_threshold,
    )


def _group_comparison_rows(
    *,
    frame: pd.DataFrame,
    selected_prefix: str,
    group_cols: list[str],
    bottom15_threshold: float,
    top15_threshold: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped = frame.groupby(group_cols, dropna=False, sort=False)
    for group_key, group in grouped:
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        original = _topk_summary_for_frame(
            group,
            selected_col=f"original_selected_top20",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
        selected = _topk_summary_for_frame(
            group,
            selected_col=f"{selected_prefix}_selected_top20",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
        rows.append(
            {
                **{str(col): value for col, value in zip(group_cols, group_key)},
                "original": original,
                "challenger": selected,
                "delta": {
                    "mean_forward_ret_20d": None
                    if original["mean_forward_ret_20d"] is None or selected["mean_forward_ret_20d"] is None
                    else float(selected["mean_forward_ret_20d"] - original["mean_forward_ret_20d"]),
                    "median_forward_ret_20d": None
                    if original["median_forward_ret_20d"] is None or selected["median_forward_ret_20d"] is None
                    else float(selected["median_forward_ret_20d"] - original["median_forward_ret_20d"]),
                    "mean_path_value_score_v1": None
                    if original["mean_path_value_score_v1"] is None or selected["mean_path_value_score_v1"] is None
                    else float(selected["mean_path_value_score_v1"] - original["mean_path_value_score_v1"]),
                    "median_path_value_score_v1": None
                    if original["median_path_value_score_v1"] is None or selected["median_path_value_score_v1"] is None
                    else float(selected["median_path_value_score_v1"] - original["median_path_value_score_v1"]),
                    "bottom15_contamination_rate": None
                    if original["bottom15_contamination_rate"] is None or selected["bottom15_contamination_rate"] is None
                    else float(selected["bottom15_contamination_rate"] - original["bottom15_contamination_rate"]),
                    "bad_pick_family_contamination_rate": None
                    if original["bad_pick_family_contamination_rate"] is None or selected["bad_pick_family_contamination_rate"] is None
                    else float(selected["bad_pick_family_contamination_rate"] - original["bad_pick_family_contamination_rate"]),
                },
            }
        )
    return rows


def _summarize_group_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [row["delta"]["mean_forward_ret_20d"] for row in rows if row["delta"]["mean_forward_ret_20d"] is not None]
    zero_pass_month_count = int(sum(1 for row in rows if int(row["challenger"]["selected_count"]) == 0))
    return {
        "group_count": len(rows),
        "win_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] > 0)),
        "loss_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] < 0)),
        "flat_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] == 0)),
        "zero_pass_month_count": zero_pass_month_count,
        "zero_pass_month_rate": float(zero_pass_month_count / max(1, len(rows))),
        "worst_delta_mean_forward_ret_20d": None if not deltas else float(min(deltas)),
        "best_delta_mean_forward_ret_20d": None if not deltas else float(max(deltas)),
    }


def _pool_topk_metrics(frame: pd.DataFrame, *, bottom15_threshold: float, top15_threshold: float) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        selected_col = f"selected_top{top_k}"
        output[str(top_k)] = _aggregate_selected_rows(
            frame,
            selected_col=selected_col,
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
    return output


def _selected_pool_frame(frame: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    selected_cols = [f"{prefix}_selected_top{top_k}" for top_k in TOP_K_VALUES]
    mask = pd.Series(False, index=frame.index)
    for col in selected_cols:
        mask = mask | frame[col].fillna(False).astype(bool)
    return frame.loc[mask].copy()


def _build_pool_comparison(
    *,
    original: pd.DataFrame,
    broad: pd.DataFrame,
    strict: pd.DataFrame,
    exclude_only: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    pools = {
        "original": original,
        "prefilter_primary_watch": broad,
        "prefilter_primary_only": strict,
        "exclude_only_analysis": exclude_only,
    }
    ranked = {
        name: _rank_pool(frame, score_col="score", prefix=name)
        for name, frame in pools.items()
    }
    metrics = {
        name: {
            "candidate_count": int(len(frame)),
            "coverage_rate": float(len(frame) / max(1, len(original))),
            "topk": _pool_topk_metrics(
                _rename_selection_columns(frame, prefix=name),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            ),
        }
        for name, frame in ranked.items()
    }
    original_metrics = metrics["original"]["topk"]
    delta_vs_original: dict[str, Any] = {}
    for name, pool_metrics in metrics.items():
        if name == "original":
            continue
        delta_vs_original[name] = {}
        for top_k in TOP_K_VALUES:
            top_key = str(top_k)
            delta_vs_original[name][top_key] = {
                key: (
                    None
                    if original_metrics[top_key][key] is None or pool_metrics["topk"][top_key][key] is None
                    else float(pool_metrics["topk"][top_key][key] - original_metrics[top_key][key])
                )
                for key in [
                    "mean_forward_ret_20d",
                    "median_forward_ret_20d",
                    "mean_path_value_score_v1",
                    "median_path_value_score_v1",
                    "top15_capture_rate",
                    "bottom15_contamination_rate",
                    "bad_pick_family_contamination_rate",
                ]
            }
    strict_vs_broad = {
        top_key: {
            key: (
                None
                if metrics["prefilter_primary_watch"]["topk"][top_key][key] is None or metrics["prefilter_primary_only"]["topk"][top_key][key] is None
                else float(metrics["prefilter_primary_only"]["topk"][top_key][key] - metrics["prefilter_primary_watch"]["topk"][top_key][key])
            )
            for key in [
                "mean_forward_ret_20d",
                "median_forward_ret_20d",
                "mean_path_value_score_v1",
                "median_path_value_score_v1",
                "top15_capture_rate",
                "bottom15_contamination_rate",
                "bad_pick_family_contamination_rate",
            ]
        }
        for top_key in [str(top_k) for top_k in TOP_K_VALUES]
    }
    return {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1_candidate_snapshots",
            "original_score": "score",
            "grouping": ["anchor_date", "side"],
            "top_k_values": list(TOP_K_VALUES),
            "no_silent_fallback": True,
            "pre_filter_is_analysis_only": True,
        },
        "candidate_universe": {
            "original_row_count": int(len(original)),
            "broad_pool_row_count": int(len(broad)),
            "strict_pool_row_count": int(len(strict)),
            "exclude_only_analysis_row_count": int(len(exclude_only)),
            "broad_pool_coverage_rate": float(len(broad) / max(1, len(original))),
            "strict_pool_coverage_rate": float(len(strict) / max(1, len(original))),
            "exclude_only_analysis_coverage_rate": float(len(exclude_only) / max(1, len(original))),
        },
        "pools": metrics,
        "delta_vs_original": delta_vs_original,
        "strict_vs_broad_delta": strict_vs_broad,
    }


def _rename_selection_columns(frame: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    out = frame.copy()
    for top_k in TOP_K_VALUES:
        out[f"selected_top{top_k}"] = out[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)
    return out


def _build_monthly_comparison(
    *,
    original: pd.DataFrame,
    broad: pd.DataFrame,
    strict: pd.DataFrame,
    exclude_only: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    pools = {
        "prefilter_primary_watch": broad,
        "prefilter_primary_only": strict,
        "exclude_only_analysis": exclude_only,
    }
    result: dict[str, Any] = {"schema_version": MONTHLY_SCHEMA_VERSION, "generated_at": _utc_now(), "topk": {}}
    for top_k in TOP_K_VALUES:
        top_key = str(top_k)
        original_frame = _rename_selection_columns(original, prefix="original")
        rows_by_pool: dict[str, list[dict[str, Any]]] = {}
        summaries: dict[str, Any] = {}
        for pool_name, pool_frame in pools.items():
            pool_named = _rename_selection_columns(pool_frame, prefix=pool_name)
            rows: list[dict[str, Any]] = []
            for month in sorted(original_frame["month_bucket"].dropna().astype(str).unique().tolist()):
                orig_group = original_frame.loc[original_frame["month_bucket"].astype(str) == month].copy()
                pool_group = pool_named.loc[pool_named["month_bucket"].astype(str) == month].copy()
                orig_sel = _aggregate_selected_rows(
                    orig_group,
                    selected_col=f"original_selected_top{top_k}",
                    bottom15_threshold=bottom15_threshold,
                    top15_threshold=top15_threshold,
                )
                pool_sel = _aggregate_selected_rows(
                    pool_group,
                    selected_col=f"{pool_name}_selected_top{top_k}",
                    bottom15_threshold=bottom15_threshold,
                    top15_threshold=top15_threshold,
                )
                rows.append(
                    {
                        "month_bucket": month,
                        "original": orig_sel,
                        "challenger": pool_sel,
                        "delta": {
                            "mean_forward_ret_20d": None
                            if orig_sel["mean_forward_ret_20d"] is None or pool_sel["mean_forward_ret_20d"] is None
                            else float(pool_sel["mean_forward_ret_20d"] - orig_sel["mean_forward_ret_20d"]),
                            "mean_path_value_score_v1": None
                            if orig_sel["mean_path_value_score_v1"] is None or pool_sel["mean_path_value_score_v1"] is None
                            else float(pool_sel["mean_path_value_score_v1"] - orig_sel["mean_path_value_score_v1"]),
                            "bottom15_contamination_rate": None
                            if orig_sel["bottom15_contamination_rate"] is None or pool_sel["bottom15_contamination_rate"] is None
                            else float(pool_sel["bottom15_contamination_rate"] - orig_sel["bottom15_contamination_rate"]),
                            "bad_pick_family_contamination_rate": None
                            if orig_sel["bad_pick_family_contamination_rate"] is None or pool_sel["bad_pick_family_contamination_rate"] is None
                            else float(pool_sel["bad_pick_family_contamination_rate"] - orig_sel["bad_pick_family_contamination_rate"]),
                        },
                    }
                )
            rows_by_pool[pool_name] = rows
            summaries[pool_name] = _summarize_group_rows(rows)
        result["topk"][top_key] = {"rows_by_pool": rows_by_pool, "summary_by_pool": summaries}
    return result


def _build_context_comparison(
    *,
    original: pd.DataFrame,
    broad: pd.DataFrame,
    strict: pd.DataFrame,
    exclude_only: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": CONTEXT_SCHEMA_VERSION, "generated_at": _utc_now(), "topk": {}}
    groups = [
        ("monthly_weekly", ["monthly_context", "weekly_context"]),
        ("dominant_regime_context", ["dominant_regime_context"]),
    ]
    pools = {
        "prefilter_primary_watch": broad,
        "prefilter_primary_only": strict,
        "exclude_only_analysis": exclude_only,
    }
    for top_k in TOP_K_VALUES:
        top_key = str(top_k)
        original_named = _rename_selection_columns(original, prefix="original")
        top_entry: dict[str, Any] = {}
        for group_name, group_cols in groups:
            group_entry: dict[str, Any] = {}
            for pool_name, pool_frame in pools.items():
                pool_named = _rename_selection_columns(pool_frame, prefix=pool_name)
                rows = _group_comparison_rows(
                    frame=pd.concat([original_named, pool_named], axis=0, ignore_index=True, sort=False),
                    selected_prefix=pool_name,
                    group_cols=group_cols,
                    bottom15_threshold=bottom15_threshold,
                    top15_threshold=top15_threshold,
                )
                group_entry[pool_name] = {
                    "rows": rows,
                    "summary": _summarize_group_rows(rows),
                }
            top_entry[group_name] = group_entry
        result["topk"][top_key] = top_entry
    return result


def _decision_from_metrics(pool_comparison: dict[str, Any]) -> dict[str, Any]:
    deltas = pool_comparison["delta_vs_original"]
    broad = deltas["prefilter_primary_watch"]
    strict = deltas["prefilter_primary_only"]
    broad_top5 = broad["5"]["mean_path_value_score_v1"]
    broad_top10 = broad["10"]["mean_path_value_score_v1"]
    strict_top5 = strict["5"]["mean_path_value_score_v1"]
    strict_top10 = strict["10"]["mean_path_value_score_v1"]
    broad_bottom15 = broad["5"]["bottom15_contamination_rate"]
    strict_bottom15 = strict["5"]["bottom15_contamination_rate"]
    coverage = pool_comparison["candidate_universe"]
    coverage_ok = float(coverage["broad_pool_coverage_rate"]) >= 0.25
    topk_move = any(
        abs(float(deltas["prefilter_primary_watch"][top_key]["mean_forward_ret_20d"] or 0.0)) > 0.0
        or abs(float(deltas["prefilter_primary_only"][top_key]["mean_forward_ret_20d"] or 0.0)) > 0.0
        for top_key in ["5", "10", "20"]
    )
    if (broad_top5 is not None and broad_top5 > 0) and (broad_top10 is not None and broad_top10 > 0) and coverage_ok and topk_move:
        decision = "keep"
        reason = "prefilter_improves_topk_without_score_adjustment"
    elif coverage["broad_pool_coverage_rate"] < 0.15 or not topk_move:
        decision = "drop"
        reason = "prefiltering_is_too_sparse_or_topk_did_not_move"
    else:
        decision = "hold"
        reason = "prefilter_signal_exists_but_top5_or_bottom15_remains_mixed"
    if strict_top5 is not None and broad_top5 is not None and strict_top5 > broad_top5:
        reason = reason + "_strict_pool_beats_broad_top5"
    if strict_bottom15 is not None and broad_bottom15 is not None and strict_bottom15 > broad_bottom15:
        reason = reason + "_strict_pool_worsens_bottom15"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "same_condition_contract": True,
        "not_meemee_reflectable": True,
        "production_reflection_allowed": False,
        "recommendation": decision,
        "typed_reasons": [reason],
    }


def build_artifacts(
    *,
    candidate_input_dir: Path,
    family_session: Path,
    context_session: Path,
    shape_session: Path,
    freeze_session: Path,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    family_payload = _load_family_session(family_session)
    context_payload = _load_context_session(context_session)
    shape_payload = _load_shape_session(shape_session)
    freeze_payload = _load_freeze_session(freeze_session)
    thresholds = _extract_source_thresholds(
        {
            "manifest": family_payload["manifest"],
            "family_summary_report": family_payload["summary"],
        }
    )
    top15_threshold, bottom15_threshold, source_thresholds = thresholds
    candidate_rows = _load_candidate_rows(candidate_input_dir)
    candidate_rows = _apply_anchor_limit(candidate_rows, limit_anchor_dates)
    shape_classification_map = _build_shape_classification_map(shape_payload["classification"])
    joined = _join_candidate_signals(
        candidate_rows=candidate_rows,
        family_row_parquet=family_payload["row_parquet"],
        shape_row_parquet=shape_payload["row_parquet"],
        shape_classification_map=shape_classification_map,
    )
    joined["shape_joined"] = joined["shape_joined"].fillna(False).astype(bool)
    joined["include_in_broad_pool"] = joined["include_in_broad_pool"].fillna(False).astype(bool)
    joined["include_in_strict_pool"] = joined["include_in_strict_pool"].fillna(False).astype(bool)
    joined["include_in_exclude_only_pool"] = joined["include_in_exclude_only_pool"].fillna(False).astype(bool)

    original_ranked = _rank_pool(joined, score_col="score", prefix="original")
    broad_frame = joined.loc[joined["include_in_broad_pool"]].copy().reset_index(drop=True)
    strict_frame = joined.loc[joined["include_in_strict_pool"]].copy().reset_index(drop=True)
    exclude_only_frame = joined.loc[joined["include_in_exclude_only_pool"]].copy().reset_index(drop=True)
    broad_ranked = _rank_pool(broad_frame, score_col="score", prefix="prefilter_primary_watch")
    strict_ranked = _rank_pool(strict_frame, score_col="score", prefix="prefilter_primary_only")
    exclude_ranked = _rank_pool(exclude_only_frame, score_col="score", prefix="exclude_only_analysis")

    # Rank original frame for comparisons.
    original_ranked = _rename_selection_columns(original_ranked, prefix="original")
    broad_ranked = _rename_selection_columns(broad_ranked, prefix="prefilter_primary_watch")
    strict_ranked = _rename_selection_columns(strict_ranked, prefix="prefilter_primary_only")
    exclude_ranked = _rename_selection_columns(exclude_ranked, prefix="exclude_only_analysis")

    original_selected = original_ranked
    broad_selected = broad_ranked
    strict_selected = strict_ranked
    exclude_selected = exclude_ranked

    candidate_pool_comparison = _build_pool_comparison(
        original=original_selected,
        broad=broad_selected,
        strict=strict_selected,
        exclude_only=exclude_selected,
        bottom15_threshold=float(bottom15_threshold),
        top15_threshold=float(top15_threshold),
    )
    monthly_comparison = _build_monthly_comparison(
        original=original_selected,
        broad=broad_selected,
        strict=strict_selected,
        exclude_only=exclude_selected,
        bottom15_threshold=float(bottom15_threshold),
        top15_threshold=float(top15_threshold),
    )
    context_comparison = _build_context_comparison(
        original=original_selected,
        broad=broad_selected,
        strict=strict_selected,
        exclude_only=exclude_selected,
        bottom15_threshold=float(bottom15_threshold),
        top15_threshold=float(top15_threshold),
    )

    topk_diff_rows = []
    for _, row in original_selected.iterrows():
        record = {
            "candidate_idx": int(row["candidate_idx"]),
            "anchor_date": row["anchor_date"],
            "symbol": row["symbol"],
            "side": row["side"],
            "score": float(row["score"]),
            "shape_joined": bool(row["shape_joined"]),
            "prefilter_bucket": row["prefilter_bucket"],
            "shape_classification": row["shape_classification"],
            "conditional_high_value": bool(row["conditional_high_value"]),
            "monthly_context": row["monthly_context"],
            "weekly_context": row["weekly_context"],
            "family_classification": row["family_classification"],
            "stable_high_value_family": bool(row["stable_high_value_family"]),
            "stable_bad_pick_family": bool(row["stable_bad_pick_family"]),
            "family_bad_pick_regime": row["family_bad_pick_regime"],
            "dominant_regime_context": row["dominant_regime_context"],
        }
        changed_any = False
        for pool_name, frame in {
            "prefilter_primary_watch": broad_selected,
            "prefilter_primary_only": strict_selected,
            "exclude_only_analysis": exclude_selected,
        }.items():
            pool_keys = set(map(tuple, frame.loc[frame[f"{pool_name}_selected_top20"] == True, ["anchor_date", "symbol", "side"]].astype(str).values.tolist()))  # noqa: E712
            key = (str(row["anchor_date"]), str(row["symbol"]), str(row["side"]))
            record[f"{pool_name}_top5"] = key in set(map(tuple, frame.loc[frame[f"{pool_name}_selected_top5"] == True, ["anchor_date", "symbol", "side"]].astype(str).values.tolist()))  # noqa: E712
            record[f"{pool_name}_top10"] = key in set(map(tuple, frame.loc[frame[f"{pool_name}_selected_top10"] == True, ["anchor_date", "symbol", "side"]].astype(str).values.tolist()))  # noqa: E712
            record[f"{pool_name}_top20"] = key in pool_keys
        for top_k in TOP_K_VALUES:
            key = f"original_selected_top{top_k}"
            record[key] = bool(row[key])
        for pool_name in ["prefilter_primary_watch", "prefilter_primary_only", "exclude_only_analysis"]:
            changed_any = changed_any or any(record[f"{pool_name}_top{top_k}"] != record[f"original_selected_top{top_k}"] for top_k in TOP_K_VALUES)
        if changed_any:
            topk_diff_rows.append(record)

    policy_payload = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_artifacts": {
            "family_session": str(family_session),
            "context_session": str(context_session),
            "shape_session": str(shape_session),
            "freeze_session": str(freeze_session),
        },
        "policy_rules": {
            "KEEP_PRIMARY": "conditional_high_value == true and shape_classification == shape_positive_modifier",
            "KEEP_WATCH": "conditional_high_value == true and shape_classification in {shape_context_dependent, shape_missing}",
            "DOWNGRADE": "bad_pick_diagnostic_present == true and conditional_high_value == false",
            "EXCLUDE": "stable_bad_pick_family == true and conditional_high_value == false",
        },
        "diagnostic_definitions": {
            "strong_bad_pick_diagnostic": "stable_bad_pick_family == true",
            "bad_pick_diagnostic": "stable_bad_pick_family == true or (family_classification == regime_dependent_family and family_bad_pick_regime == dominant_regime_context)",
            "shape_positive_modifier": True,
            "shape_context_dependent": True,
        },
        "shape_classification_map_size": len(shape_classification_map),
        "thresholds": {
            "top15_score_threshold": float(top15_threshold),
            "bottom15_score_threshold": float(bottom15_threshold),
            "source_thresholds": source_thresholds,
        },
        "freeze_reference": {
            "lineage_summary": freeze_payload["lineage_summary"].get("decision"),
            "freeze_decision": freeze_payload["freeze_decision"].get("decision"),
            "freeze_reason": freeze_payload["freeze_decision"].get("decision_reason"),
        },
    }

    coverage_payload = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_count_original": int(len(joined)),
        "candidate_count_broad": int(len(broad_selected)),
        "candidate_count_strict": int(len(strict_selected)),
        "candidate_count_exclude_only_analysis": int(len(exclude_selected)),
        "coverage_rate_broad": float(len(broad_selected) / max(1, len(joined))),
        "coverage_rate_strict": float(len(strict_selected) / max(1, len(joined))),
        "coverage_rate_exclude_only_analysis": float(len(exclude_selected) / max(1, len(joined))),
        "join_coverage": {
            "shape_joined_count": int(joined["shape_joined"].sum()),
            "shape_join_missing_count": int((~joined["shape_joined"]).sum()),
            "shape_join_rate": float(joined["shape_joined"].mean()) if len(joined) else None,
            "conditional_high_value_count": int(joined["conditional_high_value"].sum()),
            "keep_primary_count": int((joined["prefilter_bucket"] == "KEEP_PRIMARY").sum()),
            "keep_watch_count": int((joined["prefilter_bucket"] == "KEEP_WATCH").sum()),
            "downgrade_count": int((joined["prefilter_bucket"] == "DOWNGRADE").sum()),
            "exclude_count": int((joined["prefilter_bucket"] == "EXCLUDE").sum()),
        },
        "source_sessions": {
            "family_session_id": family_session.name,
            "context_session_id": context_session.name,
            "shape_session_id": shape_session.name,
            "freeze_session_id": freeze_session.name,
        },
        "no_lookahead_inherited": bool(
            joined.loc[joined["shape_joined"], "monthly_context_no_lookahead"].fillna(False).astype(bool).all()
            and joined.loc[joined["shape_joined"], "weekly_context_no_lookahead"].fillna(False).astype(bool).all()
        ),
        "monthly_context_no_lookahead": bool(
            joined.loc[joined["shape_joined"], "monthly_context_no_lookahead"].fillna(False).astype(bool).all()
        ),
        "weekly_context_no_lookahead": bool(
            joined.loc[joined["shape_joined"], "weekly_context_no_lookahead"].fillna(False).astype(bool).all()
        ),
    }

    decision_payload = _decision_from_metrics(candidate_pool_comparison)
    decision_payload["coverage_summary"] = coverage_payload
    decision_payload["source_sessions"] = coverage_payload["source_sessions"]
    decision_payload["candidate_pool_counts"] = {
        "original": int(len(joined)),
        "broad": int(len(broad_selected)),
        "strict": int(len(strict_selected)),
        "exclude_only_analysis": int(len(exclude_selected)),
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_candidate_input_dir": str(candidate_input_dir),
        "source_family_session": str(family_session),
        "source_context_session": str(context_session),
        "source_shape_session": str(shape_session),
        "source_freeze_session": str(freeze_session),
        "source_session_ids": {
            "family": family_session.name,
            "context": context_session.name,
            "shape": shape_session.name,
            "freeze": freeze_session.name,
        },
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1_candidate_snapshots",
            "dedup_rule": "drop_duplicates(anchor_date, symbol, side, keep='first')",
            "score_field": "score",
            "ranking_groups": ["anchor_date", "side"],
            "ranking_sort": ["score desc", "rank asc", "symbol asc", "candidate_idx asc"],
            "top_k_values": list(TOP_K_VALUES),
            "pre_filter_scope": "analysis only",
        },
        "candidate_row_counts": {
            "original_input": int(len(candidate_rows)),
            "after_anchor_limit": int(len(candidate_rows)),
            "deduped_unique_rows": int(len(joined)),
        },
        "thresholds": {
            "top15_score_threshold": float(top15_threshold),
            "bottom15_score_threshold": float(bottom15_threshold),
            "source_thresholds": source_thresholds,
        },
        "artifact_paths": {
            "lineage_freeze_artifact": str(freeze_session),
        },
    }

    return {
        "manifest": manifest_payload,
        "policy": policy_payload,
        "coverage": coverage_payload,
        "decision": decision_payload,
        "candidate_pool_comparison": candidate_pool_comparison,
        "monthly_comparison": monthly_comparison,
        "context_comparison": context_comparison,
        "candidate_prefilter_rows": joined,
        "topk_membership_diff": pd.DataFrame(topk_diff_rows),
    }


def write_artifacts(*, output_root: Path, session_id: str | None = None, **kwargs: Any) -> Path:
    payload = build_artifacts(**kwargs)
    final_session_id = session_id or _make_session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    _write_json(session_root / "run_manifest.json", payload["manifest"])
    _write_json(session_root / "candidate_prefilter_policy.json", payload["policy"])
    _write_json(session_root / "candidate_prefilter_coverage_summary.json", payload["coverage"])
    _write_json(session_root / "candidate_pool_comparison.json", payload["candidate_pool_comparison"])
    _write_json(session_root / "monthly_comparison.json", payload["monthly_comparison"])
    _write_json(session_root / "context_comparison.json", payload["context_comparison"])
    _write_json(session_root / "candidate_generation_pre_filter_context_shape_v1_decision.json", payload["decision"])

    payload["candidate_prefilter_rows"].to_parquet(session_root / "candidate_prefilter_rows.parquet", index=False)
    payload["topk_membership_diff"].to_parquet(session_root / "topk_membership_diff.parquet", index=False)

    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_complete": True,
            "generated_at": _utc_now(),
            "session_root": str(session_root),
            "files": [
                "run_manifest.json",
                "candidate_prefilter_policy.json",
                "candidate_prefilter_coverage_summary.json",
                "candidate_pool_comparison.json",
                "monthly_comparison.json",
                "context_comparison.json",
                "topk_membership_diff.parquet",
                "candidate_prefilter_rows.parquet",
                "candidate_generation_pre_filter_context_shape_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return session_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research whether context/shape signals can pre-filter candidate generation.")
    parser.add_argument("--candidate-input-dir", default=str(DEFAULT_CANDIDATE_INPUT_DIR))
    parser.add_argument("--source-family-session", default=str(DEFAULT_FAMILY_SESSION))
    parser.add_argument("--source-context-session", default=str(DEFAULT_CONTEXT_SESSION))
    parser.add_argument("--source-shape-session", default=str(DEFAULT_SHAPE_SESSION))
    parser.add_argument("--source-freeze-session", default=str(DEFAULT_FREEZE_SESSION))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-anchor-dates", type=int, default=DEFAULT_LIMIT_ANCHOR_DATES)
    parser.add_argument("--session-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    output_root = _resolve_output_root(args.output_root)
    candidate_input_dir = _resolve_source_session(args.candidate_input_dir, DEFAULT_CANDIDATE_INPUT_DIR, "candidate input dir")
    family_session = _resolve_source_session(args.source_family_session, DEFAULT_FAMILY_SESSION, "family session")
    context_session = _resolve_source_session(args.source_context_session, DEFAULT_CONTEXT_SESSION, "context session")
    shape_session = _resolve_source_session(args.source_shape_session, DEFAULT_SHAPE_SESSION, "shape session")
    freeze_session = _resolve_source_session(args.source_freeze_session, DEFAULT_FREEZE_SESSION, "freeze session")
    write_artifacts(
        output_root=output_root,
        session_id=args.session_id,
        candidate_input_dir=candidate_input_dir,
        family_session=family_session,
        context_session=context_session,
        shape_session=shape_session,
        freeze_session=freeze_session,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
