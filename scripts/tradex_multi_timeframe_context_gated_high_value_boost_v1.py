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

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    HIGH_VALUE_BOOST,
    MAX_TOTAL_BOOST,
    TOP_K_VALUES,
    _aggregate_selected_rows,
    _apply_anchor_limit,
    _load_candidate_rows,
    _load_json,
    _make_session_id,
    _progress_log,
    _rank_selection,
    _resolve_candidate_input_dir as _resolve_candidate_input_dir_impl,
    _safe_float,
    _safe_int,
    _selection_diff_keys,
    _write_json,
    _load_source_family_session,
)
from scripts.tradex_multi_timeframe_conditional_state_value_v1 import (  # noqa: E402
    CONTEXT_SCHEMA_VERSION,
    SUMMARY_SCHEMA_VERSION,
    _build_group_summary_sql,
    _classify_conditional_group,
    _count_classes,
)

DEFAULT_CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
DEFAULT_SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_PRIOR_GLOBAL_BOOST_SESSION = Path(r"G:\Tradex\ma_state_family_high_value_boost_v1\20260429T084326Z-2ae0f0de")
DEFAULT_CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\multi_timeframe_context_gated_high_value_boost_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1"
COMPARE_SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_manifest_v1"
MONTHLY_SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_COMPARE_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_context_comparison_v1"
DELTA_SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_global_boost_delta_v1"
COVERAGE_SCHEMA_VERSION = "tradex_multi_timeframe_context_gated_high_value_boost_v1_boost_coverage_v1"

CONDITIONAL_HIGH_VALUE_BOOST = 0.06
BOOST_CAP = 0.06
TOP_EXAMPLE_LIMIT = 50


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


def _load_prior_global_boost_session(prior_global_boost_session: Path) -> dict[str, Any]:
    compare = _load_json(prior_global_boost_session / "ma_state_family_high_value_boost_v1_compare.json")
    decision = _load_json(prior_global_boost_session / "ma_state_family_high_value_boost_v1_decision.json")
    coverage = _load_json(prior_global_boost_session / "boost_coverage_summary.json")
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


def _build_context_comparison(
    frame: pd.DataFrame,
    *,
    selected_suffix: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    champ_col_name = f"champion_selected_top{selected_suffix}"
    chal_col_name = f"challenger_selected_top{selected_suffix}"

    def _compare_for_group(frame_group: pd.DataFrame, group_col: str) -> dict[str, Any]:
        rows = []
        for group_value in sorted(frame_group[group_col].fillna("unknown").astype(str).unique().tolist()):
            sub = frame_group.loc[frame_group[group_col].fillna("unknown").astype(str) == group_value].copy()
            champ = _aggregate_selected_rows(
                sub,
                selected_col=champ_col_name,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            chal = _aggregate_selected_rows(
                sub,
                selected_col=chal_col_name,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            rows.append(
                {
                    group_col: group_value,
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
                        "bad_pick_family_contamination_rate": None
                        if champ["bad_pick_family_contamination_rate"] is None or chal["bad_pick_family_contamination_rate"] is None
                        else float(chal["bad_pick_family_contamination_rate"] - champ["bad_pick_family_contamination_rate"]),
                    },
                }
            )
        deltas = [row["delta"]["mean_forward_ret_20d"] for row in rows if row["delta"]["mean_forward_ret_20d"] is not None]
        return {
            "schema_version": CONTEXT_SCHEMA_COMPARE_VERSION,
            "generated_at": _utc_now(),
            "group_col": group_col,
            "rows": rows,
            "summary": {
                "group_count": int(len(rows)),
                "win_group_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] > 0)),
                "loss_group_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] < 0)),
                "flat_group_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] == 0)),
                "worst_group_delta_mean_forward_ret_20d": None if not deltas else float(min(deltas)),
                "best_group_delta_mean_forward_ret_20d": None if not deltas else float(max(deltas)),
            },
        }

    monthly_rows = _compare_for_group(frame, "monthly_context")
    weekly_rows = _compare_for_group(frame, "weekly_context")
    triple_frame = frame.copy()
    triple_frame["context_bucket"] = (
        triple_frame["monthly_context"].fillna("monthly_unknown").astype(str)
        + "|"
        + triple_frame["weekly_context"].fillna("weekly_unknown").astype(str)
    )
    combo_rows = _compare_for_group(triple_frame, "context_bucket")
    return {
        "schema_version": CONTEXT_SCHEMA_COMPARE_VERSION,
        "generated_at": _utc_now(),
        "monthly_context_rows": monthly_rows,
        "weekly_context_rows": weekly_rows,
        "context_bucket_rows": combo_rows,
    }


def _build_global_boost_delta(compare_payload: dict[str, Any], prior_boost_compare: dict[str, Any]) -> dict[str, Any]:
    delta_rows: dict[str, Any] = {}
    for top_k in map(str, TOP_K_VALUES):
        current = compare_payload["champion_vs_challenger"]["selection_only"][top_k]["delta"]
        prior = prior_boost_compare["champion_vs_challenger"]["selection_only"][top_k]["delta"]
        delta_rows[top_k] = {
            "mean_forward_ret_20d_delta_vs_prior_boost": None
            if current["mean_forward_ret_20d"] is None or prior["mean_forward_ret_20d"] is None
            else float(current["mean_forward_ret_20d"] - prior["mean_forward_ret_20d"]),
            "median_forward_ret_20d_delta_vs_prior_boost": None
            if current["median_forward_ret_20d"] is None or prior["median_forward_ret_20d"] is None
            else float(current["median_forward_ret_20d"] - prior["median_forward_ret_20d"]),
            "mean_path_value_score_v1_delta_vs_prior_boost": None
            if current["mean_path_value_score_v1"] is None or prior["mean_path_value_score_v1"] is None
            else float(current["mean_path_value_score_v1"] - prior["mean_path_value_score_v1"]),
            "median_path_value_score_v1_delta_vs_prior_boost": None
            if current["median_path_value_score_v1"] is None or prior["median_path_value_score_v1"] is None
            else float(current["median_path_value_score_v1"] - prior["median_path_value_score_v1"]),
            "top15_capture_rate_delta_vs_prior_boost": None
            if current["top15_capture_rate"] is None or prior["top15_capture_rate"] is None
            else float(current["top15_capture_rate"] - prior["top15_capture_rate"]),
            "bottom15_contamination_rate_delta_vs_prior_boost": None
            if current["bottom15_contamination_rate"] is None or prior["bottom15_contamination_rate"] is None
            else float(current["bottom15_contamination_rate"] - prior["bottom15_contamination_rate"]),
            "bad_pick_family_contamination_rate_delta_vs_prior_boost": None
            if current["bad_pick_family_contamination_rate"] is None or prior["bad_pick_family_contamination_rate"] is None
            else float(current["bad_pick_family_contamination_rate"] - prior["bad_pick_family_contamination_rate"]),
            "changed_top_members_count_delta_vs_prior_boost": {
                "top5": int(compare_payload["champion_vs_challenger"]["branching_metrics"]["changed_top5_members_count"] - prior_boost_compare["champion_vs_challenger"]["branching_metrics"]["changed_top5_members_count"]),
                "top10": int(compare_payload["champion_vs_challenger"]["branching_metrics"]["changed_top10_members_count"] - prior_boost_compare["champion_vs_challenger"]["branching_metrics"]["changed_top10_members_count"]),
                "top20": int(compare_payload["champion_vs_challenger"]["branching_metrics"]["changed_top20_members_count"] - prior_boost_compare["champion_vs_challenger"]["branching_metrics"]["changed_top20_members_count"]),
            },
        }
    return {
        "schema_version": DELTA_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "against_prior_global_boost": delta_rows,
    }


def _decision_from_compare(compare_payload: dict[str, Any], context_summary: dict[str, Any], coverage_payload: dict[str, Any]) -> tuple[str, str]:
    top5 = compare_payload["champion_vs_challenger"]["selection_only"]["5"]["delta"]
    top10 = compare_payload["champion_vs_challenger"]["selection_only"]["10"]["delta"]
    top20 = compare_payload["champion_vs_challenger"]["selection_only"]["20"]["delta"]
    top5_improves = (top5["mean_path_value_score_v1"] is not None and top5["mean_path_value_score_v1"] > 0) or (
        top5["mean_forward_ret_20d"] is not None and top5["mean_forward_ret_20d"] >= 0
    )
    top10_improves = (top10["mean_path_value_score_v1"] is not None and top10["mean_path_value_score_v1"] > 0) or (
        top10["mean_forward_ret_20d"] is not None and top10["mean_forward_ret_20d"] >= 0
    )
    bottom15_ok = all(
        delta is None or delta <= 0.001
        for delta in (
            top5["bottom15_contamination_rate"],
            top10["bottom15_contamination_rate"],
            top20["bottom15_contamination_rate"],
        )
    )
    top5_regress = (top5["mean_forward_ret_20d"] is not None and top5["mean_forward_ret_20d"] < 0) or (
        top5["mean_path_value_score_v1"] is not None and top5["mean_path_value_score_v1"] < 0
    )
    if top5_improves and top10_improves and bottom15_ok and not top5_regress and context_summary["triple_level"]["class_counts"]["conditional_high_value"] >= 1000:
        return "keep", "conditional_high_value_gate_translates_to_topk_improvement"
    if top10_improves or coverage_payload.get("boost_applied_rows", 0) > 0:
        return "hold", "context_gated_signal_exists_but_top5_or_bottom15_remains_mixed"
    return "drop", "context_gated_boost_does_not_move_topk"


def run_multi_timeframe_context_gated_high_value_boost_v1(
    *,
    source_context_session: str | Path | None = None,
    source_family_session: str | Path | None = None,
    prior_global_boost_session: str | Path | None = None,
    candidate_input_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = DEFAULT_LIMIT_ANCHOR_DATES,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    context_session_path = _resolve_context_session(source_context_session)
    source_family_session_path = _resolve_source_family_session(source_family_session)
    prior_global_boost_session_path = _resolve_prior_global_boost_session(prior_global_boost_session)
    candidate_input_path = _resolve_candidate_input_dir(candidate_input_dir)
    output_root_path = _resolve_output_root(output_root)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(
        f"start context_session={context_session_path} source_family={source_family_session_path} prior_global_boost={prior_global_boost_session_path} candidate_input={candidate_input_path} out_root={output_root_path} session={session_id}"
    )

    context_payloads = _load_context_session(context_session_path)
    source_family_payloads = _load_source_family_session(source_family_session_path)
    prior_global_boost_payloads = _load_prior_global_boost_session(prior_global_boost_session_path)
    candidate_rows = _load_candidate_rows(candidate_input_path)
    candidate_rows = _apply_anchor_limit(candidate_rows, limit_anchor_dates)
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
    if triple_class_counts.get("conditional_high_value", 0) != int(context_payloads["classification"]["triple_level"]["class_counts"]["conditional_high_value"]):
        raise RuntimeError("triple-level conditional_high_value count does not match authoritative context artifact")

    conn = duckdb.connect()
    try:
        conn.register("candidate_input", candidate_rows)
        conn.register("high_value_gate", high_value_gate)
        conn.execute(f"CREATE TEMP VIEW context_rows AS SELECT * FROM read_parquet('{context_payloads['row_parquet'].as_posix()}')")
        cand_sql = f"""
        WITH joined AS (
            SELECT
                c.candidate_idx,
                c.anchor_date,
                c.trade_date,
                c.month_bucket,
                c.side,
                c.symbol,
                c.rank,
                c.score,
                c.market_regime_bucket,
                c.champion_score,
                c.champion_rank,
                r.state_family_id,
                r.position_state_id,
                r.family_classification,
                r.stable_high_value_family,
                r.stable_bad_pick_family,
                r.regime_dependent_family,
                r.unstable_or_sparse_family,
                r.neutral_family,
                r.family_regime_context,
                r.family_bad_pick_regime,
                r.dominant_regime_context,
                r.family_sample_count,
                r.family_unique_symbol_count,
                r.family_month_count,
                r.family_mean_forward_ret_5d,
                r.family_mean_forward_ret_10d,
                r.family_mean_forward_ret_20d,
                r.family_median_forward_ret_20d,
                r.family_mean_mfe_20d,
                r.family_mean_mae_20d,
                r.family_mean_path_value_score_v1,
                r.family_median_path_value_score_v1,
                r.family_plus5_before_minus5_rate,
                r.family_minus5_before_plus5_rate,
                r.family_top15_rate,
                r.family_bottom15_rate,
                r.family_months_observed,
                r.family_positive_month_rate,
                r.family_worst_month_mean_path_value,
                r.family_best_month_mean_path_value,
                r.family_mean_monthly_path_value,
                r.family_std_monthly_path_value,
                r.family_month_sample_count,
                r.family_regime_count,
                r.family_regime_consistency_score,
                r.family_score_spread,
                r.entry_next_open,
                r.entry_day_close,
                r.forward_window_days,
                r.forward_ret_5d,
                r.forward_ret_10d,
                r.forward_ret_20d,
                r.mfe_20d,
                r.mae_20d,
                r.days_to_mfe_20d,
                r.days_to_mae_20d,
                r.days_to_positive_close,
                r.days_to_plus_3pct,
                r.days_to_plus_5pct,
                r.days_to_minus_3pct,
                r.days_to_minus_5pct,
                r.hit_plus_5_before_minus_5,
                r.hit_minus_5_before_plus_5,
                r.hit_plus_3_before_minus_3,
                r.hit_minus_3_before_plus_3,
                r.hit_plus_1atr_before_minus_1atr,
                r.mfe_atr_20d,
                r.mae_atr_20d,
                r.close_above_entry_days_20d,
                r.close_below_entry_days_20d,
                r.path_value_score_v1,
                r.monthly_context,
                r.monthly_context_date,
                r.monthly_context_source,
                r.monthly_context_no_lookahead,
                r.weekly_context,
                r.weekly_context_date,
                r.weekly_context_source,
                r.weekly_context_no_lookahead,
                r.top15_label,
                r.bottom15_label,
                CASE WHEN g.state_family_id IS NOT NULL THEN TRUE ELSE FALSE END AS conditional_high_value,
                CASE WHEN g.state_family_id IS NOT NULL THEN {CONDITIONAL_HIGH_VALUE_BOOST} ELSE 0.0 END AS score_adjustment
            FROM candidate_input c
            LEFT JOIN context_rows r
              ON r.code = c.symbol AND r.trade_date = c.trade_date
            LEFT JOIN high_value_gate g
              ON g.monthly_context = r.monthly_context
             AND g.weekly_context = r.weekly_context
             AND g.state_family_id = r.state_family_id
        )
        SELECT
            *,
            score + score_adjustment AS challenger_score
        FROM joined
        ORDER BY candidate_idx
        """
        frame = conn.execute(cand_sql).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        raise RuntimeError("no enriched candidate rows produced")

    row_count = int(len(frame))
    candidate_count = int(frame["candidate_idx"].nunique())
    context_match_rate = float(frame["state_family_id"].notna().mean())
    gate_match_rate = float(frame["conditional_high_value"].fillna(False).astype(bool).mean())
    monthly_no_lookahead_rate = float(frame["monthly_context_no_lookahead"].fillna(False).astype(bool).mean())
    weekly_no_lookahead_rate = float(frame["weekly_context_no_lookahead"].fillna(False).astype(bool).mean())

    frame = _rank_selection(frame, score_col="score", prefix="champion")
    frame = _rank_selection(frame, score_col="challenger_score", prefix="challenger")

    compare_payload: dict[str, Any] = {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_global_boost_session_path": str(prior_global_boost_session_path),
        "same_condition_contract": {
            "universe": "integrated_guarded_v1_candidate_snapshots",
            "period": "matched candidate anchor dates from the verified stress200 compare path",
            "top_k_values": list(TOP_K_VALUES),
            "cost_slippage": "inherited from source compare path; not changed in this branch",
            "artifact_detail_level": "summary json + parquet diff rows",
            "no_silent_fallback": True,
        },
        "boost_formula": {
            "conditional_high_value_boost": CONDITIONAL_HIGH_VALUE_BOOST,
            "boost_cap": BOOST_CAP,
            "conditional_high_value_definition": "triple-level conditional_high_value from the verified multi-timeframe analysis session",
            "note": "boost is applied only when monthly_context × weekly_context × daily_state_family_id is classified as conditional_high_value",
        },
        "boost_coverage": {},
        "candidate_universe": {
            "row_count": int(len(frame)),
            "anchor_count": int(frame["anchor_date"].nunique()),
            "side_count": int(frame["side"].nunique()),
            "monthly_context_count": int(frame["monthly_context"].nunique()),
            "weekly_context_count": int(frame["weekly_context"].nunique()),
        },
        "source_thresholds": {
            "top15_score_threshold": top15_threshold,
            "bottom15_score_threshold": bottom15_threshold,
            "source_threshold_keys": sorted(map(str, source_thresholds.keys())),
        },
    }

    compare_by_topk: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
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
            "selection_divergence_reason": "context_gated_high_value_boost_v1_vs_champion",
            "turnover_proxy": None if union_top20 == 0 else float((changed_top10_members_count + changed_top20_members_count) / max(1, union_top10 + union_top20)),
        },
    }

    monthly_comparison = {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "rows": {
            str(top_k): _build_context_comparison(
                frame,
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in TOP_K_VALUES
        },
    }
    context_comparison = {
        "schema_version": CONTEXT_SCHEMA_COMPARE_VERSION,
        "generated_at": _utc_now(),
        "monthly_context_level": {
            str(top_k): _build_context_comparison(
                frame,
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in TOP_K_VALUES
        },
        "weekly_context_level": {
            str(top_k): _build_context_comparison(
                frame,
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in TOP_K_VALUES
        },
        "context_bucket_level": {
            str(top_k): _build_context_comparison(
                frame.assign(context_bucket=frame["monthly_context"].fillna("monthly_unknown").astype(str) + "|" + frame["weekly_context"].fillna("weekly_unknown").astype(str)),
                selected_suffix=str(top_k),
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            for top_k in TOP_K_VALUES
        },
    }

    coverage_payload = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_rows": int(len(frame)),
        "matched_context_rows": int(frame["state_family_id"].notna().sum()),
        "unmatched_context_rows": int(len(frame) - int(frame["state_family_id"].notna().sum())),
        "matched_context_rate": None if len(frame) == 0 else float(frame["state_family_id"].notna().mean()),
        "boost_applied_rows": int(frame["conditional_high_value"].fillna(False).astype(bool).sum()),
        "boost_applied_rate": None if len(frame) == 0 else float(frame["conditional_high_value"].fillna(False).astype(bool).mean()),
        "conditional_high_value_gate_count": int(len(high_value_gate)),
        "triple_level_high_value_count": int(triple_class_counts["conditional_high_value"]),
        "monthly_context_counts": monthly_context_class_counts,
        "weekly_context_counts": weekly_context_class_counts,
        "candidate_rows_by_context_bucket": {
            bucket: int((frame["monthly_context"].fillna("monthly_unknown").astype(str) + "|" + frame["weekly_context"].fillna("weekly_unknown").astype(str) == bucket).sum())
            for bucket in sorted((frame["monthly_context"].fillna("monthly_unknown").astype(str) + "|" + frame["weekly_context"].fillna("weekly_unknown").astype(str)).unique().tolist())
        },
        "boosted_rows_by_context_bucket": {
            bucket: int(((frame["monthly_context"].fillna("monthly_unknown").astype(str) + "|" + frame["weekly_context"].fillna("weekly_unknown").astype(str) == bucket) & frame["conditional_high_value"].fillna(False).astype(bool)).sum())
            for bucket in sorted((frame["monthly_context"].fillna("monthly_unknown").astype(str) + "|" + frame["weekly_context"].fillna("weekly_unknown").astype(str)).unique().tolist())
        },
        "monthly_context_no_lookahead_rate": monthly_no_lookahead_rate,
        "weekly_context_no_lookahead_rate": weekly_no_lookahead_rate,
        "family_high_value_definition": "conditional_high_value from monthly_context × weekly_context × daily_state_family_id triple gate",
    }

    delta_payload = _build_global_boost_delta(compare_payload, prior_global_boost_payloads["compare"])
    decision_recommendation, decision_reason = _decision_from_compare(compare_payload, context_payloads["classification"], coverage_payload)
    decision_payload = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_global_boost_session_path": str(prior_global_boost_session_path),
        "baseline_metrics": context_payloads["summary"]["baseline_metrics"],
        "global_family_counts": context_payloads["decision"]["global_family_counts"],
        "triple_level_class_counts": triple_class_counts,
        "monthly_context_level_class_counts": monthly_context_class_counts,
        "weekly_context_level_class_counts": weekly_context_class_counts,
        "boost_coverage": coverage_payload,
        "global_boost_vs_context_gated_delta": delta_payload,
        "recommendation": decision_recommendation,
        "typed_reasons": [decision_reason],
    }

    output_files = {
        "run_manifest_json": session_tmp / "run_manifest.json",
        "multi_timeframe_context_gated_high_value_boost_v1_compare_json": session_tmp / "multi_timeframe_context_gated_high_value_boost_v1_compare.json",
        "multi_timeframe_context_gated_high_value_boost_v1_decision_json": session_tmp / "multi_timeframe_context_gated_high_value_boost_v1_decision.json",
        "boost_coverage_summary_json": session_tmp / "boost_coverage_summary.json",
        "global_boost_vs_context_gated_delta_json": session_tmp / "global_boost_vs_context_gated_delta.json",
        "monthly_comparison_json": session_tmp / "monthly_comparison.json",
        "context_comparison_json": session_tmp / "context_comparison.json",
        "topk_membership_diff_parquet": session_tmp / "topk_membership_diff.parquet",
        "_artifact_complete_json": session_tmp / "_ARTIFACT_COMPLETE.json",
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "prior_global_boost_session_path": str(prior_global_boost_session_path),
        "candidate_input_dir": str(candidate_input_path),
        "output_root": str(output_root_path),
        "source_artifacts": {
            "context_run_manifest_json": str(context_session_path / "run_manifest.json"),
            "context_decision_json": str(context_session_path / "multi_timeframe_conditional_state_value_v1_decision.json"),
            "context_summary_json": str(context_session_path / "conditional_state_value_summary.json"),
            "context_classification_json": str(context_session_path / "conditional_state_classification.json"),
            "context_global_vs_conditional_comparison_json": str(context_session_path / "global_vs_conditional_comparison.json"),
            "context_rows_parquet": str(context_payloads["row_parquet"]),
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "prior_global_boost_compare_json": str(prior_global_boost_session_path / "ma_state_family_high_value_boost_v1_compare.json"),
            "prior_global_boost_decision_json": str(prior_global_boost_session_path / "ma_state_family_high_value_boost_v1_decision.json"),
        },
        "output_artifacts": {
            key: str(session_final / path.name)
            for key, path in output_files.items()
        },
        "no_lookahead_inherited": bool(context_payloads["decision"].get("no_lookahead_inherited", True)),
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
        "conditional_high_value_gate_count": int(len(high_value_gate)),
        "conditional_row_count": row_count,
        "conditional_candidate_count": candidate_count,
        "matched_context_rate": context_match_rate,
        "matched_high_value_gate_rate": gate_match_rate,
        "monthly_context_count": int(frame["monthly_context"].nunique()),
        "weekly_context_count": int(frame["weekly_context"].nunique()),
        "authoritative_source": "conditional_state_rows.parquet from the verified multi-timeframe conditional analysis session plus triple-level conditional_high_value gate",
    }

    _write_json(output_files["run_manifest_json"], manifest_payload)
    _write_json(output_files["multi_timeframe_context_gated_high_value_boost_v1_compare_json"], compare_payload)
    _write_json(output_files["multi_timeframe_context_gated_high_value_boost_v1_decision_json"], decision_payload)
    _write_json(output_files["boost_coverage_summary_json"], coverage_payload)
    _write_json(output_files["global_boost_vs_context_gated_delta_json"], delta_payload)
    _write_json(output_files["monthly_comparison_json"], monthly_comparison)
    _write_json(output_files["context_comparison_json"], context_comparison)

    diff_frame = frame.copy()
    diff_frame["selection_changed_top5"] = diff_frame["champion_selected_top5"] != diff_frame["challenger_selected_top5"]
    diff_frame["selection_changed_top10"] = diff_frame["champion_selected_top10"] != diff_frame["challenger_selected_top10"]
    diff_frame["selection_changed_top20"] = diff_frame["champion_selected_top20"] != diff_frame["challenger_selected_top20"]
    diff_frame.to_parquet(output_files["topk_membership_diff_parquet"], index=False)

    _write_json(output_files["_artifact_complete_json"], {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "validated": True,
    })

    for json_path in (
        output_files["run_manifest_json"],
        output_files["multi_timeframe_context_gated_high_value_boost_v1_compare_json"],
        output_files["multi_timeframe_context_gated_high_value_boost_v1_decision_json"],
        output_files["boost_coverage_summary_json"],
        output_files["global_boost_vs_context_gated_delta_json"],
        output_files["monthly_comparison_json"],
        output_files["context_comparison_json"],
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
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "prior_global_boost_session_id": prior_global_boost_session_path.name,
        "row_count": row_count,
        "candidate_count": candidate_count,
        "conditional_high_value_gate_count": int(len(high_value_gate)),
        "matched_context_rate": context_match_rate,
        "matched_high_value_gate_rate": gate_match_rate,
        "decision": decision_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX multi-timeframe context-gated high-value boost analysis.")
    parser.add_argument("--source-context-session", default=str(DEFAULT_CONTEXT_SESSION))
    parser.add_argument("--source-family-session", default=str(DEFAULT_SOURCE_FAMILY_SESSION))
    parser.add_argument("--prior-global-boost-session", default=str(DEFAULT_PRIOR_GLOBAL_BOOST_SESSION))
    parser.add_argument("--candidate-input-dir", default=str(DEFAULT_CANDIDATE_INPUT_DIR))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    args = parser.parse_args(argv)

    run_multi_timeframe_context_gated_high_value_boost_v1(
        source_context_session=args.source_context_session,
        source_family_session=args.source_family_session,
        prior_global_boost_session=args.prior_global_boost_session,
        candidate_input_dir=args.candidate_input_dir,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
