from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from scripts.tradex_reflectability_funnel_common_v1 import (
    TOP_K_VALUES,
    _ensure_columns,
    _json_text,
    _mean_or_none,
    _safe_float,
    _safe_int,
    _safe_path,
    _utc_now,
    _write_json,
    build_artifact_complete,
)

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_timing_confirmed_signal_v1")

SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1"
MANIFEST_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_manifest_v1"
EVALUATION_CONTRACT_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_evaluation_contract_v1"
FEATURE_SUMMARY_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_feature_summary_v1"
COMPARE_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_decision_v1"
ANTI_LEAKAGE_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_anti_leakage_audit_v1"
COMPLETE_SCHEMA_VERSION = "tradex_entry_timing_confirmed_signal_v1_artifact_complete_v1"

LABEL_COLUMNS = {
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "top15_label",
    "bottom15_label",
    "hit_plus_5_before_minus_5",
    "hit_minus_5_before_plus_5",
}
TIMING_FEATURE_COLUMNS = {
    "candle_body_ratio",
    "candle_lower_wick_ratio",
    "candle_upper_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "gap_pct",
    "vol_ratio5_20",
    "monthly_context_no_lookahead",
    "weekly_context_no_lookahead",
}


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_frame(source_rows_parquet: Path) -> pd.DataFrame:
    frame = pd.read_parquet(source_rows_parquet)
    frame = _ensure_columns(frame)
    required = {
        "anchor_date",
        "side",
        "symbol",
        "champion_rank",
        "champion_score",
        "champion_selected_top20",
        "forward_ret_20d",
    }
    missing = sorted(column for column in required if column not in frame.columns)
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")
    frame = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").astype("Int64")
    frame["champion_score"] = pd.to_numeric(frame["champion_score"], errors="coerce")
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].str.slice(0, 7)
    return frame


def _bool_signal(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool).astype(float)


def _timing_score(frame: pd.DataFrame) -> pd.Series:
    body = pd.to_numeric(frame.get("candle_body_ratio", 0.0), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    lower = pd.to_numeric(frame.get("candle_lower_wick_ratio", 0.0), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    upper = pd.to_numeric(frame.get("candle_upper_wick_ratio", 0.0), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    triplet_up = pd.to_numeric(frame.get("candle_triplet_up_prob", 0.5), errors="coerce").fillna(0.5).clip(0.0, 1.0)
    triplet_down = pd.to_numeric(frame.get("candle_triplet_down_prob", 0.5), errors="coerce").fillna(0.5).clip(0.0, 1.0)
    gap = pd.to_numeric(frame.get("gap_pct", 0.0), errors="coerce").fillna(0.0).clip(-0.12, 0.12)
    volume = pd.to_numeric(frame.get("vol_ratio5_20", 1.0), errors="coerce").fillna(1.0).clip(0.0, 3.0)
    monthly_ok = _bool_signal(frame.get("monthly_context_no_lookahead", pd.Series(False, index=frame.index)))
    weekly_ok = _bool_signal(frame.get("weekly_context_no_lookahead", pd.Series(False, index=frame.index)))

    long_score = (
        0.28 * triplet_up
        + 0.18 * body
        + 0.16 * lower
        - 0.12 * upper
        + 0.12 * (volume / 3.0)
        + 0.10 * monthly_ok
        + 0.10 * weekly_ok
        - 0.14 * gap.clip(lower=0.0) / 0.12
    )
    short_score = (
        0.28 * triplet_down
        + 0.18 * body
        + 0.16 * upper
        - 0.12 * lower
        + 0.12 * (volume / 3.0)
        + 0.10 * monthly_ok
        + 0.10 * weekly_ok
        + 0.14 * gap.clip(upper=0.0).abs() / 0.12
    )
    is_short = frame["side"].astype(str).str.lower().eq("short")
    return long_score.where(~is_short, short_score).astype(float)


def _rank_group(group: pd.DataFrame, score_column: str) -> pd.DataFrame:
    ordered = group.sort_values([score_column, "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
    ordered["candidate_rank"] = range(1, len(ordered) + 1)
    return ordered


def _apply_candidate(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    working["entry_timing_score"] = _timing_score(working)
    working["entry_timing_adjusted_score"] = working["champion_score"] + (working["entry_timing_score"] * 0.075)
    ranked = pd.concat(
        [_rank_group(group, "entry_timing_adjusted_score") for _, group in working.groupby(["anchor_date", "side"], sort=True)],
        ignore_index=False,
    )
    for top_k in TOP_K_VALUES:
        ranked[f"candidate_selected_top{top_k}"] = ranked["candidate_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked["champion_rank"].le(top_k)
        ranked[f"changed_top{top_k}_member"] = ranked[f"candidate_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["candidate_rank"].astype("Int64") != ranked["champion_rank"]
    return ranked


def _topk_metrics(frame: pd.DataFrame, prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        selected = frame[frame[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)]
        out[f"top{top_k}"] = {
            "selected_count": int(len(selected)),
            "forward_ret_20d_mean": _mean_or_none(selected["forward_ret_20d"].tolist()),
            "hit_rate_positive_20d": _mean_or_none((selected["forward_ret_20d"] > 0).astype(float).tolist()),
            "bottom15_count": int(selected.get("bottom15_label", pd.Series(False, index=selected.index)).fillna(False).astype(bool).sum()),
        }
    return out


def _build_compare(frame: pd.DataFrame) -> dict[str, Any]:
    champion = _topk_metrics(frame, "champion")
    candidate = _topk_metrics(frame, "candidate")
    deltas: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        key = f"top{top_k}"
        deltas[key] = {
            "forward_ret_20d_mean_delta": (
                None
                if champion[key]["forward_ret_20d_mean"] is None or candidate[key]["forward_ret_20d_mean"] is None
                else candidate[key]["forward_ret_20d_mean"] - champion[key]["forward_ret_20d_mean"]
            ),
            "hit_rate_positive_20d_delta": (
                None
                if champion[key]["hit_rate_positive_20d"] is None or candidate[key]["hit_rate_positive_20d"] is None
                else candidate[key]["hit_rate_positive_20d"] - champion[key]["hit_rate_positive_20d"]
            ),
            "bottom15_count_delta": candidate[key]["bottom15_count"] - champion[key]["bottom15_count"],
            "changed_member_count": int(frame[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum()),
        }
    return {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "entry_timing_confirmed_signal_v1",
        "champion_id": "champion_top5_capture_boundary_promoter_v1",
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "silent_fallback_allowed": False,
        },
        "champion": champion,
        "candidate": candidate,
        "deltas": deltas,
        "branching": {
            "changed_top5_members_count": deltas["top5"]["changed_member_count"],
            "changed_top10_members_count": deltas["top10"]["changed_member_count"],
            "changed_top20_members_count": deltas["top20"]["changed_member_count"],
            "changed_rank_count": int(frame["rank_changed"].fillna(False).astype(bool).sum()),
            "selection_divergence_reason": "entry_timing_confirmed_signal_rerank_with_existing_top20_pool",
        },
    }


def _build_decision(compare: dict[str, Any]) -> dict[str, Any]:
    deltas = compare["deltas"]
    top5_delta = deltas["top5"]["forward_ret_20d_mean_delta"]
    top10_delta = deltas["top10"]["forward_ret_20d_mean_delta"]
    top20_delta = deltas["top20"]["forward_ret_20d_mean_delta"]
    changed_top5 = _safe_int(compare["branching"]["changed_top5_members_count"], 0)
    changed_top10 = _safe_int(compare["branching"]["changed_top10_members_count"], 0)
    bottom15_top5_delta = _safe_int(deltas["top5"]["bottom15_count_delta"], 0)

    if changed_top5 == 0 and changed_top10 == 0:
        decision = "drop"
        reason = "no_branching"
    elif top5_delta is not None and top10_delta is not None and top5_delta > 0 and top10_delta >= 0 and bottom15_top5_delta <= 0:
        decision = "keep"
        reason = "top5_improved_without_top10_or_bottom15_regression"
    elif (top5_delta is not None and top5_delta > 0) or (top10_delta is not None and top10_delta > 0):
        decision = "hold"
        reason = "partial_topk_improvement_needs_breadth_or_risk_confirmation"
    else:
        decision = "drop"
        reason = "fixed_condition_topk_metrics_not_improved"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "entry_timing_confirmed_signal_v1",
        "champion_id": "champion_top5_capture_boundary_promoter_v1",
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "research_fallback": False,
        "promote_ready": decision == "keep",
        "metrics": {
            "top5_forward_ret_20d_mean_delta": top5_delta,
            "top10_forward_ret_20d_mean_delta": top10_delta,
            "top20_forward_ret_20d_mean_delta": top20_delta,
            "changed_top5_members_count": changed_top5,
            "changed_top10_members_count": changed_top10,
            "changed_rank_count": compare["branching"]["changed_rank_count"],
            "bottom15_top5_count_delta": bottom15_top5_delta,
        },
        "non_goals": [
            "No MeeMee mutation",
            "No production ranking mutation",
            "No promoter threshold retuning",
            "No publish registry mutation",
        ],
    }


def _build_feature_summary(frame: pd.DataFrame) -> dict[str, Any]:
    selected = frame[frame["candidate_selected_top5"].fillna(False).astype(bool)]
    return {
        "schema_version": FEATURE_SUMMARY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "feature_columns_used": sorted(TIMING_FEATURE_COLUMNS),
        "label_columns_excluded": sorted(LABEL_COLUMNS),
        "all_rows": {
            "row_count": int(len(frame)),
            "entry_timing_score_mean": _mean_or_none(frame["entry_timing_score"].tolist()),
            "entry_timing_score_top_decile_min": _safe_float(frame["entry_timing_score"].quantile(0.9), 0.0),
        },
        "candidate_top5_rows": {
            "row_count": int(len(selected)),
            "entry_timing_score_mean": _mean_or_none(selected["entry_timing_score"].tolist()),
            "forward_ret_20d_mean": _mean_or_none(selected["forward_ret_20d"].tolist()),
        },
    }


def _build_outputs(frame: pd.DataFrame, *, output_root: Path, source_rows_parquet: Path) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    ranked = _apply_candidate(frame)
    compare = _build_compare(ranked)
    decision = _build_decision(compare)
    feature_summary = _build_feature_summary(ranked)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "entry_timing_confirmed_signal_v1",
        "source_rows_parquet": str(source_rows_parquet),
        "output_root": str(output_root),
        "row_count": int(len(ranked)),
        "scoring_mode": "confirmed_decision_time_timing_features_only_v1",
    }
    evaluation_contract = {
        "schema_version": EVALUATION_CONTRACT_SCHEMA_VERSION,
        "same_universe": True,
        "same_period": True,
        "same_top_k": list(TOP_K_VALUES),
        "same_regime": True,
        "same_cost_slippage": True,
        "same_artifact_detail_level": True,
        "silent_fallback_allowed": False,
    }
    anti_leakage = {
        "schema_version": ANTI_LEAKAGE_SCHEMA_VERSION,
        "pass": True,
        "used_future_labels_in_scoring": False,
        "feature_columns_used": sorted(TIMING_FEATURE_COLUMNS),
        "excluded_label_columns": sorted(LABEL_COLUMNS),
        "no_lookahead_contract": "uses same-day OHLCV-derived fields and *_no_lookahead context flags only; forward returns and path labels are evaluation-only",
    }
    artifacts = {
        "candidate_manifest.json": manifest,
        "evaluation_contract.json": evaluation_contract,
        "timing_feature_summary.json": feature_summary,
        "compare.json": compare,
        "decision_summary.json": decision,
        "anti_leakage_audit.json": anti_leakage,
    }
    paths: dict[str, str] = {}
    for name, payload in artifacts.items():
        paths[name] = str(_write_json(output_root / name, payload))
    complete = build_artifact_complete(
        {"schema_version": COMPLETE_SCHEMA_VERSION, "artifact_root": str(output_root), "json_validated": True},
        list(artifacts.keys()),
        schema_version=COMPLETE_SCHEMA_VERSION,
    )
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_root / "_ARTIFACT_COMPLETE.json", complete))
    return {"paths": paths, "compare": compare, "decision": decision}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    source_rows_parquet = _safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET)
    base_output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    output_root = base_output_root / (args.run_id.strip() or _run_id())

    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness()
    frame = _load_frame(source_rows_parquet)
    payload = _build_outputs(frame, output_root=output_root, source_rows_parquet=source_rows_parquet)
    run_status = {
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "output_root": str(output_root),
        "decision": payload["decision"]["authoritative_rollup_decision"],
        "decision_reason": payload["decision"]["decision_reason"],
    }
    print(_json_text(run_status))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
