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
    _safe_float,
    _safe_int,
    _write_json,
)


DEFAULT_PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac")
DEFAULT_CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
DEFAULT_SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
DEFAULT_FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\ma_context_shape_direct_adjustment_line\20260429T143302Z-8f34ef9d")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_two_stage_admission_context_shape_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1"
COMPARE_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_manifest_v1"
POLICY_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_policy_v1"
COVERAGE_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_coverage_v1"
MONTHLY_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_VERSION = "tradex_candidate_generation_two_stage_admission_context_shape_v1_context_comparison_v1"

TOP_K_VALUES = (5, 10, 20)
STAGE_PRIORITY = {"PRIMARY": 0, "WATCH": 1, "DOWNGRADE": 2, "EXCLUDE": 99}


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


def _load_prefilter_session(session_path: Path) -> dict[str, Any]:
    manifest = _load_json(session_path / "run_manifest.json")
    policy = _load_json(session_path / "candidate_prefilter_policy.json")
    coverage = _load_json(session_path / "candidate_prefilter_coverage_summary.json")
    comparison = _load_json(session_path / "candidate_pool_comparison.json")
    decision = _load_json(session_path / "candidate_generation_pre_filter_context_shape_v1_decision.json")
    row_parquet = session_path / "candidate_prefilter_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "policy": policy,
        "coverage": coverage,
        "comparison": comparison,
        "decision": decision,
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


def _extract_thresholds_from_prefilter(policy_payload: dict[str, Any]) -> tuple[float, float, dict[str, Any]]:
    thresholds = policy_payload.get("thresholds") or {}
    top15_threshold = _safe_float(thresholds.get("top15_score_threshold"))
    bottom15_threshold = _safe_float(thresholds.get("bottom15_score_threshold"))
    if top15_threshold is None or bottom15_threshold is None:
        raise RuntimeError("missing top15/bottom15 thresholds from prefilter policy")
    return float(top15_threshold), float(bottom15_threshold), thresholds


def _load_prefilter_rows(session_path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(session_path / "candidate_prefilter_rows.parquet")
    frame = frame.copy()
    if "score" not in frame.columns and "candidate_score" in frame.columns:
        frame["score"] = frame["candidate_score"]
    if "rank" not in frame.columns and "candidate_rank" in frame.columns:
        frame["rank"] = frame["candidate_rank"]
    frame["admission_stage"] = frame["prefilter_bucket"].map(
        {
            "KEEP_PRIMARY": "PRIMARY",
            "KEEP_WATCH": "WATCH",
            "DOWNGRADE": "DOWNGRADE",
            "EXCLUDE": "EXCLUDE",
        }
    ).fillna("WATCH")
    frame["stage_priority"] = frame["admission_stage"].map(STAGE_PRIORITY).fillna(99).astype(int)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["prefilter_bucket"] = frame["prefilter_bucket"].fillna("WATCH").astype(str)
    frame["prefilter_reason"] = frame["prefilter_reason"].apply(lambda x: x if isinstance(x, list) else [])
    frame["monthly_context"] = frame["monthly_context"].fillna("unknown").astype(str)
    frame["weekly_context"] = frame["weekly_context"].fillna("unknown").astype(str)
    frame["dominant_regime_context"] = frame["dominant_regime_context"].fillna("unknown").astype(str)
    frame["family_classification"] = frame["family_classification"].fillna("unknown").astype(str)
    frame["shape_classification"] = frame["shape_classification"].fillna("shape_missing").astype(str)
    frame["conditional_high_value"] = frame["conditional_high_value"].fillna(False).astype(bool)
    frame["stable_high_value_family"] = frame["stable_high_value_family"].fillna(False).astype(bool)
    frame["stable_bad_pick_family"] = frame["stable_bad_pick_family"].fillna(False).astype(bool)
    return frame


def _normalize_candidate_idx(frame: pd.DataFrame) -> pd.Series:
    candidate_idx = pd.to_numeric(frame["candidate_idx"], errors="coerce")
    if candidate_idx.isna().any():
        fallback = pd.Series(frame.index, index=frame.index, dtype="int64")
        candidate_idx = candidate_idx.where(~candidate_idx.isna(), fallback)
    return candidate_idx.astype(int)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if pd.isna(value):
        return default
    return bool(value)


def _sort_for_original(group: pd.DataFrame) -> pd.DataFrame:
    return group.sort_values(["score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], kind="stable")


def _sort_for_two_stage(group: pd.DataFrame) -> pd.DataFrame:
    if "stage_priority" not in group.columns:
        group = group.copy()
        group["stage_priority"] = group["admission_stage"].map(STAGE_PRIORITY).fillna(99).astype(int)
    return group.sort_values(["stage_priority", "score", "rank", "symbol", "candidate_idx"], ascending=[True, False, True, True, True], kind="stable")


def _select_pool(
    frame: pd.DataFrame,
    *,
    top_k: int,
    pool_mode: str,
) -> tuple[pd.Series, pd.DataFrame]:
    selected_indices: list[int] = []
    group_rows: list[dict[str, Any]] = []
    for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        if pool_mode == "original":
            eligible = group
            selected = _sort_for_original(eligible).head(top_k)
        elif pool_mode == "primary_only":
            eligible = group.loc[group["admission_stage"] == "PRIMARY"].copy()
            selected = _sort_for_original(eligible).head(top_k)
        elif pool_mode == "watch_only_reference":
            eligible = group.loc[group["admission_stage"] == "WATCH"].copy()
            selected = _sort_for_original(eligible).head(top_k)
        elif pool_mode == "primary_watch_backfill":
            eligible = group.loc[group["admission_stage"].isin(["PRIMARY", "WATCH", "DOWNGRADE"])].copy()
            selected = _sort_for_two_stage(eligible).head(top_k)
        else:
            raise ValueError(f"unknown pool_mode: {pool_mode}")

        selected_indices.extend(selected.index.tolist())
        stage_counts = selected["admission_stage"].value_counts().to_dict()
        group_rows.append(
            {
                "anchor_date": str(anchor_date),
                "side": str(side),
                "eligible_count": int(len(eligible)),
                "selected_count": int(len(selected)),
                "primary_selected_count": int(stage_counts.get("PRIMARY", 0)),
                "watch_selected_count": int(stage_counts.get("WATCH", 0)),
                "downgrade_selected_count": int(stage_counts.get("DOWNGRADE", 0)),
                "zero_pass_group": int(len(selected) == 0),
                "backfill_group": int(stage_counts.get("WATCH", 0) > 0 or stage_counts.get("DOWNGRADE", 0) > 0),
                "primary_coverage_rate": float(stage_counts.get("PRIMARY", 0) / max(1, top_k)),
                "watch_backfill_rate": float(stage_counts.get("WATCH", 0) / max(1, top_k)),
                "downgrade_backfill_rate": float(stage_counts.get("DOWNGRADE", 0) / max(1, top_k)),
            }
        )

    flags = pd.Series(False, index=frame.index, dtype=bool)
    if selected_indices:
        flags.loc[selected_indices] = True
    return flags, pd.DataFrame(group_rows)


def _topk_summary(frame: pd.DataFrame, *, selected_col: str, bottom15_threshold: float, top15_threshold: float) -> dict[str, Any]:
    return _aggregate_selected_rows(
        frame,
        selected_col=selected_col,
        bottom15_threshold=bottom15_threshold,
        top15_threshold=top15_threshold,
    )


def _build_pool_metrics(
    frame: pd.DataFrame,
    *,
    pool_prefix: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        metrics[str(top_k)] = _topk_summary(
            frame,
            selected_col=f"{pool_prefix}_selected_top{top_k}",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
    return metrics


def _build_delta_vs_original(
    *,
    original_metrics: dict[str, Any],
    challenger_metrics: dict[str, Any],
) -> dict[str, Any]:
    delta: dict[str, Any] = {}
    for top_k in [str(top_k) for top_k in TOP_K_VALUES]:
        delta[top_k] = {}
        for key in [
            "mean_forward_ret_20d",
            "median_forward_ret_20d",
            "mean_path_value_score_v1",
            "median_path_value_score_v1",
            "top15_capture_rate",
            "bottom15_contamination_rate",
            "bad_pick_family_contamination_rate",
        ]:
            base = original_metrics[top_k][key]
            alt = challenger_metrics[top_k][key]
            delta[top_k][key] = None if base is None or alt is None else float(alt - base)
    return delta


def _build_pool_comparison(
    *,
    frame: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    pools = {
        "original": frame,
        "primary_only": frame.loc[frame["admission_stage"].eq("PRIMARY")].copy(),
        "primary_watch_backfill": frame.loc[frame["admission_stage"].isin(["PRIMARY", "WATCH", "DOWNGRADE"])].copy(),
        "watch_only_reference": frame.loc[frame["admission_stage"].eq("WATCH")].copy(),
    }
    ranked: dict[str, pd.DataFrame] = {}
    for name, pool in pools.items():
        pool = pool.copy().reset_index(drop=False).rename(columns={"index": "source_row_index"})
        pool["ranked_index"] = pool["source_row_index"]
        ranked[name] = pool

    for name, pool in ranked.items():
        pool_flags: dict[str, pd.Series] = {}
        for top_k in TOP_K_VALUES:
            flags, _ = _select_pool(
                pool,
                top_k=top_k,
                pool_mode="original" if name == "original" else ("primary_only" if name == "primary_only" else ("watch_only_reference" if name == "watch_only_reference" else "primary_watch_backfill")),
            )
            pool_flags[f"{name}_selected_top{top_k}"] = flags
        for col, flags in pool_flags.items():
            pool[col] = flags
        ranked[name] = pool

    metrics = {
        name: {
            "candidate_count": int(len(pool)),
            "coverage_rate": float(len(pool) / max(1, len(frame))),
            "topk": _build_pool_metrics(pool, pool_prefix=name, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold),
        }
        for name, pool in ranked.items()
    }
    original_metrics = metrics["original"]["topk"]
    delta_vs_original = {
        name: _build_delta_vs_original(original_metrics=original_metrics, challenger_metrics=pool_metrics["topk"])
        for name, pool_metrics in metrics.items()
        if name != "original"
    }
    backfill_vs_primary = _build_delta_vs_original(
        original_metrics=metrics["primary_only"]["topk"],
        challenger_metrics=metrics["primary_watch_backfill"]["topk"],
    )
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
            "two_stage_admission": True,
        },
        "candidate_universe": {
            "original_row_count": int(len(frame)),
            "primary_only_row_count": int(len(pools["primary_only"])),
            "primary_watch_backfill_row_count": int(len(pools["primary_watch_backfill"])),
            "watch_only_reference_row_count": int(len(pools["watch_only_reference"])),
            "primary_only_coverage_rate": float(len(pools["primary_only"]) / max(1, len(frame))),
            "primary_watch_backfill_coverage_rate": float(len(pools["primary_watch_backfill"]) / max(1, len(frame))),
            "watch_only_reference_coverage_rate": float(len(pools["watch_only_reference"]) / max(1, len(frame))),
        },
        "pools": metrics,
        "delta_vs_original": delta_vs_original,
        "primary_only_vs_backfill_delta": backfill_vs_primary,
    }


def _group_rows_for_comparison(
    *,
    frame: pd.DataFrame,
    selected_prefix: str,
    original_top_k: int,
    selected_top_k: int,
    group_cols: list[str],
    bottom15_threshold: float,
    top15_threshold: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group_key, group in frame.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        original = _topk_summary(
            group,
            selected_col=f"original_selected_top{original_top_k}",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
        challenger = _topk_summary(
            group,
            selected_col=f"{selected_prefix}_selected_top{selected_top_k}",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
        rows.append(
            {
                **{str(col): value for col, value in zip(group_cols, group_key)},
                "original": original,
                "challenger": challenger,
                "delta": {
                    "mean_forward_ret_20d": None
                    if original["mean_forward_ret_20d"] is None or challenger["mean_forward_ret_20d"] is None
                    else float(challenger["mean_forward_ret_20d"] - original["mean_forward_ret_20d"]),
                    "median_forward_ret_20d": None
                    if original["median_forward_ret_20d"] is None or challenger["median_forward_ret_20d"] is None
                    else float(challenger["median_forward_ret_20d"] - original["median_forward_ret_20d"]),
                    "mean_path_value_score_v1": None
                    if original["mean_path_value_score_v1"] is None or challenger["mean_path_value_score_v1"] is None
                    else float(challenger["mean_path_value_score_v1"] - original["mean_path_value_score_v1"]),
                    "median_path_value_score_v1": None
                    if original["median_path_value_score_v1"] is None or challenger["median_path_value_score_v1"] is None
                    else float(challenger["median_path_value_score_v1"] - original["median_path_value_score_v1"]),
                    "bottom15_contamination_rate": None
                    if original["bottom15_contamination_rate"] is None or challenger["bottom15_contamination_rate"] is None
                    else float(challenger["bottom15_contamination_rate"] - original["bottom15_contamination_rate"]),
                    "bad_pick_family_contamination_rate": None
                    if original["bad_pick_family_contamination_rate"] is None or challenger["bad_pick_family_contamination_rate"] is None
                    else float(challenger["bad_pick_family_contamination_rate"] - original["bad_pick_family_contamination_rate"]),
                },
            }
        )
    return rows


def _summarize_group_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [row["delta"]["mean_forward_ret_20d"] for row in rows if row["delta"]["mean_forward_ret_20d"] is not None]
    zero_pass_group_count = int(sum(1 for row in rows if int(row["challenger"]["selected_count"]) == 0))
    return {
        "group_count": len(rows),
        "win_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] > 0)),
        "loss_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] < 0)),
        "flat_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] == 0)),
        "zero_pass_group_count": zero_pass_group_count,
        "zero_pass_group_rate": float(zero_pass_group_count / max(1, len(rows))),
        "worst_delta_mean_forward_ret_20d": None if not deltas else float(min(deltas)),
        "best_delta_mean_forward_ret_20d": None if not deltas else float(max(deltas)),
    }


def _build_monthly_comparison(
    *,
    frame: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": MONTHLY_SCHEMA_VERSION, "generated_at": _utc_now(), "topk": {}}
    pools = {
        "primary_only": frame.loc[frame["admission_stage"].eq("PRIMARY")].copy(),
        "primary_watch_backfill": frame.loc[frame["admission_stage"].isin(["PRIMARY", "WATCH", "DOWNGRADE"])].copy(),
        "watch_only_reference": frame.loc[frame["admission_stage"].eq("WATCH")].copy(),
    }
    for top_k in TOP_K_VALUES:
        top_key = str(top_k)
        top_entry: dict[str, Any] = {}
        original_named = frame.copy()
        original_named[f"original_selected_top{top_k}"] = original_named[f"original_selected_top{top_k}"].fillna(False).astype(bool)
        for pool_name, pool_frame in pools.items():
            pool_named = pool_frame.copy()
            rows: list[dict[str, Any]] = []
            for month in sorted(frame["month_bucket"].dropna().astype(str).unique().tolist()):
                orig_group = original_named.loc[original_named["month_bucket"].astype(str) == month].copy()
                pool_group = pool_named.loc[pool_named["month_bucket"].astype(str) == month].copy()
                orig_sel = _topk_summary(orig_group, selected_col=f"original_selected_top{top_k}", bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
                pool_sel = _topk_summary(pool_group, selected_col=f"{pool_name}_selected_top{top_k}", bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
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
            top_entry[pool_name] = {
                "rows": rows,
                "summary": _summarize_group_rows(rows),
            }
        result["topk"][top_key] = top_entry
    return result


def _build_context_comparison(
    *,
    frame: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": CONTEXT_SCHEMA_VERSION, "generated_at": _utc_now(), "topk": {}}
    groups = [
        ("monthly_weekly", ["monthly_context", "weekly_context"]),
        ("dominant_regime_context", ["dominant_regime_context"]),
    ]
    pools = {
        "primary_only": frame.loc[frame["admission_stage"].eq("PRIMARY")].copy(),
        "primary_watch_backfill": frame.loc[frame["admission_stage"].isin(["PRIMARY", "WATCH", "DOWNGRADE"])].copy(),
        "watch_only_reference": frame.loc[frame["admission_stage"].eq("WATCH")].copy(),
    }
    for top_k in TOP_K_VALUES:
        top_key = str(top_k)
        top_entry: dict[str, Any] = {}
        for group_name, group_cols in groups:
            group_entry: dict[str, Any] = {}
            for pool_name, pool_frame in pools.items():
                rows = _group_rows_for_comparison(
                    frame=frame,
                    selected_prefix=pool_name,
                    original_top_k=top_k,
                    selected_top_k=top_k,
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
    primary_only = deltas["primary_only"]
    backfill = deltas["primary_watch_backfill"]
    watch_only = deltas["watch_only_reference"]
    strict_top5 = primary_only["5"]["mean_path_value_score_v1"]
    strict_top10 = primary_only["10"]["mean_path_value_score_v1"]
    backfill_top5 = backfill["5"]["mean_path_value_score_v1"]
    backfill_top10 = backfill["10"]["mean_path_value_score_v1"]
    backfill_ret5 = backfill["5"]["mean_forward_ret_20d"]
    backfill_ret10 = backfill["10"]["mean_forward_ret_20d"]
    backfill_bottom15 = backfill["5"]["bottom15_contamination_rate"]
    backfill_bottom15_top10 = backfill["10"]["bottom15_contamination_rate"]
    coverage = pool_comparison["candidate_universe"]
    backfill_improves_path = (
        backfill_top5 is not None
        and backfill_top10 is not None
        and backfill_top5 > 0
        and backfill_top10 > 0
    )
    backfill_improves_return = (
        backfill_ret5 is not None
        and backfill_ret10 is not None
        and backfill_ret5 >= 0
        and backfill_ret10 >= 0
    )
    backfill_bottom15_ok = (
        backfill_bottom15 is not None
        and backfill_bottom15_top10 is not None
        and backfill_bottom15 <= 0
        and backfill_bottom15_top10 <= 0
    )
    if backfill_improves_path and backfill_improves_return and backfill_bottom15_ok:
        decision = "keep"
        reason = "primary_watch_backfill_recovers_quality_without_score_adjustment"
    elif coverage["primary_watch_backfill_coverage_rate"] < 0.15 or (
        backfill_top5 is not None and backfill_top10 is not None and backfill_top5 == 0 and backfill_top10 == 0
    ):
        decision = "drop"
        reason = "two_stage_admission_is_too_sparse_or_topk_did_not_move"
    else:
        decision = "hold"
        reason = "two_stage_signal_exists_but_top5_or_bottom15_remains_mixed"
    if strict_top5 is not None and backfill_top5 is not None and backfill_top5 > strict_top5:
        reason = reason + "_backfill_beats_primary_only_top5"
    if strict_top10 is not None and backfill_top10 is not None and backfill_top10 > strict_top10:
        reason = reason + "_backfill_beats_primary_only_top10"
    strict_bottom15 = primary_only["5"]["bottom15_contamination_rate"]
    if strict_bottom15 is not None and backfill_bottom15 is not None and backfill_bottom15 > strict_bottom15:
        reason = reason + "_backfill_worsens_bottom15"
    if watch_only["5"]["mean_path_value_score_v1"] is not None and backfill_top5 is not None and watch_only["5"]["mean_path_value_score_v1"] > backfill_top5:
        reason = reason + "_watch_only_outperforms_backfill_top5"
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
    prefilter_session: Path,
    candidate_input_dir: Path,
    family_session: Path,
    context_session: Path,
    shape_session: Path,
    freeze_session: Path,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    prefilter_payload = _load_prefilter_session(prefilter_session)
    freeze_payload = _load_freeze_session(freeze_session)
    top15_threshold, bottom15_threshold, source_thresholds = _extract_thresholds_from_prefilter(prefilter_payload["policy"])
    prefilter_rows = _load_prefilter_rows(prefilter_session)
    prefilter_rows = _apply_anchor_limit(prefilter_rows, limit_anchor_dates)
    raw_candidate_rows = _load_candidate_rows(candidate_input_dir)
    raw_candidate_rows = _apply_anchor_limit(raw_candidate_rows, limit_anchor_dates)
    if len(raw_candidate_rows.drop_duplicates(["anchor_date", "symbol", "side"], keep="first")) != len(prefilter_rows):
        raise RuntimeError("candidate row counts do not reconcile between raw candidate snapshot and prefilter rows")

    frame = prefilter_rows.copy().reset_index(drop=True)
    frame["stage_priority"] = frame["admission_stage"].map(STAGE_PRIORITY).fillna(99).astype(int)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce").fillna(-1e9)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce").fillna(1_000_000).astype(int)
    frame["candidate_idx"] = _normalize_candidate_idx(frame)
    frame["month_bucket"] = frame["month_bucket"].fillna(frame["anchor_date"].astype(str).str.slice(0, 7)).astype(str)
    frame["selected_keys_note"] = frame["prefilter_bucket"].astype(str) + ":" + frame["admission_stage"].astype(str)

    # Build selection flags and per-group coverage stats.
    for pool_mode, prefix in {
        "original": "original",
        "primary_only": "primary_only",
        "primary_watch_backfill": "primary_watch_backfill",
        "watch_only_reference": "watch_only_reference",
    }.items():
        for top_k in TOP_K_VALUES:
            flags, group_rows = _select_pool(frame, top_k=top_k, pool_mode=pool_mode)
            frame[f"{prefix}_selected_top{top_k}"] = flags.fillna(False).astype(bool)
            if pool_mode == "primary_watch_backfill":
                frame[f"{prefix}_primary_available_top{top_k}"] = False
        if pool_mode == "primary_watch_backfill":
            # Store the group-level coverage stats only once per top-k in the final summary below.
            pass

    original_selected = frame.copy()
    primary_only_selected = frame.copy()
    backfill_selected = frame.copy()
    watch_only_selected = frame.copy()

    candidate_pool_comparison = _build_pool_comparison(
        frame=frame,
        bottom15_threshold=float(bottom15_threshold),
        top15_threshold=float(top15_threshold),
    )
    monthly_comparison = _build_monthly_comparison(
        frame=frame,
        bottom15_threshold=float(bottom15_threshold),
        top15_threshold=float(top15_threshold),
    )
    context_comparison = _build_context_comparison(
        frame=frame,
        bottom15_threshold=float(bottom15_threshold),
        top15_threshold=float(top15_threshold),
    )

    stage_summary: dict[str, Any] = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_sessions": {
            "prefilter_session_id": prefilter_session.name,
            "family_session_id": family_session.name,
            "context_session_id": context_session.name,
            "shape_session_id": shape_session.name,
            "freeze_session_id": freeze_session.name,
        },
        "no_lookahead_inherited": bool(
            frame.loc[frame["admission_stage"].ne("EXCLUDE"), "monthly_context_no_lookahead"].fillna(False).astype(bool).all()
            and frame.loc[frame["admission_stage"].ne("EXCLUDE"), "weekly_context_no_lookahead"].fillna(False).astype(bool).all()
        ),
        "monthly_context_no_lookahead": bool(frame.loc[frame["admission_stage"].ne("EXCLUDE"), "monthly_context_no_lookahead"].fillna(False).astype(bool).all()),
                "weekly_context_no_lookahead": bool(frame.loc[frame["admission_stage"].ne("EXCLUDE"), "weekly_context_no_lookahead"].fillna(False).astype(bool).all()),
        "bucket_counts": {
            "primary_count": int((frame["admission_stage"] == "PRIMARY").sum()),
            "watch_count": int((frame["admission_stage"] == "WATCH").sum()),
            "downgrade_count": int((frame["admission_stage"] == "DOWNGRADE").sum()),
            "exclude_count": int((frame["admission_stage"] == "EXCLUDE").sum()),
        },
        "group_count": int(frame.groupby(["anchor_date", "side"]).ngroups),
        "backfill_by_topk": {},
    }
    monthly_no_lookahead_mask = frame["monthly_context_no_lookahead"].notna()
    weekly_no_lookahead_mask = frame["weekly_context_no_lookahead"].notna()
    stage_summary["monthly_context_no_lookahead_missing_count"] = int((~monthly_no_lookahead_mask).sum())
    stage_summary["weekly_context_no_lookahead_missing_count"] = int((~weekly_no_lookahead_mask).sum())
    stage_summary["no_lookahead_inherited"] = bool(
        frame.loc[monthly_no_lookahead_mask, "monthly_context_no_lookahead"].fillna(False).astype(bool).all()
        and frame.loc[weekly_no_lookahead_mask, "weekly_context_no_lookahead"].fillna(False).astype(bool).all()
    )
    stage_summary["monthly_context_no_lookahead"] = bool(
        frame.loc[monthly_no_lookahead_mask, "monthly_context_no_lookahead"].fillna(False).astype(bool).all()
    ) if monthly_no_lookahead_mask.any() else False
    stage_summary["weekly_context_no_lookahead"] = bool(
        frame.loc[weekly_no_lookahead_mask, "weekly_context_no_lookahead"].fillna(False).astype(bool).all()
    ) if weekly_no_lookahead_mask.any() else False
    for top_k in TOP_K_VALUES:
        pool_flags, group_rows = _select_pool(frame, top_k=top_k, pool_mode="primary_watch_backfill")
        selected_rows = frame.loc[pool_flags].copy()
        zero_pass_group_count = int((group_rows["selected_count"] == 0).sum())
        backfill_group_count = int((group_rows["watch_selected_count"] > 0).sum())
        stage_summary["backfill_by_topk"][str(top_k)] = {
            "eligible_group_count": int(len(group_rows)),
            "zero_pass_group_count": zero_pass_group_count,
            "backfill_group_count": backfill_group_count,
            "avg_primary_coverage_per_anchor_date_side": float(group_rows["primary_selected_count"].mean()) if len(group_rows) else None,
            "avg_primary_coverage_rate": float(group_rows["primary_coverage_rate"].mean()) if len(group_rows) else None,
            "avg_watch_backfill_count": float(group_rows["watch_selected_count"].mean()) if len(group_rows) else None,
            "avg_watch_backfill_rate": float(group_rows["watch_backfill_rate"].mean()) if len(group_rows) else None,
            "avg_downgrade_backfill_count": float(group_rows["downgrade_selected_count"].mean()) if len(group_rows) else None,
            "selected_count": int(len(selected_rows)),
        }

    decision_payload = _decision_from_metrics(candidate_pool_comparison)
    decision_payload["coverage_summary"] = stage_summary
    decision_payload["source_sessions"] = stage_summary["source_sessions"]
    decision_payload["candidate_pool_counts"] = {
        "original": int(len(frame)),
        "primary_only": int((frame["admission_stage"] == "PRIMARY").sum()),
        "primary_watch_backfill": int(len(frame.loc[frame["admission_stage"].isin(["PRIMARY", "WATCH", "DOWNGRADE"])])),
        "watch_only_reference": int((frame["admission_stage"] == "WATCH").sum()),
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_prefilter_session": str(prefilter_session),
        "source_candidate_input_dir": str(candidate_input_dir),
        "source_family_session": str(family_session),
        "source_context_session": str(context_session),
        "source_shape_session": str(shape_session),
        "source_freeze_session": str(freeze_session),
        "source_session_ids": {
            "prefilter": prefilter_session.name,
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
            "two_stage_admission": True,
            "pre_filter_scope": "analysis only",
        },
        "candidate_row_counts": {
            "original_input": int(len(raw_candidate_rows)),
            "after_anchor_limit": int(len(raw_candidate_rows)),
            "prefilter_reconciled_rows": int(len(frame)),
        },
        "thresholds": {
            "top15_score_threshold": float(top15_threshold),
            "bottom15_score_threshold": float(bottom15_threshold),
            "source_thresholds": source_thresholds,
        },
        "artifact_paths": {
            "closed_line_freeze_artifact": str(freeze_session),
        },
    }

    # Row-level detail for the final parquet
    diff_rows: list[dict[str, Any]] = []
    original_keys = {
        top_k: set(map(tuple, frame.loc[frame[f"original_selected_top{top_k}"], ["anchor_date", "symbol", "side"]].astype(str).values.tolist()))
        for top_k in TOP_K_VALUES
    }
    pool_prefixes = ["primary_only", "primary_watch_backfill", "watch_only_reference"]
    for _, row in frame.iterrows():
        record = {
            "candidate_idx": int(row["candidate_idx"]),
            "anchor_date": row["anchor_date"],
            "symbol": row["symbol"],
            "side": row["side"],
            "score": float(row["score"]),
            "prefilter_bucket": row["prefilter_bucket"],
            "admission_stage": row["admission_stage"],
            "stage_priority": int(row["stage_priority"]),
            "shape_joined": _safe_bool(row.get("shape_joined", False)),
            "shape_classification": row.get("shape_classification"),
            "conditional_high_value": _safe_bool(row.get("conditional_high_value", False)),
            "monthly_context": row.get("monthly_context"),
            "weekly_context": row.get("weekly_context"),
            "family_classification": row.get("family_classification"),
            "stable_high_value_family": _safe_bool(row.get("stable_high_value_family", False)),
            "stable_bad_pick_family": _safe_bool(row.get("stable_bad_pick_family", False)),
            "dominant_regime_context": row.get("dominant_regime_context"),
            "prefilter_reason": row.get("prefilter_reason"),
            "forward_ret_20d": _safe_float(row.get("forward_ret_20d")),
            "path_value_score_v1": _safe_float(row.get("path_value_score_v1")),
            "mfe_20d": _safe_float(row.get("mfe_20d")),
            "mae_20d": _safe_float(row.get("mae_20d")),
            "monthly_context_no_lookahead": _safe_bool(row.get("monthly_context_no_lookahead", False)),
            "weekly_context_no_lookahead": _safe_bool(row.get("weekly_context_no_lookahead", False)),
        }
        changed = False
        for top_k in TOP_K_VALUES:
            record[f"original_selected_top{top_k}"] = bool(row[f"original_selected_top{top_k}"])
            for prefix in pool_prefixes:
                record[f"{prefix}_selected_top{top_k}"] = bool(row[f"{prefix}_selected_top{top_k}"])
                changed = changed or (record[f"{prefix}_selected_top{top_k}"] != record[f"original_selected_top{top_k}"])
        if changed:
            diff_rows.append(record)

    return {
        "manifest": manifest_payload,
        "policy": {
            "schema_version": POLICY_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "source_artifacts": {
                "prefilter_session": str(prefilter_session),
                "family_session": str(family_session),
                "context_session": str(context_session),
                "shape_session": str(shape_session),
                "freeze_session": str(freeze_session),
            },
            "stage_rules": {
                "PRIMARY": "rank KEEP_PRIMARY rows by original score",
                "WATCH": "rank KEEP_WATCH rows by original score and use only when PRIMARY coverage is insufficient",
                "DOWNGRADE": "emergency backfill only; not used in current corpus",
                "EXCLUDE": "never admit",
            },
            "stage_order": ["PRIMARY", "WATCH", "DOWNGRADE", "EXCLUDE"],
            "freeze_reference": {
                "lineage_summary": freeze_payload["lineage_summary"].get("decision"),
                "freeze_decision": freeze_payload["freeze_decision"].get("decision"),
                "freeze_reason": freeze_payload["freeze_decision"].get("decision_reason"),
            },
            "previous_prefilter_policy": {
                "broad_pool_was_no_op": True,
                "strict_pool_row_count": int(_safe_int(prefilter_payload["coverage"]["candidate_count_strict"], 0) or 0),
                "keep_primary_count": int(_safe_int(prefilter_payload["coverage"]["join_coverage"]["keep_primary_count"], 0) or 0),
                "keep_watch_count": int(_safe_int(prefilter_payload["coverage"]["join_coverage"]["keep_watch_count"], 0) or 0),
            },
            "thresholds": {
                "top15_score_threshold": float(top15_threshold),
                "bottom15_score_threshold": float(bottom15_threshold),
                "source_thresholds": source_thresholds,
            },
        },
        "coverage": stage_summary,
        "decision": decision_payload,
        "candidate_pool_comparison": candidate_pool_comparison,
        "monthly_comparison": monthly_comparison,
        "context_comparison": context_comparison,
        "candidate_two_stage_rows": frame,
        "topk_membership_diff": pd.DataFrame(diff_rows),
    }


def write_artifacts(*, output_root: Path, session_id: str | None = None, **kwargs: Any) -> Path:
    payload = build_artifacts(**kwargs)
    final_session_id = session_id or _make_session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    _write_json(session_root / "run_manifest.json", payload["manifest"])
    _write_json(session_root / "two_stage_admission_policy.json", payload["policy"])
    _write_json(session_root / "candidate_stage_coverage_summary.json", payload["coverage"])
    _write_json(session_root / "candidate_pool_comparison.json", payload["candidate_pool_comparison"])
    _write_json(session_root / "monthly_comparison.json", payload["monthly_comparison"])
    _write_json(session_root / "context_comparison.json", payload["context_comparison"])
    _write_json(session_root / "candidate_generation_two_stage_admission_context_shape_v1_decision.json", payload["decision"])

    payload["candidate_two_stage_rows"].to_parquet(session_root / "candidate_two_stage_rows.parquet", index=False)
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
                "two_stage_admission_policy.json",
                "candidate_stage_coverage_summary.json",
                "candidate_pool_comparison.json",
                "monthly_comparison.json",
                "context_comparison.json",
                "topk_membership_diff.parquet",
                "candidate_two_stage_rows.parquet",
                "candidate_generation_two_stage_admission_context_shape_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return session_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research whether context/shape signals can drive two-stage candidate admission.")
    parser.add_argument("--prefilter-session", default=str(DEFAULT_PREFILTER_SESSION))
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
    prefilter_session = _resolve_source_session(args.prefilter_session, DEFAULT_PREFILTER_SESSION, "prefilter session")
    candidate_input_dir = _resolve_source_session(args.candidate_input_dir, DEFAULT_CANDIDATE_INPUT_DIR, "candidate input dir")
    family_session = _resolve_source_session(args.source_family_session, DEFAULT_FAMILY_SESSION, "family session")
    context_session = _resolve_source_session(args.source_context_session, DEFAULT_CONTEXT_SESSION, "context session")
    shape_session = _resolve_source_session(args.source_shape_session, DEFAULT_SHAPE_SESSION, "shape session")
    freeze_session = _resolve_source_session(args.source_freeze_session, DEFAULT_FREEZE_SESSION, "freeze session")
    write_artifacts(
        output_root=output_root,
        session_id=args.session_id,
        prefilter_session=prefilter_session,
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
