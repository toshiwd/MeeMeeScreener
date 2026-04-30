from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import (  # noqa: E402
    get_rankings_freshness,
    get_runtime_stock_db_status,
)
from scripts.tradex_bad_pick_root_cause_audit_v1 import (  # noqa: E402
    BAD_OUTCOME_THRESHOLD,
    _add_outcome_labels,
    _load_policy_feature_overlay,
)
from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _load_json,
    _make_session_id,
    _safe_float,
    _safe_int,
    _write_json,
)

DEFAULT_CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac")
DEFAULT_AUDIT_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_BOUNDARY_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_POLICY_LEDGER = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\late_entry_after_extended_rise_veto_v1")
DEFAULT_CANDIDATE_SURFACE_NAME = "candidate_prefilter_rows.parquet"

SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1"
MANIFEST_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_input_resolution_v1"
POLICY_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_policy_v1"
COMPARE_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_compare_v1"
PRECISION_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_precision_recall_v1"
MONTHLY_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_context_comparison_v1"
DECISION_SCHEMA_VERSION = "tradex_late_entry_after_extended_rise_veto_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
VETO_REASON = "late_entry_after_extended_rise"


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
        return None if pd.isna(value) else value.isoformat()
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


def _resolve_source_path(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _resolve_candidate_surface_path(candidate_input_dir: Path) -> Path:
    if candidate_input_dir.is_dir():
        path = candidate_input_dir / DEFAULT_CANDIDATE_SURFACE_NAME
    else:
        path = candidate_input_dir
    if not path.exists():
        raise FileNotFoundError(f"candidate surface parquet not found: {path}")
    return path


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str)
    if "candidate_idx" in frame.columns:
        frame["candidate_idx"] = pd.to_numeric(frame["candidate_idx"], errors="coerce")
    if "candidate_rank" in frame.columns:
        frame["candidate_rank"] = pd.to_numeric(frame["candidate_rank"], errors="coerce")
    if "score" in frame.columns:
        frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    if "rank" in frame.columns:
        frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    return frame


def _load_candidate_surface(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    required = {"anchor_date", "symbol", "side", "candidate_rank", "score", "champion_selected_top5", "champion_selected_top10", "champion_selected_top20"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"candidate surface missing required columns: {missing}")
    return _normalize_frame(frame)


def _load_audit_cases(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    required = {"anchor_date", "symbol", "side", "root_cause_code", "root_cause_confidence", "top15_label", "bottom15_label"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"audit cases missing required columns: {missing}")
    frame = _normalize_frame(frame)
    frame = _add_outcome_labels(frame)
    return frame


def _load_boundary_rows(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    return _normalize_frame(frame)


def _merge_audit_annotations(frame: pd.DataFrame, audit_cases: pd.DataFrame, boundary_rows: pd.DataFrame) -> pd.DataFrame:
    audit_cols = [
        "anchor_date",
        "symbol",
        "side",
        "root_cause_code",
        "root_cause_confidence",
        "evidence_fields_used",
        "root_cause_notes",
        "missing_fields",
        "is_bad_pick",
        "is_good_pick",
        "is_neutral_pick",
        "bad_pick_scope",
        "topk_bucket",
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "dominant_regime_context",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
    ]
    audit_overlay = audit_cases[audit_cols].copy()
    merged = frame.merge(audit_overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_audit"))
    boundary_cols = [
        "anchor_date",
        "symbol",
        "side",
        "boundary_candidate_count",
        "boundary_rank_range",
        "boundary_mean_forward_ret_20d",
        "boundary_median_forward_ret_20d",
        "boundary_mean_path_value_score_v1",
        "boundary_median_path_value_score_v1",
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
    ]
    boundary_overlay = boundary_rows[[col for col in boundary_cols if col in boundary_rows.columns]].copy()
    if not boundary_overlay.empty:
        merged = merged.merge(boundary_overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_boundary"))
    merged["audit_bad_pick"] = merged["is_bad_pick"].astype("boolean").fillna(False).astype(bool)
    merged["audit_good_pick"] = merged["is_good_pick"].astype("boolean").fillna(False).astype(bool)
    merged["audit_neutral_pick"] = merged["is_neutral_pick"].astype("boolean").fillna(False).astype(bool)
    merged["audit_root_cause_code"] = merged["root_cause_code"].fillna("none").astype(str)
    merged["audit_root_cause_confidence"] = merged["root_cause_confidence"].fillna("unknown").astype(str)
    merged["audit_is_top15_outcome"] = merged["top15_label"].fillna(False).astype(bool)
    merged["audit_is_bottom15_outcome"] = merged["bottom15_label"].fillna(False).astype(bool)
    merged["audit_is_materially_negative"] = pd.to_numeric(merged["forward_ret_20d"], errors="coerce").fillna(0.0) <= BAD_OUTCOME_THRESHOLD
    return merged


def _late_entry_veto_mask(frame: pd.DataFrame) -> pd.Series:
    monthly = frame["monthly_context"].fillna("unknown").astype(str)
    weekly = frame["weekly_context"].fillna("unknown").astype(str)
    daily = frame.get("daily_main_state_ctx", pd.Series([None] * len(frame), index=frame.index)).fillna("unknown").astype(str)
    regime = frame["dominant_regime_context"].fillna("unknown").astype(str)
    side = frame["side"].fillna("unknown").astype(str)
    dist_ma20 = pd.to_numeric(frame.get("dist_ma20_pct"), errors="coerce")
    dist_ma60 = pd.to_numeric(frame.get("dist_ma60_pct"), errors="coerce")
    monthly_lookahead = frame.get("monthly_context_no_lookahead", pd.Series([False] * len(frame), index=frame.index)).fillna(False).astype(bool)
    weekly_lookahead = frame.get("weekly_context_no_lookahead", pd.Series([False] * len(frame), index=frame.index)).fillna(False).astype(bool)
    return (
        side.eq("long")
        & regime.eq("C:risk_on_trend")
        & monthly_lookahead
        & weekly_lookahead
        & monthly.eq("monthly_overextended")
        & weekly.eq("weekly_overextended")
        & (
            (daily.isin({"daily_reversal_up_candidate", "daily_up_mid"}))
            | (dist_ma20 >= 0.04)
            | (dist_ma60 >= 0.06)
        )
    )


def _sort_columns(frame: pd.DataFrame) -> list[str]:
    columns = []
    if "score" in frame.columns:
        columns.append("score")
    elif "candidate_score" in frame.columns:
        columns.append("candidate_score")
    for candidate in ("candidate_rank", "rank", "candidate_idx", "symbol"):
        if candidate in frame.columns and candidate not in columns:
            columns.append(candidate)
    return columns


def _rank_group(group: pd.DataFrame, *, mode: str, veto_col: str) -> pd.DataFrame:
    group = group.copy()
    sort_columns = _sort_columns(group)
    ascending = [False] + [True] * (len(sort_columns) - 1)
    if mode == "original":
        ranked = group.sort_values(by=sort_columns, ascending=ascending, kind="mergesort")
    elif mode == "drop":
        ranked = group.loc[~group[veto_col]].sort_values(by=sort_columns, ascending=ascending, kind="mergesort")
    elif mode == "deprioritize":
        ranked = group.assign(_late_entry_bucket=group[veto_col].fillna(False).astype(int)).sort_values(
            by=["_late_entry_bucket", *sort_columns],
            ascending=[True, *ascending],
            kind="mergesort",
        )
        ranked = ranked.drop(columns=["_late_entry_bucket"])
    else:
        raise ValueError(f"unknown ranking mode: {mode}")
    ranked = ranked.reset_index()
    ranked["variant_position"] = range(1, len(ranked) + 1)
    return ranked


def _apply_variant_ranking(frame: pd.DataFrame, *, variant_name: str, mode: str, veto_col: str) -> pd.DataFrame:
    out = frame.copy()
    out[f"{variant_name}_position"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
    for top_k in TOP_K_VALUES:
        out[f"{variant_name}_selected_top{top_k}"] = False
    if out.empty:
        return out
    for _, group in out.groupby(["anchor_date", "side"], dropna=False, sort=False):
        ranked = _rank_group(group, mode=mode, veto_col=veto_col)
        idx = ranked["index"].astype(int).tolist()
        out.loc[idx, f"{variant_name}_position"] = pd.Series(ranked["variant_position"].astype(int).values, index=idx, dtype="Int64")
        for top_k in TOP_K_VALUES:
            selected_idx = ranked.loc[ranked["variant_position"] <= top_k, "index"].astype(int).tolist()
            if selected_idx:
                out.loc[selected_idx, f"{variant_name}_selected_top{top_k}"] = True
    return out


def _selected_keys(frame: pd.DataFrame, selected_col: str) -> set[tuple[str, str, str]]:
    mask = frame[selected_col].fillna(False).astype(bool)
    keys = frame.loc[mask, ["anchor_date", "symbol", "side"]].astype(str).values.tolist()
    return {tuple(item) for item in keys}


def _selected_summary(frame: pd.DataFrame, selected_col: str) -> dict[str, Any]:
    selected = frame.loc[frame[selected_col].fillna(False).astype(bool)].copy()
    total = int(len(selected))
    if total == 0:
        return {
            "selected_count": 0,
            "mean_forward_ret_20d": None,
            "median_forward_ret_20d": None,
            "mean_path_value_score_v1": None,
            "median_path_value_score_v1": None,
            "top15_capture_rate": None,
            "bottom15_contamination_rate": None,
            "bad_pick_contamination_rate": None,
            "bad_pick_count": 0,
            "materially_negative_count": 0,
            "win_rate": None,
        }
    return {
        "selected_count": total,
        "mean_forward_ret_20d": _safe_float(selected["forward_ret_20d"].mean()),
        "median_forward_ret_20d": _safe_float(selected["forward_ret_20d"].median()),
        "mean_path_value_score_v1": _safe_float(selected["path_value_score_v1"].mean()),
        "median_path_value_score_v1": _safe_float(selected["path_value_score_v1"].median()),
        "top15_capture_rate": _safe_float(selected["audit_is_top15_outcome"].mean()),
        "bottom15_contamination_rate": _safe_float(selected["audit_is_bottom15_outcome"].mean()),
        "bad_pick_contamination_rate": _safe_float(selected["audit_bad_pick"].mean()),
        "bad_pick_count": int(selected["audit_bad_pick"].sum()),
        "materially_negative_count": int(selected["audit_is_materially_negative"].sum()),
        "win_rate": _safe_float((selected["forward_ret_20d"] > 0).mean()),
    }


def _summary_delta(challenger: dict[str, Any], original: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in [
        "mean_forward_ret_20d",
        "median_forward_ret_20d",
        "mean_path_value_score_v1",
        "median_path_value_score_v1",
        "top15_capture_rate",
        "bottom15_contamination_rate",
        "bad_pick_contamination_rate",
        "win_rate",
    ]:
        left = challenger.get(key)
        right = original.get(key)
        out[key] = None if left is None or right is None else float(left - right)
    return out


def _group_comparison(
    frame: pd.DataFrame,
    *,
    group_col: str,
    variant_prefix: str,
    top_k: int,
) -> list[dict[str, Any]]:
    original_col = f"champion_selected_top{top_k}"
    variant_col = f"{variant_prefix}_selected_top{top_k}"
    rows: list[dict[str, Any]] = []
    for group_value, group in frame.groupby(group_col, dropna=False, sort=False):
        original = _selected_summary(group, original_col)
        challenger = _selected_summary(group, variant_col)
        delta = _summary_delta(challenger, original)
        outcome = "flat"
        if delta["mean_path_value_score_v1"] is not None:
            if delta["mean_path_value_score_v1"] > 0:
                outcome = "win"
            elif delta["mean_path_value_score_v1"] < 0:
                outcome = "loss"
        rows.append(
            {
                group_col: group_value,
                "original": original,
                "challenger": challenger,
                "delta": delta,
                "outcome": outcome,
                "group_count": int(len(group)),
                "variant_selected_count": int(group[variant_col].fillna(False).astype(bool).sum()),
                "original_selected_count": int(group[original_col].fillna(False).astype(bool).sum()),
            }
        )
    return rows


def _group_comparison_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for row in rows if row.get("outcome") == "win")
    losses = sum(1 for row in rows if row.get("outcome") == "loss")
    flats = sum(1 for row in rows if row.get("outcome") == "flat")
    deltas = [row.get("delta", {}).get("mean_path_value_score_v1") for row in rows if row.get("delta", {}).get("mean_path_value_score_v1") is not None]
    return {
        "group_count": int(len(rows)),
        "win_count": int(wins),
        "loss_count": int(losses),
        "flat_count": int(flats),
        "best_group_delta_mean_path_value_score_v1": None if not deltas else float(max(deltas)),
        "worst_group_delta_mean_path_value_score_v1": None if not deltas else float(min(deltas)),
    }


def _build_variant_metrics(frame: pd.DataFrame, variant_prefix: str, *, candidate_count: int) -> dict[str, Any]:
    coverage_rate = None if len(frame) == 0 else float(candidate_count / len(frame))
    selected_summary = {
        str(top_k): _selected_summary(frame, f"{variant_prefix}_selected_top{top_k}")
        for top_k in TOP_K_VALUES
    }
    return {
        "candidate_count": candidate_count,
        "coverage_rate": coverage_rate,
        "topk": selected_summary,
    }


def _build_compare_payload(frame: pd.DataFrame) -> dict[str, Any]:
    original_metrics = {
        str(top_k): _selected_summary(frame, f"champion_selected_top{top_k}")
        for top_k in TOP_K_VALUES
    }
    variants = ["late_entry_veto_drop", "late_entry_veto_deprioritize"]
    metrics = {
        "late_entry_veto_drop": _build_variant_metrics(frame, "late_entry_veto_drop", candidate_count=int(frame.loc[~frame["late_entry_veto_flag"].fillna(False).astype(bool)].shape[0])),
        "late_entry_veto_deprioritize": _build_variant_metrics(frame, "late_entry_veto_deprioritize", candidate_count=int(len(frame))),
    }
    delta_vs_original = {
        variant: {
            str(top_k): _summary_delta(metrics[variant]["topk"][str(top_k)], original_metrics[str(top_k)])
            for top_k in TOP_K_VALUES
        }
        for variant in variants
    }

    branch = {}
    for variant in variants:
        branch[variant] = {}
        for top_k in TOP_K_VALUES:
            original_keys = _selected_keys(frame, f"champion_selected_top{top_k}")
            variant_keys = _selected_keys(frame, f"{variant}_selected_top{top_k}")
            intersection = len(original_keys & variant_keys)
            union = len(original_keys | variant_keys)
            branch[variant][str(top_k)] = {
                "intersection_count": int(intersection),
                "union_count": int(union),
                "changed_members_count": int(len(original_keys ^ variant_keys)),
                "overlap_ratio": None if union == 0 else float(intersection / union),
            }
        rank_changes = frame.loc[
            frame["champion_selected_top20"].fillna(False).astype(bool) & frame[f"{variant}_selected_top20"].fillna(False).astype(bool),
            ["candidate_rank", f"{variant}_position"],
        ].copy()
        if rank_changes.empty:
            branch[variant]["changed_rank_count"] = 0
        else:
            branch[variant]["changed_rank_count"] = int(
                (
                    pd.to_numeric(rank_changes["candidate_rank"], errors="coerce").astype("Int64")
                    != pd.to_numeric(rank_changes[f"{variant}_position"], errors="coerce").astype("Int64")
                ).sum()
            )

    return {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1_candidate_snapshots",
            "candidate_surface": "candidate_prefilter_rows.parquet",
            "score_field": "score",
            "ranking_groups": ["anchor_date", "side"],
            "top_k_values": list(TOP_K_VALUES),
            "no_silent_fallback": True,
            "pre_filter_is_analysis_only": True,
        },
        "candidate_universe": {
            "original_row_count": int(len(frame)),
            "late_entry_veto_drop_row_count": int(frame.loc[~frame["late_entry_veto_flag"]].shape[0]),
            "late_entry_veto_deprioritize_row_count": int(len(frame)),
            "late_entry_veto_rate": _safe_float(frame["late_entry_veto_flag"].mean()),
        },
        "variant_metrics": metrics,
        "delta_vs_original": delta_vs_original,
        "branching_metrics": branch,
    }


def _build_precision_recall_summary(frame: pd.DataFrame) -> dict[str, Any]:
    veto_mask = frame["late_entry_veto_flag"].fillna(False).astype(bool)
    bad_mask = frame["audit_bad_pick"].fillna(False).astype(bool)
    tp = int((veto_mask & bad_mask).sum())
    fp = int((veto_mask & ~bad_mask).sum())
    fn = int((~veto_mask & bad_mask).sum())
    vetoed_top15_lost = int(((frame["champion_selected_top5"] | frame["champion_selected_top10"]) & frame["audit_is_top15_outcome"] & ~veto_mask).sum())
    removed_bottom15 = int(((frame["champion_selected_top5"] | frame["champion_selected_top10"]) & frame["audit_is_bottom15_outcome"] & veto_mask).sum())
    return {
        "schema_version": PRECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "vetoed_candidate_count": int(veto_mask.sum()),
        "true_positive_veto_count": tp,
        "false_positive_veto_count": fp,
        "veto_precision": None if tp + fp == 0 else float(tp / (tp + fp)),
        "veto_recall_on_bad_picks": None if tp + fn == 0 else float(tp / (tp + fn)),
        "veto_recall_on_bad_pick_cohort": None if frame["audit_bad_pick"].sum() == 0 else float(tp / int(frame["audit_bad_pick"].sum())),
        "lost_top15_count": vetoed_top15_lost,
        "removed_bottom15_count": removed_bottom15,
        "matched_bad_pick_count": int(bad_mask.sum()),
        "matched_good_pick_count": int((~bad_mask & frame["champion_selected_top20"].fillna(False).astype(bool)).sum()),
    }


def _build_monthly_summary(frame: pd.DataFrame, variant_prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": MONTHLY_SCHEMA_VERSION, "generated_at": _utc_now(), "variants": {}, "summary": {}}
    for top_k in TOP_K_VALUES:
        rows = _group_comparison(frame, group_col="month_bucket", variant_prefix=variant_prefix, top_k=top_k)
        out["variants"][str(top_k)] = rows
        out["summary"][str(top_k)] = _group_comparison_summary(rows)
    return out


def _build_context_summary(frame: pd.DataFrame, variant_prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": CONTEXT_SCHEMA_VERSION, "generated_at": _utc_now(), "variants": {}, "summary": {}}
    for top_k in TOP_K_VALUES:
        out["variants"].setdefault(str(top_k), {})
        rows = _group_comparison(
            frame,
            group_col="dominant_regime_context",
            variant_prefix=variant_prefix,
            top_k=top_k,
        )
        out["variants"][str(top_k)]["dominant_regime_context"] = rows
        out["summary"][str(top_k)] = {
            "dominant_regime_context": _group_comparison_summary(rows),
        }
    return out


def _build_decision(compare_payload: dict[str, Any], precision_payload: dict[str, Any]) -> dict[str, Any]:
    drop = compare_payload["delta_vs_original"]["late_entry_veto_drop"]
    deprioritize = compare_payload["delta_vs_original"]["late_entry_veto_deprioritize"]
    drop_top5 = drop["5"]["mean_path_value_score_v1"]
    drop_top10 = drop["10"]["mean_path_value_score_v1"]
    drop_bottom15 = drop["5"]["bottom15_contamination_rate"]
    deprioritize_top5 = deprioritize["5"]["mean_path_value_score_v1"]
    deprioritize_top10 = deprioritize["10"]["mean_path_value_score_v1"]
    deprioritize_bottom15 = deprioritize["5"]["bottom15_contamination_rate"]
    if (
        (drop_top5 is not None and drop_top5 > 0)
        or (drop_top10 is not None and drop_top10 > 0)
        or (deprioritize_top5 is not None and deprioritize_top5 > 0)
        or (deprioritize_top10 is not None and deprioritize_top10 > 0)
    ) and (
        (drop_bottom15 is not None and drop_bottom15 <= 0)
        or (deprioritize_bottom15 is not None and deprioritize_bottom15 <= 0)
    ) and precision_payload["veto_precision"] is not None and precision_payload["veto_precision"] >= 0.5:
        decision = "keep"
        reason = "late_entry_veto_improves_topk_without_material_false_positive_cost"
    elif (
        (drop_top5 is not None and drop_top5 > 0)
        or (drop_top10 is not None and drop_top10 > 0)
        or (deprioritize_top5 is not None and deprioritize_top5 > 0)
        or (deprioritize_top10 is not None and deprioritize_top10 > 0)
    ) or (
        precision_payload["veto_precision"] is not None and precision_payload["veto_precision"] >= 0.35
    ):
        decision = "hold"
        reason = "late_entry_signal_exists_but_top5_or_bottom15_remains_mixed"
    else:
        decision = "drop"
        reason = "late_entry_veto_did_not_improve_same_condition_topk"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "typed_reasons": [reason],
        "primary_next_axis_root_cause": "late_entry_after_extended_rise",
        "next_single_axis_challenger_recommended": decision != "drop",
        "not_meemee_reflectable": True,
    }


def _build_policy_payload(
    *,
    audit_session: Path,
    boundary_session: Path,
    policy_ledger_path: Path,
    selected_frame: pd.DataFrame,
    late_entry_rows: pd.DataFrame,
    veto_mask: pd.Series,
) -> dict[str, Any]:
    selected_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in selected_frame.loc[selected_frame["champion_selected_top20"].fillna(False).astype(bool), ["anchor_date", "symbol", "side"]].drop_duplicates().itertuples(index=False)
    }
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_audit_session": str(audit_session),
        "source_boundary_session": str(boundary_session),
        "source_policy_ledger": str(policy_ledger_path),
        "selected_candidate_count": int(len(selected_frame)),
        "audit_bad_pick_count": int(late_entry_rows["audit_bad_pick"].sum()),
        "late_entry_bad_pick_count": int(late_entry_rows["audit_bad_pick"].sum()),
        "late_entry_veto_candidate_count": int(veto_mask.sum()),
        "late_entry_veto_bad_pick_overlap": int((veto_mask & late_entry_rows["audit_bad_pick"].fillna(False).astype(bool)).sum()),
        "late_entry_veto_good_pick_overlap": int((veto_mask & ~late_entry_rows["audit_bad_pick"].fillna(False).astype(bool)).sum()),
        "candidate_condition": {
            "side": "long",
            "dominant_regime_context": "C:risk_on_trend",
            "monthly_context": "monthly_overextended",
            "weekly_context": "weekly_overextended",
            "monthly_context_no_lookahead": True,
            "weekly_context_no_lookahead": True,
            "daily_main_state_ctx": ["daily_reversal_up_candidate", "daily_up_mid"],
            "dist_ma20_pct_min": 0.04,
            "dist_ma60_pct_min": 0.06,
        },
        "policy_source": {
            "root_cause": VETO_REASON,
            "confidence": "high",
            "evidence_fields": [
                "monthly_context",
                "weekly_context",
                "daily_main_state_ctx",
                "dist_ma20_pct",
                "dist_ma60_pct",
                "dominant_regime_context",
                "side",
            ],
            "note": "narrow point-in-time reconstruction of the audit root cause; no score adjustment used",
        },
        "selected_key_count": int(len(selected_keys)),
        "policy_ledger_rows_scanned": int(len(selected_frame)),
    }


def build_artifacts(
    *,
    candidate_input_dir: Path,
    audit_session: Path,
    boundary_session: Path,
    policy_ledger_path: Path,
    output_root: Path,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    runtime_status = get_runtime_stock_db_status()
    long_rankings = get_rankings_freshness(direction="up", risk_mode="balanced")
    short_rankings = get_rankings_freshness(direction="down", risk_mode="balanced")
    session_id = _make_session_id()

    candidate_surface_path = _resolve_candidate_surface_path(candidate_input_dir)
    candidate_surface = _load_candidate_surface(candidate_surface_path)
    if limit_anchor_dates and limit_anchor_dates > 0:
        anchors = sorted(candidate_surface["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate_surface = candidate_surface.loc[candidate_surface["anchor_date"].isin(anchors)].copy()
    audit_session = audit_session.resolve()
    boundary_session = boundary_session.resolve()

    audit_cases = _load_audit_cases(audit_session / "bad_pick_cases.parquet")
    boundary_rows = _load_boundary_rows(boundary_session / "boundary_near_miss_comparison.parquet")
    selected_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in candidate_surface.loc[candidate_surface["champion_selected_top20"].fillna(False).astype(bool), ["anchor_date", "symbol", "side"]].drop_duplicates().itertuples(index=False)
    }
    policy_overlay = _load_policy_feature_overlay(policy_ledger_path, selected_keys)
    if policy_overlay.empty:
        raise RuntimeError("policy overlay is empty; cannot reconstruct late-entry veto inputs")
    policy_overlay["anchor_date"] = policy_overlay["anchor_date"].astype(str)
    policy_overlay["symbol"] = policy_overlay["symbol"].astype(str)
    policy_overlay["side"] = policy_overlay["side"].astype(str)
    frame = candidate_surface.merge(policy_overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_policy"))
    frame = _merge_audit_annotations(frame, audit_cases, boundary_rows)
    frame = _add_outcome_labels(frame)
    frame["late_entry_veto_flag"] = _late_entry_veto_mask(frame)
    frame["late_entry_veto_reason"] = frame["late_entry_veto_flag"].map(
        lambda flag: VETO_REASON if bool(flag) else "not_late_entry_after_extended_rise"
    )
    frame["late_entry_veto_bucket"] = frame["late_entry_veto_flag"].map(lambda flag: "EXCLUDE" if bool(flag) else "KEEP")
    frame = _apply_variant_ranking(frame, variant_name="late_entry_veto_drop", mode="drop", veto_col="late_entry_veto_flag")
    frame = _apply_variant_ranking(frame, variant_name="late_entry_veto_deprioritize", mode="deprioritize", veto_col="late_entry_veto_flag")

    compare_payload = _build_compare_payload(frame)
    precision_payload = _build_precision_recall_summary(frame)
    monthly_comparison = {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "variants": {
            variant: {str(top_k): _group_comparison(frame, group_col="month_bucket", variant_prefix=variant, top_k=top_k) for top_k in TOP_K_VALUES}
            for variant in ("late_entry_veto_drop", "late_entry_veto_deprioritize")
        },
        "summary": {
            variant: {str(top_k): _group_comparison_summary(_group_comparison(frame, group_col="month_bucket", variant_prefix=variant, top_k=top_k)) for top_k in TOP_K_VALUES}
            for variant in ("late_entry_veto_drop", "late_entry_veto_deprioritize")
        },
    }
    context_comparison = {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "variants": {
            variant: {str(top_k): _group_comparison(frame, group_col="dominant_regime_context", variant_prefix=variant, top_k=top_k) for top_k in TOP_K_VALUES}
            for variant in ("late_entry_veto_drop", "late_entry_veto_deprioritize")
        },
        "summary": {
            variant: {str(top_k): _group_comparison_summary(_group_comparison(frame, group_col="dominant_regime_context", variant_prefix=variant, top_k=top_k)) for top_k in TOP_K_VALUES}
            for variant in ("late_entry_veto_drop", "late_entry_veto_deprioritize")
        },
    }
    candidate_stage_coverage = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_rows": int(len(frame)),
        "veto_candidates": int(frame["late_entry_veto_flag"].sum()),
        "veto_rate": _safe_float(frame["late_entry_veto_flag"].mean()),
        "monthly_context_coverage": _safe_float(frame["monthly_context"].fillna("unknown").astype(str).ne("unknown").mean()),
        "weekly_context_coverage": _safe_float(frame["weekly_context"].fillna("unknown").astype(str).ne("unknown").mean()),
        "policy_overlay_rows": int(len(policy_overlay)),
        "policy_overlay_coverage_rate": None if len(frame) == 0 else float(len(policy_overlay) / len(frame)),
        "audit_bad_pick_rows": int(frame["audit_bad_pick"].sum()),
        "audit_bad_pick_rate": _safe_float(frame["audit_bad_pick"].mean()),
    }
    policy_payload = _build_policy_payload(
        audit_session=audit_session,
        boundary_session=boundary_session,
        policy_ledger_path=policy_ledger_path,
        selected_frame=frame,
        late_entry_rows=frame,
        veto_mask=frame["late_entry_veto_flag"].fillna(False).astype(bool),
    )
    policy_payload["coverage_summary"] = candidate_stage_coverage
    decision_payload = _build_decision(compare_payload, precision_payload)
    decision_payload["source_sessions"] = {
        "candidate_input_dir": str(candidate_input_dir),
        "audit_session": str(audit_session),
        "boundary_session": str(boundary_session),
        "policy_ledger_path": str(policy_ledger_path),
    }
    decision_payload["runtime_status"] = runtime_status
    decision_payload["rankings_freshness"] = {
        "up": long_rankings,
        "down": short_rankings,
    }
    decision_payload["candidate_counts"] = {
        "original": int(len(candidate_surface)),
        "late_entry_veto_drop": int(frame.loc[~frame["late_entry_veto_flag"]].shape[0]),
        "late_entry_veto_deprioritize": int(len(frame)),
        "veto_candidates": int(frame["late_entry_veto_flag"].sum()),
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "selected_candidate_source": {
            "path": str(candidate_surface_path),
            "kind": "candidate_prefilter_rows_parquet",
            "reason": "authoritative row-level champion topK surface with realized outcomes and candidate universe coverage",
        },
        "rejected_alternatives": [
            {
                "path": str(DEFAULT_CANDIDATE_INPUT_DIR / "integrated_guarded_v1_candidate_snapshots.json"),
                "reason": "raw snapshot lacks the row-level audit overlays used for the veto audit",
            },
            {
                "path": str(DEFAULT_CANDIDATE_INPUT_DIR / "integrated_guarded_v1_selection_only_ledger.json"),
                "reason": "selection ledger alone does not provide the audited outcome and context fields for this veto audit",
            },
        ],
        "auxiliary_inputs": [
            {
                "path": str(policy_ledger_path),
                "reason": "point-in-time context overlay required to reconstruct the narrow late-entry veto condition",
            },
            {
                "path": str(audit_session / "bad_pick_cases.parquet"),
                "reason": "authoritative bad-pick labels and root-cause annotations",
            },
            {
                "path": str(boundary_session / "boundary_near_miss_comparison.parquet"),
                "reason": "near-miss diagnostic cross-check for the audit lineage",
            },
        ],
        "authoritative_candidate_source": str(candidate_surface_path),
        "selected_source_is_authoritative": True,
        "no_silent_fallback": True,
    }

    diff_rows = []
    for _, row in frame.iterrows():
        row_key = (str(row["anchor_date"]), str(row["symbol"]), str(row["side"]))
        changed_any = False
        record = {
            "candidate_idx": _safe_int(row.get("candidate_idx")),
            "anchor_date": row["anchor_date"],
            "symbol": row["symbol"],
            "side": row["side"],
            "score": _safe_float(row.get("score")),
            "late_entry_veto_flag": bool(row["late_entry_veto_flag"]),
            "audit_bad_pick": bool(row["audit_bad_pick"]),
            "audit_root_cause_code": row["audit_root_cause_code"],
            "audit_root_cause_confidence": row["audit_root_cause_confidence"],
            "monthly_context": row["monthly_context"],
            "weekly_context": row["weekly_context"],
            "dominant_regime_context": row["dominant_regime_context"],
            "veto_bucket": row["late_entry_veto_bucket"],
        }
        for variant in ("late_entry_veto_drop", "late_entry_veto_deprioritize"):
            for top_k in TOP_K_VALUES:
                selected = bool(row[f"{variant}_selected_top{top_k}"])
                record[f"{variant}_selected_top{top_k}"] = selected
                record[f"original_selected_top{top_k}"] = bool(row[f"champion_selected_top{top_k}"])
            if any(record[f"{variant}_selected_top{top_k}"] != record[f"original_selected_top{top_k}"] for top_k in TOP_K_VALUES):
                changed_any = True
        if changed_any:
            diff_rows.append(record)

    row_count = int(len(frame))
    veto_count = int(frame["late_entry_veto_flag"].fillna(False).astype(bool).sum())

    output_files = {
        "run_manifest": output_root / "run_manifest.json",
        "input_resolution": output_root / "input_resolution.json",
        "veto_policy": output_root / "veto_policy.json",
        "candidate_veto_rows": output_root / "candidate_veto_rows.parquet",
        "veto_pool_comparison": output_root / "veto_pool_comparison.json",
        "monthly_comparison": output_root / "monthly_comparison.json",
        "context_comparison": output_root / "context_comparison.json",
        "topk_membership_diff": output_root / "topk_membership_diff.parquet",
        "veto_precision_recall_summary": output_root / "veto_precision_recall_summary.json",
        "decision": output_root / "late_entry_after_extended_rise_veto_v1_decision.json",
        "_artifact_complete": output_root / "_ARTIFACT_COMPLETE.json",
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_candidate_surface_path": str(candidate_surface_path),
        "source_audit_session": str(audit_session),
        "source_boundary_session": str(boundary_session),
        "source_policy_ledger_path": str(policy_ledger_path),
        "output_root": str(output_root),
        "source_artifacts": {
            "candidate_prefilter_rows_parquet": str(candidate_surface_path),
            "bad_pick_cases_parquet": str(audit_session / "bad_pick_cases.parquet"),
            "boundary_near_miss_comparison_parquet": str(boundary_session / "boundary_near_miss_comparison.parquet"),
            "root_cause_taxonomy_summary_json": str(audit_session / "root_cause_taxonomy_summary.json"),
            "feature_contrast_summary_json": str(audit_session / "feature_contrast_summary.json"),
            "veto_hypothesis_backlog_json": str(audit_session / "veto_hypothesis_backlog.json"),
            "policy_trade_ledger_json": str(policy_ledger_path),
        },
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1_candidate_snapshots",
            "score_field": "score",
            "grouping": ["anchor_date", "side"],
            "top_k_values": list(TOP_K_VALUES),
            "no_silent_fallback": True,
            "ranking_adjustment_used": False,
            "single_axis": "late_entry_after_extended_rise",
        },
        "candidate_row_counts": {
            "original_input_rows": int(len(candidate_surface)),
            "after_limit_anchor_dates": int(len(candidate_surface)),
            "policy_overlay_rows": int(len(policy_overlay)),
            "late_entry_veto_candidates": veto_count,
        },
        "runtime_status": runtime_status,
        "rankings_freshness": {
            "up": long_rankings,
            "down": short_rankings,
        },
        "no_lookahead_inherited": bool(frame.loc[frame["late_entry_veto_flag"], "monthly_context_no_lookahead"].fillna(False).astype(bool).all()
            and frame.loc[frame["late_entry_veto_flag"], "weekly_context_no_lookahead"].fillna(False).astype(bool).all()),
    }
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=False)

    _write_json(session_root / "run_manifest.json", manifest_payload)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "veto_policy.json", policy_payload)
    frame.to_parquet(session_root / "candidate_veto_rows.parquet", index=False)
    _write_json(session_root / "veto_pool_comparison.json", compare_payload)
    _write_json(session_root / "monthly_comparison.json", monthly_comparison)
    _write_json(session_root / "context_comparison.json", context_comparison)
    pd.DataFrame(diff_rows).to_parquet(session_root / "topk_membership_diff.parquet", index=False)
    _write_json(session_root / "veto_precision_recall_summary.json", precision_payload)
    _write_json(session_root / "late_entry_after_extended_rise_veto_v1_decision.json", decision_payload)
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "session_id": session_id,
            "required_files": [
                "run_manifest.json",
                "input_resolution.json",
                "veto_policy.json",
                "candidate_veto_rows.parquet",
                "veto_pool_comparison.json",
                "monthly_comparison.json",
                "context_comparison.json",
                "topk_membership_diff.parquet",
                "veto_precision_recall_summary.json",
                "late_entry_after_extended_rise_veto_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "veto_policy": True,
                "candidate_veto_rows_parquet": True,
                "veto_pool_comparison": True,
                "monthly_comparison": True,
                "context_comparison": True,
                "topk_membership_diff_parquet": True,
                "veto_precision_recall_summary": True,
                "decision": True,
            },
            "row_reconciliation": {
                "candidate_rows": int(len(frame)),
                "veto_candidates": veto_count,
                "audit_bad_pick_rows": int(frame["audit_bad_pick"].sum()),
                "diff_rows": int(len(diff_rows)),
            },
        },
    )

    for path in [
        session_root / "run_manifest.json",
        session_root / "input_resolution.json",
        session_root / "veto_policy.json",
        session_root / "veto_pool_comparison.json",
        session_root / "monthly_comparison.json",
        session_root / "context_comparison.json",
        session_root / "veto_precision_recall_summary.json",
        session_root / "late_entry_after_extended_rise_veto_v1_decision.json",
        session_root / "_ARTIFACT_COMPLETE.json",
    ]:
        json.loads(path.read_text(encoding="utf-8"))
    pd.read_parquet(session_root / "candidate_veto_rows.parquet")
    pd.read_parquet(session_root / "topk_membership_diff.parquet")

    return {
        "session_id": session_id,
        "session_dir": str(session_root),
        "candidate_rows": int(len(frame)),
        "veto_candidates": veto_count,
        "decision": decision_payload,
        "compare": compare_payload,
        "precision": precision_payload,
        "policy": policy_payload,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX late-entry veto challenger")
    parser.add_argument("--candidate-input-dir", type=str, default=None)
    parser.add_argument("--source-audit-session", type=str, default=None)
    parser.add_argument("--source-boundary-session", type=str, default=None)
    parser.add_argument("--policy-ledger-path", type=str, default=None)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    candidate_input_dir = _resolve_source_path(args.candidate_input_dir, DEFAULT_CANDIDATE_INPUT_DIR, "candidate input dir")
    audit_session = _resolve_source_path(args.source_audit_session, DEFAULT_AUDIT_SESSION, "audit session")
    boundary_session = _resolve_source_path(args.source_boundary_session, DEFAULT_BOUNDARY_SESSION, "boundary session")
    policy_ledger_path = _resolve_source_path(args.policy_ledger_path, DEFAULT_POLICY_LEDGER, "policy ledger path")
    output_root = _resolve_output_root(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    result = build_artifacts(
        candidate_input_dir=candidate_input_dir,
        audit_session=audit_session,
        boundary_session=boundary_session,
        policy_ledger_path=policy_ledger_path,
        output_root=output_root,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    print(json.dumps({"session_id": result["session_id"], "session_dir": result["session_dir"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
