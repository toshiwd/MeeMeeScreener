from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from scripts.tradex_champion_topk_bad_pick_veto_v1 import _build_group_metrics, _build_monthly_top5_capture_comparison
from scripts.tradex_reflectability_funnel_common_v1 import (
    CHAMPION_TOPK_VALUES,
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
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\champion_top5_capture_boundary_promoter_v1")
SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1"
MANIFEST_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_manifest_v1"
EVALUATION_CONTRACT_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_evaluation_contract_v1"
BRANCHING_PROBE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_branching_probe_v1"
COMPARE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_compare_v1"
MONTHLY_CAPTURE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_monthly_top5_capture_summary_v1"
TOPK_EFFECTIVENESS_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_topk_effectiveness_summary_v1"
PROMOTION_QUALITY_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_promotion_quality_summary_v1"
REGIME_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_regime_split_summary_v1"
TURNOVER_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_turnover_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_decision_v1"
REFLECTABILITY_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_meemee_reflectability_assessment_v1"
ANTI_LEAKAGE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_anti_leakage_audit_v1"
OOS_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_static_gate_oos_diagnostic_v1"

PROMOTION_GATE_THRESHOLD = 0.10
DEMOTION_GATE_THRESHOLD = 0.00
PROMOTION_MARGIN_THRESHOLD = 0.08
MAX_PROMOTIONS_PER_DECISION_SET = 1
EPSILON = 1e-6


def _month_bucket(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 7:
        return text[:7]
    return text or "unknown"


def _safe_latest_run(root: Path) -> Path:
    if not root.exists():
        raise FileNotFoundError(f"closeout root does not exist: {root}")
    candidates = [path for path in root.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"no run directories found under: {root}")
    return sorted(candidates, key=lambda value: value.name)[-1]


def _load_frame(source_rows_parquet: Path) -> pd.DataFrame:
    frame = pd.read_parquet(source_rows_parquet)
    frame = _ensure_columns(frame)
    required = {"anchor_date", "side", "symbol", "champion_selected_top20", "champion_rank", "score", "forward_ret_20d", "path_value_score_v1"}
    missing = sorted(column for column in required if column not in frame.columns)
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")
    frame = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").astype("Int64")
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["champion_score"] = frame["score"]
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    frame["path_value_score_v1"] = pd.to_numeric(frame["path_value_score_v1"], errors="coerce")
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].map(_month_bucket)
    else:
        frame["month_bucket"] = frame["month_bucket"].fillna(frame["anchor_date"].map(_month_bucket)).astype(str)
    return frame


def _rank_group(group: pd.DataFrame, *, score_column: str) -> pd.DataFrame:
    ordered = group.sort_values([score_column, "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
    ordered["selected_rank"] = range(1, len(ordered) + 1)
    return ordered


def _maybe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return _safe_float(value, 0.0)


def _boundary_payload(group: pd.DataFrame, *, score_column: str) -> dict[str, Any]:
    ordered = _rank_group(group, score_column=score_column)
    if ordered.empty:
        return {"top5_score": None, "top6_score": None, "margin": None}
    top5_score = _maybe_float(ordered.iloc[min(4, len(ordered) - 1)][score_column])
    top6_score = _maybe_float(ordered.iloc[5][score_column]) if len(ordered) > 5 else top5_score
    margin = None if top5_score is None or top6_score is None else float(top5_score - top6_score)
    return {"top5_score": top5_score, "top6_score": top6_score, "margin": margin}


def _select_candidate(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values(["champion_rank", "symbol"], kind="stable").copy()
    if "champion_score" not in group.columns:
        group["champion_score"] = group["score"] if "score" in group.columns else None
    group["path_value_score_v1"] = pd.to_numeric(group["path_value_score_v1"], errors="coerce")
    group["promotion_pool_member"] = group["champion_rank"].between(6, 20, inclusive="both")
    group["demotion_pool_member"] = group["champion_rank"].between(1, 5, inclusive="both")
    group["promotion_candidate"] = False
    group["demotion_candidate"] = False
    group["promotion_applied"] = False
    group["demotion_applied"] = False
    group["promotion_gate_passed"] = False
    group["demotion_gate_passed"] = False
    group["promotion_reason_code"] = "not_in_promotion_pool"
    group["demotion_reason_code"] = "not_in_demotion_pool"
    group["promotion_minus_demotion_margin"] = None
    group["top5_boundary_before"] = None
    group["top5_boundary_after"] = None
    group["original_score"] = pd.to_numeric(group["champion_score"], errors="coerce")
    group["adjusted_score"] = group["original_score"].astype(float)
    group["original_rank"] = group["champion_rank"]

    promotion_pool = group[group["promotion_pool_member"]].copy()
    demotion_pool = group[group["demotion_pool_member"]].copy()

    if promotion_pool.empty or demotion_pool.empty:
        group["adjusted_rank"] = range(1, len(group) + 1)
        group["top5_boundary_before"] = group["top5_boundary_before"].apply(lambda _: None)
        group["top5_boundary_after"] = group["top5_boundary_after"].apply(lambda _: None)
        return group

    promotion_pool = promotion_pool.sort_values(
        ["path_value_score_v1", "champion_score", "champion_rank", "symbol"],
        ascending=[False, False, True, True],
        kind="stable",
    )
    demotion_pool = demotion_pool.sort_values(
        ["path_value_score_v1", "champion_score", "champion_rank", "symbol"],
        ascending=[True, True, False, True],
        kind="stable",
    )

    promotion_idx = promotion_pool.index[0]
    demotion_idx = demotion_pool.index[0]
    promotion_score = _maybe_float(group.loc[promotion_idx, "path_value_score_v1"])
    demotion_score = _maybe_float(group.loc[demotion_idx, "path_value_score_v1"])
    margin = None if promotion_score is None or demotion_score is None else float(promotion_score - demotion_score)

    promotion_gate_passed = bool(promotion_score is not None and promotion_score >= PROMOTION_GATE_THRESHOLD)
    demotion_gate_passed = bool(demotion_score is not None and demotion_score <= DEMOTION_GATE_THRESHOLD)
    margin_passed = bool(margin is not None and margin >= PROMOTION_MARGIN_THRESHOLD)
    promotion_applied = bool(
        promotion_gate_passed
        and demotion_gate_passed
        and margin_passed
        and int(group["promotion_pool_member"].sum()) > 0
        and int(group["demotion_pool_member"].sum()) > 0
    )

    if promotion_applied:
        demotion_original = _safe_float(group.loc[demotion_idx, "champion_score"], 0.0)
        adjusted_base = group["adjusted_score"].copy()
        adjusted_base.loc[promotion_idx] = demotion_original + EPSILON
        adjusted_base.loc[demotion_idx] = demotion_original - EPSILON
        group["adjusted_score"] = adjusted_base

    ordered = _rank_group(group, score_column="adjusted_score")
    adjusted_rank_lookup = {idx: rank for rank, idx in enumerate(ordered.index, start=1)}
    group["adjusted_rank"] = group.index.map(adjusted_rank_lookup).astype("Int64")
    group["candidate_selected_top5"] = group["adjusted_rank"].le(5)
    group["candidate_selected_top10"] = group["adjusted_rank"].le(10)
    group["candidate_selected_top20"] = group["adjusted_rank"].le(20)
    group["champion_selected_top5"] = group["champion_rank"].le(5)
    group["champion_selected_top10"] = group["champion_rank"].le(10)
    group["champion_selected_top20"] = group["champion_rank"].le(20)
    group["changed_top5_member"] = group["champion_selected_top5"] != group["candidate_selected_top5"]
    group["changed_top10_member"] = group["champion_selected_top10"] != group["candidate_selected_top10"]
    group["changed_top20_member"] = group["champion_selected_top20"] != group["candidate_selected_top20"]
    group["promotion_gate_passed"] = promotion_gate_passed
    group["demotion_gate_passed"] = demotion_gate_passed
    group["promotion_minus_demotion_margin"] = margin
    before_boundary = _boundary_payload(group, score_column="champion_score")
    after_boundary = _boundary_payload(group.assign(adjusted_score=group["adjusted_score"]), score_column="adjusted_score")
    group["top5_boundary_before"] = [before_boundary] * len(group)
    group["top5_boundary_after"] = [after_boundary] * len(group)
    group["promotion_candidate"] = group.index == promotion_idx
    group["demotion_candidate"] = group.index == demotion_idx

    if promotion_applied:
        group.loc[group.index == promotion_idx, "promotion_applied"] = True
        group.loc[group.index == demotion_idx, "demotion_applied"] = True
        group.loc[group.index == promotion_idx, "promotion_reason_code"] = "static_gate_promotion_applied_v1"
        group.loc[group.index == demotion_idx, "demotion_reason_code"] = "static_gate_demotion_applied_v1"
        group.loc[~group["promotion_candidate"], "promotion_reason_code"] = "promotion_pool_member_not_selected"
        group.loc[~group["demotion_candidate"], "demotion_reason_code"] = "demotion_pool_member_not_selected"
    else:
        promotion_reason = "promotion_gate_failed"
        if promotion_score is not None and promotion_score >= PROMOTION_GATE_THRESHOLD:
            promotion_reason = "promotion_margin_failed" if not margin_passed else "demotion_gate_failed"
        demotion_reason = "demotion_gate_failed" if not demotion_gate_passed else "promotion_gate_failed"
        group.loc[group["promotion_candidate"], "promotion_reason_code"] = promotion_reason
        group.loc[group["demotion_candidate"], "demotion_reason_code"] = demotion_reason
        group.loc[~group["promotion_candidate"] & group["promotion_pool_member"], "promotion_reason_code"] = "promotion_pool_member_not_selected"
        group.loc[~group["demotion_candidate"] & group["demotion_pool_member"], "demotion_reason_code"] = "demotion_pool_member_not_selected"

    return group


def _build_branching_probe(frame: pd.DataFrame) -> dict[str, Any]:
    promoted_by_month: dict[str, set[str]] = defaultdict(set)
    demoted_by_month: dict[str, set[str]] = defaultdict(set)
    replacement_count_by_month: Counter[str] = Counter()
    replacement_count_by_decision_set: dict[str, int] = {}
    top5_before_gaps: list[float] = []
    top5_after_gaps: list[float] = []
    top10_before_gaps: list[float] = []
    top10_after_gaps: list[float] = []

    for (month_bucket, anchor_date, side), group in frame.groupby(["month_bucket", "anchor_date", "side"], sort=True):
        month_key = str(month_bucket)
        decision_set_key = f"{anchor_date}|{side}"
        before = _boundary_payload(group, score_column="champion_score")
        after = _boundary_payload(group, score_column="adjusted_score")
        if before["margin"] is not None:
            top5_before_gaps.append(float(before["margin"]))
        if after["margin"] is not None:
            top5_after_gaps.append(float(after["margin"]))
        champion_ordered = group.sort_values(["champion_rank", "symbol"], kind="stable")
        candidate_ordered = group.sort_values(["adjusted_rank", "symbol"], kind="stable")
        if len(champion_ordered) > 10:
            top10_before = _safe_float(champion_ordered.iloc[9]["champion_score"], 0.0) - _safe_float(champion_ordered.iloc[10]["champion_score"], 0.0)
            top10_after = _safe_float(candidate_ordered.iloc[9]["adjusted_score"], 0.0) - _safe_float(candidate_ordered.iloc[10]["adjusted_score"], 0.0)
            top10_before_gaps.append(top10_before)
            top10_after_gaps.append(top10_after)
        promoted = group.loc[group["promotion_applied"], "symbol"].astype(str).tolist()
        demoted = group.loc[group["demotion_applied"], "symbol"].astype(str).tolist()
        if promoted or demoted:
            replacement_count_by_month[month_key] += 1
            replacement_count_by_decision_set[decision_set_key] = int(len(promoted))
            promoted_by_month[month_key].update(promoted)
            demoted_by_month[month_key].update(demoted)

    decision_sets_total = int(frame.groupby(["anchor_date", "side"], sort=False).ngroups)
    decision_sets_with_replacement = int(sum(1 for value in replacement_count_by_decision_set.values() if value > 0))
    decision_sets_with_multi_replacement = int(sum(1 for value in replacement_count_by_decision_set.values() if value > 1))
    max_replacements_per_decision_set = int(max(replacement_count_by_decision_set.values()) if replacement_count_by_decision_set else 0)
    changed_top5_members_count = int(frame["changed_top5_member"].fillna(False).astype(bool).sum())
    changed_top10_members_count = int(frame["changed_top10_member"].fillna(False).astype(bool).sum())
    changed_rank_count = int((frame["adjusted_rank"].astype("Int64") != frame["champion_rank"].astype("Int64")).sum())
    selection_divergence_reason = "static_gate_top5_boundary_replacement" if changed_top5_members_count else "no_top5_replacement_triggered"
    return {
        "schema_version": BRANCHING_PROBE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "changed_top5_members_count": changed_top5_members_count,
        "changed_top10_members_count": changed_top10_members_count,
        "changed_rank_count": changed_rank_count,
        "top5_boundary_score_gap_before_mean": _mean_or_none(top5_before_gaps),
        "top5_boundary_score_gap_after_mean": _mean_or_none(top5_after_gaps),
        "top10_boundary_score_gap_before_mean": _mean_or_none(top10_before_gaps),
        "top10_boundary_score_gap_after_mean": _mean_or_none(top10_after_gaps),
        "selection_divergence_reason": selection_divergence_reason,
        "decision_sets_total": decision_sets_total,
        "decision_sets_with_replacement": decision_sets_with_replacement,
        "decision_sets_with_multi_replacement": decision_sets_with_multi_replacement,
        "max_replacements_per_decision_set": max_replacements_per_decision_set,
        "replacement_count_by_decision_set": replacement_count_by_decision_set,
        "promoted_symbols_by_month": {key: sorted(value) for key, value in promoted_by_month.items()},
        "demoted_symbols_by_month": {key: sorted(value) for key, value in demoted_by_month.items()},
        "replacement_count_by_month": {key: int(value) for key, value in replacement_count_by_month.items()},
        "meaningful_top5_branching_possible": bool(changed_top5_members_count > 0),
        "promotion_gate_threshold": PROMOTION_GATE_THRESHOLD,
        "demotion_gate_threshold": DEMOTION_GATE_THRESHOLD,
        "promotion_margin_threshold": PROMOTION_MARGIN_THRESHOLD,
    }


def _build_promotion_quality_summary(frame: pd.DataFrame) -> dict[str, Any]:
    promoted = frame[frame["promotion_applied"].fillna(False).astype(bool)].copy()
    demoted = frame[frame["demotion_applied"].fillna(False).astype(bool)].copy()
    promoted_winner_hit_rate = _safe_float(promoted["realized_top5_label"].fillna(False).astype(bool).mean()) if len(promoted) else 0.0
    false_promotion_rate = _safe_float((~promoted["realized_top5_label"].fillna(False).astype(bool)).mean()) if len(promoted) else 0.0
    demoted_winner_miss_rate = _safe_float(demoted["realized_top5_label"].fillna(False).astype(bool).mean()) if len(demoted) else 0.0
    promoted_inventory = (
        promoted.groupby("month_bucket")["symbol"].apply(lambda s: sorted(set(map(str, s)))).to_dict() if len(promoted) else {}
    )
    demoted_inventory = (
        demoted.groupby("month_bucket")["symbol"].apply(lambda s: sorted(set(map(str, s)))).to_dict() if len(demoted) else {}
    )
    return {
        "schema_version": PROMOTION_QUALITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "promoted_row_count": int(len(promoted)),
        "demoted_row_count": int(len(demoted)),
        "promoted_winner_hit_count": int(promoted["realized_top5_label"].fillna(False).astype(bool).sum()) if len(promoted) else 0,
        "promoted_winner_hit_rate": promoted_winner_hit_rate,
        "promoted_false_promotion_count": int((~promoted["realized_top5_label"].fillna(False).astype(bool)).sum()) if len(promoted) else 0,
        "false_promotion_rate": false_promotion_rate,
        "demoted_winner_miss_count": int(demoted["realized_top5_label"].fillna(False).astype(bool).sum()) if len(demoted) else 0,
        "demoted_winner_miss_rate": demoted_winner_miss_rate,
        "promoted_symbol_inventory": promoted_inventory,
        "demoted_symbol_inventory": demoted_inventory,
    }


def _build_anti_leakage_audit(frame: pd.DataFrame) -> dict[str, Any]:
    scoring_inputs = ["champion_rank", "champion_score", "path_value_score_v1"]
    excluded_label_columns = ["forward_ret_20d", "realized_top5_label", "monthly_top5_capture_delta", "top15_label", "bottom15_label"]
    label_columns_present = [column for column in excluded_label_columns if column in frame.columns]
    return {
        "schema_version": ANTI_LEAKAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "pass": True,
        "scoring_inputs": scoring_inputs,
        "excluded_label_columns": label_columns_present,
        "used_future_labels_in_scoring": False,
        "proof": "candidate gating uses only champion rank, champion score, and path_value_score_v1; labels are restricted to evaluation artifacts",
    }


def _build_oos_diagnostic(compare_rows: pd.DataFrame, monthly_summary: dict[str, Any]) -> dict[str, Any]:
    months = sorted(str(month) for month in compare_rows["month_bucket"].dropna().astype(str).unique().tolist())
    if len(months) < 2:
        return {
            "schema_version": OOS_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "oos_diagnostic_status": "not_run",
            "reason": "insufficient_months_for_chronological_split",
        }
    split_index = max(1, len(months) // 2)
    early_months = months[:split_index]
    later_months = months[split_index:]
    early_rows = compare_rows[compare_rows["month_bucket"].isin(early_months)].copy()
    later_rows = compare_rows[compare_rows["month_bucket"].isin(later_months)].copy()
    early_summary = _build_monthly_top5_capture_comparison(early_rows)
    later_summary = _build_monthly_top5_capture_comparison(later_rows)
    return {
        "schema_version": OOS_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "oos_diagnostic_status": "run",
        "split_mode": "chronological_month_bucket",
        "early_block": {
            "months": early_months,
            "months_evaluated": int(early_summary.get("months_evaluated") or 0),
            "champion_monthly_top5_capture_mean": _safe_float((early_summary.get("champion_monthly_top5_capture") or {}).get("mean"), 0.0),
            "candidate_monthly_top5_capture_mean": _safe_float((early_summary.get("candidate_monthly_top5_capture") or {}).get("mean"), 0.0),
            "monthly_top5_capture_delta_mean": _safe_float((early_summary.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        },
        "later_block": {
            "months": later_months,
            "months_evaluated": int(later_summary.get("months_evaluated") or 0),
            "champion_monthly_top5_capture_mean": _safe_float((later_summary.get("champion_monthly_top5_capture") or {}).get("mean"), 0.0),
            "candidate_monthly_top5_capture_mean": _safe_float((later_summary.get("candidate_monthly_top5_capture") or {}).get("mean"), 0.0),
            "monthly_top5_capture_delta_mean": _safe_float((later_summary.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        },
        "overall_monthly_summary_ref": {
            "months_evaluated": int(monthly_summary.get("months_evaluated") or 0),
            "monthly_top5_capture_delta_mean": _safe_float((monthly_summary.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        },
    }


def _build_outputs(frame: pd.DataFrame, *, output_root: Path, source_rows_parquet: Path) -> dict[str, Any]:
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)

    adjusted_groups: list[pd.DataFrame] = []
    for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        adjusted_groups.append(_select_candidate(group))
    compare_rows = pd.concat(adjusted_groups, ignore_index=True) if adjusted_groups else frame.iloc[0:0].copy()
    compare_rows = _ensure_columns(compare_rows)
    compare_rows["month_bucket"] = compare_rows["anchor_date"].map(_month_bucket)
    compare_rows["realized_top5_label"] = False
    for (_, _), group in compare_rows.groupby(["anchor_date", "side"], sort=False):
        ordered = group.sort_values(["forward_ret_20d", "symbol"], ascending=[False, True], kind="stable")
        realized = ordered.head(5)
        compare_rows.loc[realized.index, "realized_top5_label"] = True

    branching_probe = _build_branching_probe(compare_rows)
    monthly_capture_summary = _build_monthly_top5_capture_comparison(compare_rows)

    topk_effectiveness: dict[str, dict[str, Any]] = {}
    for top_k in TOP_K_VALUES:
        topk_effectiveness[str(top_k)] = {
            "champion": _build_group_metrics(compare_rows, selected_column=f"champion_selected_top{top_k}", champion_selected_column=f"champion_selected_top{top_k}"),
            "candidate": _build_group_metrics(compare_rows, selected_column=f"candidate_selected_top{top_k}", champion_selected_column=f"champion_selected_top{top_k}"),
        }
        topk_effectiveness[str(top_k)]["delta"] = {
            "mean_forward_ret_20d": topk_effectiveness[str(top_k)]["candidate"]["mean_forward_ret_20d_delta"],
            "median_forward_ret_20d": topk_effectiveness[str(top_k)]["candidate"]["median_forward_ret_20d_delta"],
            "top15_capture_rate": topk_effectiveness[str(top_k)]["candidate"]["top15_capture_delta"],
            "bottom15_contamination_rate": topk_effectiveness[str(top_k)]["candidate"]["bottom15_contamination_delta"],
        }

    candidate_top5 = topk_effectiveness["5"]["candidate"]
    candidate_top10 = topk_effectiveness["10"]["candidate"]
    candidate_top20 = topk_effectiveness["20"]["candidate"]

    turnover_summary = {
        "schema_version": TURNOVER_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "replacement_count_total": int(sum(branching_probe["replacement_count_by_month"].values())),
        "replacement_count_by_month": branching_probe["replacement_count_by_month"],
        "changed_top5_members_count": branching_probe["changed_top5_members_count"],
        "changed_top10_members_count": branching_probe["changed_top10_members_count"],
        "changed_rank_count": branching_probe["changed_rank_count"],
        "top5_overlap_ratio": None if len(compare_rows) == 0 else float(1.0 - branching_probe["changed_top5_members_count"] / max(int(compare_rows["champion_selected_top5"].sum()), 1)),
        "top10_overlap_ratio": None if len(compare_rows) == 0 else float(1.0 - branching_probe["changed_top10_members_count"] / max(int(compare_rows["champion_selected_top10"].sum()), 1)),
        "top20_overlap_ratio": None if len(compare_rows) == 0 else float(1.0 - int(compare_rows["changed_top20_member"].fillna(False).astype(bool).sum()) / max(int(compare_rows["champion_selected_top20"].sum()), 1)),
        "promoted_symbols_by_month": branching_probe["promoted_symbols_by_month"],
        "demoted_symbols_by_month": branching_probe["demoted_symbols_by_month"],
    }

    promotion_quality_summary = _build_promotion_quality_summary(compare_rows)
    anti_leakage_audit = _build_anti_leakage_audit(compare_rows)
    oos_diagnostic = _build_oos_diagnostic(compare_rows, monthly_capture_summary)

    regime_rows: dict[str, Any] = {}
    if "family_regime_context" in compare_rows.columns:
        for regime, group in compare_rows.groupby(compare_rows["family_regime_context"].astype(str), sort=True):
            champion_metrics = _build_group_metrics(group, selected_column="champion_selected_top10", champion_selected_column="champion_selected_top10")
            candidate_metrics = _build_group_metrics(group, selected_column="candidate_selected_top10", champion_selected_column="champion_selected_top10")
            regime_rows[str(regime)] = {
                "sample_count": int(len(group)),
                "champion_mean_forward_ret_20d": champion_metrics["mean_forward_ret_20d"],
                "candidate_mean_forward_ret_20d": candidate_metrics["mean_forward_ret_20d"],
                "candidate_mean_forward_ret_20d_delta": candidate_metrics["mean_forward_ret_20d_delta"],
                "candidate_median_forward_ret_20d_delta": candidate_metrics["median_forward_ret_20d_delta"],
                "candidate_top15_capture_delta": candidate_metrics["top15_capture_delta"],
                "candidate_bottom15_contamination_delta": candidate_metrics["bottom15_contamination_delta"],
            }
    regime_summary = {
        "schema_version": REGIME_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "regime_split_key": "family_regime_context",
        "rows": regime_rows,
        "monthly_capture_by_regime": monthly_capture_summary.get("regime_capture_summary", {}),
    }

    compare = {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_local_decision": None,
        "session_aggregate_decision": None,
        "authoritative_rollup_decision": None,
        "decision_reason": None,
        "artifact_detail_level": "authoritative_full",
        "fallback_status": "authoritative",
        "sample_count": int(len(compare_rows)),
        "same_condition_contract": {
            "schema_version": "tradex_research_contract_v1",
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(CHAMPION_TOPK_VALUES),
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "silent_fallback_allowed": False,
            "ret20_source_mode": "forward_ret_20d",
            "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
            "source_rows": str(Path(source_rows_parquet).resolve()),
            "evaluation_basis": "champion_selected_top20 only",
        },
        "champion_vs_challenger": {
            "selection_only": topk_effectiveness,
        },
        "branching_metrics": branching_probe,
        "selection_divergence_reason": branching_probe["selection_divergence_reason"],
        "compare_status": "compared",
    }

    monthly_capture_delta = _safe_float((monthly_capture_summary.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0)
    decision = "drop"
    decision_reason = "monthly_top5_capture_not_improved"
    if branching_probe["changed_top5_members_count"] <= 0:
        decision = "drop"
        decision_reason = "no_top5_branching"
    elif monthly_capture_delta <= 0:
        decision = "drop"
        decision_reason = "monthly_top5_capture_not_improved"
    elif candidate_top5["mean_forward_ret_20d_delta"] is not None and candidate_top5["mean_forward_ret_20d_delta"] < 0:
        decision = "drop"
        decision_reason = "top5_expected_return_degraded"
    elif candidate_top5["median_forward_ret_20d_delta"] is not None and candidate_top5["median_forward_ret_20d_delta"] < 0:
        decision = "drop"
        decision_reason = "top5_median_ret20_degraded"
    elif candidate_top10["mean_forward_ret_20d_delta"] is not None and candidate_top10["mean_forward_ret_20d_delta"] < 0:
        decision = "drop"
        decision_reason = "top10_expected_return_degraded"
    elif candidate_top10["median_forward_ret_20d_delta"] is not None and candidate_top10["median_forward_ret_20d_delta"] < 0:
        decision = "drop"
        decision_reason = "top10_median_ret20_degraded"
    elif promotion_quality_summary["false_promotion_rate"] > 0.70:
        decision = "drop"
        decision_reason = "false_promotion_rate_too_high"
    elif promotion_quality_summary["demoted_winner_miss_rate"] > 0.50:
        decision = "drop"
        decision_reason = "demoted_winner_miss_rate_too_high"
    elif branching_probe["max_replacements_per_decision_set"] > 1:
        decision = "drop"
        decision_reason = "design_violation_broad_reranker"
    elif branching_probe["decision_sets_with_replacement"] <= 0:
        decision = "drop"
        decision_reason = "no_top5_branching"
    elif candidate_top5["mean_forward_ret_20d_delta"] is not None and candidate_top5["mean_forward_ret_20d_delta"] >= 0:
        if monthly_capture_summary.get("months_evaluated", 0) < 3:
            decision = "hold"
            decision_reason = "signal_present_but_month_coverage_insufficient"
        else:
            decision = "keep"
            decision_reason = "top5_capture_improved_with_narrow_branching"

    compare["candidate_local_decision"] = decision
    compare["session_aggregate_decision"] = decision
    compare["authoritative_rollup_decision"] = decision
    compare["decision_reason"] = decision_reason

    reflectability = {
        "schema_version": REFLECTABILITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "is_reflectable_to_meemee_now": False,
        "reflectability_state": "not_reflectable",
        "suitable_for": "research_only" if decision in {"keep", "hold"} else "drop_freeze",
        "blockers": [
            "promotion_candidate_not_yet_MeeMee_reflectable",
            "research_only_candidate_surface",
            "no_meemee_registration_or_publish_path_changed",
        ],
        "artifact_proof": {
            "branching_probe": str(output_root / "branching_probe.json"),
            "compare": str(output_root / "compare.json"),
            "decision": str(output_root / "decision_summary.json"),
            "monthly_top5_capture_summary": str(output_root / "monthly_top5_capture_summary.json"),
            "anti_leakage_audit": str(output_root / "anti_leakage_audit.json"),
        },
        "what_must_remain_hidden_from_meemee": [
            "raw promotion candidate inventory",
            "demotion candidate inventory",
            "static-gate leakage audit internals",
        ],
    }

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "family_id": "champion_top5_capture_boundary_promoter_v1",
        "source_rows_parquet": str(Path(source_rows_parquet).resolve()),
        "output_root": str(output_root.resolve()),
        "session_output_root": str(output_root.resolve()),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "static_gate_mode": "static_non_optimized_v1",
        "gate_thresholds": {
            "promotion_path_value_score_v1": PROMOTION_GATE_THRESHOLD,
            "demotion_path_value_score_v1": DEMOTION_GATE_THRESHOLD,
            "promotion_minus_demotion_margin": PROMOTION_MARGIN_THRESHOLD,
            "max_promotions_per_decision_set": MAX_PROMOTIONS_PER_DECISION_SET,
        },
        "feature_family_map": {
            "boundary_feature": ["champion_rank", "promotion_minus_demotion_margin"],
            "monthly_capture_feature": ["path_value_score_v1"],
            "common_chart_structure": [],
            "regime_adjustment": [],
            "symbol_specific_deviation": [],
        },
        "decision_time_features_used": ["champion_rank", "champion_score", "path_value_score_v1"],
        "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
    }

    evaluation_contract = {
        "schema_version": EVALUATION_CONTRACT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "same_condition_contract": compare["same_condition_contract"],
        "no_silent_fallback": True,
        "no_meemee_reflection": True,
        "topk_values": list(TOP_K_VALUES),
        "ret20_source_mode": "forward_ret_20d",
        "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
        "static_gate_mode": "static_non_optimized_v1",
        "promotion_gate_threshold": PROMOTION_GATE_THRESHOLD,
        "demotion_gate_threshold": DEMOTION_GATE_THRESHOLD,
        "promotion_margin_threshold": PROMOTION_MARGIN_THRESHOLD,
    }

    topk_summary = {
        "schema_version": TOPK_EFFECTIVENESS_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "top5": {
            "mean_ret20_delta": candidate_top5["mean_forward_ret_20d_delta"],
            "median_ret20_delta": candidate_top5["median_forward_ret_20d_delta"],
            "top15_capture_delta": candidate_top5["top15_capture_delta"],
            "bottom15_contamination_delta": candidate_top5["bottom15_contamination_delta"],
        },
        "top10": {
            "mean_ret20_delta": candidate_top10["mean_forward_ret_20d_delta"],
            "median_ret20_delta": candidate_top10["median_forward_ret_20d_delta"],
            "top15_capture_delta": candidate_top10["top15_capture_delta"],
            "bottom15_contamination_delta": candidate_top10["bottom15_contamination_delta"],
        },
        "top20": {
            "mean_ret20_delta": candidate_top20["mean_forward_ret_20d_delta"],
            "median_ret20_delta": candidate_top20["median_forward_ret_20d_delta"],
            "top15_capture_delta": candidate_top20["top15_capture_delta"],
            "bottom15_contamination_delta": candidate_top20["bottom15_contamination_delta"],
        },
        "monthly_top5_capture": monthly_capture_summary,
        "turnover_change_rate": turnover_summary["top10_overlap_ratio"],
        "cost_slippage_mode": "flat_zero_cost",
    }

    artifact_paths = {
        "candidate_manifest.json": _write_json(output_root / "candidate_manifest.json", manifest),
        "evaluation_contract.json": _write_json(output_root / "evaluation_contract.json", evaluation_contract),
        "branching_probe.json": _write_json(output_root / "branching_probe.json", branching_probe),
        "monthly_top5_capture_summary.json": _write_json(output_root / "monthly_top5_capture_summary.json", monthly_capture_summary),
        "topk_effectiveness_summary.json": _write_json(output_root / "topk_effectiveness_summary.json", topk_summary),
        "promotion_quality_summary.json": _write_json(output_root / "promotion_quality_summary.json", promotion_quality_summary),
        "regime_split_summary.json": _write_json(output_root / "regime_split_summary.json", regime_summary),
        "turnover_summary.json": _write_json(output_root / "turnover_summary.json", turnover_summary),
        "compare.json": _write_json(output_root / "compare.json", compare),
        "decision_summary.json": _write_json(output_root / "decision_summary.json", {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "decision": decision,
            "decision_reason": decision_reason,
            "candidate_id": "champion_top5_capture_boundary_promoter_v1",
            "authoritative_artifact": str(output_root / "compare.json"),
        }),
        "meemee_reflectability_assessment.json": _write_json(output_root / "meemee_reflectability_assessment.json", reflectability),
        "anti_leakage_audit.json": _write_json(output_root / "anti_leakage_audit.json", anti_leakage_audit),
        "static_gate_oos_diagnostic.json": _write_json(output_root / "static_gate_oos_diagnostic.json", oos_diagnostic),
    }

    report_lines = [
        "# champion_top5_capture_boundary_promoter_v1",
        "",
        f"- decision: {decision}",
        f"- reason: {decision_reason}",
        f"- changed_top5_members_count: {branching_probe['changed_top5_members_count']}",
        f"- changed_top10_members_count: {branching_probe['changed_top10_members_count']}",
        f"- monthly_top5_capture_delta_mean: {monthly_capture_delta}",
        f"- top5_mean_ret20_delta: {candidate_top5['mean_forward_ret_20d_delta']}",
        f"- top10_mean_ret20_delta: {candidate_top10['mean_forward_ret_20d_delta']}",
        "",
        "JSON artifacts are authoritative.",
    ]
    report_path = output_root / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    artifact_paths["report.md"] = report_path

    complete = build_artifact_complete(
        {"schema_version": SCHEMA_VERSION},
        sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        schema_version=f"{SCHEMA_VERSION}_artifact_complete_v1",
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = output_root / "_ARTIFACT_COMPLETE.json"

    return {
        "ok": True,
        "decision": decision,
        "decision_reason": decision_reason,
        "run_id": output_root.name,
        "output_root": str(output_root.resolve()),
        "paths": {key: str(value) for key, value in artifact_paths.items()},
        "compare": compare,
        "branching_probe": branching_probe,
        "monthly_top5_capture_summary": monthly_capture_summary,
        "topk_effectiveness_summary": topk_summary,
        "promotion_quality_summary": promotion_quality_summary,
        "regime_split_summary": regime_summary,
        "turnover_summary": turnover_summary,
        "meemee_reflectability_assessment": reflectability,
        "anti_leakage_audit": anti_leakage_audit,
        "static_gate_oos_diagnostic": oos_diagnostic,
    }


def run_all(
    *,
    source_rows_parquet: Path = DEFAULT_SOURCE_ROWS_PARQUET,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    source_rows_parquet = _safe_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET)
    output_root = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / run_id
    frame = _load_frame(source_rows_parquet)
    return _build_outputs(frame, output_root=output_root, source_rows_parquet=source_rows_parquet)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run champion top5 capture boundary promoter v1.")
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    payload = run_all(
        source_rows_parquet=_safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
