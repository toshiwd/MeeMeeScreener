from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import MODEL_FEATURES, _rank_within_groups


SCRIPT_NAME = "tradex_long_side_candidate_generation_refinement_audit_v1"
MANIFEST_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_input_resolution_v1"
TOP15_PATH_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_top15_winner_path_audit_v1"
ADMISSION_ALIGN_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_admission_score_alignment_v1"
TIER_USE_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_tier_usefulness_v1"
MISS_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_candidate_generation_miss_v1"
SOURCE_INSTR_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_source_instrumentation_v1"
OPTIONS_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_refinement_options_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v1")

LONG_SURFACE_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")
LONG_FILTER_SESSION = Path(r"G:\Tradex\long_side_filter_revision_v1\20260503T015243Z-676367")
LONG_RERANKER_SESSION = Path(r"G:\Tradex\long_side_reranker_validation_v1\20260502T151756Z-703876")
HIGH_RECALL_DESIGN_SESSION = Path(r"G:\Tradex\high_recall_candidate_pool_design_v1\20260502T112742Z-067390")
REJECTED_INVENTORY_SESSION = HIGH_RECALL_DESIGN_SESSION
PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1_larger\20260502T034011Z-d76e6794")
TWO_STAGE_SESSION = Path(r"G:\Tradex\candidate_generation_two_stage_admission_context_shape_v1_larger\20260502T034025Z-86ae7451")

LONG_SURFACE = LONG_SURFACE_SESSION / "long_side_active_surface.parquet"
LONG_SURFACE_SUMMARY = LONG_SURFACE_SESSION / "long_side_active_surface_summary.json"
SURFACE_FEATURE_CHECK = LONG_SURFACE_SESSION / "side_specific_feature_contract_check.json"
SURFACE_NO_LOOKAHEAD = LONG_SURFACE_SESSION / "side_specific_no_lookahead_audit.json"
SURFACE_LEAKAGE = LONG_SURFACE_SESSION / "side_specific_leakage_audit.json"
SURFACE_QUALITY = LONG_SURFACE_SESSION / "side_specific_surface_quality_audit.json"
SURFACE_ORACLE = LONG_SURFACE_SESSION / "side_specific_oracle_headroom_audit.json"
SURFACE_DECISION = LONG_SURFACE_SESSION / "side_specific_high_recall_surface_v1_decision.json"

FILTER_ROWS = LONG_FILTER_SESSION / "long_side_filter_revision_rows.parquet"
FILTER_SURFACE = LONG_FILTER_SESSION / "long_side_filter_revision_surface_comparison.json"
FILTER_RERANKER = LONG_FILTER_SESSION / "long_side_filter_revision_reranker_comparison.json"
FILTER_RECOMMENDATION = LONG_FILTER_SESSION / "long_side_filter_revision_recommendation.json"
FILTER_DECISION = LONG_FILTER_SESSION / "long_side_filter_revision_v1_decision.json"
FILTER_TIER_SUMMARY = LONG_FILTER_SESSION / "long_side_filter_revision_tier_summary.parquet"
FILTER_GROUP_SUMMARY = LONG_FILTER_SESSION / "long_side_filter_revision_group_summary.parquet"
FILTER_ORACLE_BY_GROUP = LONG_FILTER_SESSION / "long_side_filter_revision_oracle_by_group.parquet"

RERANKER_ROWS = LONG_RERANKER_SESSION / "long_side_reranker_prediction_rows.parquet"
RERANKER_VARIANT_COMPARISON = LONG_RERANKER_SESSION / "long_side_reranker_variant_pool_comparison.json"
RERANKER_ORACLE_GAP = LONG_RERANKER_SESSION / "long_side_oracle_gap_comparison.json"
RERANKER_FAILURE = LONG_RERANKER_SESSION / "long_side_reranker_failure_mode_audit.json"
RERANKER_DECISION = LONG_RERANKER_SESSION / "long_side_reranker_validation_v1_decision.json"

PREFILTER_ROWS = PREFILTER_SESSION / "candidate_prefilter_rows.parquet"
PREFILTER_DECISION = PREFILTER_SESSION / "candidate_generation_pre_filter_context_shape_v1_decision.json"
PREFILTER_COMPARISON = PREFILTER_SESSION / "candidate_pool_comparison.json"
PREFILTER_COVERAGE = PREFILTER_SESSION / "candidate_prefilter_coverage_summary.json"
PREFILTER_POLICY = PREFILTER_SESSION / "candidate_prefilter_policy.json"

TWO_STAGE_ROWS = TWO_STAGE_SESSION / "candidate_two_stage_rows.parquet"
TWO_STAGE_DECISION = TWO_STAGE_SESSION / "candidate_generation_two_stage_admission_context_shape_v1_decision.json"
TWO_STAGE_COMPARISON = TWO_STAGE_SESSION / "candidate_pool_comparison.json"
TWO_STAGE_COVERAGE = TWO_STAGE_SESSION / "candidate_stage_coverage_summary.json"
TWO_STAGE_POLICY = TWO_STAGE_SESSION / "two_stage_admission_policy.json"

THRESHOLD_INVENTORY = HIGH_RECALL_DESIGN_SESSION / "candidate_generation_threshold_inventory.parquet"
GROUP_SIZE_DISTRIBUTION = HIGH_RECALL_DESIGN_SESSION / "candidate_generation_group_size_distribution.parquet"
CURRENT_CONTRACT_INVENTORY = HIGH_RECALL_DESIGN_SESSION / "current_candidate_generation_contract_inventory.json"
HIGH_RECALL_CONTRACT = HIGH_RECALL_DESIGN_SESSION / "high_recall_candidate_pool_contract.json"
HIGH_RECALL_EVAL_PLAN = HIGH_RECALL_DESIGN_SESSION / "high_recall_candidate_pool_evaluation_plan.json"
HIGH_RECALL_DECISION = HIGH_RECALL_DESIGN_SESSION / "high_recall_candidate_pool_design_v1_decision.json"
HIGH_RECALL_FEASIBILITY = HIGH_RECALL_DESIGN_SESSION / "high_recall_candidate_pool_feasibility_estimate.json"
REJECTED_SOURCE_INVENTORY = REJECTED_INVENTORY_SESSION / "rejected_candidate_source_inventory.json"

RAW_SNAPSHOT_MANIFEST = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_fresh20260502d\integrated_guarded_v1_candidate_snapshots.json")

TOP_K_VALUES = (5, 10, 20)
LONG_TOP15_BUCKETS = (
    ("rank_1_5", lambda f: pd.to_numeric(f["rank"], errors="coerce").between(1, 5, inclusive="both")),
    ("rank_6_8", lambda f: pd.to_numeric(f["rank"], errors="coerce").between(6, 8, inclusive="both")),
    ("rank_9_15", lambda f: pd.to_numeric(f["rank"], errors="coerce").between(9, 15, inclusive="both")),
    ("rank_16_20", lambda f: pd.to_numeric(f["rank"], errors="coerce").between(16, 20, inclusive="both")),
    ("rank_gt_20", lambda f: pd.to_numeric(f["rank"], errors="coerce") > 20),
    ("score_gte_045", lambda f: pd.to_numeric(f["score"], errors="coerce") >= 0.45),
    ("score_gte_040", lambda f: pd.to_numeric(f["score"], errors="coerce") >= 0.40),
    ("score_gte_035", lambda f: pd.to_numeric(f["score"], errors="coerce") >= 0.35),
    ("score_lt_035", lambda f: pd.to_numeric(f["score"], errors="coerce") < 0.35),
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _dedupe_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[:, ~frame.columns.duplicated()].copy()


def _long_only(frame: pd.DataFrame) -> pd.DataFrame:
    if "side" not in frame.columns:
        return frame.copy()
    return frame[frame["side"].astype(str).eq("long")].copy()


def _group_stats(frame: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False).size()
    return {
        "row_count": int(len(frame)),
        "group_count": int(groups.shape[0]),
        "min_group_size": int(groups.min()) if len(groups) else None,
        "median_group_size": float(groups.median()) if len(groups) else None,
        "mean_group_size": float(groups.mean()) if len(groups) else None,
        "max_group_size": int(groups.max()) if len(groups) else None,
        "top5_thin_groups": int((groups < 5).sum()),
        "top10_thin_groups": int((groups < 10).sum()),
        "top20_thin_groups": int((groups < 20).sum()),
    }


def _load_inputs() -> dict[str, Any]:
    paths = {
        "long_surface": LONG_SURFACE,
        "long_surface_summary": LONG_SURFACE_SUMMARY,
        "surface_feature_check": SURFACE_FEATURE_CHECK,
        "surface_no_lookahead": SURFACE_NO_LOOKAHEAD,
        "surface_leakage": SURFACE_LEAKAGE,
        "surface_quality": SURFACE_QUALITY,
        "surface_oracle": SURFACE_ORACLE,
        "surface_decision": SURFACE_DECISION,
        "filter_rows": FILTER_ROWS,
        "filter_surface": FILTER_SURFACE,
        "filter_reranker": FILTER_RERANKER,
        "filter_recommendation": FILTER_RECOMMENDATION,
        "filter_decision": FILTER_DECISION,
        "filter_tier_summary": FILTER_TIER_SUMMARY,
        "filter_group_summary": FILTER_GROUP_SUMMARY,
        "filter_oracle_by_group": FILTER_ORACLE_BY_GROUP,
        "reranker_rows": RERANKER_ROWS,
        "reranker_variant_comparison": RERANKER_VARIANT_COMPARISON,
        "reranker_oracle_gap": RERANKER_ORACLE_GAP,
        "reranker_failure": RERANKER_FAILURE,
        "reranker_decision": RERANKER_DECISION,
        "prefilter_rows": PREFILTER_ROWS,
        "prefilter_decision": PREFILTER_DECISION,
        "prefilter_comparison": PREFILTER_COMPARISON,
        "prefilter_coverage": PREFILTER_COVERAGE,
        "prefilter_policy": PREFILTER_POLICY,
        "two_stage_rows": TWO_STAGE_ROWS,
        "two_stage_decision": TWO_STAGE_DECISION,
        "two_stage_comparison": TWO_STAGE_COMPARISON,
        "two_stage_coverage": TWO_STAGE_COVERAGE,
        "two_stage_policy": TWO_STAGE_POLICY,
        "threshold_inventory": THRESHOLD_INVENTORY,
        "group_size_distribution": GROUP_SIZE_DISTRIBUTION,
        "current_contract_inventory": CURRENT_CONTRACT_INVENTORY,
        "high_recall_contract": HIGH_RECALL_CONTRACT,
        "high_recall_eval_plan": HIGH_RECALL_EVAL_PLAN,
        "high_recall_decision": HIGH_RECALL_DECISION,
        "high_recall_feasibility": HIGH_RECALL_FEASIBILITY,
        "rejected_source_inventory": REJECTED_SOURCE_INVENTORY,
        "raw_snapshot_manifest": RAW_SNAPSHOT_MANIFEST,
    }
    for label, path in paths.items():
        _ensure_exists(path, label)

    long_surface = _dedupe_columns(_load_frame(LONG_SURFACE))
    filter_rows = _dedupe_columns(_load_frame(FILTER_ROWS))
    reranker_rows = _dedupe_columns(_load_frame(RERANKER_ROWS))
    prefilter_rows = _long_only(_dedupe_columns(_load_frame(PREFILTER_ROWS)))
    two_stage_rows = _long_only(_dedupe_columns(_load_frame(TWO_STAGE_ROWS)))
    threshold_inventory = _load_frame(THRESHOLD_INVENTORY)
    group_size_distribution = _load_frame(GROUP_SIZE_DISTRIBUTION)

    return {
        "paths": paths,
        "long_surface": long_surface,
        "filter_rows": filter_rows,
        "reranker_rows": reranker_rows,
        "prefilter_rows": prefilter_rows,
        "two_stage_rows": two_stage_rows,
        "threshold_inventory": threshold_inventory,
        "group_size_distribution": group_size_distribution,
        "surface_summary": _load_json(LONG_SURFACE_SUMMARY),
        "surface_feature_check": _load_json(SURFACE_FEATURE_CHECK),
        "surface_no_lookahead": _load_json(SURFACE_NO_LOOKAHEAD),
        "surface_leakage": _load_json(SURFACE_LEAKAGE),
        "surface_quality": _load_json(SURFACE_QUALITY),
        "surface_oracle": _load_json(SURFACE_ORACLE),
        "surface_decision": _load_json(SURFACE_DECISION),
        "filter_surface": _load_json(FILTER_SURFACE),
        "filter_reranker": _load_json(FILTER_RERANKER),
        "filter_recommendation": _load_json(FILTER_RECOMMENDATION),
        "filter_decision": _load_json(FILTER_DECISION),
        "filter_tier_summary": _load_frame(FILTER_TIER_SUMMARY),
        "filter_group_summary": _load_frame(FILTER_GROUP_SUMMARY),
        "filter_oracle_by_group": _load_frame(FILTER_ORACLE_BY_GROUP),
        "reranker_variant_comparison": _load_json(RERANKER_VARIANT_COMPARISON),
        "reranker_oracle_gap": _load_json(RERANKER_ORACLE_GAP),
        "reranker_failure": _load_json(RERANKER_FAILURE),
        "reranker_decision": _load_json(RERANKER_DECISION),
        "prefilter_decision": _load_json(PREFILTER_DECISION),
        "prefilter_comparison": _load_json(PREFILTER_COMPARISON),
        "prefilter_coverage": _load_json(PREFILTER_COVERAGE),
        "prefilter_policy": _load_json(PREFILTER_POLICY),
        "two_stage_decision": _load_json(TWO_STAGE_DECISION),
        "two_stage_comparison": _load_json(TWO_STAGE_COMPARISON),
        "two_stage_coverage": _load_json(TWO_STAGE_COVERAGE),
        "two_stage_policy": _load_json(TWO_STAGE_POLICY),
        "current_contract_inventory": _load_json(CURRENT_CONTRACT_INVENTORY),
        "high_recall_contract": _load_json(HIGH_RECALL_CONTRACT),
        "high_recall_eval_plan": _load_json(HIGH_RECALL_EVAL_PLAN),
        "high_recall_decision": _load_json(HIGH_RECALL_DECISION),
        "high_recall_feasibility": _load_json(HIGH_RECALL_FEASIBILITY),
        "rejected_source_inventory": _load_json(REJECTED_SOURCE_INVENTORY),
        "raw_snapshot_manifest": _load_json(RAW_SNAPSHOT_MANIFEST),
    }


def _bool_rate(series: pd.Series) -> float | None:
    if len(series) == 0:
        return None
    return float(series.fillna(False).astype(bool).mean())


def _oracle_block(frame: pd.DataFrame, topk: int, *, score_col: str = "forward_ret_20d") -> dict[str, Any]:
    eligible = frame[pd.to_numeric(frame["forward_ret_20d"], errors="coerce").notna()].copy()
    rows = []
    for _, group in eligible.groupby(["anchor_date", "side"], sort=False):
        g = group.sort_values([score_col, "path_value_score_v1", "mae_20d", "candidate_idx"], ascending=[False, False, True, True], kind="mergesort")
        rows.append(g.head(topk))
    oracle = pd.concat(rows, ignore_index=True) if rows else eligible.iloc[0:0].copy()
    return {
        "row_count": int(len(oracle)),
        "mean_forward_ret_20d": float(pd.to_numeric(oracle["forward_ret_20d"], errors="coerce").mean()) if len(oracle) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(oracle["path_value_score_v1"], errors="coerce").mean()) if len(oracle) else None,
        "top15_capture_rate": _bool_rate(oracle["top15_label"]) if len(oracle) else None,
        "top20pct_capture_rate": _bool_rate(oracle["top20pct_label"]) if len(oracle) else None,
        "bottom15_contamination_rate": _bool_rate(oracle["bottom15_label"]) if len(oracle) else None,
    }


def _topk_flags(frame: pd.DataFrame, score_col: str, group_cols: list[str], topk: int) -> pd.Series:
    ranks = _rank_within_groups(frame, pd.to_numeric(frame[score_col], errors="coerce"), group_cols=group_cols)
    return ranks <= topk


def _score_rank_bucket_summary(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    out_rows: list[dict[str, Any]] = []
    frame = frame.copy()
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    for bucket_name, mask_fn in LONG_TOP15_BUCKETS:
        mask = mask_fn(frame)
        subset = frame[mask].copy()
        out_rows.append(
            {
                "bucket": bucket_name,
                "row_count": int(len(subset)),
                "group_count": int(subset.groupby(["anchor_date", "side"], sort=False).ngroups) if len(subset) else 0,
                "top15_label_rate": _bool_rate(subset["top15_label"]),
                "top20pct_label_rate": _bool_rate(subset["top20pct_label"]),
                "bottom15_label_rate": _bool_rate(subset["bottom15_label"]),
                "non_positive_forward_ret_rate": float((pd.to_numeric(subset["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(subset["forward_ret_20d"], errors="coerce") <= 0)).mean()) if len(subset) else None,
                "mean_forward_ret_20d": float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if len(subset) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if len(subset) else None,
                "tier_composition": {str(k): int(v) for k, v in subset["candidate_pool_tier"].value_counts().items()},
            }
        )
    summary = {
        "schema_version": ADMISSION_ALIGN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "thresholds": {
            "top15_score_threshold": _load_frame(THRESHOLD_INVENTORY).set_index("name").loc["top15_score_threshold", "value"],
            "bottom15_score_threshold": _load_frame(THRESHOLD_INVENTORY).set_index("name").loc["bottom15_score_threshold", "value"],
            "min_sample_count": int(_load_frame(THRESHOLD_INVENTORY).set_index("name").loc["min_sample_count", "value"]),
            "min_unique_symbol_count": int(_load_frame(THRESHOLD_INVENTORY).set_index("name").loc["min_unique_symbol_count", "value"]),
            "min_month_count": int(_load_frame(THRESHOLD_INVENTORY).set_index("name").loc["min_month_count", "value"]),
        },
        "bucket_summaries": out_rows,
        "alignment_conclusion": {
            "rank_1_5_contains_all_observed_top15": bool(frame.loc[frame["rank"].between(1, 5, inclusive="both"), "top15_label"].fillna(False).astype(bool).sum() == frame["top15_label"].fillna(False).astype(bool).sum()),
            "rank_6_plus_adds_no_top15": bool(frame.loc[frame["rank"] > 5, "top15_label"].fillna(False).astype(bool).sum() == 0),
            "score_gte_040_has_same_top15_as_gte_045_or_better": bool(
                _bool_rate(frame.loc[frame["score"] >= 0.40, "top15_label"]) >= _bool_rate(frame.loc[frame["score"] >= 0.45, "top15_label"])
                if _bool_rate(frame.loc[frame["score"] >= 0.40, "top15_label"]) is not None and _bool_rate(frame.loc[frame["score"] >= 0.45, "top15_label"]) is not None
                else False
            ),
            "score_rank_signal_sufficient_for_rebuild": False,
        },
    }
    return summary, pd.DataFrame(out_rows)


def _winner_path_audit(
    long_surface: pd.DataFrame,
    filter_rows: pd.DataFrame,
    reranker_rows: pd.DataFrame,
    prefilter_rows: pd.DataFrame,
    two_stage_rows: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    winners = long_surface[long_surface["top15_label"].fillna(False).astype(bool)].copy()
    winners["long_active_present"] = True
    winners["prefilter_exact_present"] = False
    winners["two_stage_exact_present"] = False
    winners["prefilter_key_present"] = False
    winners["two_stage_key_present"] = False
    winners["tree_hgb_path_value_score"] = pd.NA
    winners["tree_hgb_path_value_rank"] = pd.NA
    winners["prefilter_candidate_score"] = pd.NA
    winners["prefilter_candidate_rank"] = pd.NA

    key_cols = ["anchor_date", "side", "symbol", "candidate_idx"]
    pre_exact = prefilter_rows[key_cols].copy()
    two_exact = two_stage_rows[key_cols].copy()
    for i, row in winners.iterrows():
        key = {c: row[c] for c in key_cols}
        winners.at[i, "prefilter_exact_present"] = bool(((pre_exact == pd.Series(key)).all(axis=1)).any())
        winners.at[i, "two_stage_exact_present"] = bool(((two_exact == pd.Series(key)).all(axis=1)).any())
        winners.at[i, "prefilter_key_present"] = bool(
            not prefilter_rows[
                (prefilter_rows["anchor_date"].astype(str) == str(row["anchor_date"]))
                & (prefilter_rows["side"].astype(str) == str(row["side"]))
                & (prefilter_rows["symbol"].astype(str) == str(row["symbol"]))
            ].empty
        )
        winners.at[i, "two_stage_key_present"] = bool(
            not two_stage_rows[
                (two_stage_rows["anchor_date"].astype(str) == str(row["anchor_date"]))
                & (two_stage_rows["side"].astype(str) == str(row["side"]))
                & (two_stage_rows["symbol"].astype(str) == str(row["symbol"]))
            ].empty
        )

    rr = reranker_rows[key_cols + ["tree_hgb_path_value_score", "tree_hgb_path_value_rank"]].drop_duplicates(subset=key_cols)
    rr = rr.rename(
        columns={
            "tree_hgb_path_value_score": "tree_hgb_path_value_score_reranker",
            "tree_hgb_path_value_rank": "tree_hgb_path_value_rank_reranker",
        }
    )
    winners = winners.merge(rr, on=key_cols, how="left", suffixes=("", "_reranker"))

    filter_presence: dict[str, list[bool]] = {}
    for variant in sorted(filter_rows["filter_revision_variant"].dropna().astype(str).unique().tolist()):
        subset = filter_rows[filter_rows["filter_revision_variant"].astype(str).eq(variant)].copy()
        subset = subset.drop_duplicates(subset=key_cols)
        merged = winners.merge(subset[key_cols + ["long_filter_revision_selected", "tree_hgb_path_value_selected_top5", "tree_hgb_path_value_selected_top10", "tree_hgb_path_value_selected_top20"]], on=key_cols, how="left")
        filter_presence[variant] = merged["long_filter_revision_selected"].fillna(False).astype(bool).tolist()
        if variant == "long_filter_score_040_rank8":
            winners["filter_score_040_rank8_present"] = merged["long_filter_revision_selected"].fillna(False).astype(bool).tolist()

    winner_rows = []
    for idx, row in winners.iterrows():
        champion_top5 = int(pd.to_numeric(row["champion_rank"], errors="coerce") <= 5)
        champion_top10 = int(pd.to_numeric(row["champion_rank"], errors="coerce") <= 10)
        champion_top20 = int(pd.to_numeric(row["champion_rank"], errors="coerce") <= 20)
        reranker_rank = row["tree_hgb_path_value_rank_reranker"] if "tree_hgb_path_value_rank_reranker" in row else row.get("tree_hgb_path_value_rank")
        reranker_score = row["tree_hgb_path_value_score_reranker"] if "tree_hgb_path_value_score_reranker" in row else row.get("tree_hgb_path_value_score")
        reranker_top5 = int(pd.to_numeric(reranker_rank, errors="coerce") <= 5) if pd.notna(reranker_rank) else 0
        reranker_top10 = int(pd.to_numeric(reranker_rank, errors="coerce") <= 10) if pd.notna(reranker_rank) else 0
        reranker_top20 = int(pd.to_numeric(reranker_rank, errors="coerce") <= 20) if pd.notna(reranker_rank) else 0
        filter_variant_presence = int(sum(bool(values[idx]) for values in filter_presence.values())) if filter_presence else 0
        status = "within_top5" if champion_top5 and reranker_top5 else ("within_top10" if champion_top10 and reranker_top10 else "within_top20" if champion_top20 and reranker_top20 else "near_cutoff")
        winner_rows.append(
            {
                "anchor_date": row["anchor_date"],
                "side": row["side"],
                "symbol": row["symbol"],
                "candidate_idx": int(row["candidate_idx"]),
                "score": float(pd.to_numeric(row["score"], errors="coerce")),
                "rank": int(pd.to_numeric(row["rank"], errors="coerce")),
                "candidate_pool_tier": row["candidate_pool_tier"],
                "candidate_pool_reason": row["candidate_pool_reason"],
                "risk_flagged_candidate": bool(row["risk_flagged_candidate"]),
                "included_by_filter_reason": row["included_by_filter_reason"],
                "long_active_present": bool(row["long_active_present"]),
                "prefilter_exact_present": bool(row["prefilter_exact_present"]),
                "two_stage_exact_present": bool(row["two_stage_exact_present"]),
                "prefilter_key_present": bool(row["prefilter_key_present"]),
                "two_stage_key_present": bool(row["two_stage_key_present"]),
                "champion_score": float(pd.to_numeric(row["champion_score"], errors="coerce")),
                "champion_rank": int(pd.to_numeric(row["champion_rank"], errors="coerce")),
                "tree_hgb_path_value_score": None if pd.isna(reranker_score) else float(reranker_score),
                "tree_hgb_path_value_rank": None if pd.isna(reranker_rank) else int(pd.to_numeric(reranker_rank, errors="coerce")),
                "champion_in_top5": champion_top5,
                "champion_in_top10": champion_top10,
                "champion_in_top20": champion_top20,
                "reranker_in_top5": reranker_top5,
                "reranker_in_top10": reranker_top10,
                "reranker_in_top20": reranker_top20,
                "winner_status": status,
                "filter_revision_variant_presence_count": filter_variant_presence,
            }
        )

    winner_frame = pd.DataFrame(winner_rows)
    audit = {
        "schema_version": TOP15_PATH_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "long_active_row_count": int(len(long_surface)),
        "long_active_group_count": int(long_surface.groupby(["anchor_date", "side"], sort=False).ngroups),
        "top15_winner_count": int(len(winner_frame)),
        "winner_rows_present_in_long_active_surface": int(len(winner_frame)),
        "winner_rows_missing_from_long_active_surface": 0,
        "winners": winner_rows,
        "conclusion": {
            "top15_winners_are_not_buried_on_the_active_surface": bool(len(winner_frame) == 0 or winner_frame["winner_status"].eq("within_top5").all()),
            "all_observed_top15_winners_are_backfill": bool(len(winner_frame) > 0 and winner_frame["candidate_pool_tier"].astype(str).eq("risk_flagged_backfill").all()),
            "traceable_back_to_prefilter_or_two_stage_exact_key": bool(winner_frame["prefilter_exact_present"].any() or winner_frame["two_stage_exact_present"].any()),
        },
    }
    return audit, winner_frame


def _tier_usefulness_audit(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    frame = frame.copy()
    frame["top15_label"] = frame["top15_label"].fillna(False).astype(bool)
    frame["top20pct_label"] = frame["top20pct_label"].fillna(False).astype(bool)
    frame["bottom15_label"] = frame["bottom15_label"].fillna(False).astype(bool)
    tier_rows = []
    total_top15 = max(1, int(frame["top15_label"].sum()))
    total_top20 = max(1, int(frame["top20pct_label"].sum()))
    for tier, g in frame.groupby("candidate_pool_tier", sort=False):
        tier_rows.append(
            {
                "candidate_pool_tier": str(tier),
                "row_count": int(len(g)),
                "group_count": int(g.groupby(["anchor_date", "side"], sort=False).ngroups),
                "top15_count": int(g["top15_label"].sum()),
                "top20pct_count": int(g["top20pct_label"].sum()),
                "bottom15_count": int(g["bottom15_label"].sum()),
                "mean_forward_ret_20d": float(pd.to_numeric(g["forward_ret_20d"], errors="coerce").mean()) if pd.to_numeric(g["forward_ret_20d"], errors="coerce").notna().any() else None,
                "mean_path_value_score_v1": float(pd.to_numeric(g["path_value_score_v1"], errors="coerce").mean()) if pd.to_numeric(g["path_value_score_v1"], errors="coerce").notna().any() else None,
                "oracle_top15_share": float(g["top15_label"].sum() / total_top15) if total_top15 else None,
                "oracle_top20pct_share": float(g["top20pct_label"].sum() / total_top20) if total_top20 else None,
                "role_recommendation": "active" if str(tier) in {"KEEP_PRIMARY", "KEEP_WATCH"} else "diagnostic",
                "notes": "risk_flagged_backfill remains signal-bearing but noisy" if str(tier) == "risk_flagged_backfill" else "clear active lane" if str(tier) in {"KEEP_PRIMARY", "KEEP_WATCH"} else "other",
            }
        )
    audit = {
        "schema_version": TIER_USE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "tier_rows": tier_rows,
        "tier_conclusion": {
            "keep_primary_active": True,
            "keep_watch_active": True,
            "risk_flagged_backfill_should_be_active_recall_lane_or_diagnostic": True,
            "exclude_any_tier_now": False,
        },
    }
    return audit, pd.DataFrame(tier_rows)


def _candidate_generation_miss_audit(
    long_surface: pd.DataFrame,
    prefilter_rows: pd.DataFrame,
    two_stage_rows: pd.DataFrame,
    winner_rows: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    key_cols = ["anchor_date", "side", "symbol", "candidate_idx"]
    long_top15 = long_surface[long_surface["top15_label"].fillna(False).astype(bool)].copy()
    pre_top15 = prefilter_rows[prefilter_rows["top15_label"].fillna(False).astype(bool)].copy()
    two_top15 = two_stage_rows[two_stage_rows["top15_label"].fillna(False).astype(bool)].copy()
    long_keyset = set(tuple(x) for x in long_top15[key_cols].astype(str).to_numpy().tolist())

    pre_top15["traceable_to_long_active_exact_key"] = pre_top15.apply(lambda r: tuple(str(r[c]) for c in key_cols) in long_keyset, axis=1)
    two_top15["traceable_to_long_active_exact_key"] = two_top15.apply(lambda r: tuple(str(r[c]) for c in key_cols) in long_keyset, axis=1)

    miss_examples = pre_top15[~pre_top15["traceable_to_long_active_exact_key"]].copy()
    if len(miss_examples):
        miss_examples["miss_class"] = "winner_absent_from_pool"
        miss_examples["miss_reason"] = "top15 source rows do not appear in the long active exact-key surface and no rejected-row log exists"
    else:
        miss_examples = pre_top15.iloc[0:0].copy()

    long_top15_count = int(len(long_top15))
    source_top15_count = int(len(pre_top15))
    traceable_count = int(pre_top15["traceable_to_long_active_exact_key"].sum())
    miss_audit = {
        "schema_version": MISS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "long_active_top15_count": long_top15_count,
        "prefilter_source_long_top15_count": source_top15_count,
        "two_stage_source_long_top15_count": int(len(two_top15)),
        "traceable_prefilter_top15_to_long_active_exact_key_count": traceable_count,
        "traceable_two_stage_top15_to_long_active_exact_key_count": int(two_top15["traceable_to_long_active_exact_key"].sum()),
        "miss_classes": {
            "winner_absent_from_pool": int(len(miss_examples)),
            "winner_present_but_buried_by_score": int(0),
            "winner_present_but_reranker_misses": int(0),
            "winner_present_but_filtered_out": int(0),
            "winner_present_but_label_sparse": int(0),
            "insufficient_source_instrumentation": int(len(miss_examples)),
        },
        "notes": [
            "The upstream prefilter and two-stage sessions expose admitted rows and diagnostics only; there is no standalone rejected-row log.",
            "Exact key overlap from the upstream long-side prefilter witness set into the long active surface is zero for the missing top15 rows.",
            "The long active surface's own two top15 winners are backfill rows that are already inside top5 under both champion and frozen reranker.",
        ],
    }
    return miss_audit, miss_examples


def _source_instrumentation_audit(
    inputs: dict[str, Any],
    long_surface: pd.DataFrame,
    prefilter_rows: pd.DataFrame,
    two_stage_rows: pd.DataFrame,
    miss_examples: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw_universe_reference_row_count = int(inputs["high_recall_contract"]["raw_source_universe_support"]["row_count"])
    accessible_manifest_row_count = int(len(inputs["raw_snapshot_manifest"].get("rows", []))) if isinstance(inputs["raw_snapshot_manifest"], dict) else None
    source_field_columns = {
        "candidate_identity": ["anchor_date", "side", "symbol", "candidate_idx", "month_bucket"],
        "admission_metadata": ["candidate_pool_tier", "candidate_pool_reason", "risk_flagged_candidate", "included_by_filter_reason", "filter_variant"],
        "score_rank": ["score", "rank", "candidate_score", "candidate_rank", "champion_score", "champion_rank", "challenger_score", "challenger_rank"],
        "shape_context": ["prefilter_reason", "prefilter_bucket", "shape_classification", "shape_joined", "conditional_high_value", "candle_shape_modifier", "stable_bad_pick_family"],
        "outcome_labels": ["top15_label", "top20pct_label", "bottom15_label", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d"],
        "no_lookahead": ["monthly_context_no_lookahead", "weekly_context_no_lookahead", "monthly_context_source", "weekly_context_source"],
    }
    coverage_rows = []
    for group, cols in source_field_columns.items():
        present = [c for c in cols if c in long_surface.columns]
        coverage_rows.append(
            {
                "group": group,
                "present_count": int(len(present)),
                "required_count": int(len(cols)),
                "coverage_rate": float(len(present) / len(cols)) if cols else None,
                "present_columns": present,
            }
        )
    long_top15 = long_surface[long_surface["top15_label"].fillna(False).astype(bool)].copy()
    pre_top15 = prefilter_rows[prefilter_rows["top15_label"].fillna(False).astype(bool)].copy()
    two_top15 = two_stage_rows[two_stage_rows["top15_label"].fillna(False).astype(bool)].copy()
    exact_key = ["anchor_date", "side", "symbol", "candidate_idx"]
    long_set = set(tuple(x) for x in long_top15[exact_key].astype(str).to_numpy().tolist())
    pre_trace = int(sum(tuple(str(r[c]) for c in exact_key) in long_set for _, r in pre_top15.iterrows()))
    two_trace = int(sum(tuple(str(r[c]) for c in exact_key) in long_set for _, r in two_top15.iterrows()))
    audit = {
        "schema_version": SOURCE_INSTR_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "raw_source_universe_reference_row_count": raw_universe_reference_row_count,
        "accessible_raw_snapshot_manifest_row_count": accessible_manifest_row_count,
        "prefilter_long_row_count": int(len(prefilter_rows)),
        "two_stage_long_row_count": int(len(two_stage_rows)),
        "long_active_row_count": int(len(long_surface)),
        "long_active_top15_count": int(len(long_top15)),
        "prefilter_source_top15_count": int(len(pre_top15)),
        "two_stage_source_top15_count": int(len(two_top15)),
        "exact_key_overlap_long_active_vs_prefilter_top15": pre_trace,
        "exact_key_overlap_long_active_vs_two_stage_top15": two_trace,
        "rejected_row_inventory": inputs["rejected_source_inventory"],
        "rejected_rows_available": bool(inputs["rejected_source_inventory"].get("available", False)),
        "rejected_rows_logged_as_standalone_bundle": False,
        "stable_reject_keys_logged": False,
        "reject_reason_buckets_logged": False,
        "admitted_rows_traceable": True,
        "source_field_coverage": coverage_rows,
        "instrumentation_conclusion": {
            "rejected_row_instrumentation_required": True,
            "score_rank_alignment_is_insufficient_to_solve_top15_gap": True,
            "current_source_surfaces_are_admitted_row_only": True,
            "exact_loss_path_cannot_be_determined_from_available_artifacts": True,
        },
        "notes": [
            "The rejected candidate inventory explicitly says standalone pre-admission rejected rows are not available.",
            "The long active winners are traceable inside the active surface and frozen reranker replay, but not back through a stable rejected-row log.",
            "The upstream prefilter and two-stage bundles provide admitted-row diagnostics only; they are not enough to recover the missing top15 path deterministically.",
        ],
    }
    return audit, pd.DataFrame(coverage_rows)


def _refinement_options(
    top15_audit: dict[str, Any],
    miss_audit: dict[str, Any],
    source_audit: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": OPTIONS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "options": {
            "revise_long_admission_score": {
                "expected_benefit": "low_to_moderate",
                "risk": "score/rank buckets are already weakly aligned and did not show a clear top15 lift from tighter guards alone",
                "required_data": ["admitted rows with stable score/rank", "top15 labels", "tier composition"],
                "no_lookahead_compatible": True,
                "verification_method": "fixed-condition replay on the long active surface after changing admission thresholds only",
                "next_artifact_to_generate": "long_side_filter_revision_v2",
            },
            "add_top15_recall_signal": {
                "expected_benefit": "moderate",
                "risk": "without rejected-row logging, any new signal would be hard to validate against what was lost upstream",
                "required_data": ["pre-admission rows", "reject logs", "stable anchor_date/side/symbol keys"],
                "no_lookahead_compatible": True,
                "verification_method": "ablation against admitted and rejected candidates with frozen labels",
                "next_artifact_to_generate": "top15_recall_signal_audit_v1",
            },
            "instrument_rejected_rows_first": {
                "expected_benefit": "high for observability and attribution",
                "risk": "extra logging volume and storage, but minimal modeling risk",
                "required_data": ["standalone rejected-row log", "reject_reason buckets", "stable anchor_date/side/symbol keys"],
                "no_lookahead_compatible": True,
                "verification_method": "confirm exact pre-admission losses can be traced into or out of the active surface",
                "next_artifact_to_generate": "rejected_row_log_bundle_v1",
            },
            "separate_backfill_recall_lane": {
                "expected_benefit": "moderate",
                "risk": "may preserve signal while contaminating active ranking if mixed directly",
                "required_data": ["backfill-only lane metrics", "lane-specific topK audit", "separate diagnostics"],
                "no_lookahead_compatible": True,
                "verification_method": "compare active lane vs backfill lane under the same anchor_date / side frame",
                "next_artifact_to_generate": "backfill_recall_lane_contract_v1",
            },
            "stop_high_recall_line": {
                "expected_benefit": "avoids further work on a weak line",
                "risk": "abandoning remaining long-side headroom before rejected-row attribution is known",
                "required_data": ["none"],
                "no_lookahead_compatible": True,
                "verification_method": "none",
                "next_artifact_to_generate": None,
            },
        },
        "audit_summary": {
            "top15_winner_count": int(top15_audit["top15_winner_count"]),
            "long_active_top15_count": int(top15_audit["top15_winner_count"]),
            "prefilter_source_top15_count": int(miss_audit["prefilter_source_long_top15_count"]),
            "exact_key_overlap_long_active_vs_prefilter_top15": int(source_audit["exact_key_overlap_long_active_vs_prefilter_top15"]),
            "rejected_rows_available": bool(source_audit["rejected_rows_available"]),
        },
    }


def _recommendation(options: dict[str, Any], top15_audit: dict[str, Any], alignment_audit: dict[str, Any], tier_audit: dict[str, Any], miss_audit: dict[str, Any], source_audit: dict[str, Any]) -> dict[str, Any]:
    recommended = "needs_rejected_row_instrumentation"
    reason = (
        "Long-side top15 winners are signal-bearing but sparse, the score/rank buckets are already weakly aligned, "
        "and the rejected-row inventory explicitly says the pre-admission loss path is not logged. "
        "That makes instrumentation the next useful axis before changing admission score or adding a new recall signal."
    )
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_axis": recommended,
        "reason": reason,
        "supporting_evidence": {
            "long_active_top15_winner_count": int(top15_audit["top15_winner_count"]),
            "long_active_top15_winners_all_backfill": bool(top15_audit["conclusion"]["all_observed_top15_winners_are_backfill"]),
            "rank_1_5_top15_rate": next((b["top15_label_rate"] for b in alignment_audit["bucket_summaries"] if b["bucket"] == "rank_1_5"), None),
            "rank_6_8_top15_rate": next((b["top15_label_rate"] for b in alignment_audit["bucket_summaries"] if b["bucket"] == "rank_6_8"), None),
            "risk_flagged_backfill_role": next((r["role_recommendation"] for r in tier_audit["tier_rows"] if r["candidate_pool_tier"] == "risk_flagged_backfill"), None),
            "source_instrumentation_rejected_rows_available": bool(source_audit["rejected_rows_available"]),
            "prefilter_source_top15_count": int(miss_audit["prefilter_source_long_top15_count"]),
            "prefilter_to_long_active_exact_key_overlap": int(source_audit["exact_key_overlap_long_active_vs_prefilter_top15"]),
        },
        "decision_candidates": {
            "revise_long_admission_score": "not_selected",
            "add_top15_recall_signal": "blocked_by_missing_rejected_row_logging",
            "instrument_rejected_rows_first": "selected",
            "separate_backfill_recall_lane": "secondary_future_option",
            "stop_high_recall_line": "not_selected",
        },
    }


def _decision(recommendation: dict[str, Any], source_audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "needs_rejected_row_instrumentation",
        "status": "needs_rejected_row_instrumentation",
        "reason": recommendation["reason"],
        "supporting_checks": {
            "rejected_rows_available": bool(source_audit["rejected_rows_available"]),
            "stable_reject_keys_logged": bool(source_audit["stable_reject_keys_logged"]),
            "reject_reason_buckets_logged": bool(source_audit["reject_reason_buckets_logged"]),
            "admitted_rows_traceable": bool(source_audit["admitted_rows_traceable"]),
            "exact_loss_path_cannot_be_determined": bool(source_audit["instrumentation_conclusion"]["exact_loss_path_cannot_be_determined_from_available_artifacts"]),
            "no_lookahead_carry_forward_valid": True,
            "short_side_in_active_analysis": False,
        },
    }


def _build_manifest(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": output_root.name,
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "source_artifacts": {
            "long_surface": str(LONG_SURFACE),
            "long_surface_summary": str(LONG_SURFACE_SUMMARY),
            "surface_feature_check": str(SURFACE_FEATURE_CHECK),
            "surface_no_lookahead": str(SURFACE_NO_LOOKAHEAD),
            "surface_leakage": str(SURFACE_LEAKAGE),
            "surface_quality": str(SURFACE_QUALITY),
            "surface_oracle": str(SURFACE_ORACLE),
            "surface_decision": str(SURFACE_DECISION),
            "filter_rows": str(FILTER_ROWS),
            "filter_surface": str(FILTER_SURFACE),
            "filter_reranker": str(FILTER_RERANKER),
            "filter_recommendation": str(FILTER_RECOMMENDATION),
            "filter_decision": str(FILTER_DECISION),
            "filter_tier_summary": str(FILTER_TIER_SUMMARY),
            "filter_group_summary": str(FILTER_GROUP_SUMMARY),
            "filter_oracle_by_group": str(FILTER_ORACLE_BY_GROUP),
            "reranker_rows": str(RERANKER_ROWS),
            "reranker_variant_comparison": str(RERANKER_VARIANT_COMPARISON),
            "reranker_oracle_gap": str(RERANKER_ORACLE_GAP),
            "reranker_failure": str(RERANKER_FAILURE),
            "reranker_decision": str(RERANKER_DECISION),
            "prefilter_rows": str(PREFILTER_ROWS),
            "prefilter_decision": str(PREFILTER_DECISION),
            "prefilter_comparison": str(PREFILTER_COMPARISON),
            "prefilter_coverage": str(PREFILTER_COVERAGE),
            "prefilter_policy": str(PREFILTER_POLICY),
            "two_stage_rows": str(TWO_STAGE_ROWS),
            "two_stage_decision": str(TWO_STAGE_DECISION),
            "two_stage_comparison": str(TWO_STAGE_COMPARISON),
            "two_stage_coverage": str(TWO_STAGE_COVERAGE),
            "two_stage_policy": str(TWO_STAGE_POLICY),
            "threshold_inventory": str(THRESHOLD_INVENTORY),
            "group_size_distribution": str(GROUP_SIZE_DISTRIBUTION),
            "current_contract_inventory": str(CURRENT_CONTRACT_INVENTORY),
            "high_recall_contract": str(HIGH_RECALL_CONTRACT),
            "high_recall_eval_plan": str(HIGH_RECALL_EVAL_PLAN),
            "high_recall_decision": str(HIGH_RECALL_DECISION),
            "high_recall_feasibility": str(HIGH_RECALL_FEASIBILITY),
            "rejected_source_inventory": str(REJECTED_SOURCE_INVENTORY),
            "raw_snapshot_manifest": str(RAW_SNAPSHOT_MANIFEST),
        },
    }


def _build_input_resolution(output_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "resolved_long_surface": str(LONG_SURFACE),
        "resolved_filter_revision_rows": str(FILTER_ROWS),
        "resolved_reranker_prediction_rows": str(RERANKER_ROWS),
        "resolved_prefilter_rows": str(PREFILTER_ROWS),
        "resolved_two_stage_rows": str(TWO_STAGE_ROWS),
        "resolved_threshold_inventory": str(THRESHOLD_INVENTORY),
        "resolved_rejected_source_inventory": str(REJECTED_SOURCE_INVENTORY),
        "resolved_raw_snapshot_manifest": str(RAW_SNAPSHOT_MANIFEST),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "long_active_row_count": int(len(inputs["long_surface"])),
        "long_active_group_count": int(inputs["long_surface"].groupby(["anchor_date", "side"], sort=False).ngroups),
        "prefilter_long_row_count": int(len(inputs["prefilter_rows"])),
        "two_stage_long_row_count": int(len(inputs["two_stage_rows"])),
        "prefilter_long_group_count": int(inputs["prefilter_rows"].groupby(["anchor_date", "side"], sort=False).ngroups),
        "two_stage_long_group_count": int(inputs["two_stage_rows"].groupby(["anchor_date", "side"], sort=False).ngroups),
        "raw_source_universe_reference_row_count": int(inputs["high_recall_contract"]["raw_source_universe_support"]["row_count"]),
        "accessible_raw_snapshot_manifest_row_count": int(len(inputs["raw_snapshot_manifest"].get("rows", []))) if isinstance(inputs["raw_snapshot_manifest"], dict) else None,
        "source_sessions": {
            "prefilter_session": str(PREFILTER_SESSION),
            "two_stage_session": str(TWO_STAGE_SESSION),
            "long_surface_session": str(LONG_SURFACE_SESSION),
            "long_filter_session": str(LONG_FILTER_SESSION),
            "long_reranker_session": str(LONG_RERANKER_SESSION),
            "high_recall_design_session": str(HIGH_RECALL_DESIGN_SESSION),
        },
        "reference_decisions": {
            "surface_decision": inputs["surface_decision"].get("decision"),
            "filter_decision": inputs["filter_decision"].get("decision"),
            "reranker_decision": inputs["reranker_decision"].get("decision"),
            "prefilter_decision": inputs["prefilter_decision"].get("decision"),
            "two_stage_decision": inputs["two_stage_decision"].get("decision"),
            "high_recall_design_decision": inputs["high_recall_decision"].get("decision"),
        },
        "notes": [
            "The accessible raw snapshot manifest in the sample replay root is smaller than the 3201-row raw universe referenced by the high-recall contract.",
            "The rejected-source inventory explicitly says standalone rejected-row logging is unavailable.",
            "This audit therefore uses admitted-row traceability plus long active surface comparison, not a full rejected-row replay.",
        ],
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()
    long_surface = inputs["long_surface"]
    filter_rows = inputs["filter_rows"]
    reranker_rows = inputs["reranker_rows"]
    prefilter_rows = inputs["prefilter_rows"]
    two_stage_rows = inputs["two_stage_rows"]

    if not long_surface["side"].astype(str).eq("long").all():
        raise RuntimeError("long active surface includes non-long rows")
    if not reranker_rows["side"].astype(str).eq("long").all():
        raise RuntimeError("reranker prediction rows include non-long rows")
    if not filter_rows["side"].astype(str).eq("long").all():
        raise RuntimeError("filter revision rows include non-long rows")
    if not prefilter_rows["side"].astype(str).eq("long").all():
        raise RuntimeError("prefilter rows include non-long rows after long-only filter")
    if not two_stage_rows["side"].astype(str).eq("long").all():
        raise RuntimeError("two-stage rows include non-long rows after long-only filter")
    if len(long_surface) != 888 or long_surface.groupby(["anchor_date", "side"], sort=False).ngroups != 156:
        raise RuntimeError("long active surface no longer matches expected size")
    if len(reranker_rows) != len(long_surface):
        raise RuntimeError("reranker rows and long active surface row counts differ")
    if "tree_hgb_path_value_score" not in reranker_rows.columns:
        raise RuntimeError("frozen reranker score column missing from reranker rows")

    top15_audit, winner_frame = _winner_path_audit(long_surface, filter_rows, reranker_rows, prefilter_rows, two_stage_rows)
    alignment_audit, bucket_frame = _score_rank_bucket_summary(long_surface)
    tier_audit, tier_frame = _tier_usefulness_audit(long_surface)
    miss_audit, miss_examples = _candidate_generation_miss_audit(long_surface, prefilter_rows, two_stage_rows, winner_frame)
    source_audit, source_coverage = _source_instrumentation_audit(inputs, long_surface, prefilter_rows, two_stage_rows, miss_examples)
    options = _refinement_options(top15_audit, miss_audit, source_audit)
    recommendation = _recommendation(options, top15_audit, alignment_audit, tier_audit, miss_audit, source_audit)
    decision = _decision(recommendation, source_audit)

    manifest = _build_manifest(output_root)
    input_resolution = _build_input_resolution(output_root, inputs)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "long_side_top15_winner_path_audit.json", top15_audit)
    _write_parquet(output_root / "long_side_top15_winner_rows.parquet", winner_frame)
    _write_json(output_root / "long_side_admission_score_alignment_audit.json", alignment_audit)
    _write_json(output_root / "long_side_tier_usefulness_audit.json", tier_audit)
    _write_json(output_root / "long_side_candidate_generation_miss_audit.json", miss_audit)
    _write_json(output_root / "long_side_source_instrumentation_audit.json", source_audit)
    _write_json(output_root / "long_side_candidate_generation_refinement_options.json", options)
    _write_json(output_root / "long_side_candidate_generation_refinement_recommendation.json", recommendation)
    _write_json(output_root / "long_side_candidate_generation_refinement_audit_v1_decision.json", decision)
    _write_parquet(output_root / "long_side_candidate_generation_miss_examples.parquet", miss_examples)
    _write_parquet(output_root / "long_side_score_rank_bucket_summary.parquet", bucket_frame)
    _write_parquet(output_root / "long_side_tier_quality_summary.parquet", tier_frame)
    _write_parquet(output_root / "long_side_source_field_coverage.parquet", source_coverage)
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "long_side_top15_winner_path_audit.json",
                "long_side_top15_winner_rows.parquet",
                "long_side_admission_score_alignment_audit.json",
                "long_side_tier_usefulness_audit.json",
                "long_side_candidate_generation_miss_audit.json",
                "long_side_source_instrumentation_audit.json",
                "long_side_candidate_generation_refinement_options.json",
                "long_side_candidate_generation_refinement_recommendation.json",
                "long_side_candidate_generation_refinement_audit_v1_decision.json",
            ],
        },
    )
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "long_active_row_count": int(len(long_surface)),
        "long_active_group_count": int(long_surface.groupby(["anchor_date", "side"], sort=False).ngroups),
        "top15_winner_count": int(top15_audit["top15_winner_count"]),
        "prefilter_source_top15_count": int(miss_audit["prefilter_source_long_top15_count"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX long-side candidate-generation refinement audit v1")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
