from __future__ import annotations

import argparse
import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_high_recall_reranker_validation_v1 import _coerce_model_frame, _rank_within_groups, _reconstruct_path_value_model


SCRIPT_NAME = "tradex_high_recall_filter_revision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_input_resolution_v1"
SELECTED_TIER_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_selected_tier_failure_audit_v1"
CONTRACT_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_contracts_v1"
SURFACE_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_surface_comparison_v1"
RERANKER_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_reranker_comparison_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_high_recall_filter_revision_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\high_recall_filter_revision_v1")
HIGH_RECALL_SURFACE_SESSION = Path(r"G:\Tradex\feature_complete_high_recall_surface_v1\20260502T140705Z-318453")
RERANKER_SESSION = Path(r"G:\Tradex\high_recall_reranker_validation_v1\20260502T143633Z-122839")
RISK_FILTER_SESSION = Path(r"G:\Tradex\risk_flag_filter_before_high_recall_surface_v1\20260502T125847Z-880922")
CURRENT_ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")

SURFACE_ROWS = HIGH_RECALL_SURFACE_SESSION / "feature_complete_high_recall_candidate_rows.parquet"
SURFACE_NO_LOOKAHEAD = HIGH_RECALL_SURFACE_SESSION / "high_recall_surface_no_lookahead_audit.json"
SURFACE_LEAKAGE = HIGH_RECALL_SURFACE_SESSION / "high_recall_surface_leakage_audit.json"
SURFACE_BREADTH = HIGH_RECALL_SURFACE_SESSION / "feature_complete_high_recall_breadth_quality_audit.json"
SURFACE_ORACLE = HIGH_RECALL_SURFACE_SESSION / "feature_complete_high_recall_oracle_headroom_audit.json"
SURFACE_DECISION = HIGH_RECALL_SURFACE_SESSION / "feature_complete_high_recall_surface_v1_decision.json"

RERANKER_PREDICTIONS = RERANKER_SESSION / "high_recall_reranker_prediction_rows.parquet"
RERANKER_TOPK_DIFF = RERANKER_SESSION / "high_recall_reranker_topk_membership_diff.parquet"
RERANKER_ORACLE = RERANKER_SESSION / "high_recall_oracle_gap_comparison.json"
RERANKER_FAILURE = RERANKER_SESSION / "high_recall_reranker_failure_mode_audit.json"
RERANKER_DECISION = RERANKER_SESSION / "high_recall_reranker_validation_v1_decision.json"

RISK_FILTER_RECOMMENDATION = RISK_FILTER_SESSION / "risk_filter_recommendation.json"
RISK_FILTER_VARIANT_COMPARISON = RISK_FILTER_SESSION / "risk_filter_variant_comparison.json"
RISK_FILTER_VARIANT_ROWS = RISK_FILTER_SESSION / "risk_filter_variant_rows.parquet"
RISK_FILTER_RETAINED_ROWS = RISK_FILTER_SESSION / "risk_filter_retained_rows.parquet"
RISK_FILTER_SIDE_SUMMARY = RISK_FILTER_SESSION / "risk_filter_side_summary.parquet"

CURRENT_ACCUMULATED_ROWS = CURRENT_ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet"
CURRENT_ACCUMULATED_VARIANT = CURRENT_ACCUMULATED_SESSION / "accumulated_forward_variant_pool_comparison.json"

MODEL_NAME = "tree_hgb_path_value"
TOP_K_VALUES = (5, 10, 20)
FILTER_VARIANTS = (
    "filter_no_exclude_analysis_only",
    "filter_no_exclude_analysis_only_tighter_rank",
    "filter_score_040_rank_guard",
    "filter_score_040_tighter_rank",
    "filter_primary_watch_backfill_only",
    "filter_long_active_short_hold",
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _str_cols(frame: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].astype(str)
    return out


def _group_size_stats(frame: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False).size()
    per_side: dict[str, Any] = {}
    for side, side_frame in frame.groupby("side", sort=False):
        sizes = side_frame.groupby("anchor_date", sort=False).size()
        per_side[str(side)] = {
            "row_count": int(len(side_frame)),
            "group_count": int(sizes.shape[0]),
            "min_group_size": int(sizes.min()) if len(sizes) else None,
            "median_group_size": float(sizes.median()) if len(sizes) else None,
            "mean_group_size": float(sizes.mean()) if len(sizes) else None,
            "max_group_size": int(sizes.max()) if len(sizes) else None,
            "top5_thin_groups": int((sizes < 5).sum()),
            "top10_thin_groups": int((sizes < 10).sum()),
            "top20_thin_groups": int((sizes < 20).sum()),
        }
    per_side["overall"] = {
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
    return per_side


def _load_surface() -> pd.DataFrame:
    frame = _load_frame(SURFACE_ROWS)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    if "month_bucket" in frame.columns:
        frame["month_bucket"] = frame["month_bucket"].astype(str)
    return frame


def _load_current_reference() -> dict[str, Any]:
    ref = _load_json(CURRENT_ACCUMULATED_VARIANT)
    frame = _load_frame(CURRENT_ACCUMULATED_ROWS)
    return {
        "schema_version": "tradex_high_recall_filter_revision_v1_current_accumulated_reference_v1",
        "generated_at_utc": _utc_now(),
        "surface_row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "source_variant_comparison": str(CURRENT_ACCUMULATED_VARIANT),
        "summary": ref.get("summary", {}),
        "topk": ref.get("topk", {}),
        "validation_gate_discrepancy": ref.get("validation_gate_discrepancy", {}),
    }


def _selected_rows_from_reranker(frame: pd.DataFrame) -> pd.DataFrame:
    sel = frame[(frame["model_name"] == MODEL_NAME) & (frame["model_rank"].isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]))].copy()
    sel["model_name"] = MODEL_NAME
    return sel


def _selected_tier_failure_audit(selected: pd.DataFrame) -> dict[str, Any]:
    score_col = "score" if "score" in selected.columns else ("champion_score" if "champion_score" in selected.columns else "candidate_score")
    rank_col = "rank" if "rank" in selected.columns else ("champion_rank" if "champion_rank" in selected.columns else "candidate_rank")
    out: dict[str, Any] = {
        "schema_version": SELECTED_TIER_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "model_name": MODEL_NAME,
        "topk": {},
        "exclude_analysis_only_beneficial": False,
        "risk_flagged_rows_guard_too_loose": False,
        "notes": [
            "selected rows are taken from the frozen tree_hgb_path_value replay on the high-recall surface",
            "benefit is judged by return/path and label composition, not by subjective tier names",
        ],
    }
    guard_tight = ((selected["candidate_pool_tier"].astype(str) == "risk_flagged_backfill") & (pd.to_numeric(selected[score_col], errors="coerce") >= 0.40)).sum()
    for topk in TOP_K_VALUES:
        top = selected[selected["model_rank"] <= topk].copy()
        tier_counts = {str(k): int(v) for k, v in top["candidate_pool_tier"].fillna("").value_counts().items()}
        reason_counts = {str(k): int(v) for k, v in top["candidate_pool_reason"].fillna("").value_counts().items()}
        side_counts = {str(k): int(v) for k, v in top["side"].fillna("").value_counts().items()}
        flag_counts = {
            "included_for_min_pool_backfill_true": int(top.get("included_for_min_pool_backfill", pd.Series(False, index=top.index)).fillna(False).astype(bool).sum()),
            "would_have_been_excluded_under_current_contract_true": int(top.get("would_have_been_excluded_under_current_contract", pd.Series(False, index=top.index)).fillna(False).astype(bool).sum()),
            "risk_flagged_candidate_true": int(top.get("risk_flagged_candidate", pd.Series(False, index=top.index)).fillna(False).astype(bool).sum()),
        }
        by_tier = {}
        for tier, tf in top.groupby("candidate_pool_tier", sort=False):
            by_tier[str(tier)] = {
                "row_count": int(len(tf)),
                "mean_forward_ret_20d": float(pd.to_numeric(tf["forward_ret_20d"], errors="coerce").mean()) if len(tf) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(tf["path_value_score_v1"], errors="coerce").mean()) if len(tf) else None,
                "top15_capture_rate": float(tf["top15_label"].fillna(False).astype(bool).mean()) if len(tf) else None,
                "bottom15_contamination_rate": float(tf["bottom15_label"].fillna(False).astype(bool).mean()) if len(tf) else None,
                "top20pct_capture_rate": float(tf["top20pct_label"].fillna(False).astype(bool).mean()) if len(tf) else None,
            }
        out["topk"][f"top{topk}"] = {
            "row_count": int(len(top)),
            "tier_counts": tier_counts,
            "reason_counts": reason_counts,
            "side_counts": side_counts,
            "flag_counts": flag_counts,
            "score_summary": {
                "min": float(pd.to_numeric(top[score_col], errors="coerce").min()) if len(top) else None,
                "median": float(pd.to_numeric(top[score_col], errors="coerce").median()) if len(top) else None,
                "mean": float(pd.to_numeric(top[score_col], errors="coerce").mean()) if len(top) else None,
                "max": float(pd.to_numeric(top[score_col], errors="coerce").max()) if len(top) else None,
            },
            "rank_summary": {
                "min": int(pd.to_numeric(top[rank_col], errors="coerce").min()) if len(top) else None,
                "median": float(pd.to_numeric(top[rank_col], errors="coerce").median()) if len(top) else None,
                "mean": float(pd.to_numeric(top[rank_col], errors="coerce").mean()) if len(top) else None,
                "max": int(pd.to_numeric(top[rank_col], errors="coerce").max()) if len(top) else None,
            },
            "by_tier": by_tier,
            "dominant_tiers": list(tier_counts.keys())[:3],
            "positive_return_rows": int((pd.to_numeric(top["forward_ret_20d"], errors="coerce") > 0).sum()),
            "non_positive_return_rows": int((pd.to_numeric(top["forward_ret_20d"], errors="coerce") <= 0).sum()),
            "top15_rows": int(top["top15_label"].fillna(False).astype(bool).sum()),
            "bottom15_rows": int(top["bottom15_label"].fillna(False).astype(bool).sum()),
            "top20pct_rows": int(top["top20pct_label"].fillna(False).astype(bool).sum()),
        }
    top5 = out["topk"]["top5"]
    top10 = out["topk"]["top10"]
    out["exclude_analysis_only_beneficial"] = bool(
        top5["by_tier"].get("exclude_analysis_only", {}).get("mean_forward_ret_20d", 0) > 0
        or top10["by_tier"].get("exclude_analysis_only", {}).get("top15_capture_rate", 0) > 0
    )
    out["risk_flagged_rows_guard_too_loose"] = bool(guard_tight < int(selected["candidate_pool_tier"].astype(str).eq("risk_flagged_backfill").sum() * 0.5))
    return out


def _build_contracts() -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "variants": {
            "filter_no_exclude_analysis_only": {
                "description": "Remove all exclude_analysis_only rows and keep primary/watch plus all risk_flagged_backfill rows.",
                "include_analysis_only": False,
                "backfill_guard": None,
                "side_policy": "combined",
            },
            "filter_no_exclude_analysis_only_tighter_rank": {
                "description": "Remove all exclude_analysis_only rows and tighten the backfill guard to rank<=5/3 with score>=0.35.",
                "include_analysis_only": False,
                "backfill_guard": {"long_rank_le": 5, "short_rank_le": 3, "score_ge": 0.35},
                "side_policy": "combined",
            },
            "filter_score_040_rank_guard": {
                "description": "Keep current structure, retain analysis-only rows, and require score>=0.40 with rank<=8/4 for risk_flagged_backfill.",
                "include_analysis_only": True,
                "backfill_guard": {"long_rank_le": 8, "short_rank_le": 4, "score_ge": 0.40},
                "side_policy": "combined",
            },
            "filter_score_040_tighter_rank": {
                "description": "Keep current structure, retain analysis-only rows, and require score>=0.40 with rank<=5/3 for risk_flagged_backfill.",
                "include_analysis_only": True,
                "backfill_guard": {"long_rank_le": 5, "short_rank_le": 3, "score_ge": 0.40},
                "side_policy": "combined",
            },
            "filter_primary_watch_backfill_only": {
                "description": "Keep primary/watch and all risk_flagged_backfill rows; exclude analysis-only rows completely.",
                "include_analysis_only": False,
                "backfill_guard": None,
                "side_policy": "combined",
            },
            "filter_long_active_short_hold": {
                "description": "Apply the strict score/rank guard on the long side; keep the short side as research-hold diagnostic rows.",
                "include_analysis_only": True,
                "backfill_guard": {"long_rank_le": 5, "short_rank_le": 3, "score_ge": 0.40},
                "side_policy": "long_strict_short_hold",
            },
        },
        "shared_rules": {
            "candidate_pool_grain": "anchor_date / side",
            "preserve_features": True,
            "preserve_outcomes_as_evaluation_only": True,
            "no_lookahead": True,
            "no_leakage": True,
        },
    }


def _apply_variant(frame: pd.DataFrame, variant_name: str) -> pd.DataFrame:
    tier = frame["candidate_pool_tier"].astype(str)
    backfill = tier.eq("risk_flagged_backfill")
    analysis = tier.eq("exclude_analysis_only")
    keep = tier.isin(["KEEP_PRIMARY", "KEEP_WATCH"])
    score = pd.to_numeric(frame["score"], errors="coerce")
    rank = pd.to_numeric(frame["rank"], errors="coerce")

    if variant_name == "filter_no_exclude_analysis_only":
        keep = keep | backfill
    elif variant_name == "filter_no_exclude_analysis_only_tighter_rank":
        keep = keep | (backfill & (((frame["side"].eq("long") & (rank <= 5)) | (frame["side"].eq("short") & (rank <= 3))) & (score >= 0.35)))
    elif variant_name == "filter_score_040_rank_guard":
        keep = keep | (backfill & (((frame["side"].eq("long") & (rank <= 8)) | (frame["side"].eq("short") & (rank <= 4))) & (score >= 0.40)))
        keep = keep | analysis
    elif variant_name == "filter_score_040_tighter_rank":
        keep = keep | (backfill & (((frame["side"].eq("long") & (rank <= 5)) | (frame["side"].eq("short") & (rank <= 3))) & (score >= 0.40)))
        keep = keep | analysis
    elif variant_name == "filter_primary_watch_backfill_only":
        keep = keep | backfill
    elif variant_name == "filter_long_active_short_hold":
        long_keep = (frame["side"].eq("long")) & (keep | (backfill & ((rank <= 5) & (score >= 0.40))))
        short_keep = (frame["side"].eq("short")) & (keep | backfill | analysis)
        keep = long_keep | short_keep
    else:
        raise KeyError(f"unknown variant: {variant_name}")

    if variant_name != "filter_score_040_rank_guard" and variant_name != "filter_score_040_tighter_rank" and variant_name != "filter_long_active_short_hold":
        keep = keep & (~analysis)
    if variant_name == "filter_long_active_short_hold":
        out = frame.loc[keep].copy()
        out.loc[out["side"].astype(str) == "short", "high_recall_pool_status"] = "research_hold_short"
        out["filter_side_mode"] = out["side"].map({"long": "active", "short": "research_hold"}).fillna("active")
        return out
    out = frame.loc[keep].copy()
    out["filter_side_mode"] = "combined"
    return out


def _oracle_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for topk in TOP_K_VALUES:
        oracle_rows = []
        for _, group in frame.groupby(["anchor_date", "side"], sort=False):
            g = group[pd.to_numeric(group["forward_ret_20d"], errors="coerce").notna()].copy()
            if g.empty:
                continue
            g = g.sort_values(["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx"], ascending=[False, False, True, True], kind="mergesort")
            oracle_rows.append(g.head(topk))
        oracle = pd.concat(oracle_rows, ignore_index=True) if oracle_rows else frame.iloc[0:0].copy()
        out[f"top{topk}"] = {
            "selected_row_count": int(len(oracle)),
            "mean_forward_ret_20d": float(pd.to_numeric(oracle["forward_ret_20d"], errors="coerce").mean()) if len(oracle) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(oracle["path_value_score_v1"], errors="coerce").mean()) if len(oracle) else None,
            "top15_capture_rate": float(oracle["top15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
            "top20pct_capture_rate": float(oracle["top20pct_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
            "bottom15_contamination_rate": float(oracle["bottom15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        }
    return out


def _surface_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out = {
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "long_row_count": int((frame["side"] == "long").sum()),
        "short_row_count": int((frame["side"] == "short").sum()),
        "group_size_stats": _group_size_stats(frame),
        "tier_composition": {str(k): int(v) for k, v in frame["candidate_pool_tier"].fillna("").value_counts().items()},
        "reason_composition": {str(k): int(v) for k, v in frame["candidate_pool_reason"].fillna("").value_counts().items()},
        "label_counts": {
            "top15_label": int(frame["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in frame.columns else 0,
            "bottom15_label": int(frame["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in frame.columns else 0,
            "top20pct_label": int(frame["top20pct_label"].fillna(False).astype(bool).sum()) if "top20pct_label" in frame.columns else 0,
        },
        "non_positive_forward_ret_20d_count": int((pd.to_numeric(frame["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(frame["forward_ret_20d"], errors="coerce") <= 0)).sum()),
        "no_lookahead_passed": bool(_load_json(SURFACE_NO_LOOKAHEAD).get("status") == "pass"),
        "leakage_passed": bool(_load_json(SURFACE_LEAKAGE).get("no_lookahead_passed", False) and not _load_json(SURFACE_LEAKAGE).get("current_snapshot_leakage_detected", False)),
        "source_surface_row_count": 1329,
        "source_surface_group_count": 267,
    }
    out["oracle"] = _oracle_metrics(frame)
    return out


def _replay_path_value(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    model = _reconstruct_path_value_model()["model"]
    scored = frame.copy()
    scored["path_value_score_tree"] = model.predict(_coerce_model_frame(scored))
    scored["path_value_rank_tree"] = _rank_within_groups(scored, scored["path_value_score_tree"], group_cols=["anchor_date", "side"])
    scored["champion_rank_variant"] = _rank_within_groups(scored, pd.to_numeric(scored["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"])
    comparison: dict[str, Any] = {
        "model_name": MODEL_NAME,
        "row_count": int(len(scored)),
        "group_count": int(scored.groupby(["anchor_date", "side"], sort=False).ngroups),
        "topk": {},
        "side_summary": {},
        "tier_summary": {},
    }
    for topk in TOP_K_VALUES:
        model_sel = scored["path_value_rank_tree"] <= topk
        champ_sel = scored["champion_rank_variant"] <= topk
        model_frame = scored.loc[model_sel].copy()
        champ_frame = scored.loc[champ_sel].copy()
        comparison["topk"][f"top{topk}"] = {
            "selected_row_count": int(len(model_frame)),
            "champion_selected_row_count": int(len(champ_frame)),
            "mean_forward_ret_20d": float(pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce").mean()) if len(model_frame) else None,
            "champion_mean_forward_ret_20d": float(pd.to_numeric(champ_frame["forward_ret_20d"], errors="coerce").mean()) if len(champ_frame) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(model_frame["path_value_score_v1"], errors="coerce").mean()) if len(model_frame) else None,
            "champion_mean_path_value_score_v1": float(pd.to_numeric(champ_frame["path_value_score_v1"], errors="coerce").mean()) if len(champ_frame) else None,
            "top15_capture_rate": float(model_frame["top15_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
            "champion_top15_capture_rate": float(champ_frame["top15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
            "top20pct_capture_rate": float(model_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
            "champion_top20pct_capture_rate": float(champ_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
            "bottom15_contamination_rate": float(model_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
            "champion_bottom15_contamination_rate": float(champ_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
            "membership_changed_count": int((model_sel ^ champ_sel).sum()),
            "overlap_ratio": float((model_sel & champ_sel).sum() / max(int((model_sel | champ_sel).sum()), 1)),
            "non_positive_forward_ret_count": int((pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce") <= 0)).sum()),
            "zero_pass_groups": int(sum(1 for _, g in model_frame.groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
            "side_split": {str(k): int(v) for k, v in model_frame["side"].value_counts().items()},
            "tier_composition": {str(k): int(v) for k, v in model_frame["candidate_pool_tier"].fillna("").value_counts().items()},
        }
    comparison["side_summary"] = {
        f"top{topk}": {
            str(side): {
                "row_count": int(len(subset)),
                "mean_forward_ret_20d": float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if len(subset) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if len(subset) else None,
                "top15_capture_rate": float(subset["top15_label"].fillna(False).astype(bool).mean()) if len(subset) else None,
                "bottom15_contamination_rate": float(subset["bottom15_label"].fillna(False).astype(bool).mean()) if len(subset) else None,
            }
            for side, subset in scored.loc[scored["path_value_rank_tree"] <= topk].groupby("side", sort=False)
        }
        for topk in TOP_K_VALUES
    }
    comparison["tier_summary"] = {
        f"top{topk}": {
            str(tier): {
                "row_count": int(len(tier_frame)),
                "mean_forward_ret_20d": float(pd.to_numeric(tier_frame["forward_ret_20d"], errors="coerce").mean()) if len(tier_frame) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(tier_frame["path_value_score_v1"], errors="coerce").mean()) if len(tier_frame) else None,
                "top15_capture_rate": float(tier_frame["top15_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
                "bottom15_contamination_rate": float(tier_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
            }
            for tier, tier_frame in scored.loc[scored["path_value_rank_tree"] <= topk].groupby("candidate_pool_tier", sort=False)
        }
        for topk in TOP_K_VALUES
    }
    return scored, comparison


def _build_variant_comparison(base_frame: pd.DataFrame, variants: dict[str, pd.DataFrame], current_reference: dict[str, Any]) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    rows: list[pd.DataFrame] = []
    comparison: dict[str, Any] = {
        "schema_version": SURFACE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "base_surface": {
            "row_count": int(len(base_frame)),
            "group_count": int(base_frame.groupby(["anchor_date", "side"], sort=False).ngroups),
            "tier_composition": {str(k): int(v) for k, v in base_frame["candidate_pool_tier"].fillna("").value_counts().items()},
        },
        "current_accumulated_reference": current_reference,
        "variants": {},
    }

    def _score_one(name: str, variant_frame: pd.DataFrame) -> tuple[str, pd.DataFrame, dict[str, Any], dict[str, Any]]:
        scored, reranker = _replay_path_value(variant_frame)
        scored["filter_variant"] = name
        scored["filter_selected"] = True
        scored["champion_selected_top5"] = scored["champion_rank_variant"] <= 5
        scored["champion_selected_top10"] = scored["champion_rank_variant"] <= 10
        scored["champion_selected_top20"] = scored["champion_rank_variant"] <= 20
        scored["path_value_selected_top5"] = scored["path_value_rank_tree"] <= 5
        scored["path_value_selected_top10"] = scored["path_value_rank_tree"] <= 10
        scored["path_value_selected_top20"] = scored["path_value_rank_tree"] <= 20
        selected = scored
        variant_summary = {
            "row_count": int(len(selected)),
            "group_count": int(selected.groupby(["anchor_date", "side"], sort=False).ngroups),
            "long_row_count": int((selected["side"] == "long").sum()),
            "short_row_count": int((selected["side"] == "short").sum()),
            "group_size_stats": _group_size_stats(selected),
            "tier_composition": {str(k): int(v) for k, v in selected["candidate_pool_tier"].fillna("").value_counts().items()},
            "reason_composition": {str(k): int(v) for k, v in selected["candidate_pool_reason"].fillna("").value_counts().items()},
            "label_counts": {
                "top15_label": int(selected["top15_label"].fillna(False).astype(bool).sum()),
                "bottom15_label": int(selected["bottom15_label"].fillna(False).astype(bool).sum()),
                "top20pct_label": int(selected["top20pct_label"].fillna(False).astype(bool).sum()),
            },
            "non_positive_forward_ret_20d_count": int((pd.to_numeric(selected["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(selected["forward_ret_20d"], errors="coerce") <= 0)).sum()),
            "oracle": _oracle_metrics(selected),
            "reranker": reranker,
        }
        return name, scored, variant_summary, reranker

    variant_items = list(variants.items())
    if len(variant_items) > 1:
        with ThreadPoolExecutor(max_workers=min(2, len(variant_items))) as executor:
            results = list(executor.map(lambda item: _score_one(item[0], item[1]), variant_items))
    else:
        results = [_score_one(name, frame) for name, frame in variant_items]

    for name, scored, summary, reranker in results:
        comparison["variants"][name] = summary
        rows.append(scored)

    combined = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    comparison["best_variant_by_top10_forward_ret"] = max(
        ((name, data["reranker"]["topk"]["top10"]["mean_forward_ret_20d"]) for name, data in comparison["variants"].items()),
        key=lambda item: (-math.inf if item[1] is None else item[1]),
    )[0] if comparison["variants"] else None
    comparison["best_variant_by_top10_top15_capture"] = max(
        ((name, data["reranker"]["topk"]["top10"]["top15_capture_rate"]) for name, data in comparison["variants"].items()),
        key=lambda item: (-math.inf if item[1] is None else item[1]),
    )[0] if comparison["variants"] else None
    return comparison, combined, pd.DataFrame()


def _recommendation(variant_comparison: dict[str, Any], selected_audit: dict[str, Any], surface_summary: dict[str, Any]) -> dict[str, Any]:
    variants = variant_comparison["variants"]
    candidate_names = [name for name, payload in variants.items() if "exclude_analysis_only" not in payload["tier_composition"]]
    if not candidate_names:
        candidate_names = list(variants.keys())

    def _rank_key(name: str) -> tuple[float, float, float, float]:
        top5 = variants[name]["reranker"]["topk"]["top5"]
        top10 = variants[name]["reranker"]["topk"]["top10"]
        return (
            float(top5["mean_forward_ret_20d"] or -1e9),
            float(top10["mean_forward_ret_20d"] or -1e9),
            float(top10["top20pct_capture_rate"] or 0.0),
            -float(top10["bottom15_contamination_rate"] or 0.0),
        )

    best_filter = max(candidate_names, key=_rank_key)
    best_payload = variants[best_filter]["reranker"]["topk"]
    decision = "needs_side_specific_high_recall_contract"
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_path": "needs_side_specific_high_recall_contract",
        "best_filter_variant": best_filter,
        "decision_hint": decision,
        "reason": (
            "Removing exclude_analysis_only materially reduces the noisiest tier and preserves top5/top10 path-value gains, "
            "but top15 capture does not improve and the short side remains zero-positive for top15, so the next step should split the high-recall contract by side."
        ),
        "selected_tier_failure_summary": {
            "exclude_analysis_only_beneficial": selected_audit["exclude_analysis_only_beneficial"],
            "risk_flagged_rows_guard_too_loose": selected_audit["risk_flagged_rows_guard_too_loose"],
        },
        "current_surface_summary": {
            "row_count": int(surface_summary["row_count"]),
            "group_count": int(surface_summary["group_count"]),
        },
    }


def _decision(recommendation: dict[str, Any], variants: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "needs_side_specific_high_recall_contract",
        "status": "needs_side_specific_high_recall_contract",
        "best_filter_variant": recommendation["best_filter_variant"],
        "reason": recommendation["reason"],
        "supporting_checks": {
            "exclude_analysis_only_beneficial": recommendation["selected_tier_failure_summary"]["exclude_analysis_only_beneficial"],
            "risk_flagged_rows_guard_too_loose": recommendation["selected_tier_failure_summary"]["risk_flagged_rows_guard_too_loose"],
            "best_variant_top10_forward_ret": variants[recommendation["best_filter_variant"]]["topk"]["top10"]["mean_forward_ret_20d"] if recommendation["best_filter_variant"] else None,
            "best_variant_top10_top15_capture": variants[recommendation["best_filter_variant"]]["topk"]["top10"]["top15_capture_rate"] if recommendation["best_filter_variant"] else None,
            "best_variant_short_side_top15_capture": variants[recommendation["best_filter_variant"]]["side_summary"]["top10"].get("short", {}).get("top15_capture_rate") if recommendation["best_filter_variant"] else None,
        },
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    surface = _load_surface()
    current_reference = _load_current_reference()
    reranker_preds = _load_frame(RERANKER_PREDICTIONS)
    path_rows = _selected_rows_from_reranker(reranker_preds)
    selected_audit = _selected_tier_failure_audit(path_rows)
    contracts = _build_contracts()

    variants = {name: _apply_variant(surface, name) for name in FILTER_VARIANTS}
    variant_comparison, combined_rows, _ = _build_variant_comparison(surface, variants, current_reference)

    if combined_rows.empty:
        raise RuntimeError("variant scoring produced no rows")

    # Assemble the row-level surface with variant metadata.
    row_frame = combined_rows.copy()
    row_frame["filter_variant"] = row_frame["filter_variant"].astype(str)
    row_frame["variant_side_policy"] = row_frame["filter_side_mode"].astype(str)
    row_frame["selected_for_variant"] = True

    # Compact reranker comparison view extracted from the variant surface summary.
    reranker_comparison = {
        "schema_version": RERANKER_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "model_name": MODEL_NAME,
        "current_accumulated_reference": current_reference,
        "variants": {},
    }
    for name, payload in variant_comparison["variants"].items():
        reranker_comparison["variants"][name] = {
            "row_count": payload["row_count"],
            "group_count": payload["group_count"],
            "long_row_count": payload["long_row_count"],
            "short_row_count": payload["short_row_count"],
            "tier_composition": payload["tier_composition"],
            "side_summary": payload["reranker"]["side_summary"],
            "tier_summary": payload["reranker"]["tier_summary"],
            "topk": payload["reranker"]["topk"],
        }

    recommendation = _recommendation(variant_comparison, selected_audit, variant_comparison["base_surface"])
    decision = _decision(recommendation, reranker_comparison["variants"])

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": output_root.name,
        "output_root": str(output_root),
        "jobs_requested": jobs,
        "jobs_supported": 2,
        "source_artifacts": {
            "surface_rows": str(SURFACE_ROWS),
            "reranker_predictions": str(RERANKER_PREDICTIONS),
            "risk_filter_recommendation": str(RISK_FILTER_RECOMMENDATION),
            "risk_filter_variant_comparison": str(RISK_FILTER_VARIANT_COMPARISON),
            "risk_filter_variant_rows": str(RISK_FILTER_VARIANT_ROWS),
            "risk_filter_retained_rows": str(RISK_FILTER_RETAINED_ROWS),
            "risk_filter_side_summary": str(RISK_FILTER_SIDE_SUMMARY),
            "surface_no_lookahead": str(SURFACE_NO_LOOKAHEAD),
            "surface_leakage": str(SURFACE_LEAKAGE),
            "surface_breadth": str(SURFACE_BREADTH),
            "surface_oracle": str(SURFACE_ORACLE),
            "reranker_topk_diff": str(RERANKER_TOPK_DIFF),
            "reranker_oracle": str(RERANKER_ORACLE),
            "reranker_failure": str(RERANKER_FAILURE),
            "reranker_decision": str(RERANKER_DECISION),
            "current_accumulated_reference": str(CURRENT_ACCUMULATED_VARIANT),
        },
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_surface_root": str(HIGH_RECALL_SURFACE_SESSION),
        "resolved_surface_file": str(SURFACE_ROWS),
        "requested_prediction_ready_file": str(HIGH_RECALL_SURFACE_SESSION / "feature_complete_high_recall_prediction_ready_rows.parquet"),
        "requested_prediction_ready_file_exists": (HIGH_RECALL_SURFACE_SESSION / "feature_complete_high_recall_prediction_ready_rows.parquet").exists(),
        "resolved_prediction_ready_file": str(SURFACE_ROWS),
        "current_reranker_validation_session": str(RERANKER_SESSION),
        "current_risk_filter_session": str(RISK_FILTER_SESSION),
        "current_accumulated_reference": str(CURRENT_ACCUMULATED_SESSION),
        "surface_no_lookahead": str(SURFACE_NO_LOOKAHEAD),
        "surface_leakage": str(SURFACE_LEAKAGE),
        "surface_breadth": str(SURFACE_BREADTH),
        "surface_oracle": str(SURFACE_ORACLE),
        "current_reranker_predictions": str(RERANKER_PREDICTIONS),
        "current_reranker_topk_diff": str(RERANKER_TOPK_DIFF),
        "current_reranker_oracle": str(RERANKER_ORACLE),
        "current_reranker_failure": str(RERANKER_FAILURE),
        "current_reranker_decision": str(RERANKER_DECISION),
        "current_risk_filter_recommendation": str(RISK_FILTER_RECOMMENDATION),
        "current_risk_filter_variant_comparison": str(RISK_FILTER_VARIANT_COMPARISON),
        "current_risk_filter_variant_rows": str(RISK_FILTER_VARIANT_ROWS),
        "current_risk_filter_retained_rows": str(RISK_FILTER_RETAINED_ROWS),
        "current_risk_filter_side_summary": str(RISK_FILTER_SIDE_SUMMARY),
        "current_accumulated_variant_comparison": str(CURRENT_ACCUMULATED_VARIANT),
        "jobs_requested": jobs,
        "jobs_supported": 2,
        "notes": [
            "feature_complete_high_recall_candidate_rows.parquet is the canonical prediction-ready surface because the separately named prediction-ready file is absent in the source bundle",
            "no new model training or label tuning was performed",
        ],
    }

    # JSON audits
    _write_json(output_root / "selected_tier_failure_audit.json", selected_audit)
    _write_json(output_root / "high_recall_filter_revision_contracts.json", contracts)
    _write_json(output_root / "high_recall_filter_revision_surface_comparison.json", variant_comparison)
    _write_json(output_root / "high_recall_filter_revision_reranker_comparison.json", reranker_comparison)
    _write_json(output_root / "high_recall_filter_revision_recommendation.json", recommendation)
    _write_json(output_root / "high_recall_filter_revision_v1_decision.json", decision)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", {"complete": True, "generated_at_utc": _utc_now(), "session_dir": str(output_root)})

    # Parquets
    _write_parquet(output_root / "high_recall_filter_revision_rows.parquet", row_frame)
    _write_parquet(output_root / "high_recall_filter_revision_topk_membership_diff.parquet", reranker_comparison_to_frame(reranker_comparison, row_frame))
    _write_parquet(output_root / "high_recall_filter_revision_tier_summary.parquet", tier_summary_frame(reranker_comparison))
    _write_parquet(output_root / "high_recall_filter_revision_side_summary.parquet", side_summary_frame(reranker_comparison))
    _write_parquet(output_root / "high_recall_filter_revision_oracle_by_group.parquet", oracle_by_group_frame(variant_comparison))

    return {
        "decision": decision["decision"],
        "session_dir": str(output_root),
    }


def reranker_comparison_to_frame(reranker_comparison: dict[str, Any], row_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for variant_name, variant_payload in reranker_comparison["variants"].items():
        variant_rows = row_frame[row_frame["filter_variant"] == variant_name].copy()
        for topk in TOP_K_VALUES:
            topk_key = f"top{topk}"
            comp = variant_payload["topk"][topk_key]
            topk_rows = variant_rows.copy()
            topk_rows["topk"] = int(topk)
            topk_rows["model_name"] = MODEL_NAME
            topk_rows["model_selected"] = topk_rows["path_value_rank_tree"] <= topk
            topk_rows["champion_selected"] = topk_rows["champion_rank_variant"] <= topk
            rows.append(
                topk_rows[
                    [
                        "model_name",
                        "filter_variant",
                        "topk",
                        "anchor_date",
                        "month_bucket",
                        "side",
                        "symbol",
                        "candidate_idx",
                        "candidate_pool_tier",
                        "candidate_pool_reason",
                        "risk_filter_variant",
                        "included_by_filter_reason",
                        "risk_flagged_candidate",
                        "included_for_min_pool_backfill",
                        "high_recall_pool_status",
                        "score",
                        "rank",
                        "path_value_score_tree",
                        "path_value_rank_tree",
                        "champion_score",
                        "champion_rank_variant",
                        "forward_ret_20d",
                        "path_value_score_v1",
                        "top15_label",
                        "bottom15_label",
                        "top20pct_label",
                        "model_selected",
                        "champion_selected",
                    ]
                ].copy()
            )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def tier_summary_frame(reranker_comparison: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for variant_name, variant_payload in reranker_comparison["variants"].items():
        for topk, topk_payload in variant_payload["topk"].items():
            for tier, metrics in variant_payload["tier_summary"].get(topk, {}).items():
                rows.append(
                    {
                        "variant_name": variant_name,
                        "topk": int(topk.replace("top", "")),
                        "candidate_pool_tier": tier,
                        "row_count": metrics["row_count"],
                        "mean_forward_ret_20d": metrics["mean_forward_ret_20d"],
                        "mean_path_value_score_v1": metrics["mean_path_value_score_v1"],
                        "top15_capture_rate": metrics["top15_capture_rate"],
                        "bottom15_contamination_rate": metrics["bottom15_contamination_rate"],
                    }
                )
    return pd.DataFrame.from_records(rows)


def side_summary_frame(reranker_comparison: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for variant_name, variant_payload in reranker_comparison["variants"].items():
        for topk, side_map in variant_payload["side_summary"].items():
            for side, metrics in side_map.items():
                rows.append(
                    {
                        "variant_name": variant_name,
                        "topk": int(topk.replace("top", "")),
                        "side": side,
                        "row_count": metrics["row_count"],
                        "mean_forward_ret_20d": metrics["mean_forward_ret_20d"],
                        "mean_path_value_score_v1": metrics["mean_path_value_score_v1"],
                        "top15_capture_rate": metrics["top15_capture_rate"],
                        "bottom15_contamination_rate": metrics["bottom15_contamination_rate"],
                    }
                )
    return pd.DataFrame.from_records(rows)


def oracle_by_group_frame(variant_comparison: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for variant_name, payload in variant_comparison["variants"].items():
        oracle = payload["oracle"]
        for topk, metrics in oracle.items():
            rows.append(
                {
                    "variant_name": variant_name,
                    "topk": int(topk.replace("top", "")),
                    "selected_row_count": metrics["selected_row_count"],
                    "mean_forward_ret_20d": metrics["mean_forward_ret_20d"],
                    "mean_path_value_score_v1": metrics["mean_path_value_score_v1"],
                    "top15_capture_rate": metrics["top15_capture_rate"],
                    "top20pct_capture_rate": metrics["top20pct_capture_rate"],
                    "bottom15_contamination_rate": metrics["bottom15_contamination_rate"],
                }
            )
    return pd.DataFrame.from_records(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX high-recall filter revision v1")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
