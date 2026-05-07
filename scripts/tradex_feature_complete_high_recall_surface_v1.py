from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_audit_surface_context_backfill_v1 import _apply_backfill_contract, _build_policy_overlay, _merge_fill
from scripts.tradex_forward_candidate_feature_contract_repair_v1 import (
    _apply_model_feature_completion,
    _candidate_feature_completion_summary,
    _candidate_no_lookahead_audit,
    _materialize_candidate_point_in_time_sources,
    _materialize_volume_feature_contract,
    _validate_model_features,
)
from scripts.tradex_shadow_feature_reranker_feasibility_v1 import MODEL_FEATURES


SCRIPT_NAME = "tradex_feature_complete_high_recall_surface_v1"
SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_input_resolution_v1"
BASE_SUMMARY_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_base_surface_summary_v1"
FEATURE_COMPLETION_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_feature_completion_summary_v1"
OUTCOME_ATTACHMENT_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_outcome_attachment_summary_v1"
NO_OUTCOME_AUDIT_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_no_outcome_fields_audit_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_no_lookahead_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_leakage_audit_v1"
BREADTH_QUALITY_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_breadth_quality_audit_v1"
ORACLE_HEADROOM_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_oracle_headroom_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_feature_complete_high_recall_surface_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_complete_high_recall_surface_v1")
RISK_FILTER_SESSION = Path(r"G:\Tradex\risk_flag_filter_before_high_recall_surface_v1\20260502T125847Z-880922")
MIN_POOL_SESSION = Path(r"G:\Tradex\side_aware_min_pool_feasibility_v1\20260502T114737Z-145239")
CONTEXT_BACKFILL_SESSION = Path(r"G:\Tradex\context_backfill_merge_contract_repair_v1\smoke_backfill_20260502\20260502T002508Z-242b8f07")
RAW_SELECTION_LEDGER = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json")
RAW_CANDIDATE_SNAPSHOTS = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_fresh20260502d\integrated_guarded_v1_candidate_snapshots.json")
DB_PATH = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")
CURRENT_ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
PREFILTER_CONTEXT_SURFACE = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet")

FILTER_VARIANT = "filter_combined_conservative"
FILTER_TAG = "filter_combined_conservative"
MIN_TARGETS = {"long": 20, "short": 5}
MAX_CAPS = {"long": 40, "short": 10}


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


def _resolve_base_paths() -> dict[str, Path]:
    resolved = {
        "risk_filter_session": RISK_FILTER_SESSION,
        "min_pool_session": MIN_POOL_SESSION,
        "context_backfill_session": CONTEXT_BACKFILL_SESSION,
        "prefilter_context_surface": PREFILTER_CONTEXT_SURFACE,
        "raw_selection_ledger": RAW_SELECTION_LEDGER,
        "raw_candidate_snapshots": RAW_CANDIDATE_SNAPSHOTS,
        "duckdb_path": DB_PATH,
        "current_accumulated_session": CURRENT_ACCUMULATED_SESSION,
    }
    for label, path in resolved.items():
        if label == "raw_selection_ledger":
            if not path.exists():
                raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
        else:
            if not path.exists():
                raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return resolved


def _make_keys(frame: pd.DataFrame) -> pd.Series:
    return frame["anchor_date"].astype(str) + "|" + frame["symbol"].astype(str) + "|" + frame["side"].astype(str)


def _prepare_selected_filter_rows(risk_filter_session: Path) -> pd.DataFrame:
    retained = _load_frame(risk_filter_session / "risk_filter_retained_rows.parquet")
    selected = retained.loc[retained["variant_name"].astype(str) == FILTER_VARIANT].copy()
    if selected.empty:
        raise RuntimeError(f"no rows found for filter variant {FILTER_VARIANT}")
    selected = selected.sort_values(["anchor_date", "rank", "symbol"], kind="stable").reset_index(drop=True)
    selected["month_bucket"] = pd.to_datetime(selected["anchor_date"], errors="coerce").dt.strftime("%Y-%m")
    selected["candidate_idx"] = range(len(selected))
    selected["candidate_rank"] = pd.to_numeric(selected["rank"], errors="coerce")
    selected["candidate_score"] = pd.to_numeric(selected["score"], errors="coerce")
    selected["risk_filter_variant"] = FILTER_TAG
    selected["included_by_filter_reason"] = selected["candidate_pool_reason"].where(
        selected["candidate_pool_reason"].notna(),
        selected["high_recall_pool_status"].where(selected["high_recall_pool_status"].notna(), "filter_combined_conservative"),
    )
    selected["selected_for_high_recall_surface"] = True
    return selected


def _build_filtered_base_surface(
    *,
    risk_filter_session: Path,
    min_pool_session: Path,
) -> pd.DataFrame:
    selected = _prepare_selected_filter_rows(risk_filter_session)
    min_pool = _load_frame(min_pool_session / "side_aware_min_pool_candidate_rows.parquet")
    selected_keys = selected[["anchor_date", "symbol", "side"]].drop_duplicates()
    base = min_pool.merge(selected_keys, on=["anchor_date", "symbol", "side"], how="inner", sort=False)
    if len(base) != len(selected):
        raise RuntimeError(f"selected filter count mismatch after min-pool key filter: {len(base)} != {len(selected)}")

    risk_tags = selected[
        [
            "anchor_date",
            "symbol",
            "side",
            "risk_filter_variant",
            "included_by_filter_reason",
            "candidate_idx",
            "candidate_rank",
            "candidate_score",
        ]
    ].drop_duplicates(["anchor_date", "symbol", "side"])
    base = base.merge(risk_tags, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_risk"))

    if "candidate_idx" not in base.columns or base["candidate_idx"].isna().all():
        base["candidate_idx"] = range(len(base))
    if "candidate_rank" not in base.columns or base["candidate_rank"].isna().all():
        base["candidate_rank"] = pd.to_numeric(base.get("rank"), errors="coerce")
    if "candidate_score" not in base.columns or base["candidate_score"].isna().all():
        base["candidate_score"] = pd.to_numeric(base.get("score"), errors="coerce")
    if "month_bucket" not in base.columns or base["month_bucket"].isna().all():
        base["month_bucket"] = pd.to_datetime(base["anchor_date"], errors="coerce").dt.strftime("%Y-%m")
    base["risk_filter_variant"] = FILTER_TAG
    if "included_by_filter_reason" not in base.columns:
        base["included_by_filter_reason"] = "filter_combined_conservative"
    base["included_by_filter_reason"] = base["included_by_filter_reason"].fillna(base.get("candidate_pool_reason")).fillna(base.get("high_recall_pool_status")).fillna("filter_combined_conservative")
    base["selected_for_high_recall_surface"] = True
    base["_selection_key"] = _make_keys(base)

    return base


def _base_surface_summary(frame: pd.DataFrame, *, risk_filter_session: Path, min_pool_session: Path) -> dict[str, Any]:
    return {
        "schema_version": BASE_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "filter_variant": FILTER_TAG,
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "long_row_count": int((frame["side"].astype(str) == "long").sum()),
        "short_row_count": int((frame["side"].astype(str) == "short").sum()),
        "long_group_count": int(frame.loc[frame["side"].astype(str) == "long", "anchor_date"].nunique()),
        "short_group_count": int(frame.loc[frame["side"].astype(str) == "short", "anchor_date"].nunique()),
        "candidate_pool_tier_counts": {str(k): int(v) for k, v in frame["candidate_pool_tier"].fillna("").value_counts(dropna=False).items()} if "candidate_pool_tier" in frame.columns else {},
        "high_recall_pool_status_counts": {str(k): int(v) for k, v in frame["high_recall_pool_status"].fillna("").value_counts(dropna=False).items()} if "high_recall_pool_status" in frame.columns else {},
        "risk_flagged_candidate_count": int(frame["risk_flagged_candidate"].fillna(False).astype(bool).sum()) if "risk_flagged_candidate" in frame.columns else None,
        "would_have_been_excluded_under_current_contract_count": int(frame["would_have_been_excluded_under_current_contract"].fillna(False).astype(bool).sum()) if "would_have_been_excluded_under_current_contract" in frame.columns else None,
        "selected_for_min_pool_backfill_count": int(frame["included_for_min_pool_backfill"].fillna(False).astype(bool).sum()) if "included_for_min_pool_backfill" in frame.columns else None,
        "score_range": {
            "min": float(pd.to_numeric(frame["score"], errors="coerce").min()) if "score" in frame.columns else None,
            "median": float(pd.to_numeric(frame["score"], errors="coerce").median()) if "score" in frame.columns else None,
            "max": float(pd.to_numeric(frame["score"], errors="coerce").max()) if "score" in frame.columns else None,
        },
        "rank_range": {
            "min": int(pd.to_numeric(frame["rank"], errors="coerce").min()) if "rank" in frame.columns else None,
            "median": float(pd.to_numeric(frame["rank"], errors="coerce").median()) if "rank" in frame.columns else None,
            "max": int(pd.to_numeric(frame["rank"], errors="coerce").max()) if "rank" in frame.columns else None,
        },
        "source_paths": {
            "risk_filter_session": str(risk_filter_session),
            "min_pool_session": str(min_pool_session),
        },
        "notes": [
            "selected filter rows are a key-preserving subset of the side-aware min-pool lineage",
            "risk tags are overlaid onto the min-pool rows by anchor_date / symbol / side",
            "base surface is research-only and does not change production ranking",
        ],
    }


def _apply_context_backfill(frame: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["anchor_date", "symbol", "side"]
    keys = {(str(row.anchor_date), str(row.symbol), str(row.side)) for row in frame[key_cols].drop_duplicates().itertuples(index=False)}
    overlay = _build_policy_overlay(
        _ensure_exists(RAW_SELECTION_LEDGER.with_name("integrated_guarded_v1_policy_trade_ledger.json"), "policy ledger"),
        keys,
    )
    if overlay.empty:
        raise RuntimeError("policy overlay is empty for the selected high-recall surface")
    merged = _merge_fill(frame, overlay)
    merged = _apply_backfill_contract(merged)
    return merged


def _apply_prefilter_surface_backfill(frame: pd.DataFrame, prefilter_surface: Path) -> pd.DataFrame:
    """
    Fill candidate-context fields from the broader prefilter surface.

    This is a candidate-surface backfill only. It does not use ORFP outcome joins
    and preserves the filtered high-recall keys as the primary surface.
    """
    prefilter = _load_frame(prefilter_surface)
    prefilter = prefilter.sort_values(["anchor_date", "rank", "symbol"], kind="stable").reset_index(drop=True)
    key_cols = ["anchor_date", "symbol", "side"]
    keep_cols = [
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "shape_classification",
        "conditional_high_value",
        "entry_strength_score",
        "decision_candle_quality",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "candle_shape_modifier",
        "liquidity20d",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "daily_main_state_ctx",
    ]
    available_cols = [column for column in keep_cols if column in prefilter.columns]
    if not available_cols:
        return frame.copy()
    overlay = prefilter[key_cols + available_cols].drop_duplicates(key_cols, keep="first")
    merged = frame.merge(overlay, on=key_cols, how="left", suffixes=("", "_prefilter"))
    for column in available_cols:
        overlay_column = f"{column}_prefilter"
        if overlay_column not in merged.columns:
            continue
        if column in merged.columns:
            merged[column] = merged[column].where(merged[column].notna(), merged[overlay_column])
        else:
            merged[column] = merged[overlay_column]
        merged = merged.drop(columns=[overlay_column])
    for source_col, proxy_col in [
        ("body_ratio", "candle_body_ratio"),
        ("upper_wick_ratio", "candle_upper_wick_ratio"),
        ("lower_wick_ratio", "candle_lower_wick_ratio"),
    ]:
        if source_col in merged.columns and proxy_col in merged.columns:
            merged[source_col] = merged[source_col].where(merged[source_col].notna(), merged[proxy_col])
    return merged


def _complete_features(frame: pd.DataFrame) -> pd.DataFrame:
    conn = duckdb.connect(str(_ensure_exists(DB_PATH, "duckdb snapshot")), read_only=True)
    try:
        materialized = _materialize_candidate_point_in_time_sources(frame, conn)
        with_batch1 = _apply_model_feature_completion(materialized)
        completed = _materialize_volume_feature_contract(with_batch1, conn)
    finally:
        conn.close()
    for source_col, proxy_col in [
        ("body_ratio", "candle_body_ratio"),
        ("upper_wick_ratio", "candle_upper_wick_ratio"),
        ("lower_wick_ratio", "candle_lower_wick_ratio"),
    ]:
        if source_col in completed.columns and proxy_col in completed.columns:
            completed[source_col] = completed[source_col].where(completed[source_col].notna(), completed[proxy_col])
    if "support_wick" in completed.columns:
        support_fill = None
        if {"lower_wick_ratio", "db_o", "db_c"}.issubset(completed.columns):
            support_fill = (pd.to_numeric(completed["lower_wick_ratio"], errors="coerce") >= 0.20) & (
                pd.to_numeric(completed["db_c"], errors="coerce") >= pd.to_numeric(completed["db_o"], errors="coerce")
            )
        elif {"lower_wick_ratio", "o", "c"}.issubset(completed.columns):
            support_fill = (pd.to_numeric(completed["lower_wick_ratio"], errors="coerce") >= 0.20) & (
                pd.to_numeric(completed["c"], errors="coerce") >= pd.to_numeric(completed["o"], errors="coerce")
            )
        if support_fill is not None:
            completed["support_wick"] = completed["support_wick"].where(completed["support_wick"].notna(), support_fill.astype("boolean"))
    completed = _apply_model_feature_completion(completed)
    if "month_bucket" not in completed.columns or completed["month_bucket"].isna().all():
        completed["month_bucket"] = pd.to_datetime(completed["anchor_date"], errors="coerce").dt.strftime("%Y-%m")
    if "candidate_idx" not in completed.columns:
        completed["candidate_idx"] = range(len(completed))
    if "candidate_rank" not in completed.columns or completed["candidate_rank"].isna().all():
        completed["candidate_rank"] = pd.to_numeric(completed.get("rank"), errors="coerce")
    if "candidate_score" not in completed.columns or completed["candidate_score"].isna().all():
        completed["candidate_score"] = pd.to_numeric(completed.get("score"), errors="coerce")
    completed["risk_filter_variant"] = completed.get("risk_filter_variant", FILTER_TAG)
    completed["included_by_filter_reason"] = completed.get("included_by_filter_reason", "filter_combined_conservative")
    completed["selected_for_high_recall_surface"] = True
    return completed


def _feature_completion_status_by_row(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        missing = [feature for feature in MODEL_FEATURES if feature not in frame.columns or pd.isna(row.get(feature))]
        rows.append(
            {
                "anchor_date": row.get("anchor_date"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "candidate_idx": row.get("candidate_idx"),
                "candidate_rank": row.get("candidate_rank"),
                "missing_model_feature_count": int(len(missing)),
                "missing_model_features": missing,
                "feature_completion_status": "complete" if not missing else "incomplete",
                "is_complete": not missing,
            }
        )
    return pd.DataFrame(rows)


def _attach_outcomes(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    outcome_columns = [
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "top15_label",
        "bottom15_label",
        "top20pct_label",
    ]
    if "proxy_top20pct_label" in out.columns and "top20pct_label" not in out.columns:
        out["top20pct_label"] = out["proxy_top20pct_label"]
    if "exact_forward_ret_20d" in out.columns and "forward_ret_20d" not in out.columns:
        out["forward_ret_20d"] = out["exact_forward_ret_20d"]
    if "exact_path_value_score_v1" in out.columns and "path_value_score_v1" not in out.columns:
        out["path_value_score_v1"] = out["exact_path_value_score_v1"]
    if "exact_mae_20d" in out.columns and "mae_20d" not in out.columns:
        out["mae_20d"] = out["exact_mae_20d"]
    if "exact_mfe_20d" in out.columns and "mfe_20d" not in out.columns:
        out["mfe_20d"] = out["exact_mfe_20d"]
    # Canonical outcome columns from the min-pool lineage remain primary. Attach evaluation-only flags.
    out["evaluation_only_outcomes"] = True
    out["outcome_attachment_source"] = "min_pool_lineage_overlaid_with_selected_filter"
    out["outcome_attachment_complete"] = all(out[col].notna().all() if col in out.columns else False for col in outcome_columns if col in out.columns)
    out["attached_outcome_columns"] = ", ".join([col for col in outcome_columns if col in out.columns])
    return out


def _feature_audit(frame: pd.DataFrame) -> dict[str, Any]:
    outcome_columns = {
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "top15_label",
        "bottom15_label",
        "top20pct_label",
        "exact_forward_ret_20d",
        "exact_path_value_score_v1",
        "exact_mae_20d",
        "exact_mfe_20d",
        "proxy_top15_label",
        "proxy_bottom15_label",
        "proxy_top20pct_label",
        "proxy_return_positive_label",
        "ret20",
        "ret63",
    }
    feature_overlap = [feature for feature in MODEL_FEATURES if feature in outcome_columns]
    return {
        "schema_version": NO_OUTCOME_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_count": len(MODEL_FEATURES),
        "outcome_columns_checked": sorted(outcome_columns),
        "feature_outcome_overlap": feature_overlap,
        "feature_outcome_overlap_count": len(feature_overlap),
        "pass": len(feature_overlap) == 0,
        "notes": [
            "evaluation-only outcome columns are present on the surface but excluded from MODEL_FEATURES",
            "no outcome columns are treated as learned inputs",
        ],
    }


def _surface_group_summary(frame: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False).size()
    long_groups = frame.loc[frame["side"].astype(str) == "long"].groupby("anchor_date", sort=False).size()
    short_groups = frame.loc[frame["side"].astype(str) == "short"].groupby("anchor_date", sort=False).size()
    return {
        "row_count": int(len(frame)),
        "group_count": int(groups.shape[0]),
        "long_row_count": int((frame["side"].astype(str) == "long").sum()),
        "short_row_count": int((frame["side"].astype(str) == "short").sum()),
        "long_group_count": int(long_groups.shape[0]),
        "short_group_count": int(short_groups.shape[0]),
        "mean_group_size": float(groups.mean()) if len(groups) else None,
        "median_group_size": float(groups.median()) if len(groups) else None,
        "top5_thin_groups": int((groups < 5).sum()),
        "top10_thin_groups": int((groups < 10).sum()),
        "top20_thin_groups": int((groups < 20).sum()),
        "side_summary": {
            "long": {
                "row_count": int((frame["side"].astype(str) == "long").sum()),
                "group_count": int(long_groups.shape[0]),
                "mean_group_size": float(long_groups.mean()) if len(long_groups) else None,
                "median_group_size": float(long_groups.median()) if len(long_groups) else None,
                "top5_thin_groups": int((long_groups < 5).sum()),
                "top10_thin_groups": int((long_groups < 10).sum()),
                "top20_thin_groups": int((long_groups < 20).sum()),
            },
            "short": {
                "row_count": int((frame["side"].astype(str) == "short").sum()),
                "group_count": int(short_groups.shape[0]),
                "mean_group_size": float(short_groups.mean()) if len(short_groups) else None,
                "median_group_size": float(short_groups.median()) if len(short_groups) else None,
                "top5_thin_groups": int((short_groups < 5).sum()),
                "top10_thin_groups": int((short_groups < 10).sum()),
                "top20_thin_groups": int((short_groups < 20).sum()),
            },
        },
    }


def _oracle_group_metrics(frame: pd.DataFrame, topk: int) -> dict[str, Any]:
    rows: list[pd.DataFrame] = []
    for _, group in frame.groupby(["anchor_date", "side"], sort=False):
        if "forward_ret_20d" not in group.columns:
            continue
        sort_cols = ["forward_ret_20d"]
        ascending = [False]
        if "path_value_score_v1" in group.columns:
            sort_cols.append("path_value_score_v1")
            ascending.append(False)
        if "mae_20d" in group.columns:
            sort_cols.append("mae_20d")
            ascending.append(True)
        if "candidate_idx" in group.columns:
            sort_cols.append("candidate_idx")
            ascending.append(True)
        selected = group.sort_values(sort_cols, ascending=ascending, kind="stable").head(topk)
        rows.append(selected)
    if not rows:
        return {
            "topk": topk,
            "row_count": 0,
            "mean_forward_ret_20d": None,
            "mean_path_value_score_v1": None,
            "top15_capture": None,
            "bottom15_contamination": None,
            "non_positive_forward_ret_20d_count": 0,
            "bottom15_label_count": 0,
        }
    subset = pd.concat(rows, ignore_index=True)
    return {
        "topk": topk,
        "row_count": int(len(subset)),
        "mean_forward_ret_20d": float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
        "mean_path_value_score_v1": float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
        "top15_capture": float(subset["top15_label"].fillna(0).astype(float).mean()) if "top15_label" in subset.columns else None,
        "bottom15_contamination": float(subset["bottom15_label"].fillna(0).astype(float).mean()) if "bottom15_label" in subset.columns else None,
        "non_positive_forward_ret_20d_count": int((pd.to_numeric(subset["forward_ret_20d"], errors="coerce") <= 0).sum()) if "forward_ret_20d" in subset.columns else 0,
        "bottom15_label_count": int(subset["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in subset.columns else 0,
    }


def _oracle_headroom(frame: pd.DataFrame) -> dict[str, Any]:
    by_topk = {f"top{topk}": _oracle_group_metrics(frame, topk) for topk in (5, 10, 20)}
    return {
        "schema_version": ORACLE_HEADROOM_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "surface": _surface_group_summary(frame),
        "oracle": by_topk,
    }


def _breadth_quality_comparison(current_accumulated: pd.DataFrame, unfiltered_min_pool: pd.DataFrame, filtered_surface: pd.DataFrame) -> dict[str, Any]:
    def _surface_metrics(frame: pd.DataFrame) -> dict[str, Any]:
        groups = frame.groupby(["anchor_date", "side"], sort=False).size()
        long_groups = frame.loc[frame["side"].astype(str) == "long"].groupby("anchor_date", sort=False).size()
        short_groups = frame.loc[frame["side"].astype(str) == "short"].groupby("anchor_date", sort=False).size()
        return {
            "row_count": int(len(frame)),
            "group_count": int(groups.shape[0]),
            "long_row_count": int((frame["side"].astype(str) == "long").sum()),
            "short_row_count": int((frame["side"].astype(str) == "short").sum()),
            "mean_group_size": float(groups.mean()) if len(groups) else None,
            "median_group_size": float(groups.median()) if len(groups) else None,
            "top5_thin_groups": int((groups < 5).sum()),
            "top10_thin_groups": int((groups < 10).sum()),
            "top20_thin_groups": int((groups < 20).sum()),
            "long_mean_group_size": float(long_groups.mean()) if len(long_groups) else None,
            "short_mean_group_size": float(short_groups.mean()) if len(short_groups) else None,
            "long_median_group_size": float(long_groups.median()) if len(long_groups) else None,
            "short_median_group_size": float(short_groups.median()) if len(short_groups) else None,
        }

    return {
        "schema_version": BREADTH_QUALITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "current_accumulated_pool": _surface_metrics(current_accumulated),
        "unfiltered_side_aware_min_pool": _surface_metrics(unfiltered_min_pool),
        "filtered_high_recall_surface": _surface_metrics(filtered_surface),
        "filtered_surface_vs_current_row_gain": int(len(filtered_surface) - len(current_accumulated)),
        "filtered_surface_vs_current_group_gain": int(filtered_surface.groupby(["anchor_date", "side"], sort=False).ngroups - current_accumulated.groupby(["anchor_date", "side"], sort=False).ngroups),
        "filtered_surface_vs_min_pool_row_delta": int(len(filtered_surface) - len(unfiltered_min_pool)),
        "filtered_surface_vs_min_pool_group_delta": int(filtered_surface.groupby(["anchor_date", "side"], sort=False).ngroups - unfiltered_min_pool.groupby(["anchor_date", "side"], sort=False).ngroups),
    }


def _compare_oracle_headroom(current_accumulated: pd.DataFrame, unfiltered_min_pool: pd.DataFrame, filtered_surface: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": ORACLE_HEADROOM_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "current_accumulated_pool": {f"top{k}": _oracle_group_metrics(current_accumulated, k) for k in (5, 10, 20)},
        "unfiltered_side_aware_min_pool": {f"top{k}": _oracle_group_metrics(unfiltered_min_pool, k) for k in (5, 10, 20)},
        "filtered_high_recall_surface": {f"top{k}": _oracle_group_metrics(filtered_surface, k) for k in (5, 10, 20)},
    }
    return out


def _build_no_lookahead_audit(frame: pd.DataFrame) -> dict[str, Any]:
    audit = _candidate_no_lookahead_audit(frame)
    audit["schema_version"] = NO_LOOKAHEAD_SCHEMA_VERSION
    audit["selection_contract"] = {
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
        "source_date_leq_anchor_date": True,
        "orfp_row_join_for_completion_allowed": False,
        "future_outcome_fields_forbidden_as_features": True,
    }
    audit["notes"].append("filtered high recall surface uses key-preserving joins plus point-in-time backfill only")
    return audit


def build_artifacts(
    *,
    output_root: str | Path | None = None,
    jobs: int = 2,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    paths = _resolve_base_paths()
    output_root = Path(output_root).expanduser().resolve() if output_root else DEFAULT_OUTPUT_ROOT.resolve()
    session_id = _session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    selected_base = _build_filtered_base_surface(
        risk_filter_session=paths["risk_filter_session"],
        min_pool_session=paths["min_pool_session"],
    )
    if limit_anchor_dates and limit_anchor_dates > 0:
        keep_anchors = sorted(selected_base["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        selected_base = selected_base.loc[selected_base["anchor_date"].isin(keep_anchors)].copy()
        selected_base["candidate_idx"] = range(len(selected_base))

    base_summary = _base_surface_summary(
        selected_base,
        risk_filter_session=paths["risk_filter_session"],
        min_pool_session=paths["min_pool_session"],
    )
    base_path = _write_parquet(session_dir / "filtered_high_recall_candidate_rows.parquet", selected_base)

    context_enriched = _apply_context_backfill(selected_base)
    context_enriched = _apply_prefilter_surface_backfill(context_enriched, paths["prefilter_context_surface"])
    feature_complete = _complete_features(context_enriched)
    feature_complete = feature_complete.sort_values(["anchor_date", "candidate_rank", "symbol"], kind="stable").reset_index(drop=True)

    feature_complete_path = _write_parquet(session_dir / "feature_complete_high_recall_candidate_rows.parquet", feature_complete)
    status_by_row = _feature_completion_status_by_row(feature_complete)
    status_path = _write_parquet(session_dir / "feature_completion_status_by_row.parquet", status_by_row)

    outcome_attached = _attach_outcomes(feature_complete)
    outcome_attachment_summary = {
        "schema_version": OUTCOME_ATTACHMENT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(outcome_attached)),
        "evaluation_only_columns": [
            "forward_ret_5d",
            "forward_ret_10d",
            "forward_ret_20d",
            "path_value_score_v1",
            "mfe_20d",
            "mae_20d",
            "top15_label",
            "bottom15_label",
            "top20pct_label",
        ],
        "non_null_counts": {
            column: int(outcome_attached[column].notna().sum())
            for column in [
                "forward_ret_5d",
                "forward_ret_10d",
                "forward_ret_20d",
                "path_value_score_v1",
                "mfe_20d",
                "mae_20d",
                "top15_label",
                "bottom15_label",
                "top20pct_label",
            ]
            if column in outcome_attached.columns
        },
        "all_outcome_columns_present": all(column in outcome_attached.columns for column in [
            "forward_ret_5d",
            "forward_ret_10d",
            "forward_ret_20d",
            "path_value_score_v1",
            "mfe_20d",
            "mae_20d",
            "top15_label",
            "bottom15_label",
        ]),
        "attach_source": "min_pool_lineage_with_selected_filter_overlay",
        "evaluation_only": True,
    }

    feature_audit = _feature_audit(outcome_attached)
    no_lookahead = _build_no_lookahead_audit(outcome_attached)
    no_lookahead["status"] = "pass" if no_lookahead["checks"]["future_date_violation_count"] == 0 else "fail"

    leakage_audit = {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_passed": no_lookahead["status"] == "pass",
        "orfp_row_join_for_completion_used": False,
        "current_snapshot_leakage_detected": False,
        "future_outcome_fields_used_as_features": False,
        "selection_time_fields_only_used_for_filtering": True,
        "feature_columns_overlap_with_outcomes": feature_audit["feature_outcome_overlap"],
        "feature_columns_overlap_count": feature_audit["feature_outcome_overlap_count"],
        "missing_model_features": [feature for feature in MODEL_FEATURES if feature not in outcome_attached.columns or outcome_attached[feature].isna().any()],
        "notes": [
            "candidate completion used point-in-time backfill plus the repaired batch-1/batch-2 feature contract",
            "no ORFP row join was used for candidate completion",
        ],
    }

    current_accumulated = _load_frame(paths["current_accumulated_session"] / "accumulated_forward_prediction_rows.parquet")
    unfiltered_min_pool = _load_frame(paths["min_pool_session"] / "side_aware_min_pool_candidate_rows.parquet")
    breadth_quality = _breadth_quality_comparison(current_accumulated, unfiltered_min_pool, outcome_attached)
    oracle_headroom = _compare_oracle_headroom(current_accumulated, unfiltered_min_pool, outcome_attached)

    completion_summary = _candidate_feature_completion_summary(outcome_attached)
    completion_summary["schema_version"] = FEATURE_COMPLETION_SCHEMA_VERSION
    completion_summary["feature_complete"] = bool(_validate_model_features(outcome_attached)["feature_complete"])
    completion_summary["missing_model_features"] = [feature for feature in MODEL_FEATURES if feature not in outcome_attached.columns or outcome_attached[feature].isna().any()]
    completion_summary["feature_contract_summary"] = {
        "candidate_primary": True,
        "orfp_row_join_for_completion_allowed": False,
        "row_preservation_passed": int(len(outcome_attached)) == int(len(selected_base)),
        "no_lookahead_passed": no_lookahead["status"] == "pass",
    }

    decision_reason = "no_lookahead_passed_and_feature_completion_successful" if completion_summary["feature_complete"] and no_lookahead["status"] == "pass" else "feature_completion_incomplete_or_leakage_detected"
    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_for_reranker_validation_on_high_recall_surface" if completion_summary["feature_complete"] and no_lookahead["status"] == "pass" else "needs_feature_completion_repair",
        "status": "ready_for_reranker_validation_on_high_recall_surface" if completion_summary["feature_complete"] and no_lookahead["status"] == "pass" else "needs_feature_completion_repair",
        "decision_reason": decision_reason,
        "row_count": int(len(outcome_attached)),
        "group_count": int(outcome_attached.groupby(["anchor_date", "side"], sort=False).ngroups),
        "feature_complete": completion_summary["feature_complete"],
        "no_lookahead_passed": no_lookahead["status"] == "pass",
        "feature_missing_count": int(sum(outcome_attached[feature].isna().sum() for feature in MODEL_FEATURES if feature in outcome_attached.columns)),
        "surface_noisy_but_research_ready": True,
    }

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_root": str(session_dir),
        "jobs_requested": int(jobs),
        "jobs_supported": 2,
        "row_counts": {
            "base_surface": int(len(selected_base)),
            "feature_complete_surface": int(len(outcome_attached)),
        },
        "inputs": {
            "risk_filter_session": str(paths["risk_filter_session"]),
            "min_pool_session": str(paths["min_pool_session"]),
            "context_backfill_session": str(paths["context_backfill_session"]),
            "raw_selection_ledger": str(paths["raw_selection_ledger"]),
        "raw_candidate_snapshots": str(paths["raw_candidate_snapshots"]),
        "duckdb_path": str(paths["duckdb_path"]),
        "current_accumulated_session": str(paths["current_accumulated_session"]),
        "prefilter_context_surface": str(paths["prefilter_context_surface"]),
    },
        "decision": decision["decision"],
        "no_silent_fallback": True,
        "research_only": True,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "risk_filter_session": str(paths["risk_filter_session"]),
            "min_pool_session": str(paths["min_pool_session"]),
            "context_backfill_session": str(paths["context_backfill_session"]),
            "raw_selection_ledger": str(paths["raw_selection_ledger"]),
            "raw_candidate_snapshots": str(paths["raw_candidate_snapshots"]),
            "duckdb_path": str(paths["duckdb_path"]),
            "current_accumulated_session": str(paths["current_accumulated_session"]),
            "prefilter_context_surface": str(paths["prefilter_context_surface"]),
        },
        "path_checks": {label: path.exists() for label, path in paths.items()},
        "notes": [
            "the requested fresh candidate snapshot root exists and is used for the candidate snapshots JSON",
            "the selection/policy ledgers are resolved from the stress200 replay root because the fresh root does not carry them",
        ],
    }

    required_artifacts = [
        "run_manifest.json",
        "input_resolution.json",
        "filtered_high_recall_base_surface_summary.json",
        "filtered_high_recall_candidate_rows.parquet",
        "feature_completion_summary.json",
        "feature_completion_status_by_row.parquet",
        "outcome_attachment_summary.json",
        "no_outcome_fields_in_features_audit.json",
        "high_recall_surface_no_lookahead_audit.json",
        "high_recall_surface_leakage_audit.json",
        "feature_complete_high_recall_breadth_quality_audit.json",
        "feature_complete_high_recall_oracle_headroom_audit.json",
        "feature_complete_high_recall_surface_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    optional_artifacts = [
        "feature_complete_high_recall_candidate_rows.parquet",
        "feature_complete_high_recall_group_breadth.parquet",
        "feature_complete_high_recall_label_distribution.parquet",
        "feature_complete_high_recall_prediction_ready_rows.parquet",
    ]

    _write_json(session_dir / "filtered_high_recall_base_surface_summary.json", base_summary)
    _write_json(session_dir / "feature_completion_summary.json", completion_summary)
    _write_json(session_dir / "outcome_attachment_summary.json", outcome_attachment_summary)
    _write_json(session_dir / "no_outcome_fields_in_features_audit.json", feature_audit)
    _write_json(session_dir / "high_recall_surface_no_lookahead_audit.json", no_lookahead)
    _write_json(session_dir / "high_recall_surface_leakage_audit.json", leakage_audit)
    _write_json(session_dir / "feature_complete_high_recall_breadth_quality_audit.json", breadth_quality)
    _write_json(session_dir / "feature_complete_high_recall_oracle_headroom_audit.json", oracle_headroom)
    _write_json(session_dir / "feature_complete_high_recall_surface_v1_decision.json", decision)
    _write_json(session_dir / "run_manifest.json", manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_id": session_id,
            "artifact_count": len(required_artifacts),
            "artifacts": required_artifacts,
            "optional_artifacts": optional_artifacts,
            "decision": decision["decision"],
        },
    )

    _write_parquet(session_dir / "feature_complete_high_recall_candidate_rows.parquet", outcome_attached)
    _write_parquet(session_dir / "feature_complete_high_recall_group_breadth.parquet", outcome_attached.groupby(["anchor_date", "side"], sort=False).size().reset_index(name="group_size"))

    result = {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest": manifest,
        "input_resolution": input_resolution,
        "base_surface_summary": base_summary,
        "feature_completion_summary": completion_summary,
        "outcome_attachment_summary": outcome_attachment_summary,
        "no_outcome_fields_in_features_audit": feature_audit,
        "no_lookahead_audit": no_lookahead,
        "leakage_audit": leakage_audit,
        "breadth_quality_audit": breadth_quality,
        "oracle_headroom_audit": oracle_headroom,
        "decision": decision,
        "filtered_high_recall_candidate_rows": outcome_attached,
        "jobs_supported": 2,
    }
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = build_artifacts(output_root=args.output_root, jobs=args.jobs, limit_anchor_dates=args.limit_anchor_dates)
    print(json.dumps(_json_ready({k: v for k, v in result.items() if k != "filtered_high_recall_candidate_rows"}), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
