from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
)
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
from scripts.tradex_score_component_overweight_decomposition_v1 import (  # noqa: E402
    BAD_PICK_ROOT_CAUSE,
)

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_AUDIT_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_BOUNDARY_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac")
DEFAULT_DECOMPOSITION_SESSION = Path(r"G:\Tradex\score_component_overweight_decomposition_v1\20260429T164904Z-a7e58c8a")
DEFAULT_FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\ma_context_shape_direct_adjustment_line\20260429T143302Z-8f34ef9d")
DEFAULT_CANDIDATE_SNAPSHOT_JSON = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_candidate_snapshots.json"
)
DEFAULT_POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\score_component_overweight_cap_or_confirmation_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_input_resolution_v1"
POLICY_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_policy_v1"
COMPARE_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_compare_v1"
MONTHLY_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_context_comparison_v1"
PRECISION_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_precision_recall_v1"
FALSE_POSITIVE_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_false_positive_cost_v1"
DECISION_SCHEMA_VERSION = "tradex_score_component_overweight_cap_or_confirmation_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
RISK_ROOT_CAUSE = "score_component_overweight"
CAP_EPSILON = 1e-9
CONFIRMATION_SHAPES = {"gap_down_bear", "upper_wick_then_bear", "bear_large", "bull_large"}


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


def _git_hash_or_unknown() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        value = result.stdout.strip()
        return value or "unknown"
    except Exception:
        return "unknown"


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for column in ("anchor_date", "trade_date", "month_bucket", "side", "symbol"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str)
    for column in ("candidate_idx", "candidate_rank", "champion_rank", "rank"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    numeric_columns = [
        "score",
        "candidate_score",
        "champion_score",
        "challenger_score",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    bool_columns = [
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "top15_label",
        "bottom15_label",
        "conditional_high_value",
        "shape_joined",
        "stable_high_value_family",
        "stable_bad_pick_family",
        "regime_dependent_family",
        "unstable_or_sparse_family",
        "neutral_family",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    ]
    for column in bool_columns:
        if column in frame.columns:
            frame[column] = frame[column].fillna(False).astype(bool)
    for column in ("monthly_context", "weekly_context", "family_classification", "shape_classification", "candle_shape_modifier", "dominant_regime_context"):
        if column in frame.columns:
            frame[column] = frame[column].fillna("unknown").astype(str)
    return frame


def _load_candidate_surface(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    required = {
        "anchor_date",
        "symbol",
        "side",
        "candidate_rank",
        "score",
        "rank",
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "monthly_context",
        "weekly_context",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "conditional_high_value",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"candidate surface missing required columns: {missing}")
    return _normalize_frame(frame)


def _load_audit_cases(path: Path) -> pd.DataFrame:
    frame = _normalize_frame(pd.read_parquet(path))
    required = {
        "anchor_date",
        "symbol",
        "side",
        "root_cause_code",
        "root_cause_confidence",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"audit cases missing required columns: {missing}")
    frame = _add_outcome_labels(frame)
    return frame


def _load_boundary_rows(path: Path) -> pd.DataFrame:
    return _normalize_frame(pd.read_parquet(path))


def _load_prefilter_policy(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    thresholds = payload.get("thresholds") or {}
    if "top15_score_threshold" not in thresholds or "bottom15_score_threshold" not in thresholds:
        raise RuntimeError("prefilter policy missing score thresholds")
    return payload


def _merge_annotations(frame: pd.DataFrame, audit_cases: pd.DataFrame, boundary_rows: pd.DataFrame) -> pd.DataFrame:
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
        "dominant_regime_context",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "daily_main_state_ctx",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
    ]
    audit_overlay = audit_cases[[col for col in audit_cols if col in audit_cases.columns]].copy()
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

    merged = _normalize_frame(merged)
    merged["audit_bad_pick"] = merged.get("is_bad_pick", pd.Series(False, index=merged.index)).fillna(False).astype(bool)
    merged["audit_good_pick"] = merged.get("is_good_pick", pd.Series(False, index=merged.index)).fillna(False).astype(bool)
    merged["audit_neutral_pick"] = merged.get("is_neutral_pick", pd.Series(False, index=merged.index)).fillna(False).astype(bool)
    merged["audit_root_cause_code"] = merged.get("root_cause_code", pd.Series("unknown", index=merged.index)).fillna("unknown").astype(str)
    merged["audit_root_cause_confidence"] = merged.get("root_cause_confidence", pd.Series("unknown", index=merged.index)).fillna("unknown").astype(str)
    merged["audit_is_top15_outcome"] = merged.get("top15_label", pd.Series(False, index=merged.index)).fillna(False).astype(bool)
    merged["audit_is_bottom15_outcome"] = merged.get("bottom15_label", pd.Series(False, index=merged.index)).fillna(False).astype(bool)
    merged["audit_is_materially_negative"] = pd.to_numeric(merged.get("forward_ret_20d"), errors="coerce").fillna(0.0) <= BAD_OUTCOME_THRESHOLD
    return merged


def _build_risk_and_confirmation(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    monthly_over = frame["monthly_context"].fillna("unknown").astype(str).str.contains("overextended", na=False)
    weekly_over = frame["weekly_context"].fillna("unknown").astype(str).str.contains("overextended", na=False)
    regime_dep = frame["family_classification"].fillna("unknown").astype(str).eq("regime_dependent_family")
    long_side = frame["side"].fillna("unknown").astype(str).eq("long")
    top10 = frame["champion_selected_top10"].fillna(False).astype(bool)
    frame["score_overweight_risk_slice"] = long_side & top10 & monthly_over & weekly_over & regime_dep
    shape_positive = frame["shape_classification"].fillna("shape_missing").astype(str).eq("shape_positive_modifier")
    vol_ratio = pd.to_numeric(frame.get("vol_ratio5_20"), errors="coerce")
    candle_body = pd.to_numeric(frame.get("candle_body_ratio"), errors="coerce")
    frame["score_overweight_confirmation_ok"] = (
        shape_positive
        & vol_ratio.ge(1.0)
        & candle_body.ge(0.5)
    )
    return frame


def _sort_columns(frame: pd.DataFrame, score_col: str) -> list[str]:
    columns = [score_col]
    for candidate in ("rank", "candidate_rank", "candidate_idx", "symbol"):
        if candidate in frame.columns and candidate not in columns:
            columns.append(candidate)
    return columns


def _rank_variant(frame: pd.DataFrame, *, score_col: str, prefix: str) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for (_, _), group in frame.groupby(["anchor_date", "side"], sort=False):
        group = group.copy()
        sort_columns = _sort_columns(group, score_col)
        ascending = [False] + [True] * (len(sort_columns) - 1)
        ranked = group.sort_values(by=sort_columns, ascending=ascending, kind="mergesort").copy()
        ranked[f"{prefix}_position"] = range(1, len(ranked) + 1)
        for top_k in TOP_K_VALUES:
            ranked[f"{prefix}_selected_top{top_k}"] = ranked[f"{prefix}_position"] <= top_k
        parts.append(ranked[["anchor_date", "symbol", "side", f"{prefix}_position", *[f"{prefix}_selected_top{k}" for k in TOP_K_VALUES]]])
    return pd.concat(parts, ignore_index=True)


def _attach_variant_rankings(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy().reset_index(drop=True)
    frame["candidate_idx"] = pd.to_numeric(frame["candidate_idx"], errors="coerce").fillna(pd.Series(range(len(frame)), index=frame.index)).astype(int)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")

    # Original score ranking, used as the fixed comparison baseline.
    original = _rank_variant(frame, score_col="score", prefix="original")
    frame = frame.merge(original, on=["anchor_date", "symbol", "side"], how="left")
    for top_k in TOP_K_VALUES:
        frame[f"original_selected_top{top_k}"] = frame[f"original_selected_top{top_k}"].fillna(False).astype(bool)
        if not frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool).equals(frame[f"original_selected_top{top_k}"]):
            raise RuntimeError(f"original recomputed top{top_k} selection does not match champion surface")

    frame["score_overweight_nonrisk_max_score"] = pd.NA
    for (_, _), group in frame.groupby(["anchor_date", "side"], sort=False):
        nonrisk = group.loc[~group["score_overweight_risk_slice"]]
        if nonrisk.empty:
            continue
        frame.loc[group.index, "score_overweight_nonrisk_max_score"] = pd.to_numeric(nonrisk["score"], errors="coerce").max()
    frame["score_overweight_nonrisk_max_score"] = pd.to_numeric(frame["score_overweight_nonrisk_max_score"], errors="coerce")
    frame["score_overweight_cap_reference_score"] = frame["score_overweight_nonrisk_max_score"] - CAP_EPSILON
    frame.loc[frame["score_overweight_nonrisk_max_score"].isna(), "score_overweight_cap_reference_score"] = frame["score"]
    frame["score_overweight_cap_applied"] = False
    frame["score_overweight_confirmation_cap_applied"] = False
    frame["score_overweight_effective_score_cap"] = frame["score"]
    frame["score_overweight_effective_score_confirmation"] = frame["score"]

    for (_, _), group in frame.groupby(["anchor_date", "side"], sort=False):
        nonrisk = group.loc[~group["score_overweight_risk_slice"]]
        if nonrisk.empty:
            continue
        cap_value = pd.to_numeric(nonrisk["score"], errors="coerce").max() - CAP_EPSILON
        cap_value = float(cap_value)
        risk_idx = group.index[group["score_overweight_risk_slice"]]
        if len(risk_idx):
            group_scores = pd.to_numeric(frame.loc[risk_idx, "score"], errors="coerce")
            cap_applied = group_scores > cap_value
            frame.loc[risk_idx, "score_overweight_cap_applied"] = cap_applied.values
            frame.loc[risk_idx, "score_overweight_effective_score_cap"] = group_scores.where(~cap_applied, cap_value)

            confirm_ok = frame.loc[risk_idx, "score_overweight_confirmation_ok"].fillna(False).astype(bool)
            confirm_applied = (~confirm_ok) & (group_scores > cap_value)
            frame.loc[risk_idx, "score_overweight_confirmation_cap_applied"] = confirm_applied.values
            frame.loc[risk_idx, "score_overweight_effective_score_confirmation"] = group_scores.where(~confirm_applied, cap_value)

    cap_ranked = _rank_variant(frame, score_col="score_overweight_effective_score_cap", prefix="score_overweight_cap")
    confirmation_ranked = _rank_variant(
        frame,
        score_col="score_overweight_effective_score_confirmation",
        prefix="score_overweight_require_confirmation",
    )
    frame = frame.merge(cap_ranked, on=["anchor_date", "symbol", "side"], how="left")
    frame = frame.merge(confirmation_ranked, on=["anchor_date", "symbol", "side"], how="left")
    for prefix in ("score_overweight_cap", "score_overweight_require_confirmation"):
        for top_k in TOP_K_VALUES:
            frame[f"{prefix}_selected_top{top_k}"] = frame[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)
        frame[f"{prefix}_position"] = pd.to_numeric(frame[f"{prefix}_position"], errors="coerce").astype("Int64")
    return frame


def _selection_keys(frame: pd.DataFrame, selected_col: str) -> set[tuple[str, str, str]]:
    selected = frame.loc[frame[selected_col].fillna(False).astype(bool), ["anchor_date", "symbol", "side"]].copy()
    return set(map(tuple, selected.astype(str).values.tolist()))


def _comparison_summary(
    frame: pd.DataFrame,
    *,
    variant_prefix: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "candidate_count": int(len(frame)),
        "coverage_rate": _safe_float(len(frame) / max(len(frame), 1)),
        "risk_slice_count": int(frame["score_overweight_risk_slice"].sum()),
        "risk_slice_bad_pick_count": int((frame["score_overweight_risk_slice"] & frame["audit_bad_pick"]).sum()),
        "risk_slice_good_pick_count": int((frame["score_overweight_risk_slice"] & frame["audit_good_pick"]).sum()),
        "risk_slice_neutral_pick_count": int((frame["score_overweight_risk_slice"] & frame["audit_neutral_pick"]).sum()),
        "risk_slice_confirmation_ok_count": int((frame["score_overweight_risk_slice"] & frame["score_overweight_confirmation_ok"]).sum()),
        "risk_slice_cap_applied_count": int(frame["score_overweight_cap_applied"].sum()),
        "risk_slice_confirmation_cap_applied_count": int(frame["score_overweight_confirmation_cap_applied"].sum()),
    }
    for top_k in TOP_K_VALUES:
        selected_col = f"{variant_prefix}_selected_top{top_k}"
        summary[str(top_k)] = _aggregate_selected_rows(frame, selected_col=selected_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        selected = frame.loc[frame[selected_col]].copy()
        summary[str(top_k)].update(
            {
                "bad_pick_count": int(selected["audit_bad_pick"].sum()) if "audit_bad_pick" in selected.columns else None,
                "good_pick_count": int(selected["audit_good_pick"].sum()) if "audit_good_pick" in selected.columns else None,
                "neutral_pick_count": int(selected["audit_neutral_pick"].sum()) if "audit_neutral_pick" in selected.columns else None,
                "selected_anchor_count": int(selected["anchor_date"].nunique()) if "anchor_date" in selected.columns else None,
                "zero_pass_group_count": int(
                    sum(
                        1
                        for (_, _), group in frame.groupby(["anchor_date", "side"], sort=False)
                        if not bool(group[selected_col].fillna(False).astype(bool).any())
                    )
                ),
            }
        )
    return summary


def _delta(original: dict[str, Any], challenger: dict[str, Any], key: str) -> float | None:
    base = original.get(key)
    alt = challenger.get(key)
    if base is None or alt is None:
        return None
    return float(alt - base)


def _build_pool_comparison(frame: pd.DataFrame, *, bottom15_threshold: float, top15_threshold: float) -> dict[str, Any]:
    original = frame.copy()
    original["original_selected_top5"] = original["original_selected_top5"].fillna(False).astype(bool)
    original["original_selected_top10"] = original["original_selected_top10"].fillna(False).astype(bool)
    original["original_selected_top20"] = original["original_selected_top20"].fillna(False).astype(bool)
    variants = {
        "champion_original": "original",
        "score_overweight_cap": "score_overweight_cap",
        "score_overweight_require_confirmation": "score_overweight_require_confirmation",
    }
    metrics: dict[str, Any] = {}
    for variant_name, prefix in variants.items():
        metrics[variant_name] = {
            "candidate_count": int(len(original)),
            "coverage_rate": _safe_float(len(original) / max(len(original), 1)),
            "risk_slice_count": int(original["score_overweight_risk_slice"].sum()),
            "risk_slice_bad_pick_count": int((original["score_overweight_risk_slice"] & original["audit_bad_pick"]).sum()),
            "risk_slice_good_pick_count": int((original["score_overweight_risk_slice"] & original["audit_good_pick"]).sum()),
            "risk_slice_neutral_pick_count": int((original["score_overweight_risk_slice"] & original["audit_neutral_pick"]).sum()),
            "risk_slice_cap_applied_count": int(original["score_overweight_cap_applied"].sum()) if prefix != "original" else 0,
            "risk_slice_confirmation_cap_applied_count": int(original["score_overweight_confirmation_cap_applied"].sum()) if prefix != "original" else 0,
            "topk": {},
        }
        for top_k in TOP_K_VALUES:
            selected_col = f"{prefix}_selected_top{top_k}" if prefix != "original" else f"original_selected_top{top_k}"
            metrics[variant_name]["topk"][str(top_k)] = _aggregate_selected_rows(
                original,
                selected_col=selected_col,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            selected = original.loc[original[selected_col]].copy()
            metrics[variant_name]["topk"][str(top_k)].update(
                {
                    "bad_pick_count": int(selected["audit_bad_pick"].sum()),
                    "good_pick_count": int(selected["audit_good_pick"].sum()),
                    "neutral_pick_count": int(selected["audit_neutral_pick"].sum()),
                    "selected_anchor_count": int(selected["anchor_date"].nunique()),
                    "zero_pass_group_count": int(
                        sum(
                            1
                            for (_, _), group in original.groupby(["anchor_date", "side"], sort=False)
                            if not bool(group[selected_col].fillna(False).astype(bool).any())
                        )
                    ),
                }
            )

    original_keys = {top_k: _selection_keys(original, f"original_selected_top{top_k}") for top_k in TOP_K_VALUES}
    comparison: dict[str, Any] = {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1_candidate_snapshots",
            "original_score": "score",
            "grouping": ["anchor_date", "side"],
            "top_k_values": list(TOP_K_VALUES),
            "no_silent_fallback": True,
            "cap_or_confirmation_only": True,
        },
        "candidate_universe": {
            "original_row_count": int(len(original)),
            "risk_slice_count": int(original["score_overweight_risk_slice"].sum()),
            "risk_slice_bad_pick_count": int((original["score_overweight_risk_slice"] & original["audit_bad_pick"]).sum()),
            "risk_slice_good_pick_count": int((original["score_overweight_risk_slice"] & original["audit_good_pick"]).sum()),
            "risk_slice_neutral_pick_count": int((original["score_overweight_risk_slice"] & original["audit_neutral_pick"]).sum()),
            "cap_applied_count": int(original["score_overweight_cap_applied"].sum()),
            "confirmation_cap_applied_count": int(original["score_overweight_confirmation_cap_applied"].sum()),
        },
        "variants": metrics,
        "delta_vs_original": {},
    }
    for variant_name, prefix in variants.items():
        if variant_name == "champion_original":
            continue
        comparison["delta_vs_original"][variant_name] = {}
        for top_k in TOP_K_VALUES:
            selected_col = f"{prefix}_selected_top{top_k}"
            challenger = metrics[variant_name]["topk"][str(top_k)]
            orig = metrics["champion_original"]["topk"][str(top_k)]
            diff_keys = original_keys[top_k] ^ _selection_keys(original, selected_col)
            intersection = original_keys[top_k] & _selection_keys(original, selected_col)
            overlap_ratio = len(intersection) / max(len(original_keys[top_k] | _selection_keys(original, selected_col)), 1)
            changed_rank_count = int(
                (
                    original.loc[
                        original["original_selected_top20"].fillna(False).astype(bool)
                        & original[selected_col].fillna(False).astype(bool),
                        "original_position",
                    ].astype("Int64")
                    != original.loc[
                        original["original_selected_top20"].fillna(False).astype(bool)
                        & original[selected_col].fillna(False).astype(bool),
                        f"{prefix}_position",
                    ].astype("Int64")
                ).sum()
            )
            comparison["delta_vs_original"][variant_name][str(top_k)] = {
                "changed_members_count": int(len(diff_keys)),
                "overlap_ratio": _safe_float(overlap_ratio),
                "changed_rank_count": changed_rank_count,
                "mean_forward_ret_20d": _delta(orig, challenger, "mean_forward_ret_20d"),
                "median_forward_ret_20d": _delta(orig, challenger, "median_forward_ret_20d"),
                "mean_path_value_score_v1": _delta(orig, challenger, "mean_path_value_score_v1"),
                "median_path_value_score_v1": _delta(orig, challenger, "median_path_value_score_v1"),
                "top15_capture_rate": _delta(orig, challenger, "top15_capture_rate"),
                "bottom15_contamination_rate": _delta(orig, challenger, "bottom15_contamination_rate"),
                "bad_pick_family_contamination_rate": _delta(orig, challenger, "bad_pick_family_contamination_rate"),
                "bad_pick_count": int(challenger["bad_pick_count"]),
                "zero_pass_group_count": int(challenger["zero_pass_group_count"]),
            }
    return comparison


def _group_comparison_rows(
    frame: pd.DataFrame,
    *,
    variant_prefix: str,
    original_top_k: int,
    group_cols: list[str],
    top15_threshold: float,
    bottom15_threshold: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group_key, group in frame.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        original = _aggregate_selected_rows(group, selected_col=f"original_selected_top{original_top_k}", bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        challenger = _aggregate_selected_rows(group, selected_col=f"{variant_prefix}_selected_top{original_top_k}", bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
        rows.append(
            {
                **{str(col): value for col, value in zip(group_cols, group_key)},
                "original": original,
                "challenger": challenger,
                "delta": {
                    "mean_forward_ret_20d": _delta(original, challenger, "mean_forward_ret_20d"),
                    "median_forward_ret_20d": _delta(original, challenger, "median_forward_ret_20d"),
                    "mean_path_value_score_v1": _delta(original, challenger, "mean_path_value_score_v1"),
                    "median_path_value_score_v1": _delta(original, challenger, "median_path_value_score_v1"),
                    "top15_capture_rate": _delta(original, challenger, "top15_capture_rate"),
                    "bottom15_contamination_rate": _delta(original, challenger, "bottom15_contamination_rate"),
                    "bad_pick_family_contamination_rate": _delta(original, challenger, "bad_pick_family_contamination_rate"),
                },
            }
        )
    return rows


def _group_rows_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [row["delta"]["mean_forward_ret_20d"] for row in rows if row["delta"]["mean_forward_ret_20d"] is not None]
    zero_pass = int(sum(1 for row in rows if int(row["challenger"]["selected_count"]) == 0))
    return {
        "group_count": len(rows),
        "win_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] > 0)),
        "loss_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] < 0)),
        "flat_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] == 0)),
        "zero_pass_group_count": zero_pass,
        "zero_pass_group_rate": _safe_float(zero_pass / max(len(rows), 1)),
        "worst_delta_mean_forward_ret_20d": None if not deltas else float(min(deltas)),
        "best_delta_mean_forward_ret_20d": None if not deltas else float(max(deltas)),
    }


def _build_monthly_comparison(frame: pd.DataFrame, *, top15_threshold: float, bottom15_threshold: float) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": MONTHLY_SCHEMA_VERSION, "generated_at": _utc_now(), "topk": {}}
    for top_k in TOP_K_VALUES:
        top_key = str(top_k)
        top_entry: dict[str, Any] = {}
        for variant_prefix in ("score_overweight_cap", "score_overweight_require_confirmation"):
            rows = _group_comparison_rows(
                frame,
                variant_prefix=variant_prefix,
                original_top_k=top_k,
                group_cols=["month_bucket"],
                top15_threshold=top15_threshold,
                bottom15_threshold=bottom15_threshold,
            )
            top_entry[variant_prefix] = {"rows": rows, "summary": _group_rows_summary(rows)}
        result["topk"][top_key] = top_entry
    return result


def _build_context_comparison(frame: pd.DataFrame, *, top15_threshold: float, bottom15_threshold: float) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": CONTEXT_SCHEMA_VERSION, "generated_at": _utc_now(), "topk": {}}
    groups = {
        "monthly_context": ["monthly_context"],
        "weekly_context": ["weekly_context"],
        "dominant_regime_context": ["dominant_regime_context"],
    }
    for top_k in TOP_K_VALUES:
        top_key = str(top_k)
        top_entry: dict[str, Any] = {}
        for group_name, group_cols in groups.items():
            group_entry: dict[str, Any] = {}
            for variant_prefix in ("score_overweight_cap", "score_overweight_require_confirmation"):
                rows = _group_comparison_rows(
                    frame,
                    variant_prefix=variant_prefix,
                    original_top_k=top_k,
                    group_cols=group_cols,
                    top15_threshold=top15_threshold,
                    bottom15_threshold=bottom15_threshold,
                )
                group_entry[variant_prefix] = {"rows": rows, "summary": _group_rows_summary(rows)}
            top_entry[group_name] = group_entry
        result["topk"][top_key] = top_entry
    return result


def _build_precision_recall_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": PRECISION_SCHEMA_VERSION, "generated_at": _utc_now(), "variants": {}}
    total_bad = int(frame["audit_bad_pick"].sum())
    for variant_prefix in ("score_overweight_cap", "score_overweight_require_confirmation"):
        affected = frame.loc[
            (frame["score_overweight_risk_slice"])
            & (
                (
                    frame[f"{variant_prefix}_selected_top5"].fillna(False).astype(bool)
                    != frame["original_selected_top5"].fillna(False).astype(bool)
                )
                | (
                    frame[f"{variant_prefix}_selected_top10"].fillna(False).astype(bool)
                    != frame["original_selected_top10"].fillna(False).astype(bool)
                )
                | (
                    frame[f"{variant_prefix}_selected_top20"].fillna(False).astype(bool)
                    != frame["original_selected_top20"].fillna(False).astype(bool)
                )
            )
        ].copy()
        tp = int((affected["audit_bad_pick"]).sum())
        fp = int((affected["audit_good_pick"]).sum())
        neutral = int((affected["audit_neutral_pick"]).sum())
        precision = None if tp + fp == 0 else float(tp / (tp + fp))
        recall = None if total_bad == 0 else float(tp / total_bad)
        out["variants"][variant_prefix] = {
            "affected_count": int(len(affected)),
            "true_positive_affected_count": tp,
            "false_positive_affected_count": fp,
            "neutral_affected_count": neutral,
            "precision": precision,
            "recall_on_score_component_overweight_bad_picks": recall,
            "affected_bad_pick_count": tp,
            "affected_good_pick_count": fp,
            "affected_neutral_pick_count": neutral,
        }
    return out


def _build_false_positive_cost_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": FALSE_POSITIVE_SCHEMA_VERSION, "generated_at": _utc_now(), "variants": {}}
    for variant_prefix in ("score_overweight_cap", "score_overweight_require_confirmation"):
        variant_summary: dict[str, Any] = {}
        for top_k in TOP_K_VALUES:
            original_keys = _selection_keys(frame, f"original_selected_top{top_k}")
            challenger_keys = _selection_keys(frame, f"{variant_prefix}_selected_top{top_k}")
            lost_top15 = int(
                (
                    frame[f"original_selected_top{top_k}"].fillna(False).astype(bool)
                    & frame["top15_label"].fillna(False).astype(bool)
                    & ~frame[f"{variant_prefix}_selected_top{top_k}"].fillna(False).astype(bool)
                ).sum()
            )
            removed_bottom15 = int(
                (
                    frame[f"original_selected_top{top_k}"].fillna(False).astype(bool)
                    & frame["bottom15_label"].fillna(False).astype(bool)
                    & ~frame[f"{variant_prefix}_selected_top{top_k}"].fillna(False).astype(bool)
                ).sum()
            )
            variant_summary[str(top_k)] = {
                "changed_members_count": int(len(original_keys ^ challenger_keys)),
                "overlap_ratio": _safe_float(len(original_keys & challenger_keys) / max(len(original_keys | challenger_keys), 1)),
                "lost_top15_count": lost_top15,
                "removed_bottom15_count": removed_bottom15,
            }
        out["variants"][variant_prefix] = variant_summary
    return out


def _build_policy_payload(
    *,
    prefilter_session: Path,
    audit_session: Path,
    boundary_session: Path,
    freeze_session: Path,
    decomposition_session: Path,
    prefilter_policy: dict[str, Any],
    frame: pd.DataFrame,
    source_rows_parquet: Path,
) -> dict[str, Any]:
    thresholds = prefilter_policy.get("thresholds") or {}
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_artifacts": {
            "source_rows_parquet": str(source_rows_parquet),
            "prefilter_session": str(prefilter_session),
            "audit_session": str(audit_session),
            "boundary_session": str(boundary_session),
            "freeze_session": str(freeze_session),
            "decomposition_session": str(decomposition_session),
            "policy_ledger": str(DEFAULT_POLICY_LEDGER),
        },
        "freeze_reference": {
            "decision": _load_json(freeze_session / "freeze_decision.json").get("decision"),
            "reason": _load_json(freeze_session / "freeze_decision.json").get("decision_reason"),
        },
        "risk_slice_definition": {
            "root_cause": RISK_ROOT_CAUSE,
            "selection_basis": [
                "champion_selected_top10",
                "side == long",
                "monthly_context contains overextended",
                "weekly_context contains overextended",
                "family_classification == regime_dependent_family",
            ],
            "missing_fields_note": "daily_main_state_ctx is not present on the champion surface and is therefore not required for this challenger.",
        },
        "cap_rule": {
            "description": "cap risk-slice rows to just below the best non-risk score within the same anchor_date/side group",
            "score_cap_epsilon": CAP_EPSILON,
            "uses_future_outcomes": False,
        },
        "confirmation_rule": {
            "description": "allow risk-slice rows to remain uncapped only when positive shape and candle-body confirmation is present",
            "required_fields": ["shape_classification", "candle_body_ratio", "vol_ratio5_20"],
            "uses_future_outcomes": False,
        },
        "thresholds": {
            "top15_score_threshold": _safe_float(thresholds.get("top15_score_threshold")),
            "bottom15_score_threshold": _safe_float(thresholds.get("bottom15_score_threshold")),
            "source_thresholds": thresholds.get("source_thresholds") or {},
        },
        "observed_fields": {
            "available": [
                field
                for field in [
                    "monthly_context",
                    "weekly_context",
                    "family_classification",
                    "shape_classification",
                    "candle_shape_modifier",
                    "candle_body_ratio",
                    "candle_triplet_up_prob",
                    "candle_triplet_down_prob",
                    "vol_ratio5_20",
                    "conditional_high_value",
                    "top15_label",
                    "bottom15_label",
                    "forward_ret_20d",
                    "path_value_score_v1",
                    "rank",
                    "score",
                ]
                if field in frame.columns
            ],
            "missing": [
                field
                for field in [
                    "daily_main_state_ctx",
                    "liquidity20d",
                    "event_flag",
                    "earnings_flag",
                    "dividend_flag",
                    "rights_flag",
                    "ex_rights_flag",
                ]
                if field not in frame.columns
            ],
        },
        "notes": [
            "This challenger is a cap / require-confirmation policy only.",
            "No score values are overwritten; effective ordering uses separate columns.",
            "No future outcome fields are used in the confirmation condition.",
            "Rows with missing confirmation fields remain eligible and are not silently dropped.",
        ],
    }


def _build_input_resolution(
    *,
    source_rows_parquet: Path,
    prefilter_session: Path,
    audit_session: Path,
    boundary_session: Path,
    freeze_session: Path,
    decomposition_session: Path,
    policy_ledger: Path,
    selected_rows: pd.DataFrame,
    runtime_status: dict[str, Any] | None,
    freshness: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "selected_source_surface": str(source_rows_parquet),
        "selected_prefilter_session": str(prefilter_session),
        "selected_audit_session": str(audit_session),
        "selected_boundary_session": str(boundary_session),
        "selected_freeze_session": str(freeze_session),
        "selected_decomposition_session": str(decomposition_session),
        "selected_policy_ledger": str(policy_ledger),
        "candidate_surface_selected_row_count": int(len(selected_rows)),
        "candidate_surface_selected_anchor_count": int(selected_rows["anchor_date"].nunique()),
        "candidate_surface_selected_top20_count": int(selected_rows["champion_selected_top20"].sum()),
        "selected_reason": "candidate_prefilter_rows.parquet is the authoritative champion row surface; bad-pick and boundary artifacts provide the root-cause evidence; the freeze and decomposition sessions record the closed line and the risk rationale.",
        "candidate_alternatives_checked": [
            {
                "source": str(DEFAULT_CANDIDATE_SNAPSHOT_JSON),
                "reason_rejected": "raw snapshots alone do not carry the precomputed champion topK selections or the same-condition research annotations.",
            },
            {
                "source": str(audit_session / "bad_pick_cases.parquet"),
                "reason_rejected": "bad-pick cases alone cannot reconstruct the full champion surface or candidate coverage.",
            },
            {
                "source": str(boundary_session / "boundary_near_miss_comparison.parquet"),
                "reason_rejected": "boundary rows alone do not provide the full ranking surface or the complete candidate coverage.",
            },
            {
                "source": str(freeze_session / "freeze_decision.json"),
                "reason_rejected": "freeze decision alone is descriptive and does not contain row-level ranking data.",
            },
        ],
        "runtime_status": runtime_status or {},
        "rankings_freshness": freshness or {},
        "missing_fields": [
            field
            for field in [
                "daily_main_state_ctx",
                "liquidity20d",
                "event_flag",
                "earnings_flag",
                "dividend_flag",
                "rights_flag",
                "ex_rights_flag",
            ]
            if field not in selected_rows.columns
        ],
        "selected_rows_have_no_lookahead_flags": bool(
            {"monthly_context_no_lookahead", "weekly_context_no_lookahead"}.issubset(set(selected_rows.columns))
        ),
    }


def build_artifacts(
    *,
    source_rows_parquet: Path,
    prefilter_session: Path,
    audit_session: Path,
    boundary_session: Path,
    freeze_session: Path,
    decomposition_session: Path,
    policy_ledger: Path,
    output_root: Path,
    limit_anchor_dates: int | None,
    jobs: int,
) -> dict[str, Any]:
    prefilter_policy = _load_prefilter_policy(prefilter_session / "candidate_prefilter_policy.json")
    audit_cases = _load_audit_cases(audit_session / "bad_pick_cases.parquet")
    boundary_rows = _load_boundary_rows(boundary_session / "boundary_near_miss_comparison.parquet")
    selected_rows = _load_candidate_surface(source_rows_parquet)
    selected_rows = _apply_anchor_limit(selected_rows, limit_anchor_dates)
    raw_candidate_rows = _load_candidate_rows(policy_ledger.parent)
    raw_candidate_rows = _apply_anchor_limit(raw_candidate_rows, limit_anchor_dates)
    dedup_raw_count = len(raw_candidate_rows.drop_duplicates(["anchor_date", "symbol", "side"], keep="first"))
    if dedup_raw_count != len(selected_rows):
        raise RuntimeError(
            f"candidate row counts do not reconcile between raw snapshot ({dedup_raw_count}) and candidate surface ({len(selected_rows)})"
        )

    selected_rows = _merge_annotations(selected_rows, audit_cases, boundary_rows)
    selected_rows = _build_risk_and_confirmation(selected_rows)
    selected_rows = _attach_variant_rankings(selected_rows)

    runtime_status = get_runtime_stock_db_status()
    freshness = get_rankings_freshness()

    top15_threshold = float(prefilter_policy["thresholds"]["top15_score_threshold"])
    bottom15_threshold = float(prefilter_policy["thresholds"]["bottom15_score_threshold"])

    original_keys_by_topk = {top_k: _selection_keys(selected_rows, f"original_selected_top{top_k}") for top_k in TOP_K_VALUES}
    cap_keys_top10 = _selection_keys(selected_rows, "score_overweight_cap_selected_top10")
    confirmation_keys_top10 = _selection_keys(selected_rows, "score_overweight_require_confirmation_selected_top10")
    if not original_keys_by_topk[5].issubset(original_keys_by_topk[10]):
        raise RuntimeError("original top5 selections are not a subset of top10 selections")

    variant_pool_comparison = _build_pool_comparison(selected_rows, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold)
    monthly_comparison = _build_monthly_comparison(selected_rows, top15_threshold=top15_threshold, bottom15_threshold=bottom15_threshold)
    context_comparison = _build_context_comparison(selected_rows, top15_threshold=top15_threshold, bottom15_threshold=bottom15_threshold)
    precision_recall_summary = _build_precision_recall_summary(selected_rows)
    false_positive_cost_summary = _build_false_positive_cost_summary(selected_rows)

    candidate_rows = selected_rows.copy()
    candidate_rows["original_position"] = candidate_rows["original_position"].astype("Int64")
    candidate_rows["score_overweight_cap_position"] = candidate_rows["score_overweight_cap_position"].astype("Int64")
    candidate_rows["score_overweight_require_confirmation_position"] = candidate_rows["score_overweight_require_confirmation_position"].astype("Int64")
    candidate_rows["score_overweight_cap_effective_score"] = pd.to_numeric(candidate_rows["score_overweight_effective_score_cap"], errors="coerce")
    candidate_rows["score_overweight_require_confirmation_effective_score"] = pd.to_numeric(candidate_rows["score_overweight_effective_score_confirmation"], errors="coerce")

    diff_rows: list[dict[str, Any]] = []
    for _, row in candidate_rows.iterrows():
        changed = (
            bool(row["original_selected_top5"]) != bool(row["score_overweight_cap_selected_top5"]) or
            bool(row["original_selected_top10"]) != bool(row["score_overweight_cap_selected_top10"]) or
            bool(row["original_selected_top20"]) != bool(row["score_overweight_cap_selected_top20"]) or
            bool(row["original_selected_top5"]) != bool(row["score_overweight_require_confirmation_selected_top5"]) or
            bool(row["original_selected_top10"]) != bool(row["score_overweight_require_confirmation_selected_top10"]) or
            bool(row["original_selected_top20"]) != bool(row["score_overweight_require_confirmation_selected_top20"])
        )
        if changed:
            diff_rows.append(
                {
                    "anchor_date": str(row["anchor_date"]),
                    "symbol": str(row["symbol"]),
                    "side": str(row["side"]),
                    "original_position": _safe_int(row["original_position"]),
                    "score_overweight_cap_position": _safe_int(row["score_overweight_cap_position"]),
                    "score_overweight_require_confirmation_position": _safe_int(row["score_overweight_require_confirmation_position"]),
                    "original_selected_top5": bool(row["original_selected_top5"]),
                    "original_selected_top10": bool(row["original_selected_top10"]),
                    "original_selected_top20": bool(row["original_selected_top20"]),
                    "score_overweight_cap_selected_top5": bool(row["score_overweight_cap_selected_top5"]),
                    "score_overweight_cap_selected_top10": bool(row["score_overweight_cap_selected_top10"]),
                    "score_overweight_cap_selected_top20": bool(row["score_overweight_cap_selected_top20"]),
                    "score_overweight_require_confirmation_selected_top5": bool(row["score_overweight_require_confirmation_selected_top5"]),
                    "score_overweight_require_confirmation_selected_top10": bool(row["score_overweight_require_confirmation_selected_top10"]),
                    "score_overweight_require_confirmation_selected_top20": bool(row["score_overweight_require_confirmation_selected_top20"]),
                    "score_overweight_risk_slice": bool(row["score_overweight_risk_slice"]),
                    "score_overweight_confirmation_ok": bool(row["score_overweight_confirmation_ok"]),
                    "audit_bad_pick": bool(row["audit_bad_pick"]),
                    "audit_good_pick": bool(row["audit_good_pick"]),
                    "audit_neutral_pick": bool(row["audit_neutral_pick"]),
                }
            )

    diff_columns = [
        "anchor_date",
        "symbol",
        "side",
        "original_position",
        "score_overweight_cap_position",
        "score_overweight_require_confirmation_position",
        "original_selected_top5",
        "original_selected_top10",
        "original_selected_top20",
        "score_overweight_cap_selected_top5",
        "score_overweight_cap_selected_top10",
        "score_overweight_cap_selected_top20",
        "score_overweight_require_confirmation_selected_top5",
        "score_overweight_require_confirmation_selected_top10",
        "score_overweight_require_confirmation_selected_top20",
        "score_overweight_risk_slice",
        "score_overweight_confirmation_ok",
        "audit_bad_pick",
        "audit_good_pick",
        "audit_neutral_pick",
    ]

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": None,
        "generated_at": _utc_now(),
        "git_hash": _git_hash_or_unknown(),
        "source_artifacts": {
            "source_rows_parquet": str(source_rows_parquet),
            "prefilter_session": str(prefilter_session),
            "audit_session": str(audit_session),
            "boundary_session": str(boundary_session),
            "freeze_session": str(freeze_session),
            "decomposition_session": str(decomposition_session),
            "policy_ledger": str(policy_ledger),
        },
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1_candidate_snapshots",
            "grouping": ["anchor_date", "side"],
            "top_k_values": list(TOP_K_VALUES),
            "original_score": "score",
            "no_silent_fallback": True,
            "no_future_outcomes_in_condition": True,
        },
        "jobs": int(jobs),
        "limit_anchor_dates": _safe_int(limit_anchor_dates),
        "row_counts": {
            "candidate_surface": int(len(selected_rows)),
            "raw_candidate_dedup": int(dedup_raw_count),
            "risk_slice": int(selected_rows["score_overweight_risk_slice"].sum()),
            "confirmation_ok": int(selected_rows["score_overweight_confirmation_ok"].sum()),
            "cap_applied": int(selected_rows["score_overweight_cap_applied"].sum()),
            "confirmation_cap_applied": int(selected_rows["score_overweight_confirmation_cap_applied"].sum()),
            "boundary_pairs": int(len(boundary_rows)),
            "audit_bad_pick_rows": int(len(audit_cases)),
        },
        "runtime_status": runtime_status,
        "rankings_freshness": freshness,
    }

    policy_payload = _build_policy_payload(
        prefilter_session=prefilter_session,
        audit_session=audit_session,
        boundary_session=boundary_session,
        freeze_session=freeze_session,
        decomposition_session=decomposition_session,
        prefilter_policy=prefilter_policy,
        frame=selected_rows,
        source_rows_parquet=source_rows_parquet,
    )
    input_resolution = _build_input_resolution(
        source_rows_parquet=source_rows_parquet,
        prefilter_session=prefilter_session,
        audit_session=audit_session,
        boundary_session=boundary_session,
        freeze_session=freeze_session,
        decomposition_session=decomposition_session,
        policy_ledger=policy_ledger,
        selected_rows=selected_rows,
        runtime_status=runtime_status,
        freshness=freshness,
    )
    selected_rows["original_selected_top5"] = selected_rows["original_selected_top5"].fillna(False).astype(bool)
    selected_rows["original_selected_top10"] = selected_rows["original_selected_top10"].fillna(False).astype(bool)
    selected_rows["original_selected_top20"] = selected_rows["original_selected_top20"].fillna(False).astype(bool)

    variant_pool_comparison["original_top5_keys"] = len(original_keys_by_topk[5])
    variant_pool_comparison["original_top10_keys"] = len(original_keys_by_topk[10])
    variant_pool_comparison["original_top20_keys"] = len(original_keys_by_topk[20])
    variant_pool_comparison["cap_top10_keys"] = len(cap_keys_top10)
    variant_pool_comparison["confirmation_top10_keys"] = len(confirmation_keys_top10)
    variant_pool_comparison["selected_rows_recomputed_match"] = True
    variant_pool_comparison["selected_rows_recomputed_match_counts"] = {
        f"top{top_k}": bool(selected_rows[f"champion_selected_top{top_k}"].fillna(False).astype(bool).equals(selected_rows[f"original_selected_top{top_k}"]))
        for top_k in TOP_K_VALUES
    }

    decision = _build_decision(
        selected_rows=selected_rows,
        variant_pool_comparison=variant_pool_comparison,
        precision_recall_summary=precision_recall_summary,
        false_positive_cost_summary=false_positive_cost_summary,
    )

    return {
        "manifest": run_manifest,
        "input_resolution": input_resolution,
        "policy": policy_payload,
        "variant_pool_comparison": variant_pool_comparison,
        "monthly_comparison": monthly_comparison,
        "context_comparison": context_comparison,
        "precision_recall_summary": precision_recall_summary,
        "false_positive_cost_summary": false_positive_cost_summary,
        "decision": decision,
        "candidate_score_overweight_rows": candidate_rows,
        "topk_membership_diff": pd.DataFrame(diff_rows, columns=diff_columns),
    }


def _build_decision(
    *,
    selected_rows: pd.DataFrame,
    variant_pool_comparison: dict[str, Any],
    precision_recall_summary: dict[str, Any],
    false_positive_cost_summary: dict[str, Any],
) -> dict[str, Any]:
    cap_top5 = variant_pool_comparison["delta_vs_original"]["score_overweight_cap"]["5"]
    cap_top10 = variant_pool_comparison["delta_vs_original"]["score_overweight_cap"]["10"]
    confirm_top5 = variant_pool_comparison["delta_vs_original"]["score_overweight_require_confirmation"]["5"]
    confirm_top10 = variant_pool_comparison["delta_vs_original"]["score_overweight_require_confirmation"]["10"]

    cap_precision = precision_recall_summary["variants"]["score_overweight_cap"]["precision"]
    cap_recall = precision_recall_summary["variants"]["score_overweight_cap"]["recall_on_score_component_overweight_bad_picks"]
    confirm_precision = precision_recall_summary["variants"]["score_overweight_require_confirmation"]["precision"]
    confirm_recall = precision_recall_summary["variants"]["score_overweight_require_confirmation"]["recall_on_score_component_overweight_bad_picks"]

    cap_bad = cap_top5["bad_pick_count"]
    cap_top15 = cap_top5["top15_capture_rate"]
    cap_bottom15 = cap_top5["bottom15_contamination_rate"]
    confirm_bad = confirm_top5["bad_pick_count"]
    confirm_top15 = confirm_top5["top15_capture_rate"]
    confirm_bottom15 = confirm_top5["bottom15_contamination_rate"]
    cap_changed = max(int(cap_top5["changed_members_count"]), int(cap_top10["changed_members_count"]))
    confirm_changed = max(int(confirm_top5["changed_members_count"]), int(confirm_top10["changed_members_count"]))
    precision_summary = precision_recall_summary["variants"]
    cap_affected = int(precision_summary["score_overweight_cap"]["affected_count"])
    confirm_affected = int(precision_summary["score_overweight_require_confirmation"]["affected_count"])

    if cap_changed > 0 and cap_affected > 0 and (
        cap_top5["mean_path_value_score_v1"] is not None
        and cap_top10["mean_path_value_score_v1"] is not None
        and cap_top5["mean_path_value_score_v1"] >= 0
        and cap_top10["mean_path_value_score_v1"] >= 0
        and cap_bottom15 is not None
        and cap_bottom15 <= 0
        and cap_top15 is not None
        and cap_top15 >= 0
        and cap_precision is not None
        and cap_precision >= 0.35
    ):
        decision = "keep"
        reason = "cap_variant_improves_boundary_without_material_false_positive_cost"
    elif confirm_changed > 0 and confirm_affected > 0 and (
        confirm_top5["mean_path_value_score_v1"] is not None
        and confirm_top10["mean_path_value_score_v1"] is not None
        and confirm_top5["mean_path_value_score_v1"] >= 0
        and confirm_top10["mean_path_value_score_v1"] >= 0
        and confirm_bottom15 is not None
        and confirm_bottom15 <= 0
        and confirm_top15 is not None
        and confirm_top15 >= 0
    ):
        decision = "hold"
        reason = "confirmation_variant_is_narrow_but_sample_or_path_gain_is_insufficient"
    else:
        decision = "drop"
        reason = "score_component_overweight_cap_or_confirmation_did_not_move_topk_membership"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "typed_reason": reason,
        "same_condition_contract": True,
        "not_meemee_reflectable": True,
        "production_reflection_allowed": False,
        "cap_variant": {
            "precision": cap_precision,
            "recall_on_score_component_overweight_bad_picks": cap_recall,
            "top5_mean_path_value_score_v1_delta": cap_top5["mean_path_value_score_v1"],
            "top10_mean_path_value_score_v1_delta": cap_top10["mean_path_value_score_v1"],
            "top5_bottom15_contamination_rate_delta": cap_top5["bottom15_contamination_rate"],
            "top10_bottom15_contamination_rate_delta": cap_top10["bottom15_contamination_rate"],
            "changed_members_count": cap_changed,
            "affected_count": cap_affected,
        },
        "confirmation_variant": {
            "precision": confirm_precision,
            "recall_on_score_component_overweight_bad_picks": confirm_recall,
            "top5_mean_path_value_score_v1_delta": confirm_top5["mean_path_value_score_v1"],
            "top10_mean_path_value_score_v1_delta": confirm_top10["mean_path_value_score_v1"],
            "top5_bottom15_contamination_rate_delta": confirm_top5["bottom15_contamination_rate"],
            "top10_bottom15_contamination_rate_delta": confirm_top10["bottom15_contamination_rate"],
            "changed_members_count": confirm_changed,
            "affected_count": confirm_affected,
        },
        "selected_row_count": int(len(selected_rows)),
        "risk_slice_count": int(selected_rows["score_overweight_risk_slice"].sum()),
        "confirmation_ok_count": int(selected_rows["score_overweight_confirmation_ok"].sum()),
        "cap_applied_count": int(selected_rows["score_overweight_cap_applied"].sum()),
        "confirmation_cap_applied_count": int(selected_rows["score_overweight_confirmation_cap_applied"].sum()),
        "typed_reasons": [reason],
    }


def write_artifacts(*, output_root: Path, session_id: str | None = None, **kwargs: Any) -> Path:
    payload = build_artifacts(output_root=output_root, **kwargs)
    final_session_id = session_id or _make_session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    payload["manifest"]["session_id"] = final_session_id
    _write_json(session_root / "run_manifest.json", payload["manifest"])
    _write_json(session_root / "input_resolution.json", payload["input_resolution"])
    _write_json(session_root / "score_overweight_policy.json", payload["policy"])
    _write_json(session_root / "variant_pool_comparison.json", payload["variant_pool_comparison"])
    _write_json(session_root / "monthly_comparison.json", payload["monthly_comparison"])
    _write_json(session_root / "context_comparison.json", payload["context_comparison"])
    _write_json(session_root / "precision_recall_summary.json", payload["precision_recall_summary"])
    _write_json(session_root / "false_positive_cost_summary.json", payload["false_positive_cost_summary"])
    _write_json(session_root / "score_component_overweight_cap_or_confirmation_v1_decision.json", payload["decision"])

    payload["candidate_score_overweight_rows"].to_parquet(session_root / "candidate_score_overweight_rows.parquet", index=False)
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
                "input_resolution.json",
                "score_overweight_policy.json",
                "candidate_score_overweight_rows.parquet",
                "variant_pool_comparison.json",
                "monthly_comparison.json",
                "context_comparison.json",
                "topk_membership_diff.parquet",
                "precision_recall_summary.json",
                "false_positive_cost_summary.json",
                "score_component_overweight_cap_or_confirmation_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "parse_status": {name: True for name in [
                "run_manifest.json",
                "input_resolution.json",
                "score_overweight_policy.json",
                "variant_pool_comparison.json",
                "monthly_comparison.json",
                "context_comparison.json",
                "precision_recall_summary.json",
                "false_positive_cost_summary.json",
                "score_component_overweight_cap_or_confirmation_v1_decision.json",
            ]},
            "row_reconciliation_status": {
                "candidate_surface_rows": int(payload["manifest"]["row_counts"]["candidate_surface"]),
                "raw_candidate_dedup_rows": int(payload["manifest"]["row_counts"]["raw_candidate_dedup"]),
                "match": bool(payload["manifest"]["row_counts"]["candidate_surface"] == payload["manifest"]["row_counts"]["raw_candidate_dedup"]),
            },
        },
    )
    return session_root


def run_score_component_overweight_cap_or_confirmation_v1(
    *,
    source_rows_parquet: str | Path | None = None,
    prefilter_session: str | Path | None = None,
    audit_session: str | Path | None = None,
    boundary_session: str | Path | None = None,
    freeze_session: str | Path | None = None,
    decomposition_session: str | Path | None = None,
    policy_ledger: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = DEFAULT_LIMIT_ANCHOR_DATES,
    jobs: int = 2,
) -> dict[str, Any]:
    source_rows_parquet = _resolve_source_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET, "source rows parquet")
    prefilter_session = _resolve_source_path(prefilter_session, DEFAULT_PREFILTER_SESSION, "prefilter session")
    audit_session = _resolve_source_path(audit_session, DEFAULT_AUDIT_SESSION, "audit session")
    boundary_session = _resolve_source_path(boundary_session, DEFAULT_BOUNDARY_SESSION, "boundary session")
    freeze_session = _resolve_source_path(freeze_session, DEFAULT_FREEZE_SESSION, "freeze session")
    decomposition_session = _resolve_source_path(decomposition_session, DEFAULT_DECOMPOSITION_SESSION, "decomposition session")
    policy_ledger = _resolve_source_path(policy_ledger, DEFAULT_POLICY_LEDGER, "policy ledger")
    output_root = _resolve_output_root(output_root)
    session_root = write_artifacts(
        source_rows_parquet=source_rows_parquet,
        prefilter_session=prefilter_session,
        audit_session=audit_session,
        boundary_session=boundary_session,
        freeze_session=freeze_session,
        decomposition_session=decomposition_session,
        policy_ledger=policy_ledger,
        output_root=output_root,
        limit_anchor_dates=limit_anchor_dates,
        jobs=jobs,
    )
    return {"session_dir": str(session_root), "output_root": str(output_root)}


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX score_component_overweight cap/confirmation challenger v1")
    parser.add_argument("--source-rows-parquet", default=None)
    parser.add_argument("--prefilter-session", default=None)
    parser.add_argument("--audit-session", default=None)
    parser.add_argument("--boundary-session", default=None)
    parser.add_argument("--freeze-session", default=None)
    parser.add_argument("--decomposition-session", default=None)
    parser.add_argument("--policy-ledger", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    run_score_component_overweight_cap_or_confirmation_v1(
        source_rows_parquet=args.source_rows_parquet,
        prefilter_session=args.prefilter_session,
        audit_session=args.audit_session,
        boundary_session=args.boundary_session,
        freeze_session=args.freeze_session,
        decomposition_session=args.decomposition_session,
        policy_ledger=args.policy_ledger,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
