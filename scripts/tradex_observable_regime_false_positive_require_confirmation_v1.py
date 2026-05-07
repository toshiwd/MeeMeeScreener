from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_observable_regime_false_positive_require_confirmation_v1"
FAMILY_ID = "observable_regime_false_positive_require_confirmation_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1")
DEFAULT_BACKFILL_SESSION = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646")
DEFAULT_RECLASSIFICATION_SESSION = Path(r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991")
DEFAULT_CANDIDATE_SURFACE = DEFAULT_BACKFILL_SESSION / "candidate_prefilter_rows_context_enriched.parquet"
DEFAULT_UNKNOWN_RECLASSIFICATION = DEFAULT_RECLASSIFICATION_SESSION / "enriched_unknown_reclassification_rows.parquet"
DEFAULT_NO_LOOKAHEAD_AUDIT = DEFAULT_BACKFILL_SESSION / "no_lookahead_context_audit.json"
DEFAULT_CONTEXT_JOIN_CONTRACT = DEFAULT_BACKFILL_SESSION / "context_join_contract.json"
DEFAULT_FUTURE_CHALLENGER_CANDIDATES = DEFAULT_RECLASSIFICATION_SESSION / "future_challenger_candidates.json"
DEFAULT_ENRICHED_TAXONOMY = DEFAULT_RECLASSIFICATION_SESSION / "enriched_root_cause_taxonomy_summary.json"
DEFAULT_ENRICHED_FAMILY_BREAKDOWN = DEFAULT_RECLASSIFICATION_SESSION / "enriched_root_cause_family_breakdown.json"
DEFAULT_ENRICHED_PAIRWISE = DEFAULT_RECLASSIFICATION_SESSION / "enriched_unknown_boundary_pairwise.parquet"
DEFAULT_ENRICHED_PAIRWISE_SUMMARY = DEFAULT_RECLASSIFICATION_SESSION / "enriched_unknown_boundary_pairwise_summary.json"
DEFAULT_DECISION_SOURCE = DEFAULT_RECLASSIFICATION_SESSION / "bad_pick_unknown_reclassification_enriched_v1_decision.json"

SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_input_resolution_v1"
PROFILE_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_profile_v1"
POLICY_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_policy_v1"
POOL_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_pool_comparison_v1"
MONTHLY_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_context_comparison_v1"
DIFF_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_topk_membership_diff_v1"
PRECISION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_precision_recall_summary_v1"
FALSE_POS_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_false_positive_cost_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
RISK_FAMILY_CODE = "observable_regime_false_positive"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def _unique_ordered(items: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    seen: set[Any] = set()
    for item in items:
        if item is None:
            continue
        marker = item
        if marker in seen:
            continue
        seen.add(marker)
        out.append(item)
    return out


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "<na>", "none", "null"}:
        return ""
    return text


def _is_overextended(value: Any) -> bool:
    text = _normalize_text(value).lower()
    return "overextended" in text


def _is_missing_token(value: Any) -> bool:
    text = _normalize_text(value)
    return text == ""


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    for column in ("anchor_date", "side", "symbol"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _source_inventory(path: Path, *, grain: str, date_key: str, symbol_key: str, point_in_time_safe: bool, no_lookahead_flag: bool | str, coverage_estimate: float | None, fields: list[str]) -> dict[str, Any]:
    return {
        "path": str(path),
        "grain": grain,
        "date_key": date_key,
        "symbol_key": symbol_key,
        "point_in_time_safe": point_in_time_safe,
        "no_lookahead_flag": no_lookahead_flag,
        "coverage_estimate": coverage_estimate,
        "fields": fields,
    }


def _select_topk(frame: pd.DataFrame, *, confirmed_col: str, score_col: str = "score") -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for (_, _side), group in frame.groupby(["anchor_date", "side"], sort=False):
        ordered = group.sort_values([confirmed_col, score_col], ascending=[False, False]).copy()
        ordered["variant_group_rank"] = range(1, len(ordered) + 1)
        for k in TOP_K_VALUES:
            ordered[f"variant_selected_top{k}"] = False
            limit = min(k, len(ordered))
            if limit:
                ordered.iloc[:limit, ordered.columns.get_loc(f"variant_selected_top{k}")] = True
        rows.append(ordered)
    if not rows:
        return frame.copy()
    return pd.concat(rows, ignore_index=True)


def _select_baseline(frame: pd.DataFrame, *, score_col: str = "score") -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for (_, _side), group in frame.groupby(["anchor_date", "side"], sort=False):
        ordered = group.sort_values([score_col], ascending=[False]).copy()
        ordered["baseline_group_rank"] = range(1, len(ordered) + 1)
        for k in TOP_K_VALUES:
            ordered[f"baseline_selected_top{k}"] = False
            limit = min(k, len(ordered))
            if limit:
                ordered.iloc[:limit, ordered.columns.get_loc(f"baseline_selected_top{k}")] = True
        rows.append(ordered)
    if not rows:
        return frame.copy()
    return pd.concat(rows, ignore_index=True)


def _path_stats(series: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce")
    return {
        "mean": _safe_float(numeric.mean()),
        "median": _safe_float(numeric.median()),
        "min": _safe_float(numeric.min()),
        "max": _safe_float(numeric.max()),
    }


def _build_pair_summary(pairwise: pd.DataFrame) -> dict[str, Any]:
    if pairwise.empty:
        return {
            "pair_count": 0,
            "matched_near_miss_count": 0,
            "selected_higher_score_count": 0,
            "selected_worse_path_count": 0,
            "selected_higher_score_and_worse_path_count": 0,
            "monthly_alignment_same_count": 0,
            "weekly_alignment_same_count": 0,
            "daily_alignment_same_count": 0,
            "shape_alignment_same_count": 0,
            "score_gap_mean": None,
            "score_gap_median": None,
            "forward_ret_20d_gap_mean": None,
            "forward_ret_20d_gap_median": None,
            "path_value_gap_mean": None,
            "path_value_gap_median": None,
        }
    score_gap = pd.to_numeric(pairwise["score"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_score"], errors="coerce"))
    ret_gap = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_forward_ret_20d"], errors="coerce"))
    path_gap = pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_path_value_score_v1"], errors="coerce"))
    return {
        "pair_count": int(len(pairwise)),
        "matched_near_miss_count": int(pairwise["near_miss_joined"].sum()) if "near_miss_joined" in pairwise.columns else int(len(pairwise)),
        "selected_higher_score_count": int((score_gap > 0).sum()),
        "selected_worse_path_count": int((ret_gap < 0).sum()),
        "selected_higher_score_and_worse_path_count": int(((score_gap > 0) & (ret_gap < 0)).sum()),
        "monthly_alignment_same_count": int(pairwise["monthly_alignment_same"].sum()) if "monthly_alignment_same" in pairwise.columns else 0,
        "weekly_alignment_same_count": int(pairwise["weekly_alignment_same"].sum()) if "weekly_alignment_same" in pairwise.columns else 0,
        "daily_alignment_same_count": int(pairwise["daily_alignment_same"].sum()) if "daily_alignment_same" in pairwise.columns else 0,
        "shape_alignment_same_count": int(pairwise["shape_alignment_same"].sum()) if "shape_alignment_same" in pairwise.columns else 0,
        "score_gap_mean": _safe_float(score_gap.mean()),
        "score_gap_median": _safe_float(score_gap.median()),
        "forward_ret_20d_gap_mean": _safe_float(ret_gap.mean()),
        "forward_ret_20d_gap_median": _safe_float(ret_gap.median()),
        "path_value_gap_mean": _safe_float(path_gap.mean()),
        "path_value_gap_median": _safe_float(path_gap.median()),
    }


def _count_labels(frame: pd.DataFrame, column: str) -> dict[str, int]:
    return {str(key): int(value) for key, value in frame[column].value_counts(dropna=False).items()}


def _join_candidate_family(candidate: pd.DataFrame, family_rows: pd.DataFrame) -> pd.DataFrame:
    key = ["anchor_date", "symbol", "side", "rank"]
    frame = candidate.merge(
        family_rows[key + ["reclassified_root_cause_code", "reclassification_confidence", "missingness_class"]],
        on=key,
        how="left",
        suffixes=("", "_family"),
    )
    frame["family_code"] = frame["reclassified_root_cause_code"].fillna("other").astype("string")
    frame["is_risk_family"] = frame["family_code"].eq(RISK_FAMILY_CODE)
    frame["top15_label"] = frame["top15_label"].fillna(False).astype(bool)
    frame["bottom15_label"] = frame["bottom15_label"].fillna(False).astype(bool)
    return frame


def _confirmation_mask(frame: pd.DataFrame) -> pd.Series:
    shape_confirm = frame["shape_classification"].isin({"shape_positive_modifier", "shape_context_dependent"})
    family_confirm = frame["family_classification"].isin({"regime_dependent_family", "stable_high_value_family"})
    monthly_week_confirm = ~(
        frame["monthly_context"].astype("string").fillna("").map(_is_overextended)
        & frame["weekly_context"].astype("string").fillna("").map(_is_overextended)
    )
    return (shape_confirm | family_confirm) & monthly_week_confirm


def _build_family_profile(frame: pd.DataFrame, pairwise: pd.DataFrame, pairwise_summary: dict[str, Any]) -> dict[str, Any]:
    risk = frame.loc[frame["family_code"].eq(RISK_FAMILY_CODE)].copy()
    top5 = risk["champion_selected_top5"].fillna(False).astype(bool)
    top10 = risk["champion_selected_top10"].fillna(False).astype(bool)
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "family_code": RISK_FAMILY_CODE,
        "count": int(len(risk)),
        "top5_count": int(top5.sum()),
        "top10_count": int((top10 & ~top5).sum()),
        "top20_count": int(risk["champion_selected_top20"].fillna(False).astype(bool).sum()),
        "side_split": _count_labels(risk, "side"),
        "month_split": _count_labels(risk, "month_bucket"),
        "regime_split": _count_labels(risk, "dominant_regime_context"),
        "monthly_state_distribution": _count_labels(risk, "monthly_main_state_ctx_backfilled"),
        "weekly_state_distribution": _count_labels(risk, "weekly_main_state_ctx_backfilled"),
        "daily_state_distribution": _count_labels(risk, "daily_main_state_ctx_backfilled"),
        "shape_classification_distribution": _count_labels(risk, "shape_classification"),
        "family_classification_distribution": _count_labels(risk, "family_classification"),
        "ma_distance_distribution": {
            "dist_ma20_pct": _path_stats(risk["dist_ma20_pct"]),
            "dist_ma60_pct": _path_stats(risk["dist_ma60_pct"]),
            "gap_pct": _path_stats(risk["gap_pct"]),
        },
        "liquidity_coverage": _safe_float(risk["liquidity20d"].notna().mean()) if "liquidity20d" in risk.columns else None,
        "volume_coverage": _safe_float(risk["vol_ratio5_20"].notna().mean()) if "vol_ratio5_20" in risk.columns else None,
        "no_lookahead_flag_coverage": {
            "monthly_context_no_lookahead": _safe_float(risk["monthly_context_no_lookahead_backfilled"].notna().mean()) if "monthly_context_no_lookahead_backfilled" in risk.columns else None,
            "weekly_context_no_lookahead": _safe_float(risk["weekly_context_no_lookahead_backfilled"].notna().mean()) if "weekly_context_no_lookahead_backfilled" in risk.columns else None,
            "daily_main_state_ctx_no_lookahead": _safe_float(risk["daily_main_state_ctx_no_lookahead_backfilled"].notna().mean()) if "daily_main_state_ctx_no_lookahead_backfilled" in risk.columns else None,
        },
        "overlap_with_good_picks": {
            "top15_label_count": int(risk["top15_label"].sum()),
            "bottom15_label_count": int(risk["bottom15_label"].sum()),
            "top15_label_rate": _safe_float(risk["top15_label"].mean()),
            "bottom15_label_rate": _safe_float(risk["bottom15_label"].mean()),
        },
        "boundary_near_miss_summary": {
            "matched_near_miss_count": int(pairwise_summary.get("matched_near_miss_count", 0)),
            "boundary_match_rate": _safe_float(pairwise_summary.get("matched_near_miss_count", 0) / max(len(risk), 1)),
            "selected_higher_score_and_worse_path_count": int(pairwise_summary.get("selected_higher_score_and_worse_path_count", 0)),
            "score_gap_mean": _safe_float(pairwise_summary.get("score_gap_mean")),
            "forward_ret_20d_gap_mean": _safe_float(pairwise_summary.get("forward_ret_20d_gap_mean")),
            "path_value_gap_mean": _safe_float(pairwise_summary.get("path_value_gap_mean")),
        },
        "notes": [
            "family is large and top5-heavy on the enriched unknown rerun",
            "confirmation rule uses monthly / weekly context plus shape and family classification",
        ],
    }


def _build_policy_spec() -> dict[str, Any]:
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "family_code": RISK_FAMILY_CODE,
        "policy_name": "observable_regime_false_positive_require_confirmation_v1",
        "rule_type": "require-confirmation",
        "rule": {
            "keep_original_ordering_when": [
                "shape_classification in {shape_positive_modifier, shape_context_dependent}",
                "OR family_classification in {regime_dependent_family, stable_high_value_family}",
                "AND not(monthly_context is overextended AND weekly_context is overextended)",
            ],
            "deprioritize_when": [
                "family_code == observable_regime_false_positive",
                "and confirmation is absent",
                "rows stay sorted by original score after non-risk rows",
            ],
        },
        "required_fields": [
            "monthly_context",
            "weekly_context",
            "monthly_main_state_ctx_backfilled",
            "weekly_main_state_ctx_backfilled",
            "daily_main_state_ctx_backfilled",
            "shape_classification",
            "family_classification",
            "score",
        ],
        "no_lookahead_safe_fields": [
            "monthly_context",
            "weekly_context",
            "monthly_main_state_ctx_backfilled",
            "weekly_main_state_ctx_backfilled",
            "daily_main_state_ctx_backfilled",
            "shape_classification",
            "family_classification",
        ],
        "excluded_fields": [
            "forward_ret_20d",
            "ret_5",
            "ret_10",
            "ret_20",
            "path_value_score_v1",
            "realized_pnl",
            "future_pnl",
        ],
        "why_no_lookahead_safe": "The confirmation uses only backfilled same-day or earlier monthly / weekly / daily state surfaces and derived classification fields; no future return or realized PnL fields are consulted.",
        "why_not_frozen_line": "This is a confirmation rule on the enriched observable regime family, not the frozen cash-gate or score-overweight lines.",
        "overfit_guardrail": "The rule is fixed and does not key off outcome horizons, month pockets, or symbol-specific thresholds.",
    }


def _build_pool_comparison(baseline: pd.DataFrame, variant: pd.DataFrame, merged: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": POOL_SCHEMA_VERSION,
        "baseline": {},
        "variant": {},
        "comparison": {},
        "topk_rows": {},
    }
    for k in TOP_K_VALUES:
        baseline_sel = baseline.loc[baseline[f"baseline_selected_top{k}"]].copy()
        variant_sel = variant.loc[variant[f"variant_selected_top{k}"]].copy()
        summary["baseline"][f"top{k}"] = {
            "count": int(len(baseline_sel)),
            "mean_forward_ret_20d": _safe_float(pd.to_numeric(baseline_sel["forward_ret_20d"], errors="coerce").mean()),
            "mean_path_value_score_v1": _safe_float(pd.to_numeric(baseline_sel["path_value_score_v1"], errors="coerce").mean()),
            "top15_count": int(baseline_sel["top15_label"].sum()),
            "bottom15_count": int(baseline_sel["bottom15_label"].sum()),
            "risk_family_count": int(baseline_sel["is_risk_family"].sum()),
            "top15_precision": _safe_float(baseline_sel["top15_label"].mean()),
            "bottom15_precision": _safe_float(baseline_sel["bottom15_label"].mean()),
        }
        summary["variant"][f"top{k}"] = {
            "count": int(len(variant_sel)),
            "mean_forward_ret_20d": _safe_float(pd.to_numeric(variant_sel["forward_ret_20d"], errors="coerce").mean()),
            "mean_path_value_score_v1": _safe_float(pd.to_numeric(variant_sel["path_value_score_v1"], errors="coerce").mean()),
            "top15_count": int(variant_sel["top15_label"].sum()),
            "bottom15_count": int(variant_sel["bottom15_label"].sum()),
            "risk_family_count": int(variant_sel["is_risk_family"].sum()),
            "top15_precision": _safe_float(variant_sel["top15_label"].mean()),
            "bottom15_precision": _safe_float(variant_sel["bottom15_label"].mean()),
        }
        summary["comparison"][f"top{k}"] = {
            "delta_mean_forward_ret_20d": _safe_float(summary["variant"][f"top{k}"]["mean_forward_ret_20d"] - summary["baseline"][f"top{k}"]["mean_forward_ret_20d"]),
            "delta_mean_path_value_score_v1": _safe_float(summary["variant"][f"top{k}"]["mean_path_value_score_v1"] - summary["baseline"][f"top{k}"]["mean_path_value_score_v1"]),
            "delta_top15_count": int(summary["variant"][f"top{k}"]["top15_count"] - summary["baseline"][f"top{k}"]["top15_count"]),
            "delta_bottom15_count": int(summary["variant"][f"top{k}"]["bottom15_count"] - summary["baseline"][f"top{k}"]["bottom15_count"]),
            "delta_risk_family_count": int(summary["variant"][f"top{k}"]["risk_family_count"] - summary["baseline"][f"top{k}"]["risk_family_count"]),
        }
    summary["changed_members"] = {
        "top5": int((merged["baseline_selected_top5"] != merged["variant_selected_top5"]).sum()),
        "top10": int((merged["baseline_selected_top10"] != merged["variant_selected_top10"]).sum()),
        "top20": int((merged["baseline_selected_top20"] != merged["variant_selected_top20"]).sum()),
    }
    summary["overlap_ratio_vs_original"] = {
        "top5": _safe_float((merged["baseline_selected_top5"] & merged["variant_selected_top5"]).sum() / max(int(merged["baseline_selected_top5"].sum()), 1)),
        "top10": _safe_float((merged["baseline_selected_top10"] & merged["variant_selected_top10"]).sum() / max(int(merged["baseline_selected_top10"].sum()), 1)),
        "top20": _safe_float((merged["baseline_selected_top20"] & merged["variant_selected_top20"]).sum() / max(int(merged["baseline_selected_top20"].sum()), 1)),
    }
    summary["changed_rank_count"] = int((merged["baseline_group_rank"] != merged["variant_group_rank"]).sum())
    summary["selection_divergence_reason"] = "risk_family_rows_without_confirmation_are_deprioritized_below_non_risk_rows"
    summary["zero_pass_groups"] = {
        "top5": int(merged.groupby(["anchor_date", "side"], sort=False)["variant_selected_top5"].sum().eq(0).sum()),
        "top10": int(merged.groupby(["anchor_date", "side"], sort=False)["variant_selected_top10"].sum().eq(0).sum()),
    }
    return summary


def _bucket_summary(frame: pd.DataFrame, bucket_col: str, baseline_col: str, variant_col: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket, group in frame.groupby(bucket_col, dropna=False):
        baseline_sel = group.loc[group[baseline_col]]
        variant_sel = group.loc[group[variant_col]]
        rows.append(
            {
                "bucket": "" if pd.isna(bucket) else str(bucket),
                "baseline_count": int(len(baseline_sel)),
                "variant_count": int(len(variant_sel)),
                "baseline_mean_forward_ret_20d": _safe_float(pd.to_numeric(baseline_sel["forward_ret_20d"], errors="coerce").mean()),
                "variant_mean_forward_ret_20d": _safe_float(pd.to_numeric(variant_sel["forward_ret_20d"], errors="coerce").mean()),
                "baseline_mean_path_value_score_v1": _safe_float(pd.to_numeric(baseline_sel["path_value_score_v1"], errors="coerce").mean()),
                "variant_mean_path_value_score_v1": _safe_float(pd.to_numeric(variant_sel["path_value_score_v1"], errors="coerce").mean()),
                "baseline_top15_count": int(baseline_sel["top15_label"].sum()),
                "variant_top15_count": int(variant_sel["top15_label"].sum()),
                "baseline_bottom15_count": int(baseline_sel["bottom15_label"].sum()),
                "variant_bottom15_count": int(variant_sel["bottom15_label"].sum()),
            }
        )
    return sorted(rows, key=lambda row: (-max(row["baseline_count"], row["variant_count"]), row["bucket"]))


def _build_monthly_comparison(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "top5": _bucket_summary(frame, "month_bucket", "baseline_selected_top5", "variant_selected_top5"),
        "top10": _bucket_summary(frame, "month_bucket", "baseline_selected_top10", "variant_selected_top10"),
    }


def _build_context_comparison(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "monthly_state_buckets_top5": _bucket_summary(frame, "monthly_main_state_ctx_backfilled", "baseline_selected_top5", "variant_selected_top5"),
        "monthly_state_buckets_top10": _bucket_summary(frame, "monthly_main_state_ctx_backfilled", "baseline_selected_top10", "variant_selected_top10"),
        "weekly_state_buckets_top5": _bucket_summary(frame, "weekly_main_state_ctx_backfilled", "baseline_selected_top5", "variant_selected_top5"),
        "weekly_state_buckets_top10": _bucket_summary(frame, "weekly_main_state_ctx_backfilled", "baseline_selected_top10", "variant_selected_top10"),
        "daily_state_buckets_top5": _bucket_summary(frame, "daily_main_state_ctx_backfilled", "baseline_selected_top5", "variant_selected_top5"),
        "daily_state_buckets_top10": _bucket_summary(frame, "daily_main_state_ctx_backfilled", "baseline_selected_top10", "variant_selected_top10"),
        "regime_buckets_top5": _bucket_summary(frame, "dominant_regime_context", "baseline_selected_top5", "variant_selected_top5"),
        "regime_buckets_top10": _bucket_summary(frame, "dominant_regime_context", "baseline_selected_top10", "variant_selected_top10"),
    }


def _build_precision_recall_summary(merged: pd.DataFrame) -> dict[str, Any]:
    baseline_changed_top5 = merged["baseline_selected_top5"] & ~merged["variant_selected_top5"]
    baseline_changed_top10 = merged["baseline_selected_top10"] & ~merged["variant_selected_top10"]
    variant_top5 = merged["variant_selected_top5"]
    variant_top10 = merged["variant_selected_top10"]
    baseline_top5 = merged["baseline_selected_top5"]
    baseline_top10 = merged["baseline_selected_top10"]
    selected_scope = baseline_top10 | variant_top10

    deprioritized_bad = int((baseline_changed_top10 & merged["bottom15_label"]).sum())
    deprioritized_good = int((baseline_changed_top10 & merged["top15_label"]).sum())
    confirmed_bad = int((variant_top10 & merged["bottom15_label"]).sum())
    confirmed_good = int((variant_top10 & merged["top15_label"]).sum())

    total_risk_top10 = int((baseline_top10 & merged["is_risk_family"]).sum())
    removed_risk_top10 = int((baseline_changed_top10 & merged["is_risk_family"]).sum())

    total_top15 = int((baseline_top10 & merged["top15_label"]).sum())
    lost_top15 = int((baseline_changed_top10 & merged["top15_label"]).sum())
    total_bottom15 = int((baseline_top10 & merged["bottom15_label"]).sum())
    removed_bottom15 = int((baseline_changed_top10 & merged["bottom15_label"]).sum())

    return {
        "schema_version": PRECISION_SCHEMA_VERSION,
        "deprioritized_actual_bad_picks": deprioritized_bad,
        "deprioritized_actual_good_picks": deprioritized_good,
        "confirmed_actual_bad_picks": confirmed_bad,
        "confirmed_actual_good_picks": confirmed_good,
        "lost_top15_count": lost_top15,
        "removed_bottom15_count": removed_bottom15,
        "precision_on_bad_pick_removal": _safe_float(deprioritized_bad / max(deprioritized_bad + deprioritized_good, 1)),
        "recall_on_observable_regime_false_positive_bad_picks": _safe_float(removed_risk_top10 / max(total_risk_top10, 1)),
        "top15_capture_rate": _safe_float((variant_top10 & merged["top15_label"]).sum() / max(int(merged["top15_label"].sum()), 1)),
        "bottom15_contamination_rate": _safe_float((variant_top10 & merged["bottom15_label"]).sum() / max(int(merged["bottom15_label"].sum()), 1)),
        "baseline_scope_count": int(selected_scope.sum()),
        "baseline_top15_count": total_top15,
        "baseline_bottom15_count": total_bottom15,
    }


def _build_false_positive_cost_summary(merged: pd.DataFrame) -> dict[str, Any]:
    variant_top10 = merged["variant_selected_top10"]
    baseline_top10 = merged["baseline_selected_top10"]
    return {
        "schema_version": FALSE_POS_SCHEMA_VERSION,
        "baseline_risk_selected_top5": int((merged["baseline_selected_top5"] & merged["is_risk_family"]).sum()),
        "baseline_risk_selected_top10": int((merged["baseline_selected_top10"] & merged["is_risk_family"]).sum()),
        "variant_risk_selected_top5": int((merged["variant_selected_top5"] & merged["is_risk_family"]).sum()),
        "variant_risk_selected_top10": int((merged["variant_selected_top10"] & merged["is_risk_family"]).sum()),
        "variant_risk_deprioritized_top5": int(((merged["baseline_selected_top5"] & ~merged["variant_selected_top5"]) & merged["is_risk_family"]).sum()),
        "variant_risk_deprioritized_top10": int(((merged["baseline_selected_top10"] & ~merged["variant_selected_top10"]) & merged["is_risk_family"]).sum()),
        "baseline_bottom15_selected_top10": int((baseline_top10 & merged["bottom15_label"]).sum()),
        "variant_bottom15_selected_top10": int((variant_top10 & merged["bottom15_label"]).sum()),
        "baseline_top15_selected_top10": int((baseline_top10 & merged["top15_label"]).sum()),
        "variant_top15_selected_top10": int((variant_top10 & merged["top15_label"]).sum()),
        "bottom15_precision_baseline_top10": _safe_float((baseline_top10 & merged["bottom15_label"]).sum() / max(int(baseline_top10.sum()), 1)),
        "bottom15_precision_variant_top10": _safe_float((variant_top10 & merged["bottom15_label"]).sum() / max(int(variant_top10.sum()), 1)),
        "top15_precision_baseline_top10": _safe_float((baseline_top10 & merged["top15_label"]).sum() / max(int(baseline_top10.sum()), 1)),
        "top15_precision_variant_top10": _safe_float((variant_top10 & merged["top15_label"]).sum() / max(int(variant_top10.sum()), 1)),
    }


def _build_topk_membership_diff(merged: pd.DataFrame) -> pd.DataFrame:
    diff = merged[
        [
            "anchor_date",
            "symbol",
            "side",
            "rank",
            "score",
            "family_code",
            "is_risk_family",
            "baseline_group_rank",
            "variant_group_rank",
            "baseline_selected_top5",
            "baseline_selected_top10",
            "baseline_selected_top20",
            "variant_selected_top5",
            "variant_selected_top10",
            "variant_selected_top20",
            "top15_label",
            "bottom15_label",
            "forward_ret_20d",
            "path_value_score_v1",
            "monthly_context",
            "weekly_context",
            "daily_main_state_ctx_backfilled",
            "monthly_main_state_ctx_backfilled",
            "weekly_main_state_ctx_backfilled",
            "shape_classification",
            "family_classification",
            "confirmed",
        ]
    ].copy()
    diff["changed_top5_member"] = diff["baseline_selected_top5"] != diff["variant_selected_top5"]
    diff["changed_top10_member"] = diff["baseline_selected_top10"] != diff["variant_selected_top10"]
    diff["changed_top20_member"] = diff["baseline_selected_top20"] != diff["variant_selected_top20"]
    return diff


def _build_input_resolution(candidate_path: Path, unknown_path: Path, backfill_session: Path, reclass_session: Path, no_lookahead: Path, join_contract: Path, future_candidates: Path, taxonomy: Path, family_breakdown: Path, decision_source: Path) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "candidate_surface_path": str(candidate_path),
        "unknown_reclassification_path": str(unknown_path),
        "backfill_session": str(backfill_session),
        "reclassification_session": str(reclass_session),
        "no_lookahead_context_audit_path": str(no_lookahead),
        "context_join_contract_path": str(join_contract),
        "future_challenger_candidates_path": str(future_candidates),
        "enriched_taxonomy_path": str(taxonomy),
        "enriched_family_breakdown_path": str(family_breakdown),
        "decision_source_path": str(decision_source),
        "resolved_status": "ok",
    }


def _build_manifest(
    *,
    session_id: str,
    output_root: Path,
    candidate_path: Path,
    unknown_path: Path,
    backfill_session: Path,
    reclass_session: Path,
    jobs_requested: int,
    jobs_supported: int,
    variant_rule: str,
    baseline_rows: int,
    variant_rows: int,
) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script": SCRIPT_NAME,
        "family_id": FAMILY_ID,
        "session_id": session_id,
        "created_utc": _utc_now(),
        "output_root": str(output_root),
        "candidate_surface_path": str(candidate_path),
        "unknown_reclassification_path": str(unknown_path),
        "backfill_session": str(backfill_session),
        "reclassification_session": str(reclass_session),
        "jobs_requested": int(jobs_requested),
        "jobs_supported": int(jobs_supported),
        "variant_rule": variant_rule,
        "baseline_rows": int(baseline_rows),
        "variant_rows": int(variant_rows),
        "candidate_generated": True,
        "meemee_reflectable": False,
        "production_ranking_changed": False,
    }


def _build_decision(variant: pd.DataFrame, baseline: pd.DataFrame) -> dict[str, Any]:
    baseline_top5 = baseline[baseline["baseline_selected_top5"]]
    variant_top5 = variant[variant["variant_selected_top5"]]
    baseline_top10 = baseline[baseline["baseline_selected_top10"]]
    variant_top10 = variant[variant["variant_selected_top10"]]

    baseline_top5_path = pd.to_numeric(baseline_top5["path_value_score_v1"], errors="coerce").mean()
    variant_top5_path = pd.to_numeric(variant_top5["path_value_score_v1"], errors="coerce").mean()
    baseline_top10_path = pd.to_numeric(baseline_top10["path_value_score_v1"], errors="coerce").mean()
    variant_top10_path = pd.to_numeric(variant_top10["path_value_score_v1"], errors="coerce").mean()
    baseline_top5_ret = pd.to_numeric(baseline_top5["forward_ret_20d"], errors="coerce").mean()
    variant_top5_ret = pd.to_numeric(variant_top5["forward_ret_20d"], errors="coerce").mean()
    baseline_top10_ret = pd.to_numeric(baseline_top10["forward_ret_20d"], errors="coerce").mean()
    variant_top10_ret = pd.to_numeric(variant_top10["forward_ret_20d"], errors="coerce").mean()
    baseline_bottom15 = int(baseline_top10["bottom15_label"].sum())
    variant_bottom15 = int(variant_top10["bottom15_label"].sum())

    decision = "hold"
    reason = "top5_top10_path_quality_improves_but_bottom15_contamination_worsens_and_false_positive_cost_is_not_yet_conclusive"
    if (
        variant_top5_path <= baseline_top5_path
        or variant_top10_path <= baseline_top10_path
        or variant_top5_ret <= baseline_top5_ret
        or variant_top10_ret <= baseline_top10_ret
    ):
        decision = "drop"
        reason = "top5_top10_path_or_return_did_not_improve_under_confirmation_rule"
    elif variant_bottom15 < baseline_bottom15:
        decision = "keep"
        reason = "top5_top10_quality_improved_and_bottom15_contamination_improved"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "status": decision,
        "reason": reason,
        "promote_ready": False,
        "meemee_reflectable": False,
        "authoritative_artifact": "variant_pool_comparison.json",
        "baseline_top5_mean_forward_ret_20d": _safe_float(baseline_top5_ret),
        "variant_top5_mean_forward_ret_20d": _safe_float(variant_top5_ret),
        "baseline_top10_mean_forward_ret_20d": _safe_float(baseline_top10_ret),
        "variant_top10_mean_forward_ret_20d": _safe_float(variant_top10_ret),
        "baseline_top5_mean_path_value_score_v1": _safe_float(baseline_top5_path),
        "variant_top5_mean_path_value_score_v1": _safe_float(variant_top5_path),
        "baseline_top10_mean_path_value_score_v1": _safe_float(baseline_top10_path),
        "variant_top10_mean_path_value_score_v1": _safe_float(variant_top10_path),
        "baseline_bottom15_count_top10": baseline_bottom15,
        "variant_bottom15_count_top10": variant_bottom15,
    }


def run_observable_regime_false_positive_require_confirmation_v1(
    *,
    candidate_surface_path: str | Path | None = None,
    unknown_reclassification_path: str | Path | None = None,
    backfill_session: str | Path | None = None,
    reclassification_session: str | Path | None = None,
    no_lookahead_context_audit_path: str | Path | None = None,
    context_join_contract_path: str | Path | None = None,
    future_challenger_candidates_path: str | Path | None = None,
    enriched_taxonomy_path: str | Path | None = None,
    enriched_family_breakdown_path: str | Path | None = None,
    decision_source_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    candidate_path = _safe_path(candidate_surface_path, DEFAULT_CANDIDATE_SURFACE)
    unknown_path = _safe_path(unknown_reclassification_path, DEFAULT_UNKNOWN_RECLASSIFICATION)
    backfill_session_path = _safe_path(backfill_session, DEFAULT_BACKFILL_SESSION)
    reclass_session_path = _safe_path(reclassification_session, DEFAULT_RECLASSIFICATION_SESSION)
    no_lookahead_path = _safe_path(no_lookahead_context_audit_path, DEFAULT_NO_LOOKAHEAD_AUDIT)
    join_contract_path = _safe_path(context_join_contract_path, DEFAULT_CONTEXT_JOIN_CONTRACT)
    future_candidates_path = _safe_path(future_challenger_candidates_path, DEFAULT_FUTURE_CHALLENGER_CANDIDATES)
    taxonomy_path = _safe_path(enriched_taxonomy_path, DEFAULT_ENRICHED_TAXONOMY)
    family_breakdown_path = _safe_path(enriched_family_breakdown_path, DEFAULT_ENRICHED_FAMILY_BREAKDOWN)
    decision_source = _safe_path(decision_source_path, DEFAULT_DECISION_SOURCE)
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    if not candidate_path.exists():
        raise FileNotFoundError(candidate_path)
    if not unknown_path.exists():
        raise FileNotFoundError(unknown_path)

    candidate = _load_frame(candidate_path)
    family_rows = _load_frame(unknown_path)
    if limit_anchor_dates is not None:
        selected_dates = sorted(candidate["anchor_date"].dropna().unique().tolist())[: int(limit_anchor_dates)]
        candidate = candidate.loc[candidate["anchor_date"].isin(selected_dates)].copy()
        family_rows = family_rows.loc[family_rows["anchor_date"].isin(selected_dates)].copy()

    if len(candidate) == 0:
        raise ValueError("candidate surface is empty after limit_anchor_dates filtering")

    no_lookahead = _load_json(no_lookahead_path)
    join_contract = _load_json(join_contract_path)
    future_candidates = _load_json(future_candidates_path)
    taxonomy = _load_json(taxonomy_path)
    family_breakdown = _load_json(family_breakdown_path)
    decision_source_json = _load_json(decision_source)

    no_lookahead_status = no_lookahead.get("status") or no_lookahead.get("no_lookahead_audit_status")
    if no_lookahead_status != "pass":
        raise RuntimeError(f"no-lookahead audit did not pass: {no_lookahead_status}")

    joined = _join_candidate_family(candidate, family_rows)
    joined = joined.sort_values(["anchor_date", "side", "score"], ascending=[True, True, False]).reset_index(drop=True)
    joined["confirmed"] = (~joined["is_risk_family"]) | _confirmation_mask(joined)
    joined["confirmation_reason"] = "non_risk"  # default
    joined.loc[joined["is_risk_family"] & joined["confirmed"], "confirmation_reason"] = "shape_family_and_month_week_confirmed"
    joined.loc[joined["is_risk_family"] & ~joined["confirmed"], "confirmation_reason"] = "risk_no_confirmation"
    joined["effective_rank_priority"] = joined["confirmed"].astype(int)
    joined["effective_rank_score"] = joined["score"]

    baseline = _select_baseline(joined)
    variant = _select_topk(joined, confirmed_col="confirmed")

    baseline = baseline.sort_values(["anchor_date", "side", "baseline_group_rank", "symbol"], ascending=[True, True, True, True]).reset_index(drop=True)
    variant = variant.sort_values(["anchor_date", "side", "variant_group_rank", "symbol"], ascending=[True, True, True, True]).reset_index(drop=True)

    merged = baseline.merge(
        variant[
            [
                "anchor_date",
                "symbol",
                "side",
                "rank",
                "variant_group_rank",
                "variant_selected_top5",
                "variant_selected_top10",
                "variant_selected_top20",
                "confirmed",
                "confirmation_reason",
                "effective_rank_priority",
                "effective_rank_score",
            ]
        ],
        on=["anchor_date", "symbol", "side", "rank"],
        how="left",
        suffixes=("_baseline", "_variant"),
    )
    merged["variant_selected_top5"] = merged["variant_selected_top5"].fillna(False).astype(bool)
    merged["variant_selected_top10"] = merged["variant_selected_top10"].fillna(False).astype(bool)
    merged["variant_selected_top20"] = merged["variant_selected_top20"].fillna(False).astype(bool)
    if "confirmed_variant" in merged.columns:
        merged["confirmed"] = merged["confirmed_variant"].fillna(False).astype(bool)
    elif "confirmed" in merged.columns:
        merged["confirmed"] = merged["confirmed"].fillna(False).astype(bool)
    else:
        merged["confirmed"] = False
    if "confirmation_reason_variant" in merged.columns:
        merged["confirmation_reason"] = merged["confirmation_reason_variant"].fillna("missing")
    elif "confirmation_reason" in merged.columns:
        merged["confirmation_reason"] = merged["confirmation_reason"].fillna("missing")
    else:
        merged["confirmation_reason"] = "missing"
    merged["changed_top5_member"] = merged["baseline_selected_top5"] != merged["variant_selected_top5"]
    merged["changed_top10_member"] = merged["baseline_selected_top10"] != merged["variant_selected_top10"]
    merged["changed_top20_member"] = merged["baseline_selected_top20"] != merged["variant_selected_top20"]
    merged["changed_rank"] = merged["baseline_group_rank"] != merged["variant_group_rank"]

    baseline_rows = int(len(baseline))
    variant_rows = int(len(variant))

    pairwise_frame = _load_frame(DEFAULT_ENRICHED_PAIRWISE)
    risk_pairwise = pairwise_frame.loc[pairwise_frame["reclassified_root_cause_code"] == RISK_FAMILY_CODE].copy()
    risk_pairwise_summary = _build_pair_summary(risk_pairwise if not risk_pairwise.empty else pairwise_frame)
    profile = _build_family_profile(merged, risk_pairwise if not risk_pairwise.empty else pairwise_frame, risk_pairwise_summary)
    policy = _build_policy_spec()
    pool = _build_pool_comparison(baseline, variant, merged)
    monthly = _build_monthly_comparison(merged)
    context = _build_context_comparison(merged)
    diff = _build_topk_membership_diff(merged)
    precision = _build_precision_recall_summary(merged)
    false_positive = _build_false_positive_cost_summary(merged)
    decision = _build_decision(variant, baseline)

    session_id = _make_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    run_manifest = _build_manifest(
        session_id=session_id,
        output_root=output_root_path,
        candidate_path=candidate_path,
        unknown_path=unknown_path,
        backfill_session=backfill_session_path,
        reclass_session=reclass_session_path,
        jobs_requested=jobs,
        jobs_supported=1,
        variant_rule="shape_or_family_confirmed_and_month_week_not_overextended",
        baseline_rows=baseline_rows,
        variant_rows=variant_rows,
    )

    input_resolution = _build_input_resolution(
        candidate_path,
        unknown_path,
        backfill_session_path,
        reclass_session_path,
        no_lookahead_path,
        join_contract_path,
        future_candidates_path,
        taxonomy_path,
        family_breakdown_path,
        decision_source,
    )

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "observable_regime_false_positive_profile.json", profile)
    _write_json(session_dir / "confirmation_policy.json", policy)
    _write_json(session_dir / "variant_pool_comparison.json", pool)
    _write_json(session_dir / "monthly_comparison.json", monthly)
    _write_json(session_dir / "context_comparison.json", context)
    _write_json(session_dir / "precision_recall_summary.json", precision)
    _write_json(session_dir / "false_positive_cost_summary.json", false_positive)
    _write_json(session_dir / "observable_regime_false_positive_require_confirmation_v1_decision.json", decision)
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {
        "schema_version": SCHEMA_VERSION,
        "session_id": session_id,
        "artifact_count": 13,
        "row_reconciliation": {
            "baseline_rows": baseline_rows,
            "variant_rows": variant_rows,
            "top5_changed_members_count": int(merged["changed_top5_member"].sum()),
            "top10_changed_members_count": int(merged["changed_top10_member"].sum()),
            "changed_rank_count": int(merged["changed_rank"].sum()),
        },
        "parse_status": {
            "run_manifest": True,
            "input_resolution": True,
            "observable_regime_false_positive_profile": True,
            "confirmation_policy": True,
            "candidate_confirmation_rows": True,
            "variant_pool_comparison": True,
            "monthly_comparison": True,
            "context_comparison": True,
            "topk_membership_diff": True,
            "precision_recall_summary": True,
            "false_positive_cost_summary": True,
            "decision": True,
        },
        "verification_status": "generated",
    })

    _write_parquet(session_dir / "candidate_confirmation_rows.parquet", variant)
    _write_parquet(session_dir / "topk_membership_diff.parquet", diff)

    # optional reference for debugging, but not a decision input
    ref = merged.loc[merged["is_risk_family"]].copy()
    if not ref.empty:
        _write_json(
            session_dir / "risk_rows_reference_summary.json",
            {
                "schema_version": SCHEMA_VERSION + "_risk_reference_v1",
                "count": int(len(ref)),
                "top5_count": int(ref["baseline_selected_top5"].sum()),
                "top10_count": int(ref["baseline_selected_top10"].sum()),
                "top15_count": int(ref["top15_label"].sum()),
                "bottom15_count": int(ref["bottom15_label"].sum()),
                "mean_forward_ret_20d": _safe_float(pd.to_numeric(ref["forward_ret_20d"], errors="coerce").mean()),
                "mean_path_value_score_v1": _safe_float(pd.to_numeric(ref["path_value_score_v1"], errors="coerce").mean()),
            },
        )

    # surface the raw source summary references for traceability
    _write_json(
        session_dir / "source_traceability.json",
        {
            "schema_version": SCHEMA_VERSION + "_traceability_v1",
            "no_lookahead_context_audit_status": no_lookahead_status,
            "context_join_contract_join_mode": join_contract.get("join_mode"),
            "future_challenger_candidate_count": len(future_candidates.get("candidates", [])) if isinstance(future_candidates, dict) else None,
            "taxonomy_family_count": len(family_breakdown.get("family_rows", [])) if isinstance(family_breakdown, dict) else None,
            "decision_source": decision_source_json,
        },
    )

    return {
        "session_dir": str(session_dir),
        "decision": decision["decision"],
        "session_id": session_id,
        "baseline_rows": baseline_rows,
        "variant_rows": variant_rows,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-surface-path", default=None)
    parser.add_argument("--unknown-reclassification-path", default=None)
    parser.add_argument("--backfill-session", default=None)
    parser.add_argument("--reclassification-session", default=None)
    parser.add_argument("--no-lookahead-context-audit-path", default=None)
    parser.add_argument("--context-join-contract-path", default=None)
    parser.add_argument("--future-challenger-candidates-path", default=None)
    parser.add_argument("--enriched-taxonomy-path", default=None)
    parser.add_argument("--enriched-family-breakdown-path", default=None)
    parser.add_argument("--decision-source-path", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    result = run_observable_regime_false_positive_require_confirmation_v1(
        candidate_surface_path=args.candidate_surface_path,
        unknown_reclassification_path=args.unknown_reclassification_path,
        backfill_session=args.backfill_session,
        reclassification_session=args.reclassification_session,
        no_lookahead_context_audit_path=args.no_lookahead_context_audit_path,
        context_join_contract_path=args.context_join_contract_path,
        future_challenger_candidates_path=args.future_challenger_candidates_path,
        enriched_taxonomy_path=args.enriched_taxonomy_path,
        enriched_family_breakdown_path=args.enriched_family_breakdown_path,
        decision_source_path=args.decision_source_path,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
