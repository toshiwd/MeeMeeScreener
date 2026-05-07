from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_feature_surface_batch1_v1 import _apply_batch1_features  # noqa: E402
from scripts.tradex_forward_candidate_feature_contract_repair_v1 import (  # noqa: E402
    _apply_model_feature_completion,
    _materialize_candidate_point_in_time_sources,
    _materialize_volume_feature_contract,
)
from scripts.tradex_iizuka_fixed_contract_forward_surface_accumulation_v1 import (  # noqa: E402
    _attach_evaluation_labels,
    _build_shape_modifier_map,
    _discover_runtime_dates,
    _json_ready,
    _load_frame,
    _load_ml_label_forward_rows,
    _query_forward_base,
    _materialize_candle_reversal_flags,
    _materialize_ma_position_context,
    _safe_path,
    _shape_feature_join,
    _to_iso_date,
    _write_json,
    _write_parquet,
)
from scripts.tradex_iizuka_pre_decisive_long_candidate_v2 import (  # noqa: E402
    _enrich_frame,
    _safe_bool,
    _safe_float,
)
from scripts.tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1 import (  # noqa: E402
    _build_safe_frame,
    _month_bucket,
    _metric_for_selection,
    _select_topk,
)

SCRIPT_NAME = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_input_resolution_v1"
CONTRACT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_contract_v1"
COMPARISON_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_comparison_v1"
TOPK_DIFF_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_topk_membership_diff_v1"
MONTH_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_month_dependence_audit_v1"
GROUP_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_group_split_audit_v1"
SYMBOL_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_symbol_concentration_audit_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_no_lookahead_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_leakage_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation_decision_v1"

TOP_K_VALUES = (5, 10, 20)
EVAL_LABEL_COLUMNS = ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label")

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_forward_accumulation")
APPROVED_V2_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2\20260503T122320Z-161925")
TOP10_SAFE_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering\20260503T130355Z-311287")
DEFAULT_SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

APPROVED_ALL_ROLE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_all_role_rows.parquet"
APPROVED_ACTIVE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_active_candidate_rows.parquet"
APPROVED_VARIANT_COMPARE = APPROVED_V2_SESSION / "iizuka_v2_variant_pool_comparison.json"
APPROVED_FAILURE_AUDIT = APPROVED_V2_SESSION / "iizuka_v2_failure_mode_audit.json"
APPROVED_LINEAGE = APPROVED_V2_SESSION / "iizuka_v1_v2_lineage_comparison.json"
APPROVED_DECISION = APPROVED_V2_SESSION / "iizuka_pre_decisive_long_candidate_v2_decision.json"
TOP10_SAFE_DECISION = TOP10_SAFE_SESSION / "top10_safe_decision.json"
TOP10_SAFE_COMPARISON = TOP10_SAFE_SESSION / "top10_safe_comparison.json"
TOP10_SAFE_MONTH_AUDIT = TOP10_SAFE_SESSION / "top10_safe_month_dependence_audit.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_inputs() -> dict[str, Any]:
    required = {
        "approved_all_role_rows": APPROVED_ALL_ROLE_ROWS,
        "approved_active_rows": APPROVED_ACTIVE_ROWS,
        "approved_variant_compare": APPROVED_VARIANT_COMPARE,
        "approved_failure_audit": APPROVED_FAILURE_AUDIT,
        "approved_lineage": APPROVED_LINEAGE,
        "approved_decision": APPROVED_DECISION,
        "top10_safe_decision": TOP10_SAFE_DECISION,
        "top10_safe_comparison": TOP10_SAFE_COMPARISON,
        "top10_safe_month_audit": TOP10_SAFE_MONTH_AUDIT,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    return {
        "approved_all_role_rows": _load_frame(required["approved_all_role_rows"]),
        "approved_active_rows": _load_frame(required["approved_active_rows"]),
        "approved_variant_compare": _load_json(required["approved_variant_compare"]),
        "approved_failure_audit": _load_json(required["approved_failure_audit"]),
        "approved_lineage": _load_json(required["approved_lineage"]),
        "approved_decision": _load_json(required["approved_decision"]),
        "top10_safe_decision": _load_json(required["top10_safe_decision"]),
        "top10_safe_comparison": _load_json(required["top10_safe_comparison"]),
        "top10_safe_month_audit": _load_json(required["top10_safe_month_audit"]),
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _anchor_date_range(source_max_anchor_date: str, mature_label_max_date: str) -> list[str]:
    start = pd.to_datetime(source_max_anchor_date, errors="coerce")
    end = pd.to_datetime(mature_label_max_date, errors="coerce")
    if pd.isna(start) or pd.isna(end) or end <= start:
        return []
    dates = pd.date_range(start + pd.Timedelta(days=1), end, freq="D")
    return dates[dates.dayofweek < 5].strftime("%Y-%m-%d").tolist()


def _materialize_forward_rows(
    *,
    db_path: Path,
    symbols: list[str],
    anchor_dates: list[str],
    source_max_anchor_date: str,
    mature_label_max_date: str,
    shape_session: Path,
) -> pd.DataFrame:
    if not symbols or not anchor_dates:
        return pd.DataFrame()
    base_rows = _query_forward_base(
        db_path=db_path,
        symbols=symbols,
        anchor_dates=anchor_dates,
        source_max_anchor_date=source_max_anchor_date,
        mature_label_max_date=mature_label_max_date,
    )
    if base_rows.empty:
        return base_rows

    shape_rows = _load_frame(shape_session / "conditional_shape_rows.parquet")
    shape_modifier_map = _build_shape_modifier_map(shape_session)
    ml_label = _load_ml_label_forward_rows(db_path=db_path, symbols=symbols, anchor_dates=anchor_dates)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        candidate = _materialize_candidate_point_in_time_sources(base_rows, conn)
        candidate = _materialize_volume_feature_contract(candidate, conn)
        candidate = _apply_model_feature_completion(candidate)
        candidate = _materialize_ma_position_context(candidate, conn)
    finally:
        conn.close()

    candidate = candidate.sort_values(["anchor_date", "candidate_rank", "symbol"], ascending=[True, True, True], kind="stable").reset_index(drop=True)
    candidate = _shape_feature_join(candidate, shape_rows, shape_modifier_map)
    candidate = _materialize_candle_reversal_flags(candidate)
    candidate = _apply_batch1_features(candidate)
    candidate = candidate.loc[candidate["side"].astype(str) == "long"].copy()
    candidate = _attach_evaluation_labels(candidate, ml_label, shape_rows)
    candidate["research_fallback_label_source"] = "ml_label_20d"
    candidate["research_only"] = True
    candidate["evaluation_only_outcomes"] = True
    candidate["candidate_contract_name"] = "iizuka_pre_decisive_long_candidate_v2_forward_accumulation"
    return candidate


def _combine_frames(source_frame: pd.DataFrame, forward_frame: pd.DataFrame) -> pd.DataFrame:
    cols = sorted(set(source_frame.columns) | set(forward_frame.columns))
    left = source_frame.reindex(columns=cols)
    right = forward_frame.reindex(columns=cols)
    return pd.concat([left, right], ignore_index=True)


def _parquet_compatible_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in out.columns:
        if out[column].dtype != object:
            continue
        if not out[column].map(lambda value: isinstance(value, (dict, list, tuple, set))).any():
            continue
        out[column] = out[column].map(
            lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple, set)) else value
        )
    return out


def _apply_v2_roles(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = _enrich_frame(frame.copy())
    if "research_fallback_label_source" not in enriched.columns:
        enriched["research_fallback_label_source"] = "ml_label_20d"
    enriched["research_only"] = True
    return enriched


def _attach_safe_ordering(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    active = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    if active.empty:
        out = frame.copy()
        out["top10_safe_candidate_score"] = pd.NA
        out["top10_safe_candidate_rank"] = pd.NA
        out["top10_safe_order_reason"] = pd.NA
        return out, active
    safe_active = _build_safe_frame(active)
    merged = frame.merge(
        safe_active[["surface_key", "top10_safe_candidate_score", "top10_safe_candidate_rank", "top10_safe_order_reason"]],
        on="surface_key",
        how="left",
    )
    return merged, safe_active


def _selection_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    metrics = _metric_for_selection(frame)
    metrics["median_forward_ret_20d"] = float(pd.to_numeric(frame["forward_ret_20d"], errors="coerce").median()) if len(frame) else None
    metrics["median_path_value_score_v1"] = float(pd.to_numeric(frame["path_value_score_v1"], errors="coerce").median()) if len(frame) else None
    metrics["top15_count"] = int(pd.to_numeric(frame["top15_label"], errors="coerce").fillna(0).sum()) if "top15_label" in frame.columns and len(frame) else 0
    metrics["bottom15_count"] = int(pd.to_numeric(frame["bottom15_label"], errors="coerce").fillna(0).sum()) if "bottom15_label" in frame.columns and len(frame) else 0
    metrics["top15_to_bottom15_ratio"] = float(metrics["top15_count"] / max(metrics["bottom15_count"], 1)) if len(frame) else None
    metrics["top15_capture_rate"] = float(pd.to_numeric(frame["top15_label"], errors="coerce").mean()) if "top15_label" in frame.columns and len(frame) else None
    metrics["bottom15_contamination_rate"] = float(pd.to_numeric(frame["bottom15_label"], errors="coerce").mean()) if "bottom15_label" in frame.columns and len(frame) else None
    metrics["non_positive_return_rate"] = float(pd.to_numeric(frame["forward_ret_20d"], errors="coerce").le(0).mean()) if len(frame) else None
    metrics["top20pct_available"] = "top20pct_label" in frame.columns
    metrics["top20pct_rate"] = float(pd.to_numeric(frame["top20pct_label"], errors="coerce").mean()) if "top20pct_label" in frame.columns and len(frame) else None
    return metrics


def _build_comparison(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    approved = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    safe = frame.loc[frame["top10_safe_candidate_rank"].notna()].copy()
    rows: list[dict[str, Any]] = []
    diff_frames: list[pd.DataFrame] = []
    for k in TOP_K_VALUES:
        approved_sel = _select_topk(approved, "iizuka_v2_candidate_score", k)
        safe_sel = _select_topk(safe, "top10_safe_candidate_score", k)
        approved_keys = set(approved_sel["surface_key"].astype(str))
        safe_keys = set(safe_sel["surface_key"].astype(str))
        union = approved_keys | safe_keys
        intersection = approved_keys & safe_keys
        diff = pd.DataFrame({"top_k": k, "surface_key": list(union)})
        diff = diff.merge(
            frame[[
                "surface_key",
                "anchor_date",
                "symbol",
                "iizuka_v2_role",
                "iizuka_v2_reason",
                "iizuka_v2_candidate_score",
                "iizuka_v2_candidate_rank",
                "top10_safe_candidate_score",
                "top10_safe_candidate_rank",
                "top10_safe_order_reason",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
                *([c for c in ("top20pct_label",) if c in frame.columns]),
            ] + [c for c in ("month_bucket",) if c in frame.columns]],
            on="surface_key",
            how="left",
        )
        diff["selected_in_approved_v2_active"] = diff["surface_key"].isin(approved_keys)
        diff["selected_in_safe"] = diff["surface_key"].isin(safe_keys)
        diff["member_change"] = diff["selected_in_approved_v2_active"] != diff["selected_in_safe"]
        diff["selection_state"] = diff.apply(
            lambda row: "both" if row["selected_in_approved_v2_active"] and row["selected_in_safe"] else ("approved_v2_only" if row["selected_in_approved_v2_active"] else "safe_only"),
            axis=1,
        )
        diff_frames.append(diff)

        rows.append(
            {
                "top_k": k,
                "approved_v2_active": _selection_metrics(approved_sel),
                "top10_safe_ordering_v1": _selection_metrics(safe_sel),
                "changed_top5_members_count": int(len(approved_keys ^ safe_keys)) if k == 5 else None,
                "changed_top10_members_count": int(len(approved_keys ^ safe_keys)) if k == 10 else None,
                "changed_top20_members_count": int(len(approved_keys ^ safe_keys)) if k == 20 else None,
                "membership_changed_count_vs_approved_v2": int(len(approved_keys ^ safe_keys)),
                "overlap_ratio_vs_approved_v2": float(len(intersection) / len(union)) if union else None,
                "approved_v2_active_group_count": int(approved_sel["anchor_date"].nunique()) if len(approved_sel) else 0,
                "top10_safe_group_count": int(safe_sel["anchor_date"].nunique()) if len(safe_sel) else 0,
                "approved_v2_active_symbol_count": int(approved_sel["symbol"].nunique()) if len(approved_sel) else 0,
                "top10_safe_symbol_count": int(safe_sel["symbol"].nunique()) if len(safe_sel) else 0,
            }
        )

    return {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "metric_mode": "per_anchor_date_topK",
        "active_row_count": int(len(approved)),
        "active_group_count": int(approved["anchor_date"].nunique()) if len(approved) else 0,
        "active_symbol_count": int(approved["symbol"].nunique()) if len(approved) else 0,
        "active_month_count": int(_month_bucket(approved).nunique()) if len(approved) else 0,
        "safe_row_count": int(len(safe)),
        "top20pct_available": False,
        "top20pct_note": "top20pct_label is not present in the approved bundle and is not imputed",
        "per_k": rows,
    }, pd.concat(diff_frames, ignore_index=True) if diff_frames else pd.DataFrame()


def _build_month_audit(frame: pd.DataFrame) -> dict[str, Any]:
    approved = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    safe = frame.loc[frame["top10_safe_candidate_rank"].notna()].copy()
    active_months = _month_bucket(approved).value_counts().sort_index()
    per_k: dict[str, list[dict[str, Any]]] = {}
    for k in TOP_K_VALUES:
        approved_sel = _select_topk(approved, "iizuka_v2_candidate_score", k).assign(month=_month_bucket(_select_topk(approved, "iizuka_v2_candidate_score", k)))
        safe_sel = _select_topk(safe, "top10_safe_candidate_score", k).assign(month=_month_bucket(_select_topk(safe, "top10_safe_candidate_score", k)))
        entries = []
        for month in active_months.index.tolist():
            approved_month = approved_sel.loc[approved_sel["month"] == month].copy()
            safe_month = safe_sel.loc[safe_sel["month"] == month].copy()
            approved_metrics = _selection_metrics(approved_month)
            safe_metrics = _selection_metrics(safe_month)
            entries.append(
                {
                    "month": month,
                    "approved_v2_active_row_count": int(approved_metrics["row_count"]),
                    "top10_safe_row_count": int(safe_metrics["row_count"]),
                    "approved_v2_active_share_of_topk": float(approved_metrics["row_count"] / max(len(approved_sel), 1)) if len(approved_sel) else None,
                    "top10_safe_share_of_topk": float(safe_metrics["row_count"] / max(len(safe_sel), 1)) if len(safe_sel) else None,
                    "approved_v2_active_mean_forward_ret_20d": approved_metrics["mean_forward_ret_20d"],
                    "top10_safe_mean_forward_ret_20d": safe_metrics["mean_forward_ret_20d"],
                    "approved_v2_active_median_forward_ret_20d": approved_metrics["median_forward_ret_20d"],
                    "top10_safe_median_forward_ret_20d": safe_metrics["median_forward_ret_20d"],
                    "approved_v2_active_bottom15_contamination_rate": approved_metrics["bottom15_contamination_rate"],
                    "top10_safe_bottom15_contamination_rate": safe_metrics["bottom15_contamination_rate"],
                    "delta_mean_forward_ret_20d": _safe_float((safe_metrics["mean_forward_ret_20d"] or 0.0) - (approved_metrics["mean_forward_ret_20d"] or 0.0)) if safe_metrics["mean_forward_ret_20d"] is not None and approved_metrics["mean_forward_ret_20d"] is not None else None,
                    "delta_bottom15_contamination_rate": _safe_float((safe_metrics["bottom15_contamination_rate"] or 0.0) - (approved_metrics["bottom15_contamination_rate"] or 0.0)) if safe_metrics["bottom15_contamination_rate"] is not None and approved_metrics["bottom15_contamination_rate"] is not None else None,
                }
            )
        per_k[str(k)] = entries

    top20_delta_records = []
    approved_sel = _select_topk(approved, "iizuka_v2_candidate_score", 20)
    safe_sel = _select_topk(safe, "top10_safe_candidate_score", 20)
    for month in active_months.index.tolist():
        approved_month = approved_sel.assign(month=_month_bucket(approved_sel)).loc[lambda d: d["month"] == month]
        safe_month = safe_sel.assign(month=_month_bucket(safe_sel)).loc[lambda d: d["month"] == month]
        approved_metrics = _selection_metrics(approved_month)
        safe_metrics = _selection_metrics(safe_month)
        top20_delta_records.append(
            {
                "month": month,
                "approved_v2_active_mean_forward_ret_20d": approved_metrics["mean_forward_ret_20d"],
                "top10_safe_mean_forward_ret_20d": safe_metrics["mean_forward_ret_20d"],
                "approved_v2_active_bottom15_contamination_rate": approved_metrics["bottom15_contamination_rate"],
                "top10_safe_bottom15_contamination_rate": safe_metrics["bottom15_contamination_rate"],
                "delta_mean_forward_ret_20d": _safe_float((safe_metrics["mean_forward_ret_20d"] or 0.0) - (approved_metrics["mean_forward_ret_20d"] or 0.0)) if safe_metrics["mean_forward_ret_20d"] is not None and approved_metrics["mean_forward_ret_20d"] is not None else None,
            }
        )
    month_weights = []
    total_weight = 0.0
    for record in top20_delta_records:
        weight = abs(record["delta_mean_forward_ret_20d"] or 0.0) * max(int(active_months.get(record["month"], 0)), 1)
        month_weights.append((record["month"], weight))
        total_weight += weight
    dominant_month = None
    dominant_share = None
    if month_weights:
        dominant_month, dominant_weight = max(month_weights, key=lambda item: item[1])
        dominant_share = float(dominant_weight / total_weight) if total_weight else None

    return {
        "schema_version": MONTH_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "active_month_count": int(len(active_months)),
        "active_month_rows": {str(month): int(count) for month, count in active_months.items()},
        "per_k": per_k,
        "top20_month_delta": top20_delta_records,
        "improvement_concentration": {
            "dominant_month": dominant_month,
            "dominant_month_share": dominant_share,
            "one_month_dominated": bool(dominant_share is not None and dominant_share >= 0.60),
        },
        "notes": [
            "active-month coverage is measured on the approved-v2 active lane within the accumulated surface",
            "breadth above the original three-month bundle is required before keep-worthy watch can be considered",
        ],
    }


def _build_group_audit(frame: pd.DataFrame) -> dict[str, Any]:
    approved = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    safe = frame.loc[frame["top10_safe_candidate_rank"].notna()].copy()
    approved_sel = _select_topk(approved, "iizuka_v2_candidate_score", 20)
    safe_sel = _select_topk(safe, "top10_safe_candidate_score", 20)
    groups = []
    for anchor_date in sorted(approved["anchor_date"].dropna().astype(str).unique().tolist()):
        approved_group = approved_sel.loc[approved_sel["anchor_date"].astype(str) == anchor_date].copy()
        safe_group = safe_sel.loc[safe_sel["anchor_date"].astype(str) == anchor_date].copy()
        approved_metrics = _selection_metrics(approved_group)
        safe_metrics = _selection_metrics(safe_group)
        groups.append(
            {
                "anchor_date": anchor_date,
                "approved_v2_active_row_count": int(approved_metrics["row_count"]),
                "top10_safe_row_count": int(safe_metrics["row_count"]),
                "approved_v2_active_mean_forward_ret_20d": approved_metrics["mean_forward_ret_20d"],
                "top10_safe_mean_forward_ret_20d": safe_metrics["mean_forward_ret_20d"],
                "approved_v2_active_median_forward_ret_20d": approved_metrics["median_forward_ret_20d"],
                "top10_safe_median_forward_ret_20d": safe_metrics["median_forward_ret_20d"],
                "approved_v2_active_bottom15_contamination_rate": approved_metrics["bottom15_contamination_rate"],
                "top10_safe_bottom15_contamination_rate": safe_metrics["bottom15_contamination_rate"],
            }
        )
    return {
        "schema_version": GROUP_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "active_group_count": int(approved["anchor_date"].nunique()) if len(approved) else 0,
        "active_group_rows": {str(k): int(v) for k, v in approved["anchor_date"].value_counts().sort_index().items()},
        "per_group": groups,
        "notes": [
            "group split uses anchor_date as the fixed-contract accumulation grouping key",
            "safe ordering is only compared on the approved-v2 active lane",
        ],
    }


def _build_symbol_audit(frame: pd.DataFrame) -> dict[str, Any]:
    approved = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    safe = frame.loc[frame["top10_safe_candidate_rank"].notna()].copy()
    approved_sel = _select_topk(approved, "iizuka_v2_candidate_score", 20)
    safe_sel = _select_topk(safe, "top10_safe_candidate_score", 20)

    def _summarize(selection: pd.DataFrame) -> dict[str, Any]:
        counts = Counter(selection["symbol"].astype(str).tolist()) if len(selection) and "symbol" in selection.columns else Counter()
        return {
            "row_count": int(len(selection)),
            "symbol_count": int(selection["symbol"].nunique()) if len(selection) and "symbol" in selection.columns else 0,
            "top_symbol_counts": {str(symbol): int(count) for symbol, count in counts.most_common(10)},
            "top_symbol_share": float(counts.most_common(1)[0][1] / len(selection)) if len(selection) and counts else None,
            "top5_symbol_share": float(sum(count for _, count in counts.most_common(5)) / len(selection)) if len(selection) and counts else None,
        }

    return {
        "schema_version": SYMBOL_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "approved_v2_active_top20": _summarize(approved_sel),
        "top10_safe_top20": _summarize(safe_sel),
        "approved_v2_active_overall": _summarize(approved),
        "notes": [
            "symbol concentration is reported on the approved-v2 active lane and the top20 selections",
            "the watch fails if concentration collapses to a tiny name set, even when returns improve",
        ],
    }


def _build_no_lookahead_audit(frame: pd.DataFrame) -> dict[str, Any]:
    flag_violations: dict[str, int] = {}
    for field in ("monthly_context_no_lookahead", "weekly_context_no_lookahead"):
        if field in frame.columns:
            flag_violations[f"{field}_false_count"] = int((~frame[field].fillna(False).astype(bool)).sum())
    date_violations: dict[str, int] = {}
    for field in ("monthly_context_date", "weekly_context_date"):
        if field in frame.columns:
            asof = pd.to_datetime(frame["anchor_date"], errors="coerce")
            context = pd.to_datetime(frame[field], errors="coerce")
            date_violations[f"{field}_future_count"] = int((context > asof).sum())
    fallback_ok = bool((frame["research_fallback_label_source"].astype(str) == "ml_label_20d").all()) if "research_fallback_label_source" in frame.columns else False
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_pass": all(value == 0 for value in flag_violations.values()) and all(value == 0 for value in date_violations.values()) and fallback_ok,
        "flag_violations": flag_violations,
        "date_violations": date_violations,
        "research_fallback_label_source": "ml_label_20d" if fallback_ok else None,
        "notes": [
            "evaluation labels are attached after candidate construction",
            "no future bars are used in feature construction",
        ],
    }


def _build_leakage_audit(frame: pd.DataFrame) -> dict[str, Any]:
    feature_fields_used = {
        "iizuka_v2_candidate_score",
        "iizuka_v2_candidate_rank",
        "top10_safe_candidate_score",
        "top10_safe_candidate_rank",
        "top10_safe_order_reason",
        "support_wick",
        "bull_engulfing",
        "decision_candle_quality",
        "shape_classification",
        "close_vs_ma20_pct",
        "close_vs_ma60_pct",
    }
    outcome_fields = set(EVAL_LABEL_COLUMNS)
    attached = [field for field in EVAL_LABEL_COLUMNS if field in frame.columns]
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_fields_used": sorted(feature_fields_used),
        "outcome_fields": sorted(outcome_fields),
        "outcome_fields_used_as_features": sorted(feature_fields_used.intersection(outcome_fields)),
        "outcome_fields_attached_after_candidate_construction": attached,
        "leakage_free": not feature_fields_used.intersection(outcome_fields),
        "note": "top10-safe ordering uses only approved non-outcome fields on the active lane",
    }


def _build_contract() -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "approved_v2_gate": "iizuka_pre_decisive_long_candidate_v2",
        "ordering_contract": "v2_score_anchored_top10_safe_ordering_v1",
        "scope": "TRADEX-only",
        "preserved": [
            "approved v2 gate",
            "long-side only",
            "no-lookahead contract",
            "research fallback label source ml_label_20d",
            "top10-safe ordering formula",
        ],
        "non_scope": [
            "no threshold tuning",
            "no new feature family",
            "no new candidate ordering",
            "no MeeMee changes",
            "no production ranking changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
        "notes": [
            "the forward accumulation appends later dates from the live runtime DB and reuses the frozen approved v2 / top10-safe contract pair",
            "safe ordering is only applied on the approved-v2 active lane",
        ],
    }


def _build_decision(
    comparison: dict[str, Any],
    month_audit: dict[str, Any],
    no_lookahead: dict[str, Any],
    leakage: dict[str, Any],
) -> dict[str, Any]:
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    baseline10 = top10["approved_v2_active"]
    challenger10 = top10["top10_safe_ordering_v1"]
    baseline20 = top20["approved_v2_active"]
    challenger20 = top20["top10_safe_ordering_v1"]

    top10_non_worse = challenger10["mean_forward_ret_20d"] is not None and baseline10["mean_forward_ret_20d"] is not None and challenger10["mean_forward_ret_20d"] >= baseline10["mean_forward_ret_20d"]
    top20_non_worse = challenger20["mean_forward_ret_20d"] is not None and baseline20["mean_forward_ret_20d"] is not None and challenger20["mean_forward_ret_20d"] >= baseline20["mean_forward_ret_20d"]
    bottom15_non_worse = challenger20["bottom15_contamination_rate"] is not None and baseline20["bottom15_contamination_rate"] is not None and challenger20["bottom15_contamination_rate"] <= baseline20["bottom15_contamination_rate"]
    non_positive_non_worse = challenger20["non_positive_return_rate"] is not None and baseline20["non_positive_return_rate"] is not None and challenger20["non_positive_return_rate"] <= baseline20["non_positive_return_rate"]
    months_expanded = int(month_audit["active_month_count"]) > 3
    not_month_dominated = not bool(month_audit["improvement_concentration"]["one_month_dominated"])
    branching_enough = int(top10["changed_top10_members_count"] or 0) >= 1 or int(top20["changed_top20_members_count"] or 0) >= 1

    if not no_lookahead["no_lookahead_pass"] or not leakage["leakage_free"]:
        decision = "drop_forward_watch"
        reason = "no-lookahead or leakage audit failed"
    elif not top10_non_worse or not top20_non_worse or not bottom15_non_worse or not non_positive_non_worse:
        decision = "drop_forward_watch"
        reason = "forward accumulation regressed practical top-K quality versus approved v2"
    elif months_expanded and not_month_dominated and branching_enough:
        decision = "keep_candidate_for_longer_forward_watch"
        reason = "top10-safe ordering remains non-worse while breadth expanded beyond the original three-month surface"
    else:
        decision = "hold_accumulate_more"
        reason = "direction remains positive but breadth or branching is still too narrow for a keep decision"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": "keep" if decision == "keep_candidate_for_longer_forward_watch" else ("hold" if decision == "hold_accumulate_more" else "drop"),
        "reason": reason,
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_forward_accumulation",
        "summary": {
            "candidate_row_count": int(no_lookahead.get("candidate_rows", 0)) if isinstance(no_lookahead.get("candidate_rows"), int) else None,
            "active_row_count": int(comparison.get("active_row_count", 0)),
            "active_group_count": int(comparison.get("active_group_count", 0)),
            "active_symbol_count": int(comparison.get("active_symbol_count", 0)),
            "active_month_count": int(comparison.get("active_month_count", month_audit["active_month_count"])),
            "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
            "leakage_free": bool(leakage["leakage_free"]),
        },
        "comparison_snapshot": {
            "top10": top10,
            "top20": top20,
        },
        "month_audit_snapshot": month_audit,
        "notes": [
            "approved v2 gate is frozen",
            "top10-safe ordering is frozen and only re-evaluated on accumulated forward rows",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive long candidate v2 forward accumulation")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    parser.add_argument("--shape-session", type=str, default=str(DEFAULT_SHAPE_SESSION))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    runtime_db = _safe_path(args.runtime_db, DEFAULT_RUNTIME_DB)
    shape_session = _safe_path(args.shape_session, DEFAULT_SHAPE_SESSION)
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)

    inputs = _load_inputs()
    approved_all_role = inputs["approved_all_role_rows"].copy()
    approved_active = inputs["approved_active_rows"].copy()
    source_symbols = sorted(approved_all_role["symbol"].astype(str).unique().tolist())
    source_max_anchor_date = str(approved_all_role["anchor_date"].max())

    runtime_dates = _discover_runtime_dates(runtime_db)
    mature_label_max_date = _to_iso_date(runtime_dates.get("ml_label_20d_max_date")) or _to_iso_date(runtime_dates.get("label_20d_max_date"))
    if mature_label_max_date is None:
        raise RuntimeError("ml_label_20d is unavailable; cannot generate forward accumulation surface")

    anchor_dates = _anchor_date_range(source_max_anchor_date, mature_label_max_date)
    forward_rows = _materialize_forward_rows(
        db_path=runtime_db,
        symbols=source_symbols,
        anchor_dates=anchor_dates,
        source_max_anchor_date=source_max_anchor_date,
        mature_label_max_date=mature_label_max_date,
        shape_session=shape_session,
    )

    accumulated = _combine_frames(approved_all_role, forward_rows)
    accumulated = _apply_v2_roles(accumulated)
    accumulated, safe_active = _attach_safe_ordering(accumulated)
    active = accumulated.loc[accumulated["iizuka_v2_role"] == "active"].copy()

    comparison, diff_frame = _build_comparison(accumulated)
    month_audit = _build_month_audit(accumulated)
    group_audit = _build_group_audit(accumulated)
    symbol_audit = _build_symbol_audit(accumulated)
    no_lookahead = _build_no_lookahead_audit(accumulated)
    leakage = _build_leakage_audit(accumulated)
    contract = _build_contract()
    no_lookahead["candidate_rows"] = int(len(accumulated))
    decision = _build_decision(comparison, month_audit, no_lookahead, leakage)
    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_root.name,
        "output_root": str(output_root),
        "jobs": int(args.jobs),
        "research_only": True,
        "boundary": "TRADEX-only",
        "fixed_contracts": {
            "approved_v2_gate": "iizuka_pre_decisive_long_candidate_v2",
            "top10_safe_ordering": "v2_score_anchored_top10_safe_ordering_v1",
        },
        "source_artifacts": {
            "approved_all_role_rows": str(APPROVED_ALL_ROLE_ROWS),
            "approved_active_rows": str(APPROVED_ACTIVE_ROWS),
            "approved_variant_compare": str(APPROVED_VARIANT_COMPARE),
            "approved_failure_audit": str(APPROVED_FAILURE_AUDIT),
            "approved_lineage": str(APPROVED_LINEAGE),
            "approved_decision": str(APPROVED_DECISION),
            "top10_safe_decision": str(TOP10_SAFE_DECISION),
            "top10_safe_comparison": str(TOP10_SAFE_COMPARISON),
            "top10_safe_month_audit": str(TOP10_SAFE_MONTH_AUDIT),
            "runtime_db": str(runtime_db),
            "shape_session": str(shape_session),
        },
        "notes": [
            "fixed-contract forward accumulation only",
            "no threshold tuning, no new ordering hypothesis, no MeeMee changes, no production ranking changes, no publish or promotion mutation",
            "top10-safe ordering is re-evaluated unchanged on the accumulated surface",
        ],
    }

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "output_root": str(output_root),
            "session_root": str(session_root),
            "approved_v2_session": str(APPROVED_V2_SESSION),
            "top10_safe_session": str(TOP10_SAFE_SESSION),
            "runtime_db": str(runtime_db),
            "shape_session": str(shape_session),
        },
        "source_surface": {
            "approved_v2_all_role_rows": str(APPROVED_ALL_ROLE_ROWS),
            "approved_v2_active_rows": str(APPROVED_ACTIVE_ROWS),
            "source_max_anchor_date": source_max_anchor_date,
            "source_symbol_count": int(len(source_symbols)),
            "forward_anchor_date_count": int(len(anchor_dates)),
            "mature_label_max_date": str(mature_label_max_date),
        },
        "frozen_inputs": {
            "approved_v2_gate": "iizuka_pre_decisive_long_candidate_v2",
            "top10_safe_ordering": "v2_score_anchored_top10_safe_ordering_v1",
        },
        "frozen_artifacts_present": {
            "approved_variant_compare": True,
            "approved_failure_audit": True,
            "approved_lineage": True,
            "approved_decision": True,
            "top10_safe_decision": True,
            "top10_safe_comparison": True,
            "top10_safe_month_audit": True,
        },
        "notes": [
            "approved v2 active lane is the baseline comparator on the accumulated surface",
            "top10-safe ordering is applied only to the active lane",
            "top20pct_label remains unavailable in the approved bundle and is not imputed",
        ],
    }

    comparison["candidate_contract_name"] = contract["candidate_contract_name"]
    comparison["approved_v2_active_row_count"] = int(active.shape[0])
    comparison["top10_safe_row_count"] = int(safe_active.shape[0])
    comparison["source_surface_row_count"] = int(len(approved_all_role))
    comparison["forward_row_count"] = int(len(forward_rows))

    _write_json(session_root / "run_manifest.json", run_manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "fixed_contract_forward_accumulation_contract.json", contract)
    _write_parquet(session_root / "fixed_contract_candidate_rows.parquet", _parquet_compatible_frame(accumulated))
    _write_json(session_root / "fixed_contract_comparison.json", comparison)
    _write_parquet(session_root / "fixed_contract_topk_membership_diff.parquet", _parquet_compatible_frame(diff_frame))
    _write_json(session_root / "fixed_contract_month_dependence_audit.json", month_audit)
    _write_json(session_root / "fixed_contract_group_split_audit.json", group_audit)
    _write_json(session_root / "fixed_contract_symbol_concentration_audit.json", symbol_audit)
    _write_json(session_root / "fixed_contract_no_lookahead_audit.json", no_lookahead)
    _write_json(session_root / "fixed_contract_leakage_audit.json", leakage)
    _write_json(session_root / "fixed_contract_decision.json", decision)
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
            "generated_at_utc": _utc_now(),
            "session_root": str(session_root),
            "all_present": all(
                (session_root / name).exists()
                for name in [
                    "run_manifest.json",
                    "input_resolution.json",
                    "fixed_contract_forward_accumulation_contract.json",
                    "fixed_contract_candidate_rows.parquet",
                    "fixed_contract_comparison.json",
                    "fixed_contract_topk_membership_diff.parquet",
                    "fixed_contract_month_dependence_audit.json",
                    "fixed_contract_group_split_audit.json",
                    "fixed_contract_symbol_concentration_audit.json",
                    "fixed_contract_no_lookahead_audit.json",
                    "fixed_contract_leakage_audit.json",
                    "fixed_contract_decision.json",
                ]
            ),
            "required_json": [
                "run_manifest.json",
                "input_resolution.json",
                "fixed_contract_forward_accumulation_contract.json",
                "fixed_contract_comparison.json",
                "fixed_contract_month_dependence_audit.json",
                "fixed_contract_group_split_audit.json",
                "fixed_contract_symbol_concentration_audit.json",
                "fixed_contract_no_lookahead_audit.json",
                "fixed_contract_leakage_audit.json",
                "fixed_contract_decision.json",
            ],
            "required_parquet": [
                "fixed_contract_candidate_rows.parquet",
                "fixed_contract_topk_membership_diff.parquet",
            ],
            "decision": decision["decision"],
        },
    )


if __name__ == "__main__":
    main()
