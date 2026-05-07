from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_iizuka_pre_decisive_long_candidate_v2 import (  # noqa: E402
    _json_ready,
    _load_frame,
    _load_json,
    _metric_bundle,
    _safe_bool,
    _safe_float,
    _write_json,
    _write_parquet,
)

SCRIPT_NAME = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_input_resolution_v1"
CONTRACT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_contract_v1"
COMPARISON_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_comparison_v1"
MONTH_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_month_audit_v1"
FAILURE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_failure_mode_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_no_lookahead_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_leakage_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design_decision_v1"

TOP_K_VALUES = (5, 10, 20)
EVAL_LABEL_COLUMNS = ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label")

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_challenger_design")
APPROVED_V2_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2\20260503T122320Z-161925")

APPROVED_ACTIVE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_active_candidate_rows.parquet"
APPROVED_ALL_ROLE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_all_role_rows.parquet"
APPROVED_VARIANT_COMPARISON = APPROVED_V2_SESSION / "iizuka_v2_variant_pool_comparison.json"
APPROVED_FAILURE_AUDIT = APPROVED_V2_SESSION / "iizuka_v2_failure_mode_audit.json"
APPROVED_LINEAGE = APPROVED_V2_SESSION / "iizuka_v1_v2_lineage_comparison.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _month_bucket(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["anchor_date"], errors="coerce").dt.strftime("%Y-%m")


def _load_inputs() -> dict[str, Any]:
    required = {
        "approved_active_rows": APPROVED_ACTIVE_ROWS,
        "approved_all_role_rows": APPROVED_ALL_ROLE_ROWS,
        "approved_variant_comparison": APPROVED_VARIANT_COMPARISON,
        "approved_failure_audit": APPROVED_FAILURE_AUDIT,
        "approved_lineage": APPROVED_LINEAGE,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    return {
        "approved_active_rows": _load_frame(required["approved_active_rows"]),
        "approved_all_role_rows": _load_frame(required["approved_all_role_rows"]),
        "approved_variant_comparison": _load_json(required["approved_variant_comparison"]),
        "approved_failure_audit": _load_json(required["approved_failure_audit"]),
        "approved_lineage": _load_json(required["approved_lineage"]),
    }


def _build_manifest(output_root: Path, session_root: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_root.name,
        "output_root": str(output_root),
        "research_only": True,
        "boundary": "TRADEX-only",
        "source_artifacts": {
            "approved_active_rows": str(APPROVED_ACTIVE_ROWS),
            "approved_all_role_rows": str(APPROVED_ALL_ROLE_ROWS),
            "approved_variant_comparison": str(APPROVED_VARIANT_COMPARISON),
            "approved_failure_audit": str(APPROVED_FAILURE_AUDIT),
            "approved_lineage": str(APPROVED_LINEAGE),
        },
        "notes": [
            "single challenger only",
            "no gate threshold changes",
            "no production ranking changes",
            "no MeeMee changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
    }


def _build_input_resolution(output_root: Path, session_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    active = inputs["approved_active_rows"]
    all_role = inputs["approved_all_role_rows"]
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_root": str(session_root),
        "approved_v2_session": str(APPROVED_V2_SESSION),
        "row_coverage": {
            "approved_active_rows": int(len(active)),
            "approved_all_role_rows": int(len(all_role)),
            "approved_active_groups": int(active["anchor_date"].nunique()) if "anchor_date" in active.columns else 0,
            "approved_active_symbols": int(active["symbol"].nunique()) if "symbol" in active.columns else 0,
            "approved_active_months": int(_month_bucket(active).nunique()) if len(active) else 0,
        },
        "artifacts_present": {
            "approved_variant_comparison": True,
            "approved_failure_audit": True,
            "approved_lineage": True,
        },
        "notes": [
            "approved v2 active rows are the candidate universe for the single challenger",
            "top20pct_label remains unavailable in the approved bundle and is not imputed",
        ],
    }


def _bucket_priority(value: Any, mapping: dict[str, int]) -> int:
    return int(mapping.get(str(value), 0))


def _safe_abs(value: Any) -> float:
    if value is None:
        return float("inf")
    try:
        if pd.isna(value):
            return float("inf")
    except Exception:
        pass
    return abs(float(value))


def _build_challenger_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "research_fallback_label_source" not in out.columns:
        out["research_fallback_label_source"] = "ml_label_20d"
    out["candidate_contract_name"] = "iizuka_pre_decisive_long_candidate_v2_challenger_design"
    out["research_only"] = True
    out["v2_challenger_candidate_score"] = pd.to_numeric(out.get("iizuka_v2_candidate_score"), errors="coerce")
    out["v2_challenger_signal_priority"] = out["signal_quality_bucket"].map(
        {
            "signal_quality_high": 2,
            "signal_quality_mid": 1,
            "signal_quality_low": 0,
        }
    ).fillna(0).astype(int)
    out["v2_challenger_volume_priority"] = out["volume_participation_bucket"].map(
        {
            "volume_neutral": 2,
            "volume_confirmed": 1,
            "volume_weak": 0,
        }
    ).fillna(0).astype(int)
    out["v2_challenger_candle_priority"] = out["decision_candle_quality"].map(
        {
            "candle_strong": 1,
            "candle_mixed": 0,
            "candle_weak": 0,
        }
    ).fillna(0).astype(int)
    out["v2_challenger_shape_priority"] = out["shape_classification"].map(
        {
            "shape_positive_modifier": 1,
            "shape_context_dependent": 0,
        }
    ).fillna(0).astype(int)
    support_series = out["support_wick"] if "support_wick" in out.columns else pd.Series(False, index=out.index)
    engulf_series = out["bull_engulfing"] if "bull_engulfing" in out.columns else pd.Series(False, index=out.index)
    out["v2_challenger_support_priority"] = support_series.fillna(False).astype(bool).astype(int)
    out["v2_challenger_bull_engulfing_priority"] = engulf_series.fillna(False).astype(bool).astype(int)
    out["v2_challenger_ma20_closeness"] = out["close_vs_ma20_pct"].map(_safe_abs) if "close_vs_ma20_pct" in out.columns else pd.Series(float("inf"), index=out.index)
    out["v2_challenger_ma60_closeness"] = out["close_vs_ma60_pct"].map(_safe_abs) if "close_vs_ma60_pct" in out.columns else pd.Series(float("inf"), index=out.index)
    out["v2_challenger_order_reason"] = out.apply(
        lambda row: "|".join(
            [
                f"signal_{row['v2_challenger_signal_priority']}",
                f"volume_{row['v2_challenger_volume_priority']}",
                f"candle_{row['v2_challenger_candle_priority']}",
                f"shape_{row['v2_challenger_shape_priority']}",
                "support_wick" if _safe_bool(row.get("support_wick")) else "no_support_wick",
                "bull_engulfing" if _safe_bool(row.get("bull_engulfing")) else "no_bull_engulfing",
            ]
        ),
        axis=1,
    )
    sort_cols = [
        "v2_challenger_signal_priority",
        "v2_challenger_volume_priority",
        "v2_challenger_candle_priority",
        "v2_challenger_shape_priority",
        "v2_challenger_support_priority",
        "v2_challenger_bull_engulfing_priority",
        "v2_challenger_ma20_closeness",
        "v2_challenger_ma60_closeness",
        "v2_challenger_candidate_score",
        "champion_rank",
        "symbol",
    ]
    out = out.sort_values(
        sort_cols,
        ascending=[False, False, False, False, False, False, True, True, False, True, True],
        kind="stable",
    ).reset_index(drop=True)
    out["v2_challenger_candidate_rank"] = out.groupby("anchor_date").cumcount() + 1
    return out


def _records(frame: pd.DataFrame, fields: list[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    use = [field for field in fields if field in frame.columns]
    return [_json_ready(dict(row)) for row in frame[use].to_dict(orient="records")]


def _metric_for_selection(frame: pd.DataFrame) -> dict[str, Any]:
    metrics = _metric_bundle(frame)
    row_count = int(metrics["row_count"])
    month_count = int(_month_bucket(frame).nunique()) if len(frame) else 0
    metrics["group_count"] = int(frame["anchor_date"].nunique()) if "anchor_date" in frame.columns and len(frame) else 0
    metrics["symbol_count"] = int(frame["symbol"].nunique()) if "symbol" in frame.columns and len(frame) else 0
    metrics["month_count"] = month_count
    metrics["median_forward_ret_20d"] = float(pd.to_numeric(frame["forward_ret_20d"], errors="coerce").median()) if len(frame) and "forward_ret_20d" in frame.columns else None
    metrics["median_path_value_score_v1"] = float(pd.to_numeric(frame["path_value_score_v1"], errors="coerce").median()) if len(frame) and "path_value_score_v1" in frame.columns else None
    metrics["top15_capture_rate"] = float(metrics["top15_count"] / row_count) if row_count else None
    metrics["bottom15_contamination_rate"] = float(metrics["bottom15_count"] / row_count) if row_count else None
    metrics["top15_to_bottom15_ratio"] = float(metrics["top15_count"] / max(metrics["bottom15_count"], 1)) if row_count else None
    metrics["non_positive_return_rate"] = float(pd.to_numeric(frame["forward_ret_20d"], errors="coerce").le(0).mean()) if len(frame) and "forward_ret_20d" in frame.columns else None
    metrics["top20pct_available"] = "top20pct_label" in frame.columns
    metrics["top20pct_rate"] = float(pd.to_numeric(frame["top20pct_label"], errors="coerce").mean()) if "top20pct_label" in frame.columns and len(frame) else None
    symbol_counts = Counter(frame["symbol"].astype(str).tolist()) if len(frame) and "symbol" in frame.columns else Counter()
    metrics["top_symbol_counts"] = {str(symbol): int(count) for symbol, count in symbol_counts.most_common(10)}
    metrics["top_symbol_share"] = float(sum(count for _, count in symbol_counts.most_common(1)) / row_count) if row_count and symbol_counts else None
    metrics["top5_symbol_share"] = float(sum(count for _, count in symbol_counts.most_common(5)) / row_count) if row_count and symbol_counts else None
    return metrics


def _select_topk(frame: pd.DataFrame, sort_cols: list[str], ascending: list[bool], k: int) -> pd.DataFrame:
    selected = frame.copy()
    selected = selected.sort_values(sort_cols, ascending=ascending, kind="stable").reset_index(drop=True)
    selected["selected_rank"] = selected.groupby("anchor_date").cumcount() + 1
    return selected.loc[selected["selected_rank"] <= k].copy()


def _select_champion_topk(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    return _select_topk(frame, ["anchor_date", "champion_score", "champion_rank", "symbol"], [True, False, True, True], k)


def _select_v1_topk(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    return _select_topk(frame, ["anchor_date", "iizuka_candidate_score", "champion_rank", "symbol"], [True, False, True, True], k)


def _select_v2_topk(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    active = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    return _select_topk(active, ["anchor_date", "iizuka_v2_candidate_score", "champion_rank", "symbol"], [True, False, True, True], k)


def _select_challenger_topk(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    active = frame.loc[frame["v2_challenger_candidate_rank"].notna()].copy()
    return _select_topk(
        active,
        [
            "anchor_date",
            "v2_challenger_signal_priority",
            "v2_challenger_volume_priority",
            "v2_challenger_candle_priority",
            "v2_challenger_shape_priority",
            "v2_challenger_support_priority",
            "v2_challenger_bull_engulfing_priority",
            "v2_challenger_ma20_closeness",
            "v2_challenger_ma60_closeness",
            "v2_challenger_candidate_score",
            "champion_rank",
            "symbol",
        ],
        [True, False, False, False, False, False, False, True, True, False, True, True],
        k,
    )


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
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_pass": all(value == 0 for value in flag_violations.values()) and all(value == 0 for value in date_violations.values()),
        "flag_violations": flag_violations,
        "date_violations": date_violations,
        "notes": [
            "approved v2 rows already satisfy the no-lookahead contract",
            "challenger ranking uses only emitted non-outcome row-local fields",
        ],
    }


def _build_leakage_audit(frame: pd.DataFrame) -> dict[str, Any]:
    feature_fields_used = {
        "iizuka_v2_candidate_score",
        "signal_quality_bucket",
        "volume_participation_bucket",
        "decision_candle_quality",
        "shape_classification",
        "support_wick",
        "bull_engulfing",
        "close_vs_ma20_pct",
        "close_vs_ma60_pct",
        "ma20_slope_1",
        "ma60_slope_1",
        "v2_challenger_signal_priority",
        "v2_challenger_volume_priority",
        "v2_challenger_candle_priority",
        "v2_challenger_shape_priority",
        "v2_challenger_support_priority",
        "v2_challenger_bull_engulfing_priority",
        "v2_challenger_ma20_closeness",
        "v2_challenger_ma60_closeness",
    }
    outcome_fields = set(EVAL_LABEL_COLUMNS)
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_fields_used": sorted(feature_fields_used),
        "outcome_fields": sorted(outcome_fields),
        "outcome_fields_used_as_features": sorted(feature_fields_used.intersection(outcome_fields)),
        "outcome_fields_attached_after_candidate_construction": sorted([field for field in EVAL_LABEL_COLUMNS if field in frame.columns]),
        "leakage_free": not feature_fields_used.intersection(outcome_fields),
        "note": "challenger uses only row-local non-outcome features emitted in the approved v2 bundle",
    }


def _compare_topk(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    diff_rows: list[pd.DataFrame] = []
    challenger = frame.loc[frame["v2_challenger_candidate_rank"].notna()].copy()
    for k in TOP_K_VALUES:
        champion_sel = _select_champion_topk(frame, k)
        v1_sel = _select_v1_topk(frame, k)
        v2_sel = _select_v2_topk(frame, k)
        challenger_sel = _select_challenger_topk(frame, k)

        champ_keys = set(champion_sel["surface_key"].astype(str)) if len(champion_sel) else set()
        v1_keys = set(v1_sel["surface_key"].astype(str)) if len(v1_sel) else set()
        v2_keys = set(v2_sel["surface_key"].astype(str)) if len(v2_sel) else set()
        chal_keys = set(challenger_sel["surface_key"].astype(str)) if len(challenger_sel) else set()
        union = champ_keys | v1_keys | v2_keys | chal_keys
        diff = pd.DataFrame({"top_k": k, "surface_key": list(union)})
        diff = diff.merge(
            frame[[
                "surface_key",
                "anchor_date",
                "symbol",
                "side",
                "champion_score",
                "champion_rank",
                "iizuka_candidate_score",
                "iizuka_candidate_rank",
                "iizuka_v2_candidate_score",
                "iizuka_v2_candidate_rank",
                "v2_challenger_candidate_score",
                "v2_challenger_candidate_rank",
                "v2_challenger_order_reason",
                "iizuka_v2_role",
                "iizuka_v2_reason",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
                *([c for c in ("top20pct_label",) if c in frame.columns]),
            ] + [c for c in ("month_bucket",) if c in frame.columns]],
            on="surface_key",
            how="left",
        )
        diff["selected_in_champion"] = diff["surface_key"].isin(champ_keys)
        diff["selected_in_v1"] = diff["surface_key"].isin(v1_keys)
        diff["selected_in_v2_active"] = diff["surface_key"].isin(v2_keys)
        diff["selected_in_challenger"] = diff["surface_key"].isin(chal_keys)
        diff["selection_state"] = diff.apply(
            lambda row: "|".join(
                [
                    label
                    for label, flag in (
                        ("champion", row["selected_in_champion"]),
                        ("v1", row["selected_in_v1"]),
                        ("v2_active", row["selected_in_v2_active"]),
                        ("challenger", row["selected_in_challenger"]),
                    )
                    if flag
                ]
            )
            or "none",
            axis=1,
        )
        diff["member_change_v2_challenger"] = diff["selected_in_v2_active"] != diff["selected_in_challenger"]
        diff["member_change_champion_challenger"] = diff["selected_in_champion"] != diff["selected_in_challenger"]
        diff["member_change_v1_challenger"] = diff["selected_in_v1"] != diff["selected_in_challenger"]
        diff["top_k"] = k
        diff_rows.append(diff)

        champion_metrics = _metric_for_selection(champion_sel)
        v1_metrics = _metric_for_selection(v1_sel)
        v2_metrics = _metric_for_selection(v2_sel)
        challenger_metrics = _metric_for_selection(challenger_sel)
        rows.append(
            {
                "top_k": k,
                "champion": champion_metrics,
                "v1": v1_metrics,
                "v2_active": v2_metrics,
                "challenger": challenger_metrics,
                "changed_top5_members_count": int(len(v2_keys ^ chal_keys)) if k == 5 else None,
                "changed_top10_members_count": int(len(v2_keys ^ chal_keys)) if k == 10 else None,
                "changed_top20_members_count": int(len(v2_keys ^ chal_keys)) if k == 20 else None,
                "membership_changed_count_v2_challenger": int(len(v2_keys ^ chal_keys)),
                "membership_changed_count_champion_challenger": int(len(champ_keys ^ chal_keys)),
                "membership_changed_count_v1_challenger": int(len(v1_keys ^ chal_keys)),
                "overlap_ratio_v2_challenger": float(len(v2_keys & chal_keys) / len(v2_keys | chal_keys)) if (v2_keys | chal_keys) else None,
                "overlap_ratio_champion_challenger": float(len(champ_keys & chal_keys) / len(champ_keys | chal_keys)) if (champ_keys | chal_keys) else None,
                "overlap_ratio_v1_challenger": float(len(v1_keys & chal_keys) / len(v1_keys | chal_keys)) if (v1_keys | chal_keys) else None,
                "top15_retained_vs_v2_active": int(len(set(v2_sel["surface_key"]) & chal_keys)),
                "top15_lost_vs_v2_active": int(len(set(v2_sel["surface_key"]) - chal_keys)),
                "bottom15_removed_vs_v2_active": int(len(set(frame.loc[(frame["iizuka_v2_role"] == "active") & frame["bottom15_label"].fillna(False).astype(bool), "surface_key"]) - chal_keys)),
            }
        )

        diff_rows[-1]["selection_gap_count_v2_challenger"] = int(len(v2_keys ^ chal_keys))

    comparison = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_challenger_design",
        "challenger_name": "stability_first_rerank",
        "metric_mode": "per_anchor_date_topK",
        "top20pct_available": False,
        "top20pct_note": "top20pct_label is missing from the approved bundle and is not imputed",
        "per_k": rows,
        "challenge_frame_summary": {
            "row_count": int(len(challenger)),
            "group_count": int(challenger["anchor_date"].nunique()) if len(challenger) else 0,
            "symbol_count": int(challenger["symbol"].nunique()) if len(challenger) else 0,
            "month_count": int(_month_bucket(challenger).nunique()) if len(challenger) else 0,
        },
    }
    diff_frame = pd.concat(diff_rows, ignore_index=True) if diff_rows else pd.DataFrame()
    failure_mode = {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "per_k": {},
    }
    for item in rows:
        k = str(item["top_k"])
        challenger_sel = _select_challenger_topk(frame, item["top_k"])
        v2_sel = _select_v2_topk(frame, item["top_k"])
        failure_mode["per_k"][k] = {
            "reason_block_contribution_challenger": {str(key): int(value) for key, value in challenger_sel["iizuka_v2_reason"].value_counts().head(10).items()},
            "false_positive_cost_challenger": {
                "bottom15_count": int(pd.to_numeric(challenger_sel["bottom15_label"], errors="coerce").fillna(0).sum()) if len(challenger_sel) else 0,
                "bottom15_rate": float(pd.to_numeric(challenger_sel["bottom15_label"], errors="coerce").mean()) if len(challenger_sel) else None,
            },
            "reason_block_contribution_v2_active": {str(key): int(value) for key, value in v2_sel["iizuka_v2_reason"].value_counts().head(10).items()},
            "comparison_snapshot": {
                "challenger": item["challenger"],
                "v2_active": item["v2_active"],
            },
        }
    return comparison, diff_frame, failure_mode


def _build_month_audit(frame: pd.DataFrame, comparison: dict[str, Any]) -> dict[str, Any]:
    challenger_top20 = _select_challenger_topk(frame, 20)
    v2_top20 = _select_v2_topk(frame, 20)
    active_months = _month_bucket(frame).value_counts().sort_index()
    per_k: dict[str, list[dict[str, Any]]] = {}
    month_rows = []
    deltas = []
    for month in active_months.index.tolist():
        chal_month = challenger_top20.copy().assign(month=_month_bucket(challenger_top20)).loc[lambda d: d["month"] == month]
        v2_month = v2_top20.copy().assign(month=_month_bucket(v2_top20)).loc[lambda d: d["month"] == month]
        chal_metrics = _metric_for_selection(chal_month)
        v2_metrics = _metric_for_selection(v2_month)
        delta_mean = None
        delta_median = None
        delta_bottom15 = None
        if chal_metrics["mean_forward_ret_20d"] is not None and v2_metrics["mean_forward_ret_20d"] is not None:
            delta_mean = _safe_float(chal_metrics["mean_forward_ret_20d"] - v2_metrics["mean_forward_ret_20d"])
        if chal_metrics["median_forward_ret_20d"] is not None and v2_metrics["median_forward_ret_20d"] is not None:
            delta_median = _safe_float(chal_metrics["median_forward_ret_20d"] - v2_metrics["median_forward_ret_20d"])
        if chal_metrics["bottom15_contamination_rate"] is not None and v2_metrics["bottom15_contamination_rate"] is not None:
            delta_bottom15 = _safe_float(chal_metrics["bottom15_contamination_rate"] - v2_metrics["bottom15_contamination_rate"])
        weight = abs(delta_mean or 0.0) * max(int(chal_metrics["row_count"]), 1)
        deltas.append((month, weight, delta_mean))
        month_rows.append(
            {
                "month": month,
                "active_row_count": int(active_months.loc[month]),
                "active_row_share": float(active_months.loc[month] / max(len(frame), 1)),
                "challenger_top20_row_count": int(chal_metrics["row_count"]),
                "challenger_top20_row_share": float(chal_metrics["row_count"] / max(len(challenger_top20), 1)) if len(challenger_top20) else None,
                "challenger_top20_mean_forward_ret_20d": chal_metrics["mean_forward_ret_20d"],
                "challenger_top20_median_forward_ret_20d": chal_metrics["median_forward_ret_20d"],
                "challenger_top20_bottom15_contamination_rate": chal_metrics["bottom15_contamination_rate"],
                "v2_active_top20_mean_forward_ret_20d": v2_metrics["mean_forward_ret_20d"],
                "v2_active_top20_median_forward_ret_20d": v2_metrics["median_forward_ret_20d"],
                "v2_active_top20_bottom15_contamination_rate": v2_metrics["bottom15_contamination_rate"],
                "delta_mean_forward_ret_20d": delta_mean,
                "delta_median_forward_ret_20d": delta_median,
                "delta_bottom15_contamination_rate": delta_bottom15,
            }
        )
    for k in TOP_K_VALUES:
        challenger_sel = _select_challenger_topk(frame, k).assign(month=_month_bucket(_select_challenger_topk(frame, k)))
        v2_sel = _select_v2_topk(frame, k).assign(month=_month_bucket(_select_v2_topk(frame, k)))
        month_entries = []
        for month in active_months.index.tolist():
            chal_month = challenger_sel.loc[challenger_sel["month"] == month].copy()
            v2_month = v2_sel.loc[v2_sel["month"] == month].copy()
            chal_metrics = _metric_for_selection(chal_month)
            v2_metrics = _metric_for_selection(v2_month)
            month_entries.append(
                {
                    "month": month,
                    "challenger_row_count": int(chal_metrics["row_count"]),
                    "challenger_share_of_topk": float(chal_metrics["row_count"] / max(len(challenger_sel), 1)) if len(challenger_sel) else None,
                    "challenger_mean_forward_ret_20d": chal_metrics["mean_forward_ret_20d"],
                    "challenger_median_forward_ret_20d": chal_metrics["median_forward_ret_20d"],
                    "challenger_bottom15_contamination_rate": chal_metrics["bottom15_contamination_rate"],
                    "v2_active_row_count": int(v2_metrics["row_count"]),
                    "v2_active_share_of_topk": float(v2_metrics["row_count"] / max(len(v2_sel), 1)) if len(v2_sel) else None,
                    "v2_active_mean_forward_ret_20d": v2_metrics["mean_forward_ret_20d"],
                    "v2_active_median_forward_ret_20d": v2_metrics["median_forward_ret_20d"],
                    "v2_active_bottom15_contamination_rate": v2_metrics["bottom15_contamination_rate"],
                    "delta_mean_forward_ret_20d": _safe_float((chal_metrics["mean_forward_ret_20d"] or 0.0) - (v2_metrics["mean_forward_ret_20d"] or 0.0)) if chal_metrics["mean_forward_ret_20d"] is not None and v2_metrics["mean_forward_ret_20d"] is not None else None,
                }
            )
        per_k[str(k)] = month_entries
    total_weight = sum(weight for _, weight, _ in deltas) or 0.0
    dominant_month = None
    dominant_share = None
    if deltas:
        dominant_month, dominant_weight, dominant_delta = max(deltas, key=lambda item: item[1])
        dominant_share = float(dominant_weight / total_weight) if total_weight else None
    return {
        "schema_version": MONTH_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "active_month_count": int(len(active_months)),
        "active_month_rows": {str(month): int(count) for month, count in active_months.items()},
        "per_month": month_rows,
        "per_k": per_k,
        "improvement_concentration": {
            "dominant_month": dominant_month,
            "dominant_month_share": dominant_share,
            "one_month_dominated": bool(dominant_share is not None and dominant_share >= 0.60),
        },
        "notes": [
            "top20 month audit compares challenger top20 against the approved v2 active surface top20",
            "3 active months is a narrow breadth signal and is reported explicitly",
        ],
    }


def _build_contract(frame: pd.DataFrame) -> dict[str, Any]:
    volume_priority = {
        "volume_neutral": 2,
        "volume_confirmed": 1,
        "volume_weak": 0,
    }
    signal_priority = {
        "signal_quality_high": 2,
        "signal_quality_mid": 1,
        "signal_quality_low": 0,
    }
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_challenger_design",
        "challenger_name": "stability_first_rerank",
        "scope": "TRADEX-only",
        "preserved": [
            "approved v2 gate",
            "long-side only",
            "no-lookahead contract",
            "approved label source policy",
            "approved active/diagnostic/excluded lanes",
        ],
        "ordering": [
            "signal_quality_bucket priority",
            "volume_participation_bucket priority",
            "decision_candle_quality priority",
            "shape_classification priority",
            "support_wick",
            "bull_engulfing",
            "distance-to-ma20 closeness",
            "distance-to-ma60 closeness",
            "approved v2 candidate score",
            "champion_rank",
            "symbol",
        ],
        "priority_maps": {
            "signal_quality_bucket": signal_priority,
            "volume_participation_bucket": volume_priority,
        },
        "required_non_outcome_fields": [
            "signal_quality_bucket",
            "volume_participation_bucket",
            "decision_candle_quality",
            "shape_classification",
            "support_wick",
            "bull_engulfing",
            "close_vs_ma20_pct",
            "close_vs_ma60_pct",
            "iizuka_v2_candidate_score",
        ],
        "required_non_outcome_fields_present": {field: field in frame.columns for field in [
            "signal_quality_bucket",
            "volume_participation_bucket",
            "decision_candle_quality",
            "shape_classification",
            "support_wick",
            "bull_engulfing",
            "close_vs_ma20_pct",
            "close_vs_ma60_pct",
            "iizuka_v2_candidate_score",
        ]},
        "non_scope": [
            "no gate threshold changes",
            "no additional challengers",
            "no model training",
            "no reranker retuning",
            "no outcome-field feature use",
            "no MeeMee reflection",
            "no production ranking changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
        "notes": [
            "challenger is a single stability-first rerank inside the approved v2 active lane",
            "ranking uses only row-local non-outcome modifiers already emitted in the approved bundle",
            "top20pct_label is not available in the approved bundle and is recorded as missing rather than imputed",
        ],
    }


def _build_decision(comparison: dict[str, Any], month_audit: dict[str, Any], no_lookahead: dict[str, Any], leakage: dict[str, Any]) -> dict[str, Any]:
    top5 = next(item for item in comparison["per_k"] if item["top_k"] == 5)
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    challenger = top20["challenger"]
    v2_active = top20["v2_active"]
    decision = "hold"
    reason = "challenger is promising but breadth remains narrow and month dependence must be checked before keep"

    if not no_lookahead["no_lookahead_pass"] or not leakage["leakage_free"]:
        decision = "drop"
        reason = "no-lookahead or leakage audit failed"
    elif challenger["row_count"] == 0:
        decision = "drop"
        reason = "challenger selection is empty"
    else:
        top10_improves = top10["challenger"]["mean_forward_ret_20d"] is not None and v2_active["mean_forward_ret_20d"] is not None and top10["challenger"]["mean_forward_ret_20d"] > v2_active["mean_forward_ret_20d"]
        top20_improves = top20["challenger"]["mean_forward_ret_20d"] is not None and v2_active["mean_forward_ret_20d"] is not None and top20["challenger"]["mean_forward_ret_20d"] > v2_active["mean_forward_ret_20d"]
        bottom15_ok = top20["challenger"]["bottom15_contamination_rate"] is not None and v2_active["bottom15_contamination_rate"] is not None and top20["challenger"]["bottom15_contamination_rate"] <= v2_active["bottom15_contamination_rate"]
        non_positive_ok = top20["challenger"]["non_positive_return_rate"] is not None and v2_active["non_positive_return_rate"] is not None and top20["challenger"]["non_positive_return_rate"] <= v2_active["non_positive_return_rate"]
        membership_changed_enough = bool(top20["changed_top20_members_count"] is not None and top20["changed_top20_members_count"] >= 20)
        not_month_dominated = not month_audit["improvement_concentration"]["one_month_dominated"]
        narrow_breadth = month_audit["active_month_count"] <= 3
        if top10_improves and top20_improves and bottom15_ok and non_positive_ok and membership_changed_enough and not_month_dominated and not narrow_breadth:
            decision = "keep"
            reason = "challenger improves top10/top20 versus approved v2, does not worsen bottom15 or non-positive rate, and is not month-dominated"
        elif top20_improves and top10_improves and bottom15_ok and non_positive_ok and membership_changed_enough and not_month_dominated and narrow_breadth:
            decision = "hold"
            reason = "challenger improves structure but active breadth is still only three months, so keep is not yet justified"
        elif top20_improves and top10_improves and bottom15_ok and non_positive_ok and not_month_dominated:
            decision = "hold"
            reason = "challenger improves but sample breadth remains narrow enough to hold before keep"
        elif not top10_improves or not top20_improves:
            decision = "drop"
            reason = "challenger does not improve both top10 and top20 versus approved v2"
        elif not bottom15_ok:
            decision = "drop"
            reason = "challenger worsens bottom15 contamination versus approved v2"
        elif not non_positive_ok:
            decision = "drop"
            reason = "challenger worsens non-positive return rate versus approved v2"
        elif not membership_changed_enough:
            decision = "drop"
            reason = "challenger is effectively identical to approved v2"
        elif month_audit["improvement_concentration"]["one_month_dominated"]:
            decision = "hold"
            reason = "improvement is month-dominated, so keep is not justified"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v2_challenger_design",
        "summary": {
            "approved_v2_active_row_count": int(v2_active["row_count"]),
            "challenger_row_count": int(challenger["row_count"]),
            "challenger_group_count": int(challenger["group_count"]),
            "challenger_symbol_count": int(challenger["symbol_count"]),
            "challenger_month_count": int(challenger["month_count"]),
            "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
            "leakage_free": bool(leakage["leakage_free"]),
            "top20pct_available": False,
        },
        "comparison_snapshot": {
            "top10": top10,
            "top20": top20,
        },
        "month_audit_snapshot": month_audit,
        "notes": [
            "challenger modifies only ranking inside the approved v2 active lane",
            "top20pct_label is unavailable in the approved bundle and is not imputed",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive long candidate v2 challenger design")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = Path(args.output_root).expanduser().resolve()
    session_id = _session_id()
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=True)

    inputs = _load_inputs()
    approved_active = inputs["approved_active_rows"]
    challenger_frame = _build_challenger_frame(approved_active)
    challenger_frame = challenger_frame.sort_values(["anchor_date", "v2_challenger_candidate_rank", "champion_rank", "symbol"], ascending=[True, True, True, True], kind="stable").reset_index(drop=True)
    comparison_frame = inputs["approved_all_role_rows"].merge(
        challenger_frame[[
            "surface_key",
            "v2_challenger_candidate_score",
            "v2_challenger_candidate_rank",
            "v2_challenger_signal_priority",
            "v2_challenger_volume_priority",
            "v2_challenger_candle_priority",
            "v2_challenger_shape_priority",
            "v2_challenger_support_priority",
            "v2_challenger_bull_engulfing_priority",
            "v2_challenger_ma20_closeness",
            "v2_challenger_ma60_closeness",
            "v2_challenger_order_reason",
        ]],
        on="surface_key",
        how="left",
    )

    contract = _build_contract(challenger_frame)
    no_lookahead = _build_no_lookahead_audit(challenger_frame)
    leakage = _build_leakage_audit(challenger_frame)
    comparison, diff_frame, failure_mode = _compare_topk(comparison_frame)
    month_audit = _build_month_audit(challenger_frame, comparison)
    decision = _build_decision(comparison, month_audit, no_lookahead, leakage)

    _write_json(session_root / "run_manifest.json", _build_manifest(output_root, session_root))
    _write_json(session_root / "input_resolution.json", _build_input_resolution(output_root, session_root, inputs))
    _write_json(session_root / "challenger_design_contract.json", contract)
    _write_parquet(session_root / "v2_challenger_candidate_rows.parquet", challenger_frame)
    _write_json(session_root / "v2_challenger_comparison.json", comparison)
    _write_parquet(session_root / "v2_challenger_topk_membership_diff.parquet", diff_frame)
    _write_json(session_root / "v2_challenger_month_dependence_audit.json", month_audit)
    _write_json(session_root / "v2_challenger_failure_mode_audit.json", failure_mode)
    _write_json(session_root / "v2_challenger_no_lookahead_audit.json", no_lookahead)
    _write_json(session_root / "v2_challenger_leakage_audit.json", leakage)
    _write_json(session_root / "v2_challenger_decision.json", decision)
    _write_parquet(
        session_root / "v2_challenger_candidate_examples.parquet",
        challenger_frame.head(50)[[
            "anchor_date",
            "symbol",
            "iizuka_v2_reason",
            "v2_challenger_order_reason",
            "forward_ret_20d",
            "path_value_score_v1",
            "top15_label",
            "bottom15_label",
            "v2_challenger_candidate_score",
            "v2_challenger_candidate_rank",
        ]].copy(),
    )
    _write_parquet(
        session_root / "v2_challenger_reason_block_summary.parquet",
        challenger_frame[[
            "anchor_date",
            "symbol",
            "iizuka_v2_reason",
            "v2_challenger_order_reason",
            "forward_ret_20d",
            "path_value_score_v1",
            "top15_label",
            "bottom15_label",
        ]].copy(),
    )
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_root": str(session_root),
            "output_root": str(output_root),
            "artifact_count": 12,
            "artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "challenger_design_contract.json",
                "v2_challenger_candidate_rows.parquet",
                "v2_challenger_comparison.json",
                "v2_challenger_topk_membership_diff.parquet",
                "v2_challenger_month_dependence_audit.json",
                "v2_challenger_failure_mode_audit.json",
                "v2_challenger_no_lookahead_audit.json",
                "v2_challenger_leakage_audit.json",
                "v2_challenger_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "decision": decision["decision"],
        },
    )


if __name__ == "__main__":
    main()
