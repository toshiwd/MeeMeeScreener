from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status

from scripts.tradex_reflectability_funnel_common_v1 import (
    CHAMPION_TOPK_VALUES,
    TOP_K_VALUES,
    _ensure_columns,
    _json_ready,
    _json_text,
    _load_json,
    _mean_or_none,
    _safe_float,
    _safe_int,
    _safe_path,
    _utc_now,
    _write_json,
)

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\champion_topk_bad_pick_veto_v1")
SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1"
MANIFEST_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_manifest_v1"
EVALUATION_CONTRACT_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_evaluation_contract_v1"
BRANCHING_PROBE_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_branching_probe_v1"
COMPARE_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_compare_v1"
BAD_PICK_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_bad_pick_removal_summary_v1"
EFFECTIVENESS_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_topk_effectiveness_summary_v1"
REGIME_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_regime_split_summary_v1"
TURNOVER_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_turnover_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_decision_v1"
REFLECTABILITY_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_meemee_reflectability_assessment_v1"


def _context_overextended(monthly_context: str, weekly_context: str) -> bool:
    return monthly_context in {"monthly_overextended", "monthly_downtrend"} or weekly_context in {"weekly_overextended", "weekly_downtrend"}


def _symbol_trailing_median(frame: pd.DataFrame, column: str) -> pd.Series:
    working = frame[["symbol", "anchor_date", column]].copy()
    working["_orig_index"] = range(len(working))
    working[column] = pd.to_numeric(working[column], errors="coerce")
    working = working.sort_values(["symbol", "anchor_date", "_orig_index"], kind="stable")
    working["_trailing_median"] = working.groupby("symbol")[column].transform(lambda group: group.shift(1).expanding(min_periods=1).median())
    return working.sort_values("_orig_index", kind="stable")["_trailing_median"].reset_index(drop=True).set_axis(frame.index)


def _month_bucket_from_anchor_date(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 7:
        return text[:7]
    return text or "unknown"


def _top5_symbols(frame: pd.DataFrame, *, score_column: str) -> list[str]:
    if score_column not in frame.columns or "symbol" not in frame.columns:
        return []
    ranked = frame[["symbol", score_column]].copy()
    ranked = ranked.dropna(subset=["symbol", score_column])
    if ranked.empty:
        return []
    ranked[score_column] = pd.to_numeric(ranked[score_column], errors="coerce")
    ranked = ranked.dropna(subset=[score_column])
    ranked = ranked.sort_values([score_column, "symbol"], ascending=[False, True], kind="stable")
    symbols = ranked["symbol"].astype(str).tolist()
    return list(dict.fromkeys(symbols[:5]))


def _build_monthly_top5_capture_summary(
    frame: pd.DataFrame,
    *,
    score_column: str,
    capture_label: str,
    regime_column: str | None = None,
) -> dict[str, Any]:
    work = frame.copy()
    if "month_bucket" not in work.columns:
        work["month_bucket"] = work["anchor_date"].map(_month_bucket_from_anchor_date) if "anchor_date" in work.columns else "unknown"
    else:
        work["month_bucket"] = work["month_bucket"].fillna(work["anchor_date"].map(_month_bucket_from_anchor_date) if "anchor_date" in work.columns else "unknown").astype(str)
    work["anchor_date"] = work["anchor_date"].astype(str) if "anchor_date" in work.columns else "unknown"
    if "symbol" not in work.columns or "forward_ret_20d" not in work.columns or score_column not in work.columns:
        return {
            "schema_version": f"{SCHEMA_VERSION}_monthly_top5_capture_summary_v1",
            "generated_at": _utc_now(),
            "capture_label": capture_label,
            "available": False,
            "reason": "missing_required_columns",
            "months_evaluated": 0,
            "champion_monthly_top5_capture": None,
            "candidate_monthly_top5_capture": None,
            "monthly_top5_capture_delta": None,
            "zero_capture_months": 0,
            "degraded_months": 0,
            "improved_months": 0,
            "unchanged_months": 0,
            "cannot_evaluate_months": [],
            "months": [],
            "by_regime": {},
        }

    month_details: list[dict[str, Any]] = []
    cannot_evaluate_months: list[dict[str, Any]] = []
    month_rates: list[float] = []
    zero_capture_months = 0
    degraded_months = 0
    improved_months = 0
    unchanged_months = 0

    for month, month_group in sorted(work.groupby("month_bucket", dropna=False), key=lambda item: str(item[0])):
        month_target_union: set[str] = set()
        month_model_union: set[str] = set()
        month_target_ret20: list[float] = []
        month_model_ret20: list[float] = []
        month_capture_ret20: list[float] = []
        month_date_details: list[dict[str, Any]] = []
        month_reasons: list[str] = []
        for anchor_date, day_group in sorted(month_group.groupby("anchor_date", dropna=False), key=lambda item: str(item[0])):
            day_group = day_group.copy()
            day_group["forward_ret_20d"] = pd.to_numeric(day_group["forward_ret_20d"], errors="coerce")
            day_group[score_column] = pd.to_numeric(day_group[score_column], errors="coerce")
            eligible = day_group.dropna(subset=["symbol", "forward_ret_20d", score_column])
            if eligible.empty:
                month_reasons.append(f"{anchor_date}:missing_required_rows")
                continue
            target_codes = _top5_symbols(eligible, score_column="forward_ret_20d")
            model_codes = _top5_symbols(eligible, score_column=score_column)
            if not target_codes:
                month_reasons.append(f"{anchor_date}:no_target_top5")
                continue
            if not model_codes:
                month_reasons.append(f"{anchor_date}:no_model_top5")
                continue
            month_target_union.update(target_codes)
            month_model_union.update(model_codes)
            month_target_ret20.append(float(pd.to_numeric(eligible.loc[eligible["symbol"].astype(str).isin(target_codes), "forward_ret_20d"], errors="coerce").mean()))
            month_model_ret20.append(float(pd.to_numeric(eligible.loc[eligible["symbol"].astype(str).isin(model_codes), "forward_ret_20d"], errors="coerce").mean()))
            month_capture_codes = sorted(set(target_codes) & set(model_codes))
            month_capture_ret20.append(float(pd.to_numeric(eligible.loc[eligible["symbol"].astype(str).isin(month_capture_codes), "forward_ret_20d"], errors="coerce").mean()) if month_capture_codes else 0.0)
            month_date_details.append(
                {
                    "anchor_date": anchor_date,
                    "target_top5_codes": target_codes,
                    "model_top5_codes": model_codes,
                    "capture_codes": month_capture_codes,
                    "capture_count": len(month_capture_codes),
                    "target_count": len(target_codes),
                    "model_count": len(model_codes),
                }
            )
        if not month_date_details:
            cannot_evaluate_months.append({"month_bucket": str(month), "reason": "; ".join(sorted(set(month_reasons))) or "no_evaluable_dates"})
            continue
        capture_codes = sorted(month_target_union & month_model_union)
        capture_count = len(capture_codes)
        target_count = len(month_target_union)
        model_count = len(month_model_union)
        capture_rate = float(capture_count / target_count) if target_count else 0.0
        month_rates.append(capture_rate)
        if capture_rate <= 0.0:
            zero_capture_months += 1
        month_delta = None
        month_detail = {
            "month_bucket": str(month),
            "capture_label": capture_label,
            "evaluated_dates": len(month_date_details),
            "target_union_count": target_count,
            "model_union_count": model_count,
            "capture_count": capture_count,
            "capture_rate": capture_rate,
            "target_ret20_mean": _safe_float(pd.Series(month_target_ret20).mean()),
            "model_ret20_mean": _safe_float(pd.Series(month_model_ret20).mean()),
            "capture_ret20_mean": _safe_float(pd.Series(month_capture_ret20).mean()),
            "target_union_codes": sorted(month_target_union),
            "model_union_codes": sorted(month_model_union),
            "capture_codes": capture_codes,
            "date_details": month_date_details,
            "cannot_evaluate_reasons": sorted(set(month_reasons)),
        }
        month_details.append(month_detail)

    regime_summary: dict[str, Any] = {}
    if regime_column and regime_column in work.columns:
        for regime_value, regime_group in work.groupby(work[regime_column].astype(str), dropna=False, sort=True):
            regime_summary[str(regime_value)] = _build_monthly_top5_capture_summary(
                regime_group,
                score_column=score_column,
                capture_label=f"{capture_label}:{regime_column}",
                regime_column=None,
            )

    return {
        "schema_version": f"{SCHEMA_VERSION}_monthly_top5_capture_summary_v1",
        "generated_at": _utc_now(),
        "capture_label": capture_label,
        "available": True,
        "metric_definition": "intersection of monthly unions: realized forward_ret_20d top5 vs model ranking top5",
        "months_evaluated": len(month_details),
        "month_count": len(month_details),
        "champion_monthly_top5_capture": None,
        "candidate_monthly_top5_capture": None,
        "monthly_top5_capture_delta": None,
        "zero_capture_months": zero_capture_months,
        "degraded_months": degraded_months,
        "improved_months": improved_months,
        "unchanged_months": unchanged_months,
        "months": month_details,
        "cannot_evaluate_months": cannot_evaluate_months,
        "by_regime": regime_summary,
        "mean_capture_rate": _safe_float(pd.Series(month_rates).mean()) if month_rates else 0.0,
        "median_capture_rate": _safe_float(pd.Series(month_rates).median()) if month_rates else 0.0,
    }


def _build_monthly_top5_capture_comparison(frame: pd.DataFrame) -> dict[str, Any]:
    regime_column = None
    for candidate_column in ("family_regime_context", "market_regime_bucket", "dominant_regime_context"):
        if candidate_column in frame.columns:
            regime_column = candidate_column
            break
    champion_score_column = "champion_score" if "champion_score" in frame.columns else "score"
    candidate_score_column = "adjusted_score" if "adjusted_score" in frame.columns else "score"
    champion_summary = _build_monthly_top5_capture_summary(
        frame,
        score_column=champion_score_column,
        capture_label="champion",
        regime_column=regime_column,
    )
    candidate_summary = _build_monthly_top5_capture_summary(
        frame,
        score_column=candidate_score_column,
        capture_label="candidate",
        regime_column=regime_column,
    )

    champion_months = {str(item.get("month_bucket")): item for item in champion_summary.get("months", []) if isinstance(item, dict)}
    candidate_months = {str(item.get("month_bucket")): item for item in candidate_summary.get("months", []) if isinstance(item, dict)}
    month_keys = sorted(set(champion_months) & set(candidate_months))

    month_rows: list[dict[str, Any]] = []
    skipped_months: list[dict[str, Any]] = []
    champion_rates: list[float] = []
    candidate_rates: list[float] = []
    deltas: list[float] = []
    zero_capture_months = 0
    degraded_months = 0
    improved_months = 0
    unchanged_months = 0

    for month in month_keys:
        champion_month = champion_months.get(month)
        candidate_month = candidate_months.get(month)
        if not champion_month or not candidate_month:
            skipped_months.append(
                {
                    "month_bucket": month,
                    "reason": "missing_champion_or_candidate_month_summary",
                }
            )
            continue
        champion_rate = _safe_float(champion_month.get("capture_rate"), 0.0)
        candidate_rate = _safe_float(candidate_month.get("capture_rate"), 0.0)
        delta = candidate_rate - champion_rate
        champion_rates.append(champion_rate)
        candidate_rates.append(candidate_rate)
        deltas.append(delta)
        if candidate_rate <= 0.0:
            zero_capture_months += 1
        if delta > 0:
            improved_months += 1
        elif delta < 0:
            degraded_months += 1
        else:
            unchanged_months += 1
        month_rows.append(
            {
                "month_bucket": month,
                "champion_capture_rate": champion_rate,
                "candidate_capture_rate": candidate_rate,
                "monthly_top5_capture_delta": delta,
                "champion_capture_count": int(champion_month.get("capture_count") or 0),
                "candidate_capture_count": int(candidate_month.get("capture_count") or 0),
                "champion_target_union_count": int(champion_month.get("target_union_count") or 0),
                "candidate_target_union_count": int(candidate_month.get("target_union_count") or 0),
                "champion_model_union_count": int(champion_month.get("model_union_count") or 0),
                "candidate_model_union_count": int(candidate_month.get("model_union_count") or 0),
                "capture_status": "improved" if delta > 0 else "degraded" if delta < 0 else "unchanged",
                "champion_cannot_evaluate_reasons": champion_month.get("cannot_evaluate_reasons") or [],
                "candidate_cannot_evaluate_reasons": candidate_month.get("cannot_evaluate_reasons") or [],
            }
        )

    regime_rows: dict[str, Any] = {}
    champion_regime_map = champion_summary.get("by_regime") if isinstance(champion_summary.get("by_regime"), dict) else {}
    candidate_regime_map = candidate_summary.get("by_regime") if isinstance(candidate_summary.get("by_regime"), dict) else {}
    for regime_key in sorted(set(champion_regime_map) | set(candidate_regime_map)):
        champ_regime = champion_regime_map.get(regime_key) if isinstance(champion_regime_map.get(regime_key), dict) else None
        cand_regime = candidate_regime_map.get(regime_key) if isinstance(candidate_regime_map.get(regime_key), dict) else None
        if not champ_regime or not cand_regime:
            continue
        regime_rows[str(regime_key)] = {
            "champion_months_evaluated": int(champ_regime.get("months_evaluated") or 0),
            "candidate_months_evaluated": int(cand_regime.get("months_evaluated") or 0),
            "champion_mean_capture_rate": _safe_float(champ_regime.get("mean_capture_rate"), 0.0),
            "candidate_mean_capture_rate": _safe_float(cand_regime.get("mean_capture_rate"), 0.0),
            "monthly_top5_capture_delta": _safe_float(cand_regime.get("mean_capture_rate"), 0.0) - _safe_float(champ_regime.get("mean_capture_rate"), 0.0),
        }

    champion_mean = _safe_float(champion_summary.get("mean_capture_rate"), 0.0)
    candidate_mean = _safe_float(candidate_summary.get("mean_capture_rate"), 0.0)
    champion_median = _safe_float(champion_summary.get("median_capture_rate"), 0.0)
    candidate_median = _safe_float(candidate_summary.get("median_capture_rate"), 0.0)

    return {
        "schema_version": f"{SCHEMA_VERSION}_monthly_top5_capture_comparison_v1",
        "generated_at": _utc_now(),
        "available": bool(month_rows),
        "metric_definition": "intersection of monthly unions: realized forward_ret_20d top5 vs model ranking top5",
        "months_evaluated": len(month_rows),
        "champion_monthly_top5_capture": {
            "mean": champion_mean,
            "median": champion_median,
            "months_evaluated": int(champion_summary.get("months_evaluated") or 0),
            "zero_capture_months": int(champion_summary.get("zero_capture_months") or 0),
            "months": champion_summary.get("months") or [],
            "cannot_evaluate_months": champion_summary.get("cannot_evaluate_months") or [],
        },
        "candidate_monthly_top5_capture": {
            "mean": candidate_mean,
            "median": candidate_median,
            "months_evaluated": int(candidate_summary.get("months_evaluated") or 0),
            "zero_capture_months": int(candidate_summary.get("zero_capture_months") or 0),
            "months": candidate_summary.get("months") or [],
            "cannot_evaluate_months": candidate_summary.get("cannot_evaluate_months") or [],
        },
        "monthly_top5_capture_delta": {
            "mean": candidate_mean - champion_mean,
            "median": candidate_median - champion_median,
            "months_evaluated": len(month_rows),
            "zero_capture_months": zero_capture_months,
            "degraded_months": degraded_months,
            "improved_months": improved_months,
            "unchanged_months": unchanged_months,
            "months": month_rows,
            "skipped_months": skipped_months,
        },
        "regime_capture_summary": regime_rows,
        "zero_capture_months": zero_capture_months,
        "degraded_months": degraded_months,
        "improved_months": improved_months,
        "unchanged_months": unchanged_months,
        "skipped_months": skipped_months,
        "candidate_monthly_top5_capture_delta_mean": candidate_mean - champion_mean,
    }


def _classify_veto_reason(row: pd.Series, *, symbol_vol_median: float | None) -> tuple[bool, str, float, dict[str, Any]]:
    monthly = str(row.get("monthly_context") or "")
    weekly = str(row.get("weekly_context") or "")
    rank = _safe_int(row.get("champion_rank") or row.get("rank") or row.get("candidate_rank"), 0)
    vol_ratio = _safe_float(row.get("vol_ratio5_20"), 0.0)
    body_ratio = _safe_float(row.get("candle_body_ratio"), 0.0)
    gap_pct = _safe_float(row.get("gap_pct"), 0.0)
    overextended = _context_overextended(monthly, weekly)
    volume_deteriorated = vol_ratio < 0.85
    symbol_vol_abnormal = symbol_vol_median is not None and vol_ratio < symbol_vol_median * 0.90
    boundary_watch = rank >= 9
    risk_score = 0.0
    if overextended:
        risk_score += 0.38
    if volume_deteriorated:
        risk_score += 0.20
    if boundary_watch:
        risk_score += 0.08
    if symbol_vol_abnormal:
        risk_score += 0.10
    if body_ratio < 0.20 and volume_deteriorated:
        risk_score += 0.05
    if abs(gap_pct) > 0.01 and overextended:
        risk_score += 0.04

    veto_applied = bool(overextended and volume_deteriorated and boundary_watch and risk_score >= 0.60)
    reason_code = "top10_overextended_low_volume_veto" if veto_applied else "accepted_top20_champion_order"
    features = {
        "monthly_context": monthly,
        "weekly_context": weekly,
        "champion_rank": rank,
        "vol_ratio5_20": vol_ratio,
        "candle_body_ratio": body_ratio,
        "gap_pct": gap_pct,
        "symbol_vol_ratio_median": symbol_vol_median,
        "symbol_vol_ratio_delta": None if symbol_vol_median is None else float(vol_ratio - symbol_vol_median),
        "overextended_context": overextended,
        "volume_deteriorated": volume_deteriorated,
        "symbol_vol_abnormal": symbol_vol_abnormal,
        "boundary_watch": boundary_watch,
        "risk_score": risk_score,
        "threshold": 0.60,
    }
    return veto_applied, reason_code, risk_score, features


def _build_adjusted_group(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values(["champion_rank", "symbol"], kind="stable").copy()
    if group.empty:
        return group
    if "symbol_vol_ratio_median" not in group.columns:
        group["symbol_vol_ratio_median"] = None
    decisions: list[dict[str, Any]] = []
    for _, row in group.iterrows():
        veto_applied, reason_code, risk_score, features = _classify_veto_reason(row, symbol_vol_median=_safe_float(row.get("symbol_vol_ratio_median")) if pd.notna(row.get("symbol_vol_ratio_median")) else None)
        decisions.append(
            {
                "veto_applied": veto_applied,
                "veto_reason_code": reason_code,
                "veto_risk_score": risk_score,
                "veto_feature_values": features,
            }
        )
    decision_frame = pd.DataFrame(decisions, index=group.index)
    group = pd.concat([group, decision_frame], axis=1)
    keepers = group[~group["veto_applied"].fillna(False).astype(bool)].copy()
    vetoed = group[group["veto_applied"].fillna(False).astype(bool)].copy()
    adjusted = pd.concat([keepers.iloc[:10], vetoed, keepers.iloc[10:]], ignore_index=True)
    if adjusted.empty:
        return adjusted
    champion_boundary_before = _safe_float(group.iloc[min(9, len(group) - 1)]["champion_score"], 0.0) if "champion_score" in group.columns else _safe_float(group.iloc[min(9, len(group) - 1)]["score"], 0.0)
    next_boundary_before = _safe_float(group.iloc[min(10, len(group) - 1)]["champion_score"], champion_boundary_before) if len(group) > 10 else champion_boundary_before
    adjusted["original_rank"] = pd.to_numeric(adjusted["champion_rank"], errors="coerce").astype("Int64")
    adjusted["adjusted_rank"] = range(1, len(adjusted) + 1)
    adjusted["original_score"] = pd.to_numeric(adjusted["champion_score"] if "champion_score" in adjusted.columns else adjusted["score"], errors="coerce")
    adjusted["adjusted_score"] = adjusted["original_score"].astype(float)
    if "champion_score" not in adjusted.columns and "score" in adjusted.columns:
        adjusted["champion_score"] = adjusted["score"]
    if "veto_applied" not in adjusted.columns:
        adjusted["veto_applied"] = False
    if "veto_reason_code" not in adjusted.columns:
        adjusted["veto_reason_code"] = "accepted_top20_champion_order"
    adjusted.loc[adjusted["veto_applied"].fillna(False).astype(bool), "adjusted_score"] = max(next_boundary_before - 1e-6, champion_boundary_before - 1e-6)
    adjusted["topK_boundary_before"] = adjusted["veto_feature_values"].apply(
        lambda feature_values: {"top5_score": None, "top10_score": champion_boundary_before, "next_score": next_boundary_before}
    )
    adjusted_boundary_after = _safe_float(adjusted.iloc[min(9, len(adjusted) - 1)]["adjusted_score"], champion_boundary_before) if len(adjusted) else champion_boundary_before
    adjusted["topK_boundary_after"] = adjusted["veto_feature_values"].apply(
        lambda feature_values: {"top5_score": None, "top10_score": adjusted_boundary_after, "next_score": _safe_float(adjusted.iloc[min(10, len(adjusted) - 1)]["adjusted_score"], adjusted_boundary_after) if len(adjusted) > 10 else adjusted_boundary_after}
    )
    adjusted["candidate_rank"] = adjusted["original_rank"]
    adjusted["champion_selected_top5"] = adjusted["champion_selected_top5"].fillna(False).astype(bool)
    adjusted["champion_selected_top10"] = adjusted["champion_selected_top10"].fillna(False).astype(bool)
    adjusted["champion_selected_top20"] = adjusted["champion_selected_top20"].fillna(False).astype(bool)
    adjusted["candidate_selected_top5"] = adjusted["adjusted_rank"].le(5)
    adjusted["candidate_selected_top10"] = adjusted["adjusted_rank"].le(10)
    adjusted["candidate_selected_top20"] = adjusted["adjusted_rank"].le(20)
    adjusted["changed_top5_member"] = adjusted["champion_selected_top5"] != adjusted["candidate_selected_top5"]
    adjusted["changed_top10_member"] = adjusted["champion_selected_top10"] != adjusted["candidate_selected_top10"]
    adjusted["changed_top20_member"] = adjusted["champion_selected_top20"] != adjusted["candidate_selected_top20"]
    adjusted["selection_divergence_reason"] = adjusted.apply(
        lambda row: "top10_bad_pick_veto_replacement" if bool(row["veto_applied"]) else "champion_order_preserved",
        axis=1,
    )
    adjusted["original_score"] = pd.to_numeric(adjusted["original_score"], errors="coerce")
    adjusted["adjusted_score"] = pd.to_numeric(adjusted["adjusted_score"], errors="coerce")
    return adjusted


def _build_group_metrics(frame: pd.DataFrame, *, selected_column: str, champion_selected_column: str) -> dict[str, Any]:
    selected = frame[frame[selected_column].fillna(False).astype(bool)].copy()
    champion = frame[frame[champion_selected_column].fillna(False).astype(bool)].copy()
    out = {
        "selected_row_count": int(len(selected)),
        "champion_selected_row_count": int(len(champion)),
        "mean_forward_ret_20d": _safe_float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in selected.columns else None,
        "median_forward_ret_20d": _safe_float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").median()) if "forward_ret_20d" in selected.columns else None,
        "champion_mean_forward_ret_20d": _safe_float(pd.to_numeric(champion["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in champion.columns else None,
        "champion_median_forward_ret_20d": _safe_float(pd.to_numeric(champion["forward_ret_20d"], errors="coerce").median()) if "forward_ret_20d" in champion.columns else None,
        "top15_capture_rate": _safe_float(selected["top15_label"].fillna(False).astype(bool).mean()) if "top15_label" in selected.columns else None,
        "champion_top15_capture_rate": _safe_float(champion["top15_label"].fillna(False).astype(bool).mean()) if "top15_label" in champion.columns else None,
        "bottom15_contamination_rate": _safe_float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if "bottom15_label" in selected.columns else None,
        "champion_bottom15_contamination_rate": _safe_float(champion["bottom15_label"].fillna(False).astype(bool).mean()) if "bottom15_label" in champion.columns else None,
        "selected_zero_top15_count": int((selected["top15_label"].fillna(False).astype(bool) == False).all()) if "top15_label" in selected.columns and len(selected) else 0,  # noqa: E712
    }
    out["top15_capture_delta"] = None if out["top15_capture_rate"] is None or out["champion_top15_capture_rate"] is None else float(out["top15_capture_rate"] - out["champion_top15_capture_rate"])
    out["bottom15_contamination_delta"] = None if out["bottom15_contamination_rate"] is None or out["champion_bottom15_contamination_rate"] is None else float(out["bottom15_contamination_rate"] - out["champion_bottom15_contamination_rate"])
    out["mean_forward_ret_20d_delta"] = None if out["mean_forward_ret_20d"] is None or out["champion_mean_forward_ret_20d"] is None else float(out["mean_forward_ret_20d"] - out["champion_mean_forward_ret_20d"])
    out["median_forward_ret_20d_delta"] = None if out["median_forward_ret_20d"] is None or out["champion_median_forward_ret_20d"] is None else float(out["median_forward_ret_20d"] - out["champion_median_forward_ret_20d"])
    return out


def _group_boundary_gap(group: pd.DataFrame, *, score_column: str) -> dict[str, float | None]:
    ordered = group.sort_values(["adjusted_rank" if score_column == "adjusted_score" else "champion_rank", "symbol"], kind="stable").copy()
    if ordered.empty:
        return {"top5_boundary_score_gap": None, "top10_boundary_score_gap": None}
    score = pd.to_numeric(ordered[score_column], errors="coerce")
    top5_gap = None
    top10_gap = None
    if len(score) >= 6:
        top5_gap = _safe_float(score.iloc[4] - score.iloc[5])
    if len(score) >= 11:
        top10_gap = _safe_float(score.iloc[9] - score.iloc[10])
    return {"top5_boundary_score_gap": top5_gap, "top10_boundary_score_gap": top10_gap}


def _build_branching_probe(frame: pd.DataFrame) -> dict[str, Any]:
    changed_top5_members_count = int(frame["changed_top5_member"].fillna(False).astype(bool).sum())
    changed_top10_members_count = int(frame["changed_top10_member"].fillna(False).astype(bool).sum())
    changed_rank_count = int((frame["adjusted_rank"].astype("Int64") != frame["champion_rank"].astype("Int64")).sum())
    top5_gaps_before: list[float] = []
    top10_gaps_before: list[float] = []
    top5_gaps_after: list[float] = []
    top10_gaps_after: list[float] = []
    for (_, _), group in frame.groupby(["anchor_date", "side"], sort=False):
        before = _group_boundary_gap(group, score_column="champion_score")
        after = _group_boundary_gap(group, score_column="adjusted_score")
        if before["top5_boundary_score_gap"] is not None:
            top5_gaps_before.append(before["top5_boundary_score_gap"])
        if before["top10_boundary_score_gap"] is not None:
            top10_gaps_before.append(before["top10_boundary_score_gap"])
        if after["top5_boundary_score_gap"] is not None:
            top5_gaps_after.append(after["top5_boundary_score_gap"])
        if after["top10_boundary_score_gap"] is not None:
            top10_gaps_after.append(after["top10_boundary_score_gap"])
    return {
        "schema_version": BRANCHING_PROBE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "changed_top5_members_count": changed_top5_members_count,
        "changed_top10_members_count": changed_top10_members_count,
        "changed_rank_count": changed_rank_count,
        "top5_boundary_score_gap_before_mean": _mean_or_none(top5_gaps_before),
        "top5_boundary_score_gap_after_mean": _mean_or_none(top5_gaps_after),
        "top10_boundary_score_gap_before_mean": _mean_or_none(top10_gaps_before),
        "top10_boundary_score_gap_after_mean": _mean_or_none(top10_gaps_after),
        "selection_divergence_reason": "top10_bad_pick_veto_replacement" if changed_top10_members_count else "no_top10_veto_triggered",
        "meaningful_topk_branching_possible": bool(changed_top10_members_count > 0),
    }


def run_champion_topk_bad_pick_veto(
    *,
    source_rows_parquet: Path = DEFAULT_SOURCE_ROWS_PARQUET,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_output_root = output_root / run_id
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)

    frame = pd.read_parquet(source_rows_parquet)
    frame = _ensure_columns(frame)
    required_columns = {"anchor_date", "symbol", "side", "champion_selected_top20", "champion_rank", "score", "forward_ret_20d", "top15_label", "bottom15_label"}
    missing = sorted(column for column in required_columns if column not in frame.columns)
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")

    selected = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    selected["symbol_vol_ratio_median"] = _symbol_trailing_median(selected, "vol_ratio5_20") if "vol_ratio5_20" in selected.columns else None
    selected["symbol_body_ratio_median"] = _symbol_trailing_median(selected, "candle_body_ratio") if "candle_body_ratio" in selected.columns else None
    selected["symbol_gap_pct_median"] = _symbol_trailing_median(selected, "gap_pct") if "gap_pct" in selected.columns else None

    adjusted_groups: list[pd.DataFrame] = []
    for (anchor_date, side), group in selected.groupby(["anchor_date", "side"], sort=False):
        adjusted_groups.append(_build_adjusted_group(group))
    adjusted = pd.concat(adjusted_groups, ignore_index=True) if adjusted_groups else selected.iloc[0:0].copy()
    adjusted = _ensure_columns(adjusted)

    compare_rows = adjusted.copy()
    compare_rows["champion_selected_top5"] = compare_rows["champion_selected_top5"].fillna(False).astype(bool)
    compare_rows["champion_selected_top10"] = compare_rows["champion_selected_top10"].fillna(False).astype(bool)
    compare_rows["champion_selected_top20"] = compare_rows["champion_selected_top20"].fillna(False).astype(bool)
    compare_rows["candidate_selected_top5"] = compare_rows["candidate_selected_top5"].fillna(False).astype(bool)
    compare_rows["candidate_selected_top10"] = compare_rows["candidate_selected_top10"].fillna(False).astype(bool)
    compare_rows["candidate_selected_top20"] = compare_rows["candidate_selected_top20"].fillna(False).astype(bool)
    compare_rows["month_bucket"] = compare_rows["anchor_date"].map(_month_bucket_from_anchor_date)

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

    candidate_top10 = topk_effectiveness["10"]["candidate"]
    champion_top10 = topk_effectiveness["10"]["champion"]
    candidate_top5 = topk_effectiveness["5"]["candidate"]
    champion_top5 = topk_effectiveness["5"]["champion"]
    candidate_top20 = topk_effectiveness["20"]["candidate"]
    champion_top20 = topk_effectiveness["20"]["champion"]

    bad_pick_removal_count = int((compare_rows["veto_applied"].fillna(False).astype(bool) & compare_rows["bottom15_label"].fillna(False).astype(bool)).sum())
    bad_pick_removed_good_count = int((compare_rows["veto_applied"].fillna(False).astype(bool) & compare_rows["top15_label"].fillna(False).astype(bool)).sum())
    turnover_change_rate = None if len(compare_rows) == 0 else float(branching_probe["changed_top10_members_count"] / max(int(compare_rows["champion_selected_top10"].fillna(False).astype(bool).sum()), 1))
    zero_pass_or_negative_window_count = int(compare_rows.groupby(["anchor_date", "side"])["forward_ret_20d"].mean().fillna(0.0).lt(0).sum())

    regime_summary: dict[str, dict[str, Any]] = {}
    if "family_regime_context" in compare_rows.columns:
        for regime, group in compare_rows.groupby(compare_rows["family_regime_context"].astype(str), sort=True):
            candidate = _build_group_metrics(group, selected_column="candidate_selected_top10", champion_selected_column="champion_selected_top10")
            regime_summary[str(regime)] = {
                "sample_count": int(len(group)),
                "candidate_mean_forward_ret_20d_delta": candidate["mean_forward_ret_20d_delta"],
                "candidate_median_forward_ret_20d_delta": candidate["median_forward_ret_20d_delta"],
                "candidate_top15_capture_delta": candidate["top15_capture_delta"],
                "candidate_bottom15_contamination_delta": candidate["bottom15_contamination_delta"],
            }

    turnover_summary = {
        "schema_version": TURNOVER_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "turnover_change_rate": turnover_change_rate,
        "changed_top5_members_count": branching_probe["changed_top5_members_count"],
        "changed_top10_members_count": branching_probe["changed_top10_members_count"],
        "changed_rank_count": branching_probe["changed_rank_count"],
        "top5_overlap_ratio": None if len(compare_rows) == 0 else float(1.0 - branching_probe["changed_top5_members_count"] / max(int(compare_rows["champion_selected_top5"].fillna(False).astype(bool).sum()), 1)),
        "top10_overlap_ratio": None if len(compare_rows) == 0 else float(1.0 - branching_probe["changed_top10_members_count"] / max(int(compare_rows["champion_selected_top10"].fillna(False).astype(bool).sum()), 1)),
        "top20_overlap_ratio": None if len(compare_rows) == 0 else float(1.0 - int(compare_rows["changed_top20_member"].fillna(False).astype(bool).sum()) / max(int(compare_rows["champion_selected_top20"].fillna(False).astype(bool).sum()), 1)),
        "zero_pass_or_negative_window_count": zero_pass_or_negative_window_count,
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
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "silent_fallback_allowed": False,
            "ret20_source_mode": "forward_ret_20d",
            "candidate_build_order_mode": "champion_rank_preserve_then_narrow_top10_veto",
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

    bad_pick_summary = {
        "schema_version": BAD_PICK_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "veto_applied_count": int(compare_rows["veto_applied"].fillna(False).astype(bool).sum()),
        "bad_pick_removal_count": bad_pick_removal_count,
        "bad_pick_removed_good_count": bad_pick_removed_good_count,
        "bad_pick_precision": None
        if bad_pick_removal_count + bad_pick_removed_good_count == 0
        else float(bad_pick_removal_count / (bad_pick_removal_count + bad_pick_removed_good_count)),
        "top10_veto_rows": int((compare_rows["veto_applied"].fillna(False).astype(bool) & compare_rows["champion_selected_top10"].fillna(False).astype(bool)).sum()),
        "top10_boundary_score_gap_before_mean": branching_probe["top10_boundary_score_gap_before_mean"],
        "top10_boundary_score_gap_after_mean": branching_probe["top10_boundary_score_gap_after_mean"],
        "top5_boundary_score_gap_before_mean": branching_probe["top5_boundary_score_gap_before_mean"],
        "top5_boundary_score_gap_after_mean": branching_probe["top5_boundary_score_gap_after_mean"],
    }

    effect_summary = {
        "schema_version": EFFECTIVENESS_SCHEMA_VERSION,
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
        "zero_pass_or_negative_window_count": zero_pass_or_negative_window_count,
        "cost_slippage_mode": "flat_zero_cost",
    }

    regime_summary_payload = {
        "schema_version": REGIME_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "regime_split_key": "family_regime_context",
        "rows": regime_summary,
    }

    monthly_capture_delta = _safe_float(monthly_capture_summary.get("monthly_top5_capture_delta", {}).get("mean") if isinstance(monthly_capture_summary.get("monthly_top5_capture_delta"), dict) else None, 0.0)
    monthly_capture_candidate_mean = _safe_float((monthly_capture_summary.get("candidate_monthly_top5_capture") or {}).get("mean"), 0.0)
    monthly_capture_champion_mean = _safe_float((monthly_capture_summary.get("champion_monthly_top5_capture") or {}).get("mean"), 0.0)
    monthly_capture_months_evaluated = int(monthly_capture_summary.get("months_evaluated") or 0)
    monthly_capture_zero_capture_months = int(monthly_capture_summary.get("zero_capture_months") or 0)
    monthly_capture_improved_months = int(monthly_capture_summary.get("improved_months") or 0)
    monthly_capture_degraded_months = int(monthly_capture_summary.get("degraded_months") or 0)
    monthly_capture_unchanged_months = int(monthly_capture_summary.get("unchanged_months") or 0)

    decision = "drop"
    decision_reason = "monthly_capture_not_improved_and_top10_ret20_negative"
    if branching_probe["changed_top10_members_count"] <= 0:
        decision = "drop"
        decision_reason = "no_real_top10_branching"
    elif bad_pick_removal_count <= 0:
        decision = "drop"
        decision_reason = "bad_pick_removal_failed"
    elif monthly_capture_delta <= 0:
        decision = "drop"
        decision_reason = "monthly_top5_capture_not_improved"
    elif candidate_top10["mean_forward_ret_20d_delta"] is not None and candidate_top10["mean_forward_ret_20d_delta"] < 0:
        decision = "drop"
        decision_reason = "top10_expected_return_degraded"
    elif candidate_top10["median_forward_ret_20d_delta"] is not None and candidate_top10["median_forward_ret_20d_delta"] < 0:
        decision = "drop"
        decision_reason = "top10_median_ret20_degraded"
    elif candidate_top10["bottom15_contamination_delta"] is not None and candidate_top10["bottom15_contamination_delta"] > 0:
        decision = "drop"
        decision_reason = "bottom15_contamination_worsened"
    elif candidate_top10["mean_forward_ret_20d_delta"] is not None and candidate_top10["mean_forward_ret_20d_delta"] >= 0 and candidate_top10["bottom15_contamination_delta"] is not None and candidate_top10["bottom15_contamination_delta"] < 0:
        if monthly_capture_months_evaluated < 3:
            decision = "hold"
            decision_reason = "monthly_capture_signal_present_but_month_coverage_insufficient"
        else:
            decision = "keep"
            decision_reason = "monthly_capture_improved_with_narrow_branching"

    compare["candidate_local_decision"] = decision
    compare["session_aggregate_decision"] = decision
    compare["authoritative_rollup_decision"] = decision
    compare["decision_reason"] = decision_reason

    reflectability = {
        "schema_version": REFLECTABILITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "is_reflectable_to_meemee_now": False,
        "reflectability_state": "not_reflectable",
        "suitable_for": "analysis_marker_only" if decision == "hold" else "drop_freeze",
        "blockers": [
            "monthly_top5_capture_not_improved",
            "top10_expected_return_degraded",
            "no_meemee_registration_or_publish_path_changed",
            "research_only_candidate_surface",
        ],
        "artifact_proof": {
            "branching_probe": str(session_output_root / "branching_probe.json"),
            "compare": str(session_output_root / "compare.json"),
            "decision": str(session_output_root / "decision_summary.json"),
            "monthly_top5_capture_summary": str(session_output_root / "monthly_top5_capture_summary.json"),
        },
        "what_must_remain_hidden_from_meemee": [
            "raw research-only veto features",
            "intermediate compare diagnostics",
            "artifact scan inventory details",
        ],
    }

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "champion_topk_bad_pick_veto_v1",
        "family_id": "tradex_champion_topk_bad_pick_veto_v1",
        "source_rows_parquet": str(Path(source_rows_parquet).resolve()),
        "output_root": str(session_output_root.resolve()),
        "session_output_root": str(session_output_root.resolve()),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "same_condition_contract": compare["same_condition_contract"],
        "veto_rule": {
            "description": "Top10-only veto for overextended low-volume candidates with optional symbol-normalized volume abnormality.",
            "threshold": 0.60,
            "build_order_mode": "champion_rank_preserve_then_narrow_top10_veto",
            "feature_families": {
                "common_feature": ["monthly_context", "weekly_context", "vol_ratio5_20", "candle_body_ratio", "gap_pct"],
                "regime_adjustment": ["monthly_context", "weekly_context"],
                "boundary_feature": ["champion_rank"],
                "bad_pick_removal": ["overextended_context", "volume_deteriorated"],
                "symbol_specific_deviation": ["symbol_vol_ratio_median"],
            },
        },
    }

    artifact_paths = {
        "candidate_manifest.json": _write_json(session_output_root / "candidate_manifest.json", manifest),
        "evaluation_contract.json": _write_json(session_output_root / "evaluation_contract.json", {
            "schema_version": EVALUATION_CONTRACT_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "same_condition_contract": compare["same_condition_contract"],
            "no_silent_fallback": True,
            "no_meemee_reflection": True,
            "topk_values": list(TOP_K_VALUES),
            "ret20_source_mode": "forward_ret_20d",
            "candidate_build_order_mode": "champion_rank_preserve_then_narrow_top10_veto",
            "monthly_capture_method": monthly_capture_summary["metric_definition"],
        }),
        "branching_probe.json": _write_json(session_output_root / "branching_probe.json", branching_probe),
        "compare.json": _write_json(session_output_root / "compare.json", compare),
        "monthly_top5_capture_summary.json": _write_json(session_output_root / "monthly_top5_capture_summary.json", monthly_capture_summary),
        "bad_pick_removal_summary.json": _write_json(session_output_root / "bad_pick_removal_summary.json", bad_pick_summary),
        "topk_effectiveness_summary.json": _write_json(session_output_root / "topk_effectiveness_summary.json", effect_summary),
        "regime_split_summary.json": _write_json(session_output_root / "regime_split_summary.json", regime_summary_payload),
        "turnover_summary.json": _write_json(session_output_root / "turnover_summary.json", turnover_summary),
        "decision_summary.json": _write_json(session_output_root / "decision_summary.json", {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "decision": decision,
            "decision_reason": decision_reason,
            "candidate_id": "champion_topk_bad_pick_veto_v1",
            "authoritative_artifact": str(session_output_root / "compare.json"),
        }),
        "meemee_reflectability_assessment.json": _write_json(session_output_root / "meemee_reflectability_assessment.json", reflectability),
    }

    report_lines = [
        "# champion_topk_bad_pick_veto_v1",
        "",
        f"- decision: {decision}",
        f"- reason: {decision_reason}",
        f"- changed_top10_members_count: {branching_probe['changed_top10_members_count']}",
        f"- bad_pick_removal_count: {bad_pick_removal_count}",
        f"- top10_mean_ret20_delta: {candidate_top10['mean_forward_ret_20d_delta']}",
        f"- top10_bottom15_contamination_delta: {candidate_top10['bottom15_contamination_delta']}",
        f"- monthly_top5_capture_delta_mean: {monthly_capture_delta}",
        f"- monthly_top5_capture_months_evaluated: {monthly_capture_months_evaluated}",
        f"- monthly_top5_capture_zero_months: {monthly_capture_zero_capture_months}",
        "",
        "JSON artifacts are authoritative.",
    ]
    report_path = session_output_root / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    artifact_paths["report.md"] = report_path

    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "artifact_count": len(artifact_paths) + 1,
        "artifacts": sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        "decision": decision,
    }
    _write_json(session_output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = session_output_root / "_ARTIFACT_COMPLETE.json"

    return {
        "ok": True,
        "decision": decision,
        "decision_reason": decision_reason,
        "run_id": run_id,
        "output_root": str(session_output_root.resolve()),
        "session_output_root": str(session_output_root.resolve()),
        "paths": {key: str(value) for key, value in artifact_paths.items()},
        "compare": compare,
        "branching_probe": branching_probe,
        "bad_pick_removal_summary": bad_pick_summary,
        "topk_effectiveness_summary": effect_summary,
        "monthly_top5_capture_summary": monthly_capture_summary,
        "regime_split_summary": regime_summary_payload,
        "turnover_summary": turnover_summary,
        "meemee_reflectability_assessment": reflectability,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run champion topK bad-pick veto validation.")
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    payload = run_champion_topk_bad_pick_veto(
        source_rows_parquet=_safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
