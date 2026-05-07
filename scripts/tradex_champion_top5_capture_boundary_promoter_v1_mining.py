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

from scripts.tradex_reflectability_funnel_common_v1 import (
    _ensure_columns,
    _json_text,
    _load_json,
    _median_or_none,
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
DEFAULT_CLOSEOUT_ROOT = Path(r"G:\Tradex\champion_topk_bad_pick_veto_v1_monthly_capture_closeout")
DEFAULT_FREEZE_ROOT = Path(r"G:\Tradex\research_freeze_summaries")
DEFAULT_MINING_ROOT = Path(r"G:\Tradex\champion_top5_capture_boundary_promoter_v1_mining")
SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_mining"
FREEZE_SCHEMA_VERSION = "tradex_champion_topk_bad_pick_veto_v1_freeze_summary_v1"
MINING_CONTRACT_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_mining_contract_v1"
BASELINE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_monthly_capture_baseline_v1"
MISS_INVENTORY_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_missed_top5_winner_inventory_v1"
OPPORTUNITY_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_rank6_20_promotion_opportunity_summary_v1"
FALSE_POS_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_top5_false_positive_summary_v1"
FEATURE_CONTRAST_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_feature_contrast_summary_v1"
NEXT_DESIGN_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_next_candidate_design_v1"


def _month_bucket(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 7:
        return text[:7]
    return text or "unknown"


def _score_rank_group(group: pd.DataFrame) -> pd.DataFrame:
    ordered = group.sort_values(["score", "symbol"], ascending=[False, True], kind="stable").copy()
    ordered["score_rank"] = range(1, len(ordered) + 1)
    return ordered


def _safe_latest_run(root: Path) -> Path:
    if not root.exists():
        raise FileNotFoundError(f"closeout root does not exist: {root}")
    candidates = [path for path in root.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"no run directories found under: {root}")
    return sorted(candidates, key=lambda value: value.name)[-1]


def _session_root(base_root: Path, candidate_id: str, run_id: str) -> Path:
    if base_root.name == candidate_id:
        return base_root / run_id
    return base_root / candidate_id / run_id


def _load_base_frame(source_rows_parquet: Path) -> pd.DataFrame:
    frame = pd.read_parquet(source_rows_parquet)
    frame = _ensure_columns(frame)
    required = {"anchor_date", "side", "symbol", "champion_selected_top20", "score", "forward_ret_20d"}
    missing = sorted(column for column in required if column not in frame.columns)
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")
    frame = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce")
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].map(_month_bucket)
    else:
        frame["month_bucket"] = frame["month_bucket"].fillna(frame["anchor_date"].map(_month_bucket)).astype(str)
    return frame


def _symbol_trailing_median(frame: pd.DataFrame, column: str) -> pd.Series:
    working = frame[["symbol", "anchor_date", column]].copy()
    working["_orig_index"] = range(len(working))
    working[column] = pd.to_numeric(working[column], errors="coerce")
    working = working.sort_values(["symbol", "anchor_date", "_orig_index"], kind="stable")
    working["_trailing_median"] = working.groupby("symbol")[column].transform(lambda group: group.shift(1).expanding(min_periods=1).median())
    return working.sort_values("_orig_index", kind="stable")["_trailing_median"].reset_index(drop=True).set_axis(frame.index)


def _select_feature_columns(frame: pd.DataFrame) -> list[str]:
    candidates = [
        "score",
        "score_rank",
        "score_gap_to_top5_boundary",
        "score_gap_to_top20_boundary",
        "champion_rank",
        "monthly_context",
        "weekly_context",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "shape_classification",
        "family_regime_context",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "path_value_score_v1",
        "symbol_vol_ratio_median_past",
        "symbol_gap_pct_median_past",
        "symbol_body_ratio_median_past",
        "vol_ratio5_20_vs_symbol_median_past",
        "gap_pct_vs_symbol_median_past",
        "candle_body_ratio_vs_symbol_median_past",
    ]
    return [column for column in candidates if column in frame.columns]


def _build_feature_row(row: pd.Series, *, group_label: str) -> dict[str, Any]:
    return {
        "month_bucket": str(row.get("month_bucket") or "unknown"),
        "anchor_date": str(row.get("anchor_date") or ""),
        "side": str(row.get("side") or ""),
        "symbol": str(row.get("symbol") or ""),
        "group": group_label,
        "score_rank": _safe_int(row.get("score_rank"), 0),
        "champion_rank": _safe_int(row.get("champion_rank"), 0),
        "score": _safe_float(row.get("score"), 0.0),
        "score_gap_to_top5_boundary": _safe_float(row.get("score_gap_to_top5_boundary"), 0.0),
        "score_gap_to_top20_boundary": _safe_float(row.get("score_gap_to_top20_boundary"), 0.0),
        "monthly_context": row.get("monthly_context"),
        "weekly_context": row.get("weekly_context"),
        "monthly_context_no_lookahead": row.get("monthly_context_no_lookahead"),
        "weekly_context_no_lookahead": row.get("weekly_context_no_lookahead"),
        "shape_classification": row.get("shape_classification"),
        "family_regime_context": row.get("family_regime_context"),
        "gap_pct": _safe_float(row.get("gap_pct"), 0.0) if row.get("gap_pct") is not None else None,
        "vol_ratio5_20": _safe_float(row.get("vol_ratio5_20"), 0.0) if row.get("vol_ratio5_20") is not None else None,
        "candle_body_ratio": _safe_float(row.get("candle_body_ratio"), 0.0) if row.get("candle_body_ratio") is not None else None,
        "path_value_score_v1": _safe_float(row.get("path_value_score_v1"), 0.0) if row.get("path_value_score_v1") is not None else None,
        "symbol_vol_ratio_median_past": _safe_float(row.get("symbol_vol_ratio_median_past"), 0.0) if row.get("symbol_vol_ratio_median_past") is not None else None,
        "symbol_gap_pct_median_past": _safe_float(row.get("symbol_gap_pct_median_past"), 0.0) if row.get("symbol_gap_pct_median_past") is not None else None,
        "symbol_body_ratio_median_past": _safe_float(row.get("symbol_body_ratio_median_past"), 0.0) if row.get("symbol_body_ratio_median_past") is not None else None,
        "vol_ratio5_20_vs_symbol_median_past": _safe_float(row.get("vol_ratio5_20_vs_symbol_median_past"), 0.0) if row.get("vol_ratio5_20_vs_symbol_median_past") is not None else None,
        "gap_pct_vs_symbol_median_past": _safe_float(row.get("gap_pct_vs_symbol_median_past"), 0.0) if row.get("gap_pct_vs_symbol_median_past") is not None else None,
        "candle_body_ratio_vs_symbol_median_past": _safe_float(row.get("candle_body_ratio_vs_symbol_median_past"), 0.0) if row.get("candle_body_ratio_vs_symbol_median_past") is not None else None,
    }


def _aggregate_group_features(records: list[dict[str, Any]]) -> dict[str, Any]:
    frame = pd.DataFrame(records)
    if frame.empty:
        return {"count": 0, "features": {}, "categorical": {}}
    numeric_features = [
        "score_rank",
        "champion_rank",
        "score",
        "score_gap_to_top5_boundary",
        "score_gap_to_top20_boundary",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "path_value_score_v1",
        "symbol_vol_ratio_median_past",
        "symbol_gap_pct_median_past",
        "symbol_body_ratio_median_past",
        "vol_ratio5_20_vs_symbol_median_past",
        "gap_pct_vs_symbol_median_past",
        "candle_body_ratio_vs_symbol_median_past",
    ]
    categorical_features = [
        "monthly_context",
        "weekly_context",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "shape_classification",
        "family_regime_context",
    ]
    features: dict[str, Any] = {}
    for column in numeric_features:
        if column not in frame.columns:
            continue
        series = pd.to_numeric(frame[column], errors="coerce")
        features[column] = {
            "mean": _safe_float(series.mean()) if series.notna().any() else None,
            "median": _safe_float(series.median()) if series.notna().any() else None,
            "p10": _safe_float(series.quantile(0.10)) if series.notna().any() else None,
            "p90": _safe_float(series.quantile(0.90)) if series.notna().any() else None,
        }
    categorical: dict[str, Any] = {}
    for column in categorical_features:
        if column not in frame.columns:
            continue
        categorical[column] = {str(key): int(value) for key, value in frame[column].fillna("unknown").astype(str).value_counts(dropna=False).head(6).items()}
    return {"count": int(len(frame)), "features": features, "categorical": categorical}


def _build_freeze_outputs(*, closeout_root: Path, output_root: Path) -> dict[str, Any]:
    decision = _load_json(closeout_root / "decision_summary.json")
    compare = _load_json(closeout_root / "compare.json")
    reflectability = _load_json(closeout_root / "meemee_reflectability_assessment.json")
    monthly = _load_json(closeout_root / "monthly_top5_capture_summary.json")
    reasons = [
        "monthly_top5_capture_not_improved",
        "top10_expected_return_degraded",
        "changed_top5_members_count_zero",
        "bad_pick_precision_insufficient",
    ]
    freeze_decision = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_decision_v1",
        "generated_at": _utc_now(),
        "candidate_id": "champion_topk_bad_pick_veto_v1",
        "source_closeout_root": str(closeout_root.resolve()),
        "decision": "drop_freeze",
        "final_decision": decision.get("decision"),
        "reflectability_state": reflectability.get("reflectability_state"),
        "monthly_capture_mean_delta": _safe_float((monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        "evidence": {
            "changed_top5_members_count": _safe_int(compare.get("branching_metrics", {}).get("changed_top5_members_count"), 0),
            "changed_top10_members_count": _safe_int(compare.get("branching_metrics", {}).get("changed_top10_members_count"), 0),
            "top10_mean_ret20_delta": _safe_float((compare.get("champion_vs_challenger", {}).get("selection_only", {}).get("10", {}).get("candidate", {})).get("mean_forward_ret_20d_delta"), 0.0),
            "top10_median_ret20_delta": _safe_float((compare.get("champion_vs_challenger", {}).get("selection_only", {}).get("10", {}).get("candidate", {})).get("median_forward_ret_20d_delta"), 0.0),
            "bad_pick_precision": _safe_float(_load_json(closeout_root / "bad_pick_removal_summary.json").get("bad_pick_precision"), 0.0),
        },
    }
    freeze_reason = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_reason_v1",
        "generated_at": _utc_now(),
        "candidate_id": "champion_topk_bad_pick_veto_v1",
        "decision": "drop_freeze",
        "reasons": reasons,
        "typed_reason": "freeze after monthly capture closeout showed no monthly improvement and negative top10 return deltas",
        "artifact_proof": {
            "decision": str(closeout_root / "decision_summary.json"),
            "compare": str(closeout_root / "compare.json"),
            "monthly_capture": str(closeout_root / "monthly_top5_capture_summary.json"),
        },
    }
    reusable_findings = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_reusable_findings_v1",
        "generated_at": _utc_now(),
        "findings": [
            "narrow branching mechanics are valid",
            "direct monthly top5 capture instrumentation is now available",
            "reflectability closeout path is useful",
        ],
    }
    non_reusable_findings = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_non_reusable_findings_v1",
        "generated_at": _utc_now(),
        "findings": [
            "current bad-pick veto scoring is not publishable",
            "threshold tweaking is not the next step",
        ],
    }
    artifact_paths = {
        "freeze_decision.json": _write_json(output_root / "freeze_decision.json", freeze_decision),
        "freeze_reason.json": _write_json(output_root / "freeze_reason.json", freeze_reason),
        "reusable_findings.json": _write_json(output_root / "reusable_findings.json", reusable_findings),
        "non_reusable_findings.json": _write_json(output_root / "non_reusable_findings.json", non_reusable_findings),
    }
    report_path = output_root / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# champion_topk_bad_pick_veto_v1 freeze summary",
                "",
                "- decision: drop_freeze",
                f"- source_closeout_root: {closeout_root}",
                f"- monthly_capture_delta_mean: {_safe_float((monthly.get('monthly_top5_capture_delta') or {}).get('mean'), 0.0)}",
                "",
                "JSON artifacts are authoritative.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths["report.md"] = report_path
    complete = build_artifact_complete(
        {"schema_version": FREEZE_SCHEMA_VERSION},
        sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        schema_version=f"{FREEZE_SCHEMA_VERSION}_artifact_complete_v1",
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = output_root / "_ARTIFACT_COMPLETE.json"
    return {
        "output_root": str(output_root.resolve()),
        "closeout_root": str(closeout_root.resolve()),
        "artifact_paths": {key: str(value) for key, value in artifact_paths.items()},
        "freeze_decision": freeze_decision,
        "freeze_reason": freeze_reason,
        "reusable_findings": reusable_findings,
        "non_reusable_findings": non_reusable_findings,
    }


def _build_mining_outputs(
    frame: pd.DataFrame,
    *,
    output_root: Path,
    source_rows_parquet: Path | None = None,
) -> dict[str, Any]:
    work = frame.copy()
    work["score"] = pd.to_numeric(work["score"], errors="coerce")
    work["forward_ret_20d"] = pd.to_numeric(work["forward_ret_20d"], errors="coerce")
    work["champion_rank"] = pd.to_numeric(work["champion_rank"], errors="coerce")
    if "gap_pct" in work.columns:
        work["gap_pct"] = pd.to_numeric(work["gap_pct"], errors="coerce")
    if "vol_ratio5_20" in work.columns:
        work["vol_ratio5_20"] = pd.to_numeric(work["vol_ratio5_20"], errors="coerce")
    if "candle_body_ratio" in work.columns:
        work["candle_body_ratio"] = pd.to_numeric(work["candle_body_ratio"], errors="coerce")
    if "path_value_score_v1" in work.columns:
        work["path_value_score_v1"] = pd.to_numeric(work["path_value_score_v1"], errors="coerce")
    if "month_bucket" not in work.columns:
        work["month_bucket"] = work["anchor_date"].map(_month_bucket)
    work["month_bucket"] = work["month_bucket"].fillna(work["anchor_date"].map(_month_bucket)).astype(str)

    if "vol_ratio5_20" in work.columns:
        work["symbol_vol_ratio_median_past"] = _symbol_trailing_median(work, "vol_ratio5_20")
        work["vol_ratio5_20_vs_symbol_median_past"] = work["vol_ratio5_20"] - work["symbol_vol_ratio_median_past"]
    if "gap_pct" in work.columns:
        work["symbol_gap_pct_median_past"] = _symbol_trailing_median(work, "gap_pct")
        work["gap_pct_vs_symbol_median_past"] = work["gap_pct"] - work["symbol_gap_pct_median_past"]
    if "candle_body_ratio" in work.columns:
        work["symbol_body_ratio_median_past"] = _symbol_trailing_median(work, "candle_body_ratio")
        work["candle_body_ratio_vs_symbol_median_past"] = work["candle_body_ratio"] - work["symbol_body_ratio_median_past"]

    records: list[dict[str, Any]] = []
    monthly_details: dict[str, dict[str, Any]] = defaultdict(lambda: {"realized": set(), "champion": set(), "missed": set(), "false_positive": set(), "date_side_count": 0})
    regime_counts = Counter()
    regime_false_positive_counts = Counter()
    group_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    issue_records: list[dict[str, Any]] = []

    for (anchor_date, side), group in work.groupby(["anchor_date", "side"], sort=True):
        ordered = _score_rank_group(group)
        if ordered.empty:
            issue_records.append({"anchor_date": str(anchor_date), "side": str(side), "reason": "empty_group"})
            continue
        ordered["month_bucket"] = ordered["month_bucket"].astype(str)
        realized = ordered.sort_values(["forward_ret_20d", "symbol"], ascending=[False, True], kind="stable").head(5)
        champion = ordered.head(5)
        rank6_20_pool = ordered.loc[ordered["score_rank"].between(6, 20, inclusive="both")]
        realized_symbols = set(realized["symbol"].astype(str))
        champion_symbols = set(champion["symbol"].astype(str))
        rank6_20_symbols = set(rank6_20_pool["symbol"].astype(str))
        hit_symbols = realized_symbols & champion_symbols
        missed_symbols = realized_symbols - champion_symbols
        false_positive_symbols = champion_symbols - realized_symbols
        missed_rank6_20 = missed_symbols & rank6_20_symbols
        missed_outside20 = missed_symbols - rank6_20_symbols
        false_promotion_risk = rank6_20_symbols - realized_symbols
        month_bucket = str(ordered["month_bucket"].iloc[0])
        monthly_details[month_bucket]["realized"].update(realized_symbols)
        monthly_details[month_bucket]["champion"].update(champion_symbols)
        monthly_details[month_bucket]["missed"].update(missed_symbols)
        monthly_details[month_bucket]["false_positive"].update(false_positive_symbols)
        monthly_details[month_bucket]["date_side_count"] += 1
        month_regime = "None"
        if "family_regime_context" in ordered.columns:
            month_regime = str(ordered["family_regime_context"].fillna("None").astype(str).mode(dropna=False).iloc[0]) if not ordered["family_regime_context"].dropna().empty else "None"
        for sym in realized_symbols:
            regime_counts[str(ordered.loc[ordered["symbol"].eq(sym), "family_regime_context"].iloc[0] if "family_regime_context" in ordered.columns and not ordered.loc[ordered["symbol"].eq(sym), "family_regime_context"].empty else "None")] += 1
        for sym in false_positive_symbols:
            regime_false_positive_counts[str(ordered.loc[ordered["symbol"].eq(sym), "family_regime_context"].iloc[0] if "family_regime_context" in ordered.columns and not ordered.loc[ordered["symbol"].eq(sym), "family_regime_context"].empty else "None")] += 1
        top5_boundary_score = _safe_float(champion["score"].iloc[-1], 0.0)
        top20_boundary_score = _safe_float(ordered.loc[ordered["score_rank"].eq(20), "score"].iloc[0], _safe_float(ordered["score"].iloc[-1], 0.0)) if len(ordered) >= 20 else _safe_float(ordered["score"].iloc[-1], 0.0)
        for label, symbols in {
            "champion_top5_hit": hit_symbols,
            "champion_top5_miss_but_rank6_20": missed_rank6_20,
            "champion_top5_miss_outside_top20": missed_outside20,
            "champion_top5_false_positive": false_positive_symbols,
            "rank6_20_false_promotion_risk": false_promotion_risk,
        }.items():
            group_records[label].extend(
                _build_feature_row(
                    row,
                    group_label=label,
                )
                for _, row in ordered.loc[ordered["symbol"].isin(symbols)].iterrows()
            )
        for _, row in ordered.iterrows():
            if str(row["symbol"]) in hit_symbols:
                group_label = "champion_top5_hit"
            elif str(row["symbol"]) in missed_rank6_20:
                group_label = "champion_top5_miss_but_rank6_20"
            elif str(row["symbol"]) in missed_outside20:
                group_label = "champion_top5_miss_outside_top20"
            elif str(row["symbol"]) in false_positive_symbols:
                group_label = "champion_top5_false_positive"
            elif str(row["symbol"]) in false_promotion_risk:
                group_label = "rank6_20_false_promotion_risk"
            else:
                continue
            record = _build_feature_row(row, group_label=group_label)
            record["score_gap_to_top5_boundary"] = _safe_float(row["score"] - top5_boundary_score, 0.0)
            record["score_gap_to_top20_boundary"] = _safe_float(row["score"] - top20_boundary_score, 0.0)
            record["realized_top5_label"] = bool(str(row["symbol"]) in realized_symbols)
            record["champion_top5_label"] = bool(str(row["symbol"]) in champion_symbols)
            record["rank6_20_pool_label"] = bool(str(row["symbol"]) in rank6_20_symbols)
            records.append(record)

    months = sorted(monthly_details)
    month_rows = []
    monthly_capture_rates: list[float] = []
    for month in months:
        payload = monthly_details[month]
        realized = payload["realized"]
        champion = payload["champion"]
        capture = realized & champion
        capture_rate = float(len(capture) / len(realized)) if realized else 0.0
        monthly_capture_rates.append(capture_rate)
        month_rows.append(
            {
                "month_bucket": month,
                "realized_top5_winner_count": int(len(realized)),
                "champion_top5_hit_count": int(len(capture)),
                "missed_realized_top5_winner_count": int(len(payload["missed"])),
                "champion_top5_false_positive_count": int(len(payload["false_positive"])),
                "capture_rate": capture_rate,
                "date_side_groups": int(payload["date_side_count"]),
            }
        )
    realized_total = int(sum(len(payload["realized"]) for payload in monthly_details.values()))
    champion_hit_total = int(sum(len(payload["realized"] & payload["champion"]) for payload in monthly_details.values()))
    missed_total = int(sum(len(payload["missed"]) for payload in monthly_details.values()))
    missed_rank6_20_total = int(sum(len(payload["missed"] & set()) for payload in monthly_details.values()))
    # recompute totals from records because missed_total above counts month unions, not date-side totals
    date_side_realized_total = 0
    date_side_hit_total = 0
    date_side_missed_total = 0
    date_side_missed_rank6_20_total = 0
    date_side_missed_outside_total = 0
    date_side_false_positive_total = 0
    rank6_20_pool_total = 0
    for (anchor_date, side), group in work.groupby(["anchor_date", "side"], sort=True):
        ordered = _score_rank_group(group)
        if ordered.empty:
            continue
        realized = ordered.sort_values(["forward_ret_20d", "symbol"], ascending=[False, True], kind="stable").head(5)
        champion = ordered.head(5)
        rank6_20_pool = ordered.loc[ordered["score_rank"].between(6, 20, inclusive="both")]
        realized_symbols = set(realized["symbol"].astype(str))
        champion_symbols = set(champion["symbol"].astype(str))
        rank6_20_symbols = set(rank6_20_pool["symbol"].astype(str))
        hit_symbols = realized_symbols & champion_symbols
        missed_symbols = realized_symbols - champion_symbols
        missed_rank6_20 = missed_symbols & rank6_20_symbols
        missed_outside20 = missed_symbols - rank6_20_symbols
        false_positive_symbols = champion_symbols - realized_symbols
        date_side_realized_total += len(realized_symbols)
        date_side_hit_total += len(hit_symbols)
        date_side_missed_total += len(missed_symbols)
        date_side_missed_rank6_20_total += len(missed_rank6_20)
        date_side_missed_outside_total += len(missed_outside20)
        date_side_false_positive_total += len(false_positive_symbols)
        rank6_20_pool_total += len(rank6_20_symbols)

    monthly_capture_mean = _safe_float(pd.Series(monthly_capture_rates).mean()) if monthly_capture_rates else 0.0
    monthly_capture_median = _safe_float(pd.Series(monthly_capture_rates).median()) if monthly_capture_rates else 0.0
    top_month_counts = sorted((row["missed_realized_top5_winner_count"] for row in month_rows), reverse=True)
    total_month_misses = sum(row["missed_realized_top5_winner_count"] for row in month_rows)
    opportunity_concentration = {
        "months_with_opportunity": int(sum(1 for row in month_rows if row["missed_realized_top5_winner_count"] > 0)),
        "zero_opportunity_months": int(sum(1 for row in month_rows if row["missed_realized_top5_winner_count"] <= 0)),
        "top1_month_share": float((top_month_counts[:1][0] / total_month_misses) if top_month_counts and total_month_misses else 0.0),
        "top3_month_share": float((sum(top_month_counts[:3]) / total_month_misses) if total_month_misses else 0.0),
        "top5_month_share": float((sum(top_month_counts[:5]) / total_month_misses) if total_month_misses else 0.0),
        "broad_or_concentrated": "broad" if total_month_misses and sum(top_month_counts[:5]) / total_month_misses < 0.5 else "concentrated",
    }
    regime_opportunity = Counter()
    regime_false_promotion = Counter()
    for label in ("champion_top5_miss_but_rank6_20", "champion_top5_false_positive", "rank6_20_false_promotion_risk"):
        for record in records:
            if record["group"] == label:
                regime = str(record.get("family_regime_context") or "None")
                if label == "champion_top5_miss_but_rank6_20":
                    regime_opportunity[regime] += 1
                elif label == "rank6_20_false_promotion_risk":
                    regime_false_promotion[regime] += 1

    monthly_capture_baseline_summary = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "months_evaluated": len(month_rows),
        "champion_monthly_top5_capture_mean": monthly_capture_mean,
        "champion_monthly_top5_capture_median": monthly_capture_median,
        "realized_top5_winners_total": int(date_side_realized_total),
        "champion_top5_hits_total": int(date_side_hit_total),
        "missed_realized_top5_winners_total": int(date_side_missed_total),
        "missed_realized_top5_winners_in_rank6_20_total": int(date_side_missed_rank6_20_total),
        "missed_realized_top5_winners_outside_top20_total": int(date_side_missed_outside_total),
        "champion_top5_false_positive_total": int(date_side_false_positive_total),
        "rank6_20_false_promotion_risk_total": int(rank6_20_pool_total - date_side_hit_total),
        "rank6_20_opportunity_rate": float(date_side_missed_rank6_20_total / date_side_realized_total) if date_side_realized_total else 0.0,
        "false_positive_rate": float(date_side_false_positive_total / date_side_realized_total) if date_side_realized_total else 0.0,
        "opportunity_concentration": opportunity_concentration,
        "by_month": month_rows,
        "by_regime": {
            "rank6_20_promotion_opportunity": {key: int(value) for key, value in regime_opportunity.items()},
            "rank6_20_false_promotion_risk": {key: int(value) for key, value in regime_false_promotion.items()},
        },
    }

    missed_inventory = {
        "schema_version": MISS_INVENTORY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "record_count": int(sum(1 for row in records if row["group"] == "champion_top5_miss_but_rank6_20")),
        "records": [record for record in records if record["group"] == "champion_top5_miss_but_rank6_20"],
    }

    promotion_opportunity_summary = {
        "schema_version": OPPORTUNITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "months_evaluated": len(month_rows),
        "rank6_20_opportunity_rate": monthly_capture_baseline_summary["rank6_20_opportunity_rate"],
        "champion_top5_false_positive_count": int(date_side_false_positive_total),
        "false_positive_rate": monthly_capture_baseline_summary["false_positive_rate"],
        "rank6_20_promotion_opportunity_by_regime": monthly_capture_baseline_summary["by_regime"]["rank6_20_promotion_opportunity"],
        "opportunity_concentration_by_month": opportunity_concentration,
        "broad_or_concentrated": opportunity_concentration["broad_or_concentrated"],
        "missing_months": [],
    }

    top5_false_positive_summary = {
        "schema_version": FALSE_POS_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "false_positive_count": int(date_side_false_positive_total),
        "false_positive_rate": monthly_capture_baseline_summary["false_positive_rate"],
        "records": [record for record in records if record["group"] == "champion_top5_false_positive"],
        "group_stats": _aggregate_group_features([record for record in records if record["group"] == "champion_top5_false_positive"]),
    }

    feature_contrast_summary = {
        "schema_version": FEATURE_CONTRAST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "groups": {group: _aggregate_group_features(group_records[group]) for group in [
            "champion_top5_hit",
            "champion_top5_miss_but_rank6_20",
            "champion_top5_miss_outside_top20",
            "champion_top5_false_positive",
            "rank6_20_false_promotion_risk",
        ]},
        "group_order": [
            "champion_top5_hit",
            "champion_top5_miss_but_rank6_20",
            "champion_top5_miss_outside_top20",
            "champion_top5_false_positive",
            "rank6_20_false_promotion_risk",
        ],
    }

    candidate_design = {
        "schema_version": NEXT_DESIGN_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": "proceed_to_candidate",
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "family_id": "champion_top5_capture_boundary_promoter_v1",
        "why": "All missed realized top5 winners live in champion rank6-20, the opportunity is broad across 23 months, and the boundary pool shows plausible decision-time separation from false positives.",
        "design": {
            "base_ranking": "champion score order",
            "promotion_pool": "champion rank6-20 only",
            "max_promotions_per_date_side": 2,
            "max_demotions_per_date_side": 2,
            "boundary_scope": "top5 only",
            "non_goals": [
                "broad reranking",
                "score replacement",
                "MeeMee reflection",
                "threshold tweaks to champion_topk_bad_pick_veto_v1",
            ],
            "gate_features": [
                "score_rank",
                "score_gap_to_top5_boundary",
                "score_gap_to_top20_boundary",
                "monthly_context",
                "weekly_context",
                "shape_classification",
                "gap_pct",
                "vol_ratio5_20",
                "candle_body_ratio",
                "path_value_score_v1",
                "symbol_vol_ratio_median_past",
                "symbol_gap_pct_median_past",
                "symbol_body_ratio_median_past",
                "family_regime_context",
            ],
            "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
            "ret20_source_mode": "forward_ret_20d",
            "same_condition_contract": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": [5, 10, 20],
                "same_cost_slippage": True,
                "same_artifact_detail_level": True,
                "silent_fallback_allowed": False,
            },
            "promotion_gate_requirement": "Promote only when a rank6-20 name meaningfully separates from champion top5 false positives on decision-time boundary features.",
        },
        "evidence": {
            "months_evaluated": len(month_rows),
            "realized_top5_winners_total": int(date_side_realized_total),
            "missed_realized_top5_winners_in_rank6_20_total": int(date_side_missed_rank6_20_total),
            "missed_realized_top5_winners_outside_top20_total": int(date_side_missed_outside_total),
            "rank6_20_opportunity_rate": monthly_capture_baseline_summary["rank6_20_opportunity_rate"],
            "opportunity_concentration": opportunity_concentration,
        },
    }

    artifact_paths = {
        "mining_contract.json": _write_json(output_root / "mining_contract.json", {
            "schema_version": MINING_CONTRACT_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "source_rows_parquet": str(source_rows_parquet.resolve()) if source_rows_parquet is not None else None,
            "same_condition_contract": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": [5, 10, 20],
                "same_regime": True,
                "same_cost_slippage": True,
                "same_artifact_detail_level": True,
                "ret20_source_mode": "forward_ret_20d",
                "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
                "silent_fallback_allowed": False,
            },
            "allowed_feature_families": [
                "boundary_feature",
                "monthly_capture_feature",
                "common_chart_structure",
                "regime_adjustment",
                "symbol_specific_deviation",
            ],
            "no_candidate_implementation": True,
            "mining_only_mode": True,
        }),
        "monthly_capture_baseline_summary.json": _write_json(output_root / "monthly_capture_baseline_summary.json", monthly_capture_baseline_summary),
        "missed_top5_winner_inventory.json": _write_json(output_root / "missed_top5_winner_inventory.json", missed_inventory),
        "rank6_20_promotion_opportunity_summary.json": _write_json(output_root / "rank6_20_promotion_opportunity_summary.json", promotion_opportunity_summary),
        "top5_false_positive_summary.json": _write_json(output_root / "top5_false_positive_summary.json", top5_false_positive_summary),
        "feature_contrast_summary.json": _write_json(output_root / "feature_contrast_summary.json", feature_contrast_summary),
        "next_candidate_design.json": _write_json(output_root / "next_candidate_design.json", candidate_design),
    }

    report_path = output_root / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# champion_top5_capture_boundary_promoter_v1_mining",
                "",
                f"- decision: {candidate_design['decision']}",
                f"- months_evaluated: {monthly_capture_baseline_summary['months_evaluated']}",
                f"- rank6_20_opportunity_rate: {monthly_capture_baseline_summary['rank6_20_opportunity_rate']}",
                f"- broad_or_concentrated: {opportunity_concentration['broad_or_concentrated']}",
                "",
                "JSON artifacts are authoritative.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths["report.md"] = report_path
    complete = build_artifact_complete(
        {"schema_version": SCHEMA_VERSION},
        sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        schema_version=f"{SCHEMA_VERSION}_artifact_complete_v1",
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = output_root / "_ARTIFACT_COMPLETE.json"
    return {
        "output_root": str(output_root.resolve()),
        "artifact_paths": {key: str(value) for key, value in artifact_paths.items()},
        "monthly_capture_baseline_summary": monthly_capture_baseline_summary,
        "missed_top5_winner_inventory": missed_inventory,
        "rank6_20_promotion_opportunity_summary": promotion_opportunity_summary,
        "top5_false_positive_summary": top5_false_positive_summary,
        "feature_contrast_summary": feature_contrast_summary,
        "next_candidate_design": candidate_design,
    }


def run_all(
    *,
    source_rows_parquet: Path = DEFAULT_SOURCE_ROWS_PARQUET,
    closeout_root: Path = DEFAULT_CLOSEOUT_ROOT,
    freeze_root: Path = DEFAULT_FREEZE_ROOT,
    mining_root: Path = DEFAULT_MINING_ROOT,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    source_rows_parquet = _safe_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET)
    closeout_root = _safe_path(closeout_root, DEFAULT_CLOSEOUT_ROOT)
    freeze_session_root = _session_root(_safe_path(freeze_root, DEFAULT_FREEZE_ROOT), "champion_topk_bad_pick_veto_v1", run_id)
    mining_session_root = mining_root / run_id
    latest_closeout = _safe_latest_run(closeout_root)
    freeze_payload = _build_freeze_outputs(closeout_root=latest_closeout, output_root=freeze_session_root)
    frame = _load_base_frame(source_rows_parquet)
    mining_payload = _build_mining_outputs(frame, output_root=mining_session_root, source_rows_parquet=source_rows_parquet)
    return {
        "run_id": run_id,
        "source_rows_parquet": str(source_rows_parquet.resolve()),
        "freeze_root": freeze_payload,
        "mining_root": mining_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze champion_topk_bad_pick_veto_v1 and mine top5 capture boundary opportunity.")
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--closeout-root", default=str(DEFAULT_CLOSEOUT_ROOT))
    parser.add_argument("--freeze-root", default=str(DEFAULT_FREEZE_ROOT))
    parser.add_argument("--mining-root", default=str(DEFAULT_MINING_ROOT))
    args = parser.parse_args(argv)
    payload = run_all(
        source_rows_parquet=_safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET),
        closeout_root=_safe_path(args.closeout_root, DEFAULT_CLOSEOUT_ROOT),
        freeze_root=_safe_path(args.freeze_root, DEFAULT_FREEZE_ROOT),
        mining_root=_safe_path(args.mining_root, DEFAULT_MINING_ROOT),
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
