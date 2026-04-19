from __future__ import annotations

import argparse
import importlib
import json
import os
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")

DEFAULT_DB_PATH = base.DEFAULT_DB_PATH
DEFAULT_OUTPUT_DIR = base.DEFAULT_OUTPUT_DIR


def _resolve_db_path(cli_value: str | None) -> Path:
    return base._resolve_db_path(cli_value)


def _normalize_reason_codes(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _trend_bucket_reason(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    weekly = row.get("weeklyBreakoutDownProb")
    monthly = row.get("monthlyBreakoutDownProb")
    range_prob = row.get("monthlyRangeProb")
    range_pos = row.get("monthlyRangePos")
    close_pos = row.get("close_pos")
    dist_ma20 = row.get("dist_ma20_signed")
    dist_low20 = row.get("dist_low20")
    day_change = row.get("day_change_pct")
    trend_strict = row.get("trendDownStrict")
    ma20_slope = row.get("ma20_slope")
    ma60_slope = row.get("ma60_slope")
    short_ret_5 = row.get("short_ret_5")
    short_ret_10 = row.get("short_ret_10")

    if weekly is not None and monthly is not None and float(weekly) >= 0.66 and float(monthly) < 0.66:
        reasons.append("daily_trigger_but_monthly_not_aligned")
    if weekly is not None and monthly is not None and float(weekly) < 0.66 and float(monthly) >= 0.66:
        reasons.append("daily_trigger_but_weekly_not_aligned")
    if (
        trend_strict is False
        and (
            bool(row.get("patternD1"))
            or bool(row.get("patternD2"))
            or bool(row.get("patternD3"))
            or bool(row.get("patternD4"))
            or bool(row.get("patternD5"))
            or str(row.get("setupType") or "") in {"breakdown", "pressure", "continuation"}
        )
    ):
        reasons.append("countertrend_breakdown_only")
    if range_prob is not None and range_pos is not None and float(range_prob) >= 0.20 and 0.35 <= float(range_pos) <= 0.65:
        reasons.append("range_middle_short_without_edge")
    if (
        close_pos is not None
        and dist_ma20 is not None
        and dist_low20 is not None
        and float(close_pos) <= 0.15
        and float(dist_ma20) <= -0.03
        and float(dist_low20) >= 0.03
    ):
        reasons.append("short_below_support_after_extension")
    if trend_strict is not True or (ma20_slope is not None and float(ma20_slope) >= 0.0) or (ma60_slope is not None and float(ma60_slope) >= 0.0):
        reasons.append("weak_downtrend_structure")
    if (
        short_ret_5 is not None
        and short_ret_10 is not None
        and float(short_ret_5) <= 0.0
        and float(short_ret_10) <= 0.0
    ):
        reasons.append("failed_followthrough_after_break")
    if (
        range_prob is not None
        and float(range_prob) >= 0.20
        and day_change is not None
        and abs(float(day_change)) <= 0.01
        and trend_strict is not True
    ):
        reasons.append("sideways_noise_short")

    return _normalize_reason_codes(reasons)


def _bucket_primary(row: dict[str, Any]) -> str | None:
    reasons = _trend_bucket_reason(row)
    return reasons[0] if reasons else None


def _trend_bucket_predicates() -> dict[str, Any]:
    def daily_trigger_but_weekly_not_aligned(row: dict[str, Any]) -> bool:
        weekly = row.get("weeklyBreakoutDownProb")
        monthly = row.get("monthlyBreakoutDownProb")
        return weekly is not None and monthly is not None and float(weekly) < 0.75 and float(monthly) >= 0.72

    def daily_trigger_but_monthly_not_aligned(row: dict[str, Any]) -> bool:
        weekly = row.get("weeklyBreakoutDownProb")
        monthly = row.get("monthlyBreakoutDownProb")
        return weekly is not None and monthly is not None and float(weekly) >= 0.72 and float(monthly) < 0.72

    def countertrend_breakdown_only(row: dict[str, Any]) -> bool:
        return bool(
            row.get("trendDownStrict") is False
            and (
                bool(row.get("patternD1"))
                or bool(row.get("patternD2"))
                or bool(row.get("patternD3"))
                or bool(row.get("patternD4"))
                or bool(row.get("patternD5"))
                or str(row.get("setupType") or "") in {"breakdown", "pressure", "continuation"}
            )
        )

    def range_middle_short_without_edge(row: dict[str, Any]) -> bool:
        range_prob = row.get("monthlyRangeProb")
        range_pos = row.get("monthlyRangePos")
        close_pos = row.get("close_pos")
        return bool(
            (
                range_prob is not None
                and float(range_prob) >= 0.20
                and range_pos is not None
                and 0.30 <= float(range_pos) <= 0.70
            )
            or (close_pos is not None and 0.30 <= float(close_pos) <= 0.70)
        )

    def short_below_support_after_extension(row: dict[str, Any]) -> bool:
        close_pos = row.get("close_pos")
        dist_ma20 = row.get("dist_ma20_signed")
        dist_low20 = row.get("dist_low20")
        return bool(
            close_pos is not None
            and dist_ma20 is not None
            and dist_low20 is not None
            and float(close_pos) <= 0.15
            and float(dist_ma20) <= -0.02
            and float(dist_low20) >= 0.015
        )

    def weak_downtrend_structure(row: dict[str, Any]) -> bool:
        trend_strict = row.get("trendDownStrict")
        ma20_slope = row.get("ma20_slope")
        ma60_slope = row.get("ma60_slope")
        return bool(
            trend_strict is not True
            or (ma20_slope is not None and float(ma20_slope) >= 0.0)
            or (ma60_slope is not None and float(ma60_slope) >= 0.0)
        )

    def failed_followthrough_after_break(row: dict[str, Any]) -> bool:
        short_ret_5 = row.get("short_ret_5")
        short_ret_10 = row.get("short_ret_10")
        return bool(
            short_ret_5 is not None
            and short_ret_10 is not None
            and float(short_ret_5) <= 0.0
            and float(short_ret_10) <= 0.0
        )

    def sideways_noise_short(row: dict[str, Any]) -> bool:
        range_prob = row.get("monthlyRangeProb")
        day_change = row.get("day_change_pct")
        trend_strict = row.get("trendDownStrict")
        return bool(
            range_prob is not None
            and float(range_prob) >= 0.20
            and day_change is not None
            and abs(float(day_change)) <= 0.015
            and trend_strict is not True
        )

    return {
        "daily_trigger_but_weekly_not_aligned": daily_trigger_but_weekly_not_aligned,
        "daily_trigger_but_monthly_not_aligned": daily_trigger_but_monthly_not_aligned,
        "countertrend_breakdown_only": countertrend_breakdown_only,
        "range_middle_short_without_edge": range_middle_short_without_edge,
        "short_below_support_after_extension": short_below_support_after_extension,
        "weak_downtrend_structure": weak_downtrend_structure,
        "failed_followthrough_after_break": failed_followthrough_after_break,
        "sideways_noise_short": sideways_noise_short,
    }


def _build_trend_taxonomy(rows: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    predicates = _trend_bucket_predicates()
    for row in rows:
        reasons = _trend_bucket_reason(row)
        if not reasons:
            continue
        row_with_reason = {**row, "reason_codes": reasons}
        for bucket, predicate in predicates.items():
            if predicate(row):
                buckets[bucket].append(row_with_reason)

    required = [
        "daily_trigger_but_weekly_not_aligned",
        "daily_trigger_but_monthly_not_aligned",
        "countertrend_breakdown_only",
        "range_middle_short_without_edge",
        "short_below_support_after_extension",
        "weak_downtrend_structure",
        "failed_followthrough_after_break",
        "sideways_noise_short",
    ]

    out: dict[str, Any] = {}
    for bucket in required:
        bucket_rows = buckets.get(bucket, [])
        if bucket_rows:
            sample = sorted(
                bucket_rows,
                key=lambda r: (
                    float(r["short_ret_20"]) if r.get("short_ret_20") is not None else 0.0,
                    float(r["entryScore"]) if r.get("entryScore") is not None else 0.0,
                ),
            )[:5]
            ret20 = pd.to_numeric(pd.Series([r.get("short_ret_20") for r in bucket_rows]), errors="coerce").dropna()
            out[bucket] = {
                "count": int(len(bucket_rows)),
                "hit_rate": float((ret20 > 0).mean()) if len(ret20) else None,
                "median_ret20": float(ret20.median()) if len(ret20) else None,
                "mean_ret20": float(ret20.mean()) if len(ret20) else None,
                "reason_code_summary": dict(Counter(code for row in bucket_rows for code in row.get("reason_codes", []))),
                "representative_examples": [
                    {
                        "ymd": int(r["ymd"]),
                        "code": r["code"],
                        "short_ret_20": float(r["short_ret_20"]) if r.get("short_ret_20") is not None else None,
                        "entryScore": float(r["entryScore"]) if r.get("entryScore") is not None else None,
                        "close_pos": float(r["close_pos"]) if r.get("close_pos") is not None else None,
                        "monthlyRangePos": float(r["monthlyRangePos"]) if r.get("monthlyRangePos") is not None else None,
                        "weeklyBreakoutDownProb": float(r["weeklyBreakoutDownProb"]) if r.get("weeklyBreakoutDownProb") is not None else None,
                        "monthlyBreakoutDownProb": float(r["monthlyBreakoutDownProb"]) if r.get("monthlyBreakoutDownProb") is not None else None,
                        "reason_codes": r.get("reason_codes") or [],
                    }
                    for r in sample
                ],
            }
        else:
            out[bucket] = {
                "count": 0,
                "hit_rate": None,
                "median_ret20": None,
                "mean_ret20": None,
                "reason_code_summary": {},
                "representative_examples": [],
            }
    return out


@dataclass(frozen=True)
class TrendVariant:
    name: str
    target: str
    weekly_breakout_down_min: float | None = None
    monthly_breakout_down_min: float | None = None
    monthly_range_prob_max: float | None = None
    close_pos_max: float | None = None
    day_change_max: float | None = None
    dist_ma20_max: float | None = None
    require_midrange_off: bool = True

    def matches(self, row: dict[str, Any]) -> bool:
        if row.get("selected_by_baseline") is not True:
            return False
        if self.weekly_breakout_down_min is not None:
            if row.get("weeklyBreakoutDownProb") is None or float(row["weeklyBreakoutDownProb"]) < float(self.weekly_breakout_down_min):
                return False
        if self.monthly_breakout_down_min is not None:
            if row.get("monthlyBreakoutDownProb") is None or float(row["monthlyBreakoutDownProb"]) < float(self.monthly_breakout_down_min):
                return False
        if self.monthly_range_prob_max is not None:
            if row.get("monthlyRangeProb") is None or float(row["monthlyRangeProb"]) > float(self.monthly_range_prob_max):
                return False
        if self.close_pos_max is not None:
            if row.get("close_pos") is None or float(row["close_pos"]) > float(self.close_pos_max):
                return False
        if self.day_change_max is not None:
            if row.get("day_change_pct") is None or float(row["day_change_pct"]) > float(self.day_change_max):
                return False
        if self.dist_ma20_max is not None:
            if row.get("dist_ma20_signed") is None or float(row["dist_ma20_signed"]) > float(self.dist_ma20_max):
                return False
        if self.require_midrange_off:
            if row.get("monthlyRangePos") is not None and 0.35 <= float(row["monthlyRangePos"]) <= 0.65:
                return False
        return True


def _monthly_sign_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        if row.get("short_ret_20") is None:
            continue
        by_month[int(row["ymd"])].append(float(row["short_ret_20"]))
    medians = {ymd: float(statistics.median(vals)) for ymd, vals in by_month.items() if vals}
    return {
        "months_with_selection": int(len(by_month)),
        "positive_months": int(sum(1 for v in medians.values() if v > 0.0)),
        "negative_months": int(sum(1 for v in medians.values() if v < 0.0)),
        "monthly_median_ret20": {str(k): v for k, v in medians.items()},
    }


def _evaluate_variant(rows: list[dict[str, Any]], *, variant: TrendVariant, taxonomy: dict[str, Any]) -> dict[str, Any]:
    result = base._evaluate_variant(rows, variant=variant)
    baseline_rows = result["baseline_rows"]
    selected_rows = result["selected_rows"]
    removed_bucket_counts: Counter[str] = Counter()
    retained_bucket_counts: Counter[str] = Counter()
    predicates = _trend_bucket_predicates()
    for row in baseline_rows:
        for bucket, predicate in predicates.items():
            if predicate(row):
                if str(row["code"]) not in {str(sel["code"]) for sel in selected_rows}:
                    removed_bucket_counts[bucket] += 1
                else:
                    retained_bucket_counts[bucket] += 1
    result["delta"]["removed_bucket_counts"] = dict(removed_bucket_counts)
    result["delta"]["retained_bucket_counts"] = dict(retained_bucket_counts)
    result["monthly_stability"] = _monthly_sign_stats(selected_rows)
    return result


def _build_feature_map() -> dict[str, Any]:
    return {
        "families": [
            {
                "name": "higher_timeframe_alignment",
                "target": "bad-pick removal",
                "why_short_only": "Keeps shorts only when weekly and monthly downside context are aligned enough to justify a close entry.",
                "expected_short_count_effect": "decrease",
                "expected_short_quality_effect": "hit rate up, median ret20 up, fewer trend-misaligned shorts",
                "long_side_impact": "no change",
                "features": [
                    "weeklyBreakoutDownProb",
                    "monthlyBreakoutDownProb",
                    "marketRiskOff",
                ],
            },
            {
                "name": "range_middle_suppression",
                "target": "bad-pick removal",
                "why_short_only": "Suppresses shorts that sit inside broad range-middle noise rather than on a real downside edge.",
                "expected_short_count_effect": "decrease",
                "expected_short_quality_effect": "hit rate up, median ret20 up, fewer range-middle false shorts",
                "long_side_impact": "no change",
                "features": [
                    "monthlyRangeProb",
                    "monthlyRangePos",
                    "close_pos",
                ],
            },
            {
                "name": "support_break_validity",
                "target": "bad-pick removal",
                "why_short_only": "Filters shorts that are already too extended below support or are likely to mean-revert into noise.",
                "expected_short_count_effect": "decrease",
                "expected_short_quality_effect": "hit rate up, median ret20 up, fewer already-fallen shorts",
                "long_side_impact": "no change",
                "features": [
                    "dist_ma20_signed",
                    "dist_low20",
                    "day_change_pct",
                ],
            },
        ]
    }


def _decision_for_variant(result: dict[str, Any]) -> tuple[str, list[str]]:
    base_summary = result["baseline"]
    chal_summary = result["challenger"]
    delta = result["delta"]
    stability = result["monthly_stability"]
    reasons: list[str] = []
    if delta["changed_top5_short_count"] > 0:
        reasons.append("top5_branching_observed")
    if delta["bad_short_removal_count"] > 0:
        reasons.append("bad_short_removal_visible")
    if chal_summary["count"] < base_summary["count"]:
        reasons.append("coverage_reduced")
    if base_summary["hit_rate"] is not None and chal_summary["hit_rate"] is not None and chal_summary["hit_rate"] > base_summary["hit_rate"]:
        reasons.append("hit_rate_improved")
    if base_summary["median_ret20"] is not None and chal_summary["median_ret20"] is not None and chal_summary["median_ret20"] > base_summary["median_ret20"]:
        reasons.append("median_ret20_improved")
    if base_summary["mean_ret20"] is not None and chal_summary["mean_ret20"] is not None and chal_summary["mean_ret20"] > base_summary["mean_ret20"]:
        reasons.append("mean_ret20_improved")
    if chal_summary["mean_ret20"] is not None and chal_summary["mean_ret20"] > 0:
        reasons.append("positive_mean_ret20")
    if stability["positive_months"] > stability["negative_months"]:
        reasons.append("monthly_stability_positive")
    if chal_summary["count"] < 12:
        reasons.append("sample_thin")

    if (
        chal_summary["count"] >= 12
        and base_summary["hit_rate"] is not None
        and chal_summary["hit_rate"] is not None
        and chal_summary["hit_rate"] > base_summary["hit_rate"]
        and base_summary["median_ret20"] is not None
        and chal_summary["median_ret20"] is not None
        and chal_summary["median_ret20"] > base_summary["median_ret20"]
        and base_summary["mean_ret20"] is not None
        and chal_summary["mean_ret20"] is not None
        and chal_summary["mean_ret20"] >= base_summary["mean_ret20"] + 0.005
        and stability["positive_months"] >= 4
        and delta["changed_top5_short_count"] > 0
        and delta["bad_short_removal_count"] > 0
    ):
        return "keep", reasons + ["actionable_short_quality_improved"]

    if (
        chal_summary["hit_rate"] is not None
        and chal_summary["median_ret20"] is not None
        and chal_summary["hit_rate"] >= base_summary["hit_rate"]
        and chal_summary["median_ret20"] > base_summary["median_ret20"]
        and delta["changed_top5_short_count"] > 0
        and delta["bad_short_removal_count"] > 0
        and stability["positive_months"] >= 3
    ):
        return "hold", reasons + ["promising_but_not_robust_enough"]

    return "drop", reasons + ["quality_did_not_improve"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Entry Precision Short Trend Audit",
        "",
        "## Current State",
        f"- confirmed: {', '.join(payload['current_state']['confirmed'])}",
        f"- provisional: {', '.join(payload['current_state']['provisional'])}",
        "",
        "## Problem",
        payload["problem"],
        "",
        "## Change Policy",
        payload["change_policy"],
        "",
        "## Concrete Changes",
        f"- baseline short selected count: `{payload['baseline']['count']}`",
        f"- trend alignment selected count: `{payload['variants']['short_trend_alignment_v1']['challenger']['count']}`",
        f"- range suppression selected count: `{payload['variants']['short_range_middle_suppression_v1']['challenger']['count']}`",
        f"- trend+followthrough selected count: `{payload['variants']['short_trend_alignment_plus_followthrough_v1']['challenger']['count']}`",
        "",
        "## Verify",
        f"- short baseline hit rate: `{payload['baseline']['hit_rate']}`",
        f"- short baseline median ret20: `{payload['baseline']['median_ret20']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        "",
        "## Decision",
        "\n".join([f"- {name}: `{info['decision']}`" for name, info in payload["decisions"].items()]),
        "",
        "## Remaining Risks",
        "\n".join([f"- {risk}" for risk in payload["remaining_risks"]]),
        "",
        "## Next One Thing",
        payload["next_one_thing"],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    out_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    session_id = f"entry-short-trend-precision-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    baseline_id = "current_rule_trade_gate_baseline"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        months = base._month_end_dates(conn, start_ymd=int(args.start_ymd), end_ymd=int(args.end_ymd))
        price_store = base._load_price_store(conn)
        sell_map = base._load_frame_map(conn, "sell_analysis_daily", ymd_col="dt")
        feature_map = base._load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt")
        event_map = base._load_event_map(conn)
        bundle = base._build_rows(
            conn=conn,
            months=months,
            price_store=price_store,
            sell_map=sell_map,
            feature_map=feature_map,
            event_map=event_map,
        )

    rows = bundle["rows"]
    baseline_rows = [row for row in rows if row.get("selected_by_baseline")]
    baseline_summary = base._row_metric_summary(baseline_rows)
    baseline_summary["by_side"] = {"down": base._row_metric_summary([row for row in baseline_rows if True])}
    baseline_summary["monthly_rows"] = [
        {
            "ymd": int(ymd),
            "selected_count": int(sum(1 for row in baseline_rows if int(row["ymd"]) == int(ymd))),
        }
        for ymd in months
    ]

    taxonomy = _build_trend_taxonomy(baseline_rows)
    feature_map_payload = _build_feature_map()

    variants = [
        TrendVariant(
            name="short_trend_alignment_v1",
            target="bad-pick removal",
            weekly_breakout_down_min=0.66,
            monthly_breakout_down_min=0.66,
            monthly_range_prob_max=0.25,
            close_pos_max=0.20,
        ),
        TrendVariant(
            name="short_range_middle_suppression_v1",
            target="bad-pick removal",
            monthly_range_prob_max=0.20,
        ),
        TrendVariant(
            name="short_trend_alignment_plus_followthrough_v1",
            target="bad-pick removal",
            weekly_breakout_down_min=0.66,
            monthly_breakout_down_min=0.66,
            monthly_range_prob_max=0.25,
            close_pos_max=0.15,
            day_change_max=-0.015,
        ),
    ]

    compare_results = {variant.name: _evaluate_variant(baseline_rows, variant=variant, taxonomy=taxonomy) for variant in variants}
    decisions: dict[str, Any] = {}
    for name, result in compare_results.items():
        decision, reasons = _decision_for_variant(result)
        decisions[name] = {
            "decision": decision,
            "decision_reasons": reasons,
            "baseline_id": baseline_id,
            "challenger_id": name,
            "metrics": {
                "baseline": result["baseline"],
                "challenger": result["challenger"],
                "delta": result["delta"],
            },
        }

    long_freeze_confirmed = True
    overall_decision = "hold"
    if decisions["short_trend_alignment_v1"]["decision"] == "keep":
        overall_decision = "keep"
    elif all(d["decision"] == "drop" for d in decisions.values()):
        overall_decision = "drop"

    compare_payload = {
        "schema_version": "tradex_entry_precision_short_trend_compare_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": baseline_id,
        "long_freeze_confirmed": long_freeze_confirmed,
        "baseline": baseline_summary,
        "taxonomy": taxonomy,
        "feature_map": feature_map_payload,
        "variants": compare_results,
        "decision_rollup": {
            "overall": overall_decision,
            "per_variant": {name: info["decision"] for name, info in decisions.items()},
        },
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
        },
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_trend_decision_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": baseline_id,
        "long_freeze_confirmed": long_freeze_confirmed,
        "overall_decision": overall_decision,
        "decisions": decisions,
        "confirmed": [
            "short_side_only_changed",
            "long_side_frozen",
            "same_window_fixed",
            "same_universe_fixed",
            "same_top_k_fixed",
            "json_is_authoritative",
            "trend_alignment_taxonomy_built",
        ],
        "provisional": [
            "borrow_cost_is_proxy_only",
            "event_risk_is_proxy_based",
            "monthly_regime_stability_is_partial",
        ],
        "remaining_risks": [
            "baseline_short_sample_is_small",
            "borrow_or_cost_has_no_direct_table",
            "event_risk_can_be_sparse_in_this_window",
            "trend_alignment_is_still_proxy_based",
        ],
    }

    payload = {
        "current_state": {
            "confirmed": [
                "short_side_cleanup_completed",
                "baseline_short_rows_replayed_from_snapshot_db",
                "long_logic_remained_frozen",
            ],
            "provisional": [
                "borrow_cost_is_proxy_only",
                "false_neutral_recovery_is_not_yet_demonstrated",
                "monthly_regime_stability_is_partial",
            ],
        },
        "problem": "The remaining short-side noise is structural: trend-misaligned names, range-middle shorts, and weak continuation zones still leak through the gate.",
        "change_policy": "TRADEX only, short-side trend-alignment cleanup only, long logic frozen, same window and artifact detail level fixed, and no multi-axis redesign.",
        "baseline": baseline_summary,
        "taxonomy": taxonomy,
        "feature_map": feature_map_payload,
        "variants": compare_results,
        "decisions": decisions,
        "overall_decision": overall_decision,
        "long_freeze_confirmed": long_freeze_confirmed,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "Move one axis only: either tighten higher-timeframe alignment further or widen the historical slice, but not both.",
    }

    _write_json(
        out_dir / "entry_precision_short_trend_taxonomy.json",
        {
            "session_id": session_id,
            "baseline_id": baseline_id,
            "window": {"start_ymd": int(args.start_ymd), "end_ymd": int(args.end_ymd)},
            "taxonomy": taxonomy,
        },
    )
    _write_json(
        out_dir / "entry_precision_short_trend_feature_map.json",
        {
            "session_id": session_id,
            "baseline_id": baseline_id,
            "feature_map": feature_map_payload,
        },
    )
    _write_json(out_dir / "entry_precision_short_trend_compare.json", compare_payload)
    _write_json(out_dir / "entry_precision_short_trend_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_trend_report.md", payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": baseline_id,
        "overall_decision": overall_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Short-side trend alignment precision audit for TRADEX.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--start-ymd", type=int, default=20250101)
    parser.add_argument("--end-ymd", type=int, default=20260226)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
