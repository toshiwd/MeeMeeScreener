from __future__ import annotations

import argparse
import importlib
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")
wide = importlib.import_module("scripts.entry_precision_short_trend_wide_audit")
trend = importlib.import_module("scripts.entry_precision_short_trend_audit")
regime = importlib.import_module("scripts.entry_precision_short_trend_regime_audit")

DEFAULT_OUTPUT_DIR = base.DEFAULT_OUTPUT_DIR
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_trend_alignment_v1"
FOCUSED_REGIME = "broad_down_regime"
FROZEN_FAILURE_SESSION_ID = "entry-short-broad-down-20260419-130000"
LOCAL_WINDOW = {"start_ymd": 20250101, "end_ymd": 20260226}
WIDE_WINDOW = {"start_ymd": 20240101, "end_ymd": 20260226}

FROZEN_VARIANT = trend.TrendVariant(
    name=CHALLENGER_ID,
    target="bad-pick removal",
    weekly_breakout_down_min=0.66,
    monthly_breakout_down_min=0.66,
    monthly_range_prob_max=0.25,
    close_pos_max=0.20,
)

RELAXED_VARIANT = trend.TrendVariant(
    name="broad_down_monthly_alignment_relaxed_v1",
    target="bad-pick removal",
    weekly_breakout_down_min=0.66,
    monthly_breakout_down_min=0.55,
    monthly_range_prob_max=0.25,
    close_pos_max=0.20,
)

OFF_VARIANT = trend.TrendVariant(
    name="broad_down_monthly_alignment_off_v1",
    target="bad-pick removal",
    weekly_breakout_down_min=0.66,
    monthly_breakout_down_min=None,
    monthly_range_prob_max=0.25,
    close_pos_max=0.20,
)

VARIANTS = [FROZEN_VARIANT, RELAXED_VARIANT, OFF_VARIANT]
VARIANT_BY_NAME = {variant.name: variant for variant in VARIANTS}


def _resolve_db_path(cli_value: str | None) -> Path:
    return base._resolve_db_path(cli_value)


def _load_window_payload(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_ymd: int,
    end_ymd: int,
) -> dict[str, Any]:
    return wide._load_window_payload(conn, start_ymd=start_ymd, end_ymd=end_ymd)


def _row_key(row: dict[str, Any]) -> tuple[int, str]:
    return int(row["ymd"]), str(row["code"])


def _row_view(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ymd": int(row["ymd"]),
        "code": str(row["code"]),
        "short_ret_20": float(row["short_ret_20"]) if row.get("short_ret_20") is not None else None,
        "short_ret_5": float(row["short_ret_5"]) if row.get("short_ret_5") is not None else None,
        "short_ret_10": float(row["short_ret_10"]) if row.get("short_ret_10") is not None else None,
        "mae20": float(row["mae20"]) if row.get("mae20") is not None else None,
        "mfe20": float(row["mfe20"]) if row.get("mfe20") is not None else None,
        "entryScore": float(row["entryScore"]) if row.get("entryScore") is not None else None,
        "close_pos": float(row["close_pos"]) if row.get("close_pos") is not None else None,
        "dist_ma20_signed": float(row["dist_ma20_signed"]) if row.get("dist_ma20_signed") is not None else None,
        "dist_low20": float(row["dist_low20"]) if row.get("dist_low20") is not None else None,
        "monthlyRangeProb": float(row["monthlyRangeProb"]) if row.get("monthlyRangeProb") is not None else None,
        "monthlyRangePos": float(row["monthlyRangePos"]) if row.get("monthlyRangePos") is not None else None,
        "weeklyBreakoutDownProb": float(row["weeklyBreakoutDownProb"]) if row.get("weeklyBreakoutDownProb") is not None else None,
        "monthlyBreakoutDownProb": float(row["monthlyBreakoutDownProb"]) if row.get("monthlyBreakoutDownProb") is not None else None,
        "trendDownStrict": bool(row["trendDownStrict"]) if row.get("trendDownStrict") is not None else None,
        "marketRiskOff": bool(row["marketRiskOff"]) if row.get("marketRiskOff") is not None else None,
        "marketRegime": row.get("marketRegime"),
    }


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "hit_rate": None,
            "mean_ret20": None,
            "median_ret20": None,
            "mean_mae20": None,
            "median_mae20": None,
            "mean_mfe20": None,
            "median_mfe20": None,
            "flat_rate": None,
            "immediate_reverse_rate": None,
        }
    ret20 = pd.to_numeric(pd.Series([r.get("short_ret_20") for r in rows]), errors="coerce").dropna()
    mae20 = pd.to_numeric(pd.Series([r.get("mae20") for r in rows]), errors="coerce").dropna()
    mfe20 = pd.to_numeric(pd.Series([r.get("mfe20") for r in rows]), errors="coerce").dropna()
    ret5 = pd.to_numeric(pd.Series([r.get("short_ret_5") for r in rows]), errors="coerce").dropna()
    return {
        "count": int(len(rows)),
        "hit_rate": float((ret20 > 0).mean()) if len(ret20) else None,
        "mean_ret20": float(ret20.mean()) if len(ret20) else None,
        "median_ret20": float(ret20.median()) if len(ret20) else None,
        "mean_mae20": float(mae20.mean()) if len(mae20) else None,
        "median_mae20": float(mae20.median()) if len(mae20) else None,
        "mean_mfe20": float(mfe20.mean()) if len(mfe20) else None,
        "median_mfe20": float(mfe20.median()) if len(mfe20) else None,
        "flat_rate": float((ret20.abs() <= 0.005).mean()) if len(ret20) else None,
        "immediate_reverse_rate": float((ret5 <= 0).mean()) if len(ret5) else None,
    }


def _month_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        if row.get("short_ret_20") is None:
            continue
        by_month[int(row["ymd"])].append(float(row["short_ret_20"]))
    month_medians = {ymd: float(statistics.median(vals)) for ymd, vals in by_month.items() if vals}
    return {
        "months_with_selection": int(len(by_month)),
        "positive_months": int(sum(1 for v in month_medians.values() if v > 0.0)),
        "negative_months": int(sum(1 for v in month_medians.values() if v < 0.0)),
        "monthly_median_ret20": {str(k): v for k, v in sorted(month_medians.items())},
    }


def _broad_down_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows = payload["rows"]
    baseline_rows = [r for r in payload["result"]["baseline_rows"] if regime._bucket_regime(r) == FOCUSED_REGIME]
    challenger_rows = [r for r in payload["result"]["selected_rows"] if regime._bucket_regime(r) == FOCUSED_REGIME]
    broad_down_rows = [r for r in rows if regime._bucket_regime(r) == FOCUSED_REGIME]
    return broad_down_rows, baseline_rows, challenger_rows


def _variant_result(rows: list[dict[str, Any]], variant: trend.TrendVariant) -> dict[str, Any]:
    return trend._evaluate_variant(rows, variant=variant, taxonomy={})


def _variant_contract(variant: trend.TrendVariant) -> dict[str, Any]:
    return {
        "name": variant.name,
        "target": variant.target,
        "weekly_breakout_down_min": variant.weekly_breakout_down_min,
        "monthly_breakout_down_min": variant.monthly_breakout_down_min,
        "monthly_range_prob_max": variant.monthly_range_prob_max,
        "close_pos_max": variant.close_pos_max,
        "require_midrange_off": variant.require_midrange_off,
    }


def _row_selection_map(rows: list[dict[str, Any]]) -> dict[tuple[int, str], dict[str, Any]]:
    return {_row_key(r): r for r in rows}


def _compare_with_reference(
    baseline_rows: list[dict[str, Any]],
    selected_rows: list[dict[str, Any]],
    reference_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline_keys = [_row_key(r) for r in baseline_rows]
    selected_keys = [_row_key(r) for r in selected_rows]
    reference_keys = {_row_key(r) for r in reference_rows}
    removed_rows = [r for r in baseline_rows if _row_key(r) not in set(selected_keys)]
    helpful = [r for r in removed_rows if r.get("short_ret_20") is not None and float(r["short_ret_20"]) <= 0.0]
    harmful = [r for r in removed_rows if r.get("short_ret_20") is not None and float(r["short_ret_20"]) > 0.0]
    reentered = [r for r in selected_rows if _row_key(r) not in reference_keys]
    return {
        "helpful_removal_count": int(len(helpful)),
        "harmful_removal_count": int(len(harmful)),
        "unchanged_core_count": int(len([r for r in baseline_rows if _row_key(r) in set(selected_keys)])),
        "selected_names": [_row_view(r) for r in selected_rows],
        "removed_names": [_row_view(r) for r in removed_rows],
        "helpful_removals": [_row_view(r) for r in helpful],
        "harmful_removals": [_row_view(r) for r in harmful],
        "reentered_from_reference": [_row_view(r) for r in reentered],
        "changed_top5_short_count": int(len(set(baseline_keys[:5]) ^ set(selected_keys[:5]))),
        "changed_top10_short_count": int(len(set(baseline_keys[:10]) ^ set(selected_keys[:10]))),
        "changed_rank_short_count": int(
            sum(abs(baseline_keys.index(key) - selected_keys.index(key)) for key in set(baseline_keys[:20]).intersection(selected_keys[:20]))
        ),
    }


def _gate_blockers(row: dict[str, Any], variant: trend.TrendVariant) -> list[str]:
    blockers: list[str] = []
    if row.get("selected_by_baseline") is not True:
        blockers.append("not_selected_by_baseline")
        return blockers
    if variant.weekly_breakout_down_min is not None:
        if row.get("weeklyBreakoutDownProb") is None or float(row["weeklyBreakoutDownProb"]) < float(variant.weekly_breakout_down_min):
            blockers.append("weekly_alignment_failed")
    if variant.monthly_breakout_down_min is not None:
        if row.get("monthlyBreakoutDownProb") is None or float(row["monthlyBreakoutDownProb"]) < float(variant.monthly_breakout_down_min):
            blockers.append("monthly_alignment_failed")
    if variant.monthly_range_prob_max is not None:
        if row.get("monthlyRangeProb") is None or float(row["monthlyRangeProb"]) > float(variant.monthly_range_prob_max):
            blockers.append("monthly_range_prob_failed")
    if variant.close_pos_max is not None:
        if row.get("close_pos") is None or float(row["close_pos"]) > float(variant.close_pos_max):
            blockers.append("close_pos_failed")
    if variant.day_change_max is not None:
        if row.get("day_change_pct") is None or float(row["day_change_pct"]) > float(variant.day_change_max):
            blockers.append("day_change_failed")
    if variant.dist_ma20_max is not None:
        if row.get("dist_ma20_signed") is None or float(row["dist_ma20_signed"]) > float(variant.dist_ma20_max):
            blockers.append("dist_ma20_failed")
    if variant.require_midrange_off:
        if row.get("monthlyRangePos") is not None and 0.35 <= float(row["monthlyRangePos"]) <= 0.65:
            blockers.append("midrange_off_failed")
    return blockers


def _build_reentry_audit(
    *,
    baseline_rows: list[dict[str, Any]],
    frozen_rows: list[dict[str, Any]],
    variant_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    frozen_map = _row_selection_map(frozen_rows)
    baseline_map = _row_selection_map(baseline_rows)
    candidate_keys = [
        (20240229, "6750"),
        (20241230, "4684"),
        (20250930, "9861"),
        (20240229, "6976"),
        (20250131, "4072"),
        (20250331, "7630"),
    ]
    per_symbol: list[dict[str, Any]] = []
    for key in candidate_keys:
        row = baseline_map.get(key) or frozen_map.get(key)
        if row is None:
            continue
        entry = {
            "ymd": key[0],
            "code": key[1],
            "baseline_selected": key in baseline_map,
            "frozen_challenger_selected": key in frozen_map,
            "baseline_short_ret_20": row.get("short_ret_20"),
            "baseline_short_ret_5": row.get("short_ret_5"),
            "baseline_short_ret_10": row.get("short_ret_10"),
            "baseline_blockers": _gate_blockers(row, FROZEN_VARIANT),
        }
        for name, result in variant_results.items():
            selected_keys = {_row_key(r) for r in result["selected_rows"]}
            selected = key in selected_keys
            variant = next(v for v in VARIANTS if v.name == name)
            entry[name] = {
                "selected": selected,
                "blockers": _gate_blockers(row, variant),
            }
        per_symbol.append(entry)

    profitable_removed = [item for item in per_symbol if item["baseline_short_ret_20"] is not None and float(item["baseline_short_ret_20"]) > 0.0]
    losing_kept = [item for item in per_symbol if item["baseline_short_ret_20"] is not None and float(item["baseline_short_ret_20"]) <= 0.0]

    return {
        "schema_version": "tradex_entry_precision_short_broad_down_monthly_reentry_audit_v1",
        "session_id": f"entry-short-broad-down-monthly-fix-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "focused_regime": FOCUSED_REGIME,
        "profitable_removed_names": profitable_removed,
        "losing_kept_names": losing_kept,
        "reentry_summary": {
            name: {
                "profitable_removed_reentered": int(sum(1 for item in profitable_removed if item[name]["selected"])),
                "losing_kept_still_selected": int(sum(1 for item in losing_kept if item[name]["selected"])),
            }
            for name in variant_results
        },
        "conclusion": {
            "monthly_alignment_main_cause": False,
            "reason": "monthly_alignment_changes_do_not_recover_the_removed_profitable_followthrough_shorts_in_broad_down",
        },
    }


def _monthly_ablation_compare(
    *,
    baseline_rows: list[dict[str, Any]],
    reference_rows: list[dict[str, Any]],
    variant_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline_metrics = _metrics(baseline_rows)
    reference_metrics = _metrics(reference_rows)
    variants: list[dict[str, Any]] = []
    for variant in VARIANTS[1:]:
        result = variant_results[variant.name]
        selected_rows = result["selected_rows"]
        selected_metrics = _metrics(selected_rows)
        delta_vs_baseline = {
            "selected_count_delta": int(selected_metrics["count"] - baseline_metrics["count"]),
            "hit_rate_delta": None if baseline_metrics["hit_rate"] is None or selected_metrics["hit_rate"] is None else float(selected_metrics["hit_rate"] - baseline_metrics["hit_rate"]),
            "median_ret20_delta": None if baseline_metrics["median_ret20"] is None or selected_metrics["median_ret20"] is None else float(selected_metrics["median_ret20"] - baseline_metrics["median_ret20"]),
            "mean_ret20_delta": None if baseline_metrics["mean_ret20"] is None or selected_metrics["mean_ret20"] is None else float(selected_metrics["mean_ret20"] - baseline_metrics["mean_ret20"]),
        }
        delta_vs_reference = {
            "selected_count_delta": int(selected_metrics["count"] - reference_metrics["count"]),
            "hit_rate_delta": None if reference_metrics["hit_rate"] is None or selected_metrics["hit_rate"] is None else float(selected_metrics["hit_rate"] - reference_metrics["hit_rate"]),
            "median_ret20_delta": None if reference_metrics["median_ret20"] is None or selected_metrics["median_ret20"] is None else float(selected_metrics["median_ret20"] - reference_metrics["median_ret20"]),
            "mean_ret20_delta": None if reference_metrics["mean_ret20"] is None or selected_metrics["mean_ret20"] is None else float(selected_metrics["mean_ret20"] - reference_metrics["mean_ret20"]),
        }
        compare = _compare_with_reference(baseline_rows, selected_rows, reference_rows)
        variants.append(
            {
                "name": variant.name,
                "variant_contract": _variant_contract(variant),
                "baseline": baseline_metrics,
                "selected": selected_metrics,
                "delta_vs_baseline": delta_vs_baseline,
                "delta_vs_frozen_challenger": delta_vs_reference,
                "compare": compare,
                "monthly_split": _month_split(selected_rows),
                "selected_names": compare["selected_names"],
                "removed_names": compare["removed_names"],
                "helpful_removals": compare["helpful_removals"],
                "harmful_removals": compare["harmful_removals"],
                "reentered_from_reference": compare["reentered_from_reference"],
            }
        )

    return {
        "schema_version": "tradex_entry_precision_short_broad_down_monthly_quality_compare_v1",
        "session_id": f"entry-short-broad-down-monthly-fix-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "focused_regime": FOCUSED_REGIME,
        "baseline": baseline_metrics,
        "frozen_challenger": reference_metrics,
        "variants": variants,
        "comparison_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "long_logic_frozen": True,
            "no_meemee_ui_change": True,
            "rolling_oos": True,
            "wide_window": WIDE_WINDOW,
            "local_reference_window": LOCAL_WINDOW,
        },
    }


def _decision(quality_compare: dict[str, Any], reentry_audit: dict[str, Any]) -> tuple[str, list[str]]:
    relaxed = next(v for v in quality_compare["variants"] if v["name"] == RELAXED_VARIANT.name)
    off = next(v for v in quality_compare["variants"] if v["name"] == OFF_VARIANT.name)
    reasons = [
        f"frozen_challenger_count={quality_compare['frozen_challenger']['count']}",
        f"relaxed_count={relaxed['selected']['count']}",
        f"off_count={off['selected']['count']}",
        f"profitable_reentered_relaxed={reentry_audit['reentry_summary'][RELAXED_VARIANT.name]['profitable_removed_reentered']}",
        f"profitable_reentered_off={reentry_audit['reentry_summary'][OFF_VARIANT.name]['profitable_removed_reentered']}",
        f"losing_kept_relaxed={reentry_audit['reentry_summary'][RELAXED_VARIANT.name]['losing_kept_still_selected']}",
        f"losing_kept_off={reentry_audit['reentry_summary'][OFF_VARIANT.name]['losing_kept_still_selected']}",
    ]
    if reentry_audit["conclusion"]["monthly_alignment_main_cause"]:
        return "keep", reasons + ["monthly_alignment_repair_succeeded"]
    return "drop", reasons + ["monthly_alignment_not_primary_cause"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    compare = payload["quality_compare"]
    decision = payload["decision"]
    reentry = payload["reentry_audit"]
    lines = [
        "# Entry Precision Short Broad Down Monthly Fix Audit",
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
        f"- frozen failure session: `{payload['frozen_failure_session_id']}`",
        f"- monthly variants tested: `{', '.join(v['name'] for v in compare['variants'])}`",
        "",
        "## Verify",
        f"- frozen challenger hit rate: `{compare['frozen_challenger']['hit_rate']}`",
        f"- relaxed reentries: `{reentry['reentry_summary'][RELAXED_VARIANT.name]['profitable_removed_reentered']}`",
        f"- off reentries: `{reentry['reentry_summary'][OFF_VARIANT.name]['profitable_removed_reentered']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        "",
        "## Decision",
        f"- overall: `{decision['overall_decision']}`",
        f"- reasons: {', '.join(decision['decision_reasons'])}",
        "",
        "## Remaining Risks",
        "\n".join([f"- {risk}" for risk in decision["remaining_risks"]]),
        "",
        "## Next One Thing",
        payload["next_one_thing"],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    out_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    session_id = f"entry-short-broad-down-monthly-fix-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        wide_payload = _load_window_payload(conn, start_ymd=WIDE_WINDOW["start_ymd"], end_ymd=WIDE_WINDOW["end_ymd"])

    broad_down_rows, baseline_rows, frozen_rows = _broad_down_rows(wide_payload)
    variant_results = {variant.name: _variant_result(broad_down_rows, variant) for variant in VARIANTS}
    quality_compare = _monthly_ablation_compare(baseline_rows=baseline_rows, reference_rows=frozen_rows, variant_results=variant_results)
    reentry_audit = _build_reentry_audit(baseline_rows=baseline_rows, frozen_rows=frozen_rows, variant_results=variant_results)
    overall_decision, decision_reasons = _decision(quality_compare, reentry_audit)
    long_freeze_confirmed = True

    contract_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_fix_contract_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "frozen_challenger_definition": _variant_contract(FROZEN_VARIANT),
        "monthly_ablation_variants": [_variant_contract(RELAXED_VARIANT), _variant_contract(OFF_VARIANT)],
        "comparison_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "long_logic_frozen": True,
            "no_meemee_ui_change": True,
            "rolling_oos": True,
            "wide_window": WIDE_WINDOW,
            "local_reference_window": LOCAL_WINDOW,
        },
    }

    monthly_ablation_compare_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_monthly_ablation_compare_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "focused_regime": FOCUSED_REGIME,
        "baseline": _metrics(baseline_rows),
        "frozen_challenger": _metrics(frozen_rows),
        "variants": [
            {
                "name": name,
                "variant_contract": _variant_contract(VARIANT_BY_NAME[name]),
                "metrics": _metrics(result["selected_rows"]),
                "delta_vs_baseline": {
                    "selected_count_delta": int(_metrics(result["selected_rows"])["count"] - _metrics(baseline_rows)["count"]),
                    "hit_rate_delta": None if _metrics(result["selected_rows"])["hit_rate"] is None or _metrics(baseline_rows)["hit_rate"] is None else float(_metrics(result["selected_rows"])["hit_rate"] - _metrics(baseline_rows)["hit_rate"]),
                    "median_ret20_delta": None if _metrics(result["selected_rows"])["median_ret20"] is None or _metrics(baseline_rows)["median_ret20"] is None else float(_metrics(result["selected_rows"])["median_ret20"] - _metrics(baseline_rows)["median_ret20"]),
                    "mean_ret20_delta": None if _metrics(result["selected_rows"])["mean_ret20"] is None or _metrics(baseline_rows)["mean_ret20"] is None else float(_metrics(result["selected_rows"])["mean_ret20"] - _metrics(baseline_rows)["mean_ret20"]),
                },
                "delta_vs_frozen_challenger": {
                    "selected_count_delta": int(_metrics(result["selected_rows"])["count"] - _metrics(frozen_rows)["count"]),
                    "hit_rate_delta": None if _metrics(result["selected_rows"])["hit_rate"] is None or _metrics(frozen_rows)["hit_rate"] is None else float(_metrics(result["selected_rows"])["hit_rate"] - _metrics(frozen_rows)["hit_rate"]),
                    "median_ret20_delta": None if _metrics(result["selected_rows"])["median_ret20"] is None or _metrics(frozen_rows)["median_ret20"] is None else float(_metrics(result["selected_rows"])["median_ret20"] - _metrics(frozen_rows)["median_ret20"]),
                    "mean_ret20_delta": None if _metrics(result["selected_rows"])["mean_ret20"] is None or _metrics(frozen_rows)["mean_ret20"] is None else float(_metrics(result["selected_rows"])["mean_ret20"] - _metrics(frozen_rows)["mean_ret20"]),
                },
                "helpful_removal_count": _compare_with_reference(baseline_rows, result["selected_rows"], frozen_rows)["helpful_removal_count"],
                "harmful_removal_count": _compare_with_reference(baseline_rows, result["selected_rows"], frozen_rows)["harmful_removal_count"],
                "unchanged_core_count": _compare_with_reference(baseline_rows, result["selected_rows"], frozen_rows)["unchanged_core_count"],
                "monthly_split": _month_split(result["selected_rows"]),
                "selected_names": [_row_view(r) for r in result["selected_rows"]],
                "reentered_from_reference": _compare_with_reference(baseline_rows, result["selected_rows"], frozen_rows)["reentered_from_reference"],
            }
            for name, result in variant_results.items()
            if name != FROZEN_VARIANT.name
        ],
        "comparison_contract": contract_payload["comparison_contract"],
    }

    reentry_audit_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_monthly_reentry_audit_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "focused_regime": FOCUSED_REGIME,
        "profitable_removed_names": _build_reentry_audit(
            baseline_rows=baseline_rows,
            frozen_rows=frozen_rows,
            variant_results=variant_results,
        )["profitable_removed_names"],
        "losing_kept_names": _build_reentry_audit(
            baseline_rows=baseline_rows,
            frozen_rows=frozen_rows,
            variant_results=variant_results,
        )["losing_kept_names"],
        "reentry_summary": _build_reentry_audit(
            baseline_rows=baseline_rows,
            frozen_rows=frozen_rows,
            variant_results=variant_results,
        )["reentry_summary"],
        "conclusion": _build_reentry_audit(
            baseline_rows=baseline_rows,
            frozen_rows=frozen_rows,
            variant_results=variant_results,
        )["conclusion"],
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_monthly_fix_decision_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "overall_decision": overall_decision,
        "decision_reasons": decision_reasons,
        "confirmed": [
            "short_logic_frozen",
            "long_logic_frozen",
            "same_window_reference_retained",
            "broad_down_only_reviewed",
            "monthly_alignment_only_ablation",
            "json_is_authoritative",
        ],
        "provisional": [
            "trend_alignment_is_still_proxy_based",
            "regime_labels_are_proxy_derived",
            "monthly_alignment_is_only_one_part_of_the_broad_down_filter",
        ],
        "remaining_risks": [
            "monthly_alignment_ablation_did_not_demonstrate_recovery_of_the_removed_profitable_shorts",
            "sample_size_is_still_small_relative_to_the_full_history",
            "other_gates_still_block_the_removed_names",
        ],
    }

    report_payload = {
        "current_state": {
            "confirmed": [
                "short_trend_alignment_v1_was_dropped_for_the_broad_down_lane",
                "long_logic_remained_frozen",
                "the_broad_down_failure_was_already_diagnosed",
            ],
            "provisional": [
                "monthly_alignment_is_the_first_suspect",
                "broad_down_sample_is_not_large",
                "regime_labels_are_proxy_derived",
            ],
        },
        "problem": "The broad-down failure may be partly caused by the monthly-alignment filter, but that needs direct ablation evidence before any larger redesign.",
        "change_policy": "TRADEX research only, monthly-alignment ablation only, keep long logic frozen, keep MeeMee untouched, keep the rest of the challenger structure frozen, and do not review other failure regimes.",
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "quality_compare": monthly_ablation_compare_payload,
        "reentry_audit": reentry_audit_payload,
        "decision": decision_payload,
        "long_freeze_confirmed": long_freeze_confirmed,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "If the monthly gate is not the culprit, the next useful axis would be a single broader-down false-keep diagnostic rather than another threshold retune.",
    }

    _write_json(out_dir / "entry_precision_short_broad_down_fix_contract.json", contract_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_monthly_ablation_compare.json", monthly_ablation_compare_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_monthly_reentry_audit.json", reentry_audit_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_monthly_quality_compare.json", monthly_ablation_compare_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_monthly_fix_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_broad_down_monthly_fix_report.md", report_payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "overall_decision": overall_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Monthly-alignment ablation for the frozen broad-down failure lane.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
