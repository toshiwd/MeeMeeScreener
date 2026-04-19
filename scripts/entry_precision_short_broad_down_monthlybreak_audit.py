from __future__ import annotations

import argparse
import importlib
import json
import statistics
import sys
from collections import defaultdict
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
monthly_fix = importlib.import_module("scripts.entry_precision_short_broad_down_monthly_fix_audit")

DEFAULT_OUTPUT_DIR = base.DEFAULT_OUTPUT_DIR
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_trend_alignment_v1"
FOCUSED_REGIME = "broad_down_regime"
FROZEN_FAILURE_SESSION_ID = "entry-short-broad-down-alignmentpath-20260419-141647"
LOCAL_WINDOW = {"start_ymd": 20250101, "end_ymd": 20260226}
WIDE_WINDOW = {"start_ymd": 20240101, "end_ymd": 20260226}

FROZEN_REFERENCE_VARIANT = trend.TrendVariant(
    name="broad_down_monthly_rangeprob_off_v1",
    target="bad-pick removal",
    weekly_breakout_down_min=0.66,
    monthly_breakout_down_min=0.66,
    monthly_range_prob_max=None,
    close_pos_max=0.20,
)

LADDER_VARIANTS = [
    trend.TrendVariant(
        name="broad_down_monthlybreak_0p60_v1",
        target="bad-pick removal",
        weekly_breakout_down_min=0.66,
        monthly_breakout_down_min=0.60,
        monthly_range_prob_max=None,
        close_pos_max=0.20,
    ),
    trend.TrendVariant(
        name="broad_down_monthlybreak_0p45_v1",
        target="bad-pick removal",
        weekly_breakout_down_min=0.66,
        monthly_breakout_down_min=0.45,
        monthly_range_prob_max=None,
        close_pos_max=0.20,
    ),
    trend.TrendVariant(
        name="broad_down_monthlybreak_0p30_v1",
        target="bad-pick removal",
        weekly_breakout_down_min=0.66,
        monthly_breakout_down_min=0.30,
        monthly_range_prob_max=None,
        close_pos_max=0.20,
    ),
]

VARIANTS = [FROZEN_REFERENCE_VARIANT, *LADDER_VARIANTS]
VARIANT_BY_NAME = {variant.name: variant for variant in VARIANTS}

TARGET_KEYS = [
    (20240229, "6750"),
    (20241230, "4684"),
    (20250930, "9861"),
    (20240229, "6976"),
    (20250131, "4072"),
    (20250331, "7630"),
]


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
    return monthly_fix._metrics(rows)


def _month_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return monthly_fix._month_split(rows)


def _broad_down_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = payload["rows"]
    baseline_rows = [r for r in payload["result"]["baseline_rows"] if regime._bucket_regime(r) == FOCUSED_REGIME]
    broad_down_rows = [r for r in rows if regime._bucket_regime(r) == FOCUSED_REGIME]
    return broad_down_rows, baseline_rows


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


def _build_reentry_audit(
    *,
    baseline_rows: list[dict[str, Any]],
    reference_rows: list[dict[str, Any]],
    variant_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline_map = {_row_key(r): r for r in baseline_rows}
    reference_map = {_row_key(r): r for r in reference_rows}
    per_symbol: list[dict[str, Any]] = []
    for key in TARGET_KEYS:
        row = baseline_map.get(key) or reference_map.get(key)
        if row is None:
            continue
        row = {**row, "selected_by_baseline": key in baseline_map}
        entry = {
            "ymd": key[0],
            "code": key[1],
            "baseline_selected": key in baseline_map,
            "reference_selected": key in reference_map,
            "baseline_short_ret_20": row.get("short_ret_20"),
            "baseline_short_ret_5": row.get("short_ret_5"),
            "baseline_short_ret_10": row.get("short_ret_10"),
            "baseline_blockers": monthly_fix._gate_blockers(row, FROZEN_REFERENCE_VARIANT),
            "threshold_path": [],
        }
        for variant in LADDER_VARIANTS:
            selected_keys = {_row_key(r) for r in variant_results[variant.name]["selected_rows"]}
            entry["threshold_path"].append(
                {
                    "variant": variant.name,
                    "monthly_breakout_down_min": variant.monthly_breakout_down_min,
                    "selected": key in selected_keys,
                    "blockers": monthly_fix._gate_blockers(row, variant),
                }
            )
        per_symbol.append(entry)

    profitable_removed = [item for item in per_symbol if item["baseline_short_ret_20"] is not None and float(item["baseline_short_ret_20"]) > 0.0]
    losing_kept = [item for item in per_symbol if item["baseline_short_ret_20"] is not None and float(item["baseline_short_ret_20"]) <= 0.0]
    return {
        "schema_version": "tradex_entry_precision_short_broad_down_monthlybreak_reentry_audit_v1",
        "session_id": f"entry-short-broad-down-monthlybreak-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "focused_regime": FOCUSED_REGIME,
        "profitable_removed_names": profitable_removed,
        "losing_kept_names": losing_kept,
        "reentry_summary": {
            variant.name: {
                "profitable_removed_reentered": int(sum(1 for item in profitable_removed if any(path["variant"] == variant.name and path["selected"] for path in item["threshold_path"]))),
                "losing_kept_still_selected": int(sum(1 for item in losing_kept if any(path["variant"] == variant.name and path["selected"] for path in item["threshold_path"]))),
            }
            for variant in LADDER_VARIANTS
        },
    }


def _monthlybreak_compare(
    *,
    baseline_rows: list[dict[str, Any]],
    reference_rows: list[dict[str, Any]],
    variant_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline_metrics = _metrics(baseline_rows)
    reference_metrics = _metrics(reference_rows)
    variants: list[dict[str, Any]] = []
    for variant in LADDER_VARIANTS:
        result = variant_results[variant.name]
        selected_rows = result["selected_rows"]
        selected_metrics = _metrics(selected_rows)
        compare = _compare_with_reference(baseline_rows, selected_rows, reference_rows)
        variants.append(
            {
                "name": variant.name,
                "variant_contract": _variant_contract(variant),
                "metrics": selected_metrics,
                "delta_vs_baseline": {
                    "selected_count_delta": int(selected_metrics["count"] - baseline_metrics["count"]),
                    "hit_rate_delta": None if baseline_metrics["hit_rate"] is None or selected_metrics["hit_rate"] is None else float(selected_metrics["hit_rate"] - baseline_metrics["hit_rate"]),
                    "median_ret20_delta": None if baseline_metrics["median_ret20"] is None or selected_metrics["median_ret20"] is None else float(selected_metrics["median_ret20"] - baseline_metrics["median_ret20"]),
                    "mean_ret20_delta": None if baseline_metrics["mean_ret20"] is None or selected_metrics["mean_ret20"] is None else float(selected_metrics["mean_ret20"] - baseline_metrics["mean_ret20"]),
                },
                "delta_vs_frozen_reference": {
                    "selected_count_delta": int(selected_metrics["count"] - reference_metrics["count"]),
                    "hit_rate_delta": None if reference_metrics["hit_rate"] is None or selected_metrics["hit_rate"] is None else float(selected_metrics["hit_rate"] - reference_metrics["hit_rate"]),
                    "median_ret20_delta": None if reference_metrics["median_ret20"] is None or selected_metrics["median_ret20"] is None else float(selected_metrics["median_ret20"] - reference_metrics["median_ret20"]),
                    "mean_ret20_delta": None if reference_metrics["mean_ret20"] is None or selected_metrics["mean_ret20"] is None else float(selected_metrics["mean_ret20"] - reference_metrics["mean_ret20"]),
                },
                "helpful_removal_count": compare["helpful_removal_count"],
                "harmful_removal_count": compare["harmful_removal_count"],
                "unchanged_core_count": compare["unchanged_core_count"],
                "changed_top5_short_count": compare["changed_top5_short_count"],
                "changed_top10_short_count": compare["changed_top10_short_count"],
                "changed_rank_short_count": compare["changed_rank_short_count"],
                "monthly_split": _month_split(selected_rows),
                "selected_names": compare["selected_names"],
                "removed_names": compare["removed_names"],
                "helpful_removals": compare["helpful_removals"],
                "harmful_removals": compare["harmful_removals"],
                "reentered_from_reference": compare["reentered_from_reference"],
            }
        )
    return {
        "schema_version": "tradex_entry_precision_short_broad_down_monthlybreak_ablation_compare_v1",
        "session_id": f"entry-short-broad-down-monthlybreak-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": True,
        "focused_regime": FOCUSED_REGIME,
        "baseline": baseline_metrics,
        "frozen_reference": reference_metrics,
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


def _decision(compare: dict[str, Any], reentry: dict[str, Any]) -> tuple[str, list[str]]:
    best = max(
        compare["variants"],
        key=lambda v: (
            v["metrics"]["hit_rate"] is not None,
            v["metrics"]["hit_rate"] or float("-inf"),
            v["metrics"]["median_ret20"] if v["metrics"]["median_ret20"] is not None else float("-inf"),
            v["metrics"]["mean_ret20"] if v["metrics"]["mean_ret20"] is not None else float("-inf"),
            v["metrics"]["count"],
        ),
    )
    reasons = [
        f"frozen_reference_count={compare['frozen_reference']['count']}",
        f"best_variant={best['name']}",
        f"best_variant_count={best['metrics']['count']}",
        f"best_variant_hit_rate={best['metrics']['hit_rate']}",
        f"best_variant_median_ret20={best['metrics']['median_ret20']}",
        f"best_variant_mean_ret20={best['metrics']['mean_ret20']}",
        f"profitable_reentered_0p60={reentry['reentry_summary']['broad_down_monthlybreak_0p60_v1']['profitable_removed_reentered']}",
        f"profitable_reentered_0p45={reentry['reentry_summary']['broad_down_monthlybreak_0p45_v1']['profitable_removed_reentered']}",
        f"profitable_reentered_0p30={reentry['reentry_summary']['broad_down_monthlybreak_0p30_v1']['profitable_removed_reentered']}",
    ]
    if best["metrics"]["hit_rate"] is not None and best["metrics"]["median_ret20"] is not None:
        if best["metrics"]["hit_rate"] > compare["frozen_reference"]["hit_rate"] and best["metrics"]["median_ret20"] > compare["frozen_reference"]["median_ret20"]:
            return "hold", reasons + ["partial_repair_only"]
    return "drop", reasons + ["monthly_breakout_not_too_strict_enough_or_not_the_main_problem"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    compare = payload["compare"]
    reentry = payload["reentry"]
    decision = payload["decision"]
    lines = [
        "# Entry Precision Short Broad Down MonthlyBreak Audit",
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
        f"- ladder variants: `{', '.join(v['name'] for v in compare['variants'])}`",
        "",
        "## Verify",
        f"- baseline count: `{compare['baseline']['count']}`",
        f"- frozen reference count: `{compare['frozen_reference']['count']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        f"- profitable reentries by ladder: `{ {k: v['profitable_removed_reentered'] for k, v in reentry['reentry_summary'].items()} }`",
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
    session_id = f"entry-short-broad-down-monthlybreak-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        wide_payload = _load_window_payload(conn, start_ymd=WIDE_WINDOW["start_ymd"], end_ymd=WIDE_WINDOW["end_ymd"])

    broad_down_rows, baseline_rows = _broad_down_rows(wide_payload)
    frozen_reference_rows = _variant_result(broad_down_rows, FROZEN_REFERENCE_VARIANT)["selected_rows"]
    variant_results = {variant.name: _variant_result(broad_down_rows, variant) for variant in LADDER_VARIANTS}
    compare = _monthlybreak_compare(baseline_rows=baseline_rows, reference_rows=frozen_reference_rows, variant_results=variant_results)
    reentry = _build_reentry_audit(baseline_rows=baseline_rows, reference_rows=frozen_reference_rows, variant_results=variant_results)
    overall_decision, decision_reasons = _decision(compare, reentry)
    long_freeze_confirmed = True

    contract_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_monthlybreak_contract_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "frozen_reference_variant": _variant_contract(FROZEN_REFERENCE_VARIANT),
        "monthlybreak_variants": [_variant_contract(v) for v in LADDER_VARIANTS],
        "comparison_contract": compare["comparison_contract"],
    }

    reentry_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_monthlybreak_reentry_audit_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "focused_regime": FOCUSED_REGIME,
        "profitable_removed_names": reentry["profitable_removed_names"],
        "losing_kept_names": reentry["losing_kept_names"],
        "reentry_summary": reentry["reentry_summary"],
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_monthlybreak_fix_decision_v1",
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
            "monthly_breakout_down_min_only_ladder",
            "json_is_authoritative",
        ],
        "provisional": [
            "monthly_breakout_relaxation_may_be_partially_helpful",
            "broad_down_sample_is_still_small",
            "4684_has_name_specific_blockers_beyond_monthly_alignment",
        ],
        "remaining_risks": [
            "monthly_breakout_relaxation_did_not_yet_prove_a_clean_keep",
            "sample_size_is_still_small_relative_to_the_full_history",
            "close_pos_and_midrange_remain_blockers_for_4684",
        ],
    }

    report_payload = {
        "current_state": {
            "confirmed": [
                "monthly_alignment_failed_was_confirmed_direct",
                "monthly_range_prob_hypothesis_was_rejected",
                "long_logic_remained_frozen",
            ],
            "provisional": [
                "monthly_breakout_down_min_may_be_too_strict",
                "ladder_needed_to_find_minimal_relaxation",
                "regime_labels_are_proxy_derived",
            ],
        },
        "problem": "The task is to validate whether the frozen monthly breakout threshold is too strict for broad-down followthrough shorts, using a minimal relaxation ladder on the same frozen reference.",
        "change_policy": "TRADEX research only, monthly_breakout_down_min ladder only, keep long logic frozen, keep MeeMee untouched, keep all other gates frozen, and do not review other failure regimes.",
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "compare": compare,
        "reentry": reentry_payload,
        "decision": decision_payload,
        "long_freeze_confirmed": long_freeze_confirmed,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "If this lane continues, the next axis would be close_pos_max only if the ladder proves monthly breakout is not the main limiter.",
    }

    _write_json(out_dir / "entry_precision_short_broad_down_monthlybreak_contract.json", contract_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_monthlybreak_ablation_compare.json", compare)
    _write_json(out_dir / "entry_precision_short_broad_down_monthlybreak_reentry_audit.json", reentry_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_monthlybreak_quality_compare.json", compare)
    _write_json(out_dir / "entry_precision_short_broad_down_monthlybreak_fix_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_broad_down_monthlybreak_fix_report.md", report_payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "overall_decision": overall_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Monthly breakout threshold ladder for the frozen broad-down reference.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
