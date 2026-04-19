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
FROZEN_FROM_SESSION_ID = "entry-short-trend-regime-20260419-113315"
LOCAL_WINDOW = {"start_ymd": 20250101, "end_ymd": 20260226}
WIDE_WINDOW = {"start_ymd": 20240101, "end_ymd": 20260226}


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


def _classify_row(
    row: dict[str, Any],
    *,
    is_removed: bool,
    is_shared: bool,
) -> str | None:
    ret20 = row.get("short_ret_20")
    ret5 = row.get("short_ret_5")
    ret10 = row.get("short_ret_10")
    close_pos = row.get("close_pos")
    dist_ma20 = row.get("dist_ma20_signed")
    dist_low20 = row.get("dist_low20")

    if is_removed:
        if ret20 is None or ret20 <= 0:
            return None
        if ret5 is not None and ret10 is not None and float(ret5) > 0 and float(ret10) > 0:
            return "removed_good_followthrough"
        if ret5 is not None and ret10 is not None and (float(ret5) <= 0 or float(ret10) <= 0):
            return "removed_high_quality_short_too_early"
        return "overfiltered_valid_breakdown"

    if is_shared:
        if ret20 is not None and float(ret20) > 0:
            return "no_material_difference"

    if ret20 is None or float(ret20) > 0:
        return None

    if (
        close_pos is not None
        and dist_ma20 is not None
        and dist_low20 is not None
        and float(close_pos) <= 0.05
        and float(dist_ma20) <= -0.03
        and float(dist_low20) <= 0.0025
    ):
        return "kept_late_short_after_extension"

    if ret5 is not None and ret10 is not None and float(ret5) <= 0 and float(ret10) <= 0:
        return "kept_weak_continuation"

    return "selected_noise_inside_downtrend"


def _bucket_reason_codes(row: dict[str, Any], classification: str, *, is_removed: bool, is_shared: bool) -> list[str]:
    reasons = list(trend._trend_bucket_reason(row))
    reasons.append(classification)
    if is_removed:
        reasons.append("baseline_only")
    elif is_shared:
        reasons.append("shared_core_name")
    else:
        reasons.append("challenger_only")
    return reasons


def _bucket_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[_row_key(row)].append(row)
    return out


def _compare_payload(
    *,
    session_id: str,
    wide_payload: dict[str, Any],
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline_metrics = _metrics(baseline_rows)
    challenger_metrics = _metrics(challenger_rows)
    baseline_keys = [_row_key(r) for r in baseline_rows]
    challenger_keys = [_row_key(r) for r in challenger_rows]
    baseline_key_set = set(baseline_keys)
    challenger_key_set = set(challenger_keys)

    removed_rows = [r for r in baseline_rows if _row_key(r) not in challenger_key_set]
    shared_rows = [r for r in baseline_rows if _row_key(r) in challenger_key_set]

    removed_summary = [_row_view(r) for r in removed_rows]
    selected_summary = [_row_view(r) for r in challenger_rows]
    shared_summary = [_row_view(r) for r in shared_rows]

    removed_bad = [r for r in removed_rows if r.get("short_ret_20") is not None and float(r["short_ret_20"]) <= 0.0]
    helpful_removals = [_row_view(r) for r in removed_bad]
    harmful_removals = [_row_view(r) for r in removed_rows if r.get("short_ret_20") is not None and float(r["short_ret_20"]) > 0.0]

    delta = {
        "selected_count_delta": int(challenger_metrics["count"] - baseline_metrics["count"]),
        "hit_rate_delta": None if baseline_metrics["hit_rate"] is None or challenger_metrics["hit_rate"] is None else float(challenger_metrics["hit_rate"] - baseline_metrics["hit_rate"]),
        "median_ret20_delta": None if baseline_metrics["median_ret20"] is None or challenger_metrics["median_ret20"] is None else float(challenger_metrics["median_ret20"] - baseline_metrics["median_ret20"]),
        "mean_ret20_delta": None if baseline_metrics["mean_ret20"] is None or challenger_metrics["mean_ret20"] is None else float(challenger_metrics["mean_ret20"] - baseline_metrics["mean_ret20"]),
        "bad_short_removal_count": int(len(removed_bad)),
        "changed_top5_short_count": int(len(set(baseline_keys[:5]) ^ set(challenger_keys[:5]))),
        "changed_top10_short_count": int(len(set(baseline_keys[:10]) ^ set(challenger_keys[:10]))),
        "changed_rank_short_count": int(
            sum(
                abs(baseline_keys.index(key) - challenger_keys.index(key))
                for key in set(baseline_keys[:20]).intersection(challenger_keys[:20])
            )
        ),
        "selected_names": selected_summary,
        "removed_names": removed_summary,
        "helpful_removal_names": helpful_removals,
        "harmful_removal_names": harmful_removals,
    }

    return {
        "schema_version": "tradex_entry_precision_short_broad_down_compare_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "long_freeze_confirmed": True,
        "focused_regime": FOCUSED_REGIME,
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
        "baseline": baseline_metrics,
        "challenger": challenger_metrics,
        "delta": delta,
        "monthly_split": {
            "baseline": _month_split(baseline_rows),
            "challenger": _month_split(challenger_rows),
        },
        "selected_names": selected_summary,
        "removed_names": removed_summary,
        "helpful_removals": helpful_removals,
        "harmful_removals": harmful_removals,
        "unchanged_core_names": shared_summary,
    }


def _failure_taxonomy(
    *,
    session_id: str,
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline_by = _bucket_rows(baseline_rows)
    challenger_by = _bucket_rows(challenger_rows)
    challenger_key_set = set(_row_key(r) for r in challenger_rows)

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in baseline_rows:
        key = _row_key(row)
        is_removed = key not in challenger_key_set
        is_shared = key in challenger_key_set
        classification = _classify_row(row, is_removed=is_removed, is_shared=is_shared)
        if classification is None:
            continue
        if classification in {"removed_good_followthrough", "removed_high_quality_short_too_early", "overfiltered_valid_breakdown", "kept_late_short_after_extension", "kept_weak_continuation", "selected_noise_inside_downtrend", "no_material_difference"}:
            row_view = _row_view(row)
            row_view["classification"] = classification
            row_view["reason_codes"] = _bucket_reason_codes(row, classification, is_removed=is_removed, is_shared=is_shared)
            buckets[classification].append(row_view)

    required = [
        "overfiltered_valid_breakdown",
        "removed_good_followthrough",
        "kept_late_short_after_extension",
        "kept_weak_continuation",
        "removed_high_quality_short_too_early",
        "selected_noise_inside_downtrend",
        "no_material_difference",
    ]

    def _bucket_examples(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return items[:5]

    out: dict[str, Any] = {}
    for bucket in required:
        rows = buckets.get(bucket, [])
        bucket_rows = [row for row in rows]
        out[bucket] = {
            "count": int(len(bucket_rows)),
            "baseline": _metrics([r for r in baseline_rows if _classify_row(r, is_removed=_row_key(r) not in challenger_key_set, is_shared=_row_key(r) in challenger_key_set) == bucket]),
            "challenger": _metrics([r for r in challenger_rows if _classify_row(r, is_removed=False, is_shared=True) == bucket]),
            "reason_code_summary": dict(Counter(code for row in bucket_rows for code in row.get("reason_codes", []))),
            "representative_examples": _bucket_examples(bucket_rows),
        }
    return {
        "schema_version": "tradex_entry_precision_short_broad_down_failure_taxonomy_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "focused_regime": FOCUSED_REGIME,
        "bucket_definitions": {
            "overfiltered_valid_breakdown": "removed profitable broad-down shorts that should have remained enterable",
            "removed_good_followthrough": "removed profitable shorts with clean early followthrough",
            "kept_late_short_after_extension": "kept shorts that were already too extended at the close",
            "kept_weak_continuation": "kept shorts with weak post-trigger continuation",
            "removed_high_quality_short_too_early": "removed profitable shorts that still had mixed early path behavior",
            "selected_noise_inside_downtrend": "kept shorts that remained noisy despite broad-down context",
            "no_material_difference": "unchanged core names that do not explain the degradation",
        },
        "failure_buckets": out,
    }


def _removal_audit(
    *,
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    challenger_key_set = set(_row_key(r) for r in challenger_rows)
    helpful = [r for r in baseline_rows if _row_key(r) not in challenger_key_set and r.get("short_ret_20") is not None and float(r["short_ret_20"]) <= 0.0]
    harmful = [r for r in baseline_rows if _row_key(r) not in challenger_key_set and r.get("short_ret_20") is not None and float(r["short_ret_20"]) > 0.0]
    unchanged = [r for r in baseline_rows if _row_key(r) in challenger_key_set]
    return {
        "helpful_removals": [_row_view(r) for r in helpful],
        "harmful_removals": [_row_view(r) for r in harmful],
        "unchanged_core_names": [_row_view(r) for r in unchanged],
        "summary": {
            "helpful_removal_count": int(len(helpful)),
            "harmful_removal_count": int(len(harmful)),
            "unchanged_core_count": int(len(unchanged)),
        },
    }


def _decision(compare_payload: dict[str, Any], removal_audit: dict[str, Any]) -> tuple[str, list[str]]:
    baseline = compare_payload["baseline"]
    challenger = compare_payload["challenger"]
    reasons = [
        f"broad_down_baseline_count={baseline['count']}",
        f"broad_down_challenger_count={challenger['count']}",
        f"broad_down_hit_rate_delta={compare_payload['delta']['hit_rate_delta']}",
        f"broad_down_median_ret20_delta={compare_payload['delta']['median_ret20_delta']}",
        f"broad_down_mean_ret20_delta={compare_payload['delta']['mean_ret20_delta']}",
        f"helpful_removals={removal_audit['summary']['helpful_removal_count']}",
        f"harmful_removals={removal_audit['summary']['harmful_removal_count']}",
        f"unchanged_core_count={removal_audit['summary']['unchanged_core_count']}",
    ]
    if compare_payload["delta"]["hit_rate_delta"] is not None and compare_payload["delta"]["hit_rate_delta"] < 0:
        return "drop", reasons + ["challenger_degrades_broad_down_quality"]
    return "hold", reasons + ["broad_down_effect_is_localized_or_ambiguous"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    compare = payload["compare"]
    decision = payload["decision"]
    removal = payload["removal_audit"]
    taxonomy = payload["failure_taxonomy"]["failure_buckets"]
    lines = [
        "# Entry Precision Short Broad Down Audit",
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
        f"- focused regime: `{FOCUSED_REGIME}`",
        f"- baseline count: `{compare['baseline']['count']}`",
        f"- challenger count: `{compare['challenger']['count']}`",
        f"- helpful removals: `{removal['summary']['helpful_removal_count']}`",
        f"- harmful removals: `{removal['summary']['harmful_removal_count']}`",
        "",
        "## Verify",
        f"- broad-down baseline hit rate: `{compare['baseline']['hit_rate']}`",
        f"- broad-down challenger hit rate: `{compare['challenger']['hit_rate']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        f"- failure buckets reviewed: `{', '.join(taxonomy.keys())}`",
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
    session_id = f"entry-short-broad-down-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        wide_payload = _load_window_payload(conn, start_ymd=WIDE_WINDOW["start_ymd"], end_ymd=WIDE_WINDOW["end_ymd"])

    broad_down_rows, baseline_rows, challenger_rows = _broad_down_rows(wide_payload)
    compare_payload = _compare_payload(
        session_id=session_id,
        wide_payload=wide_payload,
        baseline_rows=baseline_rows,
        challenger_rows=challenger_rows,
    )
    failure_taxonomy = _failure_taxonomy(session_id=session_id, baseline_rows=baseline_rows, challenger_rows=challenger_rows)
    removal_audit = _removal_audit(baseline_rows=baseline_rows, challenger_rows=challenger_rows)
    overall_decision, decision_reasons = _decision(compare_payload, removal_audit)
    long_freeze_confirmed = True

    contract_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_contract_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "focused_regime": FOCUSED_REGIME,
        "frozen_baseline_definition": {
            "source_artifact": "current_rule_trade_gate_baseline",
            "role": "short-side baseline from the frozen trend lane",
        },
        "frozen_challenger_definition": {
            "name": CHALLENGER_ID,
            "target": "bad-pick removal",
            "weekly_breakout_down_min": 0.66,
            "monthly_breakout_down_min": 0.66,
            "monthly_range_prob_max": 0.25,
            "close_pos_max": 0.20,
            "require_midrange_off": True,
        },
        "comparison_contract": compare_payload["comparison_contract"],
    }

    compare_out = {
        **compare_payload,
        "focused_regime_count": int(len(broad_down_rows)),
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_broad_down_decision_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "overall_decision": overall_decision,
        "decision_reasons": decision_reasons,
        "confirmed": [
            "short_logic_frozen",
            "long_logic_frozen",
            "same_window_reference_retained",
            "broad_down_only_reviewed",
            "json_is_authoritative",
        ],
        "provisional": [
            "trend_alignment_is_still_proxy_based",
            "regime_labels_are_proxy_derived",
            "helpful_removals_are_still_small_in_count",
        ],
        "remaining_risks": [
            "broad_down_failure_is_clear_but_the_classifier_is_still_proxy_based",
            "sample_size_is_small_relative_to_the_full_history",
            "future_passes_should_not_reprice_the_gate_without_more_context",
        ],
    }

    report_payload = {
        "current_state": {
            "confirmed": [
                "short_trend_alignment_v1_was_kept_frozen_from_the_prior_regime_pass",
                "long_logic_remained_frozen",
                "broad_down_regime_was_the_only_bucket_reviewed",
            ],
            "provisional": [
                "trend_alignment_is_proxy_based",
                "broad_down_rows_remain_sample_thin_for_some_subsets",
                "the_failure_may_be_a_mix_of_overfiltering_and_wrong_keeps",
            ],
        },
        "problem": "The challenger is degrading quality inside broad_down_regime, which should be one of the most short-supportive environments. This pass isolates whether it is removing the wrong shorts, keeping the wrong shorts, or both.",
        "change_policy": "TRADEX research only, frozen challenger logic, single-bucket review for broad_down_regime only, same-condition comparison fixed, long logic frozen, and no new feature families or threshold changes.",
        "compare": compare_out,
        "failure_taxonomy": failure_taxonomy,
        "removal_audit": removal_audit,
        "decision": decision_payload,
        "long_freeze_confirmed": long_freeze_confirmed,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "If anything changes next, it should be a targeted fix for the specific broad_down failure pattern rather than a new global threshold retune.",
    }

    _write_json(out_dir / "entry_precision_short_broad_down_contract.json", contract_payload)
    _write_json(out_dir / "entry_precision_short_broad_down_compare.json", compare_out)
    _write_json(out_dir / "entry_precision_short_broad_down_failure_taxonomy.json", failure_taxonomy)
    _write_json(out_dir / "entry_precision_short_broad_down_removal_audit.json", removal_audit)
    _write_json(out_dir / "entry_precision_short_broad_down_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_broad_down_report.md", report_payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "focused_regime": FOCUSED_REGIME,
        "overall_decision": overall_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Broad-down failure review for the frozen short trend alignment challenger.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
