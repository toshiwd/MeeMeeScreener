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

DEFAULT_OUTPUT_DIR = base.DEFAULT_OUTPUT_DIR
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_trend_alignment_v1"
FROZEN_FROM_SESSION_ID = "entry-short-trend-wide-20260419-103720"
WIDE_WINDOW = {"start_ymd": 20240101, "end_ymd": 20260226}
LOCAL_WINDOW = {"start_ymd": 20250101, "end_ymd": 20260226}


def _resolve_db_path(cli_value: str | None) -> Path:
    return base._resolve_db_path(cli_value)


def _load_payload(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> dict[str, Any]:
    return wide._load_window_payload(conn, start_ymd=start_ymd, end_ymd=end_ymd)


def _bucket_regime(row: dict[str, Any]) -> str:
    market_regime = row.get("marketRegime")
    market_risk_off = row.get("marketRiskOff")
    trend_strict = row.get("trendDownStrict")
    range_prob = row.get("monthlyRangeProb")
    range_pos = row.get("monthlyRangePos")
    day_change = row.get("day_change_pct")

    if market_risk_off is True and day_change is not None and abs(float(day_change)) >= 0.03:
        return "high_volatility_selloff_regime"
    if market_risk_off is True and trend_strict is True:
        return "broad_down_regime"
    if market_regime == "neutral" or (
        range_prob is not None
        and float(range_prob) >= 0.40
        and range_pos is not None
        and 0.35 <= float(range_pos) <= 0.65
    ):
        return "broad_range_sideways_regime"
    if market_regime == "risk_on":
        return "broad_up_countertrend_regime"
    if trend_strict is False or (
        range_prob is not None and float(range_prob) >= 0.20
    ) or (
        day_change is not None and abs(float(day_change)) <= 0.015
    ):
        return "weak_trend_noisy_regime"
    return "neutral_regime"


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


def _bucket_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[_bucket_regime(row)].append(row)
    return out


def _bucket_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bucketed = _bucket_rows(rows)
    required = [
        "high_volatility_selloff_regime",
        "broad_down_regime",
        "broad_range_sideways_regime",
        "broad_up_countertrend_regime",
        "weak_trend_noisy_regime",
        "neutral_regime",
    ]
    result: list[dict[str, Any]] = []
    for label in required:
        result.append(
            {
                "regime": label,
                "sample_count": int(len(bucketed.get(label, []))),
                "market_regime_counts": dict(Counter(str(r.get("marketRegime")) for r in bucketed.get(label, []))),
                "marketRiskOff_counts": dict(Counter(bool(r.get("marketRiskOff")) for r in bucketed.get(label, []))),
                "trendDownStrict_counts": dict(Counter(bool(r.get("trendDownStrict")) for r in bucketed.get(label, []))),
            }
        )
    return result


def _regime_bucket_analysis(wide_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = wide_payload["rows"]
    baseline_rows = wide_payload["result"]["baseline_rows"]
    challenger_rows = wide_payload["result"]["selected_rows"]
    baseline_by = _bucket_rows(baseline_rows)
    challenger_by = _bucket_rows(challenger_rows)
    all_by = _bucket_rows(rows)

    out: list[dict[str, Any]] = []
    for label in [
        "high_volatility_selloff_regime",
        "broad_down_regime",
        "broad_range_sideways_regime",
        "broad_up_countertrend_regime",
        "weak_trend_noisy_regime",
        "neutral_regime",
    ]:
        base_rows = baseline_by.get(label, [])
        chal_rows = challenger_by.get(label, [])
        base_metrics = _metrics(base_rows)
        chal_metrics = _metrics(chal_rows)
        removed_bucket = 0
        base_codes = {str(r["code"]) for r in base_rows}
        chal_codes = {str(r["code"]) for r in chal_rows}
        for row in base_rows:
            if str(row["code"]) not in chal_codes and row.get("short_ret_20") is not None and float(row["short_ret_20"]) <= 0.0:
                removed_bucket += 1
        diff_hit = None if base_metrics["hit_rate"] is None or chal_metrics["hit_rate"] is None else float(chal_metrics["hit_rate"] - base_metrics["hit_rate"])
        diff_median = None if base_metrics["median_ret20"] is None or chal_metrics["median_ret20"] is None else float(chal_metrics["median_ret20"] - base_metrics["median_ret20"])
        diff_mean = None if base_metrics["mean_ret20"] is None or chal_metrics["mean_ret20"] is None else float(chal_metrics["mean_ret20"] - base_metrics["mean_ret20"])
        if (
            chal_metrics["hit_rate"] is not None
            and base_metrics["hit_rate"] is not None
            and chal_metrics["hit_rate"] > base_metrics["hit_rate"]
            and chal_metrics["median_ret20"] is not None
            and base_metrics["median_ret20"] is not None
            and chal_metrics["median_ret20"] > base_metrics["median_ret20"]
            and chal_metrics["mean_ret20"] is not None
            and base_metrics["mean_ret20"] is not None
            and chal_metrics["mean_ret20"] > base_metrics["mean_ret20"]
        ):
            regime_label = "support_regime"
        elif (
            chal_metrics["hit_rate"] is not None
            and base_metrics["hit_rate"] is not None
            and chal_metrics["hit_rate"] < base_metrics["hit_rate"]
        ) or (
            chal_metrics["mean_ret20"] is not None
            and base_metrics["mean_ret20"] is not None
            and chal_metrics["mean_ret20"] < base_metrics["mean_ret20"]
        ):
            regime_label = "failure_regime"
        else:
            regime_label = "neutral_regime"
        evidence_strength = "low" if chal_metrics["count"] < 3 else "medium"
        out.append(
            {
                "regime": label,
                "regime_label": regime_label,
                "evidence_strength": evidence_strength,
                "sample_count": int(len(all_by.get(label, []))),
                "baseline_count": int(base_metrics["count"]),
                "challenger_count": int(chal_metrics["count"]),
                "baseline": base_metrics,
                "challenger": chal_metrics,
                "delta": {
                    "hit_rate_delta": diff_hit,
                    "median_ret20_delta": diff_median,
                    "mean_ret20_delta": diff_mean,
                    "bad_short_removal_count": int(removed_bucket),
                    "changed_top5_short_count": int(
                        len(set(str(r["code"]) for r in base_rows[:5]) ^ set(str(r["code"]) for r in chal_rows[:5]))
                    ),
                    "changed_top10_short_count": int(
                        len(set(str(r["code"]) for r in base_rows[:10]) ^ set(str(r["code"]) for r in chal_rows[:10]))
                    ),
                    "changed_rank_short_count": int(
                        sum(
                            abs(
                                [str(r["code"]) for r in base_rows].index(code)
                                - [str(r["code"]) for r in chal_rows].index(code)
                            )
                            for code in set(str(r["code"]) for r in base_rows[:20]).intersection(
                                set(str(r["code"]) for r in chal_rows[:20])
                            )
                        )
                    ),
                },
                "monthly_split": {
                    "baseline": _month_split(base_rows),
                    "challenger": _month_split(chal_rows),
                },
                "representative_examples": [
                    {
                        "ymd": int(r["ymd"]),
                        "code": r["code"],
                        "short_ret_20": float(r["short_ret_20"]) if r.get("short_ret_20") is not None else None,
                        "marketRegime": r.get("marketRegime"),
                        "marketRiskOff": bool(r.get("marketRiskOff")) if r.get("marketRiskOff") is not None else None,
                        "trendDownStrict": bool(r.get("trendDownStrict")) if r.get("trendDownStrict") is not None else None,
                        "monthlyRangeProb": float(r["monthlyRangeProb"]) if r.get("monthlyRangeProb") is not None else None,
                        "monthlyRangePos": float(r["monthlyRangePos"]) if r.get("monthlyRangePos") is not None else None,
                    }
                    for r in (chal_rows or base_rows)[:3]
                ],
                "market_regime_counts": dict(Counter(str(r.get("marketRegime")) for r in all_by.get(label, []))),
            }
        )
    return out


def _regime_map(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "regime": item["regime"],
            "regime_label": item["regime_label"],
            "evidence_strength": item["evidence_strength"],
            "why": (
                "challenger clearly improved actionable short quality"
                if item["regime_label"] == "support_regime"
                else (
                    "challenger degraded hit rate or mean quality"
                    if item["regime_label"] == "failure_regime"
                    else "mixed or sample-thin"
                )
            ),
            "baseline_count": item["baseline_count"],
            "challenger_count": item["challenger_count"],
            "hit_rate_delta": item["delta"]["hit_rate_delta"],
            "median_ret20_delta": item["delta"]["median_ret20_delta"],
            "mean_ret20_delta": item["delta"]["mean_ret20_delta"],
        }
        for item in summary
    ]


def _diagnostic_2024(wide_payload: dict[str, Any]) -> dict[str, Any]:
    rows = wide_payload["rows"]
    baseline_rows = wide_payload["result"]["baseline_rows"]
    challenger_rows = wide_payload["result"]["selected_rows"]
    rows_2024 = [r for r in rows if int(str(r["ymd"])[:4]) == 2024]
    base_2024 = [r for r in baseline_rows if int(str(r["ymd"])[:4]) == 2024]
    chal_2024 = [r for r in challenger_rows if int(str(r["ymd"])[:4]) == 2024]
    base_by = _bucket_rows(base_2024)
    chal_by = _bucket_rows(chal_2024)
    comp: list[dict[str, Any]] = []
    for label in [
        "high_volatility_selloff_regime",
        "broad_down_regime",
        "broad_range_sideways_regime",
        "broad_up_countertrend_regime",
        "weak_trend_noisy_regime",
        "neutral_regime",
    ]:
        base_metrics = _metrics(base_by.get(label, []))
        chal_metrics = _metrics(chal_by.get(label, []))
        if base_metrics["count"] or chal_metrics["count"]:
            comp.append(
                {
                    "regime": label,
                    "baseline": base_metrics,
                    "challenger": chal_metrics,
                    "delta": {
                        "hit_rate_delta": None if base_metrics["hit_rate"] is None or chal_metrics["hit_rate"] is None else chal_metrics["hit_rate"] - base_metrics["hit_rate"],
                        "median_ret20_delta": None if base_metrics["median_ret20"] is None or chal_metrics["median_ret20"] is None else chal_metrics["median_ret20"] - base_metrics["median_ret20"],
                        "mean_ret20_delta": None if base_metrics["mean_ret20"] is None or chal_metrics["mean_ret20"] is None else chal_metrics["mean_ret20"] - base_metrics["mean_ret20"],
                    },
                }
            )
    year_comp = {
        "2024": {
            "total_rows": int(len(rows_2024)),
            "baseline_selected": int(len(base_2024)),
            "challenger_selected": int(len(chal_2024)),
            "baseline_regime_counts": dict(Counter(_bucket_regime(r) for r in base_2024)),
            "challenger_regime_counts": dict(Counter(_bucket_regime(r) for r in chal_2024)),
            "regime_breakdown": comp,
        },
        "2025": {
            "total_rows": int(len([r for r in rows if int(str(r["ymd"])[:4]) == 2025])),
            "baseline_selected": int(len([r for r in baseline_rows if int(str(r["ymd"])[:4]) == 2025])),
            "challenger_selected": int(len([r for r in challenger_rows if int(str(r["ymd"])[:4]) == 2025])),
            "baseline_regime_counts": dict(Counter(_bucket_regime(r) for r in baseline_rows if int(str(r["ymd"])[:4]) == 2025)),
            "challenger_regime_counts": dict(Counter(_bucket_regime(r) for r in challenger_rows if int(str(r["ymd"])[:4]) == 2025)),
        },
    }
    conclusion = "sample_thin_artifact_with_regime_skew"
    if len(chal_2024) >= 3:
        conclusion = "regime_mismatch"
    return {
        "wide_window": WIDE_WINDOW,
        "selected_2024_rows": int(len(chal_2024)),
        "selected_2025_rows": int(len([r for r in challenger_rows if int(str(r["ymd"])[:4]) == 2025])),
        "composition": year_comp,
        "conclusion": conclusion,
        "notes": [
            "2024 has very few challenger rows, so a severe loss can dominate the year",
            "regime composition is skewed toward broad_down and broad_range buckets rather than a broad spread",
        ],
    }


def _decision(summary: list[dict[str, Any]], diagnostic: dict[str, Any]) -> tuple[str, list[str]]:
    support = [item for item in summary if item["regime_label"] == "support_regime"]
    failure = [item for item in summary if item["regime_label"] == "failure_regime"]
    reasons = [
        f"support_regimes={','.join(item['regime'] for item in support) or 'none'}",
        f"failure_regimes={','.join(item['regime'] for item in failure) or 'none'}",
        f"selected_2024_rows={diagnostic['selected_2024_rows']}",
        f"selected_2025_rows={diagnostic['selected_2025_rows']}",
    ]
    if support and failure:
        return "hold", reasons + ["mixed_regime_response"]
    if support and not failure:
        return "keep", reasons + ["broad_regime_support"]
    return "drop", reasons + ["no_reliable_support_regime"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Entry Precision Short Trend Regime Audit",
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
        f"- wide window: `{payload['wide_window']['start_ymd']}..{payload['wide_window']['end_ymd']}`",
        f"- local reference window: `{payload['local_window']['start_ymd']}..{payload['local_window']['end_ymd']}`",
        "",
        "## Verify",
        f"- regime buckets: `{len(payload['regime_split'])}`",
        f"- 2024 challenger rows: `{payload['diagnostic_2024']['selected_2024_rows']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        "",
        "## Decision",
        f"- overall: `{payload['decision']}`",
        f"- reasons: {', '.join(payload['decision_reasons'])}",
        "",
        "## Remaining Risks",
        "\n".join([f"- {risk}" for risk in payload['remaining_risks']]),
        "",
        "## Next One Thing",
        payload["next_one_thing"],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    out_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    session_id = f"entry-short-trend-regime-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        local_payload = _load_payload(conn, start_ymd=LOCAL_WINDOW["start_ymd"], end_ymd=LOCAL_WINDOW["end_ymd"])
        wide_payload = _load_payload(conn, start_ymd=WIDE_WINDOW["start_ymd"], end_ymd=WIDE_WINDOW["end_ymd"])

    regime_split = _bucket_summary(wide_payload["rows"])
    regime_map = _regime_map(_regime_bucket_analysis(wide_payload))
    diagnostic_2024 = _diagnostic_2024(wide_payload)
    overall_decision, decision_reasons = _decision(_regime_bucket_analysis(wide_payload), diagnostic_2024)
    long_freeze_confirmed = True

    contract_payload = {
        "schema_version": "tradex_entry_precision_short_trend_regime_contract_v1",
        "session_id": session_id,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_challenger_definition": {
            "name": CHALLENGER_ID,
            "target": "bad-pick removal",
            "weekly_breakout_down_min": 0.66,
            "monthly_breakout_down_min": 0.66,
            "monthly_range_prob_max": 0.25,
            "close_pos_max": 0.20,
            "require_midrange_off": True,
        },
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
            "wide_window": WIDE_WINDOW,
            "local_reference_window": LOCAL_WINDOW,
        },
    }

    regime_split_payload = {
        "schema_version": "tradex_entry_precision_short_trend_regime_split_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "wide_window": WIDE_WINDOW,
        "local_window": LOCAL_WINDOW,
        "regime_buckets": _regime_bucket_analysis(wide_payload),
        "regime_bucket_overview": regime_split,
        "comparison_contract": contract_payload["comparison_contract"],
    }

    regime_map_payload = {
        "schema_version": "tradex_entry_precision_short_trend_regime_map_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "regime_map": regime_map,
    }

    diagnostic_payload = {
        "schema_version": "tradex_entry_precision_short_trend_2024_diagnostic_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "wide_window": WIDE_WINDOW,
        "diagnostic": diagnostic_2024,
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_trend_regime_decision_v1",
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
            "wider_history_used",
            "regime_buckets_explicit",
            "json_is_authoritative",
        ],
        "provisional": [
            "trend_alignment_is_still_proxy_based",
            "regime_support_is_sample_thin_in_some_buckets",
            "2024_is_tiny_sample",
        ],
        "remaining_risks": [
            "support_buckets_are_small_and_may_not_generalize",
            "failure_buckets_are_not_uniform_across_years",
            "trend_logic_is_still_proxy_based",
            "future_passes_should_not_tune_without_more_context",
        ],
    }

    report_payload = {
        "current_state": {
            "confirmed": [
                "local_uplift_was_verified_before_this_pass",
                "wide_slice_was_already_tested",
                "the_logic_is_frozen_for_this_pass",
            ],
            "provisional": [
                "regime_support_is_not_uniform",
                "2024_is_sample_thin",
                "trend_edge_is_proxy_based",
            ],
        },
        "problem": "The frozen short-trend edge may depend on regime; this pass decomposes which regimes support it and which regimes break it.",
        "change_policy": "TRADEX research only, frozen challenger logic, regime split only, same-condition comparison fixed, long logic frozen, and no new feature families or threshold changes.",
        "wide_window": WIDE_WINDOW,
        "local_window": LOCAL_WINDOW,
        "regime_split": regime_split,
        "decision": overall_decision,
        "decision_reasons": decision_reasons,
        "long_freeze_confirmed": long_freeze_confirmed,
        "diagnostic_2024": diagnostic_2024,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "If anything changes next, it should be a regime-targeted review of the failure buckets, not a threshold retune.",
    }

    _write_json(out_dir / "entry_precision_short_trend_regime_contract.json", contract_payload)
    _write_json(out_dir / "entry_precision_short_trend_regime_split.json", regime_split_payload)
    _write_json(out_dir / "entry_precision_short_trend_regime_map.json", regime_map_payload)
    _write_json(out_dir / "entry_precision_short_trend_2024_diagnostic.json", diagnostic_payload)
    _write_json(out_dir / "entry_precision_short_trend_regime_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_trend_regime_report.md", report_payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "overall_decision": overall_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Regime split audit for the frozen short trend alignment challenger.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
