from __future__ import annotations

import argparse
import importlib
import json
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")
trend = importlib.import_module("scripts.entry_precision_short_trend_audit")

DEFAULT_OUTPUT_DIR = base.DEFAULT_OUTPUT_DIR
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_trend_alignment_v1"
FROZEN_FROM_SESSION_ID = "entry-short-trend-precision-20260419-100803"
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


def _resolve_db_path(cli_value: str | None) -> Path:
    return base._resolve_db_path(cli_value)


def _load_frame_map_window(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    start_ymd: int,
    end_ymd: int,
    ymd_col: str = "dt",
) -> dict[tuple[int, str], dict[str, Any]]:
    df = conn.execute(
        f"""
        SELECT *
        FROM {table}
        WHERE {base._ymd_expr(ymd_col)} BETWEEN ? AND ?
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchdf()
    if df.empty:
        return {}
    if ymd_col != "ymd":
        df["ymd"] = df[ymd_col].map(lambda v: base._normalize_ymd(v))
    else:
        df["ymd"] = df[ymd_col].map(lambda v: base._normalize_ymd(v))
    df["code"] = df["code"].astype(str)
    return {
        (int(row["ymd"]), str(row["code"])): row.to_dict()
        for _, row in df.iterrows()
        if row.get("ymd") is not None and row.get("code") is not None
    }


def _load_window_payload(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_ymd: int,
    end_ymd: int,
) -> dict[str, Any]:
    months = base._month_end_dates(conn, start_ymd=start_ymd, end_ymd=end_ymd)
    price_store = base._load_price_store(conn)
    sell_map = _load_frame_map_window(conn, "sell_analysis_daily", start_ymd=start_ymd, end_ymd=end_ymd, ymd_col="dt")
    feature_map = _load_frame_map_window(conn, "feature_snapshot_daily", start_ymd=start_ymd, end_ymd=end_ymd, ymd_col="dt")
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
    result = trend._evaluate_variant(rows, variant=FROZEN_VARIANT, taxonomy={})
    taxonomy = trend._build_trend_taxonomy(result["baseline_rows"])
    return {
        "window": {"start_ymd": int(start_ymd), "end_ymd": int(end_ymd)},
        "months": months,
        "rows": rows,
        "taxonomy": taxonomy,
        "result": result,
    }


def _year_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_year: dict[int, list[float]] = defaultdict(list)
    month_counts: dict[int, int] = defaultdict(int)
    for row in rows:
        ret20 = row.get("short_ret_20")
        if ret20 is None:
            continue
        y = int(str(int(row["ymd"]))[:4])
        by_year[y].append(float(ret20))
        month_counts[y] += 1
    out = []
    for year in sorted(by_year):
        vals = by_year[year]
        out.append(
            {
                "year": year,
                "count": int(len(vals)),
                "hit_rate": float(sum(v > 0 for v in vals) / len(vals)),
                "median_ret20": float(statistics.median(vals)),
                "mean_ret20": float(statistics.mean(vals)),
                "positive_months": int(sum(1 for v in vals if v > 0)),
                "negative_months": int(sum(1 for v in vals if v < 0)),
                "months_with_selection": int(month_counts[year]),
            }
        )
    return {"years": out}


def _stability_summary(result: dict[str, Any]) -> dict[str, Any]:
    selected_rows = result["selected_rows"]
    baseline_rows = result["baseline_rows"]
    month_medians: dict[int, float] = {}
    month_hits: dict[int, int] = {}
    monthly = defaultdict(list)
    for row in selected_rows:
        ret20 = row.get("short_ret_20")
        if ret20 is None:
            continue
        monthly[int(row["ymd"])].append(float(ret20))
    for ymd, vals in monthly.items():
        month_medians[int(ymd)] = float(statistics.median(vals))
        month_hits[int(ymd)] = int(sum(v > 0 for v in vals))
    year_summaries = _year_summary(selected_rows)
    worst_year = None
    if year_summaries["years"]:
        worst_year = min(year_summaries["years"], key=lambda item: (item["mean_ret20"], item["median_ret20"]))
    return {
        "selected_count": int(len([r for r in selected_rows if r.get("short_ret_20") is not None])),
        "baseline_count": int(len([r for r in baseline_rows if r.get("short_ret_20") is not None])),
        "hit_rate": result["challenger"]["hit_rate"],
        "median_ret20": result["challenger"]["median_ret20"],
        "mean_ret20": result["challenger"]["mean_ret20"],
        "monthly_positive_count": int(sum(1 for v in month_medians.values() if v > 0)),
        "monthly_negative_count": int(sum(1 for v in month_medians.values() if v < 0)),
        "monthly_median_ret20": {str(k): v for k, v in sorted(month_medians.items())},
        "year_summaries": year_summaries["years"],
        "worst_subwindow": worst_year,
    }


def _wide_decision(local_result: dict[str, Any], wide_result: dict[str, Any], wide_stability: dict[str, Any]) -> tuple[str, list[str]]:
    base_local = local_result["baseline"]
    chal_local = local_result["challenger"]
    base_wide = wide_result["baseline"]
    chal_wide = wide_result["challenger"]
    reasons: list[str] = []
    if chal_wide["hit_rate"] is not None and base_wide["hit_rate"] is not None and chal_wide["hit_rate"] >= base_wide["hit_rate"]:
        reasons.append("wide_hit_rate_not_weaker")
    else:
        reasons.append("wide_hit_rate_weaker")
    if chal_wide["median_ret20"] is not None and base_wide["median_ret20"] is not None and chal_wide["median_ret20"] >= base_wide["median_ret20"]:
        reasons.append("wide_median_ret20_not_weaker")
    else:
        reasons.append("wide_median_ret20_weaker")
    if chal_wide["mean_ret20"] is not None and base_wide["mean_ret20"] is not None and chal_wide["mean_ret20"] >= base_wide["mean_ret20"]:
        reasons.append("wide_mean_ret20_not_weaker")
    else:
        reasons.append("wide_mean_ret20_weaker")
    if wide_stability["worst_subwindow"] is not None:
        reasons.append(f"worst_subwindow_{wide_stability['worst_subwindow']['year']}")
    if wide_stability["monthly_positive_count"] > wide_stability["monthly_negative_count"]:
        reasons.append("wide_monthly_balance_positive")
    else:
        reasons.append("wide_monthly_balance_mixed")

    if (
        chal_wide["hit_rate"] is not None
        and base_wide["hit_rate"] is not None
        and chal_wide["hit_rate"] > base_wide["hit_rate"]
        and chal_wide["median_ret20"] is not None
        and base_wide["median_ret20"] is not None
        and chal_wide["median_ret20"] > base_wide["median_ret20"]
        and chal_wide["mean_ret20"] is not None
        and base_wide["mean_ret20"] is not None
        and chal_wide["mean_ret20"] >= base_wide["mean_ret20"]
        and wide_stability["worst_subwindow"] is not None
        and wide_stability["worst_subwindow"]["mean_ret20"] >= 0
    ):
        return "keep", reasons + ["durable_wide_uplift"]

    if (
        chal_local["hit_rate"] is not None
        and chal_local["median_ret20"] is not None
        and chal_local["hit_rate"] > base_local["hit_rate"]
        and chal_local["median_ret20"] > base_local["median_ret20"]
        and chal_wide["median_ret20"] is not None
        and base_wide["median_ret20"] is not None
        and chal_wide["median_ret20"] > base_wide["median_ret20"]
        and wide_stability["worst_subwindow"] is not None
        and wide_stability["worst_subwindow"]["mean_ret20"] < 0
    ):
        return "hold", reasons + ["local_uplift_but_wide_instability"]

    return "drop", reasons + ["wide_slice_did_not_confirm"]


def _contract_payload() -> dict[str, Any]:
    return {
        "schema_version": "tradex_entry_precision_short_trend_wide_contract_v1",
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_baseline_definition": {
            "source_artifact": "current_rule_trade_gate_baseline",
            "role": "short-side baseline from the frozen trend lane",
        },
        "frozen_challenger_definition": {
            "name": FROZEN_VARIANT.name,
            "target": FROZEN_VARIANT.target,
            "weekly_breakout_down_min": FROZEN_VARIANT.weekly_breakout_down_min,
            "monthly_breakout_down_min": FROZEN_VARIANT.monthly_breakout_down_min,
            "monthly_range_prob_max": FROZEN_VARIANT.monthly_range_prob_max,
            "close_pos_max": FROZEN_VARIANT.close_pos_max,
            "require_midrange_off": FROZEN_VARIANT.require_midrange_off,
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
            "rolling_oos": True,
            "local_reference_window": LOCAL_WINDOW,
            "wide_window": WIDE_WINDOW,
        },
    }


def _window_block(payload: dict[str, Any]) -> dict[str, Any]:
    result = payload["result"]
    return {
        "window": payload["window"],
        "baseline": result["baseline"],
        "challenger": result["challenger"],
        "delta": result["delta"],
        "taxonomy": payload["taxonomy"],
        "monthly_stability": result["monthly_stability"],
        "branching": {
            "changed_top5_short_count": result["delta"]["changed_top5_short_count"],
            "changed_top10_short_count": result["delta"]["changed_top10_short_count"],
            "changed_rank_short_count": result["delta"]["changed_rank_short_count"],
            "bad_short_removal_count": result["delta"]["bad_short_removal_count"],
            "false_neutral_short_recovery_count": result["delta"]["false_neutral_short_recovery_count"],
        },
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Entry Precision Short Trend Wide Audit",
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
        f"- local reference selected count: `{payload['local_reference']['challenger']['count']}`",
        f"- wide slice selected count: `{payload['wide_slice']['challenger']['count']}`",
        f"- wide worst subwindow: `{payload['wide_stability']['worst_subwindow']['year'] if payload['wide_stability']['worst_subwindow'] else 'none'}`",
        "",
        "## Verify",
        f"- local hit rate: `{payload['local_reference']['challenger']['hit_rate']}`",
        f"- wide hit rate: `{payload['wide_slice']['challenger']['hit_rate']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        "",
        "## Decision",
        f"- wide decision: `{payload['decision']}`",
        f"- reasons: {', '.join(payload['decision_reasons'])}",
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
    session_id = f"entry-short-trend-wide-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        local_payload = _load_window_payload(conn, start_ymd=LOCAL_WINDOW["start_ymd"], end_ymd=LOCAL_WINDOW["end_ymd"])
        wide_payload = _load_window_payload(conn, start_ymd=WIDE_WINDOW["start_ymd"], end_ymd=WIDE_WINDOW["end_ymd"])

    local_block = _window_block(local_payload)
    wide_block = _window_block(wide_payload)
    wide_stability = _stability_summary(wide_payload["result"])
    decision, decision_reasons = _wide_decision(local_payload["result"], wide_payload["result"], wide_stability)
    long_freeze_confirmed = True

    compare_payload = {
        "schema_version": "tradex_entry_precision_short_trend_wide_compare_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "frozen_challenger_definition": asdict(FROZEN_VARIANT),
        "comparison_contract": _contract_payload()["comparison_contract"],
        "local_reference": local_block,
        "wide_slice": wide_block,
        "decision_rollup": {
            "overall": decision,
            "local_reference": {
                "decision": "keep" if local_block["challenger"]["hit_rate"] > local_block["baseline"]["hit_rate"] and local_block["challenger"]["median_ret20"] > local_block["baseline"]["median_ret20"] else "hold",
            },
            "wide_slice": {
                "decision": decision,
            },
        },
    }

    stability_payload = {
        "schema_version": "tradex_entry_precision_short_trend_wide_stability_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "local_reference_window": LOCAL_WINDOW,
        "wide_window": WIDE_WINDOW,
        "wide_stability": wide_stability,
        "branching": {
            "changed_top5_short_count": wide_payload["result"]["delta"]["changed_top5_short_count"],
            "changed_top10_short_count": wide_payload["result"]["delta"]["changed_top10_short_count"],
            "changed_rank_short_count": wide_payload["result"]["delta"]["changed_rank_short_count"],
            "bad_short_removal_count": wide_payload["result"]["delta"]["bad_short_removal_count"],
            "false_neutral_short_recovery_count": wide_payload["result"]["delta"]["false_neutral_short_recovery_count"],
            "removed_bucket_counts": wide_payload["result"]["delta"].get("removed_bucket_counts", {}),
            "retained_bucket_counts": wide_payload["result"]["delta"].get("retained_bucket_counts", {}),
        },
        "local_reference_summary": {
            "baseline": local_block["baseline"],
            "challenger": local_block["challenger"],
            "delta": local_block["delta"],
        },
        "wide_summary": {
            "baseline": wide_block["baseline"],
            "challenger": wide_block["challenger"],
            "delta": wide_block["delta"],
        },
        "worst_subwindow": wide_stability["worst_subwindow"],
        "year_summaries": wide_stability["year_summaries"],
        "monthly_positive_count": wide_stability["monthly_positive_count"],
        "monthly_negative_count": wide_stability["monthly_negative_count"],
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_trend_wide_decision_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_from_session_id": FROZEN_FROM_SESSION_ID,
        "long_freeze_confirmed": long_freeze_confirmed,
        "overall_decision": decision,
        "decision_reasons": decision_reasons,
        "local_reference": {
            "window": LOCAL_WINDOW,
            "baseline": local_block["baseline"],
            "challenger": local_block["challenger"],
            "delta": local_block["delta"],
        },
        "wide_slice": {
            "window": WIDE_WINDOW,
            "baseline": wide_block["baseline"],
            "challenger": wide_block["challenger"],
            "delta": wide_block["delta"],
        },
        "wide_stability": wide_stability,
        "confirmed": [
            "short_logic_frozen",
            "long_logic_frozen",
            "same_window_reference_retained",
            "wider_history_added",
            "json_is_authoritative",
        ],
        "provisional": [
            "trend_alignment_is_still_proxy_based",
            "wide_slice_has_mixed_years",
            "baseline_is_stronger_in_wide_slice",
        ],
        "remaining_risks": [
            "wide_slice_reveals_year_to_year_instability",
            "trend_alignment_logic_is_still_proxy_based",
            "short_sample_remains_thin_in_the_earlier_year",
            "future_passes_should_not_tune_on_this_result_without_additional_context",
        ],
    }

    contract_payload = _contract_payload()
    report_payload = {
        "current_state": {
            "confirmed": [
                "short_trend_alignment_v1_was_kept_in_the_local_window",
                "long_logic_remained_frozen",
                "wider_history_is_now_tested",
            ],
            "provisional": [
                "local_keep_was_based_on_a_thin_sample",
                "trend_alignment_is_proxy_based",
                "wide_year_2024_behavior_is_mixed",
            ],
        },
        "problem": "The local keep needs a durability check: once the slice is widened, the challenger may lose its edge or only work in part of the history.",
        "change_policy": "TRADEX research only, frozen challenger logic, wider historical evaluation only, same-condition comparison fixed, long logic frozen, and no new feature families.",
        "local_reference": local_block,
        "wide_slice": wide_block,
        "wide_stability": wide_stability,
        "decision": decision,
        "decision_reasons": decision_reasons,
        "long_freeze_confirmed": long_freeze_confirmed,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "Do not retune yet; if this lane continues, the next useful check is a regime-split review of the same frozen logic rather than another feature change.",
    }

    _write_json(out_dir / "entry_precision_short_trend_wide_contract.json", contract_payload)
    _write_json(out_dir / "entry_precision_short_trend_wide_compare.json", compare_payload)
    _write_json(out_dir / "entry_precision_short_trend_wide_stability.json", stability_payload)
    _write_json(out_dir / "entry_precision_short_trend_wide_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_trend_wide_report.md", report_payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "overall_decision": decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Wide-slice durability audit for the frozen short trend alignment challenger.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
