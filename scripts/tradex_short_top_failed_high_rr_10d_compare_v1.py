from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from scripts.tradex_short_collapse_shape_10d_compare_v1 import (
    _load_daily,
    _pct,
    _rolling_max,
    _rolling_mean,
    _rolling_min,
    _round,
)
from scripts.tradex_short_top_failed_high_confirm_10d_compare_v1 import (
    _confirmation_entries,
    _evaluate,
    _summarize,
)
from scripts.tradex_short_top_first_failure_10d_compare_v1 import _features, _tags


AXIS_ID = "short_top_failed_high_rr_10d_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_top_failed_high_rr_10d_compare_v1")
RR_THRESHOLDS = [1.0, 1.3, 1.5, 2.0]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _attach_rr(metrics: dict[str, Any]) -> dict[str, Any]:
    entry_price = float(metrics["entry_price"])
    stop_price = float(metrics["stop_price"])
    risk = max(stop_price - entry_price, 0.0)
    reward5 = entry_price * 0.05
    rr_to_5pct = reward5 / risk if risk > 0 else None
    return {
        **metrics,
        "risk_pct": risk / entry_price if entry_price > 0 else None,
        "rr_to_5pct": rr_to_5pct,
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary.get("event_count", 0) < 200:
        return "drop", "insufficient_sample"
    positive = (summary.get("close10_short_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.50
    year_ok = (summary.get("positive_year_rate") or 0) >= 0.60
    stop_ok = (summary.get("stop_first_rate") or 1) <= 0.45
    target_ok = (summary.get("target5_first_rate") or 0) >= 0.35
    risk_ok = (summary.get("avg_risk_pct") or 1) <= 0.055
    if positive and month_ok and year_ok and stop_ok and target_ok and risk_ok:
        return "keep", "rr_filtered_failed_high_pattern_cleared_10d_short_edge_gates"
    if positive and risk_ok and (month_ok or year_ok):
        return "hold", "positive_return_but_rr_filtered_stability_or_target_gate_incomplete"
    return "drop", "no_positive_10d_rr_filtered_failed_high_edge"


def _summary_with_rr(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = _summarize(rows)
    if not rows:
        return base
    rr_values = [float(row["rr_to_5pct"]) for row in rows if row.get("rr_to_5pct") is not None]
    risk_values = [float(row["risk_pct"]) for row in rows if row.get("risk_pct") is not None]
    return {
        **base,
        "avg_rr_to_5pct": _round(mean(rr_values)) if rr_values else None,
        "median_rr_to_5pct": _round(median(rr_values)) if rr_values else None,
        "avg_risk_pct": _round(mean(risk_values)) if risk_values else None,
    }


def run(*, db_path: Path, output_root: Path, horizon: int, limit_codes: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        daily = _load_daily(conn, limit_codes)
    finally:
        conn.close()
    events: list[dict[str, Any]] = []
    for code, bars in daily.items():
        closes = [row["c"] for row in bars]
        lows = [row["l"] for row in bars]
        highs = [row["h"] for row in bars]
        vols = [row["v"] for row in bars]
        ma7s = _rolling_mean(closes, 7)
        ma20s = _rolling_mean(closes, 20)
        ma60s = _rolling_mean(closes, 60)
        vol20s = _rolling_mean(vols, 20)
        high20s = _rolling_max(highs, 20)
        high60s = _rolling_max(highs, 60)
        high120s = _rolling_max(highs, 120)
        low60s = _rolling_min(lows, 60)
        for index in range(120, len(bars) - horizon - 3):
            feat = _features(bars, index, closes, ma7s, ma20s, ma60s, vol20s, high20s, high60s, high120s, low60s)
            if feat is None:
                continue
            for base_tag in _tags(bars, index, feat):
                for entry in _confirmation_entries(bars, index, base_tag, ma7s, ma20s):
                    metrics = _evaluate(bars, index, entry, horizon)
                    if metrics is None:
                        continue
                    enriched = _attach_rr(metrics)
                    events.append(
                        {
                            "code": code,
                            "base_tag": base_tag,
                            **{
                                key: _round(feat[key])
                                for key in [
                                    "ret20",
                                    "ret60",
                                    "ret120",
                                    "volume_vs20",
                                    "close_pos",
                                    "upper_wick",
                                    "range60_pos",
                                    "dist_ma7",
                                    "dist_ma20",
                                    "close_vs_high60",
                                ]
                            },
                            **enriched,
                        }
                    )
    rows = []
    for confirm_tag in sorted({row["confirm_tag"] for row in events}):
        tag_rows = [row for row in events if row["confirm_tag"] == confirm_tag]
        for threshold in RR_THRESHOLDS:
            filtered = [row for row in tag_rows if row.get("rr_to_5pct") is not None and float(row["rr_to_5pct"]) >= threshold]
            summary = _summary_with_rr(filtered)
            decision, reason = _decision(summary)
            rows.append(
                {
                    "pattern_tag": confirm_tag,
                    "rr_threshold": threshold,
                    "decision": decision,
                    "reason": reason,
                    **summary,
                }
            )
    rows.sort(
        key=lambda row: (
            row["decision"] == "keep",
            row["decision"] == "hold",
            row.get("close10_short_ret_mean") or -1,
            row.get("target5_first_rate") or 0,
        ),
        reverse=True,
    )
    keep = [row for row in rows if row["decision"] == "keep"]
    hold = [row for row in rows if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_failed_high_rr_branch"
        reason = "at_least_one_rr_filtered_failed_high_pattern_cleared_10d_short_edge_gates"
    elif hold:
        rollup_decision = "hold_failed_high_rr_branch"
        reason = "at_least_one_rr_filtered_failed_high_pattern_positive_but_not_trade_ready"
    else:
        rollup_decision = "drop_failed_high_rr_branch"
        reason = "no_rr_filtered_failed_high_pattern_cleared_10d_short_edge_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "rr_filter_only",
            "horizon": horizon,
            "base_signal": "top_first_failure_tags",
            "confirmation_window": "1_to_3_sessions_after_base_signal",
            "entry": "confirmation_day_close",
            "stop_observation": "base_signal_high_plus_0.5pct",
            "rr_definition": "5pct_downside_target_reward / stop_price_minus_entry_price",
            "rr_thresholds": RR_THRESHOLDS,
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "family_leaderboard": rows,
        "decision": {
            "candidate_local_decision": rollup_decision,
            "session_aggregate_decision": rollup_decision,
            "authoritative_rollup_decision": rollup_decision,
            "reason": reason,
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "compare.json", report)
    _write_json(output_dir / "family_leaderboard.json", {"rows": rows})
    _write_json(output_dir / "session_leaderboard_rollup.json", {"decision": report["decision"], "family_leaderboard": rows})
    _write_csv(output_dir / "event_sample.csv", events[:1000])
    _write_json(output_root / "latest_compare.json", {"run_root": str(output_dir), **report})
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "all_present": all(
                (output_dir / name).exists()
                for name in ["compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "event_sample.csv"]
            ),
            "authoritative_rollup_decision": rollup_decision,
            "run_root": str(output_dir),
        },
    )
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--horizon", type=int, default=10)
    parser.add_argument("--limit-codes", type=int, default=None)
    args = parser.parse_args()
    db_path = args.db_path or resolve_runtime_stock_db_path()
    print(run(db_path=db_path, output_root=args.output_root, horizon=args.horizon, limit_codes=args.limit_codes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
