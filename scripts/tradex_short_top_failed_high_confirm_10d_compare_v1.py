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
from scripts.tradex_short_top_first_failure_10d_compare_v1 import _features, _tags


AXIS_ID = "short_top_failed_high_confirm_10d_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_top_failed_high_confirm_10d_compare_v1")


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


def _close_pos(row: dict[str, float]) -> float:
    rng = row["h"] - row["l"]
    return (row["c"] - row["l"]) / rng if rng > 0 else 0.5


def _confirmation_entries(
    bars: list[dict[str, float]],
    signal_index: int,
    base_tag: str,
    ma7s: list[float | None],
    ma20s: list[float | None],
) -> list[dict[str, Any]]:
    signal = bars[signal_index]
    stop_price = signal["h"] * 1.005
    entries: list[dict[str, Any]] = []
    prior_low = signal["l"]
    for offset in range(1, 4):
        idx = signal_index + offset
        if idx >= len(bars) or ma7s[idx] is None or ma20s[idx] is None:
            continue
        window = bars[signal_index + 1 : idx + 1]
        if any(row["h"] >= stop_price for row in window):
            break
        row = bars[idx]
        weak_close = row["c"] < row["o"] and _close_pos(row) <= 0.45
        ma7_fail = row["c"] < float(ma7s[idx])
        ma20_fail = row["c"] < float(ma20s[idx])
        prior_low_break = row["l"] < prior_low
        if weak_close and ma7_fail:
            entries.append(
                {
                    "confirm_tag": f"{base_tag}__no_high_ma7_fail_confirm",
                    "entry_index": idx,
                    "entry_offset": offset,
                    "entry_reason": "no_high_update_then_weak_close_below_ma7",
                    "stop_price": stop_price,
                }
            )
            break
        if weak_close and prior_low_break:
            entries.append(
                {
                    "confirm_tag": f"{base_tag}__no_high_prior_low_break_confirm",
                    "entry_index": idx,
                    "entry_offset": offset,
                    "entry_reason": "no_high_update_then_prior_low_break",
                    "stop_price": stop_price,
                }
            )
            break
        if weak_close and ma20_fail and offset <= 3:
            entries.append(
                {
                    "confirm_tag": f"{base_tag}__no_high_ma20_fail_confirm",
                    "entry_index": idx,
                    "entry_offset": offset,
                    "entry_reason": "no_high_update_then_weak_close_below_ma20",
                    "stop_price": stop_price,
                }
            )
            break
        prior_low = min(prior_low, row["l"])
    return entries


def _evaluate(bars: list[dict[str, float]], signal_index: int, entry: dict[str, Any], horizon: int) -> dict[str, Any] | None:
    entry_index = int(entry["entry_index"])
    if entry_index + horizon >= len(bars):
        return None
    entry_bar = bars[entry_index]
    entry_price = entry_bar["c"]
    if entry_price <= 0:
        return None
    future = bars[entry_index + 1 : entry_index + horizon + 1]
    lows = [row["l"] for row in future]
    highs = [row["h"] for row in future]
    closes = [row["c"] for row in future]
    stop_price = float(entry["stop_price"])
    target5_day = next((i + 1 for i, low in enumerate(lows) if low <= entry_price * 0.95), None)
    target8_day = next((i + 1 for i, low in enumerate(lows) if low <= entry_price * 0.92), None)
    stop_day = next((i + 1 for i, high in enumerate(highs) if high >= stop_price), None)
    first_event = "time"
    if target5_day is not None and (stop_day is None or target5_day <= stop_day):
        first_event = "target5"
    elif stop_day is not None:
        first_event = "stop_peak_high"
    return {
        **entry,
        "signal_ymd": int(bars[signal_index]["ymd"]),
        "entry_ymd": int(entry_bar["ymd"]),
        "entry_price": entry_price,
        "stop_pct": stop_price / entry_price - 1,
        "target5_hit_10d": target5_day is not None,
        "target8_hit_10d": target8_day is not None,
        "target5_first": first_event == "target5",
        "stop_first": first_event == "stop_peak_high",
        "first_event": first_event,
        "close10_short_ret": (entry_price - closes[-1]) / entry_price,
        "mfe10_short": (entry_price - min(lows)) / entry_price,
        "mae10_short": (entry_price - max(highs)) / entry_price,
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    returns = [float(row["close10_short_ret"]) for row in rows]
    by_month: dict[str, list[float]] = defaultdict(list)
    by_year: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        ymd = str(row["entry_ymd"])
        by_month[ymd[:6]].append(float(row["close10_short_ret"]))
        by_year[ymd[:4]].append(float(row["close10_short_ret"]))
    usable_months = [values for values in by_month.values() if len(values) >= 5]
    usable_years = [values for values in by_year.values() if len(values) >= 10]
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "avg_entry_offset": _round(mean(float(row["entry_offset"]) for row in rows)),
        "target5_hit_10d_rate": _round(sum(1 for row in rows if row["target5_hit_10d"]) / len(rows)),
        "target8_hit_10d_rate": _round(sum(1 for row in rows if row["target8_hit_10d"]) / len(rows)),
        "target5_first_rate": _round(sum(1 for row in rows if row["target5_first"]) / len(rows)),
        "stop_first_rate": _round(sum(1 for row in rows if row["stop_first"]) / len(rows)),
        "avg_stop_pct": _round(mean(float(row["stop_pct"]) for row in rows)),
        "close10_short_ret_mean": _round(mean(returns)),
        "close10_short_ret_median": _round(median(returns)),
        "close10_short_positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "positive_month_rate": _round(sum(1 for values in usable_months if mean(values) > 0) / len(usable_months)) if usable_months else None,
        "positive_year_rate": _round(sum(1 for values in usable_years if mean(values) > 0) / len(usable_years)) if usable_years else None,
        "usable_month_count": len(usable_months),
        "usable_year_count": len(usable_years),
        "close10_short_ret_p10": _round(_pct(returns, 0.10)),
        "close10_short_ret_p90": _round(_pct(returns, 0.90)),
        "mfe10_short_mean": _round(mean(float(row["mfe10_short"]) for row in rows)),
        "mae10_short_mean": _round(mean(float(row["mae10_short"]) for row in rows)),
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary.get("event_count", 0) < 200:
        return "drop", "insufficient_sample"
    positive = (summary.get("close10_short_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.50
    year_ok = (summary.get("positive_year_rate") or 0) >= 0.60
    stop_ok = (summary.get("stop_first_rate") or 1) <= 0.45
    target_ok = (summary.get("target5_first_rate") or 0) >= 0.35
    tight_risk = (summary.get("avg_stop_pct") or 1) <= 0.065
    if positive and month_ok and year_ok and stop_ok and target_ok and tight_risk:
        return "keep", "confirmed_failed_high_pattern_cleared_10d_short_edge_gates"
    if positive and tight_risk and (month_ok or year_ok):
        return "hold", "positive_return_but_confirmation_stability_or_target_gate_incomplete"
    return "drop", "no_positive_10d_confirmed_failed_high_edge"


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
            base_tags = _tags(bars, index, feat)
            if not base_tags:
                continue
            for base_tag in base_tags:
                for entry in _confirmation_entries(bars, index, base_tag, ma7s, ma20s):
                    metrics = _evaluate(bars, index, entry, horizon)
                    if metrics is None:
                        continue
                    events.append(
                        {
                            "code": code,
                            "base_tag": base_tag,
                            **{key: _round(feat[key]) for key in ["ret20", "ret60", "ret120", "volume_vs20", "close_pos", "upper_wick", "range60_pos", "dist_ma7", "dist_ma20", "close_vs_high60"]},
                            **metrics,
                        }
                    )
    rows = []
    for tag in sorted({row["confirm_tag"] for row in events}):
        tag_rows = [row for row in events if row["confirm_tag"] == tag]
        summary = _summarize(tag_rows)
        decision, reason = _decision(summary)
        rows.append({"pattern_tag": tag, "decision": decision, "reason": reason, **summary})
    rows.sort(key=lambda row: (row["decision"] == "keep", row["decision"] == "hold", row.get("close10_short_ret_mean") or -1), reverse=True)
    keep = [row for row in rows if row["decision"] == "keep"]
    hold = [row for row in rows if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_failed_high_confirm_branch"
        reason = "at_least_one_failed_high_confirmation_pattern_cleared_10d_short_edge_gates"
    elif hold:
        rollup_decision = "hold_failed_high_confirm_branch"
        reason = "at_least_one_failed_high_confirmation_pattern_positive_but_not_trade_ready"
    else:
        rollup_decision = "drop_failed_high_confirm_branch"
        reason = "no_failed_high_confirmation_pattern_cleared_10d_short_edge_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "failed_high_followup_confirmation_only",
            "horizon": horizon,
            "base_signal": "top_first_failure_tags_from_short_top_first_failure_10d_compare_v1",
            "confirmation_window": "1_to_3_sessions_after_base_signal",
            "entry": "confirmation_day_close",
            "stop_observation": "base_signal_high_plus_0.5pct",
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "confirmation_definitions": {
            "no_high_ma7_fail_confirm": "no high update above base high+0.5%, then weak close below MA7",
            "no_high_prior_low_break_confirm": "no high update above base high+0.5%, then prior low break",
            "no_high_ma20_fail_confirm": "no high update above base high+0.5%, then weak close below MA20",
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
