from __future__ import annotations

import argparse
import csv
import json
import math
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
from scripts.tradex_short_high_zone_failure_to_ma20_v1 import FEATURE_SQL as HIGH_ZONE_SQL
from scripts.tradex_short_ma20_to_ma60_followthrough_v1 import FEATURE_SQL as MA20_SQL
from scripts.tradex_short_ma60_to_ma100_breakdown_v1 import FEATURE_SQL as MA60_SQL


AXIS_ID = "short_10d_target_entry_timing_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_10d_target_entry_timing_compare_v1")


FAMILIES = {
    "ma20_break_to_ma60": {"sql": MA20_SQL, "target": "ma60", "stop": "ma20", "rejection_ma": "ma20"},
    "high_zone_failure_to_ma20": {"sql": HIGH_ZONE_SQL, "target": "ma20", "stop": "event_high", "rejection_ma": "ma20"},
    "ma60_break_to_ma100_support": {"sql": MA60_SQL, "target": "ma100_or_low100", "stop": "ma60", "rejection_ma": "ma60"},
}


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


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - idx) + ordered[hi] * (idx - lo)


def _rows(conn: duckdb.DuckDBPyConnection, sql: str, limit_events: int | None) -> list[dict[str, Any]]:
    query = sql
    if limit_events is not None:
        query = f"SELECT * FROM ({sql}) LIMIT {int(limit_events)}"
    cols = [col[0] for col in conn.execute(query).description]
    return [{key: _clean(value) for key, value in zip(cols, row)} for row in conn.fetchall()]


def _future(row: dict[str, Any], day: int, field: str) -> float | None:
    value = row.get(f"f{day}_{field}")
    if value is None:
        return None
    return float(value)


def _entry_candidates(row: dict[str, Any], family_cfg: dict[str, str]) -> list[dict[str, Any]]:
    entries = [
        {"timing": "signal_close", "entry_day": 0, "entry_ymd": int(row["ymd"]), "entry_price": float(row["c"])},
    ]
    if row.get("f1_c") is not None and float(row["f1_c"]) < float(row["c"]):
        entries.append(
            {
                "timing": "next_day_continuation",
                "entry_day": 1,
                "entry_ymd": int(row["f1_ymd"]),
                "entry_price": float(row["f1_c"]),
            }
        )
    rejection_key = family_cfg["rejection_ma"]
    for day in range(1, 4):
        high = _future(row, day, "h")
        close = _future(row, day, "c")
        open_ = _future(row, day, "o")
        ma = _future(row, day, rejection_key)
        ymd = row.get(f"f{day}_ymd")
        if high is None or close is None or ma is None or ymd is None:
            continue
        if high >= ma and close < ma:
            entries.append(
                {
                    "timing": f"{rejection_key}_rejection_3d",
                    "entry_day": day,
                    "entry_ymd": int(ymd),
                    "entry_price": close,
                }
            )
            break
        if open_ is not None and high >= ma and close < open_ and close < float(row["c"]):
            entries.append(
                {
                    "timing": "small_rebound_bearish_failure_3d",
                    "entry_day": day,
                    "entry_ymd": int(ymd),
                    "entry_price": close,
                }
            )
            break
    return entries


def _support_for(row: dict[str, Any], day: int) -> tuple[str | None, float | None]:
    ma100 = _future(row, day, "ma100")
    low100 = _future(row, day, "low100")
    support = None
    kind = None
    if ma100 is not None:
        support = ma100
        kind = "ma100"
    if low100 is not None and (support is None or low100 > support):
        support = low100
        kind = "low100_support"
    return kind, support


def _evaluate(row: dict[str, Any], family_cfg: dict[str, str], entry: dict[str, Any], horizon: int) -> dict[str, Any] | None:
    entry_price = float(entry["entry_price"])
    entry_day = int(entry["entry_day"])
    if entry_price <= 0 or entry_day >= horizon:
        return None
    target_day: int | None = None
    stop_day: int | None = None
    target_kind: str | None = None
    target_ret: float | None = None
    best_mfe = 0.0
    worst_mae = 0.0
    close_h = _future(row, horizon, "c")
    if close_h is None:
        return None
    event_high = float(row["h"])
    for day in range(entry_day + 1, horizon + 1):
        low = _future(row, day, "l")
        high = _future(row, day, "h")
        close = _future(row, day, "c")
        if low is None or high is None or close is None:
            continue
        best_mfe = max(best_mfe, (entry_price - low) / entry_price)
        worst_mae = min(worst_mae, (entry_price - high) / entry_price)
        if target_day is None:
            if family_cfg["target"] == "ma60":
                target = _future(row, day, "ma60")
                kind = "ma60"
            elif family_cfg["target"] == "ma20":
                target = _future(row, day, "ma20")
                kind = "ma20"
            else:
                kind, target = _support_for(row, day)
            if target is not None and low <= target:
                target_day = day - entry_day
                target_kind = kind
                target_ret = (entry_price - target) / entry_price
        if stop_day is None:
            if family_cfg["stop"] == "event_high":
                stopped = close >= event_high
            else:
                stop_ma = _future(row, day, family_cfg["stop"])
                stopped = stop_ma is not None and close >= stop_ma
            if stopped:
                stop_day = day - entry_day
    first_event = "time"
    first_day = horizon - entry_day
    if target_day is not None and (stop_day is None or target_day <= stop_day):
        first_event = f"target_{target_kind}"
        first_day = target_day
    elif stop_day is not None:
        first_event = f"{family_cfg['stop']}_stop"
        first_day = stop_day
    return {
        **entry,
        "target_hit_10d": target_day is not None,
        "target_day": target_day,
        "stop_hit_10d": stop_day is not None,
        "stop_day": stop_day,
        "first_event": first_event,
        "first_event_day": first_day,
        "target_return_if_hit": target_ret,
        "close10_short_ret": (entry_price - close_h) / entry_price,
        "mfe10_short": best_mfe,
        "mae10_short": worst_mae,
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    returns = [float(row["close10_short_ret"]) for row in rows]
    target_returns = [float(row["target_return_if_hit"]) for row in rows if row.get("target_return_if_hit") is not None]
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "target_hit_10d_rate": _round(sum(1 for row in rows if row["target_hit_10d"]) / len(rows)),
        "target_first_rate": _round(sum(1 for row in rows if str(row["first_event"]).startswith("target_")) / len(rows)),
        "stop_first_rate": _round(sum(1 for row in rows if str(row["first_event"]).endswith("_stop")) / len(rows)),
        "close10_short_ret_mean": _round(mean(returns)),
        "close10_short_ret_median": _round(median(returns)),
        "close10_short_positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "target_return_if_hit_mean": _round(mean(target_returns)) if target_returns else None,
        "mfe10_short_mean": _round(mean(float(row["mfe10_short"]) for row in rows)),
        "mae10_short_mean": _round(mean(float(row["mae10_short"]) for row in rows)),
        "close10_short_ret_p10": _round(_pct(returns, 0.10)),
        "close10_short_ret_p90": _round(_pct(returns, 0.90)),
    }


def _by_month(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["entry_ymd"])[:6]].append(row)
    monthly = []
    for yyyymm, items in sorted(grouped.items()):
        summary = _summarize(items)
        monthly.append({"yyyymm": yyyymm, **summary})
    usable = [row for row in monthly if row["event_count"] >= 10]
    return {
        "usable_month_count": len(usable),
        "positive_month_rate": _round(sum(1 for row in usable if (row.get("close10_short_ret_mean") or 0) > 0) / len(usable)) if usable else None,
        "monthly": monthly,
    }


def _decision(summary: dict[str, Any], baseline: dict[str, Any]) -> tuple[str, str]:
    if summary["event_count"] < 100:
        return "drop", "insufficient_sample"
    improved_return = (summary.get("close10_short_ret_mean") or -1) > (baseline.get("close10_short_ret_mean") or -1) + 0.003
    positive = (summary.get("close10_short_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.45
    stop_ok = (summary.get("stop_first_rate") or 1) <= (baseline.get("stop_first_rate") or 1)
    if positive and month_ok and stop_ok:
        return "keep", "positive_return_month_stable_stop_not_worse"
    if improved_return and month_ok:
        return "hold", "return_improved_but_not_trade_ready"
    return "drop", "no_same_condition_entry_timing_improvement"


def run(*, db_path: Path, output_root: Path, horizon: int, limit_events: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    all_rows: list[dict[str, Any]] = []
    family_summaries = []
    try:
        for family, cfg in FAMILIES.items():
            source_rows = _rows(conn, cfg["sql"], limit_events)
            evaluated: list[dict[str, Any]] = []
            for row in source_rows:
                for entry in _entry_candidates(row, cfg):
                    metrics = _evaluate(row, cfg, entry, horizon)
                    if metrics is None:
                        continue
                    evaluated.append(
                        {
                            "family": family,
                            "code": str(row["code"]),
                            "signal_ymd": int(row["ymd"]),
                            **metrics,
                        }
                    )
            by_timing = []
            baseline = {}
            for timing in sorted({row["timing"] for row in evaluated}):
                rows = [row for row in evaluated if row["timing"] == timing]
                summary = {**_summarize(rows), **{k: v for k, v in _by_month(rows).items() if k != "monthly"}}
                if timing == "signal_close":
                    baseline = summary
                decision, reason = _decision(summary, baseline or summary)
                by_timing.append({"family": family, "timing": timing, "decision": decision, "reason": reason, **summary})
            by_timing.sort(key=lambda row: (row["decision"] == "keep", row["decision"] == "hold", row.get("close10_short_ret_mean") or -1), reverse=True)
            family_summaries.append(
                {
                    "family": family,
                    "source_event_count": len(source_rows),
                    "evaluated_entry_count": len(evaluated),
                    "baseline_signal_close": baseline,
                    "timing_leaderboard": by_timing,
                }
            )
            all_rows.extend(evaluated)
    finally:
        conn.close()
    overall_rows = []
    for timing in sorted({row["timing"] for row in all_rows}):
        rows = [row for row in all_rows if row["timing"] == timing]
        summary = {**_summarize(rows), **{k: v for k, v in _by_month(rows).items() if k != "monthly"}}
        overall_rows.append({"timing": timing, **summary})
    overall_rows.sort(key=lambda row: (row.get("close10_short_ret_mean") or -1, row.get("positive_month_rate") or 0), reverse=True)
    keep_rows = [
        row
        for family in family_summaries
        for row in family["timing_leaderboard"]
        if row["decision"] == "keep"
    ]
    hold_rows = [
        row
        for family in family_summaries
        for row in family["timing_leaderboard"]
        if row["decision"] == "hold"
    ]
    if keep_rows:
        rollup_decision = "keep_entry_timing_branch"
        reason = "at_least_one_family_timing_branch_cleared_positive_return_month_stability_gate"
    elif hold_rows:
        rollup_decision = "hold_entry_timing_branch"
        reason = "timing_improved_some_family_but_not_trade_ready"
    else:
        rollup_decision = "drop_entry_timing_branch"
        reason = "no_timing_variant_cleared_same_condition_profitability_and_stability_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "fixed_evaluation_conditions": {
            "families": list(FAMILIES),
            "entry_timing_axis_only": True,
            "horizon": horizon,
            "limit_events": limit_events,
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "overall_timing_leaderboard": overall_rows,
        "family_summaries": family_summaries,
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
    _write_json(output_dir / "entry_timing_compare.json", report)
    _write_json(output_dir / "family_leaderboard.json", {"rows": family_summaries})
    _write_json(output_dir / "session_leaderboard_rollup.json", {"decision": report["decision"], "overall_timing_leaderboard": overall_rows})
    _write_csv(output_dir / "event_sample.csv", all_rows[:1000])
    _write_json(output_root / "latest_entry_timing_compare.json", {"run_root": str(output_dir), **report})
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "all_present": all(
                (output_dir / name).exists()
                for name in ["entry_timing_compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "event_sample.csv"]
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
    parser.add_argument("--limit-events", type=int, default=None)
    args = parser.parse_args()
    db_path = args.db_path or resolve_runtime_stock_db_path()
    print(run(db_path=db_path, output_root=args.output_root, horizon=args.horizon, limit_events=args.limit_events))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
