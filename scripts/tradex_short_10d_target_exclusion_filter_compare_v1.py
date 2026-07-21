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
from scripts.tradex_short_10d_target_entry_timing_compare_v1 import FAMILIES, _evaluate, _rows


AXIS_ID = "short_10d_target_exclusion_filter_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_10d_target_exclusion_filter_compare_v1")


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


def _regime_rows(conn: duckdb.DuckDBPyConnection) -> dict[int, dict[str, Any]]:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='market_regime_daily'"
    ).fetchone()[0]
    if not table_exists:
        return {}
    rows = conn.execute(
        """
        SELECT
          CAST(dt AS INTEGER) AS ymd,
          regime_id,
          breadth_above_ma20,
          breadth_above_ma60,
          advancers_ratio,
          index_close_vs_ma20,
          index_close_vs_ma60,
          regime_score
        FROM market_regime_daily
        """
    ).fetchall()
    cols = ["ymd", "regime_id", "breadth_above_ma20", "breadth_above_ma60", "advancers_ratio", "index_close_vs_ma20", "index_close_vs_ma60", "regime_score"]
    return {int(row[0]): dict(zip(cols, row)) for row in rows}


def _f(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _blocked_reasons(row: dict[str, Any]) -> list[str]:
    reasons = []
    if _f(row, "regime_score") >= 0.60 or (
        _f(row, "breadth_above_ma20") >= 0.60 and _f(row, "index_close_vs_ma20") > 0
    ):
        reasons.append("strong_market_regime")
    if _f(row, "ret60") >= 0.20 and _f(row, "range60_pos") >= 0.65:
        reasons.append("mature_rising_high_zone")
    if _f(row, "ret20") >= 0.12 and _f(row, "volume_vs20", 1.0) >= 1.30:
        reasons.append("hot_momentum_volume")
    if _f(row, "dist_ma60") >= 0.08 and _f(row, "ret60") >= 0.10:
        reasons.append("far_above_ma60_uptrend")
    return reasons


FILTERS = {
    "all_signal_close": [],
    "exclude_strong_market_regime": ["strong_market_regime"],
    "exclude_mature_rising_high_zone": ["mature_rising_high_zone"],
    "exclude_hot_momentum_volume": ["hot_momentum_volume"],
    "exclude_far_above_ma60_uptrend": ["far_above_ma60_uptrend"],
    "exclude_all_uptrend_risk": [
        "strong_market_regime",
        "mature_rising_high_zone",
        "hot_momentum_volume",
        "far_above_ma60_uptrend",
    ],
}


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    returns = [float(row["close10_short_ret"]) for row in rows]
    by_month: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_month[str(row["entry_ymd"])[:6]].append(float(row["close10_short_ret"]))
    usable_months = [values for values in by_month.values() if len(values) >= 10]
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "target_hit_10d_rate": _round(sum(1 for row in rows if row["target_hit_10d"]) / len(rows)),
        "target_first_rate": _round(sum(1 for row in rows if str(row["first_event"]).startswith("target_")) / len(rows)),
        "stop_first_rate": _round(sum(1 for row in rows if str(row["first_event"]).endswith("_stop")) / len(rows)),
        "close10_short_ret_mean": _round(mean(returns)),
        "close10_short_ret_median": _round(median(returns)),
        "close10_short_positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "positive_month_rate": _round(sum(1 for values in usable_months if mean(values) > 0) / len(usable_months)) if usable_months else None,
        "usable_month_count": len(usable_months),
        "close10_short_ret_p10": _round(_pct(returns, 0.10)),
        "close10_short_ret_p90": _round(_pct(returns, 0.90)),
        "mfe10_short_mean": _round(mean(float(row["mfe10_short"]) for row in rows)),
        "mae10_short_mean": _round(mean(float(row["mae10_short"]) for row in rows)),
    }


def _decision(kept: dict[str, Any], skipped: dict[str, Any], baseline: dict[str, Any]) -> tuple[str, str]:
    if kept.get("event_count", 0) < 500:
        return "drop", "insufficient_kept_sample"
    return_improved = (kept.get("close10_short_ret_mean") or -1) >= (baseline.get("close10_short_ret_mean") or -1) + 0.003
    positive = (kept.get("close10_short_ret_mean") or 0) > 0
    month_ok = (kept.get("positive_month_rate") or 0) >= 0.45
    skipped_worse = skipped.get("event_count", 0) == 0 or (skipped.get("close10_short_ret_mean") or 1) <= (kept.get("close10_short_ret_mean") or -1)
    stop_ok = (kept.get("stop_first_rate") or 1) <= (baseline.get("stop_first_rate") or 1)
    if positive and month_ok and skipped_worse and stop_ok:
        return "keep", "positive_return_month_stable_and_skipped_worse"
    if return_improved and skipped_worse:
        return "hold", "return_improved_but_not_trade_ready"
    return "drop", "no_same_condition_exclusion_improvement"


def run(*, db_path: Path, output_root: Path, horizon: int, limit_events: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    all_events: list[dict[str, Any]] = []
    family_summaries = []
    try:
        regimes = _regime_rows(conn)
        for family, cfg in FAMILIES.items():
            source_rows = _rows(conn, cfg["sql"], limit_events)
            events = []
            for row in source_rows:
                entry = {"timing": "signal_close", "entry_day": 0, "entry_ymd": int(row["ymd"]), "entry_price": float(row["c"])}
                metrics = _evaluate(row, cfg, entry, horizon)
                if metrics is None:
                    continue
                regime = regimes.get(int(row["ymd"]), {})
                enriched = {
                    "family": family,
                    "code": str(row["code"]),
                    "signal_ymd": int(row["ymd"]),
                    **{key: row.get(key) for key in ["ret20", "ret60", "range60_pos", "dist_ma20", "dist_ma60", "volume_vs20"]},
                    **{key: regime.get(key) for key in ["regime_id", "breadth_above_ma20", "breadth_above_ma60", "advancers_ratio", "index_close_vs_ma20", "index_close_vs_ma60", "regime_score"]},
                    **metrics,
                }
                enriched["blocked_reasons"] = _blocked_reasons(enriched)
                events.append(enriched)
            baseline = _summarize(events)
            leaderboard = []
            for filter_id, blocked in FILTERS.items():
                if blocked:
                    kept_rows = [row for row in events if not set(blocked).intersection(row["blocked_reasons"])]
                    skipped_rows = [row for row in events if set(blocked).intersection(row["blocked_reasons"])]
                else:
                    kept_rows = list(events)
                    skipped_rows = []
                kept = _summarize(kept_rows)
                skipped = _summarize(skipped_rows)
                decision, reason = _decision(kept, skipped, baseline)
                leaderboard.append(
                    {
                        "family": family,
                        "filter_id": filter_id,
                        "blocked_reason_set": blocked,
                        "decision": decision,
                        "reason": reason,
                        "kept": kept,
                        "skipped": skipped,
                    }
                )
            leaderboard.sort(
                key=lambda row: (
                    row["decision"] == "keep",
                    row["decision"] == "hold",
                    row["kept"].get("close10_short_ret_mean") or -1,
                    row["kept"].get("positive_month_rate") or 0,
                ),
                reverse=True,
            )
            family_summaries.append(
                {
                    "family": family,
                    "source_event_count": len(source_rows),
                    "evaluated_event_count": len(events),
                    "baseline": baseline,
                    "filter_leaderboard": leaderboard,
                }
            )
            all_events.extend(events)
    finally:
        conn.close()
    all_baseline = _summarize(all_events)
    overall = []
    for filter_id, blocked in FILTERS.items():
        if blocked:
            kept_rows = [row for row in all_events if not set(blocked).intersection(row["blocked_reasons"])]
            skipped_rows = [row for row in all_events if set(blocked).intersection(row["blocked_reasons"])]
        else:
            kept_rows = list(all_events)
            skipped_rows = []
        kept = _summarize(kept_rows)
        skipped = _summarize(skipped_rows)
        decision, reason = _decision(kept, skipped, all_baseline)
        overall.append({"filter_id": filter_id, "blocked_reason_set": blocked, "decision": decision, "reason": reason, "kept": kept, "skipped": skipped})
    overall.sort(key=lambda row: (row["decision"] == "keep", row["decision"] == "hold", row["kept"].get("close10_short_ret_mean") or -1), reverse=True)
    keep = [row for row in overall if row["decision"] == "keep"]
    hold = [row for row in overall if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_exclusion_filter_branch"
        reason = "at_least_one_exclusion_filter_cleared_profitability_stability_and_skipped_worse_gates"
    elif hold:
        rollup_decision = "hold_exclusion_filter_branch"
        reason = "at_least_one_exclusion_filter_improved_return_but_not_trade_ready"
    else:
        rollup_decision = "drop_exclusion_filter_branch"
        reason = "no_exclusion_filter_cleared_same_condition_profitability_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "fixed_evaluation_conditions": {
            "families": list(FAMILIES),
            "entry": "signal_close_fixed",
            "changed_axis": "exclusion_filter_only",
            "horizon": horizon,
            "limit_events": limit_events,
            "confirmed_non_yahoo_daily_bars": True,
            "market_regime_daily_used_when_available": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "filter_definitions": FILTERS,
        "overall_filter_leaderboard": overall,
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
    _write_json(output_dir / "exclusion_filter_compare.json", report)
    _write_json(output_dir / "family_leaderboard.json", {"rows": family_summaries})
    _write_json(output_dir / "session_leaderboard_rollup.json", {"decision": report["decision"], "overall_filter_leaderboard": overall})
    _write_csv(output_dir / "event_sample.csv", all_events[:1000])
    _write_json(output_root / "latest_exclusion_filter_compare.json", {"run_root": str(output_dir), **report})
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "all_present": all(
                (output_dir / name).exists()
                for name in ["exclusion_filter_compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "event_sample.csv"]
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
