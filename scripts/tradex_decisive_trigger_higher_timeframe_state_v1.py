from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_decisive_trigger_full_universe_v1 as base


AXIS_ID = "decisive_trigger_higher_timeframe_state_v1"
SCHEMA_VERSION = "tradex_decisive_trigger_higher_timeframe_state_v1.compare.v1"
TARGETS = {
    "BUY_DECISIVE_CONTINUATION": "M_UP_W_STRONG",
    "SELL_DECISIVE_RETURN_SELL": "M_UP_W_PULLBACK",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def load_bars(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        bars = conn.execute(
            """
            SELECT code, strftime(to_timestamp(date), '%Y-%m-%d') AS trade_date, c, source
            FROM daily_bars
            WHERE source='pan'
              AND date BETWEEN epoch(TIMESTAMP '2022-01-01') AND epoch(TIMESTAMP '2026-07-17')
            ORDER BY code, date
            """
        ).fetchdf()
    if bars.empty or bars.duplicated(["code", "trade_date"]).any():
        raise RuntimeError("PIT higher-timeframe input is empty or duplicated")
    return bars


def add_pit_higher_timeframe(group: pd.DataFrame) -> pd.DataFrame:
    frame = group.sort_values("trade_date").copy()
    dates = pd.to_datetime(frame["trade_date"])
    specs = (("W-FRI", "w", (7, 20)), ("M", "m", (20,)))
    for frequency, prefix, windows in specs:
        period_key = dates.dt.to_period(frequency)
        completed_period_closes = frame.groupby(period_key)["c"].last()
        for window in windows:
            prior_sum = completed_period_closes.shift(1).rolling(window - 1, min_periods=window - 1).sum()
            frame[f"{prefix}ma{window}_pit"] = (period_key.map(prior_sum).to_numpy() + frame["c"].to_numpy()) / window
    return frame


def build_state_table(bars: pd.DataFrame) -> pd.DataFrame:
    states = pd.concat(
        [add_pit_higher_timeframe(group) for _, group in bars.groupby("code", sort=False)],
        ignore_index=True,
    )
    required = ["mma20_pit", "wma7_pit", "wma20_pit"]
    known = states[required].notna().all(axis=1)
    monthly_up = states["c"] > states["mma20_pit"]
    above_w7 = states["c"] > states["wma7_pit"]
    above_w20 = states["c"] > states["wma20_pit"]
    weekly_order_up = states["wma7_pit"] > states["wma20_pit"]
    states["higher_timeframe_state"] = "UNKNOWN"
    states.loc[known & monthly_up & above_w7 & weekly_order_up, "higher_timeframe_state"] = "M_UP_W_STRONG"
    states.loc[known & monthly_up & ~above_w7 & above_w20, "higher_timeframe_state"] = "M_UP_W_PULLBACK"
    states.loc[known & monthly_up & ~above_w20, "higher_timeframe_state"] = "M_UP_W_BROKEN"
    states.loc[known & monthly_up & ~states["higher_timeframe_state"].isin(["M_UP_W_STRONG", "M_UP_W_PULLBACK", "M_UP_W_BROKEN"]), "higher_timeframe_state"] = "M_UP_W_MIXED"
    states.loc[known & ~monthly_up & above_w7 & weekly_order_up, "higher_timeframe_state"] = "M_DOWN_W_STRONG"
    states.loc[known & ~monthly_up & ~states["higher_timeframe_state"].eq("M_DOWN_W_STRONG"), "higher_timeframe_state"] = "M_DOWN_OTHER"
    return states[["code", "trade_date", "c", *required, "higher_timeframe_state"]]


def enrich_events(events: pd.DataFrame, states: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    before = len(events)
    if states.duplicated(["code", "trade_date"]).any():
        raise RuntimeError("Higher-timeframe state join key is not unique")
    enriched = events.merge(states, on=["code", "trade_date"], how="left", validate="many_to_one", suffixes=("", "_state"))
    quality = {
        "event_rows_before_join": int(before),
        "event_rows_after_join": int(len(enriched)),
        "join_row_multiplier": float(len(enriched) / before) if before else None,
        "matched_state_rows": int(enriched["higher_timeframe_state"].notna().sum()),
        "unknown_state_rows": int(enriched["higher_timeframe_state"].fillna("UNKNOWN").eq("UNKNOWN").sum()),
        "duplicate_event_keys_after_join": int(enriched.duplicated(["code", "trade_date", "event_type"]).sum()),
    }
    enriched["higher_timeframe_state"] = enriched["higher_timeframe_state"].fillna("UNKNOWN")
    return enriched, quality


def temporal_positive(events: pd.DataFrame) -> bool:
    completed = events[events["outcome_complete20"]]
    rows = [group for _, group in completed.groupby("event_year") if len(group) >= 20]
    return bool(rows) and all(group["directional_ret20"].mean() > 0 for group in rows)


def build_compare(events: pd.DataFrame, quality: dict[str, Any], source_compare: dict[str, Any]) -> dict[str, Any]:
    state_results: dict[str, Any] = {}
    family_comparisons: dict[str, Any] = {}
    decisions: dict[str, Any] = {}
    for event_type, allowed_state in TARGETS.items():
        family = events[events["event_type"] == event_type]
        base_summary = base.summarize(family)
        state_results[event_type] = {
            state: base.summarize(group)
            for state, group in family.groupby("higher_timeframe_state")
        }
        allowed = family[family["higher_timeframe_state"] == allowed_state]
        allowed_summary = base.summarize(allowed)
        retention = len(allowed) / len(family) if len(family) else 0.0
        lift_mean = (allowed_summary["directional_ret20_mean_pct"] or 0) - (base_summary["directional_ret20_mean_pct"] or 0)
        lift_win = (allowed_summary["directional_ret20_win_rate"] or 0) - (base_summary["directional_ret20_win_rate"] or 0)
        gates = {
            "complete20_count_ge_50": allowed_summary["complete20_count"] >= 50,
            "event_retention_ge_0_20": retention >= 0.20,
            "mean20_lift_gt_0": lift_mean > 0,
            "win_rate_lift_gt_0": lift_win > 0,
            "median20_gt_0": (allowed_summary["directional_ret20_median_pct"] or -999) > 0,
            "trim5_mean20_gt_0": (allowed_summary["directional_ret20_trim5_mean_pct"] or -999) > 0,
            "symbol_equal_mean20_gt_0": (allowed_summary["directional_ret20_symbol_equal_mean_pct"] or -999) > 0,
            "completed_years_n_ge_20_all_positive_mean20": temporal_positive(allowed),
        }
        keep = all(gates.values())
        family_comparisons[event_type] = {
            "allowed_state": allowed_state,
            "baseline": base_summary,
            "allowed": allowed_summary,
            "event_retention_rate": retention,
            "directional_ret20_mean_lift_pct_points": lift_mean,
            "directional_ret20_win_rate_lift": lift_win,
        }
        decisions[event_type] = {
            "candidate_local_decision": "keep_review_only" if keep else "drop",
            "gates": gates,
            "authoritative_decision": "keep_review_only" if keep else "drop",
        }
    kept = [key for key, value in decisions.items() if value["authoritative_decision"] == "keep_review_only"]
    return {
        "schema_version": SCHEMA_VERSION,
        "axis_id": AXIS_ID,
        "artifact_role": "authoritative_higher_timeframe_state_gate_validation",
        "review_only": True,
        "fixed_conditions": {
            "source_event_artifact": source_compare.get("axis_id"),
            "event_thresholds_changed": False,
            "position_sizing_changed": False,
            "hedge_ratio_changed": False,
            "higher_timeframe_axis_only": True,
            "pit_contract": "current daily close plus prior completed weekly/monthly closes",
            "monthly_permission": "current close versus PIT 20-month average",
            "weekly_permission": "current close versus PIT 7/20-week averages",
            "continuation_allowed_state": TARGETS["BUY_DECISIVE_CONTINUATION"],
            "return_sell_allowed_state": TARGETS["SELL_DECISIVE_RETURN_SELL"],
            "future_used_for_gate": False,
            "future_used_for_outcome_only": True,
        },
        "data_quality": quality,
        "state_results": state_results,
        "authoritative_results": family_comparisons,
        "observed_branching": {
            event_type: {
                "baseline_events": comparison["baseline"]["event_count"],
                "retained_events": comparison["allowed"]["event_count"],
                "removed_events": comparison["baseline"]["event_count"] - comparison["allowed"]["event_count"],
                "retention_rate": comparison["event_retention_rate"],
                "selection_divergence_reason": f"retain only {comparison['allowed_state']}",
            }
            for event_type, comparison in family_comparisons.items()
        },
        "family_decisions": decisions,
        "judgment": {
            "candidate_local_decision": "keep_selected_higher_timeframe_gates" if kept else "drop_axis",
            "kept_families": kept,
            "session_aggregate_decision": "hold_review_only_no_meemee_reflection",
            "authoritative_rollup_decision": "hold_review_only_higher_timeframe_axis_complete",
            "reason_type": "pit_higher_timeframe_state_lift_and_temporal_gates",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production trading logic", "daily trigger thresholds", "position sizing", "hedge ratios"],
        "remaining_risks": [
            "allowed states were chosen from the 5541 teacher episode and require unseen-period validation",
            "current partial week and month are PIT-visible but not completed higher-timeframe bars",
            "survivorship bias remains in the current runtime symbol universe",
            "state buckets do not encode monthly range position or prior major high proximity",
        ],
    }


def run(db_path: Path, source_run: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    events = pd.read_parquet(source_run / "decisive_event_ledger.parquet")
    source_compare = json.loads((source_run / "compare.json").read_text(encoding="utf-8"))
    states = build_state_table(load_bars(db_path))
    enriched, quality = enrich_events(events, states)
    compare = build_compare(enriched, quality, source_compare)
    ledger_path = output / "higher_timeframe_event_ledger.parquet"
    compare_path = output / "compare.json"
    audit_path = output / "audit.json"
    enriched.to_parquet(ledger_path, index=False)
    _write_json(compare_path, compare)
    _write_json(
        audit_path,
        {
            "schema_version": "tradex_decisive_trigger_higher_timeframe_state_v1.audit.v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path.resolve()),
            "db_read_only": True,
            "source_run": str(source_run.resolve()),
            "source_compare_sha256": _sha256(source_run / "compare.json"),
            "source_ledger_sha256": _sha256(source_run / "decisive_event_ledger.parquet"),
            "quality": quality,
            "future_used_for_gate": False,
            "review_only": True,
        },
    )
    _write_json(
        output / "_ARTIFACT_COMPLETE.json",
        {
            "complete": True,
            "authoritative": "compare.json",
            "compare_sha256": _sha256(compare_path),
            "audit_sha256": _sha256(audit_path),
            "ledger_sha256": _sha256(ledger_path),
        },
    )
    return {"output": str(output.resolve()), "judgment": compare["judgment"], "family_decisions": compare["family_decisions"]}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--source-run", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(json.dumps(run(args.db, args.source_run, args.output), ensure_ascii=False, indent=2))
