from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha, _write
from scripts.tradex_two_sided_sell_only_exposure_cap_v1 import (
    SELL_VOLUME_CAP,
    allocate,
    correct_sell_cost_contract,
    evaluate,
)

AXIS_ID = "tradex_two_sided_buy_topk_pruning_v1"
LEDGER_ROOT = Path(r"G:\Tradex\tradex_two_sided_pit_allocation_v1\20260713T100616Z-tradex_two_sided_pit_allocation_v1")
BASE_COMPARE = Path(r"G:\Tradex\tradex_two_sided_sell_only_exposure_cap_completion_v1\20260713T125123Z-tradex_two_sided_sell_only_exposure_cap_completion_v1\compare.json")
DEFAULT_OUT = Path(r"G:\Tradex\tradex_two_sided_buy_topk_pruning_v1")
PERIODS = {"train": (20240101, 20241231), "validation": (20250101, 20251231), "shadow": (20260101, 20261231)}


def _june_metrics(rows: pd.DataFrame) -> dict:
    rows = rows[rows.signal_ymd.between(20260601, 20260630)]
    daily = rows.groupby("signal_ymd").trade_return_h10.mean()
    loss = -float(daily[daily < 0].sum())
    return {
        "trade_count": int(len(rows)),
        "signal_days": int(len(daily)),
        "trade_win_rate": float((rows.trade_return_h10 > 0).mean()) if len(rows) else None,
        "trade_expectancy": float(rows.trade_return_h10.mean()) if len(rows) else None,
        "daily_win_rate": float((daily > 0).mean()) if len(daily) else None,
        "daily_expectancy": float(daily.mean()) if len(daily) else None,
        "daily_profit_factor": float(daily[daily > 0].sum()) / loss if loss else None,
    }


def generate(db_path: Path, out_root: Path = DEFAULT_OUT) -> Path:
    buy = pd.read_parquet(LEDGER_ROOT / "fixed_buy_ledger_with_exit_ymd.parquet")
    sell = correct_sell_cost_contract(pd.read_parquet(LEDGER_ROOT / "fixed_sell_ledger_with_exit_ymd.parquet"))
    sell = sell[sell.v <= SELL_VOLUME_CAP].copy()
    with duckdb.connect(str(db_path), read_only=True) as conn:
        calendar = conn.execute(
            "select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) d "
            "from daily_bars where source='pan' and date>=epoch(date '2024-01-01') order by d"
        ).fetchdf().d.astype(int).tolist()
    periods = {**PERIODS, "shadow": (20260101, max(calendar))}
    variants = []
    for top_k in range(1, 11):
        selected_buy = buy[buy["rank"] <= top_k].copy()
        timeline = allocate(selected_buy, sell, 0.25)
        metrics = {name: evaluate(timeline, selected_buy, calendar, start, end)["portfolio"] for name, (start, end) in periods.items()}
        variants.append({"top_k": top_k, "metrics": metrics, "june_2026": _june_metrics(selected_buy)})
    chosen = max(
        variants,
        key=lambda row: (
            row["metrics"]["train"]["calendar_profit_factor"] or -1,
            row["metrics"]["train"]["calendar_expectancy"] or -1,
        ),
    )
    baseline = next(row for row in variants if row["top_k"] == 10)
    validation = chosen["metrics"]["validation"]
    shadow = chosen["metrics"]["shadow"]
    gates = {
        "validation_pf_ge_1_30": (validation["calendar_profit_factor"] or 0) >= 1.30,
        "validation_expectancy_positive": (validation["calendar_expectancy"] or 0) > 0,
        "shadow_pf_ge_1_30": (shadow["calendar_profit_factor"] or 0) >= 1.30,
        "shadow_expectancy_positive": (shadow["calendar_expectancy"] or 0) > 0,
        "frequency_ge_1_week_both": validation["signals_per_week"] >= 1 and shadow["signals_per_week"] >= 1,
        "validation_not_worse_than_baseline_pf": validation["calendar_profit_factor"] >= baseline["metrics"]["validation"]["calendar_profit_factor"],
        "shadow_not_worse_than_baseline_pf": shadow["calendar_profit_factor"] >= baseline["metrics"]["shadow"]["calendar_profit_factor"],
    }
    keep = all(gates.values())
    run_root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    run_root.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "single_axis": "BUY maximum admitted source rank only",
            "top_k_values": list(range(1, 11)),
            "selection": "2024 PF maximum, expectancy tie-break",
            "validation": "2025",
            "untouched_shadow": "2026",
            "buy_sell_allocation": "BUY100; both BUY90/SELL10; SELL-only SELL25/cash75",
            "entry_exit_costs_sell_rule_changed": False,
            "fallback": False,
        },
        "source_artifacts": [
            {"path": str(db_path), "sha256": _sha(db_path)},
            {"path": str(BASE_COMPARE), "sha256": _sha(BASE_COMPARE)},
        ],
        "variants": variants,
        "selected_variant": chosen,
        "baseline_top10": baseline,
        "adoption_gates": gates,
        "decision": {
            "candidate_local_decision": "keep" if keep else "drop",
            "authoritative_rollup_decision": "review_only",
            "reason_type": "all_fixed_condition_gates_pass" if keep else "validation_or_cross_period_stability_failed",
        },
        "silent_fallback_used": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    compare_path = run_root / "compare.json"
    _write(compare_path, payload)
    _write(run_root / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(compare_path)})
    return compare_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
