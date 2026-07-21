"""Validate exit, partial-profit, trailing, and re-add policies for the kept short selector."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


TAKE_FRACTIONS = (1 / 3, 0.5, 2 / 3, 1.0)
TRAIL_POINTS = (3.0, 5.0, 7.0, 10.0)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reversal_exit_day(row) -> int | None:
    closes = list(row.future_close_pct)
    signal_high_pct = 100.0 * (row.signal_high_price / row.entry_open_20d - 1.0)
    for idx in range(1, len(closes)):
        if (
            closes[idx] > signal_high_pct
            and closes[idx - 1] > signal_high_pct
            and closes[idx] >= closes[idx - 1]
        ):
            return idx
    return None


def simulate(row, take_fraction: float, trail_points: float, readd: bool) -> dict:
    opens = list(row.future_open_pct)
    highs = list(row.future_high_pct)
    lows = list(row.future_low_pct)
    closes = list(row.future_close_pct)
    reverse_day = reversal_exit_day(row)
    realized = 0.0
    weight = 1.0
    took_profit = False
    trough = float("inf")
    rebound = False
    added = False
    add_entry = None
    exit_day = len(closes) - 1
    exit_close = closes[-1]
    exit_reason = "day20_close"
    for idx in range(len(closes)):
        trough = min(trough, lows[idx])
        if not took_profit and reverse_day is not None and idx == reverse_day:
            exit_day = idx
            exit_close = closes[idx]
            exit_reason = "two_close_signal_high_recovery"
            break
        if not took_profit and lows[idx] <= -3.0:
            realized += take_fraction * 3.0
            weight -= take_fraction
            took_profit = True
        if took_profit and highs[idx] >= trough + 5.0:
            rebound = True
        if (
            readd
            and took_profit
            and rebound
            and not added
            and idx >= 1
            and closes[idx] < opens[idx]
            and closes[idx] < lows[idx - 1]
        ):
            weight += take_fraction
            add_entry = closes[idx]
            added = True
        if took_profit and weight > 0 and closes[idx] >= trough + trail_points:
            exit_day = idx
            exit_close = closes[idx]
            exit_reason = f"trailing_rebound_{trail_points:g}"
            break
    pnl = realized + weight * (-exit_close)
    if added and add_entry is not None:
        pnl += take_fraction * add_entry
    return {
        "pnl_pct": pnl,
        "exit_day": exit_day + 1,
        "exit_reason": exit_reason,
        "took_profit": took_profit,
        "readded": added,
    }


def metrics(frame: pd.DataFrame) -> dict:
    years = {
        str(int(year)): {"n": int(len(rows)), "mean_pnl_pct": float(rows.pnl_pct.mean())}
        for year, rows in frame.groupby("year")
    }
    return {
        "n": int(len(frame)),
        "mean_pnl_pct": float(frame.pnl_pct.mean()),
        "median_pnl_pct": float(frame.pnl_pct.median()),
        "loss_rate": float(frame.pnl_pct.lt(0).mean()),
        "tail_10pct_pnl": float(frame.pnl_pct.quantile(0.10)),
        "upper_90pct_pnl": float(frame.pnl_pct.quantile(0.90)),
        "mean_hold_days": float(frame.exit_day.mean()),
        "readd_rate": float(frame.readded.mean()),
        "positive_years": int(sum(row["mean_pnl_pct"] > 0 for row in years.values())),
        "year_count": int(len(years)),
        "years": years,
    }


def trigger_metrics(data: pd.DataFrame) -> dict:
    trigger_days = pd.Series(
        [reversal_exit_day(row) for row in data.itertuples(index=False)],
        index=data.index,
        dtype="float",
    )
    reversal = data.path_class.eq("TrueUpsideReversal")
    delayed = data.path_class.eq("ReboundThenDrop")
    success = data.path_class.isin(["ImmediateDrop", "ReboundThenDrop"])
    before_drop = trigger_days.notna() & (
        data.first_3pct_drop_day_20d.isna()
        | trigger_days.add(1).lt(data.first_3pct_drop_day_20d)
    )
    return {
        "true_reversal_detection_rate": float(trigger_days[reversal].notna().mean()),
        "delayed_drop_false_exit_rate": float(before_drop[delayed].mean()),
        "all_success_false_exit_rate": float(before_drop[success].mean()),
        "median_true_reversal_exit_day": float(trigger_days[reversal].add(1).median()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(args.events).copy()
    data["year"] = data.signal_ymd // 10000
    policies = {}
    ledgers = {}
    for fraction in TAKE_FRACTIONS:
        for trail in TRAIL_POINTS:
            for readd in (False, True):
                name = f"take_{fraction:.4f}_trail_{trail:g}_readd_{str(readd).lower()}"
                rows = []
                for row in data.itertuples(index=False):
                    result = simulate(row, fraction, trail, readd)
                    rows.append({
                        "code": str(row.code),
                        "signal_ymd": int(row.signal_ymd),
                        "year": int(row.signal_ymd) // 10000,
                        "path_class": str(row.path_class),
                        **result,
                    })
                ledger = pd.DataFrame(rows)
                ledgers[name] = ledger
                policies[name] = metrics(ledger)
    hold_candidates = [
        (name, row)
        for name, row in policies.items()
        if "_readd_false" in name
        and not name.startswith("take_1.0000")
        and row["mean_pnl_pct"] > 0
        and row["median_pnl_pct"] > 0
        and row["tail_10pct_pnl"] > -10
        and row["loss_rate"] < 0.25
        and row["positive_years"] >= 5
        and all(
            values["mean_pnl_pct"] > 0
            for year, values in row["years"].items()
            if int(year) >= 2024
        )
    ]
    selected_name, selected = max(
        hold_candidates,
        key=lambda item: (
            item[1]["mean_pnl_pct"],
            item[1]["median_pnl_pct"],
            item[1]["tail_10pct_pnl"],
        ),
        default=(None, None),
    )
    if selected_name is None:
        raise RuntimeError("no retained-runner policy passed the fixed gates")
    selected_ledger = ledgers[selected_name]
    selected_ledger.to_parquet(args.output / "selected_management_ledger.parquet", index=False)
    selected_readd_name = selected_name.replace("_readd_false", "_readd_true")
    selected_readd = policies[selected_readd_name]
    trigger = trigger_metrics(data)
    checks = {
        "true_reversal_detection_ge_50": trigger["true_reversal_detection_rate"] >= 0.50,
        "delayed_drop_false_exit_le_25": trigger["delayed_drop_false_exit_rate"] <= 0.25,
        "selected_mean_positive": selected["mean_pnl_pct"] > 0,
        "selected_median_positive": selected["median_pnl_pct"] > 0,
        "selected_tail_gt_minus10": selected["tail_10pct_pnl"] > -10,
        "selected_positive_years_ge_5": selected["positive_years"] >= 5,
        "all_2024plus_positive": all(
            row["mean_pnl_pct"] > 0
            for year, row in selected["years"].items()
            if int(year) >= 2024
        ),
        "readd_does_not_improve_mean": selected_readd["mean_pnl_pct"] <= selected["mean_pnl_pct"],
        "readd_does_not_improve_tail": selected_readd["tail_10pct_pnl"] <= selected["tail_10pct_pnl"],
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_position_management_rollup_v1.compare.v1",
        "artifact_role": "authoritative_short_position_management_rollup",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "selector": "kept short initial entry fusion v1",
            "horizon_sessions": 20,
            "pre_drop_reversal_exit": (
                "two consecutive closes above signal high and second close >= first"
            ),
            "profit_trigger": "intraday low reaches -3% from initial entry",
            "take_fractions": list(TAKE_FRACTIONS),
            "trailing_points_from_running_low": list(TRAIL_POINTS),
            "readd_rule": (
                "after profit trigger and >=5 point rebound, bearish close below prior low"
            ),
            "costs": "ignored",
        },
        "authoritative_result": {
            "reversal_trigger": trigger,
            "policy_grid": policies,
            "selected_hold_policy": selected_name,
            "selected_hold_policy_metrics": selected,
            "same_policy_with_readd": {
                "name": selected_readd_name,
                "metrics": selected_readd,
            },
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(policies)),
            "selection_divergence_reason": (
                "compares profit fraction, trailing rebound, and re-add management"
            ),
            "policy_count": int(len(policies)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": (
                "keep_position_management_no_readd" if keep else "hold_management"
            ),
            "authoritative_rollup_decision": (
                "keep_short_position_management_v1_review_only"
                if keep else "hold_continue_management_research"
            ),
            "reason_type": (
                "exit_and_partial_profit_gates_passed_while_readd_worsened_results"
                if keep else "one_or_more_management_gates_failed"
            ),
        },
        "not_changed": ["selector", "MeeMee", "ranking", "runtime DB", "production logic"],
        "remaining_risks": [
            "daily OHLC cannot establish intraday order beyond explicit assumptions",
            "2022 remains adverse",
            "re-add rule may require intraday or weekly context not represented here",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"events": {"path": str(args.events.resolve()), "sha256": sha(args.events)}},
        "compare_sha256": sha(compare),
        "ledger_sha256": sha(args.output / "selected_management_ledger.parquet"),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
