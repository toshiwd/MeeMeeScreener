from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


VARIANTS = [
    {"window": 40, "min_mean": 0.0, "min_win": 0.45},
    {"window": 40, "min_mean": 0.0, "min_win": 0.50},
    {"window": 60, "min_mean": 0.0, "min_win": 0.45},
    {"window": 60, "min_mean": 0.0, "min_win": 0.50},
    {"window": 120, "min_mean": 0.0, "min_win": 0.45},
    {"window": 120, "min_mean": 0.0, "min_win": 0.50},
]


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0, "codes": 0, "mean_return_pct": None, "median_return_pct": None,
                "win_rate": None, "severe_loss5_rate": None, "top3_positive_profit_share": None}
    positive = frame.loc[frame.realized_ret > 0, "realized_ret"]
    total = float(positive.sum())
    return {"n": int(len(frame)), "codes": int(frame.code.nunique()),
            "mean_return_pct": float(frame.realized_ret.mean()),
            "median_return_pct": float(frame.realized_ret.median()),
            "win_rate": float(frame.realized_ret.gt(0).mean()),
            "severe_loss5_rate": float(frame.realized_ret.le(-5).mean()),
            "top3_positive_profit_share": None if total <= 0 else float(positive.nlargest(3).sum() / total)}


def dedupe(frame: pd.DataFrame) -> pd.DataFrame:
    kept: list[int] = []
    for _, group in frame.sort_values(["code", "bar_index"]).groupby("code", sort=False):
        last = -10**9
        for index, bar_index in zip(group.index, group.bar_index):
            if int(bar_index) - last > 5:
                kept.append(index)
                last = int(bar_index)
    return frame.loc[kept].copy()


def add_permission(rows: pd.DataFrame, variant: dict) -> pd.DataFrame:
    rows = rows.sort_values(["signal_date", "code"]).copy()
    history = rows.sort_values("exit_date")
    dates = rows.signal_date.drop_duplicates().sort_values()
    permission: dict[pd.Timestamp, dict] = {}
    end = 0
    completed: list[float] = []
    history_records = history[["exit_date", "realized_ret"]].to_records(index=False)
    for date in dates:
        while end < len(history_records) and pd.Timestamp(history_records[end][0]) < date:
            completed.append(float(history_records[end][1]))
            end += 1
        sample = completed[-variant["window"]:]
        mean = None if len(sample) < variant["window"] else sum(sample) / len(sample)
        win = None if len(sample) < variant["window"] else sum(value > 0 for value in sample) / len(sample)
        permission[date] = {"prior_n": len(sample), "prior_mean": mean, "prior_win": win,
                            "allowed": mean is not None and mean > variant["min_mean"] and win >= variant["min_win"]}
    rows["prior_n"] = rows.signal_date.map(lambda x: permission[x]["prior_n"])
    rows["prior_mean"] = rows.signal_date.map(lambda x: permission[x]["prior_mean"])
    rows["prior_win"] = rows.signal_date.map(lambda x: permission[x]["prior_win"])
    rows["allowed"] = rows.signal_date.map(lambda x: permission[x]["allowed"])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--source-compare", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    rows = pd.read_parquet(args.ledger)
    rows["signal_date"] = pd.to_datetime(rows.signal_date)
    rows["exit_date"] = pd.to_datetime(rows.exit_date)
    variants = []
    for variant in VARIANTS:
        evaluated = add_permission(rows, variant)
        validation = dedupe(evaluated[evaluated.year.eq(2025) & evaluated.allowed])
        variants.append({"variant": variant, "validation": metrics(validation)})
    selectable = [item for item in variants if item["validation"]["n"] >= 250
                  and (item["validation"]["mean_return_pct"] or -99) > 0
                  and (item["validation"]["win_rate"] or 0) >= .50
                  and (item["validation"]["severe_loss5_rate"] or 1) <= .03]
    selected = max(selectable, key=lambda x: x["validation"]["mean_return_pct"], default=None)
    if selected:
        evaluated = add_permission(rows, selected["variant"])
        test = dedupe(evaluated[evaluated.year.eq(2026) & evaluated.allowed])
    else:
        evaluated = rows.assign(allowed=False)
        test = evaluated.iloc[0:0].copy()
    test_metrics = metrics(test)
    monthly = {str(month): metrics(group) for month, group in test.groupby(test.signal_date.dt.to_period("M"))}
    positive_months = sum((item["mean_return_pct"] or -99) > 0 for item in monthly.values())
    checks = {"selected_on_2025_only": selected is not None, "test_n_at_least_250": test_metrics["n"] >= 250,
              "test_mean_positive": (test_metrics["mean_return_pct"] or -99) > 0,
              "test_win_rate_at_least_50pct": (test_metrics["win_rate"] or 0) >= .50,
              "test_severe_loss5_at_most_3pct": (test_metrics["severe_loss5_rate"] or 1) <= .03,
              "test_top3_profit_share_at_most_35pct": (test_metrics["top3_positive_profit_share"] or 1) <= .35,
              "test_months_majority_positive": bool(monthly) and positive_months / len(monthly) >= .70}
    decision = "hold_for_portfolio_gate" if all(checks.values()) else "drop"
    payload = {"schema_version": "tradex_long_pit_rolling_permission_v1.compare.v1", "artifact_role": "authoritative",
               "generated_at": datetime.now(timezone.utc).isoformat(),
               "fixed_evaluation_conditions": {"axis": "rolling permission from strictly completed prior family trades",
                   "selection_period": 2025, "untouched_test": 2026, "variants": VARIANTS,
                   "no_lookahead": "exit_date must be strictly earlier than decision signal_date",
                   "source_compare": args.source_compare, "production_ranking_changed": False,
                   "runtime_db_write": False, "meemee_reflection_allowed": False},
               "authoritative_result": {"variants": variants, "selected": selected, "test_2026": test_metrics,
                   "monthly_2026": monthly, "checks": checks},
               "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None,
                   "changed_rank_count": int(len(test)), "selection_divergence_reason": "prior-completed-trade regime permission"},
               "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": decision,
                   "reason_type": "point_in_time_rolling_permission_gate"},
               "remaining_risks": ["portfolio capital allocation pending if event gate passes"]}
    test.to_parquet(output / "permitted_test_ledger.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"selected": selected, "test": test_metrics, "checks": checks, "decision": decision}, ensure_ascii=False))


if __name__ == "__main__":
    main()
