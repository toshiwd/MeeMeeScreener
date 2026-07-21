from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

from tradex_extreme_high_roll_composite_denial_v1 import metrics


POLICIES = ("baseline", "adverse3_strong_close", "two_close_reclaim", "setup_high_strong_close")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--timing-ledger", required=True)
    ap.add_argument("--timing-compare", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    events = pd.read_csv(a.timing_ledger, dtype={"code": str})
    events = events[events["window"].eq(5)].copy()
    timing = json.loads(Path(a.timing_compare).read_text(encoding="utf-8"))
    conn = duckdb.connect(a.db, read_only=True)
    conn.register("codes", events[["code"]].drop_duplicates())
    bars = conn.execute(
        "select d.code,d.date,d.o,d.h,d.l,d.c from daily_bars d semi join codes x on d.code=x.code "
        "where d.source='pan' order by d.code,d.date"
    ).df()
    conn.close()
    bars["code"] = bars["code"].astype(str)
    groups = {k: v.reset_index(drop=True) for k, v in bars.groupby("code", sort=False)}
    records = []
    for event_id, event in enumerate(events.itertuples(index=False)):
        g = groups[event.code]
        dates = pd.to_datetime(g["date"], unit="s").dt.strftime("%Y%m%d").astype(int)
        entry_i = int(g.index[dates.eq(int(event.confirm_ymd))][0])
        setup_i = int(g.index[dates.eq(int(event.setup_ymd))][0])
        path = g.iloc[entry_i + 1 : entry_i + 6]
        entry, setup_high = float(event.entry_close), float(g.loc[setup_i, "h"])
        for policy in POLICIES:
            reason, exit_h, exit_close, streak = "time5", 5, float(path.iloc[-1]["c"]), 0
            for h, bar in enumerate(path.itertuples(index=False), 1):
                close = float(bar.c)
                if entry / close - 1 >= 0.10:
                    reason, exit_h, exit_close = "profit_take", h, close
                    break
                span = max(float(bar.h) - float(bar.l), 1e-12)
                strong_bull = close > float(bar.o) and (close - float(bar.l)) / span >= 0.70
                streak = streak + 1 if close > entry else 0
                denied = (
                    (policy == "adverse3_strong_close" and close >= entry * 1.03 and strong_bull)
                    or (policy == "two_close_reclaim" and streak >= 2 and strong_bull)
                    or (policy == "setup_high_strong_close" and close > setup_high and strong_bull)
                )
                if denied:
                    reason, exit_h, exit_close = "composite_denial", h, close
                    break
            records.append({
                "event_id": event_id, "code": event.code, "setup_ymd": int(event.setup_ymd),
                "confirm_ymd": int(event.confirm_ymd), "policy": policy, "exit_h": exit_h,
                "exit_reason": reason, "short_return": entry / exit_close - 1,
            })
    ledger = pd.DataFrame(records)
    table = {p: metrics(ledger[ledger["policy"].eq(p)]) for p in POLICIES}
    base = table["baseline"]
    for p in POLICIES[1:]:
        table[p]["mean_return_delta"] = table[p]["mean_return"] - base["mean_return"]
        table[p]["severe_loss_rate_delta"] = table[p]["severe_loss_le_minus_10pct_rate"] - base["severe_loss_le_minus_10pct_rate"]
    passed = [p for p in POLICIES[1:] if table[p]["mean_return"] >= base["mean_return"] and table[p]["severe_loss_le_minus_10pct_rate"] < base["severe_loss_le_minus_10pct_rate"]]
    chosen = max(passed, key=lambda p: table[p]["mean_return"]) if passed else None
    decision = "keep" if chosen else "drop"
    expected = timing["authoritative_result"]["candidates"]["second_down_w5"]["metrics"]
    payload = {
        "schema_version": "tradex_extreme_high_roll_w5_denial_confirm_v1.compare.v1",
        "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {
            "population": "corrected second_down_w5 events", "entry": "confirmation close",
            "exit": "profit_take_10pct_or_time5_strict", "changed_axis": "composite denial only",
            "cost_slippage": "not_applied", "production_ranking_changed": False,
            "runtime_db_write": False, "meemee_reflection_allowed": False,
        },
        "authoritative_result": {"baseline": base, "policies": table, "chosen_policy": chosen},
        "baseline_reconciliation": {
            "expected_rows": expected["row_count"], "actual_rows": base["row_count"],
            "expected_mean": expected["mean_return"], "actual_mean": base["mean_return"],
            "exact": expected["row_count"] == base["row_count"] and abs(expected["mean_return"] - base["mean_return"]) < 1e-12,
        },
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "entry population fixed; exit only"},
        "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": f"w5_composite_denial_{decision}", "router_reflection_allowed": False, "reason_type": "mean_not_reduced_and_severe_loss_reduced"},
    }
    ledger.to_csv(out / "w5_denial_event_ledger.csv", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
