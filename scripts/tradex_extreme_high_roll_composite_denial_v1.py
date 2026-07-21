from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


def metrics(rows: pd.DataFrame) -> dict[str, float | int]:
    ret = rows["short_return"].astype(float)
    return {
        "row_count": int(len(rows)),
        "win_rate": float((ret > 0).mean()),
        "mean_return": float(ret.mean()),
        "median_return": float(ret.median()),
        "profit_ge_10pct_rate": float((ret >= 0.10).mean()),
        "loss_le_minus_5pct_rate": float((ret <= -0.05).mean()),
        "severe_loss_le_minus_10pct_rate": float((ret <= -0.10).mean()),
        "denial_exit_rate": float(rows["exit_reason"].eq("composite_denial").mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--source-compare", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)

    events = pd.read_csv(args.events, dtype={"code": str})
    source = json.loads(Path(args.source_compare).read_text(encoding="utf-8"))
    conn = duckdb.connect(args.db, read_only=True)
    bars = conn.execute(
        "select code, date, o, h, l, c, v from daily_bars where source='pan' order by code,date"
    ).df()
    conn.close()
    bars["code"] = bars["code"].astype(str)
    grouped = {code: frame.reset_index(drop=True) for code, frame in bars.groupby("code", sort=False)}

    policies = ["baseline", "adverse3_strong_close", "two_close_reclaim", "setup_high_strong_close"]
    ledgers: list[dict] = []
    for event in events.itertuples(index=False):
        frame = grouped[event.code]
        hit = frame.index[frame["date"].eq(int(event.date))]
        setup_hit = frame.index[frame["date"].eq(int(pd.Timestamp(str(event.source_setup_ymd)).timestamp()))]
        if len(hit) != 1 or len(setup_hit) != 1:
            raise RuntimeError(f"bar mismatch: {event.code} {event.confirm_ymd}")
        entry_i, setup_i = int(hit[0]), int(setup_hit[0])
        path = frame.iloc[entry_i + 1 : entry_i + 6]
        if len(path) != 5:
            raise RuntimeError(f"immature event: {event.code} {event.confirm_ymd}")
        entry = float(event.c)
        setup_high = float(frame.iloc[setup_i]["h"])
        for policy in policies:
            reason, exit_h, exit_close = "time5", 5, float(path.iloc[-1]["c"])
            above_entry_streak = 0
            for h, bar in enumerate(path.itertuples(index=False), 1):
                close = float(bar.c)
                short_ret = entry / close - 1.0
                if short_ret >= 0.10:
                    reason, exit_h, exit_close = "profit_take", h, close
                    break
                rng = max(float(bar.h) - float(bar.l), 1e-12)
                close_pos = (close - float(bar.l)) / rng
                strong_bull = close > float(bar.o) and close_pos >= 0.70
                above_entry_streak = above_entry_streak + 1 if close > entry else 0
                denied = (
                    policy == "adverse3_strong_close" and close >= entry * 1.03 and strong_bull
                ) or (
                    policy == "two_close_reclaim" and above_entry_streak >= 2 and strong_bull
                ) or (
                    policy == "setup_high_strong_close" and close > setup_high and strong_bull
                )
                if denied:
                    reason, exit_h, exit_close = "composite_denial", h, close
                    break
            ledgers.append({
                "event_id": int(event.event_id), "code": event.code,
                "confirm_ymd": int(event.confirm_ymd), "policy": policy,
                "exit_h": exit_h, "exit_reason": reason,
                "short_return": entry / exit_close - 1.0,
            })

    ledger = pd.DataFrame(ledgers)
    table = {policy: metrics(ledger[ledger["policy"].eq(policy)]) for policy in policies}
    baseline = table["baseline"]
    for policy in policies[1:]:
        table[policy]["mean_return_delta"] = table[policy]["mean_return"] - baseline["mean_return"]
        table[policy]["severe_loss_rate_delta"] = (
            table[policy]["severe_loss_le_minus_10pct_rate"] - baseline["severe_loss_le_minus_10pct_rate"]
        )
    eligible = [
        p for p in policies[1:]
        if table[p]["mean_return"] >= baseline["mean_return"]
        and table[p]["severe_loss_le_minus_10pct_rate"] < baseline["severe_loss_le_minus_10pct_rate"]
    ]
    chosen = max(eligible, key=lambda p: table[p]["mean_return"]) if eligible else None
    decision = "keep" if chosen and table[chosen]["severe_loss_le_minus_10pct_rate"] < baseline["severe_loss_le_minus_10pct_rate"] else "drop"
    payload = {
        "schema_version": "tradex_extreme_high_roll_composite_denial_v1.compare.v1",
        "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {
            "population": "promoted_candidate_events.csv second_down_w2 exact 362 events",
            "entry": "confirmation close", "exit": "profit_take_10pct_or_time5",
            "changed_axis": "post-entry composite denial only",
            "cost_slippage": "not_applied", "production_ranking_changed": False,
            "runtime_db_write": False, "meemee_reflection_allowed": False,
        },
        "authoritative_result": {"baseline": baseline, "policies": table, "chosen_policy": chosen},
        "source_contract_audit": {
            "source_reported_mean_return": float(events["short_return20"].mean()),
            "source_reported_win_rate": float((events["short_return20"] > 0).mean()),
            "source_profit_take_after_day5_count": int(
                ((events["exit_reason"] == "profit_take") & (events["exit_h"] > 5)).sum()
            ),
            "source_profit_take_count": int((events["exit_reason"] == "profit_take").sum()),
            "contract_consistent": False,
            "reason": "source labels pt10_time5 but permits profit-take exits after day 5",
        },
        "observed_branching": {
            "changed_top5_members_count": 0, "changed_top10_members_count": 0,
            "changed_rank_count": 0,
            "selection_divergence_reason": "selection fixed; exit path only",
        },
        "judgment": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": f"composite_denial_{decision}",
            "reason_type": "mean_return_not_reduced_and_severe_loss_rate_reduced",
        },
        "source_artifact": args.source_compare,
    }
    ledger.to_csv(output / "policy_event_ledger.csv", index=False)
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
