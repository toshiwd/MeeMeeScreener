from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


WINDOWS = (1, 2, 3, 5)


def summarize(frame: pd.DataFrame) -> dict:
    r = frame["short_return"]
    yearly = frame.groupby("year")["short_return"].agg(["count", "mean"])
    weak = [str(int(y)) for y, row in yearly.iterrows() if row["mean"] < 0]
    return {
        "row_count": int(len(frame)),
        "date_count": int(frame["confirm_ymd"].nunique()),
        "code_count": int(frame["code"].nunique()),
        "win_rate": float((r > 0).mean()),
        "mean_return": float(r.mean()),
        "median_return": float(r.median()),
        "profit_ge_10pct_rate": float((r >= 0.10).mean()),
        "loss_le_minus_5pct_rate": float((r <= -0.05).mean()),
        "severe_loss_le_minus_10pct_rate": float((r <= -0.10).mean()),
        "avg_confirm_lag": float(frame["confirm_lag"].mean()),
        "weak_years_mean_below_zero": weak,
        "yearly": {str(int(y)): {"n": int(v["count"]), "mean_return": float(v["mean"])} for y, v in yearly.iterrows()},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--setups", required=True)
    ap.add_argument("--source-compare", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    setups = pd.read_csv(a.setups, dtype={"code": str})
    source = json.loads(Path(a.source_compare).read_text(encoding="utf-8"))
    conn = duckdb.connect(a.db, read_only=True)
    conn.register("codes", setups[["code"]].drop_duplicates())
    bars = conn.execute(
        "select d.code,d.date,d.o,d.h,d.l,d.c,d.v from daily_bars d "
        "semi join codes x on d.code=x.code where d.source='pan' order by d.code,d.date"
    ).df()
    conn.close()
    bars["code"] = bars["code"].astype(str)
    groups = {k: v.reset_index(drop=True) for k, v in bars.groupby("code", sort=False)}
    rows = []
    for setup in setups.itertuples(index=False):
        g = groups[setup.code]
        hits = g.index[g["date"].eq(int(setup.date))]
        if len(hits) != 1:
            raise RuntimeError(f"setup mismatch {setup.code} {setup.ymd}")
        i = int(hits[0])
        confirmations = []
        for lag in range(1, max(WINDOWS) + 1):
            j = i + lag
            c0 = float(g.loc[j, "c"])
            rule = (
                c0 / float(g.loc[j - 1, "c"]) - 1 < -0.005
                and c0 / float(g.loc[j - 5, "c"]) - 1 <= c0 / float(g.loc[j - 20, "c"]) - 1
            )
            confirmations.append(rule)
        for window in WINDOWS:
            valid = [x for x in range(window) if confirmations[x]]
            if not valid:
                continue
            lag = valid[0] + 1
            j = i + lag
            path = g.iloc[j + 1 : j + 6]
            if len(path) != 5:
                raise RuntimeError(f"immature confirmation {setup.code} {setup.ymd}")
            entry = float(g.loc[j, "c"])
            exit_h, reason, exit_close = 5, "time5", float(path.iloc[-1]["c"])
            for h, bar in enumerate(path.itertuples(index=False), 1):
                close = float(bar.c)
                if entry / close - 1 >= 0.10:
                    exit_h, reason, exit_close = h, "profit_take", close
                    break
            confirm_ymd = int(pd.to_datetime(int(g.loc[j, "date"]), unit="s").strftime("%Y%m%d"))
            rows.append({
                "code": setup.code, "setup_ymd": int(setup.ymd), "confirm_ymd": confirm_ymd,
                "year": confirm_ymd // 10000, "window": window, "confirm_lag": lag,
                "entry_close": entry, "exit_h": exit_h, "exit_reason": reason,
                "short_return": entry / exit_close - 1,
            })
    ledger = pd.DataFrame(rows)
    results = {}
    source_counts = {x["key"]: int(x["metrics"]["row_count"]) for x in source["grid"]}
    for window in WINDOWS:
        key = f"second_down_w{window}"
        m = summarize(ledger[ledger["window"].eq(window)])
        checks = {
            "sample_at_least_250": m["row_count"] >= 250,
            "mean_positive": m["mean_return"] > 0,
            "severe_loss_under_10pct": m["severe_loss_le_minus_10pct_rate"] < 0.10,
            "weak_year_count_at_most_3": len(m["weak_years_mean_below_zero"]) <= 3,
        }
        results[key] = {
            "metrics": m, "checks": checks, "gate_pass": all(checks.values()),
            "source_reported_count": source_counts[key],
            "reproduction_count_delta": m["row_count"] - source_counts[key],
        }
    passed = [k for k, v in results.items() if v["gate_pass"]]
    chosen = max(passed, key=lambda k: results[k]["metrics"]["mean_return"]) if passed else None
    decision = "keep" if chosen else "drop"
    payload = {
        "schema_version": "tradex_extreme_high_roll_timing_corrected_v1.compare.v1",
        "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {
            "setup_population": "fixed 512 candidate_extreme_high_roll events",
            "confirmation_rule": "ret1 < -0.005 and ret5 <= ret20",
            "changed_axis": "confirmation window 1,2,3,5 sessions only",
            "entry": "confirmation close", "exit": "profit_take_10pct_or_time5_strict",
            "cost_slippage": "not_applied", "runtime_db_write": False,
            "production_ranking_changed": False, "meemee_reflection_allowed": False,
        },
        "authoritative_result": {"candidates": results, "chosen_candidate": chosen},
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "confirmation window changes event membership; ranking not evaluated",
        },
        "source_contract_audit": {
            "source_exit_contract_consistent": False,
            "w2_unexplained_missing_event": {"code": "5246", "setup_ymd": 20240227, "confirm_ymd": 20240229},
        },
        "judgment": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": f"corrected_timing_{decision}",
            "reason_type": "strict_5day_exit_and_reproducible_second_down_rule",
            "router_reflection_allowed": False,
        },
        "source_artifact": a.source_compare,
    }
    ledger.to_csv(out / "corrected_timing_event_ledger.csv", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(payload["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
