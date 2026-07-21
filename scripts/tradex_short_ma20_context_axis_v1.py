"""Audit MA20 position and slope inside the frozen current short event population."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _load_daily


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    ret = frame.return_fixed3_pct.astype(float)
    x = frame.assign(year=frame.signal_ymd // 10000)
    years = {str(year): {"n": int(len(rows)), "mean_return": float(rows.return_fixed3_pct.mean())}
             for year, rows in x.groupby("year")}
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "mean_return": float(ret.mean()),
        "median_return": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_rate": float(frame.exit_reason.isin(["target", "gap_target"]).mean()),
        "stop_rate": float(frame.exit_reason.isin(["stop_first", "gap_stop"]).mean()),
        "positive_year_rate": float(sum(row["mean_return"] > 0 for row in years.values()) / len(years)),
        "years": years,
    }


def add_ma20_context(daily):
    frames = []
    for code, group in daily.groupby("code", sort=False):
        g = group.sort_values("ymd").copy()
        g["ma20_context"] = g.c.rolling(20, min_periods=20).mean()
        g["ma20_slope5_pct"] = (g.ma20_context / g.ma20_context.shift(5) - 1) * 100
        g["close_vs_ma20_pct"] = (g.c / g.ma20_context - 1) * 100
        frames.append(g[["code", "ymd", "c", "ma20_context", "ma20_slope5_pct", "close_vs_ma20_pct"]])
    return pd.concat(frames, ignore_index=True)


def state(row):
    if row.c < row.ma20_context:
        return "BelowMA20"
    if row.ma20_slope5_pct > 0:
        return "AboveRisingMA20"
    return "AboveFlatFallingMA20"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(a.events)
    daily = _load_daily(a.db, None)
    context = add_ma20_context(daily)
    data = events.merge(context, left_on=["code", "signal_ymd"], right_on=["code", "ymd"], validate="many_to_one")
    data["ma20_state"] = data.apply(state, axis=1)
    data.to_parquet(a.output / "ma20_context_event_ledger.parquet", index=False)

    states = {name: metrics(data.loc[data.ma20_state.eq(name)])
              for name in ["BelowMA20", "AboveFlatFallingMA20", "AboveRisingMA20"]}
    baseline = metrics(data)
    allowed = data.loc[~data.ma20_state.eq("AboveRisingMA20")]
    challenger = metrics(allowed)
    recent = data.loc[data.signal_ymd.ge(20240101)]
    recent_allowed = allowed.loc[allowed.signal_ymd.ge(20240101)]

    anchor_daily = context.loc[context.code.astype(str).eq("9381") & context.ymd.eq(20260716)]
    anchor = None
    if not anchor_daily.empty:
        row = anchor_daily.iloc[-1]
        anchor = {
            "code": "9381",
            "signal_ymd": 20260716,
            "close": float(row.c),
            "ma20": float(row.ma20_context),
            "ma20_slope5_pct": float(row.ma20_slope5_pct),
            "close_vs_ma20_pct": float(row.close_vs_ma20_pct),
            "ma20_state": state(row),
            "decision": "TrendPullbackBlock" if state(row) == "AboveRisingMA20" else "not_blocked",
        }
    recent_years = challenger["years"]
    checks = {
        "9381_trend_pullback_block": bool(anchor and anchor["decision"] == "TrendPullbackBlock"),
        "challenger_mean_gt_baseline": challenger["mean_return"] > baseline["mean_return"],
        "challenger_stop_rate_le_baseline": challenger["stop_rate"] <= baseline["stop_rate"],
        "retained_n_ge_60pct": challenger["n"] >= baseline["n"] * 0.60,
        "recent_mean_gt_baseline_recent": metrics(recent_allowed)["mean_return"] > metrics(recent)["mean_return"],
        "positive_2024plus_years_ge_2": sum(
            row["mean_return"] > 0 for year, row in recent_years.items() if int(year) >= 2024
        ) >= 2,
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_ma20_context_axis_v1.compare.v1",
        "artifact_role": "authoritative_short_ma20_context_axis",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": "frozen current pre-crash EntryReady event ledger",
            "axis_changed": "signal close position vs MA20 and MA20 5-session slope only",
            "states": ["BelowMA20", "AboveFlatFallingMA20", "AboveRisingMA20"],
            "challenger": "veto AboveRisingMA20 as TrendPullbackBlock",
            "execution": "inherited next open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "period": "2019-2026",
            "costs": "ignored",
            "weekly_inputs": [],
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "baseline": baseline,
            "states": states,
            "challenger_without_above_rising_ma20": challenger,
            "recent_2024plus_baseline": metrics(recent),
            "recent_2024plus_challenger": metrics(recent_allowed),
            "anchor_9381": anchor,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(states["AboveRisingMA20"]["n"]),
            "selection_divergence_reason": "vetoes short entries while price remains above a rising MA20",
            "state_counts": {name: states[name]["n"] for name in states},
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_trend_pullback_block" if keep else "hold_ma20_axis",
            "authoritative_rollup_decision": "keep_above_rising_ma20_veto_review_only" if keep else "hold",
            "reason_type": "anchor_and_fixed_outcome_gates_passed" if keep else "one_or_more_ma20_gates_failed",
        },
        "not_changed": ["episode axis", "event axis", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "events": int(len(data)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "ma20_context_event_ledger.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "states": states, "challenger": challenger, "anchor": anchor, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
