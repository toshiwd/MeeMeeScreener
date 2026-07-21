"""Reveal and evaluate the frozen unused-code downside-room sample."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


THRESHOLD_ATR = 0.5
GATED_ACTIONS = {"PROBE", "REENTRY_PROBE", "ADD"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evaluate(group: pd.DataFrame, signal_ymd: int) -> dict:
    hit = group.index[group.ymd.eq(signal_ymd)]
    if len(hit) != 1 or int(hit[0]) + 5 >= len(group):
        return {"status": "censored"}
    window = group.iloc[int(hit[0]) + 1:int(hit[0]) + 6]
    entry = float(window.iloc[0].o)
    target, stop = entry * 0.97, entry * 1.03
    exit_price, outcome, reason = float(window.iloc[-1].c), "N", "horizon_close"
    exit_ymd = int(window.iloc[-1].ymd)
    for row in window.itertuples():
        if row.o >= stop:
            exit_price, outcome, reason, exit_ymd = float(row.o), "R", "gap_stop", int(row.ymd); break
        if row.o <= target:
            exit_price, outcome, reason, exit_ymd = float(row.o), "D", "gap_target", int(row.ymd); break
        if row.h >= stop and row.l <= target:
            exit_price, outcome, reason, exit_ymd = stop, "R", "same_bar_stop_first", int(row.ymd); break
        if row.h >= stop:
            exit_price, outcome, reason, exit_ymd = stop, "R", "stop", int(row.ymd); break
        if row.l <= target:
            exit_price, outcome, reason, exit_ymd = target, "D", "target", int(row.ymd); break
    return {
        "status": "complete", "entry_ymd": int(window.iloc[0].ymd), "exit_ymd": exit_ymd,
        "entry_open": entry, "exit_price_fixed3": exit_price, "exit_reason_fixed3": reason,
        "outcome_fixed3": outcome, "return_fixed3_pct": 100 * (entry - exit_price) / entry,
        "return_h5_close_pct": 100 * (entry - float(window.iloc[-1].c)) / entry,
    }


def stats(frame: pd.DataFrame) -> dict:
    x = frame[frame.status.eq("complete")].copy()
    v = x.return_fixed3_pct.dropna()
    gain, loss = v[v > 0].sum(), -v[v < 0].sum()
    ordered = x.sort_values(["exit_ymd", "code"])
    equity = ordered.return_fixed3_pct.cumsum()
    drawdown = equity - equity.cummax() if len(equity) else pd.Series(dtype=float)
    run = max_run = 0
    for value in ordered.return_fixed3_pct:
        run = run + 1 if value < 0 else 0
        max_run = max(max_run, run)
    concurrent = 0
    if len(ordered):
        for day in sorted(set(ordered.entry_ymd).union(ordered.exit_ymd)):
            concurrent = max(concurrent, int(((ordered.entry_ymd <= day) & (ordered.exit_ymd >= day)).sum()))
    return {
        "n": int(len(frame)), "completed": int(len(x)),
        "D": int(x.outcome_fixed3.eq("D").sum()), "R": int(x.outcome_fixed3.eq("R").sum()),
        "N": int(x.outcome_fixed3.eq("N").sum()),
        "D_rate": None if x.empty else float(x.outcome_fixed3.eq("D").mean()),
        "R_rate": None if x.empty else float(x.outcome_fixed3.eq("R").mean()),
        "mean_fixed3_pct": None if v.empty else float(v.mean()),
        "mean_h5_close_pct": None if x.empty else float(x.return_h5_close_pct.mean()),
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": None if v.empty else float(v.min()),
        "sum_return_units_pct": float(v.sum()),
        "max_drawdown_units_pct": 0.0 if drawdown.empty else float(drawdown.min()),
        "max_loss_streak": int(max_run), "max_concurrent": int(concurrent),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sample", type=Path, required=True)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    sample_path = args.sample / "unseen_sample_frozen.parquet"
    sample = pd.read_parquet(sample_path)
    codes = sample.code.astype(str).str.zfill(4).unique().tolist()
    con = duckdb.connect(str(args.db), read_only=True)
    prices = con.execute(
        "select code, strftime(to_timestamp(date), '%Y%m%d')::integer ymd, o, h, l, c "
        "from daily_bars where code in (select unnest(?)) order by code, date", [codes]
    ).fetchdf()
    prices.code = prices.code.astype(str).str.zfill(4)
    histories = {code: g.reset_index(drop=True) for code, g in prices.groupby("code")}

    rows = []
    for item in sample.itertuples():
        g = histories[item.code]
        hit = g.index[g.ymd.eq(int(item.ymd))]
        if len(hit) != 1:
            raise RuntimeError(f"missing signal bar {item.code} {item.ymd}")
        i = int(hit[0]); prior = g.iloc[:i]; through = g.iloc[:i + 1]
        prev = through.c.shift(1)
        tr = pd.concat([through.h - through.l, (through.h - prev).abs(), (through.l - prev).abs()], axis=1).max(axis=1)
        atr14 = float(tr.tail(14).mean()); close = float(g.iloc[i].c)
        supports = {"prior_low20": float(prior.tail(20).l.min()), "prior_low60": float(prior.tail(60).l.min())}
        for n in (20, 60, 100, 200):
            supports[f"ma{n}"] = float(through.c.tail(n).mean()) if len(through) >= n else np.nan
        lower = {k: v for k, v in supports.items() if np.isfinite(v) and v < close}
        support_type, support = max(lower.items(), key=lambda z: z[1]) if lower else (None, np.nan)
        room = (close - support) / atr14 if np.isfinite(support) and atr14 > 0 else np.nan
        gate_pass = item.action not in GATED_ACTIONS or bool(room >= THRESHOLD_ATR)
        outcome = evaluate(g, int(item.ymd))
        rows.append({
            **item._asdict(), "signal_close": close, "atr14": atr14,
            "nearest_lower_support_type": support_type, "nearest_lower_support": support,
            "downside_room_atr": room, "gate_pass": gate_pass,
            "gate_action": item.action if gate_pass else "PROBE_RISK_NO_ADD" if item.action == "ADD" else "WAIT_OR_MIN_PROBE",
            **supports, **outcome,
        })
    ledger = pd.DataFrame(rows)
    ledger_path = args.output / "unseen_validation_ledger.parquet"
    ledger.to_parquet(ledger_path, index=False)
    baseline, challenger = ledger, ledger[ledger.gate_pass]
    base_stats, challenge_stats = stats(baseline), stats(challenger)
    result = {
        "schema_version": "tradex_downside_room_unseen_validation_v1.compare.v1",
        "artifact_role": "authoritative_threshold_unseen_validation",
        "review_only": True,
        "fixed_conditions": {
            "sample_frozen_before_outcome_reveal": True, "threshold_atr": THRESHOLD_ATR,
            "CORE": "always pass", "gated_actions": sorted(GATED_ACTIONS),
            "support_candidates": ["prior_low20", "prior_low60", "ma20", "ma60", "ma100", "ma200"],
            "prior_lows_exclude_signal_day": True, "execution": "next_session_open", "horizon_sessions": 5,
            "barriers": "short target -3%, stop +3%, same-bar stop-first", "costs": "ignored", "weekly_inputs": [],
            "validation_type": "unused-code threshold validation; not clean model-training OOS",
        },
        "model_alone": base_stats, "complement_gate": challenge_stats,
        "by_action_model": {str(k): stats(v) for k, v in baseline.groupby("action")},
        "by_action_gate": {str(k): stats(v) for k, v in challenger.groupby("action")},
        "environment_breadth_model": {str(k): stats(v) for k, v in baseline.groupby("monthly_state")},
        "environment_breadth_gate": {str(k): stats(v) for k, v in challenger.groupby("monthly_state")},
        "observed_branching": {
            "baseline_candidates": int(len(baseline)), "challenger_candidates": int(len(challenger)),
            "removed_candidates": int((~ledger.gate_pass).sum()),
            "removed_by_action": {str(k): int(v) for k, v in ledger.loc[~ledger.gate_pass, "action"].value_counts().items()},
            "D_retention": None if base_stats["D"] == 0 else challenge_stats["D"] / base_stats["D"],
            "R_removed": base_stats["R"] - challenge_stats["R"],
            "selection_divergence_reason": "fixed 0.5 ATR room gate applied only to staged actions",
        },
        "human_alone": {"status": "pending_blind_direction_labels_on_same_frozen_sample"},
        "judgment": {"decision": "pending_metric_gate"},
        "not_changed": ["model actions", "MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    keep = (
        challenge_stats["R_rate"] <= base_stats["R_rate"]
        and challenge_stats["max_loss_pct"] >= base_stats["max_loss_pct"]
        and result["observed_branching"]["D_retention"] >= 0.70
        and challenge_stats["mean_fixed3_pct"] > 0
    )
    result["judgment"] = {
        "candidate_local_decision": "keep" if keep else "drop",
        "authoritative_rollup_decision": "hold_pending_human_same_sample_review" if keep else "drop",
        "keep_conditions": {"R_rate_not_worse": challenge_stats["R_rate"] <= base_stats["R_rate"],
            "max_loss_not_worse": challenge_stats["max_loss_pct"] >= base_stats["max_loss_pct"],
            "D_retention_ge_0_70": result["observed_branching"]["D_retention"] >= 0.70,
            "positive_expectancy": challenge_stats["mean_fixed3_pct"] > 0},
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sample_compare_sha256": sha(args.sample / "compare.json"), "sample_ledger_sha256": sha(sample_path),
        "db_path": str(args.db.resolve()), "db_read_only": True, "rows": int(len(ledger)),
        "completed": int(ledger.status.eq("complete").sum()), "weekly_columns_used": [],
        "selection_recomputed_after_reveal": False, "ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "model": base_stats, "gate": challenge_stats,
        "branching": result["observed_branching"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
