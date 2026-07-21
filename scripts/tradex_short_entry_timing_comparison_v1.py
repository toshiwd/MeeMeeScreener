from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def stats(g: pd.DataFrame) -> dict:
    active = g[g.executed]
    missed = g[~g.executed]
    return {
        "signals": int(len(g)), "executed": int(len(active)),
        "execution_rate": float(g.executed.mean()),
        "drop5_rate_all_signals": float(g.hit5.eq(True).mean()),
        "drop5_rate_executed": float(active.hit5.mean()) if len(active) else None,
        "median_adverse5_pct": float(active.adverse5_pct.median()) if len(active) else None,
        "p90_adverse5_pct": float(active.adverse5_pct.quantile(.9)) if len(active) else None,
        "median_favorable5_pct": float(active.favorable5_pct.median()) if len(active) else None,
        "missed_baseline_hits": int(missed.baseline_hit5.sum()),
        "missed_baseline_hit_rate_all_signals": float(missed.baseline_hit5.sum() / len(g)) if len(g) else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    out = Path(a.output); out.mkdir(parents=True, exist_ok=False)
    ledger = pd.read_parquet(a.ledger)
    ledger = ledger[ledger.action_tier.isin(["Core", "Probe"])].copy()
    bars = pd.read_parquet(a.inventory, columns=["code", "bar_index", "o", "h", "l", "c"])
    base = ledger[["code", "ymd", "bar_index", "period", "action_tier"]].merge(
        bars, on=["code", "bar_index"], how="left", validate="many_to_one")
    for k in range(1, 7):
        z = bars.rename(columns={"bar_index": "future_index", "o": f"o{k}", "h": f"h{k}", "l": f"l{k}", "c": f"c{k}"})
        base["future_index"] = base.bar_index + k
        base = base.merge(z, on=["code", "future_index"], how="left", validate="many_to_one").drop(columns="future_index")

    rows = []
    definitions = {
        "当日引け": (base.c, pd.Series(True, index=base.index), 1),
        "翌日寄り": (base.o1, base.o1.notna(), 1),
        "翌日安値割れ確認後": (base.o2, base.c1.lt(base.l) & base.o2.notna(), 2),
    }
    baseline_low = base[[f"l{k}" for k in range(1, 6)]].min(axis=1)
    baseline_hit = ((base.o1 - baseline_low) / base.o1 * 100).ge(5)
    for name, (entry, execute, start) in definitions.items():
        lows = base[[f"l{k}" for k in range(start, start + 5)]].min(axis=1)
        highs = base[[f"h{k}" for k in range(start, start + 5)]].max(axis=1)
        q = base[["code", "ymd", "period", "action_tier"]].copy()
        q["entry_method"] = name; q["executed"] = execute; q["entry_price"] = entry.where(execute)
        q["favorable5_pct"] = ((q.entry_price - lows) / q.entry_price * 100).where(execute)
        q["adverse5_pct"] = ((highs - q.entry_price) / q.entry_price * 100).where(execute)
        q["hit5"] = q.favorable5_pct.ge(5).where(execute)
        q["baseline_hit5"] = baseline_hit
        rows.append(q)
    detail = pd.concat(rows, ignore_index=True)
    summary = pd.DataFrame([
        {"period": p, "action_tier": t, "entry_method": m, **stats(g)}
        for (p, t, m), g in detail.groupby(["period", "action_tier", "entry_method"], observed=True)
    ])
    detail.to_parquet(out / "entry_timing_episode_ledger.parquet", index=False)
    yearly = pd.DataFrame([
        {"year": int(y), "action_tier": t, "entry_method": m, **stats(g)}
        for (y, t, m), g in detail.assign(year=detail.ymd // 10000).groupby(
            ["year", "action_tier", "entry_method"], observed=True)
    ])
    yearly.to_parquet(out / "entry_timing_yearly_metrics.parquet", index=False)
    summary.to_parquet(out / "entry_timing_metrics.parquet", index=False)
    val = summary[summary.period.eq("validation")].to_dict("records")
    result = {
        "schema_version": "tradex_short_entry_timing_comparison_v1.compare.v1",
        "artifact_role": "authoritative_short_entry_timing_comparison",
        "review_only": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {"development": "2019-2023", "validation": "2024-2026", "horizon": 5,
            "methods": {"当日引け": "signal close", "翌日寄り": "next open", "翌日安値割れ確認後": "if next close < signal low, following open"},
            "costs": "ignored", "population": "fixed Core and Probe membership"},
        "authoritative_result": {"validation": val,
            "validation_years": yearly[yearly.year.ge(2024)].to_dict("records")},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int((detail.entry_method.eq("翌日安値割れ確認後") & ~detail.executed).sum()),
            "selection_divergence_reason": "weakness confirmation delays and suppresses entries"},
        "judgment": {"candidate_local_decision": "hold", "session_aggregate_decision": "hold_compare_before_entry_adoption",
            "authoritative_rollup_decision": "hold_entry_timing_v1_until_yearly_and_missed_move_audit",
            "reason_type": "first_same_population_execution_comparison_requires_followup"},
        "not_changed": ["candidate membership", "MeeMee", "ranking", "runtime DB", "production logic"],
        "remaining_risks": ["close execution realism", "confirmation opportunity cost", "year stability not yet gated"]}
    (out / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2), encoding="utf-8")
    print(json.dumps(val, ensure_ascii=False))


if __name__ == "__main__": main()
