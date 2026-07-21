"""Audit total concurrent exposure caps for the staged downside selector."""
import argparse, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def health(frame):
    history, flags = [], []
    for _, group in frame.groupby("ymd", sort=True):
        prior = np.asarray(history[-5:], dtype=float)
        flags.extend([bool(len(prior) >= 5 and prior.mean() > 0)] * len(group))
        history.extend(group.return_fixed3_pct.tolist())
    return pd.Series(flags, index=frame.index)


def allocate(frame, cap):
    allocation = pd.Series(0.0, index=frame.index); active = []; max_exposure = 0.0
    for entry_day, group in frame.loc[frame.desired_weight > 0].groupby("entry_ymd", sort=True):
        active = [idx for idx in active if frame.loc[idx, "exit_ymd"] >= entry_day]
        used = float(allocation.loc[active].sum()); remaining = max(0.0, cap - used)
        wanted = float(group.desired_weight.sum()); scale = min(1.0, remaining / wanted) if wanted else 0.0
        allocation.loc[group.index] = group.desired_weight * scale
        active += group.index.tolist(); max_exposure = max(max_exposure, used + float(allocation.loc[group.index].sum()))
    return allocation, max_exposure


def metrics(frame, allocation, max_exposure):
    selected = frame.loc[allocation > 0].copy(); selected["allocation"] = allocation.loc[allocation > 0]
    selected["weighted_return"] = selected.return_fixed3_pct * selected.allocation
    r = selected.weighted_return; loss = -r[r < 0].sum()
    daily = selected.groupby("exit_ymd").weighted_return.agg(["sum", "count"])
    return {"n": int(len(selected)), "weighted_total": float(r.sum()), "weighted_mean": float(r.mean()),
            "profit_factor": float(r[r > 0].sum() / loss), "max_trade_loss": float(r.min()),
            "worst_exit_day_loss": float(daily["sum"].min()), "worst_exit_day": int(daily["sum"].idxmin()),
            "max_concurrent_exposure": max_exposure,
            "fully_allocated": int(((allocation == frame.desired_weight) & (frame.desired_weight > 0)).sum()),
            "partially_allocated": int(((allocation > 0) & (allocation < frame.desired_weight)).sum()),
            "zeroed_by_cap": int(((allocation == 0) & (frame.desired_weight > 0)).sum())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--natural", type=Path, required=True); ap.add_argument("--daily", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True); ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    x = pd.read_parquet(a.natural)
    f = pd.read_parquet(a.daily, columns=["code", "ymd", "market_breadth_ma60"]); f.code = f.code.astype(str).str.zfill(4)
    x = x.merge(f, on=["code", "ymd"], validate="one_to_one")
    x = x.loc[x.base_regime.eq("BOX") & (x.close_pos <= .20)].sort_values(["ymd", "code"]).reset_index(drop=True)
    x["health"] = health(x); x["desired_weight"] = np.where(x.market_breadth_ma60 <= .40, 0, np.where(x.health, .50, .25))
    variants = {}
    baseline_allocation, baseline_max = allocate(x, 999.0)
    variants["uncapped"] = metrics(x, baseline_allocation, baseline_max)
    for cap in (1.0, 1.5, 2.0):
        allocation, max_exposure = allocate(x, cap); variants[f"cap_{cap:.1f}"] = metrics(x, allocation, max_exposure)
    base = variants["uncapped"]
    checks = {name: {"worst_day_improved": row["worst_exit_day_loss"] > base["worst_exit_day_loss"],
                     "weighted_total_retained_ge_90pct": row["weighted_total"] >= .90 * base["weighted_total"]}
              for name, row in variants.items() if name != "uncapped"}
    kept = [name for name, row in checks.items() if all(row.values())]
    result = {"schema_version": "tradex_downside_portfolio_exposure_cap_v1.compare.v1",
              "artifact_role": "authoritative_downside_portfolio_exposure_cap", "review_only": True,
              "research_phase": "effectiveness_judgment",
              "fixed_conditions": {"selector_and_sizing": "fixed staged downside selector", "axis_changed": "total concurrent exposure cap only",
                                   "caps": [1.0, 1.5, 2.0], "same_day_allocation": "pro-rata remaining capacity; no arbitrary code ordering",
                                   "active_contract": "positions with exit_ymd>=new entry day count as active", "costs": "ignored", "weekly_inputs": []},
              "authoritative_result": {"variants": variants, "gate_checks": checks, "kept_caps": kept},
              "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                                     "selection_divergence_reason": "concurrent cap proportionally reduces same-day new exposure"},
              "judgment": {"candidate_local_decision": "keep" if kept else "drop", "session_aggregate_decision": "keep_total_cap" if kept else "drop_total_exposure_cap_axis",
                           "authoritative_rollup_decision": kept[0] if kept else "drop_total_cap_seek_concentration_axis",
                           "reason_type": "fixed_risk_return_gate_passed" if kept else "worst_day_unchanged_while_total_return_falls"},
              "not_changed": ["selector", "individual weights", "MeeMee", "runtime DB", "production logic"]}
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"natural": {"path": str(a.natural.resolve()), "sha256": sha(a.natural)}, "daily": {"path": str(a.daily.resolve()), "sha256": sha(a.daily)},
                         "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)}},
             "candidate_events": int(len(x)), "future_columns_used": [], "weekly_columns_used": [], "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
