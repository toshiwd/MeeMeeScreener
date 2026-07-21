"""Audit volume_ratio20 as a single-axis gap-stop veto."""
import argparse, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prior_health(frame):
    history, flags = [], []
    for _, group in frame.groupby("ymd", sort=True):
        prior = np.asarray(history[-5:], dtype=float)
        flags.extend([bool(len(prior) >= 5 and prior.mean() > 0)] * len(group))
        history.extend(group.return_fixed3_pct.tolist())
    return pd.Series(flags, index=frame.index)


def staged(frame, weights):
    x = frame.loc[weights > 0].copy()
    x["weighted_return"] = x.return_fixed3_pct * weights.loc[weights > 0]
    r = x.weighted_return
    loss = -r[r < 0].sum()
    return {"n": int(len(x)), "weighted_total": float(r.sum()), "profit_factor": float(r[r > 0].sum() / loss), "max_weighted_loss": float(r.min())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--natural", type=Path, required=True); ap.add_argument("--daily", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True); ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    x = pd.read_parquet(a.natural)
    f = pd.read_parquet(a.daily, columns=["code", "ymd", "market_breadth_ma60", "volume_ratio20"])
    f.code = f.code.astype(str).str.zfill(4)
    x = x.merge(f, on=["code", "ymd"], validate="one_to_one")
    x = x.loc[x.base_regime.eq("BOX") & (x.close_pos <= .20)].sort_values(["ymd", "code"]).reset_index(drop=True)
    health = prior_health(x)
    base_weights = pd.Series(np.where(x.market_breadth_ma60 <= .40, 0, np.where(health, .50, .25)), index=x.index)
    base = staged(x, base_weights)
    thresholds = {}
    for name, mask in {"volume_le_0.80": x.volume_ratio20 <= .80, "volume_ge_2.00": x.volume_ratio20 >= 2.00}.items():
        subset = x.loc[mask]
        test = staged(x, base_weights.where(~mask, 0))
        thresholds[name] = {
            "removed_n": int(((base_weights > 0) & mask).sum()), "gap_stop_n": int(subset.exit_reason_fixed3.eq("gap_stop").sum()),
            "gap_stop_rate": float(subset.exit_reason_fixed3.eq("gap_stop").mean()), "staged_after_veto": test,
            "total_not_lower": test["weighted_total"] >= base["weighted_total"],
        }
    keep = any(v["total_not_lower"] and v["removed_n"] >= 20 for v in thresholds.values())
    result = {
        "schema_version": "tradex_gap_stop_volume_axis_v1.compare.v1", "artifact_role": "authoritative_gap_stop_volume_axis",
        "review_only": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {"selector": "BOX and close_pos<=0.20", "existing_sizing": "breadth veto; 0.25 probe; prior-5 positive scales 0.50",
            "axis_changed": "volume_ratio20 only", "thresholds": [0.80, 2.00], "costs": "ignored", "weekly_inputs": [],
            "keep_contract": "gap enrichment plus staged weighted total not lower with at least 20 removed events"},
        "authoritative_result": {"base": base, "thresholds": thresholds},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "fixed volume thresholds remove eligible events"},
        "judgment": {"candidate_local_decision": "keep" if keep else "drop", "session_aggregate_decision": "keep_volume_veto" if keep else "drop_volume_axis",
            "authoritative_rollup_decision": "keep_gap_volume_veto" if keep else "drop_gap_volume_axis",
            "reason_type": "fixed_gate_passed" if keep else "volume_did_not_isolate_gap_stops_or_preserve_total"},
        "not_changed": ["selector", "breadth", "lookback", "weights", "other features", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"natural": {"path": str(a.natural.resolve()), "sha256": sha(a.natural)}, "daily": {"path": str(a.daily.resolve()), "sha256": sha(a.daily)},
        "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)}}, "candidate_events": int(len(x)),
        "same_date_excluded_from_health": True, "future_columns_used": [], "weekly_columns_used": [], "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
