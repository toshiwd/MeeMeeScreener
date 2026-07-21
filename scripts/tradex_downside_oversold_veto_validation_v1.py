"""Validate oversold_risk as a single-axis veto on the staged downside selector."""
import argparse, hashlib, json
from pathlib import Path

import numpy as np
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prior_health(frame, lookback=5):
    history, flags = [], []
    for _, group in frame.groupby("ymd", sort=True):
        prior = np.asarray(history[-lookback:], dtype=float)
        flags.extend([bool(len(prior) >= lookback and prior.mean() > 0)] * len(group))
        history.extend(group.return_fixed3_pct.tolist())
    return pd.Series(flags, index=frame.index)


def metrics(frame, weights):
    selected = frame.loc[weights > 0].copy()
    selected["weight"] = weights.loc[weights > 0]
    selected["weighted_return"] = selected.return_fixed3_pct * selected.weight
    values = selected.weighted_return
    loss = float(-values[values < 0].sum())
    halves = selected.groupby("half").weighted_return.agg(["size", "sum"])
    return {
        "n": int(len(selected)),
        "weighted_total": float(values.sum()),
        "weighted_mean": float(values.mean()),
        "profit_factor": float(values[values > 0].sum() / loss) if loss else None,
        "max_weighted_loss": float(values.min()),
        "positive_halves": int((halves["sum"] > 0).sum()),
        "half_count": int(len(halves)),
        "year_2026_weighted_total": float(selected.loc[selected.year.eq(2026), "weighted_return"].sum()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--natural", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--parent-compare", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    natural = pd.read_parquet(args.natural)
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "market_breadth_ma60", "oversold_risk"])
    daily.code = daily.code.astype(str).str.zfill(4)
    data = natural.merge(daily, on=["code", "ymd"], validate="one_to_one")
    data = data.loc[data.base_regime.eq("BOX") & (data.close_pos <= 0.20)].sort_values(["ymd", "code"]).reset_index(drop=True)
    health = prior_health(data)
    base_weights = pd.Series(np.where(data.market_breadth_ma60 <= 0.40, 0.0, np.where(health, 0.50, 0.25)), index=data.index)
    veto_weights = base_weights.where(~data.oversold_risk.eq(1), 0.0)
    base = metrics(data, base_weights)
    veto = metrics(data, veto_weights)
    removed = data.loc[(base_weights > 0) & data.oversold_risk.eq(1)]
    removed_raw = {
        "n": int(len(removed)), "D": int(removed.outcome_fixed3.eq("D").sum()),
        "R": int(removed.outcome_fixed3.eq("R").sum()), "N": int(removed.outcome_fixed3.eq("N").sum()),
        "mean_fixed3_pct": float(removed.return_fixed3_pct.mean()),
        "years": {str(year): int(len(group)) for year, group in removed.groupby("year")},
    }
    checks = {
        "pf_improvement_ge_0_05": veto["profit_factor"] - base["profit_factor"] >= 0.05,
        "weighted_total_not_lower": veto["weighted_total"] >= base["weighted_total"],
        "positive_halves_not_lower": veto["positive_halves"] >= base["positive_halves"],
        "max_weighted_loss_improved": veto["max_weighted_loss"] > base["max_weighted_loss"],
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_downside_oversold_veto_validation_v1.compare.v1",
        "artifact_role": "authoritative_downside_oversold_veto_validation",
        "review_only": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "selector": "base_regime=BOX and close_pos<=0.20",
            "existing_sizing": "breadth_ma60<=0.40 no-trade; otherwise 0.25; prior-5 mean>0 scales to 0.50",
            "axis_changed": "oversold_risk=1 no-trade veto only",
            "execution": "next_session_open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "costs": "ignored", "weekly_inputs": [],
            "keep_contract": "PF improvement>=0.05, total not lower, positive halves not lower, maximum weighted loss improved",
        },
        "authoritative_result": {"base": base, "oversold_veto": veto, "removed": removed_raw, "gate_checks": checks},
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "oversold_risk removes otherwise eligible staged events",
            "removed_events": int(len(removed)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": "keep_oversold_veto" if keep else "drop_oversold_veto",
            "authoritative_rollup_decision": "keep_oversold_no_trade" if keep else "drop_oversold_veto_keep_diagnostic_only",
            "reason_type": "fixed_operational_gate_passed" if keep else "reversal_enrichment_did_not_improve_staged_operation",
        },
        "not_changed": ["selector", "breadth threshold", "lookback", "position weights", "other features", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"natural": {"path": str(args.natural.resolve()), "sha256": sha(args.natural)}, "daily": {"path": str(args.daily.resolve()), "sha256": sha(args.daily)}, "parent_compare": {"path": str(args.parent_compare.resolve()), "sha256": sha(args.parent_compare)}},
        "candidate_events": int(len(data)), "same_date_excluded_from_health": True,
        "future_columns_used": [], "weekly_columns_used": [], "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
