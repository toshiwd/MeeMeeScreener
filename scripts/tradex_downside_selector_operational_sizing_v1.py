"""Validate practical no-trade/probe/scale-up sizing for the downside selector."""
import argparse, hashlib, json
from pathlib import Path

import numpy as np
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def weighted_metrics(frame, weight):
    selected = frame.loc[weight > 0].copy()
    selected["weight"] = weight.loc[weight > 0]
    selected["weighted_return"] = selected.return_fixed3_pct * selected.weight
    values = selected.weighted_return
    loss = float(-values[values < 0].sum())
    halves = {str(key): {"n": int(len(group)), "weighted_sum": float(group.weighted_return.sum()), "weighted_mean": float(group.weighted_return.mean())} for key, group in selected.groupby("half")}
    years = {str(key): {"n": int(len(group)), "weighted_sum": float(group.weighted_return.sum()), "weighted_mean": float(group.weighted_return.mean())} for key, group in selected.groupby("year")}
    recent = [row for key, row in halves.items() if key >= "2024H1" and row["n"] >= 20]
    return {
        "n": int(len(selected)), "weighted_mean": float(values.mean()), "weighted_total": float(values.sum()),
        "profit_factor": float(values[values > 0].sum() / loss) if loss else None,
        "max_weighted_loss": float(values.min()), "unique_codes": int(selected.code.nunique()),
        "positive_halves": sum(row["weighted_sum"] > 0 for row in halves.values()), "half_count": len(halves),
        "recent_positive_completed_halves": sum(row["weighted_sum"] > 0 for row in recent), "recent_completed_halves": len(recent),
        "years": years, "halves": halves,
    }


def prior_date_health(frame, lookback):
    history, flags = [], []
    for _, group in frame.groupby("ymd", sort=True):
        prior = np.asarray(history[-lookback:], dtype=float)
        flags.extend([bool(len(prior) >= lookback and prior.mean() > 0)] * len(group))
        history.extend(group.return_fixed3_pct.tolist())
    return pd.Series(flags, index=frame.index)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--natural", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--parent-compare", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    natural = pd.read_parquet(args.natural)
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "market_breadth_ma60"])
    daily.code = daily.code.astype(str).str.zfill(4)
    data = natural.merge(daily, on=["code", "ymd"], validate="one_to_one")
    data = data.loc[data.base_regime.eq("BOX") & (data.close_pos <= 0.20)].sort_values(["ymd", "code"]).reset_index(drop=True)
    health5 = prior_date_health(data, 5)
    breadth_ok = data.market_breadth_ma60 > 0.40
    variants = {
        "static_full": pd.Series(1.0, index=data.index),
        "breadth_veto_full": breadth_ok.astype(float),
        "staged_probe025_scale050": pd.Series(np.where(~breadth_ok, 0.0, np.where(health5, 0.50, 0.25)), index=data.index),
        "staged_probe025_scale100_diagnostic": pd.Series(np.where(~breadth_ok, 0.0, np.where(health5, 1.00, 0.25)), index=data.index),
    }
    results = {name: weighted_metrics(data, weights) for name, weights in variants.items()}
    staged = results["staged_probe025_scale050"]
    gate_checks = {
        "n_ge_300": staged["n"] >= 300,
        "profit_factor_gt_1_40": staged["profit_factor"] > 1.40,
        "weighted_mean_gt_0_15": staged["weighted_mean"] > 0.15,
        "max_weighted_loss_gt_minus3": staged["max_weighted_loss"] > -3.0,
        "positive_halves_ge_two_thirds": staged["positive_halves"] * 3 >= staged["half_count"] * 2,
        "year_2026_positive": staged["years"]["2026"]["weighted_sum"] > 0,
    }
    practical = all(gate_checks.values())
    result = {
        "schema_version": "tradex_downside_selector_operational_sizing_v1.compare.v1",
        "artifact_role": "authoritative_downside_selector_operational_sizing",
        "review_only": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "selector": "base_regime=BOX and close_pos<=0.20",
            "no_trade": "market_breadth_ma60<=0.40", "probe_weight": 0.25,
            "scale_weight": 0.50, "scale_trigger": "mean of prior 5 completed candidate returns >0; same-date events excluded",
            "execution": "next_session_open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "costs": "ignored", "weekly_inputs": [], "full_size_weight_1": "prohibited",
        },
        "authoritative_result": {"variants": results, "gate_checks": gate_checks,
            "operational_scope": {"no_trade_veto_ready": practical, "probe_ready": practical, "scale_to_half_ready": practical, "full_size_ready": False}},
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "PIT breadth veto plus prior-only rolling health changes exposure, not candidate membership",
            "no_trade_events": int((~breadth_ok).sum()), "probe_events": int((breadth_ok & ~health5).sum()), "scale_events": int((breadth_ok & health5).sum()),
        },
        "judgment": {
            "candidate_local_decision": "keep" if practical else "hold",
            "session_aggregate_decision": "practical_staged_selector_ready_review_only" if practical else "staged_gate_failed",
            "authoritative_rollup_decision": "practical_keep_no_trade_probe_scale_half_fullsize_prohibited" if practical else "hold_continue_research",
            "reason_type": "risk_limited_staging_passed_fixed_gates" if practical else "staged_sizing_failed_fixed_gates",
        },
        "not_changed": ["selector membership", "cost model", "weekly inputs", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"natural": {"path": str(args.natural.resolve()), "sha256": sha(args.natural)}, "daily": {"path": str(args.daily.resolve()), "sha256": sha(args.daily)}, "parent_compare": {"path": str(args.parent_compare.resolve()), "sha256": sha(args.parent_compare)}},
        "candidate_events": int(len(data)), "same_date_excluded_from_health": True, "future_columns_used": [], "weekly_columns_used": [], "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "staged": staged, "gate_checks": gate_checks, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
