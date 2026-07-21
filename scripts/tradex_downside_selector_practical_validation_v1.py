"""Validate the discovered downside selector on the natural event population."""
import argparse, hashlib, json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from tradex_downside_environment_family_dataset_v1 import evaluate

SELL = {"PROBE", "CORE", "ADD", "REENTRY_PROBE"}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    values = frame.return_fixed3_pct.astype(float)
    gross_profit = float(values[values > 0].sum())
    gross_loss = float(-values[values < 0].sum())
    return {
        "n": int(len(frame)), "D": int(frame.outcome_fixed3.eq("D").sum()),
        "R": int(frame.outcome_fixed3.eq("R").sum()), "N": int(frame.outcome_fixed3.eq("N").sum()),
        "D_rate": float(frame.outcome_fixed3.eq("D").mean()) if len(frame) else None,
        "mean_fixed3_pct": float(values.mean()) if len(frame) else None,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "max_loss_pct": float(values.min()) if len(frame) else None,
        "unique_codes": int(frame.code.nunique()),
    }


def bootstrap_mean_ci(values, seed=20260716, samples=10000):
    values = np.asarray(values, dtype=float)
    rng = np.random.default_rng(seed)
    means = rng.choice(values, size=(samples, len(values)), replace=True).mean(axis=1)
    return {"samples": samples, "p025": float(np.quantile(means, 0.025)), "p50": float(np.quantile(means, 0.5)), "p975": float(np.quantile(means, 0.975))}


def candidate_result(frame):
    by_year = {str(year): metrics(group) for year, group in frame.groupby("year")}
    by_half = {str(key): metrics(group) for key, group in frame.groupby("half")}
    counts = frame.code.value_counts()
    return {
        "overall": metrics(frame), "years": by_year, "halves": by_half,
        "positive_years": sum(row["mean_fixed3_pct"] > 0 for row in by_year.values()),
        "positive_halves": sum(row["mean_fixed3_pct"] > 0 for row in by_half.values()),
        "half_count": len(by_half),
        "top_code": str(counts.index[0]) if len(counts) else None,
        "top_code_events": int(counts.iloc[0]) if len(counts) else 0,
        "top_code_share": float(counts.iloc[0] / len(frame)) if len(counts) else None,
        "bootstrap_mean_ci": bootstrap_mean_ci(frame.return_fixed3_pct),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--actions", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--monthly", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    actions = pd.read_parquet(args.actions)
    actions.code = actions.code.astype(str).str.zfill(4)
    actions = actions.loc[actions.action.isin(SELL)].drop_duplicates(["code", "ymd"])[["code", "ymd", "action"]]
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "close_pos", "market_breadth_ma20", "dist_ma20_atr"])
    monthly = pd.read_parquet(args.monthly, columns=["code", "ymd", "base_regime", "monthly_selection_state"])
    for frame in (daily, monthly):
        frame.code = frame.code.astype(str).str.zfill(4)
    pit = actions.merge(daily, on=["code", "ymd"], validate="one_to_one").merge(monthly, on=["code", "ymd"], validate="one_to_one")
    pit["year"] = pit.ymd // 10000
    pit["half"] = pit.year.astype(str) + "H" + np.where((pit.ymd // 100) % 100 <= 6, "1", "2")

    connection = duckdb.connect(str(args.db), read_only=True)
    prices = connection.execute(
        "select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,o,h,l,c from daily_bars where code in (select unnest(?)) order by code,date",
        [pit.code.unique().tolist()],
    ).fetchdf()
    connection.close()
    prices.code = prices.code.astype(str).str.zfill(4)
    histories = {code: group.reset_index(drop=True) for code, group in prices.groupby("code")}
    outcomes = [evaluate(histories[row.code], int(row.ymd)) for row in pit.itertuples()]
    natural = pd.concat([pit.reset_index(drop=True), pd.DataFrame(outcomes)], axis=1)
    natural = natural.loc[natural.status.eq("complete")].reset_index(drop=True)
    natural.to_parquet(args.output / "natural_event_ledger.parquet", index=False)

    masks = {
        "baseline": pd.Series(True, index=natural.index),
        "close_pos_le_0.20": natural.close_pos <= 0.20,
        "BOX_close_pos_le_0.20": natural.base_regime.eq("BOX") & (natural.close_pos <= 0.20),
        "BOX_MATURE_close_pos_le_0.20": natural.base_regime.eq("BOX") & natural.monthly_selection_state.eq("MATURE_BOX_UPPER") & (natural.close_pos <= 0.20),
        "BOX_close_pos_le_0.20_breadth_gt_0.40": natural.base_regime.eq("BOX") & (natural.close_pos <= 0.20) & (natural.market_breadth_ma20 > 0.40),
        "BOX_close_pos_le_0.20_above_MA20": natural.base_regime.eq("BOX") & (natural.close_pos <= 0.20) & (natural.dist_ma20_atr > 0),
    }
    candidates = {name: candidate_result(natural.loc[mask]) for name, mask in masks.items()}
    champion = candidates["BOX_close_pos_le_0.20"]
    recent = [champion["years"].get(str(year)) for year in (2024, 2025, 2026)]
    recent_completed_halves = [row for key, row in champion["halves"].items() if key >= "2024H1" and row["n"] >= 20]
    gate_checks = {
        "n_ge_300": champion["overall"]["n"] >= 300,
        "overall_pf_gt_1_20": champion["overall"]["profit_factor"] > 1.20,
        "overall_mean_gt_0_20": champion["overall"]["mean_fixed3_pct"] > 0.20,
        "positive_years_ge_6": champion["positive_years"] >= 6,
        "positive_halves_ge_two_thirds": champion["positive_halves"] * 3 >= champion["half_count"] * 2,
        "all_2024_2026_positive": all(row and row["mean_fixed3_pct"] > 0 for row in recent),
        "year_2026_n_ge_25_pf_gt_1": bool(recent[2] and recent[2]["n"] >= 25 and recent[2]["profit_factor"] > 1),
        "top_code_share_le_0_05": champion["top_code_share"] <= 0.05,
        "bootstrap_lower_bound_gt_0": champion["bootstrap_mean_ci"]["p025"] > 0,
    }
    practical = all(gate_checks.values())
    full_size_ready = len(recent_completed_halves) >= 4 and sum(row["mean_fixed3_pct"] > 0 for row in recent_completed_halves) >= 4
    result = {
        "schema_version": "tradex_downside_selector_practical_validation_v1.compare.v2",
        "artifact_role": "authoritative_downside_selector_practical_validation",
        "review_only": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "natural_population": "all deduped eligible model sell events 2019-2026 with complete outcomes",
            "execution": "next_session_open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "costs": "ignored", "weekly_inputs": [],
            "candidate_fixed": "base_regime=BOX and close_pos<=0.20",
            "practical_gate": list(gate_checks.keys()),
            "year_2019_role": "pre-discovery stress period with small sample",
            "year_2026_role": "post-discovery recent confirmation through available complete events",
        },
        "authoritative_result": {
            "candidates": candidates, "champion": "BOX_close_pos_le_0.20", "gate_checks": gate_checks,
            "operational_scope": {
                "probe_selector_ready": practical, "core_full_size_ready": full_size_ready,
                "recent_completed_halves": len(recent_completed_halves),
                "recent_positive_completed_halves": sum(row["mean_fixed3_pct"] > 0 for row in recent_completed_halves),
                "dropped_additional_axes": ["MATURE_BOX_UPPER", "market_breadth_gt_0.40", "above_MA20"],
            },
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "natural population filtered by PIT BOX and close position only",
            "selected_events": champion["overall"]["n"], "selected_codes": champion["overall"]["unique_codes"],
        },
        "judgment": {
            "candidate_local_decision": "keep" if practical else "hold",
            "session_aggregate_decision": "practical_probe_selector_ready_review_only" if practical else "practical_gate_failed",
            "authoritative_rollup_decision": "practical_keep_BOX_close20_probe_only_core_not_validated" if practical and not full_size_ready else "practical_keep_BOX_close20_review_only" if practical else "hold_continue_research",
            "reason_type": "probe_gates_passed_but_recent_half_stability_blocks_core" if practical and not full_size_ready else "all_fixed_practical_gates_passed" if practical else "one_or_more_fixed_practical_gates_failed",
        },
        "not_changed": ["cost model", "weekly inputs", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {name: {"path": str(path.resolve()), "sha256": sha(path)} for name, path in {"actions": args.actions, "daily": args.daily, "monthly": args.monthly}.items()},
        "db": {"path": str(args.db.resolve()), "read_only": True}, "complete_events": int(len(natural)),
        "unique_events": int(natural[["code", "ymd"]].drop_duplicates().shape[0]), "weekly_columns_used": [], "future_columns_used": [],
        "ledger_sha256": sha(args.output / "natural_event_ledger.parquet"), "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "champion": champion, "gate_checks": gate_checks, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
