"""Diagnose a monthly-environment-only short permission gate across frozen blind samples."""
import argparse
import hashlib
import json
from itertools import combinations
from pathlib import Path

import pandas as pd


SELL_ACTIONS = {"PROBE", "CORE", "ADD", "REENTRY_PROBE"}
ENVIRONMENTS = [
    "HIGH_ZONE_FAILURE",
    "MATURE_BOX_UPPER",
    "POST_BOX_RETURN_SELL",
    "UNKNOWN_OR_BREAKDOWN",
]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def stats(frame):
    x = frame[frame.status.eq("complete")].sort_values(["exit_ymd", "code"], na_position="last")
    values = x.return_fixed3_pct
    gain = values[values > 0].sum()
    loss = -values[values < 0].sum()
    equity = values.cumsum()
    drawdown = equity - equity.cummax() if len(equity) else pd.Series(dtype=float)
    run = max_run = 0
    for value in values:
        run = run + 1 if value < 0 else 0
        max_run = max(max_run, run)
    concurrent = 0
    if len(x) and {"entry_ymd", "exit_ymd"}.issubset(x.columns):
        for day in sorted(set(x.entry_ymd.dropna()).union(x.exit_ymd.dropna())):
            concurrent = max(concurrent, int(((x.entry_ymd <= day) & (x.exit_ymd >= day)).sum()))
    return {
        "n": int(len(frame)),
        "completed": int(len(x)),
        "D": int(x.outcome_fixed3.eq("D").sum()),
        "R": int(x.outcome_fixed3.eq("R").sum()),
        "N": int(x.outcome_fixed3.eq("N").sum()),
        "D_rate": float(x.outcome_fixed3.eq("D").mean()) if len(x) else None,
        "R_rate": float(x.outcome_fixed3.eq("R").mean()) if len(x) else None,
        "mean_fixed3_pct": float(values.mean()) if len(values) else None,
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": float(values.min()) if len(values) else None,
        "sum_return_units_pct": float(values.sum()),
        "max_drawdown_units_pct": 0.0 if drawdown.empty else float(drawdown.min()),
        "max_loss_streak": int(max_run),
        "max_concurrent": int(concurrent),
    }


def normalize(frame, sample):
    result = frame.copy()
    result["sample"] = sample
    result["code"] = result.code.astype(str).str.zfill(4)
    result["monthly_state"] = result.monthly_state.fillna("UNKNOWN_OR_BREAKDOWN")
    result["year"] = result.ymd.astype(int) // 10000
    result["model_direction"] = result.model_action.map(
        lambda value: "SELL" if value in SELL_ACTIONS else "NO_SELL"
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample40-ledger", type=Path, required=True)
    parser.add_argument("--sample40-sealed", type=Path, required=True)
    parser.add_argument("--sample32a", type=Path, required=True)
    parser.add_argument("--sample32b", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    sample40 = pd.read_parquet(args.sample40_ledger)
    sealed40 = pd.read_parquet(args.sample40_sealed, columns=["case_id", "monthly_state"])
    sample40 = sample40.merge(sealed40, on="case_id", validate="one_to_one")
    frames = [
        normalize(sample40, "fixed40"),
        normalize(pd.read_parquet(args.sample32a), "fresh32a"),
        normalize(pd.read_parquet(args.sample32b), "fresh32b"),
    ]
    common = [
        "sample", "case_id", "code", "ymd", "year", "bucket", "model_action",
        "human_direction", "model_direction", "monthly_state", "status", "entry_ymd",
        "exit_ymd", "outcome_fixed3", "return_fixed3_pct", "return_h5_close_pct",
    ]
    for frame in frames:
        for column in common:
            if column not in frame:
                frame[column] = pd.NA
    ledger = pd.concat([frame[common] for frame in frames], ignore_index=True)
    ledger_path = args.output / "monthly_environment_diagnostic_ledger.parquet"
    ledger.to_parquet(ledger_path, index=False)

    model = ledger[ledger.model_direction.eq("SELL")]
    human = ledger[ledger.human_direction.eq("SELL")]
    baseline = stats(model)
    candidates = []
    for size in range(1, len(ENVIRONMENTS) + 1):
        for allowed in combinations(ENVIRONMENTS, size):
            selected = model[model.monthly_state.isin(allowed)]
            metric = stats(selected)
            retention = metric["D"] / baseline["D"] if baseline["D"] else None
            keep = bool(
                retention is not None
                and retention >= 0.70
                and metric["R_rate"] < baseline["R_rate"]
                and metric["max_loss_pct"] > baseline["max_loss_pct"]
                and metric["mean_fixed3_pct"] > 0
                and len(allowed) < len(ENVIRONMENTS)
            )
            candidates.append({"allowed_environments": list(allowed), "D_retention": retention,
                               "metrics": metric, "keep_conditions_pass": keep})

    viable = [candidate for candidate in candidates if candidate["keep_conditions_pass"]]
    fixed_candidate = max(
        viable,
        key=lambda candidate: (
            candidate["D_retention"],
            candidate["metrics"]["mean_fixed3_pct"],
        ),
    ) if viable else None
    best_nontrivial = max(
        (candidate for candidate in candidates if len(candidate["allowed_environments"]) < len(ENVIRONMENTS)),
        key=lambda candidate: (
            candidate["D_retention"],
            candidate["metrics"]["mean_fixed3_pct"],
        ),
    )
    by_sample_environment = {}
    for (sample, environment), group in model.groupby(["sample", "monthly_state"]):
        by_sample_environment.setdefault(str(sample), {})[str(environment)] = stats(group)

    result = {
        "schema_version": "tradex_monthly_environment_gate_diagnostic_v1.compare.v1",
        "artifact_role": "authoritative_monthly_environment_only_axis_decision",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "samples": {"fixed40": 40, "fresh32a": 32, "fresh32b": 32},
            "execution": "next_session_open",
            "horizon_sessions": 5,
            "barriers": "short target -3%, stop +3%, same-bar stop-first",
            "costs": "ignored",
            "weekly_inputs": [],
            "changed_axis_only": "monthly_environment_permission",
        },
        "current_champion": "model_sell_without_monthly_environment_gate",
        "current_challengers": "all_nonempty_subsets_of_four_monthly_environment_states",
        "authoritative_results": {
            "model_sell_baseline": baseline,
            "human_sell_reference": stats(human),
        },
        "by_sample_environment_model_sell": by_sample_environment,
        "by_year_environment_model_sell": {
            f"{year}:{environment}": stats(group)
            for (year, environment), group in model.groupby(["year", "monthly_state"])
        },
        "by_action_environment_model_sell": {
            f"{action}:{environment}": stats(group)
            for (action, environment), group in model.groupby(["model_action", "monthly_state"])
        },
        "direction_pairs": {
            str(pair): stats(group)
            for pair, group in ledger.groupby(ledger.model_direction + "__" + ledger.human_direction)
        },
        "candidate_search": candidates,
        "observed_branching": {
            "model_sell_candidates": baseline["n"],
            "monthly_environment_states": ENVIRONMENTS,
            "tested_permission_subsets": len(candidates),
            "viable_nontrivial_candidates": len(viable),
            "best_nontrivial_by_D_retention": best_nontrivial,
            "sample_environment_reversal": {
                "environment": "POST_BOX_RETURN_SELL",
                "fresh32a_mean_fixed3_pct": by_sample_environment["fresh32a"]["POST_BOX_RETURN_SELL"]["mean_fixed3_pct"],
                "fresh32b_mean_fixed3_pct": by_sample_environment["fresh32b"]["POST_BOX_RETURN_SELL"]["mean_fixed3_pct"],
            },
        },
        "judgment": {
            "candidate_local_decision": "hold" if fixed_candidate else "drop",
            "session_aggregate_decision": "hold_pending_unused_validation" if fixed_candidate else "drop_monthly_environment_only_axis",
            "authoritative_rollup_decision": "hold_pending_unused_validation" if fixed_candidate else "drop",
            "fixed_candidate": fixed_candidate,
            "reason": "one monthly-environment permission rule is frozen for unused validation; it is not a keep until fresh evidence passes all gates" if fixed_candidate else "no nontrivial monthly-environment permission subset passed all adjustment gates",
            "unused_validation_sample_required": bool(fixed_candidate),
            "next_axis_if_validation_drops": "market_regime_only",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            name: {"path": str(path.resolve()), "sha256": sha(path)}
            for name, path in {
                "sample40_ledger": args.sample40_ledger,
                "sample40_sealed": args.sample40_sealed,
                "sample32a": args.sample32a,
                "sample32b": args.sample32b,
            }.items()
        },
        "rows": int(len(ledger)),
        "unique_code_ymd": int(ledger[["code", "ymd"]].drop_duplicates().shape[0]),
        "weekly_columns_used": [],
        "runtime_writes": 0,
        "ledger_sha256": sha(ledger_path),
        "compare_sha256": sha(compare_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "judgment": result["judgment"],
                      "observed_branching": result["observed_branching"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
