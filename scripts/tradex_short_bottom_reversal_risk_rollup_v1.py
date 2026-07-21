"""Test a six-month bottom-reversal risk filter on the kept short selector."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


DEVELOPMENT_END_YEAR = 2023
VALIDATION_START_YEAR = 2024
RET60_THRESHOLDS = (-0.20, -0.30, -0.40, -0.50)
LOW_DISTANCE_THRESHOLDS = (0.00, 0.03, 0.05, 0.10)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0}
    true_reversal = frame.path_class.eq("TrueUpsideReversal")
    success = frame.path_class.isin(
        ["ImmediateDrop", "ImmediateDropThenReverse", "ReboundThenDrop", "SidewaysThenDrop"]
    )
    years = {}
    for year, rows in frame.groupby("year"):
        years[str(int(year))] = {
            "n": int(len(rows)),
            "target_3pct_hit_rate": float(rows.target_3pct_hit.mean()),
            "fast_clean_target_3pct_hit_rate": float(rows.fast_clean_target_3pct_hit.mean()),
            "true_reversal_rate": float(rows.path_class.eq("TrueUpsideReversal").mean()),
        }
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.astype(str).nunique()),
        "target_3pct_hit_rate": float(frame.target_3pct_hit.mean()),
        "fast_clean_target_3pct_hit_rate": float(frame.fast_clean_target_3pct_hit.mean()),
        "true_reversal_n": int(true_reversal.sum()),
        "true_reversal_rate": float(true_reversal.mean()),
        "success_n": int(success.sum()),
        "success_rate": float(success.mean()),
        "median_low_20d_pct": float(frame.low_20d_pct.median()),
        "median_high_20d_pct": float(frame.high_20d_pct.median()),
        "years": years,
    }


def comparison(baseline: pd.DataFrame, kept: pd.DataFrame, flagged: pd.DataFrame) -> dict:
    base = metrics(baseline)
    keep = metrics(kept)
    flag = metrics(flagged)
    base_success = max(base.get("success_n", 0), 1)
    base_reversal = max(base.get("true_reversal_n", 0), 1)
    return {
        "baseline": base,
        "kept": keep,
        "flagged": flag,
        "retained_member_rate": float(len(kept) / len(baseline)) if len(baseline) else 0.0,
        "success_retention_rate": float(keep.get("success_n", 0) / base_success),
        "true_reversal_reduction_rate": float(
            1.0 - keep.get("true_reversal_n", 0) / base_reversal
        ),
        "target_3pct_hit_delta": float(
            keep.get("target_3pct_hit_rate", 0.0) - base.get("target_3pct_hit_rate", 0.0)
        ),
        "fast_clean_delta": float(
            keep.get("fast_clean_target_3pct_hit_rate", 0.0)
            - base.get("fast_clean_target_3pct_hit_rate", 0.0)
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(args.events).copy()
    data["year"] = data.signal_ymd // 10000
    development = data.loc[data.year.le(DEVELOPMENT_END_YEAR)].copy()
    validation = data.loc[data.year.ge(VALIDATION_START_YEAR)].copy()

    grid = {}
    eligible = []
    for ret60 in RET60_THRESHOLDS:
        for low_distance in LOW_DISTANCE_THRESHOLDS:
            name = f"ret60_le_{abs(int(ret60 * 100))}_low80_le_{int(low_distance * 100)}"
            dev_flag = development.ret_60_0.le(ret60) & development.dist_prior_80_low.le(low_distance)
            result = comparison(development, development.loc[~dev_flag], development.loc[dev_flag])
            checks = {
                "retained_members_ge_70pct": result["retained_member_rate"] >= 0.70,
                "success_retention_ge_70pct": result["success_retention_rate"] >= 0.70,
                "true_reversal_reduction_ge_25pct": result["true_reversal_reduction_rate"] >= 0.25,
                "target3_delta_ge_minus_2pp": result["target_3pct_hit_delta"] >= -0.02,
            }
            grid[name] = {
                "definition": {"ret_60_0_max": ret60, "dist_prior_80_low_max": low_distance},
                "development": result,
                "gate_checks": checks,
            }
            if all(checks.values()):
                eligible.append((name, result))

    selected_name = max(
        eligible,
        key=lambda item: (
            item[1]["true_reversal_reduction_rate"],
            item[1]["success_retention_rate"],
            item[1]["target_3pct_hit_delta"],
        ),
        default=(None, None),
    )[0]
    if selected_name is None:
        selected_name = max(
            grid,
            key=lambda name: (
                grid[name]["development"]["true_reversal_reduction_rate"],
                grid[name]["development"]["success_retention_rate"],
            ),
        )

    definition = grid[selected_name]["definition"]
    risk = data.ret_60_0.le(definition["ret_60_0_max"]) & data.dist_prior_80_low.le(
        definition["dist_prior_80_low_max"]
    )
    data["bottom_reversal_risk"] = risk
    data["bottom_reversal_risk_rule"] = selected_name
    data.to_parquet(args.output / "bottom_reversal_risk_ledger.parquet", index=False)

    dev_risk = risk & data.year.le(DEVELOPMENT_END_YEAR)
    val_risk = risk & data.year.ge(VALIDATION_START_YEAR)
    development_result = comparison(development, data.loc[data.year.le(DEVELOPMENT_END_YEAR) & ~risk], data.loc[dev_risk])
    validation_result = comparison(validation, data.loc[data.year.ge(VALIDATION_START_YEAR) & ~risk], data.loc[val_risk])
    full_result = comparison(data, data.loc[~risk], data.loc[risk])

    validation_checks = {
        "validation_n_ge_40": validation_result["baseline"]["n"] >= 40,
        "success_retention_ge_70pct": validation_result["success_retention_rate"] >= 0.70,
        "true_reversal_reduction_ge_25pct": validation_result["true_reversal_reduction_rate"] >= 0.25,
        "target3_not_worse": validation_result["target_3pct_hit_delta"] >= 0.0,
        "fast_clean_not_worse": validation_result["fast_clean_delta"] >= 0.0,
    }
    keep = all(grid[selected_name]["gate_checks"].values()) and all(validation_checks.values())
    result = {
        "schema_version": "tradex_short_bottom_reversal_risk_rollup_v1.compare.v1",
        "artifact_role": "authoritative_short_bottom_reversal_risk_rollup",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_selector": "kept short initial entry fusion v1 with complete 20-session paths",
            "development_period": "2020-2023",
            "validation_period": "2024-2026",
            "changed_axis": "six-month decline progress using ret_60_0 and distance to prior 80-session low",
            "entry": "next session open",
            "primary_win": "3% decline within 5 sessions regardless of prior rebound",
            "timing_quality": "3% decline within 3 sessions before 3% adverse move",
            "costs": "ignored",
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "development_grid": grid,
            "selected_name": selected_name,
            "selected_definition": definition,
            "selected_by_pre_2024_only": True,
            "development": development_result,
            "validation": validation_result,
            "full": full_result,
            "validation_gate_checks": validation_checks,
            "example_4493_20220217_flagged": bool(
                data.loc[(data.code.astype(str) == "4493") & (data.signal_ymd == 20220217), "bottom_reversal_risk"].any()
            ),
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(risk.sum()),
            "selection_divergence_reason": "flags advanced six-month declines near the prior 80-session low",
            "parent_members": int(len(data)),
            "kept_members": int((~risk).sum()),
            "flagged_members": int(risk.sum()),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_bottom_reversal_risk" if keep else "hold_bottom_reversal_risk",
            "authoritative_rollup_decision": (
                "keep_short_bottom_reversal_risk_v1_review_only"
                if keep else "hold_continue_six_month_shape_research"
            ),
            "reason_type": (
                "development_selected_filter_reduced_true_reversals_without_validation_damage"
                if keep else "validation_or_retention_gate_failed"
            ),
        },
        "not_changed": ["monthly range age", "price band", "entry timing", "position management", "MeeMee", "ranking", "runtime DB"],
        "remaining_risks": [
            "daily scalar features do not encode the full monthly chart shape",
            "true-reversal samples are limited",
            "2022 remains a known adverse regime",
            "risk flag is review-only and not a production exclusion",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"events": {"path": str(args.events.resolve()), "sha256": sha(args.events)}},
        "compare_sha256": sha(compare),
        "ledger_sha256": sha(args.output / "bottom_reversal_risk_ledger.parquet"),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
