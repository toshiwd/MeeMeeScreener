"""Find direction-stable observable drivers at NEW_CORE close without thresholds."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = tuple(range(2019, 2026))
FEATURES = [
    "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "close_pos", "ret3", "ret5",
    "pos20", "bear_count5", "bear_body5_atr", "upper_supply_count5", "lower_rejection_count5",
    "low_close_count3", "dist_ma7_atr", "dist_ma20_atr", "dist_ma60_atr", "ma7_slope5_atr",
    "ma20_slope5_atr", "ma60_slope5_atr", "volume_ratio20", "support_break_depth_atr",
    "oversold_risk", "weekly_lower_high", "weekly_upper_wick_ratio", "weekly_close_pos",
    "monthly_high_failure", "range20_pct",
]
GROUPS = {
    **{x: "candle" for x in ["body_ratio", "upper_wick_ratio", "lower_wick_ratio", "close_pos"]},
    **{x: "move_location" for x in ["ret3", "ret5", "pos20", "range20_pct"]},
    **{x: "sequence" for x in ["bear_count5", "bear_body5_atr", "upper_supply_count5", "lower_rejection_count5", "low_close_count3"]},
    **{x: "ma_position" for x in ["dist_ma7_atr", "dist_ma20_atr", "dist_ma60_atr"]},
    **{x: "ma_slope" for x in ["ma7_slope5_atr", "ma20_slope5_atr", "ma60_slope5_atr"]},
    "volume_ratio20": "volume", "support_break_depth_atr": "support", "oversold_risk": "risk",
    **{x: "weekly" for x in ["weekly_lower_high", "weekly_upper_wick_ratio", "weekly_close_pos"]},
    "monthly_high_failure": "monthly",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def auc_high(down: pd.Series, rebound: pd.Series) -> float | None:
    down = down.dropna(); rebound = rebound.dropna()
    if down.empty or rebound.empty:
        return None
    combined = pd.concat([down.rename("value"), rebound.rename("value")], ignore_index=True)
    ranks = combined.rank(method="average")
    nd, nr = len(down), len(rebound)
    return float((ranks.iloc[:nd].sum() - nd * (nd + 1) / 2) / (nd * nr))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence-ledger", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    seq = pd.read_parquet(args.sequence_ledger)
    core = seq[
        seq.gd_ymd.notna() & seq.ma20_rebreak_ymd.notna()
        & (seq.ma20_rebreak_ymd > seq.gd_ymd) & seq.weak_rebound
        & (seq.max_consecutive_closes_above_ma7 < 7)
    ].copy()
    core["ymd"] = core.ma20_rebreak_ymd.astype(int)
    core["year"] = core.ymd.astype(str).str[:4].astype(int)
    core["outcome"] = core.ma20_rebreak_outcome_fixed3_h5
    core["code"] = core.code.astype(str).str.zfill(4)
    core = core[core.year.isin(YEARS) & core.outcome.isin(["down_first", "rebound_first"])]
    duplicates_removed = int(core.duplicated(["code", "ymd", "outcome"]).sum())
    core = core.drop_duplicates(["code", "ymd", "outcome"])[["code", "ymd", "year", "outcome"]]

    feature = pd.read_parquet(args.features, columns=["code", "ymd"] + FEATURES)
    feature["code"] = feature.code.astype(str).str.zfill(4)
    joined = core.merge(feature, on=["code", "ymd"], how="left", validate="one_to_one")

    drivers = []
    for name in FEATURES:
        yearly = {}; aucs = []
        for year in YEARS:
            z = joined[joined.year.eq(year)]
            down = z[z.outcome.eq("down_first")][name]
            rebound = z[z.outcome.eq("rebound_first")][name]
            auc = auc_high(down, rebound); aucs.append(auc)
            yearly[str(year)] = {
                "down_n": int(down.notna().sum()), "rebound_n": int(rebound.notna().sum()),
                "down_median": None if down.dropna().empty else float(down.median()),
                "rebound_median": None if rebound.dropna().empty else float(rebound.median()),
                "auc_high_value_predicts_down": auc,
            }
        higher = all(value is not None and value > 0.5 for value in aucs)
        lower = all(value is not None and value < 0.5 for value in aucs)
        stable = higher or lower
        strength = min(abs(value - 0.5) for value in aucs if value is not None)
        drivers.append({
            "feature": name, "group": GROUPS[name], "year_results": yearly,
            "consistent_direction": "HIGHER_IS_DOWN" if higher else "LOWER_IS_DOWN" if lower else "UNSTABLE",
            "stable_min_auc_distance": float(strength), "stable_direction_pass": stable,
        })
    drivers.sort(key=lambda row: (row["stable_direction_pass"], row["stable_min_auc_distance"]), reverse=True)
    stable = [row for row in drivers if row["stable_direction_pass"]]
    data = {
        "schema_version": "tradex_new_core_driver_direction_v1.leaderboard.v1",
        "artifact_role": "authoritative_diagnostic",
        "axis": "observable feature direction at NEW_CORE close",
        "fixed_conditions": {
            "entry_family": "WEAK_REBOUND_MA20_REBREAK_CORE unchanged",
            "years": list(YEARS), "outcomes": "fixed3 h5 down_first versus rebound_first only",
            "driver_measure": "Mann-Whitney probability that down feature value exceeds rebound value",
            "stable": "AUC strictly on the same side of 0.5 in every year",
            "threshold_selection": "none",
        },
        "driver_leaderboard": drivers,
        "stable_driver_count": len(stable),
        "observed_branching": {
            "features_tested": len(FEATURES), "stable_direction_features": len(stable),
            "changed_rank_count": len(FEATURES),
            "selection_divergence_reason": "diagnostic ranking only; no event is selected or removed",
        },
        "judgment": {
            "decision": "hold" if stable else "drop",
            "next_single_axis_feature": None if not stable else stable[0]["feature"],
            "next_single_axis_direction": None if not stable else stable[0]["consistent_direction"],
            "reason": "stable direction exists for point-score testing" if stable else "no single observable feature has stable direction across all years",
        },
        "not_changed": ["entry events", "feature thresholds", "monthly environment", "position lifecycle", "MeeMee", "ranking", "runtime DB"],
    }
    leader = args.output / "driver_leaderboard.json"
    leader.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "compare.json").write_text(json.dumps({
        "schema_version": data["schema_version"], "artifact_role": "authoritative_pointer",
        "authoritative": "driver_leaderboard.json", "judgment": data["judgment"],
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "events": int(len(joined)), "features": len(FEATURES),
        "duplicates_removed": duplicates_removed, "duplicate_events": int(joined.duplicated(["code", "ymd"]).sum()),
        "missing_cells": int(joined[FEATURES].isna().sum().sum()), "future_used_for_selection": False,
        "review_only": True, "sequence_sha256": sha(args.sequence_ledger), "feature_sha256": sha(args.features),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({
        "complete": True, "authoritative": "driver_leaderboard.json", "sha256": sha(leader),
    }, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "stable": [
        {"feature": row["feature"], "direction": row["consistent_direction"], "strength": row["stable_min_auc_distance"]}
        for row in stable
    ], "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
