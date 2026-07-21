from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def metric(group: pd.DataFrame, denominator: int) -> dict:
    return {
        "n": int(len(group)), "codes": int(group["code"].nunique()),
        "retention_rate": float(len(group) / denominator),
        "upward_continuation_rate": float(group["post_observation_upward"].mean()),
        "eventual_drop5_rate": float(group["post_observation_drop5"].mean()),
        "unresolved_rate": float(group["post_observation_unresolved"].mean()),
        "mean_close15_pct": float(group["post_observation_close15_pct"].mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", required=True)
    parser.add_argument("--daily", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    paths = pd.read_parquet(args.paths).copy()
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "o", "h", "l", "c", "bar_index"])
    paths["code"] = paths["code"].astype(str).str.zfill(4)
    daily["code"] = daily["code"].astype(str).str.zfill(4)
    rows = []
    for code, events in paths.groupby("code", sort=False):
        bars = daily[daily["code"].eq(code)].sort_values("bar_index").reset_index(drop=True)
        if bars.empty:
            continue
        ymd_to_pos = pd.Series(bars.index.values, index=bars["ymd"]).to_dict()
        for event in events.itertuples():
            if event.rebound_ymd not in ymd_to_pos:
                continue
            rebound_pos = int(ymd_to_pos[event.rebound_ymd])
            observation_start = rebound_pos + 1
            observation_end = rebound_pos + 5
            entry_pos = rebound_pos + 6
            outcome_end = entry_pos + 14
            if outcome_end >= len(bars):
                continue
            rebound_high = float(bars.iloc[rebound_pos]["h"])
            observation = bars.iloc[observation_start : observation_end + 1]
            close_update = bool(observation["c"].gt(rebound_high).any())
            high_update = bool(observation["h"].gt(rebound_high).any())
            state = "終値更新" if close_update else "日中高値のみ更新" if high_update else "更新なし"
            entry_open = float(bars.iloc[entry_pos]["o"])
            future = bars.iloc[entry_pos : outcome_end + 1]
            drop5 = bool(float(future["l"].min()) <= entry_open * 0.95)
            close15_pct = 100 * (float(future.iloc[-1]["c"]) / entry_open - 1)
            upward = bool(not drop5 and close15_pct >= 3)
            rows.append({
                **event._asdict(), "rebound_high": rebound_high,
                "observation_end_ymd": int(bars.iloc[observation_end]["ymd"]),
                "entry_ymd": int(bars.iloc[entry_pos]["ymd"]), "high_update_state": state,
                "post_observation_entry_open": entry_open,
                "post_observation_close15_pct": close15_pct,
                "post_observation_drop5": drop5,
                "post_observation_upward": upward,
                "post_observation_unresolved": bool(not drop5 and not upward),
            })
    ledger = pd.DataFrame(rows)
    if ledger.empty:
        raise RuntimeError("no eligible events")
    baseline_rows, state_rows, yearly_rows = [], [], []
    for period, group in ledger.groupby("period"):
        baseline_rows.append({"period": period, **metric(group, len(group))})
        for state, state_group in group.groupby("high_update_state"):
            state_rows.append({"period": period, "state": state, **metric(state_group, len(group))})
    for year, group in ledger.assign(year=ledger["signal_ymd"] // 10000).groupby("year"):
        base = metric(group, len(group))
        for state, state_group in group.groupby("high_update_state"):
            row = {"year": int(year), "state": state, **metric(state_group, len(group))}
            row["upward_lift_vs_year"] = row["upward_continuation_rate"] - base["upward_continuation_rate"]
            row["drop_reduction_vs_year"] = base["eventual_drop5_rate"] - row["eventual_drop5_rate"]
            yearly_rows.append(row)
    baseline, states, yearly = pd.DataFrame(baseline_rows), pd.DataFrame(state_rows), pd.DataFrame(yearly_rows)
    dev_base = baseline[baseline["period"].eq("development")].iloc[0]
    dev_states = states[states["period"].eq("development")].copy()
    dev_states["score"] = dev_states["upward_continuation_rate"] - dev_base["upward_continuation_rate"] + dev_base["eventual_drop5_rate"] - dev_states["eventual_drop5_rate"]
    eligible = dev_states[
        dev_states["upward_continuation_rate"].gt(dev_base["upward_continuation_rate"])
        & dev_states["eventual_drop5_rate"].lt(dev_base["eventual_drop5_rate"])
        & dev_states["retention_rate"].ge(0.20)
    ]
    selected = None if eligible.empty else str(eligible.sort_values("score", ascending=False).iloc[0]["state"])
    val_base = baseline[baseline["period"].eq("validation")].iloc[0]
    val = states[states["period"].eq("validation") & states["state"].eq(selected)]
    val_years = yearly[yearly["year"].between(2024, 2026) & yearly["state"].eq(selected)]
    checks = {
        "selected_on_development_only": selected is not None,
        "validation_retention_ge20": bool(len(val) == 1 and val.iloc[0]["retention_rate"] >= 0.20),
        "validation_upward_lift_ge5pp": bool(len(val) == 1 and val.iloc[0]["upward_continuation_rate"] >= val_base["upward_continuation_rate"] + 0.05),
        "validation_drop_reduction_ge5pp": bool(len(val) == 1 and val.iloc[0]["eventual_drop5_rate"] <= val_base["eventual_drop5_rate"] - 0.05),
        "all_validation_years_same_direction": bool(len(val_years) == 3 and val_years["upward_lift_vs_year"].gt(0).all() and val_years["drop_reduction_vs_year"].gt(0).all()),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_denial_rebound_high_update_v1.compare.v1",
        "artifact_role": "authoritative_short_denial_rebound_high_update", "review_only": True,
        "fixed_conditions": {
            "population": "Core signal denied by +3% rebound before -5% drop",
            "changed_axis": "five-session relation to rebound-session high only",
            "states": ["終値更新", "日中高値のみ更新", "更新なし"],
            "observation_horizon": 5, "entry_time": "observation-end following open",
            "outcome_horizon": 15, "upward": "no -5% low and session-15 close >= +3%",
            "decline": "15-session low <= -5%", "development": "2019-2023", "validation": "2024-2026",
        },
        "authoritative_result": {"selected_state": selected, "baseline": baseline.to_dict("records"), "states": states.to_dict("records"), "validation_years": val_years.to_dict("records"), "gate_checks": checks},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": int(len(ledger[ledger["high_update_state"].eq(selected)])) if selected else 0, "selection_divergence_reason": "fixed observation state split only"},
        "judgment": {"candidate_local_decision": "keep" if keep else "drop", "session_aggregate_decision": "keep_rebound_high_update" if keep else "drop_rebound_high_update", "authoritative_rollup_decision": "keep_short_denial_rebound_high_update_v1_review_only" if keep else "drop_short_denial_rebound_high_update_v1", "reason_type": "development_selected_validation_lift_drop_retention_year_gates"},
        "not_changed": ["売りシグナル", "売り候補", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "rebound_high_update_ledger.parquet", index=False)
    baseline.to_parquet(output / "rebound_high_update_baseline.parquet", index=False)
    states.to_parquet(output / "rebound_high_update_state_metrics.parquet", index=False)
    yearly.to_parquet(output / "rebound_high_update_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
