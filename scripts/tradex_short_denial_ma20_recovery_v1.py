from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def metric(group: pd.DataFrame, denominator: int) -> dict:
    return {
        "n": int(len(group)),
        "codes": int(group["code"].nunique()),
        "retention_rate": float(len(group) / denominator),
        "upward_continuation_rate": float(group["upward_continuation"].mean()),
        "eventual_drop5_rate": float(group["eventual_drop5"].mean()),
        "unresolved_rate": float(group["unresolved"].mean()),
        "mean_close_vs_ma20": float(group["rebound_close_vs_ma20"].mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", required=True)
    parser.add_argument("--daily", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)

    paths = pd.read_parquet(args.paths)
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "close_vs_ma20"])
    paths["code"] = paths["code"].astype(str).str.zfill(4)
    daily["code"] = daily["code"].astype(str).str.zfill(4)
    daily = daily.rename(columns={"ymd": "rebound_ymd", "close_vs_ma20": "rebound_close_vs_ma20"})
    frame = paths.merge(daily, on=["code", "rebound_ymd"], how="left", validate="many_to_one")
    missing = int(frame["rebound_close_vs_ma20"].isna().sum())
    frame = frame.dropna(subset=["rebound_close_vs_ma20"]).copy()

    development = frame[frame["period"].eq("development")]
    quantiles = development["rebound_close_vs_ma20"].quantile([0.25, 0.50, 0.75]).to_dict()
    edges = [-float("inf"), quantiles[0.25], quantiles[0.50], quantiles[0.75], float("inf")]
    labels = ["20MA距離_低位25%", "20MA距離_中低位", "20MA距離_中高位", "20MA距離_高位25%"]
    frame["ma20_band"] = pd.cut(frame["rebound_close_vs_ma20"], bins=edges, labels=labels, include_lowest=True)

    baseline_rows = []
    band_rows = []
    yearly_rows = []
    for period, group in frame.groupby("period"):
        baseline_rows.append({"period": period, **metric(group, len(group))})
        for band, band_group in group.groupby("ma20_band", observed=True):
            band_rows.append({"period": period, "ma20_band": str(band), **metric(band_group, len(group))})
    for year, group in frame.assign(year=frame["signal_ymd"] // 10000).groupby("year"):
        baseline = metric(group, len(group))
        for band, band_group in group.groupby("ma20_band", observed=True):
            row = {"year": int(year), "ma20_band": str(band), **metric(band_group, len(group))}
            row["upward_lift_vs_year"] = row["upward_continuation_rate"] - baseline["upward_continuation_rate"]
            row["drop_reduction_vs_year"] = baseline["eventual_drop5_rate"] - row["eventual_drop5_rate"]
            yearly_rows.append(row)
    baseline = pd.DataFrame(baseline_rows)
    bands = pd.DataFrame(band_rows)
    yearly = pd.DataFrame(yearly_rows)

    dev_base = baseline[baseline["period"].eq("development")].iloc[0]
    dev_bands = bands[bands["period"].eq("development")].copy()
    dev_bands["development_score"] = (
        dev_bands["upward_continuation_rate"] - dev_base["upward_continuation_rate"]
        + dev_base["eventual_drop5_rate"] - dev_bands["eventual_drop5_rate"]
    )
    eligible = dev_bands[
        dev_bands["upward_continuation_rate"].gt(dev_base["upward_continuation_rate"])
        & dev_bands["eventual_drop5_rate"].lt(dev_base["eventual_drop5_rate"])
        & dev_bands["retention_rate"].ge(0.20)
    ]
    selected = None if eligible.empty else str(eligible.sort_values("development_score", ascending=False).iloc[0]["ma20_band"])
    val_base = baseline[baseline["period"].eq("validation")].iloc[0]
    val = bands[bands["period"].eq("validation") & bands["ma20_band"].eq(selected)]
    validation_years = yearly[yearly["year"].between(2024, 2026) & yearly["ma20_band"].eq(selected)]
    checks = {
        "join_missing_zero": missing == 0,
        "selected_on_development_only": selected is not None,
        "validation_retention_ge20": bool(len(val) == 1 and val.iloc[0]["retention_rate"] >= 0.20),
        "validation_upward_lift_ge5pp": bool(len(val) == 1 and val.iloc[0]["upward_continuation_rate"] >= val_base["upward_continuation_rate"] + 0.05),
        "validation_drop_reduction_ge5pp": bool(len(val) == 1 and val.iloc[0]["eventual_drop5_rate"] <= val_base["eventual_drop5_rate"] - 0.05),
        "all_validation_years_same_direction": bool(len(validation_years) == 3 and validation_years["upward_lift_vs_year"].gt(0).all() and validation_years["drop_reduction_vs_year"].gt(0).all()),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_denial_ma20_recovery_v1.compare.v1",
        "artifact_role": "authoritative_short_denial_ma20_recovery",
        "review_only": True,
        "fixed_conditions": {
            "population": "Core signal denied by +3% rebound before -5% drop within 5 sessions",
            "changed_axis": "rebound-day close distance from MA20 only",
            "bands": "development quartiles fixed before validation", "development": "2019-2023",
            "validation": "2024-2026", "minimum_retention": 0.20,
            "required_upward_lift": 0.05, "required_drop_reduction": 0.05,
        },
        "authoritative_result": {
            "development_quantiles": {str(k): float(v) for k, v in quantiles.items()},
            "selected_band": selected, "baseline": baseline.to_dict("records"),
            "bands": bands.to_dict("records"), "validation_years": validation_years.to_dict("records"),
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int(len(frame[frame["ma20_band"].eq(selected)])) if selected else 0,
            "selection_divergence_reason": "rebound population split by development-fixed MA20 distance band",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": "keep_ma20_recovery" if keep else "drop_ma20_recovery",
            "authoritative_rollup_decision": "keep_short_denial_ma20_recovery_v1_review_only" if keep else "drop_short_denial_ma20_recovery_v1",
            "reason_type": "development_selected_validation_lift_drop_retention_year_gates",
        },
        "not_changed": ["売りシグナル", "売り候補", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    frame.to_parquet(output / "ma20_recovery_ledger.parquet", index=False)
    baseline.to_parquet(output / "ma20_recovery_baseline.parquet", index=False)
    bands.to_parquet(output / "ma20_recovery_band_metrics.parquet", index=False)
    yearly.to_parquet(output / "ma20_recovery_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
