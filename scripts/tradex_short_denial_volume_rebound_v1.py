from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def metric(group: pd.DataFrame, denominator: int) -> dict:
    return {
        "n": int(len(group)), "codes": int(group["code"].nunique()),
        "retention_rate": float(len(group) / denominator),
        "upward_continuation_rate": float(group["post_rebound_upward"].mean()),
        "eventual_drop5_rate": float(group["post_rebound_drop5"].mean()),
        "unresolved_rate": float(group["post_rebound_unresolved"].mean()),
        "mean_close15_pct": float(group["post_rebound_close15_pct"].mean()),
        "mean_volume_ratio20": float(group["rebound_volume_ratio20"].mean()),
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
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "o", "h", "l", "c", "bar_index", "volume_ratio20"])
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
            entry_pos = rebound_pos + 1
            outcome_end = entry_pos + 14
            if outcome_end >= len(bars):
                continue
            ratio = float(bars.iloc[rebound_pos]["volume_ratio20"])
            if not np.isfinite(ratio):
                continue
            entry_open = float(bars.iloc[entry_pos]["o"])
            future = bars.iloc[entry_pos : outcome_end + 1]
            drop5 = bool(float(future["l"].min()) <= entry_open * 0.95)
            close15_pct = 100 * (float(future.iloc[-1]["c"]) / entry_open - 1)
            upward = bool(not drop5 and close15_pct >= 3)
            rows.append({
                **event._asdict(), "rebound_volume_ratio20": ratio,
                "entry_ymd": int(bars.iloc[entry_pos]["ymd"]),
                "post_rebound_entry_open": entry_open,
                "post_rebound_close15_pct": close15_pct,
                "post_rebound_drop5": drop5,
                "post_rebound_upward": upward,
                "post_rebound_unresolved": bool(not drop5 and not upward),
            })
    ledger = pd.DataFrame(rows)
    if ledger.empty:
        raise RuntimeError("no eligible events")
    development = ledger[ledger["period"].eq("development")]
    quantiles = development["rebound_volume_ratio20"].quantile([0.25, 0.50, 0.75]).to_dict()
    edges = [-float("inf"), quantiles[0.25], quantiles[0.50], quantiles[0.75], float("inf")]
    labels = ["出来高倍率_低位25%", "出来高倍率_中低位", "出来高倍率_中高位", "出来高倍率_高位25%"]
    ledger["volume_band"] = pd.cut(ledger["rebound_volume_ratio20"], bins=edges, labels=labels, include_lowest=True)
    baseline_rows, band_rows, yearly_rows = [], [], []
    for period, group in ledger.groupby("period"):
        baseline_rows.append({"period": period, **metric(group, len(group))})
        for band, band_group in group.groupby("volume_band", observed=True):
            band_rows.append({"period": period, "volume_band": str(band), **metric(band_group, len(group))})
    for year, group in ledger.assign(year=ledger["signal_ymd"] // 10000).groupby("year"):
        base = metric(group, len(group))
        for band, band_group in group.groupby("volume_band", observed=True):
            row = {"year": int(year), "volume_band": str(band), **metric(band_group, len(group))}
            row["upward_lift_vs_year"] = row["upward_continuation_rate"] - base["upward_continuation_rate"]
            row["drop_reduction_vs_year"] = base["eventual_drop5_rate"] - row["eventual_drop5_rate"]
            yearly_rows.append(row)
    baseline, bands, yearly = pd.DataFrame(baseline_rows), pd.DataFrame(band_rows), pd.DataFrame(yearly_rows)
    dev_base = baseline[baseline["period"].eq("development")].iloc[0]
    dev_bands = bands[bands["period"].eq("development")].copy()
    dev_bands["score"] = dev_bands["upward_continuation_rate"] - dev_base["upward_continuation_rate"] + dev_base["eventual_drop5_rate"] - dev_bands["eventual_drop5_rate"]
    eligible = dev_bands[
        dev_bands["upward_continuation_rate"].gt(dev_base["upward_continuation_rate"])
        & dev_bands["eventual_drop5_rate"].lt(dev_base["eventual_drop5_rate"])
        & dev_bands["retention_rate"].ge(0.20)
    ]
    selected = None if eligible.empty else str(eligible.sort_values("score", ascending=False).iloc[0]["volume_band"])
    val_base = baseline[baseline["period"].eq("validation")].iloc[0]
    val = bands[bands["period"].eq("validation") & bands["volume_band"].eq(selected)]
    val_years = yearly[yearly["year"].between(2024, 2026) & yearly["volume_band"].eq(selected)]
    checks = {
        "selected_on_development_only": selected is not None,
        "validation_retention_ge20": bool(len(val) == 1 and val.iloc[0]["retention_rate"] >= 0.20),
        "validation_upward_lift_ge5pp": bool(len(val) == 1 and val.iloc[0]["upward_continuation_rate"] >= val_base["upward_continuation_rate"] + 0.05),
        "validation_drop_reduction_ge5pp": bool(len(val) == 1 and val.iloc[0]["eventual_drop5_rate"] <= val_base["eventual_drop5_rate"] - 0.05),
        "all_validation_years_same_direction": bool(len(val_years) == 3 and val_years["upward_lift_vs_year"].gt(0).all() and val_years["drop_reduction_vs_year"].gt(0).all()),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_denial_volume_rebound_v1.compare.v1",
        "artifact_role": "authoritative_short_denial_volume_rebound", "review_only": True,
        "fixed_conditions": {
            "population": "Core signal denied by +3% rebound before -5% drop",
            "changed_axis": "rebound-session volume ratio versus trailing 20-session average only",
            "bands": "development quartiles fixed before validation", "entry_time": "rebound following open",
            "outcome_horizon": 15, "upward": "no -5% low and session-15 close >= +3%",
            "decline": "15-session low <= -5%", "development": "2019-2023", "validation": "2024-2026",
        },
        "authoritative_result": {"development_quantiles": {str(k): float(v) for k, v in quantiles.items()}, "selected_band": selected, "baseline": baseline.to_dict("records"), "bands": bands.to_dict("records"), "validation_years": val_years.to_dict("records"), "gate_checks": checks},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": int(len(ledger[ledger["volume_band"].eq(selected)])) if selected else 0, "selection_divergence_reason": "development-fixed volume band split only"},
        "judgment": {"candidate_local_decision": "keep" if keep else "drop", "session_aggregate_decision": "keep_volume_rebound" if keep else "drop_volume_rebound", "authoritative_rollup_decision": "keep_short_denial_volume_rebound_v1_review_only" if keep else "drop_short_denial_volume_rebound_v1", "reason_type": "development_selected_validation_lift_drop_retention_year_gates"},
        "not_changed": ["価格形状との結合", "売りシグナル", "売り候補", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "volume_rebound_ledger.parquet", index=False)
    baseline.to_parquet(output / "volume_rebound_baseline.parquet", index=False)
    bands.to_parquet(output / "volume_rebound_band_metrics.parquet", index=False)
    yearly.to_parquet(output / "volume_rebound_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
