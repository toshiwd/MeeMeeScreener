from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import RobustScaler


MARKET_FEATURES = ["market_mean_ret1", "market_mean_ret5", "market_breadth_ma20"]


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
    parser.add_argument("--states", required=True)
    parser.add_argument("--daily", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    ledger = pd.read_parquet(args.states).copy()
    daily = pd.read_parquet(args.daily, columns=["ymd", "ret1", "ret5", "close_vs_ma20"])
    market = daily.groupby("ymd", as_index=False).agg(
        market_mean_ret1=("ret1", "mean"),
        market_mean_ret5=("ret5", "mean"),
        market_breadth_ma20=("close_vs_ma20", lambda values: float(values.gt(0).mean())),
        market_names=("ret1", "count"),
    ).sort_values("ymd").reset_index(drop=True)
    market[MARKET_FEATURES] = market[MARKET_FEATURES].replace([np.inf, -np.inf], np.nan)
    market[MARKET_FEATURES] = market[MARKET_FEATURES].fillna(market[MARKET_FEATURES].median())
    dev_market = market[market["ymd"].between(20190101, 20231231)]
    scaler = RobustScaler(quantile_range=(10, 90))
    dev_scaled = np.clip(scaler.fit_transform(dev_market[MARKET_FEATURES]), -5, 5)
    model = KMeans(n_clusters=3, random_state=20260719, n_init=20).fit(dev_scaled)
    all_scaled = np.clip(scaler.transform(market[MARKET_FEATURES]), -5, 5)
    market["market_cluster"] = model.predict(all_scaled)
    centroids = pd.DataFrame(scaler.inverse_transform(model.cluster_centers_), columns=MARKET_FEATURES)
    centroids["strength_score"] = (
        centroids["market_mean_ret1"] + centroids["market_mean_ret5"]
        + centroids["market_breadth_ma20"]
    )
    ordered = centroids["strength_score"].sort_values().index.tolist()
    regime_map = {int(ordered[0]): "弱い市場", int(ordered[1]): "中立市場", int(ordered[2]): "強い市場"}
    market["market_regime"] = market["market_cluster"].map(regime_map)
    centroids["market_cluster"] = centroids.index
    centroids["market_regime"] = centroids["market_cluster"].map(regime_map)

    dates = market["ymd"].tolist()
    next_date = {dates[index]: dates[index + 1] for index in range(len(dates) - 1)}
    ledger["observation_ymd"] = ledger["rebound_ymd"].map(next_date)
    before = len(ledger)
    ledger = ledger.merge(
        market[["ymd", "market_regime", *MARKET_FEATURES, "market_names"]].rename(columns={"ymd": "observation_ymd"}),
        on="observation_ymd", how="left", validate="many_to_one",
    )
    missing = int(ledger["market_regime"].isna().sum())

    regime_rows, cell_rows, yearly_rows = [], [], []
    for (period, regime), group in ledger.groupby(["period", "market_regime"]):
        regime_rows.append({"period": period, "market_regime": regime, **metric(group, len(ledger[ledger["period"].eq(period)]))})
        for family, family_group in group.groupby("state_family"):
            cell_rows.append({"period": period, "market_regime": regime, "state_family": family, **metric(family_group, len(group))})
    for (year, regime), group in ledger.assign(year=ledger["signal_ymd"] // 10000).groupby(["year", "market_regime"]):
        base = metric(group, len(group))
        for family, family_group in group.groupby("state_family"):
            row = {"year": int(year), "market_regime": regime, "state_family": family, **metric(family_group, len(group))}
            row["upward_lift_vs_regime_year"] = row["upward_continuation_rate"] - base["upward_continuation_rate"]
            row["drop_reduction_vs_regime_year"] = base["eventual_drop5_rate"] - row["eventual_drop5_rate"]
            yearly_rows.append(row)
    regimes, cells, yearly = pd.DataFrame(regime_rows), pd.DataFrame(cell_rows), pd.DataFrame(yearly_rows)
    dev_cells = cells[cells["period"].eq("development")].set_index(["market_regime", "state_family"])
    val_cells = cells[cells["period"].eq("validation")].set_index(["market_regime", "state_family"])
    dev_regimes = regimes[regimes["period"].eq("development")].set_index("market_regime")
    val_regimes = regimes[regimes["period"].eq("validation")].set_index("market_regime")
    decisions = []
    for key, dev_row in dev_cells.iterrows():
        regime, family = key
        if key not in val_cells.index:
            continue
        val_row = val_cells.loc[key]
        dev_base, val_base = dev_regimes.loc[regime], val_regimes.loc[regime]
        dev_up = dev_row["upward_continuation_rate"] - dev_base["upward_continuation_rate"]
        dev_drop = dev_base["eventual_drop5_rate"] - dev_row["eventual_drop5_rate"]
        val_up = val_row["upward_continuation_rate"] - val_base["upward_continuation_rate"]
        val_drop = val_base["eventual_drop5_rate"] - val_row["eventual_drop5_rate"]
        years = yearly[
            yearly["year"].between(2024, 2026)
            & yearly["market_regime"].eq(regime)
            & yearly["state_family"].eq(family)
        ]
        positive_years = int((years["upward_lift_vs_regime_year"].gt(0) & years["drop_reduction_vs_regime_year"].gt(0)).sum())
        negative_years = int((years["upward_lift_vs_regime_year"].lt(0) & years["drop_reduction_vs_regime_year"].lt(0)).sum())
        if dev_row["n"] >= 200 and val_row["n"] >= 100 and dev_up > 0 and dev_drop > 0 and val_up > 0 and val_drop > 0 and positive_years >= 2:
            role = "買い優位"
        elif dev_row["n"] >= 200 and val_row["n"] >= 100 and dev_up < 0 and dev_drop < 0 and val_up < 0 and val_drop < 0 and negative_years >= 2:
            role = "売り再監視"
        else:
            role = "混合・保留"
        decisions.append({
            "market_regime": regime, "state_family": family,
            "development_n": int(dev_row["n"]), "validation_n": int(val_row["n"]),
            "development_upward_lift": float(dev_up), "development_drop_reduction": float(dev_drop),
            "validation_upward_lift": float(val_up), "validation_drop_reduction": float(val_drop),
            "positive_validation_years": positive_years, "negative_validation_years": negative_years,
            "practical_role": role,
        })
    decision_table = pd.DataFrame(decisions)
    stable_buy = decision_table[decision_table["practical_role"].eq("買い優位")]
    stable_sell = decision_table[decision_table["practical_role"].eq("売り再監視")]
    checks = {
        "all_state_rows_retained": len(ledger) == before,
        "market_join_missing_zero": missing == 0,
        "three_regimes_both_periods": int(regimes.groupby("period")["market_regime"].nunique().min()) == 3,
        "all_18_cells_both_periods": int(cells.groupby("period").size().min()) == 18,
        "stable_buy_cell_exists": len(stable_buy) > 0,
        "stable_sell_cell_exists": len(stable_sell) > 0,
    }
    keep = checks["all_state_rows_retained"] and checks["market_join_missing_zero"] and checks["three_regimes_both_periods"] and checks["all_18_cells_both_periods"] and (checks["stable_buy_cell_exists"] or checks["stable_sell_cell_exists"])
    result = {
        "schema_version": "tradex_short_denial_composite_market_regime_v1.compare.v1",
        "artifact_role": "authoritative_short_denial_composite_market_regime", "review_only": True,
        "fixed_conditions": {
            "state_families": 6, "market_regimes": 3,
            "market_features": MARKET_FEATURES, "market_fit": "development dates only, outcome blind KMeans",
            "regime_observation": "session after rebound, before following-open entry",
            "cell_count": 18, "all_events_retained": True,
            "development": "2019-2023", "validation": "2024-2026",
        },
        "authoritative_result": {
            "market_centroids": json.loads(centroids.to_json(orient="records", force_ascii=False)),
            "regimes": json.loads(regimes.to_json(orient="records", force_ascii=False)),
            "cells": json.loads(cells.to_json(orient="records", force_ascii=False)),
            "decision_table": json.loads(decision_table.to_json(orient="records", force_ascii=False)),
            "stable_buy_cells": json.loads(stable_buy.to_json(orient="records", force_ascii=False)),
            "stable_sell_cells": json.loads(stable_sell.to_json(orient="records", force_ascii=False)),
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int(len(ledger)),
            "selection_divergence_reason": "six fixed state families conditioned by three outcome-blind market regimes; no exclusions",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_market_conditioned_state_table" if keep else "hold_market_conditioned_state_table",
            "authoritative_rollup_decision": "keep_short_denial_composite_market_regime_v1_review_only" if keep else "hold_short_denial_composite_market_regime_v1",
            "reason_type": "full_retention_regime_cell_development_validation_year_consistency",
        },
        "not_changed": ["単独軸スクリーニング", "候補除外", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "market_conditioned_state_ledger.parquet", index=False)
    market.to_parquet(output / "market_regime_daily.parquet", index=False)
    regimes.to_parquet(output / "market_regime_metrics.parquet", index=False)
    cells.to_parquet(output / "market_conditioned_state_metrics.parquet", index=False)
    yearly.to_parquet(output / "market_conditioned_state_yearly_metrics.parquet", index=False)
    decision_table.to_parquet(output / "market_conditioned_decision_table.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
