from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler


FEATURES = [
    "rebound_close_vs_ma20", "rebound_volume_ratio20", "rebound_body_ratio",
    "rebound_close_position", "rebound_upper_wick_ratio", "rebound_lower_wick_ratio",
    "rebound_ret1", "rebound_ret5", "rebound_dist_high20", "next_ret1",
    "next_higher_low", "next_higher_high", "next_close_above_rebound_high",
    "next_close_position", "next_day_resignal",
]


def metric(group: pd.DataFrame, denominator: int) -> dict:
    return {
        "n": int(len(group)), "codes": int(group["code"].nunique()),
        "retention_rate": float(len(group) / denominator),
        "upward_continuation_rate": float(group["post_observation_upward"].mean()),
        "eventual_drop5_rate": float(group["post_observation_drop5"].mean()),
        "unresolved_rate": float(group["post_observation_unresolved"].mean()),
        "mean_close15_pct": float(group["post_observation_close15_pct"].mean()),
    }


def assign_feature_labels(centroids: pd.DataFrame) -> dict[int, str]:
    remaining = set(int(index) for index in centroids.index)
    labels: dict[int, str] = {}
    buy_score = (
        centroids["next_ret1"] + centroids["next_higher_low"]
        + centroids["next_close_above_rebound_high"] + centroids["next_close_position"]
        + centroids["rebound_close_vs_ma20"] - centroids["rebound_upper_wick_ratio"]
    )
    buy = int(buy_score.idxmax()); labels[buy] = "買い加速型"; remaining.remove(buy)
    absorb_score = centroids["next_day_resignal"] + centroids["next_ret1"] + centroids["next_higher_low"]
    absorb = int(absorb_score.loc[list(remaining)].idxmax()); labels[absorb] = "売り吸収型"; remaining.remove(absorb)
    sell_score = (
        -centroids["next_ret1"] - centroids["next_higher_low"]
        + centroids["next_day_resignal"] + centroids["rebound_upper_wick_ratio"]
        - centroids["next_close_position"]
    )
    sell = int(sell_score.loc[list(remaining)].idxmax()); labels[sell] = "下げ直し警戒型"; remaining.remove(sell)
    volume = int(centroids.loc[list(remaining), "rebound_volume_ratio20"].idxmax())
    labels[volume] = "出来高主導型"; remaining.remove(volume)
    high = int(centroids.loc[list(remaining), "rebound_close_vs_ma20"].idxmax())
    labels[high] = "高位置反発型"; remaining.remove(high)
    labels[int(next(iter(remaining)))] = "混合反発型"
    return labels


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", required=True)
    parser.add_argument("--daily", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    paths = pd.read_parquet(args.paths).copy()
    daily_columns = [
        "code", "ymd", "o", "h", "l", "c", "bar_index", "close_vs_ma20",
        "volume_ratio20", "body_ratio", "close_position", "upper_wick_ratio",
        "lower_wick_ratio", "ret1", "ret5", "dist_high20",
    ]
    daily = pd.read_parquet(args.daily, columns=daily_columns)
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
            next_pos, entry_pos, end_pos = rebound_pos + 1, rebound_pos + 2, rebound_pos + 16
            if end_pos >= len(bars):
                continue
            rebound, next_bar = bars.iloc[rebound_pos], bars.iloc[next_pos]
            entry_open = float(bars.iloc[entry_pos]["o"])
            future = bars.iloc[entry_pos : end_pos + 1]
            drop5 = bool(float(future["l"].min()) <= entry_open * 0.95)
            close15_pct = 100 * (float(future.iloc[-1]["c"]) / entry_open - 1)
            upward = bool(not drop5 and close15_pct >= 3)
            rows.append({
                **event._asdict(),
                "rebound_close_vs_ma20": float(rebound["close_vs_ma20"]),
                "rebound_volume_ratio20": float(rebound["volume_ratio20"]),
                "rebound_body_ratio": float(rebound["body_ratio"]),
                "rebound_close_position": float(rebound["close_position"]),
                "rebound_upper_wick_ratio": float(rebound["upper_wick_ratio"]),
                "rebound_lower_wick_ratio": float(rebound["lower_wick_ratio"]),
                "rebound_ret1": float(rebound["ret1"]), "rebound_ret5": float(rebound["ret5"]),
                "rebound_dist_high20": float(rebound["dist_high20"]),
                "next_ret1": float(next_bar["c"] / rebound["c"] - 1),
                "next_higher_low": float(next_bar["l"] > rebound["l"]),
                "next_higher_high": float(next_bar["h"] > rebound["h"]),
                "next_close_above_rebound_high": float(next_bar["c"] > rebound["h"]),
                "next_close_position": float((next_bar["c"] - next_bar["l"]) / max(next_bar["h"] - next_bar["l"], 1e-9)),
                "next_day_resignal": float(bool(event.core_probe_resignal and event.core_probe_resignal_days == 1)),
                "entry_ymd": int(bars.iloc[entry_pos]["ymd"]),
                "post_observation_entry_open": entry_open,
                "post_observation_close15_pct": close15_pct,
                "post_observation_drop5": drop5, "post_observation_upward": upward,
                "post_observation_unresolved": bool(not drop5 and not upward),
            })
    ledger = pd.DataFrame(rows)
    if ledger.empty:
        raise RuntimeError("no eligible events")
    development_mask = ledger["period"].eq("development")
    imputer = SimpleImputer(strategy="median")
    scaler = RobustScaler(quantile_range=(10, 90))
    dev_imputed = imputer.fit_transform(ledger.loc[development_mask, FEATURES])
    dev_scaled = np.clip(scaler.fit_transform(dev_imputed), -5, 5)
    model = KMeans(n_clusters=6, random_state=20260719, n_init=20)
    model.fit(dev_scaled)
    all_scaled = np.clip(scaler.transform(imputer.transform(ledger[FEATURES])), -5, 5)
    ledger["cluster_id"] = model.predict(all_scaled)
    centroid_raw = pd.DataFrame(
        scaler.inverse_transform(model.cluster_centers_), columns=FEATURES
    )
    centroid_raw.index.name = "cluster_id"
    feature_labels = assign_feature_labels(centroid_raw)
    ledger["state_family"] = ledger["cluster_id"].map(feature_labels)

    baseline_rows, family_rows, yearly_rows = [], [], []
    for period, group in ledger.groupby("period"):
        baseline_rows.append({"period": period, **metric(group, len(group))})
        for family, family_group in group.groupby("state_family"):
            family_rows.append({"period": period, "state_family": family, **metric(family_group, len(group))})
    for year, group in ledger.assign(year=ledger["signal_ymd"] // 10000).groupby("year"):
        base = metric(group, len(group))
        for family, family_group in group.groupby("state_family"):
            row = {"year": int(year), "state_family": family, **metric(family_group, len(group))}
            row["upward_lift_vs_year"] = row["upward_continuation_rate"] - base["upward_continuation_rate"]
            row["drop_reduction_vs_year"] = base["eventual_drop5_rate"] - row["eventual_drop5_rate"]
            yearly_rows.append(row)
    baseline, families, yearly = pd.DataFrame(baseline_rows), pd.DataFrame(family_rows), pd.DataFrame(yearly_rows)
    dev_families = families[families["period"].eq("development")].copy()
    dev_families["action_score"] = dev_families["upward_continuation_rate"] - dev_families["eventual_drop5_rate"]
    ranked = dev_families.sort_values("action_score", ascending=False)
    buy_family = str(ranked.iloc[0]["state_family"])
    trial_family = str(ranked.iloc[1]["state_family"])
    sell_family = str(ranked.iloc[-1]["state_family"])
    roles = {family: "混合監視" for family in feature_labels.values()}
    roles[buy_family] = "買い本命候補"; roles[trial_family] = "試し玉候補"; roles[sell_family] = "売り再監視"
    ledger["operational_role"] = ledger["state_family"].map(roles)
    families["operational_role"] = families["state_family"].map(roles)
    yearly["operational_role"] = yearly["state_family"].map(roles)
    val_base = baseline[baseline["period"].eq("validation")].iloc[0]
    val_buy = families[families["period"].eq("validation") & families["state_family"].eq(buy_family)].iloc[0]
    buy_years = yearly[yearly["year"].between(2024, 2026) & yearly["state_family"].eq(buy_family)]
    checks = {
        "all_events_classified": int(ledger["state_family"].isna().sum()) == 0,
        "six_families_both_periods": int(families.groupby("period")["state_family"].nunique().min()) == 6,
        "roles_selected_on_development_only": True,
        "buy_family_validation_n_ge500": int(val_buy["n"]) >= 500,
        "buy_family_validation_upward_above_baseline": float(val_buy["upward_continuation_rate"]) > float(val_base["upward_continuation_rate"]),
        "buy_family_validation_drop_below_baseline": float(val_buy["eventual_drop5_rate"]) < float(val_base["eventual_drop5_rate"]),
        "buy_family_all_years_same_direction": bool(len(buy_years) == 3 and buy_years["upward_lift_vs_year"].gt(0).all() and buy_years["drop_reduction_vs_year"].gt(0).all()),
    }
    keep = all(checks.values())
    centroid_records = centroid_raw.reset_index().assign(state_family=lambda x: x["cluster_id"].map(feature_labels))
    result = {
        "schema_version": "tradex_short_denial_composite_state_family_v1.compare.v1",
        "artifact_role": "authoritative_short_denial_composite_state_family", "review_only": True,
        "fixed_conditions": {
            "population": "Core signal denied by +3% rebound before -5% drop",
            "classification": "six-cluster KMeans fit on development observable features only",
            "feature_count": len(FEATURES), "observation": "rebound session plus next session",
            "entry_time": "observation following open", "outcome_horizon": 15,
            "development": "2019-2023", "validation": "2024-2026", "all_events_retained": True,
        },
        "authoritative_result": {
            "buy_family": buy_family, "trial_family": trial_family, "sell_rewatch_family": sell_family,
            "role_map": roles, "baseline": json.loads(baseline.to_json(orient="records", force_ascii=False)),
            "families": json.loads(families.to_json(orient="records", force_ascii=False)),
            "buy_family_validation_years": json.loads(buy_years.to_json(orient="records", force_ascii=False)),
            "centroids": json.loads(centroid_records.to_json(orient="records", force_ascii=False)),
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int(len(ledger)),
            "selection_divergence_reason": "all rebound events assigned to feature-combination families; no hard screening",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_composite_state_family" if keep else "hold_composite_state_family",
            "authoritative_rollup_decision": "keep_short_denial_composite_state_family_v1_review_only" if keep else "hold_short_denial_composite_state_family_v1",
            "reason_type": "unsupervised_branching_validation_direction_and_year_gates",
        },
        "not_changed": ["売りシグナル", "候補除外", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "composite_state_ledger.parquet", index=False)
    baseline.to_parquet(output / "composite_state_baseline.parquet", index=False)
    families.to_parquet(output / "composite_state_family_metrics.parquet", index=False)
    yearly.to_parquet(output / "composite_state_yearly_metrics.parquet", index=False)
    centroid_records.to_parquet(output / "composite_state_centroids.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
