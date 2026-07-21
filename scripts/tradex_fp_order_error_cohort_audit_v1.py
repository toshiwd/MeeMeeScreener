from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


FEATURES = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
LABELS = Path(r"G:\Tradex\fp_order_v1\20260714T075050Z-tradex_nikkei225_first_passage_order_v1\first_passage_label_ledger.parquet")
PROBS = Path(r"G:\Tradex\fp_order_v1\20260714T075050Z-tradex_nikkei225_first_passage_order_v1\probability_ledger_2026.parquet")
OUT = Path(r"G:\Tradex\fp_order_error_cohort_audit_v1\20260714T-error-cohort-audit-v1")


def b3(x: pd.Series, cuts: list[float], names: list[str]) -> pd.Series:
    return pd.cut(x, [-np.inf, *cuts, np.inf], labels=names, ordered=True)


def summarize(df: pd.DataFrame, col: str) -> list[dict]:
    z = df[df.label.isin(["down_first", "rebound_first"])].copy()
    rows = []
    for (h, val), g in z.groupby(["horizon", col], observed=True):
        n = len(g)
        nd = int((g.label == "down_first").sum())
        nr = int((g.label == "rebound_first").sum())
        rows.append({"horizon": int(h), "axis": col, "cohort": str(val), "n_directional": n,
                     "down_n": nd, "rebound_n": nr, "down_share": nd / n,
                     "down_to_rebound_odds": nd / nr if nr else None})
    return rows


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cols = ["code", "ymd", "c", "atr14", "body_ratio", "upper_wick_ratio", "lower_wick_ratio",
            "close_pos", "ret10", "pos20", "range20_pct", "bear_count5", "upper_supply_count5",
            "lower_rejection_count5", "dist_ma20_atr", "dist_ma60_atr", "ma20_slope5_atr",
            "ma60_slope5_atr", "volume_ratio20", "cross_ma20", "reclaim_ma20", "support_break",
            "support_break_depth_atr", "oversold_risk"]
    f = pd.read_parquet(FEATURES, columns=cols).sort_values(["code", "ymd"])
    f["ret20"] = f.groupby("code", observed=True).c.pct_change(20, fill_method=None)
    f["ma20_position"] = b3(f.dist_ma20_atr, [-0.5, 0.5], ["below", "near", "above"])
    f["ma60_position"] = b3(f.dist_ma60_atr, [-0.5, 0.5], ["below", "near", "above"])
    f["ret20_state"] = b3(f.ret20, [-0.05, 0.05], ["down", "flat", "up"])
    f["candle_shape"] = np.select(
        [f.upper_wick_ratio >= 0.4, f.lower_wick_ratio >= 0.4, f.body_ratio >= 0.6],
        ["upper_wick", "lower_wick", "large_body"], default="small_body")
    f["sideways"] = np.select(
        [(f.ret20.abs() <= 0.03) & (f.ma20_slope5_atr.abs() <= 0.08),
         (f.ret20.abs() >= 0.08)], ["sideways", "trending"], default="mixed")
    f["support_state"] = np.select(
        [f.support_break.eq(1), f.pos20 <= 0.15, f.pos20 >= 0.85],
        ["broken", "near_support", "near_resistance"], default="midrange")
    f["oversold_state"] = np.where(f.oversold_risk.eq(1), "oversold", "not_oversold")
    f["supply_rejection"] = np.select(
        [(f.upper_supply_count5 >= 2) & (f.lower_rejection_count5 == 0),
         (f.lower_rejection_count5 >= 2) & (f.upper_supply_count5 == 0)],
        ["upper_supply", "lower_rejection"], default="mixed_none")

    lab = pd.read_parquet(LABELS, columns=["code", "ymd", "horizon", "label", "outcome_kind"])
    lab = lab[(lab.ymd >= 20230101) & (lab.ymd <= 20251231) & lab.horizon.isin([1, 3, 5, 10])]
    d = lab.merge(f, on=["code", "ymd"], how="inner", validate="many_to_one")
    axes = ["ma20_position", "ma60_position", "ret20_state", "candle_shape", "sideways",
            "support_state", "oversold_state", "supply_rejection"]
    rows = []
    for axis in axes:
        rows.extend(summarize(d, axis))
    cohort = pd.DataFrame(rows)
    cohort.to_csv(OUT / "cohort_direction_rates_2023_2025.csv", index=False)

    # Joint states with sufficient breadth; rank by directional separation from horizon base rate.
    directional = d[d.label.isin(["down_first", "rebound_first"])].copy()
    base = directional.groupby("horizon").label.apply(lambda s: (s == "down_first").mean()).to_dict()
    joints = []
    for a, b in [("ma20_position", "ret20_state"), ("support_state", "candle_shape"),
                 ("sideways", "candle_shape"), ("ma60_position", "support_state"),
                 ("oversold_state", "support_state"), ("supply_rejection", "ret20_state")]:
        for (h, va, vb), g in directional.groupby(["horizon", a, b], observed=True):
            if len(g) < 500:
                continue
            ds = float((g.label == "down_first").mean())
            joints.append({"horizon": int(h), "axis_a": a, "cohort_a": str(va), "axis_b": b,
                           "cohort_b": str(vb), "n_directional": len(g), "down_share": ds,
                           "delta_vs_horizon": ds - base[int(h)]})
    joint = pd.DataFrame(joints).sort_values(["horizon", "delta_vs_horizon"])
    joint.to_csv(OUT / "joint_cohort_direction_rates_2023_2025.csv", index=False)

    numeric = ["body_ratio", "upper_wick_ratio", "lower_wick_ratio", "close_pos", "ret10", "ret20",
               "pos20", "range20_pct", "bear_count5", "upper_supply_count5", "lower_rejection_count5",
               "dist_ma20_atr", "dist_ma60_atr", "ma20_slope5_atr", "ma60_slope5_atr",
               "volume_ratio20", "support_break_depth_atr"]
    smd_rows = []
    for h, gh in directional.groupby("horizon"):
        for col in numeric:
            a = gh.loc[gh.label == "down_first", col].dropna()
            b = gh.loc[gh.label == "rebound_first", col].dropna()
            pooled = np.sqrt((a.var(ddof=1) + b.var(ddof=1)) / 2)
            smd_rows.append({"horizon": int(h), "feature": col, "down_mean": a.mean(),
                             "rebound_mean": b.mean(), "standardized_mean_difference":
                             (a.mean() - b.mean()) / pooled if pooled > 0 else None})
    pd.DataFrame(smd_rows).to_csv(OUT / "feature_smd_2023_2025.csv", index=False)

    outcome = (d[d.label.isin(["down_first", "rebound_first"])]
               .groupby(["horizon", "label", "outcome_kind"], observed=True).size().rename("n").reset_index())
    outcome["share_within_label"] = outcome.n / outcome.groupby(["horizon", "label"]).n.transform("sum")
    outcome.to_csv(OUT / "outcome_kind_mix_2023_2025.csv", index=False)

    # Direct model error audit is only possible for saved 2026 probability rows.
    p = pd.read_parquet(PROBS)
    l26 = pd.read_parquet(LABELS, columns=["code", "ymd", "horizon", "label"])
    x = p.merge(l26, on=["code", "ymd", "horizon"], how="inner", validate="one_to_one")
    x = x.merge(f, on=["code", "ymd"], how="left", validate="many_to_one")
    x["pred"] = x[["p_down", "p_rebound", "p_neutral"]].idxmax(axis=1).str.removeprefix("p_").map(
        {"down": "down_first", "rebound": "rebound_first", "neutral": "neutral"})
    x["direction_confusion"] = np.select(
        [(x.label == "down_first") & (x.pred == "rebound_first"), (x.label == "rebound_first") & (x.pred == "down_first")],
        ["down_as_rebound", "rebound_as_down"], default="other")
    err_rows = []
    for axis in axes:
        for (h, val), g in x[x.label.isin(["down_first", "rebound_first"])].groupby(["horizon", axis], observed=True):
            err_rows.append({"horizon": int(h), "axis": axis, "cohort": str(val), "n_directional": len(g),
                             "down_as_rebound_rate": float(((g.label == "down_first") & (g.pred == "rebound_first")).mean()),
                             "rebound_as_down_rate": float(((g.label == "rebound_first") & (g.pred == "down_first")).mean()),
                             "direction_accuracy": float((g.label == g.pred).mean())})
    pd.DataFrame(err_rows).to_csv(OUT / "model_confusion_by_cohort_2026.csv", index=False)

    result = {
        "schema_version": "tradex.fp_order_error_cohort_audit.v1",
        "artifact_role": "review_only_diagnostic",
        "authoritative_sources": [str(FEATURES), str(LABELS), str(PROBS)],
        "fixed_scope": {"structural_cohorts": "2023-01-01..2025-12-31", "direct_model_error": "saved 2026 rows only",
                        "horizons": [1, 3, 5, 10], "threshold_tuning": False},
        "source_limit": "OOF checkpoint predictions are not persisted; 2023-25 results diagnose label separability, not model confusion.",
        "row_counts": {"joined_2023_2025": len(d), "directional_2023_2025": len(directional), "probability_rows_2026_joined": len(x)},
        "base_down_share_directional": {str(k): v for k, v in base.items()},
        "artifacts": ["cohort_direction_rates_2023_2025.csv", "joint_cohort_direction_rates_2023_2025.csv",
                      "feature_smd_2023_2025.csv", "outcome_kind_mix_2023_2025.csv",
                      "model_confusion_by_cohort_2026.csv"],
    }
    (OUT / "audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
