from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score


AXIS_ID = "tradex_nikkei225_relative_signal_audit_v1"
FEATURES = [
    "rel_logret_5", "rel_logret_10", "rel_logret_20",
    "beta60_prior", "residual_1", "residual_5", "residual_10", "residual_20",
]
RETURN_LIKE = {x for x in FEATURES if x != "beta60_prior"}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_features(d: pd.DataFrame) -> pd.DataFrame:
    d = d.sort_values(["code", "ymd"]).copy()
    d["stock_ret1"] = d.groupby("code", sort=False)["c"].pct_change()
    d["stock_logret1"] = np.log1p(d["stock_ret1"].clip(lower=-0.999999))
    d["market_logret1"] = np.log1p(d["market_mean_ret1"].clip(lower=-0.999999))
    d["rel_logret_1"] = d["stock_logret1"] - d["market_logret1"]

    # Beta is fitted only on observations strictly before the assessment date.
    def one(g: pd.DataFrame) -> pd.DataFrame:
        s = g["stock_ret1"]
        m = g["market_mean_ret1"]
        cov = s.rolling(60, min_periods=40).cov(m).shift(1)
        var = m.rolling(60, min_periods=40).var().shift(1)
        g["beta60_prior"] = cov / var.replace(0.0, np.nan)
        g["residual_1"] = s - g["beta60_prior"] * m
        for w in (5, 10, 20):
            g[f"rel_logret_{w}"] = g["rel_logret_1"].rolling(w, min_periods=w).sum()
            g[f"residual_{w}"] = g["residual_1"].rolling(w, min_periods=w).sum()
        return g

    return d.groupby("code", group_keys=False, sort=False).apply(one).reset_index(drop=True)


def auc_down_vs_rebound(x: pd.Series, label: pd.Series, feature: str) -> tuple[float | None, int]:
    use = x.notna() & label.isin(["down_first", "rebound_first"])
    if use.sum() == 0 or label[use].nunique() < 2:
        return None, int(use.sum())
    # Pre-registered sign: weak relative/residual return implies DOWN. Beta has no directional sign.
    if feature == "beta60_prior":
        return None, int(use.sum())
    return float(roc_auc_score((label[use] == "down_first").astype(int), -x[use])), int(use.sum())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--labels", required=True)
    ap.add_argument("--out-root", default=r"G:\Tradex\relative_signal_audit_v1")
    args = ap.parse_args()
    src = Path(args.features)
    lab_src = Path(args.labels)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path(args.out_root) / f"{stamp}-{AXIS_ID}"
    out.mkdir(parents=True, exist_ok=False)

    raw = pd.read_parquet(src, columns=["code", "ymd", "c", "market_mean_ret1"])
    raw["code"] = raw["code"].astype(str)
    feat = build_features(raw)
    labels = pd.read_parquet(lab_src, columns=["code", "ymd", "horizon", "label"])
    labels["code"] = labels["code"].astype(str)
    d = labels.merge(feat[["code", "ymd", *FEATURES]], on=["code", "ymd"], how="inner", validate="many_to_one")
    d["year"] = d["ymd"].astype(str).str[:4].astype(int)

    ref = feat[(feat["ymd"] >= 20190101) & (feat["ymd"] <= 20221231)]
    cuts = {}
    for f in FEATURES:
        q = ref[f].dropna().quantile([1 / 3, 2 / 3]).tolist()
        cuts[f] = [float(q[0]), float(q[1])]

    rows = []
    for h in (1, 3, 5, 10):
        dh = d[d.horizon == h]
        for year in (2023, 2024, 2025):
            dy = dh[dh.year == year]
            overall = dy["label"].value_counts(normalize=True).to_dict()
            for f in FEATURES:
                q1, q2 = cuts[f]
                bins = pd.cut(dy[f], [-np.inf, q1, q2, np.inf], labels=["low", "mid", "high"])
                auc, auc_n = auc_down_vs_rebound(dy[f], dy.label, f)
                for bucket in ("low", "mid", "high"):
                    z = dy[bins == bucket]
                    shares = z.label.value_counts(normalize=True).to_dict()
                    rows.append({
                        "horizon": h, "year": year, "feature": f, "bucket": bucket,
                        "n": int(len(z)), "down_share": shares.get("down_first"),
                        "rebound_share": shares.get("rebound_first"), "neutral_share": shares.get("neutral"),
                        "down_lift_vs_year": (shares.get("down_first", 0.0) - overall.get("down_first", 0.0)),
                        "rebound_lift_vs_year": (shares.get("rebound_first", 0.0) - overall.get("rebound_first", 0.0)),
                        "fixed_sign_auc_down_vs_rebound": auc, "auc_n": auc_n,
                    })
    tbl = pd.DataFrame(rows)
    csv_path = out / "year_bucket_diagnostics.csv"
    tbl.to_csv(csv_path, index=False, encoding="utf-8")

    summaries = []
    for (h, f), g in tbl.groupby(["horizon", "feature"]):
        piv = g.pivot(index="year", columns="bucket", values=["down_share", "rebound_share"])
        spreads = []
        for y in (2023, 2024, 2025):
            if y in piv.index:
                spreads.append(float((piv.loc[y, ("down_share", "low")] - piv.loc[y, ("down_share", "high")])
                                     - (piv.loc[y, ("rebound_share", "low")] - piv.loc[y, ("rebound_share", "high")])))
        aucs = g.groupby("year")["fixed_sign_auc_down_vs_rebound"].first().dropna().to_dict()
        directional = f in RETURN_LIKE
        stable_down = directional and len(spreads) == 3 and all(v > 0.01 for v in spreads)
        stable_rebound = directional and len(spreads) == 3 and all(v < -0.01 for v in spreads)
        summaries.append({"horizon": int(h), "feature": f, "pre_registered_direction": "low_implies_down" if directional else "none_diagnostic_only",
                          "year_low_vs_high_down_minus_rebound_spread": {str(y): spreads[i] for i, y in enumerate((2023, 2024, 2025))},
                          "fixed_sign_auc_by_year": {str(k): float(v) for k, v in aucs.items()},
                          "stable_observed_direction": ("low_implies_down" if stable_down else
                                                        "low_implies_rebound" if stable_rebound else "unstable_or_below_1pp"),
                          "sign_stable_all_years_and_min_1pp": bool(stable_down or stable_rebound)})

    result = {
        "axis_id": AXIS_ID,
        "decision": ("hold_rebound_risk_only_not_sell_axis" if any(x["stable_observed_direction"] == "low_implies_rebound" for x in summaries)
                     else "hold_sell_axis" if any(x["stable_observed_direction"] == "low_implies_down" for x in summaries)
                     else "drop_no_stable_relative_signal"),
        "scope": "read_only_diagnostic; 2023_2025_never_used_for_threshold_optimization",
        "sources": {"features": str(src), "labels": str(lab_src)},
        "contracts": {
            "pit": "stock close and market_mean_ret1 through t; beta60 uses t-60..t-1 only",
            "tertiles": "fixed once from 2019-2022 pooled observations",
            "direction": "for relative/residual returns only, low fixed a priori as down direction; beta directional AUC omitted",
            "stability_gate": "same signed low-vs-high down-minus-rebound spread in 2023, 2024, 2025 and abs(each)>1pp; sign is reported, not flipped to improve AUC",
            "no_model_selection": True,
        },
        "row_counts": {"feature_rows": int(len(feat)), "joined_label_rows": int(len(d))},
        "reference_tertile_cuts": cuts,
        "summaries": summaries,
        "authoritative_detail_csv": str(csv_path),
        "non_scope": ["MeeMee", "runtime_db", "ranking", "production_model"],
    }
    compare = out / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    audit = {"complete": True, "compare_sha256": sha256(compare), "detail_sha256": sha256(csv_path),
             "feature_source_sha256": sha256(src), "label_source_sha256": sha256(lab_src)}
    (out / "audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    (out / "complete.marker.json").write_text(json.dumps({"complete": True, "compare_sha256": audit["compare_sha256"]}, indent=2), encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
