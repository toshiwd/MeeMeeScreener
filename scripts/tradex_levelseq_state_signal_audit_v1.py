"""Read-only diagnostic audit of level-sequence states against first-passage labels.

The audit is intentionally descriptive.  It uses 2023-2025 only, preserves the
feature generator's integer state codes, and does not search thresholds or emit
an actionable trading rule.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


LEVELS = ("support20_prior", "resistance20_prior", "ma7", "ma20", "ma60")
HORIZONS = (1, 3, 5, 10)
YEARS = (2023, 2024, 2025)
LABELS = ("down_first", "rebound_first", "neutral")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def slope_sign(s: pd.Series) -> pd.Series:
    a = np.select([s.isna(), s < 0, s > 0], ["missing", "negative", "positive"], default="zero")
    return pd.Series(a, index=s.index, dtype="object")


def make_states(f: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    out = f[["code", "ymd"]].copy()
    catalog: list[dict] = []
    for p in LEVELS:
        for suffix in ("lifecycle_state", "rebreak_flag", "no_reset_below", "test_count5", "test_count20"):
            col = f"{p}_{suffix}"
            out[col] = f[col].astype("Int64").astype(str)
            catalog.append({"state": col, "source": col, "transform": "identity_integer_code"})
        col = f"{p}_test_depth_slope20"
        state = f"{col}_sign"
        out[state] = slope_sign(f[col])
        catalog.append({"state": state, "source": col, "transform": "sign_only_no_fitted_threshold"})
        joint = f"{p}_lifecycle_rebreak_no_reset_code"
        out[joint] = (
            f[f"{p}_lifecycle_state"].astype("Int64").astype(str)
            + "|" + f[f"{p}_rebreak_flag"].astype("Int64").astype(str)
            + "|" + f[f"{p}_no_reset_below"].astype("Int64").astype(str)
        )
        catalog.append({
            "state": joint,
            "source": [f"{p}_lifecycle_state", f"{p}_rebreak_flag", f"{p}_no_reset_below"],
            "transform": "exact_code_tuple",
        })
    for col in (
        "box_breakout_reentry_order", "box_clear_break_side", "box_clear_break_confirmed2",
        "box_upper_failed_excursion_count20", "box_lower_failed_excursion_count20",
    ):
        out[col] = f[col].astype("Int64").astype(str)
        catalog.append({"state": col, "source": col, "transform": "identity_integer_code"})
    out["box_order_break_side_code"] = (
        f["box_breakout_reentry_order"].astype("Int64").astype(str)
        + "|" + f["box_clear_break_side"].astype("Int64").astype(str)
        + "|" + f["box_clear_break_confirmed2"].astype("Int64").astype(str)
    )
    catalog.append({
        "state": "box_order_break_side_code",
        "source": ["box_breakout_reentry_order", "box_clear_break_side", "box_clear_break_confirmed2"],
        "transform": "exact_code_tuple",
    })
    return out, catalog


def summarize(g: pd.DataFrame) -> dict:
    vc = g["label"].value_counts()
    n = len(g)
    return {
        "n": int(n),
        "codes": int(g["code"].nunique()),
        "months": int(g["month"].nunique()),
        **{f"{lab}_n": int(vc.get(lab, 0)) for lab in LABELS},
        **{f"{lab}_share": float(vc.get(lab, 0) / n) if n else None for lab in LABELS},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--labels", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, required=True)
    args = ap.parse_args()

    f = pd.read_parquet(args.features)
    l = pd.read_parquet(args.labels)
    f["year"] = f["ymd"] // 10000
    l["year"] = l["ymd"] // 10000
    f = f[f.year.isin(YEARS)].copy()
    l = l[l.year.isin(YEARS) & l.horizon.isin(HORIZONS)].copy()
    assert not f.duplicated(["code", "ymd"]).any()
    assert not l.duplicated(["code", "ymd", "horizon"]).any()
    assert set(l.label.unique()).issubset(set(LABELS))

    states, catalog = make_states(f)
    joined = l.merge(states, on=["code", "ymd"], how="left", validate="many_to_one", indicator=True)
    coverage = float((joined._merge == "both").mean())
    unmatched = int((joined._merge != "both").sum())
    joined = joined[joined._merge == "both"].drop(columns="_merge")
    joined["month"] = joined["ymd"] // 100

    baselines = []
    for (h, y), g in joined.groupby(["horizon", "year"], observed=True):
        baselines.append({"horizon": int(h), "year": int(y), **summarize(g)})

    rows = []
    stability = []
    for spec in catalog:
        name = spec["state"]
        for (h, value), g in joined.groupby(["horizon", name], observed=True, dropna=False):
            pooled = summarize(g)
            year_rows = []
            for y in YEARS:
                gy = g[g.year == y]
                year_rows.append({"year": y, **summarize(gy)})
            rows.append({"state": name, "value": str(value), "horizon": int(h), "pooled": pooled, "by_year": year_rows})
            shares = {lab: [r[f"{lab}_share"] for r in year_rows if r["n"] > 0] for lab in LABELS}
            stability.append({
                "state": name, "value": str(value), "horizon": int(h),
                "years_present": int(sum(r["n"] > 0 for r in year_rows)),
                "min_year_n": int(min(r["n"] for r in year_rows)),
                **{f"{lab}_share_year_range": float(max(v) - min(v)) if v else None for lab, v in shares.items()},
            })

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.output_root / f"{stamp}-tradex_levelseq_state_signal_audit_v1"
    out.mkdir(parents=True, exist_ok=False)
    artifact = {
        "schema_version": "tradex_levelseq_state_signal_audit_v1",
        "decision": "diagnostic_only_no_threshold_search_no_trade_rule",
        "scope": {"years": list(YEARS), "horizons": list(HORIZONS), "use": "2023-2025 diagnostic only"},
        "contracts": {
            "read_only": True, "threshold_search": False, "model_selection": False,
            "meemee_write": False, "runtime_db_write": False, "ranking_write": False,
            "continuous_transform": "depth slope sign only; zero is the fixed mathematical boundary",
        },
        "sources": {
            "features": {"path": str(args.features), "sha256": sha256(args.features)},
            "labels": {"path": str(args.labels), "sha256": sha256(args.labels)},
        },
        "grain_checks": {
            "feature_rows_2023_2025": int(len(f)), "feature_codes": int(f.code.nunique()),
            "label_rows_2023_2025": int(len(l)), "joined_rows": int(len(joined)),
            "join_coverage": coverage, "unmatched_labels": unmatched,
            "feature_key_unique": True, "label_key_unique": True,
        },
        "state_catalog": catalog,
        "baselines": baselines,
        "state_results": rows,
        "year_stability": stability,
    }
    audit = out / "state_signal_audit.json"
    audit.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    complete = {
        "schema_version": "artifact_complete_v1", "complete": True,
        "authoritative_artifact": audit.name, "sha256": sha256(audit),
    }
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps(complete, indent=2), encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
