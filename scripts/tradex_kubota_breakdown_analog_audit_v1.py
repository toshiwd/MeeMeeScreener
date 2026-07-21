#!/usr/bin/env python
"""Review-only, point-in-time retrieval of 6326-style breakdown analogs.

This is a hypothesis/audit tool, not a selector.  Every setup field is built
from bars available at or before ``ymd``.  Forward columns are used only after
cohort membership has been frozen, to describe subsequent outcomes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_LEDGER = Path(
    r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1"
    r"\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1"
    r"\daily_assessment_features.parquet"
)


def _run_length(x: np.ndarray) -> np.ndarray:
    out = np.zeros(len(x), dtype=np.int32)
    n = 0
    for i, value in enumerate(x):
        n = n + 1 if bool(value) else 0
        out[i] = n
    return out


def _features(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("ymd").copy()
    o, h, l, c = (g[k].astype(float) for k in ("o", "h", "l", "c"))
    atr = g["atr14"].astype(float).replace(0, np.nan)
    ma7, ma20, ma60, ma100 = (g[k].astype(float) for k in ("ma7", "ma20", "ma60", "ma100"))

    g["above_ma100_run"] = _run_length((c > ma100).fillna(False).to_numpy())
    g["ma7_rising"] = ma7 > ma7.shift(1)
    g["break_ma7"] = (c < ma7) & (c.shift(1) >= ma7.shift(1))
    g["break_ma20"] = (c < ma20) & (c.shift(1) >= ma20.shift(1))
    g["gap_pct"] = o / c.shift(1) - 1.0
    g["gd"] = (g["gap_pct"] <= -0.01) & (o < l.shift(1))
    g["body_atr"] = (c - o) / atr
    g["high_zone"] = c / h.shift(1).rolling(120, min_periods=60).max() >= 0.90

    # A point-in-time state machine: qualifying impulse in the prior 8..35 bars,
    # followed by a later retry whose high comes close but does not exceed it.
    impulse = (g["body_atr"] >= 2.0) & (g["above_ma100_run"] >= 80) & g["high_zone"]
    impulse_age = np.full(len(g), np.nan)
    retry_failed = np.zeros(len(g), dtype=bool)
    retry_count = np.zeros(len(g), dtype=np.int16)
    impulse_high = np.full(len(g), np.nan)
    hs = h.to_numpy()
    cs = c.to_numpy()
    imp = impulse.fillna(False).to_numpy()
    for i in range(len(g)):
        candidates = np.flatnonzero(imp[max(0, i - 35) : max(0, i - 7)])
        if not len(candidates):
            continue
        j = max(0, i - 35) + int(candidates[-1])
        top = hs[j]
        # Retry must occur after the impulse and before the 7MA-break precursor.
        rh = hs[j + 2 : i]
        rc = cs[j + 2 : i]
        if not len(rh):
            continue
        near = (rh >= 0.97 * top) & (rh <= 1.015 * top) & (rc < top)
        impulse_age[i] = i - j
        retry_count[i] = int(near.sum())
        retry_failed[i] = bool(near.any() and np.nanmax(rh) <= 1.015 * top)
        impulse_high[i] = top
    g["impulse_age"] = impulse_age
    g["failed_retry"] = retry_failed
    g["failed_retry_count"] = retry_count
    g["impulse_high"] = impulse_high
    prior_break = g["break_ma7"].shift(1).eq(True)
    prior_rising = g["ma7_rising"].shift(1).eq(True)
    g["precursor_7break_rising"] = prior_break & prior_rising
    g["trigger_gd_ma20"] = g["gd"] & g["break_ma20"]
    g["long_high_context"] = g["impulse_age"].notna()
    g["exact_setup"] = g["long_high_context"] & g["failed_retry"] & g["precursor_7break_rising"] & g["trigger_gd_ma20"]
    g["relaxed_setup"] = g["long_high_context"] & g["failed_retry"] & g["trigger_gd_ma20"]
    g["counter_early_only"] = g["long_high_context"] & g["failed_retry"] & g["precursor_7break_rising"] & ~g["trigger_gd_ma20"]
    g["counter_trigger_no_top"] = g["trigger_gd_ma20"] & ~g["long_high_context"]
    g["dist_ma60_atr_entry"] = (c - ma60) / atr
    return g


def _stats(d: pd.DataFrame, mask: pd.Series) -> dict:
    x = d.loc[mask]
    out = {"n": int(len(x)), "codes": int(x.code.nunique()), "years": x.ymd.str[:4].value_counts().sort_index().to_dict()}
    for horizon in (1, 3, 5, 10):
        down, up = f"down_exc_{horizon}", f"up_exc_{horizon}"
        if down in x and up in x:
            valid = x[[down, up]].dropna()
            out[f"h{horizon}"] = {
                "n": int(len(valid)),
                "down_mean": float(valid[down].mean()) if len(valid) else None,
                "up_mean": float(valid[up].mean()) if len(valid) else None,
                "down_3pct_share": float((valid[down] <= -0.03).mean()) if len(valid) else None,
                "rebound_first_proxy_share": float((valid[up] > -valid[down]).mean()) if len(valid) else None,
            }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    raw = pd.read_parquet(args.ledger)
    raw["ymd"] = raw["ymd"].astype(str)
    d = pd.concat([_features(g) for _, g in raw.groupby("code", sort=False)], ignore_index=True)

    cohorts = {
        "exact_setup": d["exact_setup"],
        "relaxed_without_required_prior_7break": d["relaxed_setup"],
        "counter_early_signal_without_trigger": d["counter_early_only"],
        "counter_gd_ma20_without_long_top_context": d["counter_trigger_no_top"],
    }
    columns = [
        "code", "ymd", "o", "h", "l", "c", "atr14", "ma7", "ma20", "ma60", "ma100",
        "above_ma100_run", "impulse_age", "failed_retry_count", "gap_pct", "ma7_rising",
        "precursor_7break_rising", "trigger_gd_ma20", "dist_ma60_atr_entry",
        "down_exc_1", "up_exc_1", "down_exc_3", "up_exc_3", "down_exc_5", "up_exc_5",
        "down_exc_10", "up_exc_10", "exact_setup", "relaxed_setup",
    ]
    cohort_text = pd.Series("", index=d.index, dtype=object)
    for name, mask in cohorts.items():
        cohort_text.loc[mask] = cohort_text.loc[mask].map(lambda old: f"{old},{name}".strip(","))
    selected = d.loc[np.logical_or.reduce([m.to_numpy() for m in cohorts.values()]), columns].copy()
    selected["cohorts"] = cohort_text.loc[selected.index]
    selected.to_csv(args.output / "analog_rows.csv", index=False)

    kubota = d[(d.code.astype(str) == "6326") & d.ymd.isin(["20260303", "20260304"])][columns].to_dict("records")
    report = {
        "schema_version": "tradex_kubota_breakdown_analog_audit_v1",
        "research_only": True,
        "selection_is_pit": True,
        "outcome_columns_excluded_from_membership": True,
        "hypothesis_status": "user_example_derived_not_independently_validated",
        "source": str(args.ledger),
        "source_sha256": hashlib.sha256(args.ledger.read_bytes()).hexdigest(),
        "definitions": {
            "long_high_impulse": "bull body >=2 ATR, close >=90% prior 120-bar high, >=80 consecutive closes above MA100; impulse 8..35 bars before trigger",
            "failed_retry": "post-impulse high reaches 97%..101.5% impulse high but closes below impulse high and never exceeds 101.5%",
            "early_precursor": "previous bar crosses below rising MA7",
            "trigger": "open <= previous close-1%, open below previous low, and close crosses below MA20",
        },
        "cohorts": {name: _stats(d, mask) for name, mask in cohorts.items()},
        "kubota_reference_rows": kubota,
        "judgment_rule": {
            "minimum_breadth_for_effectiveness_claim": "at least 100 events, 30 codes, and 3 calendar years",
            "below_gate": "unjudgeable; use only to refine definitions and request counterexamples",
        },
    }
    (args.output / "audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    hashes = {}
    for name in ("analog_rows.csv", "audit.json"):
        hashes[name] = hashlib.sha256((args.output / name).read_bytes()).hexdigest()
    (args.output / "complete.json").write_text(json.dumps({"complete": True, "sha256": hashes}, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
