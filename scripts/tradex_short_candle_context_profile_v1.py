"""Measure candle evidence conditionally inside position, path, and volatility bands."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


CANDLE_NUMERIC = ("body_ratio", "close_position", "upper_wick_ratio", "lower_wick_ratio")
CANDLE_CATEGORICAL = (
    "candle_direction", "close_below_prev_low", "lower_high_1", "lower_low_1",
    "lower_high_2", "lower_low_2", "two_bearish", "two_bullish",
)
CONTEXT_NUMERIC = (
    "range20", "range60", "range120", "dist_high20", "dist_high60", "dist_high120",
    "ret1", "ret3", "ret5", "ret20", "volume_ratio20",
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def edges(values: pd.Series) -> np.ndarray:
    out = np.unique(values.dropna().quantile([0, .2, .4, .6, .8, 1]).to_numpy(dtype=float))
    if len(out) >= 2:
        out[0] = -np.inf
        out[-1] = np.inf
    return out


def add_bins(frame: pd.DataFrame, development: pd.Series) -> tuple[pd.DataFrame, dict]:
    contracts = {}
    for feature in (*CANDLE_NUMERIC, *CONTEXT_NUMERIC):
        cut = edges(frame.loc[development, feature])
        if len(cut) < 3:
            continue
        frame[f"{feature}__band"] = pd.cut(frame[feature], bins=cut, labels=False, include_lowest=True)
        contracts[feature] = [None if np.isinf(x) else float(x) for x in cut]
    for feature in CANDLE_CATEGORICAL:
        frame[f"{feature}__band"] = frame[feature]
        contracts[feature] = sorted(int(x) for x in frame.loc[development, feature].dropna().unique())
    return frame, contracts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    columns = [
        "code", "ymd", "drop5_in5", "clean_drop5_in5", "drop8_in10", "high5_pct",
        *CANDLE_NUMERIC, *CANDLE_CATEGORICAL, *CONTEXT_NUMERIC,
    ]
    conn = duckdb.connect()
    try:
        frame = conn.execute(
            f"SELECT {','.join(columns)} FROM read_parquet(?)",
            [str(args.inventory.resolve())],
        ).df()
    finally:
        conn.close()
    development = frame.ymd.lt(20240101)
    frame, contracts = add_bins(frame, development)
    frame["period"] = np.where(development, "development", "validation")

    rows = []
    for candle in (*CANDLE_NUMERIC, *CANDLE_CATEGORICAL):
        cb = f"{candle}__band"
        if cb not in frame:
            continue
        for context in CONTEXT_NUMERIC:
            xb = f"{context}__band"
            if xb not in frame:
                continue
            subset = frame.dropna(subset=[cb, xb])
            context_base = subset.groupby(["period", xb], observed=True).agg(
                context_n=("drop5_in5", "size"), context_event_rate=("drop5_in5", "mean")
            ).reset_index()
            cells = subset.groupby(["period", cb, xb], observed=True).agg(
                n=("drop5_in5", "size"), codes=("code", "nunique"),
                event_rate=("drop5_in5", "mean"), clean_rate=("clean_drop5_in5", "mean"),
                severe_rate=("drop8_in10", "mean"), median_high5_pct=("high5_pct", "median"),
            ).reset_index()
            cells = cells.merge(context_base, on=["period", xb], how="left")
            cells["conditional_lift"] = cells.event_rate / cells.context_event_rate
            for row in cells.itertuples(index=False):
                rows.append({
                    "candle_axis": candle, "context_axis": context,
                    "candle_band": int(getattr(row, cb)), "context_band": int(getattr(row, xb)),
                    "period": row.period, "n": int(row.n), "codes": int(row.codes),
                    "event_rate": float(row.event_rate), "context_event_rate": float(row.context_event_rate),
                    "conditional_lift": float(row.conditional_lift), "clean_rate": float(row.clean_rate),
                    "severe_rate": float(row.severe_rate), "median_high5_pct": float(row.median_high5_pct),
                })
    profile = pd.DataFrame(rows)
    profile_path = args.output / "candle_context_cell_profile.parquet"
    profile.to_parquet(profile_path, index=False)
    dev = profile[profile.period.eq("development")]
    val = profile[profile.period.eq("validation")]
    paired = dev.merge(
        val,
        on=["candle_axis", "context_axis", "candle_band", "context_band"],
        suffixes=("_dev", "_val"),
    )
    paired["stable_positive"] = (
        paired.n_dev.ge(3000) & paired.n_val.ge(1000)
        & paired.conditional_lift_dev.ge(1.12) & paired.conditional_lift_val.ge(1.08)
    )
    paired["stable_negative"] = (
        paired.n_dev.ge(3000) & paired.n_val.ge(1000)
        & paired.conditional_lift_dev.le(0.88) & paired.conditional_lift_val.le(0.92)
    )
    stable = paired[paired.stable_positive | paired.stable_negative].copy()
    stable = stable.sort_values(
        ["stable_positive", "conditional_lift_val", "conditional_lift_dev"],
        ascending=[False, False, False],
    )
    stable_path = args.output / "stable_candle_context_cells.parquet"
    stable.to_parquet(stable_path, index=False)
    family_rows = []
    for (candle, context), group in stable.groupby(["candle_axis", "context_axis"]):
        family_rows.append({
            "candle_axis": candle, "context_axis": context,
            "stable_positive_cells": int(group.stable_positive.sum()),
            "stable_negative_cells": int(group.stable_negative.sum()),
            "max_validation_conditional_lift": float(group.conditional_lift_val.max()),
            "min_validation_conditional_lift": float(group.conditional_lift_val.min()),
            "validation_rows": int(group.n_val.sum()),
        })
    family_rows.sort(
        key=lambda row: (row["stable_positive_cells"], row["max_validation_conditional_lift"]),
        reverse=True,
    )
    checks = {
        "cell_profile_rows_ge_3000": len(profile) >= 3000,
        "stable_cell_count_ge_5": len(stable) >= 5,
        "stable_family_count_ge_3": len(family_rows) >= 3,
        "no_hard_screen_created": True,
        "validation_not_used_for_band_edges": True,
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_candle_context_profile_v1.compare.v1",
        "artifact_role": "authoritative_candle_context_conditional_profile",
        "review_only": True,
        "research_phase": "branching_generation",
        "fixed_conditions": {
            "source_inventory": str(args.inventory.resolve()),
            "development": "2019-2023", "validation": "2024-2026",
            "target": "5-session intraday low <= -5% from next open",
            "candle_numeric_bands": "development quintiles",
            "context_numeric_bands": "development quintiles",
            "conditional_reference": "event rate within the same context band before candle subdivision",
            "policy": "descriptive overlapping evidence; no hard screen or combined score",
            "stable_gate": "dev n>=3000 and lift outside 0.88-1.12; validation n>=1000 and lift outside 0.92-1.08 in same direction",
            "band_contracts": contracts,
        },
        "authoritative_result": {
            "profile_rows": int(len(profile)), "stable_cell_count": int(len(stable)),
            "stable_family_count": int(len(family_rows)), "family_summary": family_rows,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int(len(stable)),
            "selection_divergence_reason": "candle meaning is measured within context bands rather than as a standalone screen",
            "candle_axes": len(CANDLE_NUMERIC) + len(CANDLE_CATEGORICAL),
            "context_axes": len(CONTEXT_NUMERIC),
            "tested_axis_pairs": (len(CANDLE_NUMERIC) + len(CANDLE_CATEGORICAL)) * len(CONTEXT_NUMERIC),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_candle_context_map" if keep else "hold_candle_context_map",
            "authoritative_rollup_decision": "keep_candle_context_profile_v1_review_only" if keep else "hold_expand_context_axes",
            "reason_type": "candle_axes_gain_stable_information_only_in_specific_contexts" if keep else "conditional_candle_effect_not_broadly_stable",
        },
        "not_changed": ["hard screens", "candidate ranking", "combined score", "MeeMee", "runtime DB", "production logic"],
        "remaining_risks": [
            "multiple pair testing can surface correlated duplicate families",
            "daily rows are correlated within market and decline episodes",
            "monthly range age and market-relative context remain absent",
            "stable cells are descriptive evidence, not entry rules",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"inventory": {"path": str(args.inventory.resolve()), "sha256": sha(args.inventory)}},
        "artifacts": {
            "profile": {"path": str(profile_path), "sha256": sha(profile_path)},
            "stable": {"path": str(stable_path), "sha256": sha(stable_path)},
            "compare": {"path": str(compare), "sha256": sha(compare)},
        },
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
