"""Profile independent pre-event axes without turning them into hard screens."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import duckdb
import pandas as pd


NUMERIC_FEATURES = (
    "c", "ret1", "ret3", "ret5", "ret10", "ret20", "ret40", "ret60", "ret120",
    "body_ratio", "close_position", "upper_wick_ratio", "lower_wick_ratio",
    "volume_ratio20", "close_vs_ma20", "close_vs_ma60", "range20", "range60", "range120",
    "dist_high20", "dist_low20", "dist_high60", "dist_low60", "dist_high120", "dist_low120",
)
BINARY_FEATURES = (
    "candle_direction", "lower_high_1", "lower_low_1", "lower_high_2", "lower_low_2",
    "outside_bar", "inside_bar", "close_below_prev_low", "close_above_prev_high",
    "two_bearish", "two_bullish",
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def finite(value) -> float | None:
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def aggregate(conn: duckdb.DuckDBPyConnection, path: Path, where: str) -> dict:
    row = conn.execute(f"""
      SELECT count(*) n, avg(drop5_in5) event_rate, avg(clean_drop5_in5) clean_rate,
             avg(drop8_in10) severe10_rate, avg(drop10_in20) severe20_rate
      FROM read_parquet('{sql_path(path)}') WHERE {where}
    """).fetchone()
    return dict(zip(("n","event_rate","clean_rate","severe10_rate","severe20_rate"),
                    [int(row[0]), *[finite(x) for x in row[1:]]]))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect()
    try:
        development_baseline = aggregate(conn, args.inventory, "ymd < 20240101")
        validation_baseline = aggregate(conn, args.inventory, "ymd >= 20240101")
        rows: list[dict] = []
        for feature in NUMERIC_FEATURES:
            quantiles = conn.execute(f"""
              SELECT quantile_cont({feature}, [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
              FROM read_parquet('{sql_path(args.inventory)}')
              WHERE ymd < 20240101 AND {feature} IS NOT NULL AND isfinite({feature})
            """).fetchone()[0]
            edges = []
            for value in quantiles:
                value = float(value)
                if not edges or value > edges[-1]:
                    edges.append(value)
            if len(edges) < 3:
                continue
            for idx, (lower, upper) in enumerate(zip(edges[:-1], edges[1:])):
                op = "<=" if idx == len(edges) - 2 else "<"
                condition = f"{feature}>={lower!r} AND {feature}{op}{upper!r}"
                for period, period_where, baseline in (
                    ("development", "ymd < 20240101", development_baseline),
                    ("validation", "ymd >= 20240101", validation_baseline),
                ):
                    result = aggregate(conn, args.inventory, f"{period_where} AND {condition}")
                    rows.append({
                        "feature": feature, "bucket": idx + 1, "lower": lower, "upper": upper,
                        "period": period, **result,
                        "event_rate_lift": (
                            None if result["event_rate"] is None else result["event_rate"] / baseline["event_rate"]
                        ),
                    })
        for feature in BINARY_FEATURES:
            values = [r[0] for r in conn.execute(
                f"SELECT DISTINCT {feature} FROM read_parquet('{sql_path(args.inventory)}') WHERE {feature} IS NOT NULL ORDER BY 1"
            ).fetchall()]
            for value in values:
                for period, period_where, baseline in (
                    ("development", "ymd < 20240101", development_baseline),
                    ("validation", "ymd >= 20240101", validation_baseline),
                ):
                    result = aggregate(conn, args.inventory, f"{period_where} AND {feature}={int(value)}")
                    rows.append({
                        "feature": feature, "bucket": int(value), "lower": float(value), "upper": float(value),
                        "period": period, **result,
                        "event_rate_lift": (
                            None if result["event_rate"] is None else result["event_rate"] / baseline["event_rate"]
                        ),
                    })
    finally:
        conn.close()

    profile = pd.DataFrame(rows)
    profile_path = args.output / "axis_band_profile.parquet"
    profile.to_parquet(profile_path, index=False)
    dev = profile.loc[profile.period.eq("development")].copy()
    val = profile.loc[profile.period.eq("validation")].copy()
    paired = dev.merge(val, on=["feature","bucket","lower","upper"], suffixes=("_dev","_val"))
    paired["stable_positive_band"] = (
        paired.n_dev.ge(1000) & paired.n_val.ge(500)
        & paired.event_rate_lift_dev.ge(1.20) & paired.event_rate_lift_val.ge(1.10)
    )
    paired["stable_negative_band"] = (
        paired.n_dev.ge(1000) & paired.n_val.ge(500)
        & paired.event_rate_lift_dev.le(0.85) & paired.event_rate_lift_val.le(0.90)
    )
    stable = paired.loc[paired.stable_positive_band | paired.stable_negative_band].copy()
    stable = stable.sort_values(
        ["stable_positive_band","event_rate_lift_val","event_rate_lift_dev"],
        ascending=[False,False,False],
    )
    stable_path = args.output / "stable_axis_bands.parquet"
    stable.to_parquet(stable_path, index=False)
    feature_summary = []
    for feature, group in stable.groupby("feature"):
        feature_summary.append({
            "feature": feature,
            "stable_positive_bands": int(group.stable_positive_band.sum()),
            "stable_negative_bands": int(group.stable_negative_band.sum()),
            "max_validation_lift": finite(group.event_rate_lift_val.max()),
            "min_validation_lift": finite(group.event_rate_lift_val.min()),
            "validation_rows_across_stable_bands": int(group.n_val.sum()),
        })
    feature_summary.sort(key=lambda row: (row["stable_positive_bands"], row["max_validation_lift"] or 0), reverse=True)
    checks = {
        "profile_rows_ge_400": len(profile) >= 400,
        "stable_band_count_ge_10": len(stable) >= 10,
        "stable_feature_count_ge_5": len(feature_summary) >= 5,
        "development_daily_n_ge_500000": development_baseline["n"] >= 500000,
        "validation_daily_n_ge_200000": validation_baseline["n"] >= 200000,
    }
    result = {
        "schema_version": "tradex_short_decline_axis_profile_v1.compare.v1",
        "artifact_role": "authoritative_independent_decline_axis_band_profile",
        "review_only": True,
        "research_phase": "comparison_stabilization",
        "fixed_conditions": {
            "source_inventory": str(args.inventory.resolve()),
            "development": "2019-2023",
            "validation": "2024-2026-06-17 complete-forward rows",
            "target": "5-session intraday low <= -5% from next open",
            "numeric_bands": "development-period deciles; duplicated edges collapsed",
            "axis_policy": "independent descriptive bands only; no hard screen and no combined rule",
            "stable_positive_gate": "dev n>=1000 lift>=1.20; validation n>=500 lift>=1.10",
            "stable_negative_gate": "dev n>=1000 lift<=0.85; validation n>=500 lift<=0.90",
        },
        "authoritative_result": {
            "development_baseline": development_baseline,
            "validation_baseline": validation_baseline,
            "profile_rows": int(len(profile)),
            "stable_band_count": int(len(stable)),
            "stable_feature_count": int(len(feature_summary)),
            "feature_summary": feature_summary,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(stable)),
            "selection_divergence_reason": "independent bands expose outcome-rate gradients without selecting candidates",
            "profiled_numeric_axes": len(NUMERIC_FEATURES),
            "profiled_categorical_axes": len(BINARY_FEATURES),
        },
        "judgment": {
            "candidate_local_decision": "keep" if all(checks.values()) else "hold",
            "session_aggregate_decision": "keep_independent_axis_map" if all(checks.values()) else "hold_axis_map",
            "authoritative_rollup_decision": "keep_decline_axis_profile_v1_review_only" if all(checks.values()) else "hold_expand_axis_profile",
            "reason_type": "multiple_axes_show_stable_nonbinary_gradients" if all(checks.values()) else "insufficient_stable_axis_breadth",
        },
        "not_changed": ["candidate universe", "hard screens", "combined score", "MeeMee", "ranking", "runtime DB"],
        "remaining_risks": [
            "single-axis lift does not establish causal or combined usefulness",
            "overlapping daily rows remain correlated within decline episodes",
            "monthly range age and market-relative axes are absent",
            "corporate-action robustness is not yet applied",
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
