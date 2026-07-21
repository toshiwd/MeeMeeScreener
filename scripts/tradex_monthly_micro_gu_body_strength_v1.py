"""Split the fixed 0-0.5% GU setup by signal-day bearish body strength."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


BANDS = ["weak_lt_0.4", "clear_0.4_0.7", "extreme_ge_0.7"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "ret5_mean": float(frame.ret5_pct.mean()),
        "ret10_mean": float(frame.ret10_pct.mean()),
        "ret20_mean": float(frame.ret20_pct.mean()),
        "ret20_median": float(frame.ret20_pct.median()),
        "ret20_positive_rate": float((frame.ret20_pct > 0).mean()),
        "max_up20_ge_10_rate": float((frame.max_up20_pct >= 10).mean()),
        "max_down20_le_minus5_rate": float((frame.max_down20_pct <= -5).mean()),
    }


def bootstrap(values, seed):
    values = np.asarray(values, dtype=float)
    if not len(values):
        return {"samples": 0}
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(10000, len(values)), replace=True).mean(axis=1)
    return {
        "samples": 10000,
        "ret20_mean_ci95_low": float(np.quantile(draws, 0.025)),
        "ret20_mean_ci95_high": float(np.quantile(draws, 0.975)),
        "probability_mean_gt_zero": float((draws > 0).mean()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(a.events)
    events = events.loc[events.gu_band.astype(str).eq("0_0.5")].copy()
    con = duckdb.connect(str(a.db), read_only=True)
    bars = con.execute(
        """select code, strftime(to_timestamp(date),'%Y%m%d')::integer ymd,
                  o, h, l, c
           from daily_bars where code in (select unnest(?))""",
        [events.code.unique().tolist()],
    ).fetchdf()
    con.close()
    bars.code = bars.code.astype(str).str.zfill(4)
    selected = events.merge(
        bars, left_on=["code", "signal_ymd"], right_on=["code", "ymd"], validate="one_to_one"
    )
    selected["bearish_body_ratio"] = (selected.o - selected.c) / (selected.h - selected.l).where(
        selected.h > selected.l
    )
    if selected.bearish_body_ratio.isna().any():
        raise ValueError("zero-range signal bar found")
    selected["body_band"] = pd.cut(
        selected.bearish_body_ratio,
        [-np.inf, 0.4, 0.7, np.inf],
        labels=BANDS,
        right=False,
    )
    selected.to_parquet(a.output / "monthly_micro_gu_body_strength_events.parquet", index=False)

    groups = {band: metrics(selected.loc[selected.body_band.eq(band)]) for band in BANDS}
    years = {
        str(year): {band: metrics(rows.loc[rows.body_band.eq(band)]) for band in BANDS}
        for year, rows in selected.groupby("year")
    }
    boot = {
        band: bootstrap(selected.loc[selected.body_band.eq(band), "ret20_pct"], 20260718 + i)
        for i, band in enumerate(BANDS)
    }
    sensitivity_ranges = [(0.30, 0.60), (0.35, 0.65), (0.40, 0.70), (0.45, 0.75), (0.50, 0.80)]
    sensitivity = {
        f"{lo:.2f}_{hi:.2f}": metrics(
            selected.loc[selected.bearish_body_ratio.ge(lo) & selected.bearish_body_ratio.lt(hi)]
        )
        for lo, hi in sensitivity_ranges
    }
    sensitivity_checks = {
        key: {
            "ret20_mean_gt_5": row["ret20_mean"] > 5,
            "ret20_positive_rate_ge_0.75": row["ret20_positive_rate"] >= 0.75,
            "deep_drawdown_rate_le_0.25": row["max_down20_le_minus5_rate"] <= 0.25,
        }
        for key, row in sensitivity.items()
    }
    target = groups["clear_0.4_0.7"]
    checks = {
        "n_ge_10": target["n"] >= 10,
        "ret20_mean_gt_other_bands": target["ret20_mean"]
        > max(groups["weak_lt_0.4"]["ret20_mean"], groups["extreme_ge_0.7"]["ret20_mean"]),
        "ret20_positive_rate_ge_0.75": target["ret20_positive_rate"] >= 0.75,
        "deep_drawdown_rate_le_0.20": target["max_down20_le_minus5_rate"] <= 0.20,
        "bootstrap_probability_gt_zero_ge_0.95": boot["clear_0.4_0.7"]["probability_mean_gt_zero"] >= 0.95,
        "all_neighboring_thresholds_pass": all(all(row.values()) for row in sensitivity_checks.values()),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_monthly_micro_gu_body_strength_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_body_strength",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "axis_changed": "signal-day bearish body ratio only",
            "bearish_body_ratio": "(open-close)/(high-low); bullish values are below zero",
            "body_bands": BANDS,
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "all_events": metrics(selected),
            "groups": groups,
            "years": years,
            "bootstrap_ret20_mean": boot,
            "threshold_sensitivity": sensitivity,
            "threshold_sensitivity_checks": sensitivity_checks,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "fixed micro-GU events split only by signal candle body strength",
            "group_counts": {band: groups[band]["n"] for band in BANDS},
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_clear_non_extreme_bear_body" if keep else "hold_body_strength",
            "authoritative_rollup_decision": (
                "keep_body_ratio_0.4_to_0.7_review_only" if keep else "hold"
            ),
            "reason_type": "clear_non_extreme_capitulation_body_gates_passed" if keep else "gate_failed",
        },
        "not_changed": [
            "monthly range definition",
            "capitulation return threshold",
            "GU magnitude bands",
            "volume",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "small target sample",
            "thresholds are exploratory candle taxonomy and require out-of-sample validation",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "selected_events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "monthly_micro_gu_body_strength_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "groups": groups, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
