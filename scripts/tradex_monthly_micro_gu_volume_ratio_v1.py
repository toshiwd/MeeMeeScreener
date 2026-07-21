"""Compare signal-day volume ratio for the fixed monthly micro-GU setup."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


BANDS = ["lt_1", "1_1.5", "1.5_2", "2_plus"]


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
        """select code, strftime(to_timestamp(date),'%Y%m%d')::integer ymd, v
           from daily_bars where code in (select unnest(?)) order by code,date""",
        [events.code.unique().tolist()],
    ).fetchdf()
    con.close()
    bars.code = bars.code.astype(str).str.zfill(4)
    bars["previous_20d_volume_median"] = bars.groupby("code").v.transform(
        lambda values: values.shift(1).rolling(20, min_periods=20).median()
    )
    bars["volume_ratio_20d_median"] = bars.v / bars.previous_20d_volume_median
    selected = events.merge(
        bars[["code", "ymd", "v", "previous_20d_volume_median", "volume_ratio_20d_median"]],
        left_on=["code", "signal_ymd"],
        right_on=["code", "ymd"],
        validate="one_to_one",
    )
    if selected.volume_ratio_20d_median.isna().any():
        raise ValueError("missing previous-20-day volume baseline")
    selected["volume_band"] = pd.cut(
        selected.volume_ratio_20d_median,
        [-np.inf, 1, 1.5, 2, np.inf],
        labels=BANDS,
        right=False,
    )
    selected.to_parquet(a.output / "monthly_micro_gu_volume_ratio_events.parquet", index=False)

    groups = {band: metrics(selected.loc[selected.volume_band.eq(band)]) for band in BANDS}
    low = metrics(selected.loc[selected.volume_ratio_20d_median.lt(1.5)])
    confirmed = metrics(selected.loc[selected.volume_ratio_20d_median.ge(1.5)])
    years = {
        str(year): {
            "lt_1.5": metrics(rows.loc[rows.volume_ratio_20d_median.lt(1.5)]),
            "ge_1.5": metrics(rows.loc[rows.volume_ratio_20d_median.ge(1.5)]),
        }
        for year, rows in selected.groupby("year")
    }
    boot = {
        "lt_1.5": bootstrap(
            selected.loc[selected.volume_ratio_20d_median.lt(1.5), "ret20_pct"], 20260719
        ),
        "ge_1.5": bootstrap(
            selected.loc[selected.volume_ratio_20d_median.ge(1.5), "ret20_pct"], 20260720
        ),
    }
    checks = {
        "confirmed_n_ge_20": confirmed["n"] >= 20,
        "confirmed_ret20_mean_gt_low": confirmed["ret20_mean"] > low["ret20_mean"],
        "confirmed_ret20_positive_rate_ge_0.75": confirmed["ret20_positive_rate"] >= 0.75,
        "confirmed_deep_drawdown_rate_le_0.25": confirmed["max_down20_le_minus5_rate"] <= 0.25,
        "confirmed_bootstrap_probability_gt_zero_ge_0.95": (
            boot["ge_1.5"]["probability_mean_gt_zero"] >= 0.95
        ),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_monthly_micro_gu_volume_ratio_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_volume_ratio",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "axis_changed": "signal-day volume / previous 20-session volume median only",
            "volume_bands": BANDS,
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "all_events": metrics(selected),
            "groups": groups,
            "threshold_comparison": {"lt_1.5": low, "ge_1.5": confirmed},
            "years": years,
            "bootstrap_ret20_mean": boot,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "fixed micro-GU events split only by volume expansion",
            "group_counts": {band: groups[band]["n"] for band in BANDS},
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_volume_ratio_ge_1.5" if keep else "hold_volume_axis",
            "authoritative_rollup_decision": (
                "keep_volume_ratio_ge_1.5_review_only" if keep else "hold"
            ),
            "reason_type": "participation_expansion_gates_passed" if keep else "gate_failed",
        },
        "not_changed": [
            "monthly range definition",
            "capitulation return threshold",
            "GU magnitude",
            "body ratio selection",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "small low-volume comparison group",
            "volume threshold is exploratory",
            "interaction with body strength not yet adopted",
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
        "ledger_sha256": sha(a.output / "monthly_micro_gu_volume_ratio_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {"output": str(a.output), "groups": groups, "threshold": {"lt_1.5": low, "ge_1.5": confirmed}, "checks": checks},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
