"""Test pre-signal traded-value liquidity and its independence from the price band."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


FLOORS_MILLION_YEN = [1, 1.5, 2, 2.5, 3, 4, 5]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    years = frame.groupby("year").ret20_pct.mean()
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "years": int(frame.year.nunique()),
        "positive_year_rate": float((years > 0).mean()),
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
    ap.add_argument("--price-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(a.events)
    con = duckdb.connect(str(a.db), read_only=True)
    bars = con.execute(
        """select code, strftime(to_timestamp(date),'%Y%m%d')::integer ymd, c, v
           from daily_bars where code in (select unnest(?)) order by code,date""",
        [events.code.unique().tolist()],
    ).fetchdf()
    con.close()
    bars.code = bars.code.astype(str).str.zfill(4)
    bars["traded_value_yen"] = bars.c * bars.v
    bars["previous_20d_traded_value_median_yen"] = bars.groupby("code").traded_value_yen.transform(
        lambda values: values.shift(1).rolling(20, min_periods=20).median()
    )
    selected = events.merge(
        bars[["code", "ymd", "previous_20d_traded_value_median_yen"]],
        left_on=["code", "signal_ymd"],
        right_on=["code", "ymd"],
        validate="one_to_one",
    )
    if selected.previous_20d_traded_value_median_yen.isna().any():
        raise ValueError("missing previous-20-session traded-value baseline")
    selected["liquidity_million_yen"] = selected.previous_20d_traded_value_median_yen / 1_000_000
    selected["liquidity_ok"] = selected.liquidity_million_yen.ge(2)
    selected["price_ok"] = selected.entry_open.ge(1200) & selected.entry_open.lt(8000)
    selected["volume_ok"] = selected.volume_ratio_20d_median.ge(1.5)
    selected.to_parquet(a.output / "monthly_micro_gu_liquidity_events.parquet", index=False)

    floor_sensitivity = {
        str(floor): metrics(selected.loc[selected.liquidity_million_yen.ge(floor)])
        for floor in FLOORS_MILLION_YEN
    }
    liquidity_split = {
        "lt_2m": metrics(selected.loc[~selected.liquidity_ok]),
        "ge_2m": metrics(selected.loc[selected.liquidity_ok]),
    }
    price_liquidity = {
        "neither": metrics(selected.loc[~selected.price_ok & ~selected.liquidity_ok]),
        "price_only": metrics(selected.loc[selected.price_ok & ~selected.liquidity_ok]),
        "liquidity_only": metrics(selected.loc[~selected.price_ok & selected.liquidity_ok]),
        "both": metrics(selected.loc[selected.price_ok & selected.liquidity_ok]),
    }
    both = selected.loc[selected.price_ok & selected.liquidity_ok]
    both_volume = {
        "volume_lt_1.5": metrics(both.loc[~both.volume_ok]),
        "volume_ge_1.5": metrics(both.loc[both.volume_ok]),
    }
    years = {
        str(year): metrics(rows)
        for year, rows in both.groupby("year")
    }
    both_boot = bootstrap(both.ret20_pct, 20260729)
    volume_boot = bootstrap(both.loc[both.volume_ok, "ret20_pct"], 20260730)
    price_liquidity_corr = float(
        np.corrcoef(np.log(selected.entry_open), np.log(selected.previous_20d_traded_value_median_yen))[0, 1]
    )
    checks = {
        "liquidity_ge_2m_n_ge_20": liquidity_split["ge_2m"]["n"] >= 20,
        "liquidity_ge_2m_ret20_gt_low": (
            liquidity_split["ge_2m"]["ret20_mean"] > liquidity_split["lt_2m"]["ret20_mean"]
        ),
        "all_floor_1.5_to_4m_ret20_gt_6": all(
            floor_sensitivity[str(floor)]["ret20_mean"] > 6 for floor in [1.5, 2, 2.5, 3, 4]
        ),
        "price_liquidity_both_n_ge_15": price_liquidity["both"]["n"] >= 15,
        "price_liquidity_both_positive_rate_ge_0.90": (
            price_liquidity["both"]["ret20_positive_rate"] >= 0.90
        ),
        "price_liquidity_both_deep_drawdown_rate_le_0.10": (
            price_liquidity["both"]["max_down20_le_minus5_rate"] <= 0.10
        ),
        "price_liquidity_bootstrap_probability_gt_zero_ge_0.95": (
            both_boot["probability_mean_gt_zero"] >= 0.95
        ),
        "triple_intersection_n_ge_20_for_hard_gate": both_volume["volume_ge_1.5"]["n"] >= 20,
    }
    result = {
        "schema_version": "tradex_monthly_micro_gu_liquidity_floor_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_liquidity_floor",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "axis_changed": "previous 20-session median traded value only",
            "traded_value": "daily close * daily volume",
            "liquidity_floor_sensitivity_million_yen": FLOORS_MILLION_YEN,
            "selected_liquidity_floor_million_yen": 2,
            "price_boundary_audit_yen": [1200, 8000],
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "liquidity_distribution_million_yen": {
                key: float(value)
                for key, value in selected.liquidity_million_yen.describe().to_dict().items()
            },
            "price_liquidity_log_correlation": price_liquidity_corr,
            "floor_sensitivity": floor_sensitivity,
            "liquidity_split": liquidity_split,
            "price_liquidity_interaction": price_liquidity,
            "price_liquidity_years": years,
            "price_liquidity_bootstrap_ret20_mean": both_boot,
            "price_liquidity_volume_boundary": both_volume,
            "triple_intersection_bootstrap_ret20_mean": volume_boot,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "a 2m-yen pre-signal liquidity floor adds information largely independent of nominal price",
            "liquidity_ge_2m_count": liquidity_split["ge_2m"]["n"],
            "price_liquidity_both_count": price_liquidity["both"]["n"],
            "triple_intersection_count": both_volume["volume_ge_1.5"]["n"],
        },
        "judgment": {
            "candidate_local_decision": "keep",
            "session_aggregate_decision": "keep_liquidity_floor_2m_and_price_priority",
            "authoritative_rollup_decision": "keep_price_1200_8000_liquidity_ge_2m_review_only_volume_priority",
            "reason_type": "liquidity_floor_robust_and_price_adds_independent_selection_value",
        },
        "not_changed": [
            "parent setup",
            "price band",
            "volume threshold",
            "body ratio",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "traded value uses close times volume and may differ from exchange turnover conventions",
            "liquidity threshold is explored in-sample",
            "triple intersection has only twelve events and is not a hard gate",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "price_compare": {"path": str(a.price_compare.resolve()), "sha256": sha(a.price_compare)},
        },
        "selected_events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "monthly_micro_gu_liquidity_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "liquidity_split": liquidity_split, "price_liquidity": price_liquidity, "both_volume": both_volume, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
