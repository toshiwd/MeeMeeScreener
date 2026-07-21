from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from tradex_long_fresh_family_events_v1 import FAMILIES, add_scores
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows, metrics


FEATURES = [
    "ret1", "ret5", "ret20", "ret60", "gap_ma20", "gap_ma60",
    "ma20_slope5", "ma60_slope5", "close_pos", "lower_wick_ratio",
    "upper_wick_ratio", "body_ratio", "volume_ratio20", "realized_vol20",
    "market_breadth_ma20", "market_advancers_ratio",
]
K_VALUES = [30, 50, 100, 200]
COST_PCT = 0.3


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--board", type=Path, required=True)
    parser.add_argument("--chart-review", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--code", default="6724")
    args = parser.parse_args()

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    board = json.loads(args.board.read_text(encoding="utf-8"))
    chart_review = json.loads(args.chart_review.read_text(encoding="utf-8"))
    target_code = str(args.code)
    target_board = next(row for row in board["authoritative_result"]["rows"] if str(row["code"]) == target_code)
    target_chart = next(row for row in chart_review["authoritative_result"]["rows"] if str(row["code"]) == target_code)
    target_family = target_board["family"]

    data = load_rows(str(args.db), broad_trigger=False, min_date="2016-01-01")
    data["signal_dt"] = pd.to_datetime(data.date, unit="s")
    data = add_scores(data)
    family_events = []
    for family in FAMILIES:
        selected = (data.sort_values(["date", family, "code"], ascending=[True, False, True])
                    .groupby("date", sort=False).head(3).copy())
        selected["family"] = family
        family_events.append(selected)
    selected = pd.concat(family_events, ignore_index=True)
    target_candidates = selected[(selected.code.astype(str) == target_code) &
                                 (selected.date == selected.date.max()) &
                                 (selected.family == target_family)]
    if target_candidates.empty:
        raise RuntimeError(f"target {target_code} is not a latest top-3 {target_family} event")
    target = target_candidates.iloc[0]

    pool = selected[(selected.family == target_family) & selected.p1_o.notna() &
                    selected.p20_c.notna() & (selected.date < target.date)].copy()
    pool["realized_ret"] = 100.0 * (pool.p20_c / pool.p1_o - 1.0) - COST_PCT
    development = pool[pool.signal_dt.dt.year.between(2016, 2023)]
    medians = development[FEATURES].median()
    scales = development[FEATURES].quantile(0.75) - development[FEATURES].quantile(0.25)
    scales = scales.mask(scales.abs() < 1e-12, development[FEATURES].std()).fillna(1.0)
    standardized = (pool[FEATURES].astype(float) - medians) / scales
    target_standardized = (target[FEATURES].astype(float) - medians) / scales
    pool["distance"] = np.sqrt(((standardized - target_standardized) ** 2).mean(axis=1))
    pool = pool.sort_values(["distance", "date", "code"])

    summaries = []
    for k in K_VALUES:
        nearest = pool.head(k)
        row = metrics(nearest)
        row["k"] = k
        row["max_distance"] = float(nearest.distance.max())
        summaries.append(row)
    stable_positive = all(row["mean_return_pct"] > 0 and row["win_rate"] >= 0.50 for row in summaries)
    severe_loss_min = min(row["severe_loss5_rate"] for row in summaries)
    severe_loss_max = max(row["severe_loss5_rate"] for row in summaries)
    starter_only = stable_positive and severe_loss_max >= 0.15
    checks = {
        "source_board_ready_review_only": board["judgment"]["authoritative_rollup_decision"] == "READY_REVIEW_ONLY",
        "source_chart_review_ready_review_only": chart_review["judgment"]["authoritative_rollup_decision"] == "READY_REVIEW_ONLY",
        "target_is_chart_starter": target_chart["status"] == "Starter",
        "development_scaler_is_2016_2023_only": int(development.signal_dt.dt.year.min()) == 2016 and int(development.signal_dt.dt.year.max()) == 2023,
        "all_k_have_full_sample": all(row["n"] == row["k"] for row in summaries),
        "all_k_mean_positive_and_win_at_least_half": stable_positive,
        "severe_loss_tail_requires_small_size": starter_only,
        "no_future_target_outcome_used": bool(pd.isna(target.p20_c)),
    }
    decision = "starter_review_only" if all(checks.values()) else "hold"
    nearest_examples = pool.head(20)[["code", "stock_name", "signal_dt", "distance", "realized_ret"]].copy()
    nearest_examples["signal_date"] = nearest_examples.signal_dt.dt.date.astype(str)

    payload = {
        "schema_version": "tradex_long_fresh_current_analogue_v1.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_as_of": board["latest_as_of"],
        "source_board": str(args.board),
        "source_chart_review": str(args.chart_review),
        "fixed_evaluation_conditions": {
            "target_code": target_code,
            "family": target_family,
            "features": FEATURES,
            "distance": "root mean squared robust-scaled distance",
            "scaler_period": "2016-2023 median and IQR",
            "analogue_pool": "same-family historical top-3 events from 2016 through latest matured event before target",
            "k_values": K_VALUES,
            "entry": "next session open",
            "outcome": "session-20 close",
            "round_trip_cost_pct": COST_PCT,
            "production_changed": False,
        },
        "authoritative_result": {
            "target_features": {name: float(target[name]) for name in FEATURES},
            "k_summaries": summaries,
            "stability_envelope": {
                "mean_return_pct_min": min(row["mean_return_pct"] for row in summaries),
                "mean_return_pct_max": max(row["mean_return_pct"] for row in summaries),
                "win_rate_min": min(row["win_rate"] for row in summaries),
                "win_rate_max": max(row["win_rate"] for row in summaries),
                "severe_loss5_rate_min": severe_loss_min,
                "severe_loss5_rate_max": severe_loss_max,
            },
            "nearest_20": nearest_examples.drop(columns="signal_dt").to_dict("records"),
            "checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": 0,
            "changed_top10_members_count": 0,
            "changed_rank_count": 0,
            "selection_divergence_reason": "current-case analogue validation only; ranking and candidate membership are unchanged",
        },
        "judgment": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": decision,
            "reason_type": "multi_axis_analogue_stability_with_material_loss_tail",
            "proposed_weight_pct": target_board["proposed_weight_pct"] if decision == "starter_review_only" else 0.0,
        },
        "remaining_risks": [
            "Nearest-neighbour evidence is observational and not a separately trained trading rule",
            "The severe-loss tail remains material across every fixed K",
            "The next-session opening price and gap are unknown",
        ],
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"decision": decision, "k_summaries": summaries, "checks": checks}, ensure_ascii=False))


if __name__ == "__main__":
    sys.path[:0] = [str(Path.cwd()), str(Path.cwd() / "scripts")]
    main()
