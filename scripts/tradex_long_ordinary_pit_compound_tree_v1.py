from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor, _tree


FEATURES = [
    "ret1", "ret3", "ret5", "ret10", "ret20", "ret60", "range20", "range60",
    "dist_low20", "dist_high20", "close_pos", "gap_ma20", "gap_ma60",
    "ma20_slope5", "ma60_slope5", "lower_wick_ratio", "upper_wick_ratio",
    "body_ratio", "volume_ratio20", "realized_vol20", "market_breadth_ma20",
    "market_mean_ret1", "market_advancers_ratio", "market_dispersion_ret1",
]


def dedupe(frame: pd.DataFrame) -> pd.DataFrame:
    kept: list[int] = []
    for _, group in frame.sort_values(["code", "bar_index"]).groupby("code", sort=False):
        last = -10**9
        for index, bar_index in zip(group.index, group["bar_index"]):
            if int(bar_index) - last > 5:
                kept.append(index)
                last = int(bar_index)
    return frame.loc[kept].copy()


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0, "codes": 0, "mean_return_pct": None, "median_return_pct": None,
                "win_rate": None, "severe_loss5_rate": None, "top3_positive_profit_share": None}
    positive = frame.loc[frame.realized_ret > 0, "realized_ret"]
    total_positive = float(positive.sum())
    return {
        "n": int(len(frame)), "codes": int(frame.code.nunique()),
        "mean_return_pct": float(frame.realized_ret.mean()),
        "median_return_pct": float(frame.realized_ret.median()),
        "win_rate": float(frame.realized_ret.gt(0).mean()),
        "severe_loss5_rate": float(frame.realized_ret.le(-5).mean()),
        "top3_positive_profit_share": None if total_positive <= 0 else float(positive.nlargest(3).sum() / total_positive),
    }


def tree_paths(tree: DecisionTreeRegressor) -> dict[int, list[str]]:
    t = tree.tree_
    result: dict[int, list[str]] = {}
    def walk(node: int, clauses: list[str]) -> None:
        if t.feature[node] == _tree.TREE_UNDEFINED:
            result[int(node)] = clauses
            return
        feature = FEATURES[t.feature[node]]
        threshold = float(t.threshold[node])
        walk(t.children_left[node], clauses + [f"{feature} <= {threshold:.10g}"])
        walk(t.children_right[node], clauses + [f"{feature} > {threshold:.10g}"])
    walk(0, [])
    return result


def load_rows(db_path: str, *, broad_trigger: bool = True, min_date: str | None = None) -> pd.DataFrame:
    trigger_clause = "AND f.ret3 BETWEEN 0.03 AND 0.20 AND f.c > f.ma20 AND f.range20 > 0.03" if broad_trigger else ""
    date_clause = f"AND f.date >= epoch(strptime('{min_date}', '%Y-%m-%d'))" if min_date else ""
    query = f"""
    WITH raw AS (
      SELECT b.code, b.date, b.o, b.h, b.l, b.c, b.v,
        row_number() OVER (PARTITION BY b.code ORDER BY b.date) AS bar_index,
        lag(b.c, 1) OVER w AS c1, lag(b.c, 3) OVER w AS c3,
        lag(b.c, 5) OVER w AS c5, lag(b.c, 10) OVER w AS c10,
        lag(b.c, 20) OVER w AS c20, lag(b.c, 60) OVER w AS c60,
        min(b.l) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS low20,
        max(b.h) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high20,
        min(b.l) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
        max(b.h) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
        avg(b.c) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
        avg(b.c) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING) AS ma20_5ago,
        avg(b.c) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
        avg(b.c) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 64 PRECEDING AND 5 PRECEDING) AS ma60_5ago,
        avg(b.v) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol20,
        lead(b.date, 1) OVER w AS p1_date,
        lead(b.o, 1) OVER w AS p1_o, lead(b.h, 1) OVER w AS p1_h,
        lead(b.l, 1) OVER w AS p1_l, lead(b.c, 1) OVER w AS p1_c,
        lead(b.o, 2) OVER w AS p2_o, lead(b.h, 2) OVER w AS p2_h,
        lead(b.l, 2) OVER w AS p2_l, lead(b.c, 2) OVER w AS p2_c,
        lead(b.o, 3) OVER w AS p3_o, lead(b.h, 3) OVER w AS p3_h,
        lead(b.l, 3) OVER w AS p3_l, lead(b.c, 3) OVER w AS p3_c,
        lead(b.o, 4) OVER w AS p4_o, lead(b.h, 4) OVER w AS p4_h,
        lead(b.l, 4) OVER w AS p4_l, lead(b.c, 4) OVER w AS p4_c,
        lead(b.o, 5) OVER w AS p5_o, lead(b.h, 5) OVER w AS p5_h,
        lead(b.l, 5) OVER w AS p5_l, lead(b.c, 5) OVER w AS p5_c,
        lead(b.c, 10) OVER w AS p10_c, lead(b.c, 20) OVER w AS p20_c,
        lead(b.date, 3) OVER w AS p3_date, lead(b.date, 5) OVER w AS p5_date,
        lead(b.date, 10) OVER w AS p10_date, lead(b.date, 20) OVER w AS p20_date
      FROM daily_bars b
      WINDOW w AS (PARTITION BY b.code ORDER BY b.date)
    ), features AS (
      SELECT *,
        c / c1 - 1 AS ret1, c / c3 - 1 AS ret3, c / c5 - 1 AS ret5,
        c / c10 - 1 AS ret10, c / c20 - 1 AS ret20, c / c60 - 1 AS ret60,
        high20 / low20 - 1 AS range20, high60 / low60 - 1 AS range60,
        c / low20 - 1 AS dist_low20, c / high20 - 1 AS dist_high20,
        (c - l) / nullif(h - l, 0) AS close_pos,
        c / ma20 - 1 AS gap_ma20, c / ma60 - 1 AS gap_ma60,
        ma20 / ma20_5ago - 1 AS ma20_slope5, ma60 / ma60_5ago - 1 AS ma60_slope5,
        (least(o, c) - l) / nullif(h - l, 0) AS lower_wick_ratio,
        (h - greatest(o, c)) / nullif(h - l, 0) AS upper_wick_ratio,
        abs(c - o) / nullif(h - l, 0) AS body_ratio,
        v / nullif(vol20, 0) AS volume_ratio20,
        stddev_samp(c / nullif(c1, 0) - 1) OVER (
          PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS realized_vol20
      FROM raw
    ), breadth AS (
      SELECT date, avg(CASE WHEN c > ma20 THEN 1.0 ELSE 0.0 END) AS market_breadth_ma20,
        avg(ret1) AS market_mean_ret1,
        avg(CASE WHEN ret1 > 0 THEN 1.0 ELSE 0.0 END) AS market_advancers_ratio,
        stddev_samp(ret1) AS market_dispersion_ret1
      FROM features
      JOIN industry_master breadth_industry USING (code)
      WHERE breadth_industry.market_code IN (
        'プライム（内国株式）',
        'スタンダード（内国株式）',
        'グロース（内国株式）'
      )
        AND coalesce(breadth_industry.name, '') NOT LIKE '%種類株%'
        AND coalesce(breadth_industry.name, '') NOT LIKE '%優先株%'
      GROUP BY date
    )
    SELECT f.*, br.market_breadth_ma20, br.market_mean_ret1, br.market_advancers_ratio,
      br.market_dispersion_ret1, coalesce(i.name, m.name, '') AS stock_name
    FROM features f
    JOIN breadth br USING (date)
    LEFT JOIN industry_master i USING (code)
    LEFT JOIN stock_meta m USING (code)
    WHERE i.market_code IN (
      'プライム（内国株式）',
      'スタンダード（内国株式）',
      'グロース（内国株式）'
    )
      AND coalesce(i.name, '') NOT LIKE '%種類株%'
      AND coalesce(i.name, '') NOT LIKE '%優先株%'
      AND f.c >= 100 AND f.v > 0
      {trigger_clause}
      {date_clause}
      AND abs(f.ret1) < 0.50
    """
    with duckdb.connect(db_path, read_only=True) as conn:
        return conn.execute(query).fetchdf()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--exit-session", type=int, choices=[3, 5, 10], default=3)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    sys.path[:0] = [str(Path.cwd()), str(Path.cwd() / "app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime = get_runtime_stock_db_status()
    data = load_rows(runtime["selected_runtime_db_path"])
    exit_column = {3: "p3_c", 5: "p5_c", 10: "p10_c"}[args.exit_session]
    exit_date_column = {3: "p3_date", 5: "p5_date", 10: "p10_date"}[args.exit_session]
    data = data[data[exit_column].notna()].copy()
    data["exit_date"] = pd.to_datetime(data[exit_date_column], unit="s")
    data["signal_date"] = pd.to_datetime(data.date, unit="s")
    data["year"] = data.signal_date.dt.year
    entry = data.p1_o
    immediate = data.p1_h.div(entry).sub(1).mul(100).ge(3) & data.p1_l.div(entry).sub(1).mul(100).gt(-3)
    stall = (data.p1_c.div(entry).sub(1).mul(100).le(-2) | data.p1_l.div(entry).sub(1).mul(100).le(-3)) & ~immediate
    exit_close = data[exit_column]
    data["realized_ret"] = 25 * (exit_close / entry - 1)
    data.loc[immediate, "realized_ret"] = 100 * (
        .25 * (exit_close.loc[immediate] / entry.loc[immediate] - 1)
        + .75 * (exit_close.loc[immediate] / data.loc[immediate, "p2_o"] - 1)
    )
    data.loc[stall, "realized_ret"] = 25 * (data.loc[stall, "p2_o"] / entry.loc[stall] - 1)

    discovery = data.year.between(2019, 2024)
    imputer = SimpleImputer(strategy="median")
    x_train = imputer.fit_transform(data.loc[discovery, FEATURES])
    tree = DecisionTreeRegressor(max_depth=5, min_samples_leaf=3000, random_state=20260720)
    tree.fit(x_train, data.loc[discovery, "realized_ret"])
    data["leaf"] = tree.apply(imputer.transform(data[FEATURES])).astype(int)
    paths = tree_paths(tree)

    discovery_rows = []
    validation_rows = []
    for leaf, frame in data.groupby("leaf"):
        rule = paths[int(leaf)]
        discovery_rows.append({"leaf": int(leaf), "rule": rule, **metrics(dedupe(frame[frame.year.between(2019, 2024)]))})
        validation_rows.append({"leaf": int(leaf), "rule": rule, **metrics(dedupe(frame[frame.year.eq(2025)]))})
    eligible = []
    for row in validation_rows:
        discovery_row = next(item for item in discovery_rows if item["leaf"] == row["leaf"])
        discovery_years = [
            metrics(dedupe(data[data.year.eq(year) & data.leaf.eq(row["leaf"])]))
            for year in range(2019, 2025)
        ]
        distinct = len({clause.split()[0] for clause in row["rule"]})
        if (distinct >= 2 and row["n"] >= 250 and (row["mean_return_pct"] or -99) > 0
                and (row["win_rate"] or 0) >= .50 and (row["severe_loss5_rate"] or 1) <= .03
                and (row["top3_positive_profit_share"] or 1) <= .35
                and (discovery_row["mean_return_pct"] or -99) > 0
                and sum((item["mean_return_pct"] or -99) > 0 for item in discovery_years) >= 5):
            eligible.append(row["leaf"])
    test = dedupe(data[data.year.eq(2026) & data.leaf.isin(eligible)])
    test_metrics = metrics(test)
    monthly = {str(month): metrics(group) for month, group in test.groupby(test.signal_date.dt.to_period("M"))}
    year_metrics = {str(year): metrics(dedupe(data[data.year.eq(year) & data.leaf.isin(eligible)])) for year in range(2019, 2027)}
    positive_months = sum((m["mean_return_pct"] or -99) > 0 for m in monthly.values())
    checks = {
        "validation_selected_without_2026": bool(eligible),
        "test_n_at_least_250": test_metrics["n"] >= 250,
        "test_mean_positive": (test_metrics["mean_return_pct"] or -99) > 0,
        "test_win_rate_at_least_50pct": (test_metrics["win_rate"] or 0) >= .50,
        "test_severe_loss5_at_most_3pct": (test_metrics["severe_loss5_rate"] or 1) <= .03,
        "test_top3_profit_share_at_most_35pct": (test_metrics["top3_positive_profit_share"] or 1) <= .35,
        "test_months_majority_positive": bool(monthly) and positive_months / len(monthly) >= .70,
        "every_year_positive": all((row["mean_return_pct"] or -99) > 0 for row in year_metrics.values()),
    }
    decision = "hold_for_portfolio_gate" if all(checks.values()) else "drop"
    payload = {
        "schema_version": "tradex_long_ordinary_pit_compound_tree_v1.compare.v1",
        "artifact_role": "authoritative", "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime": runtime,
        "fixed_evaluation_conditions": {
            "universe": "PAN ordinary stocks; ETF/ETN excluded", "broad_trigger": "ret3 3%-20%, close>MA20, range20>3%",
            "discovery": "2019-2024", "validation_selection": "2025", "untouched_test": "2026 through latest mature signal",
            "features": FEATURES, "tree": {"max_depth": 5, "min_samples_leaf": 3000, "random_state": 20260720},
            "compound_gate": "two or more distinct features plus positive discovery aggregate and at least 5 of 6 discovery years",
            "execution": f"next-open practical staged management; exit session {args.exit_session} close",
            "dedupe": "same code within 5 trading rows keep earliest", "costs": "ignored by standing research contract",
        },
        "authoritative_result": {"eligible_leaves": eligible, "discovery_leaves": discovery_rows,
            "validation_leaves": validation_rows, "test_2026": test_metrics, "monthly_2026": monthly,
            "year_metrics": year_metrics, "checks": checks},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int(len(test)), "selection_divergence_reason": "train-only multi-feature chart-state leaves"},
        "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": decision,
            "reason_type": "strict_point_in_time_event_gate"},
        "non_scope": ["portfolio capital allocation", "MeeMee reflection", "ranking mutation", "runtime DB write"],
        "remaining_risks": ["portfolio gate pending if event gate passes", "corporate action audit limited to one-day 50pct cap"],
    }
    test.to_parquet(output / "test_signal_ledger.parquet", index=False)
    data[data.leaf.isin(eligible)].to_parquet(output / "selected_history_ledger.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"eligible": eligible, "test": test_metrics, "checks": checks, "decision": decision}, ensure_ascii=False))


if __name__ == "__main__":
    main()
