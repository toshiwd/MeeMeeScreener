from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb
import pandas as pd


AXIS_ID = "origin5541_dynamic_hedge_policy_v1"
SCHEMA_VERSION = "tradex_origin5541_dynamic_hedge_replay_v1.compare.v1"
CODE = "5541"
LOT_SIZE = 100
ARTICLE_REPORTED_PNL_YEN = 580_000

# Headline positions from the source article. Position notation is short-long.
# Unchanged article dates are intentionally omitted; the latest state is carried forward.
ARTICLE_POSITION_SCHEDULE: tuple[tuple[str, int, int], ...] = (
    ("2025-09-19", 1, 2),
    ("2025-09-22", 1, 5),
    ("2025-09-24", 1, 8),
    ("2025-09-25", 2, 10),
    ("2025-09-26", 2, 12),
    ("2025-09-29", 2, 17),
    ("2025-09-30", 6, 13),
    ("2025-10-01", 8, 14),
    ("2025-10-02", 9, 16),
    ("2025-10-06", 10, 18),
    ("2025-10-07", 5, 20),
    ("2025-10-08", 6, 22),
    ("2025-10-10", 6, 27),
    ("2025-10-14", 5, 28),
    ("2025-10-17", 8, 15),
    ("2025-10-21", 5, 17),
    ("2025-10-23", 5, 21),
    ("2025-10-24", 5, 26),
    ("2025-10-27", 5, 27),
    ("2025-10-28", 5, 25),
    ("2025-10-30", 6, 27),
    ("2025-11-07", 6, 30),
    ("2025-12-02", 8, 7),
    ("2025-12-03", 9, 9),
    ("2025-12-04", 8, 12),
    ("2025-12-05", 10, 7),
    ("2025-12-08", 11, 9),
    ("2025-12-09", 11, 5),
    ("2025-12-10", 6, 5),
    ("2025-12-11", 8, 3),
    ("2025-12-22", 6, 6),
    ("2025-12-23", 4, 7),
    ("2025-12-24", 6, 3),
    ("2025-12-25", 7, 2),
    ("2025-12-26", 0, 0),
)


@dataclass(frozen=True)
class Policy:
    policy_id: str
    short_units: Callable[[int, int], int]


POLICIES = (
    Policy("article_dynamic_hedge", lambda article_short, _long: article_short),
    Policy("no_hedge", lambda _article_short, _long: 0),
    Policy("fixed_50pct_hedge", lambda _article_short, long: math.floor(long * 0.5 + 0.5)),
    Policy("risk_equivalent_long_reduction", lambda _article_short, _long: 0),
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    return None if denominator == 0 else numerator / denominator


def load_prices(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        frame = conn.execute(
            """
            SELECT strftime(to_timestamp(date), '%Y-%m-%d') AS trade_date,
                   o, h, l, c, v, source
            FROM daily_bars
            WHERE code = ?
              AND date BETWEEN epoch(TIMESTAMP '2025-09-19') AND epoch(TIMESTAMP '2025-12-26')
            ORDER BY date
            """,
            [CODE],
        ).fetchdf()
    if frame.empty:
        raise RuntimeError("No 5541 bars found for the fixed replay period")
    if frame["trade_date"].duplicated().any():
        raise RuntimeError("Duplicate 5541 trade dates in runtime DB")
    expected = {row[0] for row in ARTICLE_POSITION_SCHEDULE}
    missing = sorted(expected - set(frame["trade_date"]))
    if missing:
        raise RuntimeError(f"Missing scheduled bars: {missing}")
    return frame


def replay_policy(prices: pd.DataFrame, policy: Policy) -> tuple[dict[str, Any], pd.DataFrame]:
    changes = {date: (short_units, long_units) for date, short_units, long_units in ARTICLE_POSITION_SCHEDULE}
    cash = 0.0
    short_units = 0
    long_units = 0
    peak_pnl = 0.0
    max_drawdown = 0.0
    turnover = 0.0
    peak_gross_notional = 0.0
    rows: list[dict[str, Any]] = []

    for bar in prices.itertuples(index=False):
        changed = bar.trade_date in changes
        article_short, target_long = changes.get(bar.trade_date, (short_units, long_units))
        if changed:
            if policy.policy_id == "risk_equivalent_long_reduction":
                article_net = target_long - article_short
                target_short = max(-article_net, 0)
                target_long = max(article_net, 0)
            else:
                target_short = policy.short_units(article_short, target_long)
            delta_long = target_long - long_units
            delta_short = target_short - short_units
            cash -= delta_long * LOT_SIZE * float(bar.c)
            cash += delta_short * LOT_SIZE * float(bar.c)
            turnover += (abs(delta_long) + abs(delta_short)) * LOT_SIZE * float(bar.c)
            long_units = target_long
            short_units = target_short

        pnl = cash + (long_units - short_units) * LOT_SIZE * float(bar.c)
        peak_pnl = max(peak_pnl, pnl)
        drawdown = pnl - peak_pnl
        max_drawdown = min(max_drawdown, drawdown)
        gross_notional = (long_units + short_units) * LOT_SIZE * float(bar.c)
        peak_gross_notional = max(peak_gross_notional, gross_notional)
        rows.append(
            {
                "policy_id": policy.policy_id,
                "trade_date": bar.trade_date,
                "close": float(bar.c),
                "position_changed": changed,
                "short_units": short_units,
                "long_units": long_units,
                "net_long_units": long_units - short_units,
                "hedge_ratio": None if long_units == 0 else short_units / long_units,
                "gross_notional_yen": gross_notional,
                "pnl_yen": pnl,
                "drawdown_yen": drawdown,
            }
        )

    curve = pd.DataFrame(rows)
    final_pnl = float(curve.iloc[-1]["pnl_yen"])
    return (
        {
            "policy_id": policy.policy_id,
            "final_pnl_yen": final_pnl,
            "max_drawdown_yen": float(max_drawdown),
            "peak_gross_notional_yen": float(peak_gross_notional),
            "turnover_notional_yen": float(turnover),
            "return_on_peak_gross_pct": None if peak_gross_notional == 0 else final_pnl / peak_gross_notional * 100,
            "position_change_count": len(ARTICLE_POSITION_SCHEDULE),
        },
        curve,
    )


def build_compare(prices: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    metrics: dict[str, dict[str, Any]] = {}
    curves: list[pd.DataFrame] = []
    for policy in POLICIES:
        policy_metrics, curve = replay_policy(prices, policy)
        metrics[policy.policy_id] = policy_metrics
        curves.append(curve)

    article = metrics["article_dynamic_hedge"]
    no_hedge = metrics["no_hedge"]
    fixed = metrics["fixed_50pct_hedge"]
    reduced = metrics["risk_equivalent_long_reduction"]
    replication_error = abs(article["final_pnl_yen"] - ARTICLE_REPORTED_PNL_YEN) / ARTICLE_REPORTED_PNL_YEN
    drawdown_ratio = _safe_ratio(abs(article["max_drawdown_yen"]), abs(no_hedge["max_drawdown_yen"]))
    drawdown_improvement = None if drawdown_ratio is None else 1 - drawdown_ratio
    profit_retention = _safe_ratio(article["final_pnl_yen"], no_hedge["final_pnl_yen"])
    fixed_profit_ratio = _safe_ratio(article["final_pnl_yen"], fixed["final_pnl_yen"])
    fixed_profit_advantage = None if fixed_profit_ratio is None else fixed_profit_ratio - 1
    changed_dates = sum(
        1
        for _date, article_short, long_units in ARTICLE_POSITION_SCHEDULE
        if article_short != math.floor(long_units * 0.5 + 0.5)
    )
    local_keep = (
        replication_error <= 0.20
        and drawdown_improvement is not None
        and drawdown_improvement >= 0.20
        and profit_retention is not None
        and profit_retention >= 0.80
    )

    def state_as_of(target_date: str) -> tuple[int, int]:
        state = (0, 0)
        for trade_date, short_units, long_units in ARTICLE_POSITION_SCHEDULE:
            if trade_date > target_date:
                break
            state = (short_units, long_units)
        return state

    capacity_anchors: dict[str, dict[str, Any]] = {}
    for anchor in ("2025-09-30", "2025-10-01", "2025-10-17", "2025-11-04", "2025-11-07"):
        article_short, article_long = state_as_of(anchor)
        equivalent_long = max(article_long - article_short, 0)
        capacity_anchors[anchor] = {
            "article_short_units": article_short,
            "article_long_units": article_long,
            "article_net_long_units": article_long - article_short,
            "risk_equivalent_long_only_units": equivalent_long,
            "preserved_core_long_units": article_long - equivalent_long,
            "article_long_inventory_advantage_pct": None
            if equivalent_long == 0
            else (article_long / equivalent_long - 1) * 100,
        }
    article_curve = pd.concat(curves, ignore_index=True)
    article_daily = article_curve[article_curve["policy_id"] == "article_dynamic_hedge"].reset_index(drop=True)
    reduced_daily = article_curve[article_curve["policy_id"] == "risk_equivalent_long_reduction"].reset_index(drop=True)
    max_daily_pnl_difference = float((article_daily["pnl_yen"] - reduced_daily["pnl_yen"]).abs().max())

    compare = {
        "schema_version": SCHEMA_VERSION,
        "axis_id": AXIS_ID,
        "artifact_role": "authoritative_single_ticker_dynamic_hedge_policy_replay",
        "review_only": True,
        "fixed_conditions": {
            "code": CODE,
            "period": {"start": "2025-09-19", "end": "2025-12-26"},
            "long_position_schedule": "article_headline_positions_fixed_for_all_policies",
            "execution": "same_day_confirmed_close_proxy",
            "lot_size": LOT_SIZE,
            "costs_slippage_borrow": "ignored",
            "price_source": "MeeMee runtime DB confirmed PAN daily_bars",
            "selection_logic_changed": False,
            "exit_dates_changed": False,
        },
        "policy_definitions": {
            "article_dynamic_hedge": "article headline short units",
            "no_hedge": "short units fixed at zero; article long schedule unchanged",
            "fixed_50pct_hedge": "short units rounded half-up to 50% of article long units",
            "risk_equivalent_long_reduction": "replace each article long-short state with the same net exposure using one side only",
        },
        "authoritative_results": metrics,
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "position-management axis only; selection is fixed to 5541",
            "article_vs_no_hedge_changed_position_dates": sum(1 for _, short, _ in ARTICLE_POSITION_SCHEDULE if short != 0),
            "article_vs_fixed50_changed_position_dates": changed_dates,
            "article_vs_risk_equivalent_changed_position_dates": sum(
                1 for _, short_units, long_units in ARTICLE_POSITION_SCHEDULE if short_units > 0 and long_units > 0
            ),
            "position_schedule_dates": len(ARTICLE_POSITION_SCHEDULE),
        },
        "comparison": {
            "article_reported_pnl_yen": ARTICLE_REPORTED_PNL_YEN,
            "article_proxy_replication_error_pct": replication_error * 100,
            "article_drawdown_improvement_vs_no_hedge_pct": None if drawdown_improvement is None else drawdown_improvement * 100,
            "article_profit_retention_vs_no_hedge_pct": None if profit_retention is None else profit_retention * 100,
            "article_profit_advantage_vs_fixed50_pct": None if fixed_profit_advantage is None else fixed_profit_advantage * 100,
            "pareto_interpretation": "article policy is an intermediate profit/drawdown frontier point; it does not dominate either extreme",
        },
        "capacity_retention_analysis": {
            "interpretation": "hedging preserves gross core-long inventory during corrections while controlling net exposure; it does not create extra net beta by itself",
            "risk_equivalent_final_pnl_yen": reduced["final_pnl_yen"],
            "max_daily_pnl_difference_vs_risk_equivalent_yen": max_daily_pnl_difference,
            "zero_cost_net_exposure_equivalence_confirmed": max_daily_pnl_difference <= 0.01,
            "breakout_decision_date": "2025-11-04",
            "breakout_article_long_units": capacity_anchors["2025-11-04"]["article_long_units"],
            "breakout_article_short_units": capacity_anchors["2025-11-04"]["article_short_units"],
            "breakout_net_long_units": capacity_anchors["2025-11-04"]["article_net_long_units"],
            "breakout_risk_equivalent_long_units": capacity_anchors["2025-11-04"]["risk_equivalent_long_only_units"],
            "breakout_preserved_core_long_units": capacity_anchors["2025-11-04"]["preserved_core_long_units"],
            "breakout_long_inventory_advantage_pct": capacity_anchors["2025-11-04"]["article_long_inventory_advantage_pct"],
            "anchor_states": capacity_anchors,
        },
        "judgment": {
            "candidate_local_decision": "keep_for_broader_validation" if local_keep else "hold",
            "session_aggregate_decision": "hold_single_ticker_only",
            "authoritative_rollup_decision": "hold_review_only_pending_multi_ticker_validation",
            "reason_type": "replicated_pnl_drawdown_and_core_long_inventory_retention_but_single_case" if local_keep else "single_ticker_gate_failed",
            "keep_conditions": {
                "reported_pnl_replication_error_le_20pct": replication_error <= 0.20,
                "drawdown_improvement_vs_no_hedge_ge_20pct": drawdown_improvement is not None and drawdown_improvement >= 0.20,
                "profit_retention_vs_no_hedge_ge_80pct": profit_retention is not None and profit_retention >= 0.80,
                "breakout_core_long_units_preserved_ge_1": capacity_anchors["2025-11-04"]["preserved_core_long_units"] >= 1,
                "risk_equivalent_pnl_parity_verified": max_daily_pnl_difference <= 0.01,
            },
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production trading logic", "candidate selection"],
        "remaining_risks": [
            "same-day close is a proxy for unknown article execution prices",
            "article long schedule was influenced by its hedge state, so counterfactual policies are not fully causal",
            "single successful ticker cannot establish cross-sectional usefulness",
            "article text contains at least one likely side-label typo on 2025-09-30",
            "without costs, tax, margin constraints, or execution differences, a hedged book and its one-sided net equivalent have identical mark-to-market PnL",
        ],
    }
    return compare, article_curve


def run(db_path: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    prices = load_prices(db_path)
    compare, curve = build_compare(prices)
    curve_path = output / "policy_equity_curve.csv"
    schedule_path = output / "article_position_schedule.csv"
    compare_path = output / "compare.json"
    audit_path = output / "audit.json"
    complete_path = output / "_ARTIFACT_COMPLETE.json"
    curve.to_csv(curve_path, index=False)
    pd.DataFrame(ARTICLE_POSITION_SCHEDULE, columns=["trade_date", "short_units", "long_units"]).to_csv(schedule_path, index=False)
    _write_json(compare_path, compare)
    audit = {
        "schema_version": "tradex_origin5541_dynamic_hedge_replay_v1.audit.v1",
        "generated_at": _utc_now(),
        "db_path": str(db_path.resolve()),
        "db_read_only": True,
        "daily_bar_rows": len(prices),
        "confirmed_source_values": sorted(prices["source"].dropna().unique().tolist()),
        "schedule_rows": len(ARTICLE_POSITION_SCHEDULE),
        "policy_count": len(POLICIES),
        "future_used_for_selection": False,
        "review_only": True,
    }
    _write_json(audit_path, audit)
    _write_json(
        complete_path,
        {
            "complete": True,
            "authoritative": "compare.json",
            "compare_sha256": _sha256(compare_path),
            "audit_sha256": _sha256(audit_path),
            "equity_curve_sha256": _sha256(curve_path),
            "schedule_sha256": _sha256(schedule_path),
        },
    )
    return {"output": str(output.resolve()), "judgment": compare["judgment"], "results": compare["authoritative_results"]}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(json.dumps(run(args.db, args.output), ensure_ascii=False, indent=2))
