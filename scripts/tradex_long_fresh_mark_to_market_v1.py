from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from tradex_long_trend_pullback_portfolio_v1 import COST, simulate


DEFAULT_EVENTS = Path(
    r"G:\Tradex\tradex_long_fresh_family_events_v1\20260720T-authoritative-v4\fresh_family_events.parquet"
)
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
VOL_CAP = 0.015
BASE_WEIGHT = 0.05
MAX_POSITIONS = 20


def accepted_trades(events_path: Path) -> pd.DataFrame:
    events = pd.read_parquet(events_path).rename(
        columns={
            "p1_date": "entry_date",
            "p1_o": "entry_price",
            "p20_date": "exit_date",
            "p20_c": "exit_price",
        }
    )
    events["rank"] = -events.family_score
    trades = simulate(events, MAX_POSITIONS).reset_index(drop=True)
    trades["trade_id"] = trades.index.astype("int64")
    trades["equal_weight"] = BASE_WEIGHT
    trades["vol_cap_weight"] = BASE_WEIGHT * (
        VOL_CAP / trades.realized_vol20.clip(lower=0.001)
    ).clip(upper=1.0)
    return trades


def load_marks(db_path: Path, trades: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    keys = trades[["trade_id", "code", "entry_date", "exit_date"]].copy()
    with duckdb.connect(str(db_path), read_only=True) as conn:
        conn.register("trade_keys", keys)
        marks = conn.execute(
            """
            SELECT t.trade_id, b.date, b.c AS close
            FROM trade_keys t
            JOIN daily_bars b
              ON b.code = t.code
             AND b.date BETWEEN t.entry_date AND t.exit_date
            ORDER BY b.date, t.trade_id
            """
        ).fetchdf()
        calendar = conn.execute(
            """
            SELECT DISTINCT date
            FROM daily_bars
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            [int(trades.entry_date.min()), int(trades.exit_date.max())],
        ).fetchdf()
    return marks, calendar


def nav_series(
    trades: pd.DataFrame, marks: pd.DataFrame, calendar: pd.DataFrame, weight_col: str
) -> tuple[pd.DataFrame, dict]:
    t = trades.set_index("trade_id")
    m = marks.join(t[["entry_price", weight_col]], on="trade_id")
    m["open_pnl"] = m[weight_col] * (m.close / m.entry_price - 1.0)
    open_pnl = m.groupby("date").open_pnl.sum()

    realized = trades.copy()
    realized["realized_pnl"] = realized[weight_col] * (
        realized.exit_price / realized.entry_price - 1.0 - COST / 100.0
    )
    realized_by_day = realized.groupby("exit_date").realized_pnl.sum()
    exit_cost_by_day = trades.assign(exit_cost=trades[weight_col] * COST / 100.0).groupby("exit_date").exit_cost.sum()

    rows = []
    cumulative_realized = 0.0
    for day in calendar.date.astype("int64"):
        # Positions remain marked through the exit close. Their realized P&L is
        # carried from the next trading date, avoiding double counting on exit.
        active = trades[(trades.entry_date <= day) & (trades.exit_date >= day)]
        nav = 1.0 + cumulative_realized + float(open_pnl.get(day, 0.0)) - float(exit_cost_by_day.get(day, 0.0))
        rows.append(
            {
                "date": day,
                "nav": nav,
                "active_positions": int(len(active)),
                "gross_exposure": float(active[weight_col].sum()),
            }
        )
        cumulative_realized += float(realized_by_day.get(day, 0.0))

    daily = pd.DataFrame(rows)
    terminal = 1.0 + float(realized.realized_pnl.sum())
    if abs(float(daily.nav.iloc[-1]) - terminal) > 1e-10:
        raise RuntimeError("terminal NAV does not reconcile to realized trade ledger")
    daily["dt"] = pd.to_datetime(daily.date, unit="s")
    daily["peak_nav"] = daily.nav.cummax()
    daily["drawdown"] = daily.nav / daily.peak_nav - 1.0

    month_end = daily.groupby(daily.dt.dt.to_period("M")).tail(1).copy()
    month_end["return"] = month_end.nav.pct_change()
    first_month = month_end.index[0]
    month_end.loc[first_month, "return"] = month_end.loc[first_month, "nav"] - 1.0
    year_end = daily.groupby(daily.dt.dt.year).tail(1).copy()
    year_end["return"] = year_end.nav.pct_change()
    first_year = year_end.index[0]
    year_end.loc[first_year, "return"] = year_end.loc[first_year, "nav"] - 1.0

    summary = {
        "trades": int(len(trades)),
        "terminal_return_pct": 100.0 * (terminal - 1.0),
        "max_drawdown_pct": 100.0 * float(daily.drawdown.min()),
        "max_positions": int(daily.active_positions.max()),
        "max_gross_exposure_pct": 100.0 * float(daily.gross_exposure.max()),
        "mean_gross_exposure_pct": 100.0 * float(daily.gross_exposure.mean()),
        "positive_month_rate": float((month_end["return"] > 0).mean()),
        "positive_year_rate": float((year_end["return"] > 0).mean()),
        "worst_month_pct": 100.0 * float(month_end["return"].min()),
        "worst_year_pct": 100.0 * float(year_end["return"].min()),
        "monthly_returns_pct": {
            str(p): 100.0 * float(r)
            for p, r in zip(month_end.dt.dt.to_period("M"), month_end["return"])
        },
        "yearly_returns_pct": {
            str(y): 100.0 * float(r)
            for y, r in zip(year_end.dt.dt.year, year_end["return"])
        },
    }
    return daily.drop(columns=["peak_nav"]), summary


def period_summary(daily: pd.DataFrame, start_year: int, end_year: int) -> dict:
    d = daily[daily.dt.dt.year.between(start_year, end_year)].copy()
    if d.empty:
        return {}
    start_nav = float(d.nav.iloc[0])
    d["period_nav"] = d.nav / start_nav
    d["peak"] = d.period_nav.cummax()
    d["dd"] = d.period_nav / d.peak - 1.0
    month_end = d.groupby(d.dt.dt.to_period("M")).tail(1).copy()
    month_end["ret"] = month_end.nav.pct_change()
    month_end.loc[month_end.index[0], "ret"] = month_end.nav.iloc[0] / start_nav - 1.0
    year_end = d.groupby(d.dt.dt.year).tail(1).copy()
    year_end["ret"] = year_end.nav.pct_change()
    year_end.loc[year_end.index[0], "ret"] = year_end.nav.iloc[0] / start_nav - 1.0
    return {
        "start_year": start_year,
        "end_year": end_year,
        "return_pct": 100.0 * (float(d.nav.iloc[-1]) / start_nav - 1.0),
        "max_drawdown_pct": 100.0 * float(d.dd.min()),
        "positive_month_rate": float((month_end.ret > 0).mean()),
        "positive_year_rate": float((year_end.ret > 0).mean()),
        "worst_month_pct": 100.0 * float(month_end.ret.min()),
        "max_gross_exposure_pct": 100.0 * float(d.gross_exposure.max()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)

    trades = accepted_trades(args.events)
    marks, calendar = load_marks(args.db, trades)
    equal_daily, equal_all = nav_series(trades, marks, calendar, "equal_weight")
    risk_daily, risk_all = nav_series(trades, marks, calendar, "vol_cap_weight")

    periods = {"development": (2016, 2023), "validation": (2024, 2025), "audit_2026": (2026, 2026)}
    equal_periods = {k: period_summary(equal_daily, *v) for k, v in periods.items()}
    risk_periods = {k: period_summary(risk_daily, *v) for k, v in periods.items()}
    checks = {
        "terminal_reconciles_equal": abs(equal_all["terminal_return_pct"] - float((trades.raw_return_pct * BASE_WEIGHT).sum())) < 1e-9,
        "terminal_reconciles_vol_cap": abs(risk_all["terminal_return_pct"] - float((trades.raw_return_pct * trades.vol_cap_weight).sum())) < 1e-9,
        "positions_at_most_20": risk_all["max_positions"] <= MAX_POSITIONS,
        "exposure_at_most_100pct": risk_all["max_gross_exposure_pct"] <= 100.0 + 1e-9,
        "positive_return_all_periods": all(v["return_pct"] > 0 for v in risk_periods.values()),
        "positive_months_majority_all_periods": all(v["positive_month_rate"] > 0.5 for v in risk_periods.values()),
        "drawdown_improves_each_period": all(
            risk_periods[k]["max_drawdown_pct"] > equal_periods[k]["max_drawdown_pct"] for k in periods
        ),
        "worst_month_improves_each_period": all(
            risk_periods[k]["worst_month_pct"] > equal_periods[k]["worst_month_pct"] for k in periods
        ),
    }
    decision = "keep_for_final_adoption_audit" if all(checks.values()) else "drop"
    payload = {
        "schema_version": "tradex_long_fresh_mark_to_market_v1.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixed_evaluation_conditions": {
            "events": str(args.events),
            "runtime_db": str(args.db),
            "universe": "ordinary domestic stocks only inherited from event ledger",
            "entry": "next session open",
            "exit": "session-20 close",
            "round_trip_cost_pct": COST,
            "max_positions": MAX_POSITIONS,
            "baseline": "same accepted trades, equal 5% initial-capital allocation",
            "challenger": "same accepted trades, 5% base scaled by 1.5% / realized volatility 20d; residual cash",
            "daily_valuation": "initial-capital fixed weights; close mark-to-market; cost charged at exit",
            "axis_changed": "capital allocation only",
            "production_changed": False,
        },
        "authoritative_result": {
            "equal_baseline_all_history": equal_all,
            "vol_cap_challenger_all_history": risk_all,
            "equal_periods": equal_periods,
            "vol_cap_periods": risk_periods,
            "checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": 0,
            "changed_top10_members_count": 0,
            "changed_rank_count": 0,
            "selection_divergence_reason": "none; only position size differs",
        },
        "judgment": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": decision,
            "reason_type": "same_trade_mark_to_market_risk_comparison",
        },
        "remaining_risks": [
            "final look-ahead and requirement-by-requirement adoption audit remains",
            "fixed initial-capital sizing is not compounded sizing",
        ],
    }
    trades.to_parquet(out / "accepted_trades.parquet", index=False)
    equal_daily.to_parquet(out / "equal_daily_nav.parquet", index=False)
    risk_daily.to_parquet(out / "vol_cap_daily_nav.parquet", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8"
    )
    print(json.dumps({"checks": checks, "decision": decision, "equal": equal_periods, "challenger": risk_periods}, ensure_ascii=False))


if __name__ == "__main__":
    main()
