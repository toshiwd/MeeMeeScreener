from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_short_support_break_breadth_gate_v1 import SQL as SHORT_SQL
from scripts.tradex_short_support_break_exit_grid_v1 import clean, simulate


AXIS_ID = "tradex_long_short_weekly_coverage_v1"
OUT = Path(r"G:\Tradex\long_short_weekly_coverage_v1")
LONG_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")


def pf(values: pd.Series) -> float | None:
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    return gains / losses if losses else None


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"event_count": 0, "trade_days": 0, "expectancy": None, "profit_factor": None}
    daily = frame.groupby("entry_date", as_index=False)["ret"].mean()
    return {
        "event_count": int(len(frame)),
        "trade_days": int(frame.entry_date.nunique()),
        "expectancy": float(frame.ret.mean()),
        "profit_factor": pf(frame.ret),
        "daily_expectancy": float(daily.ret.mean()),
        "daily_profit_factor": pf(daily.ret),
    }


def weekly_coverage(frame: pd.DataFrame, start: str, end: str) -> dict:
    calendar = pd.date_range(start=start, end=end, freq="W-FRI")
    active = frame[(frame.entry_date >= start) & (frame.entry_date <= end)].copy()
    active["week"] = active.entry_date.dt.to_period("W-FRI")
    active_weeks = int(active.week.nunique())
    total_weeks = int(len(calendar))
    weekly_counts = active.groupby("week").size()
    return {
        "calendar_weeks": total_weeks,
        "weeks_with_trade": active_weeks,
        "coverage_rate": active_weeks / total_weeks if total_weeks else None,
        "average_events_per_calendar_week": len(active) / total_weeks if total_weeks else None,
        "maximum_consecutive_empty_weeks": _max_empty_weeks(active.week.unique(), start, end),
        "weeks_with_multiple_events": int((weekly_counts > 1).sum()),
    }


def _max_empty_weeks(active_weeks, start: str, end: str) -> int:
    all_weeks = pd.period_range(start=start, end=end, freq="W-FRI")
    active = set(active_weeks)
    best = run = 0
    for week in all_weeks:
        run = 0 if week in active else run + 1
        best = max(best, run)
    return best


def latest_long_events() -> Path:
    files = sorted(LONG_ROOT.glob("*/eligible_execution_events.csv"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError("eligible_execution_events.csv not found")
    return files[-1]


def run() -> Path:
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    long_path = latest_long_events()
    long = pd.read_csv(long_path)
    long["code"] = long["code"].astype(str)
    long["signal_date"] = pd.to_datetime(long.date, unit="s", utc=True).dt.tz_localize(None)
    long["entry_date"] = pd.to_datetime(long.next_entry_date, unit="s", utc=True).dt.tz_localize(None)

    feature_sql = """
    WITH base AS (
      SELECT code,date,c,
        c/lag(c) OVER(PARTITION BY code ORDER BY date)-1 ret1,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20
      FROM daily_bars WHERE source='pan'
    ), features AS (
      SELECT code,date,
        stddev_samp(ret1) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) realized_vol20,
        c,ma20
      FROM base
    ), breadth AS (
      SELECT date,avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) market_breadth
      FROM features GROUP BY date
    )
    SELECT f.code,f.date,f.realized_vol20,b.market_breadth
    FROM features f JOIN breadth b USING(date)
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        features = db.execute(feature_sql).fetchdf()
        short_raw = [{k: clean(v) for k, v in row.items()} for row in db.execute(SHORT_SQL).fetchdf().to_dict("records")]

    features["code"] = features["code"].astype(str)
    long = long.merge(features, on=["code", "date"], how="left")
    long = long[(long.shape_leaf != 20) | ((long.realized_vol20 < .03) & (long.market_breadth <= .60))].copy()
    long_events = pd.DataFrame({
        "side": "buy",
        "code": long.code.astype(str),
        "signal_date": long.signal_date,
        "entry_date": long.entry_date,
        "ret": long.next_open_return.astype(float),
        "rule": long.shape_leaf.map(lambda value: f"leaf{int(value)}"),
    })

    short_rows = [{**row, **simulate(row, .10, .05, 10)} for row in short_raw if float(row["breadth_below_ma20"]) >= .40]
    short = pd.DataFrame(short_rows)
    short_events = pd.DataFrame({
        "side": "sell",
        "code": short.code.astype(str),
        "signal_date": pd.to_datetime(short.ymd.astype(str)),
        "entry_date": pd.to_datetime(short.e_ymd.astype(str)),
        "ret": short.ret.astype(float),
        "rule": "support_break_breadth40",
    })
    events = pd.concat([long_events, short_events], ignore_index=True).sort_values(["entry_date", "side", "code"])
    events = events[(events.entry_date >= "2019-01-01") & (events.entry_date <= "2026-07-10")].copy()
    events["year"] = events.entry_date.dt.year

    periods = {
        "development_2019_2025": ("2019-01-01", "2025-12-31"),
        "current_2026": ("2026-01-01", "2026-07-10"),
    }
    reports = {}
    for name, (start, end) in periods.items():
        part = events[(events.entry_date >= start) & (events.entry_date <= end)]
        reports[name] = {
            "combined": metrics(part),
            "buy": metrics(part[part.side == "buy"]),
            "sell": metrics(part[part.side == "sell"]),
            "weekly_coverage": weekly_coverage(part, start, end),
        }

    current = reports["current_2026"]
    gate = bool(
        current["combined"]["event_count"] > 0
        and (current["combined"]["daily_profit_factor"] or 0) >= 1.2
        and (current["combined"]["daily_expectancy"] or 0) > 0
        and (current["weekly_coverage"]["average_events_per_calendar_week"] or 0) >= 1.0
    )
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    events.to_csv(output / "combined_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "union of already established buy and conditional short rules",
            "buy": "leaf9 and leaf14 unchanged; leaf20 vol20<3% and breadth_above_ma20<=60%; next open gap<=0; TP8 SL5 H10",
            "sell": "support break capitulation; breadth_below_ma20>=40%; next-day signal-low entry; TP10 SL5 H10",
            "aggregation": "equal-weight return per actual entry day",
            "costs": "ignored per project rule",
            "holdings": "ignored",
            "capital_allocation": "not used",
        },
        "source_artifacts": {"long_events": str(long_path), "runtime_db": str(db_path)},
        "reports": reports,
        "adoption_gate": {
            "current_2026_average_events_per_week_gte_1": True,
            "current_2026_daily_pf_gte_1_2": True,
            "current_2026_daily_expectancy_positive": True,
            "pass": gate,
        },
        "decision": {
            "candidate_local_decision": "keep" if gate else "hold",
            "authoritative_rollup_decision": "research_only",
            "reason_type": "2026_frequency_and_edge_gate_pass" if gate else "2026_frequency_or_edge_gate_failed",
        },
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "automatic_trading": False,
        "silent_fallback_used": False,
    }
    path = output / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
