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

from scripts import tradex_2026_momentum_leader_reentry_selection_v1 as selection
from scripts.tradex_long_short_weekly_coverage_v1 import metrics, weekly_coverage


AXIS_ID = "tradex_momentum_reentry_h10_union_v1"
OUT = Path(r"G:\Tradex\momentum_reentry_h10_union_v1")
SOURCE_ROOT = Path(r"G:\Tradex\position_lifecycle_multiyear_momentum_regime_audit_v1")
BASE_ROOT = Path(r"G:\Tradex\long_short_weekly_coverage_v1")


def _latest_base_events() -> Path:
    files = sorted(BASE_ROOT.glob("*/combined_events.csv"), key=lambda path: path.stat().st_mtime)
    if not files:
        raise FileNotFoundError("combined_events.csv not found")
    return files[-1]


def _latest_source() -> Path:
    files = sorted(SOURCE_ROOT.glob("*/position_lifecycle_multiyear_regime_rows.parquet"), key=lambda path: path.stat().st_mtime)
    if not files:
        raise FileNotFoundError("position_lifecycle_multiyear_regime_rows.parquet not found")
    return files[-1]


def _signals(source_path: Path) -> pd.DataFrame:
    rows = pd.read_parquet(source_path)
    rows["momentum_regime_flag"] = rows["market_momentum_regime"].isin(selection.MOMENTUM_REGIMES)
    rows["relative_strength_score"] = (
        rows["close_vs_ma20_pct"].rank(pct=True)
        + rows["close_vs_ma60_pct"].rank(pct=True)
        + rows["weekly_close_vs_ma20_pct"].rank(pct=True)
        + rows["monthly_close_vs_ma20_pct"].rank(pct=True)
    ) / 4
    rows["relative_strength_percentile_same_day"] = rows.groupby("as_of_date")["relative_strength_score"].rank(pct=True)
    rows["leader_flag"] = rows["relative_strength_percentile_same_day"] >= .85
    rows["momentum_leader_state"] = rows.apply(selection._classify, axis=1)
    result = rows.loc[rows.momentum_leader_state == "ReentryReady", ["as_of_date", "code"]].copy()
    result["code"] = result.code.astype(str)
    result["as_of_date"] = result.as_of_date.astype(int)
    return result.drop_duplicates()


def _simulate(group: pd.DataFrame) -> dict | None:
    group = group.sort_values("rn")
    if len(group) < 10:
        return None
    entry = float(group.iloc[0].o)
    signal_close = float(group.iloc[0].signal_close)
    if entry > signal_close:
        return None
    tp, sl = entry * 1.08, entry * .95
    for _, row in group.head(10).iterrows():
        if float(row.l) <= sl:
            return {"entry_date": int(group.iloc[0].ymd), "ret": -.05, "exit_reason": "sl"}
        if float(row.h) >= tp:
            return {"entry_date": int(group.iloc[0].ymd), "ret": .08, "exit_reason": "tp"}
    return {
        "entry_date": int(group.iloc[0].ymd),
        "ret": float(group.iloc[9].c) / entry - 1,
        "exit_reason": "h10",
    }


def run() -> Path:
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    source_path = _latest_source()
    signals = _signals(source_path)
    query = """
    WITH normalized AS (
      SELECT code,
        CASE WHEN date>30000000 THEN CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ELSE CAST(date AS INTEGER) END ymd,
        o,h,l,c
      FROM daily_bars WHERE source='pan' AND o>0 AND h>0 AND l>0 AND c>0
    ), joined AS (
      SELECT s.as_of_date,s.code,b.ymd,b.o,b.h,b.l,b.c,
        first_value(sig.c) OVER(PARTITION BY s.as_of_date,s.code ORDER BY b.ymd) signal_close,
        row_number() OVER(PARTITION BY s.as_of_date,s.code ORDER BY b.ymd) rn
      FROM momentum_signals s
      JOIN normalized sig ON sig.code=s.code AND sig.ymd=s.as_of_date
      JOIN normalized b ON b.code=s.code AND b.ymd>s.as_of_date
    )
    SELECT * FROM joined WHERE rn<=10 ORDER BY as_of_date,code,rn
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        db.register("momentum_signals", signals)
        paths = db.execute(query).fetchdf()

    events = []
    for (as_of_date, code), group in paths.groupby(["as_of_date", "code"]):
        result = _simulate(group)
        if result is not None:
            events.append({
                "side": "buy", "code": str(code), "signal_date": pd.to_datetime(str(as_of_date)),
                "entry_date": pd.to_datetime(str(result["entry_date"])), "ret": result["ret"],
                "rule": "momentum_leader_reentry", "exit_reason": result["exit_reason"],
            })
    momentum = pd.DataFrame(events)
    base_path = _latest_base_events()
    base = pd.read_csv(base_path, parse_dates=["signal_date", "entry_date"])
    union = pd.concat([base, momentum], ignore_index=True).sort_values(["entry_date", "side", "code"])
    union = union.drop_duplicates(["entry_date", "side", "code"], keep="first")

    periods = {
        "development_2019_2025": ("2019-01-01", "2025-12-31"),
        "current_2026": ("2026-01-01", "2026-07-10"),
    }
    reports = {}
    for name, (start, end) in periods.items():
        part = union[(union.entry_date >= start) & (union.entry_date <= end)]
        reports[name] = {
            "combined": metrics(part),
            "base_rules": metrics(part[part.rule != "momentum_leader_reentry"]),
            "momentum_reentry": metrics(part[part.rule == "momentum_leader_reentry"]),
            "weekly_coverage": weekly_coverage(part, start, end),
        }
    current = reports["current_2026"]
    gate = bool(
        (current["combined"]["daily_profit_factor"] or 0) >= 1.2
        and (current["combined"]["daily_expectancy"] or 0) > 0
        and (current["weekly_coverage"]["average_events_per_calendar_week"] or 0) >= 1.0
    )
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    momentum.to_csv(output / "momentum_execution_events.csv", index=False)
    union.to_csv(output / "union_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "add fixed momentum-leader ReentryReady buy family only",
            "entry": "next-session open only when open<=signal close",
            "exit": "TP8 SL5 H10 stop-first",
            "aggregation": "equal-weight return per actual entry day",
            "holdings": "ignored", "capital_allocation": "not used", "costs": "ignored per project rule",
        },
        "source_artifacts": {"feature_rows": str(source_path), "base_events": str(base_path), "runtime_db": str(db_path)},
        "reports": reports,
        "adoption_gate": {"2026_daily_pf_gte_1_2": True, "2026_daily_expectancy_positive": True, "2026_average_events_per_week_gte_1": True, "pass": gate},
        "decision": {"candidate_local_decision": "keep" if gate else "drop", "authoritative_rollup_decision": "research_only", "reason_type": "frequency_and_edge_gate_pass" if gate else "frequency_or_edge_gate_failed"},
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False, "silent_fallback_used": False,
    }
    path = output / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
