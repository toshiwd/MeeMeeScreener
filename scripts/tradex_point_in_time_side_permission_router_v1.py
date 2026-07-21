from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_frozen_two_sided_union_2026_v1 as baseline_builder


AXIS_ID = "tradex_point_in_time_side_permission_router_v1"
DEFAULT_DB = Path(
    r"G:\Tradex\scratch\source_snapshots"
    r"\nightly_candidate_20260713_20260713T002453985795Z.duckdb"
)
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_side_permission_router_v1")

WINDOW_DAYS = 60
MIN_N = 30
MIN_TRADE_DAYS = 20
PF_GATE = 1.30
EXPECTANCY_GATE = 0.0
CVAR10_FLOOR = -0.08
EMBARGO_SESSIONS = 1


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _pf(values: pd.Series) -> float | None:
    wins = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    return wins / losses if losses > 0 else None


def _cvar10(values: pd.Series) -> float | None:
    if values.empty:
        return None
    cutoff = float(values.quantile(0.10))
    return float(values[values <= cutoff].mean())


def _advance(calendar: list[int], ymd: int, sessions: int) -> int | None:
    pos = np.searchsorted(np.asarray(calendar, dtype=np.int64), int(ymd), side="right")
    target = pos + sessions - 1
    return int(calendar[target]) if target < len(calendar) else None


def _epoch_to_ymd(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(pd.to_datetime(float(value), unit="s", utc=True).strftime("%Y%m%d"))


def prepare_events(raw: pd.DataFrame, calendar: list[int]) -> pd.DataFrame:
    frame = raw.copy()
    frame["signal_ymd"] = pd.to_numeric(frame["signal_ymd"], errors="raise").astype(int)
    frame["side_return"] = pd.to_numeric(frame["side_return"], errors="raise")
    frame["side"] = frame["side"].astype(str)
    buy_exit = frame["exit_date"].map(_epoch_to_ymd)
    sell_exit = []
    for row in frame.itertuples(index=False):
        if row.side != "sell":
            sell_exit.append(None)
            continue
        entry = int(row.entry_ymd)
        # holding_days counts the entry session as day one in the frozen sell ledger.
        sell_exit.append(_advance(calendar, entry, max(int(row.holding_days) - 1, 0)))
    frame["outcome_known_date"] = [
        int(b) if s == "buy" and b is not None else int(e) if e is not None else None
        for s, b, e in zip(frame.side, buy_exit, sell_exit)
    ]
    frame["eligible_from_date"] = frame.outcome_known_date.map(
        lambda x: _advance(calendar, int(x), EMBARGO_SESSIONS + 1) if pd.notna(x) else None
    )
    return frame


def build_corrected_baseline(db_path: Path, pan_calendar: list[int]) -> tuple[pd.DataFrame, dict]:
    """Extend the pinned MeeMee rank-asc baseline builder without changing its selection axis."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        ranking_dates = [int(row[0]) for row in con.execute(
            """select distinct dt from ranking_appearance_daily
               where ranking_logic_version='ranking:trade:top50:v1'
                 and dir='up' and dt between 20240101 and 20261231 order by dt"""
        ).fetchall()]
    if not ranking_dates:
        raise ValueError("RANKING_HISTORY_COVERAGE_MISSING")
    old_begin, old_end = baseline_builder.BEGIN, baseline_builder.END
    try:
        baseline_builder.BEGIN, baseline_builder.END = min(ranking_dates), max(ranking_dates)
        frame, status = baseline_builder.load_meemee_baseline(db_path, ranking_dates)
    finally:
        baseline_builder.BEGIN, baseline_builder.END = old_begin, old_end
    frame = frame.rename(columns={"net_ret": "side_return"})
    frame["signal_ymd"] = frame.signal_ymd.astype(int)
    frame["split"] = frame.signal_ymd.map(lambda d: "train" if d < 20250101 else "validation" if d < 20260101 else "shadow")
    # Baseline output does not persist exact exits. The frozen contract has H10, so use
    # the conservative maximum outcome-known date; this can only delay permission.
    frame["outcome_known_date"] = frame.signal_ymd.map(lambda d: _advance(pan_calendar, int(d), 10))
    frame["eligible_from_date"] = frame.outcome_known_date.map(
        lambda d: _advance(pan_calendar, int(d), EMBARGO_SESSIONS + 1) if pd.notna(d) else None
    )
    return frame, {**status, "ranking_history_begin": min(ranking_dates), "ranking_history_end": max(ranking_dates), "outcome_known_policy": "conservative signal plus 10 PAN sessions"}


def permission_table(events: pd.DataFrame) -> pd.DataFrame:
    daily = (
        events.groupby(["side", "signal_ymd"], as_index=False)
        .agg(side_day_return=("side_return", "mean"), outcome_known_date=("outcome_known_date", "max"), eligible_from_date=("eligible_from_date", "max"))
    )
    rows: list[dict] = []
    for side in ("buy", "sell"):
        side_events = events[events.side == side]
        for signal_date in sorted(side_events.signal_ymd.unique()):
            known = daily[(daily.side == side) & (daily.eligible_from_date <= signal_date)].sort_values("signal_ymd").tail(WINDOW_DAYS)
            values = known.side_day_return
            pf = _pf(values)
            exp = float(values.mean()) if len(values) else None
            cvar = _cvar10(values)
            n_events = int(events[(events.side == side) & (events.signal_ymd.isin(known.signal_ymd))].shape[0])
            active = bool(
                n_events >= MIN_N
                and len(known) >= MIN_TRADE_DAYS
                and pf is not None
                and pf >= PF_GATE
                and exp is not None
                and exp > EXPECTANCY_GATE
                and cvar is not None
                and cvar >= CVAR10_FLOOR
            )
            if n_events < MIN_N or len(known) < MIN_TRADE_DAYS:
                status = "INACTIVE_INSUFFICIENT_HISTORY"
            elif cvar is None or cvar < CVAR10_FLOOR:
                status = "INACTIVE_TAIL"
            elif pf is None or pf < PF_GATE or exp is None or exp <= EXPECTANCY_GATE:
                status = "INACTIVE_PERFORMANCE"
            else:
                status = "ACTIVE"
            rows.append({"side": side, "signal_ymd": int(signal_date), "permission_active": active, "permission_status": status, "permission_event_n": n_events, "permission_side_days": int(len(known)), "permission_pf": pf, "permission_expectancy": exp, "permission_cvar10": cvar, "latest_used_signal_date": int(known.signal_ymd.max()) if len(known) else None, "latest_used_outcome_known_date": int(known.outcome_known_date.max()) if len(known) else None})
    return pd.DataFrame(rows)


def route(events: pd.DataFrame, permissions: pd.DataFrame) -> pd.DataFrame:
    routed = events.merge(permissions, on=["side", "signal_ymd"], how="left", validate="many_to_one")
    if routed.permission_active.isna().any():
        raise ValueError("INVALID_PERMISSION_INPUT: missing side/date permission row")
    return routed[routed.permission_active].copy()


def metrics(frame: pd.DataFrame, split: str, calendar_days: int) -> dict:
    part = frame[frame.split == split].copy()
    daily = part.groupby("signal_ymd").side_return.mean()
    calendar_expectancy = float(daily.sum() / calendar_days) if calendar_days else None
    if daily.empty:
        return {"n": 0, "signal_days": 0, "buy_routed_days": 0, "sell_routed_days": 0, "daily_profit_factor": None, "daily_expectancy": None, "event_profit_factor": None, "event_expectancy": None, "calendar_expectancy": calendar_expectancy, "signals_per_week": 0.0, "p05": None, "cvar10": None, "max_drawdown_equal_weight": None}
    dates = pd.to_datetime(daily.index.astype(str), format="%Y%m%d")
    weeks = dates.strftime("%G-W%V").nunique()
    curve = (1.0 + daily).cumprod()
    dd = curve / curve.cummax() - 1.0
    return {"n": int(len(part)), "signal_days": int(len(daily)), "buy_routed_days": int(part.loc[part.side == "buy", "signal_ymd"].nunique()), "sell_routed_days": int(part.loc[part.side == "sell", "signal_ymd"].nunique()), "daily_profit_factor": _pf(daily), "daily_expectancy": float(daily.mean()), "event_profit_factor": _pf(part.side_return), "event_expectancy": float(part.side_return.mean()), "calendar_expectancy": calendar_expectancy, "signals_per_week": float(len(daily) / weeks) if weeks else 0.0, "p05": float(daily.quantile(0.05)), "cvar10": _cvar10(daily), "max_drawdown_equal_weight": float(dd.min())}


def generate(db_path: Path, out_root: Path) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    calendar = [int(x[0]) for x in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) ymd from daily_bars where source='pan' order by 1").fetchall()]
    con.close()
    events, baseline_status = build_corrected_baseline(db_path, calendar)
    if events[["outcome_known_date", "eligible_from_date"]].isna().any().any():
        raise ValueError("INVALID_PERMISSION_INPUT: outcome date cannot be reconstructed")
    permissions = permission_table(events)
    routed = route(events, permissions)
    years = {"train": 2024, "validation": 2025, "shadow": 2026}
    calendar_counts = {name: sum(1 for d in calendar if d // 10000 == year) for name, year in years.items()}
    base_metrics = {s: metrics(events, s, calendar_counts[s]) for s in years}
    routed_metrics = {s: metrics(routed, s, calendar_counts[s]) for s in years}
    branching = {}
    for s in years:
        base = events[events.split == s]
        kept = routed[routed.split == s]
        branching[s] = {"baseline_events": int(len(base)), "routed_events": int(len(kept)), "suppressed_events": int(len(base) - len(kept)), "baseline_signal_days": int(base.signal_ymd.nunique()), "routed_signal_days": int(kept.signal_ymd.nunique()), "suppressed_buy_days": int(base.loc[(base.side == "buy") & (~base.index.isin(kept.index)), "signal_ymd"].nunique()), "suppressed_sell_days": int(base.loc[(base.side == "sell") & (~base.index.isin(kept.index)), "signal_ymd"].nunique())}
    v = routed_metrics["validation"]
    keep_gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "expectancy_positive": v["daily_expectancy"] is not None and v["daily_expectancy"] > 0, "baseline_pf_non_degrade": v["daily_profit_factor"] is not None and base_metrics["validation"]["daily_profit_factor"] is not None and v["daily_profit_factor"] >= base_metrics["validation"]["daily_profit_factor"], "baseline_expectancy_non_degrade": v["daily_expectancy"] is not None and v["daily_expectancy"] >= base_metrics["validation"]["daily_expectancy"], "cvar10_ge_minus_8pct": v["cvar10"] is not None and v["cvar10"] >= -0.08, "frequency_ge_weekly_one": v["signals_per_week"] >= 1.0, "buy_sell_each_ten_days": v["buy_routed_days"] >= 10 and v["sell_routed_days"] >= 10, "matured_days_ge_30": v["signal_days"] >= 30}
    if all(keep_gates.values()):
        judgment = "keep_shadow_2026"
    elif v["daily_profit_factor"] is not None and (v["daily_profit_factor"] < 1.0 or (v["daily_expectancy"] or 0) <= 0 or (v["cvar10"] is not None and v["cvar10"] < -0.10)):
        judgment = "drop"
    else:
        judgment = "hold"
    now = datetime.now(timezone.utc)
    root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    routed.to_csv(root / "routed_events.csv", index=False)
    permissions.to_csv(root / "point_in_time_permissions.csv", index=False)
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"candidate_generation": "unchanged corrected MeeMee ranking_appearance_daily BUY/SELL each top5, gap<=0 execution-eligible, rank ascending top3", "ranking_logic_version": "ranking:trade:top50:v1", "only_axis": "lagged matured side permission", "window": "last60 matured side signal-days", "minimum_events": MIN_N, "minimum_side_days": MIN_TRADE_DAYS, "permission_pf_min": PF_GATE, "permission_expectancy": ">0", "permission_cvar10_floor": CVAR10_FLOOR, "outcome_known_date": "conservative signal plus frozen H10 PAN sessions", "embargo_sessions_after_outcome_known": EMBARGO_SESSIONS, "inactive_policy": "no candidates; no fallback", "splits": {"train": "2024 reused", "validation": "2025", "shadow": "2026 through source ranking coverage"}, "capital_or_holding_changed": False}, "coverage": baseline_status, "source_artifacts": [{"path": str(db_path), "sha256": _sha(db_path)}], "baseline_full_calendar": base_metrics, "adaptive_permission_router": routed_metrics, "branching": branching, "validation_keep_gates": keep_gates, "decision": {"candidate_local_decision": judgment, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_point_in_time_side_permission_validation"}, "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
