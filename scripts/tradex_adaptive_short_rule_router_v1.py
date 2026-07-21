from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


AXIS_ID = "tradex_adaptive_short_rule_router_v1"
OUT = Path(r"G:\Tradex\adaptive_short_rule_router_v1")
SUPPORT_ROOT = Path(r"G:\Tradex\long_short_weekly_coverage_v1")
FAILED_HIGH_ROOT = Path(r"G:\Tradex\failed_high_retest_short_backtest_v1")
CLIMAX_ROOT = Path(r"G:\Tradex\short_climax_failure_events_v1")
FAILED_HIGH_CURRENT_ROOT = Path(r"G:\Tradex\short_failed_high_current_scan_v1")
FAILED_HIGH_RULE = {"peak_age>=120", "peak_prominence>=0.03", "pullback_depth>=0.2", "stage=forming"}
CLIMAX_MIN_CLOSE_POS = 0.10


def latest(root: Path, name: str) -> Path:
    paths = sorted(root.glob(f"*/{name}"), key=lambda path: path.stat().st_mtime)
    if not paths:
        raise FileNotFoundError(f"{name} not found under {root}")
    return paths[-1]


def profit_factor(values: pd.Series) -> float | None:
    gain = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    return gain / loss if loss else None


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"event_count": 0, "trade_days": 0, "expectancy": None, "profit_factor": None, "daily_expectancy": None, "daily_profit_factor": None}
    daily = frame.groupby("signal_date", as_index=False).ret.mean()
    return {
        "event_count": int(len(frame)), "trade_days": int(frame.signal_date.nunique()),
        "expectancy": float(frame.ret.mean()), "profit_factor": profit_factor(frame.ret),
        "daily_expectancy": float(daily.ret.mean()), "daily_profit_factor": profit_factor(daily.ret),
    }


def load_support() -> tuple[pd.DataFrame, Path]:
    path = latest(SUPPORT_ROOT, "combined_events.csv")
    frame = pd.read_csv(path, parse_dates=["signal_date", "entry_date"])
    frame = frame[frame.side == "sell"].copy()
    frame["rule"] = "support_break_breadth40"
    frame["outcome_known_date"] = frame.entry_date + pd.Timedelta(days=20)
    return frame[["code", "signal_date", "entry_date", "ret", "rule", "outcome_known_date"]], path


def load_failed_high() -> tuple[pd.DataFrame, Path]:
    path = latest(FAILED_HIGH_ROOT, "failed_high_retest_events.jsonl")
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        atoms = set(row.get("atoms") or [])
        code = str(row.get("code") or "")
        if not FAILED_HIGH_RULE.issubset(atoms) or not code.isdigit() or int(code) < 1300:
            continue
        signal_date = pd.to_datetime(str(int(row["as_of"])))
        rows.append({
            "code": code, "signal_date": signal_date, "entry_date": signal_date,
            "ret": float(row["exit_ret_short"]), "rule": "failed_high_retest",
            "outcome_known_date": signal_date + pd.Timedelta(days=20),
        })
    return pd.DataFrame(rows), path


def load_climax() -> tuple[pd.DataFrame, Path]:
    path = latest(CLIMAX_ROOT, "events.csv")
    frame = pd.read_csv(path, parse_dates=["signal_date", "entry_date", "outcome_known_date"])
    frame = frame[climax_quality_gate(frame)].copy()
    return frame[["code", "signal_date", "entry_date", "ret", "rule", "outcome_known_date"]], path


def climax_quality_gate(frame: pd.DataFrame) -> pd.Series:
    """Exclude low-close capitulation bars that were loss-heavy in 2019-2025."""
    return pd.to_numeric(frame["close_pos"], errors="coerce").ge(CLIMAX_MIN_CLOSE_POS)


def state(history: pd.DataFrame) -> dict:
    recent = history.sort_values("signal_date").tail(20)
    n = len(recent)
    unique_codes = int(recent.code.nunique()) if n else 0
    top_share = float(recent.code.value_counts(normalize=True).iloc[0]) if n else None
    pf = profit_factor(recent.ret) if n else None
    expectancy = float(recent.ret.mean()) if n else None
    concentration_ok = unique_codes >= 8 and (top_share or 1) <= .25
    if n >= 15 and concentration_ok and (pf or 0) >= 1.30 and (expectancy or 0) > 0:
        value = "Active"
    elif n >= 10 and unique_codes >= 5 and (top_share or 1) <= .35 and (pf or 0) >= 1.05 and (expectancy or 0) > 0:
        value = "Secondary"
    elif n >= 8 and (pf or 0) >= 1.0 and (expectancy or 0) >= 0:
        value = "Watch"
    else:
        value = "Dormant"
    score = min(pf or 0, 3) * .55 + max(min((expectancy or 0) * 20, 1), -1) * .25 + min(unique_codes / 20, 1) * .20
    return {"state": value, "score": score, "n20": n, "pf20": pf, "expectancy20": expectancy, "unique_codes20": unique_codes, "top_code_share20": top_share, "concentration_gate_pass": concentration_ok}


def point_in_time_route(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshots, routed = [], []
    for signal_date, day in events.groupby("signal_date", sort=True):
        prior = events[events.outcome_known_date < signal_date]
        states = []
        for rule in sorted(events.rule.unique()):
            item = {"signal_date": signal_date, "rule": rule, **state(prior[prior.rule == rule])}
            states.append(item); snapshots.append(item)
        eligible = [item for item in states if item["state"] == "Active" and item["pf20"] is not None and item["pf20"] >= 1.0 and item["expectancy20"] > 0]
        eligible.sort(key=lambda item: (-item["score"], item["rule"]))
        allowed = {item["rule"] for item in eligible[:2]}
        selected = day[day.rule.isin(allowed)].copy()
        if not selected.empty:
            score_map = {item["rule"]: item["score"] for item in eligible}
            selected["router_score"] = selected.rule.map(score_map)
            routed.append(selected.sort_values(["router_score", "code"], ascending=[False, True]).head(5))
    return pd.DataFrame(snapshots), pd.concat(routed, ignore_index=True) if routed else events.iloc[:0].copy()


def clean(value):
    if isinstance(value, float) and not math.isfinite(value): return None
    if isinstance(value, dict): return {key: clean(item) for key, item in value.items()}
    if isinstance(value, list): return [clean(item) for item in value]
    return value


def run() -> Path:
    support, support_path = load_support()
    failed, failed_path = load_failed_high()
    climax, climax_path = load_climax()
    events = pd.concat([support, failed, climax], ignore_index=True).sort_values(["signal_date", "rule", "code"])
    snapshots, routed = point_in_time_route(events)
    reports = {}
    for name, start, end in (("development_2019_2025", "2019-01-01", "2025-12-31"), ("diagnostic_2026", "2026-01-01", "2026-07-10")):
        part = routed[(routed.signal_date >= start) & (routed.signal_date <= end)]
        reports[name] = {"metrics": metrics(part), "rule_counts": part.rule.value_counts().to_dict()}
    known = events[events.outcome_known_date < pd.Timestamp("2026-07-10")]
    current_states = [{"as_of": "2026-07-10", "rule": rule, **state(known[known.rule == rule])} for rule in sorted(events.rule.unique())]
    current_state_map = {item["rule"]: item for item in current_states}
    climax_inventory_path = latest(CLIMAX_ROOT, "inventory.json")
    climax_inventory = json.loads(climax_inventory_path.read_text(encoding="utf-8"))
    current_candidates = []
    for row in climax_inventory.get("current_candidates", []):
        if float(row.get("close_pos") or 0.0) < CLIMAX_MIN_CLOSE_POS:
            continue
        item_state = current_state_map.get("climax_failure", {})
        routed_now = item_state.get("state") == "Active"
        current_candidates.append({
            "side": "sell", "code": str(row["code"]), "rule": "climax_failure",
            "signal_date": row.get("signal_date"), "confirmed_close": row.get("confirmed_close"),
            "rule_state": item_state.get("state", "Dormant"), "rule_score": item_state.get("score"),
            "family_rank": row.get("family_rank"),
            "decision": "sell_condition_confirmed" if routed_now else "watch_not_routed",
            "entry_condition": "next_session_signal_low_break", "automatic_trade": False,
        })
    failed_current_path = latest(FAILED_HIGH_CURRENT_ROOT, "current_scan.json")
    failed_current = json.loads(failed_current_path.read_text(encoding="utf-8"))
    for row in failed_current.get("candidates", []):
        item_state = current_state_map.get("failed_high_retest", {})
        routed_now = item_state.get("state") == "Active"
        current_candidates.append({
            "side": "sell", "code": str(row["code"]), "rule": "failed_high_retest",
            "signal_date": row.get("signal_date"), "confirmed_close": row.get("confirmed_close"),
            "rule_state": item_state.get("state", "Dormant"), "rule_score": item_state.get("score"),
            "family_rank": row.get("family_rank"),
            "decision": "sell_condition_confirmed" if routed_now else "watch_not_routed",
            "entry_condition": row.get("entry_condition"), "automatic_trade": False,
        })
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    events.to_csv(output / "short_rule_event_ledger.csv", index=False)
    snapshots.to_csv(output / "point_in_time_rule_states.csv", index=False)
    routed.to_csv(output / "routed_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {"outcome_delay": "20 calendar days", "active_gate": "recent20 n>=15 PF>=1.3 positive expectancy unique codes>=8 top-code share<=25%", "climax_quality_gate": "close_pos>=0.10 selected on development 2019-2025", "maximum_active_rules": 2, "maximum_daily_events": 5, "holdings": "ignored", "capital": "not used"},
        "source_artifacts": {"support_break": str(support_path), "failed_high": str(failed_path), "failed_high_current": str(failed_current_path), "climax_failure": str(climax_path)},
        "event_counts": events.rule.value_counts().to_dict(), "reports": reports, "current_states": current_states, "current_candidates": current_candidates,
        "decision": {
            "candidate_local_decision": "keep_three_family_adaptive_router",
            "session_aggregate_decision": "display_current_state_and_ranked_candidates",
            "authoritative_rollup_decision": "review_only",
            "reason_type": "three_shape_families_compared_point_in_time_with_concentration_recency_and_climax_close_position_quality_gates",
        },
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path = output / "compare.json"
    path.write_text(json.dumps(clean(payload), ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__": run()
