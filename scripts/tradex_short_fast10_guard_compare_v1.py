from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from scripts.tradex_adaptive_short_rule_router_v1 import (
        load_climax, load_failed_high, load_support, metrics as base_metrics, state,
    )
except ModuleNotFoundError:  # Direct execution: python scripts/<file>.py
    from tradex_adaptive_short_rule_router_v1 import (
        load_climax, load_failed_high, load_support, metrics as base_metrics, state,
    )


AXIS_ID = "tradex_short_fast10_guard_compare_v1"
OUT = Path(r"G:\Tradex\tradex_short_fast10_guard_compare_v1")
DECISION_THRESHOLDS = {
    "keep": {
        "development": {"daily_profit_factor_min": 1.15, "daily_profit_factor_delta_min": 0.05, "daily_expectancy_positive": True, "utilization_min": 0.70, "trade_days_min": 40, "unique_codes_min": 20, "top_code_share_max": 0.20},
        "diagnostic_2026": {"daily_profit_factor_min": 1.10, "daily_expectancy_positive": True, "utilization_min": 0.60, "trade_days_min": 12, "unique_codes_min": 10, "top_code_share_max": 0.25},
        "false_stop_rate_max": {"development": 0.25, "diagnostic_2026": 0.33},
        "reaction_lag_max": {"median": 3.0, "p90": 6.0},
    },
    "drop": {"development_daily_profit_factor_delta_max": -0.05, "development_daily_expectancy_delta_max": -0.001, "development_utilization_below": 0.50, "development_top_code_share_above": 0.20, "false_stop_rate_above": 0.40, "reaction_lag_median_above": 5.0},
    "otherwise": "hold",
}


def profit_factor(values: pd.Series) -> float | None:
    gain = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    return gain / loss if loss else None


def fast10_guard(history: pd.DataFrame) -> dict:
    recent = history.sort_values(["outcome_known_date", "signal_date"]).tail(10)
    n = len(recent)
    pf = profit_factor(recent.ret) if n else None
    expectancy = float(recent.ret.mean()) if n else None
    triggered = n >= 8 and ((pf is not None and pf < 0.90) or (expectancy is not None and expectancy <= -0.0025))
    return {"fast10_n": n, "fast10_pf": pf, "fast10_expectancy": expectancy, "fast10_triggered": triggered}


def route(events: pd.DataFrame, use_fast10: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshots, routed = [], []
    for signal_date, day in events.groupby("signal_date", sort=True):
        # Strictly earlier known outcomes only; equality is deliberately excluded.
        prior = events[events.outcome_known_date < signal_date]
        states = []
        for rule in sorted(events.rule.unique()):
            history = prior[prior.rule == rule]
            item = {"signal_date": signal_date, "rule": rule, **state(history), **fast10_guard(history)}
            item["base_state"] = item["state"]
            if use_fast10 and item["state"] == "Active" and item["fast10_triggered"]:
                item["state"] = "Watch"
            states.append(item)
            snapshots.append(item)
        eligible = [item for item in states if item["state"] == "Active"]
        eligible.sort(key=lambda item: (-item["score"], item["rule"]))
        allowed = {item["rule"] for item in eligible[:2]}
        selected = day[day.rule.isin(allowed)].copy()
        if not selected.empty:
            scores = {item["rule"]: item["score"] for item in eligible}
            selected["router_score"] = selected.rule.map(scores)
            routed.append(selected.sort_values(["router_score", "code"], ascending=[False, True]).head(5))
    return pd.DataFrame(snapshots), pd.concat(routed, ignore_index=True) if routed else events.iloc[:0].copy()


def report(frame: pd.DataFrame, b0_count: int) -> dict:
    result = base_metrics(frame)
    result["utilization_vs_b0"] = float(len(frame) / b0_count) if b0_count else None
    result["unique_codes"] = int(frame.code.nunique()) if not frame.empty else 0
    result["top_code_share"] = float(frame.code.value_counts(normalize=True).iloc[0]) if not frame.empty else None
    result["rule_count"] = int(frame.rule.nunique()) if not frame.empty else 0
    return result


def oracle_instrumentation(events: pd.DataFrame, snapshots: pd.DataFrame, start: str, end: str) -> dict:
    """Evaluation-only oracle. Its columns never feed state, selection, or routing."""
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    days = events[(events.signal_date >= start_ts) & (events.signal_date <= end_ts)].groupby(["rule", "signal_date"], as_index=False).ret.mean()
    states = snapshots[(snapshots.signal_date >= start_ts) & (snapshots.signal_date <= end_ts)].copy()
    false_flags, lags = [], []
    eligible_stops = 0
    oracle_starts = 0
    for rule, family in days.groupby("rule"):
        family = family.sort_values("signal_date").reset_index(drop=True)
        family_states = states[states.rule == rule].set_index("signal_date")
        b1_states = []
        for date in family.signal_date:
            b1_states.append(family_states.loc[date, "state"] if date in family_states.index else None)
        # A stop is the transition from Active to non-Active on family signal days.
        for i in range(1, len(family)):
            if b1_states[i - 1] == "Active" and b1_states[i] != "Active":
                future = family.iloc[i + 1:i + 6].ret
                if len(future) < 5:
                    continue
                eligible_stops += 1
                pf = profit_factor(future)
                false_flags.append(bool(pf is not None and pf >= 1.20 and float(future.mean()) > 0))
        # Oracle deterioration begins at the first day of each future-10 bad episode.
        bad = []
        for i in range(len(family)):
            future10 = family.iloc[i + 1:i + 11].ret
            if len(future10) < 10:  # incomplete tail is excluded
                bad.append(False)
                continue
            pf = profit_factor(future10)
            bad.append(bool(pf is not None and pf < 0.90 and float(future10.mean()) <= 0))
        for i, is_bad in enumerate(bad):
            if not is_bad or (i > 0 and bad[i - 1]):
                continue
            oracle_starts += 1
            lag = 10
            for step in range(0, 11):
                j = i + step
                if j >= len(b1_states):
                    break
                if b1_states[j] != "Active":
                    lag = step
                    break
            lags.append(lag)
    return {
        "false_stop_rate": float(sum(false_flags) / len(false_flags)) if false_flags else None,
        "false_stop_count": int(sum(false_flags)), "eligible_stop_count": eligible_stops,
        "reaction_lag_median": float(pd.Series(lags).median()) if lags else None,
        "reaction_lag_p90": float(pd.Series(lags).quantile(.90)) if lags else None,
        "reaction_lag_episode_count": oracle_starts,
        "oracle_definition": "evaluation_only; false-stop uses next5 completed family-signal-day PF>=1.20 and expectancy>0; lag uses first future10 PF<0.90 and expectancy<=0, capped at 10",
    }


def decide(dev0: dict, dev1: dict, diag0: dict, diag1: dict, instrumentation: dict | None = None) -> tuple[str, list[str]]:
    """Predeclared same-condition thresholds; diagnostic cannot rescue failed development."""
    dev_pf_delta = (dev1["daily_profit_factor"] or 0) - (dev0["daily_profit_factor"] or 0)
    dev_exp_delta = (dev1["daily_expectancy"] or 0) - (dev0["daily_expectancy"] or 0)
    diag_pf_delta = (diag1["daily_profit_factor"] or 0) - (diag0["daily_profit_factor"] or 0)
    utilization = dev1["utilization_vs_b0"] or 0
    instrumentation = instrumentation or {}
    dev_inst = instrumentation.get("development_2019_2025") or {}
    diag_inst = instrumentation.get("diagnostic_2026") or {}
    instrumentation_ok = all(value is not None for value in (dev_inst.get("false_stop_rate"), diag_inst.get("false_stop_rate"), dev_inst.get("reaction_lag_median"), dev_inst.get("reaction_lag_p90"), diag_inst.get("reaction_lag_median"), diag_inst.get("reaction_lag_p90")))
    oracle_keep = instrumentation_ok and dev_inst["false_stop_rate"] <= .25 and diag_inst["false_stop_rate"] <= .33 and dev_inst["reaction_lag_median"] <= 3 and dev_inst["reaction_lag_p90"] <= 6 and diag_inst["reaction_lag_median"] <= 3 and diag_inst["reaction_lag_p90"] <= 6
    dev_keep = (dev1["daily_profit_factor"] or 0) >= 1.15 and dev_pf_delta >= 0.05 and (dev1["daily_expectancy"] or 0) > 0 and utilization >= 0.70 and dev1["trade_days"] >= 40 and dev1["unique_codes"] >= 20 and (dev1["top_code_share"] or 1) <= 0.20
    diag_keep = (diag1["daily_profit_factor"] or 0) >= 1.10 and (diag1["daily_expectancy"] or 0) > 0 and (diag1["utilization_vs_b0"] or 0) >= 0.60 and diag1["trade_days"] >= 12 and diag1["unique_codes"] >= 10 and (diag1["top_code_share"] or 1) <= 0.25
    reasons = [f"dev_daily_pf_delta={dev_pf_delta:.6f}", f"dev_daily_expectancy_delta={dev_exp_delta:.6f}", f"diagnostic_daily_pf_delta={diag_pf_delta:.6f}", f"development_utilization={utilization:.6f}", f"development_keep_gate={dev_keep}", f"diagnostic_keep_gate={diag_keep}", f"required_instrumentation_complete={instrumentation_ok}", f"oracle_keep_gate={oracle_keep}"]
    if dev_keep and diag_keep and oracle_keep:
        return "keep", reasons
    oracle_drop = instrumentation_ok and (dev_inst["false_stop_rate"] > .40 or diag_inst["false_stop_rate"] > .40 or dev_inst["reaction_lag_median"] > 5 or diag_inst["reaction_lag_median"] > 5)
    if dev_pf_delta <= -0.05 or dev_exp_delta <= -0.001 or utilization < 0.50 or (dev1["top_code_share"] or 1) > 0.20 or oracle_drop:
        return "drop", reasons
    return "hold", reasons


def clean(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def run() -> Path:
    support, support_path = load_support()
    failed, failed_path = load_failed_high()
    climax, climax_path = load_climax()
    events = pd.concat([support, failed, climax], ignore_index=True).sort_values(["signal_date", "rule", "code"])
    snap0, routed0 = route(events, False)
    snap1, routed1 = route(events, True)
    reports = {}
    periods = (("development_2019_2025", "2019-01-01", "2025-12-31"), ("diagnostic_2026", "2026-01-01", "2026-07-10"))
    for name, start, end in periods:
        p0 = routed0[(routed0.signal_date >= start) & (routed0.signal_date <= end)]
        p1 = routed1[(routed1.signal_date >= start) & (routed1.signal_date <= end)]
        reports[name] = {"B0_current_recent20": report(p0, len(p0)), "B1_fast10_guard": report(p1, len(p0))}
    dev = reports["development_2019_2025"]
    diag = reports["diagnostic_2026"]
    instrumentation = {
        "development_2019_2025": oracle_instrumentation(events, snap1, "2019-01-01", "2025-12-31"),
        "diagnostic_2026": oracle_instrumentation(events, snap1, "2026-01-01", "2026-07-10"),
    }
    reports["development_2019_2025"]["B1_fast10_guard"]["oracle_instrumentation"] = instrumentation["development_2019_2025"]
    reports["diagnostic_2026"]["B1_fast10_guard"]["oracle_instrumentation"] = instrumentation["diagnostic_2026"]
    judgment, reasons = decide(dev["B0_current_recent20"], dev["B1_fast10_guard"], diag["B0_current_recent20"], diag["B1_fast10_guard"], instrumentation)
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    snap0.to_csv(output / "B0_point_in_time_states.csv", index=False)
    snap1.to_csv(output / "B1_point_in_time_states.csv", index=False)
    routed0.to_csv(output / "B0_routed_events.csv", index=False)
    routed1.to_csv(output / "B1_routed_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {"axis": "fast10_guard_only", "B0": "current recent20 state unchanged", "B1": "when n10>=8 and (PF10<0.90 or expectancy10<=-0.0025), Active is downgraded one level to Watch", "point_in_time": "only outcome_known_date < signal_date", "development": "2019-01-01..2025-12-31", "diagnostic": "2026-01-01..2026-07-10", "same_universe_period_topk_regime_costs": True},
        "predeclared_decision_thresholds": DECISION_THRESHOLDS,
        "instrumentation": instrumentation,
        "source_artifacts": {"support_break": str(support_path), "failed_high": str(failed_path), "climax_failure": str(climax_path)},
        "reports": reports, "judgment": {"candidate_local_decision": judgment, "reason_type": "predeclared_fixed_condition_fast10_guard_comparison", "reasons": reasons, "authoritative_rollup_decision": "review_only"},
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    path = output / "compare.json"
    (output / "decision_thresholds.json").write_text(json.dumps(DECISION_THRESHOLDS, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    path.write_text(json.dumps(clean(payload), ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
