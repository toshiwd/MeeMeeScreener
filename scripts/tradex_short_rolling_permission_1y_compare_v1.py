from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from scripts.tradex_adaptive_short_rule_router_v1 import load_climax, load_failed_high, load_support, state
    from scripts.tradex_short_fast10_guard_compare_v1 import report, route as route_b0
    from scripts.tradex_short_rolling_permission_compare_v1 import THRESHOLDS, branching, daily_metrics, opportunity_loss
except ModuleNotFoundError:
    from tradex_adaptive_short_rule_router_v1 import load_climax, load_failed_high, load_support, state
    from tradex_short_fast10_guard_compare_v1 import report, route as route_b0
    from tradex_short_rolling_permission_compare_v1 import THRESHOLDS, branching, daily_metrics, opportunity_loss

AXIS_ID = "tradex_short_rolling_permission_1y_compare_v1"
OUT = Path(r"G:\Tradex\tradex_short_rolling_permission_1y_compare_v1")


def permission_at(history: pd.DataFrame, signal_date: pd.Timestamp) -> dict:
    start = signal_date - pd.DateOffset(years=1)
    known = history[(history.outcome_known_date < signal_date) & (history.signal_date >= start)]
    m = daily_metrics(known)
    if m["event_count"] < 30:
        status, allowed = "insufficient_history", False
    else:
        allowed = bool((m["daily_profit_factor"] or 0) >= 1.20 and (m["daily_expectancy"] or 0) > 0)
        status = "permitted" if allowed else "denied_performance"
    return {"permission": allowed, "permission_status": status, "known_start": start, "known_n": m["event_count"], "known_trade_days": m["trade_days"], "known_daily_profit_factor": m["daily_profit_factor"], "known_daily_expectancy": m["daily_expectancy"]}


def route_b4(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    audit, routed = [], []
    for signal_date, day in events.groupby("signal_date", sort=True):
        prior = events[events.outcome_known_date < signal_date]
        eligible = []
        for rule in sorted(events.rule.unique()):
            base = state(prior[prior.rule == rule]); permission = permission_at(events[events.rule == rule], signal_date)
            item = {"signal_date": signal_date, "rule": rule, **base, **permission}; audit.append(item)
            if base["state"] == "Active" and permission["permission"]: eligible.append(item)
        eligible.sort(key=lambda x: (-x["score"], x["rule"])); allowed = {x["rule"] for x in eligible[:2]}
        selected = day[day.rule.isin(allowed)].copy()
        if not selected.empty:
            selected["router_score"] = selected.rule.map({x["rule"]: x["score"] for x in eligible})
            routed.append(selected.sort_values(["router_score", "code"], ascending=[False, True]).head(5))
    return pd.DataFrame(audit), pd.concat(routed, ignore_index=True) if routed else events.iloc[:0].copy()


def run() -> Path:
    support, sp = load_support(); failed, fp = load_failed_high(); climax, cp = load_climax()
    events = pd.concat([support, failed, climax], ignore_index=True).sort_values(["signal_date", "rule", "code"])
    _, b0 = route_b0(events, False); audit, b4 = route_b4(events); reports = {}
    for name, start, end in (("development_2019_2025", "2019-01-01", "2025-12-31"), ("diagnostic_2026", "2026-01-01", "2026-07-10")):
        p0 = b0[(b0.signal_date >= start) & (b0.signal_date <= end)]; p4 = b4[(b4.signal_date >= start) & (b4.signal_date <= end)]
        loss = opportunity_loss(p0, p4); loss["definition"] = "share of B0-routed family-signal-days removed by B4 whose counterfactual daily mean return was positive; evaluation-only"
        branch = branching(p0, p4); branch["selection_divergence_reason"] = "one_year_family_permission_denied_or_insufficient_history"
        reports[name] = {"B0_current_recent20": report(p0, len(p0)), "B4_rolling_permission_1y": report(p4, len(p0)), "opportunity_loss": loss, "branching": branch}
    d0, d4 = reports["development_2019_2025"]["B0_current_recent20"], reports["development_2019_2025"]["B4_rolling_permission_1y"]
    v4 = reports["diagnostic_2026"]["B4_rolling_permission_1y"]
    dev_ok = (d4["daily_profit_factor"] or 0) >= (d0["daily_profit_factor"] or 0) + .05 and (d4["utilization_vs_b0"] or 0) >= .60 and d4["trade_days"] >= 40 and d4["unique_codes"] >= 20
    val_ok = (v4["daily_profit_factor"] or 0) >= 1.10 and (v4["daily_expectancy"] or 0) > 0 and (v4["utilization_vs_b0"] or 0) >= .60 and v4["trade_days"] >= 12 and v4["unique_codes"] >= 10
    drop = any(x["opportunity_loss_rate"] is not None and x["opportunity_loss_rate"] > .40 for x in (reports["development_2019_2025"]["opportunity_loss"], reports["diagnostic_2026"]["opportunity_loss"]))
    decision = "drop" if drop else "keep" if dev_ok and val_ok else "hold"
    output = OUT / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    audit.to_csv(output / "point_in_time_permission_audit.csv", index=False); b0.to_csv(output / "B0_routed_events.csv", index=False); b4.to_csv(output / "B4_routed_events.csv", index=False)
    thresholds = json.loads(json.dumps(THRESHOLDS)); thresholds["permission"]["trailing_years"] = 1
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "fixed_evaluation_conditions": {"single_changed_axis": "permission lookback 4 years to 1 year", "point_in_time": "outcome_known_date < signal_date", "permission": "same family n>=30 daily PF>=1.20 expectancy>0", "history_shortage": "explicit denial; no fallback", "B0_unchanged": True}, "predeclared_thresholds": thresholds, "source_artifacts": {"support_break": str(sp), "failed_high": str(fp), "climax_failure": str(cp)}, "reports": reports, "permission_status_counts": audit.permission_status.value_counts().to_dict(), "decision": {"candidate_local_decision": decision, "development_gate": dev_ok, "diagnostic_gate": val_ok, "opportunity_loss_drop_gate": drop, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_condition_one_year_family_permission"}, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    (output / "decision_thresholds.json").write_text(json.dumps(thresholds, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    path = output / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8"); print(path); return path


if __name__ == "__main__": run()
