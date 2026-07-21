from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from scripts.tradex_adaptive_short_rule_router_v1 import load_climax, load_failed_high, load_support, state
    from scripts.tradex_short_fast10_guard_compare_v1 import profit_factor, report, route as route_b0
except ModuleNotFoundError:
    from tradex_adaptive_short_rule_router_v1 import load_climax, load_failed_high, load_support, state
    from tradex_short_fast10_guard_compare_v1 import profit_factor, report, route as route_b0


AXIS_ID = "tradex_short_rolling_permission_compare_v1"
OUT = Path(r"G:\Tradex\tradex_short_rolling_permission_compare_v1")
THRESHOLDS = {
    "permission": {"trailing_years": 4, "known_event_min": 30, "daily_profit_factor_min": 1.20, "daily_expectancy_positive": True},
    "keep": {"development_daily_pf_delta_min": .05, "development_utilization_min": .60, "development_trade_days_min": 40, "development_unique_codes_min": 20, "diagnostic_daily_pf_min": 1.10, "diagnostic_expectancy_positive": True, "diagnostic_utilization_min": .60, "diagnostic_trade_days_min": 12, "diagnostic_unique_codes_min": 10},
    "drop": {"opportunity_loss_rate_above": .40}, "otherwise": "hold",
}


def daily_metrics(frame: pd.DataFrame) -> dict:
    if frame.empty: return {"event_count": 0, "trade_days": 0, "daily_profit_factor": None, "daily_expectancy": None}
    daily = frame.groupby("signal_date", as_index=False).ret.mean().ret
    return {"event_count": int(len(frame)), "trade_days": int(frame.signal_date.nunique()), "daily_profit_factor": profit_factor(daily), "daily_expectancy": float(daily.mean())}


def permission_at(history: pd.DataFrame, signal_date: pd.Timestamp) -> dict:
    start = signal_date - pd.DateOffset(years=4)
    known = history[(history.outcome_known_date < signal_date) & (history.signal_date >= start)]
    m = daily_metrics(known)
    if m["event_count"] < 30:
        status, allowed = "insufficient_history", False
    else:
        allowed = bool((m["daily_profit_factor"] or 0) >= 1.20 and (m["daily_expectancy"] or 0) > 0)
        status = "permitted" if allowed else "denied_performance"
    return {"permission": allowed, "permission_status": status, "known_start": start, "known_n": m["event_count"], "known_trade_days": m["trade_days"], "known_daily_profit_factor": m["daily_profit_factor"], "known_daily_expectancy": m["daily_expectancy"]}


def route_b3(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    audit, routed = [], []
    for signal_date, day in events.groupby("signal_date", sort=True):
        prior = events[events.outcome_known_date < signal_date]
        eligible = []
        for rule in sorted(events.rule.unique()):
            base = state(prior[prior.rule == rule])
            permission = permission_at(events[events.rule == rule], signal_date)
            item = {"signal_date": signal_date, "rule": rule, **base, **permission}
            audit.append(item)
            if base["state"] == "Active" and permission["permission"]:
                eligible.append(item)
        eligible.sort(key=lambda item: (-item["score"], item["rule"]))
        allowed = {item["rule"] for item in eligible[:2]}
        selected = day[day.rule.isin(allowed)].copy()
        if not selected.empty:
            selected["router_score"] = selected.rule.map({item["rule"]: item["score"] for item in eligible})
            routed.append(selected.sort_values(["router_score", "code"], ascending=[False, True]).head(5))
    return pd.DataFrame(audit), pd.concat(routed, ignore_index=True) if routed else events.iloc[:0].copy()


def opportunity_loss(b0: pd.DataFrame, b3: pd.DataFrame) -> dict:
    keys = ["signal_date", "rule", "code"]
    kept = set(map(tuple, b3[keys].astype(str).to_numpy())) if not b3.empty else set()
    removed = b0[[*keys, "ret"]].copy()
    mask = [tuple(row) not in kept for row in removed[keys].astype(str).to_numpy()]
    removed = removed[mask]
    daily = removed.groupby(["rule", "signal_date"], as_index=False).ret.mean() if not removed.empty else removed
    return {"removed_event_count": int(len(removed)), "removed_family_signal_days": int(len(daily)), "profitable_removed_days": int((daily.ret > 0).sum()) if not daily.empty else 0, "opportunity_loss_rate": float((daily.ret > 0).mean()) if not daily.empty else None, "definition": "share of B0-routed family-signal-days removed by B3 whose counterfactual daily mean return was positive; evaluation-only"}


def branching(b0: pd.DataFrame, b3: pd.DataFrame) -> dict:
    k0 = set(zip(b0.signal_date.astype(str), b0.rule, b0.code.astype(str)))
    k3 = set(zip(b3.signal_date.astype(str), b3.rule, b3.code.astype(str)))
    dates0 = set(b0.signal_date.astype(str)); dates3 = set(b3.signal_date.astype(str))
    return {"changed_members_count": len(k0 ^ k3), "removed_members_count": len(k0 - k3), "added_members_count": len(k3 - k0), "changed_trade_day_count": len(dates0 ^ dates3), "selection_divergence_reason": "four_year_family_permission_denied_or_insufficient_history"}


def clean(v):
    if isinstance(v, float) and not math.isfinite(v): return None
    if isinstance(v, dict): return {k: clean(x) for k, x in v.items()}
    return v


def run() -> Path:
    support, sp = load_support(); failed, fp = load_failed_high(); climax, cp = load_climax()
    events = pd.concat([support, failed, climax], ignore_index=True).sort_values(["signal_date", "rule", "code"])
    _, b0 = route_b0(events, False); audit, b3 = route_b3(events)
    reports = {}
    for name, start, end in (("development_2019_2025", "2019-01-01", "2025-12-31"), ("diagnostic_2026", "2026-01-01", "2026-07-10")):
        p0 = b0[(b0.signal_date >= start) & (b0.signal_date <= end)]; p3 = b3[(b3.signal_date >= start) & (b3.signal_date <= end)]
        reports[name] = {"B0_current_recent20": report(p0, len(p0)), "B3_rolling_permission": report(p3, len(p0)), "opportunity_loss": opportunity_loss(p0, p3), "branching": branching(p0, p3)}
    d0, d3 = reports["development_2019_2025"]["B0_current_recent20"], reports["development_2019_2025"]["B3_rolling_permission"]
    v3 = reports["diagnostic_2026"]["B3_rolling_permission"]
    losses = [reports[x]["opportunity_loss"]["opportunity_loss_rate"] for x in reports]
    dev_ok = (d3["daily_profit_factor"] or 0) >= (d0["daily_profit_factor"] or 0) + .05 and (d3["utilization_vs_b0"] or 0) >= .60 and d3["trade_days"] >= 40 and d3["unique_codes"] >= 20
    val_ok = (v3["daily_profit_factor"] or 0) >= 1.10 and (v3["daily_expectancy"] or 0) > 0 and (v3["utilization_vs_b0"] or 0) >= .60 and v3["trade_days"] >= 12 and v3["unique_codes"] >= 10
    drop = any(x is not None and x > .40 for x in losses)
    decision = "drop" if drop else "keep" if dev_ok and val_ok else "hold"
    output = OUT / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    audit.to_csv(output / "point_in_time_permission_audit.csv", index=False); b0.to_csv(output / "B0_routed_events.csv", index=False); b3.to_csv(output / "B3_routed_events.csv", index=False)
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "fixed_evaluation_conditions": {"axis": "four_year_family_rolling_permission_only", "point_in_time": "outcome_known_date < signal_date", "permission": "n>=30, daily PF>=1.20, daily expectancy>0", "no_silent_fallback": True, "early_history": "insufficient_history is explicit denial", "B0_unchanged": True}, "predeclared_thresholds": THRESHOLDS, "source_artifacts": {"support_break": str(sp), "failed_high": str(fp), "climax_failure": str(cp)}, "reports": reports, "permission_status_counts": audit.permission_status.value_counts().to_dict(), "decision": {"candidate_local_decision": decision, "development_gate": dev_ok, "diagnostic_gate": val_ok, "opportunity_loss_drop_gate": drop, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_condition_four_year_family_permission"}, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    (output / "decision_thresholds.json").write_text(json.dumps(THRESHOLDS, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    path = output / "compare.json"; path.write_text(json.dumps(clean(payload), ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8"); print(path); return path


if __name__ == "__main__": run()
