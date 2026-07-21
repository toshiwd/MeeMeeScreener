from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from scripts.tradex_adaptive_short_rule_router_v1 import load_climax, load_failed_high, load_support, state
    from scripts.tradex_short_fast10_guard_compare_v1 import DECISION_THRESHOLDS, decide, oracle_instrumentation, profit_factor, report
except ModuleNotFoundError:
    from tradex_adaptive_short_rule_router_v1 import load_climax, load_failed_high, load_support, state
    from tradex_short_fast10_guard_compare_v1 import DECISION_THRESHOLDS, decide, oracle_instrumentation, profit_factor, report


AXIS_ID = "tradex_short_dual5of10_guard_compare_v1"
OUT = Path(r"G:\Tradex\tradex_short_dual5of10_guard_compare_v1")


def dual5of10_guard(history: pd.DataFrame) -> dict:
    recent = history.sort_values(["outcome_known_date", "signal_date"]).tail(10)
    n = len(recent)
    pf = profit_factor(recent.ret) if n else None
    loss_count5 = int((recent.tail(5).ret < 0).sum()) if n >= 5 else None
    triggered = n == 10 and loss_count5 >= 3 and pf is not None and pf < 1.0
    return {"dual10_n": n, "dual10_pf": pf, "loss_count5": loss_count5, "dual5of10_triggered": triggered}


def route(events: pd.DataFrame, use_guard: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshots, routed = [], []
    for signal_date, day in events.groupby("signal_date", sort=True):
        prior = events[events.outcome_known_date < signal_date]
        states = []
        for rule in sorted(events.rule.unique()):
            history = prior[prior.rule == rule]
            item = {"signal_date": signal_date, "rule": rule, **state(history), **dual5of10_guard(history)}
            item["base_state"] = item["state"]
            if use_guard and item["state"] == "Active" and item["dual5of10_triggered"]:
                item["state"] = "Watch"
            states.append(item); snapshots.append(item)
        eligible = [item for item in states if item["state"] == "Active"]
        eligible.sort(key=lambda item: (-item["score"], item["rule"]))
        allowed = {item["rule"] for item in eligible[:2]}
        selected = day[day.rule.isin(allowed)].copy()
        if not selected.empty:
            selected["router_score"] = selected.rule.map({item["rule"]: item["score"] for item in eligible})
            routed.append(selected.sort_values(["router_score", "code"], ascending=[False, True]).head(5))
    return pd.DataFrame(snapshots), pd.concat(routed, ignore_index=True) if routed else events.iloc[:0].copy()


def clean(value):
    if isinstance(value, float) and not math.isfinite(value): return None
    if isinstance(value, dict): return {key: clean(item) for key, item in value.items()}
    if isinstance(value, list): return [clean(item) for item in value]
    return value


def run() -> Path:
    support, support_path = load_support(); failed, failed_path = load_failed_high(); climax, climax_path = load_climax()
    events = pd.concat([support, failed, climax], ignore_index=True).sort_values(["signal_date", "rule", "code"])
    snap0, routed0 = route(events, False); snap2, routed2 = route(events, True)
    reports, instrumentation = {}, {}
    periods = (("development_2019_2025", "2019-01-01", "2025-12-31"), ("diagnostic_2026", "2026-01-01", "2026-07-10"))
    for name, start, end in periods:
        p0 = routed0[(routed0.signal_date >= start) & (routed0.signal_date <= end)]
        p2 = routed2[(routed2.signal_date >= start) & (routed2.signal_date <= end)]
        inst = oracle_instrumentation(events, snap2, start, end)
        instrumentation[name] = inst
        b2 = report(p2, len(p0)); b2["oracle_instrumentation"] = inst
        reports[name] = {"B0_current_recent20": report(p0, len(p0)), "B2_dual5of10_guard": b2}
    dev, diag = reports["development_2019_2025"], reports["diagnostic_2026"]
    judgment, reasons = decide(dev["B0_current_recent20"], dev["B2_dual5of10_guard"], diag["B0_current_recent20"], diag["B2_dual5of10_guard"], instrumentation)
    output = OUT / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    snap0.to_csv(output / "B0_point_in_time_states.csv", index=False); snap2.to_csv(output / "B2_point_in_time_states.csv", index=False)
    routed0.to_csv(output / "B0_routed_events.csv", index=False); routed2.to_csv(output / "B2_routed_events.csv", index=False)
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "fixed_evaluation_conditions": {"axis": "dual5of10_guard_only", "B0": "current recent20 unchanged", "B2": "n10==10 and last5 loss count>=3 and PF10<1.0 downgrades Active to Watch", "B1_combined": False, "point_in_time": "only outcome_known_date < signal_date", "development": "2019-01-01..2025-12-31", "diagnostic": "2026-01-01..2026-07-10", "same_universe_period_topk_regime_costs": True}, "predeclared_decision_thresholds": DECISION_THRESHOLDS, "source_artifacts": {"support_break": str(support_path), "failed_high": str(failed_path), "climax_failure": str(climax_path)}, "reports": reports, "instrumentation": instrumentation, "judgment": {"candidate_local_decision": judgment, "reason_type": "predeclared_fixed_condition_dual5of10_guard_comparison", "reasons": reasons, "authoritative_rollup_decision": "review_only"}, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    (output / "decision_thresholds.json").write_text(json.dumps(DECISION_THRESHOLDS, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    path = output / "compare.json"; path.write_text(json.dumps(clean(payload), ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    print(path); return path


if __name__ == "__main__": run()
