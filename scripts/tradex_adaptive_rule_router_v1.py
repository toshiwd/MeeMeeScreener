from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_long_short_weekly_coverage_v1 import metrics, weekly_coverage


AXIS_ID = "tradex_adaptive_rule_router_v1"
OUT = Path(r"G:\Tradex\adaptive_rule_router_v1")
UNION_ROOT = Path(r"G:\Tradex\momentum_reentry_h10_union_v1")
MA_ROOT = Path(r"G:\Tradex\long_ma_weekly_reversal_axis_v1")
CURRENT_ROOT = Path(r"G:\Tradex\unified_current_opportunity_board_v1")
DORMANT_ROOT = Path(r"G:\Tradex\adaptive_dormant_family_events_v1")
CURRENT_FAMILY_ROOT = Path(r"G:\Tradex\adaptive_current_family_scan_v1")
CAPITULATION_ROOT = Path(r"G:\Tradex\riskoff_capitulation_reversal_long_v1")
MA20_RECLAIM_ROOT = Path(r"G:\Tradex\ma20_reclaim_family_events_v1")
ROLLING_PERMISSION_RULES = {"volatility_contraction_breakout", "riskoff_capitulation_reversal_long"}


def json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    return value


def latest(root: Path, name: str) -> Path:
    files = sorted(root.glob(f"*/{name}"), key=lambda path: path.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"{name} not found under {root}")
    return files[-1]


def profit_factor(values: pd.Series) -> float | None:
    gain = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    return gain / loss if loss else None


def simulate_ma60(db_path: Path) -> pd.DataFrame:
    source = latest(MA_ROOT, "selected_events.csv")
    frame = pd.read_csv(source)
    lookup = frame[["code", "d10"]].copy()
    lookup["code"] = lookup.code.astype(str)
    with duckdb.connect(str(db_path), read_only=True) as db:
        db.register("ma_exit_dates", lookup)
        closes = db.execute("""
          SELECT x.code,x.d10,b.c AS close10
          FROM ma_exit_dates x JOIN daily_bars b
            ON b.source='pan' AND b.code=x.code AND b.date=x.d10
        """).fetchdf()
    closes["code"] = closes.code.astype(str)
    close_map = {(row.code, int(row.d10)): float(row.close10) for row in closes.itertuples()}
    rows = []
    for item in frame.to_dict("records"):
        entry = float(item["next_open"])
        ret, exit_offset = None, 10
        for offset in range(1, 11):
            if float(item[f"l{offset}"]) <= entry * .95:
                ret, exit_offset = -.05, offset
                break
            if float(item[f"h{offset}"]) >= entry * 1.08:
                ret, exit_offset = .08, offset
                break
        if ret is None:
            ret = close_map[(str(item["code"]), int(item["d10"]))] / entry - 1
        rows.append({
            "side": "buy", "code": str(item["code"]), "signal_date": pd.to_datetime(str(int(item["ymd"]))),
            "entry_date": pd.to_datetime(int(item["next_entry_date"]), unit="s"), "ret": ret,
            "rule": "ma60_weekly_reversal", "exit_offset": exit_offset,
        })
    return pd.DataFrame(rows)


def regime_for_breadth(value: float) -> str:
    if value >= .60:
        return "broad_up"
    if value <= .45:
        return "risk_off"
    return "mixed"


def status(history: pd.DataFrame, regime: str) -> dict:
    recent20 = history.tail(20)
    recent60 = history.tail(60)
    same = history[history.regime == regime].tail(20)
    pf20 = profit_factor(recent20.ret) if len(recent20) else None
    pf60 = profit_factor(recent60.ret) if len(recent60) else None
    pf_same = profit_factor(same.ret) if len(same) else None
    exp20 = float(recent20.ret.mean()) if len(recent20) else None
    exp60 = float(recent60.ret.mean()) if len(recent60) else None
    exp_same = float(same.ret.mean()) if len(same) else None
    if len(same) >= 15 and (pf_same or 0) >= 1.30 and (exp_same or 0) > 0 and (pf60 or 0) >= .95:
        state = "Active"
    elif len(same) >= 10 and (pf_same or 0) >= 1.05 and (exp_same or 0) > 0 and (pf60 or 0) >= .90:
        state = "Secondary"
    elif len(same) >= 8 and (pf_same or 0) >= 1.0 and (exp_same or 0) >= 0:
        state = "Watch"
    else:
        state = "Dormant"
    score = (
        (min(pf_same or 0, 3) * 0.50)
        + (min(pf60 or 0, 3) * 0.20)
        + (min(pf20 or 0, 3) * 0.15)
        + (max(min((exp_same or 0) * 20, 1), -1) * 0.15)
    )
    return {
        "state": state, "score": score, "n20": len(recent20), "pf20": pf20, "expectancy20": exp20,
        "n60": len(recent60), "pf60": pf60, "expectancy60": exp60,
        "same_regime_n": len(same), "same_regime_pf": pf_same, "same_regime_expectancy": exp_same,
    }


def apply_policy(candidates: pd.DataFrame, snapshots: pd.DataFrame, states: set[str], top_rules: int, recent_guard: bool = False) -> pd.DataFrame:
    routed = []
    for entry_date, day in candidates.groupby("entry_date", sort=True):
        eligible = snapshots[(snapshots.entry_date == entry_date) & snapshots.state.isin(states) & snapshots.permission_allowed]
        if recent_guard:
            eligible = eligible[(eligible.n20 >= 15) & (eligible.pf20 >= 1.0) & (eligible.expectancy20 > 0)]
        eligible = eligible.sort_values(["score", "rule"], ascending=[False, True])
        if eligible.empty:
            continue
        allowed = set(eligible.head(top_rules).rule)
        selected = day[day.rule.isin(allowed)].copy()
        if selected.empty:
            continue
        score_map = eligible.set_index("rule").score.to_dict()
        state_map = eligible.set_index("rule").state.to_dict()
        selected["router_score"] = selected.rule.map(score_map)
        selected["router_state"] = selected.rule.map(state_map)
        routed.append(selected.sort_values(["router_score", "rule", "code"], ascending=[False, True, True]).head(5))
    return pd.concat(routed, ignore_index=True) if routed else candidates.iloc[:0].copy()


def run() -> Path:
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    union_path = latest(UNION_ROOT, "union_events.csv")
    candidates = pd.read_csv(union_path, parse_dates=["signal_date", "entry_date"])
    dormant_path = latest(DORMANT_ROOT, "dormant_family_events.csv")
    dormant = pd.read_csv(dormant_path, parse_dates=["signal_date", "entry_date"])
    capitulation_path = latest(CAPITULATION_ROOT, "events.csv")
    capitulation = pd.read_csv(capitulation_path, parse_dates=["signal_date", "entry_date"])
    capitulation = capitulation.drop(columns=["breadth_above_ma20", "regime"], errors="ignore")
    ma20_reclaim_path = latest(MA20_RECLAIM_ROOT, "events.csv")
    ma20_reclaim = pd.read_csv(ma20_reclaim_path, parse_dates=["signal_date", "entry_date"])
    candidates = pd.concat([candidates, simulate_ma60(db_path), dormant, capitulation, ma20_reclaim], ignore_index=True)
    candidates["code"] = candidates.code.astype(str)
    candidates["outcome_known_date"] = candidates.entry_date + pd.Timedelta(days=20)

    signal_dates = pd.DataFrame({"date": sorted(candidates.signal_date.dt.strftime("%Y%m%d").astype(int).unique())})
    query = """
    WITH base AS (
      SELECT CASE WHEN date>30000000 THEN CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ELSE CAST(date AS INTEGER) END ymd,
        code,c,avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20
      FROM daily_bars WHERE source='pan'
    )
    SELECT ymd,avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) breadth_above_ma20
    FROM base JOIN signal_dates ON ymd=signal_dates.date WHERE ma20 IS NOT NULL GROUP BY ymd
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        db.register("signal_dates", signal_dates)
        breadth = db.execute(query).fetchdf()
    breadth["signal_date"] = pd.to_datetime(breadth.ymd.astype(str))
    candidates = candidates.merge(breadth[["signal_date", "breadth_above_ma20"]], on="signal_date", how="left")
    candidates["regime"] = candidates.breadth_above_ma20.map(regime_for_breadth)
    candidates = candidates.sort_values(["entry_date", "rule", "code"]).reset_index(drop=True)

    permission_rows = []
    permission_source = candidates[(candidates.entry_date >= "2019-01-01") & (candidates.entry_date <= "2025-12-31")]
    for (rule, regime), group in permission_source.groupby(["rule", "regime"]):
        group_pf = profit_factor(group.ret)
        allowed = bool(len(group) >= 30 and (group_pf or 0) >= 1.30 and float(group.ret.mean()) > 0)
        permission_rows.append({"rule": rule, "regime": regime, "sample_count": len(group), "profit_factor": group_pf, "expectancy": float(group.ret.mean()), "allowed": allowed})
    permission_frame = pd.DataFrame(permission_rows)
    regime_permissions = set(map(tuple, permission_frame[permission_frame.allowed][["rule", "regime"]].values.tolist()))

    def permission_at(rule: str, regime: str, as_of: pd.Timestamp, prior: pd.DataFrame) -> tuple[bool, str, dict]:
        if rule not in ROLLING_PERMISSION_RULES:
            return (rule, regime) in regime_permissions, "fixed_2019_2025", {}
        start = as_of - pd.DateOffset(years=4)
        known = prior[(prior.rule == rule) & (prior.regime == regime) & (prior.entry_date >= start)].sort_values("entry_date")
        m = metrics(known)
        allowed = bool((m.get("event_count") or 0) >= 30 and (m.get("daily_profit_factor") or 0) >= 1.2 and (m.get("daily_expectancy") or 0) > 0)
        return allowed, "rolling_4y_point_in_time", {"permission_n": m.get("event_count"), "permission_daily_pf": m.get("daily_profit_factor"), "permission_daily_expectancy": m.get("daily_expectancy")}

    snapshots = []
    for entry_date, day in candidates.groupby("entry_date", sort=True):
        prior = candidates[candidates.outcome_known_date < entry_date]
        evaluated = []
        for rule, group in day.groupby("rule"):
            current_regime = str(group.iloc[0].regime)
            rule_history = prior[prior.rule == rule].sort_values("entry_date")
            allowed, permission_mode, permission_metrics = permission_at(rule, current_regime, entry_date, prior)
            item = {"entry_date": entry_date, "rule": rule, "regime": current_regime, **status(rule_history, current_regime), "permission_allowed": allowed, "permission_mode": permission_mode, **permission_metrics}
            evaluated.append(item)
            snapshots.append(item)
    snapshots_frame = pd.DataFrame(snapshots)

    policies = [
        {"policy": "active_secondary_top3", "states": {"Active", "Secondary"}, "top_rules": 3},
        {"policy": "active_only_top3", "states": {"Active"}, "top_rules": 3},
        {"policy": "active_recent_guard_top3", "states": {"Active"}, "top_rules": 3, "recent_guard": True},
        {"policy": "active_only_top2", "states": {"Active"}, "top_rules": 2},
        {"policy": "active_only_top1", "states": {"Active"}, "top_rules": 1},
    ]
    policy_results = []
    routed_by_policy = {}
    for policy in policies:
        frame = apply_policy(candidates, snapshots_frame, policy["states"], policy["top_rules"], policy.get("recent_guard", False))
        routed_by_policy[policy["policy"]] = frame
        development = frame[(frame.entry_date >= "2019-01-01") & (frame.entry_date <= "2025-12-31")]
        oos = frame[(frame.entry_date >= "2026-01-01") & (frame.entry_date <= "2026-07-10")]
        yearly = []
        for year in range(2019, 2026):
            year_frame = development[development.entry_date.dt.year == year]
            yearly.append({"year": year, **metrics(year_frame)})
        year_pfs = [row["daily_profit_factor"] or 0 for row in yearly]
        item = {"policy": policy["policy"], "states": sorted(policy["states"]), "top_rules": policy["top_rules"], "development_metrics": metrics(development), "development_weekly_coverage": weekly_coverage(development, "2019-01-01", "2025-12-31"), "development_yearly": yearly, "development_red_year_count": sum((row.get("daily_expectancy") or 0) <= 0 for row in yearly), "development_minimum_year_daily_pf": min(year_pfs) if year_pfs else None, "diagnostic_2026_metrics": metrics(oos), "diagnostic_2026_weekly_coverage": weekly_coverage(oos, "2026-01-01", "2026-07-10")}
        item["recent_guard"] = policy.get("recent_guard", False)
        item["development_gate_pass"] = bool((item["development_metrics"].get("daily_profit_factor") or 0) >= 1.2 and (item["development_metrics"].get("daily_expectancy") or 0) > 0 and (item["development_weekly_coverage"].get("average_events_per_calendar_week") or 0) >= 1.0)
        policy_results.append(item)
    chosen = next(item for item in policy_results if item["policy"] == "active_recent_guard_top3")
    routed_frame = routed_by_policy[chosen["policy"]] if chosen else candidates.iloc[:0].copy()

    periods = {"development_2019_2025": ("2019-01-01", "2025-12-31"), "untouched_2026": ("2026-01-01", "2026-07-10")}
    reports = {}
    for name, (start, end) in periods.items():
        part = routed_frame[(routed_frame.entry_date >= start) & (routed_frame.entry_date <= end)]
        reports[name] = {"metrics": metrics(part), "weekly_coverage": weekly_coverage(part, start, end), "rule_counts": part.rule.value_counts().to_dict()}
    test = reports["untouched_2026"]
    passed = bool((test["metrics"].get("daily_profit_factor") or 0) >= 1.2 and (test["metrics"].get("daily_expectancy") or 0) > 0 and (test["weekly_coverage"].get("average_events_per_calendar_week") or 0) >= 1.0)

    confirmed_date = pd.to_datetime(runtime["latest_confirmed_daily_bars_date_iso"])
    current_source = latest(CURRENT_ROOT, "current_opportunity_board.json")
    current_payload = json.loads(current_source.read_text(encoding="utf-8"))
    current_family_source = latest(CURRENT_FAMILY_ROOT, "current_family_scan.json")
    current_family_payload = json.loads(current_family_source.read_text(encoding="utf-8"))
    current_breadth = float(current_payload["market_breadth"]["above_ma20"])
    current_regime = regime_for_breadth(current_breadth)
    known_now = candidates[candidates.outcome_known_date < confirmed_date]
    current_states = []
    for rule in sorted(candidates.rule.unique()):
        rule_history = known_now[known_now.rule == rule].sort_values("entry_date")
        allowed, permission_mode, permission_metrics = permission_at(rule, current_regime, confirmed_date, known_now)
        current_states.append({"as_of": confirmed_date.strftime("%Y-%m-%d"), "rule": rule, "regime": current_regime, **status(rule_history, current_regime), "regime_permission_allowed": allowed, "permission_mode": permission_mode, **permission_metrics})
    current_states_frame = pd.DataFrame(current_states).sort_values(["score", "rule"], ascending=[False, True])
    state_map = current_states_frame.set_index("rule").state.to_dict()
    score_map = current_states_frame.set_index("rule").score.to_dict()
    current_eligible = current_states_frame[current_states_frame.state.isin(set(chosen["states"]) if chosen else set()) & current_states_frame.regime_permission_allowed].copy()
    if chosen and chosen.get("recent_guard"):
        current_eligible = current_eligible[(current_eligible.n20 >= 15) & (current_eligible.pf20 >= 1.0) & (current_eligible.expectancy20 > 0)]
    current_eligible = current_eligible.head(int(chosen["top_rules"]) if chosen else 0).copy()
    current_priority = {rule: rank for rank, rule in enumerate(current_eligible.rule, start=1)}
    current_candidates = []
    rule_alias = {"shallow_high_zone_leaf_9": "leaf9", "shallow_high_zone_leaf_14": "leaf14", "shallow_high_zone_leaf_20": "leaf20"}
    current_source_rows = list(current_payload.get("candidates", [])) + list(current_family_payload.get("candidates", []))
    for row in current_source_rows:
        rule = rule_alias.get(row.get("rule"), row.get("rule"))
        routed_state = state_map.get(rule, "Dormant")
        routed = rule in current_priority
        current_candidates.append({**row, "router_rule": rule, "router_state": routed_state, "router_score": score_map.get(rule), "router_priority_rank": current_priority.get(rule), "router_verdict": "review_entry" if routed else "watch_not_routed"})
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    candidates.to_csv(output / "unified_rule_event_ledger.csv", index=False)
    snapshots_frame.to_csv(output / "point_in_time_rule_states.csv", index=False)
    routed_frame.to_csv(output / "routed_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "rules": sorted(candidates.rule.unique().tolist()), "outcome_availability_delay": "entry date + 20 calendar days",
            "activation_policy_axis": [policy["policy"] for policy in policies],
            "policy_selection": "fixed operational contract: Active permission-allowed rules with lagged recent20 n>=15 PF>=1.0 and positive expectancy; top3 by lagged score",
            "active_gate": "same-regime recent n>=15 PF>=1.3 exp>0 and all-regime PF60>=0.95",
            "secondary_gate": "same-regime recent n>=10 PF>=1.05 exp>0 and all-regime PF60>=0.90", "holdings": "ignored", "capital": "not used", "costs": "ignored",
            "regime_permission": "2019-2025 fixed rule-regime cells with n>=30, PF>=1.3, positive expectancy",
        },
        "source_artifacts": {"union_events": str(union_path), "ma60_events": str(latest(MA_ROOT, "selected_events.csv")), "dormant_family_events": str(dormant_path), "capitulation_events": str(capitulation_path), "ma20_reclaim_events": str(ma20_reclaim_path), "current_family_scan": str(current_family_source), "runtime_db": str(db_path)},
        "regime_permissions": permission_frame.to_dict("records"), "policy_variants": policy_results, "selected_policy": chosen, "reports": reports, "current_as_of": confirmed_date.strftime("%Y-%m-%d"), "current_regime": current_regime,
        "current_rule_states": current_states_frame.to_dict("records"), "current_active_rule_priority": current_eligible.to_dict("records"), "current_candidates": current_candidates,
        "adoption_gate": {"2026_daily_pf_gte_1_2": True, "2026_expectancy_positive": True, "2026_average_events_per_week_gte_1": True, "pass": passed},
        "decision": {"candidate_local_decision": "keep_forward_monitor" if passed else "hold", "authoritative_rollup_decision": "research_only", "reason_type": "fixed_active_only_router_gate_pass_requires_forward_confirmation" if passed else "adaptive_router_gate_failed"},
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False, "silent_fallback_used": False,
    }
    path = output / "compare.json"
    path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
