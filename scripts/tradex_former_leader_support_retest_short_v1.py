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

from scripts.tradex_long_short_weekly_coverage_v1 import metrics


AXIS_ID = "tradex_former_leader_support_retest_short_v1"
OUT = Path(r"G:\Tradex\former_leader_support_retest_short_v1")
TP, SL, HOLD = 0.10, 0.05, 10


def _evaluate(raw: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for item in raw.to_dict("records"):
        trigger = float(item["l"])
        if float(item["l1"]) > trigger:
            continue
        entry = min(trigger, float(item["o1"]))
        ret, exit_offset = None, HOLD
        for offset in range(1, HOLD + 1):
            if float(item[f"h{offset}"]) >= entry * (1 + SL):
                ret, exit_offset = -SL, offset
                break
            if float(item[f"l{offset}"]) <= entry * (1 - TP):
                ret, exit_offset = TP, offset
                break
        if ret is None:
            ret = 1 - float(item[f"c{HOLD}"]) / entry
        is_former_leader = bool(item["former_leader"])
        peak = float(item["recent_leader_peak"]) if is_former_leader else float("nan")
        full_drawdown_horizon = int(item["future_count120"]) == 120
        future_low120 = float(item["future_low120"])
        drawdown120 = 1 - future_low120 / peak if is_former_leader and full_drawdown_horizon else None
        rows.append(
            {
                "code": str(item["code"]),
                "signal_date": pd.to_datetime(int(item["date"]), unit="s"),
                "entry_date": pd.to_datetime(int(item["d1"]), unit="s"),
                "ret": ret,
                "exit_offset": exit_offset,
                "former_leader": is_former_leader,
                "leader_rank_peak": float(item["recent_leader_rank"]),
                "leader_return_peak": float(item["recent_leader_return"]),
                "drawdown_from_leader_peak_120d": drawdown120,
                "hit_50pct_drawdown_120d": drawdown120 >= 0.50 if drawdown120 is not None else None,
                "hit_80pct_drawdown_120d": drawdown120 >= 0.80 if drawdown120 is not None else None,
            }
        )
    return pd.DataFrame(rows)


def _report(events: pd.DataFrame) -> dict:
    report: dict = {}
    periods = [
        ("development_2019_2023", "2019-01-01", "2023-12-31"),
        ("test_2024_2025", "2024-01-01", "2025-12-31"),
        ("forward_2026", "2026-01-01", "2026-07-10"),
    ]
    for label, start, end in periods:
        period = events[(events.entry_date >= start) & (events.entry_date <= end)]
        groups = {}
        for group_name, part in (
            ("all_support_retest_failures", period),
            ("former_leader_only", period[period.former_leader]),
        ):
            base = metrics(part)
            diagnostic = part[part.former_leader & part.drawdown_from_leader_peak_120d.notna()]
            base.update(
                {
                    "former_leader_drawdown_diagnostic_count": int(len(diagnostic)),
                    "hit_50pct_drawdown_120d_rate": float(diagnostic.hit_50pct_drawdown_120d.mean()) if len(diagnostic) else None,
                    "hit_80pct_drawdown_120d_rate": float(diagnostic.hit_80pct_drawdown_120d.mean()) if len(diagnostic) else None,
                    "median_drawdown_from_leader_peak_120d": float(diagnostic.drawdown_from_leader_peak_120d.median()) if len(diagnostic) else None,
                }
            )
            groups[group_name] = base
        report[label] = groups
    return report


def run() -> Path:
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    freshness = get_rankings_freshness(direction="down", limit=20)
    db_path = Path(runtime["selected_runtime_db_path"])
    leads = ",".join(
        f"lead(o,{i}) over w o{i},lead(h,{i}) over w h{i},lead(l,{i}) over w l{i},lead(c,{i}) over w c{i},lead(date,{i}) over w d{i}"
        for i in range(1, HOLD + 1)
    )
    sql = f"""
    WITH base AS (
      SELECT code,date,o,h,l,c,v,row_number() OVER w rn,
        lag(c,120) OVER w c120,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior_low20,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 120 FOLLOWING) future_low120,
        count(*) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 120 FOLLOWING) future_count120,
        {leads}
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), ranked AS (
      SELECT *,c/c120-1 ret120,
        percent_rank() OVER(PARTITION BY date ORDER BY c/c120-1) leader_rank
      FROM base WHERE c120 IS NOT NULL
    ), history AS (
      SELECT *,
        max(CASE WHEN leader_rank>=.90 AND ret120>=.50 THEN 1 ELSE 0 END)
          OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) former_leader,
        max(CASE WHEN leader_rank>=.90 AND ret120>=.50 THEN leader_rank END)
          OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) recent_leader_rank,
        max(CASE WHEN leader_rank>=.90 AND ret120>=.50 THEN ret120 END)
          OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) recent_leader_return,
        max(CASE WHEN leader_rank>=.90 AND ret120>=.50 THEN c END)
          OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) recent_leader_peak
      FROM ranked
    ), breaks AS (
      SELECT code,date break_date,rn break_rn,prior_low20 broken_support
      FROM history WHERE prior_low20 IS NOT NULL AND c<prior_low20
    ), pairs AS (
      SELECT s.*,k.break_date,k.broken_support,
        row_number() OVER(PARTITION BY s.code,s.date ORDER BY k.break_rn DESC) anchor_rank
      FROM history s JOIN breaks k ON s.code=k.code AND s.rn-k.break_rn BETWEEN 1 AND 5
      WHERE s.d{HOLD} IS NOT NULL
        AND s.h>=k.broken_support*.98 AND s.h<=k.broken_support*1.03
        AND s.c<k.broken_support AND s.c<s.o AND (s.c-s.l)/nullif(s.h-s.l,0)<=.40
    )
    SELECT * FROM pairs WHERE anchor_rank=1 AND date>=1546300800 ORDER BY date,code
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        raw = db.execute(sql).fetchdf()
    events = _evaluate(raw)
    reports = _report(events)
    test = reports["test_2024_2025"]
    base, leader = test["all_support_retest_failures"], test["former_leader_only"]
    sufficient = (leader.get("event_count") or 0) >= 30
    improved = (
        sufficient
        and (leader.get("daily_expectancy") or 0) > (base.get("daily_expectancy") or 0)
        and (leader.get("daily_profit_factor") or 0) >= 1.20
    )
    forward = reports["forward_2026"]["former_leader_only"]
    forward_confirmed = (forward.get("event_count") or 0) >= 5 and (forward.get("daily_expectancy") or 0) > 0
    decision = "keep" if improved and forward_confirmed else "hold" if improved else "drop"
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    events.to_csv(output / "events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "all codes with latest confirmed PAN coverage",
            "periods": ["2019-2023 development", "2024-2025 test", "2026 forward through confirmed 2026-07-10"],
            "champion": "all 20d-support-break then 1-5d old-support retest failures",
            "changed_axis_only": "former leader: in prior 60 sessions, 120-session return was cross-sectional top decile and at least +50%",
            "entry": "next-session signal-low break; fill=min(signal low,next open); no entry without break",
            "exit": "short TP10 SL5 H10 stop-first including entry day",
            "cost_slippage_borrow": "ignored",
            "drawdown_diagnostic": "future 120-session low versus observable recent former-leader peak; not an ex-post major-top label",
        },
        "runtime_freshness": {"runtime": runtime, "down_rankings": freshness},
        "reports": reports,
        "observed_branching": {
            "baseline_event_count": int(len(events)),
            "former_leader_event_count": int(events.former_leader.sum()),
            "changed_members_count": int((~events.former_leader).sum()),
            "selection_divergence_reason": "former_leader_history_filter",
        },
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "research_only",
            "reason_type": "test_and_forward_gate_pass" if decision == "keep" else "test_edge_forward_insufficient" if decision == "hold" else "former_leader_filter_did_not_improve_fixed_entry_rule",
        },
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "automatic_meemee_reflection": False,
    }
    path = output / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
