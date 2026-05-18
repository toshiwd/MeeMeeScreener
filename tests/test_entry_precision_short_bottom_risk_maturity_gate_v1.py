from __future__ import annotations

import csv
import json
from argparse import Namespace
from datetime import date, timedelta
from pathlib import Path

import duckdb

from scripts import entry_precision_short_bottom_risk_maturity_gate_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _business_dates(start: date, count: int) -> list[int]:
    out: list[int] = []
    cursor = start
    while len(out) < count:
        if cursor.weekday() < 5:
            out.append(int(cursor.strftime("%Y%m%d")))
        cursor += timedelta(days=1)
    return out


def _build_runtime_db(path: Path, *, dates_by_code: dict[str, list[int]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE daily_bars (code VARCHAR, date BIGINT)")
        rows = [(code, int(ymd)) for code, dates in dates_by_code.items() for ymd in dates]
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?)", rows)
    finally:
        conn.close()
    return path


def _build_source_roots(tmp_path: Path, *, rows: list[dict]) -> Path:
    diagnostic_root = tmp_path / "diagnostic_root"
    closed_root = tmp_path / "closed_root"
    _write_csv(diagnostic_root / "short_bottom_risk_confusion_groups.csv", rows)
    compare = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_closed_horizon_compare_v1",
        "session_id": "synthetic",
        "generated_at": "2026-05-17T00:00:00+00:00",
        "source_root": str(diagnostic_root),
        "baseline_id": "current_rule_trade_gate_baseline",
        "challenger_id": "short_cleanup_bottom_risk_v1",
        "closed_horizon_summary": {
            "baseline": {"count": 14, "hit_rate": 0.57, "mean_ret20": 0.006, "median_ret20": 0.007},
            "challenger": {"count": 8, "hit_rate": 0.75, "mean_ret20": 0.022, "median_ret20": 0.012},
            "delta": {
                "hit_rate_delta": 0.18,
                "mean_ret20_delta": 0.016,
                "median_ret20_delta": 0.005,
                "retained_bad_known": 2,
                "removed_good_known": 2,
                "removed_bad_known": 4,
                "kept_good_known": 6,
                "baseline_unknown_count": 17,
                "challenger_unknown_count": 7,
                "removed_unknown_count": 10,
                "retained_unknown_count": 7,
            },
            "closed_horizon_keep_persistence": True,
            "completed_month_count": 5,
        },
    }
    monthly = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_monthly_stability_v1",
        "session_id": "synthetic",
        "completed_months": [],
        "rollup": {
            "completed_bucket_count": 5,
            "months_with_both_sides": 3,
            "months_with_challenger_absent": 2,
            "months_with_mean_ret20_gain": 1,
            "months_with_mean_ret20_loss": 1,
            "months_with_mean_ret20_flat": 1,
            "mixed_stability": True,
        },
    }
    unknown_impact = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_unknown_impact_v1",
        "unknown_materiality": True,
        "baseline_unknown_count": 17,
        "challenger_unknown_count": 7,
    }
    decision = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_stability_decision_v1",
        "decision": "hold_until_unknown_horizon_completes",
    }
    no_lookahead = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_closed_horizon_no_lookahead_audit_v1",
        "no_lookahead_pass": True,
        "future_outcome_fields_used_in_selection": [],
    }
    _write_json(closed_root / "short_bottom_risk_closed_horizon_compare.json", compare)
    _write_json(closed_root / "short_bottom_risk_monthly_stability.json", monthly)
    _write_json(closed_root / "short_bottom_risk_unknown_impact.json", unknown_impact)
    _write_json(closed_root / "short_bottom_risk_stability_decision.json", decision)
    _write_json(closed_root / "no_lookahead_audit.json", no_lookahead)
    return closed_root


def _source_row(ymd: int, code: str, group: str, *, baseline: bool, challenger: bool, known: bool) -> dict[str, str]:
    return {
        "ymd": str(ymd),
        "code": code,
        "confusion_group": group,
        "baseline_selected": str(baseline),
        "challenger_selected": str(challenger),
        "outcome_known": str(known),
        "outcome_positive": "",
        "outcome_bucket": "missing",
        "short_ret_20": "",
        "short_ret_10": "",
        "short_ret_5": "",
        "close_pos": "0.1",
        "dist_low20": "0.002",
        "dist_ma20_signed": "-0.03",
        "day_change_pct": "-0.02",
        "monthlyRangeProb": "0.2",
        "monthlyRangePos": "0.1",
        "weeklyBreakoutDownProb": "0.7",
        "monthlyBreakoutDownProb": "0.6",
        "marketRiskOff": "True",
        "marketRegime": "risk_off",
        "trendDownStrict": "True",
        "entryScore": "0.8",
        "tradePriorityScore": "0.7",
        "liquidity20d": "1000000",
        "mae20": "0.01",
        "mfe20": "0.03",
        "baseline_rank": "1",
        "tradeDecisionReasons": "[]",
        "tradeRiskWatch": "[]",
    }


def test_synthetic_root_waits_until_full_horizon_matures(tmp_path: Path, monkeypatch) -> None:
    dates = _business_dates(date(2025, 3, 3), 25)
    runtime_db = _build_runtime_db(
        tmp_path / "stocks.duckdb",
        dates_by_code={
            "A100": dates,
            "A200": dates,
            "A300": dates,
        },
    )
    source_rows = [
        _source_row(dates[0], "A100", "retained_unknown", baseline=True, challenger=True, known=False),
        _source_row(dates[9], "A200", "removed_unknown", baseline=True, challenger=False, known=False),
        _source_row(dates[4], "A300", "retained_unknown", baseline=True, challenger=True, known=False),
    ]
    source_root = _build_source_roots(tmp_path, rows=source_rows)

    runtime_status = {
        "confirmed": True,
        "selected_runtime_db_path": str(runtime_db),
        "latest_available_global_date": dates[-1],
        "latest_available_global_date_iso": date.fromisoformat(str(dates[-1])[:4] + "-" + str(dates[-1])[4:6] + "-" + str(dates[-1])[6:8]).isoformat(),
        "freshness_state": "fresh",
    }
    freshness = {
        "freshness_state": "fresh",
        "freshness_days": 1,
        "stale": False,
        "current_candidate_available": True,
        "snapshot_as_of": "2025-04-04",
    }
    monkeypatch.setattr(mod, "_get_runtime_stock_db_status", lambda: dict(runtime_status))
    monkeypatch.setattr(mod, "_get_rankings_freshness", lambda **_kwargs: dict(freshness))

    result = mod.run(
        Namespace(
            source_root=str(source_root),
            output_root=str(tmp_path / "out"),
        )
    )

    assert result["decision"] == "wait_until_full_horizon_matures"
    assert result["partial_recheck_ready_now"] is True
    assert result["full_recheck_ready_now"] is False
    out_root = Path(result["output_dir"])
    decision = json.loads((out_root / "short_bottom_risk_frozen_watch_decision.json").read_text(encoding="utf-8"))
    plan = json.loads((out_root / "short_bottom_risk_recheck_plan.json").read_text(encoding="utf-8"))
    assert decision["decision"] == "wait_until_full_horizon_matures"
    assert plan["recheck_state"] == "ready_for_partial_recheck"
    rows = list(csv.DictReader((out_root / "short_bottom_risk_unknown_rows.csv").open("r", encoding="utf-8")))
    assert any(row["current_eligibility_state"] in {"waiting_for_horizon", "waiting_for_future_sessions"} for row in rows)
    assert any(row["current_eligibility_state"] == "matured_available_now" for row in rows)


def test_real_closed_horizon_root_is_keep_frozen_watch_candidate(tmp_path: Path, monkeypatch) -> None:
    source_root = Path(
        r"G:\Tradex\entry_precision_short_bottom_risk_closed_horizon_stability_v1\20260517T030047Z-entry-short-bottom-risk-closed-horizon-stability-v1"
    )
    assert (source_root / "short_bottom_risk_closed_horizon_compare.json").exists()
    assert (source_root / "short_bottom_risk_monthly_stability.json").exists()
    assert (source_root / "short_bottom_risk_unknown_impact.json").exists()
    assert (source_root / "short_bottom_risk_stability_decision.json").exists()
    compare = json.loads((source_root / "short_bottom_risk_closed_horizon_compare.json").read_text(encoding="utf-8"))
    runtime_db = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
    runtime_status = {
        "confirmed": True,
        "selected_runtime_db_path": str(runtime_db),
        "latest_available_global_date": 20260515,
        "latest_available_global_date_iso": "2026-05-15",
        "freshness_state": "fresh",
    }
    freshness = {
        "freshness_state": "fresh",
        "freshness_days": 2,
        "stale": False,
        "current_candidate_available": True,
        "snapshot_as_of": "2026-05-14",
    }
    monkeypatch.setattr(mod, "_get_runtime_stock_db_status", lambda: dict(runtime_status))
    monkeypatch.setattr(mod, "_get_rankings_freshness", lambda **_kwargs: dict(freshness))

    result = mod.run(
        Namespace(
            source_root=str(source_root),
            output_root=str(tmp_path / "out"),
        )
    )

    assert result["decision"] == "keep_frozen_watch_candidate"
    assert result["full_recheck_ready_now"] is True
    assert result["permanently_unresolvable_count"] == 0
    out_root = Path(result["output_dir"])
    for name in [
        "short_bottom_risk_maturity_gate_contract.json",
        "short_bottom_risk_unknown_rows.csv",
        "short_bottom_risk_maturity_calendar.json",
        "short_bottom_risk_recheck_plan.json",
        "short_bottom_risk_recheck_acceptance_gate.json",
        "short_bottom_risk_frozen_watch_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert (out_root / name).exists()

    unknown_rows = list(csv.DictReader((out_root / "short_bottom_risk_unknown_rows.csv").open("r", encoding="utf-8")))
    assert len(unknown_rows) == compare["closed_horizon_summary"]["delta"]["baseline_unknown_count"]
    assert all(row["current_eligibility_state"] == "matured_available_now" for row in unknown_rows)
    calendar = json.loads((out_root / "short_bottom_risk_maturity_calendar.json").read_text(encoding="utf-8"))
    assert calendar["maturity_summary"]["full_ready_now"] is True
    assert calendar["maturity_summary"]["all_unknown_rows_matured_now"] is True
    plan = json.loads((out_root / "short_bottom_risk_recheck_plan.json").read_text(encoding="utf-8"))
    assert plan["decision"] == "ready_for_full_recheck"
    assert plan["recheck_state"] == "ready_for_full_recheck"
    gate = json.loads((out_root / "short_bottom_risk_recheck_acceptance_gate.json").read_text(encoding="utf-8"))
    assert gate["gate_state"] == "armed_for_future_rerun"
    assert gate["current_ready_for_rerun"] is True


def test_frozen_watch_decision_drops_when_unresolvable_unknowns_exist() -> None:
    decision = mod._build_frozen_watch_decision(
        session_id="synthetic",
        calendar_payload={
            "maturity_summary": {
                "permanently_unresolvable_count": 1,
                "full_ready_now": False,
                "partial_ready_now": False,
            }
        },
        source_context={
            "decision": {"decision": "hold_until_unknown_horizon_completes"},
            "unknown_impact": {"unknown_materiality": True},
        },
        recheck_plan={"recheck_state": "drop_due_to_unresolvable_unknowns"},
        unknown_rows=[],
    )
    assert decision["decision"] == "drop_due_to_unresolvable_unknowns"
    assert "unresolvable" in decision["decision_reasons"][0]
