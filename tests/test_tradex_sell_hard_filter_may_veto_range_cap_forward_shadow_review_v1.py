from __future__ import annotations

import csv
import json
import copy
from pathlib import Path

import duckdb
import pytest

from scripts import tradex_sell_hard_filter_may_veto_range_cap_forward_shadow_review_v1 as mod
from tests.test_tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 import _row


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _all_month_dates() -> list[int]:
    dates: list[int] = []
    for year in (2024, 2025, 2026):
        for month in range(1, 13):
            dates.append(int(f"{year:04d}{month:02d}05"))
    return dates


def _build_rows() -> list[dict]:
    codes = [f"S{i:03d}" for i in range(1, 21)]
    rows: list[dict] = []
    for date_index, as_of_date in enumerate(_all_month_dates()):
        offset = (date_index * 5) % len(codes)
        for rank in range(1, 6):
            code = codes[(offset + rank - 1) % len(codes)]
            row = _row(as_of_date, rank, code, 0.25, 100.0, 90.0)
            row["monthly_range_prob"] = 0.10
            rows.append(row)
    return rows


def _write_duckdb_runtime(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE industry_master (
                code TEXT,
                name TEXT,
                sector33_code TEXT,
                sector33_name TEXT,
                market_code TEXT
            )
            """
        )
        rows = []
        sector_names = ["Sector A", "Sector B", "Sector C", "Sector D", "Sector E"]
        for index, code in enumerate(f"S{i:03d}" for i in range(1, 21)):
            sector_index = index // 4
            rows.append((code, f"Name {code}", f"SEC{sector_index + 1:02d}", sector_names[sector_index], f"MKT{sector_index + 1:02d}"))
        conn.executemany("INSERT INTO industry_master VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def _build_source_artifacts(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    source_root = tmp_path / "source_raw"
    compare_run_root = tmp_path / "compare_run"
    range_cap_root = tmp_path / "range_cap_root"
    runtime_db_path = tmp_path / "runtime.duckdb"
    rows = _build_rows()
    _write_jsonl(source_root / "candidate_outcome_table_top50.jsonl", rows)
    _write_json(
        compare_run_root / "hard_filter_contract.json",
        {
            "schema_version": "sell_monthly_breakout_hard_filter_compare_v1_contract_v1",
            "source_root": str(source_root),
            "threshold": 0.10,
            "threshold_source": "monthly_breakout_up_prob_low_q25",
        },
    )
    _write_json(
        compare_run_root / "hard_filter_compare.json",
        {
            "schema_version": "sell_monthly_breakout_hard_filter_compare_v1_compare_v1",
            "delta": {
                "changed_top5_members_count": 72,
                "changed_rank_count": 48,
                "filtered_baseline_top5_candidate_count": 36,
                "insufficient_refill_dates": 0,
            },
        },
    )
    _write_json(
        compare_run_root / "hard_filter_decision.json",
        {
            "schema_version": "sell_monthly_breakout_hard_filter_compare_v1_decision_v1",
            "decision": "keep_for_portfolio_replay",
            "authoritative_rollup_decision": "keep_for_portfolio_replay",
            "buy_level_equivalence_reached": True,
            "shadow_trade_candidate": True,
            "blockers": [],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        compare_run_root / "hard_filter_no_lookahead_audit.json",
        {
            "schema_version": "sell_monthly_breakout_hard_filter_compare_v1_no_lookahead_audit_v1",
            "no_lookahead_pass": True,
            "future_outcome_fields_used_in_selection": [],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        compare_run_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": "sell_monthly_breakout_hard_filter_compare_v1_artifact_complete_v1",
            "complete": True,
            "artifact_refs": {
                "hard_filter_contract": str(compare_run_root / "hard_filter_contract.json"),
                "hard_filter_compare": str(compare_run_root / "hard_filter_compare.json"),
                "hard_filter_decision": str(compare_run_root / "hard_filter_decision.json"),
                "hard_filter_no_lookahead_audit": str(compare_run_root / "hard_filter_no_lookahead_audit.json"),
            },
        },
    )
    _write_json(
        range_cap_root / "final_range_cap_decision.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_decision_v1",
            "decision": "keep_as_buy_level_equivalent_research_candidate",
            "authoritative_rollup_decision": "keep_as_buy_level_equivalent_research_candidate",
            "buy_level_equivalence_reached": True,
            "shadow_trade_candidate": True,
            "blockers": [],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        range_cap_root / "range_cap_contract.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_contract_v1",
            "source_compare_run_root": str(compare_run_root),
            "calendar_veto_rule": "exclude entries with as_of_date month == 5",
            "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
            "selection_threshold_changed": False,
            "veto_logic_changed": False,
            "sizing_changed": False,
            "replay_semantics_changed": False,
        },
    )
    _write_json(
        range_cap_root / "range_cap_compare.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_compare_v1",
            "source_may_veto": {
                "total_return": 0.187,
                "max_drawdown": -0.203,
                "profit_factor": 1.078,
                "bad_pick_count": 10,
                "severe_loser_count": 4,
            },
            "challenger": {
                "total_return": 0.243,
                "max_drawdown": -0.159,
                "profit_factor": 1.390,
                "bad_pick_count": 5,
                "severe_loser_count": 3,
            },
            "delta": {
                "total_return_delta": 0.056,
                "max_drawdown_delta": 0.044,
                "bad_pick_delta": -5,
                "severe_loser_delta": -1,
            },
        },
    )
    _write_json(
        range_cap_root / "yearly_performance.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_yearly_v1",
            "challenger": [
                {"year": 2024, "return_on_base_capital": 0.072, "classification": "positive", "trade_count": 60},
                {"year": 2025, "return_on_base_capital": 0.084, "classification": "positive", "trade_count": 60},
                {"year": 2026, "return_on_base_capital": 0.091, "classification": "positive", "trade_count": 60},
            ],
        },
    )
    _write_json(
        range_cap_root / "monthly_performance.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_monthly_v1",
            "challenger": [
                {
                    "month": date,
                    "return_on_base_capital": 0.004 + (index % 7) * 0.002,
                    "classification": "positive",
                    "trade_count": 5,
                }
                for index, date in enumerate(
                    [f"{year:04d}-{month:02d}" for year in (2024, 2025, 2026) for month in range(1, 13)]
                )
            ],
        },
    )
    _write_json(
        range_cap_root / "no_lookahead_audit.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_no_lookahead_audit_v1",
            "no_lookahead_pass": True,
            "future_outcome_fields_used_in_selection_sizing_or_veto": [],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        range_cap_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_artifact_complete_v1",
            "complete": True,
            "artifact_refs": {
                "final_range_cap_decision": str(range_cap_root / "final_range_cap_decision.json"),
                "range_cap_contract": str(range_cap_root / "range_cap_contract.json"),
                "range_cap_compare": str(range_cap_root / "range_cap_compare.json"),
                "yearly_performance": str(range_cap_root / "yearly_performance.json"),
                "monthly_performance": str(range_cap_root / "monthly_performance.json"),
                "no_lookahead_audit": str(range_cap_root / "no_lookahead_audit.json"),
            },
        },
    )
    _write_duckdb_runtime(runtime_db_path)
    return range_cap_root, compare_run_root, source_root, runtime_db_path


def _patch_runtime_services(monkeypatch: pytest.MonkeyPatch, runtime_db_path: Path) -> None:
    runtime_status = {
        "confirmed": True,
        "selected_runtime_db_path": str(runtime_db_path),
        "validated": True,
        "db_exists": True,
        "freshness_state": "fresh",
        "stale": False,
        "resolution_source": "test_fixture",
        "resolution_reason": "fixture",
    }
    freshness = {
        "freshness_state": "fresh",
        "freshness_days": 2,
        "stale": False,
        "current_candidate_available": True,
        "snapshot_as_of": "2026-05-14",
    }
    buy_candidates = {
        "confirmed_actionable_buy_candidates": [
            {"code": "S004", "name": "Overlap"},
            {"code": "S101", "name": "Other"},
            {"code": "S102", "name": "Other"},
        ]
    }
    borrow_profiles = {
        f"S{i:03d}": {
            "latestBalance": {"loanRatio": 0.72, "issueName": f"Issue {i:03d}", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": f"Issue {i:03d}", "marketName": "TSE"},
            "restrictions": [],
        }
        for i in range(1, 21)
    }
    borrow_profiles["S011"]["latestBalance"]["loanRatio"] = 1.20
    borrow_profiles["S011"]["latestFee"]["currentFeeYen"] = 12.0
    borrow_profiles["S020"]["latestBalance"]["loanRatio"] = 1.05
    borrow_profiles["S020"]["latestFee"]["currentFeeYen"] = 8.0
    borrow_profiles["S020"]["restrictions"] = [
        {"noticeDate": 20260501, "measureType": "売禁", "marketName": "TSE"}
    ]

    monkeypatch.setattr(mod, "get_runtime_stock_db_status", lambda: dict(runtime_status))
    monkeypatch.setattr(mod, "get_rankings_freshness", lambda **_kwargs: dict(freshness))
    monkeypatch.setattr(mod.rankings_cache, "get_rankings", lambda *args, **kwargs: dict(buy_candidates))

    def fake_load_taisyaku_snapshot(code: str, **_kwargs) -> dict:
        profile = borrow_profiles[str(code)]
        return copy.deepcopy(profile)

    monkeypatch.setattr(mod, "load_taisyaku_snapshot", fake_load_taisyaku_snapshot)


def test_forward_shadow_keep_writes_required_artifacts_and_reports_small_borrow_gap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    range_cap_root, compare_run_root, source_root, runtime_db_path = _build_source_artifacts(tmp_path)
    _patch_runtime_services(monkeypatch, runtime_db_path)

    result = mod.run(source_range_cap_root=range_cap_root, output_root=tmp_path / "out")
    out_dir = Path(result["output_dir"])

    expected = {
        "shadow_review_contract.json",
        "shadow_candidate_daily_events.csv",
        "shadow_candidate_summary.json",
        "borrow_availability_gap_report.json",
        "forward_shadow_review_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in out_dir.iterdir()}
    assert result["decision"] == "keep_for_forward_shadow"

    complete = json.loads((out_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    contract = json.loads((out_dir / "shadow_review_contract.json").read_text(encoding="utf-8"))
    summary = json.loads((out_dir / "shadow_candidate_summary.json").read_text(encoding="utf-8"))
    borrow_report = json.loads((out_dir / "borrow_availability_gap_report.json").read_text(encoding="utf-8"))
    decision = json.loads((out_dir / "forward_shadow_review_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((out_dir / "no_lookahead_audit.json").read_text(encoding="utf-8"))

    with (out_dir / "shadow_candidate_daily_events.csv").open("r", encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))

    assert complete["complete"] is True
    assert set(complete["artifact_refs"]) == {
        "shadow_review_contract",
        "shadow_candidate_daily_events",
        "shadow_candidate_summary",
        "borrow_availability_gap_report",
        "forward_shadow_review_decision",
        "no_lookahead_audit",
        "_ARTIFACT_COMPLETE",
    }
    assert contract["fixed_evaluation_conditions"] == {
        "same_universe": True,
        "same_period": True,
        "same_top_k": True,
        "same_regime_condition": True,
        "same_cost_slippage": True,
        "same_artifact_detail_level": True,
        "no_lookahead_contract": True,
    }
    assert "MeeMee" in contract["non_scope"]
    assert decision["decision"] == "keep_for_forward_shadow"
    assert decision["shadow_trade_candidate"] is True
    assert decision["forward_shadow_ready"] is True
    assert decision["blockers"] == []
    assert audit["future_outcome_fields_used_in_selection_sizing_or_veto"] == []
    assert audit["no_lookahead_pass"] is True

    selection_summary = summary["selection_summary"]
    buy_overlap_summary = summary["buy_overlap_summary"]
    borrow_summary = summary["borrow_summary"]
    fragility_summary = summary["fragility_summary"]

    assert len(csv_rows) == selection_summary["selected_event_count"]
    assert selection_summary["selected_event_count"] >= 50
    assert selection_summary["selected_month_count"] >= 12
    assert selection_summary["selected_year_count"] == 3
    assert selection_summary["code_top1_share"] <= 0.10
    assert selection_summary["sector_top1_share"] <= 0.25
    assert buy_overlap_summary["available"] is True
    assert buy_overlap_summary["candidate_count"] == 3
    assert buy_overlap_summary["overlap_code_count"] >= 1
    assert borrow_summary["candidate_code_count"] == 20
    assert borrow_summary["hard_gap_code_count"] == 1
    assert borrow_summary["hard_gap_event_share"] < 0.10
    assert fragility_summary["calendar_overfit_flag"] is False
    assert any(row["current_buy_overlap"] == "True" for row in csv_rows)
    assert any(row["borrow_hard_gap"] == "True" for row in csv_rows)


@pytest.mark.parametrize(
    "selection_summary, borrow_summary, buy_snapshot, expected_decision, expected_blocker",
    [
        (
            {
                "selected_event_count": 40,
                "selected_month_count": 10,
                "selected_year_count": 2,
                "selected_code_count": 8,
                "code_top1_share": 0.08,
                "sector_top1_share": 0.20,
                "may_veto_removed_share": 0.05,
                "range_cap_removed_share": 0.0,
                "negative_year_count": 0,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0},
            {"available": True, "candidate_count": 2},
            "hold_requires_more_forward_data",
            "too_few_events",
        ),
        (
            {
                "selected_event_count": 120,
                "selected_month_count": 33,
                "selected_year_count": 3,
                "selected_code_count": 20,
                "code_top1_share": 0.05,
                "sector_top1_share": 0.20,
                "may_veto_removed_share": 0.05,
                "range_cap_removed_share": 0.0,
                "negative_year_count": 0,
            },
            {"hard_gap_event_share": 0.12, "hard_gap_code_count": 3},
            {"available": True, "candidate_count": 2},
            "drop_as_untradable_due_to_borrow_gap",
            "borrow_gap_too_large",
        ),
        (
            {
                "selected_event_count": 120,
                "selected_month_count": 33,
                "selected_year_count": 3,
                "selected_code_count": 20,
                "code_top1_share": 0.05,
                "sector_top1_share": 0.20,
                "may_veto_removed_share": 0.05,
                "range_cap_removed_share": 0.0,
                "negative_year_count": 1,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0},
            {"available": True, "candidate_count": 2},
            "drop_as_calendar_overfit",
            "calendar_rule_fragility",
        ),
    ],
)
def test_forward_shadow_decision_labels_cover_hold_and_drop_variants(
    selection_summary: dict,
    borrow_summary: dict,
    buy_snapshot: dict,
    expected_decision: str,
    expected_blocker: str,
) -> None:
    decision = mod._build_decision(
        selection_summary=selection_summary,
        borrow_summary=borrow_summary,
        buy_snapshot=buy_snapshot,
    )

    assert decision["decision"] == expected_decision
    assert expected_blocker in decision["blockers"]
