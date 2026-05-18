from __future__ import annotations

import csv
import copy
import json
from pathlib import Path

import duckdb
import pytest

from scripts import tradex_sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_duckdb_runtime(path: Path, codes: list[str]) -> None:
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
        for index, code in enumerate(codes):
            rows.append((code, f"Name {code}", f"SEC{index + 1:02d}", f"Sector {index + 1}", "TSE"))
        conn.executemany("INSERT INTO industry_master VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def _build_frozen_artifacts(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    source_root = tmp_path / "source_raw"
    compare_run_root = tmp_path / "compare_run"
    range_cap_root = tmp_path / "range_cap_root"
    runtime_db_path = tmp_path / "runtime.duckdb"

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
                    "month": f"{year:04d}-{month:02d}",
                    "return_on_base_capital": 0.004 + (index % 7) * 0.002,
                    "classification": "positive",
                    "trade_count": 5,
                }
                for index, (year, month) in enumerate(
                    [(year, month) for year in (2024, 2025, 2026) for month in range(1, 13)]
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
    _write_jsonl(
        source_root / "candidate_outcome_table_top50.jsonl",
        [
            {
                "as_of_date": 20240605,
                "entry_date": 20240606,
                "exit_date": 20240626,
                "year": "2024",
                "month": "2024-06",
                "rank": 1,
                "code": "S001",
                "name": "Source One",
                "side": "sell",
                "execution_available": True,
                "monthly_breakout_up_prob": 0.31,
                "monthly_range_prob": 0.18,
                "short_ret20_next_open_to_20d_close": 0.02,
                "bad_pick": False,
                "severe_loser": False,
            }
        ],
    )
    _write_duckdb_runtime(runtime_db_path, [f"S{i:03d}" for i in range(1, 8)])
    return range_cap_root, compare_run_root, source_root, runtime_db_path


def _build_recent_rows() -> list[dict]:
    rows: list[dict] = []
    for day in range(5, 15):
        anchor_date = f"2026-06-{day:02d}"
        for rank, code in enumerate(["S001", "S002", "S003", "S004", "S005"], start=1):
            rows.append(
                {
                    "anchor_date": anchor_date,
                    "anchor_ymd": int(anchor_date.replace("-", "")),
                    "symbol": code,
                    "name": f"Name {code}",
                    "side": "short",
                    "champion_rank": rank,
                    "runtime_rank": rank,
                    "champion_score": 0.9 - rank * 0.05,
                    "display_score": 0.9 - rank * 0.05,
                    "signal_state": "signal",
                    "entry_qualified": True,
                    "setup_type": "trade",
                    "status": "active",
                }
            )
    return rows


def _build_live_candidates() -> list[dict]:
    return [
        {"code": "S001", "name": "Name S001", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.31, "monthlyRangeProb": 0.18, "tradePriorityScore": 0.90, "displayScore": 0.90},
        {"code": "S002", "name": "Name S002", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.29, "monthlyRangeProb": 0.12, "tradePriorityScore": 0.86, "displayScore": 0.86},
        {"code": "S003", "name": "Name S003", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.27, "monthlyRangeProb": 0.14, "tradePriorityScore": 0.83, "displayScore": 0.83},
        {"code": "S004", "name": "Name S004", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.24, "monthlyRangeProb": 0.26, "tradePriorityScore": 0.79, "displayScore": 0.79},
        {"code": "S005", "name": "Name S005", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.22, "monthlyRangeProb": 0.24, "tradePriorityScore": 0.75, "displayScore": 0.75},
        {"code": "S006", "name": "Name S006", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.05, "monthlyRangeProb": 0.16, "tradePriorityScore": 0.71, "displayScore": 0.71},
        {"code": "S007", "name": "Name S007", "asOf": "2026-06-14", "monthlyBreakoutUpProb": 0.26, "monthlyRangeProb": 0.55, "tradePriorityScore": 0.69, "displayScore": 0.69},
    ]


def _build_buy_candidates() -> list[dict]:
    return [
        {"code": "S002", "name": "Name S002", "tradePriorityScore": 0.88, "displayScore": 0.88},
        {"code": "S101", "name": "Other 101", "tradePriorityScore": 0.83, "displayScore": 0.83},
        {"code": "S102", "name": "Other 102", "tradePriorityScore": 0.80, "displayScore": 0.80},
    ]


def _patch_runtime_services(
    monkeypatch: pytest.MonkeyPatch,
    runtime_db_path: Path,
    *,
    current_short_candidates: list[dict],
    current_buy_candidates: list[dict],
    recent_rows: list[dict],
    borrow_profiles: dict[str, dict],
) -> None:
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
        "freshness_days": 1,
        "stale": False,
        "current_candidate_available": True,
        "snapshot_as_of": "2026-06-14",
    }
    monkeypatch.setattr(mod, "get_runtime_stock_db_status", lambda: dict(runtime_status))

    def fake_get_rankings_freshness(*, direction: str, **_kwargs) -> dict:
        payload = dict(freshness)
        payload["snapshot_as_of"] = "2026-06-14"
        payload["current_candidate_available"] = True
        payload["direction"] = direction
        return payload

    monkeypatch.setattr(mod, "get_rankings_freshness", fake_get_rankings_freshness)

    def fake_get_rankings(tf: str, which: str, direction: str, limit: int, **_kwargs) -> dict:
        if direction == "down":
            return {
                "snapshot_as_of": "2026-06-14",
                "confirmed_snapshot_as_of": "2026-06-14",
                "confirmed_actionable_short_candidates": copy.deepcopy(current_short_candidates[:limit]),
                "confirmed_actionable_buy_candidates": [],
            }
        return {
            "snapshot_as_of": "2026-06-14",
            "confirmed_snapshot_as_of": "2026-06-14",
            "confirmed_actionable_buy_candidates": copy.deepcopy(current_buy_candidates[:limit]),
            "confirmed_actionable_short_candidates": [],
        }

    monkeypatch.setattr(mod.rankings_cache, "get_rankings", fake_get_rankings)
    monkeypatch.setattr(mod, "load_recent_runtime_ranking_rows", lambda *_args, **_kwargs: list(recent_rows))

    def fake_load_taisyaku_snapshot(code: str, **_kwargs) -> dict:
        return copy.deepcopy(borrow_profiles[str(code)])

    monkeypatch.setattr(mod, "load_taisyaku_snapshot", fake_load_taisyaku_snapshot)


def _build_borrow_profiles() -> dict[str, dict]:
    profiles = {}
    for index in range(1, 8):
        code = f"S{index:03d}"
        profiles[code] = {
            "latestBalance": {
                "loanRatio": 0.72,
                "issueName": f"Issue {code}",
                "marketName": "TSE",
            },
            "latestFee": {
                "currentFeeYen": 0.0,
                "issueName": f"Issue {code}",
                "marketName": "TSE",
            },
            "restrictions": [],
        }
    profiles["S004"]["latestFee"]["currentFeeYen"] = 12.0
    profiles["S005"]["latestBalance"]["loanRatio"] = 1.05
    return profiles


def test_live_shadow_watch_writes_required_artifacts_and_continues(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    range_cap_root, compare_run_root, source_root, runtime_db_path = _build_frozen_artifacts(tmp_path)
    recent_rows = _build_recent_rows()
    live_short_candidates = _build_live_candidates()
    live_buy_candidates = _build_buy_candidates()
    borrow_profiles = _build_borrow_profiles()
    _patch_runtime_services(
        monkeypatch,
        runtime_db_path,
        current_short_candidates=live_short_candidates,
        current_buy_candidates=live_buy_candidates,
        recent_rows=recent_rows,
        borrow_profiles=borrow_profiles,
    )

    result = mod.run(source_range_cap_root=range_cap_root, output_root=tmp_path / "out")
    out_dir = Path(result["output_root"])

    expected = {
        "live_shadow_watch_contract.json",
        "live_shadow_daily_candidates.csv",
        "live_shadow_borrow_status.csv",
        "live_shadow_buy_overlap.csv",
        "live_shadow_concentration_summary.json",
        "live_shadow_operability_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in out_dir.iterdir()}
    assert result["decision"] == "continue_live_shadow"

    contract = json.loads((out_dir / "live_shadow_watch_contract.json").read_text(encoding="utf-8"))
    summary = json.loads((out_dir / "live_shadow_concentration_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((out_dir / "live_shadow_operability_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((out_dir / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((out_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    with (out_dir / "live_shadow_daily_candidates.csv").open("r", encoding="utf-8", newline="") as handle:
        candidate_rows = list(csv.DictReader(handle))
    with (out_dir / "live_shadow_borrow_status.csv").open("r", encoding="utf-8", newline="") as handle:
        borrow_rows = list(csv.DictReader(handle))
    with (out_dir / "live_shadow_buy_overlap.csv").open("r", encoding="utf-8", newline="") as handle:
        overlap_rows = list(csv.DictReader(handle))

    assert complete["complete"] is True
    assert set(complete["artifact_refs"]) == {
        "live_shadow_watch_contract",
        "live_shadow_daily_candidates",
        "live_shadow_borrow_status",
        "live_shadow_buy_overlap",
        "live_shadow_concentration_summary",
        "live_shadow_operability_decision",
        "no_lookahead_audit",
    }
    assert contract["frozen_rule"]["threshold"] == 0.10
    assert contract["live_window"]["recent_dates"] == 20
    assert decision["decision"] == "continue_live_shadow"
    assert decision["live_shadow_ready"] is True
    assert decision["shadow_trade_candidate"] is True
    assert decision["blockers"] == []
    assert audit["no_lookahead_pass"] is True
    assert audit["future_outcome_fields_used_in_selection_sizing_or_veto"] == []
    assert summary["selection_summary"]["selected_event_count"] == 5
    assert summary["selection_summary"]["selected_code_count"] == 5
    assert summary["borrow_summary"]["hard_gap_code_count"] == 0
    assert summary["buy_overlap_summary"]["overlap_code_count"] == 1
    assert summary["recent_window_summary"]["persistent_code_count"] == 5
    assert len(candidate_rows) == 5
    assert len(borrow_rows) == 5
    assert len(overlap_rows) == 1
    assert any(row["current_buy_overlap"] == "True" for row in candidate_rows)
    assert any(row["borrow_soft_cost"] == "True" for row in candidate_rows)


@pytest.mark.parametrize(
    "selection_summary, borrow_summary, buy_overlap_summary, recent_window_summary, runtime_summary, expected_decision, expected_blocker",
    [
        (
            {
                "current_universe_count": 0,
                "threshold_selected_count": 0,
                "selected_event_count": 0,
                "selected_code_count": 0,
                "selected_day_count": 0,
                "selected_month_count": 0,
                "selected_year_count": 0,
                "code_top1_share": 0.0,
                "code_top3_share": 0.0,
                "sector_top1_share": 0.0,
                "sector_top3_share": 0.0,
                "may_veto_removed_share": 0.0,
                "range_cap_removed_share": 0.0,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_cost_event_share": 0.0, "soft_cost_code_count": 0},
            {"available": False, "candidate_count": 0, "overlap_event_share": 0.0, "overlap_code_count": 0},
            {"window_day_count": 0, "persistent_code_count": 0, "persistent_code_share": None, "median_presence_ratio": None, "mean_presence_ratio": None},
            {"current_candidate_available": False},
            "hold_due_to_insufficient_live_events",
            "insufficient_live_events",
        ),
        (
            {
                "current_universe_count": 10,
                "threshold_selected_count": 10,
                "selected_event_count": 10,
                "selected_code_count": 5,
                "selected_day_count": 10,
                "selected_month_count": 1,
                "selected_year_count": 1,
                "code_top1_share": 0.20,
                "code_top3_share": 0.60,
                "sector_top1_share": 0.20,
                "sector_top3_share": 0.60,
                "may_veto_removed_share": 0.0,
                "range_cap_removed_share": 0.0,
            },
            {"hard_gap_event_share": 0.12, "hard_gap_code_count": 2, "soft_cost_event_share": 0.0, "soft_cost_code_count": 0},
            {"available": True, "candidate_count": 3, "overlap_event_share": 0.0, "overlap_code_count": 0},
            {"window_day_count": 10, "persistent_code_count": 5, "persistent_code_share": 1.0, "median_presence_ratio": 0.5, "mean_presence_ratio": 0.5},
            {"current_candidate_available": True},
            "drop_due_to_hard_borrow_gap",
            "borrow_gap_too_large",
        ),
        (
            {
                "current_universe_count": 10,
                "threshold_selected_count": 10,
                "selected_event_count": 10,
                "selected_code_count": 5,
                "selected_day_count": 10,
                "selected_month_count": 1,
                "selected_year_count": 1,
                "code_top1_share": 0.55,
                "code_top3_share": 0.80,
                "sector_top1_share": 0.20,
                "sector_top3_share": 0.60,
                "may_veto_removed_share": 0.0,
                "range_cap_removed_share": 0.0,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_cost_event_share": 0.0, "soft_cost_code_count": 0},
            {"available": True, "candidate_count": 3, "overlap_event_share": 0.0, "overlap_code_count": 0},
            {"window_day_count": 10, "persistent_code_count": 5, "persistent_code_share": 1.0, "median_presence_ratio": 0.5, "mean_presence_ratio": 0.5},
            {"current_candidate_available": True},
            "drop_due_to_concentration",
            "concentration_too_high",
        ),
        (
            {
                "current_universe_count": 10,
                "threshold_selected_count": 10,
                "selected_event_count": 10,
                "selected_code_count": 5,
                "selected_day_count": 10,
                "selected_month_count": 1,
                "selected_year_count": 1,
                "code_top1_share": 0.20,
                "code_top3_share": 0.60,
                "sector_top1_share": 0.20,
                "sector_top3_share": 0.60,
                "may_veto_removed_share": 0.0,
                "range_cap_removed_share": 0.0,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_cost_event_share": 0.0, "soft_cost_code_count": 0},
            {"available": True, "candidate_count": 3, "overlap_event_share": 0.45, "overlap_code_count": 4},
            {"window_day_count": 10, "persistent_code_count": 5, "persistent_code_share": 1.0, "median_presence_ratio": 0.5, "mean_presence_ratio": 0.5},
            {"current_candidate_available": True},
            "drop_due_to_buy_overlap_conflict",
            "buy_overlap_conflict",
        ),
        (
            {
                "current_universe_count": 20,
                "threshold_selected_count": 6,
                "selected_event_count": 4,
                "selected_code_count": 4,
                "selected_day_count": 4,
                "selected_month_count": 1,
                "selected_year_count": 1,
                "code_top1_share": 0.20,
                "code_top3_share": 0.60,
                "sector_top1_share": 0.20,
                "sector_top3_share": 0.60,
                "may_veto_removed_share": 0.50,
                "range_cap_removed_share": 0.30,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_cost_event_share": 0.0, "soft_cost_code_count": 0},
            {"available": True, "candidate_count": 3, "overlap_event_share": 0.0, "overlap_code_count": 0},
            {"window_day_count": 12, "persistent_code_count": 0, "persistent_code_share": 0.0, "median_presence_ratio": 0.05, "mean_presence_ratio": 0.05},
            {"current_candidate_available": True},
            "drop_due_to_forward_decay",
            "forward_decay",
        ),
        (
            {
                "current_universe_count": 10,
                "threshold_selected_count": 10,
                "selected_event_count": 10,
                "selected_code_count": 5,
                "selected_day_count": 10,
                "selected_month_count": 1,
                "selected_year_count": 1,
                "code_top1_share": 0.20,
                "code_top3_share": 0.60,
                "sector_top1_share": 0.20,
                "sector_top3_share": 0.60,
                "may_veto_removed_share": 0.0,
                "range_cap_removed_share": 0.0,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_cost_event_share": 0.70, "soft_cost_code_count": 4},
            {"available": True, "candidate_count": 3, "overlap_event_share": 0.0, "overlap_code_count": 0},
            {"window_day_count": 10, "persistent_code_count": 5, "persistent_code_share": 1.0, "median_presence_ratio": 0.5, "mean_presence_ratio": 0.5},
            {"current_candidate_available": True},
            "hold_due_to_soft_borrow_cost",
            "soft_borrow_cost_too_broad",
        ),
        (
            {
                "current_universe_count": 7,
                "threshold_selected_count": 6,
                "selected_event_count": 5,
                "selected_code_count": 5,
                "selected_day_count": 1,
                "selected_month_count": 1,
                "selected_year_count": 1,
                "code_top1_share": 0.20,
                "code_top3_share": 0.60,
                "sector_top1_share": 0.20,
                "sector_top3_share": 0.60,
                "may_veto_removed_share": 0.0,
                "range_cap_removed_share": 0.17,
            },
            {"hard_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_cost_event_share": 0.40, "soft_cost_code_count": 2},
            {"available": True, "candidate_count": 3, "overlap_event_share": 0.20, "overlap_code_count": 1},
            {"window_day_count": 10, "persistent_code_count": 5, "persistent_code_share": 1.0, "median_presence_ratio": 0.5, "mean_presence_ratio": 0.5},
            {"current_candidate_available": True},
            "continue_live_shadow",
            "none",
        ),
    ],
)
def test_live_shadow_decision_labels_cover_keep_hold_and_drop_variants(
    selection_summary: dict,
    borrow_summary: dict,
    buy_overlap_summary: dict,
    recent_window_summary: dict,
    runtime_summary: dict,
    expected_decision: str,
    expected_blocker: str,
) -> None:
    decision = mod._build_operability_decision(
        selection_summary=selection_summary,
        borrow_summary=borrow_summary,
        buy_overlap_summary=buy_overlap_summary,
        recent_window_summary=recent_window_summary,
        runtime_summary=runtime_summary,
    )

    assert decision["decision"] == expected_decision
    if expected_blocker != "none":
        assert expected_blocker in decision["blockers"]
