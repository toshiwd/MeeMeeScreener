from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from scripts import tradex_sell_hard_filter_may_veto_range_cap_dependency_decomposition_v1 as mod


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


def _row(
    as_of_date: int,
    rank: int,
    code: str,
    monthly_breakout_up_prob: float,
    monthly_range_prob: float,
    short_ret20: float,
) -> dict:
    return {
        "as_of_date": as_of_date,
        "entry_date": as_of_date + 1,
        "exit_date": as_of_date + 21,
        "year": str(as_of_date)[:4],
        "month": f"{str(as_of_date)[:4]}-{str(as_of_date)[4:6]}",
        "rank": rank,
        "code": code,
        "name": f"Name {code}",
        "side": "sell",
        "execution_available": True,
        "monthly_breakout_up_prob": monthly_breakout_up_prob,
        "monthly_range_prob": monthly_range_prob,
        "short_ret20_next_open_to_20d_close": short_ret20,
        "bad_pick": short_ret20 > 0.0,
        "severe_loser": short_ret20 > 0.04,
    }


def _build_source_rows() -> list[dict]:
    rows: list[dict] = []
    rows.extend(
        [
            _row(20240505, 1, "H001", 0.31, 0.10, -0.06),
            _row(20240505, 2, "H002", 0.29, 0.85, 0.05),
            _row(20240505, 3, "H003", 0.27, 0.12, -0.04),
            _row(20240505, 4, "H004", 0.25, 0.72, 0.06),
            _row(20240505, 5, "H005", 0.21, 0.18, -0.03),
            _row(20240605, 1, "I001", 0.32, 0.14, -0.02),
            _row(20240605, 2, "I002", 0.28, 0.80, 0.05),
            _row(20240605, 3, "I003", 0.26, 0.16, -0.05),
            _row(20240605, 4, "I004", 0.24, 0.76, 0.04),
            _row(20240605, 5, "I005", 0.22, 0.20, -0.01),
            _row(20250505, 1, "J001", 0.31, 0.11, -0.07),
            _row(20250505, 2, "J002", 0.29, 0.83, 0.03),
            _row(20250505, 3, "J003", 0.27, 0.13, -0.04),
            _row(20250505, 4, "J004", 0.25, 0.79, 0.05),
            _row(20250505, 5, "J005", 0.23, 0.19, -0.02),
        ]
    )
    return rows


def _build_recent_rows() -> list[dict]:
    rows: list[dict] = []
    for day in range(1, 11):
        anchor_date = f"2026-05-{day:02d}"
        for rank, code in enumerate(["8252", "3397", "3293", "3295", "2471"], start=1):
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


def _build_live_short_candidates() -> list[dict]:
    return [
        {"code": "8252", "name": "Name 8252", "asOf": "2026-05-14", "monthlyBreakoutUpProb": 0.4857857008604295, "monthlyRangeProb": 0.6046201443473724, "tradePriorityScore": 0.9284773060029283, "displayScore": 0.9284773060029283},
        {"code": "3397", "name": "Name 3397", "asOf": "2026-05-14", "monthlyBreakoutUpProb": 0.3267450214409196, "monthlyRangeProb": 0.18961427019824156, "tradePriorityScore": 0.9240849194729137, "displayScore": 0.9240849194729137},
        {"code": "3293", "name": "Name 3293", "asOf": "2026-05-14", "monthlyBreakoutUpProb": 0.6646198830409358, "monthlyRangeProb": 0.8137426900584797, "tradePriorityScore": 0.8880673499267935, "displayScore": 0.8880673499267935},
        {"code": "3295", "name": "Name 3295", "asOf": "2026-05-14", "monthlyBreakoutUpProb": 0.4754109640114155, "monthlyRangeProb": 0.6469844562395353, "tradePriorityScore": 0.8753294289897511, "displayScore": 0.8753294289897511},
        {"code": "2471", "name": "Name 2471", "asOf": "2026-05-14", "monthlyBreakoutUpProb": 0.14488139825218482, "monthlyRangeProb": 0.10819826057763937, "tradePriorityScore": 0.8643484626647145, "displayScore": 0.8643484626647145},
    ]


def _build_live_buy_candidates() -> list[dict]:
    return [
        {"code": "B101", "name": "Buy 101", "tradePriorityScore": 0.88, "displayScore": 0.88},
        {"code": "B102", "name": "Buy 102", "tradePriorityScore": 0.84, "displayScore": 0.84},
        {"code": "B103", "name": "Buy 103", "tradePriorityScore": 0.80, "displayScore": 0.80},
    ]


def _build_borrow_profiles() -> dict[str, dict]:
    profiles: dict[str, dict] = {}
    for code in ["8252", "3397", "3293", "3295", "2471"]:
        profiles[code] = {
            "latestBalance": {"loanRatio": 0.72, "issueName": f"Issue {code}", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": f"Issue {code}", "marketName": "TSE"},
            "restrictions": [],
        }
    return profiles


def _build_fixture_artifacts(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    source_root = tmp_path / "source_raw"
    compare_run_root = tmp_path / "compare_run"
    range_cap_root = tmp_path / "range_cap_root"
    live_root = tmp_path / "live_root"
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
                "changed_top5_members_count": 12,
                "changed_rank_count": 8,
                "filtered_baseline_top5_candidate_count": 6,
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
                {"month": f"{year:04d}-{month:02d}", "return_on_base_capital": 0.004 + (index % 7) * 0.002, "classification": "positive", "trade_count": 5}
                for index, (year, month) in enumerate([(year, month) for year in (2024, 2025, 2026) for month in range(1, 13)])
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

    _write_jsonl(source_root / "candidate_outcome_table_top50.jsonl", _build_source_rows())
    _write_duckdb_runtime(runtime_db_path, ["8252", "3397", "3293", "3295", "2471"])

    _write_json(
        live_root / "live_shadow_watch_contract.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1_contract_v1",
            "axis": "may_veto_plus_monthly_range_lt_0_5_v1",
            "source_range_cap_root": str(range_cap_root),
            "source_compare_run_root": str(compare_run_root),
            "source_raw_rows_root": str(source_root),
            "source_authoritative_decision": str(range_cap_root / "final_range_cap_decision.json"),
            "frozen_rule": {
                "threshold_source": "monthly_breakout_up_prob_low_q25",
                "threshold": 0.10,
                "calendar_veto_rule": "exclude entries with as_of_date month == 5",
                "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
                "selection_threshold_changed": False,
                "veto_logic_changed": False,
                "sizing_changed": False,
                "replay_semantics_changed": False,
            },
            "live_window": {
                "live_limit": 50,
                "buy_limit": 50,
                "recent_dates": 20,
                "rank_limit": 50,
                "direction": "down",
                "buy_direction": "up",
            },
            "fixed_evaluation_conditions": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": True,
                "same_regime_condition": True,
                "same_cost_slippage": True,
                "same_artifact_detail_level": True,
                "no_lookahead_contract": True,
            },
        },
    )
    _write_json(
        live_root / "live_shadow_concentration_summary.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1_concentration_summary_v1",
            "axis": "may_veto_plus_monthly_range_lt_0_5_v1",
            "current_short_universe_codes": ["8252", "3397", "3293", "3295", "2471"],
            "current_selected_candidate_rows": 0,
            "frozen_rule_summary": {
                "current_universe_count": 5,
                "threshold_selected_count": 5,
                "selected_event_count": 0,
                "selected_code_count": 0,
                "may_veto_removed_count": 5,
                "range_cap_removed_count": 0,
                "frozen_field_missing_count": 0,
            },
            "selection_summary": {
                "current_universe_count": 5,
                "threshold_selected_count": 5,
                "selected_event_count": 0,
                "selected_code_count": 0,
                "selected_day_count": 0,
                "selected_month_count": 0,
                "selected_year_count": 0,
                "code_top1_share": 0.0,
                "code_top3_share": 0.0,
                "sector_top1_share": 0.0,
                "sector_top3_share": 0.0,
                "may_veto_removed_count": 5,
                "may_veto_removed_share": 1.0,
                "range_cap_removed_count": 0,
                "range_cap_removed_share": 0.0,
                "threshold_removed_count": 0,
                "threshold_removed_share": 0.0,
                "frozen_field_missing_count": 0,
                "calendar_overfit_flag": True,
                "selected_codes": [],
                "selected_days": [],
                "selected_months": [],
                "selected_years": [],
                "concentration": {
                    "code": {"unique_count": 0, "top1_code": None, "top1_count": 0, "top1_share": 0.0, "top3_count": 0, "top3_share": 0.0},
                    "sector": {"unique_count": 0, "top1_sector": None, "top1_count": 0, "top1_share": 0.0, "top3_count": 0, "top3_share": 0.0},
                },
            },
            "borrow_summary": {
                "candidate_code_count": 0,
                "hard_gap_code_count": 0,
                "hard_gap_event_count": 0,
                "hard_gap_event_share": 0.0,
                "soft_cost_code_count": 0,
                "soft_cost_event_count": 0,
                "soft_cost_event_share": 0.0,
                "hard_gap_codes_sample": [],
                "soft_cost_codes_sample": [],
            },
            "buy_overlap_summary": {
                "available": True,
                "snapshot_as_of": "2026-05-14",
                "candidate_count": 3,
                "overlap_code_count": 0,
                "overlap_event_count": 0,
                "overlap_event_share": 0.0,
                "overlap_codes": [],
            },
            "recent_window_summary": {
                "available": True,
                "recent_dates": 20,
                "rank_limit": 50,
                "row_count": 50,
                "day_count": 20,
                "month_count": 2,
                "year_count": 1,
                "window_day_count": 20,
                "window_month_count": 2,
                "window_year_count": 1,
                "persistent_code_count": 0,
                "persistent_code_share": None,
                "mean_presence_ratio": None,
                "median_presence_ratio": None,
            },
        },
    )
    _write_json(
        live_root / "live_shadow_operability_decision.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1_operability_decision_v1",
            "decision": "drop_due_to_forward_decay",
            "authoritative_rollup_decision": "drop_due_to_forward_decay",
            "decision_reason": "frozen rule is not carrying forward with enough density or persistence",
            "typed_reasons": ["forward_decay"],
            "blockers": ["forward_decay"],
            "shadow_trade_candidate": False,
            "live_shadow_ready": False,
            "buy_level_equivalence_reached": True,
            "current_candidate_available": True,
            "current_universe_count": 5,
            "selected_event_count": 0,
            "selected_code_count": 0,
            "selected_month_count": 0,
            "selected_year_count": 0,
            "current_buy_overlap_available": True,
            "next_gate": None,
            "production_ranking_changed": False,
            "active_champion_changed": False,
            "publish_run": False,
            "live_sell_signal_added": False,
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        live_root / "no_lookahead_audit.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1_no_lookahead_audit_v1",
            "no_lookahead_pass": True,
            "selection_fields": ["code", "name", "asOf", "monthlyBreakoutUpProb", "monthlyRangeProb", "tradePriorityScore", "displayScore"],
            "borrow_fields": ["latestBalance.loanRatio", "latestFee.currentFeeYen", "restrictions"],
            "observation_fields": ["anchor_date", "symbol", "champion_rank", "runtime_rank", "display_score", "signal_state", "entry_qualified", "setup_type", "status"],
            "future_outcome_fields_used_in_selection_sizing_or_veto": [],
            "current_row_count": 0,
            "recent_row_count": 50,
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        live_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1_artifact_complete_v1",
            "complete": True,
            "artifact_refs": {
                "live_shadow_watch_contract": str(live_root / "live_shadow_watch_contract.json"),
                "live_shadow_concentration_summary": str(live_root / "live_shadow_concentration_summary.json"),
                "live_shadow_operability_decision": str(live_root / "live_shadow_operability_decision.json"),
                "no_lookahead_audit": str(live_root / "no_lookahead_audit.json"),
            },
        },
    )

    return live_root, range_cap_root, compare_run_root, source_root, runtime_db_path


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
        "snapshot_as_of": "2026-05-14",
    }
    monkeypatch.setattr(mod.live, "get_runtime_stock_db_status", lambda: dict(runtime_status))

    def fake_get_rankings_freshness(*, direction: str, **_kwargs) -> dict:
        payload = dict(freshness)
        payload["snapshot_as_of"] = "2026-05-14"
        payload["current_candidate_available"] = True
        payload["direction"] = direction
        return payload

    monkeypatch.setattr(mod.live, "get_rankings_freshness", fake_get_rankings_freshness)

    def fake_get_rankings(tf: str, which: str, direction: str, limit: int, **_kwargs) -> dict:
        if direction == "down":
            return {
                "snapshot_as_of": "2026-05-14",
                "confirmed_snapshot_as_of": "2026-05-14",
                "confirmed_actionable_short_candidates": current_short_candidates[:limit],
                "confirmed_actionable_buy_candidates": [],
            }
        return {
            "snapshot_as_of": "2026-05-14",
            "confirmed_snapshot_as_of": "2026-05-14",
            "confirmed_actionable_buy_candidates": current_buy_candidates[:limit],
            "confirmed_actionable_short_candidates": [],
        }

    monkeypatch.setattr(mod.live.rankings_cache, "get_rankings", fake_get_rankings)
    monkeypatch.setattr(mod.live, "load_recent_runtime_ranking_rows", lambda *_args, **_kwargs: list(recent_rows))

    def fake_load_taisyaku_snapshot(code: str, **_kwargs) -> dict:
        return dict(borrow_profiles[str(code)])

    monkeypatch.setattr(mod.live, "load_taisyaku_snapshot", fake_load_taisyaku_snapshot)


def test_dependency_decomposition_run_writes_required_artifacts_and_splits_rule(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    live_root, _range_root, _compare_root, _source_root, runtime_db_path = _build_fixture_artifacts(tmp_path)
    recent_rows = _build_recent_rows()
    short_candidates = _build_live_short_candidates()
    buy_candidates = _build_live_buy_candidates()
    borrow_profiles = _build_borrow_profiles()
    _patch_runtime_services(
        monkeypatch,
        runtime_db_path,
        current_short_candidates=short_candidates,
        current_buy_candidates=buy_candidates,
        recent_rows=recent_rows,
        borrow_profiles=borrow_profiles,
    )

    result = mod.run(source_live_root=live_root, output_root=tmp_path / "out")
    out_dir = Path(result["output_root"])

    expected = {
        "may_veto_dependency_contract.json",
        "current_removed_candidates.csv",
        "historical_may_veto_contribution.json",
        "range_cap_contribution.json",
        "interaction_contribution.json",
        "forward_decay_diagnosis.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in out_dir.iterdir()}
    assert result["decision"] == "split_rule_and_retest_range_cap_only"

    contract = json.loads((out_dir / "may_veto_dependency_contract.json").read_text(encoding="utf-8"))
    current_rows = list(csv.DictReader((out_dir / "current_removed_candidates.csv").open("r", encoding="utf-8", newline="")))
    may = json.loads((out_dir / "historical_may_veto_contribution.json").read_text(encoding="utf-8"))
    range_cap = json.loads((out_dir / "range_cap_contribution.json").read_text(encoding="utf-8"))
    interaction = json.loads((out_dir / "interaction_contribution.json").read_text(encoding="utf-8"))
    diagnosis = json.loads((out_dir / "forward_decay_diagnosis.json").read_text(encoding="utf-8"))
    audit = json.loads((out_dir / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((out_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert contract["frozen_rule"]["threshold"] == 0.10
    assert contract["live_watch_state"]["decision"] == "drop_due_to_forward_decay"
    assert contract["live_watch_state"]["current_universe_count"] == 5
    assert diagnosis["decision"] == "split_rule_and_retest_range_cap_only"
    assert diagnosis["post_may_live_watch_required"] is True
    assert diagnosis["calendar_stop_expected"] is False
    assert diagnosis["current_live"]["may_veto_removed_count"] == 5
    assert diagnosis["current_live"]["range_cap_pass_count_independent"] == 2
    assert diagnosis["current_live"]["range_cap_survivor_codes"] == ["2471", "3397"]
    assert audit["no_lookahead_pass"] is True
    assert complete["complete"] is True
    assert may["delta_vs_selected_pool"]["may_veto_only"]["mean_short_ret20_delta"] > 0.0
    assert range_cap["delta_vs_selected_pool"]["range_cap_only"]["mean_short_ret20_delta"] < 0.0
    assert interaction["overlap_removed_count"] == 0 or interaction["overlap_removed_count"] >= 0
    assert len(current_rows) == 5
    assert sum(1 for row in current_rows if row["may_veto_passed"] == "False") == 5
    assert sum(1 for row in current_rows if row["range_cap_passed"] == "True") == 2
    assert sum(1 for row in current_rows if row["removed_by"] == "may_veto") == 2
    assert sum(1 for row in current_rows if row["removed_by"] == "may_veto_and_range_cap") == 3


@pytest.mark.parametrize(
    "live_summary, may_delta, range_delta, expected",
    [
        (
            {"current_universe_count": 5, "threshold_selected_count": 5, "selected_event_count": 0, "selected_code_count": 0, "may_veto_removed_count": 5, "may_veto_removed_share": 1.0, "range_cap_removed_count_independent": 3, "range_cap_removed_share_independent": 0.6, "range_cap_pass_count_independent": 2, "current_range_cap_survivor_count": 2, "current_range_cap_survivor_codes": ["A", "B"], "current_may_veto_removed_codes": ["A", "B", "C", "D", "E"]},
            {"delta_vs_selected_pool": {"may_veto_only": {"mean_short_ret20_delta": 0.01}}},
            {"delta_vs_selected_pool": {"range_cap_only": {"mean_short_ret20_delta": -0.02}}},
            "split_rule_and_retest_range_cap_only",
        ),
        (
            {"current_universe_count": 5, "threshold_selected_count": 5, "selected_event_count": 0, "selected_code_count": 0, "may_veto_removed_count": 5, "may_veto_removed_share": 1.0, "range_cap_removed_count_independent": 5, "range_cap_removed_share_independent": 1.0, "range_cap_pass_count_independent": 0, "current_range_cap_survivor_count": 0, "current_range_cap_survivor_codes": [], "current_may_veto_removed_codes": ["A", "B", "C", "D", "E"]},
            {"delta_vs_selected_pool": {"may_veto_only": {"mean_short_ret20_delta": 0.01}}},
            {"delta_vs_selected_pool": {"range_cap_only": {"mean_short_ret20_delta": -0.02}}},
            "hold_as_calendar_stop_expected",
        ),
        (
            {"current_universe_count": 5, "threshold_selected_count": 5, "selected_event_count": 0, "selected_code_count": 0, "may_veto_removed_count": 3, "may_veto_removed_share": 0.6, "range_cap_removed_count_independent": 5, "range_cap_removed_share_independent": 1.0, "range_cap_pass_count_independent": 0, "current_range_cap_survivor_count": 0, "current_range_cap_survivor_codes": [], "current_may_veto_removed_codes": ["A", "B", "C"]},
            {"delta_vs_selected_pool": {"may_veto_only": {"mean_short_ret20_delta": 0.0}}},
            {"delta_vs_selected_pool": {"range_cap_only": {"mean_short_ret20_delta": 0.0}}},
            "drop_as_forward_decay",
        ),
    ],
)
def test_forward_decay_diagnosis_label_branches(
    live_summary: dict[str, object],
    may_delta: dict[str, object],
    range_delta: dict[str, object],
    expected: str,
) -> None:
    diagnosis = mod._build_forward_decay_diagnosis(
        input_context={},
        live_summary=live_summary,
        may_contribution=may_delta,
        range_contribution=range_delta,
        interaction_contribution={
            "overlap_removed_count": 1,
            "overlap_removed_share_of_selected": 0.1,
            "delta_vs_selected_pool": {"combined": {"mean_short_ret20_delta": -0.01}},
        },
    )
    assert diagnosis["decision"] == expected
