from __future__ import annotations

import copy
import csv
import json
from pathlib import Path

import duckdb
import pytest

from scripts import tradex_sell_hard_filter_range_cap_only_without_may_veto_v1 as mod


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


def _summary(
    *,
    total_return: float,
    max_drawdown: float,
    profit_factor: float,
    bad_pick_count: int,
    severe_loser_count: int,
    trade_count: int,
    win_rate: float,
    label: str,
) -> dict:
    return {
        "label": label,
        "variant_id": "rank3_full_else_half_position_sizing_v1",
        "base_capital_jpy": 10_000_000.0,
        "final_equity": 10_000_000.0 * (1.0 + total_return),
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "number_of_trades": trade_count,
        "win_rate": win_rate,
        "average_win": 0.0,
        "average_loss": 0.0,
        "profit_factor": profit_factor,
        "bad_pick_count": bad_pick_count,
        "severe_loser_count": severe_loser_count,
    }


def _year_rows(data: list[tuple[str, float, int, int]]) -> list[dict]:
    rows: list[dict] = []
    for year, return_on_base_capital, bad_pick_count, severe_loser_count in data:
        rows.append(
            {
                "year": year,
                "trade_count": 10,
                "return_on_base_capital": return_on_base_capital,
                "win_rate": 0.5,
                "bad_pick_count": bad_pick_count,
                "severe_loser_count": severe_loser_count,
                "classification": "positive" if return_on_base_capital > 0 else "negative",
            }
        )
    return rows


def _month_rows() -> list[dict]:
    months: list[str] = []
    year = 2024
    month = 4
    for _ in range(24):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            year += 1
            month = 1
    rows: list[dict] = []
    for index, month_key in enumerate(months):
        rows.append(
            {
                "month": month_key,
                "trade_count": 3,
                "return_on_base_capital": 0.01 if index % 5 else -0.01,
                "win_rate": 0.66,
                "bad_pick_count": 1,
                "severe_loser_count": 0,
                "classification": "positive" if index % 5 else "negative",
            }
        )
    return rows


def _build_fixture_artifacts(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    input_root = tmp_path / "input_root"
    compare_run_root = tmp_path / "compare_run"
    range_cap_root = tmp_path / "range_cap_root"
    runtime_db_path = tmp_path / "runtime.duckdb"

    _write_json(
        compare_run_root / "hard_filter_contract.json",
        {
            "schema_version": "sell_monthly_breakout_hard_filter_compare_v1_contract_v1",
            "source_root": str(tmp_path / "source_raw"),
            "threshold": 0.0325642646439107,
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
                "total_return": 0.04214973147949386,
                "max_drawdown": -0.17984831729518802,
                "profit_factor": 1.1441542970383443,
                "bad_pick_count": 37,
                "severe_loser_count": 20,
            },
            "challenger": {
                "total_return": 0.2429705075402817,
                "max_drawdown": -0.1585458007549072,
                "profit_factor": 1.39025623390792,
                "bad_pick_count": 33,
                "severe_loser_count": 18,
            },
            "delta": {
                "total_return_delta": 0.20082077606078785,
                "max_drawdown_delta": 0.021302516540280814,
                "bad_pick_delta": -4,
                "severe_loser_delta": -2,
            },
        },
    )
    _write_json(
        range_cap_root / "yearly_performance.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_yearly_v1",
            "challenger": _year_rows(
                [
                    ("2024", 0.06873536181518415, 12, 7),
                    ("2025", 0.14967822, 16, 8),
                    ("2026", 0.09932677999999998, 5, 3),
                ]
            ),
        },
    )
    _write_json(
        range_cap_root / "monthly_performance.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_v1_monthly_v1",
            "challenger": _month_rows(),
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
    _write_json(
        input_root / "may_veto_dependency_contract.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_dependency_decomposition_v1_contract_v1",
            "axis": "may_veto_plus_monthly_range_lt_0_5_v1",
            "input_live_watch_root": str(tmp_path / "live_watch_root"),
            "source_range_cap_root": str(range_cap_root),
            "source_compare_run_root": str(compare_run_root),
            "source_raw_rows_root": str(tmp_path / "source_raw"),
            "source_authoritative_decision": str(range_cap_root / "final_range_cap_decision.json"),
            "frozen_rule": {
                "threshold_source": "monthly_breakout_up_prob_low_q25",
                "threshold": 0.0325642646439107,
                "calendar_veto_rule": "exclude entries with as_of_date month == 5",
                "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
                "selection_threshold_changed": False,
                "veto_logic_changed": False,
                "sizing_changed": False,
                "replay_semantics_changed": False,
            },
            "live_watch_state": {
                "decision": "drop_due_to_forward_decay",
                "current_universe_count": 5,
                "threshold_selected_count": 5,
                "selected_event_count": 0,
                "may_veto_removed_count": 5,
                "may_veto_removed_share": 1.0,
                "range_cap_removed_count_sequential": 0,
                "range_cap_removed_share_sequential": 0.0,
                "current_short_universe_codes": ["2471", "3293", "3295", "3397", "8252"],
                "current_selected_candidate_rows": 0,
            },
            "fixed_evaluation_conditions": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": True,
                "same_regime_condition": True,
                "same_cost_slippage": True,
                "same_artifact_detail_level": True,
            },
            "validation_focus": [
                "May veto only contribution",
                "monthly range cap only contribution",
                "May veto + monthly range cap interaction",
                "current live removed candidates detail",
                "historical May removed candidates outcome",
                "non-May equivalent risk comparison",
                "calendar-stop versus structural forward decay",
            ],
            "decision_labels": [
                "hold_as_calendar_stop_expected",
                "hold_requires_post_may_live_watch",
                "drop_as_may_overfit",
                "drop_as_forward_decay",
                "split_rule_and_retest_range_cap_only",
            ],
            "non_scope": [
                "MeeMee",
                "production ranking",
                "active champion",
                "publish",
                "live sell signal",
                "threshold tuning",
                "May veto tuning",
                "monthly range cap tuning",
                "sizing tuning",
                "replay semantics tuning",
            ],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    _write_json(
        input_root / "forward_decay_diagnosis.json",
        {
            "schema_version": "sell_hard_filter_may_veto_range_cap_dependency_decomposition_v1_forward_decay_diagnosis_v1",
            "decision": "split_rule_and_retest_range_cap_only",
            "authoritative_rollup_decision": "split_rule_and_retest_range_cap_only",
            "decision_reason": "May veto removes all current live candidates while range cap still has live survivors and historical range-only filtering improves the short-side pocket",
            "typed_reasons": [
                "current_live_set_removed_by_may_veto",
                "range_cap_has_independent_live_survivors",
                "range_cap_improves_historical_short_mean",
                "filters_are_mostly_additive",
            ],
            "blockers": ["may_veto_blocks_live_usability"],
            "calendar_stop_expected": False,
            "structural_forward_decay": False,
            "post_may_live_watch_required": True,
            "current_live": {
                "current_universe_count": 5,
                "threshold_selected_count": 5,
                "selected_event_count": 0,
                "selected_code_count": 0,
                "may_veto_removed_count": 5,
                "may_veto_removed_share": 1.0,
                "range_cap_removed_count_independent": 3,
                "range_cap_removed_share_independent": 0.6,
                "range_cap_pass_count_independent": 2,
                "range_cap_survivor_codes": ["2471", "3397"],
                "may_veto_removed_codes": ["2471", "3293", "3295", "3397", "8252"],
            },
            "historical_support": {
                "may_veto_only_mean_short_ret20_delta": 0.0008188249578497522,
                "range_cap_only_mean_short_ret20_delta": -0.0009055800888589872,
                "combined_mean_short_ret20_delta": 1.4094031545799695e-05,
                "interaction_overlap_removed_count": 16,
                "interaction_overlap_removed_share_of_selected": 0.006213592233009709,
            },
            "recommended_next_axis": "range_cap_only_retest_without_may_veto",
            "production_ranking_changed": False,
            "active_champion_changed": False,
            "publish_run": False,
            "live_sell_signal_added": False,
            "silent_fallback_used": False,
            "research_fallback": False,
            "remaining_risks": [
                "borrow_availability_can_change_with_market_conditions",
                "soft_borrow_cost_incidence_can_expand",
                "post_may_live_watch_is_still_required_for_operational_confirmation",
            ],
        },
    )
    _write_jsonl(tmp_path / "source_raw" / "candidate_outcome_table_top50.jsonl", [{"note": "placeholder"}])
    _write_duckdb_runtime(runtime_db_path, ["8252", "3397", "3293", "3295", "2471"])
    return input_root, range_cap_root, compare_run_root, runtime_db_path


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


def _build_borrow_profiles() -> dict[str, dict]:
    profiles: dict[str, dict] = {}
    for code in ["8252", "3397", "3293", "3295", "2471"]:
        profiles[code] = {
            "latestBalance": {"loanRatio": 0.72, "issueName": f"Issue {code}", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": f"Issue {code}", "marketName": "TSE"},
            "restrictions": [],
        }
    profiles["8252"]["latestBalance"]["loanRatio"] = 1.1111111111111112
    profiles["2471"]["latestBalance"]["loanRatio"] = 1.0
    return profiles


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
                "confirmed_actionable_short_candidates": copy.deepcopy(current_short_candidates[:limit]),
                "confirmed_actionable_buy_candidates": [],
            }
        return {
            "snapshot_as_of": "2026-05-14",
            "confirmed_snapshot_as_of": "2026-05-14",
            "confirmed_actionable_buy_candidates": copy.deepcopy(current_buy_candidates[:limit]),
            "confirmed_actionable_short_candidates": [],
        }

    monkeypatch.setattr(mod.live.rankings_cache, "get_rankings", fake_get_rankings)
    monkeypatch.setattr(mod.live, "load_recent_runtime_ranking_rows", lambda *_args, **_kwargs: list(recent_rows))

    def fake_load_taisyaku_snapshot(code: str, **_kwargs) -> dict:
        return copy.deepcopy(borrow_profiles[str(code)])

    monkeypatch.setattr(mod.live, "load_taisyaku_snapshot", fake_load_taisyaku_snapshot)


def _fake_historical_retest() -> dict[str, object]:
    baseline = _summary(
        total_return=-0.5601436217791734,
        max_drawdown=-0.626779719638616,
        profit_factor=0.5982913160750212,
        bad_pick_count=42,
        severe_loser_count=24,
        trade_count=84,
        win_rate=0.5,
        label="baseline",
    )
    champion = _summary(
        total_return=0.04214973147949386,
        max_drawdown=-0.17984831729518802,
        profit_factor=1.1441542970383443,
        bad_pick_count=37,
        severe_loser_count=20,
        trade_count=84,
        win_rate=0.5595238095238095,
        label="champion",
    )
    frozen = _summary(
        total_return=0.2429705075402817,
        max_drawdown=-0.1585458007549072,
        profit_factor=1.39025623390792,
        bad_pick_count=33,
        severe_loser_count=18,
        trade_count=81,
        win_rate=0.5925925925925926,
        label="frozen",
    )
    challenger = _summary(
        total_return=0.08315749641906889,
        max_drawdown=-0.17964062674854375,
        profit_factor=1.1953283344024794,
        bad_pick_count=35,
        severe_loser_count=20,
        trade_count=84,
        win_rate=0.5833333333333334,
        label="challenger",
    )
    frozen_yearly = [
        {"year": "2024", "return_on_base_capital": 0.06873536181518415, "classification": "positive", "trade_count": 30, "bad_pick_count": 12, "severe_loser_count": 7},
        {"year": "2025", "return_on_base_capital": 0.14967822, "classification": "positive", "trade_count": 36, "bad_pick_count": 16, "severe_loser_count": 8},
        {"year": "2026", "return_on_base_capital": 0.09932677999999998, "classification": "positive", "trade_count": 15, "bad_pick_count": 5, "severe_loser_count": 3},
    ]
    challenger_yearly = [
        {"year": "2024", "return_on_base_capital": -0.020799095726317912, "classification": "negative", "trade_count": 30, "bad_pick_count": 13, "severe_loser_count": 8},
        {"year": "2025", "return_on_base_capital": 0.0865761399645996, "classification": "positive", "trade_count": 39, "bad_pick_count": 17, "severe_loser_count": 9},
        {"year": "2026", "return_on_base_capital": 0.08400922000000001, "classification": "positive", "trade_count": 15, "bad_pick_count": 5, "severe_loser_count": 3},
    ]
    months = _month_rows()
    return {
        "baseline": {"summary": baseline},
        "champion": {"summary": champion},
        "frozen": {"summary": frozen},
        "challenger": {"summary": challenger},
        "frozen_yearly": frozen_yearly,
        "challenger_yearly": challenger_yearly,
        "frozen_monthly": months,
        "challenger_monthly": months,
        "changed_top5_members_count": 189,
        "changed_rank_count": 0,
        "insufficient_refill_dates": 207,
        "threshold_selected_rows": [{}] * 84,
    }


def test_range_cap_only_retest_writes_required_artifacts_and_drops_as_insufficient(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _range_cap_root, _compare_run_root, runtime_db_path = _build_fixture_artifacts(tmp_path)
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
    monkeypatch.setattr(mod, "_build_historical_retest", lambda **_kwargs: _fake_historical_retest())

    result = mod.run(input_root=input_root, output_root=tmp_path / "out")
    out_dir = Path(result["output_root"])

    expected = {
        "range_cap_only_retest_contract.json",
        "range_cap_only_compare.json",
        "range_cap_only_yearly_performance.json",
        "range_cap_only_live_survivors.csv",
        "range_cap_only_borrow_gap_report.json",
        "range_cap_only_vs_frozen_diff.json",
        "range_cap_only_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in out_dir.iterdir()}
    assert result["decision"] == "drop_as_range_cap_insufficient"

    contract = json.loads((out_dir / "range_cap_only_retest_contract.json").read_text(encoding="utf-8"))
    compare = json.loads((out_dir / "range_cap_only_compare.json").read_text(encoding="utf-8"))
    yearly = json.loads((out_dir / "range_cap_only_yearly_performance.json").read_text(encoding="utf-8"))
    borrow = json.loads((out_dir / "range_cap_only_borrow_gap_report.json").read_text(encoding="utf-8"))
    diff = json.loads((out_dir / "range_cap_only_vs_frozen_diff.json").read_text(encoding="utf-8"))
    decision = json.loads((out_dir / "range_cap_only_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((out_dir / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((out_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    with (out_dir / "range_cap_only_live_survivors.csv").open("r", encoding="utf-8", newline="") as handle:
        survivor_rows = list(csv.DictReader(handle))

    assert contract["may_veto_removed"] is False
    assert contract["fixed_evaluation_conditions"]["same_top_k"] is True
    assert compare["current_live_survivor_count"] == 2
    assert compare["buy_overlap_code_count"] == 0
    assert compare["changed_top5_members_count"] == 189
    assert compare["changed_rank_count"] == 0
    assert compare["insufficient_refill_dates"] == 207
    assert yearly["selected_year_count"] == 3
    assert yearly["selected_month_count"] == 24
    assert borrow["summary"]["hard_borrow_gap_event_share"] == 0.0
    assert borrow["summary"]["soft_borrow_cost_event_share"] == 0.5
    assert diff["historical_selection"]["changed_top5_members_count"] == 189
    assert diff["current_live"]["current_live_survivor_count"] == 2
    assert decision["decision"] == "drop_as_range_cap_insufficient"
    assert decision["buy_level_equivalence_reached"] is False
    assert audit["no_lookahead_pass"] is True
    assert audit["future_outcome_fields_used_in_selection_or_filtering"] == []
    assert complete["complete"] is True
    assert len(survivor_rows) == 2
    assert {row["code"] for row in survivor_rows} == {"2471", "3397"}
    assert any(row["borrow_soft_cost"] == "True" for row in survivor_rows)


@pytest.mark.parametrize(
    "compare_payload, live_summary, borrow_summary, no_lookahead, expected_decision",
    [
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.25, "max_drawdown": -0.12, "profit_factor": 1.20, "bad_pick_count": 9, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": 0.05, "max_drawdown_delta": 0.03, "bad_pick_delta": -1, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 3},
            {"hard_borrow_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_borrow_cost_event_share": 0.1, "soft_cost_code_count": 0},
            {"no_lookahead_pass": True},
            "keep_for_forward_shadow",
        ),
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.24, "max_drawdown": -0.12, "profit_factor": 1.18, "bad_pick_count": 9, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": 0.04, "max_drawdown_delta": 0.03, "bad_pick_delta": -1, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 2},
            {"hard_borrow_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_borrow_cost_event_share": 0.1, "soft_cost_code_count": 0},
            {"no_lookahead_pass": True},
            "hold_requires_post_may_live_watch",
        ),
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.24, "max_drawdown": -0.12, "profit_factor": 1.18, "bad_pick_count": 9, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": 0.04, "max_drawdown_delta": 0.03, "bad_pick_delta": -1, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 3},
            {"hard_borrow_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_borrow_cost_event_share": 0.70, "soft_cost_code_count": 4},
            {"no_lookahead_pass": True},
            "hold_due_to_borrow_cost",
        ),
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.18, "max_drawdown": -0.12, "profit_factor": 1.02, "bad_pick_count": 9, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": -0.02, "max_drawdown_delta": 0.03, "bad_pick_delta": -1, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 3},
            {"hard_borrow_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_borrow_cost_event_share": 0.1, "soft_cost_code_count": 0},
            {"no_lookahead_pass": True},
            "drop_as_range_cap_insufficient",
        ),
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.25, "max_drawdown": -0.25, "profit_factor": 1.05, "bad_pick_count": 9, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": 0.05, "max_drawdown_delta": -0.10, "bad_pick_delta": -1, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 3},
            {"hard_borrow_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_borrow_cost_event_share": 0.1, "soft_cost_code_count": 0},
            {"no_lookahead_pass": True},
            "drop_due_to_drawdown_regression",
        ),
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.25, "max_drawdown": -0.12, "profit_factor": 1.05, "bad_pick_count": 12, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": 0.05, "max_drawdown_delta": 0.03, "bad_pick_delta": 2, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 3},
            {"hard_borrow_gap_event_share": 0.0, "hard_gap_code_count": 0, "soft_borrow_cost_event_share": 0.1, "soft_cost_code_count": 0},
            {"no_lookahead_pass": True},
            "drop_due_to_bad_pick_regression",
        ),
        (
                {
                    "frozen_may_veto_range_cap": {"total_return": 0.20, "max_drawdown": -0.15, "profit_factor": 1.10, "bad_pick_count": 10, "severe_loser_count": 4},
                    "range_cap_only_without_may_veto": {"total_return": 0.25, "max_drawdown": -0.12, "profit_factor": 1.05, "bad_pick_count": 9, "severe_loser_count": 4},
                    "delta_vs_frozen": {"total_return_delta": 0.05, "max_drawdown_delta": 0.03, "bad_pick_delta": -1, "severe_loser_delta": 0},
                    "insufficient_refill_dates": 0,
                    "buy_overlap_code_count": 0,
                    "changed_top5_members_count": 189,
                    "changed_rank_count": 0,
                    "selected_event_count": 84,
                    "selected_month_count": 24,
                    "selected_year_count": 3,
            },
            {"current_live_survivor_count": 3},
            {"hard_borrow_gap_event_share": 0.2, "hard_gap_code_count": 2, "soft_borrow_cost_event_share": 0.1, "soft_cost_code_count": 0},
            {"no_lookahead_pass": True},
            "drop_due_to_untradable_borrow_gap",
        ),
    ],
)
def test_build_decision_covers_all_labels(
    compare_payload: dict,
    live_summary: dict,
    borrow_summary: dict,
    no_lookahead: dict,
    expected_decision: str,
) -> None:
    decision = mod._build_decision(
        compare_payload=compare_payload,
        diff_payload={"dummy": True},
        live_summary=live_summary,
        borrow_summary=borrow_summary,
        no_lookahead=no_lookahead,
    )

    assert decision["decision"] == expected_decision
