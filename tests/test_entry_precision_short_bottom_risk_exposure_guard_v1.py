from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts import entry_precision_short_bottom_risk_exposure_guard_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _row(
    ymd: int,
    code: str,
    group: str,
    *,
    baseline_selected: bool,
    challenger_selected: bool,
    short_ret20: float,
    outcome_positive: bool,
) -> dict:
    return {
        "ymd": ymd,
        "code": code,
        "confusion_group": group,
        "baseline_selected": baseline_selected,
        "challenger_selected": challenger_selected,
        "outcome_known": True,
        "outcome_positive": outcome_positive,
        "outcome_bucket": "positive" if outcome_positive else "nonpositive",
        "short_ret_20": short_ret20,
        "short_ret_10": short_ret20 / 2.0,
        "short_ret_5": short_ret20 / 4.0,
        "close_pos": 0.0,
        "dist_low20": 0.0,
        "dist_ma20_signed": 0.0,
        "day_change_pct": 0.0,
        "monthlyRangeProb": 0.2,
        "monthlyRangePos": 0.0,
        "weeklyBreakoutDownProb": 0.0,
        "monthlyBreakoutDownProb": 0.0,
        "marketRiskOff": True,
        "marketRegime": "risk_off",
        "trendDownStrict": True,
        "entryScore": 0.8,
        "tradePriorityScore": 0.8,
        "liquidity20d": 100000.0,
        "mae20": abs(short_ret20) / 2.0,
        "mfe20": abs(short_ret20),
        "baseline_rank": 1,
        "tradeDecisionReasons": "[]",
        "tradeRiskWatch": "[]",
    }


def _build_fixture_root(tmp_path: Path) -> Path:
    source_root = tmp_path / "borrow_decomp_root"
    compare_root = tmp_path / "full_recheck_root"
    diagnostic_root = tmp_path / "diagnostic_root"

    rows = [
        _row(20250331, "A001", "kept_good", baseline_selected=True, challenger_selected=False, short_ret20=0.04, outcome_positive=True),
        _row(20250331, "A002", "retained_good", baseline_selected=True, challenger_selected=True, short_ret20=0.03, outcome_positive=True),
        _row(20250331, "A003", "retained_bad", baseline_selected=True, challenger_selected=True, short_ret20=-0.02, outcome_positive=False),
        _row(20250430, "A004", "retained_good", baseline_selected=True, challenger_selected=True, short_ret20=0.01, outcome_positive=True),
        _row(20250430, "A005", "removed_bad", baseline_selected=True, challenger_selected=False, short_ret20=-0.06, outcome_positive=False),
    ]
    _write_csv(diagnostic_root / "short_bottom_risk_confusion_groups.csv", rows)

    borrow_rows = [
        {
            "event_id": "20250331:A002:retained_good",
            "ymd": 20250331,
            "signal_date_iso": "2025-03-31",
            "code": "A002",
            "name": "Name A002",
            "confusion_group": "retained_good",
            "baseline_selected": True,
            "challenger_selected": True,
            "outcome_known": True,
            "outcome_positive": True,
            "outcome_bucket": "positive",
            "short_ret_20": 0.03,
            "short_ret_10": 0.015,
            "short_ret_5": 0.0075,
            "borrow_bucket": "soft_borrow_cost_flagged",
            "borrow_bucket_reason": "soft_cost",
            "hard_borrow_gap": False,
            "hard_borrow_gap_reason": "",
            "soft_borrow_cost_flagged": True,
            "soft_borrow_cost_reasons": json.dumps(["current_fee_positive"]),
            "borrowable_proxy_ok": False,
            "current_fee_yen": 0.05,
            "loan_ratio": 1.2,
            "restriction_count": 0,
            "sector33_code": "SEC01",
            "sector33_name": "Sector 1",
            "market_code": "TSE",
            "monthlyRangeProb": 0.2,
            "tradePriorityScore": 0.8,
            "marketRegime": "risk_off",
            "trendDownStrict": True,
        },
        {
            "event_id": "20250331:A003:retained_bad",
            "ymd": 20250331,
            "signal_date_iso": "2025-03-31",
            "code": "A003",
            "name": "Name A003",
            "confusion_group": "retained_bad",
            "baseline_selected": True,
            "challenger_selected": True,
            "outcome_known": True,
            "outcome_positive": False,
            "outcome_bucket": "nonpositive",
            "short_ret_20": -0.02,
            "short_ret_10": -0.01,
            "short_ret_5": -0.005,
            "borrow_bucket": "soft_borrow_cost_flagged",
            "borrow_bucket_reason": "soft_cost",
            "hard_borrow_gap": False,
            "hard_borrow_gap_reason": "",
            "soft_borrow_cost_flagged": True,
            "soft_borrow_cost_reasons": json.dumps(["loan_ratio_high"]),
            "borrowable_proxy_ok": False,
            "current_fee_yen": 0.0,
            "loan_ratio": 1.1,
            "restriction_count": 0,
            "sector33_code": "SEC01",
            "sector33_name": "Sector 1",
            "market_code": "TSE",
            "monthlyRangeProb": 0.2,
            "tradePriorityScore": 0.8,
            "marketRegime": "risk_off",
            "trendDownStrict": True,
        },
        {
            "event_id": "20250430:A004:retained_good",
            "ymd": 20250430,
            "signal_date_iso": "2025-04-30",
            "code": "A004",
            "name": "Name A004",
            "confusion_group": "retained_good",
            "baseline_selected": True,
            "challenger_selected": True,
            "outcome_known": True,
            "outcome_positive": True,
            "outcome_bucket": "positive",
            "short_ret_20": 0.01,
            "short_ret_10": 0.005,
            "short_ret_5": 0.0025,
            "borrow_bucket": "clean_borrowable",
            "borrow_bucket_reason": "clean",
            "hard_borrow_gap": False,
            "hard_borrow_gap_reason": "",
            "soft_borrow_cost_flagged": False,
            "soft_borrow_cost_reasons": json.dumps([]),
            "borrowable_proxy_ok": True,
            "current_fee_yen": 0.0,
            "loan_ratio": 0.3,
            "restriction_count": 0,
            "sector33_code": "SEC02",
            "sector33_name": "Sector 2",
            "market_code": "TSE",
            "monthlyRangeProb": 0.2,
            "tradePriorityScore": 0.8,
            "marketRegime": "risk_off",
            "trendDownStrict": True,
        },
    ]
    _write_csv(source_root / "short_bottom_risk_borrow_bucket_events.csv", borrow_rows)

    compare = {
        "baseline": {"count": 5, "hit_rate": 0.4, "mean_ret20": -0.01, "median_ret20": -0.02, "positive_count": 2, "nonpositive_count": 3},
        "challenger": {"count": 3, "hit_rate": 0.6666666667, "mean_ret20": 0.0066666667, "median_ret20": 0.01, "positive_count": 2, "nonpositive_count": 1},
        "delta": {"hit_rate_delta": 0.2666666667, "known_selected_count_delta": -2, "mean_ret20_delta": 0.0166666667, "median_ret20_delta": 0.03, "removed_bad_known": 1, "removed_good_known": 1, "retained_bad_known": 1, "kept_good_known": 2},
        "selection_branching": {"changed_rank_count": 1, "changed_top10_members_count": 2, "changed_top5_members_count": 2, "selection_divergence_reason": "test_fixture"},
        "full_recheck_summary": {
            "baseline": {"count": 5, "hit_rate": 0.4, "mean_ret20": -0.01, "median_ret20": -0.02, "positive_count": 2, "nonpositive_count": 3},
            "challenger": {"count": 3, "hit_rate": 0.6666666667, "mean_ret20": 0.0066666667, "median_ret20": 0.01, "positive_count": 2, "nonpositive_count": 1},
            "delta": {"hit_rate_delta": 0.2666666667, "known_selected_count_delta": -2, "mean_ret20_delta": 0.0166666667, "median_ret20_delta": 0.03, "removed_bad_known": 1, "removed_good_known": 1, "retained_bad_known": 1, "kept_good_known": 2},
            "completed_month_count": 2,
            "full_recheck_keep_persistence": True,
            "resolved_selected_count": 5,
        },
    }
    _write_json(compare_root / "short_bottom_risk_full_recheck_compare.json", compare)

    _write_json(
        source_root / "short_bottom_risk_borrow_decomposition_contract.json",
        {
            "schema_version": "tradex_entry_precision_short_bottom_risk_borrow_decomposition_v1_contract_v1",
            "session_id": "short_cleanup_bottom_risk_v1-borrow-decomposition-test",
            "generated_at": "2026-05-17T05:11:25Z",
            "axis": "short_cleanup_bottom_risk_v1",
            "input_artifacts": {
                "borrow_bucket_events": str(source_root / "short_bottom_risk_borrow_bucket_events.csv"),
                "borrow_bucket_summary": str(source_root / "short_bottom_risk_borrow_bucket_summary.json"),
                "borrow_adjusted_compare": str(source_root / "short_bottom_risk_borrow_adjusted_compare.json"),
                "full_recheck_compare": str(compare_root / "short_bottom_risk_full_recheck_compare.json"),
                "confusion_groups": str(diagnostic_root / "short_bottom_risk_confusion_groups.csv"),
            },
            "fixed_evaluation_conditions": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": True,
                "same_regime": True,
                "same_cost_slippage": True,
                "same_artifact_detail_level": True,
                "long_logic_frozen": True,
                "no_lookahead_contract": True,
                "no_meemee_ui_change": True,
                "no_production_state_change": True,
                "one_axis_only": True,
            },
        },
    )
    _write_json(
        source_root / "short_bottom_risk_borrow_decomposition_decision.json",
        {
            "schema_version": "tradex_entry_precision_short_bottom_risk_borrow_decomposition_v1_decision_v1",
            "session_id": "short_cleanup_bottom_risk_v1-borrow-decomposition-test",
            "decision": "hold_due_to_insufficient_clean_borrowable_sample",
            "source_keep_replay_decision": "hold_due_to_borrow_proxy_gap",
            "source_full_recheck_decision": "keep_for_stability_replay",
            "borrow_summary": {
                "selected_event_count": 3,
                "selected_code_count": 3,
                "hard_borrow_gap_event_count": 0,
                "hard_borrow_gap_event_share": 0.0,
                "soft_borrow_cost_event_count": 2,
                "soft_borrow_cost_event_share": 0.6666666667,
                "clean_borrowable_event_count": 1,
                "clean_borrowable_event_share": 0.3333333333,
            },
            "borrow_adjusted_compare": {
                "dependency_readout": {
                    "edge_depends_on_soft_cost_names": True,
                    "clean_sample_too_small": True,
                }
            },
        },
    )
    _write_json(
        source_root / "short_bottom_risk_borrow_bucket_summary.json",
        {
            "selected_summary": {
                "selected_event_count": 3,
                "selected_code_count": 3,
                "hard_borrow_gap_event_count": 0,
                "hard_borrow_gap_event_share": 0.0,
                "soft_borrow_cost_event_count": 2,
                "soft_borrow_cost_event_share": 0.6666666667,
                "clean_borrowable_event_count": 1,
                "clean_borrowable_event_share": 0.3333333333,
            }
        },
    )
    _write_json(
        source_root / "short_bottom_risk_borrow_adjusted_compare.json",
        {
            "dependency_readout": {
                "edge_depends_on_soft_cost_names": True,
                "clean_sample_too_small": True,
                "soft_cost_bucket_positive_count": 2,
                "soft_cost_bucket_nonpositive_count": 0,
                "soft_cost_bucket_mean_ret20": 0.03,
                "clean_bucket_positive_count": 1,
                "clean_bucket_nonpositive_count": 0,
                "clean_bucket_mean_ret20": 0.01,
            },
            "selected_borrow_gate_projection": {
                "hard_only_gate_breadth_ok": True,
                "clean_only_gate_breadth_ok": False,
            },
        },
    )
    _write_json(source_root / "no_lookahead_audit.json", {"no_lookahead_pass": True})

    return source_root


def test_exposure_guard_real_run_writes_required_artifacts_and_drops_due_to_overblocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _build_fixture_root(tmp_path)
    runtime_status = {
        "validated": True,
        "selected_runtime_db_path": str(tmp_path / "runtime.duckdb"),
        "freshness_state": "fresh",
        "freshness_days": 2,
        "runtime_db_freshness_state": "fresh",
        "runtime_db_freshness_days": 2,
        "runtime_latest_available_global_date": 20260515,
        "runtime_latest_confirmed_daily_bars_date": 20260514,
    }
    freshness = {
        "confirmed": True,
        "current_candidate_available": True,
        "direction": "short",
        "freshness_days": 3,
        "freshness_state": "fresh",
        "snapshot_as_of": "2026-05-14",
    }
    monkeypatch.setattr(mod, "get_runtime_stock_db_status", lambda: dict(runtime_status))
    monkeypatch.setattr(mod, "get_rankings_freshness", lambda **_kwargs: dict(freshness))

    result = mod.run(source_root=source_root, output_root=tmp_path / "out")
    out_dir = Path(result["output_root"])

    expected = {
        "short_bottom_risk_exposure_guard_contract.json",
        "short_bottom_risk_exposure_guard_compare.json",
        "short_bottom_risk_size_reduction_compare.json",
        "short_bottom_risk_borrow_caveat_compare.json",
        "short_bottom_risk_bad_exposure_reduction.json",
        "short_bottom_risk_exposure_guard_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in out_dir.iterdir()}
    assert result["decision"] == "drop_due_to_overblocking_good_shorts"

    compare = json.loads((out_dir / "short_bottom_risk_exposure_guard_compare.json").read_text(encoding="utf-8"))
    size_compare = json.loads((out_dir / "short_bottom_risk_size_reduction_compare.json").read_text(encoding="utf-8"))
    caveat_compare = json.loads((out_dir / "short_bottom_risk_borrow_caveat_compare.json").read_text(encoding="utf-8"))
    reduction = json.loads((out_dir / "short_bottom_risk_bad_exposure_reduction.json").read_text(encoding="utf-8"))
    decision = json.loads((out_dir / "short_bottom_risk_exposure_guard_decision.json").read_text(encoding="utf-8"))

    assert compare["baseline"]["guard_flagged_good_count"] == 2
    assert compare["baseline"]["guard_flagged_bad_count"] == 1
    assert compare["baseline"]["guard_good_vs_bad_ratio"] == pytest.approx(2.0)
    assert compare["flagged_subset_summary"]["good_count"] == 2
    assert compare["flagged_subset_summary"]["bad_count"] == 1
    assert compare["flagged_subset_summary"]["edge_vs_unflagged_mean_delta"] > 0
    assert size_compare["guard_effectiveness"]["good_short_overblocked"] is True
    assert size_compare["guard_effectiveness"]["harmful_short_exposure_reduced"] is True
    assert caveat_compare["borrow_caveat_effectiveness"]["hard_only_no_op"] is True
    assert caveat_compare["borrow_caveat_effectiveness"]["soft_caveat_no_op"] is True
    assert reduction["summary"]["good_short_overblocked"] is True
    assert reduction["summary"]["harmful_short_exposure_reduced"] is True
    assert decision["decision"] == "drop_due_to_overblocking_good_shorts"
    assert decision["criteria_state"]["no_lookahead_pass"] is True
    assert decision["criteria_state"]["production_state_unchanged"] is True


@pytest.mark.parametrize(
    "summary, compare, expected",
    [
        (
            {"harmful_short_exposure_reduced": False, "good_short_overblocked": False},
            {
                "baseline": {"guarded_effective_weight": 1.0, "guarded_hard_gap_weight": 0.0},
                "scenarios": {
                    "size_reducer": {"effective_selected_short_count": 1.0, "good_removed_weight": 0.0, "bad_removed_weight": 0.0},
                    "full_veto": {"effective_selected_short_count": 1.0, "good_removed_weight": 0.0, "bad_removed_weight": 0.0},
                },
                "deltas": {
                    "full_veto": {"borrow_soft_cost_exposure_delta": 0.0},
                    "size_reducer": {"borrow_soft_cost_exposure_delta": 0.0},
                    "hard_borrow_only_allowance": {"hard_borrow_gap_exposure_delta": 0.0},
                },
                "flagged_subset_summary": {"edge_depends_on_soft_cost_names": False, "mean_ret20": 0.0, "unflagged_mean_ret20": 0.0},
            },
            "drop_as_guard_does_not_reduce_bad_exposure",
        ),
        (
            {"harmful_short_exposure_reduced": True, "good_short_overblocked": True},
            {
                "baseline": {"guarded_effective_weight": 1.0, "guarded_hard_gap_weight": 0.0},
                "scenarios": {
                    "size_reducer": {"effective_selected_short_count": 0.5, "good_removed_weight": 0.6, "bad_removed_weight": 0.4},
                    "full_veto": {"effective_selected_short_count": 0.0, "good_removed_weight": 1.0, "bad_removed_weight": 0.9},
                },
                "deltas": {
                    "full_veto": {"borrow_soft_cost_exposure_delta": -1.0},
                    "size_reducer": {"borrow_soft_cost_exposure_delta": -0.5},
                    "hard_borrow_only_allowance": {"hard_borrow_gap_exposure_delta": 0.0},
                },
                "flagged_subset_summary": {"edge_depends_on_soft_cost_names": True, "mean_ret20": -0.1, "unflagged_mean_ret20": -0.2},
            },
            "drop_due_to_overblocking_good_shorts",
        ),
        (
            {"harmful_short_exposure_reduced": True, "good_short_overblocked": False},
            {
                "baseline": {"guarded_effective_weight": 1.0, "guarded_hard_gap_weight": 0.0},
                "scenarios": {
                    "size_reducer": {"effective_selected_short_count": 0.5, "good_removed_weight": 0.4, "bad_removed_weight": 0.6},
                    "full_veto": {"effective_selected_short_count": 0.0, "good_removed_weight": 0.4, "bad_removed_weight": 0.6},
                },
                "deltas": {
                    "full_veto": {"borrow_soft_cost_exposure_delta": -1.0},
                    "size_reducer": {"borrow_soft_cost_exposure_delta": -0.5},
                    "hard_borrow_only_allowance": {"hard_borrow_gap_exposure_delta": 0.0},
                },
                "flagged_subset_summary": {"edge_depends_on_soft_cost_names": False, "mean_ret20": 0.1, "unflagged_mean_ret20": -0.1},
            },
            "keep_as_short_exposure_reduction_guard",
        ),
    ],
)
def test_decision_builder_label_branches(summary: dict, compare: dict, expected: str) -> None:
    source_context = {"no_lookahead": {"no_lookahead_pass": True}}
    runtime_context = {"runtime_status": {"validated": True}}
    bad_reduction = {"summary": summary}
    decision = mod._build_decision(compare, bad_reduction, runtime_context, source_context)
    assert decision["decision"] == expected
    assert decision["criteria_state"]["no_lookahead_pass"] is True
