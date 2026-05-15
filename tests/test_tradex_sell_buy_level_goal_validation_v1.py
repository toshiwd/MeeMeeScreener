import json
from pathlib import Path

from scripts import tradex_sell_buy_level_goal_validation_v1 as validation
from scripts import tradex_sell_failed_followthrough_refill_rerun_v1 as refill
from scripts import tradex_sell_failed_followthrough_meemee_reflectability_v1 as reflectability
from scripts import tradex_sell_failed_followthrough_no_lookahead_repair_v1 as clean_repair


def test_sell_buy_level_goal_validation_writes_hold_decision(tmp_path: Path) -> None:
    result = validation.run(output_root=tmp_path)

    assert result["ok"] is True
    decision_path = Path(result["artifact_refs"]["decision"])
    complete_path = Path(result["artifact_refs"]["complete"])
    assert decision_path.exists()
    assert complete_path.exists()

    payload = validation._load_json(decision_path)
    assert payload["authoritative_rollup_decision"] == "hold_for_dedicated_rerun"
    assert payload["silent_fallback_used"] is False
    assert payload["research_fallback"] is False
    assert payload["meemee_reflection"] is False

    candidate_decisions = payload["candidate_local_decisions"]
    assert candidate_decisions["sell_failed_followthrough_after_break_demotion_v1"] == "hold_for_dedicated_rerun"
    assert candidate_decisions["sell_daily_trigger_but_monthly_not_aligned_demotion_v1"] == "drop_signal_not_buy_level_equivalent"


def test_refill_decision_marks_buy_level_equivalence_when_no_blockers() -> None:
    compare = {
        "baseline": {"count": 12},
        "challenger": {"count": 12},
        "delta": {
            "changed_top5_members_count": 2,
            "changed_top10_members_count": 2,
            "mean_ret20_delta": 0.01,
            "hit_rate_delta": 0.1,
            "severe_loser_rate_delta": -0.1,
            "bad_pick_removal_count": 1,
            "added_bad_pick_count": 0,
            "added_severe_loser_count": 0,
        },
        "monthly_stability": {"positive_months": 2, "negative_months": 1},
        "regime_stability": [{"count": 3, "mean_ret20": 0.02}],
    }

    decision = refill._decision(compare)

    assert decision["authoritative_rollup_decision"] == "keep_as_buy_level_equivalent_research_candidate"
    assert decision["promote_ready_equivalent"] is True
    assert decision["buy_level_equivalence_reached"] is True


def test_reflectability_artifact_integrity_check_accepts_consistent_source(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_db_path = r"G:\Tradex\scratch\source_snapshots\snapshot.duckdb"
    same_condition = {
        "same_universe": True,
        "same_period": True,
        "same_top_k": True,
        "same_regime": True,
        "same_cost": True,
        "same_artifact_detail_level": True,
        "long_short_separated": True,
        "same_month_refill": True,
        "no_silent_fallback": True,
        "refill_liquidity20d_min": 1_000_000.0,
    }
    (source_root / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps(
            {
                "status": "complete",
                "artifact_refs": {},
                "authoritative_decision": str(source_root / "sell_failed_followthrough_refill_decision.json"),
                "silent_fallback_used": False,
                "research_fallback": False,
            }
        ),
        encoding="utf-8",
    )
    (source_root / "sell_failed_followthrough_refill_compare.json").write_text(
        json.dumps(
            {
                "candidate_id": reflectability.CANDIDATE_NAME,
                "source_db_path": source_db_path,
                "baseline": {},
                "challenger": {},
                "delta": {},
                "monthly_stability": {},
                "regime_stability": [],
                "same_condition_contract": same_condition,
                "silent_fallback_used": False,
                "research_fallback": False,
                "production_ranking_changed": False,
            }
        ),
        encoding="utf-8",
    )
    (source_root / "sell_failed_followthrough_refill_contract.json").write_text(
        json.dumps(
            {
                "axis": reflectability.CANDIDATE_NAME,
                "fixed_evaluation_conditions": same_condition,
                "source_db_path": source_db_path,
                "refill_liquidity20d_min": 1_000_000.0,
            }
        ),
        encoding="utf-8",
    )
    (source_root / "sell_failed_followthrough_refill_decision.json").write_text(
        json.dumps(
            {
                "candidate_id": reflectability.CANDIDATE_NAME,
                "authoritative_rollup_decision": "keep_as_buy_level_equivalent_research_candidate",
                "buy_level_equivalence_reached": True,
                "promote_ready_equivalent": True,
                "buy_level_blockers": [],
                "silent_fallback_used": False,
                "research_fallback": False,
                "production_ranking_changed": False,
                "meemee_reflection": False,
            }
        ),
        encoding="utf-8",
    )

    result = reflectability.build_artifact_integrity_check(source_root)

    assert result["artifact_integrity_pass"] is True


def test_reflectability_no_lookahead_audit_detects_future_return_selection() -> None:
    audit = reflectability.build_no_lookahead_audit()

    assert audit["no_lookahead_pass"] is False
    assert "short_ret_5" in audit["future_return_fields_used_in_selection"]
    assert "short_ret_10" in audit["future_return_fields_used_in_selection"]
    assert "short_ret_20" in audit["future_return_fields_used_in_selection"]


def _clean_repair_rows() -> list[dict]:
    rows = []
    for rank, code in enumerate(["A", "B", "C", "D", "E"], start=1):
        rows.append(
            {
                "row_id": f"20250131:{code}",
                "ymd": 20250131,
                "code": code,
                "selected_by_baseline": rank <= 3,
                "baseline_rank": rank if rank <= 3 else None,
                "entryScore": 100 - rank,
                "tradePriorityScore": 100 - rank,
                "liquidity20d": 2_000_000.0,
                "close_pos": 0.40 if code == "B" else 0.05,
                "day_change_pct": 0.0 if code == "B" else -0.03,
                "marketRegime": "risk_on",
                "short_ret_5": -0.2 + rank / 100,
                "short_ret_10": -0.2 + rank / 100,
                "short_ret_20": 0.02 if code in {"A", "C", "D"} else -0.03,
                "mae20": 0.01,
                "mfe20": 0.02,
                "short_win_5": False,
                "short_win_10": False,
                "short_win_20": code in {"A", "C", "D"},
            }
        )
    return rows


def test_no_lookahead_clean_selection_is_independent_of_future_return_fields() -> None:
    rows = _clean_repair_rows()

    guard = clean_repair.build_selector_guard(rows)
    selection = clean_repair.build_clean_selection(rows)

    assert guard["no_lookahead_pass"] is True
    assert guard["selection_identical_after_stripping_future_return_columns"] is True
    assert guard["selection_identical_after_randomizing_future_return_columns"] is True
    assert guard["forbidden_fields_reached_selector_view"] == []
    assert selection["monthly_rows"][0]["removed_codes"] == ["B"]
    assert selection["monthly_rows"][0]["added_codes"] == ["D"]


def test_no_lookahead_clean_selector_rejects_forbidden_view_fields() -> None:
    original_allowed = set(clean_repair.ALLOWED_SELECTION_FIELDS)
    try:
        clean_repair.ALLOWED_SELECTION_FIELDS.add("short_ret_20")
        try:
            clean_repair.build_clean_selection(_clean_repair_rows())
        except RuntimeError as exc:
            assert "forbidden fields reached clean selector view" in str(exc)
        else:
            raise AssertionError("expected forbidden field guard to fail")
    finally:
        clean_repair.ALLOWED_SELECTION_FIELDS.clear()
        clean_repair.ALLOWED_SELECTION_FIELDS.update(original_allowed)
