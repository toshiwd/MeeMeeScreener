from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_long_action_policy_same_day_score_guard_diagnostic_v1 import build_same_day_score_guard_diagnostic


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_prior_dirs(root: Path) -> tuple[Path, Path]:
    rank_guard_dir = root / "rank_guard"
    prior_design_dir = root / "prior_design"
    rank_guard_dir.mkdir(parents=True, exist_ok=True)
    prior_design_dir.mkdir(parents=True, exist_ok=True)

    restored = pd.DataFrame(
        [
            {
                "window_id": "w1",
                "window_label": "flat",
                "window_start_date": "2025-03-31",
                "window_end_date": "2025-04-30",
                "date": "2025-03-31",
                "decision_date": "2025-03-31",
                "symbol": "1001",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["timing_block"],
                "baseline_execution_price": 101.0,
                "variant_execution_price": None,
                "baseline_cash": 9_899_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 101_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.074,
                "baseline_rank": 4,
                "top_candidate_score": 0.060,
                "market_regime": "uptrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_5": 0.01,
                "ret_10": 0.02,
                "ret_20": 0.05,
                "forward_ret_20d": 0.05,
                "path_value_score_v1": 1,
                "skip_class": "skipped_good_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": "2025-04-08",
                "later_buy_delay_days": 5,
                "later_buy_forward_ret_20d": 0.04,
                "later_buy_delay_cost_20d": -0.01,
                "later_buy_action": "buy",
                "later_buy_within_window": True,
                "baseline_filled": True,
                "variant_filled": False,
            },
            {
                "window_id": "w1",
                "window_label": "flat",
                "window_start_date": "2025-03-31",
                "window_end_date": "2025-04-30",
                "date": "2025-03-31",
                "decision_date": "2025-03-31",
                "symbol": "1002",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["timing_block"],
                "baseline_execution_price": 151.0,
                "variant_execution_price": None,
                "baseline_cash": 9_849_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 151_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.091,
                "baseline_rank": 4,
                "top_candidate_score": 0.091,
                "market_regime": "downtrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_5": -0.01,
                "ret_10": -0.02,
                "ret_20": -0.06,
                "forward_ret_20d": -0.06,
                "path_value_score_v1": 0,
                "skip_class": "skipped_bad_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": "2025-04-08",
                "later_buy_delay_days": 5,
                "later_buy_forward_ret_20d": -0.05,
                "later_buy_delay_cost_20d": 0.02,
                "later_buy_action": "buy",
                "later_buy_within_window": True,
                "baseline_filled": True,
                "variant_filled": False,
            },
            {
                "window_id": "w2",
                "window_label": "up",
                "window_start_date": "2025-05-01",
                "window_end_date": "2025-05-31",
                "date": "2025-05-02",
                "decision_date": "2025-05-02",
                "symbol": "1003",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["timing_block"],
                "baseline_execution_price": 121.0,
                "variant_execution_price": None,
                "baseline_cash": 9_879_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 121_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.052,
                "baseline_rank": 7,
                "top_candidate_score": -0.050,
                "market_regime": "uptrend",
                "month_key": "2025-05",
                "week_key": "2025-W18",
                "ret_5": 0.02,
                "ret_10": 0.03,
                "ret_20": 0.07,
                "forward_ret_20d": 0.07,
                "path_value_score_v1": 1,
                "skip_class": "skipped_good_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": "2025-05-13",
                "later_buy_delay_days": 7,
                "later_buy_forward_ret_20d": 0.06,
                "later_buy_delay_cost_20d": -0.01,
                "later_buy_action": "buy",
                "later_buy_within_window": True,
                "baseline_filled": True,
                "variant_filled": False,
            },
            {
                "window_id": "w2",
                "window_label": "up",
                "window_start_date": "2025-05-01",
                "window_end_date": "2025-05-31",
                "date": "2025-05-02",
                "decision_date": "2025-05-02",
                "symbol": "1004",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["timing_block"],
                "baseline_execution_price": 111.0,
                "variant_execution_price": None,
                "baseline_cash": 9_889_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 111_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.044,
                "baseline_rank": 6,
                "top_candidate_score": 0.120,
                "market_regime": "downtrend",
                "month_key": "2025-05",
                "week_key": "2025-W18",
                "ret_5": -0.01,
                "ret_10": -0.03,
                "ret_20": -0.08,
                "forward_ret_20d": -0.08,
                "path_value_score_v1": 0,
                "skip_class": "skipped_bad_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": "2025-05-13",
                "later_buy_delay_days": 7,
                "later_buy_forward_ret_20d": -0.04,
                "later_buy_delay_cost_20d": 0.02,
                "later_buy_action": "buy",
                "later_buy_within_window": True,
                "baseline_filled": True,
                "variant_filled": False,
            },
        ]
    )
    remaining = pd.DataFrame(
        [
            {
                "window_id": "w3",
                "window_label": "flat",
                "window_start_date": "2025-06-01",
                "window_end_date": "2025-06-30",
                "date": "2025-06-03",
                "decision_date": "2025-06-03",
                "symbol": "2001",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["entry_threshold_not_met"],
                "baseline_execution_price": 201.0,
                "variant_execution_price": None,
                "baseline_cash": 9_799_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 201_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.041,
                "baseline_rank": 12,
                "top_candidate_score": -0.050,
                "market_regime": "uptrend",
                "month_key": "2025-06",
                "week_key": "2025-W23",
                "ret_5": 0.01,
                "ret_10": 0.02,
                "ret_20": 0.04,
                "forward_ret_20d": 0.04,
                "path_value_score_v1": 1,
                "skip_class": "skipped_good_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": "2025-06-12",
                "later_buy_delay_days": 6,
                "later_buy_forward_ret_20d": 0.03,
                "later_buy_delay_cost_20d": -0.01,
                "later_buy_action": "buy",
                "later_buy_within_window": True,
                "baseline_filled": True,
                "variant_filled": False,
            },
            {
                "window_id": "w3",
                "window_label": "flat",
                "window_start_date": "2025-06-01",
                "window_end_date": "2025-06-30",
                "date": "2025-06-03",
                "decision_date": "2025-06-03",
                "symbol": "2002",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["cost_turnover_block"],
                "baseline_execution_price": 211.0,
                "variant_execution_price": None,
                "baseline_cash": 9_789_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 211_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.043,
                "baseline_rank": 15,
                "top_candidate_score": 0.120,
                "market_regime": "downtrend",
                "month_key": "2025-06",
                "week_key": "2025-W23",
                "ret_5": -0.02,
                "ret_10": -0.03,
                "ret_20": -0.05,
                "forward_ret_20d": -0.05,
                "path_value_score_v1": 0,
                "skip_class": "skipped_bad_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": "2025-06-12",
                "later_buy_delay_days": 6,
                "later_buy_forward_ret_20d": -0.03,
                "later_buy_delay_cost_20d": 0.02,
                "later_buy_action": "buy",
                "later_buy_within_window": True,
                "baseline_filled": True,
                "variant_filled": False,
            },
        ]
    )
    restored.to_parquet(rank_guard_dir / "restored_buy_cases.parquet", index=False)
    remaining.to_parquet(rank_guard_dir / "remaining_skipped_buy_cases.parquet", index=False)
    _write_json(rank_guard_dir / "rank_guard_diagnostic.json", {"schema_version": "x", "single_cutoff_justified": False})
    _write_json(rank_guard_dir / "rank_guard_tighten_decision.json", {"schema_version": "x", "final_status": "insufficient_rank_separation"})
    _write_json(rank_guard_dir / "skipped_buy_restoration_summary.json", {"schema_version": "x", "restored_buy_total": 4})
    _write_json(rank_guard_dir / "portfolio_economic_comparison.json", {"schema_version": "x", "pairwise_delta": {}})
    _write_json(rank_guard_dir / "drawdown_attribution_summary.json", {"schema_version": "x", "worse_drawdown_row_count": 0})
    _write_json(rank_guard_dir / "branch_effect_audit.json", {"schema_version": "x", "branch_effect_present": True})
    _write_json(rank_guard_dir / "entry_delay_cost_summary.json", {"schema_version": "x", "entry_delay_cost_mean": 0.0})
    _write_json(rank_guard_dir / "monthly_effectiveness_summary.json", {"schema_version": "x", "rows": []})
    _write_json(rank_guard_dir / "regime_effectiveness_summary.json", {"schema_version": "x", "rows": []})

    _write_json(prior_design_dir / "gate_redesign_policy_spec.json", {"schema_version": "x", "rule": {"baseline_rank": 11}})
    _write_json(prior_design_dir / "gate_redesign_feature_availability.json", {"schema_version": "x", "available_columns": list(restored.columns)})
    _write_json(prior_design_dir / "skipped_buy_restoration_summary.json", {"schema_version": "x", "restored_buy_total": 4})
    _write_json(prior_design_dir / "portfolio_economic_comparison.json", {"schema_version": "x", "pairwise_delta": {}})
    _write_json(prior_design_dir / "entry_delay_cost_summary.json", {"schema_version": "x", "entry_delay_cost_mean": 0.0})
    _write_json(prior_design_dir / "monthly_effectiveness_summary.json", {"schema_version": "x", "rows": []})
    _write_json(prior_design_dir / "regime_effectiveness_summary.json", {"schema_version": "x", "rows": []})
    _write_json(prior_design_dir / "drawdown_attribution_summary.json", {"schema_version": "x", "worse_drawdown_row_count": 0})
    return rank_guard_dir, prior_design_dir


def test_same_day_score_guard_diagnostic_generates_artifacts_and_inventory(tmp_path: Path) -> None:
    rank_guard_dir, prior_design_dir = _make_prior_dirs(tmp_path)
    output_root = tmp_path / "diag"
    result = build_same_day_score_guard_diagnostic(output_root, rank_guard_dir=rank_guard_dir, prior_design_dir=prior_design_dir)

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "same_day_score_field_inventory.json",
        "restored_good_bad_score_contrast.json",
        "remaining_skipped_score_contrast.json",
        "score_guard_conflict_cases.parquet",
        "score_guard_conflict_summary.json",
        "same_day_score_guard_hypotheses.json",
        "same_day_score_guard_diagnostic_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected <= {path.name for path in session_dir.iterdir()}

    inventory = json.loads((session_dir / "same_day_score_field_inventory.json").read_text(encoding="utf-8"))
    field_rows = {row["field"]: row for row in inventory["field_rows"]}
    assert field_rows["baseline_score"]["status"] == "confirmed usable"
    assert field_rows["top_candidate_score"]["status"] == "confirmed usable"
    assert field_rows["score_gap"]["status"] == "proxy only"
    assert field_rows["candidate_score"]["status"] == "missing"
    assert field_rows["ret_20"]["status"] == "forbidden outcome field"

    restored_contrast = json.loads((session_dir / "restored_good_bad_score_contrast.json").read_text(encoding="utf-8"))
    remaining_contrast = json.loads((session_dir / "remaining_skipped_score_contrast.json").read_text(encoding="utf-8"))
    assert restored_contrast["row_count"] == 4
    assert remaining_contrast["row_count"] == 4 - 2
    assert "baseline_score" in restored_contrast["field_contrasts"]
    assert "score_gap" in restored_contrast["field_contrasts"]

    conflict = pd.read_parquet(session_dir / "score_guard_conflict_cases.parquet")
    assert len(conflict) == 6
    assert {"score_gap", "score_abs_gap", "source_case_set"} <= set(conflict.columns)

    decision = json.loads((session_dir / "same_day_score_guard_diagnostic_decision.json").read_text(encoding="utf-8"))
    assert decision["final_status"] == "insufficient_score_separation"

    hypotheses = json.loads((session_dir / "same_day_score_guard_hypotheses.json").read_text(encoding="utf-8"))
    assert hypotheses["hypotheses"]
    assert any(hypothesis["hypothesis_id"] == "freeze_line_v1" for hypothesis in hypotheses["hypotheses"])
