from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from scripts.tradex_long_action_policy_rank_guard_tighten_v1 import build_rank_guard_tighten_review


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_prior_design_dir(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    prior = root / "prior_design"
    prior.mkdir(parents=True, exist_ok=True)
    placeholders = {
        "branch_effect_audit.json": {"schema_version": "x", "branch_effect_present": True},
        "portfolio_economic_comparison.json": {"schema_version": "x", "pairwise_delta": {}},
        "skipped_buy_restoration_summary.json": {"schema_version": "x", "restored_buy_total": 0},
        "entry_delay_cost_summary.json": {"schema_version": "x", "entry_delay_cost_mean": 0.0},
        "monthly_effectiveness_summary.json": {"schema_version": "x", "rows": []},
        "regime_effectiveness_summary.json": {"schema_version": "x", "rows": []},
        "drawdown_attribution_summary.json": {"schema_version": "x", "worse_drawdown_row_count": 0},
    }
    for name, payload in placeholders.items():
        (prior / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    frame = pd.DataFrame(
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
                "baseline_score": 0.064,
                "baseline_rank": 4,
                "top_candidate_score": 0.072,
                "market_regime": "uptrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_20": 0.05,
                "path_value_score_v1": 1,
                "skip_class": "skipped_good_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_delay_cost_20d": -0.01,
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
                "baseline_score": 0.071,
                "baseline_rank": 4,
                "top_candidate_score": 0.072,
                "market_regime": "downtrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_20": -0.06,
                "path_value_score_v1": 0,
                "skip_class": "skipped_bad_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_delay_cost_20d": 0.02,
            },
            {
                "window_id": "w1",
                "window_label": "flat",
                "window_start_date": "2025-03-31",
                "window_end_date": "2025-04-30",
                "date": "2025-03-31",
                "decision_date": "2025-03-31",
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
                "baseline_score": 0.082,
                "baseline_rank": 7,
                "top_candidate_score": 0.072,
                "market_regime": "uptrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_20": 0.07,
                "path_value_score_v1": 1,
                "skip_class": "skipped_good_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_delay_cost_20d": -0.01,
            },
        ]
    )
    frame.to_parquet(prior / "restored_buy_cases.parquet", index=False)
    pd.DataFrame([{"baseline_rank": 99, "skip_class": "skipped_bad_buy"}]).to_parquet(prior / "remaining_skipped_buy_cases.parquet", index=False)
    return prior


def test_rank_guard_tighten_runner_stops_when_good_and_bad_overlap_by_rank(tmp_path: Path):
    prior = _make_prior_design_dir(tmp_path)
    output_root = tmp_path / "rank_guard"
    result = build_rank_guard_tighten_review(output_root, prior_design_dir=prior)

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "rank_guard_diagnostic.json",
        "rank_guard_policy_spec.json",
        "branch_effect_audit.json",
        "portfolio_economic_comparison.json",
        "skipped_buy_restoration_summary.json",
        "restored_buy_cases.parquet",
        "remaining_skipped_buy_cases.parquet",
        "entry_delay_cost_summary.json",
        "monthly_effectiveness_summary.json",
        "regime_effectiveness_summary.json",
        "drawdown_attribution_summary.json",
        "rank_guard_tighten_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected <= {path.name for path in session_dir.iterdir()}

    diagnostic = _load_json(session_dir / "rank_guard_diagnostic.json")
    assert diagnostic["single_cutoff_justified"] is False
    assert diagnostic["overlap_ranks"] == [4]

    policy_spec = _load_json(session_dir / "rank_guard_policy_spec.json")
    assert policy_spec["status"] == "insufficient_rank_separation"
    assert policy_spec["selected_rank_cutoff"] is None

    decision = _load_json(session_dir / "rank_guard_tighten_decision.json")
    assert decision["final_status"] == "insufficient_rank_separation"
    assert result["candidate_generated"] is False

    restored = pd.read_parquet(session_dir / "restored_buy_cases.parquet")
    assert len(restored) == 3
