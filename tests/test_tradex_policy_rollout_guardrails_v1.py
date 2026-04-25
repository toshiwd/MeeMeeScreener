from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_chart_first_replay import ChartState, _desired_targets
from scripts.tradex_policy_rollout_guardrails_v1 import DEFAULT_INPUT_DIR, run_policy_rollout_guardrails_v1


def test_policy_rollout_guardrails_v1_keeps_top5_long_behavior() -> None:
    state = ChartState(buy_units=2, avg_buy_price=100.0)
    row = pd.Series(
        {
            "dt": 20260401,
            "c": 95.0,
            "ma20": 100.0,
            "ma60": 110.0,
            "dist_ma20_pct": -0.05,
            "upper_wick_ratio": 0.1,
            "lower_wick_ratio": 0.1,
            "reclaim_ma20": False,
            "lose_ma20": True,
            "lose_ma60": False,
            "breakout5": False,
            "breakout10": False,
            "breakdown5": False,
            "exhaustion": False,
            "support_wick": False,
            "bull_stack": False,
            "bear_stack": False,
            "daily_main_state_ctx": "daily_neutral",
            "monthly_main_state_ctx": "monthly_neutral",
            "weekly_main_state_ctx": "weekly_neutral",
        }
    )

    top5_buy, top5_sell, top5_reason = _desired_targets(
        state,
        row,
        end_date="2026-04-30",
        policy_variant="policy_rollout_guardrails_v1",
        policy_context={"rank_bucket": "top5"},
    )
    lower_buy, lower_sell, lower_reason = _desired_targets(
        state,
        row,
        end_date="2026-04-30",
        policy_variant="policy_rollout_guardrails_v1",
        policy_context={"rank_bucket": "top11_20"},
    )

    assert top5_buy == 1
    assert top5_sell == 0
    assert top5_reason["trim"]["primary"] == "lose_ma20"

    assert lower_buy == 2
    assert lower_sell == 0
    assert lower_reason["flat"]["primary"] is not None


def test_policy_rollout_guardrails_v1_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "policy_rollout_guardrails_v1"
    result = run_policy_rollout_guardrails_v1(
        input_dir=DEFAULT_INPUT_DIR,
        output_dir=output_dir,
        row_limit=5,
    )

    assert result["summary"]["policy_variant"] == "policy_rollout_guardrails_v1"
    assert result["summary"]["policy_run_rows_count"] == 5
    assert result["summary"]["diagnosis_decision"] in {"keep", "hold", "drop"}

    for key in ("summary_json", "compare_json", "by_rank_json", "by_side_json", "by_action_json", "trade_ledger_json", "decision_json"):
        assert Path(result["paths"][key]).exists(), key

    summary = json.loads(Path(result["paths"]["summary_json"]).read_text(encoding="utf-8"))
    assert summary["policy_variant"] == "policy_rollout_guardrails_v1"
    assert summary["challenger_policy_reference"]["row_count"] == 5
