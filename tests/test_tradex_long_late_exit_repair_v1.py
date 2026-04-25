from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_chart_first_replay import ChartState
from scripts.tradex_long_late_exit_repair_v1 import DEFAULT_INPUT_DIR, run_long_late_exit_repair_v1
from scripts.tradex_chart_first_replay import _desired_targets


def test_long_late_exit_repair_v1_exits_earlier_for_top6_20() -> None:
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

    baseline_buy, baseline_sell, baseline_reason = _desired_targets(state, row, end_date="2026-04-30")
    repair_buy, repair_sell, repair_reason = _desired_targets(
        state,
        row,
        end_date="2026-04-30",
        policy_variant="long_late_exit_repair_v1",
        policy_context={"rank_bucket": "top6_10"},
    )

    assert baseline_buy == 1
    assert baseline_sell == 0
    assert baseline_reason["trim"]["primary"] == "lose_ma20"
    assert repair_buy == 0
    assert repair_sell == 0
    assert repair_reason["exit"]["primary"] == "lose_ma20_early_exit"


def test_long_late_exit_repair_v1_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "long_late_exit_repair_v1"
    result = run_long_late_exit_repair_v1(
        input_dir=DEFAULT_INPUT_DIR,
        output_dir=output_dir,
        row_limit=5,
    )

    assert result["summary"]["policy_variant"] == "long_late_exit_repair_v1"
    assert result["summary"]["policy_run_rows_count"] == 5
    assert result["summary"]["diagnosis_decision"] in {"keep", "hold", "drop"}

    for key in ("summary_json", "compare_json", "by_rank_json", "by_side_json", "by_action_json", "trade_ledger_json"):
        assert Path(result["paths"][key]).exists(), key

    summary = json.loads(Path(result["paths"]["summary_json"]).read_text(encoding="utf-8"))
    assert summary["policy_variant"] == "long_late_exit_repair_v1"
    assert summary["repair_policy_reference"]["row_count"] == 5
