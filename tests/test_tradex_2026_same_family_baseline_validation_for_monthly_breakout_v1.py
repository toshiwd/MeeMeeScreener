from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_2026_same_family_baseline_validation_for_monthly_breakout_v1 import (
    REQUIRED_ARTIFACTS,
    compare_topk,
    conflict_summary,
    decide,
    score_rows,
    trading_cutoff,
)


def test_trading_cutoff_uses_twenty_trading_rows(tmp_path: Path) -> None:
    dates = pd.date_range("2026-01-01", periods=30, freq="D")
    pd.DataFrame({"date": dates.strftime("%Y-%m-%d")}).to_csv(tmp_path / "daily.csv", index=False)

    cutoff = trading_cutoff(tmp_path / "daily.csv", horizon=20)

    assert cutoff["latest_confirmed_daily_date"] == "2026-01-30"
    assert cutoff["ret20_label_safe_cutoff_date"] == "2026-01-10"


def test_score_rows_applies_same_fixed_demotion() -> None:
    rows = pd.DataFrame(
        [
            {"decision_ymd": 20260105, "code": "A", "candidate_rank": 1, "selection_score": 10, "selected_for_buy": True, "source_year": 2026, "year": 2026, "monthly_box_breakout_proxy": True, "monthly_high_zone_proxy": True, "monthly_box_inside_proxy": False, "above60_streak": 1, "days_since_ma60_reclaim": 1, "ret20": 0.01, "ret40": 0.02, "mae20": -0.01, "mfe20": 0.02},
            {"decision_ymd": 20260105, "code": "B", "candidate_rank": 2, "selection_score": 9.5, "selected_for_buy": True, "source_year": 2026, "year": 2026, "monthly_box_breakout_proxy": False, "monthly_high_zone_proxy": False, "monthly_box_inside_proxy": True, "above60_streak": 1, "days_since_ma60_reclaim": 1, "ret20": 0.03, "ret40": 0.04, "mae20": -0.01, "mfe20": 0.04},
        ]
    )

    scored = score_rows(rows)

    assert scored.loc[scored["code"] == "A", "challenger_score"].iloc[0] == 9
    assert scored.loc[scored["code"] == "B", "challenger_rank"].iloc[0] == 1


def test_compare_topk_reports_2026_label_safe() -> None:
    rows = pd.DataFrame(
        [
            {"decision_ymd": 20260105, "code": str(i), "year": 2026, "baseline_rank_recalc": i, "challenger_rank": 6 - i, "ret20": i / 100, "monthly_box_breakout_proxy": i < 3, "above60_streak": i, "days_since_ma60_reclaim": i}
            for i in range(1, 6)
        ]
    )

    summary, repl = compare_topk(rows)

    assert "2026_label_safe" in summary
    assert not repl.empty


def test_decide_holds_when_2026_label_safe_rows_thin() -> None:
    decision = decide({"rows_with_ret20": 10}, {}, {"2026_label_safe": {"top10": {}}})

    assert decision["research_decision"] == "hold_until_more_2026_labels"
