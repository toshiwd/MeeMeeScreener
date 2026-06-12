from pathlib import Path

import pandas as pd

from scripts import tradex_current_buy_lifecycle_board_v1 as mod


def test_buy_lifecycle_keeps_entry_and_held_review_states_separate(tmp_path: Path) -> None:
    source = tmp_path / "current_review_board.csv"
    pd.DataFrame(
        [
            {
                "as_of_date": 20260605,
                "code": "1301",
                "review_bucket": "Starter",
                "upside_probability_20d": 0.72,
                "downside_risk_probability_20d": 0.20,
                "entry_actionability_score": 0.80,
                "avoid_level": "clear",
                "event_risk_contract_status": "available",
                "failed_high_flag": False,
                "close_vs_ma20_pct": 0.02,
                "weekly_supportive_flag": True,
                "volatility_compression_breakout_preparation_candidate": False,
                "constructive_pullback_support_bullish_confirmation_reference_match": False,
                "early_trend_reclaim_controlled_extension_candidate": True,
                "monthly_weekly_supportive_daily_confirmation_candidate": True,
            },
            {
                "as_of_date": 20260605,
                "code": "1302",
                "review_bucket": "Wait",
                "upside_probability_20d": 0.55,
                "downside_risk_probability_20d": 0.70,
                "entry_actionability_score": 0.20,
                "avoid_level": "caution",
                "event_risk_contract_status": "available",
                "failed_high_flag": True,
                "close_vs_ma20_pct": -0.07,
                "weekly_supportive_flag": False,
                "volatility_compression_breakout_preparation_candidate": False,
                "constructive_pullback_support_bullish_confirmation_reference_match": False,
                "early_trend_reclaim_controlled_extension_candidate": False,
                "monthly_weekly_supportive_daily_confirmation_candidate": False,
            },
        ]
    ).to_csv(source, index=False)

    output = mod.run(source_path=source, source_root=tmp_path, output_root=tmp_path / "out")
    payload = mod.json.loads((output / "current_buy_lifecycle_board.json").read_text(encoding="utf-8"))

    assert payload["candidates"][0]["entry_state"] == "Starter"
    assert payload["candidates"][0]["held_position_review_state"] == "Hold"
    assert payload["candidates"][1]["entry_state"] == "Wait"
    assert payload["candidates"][1]["held_position_review_state"] == "ExitReview"
    assert payload["classification_contract"]["entry_and_held_states_are_separate"] is True
    assert payload["authoritative_decision"] == "current_buy_lifecycle_has_starter_or_accumulate_candidates"
