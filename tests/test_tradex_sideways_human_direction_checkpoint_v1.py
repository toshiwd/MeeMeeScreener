from __future__ import annotations

import pandas as pd

from scripts.tradex_sideways_human_direction_checkpoint_v1 import evaluate_signed_move_rule, first_hit_direction, stage_features


def test_first_hit_direction_separates_up_down_same_day_and_unresolved() -> None:
    assert first_hit_direction(pd.DataFrame({"h": [101, 103], "l": [99, 98]}), upper=102, lower=97) == ("UP", 2)
    assert first_hit_direction(pd.DataFrame({"h": [101], "l": [96]}), upper=102, lower=97) == ("DOWN", 1)
    assert first_hit_direction(pd.DataFrame({"h": [103], "l": [96]}), upper=102, lower=97) == ("SAME_DAY_BOTH", 1)
    assert first_hit_direction(pd.DataFrame({"h": [101], "l": [98]}), upper=102, lower=97) == ("UNRESOLVED", None)


def test_stage_features_add_one_group_at_a_time() -> None:
    stages = stage_features()
    assert list(stages) == ["price_move", "breakout", "candle_volume"]
    assert set(stages["price_move"]) < set(stages["breakout"]) < set(stages["candle_volume"])


def test_signed_move_rule_predicts_the_sign_without_future_features() -> None:
    rows = []
    for year in range(2020, 2026):
        for index, move in enumerate((-1.0, -0.5, 0.5, 1.0)):
            rows.append({"case_id": f"{year}-{index}", "year": year, "checkpoint": 2, "target_up": move > 0, "first_hit_day": 10, "move_atr": move})
    result, predictions = evaluate_signed_move_rule(pd.DataFrame(rows))
    assert result["accuracy"] == 1.0
    assert result["balanced_accuracy"] == 1.0
    assert predictions["decided"].all()
