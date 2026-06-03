from __future__ import annotations

from scripts import tradex_ma_role_meemee_readonly_catalog_phase16 as mod


def test_qualifies_requires_display_gates() -> None:
    state = {
        "stable_positive_ret20": True,
        "minimum_split_count": 250,
        "minimum_ret20_mean": 0.025,
        "test_positive_ret20_rate": 0.60,
        "test_bad_ret20_lt_minus_5pct_rate": 0.15,
    }
    assert mod._qualifies(state) is True
    state["test_bad_ret20_lt_minus_5pct_rate"] = 0.25
    assert mod._qualifies(state) is False


def test_build_rules_outputs_review_only_contract() -> None:
    rules = mod._build_rules(
        [
            {
                "stable_positive_ret20": True,
                "minimum_split_count": 250,
                "minimum_ret20_mean": 0.025,
                "test_positive_ret20_rate": 0.60,
                "test_bad_ret20_lt_minus_5pct_rate": 0.15,
                "ret20_mean_by_split": {"train": 0.03, "validation": 0.02, "test": 0.025},
                "entry_exit": "candle_shape:normal_bear|three_candle:three_bar_falling|close_ma7:below|close_ma20:below|ma7_ma20:below",
                "trend": "close_ma60:below|ma60_slope:down",
                "environment": "alignment:bear_alignment|ma100_slope:down|ma200_slope:down",
            }
        ]
    )
    assert rules[0]["meemee_display_mode"] == "review_only"
    assert rules[0]["actionability"] == "watch_context_not_trade_signal"
