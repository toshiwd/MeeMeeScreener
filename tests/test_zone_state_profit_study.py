from __future__ import annotations

import pandas as pd

from scripts.note_trade_repro_backtest import _add_daily_coordinates
from scripts.zone_state_profit_study import (
    _add_bar_parts,
    _add_ma_context,
    _compute_band_features,
    _build_research_plan,
    _render_markdown,
    _transition_focus_summary,
    _signal_return_mode,
    _state_label_series,
)


def test_compute_band_features_changes_with_lookback() -> None:
    frame = pd.DataFrame(
        {
            "code": ["1001"] * 12,
            "date": list(range(12)),
            "dt": pd.date_range("2025-01-01", periods=12, freq="D"),
            "o": [110.0] * 12,
            "h": [120.0] * 12,
            "l": [100.0] * 12,
            "c": [115.0] * 12,
            "v": [1000.0] * 12,
            "ma7": [111.0] * 12,
            "ma20": [110.0] * 12,
            "ma60": [112.0] * 12,
        }
    )
    frame.loc[0, "l"] = 50.0
    frame = _add_daily_coordinates(frame)

    extrema = _compute_band_features(frame, "extrema20")
    swing = _compute_band_features(frame, "swing10")

    assert pd.isna(extrema.loc[9, "support_ref_extrema20"])
    assert pd.notna(swing.loc[8, "support_ref_swing10"])
    assert extrema.loc[11, "support_ref_extrema20"] < swing.loc[11, "support_ref_swing10"]
    assert "support_hold_extrema20" in extrema.columns
    assert "support_hold_swing10" in swing.columns


def test_state_label_series_prefers_signal_strength_over_watch() -> None:
    frame = pd.DataFrame(
        [
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-01"),
                "week_slope": "up",
                "week_support_hold": True,
                "week_climactic": False,
                "climactic_day": False,
                "support_break_day": False,
                "ma_relation": "20_below_60",
                "ma60_state": "flat",
                "dist_ma20": 0.01,
                "bullish_reversal_2": True,
                "bullish_exhaustion_2": False,
                "bearish_reversal_2": False,
                "bearish_exhaustion_2": False,
                "support_touch_extrema20": True,
                "resistance_touch_extrema20": False,
                "support_hold_extrema20": True,
                "reclaim_support_extrema20": False,
                "lose_support_extrema20": False,
                "reject_resistance_extrema20": False,
                "breakout_resistance_extrema20": False,
                "reclaim_breakout_extrema20": False,
                "band_zone_extrema20": "lower",
                "pattern_2_family": "bullish_reversal_2",
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-02"),
                "week_slope": "up",
                "week_support_hold": True,
                "week_climactic": False,
                "climactic_day": False,
                "support_break_day": False,
                "ma_relation": "20_below_60",
                "ma60_state": "flat",
                "dist_ma20": 0.02,
                "bullish_reversal_2": False,
                "bullish_exhaustion_2": False,
                "bearish_reversal_2": False,
                "bearish_exhaustion_2": False,
                "support_touch_extrema20": True,
                "resistance_touch_extrema20": False,
                "support_hold_extrema20": True,
                "reclaim_support_extrema20": False,
                "lose_support_extrema20": False,
                "reject_resistance_extrema20": False,
                "breakout_resistance_extrema20": False,
                "reclaim_breakout_extrema20": False,
                "band_zone_extrema20": "lower",
                "pattern_2_family": "neutral_2",
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-03"),
                "week_slope": "down",
                "week_support_hold": False,
                "week_climactic": False,
                "climactic_day": False,
                "support_break_day": False,
                "ma_relation": "20_above_60",
                "ma60_state": "down",
                "dist_ma20": 0.10,
                "bullish_reversal_2": False,
                "bullish_exhaustion_2": False,
                "bearish_reversal_2": True,
                "bearish_exhaustion_2": False,
                "support_touch_extrema20": False,
                "resistance_touch_extrema20": True,
                "support_hold_extrema20": False,
                "reclaim_support_extrema20": False,
                "lose_support_extrema20": False,
                "reject_resistance_extrema20": True,
                "breakout_resistance_extrema20": False,
                "reclaim_breakout_extrema20": False,
                "band_zone_extrema20": "upper",
                "pattern_2_family": "bearish_reversal_2",
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-04"),
                "week_slope": "down",
                "week_support_hold": False,
                "week_climactic": False,
                "climactic_day": False,
                "support_break_day": False,
                "ma_relation": "20_above_60",
                "ma60_state": "up",
                "dist_ma20": -0.08,
                "bullish_reversal_2": False,
                "bullish_exhaustion_2": False,
                "bearish_reversal_2": False,
                "bearish_exhaustion_2": False,
                "support_touch_extrema20": False,
                "resistance_touch_extrema20": False,
                "support_hold_extrema20": False,
                "reclaim_support_extrema20": False,
                "lose_support_extrema20": True,
                "reject_resistance_extrema20": False,
                "breakout_resistance_extrema20": False,
                "reclaim_breakout_extrema20": False,
                "band_zone_extrema20": "lower",
                "pattern_2_family": "neutral_2",
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-05"),
                "week_slope": "up",
                "week_support_hold": False,
                "week_climactic": False,
                "climactic_day": False,
                "support_break_day": False,
                "ma_relation": "20_above_60",
                "ma60_state": "up",
                "dist_ma20": 0.12,
                "bullish_reversal_2": True,
                "bullish_exhaustion_2": False,
                "bearish_reversal_2": False,
                "bearish_exhaustion_2": False,
                "support_touch_extrema20": False,
                "resistance_touch_extrema20": True,
                "support_hold_extrema20": False,
                "reclaim_support_extrema20": False,
                "lose_support_extrema20": False,
                "reject_resistance_extrema20": False,
                "breakout_resistance_extrema20": True,
                "reclaim_breakout_extrema20": False,
                "band_zone_extrema20": "breakout",
                "pattern_2_family": "bullish_reversal_2",
            },
        ]
    )

    actual = _state_label_series(frame, "extrema20")

    assert actual.loc[0, "long_state_label"] == "long_entry"
    assert actual.loc[1, "long_state_label"] == "long_hold"
    assert actual.loc[2, "short_state_label"] == "short_entry"
    assert actual.loc[3, "long_state_label"] == "long_exit"
    assert actual.loc[4, "short_state_label"] == "short_exit"
    assert actual.loc[2, "long_state_label"] == "long_takeprofit"
    assert actual.loc[3, "short_state_label"] == "short_takeprofit"


def test_signal_return_mode_maps_takeprofit_and_exit_to_opposite_side() -> None:
    assert _signal_return_mode("long_entry") == "long"
    assert _signal_return_mode("long_takeprofit") == "short"
    assert _signal_return_mode("short_entry") == "short"
    assert _signal_return_mode("short_exit") == "long"


def test_build_research_plan_prefers_long_entry_watch_and_hold() -> None:
    result = {
        "modes": {
            "extrema20": {
                "oos_combined": {
                    "available": True,
                    "long_entry_watch": {"mean20": 0.019, "pf20": 1.7},
                    "long_hold": {"mean20": 0.018, "pf20": 1.6},
                    "long_takeprofit": {"mean20": -0.004, "pf20": 0.9},
                    "long_exit": {"mean20": -0.012, "pf20": 0.7},
                }
            },
            "swing10": {
                "oos_combined": {
                    "available": True,
                    "long_entry_watch": {"mean20": 0.015, "pf20": 1.5},
                    "long_hold": {"mean20": 0.013, "pf20": 1.4},
                    "long_takeprofit": {"mean20": -0.003, "pf20": 0.95},
                    "long_exit": {"mean20": -0.011, "pf20": 0.72},
                }
            },
        }
    }

    plan = _build_research_plan(result)

    assert plan["current_read"]["long_first"] is True
    assert plan["current_read"]["entry_state_policy"].startswith("entry_watch")
    assert plan["current_read"]["long_edge_stable"] is True
    assert plan["current_read"]["takeprofit_exit_negative"] is True
    assert plan["final_destination"]["phase_1"] == "analysis-only の state engine を固定する"
    assert plan["next_tasks"][0]["task_key"] == "long_state_transition"
    assert plan["next_tasks"][1]["task_key"] == "band_sensitivity"

    rendered = _render_markdown({"meta": {}, "comparison": {}, "modes": {}, "research_plan": plan})
    assert "## Research Plan" in rendered
    assert "### Final Destination" in rendered


def test_transition_focus_summary_includes_long_state_chain() -> None:
    frame = pd.DataFrame(
        [
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-01"),
                "long_state_label": "long_entry_watch",
                "signal_ret_20d": 0.010,
                "signal_ret_close_20d": 0.012,
                "signal_mae_20d": 0.020,
                "signal_mfe_20d": 0.040,
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-02"),
                "long_state_label": "long_hold",
                "signal_ret_20d": 0.030,
                "signal_ret_close_20d": 0.031,
                "signal_mae_20d": 0.010,
                "signal_mfe_20d": 0.050,
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-03"),
                "long_state_label": "long_takeprofit",
                "signal_ret_20d": -0.020,
                "signal_ret_close_20d": -0.021,
                "signal_mae_20d": 0.030,
                "signal_mfe_20d": 0.010,
            },
            {
                "code": "1001",
                "dt": pd.Timestamp("2025-01-04"),
                "long_state_label": "long_exit",
                "signal_ret_20d": -0.040,
                "signal_ret_close_20d": -0.039,
                "signal_mae_20d": 0.050,
                "signal_mfe_20d": 0.008,
            },
            {
                "code": "1002",
                "dt": pd.Timestamp("2025-01-01"),
                "long_state_label": "long_entry_watch",
                "signal_ret_20d": 0.000,
                "signal_ret_close_20d": 0.001,
                "signal_mae_20d": 0.015,
                "signal_mfe_20d": 0.020,
            },
            {
                "code": "1002",
                "dt": pd.Timestamp("2025-01-02"),
                "long_state_label": "long_exit",
                "signal_ret_20d": 0.000,
                "signal_ret_close_20d": 0.001,
                "signal_mae_20d": 0.012,
                "signal_mfe_20d": 0.015,
            },
            {
                "code": "1003",
                "dt": pd.Timestamp("2025-01-01"),
                "long_state_label": "long_hold",
                "signal_ret_20d": 0.000,
                "signal_ret_close_20d": 0.001,
                "signal_mae_20d": 0.018,
                "signal_mfe_20d": 0.020,
            },
            {
                "code": "1003",
                "dt": pd.Timestamp("2025-01-02"),
                "long_state_label": "long_exit",
                "signal_ret_20d": 0.000,
                "signal_ret_close_20d": 0.001,
                "signal_mae_20d": 0.014,
                "signal_mfe_20d": 0.016,
            },
        ]
    )

    rows = _transition_focus_summary(frame, min_samples=1)
    labels = {(row["from"], row["to"]): row for row in rows}

    assert ("long_entry_watch", "long_hold") in labels
    assert ("long_hold", "long_takeprofit") in labels
    assert labels[("long_entry_watch", "long_hold")]["delta_mean20"] > 0.0
    assert labels[("long_hold", "long_takeprofit")]["delta_mean20"] > 0.0
    assert labels[("long_hold", "long_exit")]["delta_mean20"] < 0.0
