from __future__ import annotations

import numpy as np
import pandas as pd

from scripts import tradex_early_signal_first_detect_v1 as m


def _features(n: int = 500) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=n)
    x = np.linspace(-1, 1, n)
    frame = pd.DataFrame({
        "signal_ymd": dates.strftime("%Y%m%d").astype(int), "code": "1000", "close": 100 + x,
        "ma20": 100 + x * .5, "ma60": 100 - x * .2,
        "weekly_breakout_up_prob": .4 + x * .1, "weekly_breakout_down_prob": .4 - x * .1,
        "weekly_range_prob": .2, "monthly_breakout_up_prob": .4 + x * .1,
        "monthly_breakout_down_prob": .4 - x * .1, "monthly_range_prob": .2,
        "candle_triplet_up_prob": .5 + x * .1, "candle_triplet_down_prob": .5 - x * .1,
        "candle_body_ratio": .5, "candle_upper_wick_ratio": .2, "candle_lower_wick_ratio": .3,
        "close_ret2": x * .01, "close_ret3": x * .02, "close_ret20": x * .04,
        "close_ret60": x * .08, "atr14_pct": .03, "range_pct": .04, "gap_pct": x * .01,
        "vol_ratio5_20": 1 + x * .1, "turnover_z20": x, "high20_dist": -.05 + x * .02,
        "low20_dist": .05 + x * .02, "rel_ret20": x * .03,
    })
    return frame


def test_side_mapping_uses_down_probability_and_past_only_delta() -> None:
    raw = _features(12)
    buy = m.attach_past_features(raw, "BUY")
    sell = m.attach_past_features(raw, "SELL")
    assert buy.weekly_direction_prob.equals(raw.weekly_breakout_up_prob)
    assert sell.weekly_direction_prob.equals(raw.weekly_breakout_down_prob)
    assert np.allclose(sell.close_ret20, -raw.close_ret20)
    assert np.isclose(buy.iloc[5].high_block_delta_5d, raw.iloc[5].high20_dist - raw.iloc[0].high20_dist)


def test_outcome_is_stop_first_and_same_bar_both_is_loss() -> None:
    features = m.attach_past_features(_features(22), "BUY")
    dates = features.signal_ymd.tolist()
    bars = pd.DataFrame([{"code": "1000", "signal_ymd": d, "o": 100, "h": 101, "l": 99, "c": 100} for d in dates])
    bars.loc[1, ["h", "l", "c"]] = [109, 94, 102]
    out = m.attach_outcomes(features, bars, "BUY")
    row = out[out.signal_ymd == dates[0]].iloc[0]
    assert row.target_before_stop20 == 0
    assert row.realized_mover20 == 1
    assert np.isclose(row.trade_return_h10, -.051)


def test_first_detect_uses_frozen_threshold_and_checkpoints() -> None:
    dates = pd.bdate_range("2024-01-02", periods=70).strftime("%Y%m%d").astype(int)
    scored = pd.DataFrame({"signal_ymd": dates, "code": "1000", "side": "BUY", "score": .1,
                           "rank": 50, "percentile": .5, "top10": False, "realized_mover20": 0})
    scored.loc[8, ["score", "rank", "percentile", "top10"]] = [.8, 8, .92, True]
    scored.loc[68, "realized_mover20"] = 1
    audit = m.build_miss_audit(scored, .7)
    assert int(audit.iloc[0].first_detect_ymd) == int(dates[8])
    assert audit.iloc[0].first_detect_lead_sessions == 60
    assert audit.iloc[0].miss_reason == "detected_early"
    assert int(audit.iloc[0].d5_ymd) == int(dates[63])
