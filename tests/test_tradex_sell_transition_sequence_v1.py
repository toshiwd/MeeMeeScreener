from __future__ import annotations

import numpy as np
import pandas as pd

from scripts import tradex_sell_transition_sequence_v1 as m


def test_transition_requires_order_and_completes_on_first_redecline() -> None:
    dates = pd.bdate_range("2024-01-02", periods=24).strftime("%Y%m%d").astype(int)
    close = [110, 109, 108, 107, 106, 104, 103, 102, 101, 100, 101, 102, 104, 103, 102, 101, 100, 99, 98, 97, 96, 95, 94, 93]
    bars = pd.DataFrame({"signal_ymd": dates, "code": "1000", "o": close, "h": np.array(close)+1, "l": np.array(close)-1, "c": close})
    frame = pd.DataFrame({"signal_ymd": dates, "code": "1000", "ma20": 120.0, "close_ret2": pd.Series(close).pct_change(2).fillna(0),
                          "trade_return_h10": 0.0, "target_before_stop20": 0.0, "realized_mover20": 0.0})
    out = m.classify_transitions(frame, bars)
    completed = out[out.transition_complete]
    assert len(completed) >= 1
    row = completed.sort_values("signal_ymd").iloc[0]
    assert row.transition_start_ymd < row.transition_bounce_ymd < row.transition_failure_ymd < row.signal_ymd


def test_all_symbols_receive_daily_rank_without_candidate_filter() -> None:
    dates = pd.bdate_range("2024-01-02", periods=12).strftime("%Y%m%d").astype(int)
    frames, bars = [], []
    for code in ("1000", "2000"):
        frames.append(pd.DataFrame({"signal_ymd": dates, "code": code, "ma20": 120.0, "close_ret2": 0.0,
                                    "trade_return_h10": 0.0, "target_before_stop20": 0.0, "realized_mover20": 0.0}))
        bars.append(pd.DataFrame({"signal_ymd": dates, "code": code, "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.0}))
    out = m.classify_transitions(pd.concat(frames), pd.concat(bars))
    assert len(out) == 24
    assert out.groupby("signal_ymd").size().eq(2).all()
    assert out.groupby("signal_ymd")["rank"].nunique().eq(2).all()
