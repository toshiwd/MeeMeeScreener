from __future__ import annotations

import numpy as np
import pandas as pd

from scripts import tradex_sideways_state_definition_v1 as subject


def _bars(closes: list[float], code: str = "1000") -> pd.DataFrame:
    close = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "signal_ymd": np.arange(20250101, 20250101 + len(close)),
            "code": code,
            "o": close,
            "h": close + 0.5,
            "l": close - 0.5,
            "c": close,
            "v": 1000.0,
        }
    )


def test_direction_efficiency_separates_sideways_from_trend_without_box_touches() -> None:
    sideways = [100, 102, 99, 101, 98, 102, 100, 99, 101, 100, 102, 99, 101, 100, 100]
    trend = list(np.linspace(100, 114, 15))
    frame = pd.concat([_bars(sideways, "flat"), _bars(trend, "trend")], ignore_index=True)
    observables = subject.build_observables(frame)
    marked = subject.mark_sideways(observables, 0.30)
    flat = marked[marked.code == "flat"].iloc[-1]
    rising = marked[marked.code == "trend"].iloc[-1]
    assert flat.direction_efficiency <= 0.30
    assert bool(flat.sideways_state)
    assert rising.direction_efficiency > 0.90
    assert not bool(rising.sideways_state)


def test_sideways_start_is_only_first_day_of_each_state_run() -> None:
    closes = [100, 102, 99, 101, 98, 102, 100, 99, 101, 100, 102, 99, 101, 100, 100, 100]
    marked = subject.mark_sideways(subject.build_observables(_bars(closes)), 0.40)
    starts = marked[marked.sideways_start]
    assert len(starts) == 1
    assert bool(marked.iloc[-1].sideways_state)


def test_forward_label_records_first_realized_expansion_direction() -> None:
    base = [100, 102, 99, 101, 98, 102, 100, 99, 101, 100, 102, 99, 101, 100, 100]
    future = [101, 102, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123]
    marked = subject.mark_sideways(subject.build_observables(_bars(base + future)), 0.40)
    labeled = subject.attach_forward_labels(marked)
    event = labeled[labeled.sideways_start].iloc[0]
    assert event.realized_direction_20 == "up"
    assert int(event.first_up_expansion_day) >= 1
    assert bool(event.expansion_20)


def test_deduplicate_events_enforces_trading_session_gap_per_code() -> None:
    events = pd.DataFrame(
        {
            "code": ["1000", "1000", "1000", "2000"],
            "signal_ymd": [20250101, 20250102, 20250201, 20250102],
            "bar_index": [10, 25, 30, 11],
        }
    )
    selected = subject.deduplicate_events(events, min_gap=20)
    assert selected[selected.code == "1000"].bar_index.tolist() == [10, 30]
    assert selected[selected.code == "2000"].bar_index.tolist() == [11]


def test_control_comparison_reports_expansion_uplift() -> None:
    events = pd.DataFrame(
        {
            "signal_ymd": [20250101, 20250102],
            "up_atr_20": [3.0, 0.5], "down_atr_20": [0.5, 0.5],
            "expansion_5": [True, False], "expansion_10": [True, False], "expansion_20": [True, False],
        }
    )
    controls = pd.DataFrame(
        {
            "signal_ymd": [20250103, 20250104],
            "up_atr_20": [0.5, 0.5], "down_atr_20": [0.5, 0.5],
            "expansion_5": [False, False], "expansion_10": [False, False], "expansion_20": [False, False],
        }
    )
    result = subject.compare_with_controls(events, controls)
    assert result["expansion_rate_20_uplift"] == 0.5
    assert result["positive_uplift_year_count"] == 1


def test_first_identifiable_checkpoint_uses_first_passing_row() -> None:
    rows = [
        {"checkpoint": 0, "identifiability_gate_pass": False},
        {"checkpoint": 1, "identifiability_gate_pass": False},
        {"checkpoint": 2, "identifiability_gate_pass": True},
        {"checkpoint": 3, "identifiability_gate_pass": True},
    ]
    assert subject.first_identifiable_checkpoint(rows) == 2


def test_forward_horizon_requires_all_future_sessions() -> None:
    frame = subject.build_observables(_bars([100.0] * 35))
    labeled = subject.attach_forward_labels(frame)
    assert pd.notna(labeled.iloc[14].up_atr_20)
    assert pd.isna(labeled.iloc[15].up_atr_20)
    assert pd.isna(labeled.iloc[-1].expansion_20)
