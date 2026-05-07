from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.tradex_iizuka_fixed_contract_forward_surface_accumulation_v1 import (
    _build_shape_modifier_map,
    _decision_from_result,
    _signal_rank_fields,
)


def test_signal_rank_fields_parse_entry_score_and_source_rank() -> None:
    row = pd.Series(
        {
            "score_snapshot_json": '{"entryScore":0.75,"tradePriorityScore":0.9}',
            "rank_snapshot_json": '{"sourceRank":17,"finalRank":3}',
        }
    )
    entry_score, source_rank = _signal_rank_fields(row)
    assert entry_score == 0.75
    assert source_rank == 17.0


def test_shape_modifier_map_contains_known_modifier() -> None:
    shape_session = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
    mapping = _build_shape_modifier_map(shape_session)
    assert mapping["gap_down_bear"] == "shape_positive_modifier"


def test_decision_defaults_to_hold_when_metrics_are_mixed() -> None:
    comparison = {
        "per_k": [
            {
                "top_k": 5,
                "champion": {"mean_forward_ret_20d": 0.01, "bottom15_contamination_rate": 0.05},
                "challenger": {"mean_forward_ret_20d": 0.009, "bottom15_contamination_rate": 0.07},
            },
            {
                "top_k": 10,
                "champion": {"mean_forward_ret_20d": 0.01, "bottom15_contamination_rate": 0.05},
                "challenger": {"mean_forward_ret_20d": 0.011, "bottom15_contamination_rate": 0.08},
            },
            {
                "top_k": 20,
                "champion": {"mean_forward_ret_20d": 0.01, "bottom15_contamination_rate": 0.05},
                "challenger": {"mean_forward_ret_20d": 0.011, "bottom15_contamination_rate": 0.08},
            },
        ]
    }
    candidate = pd.DataFrame({"anchor_date": ["2026-01-20", "2026-01-20"], "symbol": ["0001", "0002"]})
    no_lookahead = {"no_lookahead_pass": True}
    decision = _decision_from_result(comparison, no_lookahead, candidate)
    assert decision["decision"] == "needs_iizuka_contract_redesign"
