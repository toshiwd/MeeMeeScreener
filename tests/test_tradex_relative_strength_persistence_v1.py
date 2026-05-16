from __future__ import annotations

import pandas as pd

from scripts.tradex_relative_strength_persistence_v1 import _score_group


def test_score_group_prefers_persistent_relative_strength() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-05",
                "side": "long",
                "symbol": "A",
                "champion_rank": 2,
                "champion_score": 0.8,
                "forward_ret_20d": 0.0,
                "ret_5d": 0.01,
                "ret_10d": 0.02,
                "ret_20d": 0.03,
                "rel_ret_5d": 0.01,
                "rel_ret_10d": 0.02,
                "rel_ret_20d": 0.03,
                "rel_strength_persistence_ratio_20d": 0.55,
                "down_day_resilience_20d": 0.01,
                "max_relative_drawdown_20d": -0.04,
            },
            {
                "anchor_date": "2026-01-05",
                "side": "long",
                "symbol": "B",
                "champion_rank": 1,
                "champion_score": 0.9,
                "forward_ret_20d": 0.0,
                "ret_5d": 0.02,
                "ret_10d": 0.03,
                "ret_20d": 0.08,
                "rel_ret_5d": 0.02,
                "rel_ret_10d": 0.03,
                "rel_ret_20d": 0.08,
                "rel_strength_persistence_ratio_20d": 0.9,
                "down_day_resilience_20d": 0.08,
                "max_relative_drawdown_20d": -0.01,
            },
        ]
    )

    scored = _score_group(frame)

    assert scored.iloc[0]["symbol"] == "B"
    assert int(scored.iloc[0]["candidate_rank"]) == 1
    assert "relative_strength_score_v1" in scored.columns
