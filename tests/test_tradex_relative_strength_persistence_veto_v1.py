from __future__ import annotations

import pandas as pd

from scripts.tradex_relative_strength_persistence_veto_v1 import _rank_group_veto


def test_veto_preserves_champion_order_and_demotes_only_weak_names() -> None:
    rows = []
    for rank in range(1, 21):
        rows.append(
            {
                "anchor_date": "2026-01-05",
                "side": "long",
                "symbol": f"S{rank:02d}",
                "champion_rank": rank,
                "champion_score": 1.0 - rank * 0.01,
                "relative_strength_score_v1": 1.0 - rank * 0.1,
                "rel_strength_persistence_ratio_20d": 0.60,
                "rel_ret_20d": 0.02,
                "max_relative_drawdown_20d": -0.01,
                "forward_ret_20d": 0.0,
            }
        )
    frame = pd.DataFrame(rows)
    frame.loc[frame["champion_rank"].eq(3), ["relative_strength_score_v1", "rel_strength_persistence_ratio_20d", "rel_ret_20d", "max_relative_drawdown_20d"]] = [
        -10.0,
        0.40,
        -0.02,
        -0.20,
    ]

    ranked = _rank_group_veto(frame)

    assert bool(ranked.loc[ranked["symbol"].eq("S03"), "weak_rs_veto_flag"].iloc[0])
    assert int(ranked.loc[ranked["symbol"].eq("S03"), "candidate_rank"].iloc[0]) > 5
    assert ranked.sort_values("candidate_rank").head(5)["symbol"].tolist() == ["S01", "S02", "S04", "S05", "S06"]
