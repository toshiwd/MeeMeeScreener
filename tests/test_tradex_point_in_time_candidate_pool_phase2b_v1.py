from __future__ import annotations

import pandas as pd

from scripts.tradex_side_aware_min_pool_feasibility_v1 import _attach_point_in_time_candidate_pool_contract_fields


def test_attach_point_in_time_candidate_pool_contract_fields_preserves_score_and_marks_lineage() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-10",
                "symbol": "1001",
                "side": "long",
                "selected_by": "champion",
                "accepted": True,
                "include_in_broad_pool": False,
                "champion_score": 0.9,
                "champion_rank": 3,
            }
        ]
    )

    out = _attach_point_in_time_candidate_pool_contract_fields(
        frame,
        source_lineage={"resolved_raw_candidate_source": "G:/Tradex/source.json"},
    )

    assert out.loc[0, "as_of_date"] == "2026-01-10"
    assert out.loc[0, "candidate_date"] == "2026-01-10"
    assert out.loc[0, "feature_cutoff_date"] == "2026-01-10"
    assert out.loc[0, "champion_score"] == 0.9
    assert out.loc[0, "champion_rank"] == 3
    assert bool(out.loc[0, "top5_membership"]) is True
    assert bool(out.loc[0, "no_future_label_used"]) is True
    assert out.loc[0, "score_source"] == "champion_score_preserved_from_selection_ledger"
