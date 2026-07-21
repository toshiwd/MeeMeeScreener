from __future__ import annotations

import pandas as pd

from scripts.tradex_sideways_blind_review_sample_v2 import build_independent_sample
from scripts import tradex_sideways_blind_review_sample_v1 as base


def test_independent_sample_excludes_previous_codes() -> None:
    rows = []
    for year in base.YEARS:
        for group_index, state in enumerate(("positive", "near", "random")):
            for index in range(12):
                code = f"{year % 100:02d}{group_index}{index:02d}"
                rows.append({
                    "code": code, "ymd": year * 10000 + 101 + index, "year": year,
                    "direction_efficiency": 0.1 if state == "positive" else 0.21 if state == "near" else 0.8,
                    "slope_share": 0.1 if state != "random" else 0.8, "close_pos15": 0.5, "ret60": 0.0,
                    "sideways_state": state == "positive", "sideways_start": state == "positive",
                    "boundary_distance": 0.01 if state == "near" else 0.6,
                })
    candidates = pd.DataFrame(rows)
    previous = pd.DataFrame({"code": [rows[0]["code"]], "ymd": [rows[0]["ymd"]]})
    board, sealed = build_independent_sample(candidates, previous)
    assert len(board) == 120
    assert previous.code.iloc[0] not in set(sealed.code)
