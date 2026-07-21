from __future__ import annotations

import pandas as pd

from scripts import tradex_sideways_blind_review_sample_v1 as subject


def test_year_quotas_sum_and_spread() -> None:
    all_quotas = [subject.year_quotas(40, group) for group in subject.GROUP_COUNTS]
    assert all(sum(quotas.values()) == 40 for quotas in all_quotas)
    assert all(max(quotas.values()) - min(quotas.values()) <= 1 for quotas in all_quotas)
    assert all(sum(quotas[year] for quotas in all_quotas) == 20 for year in subject.YEARS)


def test_build_sample_hides_machine_columns_and_uses_unique_codes() -> None:
    rows = []
    for year in subject.YEARS:
        for group_index, state in enumerate(("positive", "near", "random")):
            for index in range(10):
                code = f"{year % 100:02d}{group_index}{index}"
                rows.append({
                    "code": code, "ymd": year * 10000 + 101 + index, "year": year,
                    "direction_efficiency": 0.1 if state == "positive" else 0.21 if state == "near" else 0.8,
                    "slope_share": 0.1 if state != "random" else 0.8,
                    "close_pos15": 0.5, "ret60": 0.0,
                    "sideways_state": state == "positive", "sideways_start": state == "positive",
                    "boundary_distance": 0.01 if state == "near" else 0.6,
                })
    board, sealed = subject.build_sample(pd.DataFrame(rows))
    assert len(board) == 120
    assert board.code.nunique() == 120
    assert "sample_group" not in board.columns
    assert sealed.sample_group.value_counts().to_dict() == subject.GROUP_COUNTS
