from __future__ import annotations

import pandas as pd

from scripts.tradex_side_aware_top20pct_label_modeling_feasibility_v1 import (
    TARGET_NAME,
    _build_side_aware_top20pct_target,
    _split_months,
)


def test_side_aware_top20pct_label_tie_breaking() -> None:
    frame = pd.DataFrame(
        {
            "candidate_idx": [0, 1, 2, 3, 4],
            "anchor_date": ["2026-01-01"] * 5,
            "side": ["long"] * 5,
            "forward_ret_20d": [0.2, 0.2, 0.2, 0.1, -0.1],
            "path_value_score_v1": [0.1, 0.2, 0.2, 0.0, 0.0],
            "mae_20d": [0.5, 0.4, 0.3, 0.2, 0.1],
            "top15_label": [False] * 5,
            "bottom15_label": [False] * 5,
        }
    )
    out = _build_side_aware_top20pct_target(frame)
    assert TARGET_NAME in out.columns
    assert out[TARGET_NAME].sum() == 1
    assert bool(out.loc[2, TARGET_NAME]) is True
    assert out.loc[2, "label_rank"] == 1
    assert out.loc[2, "label_cutoff"] == 1
    assert out.loc[2, "target_sign"] == "positive"


def test_month_split_chronology() -> None:
    months = [f"2026-{m:02d}" for m in range(1, 11)]
    split = _split_months(months)
    assert split["train"]
    assert split["validation"]
    assert split["test"]
    assert max(split["train"]) < min(split["validation"])
    assert max(split["validation"]) < min(split["test"])
