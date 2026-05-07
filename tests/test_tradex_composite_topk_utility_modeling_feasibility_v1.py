from __future__ import annotations

import pandas as pd

from scripts.tradex_composite_topk_utility_modeling_feasibility_v1 import _build_composite_target, _split_months


def test_composite_target_formula_signs() -> None:
    frame = pd.DataFrame(
        {
            "anchor_date": ["2026-01-01", "2026-01-01", "2026-01-01", "2026-01-01"],
            "side": ["long", "long", "long", "long"],
            "forward_ret_20d": [0.1, 0.2, -0.1, -0.2],
            "path_value_score_v1": [0.4, 0.3, 0.2, 0.1],
            "top15_label": [True, False, False, False],
            "bottom15_label": [False, True, False, False],
        }
    )
    out = _build_composite_target(frame)
    assert "target_composite_topk_utility_v1" in out.columns
    assert out.loc[0, "target_composite_topk_utility_v1"] > out.loc[1, "target_composite_topk_utility_v1"]
    assert out.loc[0, "target_sign"] == "positive"


def test_month_split_chronology() -> None:
    months = [f"2026-{m:02d}" for m in range(1, 11)]
    split = _split_months(months)
    assert split["train"]
    assert split["validation"]
    assert split["test"]
    assert max(split["train"]) < min(split["validation"])
    assert max(split["validation"]) < min(split["test"])
