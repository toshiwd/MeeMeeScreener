import pandas as pd
import pytest

from scripts.tradex_point_in_time_breadth_regime_shape_priority_top3_v1 import fit_regime_models, regime_name
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES


def test_regime_boundary_is_fixed():
    assert regime_name(0.3999) == "breadth_lt_0_40"
    assert regime_name(0.40) == "breadth_ge_0_40"


def test_any_thin_regime_blocks_all_models():
    rows = []
    for i in range(110):
        row = {feature: float(i % 5) for feature in FEATURES}
        row.update({"split": "train", "side_return": 0.01, "breadth_above_ma20": 0.3 if i < 99 else 0.6})
        rows.append(row)
    with pytest.raises(ValueError, match="INSUFFICIENT_REGIME_TRAIN_COVERAGE"):
        fit_regime_models(pd.DataFrame(rows))
