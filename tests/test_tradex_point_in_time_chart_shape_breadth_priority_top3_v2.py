import pandas as pd

from scripts.tradex_point_in_time_chart_shape_breadth_priority_top3_v2 import FEATURES, fit_v2
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES as V1_FEATURES


def test_v2_adds_exactly_one_feature():
    assert FEATURES[:-1] == V1_FEATURES
    assert FEATURES[-1] == "breadth_above_ma20"


def test_v2_model_contract_remains_depth2_leaf50():
    rows = []
    for i in range(120):
        row = {feature: float(i % 9) for feature in FEATURES}
        row.update({"split": "train", "side_return": 0.01 if i % 3 else -0.01})
        rows.append(row)
    model, medians = fit_v2(pd.DataFrame(rows))
    assert model.max_depth == 2
    assert model.min_samples_leaf == 50
    assert set(medians) == set(FEATURES)
