import numpy as np
import pandas as pd

from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES, fit_train_model, select


def _frame():
    rows = []
    for i in range(120):
        row = {feature: float(i % 7) for feature in FEATURES}
        row.update({"signal_ymd": 20240101 + i // 6, "code": str(1000 + i), "side": "buy" if i % 2 == 0 else "sell", "rank": i % 3 + 1, "split": "train", "side_return": 0.01 if i % 3 else -0.01})
        rows.append(row)
    return pd.DataFrame(rows)


def test_model_contract_is_fixed_and_train_only():
    model, medians = fit_train_model(_frame())
    assert model.max_depth == 2
    assert model.min_samples_leaf == 50
    assert set(medians) == set(FEATURES)


def test_missing_values_use_frozen_train_medians_and_top3_is_preserved():
    frame = _frame()
    model, medians = fit_train_model(frame)
    frame.loc[frame.index[:6], FEATURES] = np.nan
    _, selected = select(frame, model, medians)
    assert selected.groupby("signal_ymd").size().max() == 3
    assert len(selected) == frame.signal_ymd.nunique() * 3
