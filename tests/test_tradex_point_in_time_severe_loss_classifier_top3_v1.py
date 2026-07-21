import numpy as np
import pandas as pd

from scripts.tradex_point_in_time_severe_loss_classifier_top3_v1 import fit_model, select
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES


def _training():
    rows = []
    for i in range(120):
        row = {f: float(i % 3) for f in FEATURES}; row.update({"split": "train", "side_return": -0.051 if i < 60 else -0.049, "signal_ymd": 20240101 + i, "side": "buy", "rank": 1, "code": str(i)}); rows.append(row)
    return pd.DataFrame(rows)


def test_severe_loss_boundary_is_inclusive_at_minus_five_percent():
    frame = _training(); frame.loc[0, "side_return"] = -0.05
    model, _ = fit_model(frame)
    assert list(model.classes_) == [0, 1]


class _EqualProbabilityModel:
    classes_ = np.array([0, 1])
    def predict_proba(self, x):
        return np.tile([0.8, 0.2], (len(x), 1))


def test_equal_probability_uses_fixed_interleave_tie_order():
    rows=[]
    for side in ("buy", "sell"):
        for rank in (1,2,3):
            row={f:0. for f in FEATURES};row.update({"signal_ymd":20250106,"side":side,"rank":rank,"code":f"{side}{rank}","split":"validation","side_return":0.});rows.append(row)
    _, selected=select(pd.DataFrame(rows),_EqualProbabilityModel(),{f:0. for f in FEATURES})
    assert selected.code.tolist()==["buy1","sell1","buy2"]
