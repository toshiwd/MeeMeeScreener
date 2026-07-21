import numpy as np
import pandas as pd

from scripts.tradex_point_in_time_severe_loss_leaf_expectancy_top3_v1 import select
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES


class _Loss:
    classes_ = np.array([0, 1])
    def predict_proba(self, x):
        return np.column_stack([1 - x[FEATURES[0]].to_numpy(), x[FEATURES[0]].to_numpy()])


class _Regression:
    def predict(self, x): return x[FEATURES[1]].to_numpy()


def test_expectancy_only_reorders_inside_equal_loss_probability():
    rows=[]
    for i,(loss,exp) in enumerate(((.1,.1),(.1,.9),(.2,99),(.2,98))):
        row={f:0. for f in FEATURES};row[FEATURES[0]]=loss;row[FEATURES[1]]=exp;row.update({"signal_ymd":20250106,"side":"buy" if i%2==0 else "sell","rank":i//2+1,"code":str(i),"split":"validation","side_return":0.});rows.append(row)
    _, selected=select(pd.DataFrame(rows),_Loss(),{f:0. for f in FEATURES},_Regression(),{f:0. for f in FEATURES})
    assert selected.code.tolist()==["1","0","2"]


def test_fixed_interleave_is_final_tie_break():
    rows=[]
    for side in ("buy","sell"):
        for rank in (1,2,3):
            row={f:0. for f in FEATURES};row.update({"signal_ymd":20250106,"side":side,"rank":rank,"code":f"{side}{rank}","split":"validation","side_return":0.});rows.append(row)
    _, selected=select(pd.DataFrame(rows),_Loss(),{f:0. for f in FEATURES},_Regression(),{f:0. for f in FEATURES})
    assert selected.code.tolist()==["buy1","sell1","buy2"]
