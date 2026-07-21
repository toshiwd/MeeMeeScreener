import pandas as pd
import pytest
from scripts.tradex_point_in_time_side_specific_shape_priority_top3_v1 import fit_side_models
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES

def test_thin_side_blocks_all_models():
    rows=[]
    for i in range(150):
        row={f:float(i%5) for f in FEATURES};row.update({'split':'train','side':'buy' if i<120 else 'sell','side_return':.01});rows.append(row)
    with pytest.raises(ValueError,match='INSUFFICIENT_SIDE_TRAIN_COVERAGE'):fit_side_models(pd.DataFrame(rows))

def test_both_sides_use_same_fixed_tree_contract():
    rows=[]
    for side in ('buy','sell'):
        for i in range(110):
            row={f:float(i%7) for f in FEATURES};row.update({'split':'train','side':side,'side_return':.01 if i%3 else -.01});rows.append(row)
    models,_,counts=fit_side_models(pd.DataFrame(rows));assert counts=={'buy':110,'sell':110};assert all(m.max_depth==2 and m.min_samples_leaf==50 for m in models.values())
