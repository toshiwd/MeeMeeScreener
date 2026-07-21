import pandas as pd
from scripts.tradex_point_in_time_rolling_shape_priority_top3_v1 import monthly_models
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES

def _frame(n=400):
    rows=[]
    for i in range(n):
        day=20240001+i//2
        row={f:float(i%5) for f in FEATURES};row.update({'signal_ymd':day,'eligible_from_date':day+2,'side_return':.01 if i%3 else -.01,'split':'train'});rows.append(row)
    return pd.DataFrame(rows)

def test_same_day_and_not_embargoed_outcomes_are_excluded():
    frame=_frame();future={f:1. for f in FEATURES};future.update({'signal_ymd':20250101,'eligible_from_date':20250103,'side_return':99.,'split':'validation'});frame=pd.concat([frame,pd.DataFrame([future])],ignore_index=True);models,blocked=monthly_models(frame,list(range(20240001,20240202))+[20250101]);assert not blocked;spec=models['202501'];assert spec['latest_train_eligible_from_date']<=spec['first_signal_date'];assert spec['latest_train_signal_date']<spec['first_signal_date']

def test_insufficient_month_blocks_without_fallback():
    frame=_frame(50);row={f:1. for f in FEATURES};row.update({'signal_ymd':20250101,'eligible_from_date':20250120,'side_return':.01,'split':'validation'});frame=pd.concat([frame,pd.DataFrame([row])],ignore_index=True);_,blocked=monthly_models(frame,list(range(20240101,20250601)));assert blocked and blocked[0]['typed_reason']=='INSUFFICIENT_ROLLING_TRAIN_COVERAGE'
