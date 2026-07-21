import pandas as pd
from scripts import tradex_two_sided_allocation_intensity_v1 as m
def test_predeclared_weights_and_window():assert m.WEIGHTS==(.60,.75,.90) and m.WINDOW==120
def test_insufficient_remains_half():
 b=pd.DataFrame({'signal_ymd':[20240105],'exit_ymd':[20240104],'trade_return_h10':[.1]});s=pd.DataFrame({'signal_ymd':[20240105],'exit_ymd':[20240104],'trade_return_h10':[-.1]});x=m.allocate(b,s,.9);assert x.iloc[0].buy_weight==.5
def test_single_side_full_weight():
 b=pd.DataFrame({'signal_ymd':[20240105],'exit_ymd':[20240104],'trade_return_h10':[.1]});s=pd.DataFrame(columns=b.columns);x=m.allocate(b,s,.6);assert x.iloc[0].buy_weight==1 and x.iloc[0].sell_weight==0
def test_sell_cost_contract_correction_only_changes_return():
 s=pd.DataFrame({'signal_ymd':[20240105],'exit_ymd':[20240106],'trade_return_h10':[.02],'code':['1']})
 x=m.correct_sell_cost_contract(s)
 assert x.trade_return_h10.iloc[0]==.021
 assert x[['signal_ymd','exit_ymd','code']].equals(s[['signal_ymd','exit_ymd','code']])
def test_calendar_metrics_equal_tail_count_for_different_active_days():
 cal=[20250101+i for i in range(20)]
 p=pd.Series([-.1,.2],index=[cal[0],cal[1]])
 b=pd.Series([-.05,.1,.1],index=[cal[2],cal[3],cal[4]])
 pm=m.calendar_aligned_metrics(p,cal,cal[0],cal[-1]);bm=m.calendar_aligned_metrics(b,cal,cal[0],cal[-1])
 assert pm['calendar_sessions']==bm['calendar_sessions']==20
 assert pm['tail_count']==bm['tail_count']==2
 assert pm['active_signal_days']==2 and bm['active_signal_days']==3
