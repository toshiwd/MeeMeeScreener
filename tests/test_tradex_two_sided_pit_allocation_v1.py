import pandas as pd
from scripts import tradex_two_sided_pit_allocation_v1 as m
def test_windows_fixed():assert m.WINDOWS==(40,60,120)
def test_exit_must_be_strictly_before_day():
 e=pd.DataFrame({'exit_ymd':[20240105,20240106],'signal_ymd':[20240101,20240102],'trade_return_h10':[.1,-.1]});pf,ex,n=m.side_stats(e,20240106,40);assert n==1 and ex==.1
def test_insufficient_both_sides_is_half():
 b=pd.DataFrame({'signal_ymd':[20240105],'exit_ymd':[20240104],'trade_return_h10':[.1]});s=pd.DataFrame({'signal_ymd':[20240105],'exit_ymd':[20240104],'trade_return_h10':[-.1]});_,x=m.allocate(b,s,40);assert x.iloc[0].buy_weight==.5 and x.iloc[0].sell_weight==.5
