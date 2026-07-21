import pandas as pd
from scripts import tradex_strict_pit_adaptive_buy_router_v1 as m
def test_stats_exclude_same_day_and_unexited():
 e=pd.DataFrame({'exit_ymd':[20240102,20240103,pd.NA],'signal_ymd':[1,2,3],'code':['a','b','c'],'trade_return_h10':[.08,-.05,.08]});pf,exp,n=m.lane_stats(e,20240103,40);assert n==1 and pf==99 and exp==.08
def test_priority_uses_only_completed_positive_lane():
 a=pd.DataFrame({'exit_ymd':[20240101],'signal_ymd':[1],'code':['a'],'trade_return_h10':[.08]});b=pd.DataFrame({'exit_ymd':[20240101],'signal_ymd':[1],'code':['b'],'trade_return_h10':[-.05]});p=m.priorities({'a':a,'b':b},20240102,40);assert p['a']>p['b']
def test_windows_are_predeclared():assert m.WINDOWS==(40,60,120)
