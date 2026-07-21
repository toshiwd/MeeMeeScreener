import pandas as pd
from scripts.tradex_meemee_leaf_consensus_top3_v1 import select_lanes,stats
def test_consensus_branches_top3():
 r=pd.DataFrame({'code':['1','2','3','4','5'],'date':[10]*5,'dt':[20240101]*5,'rank':[1,2,3,4,5]});l=pd.DataFrame({'code':['5'],'date':[10]})
 b,c,_=select_lanes(r,l);assert list(b.code)==['1','2','3']and list(c.code)==['5','1','2']
def test_stats_money_return():
 x=pd.DataFrame({'pnl_yen':[100.,-50.],'invested_yen':[1000.,1000.],'exit_date':[2,3],'next_entry_date':[1,2]});m=stats(x);assert m['profit_factor']==2 and m['expectancy']==.025 and m['payoff_ratio']==2
