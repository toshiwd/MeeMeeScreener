import pandas as pd
from scripts import tradex_two_sided_cash_preserving_allocation_v1 as m
def histories():
 rows=[{'signal_ymd':20240101+i,'exit_ymd':20240101+i,'trade_return_h10':.1} for i in range(10)]
 b=pd.DataFrame(rows);s=pd.DataFrame([{**r,'trade_return_h10':-.1} for r in rows]);return b,s
def test_sell_only_weaker_weight_and_cash():
 b,s=histories();s=pd.concat([s,pd.DataFrame([{'signal_ymd':20240120,'exit_ymd':20240121,'trade_return_h10':.1}])],ignore_index=True)
 x=m.allocate_cash_preserving(b,s,.75);r=x[x.signal_ymd==20240120].iloc[0];assert r.sell_weight==.25 and r.cash_weight==.75
def test_buy_only_stronger_weight_and_cash():
 b,s=histories();b=pd.concat([b,pd.DataFrame([{'signal_ymd':20240120,'exit_ymd':20240121,'trade_return_h10':.1}])],ignore_index=True)
 x=m.allocate_cash_preserving(b,s,.75);r=x[x.signal_ymd==20240120].iloc[0];assert r.buy_weight==.75 and r.cash_weight==.25
def test_insufficient_single_half_cash():
 b=pd.DataFrame([{'signal_ymd':20240101,'exit_ymd':20240102,'trade_return_h10':.1}]);s=pd.DataFrame(columns=b.columns);r=m.allocate_cash_preserving(b,s,.9).iloc[0];assert r.buy_weight==.5 and r.cash_weight==.5
