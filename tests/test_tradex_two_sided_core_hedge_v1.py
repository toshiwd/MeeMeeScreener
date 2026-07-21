import pandas as pd
from scripts import tradex_two_sided_core_hedge_v1 as m
def frame(days,rets):return pd.DataFrame({'signal_ymd':days,'trade_return_h10':rets})
def test_buy_only_is_full_core():
 x=m.allocate_core_hedge(frame([1],[.1]),frame([],[]),.2).iloc[0];assert x.buy_weight==1 and x.cash_weight==0
def test_sell_only_is_partial_hedge():
 x=m.allocate_core_hedge(frame([],[]),frame([1],[.1]),.2).iloc[0];assert x.sell_weight==.2 and x.cash_weight==.8
def test_both_are_one_minus_h_and_h():
 x=m.allocate_core_hedge(frame([1],[.1]),frame([1],[-.1]),.3).iloc[0];assert x.buy_weight==.7 and x.sell_weight==.3 and x.cash_weight==0
