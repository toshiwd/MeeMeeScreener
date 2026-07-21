import pandas as pd
from scripts import tradex_two_sided_sell_only_exposure_cap_v1 as m
def f(ds,rs):return pd.DataFrame({'signal_ymd':ds,'trade_return_h10':rs})
def test_sell_only_cap_and_cash():
 r=m.allocate(f([],[]),f([1],[.1]),.5).iloc[0];assert r.sell_weight==.5 and r.cash_weight==.5
def test_buy_only_full():
 r=m.allocate(f([1],[.1]),f([],[]),.25).iloc[0];assert r.buy_weight==1 and r.cash_weight==0
def test_both_fixed_roles_independent_of_cap():
 for c in m.CAPS:
  r=m.allocate(f([1],[.1]),f([1],[-.1]),c).iloc[0];assert r.buy_weight==.9 and r.sell_weight==.1
