import pandas as pd
from scripts.tradex_leaf_vs_meemee_2026_same_condition_v1 import metric
def test_metric_uses_slippage_adjusted_money_return():
 x=pd.DataFrame({'pnl_yen':[100.,-50.],'invested_yen':[1000.,1000.],'next_entry_date':[1,2]});m=metric(x)
 assert m['n']==2 and m['expectancy']==.025 and m['profit_factor']==2 and m['pnl_yen']==50
def test_empty_metric_is_explicit():
 m=metric(pd.DataFrame(columns=['pnl_yen','invested_yen','next_entry_date']));assert m['n']==0 and m['expectancy'] is None
