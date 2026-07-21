import pandas as pd
from scripts.tradex_pinned_adaptive_vs_meemee_2026_v1 import metric
def test_calendar_includes_empty_days():
 x=pd.DataFrame({'pnl_yen':[100.],'invested_yen':[1000.],'ymd':[20260101],'exit_date':[2]});m=metric(x,[20260101,20260102]);assert m['expectancy']==.1 and m['calendar_expectancy']==.05
def test_tail_and_payoff():
 x=pd.DataFrame({'pnl_yen':[100.,-50.],'invested_yen':[1000.,1000.],'ymd':[20260101,20260102],'exit_date':[2,3]});m=metric(x,[20260101,20260102]);assert m['profit_factor']==2 and m['payoff_ratio']==2 and m['p05']<0
