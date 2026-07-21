import pandas as pd
from scripts.tradex_breadth40_exclusive_side_router_audit_v1 import metrics,prepare
def test_side_adjusted_returns():
 b=pd.DataFrame({'dt':[20240101],'pnl_yen':[100.],'invested_yen':[1000.]});s=pd.DataFrame({'signal_ymd':[20240102],'ret':[.2],'breadth_below_ma20':[.5]});b,s=prepare(b,s);assert b.side_return.iloc[0]==.1 and s.side_return.iloc[0]==.2
def test_daily_equal_weight():
 x=pd.DataFrame({'signal_ymd':[20240101,20240101,20240102],'side_return':[.1,-.1,.2],'side':['buy','buy','sell']});m=metrics(x);assert m['n']==3 and m['signal_days']==2 and m['daily_equal_weight_expectancy']==.1 and m['buy_days']==1 and m['sell_days']==1
