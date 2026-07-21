import pandas as pd
from scripts import tradex_two_sided_sell_volume_cap_v1 as m
def test_predeclared_caps_and_fixed_axis():assert m.QUANTILES==(.60,.70,.80)
def test_volume_relation_separates_cap():
 x=pd.DataFrame({'signal_ymd':[20240101]*4,'v':[10,20,30,40],'trade_return_h10':[.1,.1,-.1,-.1]})
 r=m.volume_relation(x,20240101,20241231,20)
 assert r['at_or_below_cap']['n']==2 and r['above_cap']['n']==2
def test_evaluate_same_calendar_and_tail():
 cal=[20250101+i for i in range(20)];x=pd.DataFrame({'signal_ymd':cal[:2],'portfolio_return':[-.1,.2]});b=pd.DataFrame({'signal_ymd':cal[2:5],'trade_return_h10':[-.05,.1,.1]})
 r=m.evaluate(x,b,cal,cal[0],cal[-1]);assert r['portfolio']['calendar_sessions']==r['buy_only']['calendar_sessions']==20 and r['portfolio']['tail_count']==r['buy_only']['tail_count']==2
