import pandas as pd
from scripts import tradex_two_sided_portfolio_union_v1 as m
def test_both_sides_are_half_weighted():
 b=pd.DataFrame({'signal_ymd':[20240104],'trade_return_h10':[.1]});s=pd.DataFrame({'signal_ymd':[20240104],'trade_return_h10':[-.02]});x,d=m.portfolio_metrics(b,s,20240101,20241231);assert abs(x['calendar_expectancy']-.04)<1e-12 and x['both_side_days']==1
def test_single_side_is_full_weight():
 b=pd.DataFrame({'signal_ymd':[20240104],'trade_return_h10':[.1]});s=pd.DataFrame(columns=['signal_ymd','trade_return_h10']);x,d=m.portfolio_metrics(b,s,20240101,20241231);assert abs(x['calendar_expectancy']-.1)<1e-12
def test_no_weight_variants():assert not hasattr(m,'WEIGHTS')
