import pandas as pd
from scripts import tradex_meemee_buy_selection_breadth_k_v1 as m
def test_predeclared_k_only():assert m.KS==(3,5,10)
def test_event_and_calendar_metrics_are_both_present():
 e=pd.DataFrame({'signal_ymd':[20240104,20240104,20240105],'trade_return_h10':[.1,-.05,.02],'target_before_stop20':[1,0,0],'realized_mover20':[1,0,0]});x=m.complete_metrics(e,2,e);assert 'event_profit_factor' in x and 'profit_factor' in x and 'event_expectancy' in x
def test_current_exit_is_not_an_axis():assert 'EXIT' not in vars(m)
