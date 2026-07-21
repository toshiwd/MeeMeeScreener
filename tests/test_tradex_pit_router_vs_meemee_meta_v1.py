import pandas as pd
from scripts import tradex_pit_router_vs_meemee_meta_v1 as m
def test_minimum_ten_completed_events():
 e=pd.DataFrame({'exit_ymd':[20240101]*9,'signal_ymd':range(9),'code':[str(x) for x in range(9)],'trade_return_h10':[.08]*9});assert m.choose_lane(e,e,20240102,40) is None
def test_windows_fixed():assert m.WINDOWS==(40,60,120)
