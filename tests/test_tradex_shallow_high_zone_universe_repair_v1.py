import numpy as np
import pandas as pd
from scripts import tradex_shallow_high_zone_universe_repair_v1 as m

def test_gap_through_stop_uses_actual_open():
 d=pd.bdate_range('2024-01-02',periods=12).strftime('%Y%m%d').astype(int);b=pd.DataFrame({'signal_ymd':d,'code':'1','o':100.,'h':101.,'l':99.,'c':100.,'v':1000.});b.loc[2,'o']=90.;f=pd.DataFrame({'signal_ymd':[d[0]],'code':['1']});x=m.attach_gap_outcomes(f,b);assert np.isclose(x.trade_return_h10.iloc[0],-.101)
def test_all_symbols_ranked_even_nonmatches():
 x=pd.DataFrame({'signal_ymd':[20240101]*2,'code':['1','2'],'shape_leaf':[9,0],'gap_ma60':[.01,-.1]});y=m.score(x,(9,),(9,));assert len(y)==2 and y['rank'].max()==2 and y.sort_values('rank').iloc[0].code=='1'
def test_no_reentry_uses_exit_sessions():
 e=pd.DataFrame({'signal_ymd':[20240101,20240102,20240103],'code':['1']*3,'rank':[1]*3,'exit_day_h10':[2,1,1]});x=m.no_reentry_sessions(e,[20240101,20240102,20240103],'rank');assert x.signal_ymd.tolist()==[20240101]
