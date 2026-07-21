import pandas as pd
from scripts import tradex_ma200_weekly_reversal_current_exit_v1 as m
def fixture():
 d=pd.bdate_range('2023-01-02',periods=230).strftime('%Y%m%d').astype(int);c=[100+i*.005 for i in range(230)];b=pd.DataFrame({'signal_ymd':d,'code':'1','o':c,'h':[x+.4 for x in c],'l':[x-.1 for x in c],'c':[x+.3 for x in c],'v':1000.});f=b[['signal_ymd','code']].copy();return f,b
def test_ma200_shape_is_pit_and_fixed():
 f,b=fixture();x=m.build_frame(f,b);assert x.ma200.iloc[:199].isna().all();assert x.family_hit.any()
def test_nonmatches_remain_ranked():
 x=pd.DataFrame({'signal_ymd':[20240101]*2,'code':['1','2'],'family_hit':[True,False],'support_distance':[.01,.02],'ma200_slope20':[.1,.0],'close_position':[.8,.2]});y=m.score(x,'support_distance');assert len(y)==2 and y['rank'].max()==2 and y.sort_values('rank').iloc[0].code=='1'
def test_variants_are_bounded_to_three():assert m.VARIANTS==('support_distance','ma200_slope20','close_position')
