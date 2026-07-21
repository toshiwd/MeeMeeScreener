import pandas as pd
from scripts import tradex_buy_cross_sectional_momentum_v1 as m
def test_variants_fixed():assert m.VARIANTS==('rel_ret20','close_ret60','blend')
def test_blend_is_cross_sectional_and_ranks_all():
 x=pd.DataFrame({'signal_ymd':[1,1],'code':['a','b'],'rel_ret20':[2.,1.],'close_ret60':[2.,1.],'high20_dist':[0.,0.]});z=m.score(x,'blend');assert len(z)==2 and z.sort_values('rank').iloc[0].code=='a'
