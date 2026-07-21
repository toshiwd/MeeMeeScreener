import pandas as pd
from scripts import tradex_sell_climax_failure_current_contract_v1 as m
def test_priority_variants_fixed():assert m.VARIANTS==('volume_ratio','ret20','close_pos')
def test_nonhit_ranked():
 x=pd.DataFrame({'signal_ymd':[1,1],'code':['a','b'],'family_hit':[True,False],'volume_ratio':[3,4],'ret20':[.2,.1],'close_pos':[.3,.8]});z=m.score(x,'volume_ratio');assert len(z)==2 and z.sort_values('rank').iloc[0].code=='a'
