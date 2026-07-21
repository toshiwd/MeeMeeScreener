import pandas as pd
from scripts import tradex_buy_dispersion_concentration_switch_v1 as m
def test_variants_and_direction_fixed():assert m.QUANTILES==(.4,.5,.6) and m.WIDTH==.08
def test_high_dispersion_routes_meemee():
 c=pd.DataFrame({'signal_ymd':[1,1],'code':['a','b'],'percentile':[1.,.5],'family_hit':[True,False]});r=pd.DataFrame({'signal_ymd':[1],'code':['b'],'baseline_rank':[1]});d=pd.DataFrame({'signal_ymd':[1],'rel_ret20_dispersion':[.2]});z=m.final_scores(c,r,d,.1);assert z.iloc[0].code=='b' and z.iloc[0].selected_lane=='meemee'
def test_low_dispersion_routes_contraction():
 c=pd.DataFrame({'signal_ymd':[1,1],'code':['a','b'],'percentile':[1.,.5],'family_hit':[True,False]});r=pd.DataFrame({'signal_ymd':[1],'code':['b'],'baseline_rank':[1]});d=pd.DataFrame({'signal_ymd':[1],'rel_ret20_dispersion':[.05]});z=m.final_scores(c,r,d,.1);assert z.iloc[0].code=='a' and z.iloc[0].selected_lane=='contraction'
