import pandas as pd
from scripts.tradex_meemee_shape_rerank_top3_v1 import top3,branch
def test_fixed_variant_order_and_ties():
 x=pd.DataFrame({'dt':[1]*5,'code':['1','2','3','4','5'],'rank':[1,2,3,4,5],'close_position':[.1,.2,.3,.9,.8]});z=top3(x,'close_position',False);assert list(z.code)==['4','5','3']
def test_branch_metrics():
 a=pd.DataFrame({'dt':[1]*3,'code':['1','2','3']});b=pd.DataFrame({'dt':[1]*3,'code':['1','2','4']});r=branch(a,b)[0];assert r['changed_members_count']==2 and r['changed_rank_count']==1 and r['jaccard']==.5
