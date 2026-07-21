import json,pandas as pd,pytest
from pathlib import Path
from scripts import tradex_two_sided_sell_only_exposure_cap_completion_v1 as m
def test_source_rank_coverage_is_615_days():
 b=pd.read_parquet(m.FULL_BUY,columns=['signal_ymd']);s=pd.read_parquet(m.SELL_RANK,columns=['signal_ymd']);assert b.signal_ymd.nunique()==s.signal_ymd.nunique()==615
def test_top10_preservation():
 f=pd.DataFrame({'signal_ymd':[1]*3,'code':['A','B','C'],'router_score':[3,2,1],'rank':[1,2,3]});mm=pd.DataFrame({'signal_ymd':[1,1],'code':['C','A'],'rank':[1,2]});x=m.build_buy_ranks(f,mm);assert x.head(2).code.tolist()==['C','A'] and x.head(2)['rank'].tolist()==[1,2]
def test_hash_source_list_is_complete():
 labels=set(m.source_paths(Path('db')).keys());assert labels=={'runtime_db','buy_ledger','sell_ledger','original_meemee_buy_rank','original_sell_rank','full_buy_score','upstream_p60_compare','base_compare','completion_script','completion_test'}
def test_cap_assertion_matches_upstream_and_rejects_mismatch():
 up=json.loads(m.UPSTREAM_P60.read_text(encoding='utf-8'));m.assert_cap(up);up['selected_variant']['raw_volume_cap']=1
 with pytest.raises(AssertionError):m.assert_cap(up)
def test_20260224_dense_top10_is_ten():
 f=pd.read_parquet(m.FULL_BUY);mm=pd.read_parquet(m.MEEMEE_RANK);x=m.build_buy_ranks(f,mm);q=x[x.signal_ymd.eq(20260224)];assert len(q)>=10 and q.top10.sum()==10 and q['rank'].tolist()==list(range(1,len(q)+1))
