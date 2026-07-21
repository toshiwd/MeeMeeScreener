import json
from pathlib import Path
import pandas as pd
import scripts.tradex_leaf_cap4_slot24_2026_matured_oos_v1 as m
def test_2026_only(monkeypatch,tmp_path:Path):
 s=tmp_path/'s.csv';pd.DataFrame([{'signal_year':2025,'shape_leaf':9,'horizon_date':2},{'signal_year':2026,'shape_leaf':20,'horizon_date':3}]).to_csv(s,index=False)
 c=tmp_path/'c.json';c.write_text(json.dumps({'fixed_evaluation_conditions':{'source_db':str(s),'take_profit':.08,'stop_loss':.05,'max_holding_days':10},'train_only_selected_gap_execution':{'selected_maximum_next_open_gap':0.0}}))
 def fake(x,slip):return x.assign(next_open_return=[.1,-.05],next_entry_date=[1,2],pnl_yen=[10,-5]),{'accepted_trade_count':2}
 monkeypatch.setattr(m,'candidates',lambda db,gap:pd.read_csv(s));monkeypatch.setattr(m,'replay',fake);p=m.generate(c,tmp_path/'o',s);d=json.loads(p.read_text());assert d['metrics']['n']==1 and d['metrics']['pnl_yen']==-5 and d['frozen_contract']['maximum_next_open_gap']==0.0
def test_contract_guard(tmp_path:Path):
 s=tmp_path/'s';s.write_text('x');c=tmp_path/'c';c.write_text(json.dumps({'fixed_evaluation_conditions':{'source_db':str(s),'take_profit':.07,'stop_loss':.05,'max_holding_days':10},'train_only_selected_gap_execution':{'selected_maximum_next_open_gap':0.0}}))
 try:m.generate(c,tmp_path/'o')
 except ValueError as e:assert str(e)=='FROZEN_CONTRACT_MISMATCH'
 else:assert False
