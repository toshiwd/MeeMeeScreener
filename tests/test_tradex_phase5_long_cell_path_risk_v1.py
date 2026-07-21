import json
from pathlib import Path
import pandas as pd
from scripts.tradex_phase5_long_cell_path_risk_v1 import analyze,run

def _source(p:Path)->Path:
 rows=[]
 for year in (2020,2021,2022,2023,2024,2025):
  for i in range(4):rows.append({'code':str(1000+i),'ymd':year*10000+101+i,'ret_20b':[-.2,-.05,.1,.2][i] if year<2025 else -99.,'max_drawdown_20b':[-.3,-.1,-.02,-.01][i],'max_up_20b':[.01,.03,.15,.3][i],'bars_since_cross_above_ma20':i,'lower_support_bucket':'none_near'})
 pd.DataFrame(rows).to_parquet(p,index=False);return p

def test_path_metrics_and_fixed_candidate(tmp_path:Path)->None:
 d=analyze(_source(tmp_path/'x.parquet'));x=d['candidate']['train']
 assert d['fixed_candidate']['condition_reselection'] is False and x['n']==3
 assert x['loss_tail_p5'] is not None and x['mae_p5'] is not None and x['daily_equal_weight']['days']==3
 assert d['baseline']['train']['n']==12

def test_test_period_is_physically_unopened(tmp_path:Path)->None:
 d=analyze(_source(tmp_path/'x.parquet'))
 assert d['periods']['maximum_observed_date']==20241231
 assert d['test_access']=={'status':'not_opened','rows_read':False,'metrics':None}

def test_run_outputs_authoritative_pair(tmp_path:Path)->None:
 root=run(_source(tmp_path/'x.parquet'),tmp_path/'out');assert {x.name for x in root.iterdir()}=={'compare.json','run_manifest.json'}
 assert json.loads((root/'run_manifest.json').read_text(encoding='utf-8'))['test_rows_read'] is False
