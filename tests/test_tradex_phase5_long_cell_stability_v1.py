import json
from pathlib import Path

import pandas as pd

from scripts.tradex_phase5_long_cell_stability_v1 import analyze,run


def _source(path:Path)->Path:
    rows=[]
    for year in (2020,2021,2022,2023,2024,2025):
        for i in range(3):
            rows.append({"code":str(1000+i),"ymd":year*10000+101+i,"ret_20b":(-.01 if i==0 else .04) if year<2025 else -99.0,"bars_since_cross_above_ma20":0,"lower_support_bucket":"none_near"})
        rows.append({"code":"9999","ymd":year*10000+200,"ret_20b":99.0,"bars_since_cross_above_ma20":1,"lower_support_bucket":"far"})
    pd.DataFrame(rows).to_parquet(path,index=False);return path


def test_fixed_cell_decomposition_and_test_unopened(tmp_path:Path)->None:
    d=analyze(_source(tmp_path/'x.parquet'))
    assert d['fixed_candidate']['condition_reselection'] is False
    assert d['train']['n']==9 and d['validation']['n']==6
    assert len(d['by_train_year'])==3 and len(d['leave_one_train_year_out'])==3
    assert d['stability_contract']['minimum_active_train_years']==3
    assert d['fixed_periods']['maximum_observed_date']==20241231
    assert d['test_access']=={'status':'not_opened','rows_read':False,'metrics':None}


def test_missing_regime_is_explicit_not_inferred(tmp_path:Path)->None:
    d=analyze(_source(tmp_path/'x.parquet'))
    assert d['regime_column']=='unavailable_in_source'
    assert d['by_regime_train_and_validation'][0]['regime_bucket']=='unavailable_in_source'


def test_run_writes_compare_and_manifest(tmp_path:Path)->None:
    source=_source(tmp_path/'x.parquet');root=run(source,tmp_path/'out')
    assert {p.name for p in root.iterdir()}=={'compare.json','run_manifest.json'}
    m=json.loads((root/'run_manifest.json').read_text(encoding='utf-8'))
    assert m['test_rows_read'] is False and m['artifact_role']=='authoritative'
