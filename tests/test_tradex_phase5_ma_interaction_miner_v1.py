import json
from pathlib import Path

import pandas as pd

from scripts.tradex_phase5_ma_interaction_miner_v1 import build, run


def _source(path:Path)->Path:
    rows=[]
    for year in (2020,2021,2022,2023,2024,2025):
        for i in range(4):
            ret = (-.01 if i == 0 else .05) if year < 2025 else -9.0
            rows.append({"code":str(1000+i),"ymd":year*10000+101+i,"ret_20b":ret,"below_ma20_run_bucket":"3-5","upper_ma_count_within_3pct":2,"ma_stack_state":"bear","is_upper_shadow_long":True,"bars_since_cross_above_ma20":1,"lower_support_bucket":"near"})
    pd.DataFrame(rows).to_parquet(path,index=False);return path


def test_train_selection_and_test_rows_are_never_read(tmp_path:Path)->None:
    source=_source(tmp_path/'x.parquet');compare,freeze=build(source,min_n=3,min_codes=3,max_year_share=.5)
    row=next(x for x in compare['candidate_reports'] if x['candidate_id']=='long_bars_since_cross_ma20_x_lower_support')
    assert row['train_gate_pass'] is True and row['validation_gate_pass'] is True
    assert compare['test_access']['rows_read'] is False and compare['test_access']['metrics_computed'] is False
    assert 'long_bars_since_cross_ma20_x_lower_support' in freeze['frozen_candidate_ids'] and freeze['test_metrics'] is None


def test_failed_train_keeps_validation_locked(tmp_path:Path)->None:
    source=_source(tmp_path/'x.parquet');compare,_=build(source,min_n=999,min_codes=3,max_year_share=.5)
    row=next(x for x in compare['candidate_reports'] if x['candidate_id']=='long_bars_since_cross_ma20_x_lower_support')
    assert row['train_gate_pass'] is False and row['validation']=='locked_by_train_gate'


def test_run_writes_authoritative_artifacts(tmp_path:Path)->None:
    source=_source(tmp_path/'x.parquet');root=run(source,tmp_path/'out',min_n=3,min_codes=3,max_year_share=.5)
    assert {p.name for p in root.iterdir()}=={'compare.json','run_manifest.json','test_unlock_freeze.json'}
    manifest=json.loads((root/'run_manifest.json').read_text(encoding='utf-8'))
    assert manifest['test_rows_read'] is False and manifest['artifact_role']=='authoritative'
    compare=json.loads((root/'compare.json').read_text(encoding='utf-8'))
    assert compare['fixed_evaluation_conditions']['source']==str(source)
    assert compare['fixed_evaluation_conditions']['candidates']==['short_below_ma20_run_x_upper_ma_count_3pct','short_stack_transition_x_upper_shadow','long_bars_since_cross_ma20_x_lower_support']


def test_validation_outcomes_do_not_change_train_selected_cell(tmp_path:Path)->None:
    source=_source(tmp_path/'x.parquet');first,_=build(source,min_n=3,min_codes=3,max_year_share=.5)
    frame=pd.read_parquet(source);frame.loc[frame['ymd'].between(20230101,20241231),'ret_20b']=-99.0;frame.to_parquet(source,index=False)
    second,_=build(source,min_n=3,min_codes=3,max_year_share=.5)
    for candidate_id in ('short_below_ma20_run_x_upper_ma_count_3pct','long_bars_since_cross_ma20_x_lower_support'):
        a=next(x for x in first['candidate_reports'] if x['candidate_id']==candidate_id)
        b=next(x for x in second['candidate_reports'] if x['candidate_id']==candidate_id)
        assert a['selected_cell_id']==b['selected_cell_id']
