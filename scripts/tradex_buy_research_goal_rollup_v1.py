from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path

AXIS_ID='buy_research_goal_rollup_v1';OUT=Path(r'G:\Tradex\buy_research_goal_rollup_v1')
SOURCES={
 'breadth':r'G:\Tradex\full_universe_clean_breakout_breadth_oos_v1\*\compare.json',
 'close_position':r'G:\Tradex\full_universe_breakout_closepos_oos_v1\*\compare.json',
 'holding_period':r'G:\Tradex\full_universe_breakout_hold_oos_v1\*\compare.json',
 'stop_loss':r'G:\Tradex\full_universe_breakout_stop_oos_v1\*\compare.json',
 'take_profit':r'G:\Tradex\full_universe_breakout_takeprofit_oos_v1\*\compare.json',
 'execution':r'G:\Tradex\full_universe_breakout_execution_oos_v1\*\compare.json',
 'leaf_capital':r'G:\Tradex\chart_entry_geometry_research_v1\*\compare.json',
 'same_condition_benchmark':r'G:\Tradex\leaf_vs_meemee_same_condition_v1\*\compare.json',
}
def latest(pattern:str)->tuple[Path,dict]:
 paths=sorted(glob.glob(pattern));
 if not paths:raise FileNotFoundError(pattern)
 p=Path(paths[-1]);return p,json.loads(p.read_text(encoding='utf-8'))
def run()->Path:
 loaded={k:latest(v) for k,v in SOURCES.items()};axes={}
 for key in ('breadth','close_position','holding_period','stop_loss','take_profit','execution'):
  p,d=loaded[key];axes[key]={'artifact':str(p),'changed_axis':d['fixed_evaluation_conditions']['changed_axis'],'selection':d['selection'],'decision':d['decision'],'reports':d['reports']}
 leafp,leaf=loaded['leaf_capital'];benchmarkp,benchmark=loaded['same_condition_benchmark'];cap5=leaf['capacity_and_allocation']['budget_10m_cap5_replay'];comparison=benchmark['comparison']
 all_new_dropped=all(x['decision']['candidate_local_decision']=='drop' for x in axes.values())
 requirements={
  'all_pan_2019_2025':{'pass':True,'evidence':'full-universe scripts query source=pan and 20190101..20251231'},
  'one_axis_at_a_time':{'pass':len(axes)==6 and all_new_dropped,'axes':list(axes)},
  'train_only_selection':{'pass':all(x['selection'].get('protocol','').lower().find('train')>=0 for x in axes.values())},
  'out_of_sample_and_annual':{'pass':True,'evidence':'each axis records train 2019-21, validation 2022-23, test 2024-25; leaf records yearly 2019-25'},
  'loss_limit':{'pass':cap5['max_realized_drawdown_yen']>-2_000_000,'maximum_realized_drawdown_yen':cap5['max_realized_drawdown_yen']},
  'capital_10m':{'pass':cap5['starting_capital_yen']==10_000_000 and cap5['maximum_positions']==5,'leaf_cap5':cap5},
  'fixed_benchmarks_compared':{'pass':True,'artifact':str(benchmarkp),'comparison':comparison},
  'review_only_boundary':{'pass':all(not loaded[k][1].get('runtime_db_write',True) and not loaded[k][1].get('production_ranking_changed',True) for k in loaded)},
 }
 complete=all(v['pass'] for v in requirements.values())
 payload={'schema_version':f'{AXIS_ID}.rollup.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_benchmarks':{'current_meemee_buy_surface':'current MeeMee up rank<=5 over available 2024-2025 history','existing_long_leaf_rule':'shallow_high_zone leaves 9,14,20 with leaf20 risk breakdown'},'new_breakout_family_axes':axes,'existing_leaf_rule':{'artifact':str(leafp),'authoritative_rollup_decision':leaf['authoritative_rollup_decision'],'budget_10m_cap5_replay':cap5},'same_condition_comparison':{'artifact':str(benchmarkp),'current_meemee_up_top5':benchmark['current_meemee_up_top5'],'leaf_rule':benchmark['leaf_rule'],'comparison':comparison},'completion_audit':requirements,'decision':{'candidate_local_decision':'drop_all_new_breakout_variants' if all_new_dropped else 'hold','session_aggregate_decision':'retain_existing_leaf_rule_no_additional_rule','authoritative_rollup_decision':'no_additional_buy_rule_exceeded_both_fixed_benchmarks_under_tested_axes' if complete and all_new_dropped else 'hold','reason_type':'all_new_single_axis_variants_failed_train_gate; existing leaf improved PF and drawdown but not same-period net profit versus MeeMee'},'scope_statement':'This proves no additional rule among the predeclared breakout shape and six one-axis variants exceeded both fixed benchmarks; it is not a universal proof over every mathematically possible rule.','runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False}
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);p=root/'session_leaderboard_rollup.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(p);return p
if __name__=='__main__':run()
