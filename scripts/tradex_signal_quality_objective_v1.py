from __future__ import annotations
import json
from datetime import datetime,timezone
from pathlib import Path
AXIS_ID='signal_quality_objective_v1';OUT=Path(r'G:\Tradex\signal_quality_objective_v1')
def run():
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True)
 payload={'schema_version':f'{AXIS_ID}.contract.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'comparison_stabilization','objective':'maximize per-signal win probability and profit width without a capital-allocation target, while controlling downside','fixed_splits':{'train':'2019-2021','validation':'2022-2023','untouched_test':'2024-2025'},'selection_protocol':{'selection_data':'train only','primary_score':'mean(log1p(net_return))','tie_breaks':['expectancy descending','profit_factor descending','sample_count descending'],'pareto_metrics':['win_rate','average_win','payoff_ratio','expectancy','profit_factor']},'hard_gates':{'train_min_samples':200,'validation_min_samples':100,'test_min_samples':100,'expectancy_min_each_split':0.0,'profit_factor_min_each_split':1.3,'win_rate_min_each_split':0.45,'payoff_ratio_min_each_split':1.2,'return_p05_floor':-0.05,'maximum_loss_floor':-0.05},'axes_in_order':['take_profit','stop_loss','maximum_holding_sessions'],'removed_objectives':['fixed capital amount','slot count','maximum concurrent positions','absolute portfolio PnL','MeeMee PnL comparison'],'boundary':{'owner':'TRADEX','review_only':True,'meemee_display_change_allowed':False,'runtime_db_write':False,'production_ranking_change':False},'silent_fallback_used':False}
 p=root/'objective_contract.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(p)
if __name__=='__main__':run()
