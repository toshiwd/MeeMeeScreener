from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_leaf_lot_rounding_slippage_v1 import replay
AXIS_ID='leaf_vs_meemee_operational_stress_v1';OUT=Path(r'G:\Tradex\leaf_vs_meemee_operational_stress_v1');SLIPS=(0,.001,.002)
def run():
 leafp=Path(sorted(glob.glob(r'G:\Tradex\leaf_position_cap_axis_v1\*\selected_events.csv'))[-1]);mp=Path(sorted(glob.glob(r'G:\Tradex\leaf_vs_meemee_same_condition_v1\*\meemee_cap5_events.csv'))[-1]);leaf=pd.read_csv(leafp);m=pd.read_csv(mp);rows=[]
 for s in SLIPS:
  l=replay(leaf,2_400_000,s);mm=replay(m,2_000_000,s);rows.append({'slippage_rate':s,'leaf_cap4_slot2_4m':l,'meemee_cap5_slot2m':mm,'leaf_minus_meemee_pnl_2024_2025_yen':l['pnl_2024_2025_yen']-mm['pnl_2024_2025_yen'],'same_condition_pass':bool(l['pnl_2024_2025_yen']>mm['pnl_2024_2025_yen'] and l['test_2024_2025_money_profit_factor']>=1.5 and l['max_realized_drawdown_yen']>=-1500000 and l['red_year_count']==0)})
 selected=next(r for r in rows if r['slippage_rate']==.001);root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'leaf_source':str(leafp),'meemee_source':str(mp),'round_lot':100,'leaf_slot_yen':2400000,'leaf_max_positions':4,'meemee_slot_yen':2000000,'meemee_max_positions':5,'entry_adverse_slippage':SLIPS,'same_exit_simulation':True},'variants':rows,'selection':{'operational_stress_rate':.001,'selected_result':selected},'decision':{'candidate_local_decision':'keep_for_display_only_readiness' if selected['same_condition_pass'] else 'drop','authoritative_rollup_decision':'research_only','reason_type':'same_round_lot_and_slippage_comparison'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
