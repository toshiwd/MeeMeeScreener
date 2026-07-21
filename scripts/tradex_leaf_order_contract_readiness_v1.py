from __future__ import annotations
import glob,json,math
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
AXIS_ID='leaf_order_contract_readiness_v1';OUT=Path(r'G:\Tradex\leaf_order_contract_readiness_v1');SLOT=2_400_000;SLIPS=(0,.001)
def replay(x,slip):
 accepted=[];active=[];unaffordable=0;eligible_top3=0;max_invested=0.0
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[p for p in active if p['exit_date']>=d];day=g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(3);eligible_top3+=len(day)
  for i,r in day.iterrows():
   if len(active)>=4:break
   fill=float(r.entry_price)*(1+slip);shares=math.floor(SLOT/(fill*100))*100
   if shares<=0:unaffordable+=1;continue
   invested=shares*fill;accepted.append((i,fill,shares,invested));active.append({'exit_date':r.exit_date,'invested':invested});max_invested=max(max_invested,sum(p['invested'] for p in active))
 z=x.loc[[a[0] for a in accepted]].copy();z['fill_price']=[a[1] for a in accepted];z['shares']=[a[2] for a in accepted];z['invested_yen']=[a[3] for a in accepted];z['exit_price']=z.entry_price*(1+z.next_open_return);z['pnl_yen']=z.shares*(z.exit_price-z.fill_price);daily=z.groupby('exit_date').pnl_yen.sum().sort_index();eq=1e7+daily.cumsum();dd=eq-eq.cummax();annual=z.groupby('year').pnl_yen.sum();test=z[z.year>=2024];pos=test.loc[test.pnl_yen>0,'pnl_yen'].sum();neg=-test.loc[test.pnl_yen<0,'pnl_yen'].sum();return z,{'slippage_rate':slip,'eligible_top3_rows':eligible_top3,'accepted_trade_count':len(z),'unaffordable_top3_count':unaffordable,'unaffordable_rate':unaffordable/eligible_top3,'mean_invested_yen':float(z.invested_yen.mean()),'max_concurrent_invested_yen':max_invested,'minimum_cash_buffer_yen':1e7-max_invested,'pnl_2024_2025_yen':float(test.pnl_yen.sum()),'test_money_profit_factor':float(pos/neg),'max_realized_drawdown_yen':float(dd.min()),'annual_pnl_yen':{str(int(y)):float(v) for y,v in annual.items()},'red_year_count':int((annual<=0).sum())}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[]
 for s in SLIPS:
  z,m=replay(x,s);m['operational_gate_pass']=bool(m['pnl_2024_2025_yen']>3810508 and m['test_money_profit_factor']>=1.5 and m['max_realized_drawdown_yen']>=-1500000 and m['red_year_count']==0 and m['max_concurrent_invested_yen']<=1e7);rows.append(m)
 selected=next(r for r in rows if r['slippage_rate']==.001);root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'order':'next-session opening auction market order','adverse_fill_stress':SLIPS,'slot_budget_yen':SLOT,'round_lot':100,'maximum_positions':4,'same_day_candidate_cap':3,'unaffordable_policy':'skip; do not promote rank4 or lower','benchmark_under_10bp_yen':3810508},'variants':rows,'selected_operational_contract':selected,'decision':{'candidate_local_decision':'keep_for_display_only_readiness' if selected['operational_gate_pass'] else 'drop','authoritative_rollup_decision':'research_only','reason_type':'actual_round_lot_affordability_and_cash_concurrency'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
