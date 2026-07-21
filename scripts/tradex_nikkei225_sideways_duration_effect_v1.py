from __future__ import annotations
import argparse,csv,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any

AXIS_ID='tradex_nikkei225_sideways_duration_effect_v1';SPLITS={'train_2024':(20240101,20241231),'validation_2025':(20250101,20251231),'shadow_2026':(20260101,20261231)};LENGTHS=(3,5,7,10,15)
def metric(rows:list[dict[str,Any]])->dict[str,Any]:
 x=[r for r in rows if r['ret10'] is not None]
 if not x:return {'n':0}
 return {'n':len(x),'codes':len({r['code'] for r in x}),'mean_ret5':sum(r['ret5'] for r in x if r['ret5'] is not None)/max(1,sum(r['ret5'] is not None for r in x)),'mean_ret10':sum(r['ret10'] for r in x)/len(x),'up_close10_rate':sum(r['ret10']>0 for r in x)/len(x),'down_close10_rate':sum(r['ret10']<0 for r in x)/len(x),'up_high5pct10_rate':sum(r['mfe_long']>=.05 for r in x)/len(x),'down_low5pct10_rate':sum(r['mfe_short']>=.05 for r in x)/len(x)}
def run(inp:Path,outroot:Path)->Path:
 rows=[]
 with inp.open('r',encoding='utf-8-sig',newline='') as fh:
  for r in csv.DictReader(fh):
   rows.append({'code':str(r['code']),'ymd':int(r['ymd']),'sideways20':int(r['sideways20_run_length'] or 0),'flat':int(r['flat_run_length'] or 0),'position':r['sideways_position'],'ma20_side':r['sideways_ma20_side'],'pretrend':r['pretrend_10'],'volume_compression':float(r['volume_compression_5_20']) if r['volume_compression_5_20'] else None,'irregular':r['irregular_event'].lower()=='true','ret5':float(r['ret5_forward']) if r['ret5_forward'] else None,'ret10':float(r['ret10_forward']) if r['ret10_forward'] else None,'mfe_short':float(r['mfe_short_10']) if r['mfe_short_10'] else 0.0,'mfe_long':float(r['mfe_long_10']) if r['mfe_long_10'] else 0.0})
 results={};context={}
 for family,column in [('flat_daily_bars','flat'),('range20_within_10pct','sideways20')]:
  results[family]={};context[family]={}
  for length in LENGTHS:
   events=[r for r in rows if r[column]==length and not r['irregular']]
   bysplit={name:metric([r for r in events if a<=r['ymd']<=b]) for name,(a,b) in SPLITS.items()};results[family][str(length)]=bysplit
   context[family][str(length)]={}
   for field in ('position','ma20_side','pretrend'):
    context[family][str(length)][field]={value:metric([r for r in events if r[field]==value]) for value in sorted({r[field] for r in events})}
 decisions={}
 for family,items in results.items():
  decisions[family]={}
  for length,m in items.items():
   va,sh=m['validation_2025'],m['shadow_2026'];direction='long' if (va.get('n') or 0)>=30 and (sh.get('n') or 0)>=20 and (va.get('mean_ret10') or 0)>0 and (sh.get('mean_ret10') or 0)>0 and (va.get('up_close10_rate') or 0)>.52 and (sh.get('up_close10_rate') or 0)>.52 else 'short' if (va.get('n') or 0)>=30 and (sh.get('n') or 0)>=20 and (va.get('mean_ret10') or 0)<0 and (sh.get('mean_ret10') or 0)<0 and (va.get('down_close10_rate') or 0)>.52 and (sh.get('down_close10_rate') or 0)>.52 else 'neutral_or_context_dependent';decisions[family][length]=direction
 stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=outroot/f'{stamp}-{AXIS_ID}';out.mkdir(parents=True,exist_ok=False)
 payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'universe':'current Nikkei225 registry; survivorship-biased research slice','splits':SPLITS,'changed_axis':'sideways duration only','lengths':LENGTHS,'flat_bar':'abs(close-prev_close)<=0.5ATR and daily range<=1.2ATR','sideways20':'rolling 20-bar high-low range <=10pct of close','event':'first bar reaching exact run length','horizon':[5,10],'costs':'ignored by user rule'},'source_ledger':str(inp),'duration_metrics':results,'context_diagnostics':context,'decisions':decisions,'decision':{'candidate_local_decision':'duration_effect_reviewed','authoritative_rollup_decision':'review_only'},'boundary':{'owner':'TRADEX','meemee_changed':False,'runtime_db_write':False,'production_ranking_changed':False}}
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'compare':str(out/'compare.json')},indent=2)+'\n',encoding='utf-8');return out
def main():
 p=argparse.ArgumentParser();p.add_argument('--input-csv',type=Path,required=True);p.add_argument('--output-root',type=Path,default=Path(r'G:\Tradex\tradex_nikkei225_sideways_duration_effect_v1'));a=p.parse_args();print(run(a.input_csv,a.output_root))
if __name__=='__main__':main()
