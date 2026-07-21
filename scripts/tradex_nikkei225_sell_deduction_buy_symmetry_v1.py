from __future__ import annotations

import argparse, ast, csv, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

AXIS_ID="tradex_nikkei225_sell_deduction_buy_symmetry_v1"
SPLITS={"train_2024":(20240101,20241231),"validation_2025":(20250101,20251231),"shadow_2026":(20260101,20261231)}

def _dict(value:str)->dict[str,float]:
    return {str(k):float(v) for k,v in ast.literal_eval(value or "{}").items()}

def _dedupe(rows:list[dict[str,Any]])->list[dict[str,Any]]:
    out=[];pos={};last={}
    for row in rows:
        code=row["code"];p=pos.get(code,-1)+1;pos[code]=p
        if code not in last or p-last[code]>10:out.append(row);last[code]=p
    return out

def _long(rows:list[dict[str,Any]])->dict[str,Any]:
    x=[r for r in rows if r["ret10"] is not None]
    if not x:return {"n":0,"codes":0,"positive_close10_rate":None,"mean_ret10":None,"high5pct10_rate":None,"low_minus5pct10_rate":None}
    return {"n":len(x),"codes":len({r['code'] for r in x}),"positive_close10_rate":sum(r['ret10']>0 for r in x)/len(x),"mean_ret10":sum(r['ret10'] for r in x)/len(x),"high5pct10_rate":sum(r['mfe_long']>=.05 for r in x)/len(x),"low_minus5pct10_rate":sum(r['mfe_short']>=.05 for r in x)/len(x)}

def run(input_csv:Path,out_root:Path)->Path:
    rows=[]
    with input_csv.open('r',encoding='utf-8-sig',newline='') as fh:
        for r in csv.DictReader(fh):
            rows.append({"code":str(r['code']),"ymd":int(r['ymd']),"adds":_dict(r['sell_flow_additions']),"deducts":_dict(r['sell_flow_deductions']),"irregular":r['irregular_event'].lower()=='true',"ret10":float(r['ret10_forward']) if r['ret10_forward'] else None,"mfe_short":float(r['mfe_short_10']) if r['mfe_short_10'] else 0.0,"mfe_long":float(r['mfe_long_10']) if r['mfe_long_10'] else 0.0})
    rows.sort(key=lambda r:(r['code'],r['ymd']))
    baselines={name:_long([r for r in rows if a<=r['ymd']<=b and not r['irregular']]) for name,(a,b) in SPLITS.items()}
    components=sorted({k for r in rows for k in r['deducts']})
    results={}
    for component in components:
        events=_dedupe([r for r in rows if component in r['deducts'] and not r['irregular']])
        metrics={name:_long([r for r in events if a<=r['ymd']<=b]) for name,(a,b) in SPLITS.items()}
        tr,val,sh=metrics['train_2024'],metrics['validation_2025'],metrics['shadow_2026'];bt=baselines['train_2024'];bv=baselines['validation_2025'];bs=baselines['shadow_2026']
        train_edge=(tr.get('n') or 0)>=30 and (tr.get('mean_ret10') or -1)>=(bt.get('mean_ret10') or 0)+.005 and (tr.get('positive_close10_rate') or 0)>=(bt.get('positive_close10_rate') or 0)+.05
        val_edge=(val.get('n') or 0)>=30 and (val.get('mean_ret10') or -1)>(bv.get('mean_ret10') or 0) and (val.get('positive_close10_rate') or 0)>.50
        shadow_edge=(sh.get('n') or 0)>=20 and (sh.get('mean_ret10') or -1)>(bs.get('mean_ret10') or 0) and (sh.get('positive_close10_rate') or 0)>.50
        classification='direct_buy_addition' if train_edge and val_edge and shadow_edge else 'conditional_or_regime_dependent' if train_edge else 'sell_deduction_only_not_buy_addition'
        results[component]={"classification":classification,"metrics":metrics,"baseline":baselines,"gates":{"train_edge":train_edge,"validation_edge":val_edge,"shadow_edge":shadow_edge}}
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=out_root/f'{stamp}-{AXIS_ID}';out.mkdir(parents=True,exist_ok=False)
    counts={c:sum(v['classification']==c for v in results.values()) for c in ('direct_buy_addition','conditional_or_regime_dependent','sell_deduction_only_not_buy_addition')}
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"universe":"current Nikkei225 registry; survivorship-biased research slice","splits":SPLITS,"changed_axis":"sell-deduction to direct-buy-addition symmetry only","event":"first component occurrence after 10-bar cooldown","horizon":10,"costs":"ignored by user rule"},"source_ledger":str(input_csv),"baseline":baselines,"component_results":results,"classification_counts":counts,"decision":{"candidate_local_decision":"keep_direct_symmetry_components" if counts['direct_buy_addition'] else "no_direct_symmetry_confirmed","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
    (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'compare':str(out/'compare.json')},indent=2)+"\n",encoding='utf-8');return out

def main()->None:
    p=argparse.ArgumentParser();p.add_argument('--input-csv',type=Path,required=True);p.add_argument('--output-root',type=Path,default=Path(r'G:\Tradex\tradex_nikkei225_sell_deduction_buy_symmetry_v1'));a=p.parse_args();print(run(a.input_csv,a.output_root))
if __name__=='__main__':main()
