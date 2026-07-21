from __future__ import annotations

import argparse, json, sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_s4_trigger_topology_audit_v1 as topo

AXIS_ID='tradex_nikkei225_s4_topology_with_failed_rebound_v1'
STATE_V2=Path(r'G:\Tradex\failed_rebound_before_rebreak_v1\20260714T174509Z-tradex_nikkei225_failed_rebound_before_rebreak_v1')
OUT=Path(r'G:\Tradex\s4_topology_with_failed_rebound_v1')

def load_v2()->pd.DataFrame:
 z=json.loads((STATE_V2/'_ARTIFACT_COMPLETE.json').read_text(encoding='utf-8'))
 if z.get('complete') is not True:raise ValueError('D state artifact incomplete')
 d=pd.read_parquet(topo.DAILY).sort_values(['code','ymd']).reset_index(drop=True);s=pd.read_parquet(STATE_V2/'state_ledger_v2.parquet')
 for q in (d,s):q['code']=q.code.astype(str).str.zfill(4);q['ymd']=pd.to_numeric(q.ymd).astype(int)
 x=d.merge(s,on=['code','ymd'],validate='one_to_one').sort_values(['code','ymd']).reset_index(drop=True)
 flags={'A':x.trigger_gap_down.astype(bool),'B':x.trigger_ma20_break.astype(bool),'C':x.trigger_support_break.astype(bool),'D':x.trigger_failed_rebound.astype(bool)}
 x['trigger_topology']=[''.join(k for k,v in flags.items() if bool(v.iloc[i])) or 'OTHER' for i in range(len(x))]
 er=x.groupby('code',sort=False).bull_erasure_retry_candidate.transform(lambda q:q.shift().rolling(20,min_periods=1).max()).fillna(0).astype(bool)
 retry_source=x.s2_candidate_today.astype(bool)&~x.bull_erasure_retry_candidate.astype(bool)
 rt=retry_source.groupby(x.code,sort=False).transform(lambda q:q.shift().rolling(20,min_periods=1).max()).fillna(0).astype(bool)
 x['prior_path']=np.select([er&rt,er,rt],['MIXED','ERASURE','RETRY'],'UNRESOLVED');x['topology_path']=x.trigger_topology+'__'+x.prior_path
 eligible=(x.s1_top_risk|x.s2_top_formation|x.s3_weakening)&~x.s4_sell_trigger_raw_v2&~x.s8_sell_reentry
 x['nontrigger_dedup']=base.event_mask(eligible.to_numpy(bool),x.code.to_numpy(str),x.ymd.to_numpy(int))
 x['s4_sell_trigger_event']=x.sell_action_event_v2;x['s4_sell_trigger_raw']=x.s4_sell_trigger_raw_v2|x.s8_sell_reentry
 return x

def run()->Path:
 topo.AXIS_ID=AXIS_ID;topo.OUT=OUT;topo.STATE=STATE_V2;topo.load=load_v2
 out=topo.run();p=out/'compare.json';c=json.loads(p.read_text(encoding='utf-8'));c['schema_version']=AXIS_ID+'.compare.v1';c['single_changed_axis']='exact_fixed_S4_topology_with_instrumented_D_failed_rebound';c['source']['state_root']=str(STATE_V2);c['source']['state_complete_sha256']=topo.sha(STATE_V2/'_ARTIFACT_COMPLETE.json');c['fixed_contract']['trigger_axes']={'A':'gap_down_fixed','B':'ma20_break_or_rebreak_fixed','C':'support_break_fixed','D':'failed_rebound_before_rebreak_instrumented_raw_PIT'};topo.dump(p,c);topo.dump(out/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p),'compare_sha256':topo.sha(p)});return out

def main():argparse.ArgumentParser().parse_args();print(run())
if __name__=='__main__':main()
