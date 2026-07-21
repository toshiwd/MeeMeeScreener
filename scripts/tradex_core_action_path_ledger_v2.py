import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS = range(2021, 2026)

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--sequence',type=Path,required=True);p.add_argument('--try-fail',type=Path,required=True)
    p.add_argument('--ma200',type=Path,required=True);p.add_argument('--support-break',type=Path,required=True)
    p.add_argument('--full-erasure',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
    s=pd.read_parquet(a.sequence)
    w=s[s.gd_ymd.notna()&s.ma20_rebreak_ymd.notna()&(s.ma20_rebreak_ymd>s.gd_ymd)&s.weak_rebound&(s.max_consecutive_closes_above_ma7<7)].copy()
    w['action_ymd']=w.ma20_rebreak_ymd.astype(int);w['probe_ymd']=w.erasure_ymd.astype('Int64');w['outcome_fixed3_h5']=w.ma20_rebreak_outcome_fixed3_h5
    w['action_lane']='STAGED_CORE';w['source_family']='WEAK_REBOUND_MA20_REBREAK_CORE'
    weak_raw=len(w);w=w.sort_values('probe_ymd').drop_duplicates(['code','action_ymd'],keep='last')
    t=pd.read_parquet(a.try_fail);t['action_ymd']=t.ymd.astype(int);t['outcome_fixed3_h5']=t.outcome
    t['action_lane']='STAGED_CORE';t['source_family']='UPTREND_CEILING_TRY_FAIL_PRIOR_PROBE_CORE'
    m=pd.read_parquet(a.ma200);m=m[m.core_ymd.notna()].copy();m['action_ymd']=m.core_ymd.astype(int);m['outcome_fixed3_h5']=m.core_outcome_fixed3_h5
    m['action_lane']='STAGED_CORE';m['source_family']='BOX_MA200_REJECTION_CORE'
    b=pd.read_parquet(a.support_break);b['action_ymd']=b.ymd.astype(int);b['probe_ymd']=pd.NA;b['outcome_fixed3_h5']=b.outcome
    b['action_lane']='DIRECT_CORE';b['source_family']='POSTBOX_SUPPORT_BREAK_DIRECT_CORE'
    e=pd.read_parquet(a.full_erasure);e=e[e.branch.eq('UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE')].copy()
    e['action_ymd']=e.action_ymd.astype(int);e['probe_ymd']=pd.NA;e['outcome_fixed3_h5']=e.outcome
    e['action_lane']='DIRECT_CORE';e['source_family']='UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE'
    cols=['code','action_ymd','probe_ymd','action_lane','source_family','outcome_fixed3_h5']
    raw=pd.concat([x[cols] for x in [w,t,m,b,e]],ignore_index=True);raw.code=raw.code.astype(str).str.zfill(4)
    raw['year']=raw.action_ymd.astype(str).str[:4].astype(int);raw=raw[raw.year.isin(YEARS)].copy()
    conflicts=int((raw.groupby(['code','action_ymd']).outcome_fixed3_h5.nunique()>1).sum())
    def collapse(g):
        return pd.Series({'probe_ymd':g.probe_ymd.dropna().max() if g.probe_ymd.notna().any() else pd.NA,
          'action_lanes':'|'.join(sorted(g.action_lane.unique())),'source_families':'|'.join(sorted(g.source_family.unique())),
          'outcome_fixed3_h5':g.outcome_fixed3_h5.iloc[0],'year':g.year.iloc[0],
          'source_family_count':g.source_family.nunique(),'action_lane_count':g.action_lane.nunique()})
    ledger=raw.groupby(['code','action_ymd'],as_index=False).apply(collapse,include_groups=False).reset_index(drop=True)
    ledger['research_fallback']=ledger.apply(lambda r: r.year<2023 and 'BOX_MA200_REJECTION_CORE' in r.source_families,axis=1)
    data={'schema_version':'tradex_core_action_path_ledger_v2.compare.v1','artifact_role':'authoritative_infrastructure','review_only':True,
      'contract':{'dedupe':'one code/action; weak uses latest prior erasure','lane_collision':'preserve all lane tags','outcome':'inherited fixed3 h5','years':list(YEARS)},
      'counts':{'weak_raw':weak_raw,'weak_after_latest_probe_dedupe':len(w),'raw_2021_2025':len(raw),'unique_actions':len(ledger),
        'multi_family_actions':int(ledger.source_family_count.gt(1).sum()),'multi_lane_actions':int(ledger.action_lane_count.gt(1).sum()),'outcome_conflicts':conflicts,
        'research_fallback_rows':int(ledger.research_fallback.sum())},
      'judgment':{'decision':'keep_infrastructure' if conflicts==0 else 'drop','reason':'action path is one row per observable action with collisions preserved'},
      'not_changed':['source detectors','outcomes','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');ledger.to_parquet(a.output/'action_path_ledger.parquet',index=False)
    audit={'duplicates':int(ledger.duplicated(['code','action_ymd']).sum()),'outcome_conflicts':conflicts,'future_used_for_selection':False,
      'input_sha256':{k:sha(getattr(a,k)) for k in ['sequence','try_fail','ma200','support_break','full_erasure']}}
    (a.output/'audit.json').write_text(json.dumps(audit,indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'counts':data['counts'],'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
