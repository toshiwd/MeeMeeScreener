"""Build confirmed-month PIT selection features; weekly inputs are forbidden."""
import argparse,hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def streak(s):
 b=s.fillna(False);return b.astype(int).groupby((~b).cumsum()).cumsum()
def main():
 p=argparse.ArgumentParser();p.add_argument('--monthly-layers',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 x=pd.read_parquet(a.monthly_layers);x.code=x.code.astype(str).str.zfill(4);x=x.sort_values(['code','month']).copy()
 for n in [7,20,60]:x[f'ma{n}m']=x.groupby('code').c.transform(lambda s:s.rolling(n,min_periods=n).mean());x[f'ma{n}m_slope3_pct']=x.groupby('code')[f'ma{n}m'].pct_change(3,fill_method=None)*100;x[f'above_ma{n}m_run']=x.groupby('code',group_keys=False)[f'ma{n}m'].apply(lambda s:pd.Series(0,index=s.index))
 for code,g in x.groupby('code'):
  for n in [7,20,60]:x.loc[g.index,f'above_ma{n}m_run']=streak(g.c.gt(g[f'ma{n}m'])).values
 rng=(x.h-x.l).replace(0,np.nan);x['monthly_body_ratio']=(x.c-x.o).abs()/rng;x['monthly_upper_wick_ratio']=(x.h-x[['o','c']].max(axis=1))/rng;x['monthly_lower_wick_ratio']=(x[['o','c']].min(axis=1)-x.l)/rng;x['monthly_close_pos']=(x.c-x.l)/rng
 for n in [3,6,12]:x[f'monthly_ret{n}']=x.groupby('code').c.pct_change(n,fill_method=None)
 for n in [12,24]:
  lo=x.groupby('code').l.transform(lambda s:s.rolling(n,min_periods=n).min());hi=x.groupby('code').h.transform(lambda s:s.rolling(n,min_periods=n).max());x[f'monthly_pos{n}']=(x.c-lo)/(hi-lo)
 x['confirmed_environment']=np.select([x.base_regime.eq('BOX')&x.local_box_mature,x.base_regime.eq('POST_BOX_BREAKOUT_CONSOLIDATION'),x.c.gt(x.ma7m)&x.ma7m.gt(x.ma20m)&x.ma7m_slope3_pct.gt(0),x.c.lt(x.ma7m)&x.ma7m_slope3_pct.lt(0)],['MATURE_BOX','POST_BOX_BREAKOUT_CONSOLIDATION','UP_EXTENDED','DOWN_OR_FAILURE'],default='UNCLEAR')
 cols=['code','month','source_month','effective_month','o','h','l','c','bars','base_regime','confirmed_environment','local_box_mature','local_box_upper','local_box_lower','local_box_close_position','local_close_location','ma7m','ma20m','ma60m','ma7m_slope3_pct','ma20m_slope3_pct','ma60m_slope3_pct','above_ma7m_run','above_ma20m_run','above_ma60m_run','monthly_body_ratio','monthly_upper_wick_ratio','monthly_lower_wick_ratio','monthly_close_pos','monthly_ret3','monthly_ret6','monthly_ret12','monthly_pos12','monthly_pos24'];z=x[cols].copy()
 data={'schema_version':'tradex_monthly_only_selection_features_v1.compare.v1','artifact_role':'authoritative_feature_contract','review_only':True,'fixed_conditions':{'timing':'source completed calendar month becomes effective next month','moving_averages':['7 monthly','20 monthly','60 monthly'],'environment_inputs':'monthly only','weekly_inputs':[],'future_daily_outcomes':[]},'summary':{'rows':len(z),'codes':int(z.code.nunique()),'min_month':str(z.month.min()),'max_month':str(z.month.max()),'environment_counts':z.confirmed_environment.value_counts().to_dict()},'judgment':{'decision':'ready_for_teacher_environment_audit'},'not_changed':['daily entry states','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8');z.to_parquet(a.output/'monthly_selection_features.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'weekly_columns_used':[],'future_columns_used':[],'duplicates':int(z.duplicated(['code','effective_month']).sum()),'source_sha256':sha(a.monthly_layers)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'summary':data['summary']},ensure_ascii=False))
if __name__=='__main__':main()
