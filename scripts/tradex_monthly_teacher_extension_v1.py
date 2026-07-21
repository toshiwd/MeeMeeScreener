import argparse,hashlib,json,sys
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parent))
from tradex_monthly_env_probe_add_oos_v1 import monthly_environment
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--daily',type=Path,required=True);p.add_argument('--base-monthly',type=Path,required=True);p.add_argument('--teacher-code',action='append',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
 codes={str(c).zfill(4) for c in a.teacher_code};d=pd.read_parquet(a.daily);d.code=d.code.astype(str).str.zfill(4);teacher=monthly_environment(d[d.code.isin(codes)].copy());teacher.code=teacher.code.astype(str).str.zfill(4)
 base=pd.read_parquet(a.base_monthly);base.code=base.code.astype(str).str.zfill(4);base=base[~base.code.isin(codes)]
 common=list(base.columns);missing=[c for c in common if c not in teacher.columns]
 if missing:raise ValueError(f'teacher monthly missing columns: {missing}')
 combined=pd.concat([base,teacher[common]],ignore_index=True).sort_values(['code','effective_month']);dupes=int(combined.duplicated(['code','effective_month']).sum())
 teacher['source_month']=teacher.source_month.astype(str);teacher['effective_month']=teacher.effective_month.astype(str)
 combined['source_month']=combined.source_month.astype(str);combined['effective_month']=combined.effective_month.astype(str)
 july=teacher[(teacher.code.eq('9962'))&teacher.effective_month.eq('2026-07')][['code','source_month','effective_month','environment','post_box','box_reentry','breakout_age','box_pos','local_box_mature']]
 data={'schema_version':'tradex_monthly_teacher_extension_v1.compare.v1','artifact_role':'authoritative_infrastructure','review_only':True,'axis':'extend fixed Nikkei225-centered universe with explicit teacher codes','teacher_codes':sorted(codes),'teacher_rows':len(teacher),'combined_rows':len(combined),'duplicate_code_effective_month':dupes,'anchor_9962_202607':july.where(pd.notna(july),None).to_dict('records'),'judgment':{'decision':'keep_infrastructure' if dupes==0 and len(july)==1 else 'drop','reason':'teacher must receive a PIT monthly environment without altering existing universe rows'},'not_changed':['monthly classifier','existing Nikkei225 rows','entry logic','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');combined.to_parquet(a.output/'monthly_environment_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'daily_sha256':sha(a.daily),'base_monthly_sha256':sha(a.base_monthly),'future_used':False,'duplicates':dupes},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8');print(json.dumps(data,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
