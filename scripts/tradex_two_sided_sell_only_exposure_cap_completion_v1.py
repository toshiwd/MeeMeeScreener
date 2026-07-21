from __future__ import annotations
import argparse,sys,json,hashlib
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha,_write

AXIS_ID='tradex_two_sided_sell_only_exposure_cap_completion_v1';OUT=Path(r'G:\Tradex\tradex_two_sided_sell_only_exposure_cap_completion_v1')
PIT=Path(r'G:\Tradex\tradex_two_sided_pit_allocation_v1\20260713T100616Z-tradex_two_sided_pit_allocation_v1')
RANKS=Path(r'G:\Tradex\tradex_two_sided_portfolio_union_v1\20260713T100321Z-tradex_two_sided_portfolio_union_v1')
FULL_BUY=Path(r'G:\Tradex\tradex_strict_pit_adaptive_buy_router_v1\20260713T091826Z-tradex_strict_pit_adaptive_buy_router_v1\all_symbol_daily_scores.parquet')
UPSTREAM_P60=Path(r'G:\Tradex\tradex_two_sided_sell_volume_cap_v1\20260713T122534Z-tradex_two_sided_sell_volume_cap_v1\compare.json')
BASE=Path(r'G:\Tradex\tradex_two_sided_sell_only_exposure_cap_v1\20260713T123546Z-tradex_two_sided_sell_only_exposure_cap_v1\compare.json')
BUY_LEDGER=PIT/'fixed_buy_ledger_with_exit_ymd.parquet';SELL_LEDGER=PIT/'fixed_sell_ledger_with_exit_ymd.parquet';MEEMEE_RANK=RANKS/'all_meemee_buy_ranks.parquet';SELL_RANK=RANKS/'all_support_break_sell_scores.parquet';CAP=2284.8
SCRIPT_PATH=Path(__file__).resolve();TEST_PATH=ROOT/'tests'/'test_tradex_two_sided_sell_only_exposure_cap_completion_v1.py'

def assert_cap(upstream:dict)->None:
 selected=float(upstream['selected_variant']['raw_volume_cap'])
 if abs(selected-CAP)>1e-9:raise AssertionError(f'cap mismatch: frozen={CAP}, upstream={selected}')

def build_buy_ranks(full:pd.DataFrame,meemee:pd.DataFrame)->pd.DataFrame:
 f=full[['signal_ymd','code','router_score','rank']].rename(columns={'rank':'full_buy_rank','router_score':'full_buy_score'})
 m=meemee[['signal_ymd','code','rank']].rename(columns={'rank':'meemee_source_rank'})
 x=f.merge(m,on=['signal_ymd','code'],how='outer',validate='one_to_one');x['overlay_priority']=x.meemee_source_rank.isna().astype(int)
 x=x.sort_values(['signal_ymd','overlay_priority','meemee_source_rank','full_buy_rank','code'],na_position='last',kind='stable');overlay=x.meemee_source_rank.notna();x['operational_rank']=x.groupby('signal_ymd').cumcount()+1;x['rank']=x.operational_rank;x['top10']=x['rank'].le(10);x['side']='BUY';x['rank_source']=overlay.map({True:'meemee_priority',False:'frozen_full_buy_score'})
 return x[['signal_ymd','code','rank','operational_rank','top10','side','rank_source','meemee_source_rank','full_buy_rank','full_buy_score']].reset_index(drop=True)

def assert_dense_rank(x:pd.DataFrame,rank_col:str='rank')->None:
 for d,q in x.groupby('signal_ymd',sort=False):
  if sorted(q[rank_col].astype(int).tolist())!=list(range(1,len(q)+1)):raise AssertionError(f'non-dense rank {d}')
  if len(q)>=10 and int(q[rank_col].le(10).sum())!=10:raise AssertionError(f'top10 count {d}')

def source_paths(db:Path)->dict[str,Path]:
 return {'runtime_db':db,'buy_ledger':BUY_LEDGER,'sell_ledger':SELL_LEDGER,'original_meemee_buy_rank':MEEMEE_RANK,'original_sell_rank':SELL_RANK,'full_buy_score':FULL_BUY,'upstream_p60_compare':UPSTREAM_P60,'base_compare':BASE,'completion_script':SCRIPT_PATH,'completion_test':TEST_PATH}

def generate(db:Path,out:Path)->Path:
 base=json.loads(BASE.read_text(encoding='utf-8'));up=json.loads(UPSTREAM_P60.read_text(encoding='utf-8'));assert_cap(up)
 buy_ledger=pd.read_parquet(BUY_LEDGER);sell_ledger=pd.read_parquet(SELL_LEDGER);full=pd.read_parquet(FULL_BUY);meemee=pd.read_parquet(MEEMEE_RANK);sell_rank=pd.read_parquet(SELL_RANK)
 all_buy=build_buy_ranks(full,meemee);all_sell=sell_rank.copy()
 if all_buy.duplicated(['signal_ymd','code']).any() or all_sell.duplicated(['signal_ymd','code']).any():raise AssertionError('duplicate daily symbol rank')
 assert_dense_rank(all_buy);assert_dense_rank(all_sell)
 buy_days=int(all_buy.signal_ymd.nunique());sell_days=int(all_sell.signal_ymd.nunique())
 if buy_days!=615 or sell_days!=615:raise AssertionError((buy_days,sell_days))
 overlay=meemee.merge(all_buy[['signal_ymd','code','rank','meemee_source_rank']],on=['signal_ymd','code'],validate='one_to_one')
 if not overlay['rank_x'].eq(overlay.meemee_source_rank).all():raise AssertionError('MeeMee source rank not preserved')
 relative_ok=all(g['rank_y'].is_monotonic_increasing for _,g in overlay.sort_values(['signal_ymd','rank_x']).groupby('signal_ymd'))
 if not relative_ok:raise AssertionError('MeeMee relative order not preserved')
 shared=full[['signal_ymd','code']].merge(all_sell[['signal_ymd','code']],on=['signal_ymd','code'],validate='one_to_one')
 if len(shared)!=len(all_sell):raise AssertionError('SELL full-score universe is not fully covered by frozen BUY score universe')
 with duckdb.connect(str(db),read_only=True) as q:max_date=int(q.execute("select max(cast(strftime(to_timestamp(date),'%Y%m%d') as int)) from daily_bars where source='pan'").fetchone()[0])
 if max_date!=20260710:raise AssertionError(f'DB max date changed: {max_date}')
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);all_buy.to_parquet(root/'all_buy_ranks.parquet',index=False);all_sell.to_parquet(root/'all_sell_ranks.parquet',index=False)
 provenance=[{'label':k,'path':str(p),'sha256':_sha(p)} for k,p in source_paths(db).items()]
 metric_bytes=json.dumps(base['metrics'],sort_keys=True,separators=(',',':')).encode();metric_sha=hashlib.sha256(metric_bytes).hexdigest()
 payload={'schema_version':AXIS_ID+'.compare.v2','artifact_role':'authoritative','research_phase':'effectiveness_judgment','completion_instrumentation_repair':{'strategy_frozen':True,'strategy_source':str(BASE),'changed_axis':'dense operational rank and provenance instrumentation only','buy_sell_ledgers_signals_returns_allocation_selected_cap_metrics_changed':False,'metrics_exact_equal_base':True,'base_metrics_sha256':metric_sha,'output_metrics_sha256':metric_sha,'frozen_sell_volume_cap':CAP,'upstream_cap_asserted_equal':True},'fixed_evaluation_conditions':base['fixed_evaluation_conditions'],'source_artifacts':provenance,'data_cutoffs':{'runtime_db_max_pan_date':max_date,'buy_completed_outcome_signal_cutoff':int(buy_ledger.signal_ymd.max()),'buy_completed_exit_cutoff':int(buy_ledger.exit_ymd.max()),'sell_completed_outcome_signal_cutoff':int(sell_ledger.signal_ymd.max()),'sell_completed_exit_cutoff':int(sell_ledger.exit_ymd.max())},'rank_contract':{'buy_days':buy_days,'sell_days':sell_days,'buy_min_per_day':int(all_buy.groupby('signal_ymd').size().min()),'buy_max_per_day':int(all_buy.groupby('signal_ymd').size().max()),'sell_min_per_day':int(all_sell.groupby('signal_ymd').size().min()),'sell_max_per_day':int(all_sell.groupby('signal_ymd').size().max()),'buy_rows':len(all_buy),'sell_rows':len(all_sell),'buy_duplicate_keys':0,'sell_duplicate_keys':0,'buy_dense_rank_every_day':True,'sell_dense_rank_every_day':True,'top10_count_10_every_eligible_day':True,'meemee_source_rows_checked':len(overlay),'meemee_source_rank_preserved':True,'meemee_relative_order_preserved':True,'numeric_operational_rank_equals_source_rank_claimed':False,'fallback_fills_top10_when_meemee_source_candidates_lt10':True,'shared_eligible_universe':'SELL full-score universe is an exact subset of frozen BUY full-score universe for all 615 PAN sessions','shared_rows':len(shared),'buy_operational_extra_rows':len(all_buy)-len(shared),'reason_counts_may_differ':'BUY includes frozen-score-only and MeeMee-priority overlay rows; SELL eligibility is the exact shared full-score subset'},'selected_variant':base['selected_variant'],'metrics':base['metrics'],'adoption_gates':base['adoption_gates'],'decision':base['decision'],'silent_fallback_used':False,'shadow_tuning_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p

def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
