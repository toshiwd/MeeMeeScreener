from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_failed_high_retest_short_backtest_v1 import _atoms,_load_bars,_load_codes,_signal_for_bars

AXIS_ID='tradex_short_failed_high_current_scan_v1';OUT=Path(r'G:\Tradex\short_failed_high_current_scan_v1')
RULE={"peak_age>=120","peak_prominence>=0.03","pullback_depth>=0.2","stage=forming"}

def run()->Path:
 sys.path[:0]=[str(ROOT/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();db_path=Path(runtime['selected_runtime_db_path']);end=int(runtime['latest_confirmed_daily_bars_date_iso'].replace('-',''));start=20240101
 rows=[]
 with duckdb.connect(str(db_path),read_only=True) as db:
  for code,name in _load_codes(db,start,end):
   bars=_load_bars(db,code,start,end)
   if not bars or bars[-1]['date']!=end:continue
   signal=_signal_for_bars(bars,len(bars)-1)
   if signal is None or not RULE.issubset(_atoms({'signal':signal})):continue
   rows.append({'side':'sell','code':code,'name':name,'signal_date':runtime['latest_confirmed_daily_bars_date_iso'],'confirmed_close':bars[-1]['close'],'rule':'failed_high_retest','shape_score':signal['score'],'retest_ratio':signal['retest_ratio'],'peak_age':signal['peak_age'],'pullback_depth':signal['pullback_depth'],'entry_condition':'watch_next_session_rejection_or_low_break','automatic_trade':False})
 rows.sort(key=lambda row:(-row['shape_score'],row['code']))
 for rank,row in enumerate(rows,start=1):row['family_rank']=rank
 now=datetime.now(timezone.utc);output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";output.mkdir(parents=True)
 payload={'schema_version':f'{AXIS_ID}.board.v1','confirmed_as_of':runtime['latest_confirmed_daily_bars_date_iso'],'candidate_count':len(rows),'candidates':rows[:20],'runtime_db':str(db_path),'runtime_db_write':False,'production_ranking_changed':False}
 path=output/'current_scan.json';path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(path);return path

if __name__=='__main__':run()
