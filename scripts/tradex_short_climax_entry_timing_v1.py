from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


AXIS_ID = "tradex_short_climax_entry_timing_v1"
OUT = Path(r"G:\Tradex\short_climax_entry_timing_v1")
ROOT = Path(__file__).resolve().parents[1]
MODES = ("signal_close", "next_open", "signal_low_break")


def simulate(row: dict, mode: str) -> dict | None:
    if mode == "signal_close": entry, start = float(row["c"]), 1
    elif mode == "next_open": entry, start = float(row["o1"]), 1
    else:
        entry = float(row["l"])
        start = next((i for i in range(1, 6) if float(row[f"l{i}"]) <= entry), 99)
        if start == 99: return None
    stop, target = entry * 1.08, entry * .90
    for day in range(start, 6):
        hit_stop = float(row[f"h{day}"]) >= stop
        hit_target = float(row[f"l{day}"]) <= target
        if hit_stop: return {"ret": -.08, "exit_day": day, "exit_reason": "stop"}
        if hit_target: return {"ret": .10, "exit_day": day, "exit_reason": "target"}
    return {"ret": entry / float(row["c5"]) - 1, "exit_day": 5, "exit_reason": "time"}


def summary(frame: pd.DataFrame) -> dict:
    if frame.empty: return {"n":0,"expectancy":None,"profit_factor":None,"win_rate":None,"trigger_rate":None}
    gains=float(frame.ret[frame.ret>0].sum());loss=float(-frame.ret[frame.ret<0].sum())
    return {"n":len(frame),"signal_count":int(frame.signal_id.nunique()),"expectancy":float(frame.ret.mean()),"profit_factor":gains/loss if loss else None,"win_rate":float((frame.ret>0).mean()),"trigger_rate":float(frame.signal_id.nunique()/frame.total_signals.iloc[0]),"stop_rate":float((frame.exit_reason=='stop').mean())}


def adaptive_timing(events: pd.DataFrame) -> pd.DataFrame:
    selected=[]
    dates=sorted(events.signal_date.unique())
    for date in dates:
        known=events[events.signal_date < pd.Timestamp(date)-pd.Timedelta(days=12)]
        candidates=[]
        for mode in MODES:
            recent=known[known['mode']==mode].sort_values('signal_date').tail(60)
            m=summary(recent) if len(recent) else {"n":0,"profit_factor":None,"expectancy":None}
            if m['n']>=30 and (m['profit_factor'] or 0)>=1.0 and (m['expectancy'] or 0)>0:
                candidates.append((float(m['profit_factor']),float(m['expectancy']),mode))
        if not candidates: continue
        mode=max(candidates)[2]
        day=events[(events.signal_date==date)&(events['mode']==mode)].copy()
        day['selected_mode']=mode;selected.append(day)
    return pd.concat(selected,ignore_index=True) if selected else events.iloc[:0].copy()


def run() -> Path:
    sys.path[:0]=[str(ROOT),str(ROOT/'app')]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status();db_path=Path(runtime['selected_runtime_db_path'])
    leads=','.join([f"lead(o,{i}) over w o{i},lead(h,{i}) over w h{i},lead(l,{i}) over w l{i},lead(c,{i}) over w c{i}" for i in range(1,6)])
    sql=f"""WITH b AS(SELECT code,date,o,h,l,c,v,lag(c,20) over w c20,avg(v) over(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) av20,{leads} FROM daily_bars WHERE source='pan' WINDOW w AS(PARTITION BY code ORDER BY date)),s AS(SELECT *,c/c20-1 ret20,v/av20 volume_ratio,(c-l)/nullif(h-l,0) close_pos,row_number() OVER(PARTITION BY date ORDER BY v/av20 DESC,c/c20 DESC,code) family_rank FROM b WHERE c20>0 AND av20>0 AND c5 IS NOT NULL AND c/c20-1>=.20 AND v/av20>=3 AND c<o AND (c-l)/nullif(h-l,0)<=.35) SELECT * FROM s WHERE family_rank<=3 AND CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) BETWEEN 20190101 AND 20260710 ORDER BY date,code"""
    with duckdb.connect(str(db_path),read_only=True) as db: raw=db.execute(sql).fetchdf()
    rows=[];total=len(raw)
    for signal_id,item in enumerate(raw.to_dict('records')):
        ymd=int(pd.to_datetime(int(item['date']),unit='s').strftime('%Y%m%d'))
        for mode in MODES:
            result=simulate(item,mode)
            if result is not None: rows.append({"signal_id":signal_id,"code":str(item['code']),"ymd":ymd,"signal_date":pd.to_datetime(str(ymd)),"year":ymd//10000,"mode":mode,"total_signals":total,**result})
    events=pd.DataFrame(rows)
    reports=[]
    for mode in MODES:
        part=events[events['mode']==mode]
        reports.append({"mode":mode,"development_2019_2025":summary(part[part.year<=2025]),"diagnostic_2026":summary(part[part.year==2026]),"yearly":{str(int(y)):summary(g) for y,g in part.groupby('year')}})
    eligible=[row for row in reports if (row['development_2019_2025']['profit_factor'] or 0)>=1 and (row['development_2019_2025']['expectancy'] or 0)>0]
    selected=max(eligible,key=lambda row:row['development_2019_2025']['profit_factor'])['mode'] if eligible else None
    adaptive=adaptive_timing(events)
    adaptive_report={"development_2019_2025":summary(adaptive[adaptive.year<=2025]),"diagnostic_2026":summary(adaptive[adaptive.year==2026]),"mode_counts":adaptive.selected_mode.value_counts().to_dict() if not adaptive.empty else {}}
    now=datetime.now(timezone.utc);output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";output.mkdir(parents=True)
    events.to_csv(output/'timing_events.csv',index=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","fixed_shape":"ret20>=20%; volume>=3x; bearish; close_pos<=35%; top3/day","common_exit":{"tp":.10,"sl":.08,"horizon":5,"same_bar":"stop_first"},"reports":reports,"adaptive_point_in_time":adaptive_report,"selection":{"protocol":"fixed mode needs development positive expectancy; adaptive mode uses only outcomes known 12+ calendar days earlier and recent60 n>=30 PF>=1 positive expectancy","selected_fixed_mode":selected,"current_diagnostic_preferred_mode":"next_open"},"runtime_db":str(db_path),"runtime_db_write":False,"production_ranking_changed":False}
    path=output/'compare.json';path.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=lambda v:None if isinstance(v,float) and not math.isfinite(v) else str(v))+'\n',encoding='utf-8');print(path);return path


if __name__=='__main__':run()
