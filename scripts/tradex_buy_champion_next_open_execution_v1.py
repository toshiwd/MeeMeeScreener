from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "tradex_buy_champion_next_open_execution_v1"
DEFAULT_TRADES = Path(r"G:\Tradex\buy_meemee_lower_wick_champion_readiness_v1\20260618T142218Z-buy_meemee_lower_wick_champion_readiness_v1\champion_trades.csv")
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\buy_champion_next_open_execution_v1")
STOP = 0.03
HOLD = 20


def _pf(values: pd.Series) -> float | None:
    gains=float(values[values>0].sum()); losses=float(-values[values<0].sum())
    return gains/losses if losses else None


def _summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    values=frame[column].dropna(); losses=values[values<0]
    return {"n":int(len(values)),"expectancy":float(values.mean()) if len(values) else None,"profit_factor":_pf(values),
      "win_rate":float((values>0).mean()) if len(values) else None,"stop_rate":float((frame[f"{column}_stop"]==True).mean()) if len(frame) else None,
      "loss_mean":float(losses.mean()) if len(losses) else None,"p05":float(values.quantile(.05)) if len(values) else None,
      "pnl_yen_at_2m":int(round(float(values.sum())*2_000_000)) if len(values) else 0}


def _simulate(path: pd.DataFrame, entry: float) -> tuple[float, bool, int]:
    for offset,row in enumerate(path.itertuples(index=False),start=1):
        if float(row.l) <= entry*(1-STOP): return -STOP,True,offset
    return float(path.iloc[-1].c)/entry-1,False,len(path)


def run(trades_path: Path, db_path: Path, output_root: Path) -> Path:
    source=pd.read_csv(trades_path,dtype={"code":str}); source["ymd"]=source["ymd"].astype(int)
    codes=sorted(source.code.unique().tolist()); placeholders=",".join(["?"]*len(codes))
    with duckdb.connect(str(db_path),read_only=True) as conn:
        bars=conn.execute(f"""SELECT code,CASE WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER) ELSE CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) END ymd,o,h,l,c FROM daily_bars WHERE source='pan' AND code IN ({placeholders}) ORDER BY code,ymd""",codes).fetchdf()
    by_code={str(code):group.reset_index(drop=True) for code,group in bars.groupby("code")}
    rows=[]
    for trade in source.to_dict("records"):
        history=by_code.get(str(trade["code"])); future=history[history.ymd>int(trade["ymd"])].head(HOLD) if history is not None else pd.DataFrame()
        if len(future)<HOLD: continue
        close_entry=float(trade["entry_close"]); next_open=float(future.iloc[0].o)
        close_ret,close_stop,close_days=_simulate(future,close_entry); open_ret,open_stop,open_days=_simulate(future,next_open)
        rows.append({"code":str(trade["code"]),"signal_ymd":int(trade["ymd"]),"next_entry_ymd":int(future.iloc[0].ymd),
          "signal_close":close_entry,"next_open":next_open,"gap_pct":next_open/close_entry-1,
          "signal_close_return":close_ret,"signal_close_return_stop":close_stop,"signal_close_days":close_days,
          "next_open_return":open_ret,"next_open_return_stop":open_stop,"next_open_days":open_days})
    frame=pd.DataFrame(rows); run_dir=output_root/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run_dir.mkdir(parents=True)
    close_summary=_summary(frame,"signal_close_return"); open_summary=_summary(frame,"next_open_return")
    delta={key:(open_summary.get(key)-close_summary.get(key) if isinstance(open_summary.get(key),(int,float)) and isinstance(close_summary.get(key),(int,float)) else None) for key in ("expectancy","profit_factor","win_rate","stop_rate","loss_mean","pnl_yen_at_2m")}
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":datetime.now(timezone.utc).isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"comparison_stabilization",
      "authoritative_input":str(trades_path),"fixed_evaluation_conditions":{"candidate_set":"accepted trades from existing MA60-slope/lower-wick champion","changed_axis":"execution only","reference_entry":"signal close","challenger_entry":"next-session open","stop":STOP,"max_holding_sessions":HOLD,"costs":"not modeled"},
      "reference_signal_close":close_summary,"challenger_next_open":open_summary,"delta_next_open_minus_close":delta,
      "decision":{"candidate_local_decision":"keep" if (open_summary["profit_factor"] or 0)>(close_summary["profit_factor"] or 0) and (open_summary["stop_rate"] or 1)<(close_summary["stop_rate"] or 1) else "drop","authoritative_rollup_decision":"research_only","reason":"next-open must improve PF and reduce stop rate on the fixed accepted trade set"},
      "limitations":["accepted trades are fixed from the close-entry portfolio; slot occupancy is not re-optimized","2026 rows without 20 completed sessions are excluded"],"runtime_db_write":False,"production_ranking_changed":False,"silent_fallback_used":False}
    frame.to_csv(run_dir/"trades.csv",index=False); (run_dir/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(run_dir/"compare.json"); return run_dir


def main()->int:
    p=argparse.ArgumentParser(); p.add_argument("--trades",type=Path,default=DEFAULT_TRADES); p.add_argument("--db",type=Path,default=DEFAULT_DB); p.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT_ROOT); a=p.parse_args(); run(a.trades,a.db,a.output_root); return 0


if __name__=="__main__": raise SystemExit(main())
