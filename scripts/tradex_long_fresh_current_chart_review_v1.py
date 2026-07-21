from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb

VERDICTS={
 '6724':('Starter','buy candidate','monthly advance, weekly pullback, daily close above MA20 with long lower wick'),
 '6971':('Watch','wait','monthly uptrend remains but daily close is below MA7 and MA20'),
 '5301':('Watch','wait','monthly May surge is correcting; daily is only holding near MA60'),
 '6762':('Wait','wait','second monthly decline after May spike; below daily MA7, MA20 and MA60'),
 '6506':('Wait','wait','large bearish July monthly bar; daily remains below MA7, MA20 and MA60'),
 '7220':('Avoid','avoid','May-June vertical spike and collapse; still below all major daily averages'),
 '7803':('Avoid','avoid chase','July vertical breakout at 95% of one-year range and 30% above MA20'),
}
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--board',type=Path,required=True);p.add_argument('--db',type=Path,required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);board=json.loads(a.board.read_text(encoding='utf-8'));sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.chart_reading_bundle import get_chart_reading_bundle
 rows=[]
 with duckdb.connect(str(a.db),read_only=True) as c:
  for source in board['authoritative_result']['rows']:
   code=str(source['code']);b=get_chart_reading_bundle(c,code=code,as_of_date=board['latest_as_of']);daily=b['chart_context']['daily'];weekly=b['chart_context']['weekly'];monthly=b['chart_context']['monthly'];status,action,reason=VERDICTS[code];rows.append({'code':code,'stock_name':source['stock_name'],'tradex_family':source['family'],'status':status,'new_entry':action,'confidence':'medium' if status=='Starter' else 'high','reason':reason,'proposed_weight_pct':source['proposed_weight_pct'] if status=='Starter' else 0.0,'daily_selected':daily['selected_bar'],'weekly_selected':weekly['selected_bar'],'monthly_selected':monthly['selected_bar'],'monthly_bar_count':monthly['bar_count'],'held':b['position_state'] is not None})
 starters=[x for x in rows if x['status']=='Starter'];checks={'source_board_ready_review_only':board['judgment']['authoritative_rollup_decision']=='READY_REVIEW_ONLY','all_candidates_reviewed':len(rows)==board['authoritative_result']['candidate_count'],'all_monthly_context_available':all(x['monthly_bar_count']>0 for x in rows),'no_current_holdings':not any(x['held'] for x in rows),'single_starter_only':len(starters)==1 and starters[0]['code']=='6724'};decision='READY_REVIEW_ONLY' if all(checks.values()) else 'HOLD';payload={'schema_version':'tradex_long_fresh_current_chart_review_v1.review.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'latest_as_of':board['latest_as_of'],'source_board':str(a.board),'fixed_review_order':['monthly','weekly','daily_1y_position','current_position','last_5_daily_candles'],'authoritative_result':{'starter_count':len(starters),'watch_wait_avoid_count':len(rows)-len(starters),'rows':rows,'checks':checks},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'confirmed_multitimeframe_chart_review'},'remaining_risks':['Starter is not an automatic validated buy; next open gap is unknown','chart review labels are operational confirmation and do not change TRADEX ranking']}
 (out/'chart_review.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'chart_review.json'}),encoding='utf-8');print(json.dumps({'decision':decision,'starters':starters,'checks':checks},ensure_ascii=False,default=str))
if __name__=='__main__':main()
