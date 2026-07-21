from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.backend.services.market_watch_tags import NIKKEI_225_CODES


AXIS_ID = "tradex_nikkei225_daily_assessment_feature_ledger_v1"


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int, extra_codes: list[str] | None = None) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    parquet = output / "daily_assessment_features.parquet"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute("CREATE TEMP TABLE target_codes(code VARCHAR)")
        target_codes = sorted(set(NIKKEI_225_CODES) | {str(code).zfill(4) for code in (extra_codes or [])})
        conn.executemany("INSERT INTO target_codes VALUES (?)", [(code,) for code in target_codes])
        query = r"""
        WITH raw AS (
          SELECT CAST(b.code AS VARCHAR) code,
            CASE WHEN length(CAST(abs(b.date) AS VARCHAR))=8 THEN CAST(b.date AS INTEGER)
                 ELSE CAST(strftime(to_timestamp(CAST(b.date AS BIGINT)), '%Y%m%d') AS INTEGER) END ymd,
            CAST(b.o AS DOUBLE) o,CAST(b.h AS DOUBLE) h,CAST(b.l AS DOUBLE) l,CAST(b.c AS DOUBLE) c,CAST(b.v AS DOUBLE) v
          FROM daily_bars b JOIN target_codes t ON CAST(b.code AS VARCHAR)=t.code
          WHERE coalesce(b.source,'pan') <> 'yahoo' AND b.o>0 AND b.c>0 AND b.h>=greatest(b.o,b.c) AND b.l<=least(b.o,b.c)
        ), base AS (
          SELECT *,lag(c) OVER w pclose,lag(h) OVER w phigh,lag(l) OVER w plow,
            lag(o,4) OVER w open5,lag(o,19) OVER w open20,
            lag(c,3) OVER w close3,lag(c,5) OVER w close5,lag(c,10) OVER w close10,
            avg(c) OVER w7 ma7,avg(c) OVER w20 ma20,avg(c) OVER w60 ma60,
            avg(c) OVER w100 ma100,avg(c) OVER w200 ma200,avg(v) OVER w20 vol20,
            min(l) OVER wp20 support20,max(h) OVER wp20 resistance20,
            min(l) OVER w20 low20_inc,max(h) OVER w20 high20_inc,
            min(l) OVER w5 low5_inc,max(h) OVER w5 high5_inc,
            max(h) OVER wprev5 high5_previous,max(h) OVER wprev20 high20_previous,
            lead(c,1) OVER w c1,lead(c,3) OVER w c3,lead(c,5) OVER w c5,lead(c,10) OVER w c10,
            lead(l,1) OVER w low1_f,lead(h,1) OVER w high1_f,
            least(lead(l,1) OVER w,lead(l,2) OVER w,lead(l,3) OVER w) low3_f,
            least(lead(l,1) OVER w,lead(l,2) OVER w,lead(l,3) OVER w,lead(l,4) OVER w,lead(l,5) OVER w) low5_f,
            least(lead(l,1) OVER w,lead(l,2) OVER w,lead(l,3) OVER w,lead(l,4) OVER w,lead(l,5) OVER w,lead(l,6) OVER w,lead(l,7) OVER w,lead(l,8) OVER w,lead(l,9) OVER w,lead(l,10) OVER w) low10_f,
            greatest(lead(h,1) OVER w,lead(h,2) OVER w,lead(h,3) OVER w) high3_f,
            greatest(lead(h,1) OVER w,lead(h,2) OVER w,lead(h,3) OVER w,lead(h,4) OVER w,lead(h,5) OVER w) high5_f,
            greatest(lead(h,1) OVER w,lead(h,2) OVER w,lead(h,3) OVER w,lead(h,4) OVER w,lead(h,5) OVER w,lead(h,6) OVER w,lead(h,7) OVER w,lead(h,8) OVER w,lead(h,9) OVER w,lead(h,10) OVER w) high10_f
          FROM raw
          WINDOW w AS (PARTITION BY code ORDER BY ymd),w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
            w5 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
            w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
            w100 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),w200 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 199 PRECEDING AND CURRENT ROW),
            wp20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
            wprev5 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 9 PRECEDING AND 5 PRECEDING),
            wprev20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 39 PRECEDING AND 20 PRECEDING)
        ), staged AS (
          SELECT *,lag(ma7) OVER w ma7_prev,lag(ma20) OVER w ma20_prev,lag(ma60) OVER w ma60_prev,
            lag(ma7,5) OVER w ma7_5,lag(ma20,5) OVER w ma20_5,lag(ma60,5) OVER w ma60_5,
            avg(greatest(h-l,abs(h-pclose),abs(l-pclose))) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) atr14
          FROM base WINDOW w AS (PARTITION BY code ORDER BY ymd)
        ), candle AS (
          SELECT *,abs(c-o)/nullif(h-l,0) body_ratio,(h-greatest(o,c))/nullif(h-l,0) upper_wick_ratio,
            (least(o,c)-l)/nullif(h-l,0) lower_wick_ratio,(c-l)/nullif(h-l,0) close_pos,
            c/close3-1 ret3,c/close5-1 ret5,c/close10-1 ret10,
            sum(CASE WHEN c<o THEN 1 ELSE 0 END) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) bear_count5,
            sum(greatest(o-c,0)) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) bear_body5_abs,
            sum(CASE WHEN (h-greatest(o,c))/nullif(h-l,0)>=0.25 AND (c-l)/nullif(h-l,0)<=0.55 THEN 1 ELSE 0 END) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) upper_supply_count5,
            sum(CASE WHEN (least(o,c)-l)/nullif(h-l,0)>=0.35 AND (c-l)/nullif(h-l,0)>=0.60 THEN 1 ELSE 0 END) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) lower_rejection_count5,
            sum(CASE WHEN (c-l)/nullif(h-l,0)<=0.20 THEN 1 ELSE 0 END) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) low_close_count3
          FROM staged
        ), features AS (
          SELECT code,ymd,o,h,l,c,v,atr14,ma7,ma20,ma60,ma100,ma200,vol20,support20,resistance20,
            body_ratio,upper_wick_ratio,lower_wick_ratio,close_pos,ret3,ret5,ret10,
            c/close10-1 pre_ret10,(c-low20_inc)/nullif(high20_inc-low20_inc,0) pos20,
            (high20_inc-low20_inc)/c range20_pct,bear_count5,bear_body5_abs/nullif(atr14,0) bear_body5_atr,
            upper_supply_count5,lower_rejection_count5,low_close_count3,
            (c-ma7)/nullif(atr14,0) dist_ma7_atr,(c-ma20)/nullif(atr14,0) dist_ma20_atr,(c-ma60)/nullif(atr14,0) dist_ma60_atr,
            (ma7-ma7_5)/nullif(5*atr14,0) ma7_slope5_atr,(ma20-ma20_5)/nullif(5*atr14,0) ma20_slope5_atr,(ma60-ma60_5)/nullif(5*atr14,0) ma60_slope5_atr,
            v/nullif(vol20,0) volume_ratio20,
            CASE WHEN pclose>=ma7_prev AND c<ma7 THEN 1 ELSE 0 END cross_ma7,
            CASE WHEN pclose>=ma20_prev AND c<ma20 THEN 1 ELSE 0 END cross_ma20,
            CASE WHEN pclose<ma7_prev AND c>=ma7 THEN 1 ELSE 0 END reclaim_ma7,
            CASE WHEN pclose<ma20_prev AND c>=ma20 THEN 1 ELSE 0 END reclaim_ma20,
            CASE WHEN support20 IS NOT NULL AND c<support20 THEN 1 ELSE 0 END support_break,
            (support20-c)/nullif(atr14,0) support_break_depth_atr,
            CASE WHEN ret3<=-0.05 OR ret5<=-0.08 OR (c-ma7)/nullif(atr14,0)<=-1.5 OR (c-ma20)/nullif(atr14,0)<=-2 THEN 1 ELSE 0 END oversold_risk,
            CASE WHEN high5_inc<high5_previous AND c<close5 THEN 1 ELSE 0 END weekly_lower_high,
            (high5_inc-greatest(open5,c))/nullif(high5_inc-low5_inc,0) weekly_upper_wick_ratio,
            (c-low5_inc)/nullif(high5_inc-low5_inc,0) weekly_close_pos,
            CASE WHEN high20_inc>=high20_previous AND c<open20 AND (c-low20_inc)/nullif(high20_inc-low20_inc,0)<=0.40 THEN 1 ELSE 0 END monthly_high_failure,
            c1/c-1 ret_close_1,c3/c-1 ret_close_3,c5/c-1 ret_close_5,c10/c-1 ret_close_10,
            low1_f/c-1 down_exc_1,high1_f/c-1 up_exc_1,
            low3_f/c-1 down_exc_3,low5_f/c-1 down_exc_5,low10_f/c-1 down_exc_10,
            high3_f/c-1 up_exc_3,high5_f/c-1 up_exc_5,high10_f/c-1 up_exc_10
          FROM candle
          WHERE ymd BETWEEN $start_ymd AND $end_ymd AND ma200 IS NOT NULL
        ), market AS (
          SELECT ymd,
            avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) market_breadth_ma20,
            avg(CASE WHEN c>ma60 THEN 1.0 ELSE 0.0 END) market_breadth_ma60,
            avg(CASE WHEN c>pclose THEN 1.0 ELSE 0.0 END) market_advancers_ratio,
            avg(c/nullif(pclose,0)-1) market_mean_ret1
          FROM staged WHERE ymd BETWEEN $start_ymd AND $end_ymd GROUP BY ymd
        ) SELECT f.*,m.market_breadth_ma20,m.market_breadth_ma60,m.market_advancers_ratio,m.market_mean_ret1
          FROM features f JOIN market m USING(ymd) ORDER BY f.code,f.ymd
        """
        conn.execute(f"COPY ({query}) TO '{parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)", {"start_ymd": start_ymd, "end_ymd": end_ymd})
        summary = conn.execute(f"SELECT count(*) n,count(DISTINCT code) codes,min(ymd) min_ymd,max(ymd) max_ymd,count(*) FILTER (WHERE ret_close_10 IS NULL) horizon10_missing FROM read_parquet('{parquet.as_posix()}')").fetchone()
    finally:
        conn.close()
    stat = db_path.stat()
    audit = {"schema_version": f"{AXIS_ID}.audit.v1", "artifact_role": "authoritative", "generated_at": datetime.now(timezone.utc).isoformat(), "source_db": str(db_path), "source_db_fingerprint": {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}, "fixed_conditions": {"universe": "current Nikkei225 registry plus explicit teacher codes; survivorship-biased", "extra_teacher_codes": sorted(set(extra_codes or [])), "start_ymd": start_ymd, "end_ymd": end_ymd, "source": "confirmed non-yahoo bars", "feature_time": "t or earlier", "future_columns": "labels only"}, "output_parquet": str(parquet), "rows": summary[0], "codes": summary[1], "min_ymd": summary[2], "max_ymd": summary[3], "horizon10_missing": summary[4], "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}}
    (output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "audit": str(output / "audit.json"), "parquet": str(parquet)}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--db", required=True, type=Path); parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1")); parser.add_argument("--start-ymd", type=int, default=20190101); parser.add_argument("--end-ymd", type=int, default=20260713); parser.add_argument("--extra-code", action="append", default=[]); args=parser.parse_args(); print(run(args.db,args.output_root,args.start_ymd,args.end_ymd,args.extra_code))


if __name__ == "__main__": main()
