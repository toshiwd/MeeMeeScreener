"""Build a full-universe, outcome-first short-term decline event inventory."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--start", default="2019-01-01")
    ap.add_argument("--end", default="2026-06-30")
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    daily_path = args.output / "daily_feature_outcome_inventory.parquet"
    episode_path = args.output / "decline_episode_inventory.parquet"
    conn = duckdb.connect(str(args.db), read_only=True)
    try:
        source = f"""
        WITH raw AS (
          SELECT code::VARCHAR AS code, date,
                 CAST(o AS DOUBLE) o, CAST(h AS DOUBLE) h,
                 CAST(l AS DOUBLE) l, CAST(c AS DOUBLE) c, CAST(v AS DOUBLE) v
          FROM daily_bars
          WHERE lower(coalesce(source, '')) = 'pan'
        ), windows AS (
          SELECT *,
            row_number() OVER w AS bar_index,
            lag(o,1) OVER w p1_o, lag(h,1) OVER w p1_h, lag(l,1) OVER w p1_l, lag(c,1) OVER w p1_c,
            lag(o,2) OVER w p2_o, lag(h,2) OVER w p2_h, lag(l,2) OVER w p2_l, lag(c,2) OVER w p2_c,
            lag(c,3) OVER w c3, lag(c,5) OVER w c5, lag(c,10) OVER w c10,
            lag(c,20) OVER w c20, lag(c,40) OVER w c40, lag(c,60) OVER w c60, lag(c,120) OVER w c120,
            avg(c) OVER w20 ma20, avg(c) OVER w60 ma60,
            avg(v) OVER w20 avg_v20,
            max(h) OVER w20 high20, min(l) OVER w20 low20,
            max(h) OVER w60 high60, min(l) OVER w60 low60,
            max(h) OVER w120 high120, min(l) OVER w120 low120,
            lead(o,1) OVER w entry_open,
            min(l) OVER f3 future_low3, max(h) OVER f3 future_high3,
            min(l) OVER f5 future_low5, max(h) OVER f5 future_high5,
            min(l) OVER f10 future_low10, max(h) OVER f10 future_high10,
            min(l) OVER f20 future_low20, max(h) OVER f20 future_high20,
            lead(c,3) OVER w future_close3, lead(c,5) OVER w future_close5,
            lead(c,10) OVER w future_close10, lead(c,20) OVER w future_close20,
            lead(l,1) OVER w l1, lead(l,2) OVER w l2, lead(l,3) OVER w l3,
            lead(l,4) OVER w l4, lead(l,5) OVER w l5,
            lead(h,1) OVER w h1, lead(h,2) OVER w h2, lead(h,3) OVER w h3,
            lead(h,4) OVER w h4, lead(h,5) OVER w h5
          FROM raw
          WINDOW
            w AS (PARTITION BY code ORDER BY date),
            w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
            w120 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW),
            f3 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING),
            f5 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING),
            f10 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING),
            f20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING)
        ), shaped AS (
          SELECT *,
            CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) ymd,
            c / nullif(p1_c,0) - 1 ret1, c / nullif(c3,0) - 1 ret3,
            c / nullif(c5,0) - 1 ret5, c / nullif(c10,0) - 1 ret10,
            c / nullif(c20,0) - 1 ret20, c / nullif(c40,0) - 1 ret40,
            c / nullif(c60,0) - 1 ret60, c / nullif(c120,0) - 1 ret120,
            abs(c-o) / nullif(h-l,0) body_ratio,
            (c-l) / nullif(h-l,0) close_position,
            (h-greatest(o,c)) / nullif(h-l,0) upper_wick_ratio,
            (least(o,c)-l) / nullif(h-l,0) lower_wick_ratio,
            CASE WHEN c<o THEN -1 WHEN c>o THEN 1 ELSE 0 END candle_direction,
            v / nullif(avg_v20,0) volume_ratio20,
            c / nullif(ma20,0) - 1 close_vs_ma20,
            c / nullif(ma60,0) - 1 close_vs_ma60,
            high20 / nullif(low20,0) - 1 range20,
            high60 / nullif(low60,0) - 1 range60,
            high120 / nullif(low120,0) - 1 range120,
            c / nullif(high20,0) - 1 dist_high20, c / nullif(low20,0) - 1 dist_low20,
            c / nullif(high60,0) - 1 dist_high60, c / nullif(low60,0) - 1 dist_low60,
            c / nullif(high120,0) - 1 dist_high120, c / nullif(low120,0) - 1 dist_low120,
            (h < p1_h)::INTEGER lower_high_1,
            (l < p1_l)::INTEGER lower_low_1,
            (h < p1_h AND p1_h < p2_h)::INTEGER lower_high_2,
            (l < p1_l AND p1_l < p2_l)::INTEGER lower_low_2,
            (h >= p1_h AND l <= p1_l)::INTEGER outside_bar,
            (h <= p1_h AND l >= p1_l)::INTEGER inside_bar,
            (c < p1_l)::INTEGER close_below_prev_low,
            (c > p1_h)::INTEGER close_above_prev_high,
            (c < o AND p1_c < p1_o)::INTEGER two_bearish,
            (c > o AND p1_c > p1_o)::INTEGER two_bullish,
            100.0*(future_low3/nullif(entry_open,0)-1) low3_pct,
            100.0*(future_high3/nullif(entry_open,0)-1) high3_pct,
            100.0*(future_low5/nullif(entry_open,0)-1) low5_pct,
            100.0*(future_high5/nullif(entry_open,0)-1) high5_pct,
            100.0*(future_low10/nullif(entry_open,0)-1) low10_pct,
            100.0*(future_high10/nullif(entry_open,0)-1) high10_pct,
            100.0*(future_low20/nullif(entry_open,0)-1) low20_pct,
            100.0*(future_high20/nullif(entry_open,0)-1) high20_pct,
            100.0*(future_close3/nullif(entry_open,0)-1) close3_pct,
            100.0*(future_close5/nullif(entry_open,0)-1) close5_pct,
            100.0*(future_close10/nullif(entry_open,0)-1) close10_pct,
            100.0*(future_close20/nullif(entry_open,0)-1) close20_pct,
            least(
              CASE WHEN l1 <= entry_open*.95 THEN 1 ELSE 99 END,
              CASE WHEN l2 <= entry_open*.95 THEN 2 ELSE 99 END,
              CASE WHEN l3 <= entry_open*.95 THEN 3 ELSE 99 END,
              CASE WHEN l4 <= entry_open*.95 THEN 4 ELSE 99 END,
              CASE WHEN l5 <= entry_open*.95 THEN 5 ELSE 99 END
            ) first_drop5_day,
            least(
              CASE WHEN h1 >= entry_open*1.03 THEN 1 ELSE 99 END,
              CASE WHEN h2 >= entry_open*1.03 THEN 2 ELSE 99 END,
              CASE WHEN h3 >= entry_open*1.03 THEN 3 ELSE 99 END,
              CASE WHEN h4 >= entry_open*1.03 THEN 4 ELSE 99 END,
              CASE WHEN h5 >= entry_open*1.03 THEN 5 ELSE 99 END
            ) first_rise3_day
          FROM windows
        )
        SELECT * EXCLUDE(date, future_low3, future_high3, future_low5, future_high5,
                         future_low10, future_high10, future_low20, future_high20,
                         future_close3, future_close5, future_close10, future_close20,
                         l1,l2,l3,l4,l5,h1,h2,h3,h4,h5),
          (low3_pct <= -3)::INTEGER drop3_in3,
          (low5_pct <= -5)::INTEGER drop5_in5,
          (low10_pct <= -8)::INTEGER drop8_in10,
          (low20_pct <= -10)::INTEGER drop10_in20,
          (first_drop5_day < first_rise3_day)::INTEGER clean_drop5_in5
        FROM shaped
        WHERE to_timestamp(date) >= CAST('{args.start}' AS TIMESTAMP)
          AND to_timestamp(date) <= CAST('{args.end}' AS TIMESTAMP)
          AND bar_index >= 121 AND entry_open > 0 AND future_close20 IS NOT NULL
          AND c >= 100 AND c < 100000 AND v > 0 AND h > l
        """
        conn.execute(
            f"COPY ({source}) TO '{sql_path(daily_path)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
        )
        episode_sql = f"""
        WITH targets AS (
          SELECT *, lag(bar_index) OVER (PARTITION BY code ORDER BY bar_index) prior_target_index
          FROM read_parquet('{sql_path(daily_path)}')
          WHERE drop5_in5 = 1
        ), marked AS (
          SELECT *, CASE WHEN prior_target_index IS NULL OR bar_index-prior_target_index>5 THEN 1 ELSE 0 END new_episode
          FROM targets
        ), grouped AS (
          SELECT *, sum(new_episode) OVER (PARTITION BY code ORDER BY bar_index) episode_number
          FROM marked
        ), ranked AS (
          SELECT *, row_number() OVER (PARTITION BY code, episode_number ORDER BY bar_index) episode_signal_ordinal,
                    count(*) OVER (PARTITION BY code, episode_number) episode_signal_count,
                    min(ymd) OVER (PARTITION BY code, episode_number) episode_start_ymd
          FROM grouped
        )
        SELECT * FROM ranked WHERE episode_signal_ordinal=1
        """
        conn.execute(
            f"COPY ({episode_sql}) TO '{sql_path(episode_path)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)"
        )

        daily = conn.execute(f"""
          SELECT count(*) n, count(distinct code) codes, min(ymd) min_ymd, max(ymd) max_ymd,
                 avg(drop3_in3) drop3_in3_rate, avg(drop5_in5) drop5_in5_rate,
                 avg(drop8_in10) drop8_in10_rate, avg(drop10_in20) drop10_in20_rate
          FROM read_parquet('{sql_path(daily_path)}')
        """).fetchone()
        episodes = conn.execute(f"""
          SELECT count(*) n, count(distinct code) codes,
                 median(low5_pct) median_low5_pct, median(high5_pct) median_high5_pct,
                 avg(clean_drop5_in5) clean_drop5_rate,
                 count(*) FILTER (WHERE ymd < 20240101) development_n,
                 count(*) FILTER (WHERE ymd >= 20240101) validation_n
          FROM read_parquet('{sql_path(episode_path)}')
        """).fetchone()
        strength = conn.execute(f"""
          SELECT CASE WHEN low5_pct<=-12 THEN 'le_-12'
                      WHEN low5_pct<=-8 THEN '-8_to_-12'
                      ELSE '-5_to_-8' END strength_band,
                 count(*) n, count(distinct code) codes,
                 median(low5_pct) median_low5_pct, median(high5_pct) median_high5_pct,
                 avg(clean_drop5_in5) clean_drop5_rate
          FROM read_parquet('{sql_path(episode_path)}') GROUP BY 1 ORDER BY min(low5_pct)
        """).fetchall()
    finally:
        conn.close()

    result = {
        "schema_version": "tradex_short_decline_event_inventory_v1.compare.v1",
        "artifact_role": "authoritative_full_universe_short_decline_event_inventory",
        "review_only": True,
        "research_phase": "infrastructure_stabilization",
        "fixed_conditions": {
            "universe": "all PAN daily bars with >=120 prior bars; 100<=close<100000; volume>0",
            "period": f"{args.start} through {args.end}",
            "entry_reference": "next session open",
            "primary_event": "future intraday low reaches -5% within 5 sessions",
            "episode_deduplication": "same code target anchors separated by <=5 trading bars share one episode; earliest anchor retained",
            "diagnostic_outcomes": ["-3%/3d", "-5%/5d", "-8%/10d", "-10%/20d"],
            "costs": "ignored",
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "daily_inventory": dict(zip(["n","codes","min_ymd","max_ymd","drop3_in3_rate","drop5_in5_rate","drop8_in10_rate","drop10_in20_rate"], daily)),
            "episode_inventory": dict(zip(["n","codes","median_low5_pct","median_high5_pct","clean_drop5_rate","development_n","validation_n"], episodes)),
            "strength_bands": [
                dict(zip(["strength_band","n","codes","median_low5_pct","median_high5_pct","clean_drop5_rate"], row))
                for row in strength
            ],
            "gate_checks": {
                "daily_n_ge_500000": daily[0] >= 500000,
                "episode_n_ge_10000": episodes[0] >= 10000,
                "episode_codes_ge_300": episodes[1] >= 300,
                "development_n_ge_5000": episodes[5] >= 5000,
                "validation_n_ge_2000": episodes[6] >= 2000,
            },
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(episodes[0]),
            "selection_divergence_reason": "outcome-first full-universe episode discovery without the current selector",
            "daily_rows": int(daily[0]),
            "episode_rows": int(episodes[0]),
        },
        "judgment": {
            "candidate_local_decision": "keep" if all([daily[0]>=500000, episodes[0]>=10000, episodes[1]>=300, episodes[5]>=5000, episodes[6]>=2000]) else "hold",
            "session_aggregate_decision": "keep_decline_event_inventory" if episodes[0]>=10000 else "hold_decline_event_inventory",
            "authoritative_rollup_decision": "keep_full_universe_decline_inventory_v1_review_only" if episodes[0]>=10000 else "hold_rebuild_inventory",
            "reason_type": "broad_outcome_first_sample_established" if episodes[0]>=10000 else "insufficient_episode_breadth",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production logic", "entry selector", "position management"],
        "remaining_risks": [
            "episode anchors are earliest hindsight labels, not yet tradable entry signals",
            "monthly range age and market-relative features are not yet attached",
            "corporate actions and extreme prints require later robustness checks",
            "daily OHLC cannot determine intraday order beyond first-hit day comparisons",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"db": {"path": str(args.db.resolve()), "read_only": True}},
        "artifacts": {
            "daily": {"path": str(daily_path), "sha256": sha(daily_path)},
            "episodes": {"path": str(episode_path), "sha256": sha(episode_path)},
            "compare": {"path": str(compare), "sha256": sha(compare)},
        },
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
