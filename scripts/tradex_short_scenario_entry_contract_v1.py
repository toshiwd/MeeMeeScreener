from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_scenario_entry_contract_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_scenario_entry_contract_v1")


TARGETS = [
    ("strict_high_zone_ma_stall", "first_major_break", "高値圏MA失速 x 上昇後の初回大崩れ"),
    ("strict_high_zone_ma_stall", "extended_no_pullback", "高値圏MA失速 x 長期上昇で深い押しなし"),
    ("strict_high_zone_ma_stall", "long_uptrend_mature", "高値圏MA失速 x 上昇開始から長く大崩れなし"),
    ("strict_box_upper_failure", "post_major_break_retest", "ボックス上限失敗 x 大崩れ後の戻り"),
    ("strict_monthly_extension_break", "first_major_break", "月足伸び切り崩れ x 上昇後の初回大崩れ"),
    ("strict_ma20_rejection_return", "post_major_break_retest", "戻り売り20MA拒否 x 大崩れ後の戻り"),
]


ACCEPTANCE = {
    "min_n": 80,
    "min_take3pct_low5_rate": 0.60,
    "max_stopped3pct_high5_rate": 0.30,
    "min_risk_adjusted_capture_ratio": 2.0,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _default_db_path() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    if local:
        return Path(local) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    return Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")


def _build_table(con: duckdb.DuckDBPyConnection, start: str, end: str, *, require_forward_labels: bool = True) -> None:
    # This intentionally duplicates the context feature contract so the artifact is self-contained.
    # Keep it research-only until a smaller shared feature contract is promoted.
    forward_label_where = "AND f.c_f20 IS NOT NULL AND f.after_confirm_o IS NOT NULL" if require_forward_labels else ""
    con.execute(
        f"""
CREATE OR REPLACE TEMP TABLE short_entry_contract_events AS
WITH pan AS (
  SELECT code, date, o, h, l, c, v
  FROM daily_bars
  WHERE source = 'pan'
    AND date BETWEEN epoch(date '{start}')::BIGINT AND epoch(date '{end}')::BIGINT
),
d AS (
  SELECT
    code, date, o, h, l, c, v,
    lag(o, 1) OVER w AS o1,
    lag(h, 1) OVER w AS h1,
    lag(l, 1) OVER w AS l1,
    lag(c, 1) OVER w AS c1,
    lag(c, 2) OVER w AS c2,
    lag(c, 3) OVER w AS c3,
    lag(c, 4) OVER w AS c4,
    lag(c, 5) OVER w AS c5,
    lag(c, 10) OVER w AS c10,
    lag(c, 20) OVER w AS c20,
    lag(c, 60) OVER w AS c60,
    lag(c, 120) OVER w AS c120,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
    avg(v) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS v20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prev_high20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS prev_high60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS high120,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING) AS prev_high120,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS low20,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS low120,
    lead(o, 1) OVER w AS next_o,
    lead(h, 1) OVER w AS next_h,
    lead(l, 1) OVER w AS next_l,
    lead(c, 1) OVER w AS next_c,
    lead(o, 2) OVER w AS after_confirm_o,
    lead(c, 5) OVER w AS c_f5,
    lead(c, 10) OVER w AS c_f10,
    lead(c, 20) OVER w AS c_f20,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS low_f5_from_signal,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS high_f5_from_signal,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 2 FOLLOWING AND 6 FOLLOWING) AS low_f5_from_next,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 2 FOLLOWING AND 6 FOLLOWING) AS high_f5_from_next,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 2 FOLLOWING AND 11 FOLLOWING) AS low_f10_from_next,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 2 FOLLOWING AND 11 FOLLOWING) AS high_f10_from_next,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 3 FOLLOWING AND 7 FOLLOWING) AS low_f5_from_after_confirm,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 3 FOLLOWING AND 7 FOLLOWING) AS high_f5_from_after_confirm,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 3 FOLLOWING AND 12 FOLLOWING) AS low_f10_from_after_confirm,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 3 FOLLOWING AND 12 FOLLOWING) AS high_f10_from_after_confirm
  FROM pan
  WINDOW w AS (PARTITION BY code ORDER BY date)
),
d2 AS (
  SELECT
    *,
    (c / NULLIF(ma20, 0) - 1.0) AS close_vs_ma20_raw,
    (c / NULLIF(ma60, 0) - 1.0) AS close_vs_ma60_raw,
    ((c / NULLIF(c5, 0) - 1.0) * -1.0) AS drop5_raw,
    ((c / NULLIF(c10, 0) - 1.0) * -1.0) AS drop10_raw
  FROM d
),
f AS (
  SELECT
    *,
    min(close_vs_ma20_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS min_close_vs_ma20_120,
    min(close_vs_ma60_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS min_close_vs_ma60_120,
    max(drop5_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS max_5d_drop60,
    max(drop10_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS max_10d_drop120,
    lag(ma7, 1) OVER (PARTITION BY code ORDER BY date) AS ma7_1,
    lag(ma7, 3) OVER (PARTITION BY code ORDER BY date) AS ma7_3,
    lag(ma7, 5) OVER (PARTITION BY code ORDER BY date) AS ma7_5,
    lag(ma20, 5) OVER (PARTITION BY code ORDER BY date) AS ma20_5,
    lag(ma20, 10) OVER (PARTITION BY code ORDER BY date) AS ma20_10
  FROM d2
),
m AS (
  SELECT
    code,
    month,
    o AS month_o,
    h AS month_h,
    l AS month_l,
    c AS month_c,
    lag(c, 3) OVER (PARTITION BY code ORDER BY month) AS month_c3
  FROM monthly_bars
),
features AS (
  SELECT
    f.*,
    m.month_o,
    m.month_h,
    m.month_l,
    m.month_c,
    m.month_c3,
    CASE WHEN f.h > f.l THEN (f.h - greatest(f.o, f.c)) / (f.h - f.l) ELSE NULL END AS upper_wick,
    CASE WHEN f.h > f.l THEN (least(f.o, f.c) - f.l) / (f.h - f.l) ELSE NULL END AS lower_wick,
    (f.c / f.high20 - 1.0) AS close_vs_high20,
    (f.c / f.high60 - 1.0) AS close_vs_high60,
    (f.c / f.ma20 - 1.0) AS close_vs_ma20,
    (f.ma7 / f.ma7_3 - 1.0) AS ma7_slope3,
    (f.ma7 / f.ma7_5 - 1.0) AS ma7_slope5,
    (f.ma20 / f.ma20_5 - 1.0) AS ma20_slope5,
    (f.ma20 / f.ma20_10 - 1.0) AS ma20_slope10,
    (f.c / f.c5 - 1.0) AS ret5_past,
    (f.c / f.c10 - 1.0) AS ret10_past,
    (f.c / f.c20 - 1.0) AS ret20_past,
    (f.c / f.c60 - 1.0) AS ret60_past,
    (f.c / f.c120 - 1.0) AS ret120_past,
    (f.high60 / NULLIF(f.low60, 0) - 1.0) AS range60_width,
    (f.c - f.low60) / NULLIF(f.high60 - f.low60, 0) AS range60_pos,
    (f.c - f.low20) / NULLIF(f.high20 - f.low20, 0) AS range20_pos,
    f.v / NULLIF(f.v20, 0) AS volume_spike20,
    CASE WHEN m.month_h > m.month_l THEN (m.month_h - greatest(m.month_o, m.month_c)) / (m.month_h - m.month_l) ELSE NULL END AS month_upper_wick,
    (m.month_c / NULLIF(m.month_c3, 0) - 1.0) AS month_extension3,
    ((CASE WHEN f.c < f.c1 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c1 < f.c2 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c2 < f.c3 THEN 1 ELSE 0 END)) AS red_count3,
    ((CASE WHEN f.c < f.c1 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c1 < f.c2 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c2 < f.c3 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c3 < f.c4 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c4 < f.c5 THEN 1 ELSE 0 END)) AS red_count5,
    ((CASE WHEN f.c < f.ma20 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c1 < f.ma20 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c2 < f.ma20 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c3 < f.ma20 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c4 < f.ma20 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c5 < f.ma20 THEN 1 ELSE 0 END)) AS below20_count10,
    (f.next_l < f.l AND f.next_c < f.c AND f.next_c < f.ma7) AS next_bar_down_confirmed
  FROM f
  LEFT JOIN m
    ON m.code = f.code
   AND m.month = epoch(CAST(date_trunc('month', to_timestamp(f.date)) AS DATE))::BIGINT
  WHERE 1 = 1
    {forward_label_where}
    AND f.ma60 IS NOT NULL
    AND f.ma7_5 IS NOT NULL
    AND f.ma20_5 IS NOT NULL
    AND f.ma20_10 IS NOT NULL
    AND f.c120 IS NOT NULL
    AND f.c > 100
    AND f.v > 0
),
tagged AS (
  SELECT
    *,
    (h >= prev_high60 * 1.005 OR h >= prev_high120 * 1.005) AS fresh_high_update,
    (h < prev_high60 * 1.005 AND close_vs_high60 BETWEEN -0.12 AND -0.015 AND upper_wick >= 0.25) AS failed_high_update,
    (ret120_past >= 0.20 AND min_close_vs_ma60_120 > -0.06 AND max_10d_drop120 < 0.16) AS long_uptrend_mature,
    (ret60_past >= 0.12 AND min_close_vs_ma20_120 > -0.05 AND max_5d_drop60 < 0.10) AS extended_no_pullback,
    (ret60_past >= 0.12 AND c < ma20 AND max_5d_drop60 >= 0.08 AND close_vs_high60 BETWEEN -0.20 AND -0.04) AS first_major_break,
    (max_10d_drop120 >= 0.12 AND close_vs_ma20 BETWEEN -0.02 AND 0.05 AND ma20_slope10 <= 0 AND ret10_past BETWEEN -0.03 AND 0.10) AS post_major_break_retest,
    (close_vs_high60 BETWEEN -0.12 AND -0.015 AND ret20_past >= 0.08 AND c < ma7 AND c1 < ma7_1 AND ma7_slope3 < 0 AND close_vs_ma20 BETWEEN -0.08 AND 0.03 AND red_count3 >= 2 AND lower_wick < 0.28) AS strict_high_zone_ma_stall,
    (ma7 < ma20 AND ma20_slope5 <= 0 AND below20_count10 >= 4 AND close_vs_ma20 BETWEEN -0.025 AND 0.025 AND upper_wick >= 0.32 AND c <= o AND lower_wick < 0.30 AND range20_pos BETWEEN 0.45 AND 0.85) AS strict_ma20_rejection_return,
    (range60_width BETWEEN 0.10 AND 0.24 AND range60_pos BETWEEN 0.72 AND 0.96 AND c < high60 * 0.99 AND upper_wick >= 0.35 AND ma20_slope5 BETWEEN -0.015 AND 0.015 AND red_count5 >= 2 AND lower_wick < 0.30) AS strict_box_upper_failure,
    (ma20 < ma60 AND ma20_slope10 < 0 AND close_vs_ma20 BETWEEN -0.015 AND 0.035 AND close_vs_high60 <= -0.12 AND ret10_past BETWEEN -0.02 AND 0.10 AND upper_wick >= 0.30 AND c <= o AND lower_wick < 0.30) AS strict_downtrend_pullback_ceiling,
    (month_extension3 >= 0.25 AND month_upper_wick >= 0.35 AND month_c < month_o AND c < ma20 AND ma7 < ma20 AND close_vs_high60 BETWEEN -0.20 AND -0.04 AND lower_wick < 0.30) AS strict_monthly_extension_break
  FROM features
)
SELECT * FROM tagged
"""
    )


def _metric_sql(entry_price_expr: str, low_expr: str, high_expr: str) -> str:
    return f"""
  avg(CASE WHEN {low_expr} / NULLIF({entry_price_expr}, 0) - 1.0 <= -0.03 THEN 1.0 ELSE 0.0 END) AS take3pct_low5_rate,
  avg(CASE WHEN {low_expr} / NULLIF({entry_price_expr}, 0) - 1.0 <= -0.05 THEN 1.0 ELSE 0.0 END) AS take5pct_low5_rate,
  avg(CASE WHEN {high_expr} / NULLIF({entry_price_expr}, 0) - 1.0 >= 0.03 THEN 1.0 ELSE 0.0 END) AS stopped3pct_high5_rate,
  avg(({low_expr} / NULLIF({entry_price_expr}, 0) - 1.0)) AS avg_best_down5,
  avg(({high_expr} / NULLIF({entry_price_expr}, 0) - 1.0)) AS avg_adverse_up5
"""


ENTRY_MODES = {
    "next_open_after_signal": {
        "description": "signal翌日寄りで売る",
        "where": "next_o IS NOT NULL",
        "entry": "next_o",
        "low": "low_f5_from_signal",
        "high": "high_f5_from_signal",
    },
    "break_signal_low_intraday": {
        "description": "次足でsignal安値を割ったらsignal安値近辺で売る",
        "where": "next_l < l",
        "entry": "l",
        "low": "low_f5_from_signal",
        "high": "high_f5_from_signal",
    },
    "after_confirm_next_open": {
        "description": "次足安値割れ/終値弱さ確認後、翌日寄りで売る",
        "where": "next_bar_down_confirmed AND after_confirm_o IS NOT NULL",
        "entry": "after_confirm_o",
        "low": "low_f5_from_next",
        "high": "high_f5_from_next",
    },
}


ENTRY_QUALITY_FILTERS = {
    "none": {
        "description": "追加フィルタなし",
        "where": "TRUE",
        "known_at_entry": True,
    },
    "range60_pos_ge_70": {
        "description": "signal時点で60日レンジ位置が70%以上",
        "where": "range60_pos >= 0.70",
        "known_at_entry": True,
    },
    "lower_wick_lt_20": {
        "description": "signal足の下ヒゲが短い",
        "where": "lower_wick < 0.20",
        "known_at_entry": True,
    },
    "close_not_too_far_below_ma20": {
        "description": "signal終値が20MAから下に離れすぎていない",
        "where": "close_vs_ma20 >= -0.04",
        "known_at_entry": True,
    },
}


def _summary(
    con: duckdb.DuckDBPyConnection, where_sql: str, mode: dict[str, str], quality_filter: dict[str, Any]
) -> dict[str, Any]:
    row = con.execute(
        f"""
SELECT
  count(*) AS n,
{_metric_sql(mode['entry'], mode['low'], mode['high'])}
FROM short_entry_contract_events
WHERE {where_sql} AND {mode['where']} AND {quality_filter['where']}
"""
    ).fetchone()
    cols = [d[0] for d in con.description]
    out = dict(zip(cols, row))
    if int(out.get("n") or 0):
        out["risk_adjusted_capture_ratio"] = float(out["take3pct_low5_rate"] or 0) / max(
            float(out["stopped3pct_high5_rate"] or 0), 1e-9
        )
        out["priority_edge_score"] = (
            float(out["take3pct_low5_rate"] or 0) - float(out["stopped3pct_high5_rate"] or 0)
        )
    else:
        out["risk_adjusted_capture_ratio"] = None
        out["priority_edge_score"] = None
    return out


def _decision(summary: dict[str, Any]) -> str:
    n = int(summary.get("n") or 0)
    if n < ACCEPTANCE["min_n"]:
        return "hold_low_sample"
    if (
        float(summary.get("take3pct_low5_rate") or 0) >= ACCEPTANCE["min_take3pct_low5_rate"]
        and float(summary.get("stopped3pct_high5_rate") or 1) <= ACCEPTANCE["max_stopped3pct_high5_rate"]
        and float(summary.get("risk_adjusted_capture_ratio") or 0) >= ACCEPTANCE["min_risk_adjusted_capture_ratio"]
    ):
        return "keep_candidate"
    return "drop_or_hold_needs_filter"


def run(*, db_path: Path, output_root: Path, start: str, end: str) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    _build_table(con, start, end)
    rows = []
    for scenario_id, context_id, label in TARGETS:
        where_sql = f"{scenario_id} AND {context_id}"
        for mode_id, mode in ENTRY_MODES.items():
            for quality_id, quality_filter in ENTRY_QUALITY_FILTERS.items():
                summary = _summary(con, where_sql, mode, quality_filter)
                rows.append(
                    {
                        "scenario_id": scenario_id,
                        "context_id": context_id,
                        "label": label,
                        "entry_mode": mode_id,
                        "entry_description": mode["description"],
                        "entry_quality_filter_id": quality_id,
                        "entry_quality_filter_description": quality_filter["description"],
                        "known_at_entry": quality_filter["known_at_entry"],
                        "summary": summary,
                        "candidate_local_decision": _decision(summary),
                    }
                )
    keep_rows = [row for row in rows if row["candidate_local_decision"] == "keep_candidate"]
    ranked = sorted(
        rows,
        key=lambda row: (
            1 if row["candidate_local_decision"] == "keep_candidate" else 0,
            row["summary"].get("priority_edge_score") if row["summary"].get("priority_edge_score") is not None else -999,
            row["summary"].get("n") or 0,
        ),
        reverse=True,
    )
    implementation_contract = {
        "contract_version": "short_scenario_entry_contract_v1",
        "owner": "TRADEX",
        "meemee_reflectable": False,
        "status": "review_only_until_forward_entry_validation",
        "accepted_entry_modes": [
            {
                "scenario_id": row["scenario_id"],
                "context_id": row["context_id"],
                "label": row["label"],
                "entry_mode": row["entry_mode"],
                "entry_description": row["entry_description"],
                "entry_quality_filter_id": row["entry_quality_filter_id"],
                "entry_quality_filter_description": row["entry_quality_filter_description"],
                "summary": row["summary"],
            }
            for row in keep_rows
        ],
        "minimum_acceptance": ACCEPTANCE,
    }
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "start": start,
            "end": end,
            "source": "daily_bars source=pan",
            "entry_modes": ENTRY_MODES,
            "entry_quality_filters": ENTRY_QUALITY_FILTERS,
            "acceptance": ACCEPTANCE,
            "cost_slippage": "not_applied",
        },
        "rows": ranked,
        "keep_rows": keep_rows,
        "implementation_contract": implementation_contract,
        "decision": {
            "candidate_local_decision": "entry_contract_candidates_available" if keep_rows else "hold_no_entry_contract_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "context-conditioned short scenarios were evaluated under practical entry modes; keep rows satisfy sample, downside-capture, adverse-risk, and ratio gates",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / f"{tag}-{AXIS_ID}"
    _write_json(out_dir / "entry_contract_report.json", report)
    _write_json(output_root / "latest_entry_contract_report.json", {"run_root": str(out_dir), **report})
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="2026-06-01")
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, start=args.start, end=args.end))


if __name__ == "__main__":
    main()
