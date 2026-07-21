from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_scenario_priority_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_scenario_priority_compare_v1")


SCENARIOS = [
    ("high_zone_ma_stall_short", "高値圏MA失速ショート"),
    ("spike_upper_wick_failure_short", "急騰後上ヒゲ失敗ショート"),
    ("ma20_rejection_return_short", "戻り売り20MA拒否ショート"),
    ("box_upper_failure_short", "ボックス上限失敗ショート"),
    ("monthly_extension_break_short", "月足伸び切り崩れショート"),
    ("downtrend_pullback_ceiling_short", "下落トレンド戻り天井ショート"),
]


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


def _build_event_table(con: duckdb.DuckDBPyConnection, start: str, end: str) -> None:
    scenario_cols = ",\n    ".join(
        f"case when {expr} then true else false end as {name}"
        for name, _label, expr in [
            (
                "high_zone_ma_stall_short",
                "高値圏MA失速ショート",
                "close_vs_high60 >= -0.08 and c < ma7 and c < ma20 * 1.02 and ma7_slope5 < 0 and lower_wick < 0.35",
            ),
            (
                "spike_upper_wick_failure_short",
                "急騰後上ヒゲ失敗ショート",
                "ret5_past >= 0.08 and upper_wick >= 0.45 and close_vs_high20 >= -0.04 and lower_wick < 0.35",
            ),
            (
                "ma20_rejection_return_short",
                "戻り売り20MA拒否ショート",
                "close_vs_ma20 between -0.02 and 0.04 and upper_wick >= 0.35 and ma7_slope5 < 0 and red_count5 >= 2 and lower_wick < 0.35",
            ),
            (
                "box_upper_failure_short",
                "ボックス上限失敗ショート",
                "range60_width between 0.08 and 0.28 and range60_pos >= 0.75 and upper_wick >= 0.35 and c < high60 * 0.995 and lower_wick < 0.35",
            ),
            (
                "monthly_extension_break_short",
                "月足伸び切り崩れショート",
                "month_extension3 >= 0.25 and month_upper_wick >= 0.30 and c < ma20 and close_vs_high60 >= -0.15 and lower_wick < 0.35",
            ),
            (
                "downtrend_pullback_ceiling_short",
                "下落トレンド戻り天井ショート",
                "ma20 < ma60 and ma20_slope5 < 0 and close_vs_ma20 between -0.01 and 0.05 and upper_wick >= 0.30 and lower_wick < 0.35",
            ),
        ]
    )
    con.execute(
        f"""
CREATE OR REPLACE TEMP TABLE short_scenario_events AS
WITH pan AS (
  SELECT code, date, o, h, l, c, v
  FROM daily_bars
  WHERE source = 'pan'
    AND date BETWEEN epoch(date '{start}')::BIGINT AND epoch(date '{end}')::BIGINT
),
d AS (
  SELECT
    code, date, o, h, l, c, v,
    lag(c, 1) OVER w AS c1,
    lag(c, 2) OVER w AS c2,
    lag(c, 3) OVER w AS c3,
    lag(c, 4) OVER w AS c4,
    lag(c, 5) OVER w AS c5,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
    lead(c, 5) OVER w AS c_f5,
    lead(c, 10) OVER w AS c_f10,
    lead(c, 20) OVER w AS c_f20,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS low_f5,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS high_f5,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS low_f10,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS high_f10
  FROM pan
  WINDOW w AS (PARTITION BY code ORDER BY date)
),
f AS (
  SELECT
    *,
    lag(ma7, 5) OVER (PARTITION BY code ORDER BY date) AS ma7_5,
    lag(ma20, 5) OVER (PARTITION BY code ORDER BY date) AS ma20_5
  FROM d
),
m AS (
  SELECT
    code,
    month,
    h AS month_h,
    l AS month_l,
    c AS month_c,
    o AS month_o,
    lag(c, 3) OVER (PARTITION BY code ORDER BY month) AS month_c3
  FROM monthly_bars
),
features AS (
  SELECT
    f.*,
    m.month_h,
    m.month_l,
    m.month_c,
    m.month_o,
    m.month_c3,
    (f.c_f5 / f.c - 1.0) AS ret5,
    (f.c_f10 / f.c - 1.0) AS ret10,
    (f.c_f20 / f.c - 1.0) AS ret20,
    (f.low_f5 / f.c - 1.0) AS best_down5,
    (f.high_f5 / f.c - 1.0) AS adverse_up5,
    (f.low_f10 / f.c - 1.0) AS best_down10,
    (f.high_f10 / f.c - 1.0) AS adverse_up10,
    CASE WHEN f.h > f.l THEN (f.h - greatest(f.o, f.c)) / (f.h - f.l) ELSE NULL END AS upper_wick,
    CASE WHEN f.h > f.l THEN (least(f.o, f.c) - f.l) / (f.h - f.l) ELSE NULL END AS lower_wick,
    (f.c / f.high20 - 1.0) AS close_vs_high20,
    (f.c / f.high60 - 1.0) AS close_vs_high60,
    (f.c / f.ma20 - 1.0) AS close_vs_ma20,
    (f.ma7 / f.ma7_5 - 1.0) AS ma7_slope5,
    (f.ma20 / f.ma20_5 - 1.0) AS ma20_slope5,
    (f.c / f.c5 - 1.0) AS ret5_past,
    (f.high60 / NULLIF(f.low60, 0) - 1.0) AS range60_width,
    (f.c - f.low60) / NULLIF(f.high60 - f.low60, 0) AS range60_pos,
    CASE WHEN m.month_h > m.month_l THEN (m.month_h - greatest(m.month_o, m.month_c)) / (m.month_h - m.month_l) ELSE NULL END AS month_upper_wick,
    (m.month_c / NULLIF(m.month_c3, 0) - 1.0) AS month_extension3,
    ((CASE WHEN f.c < f.c1 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c1 < f.c2 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c2 < f.c3 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c3 < f.c4 THEN 1 ELSE 0 END)
      + (CASE WHEN f.c4 < f.c5 THEN 1 ELSE 0 END)) AS red_count5
  FROM f
  LEFT JOIN m
    ON m.code = f.code
   AND m.month = epoch(CAST(date_trunc('month', to_timestamp(f.date)) AS DATE))::BIGINT
  WHERE f.c_f20 IS NOT NULL
    AND f.ma60 IS NOT NULL
    AND f.ma7_5 IS NOT NULL
    AND f.ma20_5 IS NOT NULL
    AND f.c > 100
    AND f.v > 0
)
SELECT
  *,
  {scenario_cols}
FROM features
"""
    )


def _summary_for_where(con: duckdb.DuckDBPyConnection, where_sql: str) -> dict[str, Any]:
    row = con.execute(
        f"""
SELECT
  count(*) AS n,
  avg(CASE WHEN best_down5 <= -0.03 THEN 1.0 ELSE 0.0 END) AS take3pct_low5_rate,
  avg(CASE WHEN best_down10 <= -0.05 THEN 1.0 ELSE 0.0 END) AS take5pct_low10_rate,
  avg(CASE WHEN adverse_up5 >= 0.03 THEN 1.0 ELSE 0.0 END) AS stopped3pct_high5_rate,
  avg(CASE WHEN adverse_up10 >= 0.04 THEN 1.0 ELSE 0.0 END) AS stopped4pct_high10_rate,
  avg(CASE WHEN ret5 <= -0.02 THEN 1.0 ELSE 0.0 END) AS close_down2pct5_rate,
  avg(CASE WHEN ret10 <= -0.03 THEN 1.0 ELSE 0.0 END) AS close_down3pct10_rate,
  avg(best_down5) AS avg_best_down5,
  avg(adverse_up5) AS avg_adverse_up5,
  avg(ret5) AS avg_close_ret5,
  avg(ret10) AS avg_close_ret10
FROM short_scenario_events
WHERE {where_sql}
"""
    ).fetchone()
    cols = [d[0] for d in con.description]
    payload = dict(zip(cols, row))
    n = int(payload.get("n") or 0)
    edge = None
    if n:
        edge = (
            float(payload["take3pct_low5_rate"] or 0)
            - float(payload["stopped3pct_high5_rate"] or 0)
            + 0.5 * float(payload["take5pct_low10_rate"] or 0)
            - 0.5 * float(payload["stopped4pct_high10_rate"] or 0)
        )
    payload["priority_edge_score"] = edge
    return payload


def run(*, db_path: Path, output_root: Path, start: str, end: str) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    _build_event_table(con, start, end)
    scenario_summaries = []
    for name, label in SCENARIOS:
        summary = _summary_for_where(con, f"{name}")
        scenario_summaries.append({"scenario_id": name, "label": label, **summary})

    pair_rows = []
    for a_name, a_label in SCENARIOS:
        for b_name, b_label in SCENARIOS:
            if a_name >= b_name:
                continue
            both = _summary_for_where(con, f"{a_name} AND {b_name}")
            if int(both.get("n") or 0) < 200:
                continue
            a_only = _summary_for_where(con, f"{a_name} AND NOT {b_name}")
            b_only = _summary_for_where(con, f"{b_name} AND NOT {a_name}")
            pair_rows.append(
                {
                    "scenario_a": a_name,
                    "label_a": a_label,
                    "scenario_b": b_name,
                    "label_b": b_label,
                    "both": both,
                    "a_only": a_only,
                    "b_only": b_only,
                    "priority_hint": (
                        a_name
                        if (a_only.get("priority_edge_score") or -999) >= (b_only.get("priority_edge_score") or -999)
                        else b_name
                    ),
                }
            )

    ranked = sorted(
        scenario_summaries,
        key=lambda x: (
            x.get("priority_edge_score") if x.get("priority_edge_score") is not None else -999,
            x.get("take3pct_low5_rate") or 0,
        ),
        reverse=True,
    )
    priority_order = [row["scenario_id"] for row in ranked]
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
            "entry_convention": "signal day close proxy",
            "horizons": ["5d", "10d", "20d"],
            "priority_edge_score": "take3pct_low5 - stopped3pct_high5 + 0.5*take5pct_low10 - 0.5*stopped4pct_high10",
            "same_universe": True,
            "cost_slippage": "not_applied_proxy_comparison",
        },
        "scenario_summaries": ranked,
        "pairwise_overlap": pair_rows,
        "priority_order": priority_order,
        "decision": {
            "candidate_local_decision": "scenario_priority_order_available_for_review",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "scenario families were compared under fixed historical proxy conditions; priority is based on downside capture minus adverse move risk",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / f"{tag}-{AXIS_ID}"
    _write_json(out_dir / "scenario_priority_compare.json", report)
    _write_json(output_root / "latest_scenario_priority_compare.json", {"run_root": str(out_dir), **report})
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="2026-06-01")
    args = parser.parse_args()
    out = run(db_path=args.db_path, output_root=args.output_root, start=args.start, end=args.end)
    print(out)


if __name__ == "__main__":
    main()
