from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_shape_window_length_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_window_length_compare_v1")

WINDOWS = {
    "quarter_60d": 60,
    "half_120d": 120,
    "year_240d": 240,
}

FEATURES = {
    "range_pos": {"direction": "high", "description": "window内の現在位置。高いほど高値圏"},
    "ret": {"direction": "high", "description": "window始点比の上昇率。高いほど上昇成熟"},
    "close_vs_high": {"direction": "low", "description": "window高値からの距離。0に近いほど高値近辺"},
    "max_5d_drop": {"direction": "low", "description": "window内の最大5日下落。低いほど深い押しなし"},
    "min_close_vs_ma20": {"direction": "high", "description": "window内の20MA割れ深度。高いほど崩れなし"},
}

ACCEPTANCE = {
    "min_bucket_n": 80,
    "min_take3pct_low5_rate": 0.60,
    "max_stopped3pct_high5_rate": 0.30,
    "min_risk_adjusted_capture_ratio": 2.0,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_db_path() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    if local:
        return Path(local) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    return Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_events(con: duckdb.DuckDBPyConnection, start: str, end: str) -> None:
    con.execute(
        f"""
CREATE OR REPLACE TEMP TABLE window_length_events AS
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
    lag(c, 5) OVER w AS c5,
    lag(c, 10) OVER w AS c10,
    lag(c, 20) OVER w AS c20,
    lag(c, 60) OVER w AS c60,
    lag(c, 120) OVER w AS c120,
    lag(c, 240) OVER w AS c240,
    lead(l, 1) OVER w AS next_l,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS high120,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS low120,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS high240,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS low240,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS low_f5,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS high_f5
  FROM pan
  WINDOW w AS (PARTITION BY code ORDER BY date)
),
d2 AS (
  SELECT
    *,
    lag(ma7, 1) OVER (PARTITION BY code ORDER BY date) AS ma7_1,
    lag(ma7, 3) OVER (PARTITION BY code ORDER BY date) AS ma7_3,
    lag(ma7, 5) OVER (PARTITION BY code ORDER BY date) AS ma7_5,
    (c / NULLIF(ma20, 0) - 1.0) AS close_vs_ma20_raw,
    ((c / NULLIF(c5, 0) - 1.0) * -1.0) AS drop5_raw
  FROM d
),
f AS (
  SELECT
    *,
    min(close_vs_ma20_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS min_close_vs_ma20_60,
    min(close_vs_ma20_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS min_close_vs_ma20_120,
    min(close_vs_ma20_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS min_close_vs_ma20_240,
    max(drop5_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS max_5d_drop_60,
    max(drop5_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS max_5d_drop_120,
    max(drop5_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS max_5d_drop_240
  FROM d2
),
events AS (
  SELECT
    *,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick,
    CASE WHEN h > l THEN (least(o, c) - l) / (h - l) ELSE NULL END AS lower_wick,
    c / NULLIF(c60, 0) - 1.0 AS ret_60,
    c / NULLIF(c120, 0) - 1.0 AS ret_120,
    c / NULLIF(c240, 0) - 1.0 AS ret_240,
    c / NULLIF(high60, 0) - 1.0 AS close_vs_high_60,
    c / NULLIF(high120, 0) - 1.0 AS close_vs_high_120,
    c / NULLIF(high240, 0) - 1.0 AS close_vs_high_240,
    (c - low60) / NULLIF(high60 - low60, 0) AS range_pos_60,
    (c - low120) / NULLIF(high120 - low120, 0) AS range_pos_120,
    (c - low240) / NULLIF(high240 - low240, 0) AS range_pos_240,
    (ma7 / NULLIF(ma7_3, 0) - 1.0) AS ma7_slope3,
    ((CASE WHEN c < c1 THEN 1 ELSE 0 END)
      + (CASE WHEN c1 < c2 THEN 1 ELSE 0 END)
      + (CASE WHEN c2 < c3 THEN 1 ELSE 0 END)) AS red_count3
  FROM f
)
SELECT
  *,
  (close_vs_high_60 BETWEEN -0.12 AND -0.015
    AND c < ma7
    AND c1 < ma7_1
    AND ma7_slope3 < 0
    AND c / NULLIF(ma20, 0) - 1.0 BETWEEN -0.08 AND 0.03
    AND red_count3 >= 2
    AND lower_wick < 0.28) AS base_high_zone_ma_stall,
  (low_f5 / NULLIF(l, 0) - 1.0 <= -0.03) AS take3pct_low5,
  (high_f5 / NULLIF(l, 0) - 1.0 >= 0.03) AS stopped3pct_high5
FROM events
WHERE c240 IS NOT NULL
  AND low_f5 IS NOT NULL
  AND high_f5 IS NOT NULL
  AND ma60 IS NOT NULL
  AND c > 100
  AND v > 0
"""
    )


def _score(con: duckdb.DuckDBPyConnection, window_id: str, days: int, feature_id: str, direction: str) -> dict[str, Any]:
    suffix = str(days)
    col = f"{feature_id}_{suffix}"
    where = "base_high_zone_ma_stall"
    order = "DESC" if direction == "high" else "ASC"
    rows = con.execute(
        f"""
WITH buckets AS (
  SELECT
    *,
    ntile(10) OVER (ORDER BY {col} {order} NULLS LAST) AS bucket
  FROM window_length_events
    WHERE {where} AND next_l < l AND {col} IS NOT NULL
),
summary AS (
  SELECT
    bucket,
    count(*) AS n,
    min({col}) AS min_value,
    max({col}) AS max_value,
    avg(CASE WHEN take3pct_low5 THEN 1.0 ELSE 0.0 END) AS take3pct_low5_rate,
    avg(CASE WHEN stopped3pct_high5 THEN 1.0 ELSE 0.0 END) AS stopped3pct_high5_rate
  FROM buckets
  GROUP BY bucket
)
SELECT * FROM summary ORDER BY bucket
"""
    ).fetchdf()
    bucket_rows: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    for row in rows.to_dict("records"):
        n = int(row["n"])
        take = float(row["take3pct_low5_rate"] or 0)
        stop = float(row["stopped3pct_high5_rate"] or 0)
        item = {
            "bucket": int(row["bucket"]),
            "n": n,
            "min_value": None if row["min_value"] is None else float(row["min_value"]),
            "max_value": None if row["max_value"] is None else float(row["max_value"]),
            "take3pct_low5_rate": take,
            "stopped3pct_high5_rate": stop,
            "risk_adjusted_capture_ratio": take / max(stop, 1e-9),
            "edge": take - stop,
        }
        bucket_rows.append(item)
        if n >= ACCEPTANCE["min_bucket_n"]:
            if best is None or (item["edge"], item["risk_adjusted_capture_ratio"], item["n"]) > (
                best["edge"],
                best["risk_adjusted_capture_ratio"],
                best["n"],
            ):
                best = item
    if best is None:
        decision = "hold_no_sufficient_bucket"
    elif (
        best["take3pct_low5_rate"] >= ACCEPTANCE["min_take3pct_low5_rate"]
        and best["stopped3pct_high5_rate"] <= ACCEPTANCE["max_stopped3pct_high5_rate"]
        and best["risk_adjusted_capture_ratio"] >= ACCEPTANCE["min_risk_adjusted_capture_ratio"]
    ):
        decision = "keep_window_feature_candidate"
    else:
        decision = "drop_or_hold_weak_separation"
    return {
        "window_id": window_id,
        "lookback_days": days,
        "feature_id": feature_id,
        "feature_description": FEATURES[feature_id]["description"],
        "bucket_direction": direction,
        "bucket_rows": bucket_rows,
        "best_bucket": best,
        "candidate_local_decision": decision,
    }


def _composite_scores(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    thresholds = {
        "quarter_60d": {"days": 60, "ret_mature": 0.12, "ret_strong": 0.20, "max_drop_clean": 0.10, "max_drop_break": 0.08, "min_ma20": -0.05},
        "half_120d": {"days": 120, "ret_mature": 0.20, "ret_strong": 0.35, "max_drop_clean": 0.14, "max_drop_break": 0.12, "min_ma20": -0.07},
        "year_240d": {"days": 240, "ret_mature": 0.30, "ret_strong": 0.50, "max_drop_clean": 0.18, "max_drop_break": 0.16, "min_ma20": -0.10},
    }
    out: list[dict[str, Any]] = []
    for window_id, t in thresholds.items():
        suffix = str(t["days"])
        rules = {
            "mature_high_range": f"ret_{suffix} >= {t['ret_mature']} AND range_pos_{suffix} >= 0.70",
            "strong_high_range": f"ret_{suffix} >= {t['ret_strong']} AND range_pos_{suffix} >= 0.70",
            "mature_no_drop": f"ret_{suffix} >= {t['ret_mature']} AND max_5d_drop_{suffix} < {t['max_drop_clean']}",
            "mature_no_drop_high": f"ret_{suffix} >= {t['ret_mature']} AND max_5d_drop_{suffix} < {t['max_drop_clean']} AND range_pos_{suffix} >= 0.70",
            "clean_high_range": f"min_close_vs_ma20_{suffix} > {t['min_ma20']} AND range_pos_{suffix} >= 0.70",
            "near_high_clean": f"close_vs_high_{suffix} >= -0.08 AND min_close_vs_ma20_{suffix} > {t['min_ma20']}",
            "post_break_retest": f"max_5d_drop_{suffix} >= {t['max_drop_break']} AND close_vs_high_{suffix} <= -0.08",
            "post_break_retest_high": f"max_5d_drop_{suffix} >= {t['max_drop_break']} AND close_vs_high_{suffix} <= -0.08 AND range_pos_{suffix} >= 0.70",
        }
        for rule_id, where_sql in rules.items():
            row = con.execute(
                f"""
SELECT
  count(*) AS n,
  avg(CASE WHEN take3pct_low5 THEN 1.0 ELSE 0.0 END) AS take3pct_low5_rate,
  avg(CASE WHEN stopped3pct_high5 THEN 1.0 ELSE 0.0 END) AS stopped3pct_high5_rate
FROM window_length_events
WHERE base_high_zone_ma_stall
  AND next_l < l
  AND {where_sql}
"""
            ).fetchone()
            n = int(row[0] or 0)
            take = float(row[1] or 0)
            stop = float(row[2] or 0)
            ratio = take / max(stop, 1e-9) if n else None
            edge = take - stop if n else None
            if n < ACCEPTANCE["min_bucket_n"]:
                decision = "hold_low_sample"
            elif (
                take >= ACCEPTANCE["min_take3pct_low5_rate"]
                and stop <= ACCEPTANCE["max_stopped3pct_high5_rate"]
                and (ratio or 0) >= ACCEPTANCE["min_risk_adjusted_capture_ratio"]
            ):
                decision = "keep_composite_candidate"
            else:
                decision = "drop_or_hold_weak_separation"
            out.append(
                {
                    "window_id": window_id,
                    "lookback_days": int(t["days"]),
                    "composite_rule_id": rule_id,
                    "where_sql": where_sql,
                    "summary": {
                        "n": n,
                        "take3pct_low5_rate": take,
                        "stopped3pct_high5_rate": stop,
                        "risk_adjusted_capture_ratio": ratio,
                        "edge": edge,
                    },
                    "candidate_local_decision": decision,
                }
            )
    return sorted(
        out,
        key=lambda row: (
            1 if row["candidate_local_decision"] == "keep_composite_candidate" else 0,
            row["summary"]["edge"] if row["summary"]["edge"] is not None else -999,
            row["summary"]["risk_adjusted_capture_ratio"] if row["summary"]["risk_adjusted_capture_ratio"] is not None else -999,
            row["summary"]["n"],
        ),
        reverse=True,
    )


def run(*, db_path: Path, output_root: Path, start: str, end: str) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    _build_events(con, start, end)
    base = con.execute(
        """
SELECT
  count(*) AS n,
  avg(CASE WHEN take3pct_low5 THEN 1.0 ELSE 0.0 END) AS take3pct_low5_rate,
  avg(CASE WHEN stopped3pct_high5 THEN 1.0 ELSE 0.0 END) AS stopped3pct_high5_rate
FROM window_length_events
WHERE base_high_zone_ma_stall AND next_l < l
"""
    ).fetchone()
    rows = []
    for window_id, days in WINDOWS.items():
        for feature_id, meta in FEATURES.items():
            rows.append(_score(con, window_id, days, feature_id, meta["direction"]))
    ranked = sorted(
        rows,
        key=lambda row: (
            1 if row["candidate_local_decision"] == "keep_window_feature_candidate" else 0,
            row["best_bucket"]["edge"] if row["best_bucket"] else -999,
            row["best_bucket"]["risk_adjusted_capture_ratio"] if row["best_bucket"] else -999,
            row["best_bucket"]["n"] if row["best_bucket"] else 0,
        ),
        reverse=True,
    )
    by_window = {}
    for window_id in WINDOWS:
        subset = [row for row in ranked if row["window_id"] == window_id]
        keep = [row for row in subset if row["candidate_local_decision"] == "keep_window_feature_candidate"]
        best = subset[0] if subset else None
        by_window[window_id] = {
            "keep_feature_count": len(keep),
            "best_feature_id": best["feature_id"] if best else None,
            "best_bucket": best["best_bucket"] if best else None,
            "best_decision": best["candidate_local_decision"] if best else None,
        }
    composite_rows = _composite_scores(con)
    composite_by_window = {}
    for window_id in WINDOWS:
        subset = [row for row in composite_rows if row["window_id"] == window_id]
        keep = [row for row in subset if row["candidate_local_decision"] == "keep_composite_candidate"]
        best = subset[0] if subset else None
        composite_by_window[window_id] = {
            "keep_rule_count": len(keep),
            "best_rule_id": best["composite_rule_id"] if best else None,
            "best_summary": best["summary"] if best else None,
            "best_decision": best["candidate_local_decision"] if best else None,
        }
    best_window = sorted(
        composite_by_window.items(),
        key=lambda item: (
            item[1]["keep_rule_count"],
            item[1]["best_summary"]["edge"] if item[1]["best_summary"] else -999,
            item[1]["best_summary"]["risk_adjusted_capture_ratio"] if item[1]["best_summary"] else -999,
        ),
        reverse=True,
    )[0][0]
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
            "base_event": "base_high_zone_ma_stall",
            "entry_assumption": "break_signal_low_intraday at signal low; same base event requires next_l < signal_l",
            "cost_slippage": "not_applied",
            "only_axis_changed": "lookback_window_length",
            "windows": WINDOWS,
            "acceptance": ACCEPTANCE,
        },
        "base_event_summary": {
            "n": int(base[0] or 0),
            "take3pct_low5_rate": float(base[1] or 0),
            "stopped3pct_high5_rate": float(base[2] or 0),
        },
        "rows": ranked,
        "by_window": by_window,
        "composite_rows": composite_rows,
        "composite_by_window": composite_by_window,
        "decision": {
            "candidate_local_decision": "hold_sample_window_preference",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "preferred_window_for_next_axis": best_window,
            "reason": "window lengths were compared on the same base sell event; next axis should validate the preferred window under stricter context rules and OOS stability",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / f"{tag}-{AXIS_ID}"
    _write_json(out_dir / "window_length_compare_report.json", report)
    _write_json(output_root / "latest_window_length_compare_report.json", {"run_root": str(out_dir), **report})
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
