from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from scripts.tradex_short_collapse_shape_10d_compare_v1 import (
    _load_daily,
    _pct,
    _rolling_max,
    _rolling_mean,
    _rolling_min,
    _round,
)
from scripts.tradex_short_top_failed_high_confirm_10d_compare_v1 import _confirmation_entries, _evaluate
from scripts.tradex_short_top_failed_high_rr_10d_compare_v1 import _attach_rr
from scripts.tradex_short_top_first_failure_10d_compare_v1 import _features, _tags


AXIS_ID = "short_top_failed_high_long_proxy_regime_10d_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_top_failed_high_long_proxy_regime_10d_compare_v1")
RR_THRESHOLD = 1.0

FOCUS_PATTERNS = {
    "pre_break_weak_pressure_above_ma20__no_high_ma7_fail_confirm",
    "distribution_three_red_high_zone__no_high_ma7_fail_confirm",
    "high_zone_first_ma7_fail__no_high_ma7_fail_confirm",
    "high_zone_upper_wick_first_failure__no_high_ma7_fail_confirm",
}


PROXY_FILTERS: dict[str, dict[str, Any]] = {
    "proxy_all": {"description": "RR>=1.0 focus patterns only", "fn": lambda r: True},
    "proxy_index_below_ma20": {
        "description": "equal-weight index proxy below MA20",
        "fn": lambda r: _f(r.get("proxy_index_close_vs_ma20"), 1.0) <= 0,
    },
    "proxy_weak_breadth": {
        "description": "breadth_above_ma20_proxy <= 0.45",
        "fn": lambda r: _f(r.get("proxy_breadth_above_ma20"), 1.0) <= 0.45,
    },
    "proxy_risk_off_combo": {
        "description": "breadth_above_ma20_proxy <= 0.45 and equal-weight index proxy below MA20",
        "fn": lambda r: _f(r.get("proxy_breadth_above_ma20"), 1.0) <= 0.45
        and _f(r.get("proxy_index_close_vs_ma20"), 1.0) <= 0,
    },
    "proxy_broad_weakness": {
        "description": "breadth_above_ma20_proxy <= 0.40 and advancers_ratio_proxy <= 0.45",
        "fn": lambda r: _f(r.get("proxy_breadth_above_ma20"), 1.0) <= 0.40
        and _f(r.get("proxy_advancers_ratio"), 1.0) <= 0.45,
    },
}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _market_proxy(conn: duckdb.DuckDBPyConnection) -> dict[int, dict[str, Any]]:
    rows = conn.execute(
        """
        WITH normalized AS (
          SELECT
            code,
            CASE
              WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
              ELSE CAST(date AS INTEGER)
            END AS ymd,
            c
          FROM daily_bars
          WHERE COALESCE(source, 'pan') <> 'yahoo'
            AND c > 0
        ),
        with_ma AS (
          SELECT
            *,
            avg(c) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
            lag(c, 1) OVER (PARTITION BY code ORDER BY ymd) AS prev_c
          FROM normalized
        ),
        daily AS (
          SELECT
            ymd,
            count(*) AS n,
            avg(CASE WHEN c > ma20 THEN 1.0 ELSE 0.0 END) AS breadth_above_ma20,
            avg(CASE WHEN prev_c IS NOT NULL AND c > prev_c THEN 1.0 ELSE 0.0 END) AS advancers_ratio,
            avg(c) AS ew_index
          FROM with_ma
          WHERE ma20 IS NOT NULL
          GROUP BY ymd
        ),
        projected AS (
          SELECT
            *,
            avg(ew_index) OVER (ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ew_index_ma20
          FROM daily
        )
        SELECT
          ymd,
          n,
          breadth_above_ma20,
          advancers_ratio,
          ew_index / NULLIF(ew_index_ma20, 0) - 1.0 AS index_close_vs_ma20
        FROM projected
        WHERE ew_index_ma20 IS NOT NULL
        ORDER BY ymd
        """
    ).fetchall()
    return {
        int(ymd): {
            "proxy_universe_count": int(n or 0),
            "proxy_breadth_above_ma20": float(breadth or 0),
            "proxy_advancers_ratio": float(advancers or 0),
            "proxy_index_close_vs_ma20": float(index_vs or 0),
        }
        for ymd, n, breadth, advancers, index_vs in rows
    }


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    returns = [float(row["close10_short_ret"]) for row in rows]
    by_month: dict[str, list[float]] = defaultdict(list)
    by_year: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        ymd = str(row["entry_ymd"])
        by_month[ymd[:6]].append(float(row["close10_short_ret"]))
        by_year[ymd[:4]].append(float(row["close10_short_ret"]))
    usable_months = [values for values in by_month.values() if len(values) >= 5]
    usable_years = [values for values in by_year.values() if len(values) >= 10]
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "target5_hit_10d_rate": _round(sum(1 for row in rows if row["target5_hit_10d"]) / len(rows)),
        "target8_hit_10d_rate": _round(sum(1 for row in rows if row["target8_hit_10d"]) / len(rows)),
        "target5_first_rate": _round(sum(1 for row in rows if row["target5_first"]) / len(rows)),
        "stop_first_rate": _round(sum(1 for row in rows if row["stop_first"]) / len(rows)),
        "avg_stop_pct": _round(mean(float(row["stop_pct"]) for row in rows)),
        "avg_rr_to_5pct": _round(mean(float(row["rr_to_5pct"]) for row in rows if row.get("rr_to_5pct") is not None)),
        "close10_short_ret_mean": _round(mean(returns)),
        "close10_short_ret_median": _round(median(returns)),
        "close10_short_positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "positive_month_rate": _round(sum(1 for values in usable_months if mean(values) > 0) / len(usable_months)) if usable_months else None,
        "positive_year_rate": _round(sum(1 for values in usable_years if mean(values) > 0) / len(usable_years)) if usable_years else None,
        "usable_month_count": len(usable_months),
        "usable_year_count": len(usable_years),
        "close10_short_ret_p10": _round(_pct(returns, 0.10)),
        "close10_short_ret_p90": _round(_pct(returns, 0.90)),
        "avg_proxy_breadth_above_ma20": _round(mean(_f(row.get("proxy_breadth_above_ma20")) for row in rows)),
        "avg_proxy_index_close_vs_ma20": _round(mean(_f(row.get("proxy_index_close_vs_ma20")) for row in rows)),
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary.get("event_count", 0) < 500:
        return "drop", "insufficient_sample"
    positive = (summary.get("close10_short_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.50
    year_ok = (summary.get("positive_year_rate") or 0) >= 0.60 and (summary.get("usable_year_count") or 0) >= 8
    stop_ok = (summary.get("stop_first_rate") or 1) <= 0.50
    target_ok = (summary.get("target5_first_rate") or 0) >= 0.30
    if positive and month_ok and year_ok and stop_ok and target_ok:
        return "keep", "long_proxy_regime_pattern_cleared_10d_short_edge_gates"
    if positive and (summary.get("usable_year_count") or 0) >= 5 and (month_ok or (summary.get("positive_year_rate") or 0) >= 0.55):
        return "hold", "positive_long_proxy_result_but_not_trade_ready"
    return "drop", "no_stable_long_proxy_market_weakness_edge"


def run(*, db_path: Path, output_root: Path, horizon: int, limit_codes: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        proxy = _market_proxy(conn)
        daily = _load_daily(conn, limit_codes)
    finally:
        conn.close()
    events: list[dict[str, Any]] = []
    for code, bars in daily.items():
        closes = [row["c"] for row in bars]
        lows = [row["l"] for row in bars]
        highs = [row["h"] for row in bars]
        vols = [row["v"] for row in bars]
        ma7s = _rolling_mean(closes, 7)
        ma20s = _rolling_mean(closes, 20)
        ma60s = _rolling_mean(closes, 60)
        vol20s = _rolling_mean(vols, 20)
        high20s = _rolling_max(highs, 20)
        high60s = _rolling_max(highs, 60)
        high120s = _rolling_max(highs, 120)
        low60s = _rolling_min(lows, 60)
        for index in range(120, len(bars) - horizon - 3):
            feat = _features(bars, index, closes, ma7s, ma20s, ma60s, vol20s, high20s, high60s, high120s, low60s)
            if feat is None:
                continue
            for base_tag in _tags(bars, index, feat):
                for entry in _confirmation_entries(bars, index, base_tag, ma7s, ma20s):
                    metrics = _evaluate(bars, index, entry, horizon)
                    if metrics is None:
                        continue
                    enriched = _attach_rr(metrics)
                    if enriched.get("rr_to_5pct") is None or float(enriched["rr_to_5pct"]) < RR_THRESHOLD:
                        continue
                    confirm_tag = str(enriched["confirm_tag"])
                    if confirm_tag not in FOCUS_PATTERNS:
                        continue
                    market = proxy.get(int(enriched["entry_ymd"]))
                    if not market:
                        continue
                    events.append(
                        {
                            "code": code,
                            "base_tag": base_tag,
                            **market,
                            **enriched,
                        }
                    )
    rows = []
    for confirm_tag in sorted({row["confirm_tag"] for row in events}):
        tag_rows = [row for row in events if row["confirm_tag"] == confirm_tag]
        for filter_id, spec in PROXY_FILTERS.items():
            filtered = [row for row in tag_rows if spec["fn"](row)]
            summary = _summary(filtered)
            decision, reason = _decision(summary)
            rows.append(
                {
                    "pattern_tag": confirm_tag,
                    "proxy_filter_id": filter_id,
                    "proxy_filter_description": spec["description"],
                    "decision": decision,
                    "reason": reason,
                    **summary,
                }
            )
    rows.sort(
        key=lambda row: (
            row["decision"] == "keep",
            row["decision"] == "hold",
            row.get("close10_short_ret_mean") or -1,
            row.get("positive_year_rate") or 0,
        ),
        reverse=True,
    )
    keep = [row for row in rows if row["decision"] == "keep"]
    hold = [row for row in rows if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_long_proxy_regime_branch"
        reason = "at_least_one_long_proxy_regime_pattern_cleared_10d_short_edge_gates"
    elif hold:
        rollup_decision = "hold_long_proxy_regime_branch"
        reason = "at_least_one_long_proxy_regime_pattern_positive_but_not_trade_ready"
    else:
        rollup_decision = "drop_long_proxy_regime_branch"
        reason = "no_long_proxy_regime_pattern_cleared_10d_short_edge_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "long_market_proxy_regime_only",
            "horizon": horizon,
            "rr_threshold_fixed": RR_THRESHOLD,
            "focus_patterns": sorted(FOCUS_PATTERNS),
            "proxy_filters": {key: value["description"] for key, value in PROXY_FILTERS.items()},
            "proxy_source": "daily_bars equal-weight breadth/index computed in-memory; no DB writes",
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "family_leaderboard": rows,
        "decision": {
            "candidate_local_decision": rollup_decision,
            "session_aggregate_decision": rollup_decision,
            "authoritative_rollup_decision": rollup_decision,
            "reason": reason,
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "compare.json", report)
    _write_json(output_dir / "family_leaderboard.json", {"rows": rows})
    _write_json(output_dir / "session_leaderboard_rollup.json", {"decision": report["decision"], "family_leaderboard": rows})
    _write_csv(output_dir / "event_sample.csv", events[:1000])
    _write_json(output_root / "latest_compare.json", {"run_root": str(output_dir), **report})
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "all_present": all(
                (output_dir / name).exists()
                for name in ["compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "event_sample.csv"]
            ),
            "authoritative_rollup_decision": rollup_decision,
            "run_root": str(output_dir),
        },
    )
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--horizon", type=int, default=10)
    parser.add_argument("--limit-codes", type=int, default=None)
    args = parser.parse_args()
    db_path = args.db_path or resolve_runtime_stock_db_path()
    print(run(db_path=db_path, output_root=args.output_root, horizon=args.horizon, limit_codes=args.limit_codes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
