from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from scripts.tradex_short_collapse_shape_10d_compare_v1 import (
    _load_daily,
    _rolling_max,
    _rolling_mean,
    _rolling_min,
    _round,
)
from scripts.tradex_short_top_failed_high_confirm_10d_compare_v1 import (
    _confirmation_entries,
    _evaluate,
    _summarize,
)
from scripts.tradex_short_top_failed_high_rr_10d_compare_v1 import _attach_rr
from scripts.tradex_short_top_first_failure_10d_compare_v1 import _features, _tags


AXIS_ID = "short_top_failed_high_market_weakness_10d_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_top_failed_high_market_weakness_10d_compare_v1")
RR_THRESHOLD = 1.0


MARKET_FILTERS: dict[str, dict[str, Any]] = {
    "all_rr_ge_1": {"description": "RR>=1.0 only", "fn": lambda r: True},
    "regime_risk_off_or_chaos": {
        "description": "regime_id is risk_off_trend or high_vol_chaos",
        "fn": lambda r: str(r.get("regime_id") or "") in {"risk_off_trend", "high_vol_chaos"},
    },
    "negative_regime_score": {
        "description": "regime_score <= -0.5",
        "fn": lambda r: _f(r.get("regime_score")) <= -0.5,
    },
    "weak_breadth_ma20": {
        "description": "breadth_above_ma20 <= 0.45",
        "fn": lambda r: _f(r.get("breadth_above_ma20"), 1.0) <= 0.45,
    },
    "index_below_ma20": {
        "description": "index_close_vs_ma20 <= 0",
        "fn": lambda r: _f(r.get("index_close_vs_ma20"), 1.0) <= 0.0,
    },
    "weak_combo": {
        "description": "breadth_above_ma20 <= 0.45 and index_close_vs_ma20 <= 0",
        "fn": lambda r: _f(r.get("breadth_above_ma20"), 1.0) <= 0.45 and _f(r.get("index_close_vs_ma20"), 1.0) <= 0.0,
    },
    "risk_off_combo": {
        "description": "regime_score <= -0.5 and index_close_vs_ma20 <= 0",
        "fn": lambda r: _f(r.get("regime_score")) <= -0.5 and _f(r.get("index_close_vs_ma20"), 1.0) <= 0.0,
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


def _regime_rows(conn: duckdb.DuckDBPyConnection) -> dict[int, dict[str, Any]]:
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='market_regime_daily'"
    ).fetchone()[0]
    if not exists:
        return {}
    rows = conn.execute(
        """
        SELECT
          CAST(dt AS INTEGER) AS ymd,
          regime_id,
          breadth_above_ma20,
          breadth_above_ma60,
          advancers_ratio,
          index_close_vs_ma20,
          index_close_vs_ma60,
          market_atr_pct,
          sector_dispersion,
          regime_score
        FROM market_regime_daily
        """
    ).fetchall()
    cols = [
        "ymd",
        "regime_id",
        "breadth_above_ma20",
        "breadth_above_ma60",
        "advancers_ratio",
        "index_close_vs_ma20",
        "index_close_vs_ma60",
        "market_atr_pct",
        "sector_dispersion",
        "regime_score",
    ]
    return {int(row[0]): dict(zip(cols, row)) for row in rows}


def _summary_with_market(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = _summarize(rows)
    if not rows:
        return base
    covered = [row for row in rows if row.get("regime_id") is not None]
    return {
        **base,
        "market_coverage_rate": _round(len(covered) / len(rows)),
        "avg_regime_score": _round(mean(_f(row.get("regime_score")) for row in covered)) if covered else None,
        "avg_breadth_above_ma20": _round(mean(_f(row.get("breadth_above_ma20")) for row in covered)) if covered else None,
        "avg_index_close_vs_ma20": _round(mean(_f(row.get("index_close_vs_ma20")) for row in covered)) if covered else None,
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary.get("event_count", 0) < 200:
        return "drop", "insufficient_sample"
    if (summary.get("market_coverage_rate") or 0) < 0.5:
        return "drop", "insufficient_market_regime_coverage"
    positive = (summary.get("close10_short_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.50
    year_ok = (summary.get("positive_year_rate") or 0) >= 0.60
    stop_ok = (summary.get("stop_first_rate") or 1) <= 0.45
    target_ok = (summary.get("target5_first_rate") or 0) >= 0.35
    risk_ok = (summary.get("avg_stop_pct") or 1) <= 0.065
    if positive and month_ok and year_ok and stop_ok and target_ok and risk_ok:
        return "keep", "market_filtered_failed_high_pattern_cleared_10d_short_edge_gates"
    if positive and risk_ok and (month_ok or year_ok):
        return "hold", "positive_return_but_market_filtered_stability_or_target_gate_incomplete"
    return "drop", "no_positive_10d_market_filtered_failed_high_edge"


def run(*, db_path: Path, output_root: Path, horizon: int, limit_codes: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        regimes = _regime_rows(conn)
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
                    regime = regimes.get(int(enriched["entry_ymd"]), {})
                    events.append(
                        {
                            "code": code,
                            "base_tag": base_tag,
                            **{
                                key: _round(feat[key])
                                for key in [
                                    "ret20",
                                    "ret60",
                                    "ret120",
                                    "volume_vs20",
                                    "close_pos",
                                    "upper_wick",
                                    "range60_pos",
                                    "dist_ma7",
                                    "dist_ma20",
                                    "close_vs_high60",
                                ]
                            },
                            **regime,
                            **enriched,
                        }
                    )
    rows = []
    for confirm_tag in sorted({row["confirm_tag"] for row in events}):
        tag_rows = [row for row in events if row["confirm_tag"] == confirm_tag]
        for filter_id, spec in MARKET_FILTERS.items():
            filtered = [row for row in tag_rows if spec["fn"](row)]
            summary = _summary_with_market(filtered)
            decision, reason = _decision(summary)
            rows.append(
                {
                    "pattern_tag": confirm_tag,
                    "market_filter_id": filter_id,
                    "market_filter_description": spec["description"],
                    "rr_threshold": RR_THRESHOLD,
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
            row.get("target5_first_rate") or 0,
        ),
        reverse=True,
    )
    keep = [row for row in rows if row["decision"] == "keep"]
    hold = [row for row in rows if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_failed_high_market_weakness_branch"
        reason = "at_least_one_market_filtered_failed_high_pattern_cleared_10d_short_edge_gates"
    elif hold:
        rollup_decision = "hold_failed_high_market_weakness_branch"
        reason = "at_least_one_market_filtered_failed_high_pattern_positive_but_not_trade_ready"
    else:
        rollup_decision = "drop_failed_high_market_weakness_branch"
        reason = "no_market_filtered_failed_high_pattern_cleared_10d_short_edge_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "market_weakness_filter_only",
            "horizon": horizon,
            "base_signal": "top_first_failure_tags",
            "confirmation_window": "1_to_3_sessions_after_base_signal",
            "entry": "confirmation_day_close",
            "stop_observation": "base_signal_high_plus_0.5pct",
            "rr_threshold_fixed": RR_THRESHOLD,
            "market_filters": {key: value["description"] for key, value in MARKET_FILTERS.items()},
            "confirmed_non_yahoo_daily_bars": True,
            "market_regime_daily_join": "entry_ymd equals market_regime_daily.dt",
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
