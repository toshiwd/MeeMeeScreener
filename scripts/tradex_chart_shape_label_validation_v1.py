from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backend.services.chart_shape_service import (
    classify_daily_chart_shape,
    get_chart_shape_pattern_catalog,
)
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/chart_shape_label_validation_v1")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * q)))
    return ordered[idx]


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


FORWARD_RETURN_FIELDS = ["forward_return_5", "forward_return_20", "forward_return_30", "forward_return_60"]


def _summarize_return_values(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "mean": _round(_mean(values)),
        "median": _round(_quantile(values, 0.5)),
        "positive_rate": _round(sum(1 for value in values if value > 0) / len(values) if values else None),
        "p10": _round(_quantile(values, 0.1)),
        "p90": _round(_quantile(values, 0.9)),
    }


def _summarize_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns_by_horizon: dict[str, dict[str, Any]] = {}
    for field in FORWARD_RETURN_FIELDS:
        values = [_safe_float(row.get(field)) for row in rows]
        returns_by_horizon[field] = _summarize_return_values([value for value in values if value is not None])
    returns_20 = [_safe_float(row.get("forward_return_20")) for row in rows]
    returns_20 = [value for value in returns_20 if value is not None]
    favorable_30 = [_safe_float(row.get("max_favorable_30")) for row in rows]
    favorable_30 = [value for value in favorable_30 if value is not None]
    adverse_30 = [_safe_float(row.get("max_adverse_30")) for row in rows]
    adverse_30 = [value for value in adverse_30 if value is not None]
    return {
        "sample_count": len(rows),
        "forward_returns": returns_by_horizon,
        "forward_return_20_count": len(returns_20),
        "forward_return_20_mean": _round(_mean(returns_20)),
        "forward_return_20_median": _round(_quantile(returns_20, 0.5)),
        "forward_return_20_positive_rate": _round(
            sum(1 for value in returns_20 if value > 0) / len(returns_20) if returns_20 else None
        ),
        "forward_return_20_p10": _round(_quantile(returns_20, 0.1)),
        "forward_return_20_p90": _round(_quantile(returns_20, 0.9)),
        "max_favorable_30_mean": _round(_mean(favorable_30)),
        "max_adverse_30_mean": _round(_mean(adverse_30)),
    }


def _classify_tendency(summary: dict[str, Any], baseline: dict[str, Any], *, min_count: int = 30) -> dict[str, Any]:
    count = int(summary.get("forward_return_20_count") or 0)
    mean_20 = _safe_float(summary.get("forward_return_20_mean"))
    median_20 = _safe_float(summary.get("forward_return_20_median"))
    positive_rate = _safe_float(summary.get("forward_return_20_positive_rate"))
    baseline_mean = _safe_float(baseline.get("forward_return_20_mean"))
    baseline_positive_rate = _safe_float(baseline.get("forward_return_20_positive_rate"))
    mean_delta = None if mean_20 is None or baseline_mean is None else mean_20 - baseline_mean
    positive_rate_delta = (
        None if positive_rate is None or baseline_positive_rate is None else positive_rate - baseline_positive_rate
    )
    if count < min_count:
        tendency = "insufficient_sample"
        reason = "sample_count_below_min_count"
    elif mean_delta is not None and positive_rate_delta is not None and mean_delta >= 0.01 and positive_rate_delta >= 0.03:
        tendency = "up_tendency"
        reason = "mean_and_positive_rate_above_baseline"
    elif mean_delta is not None and positive_rate_delta is not None and mean_delta <= -0.01 and positive_rate_delta <= -0.03:
        tendency = "down_tendency"
        reason = "mean_and_positive_rate_below_baseline"
    elif mean_20 is not None and median_20 is not None and mean_20 < 0 and median_20 < 0:
        tendency = "weak_down_tendency"
        reason = "mean_and_median_negative"
    elif mean_20 is not None and median_20 is not None and mean_20 > 0 and median_20 > 0:
        tendency = "weak_up_tendency"
        reason = "mean_and_median_positive"
    else:
        tendency = "mixed_or_neutral"
        reason = "directional_metrics_mixed"
    return {
        "tendency": tendency,
        "reason": reason,
        "sample_count": count,
        "min_count": min_count,
        "forward_return_20_mean_delta_vs_baseline": _round(mean_delta),
        "forward_return_20_positive_rate_delta_vs_baseline": _round(positive_rate_delta),
    }


def _load_signal_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    start_dt: int,
    end_dt: int,
    side: str,
    entry_qualified_only: bool,
    limit: int | None,
) -> list[dict[str, Any]]:
    query = """
        SELECT dt, code, side, name, entry_qualified, setup_type,
               forward_return_5, forward_return_20, forward_return_30, forward_return_60,
               max_favorable_30, max_adverse_30
        FROM signal_decision_daily
        WHERE dt BETWEEN ? AND ?
          AND side = ?
          AND forward_return_20 IS NOT NULL
    """
    params: list[Any] = [start_dt, end_dt, side]
    if entry_qualified_only:
        query += " AND entry_qualified = true"
    query += " ORDER BY dt, code"
    if limit is not None and limit > 0:
        query += " LIMIT ?"
        params.append(int(limit))
    return [
        {
            "dt": int(row[0]),
            "code": str(row[1]),
            "side": str(row[2]),
            "name": row[3],
            "entry_qualified": bool(row[4]) if row[4] is not None else None,
            "setup_type": row[5],
            "forward_return_5": row[6],
            "forward_return_20": row[7],
            "forward_return_30": row[8],
            "forward_return_60": row[9],
            "max_favorable_30": row[10],
            "max_adverse_30": row[11],
        }
        for row in con.execute(query, params).fetchall()
    ]


def _load_bars_for_code_dates(
    con: duckdb.DuckDBPyConnection,
    *,
    code: str,
    max_dt: int,
    limit: int,
) -> list[tuple[Any, ...]]:
    return con.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8
                        THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                date, o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
        )
        SELECT date, o, h, l, c, v
        FROM normalized
        WHERE ymd <= ?
        ORDER BY ymd DESC
        LIMIT ?
        """,
        [code, max_dt, limit],
    ).fetchall()[::-1]


def _load_all_bars_for_code(
    con: duckdb.DuckDBPyConnection,
    *,
    code: str,
    max_dt: int,
) -> list[tuple[int, Any, float, float, float, float, float]]:
    return con.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8
                        THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                date, o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
        )
        SELECT ymd, date, o, h, l, c, v
        FROM normalized
        WHERE ymd <= ?
        ORDER BY ymd
        """,
        [code, max_dt],
    ).fetchall()


def _bars_asof(
    bars: list[tuple[int, Any, float, float, float, float, float]],
    *,
    asof_dt: int,
    limit: int,
) -> list[tuple[Any, float, float, float, float, float]]:
    eligible = [row for row in bars if int(row[0]) <= asof_dt]
    return [(row[1], row[2], row[3], row[4], row[5], row[6]) for row in eligible[-limit:]]


def _daily_bar_date_mode(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    row = con.execute(
        """
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN 1 ELSE 0 END) AS yyyymmdd_rows,
            SUM(CASE WHEN length(CAST(abs(date) AS VARCHAR)) <> 8 THEN 1 ELSE 0 END) AS epoch_like_rows
        FROM daily_bars
        """
    ).fetchone()
    return {
        "rows": int(row[0] or 0),
        "yyyymmdd_rows": int(row[1] or 0),
        "epoch_like_rows": int(row[2] or 0),
    }


def run_validation(
    *,
    db_path: Path,
    output_root: Path,
    start_dt: int,
    end_dt: int,
    side: str,
    window: int,
    entry_qualified_only: bool,
    limit: int | None,
) -> dict[str, Any]:
    entry_scope = "entryq" if entry_qualified_only else "all"
    output_dir = output_root / f"{_now_tag()}-{side}-{entry_scope}-chart_shape_label_validation_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = _load_signal_rows(
            con,
            start_dt=start_dt,
            end_dt=end_dt,
            side=side,
            entry_qualified_only=entry_qualified_only,
            limit=limit,
        )
        by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_code[str(row["code"])].append(row)

        labeled: list[dict[str, Any]] = []
        missing_history = 0
        for code, code_rows in sorted(by_code.items()):
            max_code_dt = max(int(row["dt"]) for row in code_rows)
            all_bars = _load_all_bars_for_code(con, code=code, max_dt=max_code_dt)
            for row in code_rows:
                dt = int(row["dt"])
                bars = _bars_asof(all_bars, asof_dt=dt, limit=window + 1)
                shape = classify_daily_chart_shape(bars, requested_window=window)
                if not shape.get("confirmed"):
                    missing_history += 1
                labeled.append(
                    {
                        **row,
                        "shape_label": shape.get("shape_label"),
                        "shape_family": shape.get("shape_family"),
                        "bias": shape.get("bias"),
                        "actionability": shape.get("actionability"),
                        "shape_confidence": shape.get("confidence"),
                        "shape_reasons": shape.get("reasons") or [],
                        "shape_metrics": shape.get("metrics") or {},
                    }
                )

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        bias_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        action_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in labeled:
            groups[str(row.get("shape_label") or "unknown")].append(row)
            bias_groups[str(row.get("bias") or "unknown")].append(row)
            action_groups[str(row.get("actionability") or "unknown")].append(row)

        overall_summary = _summarize_group(labeled)
        shape_summary = {
            label: _summarize_group(group_rows)
            for label, group_rows in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))
        }
        bias_summary = {
            label: _summarize_group(group_rows)
            for label, group_rows in sorted(bias_groups.items(), key=lambda item: (-len(item[1]), item[0]))
        }
        actionability_summary = {
            label: _summarize_group(group_rows)
            for label, group_rows in sorted(action_groups.items(), key=lambda item: (-len(item[1]), item[0]))
        }
        shape_tendency = {
            label: _classify_tendency(summary, overall_summary)
            for label, summary in shape_summary.items()
        }
        bias_tendency = {
            label: _classify_tendency(summary, overall_summary)
            for label, summary in bias_summary.items()
        }
        actionability_tendency = {
            label: _classify_tendency(summary, overall_summary)
            for label, summary in actionability_summary.items()
        }
        result = {
            "authoritative_result": True,
            "research_phase": "effectiveness_judgment",
            "scope": "TRADEX-only chart shape label validation",
            "silent_fallback_used": False,
            "meemee_ranking_changed": False,
            "meemee_ui_changed_by_this_run": False,
            "fixed_evaluation_conditions": {
                "source_table": "signal_decision_daily",
                "price_source": "daily_bars",
                "start_dt": start_dt,
                "end_dt": end_dt,
                "side": side,
                "window": window,
                "entry_qualified_only": entry_qualified_only,
                "limit": limit,
                "forward_metrics": FORWARD_RETURN_FIELDS,
                "primary_forward_metric": "forward_return_20",
                "path_metrics": ["max_favorable_30", "max_adverse_30"],
            },
            "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
            "daily_bar_date_mode": _daily_bar_date_mode(con),
            "pattern_catalog": get_chart_shape_pattern_catalog(),
            "coverage": {
                "input_signal_rows": len(rows),
                "labeled_rows": len(labeled),
                "missing_history_rows": missing_history,
                "label_count": len(shape_summary),
            },
            "overall_summary": overall_summary,
            "shape_summary": shape_summary,
            "shape_tendency": shape_tendency,
            "bias_summary": bias_summary,
            "bias_tendency": bias_tendency,
            "actionability_summary": actionability_summary,
            "actionability_tendency": actionability_tendency,
            "judgment": {
                "decision": "not-yet-promotable",
                "reason": "diagnostic_tendency_analysis_no_out_of_sample_validation",
                "meemee_reflectable": False,
            },
        }
        (output_dir / "chart_shape_label_validation_summary.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        ledger_path = output_dir / "chart_shape_label_validation_ledger.jsonl"
        with ledger_path.open("w", encoding="utf-8") as fh:
            for row in labeled:
                fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        result["artifacts"] = {
            "output_dir": str(output_dir),
            "summary_json": str(output_dir / "chart_shape_label_validation_summary.json"),
            "ledger_jsonl": str(ledger_path),
        }
        (output_dir / "_ARTIFACT_COMPLETE.json").write_text(
            json.dumps(
                {
                    "complete": True,
                    "summary_json": result["artifacts"]["summary_json"],
                    "ledger_jsonl": result["artifacts"]["ledger_jsonl"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-dt", type=int, default=20250101)
    parser.add_argument("--end-dt", type=int, default=20260515)
    parser.add_argument("--side", choices=["buy", "sell"], default="buy")
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--entry-qualified-only", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    result = run_validation(
        db_path=args.db_path,
        output_root=args.output_root,
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        side=args.side,
        window=args.window,
        entry_qualified_only=bool(args.entry_qualified_only),
        limit=args.limit,
    )
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps(result["judgment"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
