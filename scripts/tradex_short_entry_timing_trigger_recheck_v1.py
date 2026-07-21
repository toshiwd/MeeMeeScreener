from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_entry_timing_trigger_recheck_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_TRIGGER_BOARD = Path(
    r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates\latest_provisional_trigger_board.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _normalize_expr() -> str:
    return """
    CASE
        WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
        ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _freshness(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT COALESCE(source, 'pan') AS src, max({_normalize_expr()}) AS max_ymd, count(*) AS row_count
        FROM daily_bars
        GROUP BY COALESCE(source, 'pan')
        ORDER BY src
        """
    ).fetchall()
    return [{"source": str(source), "max_ymd": int(max_ymd), "row_count": int(row_count)} for source, max_ymd, row_count in rows]


def _bars_after(conn: duckdb.DuckDBPyConnection, code: str, after_ymd: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                {_normalize_expr()} AS ymd,
                COALESCE(source, 'pan') AS source,
                o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
              AND COALESCE(source, 'pan') IN ('pan', 'yahoo')
              AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        ),
        ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY ymd
                       ORDER BY CASE WHEN source = 'yahoo' THEN 1 ELSE 0 END DESC
                   ) AS rn
            FROM normalized
        )
        SELECT ymd, source, o, h, l, c, v
        FROM ranked
        WHERE rn = 1 AND ymd > ?
        ORDER BY ymd
        """,
        [code, after_ymd],
    ).fetchall()
    return [
        {
            "ymd": int(ymd),
            "source": str(source),
            "open": float(open_),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume or 0),
        }
        for ymd, source, open_, high, low, close, volume in rows
    ]


def _evaluate(row: dict[str, Any], future_bars: list[dict[str, Any]]) -> dict[str, Any]:
    plan = row.get("trigger_plan", {})
    if not future_bars:
        return {
            "trigger_status": "waiting_next_bar",
            "reason": "no bar after trigger board as_of",
            "evaluated_bar": None,
        }
    bar = future_bars[0]
    break_low = float(plan["entry_review_trigger_break_low"])
    close_below = float(plan["entry_review_trigger_close_below"])
    invalid_high = float(plan["invalidate_if_high_breaks"])
    hard_invalid = float(plan["hard_invalidate_if_above"])
    if bar["high"] > hard_invalid:
        status = "hard_invalidated"
        reason = "next bar high exceeded hard invalidation level"
    elif bar["high"] > invalid_high:
        status = "invalidated"
        reason = "next bar high exceeded provisional high"
    elif bar["low"] < break_low and bar["close"] <= close_below:
        status = "triggered_strong_rejection"
        reason = "next bar broke provisional low and closed in lower rejection zone"
    elif bar["low"] < break_low:
        status = "triggered_intraday_low_break_only"
        reason = "next bar broke provisional low but close confirmation is missing"
    elif bar["close"] <= close_below:
        status = "triggered_close_rejection_only"
        reason = "next bar closed in lower rejection zone without low break"
    else:
        status = "still_waiting"
        reason = "next bar did not trigger rejection or invalidation"
    return {
        "trigger_status": status,
        "reason": reason,
        "evaluated_bar": bar,
        "remaining_future_bars": future_bars[1:],
    }


def run(*, db_path: Path, trigger_board_path: Path, output_root: Path) -> Path:
    board = json.loads(trigger_board_path.read_text(encoding="utf-8"))
    rows = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        freshness = _freshness(conn)
        for row in board.get("rows", []):
            as_of = int(row.get("provisional_bar", {}).get("ymd") or board.get("provisional_as_of"))
            future = _bars_after(conn, str(row["code"]), as_of)
            rows.append({**row, "recheck": _evaluate(row, future)})
    finally:
        conn.close()
    status_counts: dict[str, int] = {}
    for row in rows:
        status = str(row["recheck"]["trigger_status"])
        status_counts[status] = status_counts.get(status, 0) + 1
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "source_trigger_board": str(trigger_board_path),
        "runtime_freshness_by_source": freshness,
        "status_counts": status_counts,
        "rows": rows,
        "decision": {
            "candidate_local_decision": "recheck_waiting_next_bar"
            if status_counts.get("waiting_next_bar") == len(rows)
            else "recheck_status_available",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "trigger board rechecked against latest DB; statuses are review-only",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "trigger_recheck.json", report)
    _write_json(output_root / "latest_trigger_recheck.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--trigger-board", type=Path, default=DEFAULT_TRIGGER_BOARD)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, trigger_board_path=args.trigger_board, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
