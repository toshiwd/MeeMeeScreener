from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_entry_timing_trigger_board_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_WATCH_BOARD = Path(
    r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates\latest_provisional_watch_board.json"
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


def _latest_bar(conn: duckdb.DuckDBPyConnection, code: str, as_of: int) -> dict[str, Any] | None:
    row = conn.execute(
        f"""
        SELECT COALESCE(source, 'pan') AS source, {_normalize_expr()} AS ymd, o, h, l, c, v
        FROM daily_bars
        WHERE code = ?
          AND {_normalize_expr()} = ?
        ORDER BY CASE WHEN COALESCE(source, 'pan') = 'yahoo' THEN 1 ELSE 0 END DESC
        LIMIT 1
        """,
        [code, as_of],
    ).fetchone()
    if row is None:
        return None
    source, ymd, open_, high, low, close, volume = row
    return {
        "source": str(source),
        "ymd": int(ymd),
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(volume or 0),
    }


def _trigger_plan(row: dict[str, Any], bar: dict[str, Any]) -> dict[str, Any]:
    high = float(bar["high"])
    low = float(bar["low"])
    close = float(bar["close"])
    candle_range = max(high - low, 0.0)
    upper_wick = high - max(float(bar["open"]), close)
    entry_break_low = low
    reject_confirm_close = low + candle_range * 0.35
    invalid_high = high
    hard_invalid = high * 1.015
    verdict = row.get("provisional_board_verdict")
    if verdict == "top_watch_rejection_needed":
        action_rank = 1
        trigger_policy = "watch for close below provisional lower 35pct or intraday break below provisional low"
    elif verdict == "watch_rejection_needed":
        action_rank = 2
        trigger_policy = "watch only; require fresh rejection close below provisional midpoint-to-low zone"
    elif verdict == "secondary_watch_only":
        action_rank = 3
        trigger_policy = "secondary watch; require confirmed bearish follow-through before review"
    else:
        action_rank = 9
        trigger_policy = "avoid unless a new independent setup forms"
    return {
        "action_rank": action_rank,
        "trigger_policy": trigger_policy,
        "entry_review_trigger_break_low": round(entry_break_low, 4),
        "entry_review_trigger_close_below": round(reject_confirm_close, 4),
        "invalidate_if_high_breaks": round(invalid_high, 4),
        "hard_invalidate_if_above": round(hard_invalid, 4),
        "provisional_upper_wick_ratio": round(upper_wick / candle_range, 6) if candle_range > 0 else None,
        "provisional_body_ratio": round(abs(close - float(bar["open"])) / candle_range, 6) if candle_range > 0 else None,
        "risk_width_high_to_low_pct": round(high / low - 1.0, 6) if low > 0 else None,
    }


def run(*, db_path: Path, watch_board_path: Path, output_root: Path) -> Path:
    board = json.loads(watch_board_path.read_text(encoding="utf-8"))
    rows = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for row in board.get("rows", []):
            bar = _latest_bar(conn, str(row["code"]), int(board["provisional_as_of"]))
            if bar is None:
                rows.append({**row, "trigger_status": "missing_bar"})
                continue
            rows.append({**row, "provisional_bar": bar, "trigger_plan": _trigger_plan(row, bar)})
    finally:
        conn.close()
    rows.sort(
        key=lambda row: (
            row.get("trigger_plan", {}).get("action_rank", 99),
            -int(len(row.get("matched_rules", []))),
            str(row.get("code")),
        )
    )
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "source_watch_board": str(watch_board_path),
        "provisional_as_of": board.get("provisional_as_of"),
        "source_policy": "yahoo provisional watch levels; review-only until confirmed",
        "rows": rows,
        "decision": {
            "candidate_local_decision": "provisional_trigger_board_ready",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "watch candidates now have explicit next-bar continuation and invalidation levels",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "provisional_trigger_board.json", report)
    _write_json(output_root / "latest_provisional_trigger_board.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--watch-board", type=Path, default=DEFAULT_WATCH_BOARD)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, watch_board_path=args.watch_board, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
