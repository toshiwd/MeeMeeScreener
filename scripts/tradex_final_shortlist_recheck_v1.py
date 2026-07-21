from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_entry_shape_family_probe_v1 import _family_for


AXIS_ID = "final_shortlist_recheck_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_FINAL_RANK = Path(r"G:\Tradex\short_entry_shape_family_probe_v1\visual_final_rank\latest_visual_final_rank.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_shape_family_probe_v1\final_shortlist_recheck")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _normalize_date_expr() -> str:
    return """
    CASE
      WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
      ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _freshness(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT COALESCE(source, 'pan') AS src, max({_normalize_date_expr()}) AS max_ymd, count(*) AS row_count
        FROM daily_bars
        GROUP BY COALESCE(source, 'pan')
        ORDER BY src
        """
    ).fetchall()
    return [{"source": str(src), "max_ymd": int(max_ymd), "row_count": int(count)} for src, max_ymd, count in rows]


def _bars(conn: duckdb.DuckDBPyConnection, code: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
          SELECT {_normalize_date_expr()} AS ymd, COALESCE(source, 'pan') AS source, o, h, l, c, v
          FROM daily_bars
          WHERE code = ? AND COALESCE(source, 'pan') IN ('pan', 'yahoo')
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
        WHERE rn = 1
        ORDER BY ymd
        """,
        [code],
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


def _bars_for_family(bars: list[dict[str, Any]]) -> list[tuple[int, float, float, float, float, float]]:
    confirmed = [bar for bar in bars if bar["source"] != "yahoo"]
    return [
        (
            int(bar["ymd"]),
            float(bar["open"]),
            float(bar["high"]),
            float(bar["low"]),
            float(bar["close"]),
            float(bar["volume"] or 0),
        )
        for bar in confirmed
    ]


def _trigger_from_latest_confirmed(bars: list[dict[str, Any]]) -> dict[str, Any] | None:
    confirmed = [bar for bar in bars if bar["source"] != "yahoo"]
    if not confirmed:
        return None
    bar = confirmed[-1]
    high = float(bar["high"])
    low = float(bar["low"])
    close = float(bar["close"])
    candle_range = max(high - low, 0.0)
    close_below = min(close, low + candle_range * 0.35)
    return {
        "as_of": int(bar["ymd"]),
        "source_bar": bar,
        "entry_review_trigger_break_low": low,
        "entry_review_trigger_close_below": round(close_below, 4),
        "invalidate_if_high_breaks": high,
        "hard_invalidate_if_above": round(high + candle_range * 0.5, 4),
    }


def _evaluate(trigger: dict[str, Any], bars: list[dict[str, Any]]) -> dict[str, Any]:
    future = [bar for bar in bars if int(bar["ymd"]) > int(trigger["as_of"])]
    if not future:
        return {"status": "waiting_next_bar", "reason": "no bar after confirmed trigger date", "evaluated_bar": None}
    bar = future[0]
    if bar["high"] > float(trigger["hard_invalidate_if_above"]):
        status = "hard_invalidated"
        reason = "future bar high exceeded hard invalidation"
    elif bar["high"] > float(trigger["invalidate_if_high_breaks"]):
        status = "invalidated_by_high_break"
        reason = "future bar high exceeded trigger invalidation"
    elif bar["low"] < float(trigger["entry_review_trigger_break_low"]) and bar["close"] <= float(trigger["entry_review_trigger_close_below"]):
        status = "confirmed_downside_rejection"
        reason = "future bar broke low and closed below trigger close threshold"
    elif bar["low"] < float(trigger["entry_review_trigger_break_low"]):
        status = "intraday_low_break_only"
        reason = "future bar broke low without close confirmation"
    elif bar["close"] <= float(trigger["entry_review_trigger_close_below"]):
        status = "close_rejection_only"
        reason = "future bar closed below trigger close threshold without low break"
    else:
        status = "still_waiting"
        reason = "future bar has not confirmed breakdown or invalidated"
    return {"status": status, "reason": reason, "evaluated_bar": bar}


def _decision(status: str, visual_decision: str) -> str:
    if visual_decision != "keep":
        return "not_final_keep_candidate"
    if status in {"hard_invalidated", "invalidated_by_high_break"}:
        return "drop_invalidated"
    if status == "confirmed_downside_rejection":
        return "keep_strong_review"
    if status in {"intraday_low_break_only", "close_rejection_only"}:
        return "hold_probe_review"
    return "hold_waiting"


def run(*, db_path: Path, final_rank_path: Path, output_root: Path) -> Path:
    final_rank = json.loads(final_rank_path.read_text(encoding="utf-8-sig"))
    targets = [row for row in final_rank.get("rows", []) if row.get("decision") == "keep"]
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        freshness = _freshness(conn)
        rows = []
        for target in targets:
            bars = _bars(conn, str(target["code"]))
            trigger = _trigger_from_latest_confirmed(bars)
            shape_classification = _family_for(_bars_for_family(bars), int(trigger["as_of"])) if trigger else None
            recheck = _evaluate(trigger, bars) if trigger else {"status": "no_confirmed_bar", "reason": "missing confirmed bar", "evaluated_bar": None}
            rows.append(
                {
                    "code": str(target["code"]),
                    "name": str(target["name"]),
                    "visual_rank": target.get("rank"),
                    "visual_decision": target.get("decision"),
                    "shape_family": shape_classification.get("shape_family") if shape_classification else None,
                    "ma_shape_family": shape_classification.get("ma_shape_family") if shape_classification else None,
                    "shape_classification": shape_classification,
                    "trigger": trigger,
                    "recheck": recheck,
                    "candidate_local_decision": _decision(str(recheck["status"]), str(target.get("decision"))),
                }
            )
    finally:
        conn.close()
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "source_final_rank": str(final_rank_path),
        "runtime_freshness_by_source": freshness,
        "rows": rows,
        "decision": {
            "candidate_local_decision": "final_keep_recheck_available",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "visual keep candidates rechecked against latest confirmed plus yahoo overlay bars",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "final_shortlist_recheck.json", report)
    _write_json(output_root / "latest_final_shortlist_recheck.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--final-rank", type=Path, default=DEFAULT_FINAL_RANK)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, final_rank_path=args.final_rank, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
