from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services import rankings_cache as rc  # noqa: E402


@dataclass
class Candidate:
    name: str
    prob_bal: float
    turn_bal: float
    pressure_score_bal: float
    pressure_max_ev_bal: float
    overheat_strong_prob: float
    overheat_strong_turn: float


def _resolve_db_path(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path
    env = os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path
    default = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    if default.exists():
        return default
    raise FileNotFoundError("Could not resolve DB path. Pass --db-path or set STOCKS_DB_PATH.")


def _month_end_dates(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> list[int]:
    ymd_expr = """
        CASE
          WHEN TRY_CAST(date AS BIGINT) BETWEEN 19000101 AND 20991231 THEN CAST(date AS BIGINT)
          WHEN TRY_CAST(date AS BIGINT) >= 1000000000000 THEN CAST(strftime(to_timestamp(CAST(date AS BIGINT)/1000), '%Y%m%d') AS INTEGER)
          WHEN TRY_CAST(date AS BIGINT) BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
          ELSE NULL
        END
    """
    rows = conn.execute(
        f"""
        WITH d AS (
          SELECT {ymd_expr} AS ymd
          FROM daily_bars
        ),
        m AS (
          SELECT (ymd/100)::INT AS ym, MAX(ymd) AS asof_ymd
          FROM d
          WHERE ymd BETWEEN ? AND ?
          GROUP BY (ymd/100)::INT
        )
        SELECT asof_ymd
        FROM m
        ORDER BY asof_ymd
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _quarter_end_set(month_end_dates: list[int]) -> set[int]:
    quarter_months = {3, 6, 9, 12}
    return {ymd for ymd in month_end_dates if int(str(ymd)[4:6]) in quarter_months}


def _load_quarter_outcomes(
    conn: duckdb.DuckDBPyConnection,
    *,
    quarter_dates: list[int],
) -> dict[tuple[int, str], float]:
    if not quarter_dates:
        return {}
    placeholders = ", ".join("?" for _ in quarter_dates)
    ymd_expr = """
        CASE
          WHEN TRY_CAST(dt AS BIGINT) BETWEEN 19000101 AND 20991231 THEN CAST(dt AS BIGINT)
          WHEN TRY_CAST(dt AS BIGINT) >= 1000000000000 THEN CAST(strftime(to_timestamp(CAST(dt AS BIGINT)/1000), '%Y%m%d') AS INTEGER)
          WHEN TRY_CAST(dt AS BIGINT) BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp(CAST(dt AS BIGINT)), '%Y%m%d') AS INTEGER)
          ELSE NULL
        END
    """
    rows = conn.execute(
        f"""
        SELECT
          {ymd_expr} AS ymd,
          CAST(code AS VARCHAR) AS code,
          CAST(short_ret_20 AS DOUBLE) AS short_ret_20
        FROM sell_analysis_daily
        WHERE {ymd_expr} IN ({placeholders})
        """,
        [int(v) for v in quarter_dates],
    ).fetchall()
    out: dict[tuple[int, str], float] = {}
    for ymd, code, short_ret_20 in rows:
        if ymd is None or code is None or short_ret_20 is None:
            continue
        out[(int(ymd), str(code))] = float(short_ret_20)
    return out


def _snapshot_constants() -> dict[str, float]:
    keys = (
        "_ENTRY_SHORT_MIN_PROB_BALANCED",
        "_ENTRY_SHORT_MIN_TURN_BALANCED",
        "_ENTRY_SHORT_PRESSURE_SCORE_BALANCED",
        "_ENTRY_SHORT_PRESSURE_MAX_EV_BALANCED",
        "_ENTRY_SHORT_OVERHEAT_STRONG_PROB",
        "_ENTRY_SHORT_OVERHEAT_STRONG_TURN",
    )
    return {k: float(getattr(rc, k)) for k in keys}


def _apply_candidate(c: Candidate) -> None:
    rc._ENTRY_SHORT_MIN_PROB_BALANCED = float(c.prob_bal)
    rc._ENTRY_SHORT_MIN_TURN_BALANCED = float(c.turn_bal)
    rc._ENTRY_SHORT_PRESSURE_SCORE_BALANCED = float(c.pressure_score_bal)
    rc._ENTRY_SHORT_PRESSURE_MAX_EV_BALANCED = float(c.pressure_max_ev_bal)
    rc._ENTRY_SHORT_OVERHEAT_STRONG_PROB = float(c.overheat_strong_prob)
    rc._ENTRY_SHORT_OVERHEAT_STRONG_TURN = float(c.overheat_strong_turn)


def _restore_constants(snapshot: dict[str, float]) -> None:
    for k, v in snapshot.items():
        setattr(rc, k, float(v))


def _candidates() -> list[Candidate]:
    return [
        Candidate("baseline", 0.58, 0.62, 0.83, -0.0020, 0.66, 0.64),
        Candidate("relax_1", 0.57, 0.61, 0.82, -0.0015, 0.66, 0.64),
        Candidate("relax_2", 0.56, 0.60, 0.81, -0.0010, 0.65, 0.63),
        Candidate("relax_3", 0.55, 0.59, 0.80, -0.0005, 0.65, 0.63),
        Candidate("relax_4", 0.54, 0.58, 0.79, 0.0000, 0.64, 0.62),
        Candidate("prob_soft_turn_keep", 0.55, 0.62, 0.81, -0.0010, 0.65, 0.63),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Short gate tuning study for down/balanced in ranking.")
    parser.add_argument("--db-path", default="", help="stocks.duckdb path")
    parser.add_argument("--start-ymd", type=int, default=20160226)
    parser.add_argument("--end-ymd", type=int, default=20260226)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--output", default="tmp/short_gate_tuning_study_20260227.json")
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path or None)
    os.environ["STOCKS_DB_PATH"] = str(db_path)

    with duckdb.connect(str(db_path), read_only=True) as conn:
        month_dates = _month_end_dates(conn, start_ymd=int(args.start_ymd), end_ymd=int(args.end_ymd))
        quarter_set = _quarter_end_set(month_dates)
        quarter_dates = [d for d in month_dates if d in quarter_set]
        q_outcomes = _load_quarter_outcomes(conn, quarter_dates=quarter_dates)

    candidates = _candidates()
    base_consts = _snapshot_constants()
    stats: dict[str, dict[str, Any]] = {
        c.name: {
            "params": {
                "prob_bal": c.prob_bal,
                "turn_bal": c.turn_bal,
                "pressure_score_bal": c.pressure_score_bal,
                "pressure_max_ev_bal": c.pressure_max_ev_bal,
                "overheat_strong_prob": c.overheat_strong_prob,
                "overheat_strong_turn": c.overheat_strong_turn,
            },
            "months_total": len(month_dates),
            "months_nonzero": 0,
            "items_total": 0,
            "quarter_points_total": len(quarter_dates),
            "quarter_items_evaluated": 0,
            "quarter_wins": 0,
            "quarter_sum_pnl": 0.0,
            "quarter_top1_items": 0,
            "quarter_top1_wins": 0,
            "quarter_top1_sum_pnl": 0.0,
            "quarter_missing_outcome": 0,
        }
        for c in candidates
    }

    try:
        for idx, ymd in enumerate(month_dates, start=1):
            for c in candidates:
                _apply_candidate(c)
                resp = rc.get_rankings_asof(
                    "D",
                    "latest",
                    "down",
                    int(args.limit),
                    as_of=int(ymd),
                    mode="hybrid",
                    risk_mode="balanced",
                )
                items = resp.get("items") or []
                st = stats[c.name]
                n = len(items)
                st["items_total"] += n
                if n > 0:
                    st["months_nonzero"] += 1
                if ymd in quarter_set:
                    for i, item in enumerate(items):
                        code = str(item.get("code") or "")
                        pnl = q_outcomes.get((int(ymd), code))
                        if pnl is None or (not math.isfinite(float(pnl))):
                            st["quarter_missing_outcome"] += 1
                            continue
                        p = float(pnl)
                        st["quarter_items_evaluated"] += 1
                        st["quarter_sum_pnl"] += p
                        if p > 0:
                            st["quarter_wins"] += 1
                        if i == 0:
                            st["quarter_top1_items"] += 1
                            st["quarter_top1_sum_pnl"] += p
                            if p > 0:
                                st["quarter_top1_wins"] += 1
            if idx % 12 == 0 or idx == len(month_dates):
                print(f"[progress] {idx}/{len(month_dates)} months evaluated")
    finally:
        _restore_constants(base_consts)

    results: list[dict[str, Any]] = []
    for c in candidates:
        st = stats[c.name]
        months_total = max(1, int(st["months_total"]))
        nq = int(st["quarter_items_evaluated"])
        n1 = int(st["quarter_top1_items"])
        precision = (float(st["quarter_wins"]) / nq) if nq > 0 else None
        mean_pnl = (float(st["quarter_sum_pnl"]) / nq) if nq > 0 else None
        precision_top1 = (float(st["quarter_top1_wins"]) / n1) if n1 > 0 else None
        mean_pnl_top1 = (float(st["quarter_top1_sum_pnl"]) / n1) if n1 > 0 else None
        results.append(
            {
                "candidate": c.name,
                "params": st["params"],
                "months_total": int(st["months_total"]),
                "months_nonzero": int(st["months_nonzero"]),
                "months_nonzero_rate": float(st["months_nonzero"]) / months_total,
                "monthly_mean_n": float(st["items_total"]) / months_total,
                "quarter_points_total": int(st["quarter_points_total"]),
                "quarter_items_evaluated": nq,
                "quarter_missing_outcome": int(st["quarter_missing_outcome"]),
                "quarter_precision": precision,
                "quarter_mean_pnl_20d": mean_pnl,
                "quarter_top1_n": n1,
                "quarter_top1_precision": precision_top1,
                "quarter_top1_mean_pnl_20d": mean_pnl_top1,
            }
        )

    results.sort(
        key=lambda r: (
            -(r["months_nonzero_rate"] if isinstance(r["months_nonzero_rate"], (int, float)) else -1.0),
            -(r["quarter_precision"] if isinstance(r["quarter_precision"], (int, float)) else -1.0),
        )
    )

    payload = {
        "generated_at": "2026-02-27",
        "db_path": str(db_path),
        "window": {
            "start_ymd": int(args.start_ymd),
            "end_ymd": int(args.end_ymd),
            "limit": int(args.limit),
            "month_points": len(month_dates),
            "quarter_points": len(quarter_dates),
        },
        "results": results,
    }
    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] {out_path}")
    print(json.dumps(results[:3], ensure_ascii=False))


if __name__ == "__main__":
    main()
