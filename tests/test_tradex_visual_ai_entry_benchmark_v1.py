from __future__ import annotations

import json

import duckdb

from scripts.tradex_visual_ai_entry_benchmark_v1 import run_benchmark


def _score(score: float, rank: int) -> tuple[str, str]:
    return (
        json.dumps({"tradePriorityScore": score, "entryScore": score}, ensure_ascii=False),
        json.dumps({"tradePriorityScore": score, "finalRank": rank, "asOf": "2026-04-01"}, ensure_ascii=False),
    )


def test_visual_ai_entry_benchmark_writes_compare_and_branches(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE signal_decision_daily (
                dt INTEGER, code VARCHAR, side VARCHAR, logic_version VARCHAR, basis_version VARCHAR,
                name VARCHAR, entry_qualified BOOLEAN, setup_type VARCHAR, reason_snapshot_json VARCHAR,
                score_snapshot_json VARCHAR, rank_snapshot_json VARCHAR, decision_hash VARCHAR,
                forward_return_5 DOUBLE, forward_return_20 DOUBLE, forward_return_30 DOUBLE,
                forward_return_60 DOUBLE, max_favorable_30 DOUBLE, max_adverse_30 DOUBLE
            )
            """
        )
        bars = []
        for idx in range(60):
            ymd = 20260201 + idx
            # A: high-zone chase profile.
            bars.append(("1001", ymd, 100 + idx * 2, 104 + idx * 2, 99 + idx * 2, 103 + idx * 2, 1000, "pan"))
            # B: steady high hold.
            bars.append(("1002", ymd, 100 + idx, 103 + idx, 99 + idx, 102 + idx, 1000, "pan"))
            # C: neutral.
            bars.append(("1003", ymd, 100, 102, 98, 100, 1000, "pan"))
        con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", bars)
        rows = []
        for code, name, score, rank, ret20 in [
            ("1001", "Chase", 1.00, 1, -0.12),
            ("1002", "Hold", 0.98, 2, 0.08),
            ("1003", "Neutral", 0.90, 3, 0.01),
        ]:
            score_json, rank_json = _score(score, rank)
            rows.append(
                (
                    20260260,
                    code,
                    "buy",
                    "logic:test",
                    "basis:test",
                    name,
                    True,
                    "breakout",
                    "{}",
                    score_json,
                    rank_json,
                    "hash",
                    ret20 / 4,
                    ret20,
                    ret20,
                    ret20,
                    max(ret20, 0.0),
                    min(ret20, 0.0),
                )
            )
        con.executemany("INSERT INTO signal_decision_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

    result = run_benchmark(
        db_path=db_path,
        output_root=tmp_path / "out",
        start_dt=20260201,
        end_dt=20260260,
        side="buy",
    )

    assert result["scope"]["silent_fallback_used"] is False
    assert result["coverage"]["input_rows"] == 3
    assert result["compare"]["top5"]["evaluated_dates"] == 0
    assert result["artifacts"]["compare_json"].endswith("visual_ai_entry_benchmark_compare.json")
    assert result["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
