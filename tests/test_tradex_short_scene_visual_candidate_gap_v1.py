from __future__ import annotations

import json

import duckdb

from scripts.tradex_short_scene_visual_candidate_gap_v1 import _decision, run_probe


def test_candidate_gap_decision_drops_empty_gap() -> None:
    decision = _decision({"count": 0}, {"count": 10, "mean_short_ret20": 0.01, "bad_loser_rate_20": 0.1}, 0, 0)

    assert decision == {"judgment": "drop", "reason_type": "no_candidate_gap_events"}


def test_short_scene_visual_candidate_gap_writes_artifacts(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE daily_bars (code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
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
        rows = []
        for code in ["1001", "1002"]:
            price = 1200.0
            for index in range(190):
                price -= 1.0
                ymd = 20260101 + index
                rows.append((code, ymd, price, price + 6.0, price - 6.0, price, 1000, "pan"))
        con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        score_json = json.dumps({"tradePriorityScore": 1.0})
        rank_json = json.dumps({"tradePriorityScore": 1.0, "finalRank": 1})
        con.execute(
            "INSERT INTO signal_decision_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (20260280, "1001", "sell", "logic:test", "basis:test", "1001", True, "sell", "{}", score_json, rank_json, "hash", 0.01, 0.02, 0.02, 0.02, 0.02, 0.0),
        )

    result = run_probe(db_path=db_path, output_root=tmp_path / "out", start_dt=20260270, end_dt=20260290)

    assert result["scope"]["tradex_only"] is True
    assert result["scope"]["silent_fallback_used"] is False
    assert result["artifacts"]["compare_json"].endswith("short_scene_visual_candidate_gap_compare.json")
    assert result["authoritative_rollup_decision"] in {"hold", "drop"}
