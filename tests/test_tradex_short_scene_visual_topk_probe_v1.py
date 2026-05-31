from __future__ import annotations

import json

import duckdb

from scripts.tradex_short_scene_visual_topk_probe_v1 import _decision, run_probe


def _score(score: float, rank: int) -> tuple[str, str]:
    return (
        json.dumps({"tradePriorityScore": score}, ensure_ascii=False),
        json.dumps({"tradePriorityScore": score, "finalRank": rank}, ensure_ascii=False),
    )


def test_short_scene_visual_topk_probe_writes_artifacts(tmp_path) -> None:
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
        bars = []
        codes = [f"10{i:02d}" for i in range(1, 8)]
        for code in codes:
            price = 1200.0
            for index in range(190):
                price -= 1.0
                ymd = 20260101 + index
                bars.append((code, ymd, price, price + 6.0, price - 6.0, price, 1000, "pan"))
        con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", bars)
        rows = []
        for rank, code in enumerate(codes, start=1):
            score_json, rank_json = _score(1.0 - rank * 0.01, rank)
            rows.append((20260280, code, "sell", "logic:test", "basis:test", code, True, "sell", "{}", score_json, rank_json, "hash", 0.01, 0.02, 0.02, 0.02, 0.02, 0.0))
        con.executemany("INSERT INTO signal_decision_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

    result = run_probe(db_path=db_path, output_root=tmp_path / "out", start_dt=20260270, end_dt=20260290)

    assert result["scope"]["tradex_only"] is True
    assert result["scope"]["silent_fallback_used"] is False
    assert result["artifacts"]["compare_json"].endswith("short_scene_visual_topk_probe_compare.json")
    assert result["authoritative_rollup_decision"] in {"keep", "hold", "drop"}


def test_short_scene_visual_decision_drops_empty_filter() -> None:
    compare = {
        "top5": {
            "changed_rerank_member_count_total": 0,
            "changed_filter_member_count_total": 5,
            "rerank_delta": {"forward_return_20_mean": 0.0, "bad_loser_rate_20": 0.0},
            "filter_delta": {"forward_return_20_mean": 0.01, "bad_loser_rate_20": -0.1},
            "filter": {"count": 0},
        },
        "top10": {
            "rerank_delta": {"forward_return_20_mean": 0.0},
        },
    }

    assert _decision(compare) == {"judgment": "drop", "reason_type": "signal_absent_from_sell_candidate_pool"}
