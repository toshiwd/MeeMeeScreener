from __future__ import annotations

import duckdb

from scripts.tradex_chart_shape_label_validation_v1 import run_validation


def test_chart_shape_label_validation_writes_summary_and_ledger(tmp_path) -> None:
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
        con.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1001", 20260508, 100, 102, 99, 101, 1000, "pan"),
                ("1001", 20260509, 101, 103, 100, 102, 1000, "pan"),
                ("1001", 20260510, 120, 125, 119, 124, 5000, "pan"),
                ("1001", 20260511, 125, 126, 116, 117, 4000, "pan"),
                ("1001", 20260512, 117, 119, 115, 116, 3000, "pan"),
                ("1001", 20260513, 116, 118, 114, 115, 2500, "pan"),
            ],
        )
        con.executemany(
            "INSERT INTO signal_decision_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    20260513,
                    "1001",
                    "buy",
                    "logic:test",
                    "basis:test",
                    "Test",
                    True,
                    "breakout",
                    "{}",
                    "{}",
                    "{}",
                    "hash",
                    0.01,
                    -0.05,
                    -0.04,
                    -0.02,
                    0.02,
                    -0.08,
                )
            ],
        )
    result = run_validation(
        db_path=db_path,
        output_root=tmp_path / "out",
        start_dt=20260501,
        end_dt=20260531,
        side="buy",
        window=5,
        entry_qualified_only=True,
        limit=None,
    )

    assert result["silent_fallback_used"] is False
    assert result["coverage"]["labeled_rows"] == 1
    assert "gap_up_stall_fade" in result["shape_summary"]
    assert result["shape_summary"]["gap_up_stall_fade"]["forward_returns"]["forward_return_5"]["count"] == 1
    assert result["shape_tendency"]["gap_up_stall_fade"]["tendency"] == "insufficient_sample"
    assert result["fixed_evaluation_conditions"]["primary_forward_metric"] == "forward_return_20"
    assert result["judgment"]["meemee_reflectable"] is False
    assert (tmp_path / "out").exists()
