from __future__ import annotations

import duckdb

from scripts.tradex_crash_bottoming_signal_probe_v1 import run_probe


def test_crash_bottoming_signal_probe_writes_artifacts(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE daily_bars (code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        rows = []
        for code in ["1001", "1002"]:
            price = 2000.0
            for index in range(220):
                if index < 170:
                    price -= 4.0
                else:
                    price += 3.0
                ymd = 20260101 + index
                rows.append((code, ymd, price, price + 8.0, price - 8.0, price, 1000, "pan"))
        con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    result = run_probe(db_path=db_path, output_root=tmp_path / "out", start_dt=20260270, end_dt=20260295)

    assert result["scope"]["silent_fallback_used"] is False
    assert result["artifacts"]["compare_json"].endswith("crash_bottoming_signal_probe_compare.json")
    assert result["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
