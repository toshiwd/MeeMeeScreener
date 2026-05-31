from __future__ import annotations

import duckdb

from scripts.tradex_market_scene_visual_proxy_probe_v1 import run_probe


def test_market_scene_visual_proxy_probe_writes_artifacts(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE daily_bars (code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        rows = []
        for code in ["1001", "1002"]:
            price = 1000.0
            for index in range(240):
                if index < 170:
                    price += 0.3
                elif index < 210:
                    price += (index % 4 - 1.5) * 0.6
                else:
                    price += 1.2
                ymd = 20260101 + index
                rows.append((code, ymd, price, price + 5.0, price - 5.0, price, 1000, "pan"))
        con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    result = run_probe(db_path=db_path, output_root=tmp_path / "out", start_dt=20260270, end_dt=20260320)

    assert result["scope"]["tradex_only"] is True
    assert result["scope"]["silent_fallback_used"] is False
    assert result["scope"]["research_fallback_used"] is False
    assert result["meemee_reflectable"] is False
    assert result["artifacts"]["compare_json"].endswith("market_scene_visual_proxy_probe_compare.json")
    assert "long_by_scene_action_side_visual" in result["compare"]
    assert "short_by_scene_action_side_visual" in result["compare"]
    assert result["authoritative_rollup_decision"] in {"hold", "drop"}
