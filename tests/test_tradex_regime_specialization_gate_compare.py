from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_regime_specialization_gate_compare import run_gate_compare


SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")


def test_tradex_regime_specialization_gate_compare_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "regime_specialization_gate"
    result = run_gate_compare(
        source_db_path=SOURCE_DB_PATH,
        output_dir=output_dir,
        symbols=["1545", "1655", "9041", "6367", "9989"],
        start_date="2025-05-01",
        end_date="2026-04-23",
        freeze_date="2026-04-22",
        snapshot_as_of=20260422,
    )

    assert result["ok"] is True
    compare = result["compare"]
    decision = result["decision"]
    reason_rollup = result["reason_rollup"]
    paths = result["paths"]

    assert compare["same_condition_contract"]["same_period"] is True
    assert compare["same_condition_contract"]["same_source_db"].endswith("stocks.duckdb")
    assert compare["baseline_selector"]["selected_count"] == 5
    assert compare["specialized_selector"]["selected_count"] < compare["baseline_selector"]["selected_count"]
    assert decision["candidate_local_decision"] in {"hold", "keep"}
    assert reason_rollup["specialized_reason_rollup"]["no_trade"] >= 1

    compare_path = Path(paths["compare_json"])
    decision_path = Path(paths["decision_json"])
    reason_path = Path(paths["reason_rollup_json"])
    assert compare_path.exists()
    assert decision_path.exists()
    assert reason_path.exists()

    compare_payload = json.loads(compare_path.read_text(encoding="utf-8"))
    symbol_rows = {row["symbol"]: row for row in compare_payload["symbol_rows"]}
    assert symbol_rows["1545"]["specialized_gate"] == "long_tradable"
    assert symbol_rows["1655"]["specialized_gate"] == "long_tradable"
    assert symbol_rows["9041"]["specialized_gate"] == "long_tradable"
    assert symbol_rows["6367"]["specialized_gate"] == "no_trade"
    assert symbol_rows["9989"]["specialized_gate"] == "short_tradable"
