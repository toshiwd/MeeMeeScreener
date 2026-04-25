from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_chart_first_replay import run_chart_first_replay


SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")


def test_tradex_chart_first_5541_replay_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "tradex_chart_first_5541"
    result = run_chart_first_replay(
        source_db_path=SOURCE_DB_PATH,
        output_dir=output_dir,
        symbol="5541",
        start_date="2025-10-10",
        end_date="2026-01-30",
        freeze_date="2025-10-09",
    )

    assert result["config"]["policy_mode"] == "chart-first"
    assert result["aggregate"]["roundtrip_count"] >= 1
    assert result["aggregate"]["entry_count"] >= 1
    assert result["aggregate"]["hedge_count"] >= 1
    assert result["aggregate"]["final_position"] == "0-0"
    assert result["ledger_rows"] >= 1

    paths = result["paths"]
    for key in (
        "run_config",
        "daily_ledger_json",
        "daily_ledger_parquet",
        "roundtrip_summary",
        "postmortem",
        "entry_reason_report",
    ):
        assert Path(paths[key]).exists(), key

    ledger_payload = json.loads(Path(paths["daily_ledger_json"]).read_text(encoding="utf-8"))
    assert isinstance(ledger_payload["rows"], list)
    assert any(row.get("selected_action") != "stay" for row in ledger_payload["rows"])
    assert any(row.get("entry_reason_primary") for row in ledger_payload["rows"])
    assert any(row.get("hedge_reason_primary") for row in ledger_payload["rows"])
    assert any(row.get("exit_reason_primary") for row in ledger_payload["rows"])

    summary = json.loads(Path(paths["roundtrip_summary"]).read_text(encoding="utf-8"))
    assert summary["symbol"] == "5541"
    assert summary["roundtrips"]
    assert summary["aggregate"]["entry_reason_primary"]
    assert summary["aggregate"]["exit_reason_primary"]

    postmortem = json.loads(Path(paths["postmortem"]).read_text(encoding="utf-8"))
    assert postmortem["reason_reviews"]
    assert postmortem["next_improvement_axis"]["axis"] == "hedge_ratio_calibration"
