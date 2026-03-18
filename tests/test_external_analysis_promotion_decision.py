from __future__ import annotations

import json
import sys

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db
from external_analysis.runtime.promotion_decision import (
    format_promotion_decision_text_report,
    run_promotion_decision_command,
)


def _seed_promotion_review(result_db: str, ops_db: str) -> None:
    ensure_result_db(result_db)
    ensure_ops_db(ops_db)
    publish_result(
        db_path=result_db,
        publish_id="pub_2026-03-12_20260312T237000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )
    conn = duckdb.connect(ops_db)
    try:
        conn.execute(
            """
            INSERT INTO external_state_eval_readiness VALUES (
                'pub_2026-03-12_20260312T237000Z_01:readiness',
                'pub_2026-03-12_20260312T237000Z_01',
                DATE '2026-03-12',
                'state_eval_baseline_v2',
                'state_eval_challenger_v2',
                72,
                0.028,
                TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
                '[]',
                '{"champion_selected":18,"challenger_selected":23}',
                TIMESTAMP '2026-03-12 23:59:00'
            )
            """
        )
    finally:
        conn.close()


def test_run_promotion_decision_command_persists_latest_decision(tmp_path, monkeypatch) -> None:
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    _seed_promotion_review(str(result_db), str(ops_db))

    payload = run_promotion_decision_command(
        result_db_path=str(result_db),
        ops_db_path=str(ops_db),
        decision="approved",
        note="codex approved",
        actor="codex_test",
    )

    assert payload["ok"] is True
    assert payload["decision"]["decision"] == "approved"
    assert payload["decision"]["note"] == "codex approved"
    assert payload["decision"]["actor"] == "codex_test"


def test_promotion_decision_cli_writes_report(tmp_path, monkeypatch) -> None:
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    report_path = tmp_path / "promotion_decision.json"
    _seed_promotion_review(str(result_db), str(ops_db))

    argv = [
        "external_analysis",
        "promotion-decision-run",
        "--result-db-path",
        str(result_db),
        "--ops-db-path",
        str(ops_db),
        "--decision",
        "hold",
        "--note",
        "watch one more day",
        "--actor",
        "codex_cli",
        "--report-path",
        str(report_path),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert external_analysis_main() == 0
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["decision"]["decision"] == "hold"
    assert saved["decision"]["note"] == "watch one more day"


def test_format_promotion_decision_text_report_contains_core_fields() -> None:
    text = format_promotion_decision_text_report(
        {
            "as_of_date": "20260312",
            "publish": {"publish_id": "pub_demo"},
            "decision": {"decision": "rejected", "actor": "codex_cli", "note": "unstable"},
            "review": {"readiness_pass": False, "expectancy_delta": -0.02, "sample_count": 40},
        }
    )

    assert "Tradex Promotion Decision" in text
    assert "publish_id: pub_demo" in text
    assert "decision: rejected" in text
