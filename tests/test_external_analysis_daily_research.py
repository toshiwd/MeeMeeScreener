from __future__ import annotations

import shutil
import json
import sys
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

import external_analysis.__main__ as external_analysis_main_module
from external_analysis.__main__ import main as external_analysis_main
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.ops.store import persist_review_artifact, upsert_job_run
from external_analysis.results.result_schema import ensure_result_db
from external_analysis.runtime import daily_research as daily_research_module
from external_analysis.runtime.daily_research import (
    build_daily_research_dispatch,
    build_daily_research_watchlist,
    build_daily_research_tag_report,
    format_daily_research_dispatch_text_report,
    format_daily_research_history_text_report,
    format_daily_research_loop_text_report,
    format_daily_research_tag_report_text_report,
    format_daily_research_text_report,
    format_daily_research_watchlist_text_report,
    load_daily_research_history,
    run_daily_research_loop,
    run_daily_research_cycle,
)
from external_analysis.runtime.daily_research_prepare import run_daily_research_prepare
from external_analysis.similarity.store import ensure_similarity_db
from tests.test_phase2_slice_f_nightly_pipeline import _run_phase1_inputs, _seed_source_db


ROOT = Path(__file__).resolve().parents[1]
_DAILY_RESEARCH_TMP_ROOT = (ROOT / ".tmp-tests" / "daily_research").resolve()
_DAILY_RESEARCH_TMP_ROOT.mkdir(parents=True, exist_ok=True)


class _WorkspaceTemporaryDirectory:
    def __init__(self, prefix: str = "tmp_", suffix: str = "", dir: str | None = None) -> None:
        base = Path(dir) if dir is not None else _DAILY_RESEARCH_TMP_ROOT
        base.mkdir(parents=True, exist_ok=True)
        self.name = str((base / f"{prefix}{uuid4().hex}{suffix}").resolve())
        Path(self.name).mkdir(parents=True, exist_ok=True)

    def __enter__(self) -> str:
        return self.name

    def __exit__(self, exc_type, exc, tb) -> None:
        shutil.rmtree(self.name, ignore_errors=True)

@pytest.fixture(autouse=True)
def _patch_workspace_tempdirs(monkeypatch) -> None:
    monkeypatch.setattr(
        daily_research_module.tempfile,
        "TemporaryDirectory",
        _WorkspaceTemporaryDirectory,
    )
    yield


def test_daily_research_cycle_runs_end_to_end_and_writes_reports(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    report_path = tmp_path / "daily_report.json"
    text_report_path = tmp_path / "daily_report.txt"
    progress_path = tmp_path / "daily_report.progress.json"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    payload = run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
        report_path=str(report_path),
        text_report_path=str(text_report_path),
        progress_path=str(progress_path),
    )

    saved_json = json.loads(report_path.read_text(encoding="utf-8"))
    saved_text = text_report_path.read_text(encoding="utf-8")
    saved_progress = json.loads(progress_path.read_text(encoding="utf-8"))

    assert payload["ok"] is True
    assert payload["candidate"]["status"] == "success"
    assert payload["similarity"]["status"] == "success"
    assert payload["challenger"]["status"] == "success"
    assert payload["report"]["publish"]["publish_id"] == "pub_2026-03-12_20260312T235930Z_01"
    assert len(payload["report"]["action_queue"]) >= 1
    assert payload["report"]["codex_next_step"]["kind"] == "promotion_decision_pending"
    assert "promotion-decision-run" in str(payload["report"]["codex_next_step"]["suggested_command"])
    assert payload["report"]["pending_carryover"] == []
    assert set(payload["report"]["codex_brief"].keys()) == {"pending", "improving", "risk"}
    assert saved_json["publish_id"] == "pub_2026-03-12_20260312T235930Z_01"
    assert saved_json["report"]["approval_decision"] is None
    assert "Tradex Daily Research" in saved_text
    assert "today_queue:" in saved_text
    assert "codex_command:" in saved_text
    assert saved_progress["status"] == "complete"
    assert saved_progress["current_phase"] == "completed"
    assert saved_progress["as_of_date"] == str(dates[45])
    history = load_daily_research_history(ops_db_path=str(ops_db), limit=5)
    assert history["rows"][0]["publish_id"] == "pub_2026-03-12_20260312T235930Z_01"


def test_daily_research_cli_runs_with_latest_as_of_default(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    report_path = tmp_path / "cli_daily_report.json"
    text_report_path = tmp_path / "cli_daily_report.txt"
    _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    argv = [
        "external_analysis",
        "daily-research-run",
        "--source-db-path",
        str(source_db),
        "--export-db-path",
        str(export_db),
        "--label-db-path",
        str(label_db),
        "--result-db-path",
        str(result_db),
        "--similarity-db-path",
        str(similarity_db),
        "--ops-db-path",
        str(ops_db),
        "--report-path",
        str(report_path),
        "--text-report-path",
        str(text_report_path),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert external_analysis_main() == 0
    assert report_path.exists()
    assert text_report_path.exists()


def test_daily_research_history_cli_reads_persisted_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    history_json = tmp_path / "history.json"
    history_txt = tmp_path / "history.txt"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    argv = [
        "external_analysis",
        "daily-research-history",
        "--ops-db-path",
        str(ops_db),
        "--limit",
        "5",
        "--report-path",
        str(history_json),
        "--text-report-path",
        str(history_txt),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert external_analysis_main() == 0
    payload = json.loads(Path(history_json).read_text(encoding="utf-8"))
    text = Path(history_txt).read_text(encoding="utf-8")
    assert payload["rows"][0]["publish_id"] == "pub_2026-03-12_20260312T235930Z_01"
    assert "Tradex Daily Research History" in text


def test_daily_research_watchlist_cli_aggregates_pending_and_risk(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    watchlist_json = tmp_path / "watchlist.json"
    watchlist_txt = tmp_path / "watchlist.txt"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            INSERT INTO external_review_artifacts (
                review_id, review_kind, latest_end_as_of_date, replay_scope_id, nightly_scope_id, combined_scope_id,
                combined_readiness_20, combined_readiness_40, combined_readiness_60, recent_run_limit,
                recent_failure_rate, recent_quarantine_count, top_reason_codes_json, replay_summary_json,
                nightly_summary_json, combined_summary_json, summary_json, created_at
            ) VALUES (
                'daily_research:pub_old:20260314T000000000000Z',
                'daily_research',
                DATE '2026-03-13',
                NULL,
                'pub_old',
                'pub_old',
                TRUE,
                TRUE,
                TRUE,
                7,
                0.0,
                0,
                '[]',
                '{}',
                '{}',
                '{}',
                '{"report":{"codex_next_step":{"kind":"pending_carryover"},"codex_brief":{"pending":[{"publish_id":"pub_old","tag":"box_breakout","command":"python -m external_analysis promotion-decision-run --decision hold --note \\"needs_manual_review\\""}],"improving":[{"metric":"top_strategy","current":"box_breakout"}],"risk":[{"metric":"risk_watch","current":"extension_fade"}]}}}',
                TIMESTAMP '2026-03-13 23:59:00'
            )
            """
        )
    finally:
        ops_conn.close()

    argv = [
        "external_analysis",
        "daily-research-watchlist",
        "--ops-db-path",
        str(ops_db),
        "--limit",
        "5",
        "--report-path",
        str(watchlist_json),
        "--text-report-path",
        str(watchlist_txt),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert external_analysis_main() == 0
    payload = json.loads(Path(watchlist_json).read_text(encoding="utf-8"))
    text = Path(watchlist_txt).read_text(encoding="utf-8")
    assert payload["pending_promotions"][0]["publish_id"] == "pub_old"
    assert payload["pending_promotions"][0]["priority_score"] >= 100
    assert payload["pending_promotions"][0]["priority_label"] in {"medium", "high", "critical"}
    assert payload["pending_promotions"][0]["next_action_kind"] == "approve"
    assert "promotion-decision-run" in str(payload["pending_promotions"][0]["suggested_command"])
    assert payload["improving_tags"][0]["tag"] == "box_breakout"
    assert payload["improving_tags"][0]["next_action_kind"] == "observe"
    assert "daily-research-tag-report" in str(payload["improving_tags"][0]["suggested_command"])
    assert payload["persistent_risk_tags"][0]["tag"] == "extension_fade"
    assert payload["persistent_risk_tags"][0]["priority_score"] >= 10
    assert payload["persistent_risk_tags"][0]["next_action_kind"] == "avoid"
    assert "daily-research-tag-report" in str(payload["persistent_risk_tags"][0]["suggested_command"])
    assert len(payload["top_next_actions"]) >= 1
    assert payload["top_next_actions"][0]["next_action_kind"] in {"approve", "observe", "avoid"}
    assert "Tradex Daily Research Watchlist" in text


def test_daily_research_dispatch_cli_selects_top_action(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    report_json = tmp_path / "dispatch.json"
    report_txt = tmp_path / "dispatch.txt"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    argv = [
        "external_analysis",
        "daily-research-dispatch",
        "--ops-db-path",
        str(ops_db),
        "--limit",
        "5",
        "--position",
        "1",
        "--report-path",
        str(report_json),
        "--text-report-path",
        str(report_txt),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert external_analysis_main() == 0
    payload = json.loads(Path(report_json).read_text(encoding="utf-8"))
    text = Path(report_txt).read_text(encoding="utf-8")
    assert payload["selected_position"] == 1
    assert payload["selected_action"] is not None
    assert "action_summary" in payload
    assert "Tradex Daily Research Dispatch" in text


def test_format_daily_research_text_report_includes_summary_fields() -> None:
    text = format_daily_research_text_report(
        {
            "as_of_date": "20260314",
            "candidate": {"status": "success"},
            "similarity": {"status": "success"},
            "challenger": {"status": "hold"},
            "report": {
                "publish": {"publish_id": "pub_demo"},
                "daily_summary": {
                    "top_strategy": {"strategy_tag": "box_breakout"},
                    "top_candle": {"strategy_tag": "bullish_engulfing"},
                    "risk_watch": {"strategy_tag": "bearish_engulfing"},
                    "sample_watch": {"strategy_tag": "three_bar_bull_reversal"},
                },
                "promotion_review": {"readiness_pass": True, "expectancy_delta": 0.12},
                "approval_decision": {"decision": "approved", "actor": "codex_cli"},
                "pending_carryover": [
                    {
                        "publish_id": "pub_old",
                        "as_of_date": "20260313",
                        "top_strategy_tag": "box_breakout",
                        "decision_status": "pending",
                    }
                ],
                "history_comparison": {
                    "previous_publish_id": "pub_old",
                    "changes": [
                        {
                            "metric": "top_strategy",
                            "previous": "pullback_rebound",
                            "current": "box_breakout",
                        }
                    ],
                },
                "codex_brief": {
                    "pending": [{"publish_id": "pub_old", "tag": "box_breakout"}],
                    "improving": [{"metric": "top_strategy", "current": "box_breakout"}],
                    "risk": [{"metric": "risk_watch", "current": "bearish_engulfing"}],
                },
                "codex_next_step": {
                    "title": "Promotion decision already recorded",
                    "status": "recorded",
                    "suggested_command": None,
                },
                "action_queue": [
                    {
                        "label": "Review",
                        "title": "Promote challenger review",
                        "strategy_tag": "box_breakout",
                        "metric_label": "Expectancy delta",
                        "metric_value": 0.12,
                    }
                ],
            },
        }
    )

    assert "Tradex Daily Research" in text
    assert "publish_id: pub_demo" in text
    assert "top_strategy: box_breakout" in text
    assert "approval_decision: approved" in text
    assert "codex_next_step: Promotion decision already recorded" in text
    assert "pending_carryover_count: 1" in text
    assert "history_compare_target: pub_old" in text
    assert "codex_brief_pending: 1" in text
    assert "codex_brief:" in text
    assert "history_changes:" in text
    assert "pending_carryover:" in text
    assert "[Review] Promote challenger review" in text


def test_format_daily_research_history_text_report_includes_rows() -> None:
    text = format_daily_research_history_text_report(
        {
            "rows": [
                {
                    "publish_id": "pub_demo",
                    "as_of_date": "20260314",
                    "codex_next_step": {"kind": "promotion_decision_pending"},
                    "codex_brief": {"pending": [1], "improving": [1, 2], "risk": []},
                }
            ]
        }
    )

    assert "Tradex Daily Research History" in text
    assert "pub_demo" in text
    assert "next=promotion_decision_pending" in text


def test_format_daily_research_watchlist_text_report_includes_rows() -> None:
    text = format_daily_research_watchlist_text_report(
        {
            "history_rows": 3,
            "pending_promotions": [{"publish_id": "pub_demo", "count": 2, "tag": "box_breakout", "next_action_kind": "approve", "priority_label": "high", "priority_score": 200, "suggested_command": "python -m external_analysis promotion-decision-run --decision hold"}],
            "improving_tags": [{"tag": "box_breakout", "count": 2, "source_metric": "top_strategy", "next_action_kind": "observe", "priority_label": "watch", "priority_score": 10, "suggested_command": "python -m external_analysis daily-research-tag-report --strategy-tag \"box_breakout\""}],
            "persistent_risk_tags": [{"tag": "extension_fade", "count": 2, "source_metric": "risk_watch", "next_action_kind": "avoid", "priority_label": "medium", "priority_score": 20, "suggested_command": "python -m external_analysis daily-research-tag-report --strategy-tag \"extension_fade\""}],
            "top_next_actions": [{"kind": "pending_promotion", "label": "pub_demo", "next_action_kind": "approve", "priority_score": 200, "suggested_command": "python -m external_analysis promotion-decision-run --decision hold"}],
        }
    )

    assert "Tradex Daily Research Watchlist" in text
    assert "pending_promotions: 1" in text
    assert "improving_tags: 1" in text
    assert "persistent_risk_tags: 1" in text
    assert "top_next_actions: 1" in text
    assert "pending_promotion | pub_demo" in text
    assert "pub_demo" in text
    assert "box_breakout" in text
    assert "extension_fade" in text
    assert "action=approve" in text
    assert "action=observe" in text
    assert "action=avoid" in text
    assert "priority=high:200" in text
    assert "daily-research-tag-report" in text


def test_format_daily_research_dispatch_text_report_includes_selected_action() -> None:
    text = format_daily_research_dispatch_text_report(
        {
            "selected_position": 1,
            "selected_action": {
                "kind": "pending_promotion",
                "label": "pub_demo",
                "next_action_kind": "approve",
                "priority_score": 200,
                "suggested_command": "python -m external_analysis promotion-decision-run --decision hold",
            },
            "action_summary": "Approve review for pub_demo after checking the latest promotion evidence.",
        }
    )

    assert "Tradex Daily Research Dispatch" in text
    assert "selected_kind: pending_promotion" in text
    assert "selected_action_kind: approve" in text
    assert "action_summary: Approve review for pub_demo after checking the latest promotion evidence." in text


def test_build_daily_research_dispatch_reads_watchlist(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    payload = build_daily_research_dispatch(ops_db_path=str(ops_db), limit=5, position=1)

    assert payload["selected_position"] == 1
    assert "watchlist" in payload
    assert payload["selected_action"] is not None
    assert isinstance(payload["action_summary"], str)


def test_daily_research_cycle_surfaces_pending_carryover_from_history(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_daily_summaries (
                summary_id,
                publish_id,
                as_of_date,
                side_scope,
                top_strategy_tag,
                top_strategy_expectancy,
                top_candle_tag,
                top_candle_expectancy,
                risk_watch_tag,
                risk_watch_loss_rate,
                sample_watch_tag,
                sample_watch_labeled_count,
                promotion_ready,
                promotion_sample_count,
                summary_json,
                created_at
            )
            VALUES (
                'pub_legacy_20260311:all',
                'pub_legacy_20260311',
                DATE '2026-03-11',
                'all',
                'pullback_rebound',
                0.051,
                'hammer_reversal',
                0.032,
                'extension_fade',
                0.27,
                'volume_surge',
                14,
                TRUE,
                58,
                '{"top_strategy":{"strategy_tag":"pullback_rebound"}}',
                TIMESTAMP '2026-03-11 23:00:00'
            )
            """
        )
    finally:
        ops_conn.close()

    payload = run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    assert payload["report"]["pending_carryover"][0]["publish_id"] == "pub_legacy_20260311"
    assert payload["report"]["history_comparison"]["previous_publish_id"] == "pub_legacy_20260311"
    assert any(change["metric"] == "top_strategy" for change in payload["report"]["history_comparison"]["changes"])
    assert payload["report"]["codex_next_step"]["kind"] == "pending_carryover"
    assert payload["report"]["codex_brief"]["pending"][0]["publish_id"] == "pub_legacy_20260311"
    assert "pub_legacy_20260311" in str(payload["report"]["codex_next_step"]["note"])


def test_build_daily_research_watchlist_reads_history(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    payload = build_daily_research_watchlist(ops_db_path=str(ops_db), limit=5)

    assert payload["history_rows"] >= 1
    assert "pending_promotions" in payload
    assert "improving_tags" in payload
    assert "persistent_risk_tags" in payload
    assert "top_next_actions" in payload
    if payload["pending_promotions"]:
        assert "priority_score" in payload["pending_promotions"][0]
        assert "next_action_kind" in payload["pending_promotions"][0]
    if payload["improving_tags"]:
        assert "suggested_command" in payload["improving_tags"][0]
        assert payload["improving_tags"][0]["next_action_kind"] == "observe"
    if payload["persistent_risk_tags"]:
        assert "suggested_command" in payload["persistent_risk_tags"][0]
        assert payload["persistent_risk_tags"][0]["next_action_kind"] == "avoid"


def test_daily_research_tag_report_cli_reads_specific_tag(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    report_json = tmp_path / "tag_report.json"
    report_txt = tmp_path / "tag_report.txt"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    argv = [
        "external_analysis",
        "daily-research-tag-report",
        "--ops-db-path",
        str(ops_db),
        "--strategy-tag",
        "extension_fade",
        "--report-path",
        str(report_json),
        "--text-report-path",
        str(report_txt),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert external_analysis_main() == 0
    payload = json.loads(Path(report_json).read_text(encoding="utf-8"))
    text = Path(report_txt).read_text(encoding="utf-8")
    assert payload["strategy_tag"] == "extension_fade"
    assert "Tradex Daily Research Tag Report" in text


def test_format_daily_research_tag_report_text_report_includes_rows() -> None:
    text = format_daily_research_tag_report_text_report(
        {
            "strategy_tag": "extension_fade",
            "rows": [
                {"publish_id": "pub_demo", "as_of_date": "20260314", "bucket": "risk"},
            ],
        }
    )

    assert "Tradex Daily Research Tag Report" in text
    assert "strategy_tag: extension_fade" in text
    assert "bucket=risk" in text


def test_build_daily_research_tag_report_reads_history(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235930Z_01",
    )

    payload = build_daily_research_tag_report(ops_db_path=str(ops_db), strategy_tag="extension_fade", limit=5)

    assert payload["strategy_tag"] == "extension_fade"
    assert isinstance(payload["rows"], list)


def test_daily_research_observers_read_through_snapshot_when_writer_is_open(tmp_path) -> None:
    ops_db = tmp_path / "ops.duckdb"
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    persist_review_artifact(
        review_row={
            "review_id": "daily_research:pub_demo:20260319T000000000000Z",
            "review_kind": "daily_research",
            "latest_end_as_of_date": "2026-03-19",
            "replay_scope_id": None,
            "nightly_scope_id": "pub_demo",
            "combined_scope_id": "pub_demo",
            "combined_readiness_20": True,
            "combined_readiness_40": True,
            "combined_readiness_60": True,
            "recent_run_limit": 7,
            "recent_failure_rate": 0.0,
            "recent_quarantine_count": 0,
            "top_reason_codes_json": "[]",
            "replay_summary_json": "{}",
            "nightly_summary_json": "{}",
            "combined_summary_json": "{}",
            "summary_json": json.dumps(
                {
                    "report": {
                        "codex_next_step": {"kind": "promotion_decision_pending"},
                        "codex_brief": {
                            "pending": [
                                {
                                    "publish_id": "pub_demo",
                                    "tag": "box_breakout",
                                    "command": 'python -m external_analysis promotion-decision-run --decision hold --note "needs_manual_review"',
                                }
                            ],
                            "improving": [{"metric": "top_strategy", "current": "box_breakout"}],
                            "risk": [{"metric": "risk_watch", "current": "extension_fade"}],
                        },
                    }
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "created_at": created_at,
        },
        ops_db_path=str(ops_db),
    )
    upsert_job_run(
        job_id="nightly_candidate_20260319",
        job_type="nightly_candidate_pipeline",
        status="running",
        as_of_date="20260319",
        publish_id="pub_demo",
        attempt=1,
        started_at=created_at - timedelta(hours=2),
        finished_at=None,
        ops_db_path=str(ops_db),
    )

    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        history = load_daily_research_history(ops_db_path=str(ops_db), limit=5)
        watchlist = build_daily_research_watchlist(ops_db_path=str(ops_db), limit=5)
        dispatch = build_daily_research_dispatch(ops_db_path=str(ops_db), limit=5, position=1)
    finally:
        ops_conn.close()

    assert history["rows"][0]["publish_id"] == "pub_demo"
    assert history["stale_running_jobs"][0]["status"] == "stale_running"
    assert watchlist["pending_promotions"][0]["publish_id"] == "pub_demo"
    assert watchlist["stale_running_jobs"][0]["status"] == "stale_running"
    assert dispatch["selected_position"] == 1
    assert dispatch["selected_action"]["label"] == "pub_demo"


def test_daily_research_loop_stops_on_first_promotion_ready(monkeypatch, tmp_path) -> None:
    snapshot_db = tmp_path / "snapshot.duckdb"
    snapshot_db.write_text("snapshot", encoding="utf-8")
    progress_path = tmp_path / "loop.progress.json"
    attempted: list[str] = []

    monkeypatch.setattr(
        daily_research_module,
        "probe_daily_research_prepared_environment",
        lambda **_kwargs: {"prepared": True, "reason_code": "prepared_complete", "latest_trade_date": "20260319"},
    )
    monkeypatch.setattr(
        daily_research_module,
        "resolve_recent_daily_research_as_of_dates_from_export",
        lambda **_kwargs: ["20260319", "20260318", "20260317"],
    )

    def _fake_cycle(**kwargs):
        as_of_date = str(kwargs["as_of_date"])
        attempted.append(as_of_date)
        readiness = as_of_date == "20260318"
        return {
            "ok": True,
            "as_of_date": as_of_date,
            "publish_id": f"pub_{as_of_date}",
            "candidate": {"status": "success"},
            "similarity": {"status": "success"},
            "challenger": {"status": "success"},
            "report": {
                "promotion_review": {"readiness_pass": readiness},
                "daily_summary": {
                    "top_strategy": {"strategy_tag": "box_breakout"},
                    "risk_watch": {"strategy_tag": "extension_fade"},
                },
                "codex_next_step": {"kind": "promotion_decision_pending"},
            },
        }

    monkeypatch.setattr(daily_research_module, "run_daily_research_cycle", _fake_cycle)
    monkeypatch.setattr(
        daily_research_module,
        "_load_daily_research_long_candidates",
        lambda **kwargs: (
            []
            if str(kwargs.get("as_of_date")) == "20260319"
            else [
                {
                    "as_of_date": str(kwargs.get("as_of_date")),
                    "publish_id": str(kwargs.get("publish_id")),
                    "code": "1301",
                    "side": "long",
                    "rank_position": 1,
                    "candidate_score": 1.25,
                    "expected_horizon_days": 20,
                    "primary_reason_codes": ["BUY_TREND"],
                    "regime_tag": "risk_on",
                    "freshness_state": "fresh",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        daily_research_module,
        "_load_stale_running_job_observations",
        lambda **_kwargs: [
            {
                "job_id": "job_stale",
                "job_type": "nightly_candidate_pipeline",
                "status": "stale_running",
                "as_of_date": "2026-03-17",
            }
        ],
    )

    payload = run_daily_research_loop(
        source_db_path=str(tmp_path / "source.duckdb"),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        progress_path=str(progress_path),
        max_trading_days=5,
    )
    saved_progress = json.loads(progress_path.read_text(encoding="utf-8"))

    assert payload["ok"] is True
    assert attempted == ["20260319", "20260318"]
    assert payload["source_latest_as_of"] == "20260319"
    assert payload["attempted_as_of_dates"] == ["20260319", "20260318"]
    assert payload["selected_as_of_date"] == "20260318"
    assert payload["selected_publish_id"] == "pub_20260318"
    assert payload["stop_reason"] == "promotion_ready_with_long_candidates"
    assert payload["promotion_ready"] is True
    assert payload["long_candidates"][0]["side"] == "long"
    assert payload["diagnostics"]["stale_running_jobs"][0]["status"] == "stale_running"
    assert len(payload["diagnostics"]["attempts"]) == 2
    assert "Tradex Daily Research Loop" in format_daily_research_loop_text_report(payload)
    assert saved_progress["status"] == "complete"
    assert saved_progress["current_phase"] == "completed"
    assert saved_progress["selected_as_of_date"] == "20260318"


def test_daily_research_loop_returns_no_result_after_window(monkeypatch, tmp_path) -> None:
    snapshot_db = tmp_path / "snapshot.duckdb"
    snapshot_db.write_text("snapshot", encoding="utf-8")

    monkeypatch.setattr(
        daily_research_module,
        "probe_daily_research_prepared_environment",
        lambda **_kwargs: {"prepared": True, "reason_code": "prepared_complete", "latest_trade_date": "20260319"},
    )
    monkeypatch.setattr(
        daily_research_module,
        "resolve_recent_daily_research_as_of_dates_from_export",
        lambda **_kwargs: ["20260319", "20260318", "20260317"],
    )
    monkeypatch.setattr(
        daily_research_module,
        "run_daily_research_cycle",
        lambda **kwargs: {
            "ok": True,
            "as_of_date": str(kwargs["as_of_date"]),
            "publish_id": f"pub_{kwargs['as_of_date']}",
            "candidate": {"status": "success"},
            "similarity": {"status": "success"},
            "challenger": {"status": "success"},
            "report": {
                "promotion_review": {"readiness_pass": False},
                "daily_summary": {},
                "codex_next_step": {"kind": "idle"},
            },
        },
    )
    monkeypatch.setattr(daily_research_module, "_load_daily_research_long_candidates", lambda **_kwargs: [])
    monkeypatch.setattr(daily_research_module, "_load_stale_running_job_observations", lambda **_kwargs: [])

    payload = run_daily_research_loop(
        source_db_path=str(tmp_path / "source.duckdb"),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        max_trading_days=3,
    )

    assert payload["ok"] is False
    assert payload["attempted_as_of_dates"] == ["20260319", "20260318", "20260317"]
    assert payload["selected_as_of_date"] is None
    assert payload["selected_publish_id"] is None
    assert payload["stop_reason"] == "no_promotion_ready_in_window"
    assert payload["promotion_ready"] is False
    assert payload["long_candidates"] == []
    assert len(payload["diagnostics"]["attempts"]) == 3


def test_daily_research_loop_cli_routes_to_loop_runner(monkeypatch, tmp_path) -> None:
    report_path = tmp_path / "loop_report.json"
    text_report_path = tmp_path / "loop_report.txt"
    progress_path = tmp_path / "loop_report.progress.json"
    captured: dict[str, object] = {}

    def _fake_loop(**kwargs):
        captured.update(kwargs)
        report_path.write_text('{"ok": true}', encoding="utf-8")
        text_report_path.write_text("loop ok", encoding="utf-8")
        progress_path.write_text('{"status":"complete"}', encoding="utf-8")
        return {"ok": True, "attempted_as_of_dates": ["20260319"], "long_candidates": []}

    monkeypatch.setattr(external_analysis_main_module, "run_daily_research_loop", _fake_loop)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "external_analysis",
            "daily-research-loop",
            "--source-db-path",
            str(tmp_path / "source.duckdb"),
            "--export-db-path",
            str(tmp_path / "export.duckdb"),
            "--label-db-path",
            str(tmp_path / "label.duckdb"),
            "--result-db-path",
            str(tmp_path / "result.duckdb"),
            "--similarity-db-path",
            str(tmp_path / "similarity.duckdb"),
            "--ops-db-path",
            str(tmp_path / "ops.duckdb"),
            "--report-path",
            str(report_path),
            "--text-report-path",
            str(text_report_path),
            "--progress-path",
            str(progress_path),
            "--max-trading-days",
            "4",
        ],
    )

    assert external_analysis_main() == 0
    assert captured["max_trading_days"] == 4
    assert captured["progress_path"] == str(progress_path)
    assert report_path.exists()
    assert text_report_path.exists()
    assert progress_path.exists()


def test_daily_research_prepare_builds_manifest_and_reuses_same_signature(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    _seed_source_db(str(source_db))

    first_payload = run_daily_research_prepare(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
    )
    second_payload = run_daily_research_prepare(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
    )

    manifest_path = Path(first_payload["manifest_path"])
    progress_path = Path(first_payload["progress_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    conn = duckdb.connect(str(export_db), read_only=True)
    try:
        export_run_count = int(conn.execute("SELECT COUNT(*) FROM meta_export_runs").fetchone()[0])
        max_trade_date = int(conn.execute("SELECT MAX(trade_date) FROM bars_daily_export").fetchone()[0])
        indicator_count = int(conn.execute("SELECT COUNT(*) FROM indicator_daily_export").fetchone()[0])
    finally:
        conn.close()

    assert first_payload["ok"] is True
    assert first_payload["prepared"] is True
    assert first_payload["export_status"] == "complete"
    assert first_payload["label_status"] == "skip"
    assert second_payload["prepared"] is True
    assert second_payload["export_status"] == "complete"
    assert second_payload["label_status"] == "skip"
    assert manifest["prepared"] is True
    assert manifest["source_signature"] == first_payload["source_signature"]
    assert progress["status"] == "complete"
    assert progress["current_phase"] == "completed"
    assert export_run_count == 1
    assert max_trade_date == int(first_payload["latest_trade_date"])
    assert indicator_count > 0


def test_daily_research_loop_returns_prepare_required_without_prepared_env(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "loop.progress.json"

    monkeypatch.setattr(
        daily_research_module,
        "probe_daily_research_prepared_environment",
        lambda **_kwargs: {
            "prepared": False,
            "reason_code": "label_prepare_required",
            "latest_trade_date": "20260319",
        },
    )
    monkeypatch.setattr(daily_research_module, "_load_stale_running_job_observations", lambda **_kwargs: [])
    monkeypatch.setattr(
        daily_research_module,
        "run_daily_research_cycle",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("cycle should not run")),
    )

    payload = run_daily_research_loop(
        source_db_path=str(tmp_path / "source.duckdb"),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        progress_path=str(progress_path),
    )
    saved_progress = json.loads(progress_path.read_text(encoding="utf-8"))

    assert payload["ok"] is False
    assert payload["stop_reason"] == "prepare_required"
    assert payload["attempted_as_of_dates"] == []
    assert payload["diagnostics"]["prepared_environment"]["reason_code"] == "label_prepare_required"
    assert saved_progress["current_phase"] == "prepare_required"
    assert saved_progress["stop_reason"] == "prepare_required"


def test_daily_research_loop_uses_export_dates_and_prepared_cycle(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "loop.progress.json"
    attempted: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        daily_research_module,
        "probe_daily_research_prepared_environment",
        lambda **_kwargs: {"prepared": True, "reason_code": "prepared_complete", "latest_trade_date": "20260319"},
    )
    monkeypatch.setattr(
        daily_research_module,
        "resolve_recent_daily_research_as_of_dates_from_export",
        lambda **_kwargs: ["20260319", "20260318"],
    )
    monkeypatch.setattr(
        daily_research_module,
        "resolve_recent_daily_research_as_of_dates",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("source dates should not be used")),
    )

    def _fake_cycle(**kwargs):
        attempted.append((str(kwargs["as_of_date"]), bool(kwargs["require_prepared_environment"])))
        return {
            "ok": True,
            "as_of_date": str(kwargs["as_of_date"]),
            "publish_id": f"pub_{kwargs['as_of_date']}",
            "candidate": {"status": "success"},
            "similarity": {"status": "success"},
            "challenger": {"status": "success"},
            "report": {
                "promotion_review": {"readiness_pass": str(kwargs["as_of_date"]) == "20260318"},
                "daily_summary": {},
                "codex_next_step": {"kind": "promotion_decision_pending"},
            },
        }

    monkeypatch.setattr(daily_research_module, "run_daily_research_cycle", _fake_cycle)
    monkeypatch.setattr(
        daily_research_module,
        "_load_daily_research_long_candidates",
        lambda **kwargs: [] if str(kwargs.get("as_of_date")) == "20260319" else [{"side": "long", "rank_position": 1, "code": "1301"}],
    )
    monkeypatch.setattr(daily_research_module, "_load_stale_running_job_observations", lambda **_kwargs: [])

    payload = run_daily_research_loop(
        source_db_path=str(tmp_path / "source.duckdb"),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        progress_path=str(progress_path),
    )

    assert payload["selected_as_of_date"] == "20260318"
    assert attempted == [("20260319", True), ("20260318", True)]


def test_daily_research_cycle_prepared_mode_skips_export_and_labels(monkeypatch, tmp_path) -> None:
    captured: dict[str, Any] = {}

    def _fake_candidate_pipeline(**kwargs):
        captured["candidate_kwargs"] = kwargs
        return {
            "ok": True,
            "run_id": "candidate_run",
            "status": "success",
            "quarantine_reason": None,
            "baseline": {"publish_id": "pub_prepared"},
        }

    monkeypatch.setattr(
        daily_research_module,
        "run_nightly_candidate_pipeline",
        _fake_candidate_pipeline,
    )
    monkeypatch.setattr(
        daily_research_module,
        "run_nightly_similarity_pipeline",
        lambda **_kwargs: {"ok": True, "run_id": "sim_run", "status": "success", "quarantine_reason": None},
    )
    monkeypatch.setattr(
        daily_research_module,
        "run_nightly_similarity_challenger_pipeline",
        lambda **_kwargs: {"ok": True, "run_id": "challenger_run", "status": "success", "quarantine_reason": None},
    )
    monkeypatch.setattr(
        daily_research_module,
        "build_daily_research_report",
        lambda **_kwargs: {"promotion_review": {"readiness_pass": True}, "publish": {"publish_id": "pub_prepared"}},
    )
    monkeypatch.setattr(daily_research_module, "persist_review_artifact", lambda **_kwargs: None)

    payload = run_daily_research_cycle(
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        as_of_date="20260319",
        require_prepared_environment=True,
        snapshot_source=False,
    )

    assert payload["publish_id"] == "pub_prepared"
    assert captured["candidate_kwargs"]["require_prepared_environment"] is True
    assert captured["candidate_kwargs"]["snapshot_source"] is False


def test_daily_research_prepare_path_matches_direct_path_outputs(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db_direct = tmp_path / "export_direct.duckdb"
    export_db_prepared = tmp_path / "export_prepared.duckdb"
    label_db_direct = tmp_path / "label_direct.duckdb"
    label_db_prepared = tmp_path / "label_prepared.duckdb"
    result_db_direct = tmp_path / "result_direct.duckdb"
    result_db_prepared = tmp_path / "result_prepared.duckdb"
    ops_db_direct = tmp_path / "ops_direct.duckdb"
    ops_db_prepared = tmp_path / "ops_prepared.duckdb"
    similarity_db_direct = tmp_path / "similarity_direct.duckdb"
    similarity_db_prepared = tmp_path / "similarity_prepared.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db_direct), str(label_db_direct), str(result_db_direct), str(ops_db_direct))
    ensure_result_db(str(result_db_prepared))
    ensure_ops_db(str(ops_db_prepared))
    ensure_similarity_db(str(similarity_db_direct))
    ensure_similarity_db(str(similarity_db_prepared))

    direct_payload = run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db_direct),
        label_db_path=str(label_db_direct),
        result_db_path=str(result_db_direct),
        similarity_db_path=str(similarity_db_direct),
        ops_db_path=str(ops_db_direct),
        as_of_date=str(dates[45]),
        publish_id="pub_direct",
    )

    run_daily_research_prepare(
        source_db_path=str(source_db),
        export_db_path=str(export_db_prepared),
        label_db_path=str(label_db_prepared),
    )
    prepared_payload = run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(export_db_prepared),
        label_db_path=str(label_db_prepared),
        result_db_path=str(result_db_prepared),
        similarity_db_path=str(similarity_db_prepared),
        ops_db_path=str(ops_db_prepared),
        as_of_date=str(dates[45]),
        publish_id="pub_prepared",
        require_prepared_environment=True,
        snapshot_source=False,
    )

    direct_conn = duckdb.connect(str(result_db_direct), read_only=True)
    prepared_conn = duckdb.connect(str(result_db_prepared), read_only=True)
    try:
        direct_candidates = direct_conn.execute(
            """
            SELECT code, side, rank_position, candidate_score, regime_tag
            FROM candidate_daily
            WHERE publish_id = 'pub_direct'
            ORDER BY side, rank_position, code
            """
        ).fetchall()
        prepared_candidates = prepared_conn.execute(
            """
            SELECT code, side, rank_position, candidate_score, regime_tag
            FROM candidate_daily
            WHERE publish_id = 'pub_prepared'
            ORDER BY side, rank_position, code
            """
        ).fetchall()
        direct_state_eval = direct_conn.execute(
            """
            SELECT code, side, holding_band, decision_3way
            FROM state_eval_daily
            WHERE publish_id = 'pub_direct'
            ORDER BY side, code
            """
        ).fetchall()
        prepared_state_eval = prepared_conn.execute(
            """
            SELECT code, side, holding_band, decision_3way
            FROM state_eval_daily
            WHERE publish_id = 'pub_prepared'
            ORDER BY side, code
            """
        ).fetchall()
    finally:
        direct_conn.close()
        prepared_conn.close()

    assert direct_candidates == prepared_candidates
    assert direct_state_eval == prepared_state_eval
    assert direct_payload["report"]["promotion_review"]["readiness_pass"] == prepared_payload["report"]["promotion_review"]["readiness_pass"]


def test_daily_research_prepare_cli_routes_to_prepare_runner(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "prepare.progress.json"
    manifest_path = tmp_path / "prepare.json"
    captured: dict[str, object] = {}

    def _fake_prepare(**kwargs):
        captured.update(kwargs)
        progress_path.write_text('{"status":"complete"}', encoding="utf-8")
        manifest_path.write_text('{"prepared":true}', encoding="utf-8")
        return {"ok": True, "prepared": True}

    monkeypatch.setattr(external_analysis_main_module, "run_daily_research_prepare", _fake_prepare)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "external_analysis",
            "daily-research-prepare",
            "--source-db-path",
            str(tmp_path / "source.duckdb"),
            "--export-db-path",
            str(tmp_path / "export.duckdb"),
            "--label-db-path",
            str(tmp_path / "label.duckdb"),
            "--manifest-path",
            str(manifest_path),
            "--progress-path",
            str(progress_path),
        ],
    )

    assert external_analysis_main() == 0
    assert captured["manifest_path"] == str(manifest_path)
    assert captured["progress_path"] == str(progress_path)
