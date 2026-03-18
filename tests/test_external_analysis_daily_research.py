from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.runtime.daily_research import (
    build_daily_research_dispatch,
    build_daily_research_watchlist,
    build_daily_research_tag_report,
    format_daily_research_dispatch_text_report,
    format_daily_research_history_text_report,
    format_daily_research_tag_report_text_report,
    format_daily_research_text_report,
    format_daily_research_watchlist_text_report,
    load_daily_research_history,
    run_daily_research_cycle,
)
from tests.test_phase2_slice_f_nightly_pipeline import _run_phase1_inputs


def test_daily_research_cycle_runs_end_to_end_and_writes_reports(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    report_path = tmp_path / "daily_report.json"
    text_report_path = tmp_path / "daily_report.txt"
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
    )

    saved_json = json.loads(report_path.read_text(encoding="utf-8"))
    saved_text = text_report_path.read_text(encoding="utf-8")

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
