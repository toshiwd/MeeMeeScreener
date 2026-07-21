from __future__ import annotations

import json
from pathlib import Path

import duckdb

from app.backend.api.routers import tradex


def test_integrated_entry_board_api_reads_latest_artifact(tmp_path: Path, monkeypatch) -> None:
    run = tmp_path / "20260712T100000Z-board"
    run.mkdir()
    (run / "integrated_entry_board.json").write_text(
        json.dumps({
            "confirmed_as_of": "2026-07-10",
            "current_regime": "broad_up",
            "directional_bias": "buy_priority",
            "intraday_short_available": False,
            "actionable_count": 1,
            "watch_count": 1,
            "display_only": True,
            "current_regime_only": True,
            "actionable": [{"side": "buy", "code": "3479", "rule": "leaf9", "integrated_rank": 1, "rule_priority": 2, "reason": "上昇ルール合致", "severe_loss_probability": 0.08}],
            "watch": [{"side": "buy", "code": "2120", "rule": "clean_breakout", "avoid_reason": "上位3件外"}],
            "boundary": {"automatic_trading": False},
            "production_ranking_changed": False,
            "runtime_db_write": False,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(tradex, "INTEGRATED_ENTRY_BOARD_ROOT", tmp_path)
    main_root = tmp_path / "main"
    main_run = main_root / "20260713T120000Z-main"
    main_run.mkdir(parents=True)
    (main_run / "_ARTIFACT_COMPLETE.json").write_text("{}", encoding="utf-8")
    (main_run / "compare.json").write_text(json.dumps({
        "decision": {"candidate_local_decision": "keep", "authoritative_rollup_decision": "review_only"},
        "data_cutoffs": {"runtime_db_max_pan_date": 20260710},
        "selected_variant": {"sell_only_exposure_cap": 0.25, "sell_only_cash": 0.75},
        "metrics": {
            "validation": {"portfolio": {"calendar_profit_factor": 1.38, "calendar_expectancy": 0.0048}},
            "shadow": {"portfolio": {"calendar_profit_factor": 1.30, "calendar_expectancy": 0.0029}},
        },
    }), encoding="utf-8")
    with duckdb.connect() as conn:
        conn.execute("""
            COPY (SELECT 20260710::INTEGER signal_ymd, '3436'::VARCHAR code, 1::BIGINT rank,
                         1::BIGINT operational_rank, true top10, 'BUY'::VARCHAR side,
                         'meemee_priority'::VARCHAR rank_source, 1.0::DOUBLE meemee_source_rank,
                         1.0::DOUBLE full_buy_rank, 40.0::DOUBLE full_buy_score)
            TO ? (FORMAT PARQUET)
        """, [str(main_run / "all_buy_ranks.parquet")])
        conn.execute("""
            COPY (SELECT 20260710::INTEGER signal_ymd, '8416'::VARCHAR code, 1::BIGINT rank,
                         true top10, false family_hit, false setup_hit, false breadth_hit,
                         false readiness_hit)
            TO ? (FORMAT PARQUET)
        """, [str(main_run / "all_sell_ranks.parquet")])
    monkeypatch.setattr(tradex, "TWO_SIDED_MAIN_RULE_ROOT", main_root)

    payload = tradex.get_tradex_research_integrated_entry_board(limit=30)

    assert payload["available"] is True
    assert payload["confirmed_as_of"] == "2026-07-10"
    assert payload["actionable"][0]["code"] == "3479"
    assert payload["watch"][0]["rule"] == "clean_breakout"
    assert payload["actionable"][0]["reason"] == "上昇ルール合致"
    assert payload["actionable"][0]["rule_priority"] == 2
    assert payload["actionable"][0]["severe_loss_probability"] == 0.08
    assert payload["watch"][0]["avoid_reason"] == "上位3件外"
    assert payload["display_only"] is True
    assert payload["current_regime_only"] is True
    assert payload["production_ranking_changed"] is False
    assert payload["main_rule"]["available"] is True
    assert payload["main_rule"]["allocation"]["sell_only_sell"] == 0.25
    assert payload["main_rule"]["shadow_2026"]["portfolio"]["calendar_profit_factor"] == 1.30
    assert payload["main_rule"]["current_state"] == "buy_only"
    assert payload["main_rule"]["buy_ranks"][0] == {
        "signal_ymd": 20260710,
        "code": "3436",
        "rank": 1,
        "rank_source": "meemee_priority",
        "actionable": True,
    }
    assert payload["main_rule"]["sell_ranks"][0]["actionable"] is False


def test_integrated_entry_board_api_legacy_artifact_fails_closed_to_display_only(tmp_path: Path, monkeypatch) -> None:
    run = tmp_path / "20260712T100000Z-board"
    run.mkdir()
    (run / "integrated_entry_board.json").write_text(
        json.dumps({"actionable": [], "watch": [], "production_ranking_changed": False}),
        encoding="utf-8",
    )
    monkeypatch.setattr(tradex, "INTEGRATED_ENTRY_BOARD_ROOT", tmp_path)

    payload = tradex.get_tradex_research_integrated_entry_board(limit=30)

    assert payload["display_only"] is True
    assert payload["current_regime_only"] is True
    assert payload["production_ranking_changed"] is False


def test_integrated_entry_board_api_reports_missing_artifact(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(tradex, "INTEGRATED_ENTRY_BOARD_ROOT", tmp_path)

    payload = tradex.get_tradex_research_integrated_entry_board(limit=30)

    assert payload["available"] is False
    assert payload["actionable"] == []
