from __future__ import annotations

import duckdb
from fastapi.testclient import TestClient

from app.core.config import config as core_config
from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self) -> None:
        return None


def test_analysis_bridge_status_returns_degraded_not_500_when_result_db_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(tmp_path / "missing_result.duckdb"))

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "result_db_missing"


def test_analysis_bridge_candidates_and_regime_return_degraded_not_500_when_result_db_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(tmp_path / "missing_result.duckdb"))

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())

    candidates = client.get("/api/analysis-bridge/candidates")
    regime = client.get("/api/analysis-bridge/regime")

    assert candidates.status_code == 200
    assert candidates.json()["degraded"] is True
    assert candidates.json()["degrade_reason"] == "result_db_missing"
    assert candidates.json()["rows"] == []
    assert candidates.json()["publish_id"] is None
    assert candidates.json()["as_of_date"] is None
    assert candidates.json()["freshness_state"] is None
    assert regime.status_code == 200
    assert regime.json()["degraded"] is True
    assert regime.json()["degrade_reason"] == "result_db_missing"
    assert regime.json()["rows"] == []
    assert regime.json()["publish_id"] is None
    assert regime.json()["as_of_date"] is None
    assert regime.json()["freshness_state"] is None


def test_analysis_bridge_candidates_and_regime_read_published_rows(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO candidate_daily (
                publish_id, as_of_date, code, side, rank_position, candidate_score, expected_horizon_days,
                primary_reason_codes, regime_tag, freshness_state
            ) VALUES
                ('pub_2026-03-12_20260312T230000Z_01', DATE '2026-03-12', '1302', 'long', 2, 10.5, 20, '["LONG_BASELINE"]', 'risk_on', 'fresh'),
                ('pub_2026-03-12_20260312T230000Z_01', DATE '2026-03-12', '1301', 'long', 1, 12.5, 20, '["LONG_BASELINE"]', 'risk_on', 'fresh'),
                ('pub_2026-03-12_20260312T230000Z_01', DATE '2026-03-12', '1303', 'short', 1, 11.0, 20, '["SHORT_BASELINE"]', 'risk_on', 'fresh')
            """
        )
        conn.execute(
            """
            INSERT INTO regime_daily (
                publish_id, as_of_date, regime_tag, regime_score, breadth_score, volatility_state
            ) VALUES
                ('pub_2026-03-12_20260312T230000Z_01', DATE '2026-03-12', 'risk_on', 0.8, 0.6, 'normal')
            """
        )
    finally:
        conn.close()
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T230000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 3,
            "regime_daily": 1,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    candidates = client.get("/api/analysis-bridge/candidates")
    regime = client.get("/api/analysis-bridge/regime")

    assert candidates.status_code == 200
    candidate_payload = candidates.json()
    assert candidate_payload["degraded"] is False
    assert candidate_payload["publish_id"] == "pub_2026-03-12_20260312T230000Z_01"
    assert candidate_payload["as_of_date"] == "2026-03-12"
    assert candidate_payload["freshness_state"] == "fresh"
    assert len(candidate_payload["rows"]) == 3
    assert {row["side"] for row in candidate_payload["rows"]} == {"long", "short"}
    assert [(row["side"], row["rank_position"], row["code"]) for row in candidate_payload["rows"]] == [
        ("long", 1, "1301"),
        ("long", 2, "1302"),
        ("short", 1, "1303"),
    ]
    assert all("ranking_score" not in row for row in candidate_payload["rows"])

    assert regime.status_code == 200
    regime_payload = regime.json()
    assert regime_payload["degraded"] is False
    assert regime_payload["publish_id"] == "pub_2026-03-12_20260312T230000Z_01"
    assert regime_payload["as_of_date"] == "2026-03-12"
    assert regime_payload["freshness_state"] == "fresh"
    assert len(regime_payload["rows"]) == 1
    assert regime_payload["rows"][0]["regime_tag"] == "risk_on"


def test_analysis_bridge_regime_degrades_when_publish_has_multiple_rows(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO regime_daily (
                publish_id, as_of_date, regime_tag, regime_score, breadth_score, volatility_state
            ) VALUES
                ('pub_2026-03-12_20260312T231000Z_01', DATE '2026-03-12', 'risk_on', 0.8, 0.6, 'normal'),
                ('pub_2026-03-12_20260312T231000Z_01', DATE '2026-03-12', 'neutral', 0.1, 0.0, 'normal')
            """
        )
    finally:
        conn.close()
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T231000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 2,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    regime = client.get("/api/analysis-bridge/regime")

    assert regime.status_code == 200
    regime_payload = regime.json()
    assert regime_payload["degraded"] is True
    assert regime_payload["degrade_reason"] == "regime_row_corruption"
    assert regime_payload["publish_id"] == "pub_2026-03-12_20260312T231000Z_01"
    assert regime_payload["as_of_date"] == "2026-03-12"
    assert regime_payload["freshness_state"] == "fresh"
    assert regime_payload["rows"] == []


def test_analysis_bridge_state_eval_returns_short_reason_payload(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO state_eval_daily (
                publish_id, as_of_date, code, state_action, side, holding_band, strategy_tags,
                decision_3way, confidence, reason_codes, reason_text_top3, freshness_state
            ) VALUES (
                'pub_2026-03-12_20260312T232000Z_01', DATE '2026-03-12', '1301', 'enter', 'long', 'buy_21_60',
                '["box_breakout","volume_surge"]', 'enter', 0.81, '["BUY_TREND","ADVERSE_RISK_CONTROL"]',
                '["Box breakout","Similar wins lead","Adverse risk capped"]', 'fresh'
            )
            """
        )
    finally:
        conn.close()
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T232000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 1,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/state-eval")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert len(payload["rows"]) == 1
    row = payload["rows"][0]
    assert row["holding_band"] == "buy_21_60"
    assert row["strategy_tags"] == '["box_breakout","volume_surge"]'
    assert row["reason_text_top3"] == '["Box breakout","Similar wins lead","Adverse risk capped"]'


def test_analysis_bridge_state_eval_supports_code_filter(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO state_eval_daily (
                publish_id, as_of_date, code, state_action, side, holding_band, strategy_tags,
                decision_3way, confidence, reason_codes, reason_text_top3, freshness_state
            ) VALUES
            (
                'pub_2026-03-12_20260312T232100Z_01', DATE '2026-03-12', '1301', 'enter', 'long', 'buy_21_60',
                '["box_breakout"]', 'enter', 0.81, '["BUY_TREND"]',
                '["Box breakout","Similar wins lead","Adverse risk capped"]', 'fresh'
            ),
            (
                'pub_2026-03-12_20260312T232100Z_01', DATE '2026-03-12', '1302', 'wait', 'long', 'buy_5_20',
                '["pullback_rebound"]', 'wait', 0.51, '["WAIT_SETUP"]',
                '["Pullback rebound","Similar wins lead","Adverse risk capped"]', 'fresh'
            )
            """
        )
    finally:
        conn.close()
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T232100Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 2,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/state-eval?code=1302")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["code"] == "1302"


def test_analysis_bridge_internal_state_eval_tags_reads_ops_rollups(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    conn = duckdb.connect(str(result_db), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO state_eval_daily (
                publish_id, as_of_date, code, state_action, side, holding_band, strategy_tags,
                decision_3way, confidence, reason_codes, reason_text_top3, freshness_state
            ) VALUES (
                'pub_2026-03-12_20260312T233000Z_01', DATE '2026-03-12', '1301', 'enter', 'long', 'buy_21_60',
                '["box_breakout","volume_surge"]', 'enter', 0.81, '["BUY_TREND"]',
                '["Box breakout","Similar wins lead","Adverse risk capped"]', 'fresh'
            )
            """
        )
    finally:
        conn.close()
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T233000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 1,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON NOT NULL,
                worst_failure_examples JSON NOT NULL,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups (
                rollup_id, publish_id, as_of_date, side, holding_band, strategy_tag,
                observation_count, labeled_count, enter_count, wait_count, skip_count,
                expectancy_mean, adverse_mean, large_loss_rate, win_rate, teacher_alignment_mean,
                failure_count, readiness_hint, latest_failure_examples, worst_failure_examples, summary_json, created_at
            ) VALUES (
                'pub_2026-03-12_20260312T233000Z_01:long:buy_21_60:box_breakout',
                'pub_2026-03-12_20260312T233000Z_01',
                DATE '2026-03-12',
                'long',
                'buy_21_60',
                'box_breakout',
                12, 12, 7, 3, 2,
                0.051, 0.032, 0.08, 0.58, 0.67,
                3, 'needs_samples',
                '[{\"code\":\"1301\"}]',
                '[{\"code\":\"1302\"}]',
                '{\"failure_count\":3}',
                TIMESTAMP '2026-03-12 23:30:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-tags?side=long")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert payload["publish_id"] == "pub_2026-03-12_20260312T233000Z_01"
    assert len(payload["rows"]) == 1
    row = payload["rows"][0]
    assert row["strategy_tag"] == "box_breakout"
    assert row["holding_band"] == "buy_21_60"
    assert row["readiness_hint"] == "needs_samples"
    assert row["latest_failure_examples"] == '[{"code":"1301"}]'


def test_analysis_bridge_internal_state_eval_tags_csv_exports_rows(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T234000Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON NOT NULL,
                worst_failure_examples JSON NOT NULL,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups (
                rollup_id, publish_id, as_of_date, side, holding_band, strategy_tag,
                observation_count, labeled_count, enter_count, wait_count, skip_count,
                expectancy_mean, adverse_mean, large_loss_rate, win_rate, teacher_alignment_mean,
                failure_count, readiness_hint, latest_failure_examples, worst_failure_examples, summary_json, created_at
            ) VALUES (
                'pub_2026-03-12_20260312T234000Z_01:short:sell_5_10:extension_fade',
                'pub_2026-03-12_20260312T234000Z_01',
                DATE '2026-03-12',
                'short',
                'sell_5_10',
                'extension_fade',
                9, 9, 5, 2, 2,
                0.041, 0.027, 0.11, 0.55, 0.62,
                2, 'needs_samples',
                '[{\"code\":\"1303\"}]',
                '[{\"code\":\"1304\"}]',
                '{\"failure_count\":2}',
                TIMESTAMP '2026-03-12 23:40:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-tags.csv?side=short")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    body = response.text
    assert "strategy_tag" in body
    assert "extension_fade" in body
    assert "sell_5_10" in body


def test_analysis_bridge_internal_state_eval_tags_summary_groups_rows(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T235000Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON NOT NULL,
                worst_failure_examples JSON NOT NULL,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            (
                'pub_2026-03-12_20260312T235000Z_01:long:buy_21_60:box_breakout',
                'pub_2026-03-12_20260312T235000Z_01', DATE '2026-03-12', 'long', 'buy_21_60', 'box_breakout',
                70, 70, 40, 20, 10, 0.083, 0.029, 0.12, 0.64, 0.72, 4, 'promotable',
                '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:50:00'
            ),
            (
                'pub_2026-03-12_20260312T235000Z_01:short:sell_5_10:extension_fade',
                'pub_2026-03-12_20260312T235000Z_01', DATE '2026-03-12', 'short', 'sell_5_10', 'extension_fade',
                55, 55, 25, 15, 15, -0.011, 0.061, 0.47, 0.39, 0.58, 20, 'risk_heavy',
                '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:50:00'
            ),
            (
                'pub_2026-03-12_20260312T235000Z_01:long:buy_5_20:volume_surge',
                'pub_2026-03-12_20260312T235000Z_01', DATE '2026-03-12', 'long', 'buy_5_20', 'volume_surge',
                18, 18, 9, 5, 4, 0.052, 0.031, 0.14, 0.56, 0.63, 3, 'needs_samples',
                '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:50:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-tags/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    summary = payload["summary"]
    assert summary["top_expectancy"][0]["strategy_tag"] == "box_breakout"
    assert summary["risk_heavy"][0]["strategy_tag"] == "extension_fade"
    assert summary["needs_samples"][0]["strategy_tag"] == "volume_surge"


def test_analysis_bridge_internal_state_eval_candles_summary_filters_candle_tags(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T235100Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON,
                worst_failure_examples JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            (
                'r1', 'pub_2026-03-12_20260312T235100Z_01', DATE '2026-03-12', 'long', 'buy_5_20', 'bullish_engulfing',
                44, 44, 20, 10, 14, 0.061, 0.028, 0.09, 0.59, 0.64, 4, 'promotable', '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:51:00'
            ),
            (
                'r2', 'pub_2026-03-12_20260312T235100Z_01', DATE '2026-03-12', 'short', 'sell_5_10', 'shooting_star_reversal',
                51, 51, 19, 12, 20, -0.012, 0.063, 0.41, 0.38, 0.57, 19, 'risk_heavy', '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:51:00'
            ),
            (
                'r3', 'pub_2026-03-12_20260312T235100Z_01', DATE '2026-03-12', 'long', 'buy_21_60', 'box_breakout',
                60, 60, 26, 18, 16, 0.082, 0.021, 0.08, 0.66, 0.71, 3, 'promotable', '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:51:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-candles/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    summary = payload["summary"]
    assert summary["top_expectancy"][0]["strategy_tag"] == "bullish_engulfing"
    assert summary["risk_heavy"][0]["strategy_tag"] == "shooting_star_reversal"
    assert all(row["strategy_tag"] != "box_breakout" for row in payload["rows"])


def test_analysis_bridge_internal_state_eval_candle_combos_summary_filters_combo_tags(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T235150Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON,
                worst_failure_examples JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            (
                'c1', 'pub_2026-03-12_20260312T235150Z_01', DATE '2026-03-12', 'long', 'buy_5_20', 'bullish_engulfing_after_inside',
                44, 44, 20, 10, 14, 0.071, 0.026, 0.08, 0.61, 0.66, 4, 'promotable', '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:51:30'
            ),
            (
                'c2', 'pub_2026-03-12_20260312T235150Z_01', DATE '2026-03-12', 'short', 'sell_5_10', 'three_bar_bear_reversal',
                51, 51, 19, 12, 20, -0.014, 0.067, 0.43, 0.37, 0.55, 19, 'risk_heavy', '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:51:30'
            ),
            (
                'c3', 'pub_2026-03-12_20260312T235150Z_01', DATE '2026-03-12', 'long', 'buy_21_60', 'bullish_engulfing',
                60, 60, 26, 18, 16, 0.082, 0.021, 0.08, 0.66, 0.71, 3, 'promotable', '[]', '[]', '{}', TIMESTAMP '2026-03-12 23:51:30'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-candle-combos/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    summary = payload["summary"]
    assert summary["top_expectancy"][0]["strategy_tag"] == "bullish_engulfing_after_inside"
    assert summary["risk_heavy"][0]["strategy_tag"] == "three_bar_bear_reversal"
    assert all(row["strategy_tag"] != "bullish_engulfing" for row in payload["rows"])


def test_analysis_bridge_internal_state_eval_daily_summary_combines_views(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T236100Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON,
                worst_failure_examples JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_readiness (
                readiness_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                champion_version TEXT NOT NULL,
                challenger_version TEXT NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                expectancy_delta DOUBLE,
                improved_expectancy BOOLEAN NOT NULL,
                mae_non_worse BOOLEAN NOT NULL,
                adverse_move_non_worse BOOLEAN NOT NULL,
                stable_window BOOLEAN NOT NULL,
                alignment_ok BOOLEAN NOT NULL,
                readiness_pass BOOLEAN NOT NULL,
                reason_codes JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            ('a','pub_2026-03-12_20260312T236100Z_01',DATE '2026-03-12','long','buy_21_60','box_breakout',80,80,30,30,20,0.091,0.021,0.05,0.68,0.74,4,'promotable','[]','[]','{}',TIMESTAMP '2026-03-12 23:58:00'),
            ('b','pub_2026-03-12_20260312T236100Z_01',DATE '2026-03-12','long','buy_5_20','bullish_engulfing',44,44,20,10,14,0.061,0.028,0.09,0.59,0.64,4,'promotable','[]','[]','{}',TIMESTAMP '2026-03-12 23:58:00'),
            ('c','pub_2026-03-12_20260312T236100Z_01',DATE '2026-03-12','short','sell_5_10','extension_fade',51,51,19,12,20,-0.012,0.063,0.41,0.38,0.57,19,'risk_heavy','[]','[]','{}',TIMESTAMP '2026-03-12 23:58:00'),
            ('d','pub_2026-03-12_20260312T236100Z_01',DATE '2026-03-12','long','buy_5_20','volume_surge',18,18,9,5,4,0.052,0.031,0.14,0.56,0.63,3,'needs_samples','[]','[]','{}',TIMESTAMP '2026-03-12 23:58:00')
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_readiness VALUES (
                'pub_2026-03-12_20260312T236100Z_01:readiness',
                'pub_2026-03-12_20260312T236100Z_01',
                DATE '2026-03-12',
                'state_eval_baseline_v2',
                'state_eval_challenger_v2',
                64,
                0.018,
                TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
                '[]',
                '{"champion_similarity":0.58,"challenger_similarity":0.61}',
                TIMESTAMP '2026-03-12 23:58:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-daily-summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    daily_summary = payload["daily_summary"]
    assert daily_summary["top_strategy"]["strategy_tag"] == "box_breakout"
    assert daily_summary["top_candle"]["strategy_tag"] == "bullish_engulfing"
    assert daily_summary["risk_watch"]["strategy_tag"] == "extension_fade"
    assert daily_summary["sample_watch"]["strategy_tag"] == "volume_surge"
    assert daily_summary["promotion"]["readiness_pass"] is True


def test_analysis_bridge_internal_state_eval_daily_summary_history_reads_persisted_rows(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-13_20260313T010000Z_01",
        as_of_date="2026-03-13",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_daily_summaries (
                summary_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side_scope TEXT NOT NULL,
                top_strategy_tag TEXT,
                top_strategy_expectancy DOUBLE,
                top_candle_tag TEXT,
                top_candle_expectancy DOUBLE,
                risk_watch_tag TEXT,
                risk_watch_loss_rate DOUBLE,
                sample_watch_tag TEXT,
                sample_watch_labeled_count INTEGER,
                promotion_ready BOOLEAN,
                promotion_sample_count INTEGER,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_promotion_decisions (
                decision_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date TEXT,
                champion_version TEXT,
                challenger_version TEXT,
                decision TEXT NOT NULL,
                note TEXT,
                actor TEXT,
                summary_json JSON,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_daily_summaries VALUES
            (
                'pub_2026-03-13_20260313T010000Z_01:all',
                'pub_2026-03-13_20260313T010000Z_01',
                DATE '2026-03-13',
                'all',
                'box_breakout',
                0.082,
                'bullish_engulfing',
                0.061,
                'extension_fade',
                0.41,
                'volume_surge',
                18,
                TRUE,
                64,
                '{"top_strategy":{"strategy_tag":"box_breakout"},"promotion":{"readiness_pass":true}}',
                TIMESTAMP '2026-03-13 01:00:00'
            ),
            (
                'pub_2026-03-12_20260312T235900Z_01:all',
                'pub_2026-03-12_20260312T235900Z_01',
                DATE '2026-03-12',
                'all',
                'pullback_rebound',
                0.052,
                NULL,
                NULL,
                'rebound_failure',
                0.38,
                'inside_break_bull',
                12,
                FALSE,
                38,
                '{"top_strategy":{"strategy_tag":"pullback_rebound"},"promotion":{"readiness_pass":false}}',
                TIMESTAMP '2026-03-12 23:59:00'
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_promotion_decisions VALUES
            (
                'pub_2026-03-13_20260313T010000Z_01:approved:20260313T011500000000Z',
                'pub_2026-03-13_20260313T010000Z_01',
                '20260313',
                'champion_v1',
                'challenger_v2',
                'approved',
                'stable enough',
                'codex_cli',
                '{"expectancy_delta":0.12}',
                TIMESTAMP '2026-03-13 01:15:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-daily-summary/history?limit=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert len(payload["rows"]) == 2
    assert payload["rows"][0]["as_of_date"] == "2026-03-13"
    assert payload["rows"][0]["top_strategy_tag"] == "box_breakout"
    assert payload["rows"][0]["decision_status"] == "recorded"
    assert payload["rows"][0]["approval_decision"]["decision"] == "approved"
    assert payload["rows"][1]["decision_status"] == "not_ready"
    assert payload["rows"][1]["codex_command"] is None


def test_analysis_bridge_internal_state_eval_action_queue_returns_ranked_actions(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T236050Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_daily_summaries (
                summary_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side_scope TEXT NOT NULL,
                top_strategy_tag TEXT,
                top_strategy_expectancy DOUBLE,
                top_candle_tag TEXT,
                top_candle_expectancy DOUBLE,
                risk_watch_tag TEXT,
                risk_watch_loss_rate DOUBLE,
                sample_watch_tag TEXT,
                sample_watch_labeled_count INTEGER,
                promotion_ready BOOLEAN,
                promotion_sample_count INTEGER,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON,
                worst_failure_examples JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_readiness (
                readiness_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                champion_version TEXT NOT NULL,
                challenger_version TEXT NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                expectancy_delta DOUBLE,
                improved_expectancy BOOLEAN NOT NULL,
                mae_non_worse BOOLEAN NOT NULL,
                adverse_move_non_worse BOOLEAN NOT NULL,
                stable_window BOOLEAN NOT NULL,
                alignment_ok BOOLEAN NOT NULL,
                readiness_pass BOOLEAN NOT NULL,
                reason_codes JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_daily_summaries VALUES (
                'pub_2026-03-12_20260312T236050Z_01:all',
                'pub_2026-03-12_20260312T236050Z_01',
                DATE '2026-03-12',
                'all',
                'box_breakout',
                0.082,
                'bullish_engulfing_after_inside',
                0.061,
                'extension_fade',
                0.41,
                'volume_surge',
                18,
                TRUE,
                64,
                '{"top_strategy":{"side":"long","holding_band":"buy_21_60","strategy_tag":"box_breakout","expectancy_mean":0.082},"top_strategy_reason":"teacher and charts agree","risk_watch":{"side":"short","holding_band":"sell_5_10","strategy_tag":"extension_fade","large_loss_rate":0.41},"risk_watch_reason":"risk heavy setup","sample_watch":{"side":"long","holding_band":"buy_5_20","strategy_tag":"volume_surge","labeled_count":18},"sample_watch_reason":"still needs samples"}',
                TIMESTAMP '2026-03-12 23:58:00'
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            ('a1', 'pub_old', DATE '2026-03-10', 'long', 'buy_21_60', 'box_breakout', 60, 60, 28, 12, 20, 0.03, 0.02, 0.10, 0.58, 0.62, 5, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.62,"similarity_signal_mean":0.61}', TIMESTAMP '2026-03-10 01:00:00'),
            ('a2', 'pub_now', DATE '2026-03-12', 'long', 'buy_21_60', 'box_breakout', 60, 60, 34, 14, 12, 0.08, 0.02, 0.08, 0.67, 0.69, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.69,"similarity_signal_mean":0.70}', TIMESTAMP '2026-03-12 01:00:00'),
            ('a3', 'pub_old', DATE '2026-03-10', 'long', 'buy_21_60', 'bullish_engulfing_after_inside', 48, 48, 18, 12, 18, 0.02, 0.03, 0.11, 0.54, 0.59, 6, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.57,"similarity_signal_mean":0.58}', TIMESTAMP '2026-03-10 01:00:00'),
            ('a4', 'pub_now', DATE '2026-03-12', 'long', 'buy_21_60', 'bullish_engulfing_after_inside', 48, 48, 24, 10, 14, 0.07, 0.02, 0.07, 0.63, 0.66, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.65,"similarity_signal_mean":0.67}', TIMESTAMP '2026-03-12 01:00:00')
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_readiness VALUES (
                'pub_2026-03-12_20260312T236050Z_01:readiness',
                'pub_2026-03-12_20260312T236050Z_01',
                DATE '2026-03-12',
                'state_eval_baseline_v2',
                'state_eval_challenger_v2',
                64,
                0.018,
                TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
                '[]',
                '{"champion_similarity":0.58,"challenger_similarity":0.61,"champion_tag_prior":0.60,"challenger_tag_prior":0.66,"champion_combo_prior":0.57,"challenger_combo_prior":0.64}',
                TIMESTAMP '2026-03-12 23:58:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-action-queue")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert len(payload["actions"]) >= 4
    assert payload["actions"][0]["kind"] == "promotion_decision_pending"
    assert "promotion-decision-run" in payload["actions"][0]["note"]
    assert any(item["kind"] == "top_strategy" for item in payload["actions"])
    assert any(item["kind"] == "improving_combo" for item in payload["actions"])


def test_analysis_bridge_internal_state_eval_daily_summary_csv_exports_history(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-13_20260313T010500Z_01",
        as_of_date="2026-03-13",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_daily_summaries (
                summary_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side_scope TEXT NOT NULL,
                top_strategy_tag TEXT,
                top_strategy_expectancy DOUBLE,
                top_candle_tag TEXT,
                top_candle_expectancy DOUBLE,
                risk_watch_tag TEXT,
                risk_watch_loss_rate DOUBLE,
                sample_watch_tag TEXT,
                sample_watch_labeled_count INTEGER,
                promotion_ready BOOLEAN,
                promotion_sample_count INTEGER,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_daily_summaries VALUES (
                'pub_2026-03-13_20260313T010500Z_01:all',
                'pub_2026-03-13_20260313T010500Z_01',
                DATE '2026-03-13',
                'all',
                'box_breakout',
                0.082,
                'bullish_engulfing',
                0.061,
                'extension_fade',
                0.41,
                'volume_surge',
                18,
                TRUE,
                64,
                '{"top_strategy":{"strategy_tag":"box_breakout"}}',
                TIMESTAMP '2026-03-13 01:05:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-daily-summary.csv")

    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    body = response.text
    assert "top_strategy_tag" in body
    assert "decision_status" in body
    assert "box_breakout" in body


def test_analysis_bridge_internal_state_eval_trends_summarizes_recent_rollups(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-14_20260314T010000Z_01",
        as_of_date="2026-03-14",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON,
                worst_failure_examples JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            ('t1', 'pub_old', DATE '2026-03-10', 'long', 'buy_21_60', 'box_breakout', 60, 60, 20, 20, 20, 0.03, 0.03, 0.12, 0.55, 0.61, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.58,"similarity_signal_mean":0.57}', TIMESTAMP '2026-03-10 01:00:00'),
            ('t2', 'pub_mid', DATE '2026-03-12', 'long', 'buy_21_60', 'box_breakout', 60, 60, 24, 18, 18, 0.06, 0.03, 0.10, 0.58, 0.63, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.62,"similarity_signal_mean":0.61}', TIMESTAMP '2026-03-12 01:00:00'),
            ('t3', 'pub_now', DATE '2026-03-14', 'long', 'buy_21_60', 'box_breakout', 60, 60, 28, 16, 16, 0.09, 0.02, 0.08, 0.63, 0.67, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.66,"similarity_signal_mean":0.68}', TIMESTAMP '2026-03-14 01:00:00'),
            ('r1', 'pub_old', DATE '2026-03-10', 'short', 'sell_5_10', 'extension_fade', 52, 52, 20, 14, 18, 0.01, 0.05, 0.31, 0.45, 0.57, 11, 'risk_heavy', '[]', '[]', '{"teacher_signal_mean":0.53,"similarity_signal_mean":0.49}', TIMESTAMP '2026-03-10 01:00:00'),
            ('r2', 'pub_mid', DATE '2026-03-12', 'short', 'sell_5_10', 'extension_fade', 52, 52, 16, 14, 22, -0.01, 0.06, 0.39, 0.40, 0.54, 16, 'risk_heavy', '[]', '[]', '{"teacher_signal_mean":0.49,"similarity_signal_mean":0.45}', TIMESTAMP '2026-03-12 01:00:00'),
            ('r3', 'pub_now', DATE '2026-03-14', 'short', 'sell_5_10', 'extension_fade', 52, 52, 12, 14, 26, -0.04, 0.07, 0.48, 0.33, 0.50, 22, 'risk_heavy', '[]', '[]', '{"teacher_signal_mean":0.44,"similarity_signal_mean":0.41}', TIMESTAMP '2026-03-14 01:00:00')
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-trends?lookback=10&limit=3")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert payload["trends"]["improving"][0]["strategy_tag"] == "box_breakout"
    assert payload["trends"]["weakening"][0]["strategy_tag"] == "extension_fade"
    assert payload["trends"]["persistent_risk"][0]["strategy_tag"] == "extension_fade"


def test_analysis_bridge_internal_state_eval_candle_combo_trends_filters_combo_tags(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-14_20260314T010100Z_01",
        as_of_date="2026-03-14",
        freshness_state="fresh",
        table_row_counts={
            "candidate_daily": 0,
            "regime_daily": 0,
            "state_eval_daily": 0,
            "similar_cases_daily": 0,
            "similar_case_paths": 0,
        },
    )
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
                rollup_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT NOT NULL,
                strategy_tag TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                enter_count INTEGER NOT NULL,
                wait_count INTEGER NOT NULL,
                skip_count INTEGER NOT NULL,
                expectancy_mean DOUBLE,
                adverse_mean DOUBLE,
                large_loss_rate DOUBLE,
                win_rate DOUBLE,
                teacher_alignment_mean DOUBLE,
                failure_count INTEGER NOT NULL,
                readiness_hint TEXT NOT NULL,
                latest_failure_examples JSON,
                worst_failure_examples JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_tag_rollups VALUES
            ('k1', 'pub_old', DATE '2026-03-10', 'long', 'buy_5_20', 'bullish_engulfing_after_inside', 44, 44, 20, 10, 14, 0.03, 0.03, 0.12, 0.55, 0.61, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.58,"similarity_signal_mean":0.57}', TIMESTAMP '2026-03-10 01:00:00'),
            ('k2', 'pub_now', DATE '2026-03-14', 'long', 'buy_5_20', 'bullish_engulfing_after_inside', 44, 44, 25, 9, 10, 0.08, 0.02, 0.08, 0.64, 0.67, 3, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.66,"similarity_signal_mean":0.68}', TIMESTAMP '2026-03-14 01:00:00'),
            ('k3', 'pub_old', DATE '2026-03-10', 'short', 'sell_5_10', 'three_bar_bear_reversal', 50, 50, 20, 14, 16, 0.00, 0.05, 0.37, 0.45, 0.57, 11, 'risk_heavy', '[]', '[]', '{"teacher_signal_mean":0.53,"similarity_signal_mean":0.49}', TIMESTAMP '2026-03-10 01:00:00'),
            ('k4', 'pub_now', DATE '2026-03-14', 'short', 'sell_5_10', 'three_bar_bear_reversal', 50, 50, 11, 13, 26, -0.04, 0.07, 0.49, 0.33, 0.50, 22, 'risk_heavy', '[]', '[]', '{"teacher_signal_mean":0.44,"similarity_signal_mean":0.41}', TIMESTAMP '2026-03-14 01:00:00'),
            ('k5', 'pub_now', DATE '2026-03-14', 'long', 'buy_21_60', 'box_breakout', 60, 60, 28, 16, 16, 0.09, 0.02, 0.08, 0.63, 0.67, 4, 'promotable', '[]', '[]', '{"teacher_signal_mean":0.66,"similarity_signal_mean":0.68}', TIMESTAMP '2026-03-14 01:00:00')
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-candle-combo-trends?lookback=10&limit=3")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    assert payload["trends"]["improving"][0]["strategy_tag"] == "bullish_engulfing_after_inside"
    assert payload["trends"]["weakening"][0]["strategy_tag"] == "three_bar_bear_reversal"
    assert payload["trends"]["persistent_risk"][0]["strategy_tag"] == "three_bar_bear_reversal"


def test_analysis_bridge_internal_state_eval_promotion_review_returns_readiness(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T236000Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_readiness (
                readiness_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                champion_version TEXT NOT NULL,
                challenger_version TEXT NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                expectancy_delta DOUBLE,
                improved_expectancy BOOLEAN NOT NULL,
                mae_non_worse BOOLEAN NOT NULL,
                adverse_move_non_worse BOOLEAN NOT NULL,
                stable_window BOOLEAN NOT NULL,
                alignment_ok BOOLEAN NOT NULL,
                readiness_pass BOOLEAN NOT NULL,
                reason_codes JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_shadow_runs (
                shadow_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                code TEXT NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT,
                strategy_tags JSON,
                champion_decision TEXT NOT NULL,
                challenger_decision TEXT NOT NULL,
                champion_confidence DOUBLE,
                challenger_confidence DOUBLE,
                expected_return DOUBLE,
                adverse_move DOUBLE,
                teacher_alignment DOUBLE,
                label_available BOOLEAN NOT NULL,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_readiness VALUES (
                'pub_2026-03-12_20260312T236000Z_01:readiness',
                'pub_2026-03-12_20260312T236000Z_01',
                DATE '2026-03-12',
                'state_eval_baseline_v2',
                'state_eval_challenger_v2',
                64,
                0.021,
                TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
                '[]',
                '{"champion_selected":20,"challenger_selected":24}',
                TIMESTAMP '2026-03-12 23:59:00'
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_shadow_runs VALUES
            (
                's1', 'pub_2026-03-12_20260312T236000Z_01', DATE '2026-03-12', '1301', 'long', 'buy_21_60', '["box_breakout"]',
                'enter', 'enter', 0.8, 0.82, 0.05, 0.03, 0.66, TRUE, '{}', TIMESTAMP '2026-03-12 23:59:00'
            ),
            (
                's2', 'pub_2026-03-12_20260312T236000Z_01', DATE '2026-03-12', '1303', 'short', 'sell_5_10', '["extension_fade"]',
                'wait', 'enter', 0.5, 0.76, -0.01, 0.07, 0.58, TRUE, '{}', TIMESTAMP '2026-03-12 23:59:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/state-eval-promotion-review")

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    review = payload["review"]
    assert review["sample_count"] == 64
    assert review["readiness_pass"] is True
    assert review["approval_decision"] is None
    assert len(review["by_side"]) == 2


def test_analysis_bridge_internal_state_eval_promotion_decision_persists_latest_decision(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T236500Z_01",
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
    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_readiness (
                readiness_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                champion_version TEXT NOT NULL,
                challenger_version TEXT NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                expectancy_delta DOUBLE,
                improved_expectancy BOOLEAN NOT NULL,
                mae_non_worse BOOLEAN NOT NULL,
                adverse_move_non_worse BOOLEAN NOT NULL,
                stable_window BOOLEAN NOT NULL,
                alignment_ok BOOLEAN NOT NULL,
                readiness_pass BOOLEAN NOT NULL,
                reason_codes JSON,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_state_eval_shadow_runs (
                shadow_id TEXT PRIMARY KEY,
                publish_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                code TEXT NOT NULL,
                side TEXT NOT NULL,
                holding_band TEXT,
                strategy_tags JSON,
                champion_decision TEXT NOT NULL,
                challenger_decision TEXT NOT NULL,
                champion_confidence DOUBLE,
                challenger_confidence DOUBLE,
                expected_return DOUBLE,
                adverse_move DOUBLE,
                teacher_alignment DOUBLE,
                label_available BOOLEAN NOT NULL,
                summary_json JSON NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_readiness VALUES (
                'pub_2026-03-12_20260312T236500Z_01:readiness',
                'pub_2026-03-12_20260312T236500Z_01',
                DATE '2026-03-12',
                'state_eval_baseline_v2',
                'state_eval_challenger_v2',
                80,
                0.031,
                TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
                '[]',
                '{"champion_selected":20,"challenger_selected":24}',
                TIMESTAMP '2026-03-12 23:59:00'
            )
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.post(
        "/api/analysis-bridge/internal/state-eval-promotion-decision",
        json={"decision": "approved", "note": "looks stable", "actor": "test_user"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["degraded"] is False
    review = payload["review"]
    assert review["approval_decision"]["decision"] == "approved"
    assert review["approval_decision"]["note"] == "looks stable"
    assert review["approval_decision"]["actor"] == "test_user"


def test_analysis_bridge_internal_replay_progress_returns_current_run(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    data_dir = tmp_path / "data"
    ops_db = data_dir / "external_analysis" / "ops.duckdb"
    source_db = data_dir / "stocks.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    (data_dir / "external_analysis").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(core_config, "DATA_DIR", data_dir)
    ensure_result_db(str(result_db))

    source_conn = duckdb.connect(str(source_db), read_only=False)
    try:
        source_conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                source TEXT
            )
            """
        )
        source_conn.execute(
            """
            INSERT INTO daily_bars VALUES
                ('1301', 20260310, 1, 1, 1, 1, 100, 'pan'),
                ('1301', 20260311, 1, 1, 1, 1, 100, 'pan'),
                ('1301', 20260312, 1, 1, 1, 1, 100, 'pan')
            """
        )
    finally:
        source_conn.close()

    ops_conn = duckdb.connect(str(ops_db), read_only=False)
    try:
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_replay_runs (
                replay_id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                start_as_of_date DATE NOT NULL,
                end_as_of_date DATE NOT NULL,
                max_days INTEGER,
                universe_filter TEXT,
                universe_limit INTEGER,
                created_at TIMESTAMP NOT NULL,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                last_completed_as_of_date DATE,
                error_class TEXT,
                details_json JSON
            )
            """
        )
        ops_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_replay_days (
                replay_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                publish_id TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                error_class TEXT,
                details_json JSON,
                PRIMARY KEY (replay_id, as_of_date)
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_replay_runs VALUES (
                'bootstrap_demo',
                'historical_replay_runner',
                'running',
                DATE '2026-03-10',
                DATE '2026-03-12',
                NULL,
                NULL,
                NULL,
                TIMESTAMP '2026-03-14 18:20:00',
                TIMESTAMP '2026-03-14 18:21:00',
                NULL,
                DATE '2026-03-11',
                NULL,
                '{"resume": true, "current_phase": "candidate", "heartbeat_at": "2026-03-14T18:24:30", "current_publish_id": "pub3"}'
            )
            """
        )
        ops_conn.execute(
            """
            INSERT INTO external_replay_days VALUES
                ('bootstrap_demo', DATE '2026-03-10', 'success', 1, 'pub1', TIMESTAMP '2026-03-14 18:21:00', TIMESTAMP '2026-03-14 18:22:00', NULL, '{}'),
                ('bootstrap_demo', DATE '2026-03-11', 'success', 1, 'pub2', TIMESTAMP '2026-03-14 18:22:00', TIMESTAMP '2026-03-14 18:23:00', NULL, '{}'),
                ('bootstrap_demo', DATE '2026-03-12', 'running', 1, 'pub3', TIMESTAMP '2026-03-14 18:24:00', NULL, NULL, '{}')
            """
        )
    finally:
        ops_conn.close()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())
    response = client.get("/api/analysis-bridge/internal/replay-progress")

    assert response.status_code == 200
    payload = response.json()
    assert payload["running"] is True
    assert payload["current_run"]["replay_id"] == "bootstrap_demo"
    assert payload["current_run"]["total_days"] == 3
    assert payload["current_run"]["completed_days"] == 2
    assert payload["current_run"]["running_days"] == 1
    assert payload["current_run"]["current_day"]["as_of_date"] == "2026-03-12"
    assert payload["current_run"]["current_phase"] == "candidate"
    assert payload["current_run"]["last_heartbeat_at"] == "2026-03-14T18:24:30"
    assert payload["current_run"]["current_publish_id"] == "pub3"
    assert "eta_seconds" in payload["current_run"]
    assert "eta_at" in payload["current_run"]
