from __future__ import annotations

import json
from datetime import date, timedelta

import duckdb

from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot
from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.labels.rolling_labels import build_rolling_labels
from external_analysis.labels.store import ensure_label_db
from external_analysis.models import forecast_surface as forecast_surface_module
from external_analysis.models.candidate_baseline import BASELINE_VERSION, run_candidate_baseline
from external_analysis.models.state_eval_baseline import DECISION_ENTER, DECISION_SKIP, DECISION_WAIT, _decision_from_score
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.result_schema import ensure_result_db


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_candidate_export_db(export_db: str) -> list[int]:
    ensure_export_db(export_db)
    dates = _weekday_ints(date(2026, 1, 5), 70)
    patterns = {
        "1301": lambda idx: 100.0 + (idx * 1.2),
        "1302": lambda idx: 100.0 + (idx * 0.7),
        "1303": lambda idx: 140.0 - (idx * 1.0),
        "1304": lambda idx: 120.0 - (idx * 0.6),
    }
    conn = duckdb.connect(export_db, read_only=False)
    try:
        for idx, trade_date in enumerate(dates):
            for code, fn in patterns.items():
                close_price = fn(idx)
                high_price = close_price + 1.5
                low_price = close_price - 1.5
                candle_flags = "candidate-test"
                if code == "1301" and idx == 44:
                    candle_flags = "inside_bar"
                elif code == "1301" and idx == 45:
                    candle_flags = "bullish_engulfing"
                elif code == "1303" and idx == 44:
                    candle_flags = "inside_bar"
                elif code == "1303" and idx == 45:
                    candle_flags = "shooting_star,bearish_engulfing"
                conn.execute(
                    """
                    INSERT INTO bars_daily_export
                    (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [code, trade_date, close_price - 0.5, high_price, low_price, close_price, 1000 + (idx * 10), "pan", f"{code}-{idx}", "run-1"],
                )
                ma20 = close_price - 2.0 if code in {"1301", "1302"} else close_price + 2.0
                diff20_pct = (close_price / ma20) - 1.0
                conn.execute(
                    """
                    INSERT INTO indicator_daily_export
                    (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        code,
                        trade_date,
                        ma20 + 1.0,
                        ma20,
                        ma20 - 3.0,
                        None,
                        None,
                        3.0,
                        diff20_pct,
                        1.0,
                        18 if code in {"1301", "1302"} else 4,
                        6 if code in {"1301", "1302"} else 1,
                        20,
                        candle_flags,
                        f"i-{code}-{idx}",
                        "run-1",
                    ],
                )
        conn.execute(
            """
            INSERT INTO pattern_state_export
            (code, trade_date, ppp_state, abc_state, box_state, box_upper, box_lower, ranking_state, event_flags, row_hash, export_run_id)
            VALUES
            ('1301', ?, 'up', 'up', 'breakout', 0, 0, NULL, NULL, 'p-1301', 'run-1'),
            ('1303', ?, 'down', 'down', 'breakdown', 0, 0, NULL, NULL, 'p-1303', 'run-1')
            """,
            [dates[45], dates[45]],
        )
        conn.execute(
            """
            INSERT INTO trade_event_export
            (code, event_ts, event_seq, event_type, broker_label, qty, price, row_hash, export_run_id)
            VALUES
            ('1301', CAST(? AS TIMESTAMP), 1, 'SPOT_BUY', 'test', 100, 120.0, 'te-1301-1', 'run-1'),
            ('1301', CAST(? AS TIMESTAMP), 1, 'SPOT_BUY', 'test', 100, 124.0, 'te-1301-2', 'run-1'),
            ('1303', CAST(? AS TIMESTAMP), 1, 'MARGIN_OPEN_SHORT', 'test', 100, 98.0, 'te-1303-1', 'run-1')
            """,
            [
                f"{str(dates[40])[0:4]}-{str(dates[40])[4:6]}-{str(dates[40])[6:8]} 09:00:00",
                f"{str(dates[45])[0:4]}-{str(dates[45])[4:6]}-{str(dates[45])[6:8]} 09:00:00",
                f"{str(dates[45])[0:4]}-{str(dates[45])[4:6]}-{str(dates[45])[6:8]} 09:00:00",
            ],
        )
        conn.execute(
            """
            INSERT INTO position_snapshot_export
            (code, snapshot_at, spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note, row_hash, export_run_id)
            VALUES
            ('1301', CAST(? AS TIMESTAMP), 100, 0, 0, 100, 0, FALSE, NULL, 'ps-1301', 'run-1'),
            ('1303', CAST(? AS TIMESTAMP), 0, 0, 100, 0, 100, FALSE, NULL, 'ps-1303', 'run-1')
            """,
            [
                f"{str(dates[45])[0:4]}-{str(dates[45])[4:6]}-{str(dates[45])[6:8]} 15:00:00",
                f"{str(dates[45])[0:4]}-{str(dates[45])[4:6]}-{str(dates[45])[6:8]} 15:00:00",
            ],
        )
    finally:
        conn.close()
    return dates


def _seed_historical_tag_rollups(ops_db: str, as_of_date: int) -> None:
    ensure_ops_db(ops_db)
    conn = duckdb.connect(ops_db, read_only=False)
    try:
        prior_date = int(str(as_of_date - 3))
        rows = [
            [
                f"prior:{prior_date}:long:buy_5_20:bullish_engulfing_after_inside",
                "pub_prior_long",
                f"{str(prior_date)[0:4]}-{str(prior_date)[4:6]}-{str(prior_date)[6:8]}",
                "long",
                "buy_5_20",
                "bullish_engulfing_after_inside",
                64,
                64,
                32,
                18,
                14,
                0.082,
                0.028,
                0.11,
                0.67,
                0.74,
                8,
                "promotable",
                "[]",
                "[]",
                '{"teacher_signal_mean":0.71,"similarity_signal_mean":0.69}',
                "2026-03-10 00:00:00",
            ],
            [
                f"prior:{prior_date}:long:buy_21_60:bullish_engulfing_after_inside",
                "pub_prior_long_2",
                f"{str(prior_date)[0:4]}-{str(prior_date)[4:6]}-{str(prior_date)[6:8]}",
                "long",
                "buy_21_60",
                "bullish_engulfing_after_inside",
                72,
                72,
                38,
                20,
                14,
                0.094,
                0.026,
                0.10,
                0.69,
                0.76,
                7,
                "promotable",
                "[]",
                "[]",
                '{"teacher_signal_mean":0.73,"similarity_signal_mean":0.70}',
                "2026-03-10 00:00:00",
            ],
            [
                f"prior:{prior_date}:short:sell_5_10:bearish_engulfing_after_inside",
                "pub_prior_short",
                f"{str(prior_date)[0:4]}-{str(prior_date)[4:6]}-{str(prior_date)[6:8]}",
                "short",
                "sell_5_10",
                "bearish_engulfing_after_inside",
                58,
                58,
                29,
                17,
                12,
                0.061,
                0.031,
                0.13,
                0.63,
                0.70,
                10,
                "promotable",
                "[]",
                "[]",
                '{"teacher_signal_mean":0.68,"similarity_signal_mean":0.66}',
                "2026-03-10 00:00:00",
            ],
            [
                f"prior:{prior_date}:short:sell_11_20:bearish_engulfing_after_inside",
                "pub_prior_short_2",
                f"{str(prior_date)[0:4]}-{str(prior_date)[4:6]}-{str(prior_date)[6:8]}",
                "short",
                "sell_11_20",
                "bearish_engulfing_after_inside",
                61,
                61,
                31,
                18,
                12,
                0.067,
                0.029,
                0.12,
                0.65,
                0.72,
                9,
                "promotable",
                "[]",
                "[]",
                '{"teacher_signal_mean":0.69,"similarity_signal_mean":0.67}',
                "2026-03-10 00:00:00",
            ],
        ]
        conn.executemany(
            """
            INSERT INTO external_state_eval_tag_rollups (
                rollup_id, publish_id, as_of_date, side, holding_band, strategy_tag,
                observation_count, labeled_count, enter_count, wait_count, skip_count,
                expectancy_mean, adverse_mean, large_loss_rate, win_rate, teacher_alignment_mean,
                failure_count, readiness_hint, latest_failure_examples, worst_failure_examples, summary_json, created_at
            ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    finally:
        conn.close()


def test_run_candidate_baseline_publishes_candidates_regime_and_metrics(monkeypatch, tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _seed_candidate_export_db(str(export_db))
    ensure_label_db(str(label_db))
    ensure_result_db(str(result_db))
    _seed_historical_tag_rollups(str(ops_db), dates[45])
    build_rolling_labels(str(export_db), str(label_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))

    payload = run_candidate_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[45],
        publish_id="pub_2026-03-12_20260312T180000Z_01",
        freshness_state="fresh",
        ops_db_path=str(ops_db),
    )

    assert payload["ok"] is True
    assert payload["candidate_count_long"] > 0
    assert payload["candidate_count_short"] > 0

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        candidate_rows = conn.execute(
            "SELECT side, COUNT(*) FROM candidate_daily WHERE publish_id = ? GROUP BY side ORDER BY side",
            [payload["publish_id"]],
        ).fetchall()
        regime_row = conn.execute(
            "SELECT regime_tag FROM regime_daily WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
        metrics_row = conn.execute(
            "SELECT baseline_version, candidate_count_long, candidate_count_short, avg_ret_20_top20 FROM nightly_candidate_metrics WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
        state_eval_rows = conn.execute(
            "SELECT side, holding_band, strategy_tags, reason_text_top3, decision_3way, COUNT(*) FROM state_eval_daily WHERE publish_id = ? GROUP BY side, holding_band, strategy_tags, reason_text_top3, decision_3way ORDER BY side, decision_3way",
            [payload["publish_id"]],
        ).fetchall()
        manifest_row = conn.execute(
            "SELECT table_row_counts FROM publish_manifest WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
    finally:
        conn.close()
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        teacher_count = ops_conn.execute("SELECT COUNT(*) FROM external_trade_teacher_profiles").fetchone()
        teacher_rows = ops_conn.execute(
            """
            SELECT code, side, holding_band, strategy_tags, trade_count, alignment_score, position_bias, summary_json
            FROM external_trade_teacher_profiles
            ORDER BY code, side, holding_band
            """
        ).fetchall()
        readiness_row = ops_conn.execute(
            "SELECT champion_version, challenger_version, sample_count, readiness_pass, reason_codes, summary_json FROM external_state_eval_readiness WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
        failure_count = ops_conn.execute(
            "SELECT COUNT(*) FROM external_state_eval_failure_samples WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
        tag_rollup_rows = ops_conn.execute(
            """
            SELECT side, holding_band, strategy_tag, labeled_count, expectancy_mean, adverse_mean, failure_count, latest_failure_examples, worst_failure_examples
            FROM external_state_eval_tag_rollups
            WHERE publish_id = ?
            ORDER BY side, holding_band, strategy_tag
            """,
            [payload["publish_id"]],
        ).fetchall()
        daily_summary_rows = ops_conn.execute(
            """
            SELECT side_scope, top_strategy_tag, top_candle_tag, risk_watch_tag, sample_watch_tag, promotion_ready, promotion_sample_count
            FROM external_state_eval_daily_summaries
            WHERE publish_id = ?
            ORDER BY side_scope
            """,
            [payload["publish_id"]],
        ).fetchall()
    finally:
        ops_conn.close()

    assert dict(candidate_rows)["long"] > 0
    assert dict(candidate_rows)["short"] > 0
    assert regime_row is not None
    assert regime_row[0] in {"risk_on", "neutral", "risk_off"}
    assert metrics_row is not None
    assert state_eval_rows
    assert str(metrics_row[0]) == BASELINE_VERSION
    assert int(metrics_row[1]) > 0
    assert int(metrics_row[2]) > 0
    assert manifest_row is not None
    assert '"candidate_daily"' in str(manifest_row[0])
    assert '"state_eval_daily"' in str(manifest_row[0])
    assert int(teacher_count[0]) > 0
    assert teacher_rows
    assert any(int(row[4]) > 0 for row in teacher_rows)
    teacher_summary = str(teacher_rows[0][7])
    assert '"band_alignment"' in teacher_summary
    assert '"tag_alignment"' in teacher_summary
    assert '"confidence_weight"' in teacher_summary
    assert '"effective_signal"' in teacher_summary
    assert readiness_row is not None
    assert readiness_row[0] == "state_eval_baseline_v2"
    assert readiness_row[1] == "state_eval_challenger_v2"
    assert int(readiness_row[2]) >= 0
    assert isinstance(readiness_row[4], str)
    assert '"champion_similarity"' in str(readiness_row[5])
    assert '"champion_tag_prior"' in str(readiness_row[5])
    assert '"champion_combo_prior"' in str(readiness_row[5])
    assert int(failure_count[0]) >= 1
    assert tag_rollup_rows
    assert daily_summary_rows
    first_tag_rollup = tag_rollup_rows[0]
    assert first_tag_rollup[0] in {"long", "short"}
    assert first_tag_rollup[1] is not None
    assert first_tag_rollup[2] is not None
    assert int(first_tag_rollup[3]) >= 0
    assert str(first_tag_rollup[7]).startswith("[")
    assert str(first_tag_rollup[8]).startswith("[")
    first_state_eval = state_eval_rows[0]
    assert first_state_eval[1] is not None
    assert str(first_state_eval[2]).startswith("[")
    assert str(first_state_eval[3]).startswith("[")
    assert "Similar" in str(first_state_eval[3])
    assert any("Combo strength" in str(row[3]) or "Historically strong" in str(row[3]) for row in state_eval_rows)
    assert any("bullish_engulfing" in str(row[2]) or "bearish_engulfing" in str(row[2]) for row in state_eval_rows)
    assert any("bullish_engulfing_after_inside" in str(row[2]) or "bearish_engulfing_after_inside" in str(row[2]) for row in state_eval_rows)
    assert [row[0] for row in daily_summary_rows] == ["all", "long", "short"]
    assert any(row[1] is not None or row[2] is not None or row[3] is not None or row[4] is not None for row in daily_summary_rows)
    assert isinstance(daily_summary_rows[0][5], bool)

    snapshot = get_analysis_bridge_snapshot()
    assert snapshot["degraded"] is False
    assert snapshot["publish"]["publish_id"] == payload["publish_id"]
    assert snapshot["public_table_counts"]["candidate_daily"] == payload["candidate_count_long"] + payload["candidate_count_short"]
    assert snapshot["public_table_counts"]["regime_daily"] == 1
    assert snapshot["public_table_counts"]["state_eval_daily"] == payload["state_eval_count"]
    assert "candidate_component_scores" not in snapshot["public_table_counts"]
    assert "nightly_candidate_metrics" not in snapshot["public_table_counts"]


def test_run_candidate_baseline_persists_forecast_surface_with_source_db(monkeypatch, tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _seed_candidate_export_db(str(export_db))
    ensure_label_db(str(label_db))
    ensure_result_db(str(result_db))
    _seed_historical_tag_rollups(str(ops_db), dates[45])
    build_rolling_labels(str(export_db), str(label_db))
    _seed_forecast_surface_source_db(str(source_db), dates[45])
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))

    payload = run_candidate_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        source_db_path=str(source_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[45],
        publish_id="pub_2026-03-12_20260312T180000Z_02",
        freshness_state="fresh",
        ops_db_path=str(ops_db),
    )

    assert payload["ok"] is True
    assert payload["forecast_surface_saved"] is True
    assert payload["forecast_surface"] is not None
    assert int(payload["forecast_surface"]["row_count"]) > 0
    assert int(payload["forecast_surface"]["universe_code_count"]) > 0
    assert int(payload["forecast_surface"]["expected_row_count"]) == int(payload["forecast_surface"]["actual_row_count"])
    assert int(payload["forecast_surface"]["missing_row_count"]) == 0
    assert float(payload["forecast_surface"]["coverage_ratio"]) == 1.0
    assert set(payload["forecast_surface"]["side_counts"]) == {"long", "short"}
    assert int(payload["forecast_surface"]["side_counts"]["long"]) > 0
    assert int(payload["forecast_surface"]["side_counts"]["short"]) > 0
    assert isinstance(payload["forecast_surface"]["alerts"], list)
    assert payload["forecast_surface_evaluation_saved"] is True
    assert payload["forecast_surface_evaluation"] is not None
    assert payload["forecast_surface_evaluation"]["scope_type"] == "publish"

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        surface_rows = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT side) FROM forecast_surface_daily WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
        evaluation_rows = conn.execute(
            "SELECT COUNT(*) FROM forecast_surface_evaluation_runs WHERE publish_id = ?",
            [payload["publish_id"]],
        ).fetchone()
        run_row = conn.execute(
            """
            SELECT
                universe_code_count,
                expected_row_count,
                actual_row_count,
                missing_row_count,
                coverage_ratio,
                feature_frame_version,
                market_opportunity_score_enabled,
                personal_fit_score_enabled,
                side_counts_json,
                action_counts_json,
                source_context_presence_json,
                alerts_json
            FROM forecast_surface_runs
            WHERE publish_id = ?
            """,
            [payload["publish_id"]],
        ).fetchone()
        sample_row = conn.execute(
            """
            SELECT action_state, direction_prob, expected_ret_20, setup_tags, reason_codes, market_opportunity_score, personal_fit_score
            FROM forecast_surface_daily
            WHERE publish_id = ?
            ORDER BY code, side
            LIMIT 1
            """,
            [payload["publish_id"]],
        ).fetchone()
    finally:
        conn.close()

    assert surface_rows is not None
    assert int(surface_rows[0]) == int(payload["forecast_surface"]["row_count"])
    assert int(surface_rows[1]) == 2
    assert int(evaluation_rows[0]) == 1
    assert run_row is not None
    assert int(run_row[0]) == int(payload["forecast_surface"]["universe_code_count"])
    assert int(run_row[1]) == int(payload["forecast_surface"]["expected_row_count"])
    assert int(run_row[2]) == int(payload["forecast_surface"]["actual_row_count"])
    assert int(run_row[3]) == int(payload["forecast_surface"]["missing_row_count"])
    assert float(run_row[4]) == float(payload["forecast_surface"]["coverage_ratio"])
    assert run_row[5] is None or isinstance(run_row[5], str)
    assert bool(run_row[6]) is True
    assert bool(run_row[7]) is True
    assert '"long"' in str(run_row[8])
    assert '"enter"' in str(run_row[9])
    assert '"signal_decision_daily"' in str(run_row[10])
    assert str(run_row[11]).startswith("[")
    assert sample_row is not None
    assert sample_row[0] in {"enter", "wait", "skip"}
    assert float(sample_row[1]) >= 0.0
    assert str(sample_row[3]).startswith("[")
    assert str(sample_row[4]).startswith("[")
    assert float(sample_row[5]) >= 0.0
    assert 0.0 <= float(sample_row[6]) <= 1.0
    assert payload["forecast_surface_source_context_presence"]["signal_decision_daily"] is True
    assert payload["forecast_surface_source_context_presence"]["market_regime_daily"] is True


def test_forecast_surface_opportunity_score_rejects_unfavorable_signed_expected_move() -> None:
    source_context = forecast_surface_module._SourceContext(
        signal={},
        trade={},
        borrow={},
        events={},
        edinet={},
        market={},
        presence={},
    )
    row = {
        "as_of_date": 20260312,
        "code": "1301",
        "ranking_score_long": 250.0,
        "ranking_score_short": 250.0,
        "close_price": 100.0,
        "close_vs_ma20": 0.08,
        "ret_20_past": 0.04,
        "atr_ratio": 0.01,
        "volume_ratio": 1.0,
        "box_state": "box_breakout",
        "ppp_state": "none",
        "abc_state": "none",
    }

    long_row = forecast_surface_module._build_surface_row(
        row,
        side="long",
        source_context=source_context,
        market_context={},
        publish_id="pub_test",
        freshness_state="fresh",
        learned_prediction={"direction_prob": 0.95, "expected_ret_20": -0.40},
    )
    short_row = forecast_surface_module._build_surface_row(
        row,
        side="short",
        source_context=source_context,
        market_context={},
        publish_id="pub_test",
        freshness_state="fresh",
        learned_prediction={"direction_prob": 0.95, "expected_ret_20": -0.40},
    )

    assert float(long_row["expected_ret_20"]) < 0.0
    assert float(short_row["expected_ret_20"]) > 0.0
    assert float(long_row["market_opportunity_score"]) == 0.0
    assert float(short_row["market_opportunity_score"]) == 0.0
    assert float(long_row["opportunity_score"]) == 0.0
    assert float(short_row["opportunity_score"]) == 0.0


def _seed_forecast_surface_source_db(source_db: str, as_of_date: int) -> None:
    conn = duckdb.connect(source_db, read_only=False)
    try:
        conn.execute(
            """
            CREATE TABLE signal_decision_daily (
                dt INTEGER,
                code TEXT,
                side TEXT,
                entry_qualified BOOLEAN,
                setup_type TEXT,
                reason_snapshot_json TEXT,
                score_snapshot_json TEXT,
                rank_snapshot_json TEXT,
                forward_return_5 DOUBLE,
                forward_return_20 DOUBLE,
                forward_return_30 DOUBLE,
                forward_return_60 DOUBLE,
                max_favorable_30 DOUBLE,
                max_adverse_30 DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE market_regime_daily (
                dt INTEGER,
                regime_id TEXT,
                breadth_above_ma20 DOUBLE,
                breadth_above_ma60 DOUBLE,
                advancers_ratio DOUBLE,
                index_close_vs_ma20 DOUBLE,
                index_close_vs_ma60 DOUBLE,
                market_atr_pct DOUBLE,
                sector_dispersion DOUBLE,
                regime_score DOUBLE,
                label_version TEXT,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO signal_decision_daily VALUES
            (?, '1301', 'buy', TRUE, 'breakout', '{"a":1}', '{"b":2}', '{"c":3}', 0.02, 0.08, 0.10, 0.12, 0.14, -0.05)
            """,
            [as_of_date],
        )
        conn.execute(
            """
            INSERT INTO market_regime_daily VALUES
            (?, 'risk_on', 0.68, 0.64, 0.55, 0.03, 0.04, 0.18, 0.11, 0.42, 'test', TIMESTAMP '2026-03-12 09:00:00')
            """,
            [as_of_date],
        )
    finally:
        conn.close()


def test_state_eval_decision_thresholds_are_shared_between_champion_and_challenger() -> None:
    assert _decision_from_score(0.63, strict=False) == DECISION_WAIT
    assert _decision_from_score(0.63, strict=True) == DECISION_WAIT
    assert _decision_from_score(0.65, strict=False) == DECISION_ENTER
    assert _decision_from_score(0.65, strict=True) == DECISION_ENTER
    assert _decision_from_score(0.44, strict=False) == DECISION_SKIP
    assert _decision_from_score(0.44, strict=True) == DECISION_SKIP
