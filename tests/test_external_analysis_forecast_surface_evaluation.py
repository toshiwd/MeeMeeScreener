from __future__ import annotations

import sys
from datetime import date, timedelta

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.labels.store import ensure_label_db
from external_analysis.models.forecast_surface_evaluation import evaluate_forecast_surface, summarize_forecast_surface_shadow_run
from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db


def _weekday_dates(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_label_db(path: str, dates: list[int]) -> None:
    ensure_label_db(path)
    conn = duckdb.connect(path, read_only=False)
    try:
        for horizon in (5, 10, 20):
            for as_of_date in dates:
                for code, sign in (("A1", 1.0), ("A2", 0.8), ("B1", -0.9), ("B2", -0.7)):
                    ret_h = sign * (0.05 + (dates.index(as_of_date) * 0.005))
                    mfe_h = abs(ret_h) + 0.03
                    mae_h = -(abs(ret_h) + 0.02)
                    conn.execute(
                        f"""
                        INSERT INTO label_daily_h{horizon} (
                            code, as_of_date, horizon_days, ret_h, mfe_h, mae_h, days_to_mfe_h,
                            days_to_stop_h, cross_section_count, rank_ret_h, top_1pct_h, top_3pct_h,
                            top_5pct_h, future_window_start_date, future_window_end_date, purge_end_date,
                            embargo_until_date, leakage_group_id, policy_version, generation_run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            code,
                            as_of_date,
                            horizon,
                            ret_h,
                            mfe_h,
                            mae_h,
                            2,
                            4,
                            4,
                            1 if sign > 0 else 4,
                            sign > 0,
                            sign > 0,
                            sign > 0,
                            as_of_date,
                            as_of_date + 7,
                            as_of_date + 5,
                            as_of_date + 3,
                            "g1",
                            "purged-walk-forward-v1",
                            "test-run",
                        ],
                    )
    finally:
        conn.close()


def _seed_result_db(path: str, dates: list[int]) -> list[str]:
    ensure_result_db(path)
    publish_ids: list[str] = []
    conn = duckdb.connect(path, read_only=False)
    try:
        for idx, as_of_date in enumerate(dates):
            publish_id = f"pub_{as_of_date}"
            publish_ids.append(publish_id)
            candidate_rows = [
                ("B1", "long", 1, 9.5),
                ("B2", "long", 2, 9.0),
                ("A1", "short", 1, 9.6),
                ("A2", "short", 2, 9.1),
                ("A1", "long", 3, 0.1),
                ("A2", "long", 4, 0.05),
                ("B1", "short", 3, 0.1),
                ("B2", "short", 4, 0.05),
            ]
            surface_rows = [
                ("A1", "long", "enter", 0.91, 0.05, 0.08, 0.10, 0.11, 12.0, 12.0),
                ("A2", "long", "enter", 0.89, 0.04, 0.07, 0.09, 0.10, 11.0, 11.0),
                ("B1", "long", "skip", 0.20, -0.01, 0.00, 0.00, -0.02, 2.0, 2.0),
                ("B2", "long", "skip", 0.15, -0.02, -0.01, -0.01, -0.03, 1.5, 1.5),
                ("B1", "short", "enter", 0.93, 0.06, 0.09, 0.12, 0.13, 12.5, 12.5),
                ("B2", "short", "enter", 0.89, 0.05, 0.08, 0.11, 0.12, 11.5, 11.5),
                ("A1", "short", "skip", 0.18, -0.01, 0.00, 0.00, -0.02, 1.0, 1.0),
                ("A2", "short", "skip", 0.14, -0.02, -0.01, -0.01, -0.03, 0.5, 0.5),
            ]
            regime_tag = ("risk_on", "neutral", "risk_off", "risk_on")[idx]
            conn.execute(
                """
                INSERT INTO regime_daily (
                    publish_id, as_of_date, regime_tag, regime_score, breadth_score, volatility_state
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?)
                """,
                [publish_id, f"{str(as_of_date)[0:4]}-{str(as_of_date)[4:6]}-{str(as_of_date)[6:8]}", regime_tag, 0.3, 0.2, "normal"],
            )
            for code, side, rank_position, candidate_score in candidate_rows:
                conn.execute(
                    """
                    INSERT INTO candidate_daily (
                        publish_id, as_of_date, code, side, rank_position, candidate_score, expected_horizon_days,
                        primary_reason_codes, regime_tag, freshness_state
                    ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        publish_id,
                        f"{str(as_of_date)[0:4]}-{str(as_of_date)[4:6]}-{str(as_of_date)[6:8]}",
                        code,
                        side,
                        rank_position,
                        candidate_score if side == "long" else candidate_score,
                        20,
                        "[\"TEST\"]",
                        regime_tag,
                        "fresh",
                    ],
                )
            for code, side, action_state, direction_prob, ret5, ret10, ret20, mfe20, mae20, opportunity_score in surface_rows:
                conn.execute(
                    """
                    INSERT INTO forecast_surface_daily (
                        publish_id, as_of_date, code, side, action_state, direction_prob,
                        expected_ret_5, expected_ret_10, expected_ret_20, expected_mfe_20,
                        expected_mae_20, invalidation_price, setup_tags, reason_codes,
                        opportunity_score, freshness_state, created_at
                    ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        publish_id,
                        f"{str(as_of_date)[0:4]}-{str(as_of_date)[4:6]}-{str(as_of_date)[6:8]}",
                        code,
                        side,
                        action_state,
                        direction_prob,
                        ret5,
                        ret10,
                        ret20,
                        mfe20,
                        mae20,
                        100.0,
                        "[\"setup\"]",
                        "[\"REASON\"]",
                        opportunity_score,
                        "fresh",
                    ],
                )
    finally:
        conn.close()

    publish_result(
        db_path=path,
        publish_id=publish_ids[-1],
        as_of_date=f"{str(dates[-1])[0:4]}-{str(dates[-1])[4:6]}-{str(dates[-1])[6:8]}",
        freshness_state="fresh",
        table_row_counts={},
        degrade_ready=True,
    )
    return publish_ids


def _seed_forecast_surface_runs(path: str, publish_ids: list[str], dates: list[int], *, universe_count: int = 4) -> None:
    conn = duckdb.connect(path, read_only=False)
    try:
        for publish_id, as_of_date in zip(publish_ids, dates, strict=True):
            conn.execute(
                """
                INSERT INTO forecast_surface_runs (
                    publish_id,
                    as_of_date,
                    model_version,
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
                    alerts_json,
                    created_at
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    publish_id,
                    f"{str(as_of_date)[0:4]}-{str(as_of_date)[4:6]}-{str(as_of_date)[6:8]}",
                    "test-model",
                    universe_count,
                    universe_count * 2,
                    universe_count * 2,
                    0,
                    1.0,
                    "test-feature-frame",
                    True,
                    True,
                    '{"long": 4, "short": 4}',
                    '{"enter": 4, "skip": 4}',
                    '{"tdnet_disclosures": false}',
                    '["source_absent:tdnet_disclosures"]',
                ],
            )
    finally:
        conn.close()


def _seed_signal_source_db(path: str, dates: list[int]) -> None:
    conn = duckdb.connect(path, read_only=False)
    try:
        conn.execute(
            """
            CREATE TABLE signal_decision_daily (
                dt INTEGER,
                code TEXT,
                side TEXT,
                entry_qualified BOOLEAN,
                forward_return_5 DOUBLE,
                forward_return_20 DOUBLE,
                forward_return_30 DOUBLE,
                forward_return_60 DOUBLE
            )
            """
        )
        for as_of_date in dates:
            conn.execute(
                """
                INSERT INTO signal_decision_daily VALUES
                (?, 'A1', 'buy', TRUE, 0.02, 0.05, 0.06, 0.07),
                (?, 'A2', 'buy', TRUE, 0.02, 0.04, 0.05, 0.06),
                (?, 'B1', 'sell', TRUE, -0.02, -0.05, -0.06, -0.07),
                (?, 'B2', 'sell', TRUE, -0.02, -0.04, -0.05, -0.06)
                """,
                [as_of_date, as_of_date, as_of_date, as_of_date],
            )
    finally:
        conn.close()


def test_forecast_surface_walk_forward_evaluation_passes_and_persists(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 4)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)

    conn = duckdb.connect(str(result_db), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO label_daily_h20 (
                code, as_of_date, horizon_days, ret_h, mfe_h, mae_h, days_to_mfe_h, days_to_stop_h,
                cross_section_count, rank_ret_h, top_1pct_h, top_3pct_h, top_5pct_h,
                future_window_start_date, future_window_end_date, purge_end_date, embargo_until_date,
                leakage_group_id, policy_version, generation_run_id
            ) VALUES
                ('Z9', ?, 20, -0.25, 0.05, -0.30, 1, 3, 5, 5, FALSE, FALSE, FALSE, ?, ?, ?, ?, 'g1', 'purged-walk-forward-v1', 'test-run')
            """,
            [dates[-1], dates[-1], dates[-1] + 7, dates[-1] + 5, dates[-1] + 3],
        )
        conn.execute(
            """
            INSERT INTO forecast_surface_daily (
                publish_id, as_of_date, code, side, action_state, direction_prob,
                expected_ret_5, expected_ret_10, expected_ret_20, expected_mfe_20,
                expected_mae_20, invalidation_price, setup_tags, reason_codes,
                opportunity_score, freshness_state, created_at
            ) VALUES (?, CAST(? AS DATE), 'Z9', 'long', 'skip', 0.05,
                -0.01, -0.02, -0.25, 0.05, -0.30, 100.0, '[]', '[]', 999.0, 'fresh', CURRENT_TIMESTAMP)
            """,
            [publish_ids[-1], f"{str(dates[-1])[0:4]}-{str(dates[-1])[4:6]}-{str(dates[-1])[6:8]}"],
        )
    finally:
        conn.close()

    payload = evaluate_forecast_surface(
        result_db_path=str(result_db),
        label_db_path=str(label_db),
        source_db_path=str(source_db),
        top_k=2,
        min_folds=3,
        min_daily_count=3,
        persist=True,
    )

    assert payload["ok"] is True
    assert payload["scope_type"] == "walk_forward"
    assert payload["readiness_pass"] is True
    assert payload["summary"]["fold_count"] == len(publish_ids)
    assert payload["summary"]["daily_count"] == len(dates)
    assert payload["summary"]["top_k_uplift"] is not None
    assert payload["summary"]["top_combined_mean_ret20_net"] > 0
    assert payload["summary"]["candidate_combined_mean_ret20_net"] is not None
    assert payload["summary"]["top_combined_mean_ret20_net"] > payload["summary"]["candidate_combined_mean_ret20_net"]
    assert payload["summary"]["primary_gate_reason"] == "gate_passed"
    assert payload["summary"]["gate_failures_json"] == []
    assert payload["summary"]["calibration_method_long"] in {"isotonic", "platt", "none"}
    assert payload["summary"]["calibration_method_short"] in {"isotonic", "platt", "none"}

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        run_count = conn.execute(
            "SELECT COUNT(*) FROM forecast_surface_evaluation_runs WHERE scope_type = 'walk_forward'"
        ).fetchone()[0]
        fold_count = conn.execute(
            "SELECT COUNT(*) FROM forecast_surface_evaluation_folds WHERE scope_type = 'walk_forward'"
        ).fetchone()[0]
        latest = conn.execute(
            """
            SELECT readiness_pass, gate_reason, primary_gate_reason, gate_failures_json, ready_streak, recent_ready_count_20,
                   calibration_method_long, calibration_method_short, top_long_mean_ret20_net, top_short_mean_ret20_net
            FROM forecast_surface_evaluation_runs
            WHERE scope_type = 'walk_forward'
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert int(run_count) == 1
    assert int(fold_count) > 0
    assert latest is not None
    assert bool(latest[0]) is True
    assert str(latest[1]) == "gate_passed"
    assert str(latest[2]) == "gate_passed"
    assert str(latest[3]) == "[]"
    assert int(latest[4]) == 1
    assert int(latest[5]) == 1
    assert str(latest[6]) in {"isotonic", "platt", "none"}
    assert str(latest[7]) in {"isotonic", "platt", "none"}
    assert float(latest[8]) > 0
    assert float(latest[9]) > 0


def test_forecast_surface_walk_forward_evaluation_filters_publish_prefix(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 4)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)

    conn = duckdb.connect(str(result_db), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO forecast_surface_daily (
                publish_id, as_of_date, code, side, action_state, direction_prob,
                expected_ret_5, expected_ret_10, expected_ret_20, expected_mfe_20,
                expected_mae_20, invalidation_price, setup_tags, reason_codes,
                opportunity_score, freshness_state, created_at
            ) VALUES ('other_shadow', CAST('2026-03-09' AS DATE), 'A1', 'long', 'enter', 0.9,
                0.01, 0.02, 0.03, 0.04, -0.01, 100.0, '[]', '[]', 99.0, 'fresh', CURRENT_TIMESTAMP)
            """
        )
    finally:
        conn.close()

    payload = evaluate_forecast_surface(
        result_db_path=str(result_db),
        label_db_path=str(label_db),
        source_db_path=str(source_db),
        publish_id_prefix="pub_",
        top_k=2,
        min_folds=3,
        min_daily_count=3,
        persist=False,
    )

    assert payload["ok"] is True
    assert payload["scope_type"] == "walk_forward"
    assert payload["summary"]["fold_count"] == len(publish_ids)
    assert {row["publish_id"] for row in payload["folds"]} == set(publish_ids)


def test_forecast_surface_publish_evaluation_materializes_publish_streak(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 4)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)

    for publish_id in publish_ids[:-1]:
        payload = evaluate_forecast_surface(
            result_db_path=str(result_db),
            label_db_path=str(label_db),
            source_db_path=str(source_db),
            publish_id=publish_id,
            top_k=2,
            min_folds=3,
            min_daily_count=3,
            persist=True,
        )
        assert payload["ok"] is True
        assert payload["scope_type"] == "publish"

    latest_payload = evaluate_forecast_surface(
        result_db_path=str(result_db),
        label_db_path=str(label_db),
        source_db_path=str(source_db),
        publish_id=publish_ids[-1],
        top_k=2,
        min_folds=3,
        min_daily_count=3,
        persist=True,
    )

    assert latest_payload["ok"] is True
    assert latest_payload["summary"]["ready_streak"] >= 1
    assert latest_payload["summary"]["recent_ready_count_20"] >= 1
    assert latest_payload["summary"]["top_long_mean_ret20_net"] > 0

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        latest = conn.execute(
            """
            SELECT ready_streak, recent_ready_count_20
            FROM forecast_surface_evaluation_runs
            WHERE scope_type = 'publish' AND publish_id = ?
            """,
            [publish_ids[-1]],
        ).fetchone()
    finally:
        conn.close()

    assert latest is not None
    assert int(latest[0]) >= 1
    assert int(latest[1]) >= 1


def test_forecast_surface_evaluate_cli_smoke(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 4)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)

    argv = [
        "external_analysis",
        "forecast-surface-evaluate-run",
        "--result-db-path",
        str(result_db),
        "--label-db-path",
        str(label_db),
        "--source-db-path",
        str(source_db),
        "--publish-id-prefix",
        "pub_",
        "--top-k",
        "2",
        "--min-folds",
        "3",
        "--min-daily-count",
        "3",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0


def test_forecast_surface_shadow_status_passes_when_recent_runs_are_ready(tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 3)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)
    _seed_forecast_surface_runs(str(result_db), publish_ids, dates, universe_count=4)

    for publish_id in publish_ids:
        payload = evaluate_forecast_surface(
            result_db_path=str(result_db),
            label_db_path=str(label_db),
            source_db_path=str(source_db),
            publish_id=publish_id,
            top_k=2,
            persist=True,
        )
        assert payload["readiness_pass"] is True
    walk_forward_payload = evaluate_forecast_surface(
        result_db_path=str(result_db),
        label_db_path=str(label_db),
        source_db_path=str(source_db),
        publish_id_prefix="pub_",
        top_k=2,
        min_folds=3,
        min_daily_count=3,
        persist=True,
    )
    assert walk_forward_payload["readiness_pass"] is True

    status = summarize_forecast_surface_shadow_run(
        result_db_path=str(result_db),
        publish_id_prefix="pub_",
        min_days=3,
        min_universe_code_count=4,
    )

    assert status["ok"] is True
    assert status["acceptance_pass"] is True
    assert status["primary_reason"] == "gate_passed"
    assert status["observed_days"] == 3
    assert status["coverage_pass_count"] == 3
    assert status["universe_pass_count"] == 3
    assert status["gate_pass_count"] == 3
    assert status["walk_forward_gate_pass"] is True
    assert status["walk_forward"]["primary_gate_reason"] == "gate_passed"
    assert status["failures"] == []


def test_forecast_surface_shadow_status_rejects_full_coverage_tiny_universe(tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 3)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)
    _seed_forecast_surface_runs(str(result_db), publish_ids, dates, universe_count=4)

    for publish_id in publish_ids:
        evaluate_forecast_surface(
            result_db_path=str(result_db),
            label_db_path=str(label_db),
            source_db_path=str(source_db),
            publish_id=publish_id,
            top_k=2,
            persist=True,
        )
    evaluate_forecast_surface(
        result_db_path=str(result_db),
        label_db_path=str(label_db),
        source_db_path=str(source_db),
        publish_id_prefix="pub_",
        top_k=2,
        min_folds=3,
        min_daily_count=3,
        persist=True,
    )

    conn = duckdb.connect(str(result_db), read_only=False)
    try:
        conn.execute(
            """
            UPDATE forecast_surface_runs
            SET universe_code_count = 1,
                expected_row_count = 2,
                actual_row_count = 2,
                missing_row_count = 0,
                coverage_ratio = 1.0
            WHERE publish_id = ?
            """,
            [publish_ids[-1]],
        )
    finally:
        conn.close()

    status = summarize_forecast_surface_shadow_run(
        result_db_path=str(result_db),
        publish_id_prefix="pub_",
        min_days=3,
        min_universe_code_count=4,
    )

    assert status["ok"] is True
    assert status["acceptance_pass"] is False
    assert status["primary_reason"] == "universe_too_small"
    assert status["coverage_pass_count"] == 3
    assert status["universe_pass_count"] == 2
    assert status["walk_forward_gate_pass"] is True
    assert any(failure["reason"] == "universe_too_small" for failure in status["failures"])


def test_forecast_surface_shadow_status_cli_smoke(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 3)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)
    _seed_forecast_surface_runs(str(result_db), publish_ids, dates, universe_count=4)
    for publish_id in publish_ids:
        evaluate_forecast_surface(
            result_db_path=str(result_db),
            label_db_path=str(label_db),
            source_db_path=str(source_db),
            publish_id=publish_id,
            top_k=2,
            persist=True,
        )
    evaluate_forecast_surface(
        result_db_path=str(result_db),
        label_db_path=str(label_db),
        source_db_path=str(source_db),
        publish_id_prefix="pub_",
        top_k=2,
        min_folds=3,
        min_daily_count=3,
        persist=True,
    )

    argv = [
        "external_analysis",
        "forecast-surface-shadow-status-run",
        "--result-db-path",
        str(result_db),
        "--publish-id-prefix",
        "pub_",
        "--min-days",
        "3",
        "--min-universe-code-count",
        "4",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0


def test_forecast_surface_shadow_status_ignores_unrelated_walk_forward_gate_pass(tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    label_db = tmp_path / "label.duckdb"
    source_db = tmp_path / "source.duckdb"
    dates = _weekday_dates(date(2026, 3, 2), 3)
    _seed_label_db(str(label_db), dates)
    _seed_signal_source_db(str(source_db), dates)
    publish_ids = _seed_result_db(str(result_db), dates)
    _seed_forecast_surface_runs(str(result_db), publish_ids, dates, universe_count=4)
    for publish_id in publish_ids:
        evaluate_forecast_surface(
            result_db_path=str(result_db),
            label_db_path=str(label_db),
            source_db_path=str(source_db),
            publish_id=publish_id,
            top_k=2,
            persist=True,
        )

    conn = duckdb.connect(str(result_db), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO forecast_surface_evaluation_runs (
                run_id,
                scope_type,
                publish_id,
                as_of_date,
                model_version,
                top_k,
                fold_count,
                daily_count,
                horizon_count,
                top_long_mean_ret20_net,
                top_short_mean_ret20_net,
                top_combined_mean_ret20_net,
                candidate_combined_mean_ret20_net,
                direction_brier_long,
                direction_brier_short,
                calibration_gap_long,
                calibration_gap_short,
                top_k_uplift,
                worst_regime_combined_mean_ret20_net,
                min_folds,
                min_daily_count,
                primary_gate_reason,
                gate_failures_json,
                calibration_method_long,
                calibration_method_short,
                ready_streak,
                recent_ready_count_20,
                regime_breakdown_json,
                fold_metrics_json,
                readiness_pass,
                gate_reason,
                created_at
            ) VALUES (
                'walk_forward_other',
                'walk_forward',
                NULL,
                NULL,
                'test-eval',
                2,
                3,
                3,
                3,
                0.05,
                0.06,
                0.055,
                0.01,
                0.20,
                0.20,
                0.02,
                0.02,
                0.045,
                0.01,
                3,
                3,
                'gate_passed',
                '[]',
                'isotonic',
                'isotonic',
                1,
                3,
                '{}',
                '[{"publish_id":"other_20260302"}]',
                TRUE,
                'gate_passed',
                CURRENT_TIMESTAMP
            )
            """
        )
    finally:
        conn.close()

    status = summarize_forecast_surface_shadow_run(
        result_db_path=str(result_db),
        publish_id_prefix="pub_",
        min_days=3,
        min_universe_code_count=4,
    )

    assert status["acceptance_pass"] is False
    assert status["primary_reason"] == "walk_forward_evaluation_missing"
    assert status["walk_forward"] is None
