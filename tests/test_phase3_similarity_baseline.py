from __future__ import annotations

import sys
from datetime import date, timedelta

import duckdb
from fastapi.testclient import TestClient

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.similarity.baseline import run_similarity_baseline
from tests.test_external_analysis_rolling_labels import _insert_export_meta


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_source_db(source_db: str) -> list[int]:
    conn = duckdb.connect(source_db)
    dates = _weekday_ints(date(2026, 1, 5), 80)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute(
            "CREATE TABLE feature_snapshot_daily (dt INTEGER, code TEXT, close DOUBLE, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE, cnt_20_above INTEGER, cnt_7_above INTEGER, day_count INTEGER, candle_flags TEXT)"
        )
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute("CREATE TABLE positions_live (symbol TEXT, spot_qty DOUBLE, margin_long_qty DOUBLE, margin_short_qty DOUBLE, buy_qty DOUBLE, sell_qty DOUBLE, opened_at TIMESTAMP, updated_at TIMESTAMP, has_issue BOOLEAN, issue_note TEXT)")
        conn.execute("CREATE TABLE position_rounds (round_id TEXT, symbol TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP, closed_reason TEXT)")
        specs = {
            "1301": lambda idx: 100.0 + (idx * 1.6),
            "1302": lambda idx: 100.0 + (idx * 0.25),
            "1303": lambda idx: 145.0 - (idx * 1.3),
            "1304": lambda idx: 110.0 - (idx * 0.05),
            "1305": lambda idx: 100.0 if idx < 25 else 118.0 + ((idx - 25) * 0.3),
        }
        for idx, trade_date in enumerate(dates):
            for code, fn in specs.items():
                close_price = fn(idx)
                high_price = close_price + (2.5 if code == "1305" and idx >= 25 else 1.5)
                low_price = close_price - 1.5
                volume = 1000 + (idx * 10)
                if code == "1305" and idx == 25:
                    volume = 4000
                ma20 = close_price - 2.0 if code in {"1301", "1302"} else close_price + 2.0
                if code == "1305" and idx < 24:
                    ma20 = close_price + 4.0
                if code == "1305" and idx >= 25:
                    ma20 = close_price - 2.0
                conn.execute(
                    "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [code, trade_date, close_price - 0.5, high_price, low_price, close_price, volume, "pan"],
                )
                conn.execute(
                    "INSERT INTO daily_ma VALUES (?, ?, ?, ?, ?)",
                    [code, trade_date, ma20 + 1.0, ma20, ma20 - 3.0],
                )
                conn.execute(
                    "INSERT INTO feature_snapshot_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        trade_date,
                        code,
                        close_price,
                        ma20 + 1.0,
                        ma20,
                        ma20 - 3.0,
                        3.0,
                        (close_price / ma20) - 1.0,
                        1.0,
                        18 if code in {"1301", "1302", "1305"} else 4,
                        6 if code in {"1301", "1302", "1305"} else 1,
                        20,
                        "similarity",
                    ],
                )
        conn.execute("INSERT INTO monthly_bars VALUES ('1301', 202603, 90, 150, 88, 130, 10000)")
    finally:
        conn.close()
    return dates


def _run_pipeline(monkeypatch, source_db: str, export_db: str, label_db: str, result_db: str, similarity_db: str, publish_id: str, as_of_date: int) -> None:
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    commands = [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-similarity-db", "--db-path", str(similarity_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
        ["external_analysis", "anchor-window-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
        [
            "external_analysis",
            "candidate-baseline-run",
            "--export-db-path",
            str(export_db),
            "--label-db-path",
            str(label_db),
            "--result-db-path",
            str(result_db),
            "--similarity-db-path",
            str(similarity_db),
            "--as-of-date",
            str(as_of_date),
            "--publish-id",
            publish_id,
        ],
        [
            "external_analysis",
            "similarity-baseline-run",
            "--export-db-path",
            str(export_db),
            "--label-db-path",
            str(label_db),
            "--result-db-path",
            str(result_db),
            "--similarity-db-path",
            str(similarity_db),
            "--as-of-date",
            str(as_of_date),
            "--publish-id",
            publish_id,
        ],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0


def test_similarity_case_library_separates_success_failure_and_big_drop(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _seed_source_db(str(source_db))
    _run_pipeline(
        monkeypatch,
        str(source_db),
        str(export_db),
        str(label_db),
        str(result_db),
        str(similarity_db),
        "pub_2026-03-13_20260313T010000Z_01",
        dates[50],
    )

    conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        case_types = conn.execute(
            "SELECT case_type, COUNT(*) FROM case_library GROUP BY case_type ORDER BY case_type"
        ).fetchall()
        sources = conn.execute(
            "SELECT query_source, COUNT(*) FROM case_library GROUP BY query_source ORDER BY query_source"
        ).fetchall()
        setup_families = conn.execute(
            "SELECT setup_family, COUNT(*) FROM case_library GROUP BY setup_family ORDER BY setup_family"
        ).fetchall()
        trade_sides = conn.execute(
            "SELECT trade_side, COUNT(*) FROM case_library GROUP BY trade_side ORDER BY trade_side"
        ).fetchall()
        breakout_count = conn.execute(
            "SELECT COUNT(*) FROM case_library WHERE setup_family = 'range_break_pre_move' AND break_direction IN ('up', 'down')"
        ).fetchone()
    finally:
        conn.close()
    by_type = dict(case_types)
    by_source = dict(sources)
    by_setup = dict(setup_families)
    by_side = dict(trade_sides)
    assert by_type["failed_setup"] > 0
    assert by_type["pre_big_down"] > 0
    assert by_type["pre_big_up"] > 0
    assert by_source["anchor_window_query"] > 0
    assert by_source["daily_window_query"] > 0
    assert by_setup["big_loss_pre_move"] > 0
    assert by_setup["big_win_pre_move"] > 0
    assert by_setup["range_break_pre_move"] > 0
    assert by_side["long"] > 0
    assert by_side["short"] > 0
    assert int(breakout_count[0]) > 0


def test_similarity_publish_and_bridge_do_not_break_candidate_api(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _seed_source_db(str(source_db))
    publish_id = "pub_2026-03-13_20260313T020000Z_01"
    _run_pipeline(
        monkeypatch,
        str(source_db),
        str(export_db),
        str(label_db),
        str(result_db),
        str(similarity_db),
        publish_id,
        dates[50],
    )

    import app.main as main_module

    class _NoopThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self) -> None:
            return None

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
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})
    similar_paths_payload = similar_cases.json()
    case_id = similar_paths_payload["rows"][0]["case_id"]
    similar_paths = client.get("/api/analysis-bridge/similar-case-paths", params={"code": "1301", "case_id": case_id})

    assert candidates.status_code == 200
    assert candidates.json()["degraded"] is False
    assert len(candidates.json()["rows"]) > 0
    assert regime.status_code == 200
    assert regime.json()["degraded"] is False

    assert similar_cases.status_code == 200
    cases_payload = similar_cases.json()
    assert cases_payload["degraded"] is False
    assert cases_payload["publish_id"] == publish_id
    assert len(cases_payload["rows"]) > 0
    assert all("vector_json" not in row for row in cases_payload["rows"])
    assert any(row["case_type"] == "pre_big_down" for row in cases_payload["rows"]) or any(
        row["case_type"] == "failed_setup" for row in cases_payload["rows"]
    )

    assert similar_paths.status_code == 200
    paths_payload = similar_paths.json()
    assert paths_payload["degraded"] is False
    assert paths_payload["publish_id"] == publish_id
    assert len(paths_payload["rows"]) > 0


def test_similarity_failure_does_not_break_existing_candidate_publish(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _seed_source_db(str(source_db))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    publish_id = "pub_2026-03-13_20260313T030000Z_01"
    commands = [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-similarity-db", "--db-path", str(similarity_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
        ["external_analysis", "anchor-window-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
        [
            "external_analysis",
            "candidate-baseline-run",
            "--export-db-path",
            str(export_db),
            "--label-db-path",
            str(label_db),
            "--result-db-path",
            str(result_db),
            "--similarity-db-path",
            str(similarity_db),
            "--as-of-date",
            str(dates[50]),
            "--publish-id",
            publish_id,
        ],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0

    import external_analysis.similarity.baseline as similarity_module

    monkeypatch.setattr(similarity_module, "build_case_library", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("forced_similarity_failure")))

    try:
        run_similarity_baseline(
            export_db_path=str(export_db),
            label_db_path=str(label_db),
            result_db_path=str(result_db),
            similarity_db_path=str(similarity_db),
            as_of_date=dates[50],
            publish_id=publish_id,
        )
    except RuntimeError:
        pass

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        pointer = conn.execute("SELECT publish_id FROM publish_pointer WHERE pointer_name='latest_successful'").fetchone()
        candidate_count = conn.execute("SELECT COUNT(*) FROM candidate_daily WHERE publish_id = ?", [publish_id]).fetchone()
        similar_count = conn.execute("SELECT COUNT(*) FROM similar_cases_daily WHERE publish_id = ?", [publish_id]).fetchone()
    finally:
        conn.close()
    assert pointer == (publish_id,)
    assert int(candidate_count[0]) > 0
    assert int(similar_count[0]) == 0


def test_similarity_case_library_updates_only_dirty_codes(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _seed_source_db(str(source_db))
    _run_pipeline(
        monkeypatch,
        str(source_db),
        str(export_db),
        str(label_db),
        str(result_db),
        str(similarity_db),
        "pub_2026-03-13_20260313T040000Z_01",
        dates[50],
    )
    _insert_export_meta(str(export_db), run_id="run-1", source_signature="sig-1", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 400, "updated": 0, "deleted": 0}, "indicator_daily_export": {"inserted": 400, "updated": 0, "deleted": 0}})
    run_similarity_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[50],
        publish_id="pub_2026-03-13_20260313T040000Z_01",
    )
    conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        untouched_before = conn.execute("SELECT generation_run_id FROM case_library WHERE code = '1302' ORDER BY case_id LIMIT 1").fetchone()[0]
        dirty_before = conn.execute("SELECT generation_run_id FROM case_library WHERE code = '1301' ORDER BY case_id LIMIT 1").fetchone()[0]
    finally:
        conn.close()

    export_conn = duckdb.connect(str(export_db), read_only=False)
    try:
        export_conn.execute(
            "UPDATE bars_daily_export SET c = c + 5, row_hash = 'changed-1301', export_run_id = 'run-2' WHERE code = '1301' AND trade_date = ?",
            [dates[50]],
        )
        export_conn.execute(
            "UPDATE indicator_daily_export SET ma20 = ma20 + 2, row_hash = 'changed-ind-1301', export_run_id = 'run-2' WHERE code = '1301' AND trade_date = ?",
            [dates[50]],
        )
    finally:
        export_conn.close()
    _insert_export_meta(str(export_db), run_id="run-2", source_signature="sig-2", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 0, "updated": 1, "deleted": 0}, "indicator_daily_export": {"inserted": 0, "updated": 1, "deleted": 0}})

    payload = run_similarity_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[50],
        publish_id="pub_2026-03-13_20260313T040000Z_01",
    )

    conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        untouched_after = conn.execute("SELECT generation_run_id FROM case_library WHERE code = '1302' ORDER BY case_id LIMIT 1").fetchone()[0]
        dirty_after = conn.execute("SELECT generation_run_id FROM case_library WHERE code = '1301' ORDER BY case_id LIMIT 1").fetchone()[0]
    finally:
        conn.close()
    assert payload["case_library"]["cache_state"] == "partial_stale"
    assert untouched_after == untouched_before
    assert dirty_after != dirty_before
