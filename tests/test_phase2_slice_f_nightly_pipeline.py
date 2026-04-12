from __future__ import annotations

import shutil
import sys
import json
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import duckdb

from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot
import external_analysis.__main__ as external_analysis_main_module
from external_analysis.__main__ import main as external_analysis_main
from external_analysis.models import candidate_baseline as candidate_baseline_module
from external_analysis.models import forecast_surface as forecast_surface_module
from external_analysis.models import forecast_surface_evaluation as forecast_surface_evaluation_module
from external_analysis.models.candidate_baseline import run_candidate_baseline
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.runtime import nightly_pipeline as nightly_pipeline_module
from external_analysis.runtime.nightly_pipeline import run_nightly_candidate_pipeline


ROOT = Path(__file__).resolve().parents[1]
_PHASE1_CACHE_ROOT = (ROOT / ".tmp-tests" / "phase1_seed" / uuid4().hex).resolve()
_PHASE1_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
_PHASE1_CACHE: dict[str, tuple[list[int], dict[str, Path]]] = {}


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_source_db(source_db: str) -> list[int]:
    Path(source_db).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(source_db)
    dates = _weekday_ints(date(2026, 1, 5), 70)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute(
            "CREATE TABLE feature_snapshot_daily (dt INTEGER, code TEXT, close DOUBLE, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE, cnt_20_above INTEGER, cnt_7_above INTEGER, day_count INTEGER, candle_flags TEXT)"
        )
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute("CREATE TABLE positions_live (symbol TEXT, spot_qty DOUBLE, margin_long_qty DOUBLE, margin_short_qty DOUBLE, buy_qty DOUBLE, sell_qty DOUBLE, opened_at TIMESTAMP, updated_at TIMESTAMP, has_issue BOOLEAN, issue_note TEXT)")
        conn.execute("CREATE TABLE position_rounds (round_id TEXT, symbol TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP, closed_reason TEXT)")
        for idx, trade_date in enumerate(dates):
            for code, slope in (("1301", 1.2), ("1302", 0.7), ("1303", -1.0), ("1304", -0.6)):
                base = 100.0 if code != "1303" else 140.0
                close_price = base + (idx * slope)
                conn.execute(
                    "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [code, trade_date, close_price - 0.5, close_price + 1.5, close_price - 1.5, close_price, 1000 + idx, "pan"],
                )
                ma20 = close_price - 2.0 if slope > 0 else close_price + 2.0
                conn.execute(
                    "INSERT INTO daily_ma VALUES (?, ?, ?, ?, ?)",
                    [code, trade_date, ma20 + 1.0, ma20, ma20 - 3.0],
                )
                conn.execute(
                    "INSERT INTO feature_snapshot_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [trade_date, code, close_price, ma20 + 1.0, ma20, ma20 - 3.0, 3.0, (close_price / ma20) - 1.0, 1.0, 18 if slope > 0 else 4, 6 if slope > 0 else 1, 20, "nightly"],
                )
        conn.execute("INSERT INTO monthly_bars VALUES ('1301', 202603, 90, 120, 88, 110, 10000)")
    finally:
        conn.close()
    return dates


def _run_phase1_inputs(monkeypatch, source_db: str, export_db: str, label_db: str, result_db: str, ops_db: str) -> list[int]:
    source_path = Path(source_db)
    export_path = Path(export_db)
    label_path = Path(label_db)
    result_path = Path(result_db)
    ops_path = Path(ops_db)
    for path in (source_path, export_path, label_path, result_path, ops_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    dates = _seed_source_db(str(source_path))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_path))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_path))
    commands = [
        ["external_analysis", "init-result-db", "--db-path", str(result_path)],
        ["external_analysis", "init-export-db", "--db-path", str(export_path)],
        ["external_analysis", "init-label-db", "--db-path", str(label_path)],
        ["external_analysis", "init-ops-db", "--db-path", str(ops_path)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_path), "--export-db-path", str(export_path)],
        ["external_analysis", "label-build", "--export-db-path", str(export_path), "--label-db-path", str(label_path)],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0
    return dates


def test_nightly_candidate_metrics_is_idempotent_for_same_publish_id(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    payload_1 = run_candidate_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[45],
        publish_id="pub_2026-03-12_20260312T235000Z_01",
    )
    payload_2 = run_candidate_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[45],
        publish_id="pub_2026-03-12_20260312T235000Z_01",
    )

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        metric_count = conn.execute(
            "SELECT COUNT(*) FROM nightly_candidate_metrics WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235000Z_01"],
        ).fetchone()
    finally:
        conn.close()
    assert payload_1["metrics_saved"] is True
    assert payload_2["metrics_saved"] is True
    assert int(metric_count[0]) == 1


def test_phase2_slice_f_smoke_runs_nightly_pipeline_end_to_end(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    argv = [
        "external_analysis",
        "nightly-candidate-run",
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
        "--as-of-date",
        str(dates[45]),
        "--publish-id",
        "pub_2026-03-12_20260312T235500Z_01",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0

    snapshot = get_analysis_bridge_snapshot()
    result_conn = duckdb.connect(str(result_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        metric_row = result_conn.execute(
            "SELECT publish_id, candidate_count_long, candidate_count_short FROM nightly_candidate_metrics WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235500Z_01"],
        ).fetchone()
        forecast_row = result_conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT code), COUNT(DISTINCT side) FROM forecast_surface_daily WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235500Z_01"],
        ).fetchone()
        evaluation_row = result_conn.execute(
            "SELECT COUNT(*) FROM forecast_surface_evaluation_runs WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235500Z_01"],
        ).fetchone()
        job_row = ops_conn.execute(
            "SELECT status, publish_id FROM external_job_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        result_conn.close()
        ops_conn.close()
    assert snapshot["degraded"] is False
    assert snapshot["publish"]["publish_id"] == "pub_2026-03-12_20260312T235500Z_01"
    assert metric_row is not None
    assert int(metric_row[1]) > 0
    assert int(metric_row[2]) > 0
    assert forecast_row is not None
    assert int(forecast_row[0]) > 0
    assert int(forecast_row[2]) == 2
    assert evaluation_row is not None
    assert int(evaluation_row[0]) == 1
    assert job_row == ("success", "pub_2026-03-12_20260312T235500Z_01")


def test_nightly_pipeline_quarantines_metrics_failure_without_breaking_publish(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))
    ensure_ops_db(str(ops_db))

    def _raise_metrics_failure(*, result_db_path, metrics_row):
        raise RuntimeError("forced_metrics_failure")

    monkeypatch.setattr(candidate_baseline_module, "_persist_nightly_metrics", _raise_metrics_failure)

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235900Z_01",
    )

    snapshot = get_analysis_bridge_snapshot()
    result_conn = duckdb.connect(str(result_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        candidate_count = result_conn.execute(
            "SELECT COUNT(*) FROM candidate_daily WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235900Z_01"],
        ).fetchone()
        metric_count = result_conn.execute(
            "SELECT COUNT(*) FROM nightly_candidate_metrics WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235900Z_01"],
        ).fetchone()
        run_row = ops_conn.execute(
            "SELECT status, error_class FROM external_job_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        quarantine_row = ops_conn.execute(
            "SELECT reason, publish_id FROM external_job_quarantine ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        result_conn.close()
        ops_conn.close()

    assert payload["status"] == "published_with_metrics_failure"
    assert snapshot["degraded"] is False
    assert snapshot["publish"]["publish_id"] == "pub_2026-03-12_20260312T235900Z_01"
    assert int(candidate_count[0]) > 0
    assert int(metric_count[0]) == 0
    assert run_row == ("published_with_metrics_failure", "RuntimeError")
    assert quarantine_row == ("nightly_metrics_persist_failed", "pub_2026-03-12_20260312T235900Z_01")


def test_nightly_pipeline_quarantines_missing_forecast_surface_without_marking_success(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))
    ensure_ops_db(str(ops_db))

    def _missing_surface(**_kwargs):
        return {"saved": False, "row_count": 0, "source_context_presence": {}, "side_counts": {"long": 0, "short": 0}, "action_counts": {"enter": 0, "wait": 0, "skip": 0}}

    monkeypatch.setattr(forecast_surface_module, "persist_forecast_surface_daily", _missing_surface)

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T236000Z_01",
    )

    result_conn = duckdb.connect(str(result_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        surface_count = result_conn.execute(
            "SELECT COUNT(*) FROM forecast_surface_daily WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T236000Z_01"],
        ).fetchone()
        run_row = ops_conn.execute(
            "SELECT status, error_class FROM external_job_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        quarantine_row = ops_conn.execute(
            "SELECT reason, publish_id FROM external_job_quarantine ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        result_conn.close()
        ops_conn.close()

    assert payload["status"] == "published_with_surface_failure"
    assert run_row == ("published_with_surface_failure", None)
    assert quarantine_row == ("forecast_surface_not_persisted", "pub_2026-03-12_20260312T236000Z_01")
    assert int(surface_count[0]) == 0


def test_nightly_pipeline_quarantines_source_universe_preflight_failure_without_running_downstream(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER)")
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
            ('1301', 20260313),
            ('1302', 20260313),
            ('1303', 20260313),
            ('1304', 20260313),
            ('1301', 20260314)
            """
        )
    finally:
        conn.close()

    calls = {"export": 0, "labels": 0, "baseline": 0}
    recorded: dict[str, object] = {}

    monkeypatch.setattr(
        nightly_pipeline_module,
        "run_diff_export",
        lambda **_kwargs: calls.__setitem__("export", calls["export"] + 1),
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "build_rolling_labels",
        lambda **_kwargs: calls.__setitem__("labels", calls["labels"] + 1),
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "run_candidate_baseline",
        lambda **_kwargs: calls.__setitem__("baseline", calls["baseline"] + 1),
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "insert_quarantine_record",
        lambda **kwargs: recorded.setdefault("quarantine", kwargs),
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "upsert_job_run",
        lambda **kwargs: recorded.setdefault("run", []).append(kwargs),
    )

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        as_of_date="20260314",
        publish_id="pub_2026-03-14_preflight",
        snapshot_source=False,
        min_universe_code_count=3,
    )

    assert payload["ok"] is False
    assert payload["status"] == "preflight_failed"
    assert payload["quarantine_reason"] == "source_universe_too_small"
    assert payload["source_readiness"]["observed_code_count"] == 1
    assert calls == {"export": 0, "labels": 0, "baseline": 0}
    assert recorded["quarantine"]["reason"] == "source_universe_too_small"
    assert recorded["quarantine"]["publish_id"] == "pub_2026-03-14_preflight"
    assert recorded["run"][-1]["status"] == "preflight_failed"
    assert recorded["run"][-1]["publish_id"] == "pub_2026-03-14_preflight"


def test_nightly_pipeline_quarantines_missing_forecast_surface_evaluation_without_marking_success(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))
    ensure_ops_db(str(ops_db))

    def _missing_evaluation(**_kwargs):
        return None

    monkeypatch.setattr(forecast_surface_evaluation_module, "evaluate_forecast_surface", _missing_evaluation)

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T236100Z_01",
    )

    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        run_row = ops_conn.execute(
            "SELECT status, error_class FROM external_job_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        quarantine_row = ops_conn.execute(
            "SELECT reason, publish_id FROM external_job_quarantine ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        ops_conn.close()

    assert payload["status"] == "published_with_surface_failure"
    assert run_row == ("published_with_surface_failure", None)
    assert quarantine_row == ("forecast_surface_evaluation_missing", "pub_2026-03-12_20260312T236100Z_01")


def test_nightly_pipeline_fails_preflight_when_source_universe_is_too_small(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    source_conn = duckdb.connect(str(source_db), read_only=False)
    try:
        source_conn.execute(
            """
            CREATE TABLE feature_snapshot_daily (
                dt INTEGER,
                code TEXT,
                close DOUBLE,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                atr14 DOUBLE,
                diff20_pct DOUBLE,
                cnt_20_above INTEGER,
                cnt_7_above INTEGER,
                available_at INTEGER
            )
            """
        )
        source_conn.execute(
            """
            INSERT INTO feature_snapshot_daily VALUES
            (20260312, '1301', 100.0, 99.0, 98.0, 97.0, 1.0, 0.01, 10, 5, 20260312)
            """
        )
    finally:
        source_conn.close()
    captured: dict[str, object] = {}
    monkeypatch.setattr(nightly_pipeline_module, "upsert_job_run", lambda **kwargs: captured.setdefault("job_run", kwargs))
    monkeypatch.setattr(nightly_pipeline_module, "insert_quarantine_record", lambda **kwargs: captured.setdefault("quarantine", kwargs))
    monkeypatch.setattr(
        nightly_pipeline_module,
        "run_diff_export",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("export should not run on preflight failure")),
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "build_rolling_labels",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("labels should not run on preflight failure")),
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "run_candidate_baseline",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("baseline should not run on preflight failure")),
    )

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        as_of_date="20260312",
        publish_id="pub_2026-03-12_20260312T236200Z_01",
        min_universe_code_count=4,
        snapshot_source=False,
    )

    assert payload["ok"] is False
    assert payload["status"] == "preflight_failed"
    assert payload["quarantine_reason"] == "source_universe_too_small"
    assert payload["baseline"] is None
    assert payload["source_readiness"]["observed_code_count"] == 1
    assert captured["quarantine"]["reason"] == "source_universe_too_small"
    assert captured["quarantine"]["publish_id"] == "pub_2026-03-12_20260312T236200Z_01"


def test_nightly_pipeline_throttled_mode_reduces_candidate_limit(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235910Z_01",
        load_control={"mode": "throttled", "reason": "meemee_foreground_active"},
    )

    assert payload["ok"] is True
    assert payload["runtime_budget"]["candidate_limit_per_side"] == 8
    assert payload["baseline"]["candidate_limit_per_side"] == 8
    assert payload["baseline"]["candidate_count_long"] <= 8
    assert payload["baseline"]["candidate_count_short"] <= 8


def test_nightly_candidate_cli_prints_summary_payload(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        external_analysis_main_module,
        "run_nightly_candidate_pipeline",
        lambda **_kwargs: {
            "ok": True,
            "run_id": "nightly_demo",
            "status": "published_with_surface_failure",
            "quarantine_reason": "forecast_surface_evaluation_missing",
            "baseline": {
                "publish_id": "pub_demo",
                "metrics_saved": True,
                "forecast_surface_saved": True,
                "forecast_surface_evaluation_saved": False,
                "forecast_surface_evaluation_gate_reason": "evaluation_missing",
                "forecast_surface": {"rows": [{"code": "1301"}]},
            },
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "external_analysis",
            "nightly-candidate-run",
            "--as-of-date",
            "20260312",
        ],
    )

    assert external_analysis_main() == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["run_id"] == "nightly_demo"
    assert payload["baseline"]["publish_id"] == "pub_demo"
    assert "forecast_surface" not in payload["baseline"]


def test_nightly_candidate_cli_forwards_prepared_environment_flag(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _run_nightly_candidate_pipeline(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "run_id": "nightly_demo", "status": "success", "baseline": {"publish_id": "pub_demo"}}

    monkeypatch.setattr(external_analysis_main_module, "run_nightly_candidate_pipeline", _run_nightly_candidate_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "external_analysis",
            "nightly-candidate-run",
            "--as-of-date",
            "20260312",
            "--require-prepared-environment",
        ],
    )

    assert external_analysis_main() == 0
    assert captured["require_prepared_environment"] is True
