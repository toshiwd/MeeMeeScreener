from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from external_analysis.runtime import daily_research as daily_research_module
from external_analysis.runtime import historical_replay as historical_replay_module
from external_analysis.runtime import nightly_pipeline as nightly_pipeline_module
from external_analysis.runtime.source_snapshot import create_source_snapshot, probe_source_universe_readiness


def test_create_source_snapshot_copies_db_and_prunes_old_files(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_root = tmp_path / "snapshots"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER)")
        conn.execute("INSERT INTO daily_bars VALUES ('1301', 20260314)")
    finally:
        conn.close()
    wal_path = Path(f"{source_db}.wal")
    wal_path.write_text("wal", encoding="utf-8")

    first = create_source_snapshot(
        source_db_path=str(source_db),
        snapshot_root=str(snapshot_root),
        label="daily_research",
        keep_latest=1,
    )
    second = create_source_snapshot(
        source_db_path=str(source_db),
        snapshot_root=str(snapshot_root),
        label="daily_research",
        keep_latest=1,
    )

    assert Path(second["snapshot_db_path"]).exists() is True
    assert Path(second["snapshot_wal_path"]).exists() is True
    assert len(list(snapshot_root.glob("*.json"))) >= 1


def test_create_source_snapshot_materializes_feature_frame_and_marks_tdnet_absent(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_root = tmp_path / "snapshots"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute(
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
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily VALUES
            (20260314, '1301', 100.0, 99.0, 98.0, 97.0, 1.2, 0.03, 13, 7, 20260314)
            """
        )
        conn.execute("CREATE TABLE tdnet_disclosures (dt INTEGER, code TEXT)")
    finally:
        conn.close()

    snapshot = create_source_snapshot(
        source_db_path=str(source_db),
        snapshot_root=str(snapshot_root),
        label="feature_frame",
        keep_latest=1,
    )

    conn = duckdb.connect(str(snapshot["snapshot_db_path"]), read_only=True)
    try:
        cols = {str(row[1]) for row in conn.execute("PRAGMA table_info('feature_frame_daily')").fetchall()}
        row = conn.execute(
            """
            SELECT COUNT(*), MIN(source_presence_flag_tdnet_disclosures), MAX(source_presence_flag_tdnet_disclosures)
            FROM feature_frame_daily
            """
        ).fetchone()
    finally:
        conn.close()

    assert "available_at" in cols
    assert "feature_frame_version" in cols
    assert "source_presence_flag_tdnet_disclosures" in cols
    assert row is not None
    assert int(row[0]) == 1
    assert int(row[1]) == 0
    assert int(row[2]) == 0


def test_create_source_snapshot_rejects_future_available_at(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_root = tmp_path / "snapshots"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute(
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
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily VALUES
            (20260314, '1301', 100.0, 99.0, 98.0, 97.0, 1.2, 0.03, 13, 7, 20260315)
            """
        )
    finally:
        conn.close()

    with pytest.raises(ValueError, match="feature_frame_future_data"):
        create_source_snapshot(
            source_db_path=str(source_db),
            snapshot_root=str(snapshot_root),
            label="feature_frame_future",
            keep_latest=1,
        )


def test_create_source_snapshot_normalizes_pre_2001_epoch_feature_dates(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_root = tmp_path / "snapshots"
    epoch_dt = int(datetime(2001, 9, 20, tzinfo=timezone.utc).timestamp())
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute(
            """
            CREATE TABLE feature_snapshot_daily (
                dt BIGINT,
                code TEXT,
                close DOUBLE,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                atr14 DOUBLE,
                diff20_pct DOUBLE,
                cnt_20_above INTEGER,
                cnt_7_above INTEGER,
                available_at BIGINT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily VALUES
            (?, '1301', 100.0, 99.0, 98.0, 97.0, 1.2, 0.03, 13, 7, ?)
            """,
            [epoch_dt, epoch_dt],
        )
    finally:
        conn.close()

    snapshot = create_source_snapshot(
        source_db_path=str(source_db),
        snapshot_root=str(snapshot_root),
        label="feature_frame_epoch",
        keep_latest=1,
    )

    conn = duckdb.connect(str(snapshot["snapshot_db_path"]), read_only=True)
    try:
        row = conn.execute("SELECT dt, available_at FROM feature_frame_daily").fetchone()
    finally:
        conn.close()

    assert row == (20010920, 20010920)


def test_probe_source_universe_readiness_rejects_partial_feature_frame_day(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_root = tmp_path / "snapshots"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute(
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
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily VALUES
            (20260313, '1301', 100.0, 99.0, 98.0, 97.0, 1.0, 0.01, 10, 5, 20260313),
            (20260313, '1302', 100.0, 99.0, 98.0, 97.0, 1.0, 0.01, 10, 5, 20260313),
            (20260313, '1303', 100.0, 99.0, 98.0, 97.0, 1.0, 0.01, 10, 5, 20260313),
            (20260314, '1301', 100.0, 99.0, 98.0, 97.0, 1.0, 0.01, 10, 5, 20260314)
            """
        )
    finally:
        conn.close()

    snapshot = create_source_snapshot(
        source_db_path=str(source_db),
        snapshot_root=str(snapshot_root),
        label="feature_frame_readiness",
        keep_latest=1,
    )

    readiness = probe_source_universe_readiness(
        source_db_path=str(snapshot["snapshot_db_path"]),
        as_of_date="20260314",
        min_universe_code_count=3,
    )

    assert readiness["ready"] is False
    assert readiness["reason"] == "source_universe_too_small"
    assert readiness["observed_code_count"] == 1
    assert readiness["latest_trade_date"] == 20260314
    assert readiness["source_table"] == "feature_frame_daily"


def test_probe_source_universe_readiness_accepts_yyyymmdd_daily_bars_dates(tmp_path) -> None:
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
            ('1301', 20260314),
            ('1302', 20260314),
            ('1303', 20260314)
            """
        )
    finally:
        conn.close()

    readiness = probe_source_universe_readiness(
        source_db_path=str(source_db),
        as_of_date="20260314",
        min_universe_code_count=3,
    )

    assert readiness["ready"] is True
    assert readiness["reason"] == "ready"
    assert readiness["observed_code_count"] == 3
    assert readiness["latest_trade_date"] == 20260314
    assert readiness["source_table"] == "daily_bars"


def test_probe_source_universe_readiness_normalizes_pre_1e9_epoch_seconds(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("CREATE TABLE ml_feature_daily (code TEXT, dt BIGINT)")
        conn.execute(
            """
            INSERT INTO ml_feature_daily VALUES
            ('1301', 999820800),
            ('1302', 999820800)
            """
        )
    finally:
        conn.close()

    readiness = probe_source_universe_readiness(
        source_db_path=str(source_db),
        as_of_date="20010907",
        min_universe_code_count=2,
    )

    assert readiness["ready"] is True
    assert readiness["reason"] == "ready"
    assert readiness["observed_code_count"] == 2
    assert readiness["latest_trade_date"] == 20010907
    assert readiness["source_table"] == "ml_feature_daily"


def test_probe_source_universe_readiness_prefers_source_with_requested_as_of_date(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("CREATE TABLE ml_feature_daily (code TEXT, dt BIGINT)")
        conn.execute(
            """
            INSERT INTO ml_feature_daily VALUES
            ('1301', 999820800),
            ('1302', 999820800)
            """
        )
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER)")
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
            ('1301', 20260409),
            ('1302', 20260409),
            ('1303', 20260409)
            """
        )
    finally:
        conn.close()

    readiness = probe_source_universe_readiness(
        source_db_path=str(source_db),
        as_of_date="20260409",
        min_universe_code_count=3,
    )

    assert readiness["ready"] is True
    assert readiness["reason"] == "ready"
    assert readiness["observed_code_count"] == 3
    assert readiness["latest_trade_date"] == 20260409
    assert readiness["source_table"] == "daily_bars"


def test_nightly_candidate_pipeline_uses_snapshot_source(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    duckdb.connect(str(source_db)).close()
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        nightly_pipeline_module,
        "run_diff_export",
        lambda *, source_db_path, export_db_path: (
            captured.setdefault("source_db_path", str(source_db_path)),
            {"run_id": "export"},
        )[1],
    )
    monkeypatch.setattr(nightly_pipeline_module, "build_rolling_labels", lambda **_kwargs: {"run_id": "labels"})
    monkeypatch.setattr(
        nightly_pipeline_module,
        "run_candidate_baseline",
        lambda **_kwargs: {
            "publish_id": "pub_demo",
            "metrics_saved": True,
            "state_eval_count": 1,
            "forecast_surface_saved": True,
            "forecast_surface_evaluation": {"scope_type": "publish"},
            "forecast_surface_evaluation_saved": True,
        },
    )
    monkeypatch.setattr(
        nightly_pipeline_module,
        "probe_source_universe_readiness",
        lambda **_kwargs: {"ready": True, "reason": "ready", "observed_code_count": 1},
    )
    monkeypatch.setattr(nightly_pipeline_module, "upsert_job_run", lambda **_kwargs: None)

    payload = nightly_pipeline_module.run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        as_of_date="20260314",
    )

    assert payload["ok"] is True
    assert captured["source_db_path"] != str(source_db)
    assert Path(captured["source_db_path"]).exists()
    assert "source_snapshots" in captured["source_db_path"]


def test_daily_research_cycle_uses_single_snapshot_source(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_db = tmp_path / "snapshot.duckdb"
    source_db.write_text("live", encoding="utf-8")
    snapshot_db.write_text("copy", encoding="utf-8")
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        daily_research_module,
        "create_source_snapshot",
        lambda **_kwargs: {"snapshot_db_path": str(snapshot_db), "snapshot_id": "snap_daily"},
    )
    monkeypatch.setattr(
        daily_research_module,
        "resolve_latest_daily_research_as_of_date",
        lambda *, source_db_path=None: (captured.setdefault("resolved_from", str(source_db_path)), "20260314")[1],
    )
    monkeypatch.setattr(
        daily_research_module,
        "run_nightly_candidate_pipeline",
        lambda **kwargs: (
            captured.setdefault("candidate_source", str(kwargs.get("source_db_path"))),
            {
                "ok": True,
                "run_id": "candidate",
                "status": "success",
                "quarantine_reason": None,
                "baseline": {"publish_id": "pub_demo"},
            },
        )[1],
    )
    monkeypatch.setattr(
        daily_research_module,
        "run_nightly_similarity_pipeline",
        lambda **_kwargs: {"ok": True, "run_id": "similarity", "status": "success", "quarantine_reason": None},
    )
    monkeypatch.setattr(
        daily_research_module,
        "run_nightly_similarity_challenger_pipeline",
        lambda **_kwargs: {"ok": True, "run_id": "challenger", "status": "success", "quarantine_reason": None},
    )
    monkeypatch.setattr(
        daily_research_module,
        "build_daily_research_report",
        lambda **kwargs: {"publish": {"publish_id": "pub_demo"}, "action_queue": [], "source_db_path": kwargs.get("source_db_path")},
    )
    monkeypatch.setattr(daily_research_module, "persist_review_artifact", lambda **_kwargs: None)

    payload = daily_research_module.run_daily_research_cycle(
        source_db_path=str(source_db),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
    )

    assert payload["ok"] is True
    assert captured["resolved_from"] == str(snapshot_db)
    assert captured["candidate_source"] == str(snapshot_db)


def test_historical_replay_uses_snapshot_source(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    snapshot_db = tmp_path / "snapshot.duckdb"
    source_db.write_text("live", encoding="utf-8")
    snapshot_db.write_text("copy", encoding="utf-8")
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        historical_replay_module,
        "create_source_snapshot",
        lambda **_kwargs: {"snapshot_db_path": str(snapshot_db), "snapshot_id": "snap_replay"},
    )
    monkeypatch.setattr(
        historical_replay_module,
        "_select_replay_dates",
        lambda **kwargs: (captured.setdefault("dates_source", str(kwargs.get("source_db_path"))), [20260105])[1],
    )
    monkeypatch.setattr(
        historical_replay_module,
        "_select_codes",
        lambda **kwargs: (
            captured.setdefault("codes_source", str(kwargs.get("source_db_path"))),
            ["1301"],
        )[1],
    )
    monkeypatch.setattr(historical_replay_module, "upsert_replay_run", lambda **_kwargs: None)
    monkeypatch.setattr(historical_replay_module, "upsert_replay_day", lambda **_kwargs: None)
    monkeypatch.setattr(historical_replay_module, "_get_day_status", lambda **_kwargs: None)
    monkeypatch.setattr(
        historical_replay_module,
        "_run_replay_bootstrap_export",
        lambda **kwargs: {"run_id": "export", "source": captured.setdefault("export_source", str(kwargs.get("source_db_path")))},
    )
    monkeypatch.setattr(historical_replay_module, "build_rolling_labels", lambda **_kwargs: {"run_id": "labels"})
    monkeypatch.setattr(historical_replay_module, "build_case_library", lambda **_kwargs: {"run_id": "case_library", "cache_state": "fresh"})
    monkeypatch.setattr(historical_replay_module, "run_candidate_baseline", lambda **_kwargs: {"metrics_saved": True})
    monkeypatch.setattr(historical_replay_module, "run_similarity_baseline", lambda **_kwargs: {"metrics_saved": True})
    monkeypatch.setattr(historical_replay_module, "_load_replay_days", lambda **_kwargs: [{"status": "success", "publish_id": "pub_demo", "as_of_date": 20260105}])
    monkeypatch.setattr(historical_replay_module, "persist_replay_summary", lambda **_kwargs: None)
    monkeypatch.setattr(historical_replay_module, "_current_case_library_source_signature", lambda **_kwargs: "sig_demo")
    monkeypatch.setattr(historical_replay_module, "upsert_work_item", lambda **_kwargs: None)
    monkeypatch.setattr(historical_replay_module, "insert_quarantine_record", lambda **_kwargs: None)

    payload = historical_replay_module.run_historical_replay(
        source_db_path=str(source_db),
        export_db_path=str(tmp_path / "export.duckdb"),
        label_db_path=str(tmp_path / "label.duckdb"),
        result_db_path=str(tmp_path / "result.duckdb"),
        similarity_db_path=str(tmp_path / "similarity.duckdb"),
        ops_db_path=str(tmp_path / "ops.duckdb"),
        start_as_of_date="20260105",
        end_as_of_date="20260105",
        replay_id="replay_snapshot",
    )

    assert payload["ok"] is True
    assert captured["dates_source"] == str(snapshot_db)
    assert captured["codes_source"] == str(snapshot_db)
    assert captured["export_source"] == str(snapshot_db)
    assert payload["bootstrap"]["anchors"]["skipped"] is True
