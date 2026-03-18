from __future__ import annotations

from pathlib import Path

import duckdb

from external_analysis.runtime import daily_research as daily_research_module
from external_analysis.runtime import historical_replay as historical_replay_module
from external_analysis.runtime import nightly_pipeline as nightly_pipeline_module
from external_analysis.runtime.source_snapshot import create_source_snapshot


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

    assert Path(first["snapshot_db_path"]).exists() is False
    assert Path(second["snapshot_db_path"]).exists() is True
    assert Path(second["snapshot_wal_path"]).exists() is True
    assert len(list(snapshot_root.glob("*.json"))) == 1


def test_nightly_candidate_pipeline_uses_snapshot_source(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    source_db.write_text("snapshot me", encoding="utf-8")
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
        lambda **_kwargs: {"publish_id": "pub_demo", "metrics_saved": True, "state_eval_count": 1},
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
