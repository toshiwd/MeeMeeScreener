import os
from copy import deepcopy
import sys
from unittest.mock import patch

import pytest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.core import txt_followup_job, txt_update_job


@pytest.fixture(autouse=True)
def _isolate_txt_update_manifest(monkeypatch, tmp_path):
    monkeypatch.setattr(
        txt_update_job,
        "_txt_source_manifest_path",
        lambda: str(tmp_path / "txt_update_source_manifest.json"),
    )
    monkeypatch.setattr(txt_update_job, "_latest_confirmed_db_date_key", lambda: 20260101)
    monkeypatch.setattr(
        txt_update_job,
        "_seed_ingest_state_hashes_for_export",
        lambda _out_dir: {
            "seeded_files": 0,
            "total_bytes": 0,
            "elapsed_ms": 0,
            "state_path": str(tmp_path / "ingest_state.json"),
        },
    )


def _build_common_patches():
    return [
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch("app.backend.infra.panrolling.pan_import.run_pan_import", return_value=True),
        patch("app.backend.core.txt_update_job.run_vbs_export", return_value=(0, ["SUMMARY: total=1 ok=1 err=0"])),
        patch("app.backend.core.txt_update_job.run_ingest", return_value=("", "", {"rows": "10"})),
        patch("app.backend.core.txt_update_job._run_phase_batch_latest", return_value=20260101),
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db"),
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
    ]


def test_txt_update_records_failed_stage_when_scoring_fails():
    state_store: dict = {}
    patches = _build_common_patches()
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.jobs.scoring_job.ScoringJob.run", side_effect=RuntimeError("score-broken")),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
    ):
        txt_update_job.handle_txt_update("job-fail", {"auto_ml_predict": False, "auto_ml_train": False})

    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "failed"
    assert saved_state["last_failed_stage"] == "scoring"
    assert saved_state["last_error"] == "score-broken"
    assert saved_state["last_error_message"] == "Scoring refresh failed"
    assert any(
        call.args[2] == "failed" and call.kwargs.get("error") == "Scoring refresh failed"
        for call in mock_update_db.call_args_list
    )


def test_txt_update_success_records_cache_refresh_and_skips_tracking_by_default():
    state_store: dict = {}
    stage_trace: list[str] = []

    def _scoring_run(*_args, **_kwargs):
        stage_trace.append("scoring")
        return [{"code": "1301"}]

    def _refresh_cache():
        stage_trace.append("cache_refresh")

    patches = _build_common_patches()
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.jobs.scoring_job.ScoringJob.run", side_effect=_scoring_run),
            patch("app.backend.services.rankings_cache.refresh_cache", side_effect=_refresh_cache),
            patch("app.backend.services.signal_tracking_service.refresh_daily_tracking_window"),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12] as mock_refresh_tracking,
    ):
        txt_update_job.handle_txt_update("job-ok", {"auto_ml_predict": False, "auto_ml_train": False})

    assert stage_trace == ["scoring", "cache_refresh"]
    mock_refresh_tracking.assert_not_called()
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_pipeline_stage"] == "finalize"
    assert "last_cache_refresh_at" in saved_state
    assert saved_state["last_tracking_refresh_skipped_reason"] == "daily_fast_path"
    assert "last_txt_update_at" in saved_state
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)


def test_txt_update_no_change_fast_path_skips_recompute_when_cache_is_current():
    state_store: dict = {"last_cache_refresh_at": "2026-04-22T01:00:00"}
    stage_trace: list[str] = []
    patches = _build_common_patches()
    patches[3] = patch(
        "app.backend.core.txt_update_job.run_ingest",
        return_value=("", "", {"rows": "10", "changed_files": 0, "pan_finalized_rows": 0}),
    )
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch(
                "app.backend.jobs.scoring_job.ScoringJob.run",
                side_effect=lambda *_args, **_kwargs: stage_trace.append("scoring"),
            ),
            patch(
                "app.backend.services.rankings_cache.refresh_cache",
                side_effect=lambda: stage_trace.append("cache_refresh"),
            ),
            patch("app.backend.services.signal_tracking_service.refresh_daily_tracking_window"),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4] as mock_phase,
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11] as mock_refresh_cache,
        patches[12] as mock_refresh_tracking,
    ):
        txt_update_job.handle_txt_update("job-no-change", {"auto_ml_predict": False, "auto_ml_train": False})

    assert stage_trace == []
    mock_phase.assert_not_called()
    mock_refresh_cache.assert_not_called()
    mock_refresh_tracking.assert_not_called()
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_pipeline_stage"] == "finalize"
    assert saved_state["last_tracking_refresh_skipped_reason"] == "no_confirmed_change"
    assert "No confirmed TXT/PAN changes" in saved_state["last_pipeline_message"]
    assert any(call.args[2] == "success" and call.kwargs.get("progress") == 100 for call in mock_update_db.call_args_list)


def test_txt_update_manifest_no_change_skips_export_import_ranking_and_tracking():
    state_store: dict = {
        "last_cache_refresh_at": "2026-04-22T01:00:00",
        "last_cache_refresh_db_latest_key": 20260101,
    }
    manifest = {
        "schema_version": 1,
        "source_files": [{"path": "code.txt", "mtime_ns": 1, "size": 10}],
        "export_outputs": [{"path": "1001.txt", "mtime_ns": 2, "size": 20}],
        "db_latest_date": "2026-01-01",
        "ranking_snapshot_as_of": "2026-01-01",
    }
    patches = _build_common_patches()
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.core.txt_update_job._load_txt_source_manifest", return_value=manifest),
            patch("app.backend.core.txt_update_job._build_txt_source_manifest_snapshot", return_value=dict(manifest)),
            patch("app.backend.jobs.scoring_job.ScoringJob.run"),
            patch("app.backend.services.rankings_cache.refresh_cache"),
            patch("app.backend.services.signal_tracking_service.refresh_daily_tracking_window"),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2] as mock_export,
        patches[3] as mock_ingest,
        patches[4] as mock_phase,
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12] as mock_scoring,
        patches[13] as mock_refresh_cache,
        patches[14] as mock_refresh_tracking,
    ):
        txt_update_job.handle_txt_update(
            "job-manifest-no-change",
            {
                "allow_manifest_fast_noop": True,
                "auto_ml_predict": False,
                "auto_ml_train": False,
            },
        )

    mock_export.assert_not_called()
    mock_ingest.assert_not_called()
    mock_phase.assert_not_called()
    mock_scoring.assert_not_called()
    mock_refresh_cache.assert_not_called()
    mock_refresh_tracking.assert_not_called()
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_txt_update_no_change_reason"] == "source_manifest_unchanged"
    assert any(
        call.args[2] == "success"
        and call.kwargs.get("message") == "No confirmed TXT/PAN source changes detected. Daily update fast path completed."
        for call in mock_update_db.call_args_list
    )


def test_txt_update_manifest_match_still_runs_export_by_default():
    state_store: dict = {
        "last_cache_refresh_at": "2026-04-22T01:00:00",
        "last_cache_refresh_db_latest_key": 20260101,
    }
    manifest = {
        "schema_version": 1,
        "source_files": [{"path": "code.txt", "mtime_ns": 1, "size": 10}],
        "export_outputs": [{"path": "1001.txt", "mtime_ns": 2, "size": 20}],
        "db_latest_date": "2026-01-01",
        "source_latest_date": "2026-01-01",
        "ranking_snapshot_as_of": "2026-01-01",
    }
    patches = _build_common_patches()
    patches[3] = patch(
        "app.backend.core.txt_update_job.run_ingest",
        return_value=("", "", {"rows": "10", "changed_files": 0, "pan_finalized_rows": 0}),
    )
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.core.txt_update_job._load_txt_source_manifest", return_value=manifest),
            patch("app.backend.core.txt_update_job._build_txt_source_manifest_snapshot", return_value=dict(manifest)),
            patch("app.backend.jobs.scoring_job.ScoringJob.run"),
            patch("app.backend.services.rankings_cache.refresh_cache"),
            patch("app.backend.services.signal_tracking_service.refresh_daily_tracking_window"),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2] as mock_export,
        patches[3] as mock_ingest,
        patches[4] as mock_phase,
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12],
        patches[13] as mock_refresh_cache,
        patches[14] as mock_refresh_tracking,
    ):
        txt_update_job.handle_txt_update("job-manifest-refresh", {"auto_ml_predict": False, "auto_ml_train": False})

    mock_export.assert_called_once()
    mock_ingest.assert_called_once()
    mock_phase.assert_not_called()
    mock_refresh_cache.assert_not_called()
    mock_refresh_tracking.assert_not_called()
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_tracking_refresh_skipped_reason"] == "no_confirmed_change"
    assert any(
        call.args[2] == "running"
        and call.kwargs.get("message") == "Running Pan Rolling export..."
        for call in mock_update_db.call_args_list
    )


def test_txt_source_manifest_uses_latest_txt_export_date(tmp_path):
    code_path = tmp_path / "codes.txt"
    code_path.write_text("9041\n", encoding="utf-8")
    out_dir = tmp_path / "txt"
    out_dir.mkdir()
    (out_dir / "9041_test.txt").write_text(
        "9041,2026/05/14,1,1,1,1,1\n9041,2026/05/15,1,1,1,1,1\n",
        encoding="cp932",
    )

    manifest = txt_update_job._build_txt_source_manifest_snapshot(
        code_path=str(code_path),
        out_dir=str(out_dir),
        db_latest_key=20260508,
        ranking_snapshot_key=20260508,
    )

    assert manifest["source_latest_date"] == "2026-05-15"
    assert manifest["db_latest_date"] == "2026-05-08"
    assert txt_update_job._manifests_match_for_noop(dict(manifest), manifest) == (
        False,
        "source_newer_than_db",
    )


def test_txt_update_source_newer_than_db_forces_full_ingest_and_skips_hash_seed():
    state_store: dict = {
        "last_cache_refresh_at": "2026-05-08T01:00:00",
        "last_cache_refresh_db_latest_key": 20260508,
    }
    saved_snapshots: list[dict] = []
    manifest = {
        "schema_version": 1,
        "source_files": [{"path": "code.txt", "mtime_ns": 1, "size": 10}],
        "export_outputs": [{"path": "9041.txt", "mtime_ns": 2, "size": 20}],
        "source_latest_date": "2026-05-15",
        "db_latest_date": "2026-05-08",
        "ranking_snapshot_as_of": "2026-05-08",
    }

    def _save_state(state: dict) -> None:
        saved_snapshots.append(deepcopy(state))

    with (
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch("app.backend.infra.panrolling.pan_import.run_pan_import", return_value=True),
        patch("app.backend.core.txt_update_job.run_vbs_export", return_value=(0, ["SUMMARY: total=1 ok=1 err=0"])),
        patch(
            "app.backend.core.txt_update_job.run_ingest",
            return_value=("", "", {"rows": "10", "changed_files": 1, "pan_finalized_rows": 0}),
        ) as mock_ingest,
        patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state", side_effect=_save_state),
        patch("app.backend.core.txt_update_job._load_txt_source_manifest", return_value=dict(manifest)),
        patch("app.backend.core.txt_update_job._build_txt_source_manifest_snapshot", return_value=dict(manifest)),
        patch("app.backend.core.txt_update_job._seed_ingest_state_hashes_for_export") as mock_seed,
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db"),
        patch("app.backend.core.txt_update_job.job_manager.submit"),
    ):
        txt_update_job.handle_txt_update(
            "job-source-newer",
            {
                "completion_mode": "practical_fast",
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "auto_fill_missing_history": False,
            },
        )

    mock_seed.assert_not_called()
    assert mock_ingest.call_args.kwargs["incremental"] is False
    assert any(
        snapshot.get("last_ingest_hash_seed_status") == "skipped"
        and snapshot.get("last_ingest_hash_seed_result", {}).get("reason") == "source_newer_than_db"
        for snapshot in saved_snapshots
    )


def test_txt_update_seeds_ingest_hashes_before_export(monkeypatch, tmp_path):
    state_store: dict = {}
    order: list[str] = []

    def _seed_hashes(_out_dir: str) -> dict:
        order.append("seed_hashes")
        return {
            "seeded_files": 2,
            "total_bytes": 20,
            "elapsed_ms": 1,
            "state_path": str(tmp_path / "ingest_state.json"),
        }

    def _run_export(*_args, **_kwargs):
        order.append("export")
        return 0, ["SUMMARY: total=1 ok=1 err=0"]

    monkeypatch.setattr(txt_update_job, "_seed_ingest_state_hashes_for_export", _seed_hashes)

    with (
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch("app.backend.infra.panrolling.pan_import.run_pan_import", return_value=True),
        patch("app.backend.core.txt_update_job.run_vbs_export", side_effect=_run_export),
        patch(
            "app.backend.core.txt_update_job.run_ingest",
            return_value=("", "", {"rows": "10", "changed_files": 1, "pan_finalized_rows": 0}),
        ),
        patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state") as mock_save_state,
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db"),
        patch("app.backend.core.txt_update_job.job_manager.submit", return_value="followup-1"),
    ):
        txt_update_job.handle_txt_update(
            "job-seed",
            {
                "completion_mode": "practical_fast",
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "auto_fill_missing_history": False,
            },
        )

    assert order[:2] == ["seed_hashes", "export"]
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_ingest_hash_seed_status"] == "done"
    assert saved_state["last_ingest_hash_seed_result"]["seeded_files"] == 2


def test_txt_update_records_explicit_tracking_refresh_heartbeats_and_substages():
    state_store: dict = {}
    stage_trace: list[str] = []
    saved_snapshots: list[dict] = []

    def _scoring_run(*_args, **_kwargs):
        stage_trace.append("scoring")
        return [{"code": "1301"}]

    def _refresh_cache():
        stage_trace.append("cache_refresh")

    def _refresh_tracking(*_args, **kwargs):
        stage_trace.append("tracking_refresh")
        progress_cb = kwargs.get("progress_cb")
        if progress_cb is not None:
            progress_cb(
                {
                    "stage": "tracking_refresh",
                    "phase": "prepare",
                    "status": "start",
                    "substage": "tracking_refresh.prepare",
                    "processed": 0,
                    "total": 6,
                    "detail": "preparing backfill",
                    "heartbeat_at": "2026-04-22T03:40:00+09:00",
                }
            )
            progress_cb(
                {
                    "stage": "tracking_refresh",
                    "phase": "basis",
                    "status": "running",
                    "substage": "tracking_refresh.basis",
                    "processed": 2,
                    "total": 6,
                    "current_market_ymd": 20260421,
                    "current_market_date": "2026-04-21",
                    "detail": "building basis rows",
                    "heartbeat_at": "2026-04-22T03:40:02+09:00",
                }
            )
            progress_cb(
                {
                    "stage": "tracking_refresh",
                    "phase": "ranking_appearances",
                    "status": "running",
                    "substage": "tracking_refresh.ranking_appearances",
                    "processed": 5,
                    "total": 6,
                    "current_market_ymd": 20260421,
                    "current_market_date": "2026-04-21",
                    "detail": "building ranking appearances",
                    "heartbeat_at": "2026-04-22T03:40:04+09:00",
                }
            )
            progress_cb(
                {
                    "stage": "tracking_refresh",
                    "phase": "finalize",
                    "status": "done",
                    "substage": "tracking_refresh.finalize",
                    "processed": 6,
                    "total": 6,
                    "current_market_ymd": 20260421,
                    "current_market_date": "2026-04-21",
                    "detail": "tracking refresh completed",
                    "heartbeat_at": "2026-04-22T03:40:06+09:00",
                }
            )
        return {
            "ok": True,
            "market_day_window": 60,
            "from": "2025-11-01",
            "to": "2026-01-01",
            "result": {"basis": {"dates_processed": 10}, "ranking": {"appearance_upserted": 20}},
        }

    def _save_state(state: dict) -> None:
        saved_snapshots.append(deepcopy(state))

    patches = _build_common_patches()
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state", side_effect=_save_state),
            patch("app.backend.jobs.scoring_job.ScoringJob.run", side_effect=_scoring_run),
            patch("app.backend.services.rankings_cache.refresh_cache", side_effect=_refresh_cache),
            patch("app.backend.services.signal_tracking_service.refresh_daily_tracking_window", side_effect=_refresh_tracking),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12],
    ):
        txt_update_job.handle_txt_update(
            "job-heartbeat",
            {
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "run_tracking_refresh": True,
                "tracking_refresh_trigger_reason": "manual_full_refresh",
            },
        )

    assert stage_trace == ["scoring", "cache_refresh", "tracking_refresh"]
    assert mock_save_state.call_count > 0
    assert len(saved_snapshots) >= 4
    substage_sequence = [snapshot.get("last_pipeline_substage") for snapshot in saved_snapshots if snapshot.get("last_pipeline_substage")]
    assert substage_sequence[0] == "tracking_refresh.prepare"
    assert "tracking_refresh.basis" in substage_sequence
    assert "tracking_refresh.ranking_appearances" in substage_sequence
    assert substage_sequence[-1] == "tracking_refresh.finalize"
    assert saved_snapshots[-1]["last_pipeline_heartbeat_at"] == "2026-04-22T03:40:06+09:00"
    assert saved_snapshots[-1]["last_pipeline_progress_detail"]["current_phase"] == "finalize"
    assert saved_snapshots[-1]["last_pipeline_progress_detail"]["trigger_reason"] == "manual_full_refresh"
    assert any("tracking_refresh.prepare" in (call.kwargs.get("message") or "") for call in mock_update_db.call_args_list)
    assert any("tracking_refresh.finalize" in (call.kwargs.get("message") or "") for call in mock_update_db.call_args_list)


def test_txt_update_skips_legacy_recompute_when_pan_finalize_detected():
    state_store: dict = {}
    with (
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch("app.backend.infra.panrolling.pan_import.run_pan_import", return_value=True),
        patch("app.backend.core.txt_update_job.run_vbs_export", return_value=(0, ["SUMMARY: total=1 ok=1 err=0"])),
        patch(
            "app.backend.core.txt_update_job.run_ingest",
            return_value=(
                "",
                "",
                {"rows": "10", "changed_files": 0, "pan_finalized_rows": 1},
            ),
        ),
        patch("app.backend.core.txt_update_job._run_phase_batch_latest", return_value=20260101),
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db") as mock_update_db,
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
        patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state") as mock_save_state,
        patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
        patch("app.backend.services.rankings_cache.refresh_cache"),
        patch("app.backend.services.ml_service.train_models", return_value={"model_version": "m1"}) as mock_train,
        patch("app.backend.services.ml_service.predict_for_dt", return_value={"dt": 20260101, "rows": 10}) as mock_predict,
        patch("app.backend.services.ml_service.enforce_live_guard", return_value={"action": "keep"}) as mock_guard,
        patch(
            "app.backend.services.strategy_backtest_service.run_strategy_walkforward",
            return_value={
                "run_id": "swf_1",
                "summary": {"oos_total_realized_unit_pnl": 0.1, "oos_mean_profit_factor": 1.1},
                "windowing": {},
            },
        ) as mock_walkforward_run,
        patch(
            "app.backend.services.strategy_backtest_service.run_strategy_walkforward_gate",
            return_value={
                "gate_id": "swfg_1",
                "status": "pass",
                "passed": True,
                "source": {"run_id": "swf_1", "finished_at": "2026-03-06T00:00:00+00:00"},
                "thresholds": {},
            },
        ) as mock_walkforward_gate,
        patch(
            "app.backend.services.strategy_backtest_service.save_daily_walkforward_research_snapshot",
            return_value={"saved": True, "snapshot_date": 20260306, "source_run_id": "swf_1"},
        ) as mock_research_snapshot,
    ):
        txt_update_job.handle_txt_update(
            "job-pan-finalize",
            {
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "force_recompute_on_pan_finalize": True,
            },
        )

    assert mock_train.call_count == 0
    assert mock_predict.call_count == 0
    assert mock_guard.call_count == 0
    assert mock_walkforward_run.call_count == 1
    assert mock_walkforward_gate.call_count == 1
    assert mock_research_snapshot.call_count == 0
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pan_finalize_rows"] == 1
    assert "last_forced_recompute_at" in saved_state
    assert saved_state["last_phase_skip_reason"] == "legacy_analysis_disabled"
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)


def test_txt_update_practical_fast_finishes_after_ingest_and_queues_background_followup():
    state_store: dict = {}
    stage_trace: list[str] = []
    patches = _build_common_patches()
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
            patch(
                "app.backend.services.rankings_cache.refresh_cache",
                side_effect=lambda: stage_trace.append("cache_refresh"),
            ),
            patch(
                "app.backend.services.signal_tracking_service.refresh_daily_tracking_window",
                side_effect=lambda *args, **kwargs: (
                    stage_trace.append("tracking_refresh")
                    or {
                        "ok": True,
                        "market_day_window": 60,
                        "from": "2025-11-01",
                        "to": "2026-01-01",
                        "result": {"basis": {"dates_processed": 10}, "ranking": {"appearance_upserted": 20}},
                    }
                ),
            ),
            patch(
                "app.backend.core.txt_update_job.job_manager.submit",
                return_value="followup-1",
            ),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12],
        patches[13] as mock_submit,
    ):
        txt_update_job.handle_txt_update(
            "job-fast",
            {
                "completion_mode": "practical_fast",
                "auto_ml_predict": True,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "auto_fill_missing_history": False,
            },
    )

    assert stage_trace == []
    mock_submit.assert_called_once()
    submitted_type = mock_submit.call_args.args[0]
    submitted_payload = mock_submit.call_args.args[1]
    assert submitted_type == "txt_followup"
    assert submitted_payload["source_txt_job_id"] == "job-fast"
    assert submitted_payload["phase_dt"] is None
    assert submitted_payload["db_latest_after_key"] == 20260101
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_tracking_refresh_skipped_reason"] == "daily_fast_path"
    assert saved_state["last_followup_job_id"] == "followup-1"
    assert saved_state["last_followup_source_txt_job_id"] == "job-fast"
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)
    assert any(
        "Confirmed TXT/PAN bars imported; chart refresh is ready" in (call.kwargs.get("message") or "")
        for call in mock_update_db.call_args_list
    )


def test_txt_update_practical_fast_chart_only_skips_unrequested_followup():
    state_store: dict = {}
    stage_trace: list[str] = []
    patches = _build_common_patches()
    patches[3] = patch(
        "app.backend.core.txt_update_job.run_ingest",
        return_value=("", "", {"rows": "10", "changed_files": 1, "pan_finalized_rows": 2}),
    )
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
            patch(
                "app.backend.services.rankings_cache.refresh_cache",
                side_effect=lambda: stage_trace.append("cache_refresh"),
            ),
            patch("app.backend.services.signal_tracking_service.refresh_daily_tracking_window"),
            patch("app.backend.core.txt_update_job.job_manager.submit", return_value="followup-1"),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12],
        patches[13] as mock_submit,
    ):
        txt_update_job.handle_txt_update(
            "job-chart-only",
            {
                "completion_mode": "practical_fast",
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "auto_fill_missing_history": False,
                "force_recompute_on_pan_finalize": False,
            },
        )

    assert stage_trace == []
    mock_submit.assert_not_called()
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_followup_status"] == "skipped"
    assert saved_state["last_followup_skipped_reason"] == "chart_first_no_requested_followup"
    assert any(
        "followup=skip(chart_first_no_requested_followup)" in (call.kwargs.get("message") or "")
        for call in mock_update_db.call_args_list
    )


def test_txt_update_fails_when_tracking_refresh_fails():
    state_store: dict = {}
    patches = _build_common_patches()
    patches.extend(
        [
            patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
            patch("app.backend.core.txt_update_job._save_update_state"),
            patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
            patch("app.backend.services.rankings_cache.refresh_cache"),
            patch(
                "app.backend.services.signal_tracking_service.refresh_daily_tracking_window",
                side_effect=RuntimeError("tracking-broken"),
            ),
        ]
    )

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6] as mock_update_db,
        patches[7],
        patches[8],
        patches[9] as mock_save_state,
        patches[10],
        patches[11],
        patches[12],
    ):
        txt_update_job.handle_txt_update(
            "job-tracking-fail",
            {
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "run_tracking_refresh": True,
                "tracking_refresh_trigger_reason": "manual_full_refresh",
            },
        )

    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "failed"
    assert saved_state["last_failed_stage"] == "tracking_refresh"
    assert saved_state["last_error"] == "tracking-broken"
    assert saved_state["last_error_message"] == "Tracking refresh failed"
    assert any(
        call.args[2] == "failed" and call.kwargs.get("error") == "Tracking refresh failed"
        for call in mock_update_db.call_args_list
    )


def test_txt_update_preserves_pan_lock_reason_in_warning_state():
    state_store: dict = {}
    with (
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch(
            "app.backend.infra.panrolling.pan_import.run_pan_import",
            side_effect=RuntimeError("Pan import blocked by Pan-side error dialog: libStock database is in use."),
        ),
        patch("app.backend.core.txt_update_job.run_vbs_export", return_value=(0, ["SUMMARY: total=1 ok=1 err=0"])),
        patch("app.backend.core.txt_update_job.run_ingest", return_value=("", "", {"rows": "10"})),
        patch("app.backend.core.txt_update_job._run_phase_batch_latest", return_value=20260101),
        patch(
            "app.backend.core.txt_update_job._is_existing_txt_data_fresh",
            return_value=(True, "latest_txt_age_hours=0.50"),
        ),
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db") as mock_update_db,
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
        patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state") as mock_save_state,
        patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
        patch("app.backend.services.rankings_cache.refresh_cache"),
    ):
        txt_update_job.handle_txt_update(
            "job-pan-lock",
            {"auto_ml_predict": False, "auto_ml_train": False, "strict_pan_import": False, "pan_retry": 1},
        )

    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert "libStock database is in use" in saved_state["last_pan_import_warning"]
    assert any(
        call.kwargs.get("message") == "PAN import failed. Continuing with existing TXT data."
        for call in mock_update_db.call_args_list
    )


def test_txt_update_retries_transient_pan_lock_and_recovers():
    state_store: dict = {}
    with (
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch(
            "app.backend.infra.panrolling.pan_import.run_pan_import",
            side_effect=[
                RuntimeError("Pan import blocked by Pan-side error dialog: libStock database is in use."),
                True,
            ],
        ) as mock_pan_import,
        patch("app.backend.core.txt_update_job.run_vbs_export", return_value=(0, ["SUMMARY: total=1 ok=1 err=0"])),
        patch("app.backend.core.txt_update_job.run_ingest", return_value=("", "", {"rows": "10"})),
        patch("app.backend.core.txt_update_job._run_phase_batch_latest", return_value=20260101),
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db") as mock_update_db,
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
        patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state") as mock_save_state,
        patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
        patch("app.backend.services.rankings_cache.refresh_cache"),
        patch("app.backend.core.txt_update_job.time.sleep"),
    ):
        txt_update_job.handle_txt_update(
            "job-pan-lock-retry",
            {"auto_ml_predict": False, "auto_ml_train": False, "strict_pan_import": False, "pan_retry": 2},
        )

    assert mock_pan_import.call_count == 2
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_pan_import_attempts"] == 2
    assert saved_state["last_pan_import_error_kind"] == "none"
    assert "last_pan_import_warning" not in saved_state
    assert any(
        "PAN DB lock detected. Waiting" in str(call.kwargs.get("message") or "")
        for call in mock_update_db.call_args_list
    )
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)


def test_txt_update_retries_when_pan_process_is_still_active_then_recovers():
    state_store: dict = {}
    with (
        patch("app.backend.core.txt_update_job.os.path.isfile", return_value=True),
        patch(
            "app.backend.infra.panrolling.pan_import.run_pan_import",
            side_effect=[
                RuntimeError(
                    "Pan Data Manager is already running or did not exit cleanly. "
                    "Close PAN before daily update (pandtmgr.exe pids=4321)."
                ),
                True,
            ],
        ) as mock_pan_import,
        patch("app.backend.core.txt_update_job.run_vbs_export", return_value=(0, ["SUMMARY: total=1 ok=1 err=0"])),
        patch("app.backend.core.txt_update_job.run_ingest", return_value=("", "", {"rows": "10"})),
        patch("app.backend.core.txt_update_job._run_phase_batch_latest", return_value=20260101),
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_update_job.job_manager._update_db") as mock_update_db,
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
        patch("app.backend.core.txt_update_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state") as mock_save_state,
        patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
        patch("app.backend.services.rankings_cache.refresh_cache"),
        patch("app.backend.core.txt_update_job.time.sleep"),
    ):
        txt_update_job.handle_txt_update(
            "job-pan-running-retry",
            {"auto_ml_predict": False, "auto_ml_train": False, "strict_pan_import": False, "pan_retry": 2},
        )

    assert mock_pan_import.call_count == 2
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_pan_import_attempts"] == 2
    assert saved_state["last_pan_import_error_kind"] == "none"
    assert any(
        "PAN process is still active. Waiting" in str(call.kwargs.get("message") or "")
        for call in mock_update_db.call_args_list
    )
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)


def test_txt_followup_failure_does_not_revert_txt_update_success():
    state_store = {"last_pipeline_status": "success"}
    with (
        patch("app.backend.core.txt_followup_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state") as mock_save_state,
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_followup_job.job_manager._update_db") as mock_update_db,
        patch("app.backend.core.txt_followup_job.is_legacy_analysis_disabled", return_value=True),
        patch(
            "app.backend.core.analysis_prewarm_job.schedule_analysis_prewarm_if_needed",
            return_value=None,
        ),
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
        patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
        patch(
            "app.backend.services.rankings_cache.refresh_cache",
            side_effect=RuntimeError("cache-broken"),
        ),
    ):
        txt_followup_job.handle_txt_followup(
            "followup-fail",
            {
                "source_txt_job_id": "job-fast",
                "summary_line": "SUMMARY: total=1 ok=1 err=0",
                "phase_dt": 20260101,
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": False,
                "auto_walkforward_gate": False,
                "auto_fill_missing_history": False,
            },
        )

    assert mock_save_state.call_count > 0
    assert state_store["last_pipeline_status"] == "success"
    assert state_store["last_followup_status"] == "failed"
    assert state_store["last_followup_failed_stage"] == "cache_refresh"
    assert any(
        call.args[1] == "txt_followup" and call.args[2] == "failed"
        for call in mock_update_db.call_args_list
    )


def test_txt_followup_monthly_walkforward_skip_preserves_skip_state():
    current_month = txt_followup_job.datetime.now().strftime("%Y-%m")
    state_store = {
        "last_pipeline_status": "success",
        "last_walkforward_run_month_key": current_month,
        "last_walkforward_gate_month_key": current_month,
    }
    with (
        patch("app.backend.core.txt_followup_job._load_update_state", return_value=state_store),
        patch("app.backend.core.txt_update_job._save_update_state"),
        patch("app.backend.core.txt_update_job.job_manager.is_cancel_requested", return_value=False),
        patch("app.backend.core.txt_followup_job.job_manager._update_db"),
        patch("app.backend.core.txt_followup_job.is_legacy_analysis_disabled", return_value=True),
        patch(
            "app.backend.core.analysis_prewarm_job.schedule_analysis_prewarm_if_needed",
            return_value=None,
        ),
        patch("app.backend.api.dependencies.get_stock_repo", return_value=object()),
        patch("app.backend.jobs.scoring_job.ScoringJob.run", return_value=[{"code": "1301"}]),
        patch("app.backend.services.rankings_cache.refresh_cache"),
        patch("app.backend.services.strategy_backtest_service.run_strategy_walkforward") as mock_run,
        patch("app.backend.services.strategy_backtest_service.run_strategy_walkforward_gate") as mock_gate,
        patch(
            "app.backend.services.strategy_backtest_service.save_daily_walkforward_research_snapshot",
            return_value={"saved": False},
        ) as mock_research_snapshot,
    ):
        txt_followup_job.handle_txt_followup(
            "followup-skip",
            {
                "source_txt_job_id": "job-fast",
                "summary_line": "SUMMARY: total=1 ok=1 err=0",
                "phase_dt": 20260101,
                "auto_ml_predict": False,
                "auto_ml_train": False,
                "auto_walkforward_run": True,
                "auto_walkforward_gate": True,
                "auto_fill_missing_history": False,
            },
        )

    mock_run.assert_not_called()
    mock_gate.assert_not_called()
    mock_research_snapshot.assert_not_called()
    assert state_store["last_walkforward_run_skipped_reason"] == f"already_ran_month:{current_month}"
    assert state_store["last_walkforward_gate_skipped_reason"] == f"already_ran_month:{current_month}"
    assert state_store["last_followup_status"] == "success"
