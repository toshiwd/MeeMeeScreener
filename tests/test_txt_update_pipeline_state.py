import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.core import txt_update_job


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


def test_txt_update_success_records_cache_refresh_stage():
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
    ):
        txt_update_job.handle_txt_update("job-ok", {"auto_ml_predict": False, "auto_ml_train": False})

    assert stage_trace == ["scoring", "cache_refresh"]
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pipeline_status"] == "success"
    assert saved_state["last_pipeline_stage"] == "finalize"
    assert "last_cache_refresh_at" in saved_state
    assert "last_txt_update_at" in saved_state
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)


def test_txt_update_forces_recompute_when_pan_finalize_detected():
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
        ),
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

    assert mock_train.call_count == 1
    assert mock_predict.call_count == 1
    assert mock_guard.call_count == 1
    assert mock_walkforward_run.call_count == 1
    assert mock_walkforward_gate.call_count == 1
    assert mock_save_state.call_count > 0
    saved_state = mock_save_state.call_args[0][0]
    assert saved_state["last_pan_finalize_rows"] == 1
    assert "last_forced_recompute_at" in saved_state
    assert any(call.args[2] == "success" for call in mock_update_db.call_args_list)
