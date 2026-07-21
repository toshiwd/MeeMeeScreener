from app.backend.api.routers import system


class _Config:
    def load_update_state(self):
        return {
            "last_txt_update_at": "2026-06-30T10:00:00",
            "last_pipeline_status": "success",
            "last_pipeline_stage": "finalize",
            "last_pipeline_stage_durations": {
                "refresh_ml_features": {
                    "duration_sec": 1.25,
                    "status": "done",
                    "rows": 100,
                }
            },
        }


def test_system_status_exposes_pipeline_stage_durations(monkeypatch):
    monkeypatch.setattr(system.strategy_backtest_service, "get_latest_strategy_walkforward", lambda: None)
    monkeypatch.setattr(system.strategy_backtest_service, "get_latest_strategy_walkforward_gate", lambda: None)

    payload = system.get_system_status(config=_Config())

    assert payload["pipeline"]["durations"]["refresh_ml_features"]["duration_sec"] == 1.25
    assert payload["pipeline"]["durations"]["refresh_ml_features"]["rows"] == 100
