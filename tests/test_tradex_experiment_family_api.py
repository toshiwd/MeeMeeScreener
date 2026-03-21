from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.backend.api.dependencies as dependencies
import app.backend.services.tradex_experiment_service as service
from app.backend.services.tradex_experiment_store import family_compare_file, family_file, run_adopt_file, run_detail_file, run_file


@dataclass(frozen=True)
class _FakeOutput:
    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return dict(self.payload)


class _FakeRepo:
    def get_analysis_timeline(self, code: str, asof_dt: int | None, limit: int = 400):
        del asof_dt, limit
        base = int(code[-1]) if code[-1].isdigit() else 0
        return [
            {"dt": 20250105, "pUp": 0.45 + base * 0.03, "pDown": 0.55 - base * 0.03, "pTurnUp": 0.2, "pTurnDown": 0.1, "ev20Net": 0.1, "sellPDown": 0.2, "sellPTurnDown": 0.1, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.01, "shortRet10": 0.02, "shortRet20": 0.03, "shortWin5": True, "shortWin10": True, "shortWin20": True},
            {"dt": 20250115, "pUp": 0.5 + base * 0.03, "pDown": 0.5 - base * 0.03, "pTurnUp": 0.25, "pTurnDown": 0.15, "ev20Net": 0.12, "sellPDown": 0.25, "sellPTurnDown": 0.15, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.02, "shortRet10": 0.03, "shortRet20": 0.04, "shortWin5": True, "shortWin10": True, "shortWin20": True},
            {"dt": 20250205, "pUp": 0.55 + base * 0.03, "pDown": 0.45 - base * 0.03, "pTurnUp": 0.3, "pTurnDown": 0.2, "ev20Net": 0.14, "sellPDown": 0.3, "sellPTurnDown": 0.2, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.03, "shortRet10": 0.04, "shortRet20": 0.05, "shortWin5": True, "shortWin10": True, "shortWin20": True},
            {"dt": 20250215, "pUp": 0.6 + base * 0.03, "pDown": 0.4 - base * 0.03, "pTurnUp": 0.35, "pTurnDown": 0.25, "ev20Net": 0.16, "sellPDown": 0.35, "sellPTurnDown": 0.25, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.04, "shortRet10": 0.05, "shortRet20": 0.06, "shortWin5": True, "shortWin10": True, "shortWin20": True},
        ]


def _fake_run_tradex_analysis(input_contract):
    p_up = float(input_contract.analysis_p_up or 0.0)
    ready = p_up >= 0.6
    bias = "buy" if p_up >= 0.55 else "sell"
    return _FakeOutput(
        {
            "symbol": input_contract.symbol,
            "asof": input_contract.asof,
            "side_ratios": {"buy": p_up, "neutral": 1.0 - p_up, "sell": max(0.0, 1.0 - p_up - (1.0 - p_up))},
            "confidence": p_up,
            "reasons": [f"tone={bias}", "pattern=small-start", "version=v1"],
            "candidate_comparisons": [
                {
                    "candidate_key": "buy",
                    "baseline_key": "balanced",
                    "comparison_scope": "decision_scenarios",
                    "score": p_up,
                    "score_delta": p_up - 0.5,
                    "rank": 1,
                    "reasons": [f"key={input_contract.symbol}", "label=trend", f"tone={bias}"],
                    "publish_ready": ready,
                }
            ],
            "publish_readiness": {"ready": ready, "status": "ready" if ready else "not_evaluated", "reasons": ["ok"] if ready else ["weak"], "candidate_key": input_contract.symbol},
            "override_state": {"present": False, "source": None, "logic_key": None, "logic_version": None, "reason": None},
            "source": "tradex_analysis",
            "schema_version": "tradex_analysis_output_v1",
        }
    )


def _build_app() -> FastAPI:
    from app.backend.api.routers.tradex import router

    app = FastAPI()
    app.include_router(router)
    return app


def test_tradex_family_run_compare_detail_and_adopt_flow(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)

    universe = [f"10{idx:02d}" for idx in range(1, 21)]
    payload = {
        "family_name": "small-start",
        "universe": universe,
        "period": {
            "segments": [
                {"label": "phase-1", "start_date": "2025-01-01", "end_date": "2025-01-31"},
                {"label": "phase-2", "start_date": "2025-02-01", "end_date": "2025-02-28"},
            ]
        },
        "baseline_plan": {
            "plan_id": "baseline",
            "plan_version": "v1",
            "label": "Baseline",
            "minimum_confidence": 0.78,
            "signal_bias": "balanced",
            "top_k": 3,
        },
        "candidate_plans": [
            {"plan_id": "candidate-a", "plan_version": "v1", "label": "Candidate A", "minimum_confidence": 0.55, "signal_bias": "buy", "top_k": 3}
        ],
    }

    with TestClient(_build_app()) as client:
        create_response = client.post("/api/tradex/families", json=payload)
        assert create_response.status_code == 200
        family = create_response.json()["family"]
        family_id = family["family_id"]

        baseline_response = client.post(f"/api/tradex/families/{family_id}/runs", json={"run_kind": "baseline", "notes": "baseline"})
        assert baseline_response.status_code == 200
        baseline_run = baseline_response.json()["run"]
        assert baseline_run["status"] == "succeeded"

        candidate_response = client.post(
            f"/api/tradex/families/{family_id}/runs",
            json={"run_kind": "candidate", "plan_id": "candidate-a", "notes": "candidate"},
        )
        assert candidate_response.status_code == 200
        candidate_run = candidate_response.json()["run"]
        assert candidate_run["status"] == "compared"
        run_id = candidate_run["run_id"]

        compare_response = client.get(f"/api/tradex/families/{family_id}/compare")
        assert compare_response.status_code == 200
        compare = compare_response.json()["compare"]
        assert compare["baseline_run_id"] == baseline_run["run_id"]
        assert compare["candidate_results"][0]["run_id"] == run_id
        assert compare["candidate_results"][0]["status"] == "compared"

        detail_code = list(candidate_run["analysis"]["by_code"].keys())[0]
        detail_response = client.get(f"/api/tradex/runs/{run_id}/detail", params={"code": detail_code})
        assert detail_response.status_code == 200
        detail = detail_response.json()["detail"]
        assert detail["code"] == detail_code
        assert detail["summary"]["top_reasons"]

        adopt_response = client.post(
            "/api/tradex/adopt",
            json={"family_id": family_id, "run_id": run_id, "reason": "adopt candidate", "actor": "test"},
        )
        assert adopt_response.status_code == 200
        adopt = adopt_response.json()
        assert adopt["status"] == "adopt_candidate"
        assert adopt["gate"]["pass"] is True

        family_response = client.get(f"/api/tradex/families/{family_id}")
        assert family_response.status_code == 200
        refreshed_family = family_response.json()["family"]
        assert refreshed_family["status_summary"]["candidate_runs"] == 1

    assert family_file(family_id).exists()
    assert family_file(family_id).with_name("baseline.lock.json").exists()
    assert family_compare_file(family_id).exists()
    assert run_file(family_id, run_id).exists()
    assert run_detail_file(family_id, run_id, detail_code).exists()
    assert run_adopt_file(family_id, run_id).exists()
