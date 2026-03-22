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
            "diagnostics": dict(input_contract.diagnostics or {}),
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
        "probes": [
            {"probe_id": "probe-1", "code": "1001", "date": "2025-01-05", "label": "probe-1"},
            {"probe_id": "probe-2", "code": "1002", "date": "2025-01-15", "label": "probe-2"},
            {"probe_id": "probe-3", "code": "1003", "date": "2025-02-05", "label": "probe-3"},
        ],
        "baseline_plan": {
            "plan_id": "baseline",
            "plan_version": "v1",
            "label": "Baseline",
            "minimum_confidence": 0.78,
            "signal_bias": "balanced",
            "top_k": 3,
        },
        "candidate_plans": [
            {"plan_id": "candidate-a", "plan_version": "v1", "label": "Candidate A / stronger", "minimum_confidence": 0.58, "minimum_ready_rate": 0.55, "signal_bias": "balanced", "top_k": 4},
            {"plan_id": "candidate-b", "plan_version": "v1", "label": "Candidate B / simpler", "minimum_confidence": 0.36, "minimum_ready_rate": 0.25, "signal_bias": "balanced", "top_k": 2},
            {"plan_id": "candidate-c", "plan_version": "v1", "label": "Candidate C / alternative", "minimum_confidence": 0.48, "minimum_ready_rate": 0.4, "signal_bias": "sell", "top_k": 3},
        ],
    }

    with TestClient(_build_app()) as client:
        create_response = client.post("/api/tradex/families", json=payload)
        assert create_response.status_code == 200
        family = create_response.json()["family"]
        family_id = family["family_id"]
        assert len(family["probes"]) == 3

        baseline_response = client.post(f"/api/tradex/families/{family_id}/runs", json={"run_kind": "baseline", "notes": "baseline"})
        assert baseline_response.status_code == 200
        baseline_run = baseline_response.json()["run"]
        assert baseline_run["status"] == "succeeded"
        assert baseline_run["effective_config"]["plan_id"] == "baseline"
        assert len(baseline_run["effective_config"]["plan_hash"]) == 64
        assert baseline_run["engine_diagnostics"]["plan_effective"]["plan_id"] == "baseline"
        assert len(baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]) == 64
        assert len(baseline_run["engine_diagnostics"]["probe"]["engine_plan_hash"]) == 64
        assert baseline_run["engine_diagnostics"]["probe"]["feature_hash"] == baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]
        assert len(baseline_run["engine_diagnostics"]["probes"]) == 3
        assert baseline_run["readiness_summary"]["sample_count"] > 0
        assert "ready_pre_gate_rate" in baseline_run["readiness_summary"]
        assert "raw_readiness_score" in baseline_run["readiness_summary"]
        assert "gate_reason_counts" in baseline_run["readiness_summary"]
        assert baseline_run["readiness_summary"]["gate_reason_counts"]
        assert any(
            key in baseline_run["readiness_summary"]["gate_reason_counts"]
            for key in ("confidence_below_threshold", "minimum_ready_rate_not_met", "other_fallback")
        )

        candidate_a_response = client.post(f"/api/tradex/families/{family_id}/runs", json={"run_kind": "candidate", "plan_id": "candidate-a", "notes": "candidate-a"})
        candidate_b_response = client.post(f"/api/tradex/families/{family_id}/runs", json={"run_kind": "candidate", "plan_id": "candidate-b", "notes": "candidate-b"})
        candidate_c_response = client.post(f"/api/tradex/families/{family_id}/runs", json={"run_kind": "candidate", "plan_id": "candidate-c", "notes": "candidate-c"})
        assert candidate_a_response.status_code == 200
        assert candidate_b_response.status_code == 200
        assert candidate_c_response.status_code == 200

        candidate_a_run = candidate_a_response.json()["run"]
        candidate_b_run = candidate_b_response.json()["run"]
        candidate_c_run = candidate_c_response.json()["run"]
        assert candidate_a_run["status"] == "compared"
        assert candidate_b_run["status"] == "compared"
        assert candidate_c_run["status"] in {"compared", "rejected"}
        assert candidate_a_run["effective_config"]["plan_hash"] != baseline_run["effective_config"]["plan_hash"]
        assert candidate_b_run["effective_config"]["plan_hash"] != baseline_run["effective_config"]["plan_hash"]
        assert candidate_c_run["effective_config"]["plan_hash"] != baseline_run["effective_config"]["plan_hash"]
        assert candidate_a_run["engine_diagnostics"]["plan_effective"]["plan_id"] == "candidate-a"
        assert candidate_a_run["engine_diagnostics"]["probe"]["engine_plan_hash"] != baseline_run["engine_diagnostics"]["probe"]["engine_plan_hash"]
        assert candidate_a_run["engine_diagnostics"]["probe"]["engine_input_hash"] == baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]
        assert candidate_a_run["engine_diagnostics"]["probe"]["publish_not_ready_reason_label"] is not None
        assert len(candidate_a_run["engine_diagnostics"]["probes"]) == 3
        assert "readiness_summary" in candidate_a_run
        assert candidate_a_run["readiness_summary"]["sample_count"] > 0

        slot_response = client.post(f"/api/tradex/families/{family_id}/runs", json={"run_kind": "candidate", "plan_id": "candidate-a", "notes": "slot-check"})
        assert slot_response.status_code == 400
        assert slot_response.json()["detail"]["reason"] == "candidate run limit reached"

        compare_response = client.get(f"/api/tradex/families/{family_id}/compare")
        assert compare_response.status_code == 200
        compare = compare_response.json()["compare"]
        assert compare["baseline_run_id"] == baseline_run["run_id"]
        assert len(compare["candidate_results"]) == 3

        candidate_runs = {item["plan_id"]: item for item in compare["candidate_results"]}
        assert candidate_runs["candidate-a"]["status"] == "compared"
        assert candidate_runs["candidate-b"]["status"] == "compared"
        assert candidate_runs["candidate-c"]["status"] == "compared"
        assert len(candidate_runs["candidate-a"]["absolute_metric_comparisons"]) == 3
        assert len(candidate_runs["candidate-a"]["review_focus"]) == 5
        assert candidate_runs["candidate-a"]["metric_directions"]["symbol_concentration"] == "lower"
        assert "diagnostics" in candidate_runs["candidate-a"]
        assert len(candidate_runs["candidate-a"]["diagnostics"]["row_diffs"]) > 0
        probe = candidate_runs["candidate-a"]["diagnostics"]["probe_row_comparison"]
        assert probe["baseline"]["engine_input_hash"] == probe["candidate"]["engine_input_hash"]
        assert probe["baseline"]["engine_plan_hash"] != probe["candidate"]["engine_plan_hash"]
        assert probe["baseline"]["raw_readiness_score"] == probe["candidate"]["raw_readiness_score"]
        assert "publish_not_ready_reasons" in probe["candidate"]
        assert "baseline_effective_config" in candidate_runs["candidate-a"]["diagnostics"]
        assert "candidate_readiness_summary" in candidate_runs["candidate-a"]["diagnostics"]
        assert candidate_runs["candidate-a"]["diagnostics"]["baseline_effective_config"]["plan_hash"] == baseline_run["effective_config"]["plan_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["candidate_effective_config"]["plan_hash"] == candidate_a_run["effective_config"]["plan_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["baseline_engine_diagnostics"]["probe"]["engine_input_hash"] == baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["candidate_engine_diagnostics"]["probe"]["engine_plan_hash"] == candidate_a_run["engine_diagnostics"]["probe"]["engine_plan_hash"]
        assert len(candidate_runs["candidate-a"]["diagnostics"]["probe_row_comparisons"]) == 3
        assert len(probe["baseline"]["engine_input_hash"]) == 64
        assert len(probe["candidate"]["engine_input_hash"]) == 64
        assert probe["delta"]["engine_plan_hash_changed"] is True

        candidate_run = candidate_runs["candidate-a"]
        run_id = candidate_run["run_id"]
        detail_code = candidate_run["review_focus"][0]["code"]
        detail_path = run_detail_file(family_id, run_id, detail_code)
        detail_response = client.get(f"/api/tradex/runs/{run_id}/detail", params={"code": detail_code})
        assert detail_response.status_code == 200
        detail = detail_response.json()["detail"]
        assert detail["code"] == detail_code
        assert detail["summary"]["top_reasons"]
        first_mtime = detail_path.stat().st_mtime_ns
        repeat_response = client.get(f"/api/tradex/runs/{run_id}/detail", params={"code": detail_code})
        assert repeat_response.status_code == 200
        assert detail_path.stat().st_mtime_ns == first_mtime

        adopt_response = client.post(
            "/api/tradex/adopt",
            json={"family_id": family_id, "run_id": candidate_c_run["run_id"], "reason": "adopt candidate", "actor": "test"},
        )
        assert adopt_response.status_code == 200
        adopt = adopt_response.json()
        assert adopt["status"] == "rejected"
        assert adopt["gate"]["pass"] is False
        assert adopt["gate"]["reasons"]

        family_response = client.get(f"/api/tradex/families/{family_id}")
        assert family_response.status_code == 200
        refreshed_family = family_response.json()["family"]
        assert refreshed_family["status_summary"]["candidate_runs"] == 3

    assert family_file(family_id).exists()
    assert family_file(family_id).with_name("baseline.lock.json").exists()
    assert family_compare_file(family_id).exists()
    assert run_file(family_id, run_id).exists()
    assert run_detail_file(family_id, run_id, detail_code).exists()
    assert run_adopt_file(family_id, candidate_c_run["run_id"]).exists()
