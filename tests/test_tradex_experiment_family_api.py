from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

import app.backend.api.dependencies as dependencies
from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV
import app.backend.services.tradex_experiment_service as service
import app.backend.tools.tradex_research_runner as research_runner
import app.backend.tools.tradex_data_smoke_check as smoke_check
from app.backend.services.tradex_experiment_store import family_compare_file, family_file, run_adopt_file, run_detail_file, run_file
from external_analysis.contracts.analysis_input import AnalysisInputContract


@dataclass(frozen=True)
class _FakeOutput:
    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return dict(self.payload)


class _FakeRepo:
    def get_all_codes(self):
        return [f"10{idx:02d}" for idx in range(1, 61)]

    def get_analysis_timeline(self, code: str, asof_dt: int | None, limit: int = 400):
        del asof_dt, limit
        base = int(code[-1]) if code[-1].isdigit() else 0
        return [
            {"dt": 20250105, "pUp": 0.45 + base * 0.03, "pDown": 0.55 - base * 0.03, "pTurnUp": 0.2, "pTurnDown": 0.1, "ev20Net": 0.1, "sellPDown": 0.2, "sellPTurnDown": 0.1, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.01, "shortRet10": 0.02, "shortRet20": 0.03, "shortWin5": True, "shortWin10": True, "shortWin20": True},
            {"dt": 20250115, "pUp": 0.5 + base * 0.03, "pDown": 0.5 - base * 0.03, "pTurnUp": 0.25, "pTurnDown": 0.15, "ev20Net": 0.12, "sellPDown": 0.25, "sellPTurnDown": 0.15, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.02, "shortRet10": 0.03, "shortRet20": 0.04, "shortWin5": True, "shortWin10": True, "shortWin20": True},
            {"dt": 20250205, "pUp": 0.55 + base * 0.03, "pDown": 0.45 - base * 0.03, "pTurnUp": 0.3, "pTurnDown": 0.2, "ev20Net": 0.14, "sellPDown": 0.3, "sellPTurnDown": 0.2, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.03, "shortRet10": 0.04, "shortRet20": 0.05, "shortWin5": True, "shortWin10": True, "shortWin20": True},
            {"dt": 20250215, "pUp": 0.6 + base * 0.03, "pDown": 0.4 - base * 0.03, "pTurnUp": 0.35, "pTurnDown": 0.25, "ev20Net": 0.16, "sellPDown": 0.35, "sellPTurnDown": 0.25, "trendDown": False, "trendDownStrict": False, "shortRet5": 0.04, "shortRet10": 0.05, "shortRet20": 0.06, "shortWin5": True, "shortWin10": True, "shortWin20": True},
        ]

    def get_daily_bars(self, code: str, limit: int = 400, asof_dt: int | None = None):
        del code, asof_dt
        rows = []
        current = date(2024, 12, 2)
        while len(rows) < 140:
            if current.weekday() < 5:
                idx = len(rows)
                dt = int(current.strftime("%Y%m%d"))
                close = 100.0 + idx
                rows.append((dt, close - 1.5, close + 1.5, close - 2.5, close, 1_000_000.0))
            current += timedelta(days=1)
        return rows[-max(1, min(limit, len(rows))):]


def _fake_regime_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    current = date(2025, 1, 1)
    for regime_id, regime_tag in (("risk_on_trend", "up"), ("risk_off_trend", "down"), ("neutral_range", "flat")):
        for _ in range(60):
            rows.append(
                {
                    "dt": int(current.strftime("%Y%m%d")),
                    "regime_id": regime_id,
                    "regime_tag": regime_tag,
                    "regime_score": 0.1,
                    "label_version": service.TRADEX_EVAL_REGIME_LABEL_VERSION,
                }
            )
            current += timedelta(days=1)
    return rows


@pytest.fixture(autouse=True)
def _enable_legacy_analysis_for_tradex_research(monkeypatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")


def _make_summary(
    *,
    top5_mean: float,
    top5_median: float,
    top10_mean: float,
    top10_median: float,
    monthly_mean: float,
    monthly_median: float,
    zero_pass_months: int,
    turnover: float,
    dd: float,
    liquidity_mean: float,
    regime_means: list[float],
    monthly_model_means: list[float],
    liquidity_fail_rate: float = 0.0,
    sample_count: int = 12,
) -> dict[str, object]:
    months = [{"month": f"2025-{idx + 1:02d}", "model_ret20_mean": value} for idx, value in enumerate(monthly_model_means)]
    return {
        "sample_count": sample_count,
        "groups": {
            "top5": {
                "ret_20": {"mean": top5_mean, "median": top5_median, "trimmed_mean": top5_mean},
                "liquidity20d": {"mean": liquidity_mean, "median": liquidity_mean, "trimmed_mean": liquidity_mean},
                "liquidity_fail_rate": liquidity_fail_rate,
            },
            "top10": {
                "ret_20": {"mean": top10_mean, "median": top10_median, "trimmed_mean": top10_mean},
                "liquidity20d": {"mean": liquidity_mean, "median": liquidity_mean, "trimmed_mean": liquidity_mean},
                "liquidity_fail_rate": liquidity_fail_rate,
            },
        },
        "monthly_top5_capture": {
            "mean": monthly_mean,
            "median": monthly_median,
            "months": months,
        },
        "zero_pass_months": zero_pass_months,
        "turnover_proxy": turnover,
        "dd_proxy": dd,
        "regime_summary": [{"metrics": {"ret_20": {"mean": value}}} for value in regime_means],
    }


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
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
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
                {"probe_id": "probe-1", "code": "1007", "date": "2025-01-06", "label": "probe-1"},
                {"probe_id": "probe-2", "code": "1008", "date": "2025-01-15", "label": "probe-2"},
                {"probe_id": "probe-3", "code": "1009", "date": "2025-02-05", "label": "probe-3"},
            ],
        "baseline_plan": {
            "plan_id": "baseline",
            "plan_version": "v1",
            "label": "Baseline",
            "method_id": "champion_current_ranking",
            "method_title": "現行ランキング",
            "method_thesis": "現行のTRADEX標準順位をそのまま再現する。",
            "method_family": "champion",
            "minimum_confidence": 0.78,
            "signal_bias": "balanced",
            "top_k": 3,
        },
        "candidate_plans": [
            {"plan_id": "candidate-a", "plan_version": "v1", "label": "Candidate A / stronger", "method_id": "candidate_a", "method_title": "候補A", "method_thesis": "現行より少し強めに寄せる。", "method_family": "family-a", "minimum_confidence": 0.58, "minimum_ready_rate": 0.55, "signal_bias": "balanced", "top_k": 4},
            {"plan_id": "candidate-b", "plan_version": "v1", "label": "Candidate B / simpler", "method_id": "candidate_b", "method_title": "候補B", "method_thesis": "単純化してノイズを減らす。", "method_family": "family-b", "minimum_confidence": 0.36, "minimum_ready_rate": 0.25, "signal_bias": "balanced", "top_k": 2},
            {"plan_id": "candidate-c", "plan_version": "v1", "label": "Candidate C / alternative", "method_id": "candidate_c", "method_title": "候補C", "method_thesis": "逆風時の無駄な通過を減らす。", "method_family": "family-c", "minimum_confidence": 0.48, "minimum_ready_rate": 0.4, "signal_bias": "sell", "top_k": 3},
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
        assert baseline_run["method_title"] == "現行ランキング"
        assert baseline_run["method_family"] == "champion"
        assert len(baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]) == 64
        assert len(baseline_run["engine_diagnostics"]["probe"]["engine_plan_hash"]) == 64
        assert baseline_run["engine_diagnostics"]["probe"]["feature_hash"] == baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]
        assert len(baseline_run["engine_diagnostics"]["probes"]) >= 2
        assert baseline_run["readiness_summary"]["sample_count"] > 0
        assert "ready_pre_gate_rate" in baseline_run["readiness_summary"]
        assert "raw_readiness_score" in baseline_run["readiness_summary"]
        assert "gate_reason_counts" in baseline_run["readiness_summary"]
        assert baseline_run["readiness_summary"]["gate_reason_counts"]
        assert any(
            key in baseline_run["readiness_summary"]["gate_reason_counts"]
            for key in ("confidence_below_threshold", "minimum_ready_rate_not_met", "other_fallback")
        )
        assert baseline_run["waterfall_summary"]["sample_count"] == baseline_run["readiness_summary"]["sample_count"]
        assert baseline_run["waterfall_summary"]["stage_counts"]["retrieved"] == baseline_run["waterfall_summary"]["sample_count"]
        assert "shadow_gate" in baseline_run["waterfall_summary"]
        assert baseline_run["diagnostics_schema_version"] == service.TRADEX_DIAGNOSTICS_SCHEMA_VERSION
        assert baseline_run["selection_summary"]["groups"]["top5"]["analysis_ev_net"]["mean"] is not None
        assert baseline_run["selection_summary"]["kind"] == "proxy"
        assert baseline_run["selection_summary"]["source"] == "timeline_metrics"
        assert baseline_run["selection_summary"]["selection_variant"] == "champion"
        assert baseline_run["selection_challenger_summary"]["selection_variant"] == "challenger"
        assert baseline_run["selection_summary"]["monthly_top5_capture"]["kind"] == "proxy"
        assert baseline_run["selection_summary"]["monthly_top5_capture"]["source"] == "timeline_metrics"
        assert baseline_run["selection_summary"]["monthly_top5_capture"]["definition"]
        assert baseline_run["selection_summary"]["monthly_top5_capture"]["month_count"] > 0
        assert baseline_run["selection_summary"]["monthly_top5_capture"]["months"]
        assert baseline_run["selection_challenger_summary"]["monthly_top5_capture"]["month_count"] > 0
        first_sample = baseline_run["metrics"]["samples"][0]
        assert len(first_sample["ranking_input_hash"]) == 64
        assert first_sample["ranking_input_hash"] == service._ranking_input_hash(first_sample)
        assert first_sample["liquidity20d"] is not None
        assert first_sample["liquidity20d_source"] == "daily_bars_20d"
        shadow_variant = dict(first_sample)
        shadow_variant["shadow_gate"] = dict(first_sample["shadow_gate"])
        shadow_variant["shadow_gate"]["reason"] = "gate_rule_fail"
        shadow_variant["waterfall"] = dict(first_sample["waterfall"])
        shadow_variant["waterfall"]["shadow_gate"] = shadow_variant["shadow_gate"]
        base_compact = service._compact_sample(first_sample)
        shadow_compact = service._compact_sample(shadow_variant)
        assert service._ranking_input_hash(first_sample) == service._ranking_input_hash(shadow_variant)
        assert base_compact["signal"] == shadow_compact["signal"]
        assert base_compact["publish_ready"] == shadow_compact["publish_ready"]
        assert base_compact["gate_reason"] == shadow_compact["gate_reason"]

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
        assert candidate_a_run["method_title"] == "候補A"
        assert candidate_a_run["engine_diagnostics"]["probe"]["engine_plan_hash"] != baseline_run["engine_diagnostics"]["probe"]["engine_plan_hash"]
        assert candidate_a_run["engine_diagnostics"]["probe"]["engine_input_hash"] == baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]
        assert candidate_a_run["engine_diagnostics"]["probe"]["publish_not_ready_reason_label"] is not None
        assert len(candidate_a_run["engine_diagnostics"]["probes"]) >= 2
        assert "readiness_summary" in candidate_a_run
        assert candidate_a_run["readiness_summary"]["sample_count"] > 0
        assert candidate_a_run["waterfall_summary"]["shadow_gate"]["pass_count"] <= candidate_a_run["waterfall_summary"]["sample_count"]
        assert candidate_a_run["selection_summary"]["groups"]["top10"]["ret_20"]["mean"] is not None
        assert candidate_a_run["selection_challenger_summary"]["selection_variant"] == "challenger"

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
        assert "baseline_waterfall_summary" in candidate_runs["candidate-a"]["diagnostics"]
        assert "candidate_selection_summary" in candidate_runs["candidate-a"]["diagnostics"]
        assert "baseline_challenger_selection_summary" in candidate_runs["candidate-a"]["diagnostics"]
        assert "candidate_challenger_selection_summary" in candidate_runs["candidate-a"]["diagnostics"]
        assert "selection_compare" in candidate_runs["candidate-a"]["diagnostics"]
        assert candidate_runs["candidate-a"]["baseline_method"]["method_title"] == "現行ランキング"
        assert candidate_runs["candidate-a"]["candidate_method"]["method_title"] == "候補A"
        assert candidate_runs["candidate-a"]["evaluation_summary"]["candidate_method"]["method_title"] == "候補A"
        assert candidate_runs["candidate-a"]["diagnostics"]["diagnostics_schema_version"] == service.TRADEX_DIAGNOSTICS_SCHEMA_VERSION
        probe = candidate_runs["candidate-a"]["diagnostics"]["probe_row_comparison"]
        assert probe["baseline"]["engine_input_hash"] == probe["candidate"]["engine_input_hash"]
        assert probe["baseline"]["engine_plan_hash"] != probe["candidate"]["engine_plan_hash"]
        assert len(probe["baseline"]["ranking_input_hash"]) == 64
        assert len(probe["candidate"]["ranking_input_hash"]) == 64
        assert probe["baseline"]["raw_readiness_score"] == probe["candidate"]["raw_readiness_score"]
        assert "publish_not_ready_reasons" in probe["candidate"]
        assert "shadow_gate" in probe["candidate"]
        assert "baseline_effective_config" in candidate_runs["candidate-a"]["diagnostics"]
        assert "candidate_readiness_summary" in candidate_runs["candidate-a"]["diagnostics"]
        assert candidate_runs["candidate-a"]["diagnostics"]["baseline_effective_config"]["plan_hash"] == baseline_run["effective_config"]["plan_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["candidate_effective_config"]["plan_hash"] == candidate_a_run["effective_config"]["plan_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["baseline_engine_diagnostics"]["probe"]["engine_input_hash"] == baseline_run["engine_diagnostics"]["probe"]["engine_input_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["candidate_engine_diagnostics"]["probe"]["engine_plan_hash"] == candidate_a_run["engine_diagnostics"]["probe"]["engine_plan_hash"]
        assert candidate_runs["candidate-a"]["diagnostics"]["baseline_waterfall_summary"]["sample_count"] == baseline_run["waterfall_summary"]["sample_count"]
        assert len(candidate_runs["candidate-a"]["diagnostics"]["probe_row_comparisons"]) >= 2
        assert len(probe["baseline"]["engine_input_hash"]) == 64
        assert len(probe["candidate"]["engine_input_hash"]) == 64
        assert probe["delta"]["engine_plan_hash_changed"] is True
        assert probe["baseline"]["shadow_gate"]["reason"] is not None
        assert probe["candidate"]["shadow_gate"]["reason"] is not None
        assert compare["diagnostics_schema_version"] == service.TRADEX_DIAGNOSTICS_SCHEMA_VERSION
        assert candidate_runs["candidate-a"]["selection_compare"]["champion_topk_ret20_mean"] is not None
        assert candidate_runs["candidate-a"]["selection_compare"]["challenger_topk_ret20_mean"] is not None
        assert candidate_runs["candidate-a"]["selection_compare"]["promote_ready"] is False
        assert candidate_runs["candidate-a"]["selection_compare"]["champion_monthly_top5_capture"]["kind"] == "proxy"
        assert candidate_runs["candidate-a"]["selection_compare"]["challenger_monthly_top5_capture"]["kind"] == "proxy"
        assert candidate_runs["candidate-a"]["evaluation_summary"]["evaluation_window_count"] == 3
        assert candidate_runs["candidate-a"]["evaluation_summary"]["regime_tag"] == "multi_regime"
        assert len(candidate_runs["candidate-a"]["evaluation_summary"]["windows"]) == 3
        assert candidate_runs["candidate-a"]["evaluation_summary"]["report_path"]
        assert Path(candidate_runs["candidate-a"]["evaluation_summary"]["report_path"]).exists()
        assert Path(candidate_runs["candidate-a"]["evaluation_summary"]["latest_report_path"]).exists()
        rerun_eval = service.run_tradex_champion_challenger_evaluation(family_id, candidate_a_run["run_id"], emit_report=False)
        assert rerun_eval is not None
        assert rerun_eval["promote_ready"] == candidate_runs["candidate-a"]["promote_ready"]
        assert rerun_eval["evaluation_window_count"] == 3
        rerun_run = client.get(f"/api/tradex/runs/{candidate_a_run['run_id']}").json()["run"]
        assert rerun_run["effective_config"]["plan_hash"] == candidate_a_run["effective_config"]["plan_hash"]
        assert rerun_run["selection_summary"]["selection_variant"] == "champion"

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


def test_tradex_diagnostic_helpers_prioritize_liquidity_and_ignore_shadow_hash() -> None:
    base_sample = {
        "code": "1001",
        "date": "2025-01-05",
        "feature_hash": "feature-hash",
        "engine_input_hash": "engine-input-hash",
        "engine_plan_hash": "engine-plan-hash",
        "engine_feature_flags": {"flag": "on"},
        "engine_scoring_params": {"analysis_ev_net": 0.12},
        "engine_readiness_params": {"minimum_confidence": 0.5},
        "input": {"symbol": "1001", "asof": "2025-01-05"},
    }
    ranking_hash = service._ranking_input_hash(base_sample)
    shadow_variant = dict(base_sample)
    shadow_variant["shadow_gate"] = {"pass": False, "reason": "gate_rule_fail", "failure_stage": "gate_pass", "reason_order": ["gate_rule_fail"]}
    shadow_variant["waterfall"] = {"shadow_gate": shadow_variant["shadow_gate"]}
    assert service._ranking_input_hash(base_sample) == ranking_hash
    assert service._ranking_input_hash(shadow_variant) == ranking_hash

    ready_contract = AnalysisInputContract(
        symbol="1001",
        asof="2025-01-05",
        analysis_p_up=0.6,
        analysis_p_down=0.4,
        analysis_p_turn_up=0.2,
        analysis_p_turn_down=0.1,
        analysis_ev_net=0.12,
        playbook_up_score_bonus=0.0,
        playbook_down_score_bonus=0.0,
        sell_analysis={"pDown": 0.2, "pTurnDown": 0.1, "trendDown": False, "trendDownStrict": False},
        diagnostics={"liquidity20d": 1.0},
    )
    output = {
        "confidence": 0.9,
        "reasons": ["environment=ok"],
        "publish_readiness": {"ready": True},
        "candidate_comparisons": [{"rank": 1, "publish_ready": True}],
        "tone": "neutral",
        "diagnostics": {"liquidity20d": 1.0},
    }
    plan = {"minimum_confidence": 0.5, "signal_bias": "balanced", "top_k": 3}
    assert service._publish_not_ready_reasons(ready_contract, output, plan) == ["liquidity_fail"]

    output_without_liquidity = dict(output)
    output_without_liquidity["diagnostics"] = {}
    liquidity_missing_contract = AnalysisInputContract(
        symbol="1003",
        asof="2025-01-05",
        analysis_p_up=0.6,
        analysis_p_down=0.4,
        analysis_p_turn_up=0.2,
        analysis_p_turn_down=0.1,
        analysis_ev_net=0.12,
        playbook_up_score_bonus=0.0,
        playbook_down_score_bonus=0.0,
        sell_analysis={"pDown": 0.2, "pTurnDown": 0.1, "trendDown": False, "trendDownStrict": False},
        diagnostics={},
    )
    missing_liquidity_reasons = service._publish_not_ready_reasons(liquidity_missing_contract, output_without_liquidity, plan)
    assert missing_liquidity_reasons[0] == "missing_feature"
    assert service._waterfall_reason_order(missing_liquidity_reasons) == ["data_missing"]

    missing_contract = AnalysisInputContract(
        symbol="1002",
        asof="2025-01-05",
        analysis_p_up=None,
        analysis_p_down=0.4,
        analysis_p_turn_up=0.2,
        analysis_p_turn_down=0.1,
        analysis_ev_net=0.12,
        playbook_up_score_bonus=0.0,
        playbook_down_score_bonus=0.0,
        sell_analysis={"pDown": 0.2, "pTurnDown": 0.1, "trendDown": False, "trendDownStrict": False},
        diagnostics={"liquidity20d": 1.0},
    )
    missing_reasons = service._publish_not_ready_reasons(missing_contract, output, plan)
    assert missing_reasons[0] == "missing_feature"
    assert service._waterfall_reason_order(missing_reasons) == ["data_missing", "liquidity_fail"]
    waterfall = service._sample_waterfall({
        "signal": True,
        "publish_ready": False,
        "publish_not_ready_reasons": missing_reasons,
    })
    assert waterfall["failure_reason"] == "data_missing"
    assert waterfall["shadow_gate"]["reason"] == "data_missing"


def test_tradex_champion_challenger_promotion_gate() -> None:
    champion = _make_summary(
        top5_mean=0.10,
        top5_median=0.09,
        top10_mean=0.08,
        top10_median=0.07,
        monthly_mean=0.40,
        monthly_median=0.40,
        zero_pass_months=2,
        turnover=0.35,
        dd=0.12,
        liquidity_mean=60_000_000.0,
        regime_means=[0.10, 0.04, 0.08],
        monthly_model_means=[0.05, 0.06, 0.07],
    )
    challenger_better = _make_summary(
        top5_mean=0.14,
        top5_median=0.12,
        top10_mean=0.10,
        top10_median=0.09,
        monthly_mean=0.45,
        monthly_median=0.44,
        zero_pass_months=1,
        turnover=0.30,
        dd=0.08,
        liquidity_mean=75_000_000.0,
        regime_means=[0.12, 0.07, 0.11],
        monthly_model_means=[0.07, 0.08, 0.09],
    )
    challenger_worse = _make_summary(
        top5_mean=0.05,
        top5_median=0.04,
        top10_mean=0.03,
        top10_median=0.02,
        monthly_mean=0.20,
        monthly_median=0.18,
        zero_pass_months=4,
        turnover=0.50,
        dd=0.25,
        liquidity_mean=20_000_000.0,
        regime_means=[0.04, 0.02, 0.03],
        monthly_model_means=[0.01, 0.02, 0.03],
    )

    better = service._selection_comparison_summary(champion, challenger_better)
    worse = service._selection_comparison_summary(champion, challenger_worse)
    assert better["promote_ready"] is True
    assert worse["promote_ready"] is False
    assert "top5_ret20_mean_not_improved" in worse["promote_reasons"]
    assert "liquidity_quality_not_improved" in worse["promote_reasons"]


def test_tradex_evaluation_overview_promote_rules() -> None:
    champion = _make_summary(
        top5_mean=0.10,
        top5_median=0.09,
        top10_mean=0.08,
        top10_median=0.07,
        monthly_mean=0.40,
        monthly_median=0.40,
        zero_pass_months=2,
        turnover=0.35,
        dd=0.12,
        liquidity_mean=60_000_000.0,
        regime_means=[0.10, 0.04, 0.08],
        monthly_model_means=[0.05, 0.06, 0.07],
    )
    challenger_good = _make_summary(
        top5_mean=0.14,
        top5_median=0.12,
        top10_mean=0.11,
        top10_median=0.10,
        monthly_mean=0.45,
        monthly_median=0.44,
        zero_pass_months=2,
        turnover=0.30,
        dd=0.08,
        liquidity_mean=75_000_000.0,
        regime_means=[0.12, 0.07, 0.11],
        monthly_model_means=[0.07, 0.08, 0.09],
        liquidity_fail_rate=0.0,
    )
    windows = [
        {"evaluation_window_id": "up:1", "regime_tag": "up", "regime_id": "risk_on_trend", "start_date": "2025-01-01", "end_date": "2025-03-01", "trading_day_count": 60, "champion_top5_ret20_mean": 0.10, "challenger_top5_ret20_mean": 0.12},
        {"evaluation_window_id": "down:1", "regime_tag": "down", "regime_id": "risk_off_trend", "start_date": "2025-03-02", "end_date": "2025-05-01", "trading_day_count": 60, "champion_top5_ret20_mean": 0.08, "challenger_top5_ret20_mean": 0.09},
        {"evaluation_window_id": "flat:1", "regime_tag": "flat", "regime_id": "neutral_range", "start_date": "2025-05-02", "end_date": "2025-07-01", "trading_day_count": 60, "champion_top5_ret20_mean": 0.06, "challenger_top5_ret20_mean": 0.07},
    ]

    better = service._evaluation_overview_summary(champion, challenger_good, windows)
    assert better["promote_ready"] is True
    assert better["window_win_rate"] >= service.PROMOTE_MIN_MONTHLY_WIN_RATE

    challenger_bad_worst = _make_summary(
        top5_mean=0.14,
        top5_median=0.12,
        top10_mean=0.11,
        top10_median=0.10,
        monthly_mean=0.45,
        monthly_median=0.44,
        zero_pass_months=2,
        turnover=0.30,
        dd=0.08,
        liquidity_mean=75_000_000.0,
        regime_means=[0.02, 0.03, 0.04],
        monthly_model_means=[0.07, 0.08, 0.09],
    )
    worst = service._evaluation_overview_summary(champion, challenger_bad_worst, windows)
    assert worst["promote_ready"] is False
    assert "worst_regime_too_weak" in worst["promote_reasons"]

    challenger_bad_dd = _make_summary(
        top5_mean=0.15,
        top5_median=0.14,
        top10_mean=0.12,
        top10_median=0.11,
        monthly_mean=0.48,
        monthly_median=0.47,
        zero_pass_months=2,
        turnover=0.30,
        dd=0.30,
        liquidity_mean=75_000_000.0,
        regime_means=[0.12, 0.07, 0.11],
        monthly_model_means=[0.07, 0.08, 0.09],
    )
    dd = service._evaluation_overview_summary(champion, challenger_bad_dd, windows)
    assert dd["promote_ready"] is False
    assert "drawdown_too_high" in dd["promote_reasons"]

    challenger_bad_turnover = _make_summary(
        top5_mean=0.15,
        top5_median=0.14,
        top10_mean=0.12,
        top10_median=0.11,
        monthly_mean=0.48,
        monthly_median=0.47,
        zero_pass_months=2,
        turnover=0.55,
        dd=0.08,
        liquidity_mean=75_000_000.0,
        regime_means=[0.12, 0.07, 0.11],
        monthly_model_means=[0.07, 0.08, 0.09],
    )
    turnover = service._evaluation_overview_summary(champion, challenger_bad_turnover, windows)
    assert turnover["promote_ready"] is False
    assert "turnover_too_high" in turnover["promote_reasons"]

    challenger_bad_liquidity = _make_summary(
        top5_mean=0.15,
        top5_median=0.14,
        top10_mean=0.12,
        top10_median=0.11,
        monthly_mean=0.48,
        monthly_median=0.47,
        zero_pass_months=2,
        turnover=0.30,
        dd=0.08,
        liquidity_mean=10_000_000.0,
        regime_means=[0.12, 0.07, 0.11],
        monthly_model_means=[0.07, 0.08, 0.09],
        liquidity_fail_rate=0.05,
    )
    liquidity = service._evaluation_overview_summary(champion, challenger_bad_liquidity, windows)
    assert liquidity["promote_ready"] is False
    assert "liquidity_fail_rate_too_high" in liquidity["promote_reasons"]


def test_tradex_selection_summary_monthly_capture_union_and_rank_ties() -> None:
    tied_rows = [
        {"code": "1002", "analysis_ev_net": {"mean": 0.1}, "ret_20": {"mean": 0.2}},
        {"code": "1001", "analysis_ev_net": {"mean": 0.1}, "ret_20": {"mean": 0.2}},
    ]
    assert [row["code"] for row in sorted(tied_rows, key=service._selection_rank_key)] == ["1001", "1002"]

    def sample(code: str, date: str, analysis_ev_net: float, short_ret_20: float) -> dict[str, object]:
        return {
            "code": code,
            "date": date,
            "signal": True,
            "publish_ready": True,
            "engine_scoring_params": {
                "analysis_ev_net": analysis_ev_net,
                "sell_analysis": {"shortRet20": short_ret_20, "shortRet10": short_ret_20 - 1.0, "shortRet5": short_ret_20 - 2.0},
            },
        }

    samples = [
        sample("A", "2025-01-05", 10.0, 40.0),
        sample("B", "2025-01-05", 9.0, 39.0),
        sample("C", "2025-01-05", 8.0, 38.0),
        sample("D", "2025-01-05", 7.0, 37.0),
        sample("E", "2025-01-05", 1.0, 50.0),
        sample("F", "2025-01-05", 0.0, 9.0),
        sample("G", "2025-01-05", 6.0, 10.0),
        sample("A", "2025-01-06", 10.0, 5.0),
        sample("B", "2025-01-06", 9.0, 50.0),
        sample("C", "2025-01-06", 8.0, 49.0),
        sample("D", "2025-01-06", 7.0, 48.0),
        sample("E", "2025-01-06", 1.0, 47.0),
        sample("F", "2025-01-06", 0.0, 46.0),
        sample("G", "2025-01-06", 6.0, 1.0),
    ]

    summary = service._selection_summary(samples)
    monthly = summary["monthly_top5_capture"]
    assert monthly["definition"] == "intersection of monthly unions: realized ret_20 top5 vs model ranking top5"
    assert monthly["month_count"] == 1
    assert monthly["capture_count_mean"] == 4.0
    assert monthly["capture_rate_mean"] == 4.0 / 6.0
    assert monthly["months"][0]["target_union_codes"] == ["A", "B", "C", "D", "E", "F"]
    assert monthly["months"][0]["model_union_codes"] == ["A", "B", "C", "D", "G"]
    assert monthly["months"][0]["capture_codes"] == ["A", "B", "C", "D"]
    assert summary["groups"]["top5"]["codes"] == ["A", "B", "C", "D", "G"]


def test_tradex_research_runner_family_specs_are_named_and_ordered() -> None:
    specs = research_runner._build_family_specs()
    assert [spec.method_family for spec in specs] == [
        "existing-score rescaled",
        "penalty-first",
        "readiness-aware",
        "liquidity-aware",
        "regime-aware",
    ]
    assert len(specs) == 5
    for spec in specs:
        assert spec.family_title
        assert spec.family_thesis
        assert len(spec.candidates) == 2
        for candidate in spec.candidates:
            assert candidate.method_family == spec.method_family
            assert candidate.method_id
            assert candidate.method_title
            assert candidate.method_thesis
            assert candidate.plan_overrides["top_k"] == 5


def test_tradex_research_runner_session_resume_and_artifacts(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()

    family_specs = research_runner._build_family_specs()[:2]
    monkeypatch.setattr(research_runner, "_build_family_specs", lambda: family_specs)

    phase4_calls: list[dict[str, object]] = []

    def _phase4_should_not_run(*args, **kwargs):
        phase4_calls.append({"args": args, "kwargs": kwargs})
        raise AssertionError("phase4 should not run for this fixture")

    monkeypatch.setattr(research_runner, "_train_phase4_ranker", _phase4_should_not_run)

    session_id = "rs1"
    seed = 7
    result1 = research_runner.run_tradex_research_session(
        session_id=session_id,
        random_seed=seed,
        universe_size=20,
        max_candidates_per_family=2,
    )

    assert result1["status"] == "complete"
    assert result1["schema_version"] == research_runner.SESSION_SCHEMA_VERSION
    assert result1["compare_schema_version"] == research_runner.SESSION_COMPARE_SCHEMA_VERSION
    assert result1["manifest"]["session_id"] == session_id
    assert result1["manifest"]["random_seed"] == seed
    assert len(result1["manifest"]["period_segments"]) == 3
    assert len(result1["family_results"]) == 2
    assert result1["champion"]["method"]["method_title"] == "現行ランキング"
    assert result1["champion"]["method"]["method_family"] == "champion"
    assert result1["best_result"]["candidate_method"]["method_title"]
    assert result1["best_result"]["candidate_method"]["method_family"] in {spec.method_family for spec in family_specs}
    assert result1["phase4"]["status"] == "skipped"

    session_state_path = research_runner._session_state_file(session_id)
    compare_path = research_runner._session_compare_file(session_id)
    report_path = research_runner._session_report_file(session_id)
    assert session_state_path.exists()
    assert compare_path.exists()
    assert report_path.exists()

    session_state = json.loads(session_state_path.read_text(encoding="utf-8"))
    compare_state = json.loads(compare_path.read_text(encoding="utf-8"))
    assert session_state == compare_state
    assert session_state["manifest_hash"] == result1["manifest_hash"]
    assert session_state["best_result"]["candidate_method"]["method_title"] == result1["best_result"]["candidate_method"]["method_title"]
    assert session_state["manifest"]["method_families"][0]["candidate_order"] == [family_specs[0].candidates[0].method_id, family_specs[0].candidates[1].method_id]
    assert session_state["manifest"]["method_families"][1]["candidate_order"] == [family_specs[1].candidates[0].method_id, family_specs[1].candidates[1].method_id]
    assert session_state["eval_window_mode"] in {"standard", "fallback"}
    assert session_state["eval_window_mode_reason"]
    assert session_state["runtime_meta"]["eval_window_mode"] == session_state["eval_window_mode"]
    assert session_state["ret20_source_mode"] in {service.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED, service.TRADEX_RET20_SOURCE_MODE_DERIVED}
    assert session_state["ret20_source_mode_reason"] == "explicit_session_mode"

    report_text = report_path.read_text(encoding="utf-8")
    assert "eval_window_mode" in report_text
    assert "compare artifact が正本" in report_text
    assert "markdown report は派生物" in report_text
    assert result1["best_result"]["candidate_method"]["method_title"] in report_text
    assert "ret20_source_mode" in report_text
    leaderboard_path = research_runner._session_family_leaderboard_file(session_id)
    leaderboard_report_path = research_runner._session_family_leaderboard_report_file(session_id)
    assert leaderboard_path.exists()
    assert leaderboard_report_path.exists()

    leaderboard = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    assert leaderboard["schema_version"] == research_runner.SESSION_FAMILY_LEADERBOARD_SCHEMA_VERSION
    assert leaderboard["session_meta"]["session_id"] == session_id
    assert leaderboard["session_meta"]["random_seed"] == seed
    assert leaderboard["session_meta"]["generated_at"]
    assert leaderboard["session_meta"]["eval_window_mode"] in {"standard", "fallback"}
    assert leaderboard["session_meta"]["eval_window_mode_reason"]
    assert leaderboard["session_meta"]["ret20_source_mode"] in {service.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED, service.TRADEX_RET20_SOURCE_MODE_DERIVED}
    assert leaderboard["session_meta"]["ret20_source_mode_reason"] == "explicit_session_mode"
    assert isinstance(leaderboard["family_summary"], list)
    assert isinstance(leaderboard["candidate_rows"], list)
    assert leaderboard["family_summary"]
    assert leaderboard["candidate_rows"]
    assert {row["decision"] for row in leaderboard["candidate_rows"]} <= {"keep", "drop", "hold"}
    assert leaderboard["candidate_rows"][0]["method_signature_hash"]
    assert "candidate_scope_key_mismatch_reason_counts" in leaderboard["candidate_rows"][0]
    assert "scope_filter_applied_stage" in leaderboard["candidate_rows"][0]
    assert "key_normalization_mode" in leaderboard["candidate_rows"][0]
    first_row_reasons = leaderboard["candidate_rows"][0]["decision_reasons"]
    assert {reason["code"] for reason in first_row_reasons} >= {
        "top5",
        "monthly_capture",
        "zero_pass",
        "worst_regime",
        "dd",
        "turnover",
        "liquidity_fail",
    }

    partial_state = dict(session_state)
    partial_state["status"] = "running"
    partial_state["phase"] = "phase2"
    partial_state["family_results"] = session_state["family_results"][:1]
    partial_state["best_result"] = {}
    session_state_path.write_text(json.dumps(partial_state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    compare_path.write_text(json.dumps(partial_state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    result2 = research_runner.run_tradex_research_session(
        session_id=session_id,
        random_seed=seed,
        universe_size=20,
        max_candidates_per_family=2,
    )

    assert result2["status"] == "complete"
    assert result2["manifest_hash"] == result1["manifest_hash"]
    assert result2["manifest"]["method_families"] == result1["manifest"]["method_families"]
    assert result2["best_result"]["candidate_method"]["method_title"] == result1["best_result"]["candidate_method"]["method_title"]
    assert session_state_path.exists()
    assert compare_path.exists()
    assert not phase4_calls


def test_tradex_research_runner_rejects_duplicate_method_family_thesis(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()

    family_specs = (
        research_runner.FamilySpec(
            method_family="duplicate-family",
            family_title="重複家族",
            family_thesis="同一 thesis の再試行を禁止する。",
            candidates=(
                research_runner.CandidateMethodSpec(
                    method_family="duplicate-family",
                    method_id="duplicate-a",
                    method_title="重複候補A",
                    method_thesis="同じ仮説を試す。",
                    plan_overrides={"top_k": 5},
                ),
                research_runner.CandidateMethodSpec(
                    method_family="duplicate-family",
                    method_id="duplicate-b",
                    method_title="重複候補B",
                    method_thesis="同じ仮説を試す。",
                    plan_overrides={"top_k": 5},
                ),
            ),
        ),
    )
    monkeypatch.setattr(research_runner, "_build_family_specs", lambda: family_specs)

    with pytest.raises(RuntimeError, match="duplicate candidate method prohibited"):
        research_runner.run_tradex_research_session(
            session_id="dup-thesis",
            random_seed=11,
            universe_size=20,
            max_candidates_per_family=2,
        )


def test_tradex_research_runner_session_leaderboard_rollup(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()

    family_specs = research_runner._build_family_specs()[:2]
    monkeypatch.setattr(research_runner, "_build_family_specs", lambda: family_specs)

    research_runner.run_tradex_research_session(
        session_id="r1",
        random_seed=7,
        universe_size=20,
        max_candidates_per_family=2,
    )
    research_runner.run_tradex_research_session(
        session_id="r2",
        random_seed=13,
        universe_size=20,
        max_candidates_per_family=2,
    )

    rollup_path = research_runner._session_leaderboard_rollup_file()
    rollup_report_path = research_runner._session_leaderboard_rollup_report_file()
    assert rollup_path.exists()
    assert rollup_report_path.exists()

    rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
    assert rollup["schema_version"] == research_runner.SESSION_LEADERBOARD_ROLLUP_SCHEMA_VERSION
    assert rollup["session_meta"]["session_count"] >= 2
    assert set(rollup["session_meta"]["session_ids"]) >= {"r1", "r2"}
    assert isinstance(rollup["family_summary"], list)
    assert isinstance(rollup["candidate_rows"], list)
    assert rollup["family_summary"]
    assert rollup["candidate_rows"]
    assert rollup["candidate_rows"][0]["method_signature_hash"]
    assert rollup["candidate_rows"][0]["latest_decision"] in {"keep", "drop", "hold"}
    assert isinstance(rollup["candidate_rows"][0]["latest_decision_reasons"], list)
    assert "avg_top5_ret20_mean_delta" in rollup["candidate_rows"][0]
    assert rollup["family_summary"][0]["decision"] in {"keep", "drop", "hold"}
    assert rollup["family_summary"][0]["latest_decision"] in {"keep", "drop", "hold"}
    report_text = rollup_report_path.read_text(encoding="utf-8")
    assert "TRADEX Session Leaderboard Rollup" in report_text
    assert "session_count" in report_text


def test_tradex_research_runner_formats_epoch_window_dates() -> None:
    expected = datetime.fromtimestamp(777168000, tz=timezone.utc).date().isoformat()
    assert service._format_ymd_int(777168000) == expected
    assert service._format_ymd_int("777168000") == expected


def test_tradex_family_result_summary_handles_missing_candidate_results() -> None:
    family_spec = research_runner.FamilySpec(
        method_family="empty-family",
        family_title="空ファミリー",
        family_thesis="候補が空でも落ちないことを確認する。",
        candidates=(),
    )
    summary = research_runner._family_result_summary(
        family_spec=family_spec,
        family={"family_id": "family-001"},
        compare={"candidate_results": None},
    )

    assert summary["candidate_count"] == 0
    assert summary["candidate_results"] == []
    assert summary["best_candidate"] is None
    assert summary["promote_ready"] is False


def test_tradex_research_runner_coverage_marks_insufficient_samples() -> None:
    session_state = {
        "manifest": {
            "universe": ["1001", "1002"],
            "period_segments": [
                {"label": "up", "start_date": "2012-01-17", "end_date": "2012-02-20"},
                {"label": "down", "start_date": "2012-03-01", "end_date": "2012-04-01"},
                {"label": "flat", "start_date": "2012-05-01", "end_date": "2012-06-01"},
            ],
        },
        "family_results": [
            {
                "compare": {
                    "candidate_results": [
                        {
                            "evaluation_summary": {
                                "champion_selection_summary": {"sample_count": 0},
                                "challenger_selection_summary": {"sample_count": 0},
                                "windows": [],
                            }
                        }
                    ]
                }
            }
        ],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    assert coverage["confirmed_universe_count"] == 2
    assert coverage["probe_candidate_count"] == 1
    assert coverage["regime_window_count"] == 3
    assert coverage["evaluation_row_count"] == 0
    assert coverage["sample_count"] == 0
    assert coverage["first_zero_stage"] == "probe_selection"
    assert coverage["failure_stage"] == "probe_selection"
    assert coverage["insufficient_samples"] is True
    assert coverage["future_ret20_candidate_day_count"] == 0
    assert coverage["future_ret20_passed_count"] == 0
    assert coverage["future_ret20_guarded_out_count"] == 0
    assert coverage["future_ret20_failure_reason_counts"] == {}


def test_tradex_future_ret20_guard_reason_detects_source_and_end_of_data() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }

    missing_reason, missing_diag = service._future_ret20_guard_reason(
        code="1001",
        dt_key=20250108,
        point={"shortRet20": None},
        trade_sequence=trade_sequence,
    )
    assert missing_reason == "ret20_source_missing"
    assert missing_diag["source_present"] is False

    close_reason, close_diag = service._future_ret20_guard_reason(
        code="1001",
        dt_key=20250116,
        point={"shortRet20": 0.12},
        trade_sequence=trade_sequence,
    )
    assert close_reason == "candidate_after_last_valid_ret20_date"
    assert close_diag["future_trading_day_count"] < 20


def test_tradex_last_valid_ret20_candidate_date_matches_trading_horizon() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-02-05",
        "2025-02-06",
        "2025-02-07",
        "2025-02-10",
        "2025-02-11",
        "2025-02-12",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }

    assert service._last_valid_ret20_candidate_date(trade_sequence, 20) == dates[-21]


def test_tradex_future_ret20_guard_reason_flags_trading_sequence_shortage() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-02-05",
        "2025-02-06",
        "2025-02-07",
        "2025-02-10",
        "2025-02-11",
        "2025-02-12",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(15)],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }

    reason, diagnostics = service._future_ret20_guard_reason(
        code="1001",
        dt_key=20250102,
        point={"shortRet20": 0.12},
        trade_sequence=trade_sequence,
    )
    assert reason == "trading_sequence_shortage"
    assert diagnostics["missing_reason_detail"] == "future_close_out_of_range"


def test_tradex_future_ret20_guard_reason_flags_non_trading_day() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-02-05",
        "2025-02-06",
        "2025-02-07",
        "2025-02-10",
        "2025-02-11",
        "2025-02-12",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }

    reason, diagnostics = service._future_ret20_guard_reason(
        code="1001",
        dt_key=20250104,
        point={"shortRet20": 0.12},
        trade_sequence=trade_sequence,
    )
    assert reason == "regime_date_not_in_code_trading_calendar"
    assert diagnostics["join_key_status"] == "calendar_vs_trading_day_mismatch"


def test_tradex_future_ret20_join_gap_detail_classifies_scope_filter_removed_row() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }
    detail = service._future_ret20_join_gap_detail(
        code="1001",
        candidate_date="2025-01-03",
        trade_sequence=trade_sequence,
        scope_session_id="scope-a",
        scope_filter_applied_stage="analysis_points_segment_filter",
        scope_points=[{"dt": 20250103}, {"dt": 20250106}],
        code_points=[],
        selected_segment_ranges=[("2025-01-04", "2025-01-10")],
    )
    assert detail["code"] == "1001"
    assert detail["candidate_date"] == "2025-01-03"
    assert detail["scope_session_id"] == "scope-a"
    assert detail["scope_filter_applied_stage"] == "analysis_points_segment_filter"
    assert detail["candidate_side_exists"] is True
    assert detail["future_side_exists"] is True
    assert detail["regime_side_exists"] is False
    assert detail["code_calendar_has_candidate_date"] is True
    assert detail["code_calendar_has_future_trade_date"] is True
    assert detail["candidate_key_before_scope"] == "1001|2025-01-03"
    assert detail["candidate_key_after_scope"] == ""
    assert detail["scope_key_expected"] == "1001|2025-01-03"
    assert detail["key_normalization_mode"] == "code4/date_iso"
    assert detail["join_gap_reason_detail"] == "candidate_removed_after_scope_boundary"
    assert detail["mismatch_reason_detail"] == "candidate_removed_after_scope_boundary"
    assert detail["candidate_rows_before_scope_filter"] == 2
    assert detail["candidate_rows_after_scope_filter"] == 0


def test_tradex_candidate_scope_gap_detail_classifies_date_range_removed_row() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }
    detail = service._candidate_scope_gap_detail(
        code="1001",
        candidate_date="2025-01-03",
        trade_sequence=trade_sequence,
        scope_session_id="scope-a",
        scope_filter_applied_stage="analysis_points_segment_filter_after_scope_points_build",
        scope_points=[{"dt": 20250103}, {"dt": 20250106}],
        code_points=[],
        selected_segment_ranges=[("2025-01-04", "2025-01-10")],
    )
    assert detail["code"] == "1001"
    assert detail["candidate_date"] == "2025-01-03"
    assert detail["scope_filter_applied_stage"] == "analysis_points_segment_filter_after_scope_points_build"
    assert detail["candidate_side_exists"] is True
    assert detail["scope_side_exists"] is True
    assert detail["future_side_exists"] is True
    assert detail["code_calendar_has_candidate_date"] is True
    assert detail["candidate_key_before_scope"] == "1001|2025-01-03"
    assert detail["candidate_key_after_scope"] == ""
    assert detail["scope_key_expected"] == "1001|2025-01-03"
    assert detail["key_normalization_mode"] == "code4/date_iso"
    assert detail["candidate_in_scope_before_build_count"] == 2
    assert detail["candidate_in_scope_after_build_count"] == 0
    assert detail["candidate_scope_gap_reason"] == "candidate_removed_after_scope_boundary"
    assert detail["mismatch_reason_detail"] == "candidate_removed_after_scope_boundary"


def test_tradex_join_key_normalization_fixed_format() -> None:
    normalized = service._normalize_join_key(code="7", date_value=20250103)

    assert normalized["code"] == "0007"
    assert normalized["date"] == "2025-01-03"
    assert normalized["join_key"] == "0007|2025-01-03"
    assert normalized["key_normalization_mode"] == "code4/date_iso"
    assert "code_zero_padded" in normalized["key_normalization_applied"]
    assert "date_type_mismatch" in normalized["key_normalization_applied"]


def test_tradex_analysis_points_scope_bounds_are_inclusive(monkeypatch) -> None:
    class _BoundaryRepo:
        def get_analysis_timeline(self, code: str, asof_dt: int | None, limit: int = 1000):
            del code, asof_dt, limit
            return [
                {"dt": 20250102},
                {"dt": 20250103},
                {"dt": 20250104},
                {"dt": 20250105},
                {"dt": 20250106},
            ]

    points = service._analysis_points(_BoundaryRepo(), "1001", "2025-01-03", "2025-01-05")
    assert [point["dt"] for point in points] == [20250103, 20250104, 20250105]


def test_tradex_candidate_scope_gap_detail_classifies_type_mismatch() -> None:
    trade_sequence = {
        "dates": ["2025-01-03", "2025-01-04", "2025-01-05"],
        "closes": [100.0, 101.0, 102.0],
        "date_index": {"2025-01-03": 0, "2025-01-04": 1, "2025-01-05": 2},
        "last_date": "2025-01-05",
    }
    detail = service._candidate_scope_gap_detail(
        code="1001",
        candidate_date=20250103,
        trade_sequence=trade_sequence,
        scope_session_id="scope-a",
        scope_filter_applied_stage="analysis_points_segment_filter_after_scope_points_build",
        scope_points=[{"dt": 20250103}],
        code_points=[],
        selected_segment_ranges=[("2025-01-03", "2025-01-03")],
    )

    assert detail["candidate_date"] == "2025-01-03"
    assert detail["candidate_key_before_scope"] == "1001|2025-01-03"
    assert detail["candidate_key_after_scope"] == ""
    assert detail["scope_key_expected"] == "1001|2025-01-03"
    assert detail["key_normalization_mode"] == "code4/date_iso"
    assert detail["mismatch_reason_detail"] == "date_type_mismatch"
    assert detail["candidate_scope_gap_reason"] == "date_type_mismatch"


def test_tradex_session_failure_reason_classifier_types_known_errors() -> None:
    scope_failure = research_runner._build_scope_stability_failure_row(
        session_id="scope-fail",
        session_scope_id="scope-a",
        random_seed=7,
        error=RuntimeError("confirmed universe is empty: {}"),
    )
    compare_failure = research_runner._build_stability_failure_row(
        session_id="compare-fail",
        random_seed=11,
        error=RuntimeError("missing session state after completed run: compare-fail"),
    )
    assert scope_failure["session_failure_reason"] == "scope_resolution_failed"
    assert scope_failure["first_zero_stage"] == "scope_resolution_failed"
    assert scope_failure["eval_window_mode_reason"] == "scope_resolution_failed"
    assert scope_failure["ret20_source_mode_reason"] == "scope_resolution_failed"
    assert compare_failure["session_failure_reason"] == "compare_artifact_incomplete"
    assert compare_failure["first_zero_stage"] == "compare_artifact_incomplete"
    assert compare_failure["eval_window_mode_reason"] == "compare_artifact_incomplete"
    assert compare_failure["ret20_source_mode_reason"] == "compare_artifact_incomplete"
    assert scope_failure["eval_window_mode_reason"] != "session_failed"
    assert compare_failure["ret20_source_mode_reason"] != "session_failed"


def test_tradex_future_ret20_guard_reason_passes_with_twenty_future_trading_days() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-02-05",
        "2025-02-06",
        "2025-02-07",
        "2025-02-10",
        "2025-02-11",
        "2025-02-12",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }

    reason, diagnostics = service._future_ret20_guard_reason(
        code="1001",
        dt_key=20250102,
        point={"shortRet20": 0.12},
        trade_sequence=trade_sequence,
    )
    assert reason is None
    assert diagnostics["future_trading_day_count"] >= 20
    assert diagnostics["future_ret20"] is not None


def test_tradex_future_ret20_guard_reason_derived_mode_recomputes_from_daily_bars() -> None:
    dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
        "2025-01-20",
        "2025-01-21",
        "2025-01-22",
        "2025-01-23",
        "2025-01-24",
        "2025-01-27",
        "2025-01-28",
        "2025-01-29",
        "2025-01-30",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-02-05",
        "2025-02-06",
        "2025-02-07",
        "2025-02-10",
        "2025-02-11",
        "2025-02-12",
    ]
    trade_sequence = {
        "dates": dates,
        "closes": [100.0 + idx for idx in range(len(dates))],
        "date_index": {dt: idx for idx, dt in enumerate(dates)},
        "last_date": dates[-1],
    }

    reason, diagnostics = service._future_ret20_guard_reason(
        code="1001",
        dt_key=20250102,
        point={"shortRet20": None},
        trade_sequence=trade_sequence,
        source_mode=service.TRADEX_RET20_SOURCE_MODE_DERIVED,
    )
    assert reason is None
    assert diagnostics["source_mode"] == service.TRADEX_RET20_SOURCE_MODE_DERIVED
    assert diagnostics["source_table_checked"] == "daily_bars.derived_from_daily_bars"
    assert diagnostics["future_ret20"] is not None


def test_tradex_session_coverage_propagates_future_ret20_counts() -> None:
    session_state = {
        "summary": {
            "session_failure_reason_counts": {"scope_resolution_failed": 1},
            "future_ret20_coverage": {
                "candidate_day_count": 12,
                "candidate_rows_before_future_guard": 12,
                "candidate_rows_after_future_guard": 4,
                "ret20_joinable_rows": 4,
                "compare_rows_emitted": 4,
                "sample_rows_retained": 4,
                "passed_count": 3,
                "guarded_out_count": 9,
                "failure_reason_counts": {"ret20_source_missing": 7, "candidate_after_last_valid_ret20_date": 2},
                "failure_reason_counts_by_source_mode": {
                    "precomputed": {"ret20_source_missing": 7},
                    "derived_from_daily_bars": {"candidate_after_last_valid_ret20_date": 2},
                },
            }
        },
        "family_results": [],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    assert coverage["session_failure_reason_counts"]["scope_resolution_failed"] == 1
    assert coverage["future_ret20_candidate_day_count"] == 12
    assert coverage["candidate_rows_before_future_guard"] == 12
    assert coverage["candidate_rows_after_future_guard"] == 4
    assert coverage["ret20_joinable_rows"] == 4
    assert coverage["compare_rows_emitted"] == 4
    assert coverage["sample_rows_retained"] == 4
    assert coverage["future_ret20_passed_count"] == 3
    assert coverage["future_ret20_guarded_out_count"] == 9
    assert coverage["future_ret20_failure_reason_counts"]["ret20_source_missing"] == 7
    assert coverage["future_ret20_failure_reason_counts_by_source_mode"]["precomputed"]["ret20_source_missing"] == 7
    assert coverage["future_ret20_failure_reason_counts_by_source_mode"]["derived_from_daily_bars"]["candidate_after_last_valid_ret20_date"] == 2


def test_tradex_session_coverage_derives_future_ret20_counts_by_source_mode_when_missing() -> None:
    session_state = {
        "summary": {
            "future_ret20_coverage": {
                "candidate_day_count": 4,
                "passed_count": 1,
                "guarded_out_count": 3,
                "failure_reason_counts": {"ret20_source_missing": 3, "unknown_future_ret20_failure": 1},
            },
            "future_ret20_source_coverage": {"ret20_source_mode": "precomputed"},
        },
        "family_results": [],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    assert coverage["future_ret20_failure_reason_counts_by_source_mode"]["precomputed"]["ret20_source_missing"] == 3
    assert coverage["future_ret20_failure_reason_counts_by_source_mode"]["precomputed"]["unknown_future_ret20_failure"] == 1


def test_tradex_session_coverage_propagates_future_ret20_code_counts() -> None:
    session_state = {
        "summary": {
            "future_ret20_coverage": {
                "candidate_day_count": 12,
                "passed_count": 3,
                "guarded_out_count": 9,
                "failure_reason_counts": {"unknown_future_ret20_failure": 2},
            }
        },
        "family_results": [
            {
                "compare": {
                    "candidate_results": [
                        {
                            "evaluation_summary": {
                                "champion_future_ret20_code_coverage": {
                                    "candidate_guarded_by_last_valid_ret20_date_count": 1,
                                    "codes_with_any_candidate": 2,
                                    "codes_with_future_ret20_pass": 1,
                                    "codes_all_failed_future_ret20": 1,
                                    "top_failed_codes": [{"code": "1001", "failure_count": 3, "failure_reason": "unknown_future_ret20_failure"}],
                                },
                                "challenger_future_ret20_code_coverage": {
                                    "candidate_guarded_by_last_valid_ret20_date_count": 4,
                                    "codes_with_any_candidate": 5,
                                    "codes_with_future_ret20_pass": 3,
                                    "codes_all_failed_future_ret20": 2,
                                    "top_failed_codes": [{"code": "2002", "failure_count": 2, "failure_reason": "unknown_future_ret20_failure"}],
                                },
                                "champion_selection_summary": {"sample_count": 1},
                                "challenger_selection_summary": {"sample_count": 1},
                            },
                            "selection_compare": {},
                        }
                    ]
                }
            }
        ],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    assert coverage["future_ret20_candidate_guarded_by_last_valid_ret20_date_count"] == 5
    assert coverage["future_ret20_codes_with_any_candidate"] == 7
    assert coverage["future_ret20_codes_with_future_ret20_pass"] == 4
    assert coverage["future_ret20_codes_all_failed_future_ret20"] == 3
    assert coverage["future_ret20_top_failed_codes"]


def test_tradex_session_coverage_propagates_future_ret20_join_gap_counts() -> None:
    session_state = {
        "summary": {
            "future_ret20_coverage": {
                "candidate_day_count": 12,
                "passed_count": 3,
                "guarded_out_count": 9,
                "failure_reason_counts": {"unknown_future_ret20_failure": 2},
            }
        },
        "family_results": [
            {
                "compare": {
                    "candidate_results": [
                        {
                            "evaluation_summary": {
                                "champion_future_ret20_join_gap_coverage": {
                                    "after_scope_filter_count": 2,
                                    "reason_counts": {"candidate_removed_by_scope_date_range": 1},
                                    "examples": [{"join_gap_reason_detail": "candidate_removed_by_scope_date_range"}],
                                    "candidate_rows_before_scope_filter": 10,
                                    "candidate_rows_after_scope_filter": 8,
                                    "future_rows_before_scope_filter": 20,
                                    "future_rows_after_scope_filter": 18,
                                    "joinable_code_date_pairs_before_scope": 6,
                                    "joinable_code_date_pairs_after_scope": 5,
                                },
                                "challenger_future_ret20_join_gap_coverage": {
                                    "after_scope_filter_count": 4,
                                    "reason_counts": {"join_gap_after_scope_filter": 3},
                                    "examples": [{"join_gap_reason_detail": "join_gap_after_scope_filter"}],
                                    "candidate_rows_before_scope_filter": 12,
                                    "candidate_rows_after_scope_filter": 9,
                                    "future_rows_before_scope_filter": 20,
                                    "future_rows_after_scope_filter": 18,
                                    "joinable_code_date_pairs_before_scope": 7,
                                    "joinable_code_date_pairs_after_scope": 6,
                                },
                                "champion_selection_summary": {"sample_count": 1},
                                "challenger_selection_summary": {"sample_count": 1},
                            },
                            "selection_compare": {
                                "future_ret20_join_gap_coverage": {
                                    "after_scope_filter_count": 1,
                                    "reason_counts": {"scope_filter_removed_required_row": 1},
                                    "examples": [{"join_gap_reason_detail": "scope_filter_removed_required_row"}],
                                    "candidate_rows_before_scope_filter": 3,
                                    "candidate_rows_after_scope_filter": 2,
                                    "future_rows_before_scope_filter": 20,
                                    "future_rows_after_scope_filter": 18,
                                    "joinable_code_date_pairs_before_scope": 2,
                                    "joinable_code_date_pairs_after_scope": 1,
                                }
                            },
                        }
                    ]
                }
            }
        ],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    join_gap = coverage["future_ret20_join_gap_coverage"]
    assert join_gap["after_scope_filter_count"] == 7
    assert join_gap["reason_counts"]["candidate_removed_by_scope_date_range"] == 1
    assert join_gap["reason_counts"]["join_gap_after_scope_filter"] == 3
    assert join_gap["reason_counts"]["scope_filter_removed_required_row"] == 1
    assert join_gap["candidate_rows_before_scope_filter"] == 25
    assert join_gap["candidate_rows_after_scope_filter"] == 19
    assert join_gap["future_rows_before_scope_filter"] == 60
    assert join_gap["future_rows_after_scope_filter"] == 54
    assert join_gap["joinable_code_date_pairs_before_scope"] == 15
    assert join_gap["joinable_code_date_pairs_after_scope"] == 12


def test_tradex_session_coverage_propagates_candidate_scope_gap_counts() -> None:
    session_state = {
        "summary": {
            "candidate_scope_gap_coverage": {
                "scope_filter_applied_stage": "analysis_points_segment_filter_after_scope_points_build",
                "key_normalization_mode": "code4/date_iso",
                "candidate_in_scope_before_build_count": 9,
                "candidate_in_scope_after_build_count": 5,
                "candidate_removed_by_scope_boundary_count": 2,
                "candidate_scope_key_mismatch_reason_counts": {"candidate_removed_after_scope_boundary": 2},
                "candidate_scope_gap_reason_counts": {"candidate_removed_after_scope_boundary": 2},
                "candidate_scope_gap_examples": [{"candidate_scope_gap_reason": "candidate_removed_after_scope_boundary"}],
                "candidate_scope_gap_count": 2,
            }
        },
        "family_results": [
            {
                "compare": {
                    "candidate_results": [
                        {
                            "evaluation_summary": {
                                "candidate_scope_gap_coverage": {
                                    "scope_filter_applied_stage": "analysis_points_segment_filter_after_scope_points_build",
                                    "key_normalization_mode": "code4/date_iso",
                                    "candidate_in_scope_before_build_count": 4,
                                    "candidate_in_scope_after_build_count": 3,
                                    "candidate_removed_by_scope_boundary_count": 1,
                                    "candidate_scope_key_mismatch_reason_counts": {"scope_anchor_mismatch": 1},
                                    "candidate_scope_gap_reason_counts": {"scope_anchor_mismatch": 1},
                                    "candidate_scope_gap_examples": [{"candidate_scope_gap_reason": "scope_anchor_mismatch"}],
                                    "candidate_scope_gap_count": 1,
                                },
                                "champion_selection_summary": {"sample_count": 1},
                                "challenger_selection_summary": {"sample_count": 1},
                            },
                            "selection_compare": {},
                        }
                    ]
                }
            }
        ],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    gap = coverage["candidate_scope_gap_coverage"]
    assert gap["scope_filter_applied_stage"] == "analysis_points_segment_filter_after_scope_points_build"
    assert gap["key_normalization_mode"] == "code4/date_iso"
    assert gap["candidate_in_scope_before_build_count"] == 13
    assert gap["candidate_in_scope_after_build_count"] == 8
    assert gap["candidate_removed_by_scope_boundary_count"] == 3
    assert gap["candidate_scope_key_mismatch_reason_counts"]["candidate_removed_after_scope_boundary"] == 2
    assert gap["candidate_scope_key_mismatch_reason_counts"]["scope_anchor_mismatch"] == 1
    assert gap["candidate_scope_gap_reason_counts"]["candidate_removed_after_scope_boundary"] == 2
    assert gap["candidate_scope_gap_reason_counts"]["scope_anchor_mismatch"] == 1


def test_tradex_research_runner_derived_ret20_source_mode_recovers_samples(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)

    class _DerivedModeRepo(_FakeRepo):
        def get_analysis_timeline(self, code: str, asof_dt: int | None, limit: int = 400):
            rows = super().get_analysis_timeline(code, asof_dt, limit=limit)
            return [{**row, "shortRet20": None} for row in rows]

    dependencies._stock_repo = _DerivedModeRepo()
    dependencies._config_repo = object()

    result = research_runner.run_tradex_research_session(
        session_id="d1",
        random_seed=19,
        universe_size=20,
        max_candidates_per_family=1,
        ret20_source_mode=service.TRADEX_RET20_SOURCE_MODE_DERIVED,
    )

    assert result["ret20_source_mode"] == service.TRADEX_RET20_SOURCE_MODE_DERIVED
    assert result["coverage_waterfall"]["future_ret20_source_coverage"]["ret20_source_mode"] == service.TRADEX_RET20_SOURCE_MODE_DERIVED
    assert result["coverage_waterfall"]["future_ret20_candidate_day_count"] > 0
    assert result["coverage_waterfall"]["future_ret20_passed_count"] > 0
    assert result["coverage_waterfall"]["future_ret20_failure_reason_counts"].get("ret20_source_missing", 0) == 0
    assert result["coverage_waterfall"]["sample_count"] > 0


def test_tradex_session_coverage_rejects_mixed_ret20_source_mode() -> None:
    session_state = {
        "summary": {},
        "family_results": [
            {
                "compare": {
                    "candidate_results": [
                        {
                            "evaluation_summary": {
                                "future_ret20_source_coverage": {
                                    "ret20_source_mode": "precomputed",
                                    "missing_by_source_table": {"analysis_timeline.shortRet20": 1},
                                    "missing_by_code": {"1001": 1},
                                    "missing_by_month": {"2025-01": 1},
                                    "missing_near_data_end_count": 0,
                                    "missing_join_miss_count": 0,
                                    "missing_trade_sequence_shortage_count": 0,
                                    "missing_examples": [],
                                },
                                "future_ret20_coverage": {"candidate_day_count": 1, "passed_count": 1, "guarded_out_count": 0, "failure_reason_counts": {}},
                                "champion_selection_summary": {"sample_count": 1},
                                "challenger_selection_summary": {"sample_count": 1},
                            },
                            "selection_compare": {},
                        },
                        {
                            "evaluation_summary": {
                                "future_ret20_source_coverage": {
                                    "ret20_source_mode": "derived_from_daily_bars",
                                    "missing_by_source_table": {"daily_bars.derived_from_daily_bars": 1},
                                    "missing_by_code": {"1002": 1},
                                    "missing_by_month": {"2025-02": 1},
                                    "missing_near_data_end_count": 0,
                                    "missing_join_miss_count": 0,
                                    "missing_trade_sequence_shortage_count": 0,
                                    "missing_examples": [],
                                },
                                "future_ret20_coverage": {"candidate_day_count": 1, "passed_count": 1, "guarded_out_count": 0, "failure_reason_counts": {}},
                                "champion_selection_summary": {"sample_count": 1},
                                "challenger_selection_summary": {"sample_count": 1},
                            },
                            "selection_compare": {},
                        },
                    ]
                }
            }
        ],
    }

    coverage = research_runner._session_coverage_summary(session_state)
    assert coverage["future_ret20_source_coverage"]["mixed_source_mode"] is True
    assert coverage["insufficient_samples"] is True


def test_tradex_research_runner_rejects_empty_confirmed_universe(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)

    class _EmptyRepo:
        _db_path = str(tmp_path / "empty.duckdb")

        def get_all_codes(self):
            return []

    monkeypatch.setattr(dependencies, "_stock_repo", _EmptyRepo())
    monkeypatch.setattr(dependencies, "_favorites_repo", object())
    monkeypatch.setattr(dependencies, "_config_repo", object())
    monkeypatch.setattr(dependencies, "_screener_repo", object())

    with pytest.raises(RuntimeError) as exc_info:
        research_runner.run_tradex_research_session(
            session_id="empty-universe",
            random_seed=1,
            universe_size=20,
            max_candidates_per_family=1,
        )

    message = str(exc_info.value)
    assert "confirmed universe is empty" in message
    assert "codes_count" in message
    assert "data_dir" in message
    assert "db_path" in message


def test_tradex_session_leaderboard_rollup_marks_artifact_inconsistency_invalid(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(research_runner.tradex, "REPO_ROOT", tmp_path)

    session_dir = research_runner._session_dir("rollup-inconsistent")
    session_state = {
        "session_id": "rollup-inconsistent",
        "random_seed": 7,
        "eval_window_mode": "fallback",
        "ret20_source_mode": service.TRADEX_RET20_SOURCE_MODE_DERIVED,
        "coverage_waterfall": {"sample_count": 3, "insufficient_samples": False},
        "insufficient_samples": False,
    }
    family_leaderboard = {
        "schema_version": research_runner.SESSION_FAMILY_LEADERBOARD_SCHEMA_VERSION,
        "session_meta": {
            "session_id": "rollup-inconsistent",
            "random_seed": 7,
            "generated_at": "2026-03-23T00:00:00+00:00",
            "eval_window_mode": "fallback",
            "ret20_source_mode": service.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
            "sample_count": 3,
            "insufficient_samples": False,
        },
        "family_summary": [],
        "candidate_rows": [],
    }
    research_runner._write_json(session_dir / "session.json", session_state)
    research_runner._write_json(session_dir / research_runner.SESSION_FAMILY_LEADERBOARD_FILE, family_leaderboard)

    rollup = research_runner._build_session_leaderboard_rollup()
    assert rollup["overview"]["artifact_consistency_error_count"] == 1
    assert rollup["overview"]["invalid_session_count"] == 1
    assert rollup["insufficient_samples"] is True
    assert rollup["session_meta"]["invalid_session_count"] == 1


def test_tradex_research_runner_rejects_legacy_analysis_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "1")

    with pytest.raises(RuntimeError, match="legacy analysis is disabled"):
        research_runner.run_tradex_research_session(
            session_id="legacy-disabled",
            random_seed=7,
            universe_size=20,
            max_candidates_per_family=1,
        )


def test_tradex_research_runner_rejects_scope_sweep_when_legacy_analysis_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "1")

    with pytest.raises(RuntimeError, match="legacy analysis is disabled"):
        research_runner.run_tradex_scope_stability_sweep(
            session_id="legacy-disabled-scope",
            session_scope_ids=("scope-a", "scope-b"),
            random_seeds=(7, 19),
            universe_size=20,
            max_candidates_per_family=1,
        )


def test_tradex_research_runner_stability_sweep_generates_rollup(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()

    family_specs = research_runner._build_family_specs()[:2]
    monkeypatch.setattr(research_runner, "_build_family_specs", lambda: family_specs)
    monkeypatch.setattr(research_runner, "_train_phase4_ranker", lambda *args, **kwargs: {"status": "skipped", "reason": "test"})

    def _fake_research_session(
        *,
        session_id: str,
        random_seed: int,
        universe_size: int,
        max_candidates_per_family: int,
        session_scope_id: str | None = None,
        ret20_source_mode: str = service.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    ) -> dict[str, object]:
        del universe_size, max_candidates_per_family, session_scope_id, ret20_source_mode
        sample_count = 10 + (random_seed % 5)
        eval_window_mode = "fallback" if random_seed % 2 else "standard"
        state = {
            "session_id": session_id,
            "random_seed": random_seed,
            "coverage_waterfall": {
                "confirmed_universe_count": 20,
                "probe_candidate_count": 2,
                "regime_window_count": 3,
                "evaluation_row_count": sample_count,
                "sample_count": sample_count,
                "failure_stage": "passed",
                "insufficient_samples": False,
            },
            "eval_window_mode": eval_window_mode,
            "eval_window_mode_reason": "test",
            "best_result": {
                "evaluation_summary": {
                    "champion_topk_ret20_mean": 0.10,
                    "challenger_topk_ret20_mean": 0.12 + random_seed * 0.001,
                    "champion_topk10_ret20_mean": 0.08,
                    "challenger_topk10_ret20_mean": 0.09,
                    "champion_monthly_top5_capture_mean": 0.20,
                    "challenger_monthly_top5_capture_mean": 0.21,
                    "champion_zero_pass_months": 1,
                    "challenger_zero_pass_months": 1,
                    "champion_worst_regime_ret20_mean": 0.04,
                    "challenger_worst_regime_ret20_mean": 0.05,
                    "champion_dd": 0.30,
                    "challenger_dd": 0.29,
                    "champion_turnover": 0.40,
                    "challenger_turnover": 0.39,
                    "champion_liquidity_fail_rate": 0.10,
                    "challenger_liquidity_fail_rate": 0.09,
                    "windows": [
                        {"champion_top5_ret20_mean": 0.04, "challenger_top5_ret20_mean": 0.05},
                    ],
                }
            },
            "insufficient_samples": False,
            "status": "complete",
        }
        leaderboard = {
            "schema_version": research_runner.SESSION_FAMILY_LEADERBOARD_SCHEMA_VERSION,
            "session_meta": {
                "session_id": session_id,
                "random_seed": random_seed,
                "generated_at": "2025-03-23T00:00:00+09:00",
                "eval_window_mode": eval_window_mode,
                "eval_window_mode_reason": "test",
                "sample_count": sample_count,
                "insufficient_samples": False,
            },
            "overview": {
                "family_count": 1,
                "candidate_count": 1,
                "keep_family_count": 1,
                "hold_family_count": 0,
                "drop_family_count": 0,
                "keep_candidate_count": 1,
                "hold_candidate_count": 0,
                "drop_candidate_count": 0,
                "insufficient_samples": False,
            },
            "family_summary": [
                {
                    "method_family": "family-a",
                    "method_title": "family-a",
                    "method_thesis": "test",
                    "decision": "keep",
                    "decision_reasons": [{"code": "candidate_keep_present", "keep_count": 1}],
                }
            ],
            "candidate_rows": [
                {
                    "method_family": "family-a",
                    "method_title": "candidate-a",
                    "method_thesis": "test",
                    "decision": "keep",
                    "method_signature_hash": "hash-a",
                    "decision_reasons": [{"code": "top5", "status": "pass", "champion_value": 0.10, "candidate_value": 0.12, "delta": 0.02}],
                    "latest_decision": "keep",
                    "latest_decision_reasons": [{"code": "top5", "status": "pass"}],
                    "keep_count": 1,
                    "drop_count": 0,
                    "hold_count": 0,
                    "session_count": 1,
                    "avg_top5_ret20_mean_delta": 0.02,
                    "avg_top10_ret20_mean_delta": 0.01,
                    "avg_monthly_capture_delta": 0.01,
                    "avg_zero_pass_delta": 0.0,
                    "avg_worst_regime_delta": 0.01,
                    "avg_dd_delta": -0.01,
                    "avg_turnover_delta": -0.01,
                    "avg_liquidity_fail_delta": -0.01,
                }
            ],
        }
        research_runner._write_json(research_runner._session_state_file(session_id), state)
        research_runner._write_json(research_runner._session_compare_file(session_id), state)
        research_runner._write_json(research_runner._session_family_leaderboard_file(session_id), leaderboard)
        return state

    monkeypatch.setattr(research_runner, "run_tradex_research_session", _fake_research_session)

    rollup = research_runner.run_tradex_stability_sweep(
        session_id="stability",
        random_seeds=(7, 19),
        universe_size=20,
        max_candidates_per_family=1,
    )

    assert rollup["schema_version"] == research_runner.STABILITY_ROLLUP_SCHEMA_VERSION
    assert rollup["session_meta"]["session_count"] == 2
    assert rollup["session_meta"]["eval_window_mode_counts"]["fallback"] + rollup["session_meta"]["eval_window_mode_counts"]["standard"] == 2
    assert rollup["session_rows"]
    assert all(int(row["sample_count"]) > 0 for row in rollup["session_rows"])
    assert all(row["eval_window_mode"] in {"standard", "fallback"} for row in rollup["session_rows"])

    rollup_path = research_runner._stability_rollup_file()
    rollup_report_path = research_runner._stability_rollup_report_file()
    assert rollup_path.exists()
    assert rollup_report_path.exists()
    stored_rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
    assert stored_rollup["schema_version"] == research_runner.STABILITY_ROLLUP_SCHEMA_VERSION
    assert stored_rollup["session_rows"]
    report_text = rollup_report_path.read_text(encoding="utf-8")
    assert "TRADEX Stability Rollup" in report_text
    assert "eval_window_mode_counts" in report_text


def test_tradex_research_runner_scope_stability_sweep_generates_rollup(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    dependencies._stock_repo = _FakeRepo()
    dependencies._config_repo = object()

    family_specs = research_runner._build_family_specs()[:1]
    monkeypatch.setattr(research_runner, "_build_family_specs", lambda: family_specs)
    monkeypatch.setattr(research_runner, "_train_phase4_ranker", lambda *args, **kwargs: {"status": "skipped", "reason": "test"})

    def _fake_research_session(
        *,
        session_id: str,
        random_seed: int,
        universe_size: int,
        max_candidates_per_family: int,
        session_scope_id: str | None = None,
        ret20_source_mode: str = service.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    ) -> dict[str, object]:
        del universe_size, max_candidates_per_family, ret20_source_mode
        scope_id = session_scope_id or "scope-default"
        good_scope = scope_id.endswith("good")
        mixed_scope = scope_id.endswith("mixed")
        has_samples = good_scope or (mixed_scope and random_seed == 7)
        sample_count = 11 if has_samples else 0
        eval_window_mode = "fallback" if random_seed % 2 else "standard"
        comparison = {
            "champion_topk_ret20_mean": 0.10,
            "challenger_topk_ret20_mean": 0.11 if has_samples else 0.10,
            "champion_topk10_ret20_mean": 0.08,
            "challenger_topk10_ret20_mean": 0.09 if has_samples else 0.08,
            "champion_monthly_top5_capture_mean": 0.20,
            "challenger_monthly_top5_capture_mean": 0.21 if has_samples else 0.20,
            "champion_zero_pass_months": 1,
            "challenger_zero_pass_months": 1,
            "champion_worst_regime_ret20_mean": 0.04,
            "challenger_worst_regime_ret20_mean": 0.05 if has_samples else 0.04,
            "champion_dd": 0.30,
            "challenger_dd": 0.29 if has_samples else 0.30,
            "champion_turnover": 0.40,
            "challenger_turnover": 0.39 if has_samples else 0.40,
            "champion_liquidity_fail_rate": 0.10,
            "challenger_liquidity_fail_rate": 0.09 if has_samples else 0.10,
        }
        state = {
            "session_id": session_id,
            "session_scope_id": scope_id,
            "random_seed": random_seed,
            "coverage_waterfall": {
                "confirmed_universe_count": 20,
                "probe_selection_count": 2,
                "candidate_rows_built_count": 1,
                "eligible_candidate_count": 1 if has_samples else 0,
                "ret20_computable_count": 1 if has_samples else 0,
                "compare_row_count": 1,
                "sample_rows_retained_count": 1 if has_samples else 0,
                "evaluation_row_count": sample_count,
                "sample_count": sample_count,
                "sample_count_min": sample_count,
                "sample_count_max": sample_count,
                "failure_stage": "passed" if has_samples else "eligibility_passed",
                "first_zero_stage": "passed" if has_samples else "eligibility_passed",
                "insufficient_samples": not has_samples,
            },
            "eval_window_mode": eval_window_mode,
            "eval_window_mode_reason": "test",
            "best_result": {
                "selection_compare": comparison,
                "evaluation_summary": {
                    "champion_topk_ret20_mean": comparison["champion_topk_ret20_mean"],
                    "challenger_topk_ret20_mean": comparison["challenger_topk_ret20_mean"],
                    "champion_topk10_ret20_mean": comparison["champion_topk10_ret20_mean"],
                    "challenger_topk10_ret20_mean": comparison["challenger_topk10_ret20_mean"],
                    "champion_monthly_top5_capture_mean": comparison["champion_monthly_top5_capture_mean"],
                    "challenger_monthly_top5_capture_mean": comparison["challenger_monthly_top5_capture_mean"],
                    "champion_zero_pass_months": comparison["champion_zero_pass_months"],
                    "challenger_zero_pass_months": comparison["challenger_zero_pass_months"],
                    "champion_worst_regime_ret20_mean": comparison["champion_worst_regime_ret20_mean"],
                    "challenger_worst_regime_ret20_mean": comparison["challenger_worst_regime_ret20_mean"],
                    "champion_dd": comparison["champion_dd"],
                    "challenger_dd": comparison["challenger_dd"],
                    "champion_turnover": comparison["champion_turnover"],
                    "challenger_turnover": comparison["challenger_turnover"],
                    "champion_liquidity_fail_rate": comparison["champion_liquidity_fail_rate"],
                    "challenger_liquidity_fail_rate": comparison["challenger_liquidity_fail_rate"],
                    "windows": [{"champion_top5_ret20_mean": 0.04, "challenger_top5_ret20_mean": 0.05 if has_samples else 0.04}],
                },
            } if has_samples else {},
            "insufficient_samples": not has_samples,
            "status": "complete",
        }
        leaderboard = {
            "schema_version": research_runner.SESSION_FAMILY_LEADERBOARD_SCHEMA_VERSION,
            "session_meta": {
                "session_id": session_id,
                "random_seed": random_seed,
                "generated_at": "2025-03-23T00:00:00+09:00",
                "eval_window_mode": eval_window_mode,
                "eval_window_mode_reason": "test",
                "sample_count": sample_count,
                "insufficient_samples": not has_samples,
            },
            "overview": {
                "family_count": 1,
                "candidate_count": 1,
                "keep_family_count": 1 if has_samples else 0,
                "hold_family_count": 0,
                "drop_family_count": 0 if has_samples else 1,
                "keep_candidate_count": 1 if has_samples else 0,
                "hold_candidate_count": 0,
                "drop_candidate_count": 0 if has_samples else 1,
                "insufficient_samples": not has_samples,
            },
            "family_summary": [
                {
                    "method_family": "family-a",
                    "method_title": "family-a",
                    "method_thesis": "test",
                    "decision": "keep" if has_samples else "drop",
                    "decision_reasons": [{"code": "candidate_keep_present", "keep_count": 1}] if has_samples else [{"code": "all_candidates_drop", "drop_count": 1}],
                }
            ],
            "candidate_rows": [
                {
                    "method_family": "family-a",
                    "method_title": "candidate-a",
                    "method_thesis": "test",
                    "decision": "keep" if has_samples else "drop",
                    "method_signature_hash": f"hash-{scope_id}",
                    "decision_reasons": [{"code": "top5", "status": "pass", "champion_value": 0.10, "candidate_value": 0.11 if has_samples else 0.10, "delta": 0.01 if has_samples else 0.0}],
                    "latest_decision": "keep" if has_samples else "drop",
                    "latest_decision_reasons": [{"code": "top5", "status": "pass"}],
                    "keep_count": 1 if has_samples else 0,
                    "drop_count": 0 if has_samples else 1,
                    "hold_count": 0,
                    "session_count": 1,
                    "avg_top5_ret20_mean_delta": 0.01 if has_samples else 0.0,
                    "avg_top10_ret20_mean_delta": 0.01 if has_samples else 0.0,
                    "avg_monthly_capture_delta": 0.01 if has_samples else 0.0,
                    "avg_zero_pass_delta": 0.0,
                    "avg_worst_regime_delta": 0.01 if has_samples else 0.0,
                    "avg_dd_delta": -0.01 if has_samples else 0.0,
                    "avg_turnover_delta": -0.01 if has_samples else 0.0,
                    "avg_liquidity_fail_delta": -0.01 if has_samples else 0.0,
                }
            ],
        }
        research_runner._write_json(research_runner._session_state_file(session_id), state)
        research_runner._write_json(research_runner._session_compare_file(session_id), state)
        research_runner._write_json(research_runner._session_family_leaderboard_file(session_id), leaderboard)
        return state

    monkeypatch.setattr(research_runner, "run_tradex_research_session", _fake_research_session)

    rollup = research_runner.run_tradex_scope_stability_sweep(
        session_id="scope-stability",
        session_scope_ids=("scope-good", "scope-mixed", "scope-bad"),
        random_seeds=(7, 19),
        universe_size=20,
        max_candidates_per_family=1,
    )

    assert rollup["schema_version"] == research_runner.SCOPE_STABILITY_ROLLUP_SCHEMA_VERSION
    assert rollup["status"] == "invalid"
    assert rollup["overview"]["usable_scope_count"] == 1
    assert rollup["overview"]["unstable_scope_count"] == 1
    assert rollup["overview"]["unusable_scope_count"] == 1
    assert rollup["overview"]["insufficient_samples"] is True
    assert rollup["session_rows"]
    assert any(row["first_zero_stage"] == "passed" for row in rollup["session_rows"])
    assert any(row["first_zero_stage"] == "eligibility_passed" for row in rollup["session_rows"])
    assert all("session_scope_id" in row for row in rollup["session_rows"])
    assert {row["decision"] for row in rollup["scope_summary"]} == {"usable", "unstable", "unusable"}

    rollup_path = research_runner._scope_stability_rollup_file()
    rollup_report_path = research_runner._scope_stability_rollup_report_file()
    assert rollup_path.exists()
    assert rollup_report_path.exists()
    stored_rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
    assert stored_rollup["schema_version"] == research_runner.SCOPE_STABILITY_ROLLUP_SCHEMA_VERSION
    assert stored_rollup["scope_summary"]
    report_text = rollup_report_path.read_text(encoding="utf-8")
    assert "TRADEX Scope Stability Rollup" in report_text
    assert "first_zero_stage_counts" in report_text
    assert "usable" in report_text


def test_tradex_data_smoke_check_reports_confirmed_counts(monkeypatch, tmp_path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    source_db = data_dir / "stocks.duckdb"
    result_db = tmp_path / "result.duckdb"

    import duckdb

    with duckdb.connect(str(source_db)) as conn:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE)")
        conn.execute("INSERT INTO daily_bars VALUES ('1001', 20250101, 1, 1, 1, 1, 1), ('1002', 20250101, 1, 1, 1, 1, 1)")
        conn.execute("CREATE TABLE market_regime_daily (dt INTEGER, label_version TEXT, regime_id TEXT, regime_tag TEXT, regime_score DOUBLE)")
        conn.execute("INSERT INTO market_regime_daily VALUES (20250101, 'v1', 'r1', 'up', 0.1)")
    with duckdb.connect(str(result_db)) as conn:
        conn.execute("CREATE TABLE regime_daily (publish_id TEXT, as_of_date DATE, regime_tag TEXT, regime_score DOUBLE, breadth_score DOUBLE, volatility_state TEXT)")
        conn.execute("INSERT INTO regime_daily VALUES ('pub_1', '2025-01-01', 'risk_on', 0.1, 0.2, 'normal')")

    monkeypatch.setattr(dependencies, "_stock_repo", None)
    monkeypatch.setattr(dependencies, "_favorites_repo", None)
    monkeypatch.setattr(dependencies, "_config_repo", None)
    monkeypatch.setattr(dependencies, "_screener_repo", None)
    monkeypatch.setattr(service, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(research_runner.app_config, "DATA_DIR", data_dir)
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))

    payload = smoke_check.collect_tradex_data_smoke()
    assert payload["confirmed_universe_count"] == 2
    assert payload["source_db"]["table_exists"] is True
    assert payload["source_db"]["row_count"] == 2
    assert payload["source_db"]["distinct_code_count"] == 2
    assert payload["analysis_db"]["table_exists"] is True
    assert payload["analysis_db"]["row_count"] == 1
    assert payload["result_db"]["table_exists"] is True
    assert payload["result_db"]["row_count"] == 1


def test_tradex_research_runner_best_key_tiebreak() -> None:
    def candidate(plan_id: str, top5_mean: float, worst_margin: float, dd: float, turnover: float) -> dict[str, object]:
        return {
            "plan_id": plan_id,
            "evaluation_summary": {
                "challenger_topk_ret20_mean": top5_mean,
                "challenger_dd": dd,
                "challenger_turnover": turnover,
                "windows": [
                    {"champion_top5_ret20_mean": 0.10, "challenger_top5_ret20_mean": 0.10 + worst_margin},
                    {"champion_top5_ret20_mean": 0.09, "challenger_top5_ret20_mean": 0.09 + worst_margin},
                ],
            },
        }

    ranked = sorted(
        [
            candidate("top5", 0.12, 0.01, 0.20, 0.30),
            candidate("worst-margin", 0.11, 0.04, 0.20, 0.30),
            candidate("dd", 0.11, 0.04, 0.10, 0.30),
            candidate("turnover", 0.11, 0.04, 0.10, 0.20),
        ],
        key=research_runner._family_best_key,
    )

    assert [item["plan_id"] for item in ranked] == ["top5", "turnover", "dd", "worst-margin"]
