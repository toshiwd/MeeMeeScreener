from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.backend.api.dependencies as dependencies
import app.backend.services.tradex_experiment_service as service
import app.backend.tools.tradex_research_runner as research_runner
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
        for idx in range(20):
            dt = 20241201 + idx
            close = 100.0 + idx
            rows.append((dt, close - 1.5, close + 1.5, close - 2.5, close, 1_000_000.0))
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
) -> dict[str, object]:
    months = [{"month": f"2025-{idx + 1:02d}", "model_ret20_mean": value} for idx, value in enumerate(monthly_model_means)]
    return {
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
            {"probe_id": "probe-1", "code": "1001", "date": "2025-01-05", "label": "probe-1"},
            {"probe_id": "probe-2", "code": "1002", "date": "2025-01-15", "label": "probe-2"},
            {"probe_id": "probe-3", "code": "1003", "date": "2025-02-05", "label": "probe-3"},
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
        assert len(candidate_a_run["engine_diagnostics"]["probes"]) == 3
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
        assert len(candidate_runs["candidate-a"]["diagnostics"]["probe_row_comparisons"]) == 3
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

    report_text = report_path.read_text(encoding="utf-8")
    assert "compare artifact が正本" in report_text
    assert "markdown report は派生物" in report_text
    assert result1["best_result"]["candidate_method"]["method_title"] in report_text

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
