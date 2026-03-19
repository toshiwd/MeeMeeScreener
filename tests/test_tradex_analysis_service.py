from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.backend.services import tradex_analysis_service as service


class _FakeRepo:
    def get_ml_analysis_pred(self, code: str, asof_dt: int | None):
        assert code == "7203"
        assert asof_dt is None
        return (
            20260319,
            0.81,
            0.07,
            0.63,
            0.31,
            0.54,
            0.23,
            None,
            None,
            None,
            None,
            None,
            None,
            0.012,
            0.010,
            None,
            None,
            "v1",
        )

    def get_sell_analysis_snapshot(self, code: str, asof_dt: int | None):
        assert code == "7203"
        assert asof_dt is None
        return (
            20260319,
            100.0,
            -0.02,
            0.34,
            0.18,
            0.012,
            1.1,
            20260326,
            0.12,
            0.18,
            0.24,
            0.56,
            0.42,
            0.38,
            20.1,
            60.4,
            -0.011,
            -0.008,
            -0.013,
            0,
            True,
            False,
        )


class _CountingRepo(_FakeRepo):
    def __init__(self) -> None:
        self.ml_calls = 0
        self.sell_calls = 0

    def get_ml_analysis_pred(self, code: str, asof_dt: int | None):
        self.ml_calls += 1
        return super().get_ml_analysis_pred(code, asof_dt)

    def get_sell_analysis_snapshot(self, code: str, asof_dt: int | None):
        self.sell_calls += 1
        return super().get_sell_analysis_snapshot(code, asof_dt)


@dataclass(frozen=True)
class _FakeOutput:
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


def setup_function(_function) -> None:
    service.reset_tradex_detail_analysis_observability()


def test_build_tradex_detail_analysis_snapshot_calls_tradex_analysis(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run_tradex_analysis(input_contract):
        captured["input_contract"] = input_contract
        return _FakeOutput(
            {
                "symbol": "7203",
                "asof": "2026-03-19",
                "side_ratios": {"buy": 0.81, "neutral": 0.12, "sell": 0.07},
                "confidence": 0.72,
                "reasons": ["tone=up", "pattern=breakout", "version=2026-03-20"],
                "candidate_comparisons": [
                    {
                        "candidate_key": "up",
                        "baseline_key": "up",
                        "comparison_scope": "decision_scenarios",
                        "score": 0.81,
                        "score_delta": 0.0,
                        "rank": 1,
                        "reasons": ["key=up"],
                        "publish_ready": True,
                    }
                ],
                "publish_readiness": {"ready": False, "status": "not_evaluated", "reasons": []},
                "override_state": {"present": False, "source": None, "logic_key": None, "logic_version": None, "reason": None},
                "source": "tradex_analysis",
                "schema_version": "tradex_analysis_output_v1",
            }
        )

    monkeypatch.setattr(service, "run_tradex_analysis", fake_run_tradex_analysis)

    result = service.build_tradex_detail_analysis_snapshot(
        code="7203",
        asof_dt=None,
        repo=_FakeRepo(),
        enabled=True,
    )

    assert result["available"] is True
    assert result["reason"] is None
    assert result["analysis"]["symbol"] == "7203"
    assert result["analysis"]["candidate_comparisons"][0]["candidate_key"] == "up"

    input_contract = captured["input_contract"]
    assert input_contract.symbol == "7203"
    assert input_contract.asof == "2026-03-19"
    assert input_contract.analysis_p_up == 0.81
    assert input_contract.analysis_p_down == 0.07
    assert input_contract.analysis_p_turn_up == 0.54
    assert input_contract.analysis_p_turn_down == 0.23
    assert input_contract.analysis_ev_net == 0.01
    assert input_contract.sell_analysis["shortScore"] == 0.56


def test_build_tradex_detail_analysis_snapshot_degrades_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(service, "run_tradex_analysis", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not run")))

    result = service.build_tradex_detail_analysis_snapshot(
        code="7203",
        asof_dt=None,
        repo=_FakeRepo(),
        enabled=False,
    )

    assert result == {"available": False, "reason": "feature flag disabled", "analysis": None}


def test_build_tradex_detail_analysis_snapshot_caches_success_and_records_observability(monkeypatch) -> None:
    repo = _CountingRepo()
    calls = {"count": 0}

    def fake_run_tradex_analysis(input_contract):
        calls["count"] += 1
        return _FakeOutput(
            {
                "symbol": input_contract.symbol,
                "asof": input_contract.asof,
                "side_ratios": {"buy": 0.61, "neutral": 0.24, "sell": 0.15},
                "confidence": 0.77,
                "reasons": ["tone=up"],
                "candidate_comparisons": [],
                "publish_readiness": {"ready": True, "status": "ready", "reasons": []},
                "override_state": {"present": False, "source": None, "logic_key": None, "logic_version": None, "reason": None},
                "source": "tradex_analysis",
                "schema_version": "tradex_analysis_output_v1",
            }
        )

    monkeypatch.setattr(service, "run_tradex_analysis", fake_run_tradex_analysis)

    first = service.build_tradex_detail_analysis_snapshot(code="7203", asof_dt=None, repo=repo, enabled=True)
    second = service.build_tradex_detail_analysis_snapshot(code="7203", asof_dt=None, repo=repo, enabled=True)

    assert first["available"] is True
    assert second["available"] is True
    assert first["analysis"]["symbol"] == "7203"
    assert second["analysis"]["symbol"] == "7203"
    assert calls["count"] == 1
    assert repo.ml_calls == 1
    assert repo.sell_calls == 1

    observability = service.get_tradex_detail_analysis_observability()
    assert observability["success_count"] == 2
    assert observability["failure_count"] == 0
    assert observability["cache_hit_count"] == 1
    assert observability["cache_miss_count"] == 1
    assert observability["unavailable_reason_counts"] == {}
    assert observability["latency_ms_last"] is not None
    assert observability["latency_ms_avg"] is not None
    assert observability["latency_ms_max"] is not None


def test_build_tradex_detail_analysis_snapshot_records_unavailable_reason(monkeypatch) -> None:
    class _EmptyRepo(_FakeRepo):
        def get_ml_analysis_pred(self, code: str, asof_dt: int | None):
            return None

    monkeypatch.setattr(service, "run_tradex_analysis", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not run")))

    result = service.build_tradex_detail_analysis_snapshot(
        code="7203",
        asof_dt=None,
        repo=_EmptyRepo(),
        enabled=True,
    )

    assert result == {"available": False, "reason": "analysis unavailable", "analysis": None}
    observability = service.get_tradex_detail_analysis_observability()
    assert observability["failure_count"] == 1
    assert observability["unavailable_reason_counts"]["analysis unavailable"] == 1
    assert observability["last_reason"] == "analysis unavailable"
    assert observability["last_reason_at"] is not None
