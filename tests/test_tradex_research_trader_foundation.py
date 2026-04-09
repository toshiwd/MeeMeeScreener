from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import httpx
import pytest

import app.backend.api.dependencies as dependencies
from app.backend.services import tradex_research_preflight as preflight_service
from app.backend.services import tradex_research_os_store as os_store
from app.backend.services import tradex_research_trader_foundation as foundation_service
from app.backend.tools import tradex_research_os_runner as os_runner
from app.backend.tools import tradex_research_runner as tradex_runner
from tests.test_tradex_research_os_phase1 import (
    _hypothesis_payload,
    _passing_preflight,
    _reset_tradex_root,
    _write_fake_session_artifacts,
)


async def _fake_llm_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):  # noqa: ARG001
    assert "observation_snapshot=" in user_prompt
    assert images == []
    return (
        json.dumps(
            {
                "machine_action_state": "enter",
                "human_readable_judgement": "buy",
                "buy_score": 0.74,
                "environment_score": 0.71,
                "trend_score": 0.68,
                "trigger_score": 0.79,
                "risk_score": 0.52,
                "invalidation_price": 132.5,
                "invalidation_reason_code": "daily_swing_low_break",
                "reason_codes": ["close_breakout_20", "ma_trend_aligned"],
                "explanation": "20日高値終値ブレイクと移動平均整列を確認。",
                "confidence": 0.73,
            },
            ensure_ascii=False,
        ),
        {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
    )


class _ValidBarRepo:
    def __init__(self) -> None:
        self.rows = []
        current = date(2024, 8, 1)
        price = 100.0
        for idx in range(240):
            drift = 0.18 if idx < 150 else 0.45
            pullback = -0.22 if idx % 11 == 0 else 0.0
            open_price = price + 0.08
            close_price = price + drift + pullback
            high_price = max(open_price, close_price) + (1.35 if idx >= 180 else 0.9)
            low_price = min(open_price, close_price) - 0.7
            volume = 1_000_000.0 + (idx * 4_000.0) + (220_000.0 if idx >= 180 else 0.0)
            self.rows.append(
                (
                    int(current.strftime("%Y%m%d")),
                    round(open_price, 2),
                    round(high_price, 2),
                    round(low_price, 2),
                    round(close_price, 2),
                    round(volume, 2),
                )
            )
            current += timedelta(days=1)
            price = close_price

    def get_daily_bars(self, code: str, limit: int = 400, asof_dt: int | None = None):  # noqa: ARG002
        del code, asof_dt
        return self.rows[-max(1, min(limit, len(self.rows))):]


def _strategy_hypothesis_payload() -> dict[str, object]:
    payload = _hypothesis_payload()
    payload["strategy_target"] = {
        "code": "1001",
        "as_of_date": 20250210,
        "side": "long",
        "judgement_type": "close_based_daily_buy_v1",
    }
    payload["strategy_judgement"] = {
        "primary_adapter_id": "numeric_baseline_v1",
        "adapter_ids": ["numeric_baseline_v1", "structured_reasoner_v1"],
        "observation_lookback_bars": 60,
        "teacher_horizon_bars": 20,
    }
    return payload


def _configure_llm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(foundation_service.TRADEX_TRADER_LLM_ENDPOINT_ENV, "https://example.invalid/v1")
    monkeypatch.setenv(foundation_service.TRADEX_TRADER_LLM_MODEL_ENV, "gpt-test")
    monkeypatch.setenv(foundation_service.TRADEX_TRADER_LLM_API_KEY_ENV, "test-secret")


def test_observation_snapshot_contract_from_daily_bars(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_llm_env(monkeypatch)
    monkeypatch.setattr(foundation_service.OpenAICompatibleProvider, "generate", _fake_llm_generate)
    hypothesis = _strategy_hypothesis_payload()
    artifacts = foundation_service.build_strategy_foundation_artifacts(
        experiment_id="exp_foundation_test",
        hypothesis=hypothesis,
        stock_repo=_ValidBarRepo(),
    )
    assert artifacts is not None
    observation_snapshot = artifacts["observation_snapshot"]
    assert observation_snapshot["target"]["judgement_type"] == "close_based_daily_buy_v1"
    assert observation_snapshot["market_context"]["price_source"] == "daily_bars"
    assert observation_snapshot["derived_features"]["moving_averages"]["ma20"] > 0
    assert "candle_structure" in observation_snapshot["derived_features"]
    assert "breakout_context" in observation_snapshot["derived_features"]


def test_strategy_judgement_consumes_same_observation_for_two_adapters(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_llm_env(monkeypatch)
    monkeypatch.setattr(foundation_service.OpenAICompatibleProvider, "generate", _fake_llm_generate)
    hypothesis = _strategy_hypothesis_payload()
    artifacts = foundation_service.build_strategy_foundation_artifacts(
        experiment_id="exp_foundation_test",
        hypothesis=hypothesis,
        stock_repo=_ValidBarRepo(),
    )
    assert artifacts is not None
    observation_snapshot = artifacts["observation_snapshot"]
    strategy_judgement = artifacts["strategy_judgement"]
    adapter_ids = [row["adapter_id"] for row in strategy_judgement["adapter_outputs"]]
    assert adapter_ids == ["numeric_baseline_v1", "structured_reasoner_v1"]
    assert strategy_judgement["observation_snapshot_hash"] == observation_snapshot["observation_snapshot_hash"]
    assert strategy_judgement["primary_adapter_id"] == "numeric_baseline_v1"
    assert strategy_judgement["machine_action_state"] in {"enter", "wait", "skip"}
    assert strategy_judgement["human_readable_judgement"] in {"buy", "hold", "reject"}


def test_teacher_evaluation_row_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_llm_env(monkeypatch)
    monkeypatch.setattr(foundation_service.OpenAICompatibleProvider, "generate", _fake_llm_generate)
    hypothesis = _strategy_hypothesis_payload()
    artifacts = foundation_service.build_strategy_foundation_artifacts(
        experiment_id="exp_foundation_test",
        hypothesis=hypothesis,
        stock_repo=_ValidBarRepo(),
    )
    assert artifacts is not None
    teacher_row = artifacts["teacher_evaluation_row"]
    realized = teacher_row["realized_outcome_window"]
    assert realized["future_bar_count"] == 20
    assert realized["complete_horizon"] is True
    assert realized["anchor_close_price"] > 0
    assert realized["max_favorable_excursion_close_basis"] is not None
    assert "strategy_judgement_hash" in teacher_row


def test_run_hypothesis_writes_strategy_foundation_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = _reset_tradex_root(monkeypatch, tmp_path)
    _configure_llm_env(monkeypatch)
    monkeypatch.setattr(foundation_service.OpenAICompatibleProvider, "generate", _fake_llm_generate)
    hypothesis = _strategy_hypothesis_payload()
    hypothesis_path = tmp_path / "strategy-foundation-hypothesis.json"
    hypothesis_path.write_text(json.dumps(hypothesis, ensure_ascii=False, indent=2), encoding="utf-8")

    fake_artifacts = _write_fake_session_artifacts(root, "os-smoke-session", "regime-aware")
    monkeypatch.setattr(dependencies, "_stock_repo", _ValidBarRepo(), raising=False)
    monkeypatch.setattr(
        tradex_runner,
        "run_tradex_research_session",
        lambda **kwargs: {
            "status": "complete",
            "session_id": kwargs["session_id"],
            "family_results": [
                {
                    "family_id": fake_artifacts["family_id"],
                    "method_family": "regime-aware",
                    "compare_path": str(fake_artifacts["family_compare_path"]),
                }
            ],
        },
    )
    monkeypatch.setattr(preflight_service, "evaluate_preflight", lambda **kwargs: _passing_preflight(**kwargs))

    result = os_runner.run_hypothesis(hypothesis_path)
    assert result["status"] == "ok"
    assert Path(result["observation_snapshot_path"]).exists()
    assert Path(result["strategy_judgement_path"]).exists()
    assert Path(result["teacher_evaluation_row_path"]).exists()
    assert Path(result["judge_input_path"]).exists()
    assert Path(result["judge_decision_path"]).exists()
    assert Path(result["authoritative_decision_path"]).exists()
    assert Path(result["research_memory_path"]).exists()

    manifest = os_store.read_json(Path(result["experiment_manifest_path"]))
    artifact_names = {row["name"] for row in manifest["generated_artifacts"]}
    assert {"observation_snapshot", "strategy_judgement", "teacher_evaluation_row"} <= artifact_names

    observation_snapshot = os_store.read_json(Path(result["observation_snapshot_path"]))
    strategy_judgement = os_store.read_json(Path(result["strategy_judgement_path"]))
    teacher_row = os_store.read_json(Path(result["teacher_evaluation_row_path"]))
    assert strategy_judgement["observation_snapshot_hash"] == observation_snapshot["observation_snapshot_hash"]
    assert teacher_row["strategy_judgement_hash"] == strategy_judgement["strategy_judgement_hash"]
    assert Path(result["family_compare_path"]).name == "compare.json"
    assert "research_families" in result["family_compare_path"]


def test_structured_reasoner_invalid_output_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_llm_env(monkeypatch)

    async def _invalid_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):  # noqa: ARG001
        return "not-json", None

    monkeypatch.setattr(foundation_service.OpenAICompatibleProvider, "generate", _invalid_generate)

    with pytest.raises(ValueError, match="structured_reasoner_v1_invalid_output_json"):
        foundation_service.build_strategy_foundation_artifacts(
            experiment_id="exp_foundation_test",
            hypothesis=_strategy_hypothesis_payload(),
            stock_repo=_ValidBarRepo(),
        )


def test_structured_reasoner_timeout_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_llm_env(monkeypatch)

    async def _timeout_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):  # noqa: ARG001
        raise httpx.TimeoutException("timeout")

    monkeypatch.setattr(foundation_service.OpenAICompatibleProvider, "generate", _timeout_generate)

    with pytest.raises(RuntimeError, match="structured_reasoner_v1_timeout"):
        foundation_service.build_strategy_foundation_artifacts(
            experiment_id="exp_foundation_test",
            hypothesis=_strategy_hypothesis_payload(),
            stock_repo=_ValidBarRepo(),
        )
