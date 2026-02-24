from __future__ import annotations

from app.backend.services.toredex_config import ToredexConfig
from app.backend.services.toredex_hash import hash_payload
from app.backend.services.toredex_paths import resolve_runs_root
from app.backend.services.toredex_policy import build_decision


def test_toredex_hash_is_deterministic_and_ignores_runtime_fields() -> None:
    left = {
        "asOf": "2025-01-10",
        "actions": [{"ticker": "1111", "deltaUnits": 2}],
        "createdAt": "2025-01-10T10:00:00Z",
    }
    right = {
        "createdAt": "2025-01-10T10:01:00Z",
        "actions": [{"ticker": "1111", "deltaUnits": 2}],
        "asOf": "2025-01-10",
    }
    assert hash_payload(left) == hash_payload(right)


def test_toredex_runs_root_priority(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TOREDEX_RUNS_DIR", str(tmp_path / "env_runs"))
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(tmp_path / "data"))

    from_cfg = resolve_runs_root(str(tmp_path / "cfg_runs"))
    assert from_cfg == (tmp_path / "cfg_runs").resolve()

    from_env = resolve_runs_root(None)
    assert from_env == (tmp_path / "env_runs").resolve()

    monkeypatch.delenv("TOREDEX_RUNS_DIR", raising=False)
    from_data = resolve_runs_root(None)
    assert from_data == (tmp_path / "data" / "runs").resolve()


def test_toredex_policy_decision_is_deterministic() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v1",
            "mode": "LIVE",
            "topN": 50,
            "runsDir": None,
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "switchMinEvGap": 0.03,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "gameOverPct": -20.0,
                "takeProfitHintPct": 10.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v1",
        "positions": [],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": 0.1,
                    "upProb": 0.7,
                    "revRisk": 0.2,
                    "entryScore": 0.9,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                }
            ],
            "sell": [],
        },
        "meta": {"noFutureLeakOk": True},
    }

    left = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    right = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert left == right
    assert left["actions"]
    assert left["actions"][0]["deltaUnits"] in {2, 3, 5}
