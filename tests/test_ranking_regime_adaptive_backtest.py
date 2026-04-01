from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from app.core.config import config
from scripts import ranking_regime_adaptive_backtest as regime_backtest


def test_regime_adaptive_selection_uses_environment_specific_recipes() -> None:
    panel = pd.DataFrame(
        [
            {"as_of": 20240102, "rank": 1, "code": "1111", "entryQualified": True, "setupType": "watch", "displayScore": 0.35, "forward_return_5": 0.01, "forward_return_20": 0.02, "forward_return_60": 0.03},
            {"as_of": 20240102, "rank": 2, "code": "2222", "entryQualified": True, "setupType": "breakout", "displayScore": 0.80, "forward_return_5": 0.04, "forward_return_20": 0.08, "forward_return_60": 0.10},
            {"as_of": 20240102, "rank": 3, "code": "3333", "entryQualified": False, "setupType": "reject", "displayScore": 0.95, "forward_return_5": -0.02, "forward_return_20": -0.03, "forward_return_60": -0.04},
            {"as_of": 20240103, "rank": 1, "code": "4444", "entryQualified": True, "setupType": "breakout", "displayScore": 0.92, "forward_return_5": 0.05, "forward_return_20": 0.09, "forward_return_60": 0.12},
            {"as_of": 20240103, "rank": 2, "code": "5555", "entryQualified": True, "setupType": "watch", "displayScore": 0.60, "forward_return_5": 0.02, "forward_return_20": 0.03, "forward_return_60": 0.04},
            {"as_of": 20240103, "rank": 3, "code": "6666", "entryQualified": False, "setupType": "reject", "displayScore": 0.98, "forward_return_5": -0.03, "forward_return_20": -0.04, "forward_return_60": -0.05},
            {"as_of": 20240104, "rank": 1, "code": "7777", "entryQualified": True, "setupType": "breakout", "displayScore": 0.88, "forward_return_5": 0.03, "forward_return_20": 0.06, "forward_return_60": 0.07},
            {"as_of": 20240104, "rank": 2, "code": "8888", "entryQualified": True, "setupType": "watch", "displayScore": 0.77, "forward_return_5": 0.01, "forward_return_20": 0.02, "forward_return_60": 0.03},
            {"as_of": 20240104, "rank": 3, "code": "9999", "entryQualified": False, "setupType": "reject", "displayScore": 0.99, "forward_return_5": -0.05, "forward_return_20": -0.06, "forward_return_60": -0.07},
        ]
    )
    regime_lookup = pd.DataFrame(
        [
            {"dt": 20240102, "regime_id": "risk_on_trend", "regime_score": 1.2},
            {"dt": 20240103, "regime_id": "neutral_range", "regime_score": 0.1},
            {"dt": 20240104, "regime_id": "risk_off_trend", "regime_score": -0.8},
        ]
    )

    selected_hold = regime_backtest._select_panel_for_plan(  # type: ignore[attr-defined]
        panel,
        regime_lookup=regime_lookup,
        plan=regime_backtest.PLAN_MAP["regime_adaptive_hold"],
        bucket_size=10,
    )
    selected_selective = regime_backtest._select_panel_for_plan(  # type: ignore[attr-defined]
        panel,
        regime_lookup=regime_lookup,
        plan=regime_backtest.PLAN_MAP["regime_adaptive_selective"],
        bucket_size=10,
    )

    assert selected_hold.loc[selected_hold["as_of"] == 20240102, "code"].tolist()[0] == "2222"
    assert selected_hold.loc[selected_hold["as_of"] == 20240103, "code"].tolist()[0] == "4444"
    assert 20240104 not in set(selected_hold["as_of"].tolist())
    assert selected_selective.loc[selected_selective["as_of"] == 20240104, "code"].tolist()[0] == "7777"


def test_run_ranking_regime_adaptive_backtest_writes_report(monkeypatch, tmp_path: Path) -> None:
    panel = pd.DataFrame(
        [
            {"as_of": 20240102, "rank": 1, "code": "1111", "entryQualified": True, "setupType": "watch", "displayScore": 0.35, "forward_return_5": 0.01, "forward_return_20": 0.02, "forward_return_60": 0.03},
            {"as_of": 20240102, "rank": 2, "code": "2222", "entryQualified": True, "setupType": "breakout", "displayScore": 0.80, "forward_return_5": 0.04, "forward_return_20": 0.08, "forward_return_60": 0.10},
            {"as_of": 20240102, "rank": 3, "code": "3333", "entryQualified": False, "setupType": "reject", "displayScore": 0.95, "forward_return_5": -0.02, "forward_return_20": -0.03, "forward_return_60": -0.04},
            {"as_of": 20240103, "rank": 1, "code": "4444", "entryQualified": True, "setupType": "breakout", "displayScore": 0.92, "forward_return_5": 0.05, "forward_return_20": 0.09, "forward_return_60": 0.12},
            {"as_of": 20240103, "rank": 2, "code": "5555", "entryQualified": True, "setupType": "watch", "displayScore": 0.60, "forward_return_5": 0.02, "forward_return_20": 0.03, "forward_return_60": 0.04},
            {"as_of": 20240103, "rank": 3, "code": "6666", "entryQualified": False, "setupType": "reject", "displayScore": 0.98, "forward_return_5": -0.03, "forward_return_20": -0.04, "forward_return_60": -0.05},
            {"as_of": 20240104, "rank": 1, "code": "7777", "entryQualified": True, "setupType": "breakout", "displayScore": 0.88, "forward_return_5": 0.03, "forward_return_20": 0.06, "forward_return_60": 0.07},
            {"as_of": 20240104, "rank": 2, "code": "8888", "entryQualified": True, "setupType": "watch", "displayScore": 0.77, "forward_return_5": 0.01, "forward_return_20": 0.02, "forward_return_60": 0.03},
            {"as_of": 20240104, "rank": 3, "code": "9999", "entryQualified": False, "setupType": "reject", "displayScore": 0.99, "forward_return_5": -0.05, "forward_return_20": -0.06, "forward_return_60": -0.07},
        ]
    )
    regime_lookup = pd.DataFrame(
        [
            {"dt": 20240102, "regime_id": "risk_on_trend", "regime_score": 1.2},
            {"dt": 20240103, "regime_id": "neutral_range", "regime_score": 0.1},
            {"dt": 20240104, "regime_id": "risk_off_trend", "regime_score": -0.8},
        ]
    )

    def _fake_raw_backtest(*_args, **kwargs):
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        working_db = output_dir / "_working" / Path(config.DB_PATH).name
        working_db.parent.mkdir(parents=True, exist_ok=True)
        working_db.touch()
        panel.to_parquet(output_dir / "daily_selection_panel.parquet", index=False)
        return {
            "selection_variant": "baseline",
            "ranking_contract": {
                "tf": "D",
                "which": "latest",
                "direction": "up",
                "mode": "hybrid",
                "risk_mode": "balanced",
                "limit": 200,
            },
            "cohort_metrics": {},
            "coverage_metrics": {},
        }

    monkeypatch.setattr(
        regime_backtest.quality_backtest,
        "_run_raw_backtest",
        _fake_raw_backtest,
    )
    monkeypatch.setattr(
        regime_backtest,
        "_load_market_regime_lookup",
        lambda **_kwargs: regime_lookup,
    )
    monkeypatch.setattr(
        regime_backtest,
        "_resolve_existing_panel_path",
        lambda *_args, **_kwargs: None,
    )

    result = regime_backtest.run_ranking_regime_adaptive_backtest(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 4),
        output_dir=tmp_path,
        round_trip_cost=0.002,
    )

    payload = result["payload"]
    assert payload["schema_version"] == regime_backtest.REGIME_SCRIPT_SCHEMA_VERSION
    assert payload["verdict"] in {"usable", "watch", "not_usable_yet"}
    assert (tmp_path / "ranking_regime_adaptive_backtest.json").exists()
    assert (tmp_path / "ranking_regime_adaptive_backtest.md").exists()
    assert payload["best_variant"] in {"baseline", "regime_adaptive_hold", "regime_adaptive_selective"}
