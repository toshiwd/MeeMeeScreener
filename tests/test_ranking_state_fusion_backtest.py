from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts import ranking_state_fusion_backtest as fusion_backtest


def test_state_fusion_selection_prioritizes_breakout_and_limits_rejects() -> None:
    rows = [
        {
            "as_of": 20240102,
            "rank": 1,
            "code": "1111",
            "entryQualified": True,
            "setupType": "watch",
            "displayScore": 0.74,
            "forward_return_5": 0.02,
            "forward_return_20": 0.03,
            "forward_return_60": 0.05,
        },
        {
            "as_of": 20240102,
            "rank": 2,
            "code": "2222",
            "entryQualified": False,
            "setupType": "reject",
            "displayScore": 0.92,
            "forward_return_5": -0.01,
            "forward_return_20": -0.02,
            "forward_return_60": -0.03,
        },
        {
            "as_of": 20240102,
            "rank": 3,
            "code": "3333",
            "entryQualified": True,
            "setupType": "breakout",
            "displayScore": 0.88,
            "forward_return_5": 0.04,
            "forward_return_20": 0.08,
            "forward_return_60": 0.09,
        },
        {
            "as_of": 20240103,
            "rank": 1,
            "code": "4444",
            "entryQualified": True,
            "setupType": "breakout",
            "displayScore": 0.97,
            "forward_return_5": 0.05,
            "forward_return_20": 0.09,
            "forward_return_60": 0.11,
        },
        {
            "as_of": 20240103,
            "rank": 2,
            "code": "5555",
            "entryQualified": True,
            "setupType": "reject",
            "displayScore": 0.81,
            "forward_return_5": -0.02,
            "forward_return_20": -0.03,
            "forward_return_60": -0.04,
        },
    ]
    for rank in range(4, 201):
        rows.append(
            {
                "as_of": 20240102,
                "rank": rank,
                "code": f"9{rank:03d}",
                "entryQualified": False,
                "setupType": "reject",
                "displayScore": 0.10,
                "forward_return_5": -0.02,
                "forward_return_20": -0.03,
                "forward_return_60": -0.04,
            }
        )
        rows.append(
            {
                "as_of": 20240103,
                "rank": rank,
                "code": f"8{rank:03d}",
                "entryQualified": False,
                "setupType": "reject",
                "displayScore": 0.10,
                "forward_return_5": -0.02,
                "forward_return_20": -0.03,
                "forward_return_60": -0.04,
            }
        )
    panel = pd.DataFrame(rows)

    baseline = fusion_backtest._select_for_recipe(  # type: ignore[attr-defined]
        panel,
        bucket_size=10,
        recipe=fusion_backtest.RECIPES[0],
    )
    breakout_gate = fusion_backtest._select_for_recipe(  # type: ignore[attr-defined]
        panel,
        bucket_size=10,
        recipe=fusion_backtest.RECIPES[2],
    )
    weighted = fusion_backtest._select_for_recipe(  # type: ignore[attr-defined]
        panel,
        bucket_size=10,
        recipe=fusion_backtest.RECIPES[5],
    )

    assert baseline["code"].tolist()[:3] == ["1111", "2222", "3333"]
    assert breakout_gate["code"].tolist() == ["3333", "4444"]
    assert weighted["code"].tolist()[0] == "3333"
    assert weighted["code"].tolist()[1] == "1111"
    assert weighted["code"].tolist()[2] == "2222"


def test_run_ranking_state_fusion_backtest_writes_report(monkeypatch, tmp_path: Path) -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240102,
                "rank": 1,
                "code": "1111",
                "entryQualified": True,
                "setupType": "watch",
                "displayScore": 0.74,
                "forward_return_5": 0.02,
                "forward_return_20": 0.03,
                "forward_return_60": 0.05,
            },
            {
                "as_of": 20240102,
                "rank": 2,
                "code": "2222",
                "entryQualified": False,
                "setupType": "reject",
                "displayScore": 0.92,
                "forward_return_5": -0.01,
                "forward_return_20": -0.02,
                "forward_return_60": -0.03,
            },
            {
                "as_of": 20240102,
                "rank": 3,
                "code": "3333",
                "entryQualified": True,
                "setupType": "breakout",
                "displayScore": 0.88,
                "forward_return_5": 0.04,
                "forward_return_20": 0.08,
                "forward_return_60": 0.09,
            },
            {
                "as_of": 20240103,
                "rank": 1,
                "code": "4444",
                "entryQualified": True,
                "setupType": "breakout",
                "displayScore": 0.97,
                "forward_return_5": 0.05,
                "forward_return_20": 0.09,
                "forward_return_60": 0.11,
            },
            {
                "as_of": 20240103,
                "rank": 2,
                "code": "5555",
                "entryQualified": True,
                "setupType": "reject",
                "displayScore": 0.81,
                "forward_return_5": -0.02,
                "forward_return_20": -0.03,
                "forward_return_60": -0.04,
            },
        ]
    )

    def _fake_raw_backtest(*_args, **kwargs):
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
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
        fusion_backtest.quality_backtest,
        "_run_raw_backtest",
        _fake_raw_backtest,
    )

    result = fusion_backtest.run_ranking_state_fusion_backtest(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        output_dir=tmp_path,
        round_trip_cost=0.002,
    )

    payload = result["payload"]
    assert payload["schema_version"] == fusion_backtest.FUSION_SCRIPT_SCHEMA_VERSION
    assert payload["verdict"] in {"usable", "watch", "not_usable_yet"}
    assert (tmp_path / "ranking_state_fusion_backtest.json").exists()
    assert (tmp_path / "ranking_state_fusion_backtest.md").exists()
    assert "best_variant" in payload
    assert payload["variants"]["quality_breakout_gate"]["top10"]["sample_count"] > 0
