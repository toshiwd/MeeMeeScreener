from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts import ranking_entry_quality_backtest as quality_backtest


def test_select_daily_bucket_filters_entry_qualified_and_breakout() -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240102,
                "rank": 1,
                "code": "1111",
                "entryQualified": True,
                "setupType": "breakout",
                "displayScore": 0.91,
                "forward_return_5": 0.05,
                "forward_return_20": 0.10,
                "forward_return_60": 0.20,
            },
            {
                "as_of": 20240102,
                "rank": 2,
                "code": "2222",
                "entryQualified": True,
                "setupType": "watch",
                "displayScore": 0.84,
                "forward_return_5": 0.01,
                "forward_return_20": 0.02,
                "forward_return_60": 0.03,
            },
            {
                "as_of": 20240102,
                "rank": 3,
                "code": "3333",
                "entryQualified": False,
                "setupType": "watch",
                "displayScore": 0.72,
                "forward_return_5": -0.01,
                "forward_return_20": -0.02,
                "forward_return_60": -0.03,
            },
            {
                "as_of": 20240103,
                "rank": 1,
                "code": "4444",
                "entryQualified": True,
                "setupType": "breakout",
                "displayScore": 0.96,
                "forward_return_5": 0.02,
                "forward_return_20": 0.03,
                "forward_return_60": 0.04,
            },
            {
                "as_of": 20240103,
                "rank": 2,
                "code": "5555",
                "entryQualified": True,
                "setupType": "watch",
                "displayScore": 0.88,
                "forward_return_5": 0.01,
                "forward_return_20": 0.01,
                "forward_return_60": 0.01,
            },
        ]
    )

    entry_only = quality_backtest._select_daily_bucket(  # type: ignore[attr-defined]
        panel,
        bucket_size=10,
        variant="entryQualified",
        round_trip_cost=0.002,
    )
    breakout_only = quality_backtest._select_daily_bucket(  # type: ignore[attr-defined]
        panel,
        bucket_size=10,
        variant="entryQualifiedBreakout",
        round_trip_cost=0.002,
    )
    top_score = quality_backtest._select_daily_bucket(  # type: ignore[attr-defined]
        panel,
        bucket_size=10,
        variant="entryQualifiedTopQuartileScore",
        round_trip_cost=0.002,
    )

    assert entry_only["code"].tolist() == ["1111", "2222", "4444", "5555"]
    assert breakout_only["code"].tolist() == ["1111", "4444"]
    assert top_score["code"].tolist() == ["1111", "4444"]
    assert "forward_return_20_net" in top_score.columns


def test_run_ranking_entry_quality_backtest_writes_report(monkeypatch, tmp_path: Path) -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240102,
                "rank": 1,
                "code": "1111",
                "entryQualified": True,
                "setupType": "breakout",
                "displayScore": 0.91,
                "forward_return_5": 0.05,
                "forward_return_20": 0.10,
                "forward_return_60": 0.15,
            },
            {
                "as_of": 20240102,
                "rank": 2,
                "code": "2222",
                "entryQualified": True,
                "setupType": "watch",
                "displayScore": 0.86,
                "forward_return_5": 0.01,
                "forward_return_20": 0.03,
                "forward_return_60": 0.04,
            },
            {
                "as_of": 20240103,
                "rank": 1,
                "code": "3333",
                "entryQualified": True,
                "setupType": "breakout",
                "displayScore": 0.95,
                "forward_return_5": 0.03,
                "forward_return_20": 0.08,
                "forward_return_60": 0.12,
            },
            {
                "as_of": 20240103,
                "rank": 2,
                "code": "4444",
                "entryQualified": False,
                "setupType": "watch",
                "displayScore": 0.72,
                "forward_return_5": -0.01,
                "forward_return_20": -0.02,
                "forward_return_60": -0.03,
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
            "cohort_metrics": {
                "top10": {"mean_forward_return_20": 0.04},
                "top10_entryQualified": {"mean_forward_return_20": 0.05},
            },
            "coverage_metrics": {},
        }

    monkeypatch.setattr(
        quality_backtest.ranking_backtest_service,
        "run_raw_ranking_backtest",
        _fake_raw_backtest,
    )

    result = quality_backtest.run_ranking_entry_quality_backtest(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        output_dir=tmp_path,
        round_trip_cost=0.002,
    )

    payload = result["payload"]
    assert payload["schema_version"] == quality_backtest.QUALITY_SCRIPT_SCHEMA_VERSION
    assert payload["verdict"] in {"usable", "watch"}
    assert (tmp_path / "ranking_entry_quality_backtest.json").exists()
    assert (tmp_path / "ranking_entry_quality_backtest.md").exists()
    assert payload["variants"]["entryQualified"]["top10"]["return_20_net"]["mean"] is not None
    assert payload["comparison"]["entryQualified_topQuartileScore_top10_mean20_net"] is not None
