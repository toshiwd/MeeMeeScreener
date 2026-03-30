from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from app.backend.services.analysis import ranking_backtest_service


def test_compute_forward_returns_uses_next_open_and_horizon_close() -> None:
    lookup = {
        "1111": {
            "dates": [20240101, 20240102, 20240103, 20240104, 20240105, 20240108],
            "opens": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            "closes": [10.5, 11.5, 12.5, 13.5, 14.5, 15.5],
        }
    }

    returns = ranking_backtest_service._compute_forward_returns(  # type: ignore[attr-defined]
        price_lookup=lookup,
        code="1111",
        as_of_ymd=20240101,
        horizons=(5, 20, 60),
    )

    assert returns[5] == 15.5 / 11.0 - 1.0
    assert returns[20] is None
    assert returns[60] is None


def test_bucket_panel_entryqualified_preserves_original_rank_order() -> None:
    panel = pd.DataFrame(
        [
            {"as_of": 20240105, "rank": 1, "code": "A", "entryQualified": False},
            {"as_of": 20240105, "rank": 2, "code": "B", "entryQualified": True},
            {"as_of": 20240105, "rank": 3, "code": "C", "entryQualified": False},
            {"as_of": 20240105, "rank": 4, "code": "D", "entryQualified": True},
        ]
    )

    bucket = ranking_backtest_service._bucket_panel(  # type: ignore[attr-defined]
        panel,
        bucket_size=5,
        entry_only=True,
        bottom=False,
    )

    assert bucket["code"].tolist() == ["B", "D"]
    assert bucket["rank"].tolist() == [2, 4]


def test_run_raw_ranking_backtest_writes_artifacts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        ranking_backtest_service,
        "_list_trade_dates",
        lambda **_kwargs: [20240102, 20240103],
    )

    def _fake_get_rankings_asof(*_args, **kwargs):
        as_of = int(kwargs["as_of"])
        return {
            "pred_dt": as_of,
            "items": [
                {
                    "code": "1111",
                    "entryQualified": True,
                    "entryScore": 0.81,
                    "hybridScore": 0.72,
                    "setupType": "watch",
                },
                {
                    "code": "2222",
                    "entryQualified": False,
                    "entryScore": 0.55,
                    "hybridScore": 0.54,
                    "setupType": "watch",
                },
            ],
        }

    monkeypatch.setattr(ranking_backtest_service.rankings_cache, "get_rankings_asof", _fake_get_rankings_asof)
    monkeypatch.setattr(
        ranking_backtest_service,
        "_load_price_frame",
        lambda **_kwargs: pd.DataFrame(
            [
                {"code": "1111", "ymd": 20240103, "o": 100.0, "c": 101.0},
                {"code": "1111", "ymd": 20240104, "o": 102.0, "c": 103.0},
                {"code": "1111", "ymd": 20240105, "o": 104.0, "c": 105.0},
                {"code": "1111", "ymd": 20240108, "o": 106.0, "c": 107.0},
                {"code": "1111", "ymd": 20240109, "o": 108.0, "c": 109.0},
                {"code": "1111", "ymd": 20240110, "o": 110.0, "c": 111.0},
                {"code": "2222", "ymd": 20240103, "o": 50.0, "c": 49.0},
                {"code": "2222", "ymd": 20240104, "o": 49.0, "c": 48.0},
                {"code": "2222", "ymd": 20240105, "o": 48.0, "c": 47.0},
                {"code": "2222", "ymd": 20240108, "o": 47.0, "c": 46.0},
                {"code": "2222", "ymd": 20240109, "o": 46.0, "c": 45.0},
                {"code": "2222", "ymd": 20240110, "o": 45.0, "c": 44.0},
            ]
        ),
    )

    payload = ranking_backtest_service.run_raw_ranking_backtest(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        output_dir=tmp_path,
    )

    assert payload["schema_version"] == ranking_backtest_service.RAW_RANKING_BACKTEST_SCHEMA_VERSION
    assert (tmp_path / "raw_ranking_backtest.json").exists()
    assert (tmp_path / "raw_ranking_backtest.md").exists()
    assert (tmp_path / "daily_selection_panel.parquet").exists()
    assert payload["cohort_metrics"]["top5"]["sample_count"] > 0


def test_run_ranking_backtest_writes_summary(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        ranking_backtest_service,
        "run_raw_ranking_backtest",
        lambda **_kwargs: {
            "ranking_contract": {
                "tf": "D",
                "which": "latest",
                "direction": "up",
                "mode": "hybrid",
                "risk_mode": "balanced",
                "limit": 200,
            },
            "cohort_metrics": {
                "top10": {
                    "mean_forward_return_20": 0.03,
                    "lift_vs_all_ranked": 0.01,
                },
                "top10_entryQualified": {
                    "mean_forward_return_20": 0.04,
                    "lift_vs_all_ranked": 0.02,
                },
            },
        },
    )
    monkeypatch.setattr(
        ranking_backtest_service,
        "run_toredex_policy_backtest",
        lambda **_kwargs: {
            "risk_gate": {"pass": True},
            "final_metrics": {"equity": 11000000},
            "performance_breakdown": {"total_return_pct": 10.0},
            "rollup": {"worst_month_pct": -3.0, "max_turnover_pct_per_month": 120.0},
        },
    )

    result = ranking_backtest_service.run_ranking_backtest(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
        output_dir=tmp_path,
    )

    assert result["summary"]["disposition"] == "usable"
    assert (tmp_path / "ranking_backtest_summary.json").exists()
    assert (tmp_path / "ranking_backtest_summary.md").exists()


def test_extract_toredex_total_return_pct_reads_nested_net_return() -> None:
    payload = {
        "performance_breakdown": {
            "net": {
                "cum_return_pct": -12.34,
            }
        },
        "final_metrics": {
            "cum_return_pct": -99.0,
        },
    }

    actual = ranking_backtest_service._extract_toredex_total_return_pct(payload)  # type: ignore[attr-defined]

    assert actual == -12.34
