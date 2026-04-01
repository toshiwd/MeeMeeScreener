from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

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
                    "entryScore": 0.55,
                    "hybridScore": 0.72,
                    "setupType": "watch",
                },
                {
                    "code": "2222",
                    "entryQualified": False,
                    "entryScore": 0.81,
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
    assert payload["selection_variant"] == "baseline"
    assert (tmp_path / "raw_ranking_backtest.json").exists()
    assert (tmp_path / "raw_ranking_backtest.md").exists()
    assert (tmp_path / "daily_selection_panel.parquet").exists()
    assert payload["cohort_metrics"]["top5"]["sample_count"] > 0

    baseline_panel = pd.read_parquet(tmp_path / "daily_selection_panel.parquet")
    baseline_codes = baseline_panel.loc[baseline_panel["as_of"] == 20240102, "code"].tolist()
    assert baseline_codes == ["1111", "2222"]
    assert "displayScore" in baseline_panel.columns
    assert "displayScoreSource" in baseline_panel.columns

    experiment_payload = ranking_backtest_service.run_raw_ranking_backtest(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        output_dir=tmp_path,
        selection_variant=ranking_backtest_service.TRADEX_EXPERIMENT_SELECTION_VARIANT,
    )
    assert experiment_payload["selection_variant"] == ranking_backtest_service.TRADEX_EXPERIMENT_SELECTION_VARIANT
    assert (tmp_path / "raw_ranking_backtest_tradex_experiment.json").exists()
    assert (tmp_path / "raw_ranking_backtest_tradex_experiment.md").exists()
    assert (tmp_path / "daily_selection_panel_tradex_experiment.parquet").exists()

    experiment_panel = pd.read_parquet(tmp_path / "daily_selection_panel_tradex_experiment.parquet")
    experiment_codes = experiment_panel.loc[experiment_panel["as_of"] == 20240102, "code"].tolist()
    assert experiment_codes == ["2222", "1111"]
    assert set(experiment_panel["displayScoreSource"].tolist()) <= {"ranking_entry", "ranking_hybrid", "none"}


def test_run_ranking_backtest_writes_summary(monkeypatch, tmp_path: Path) -> None:
    def _make_raw_payload(*, selection_variant: str, top10_mean: float, top10_lift: float, entry_mean: float, entry_lift: float) -> dict:
        return {
            "selection_variant": selection_variant,
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
                    "mean_forward_return_5": 0.01,
                    "mean_forward_return_20": top10_mean,
                    "mean_forward_return_60": 0.07,
                    "hit_rate_20": 0.6,
                    "lift_vs_all_ranked": top10_lift,
                    "lift_vs_bottom_bucket": 0.03,
                    "daily_overlap_rate": 0.4,
                    "daily_turnover_rate": 0.6,
                },
                "top10_entryQualified": {
                    "mean_forward_return_5": 0.02,
                    "mean_forward_return_20": entry_mean,
                    "mean_forward_return_60": 0.08,
                    "hit_rate_20": 0.7,
                    "lift_vs_all_ranked": entry_lift,
                    "lift_vs_bottom_bucket": 0.04,
                    "daily_overlap_rate": 0.5,
                    "daily_turnover_rate": 0.5,
                },
            },
        }

    monkeypatch.setattr(
        ranking_backtest_service,
        "run_raw_ranking_backtest",
        lambda **kwargs: _make_raw_payload(
            selection_variant=str(kwargs.get("selection_variant") or "baseline"),
            top10_mean=0.03 if str(kwargs.get("selection_variant") or "baseline") == "baseline" else 0.04,
            top10_lift=0.01 if str(kwargs.get("selection_variant") or "baseline") == "baseline" else 0.015,
            entry_mean=0.04 if str(kwargs.get("selection_variant") or "baseline") == "baseline" else 0.05,
            entry_lift=0.02 if str(kwargs.get("selection_variant") or "baseline") == "baseline" else 0.025,
        ),
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
    assert result["summary"]["tradex_experiment"]["top10_entryQualified"]["mean_forward_return_20"] == 0.05
    assert result["summary"]["comparison"]["selection_variant_baseline"] == "baseline"
    assert result["summary"]["comparison"]["selection_variant_experiment"] == ranking_backtest_service.TRADEX_EXPERIMENT_SELECTION_VARIANT
    assert result["summary"]["comparison"]["top10_entryQualified"]["mean_forward_return_20"]["delta"] == pytest.approx(0.01)
    assert result["summary"]["comparison"]["threshold_check"]["mean_forward_return_20_non_decreasing"] is True
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
