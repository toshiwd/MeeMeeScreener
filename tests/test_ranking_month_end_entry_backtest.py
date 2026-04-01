from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts import ranking_month_end_entry_backtest as month_end_backtest


def test_month_end_dates_from_trade_dates_keeps_last_trade_of_each_month() -> None:
    trade_dates = [20240102, 20240103, 20240131, 20240201, 20240215, 20240229, 20240301]

    assert month_end_backtest._month_end_dates_from_trade_dates(trade_dates) == [20240131, 20240229, 20240301]


def test_select_variant_filters_watch_reject_and_requires_state() -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240229,
                "rank": 1,
                "code": "1001",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
                "monthlyBoxMonths": 6,
                "monthlyBoxPos": 0.82,
                "monthlyBoxWild": False,
                "boxBottomAligned": True,
                "reclaim60": 0.9,
                "v60Core": 0.8,
                "v60Strong": 0.7,
                "candleBodyRatio": 0.6,
                "candleUpperWickRatio": 0.15,
                "candleLowerWickRatio": 0.3,
                "candleTripletUp": 0.7,
                "entryScore": 0.9,
                "hybridScore": 0.85,
                "displayScore": 0.8,
            },
            {
                "as_of": 20240229,
                "rank": 2,
                "code": "1002",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "setupType": "watch",
                "monthlyBoxState": "box_upper",
                "monthlyBoxMonths": 6,
                "monthlyBoxPos": 0.78,
                "monthlyBoxWild": False,
                "boxBottomAligned": True,
                "reclaim60": 0.9,
                "v60Core": 0.8,
                "v60Strong": 0.7,
                "candleBodyRatio": 0.6,
                "candleUpperWickRatio": 0.15,
                "candleLowerWickRatio": 0.3,
                "candleTripletUp": 0.7,
                "entryScore": 0.8,
                "hybridScore": 0.75,
                "displayScore": 0.7,
            },
            {
                "as_of": 20240229,
                "rank": 3,
                "code": "1003",
                "entryQualified": True,
                "entryQualifiedByFallback": True,
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
                "monthlyBoxMonths": 6,
                "monthlyBoxPos": 0.76,
                "monthlyBoxWild": False,
                "boxBottomAligned": True,
                "reclaim60": 0.9,
                "v60Core": 0.8,
                "v60Strong": 0.7,
                "candleBodyRatio": 0.6,
                "candleUpperWickRatio": 0.15,
                "candleLowerWickRatio": 0.3,
                "candleTripletUp": 0.7,
                "entryScore": 0.7,
                "hybridScore": 0.65,
                "displayScore": 0.6,
            },
            {
                "as_of": 20240229,
                "rank": 4,
                "code": "1004",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "setupType": "breakout",
                "monthlyBoxState": "no_box",
                "monthlyBoxMonths": 2,
                "monthlyBoxPos": 0.95,
                "monthlyBoxWild": True,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.4,
                "candleUpperWickRatio": 0.3,
                "candleLowerWickRatio": 0.1,
                "candleTripletUp": 0.4,
                "entryScore": 0.95,
                "hybridScore": 0.9,
                "displayScore": 0.88,
            },
        ]
    )

    baseline = month_end_backtest._select_variant(panel, bucket_size=10, variant="baseline", direction="up")  # type: ignore[attr-defined]
    strict_buy = month_end_backtest._select_variant(panel, bucket_size=10, variant="strict_buy", direction="up")  # type: ignore[attr-defined]
    strict_buy_state = month_end_backtest._select_variant(panel, bucket_size=10, variant="strict_buy_state", direction="up")  # type: ignore[attr-defined]

    assert baseline["code"].tolist() == ["1001", "1002", "1003", "1004"]
    assert strict_buy["code"].tolist() == ["1001", "1004"]
    assert strict_buy_state["code"].tolist() == ["1001"]


def test_run_ranking_month_end_entry_backtest_writes_reports(monkeypatch, tmp_path: Path) -> None:
    month_end_dates = [20240131, 20240229]
    observed_modes: list[str] = []
    panel_map = {
        20240131: [
            {
                "code": "1001",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "entryQualifiedFallbackStage": None,
                "entryScore": 0.91,
                "hybridScore": 0.88,
                "displayScore": 0.87,
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
                "monthlyBoxMonths": 6,
                "monthlyBoxPos": 0.82,
                "monthlyBoxWild": False,
                "boxBottomAligned": True,
                "reclaim60": 0.8,
                "v60Core": 0.7,
                "v60Strong": 0.6,
                "candleBodyRatio": 0.6,
                "candleUpperWickRatio": 0.15,
                "candleLowerWickRatio": 0.3,
                "candleTripletUp": 0.72,
                "bullMarubozu": True,
                "bearMarubozu": False,
                "threeWhiteSoldiers": False,
                "threeBlackCrows": False,
                "morningStar": False,
                "bullEngulfing": True,
                "shootingStarLike": False,
                "marketRegime": "risk_on",
            },
            {
                "code": "1002",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "entryQualifiedFallbackStage": None,
                "entryScore": 0.75,
                "hybridScore": 0.7,
                "displayScore": 0.69,
                "setupType": "watch",
                "monthlyBoxState": "no_box",
                "monthlyBoxMonths": 2,
                "monthlyBoxPos": 0.95,
                "monthlyBoxWild": True,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.8,
                "candleUpperWickRatio": 0.1,
                "candleLowerWickRatio": 0.05,
                "candleTripletUp": 0.3,
                "bullMarubozu": False,
                "bearMarubozu": False,
                "threeWhiteSoldiers": False,
                "threeBlackCrows": False,
                "morningStar": False,
                "bullEngulfing": False,
                "shootingStarLike": False,
                "marketRegime": "risk_on",
            },
            {
                "code": "1003",
                "entryQualified": False,
                "entryQualifiedByFallback": False,
                "entryQualifiedFallbackStage": None,
                "entryScore": 0.6,
                "hybridScore": 0.55,
                "displayScore": 0.5,
                "setupType": "reject",
                "monthlyBoxState": "no_box",
                "monthlyBoxMonths": 1,
                "monthlyBoxPos": 0.98,
                "monthlyBoxWild": True,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.85,
                "candleUpperWickRatio": 0.05,
                "candleLowerWickRatio": 0.03,
                "candleTripletUp": 0.2,
                "bullMarubozu": False,
                "bearMarubozu": False,
                "threeWhiteSoldiers": False,
                "threeBlackCrows": False,
                "morningStar": False,
                "bullEngulfing": False,
                "shootingStarLike": False,
                "marketRegime": "risk_on",
            },
        ],
        20240229: [
            {
                "code": "2001",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "entryQualifiedFallbackStage": None,
                "entryScore": 0.93,
                "hybridScore": 0.9,
                "displayScore": 0.89,
                "setupType": "breakout20",
                "monthlyBoxState": "breakout_up",
                "monthlyBoxMonths": 8,
                "monthlyBoxPos": 0.9,
                "monthlyBoxWild": False,
                "boxBottomAligned": True,
                "reclaim60": 0.9,
                "v60Core": 0.8,
                "v60Strong": 0.7,
                "candleBodyRatio": 0.58,
                "candleUpperWickRatio": 0.12,
                "candleLowerWickRatio": 0.22,
                "candleTripletUp": 0.74,
                "bullMarubozu": True,
                "bearMarubozu": False,
                "threeWhiteSoldiers": True,
                "threeBlackCrows": False,
                "morningStar": False,
                "bullEngulfing": True,
                "shootingStarLike": False,
                "marketRegime": "risk_on",
            },
            {
                "code": "2002",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "entryQualifiedFallbackStage": None,
                "entryScore": 0.72,
                "hybridScore": 0.68,
                "displayScore": 0.66,
                "setupType": "watch",
                "monthlyBoxState": "no_box",
                "monthlyBoxMonths": 2,
                "monthlyBoxPos": 0.96,
                "monthlyBoxWild": True,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.75,
                "candleUpperWickRatio": 0.1,
                "candleLowerWickRatio": 0.08,
                "candleTripletUp": 0.28,
                "bullMarubozu": False,
                "bearMarubozu": False,
                "threeWhiteSoldiers": False,
                "threeBlackCrows": False,
                "morningStar": False,
                "bullEngulfing": False,
                "shootingStarLike": False,
                "marketRegime": "risk_on",
            },
        ],
    }
    price_lookup = {
        "1001": {
            "dates": [20240201, 20240202, 20240205, 20240206, 20240207, 20240208, 20240209, 20240213, 20240214, 20240215, 20240216, 20240219, 20240220, 20240221, 20240222, 20240226, 20240227, 20240228, 20240229, 20240301, 20240304, 20240305, 20240306, 20240307, 20240308, 20240311, 20240312, 20240313, 20240314, 20240315],
            "opens": [100 + i for i in range(30)],
            "closes": [100.5 + i * 1.5 for i in range(30)],
        },
        "1002": {
            "dates": [20240201, 20240202, 20240205, 20240206, 20240207, 20240208, 20240209, 20240213, 20240214, 20240215, 20240216, 20240219, 20240220, 20240221, 20240222, 20240226, 20240227, 20240228, 20240229, 20240301, 20240304, 20240305, 20240306, 20240307, 20240308, 20240311, 20240312, 20240313, 20240314, 20240315],
            "opens": [200 + i for i in range(30)],
            "closes": [200.2 + i * 0.2 for i in range(30)],
        },
        "2001": {
            "dates": [20240301, 20240304, 20240305, 20240306, 20240307, 20240308, 20240311, 20240312, 20240313, 20240314, 20240315, 20240318, 20240319, 20240321, 20240322, 20240325, 20240326, 20240327, 20240328, 20240329, 20240401, 20240402, 20240403, 20240404, 20240405, 20240408, 20240409, 20240410, 20240411, 20240412],
            "opens": [300 + i for i in range(30)],
            "closes": [300.5 + i * 1.2 for i in range(30)],
        },
        "2002": {
            "dates": [20240301, 20240304, 20240305, 20240306, 20240307, 20240308, 20240311, 20240312, 20240313, 20240314, 20240315, 20240318, 20240319, 20240321, 20240322, 20240325, 20240326, 20240327, 20240328, 20240329, 20240401, 20240402, 20240403, 20240404, 20240405, 20240408, 20240409, 20240410, 20240411, 20240412],
            "opens": [400 + i for i in range(30)],
            "closes": [400.2 + i * 0.1 for i in range(30)],
        },
    }

    def _fake_get_rankings_asof(tf, which, direction, limit, *, as_of, mode, risk_mode):  # noqa: ANN001
        observed_modes.append(f"{direction}:{mode}")
        rows = panel_map.get(int(as_of), [])
        return {"items": [dict(row, predDt=int(as_of)) for row in rows[: int(limit)]]}

    def _fake_load_price_frame(*, codes, start_date, end_date):  # noqa: ANN001
        return pd.DataFrame({"code": list(codes), "start": [start_date] * len(list(codes)), "end": [end_date] * len(list(codes))})

    monkeypatch.setattr(month_end_backtest, "_resolve_db_path", lambda _value: tmp_path / "stocks.duckdb")  # type: ignore[attr-defined]
    monkeypatch.setattr(month_end_backtest, "_month_window", lambda *_args, **_kwargs: month_end_dates)  # type: ignore[attr-defined]
    monkeypatch.setattr(month_end_backtest, "_latest_trade_date", lambda *_args, **_kwargs: month_end_dates[-1])  # type: ignore[attr-defined]
    monkeypatch.setattr(month_end_backtest.rankings_cache, "get_rankings_asof", _fake_get_rankings_asof)
    monkeypatch.setattr(month_end_backtest.ranking_backtest_service, "_load_price_frame", _fake_load_price_frame)  # type: ignore[attr-defined]
    monkeypatch.setattr(month_end_backtest.ranking_backtest_service, "_price_lookup_from_frame", lambda _frame: price_lookup)  # type: ignore[attr-defined]

    result = month_end_backtest.run_ranking_month_end_entry_backtest(
        months=2,
        selection_limit=2,
        output_dir=tmp_path,
        focus_ymd=20240229,
        direction="up",
    )

    payload = result["payload"]
    assert payload["schema_version"] == month_end_backtest.ENTRY_BACKTEST_SCHEMA_VERSION
    assert payload["period"]["rank_mode"] == "trade"
    assert payload["period"]["direction"] == "up"
    assert payload["verdict"] in {"usable", "watch", "not_usable_yet"}
    assert payload["variants"]["baseline"]["top10"]["sample_count"] > 0
    assert payload["variants"]["strict_buy"]["top10"]["watch_count"] == 0
    assert payload["variants"]["strict_buy"]["top10"]["reject_count"] == 0
    assert payload["variants"]["strict_buy_state"]["top10"]["watch_count"] == 0
    assert payload["variants"]["strict_buy_state"]["top10"]["reject_count"] == 0
    assert observed_modes and set(observed_modes) == {"up:trade"}
    assert (tmp_path / "ranking_month_end_entry_backtest.json").exists()
    assert (tmp_path / "ranking_month_end_entry_backtest.md").exists()
    assert "月末 仕込み専用選定 バックテスト" in (tmp_path / "ranking_month_end_entry_backtest.md").read_text(encoding="utf-8")


def test_select_variant_filters_watch_reject_and_requires_state_for_down() -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240229,
                "rank": 1,
                "code": "9001",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "setupType": "breakdown",
                "monthlyBoxState": "box_upper",
                "monthlyBoxMonths": 7,
                "monthlyBoxPos": 0.84,
                "monthlyBoxWild": False,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.62,
                "candleUpperWickRatio": 0.31,
                "candleLowerWickRatio": 0.11,
                "candleTripletDown": 0.71,
                "bearMarubozu": True,
                "threeBlackCrows": False,
                "shootingStarLike": False,
                "entryScore": 0.9,
                "hybridScore": 0.85,
                "displayScore": 0.8,
            },
            {
                "as_of": 20240229,
                "rank": 2,
                "code": "9002",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "setupType": "watch",
                "monthlyBoxState": "box_upper",
                "monthlyBoxMonths": 7,
                "monthlyBoxPos": 0.84,
                "monthlyBoxWild": False,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.62,
                "candleUpperWickRatio": 0.31,
                "candleLowerWickRatio": 0.11,
                "candleTripletDown": 0.71,
                "bearMarubozu": True,
                "threeBlackCrows": False,
                "shootingStarLike": False,
                "entryScore": 0.85,
                "hybridScore": 0.8,
                "displayScore": 0.75,
            },
            {
                "as_of": 20240229,
                "rank": 3,
                "code": "9003",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "setupType": "breakout",
                "monthlyBoxState": "no_box",
                "monthlyBoxMonths": 2,
                "monthlyBoxPos": 0.95,
                "monthlyBoxWild": True,
                "boxBottomAligned": False,
                "reclaim60": 0.0,
                "v60Core": 0.0,
                "v60Strong": 0.0,
                "candleBodyRatio": 0.35,
                "candleUpperWickRatio": 0.2,
                "candleLowerWickRatio": 0.15,
                "candleTripletDown": 0.25,
                "bearMarubozu": False,
                "threeBlackCrows": False,
                "shootingStarLike": False,
                "entryScore": 0.75,
                "hybridScore": 0.7,
                "displayScore": 0.68,
            },
        ]
    )

    baseline = month_end_backtest._select_variant(panel, bucket_size=10, variant="baseline", direction="down")  # type: ignore[attr-defined]
    strict_sell = month_end_backtest._select_variant(panel, bucket_size=10, variant="strict_sell", direction="down")  # type: ignore[attr-defined]
    strict_sell_state = month_end_backtest._select_variant(panel, bucket_size=10, variant="strict_sell_state", direction="down")  # type: ignore[attr-defined]

    assert baseline["code"].tolist() == ["9001", "9002", "9003"]
    assert strict_sell["code"].tolist() == ["9001", "9003"]
    assert strict_sell_state["code"].tolist() == ["9001"]

