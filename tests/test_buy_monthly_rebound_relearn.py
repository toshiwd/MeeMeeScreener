from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from app.backend.tools import buy_monthly_rebound_relearn as rebound


def _build_signal_frame() -> pd.DataFrame:
    rows = [
        {
            "signal_dt": 20240315,
            "code": "A",
            "side": "buy",
            "setup_type": "rebound",
            "entry_qualified": True,
            "forward_return_5": 0.04,
            "forward_return_10": 0.08,
            "forward_return_20": 0.18,
            "forward_return_30": 0.20,
            "max_favorable_30": 0.24,
            "max_adverse_30": -0.05,
            "entry_score": 0.82,
            "bar_open": 100.0,
            "bar_high": 102.0,
            "bar_low": 94.0,
            "bar_close": 101.5,
            "close": 101.5,
            "ma7": 97.0,
            "ma20": 98.0,
            "ma60": 102.0,
            "body_ratio": 0.08,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.07,
            "candle_triplet_up_prob": 0.78,
            "candle_triplet_down_prob": 0.12,
            "gap_pct": 0.03,
            "close_ret2": 0.05,
            "close_ret3": 0.06,
            "close_ret20": 0.18,
            "close_ret60": 0.20,
            "breakout20_down": 0,
            "rebound60": 1,
            "drawdown60": -0.01,
            "market_ret20": -0.03,
            "breadth_above_ma20": 0.39,
            "breadth_above_ma60": 0.34,
            "sector_ret20": -0.02,
            "rel_sector_ret20": 0.01,
            "regime_id": "capitulation_rebound",
        },
        {
            "signal_dt": 20240318,
            "code": "B",
            "side": "buy",
            "setup_type": "turn",
            "entry_qualified": True,
            "forward_return_5": 0.03,
            "forward_return_10": 0.05,
            "forward_return_20": 0.12,
            "forward_return_30": 0.14,
            "max_favorable_30": 0.18,
            "max_adverse_30": -0.04,
            "entry_score": 0.77,
            "bar_open": 90.0,
            "bar_high": 91.0,
            "bar_low": 84.0,
            "bar_close": 90.5,
            "close": 90.5,
            "ma7": 88.0,
            "ma20": 87.0,
            "ma60": 92.0,
            "body_ratio": 0.06,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.05,
            "candle_triplet_up_prob": 0.72,
            "candle_triplet_down_prob": 0.18,
            "gap_pct": 0.02,
            "close_ret2": 0.03,
            "close_ret3": 0.04,
            "close_ret20": 0.12,
            "close_ret60": 0.14,
            "breakout20_down": 0,
            "rebound60": 1,
            "drawdown60": -0.02,
            "market_ret20": -0.02,
            "breadth_above_ma20": 0.42,
            "breadth_above_ma60": 0.36,
            "sector_ret20": -0.01,
            "rel_sector_ret20": 0.00,
            "regime_id": "capitulation_rebound",
        },
        {
            "signal_dt": 20240320,
            "code": "C",
            "side": "buy",
            "setup_type": "breakout",
            "entry_qualified": True,
            "forward_return_5": 0.02,
            "forward_return_10": 0.04,
            "forward_return_20": 0.10,
            "forward_return_30": 0.11,
            "max_favorable_30": 0.13,
            "max_adverse_30": -0.03,
            "entry_score": 0.80,
            "bar_open": 95.0,
            "bar_high": 96.0,
            "bar_low": 90.0,
            "bar_close": 95.8,
            "close": 95.8,
            "ma7": 93.0,
            "ma20": 92.0,
            "ma60": 93.0,
            "body_ratio": 0.05,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.05,
            "candle_triplet_up_prob": 0.74,
            "candle_triplet_down_prob": 0.14,
            "gap_pct": 0.03,
            "close_ret2": 0.02,
            "close_ret3": 0.03,
            "close_ret20": 0.10,
            "close_ret60": 0.11,
            "breakout20_down": 0,
            "rebound60": 0,
            "drawdown60": -0.01,
            "market_ret20": 0.00,
            "breadth_above_ma20": 0.53,
            "breadth_above_ma60": 0.52,
            "sector_ret20": 0.01,
            "rel_sector_ret20": 0.01,
            "regime_id": "risk_on_range",
        },
        {
            "signal_dt": 20240322,
            "code": "D",
            "side": "buy",
            "setup_type": "turn",
            "entry_qualified": True,
            "forward_return_5": 0.01,
            "forward_return_10": 0.03,
            "forward_return_20": 0.09,
            "forward_return_30": 0.10,
            "max_favorable_30": 0.11,
            "max_adverse_30": -0.02,
            "entry_score": 0.65,
            "bar_open": 100.0,
            "bar_high": 101.0,
            "bar_low": 98.0,
            "bar_close": 100.5,
            "close": 100.5,
            "ma7": 100.0,
            "ma20": 100.0,
            "ma60": 100.0,
            "body_ratio": 0.01,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.01,
            "candle_triplet_up_prob": 0.49,
            "candle_triplet_down_prob": 0.31,
            "gap_pct": 0.0,
            "close_ret2": 0.00,
            "close_ret3": 0.00,
            "close_ret20": 0.09,
            "close_ret60": 0.10,
            "breakout20_down": 0,
            "rebound60": 0,
            "drawdown60": 0.00,
            "market_ret20": 0.00,
            "breadth_above_ma20": 0.50,
            "breadth_above_ma60": 0.50,
            "sector_ret20": 0.00,
            "rel_sector_ret20": 0.00,
            "regime_id": "neutral_range",
        },
        {
            "signal_dt": 20240325,
            "code": "E",
            "side": "buy",
            "setup_type": "rebound",
            "entry_qualified": True,
            "forward_return_5": 0.00,
            "forward_return_10": 0.01,
            "forward_return_20": 0.02,
            "forward_return_30": 0.03,
            "max_favorable_30": 0.05,
            "max_adverse_30": -0.03,
            "entry_score": 0.72,
            "bar_open": 85.0,
            "bar_high": 86.0,
            "bar_low": 81.0,
            "bar_close": 84.0,
            "close": 84.0,
            "ma7": 84.0,
            "ma20": 85.0,
            "ma60": 88.0,
            "body_ratio": 0.01,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.02,
            "candle_triplet_up_prob": 0.41,
            "candle_triplet_down_prob": 0.29,
            "gap_pct": -0.01,
            "close_ret2": 0.00,
            "close_ret3": 0.00,
            "close_ret20": 0.02,
            "close_ret60": 0.03,
            "breakout20_down": 0,
            "rebound60": 0,
            "drawdown60": 0.01,
            "market_ret20": -0.01,
            "breadth_above_ma20": 0.46,
            "breadth_above_ma60": 0.44,
            "sector_ret20": -0.01,
            "rel_sector_ret20": 0.00,
            "regime_id": "risk_off_trend",
        },
        {
            "signal_dt": 20240326,
            "code": "F",
            "side": "buy",
            "setup_type": "turn",
            "entry_qualified": True,
            "forward_return_5": -0.01,
            "forward_return_10": 0.00,
            "forward_return_20": 0.01,
            "forward_return_30": 0.02,
            "max_favorable_30": 0.04,
            "max_adverse_30": -0.06,
            "entry_score": 0.68,
            "bar_open": 88.0,
            "bar_high": 89.0,
            "bar_low": 83.0,
            "bar_close": 84.0,
            "close": 84.0,
            "ma7": 85.0,
            "ma20": 86.0,
            "ma60": 89.0,
            "body_ratio": 0.01,
            "upper_wick_ratio": 0.03,
            "lower_wick_ratio": 0.02,
            "candle_triplet_up_prob": 0.38,
            "candle_triplet_down_prob": 0.35,
            "gap_pct": -0.02,
            "close_ret2": -0.01,
            "close_ret3": 0.00,
            "close_ret20": 0.01,
            "close_ret60": 0.02,
            "breakout20_down": 0,
            "rebound60": 0,
            "drawdown60": 0.02,
            "market_ret20": -0.02,
            "breadth_above_ma20": 0.44,
            "breadth_above_ma60": 0.42,
            "sector_ret20": -0.02,
            "rel_sector_ret20": -0.01,
            "regime_id": "risk_off_trend",
        },
    ]
    return pd.DataFrame(rows)


def _build_monthly_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "A",
                "month_last_ymd": 20240229,
                "gap_ma3": -0.07,
                "gap_ma6": -0.10,
                "gap_ma12": -0.14,
                "close_pos_in_range": 0.38,
                "monthly_zone": "bear_stack",
                "monthly_bullish_context": False,
                "monthly_bearish_context": True,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": True,
            },
            {
                "code": "B",
                "month_last_ymd": 20240229,
                "gap_ma3": -0.05,
                "gap_ma6": -0.08,
                "gap_ma12": -0.11,
                "close_pos_in_range": 0.35,
                "monthly_zone": "bear_extension",
                "monthly_bullish_context": False,
                "monthly_bearish_context": True,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": True,
            },
            {
                "code": "C",
                "month_last_ymd": 20240229,
                "gap_ma3": 0.02,
                "gap_ma6": 0.04,
                "gap_ma12": 0.06,
                "close_pos_in_range": 0.58,
                "monthly_zone": "bull_stack",
                "monthly_bullish_context": True,
                "monthly_bearish_context": False,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": False,
            },
            {
                "code": "D",
                "month_last_ymd": 20240229,
                "gap_ma3": -0.01,
                "gap_ma6": -0.01,
                "gap_ma12": -0.02,
                "close_pos_in_range": 0.50,
                "monthly_zone": "sideways",
                "monthly_bullish_context": False,
                "monthly_bearish_context": False,
                "monthly_sideways_context": True,
                "monthly_extension_up": False,
                "monthly_extension_down": False,
            },
            {
                "code": "E",
                "month_last_ymd": 20240229,
                "gap_ma3": -0.03,
                "gap_ma6": -0.04,
                "gap_ma12": -0.05,
                "close_pos_in_range": 0.43,
                "monthly_zone": "bear_stack",
                "monthly_bullish_context": False,
                "monthly_bearish_context": True,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": False,
            },
            {
                "code": "F",
                "month_last_ymd": 20240229,
                "gap_ma3": -0.06,
                "gap_ma6": -0.09,
                "gap_ma12": -0.12,
                "close_pos_in_range": 0.31,
                "monthly_zone": "bear_extension",
                "monthly_bullish_context": False,
                "monthly_bearish_context": True,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": True,
            },
        ]
    )


def _fake_get_conn():
    class _Result:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return (self._value,)

    class _Conn:
        def execute(self, *args, **kwargs):
            sql = str(args[0]).lower() if args else ""
            if "select max(dt)" in sql:
                return _Result(20240325)
            return _Result(1)

    @contextmanager
    def _ctx():
        yield _Conn()

    return _ctx()


def test_buy_monthly_rebound_relearn_scores_capitulation_and_ma_reclaim(monkeypatch, tmp_path) -> None:
    signals = _build_signal_frame()
    monthly = _build_monthly_frame()
    daily = pd.DataFrame({"code": ["A"], "date_dt": [pd.Timestamp("2024-02-01")], "o": [100.0], "h": [101.0], "l": [99.0], "c": [100.0], "v": [1000]})
    monkeypatch.setattr(rebound, "get_conn", _fake_get_conn)
    monkeypatch.setattr(rebound.sell_path, "_table_exists", lambda conn, table_name: True)
    monkeypatch.setattr(rebound, "_load_buy_signal_frame", lambda conn, start_ymd, end_ymd: signals)
    monkeypatch.setattr(rebound, "_load_daily_frame", lambda conn, lookback_days: daily)
    monkeypatch.setattr(rebound.monthly_crash, "_build_monthly_frame", lambda daily_frame, config: monthly)  # type: ignore[attr-defined]

    result = rebound.run_buy_monthly_rebound_relearn(
        config=rebound.BuyMonthlyReboundConfig(
            lookback_days=3650,
            report_dir=tmp_path,
            min_rule_count=1,
        )
    )

    assert result["ok"] is True
    assert int(result["row_count"]) == 6
    combo = {row["label"]: row for row in result["combo_summary"]}
    bucket = {row["bucket"]: row for row in result["bucket_summary"]}
    assert combo["monthly_bear_stack__reversal_up"]["mean20"] > bucket["monthly_bear_stack"]["mean20"]
    assert combo["monthly_bear_extension__reversal_up"]["mean20"] > bucket["monthly_bear_extension"]["mean20"]
    assert combo["monthly_rebound_context"]["mean20"] > 0.0
    assert result["big_rebound_rules"][0]["bucket"] in {
        "monthly_bear_stack__reversal_up",
        "monthly_bear_extension__reversal_up",
        "monthly_bear_stack__ma_reclaim",
        "monthly_bear_extension__ma_reclaim",
    }
    assert float(result["recommendation"]["mean20"]) > 0.0

    json_path = tmp_path / "buy_monthly_rebound.json"
    md_path = tmp_path / "buy_monthly_rebound.md"
    rebound._write_json_report(result, json_path)
    rebound._write_markdown_report(result, md_path)
    assert json_path.exists()
    assert md_path.exists()
    report_text = md_path.read_text(encoding="utf-8")
    assert "monthly_bear_stack__reversal_up" in report_text
    assert "monthly_bear_extension__reversal_up" in report_text
