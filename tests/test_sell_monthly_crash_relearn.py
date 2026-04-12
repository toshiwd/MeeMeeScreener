from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from app.backend.tools import sell_monthly_crash_relearn as monthly_crash


def _build_signal_frame() -> pd.DataFrame:
    rows = [
        {
            "signal_dt": 20240315,
            "code": "A",
            "side": "sell",
            "setup_type": "breakdown",
            "entry_qualified": True,
            "forward_return_5": -0.02,
            "forward_return_10": -0.05,
            "forward_return_20": -0.15,
            "forward_return_30": -0.16,
            "max_favorable_30": 0.18,
            "max_adverse_30": -0.05,
            "entry_score": 0.82,
            "bar_open": 100.0,
            "bar_high": 101.0,
            "bar_low": 94.0,
            "bar_close": 95.0,
            "close": 95.0,
            "ma7": 98.0,
            "ma20": 100.0,
            "ma60": 103.0,
            "body_ratio": 0.05,
            "upper_wick_ratio": 0.02,
            "lower_wick_ratio": 0.04,
            "candle_triplet_up_prob": 0.21,
            "candle_triplet_down_prob": 0.72,
            "gap_pct": -0.03,
            "close_ret2": -0.02,
            "close_ret3": -0.03,
            "close_ret20": -0.15,
            "close_ret60": -0.18,
            "breakout20_down": 1,
            "rebound60": 0,
            "drawdown60": 0.14,
            "market_ret20": -0.04,
            "breadth_above_ma20": 0.38,
            "breadth_above_ma60": 0.31,
            "sector_ret20": -0.03,
            "rel_sector_ret20": -0.02,
            "regime_id": "risk_off_trend",
        },
        {
            "signal_dt": 20240318,
            "code": "B",
            "side": "sell",
            "setup_type": "pressure",
            "entry_qualified": True,
            "forward_return_5": -0.01,
            "forward_return_10": -0.03,
            "forward_return_20": -0.08,
            "forward_return_30": -0.09,
            "max_favorable_30": 0.11,
            "max_adverse_30": -0.04,
            "entry_score": 0.77,
            "bar_open": 110.0,
            "bar_high": 117.0,
            "bar_low": 109.0,
            "bar_close": 116.0,
            "close": 116.0,
            "ma7": 108.0,
            "ma20": 105.0,
            "ma60": 102.0,
            "body_ratio": 0.05,
            "upper_wick_ratio": 0.02,
            "lower_wick_ratio": 0.03,
            "candle_triplet_up_prob": 0.79,
            "candle_triplet_down_prob": 0.16,
            "gap_pct": 0.04,
            "close_ret2": 0.03,
            "close_ret3": 0.02,
            "close_ret20": -0.08,
            "close_ret60": -0.09,
            "breakout20_down": 0,
            "rebound60": 1,
            "drawdown60": 0.02,
            "market_ret20": 0.02,
            "breadth_above_ma20": 0.56,
            "breadth_above_ma60": 0.52,
            "sector_ret20": 0.01,
            "rel_sector_ret20": 0.00,
            "regime_id": "risk_on_range",
        },
        {
            "signal_dt": 20240320,
            "code": "C",
            "side": "sell",
            "setup_type": "breakdown",
            "entry_qualified": True,
            "forward_return_5": -0.03,
            "forward_return_10": -0.06,
            "forward_return_20": -0.12,
            "forward_return_30": -0.14,
            "max_favorable_30": 0.16,
            "max_adverse_30": -0.06,
            "entry_score": 0.80,
            "bar_open": 90.0,
            "bar_high": 91.0,
            "bar_low": 85.0,
            "bar_close": 86.0,
            "close": 86.0,
            "ma7": 92.0,
            "ma20": 95.0,
            "ma60": 98.0,
            "body_ratio": 0.04,
            "upper_wick_ratio": 0.03,
            "lower_wick_ratio": 0.04,
            "candle_triplet_up_prob": 0.18,
            "candle_triplet_down_prob": 0.77,
            "gap_pct": -0.04,
            "close_ret2": -0.04,
            "close_ret3": -0.05,
            "close_ret20": -0.12,
            "close_ret60": -0.14,
            "breakout20_down": 1,
            "rebound60": 0,
            "drawdown60": 0.09,
            "market_ret20": -0.05,
            "breadth_above_ma20": 0.35,
            "breadth_above_ma60": 0.29,
            "sector_ret20": -0.04,
            "rel_sector_ret20": -0.03,
            "regime_id": "risk_off_trend",
        },
        {
            "signal_dt": 20240322,
            "code": "D",
            "side": "sell",
            "setup_type": "pressure",
            "entry_qualified": True,
            "forward_return_5": 0.00,
            "forward_return_10": 0.01,
            "forward_return_20": 0.03,
            "forward_return_30": 0.02,
            "max_favorable_30": 0.05,
            "max_adverse_30": -0.03,
            "entry_score": 0.66,
            "bar_open": 100.0,
            "bar_high": 101.0,
            "bar_low": 99.0,
            "bar_close": 100.5,
            "close": 100.5,
            "ma7": 100.0,
            "ma20": 100.0,
            "ma60": 100.0,
            "body_ratio": 0.01,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.01,
            "candle_triplet_up_prob": 0.46,
            "candle_triplet_down_prob": 0.35,
            "gap_pct": 0.00,
            "close_ret2": 0.00,
            "close_ret3": 0.00,
            "close_ret20": 0.03,
            "close_ret60": 0.02,
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
            "side": "sell",
            "setup_type": "breakdown",
            "entry_qualified": True,
            "forward_return_5": -0.02,
            "forward_return_10": -0.05,
            "forward_return_20": -0.14,
            "forward_return_30": -0.16,
            "max_favorable_30": 0.16,
            "max_adverse_30": -0.04,
            "entry_score": 0.79,
            "bar_open": 120.0,
            "bar_high": 121.0,
            "bar_low": 109.0,
            "bar_close": 110.0,
            "close": 110.0,
            "ma7": 116.0,
            "ma20": 112.0,
            "ma60": 108.0,
            "body_ratio": 0.08,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.08,
            "candle_triplet_up_prob": 0.15,
            "candle_triplet_down_prob": 0.82,
            "gap_pct": -0.02,
            "close_ret2": -0.03,
            "close_ret3": -0.03,
            "close_ret20": -0.14,
            "close_ret60": -0.16,
            "breakout20_down": 1,
            "rebound60": 0,
            "drawdown60": 0.10,
            "market_ret20": -0.03,
            "breadth_above_ma20": 0.41,
            "breadth_above_ma60": 0.33,
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
                "gap_ma3": -0.01,
                "gap_ma6": -0.02,
                "gap_ma12": -0.03,
                "close_pos_in_range": 0.50,
                "monthly_zone": "sideways",
                "monthly_bullish_context": False,
                "monthly_bearish_context": False,
                "monthly_sideways_context": True,
                "monthly_extension_up": False,
                "monthly_extension_down": False,
            },
            {
                "code": "B",
                "month_last_ymd": 20240229,
                "gap_ma3": 0.10,
                "gap_ma6": 0.12,
                "gap_ma12": 0.08,
                "close_pos_in_range": 0.82,
                "monthly_zone": "bull_extension",
                "monthly_bullish_context": True,
                "monthly_bearish_context": False,
                "monthly_sideways_context": False,
                "monthly_extension_up": True,
                "monthly_extension_down": False,
            },
            {
                "code": "C",
                "month_last_ymd": 20240229,
                "gap_ma3": -0.11,
                "gap_ma6": -0.13,
                "gap_ma12": -0.10,
                "close_pos_in_range": 0.18,
                "monthly_zone": "bear_extension",
                "monthly_bullish_context": False,
                "monthly_bearish_context": True,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": True,
            },
            {
                "code": "D",
                "month_last_ymd": 20240229,
                "gap_ma3": 0.00,
                "gap_ma6": 0.00,
                "gap_ma12": 0.00,
                "close_pos_in_range": 0.50,
                "monthly_zone": "mid",
                "monthly_bullish_context": False,
                "monthly_bearish_context": False,
                "monthly_sideways_context": False,
                "monthly_extension_up": False,
                "monthly_extension_down": False,
            },
            {
                "code": "E",
                "month_last_ymd": 20240229,
                "gap_ma3": 0.09,
                "gap_ma6": 0.11,
                "gap_ma12": 0.07,
                "close_pos_in_range": 0.80,
                "monthly_zone": "bull_extension",
                "monthly_bullish_context": True,
                "monthly_bearish_context": False,
                "monthly_sideways_context": False,
                "monthly_extension_up": True,
                "monthly_extension_down": False,
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
                return _Result(20240322)
            return _Result(1)

    @contextmanager
    def _ctx():
        yield _Conn()

    return _ctx()


def test_sell_monthly_crash_relearn_scores_sideways_breakdown_and_monthly_spike(monkeypatch, tmp_path) -> None:
    signals = _build_signal_frame()
    monthly = _build_monthly_frame()
    daily = pd.DataFrame({"code": ["A"], "date_dt": [pd.Timestamp("2024-02-01")], "o": [100.0], "h": [101.0], "l": [99.0], "c": [100.0], "v": [1000]})
    monkeypatch.setattr(monthly_crash, "get_conn", _fake_get_conn)
    monkeypatch.setattr(monthly_crash.sell_path, "_table_exists", lambda conn, table_name: True)
    monkeypatch.setattr(monthly_crash, "_load_sell_signal_frame", lambda conn, start_ymd, end_ymd: signals)
    monkeypatch.setattr(monthly_crash, "_load_daily_frame", lambda conn, lookback_days: daily)
    monkeypatch.setattr(monthly_crash, "_build_monthly_frame", lambda daily_frame, config: monthly)

    result = monthly_crash.run_sell_monthly_crash_relearn(
        config=monthly_crash.SellMonthlyCrashConfig(
            lookback_days=3650,
            report_dir=tmp_path,
            min_rule_count=1,
        )
    )

    assert result["ok"] is True
    assert int(result["row_count"]) == 5
    assert int(result["monthly_zone"][0]["count"]) >= 1
    combo = {row["label"]: row for row in result["combo_summary"]}
    assert combo["monthly_sideways__breakdown"]["mean20"] > combo["monthly_mid"]["mean20"]
    assert combo["monthly_sideways__breakdown"]["big_drop20_rate"] > 0.0
    assert result["big_drop_rules"][0]["bucket"] == "monthly_sideways"
    assert float(result["recommendation"]["mean20"]) > 0.0
    assert combo["monthly_bull_extension__bearish_spike"]["mean20"] > 0.0
    assert combo["monthly_bull_extension__bearish_spike"]["big_drop20_rate"] >= combo["monthly_mid"]["big_drop20_rate"]

    json_path = tmp_path / "sell_monthly_crash.json"
    md_path = tmp_path / "sell_monthly_crash.md"
    monthly_crash._write_json_report(result, json_path)
    monthly_crash._write_markdown_report(result, md_path)
    assert json_path.exists()
    assert md_path.exists()
    assert "monthly_sideways__breakdown" in md_path.read_text(encoding="utf-8")
