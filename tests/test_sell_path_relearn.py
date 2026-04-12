from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from app.backend.tools import sell_path_relearn as sell_paths


def _sell_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "signal_dt": 20240110,
                "code": "S1",
                "side": "sell",
                "setup_type": "breakdown",
                "entry_qualified": True,
                "forward_return_5": -0.03,
                "forward_return_10": -0.08,
                "forward_return_20": -0.12,
                "forward_return_30": -0.15,
                "max_favorable_30": 0.16,
                "max_adverse_30": -0.05,
                "entry_score": 0.84,
                "close": 98.0,
                "ma7": 100.0,
                "ma20": 101.0,
                "ma60": 102.0,
                "body_ratio": 0.05,
                "upper_wick_ratio": 0.03,
                "lower_wick_ratio": 0.07,
                "candle_triplet_up_prob": 0.18,
                "candle_triplet_down_prob": 0.76,
                "gap_pct": -0.04,
                "close_ret5": -0.03,
                "close_ret10": -0.08,
                "close_ret20": -0.12,
                "breakout20_down": 1,
                "rebound60": 0,
                "drawdown60": 0.12,
                "market_ret20": -0.04,
                "breadth_above_ma20": 0.39,
                "breadth_above_ma60": 0.31,
                "sector_ret20": -0.03,
                "rel_sector_ret20": -0.01,
                "regime_id": "risk_off_trend",
            },
            {
                "signal_dt": 20240111,
                "code": "S2",
                "side": "sell",
                "setup_type": "pressure",
                "entry_qualified": True,
                "forward_return_5": 0.03,
                "forward_return_10": 0.01,
                "forward_return_20": -0.08,
                "forward_return_30": -0.09,
                "max_favorable_30": 0.14,
                "max_adverse_30": -0.06,
                "entry_score": 0.71,
                "close": 103.0,
                "ma7": 101.0,
                "ma20": 100.0,
                "ma60": 99.0,
                "body_ratio": 0.06,
                "upper_wick_ratio": 0.04,
                "lower_wick_ratio": 0.05,
                "candle_triplet_up_prob": 0.81,
                "candle_triplet_down_prob": 0.16,
                "gap_pct": 0.02,
                "close_ret5": 0.03,
                "close_ret10": 0.01,
                "close_ret20": -0.08,
                "breakout20_down": 0,
                "rebound60": 1,
                "drawdown60": 0.02,
                "market_ret20": -0.02,
                "breadth_above_ma20": 0.41,
                "breadth_above_ma60": 0.35,
                "sector_ret20": -0.01,
                "rel_sector_ret20": -0.02,
                "regime_id": "risk_off_trend",
            },
            {
                "signal_dt": 20240112,
                "code": "S3",
                "side": "sell",
                "setup_type": "pressure",
                "entry_qualified": True,
                "forward_return_5": 0.05,
                "forward_return_10": 0.09,
                "forward_return_20": -0.04,
                "forward_return_30": -0.02,
                "max_favorable_30": 0.07,
                "max_adverse_30": -0.08,
                "entry_score": 0.68,
                "close": 104.0,
                "ma7": 101.5,
                "ma20": 100.5,
                "ma60": 99.5,
                "body_ratio": 0.04,
                "upper_wick_ratio": 0.05,
                "lower_wick_ratio": 0.04,
                "candle_triplet_up_prob": 0.82,
                "candle_triplet_down_prob": 0.14,
                "gap_pct": 0.03,
                "close_ret5": 0.05,
                "close_ret10": 0.09,
                "close_ret20": -0.04,
                "breakout20_down": 0,
                "rebound60": 1,
                "drawdown60": 0.03,
                "market_ret20": 0.01,
                "breadth_above_ma20": 0.52,
                "breadth_above_ma60": 0.48,
                "sector_ret20": 0.02,
                "rel_sector_ret20": 0.00,
                "regime_id": "neutral_range",
            },
            {
                "signal_dt": 20240113,
                "code": "S4",
                "side": "sell",
                "setup_type": "breakdown",
                "entry_qualified": True,
                "forward_return_5": 0.04,
                "forward_return_10": 0.07,
                "forward_return_20": 0.10,
                "forward_return_30": 0.12,
                "max_favorable_30": -0.03,
                "max_adverse_30": 0.11,
                "entry_score": 0.77,
                "close": 105.0,
                "ma7": 103.0,
                "ma20": 101.0,
                "ma60": 100.0,
                "body_ratio": 0.05,
                "upper_wick_ratio": 0.04,
                "lower_wick_ratio": 0.06,
                "candle_triplet_up_prob": 0.79,
                "candle_triplet_down_prob": 0.18,
                "gap_pct": 0.04,
                "close_ret5": 0.04,
                "close_ret10": 0.07,
                "close_ret20": 0.10,
                "breakout20_down": 0,
                "rebound60": 1,
                "drawdown60": -0.02,
                "market_ret20": 0.02,
                "breadth_above_ma20": 0.57,
                "breadth_above_ma60": 0.53,
                "sector_ret20": 0.03,
                "rel_sector_ret20": 0.01,
                "regime_id": "risk_on_trend",
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
                return _Result(20240113)
            return _Result(1)

    @contextmanager
    def _ctx():
        yield _Conn()

    return _ctx()


def test_sell_path_relearn_classifies_bullish_candle_contrarian(monkeypatch, tmp_path) -> None:
    frame = _sell_frame()
    monkeypatch.setattr(sell_paths, "get_conn", _fake_get_conn)
    monkeypatch.setattr(sell_paths, "_table_exists", lambda conn, table_name: True)
    monkeypatch.setattr(sell_paths, "_load_sell_signal_frame", lambda conn, start_ymd, end_ymd: frame)

    result = sell_paths.run_sell_path_relearn(
        config=sell_paths.SellPathConfig(
            lookback_days=3650,
            report_dir=tmp_path,
            bullish_candle_prob_min=0.55,
            bearish_candle_prob_min=0.55,
        )
    )

    assert result["ok"] is True
    assert int(result["row_count"]) == 4
    assert int(result["bullish_candle"]["count"]) == 3
    assert float(result["bullish_candle"]["mean20"]) > 0.0
    assert float(result["contrarian_bullish"]["mean20"]) > 0.0
    assert float(result["breakdown_continuation"]["mean20"]) > float(result["squeeze_loss"]["mean20"])
    assert int(result["path_summary"]["bullish_candle_contrarian"]["count"]) == 2
    assert int(result["path_summary"]["squeeze_loss"]["count"]) == 1

    json_path = tmp_path / "sell_path.json"
    md_path = tmp_path / "sell_path.md"
    sell_paths._write_json_report(result, json_path)
    sell_paths._write_markdown_report(result, md_path)
    assert json_path.exists()
    assert md_path.exists()
    assert "bullish_candle_contrarian" in md_path.read_text(encoding="utf-8")
