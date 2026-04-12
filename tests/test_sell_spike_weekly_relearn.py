from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from app.backend.tools import sell_spike_weekly_relearn as spike_weekly


def _build_daily_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2024-01-02", periods=60)
    specs = {
        "A": {"entry_idx": 24, "drift_before": 0.008, "drift_after": -0.012},
        "B": {"entry_idx": 26, "drift_before": 0.003, "drift_after": 0.001},
        "C": {"entry_idx": 28, "drift_before": -0.006, "drift_after": -0.010},
        "D": {"entry_idx": 30, "drift_before": 0.0, "drift_after": 0.0},
    }
    for code, spec in specs.items():
        price = 100.0
        for idx, dt in enumerate(dates):
            if idx < spec["entry_idx"]:
                ret = spec["drift_before"]
            else:
                ret = spec["drift_after"]
            open_ = price
            close = price * (1.0 + ret)
            if code == "A" and idx == spec["entry_idx"]:
                close = price * 1.06
                high = close * 1.02
                low = open_ * 0.99
            elif code == "B" and idx == spec["entry_idx"]:
                close = price * 1.03
                high = close * 1.01
                low = open_ * 0.995
            elif code == "C" and idx == spec["entry_idx"]:
                close = price * 0.95
                high = open_ * 1.005
                low = close * 0.98
            else:
                high = max(open_, close) * 1.01
                low = min(open_, close) * 0.99
            rows.append(
                {
                    "code": code,
                    "ymd": int(dt.strftime("%Y%m%d")),
                    "date_dt": dt.to_pydatetime(),
                    "o": open_,
                    "h": high,
                    "l": low,
                    "c": close,
                    "v": 2000,
                }
            )
            price = close
    return pd.DataFrame(rows)


def _build_signal_frame(daily: pd.DataFrame) -> pd.DataFrame:
    ymds = list(daily["ymd"].astype(int).drop_duplicates())
    entries = [
        {
            "code": "A",
            "signal_dt": int(ymds[24]),
            "setup_type": "pressure",
            "entry_score": 0.74,
            "forward_return_5": 0.02,
            "forward_return_10": 0.04,
            "forward_return_20": -0.12,
            "forward_return_30": -0.15,
            "max_favorable_30": 0.18,
            "max_adverse_30": -0.07,
            "bar_open": 106.0,
            "bar_high": 112.0,
            "bar_low": 105.0,
            "bar_close": 111.0,
            "body_ratio": 0.05,
            "upper_wick_ratio": 0.04,
            "lower_wick_ratio": 0.03,
            "candle_triplet_up_prob": 0.81,
            "candle_triplet_down_prob": 0.14,
            "gap_pct": 0.05,
            "close": 111.0,
            "ma7": 105.0,
            "ma20": 102.0,
            "ma60": 98.0,
            "close_ret2": 0.06,
            "close_ret3": 0.05,
            "close_ret20": -0.12,
            "close_ret60": -0.15,
            "breakout20_down": 0,
            "rebound60": 1,
            "drawdown60": -0.01,
            "market_ret20": -0.03,
            "breadth_above_ma20": 0.39,
            "breadth_above_ma60": 0.31,
            "sector_ret20": -0.02,
            "rel_sector_ret20": -0.01,
            "regime_id": "risk_off_trend",
        },
        {
            "code": "B",
            "signal_dt": int(ymds[26]),
            "setup_type": "pressure",
            "entry_score": 0.71,
            "forward_return_5": 0.01,
            "forward_return_10": 0.00,
            "forward_return_20": -0.03,
            "forward_return_30": -0.02,
            "max_favorable_30": 0.10,
            "max_adverse_30": -0.05,
            "bar_open": 103.0,
            "bar_high": 106.0,
            "bar_low": 102.5,
            "bar_close": 105.5,
            "body_ratio": 0.03,
            "upper_wick_ratio": 0.03,
            "lower_wick_ratio": 0.02,
            "candle_triplet_up_prob": 0.79,
            "candle_triplet_down_prob": 0.17,
            "gap_pct": 0.02,
            "close": 105.5,
            "ma7": 103.5,
            "ma20": 101.5,
            "ma60": 99.5,
            "close_ret2": 0.02,
            "close_ret3": 0.01,
            "close_ret20": -0.03,
            "close_ret60": -0.02,
            "breakout20_down": 0,
            "rebound60": 1,
            "drawdown60": 0.02,
            "market_ret20": 0.01,
            "breadth_above_ma20": 0.56,
            "breadth_above_ma60": 0.52,
            "sector_ret20": 0.02,
            "rel_sector_ret20": 0.00,
            "regime_id": "risk_on_trend",
        },
        {
            "code": "C",
            "signal_dt": int(ymds[28]),
            "setup_type": "breakdown",
            "entry_score": 0.81,
            "forward_return_5": -0.02,
            "forward_return_10": -0.06,
            "forward_return_20": -0.14,
            "forward_return_30": -0.17,
            "max_favorable_30": 0.15,
            "max_adverse_30": -0.08,
            "bar_open": 95.0,
            "bar_high": 95.5,
            "bar_low": 90.5,
            "bar_close": 91.0,
            "body_ratio": 0.04,
            "upper_wick_ratio": 0.04,
            "lower_wick_ratio": 0.08,
            "candle_triplet_up_prob": 0.22,
            "candle_triplet_down_prob": 0.71,
            "gap_pct": -0.03,
            "close": 91.0,
            "ma7": 97.0,
            "ma20": 100.0,
            "ma60": 102.0,
            "close_ret2": -0.03,
            "close_ret3": -0.04,
            "close_ret20": -0.14,
            "close_ret60": -0.17,
            "breakout20_down": 1,
            "rebound60": 0,
            "drawdown60": 0.10,
            "market_ret20": -0.05,
            "breadth_above_ma20": 0.37,
            "breadth_above_ma60": 0.28,
            "sector_ret20": -0.04,
            "rel_sector_ret20": -0.03,
            "regime_id": "risk_off_trend",
        },
        {
            "code": "D",
            "signal_dt": int(ymds[30]),
            "setup_type": "pressure",
            "entry_score": 0.66,
            "forward_return_5": 0.03,
            "forward_return_10": 0.02,
            "forward_return_20": 0.01,
            "forward_return_30": 0.00,
            "max_favorable_30": 0.05,
            "max_adverse_30": -0.03,
            "bar_open": 100.0,
            "bar_high": 101.0,
            "bar_low": 99.0,
            "bar_close": 100.2,
            "body_ratio": 0.01,
            "upper_wick_ratio": 0.01,
            "lower_wick_ratio": 0.01,
            "candle_triplet_up_prob": 0.43,
            "candle_triplet_down_prob": 0.38,
            "gap_pct": 0.00,
            "close": 100.2,
            "ma7": 100.0,
            "ma20": 100.0,
            "ma60": 100.0,
            "close_ret2": 0.00,
            "close_ret3": 0.00,
            "close_ret20": 0.01,
            "close_ret60": 0.00,
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
    ]
    return pd.DataFrame(entries)


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
                return _Result(20240223)
            return _Result(1)

    @contextmanager
    def _ctx():
        yield _Conn()

    return _ctx()


def test_sell_spike_weekly_relearn_scores_weekly_position_and_spikes(monkeypatch, tmp_path) -> None:
    daily = _build_daily_frame()
    signals = _build_signal_frame(daily)
    monkeypatch.setattr(spike_weekly, "get_conn", _fake_get_conn)
    monkeypatch.setattr(spike_weekly, "_table_exists", lambda conn, table_name: True)
    monkeypatch.setattr(spike_weekly, "_load_daily_frame", lambda conn, lookback_days: daily)
    monkeypatch.setattr(spike_weekly, "_load_sell_signal_frame", lambda conn, start_ymd, end_ymd: signals)

    result = spike_weekly.run_sell_spike_weekly_relearn(
        config=spike_weekly.SellSpikeWeeklyConfig(
            lookback_days=3650,
            report_dir=tmp_path,
            min_rule_count=1,
        )
    )

    assert result["ok"] is True
    assert int(result["row_count"]) == 4
    assert int(result["daily_spike"]["bullish"]["count"]) >= 2
    assert float(result["daily_spike"]["bullish"]["mean20"]) > float(result["daily_spike"]["none"]["mean20"])
    top_rules = result["top_rules"]
    assert top_rules
    assert any(str(row["bucket"]).startswith("bull_spike") for row in top_rules)
    assert any(str(row["bucket"]).startswith("weekly_zone:bull") for row in result["weekly_zone"])
    assert float(result["recommendation"]["mean20"]) > 0.0

    json_path = tmp_path / "sell_spike_weekly.json"
    md_path = tmp_path / "sell_spike_weekly.md"
    spike_weekly._write_json_report(result, json_path)
    spike_weekly._write_markdown_report(result, md_path)
    assert json_path.exists()
    assert md_path.exists()
    assert "weekly_zone:bull" in md_path.read_text(encoding="utf-8")
