from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

from app.backend.tools import swing_gate_relearn as relearn


def _build_daily_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2024-01-02", periods=80)
    code_specs = {
        "BKA": {
            "entry_idx": 10,
            "pre_ret": 0.0,
            "post_rets": [0.05] * 6 + [0.0] * 20,
        },
        "BKB": {
            "entry_idx": 20,
            "pre_ret": 0.0,
            "post_rets": [0.04] * 6 + [0.0] * 20,
        },
        "BKC": {
            "entry_idx": 30,
            "pre_ret": 0.0,
            "post_rets": [0.0] * 3 + [-0.01] * 20,
        },
        "SKA": {
            "entry_idx": 12,
            "pre_ret": 0.0,
            "post_rets": [-0.035] * 6 + [0.0] * 20,
        },
        "SKB": {
            "entry_idx": 22,
            "pre_ret": 0.0,
            "post_rets": [-0.03] * 6 + [0.0] * 20,
        },
        "SKC": {
            "entry_idx": 32,
            "pre_ret": 0.0,
            "post_rets": [0.0] * 3 + [0.01] * 20,
        },
        "BKD": {
            "entry_idx": 50,
            "pre_ret": 0.0,
            "post_rets": [0.03] * 6 + [0.0] * 20,
        },
        "SKD": {
            "entry_idx": 52,
            "pre_ret": 0.0,
            "post_rets": [-0.025] * 6 + [0.0] * 20,
        },
    }
    for code, spec in code_specs.items():
        price = 100.0
        for idx, dt in enumerate(dates):
            if idx < int(spec["entry_idx"]):
                ret = float(spec["pre_ret"])
            else:
                post_idx = min(idx - int(spec["entry_idx"]), len(spec["post_rets"]) - 1)
                ret = float(spec["post_rets"][post_idx])
            open_ = price
            close = price * (1.0 + ret)
            high_buffer = 0.12 if code in {"BKA", "BKB", "BKD"} and idx >= int(spec["entry_idx"]) and post_idx < 4 else 0.01
            high = max(open_, close) * (1.0 + high_buffer)
            low = min(open_, close) * (1.0 - 0.01)
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
    specs = [
        ("BKA", "buy", "breakout", 10, 0.82, 0.22, 0.05, 0.24, -0.03, "risk_on_trend", 104.0, 103.0, 102.0, 101.0, 0.05, 0.18, 0.18),
        ("BKB", "buy", "breakout", 20, 0.70, 0.20, 0.04, 0.22, -0.04, "neutral_range", 104.0, 103.0, 102.0, 101.0, 0.32, 0.05, 0.05),
        ("BKC", "buy", "breakout", 30, 0.71, -0.08, -0.03, -0.10, -0.08, "risk_on_trend", 103.0, 102.5, 101.5, 100.5, 0.35, 0.05, 0.05),
        ("SKA", "sell", "breakdown", 12, 0.83, -0.18, -0.10, -0.22, 0.03, "risk_off_trend", 95.0, 98.0, 100.5, 100.0, 0.30, 0.04, 0.04),
        ("SKB", "sell", "pressure", 22, 0.68, -0.12, -0.07, -0.15, 0.06, "risk_off_trend", 98.0, 99.0, 100.5, 100.0, 0.35, 0.05, 0.05),
        ("SKC", "sell", "breakdown", 32, 0.72, 0.10, 0.04, 0.12, -0.10, "risk_off_trend", 99.0, 100.0, 100.5, 100.0, 0.34, 0.05, 0.05),
        ("BKD", "buy", "breakout", 50, 0.81, 0.21, 0.06, 0.23, 0.27, "risk_on_trend", 105.0, 104.0, 103.0, 102.0, 0.07, 0.16, 0.16),
        ("SKD", "sell", "breakdown", 52, 0.82, -0.14, -0.09, -0.20, -0.24, "risk_off_trend", 94.0, 97.0, 99.5, 100.0, 0.28, 0.05, 0.05),
    ]
    rows: list[dict[str, object]] = []
    for code, side, setup_type, signal_idx, entry_score, fr5, fr10, fr20, fr30, regime, close, ma7, ma20, ma60, body_ratio, upper_wick, lower_wick in specs:
        rows.append(
            {
                "signal_dt": int(ymds[signal_idx]),
                "code": code,
                "side": side,
                "setup_type": setup_type,
                "entry_qualified": True,
                "forward_return_5": fr5,
                "forward_return_10": fr10,
                "forward_return_20": fr20,
                "forward_return_30": fr30,
                "max_favorable_30": 0.26 if side == "buy" and entry_score >= 0.75 else (0.18 if side == "buy" else -0.04),
                "max_adverse_30": -0.08 if side == "buy" and fr20 < 0 else (-0.22 if side == "sell" and fr20 < 0 else 0.12),
                "entry_score": entry_score,
                "close": close,
                "ma7": ma7,
                "ma20": ma20,
                "ma60": ma60,
                "body_ratio": body_ratio,
                "upper_wick_ratio": upper_wick,
                "lower_wick_ratio": lower_wick,
                "regime_id": regime,
            }
        )
    return pd.DataFrame(rows)


def _fake_get_conn():
    class _Result:
        def fetchone(self):
            return (1,)

    class _Conn:
        def execute(self, *args, **kwargs):
            return _Result()

    @contextmanager
    def _ctx():
        yield _Conn()

    return _ctx()


def test_swing_gate_relearn_soft_ranking_recovers_missed_winners(monkeypatch, tmp_path) -> None:
    daily = _build_daily_frame()
    signals = _build_signal_frame(daily)
    monkeypatch.setattr(relearn, "get_conn", _fake_get_conn)
    monkeypatch.setattr(relearn, "_load_daily_frame", lambda conn, lookback_days: daily)
    monkeypatch.setattr(relearn, "_load_signal_frame", lambda conn, start_ymd, end_ymd: signals)

    result = relearn.run_swing_gate_relearn(
        config=relearn.RelearnConfig(
            lookback_days=3650,
            initial_capital=1_000_000.0,
            transaction_cost_rate=0.0,
            report_dir=tmp_path,
            max_positions_buy=1,
            max_positions_sell=1,
            review_day_1=3,
            review_day_2=5,
            review_1_mfe_min=0.01,
            review_1_ret_min=0.0,
            review_2_mfe_min=0.02,
            review_2_ret_min=0.0,
        )
    )

    assert result["ok"] is True
    assert int(result["variants"]["buy"]["strict"]["selection"]["excluded_winners"]) == 1
    assert int(result["variants"]["buy"]["soft"]["selection"]["excluded_winners"]) == 0
    assert float(result["missed_opportunity_delta"]["buy"]["winner_capture_delta"]) > 0.0
    assert int(result["variants"]["sell"]["strict"]["selection"]["excluded_winners"]) == 1
    assert int(result["variants"]["sell"]["soft"]["selection"]["excluded_winners"]) == 0
    assert float(result["missed_opportunity_delta"]["sell"]["winner_capture_delta"]) > 0.0
    assert float(result["variants"]["buy"]["soft_review"]["book"]["final_capital"]) > float(result["variants"]["buy"]["soft"]["book"]["final_capital"])
    assert float(result["variants"]["buy"]["soft_review"]["book"]["max_drawdown"]) >= float(result["variants"]["buy"]["soft"]["book"]["max_drawdown"])
    assert sum(int(v) for k, v in result["variants"]["buy"]["soft_review"]["book"]["exit_reason_counts"].items() if str(k).startswith("review_")) >= 1
    assert float(result["variants"]["sell"]["soft_review"]["book"]["final_capital"]) > float(result["variants"]["sell"]["soft"]["book"]["final_capital"])
    assert sum(int(v) for k, v in result["variants"]["sell"]["soft_review"]["book"]["exit_reason_counts"].items() if str(k).startswith("review_")) >= 1

    json_path = tmp_path / "relearn.json"
    md_path = tmp_path / "relearn.md"
    relearn._write_json_report(result, json_path)
    relearn._write_markdown_report(result, md_path)
    assert json_path.exists()
    assert md_path.exists()
    assert "winner_capture_delta" in md_path.read_text(encoding="utf-8")
    assert json_path.read_text(encoding="utf-8").strip()
