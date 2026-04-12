from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.backend.tools import buy_monthly_rebound_stability_loop as loop


def _build_frame() -> pd.DataFrame:
    rows = [
        {
            "signal_dt": 20240115,
            "code": "A",
            "forward_return_5": 0.04,
            "forward_return_10": 0.08,
            "forward_return_20": 0.18,
            "forward_return_30": 0.22,
            "monthly_rebound_context": True,
            "monthly_capitulation_context": True,
            "monthly_ma_reclaim": True,
            "daily_reversal_up": True,
            "daily_hammer_like": True,
            "daily_bullish_close": True,
            "monthly_zone": "bear_stack",
            "monthly_sideways_context": False,
            "setup_type": "rebound",
            "regime_id": "capitulation_rebound",
            "gap_ma3": -0.05,
            "gap_ma6": -0.08,
            "gap_ma12": -0.11,
            "close_pos_in_range": 0.32,
        },
        {
            "signal_dt": 20240220,
            "code": "B",
            "forward_return_5": 0.03,
            "forward_return_10": 0.07,
            "forward_return_20": 0.16,
            "forward_return_30": 0.19,
            "monthly_rebound_context": True,
            "monthly_capitulation_context": True,
            "monthly_ma_reclaim": True,
            "daily_reversal_up": True,
            "daily_hammer_like": True,
            "daily_bullish_close": True,
            "monthly_zone": "bear_stack",
            "monthly_sideways_context": False,
            "setup_type": "turn",
            "regime_id": "risk_off_trend",
            "gap_ma3": -0.04,
            "gap_ma6": -0.07,
            "gap_ma12": -0.09,
            "close_pos_in_range": 0.30,
        },
        {
            "signal_dt": 20240418,
            "code": "C",
            "forward_return_5": 0.01,
            "forward_return_10": 0.02,
            "forward_return_20": 0.03,
            "forward_return_30": 0.04,
            "monthly_rebound_context": True,
            "monthly_capitulation_context": True,
            "monthly_ma_reclaim": False,
            "daily_reversal_up": False,
            "daily_hammer_like": False,
            "daily_bullish_close": False,
            "monthly_zone": "bear_extension",
            "monthly_sideways_context": False,
            "setup_type": "breakout",
            "regime_id": "neutral_range",
            "gap_ma3": -0.02,
            "gap_ma6": -0.04,
            "gap_ma12": -0.06,
            "close_pos_in_range": 0.45,
        },
        {
            "signal_dt": 20240521,
            "code": "D",
            "forward_return_5": 0.05,
            "forward_return_10": 0.09,
            "forward_return_20": 0.14,
            "forward_return_30": 0.17,
            "monthly_rebound_context": True,
            "monthly_capitulation_context": True,
            "monthly_ma_reclaim": True,
            "daily_reversal_up": True,
            "daily_hammer_like": True,
            "daily_bullish_close": True,
            "monthly_zone": "bear_stack",
            "monthly_sideways_context": False,
            "setup_type": "rebound",
            "regime_id": "capitulation_rebound",
            "gap_ma3": -0.06,
            "gap_ma6": -0.09,
            "gap_ma12": -0.12,
            "close_pos_in_range": 0.29,
        },
        {
            "signal_dt": 20240614,
            "code": "E",
            "forward_return_5": 0.02,
            "forward_return_10": 0.04,
            "forward_return_20": 0.09,
            "forward_return_30": 0.11,
            "monthly_rebound_context": True,
            "monthly_capitulation_context": True,
            "monthly_ma_reclaim": True,
            "daily_reversal_up": True,
            "daily_hammer_like": True,
            "daily_bullish_close": True,
            "monthly_zone": "bear_stack",
            "monthly_sideways_context": False,
            "setup_type": "turn",
            "regime_id": "risk_off_trend",
            "gap_ma3": -0.04,
            "gap_ma6": -0.07,
            "gap_ma12": -0.10,
            "close_pos_in_range": 0.34,
        },
        {
            "signal_dt": 20240719,
            "code": "F",
            "forward_return_5": -0.01,
            "forward_return_10": 0.00,
            "forward_return_20": 0.01,
            "forward_return_30": 0.02,
            "monthly_rebound_context": True,
            "monthly_capitulation_context": False,
            "monthly_ma_reclaim": False,
            "daily_reversal_up": False,
            "daily_hammer_like": False,
            "daily_bullish_close": False,
            "monthly_zone": "sideways",
            "monthly_sideways_context": True,
            "setup_type": "breakout",
            "regime_id": "neutral_range",
            "gap_ma3": -0.01,
            "gap_ma6": -0.02,
            "gap_ma12": -0.03,
            "close_pos_in_range": 0.52,
        },
    ]
    return pd.DataFrame(rows)


def test_buy_monthly_rebound_stability_loop_finds_stable_monthly_bear_stack(monkeypatch, tmp_path: Path) -> None:
    frame = _build_frame()
    monkeypatch.setattr(loop.rebound, "_build_buy_monthly_rebound_frame", lambda config: (frame, 20240719))  # type: ignore[attr-defined]

    result = loop.run_buy_monthly_rebound_stability_loop(
        config=loop.BuyMonthlyReboundStabilityLoopConfig(
            lookback_days=3650,
            report_dir=tmp_path,
            min_rule_count=1,
            min_window_count=1,
        )
    )

    assert result["ok"] is True
    assert int(result["candidate_count"]) > 0
    top = result["top_candidate"]
    assert top["best_candidate"]["bucket"] in {
        "monthly_bear_stack__ma_reclaim",
        "monthly_bear_stack__reversal_up",
        "monthly_rebound_context",
    }
    assert float(top["best_candidate"]["mean20"]) > 0.0
    assert float(top["best_candidate"]["stability_score"]) > 0.0

    json_path = tmp_path / "loop.json"
    md_path = tmp_path / "loop.md"
    loop._write_json_report(result, json_path)
    loop._write_markdown_report(result, md_path)
    assert json_path.exists()
    assert md_path.exists()
    assert "Top Candidates" in md_path.read_text(encoding="utf-8")
