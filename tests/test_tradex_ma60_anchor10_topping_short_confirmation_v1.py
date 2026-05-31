from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_ma60_anchor10_topping_short_confirmation_v1 import (
    CLOSURE_ARTIFACTS,
    NEW_FAMILY_ARTIFACTS,
    _days_bucket,
    construct_anchor10_rows,
    decide,
    make_closure_artifact,
    run,
)


def test_days_bucket_contract() -> None:
    assert _days_bucket(0) == "0-5"
    assert _days_bucket(5) == "0-5"
    assert _days_bucket(6) == "6-10"
    assert _days_bucket(10) == "6-10"
    assert _days_bucket(11) == "11-20"
    assert _days_bucket(20) == "11-20"
    assert _days_bucket(21) == "out_of_window"


def test_construct_anchor10_rows_labels_short_outcomes() -> None:
    short_rows = pd.DataFrame(
        [
            {
                "source_artifact": "x",
                "source_name": "x",
                "source_type": "actual_trade_rows",
                "code": "1000",
                "decision_ymd": 20250110,
                "raw_side": "short",
                "guard_hit": "True",
                "guard_anchor_type": "anchor_10",
                "guard_anchor_ymd": 20250101,
                "ret20_long": -0.05,
                "ret40_long": -0.07,
                "short_return20": 0.05,
                "short_return40": 0.07,
                "regime_proxy": "low_volatility_monthly_breakout_high_zone",
                "year": 2025,
            },
            {
                "source_artifact": "x",
                "source_name": "x",
                "source_type": "actual_trade_rows",
                "code": "1000",
                "decision_ymd": 20250111,
                "raw_side": "short",
                "guard_hit": "False",
                "guard_anchor_type": pd.NA,
                "guard_anchor_ymd": pd.NA,
                "ret20_long": 0.03,
                "ret40_long": 0.02,
                "short_return20": -0.03,
                "short_return40": -0.02,
                "regime_proxy": "low_volatility_non_breakout",
                "year": 2025,
            },
        ]
    )
    daily = pd.DataFrame(
        {
            "code": ["1000", "1000"],
            "date_dt": pd.to_datetime(["2025-01-01", "2025-01-10"]),
            "open": [100.0, 108.0],
            "high": [101.0, 110.0],
            "low": [99.0, 104.0],
            "decision_close": [100.0, 105.0],
            "close": [100.0, 105.0],
            "ma7": [99.0, 106.0],
            "ma20": [98.0, 107.0],
            "ma60": [95.0, 100.0],
            "ma7_slope": [0.01, -0.01],
            "ma20_slope": [0.01, -0.01],
            "ma60_slope": [0.01, 0.0],
            "dist_ma7_pct": [0.01, -0.01],
            "dist_ma20_pct": [0.02, -0.02],
            "dist_ma60_pct": [0.05, 0.05],
            "below_ma7": [False, True],
            "below_ma20": [False, True],
            "large_bearish_candle": [False, True],
            "upper_wick_ratio": [0.1, 0.2],
            "bearish_engulfing": [False, False],
            "failed_high_update": [False, True],
            "volume_spike_down_day": [False, True],
            "drawdown_from_recent_high_pct": [0.0, -0.05],
        }
    )

    rows, report = construct_anchor10_rows(short_rows, daily)

    assert report["anchor10_guard_hit_rows"] == 1
    assert rows.loc[0, "helped_short"] is True or bool(rows.loc[0, "helped_short"])
    assert rows.loc[1, "anchor10_guard_miss"] is True or bool(rows.loc[1, "anchor10_guard_miss"])
    assert rows.loc[0, "days_since_anchor_bucket"] == "6-10"


def test_decide_inconclusive_when_recent_forward_coverage_low() -> None:
    rows = pd.DataFrame(
        [
            {
                "anchor10_guard_hit": True,
                "year": 2025,
                "source_type": "actual_trade_rows",
                "ret20_long": -0.03 if idx < 10 else pd.NA,
                "ret40_long": -0.05 if idx < 10 else pd.NA,
                "short_return20": 0.03 if idx < 10 else pd.NA,
                "short_return40": 0.05 if idx < 10 else pd.NA,
                "helped_short": idx < 10,
                "harmed_short": False,
                "neutral_short": False,
            }
            for idx in range(30)
        ]
    )

    decision = decide(rows)

    assert decision["research_decision"] == "inconclusive"
    assert "coverage" in decision["reason_typed"][0]


def test_make_closure_artifact_writes_non_promotion_contract(tmp_path: Path) -> None:
    failure_root = tmp_path / "failure"
    output_root = tmp_path / "closure"
    failure_root.mkdir()
    (failure_root / "research_decision.json").write_text(json.dumps({"research_decision": "drop_short_veto"}), encoding="utf-8")
    (failure_root / "recent_degradation_summary.json").write_text(json.dumps({"x": 1}), encoding="utf-8")
    (failure_root / "salvageability_summary.json").write_text(json.dumps({"salvageability_decision": "drop_short_veto"}), encoding="utf-8")

    out = make_closure_artifact(failure_root=failure_root, output_root=output_root)

    assert all((out / artifact).exists() for artifact in CLOSURE_ARTIFACTS)
    decision = json.loads((out / "research_decision.json").read_text(encoding="utf-8"))
    assert decision["research_decision"] == "family_closed_drop"
    assert decision["meemee_reflectable"] is False


def test_run_writes_required_new_family_artifacts(tmp_path: Path) -> None:
    guard_root = tmp_path / "guard"
    short_root = tmp_path / "short"
    failure_root = tmp_path / "failure"
    output_root = tmp_path / "out"
    closure_root = tmp_path / "closure"
    for path in [guard_root, short_root, failure_root]:
        path.mkdir()
    (guard_root / "selected_guard_rules.json").write_text(json.dumps({"rules": []}), encoding="utf-8")
    (short_root / "no_lookahead_audit.json").write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")
    (failure_root / "research_decision.json").write_text(json.dumps({"research_decision": "drop_short_veto"}), encoding="utf-8")
    (failure_root / "recent_degradation_summary.json").write_text(json.dumps({}), encoding="utf-8")
    (failure_root / "salvageability_summary.json").write_text(json.dumps({}), encoding="utf-8")

    dates = pd.date_range("2024-10-01", periods=90, freq="D")
    daily = pd.DataFrame(
        {
            "code": ["1000"] * len(dates),
            "date": dates.strftime("%Y-%m-%d"),
            "open": [100.0 + i for i in range(len(dates))],
            "high": [101.0 + i for i in range(len(dates))],
            "low": [99.0 + i for i in range(len(dates))],
            "close": [100.0 + i for i in range(len(dates))],
            "volume": [1000 + i for i in range(len(dates))],
        }
    )
    daily_path = tmp_path / "daily.csv"
    daily.to_csv(daily_path, index=False)
    short_rows = pd.DataFrame(
        [
            {
                "source_artifact": "x",
                "source_name": "x",
                "source_type": "actual_trade_rows",
                "code": "1000",
                "decision_ymd": 20241210,
                "raw_side": "short",
                "guard_hit": True,
                "guard_anchor_type": "anchor_10",
                "guard_anchor_ymd": 20241201,
                "guard_active_until_ymd": 20241220,
                "ret20_long": -0.03,
                "ret40_long": -0.05,
                "short_return20": 0.03,
                "short_return40": 0.05,
                "helped_veto": False,
                "harmed_veto": True,
                "neutral_veto": False,
                "ma20_break_within_20d": True,
                "ma60_break_within_20d": False,
                "ma20_and_ma60_break_within_20d": False,
                "regime_proxy": "low_volatility_monthly_breakout_high_zone",
                "year": 2024,
                "period_bucket": "2024-2026",
            }
        ]
    )
    short_rows.to_csv(short_root / "short_veto_rows.csv", index=False)

    result = run(guard_root=guard_root, short_replay_root=short_root, failure_root=failure_root, daily_path=daily_path, output_root=output_root, closure_output_root=closure_root)

    out = Path(result["output_dir"])
    assert all((out / artifact).exists() for artifact in NEW_FAMILY_ARTIFACTS)
    assert Path(result["closure_output_dir"]).exists()
