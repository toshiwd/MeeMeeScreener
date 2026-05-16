from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_daily_selection_replay_learning_v1 as mod


def _base_signal_row() -> dict[str, object]:
    return {
        "daily_ma_stack": "daily_bull_stack_5_20_60",
        "daily_ma60_slope_state": "daily_ma60_rising",
        "daily_ret20_state": "daily20_strong_up",
        "daily_candle_state": "daily_strong_bull",
        "daily_volume_state": "daily_volume_expansion",
        "daily_sequence_state": "daily_sequence_bullish",
        "weekly_trend_state": "weekly_uptrend",
        "weekly_ret4_state": "weekly4_up",
        "weekly_candle_state": "weekly_strong_bull",
        "weekly_volume_state": "weekly_volume_normal",
        "monthly_trend_state": "monthly_uptrend",
        "monthly_ret6_state": "monthly6_up",
        "monthly_candle_state": "monthly_strong_bull",
        "monthly_volume_state": "monthly_volume_normal",
        "downside_guard_blocked": False,
    }


def _make_daily(symbol: str, periods: int, offset: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-09-02", periods=periods)
    rows = []
    for idx, day in enumerate(dates):
        close = 50.0 + offset + idx * 0.11 + (0.25 if idx % 9 == 0 else 0.0)
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": close - 0.7,
                "h": close + 0.4,
                "l": close - 0.9,
                "c": close,
                "v": 1000 + idx * 7 + (900 if idx % 11 == 0 else 0),
                "source": "pan",
            }
        )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    daily = pd.concat([_make_daily(f"7{idx:03d}", 520, float(idx)) for idx in range(1, 9)], ignore_index=True)
    daily = daily.sort_values(["code", "date"]).copy()
    daily["ma20"] = daily.groupby("code")["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    daily["ma60"] = daily.groupby("code")["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["month_key"] = pd.to_datetime(daily["date"].astype(str), format="%Y%m%d").dt.to_period("M")
    monthly_rows = []
    for (code, month), group in daily.groupby(["code", "month_key"], sort=True):
        monthly_rows.append(
            {
                "code": code,
                "month": int(month.to_timestamp().strftime("%Y%m%d")),
                "o": float(group.iloc[0]["o"]),
                "h": float(group["h"].max()),
                "l": float(group["l"].min()),
                "c": float(group.iloc[-1]["c"]),
                "v": int(group["v"].sum()),
            }
        )
    monthly = pd.DataFrame(monthly_rows).sort_values(["code", "month"]).copy()
    monthly["ma20"] = monthly.groupby("code")["c"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    monthly["ma60"] = monthly.groupby("code")["c"].transform(lambda s: s.rolling(6, min_periods=1).mean())
    conn = duckdb.connect(str(path))
    try:
        daily_db = daily.drop(columns=["month_key"])
        conn.register("daily_db", daily_db)
        conn.register("monthly", monthly)
        conn.execute("CREATE TABLE daily_bars AS SELECT code, date, o, h, l, c, v, source FROM daily_db")
        conn.execute("CREATE TABLE daily_ma AS SELECT code, date, ma20, ma60 FROM daily_db")
        conn.execute("CREATE TABLE monthly_bars AS SELECT code, month, o, h, l, c, v FROM monthly")
        conn.execute("CREATE TABLE monthly_ma AS SELECT code, month, ma20, ma60 FROM monthly")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def test_entry_scoring_does_not_use_future_labels() -> None:
    positive_label_row = {**_base_signal_row(), "ret20_fwd": 0.50, "win20": True, "severe_loss20": False}
    negative_label_row = {**_base_signal_row(), "ret20_fwd": -0.50, "win20": False, "severe_loss20": True}

    assert mod.ENTRY_SCORING_FEATURE_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)
    assert mod.score_entry_candidate(positive_label_row)["score"] == mod.score_entry_candidate(negative_label_row)["score"]

    audit = mod.build_feature_availability_audit(
        pd.DataFrame(
            [
                {
                    "code": "7001",
                    "ymd": 20260105,
                    **_base_signal_row(),
                    "ret20_fwd": -0.50,
                    "win20": False,
                }
            ]
        )
    )
    assert audit["used_future_labels_in_scoring"] is False
    assert audit["no_lookahead_pass"] is True
    assert audit["silent_fallback_used"] is False


def test_entry_threshold_is_fixed_input_not_label_driven() -> None:
    row = _base_signal_row()

    loose = mod.score_entry_candidate(row, entry_score_threshold=10)
    strict = mod.score_entry_candidate(row, entry_score_threshold=20)

    assert loose["entry_allowed_by_score"] is True
    assert strict["entry_allowed_by_score"] is False
    assert loose["score"] == strict["score"]


def test_synthetic_replay_writes_required_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    result = mod.run_daily_selection_replay_learning_v1(
        source_db=db_path,
        output_root=tmp_path / "out",
        run_id="smoke",
        start_ymd=20260101,
        entry_score_threshold=10,
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    summary = json.loads((output_dir / "replay_summary.json").read_text(encoding="utf-8"))
    feature_audit = json.loads((output_dir / "feature_availability_audit.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["silent_fallback_used"] is False
    assert decision["candidate_local_decision"] in {"keep", "hold", "drop"}
    assert feature_audit["no_lookahead_pass"] is True
    assert summary["closed_trade_count"] >= 1


def test_decision_policy_keep_winning_state() -> None:
    summary = {
        "closed_trade_count": 12,
        "realized_portfolio_return": 0.08,
        "win_rate": 0.75,
        "profit_factor": 2.0,
        "max_drawdown": -0.05,
        "severe_loss_trade_rate": 0.0,
        "silent_fallback_used": False,
    }
    decision = mod.apply_decision_policy(summary, {"no_lookahead_pass": True, "used_future_labels_in_scoring": False})

    assert decision["authoritative_research_decision"] == "keep"
    assert decision["authoritative_reason"] == "winning_state"
    assert decision["winning_state_achieved"] is True


def test_decision_policy_hold_positive_insufficient_sample_or_one_risk_gate() -> None:
    insufficient_sample = {
        "closed_trade_count": 4,
        "realized_portfolio_return": 0.03,
        "win_rate": 0.75,
        "profit_factor": 2.0,
        "max_drawdown": -0.05,
        "severe_loss_trade_rate": 0.0,
        "silent_fallback_used": False,
    }
    one_risk_gate_fail = {
        "closed_trade_count": 12,
        "realized_portfolio_return": 0.03,
        "win_rate": 0.50,
        "profit_factor": 1.5,
        "max_drawdown": -0.05,
        "severe_loss_trade_rate": 0.0,
        "silent_fallback_used": False,
    }
    audit = {"no_lookahead_pass": True, "used_future_labels_in_scoring": False}

    assert mod.apply_decision_policy(insufficient_sample, audit)["authoritative_research_decision"] == "hold"
    assert mod.apply_decision_policy(one_risk_gate_fail, audit)["authoritative_research_decision"] == "hold"


def test_decision_policy_drop_when_negative_or_fallback() -> None:
    negative = {
        "closed_trade_count": 12,
        "realized_portfolio_return": -0.01,
        "win_rate": 0.75,
        "profit_factor": 2.0,
        "max_drawdown": -0.05,
        "severe_loss_trade_rate": 0.0,
        "silent_fallback_used": False,
    }
    fallback = {**negative, "realized_portfolio_return": 0.05, "silent_fallback_used": True}
    audit = {"no_lookahead_pass": True, "used_future_labels_in_scoring": False}

    assert mod.apply_decision_policy(negative, audit)["authoritative_research_decision"] == "drop"
    assert mod.apply_decision_policy(fallback, audit)["authoritative_research_decision"] == "drop"
