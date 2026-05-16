from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_ma5_reclaim_ma20_exit_probe_v1 as mod


def _manual_feature_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=14)
    rows = []
    closes = [100, 99, 98, 101, 102, 103, 104, 105, 104, 96, 95, 96, 97, 98]
    for idx, (day, close) in enumerate(zip(dates, closes)):
        rows.append(
            {
                "code": "7001",
                "ymd": int(day.strftime("%Y%m%d")),
                "date": day,
                "o": close - 0.5,
                "h": close + 1.0,
                "l": close - 1.0,
                "c": close,
                "v": 1000,
                "ma5": 100.0,
                "ma20": 100.0,
                "ma60": 90.0,
                "prev_c": 99.0,
                "prev_ma5": 100.0,
                "history_days": 100 + idx,
                "cross_above_ma5": idx == 3,
                "above_ma5": 3 <= idx <= 8,
                "ma_stack": "bull_stack_5_20_60",
                "price_vs_ma20": "price_above_ma20",
                "price_vs_ma60": "price_above_ma60",
                "ma20_vs_ma60": "ma20_above_ma60",
                "ma20_slope_state": "ma20_rising",
                "ma60_slope_state": "ma60_rising",
                "ma20_slope_20d": 0.05,
                "ma60_slope_20d": 0.04,
            }
        )
    return pd.DataFrame(rows)


def _make_cycle_rows(symbol: str, *, periods: int, offset: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    rows = []
    for idx, day in enumerate(dates):
        cycle = [0.0, -1.8, -2.4, -1.2, 0.8, 1.8, 2.6, 2.9, 1.4, -0.4, -1.5, -0.8][idx % 12]
        trend = idx * 0.03
        close = 100.0 + offset + trend + cycle
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": close - 0.2,
                "h": close + 0.9,
                "l": close - 0.9,
                "c": close,
                "v": 1000 + idx,
                "source": "pan",
            }
        )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    daily = pd.concat(
        [_make_cycle_rows(f"70{idx:02d}", periods=220, offset=float(idx)) for idx in range(1, 5)],
        ignore_index=True,
    )
    daily = daily.sort_values(["code", "date"]).copy()
    daily["ma20"] = daily.groupby("code")["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    daily["ma60"] = daily.groupby("code")["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE daily_bars AS SELECT code, date, o, h, l, c, v, source FROM daily")
        conn.execute("CREATE TABLE daily_ma AS SELECT code, date, ma20, ma60 FROM daily")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def test_entry_is_next_open_after_four_ma5_closes_and_exit_below_ma20() -> None:
    trades = mod.simulate_trades(_manual_feature_frame(), anchor_start_ymd=20250101, max_holding_days=10)

    assert len(trades) == 1
    trade = trades.iloc[0]
    assert trade["cross_date"] == "2025-01-07"
    assert trade["signal_date"] == "2025-01-10"
    assert trade["entry_date"] == "2025-01-13"
    assert trade["exit_date"] == "2025-01-15"
    assert trade["exit_reason"] == "close_below_ma20"
    assert trade["ma_stack"] == "bull_stack_5_20_60"


def test_signal_feature_set_excludes_future_labels() -> None:
    assert mod.SIGNAL_FEATURE_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)


def test_future_bar_does_not_change_existing_ma5_signal() -> None:
    base = pd.DataFrame(
        {
            "code": ["7001"] * 90,
            "ymd": [int(day.strftime("%Y%m%d")) for day in pd.bdate_range("2025-01-02", periods=90)],
            "date": pd.bdate_range("2025-01-02", periods=90),
            "o": [100 + idx * 0.1 for idx in range(90)],
            "h": [101 + idx * 0.1 for idx in range(90)],
            "l": [99 + idx * 0.1 for idx in range(90)],
            "c": [100 + idx * 0.1 for idx in range(90)],
            "v": [1000] * 90,
            "ma20": [None] * 90,
            "ma60": [None] * 90,
        }
    )
    clean = mod.build_ma_features(base)
    future = pd.concat(
        [
            base,
            pd.DataFrame(
                [
                    {
                        "code": "7001",
                        "ymd": 20260101,
                        "date": pd.Timestamp("2026-01-01"),
                        "o": 500.0,
                        "h": 500.0,
                        "l": 500.0,
                        "c": 500.0,
                        "v": 1,
                        "ma20": None,
                        "ma60": None,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    with_future = mod.build_ma_features(future)

    clean_row = clean[clean["ymd"].eq(base.iloc[-1]["ymd"])].iloc[0]
    future_row = with_future[with_future["ymd"].eq(base.iloc[-1]["ymd"])].iloc[0]
    assert clean_row["ma5"] == future_row["ma5"]
    assert clean_row["ma20"] == future_row["ma20"]


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    result = mod.run_ma5_reclaim_ma20_exit_probe_v1(
        source_db=db_path,
        output_root=tmp_path / "out",
        run_id="smoke",
        years=1,
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((output_dir / "feature_availability_audit.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["publish_bundle_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert audit["used_future_labels_in_signal"] is False
