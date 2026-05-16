from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_teppan_chart_pattern_discovery_v1 as mod


def _anchor_rows() -> pd.DataFrame:
    rows = []
    for idx in range(360):
        rows.append(
            {
                "code": f"70{idx % 60:02d}",
                "anchor_month": f"{2023 + (idx % 24) // 12}-{(idx % 24) % 12 + 1:02d}",
                "daily_ma_stack": "daily_bull_stack_5_20_60",
                "daily_ma60_slope_state": "daily_ma60_rising",
                "daily_ret20_state": "daily20_up",
                "daily_candle_state": "daily_strong_bull",
                "daily_volume_state": "daily_volume_expansion",
                "daily_sequence_state": "daily_sequence_bullish",
                "weekly_trend_state": "weekly_uptrend",
                "weekly_ret4_state": "weekly4_up",
                "weekly_candle_state": "weekly_strong_bull",
                "weekly_volume_state": "weekly_volume_expansion",
                "monthly_trend_state": "monthly_uptrend",
                "monthly_ret6_state": "monthly6_up",
                "monthly_candle_state": "monthly_strong_bull",
                "monthly_volume_state": "monthly_volume_normal",
                "ret20_fwd": 0.03 if idx % 10 != 0 else -0.01,
                "ret40_fwd": 0.04,
                "mfe20": 0.06,
                "mae20": -0.02,
                "win20": idx % 10 != 0,
                "win40": True,
                "severe_loss20": False,
            }
        )
    return pd.DataFrame(rows)


def _make_daily(symbol: str, periods: int, offset: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    rows = []
    for idx, day in enumerate(dates):
        wave = [0.0, 0.8, 1.2, 0.4, -0.3, 0.7, 1.5, 1.8, 1.0, 0.2][idx % 10]
        close = 50.0 + offset + idx * 0.06 + wave
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": close - 0.3,
                "h": close + 1.0,
                "l": close - 0.8,
                "c": close,
                "v": 1000 + idx * 5 + (500 if idx % 9 == 0 else 0),
                "source": "pan",
            }
        )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    daily = pd.concat([_make_daily(f"70{idx:02d}", 300, float(idx)) for idx in range(1, 5)], ignore_index=True)
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


def test_pattern_keys_do_not_use_future_labels() -> None:
    assert mod.SIGNAL_FEATURE_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)
    audit = mod.build_feature_availability_audit(_anchor_rows())
    assert audit["used_future_labels_in_pattern_keys"] is False
    assert audit["silent_fallback_used"] is False


def test_evaluate_pattern_families_finds_teppan_candidate() -> None:
    rows = mod.evaluate_pattern_families(_anchor_rows())

    assert rows[0]["pattern_decision"] == "teppan_candidate"
    assert rows[0]["win_rate20"] >= 0.58
    assert rows[0]["avg_ret20"] >= 0.015


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    result = mod.run_teppan_chart_pattern_discovery_v1(
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
    candidates = json.loads((output_dir / "teppan_candidates.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["candidate_scoring_created"] is False
    assert candidates["schema_version"].endswith("_teppan_candidates_v1")
