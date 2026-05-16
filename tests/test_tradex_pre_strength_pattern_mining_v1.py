from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_pre_strength_pattern_mining_v1 as mod


def _event_rows() -> pd.DataFrame:
    rows = []
    for idx in range(180):
        rows.append(
            {
                "code": f"72{idx % 40:02d}",
                "event_month": f"{2023 + (idx % 24) // 12}-{(idx % 24) % 12 + 1:02d}",
                "pre_ret20_state": "pre20_flat",
                "pre_ret5_state": "pre5_up",
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "pre_ma60_context_state": "pre_ma60_near_or_above",
                "pre_candle_energy_state": "pre_candle_energy_positive",
                "pre_wick_warning_state": "pre_wicks_clean",
                "pre_volume_state": "pre_volume_normal",
                "pre_compression_state": "pre_range_compressed",
                "weekly_prior_state": "weekly_prior_recovery",
                "monthly_prior_state": "monthly_prior_uptrend",
                "event_daily_ret20_state": "daily20_up",
                "event_daily_candle_state": "daily_strong_bull",
                "ret20_fwd": 0.026 if idx % 10 != 0 else -0.01,
                "mfe20": 0.055,
                "mae20": -0.018,
                "win20": idx % 10 != 0,
                "severe_loss20": False,
            }
        )
    return pd.DataFrame(rows)


def _make_daily(symbol: str, periods: int, offset: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    close = 60.0 + offset
    rows = []
    for idx, day in enumerate(dates):
        phase = idx % 90
        if phase < 45:
            close *= 0.9995
        elif phase < 65:
            close *= 1.012
        else:
            close *= 0.996
        if phase in range(45, 65):
            open_ = close * 0.99
            high = close * 1.004
            low = open_ * 0.997
            volume = 1800 + idx * 3
        else:
            open_ = close * 1.002
            high = open_ * 1.004
            low = close * 0.996
            volume = 1000 + idx * 2
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": open_,
                "h": high,
                "l": low,
                "c": close,
                "v": volume,
                "source": "pan",
            }
        )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    daily = pd.concat([_make_daily(f"72{idx:02d}", 320, float(idx)) for idx in range(1, 7)], ignore_index=True)
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


def test_pattern_keys_do_not_use_forward_labels() -> None:
    assert mod.SIGNAL_FEATURE_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)
    audit = mod.build_feature_availability_audit(_event_rows())
    assert audit["used_future_labels_in_pattern_keys"] is False
    assert audit["silent_fallback_used"] is False


def test_evaluate_pre_strength_patterns_finds_teppan_candidate() -> None:
    rows = mod.evaluate_pre_strength_patterns(_event_rows())

    assert rows[0]["pattern_decision"] == "pre_strength_teppan_pattern"
    assert rows[0]["win_rate20"] >= 0.60
    assert rows[0]["avg_ret20"] >= 0.018


def test_pre_strength_run_writes_required_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    result = mod.run_pre_strength_pattern_mining_v1(
        source_db=db_path,
        output_root=tmp_path / "out",
        run_id="smoke",
        years=1,
        min_history_days=80,
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    patterns = json.loads((output_dir / "pre_strength_patterns.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["candidate_scoring_created"] is False
    assert complete["publish_bundle_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert patterns["schema_version"].endswith("_pre_strength_patterns_v1")
