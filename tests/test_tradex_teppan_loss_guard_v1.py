from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_teppan_loss_guard_v1 as mod


def _opportunities() -> pd.DataFrame:
    rows = []
    for idx in range(420):
        risky = idx < 120
        rows.append(
            {
                "code": f"7{idx % 80:03d}",
                "ymd": 20240101 + idx,
                "anchor_month": f"{2023 + (idx % 24) // 12}-{(idx % 24) % 12 + 1:02d}",
                "pattern_family": "higher_frame_confirmed_daily",
                "pattern_key": "synthetic",
                "opportunity_id": f"opp-{idx}",
                "daily_ma_stack": "daily_bull_stack_5_20_60" if risky else "daily_pullback_20_over_60",
                "daily_ma60_slope_state": "daily_ma60_rising",
                "daily_ret20_state": "daily20_strong_up" if risky else "daily20_up",
                "daily_candle_state": "daily_strong_bull",
                "daily_volume_state": "daily_volume_normal",
                "daily_sequence_state": "daily_sequence_bullish",
                "weekly_trend_state": "weekly_uptrend",
                "weekly_ret4_state": "weekly4_up",
                "weekly_candle_state": "weekly_strong_bull",
                "weekly_volume_state": "weekly_volume_normal",
                "monthly_trend_state": "monthly_mixed",
                "monthly_ret6_state": "monthly6_strong_down" if risky else "monthly6_flat",
                "monthly_candle_state": "monthly_small_neutral",
                "monthly_volume_state": "monthly_volume_normal",
                "ret20_fwd": -0.12 if risky else 0.025,
                "ret40_fwd": -0.08 if risky else 0.035,
                "mfe20": 0.03 if risky else 0.06,
                "mae20": -0.14 if risky else -0.025,
                "win20": not risky,
                "win40": not risky,
                "severe_loss20": risky,
            }
        )
    return pd.DataFrame(rows)


def _make_daily(symbol: str, periods: int, offset: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    rows = []
    for idx, day in enumerate(dates):
        close = 50.0 + offset + idx * 0.08 + (0.9 if idx % 8 == 0 else 0.0)
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": close - 0.2,
                "h": close + 0.8,
                "l": close - 0.7,
                "c": close,
                "v": 1000 + idx * 3,
                "source": "pan",
            }
        )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    daily = pd.concat([_make_daily(f"7{idx:03d}", 310, float(idx)) for idx in range(1, 8)], ignore_index=True)
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


def test_guard_rules_do_not_use_future_labels() -> None:
    audit = mod.build_feature_availability_audit(_opportunities())

    assert audit["used_future_labels_in_guard_rules"] is False
    assert audit["silent_fallback_used"] is False
    assert mod.SIGNAL_FEATURE_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)


def test_primary_guard_reduces_synthetic_severe_loss() -> None:
    baseline, rows = mod.evaluate_guards(_opportunities())
    primary = next(row for row in rows if row["guard_id"] == mod.PRIMARY_GUARD_ID)

    assert baseline["severe_loss_rate20"] > primary["kept"]["severe_loss_rate20"]
    assert primary["delta_vs_baseline"]["severe_loss_rate20"] < 0
    assert primary["decision"].startswith("keep_")


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    result = mod.run_teppan_loss_guard_v1(
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
    compare = json.loads((output_dir / "guard_compare.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["publish_bundle_created"] is False
    assert compare["primary_guard_id"] == mod.PRIMARY_GUARD_ID
