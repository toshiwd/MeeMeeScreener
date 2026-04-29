from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from scripts.tradex_ma_position_path_research import (
    CLASSIFICATION_SCHEMA_VERSION,
    DECISION_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    MONTHLY_STABILITY_SCHEMA_VERSION,
    REGIME_SCHEMA_VERSION,
    SUMMARY_SCHEMA_VERSION,
    _classify_candle_tags,
    _path_threshold_hits,
    _compute_path_value_score,
    run_ma_position_path_research,
)


def _business_dates(start: str, periods: int) -> list[int]:
    dates = pd.bdate_range(start=start, periods=periods)
    return [int(ts.strftime("%Y%m%d")) for ts in dates]


def _build_temp_db(db_path: Path) -> None:
    dates = _business_dates("2024-01-02", 95)
    bars_rows: list[dict[str, object]] = []
    ma_rows: list[dict[str, object]] = []
    regime_rows: list[dict[str, object]] = []

    for idx, trade_date in enumerate(dates):
        regime_id = "flat" if idx < 26 else ("up" if idx < 52 else "down")
        regime_rows.append({"dt": trade_date, "regime_id": regime_id, "label_version": "v1"})
        for code, direction in (("AAA", 1.0), ("BBB", -1.0)):
            base = 100.0 if code == "AAA" else 180.0
            drift = direction * idx * 0.8
            close = base + drift
            open_price = close - direction * 0.25
            high = max(open_price, close) + 0.75
            low = min(open_price, close) - 0.75
            volume = 1000 + idx * 12 + (40 if code == "AAA" else 20)
            bars_rows.append(
                {
                    "code": code,
                    "date": trade_date,
                    "o": float(open_price),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "v": int(volume),
                    "source": "pan",
                }
            )

    bars_df = pd.DataFrame(bars_rows)
    bars_df.sort_values(["code", "date"], inplace=True)
    ma_df = bars_df[["code", "date", "c"]].copy()
    ma_df["ma7"] = ma_df.groupby("code", sort=False)["c"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    ma_df["ma20"] = ma_df.groupby("code", sort=False)["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    ma_df["ma60"] = ma_df.groupby("code", sort=False)["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    ma_df = ma_df.drop(columns=["c"])

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE daily_ma (
                code TEXT,
                date INTEGER,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE market_regime_daily (
                dt INTEGER,
                regime_id TEXT,
                label_version TEXT
            )
            """
        )
        conn.register("bars_df", bars_df)
        conn.register("ma_df", ma_df)
        conn.register("regime_df", pd.DataFrame(regime_rows))
        conn.execute("INSERT INTO daily_bars SELECT * FROM bars_df")
        conn.execute("INSERT INTO daily_ma SELECT * FROM ma_df")
        conn.execute("INSERT INTO market_regime_daily SELECT * FROM regime_df")
    finally:
        conn.close()


def test_path_threshold_hits_and_score_formula() -> None:
    entry = 100.0
    highs = np.array([100.5, 103.1, 104.0, 105.5, 106.0] + [106.0] * 15, dtype=np.float64)
    lows = np.array([99.8, 99.2, 96.9, 96.2, 94.9] + [94.9] * 15, dtype=np.float64)
    closes = np.array([100.2, 101.1, 102.0, 103.0, 99.5] + [99.5] * 15, dtype=np.float64)
    metrics = _path_threshold_hits(entry, highs, lows, closes, atr14=2.0)
    assert metrics["days_to_plus_3pct"] == 2
    assert metrics["days_to_plus_5pct"] == 4
    assert metrics["days_to_minus_3pct"] == 3
    assert metrics["days_to_minus_5pct"] == 5
    assert metrics["hit_plus_3_before_minus_3"] == 1
    assert metrics["hit_plus_5_before_minus_5"] == 1
    assert metrics["hit_minus_5_before_plus_5"] == 0
    assert metrics["hit_plus_1atr_before_minus_1atr"] == 1
    assert metrics["close_above_entry_days_20d"] == 4
    assert metrics["close_below_entry_days_20d"] == 16
    assert round(float(metrics["mfe_20d"]), 4) == 0.0600
    assert round(float(metrics["mae_20d"]), 4) == -0.0510
    assert round(float(metrics["mfe_atr_20d"]), 4) == 3.0000
    assert round(float(metrics["mae_atr_20d"]), 4) == -2.5500

    row = pd.Series(
        {
            "forward_ret_20d": 0.12,
            "mfe_20d": 0.18,
            "mae_20d": -0.05,
            "forward_ret_10d": 0.08,
            "close_above_entry_days_20d": 14,
            "hit_plus_5_before_minus_5": 1,
            "hit_minus_5_before_plus_5": 0,
        }
    )
    score = _compute_path_value_score(row)
    assert score is not None
    assert round(score, 6) == round(
        0.30 * 0.12 + 0.25 * 0.18 - 0.30 * 0.05 + 0.10 * 0.08 + 0.05 * (14 / 20.0) + 0.10 * 1.0,
        6,
    )

    primary, tags, details = _classify_candle_tags(
        open_price=100.0,
        high_price=106.0,
        low_price=99.0,
        close_price=105.5,
        atr14=2.0,
        prev_close=98.5,
        prev_high=99.5,
        prev_low=97.5,
    )
    assert primary == "bullish_body"
    assert "large_bullish_body" in tags
    assert "gap_up" in tags
    assert details["body_norm"] is not None


def test_tradex_ma_position_path_research_writes_summary_and_parquet_artifacts(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    out_dir = tmp_path / "research"
    _build_temp_db(db_path)

    result = run_ma_position_path_research(db_path=str(db_path), output_dir=str(out_dir), detail_limit=5)
    session_dir = Path(result["session_dir"])
    summary_path = Path(result["summary_path"])
    by_regime_path = Path(result["by_regime_path"])
    monthly_path = Path(result["monthly_stability_path"])
    classification_path = Path(result["classification_path"])
    decision_path = Path(result["decision_path"])
    manifest_path = Path(result["manifest_path"])
    detail_path = Path(result["detail_path"])

    assert session_dir.exists()
    assert summary_path.exists()
    assert by_regime_path.exists()
    assert monthly_path.exists()
    assert classification_path.exists()
    assert decision_path.exists()
    assert manifest_path.exists()
    assert detail_path.exists()

    summary = result["summary"]
    by_regime = result["by_regime"]
    monthly = result["monthly_stability"]
    classification = result["classification"]
    decision = result["decision"]
    manifest = result["manifest"]
    saved_summary = summary_path.read_text(encoding="utf-8")
    saved_by_regime = by_regime_path.read_text(encoding="utf-8")
    saved_monthly = monthly_path.read_text(encoding="utf-8")
    saved_classification = classification_path.read_text(encoding="utf-8")
    saved_decision = decision_path.read_text(encoding="utf-8")
    saved_manifest = manifest_path.read_text(encoding="utf-8")

    assert SUMMARY_SCHEMA_VERSION in saved_summary
    assert REGIME_SCHEMA_VERSION in saved_by_regime
    assert MONTHLY_STABILITY_SCHEMA_VERSION in saved_monthly
    assert CLASSIFICATION_SCHEMA_VERSION in saved_classification
    assert DECISION_SCHEMA_VERSION in saved_decision
    assert MANIFEST_SCHEMA_VERSION in saved_manifest
    assert summary["study_status"] == "confirmed"
    assert summary["overall_metrics"]["eligible_row_count"] > 0
    assert summary["overall_metrics"]["confirmed_regime_row_count"] > 0
    assert summary["state_counts"]["total_state_count"] > 0
    assert set(summary["top_state_lists"].keys()) == {
        "high_value_states",
        "weak_noise_states",
        "bad_pick_removal_states",
        "regime_dependent_states",
    }
    assert isinstance(summary["top_state_lists"]["high_value_states"], list)
    assert isinstance(summary["top_state_lists"]["bad_pick_removal_states"], list)
    assert by_regime["regime_state_row_count"] >= 0
    assert isinstance(by_regime["regime_state_summary"], list)
    assert monthly["monthly_state_row_count"] > 0
    assert classification["state_quality_counts"]
    assert decision["recommendation"] in {"hold", "keep", "drop"}
    assert manifest["no_lookahead_check"]["passed"] is True
    assert manifest["output_rows_count"] > 0

    parquet_frame = pd.read_parquet(detail_path)
    assert not parquet_frame.empty
    assert {"code", "trade_date", "position_state_id", "regime_label", "regime_source", "forward_ret_20d", "path_value_score_v1"}.issubset(parquet_frame.columns)


def test_tradex_ma_position_path_research_supports_symbol_limit(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    out_dir = tmp_path / "research_limited"
    _build_temp_db(db_path)

    result = run_ma_position_path_research(db_path=str(db_path), output_dir=str(out_dir), detail_limit=3, limit_symbols=1)
    manifest = result["manifest"]
    parquet_frame = pd.read_parquet(Path(result["detail_path"]))

    assert manifest["run_mode"] == "smoke"
    assert manifest["limit_symbols"] == 1
    assert parquet_frame["code"].nunique() == 1
