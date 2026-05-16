from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_iizuka_signal_expectancy_v1 as mod


def _synthetic_rows(days: int = 36) -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range("2026-01-01", periods=days).strftime("%Y-%m-%d").tolist()
    for symbol_index, symbol in enumerate(["1001", "1002", "1003", "1004"]):
        sector = "tech" if symbol in {"1001", "1002"} else "machinery"
        for day in range(days):
            close = 100.0 + day * 0.2 + symbol_index
            open_ = close - 0.1
            high = close + 0.4
            low = close - 0.4
            ma7 = close - 0.4
            ma20 = close - 2.0
            atr14 = 1.0
            if symbol == "1001" and day in {8, 9, 10}:
                ma7 = close + 0.3
                ma20 = close - 2.0
            if symbol == "1001" and day == 11:
                open_ = 100.0
                close = 100.6
                high = 100.8
                low = 99.0
                ma7 = 100.45
                ma20 = 98.0
                atr14 = 1.0
            rows.append(
                {
                    "symbol": symbol,
                    "date": dates[day],
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "ma7": ma7,
                    "ma20": ma20,
                    "atr14": atr14,
                    "volume": 1000 + symbol_index,
                    "monthly_C_regime": True,
                    "sector": sector,
                }
            )
    return pd.DataFrame(rows)


def test_thresholds_or_subtype_and_setup_reclaim7() -> None:
    frame = mod._attach_signal_flags(mod._normalize_source_frame(_synthetic_rows()))
    signal = frame[(frame["symbol"] == "1001") & (frame["date"] == "2026-01-16")].iloc[0]

    assert bool(signal["lower_wick_flag"]) is True
    assert bool(signal["koma_flag"]) is False
    assert signal["ma7_below_setup_count"] == 3
    assert bool(signal["ma7_below_2_to_3_setup"]) is True
    assert bool(signal["reclaim7"]) is True
    assert bool(signal["main_signal"]) is True
    assert signal["signal_subtype"] == "lower_wick_reclaim7"


def test_normalize_accepts_yyyymmdd_dates() -> None:
    rows = _synthetic_rows(days=5)
    rows["date"] = pd.to_datetime(rows["date"]).dt.strftime("%Y%m%d").astype(int)

    normalized = mod._normalize_source_frame(rows)

    assert normalized["date"].iloc[0] == "2026-01-01"


def test_normalize_accepts_epoch_second_dates() -> None:
    rows = _synthetic_rows(days=5)
    rows["date"] = (pd.to_datetime(rows["date"]).astype("int64") // 1_000_000_000).astype(int)

    normalized = mod._normalize_source_frame(rows)

    assert normalized["date"].iloc[0] == "2026-01-01"


def test_return_mae_date_semantics() -> None:
    signal_rows = mod._build_signal_rows(mod._normalize_source_frame(_synthetic_rows()))
    row = signal_rows.iloc[0]

    assert row["observation_date"] == "2026-01-16"
    assert row["decision_date"] == "2026-01-16"
    assert row["execution_date"] == "2026-01-19"
    assert row["execution_price_source"] == "next_session_open"
    assert row["feature_window_start"] == "2026-01-13"
    assert row["feature_window_end"] == "2026-01-16"
    assert row["ret5_horizon_date"] == "2026-01-26"
    assert row["ret10_horizon_date"] == "2026-02-02"
    assert row["ret20_horizon_date"] == "2026-02-16"
    assert bool(row["no_lookahead_valid"]) is True

    source = mod._normalize_source_frame(_synthetic_rows())
    symbol_rows = source[source["symbol"] == "1001"].reset_index(drop=True)
    expected_ret5 = symbol_rows.loc[17, "close"] / symbol_rows.loc[12, "open"] - 1.0
    expected_mae = symbol_rows.loc[12:31, "low"].min() / symbol_rows.loc[12, "open"] - 1.0
    assert abs(row["ret5"] - expected_ret5) < 1e-12
    assert abs(row["mae20"] - expected_mae) < 1e-12


def test_signal_rows_exclude_missing_horizon_rows() -> None:
    rows = _synthetic_rows(days=20)

    signal_rows = mod._build_signal_rows(mod._normalize_source_frame(rows))

    assert len(signal_rows) == 0
    assert signal_rows.attrs["raw_signal_row_count"] == 1
    assert signal_rows.attrs["excluded_no_lookahead_invalid_count"] == 1


def test_deterministic_baseline_sampling() -> None:
    frame = mod._attach_forward_outcomes(mod._attach_signal_flags(mod._normalize_source_frame(_synthetic_rows())))
    signal_rows = mod._build_signal_rows(mod._normalize_source_frame(_synthetic_rows()))
    first = mod._sample_baselines(frame, signal_rows, seed=7, repetitions=3)
    second = mod._sample_baselines(frame, signal_rows, seed=7, repetitions=3)

    pd.testing.assert_frame_equal(first["baseline_2"].reset_index(drop=True), second["baseline_2"].reset_index(drop=True))
    assert set(first) == {"baseline_1", "baseline_2"}
    assert first["baseline_2"]["baseline_repetition"].nunique() == 3


def test_run_signal_expectancy_writes_required_artifacts(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    _synthetic_rows().to_parquet(source_path, index=False)

    result = mod.run_signal_expectancy(
        source_rows_parquet=source_path,
        output_root=tmp_path / "out",
        random_seed=11,
        baseline_repetitions=3,
    )

    session_root = Path(result["session_root"])
    assert result["signal_count"] == 1
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (session_root / artifact).exists(), artifact
    manifest = json.loads((session_root / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["boundary"] == "TRADEX-only"
    assert manifest["random_seed"] == 11
    assert manifest["research_fallback"] is True
    compare = json.loads((session_root / "iizuka_signal_expectancy_compare.json").read_text(encoding="utf-8"))
    assert compare["primary_baseline"] == "baseline_2"
    assert compare["baselines"]["baseline_2"]["repetitions"] == 3
    audit = json.loads((session_root / "iizuka_no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["pass"] is True
