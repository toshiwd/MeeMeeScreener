from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_iizuka_signal_expectancy_phase1c_v1 as phase1c


def _synthetic_rows(days: int = 45) -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range("2026-01-01", periods=days).strftime("%Y-%m-%d").tolist()
    for symbol_index, symbol in enumerate(["1001", "1002", "1003", "1004", "1005"]):
        for day in range(days):
            close = 100.0 + day * 0.25 + symbol_index
            open_ = close - 0.1
            high = close + 0.4
            low = close - 0.4
            ma7 = close - 0.4
            ma20 = close - 2.0
            atr14 = 1.0
            if day in {8, 9, 10} and symbol in {"1001", "1002", "1003", "1004"}:
                ma7 = close + 0.3
            if day == 11 and symbol in {"1001", "1002", "1003", "1004"}:
                ma7 = close - 0.1
                if symbol == "1001":
                    open_, close, high, low = 100.0, 100.6, 100.8, 99.0
                elif symbol == "1002":
                    open_, close, high, low = 100.0, 100.1, 100.4, 99.8
                elif symbol == "1003":
                    open_, close, high, low = 100.0, 100.05, 100.2, 99.9
                else:
                    open_, close, high, low = 100.0, 100.4, 100.6, 99.2
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
                    "sector": "tech" if symbol_index < 3 else "machinery",
                    "liquidity_bucket": "liquidity_high" if symbol_index < 2 else "liquidity_mid",
                }
            )
    return pd.DataFrame(rows)


def test_combination_assignment() -> None:
    assert phase1c._combination(pd.Series({"lower_wick_flag": True, "koma_flag": True, "horizontal_flag": False})) == "lower_wick+koma"
    assert phase1c._combination(pd.Series({"lower_wick_flag": True, "koma_flag": False, "horizontal_flag": True})) == "lower_wick+horizontal"
    assert phase1c._combination(pd.Series({"lower_wick_flag": False, "koma_flag": True, "horizontal_flag": True})) == "koma+horizontal"
    assert phase1c._combination(pd.Series({"lower_wick_flag": True, "koma_flag": True, "horizontal_flag": True})) == "lower_wick+koma+horizontal"
    assert phase1c._combination(pd.Series({"lower_wick_flag": True, "koma_flag": False, "horizontal_flag": False})) is None


def test_run_phase1c_writes_required_artifacts(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    _synthetic_rows().to_parquet(source_path, index=False)

    result = phase1c.run_phase1c(
        source_rows_parquet=source_path,
        output_root=tmp_path / "out",
        random_seed=17,
        baseline_repetitions=5,
    )

    root = Path(result["session_root"])
    for artifact in phase1c.REQUIRED_ARTIFACTS:
        assert (root / artifact).exists(), artifact
    manifest = json.loads((root / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["signal_definition_changed"] is False
    assert manifest["thresholds_changed"] is False
    assert manifest["baseline_repeat_count"] == 5
    assert manifest["research_fallback"] is True
    complete = json.loads((root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["all_present"] is True
    internal = json.loads((root / "phase1c_internal_combination_compare.json").read_text(encoding="utf-8"))
    assert set(internal["combinations"]) == set(phase1c.COMBINATIONS)
    decision = json.loads((root / "phase1c_signal_decision.json").read_text(encoding="utf-8"))
    assert decision["candidate_id"] == phase1c.CANDIDATE_ID
    assert decision["authoritative_rollup_decision"] in {"keep", "hold", "drop", "analysis_only", "blocked"}
