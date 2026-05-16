from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts import tradex_pine_fire_signal_research_v1 as mod


def _synthetic_daily() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-01", periods=280)
    rows = []
    for idx, date in enumerate(dates):
        close = 100.0 + idx * 0.25
        open_ = close - 0.1
        high = close + 0.4
        low = close - 0.4
        volume = 1000.0
        if idx in {235, 236}:
            close -= 2.0
            open_ = close + 0.1
            high = open_ + 0.2
            low = close - 0.4
        if idx == 237:
            open_ = 158.0
            close = 162.0
            high = 162.2
            low = 157.8
            volume = 5000.0
        rows.append(
            {
                "symbol": "1001",
                "date": date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "source": "pan",
                "sector33_name": "machinery",
                "market_code": "TSE",
            }
        )
    return pd.DataFrame(rows)


def test_build_pine_features_finds_first_fire_signal() -> None:
    features = mod.build_pine_features(_synthetic_daily())
    signal = features.loc[features["fire_ab_first"]]

    assert not signal.empty
    row = signal.iloc[0]
    assert int(row["long_score"]) >= 5
    assert bool(row["valid_forward20"]) is True
    assert row["execution_date"] > row["date"]


def test_topk_rows_are_stable_per_date() -> None:
    daily = pd.concat([_synthetic_daily(), _synthetic_daily().assign(symbol="1002", sector33_name="tech")], ignore_index=True)
    features = mod.build_pine_features(daily)
    topk = mod._topk_rows(features, top_k=1)

    assert topk.groupby("date")["pine_rank"].max().le(1).all()
    assert topk["valid_forward20"].all()


def test_run_research_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    daily = pd.concat([_synthetic_daily(), _synthetic_daily().assign(symbol="1002", sector33_name="tech")], ignore_index=True)

    monkeypatch.setattr(mod, "load_daily_bars", lambda *_args, **_kwargs: daily)
    result = mod.run_research(
        source_db_path=tmp_path / "unused.duckdb",
        output_root=tmp_path / "out",
        start="2025-01-01",
        end=None,
        source="pan",
        random_seed=7,
        baseline_repetitions=3,
        top_k=1,
    )

    session_root = Path(result["session_root"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (session_root / artifact).exists(), artifact
