from __future__ import annotations

import pandas as pd

from scripts import tradex_gu_regime_adaptation_v1 as m


def _scores() -> pd.DataFrame:
    return pd.DataFrame({
        "signal_ymd": [20240102, 20240102, 20240103, 20240103],
        "code": ["1000", "2000", "1000", "2000"],
        "family_hit": [True, False, True, False],
        "score": [102.0, 5.0, 102.0, 5.0],
    })


def test_threshold_uses_2024_regime_median_without_outcomes() -> None:
    regime = pd.DataFrame({
        "signal_ymd": [20240102, 20240103, 20250102],
        "breadth_above_ma20": [0.4, 0.6, 0.99],
    })
    out, threshold = m.apply_regime_priority(_scores(), regime)
    assert threshold == 0.5
    assert "realized_mover20" not in out.columns


def test_nonfit_day_still_ranks_all_symbols_and_removes_gu_bonus() -> None:
    regime = pd.DataFrame({
        "signal_ymd": [20240102, 20240103],
        "breadth_above_ma20": [0.4, 0.6],
    })
    out, _ = m.apply_regime_priority(_scores(), regime)
    nonfit = out[out.signal_ymd == 20240102].sort_values("rank")
    assert len(nonfit) == 2
    assert nonfit["rank"].tolist() == [1, 2]
    assert nonfit.iloc[0].code == "2000"
    fit = out[out.signal_ymd == 20240103].sort_values("rank")
    assert fit.iloc[0].code == "1000"


def test_rank_coverage_has_no_candidate_suppression() -> None:
    regime = pd.DataFrame({
        "signal_ymd": [20240102, 20240103],
        "breadth_above_ma20": [0.4, 0.6],
    })
    out, _ = m.apply_regime_priority(_scores(), regime)
    assert out.groupby("signal_ymd").size().eq(2).all()
    assert out.groupby("signal_ymd")["rank"].max().eq(2).all()
