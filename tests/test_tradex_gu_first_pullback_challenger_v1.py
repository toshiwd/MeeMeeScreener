from __future__ import annotations

import numpy as np
import pandas as pd

from scripts import tradex_gu_first_pullback_challenger_v1 as m


def _fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2024-01-02", periods=12).strftime("%Y%m%d").astype(int)
    features = pd.DataFrame({
        "signal_ymd": np.tile(dates, 2),
        "code": np.repeat(["1000", "2000"], len(dates)),
        "close": 103.0,
        "ma7": 101.0,
        "ma20": 100.0,
    })
    bars = pd.DataFrame({
        "signal_ymd": np.tile(dates, 2),
        "code": np.repeat(["1000", "2000"], len(dates)),
        "o": 100.0, "h": 104.0, "l": 99.0, "c": 100.0,
    })
    bars.loc[(bars.code == "1000") & (bars.signal_ymd == dates[2]), "o"] = 104.0
    return features, bars


def test_fixed_gu_family_excludes_current_gap_and_sets_first_transition() -> None:
    features, bars = _fixture()
    out = m.build_gu_scores(features, bars).sort_values(["code", "signal_ymd"])
    code = out[out.code == "1000"].reset_index(drop=True)
    assert not bool(code.loc[2, "family_hit"])
    assert bool(code.loc[3, "family_hit"])
    assert bool(code.loc[3, "initial_signal"])
    assert int(code.loc[3, "initial_signal_ymd"]) == int(code.loc[3, "signal_ymd"])
    assert not bool(code.loc[4, "initial_signal"])


def test_all_symbols_are_ranked_without_candidate_suppression() -> None:
    features, bars = _fixture()
    out = m.build_gu_scores(features, bars)
    counts = out.groupby("signal_ymd").size()
    assert counts.eq(2).all()
    assert out.groupby("signal_ymd")["rank"].max().eq(2).all()
    signal_day = sorted(out.signal_ymd.unique())[3]
    ranked = out[out.signal_ymd == signal_day].sort_values("rank")
    assert ranked.iloc[0].code == "1000"
    assert bool(ranked.iloc[0].family_hit)
    assert not bool(ranked.iloc[1].family_hit)


def test_score_is_outcome_free_and_family_has_priority() -> None:
    features, bars = _fixture()
    out = m.build_gu_scores(features, bars)
    assert "realized_mover20" not in out.columns
    assert out[out.family_hit].score.min() > out[~out.family_hit].score.max()
