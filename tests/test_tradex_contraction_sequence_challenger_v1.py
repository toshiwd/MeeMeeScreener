from __future__ import annotations

import numpy as np
import pandas as pd

from scripts import tradex_contraction_sequence_challenger_v1 as m


def _fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2024-01-02", periods=35).strftime("%Y%m%d").astype(int)
    rows_f = []; rows_b = []
    for code in ("1000", "2000"):
        for i, day in enumerate(dates):
            close = 100.0
            rows_f.append({"signal_ymd": day, "code": code, "vol_ratio5_20": 1.0, "range_pct": .01})
            rows_b.append({"signal_ymd": day, "code": code, "o": close, "h": 101.0, "l": 99.0, "c": close})
    f = pd.DataFrame(rows_f); b = pd.DataFrame(rows_b)
    f.loc[(f.code == "1000") & (f.signal_ymd == dates[11]), "vol_ratio5_20"] = .7
    f.loc[(f.code == "1000") & (f.signal_ymd == dates[21]), "range_pct"] = .02
    b.loc[(b.code == "1000") & (b.signal_ymd == dates[21]), ["h", "c"]] = [103.0, 102.0]
    return f, b


def test_sequence_is_pit_and_retains_first_stage_dates() -> None:
    f, b = _fixture(); out = m.build_scores(f, b, .04).sort_values(["code", "signal_ymd"])
    p = out[out.code == "1000"].reset_index(drop=True)
    assert bool(p.loc[10, "contraction_initial"])
    assert bool(p.loc[11, "dry_up_initial"])
    assert bool(p.loc[21, "breakout_initial"])
    assert int(p.loc[21, "contraction_ymd"]) == int(p.loc[10, "signal_ymd"])
    assert int(p.loc[21, "dry_up_ymd"]) == int(p.loc[11, "signal_ymd"])
    assert int(p.loc[21, "breakout_ymd"]) == int(p.loc[21, "signal_ymd"])


def test_breakout_requires_prior_high_and_prior_range_median() -> None:
    f, b = _fixture(); p = m.build_scores(f, b, .04); row = p[(p.code == "1000") & p.breakout_initial].iloc[0]
    assert row.c > row.prior20_high
    assert row.range_pct >= 1.2 * row.prior5_range_median


def test_all_symbols_ranked_daily_without_suppression() -> None:
    f, b = _fixture(); out = m.build_scores(f, b, .04)
    assert out.groupby("signal_ymd").size().eq(2).all()
    assert out.groupby("signal_ymd")["rank"].max().eq(2).all()
    day = out[out.breakout_initial].signal_ymd.iloc[0]
    assert out[out.signal_ymd == day].sort_values("rank").iloc[0].code == "1000"


def test_variant_width_changes_contraction_boundary() -> None:
    f, b = _fixture(); b.loc[b.code == "1000", "h"] = 103.0; b.loc[b.code == "1000", "l"] = 97.0
    narrow = m.build_scores(f, b, .04); wide = m.build_scores(f, b, .08)
    assert not narrow[narrow.code == "1000"].contraction_initial.any()
    assert wide[wide.code == "1000"].contraction_initial.any()
