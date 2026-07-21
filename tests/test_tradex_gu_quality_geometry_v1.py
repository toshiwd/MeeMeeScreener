from __future__ import annotations

import pandas as pd

from scripts import tradex_gu_quality_geometry_v1 as m


def _frame() -> pd.DataFrame:
    return pd.DataFrame({
        "signal_ymd": [20240102, 20240102, 20240102], "code": ["1000", "2000", "3000"],
        "family_hit": [True, True, False], "gu_age": [2.0, 6.0, float("nan")],
        "pullback_depth_atr": [0.4, 0.8, float("nan")], "gu_anchor_ymd": [20231228.0, 20231220.0, float("nan")],
        "score": [102.0, 102.0, 5.0],
    })


def test_fast_shallow_geometry_is_exact() -> None:
    out = m.rank_variant(_frame(), "fast_shallow").sort_values("code")
    assert bool(out.iloc[0].quality_hit)
    assert not bool(out.iloc[1].quality_hit)
    assert not bool(out.iloc[2].quality_hit)


def test_mature_geometry_changes_priority_without_suppression() -> None:
    out = m.rank_variant(_frame(), "mature").sort_values("rank")
    assert len(out) == 3
    assert out.iloc[0].code == "2000"
    assert out["rank"].tolist() == [1, 2, 3]


def test_setup_initial_signal_is_tied_to_current_gu_anchor() -> None:
    frame = pd.concat([_frame(), _frame().assign(signal_ymd=20240103)], ignore_index=True)
    frame.loc[(frame.code == "1000") & (frame.signal_ymd == 20240103), "gu_anchor_ymd"] = 20240102.0
    out = m.rank_variant(frame, "fast_shallow").sort_values(["code", "signal_ymd"])
    code = out[out.code == "1000"]
    assert code.setup_initial_signal.tolist() == [True, True]
    assert code.setup_initial_signal_ymd.notna().all()


def test_variant_count_is_bounded_and_has_no_outcome_definition() -> None:
    assert 1 <= len(m.VARIANTS) <= 3
    assert all(set(spec) == {"age_min", "age_max", "depth_atr_max"} for spec in m.VARIANTS.values())
    out = m.rank_variant(_frame(), "balanced")
    assert "realized_mover20" not in out.columns
