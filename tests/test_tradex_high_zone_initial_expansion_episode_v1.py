from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_initial_expansion_episode_v1 import annotate_expansion, exposure


def _events():
    return pd.DataFrame([
        {"code":"1","signal_ymd":20240101,"o":100.0,"h":125.0,"l":95.0,"c":121.0,"o1":122.0,"dist_ma20":1.1,"ret20":1.0},
        {"code":"2","signal_ymd":20240101,"o":1000.0,"h":1410.0,"l":990.0,"c":1400.0,"o1":1420.0,"dist_ma20":1.1,"ret20":1.9},
        {"code":"3","signal_ymd":20240101,"o":2000.0,"h":2210.0,"l":1990.0,"c":2200.0,"o1":2220.0,"dist_ma20":1.1,"ret20":1.9},
    ])


def test_expansion_requires_two_axes_and_sub1500():
    rows=annotate_expansion(_events())
    assert rows.iloc[0].initial_expansion_episode
    assert rows.iloc[1].initial_expansion_episode
    assert not rows.iloc[2].initial_expansion_episode


def test_expansion_policy_keeps_entry_with_quarter_exposure():
    row=annotate_expansion(_events()).iloc[1]
    assert exposure(row,"initial_expansion25")==0.25


def test_non_expansion_remains_full_exposure():
    row=annotate_expansion(_events()).iloc[2]
    assert exposure(row,"initial_expansion25")==1.0
