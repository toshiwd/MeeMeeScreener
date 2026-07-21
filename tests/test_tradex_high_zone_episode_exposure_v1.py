from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_episode_exposure_v1 import annotate_episode, exposure


def _events():
    return pd.DataFrame([
        {"code":"1","signal_ymd":20240101,"c":500.0,"o1":510.0},
        {"code":"1","signal_ymd":20240120,"c":600.0,"o1":610.0},
        {"code":"2","signal_ymd":20240101,"c":500.0,"o1":600.0},
        {"code":"3","signal_ymd":20240101,"c":12000.0,"o1":12100.0},
    ])


def test_episode_combines_repeat_and_large_gap():
    rows=annotate_episode(_events())
    assert rows.loc[(rows.code=="1") & (rows.signal_ymd==20240120),"low_price_episode"].iloc[0]
    assert rows.loc[rows.code=="2","low_price_episode"].iloc[0]


def test_combined_policy_keeps_all_names_with_typed_exposure():
    rows=annotate_episode(_events())
    weights=[exposure(row,"combined_episode25") for _,row in rows.iterrows()]
    assert weights==[1.0,0.25,0.25,0.25]


def test_non_episode_low_price_remains_full_size():
    row=annotate_episode(_events()).iloc[0]
    assert exposure(row,"combined_episode25")==1.0
