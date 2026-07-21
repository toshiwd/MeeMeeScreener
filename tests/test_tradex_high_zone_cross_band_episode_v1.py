from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_cross_band_episode_v1 import annotate, exposure


def _events():
    return pd.DataFrame([
        {"code":"1","signal_ymd":20240101,"c":800.0,"o1":820.0},
        {"code":"1","signal_ymd":20240120,"c":1200.0,"o1":1210.0},
        {"code":"2","signal_ymd":20240101,"c":1400.0,"o1":1700.0},
        {"code":"3","signal_ymd":20240101,"c":12000.0,"o1":12100.0},
    ])


def test_episode_persists_when_price_crosses_900():
    rows=annotate(_events()); row=rows[(rows.code=="1")&(rows.signal_ymd==20240120)].iloc[0]
    assert row.cross_from_under900 and row.cross_band_episode


def test_sub3000_large_gap_is_typed_episode():
    row=annotate(_events()).query("code=='2'").iloc[0]
    assert row.sub3000_gap20 and row.cross_band_episode


def test_cross_band_policy_keeps_participation_with_quarter_size():
    rows=annotate(_events())
    assert [exposure(r,"cross_band_episode25") for _,r in rows.iterrows()]==[1.0,0.25,0.25,0.25]
