from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_three_stage_entry_v1 import _legs, price_band, strong_down_trigger


def _row(**changes):
    values = {"o":100.0,"h":105.0,"l":95.0,"c":102.0}
    for i in range(1,26): values.update({f"o{i}":101.0,f"h{i}":104.0,f"l{i}":97.0,f"c{i}":101.0,f"ma7{i}":100.0})
    values.update(changes); return pd.Series(values)


def test_strong_trigger_fills_at_five_percent_level():
    assert strong_down_trigger(_row(l2=90.0),1) == (2,96.89999999999999)


def test_three_stage_reaches_full_size_when_both_triggers_fire():
    legs,early,strong=_legs(_row(o1=108.0,l1=100.0,l2=90.0),"starter25_early25_strong50")
    assert early == 1 and strong == 2
    assert abs(sum(x[2] for x in legs)-1.0)<1e-12


def test_no_early_trigger_stays_at_starter():
    legs,early,strong=_legs(_row(),"starter25_early25_strong50")
    assert early is None and strong is None and len(legs)==1


def test_price_band_boundaries():
    assert price_band(899)=="under_900" and price_band(10000)=="10000_and_over"
