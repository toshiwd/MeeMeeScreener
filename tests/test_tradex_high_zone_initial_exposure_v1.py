from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_initial_exposure_v1 import continuation_risk, exposure, price_band


def _row(**changes):
    values={"c":2000.0,"ret20":1.0,"dist_ma20":0.1,"o1":2020.0}; values.update(changes); return pd.Series(values)


def test_high_price_keeps_starter_instead_of_exclusion():
    assert exposure(_row(c=12000.0),"high_price25")==0.25


def test_tail_price_tier_reduces_both_extremes():
    assert exposure(_row(c=800.0),"tail_price_tier")==0.75
    assert exposure(_row(c=12000.0),"tail_price_tier")==0.25


def test_continuation_risk_requires_two_conditions():
    assert continuation_risk(_row(ret20=1.3,dist_ma20=0.25))["high"] is True
    assert continuation_risk(_row(ret20=1.3))["high"] is False


def test_price_band_boundaries():
    assert price_band(900)=="900_to_3000" and price_band(10000)=="10000_and_over"
