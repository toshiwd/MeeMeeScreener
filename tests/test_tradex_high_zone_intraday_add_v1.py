from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_intraday_add_v1 import intraday_trigger, replay


def _row(**changes):
    values = {"family": "high_zone_climax", "code": "1", "signal_ymd": 20240101, "o": 100.0, "h": 105.0, "l": 95.0, "c": 102.0}
    for i in range(1, 26): values.update({f"o{i}": 102.0, f"h{i}": 104.0, f"l{i}": 97.0, f"c{i}": 101.0})
    values.update(changes); return pd.Series(values)


def test_signal_low_trigger_fills_at_crossed_level():
    assert intraday_trigger(_row(l1=94.0), "signal_low") == (1, 95.0)


def test_gap_through_fills_at_open():
    assert intraday_trigger(_row(o1=93.0, l1=90.0), "signal_low") == (1, 93.0)


def test_gu_failure_uses_prior_close_before_signal_low():
    assert intraday_trigger(_row(o1=108.0, l1=100.0), "early_failure") == (1, 102.0)


def test_all_policies_keep_starter_participation():
    ledger = replay(pd.DataFrame([_row()]))
    assert len(ledger) == 6
    assert set(ledger.state) == {"entry"}
