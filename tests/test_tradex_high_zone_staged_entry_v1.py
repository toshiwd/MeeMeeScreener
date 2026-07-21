from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_staged_entry_v1 import confirmation_offset, replay


def _row(**changes):
    values = {"family": "high_zone_climax", "code": "1", "signal_ymd": 20240101, "o": 100.0, "h": 105.0, "l": 95.0, "c": 102.0}
    for i in range(1, 26): values.update({f"o{i}": 102.0, f"h{i}": 103.0, f"l{i}": 97.0, f"c{i}": 100.0})
    values.update(changes)
    return pd.Series(values)


def test_direct_signal_low_break_confirms():
    assert confirmation_offset(_row(c1=94.0)) == 1


def test_structured_weakness_confirms_without_signal_low_break():
    assert confirmation_offset(_row(o1=103.0, h1=104.0, l1=96.0, c1=98.0)) == 1


def test_starter_keeps_every_available_signal():
    ledger = replay(pd.DataFrame([_row()]))
    assert set(ledger.state) == {"entry"}
    assert len(ledger) == 3


def test_staged_return_uses_weighted_legs():
    ledger = replay(pd.DataFrame([_row(o1=100.0, c1=94.0, c6=90.0)]))
    row = ledger[ledger.policy == "starter25_confirm75"].iloc[0]
    expected = 0.25 * (1 - 90 / 100) + 0.75 * (1 - 90 / 94)
    assert abs(row.ret5 - expected) < 1e-12
