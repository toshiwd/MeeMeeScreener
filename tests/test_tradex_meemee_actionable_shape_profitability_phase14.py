from __future__ import annotations

import numpy as np

from scripts import tradex_meemee_actionable_shape_profitability_phase14 as mod


def test_compare_reports_favorable_lift() -> None:
    rows = [
        ({"ret20": -0.1}, np.zeros(1), 0),
        ({"ret20": 0.0}, np.zeros(1), 0),
        ({"ret20": 0.1}, np.zeros(1), 1),
        ({"ret20": 0.2}, np.zeros(1), 1),
    ]
    compare = mod._compare(rows, np.asarray([0.1, 0.2, 0.8, 0.9]))
    assert compare["predicted_favorable"]["count"] == 2
    assert compare["delta"]["ret20_mean"] > 0
    assert compare["delta"]["bad_ret20_lt_minus_5pct_rate"] < 0
