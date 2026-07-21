import pandas as pd

from scripts.tradex_point_in_time_rolling_shape_blend_priority_top3_v1 import select
from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES


class _Model:
    def predict(self, frame):
        return frame[FEATURES[0]].to_numpy()


def test_fixed_half_blend_uses_both_priorities_without_suppression():
    rows = []
    for side in ("buy", "sell"):
        for rank in (1, 2, 3):
            row = {feature: 0.0 for feature in FEATURES}
            row.update({"signal_ymd": 20250106, "side": side, "rank": rank, "code": f"{side}{rank}", "split": "validation"})
            row[FEATURES[0]] = float(rank if side == "sell" else 4 - rank)
            rows.append(row)
    medians = {feature: 0.0 for feature in FEATURES}
    scored, selected = select(pd.DataFrame(rows), {"202501": {"model": _Model(), "medians": medians}})
    assert len(scored) == 6
    assert len(selected) == 3
    assert (scored.blend_priority_score == (scored.baseline_priority_percentile + scored.rolling_score_percentile) / 2).all()
    assert selected.global_rank.tolist() == [1, 2, 3]


def test_baseline_order_is_fixed_buy_first_interleave():
    rows = []
    for side in ("buy", "sell"):
        for rank in (1, 2, 3):
            row = {feature: 1.0 for feature in FEATURES}
            row.update({"signal_ymd": 20250106, "side": side, "rank": rank, "code": f"{side}{rank}", "split": "validation"})
            rows.append(row)
    scored, _ = select(pd.DataFrame(rows), {"202501": {"model": _Model(), "medians": {feature: 0.0 for feature in FEATURES}}})
    order = scored.sort_values("baseline_order").code.tolist()
    assert order == ["buy1", "sell1", "buy2", "sell2", "buy3", "sell3"]
