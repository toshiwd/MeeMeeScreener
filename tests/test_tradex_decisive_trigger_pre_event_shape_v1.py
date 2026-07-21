from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from scripts import tradex_decisive_trigger_pre_event_shape_v1 as target


def test_shape_features_are_pit_and_boolean() -> None:
    rows = []
    for index in range(50):
        close = 100 + index * 0.4
        trade_date = date(2024, 1, 1) + timedelta(days=index)
        rows.append({"code": "1", "trade_date": trade_date.isoformat(), "o": close - 0.2, "h": close + 0.5, "l": close - 0.5, "c": close, "v": 1000})
    frame = target.add_shape_features(pd.DataFrame(rows))
    assert list(frame.columns) == ["code", "trade_date", *target.SHAPE_COLUMNS]
    assert all(frame[column].dtype == bool for column in target.SHAPE_COLUMNS)


def test_blind_sample_excludes_outcomes_and_balances_classes() -> None:
    rows = []
    for event_type in ["BUY_DECISIVE_INITIAL", "BUY_DECISIVE_CONTINUATION", "SELL_DECISIVE_RETURN_SELL"]:
        for index, value in enumerate([0.1, 0.2, -0.1, -0.2]):
            row = {
                "code": f"{1000 + index}", "trade_date": f"2024-01-{index + 1:02d}", "event_type": event_type,
                "outcome_complete20": True, "directional_ret20": value, "directional_adverse20": -0.05,
            }
            row.update({column: False for column in target.SHAPE_COLUMNS})
            rows.append(row)
    review, sealed = target.build_blind_sample(pd.DataFrame(rows), per_class=2)
    assert len(review) == 12
    assert len(sealed) == 12
    assert all("directional_ret20" not in row and "outcome_class" not in row for row in review)
    counts = pd.Series([row["outcome_class"] for row in sealed]).value_counts().to_dict()
    assert counts == {"success": 6, "failure": 6}


def test_enrich_events_preserves_cardinality() -> None:
    events = pd.DataFrame([{"code": "1", "trade_date": "2024-01-01", "event_type": "BUY_DECISIVE_INITIAL"}])
    shapes = pd.DataFrame([{"code": "1", "trade_date": "2024-01-01", **{column: False for column in target.SHAPE_COLUMNS}}])
    enriched, quality = target.enrich_events(events, shapes)
    assert len(enriched) == 1
    assert quality["join_row_multiplier"] == 1.0
    assert quality["duplicate_event_keys_after_join"] == 0
