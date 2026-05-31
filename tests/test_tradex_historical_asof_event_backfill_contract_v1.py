from __future__ import annotations

from datetime import date

import pandas as pd

from scripts import tradex_historical_asof_event_backfill_contract_v1 as mod


def test_selected_snapshot_uses_latest_not_after_anchor() -> None:
    snaps = [date(2026, 1, 1), date(2026, 2, 1)]
    assert mod.selected_snapshot(date(2026, 1, 15), snaps) == date(2026, 1, 1)
    assert mod.selected_snapshot(date(2025, 12, 31), snaps) is None


def test_build_event_rows_respects_asof_snapshot() -> None:
    source = pd.DataFrame(
        [
            {"as_of_date": 20260115, "code": "1001", "ret20": 0.01, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
            {"as_of_date": 20260131, "code": "1001", "ret20": 0.01, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
        ]
    )
    snapshots = {
        date(2026, 1, 1): {
            "earnings": pd.DataFrame([{"code": "1001", "planned_date": date(2026, 1, 20)}]),
            "rights": pd.DataFrame([{"code": "1001", "last_rights_date": date(2026, 1, 16)}]),
        },
        date(2026, 2, 1): {
            "earnings": pd.DataFrame([{"code": "1001", "planned_date": date(2026, 2, 10)}]),
            "rights": pd.DataFrame(),
        },
    }
    rows = mod.build_event_rows(source, snapshots)
    assert rows.loc[0, "days_to_next_earnings"] == 5
    assert rows.loc[1, "days_to_next_earnings"] == -11
    assert rows.loc[0, "ex_rights_nearby_flag"] is True or bool(rows.loc[0, "ex_rights_nearby_flag"])


def test_decide_undercovered_when_recent_snapshot_only() -> None:
    decision, cls, reasons = mod.decide({"snapshot_selected_rate": 0.1, "earnings_feature_available_rate": 0.1}, True)
    assert decision == "historical_event_backfill_created_but_undercovered"
    assert cls == "HOLD_UNDERPOWERED"
    assert reasons


def test_feature_contract_marks_outcomes_offline() -> None:
    contract = mod.feature_contract()
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["earnings_nearby_flag"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["ret20_derived_tags"]["classification"] == "forbidden_future_leak"
