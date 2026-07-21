from __future__ import annotations

import pandas as pd

from scripts.tradex_adaptive_rule_router_v1 import apply_policy


def test_recent_guard_excludes_active_rule_with_deteriorating_recent_results() -> None:
    entry_date = pd.Timestamp("2026-07-10")
    candidates = pd.DataFrame(
        [
            {"entry_date": entry_date, "rule": "healthy", "code": "1111"},
            {"entry_date": entry_date, "rule": "stale", "code": "2222"},
        ]
    )
    snapshots = pd.DataFrame(
        [
            {"entry_date": entry_date, "rule": "healthy", "state": "Active", "permission_allowed": True, "score": 1.5, "n20": 20, "pf20": 1.4, "expectancy20": 0.01},
            {"entry_date": entry_date, "rule": "stale", "state": "Active", "permission_allowed": True, "score": 2.0, "n20": 20, "pf20": 0.6, "expectancy20": -0.01},
        ]
    )

    unguarded = apply_policy(candidates, snapshots, {"Active"}, 3)
    guarded = apply_policy(candidates, snapshots, {"Active"}, 3, recent_guard=True)

    assert set(unguarded.rule) == {"healthy", "stale"}
    assert guarded.rule.tolist() == ["healthy"]


def test_recent_guard_is_point_in_time_and_keeps_qualified_rule() -> None:
    entry_date = pd.Timestamp("2026-07-10")
    candidates = pd.DataFrame([{"entry_date": entry_date, "rule": "qualified", "code": "3333"}])
    snapshots = pd.DataFrame(
        [{"entry_date": entry_date, "rule": "qualified", "state": "Active", "permission_allowed": True, "score": 1.0, "n20": 15, "pf20": 1.0, "expectancy20": 0.001}]
    )

    result = apply_policy(candidates, snapshots, {"Active"}, 1, recent_guard=True)

    assert result.rule.tolist() == ["qualified"]
