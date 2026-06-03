from __future__ import annotations

import json

import pandas as pd

from scripts.tradex_entry_actionability_and_avoidance_surface_v1 import FEATURES, run


def _row(as_of: int, code: str, *, winner: bool, bad: bool, risky: bool) -> dict:
    row = {name: 0.01 for name in FEATURES}
    row.update({
        "as_of_date": as_of,
        "code": code,
        "winner_ret20_gt_10pct": winner,
        "bad_ret20_lt_minus_5pct": bad,
        "severe_ret20_lt_minus_10pct": bad,
        "ret20": -0.1 if bad else (0.2 if winner else 0.01),
        "weekly_supportive_flag": not risky,
        "monthly_supportive_flag": not risky,
        "recent_high_distance_pct": -0.1,
        "volume_vs_20d_avg": 1.0,
    })
    if risky:
        row["close_vs_ma20_pct"] = 0.15
        row["recent_high_distance_pct"] = 0.0
        row["volume_vs_20d_avg"] = 0.2
    return row


def test_surface_exposes_probabilities_and_explicit_avoid_reasons(tmp_path):
    rows = []
    for index in range(20):
        rows.append(_row(20230101 + index, f"T{index}", winner=index % 2 == 0, bad=index % 2 == 1, risky=index % 2 == 1))
    rows.extend([
        _row(20240101, "E1", winner=False, bad=True, risky=True),
        _row(20240102, "E2", winner=True, bad=False, risky=False),
    ])
    source = tmp_path / "source.parquet"
    pd.DataFrame(rows).to_parquet(source, index=False)

    output = run(source_path=source, output_root=tmp_path / "out")
    surface = pd.read_parquet(output / "entry_actionability_and_avoidance_surface.parquet")
    metrics = json.loads((output / "surface_metrics.json").read_text())
    decision = json.loads((output / "research_decision.json").read_text())

    assert set(surface["avoid_level"]) == {"avoid", "none"}
    assert "low_volume_participation" in json.loads(surface.loc[surface["code"] == "E1", "avoid_reason_codes"].item())
    assert surface["event_risk_contract_status"].eq("unavailable_not_silently_fallbacked").all()
    assert metrics["split_contract"]["point_in_time_features_only"] is True
    assert decision["decision_class"] == "READY_REVIEW_ONLY"
    assert decision["runtime_db_write"] is False
