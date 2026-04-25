from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_random_anchor_3m_policy_instability_diagnosis import run_policy_instability_diagnosis


STRESS200_DIR = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200")


def test_tradex_random_anchor_3m_policy_instability_diagnosis_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "policy_instability"
    payload = run_policy_instability_diagnosis(input_dir=STRESS200_DIR, output_dir=output_dir)

    assert payload["ok"] is True
    diagnosis = payload["diagnosis"]
    assert diagnosis["selection_only_edge_preserved"] is True
    assert diagnosis["policy_layer_destroyed_edge"] is True
    assert diagnosis["diagnosis_decision"] == "selection_keep_policy_hold"
    assert diagnosis["primary_failure_reason"] == "late_exit_on_long_side_in_top6_20"
    assert diagnosis["recommended_next_axis"] == "long_exit_timing_and_position_management"

    for key in (
        "diagnosis_json",
        "policy_vs_hold_gap_json",
        "policy_gap_by_anchor_json",
        "policy_gap_by_action_json",
        "policy_gap_by_side_json",
        "policy_gap_by_rank_bucket_json",
    ):
        path = Path(payload["paths"][key])
        assert path.exists(), key
        obj = json.loads(path.read_text(encoding="utf-8"))
        assert obj["generated_at"]

    action_rows = payload["policy_gap_by_action"]["rows"]
    assert action_rows[0]["action_category"] == "late_exit"
    assert action_rows[0]["realized_pnl_sum"] < 0

    side_rows = {row["side"]: row for row in payload["policy_gap_by_side"]["rows"]}
    assert "long" in side_rows
    assert "short" in side_rows
    assert side_rows["long"]["policy_vs_hold_gap_sum"] < 0

    rank_rows = {row["rank_bucket"]: row for row in payload["policy_gap_by_rank_bucket"]["rows"]}
    assert "top11_20" in rank_rows
    assert rank_rows["top11_20"]["policy_vs_hold_gap_sum"] < 0
