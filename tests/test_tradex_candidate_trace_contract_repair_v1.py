from __future__ import annotations

import pandas as pd

from scripts import portfolio_agent_replay_v1 as replay
from scripts.tradex_candidate_trace_contract_repair_v1 import _ranking_invariance


def test_candidate_trace_schema_declares_sidecar_without_ranking_change() -> None:
    schema = replay._candidate_trace_schema()

    assert schema["schema_version"] == "candidate_trace_schema_v1"
    assert schema["rules"]["ranking_order_changed"] is False
    assert "daily_candidate_trace.csv" in replay.REQUIRED_ARTIFACTS


def test_candidate_trace_row_records_missing_semantics() -> None:
    row = pd.Series(
        {
            "code": "1001",
            "entry_allowed_by_score": True,
            "downside_guard_blocked": False,
            "score_components_json": '[{"feature":"daily_ma_stack","points":3}]',
        }
    )

    trace = replay._candidate_trace_row(output_dir=__import__("pathlib").Path("x"), run_id="run", ymd=20240104, row=row, rank=1, score=12, next_open_available=True)

    assert trace["candidate_source"] is None
    assert trace["reason_codes_json"] == "[]"
    assert trace["score_component_attribution_available"] is True


def test_ranking_invariance_detects_matching_snapshot_and_trace(tmp_path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    pd.DataFrame([{"decision_ymd": 20240104, "code": "1001", "candidate_rank": 1}]).to_csv(run / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame([{"decision_date": 20240104, "code": "1001", "baseline_rank": 1}]).to_csv(run / "daily_candidate_trace.csv", index=False)

    report = _ranking_invariance([run])

    assert report["changed_rank_count"] == 0
    assert report["changed_top5_members_count"] == 0
