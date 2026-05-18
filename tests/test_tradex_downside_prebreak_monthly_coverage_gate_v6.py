from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_downside_prebreak_monthly_coverage_gate_v6 as mod


def _write_source(root: Path, rows: list[dict[str, object]]) -> None:
    child = root / "rank_limit_20"
    child.mkdir(parents=True)
    pd.DataFrame(rows).to_csv(child / "downside_prebreak_narrow_universe_monthly_rankings.csv", index=False)


def test_monthly_coverage_gate_holds_when_too_few_months(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_source(
        source,
        [
            {"month": 202501, "skipped": False, "selected_candidate_count": 20, "closed_horizon_candidate_count": 20, "unknown_candidate_count": 0, "out_of_narrow_universe_count": 30, "baseline_top_hit_rate": 0.4, "challenger_top_hit_rate": 0.6, "baseline_top_mean_ret20": 0.01, "challenger_top_mean_ret20": 0.02, "changed_top5_members_count": 4, "changed_rank_count": 1},
            {"month": 202502, "skipped": True, "skip_reason": "insufficient_closed_horizon_rows", "selected_candidate_count": 0, "closed_horizon_candidate_count": 4, "unknown_candidate_count": 20, "out_of_narrow_universe_count": 0, "changed_top5_members_count": 0, "changed_rank_count": 0},
        ],
    )

    out = tmp_path / "out"
    result = mod.run(source_root=source, output_dir=out, source_rank_limit=20)
    decision = json.loads((out / "downside_prebreak_monthly_coverage_decision.json").read_text(encoding="utf-8"))

    assert result["decision"] == "hold_due_to_thin_closed_horizon_month_coverage"
    assert decision["summary"]["evaluated_month_count"] == 1
    assert (out / "_ARTIFACT_COMPLETE.json").exists()


def test_monthly_coverage_gate_keeps_when_enough_months(tmp_path: Path) -> None:
    source = tmp_path / "source"
    rows = []
    for idx in range(6):
        rows.append(
            {
                "month": 202501 + idx,
                "skipped": False,
                "selected_candidate_count": 20,
                "closed_horizon_candidate_count": 20,
                "unknown_candidate_count": 0,
                "out_of_narrow_universe_count": 30,
                "baseline_top_hit_rate": 0.4,
                "challenger_top_hit_rate": 0.6,
                "baseline_top_mean_ret20": 0.01,
                "challenger_top_mean_ret20": 0.02,
                "changed_top5_members_count": 4,
                "changed_rank_count": 1,
            }
        )
    _write_source(source, rows)

    result = mod.run(source_root=source, output_dir=tmp_path / "out", source_rank_limit=20)

    assert result["decision"] == "keep_for_shadow_paper_replay_monthly_gate"
