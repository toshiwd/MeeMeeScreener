from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_bad_pick_removal_v1_decomposition import (
    DEFAULT_INPUT_RUN_ROOT,
    run_decomposition,
)


def test_bad_pick_removal_v1_decomposition_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_decomposition(
        input_run_root=DEFAULT_INPUT_RUN_ROOT,
        output_root=tmp_path / "bad_pick_removal_v1_decomposition",
    )

    session_dir = Path(result["session_dir"])
    required = (
        "run_config.json",
        "swap_decomposition_top5.json",
        "swap_decomposition_top10.json",
        "false_veto_examples.json",
        "weak_replacement_examples.json",
        "monthly_swap_summary.json",
        "decomposition_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for name in required:
        assert (session_dir / name).exists()

    decision = json.loads((session_dir / "decomposition_decision.json").read_text(encoding="utf-8"))
    complete = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    top5 = json.loads((session_dir / "swap_decomposition_top5.json").read_text(encoding="utf-8"))

    assert decision["primary_failure_mode"] in {
        "false_veto",
        "weak_replacement",
        "over_broad_penalty",
        "boundary_mismatch",
        "concentration",
        "missing_input_coverage",
        "insufficient_signal",
    }
    assert decision["recommended_next_axis_only"] in {
        "replacement_quality_gate_v1",
        "breakout_trap_narrowing_v1",
        "input_coverage_repair",
        "drop_bad_pick_removal_v1",
    }
    assert isinstance(decision["is_breakout_trap_signal_salvageable"], bool)
    assert top5["top_k"] == 5
    assert complete["verification"]["runtime_db_write_occurred"] is False
    assert complete["verification"]["previous_artifacts_read_only"] is True
