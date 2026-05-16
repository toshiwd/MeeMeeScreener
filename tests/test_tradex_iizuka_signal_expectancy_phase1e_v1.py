from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_iizuka_signal_expectancy_phase1e_v1 as phase1e


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _phase_roots(tmp_path: Path) -> tuple[Path, Path, Path]:
    phase1c = tmp_path / "phase1c"
    phase1d = tmp_path / "phase1d"
    surface_dir = tmp_path / "surface"
    phase1c.mkdir(parents=True)
    phase1d.mkdir(parents=True)
    surface_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "symbol": "1001",
                "decision_date": "2026-01-10",
                "execution_date": "2026-01-13",
                "mixed_internal_combination": "koma+horizontal",
            },
            {
                "symbol": "1002",
                "decision_date": "2026-01-10",
                "execution_date": "2026-01-13",
                "mixed_internal_combination": "lower_wick+horizontal",
            },
        ]
    ).to_parquet(phase1c / "phase1c_mixed_signal_rows.parquet", index=False)
    _write_json(phase1c / "phase1c_signal_decision.json", {"authoritative_rollup_decision": "keep"})
    _write_json(phase1d / "phase1d_decision.json", {"authoritative_decision": "analysis_only"})
    _write_json(phase1d / "input_resolution.json", {})
    pd.DataFrame(
        [
            {
                "symbol": "1001",
                "anchor_date": "2026-01-10",
                "side": "long",
                "champion_rank": 3,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "score": 0.5,
            }
        ]
    ).to_parquet(surface_dir / "candidate_prefilter_rows.parquet", index=False)
    return phase1c, phase1d, surface_dir


def test_run_phase1e_writes_required_artifacts(tmp_path: Path) -> None:
    phase1c, phase1d, surface_dir = _phase_roots(tmp_path)

    result = phase1e.run_phase1e(
        phase1c_root=phase1c,
        phase1d_root=phase1d,
        output_root=tmp_path / "out",
        search_root=surface_dir,
        max_sources=10,
    )

    root = Path(result["session_root"])
    for artifact in phase1e.REQUIRED_ARTIFACTS:
        assert (root / artifact).exists(), artifact
    complete = json.loads((root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["all_present"] is True
    decision = json.loads((root / "phase1e_decision.json").read_text(encoding="utf-8"))
    assert decision["authoritative_decision"] == "proceed_to_ranking_pretest"
    assert decision["matched_signal_rows"] == 1
    assert decision["topk_counts"]["top5"] == 1
