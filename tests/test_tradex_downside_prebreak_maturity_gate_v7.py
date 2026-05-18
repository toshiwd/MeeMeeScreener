from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_downside_prebreak_maturity_gate_v7 as mod


def _write_source(root: Path) -> None:
    root.mkdir(parents=True)
    pd.DataFrame(
        [
            {"month": 202501, "skipped": False, "selected_candidate_count": 20, "closed_horizon_candidate_count": 20, "unknown_candidate_count": 0},
            {"month": 202502, "skipped": True, "skip_reason": "insufficient_closed_horizon_rows", "selected_candidate_count": 0, "closed_horizon_candidate_count": 4, "unknown_candidate_count": 20},
        ]
    ).to_csv(root / "downside_prebreak_monthly_coverage_rows.csv", index=False)


def test_maturity_gate_ready_for_full_recheck(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    _write_source(source)
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    monkeypatch.setattr(mod.base, "_resolve_db_path", lambda _cli: db_path)
    monkeypatch.setattr(mod, "_load_calendar", lambda _db: [20250228] + [20250301 + idx for idx in range(25)])

    out = tmp_path / "out"
    result = mod.run(source_root=source, output_dir=out, db_path=db_path)
    decision = json.loads((out / "downside_prebreak_maturity_decision.json").read_text(encoding="utf-8"))

    assert result["decision"] == "ready_for_full_recheck"
    assert decision["next_gate"] == "rerun_v5_stability_and_v6_monthly_coverage_now"
    assert (out / "_ARTIFACT_COMPLETE.json").exists()


def test_maturity_gate_waits_when_horizon_not_ready(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    _write_source(source)
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    monkeypatch.setattr(mod.base, "_resolve_db_path", lambda _cli: db_path)
    monkeypatch.setattr(mod, "_load_calendar", lambda _db: [20250228] + [20250301 + idx for idx in range(10)])

    result = mod.run(source_root=source, output_dir=tmp_path / "out", db_path=db_path)

    assert result["decision"] == "wait_until_full_horizon_matures"
