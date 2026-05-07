from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_observable_regime_false_positive_bottom15_audit_v1 import (
    run_observable_regime_false_positive_bottom15_audit_v1,
)


CHALLENGER_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
BASELINE_SURFACE = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\candidate_prefilter_rows_context_enriched.parquet")
NO_LOOKAHEAD = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\no_lookahead_context_audit.json")


def test_bottom15_audit_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "observable_regime_false_positive_bottom15_audit_v1"
    result = run_observable_regime_false_positive_bottom15_audit_v1(
        challenger_session=CHALLENGER_SESSION,
        candidate_surface_path=BASELINE_SURFACE,
        no_lookahead_context_audit_path=NO_LOOKAHEAD,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required = (
        "run_manifest.json",
        "input_resolution.json",
        "bottom15_delta_rows.parquet",
        "bottom15_delta_summary.json",
        "added_top15_vs_bottom15_contrast.json",
        "confirmation_route_quality_summary.json",
        "bottom15_context_concentration_summary.json",
        "bottom15_guard_hypotheses.json",
        "observable_regime_false_positive_bottom15_audit_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required:
        assert (session_dir / file_name).exists()

    decision = json.loads((session_dir / "observable_regime_false_positive_bottom15_audit_v1_decision.json").read_text(encoding="utf-8"))
    summary = json.loads((session_dir / "bottom15_delta_summary.json").read_text(encoding="utf-8"))
    contrast = json.loads((session_dir / "added_top15_vs_bottom15_contrast.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert decision["decision"] in {"ready_for_one_guard_challenger", "hold_needs_more_evidence", "explanation_only", "drop_line"}
    assert summary["schema_version"].startswith("tradex_observable_regime_false_positive_bottom15_audit_v1")
    assert contrast["schema_version"].startswith("tradex_observable_regime_false_positive_bottom15_audit_v1")
    assert artifact["parse_status"]["run_manifest"] is True
    assert artifact["row_reconciliation"]["candidate_rows"] > 0

    delta = pd.read_parquet(session_dir / "bottom15_delta_rows.parquet")
    assert {"delta_type", "audit_topk", "confirmation_route"}.issubset(delta.columns)
