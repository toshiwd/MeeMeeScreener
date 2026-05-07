from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_feature_surface_edinet_event_proxy_v1 import run_edinet_event_proxy


def test_edinet_event_proxy_smoke_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_edinet_event_proxy(
        output_root=tmp_path / "feature_surface_edinet_event_proxy_v1",
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["output_dir"])
    required = {
        "run_manifest.json",
        "input_resolution.json",
        "edinet_source_profile.json",
        "edinet_proxy_taxonomy_contract.json",
        "edinet_event_feature_formula_contract.json",
        "edinet_event_coverage_summary.json",
        "edinet_event_missingness_summary.json",
        "no_lookahead_edinet_event_audit.json",
        "added_top15_vs_bottom15_edinet_contrast.json",
        "orfp_edinet_event_summary.json",
        "feature_surface_edinet_event_proxy_v1_decision.json",
        "candidate_prefilter_rows_edinet_event_enriched_v1.parquet",
        "observable_regime_false_positive_edinet_event_enriched_v1.parquet",
        "_ARTIFACT_COMPLETE.json",
    }
    names = {path.name for path in session_dir.iterdir()}
    assert required.issubset(names)

    decision = json.loads((session_dir / "feature_surface_edinet_event_proxy_v1_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((session_dir / "no_lookahead_edinet_event_audit.json").read_text(encoding="utf-8"))
    profile = json.loads((session_dir / "edinet_source_profile.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "edinet_event_coverage_summary.json").read_text(encoding="utf-8"))

    assert decision["decision"] in {
        "ready_to_rerun_reclassification_with_edinet_features",
        "needs_edinet_taxonomy_revision",
        "insufficient_edinet_coverage",
        "stop_edinet_event_proxy_line",
    }
    assert audit["candidate_surface"]["status"] == "pass"
    assert audit["orfp_surface"]["status"] == "pass"
    assert profile["row_count"] > 0
    assert coverage["candidate_rows"] > 0
    assert coverage["orfp_rows"] > 0

    candidate = pd.read_parquet(session_dir / "candidate_prefilter_rows_edinet_event_enriched_v1.parquet")
    orfp = pd.read_parquet(session_dir / "observable_regime_false_positive_edinet_event_enriched_v1.parquet")
    assert len(candidate) > 0
    assert len(orfp) > 0
    for column in (
        "edinet_recent_filing_flag",
        "edinet_recent_filing_count_20d",
        "edinet_recent_filing_count_5d",
        "edinet_financing_or_dilution_proxy_flag",
        "edinet_security_issuance_proxy_flag",
        "edinet_material_filing_activity_proxy_flag",
        "edinet_event_noise_bucket",
        "days_since_last_edinet_filing",
    ):
        assert column in candidate.columns
        assert column in orfp.columns
