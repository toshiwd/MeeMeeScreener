from __future__ import annotations

import json
from pathlib import Path

from scripts import export_high_zone_research_candidates as exporter


def test_high_zone_research_export_writes_review_only_artifacts(monkeypatch, tmp_path: Path) -> None:
    def fake_get_rankings(*args, **kwargs):
        return {
            "snapshot_as_of": "2026-07-03",
            "freshness_state": "fresh",
            "freshness_days": 1,
            "is_provisional": False,
            "high_zone_research_candidates": [
                {
                    "code": "4104",
                    "name": "Sample",
                    "highZoneChartState": "research_needed",
                    "highZoneEvidenceSampleCount": 12,
                    "highZoneEvidenceMinSampleCount": 100,
                    "highZoneEvidenceResearchRequired": True,
                    "researchOnly": True,
                    "researchCandidateBoundary": "TRADEX_REVIEW_ONLY",
                }
            ],
        }

    monkeypatch.setattr(exporter.rankings_cache, "get_rankings", fake_get_rankings)

    result = exporter.build_high_zone_research_candidate_artifacts(
        output_root=tmp_path,
        session_id="test-session",
    )

    session_root = tmp_path / "test-session"
    snapshot_path = session_root / "high_zone_research_candidates.json"
    complete_path = session_root / "_ARTIFACT_COMPLETE.json"

    assert result["session_root"] == str(session_root)
    assert result["candidate_count"] == 1
    assert snapshot_path.exists()
    assert complete_path.exists()

    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    complete = json.loads(complete_path.read_text(encoding="utf-8"))

    assert snapshot["schema_version"] == exporter.SCHEMA_VERSION
    assert snapshot["boundary"] == "TRADEX_REVIEW_ONLY"
    assert snapshot["no_runtime_mutation"] is True
    assert snapshot["candidate_count"] == 1
    assert snapshot["candidates"][0]["researchCandidateBoundary"] == "TRADEX_REVIEW_ONLY"
    assert complete["all_present"] is True
    assert complete["boundary"] == "TRADEX_REVIEW_ONLY"
