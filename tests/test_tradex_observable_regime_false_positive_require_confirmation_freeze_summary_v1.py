from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_observable_regime_false_positive_require_confirmation_freeze_summary_v1 import (
    build_freeze_summary,
)


CHALLENGER_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
BOTTOM15_AUDIT_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_audit_v1\20260501T082434Z-859316")
REBUILD_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155")


def test_freeze_summary_writes_required_artifacts(tmp_path: Path) -> None:
    result = build_freeze_summary(
        tmp_path / "freeze",
        session_id="session-test",
        challenger_session=CHALLENGER_SESSION,
        bottom15_audit_session=BOTTOM15_AUDIT_SESSION,
        rebuild_session=REBUILD_SESSION,
    )

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "lineage_summary.json",
        "metric_reconciliation_summary.json",
        "freeze_decision.json",
        "reusable_findings.json",
        "not_for_policy_reasons.json",
        "future_reopen_conditions.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    freeze_decision = json.loads((session_dir / "freeze_decision.json").read_text(encoding="utf-8"))
    assert freeze_decision["decision"] == "freeze_observable_regime_false_positive_require_confirmation_line"
    assert freeze_decision["status"] == "explanation_only"
    assert freeze_decision["promote_ready"] is False
    assert freeze_decision["meemee_reflectable"] is False
    assert freeze_decision["metric_source_status"] == "rebuilt_metrics_reconciled"
    assert freeze_decision["superseded_old_artifacts"] is True

    metric_recon = json.loads((session_dir / "metric_reconciliation_summary.json").read_text(encoding="utf-8"))
    assert metric_recon["old_bottom15_summary_invalidated"] is True
    assert metric_recon["freeze_precheck_result"] == "ready_to_freeze"
    assert metric_recon["rebuilt_summary_matches"]["authoritative_parquet"] is True
    assert metric_recon["rebuilt_summary_matches"]["variant_pool_comparison"] is True

    lineage = json.loads((session_dir / "lineage_summary.json").read_text(encoding="utf-8"))
    assert lineage["decision"] == "freeze_observable_regime_false_positive_require_confirmation_line"
    assert lineage["status"] == "explanation_only"
    assert lineage["same_condition_contract"] is True

    reusable = json.loads((session_dir / "reusable_findings.json").read_text(encoding="utf-8"))
    findings = {row["finding_id"]: row for row in reusable["findings"]}
    assert findings["require_confirmation_moved_topk"]["status"] == "confirmed"
    assert findings["bottom15_cost_unresolved"]["status"] == "confirmed"

    future = json.loads((session_dir / "future_reopen_conditions.json").read_text(encoding="utf-8"))
    assert "entry_strength" in future["example_fields"][0] or "volume" in future["example_fields"][0]
    assert "rank" in future["do_not_reopen_based_only_on"]
