from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1 import (
    run_bottom15_summary_rebuild,
)


CHALLENGER_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
BOTTOM15_AUDIT_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_audit_v1\20260501T082434Z-859316")


def _direct_counts(frame: pd.DataFrame, topk: int) -> dict[str, int]:
    baseline = frame[f"champion_selected_top{topk}"].fillna(False).astype(bool)
    variant = frame[f"variant_selected_top{topk}"].fillna(False).astype(bool)
    top15 = frame["top15_label"].fillna(False).astype(bool)
    bottom15 = frame["bottom15_label"].fillna(False).astype(bool)
    return {
        "original_selected_count": int(baseline.sum()),
        "variant_selected_count": int(variant.sum()),
        "original_top15_count": int((baseline & top15).sum()),
        "variant_top15_count": int((variant & top15).sum()),
        "original_bottom15_count": int((baseline & bottom15).sum()),
        "variant_bottom15_count": int((variant & bottom15).sum()),
        "newly_added_top15_count": int((variant & top15 & ~baseline).sum()),
        "newly_added_bottom15_count": int((variant & bottom15 & ~baseline).sum()),
        "removed_top15_count": int((baseline & top15 & ~variant).sum()),
        "removed_bottom15_count": int((baseline & bottom15 & ~variant).sum()),
        "unchanged_top15_count": int((baseline & variant & top15).sum()),
        "unchanged_bottom15_count": int((baseline & variant & bottom15).sum()),
        "membership_changed_count": int((baseline ^ variant).sum()),
        "overlap_ratio": float((baseline & variant).sum() / max(int(baseline.sum()), 1)),
    }


def test_bottom15_summary_rebuild_reconciles_authoritative_parquet(tmp_path: Path) -> None:
    result = run_bottom15_summary_rebuild(
        challenger_session=CHALLENGER_SESSION,
        bottom15_audit_session=BOTTOM15_AUDIT_SESSION,
        output_root=tmp_path / "bottom15_summary_rebuild_v1",
    )

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "bottom15_delta_summary_rebuilt.json",
        "bottom15_delta_summary_diff.json",
        "variant_pool_reconciliation.json",
        "supersedes_bottom15_delta_summary.json",
        "freeze_precheck_after_rebuild.json",
        "metric_definition_contract.json",
        "recomputed_bottom15_delta_rows.parquet",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    frame = pd.read_parquet(CHALLENGER_SESSION / "candidate_confirmation_rows.parquet")
    rebuilt = json.loads((session_dir / "bottom15_delta_summary_rebuilt.json").read_text(encoding="utf-8"))
    diff = json.loads((session_dir / "bottom15_delta_summary_diff.json").read_text(encoding="utf-8"))
    reconciliation = json.loads((session_dir / "variant_pool_reconciliation.json").read_text(encoding="utf-8"))
    freeze = json.loads((session_dir / "freeze_precheck_after_rebuild.json").read_text(encoding="utf-8"))

    assert rebuilt["schema_version"].startswith("tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1")
    assert reconciliation["status"] == "pass"
    assert freeze["decision"] == "ready_to_freeze"
    assert freeze["freeze_ready"] is True
    assert freeze["old_summary_mismatch_count"] > 0
    assert diff["status"] == "reconciled_with_authoritative_parquet"
    assert diff["mismatches"]

    for topk in (5, 10, 20):
        direct = _direct_counts(frame, topk)
        rebuilt_top = rebuilt["topk"][f"top{topk}"]
        for field, value in direct.items():
            assert rebuilt_top[field] == value, (topk, field, rebuilt_top[field], value)

    delta_rows = pd.read_parquet(session_dir / "recomputed_bottom15_delta_rows.parquet")
    assert not delta_rows.empty
    assert set(delta_rows["delta_type"].unique()) <= {
        "newly_added_bottom15",
        "removed_bottom15",
        "unchanged_bottom15",
        "newly_added_top15",
        "removed_top15",
        "unchanged_top15",
    }

