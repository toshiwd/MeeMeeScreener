from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_audit_surface_context_backfill_v1 import (
    DEFAULT_CANDIDATE_SURFACE,
    DEFAULT_UNKNOWN_RECLASSIFICATION,
    _apply_backfill_contract,
    _merge_fill,
    _normalize_backfill_join_keys,
    run_audit_surface_context_backfill_v1,
)


POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
SELECTION_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json"
)
CANDIDATE_SNAPSHOT = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_candidate_snapshots.json"
)


def test_audit_surface_context_backfill_smoke_run(tmp_path: Path) -> None:
    output_root = tmp_path / "audit_surface_context_backfill_v1"
    result = run_audit_surface_context_backfill_v1(
        candidate_surface_path=DEFAULT_CANDIDATE_SURFACE,
        unknown_reclassification_path=DEFAULT_UNKNOWN_RECLASSIFICATION,
        policy_ledger_path=POLICY_LEDGER,
        selection_ledger_path=SELECTION_LEDGER,
        candidate_snapshot_path=CANDIDATE_SNAPSHOT,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["session_dir"])
    required = {
        "run_manifest.json",
        "input_resolution.json",
        "context_source_inventory.json",
        "context_join_contract.json",
        "candidate_prefilter_rows_context_enriched.parquet",
        "unknown_reclassification_rows_context_enriched.parquet",
        "context_backfill_coverage_summary.json",
        "no_lookahead_context_audit.json",
        "reclassification_readiness_after_backfill.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert required.issubset({path.name for path in session_dir.iterdir()})

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "context_backfill_coverage_summary.json").read_text(encoding="utf-8"))
    audit = json.loads((session_dir / "no_lookahead_context_audit.json").read_text(encoding="utf-8"))
    readiness = json.loads((session_dir / "reclassification_readiness_after_backfill.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_audit_surface_context_backfill_v1_manifest_v1"
    assert artifact["parse_status"]["candidate_prefilter_rows_context_enriched_parquet"] is True
    assert artifact["parse_status"]["unknown_reclassification_rows_context_enriched_parquet"] is True
    assert artifact["row_reconciliation"]["candidate_surface_row_count_preserved"] == 1
    assert artifact["row_reconciliation"]["unknown_surface_row_count_preserved"] == 1
    assert audit["status"] == "pass"
    assert readiness["decision"] in {
        "ready_to_rerun_unknown_reclassification",
        "partial_backfill_needs_more_sources",
        "insufficient_context_sources",
    }
    assert coverage["candidate_surface_after"]["daily_main_state_ctx"]["non_null_count"] >= coverage["candidate_surface_before"]["daily_main_state_ctx"]["non_null_count"]
    assert coverage["unknown_reclassification_after"]["daily_main_state_ctx"]["non_null_count"] >= coverage["unknown_reclassification_before"]["daily_main_state_ctx"]["non_null_count"]

    candidate = pd.read_parquet(session_dir / "candidate_prefilter_rows_context_enriched.parquet")
    unknown = pd.read_parquet(session_dir / "unknown_reclassification_rows_context_enriched.parquet")

    assert len(candidate) == artifact["row_reconciliation"]["candidate_surface_rows_before"]
    assert len(unknown) == artifact["row_reconciliation"]["unknown_surface_rows_before"]
    assert "daily_main_state_ctx" in candidate.columns
    assert "daily_main_state_ctx" in unknown.columns
    assert "monthly_context_backfill_status" in candidate.columns
    assert "weekly_context_backfill_status" in candidate.columns
    assert "daily_main_state_ctx_backfill_status" in candidate.columns
    assert candidate["daily_main_state_ctx"].notna().sum() > 0
    assert unknown["daily_main_state_ctx"].notna().sum() > 0


def test_backfill_merge_normalizes_alias_keys_and_preserves_rows() -> None:
    base = pd.DataFrame(
        [
            {
                "as_of": "2026-01-22",
                "code": "8308",
                "side": "long",
                "monthly_context_no_lookahead": pd.NA,
                "weekly_context_no_lookahead": pd.NA,
                "daily_main_state_ctx": pd.NA,
            }
        ]
    )
    overlay = pd.DataFrame(
        [
            {
                "decision_date": "2026-01-22",
                "sec_code": "8308",
                "side": "long",
                "context_overlay_source": "policy_trade_ledger_exact_same_day",
                "daily_main_state_ctx": "ctx",
                "daily_main_state_ctx_backfilled": "ctx",
                "monthly_main_state_ctx": "monthly",
                "weekly_main_state_ctx": "weekly",
                "monthly_context_no_lookahead_backfilled": True,
                "weekly_context_no_lookahead_backfilled": True,
                "daily_main_state_ctx_no_lookahead_backfilled": True,
            }
        ]
    )

    merged = _merge_fill(base, overlay)
    applied = _apply_backfill_contract(merged)

    assert len(applied) == 1
    assert list(applied["anchor_date"]) == ["2026-01-22"]
    assert list(applied["symbol"]) == ["8308"]
    assert applied.loc[0, "_overlay_joined"] is True or bool(applied.loc[0, "_overlay_joined"]) is True
    assert applied.loc[0, "daily_main_state_ctx"] == "ctx"
    assert applied.loc[0, "monthly_context_backfill_status"] in {"backfilled", "existing"}
    assert applied.loc[0, "weekly_context_backfill_status"] in {"backfilled", "existing"}
    assert applied.loc[0, "daily_main_state_ctx_backfill_status"] in {"backfilled", "existing"}


def test_backfill_merge_empty_overlay_preserves_rows_and_marks_missing() -> None:
    base = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-22",
                "symbol": "8308",
                "side": "long",
                "monthly_context_no_lookahead": pd.NA,
                "weekly_context_no_lookahead": pd.NA,
                "daily_main_state_ctx": pd.NA,
            }
        ]
    )
    overlay = pd.DataFrame()

    merged = _merge_fill(base, overlay)
    applied = _apply_backfill_contract(merged)

    assert len(applied) == 1
    assert bool(applied.loc[0, "_overlay_joined"]) is False
    assert applied.loc[0, "monthly_context_backfill_status"] == "missing"
    assert applied.loc[0, "weekly_context_backfill_status"] == "missing"
    assert applied.loc[0, "daily_main_state_ctx_backfill_status"] == "missing"
    assert applied.loc[0, "monthly_context_backfill_missing_reason"] == "no_same_day_policy_overlay"
    assert applied.loc[0, "daily_main_state_ctx_backfill_missing_reason"] == "no_same_day_policy_overlay"


def test_backfill_merge_rejects_future_overlay_matches() -> None:
    base = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-22",
                "symbol": "8308",
                "side": "long",
                "monthly_context_no_lookahead": pd.NA,
                "weekly_context_no_lookahead": pd.NA,
                "daily_main_state_ctx": pd.NA,
            }
        ]
    )
    overlay = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-23",
                "symbol": "8308",
                "side": "long",
                "context_overlay_source": "policy_trade_ledger_exact_same_day",
                "daily_main_state_ctx": "ctx",
                "daily_main_state_ctx_backfilled": "ctx",
                "monthly_main_state_ctx": "monthly",
                "weekly_main_state_ctx": "weekly",
                "monthly_context_no_lookahead_backfilled": True,
                "weekly_context_no_lookahead_backfilled": True,
                "daily_main_state_ctx_no_lookahead_backfilled": True,
            }
        ]
    )

    merged = _merge_fill(base, overlay)
    applied = _apply_backfill_contract(merged)

    assert len(applied) == 1
    assert bool(applied.loc[0, "_overlay_joined"]) is False
    assert applied.loc[0, "daily_main_state_ctx_backfill_status"] == "missing"
    assert applied.loc[0, "daily_main_state_ctx_backfill_missing_reason"] == "no_same_day_policy_overlay"


def test_backfill_join_key_normalization_handles_alias_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "decision_date": "2026-01-22",
                "sec_code": "8308",
                "side": "long",
            }
        ]
    )

    normalized = _normalize_backfill_join_keys(frame, label="alias frame")

    assert list(normalized.columns[:3])[:3]
    assert "anchor_date" in normalized.columns
    assert "symbol" in normalized.columns
    assert "side" in normalized.columns
    assert list(normalized["anchor_date"]) == ["2026-01-22"]
    assert list(normalized["symbol"]) == ["8308"]
