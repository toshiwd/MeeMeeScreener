from __future__ import annotations

from external_analysis.image_rerank.split import (
    build_split_audit_manifest,
    build_time_block_split_manifest,
    classify_boundary_reasons,
)


def test_image_rerank_split_manifest_records_index_boundaries_and_reason_counts() -> None:
    ordered_dates = list(range(20260101, 20260131))
    manifest = build_time_block_split_manifest(
        run_id="run-1",
        trading_dates=ordered_dates,
        block_size_days=10,
        embargo_days=2,
        feature_lookback_days=4,
        label_horizon_days=3,
    )
    assert "trading_day_index_map" not in manifest
    assert manifest["blocks"][0]["block_start_index"] == 0
    assert manifest["blocks"][0]["block_end_index"] == 9
    assert manifest["blocks"][1]["block_start_index"] == 10
    assert manifest["blocks"][1]["block_end_index"] == 19

    audit = build_split_audit_manifest(
        split_manifest=manifest,
        sample_indices=[5, 7, 12, 22, 24],
        feature_lookback_days=4,
        label_horizon_days=3,
    )
    boundary = audit["boundary_checks"][0]
    assert boundary["protected_block_index"] == 1
    assert boundary["purge_start_index"] == 5
    assert boundary["embargo_end_index"] == 25
    assert boundary["reason_codes"] == [
        "feature_overlap",
        "label_overlap",
        "feature_and_label_overlap",
        "embargo_only",
    ]
    assert boundary["reason_counts"] == {
        "feature_overlap": 1,
        "label_overlap": 1,
        "feature_and_label_overlap": 1,
        "embargo_only": 2,
    }
    assert audit["boundary_checks"][1]["reason_codes"] == ["feature_and_label_overlap"]
    assert audit["reason_counts"] == {
        "feature_overlap": 1,
        "label_overlap": 1,
        "feature_and_label_overlap": 3,
        "embargo_only": 2,
    }
    assert audit["aggregate_visibility"] == audit["reason_counts"]

    assert classify_boundary_reasons(
        as_of_index=5,
        feature_lookback_days=4,
        label_horizon_days=3,
        protected_start_index=10,
        protected_end_index=19,
        purge_start_index=5,
        embargo_end_index=25,
    ) == ["embargo_only"]
    assert classify_boundary_reasons(
        as_of_index=7,
        feature_lookback_days=4,
        label_horizon_days=3,
        protected_start_index=10,
        protected_end_index=19,
        purge_start_index=5,
        embargo_end_index=25,
    ) == ["label_overlap"]
    assert classify_boundary_reasons(
        as_of_index=12,
        feature_lookback_days=4,
        label_horizon_days=3,
        protected_start_index=10,
        protected_end_index=19,
        purge_start_index=5,
        embargo_end_index=25,
    ) == ["feature_and_label_overlap"]
    assert classify_boundary_reasons(
        as_of_index=22,
        feature_lookback_days=4,
        label_horizon_days=3,
        protected_start_index=10,
        protected_end_index=19,
        purge_start_index=5,
        embargo_end_index=25,
    ) == ["feature_overlap"]
