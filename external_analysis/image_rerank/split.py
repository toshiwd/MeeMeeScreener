from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from external_analysis.image_rerank.dataset import normalize_as_of_date

FEATURE_OVERLAP = "feature_overlap"
LABEL_OVERLAP = "label_overlap"
FEATURE_AND_LABEL_OVERLAP = "feature_and_label_overlap"
EMBARGO_ONLY = "embargo_only"
SPLIT_SCHEMA_VERSION = "tradex_image_rerank_split_v1"

REASON_ORDER: tuple[str, ...] = (
    FEATURE_OVERLAP,
    LABEL_OVERLAP,
    FEATURE_AND_LABEL_OVERLAP,
    EMBARGO_ONLY,
)


def _split_roles(block_count: int) -> list[str]:
    if block_count <= 0:
        return []
    if block_count == 1:
        return ["train"]
    if block_count == 2:
        return ["train", "test"]
    roles = ["train"] * max(1, block_count - 2)
    roles.append("val")
    roles.append("test")
    return roles[:block_count]


def _overlaps(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a <= end_b and end_a >= start_b


def _build_reason_counts() -> dict[str, int]:
    return {reason: 0 for reason in REASON_ORDER}


def _ordered_unique_dates(trading_dates: Iterable[int]) -> list[int]:
    unique_dates = sorted({normalize_as_of_date(value) for value in trading_dates})
    if not unique_dates:
        raise RuntimeError("trading_dates is empty")
    return unique_dates


def build_time_block_split_manifest(
    *,
    run_id: str,
    trading_dates: list[int],
    block_size_days: int,
    embargo_days: int,
    feature_lookback_days: int,
    label_horizon_days: int,
) -> dict[str, Any]:
    unique_dates = _ordered_unique_dates(trading_dates)
    blocks: list[dict[str, Any]] = []
    effective_block_size = max(1, int(block_size_days))
    for index in range(0, len(unique_dates), effective_block_size):
        window = unique_dates[index : index + effective_block_size]
        blocks.append(
            {
                "block_index": len(blocks),
                "block_start_index": index,
                "block_end_index": index + len(window) - 1,
                "start_date": window[0],
                "end_date": window[-1],
                "date_count": len(window),
            }
        )
    roles = _split_roles(len(blocks))
    for block, role in zip(blocks, roles, strict=True):
        block["role"] = role

    boundary_checks: list[dict[str, Any]] = []
    last_index = len(unique_dates) - 1
    for idx in range(len(blocks) - 1):
        current_block = blocks[idx]
        protected_block = blocks[idx + 1]
        protected_start_index = int(protected_block["block_start_index"])
        protected_end_index = int(protected_block["block_end_index"])
        purge_start_index = max(0, protected_start_index - int(label_horizon_days) - int(embargo_days))
        embargo_end_index = min(last_index, protected_end_index + int(feature_lookback_days) + int(embargo_days))
        boundary_checks.append(
            {
                "boundary_index": idx,
                "left_block_index": int(current_block["block_index"]),
                "protected_block_index": int(protected_block["block_index"]),
                "protected_start_index": protected_start_index,
                "protected_end_index": protected_end_index,
                "protected_start_date": int(protected_block["start_date"]),
                "protected_end_date": int(protected_block["end_date"]),
                "purge_start_index": purge_start_index,
                "embargo_end_index": embargo_end_index,
                "reason_codes": [],
                "reason_counts": _build_reason_counts(),
                "sample_count": 0,
            }
        )

    counts = {
        "trading_date_count": len(unique_dates),
        "block_count": len(blocks),
        "train_block_count": sum(1 for block in blocks if block["role"] == "train"),
        "val_block_count": sum(1 for block in blocks if block["role"] == "val"),
        "test_block_count": sum(1 for block in blocks if block["role"] == "test"),
        "boundary_count": len(boundary_checks),
    }
    return {
        "schema_version": SPLIT_SCHEMA_VERSION,
        "run_id": run_id,
        "block_size_days": int(block_size_days),
        "embargo_days": int(embargo_days),
        "feature_lookback_days": int(feature_lookback_days),
        "label_horizon_days": int(label_horizon_days),
        "purge_rule": {
            "name": "time-block split + purge + embargo",
            "basis": "trading_day_index",
            "protected_block_rule": "boundary checks are anchored to the protected block",
            "feature_window_overlap_check": True,
            "label_horizon_overlap_check": True,
            "embargo_check": True,
            "mechanism": "purge rows whose feature window, label horizon, or embargo band intersects the protected block",
        },
        "blocks": blocks,
        "boundary_checks": boundary_checks,
        "reason_counts": _build_reason_counts(),
        "aggregate_visibility": _build_reason_counts(),
        "counts": counts,
    }


def classify_boundary_reasons(
    *,
    as_of_index: int,
    feature_lookback_days: int,
    label_horizon_days: int,
    protected_start_index: int,
    protected_end_index: int,
    purge_start_index: int,
    embargo_end_index: int,
) -> list[str]:
    feature_start_index = int(as_of_index) - int(feature_lookback_days) + 1
    feature_end_index = int(as_of_index)
    label_start_index = int(as_of_index) + 1
    label_end_index = int(as_of_index) + int(label_horizon_days)

    feature_overlap = _overlaps(feature_start_index, feature_end_index, protected_start_index, protected_end_index)
    label_overlap = _overlaps(label_start_index, label_end_index, protected_start_index, protected_end_index)
    if feature_overlap and label_overlap:
        return [FEATURE_AND_LABEL_OVERLAP]
    if feature_overlap:
        return [FEATURE_OVERLAP]
    if label_overlap:
        return [LABEL_OVERLAP]
    if purge_start_index <= int(as_of_index) < protected_start_index:
        return [EMBARGO_ONLY]
    if protected_end_index < int(as_of_index) <= embargo_end_index:
        return [EMBARGO_ONLY]
    return []


def build_split_audit_manifest(
    *,
    split_manifest: dict[str, Any],
    sample_indices: Iterable[int],
    feature_lookback_days: int,
    label_horizon_days: int,
) -> dict[str, Any]:
    indices = [int(value) for value in sample_indices]
    enriched = dict(split_manifest)
    boundary_checks: list[dict[str, Any]] = []
    aggregate_reason_counts = _build_reason_counts()

    for boundary in split_manifest.get("boundary_checks") or []:
        if not isinstance(boundary, dict):
            continue
        protected_start_index = int(boundary.get("protected_start_index") or 0)
        protected_end_index = int(boundary.get("protected_end_index") or 0)
        purge_start_index = int(boundary.get("purge_start_index") or 0)
        embargo_end_index = int(boundary.get("embargo_end_index") or 0)
        reason_counts = _build_reason_counts()
        for as_of_index in indices:
            reasons = classify_boundary_reasons(
                as_of_index=as_of_index,
                feature_lookback_days=feature_lookback_days,
                label_horizon_days=label_horizon_days,
                protected_start_index=protected_start_index,
                protected_end_index=protected_end_index,
                purge_start_index=purge_start_index,
                embargo_end_index=embargo_end_index,
            )
            if not reasons:
                continue
            reason = reasons[0]
            reason_counts[reason] += 1
            aggregate_reason_counts[reason] += 1
        updated_boundary = dict(boundary)
        updated_boundary["reason_codes"] = [reason for reason in REASON_ORDER if reason_counts[reason] > 0]
        updated_boundary["reason_counts"] = reason_counts
        updated_boundary["sample_count"] = len(indices)
        boundary_checks.append(updated_boundary)

    enriched["boundary_checks"] = boundary_checks
    enriched["reason_counts"] = aggregate_reason_counts
    enriched["aggregate_visibility"] = dict(aggregate_reason_counts)
    return enriched


def assign_split_role(*, as_of_date: int, split_manifest: dict[str, Any]) -> dict[str, Any]:
    normalized_date = normalize_as_of_date(as_of_date)
    for block in split_manifest.get("blocks") or []:
        if not isinstance(block, dict):
            continue
        start_date = int(block.get("start_date") or 0)
        end_date = int(block.get("end_date") or 0)
        if start_date <= normalized_date <= end_date:
            return {
                "split_role": str(block.get("role") or "train"),
                "block_index": int(block.get("block_index") or 0),
                "block_start_index": int(block.get("block_start_index") or 0),
                "block_end_index": int(block.get("block_end_index") or 0),
                "block_start_date": start_date,
                "block_end_date": end_date,
            }
    return {
        "split_role": "purged",
        "block_index": -1,
        "block_start_index": -1,
        "block_end_index": -1,
        "block_start_date": None,
        "block_end_date": None,
    }
