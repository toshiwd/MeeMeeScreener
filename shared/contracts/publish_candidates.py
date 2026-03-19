from __future__ import annotations

from typing import Any, NotRequired, TypedDict

from shared.contracts.logic_artifacts import (
    PublishedLogicArtifact,
    PublishedLogicManifest,
    PublishedRankingSnapshot,
    ValidationSummary,
)

PUBLISH_CANDIDATE_STATUS_CANDIDATE = "candidate"
PUBLISH_CANDIDATE_STATUS_APPROVED = "approved"
PUBLISH_CANDIDATE_STATUS_REJECTED = "rejected"
PUBLISH_CANDIDATE_STATUS_PROMOTED = "promoted"
PUBLISH_CANDIDATE_STATUS_RETIRED = "retired"


class PublishCandidateBundle(TypedDict):
    candidate_id: str
    logic_key: str
    logic_id: str
    logic_version: str
    logic_family: str
    created_at: str
    updated_at: str
    status: str
    source_publish_id: NotRequired[str | None]
    bundle_schema_version: str
    published_logic_artifact: PublishedLogicArtifact
    published_logic_manifest: PublishedLogicManifest
    validation_summary: ValidationSummary
    published_ranking_snapshot: NotRequired[PublishedRankingSnapshot | None]
    bundle_checksum: str
    approved_at: NotRequired[str | None]
    rejected_at: NotRequired[str | None]
    promoted_at: NotRequired[str | None]
    retired_at: NotRequired[str | None]
    validation_state: NotRequired[str]
    notes: NotRequired[list[str]]
    metadata: NotRequired[dict[str, Any]]


PUBLISH_CANDIDATE_BUNDLE_FIELDS: tuple[str, ...] = (
    "candidate_id",
    "logic_key",
    "logic_id",
    "logic_version",
    "logic_family",
    "created_at",
    "updated_at",
    "status",
    "source_publish_id",
    "bundle_schema_version",
    "published_logic_artifact",
    "published_logic_manifest",
    "validation_summary",
    "published_ranking_snapshot",
    "bundle_checksum",
    "approved_at",
    "rejected_at",
    "promoted_at",
    "retired_at",
    "validation_state",
    "notes",
    "metadata",
)
