from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class PublishedLogicArtifact(TypedDict):
    artifact_version: str
    logic_id: str
    logic_version: str
    logic_family: str
    feature_spec_version: str
    required_inputs: list[str]
    scorer_type: str
    params: dict[str, Any]
    thresholds: dict[str, Any]
    weights: dict[str, Any]
    output_spec: dict[str, Any]
    checksum: str


class PublishedLogicManifest(TypedDict):
    logic_id: str
    logic_version: str
    logic_family: str
    status: str
    input_schema_version: str
    output_schema_version: str
    trained_at: NotRequired[str | None]
    published_at: NotRequired[str | None]
    artifact_uri: str
    checksum: str


class PublishedRankingSnapshot(TypedDict):
    artifact_version: str
    logic_id: str
    logic_version: str
    logic_family: str
    as_of_date: str
    generated_at: str
    universe_size: int
    rows: list[dict[str, Any]]
    audit_role: str


class ValidationSummary(TypedDict):
    logic_id: str
    logic_version: str
    logic_family: str
    evaluation_scope: str
    decision: str
    champion_logic_version: NotRequired[str | None]
    challenger_logic_version: NotRequired[str | None]
    metrics: dict[str, Any]
    notes: list[str]
    created_at: str


PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE = "runtime_cache_audit_artifact"


PUBLISHED_LOGIC_ARTIFACT_FIELDS: tuple[str, ...] = (
    "artifact_version",
    "logic_id",
    "logic_version",
    "logic_family",
    "feature_spec_version",
    "required_inputs",
    "scorer_type",
    "params",
    "thresholds",
    "weights",
    "output_spec",
    "checksum",
)
PUBLISHED_LOGIC_MANIFEST_FIELDS: tuple[str, ...] = (
    "logic_id",
    "logic_version",
    "logic_family",
    "status",
    "input_schema_version",
    "output_schema_version",
    "trained_at",
    "published_at",
    "artifact_uri",
    "checksum",
)
PUBLISHED_RANKING_SNAPSHOT_FIELDS: tuple[str, ...] = (
    "artifact_version",
    "logic_id",
    "logic_version",
    "logic_family",
    "as_of_date",
    "generated_at",
    "universe_size",
    "rows",
    "audit_role",
)
VALIDATION_SUMMARY_FIELDS: tuple[str, ...] = (
    "logic_id",
    "logic_version",
    "logic_family",
    "evaluation_scope",
    "decision",
    "champion_logic_version",
    "challenger_logic_version",
    "metrics",
    "notes",
    "created_at",
)

