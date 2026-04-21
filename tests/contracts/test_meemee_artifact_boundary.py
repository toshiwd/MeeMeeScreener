from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.backend.api.routers.system as system_router
from app.backend.services.meemee_artifact_boundary import (
    BLOCKED_HOLD_ARTIFACT_FILENAMES,
    MEEMEE_SAFE_ARTIFACT_FILENAMES,
    TRADEX_ONLY_ARTIFACT_FILENAMES,
    MeeMeeArtifactBucket,
    classify_meemee_artifact,
    load_meemee_artifact_json,
    resolve_meemee_artifact_path,
)


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(system_router.router)
    return TestClient(app)


def test_meemee_safe_artifacts_resolve_and_load_json() -> None:
    expected_fields = {
        "chart_data_provenance_contract.json": ("schema_version", "chart_data_provenance_contract_v1"),
        "chart_gallery_authoritative_overwrite_contract.json": ("schema_version", "chart_gallery_authoritative_overwrite_contract_v1"),
        "chart_gallery_authoritative_adoption.json": ("schema_version", "chart_gallery_authoritative_adoption_v1"),
    }

    for artifact_name in MEEMEE_SAFE_ARTIFACT_FILENAMES:
        classification = classify_meemee_artifact(artifact_name)
        assert classification.bucket == MeeMeeArtifactBucket.MEE_MEE_SAFE
        assert classification.allowed is True

        path = resolve_meemee_artifact_path(artifact_name)
        assert path.exists()
        assert path.name == artifact_name

        payload = load_meemee_artifact_json(artifact_name)
        assert isinstance(payload, dict)
        key, value = expected_fields[artifact_name]
        assert payload[key] == value
        assert payload.get("status") == "authoritative"


@pytest.mark.parametrize(
    ("artifact_name", "payload_key", "expected_value"),
    [
        ("chart_data_provenance_contract.json", "scope", "meeMee_detail_chart_source_provenance"),
        ("chart_gallery_authoritative_overwrite_contract.json", "scope", "meeMee_detail_chart_source_overwrite_contract"),
        ("chart_gallery_authoritative_adoption.json", "adoption_status", "complete"),
    ],
)
def test_meemee_safe_artifacts_are_readable_through_the_runtime_route(
    artifact_name: str,
    payload_key: str,
    expected_value: str,
) -> None:
    with _build_client() as client:
        response = client.get(f"/api/system/meemee/artifacts/{artifact_name}")
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["artifact_name"] == artifact_name
        assert payload["bucket"] == "MeeMee-safe"
        assert payload["allowed"] is True
        assert payload["artifact"][payload_key] == expected_value


@pytest.mark.parametrize("artifact_name", TRADEX_ONLY_ARTIFACT_FILENAMES)
def test_meemee_tradex_only_artifacts_are_denied(artifact_name: str) -> None:
    classification = classify_meemee_artifact(artifact_name)
    assert classification.bucket == MeeMeeArtifactBucket.TRADEX_ONLY
    assert classification.allowed is False

    with pytest.raises(PermissionError, match="not resolvable from MeeMee runtime paths"):
        resolve_meemee_artifact_path(artifact_name)

        with pytest.raises(PermissionError, match="not resolvable from MeeMee runtime paths"):
            load_meemee_artifact_json(artifact_name)


@pytest.mark.parametrize("artifact_name", BLOCKED_HOLD_ARTIFACT_FILENAMES)
def test_meemee_blocked_hold_artifacts_are_denied(artifact_name: str) -> None:
    classification = classify_meemee_artifact(artifact_name)
    assert classification.bucket == MeeMeeArtifactBucket.BLOCKED_HOLD
    assert classification.allowed is False

    with pytest.raises(PermissionError, match="not resolvable from MeeMee runtime paths"):
        resolve_meemee_artifact_path(artifact_name)

    with pytest.raises(PermissionError, match="not resolvable from MeeMee runtime paths"):
        load_meemee_artifact_json(artifact_name)


@pytest.mark.parametrize(
    "artifact_name",
    [
        *TRADEX_ONLY_ARTIFACT_FILENAMES,
        *BLOCKED_HOLD_ARTIFACT_FILENAMES,
    ],
)
def test_meemee_artifact_runtime_route_denies_non_safe_artifacts(artifact_name: str) -> None:
    with _build_client() as client:
        response = client.get(f"/api/system/meemee/artifacts/{artifact_name}")
        assert response.status_code == 403
        assert response.json()["detail"]["artifact_name"] == artifact_name


def test_meemee_runtime_code_does_not_directly_read_research_inventory() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    runtime_roots = [
        repo_root / "app" / "backend",
        repo_root / "app" / "frontend" / "src",
    ]
    ignored_files = {
        repo_root / "app" / "backend" / "services" / "meemee_artifact_boundary.py",
    }
    forbidden_tokens = {
        "research_inventory",
        *MEEMEE_SAFE_ARTIFACT_FILENAMES,
        *TRADEX_ONLY_ARTIFACT_FILENAMES,
        *BLOCKED_HOLD_ARTIFACT_FILENAMES,
    }

    hits: list[str] = []
    for root in runtime_roots:
        for path in root.rglob("*"):
            if not path.is_file() or path in ignored_files:
                continue
            if path.suffix.lower() not in {".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}:
                continue
            text = path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                if token in text:
                    hits.append(f"{path.relative_to(repo_root)}::{token}")

    assert hits == [], f"direct MeeMee runtime artifact references found: {hits}"
