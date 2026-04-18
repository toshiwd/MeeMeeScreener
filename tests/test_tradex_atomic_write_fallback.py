from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backend.services import tradex_experiment_store as experiment_store
from app.backend.services import tradex_research_os_store as os_store


@pytest.mark.parametrize(
    ("module", "writer_name"),
    [
        (experiment_store, "write_json"),
        (os_store, "write_json"),
    ],
)
def test_atomic_write_json_permission_error_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module, writer_name: str) -> None:
    target = tmp_path / "nested" / "payload.json"
    payload = {"schema_version": "test_v1", "value": 123, "nested": {"ok": True}}

    def _deny_replace(src, dst):  # type: ignore[no-untyped-def]
        raise PermissionError("blocked by test")

    monkeypatch.setattr(module.os, "replace", _deny_replace)

    writer = getattr(module, writer_name)
    result_path = writer(target, payload)

    assert result_path == target
    assert target.exists()
    assert json.loads(target.read_text(encoding="utf-8")) == payload
