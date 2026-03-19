from __future__ import annotations

import json

from app.backend.infra.files import config_repo as config_repo_module
from app.backend.infra.files.config_repo import ConfigRepository, LOGIC_SELECTION_SCHEMA_VERSION


def test_save_logic_selection_state_is_atomic_and_versioned(monkeypatch, tmp_path) -> None:
    repo = ConfigRepository(str(tmp_path))
    replace_calls: list[tuple[str, str]] = []
    original_replace = config_repo_module.os.replace

    def _spy_replace(src: str, dst: str) -> None:
        replace_calls.append((src, dst))
        original_replace(src, dst)

    monkeypatch.setattr(config_repo_module.os, "replace", _spy_replace)

    saved_path = repo.save_logic_selection_state({"selected_logic_override": "logic_override_v9"})

    assert saved_path == repo.logic_selection_path
    assert replace_calls
    assert replace_calls[0][1] == repo.logic_selection_path
    with open(saved_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["schema_version"] == LOGIC_SELECTION_SCHEMA_VERSION
    assert payload["selected_logic_override"] == "logic_override_v9"

