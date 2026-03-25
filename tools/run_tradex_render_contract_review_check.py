from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services import tradex_experiment_service
from app.backend.tools import tradex_research_runner


FORBIDDEN_RENDER_CONSUMPTION_FIELDS = [
    "latest_image_render_consumption_summary",
    "latest_image_render_consumption_status",
    "latest_image_authoritative_render_field_name",
]
CONSUMER_HELPERS = [
    tradex_experiment_service._selection_image_render_consumption_summary,
    tradex_experiment_service._selection_image_render_consumption_context,
    tradex_experiment_service._selection_ranked_row_summary,
    tradex_experiment_service._selection_code_summary,
    tradex_research_runner._image_render_projection,
]


def _print(message: str) -> None:
    print(message, flush=True)


def _scan_consumer_paths() -> list[tuple[str, str]]:
    hits: list[tuple[str, str]] = []
    for helper in CONSUMER_HELPERS:
        source = inspect.getsource(helper)
        for forbidden in FORBIDDEN_RENDER_CONSUMPTION_FIELDS:
            if forbidden in source:
                hits.append((helper.__qualname__, forbidden))
    return hits


def _run_pytest() -> int:
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/test_tradex_experiment_family_api.py",
        "-k",
        "tradex_consumer_paths_do_not_read_latest_render_consumption_fields",
    ]
    _print(f"[pytest] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=REPO_ROOT)
    return proc.returncode


def _check_docs() -> list[str]:
    required = [
        (REPO_ROOT / "docs" / "CODEX.md", "TRADEX Review Gate"),
        (REPO_ROOT / "docs" / "tradex" / "render-contract-boundary.md", "Render Contract Boundary"),
    ]
    missing: list[str] = []
    for path, marker in required:
        if not path.exists():
            missing.append(f"{path} (missing file)")
            continue
        text = path.read_text(encoding="utf-8")
        if marker not in text:
            missing.append(f"{path} (missing marker: {marker})")
    return missing


def main() -> int:
    _print("[review-check] TRADEX render contract gate")

    hits = _scan_consumer_paths()
    if hits:
        _print("[source-scan] forbidden render-consumption readers found:")
        for helper_name, forbidden in hits:
            _print(f"  - {helper_name}: {forbidden}")
        return 1
    _print("[source-scan] ok: no forbidden render-consumption readers found in consumer paths")

    docs_missing = _check_docs()
    if docs_missing:
        _print("[docs] missing review gate or boundary markers:")
        for item in docs_missing:
            _print(f"  - {item}")
        return 1
    _print("[docs] ok: review gate and boundary markers present")

    pytest_rc = _run_pytest()
    if pytest_rc != 0:
        _print(f"[pytest] failed with exit code {pytest_rc}")
        return pytest_rc
    _print("[pytest] ok")

    _print("[review-check] ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
