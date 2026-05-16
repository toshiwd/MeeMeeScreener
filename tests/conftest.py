import os
import sys
import tempfile
import hashlib
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
APP_BACKEND = ROOT / "app" / "backend"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(APP_BACKEND) not in sys.path:
    sys.path.insert(0, str(APP_BACKEND))


# Ensure tests never touch the user's real AppData database/files.
# Sandbox 実行でも確実に書き込めるよう、workspace 配下へ隔離する。
_TEST_TEMP_ROOT = ROOT / ".tmp-tests"
_TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
os.environ["TMPDIR"] = str(_TEST_TEMP_ROOT)
os.environ["TEMP"] = str(_TEST_TEMP_ROOT)
os.environ["TMP"] = str(_TEST_TEMP_ROOT)
_TEST_LOCALAPPDATA = (_TEST_TEMP_ROOT / f"localappdata_{uuid4().hex}").resolve()
_TEST_LOCALAPPDATA.mkdir(parents=True, exist_ok=True)
os.environ["LOCALAPPDATA"] = str(_TEST_LOCALAPPDATA)
tempfile.tempdir = str(_TEST_TEMP_ROOT)
_TEST_DATA_DIR = (_TEST_TEMP_ROOT / f"meemee_screener_test_{uuid4().hex}").resolve()
_TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MEEMEE_DATA_DIR", str(_TEST_DATA_DIR))
_TEST_TRADEX_ROOT = (_TEST_TEMP_ROOT / f"tradex_{uuid4().hex}").resolve()
_TEST_TRADEX_ROOT.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MEEMEE_TRADEX_ROOT", str(_TEST_TRADEX_ROOT))
_PYTEST_TEMP_ROOT = (ROOT / ".tmp-pytest-root" / uuid4().hex).resolve()
_PYTEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("PYTEST_DEBUG_TEMPROOT", str(_PYTEST_TEMP_ROOT))

# If the backend config singleton has already been imported (e.g. via another test),
# force it to use our isolated directory.
try:
    from core.config import config  # type: ignore

    config.DATA_DIR = Path(os.environ["MEEMEE_DATA_DIR"]).resolve()
    config.ensure_dirs()
except Exception:
    pass


class _WorkspaceTmpPathFactory:
    def __init__(self, base: Path) -> None:
        self._base = base
        self._base.mkdir(parents=True, exist_ok=True)

    def getbasetemp(self) -> Path:
        return self._base

    def mktemp(self, basename: str, numbered: bool = True) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(basename or "tmp"))
        digest = hashlib.sha1(safe.encode("utf-8")).hexdigest()[:12]
        target = (self._base / f"t_{digest}").resolve()
        target.mkdir(parents=True, exist_ok=True)
        return target


@pytest.fixture(scope="session")
def tmp_path_factory() -> _WorkspaceTmpPathFactory:
    return _WorkspaceTmpPathFactory((ROOT / ".tmp-pytest-fixtures" / uuid4().hex).resolve())


@pytest.fixture
def tmp_path(request: pytest.FixtureRequest, tmp_path_factory: _WorkspaceTmpPathFactory) -> Path:
    return tmp_path_factory.mktemp(request.node.nodeid)
