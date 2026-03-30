import os
import tempfile
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_BACKEND = ROOT / "app" / "backend"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(APP_BACKEND) not in sys.path:
    sys.path.insert(0, str(APP_BACKEND))


# Ensure tests never touch the user's real AppData database/files.
_TEST_DATA_DIR = tempfile.mkdtemp(prefix="meemee_screener_test_")
os.environ.setdefault("MEEMEE_DATA_DIR", _TEST_DATA_DIR)

# If the backend config singleton has already been imported (e.g. via another test),
# force it to use our isolated directory.
try:
    from core.config import config  # type: ignore

    config.DATA_DIR = Path(os.environ["MEEMEE_DATA_DIR"]).resolve()
    config.ensure_dirs()
except Exception:
    pass
