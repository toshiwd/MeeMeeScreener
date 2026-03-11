import json
import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.desktop import launcher


class _ExitedProc:
    def poll(self):
        return 1


class _HealthyResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps({"ready": True, "ok": True}).encode("utf-8")


class _HealthyOpener:
    def open(self, _url, timeout=1):
        return _HealthyResponse()


def test_wait_for_health_reuses_healthy_backend_even_if_spawned_proc_exits():
    with (
        patch("app.desktop.launcher.urllib.request.build_opener", return_value=_HealthyOpener()),
        patch("app.desktop.launcher.time.sleep", return_value=None),
    ):
        ok, detail = launcher._wait_for_health_detail(28888, 1, proc=_ExitedProc())

    assert ok is True
    assert detail is None
