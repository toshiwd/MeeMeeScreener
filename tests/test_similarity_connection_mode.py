import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.similarity import SimilarityService


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_args, **_kwargs):
        raise RuntimeError("stop_after_connect")


def test_similarity_refresh_uses_read_only_db_conn():
    service = SimilarityService(db_path="C:/tmp/stocks.duckdb")
    seen = {}

    def _fake_get_conn_for_path(db_path, *, timeout_sec, read_only):
        seen["db_path"] = db_path
        seen["timeout_sec"] = timeout_sec
        seen["read_only"] = read_only
        return _FakeConn()

    with patch("app.backend.similarity.get_conn_for_path", side_effect=_fake_get_conn_for_path):
        try:
            service.refresh_data(incremental=False)
        except RuntimeError as exc:
            assert str(exc) == "stop_after_connect"

    assert seen["read_only"] is True
