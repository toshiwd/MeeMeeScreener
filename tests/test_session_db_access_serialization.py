import os
import sys
import threading
import time
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.db.session import get_conn_for_path


class _FakeConn:
    def close(self) -> None:
        return None


def test_same_db_path_access_is_serialized():
    entered = []
    first_entered = threading.Event()
    release_first = threading.Event()
    first_done = threading.Event()

    def _connect(*, db_path, max_wait_sec, read_only):
        entered.append((db_path, read_only, time.monotonic()))
        return _FakeConn()

    def _worker_first():
        with get_conn_for_path("C:/tmp/stocks.duckdb", timeout_sec=0.1, read_only=False):
            first_entered.set()
            release_first.wait(timeout=1.0)
        first_done.set()

    def _worker_second():
        first_entered.wait(timeout=1.0)
        with get_conn_for_path("C:/tmp/stocks.duckdb", timeout_sec=0.1, read_only=True):
            return None

    with patch("app.db.session._connect_with_retry_path", side_effect=_connect):
        t1 = threading.Thread(target=_worker_first)
        t2 = threading.Thread(target=_worker_second)
        t1.start()
        first_entered.wait(timeout=1.0)
        t2.start()
        time.sleep(0.1)
        assert len(entered) == 1
        release_first.set()
        first_done.wait(timeout=1.0)
        t1.join(timeout=1.0)
        t2.join(timeout=1.0)

    assert len(entered) == 2
    assert entered[0][2] <= entered[1][2]
