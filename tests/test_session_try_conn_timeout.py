import os
import sys
import threading
import time
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.db.session import try_get_conn, try_get_conn_for_path


def _hold_lock(lock: threading.RLock, entered: threading.Event, release: threading.Event) -> None:
    lock.acquire()
    try:
        entered.set()
        release.wait(timeout=1.0)
    finally:
        lock.release()


def test_try_get_conn_returns_none_when_access_lock_is_busy():
    lock = threading.RLock()
    entered = threading.Event()
    release = threading.Event()
    worker = threading.Thread(target=_hold_lock, args=(lock, entered, release))
    worker.start()
    entered.wait(timeout=1.0)
    try:
        with patch("app.db.session._get_db_access_lock", return_value=lock):
            start = time.monotonic()
            with try_get_conn(timeout_sec=0.05) as conn:
                elapsed = time.monotonic() - start
                assert conn is None
    finally:
        release.set()
        worker.join(timeout=1.0)

    assert elapsed < 0.2


def test_try_get_conn_for_path_returns_none_when_access_lock_is_busy():
    lock = threading.RLock()
    entered = threading.Event()
    release = threading.Event()
    worker = threading.Thread(target=_hold_lock, args=(lock, entered, release))
    worker.start()
    entered.wait(timeout=1.0)
    try:
        with patch("app.db.session._get_db_access_lock", return_value=lock):
            start = time.monotonic()
            with try_get_conn_for_path("C:/tmp/stocks.duckdb", timeout_sec=0.05, read_only=True) as conn:
                elapsed = time.monotonic() - start
                assert conn is None
    finally:
        release.set()
        worker.join(timeout=1.0)

    assert elapsed < 0.2
