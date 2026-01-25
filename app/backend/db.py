import time
import threading
import duckdb
from core.config import config


# Notes:
# - We intentionally avoid a single global DuckDB connection. In practice it can
#   serialize the whole app behind one lock during long queries (e.g. ingest),
#   which makes the UI appear "dead".
# - Windows may still surface "file is already open" errors when another *process*
#   holds the DB. For that case we do a short retry.

_OPEN_LOCK = threading.Lock()


def _connect_with_retry(max_wait_sec: float = 1.0) -> duckdb.DuckDBPyConnection:
    deadline = time.time() + max_wait_sec
    last_err: Exception | None = None
    # Guard connect() itself to reduce race-y open/close churn.
    while True:
        try:
            with _OPEN_LOCK:
                return duckdb.connect(str(config.DB_PATH))
        except Exception as exc:  # duckdb throws its own exception types
            last_err = exc
            msg = str(exc).lower()
            # If another *process* holds the file, retry briefly.
            if "already open" in msg or "used by" in msg or "cannot open file" in msg:
                if time.time() < deadline:
                    time.sleep(0.05)
                    continue
            raise


class _ConnContext:
    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self._conn = _connect_with_retry(max_wait_sec=1.0)
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            self._conn.close()
        except Exception:
            pass
        return False


def get_conn() -> _ConnContext:
    return _ConnContext()


class _TryConnContext:
    def __init__(self, timeout_sec: float = 0.0):
        self._timeout_sec = max(0.0, float(timeout_sec))
        self._conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> duckdb.DuckDBPyConnection | None:
        try:
            self._conn = _connect_with_retry(max_wait_sec=self._timeout_sec)
        except Exception:
            self._conn = None
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        return False


def try_get_conn(timeout_sec: float = 0.0) -> _TryConnContext:
    """Best-effort DB access to avoid blocking the UI during long-running tasks."""
    return _TryConnContext(timeout_sec=timeout_sec)


def init_schema() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tickers (
                code TEXT PRIMARY KEY,
                name TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                PRIMARY KEY(code, date)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_ma (
                code TEXT,
                date INTEGER,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                PRIMARY KEY(code, date)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_bars (
                code TEXT,
                month INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                PRIMARY KEY(code, month)
            );
            """
        )
        try:
            conn.execute("ALTER TABLE monthly_bars ADD COLUMN v BIGINT")
        except Exception:
            pass  # Already exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_ma (
                code TEXT,
                month INTEGER,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                PRIMARY KEY(code, month)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sys_jobs (
                id TEXT PRIMARY KEY,
                type TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                progress INTEGER,
                message TEXT,
                error TEXT
            );
            """
        )

