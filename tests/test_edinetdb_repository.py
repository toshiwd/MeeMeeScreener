from __future__ import annotations

import duckdb
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.repository import EdinetdbRepository
import app.backend.edinetdb.repository as repository_module


def test_task_enqueue_idempotent_and_force(tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)

    task_key = repo.enqueue_task(
        job_name="daily_watch",
        phase="new",
        edinet_code="E12345",
        endpoint="companies_financials",
        params={"years": 6},
        priority=10,
        force=False,
    )
    repo.mark_ok(task_key, http_status=200)

    # non-force enqueue keeps ok state
    repo.enqueue_task(
        job_name="daily_watch",
        phase="new",
        edinet_code="E12345",
        endpoint="companies_financials",
        params={"years": 6},
        priority=10,
        force=False,
    )
    task = repo.next_runnable_task("daily_watch")
    assert task is None

    # force enqueue resets to pending
    repo.enqueue_task(
        job_name="daily_watch",
        phase="new",
        edinet_code="E12345",
        endpoint="companies_financials",
        params={"years": 6},
        priority=10,
        force=True,
    )
    task = repo.next_runnable_task("daily_watch")
    assert task is not None
    assert task.task_key == task_key


def test_financials_upsert_respects_accounting_standard(tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)

    payload = {
        "items": [
            {"fiscal_year": "2024", "accounting_standard": "JP GAAP", "sales": 1},
            {"fiscal_year": "2024", "accounting_standard": "IFRS", "sales": 2},
        ]
    }
    inserted = repo.upsert_financials("E99999", payload)
    assert inserted == 2

    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT fiscal_year, accounting_standard, payload_json
            FROM edinetdb_financials
            WHERE edinet_code = 'E99999'
            ORDER BY accounting_standard
            """
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 2
    assert rows[0][1] != rows[1][1]


def test_enqueue_tasks_bulk(tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)

    rows = repo.enqueue_tasks_bulk(
        [
            {
                "job_name": "backfill_700",
                "phase": "backfill",
                "edinet_code": "E00001",
                "endpoint": "companies_detail",
                "params": {},
                "priority": 10,
            },
            {
                "job_name": "backfill_700",
                "phase": "backfill",
                "edinet_code": "E00001",
                "endpoint": "companies_financials",
                "params": {"years": 6},
                "priority": 9,
            },
        ],
        force=False,
    )
    assert len(rows) == 2

    conn = duckdb.connect(str(db_path))
    try:
        count = conn.execute("SELECT COUNT(*) FROM edinetdb_task_queue").fetchone()[0]
    finally:
        conn.close()
    assert count == 2


def test_upsert_and_list_official_documents(tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)
    inserted = repo.upsert_official_documents(
        [
            {
                "doc_id": "S100TEST1",
                "sec_code": "1301",
                "edinet_code": "E1301",
                "filer_name": "Target",
                "form_code": "030000",
                "doc_type_code": "120",
                "period_start": "2025-04-01",
                "period_end": "2026-03-31",
                "submit_datetime": "2026-03-30 15:00",
                "doc_description": "有価証券報告書",
                "csv_flag": 1,
                "pdf_flag": 1,
                "xbrl_flag": 1,
                "legal_status": "1",
                "payload": {"docID": "S100TEST1"},
            }
        ]
    )
    assert inserted == 1
    rows = repo.list_official_documents(sec_code="1301")
    assert len(rows) == 1
    assert rows[0]["doc_id"] == "S100TEST1"
    assert rows[0]["csv_flag"] == 1


def test_connect_write_uses_shared_connection_policy(tmp_path, monkeypatch):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)
    calls: list[tuple[str, object]] = []

    class _DummyConn:
        pass

    class _DummyContext:
        def __enter__(self):
            calls.append(("enter", repo._db_path))
            return _DummyConn()

        def __exit__(self, exc_type, exc, tb):
            calls.append(("exit", repo._db_path))
            return False

    def _fake_get_conn_for_path(path, *, timeout_sec=0.0, read_only=False):
        calls.append(("connect", (str(path), float(timeout_sec), bool(read_only))))
        return _DummyContext()

    def _fake_ensure_schema(conn):
        calls.append(("schema", conn.__class__.__name__))

    monkeypatch.setattr(repository_module, "get_conn_for_path", _fake_get_conn_for_path)
    monkeypatch.setattr(repository_module, "ensure_edinetdb_schema", _fake_ensure_schema)

    with repo._connect_write() as conn:
        assert isinstance(conn, _DummyConn)

    assert calls == [
        ("connect", (repo._db_path, 2.5, False)),
        ("enter", repo._db_path),
        ("schema", "_DummyConn"),
        ("exit", repo._db_path),
    ]
