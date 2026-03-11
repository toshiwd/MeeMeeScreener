from __future__ import annotations

import duckdb
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.repository import EdinetdbRepository


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
