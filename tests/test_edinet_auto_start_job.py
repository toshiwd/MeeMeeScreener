from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.core import edinet_auto_start_job
from app.backend.edinetdb.repository import EdinetdbRepository


def _prepare_non_empty_edinet_db(db_path: Path) -> EdinetdbRepository:
    repo = EdinetdbRepository(db_path)
    repo.ensure_schema()
    repo.save_company_map(
        [
            {
                "sec_code": "1301",
                "edinet_code": "E1301",
                "name": "Test Co",
                "industry": "Food",
            }
        ]
    )
    repo.upsert_financials(
        "E1301",
        {"items": [{"fiscal_year": "2025", "accounting_standard": "JP GAAP", "revenue": 1000}]},
    )
    repo.upsert_ratios(
        "E1301",
        {"items": [{"fiscal_year": "2025", "accounting_standard": "JP GAAP", "roe": 0.12}]},
    )
    return repo


def test_auto_start_submits_backfill_for_empty_tables(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("EDINETDB_API_KEY", "test-key")
    monkeypatch.delenv("EDINETDB_API_KEYS", raising=False)

    calls: list[tuple[str, dict | None, bool]] = []

    def _submit(job_type: str, payload=None, unique: bool = False, **kwargs):
        calls.append((job_type, payload, unique))
        return "job-backfill"

    monkeypatch.setattr(edinet_auto_start_job.job_manager, "submit", _submit)

    result = edinet_auto_start_job.schedule_edinet_auto_start_if_needed(source="test")

    assert result["submitted"] is True
    assert result["mode"] == "backfill_700"
    assert result["jobId"] == "job-backfill"
    assert calls[0][0] == edinet_auto_start_job.EDINETDB_BACKFILL_700_JOB_TYPE
    repo = EdinetdbRepository(db_path)
    assert repo.get_meta("auto_start_last_mode") == "backfill_700"
    assert repo.get_meta("auto_start_last_jst_date") == result["jst_date"]


def test_auto_start_submits_daily_watch_for_non_empty_tables(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _prepare_non_empty_edinet_db(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("EDINETDB_API_KEY", "test-key")
    monkeypatch.delenv("EDINETDB_API_KEYS", raising=False)

    calls: list[str] = []

    def _submit(job_type: str, payload=None, unique: bool = False, **kwargs):
        calls.append(job_type)
        return "job-daily"

    monkeypatch.setattr(edinet_auto_start_job.job_manager, "submit", _submit)

    result = edinet_auto_start_job.schedule_edinet_auto_start_if_needed(source="test")

    assert result["submitted"] is True
    assert result["mode"] == "daily_watch"
    assert calls == [edinet_auto_start_job.EDINETDB_DAILY_WATCH_JOB_TYPE]


def test_auto_start_skips_same_mode_twice_in_same_jst_day(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _prepare_non_empty_edinet_db(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("EDINETDB_API_KEY", "test-key")
    monkeypatch.delenv("EDINETDB_API_KEYS", raising=False)

    calls: list[str] = []

    def _submit(job_type: str, payload=None, unique: bool = False, **kwargs):
        calls.append(job_type)
        return f"job-{len(calls)}"

    monkeypatch.setattr(edinet_auto_start_job.job_manager, "submit", _submit)

    first = edinet_auto_start_job.schedule_edinet_auto_start_if_needed(source="test")
    second = edinet_auto_start_job.schedule_edinet_auto_start_if_needed(source="test")

    assert first["submitted"] is True
    assert second["submitted"] is False
    assert second["reason"] == "already_submitted_today"
    assert calls == [edinet_auto_start_job.EDINETDB_DAILY_WATCH_JOB_TYPE]


def test_auto_start_skips_without_api_key(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.delenv("EDINETDB_API_KEY", raising=False)
    monkeypatch.delenv("EDINETDB_API_KEYS", raising=False)

    def _fail_submit(*args, **kwargs):
        raise AssertionError("submit should not be called without API keys")

    monkeypatch.setattr(edinet_auto_start_job.job_manager, "submit", _fail_submit)

    result = edinet_auto_start_job.schedule_edinet_auto_start_if_needed(source="test")

    assert result["submitted"] is False
    assert result["reason"] == "no_api_keys"
