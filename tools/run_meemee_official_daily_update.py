from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

TERMINAL_STATUSES = {"success", "failed", "canceled", "skipped"}
SUCCESS_STATUSES = {"success"}


def _local_appdata() -> Path:
    return Path(os.getenv("LOCALAPPDATA") or str(Path.home()))


def _production_data_dir() -> Path:
    return _local_appdata() / "MeeMeeScreener" / "data"


def _production_db_path() -> Path:
    return _production_data_dir() / "stocks.duckdb"


def _development_db_path() -> Path:
    return _local_appdata() / "MeeMeeScreener-dev" / "data" / "stocks.duckdb"


def _configure_production_environment() -> None:
    data_dir = _production_data_dir()
    db_path = _production_db_path()
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "txt").mkdir(exist_ok=True)
    (data_dir / "logs").mkdir(exist_ok=True)
    os.environ["APP_ENV"] = "production"
    os.environ["ENV"] = "production"
    os.environ.pop("MEEMEE_DEV", None)
    os.environ.pop("MEEMEE_DEV_MODE", None)
    os.environ["MEEMEE_DATA_DIR"] = str(data_dir)
    os.environ["STOCKS_DB_PATH"] = str(db_path)
    os.environ.setdefault("MEEMEE_SYNC_UPDATED_DB_TO_LOCAL_PEER", "1")


def _confirmed_latest_and_yahoo_rows(db_path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {"path": str(db_path), "exists": db_path.exists()}
    if not db_path.exists():
        payload.update({"confirmed_latest": None, "yahoo_rows": None})
        return payload
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {str(row[0]) for row in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
        if "daily_bars" not in tables:
            payload.update({"confirmed_latest": None, "yahoo_rows": None, "daily_bars_missing": True})
            return payload
        payload["confirmed_latest"] = conn.execute(
            """
            SELECT MAX(
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END
            )
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
            """
        ).fetchone()[0]
        payload["yahoo_rows"] = int(conn.execute("SELECT COUNT(*) FROM daily_bars WHERE COALESCE(source, 'pan') = 'yahoo'").fetchone()[0] or 0)
        return payload
    finally:
        conn.close()


def _wait_for_jobs(job_manager: Any, job_ids: list[str], timeout_sec: int, poll_sec: int) -> dict[str, dict[str, Any]]:
    deadline = time.time() + timeout_sec
    last: dict[str, tuple[Any, ...]] = {}
    while time.time() < deadline:
        all_terminal = True
        for job_id in list(job_ids):
            status = job_manager.get_status(job_id) or job_manager.get_cached_status(job_id) or {}
            signature = (status.get("status"), status.get("progress"), status.get("message"), status.get("error"))
            if last.get(job_id) != signature:
                print("STATUS", job_id, json.dumps(status, ensure_ascii=False, default=str), flush=True)
                last[job_id] = signature
            if status.get("status") not in TERMINAL_STATUSES:
                all_terminal = False
        if all_terminal:
            return {job_id: (job_manager.get_status(job_id) or job_manager.get_cached_status(job_id) or {}) for job_id in job_ids}
        time.sleep(poll_sec)
    raise TimeoutError(f"Timed out waiting for jobs: {job_ids}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the official MeeMee daily PAN update against production DB, then sync confirmed-only dev DB.")
    parser.add_argument("--timeout-sec", type=int, default=7200)
    parser.add_argument("--poll-sec", type=int, default=15)
    parser.add_argument("--allow-missing-yahoo-followup", action="store_true")
    args = parser.parse_args()

    _configure_production_environment()

    import app.main  # noqa: F401 - registers canonical job handlers after production env is set
    from app.backend.api.routers.jobs import submit_txt_update_job
    from app.backend.core.jobs import job_manager
    from app.backend.services.dev_db_sync import sync_updated_stock_db_to_local_peer
    from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status

    prod_db = _production_db_path()
    dev_db = _development_db_path()
    before = {"production": _confirmed_latest_and_yahoo_rows(prod_db), "development": _confirmed_latest_and_yahoo_rows(dev_db)}

    request_payload = {
        "completion_mode": "full",
        "auto_ml_predict": True,
        "auto_ml_train": True,
        "force_recompute_on_pan_finalize": True,
        "allow_manifest_fast_noop": False,
        "auto_fill_missing_history": False,
        "source": "official_daily_update_1900",
    }
    response = submit_txt_update_job(request_payload, source="tools/run_meemee_official_daily_update.py")
    print("SUBMIT", json.dumps(response, ensure_ascii=False, default=str), flush=True)
    if not isinstance(response, dict) or not response.get("ok"):
        print(json.dumps({"ok": False, "stage": "submit", "response": response}, ensure_ascii=False, default=str), flush=True)
        return 2

    job_ids = [str(response["job_id"])]
    followup_id = response.get("yahoo_daily_ingest_followup_job_id")
    if followup_id:
        job_ids.append(str(followup_id))
    elif not args.allow_missing_yahoo_followup:
        print(json.dumps({"ok": False, "stage": "submit", "error": "yahoo_daily_ingest_followup_not_scheduled"}, ensure_ascii=False), flush=True)
        return 3

    final_jobs = _wait_for_jobs(job_manager, job_ids, args.timeout_sec, args.poll_sec)
    print("FINAL_JOBS", json.dumps(final_jobs, ensure_ascii=False, indent=2, default=str), flush=True)
    failed_jobs = {job_id: status for job_id, status in final_jobs.items() if status.get("status") not in SUCCESS_STATUSES}
    if failed_jobs:
        print(json.dumps({"ok": False, "stage": "job_status", "failed_jobs": failed_jobs}, ensure_ascii=False, default=str), flush=True)
        return 4

    sync_result = sync_updated_stock_db_to_local_peer(source_db_path=prod_db, target_db_path=dev_db)
    after = {"production": _confirmed_latest_and_yahoo_rows(prod_db), "development": _confirmed_latest_and_yahoo_rows(dev_db)}
    runtime_status = get_runtime_stock_db_status()
    ranking_freshness = {
        "up": get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=5),
        "down": get_rankings_freshness(tf="D", which="latest", direction="down", mode="trade", risk_mode="balanced", limit=5),
    }

    prod_latest = after["production"].get("confirmed_latest")
    dev_latest = after["development"].get("confirmed_latest")
    dev_yahoo_rows = after["development"].get("yahoo_rows")
    errors: list[str] = []
    if prod_latest is None:
        errors.append("production_confirmed_latest_missing")
    if dev_latest != prod_latest:
        errors.append(f"development_confirmed_latest_mismatch:prod={prod_latest}:dev={dev_latest}")
    if dev_yahoo_rows != 0:
        errors.append(f"development_yahoo_rows_not_zero:{dev_yahoo_rows}")

    report = {
        "ok": not errors,
        "run_finished_at": datetime.now().isoformat(),
        "before": before,
        "after": after,
        "sync_result": sync_result,
        "runtime_status": runtime_status,
        "ranking_freshness": ranking_freshness,
        "errors": errors,
    }
    print("VERIFY", json.dumps(report, ensure_ascii=False, indent=2, default=str), flush=True)
    return 0 if not errors else 5


if __name__ == "__main__":
    raise SystemExit(main())
