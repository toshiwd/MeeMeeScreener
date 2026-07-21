from __future__ import annotations

import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


AXIS_ID = "tradex_meemee_update_client_v1"
DEFAULT_BASE_URL = "http://127.0.0.1:28888"


def _request_json(method: str, url: str, *, timeout: float = 10.0) -> dict[str, Any]:
    request = Request(url, method=method, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8-sig")
            return json.loads(text) if text else {}
    except HTTPError as exc:
        text = exc.read().decode("utf-8-sig", errors="replace")
        payload = json.loads(text) if text else {}
        payload.setdefault("http_status", exc.code)
        payload.setdefault("error", str(exc))
        return payload
    except URLError as exc:
        return {
            "ok": False,
            "error": "meemee_backend_unreachable",
            "message": str(exc.reason),
            "url": url,
        }


def submit_txt_update(
    *,
    base_url: str = DEFAULT_BASE_URL,
    completion_mode: str = "full",
    run_yahoo_daily_ingest: bool = True,
    timeout: float = 10.0,
) -> dict[str, Any]:
    params = urlencode(
        {
            "completion_mode": completion_mode,
            "run_yahoo_daily_ingest": str(run_yahoo_daily_ingest).lower(),
        }
    )
    url = f"{base_url.rstrip('/')}/api/jobs/txt-update?{params}"
    payload = _request_json("POST", url, timeout=timeout)
    job_id = payload.get("job_id") or payload.get("jobId")
    return {
        "axis_id": AXIS_ID,
        "operation": "submit_txt_update",
        "base_url": base_url,
        "endpoint": "/api/jobs/txt-update",
        "ok": bool(payload.get("ok") or payload.get("started") or job_id),
        "job_id": job_id,
        "payload": payload,
    }


def get_job_status(*, job_id: str, base_url: str = DEFAULT_BASE_URL, timeout: float = 10.0) -> dict[str, Any]:
    return _request_json("GET", f"{base_url.rstrip('/')}/api/jobs/{job_id}", timeout=timeout)


def wait_for_job(
    *,
    job_id: str,
    base_url: str = DEFAULT_BASE_URL,
    wait_seconds: int = 0,
    poll_seconds: float = 5.0,
    timeout: float = 10.0,
) -> dict[str, Any]:
    if wait_seconds <= 0:
        return {
            "axis_id": AXIS_ID,
            "operation": "wait_for_job",
            "waited": False,
            "job_id": job_id,
            "status": None,
        }
    deadline = time.monotonic() + wait_seconds
    last_status: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last_status = get_job_status(job_id=job_id, base_url=base_url, timeout=timeout)
        status = str(last_status.get("status") or "").lower()
        if status in {"success", "failed", "cancelled", "canceled", "skipped"}:
            return {
                "axis_id": AXIS_ID,
                "operation": "wait_for_job",
                "waited": True,
                "job_id": job_id,
                "terminal": True,
                "status": status,
                "payload": last_status,
            }
        time.sleep(max(0.5, poll_seconds))
    return {
        "axis_id": AXIS_ID,
        "operation": "wait_for_job",
        "waited": True,
        "job_id": job_id,
        "terminal": False,
        "status": (last_status or {}).get("status"),
        "payload": last_status,
    }
