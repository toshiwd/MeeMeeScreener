from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from app.backend.edinetdb.client import ApiError, RateLimitStop, RetryableApiError
from app.backend.edinetdb.config import EdinetdbConfig
from app.backend.edinetdb.repository import EdinetdbRepository
from app.backend.edinetdb.targets import normalize_sec_code
from app.backend.edinetdb.schema import utcnow_naive

_USER_AGENT = "MeeMee-EDINET/1.0"
_DOCUMENTS_ENDPOINT = "/documents.json"
_DOCUMENTS_ENDPOINT_NAME = "official_documents_list"


def _to_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_flag(value: object) -> int:
    text = _to_text(value).lower()
    return 1 if text in {"1", "true", "yes", "on"} else 0


def _extract_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("results", "documents", "items", "data"):
        candidate = payload.get(key)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
        if isinstance(candidate, dict):
            nested = _extract_list(candidate)
            if nested:
                return nested
    for candidate in payload.values():
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


def _normalize_document_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _extract_list(payload):
        doc_id = _to_text(item.get("docID") or item.get("docId") or item.get("doc_id"))
        edinet_code = _to_text(item.get("edinetCode") or item.get("edinet_code"))
        sec_code = normalize_sec_code(item.get("secCode") or item.get("sec_code"))
        if not doc_id or not edinet_code:
            continue
        rows.append(
            {
                "doc_id": doc_id,
                "sec_code": sec_code or "",
                "edinet_code": edinet_code,
                "filer_name": _to_text(item.get("filerName") or item.get("filer_name")) or None,
                "form_code": _to_text(item.get("formCode") or item.get("form_code")) or None,
                "doc_type_code": _to_text(item.get("docTypeCode") or item.get("doc_type_code")) or None,
                "period_start": _to_text(item.get("periodStart") or item.get("period_start")) or None,
                "period_end": _to_text(item.get("periodEnd") or item.get("period_end")) or None,
                "submit_datetime": _to_text(item.get("submitDateTime") or item.get("submit_date_time")) or None,
                "doc_description": _to_text(item.get("docDescription") or item.get("doc_description")) or None,
                "csv_flag": _to_flag(item.get("csvFlag") or item.get("csv_flag")),
                "pdf_flag": _to_flag(item.get("pdfFlag") or item.get("pdf_flag")),
                "xbrl_flag": _to_flag(item.get("xbrlFlag") or item.get("xbrl_flag")),
                "legal_status": _to_text(item.get("legalStatus") or item.get("legal_status")) or None,
                "payload": item,
            }
        )
    return rows


def _normalize_sec_code_filters(values: list[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in values or []:
        code = normalize_sec_code(value)
        if code:
            normalized.add(code)
    return normalized


def _normalize_edinet_code_filters(values: list[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in values or []:
        code = _to_text(value)
        if code:
            normalized.add(code)
    return normalized


def _filter_document_rows(
    rows: list[dict[str, Any]],
    *,
    sec_codes: set[str] | None = None,
    edinet_codes: set[str] | None = None,
) -> list[dict[str, Any]]:
    sec_filter = sec_codes or set()
    edinet_filter = edinet_codes or set()
    if not sec_filter and not edinet_filter:
        return list(rows)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        sec_code = _to_text(row.get("sec_code"))
        edinet_code = _to_text(row.get("edinet_code"))
        if sec_filter and sec_code in sec_filter:
            filtered.append(row)
            continue
        if edinet_filter and edinet_code in edinet_filter:
            filtered.append(row)
    return filtered


def _build_company_rows(doc_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "sec_code": row["sec_code"],
            "edinet_code": row["edinet_code"],
            "name": row["filer_name"],
            "industry": None,
        }
        for row in doc_rows
        if _to_text(row.get("sec_code"))
    ]


def _sync_official_documents(
    *,
    repo: EdinetdbRepository,
    cfg: EdinetdbConfig,
    job_name: str,
    dates: list[date],
    sec_codes: set[str] | None = None,
    edinet_codes: set[str] | None = None,
) -> dict[str, Any]:
    if not cfg.official_api_enabled:
        return {"enabled": False, "skipped": True, "reason": "official_api_disabled"}
    if not cfg.official_api_key:
        return {"enabled": True, "skipped": True, "reason": "official_api_key_missing"}

    client = OfficialEdinetApiClient(
        api_key=cfg.official_api_key,
        base_url=cfg.official_api_base_url,
        timeout_sec=cfg.timeout_sec,
        max_retries=2,
    )
    total_docs = 0
    total_mapped = 0
    synced_dates: list[str] = []
    matched_dates: list[str] = []
    matched_sec_codes: set[str] = set()
    matched_edinet_codes: set[str] = set()

    for target_date in sorted(dates):
        response = client.get_json(_DOCUMENTS_ENDPOINT, {"date": target_date.isoformat(), "type": 2})
        repo.log_api_call(
            job_name=job_name,
            endpoint=_DOCUMENTS_ENDPOINT_NAME,
            edinet_code=None,
            http_status=response.status,
            error_type=None,
            jst_date=cfg.now_jst.date(),
        )
        doc_rows = _normalize_document_rows(response.payload)
        filtered_rows = _filter_document_rows(
            doc_rows,
            sec_codes=sec_codes,
            edinet_codes=edinet_codes,
        )
        if filtered_rows:
            matched_dates.append(target_date.isoformat())
            matched_sec_codes.update(
                _to_text(row.get("sec_code"))
                for row in filtered_rows
                if _to_text(row.get("sec_code"))
            )
            matched_edinet_codes.update(
                _to_text(row.get("edinet_code"))
                for row in filtered_rows
                if _to_text(row.get("edinet_code"))
            )
        total_docs += repo.upsert_official_documents(filtered_rows)
        company_rows = _build_company_rows(filtered_rows)
        repo.save_company_map(company_rows)
        total_mapped += len(company_rows)
        synced_dates.append(target_date.isoformat())

    return {
        "enabled": True,
        "skipped": False,
        "stop_reason": "completed",
        "synced_dates": synced_dates,
        "matched_dates": matched_dates,
        "documents": total_docs,
        "mapped_rows": total_mapped,
        "matched_sec_codes": sorted(matched_sec_codes),
        "matched_edinet_codes": sorted(matched_edinet_codes),
    }


@dataclass(frozen=True)
class OfficialApiResponse:
    status: int
    payload: Any
    url: str


class OfficialEdinetApiClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        timeout_sec: int = 20,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._max_retries = max(0, int(max_retries))

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> OfficialApiResponse:
        merged_params = dict(params or {})
        merged_params["Subscription-Key"] = self._api_key
        query = urllib.parse.urlencode({key: value for key, value in merged_params.items() if value is not None})
        relative_path = path if path.startswith("/") else f"/{path}"
        url = f"{self._base_url}{relative_path}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": _USER_AGENT,
            },
        )
        for attempt in range(self._max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                    payload = json.loads(raw) if raw else {}
                    return OfficialApiResponse(status=int(response.status), payload=payload, url=url)
            except urllib.error.HTTPError as exc:
                status = int(exc.code)
                body = exc.read().decode("utf-8", errors="replace")
                if status == 429:
                    raise RateLimitStop("official_rate_limited", status=status, body=body) from exc
                if 500 <= status < 600:
                    if attempt < self._max_retries:
                        time.sleep(2**attempt)
                        continue
                    raise RetryableApiError("official_server_error", status=status, body=body) from exc
                raise ApiError("official_client_error", status=status, body=body) from exc
            except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
                if attempt < self._max_retries:
                    time.sleep(2**attempt)
                    continue
                raise RetryableApiError(f"official_network_error:{exc}", status=None, body=None) from exc
            except json.JSONDecodeError as exc:
                raise ApiError("official_invalid_json_response", status=None, body=None) from exc
        raise RetryableApiError("official_unexpected_retry_exhaustion", status=None, body=None)


def sync_recent_official_documents(
    *,
    repo: EdinetdbRepository,
    cfg: EdinetdbConfig,
    job_name: str,
    days: int | None = None,
) -> dict[str, Any]:
    lookback_days = max(1, int(days or cfg.official_recent_days))
    dates = [cfg.now_jst.date() - timedelta(days=offset) for offset in range(lookback_days)]
    summary = _sync_official_documents(
        repo=repo,
        cfg=cfg,
        job_name=job_name,
        dates=dates,
    )
    if summary.get("skipped"):
        return {
            "lookback_days": lookback_days,
            **summary,
        }

    repo.set_meta(
        "official_documents_last_sync",
        {
            "job_name": job_name,
            "lookback_days": lookback_days,
            "synced_dates": summary.get("synced_dates") or [],
            "documents": summary.get("documents") or 0,
            "mapped_rows": summary.get("mapped_rows") or 0,
            "synced_at": cfg.now_jst.isoformat(),
        },
    )
    return {
        "lookback_days": lookback_days,
        **summary,
    }


def sync_official_documents_for_codes(
    *,
    repo: EdinetdbRepository,
    cfg: EdinetdbConfig,
    job_name: str,
    sec_codes: list[str] | None = None,
    edinet_codes: list[str] | None = None,
    days: int,
) -> dict[str, Any]:
    lookback_days = max(1, int(days))
    sec_code_filter = _normalize_sec_code_filters(sec_codes)
    edinet_code_filter = _normalize_edinet_code_filters(edinet_codes)
    if sec_code_filter:
        edinet_code_filter.update(repo.lookup_edinet_codes(sorted(sec_code_filter)).values())
    if not sec_code_filter and not edinet_code_filter:
        return {
            "enabled": False,
            "skipped": True,
            "reason": "target_codes_missing",
            "lookback_days": lookback_days,
        }

    dates = [cfg.now_jst.date() - timedelta(days=offset) for offset in range(lookback_days)]
    summary = _sync_official_documents(
        repo=repo,
        cfg=cfg,
        job_name=job_name,
        dates=dates,
        sec_codes=sec_code_filter,
        edinet_codes=edinet_code_filter,
    )
    if summary.get("skipped"):
        return {
            "lookback_days": lookback_days,
            "sec_codes": sorted(sec_code_filter),
            "edinet_codes": sorted(edinet_code_filter),
            **summary,
        }

    checked_at = utcnow_naive()
    checked_edinet_codes = set(edinet_code_filter)
    checked_edinet_codes.update(summary.get("matched_edinet_codes") or [])
    for edinet_code in sorted(checked_edinet_codes):
        if _to_text(edinet_code):
            repo.touch_company_last_checked(edinet_code, last_checked_at=checked_at)

    if len(sec_code_filter) == 1:
        target_code = next(iter(sec_code_filter))
        repo.set_meta(
            f"official_documents_last_code_sync:{target_code}",
            {
                "job_name": job_name,
                "lookback_days": lookback_days,
                "sec_code": target_code,
                "edinet_codes": sorted(checked_edinet_codes),
                "synced_dates": summary.get("synced_dates") or [],
                "matched_dates": summary.get("matched_dates") or [],
                "documents": summary.get("documents") or 0,
                "mapped_rows": summary.get("mapped_rows") or 0,
                "synced_at": cfg.now_jst.isoformat(),
            },
        )

    return {
        "lookback_days": lookback_days,
        "sec_codes": sorted(sec_code_filter),
        "edinet_codes": sorted(edinet_code_filter),
        **summary,
    }
