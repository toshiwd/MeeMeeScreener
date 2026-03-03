from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.backend.edinetdb.client import ApiError, EdinetdbClient, RateLimitStop, RetryableApiError
from app.backend.edinetdb.config import EdinetdbConfig, load_config
from app.backend.edinetdb.keyring import KeyRingClient
from app.backend.edinetdb.raw_store import write_raw_gzip
from app.backend.edinetdb.repository import EdinetdbRepository, TaskRow
from app.backend.edinetdb.targets import (
    load_code_txt_codes,
    load_favorites_codes,
    load_holdings_codes,
    load_ranking_codes,
    normalize_sec_code,
)
from app.core.config import FAVORITES_DB_PATH

JST = ZoneInfo("Asia/Tokyo")

EP_COMPANY_DETAIL = "companies_detail"
EP_FINANCIALS = "companies_financials"
EP_RATIOS = "companies_ratios"
EP_TEXT_BLOCKS = "companies_text-blocks"
EP_ANALYSIS = "companies_analysis"

META_TEXT_YEAR_SUPPORT = "text_blocks_year_support"


class BudgetLimitReached(RuntimeError):
    pass


class PhaseBudgetReached(RuntimeError):
    pass


@dataclass
class TaskResult:
    outcome: str
    http_status: int | None = None
    changed: bool = False
    inserted: int = 0


class DailyBudget:
    def __init__(self, total: int):
        self.total = max(0, int(total))
        self.used = 0

    @property
    def remaining(self) -> int:
        return max(0, self.total - self.used)

    def consume_or_raise(self) -> None:
        if self.used >= self.total:
            raise BudgetLimitReached("daily_budget_exhausted")
        self.used += 1


class PhaseBudget:
    def __init__(self, *, daily_budget: DailyBudget, cap: int):
        self._daily_budget = daily_budget
        self._cap = max(0, int(cap))
        self.used = 0

    def consume_or_raise(self) -> None:
        if self.used >= self._cap:
            raise PhaseBudgetReached("phase_budget_exhausted")
        self._daily_budget.consume_or_raise()
        self.used += 1


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _utc_now_naive() -> datetime:
    return _utc_now().replace(tzinfo=None)


def _next_day_jst_start_utc_naive(now_utc: datetime | None = None) -> datetime:
    now_utc = now_utc or _utc_now()
    now_jst = now_utc.astimezone(JST)
    next_jst = datetime.combine(now_jst.date() + timedelta(days=1), time(0, 0), tzinfo=JST)
    return next_jst.astimezone(timezone.utc).replace(tzinfo=None)


def _next_day_jst_start_iso(now_jst: datetime | None = None) -> str:
    current = now_jst.astimezone(JST) if now_jst is not None else datetime.now(tz=JST)
    nxt = datetime.combine(current.date() + timedelta(days=1), time(0, 0), tzinfo=JST)
    return nxt.isoformat()


def _hash_json(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _find_first(value: Any, keys: tuple[str, ...]) -> Any | None:
    if isinstance(value, dict):
        for key in keys:
            if key in value and value[key] is not None:
                return value[key]
        for child in value.values():
            found = _find_first(child, keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_first(child, keys)
            if found is not None:
                return found
    return None


def _to_year_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_company_rows(payload: Any) -> list[dict[str, Any]]:
    items: list[Any] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for key in ("companies", "items", "data", "results"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                items = candidate
                break
        if not items:
            for candidate in payload.values():
                if isinstance(candidate, list):
                    items = candidate
                    break
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sec_code = normalize_sec_code(
            item.get("sec_code")
            or item.get("security_code")
            or item.get("securities_code")
            or item.get("code")
        )
        edinet_code = str(
            item.get("edinet_code")
            or item.get("edinetCode")
            or item.get("edinet")
            or ""
        ).strip()
        if not sec_code or not edinet_code:
            continue
        rows.append(
            {
                "sec_code": sec_code,
                "edinet_code": edinet_code,
                "name": item.get("name") or item.get("company_name"),
                "industry": item.get("industry"),
                "updated_at": _utc_now_naive(),
            }
        )
    return rows


def _company_detail_to_map_row(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    sec_code = normalize_sec_code(
        payload.get("sec_code")
        or payload.get("security_code")
        or payload.get("securities_code")
        or payload.get("code")
    )
    edinet_code = str(
        payload.get("edinet_code")
        or payload.get("edinetCode")
        or payload.get("edinet")
        or ""
    ).strip()
    if not sec_code or not edinet_code:
        return None
    return {
        "sec_code": sec_code,
        "edinet_code": edinet_code,
        "name": payload.get("name") or payload.get("company_name"),
        "industry": payload.get("industry"),
        "updated_at": _utc_now_naive(),
    }


def _extract_latest_fiscal_year(payload: Any) -> str | None:
    raw = _find_first(
        payload,
        (
            "latest_fiscal_year",
            "latestFiscalYear",
            "fiscal_year",
            "fiscalYear",
            "latest_year",
        ),
    )
    return _to_year_text(raw)


def _call_api(
    *,
    client: Any,
    repo: EdinetdbRepository,
    cfg: EdinetdbConfig,
    job_name: str,
    endpoint: str,
    edinet_code: str | None,
    path: str,
    params: dict[str, Any] | None,
) -> tuple[int, Any]:
    response = client.get_json(path, params or {})
    now = _utc_now_naive()
    write_raw_gzip(
        raw_root=Path(cfg.raw_dir),
        endpoint=endpoint,
        edinet_code=edinet_code,
        payload={
            "fetched_at": now.isoformat(),
            "status": response.status,
            "request_path": path,
            "params": params or {},
            "response": response.payload,
        },
    )
    repo.log_api_call(
        job_name=job_name,
        endpoint=endpoint,
        edinet_code=edinet_code,
        http_status=response.status,
        error_type=None,
        called_at=now,
        jst_date=cfg.now_jst.date(),
    )
    return response.status, response.payload


def _build_client(
    *,
    cfg: EdinetdbConfig,
    on_attempt,
    max_retries: int = 3,
) -> Any:
    clients = [
        EdinetdbClient(
            api_key=key,
            base_url=cfg.base_url,
            timeout_sec=cfg.timeout_sec,
            max_retries=max_retries,
            on_attempt=on_attempt,
        )
        for key in cfg.api_keys
    ]
    if len(clients) == 1:
        return clients[0]
    return KeyRingClient(clients)


def _enqueue_full_fetch_tasks(
    repo: EdinetdbRepository,
    *,
    job_name: str,
    phase: str,
    edinet_codes: list[str],
    include_analysis: bool,
    force: bool,
    base_priority: int,
) -> int:
    tasks: list[dict[str, Any]] = []
    for edinet_code in edinet_codes:
        tasks.extend(
            [
                {
                    "job_name": job_name,
                    "phase": phase,
                    "edinet_code": edinet_code,
                    "endpoint": EP_COMPANY_DETAIL,
                    "params": {},
                    "priority": base_priority,
                },
                {
                    "job_name": job_name,
                    "phase": phase,
                    "edinet_code": edinet_code,
                    "endpoint": EP_FINANCIALS,
                    "params": {"years": 6},
                    "priority": base_priority - 1,
                },
                {
                    "job_name": job_name,
                    "phase": phase,
                    "edinet_code": edinet_code,
                    "endpoint": EP_RATIOS,
                    "params": {"years": 6},
                    "priority": base_priority - 2,
                },
                {
                    "job_name": job_name,
                    "phase": phase,
                    "edinet_code": edinet_code,
                    "endpoint": EP_TEXT_BLOCKS,
                    "params": {"mode": "auto"},
                    "priority": base_priority - 3,
                },
            ]
        )
        if include_analysis:
            tasks.append(
                {
                    "job_name": job_name,
                    "phase": phase,
                    "edinet_code": edinet_code,
                    "endpoint": EP_ANALYSIS,
                    "params": {},
                    "priority": base_priority - 9,
                }
            )
    repo.enqueue_tasks_bulk(tasks, force=force)
    return len(tasks)


def _enqueue_backfill_core_tasks(
    repo: EdinetdbRepository,
    *,
    job_name: str,
    edinet_codes: list[str],
    force: bool = False,
) -> int:
    tasks: list[dict[str, Any]] = []
    start = 1_000_000
    for idx, edinet_code in enumerate(edinet_codes):
        base = start - (idx * 10)
        tasks.extend(
            [
                {
                    "job_name": job_name,
                    "phase": "backfill_core",
                    "edinet_code": edinet_code,
                    "endpoint": EP_COMPANY_DETAIL,
                    "params": {},
                    "priority": base,
                },
                {
                    "job_name": job_name,
                    "phase": "backfill_core",
                    "edinet_code": edinet_code,
                    "endpoint": EP_FINANCIALS,
                    "params": {"years": 6},
                    "priority": base - 1,
                },
                {
                    "job_name": job_name,
                    "phase": "backfill_core",
                    "edinet_code": edinet_code,
                    "endpoint": EP_RATIOS,
                    "params": {"years": 6},
                    "priority": base - 2,
                },
            ]
        )
    repo.enqueue_tasks_bulk(tasks, force=force)
    return len(tasks)


def _enqueue_backfill_text_tasks(
    repo: EdinetdbRepository,
    *,
    job_name: str,
    edinet_codes: list[str],
    force: bool = False,
) -> int:
    tasks: list[dict[str, Any]] = []
    start = 500_000
    for idx, edinet_code in enumerate(edinet_codes):
        tasks.append(
            {
                "job_name": job_name,
                "phase": "backfill_text",
                "edinet_code": edinet_code,
                "endpoint": EP_TEXT_BLOCKS,
                "params": {"mode": "auto"},
                "priority": start - idx,
            }
        )
    repo.enqueue_tasks_bulk(tasks, force=force)
    return len(tasks)


def _enqueue_backfill_analysis_tasks(
    repo: EdinetdbRepository,
    *,
    job_name: str,
    edinet_codes: list[str],
    force: bool = False,
) -> int:
    tasks: list[dict[str, Any]] = []
    start = 300_000
    for idx, edinet_code in enumerate(edinet_codes):
        tasks.append(
            {
                "job_name": job_name,
                "phase": "backfill_analysis",
                "edinet_code": edinet_code,
                "endpoint": EP_ANALYSIS,
                "params": {},
                "priority": start - idx,
            }
        )
    repo.enqueue_tasks_bulk(tasks, force=force)
    return len(tasks)


def _process_text_blocks_task(
    *,
    repo: EdinetdbRepository,
    client: EdinetdbClient,
    cfg: EdinetdbConfig,
    job_name: str,
    task: TaskRow,
) -> TaskResult:
    path = f"/companies/{task.edinet_code}/text-blocks"
    mode = str(task.params.get("mode") or "auto")
    support = repo.get_meta(META_TEXT_YEAR_SUPPORT)
    current_year = cfg.now_jst.year
    latest = repo.get_company_latest(task.edinet_code)
    latest_year = int(str(latest.get("latest_fiscal_year")).strip()) if latest and str(latest.get("latest_fiscal_year", "")).strip().isdigit() else current_year

    if mode == "auto":
        if isinstance(support, dict) and support.get("supported") is True and support.get("param"):
            year_param = str(support.get("param"))
            for offset in range(cfg.text_years_max):
                year = latest_year - offset
                repo.enqueue_task(
                    job_name=job_name,
                    phase=task.phase,
                    edinet_code=task.edinet_code,
                    endpoint=EP_TEXT_BLOCKS,
                    params={"mode": "year", "year_param": year_param, "year": year},
                    priority=task.priority,
                    force=False,
                )
            repo.mark_skipped(task.task_key, reason="seeded_text_year_tasks")
            return TaskResult(outcome="skipped")

        if isinstance(support, dict) and support.get("supported") is False:
            try:
                status, payload = _call_api(
                    client=client,
                    repo=repo,
                    cfg=cfg,
                    job_name=job_name,
                    endpoint=EP_TEXT_BLOCKS,
                    edinet_code=task.edinet_code,
                    path=path,
                    params={},
                )
                inserted = repo.upsert_text_blocks(task.edinet_code, payload, fallback_year=None)
                repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
                return TaskResult(outcome="ok", http_status=status, inserted=inserted)
            except ApiError as exc:
                if exc.status == 404:
                    repo.mark_skipped(task.task_key, reason="text_blocks_not_found")
                    return TaskResult(outcome="skipped", http_status=exc.status)
                raise

        try:
            status, payload = _call_api(
                client=client,
                repo=repo,
                cfg=cfg,
                job_name=job_name,
                endpoint=EP_TEXT_BLOCKS,
                edinet_code=task.edinet_code,
                path=path,
                params={"year": latest_year},
            )
            repo.set_meta(META_TEXT_YEAR_SUPPORT, {"supported": True, "param": "year"})
            inserted = repo.upsert_text_blocks(task.edinet_code, payload, fallback_year=str(latest_year))
            for offset in range(1, cfg.text_years_max):
                year = latest_year - offset
                repo.enqueue_task(
                    job_name=job_name,
                    phase=task.phase,
                    edinet_code=task.edinet_code,
                    endpoint=EP_TEXT_BLOCKS,
                    params={"mode": "year", "year_param": "year", "year": year},
                    priority=task.priority,
                    force=False,
                )
            repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
            return TaskResult(outcome="ok", http_status=status, inserted=inserted)
        except ApiError as exc:
            if exc.status not in (400, 404):
                raise
            repo.log_api_call(
                job_name=job_name,
                endpoint=EP_TEXT_BLOCKS,
                edinet_code=task.edinet_code,
                http_status=exc.status,
                error_type="text_year_param_unsupported",
                called_at=_utc_now_naive(),
                jst_date=cfg.now_jst.date(),
            )
            repo.set_meta(META_TEXT_YEAR_SUPPORT, {"supported": False, "param": None})
            try:
                status, payload = _call_api(
                    client=client,
                    repo=repo,
                    cfg=cfg,
                    job_name=job_name,
                    endpoint=EP_TEXT_BLOCKS,
                    edinet_code=task.edinet_code,
                    path=path,
                    params={},
                )
                inserted = repo.upsert_text_blocks(task.edinet_code, payload, fallback_year=None)
                repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
                return TaskResult(outcome="ok", http_status=status, inserted=inserted)
            except ApiError as inner_exc:
                if inner_exc.status == 404:
                    repo.mark_skipped(task.task_key, reason="text_blocks_not_found")
                    return TaskResult(outcome="skipped", http_status=inner_exc.status)
                raise

    if mode == "year":
        year_param = str(task.params.get("year_param") or "year")
        year = int(task.params.get("year"))
        try:
            status, payload = _call_api(
                client=client,
                repo=repo,
                cfg=cfg,
                job_name=job_name,
                endpoint=EP_TEXT_BLOCKS,
                edinet_code=task.edinet_code,
                path=path,
                params={year_param: year},
            )
            inserted = repo.upsert_text_blocks(task.edinet_code, payload, fallback_year=str(year))
            repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
            return TaskResult(outcome="ok", http_status=status, inserted=inserted)
        except ApiError as exc:
            if exc.status == 404:
                repo.mark_skipped(task.task_key, reason="text_blocks_year_not_found")
                return TaskResult(outcome="skipped", http_status=exc.status)
            raise

    try:
        status, payload = _call_api(
            client=client,
            repo=repo,
            cfg=cfg,
            job_name=job_name,
            endpoint=EP_TEXT_BLOCKS,
            edinet_code=task.edinet_code,
            path=path,
            params={},
        )
        inserted = repo.upsert_text_blocks(task.edinet_code, payload, fallback_year=None)
        repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
        return TaskResult(outcome="ok", http_status=status, inserted=inserted)
    except ApiError as exc:
        if exc.status == 404:
            repo.mark_skipped(task.task_key, reason="text_blocks_not_found")
            return TaskResult(outcome="skipped", http_status=exc.status)
        raise


def _process_one_task(
    *,
    repo: EdinetdbRepository,
    client: EdinetdbClient,
    cfg: EdinetdbConfig,
    task: TaskRow,
) -> TaskResult:
    try:
        if task.endpoint == EP_COMPANY_DETAIL:
            path = f"/companies/{task.edinet_code}"
            previous = repo.get_company_latest(task.edinet_code)
            status, payload = _call_api(
                client=client,
                repo=repo,
                cfg=cfg,
                job_name=task.job_name,
                endpoint=EP_COMPANY_DETAIL,
                edinet_code=task.edinet_code,
                path=path,
                params={},
            )
            latest_hash = _hash_json(payload)
            latest_fiscal_year = _extract_latest_fiscal_year(payload)
            repo.save_company_latest(
                task.edinet_code,
                latest_fiscal_year=latest_fiscal_year,
                latest_hash=latest_hash,
                fetched_at=_utc_now_naive(),
                last_checked_at=_utc_now_naive(),
            )
            map_row = _company_detail_to_map_row(payload)
            if map_row:
                repo.save_company_map([map_row])
            changed = (
                previous is None
                or previous.get("latest_hash") != latest_hash
                or str(previous.get("latest_fiscal_year") or "") != str(latest_fiscal_year or "")
            )
            if task.job_name == "daily_watch" and bool(task.params.get("check_only")) and changed:
                _enqueue_full_fetch_tasks(
                    repo,
                    job_name="daily_watch",
                    phase="update_heavy",
                    edinet_codes=[task.edinet_code],
                    include_analysis=False,
                    force=True,
                    base_priority=340,
                )
            repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
            return TaskResult(outcome="ok", http_status=status, changed=changed)

        if task.endpoint == EP_FINANCIALS:
            path = f"/companies/{task.edinet_code}/financials"
            status, payload = _call_api(
                client=client,
                repo=repo,
                cfg=cfg,
                job_name=task.job_name,
                endpoint=EP_FINANCIALS,
                edinet_code=task.edinet_code,
                path=path,
                params={"years": int(task.params.get("years") or 6)},
            )
            inserted = repo.upsert_financials(task.edinet_code, payload)
            repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
            return TaskResult(outcome="ok", http_status=status, inserted=inserted)

        if task.endpoint == EP_RATIOS:
            path = f"/companies/{task.edinet_code}/ratios"
            status, payload = _call_api(
                client=client,
                repo=repo,
                cfg=cfg,
                job_name=task.job_name,
                endpoint=EP_RATIOS,
                edinet_code=task.edinet_code,
                path=path,
                params={"years": int(task.params.get("years") or 6)},
            )
            inserted = repo.upsert_ratios(task.edinet_code, payload)
            repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
            return TaskResult(outcome="ok", http_status=status, inserted=inserted)

        if task.endpoint == EP_TEXT_BLOCKS:
            return _process_text_blocks_task(repo=repo, client=client, cfg=cfg, job_name=task.job_name, task=task)

        if task.endpoint == EP_ANALYSIS:
            path = f"/companies/{task.edinet_code}/analysis"
            status, payload = _call_api(
                client=client,
                repo=repo,
                cfg=cfg,
                job_name=task.job_name,
                endpoint=EP_ANALYSIS,
                edinet_code=task.edinet_code,
                path=path,
                params={},
            )
            inserted = repo.upsert_analysis(task.edinet_code, payload)
            repo.mark_ok(task.task_key, http_status=status, fetched_at=_utc_now_naive())
            return TaskResult(outcome="ok", http_status=status, inserted=inserted)

        repo.mark_skipped(task.task_key, reason=f"unknown_endpoint:{task.endpoint}")
        return TaskResult(outcome="skipped")

    except PhaseBudgetReached:
        return TaskResult(outcome="phase_budget_stop")
    except BudgetLimitReached:
        return TaskResult(outcome="budget_stop")
    except RateLimitStop as exc:
        retry_at = _next_day_jst_start_utc_naive()
        repo.mark_retry_wait(
            task.task_key,
            error="rate_limited",
            retry_at=retry_at,
            http_status=exc.status,
        )
        repo.log_api_call(
            job_name=task.job_name,
            endpoint=task.endpoint,
            edinet_code=task.edinet_code,
            http_status=exc.status,
            error_type="rate_limit",
            called_at=_utc_now_naive(),
            jst_date=cfg.now_jst.date(),
        )
        return TaskResult(outcome="rate_limit", http_status=exc.status)
    except RetryableApiError as exc:
        wait_minutes = min(60, 2 ** min(task.tries, 6))
        retry_at = _utc_now_naive() + timedelta(minutes=wait_minutes)
        repo.mark_retry_wait(
            task.task_key,
            error=str(exc),
            retry_at=retry_at,
            http_status=exc.status,
        )
        repo.log_api_call(
            job_name=task.job_name,
            endpoint=task.endpoint,
            edinet_code=task.edinet_code,
            http_status=exc.status,
            error_type="retryable_error",
            called_at=_utc_now_naive(),
            jst_date=cfg.now_jst.date(),
        )
        return TaskResult(outcome="retry", http_status=exc.status)
    except ApiError as exc:
        if exc.status == 429:
            retry_at = _next_day_jst_start_utc_naive()
            repo.mark_retry_wait(
                task.task_key,
                error="rate_limited",
                retry_at=retry_at,
                http_status=exc.status,
            )
            return TaskResult(outcome="rate_limit", http_status=exc.status)
        if exc.status is not None and 400 <= exc.status < 500:
            repo.mark_failed(task.task_key, error=str(exc), http_status=exc.status)
            repo.log_api_call(
                job_name=task.job_name,
                endpoint=task.endpoint,
                edinet_code=task.edinet_code,
                http_status=exc.status,
                error_type="client_error",
                called_at=_utc_now_naive(),
                jst_date=cfg.now_jst.date(),
            )
            return TaskResult(outcome="failed", http_status=exc.status)
        retry_at = _utc_now_naive() + timedelta(minutes=15)
        repo.mark_retry_wait(task.task_key, error=str(exc), retry_at=retry_at, http_status=exc.status)
        repo.log_api_call(
            job_name=task.job_name,
            endpoint=task.endpoint,
            edinet_code=task.edinet_code,
            http_status=exc.status,
            error_type="api_error",
            called_at=_utc_now_naive(),
            jst_date=cfg.now_jst.date(),
        )
        return TaskResult(outcome="retry", http_status=exc.status)
    except Exception as exc:
        retry_at = _utc_now_naive() + timedelta(minutes=15)
        repo.mark_retry_wait(task.task_key, error=str(exc), retry_at=retry_at, http_status=None)
        repo.log_api_call(
            job_name=task.job_name,
            endpoint=task.endpoint,
            edinet_code=task.edinet_code,
            http_status=None,
            error_type="unexpected_error",
            called_at=_utc_now_naive(),
            jst_date=cfg.now_jst.date(),
        )
        return TaskResult(outcome="retry")


def _run_task_loop(
    *,
    repo: EdinetdbRepository,
    client: EdinetdbClient,
    cfg: EdinetdbConfig,
    job_name: str,
    phase: str | None = None,
) -> dict[str, Any]:
    counters = {"ok": 0, "failed": 0, "retry": 0, "skipped": 0, "changed": 0}
    stop_reason = "completed"
    while True:
        task = repo.next_runnable_task(job_name, phase=phase, now=_utc_now_naive())
        if task is None:
            break
        result = _process_one_task(repo=repo, client=client, cfg=cfg, task=task)
        if result.outcome == "ok":
            counters["ok"] += 1
            if result.changed:
                counters["changed"] += 1
            continue
        if result.outcome == "failed":
            counters["failed"] += 1
            continue
        if result.outcome == "retry":
            counters["retry"] += 1
            continue
        if result.outcome == "skipped":
            counters["skipped"] += 1
            continue
        if result.outcome in ("phase_budget_stop", "budget_stop"):
            stop_reason = result.outcome
            break
        if result.outcome == "rate_limit":
            stop_reason = "rate_limit"
            break
        stop_reason = result.outcome
        break
    return {"counters": counters, "stop_reason": stop_reason}


def _refresh_company_map(
    *,
    repo: EdinetdbRepository,
    client: EdinetdbClient,
    cfg: EdinetdbConfig,
    job_name: str,
) -> int:
    _status, payload = _call_api(
        client=client,
        repo=repo,
        cfg=cfg,
        job_name=job_name,
        endpoint="companies",
        edinet_code=None,
        path="/companies",
        params={"per_page": 5000},
    )
    rows = _extract_company_rows(payload)
    if rows:
        repo.save_company_map(rows)
    return len(rows)


def _build_summary(
    *,
    job_name: str,
    cfg: EdinetdbConfig,
    budget: DailyBudget,
    repo: EdinetdbRepository,
    extra: dict[str, Any],
) -> dict[str, Any]:
    status_counts = repo.count_tasks(job_name)
    pending = repo.pending_task_count(job_name)
    summary = {
        "job": job_name,
        "budget_total": cfg.daily_budget,
        "budget_used": budget.used,
        "budget_remaining": budget.remaining,
        "pending_tasks": pending,
        "status_counts": status_counts,
    }
    summary.update(extra)
    return summary


def _stable_bucket(sec_code: str, buckets: int) -> int:
    digest = hashlib.sha1(sec_code.encode("utf-8")).hexdigest()
    return int(digest, 16) % max(1, buckets)


def _dedup_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def run_backfill_700(cfg: EdinetdbConfig | None = None) -> dict[str, Any]:
    cfg = cfg or load_config()
    repo = EdinetdbRepository(cfg.db_path)
    budget = DailyBudget(cfg.daily_budget)
    if not cfg.api_keys:
        return {"job": "backfill_700", "skipped": True, "reason": "EDINETDB_API_KEY(S) is not set"}

    client = _build_client(cfg=cfg, on_attempt=budget.consume_or_raise, max_retries=3)
    repo.migrate_legacy_backfill_phase("backfill_700")

    map_rows = 0
    stop_reason = "completed"
    try:
        map_rows = _refresh_company_map(repo=repo, client=client, cfg=cfg, job_name="backfill_700")
    except PhaseBudgetReached:
        stop_reason = "budget_stop"
    except BudgetLimitReached:
        stop_reason = "budget_stop"
    except RateLimitStop:
        stop_reason = "rate_limit"
    except Exception:
        stop_reason = "map_refresh_failed"

    code_txt_codes = load_code_txt_codes()
    mapped = repo.lookup_edinet_codes(code_txt_codes)
    for sec_code in code_txt_codes:
        if sec_code not in mapped:
            repo.upsert_unmapped_code(sec_code, "no_edinet_mapping")

    edinet_codes = sorted(set(mapped.values()))
    queued_core = _enqueue_backfill_core_tasks(
        repo,
        job_name="backfill_700",
        edinet_codes=edinet_codes,
        force=False,
    )
    queued_text = 0
    queued_analysis = 0
    counters = {
        "core": {"ok": 0, "failed": 0, "retry": 0, "skipped": 0, "changed": 0},
        "text": {"ok": 0, "failed": 0, "retry": 0, "skipped": 0, "changed": 0},
        "analysis": {"ok": 0, "failed": 0, "retry": 0, "skipped": 0, "changed": 0},
    }

    if stop_reason == "completed":
        core_loop = _run_task_loop(
            repo=repo,
            client=client,
            cfg=cfg,
            job_name="backfill_700",
            phase="backfill_core",
        )
        stop_reason = core_loop["stop_reason"]
        counters["core"] = core_loop["counters"]

    if stop_reason == "completed" and budget.remaining > 0:
        queued_text = _enqueue_backfill_text_tasks(
            repo,
            job_name="backfill_700",
            edinet_codes=edinet_codes,
            force=False,
        )
        text_loop = _run_task_loop(
            repo=repo,
            client=client,
            cfg=cfg,
            job_name="backfill_700",
            phase="backfill_text",
        )
        stop_reason = text_loop["stop_reason"]
        counters["text"] = text_loop["counters"]

    if stop_reason == "completed" and budget.remaining > 0 and cfg.daily_budget >= 1000:
        queued_analysis = _enqueue_backfill_analysis_tasks(
            repo,
            job_name="backfill_700",
            edinet_codes=edinet_codes,
            force=False,
        )
        analysis_loop = _run_task_loop(
            repo=repo,
            client=client,
            cfg=cfg,
            job_name="backfill_700",
            phase="backfill_analysis",
        )
        stop_reason = analysis_loop["stop_reason"]
        counters["analysis"] = analysis_loop["counters"]

    return _build_summary(
        job_name="backfill_700",
        cfg=cfg,
        budget=budget,
        repo=repo,
        extra={
            "stop_reason": stop_reason,
            "resume_after_jst": _next_day_jst_start_iso() if stop_reason == "rate_limit" else None,
            "code_txt_count": len(code_txt_codes),
            "mapped_count": len(edinet_codes),
            "company_map_rows": map_rows,
            "queued_tasks": {
                "core": queued_core,
                "text": queued_text,
                "analysis": queued_analysis,
            },
            "processed": counters,
        },
    )


def run_daily_watch(cfg: EdinetdbConfig | None = None) -> dict[str, Any]:
    cfg = cfg or load_config()
    repo = EdinetdbRepository(cfg.db_path)
    budget = DailyBudget(cfg.daily_budget)
    if not cfg.api_keys:
        return {"job": "daily_watch", "skipped": True, "reason": "EDINETDB_API_KEY(S) is not set"}

    holdings = load_holdings_codes(cfg.db_path)
    favorites = load_favorites_codes(FAVORITES_DB_PATH)
    ranking = load_ranking_codes(cfg.db_path, cfg.ranking_limit)
    p0 = _dedup_preserve_order(holdings + favorites)
    p1 = _dedup_preserve_order(ranking)
    target = _dedup_preserve_order(p0 + p1)

    global_client = _build_client(cfg=cfg, on_attempt=budget.consume_or_raise, max_retries=3)

    mapped = repo.lookup_edinet_codes(target)
    missing_map = [code for code in target if code not in mapped]
    map_refreshed = False
    if missing_map and budget.remaining > 0:
        try:
            _refresh_company_map(repo=repo, client=global_client, cfg=cfg, job_name="daily_watch")
            map_refreshed = True
        except Exception:
            map_refreshed = False
        mapped = repo.lookup_edinet_codes(target)
        missing_map = [code for code in target if code not in mapped]
    for sec_code in missing_map:
        repo.upsert_unmapped_code(sec_code, "no_edinet_mapping")

    target_edinet = sorted(set(mapped.values()))
    latest_map = repo.get_company_latest_bulk(target_edinet)
    new_edinet = [code for code in target_edinet if code not in latest_map]

    p0_edinet = {mapped[sec] for sec in p0 if sec in mapped}
    ranking_only = [sec for sec in p1 if sec not in p0 and sec in mapped]
    day_bucket = cfg.now_jst.date().toordinal() % max(1, cfg.rotation_buckets)
    ranking_rotated = [sec for sec in ranking_only if _stable_bucket(sec, cfg.rotation_buckets) == day_bucket]

    ordered_new = []
    for sec_code in p0 + ranking:
        edinet_code = mapped.get(sec_code)
        if not edinet_code:
            continue
        if edinet_code in new_edinet and edinet_code not in ordered_new:
            ordered_new.append(edinet_code)

    _enqueue_full_fetch_tasks(
        repo,
        job_name="daily_watch",
        phase="new",
        edinet_codes=ordered_new,
        include_analysis=False,
        force=False,
        base_priority=500,
    )

    for sec_code in p0:
        edinet_code = mapped.get(sec_code)
        if not edinet_code or edinet_code in new_edinet:
            continue
        repo.enqueue_task(
            job_name="daily_watch",
            phase="check",
            edinet_code=edinet_code,
            endpoint=EP_COMPANY_DETAIL,
            params={"check_only": True, "priority_group": "p0"},
            priority=380,
            force=True,
        )
    for sec_code in ranking_rotated:
        edinet_code = mapped.get(sec_code)
        if not edinet_code or edinet_code in new_edinet:
            continue
        repo.enqueue_task(
            job_name="daily_watch",
            phase="check",
            edinet_code=edinet_code,
            endpoint=EP_COMPANY_DETAIL,
            params={"check_only": True, "priority_group": "p1"},
            priority=300,
            force=True,
        )

    new_cap = max(1, int(cfg.daily_budget * 0.7))
    check_cap = max(0, cfg.daily_budget - new_cap)

    new_phase_budget = PhaseBudget(daily_budget=budget, cap=new_cap)
    new_client = _build_client(cfg=cfg, on_attempt=new_phase_budget.consume_or_raise, max_retries=3)
    new_phase = _run_task_loop(
        repo=repo,
        client=new_client,
        cfg=cfg,
        job_name="daily_watch",
        phase="new",
    )

    stop_reason = new_phase["stop_reason"]
    check_phase = {"counters": {"ok": 0, "failed": 0, "retry": 0, "skipped": 0, "changed": 0}, "stop_reason": "not_started"}
    heavy_phase = {"counters": {"ok": 0, "failed": 0, "retry": 0, "skipped": 0, "changed": 0}, "stop_reason": "not_started"}

    if stop_reason not in ("rate_limit", "budget_stop"):
        check_phase_budget = PhaseBudget(daily_budget=budget, cap=check_cap)
        check_client = _build_client(cfg=cfg, on_attempt=check_phase_budget.consume_or_raise, max_retries=3)
        check_phase = _run_task_loop(
            repo=repo,
            client=check_client,
            cfg=cfg,
            job_name="daily_watch",
            phase="check",
        )
        stop_reason = check_phase["stop_reason"]

    if stop_reason not in ("rate_limit", "budget_stop"):
        heavy_client = _build_client(cfg=cfg, on_attempt=budget.consume_or_raise, max_retries=3)
        heavy_phase = _run_task_loop(
            repo=repo,
            client=heavy_client,
            cfg=cfg,
            job_name="daily_watch",
            phase="update_heavy",
        )
        if heavy_phase["stop_reason"] != "completed":
            stop_reason = heavy_phase["stop_reason"]

    return _build_summary(
        job_name="daily_watch",
        cfg=cfg,
        budget=budget,
        repo=repo,
        extra={
            "stop_reason": stop_reason,
            "resume_after_jst": _next_day_jst_start_iso() if stop_reason == "rate_limit" else None,
            "targets": {
                "holdings": len(holdings),
                "favorites": len(favorites),
                "ranking": len(ranking),
                "target_total": len(target),
                "mapped": len(target_edinet),
                "new": len(new_edinet),
                "unmapped": len(missing_map),
                "p0_mapped": len(p0_edinet),
                "p1_rotated": len(ranking_rotated),
                "map_refreshed": map_refreshed,
            },
            "processed": {
                "new_phase": new_phase["counters"],
                "check_phase": check_phase["counters"],
                "update_heavy_phase": heavy_phase["counters"],
            },
            "phase_budget": {
                "new_cap": new_cap,
                "check_cap": check_cap,
            },
        },
    )
