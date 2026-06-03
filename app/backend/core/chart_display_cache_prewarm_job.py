from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime
from typing import Any

from app.backend.api.dependencies import get_stock_repo
from app.backend.api.routers.bars import BatchBarsV3Request, batch_bars_v3
from app.backend.core.jobs import job_manager
from app.backend.edinetdb.targets import load_favorites_codes, load_holdings_codes, load_ranking_codes, normalize_sec_code
from app.backend.services.operator_mutation_lock import is_operator_mutation_active
from app.core.config import config

logger = logging.getLogger(__name__)

CHART_DISPLAY_CACHE_PREWARM_JOB_TYPE = "chart_display_cache_prewarm"
_SCHEDULER_LOCK = threading.Lock()
_SCHEDULER_THREAD: threading.Thread | None = None
_SCHEDULER_STOP_EVENT = threading.Event()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def _prewarm_enabled() -> bool:
    return _env_bool("MEEMEE_CHART_DISPLAY_PREWARM_ENABLED", True)


def _prewarm_startup_delay_sec() -> int:
    return _env_int("MEEMEE_CHART_DISPLAY_PREWARM_STARTUP_DELAY_SEC", 90, minimum=0)


def _prewarm_poll_sec() -> int:
    return _env_int("MEEMEE_CHART_DISPLAY_PREWARM_POLL_SEC", 3600, minimum=300)


def _prewarm_max_codes() -> int:
    return _env_int("MEEMEE_CHART_DISPLAY_PREWARM_MAX_CODES", 12, minimum=1, maximum=80)


def _prewarm_ranking_limit() -> int:
    return _env_int("MEEMEE_CHART_DISPLAY_PREWARM_RANKING_LIMIT", 12, minimum=1, maximum=80)


def _prewarm_per_code_delay_ms() -> int:
    return _env_int("MEEMEE_CHART_DISPLAY_PREWARM_PER_CODE_DELAY_MS", 150, minimum=0, maximum=5000)


def _explicit_prewarm_codes() -> list[str]:
    raw = str(os.getenv("MEEMEE_CHART_DISPLAY_PREWARM_CODES") or "").strip()
    if not raw:
        return []
    codes: list[str] = []
    for part in raw.replace(";", ",").split(","):
        code = normalize_sec_code(part)
        if code:
            codes.append(code)
    return codes


def collect_chart_display_prewarm_codes(*, max_codes: int | None = None) -> list[str]:
    limit = max(1, int(max_codes or _prewarm_max_codes()))
    candidates: list[str] = []
    candidates.extend(_explicit_prewarm_codes())
    try:
        candidates.extend(load_holdings_codes(config.DB_PATH))
    except Exception as exc:
        logger.debug("chart display prewarm holdings code load skipped: %s", exc)
    try:
        candidates.extend(load_favorites_codes())
    except Exception as exc:
        logger.debug("chart display prewarm favorites code load skipped: %s", exc)
    try:
        candidates.extend(load_ranking_codes(config.DB_PATH, _prewarm_ranking_limit()))
    except Exception as exc:
        logger.debug("chart display prewarm ranking code load skipped: %s", exc)

    seen: set[str] = set()
    out: list[str] = []
    for value in candidates:
        code = normalize_sec_code(value)
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
        if len(out) >= limit:
            break
    return out


def prewarm_chart_display_cache_for_codes(
    codes: list[str],
    *,
    source: str = "manual",
    per_code_delay_ms: int | None = None,
) -> dict[str, Any]:
    unique_codes: list[str] = []
    seen: set[str] = set()
    for value in codes:
        code = normalize_sec_code(value)
        if not code or code in seen:
            continue
        seen.add(code)
        unique_codes.append(code)
    if not unique_codes:
        return {"source": source, "requested": 0, "warmed": 0, "failed": 0, "items": []}
    if is_operator_mutation_active():
        return {
            "source": source,
            "requested": len(unique_codes),
            "warmed": 0,
            "failed": 0,
            "skipped": True,
            "reason": "operator_mutation_active",
            "items": [],
        }

    delay_ms = _prewarm_per_code_delay_ms() if per_code_delay_ms is None else max(0, int(per_code_delay_ms))
    repo = get_stock_repo()
    items: list[dict[str, Any]] = []
    warmed = 0
    failed = 0
    for code in unique_codes:
        started = time.perf_counter()
        try:
            response = batch_bars_v3(
                BatchBarsV3Request(
                    codes=[code],
                    timeframes=["daily", "weekly", "monthly"],
                    limit=2000,
                    timeframeLimits={"daily": 2000, "weekly": 520, "monthly": 180},
                    includeProvisional=True,
                    includeBoxes=True,
                ),
                repo=repo,
            )
            meta = response.get("meta") if isinstance(response, dict) else {}
            display_cache = meta.get("display_cache") if isinstance(meta, dict) else None
            warmed += 1
            items.append(
                {
                    "code": code,
                    "status": "warmed",
                    "elapsed_ms": int((time.perf_counter() - started) * 1000),
                    "display_cache": display_cache if isinstance(display_cache, dict) else None,
                }
            )
        except Exception as exc:
            failed += 1
            items.append(
                {
                    "code": code,
                    "status": "failed",
                    "elapsed_ms": int((time.perf_counter() - started) * 1000),
                    "error": str(exc)[:300],
                }
            )
        if delay_ms > 0 and code != unique_codes[-1]:
            time.sleep(delay_ms / 1000.0)
    return {
        "source": source,
        "requested": len(unique_codes),
        "warmed": warmed,
        "failed": failed,
        "items": items,
    }


def handle_chart_display_cache_prewarm(job_id: str, payload: dict) -> None:
    max_codes = _env_int("MEEMEE_CHART_DISPLAY_PREWARM_MAX_CODES", int(payload.get("max_codes") or _prewarm_max_codes()), minimum=1, maximum=80)
    source = str(payload.get("source") or "job")
    codes = [str(value) for value in payload.get("codes") or [] if str(value).strip()]
    if not codes:
        codes = collect_chart_display_prewarm_codes(max_codes=max_codes)

    job_manager._update_db(
        job_id,
        CHART_DISPLAY_CACHE_PREWARM_JOB_TYPE,
        "running",
        progress=10,
        message=f"Prewarming chart display cache ({len(codes)} codes)...",
    )
    result = prewarm_chart_display_cache_for_codes(codes, source=source)
    status = "skipped" if result.get("skipped") else "success"
    job_manager._update_db(
        job_id,
        CHART_DISPLAY_CACHE_PREWARM_JOB_TYPE,
        status,
        progress=100,
        finished_at=datetime.now(),
        message=(
            f"Chart display cache prewarm {status} "
            f"(requested={result.get('requested')}, warmed={result.get('warmed')}, failed={result.get('failed')})"
        ),
        error=str(result.get("reason") or "")[:800] if result.get("skipped") else None,
    )


def schedule_chart_display_cache_prewarm(*, source: str, codes: list[str] | None = None) -> str | None:
    if not _prewarm_enabled():
        return None
    payload = {
        "source": source,
        "codes": list(codes or []),
        "max_codes": _prewarm_max_codes(),
    }
    return job_manager.submit(
        CHART_DISPLAY_CACHE_PREWARM_JOB_TYPE,
        payload=payload,
        unique=True,
        message="Waiting in queue...",
        progress=0,
        lane="maintenance",
        dedupe_key=CHART_DISPLAY_CACHE_PREWARM_JOB_TYPE,
    )


def _scheduler_loop() -> None:
    startup_delay = _prewarm_startup_delay_sec()
    if startup_delay > 0 and _SCHEDULER_STOP_EVENT.wait(startup_delay):
        return
    while not _SCHEDULER_STOP_EVENT.is_set():
        try:
            schedule_chart_display_cache_prewarm(source="startup_scheduler")
        except Exception as exc:
            logger.warning("Chart display cache prewarm scheduler loop error: %s", exc)
        _SCHEDULER_STOP_EVENT.wait(_prewarm_poll_sec())


def start_chart_display_cache_prewarm_scheduler() -> None:
    if not _prewarm_enabled():
        logger.info("Chart display cache prewarm scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="chart-display-cache-prewarm-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Chart display cache prewarm scheduler started.")


def stop_chart_display_cache_prewarm_scheduler(timeout_sec: float = 1.0) -> None:
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        thread = _SCHEDULER_THREAD
        if not thread:
            return
        _SCHEDULER_STOP_EVENT.set()
    if thread.is_alive():
        thread.join(timeout=max(0.0, float(timeout_sec)))
    with _SCHEDULER_LOCK:
        _SCHEDULER_THREAD = None
