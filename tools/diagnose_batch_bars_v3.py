from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _now_stamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z").replace(":", "-").replace(".", "-")


class Timeline:
    def __init__(self) -> None:
        self._start = time.perf_counter()
        self.events: list[dict[str, Any]] = []

    def ms(self) -> int:
        return int(round((time.perf_counter() - self._start) * 1000))

    def mark(self, label: str, **payload: Any) -> None:
        event = {"label": label, "at_ms": self.ms()}
        if payload:
            event["payload"] = payload
        self.events.append(event)

    def span(self, label: str, fn: Callable[[], Any], **payload: Any) -> Any:
        self.mark(f"{label}_start", **payload)
        start = self.ms()
        try:
            result = fn()
            self.mark(f"{label}_end", duration_ms=self.ms() - start)
            return result
        except Exception as exc:
            self.mark(f"{label}_error", duration_ms=self.ms() - start, error=str(exc))
            raise


class RepoProxy:
    def __init__(self, repo: Any, timeline: Timeline) -> None:
        self._repo = repo
        self._timeline = timeline

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repo, name)

    def get_daily_bars_batch(self, codes: list[str], limit: int = 400, asof_dt: int | None = None) -> Any:
        return self._timeline.span(
            "repo_get_daily_bars_batch",
            lambda: self._repo.get_daily_bars_batch(codes, limit, asof_dt=asof_dt),
            codes=len(codes),
            limit=limit,
            asof_dt=asof_dt,
        )

    def get_weekly_bars_batch(self, codes: list[str], limit: int = 120, asof_dt: int | None = None) -> Any:
        return self._timeline.span(
            "repo_get_weekly_bars_batch",
            lambda: self._repo.get_weekly_bars_batch(codes, limit, asof_dt=asof_dt),
            codes=len(codes),
            limit=limit,
            asof_dt=asof_dt,
        )

    def get_monthly_bars_batch(
        self,
        codes: list[str],
        limit: int = 120,
        asof_dt: int | None = None,
        recent_daily_rows_by_code: dict[str, list[tuple]] | None = None,
    ) -> Any:
        return self._timeline.span(
            "repo_get_monthly_bars_batch",
            lambda: self._repo.get_monthly_bars_batch(
                codes,
                limit,
                asof_dt=asof_dt,
                recent_daily_rows_by_code=recent_daily_rows_by_code,
            ),
            codes=len(codes),
            limit=limit,
            asof_dt=asof_dt,
            has_recent_daily_rows=recent_daily_rows_by_code is not None,
        )


def _latest_artifact(pattern: str) -> Path | None:
    root = Path("artifacts/runtime-ui")
    candidates = sorted(root.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _read_json(path: Path | None) -> Any:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _summarize_frontend_cache(detail_artifact: dict[str, Any] | None) -> dict[str, Any]:
    if not detail_artifact:
        return {
            "classification": "unknown",
            "reason": "no detail-performance-diagnosis artifact was available",
            "prefetch_hit": None,
            "partial_cache_used": None,
            "seed_events": [],
        }
    perf_events = ((detail_artifact.get("detail") or {}).get("perf_events") or [])
    seed_events = [
        {
            "route": event.get("route"),
            "code": (event.get("payload") or {}).get("code"),
            "source": (event.get("payload") or {}).get("source"),
            "timeframe": (event.get("payload") or {}).get("timeframe"),
        }
        for event in perf_events
        if event.get("eventType") == "detail_chart_seed_hit"
    ]
    partial_cache_used = (detail_artifact.get("cache") or {}).get("partial_cache_used")
    pending_seed = any(event.get("source") == "pending" for event in seed_events)
    materialized_seed = any(event.get("source") in {"memory", "persistent", "cache"} for event in seed_events)
    if materialized_seed and pending_seed:
        classification = "cached_frame_not_used_during_pending_refresh"
        reason = "detail seed events include materialized cache and pending refresh"
    elif materialized_seed:
        classification = "cached_frame_used"
        reason = "detail seed events show materialized memory/persistent/cache source"
    elif pending_seed:
        classification = "api_required_for_first_render"
        reason = "detail target seed source was pending; no materialized target frame was observed before the API response"
    elif not seed_events and partial_cache_used is False:
        classification = "api_required_for_first_render"
        reason = "no materialized detail seed event was observed and partial_cache_used was false"
    else:
        classification = "unknown"
        reason = "frontend perf events do not prove whether a materialized target frame existed"
    return {
        "classification": classification,
        "reason": reason,
        "prefetch_hit": (detail_artifact.get("cache") or {}).get("prefetch_hit"),
        "partial_cache_used": partial_cache_used,
        "seed_events": seed_events,
        "source_artifact": str(Path(detail_artifact.get("_artifact_path", ""))) if detail_artifact.get("_artifact_path") else None,
    }


def _measure_handler(
    *,
    label: str,
    bars: Any,
    repo: Any,
    payload: Any,
    clear_cache: bool,
) -> dict[str, Any]:
    timeline = Timeline()
    if clear_cache:
        bars._batch_v3_cache.clear()
        bars._batch_v3_inflight.clear()
    timeline.mark("request_received", measurement_label=label, codes=len(payload.codes), timeframes=list(payload.timeframes))

    original_cache_get = bars._get_cached_batch_v3_items
    original_cache_store = bars._store_cached_batch_v3_items
    original_claim = bars._claim_batch_v3_inflight
    original_finish = bars._finish_batch_v3_inflight
    original_fetch = bars._fetch_multi_timeframe_items
    original_contract = bars.build_chart_data_freshness_contract
    original_provisional = bars.get_provisional_daily_rows_from_spark

    from app.backend.infra.duckdb import stock_repo as stock_repo_module

    original_get_conn_for_path = stock_repo_module.get_conn_for_path

    class TimedConnContext:
        def __init__(self, inner: Any, db_path: str, timeout_sec: float, read_only: bool) -> None:
            self._inner = inner
            self._db_path = db_path
            self._timeout_sec = timeout_sec
            self._read_only = read_only

        def __enter__(self) -> Any:
            timeline.mark(
                "db_connection_requested",
                db_path=self._db_path,
                timeout_sec=self._timeout_sec,
                read_only=self._read_only,
            )
            start = timeline.ms()
            conn = self._inner.__enter__()
            timeline.mark("db_connection_acquired", duration_ms=timeline.ms() - start)
            return conn

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
            timeline.mark("db_connection_release_start")
            start = timeline.ms()
            result = self._inner.__exit__(exc_type, exc, tb)
            timeline.mark("db_connection_release_end", duration_ms=timeline.ms() - start)
            return result

    def timed_get_conn_for_path(db_path: str, *, timeout_sec: float = 1.0, read_only: bool = False) -> Any:
        return TimedConnContext(
            original_get_conn_for_path(db_path, timeout_sec=timeout_sec, read_only=read_only),
            db_path,
            timeout_sec,
            read_only,
        )

    def timed_cache_get(cache_key: tuple[Any, ...]) -> Any:
        result = timeline.span("batch_cache_lookup", lambda: original_cache_get(cache_key))
        timeline.mark("batch_cache_lookup_result", hit=result is not None)
        return result

    def timed_cache_store(cache_key: tuple[Any, ...], items: Any) -> Any:
        return timeline.span("batch_cache_store", lambda: original_cache_store(cache_key, items))

    def timed_claim(cache_key: tuple[Any, ...]) -> Any:
        event, is_owner = timeline.span("batch_inflight_claim", lambda: original_claim(cache_key))
        timeline.mark("batch_inflight_claim_result", is_owner=is_owner)
        return event, is_owner

    def timed_finish(cache_key: tuple[Any, ...]) -> Any:
        return timeline.span("batch_inflight_finish", lambda: original_finish(cache_key))

    def timed_provisional(*args: Any, **kwargs: Any) -> Any:
        return timeline.span(
            "yahoo_provisional_fetch",
            lambda: original_provisional(*args, **kwargs),
            codes=len(args[0]) if args else None,
            force_refresh=kwargs.get("force_refresh"),
        )

    def timed_fetch(**kwargs: Any) -> Any:
        return timeline.span(
            "fetch_multi_timeframe_items",
            lambda: original_fetch(**kwargs),
            codes=len(kwargs.get("codes") or []),
            requested_frames=list(kwargs.get("requested_frames") or []),
            force_refresh=kwargs.get("force_refresh"),
        )

    def timed_contract(*args: Any, **kwargs: Any) -> Any:
        return timeline.span("freshness_contract_build", lambda: original_contract(*args, **kwargs))

    try:
        bars._get_cached_batch_v3_items = timed_cache_get
        bars._store_cached_batch_v3_items = timed_cache_store
        bars._claim_batch_v3_inflight = timed_claim
        bars._finish_batch_v3_inflight = timed_finish
        bars._fetch_multi_timeframe_items = timed_fetch
        bars.build_chart_data_freshness_contract = timed_contract
        bars.get_provisional_daily_rows_from_spark = timed_provisional
        stock_repo_module.get_conn_for_path = timed_get_conn_for_path

        timeline.mark("handler_entered")
        response = bars.batch_bars_v3(payload, repo=RepoProxy(repo, timeline))
        timeline.mark("handler_returned")
        serialized = timeline.span("json_serialization", lambda: json.dumps(response, ensure_ascii=False, default=str))
        timeline.mark("response_sent", payload_bytes=len(serialized.encode("utf-8")))
        items = response.get("items") if isinstance(response, dict) else {}
        row_counts: dict[str, dict[str, int]] = {}
        if isinstance(items, dict):
            for code, frames in items.items():
                row_counts[code] = {
                    frame: len((payload_frame or {}).get("bars") or [])
                    for frame, payload_frame in (frames or {}).items()
                }
        return {
            "label": label,
            "status": "ok",
            "timeline": timeline.events,
            "duration_ms": timeline.ms(),
            "payload_bytes": len(serialized.encode("utf-8")),
            "row_counts": row_counts,
        }
    except Exception as exc:
        timeline.mark("diagnosis_error", error=str(exc))
        return {
            "label": label,
            "status": "error",
            "error": str(exc),
            "timeline": timeline.events,
            "duration_ms": timeline.ms(),
        }
    finally:
        bars._get_cached_batch_v3_items = original_cache_get
        bars._store_cached_batch_v3_items = original_cache_store
        bars._claim_batch_v3_inflight = original_claim
        bars._finish_batch_v3_inflight = original_finish
        bars._fetch_multi_timeframe_items = original_fetch
        bars.build_chart_data_freshness_contract = original_contract
        bars.get_provisional_daily_rows_from_spark = original_provisional
        stock_repo_module.get_conn_for_path = original_get_conn_for_path


def _phase_duration(measurement: dict[str, Any], start_label: str, end_label: str) -> int | None:
    events = measurement.get("timeline") or []
    start = next((event.get("at_ms") for event in events if event.get("label") == start_label), None)
    end = next((event.get("at_ms") for event in events if event.get("label") == end_label), None)
    if isinstance(start, int) and isinstance(end, int):
        return end - start
    return None


def _classify(measurement: dict[str, Any], frontend_cache: dict[str, Any]) -> tuple[str, str]:
    provisional = _phase_duration(measurement, "yahoo_provisional_fetch_start", "yahoo_provisional_fetch_end") or 0
    db_methods = [
        event for event in measurement.get("timeline", [])
        if str(event.get("label", "")).startswith("repo_get_") and str(event.get("label", "")).endswith("_end")
    ]
    db_total = sum(int((event.get("payload") or {}).get("duration_ms") or 0) for event in db_methods)
    total = int(measurement.get("duration_ms") or 0)
    if frontend_cache.get("classification") == "cached_frame_not_used_during_pending_refresh":
        return "cached_frame_not_used_during_pending_refresh", str(frontend_cache.get("reason"))
    if provisional >= max(500, total * 0.4):
        return "api_yahoo_provisional_overlay", f"Yahoo provisional overlay consumed {provisional}ms of {total}ms"
    if db_total >= max(500, total * 0.4):
        return "db_query_or_connection", f"repo batch methods consumed {db_total}ms of {total}ms"
    if total >= 500 and frontend_cache.get("classification") == "api_required_for_first_render":
        return "api_required_for_first_render", str(frontend_cache.get("reason"))
    return "inconclusive", "no single measured segment dominated"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", default="6971")
    parser.add_argument("--neighbors", default="7970,7419,1417,6448")
    parser.add_argument("--data-dir", default=os.getenv("MEEMEE_DATA_DIR"))
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    os.environ.setdefault("APP_ENV", "dev")
    os.environ.setdefault("MEEMEE_EDINET_AUTO_START_ENABLED", "0")
    os.environ.setdefault("MEEMEE_EDINET_EMPTY_DB_BACKFILL_ENABLED", "0")
    os.environ.setdefault("MEEMEE_YF_DAILY_INGEST_ENABLED", "0")
    os.environ.setdefault("MEEMEE_YF_DAILY_INGEST_INTRADAY_ENABLED", "0")
    os.environ.setdefault("MEEMEE_SCREENER_SNAPSHOT_ENABLED", "0")
    os.environ.setdefault("MEEMEE_RANKINGS_WARMUP_ENABLED", "0")
    os.environ.setdefault("MEEMEE_ANALYSIS_PREWARM_ENABLED", "0")
    os.environ.setdefault("MEEMEE_RANK_QUALITY_ENABLED", "0")
    os.environ.setdefault("MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_ENABLED", "0")

    from app.backend.api.dependencies import get_stock_repo, init_resources
    from app.backend.api.routers import bars
    from app.backend.api.routers.bars import BatchBarsV3Request
    from app.core.config import config
    from app.db.session import get_connect_stats

    if args.data_dir:
        init_resources(args.data_dir)
    else:
        init_resources(str(config.DATA_DIR))

    repo = get_stock_repo()
    target_payload = BatchBarsV3Request(
        codes=[args.code],
        timeframes=["daily", "weekly", "monthly"],
        limit=2000,
        timeframeLimits={"daily": 2000, "weekly": 520, "monthly": 240},
        includeProvisional=True,
        includeBoxes=True,
    )
    neighbor_codes = [code.strip() for code in args.neighbors.split(",") if code.strip()]
    neighbor_payload = BatchBarsV3Request(
        codes=neighbor_codes,
        timeframes=["daily", "weekly", "monthly"],
        limit=2000,
        timeframeLimits={"daily": 2000, "weekly": 520, "monthly": 240},
        includeProvisional=True,
        includeBoxes=True,
    )

    detail_artifact_path = _latest_artifact("detail-performance-diagnosis-*.json")
    detail_artifact = _read_json(detail_artifact_path)
    if isinstance(detail_artifact, dict) and detail_artifact_path:
        detail_artifact["_artifact_path"] = str(detail_artifact_path)
    frontend_cache = _summarize_frontend_cache(detail_artifact)
    target_measurement = _measure_handler(
        label="target_single_cold",
        bars=bars,
        repo=repo,
        payload=target_payload,
        clear_cache=True,
    )
    target_cached_measurement = _measure_handler(
        label="target_single_cache_check",
        bars=bars,
        repo=repo,
        payload=target_payload,
        clear_cache=False,
    )
    neighbor_measurement = _measure_handler(
        label="neighbor_batch_cold",
        bars=bars,
        repo=repo,
        payload=neighbor_payload,
        clear_cache=True,
    )

    classification, reason = _classify(target_measurement, frontend_cache)
    artifact = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": {
            "status": "diagnosed" if classification != "inconclusive" else "inconclusive",
            "primary_bottleneck": classification,
            "reason": reason,
        },
        "route": {
            "router_file": "app/backend/api/routers/bars.py",
            "handler": "batch_bars_v3",
            "service_functions": [
                "_build_batch_meta",
                "_get_cached_batch_v3_items",
                "_claim_batch_v3_inflight",
                "_fetch_multi_timeframe_items",
                "build_chart_data_freshness_contract",
            ],
            "db_access_functions": [
                "StockRepository.get_daily_bars_batch",
                "StockRepository.get_weekly_bars_batch",
                "StockRepository.get_monthly_bars_batch",
                "app.db.session.get_conn_for_path",
            ],
            "query_shape": {
                "daily_weekly_monthly_separate_repo_calls": True,
                "daily_batch_query": True,
                "weekly_derived_from_daily_batch_query": True,
                "monthly_batch_query": True,
                "monthly_can_reuse_recent_daily_rows": True,
            },
            "cache": {
                "backend_in_process_cache": True,
                "backend_inflight_dedup": True,
                "ttl_sec": getattr(bars, "_BATCH_V3_CACHE_TTL_SEC", None),
            },
        },
        "frontend_cache_question": frontend_cache,
        "measurements": [
            target_measurement,
            target_cached_measurement,
            neighbor_measurement,
        ],
        "connect_stats": get_connect_stats(),
        "environment": {
            "db_path": str(config.DB_PATH),
            "serialize_duckdb_access": os.getenv("MEEMEE_SERIALIZE_DUCKDB_ACCESS", "1"),
            "yf_provisional_enabled": os.getenv("MEEMEE_YF_PROVISIONAL_ENABLED", "default_true"),
            "background_jobs_disabled_for_run": {
                key: os.getenv(key)
                for key in [
                    "MEEMEE_EDINET_AUTO_START_ENABLED",
                    "MEEMEE_EDINET_EMPTY_DB_BACKFILL_ENABLED",
                    "MEEMEE_YF_DAILY_INGEST_ENABLED",
                    "MEEMEE_YF_DAILY_INGEST_INTRADAY_ENABLED",
                    "MEEMEE_SCREENER_SNAPSHOT_ENABLED",
                    "MEEMEE_RANKINGS_WARMUP_ENABLED",
                    "MEEMEE_ANALYSIS_PREWARM_ENABLED",
                    "MEEMEE_RANK_QUALITY_ENABLED",
                    "MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_ENABLED",
                ]
            },
        },
        "recommendations": [
            "Separate Yahoo provisional overlay timing from DuckDB timing with repeated runs and optionally MEEMEE_YF_PROVISIONAL_ENABLED=0.",
            "If frontend cache classification remains api_required_for_first_render, backend/API/cache is the next optimization target.",
        ],
    }

    output = Path(args.output) if args.output else Path("artifacts/runtime-ui") / f"batch-bars-v3-diagnosis-{_now_stamp()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"artifactPath": str(output.resolve()), "summary": artifact["summary"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
