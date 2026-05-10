from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.diagnose_batch_bars_v3 import _measure_handler  # noqa: E402


def _now_stamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z").replace(":", "-").replace(".", "-")


def _event_duration(measurement: dict[str, Any], label: str) -> int | None:
    for event in measurement.get("timeline") or []:
        if event.get("label") == f"{label}_end":
            payload = event.get("payload") or {}
            value = payload.get("duration_ms")
            return int(value) if isinstance(value, int | float) else None
    return None


def _event_payloads(measurement: dict[str, Any], label: str) -> list[dict[str, Any]]:
    return [
        event.get("payload") or {}
        for event in measurement.get("timeline") or []
        if event.get("label") == label
    ]


def _connection_waits(measurement: dict[str, Any]) -> list[int]:
    waits: list[int] = []
    for event in measurement.get("timeline") or []:
        if event.get("label") != "db_connection_acquired":
            continue
        value = (event.get("payload") or {}).get("duration_ms")
        if isinstance(value, int | float):
            waits.append(int(value))
    return waits


def _cache_result(measurement: dict[str, Any]) -> dict[str, Any]:
    cache_hits = _event_payloads(measurement, "batch_cache_lookup_result")
    inflight_claims = _event_payloads(measurement, "batch_inflight_claim_result")
    return {
        "cache_hit": cache_hits[-1].get("hit") if cache_hits else None,
        "inflight_owner": inflight_claims[-1].get("is_owner") if inflight_claims else None,
        "inflight_dedupe_hit": (inflight_claims[-1].get("is_owner") is False) if inflight_claims else None,
    }


def _summarize_measurement(measurement: dict[str, Any]) -> dict[str, Any]:
    waits = _connection_waits(measurement)
    repo_daily = _event_duration(measurement, "repo_get_daily_bars_batch")
    repo_weekly = _event_duration(measurement, "repo_get_weekly_bars_batch")
    repo_monthly = _event_duration(measurement, "repo_get_monthly_bars_batch")
    provisional = _event_duration(measurement, "yahoo_provisional_fetch")
    fetch_total = _event_duration(measurement, "fetch_multi_timeframe_items")
    freshness = _event_duration(measurement, "freshness_contract_build")
    serialization = _event_duration(measurement, "json_serialization")
    cache = _cache_result(measurement)
    return {
        "label": measurement.get("label"),
        "status": measurement.get("status"),
        "duration_ms": measurement.get("duration_ms"),
        "handler_total_ms": measurement.get("duration_ms"),
        "db_connection_wait_total_ms": sum(waits),
        "db_connection_wait_max_ms": max(waits) if waits else 0,
        "db_connection_count": len(waits),
        "daily_repo_batch_ms": repo_daily,
        "weekly_repo_batch_ms": repo_weekly,
        "monthly_repo_batch_ms": repo_monthly,
        "provisional_yahoo_merge_ms": provisional,
        "fetch_transform_aggregation_ms": fetch_total,
        "freshness_contract_ms": freshness,
        "serialization_ms": serialization,
        "response_bytes": measurement.get("payload_bytes"),
        "cache": cache,
        "row_counts": measurement.get("row_counts"),
    }


def _dominant_segment(summary: dict[str, Any]) -> tuple[str, str]:
    duration = int(summary.get("duration_ms") or 0)
    segments = {
        "db_connection_wait": int(summary.get("db_connection_wait_total_ms") or 0),
        "daily_repo_batch": int(summary.get("daily_repo_batch_ms") or 0),
        "weekly_repo_batch": int(summary.get("weekly_repo_batch_ms") or 0),
        "monthly_repo_batch": int(summary.get("monthly_repo_batch_ms") or 0),
        "provisional_yahoo_merge": int(summary.get("provisional_yahoo_merge_ms") or 0),
        "serialization": int(summary.get("serialization_ms") or 0),
    }
    name, value = max(segments.items(), key=lambda item: item[1])
    if duration >= 1000 and value >= max(500, duration * 0.4):
        return name, f"{name} consumed {value}ms of {duration}ms"
    if duration >= 1000:
        return "distributed_or_uninstrumented", f"no measured segment exceeded 40% of {duration}ms"
    return "not_slow_in_direct_diagnosis", f"direct target-only request completed in {duration}ms"


def _answer_questions(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    cold = next((item for item in summaries if item.get("label") == "target_prefetch_payload_cold"), None)
    warm = next((item for item in summaries if item.get("label") == "target_prefetch_payload_warm_cache"), None)
    detail = next((item for item in summaries if item.get("label") == "target_detail_payload_cold"), None)
    cold_duration = int((cold or {}).get("duration_ms") or 0)
    warm_duration = int((warm or {}).get("duration_ms") or 0)
    detail_duration = int((detail or {}).get("duration_ms") or 0)
    cold_cache = ((cold or {}).get("cache") or {}).get("cache_hit")
    warm_cache = ((warm or {}).get("cache") or {}).get("cache_hit")
    cold_dominant, cold_reason = _dominant_segment(cold or {})
    return {
        "target_only_slow_without_neighbor_overlap": cold_duration >= 3000,
        "db_connection_serialization_wait_likely": int((cold or {}).get("db_connection_wait_total_ms") or 0) >= 500,
        "slowest_timeframe_or_segment": cold_dominant,
        "slowest_timeframe_or_segment_reason": cold_reason,
        "cache_miss_vs_hit": {
            "cold_cache_hit": cold_cache,
            "warm_cache_hit": warm_cache,
            "cold_ms": cold_duration,
            "warm_ms": warm_duration,
            "cache_materially_changes_latency": warm_duration > 0 and cold_duration > warm_duration * 3,
        },
        "duplicate_or_overlapping_target_requests_observed": any(
            ((item.get("cache") or {}).get("inflight_dedupe_hit") is True) for item in summaries
        ),
        "runtime_selection_or_favorites_competing_observed": False,
        "runtime_selection_or_favorites_note": "This in-process diagnosis did not issue concurrent runtime-selection or favorites HTTP requests.",
        "browser_payload_differs_from_detail_payload": {
            "prefetch_include_boxes": False,
            "detail_include_boxes": True,
            "prefetch_cold_ms": cold_duration,
            "detail_cold_ms": detail_duration,
        },
        "easy_cache_materialization_before_detail": (
            "backend in-process cache hit is fast after a completed identical payload, but first render still requires API when no frontend materialized frame exists"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", default="6971")
    parser.add_argument("--runs", type=int, default=3)
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

    init_resources(str(args.data_dir or config.DATA_DIR))
    repo = get_stock_repo()

    prefetch_payload = BatchBarsV3Request(
        codes=[args.code],
        timeframes=["daily", "weekly", "monthly"],
        limit=2000,
        timeframeLimits={"daily": 2000, "weekly": 520, "monthly": 240},
        includeProvisional=True,
        includeBoxes=False,
    )
    detail_payload = BatchBarsV3Request(
        codes=[args.code],
        timeframes=["daily", "weekly", "monthly"],
        limit=2000,
        timeframeLimits={"daily": 2000, "weekly": 520, "monthly": 240},
        includeProvisional=True,
        includeBoxes=True,
    )

    measurements: list[dict[str, Any]] = []
    measurements.append(
        _measure_handler(
            label="target_prefetch_payload_cold",
            bars=bars,
            repo=repo,
            payload=prefetch_payload,
            clear_cache=True,
        )
    )
    measurements.append(
        _measure_handler(
            label="target_prefetch_payload_warm_cache",
            bars=bars,
            repo=repo,
            payload=prefetch_payload,
            clear_cache=False,
        )
    )
    for index in range(max(0, args.runs - 1)):
        measurements.append(
            _measure_handler(
                label=f"target_prefetch_payload_repeat_{index + 1}",
                bars=bars,
                repo=repo,
                payload=prefetch_payload,
                clear_cache=False,
            )
        )
    measurements.append(
        _measure_handler(
            label="target_detail_payload_cold",
            bars=bars,
            repo=repo,
            payload=detail_payload,
            clear_cache=True,
        )
    )
    measurements.append(
        _measure_handler(
            label="target_detail_payload_warm_cache",
            bars=bars,
            repo=repo,
            payload=detail_payload,
            clear_cache=False,
        )
    )

    summaries = [_summarize_measurement(measurement) for measurement in measurements]
    cold_summary = summaries[0] if summaries else {}
    primary, reason = _dominant_segment(cold_summary)
    artifact = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": {
            "status": "diagnosed",
            "primary_bottleneck": primary,
            "reason": reason,
            "scope": "target-only /api/batch_bars_v3 direct handler instrumentation",
        },
        "target": {
            "code": args.code,
            "payloads": {
                "prefetch": json.loads(prefetch_payload.model_dump_json()),
                "detail": json.loads(detail_payload.model_dump_json()),
            },
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
        },
        "answers": _answer_questions(summaries),
        "summaries": summaries,
        "measurements": measurements,
        "environment": {
            "db_path": str(config.DB_PATH),
            "serialize_duckdb_access": os.getenv("MEEMEE_SERIALIZE_DUCKDB_ACCESS", "1"),
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
            "connect_stats": get_connect_stats(),
        },
        "recommendations": [
            "If browser target-only remains much slower than this direct handler run, measure the live HTTP server with concurrent runtime-selection/favorites requests.",
            "If direct cold runs are slow and dominated by a specific repo batch, optimize that timeframe query or cache materialization path before touching frontend DetailView scheduling again.",
        ],
    }

    output = Path(args.output) if args.output else Path("artifacts/runtime-ui") / f"target-batch-bars-v3-latency-{_now_stamp()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"artifactPath": str(output.resolve()), "summary": artifact["summary"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
