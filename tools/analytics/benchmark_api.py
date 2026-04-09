from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


def _request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout_sec: float = 30.0,
) -> tuple[int, Any, int]:
    body: bytes | None = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url=url, method=method.upper(), data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read()
        if not raw:
            return resp.getcode(), None, 0
        return resp.getcode(), json.loads(raw.decode("utf-8")), len(raw)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    idx = (len(values) - 1) * p
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return values[int(idx)]
    w = idx - lo
    return values[lo] * (1.0 - w) + values[hi] * w


def _format_ms(value: float) -> str:
    if math.isnan(value):
        return "nan"
    return f"{value:.2f}"


@dataclass
class RunSummary:
    name: str
    count: int
    failures: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    max_ms: float
    min_ms: float
    last_status: int | None
    mean_payload_bytes: float
    max_payload_bytes: int
    mean_item_count: float
    max_item_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "count": self.count,
            "failures": self.failures,
            "mean_ms": self.mean_ms,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "max_ms": self.max_ms,
            "min_ms": self.min_ms,
            "last_status": self.last_status,
            "mean_payload_bytes": self.mean_payload_bytes,
            "max_payload_bytes": self.max_payload_bytes,
            "mean_item_count": self.mean_item_count,
            "max_item_count": self.max_item_count,
        }


def _benchmark(
    name: str,
    fn,
    *,
    warmup: int,
    runs: int,
) -> RunSummary:
    for _ in range(max(0, warmup)):
        try:
            fn()
        except Exception:
            pass

    durations: list[float] = []
    payload_bytes_list: list[int] = []
    item_count_list: list[int] = []
    failures = 0
    last_status: int | None = None
    for _ in range(max(1, runs)):
        start = time.perf_counter()
        try:
            status, payload_bytes, item_count = fn()
            last_status = status
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            durations.append(elapsed_ms)
            payload_bytes_list.append(int(payload_bytes))
            item_count_list.append(int(item_count))
        except Exception:
            failures += 1

    durations.sort()
    if durations:
        mean_ms = statistics.fmean(durations)
        min_ms = durations[0]
        max_ms = durations[-1]
        p50_ms = _percentile(durations, 0.50)
        p95_ms = _percentile(durations, 0.95)
    else:
        mean_ms = min_ms = max_ms = p50_ms = p95_ms = float("nan")
    mean_payload_bytes = statistics.fmean(payload_bytes_list) if payload_bytes_list else float("nan")
    max_payload_bytes = max(payload_bytes_list) if payload_bytes_list else 0
    mean_item_count = statistics.fmean(item_count_list) if item_count_list else float("nan")
    max_item_count = max(item_count_list) if item_count_list else 0

    return RunSummary(
        name=name,
        count=len(durations),
        failures=failures,
        mean_ms=mean_ms,
        p50_ms=p50_ms,
        p95_ms=p95_ms,
        max_ms=max_ms,
        min_ms=min_ms,
        last_status=last_status,
        mean_payload_bytes=mean_payload_bytes,
        max_payload_bytes=max_payload_bytes,
        mean_item_count=mean_item_count,
        max_item_count=max_item_count,
    )


def _extract_codes(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            return _extract_codes(payload.get("items"))
        if isinstance(payload.get("data"), list):
            return _extract_codes(payload.get("data"))
    if not isinstance(payload, list):
        return []
    resolved: list[str] = []
    for row in payload:
        code = None
        if isinstance(row, list) and row:
            code = row[0]
        elif isinstance(row, dict):
            code = row.get("code")
        if code is None:
            continue
        text = str(code).strip()
        if text:
            resolved.append(text)
    return resolved


def _resolve_codes(base_api: str, explicit_codes: list[str], max_codes: int) -> list[str]:
    if explicit_codes:
        return explicit_codes[:max_codes]
    errors: list[str] = []
    endpoints = [
        f"{base_api}/list",
        f"{base_api}/grid/screener?limit={max(260, max_codes)}",
    ]
    for url in endpoints:
        try:
            status, payload, _ = _request_json("GET", url)
        except (urllib.error.URLError, json.JSONDecodeError) as exc:
            errors.append(f"{url}: {exc}")
            continue
        if status >= 400:
            errors.append(f"{url}: status={status}")
            continue
        codes = _extract_codes(payload)
        if codes:
            return codes[:max_codes]
        errors.append(f"{url}: no codes in payload")
    raise RuntimeError(" ; ".join(errors) if errors else "no code source available")


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark MeeMee API latency (p50/p95).")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Backend base URL")
    parser.add_argument("--runs", type=int, default=20, help="Measured runs per scenario")
    parser.add_argument("--warmup", type=int, default=3, help="Warmup runs per scenario")
    parser.add_argument("--batch-codes", type=int, default=48, help="Number of codes for batch_bars_v3")
    parser.add_argument("--limit", type=int, default=240, help="Bar limit for batch_bars_v3")
    parser.add_argument(
        "--codes",
        default="",
        help="Comma separated codes to use for batch_bars_v3. Empty means auto from /api/list.",
    )
    parser.add_argument("--output", default="", help="Optional JSON output path")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    base_api = f"{base_url}/api"
    explicit_codes = [part.strip() for part in args.codes.split(",") if part.strip()]

    try:
        codes = _resolve_codes(base_api, explicit_codes, max_codes=max(1, args.batch_codes))
    except (urllib.error.URLError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] failed to resolve codes: {exc}")
        return 1

    if not codes:
        print("[ERROR] no codes available. Provide --codes or ensure /api/list returns data.")
        return 1

    print(f"[INFO] base_url={base_url}")
    print(f"[INFO] runs={args.runs} warmup={args.warmup} codes={len(codes)} limit={args.limit}")

    def run_batch_daily() -> None:
        payload = {
            "timeframes": ["daily"],
            "codes": codes,
            "limit": int(args.limit),
            "includeProvisional": True,
        }
        status, data, payload_bytes = _request_json("POST", f"{base_api}/batch_bars_v3", payload=payload)
        if status >= 400:
            raise RuntimeError(f"POST /batch_bars_v3 daily failed: status={status}")
        item_count = len(((data or {}).get("items") or {}).keys()) if isinstance(data, dict) else 0
        return status, payload_bytes, item_count

    def run_batch_monthly() -> tuple[int, int, int]:
        payload = {
            "timeframes": ["monthly"],
            "codes": codes,
            "limit": int(args.limit),
            "includeProvisional": False,
        }
        status, data, payload_bytes = _request_json("POST", f"{base_api}/batch_bars_v3", payload=payload)
        if status >= 400:
            raise RuntimeError(f"POST /batch_bars_v3 monthly failed: status={status}")
        item_count = len(((data or {}).get("items") or {}).keys()) if isinstance(data, dict) else 0
        return status, payload_bytes, item_count

    def run_grid_screener() -> tuple[int, int, int]:
        query = urllib.parse.urlencode({"limit": 50})
        status, data, payload_bytes = _request_json("GET", f"{base_api}/grid/screener?{query}")
        if status >= 400:
            raise RuntimeError(f"GET /grid/screener failed: status={status}")
        item_count = len((data or {}).get("items") or []) if isinstance(data, dict) else 0
        return status, payload_bytes, item_count

    focus_code = codes[0]

    def run_ticker_monthly() -> tuple[int, int, int]:
        query = urllib.parse.urlencode({"code": focus_code, "limit": 120})
        status, data, payload_bytes = _request_json("GET", f"{base_api}/ticker/monthly?{query}")
        if status >= 400:
            raise RuntimeError(f"GET /ticker/monthly failed: status={status}")
        item_count = len((data or {}).get("data") or []) if isinstance(data, dict) else 0
        return status, payload_bytes, item_count

    def run_ticker_analysis() -> tuple[int, int, int]:
        query = urllib.parse.urlencode({"code": focus_code})
        status, data, payload_bytes = _request_json("GET", f"{base_api}/ticker/analysis?{query}")
        if status >= 400:
            raise RuntimeError(f"GET /ticker/analysis failed: status={status}")
        item_count = 1 if isinstance(data, dict) and data.get("item") else 0
        return status, payload_bytes, item_count

    def run_ticker_timeline() -> tuple[int, int, int]:
        query = urllib.parse.urlencode({"code": focus_code, "limit": 400})
        status, data, payload_bytes = _request_json("GET", f"{base_api}/ticker/analysis/timeline?{query}")
        if status >= 400:
            raise RuntimeError(f"GET /ticker/analysis/timeline failed: status={status}")
        item_count = len((data or {}).get("items") or []) if isinstance(data, dict) else 0
        return status, payload_bytes, item_count

    scenarios = [
        _benchmark("batch_bars_v3_daily", run_batch_daily, warmup=args.warmup, runs=args.runs),
        _benchmark("batch_bars_v3_monthly", run_batch_monthly, warmup=args.warmup, runs=args.runs),
        _benchmark("grid_screener", run_grid_screener, warmup=max(1, args.warmup // 2), runs=args.runs),
        _benchmark("ticker_monthly", run_ticker_monthly, warmup=max(1, args.warmup // 2), runs=args.runs),
        _benchmark("ticker_analysis", run_ticker_analysis, warmup=max(1, args.warmup // 2), runs=args.runs),
        _benchmark("ticker_analysis_timeline", run_ticker_timeline, warmup=max(1, args.warmup // 2), runs=args.runs),
    ]

    report = {
        "base_url": base_url,
        "runs": args.runs,
        "warmup": args.warmup,
        "codes": codes,
        "limit": int(args.limit),
        "generated_at_unix": int(time.time()),
        "scenarios": [item.to_dict() for item in scenarios],
    }

    for row in scenarios:
        print(
            f"[RESULT] {row.name}: count={row.count} failures={row.failures} "
            f"mean={_format_ms(row.mean_ms)}ms p50={_format_ms(row.p50_ms)}ms "
            f"p95={_format_ms(row.p95_ms)}ms min={_format_ms(row.min_ms)}ms max={_format_ms(row.max_ms)}ms "
            f"status={row.last_status} mean_payload_bytes={int(row.mean_payload_bytes) if not math.isnan(row.mean_payload_bytes) else 'nan'} "
            f"max_payload_bytes={row.max_payload_bytes} mean_item_count={row.mean_item_count:.2f} "
            f"max_item_count={row.max_item_count}"
        )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)
        print(f"[INFO] wrote report: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
