from __future__ import annotations

import argparse
import json
import math
import random
import shutil
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.ml.rankings_cache import get_rankings_asof  # noqa: E402
from scripts.tradex_chart_first_replay import (  # noqa: E402
    _build_entry_reason_report,
    _build_postmortem,
    _build_roundtrip_summary,
    _date_text_to_ymd,
    _json_text,
    _load_source_frames,
    _write_json,
    _write_parquet,
    _ymd_to_date_text,
    run_chart_first_replay,
)
from scripts.tradex_regime_specialization_gate_compare import (  # noqa: E402
    _baseline_label,
    _basis_lookup,
    _slug,
    _safe_float,
    _specialized_label,
)

DEFAULT_SOURCE_DB_PATH = Path(r"C:\Users\enish\Desktop\MeeMeeScreener\_internal\app\backend\stocks.duckdb")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m")
DEFAULT_SEED = 20260424
DEFAULT_ANCHOR_COUNT = 10
DEFAULT_WARMUP_DAYS = 120
DEFAULT_HORIZON_DAYS = 63
DEFAULT_POOL_LIMIT = 50
TOP_K_VALUES = (5, 10, 20)
STRESS60_REFERENCE_SOURCE_DB_PATH = Path(r"C:\Users\enish\Desktop\MeeMeeScreener\_internal\app\backend\stocks.duckdb")
SMOKE10_REFERENCE_SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _month_bucket(dt_ymd: int) -> str:
    text = _ymd_to_date_text(int(dt_ymd))
    return text[:7]


def _selection_score(item: dict[str, Any]) -> float:
    for key in ("tradePriorityScore", "entryScore", "probSide", "hybridScore", "swingScore"):
        value = item.get(key)
        try:
            if value is None:
                continue
            out = float(value)
        except Exception:
            continue
        if math.isfinite(out):
            return out
    return 0.0


def _directional_snapshot_score(item: dict[str, Any], *, direction: str) -> float:
    up_score = max(
        _safe_float(item.get("weeklyBreakoutUpProb")),
        _safe_float(item.get("monthlyBreakoutUpProb")),
    )
    down_score = max(
        _safe_float(item.get("weeklyBreakoutDownProb")),
        _safe_float(item.get("monthlyBreakoutDownProb")),
    )
    trade_score = _selection_score(item)
    if direction == "down":
        return float(max(trade_score, down_score))
    return float(max(trade_score, up_score))


def _regime_bucket(item: dict[str, Any]) -> str:
    value = str(item.get("marketRegime") or "").lower()
    if any(token in value for token in ("down", "bear", "riskoff")):
        return "down"
    if any(token in value for token in ("up", "bull", "riskon")):
        return "up"
    if any(token in value for token in ("range", "flat", "sideways")):
        return "flat"
    return "unknown"


def _row_to_json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _row_to_json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_row_to_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_row_to_json_ready(item) for item in value]
    if isinstance(value, pd.Series):
        return _row_to_json_ready(value.to_dict())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _json_ready(value: Any) -> Any:
    return _row_to_json_ready(value)


def _write_payload_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _artifact_name(base_name: str, *, artifact_tag: str | None) -> str:
    if not artifact_tag:
        return base_name
    path = Path(base_name)
    return f"{path.stem}_{_slug(artifact_tag)}{path.suffix}"


def _artifact_path(output_dir: Path, base_name: str, *, artifact_tag: str | None) -> Path:
    return output_dir / _artifact_name(base_name, artifact_tag=artifact_tag)


def _load_trading_calendar(*, source_db_path: Path) -> list[int]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER) AS dt
            FROM daily_bars
            ORDER BY dt
            """
        ).fetchall()
    finally:
        conn.close()
    return [int(row[0]) for row in rows]


def _load_signal_basis_bounds(*, source_db_path: Path) -> tuple[int, int]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT
                MIN(dt) AS min_dt,
                MAX(dt) AS max_dt
            FROM signal_basis_daily
            """
        ).fetchone()
    finally:
        conn.close()
    if not row or row[0] is None or row[1] is None:
        raise RuntimeError("signal_basis_daily coverage not found")
    return int(row[0]), int(row[1])


def _load_latest_table_date(*, source_db_path: Path, table_name: str, date_column: str) -> int | None:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        row = conn.execute(f"SELECT MAX({date_column}) FROM {table_name}").fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _db_file_provenance(path: Path) -> dict[str, Any]:
    stat = path.expanduser().resolve().stat()
    return {
        "path": str(path.expanduser().resolve()),
        "size_bytes": int(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def _unix_seconds_to_ymd_text(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y%m%d")


def _db_provenance_payload(
    *,
    requested_source_db_path: Path,
    working_source_db_path: Path,
    basis_start_dt: int,
    basis_end_dt: int,
    ranking_snapshot_as_of: list[int],
) -> dict[str, Any]:
    requested = requested_source_db_path.expanduser().resolve()
    working = working_source_db_path.expanduser().resolve()
    requested_provenance = _db_file_provenance(requested)
    working_provenance = _db_file_provenance(working)
    latest_daily_bars_date = _load_latest_table_date(source_db_path=working, table_name="daily_bars", date_column="date")
    try:
        latest_feature_snapshot_daily_date = _load_latest_table_date(
            source_db_path=working,
            table_name="feature_snapshot_daily",
            date_column="dt",
        )
    except Exception:
        latest_feature_snapshot_daily_date = None
    same_as_smoke10_reference_db = working == SMOKE10_REFERENCE_SOURCE_DB_PATH.expanduser().resolve()
    same_as_stress60_reference_db = working == STRESS60_REFERENCE_SOURCE_DB_PATH.expanduser().resolve()
    copied_db_path = None if working == requested else str(working)
    copied_db_size_bytes = None if working == requested else int(working_provenance["size_bytes"])
    copied_db_mtime = None if working == requested else working_provenance["mtime"]
    return {
        "requested_source_db_path": str(requested),
        "working_source_db_path": str(working),
        "source_db_path": str(working),
        "source_db_size_bytes": int(working_provenance["size_bytes"]),
        "source_db_mtime": working_provenance["mtime"],
        "copied_db_path": copied_db_path,
        "copied_db_size_bytes": copied_db_size_bytes,
        "copied_db_mtime": copied_db_mtime,
        "latest_daily_bars_date": _unix_seconds_to_ymd_text(latest_daily_bars_date),
        "latest_feature_snapshot_daily_date": _unix_seconds_to_ymd_text(latest_feature_snapshot_daily_date),
        "signal_basis_daily_min_date": _ymd_to_date_text(basis_start_dt),
        "signal_basis_daily_max_date": _ymd_to_date_text(basis_end_dt),
        "ranking_snapshot_as_of": [_ymd_to_date_text(int(dt)) for dt in ranking_snapshot_as_of],
        "ranking_snapshot_as_of_mode": "per_anchor_date",
        "stress60_reference_source_db_path": str(STRESS60_REFERENCE_SOURCE_DB_PATH.expanduser().resolve()),
        "stress60_same_db": same_as_stress60_reference_db,
        "smoke10_reference_source_db_path": str(SMOKE10_REFERENCE_SOURCE_DB_PATH.expanduser().resolve()),
        "smoke10_same_db": same_as_smoke10_reference_db,
        "research_fallback_db_source": not same_as_smoke10_reference_db,
    }


def _aggregate_exclusion_diagnostics(exclusion_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not exclusion_rows:
        return {
            "anchor_count": 0,
            "rows": [],
            "candidate_rows_before_skip": 0,
            "candidate_rows_after_skip": 0,
            "skipped_symbols_without_forward_bars_count": 0,
            "skipped_symbols_without_basis_row_count": 0,
            "skipped_symbols_without_basis_row_by_anchor": {},
            "skipped_symbols_without_basis_row_sample": [],
            "excluded_anchor_count_by_reason": {},
        }

    by_anchor: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "candidate_rows_before_skip": 0,
        "candidate_rows_after_skip": 0,
        "skipped_symbols_without_forward_bars_count": 0,
        "skipped_symbols_without_basis_row_count": 0,
        "skipped_symbols_without_basis_row_sample": [],
    })
    excluded_anchor_count_by_reason: Counter[str] = Counter()
    basis_samples: list[str] = []

    for row in exclusion_rows:
        anchor_date = str(row["anchor_date"])
        side = str(row["side"])
        bucket = by_anchor[anchor_date]
        bucket["candidate_rows_before_skip"] += int(row.get("candidate_rows_before_skip") or 0)
        bucket["candidate_rows_after_skip"] += int(row.get("candidate_rows_after_skip") or 0)
        bucket["skipped_symbols_without_forward_bars_count"] += int(row.get("skipped_symbols_without_forward_bars_count") or 0)
        bucket["skipped_symbols_without_basis_row_count"] += int(row.get("skipped_symbols_without_basis_row_count") or 0)
        sample = [str(value) for value in (row.get("skipped_symbols_without_basis_row_sample") or [])]
        if sample:
            bucket["skipped_symbols_without_basis_row_sample"].extend(sample[:5])
            basis_samples.extend(sample[:5])
        if int(row.get("candidate_rows_before_skip") or 0) == 0:
            excluded_anchor_count_by_reason[f"{side}_no_selected_symbols"] += 1
        if int(row.get("skipped_symbols_without_forward_bars_count") or 0) > 0:
            excluded_anchor_count_by_reason[f"{side}_without_forward_bars"] += 1
        if int(row.get("skipped_symbols_without_basis_row_count") or 0) > 0:
            excluded_anchor_count_by_reason[f"{side}_without_basis_row"] += 1
        if int(row.get("candidate_rows_after_skip") or 0) == 0 and int(row.get("candidate_rows_before_skip") or 0) > 0:
            excluded_anchor_count_by_reason[f"{side}_empty_after_skip"] += 1

    return {
        "anchor_count": len(by_anchor),
        "rows": exclusion_rows,
        "candidate_rows_before_skip": int(sum(int(row.get("candidate_rows_before_skip") or 0) for row in exclusion_rows)),
        "candidate_rows_after_skip": int(sum(int(row.get("candidate_rows_after_skip") or 0) for row in exclusion_rows)),
        "skipped_symbols_without_forward_bars_count": int(
            sum(int(row.get("skipped_symbols_without_forward_bars_count") or 0) for row in exclusion_rows)
        ),
        "skipped_symbols_without_basis_row_count": int(
            sum(int(row.get("skipped_symbols_without_basis_row_count") or 0) for row in exclusion_rows)
        ),
        "skipped_symbols_without_basis_row_by_anchor": {
            anchor_date: {
                "candidate_rows_before_skip": int(bucket["candidate_rows_before_skip"]),
                "candidate_rows_after_skip": int(bucket["candidate_rows_after_skip"]),
                "skipped_symbols_without_forward_bars_count": int(bucket["skipped_symbols_without_forward_bars_count"]),
                "skipped_symbols_without_basis_row_count": int(bucket["skipped_symbols_without_basis_row_count"]),
            }
            for anchor_date, bucket in sorted(by_anchor.items())
        },
        "skipped_symbols_without_basis_row_sample": basis_samples[:20],
        "excluded_anchor_count_by_reason": dict(sorted(excluded_anchor_count_by_reason.items())),
    }


def _ensure_usable_source_db_path(
    *,
    source_db_path: Path,
    output_dir: Path,
    artifact_tag: str | None,
) -> Path:
    candidate = source_db_path.expanduser().resolve()
    try:
        conn = duckdb.connect(str(candidate), read_only=True)
        try:
            conn.execute("SELECT 1").fetchone()
        finally:
            conn.close()
        return candidate
    except Exception:
        cache_dir = output_dir / "_db_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        working_name = _artifact_name(f"{candidate.stem}{candidate.suffix}", artifact_tag=artifact_tag)
        working_path = cache_dir / working_name
        if working_path.exists():
            return working_path
        shutil.copy2(candidate, working_path)
        return working_path


def _load_universe_symbols(*, source_db_path: Path, as_of: int) -> list[str]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT code
            FROM signal_basis_daily
            WHERE dt <= ?
            ORDER BY code
            """,
            [int(as_of)],
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _eligible_month_candidates(
    trading_dates: list[int],
    *,
    warmup_days: int,
    horizon_days: int,
    basis_start_dt: int | None = None,
    basis_end_dt: int | None = None,
) -> tuple[dict[str, list[tuple[int, int]]], dict[str, Counter[str]]]:
    by_month: dict[str, list[tuple[int, int]]] = defaultdict(list)
    excluded: dict[str, Counter[str]] = defaultdict(Counter)
    total = len(trading_dates)
    for idx, dt in enumerate(trading_dates):
        month = _month_bucket(dt)
        if idx < warmup_days:
            excluded[month]["insufficient_warmup"] += 1
            continue
        if idx + horizon_days >= total:
            excluded[month]["insufficient_forward_coverage"] += 1
            continue
        if basis_start_dt is not None and dt < int(basis_start_dt):
            excluded[month]["insufficient_basis_coverage"] += 1
            continue
        if basis_end_dt is not None and dt > int(basis_end_dt):
            excluded[month]["beyond_basis_coverage"] += 1
            continue
        by_month[month].append((idx, dt))
    return dict(by_month), dict(excluded)


@dataclass(frozen=True)
class AnchorSpec:
    anchor_date: int
    month_bucket: str
    anchor_index: int
    available_forward_days: int
    selected_reason: str
    excluded_count_by_reason: dict[str, int]


def _sample_anchor_dates(
    trading_dates: list[int],
    *,
    seed: int,
    anchor_count: int,
    warmup_days: int,
    horizon_days: int,
    basis_start_dt: int | None = None,
    basis_end_dt: int | None = None,
) -> list[AnchorSpec]:
    eligible_by_month, excluded_by_month = _eligible_month_candidates(
        trading_dates,
        warmup_days=warmup_days,
        horizon_days=horizon_days,
        basis_start_dt=basis_start_dt,
        basis_end_dt=basis_end_dt,
    )
    eligible_months = sorted(eligible_by_month)
    if not eligible_months:
        raise RuntimeError("no eligible anchor months found")
    rng = random.Random(seed)
    if int(anchor_count) <= len(eligible_months):
        sampled_months = rng.sample(eligible_months, int(anchor_count))
    else:
        sampled_months = [rng.choice(eligible_months) for _ in range(int(anchor_count))]
    sampled_months.sort()

    anchors: list[AnchorSpec] = []
    for month in sampled_months:
        month_candidates = eligible_by_month[month]
        anchor_index, anchor_date = rng.choice(month_candidates)
        month_all = [index for index, dt in enumerate(trading_dates) if _month_bucket(dt) == month]
        month_total = len(month_all)
        eligible_count = len(month_candidates)
        excluded_counts = dict(excluded_by_month.get(month, Counter()))
        excluded_counts["not_selected_in_month"] = max(0, eligible_count - 1)
        anchors.append(
            AnchorSpec(
                anchor_date=int(anchor_date),
                month_bucket=month,
                anchor_index=int(anchor_index),
                available_forward_days=max(0, len(trading_dates) - anchor_index - 1),
                selected_reason="seeded_monthly_random_sample",
                excluded_count_by_reason=excluded_counts,
            )
        )
    return anchors


def _anchor_overlap_diagnostics(anchors: list[AnchorSpec]) -> dict[str, Any]:
    anchor_date_counts = Counter(anchor.anchor_date for anchor in anchors)
    month_bucket_counts = Counter(anchor.month_bucket for anchor in anchors)
    duplicate_anchor_dates = {int(key): int(count) for key, count in anchor_date_counts.items() if count > 1}
    duplicate_month_buckets = {str(key): int(count) for key, count in month_bucket_counts.items() if count > 1}
    return {
        "schema_version": "tradex_random_anchor_overlap_diagnostics_v1",
        "generated_at": _utc_now(),
        "anchor_count": len(anchors),
        "duplicate_anchor_date_count": int(sum(count - 1 for count in anchor_date_counts.values() if count > 1)),
        "duplicate_month_bucket_count": int(sum(count - 1 for count in month_bucket_counts.values() if count > 1)),
        "duplicate_anchor_dates": duplicate_anchor_dates,
        "duplicate_month_buckets": duplicate_month_buckets,
        "anchor_date_counts": {str(int(key)): int(value) for key, value in anchor_date_counts.items()},
        "month_bucket_counts": {str(key): int(value) for key, value in month_bucket_counts.items()},
    }


def _ranking_snapshot(
    *,
    source_db_path: Path,
    as_of: int,
    direction: str,
    limit: int,
) -> dict[str, Any]:
    try:
        return get_rankings_asof(
            "D",
            "latest",
            direction,
            int(limit),
            as_of=int(as_of),
            mode="trade",
            risk_mode="balanced",
        )
    except Exception as exc:
        symbols = _load_universe_symbols(source_db_path=source_db_path, as_of=int(as_of))
        basis_lookup = _basis_lookup(source_db_path=source_db_path, as_of=int(as_of), symbols=symbols)
        items: list[dict[str, Any]] = []
        for item in basis_lookup.values():
            out = dict(item)
            out["score"] = _directional_snapshot_score(out, direction=direction)
            items.append(out)
        items.sort(
            key=lambda item: (
                item.get("score") is None,
                -float(item.get("score") or 0.0),
                str(item.get("code") or ""),
            )
        )
        logger.warning("rankings snapshot fallback to direct basis query: as_of=%s direction=%s err=%s", as_of, direction, exc)
        return {
            "items": items[: int(limit)],
            "meta": {
                "snapshot_source": "signal_basis_daily_direct_query_fallback",
                "as_of": int(as_of),
                "direction": direction,
                "limit": int(limit),
                "candidate_count": len(items),
                "fallback_error": str(exc),
            },
        }


def _full_universe_gate_coverage(
    *,
    source_db_path: Path,
    as_of: int,
) -> dict[str, Any]:
    universe_symbols = _load_universe_symbols(source_db_path=source_db_path, as_of=as_of)
    basis_lookup = _basis_lookup(source_db_path=source_db_path, as_of=as_of, symbols=universe_symbols)
    baseline_counts: Counter[str] = Counter()
    specialized_counts: Counter[str] = Counter()
    for item in basis_lookup.values():
        baseline_counts[_baseline_label(item).label] += 1
        specialized_counts[_specialized_label(item).label] += 1
    total = max(1, len(basis_lookup))

    def _rates(counts: Counter[str]) -> dict[str, Any]:
        long_count = int(counts.get("long_tradable", 0))
        short_count = int(counts.get("short_tradable", 0))
        no_trade_count = int(counts.get("no_trade", 0))
        return {
            "long_tradable_count": long_count,
            "short_tradable_count": short_count,
            "no_trade_count": no_trade_count,
            "long_tradable_rate": float(long_count / total),
            "short_tradable_rate": float(short_count / total),
            "no_trade_rate": float(no_trade_count / total),
        }

    return {
        "anchor_date": _ymd_to_date_text(int(as_of)),
        "universe_symbol_count": len(universe_symbols),
        "basis_symbol_count": len(basis_lookup),
        "baseline": _rates(baseline_counts),
        "specialized": _rates(specialized_counts),
    }


def _select_rows(items: list[dict[str, Any]], *, label_fn) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, float]]:
    rows: list[dict[str, Any]] = []
    rank_map: dict[str, int] = {}
    score_map: dict[str, float] = {}
    for rank, item in enumerate(items, start=1):
        code = str(item.get("code") or "")
        if not code:
            continue
        gate = label_fn(item)
        score = _selection_score(item)
        rows.append(
            {
                "code": code,
                "rank": int(rank),
                "score": float(score),
                "gate": gate.label,
                "gate_reason": gate.reason,
                "item": item,
            }
        )
        rank_map[code] = int(rank)
        score_map[code] = float(score)
    return rows, rank_map, score_map


def _selected_symbols(rows: list[dict[str, Any]], *, top_k: int) -> list[str]:
    selected: list[str] = []
    for row in rows:
        if row["gate"] == "no_trade":
            continue
        selected.append(row["code"])
        if len(selected) >= top_k:
            break
    return selected


def _candidate_snapshot_rows(
    *,
    anchor_spec: AnchorSpec,
    side: str,
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_map = {row["code"]: row for row in baseline_rows}
    challenger_map = {row["code"]: row for row in challenger_rows}
    baseline_rank = {code: row["rank"] for code, row in baseline_map.items() if row["gate"] != "no_trade"}
    challenger_rank = {code: row["rank"] for code, row in challenger_map.items() if row["gate"] != "no_trade"}
    baseline_score = {code: row["score"] for code, row in baseline_map.items()}
    challenger_score = {code: row["score"] for code, row in challenger_map.items()}
    baseline_sets = {k: set(_selected_symbols(baseline_rows, top_k=k)) for k in TOP_K_VALUES}
    challenger_sets = {k: set(_selected_symbols(challenger_rows, top_k=k)) for k in TOP_K_VALUES}
    out: list[dict[str, Any]] = []
    symbol_order = [row["code"] for row in baseline_rows]
    seen = set(symbol_order) | {row["code"] for row in challenger_rows}
    ordered_symbols = [code for code in symbol_order if code in seen] + [code for code in challenger_rank if code not in symbol_order]
    for code in ordered_symbols:
        baseline = baseline_map.get(code)
        challenger = challenger_map.get(code)
        if baseline is None and challenger is None:
            continue
        selected_by = []
        if code in baseline_sets[20]:
            selected_by.append("champion")
        if code in challenger_sets[20]:
            selected_by.append("challenger")
        if not selected_by:
            selected_by = ["neutral"]
        out.append(
            {
                "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
                "month_bucket": anchor_spec.month_bucket,
                "side": side,
                "symbol": code,
                "rank": baseline["rank"] if baseline else None,
                "score": baseline["score"] if baseline else None,
                "champion_rank": baseline_rank.get(code),
                "challenger_rank": challenger_rank.get(code),
                "champion_score": baseline_score.get(code),
                "challenger_score": challenger_score.get(code),
                "selected_by": "both" if selected_by == ["champion", "challenger"] else selected_by[0],
                "selected_by_methods": selected_by,
                "selection_reason": {
                    "champion": baseline["gate_reason"] if baseline else None,
                    "challenger": challenger["gate_reason"] if challenger else None,
                },
                "champion_gate": baseline["gate"] if baseline else None,
                "challenger_gate": challenger["gate"] if challenger else None,
                "champion_selected_top5": code in baseline_sets[5],
                "champion_selected_top10": code in baseline_sets[10],
                "champion_selected_top20": code in baseline_sets[20],
                "challenger_selected_top5": code in challenger_sets[5],
                "challenger_selected_top10": code in challenger_sets[10],
                "challenger_selected_top20": code in challenger_sets[20],
                "changed_top5_member": (code in baseline_sets[5]) != (code in challenger_sets[5]),
                "changed_top10_member": (code in baseline_sets[10]) != (code in challenger_sets[10]),
                "changed_top20_member": (code in baseline_sets[20]) != (code in challenger_sets[20]),
                "market_regime_bucket": _regime_bucket((baseline or challenger or {}).get("item", {})),
            }
        )
    return out


def _load_symbol_bars(
    *,
    source_db_path: Path,
    symbols: list[str],
    start_date: int,
    end_date: int,
) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["dt", "code", "o", "h", "l", "c", "v"])
    placeholders = ",".join(["?"] * len(symbols))
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        frame = conn.execute(
            f"""
            SELECT
                CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER) AS dt,
                code,
                o,
                h,
                l,
                c,
                v
            FROM daily_bars
            WHERE code IN ({placeholders})
              AND CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
            ORDER BY code, dt
            """,
            [*symbols, int(start_date), int(end_date)],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["dt"] = pd.to_numeric(frame["dt"], errors="coerce").astype("Int64")
    return frame


def _selection_only_row(
    *,
    anchor_spec: AnchorSpec,
    side: str,
    symbol: str,
    symbol_frame: pd.DataFrame,
    selected_metadata: dict[str, Any],
    trading_dates: list[int],
) -> dict[str, Any]:
    if symbol_frame.empty:
        raise RuntimeError(f"no bars found for symbol={symbol}")
    frame = symbol_frame.sort_values("dt").reset_index(drop=True)
    dt_values = [int(value) for value in frame["dt"].tolist()]
    anchor_index = dt_values.index(anchor_spec.anchor_date)
    entry_index = anchor_index + 1
    if entry_index >= len(dt_values):
        raise RuntimeError(f"missing entry day after anchor={anchor_spec.anchor_date} for symbol={symbol}")
    entry_date = dt_values[entry_index]
    entry_row = frame.iloc[entry_index]
    entry_price = float(entry_row["o"])
    exit_index_5 = min(entry_index + 4, len(dt_values) - 1)
    exit_index_10 = min(entry_index + 9, len(dt_values) - 1)
    exit_index_20 = min(entry_index + 19, len(dt_values) - 1)
    exit_index_63 = min(entry_index + 62, len(dt_values) - 1)
    exit_row_63 = frame.iloc[exit_index_63]
    exit_price = float(exit_row_63["c"])
    if side == "long":
        def _ret(exit_price_value: float) -> float:
            return (exit_price_value - entry_price) / entry_price

        mfe = (float(frame.iloc[entry_index : exit_index_63 + 1]["h"].max()) - entry_price) / entry_price
        mae = (float(frame.iloc[entry_index : exit_index_63 + 1]["l"].min()) - entry_price) / entry_price
    elif side == "short":
        def _ret(exit_price_value: float) -> float:
            return (entry_price - exit_price_value) / entry_price

        mfe = (entry_price - float(frame.iloc[entry_index : exit_index_63 + 1]["l"].min())) / entry_price
        mae = (entry_price - float(frame.iloc[entry_index : exit_index_63 + 1]["h"].max())) / entry_price
    else:
        raise ValueError(f"unsupported side={side}")

    ret5 = _ret(float(frame.iloc[exit_index_5]["c"]))
    ret10 = _ret(float(frame.iloc[exit_index_10]["c"]))
    ret20 = _ret(float(frame.iloc[exit_index_20]["c"]))
    ret63 = _ret(exit_price)
    result_bucket = "win" if ret63 > 0 else ("loss" if ret63 < 0 else "flat")
    return {
        "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
        "month_bucket": anchor_spec.month_bucket,
        "side": side,
        "symbol": symbol,
        "entry_date": _ymd_to_date_text(entry_date),
        "entry_price": entry_price,
        "exit_date": _ymd_to_date_text(dt_values[exit_index_63]),
        "exit_price": exit_price,
        "ret5": float(ret5),
        "ret10": float(ret10),
        "ret20": float(ret20),
        "ret63": float(ret63),
        "mfe63": float(mfe),
        "mae63": float(mae),
        "max_adverse_excursion": float(abs(mae)),
        "result_bucket": result_bucket,
        **selected_metadata,
    }


def _bars_for_selection_only(
    *,
    source_db_path: Path,
    symbols: list[str],
    start_date: int,
    end_date: int,
) -> pd.DataFrame:
    return _load_symbol_bars(
        source_db_path=source_db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )


def _evaluate_selection_only_rows(
    *,
    anchor_spec: AnchorSpec,
    side: str,
    candidate_rows: list[dict[str, Any]],
    bars_frame: pd.DataFrame,
    top_k_values: tuple[int, ...] = TOP_K_VALUES,
) -> list[dict[str, Any]]:
    if bars_frame.empty:
        return []
    selected_rows: list[dict[str, Any]] = []
    for row in candidate_rows:
        if row["champion_rank"] is None and row["challenger_rank"] is None:
            continue
        symbol = str(row["symbol"])
        symbol_frame = bars_frame.loc[bars_frame["code"] == symbol]
        if symbol_frame.empty:
            continue
        metadata = {
            "champion_rank": row["champion_rank"],
            "challenger_rank": row["challenger_rank"],
            "champion_score": row["champion_score"],
            "challenger_score": row["challenger_score"],
            "selected_by": row["selected_by"],
            "selected_by_methods": row["selected_by_methods"],
            "champion_selected_top5": row["champion_selected_top5"],
            "champion_selected_top10": row["champion_selected_top10"],
            "champion_selected_top20": row["champion_selected_top20"],
            "challenger_selected_top5": row["challenger_selected_top5"],
            "challenger_selected_top10": row["challenger_selected_top10"],
            "challenger_selected_top20": row["challenger_selected_top20"],
            "changed_top5_member": row["changed_top5_member"],
            "changed_top10_member": row["changed_top10_member"],
            "changed_top20_member": row["changed_top20_member"],
        }
        selection_row = _selection_only_row(
            anchor_spec=anchor_spec,
            side=side,
            symbol=symbol,
            symbol_frame=symbol_frame,
            selected_metadata=metadata,
            trading_dates=[],
        )
        selected_rows.append(selection_row)
    return selected_rows


def _policy_run_dir(
    *,
    output_dir: Path,
    anchor_spec: AnchorSpec,
    side: str,
    symbol: str,
) -> Path:
    return output_dir / "policy_runs" / f"{anchor_spec.anchor_date}_{anchor_spec.month_bucket}" / side / symbol


def _policy_run_result_row(
    *,
    anchor_spec: AnchorSpec,
    side: str,
    symbol: str,
    method_metadata: dict[str, Any],
    source_db_path: Path,
    output_dir: Path,
    trading_dates: list[int],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    run_dir = _policy_run_dir(output_dir=output_dir, anchor_spec=anchor_spec, side=side, symbol=symbol)
    anchor_index = trading_dates.index(anchor_spec.anchor_date)
    horizon_index = min(anchor_index + DEFAULT_HORIZON_DAYS - 1, len(trading_dates) - 1)
    horizon_end = trading_dates[horizon_index]
    result = run_chart_first_replay(
        source_db_path=source_db_path,
        output_dir=run_dir,
        symbol=symbol,
        start_date=_ymd_to_date_text(anchor_spec.anchor_date),
        end_date=_ymd_to_date_text(horizon_end),
        freeze_date=_ymd_to_date_text(trading_dates[max(0, anchor_index - 1)]),
    )
    summary = result["roundtrip_summary"]
    postmortem = result["postmortem"]
    ledger_json = json.loads(Path(result["paths"]["daily_ledger_json"]).read_text(encoding="utf-8"))
    annotated_rows: list[dict[str, Any]] = []
    for row in ledger_json.get("rows") or []:
        annotated_rows.append(
            {
                "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
                "month_bucket": anchor_spec.month_bucket,
                "side": side,
                "symbol": symbol,
                "trade_date": row.get("date"),
                "selection_method": method_metadata.get("selection_method"),
                "selection_source": method_metadata.get("selection_source"),
                "champion_rank": method_metadata.get("champion_rank"),
                "challenger_rank": method_metadata.get("challenger_rank"),
                "champion_selected_top5": method_metadata.get("champion_selected_top5"),
                "champion_selected_top10": method_metadata.get("champion_selected_top10"),
                "champion_selected_top20": method_metadata.get("champion_selected_top20"),
                "challenger_selected_top5": method_metadata.get("challenger_selected_top5"),
                "challenger_selected_top10": method_metadata.get("challenger_selected_top10"),
                "challenger_selected_top20": method_metadata.get("challenger_selected_top20"),
                "selected_action": row.get("selected_action"),
                "previous_position": row.get("previous_position"),
                "next_position": row.get("next_position"),
                "execution_price": row.get("execution_price"),
                "target_buy_units": row.get("target_buy_units"),
                "target_sell_units": row.get("target_sell_units"),
                "buy_delta_units": row.get("buy_delta_units"),
                "sell_delta_units": row.get("sell_delta_units"),
                "entry_reason_primary": row.get("entry_reason_primary"),
                "entry_reason_codes": row.get("entry_reason_codes"),
                "entry_reason_detail": row.get("entry_reason_detail"),
                "add_reason_primary": row.get("add_reason_primary"),
                "add_reason_codes": row.get("add_reason_codes"),
                "add_reason_detail": row.get("add_reason_detail"),
                "hedge_reason_primary": row.get("hedge_reason_primary"),
                "hedge_reason_codes": row.get("hedge_reason_codes"),
                "hedge_reason_detail": row.get("hedge_reason_detail"),
                "trim_reason_primary": row.get("trim_reason_primary"),
                "trim_reason_codes": row.get("trim_reason_codes"),
                "trim_reason_detail": row.get("trim_reason_detail"),
                "exit_reason_primary": row.get("exit_reason_primary"),
                "exit_reason_codes": row.get("exit_reason_codes"),
                "exit_reason_detail": row.get("exit_reason_detail"),
                "cover_reason_primary": row.get("cover_reason_primary"),
                "cover_reason_codes": row.get("cover_reason_codes"),
                "cover_reason_detail": row.get("cover_reason_detail"),
                "flat_reason_primary": row.get("flat_reason_primary"),
                "flat_reason_codes": row.get("flat_reason_codes"),
                "flat_reason_detail": row.get("flat_reason_detail"),
                "realized_pnl": row.get("realized_pnl"),
                "unrealized_pnl": row.get("unrealized_pnl"),
                "policy_roundtrip_count": summary["aggregate"]["roundtrip_count"],
                "policy_net_realized_pnl": summary["aggregate"]["net_realized_pnl"],
                "policy_max_drawdown_during_holding": summary["aggregate"]["max_drawdown_during_holding"],
                "policy_exit_timing": summary["aggregate"]["exits_early_or_late"],
                "policy_summary_path": result["paths"]["roundtrip_summary"],
                "policy_postmortem_path": result["paths"]["postmortem"],
            }
        )
    run_row = {
        "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
        "month_bucket": anchor_spec.month_bucket,
        "side": side,
        "symbol": symbol,
        "market_regime_bucket": method_metadata.get("market_regime_bucket"),
        **method_metadata,
        "run_output_dir": result["config"]["output_dir"],
        "roundtrip_count": summary["aggregate"]["roundtrip_count"],
        "entry_count": summary["aggregate"]["entry_count"],
        "exit_count": summary["aggregate"]["exit_count"],
        "hedge_count": summary["aggregate"]["hedge_count"],
        "stay_count": summary["aggregate"]["stay_count"],
        "net_realized_pnl": summary["aggregate"]["net_realized_pnl"],
        "max_drawdown_during_holding": summary["aggregate"]["max_drawdown_during_holding"],
        "average_capture_ratio": summary["aggregate"].get("average_capture_ratio"),
        "exits_early_or_late": summary["aggregate"].get("exits_early_or_late"),
        "roundtrip_summary": summary,
        "postmortem": postmortem,
        "selected_action_count": result["aggregate"]["roundtrip_count"],
    }
    return run_row, annotated_rows


def _method_membership(row: dict[str, Any], method: str, top_k: int) -> bool:
    return bool(row.get(f"{method}_selected_top{int(top_k)}"))


def _metric_rows_for_method(
    rows: list[dict[str, Any]],
    *,
    method: str,
    top_k: int,
) -> list[dict[str, Any]]:
    return [row for row in rows if _method_membership(row, method, top_k)]


def _summarize_selection_rows(rows: list[dict[str, Any]], *, method: str, top_k: int) -> dict[str, Any]:
    selected = _metric_rows_for_method(rows, method=method, top_k=top_k)
    if not selected:
        return {
            "selected_count": 0,
            "bad_pick_rate": None,
            "win_rate": None,
            "avg_ret63": None,
            "median_ret63": None,
            "avg_mfe63": None,
            "avg_mae63": None,
            "worst_mae63": None,
            "neutral_rate": None,
        }
    ret63 = [float(row["ret63"]) for row in selected]
    mfe63 = [float(row["mfe63"]) for row in selected]
    mae63 = [float(row["mae63"]) for row in selected]
    win_rate = sum(1 for value in ret63 if value > 0) / len(ret63)
    bad_pick_rate = sum(1 for value in ret63 if value <= 0) / len(ret63)
    return {
        "selected_count": len(selected),
        "bad_pick_rate": float(bad_pick_rate),
        "win_rate": float(win_rate),
        "avg_ret63": float(sum(ret63) / len(ret63)),
        "median_ret63": float(pd.Series(ret63).median()),
        "avg_mfe63": float(sum(mfe63) / len(mfe63)),
        "avg_mae63": float(sum(mae63) / len(mae63)),
        "worst_mae63": float(min(mae63)),
        "neutral_rate": None,
    }


def _summarize_policy_runs(runs: list[dict[str, Any]], *, method: str, top_k: int) -> dict[str, Any]:
    selected = [row for row in runs if _method_membership(row, method, top_k)]
    if not selected:
        return {
            "selected_count": 0,
            "roundtrip_count": 0,
            "net_realized_pnl": 0.0,
            "max_drawdown_during_holding": None,
            "average_capture_ratio": None,
            "exits_early_or_late": None,
            "win_rate": None,
        }
    pnl = [float(row["net_realized_pnl"]) for row in selected]
    dds = [float(row["max_drawdown_during_holding"]) for row in selected]
    capture = [row.get("average_capture_ratio") for row in selected if row.get("average_capture_ratio") is not None]
    win_rate = sum(1 for value in pnl if value > 0) / len(pnl)
    return {
        "selected_count": len(selected),
        "roundtrip_count": int(sum(int(row["roundtrip_count"]) for row in selected)),
        "net_realized_pnl": float(sum(pnl)),
        "max_drawdown_during_holding": float(min(dds)),
        "average_capture_ratio": float(sum(float(value) for value in capture) / len(capture)) if capture else None,
        "exits_early_or_late": "late" if any(str(row.get("exits_early_or_late") or "") == "late" for row in selected) else "acceptable",
        "win_rate": float(win_rate),
    }


def _aggregate_topk_metrics(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        payload[str(top_k)] = {
            "selection_only": {
                "champion": _summarize_selection_rows(rows, method="champion", top_k=top_k),
                "challenger": _summarize_selection_rows(rows, method="challenger", top_k=top_k),
            },
            "policy_trade": {
                "champion": _summarize_policy_runs(run_rows, method="champion", top_k=top_k),
                "challenger": _summarize_policy_runs(run_rows, method="challenger", top_k=top_k),
            },
        }
        champ_sel = payload[str(top_k)]["selection_only"]["champion"]
        chal_sel = payload[str(top_k)]["selection_only"]["challenger"]
        champ_pol = payload[str(top_k)]["policy_trade"]["champion"]
        chal_pol = payload[str(top_k)]["policy_trade"]["challenger"]
        payload[str(top_k)]["delta"] = {
            "selection_only_avg_ret63": None
            if champ_sel["avg_ret63"] is None or chal_sel["avg_ret63"] is None
            else float(chal_sel["avg_ret63"] - champ_sel["avg_ret63"]),
            "selection_only_bad_pick_rate": None
            if champ_sel["bad_pick_rate"] is None or chal_sel["bad_pick_rate"] is None
            else float(chal_sel["bad_pick_rate"] - champ_sel["bad_pick_rate"]),
            "policy_net_realized_pnl": float(chal_pol["net_realized_pnl"] - champ_pol["net_realized_pnl"]),
            "policy_max_drawdown_during_holding": None
            if champ_pol["max_drawdown_during_holding"] is None or chal_pol["max_drawdown_during_holding"] is None
            else float(chal_pol["max_drawdown_during_holding"] - champ_pol["max_drawdown_during_holding"]),
        }
    return payload


def _aggregate_topk_metrics_by_side(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for side in ("long", "short"):
        side_rows = [row for row in rows if row.get("side") == side]
        side_run_rows = [row for row in run_rows if row.get("side") == side]
        payload[side] = _aggregate_topk_metrics(side_rows, side_run_rows)
    return payload


def _regime_metrics(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    regime_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        regime_rows[str(row.get("market_regime_bucket") or "unknown")].append(row)
    out: dict[str, Any] = {}
    for regime, regime_group in regime_rows.items():
        if not regime_group:
            continue
        ret63 = [float(row["ret63"]) for row in regime_group]
        pnl = [float(row["net_realized_pnl"]) for row in run_rows if str(row.get("market_regime_bucket") or "unknown") == regime]
        out[regime] = {
            "selection_only_avg_ret63": float(sum(ret63) / len(ret63)),
            "selection_only_win_rate": float(sum(1 for value in ret63 if value > 0) / len(ret63)),
            "policy_net_realized_pnl": float(sum(pnl)) if pnl else 0.0,
            "count": len(regime_group),
        }
    return out


def _build_selection_rows_and_policy_runs(
    *,
    source_db_path: Path,
    anchor_spec: AnchorSpec,
    pool_limit: int,
    output_dir: Path,
    trading_dates: list[int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    up_snapshot = _ranking_snapshot(source_db_path=source_db_path, as_of=anchor_spec.anchor_date, direction="up", limit=pool_limit)
    down_snapshot = _ranking_snapshot(source_db_path=source_db_path, as_of=anchor_spec.anchor_date, direction="down", limit=pool_limit)
    up_items = list(up_snapshot.get("items") or [])
    down_items = list(down_snapshot.get("items") or [])

    baseline_up_rows, baseline_up_rank, baseline_up_score = _select_rows(up_items, label_fn=_baseline_label)
    challenger_up_rows, challenger_up_rank, challenger_up_score = _select_rows(up_items, label_fn=_specialized_label)
    baseline_down_rows, baseline_down_rank, baseline_down_score = _select_rows(down_items, label_fn=_baseline_label)
    challenger_down_rows, challenger_down_rank, challenger_down_score = _select_rows(down_items, label_fn=_specialized_label)

    candidate_rows = _candidate_snapshot_rows(
        anchor_spec=anchor_spec,
        side="long",
        baseline_rows=baseline_up_rows,
        challenger_rows=challenger_up_rows,
    ) + _candidate_snapshot_rows(
        anchor_spec=anchor_spec,
        side="short",
        baseline_rows=baseline_down_rows,
        challenger_rows=challenger_down_rows,
    )

    selected_long_symbols = sorted(
        set(row["symbol"] for row in candidate_rows if row["side"] == "long" and (row["champion_selected_top20"] or row["challenger_selected_top20"]))
    )
    selected_short_symbols = sorted(
        set(row["symbol"] for row in candidate_rows if row["side"] == "short" and (row["champion_selected_top20"] or row["challenger_selected_top20"]))
    )

    horizon_index = min(anchor_spec.anchor_index + DEFAULT_HORIZON_DAYS - 1, len(trading_dates) - 1)
    horizon_end = trading_dates[horizon_index]
    selection_rows: list[dict[str, Any]] = []
    policy_run_rows: list[dict[str, Any]] = []
    policy_ledger_rows: list[dict[str, Any]] = []
    exclusion_rows: list[dict[str, Any]] = []

    for side, symbol_list, baseline_rows, challenger_rows in (
        ("long", selected_long_symbols, baseline_up_rows, challenger_up_rows),
        ("short", selected_short_symbols, baseline_down_rows, challenger_down_rows),
    ):
        candidate_rows_before_skip = len(symbol_list)
        if not symbol_list:
            exclusion_rows.append(
                {
                    "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
                    "month_bucket": anchor_spec.month_bucket,
                    "side": side,
                    "candidate_rows_before_skip": 0,
                    "candidate_rows_after_skip": 0,
                    "skipped_symbols_without_forward_bars_count": 0,
                    "skipped_symbols_without_basis_row_count": 0,
                    "skipped_symbols_without_basis_row_sample": [],
                }
            )
            continue
        symbol_bars = _bars_for_selection_only(
            source_db_path=source_db_path,
            symbols=symbol_list,
            start_date=anchor_spec.anchor_date,
            end_date=horizon_end,
        )
        if symbol_bars.empty:
            continue
        available_symbols = {
            str(value)
            for value in symbol_bars["code"].dropna().astype(str).tolist()
        }
        symbols_without_forward_bars = [symbol for symbol in symbol_list if symbol not in available_symbols]
        basis_eligible_symbols = set(_basis_lookup(source_db_path=source_db_path, as_of=anchor_spec.anchor_date, symbols=symbol_list).keys())
        symbols_without_basis_row = [symbol for symbol in symbol_list if symbol not in basis_eligible_symbols]
        symbol_list = [symbol for symbol in symbol_list if symbol in available_symbols and symbol in basis_eligible_symbols]
        exclusion_rows.append(
            {
                "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
                "month_bucket": anchor_spec.month_bucket,
                "side": side,
                "candidate_rows_before_skip": candidate_rows_before_skip,
                "candidate_rows_after_skip": len(symbol_list),
                "skipped_symbols_without_forward_bars_count": len(symbols_without_forward_bars),
                "skipped_symbols_without_basis_row_count": len(symbols_without_basis_row),
                "skipped_symbols_without_basis_row_sample": symbols_without_basis_row[:5],
            }
        )
        if not symbol_list:
            continue
        union_rows = [row for row in candidate_rows if row["side"] == side and row["symbol"] in set(symbol_list)]
        selection_rows.extend(
            _evaluate_selection_only_rows(
                anchor_spec=anchor_spec,
                side=side,
                candidate_rows=union_rows,
                bars_frame=symbol_bars,
            )
        )
        rank_lookup = {row["symbol"]: row for row in union_rows}
        for symbol in symbol_list:
            method_metadata = rank_lookup[symbol]
            run_row, policy_rows = _policy_run_result_row(
                anchor_spec=anchor_spec,
                side=side,
                symbol=symbol,
                method_metadata=method_metadata,
                source_db_path=source_db_path,
                output_dir=output_dir,
                trading_dates=trading_dates,
            )
            policy_run_rows.append(run_row)
            policy_ledger_rows.extend(policy_rows)

    return candidate_rows, selection_rows, policy_run_rows, policy_ledger_rows, exclusion_rows


def _compare_anchor_rows(
    *,
    anchor_spec: AnchorSpec,
    candidate_rows: list[dict[str, Any]],
    selection_rows: list[dict[str, Any]],
    policy_run_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    result = {
        "anchor_date": _ymd_to_date_text(anchor_spec.anchor_date),
        "month_bucket": anchor_spec.month_bucket,
        "available_forward_days": anchor_spec.available_forward_days,
        "candidate_count": len(candidate_rows),
        "selection_only_count": len(selection_rows),
        "policy_run_count": len(policy_run_rows),
        "changed_top5_members_count": sum(1 for row in candidate_rows if row["changed_top5_member"]),
        "changed_top10_members_count": sum(1 for row in candidate_rows if row["changed_top10_member"]),
        "changed_top20_members_count": sum(1 for row in candidate_rows if row["changed_top20_member"]),
        "trend_up_preserved_count": sum(1 for row in candidate_rows if row["side"] == "long" and row["champion_selected_top20"] and row["challenger_selected_top20"]),
        "trend_down_selected_count": sum(1 for row in candidate_rows if row["side"] == "short" and row["challenger_selected_top20"]),
        "neutral_count": sum(1 for row in candidate_rows if row["selected_by"] == "neutral"),
        "regime_bucket": next((row["market_regime_bucket"] for row in candidate_rows if row["market_regime_bucket"] != "unknown"), "unknown"),
    }
    return result


def run_random_anchor_replay(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    seed: int = DEFAULT_SEED,
    anchor_count: int = DEFAULT_ANCHOR_COUNT,
    warmup_days: int = DEFAULT_WARMUP_DAYS,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    pool_limit: int = DEFAULT_POOL_LIMIT,
    artifact_tag: str | None = None,
) -> dict[str, Any]:
    usable_source_db_path = _ensure_usable_source_db_path(
        source_db_path=source_db_path,
        output_dir=output_dir,
        artifact_tag=artifact_tag,
    )
    trading_dates = _load_trading_calendar(source_db_path=usable_source_db_path)
    basis_start_dt, basis_end_dt = _load_signal_basis_bounds(source_db_path=usable_source_db_path)
    anchors = _sample_anchor_dates(
        trading_dates,
        seed=seed,
        anchor_count=anchor_count,
        warmup_days=warmup_days,
        horizon_days=horizon_days,
        basis_start_dt=basis_start_dt,
        basis_end_dt=basis_end_dt,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    ranking_snapshot_as_of = [anchor.anchor_date for anchor in anchors]
    db_provenance = _db_provenance_payload(
        requested_source_db_path=source_db_path,
        working_source_db_path=usable_source_db_path,
        basis_start_dt=basis_start_dt,
        basis_end_dt=basis_end_dt,
        ranking_snapshot_as_of=ranking_snapshot_as_of,
    )

    random_anchor_dates_payload = {
        "schema_version": "tradex_random_anchor_dates_v1",
        "generated_at": _utc_now(),
        "seed": int(seed),
        "sampling_method": "monthly_random_one_per_month",
        "anchor_count_requested": int(anchor_count),
        "anchor_count_selected": len(anchors),
        "warmup_trading_days": int(warmup_days),
        "horizon_trading_days": int(horizon_days),
        "signal_basis_coverage": {
            "min_dt": _ymd_to_date_text(basis_start_dt),
            "max_dt": _ymd_to_date_text(basis_end_dt),
        },
        "requested_source_db_path": str(source_db_path),
        "working_source_db_path": str(usable_source_db_path),
        "anchors": [
            {
                "anchor_date": _ymd_to_date_text(anchor.anchor_date),
                "month_bucket": anchor.month_bucket,
                "anchor_index": anchor.anchor_index,
                "available_forward_days": anchor.available_forward_days,
                "selected_reason": anchor.selected_reason,
                "excluded_count_by_reason": anchor.excluded_count_by_reason,
                "ranking_snapshot_as_of": _ymd_to_date_text(anchor.anchor_date),
            }
            for anchor in anchors
        ],
    }

    candidate_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    policy_run_rows: list[dict[str, Any]] = []
    policy_ledger_rows: list[dict[str, Any]] = []
    exclusion_rows: list[dict[str, Any]] = []
    anchor_compare_rows: list[dict[str, Any]] = []
    full_universe_coverage_rows: list[dict[str, Any]] = []

    for anchor_spec in anchors:
        anchor_output_dir = output_dir / "anchors" / f"{anchor_spec.anchor_date}_{anchor_spec.month_bucket}"
        (
            candidate_rows_anchor,
            selection_rows_anchor,
            policy_run_rows_anchor,
            policy_ledger_rows_anchor,
            exclusion_rows_anchor,
        ) = _build_selection_rows_and_policy_runs(
            source_db_path=usable_source_db_path,
            anchor_spec=anchor_spec,
            pool_limit=pool_limit,
            output_dir=anchor_output_dir,
            trading_dates=trading_dates,
        )
        candidate_rows.extend(candidate_rows_anchor)
        selection_rows.extend(selection_rows_anchor)
        policy_run_rows.extend(policy_run_rows_anchor)
        policy_ledger_rows.extend(policy_ledger_rows_anchor)
        exclusion_rows.extend(exclusion_rows_anchor)
        anchor_compare_rows.append(
            _compare_anchor_rows(
                anchor_spec=anchor_spec,
                candidate_rows=candidate_rows_anchor,
                selection_rows=selection_rows_anchor,
                policy_run_rows=policy_run_rows_anchor,
            )
        )
        full_universe_coverage_rows.append(
            _full_universe_gate_coverage(
                source_db_path=usable_source_db_path,
                as_of=anchor_spec.anchor_date,
            )
        )

    compare_payload = {
        "schema_version": "tradex_random_anchor_compare_v1",
        "generated_at": _utc_now(),
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": [int(k) for k in TOP_K_VALUES],
            "same_regime_condition": "confirmed data only; champion=current ranking/current selection; challenger=specialized selection gate",
            "same_cost_slippage": "existing chart-first replay contract",
            "same_execution_rule": "next_trading_day_open",
            "same_artifact_detail_level": "random_anchor_dates + candidate_snapshots + selection_only_ledger + policy_trade_ledger + compare + summary",
            "source_db_path": str(source_db_path),
            "working_source_db_path": str(usable_source_db_path),
            "selection_snapshot_source": "rankings_cache.get_rankings_asof",
            "policy_replay_source": "scripts.tradex_chart_first_replay.run_chart_first_replay",
        },
        "random_anchor_config": {
            "seed": int(seed),
            "sampling_method": "monthly_random_one_per_month",
            "anchor_count_requested": int(anchor_count),
            "anchor_count_selected": len(anchors),
            "warmup_trading_days": int(warmup_days),
            "horizon_trading_days": int(horizon_days),
            "pool_limit": int(pool_limit),
            "signal_basis_coverage": {
                "min_dt": _ymd_to_date_text(basis_start_dt),
                "max_dt": _ymd_to_date_text(basis_end_dt),
            },
            "requested_source_db_path": str(source_db_path),
            "working_source_db_path": str(usable_source_db_path),
        },
        "random_anchor_dates": random_anchor_dates_payload,
        "candidate_snapshot_rows": candidate_rows,
        "selection_only_replay_ledger_rows": selection_rows,
        "policy_trade_run_rows": policy_run_rows,
        "policy_trade_ledger_rows": policy_ledger_rows,
        "anchor_compare_rows": anchor_compare_rows,
        "full_universe_gate_coverage_rows": full_universe_coverage_rows,
    }

    topk_metrics = {}
    for top_k in TOP_K_VALUES:
        topk_metrics[str(top_k)] = {
            "selection_only": {
                "champion": _summarize_selection_rows(selection_rows, method="champion", top_k=top_k),
                "challenger": _summarize_selection_rows(selection_rows, method="challenger", top_k=top_k),
            },
            "policy_trade": {
                "champion": _summarize_policy_runs(policy_run_rows, method="champion", top_k=top_k),
                "challenger": _summarize_policy_runs(policy_run_rows, method="challenger", top_k=top_k),
            },
        }
        topk_metrics[str(top_k)]["delta"] = {
            "selection_only_avg_ret63": (
                None
                if topk_metrics[str(top_k)]["selection_only"]["champion"]["avg_ret63"] is None
                or topk_metrics[str(top_k)]["selection_only"]["challenger"]["avg_ret63"] is None
                else float(
                    topk_metrics[str(top_k)]["selection_only"]["challenger"]["avg_ret63"]
                    - topk_metrics[str(top_k)]["selection_only"]["champion"]["avg_ret63"]
                )
            ),
            "selection_only_bad_pick_rate": (
                None
                if topk_metrics[str(top_k)]["selection_only"]["champion"]["bad_pick_rate"] is None
                or topk_metrics[str(top_k)]["selection_only"]["challenger"]["bad_pick_rate"] is None
                else float(
                    topk_metrics[str(top_k)]["selection_only"]["challenger"]["bad_pick_rate"]
                    - topk_metrics[str(top_k)]["selection_only"]["champion"]["bad_pick_rate"]
                )
            ),
            "policy_net_realized_pnl": float(
                topk_metrics[str(top_k)]["policy_trade"]["challenger"]["net_realized_pnl"]
                - topk_metrics[str(top_k)]["policy_trade"]["champion"]["net_realized_pnl"]
            ),
            "policy_max_drawdown_during_holding": (
                None
                if topk_metrics[str(top_k)]["policy_trade"]["champion"]["max_drawdown_during_holding"] is None
                or topk_metrics[str(top_k)]["policy_trade"]["challenger"]["max_drawdown_during_holding"] is None
                else float(
                    topk_metrics[str(top_k)]["policy_trade"]["challenger"]["max_drawdown_during_holding"]
                    - topk_metrics[str(top_k)]["policy_trade"]["champion"]["max_drawdown_during_holding"]
                )
            ),
        }

    regime_metrics = {
        "selection_only": _regime_metrics(selection_rows, policy_run_rows),
        "policy_trade": _regime_metrics(selection_rows, policy_run_rows),
    }
    side_metrics = {
        "selection_only": _aggregate_topk_metrics_by_side(selection_rows, policy_run_rows),
        "policy_trade": _aggregate_topk_metrics_by_side(selection_rows, policy_run_rows),
    }

    def _aggregate_full_universe_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {
                "anchor_count": 0,
                "baseline": {"no_trade_rate_mean": None, "no_trade_rate_median": None},
                "specialized": {"no_trade_rate_mean": None, "no_trade_rate_median": None},
            }
        baseline_rates = [float(row["baseline"]["no_trade_rate"]) for row in rows]
        specialized_rates = [float(row["specialized"]["no_trade_rate"]) for row in rows]
        return {
            "anchor_count": len(rows),
            "baseline": {
                "no_trade_rate_mean": float(sum(baseline_rates) / len(baseline_rates)),
                "no_trade_rate_median": float(pd.Series(baseline_rates).median()),
                "no_trade_rate_min": float(min(baseline_rates)),
                "no_trade_rate_max": float(max(baseline_rates)),
            },
            "specialized": {
                "no_trade_rate_mean": float(sum(specialized_rates) / len(specialized_rates)),
                "no_trade_rate_median": float(pd.Series(specialized_rates).median()),
                "no_trade_rate_min": float(min(specialized_rates)),
                "no_trade_rate_max": float(max(specialized_rates)),
            },
        }

    compare_payload["aggregate_metrics"] = {
        "topk_metrics": topk_metrics,
        "regime_metrics": regime_metrics,
        "side_metrics": side_metrics,
        "full_universe_gate_coverage": _aggregate_full_universe_coverage(full_universe_coverage_rows),
        "selection_only": {
            "selection_only_row_count": len(selection_rows),
            "candidate_row_count": len(candidate_rows),
            "neutral_count": int(sum(1 for row in candidate_rows if row["selected_by"] == "neutral")),
            "neutral_rate": (
                None
                if not candidate_rows
                else float(sum(1 for row in candidate_rows if row["selected_by"] == "neutral") / len(candidate_rows))
            ),
            "policy_run_count": len(policy_run_rows),
            "policy_ledger_row_count": len(policy_ledger_rows),
            "anchor_compare_row_count": len(anchor_compare_rows),
        },
    }

    compare_payload["branching_metrics"] = {
        "changed_top5_members_count": int(sum(row["changed_top5_members_count"] for row in anchor_compare_rows)),
        "changed_top10_members_count": int(sum(row["changed_top10_members_count"] for row in anchor_compare_rows)),
        "changed_top20_members_count": int(sum(row["changed_top20_members_count"] for row in anchor_compare_rows)),
        "selection_divergence_reason": "monthly_random_anchor_selection_only_vs_policy_trade_replay",
    }

    compare_payload["champion_vs_challenger"] = {
        "selection_only": {
            str(top_k): {
                "champion": topk_metrics[str(top_k)]["selection_only"]["champion"],
                "challenger": topk_metrics[str(top_k)]["selection_only"]["challenger"],
                "delta": topk_metrics[str(top_k)]["delta"],
            }
            for top_k in TOP_K_VALUES
        },
        "policy_trade": {
            str(top_k): {
                "champion": topk_metrics[str(top_k)]["policy_trade"]["champion"],
                "challenger": topk_metrics[str(top_k)]["policy_trade"]["challenger"],
                "delta": topk_metrics[str(top_k)]["delta"],
            }
            for top_k in TOP_K_VALUES
        },
        "side": side_metrics,
    }

    decision_reasons: list[str] = []
    if topk_metrics["20"]["selection_only"]["challenger"]["avg_ret63"] is not None and topk_metrics["20"]["selection_only"]["champion"]["avg_ret63"] is not None:
        if topk_metrics["20"]["selection_only"]["challenger"]["avg_ret63"] > topk_metrics["20"]["selection_only"]["champion"]["avg_ret63"]:
            decision_reasons.append("selection_only_improved_ret63")
    if topk_metrics["20"]["selection_only"]["challenger"]["bad_pick_rate"] is not None and topk_metrics["20"]["selection_only"]["champion"]["bad_pick_rate"] is not None:
        if topk_metrics["20"]["selection_only"]["challenger"]["bad_pick_rate"] < topk_metrics["20"]["selection_only"]["champion"]["bad_pick_rate"]:
            decision_reasons.append("bad_pick_rate_improved")
    if topk_metrics["20"]["policy_trade"]["challenger"]["net_realized_pnl"] >= topk_metrics["20"]["policy_trade"]["champion"]["net_realized_pnl"]:
        decision_reasons.append("policy_trade_not_worse")
    if compare_payload["branching_metrics"]["changed_top20_members_count"] > 0:
        decision_reasons.append("top20_branching_observed")

    decision = "keep" if (
        topk_metrics["20"]["selection_only"]["challenger"]["avg_ret63"] is not None
        and topk_metrics["20"]["selection_only"]["champion"]["avg_ret63"] is not None
        and topk_metrics["20"]["selection_only"]["challenger"]["avg_ret63"] > topk_metrics["20"]["selection_only"]["champion"]["avg_ret63"]
        and topk_metrics["20"]["selection_only"]["challenger"]["bad_pick_rate"] is not None
        and topk_metrics["20"]["selection_only"]["champion"]["bad_pick_rate"] is not None
        and topk_metrics["20"]["selection_only"]["challenger"]["bad_pick_rate"] < topk_metrics["20"]["selection_only"]["champion"]["bad_pick_rate"]
        and topk_metrics["20"]["policy_trade"]["challenger"]["net_realized_pnl"] >= topk_metrics["20"]["policy_trade"]["champion"]["net_realized_pnl"]
    ) else "hold"
    if not decision_reasons:
        decision_reasons.append("insufficient_separation_for_keep")

    compare_payload["authoritative_rollup_decision"] = decision
    compare_payload["decision_reasons"] = decision_reasons
    compare_payload["db_provenance"] = db_provenance
    compare_payload["exclusion_diagnostics"] = _aggregate_exclusion_diagnostics(exclusion_rows)

    summary_payload = {
        "schema_version": "tradex_random_anchor_replay_summary_v1",
        "generated_at": _utc_now(),
        "seed": int(seed),
        "anchor_count": len(anchors),
        "artifact_tag": artifact_tag,
        "top_k_values": list(TOP_K_VALUES),
        "authoritative_rollup_decision": decision,
        "decision_reasons": decision_reasons,
        "same_condition_contract": compare_payload["same_condition_contract"],
        "compare_metrics": compare_payload["aggregate_metrics"],
        "branching_metrics": compare_payload["branching_metrics"],
        "side_metrics": side_metrics,
        "db_provenance": db_provenance,
        "exclusion_diagnostics": compare_payload["exclusion_diagnostics"],
        "requested_source_db_path": str(source_db_path),
        "working_source_db_path": str(usable_source_db_path),
        "paths": {
            "random_anchor_dates_json": str(_artifact_path(output_dir, "random_anchor_dates.json", artifact_tag=artifact_tag)),
            "random_anchor_candidate_snapshots_json": str(_artifact_path(output_dir, "random_anchor_candidate_snapshots.json", artifact_tag=artifact_tag)),
            "selection_only_replay_ledger_json": str(_artifact_path(output_dir, "selection_only_replay_ledger.json", artifact_tag=artifact_tag)),
            "selection_only_replay_ledger_parquet": str(_artifact_path(output_dir, "selection_only_replay_ledger.parquet", artifact_tag=artifact_tag)),
            "policy_trade_replay_ledger_json": str(_artifact_path(output_dir, "policy_trade_replay_ledger.json", artifact_tag=artifact_tag)),
            "policy_trade_replay_ledger_parquet": str(_artifact_path(output_dir, "policy_trade_replay_ledger.parquet", artifact_tag=artifact_tag)),
            "champion_vs_challenger_random_anchor_compare_json": str(_artifact_path(output_dir, "champion_vs_challenger_random_anchor_compare.json", artifact_tag=artifact_tag)),
            "full_universe_gate_coverage_json": str(_artifact_path(output_dir, "full_universe_gate_coverage.json", artifact_tag=artifact_tag)),
            "random_anchor_overlap_diagnostics_json": str(_artifact_path(output_dir, "random_anchor_overlap_diagnostics.json", artifact_tag=artifact_tag)),
            "random_anchor_db_provenance_json": str(_artifact_path(output_dir, "random_anchor_db_provenance.json", artifact_tag=artifact_tag)),
            "random_anchor_exclusion_diagnostics_json": str(_artifact_path(output_dir, "random_anchor_exclusion_diagnostics.json", artifact_tag=artifact_tag)),
            "random_anchor_replay_summary_json": str(_artifact_path(output_dir, "random_anchor_replay_summary.json", artifact_tag=artifact_tag)),
        },
        "candidate_snapshot_rows_count": len(candidate_rows),
        "selection_only_replay_rows_count": len(selection_rows),
        "policy_trade_run_rows_count": len(policy_run_rows),
        "policy_trade_ledger_rows_count": len(policy_ledger_rows),
    }

    overlap_diagnostics = _anchor_overlap_diagnostics(anchors)
    _write_payload_json(_artifact_path(output_dir, "random_anchor_dates.json", artifact_tag=artifact_tag), random_anchor_dates_payload)
    _write_payload_json(_artifact_path(output_dir, "random_anchor_candidate_snapshots.json", artifact_tag=artifact_tag), {
        "schema_version": "tradex_random_anchor_candidate_snapshots_v1",
        "generated_at": _utc_now(),
        "rows": candidate_rows,
    })
    selection_only_frame = pd.DataFrame(selection_rows)
    policy_ledger_frame = pd.DataFrame(policy_ledger_rows)
    _write_payload_json(_artifact_path(output_dir, "selection_only_replay_ledger.json", artifact_tag=artifact_tag), {
        "schema_version": "tradex_selection_only_replay_ledger_v1",
        "generated_at": _utc_now(),
        "rows": selection_rows,
    })
    if not selection_only_frame.empty:
        _write_parquet(_artifact_path(output_dir, "selection_only_replay_ledger.parquet", artifact_tag=artifact_tag), selection_only_frame)
    else:
        _write_parquet(_artifact_path(output_dir, "selection_only_replay_ledger.parquet", artifact_tag=artifact_tag), pd.DataFrame(selection_rows))
    _write_payload_json(_artifact_path(output_dir, "policy_trade_replay_ledger.json", artifact_tag=artifact_tag), {
        "schema_version": "tradex_policy_trade_replay_ledger_v1",
        "generated_at": _utc_now(),
        "rows": policy_ledger_rows,
    })
    if not policy_ledger_frame.empty:
        _write_parquet(_artifact_path(output_dir, "policy_trade_replay_ledger.parquet", artifact_tag=artifact_tag), policy_ledger_frame)
    else:
        _write_parquet(_artifact_path(output_dir, "policy_trade_replay_ledger.parquet", artifact_tag=artifact_tag), pd.DataFrame(policy_ledger_rows))
    _write_payload_json(_artifact_path(output_dir, "champion_vs_challenger_random_anchor_compare.json", artifact_tag=artifact_tag), compare_payload)
    _write_payload_json(_artifact_path(output_dir, "random_anchor_overlap_diagnostics.json", artifact_tag=artifact_tag), overlap_diagnostics)
    _write_payload_json(_artifact_path(output_dir, "random_anchor_db_provenance.json", artifact_tag=artifact_tag), {
        "schema_version": "tradex_random_anchor_db_provenance_v1",
        "generated_at": _utc_now(),
        "db_provenance": db_provenance,
    })
    _write_payload_json(_artifact_path(output_dir, "random_anchor_exclusion_diagnostics.json", artifact_tag=artifact_tag), {
        "schema_version": "tradex_random_anchor_exclusion_diagnostics_v1",
        "generated_at": _utc_now(),
        "aggregate": compare_payload["exclusion_diagnostics"],
        "rows": exclusion_rows,
    })
    _write_payload_json(_artifact_path(output_dir, "full_universe_gate_coverage.json", artifact_tag=artifact_tag), {
        "schema_version": "tradex_full_universe_gate_coverage_v1",
        "generated_at": _utc_now(),
        "rows": full_universe_coverage_rows,
        "aggregate": _aggregate_full_universe_coverage(full_universe_coverage_rows),
    })
    _write_payload_json(_artifact_path(output_dir, "random_anchor_replay_summary.json", artifact_tag=artifact_tag), summary_payload)

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "decision": decision,
        "decision_reasons": decision_reasons,
        "anchors": [anchor.__dict__ for anchor in anchors],
        "compare": compare_payload,
        "summary": summary_payload,
        "paths": summary_payload["paths"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a random-anchor 3-month TRADEX replay comparison.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--anchor-count", type=int, default=DEFAULT_ANCHOR_COUNT)
    parser.add_argument("--warmup-days", type=int, default=DEFAULT_WARMUP_DAYS)
    parser.add_argument("--horizon-days", type=int, default=DEFAULT_HORIZON_DAYS)
    parser.add_argument("--pool-limit", type=int, default=DEFAULT_POOL_LIMIT)
    parser.add_argument("--artifact-tag", default="", help="Optional suffix for output artifact filenames.")
    args = parser.parse_args(argv)
    payload = run_random_anchor_replay(
        source_db_path=Path(args.source_db_path).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        seed=int(args.seed),
        anchor_count=int(args.anchor_count),
        warmup_days=int(args.warmup_days),
        horizon_days=int(args.horizon_days),
        pool_limit=int(args.pool_limit),
        artifact_tag=str(args.artifact_tag).strip() or None,
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
