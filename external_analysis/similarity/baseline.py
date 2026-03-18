from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from bisect import bisect_left
from math import sqrt
from typing import Any

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db
from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema
from external_analysis.runtime.incremental_cache import probe_similarity_cache, upsert_manifest
from external_analysis.similarity.store import connect_similarity_db, ensure_similarity_schema

EMBEDDING_VERSION = "deterministic_similarity_v1"
CHALLENGER_EMBEDDING_VERSION = "future_path_challenger_v1"
PROMOTION_REQUIRED_STREAK = 3
TOP_K = 5
LOOKBACK_DAYS = 20
LOOKFORWARD_DAYS = 20
MAX_PUBLIC_PATH_ROWS = 20
METRICS_MAX_ATTEMPTS = 3


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_as_of_date(value: str | int) -> int:
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"unsupported as_of_date: {value}")
    return int(text)


def _as_of_date_text(value: int) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _run_id(kind: str) -> str:
    return _utcnow().strftime(f"{kind}_%Y%m%dT%H%M%S%fZ")


def _default_publish_id(as_of_date: int) -> str:
    return f"pub_{_as_of_date_text(as_of_date)}"


def _clamp_top_k(value: int) -> int:
    return max(1, min(int(value), TOP_K))


def _vector_distance(left: list[float], right: list[float]) -> float:
    if len(left) != len(right):
        raise ValueError("vector length mismatch")
    return sqrt(sum((float(a) - float(b)) ** 2 for a, b in zip(left, right, strict=True)))


def _future_signature(values: list[float]) -> str:
    return ",".join(f"{value:.4f}" for value in values)


def _json_ready_dict(values: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values.items():
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def _scope_fields(
    publish_id: str,
    *,
    scope_type: str | None = None,
    scope_id: str | None = None,
) -> tuple[str, str]:
    if scope_type and scope_id:
        return str(scope_type), str(scope_id)
    if str(publish_id).startswith("replay_"):
        parts = str(publish_id).split("_")
        if len(parts) >= 4:
            return "replay", "_".join(parts[1:-1])
        return "replay", str(publish_id)[7:]
    return "nightly", "nightly"


def _challenger_embedding_generation_key(*, source_signature: str) -> str:
    return f"challenger_embeddings::{source_signature}::{CHALLENGER_EMBEDDING_VERSION}"


def _shadow_template_key(
    *,
    source_signature: str,
    top_k: int,
    query_case_limit: int | None = None,
    candidate_pool_limit: int | None = None,
    query_scope_signature: str | None = None,
) -> str:
    limit_token = "all" if query_case_limit is None else f"q{int(query_case_limit)}"
    pool_token = "all" if candidate_pool_limit is None else f"p{int(candidate_pool_limit)}"
    scope_token = "scope_all" if not query_scope_signature else f"scope_{query_scope_signature}"
    return f"{source_signature}::{CHALLENGER_EMBEDDING_VERSION}::k{int(top_k)}::{limit_token}::{pool_token}::{scope_token}"


def _shadow_template_generation_key(*, template_key: str) -> str:
    return f"challenger_shadow_template::{template_key}"


def _classify_case(*, ret_20: float, mae_20: float, top_5pct: bool) -> tuple[str, str, bool, str | None]:
    if ret_20 <= -0.15 or mae_20 <= -0.18:
        return ("pre_big_down", "big_drop", False, "big_drop")
    if ret_20 >= 0.20 or top_5pct:
        return ("pre_big_up", "big_up", True, None)
    return ("failed_setup", "failed_setup", False, "failed_setup")


def _classify_setup_family(
    *,
    ret_20: float,
    mfe_20: float,
    mae_20: float,
    future_path_points: list[float] | None = None,
) -> tuple[str, str, str]:
    future_points = [float(value) for value in (future_path_points or [])]
    if len(future_points) >= 4:
        half_index = max(1, len(future_points) // 2)
        front_half = future_points[:half_index]
        back_half = future_points[half_index:]
        front_range = (max(front_half) - min(front_half)) if front_half else 0.0
        back_max = max(back_half) if back_half else 0.0
        back_min = min(back_half) if back_half else 0.0
        if front_range <= 0.06 and back_max >= 0.12:
            return ("long", "range_break_pre_move", "up")
        if front_range <= 0.06 and back_min <= -0.12:
            return ("short", "range_break_pre_move", "down")
    if ret_20 >= 0.20 and mae_20 > -0.08:
        return ("long", "big_win_pre_move", "up")
    if ret_20 <= -0.20 and mfe_20 < 0.12:
        return ("short", "big_win_pre_move", "down")
    if mae_20 <= -0.15:
        return ("long", "big_loss_pre_move", "down")
    if mfe_20 >= 0.15:
        return ("short", "big_loss_pre_move", "up")
    if ret_20 >= 0.0:
        return ("long", "big_loss_pre_move", "none")
    return ("short", "big_loss_pre_move", "none")


def _load_daily_case_candidates(
    export_db_path: str | None,
    label_db_path: str | None,
    *,
    codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    export_conn = connect_export_db(export_db_path)
    label_conn = connect_label_db(label_db_path)
    try:
        code_filter_sql = ""
        label_code_filter_sql = ""
        params: list[Any] = []
        if codes:
            placeholders = ", ".join(["?"] * len(codes))
            code_filter_sql = f" WHERE bars.code IN ({placeholders})"
            label_code_filter_sql = f" WHERE code IN ({placeholders})"
            params = [str(code) for code in codes]
        rows = export_conn.execute(
            f"""
            WITH bars AS (
                SELECT
                    b.code,
                    b.trade_date AS as_of_date,
                    b.c,
                    b.v,
                    i.ma20,
                    AVG(b.v) OVER (
                        PARTITION BY b.code
                        ORDER BY b.trade_date
                        ROWS BETWEEN {LOOKBACK_DAYS} PRECEDING AND 1 PRECEDING
                    ) AS avg_volume_prev,
                    LAG(b.c, {LOOKBACK_DAYS}) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS close_lookback
                FROM bars_daily_export b
                LEFT JOIN indicator_daily_export i
                  ON i.code = b.code AND i.trade_date = b.trade_date
            )
            SELECT
                bars.code,
                bars.as_of_date,
                bars.c,
                bars.v,
                bars.ma20,
                bars.avg_volume_prev,
                bars.close_lookback
            FROM bars
            {code_filter_sql}
            ORDER BY bars.code, bars.as_of_date
            """,
            params,
        ).fetchall()
        label_rows = label_conn.execute(
            f"""
            SELECT
                code,
                as_of_date,
                ret_h,
                mfe_h,
                mae_h,
                top_5pct_h
            FROM label_daily_h20
            {label_code_filter_sql}
            """,
            params,
        ).fetchall()
        anchor_rows = label_conn.execute(
            f"""
            SELECT
                anchor_id,
                code,
                anchor_type,
                anchor_date,
                outcome_ret_20,
                outcome_mfe_20,
                outcome_mae_20
            FROM anchor_window_master
            {label_code_filter_sql}
            """,
            params,
        ).fetchall()
        anchor_bar_rows = label_conn.execute(
            """
            SELECT
                anchor_id,
                rel_day,
                trade_date,
                c,
                v,
                ma20
            FROM anchor_window_bars
            ORDER BY anchor_id, rel_day
            """
        ).fetchall()
    finally:
        export_conn.close()
        label_conn.close()
    labels_by_key = {
        (str(row[0]), int(row[1])): {
            "ret_20": float(row[2]) if row[2] is not None else None,
            "mfe_20": float(row[3]) if row[3] is not None else None,
            "mae_20": float(row[4]) if row[4] is not None else None,
            "top_5pct": bool(row[5]),
        }
        for row in label_rows
    }
    daily_cases: list[dict[str, Any]] = []
    for code, as_of_date, close_price, volume_value, ma20, avg_volume_prev, close_lookback in rows:
        key = (str(code), int(as_of_date))
        label = labels_by_key.get(key)
        if not label or label["ret_20"] is None or close_price in (None, 0) or close_lookback in (None, 0):
            continue
        close_price_f = float(close_price)
        volume_norm = 1.0 if not avg_volume_prev or float(avg_volume_prev) <= 0 else float(volume_value or 0.0) / float(avg_volume_prev)
        ma20_gap = 0.0 if ma20 in (None, 0) else (close_price_f / float(ma20)) - 1.0
        momentum = (close_price_f / float(close_lookback)) - 1.0
        case_type, outcome_class, success_flag, failure_reason = _classify_case(
            ret_20=float(label["ret_20"]),
            mae_20=float(label["mae_20"] or 0.0),
            top_5pct=bool(label["top_5pct"]),
        )
        future_path = [
            round(float(label["ret_20"]) * ratio, 6)
            for ratio in (0.25, 0.5, 0.75, 1.0)
        ]
        trade_side, setup_family, break_direction = _classify_setup_family(
            ret_20=float(label["ret_20"] or 0.0),
            mfe_20=float(label["mfe_20"] or 0.0),
            mae_20=float(label["mae_20"] or 0.0),
            future_path_points=future_path,
        )
        vector = [
            round(momentum, 6),
            round(ma20_gap, 6),
            round(volume_norm - 1.0, 6),
            round(float(label["ret_20"]), 6),
        ]
        daily_cases.append(
            {
                "case_id": f"daily:{code}:{as_of_date}",
                "query_source": "daily_window_query",
                "case_type": case_type,
                "anchor_type": None,
                "code": str(code),
                "anchor_date": int(as_of_date),
                "asof_start_date": int(as_of_date),
                "asof_end_date": int(as_of_date),
                "outcome_class": outcome_class,
                "success_flag": success_flag,
                "failure_reason": failure_reason,
                "trade_side": trade_side,
                "setup_family": setup_family,
                "break_direction": break_direction,
                "future_path_signature": _future_signature(future_path),
                "embedding_version": EMBEDDING_VERSION,
                "source_snapshot_id": "export_label_snapshot",
                "vector": vector,
                    "path_rows": [
                        {
                            "rel_day": rel_day,
                            "trade_date": int(as_of_date),
                            "close_norm": (
                                float(momentum)
                                if rel_day < 0
                                else (0.0 if rel_day == 0 else float(future_path[min(rel_day - 1, len(future_path) - 1)]))
                            ),
                            "volume_norm": float(volume_norm),
                            "ma20_gap": float(ma20_gap),
                        }
                    for rel_day in range(-LOOKBACK_DAYS, LOOKFORWARD_DAYS + 1)
                ],
            }
        )
    anchor_bars_by_id: dict[str, list[dict[str, Any]]] = {}
    for anchor_id, rel_day, trade_date, close_price, volume_value, ma20 in anchor_bar_rows:
        anchor_bars_by_id.setdefault(str(anchor_id), []).append(
            {
                "rel_day": int(rel_day),
                "trade_date": int(trade_date),
                "c": float(close_price or 0.0),
                "v": float(volume_value or 0.0),
                "ma20": float(ma20 or 0.0) if ma20 is not None else None,
            }
        )
    for anchor_id, code, anchor_type, anchor_date, outcome_ret_20, outcome_mfe_20, outcome_mae_20 in anchor_rows:
        bars = sorted(anchor_bars_by_id.get(str(anchor_id), []), key=lambda item: int(item["rel_day"]))
        current_bar = next((item for item in bars if item["rel_day"] == 0), None)
        if current_bar is None or current_bar["c"] in (None, 0):
            continue
        ma20_gap = 0.0 if current_bar.get("ma20") in (None, 0.0) else (float(current_bar["c"]) / float(current_bar["ma20"])) - 1.0
        volume_base = [item["v"] for item in bars if item["rel_day"] < 0 and item["v"] > 0]
        avg_volume = sum(volume_base) / len(volume_base) if volume_base else 0.0
        volume_norm = 1.0 if avg_volume <= 0 else float(current_bar["v"]) / avg_volume
        case_type, outcome_class, success_flag, failure_reason = _classify_case(
            ret_20=float(outcome_ret_20 or 0.0),
            mae_20=float(outcome_mae_20 or 0.0),
            top_5pct=bool(float(outcome_mfe_20 or 0.0) >= 0.2),
        )
        future_path_points = [
            0.0 if current_bar["c"] == 0 else (float(item["c"]) / float(current_bar["c"])) - 1.0
            for item in bars
            if int(item["rel_day"]) > 0
        ]
        trade_side, setup_family, break_direction = _classify_setup_family(
            ret_20=float(outcome_ret_20 or 0.0),
            mfe_20=float(outcome_mfe_20 or 0.0),
            mae_20=float(outcome_mae_20 or 0.0),
            future_path_points=future_path_points,
        )
        vector = [
            round(ma20_gap, 6),
            round(volume_norm - 1.0, 6),
            round(float(outcome_ret_20 or 0.0), 6),
            round(float(outcome_mae_20 or 0.0), 6),
        ]
        daily_cases.append(
            {
                "case_id": str(anchor_id),
                "query_source": "anchor_window_query",
                "case_type": case_type,
                "anchor_type": str(anchor_type),
                "code": str(code),
                "anchor_date": int(anchor_date),
                "asof_start_date": min(int(item["trade_date"]) for item in bars),
                "asof_end_date": max(int(item["trade_date"]) for item in bars),
                "outcome_class": outcome_class,
                "success_flag": success_flag,
                "failure_reason": failure_reason,
                "trade_side": trade_side,
                "setup_family": setup_family,
                "break_direction": break_direction,
                "future_path_signature": _future_signature([float(outcome_ret_20 or 0.0), float(outcome_mfe_20 or 0.0), float(outcome_mae_20 or 0.0)]),
                "embedding_version": EMBEDDING_VERSION,
                "source_snapshot_id": "anchor_snapshot",
                "vector": vector,
                "path_rows": [
                    {
                        "rel_day": int(item["rel_day"]),
                        "trade_date": int(item["trade_date"]),
                        "close_norm": 0.0 if current_bar["c"] == 0 else (float(item["c"]) / float(current_bar["c"])) - 1.0,
                        "volume_norm": 1.0 if avg_volume <= 0 else float(item["v"]) / avg_volume,
                        "ma20_gap": 0.0 if item.get("ma20") in (None, 0.0) else (float(item["c"]) / float(item["ma20"])) - 1.0,
                    }
                    for item in bars
                ],
            }
        )
    return daily_cases


def build_case_library(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    similarity_db_path: str | None = None,
    as_of_date: str | int | None = None,
    codes: list[str] | None = None,
) -> dict[str, Any]:
    if as_of_date is None:
        raise ValueError("as_of_date is required")
    target_as_of = _normalize_as_of_date(as_of_date)
    run_id = _run_id("case")
    probe = probe_similarity_cache(
        export_db_path=export_db_path,
        similarity_db_path=similarity_db_path,
        generation_key="case_library",
        dependency_version=EMBEDDING_VERSION,
    )
    effective_codes = None if codes is None else [str(code) for code in codes]
    if probe["action"] == "partial" and probe["dirty_ranges"]:
        dirty_codes = {str(item["code"]) for item in probe["dirty_ranges"]}
        if effective_codes is None:
            effective_codes = sorted(dirty_codes)
        else:
            effective_codes = [code for code in effective_codes if code in dirty_codes]
    if probe["action"] == "skip":
        return {
            "ok": True,
            "run_id": run_id,
            "case_count": 0,
            "embedding_version": EMBEDDING_VERSION,
            "skipped": True,
            "cache_state": probe["cache_state"],
            "reason": probe["reason"],
            "dirty_ranges": [],
            "source_signature": probe.get("source_signature"),
        }
    cases = _load_daily_case_candidates(export_db_path, label_db_path, codes=effective_codes)
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            if probe["action"] == "partial" and effective_codes:
                conn.execute(
                    f"DELETE FROM case_window_bars WHERE case_id IN (SELECT case_id FROM case_library WHERE code IN ({', '.join(['?'] * len(effective_codes))}))",
                    effective_codes,
                )
                conn.execute(
                    f"DELETE FROM case_embedding_store WHERE case_id IN (SELECT case_id FROM case_library WHERE code IN ({', '.join(['?'] * len(effective_codes))}))",
                    effective_codes,
                )
                conn.execute(
                    f"DELETE FROM case_library WHERE code IN ({', '.join(['?'] * len(effective_codes))})",
                    effective_codes,
                )
            else:
                conn.execute("DELETE FROM case_library")
                conn.execute("DELETE FROM case_window_bars")
                conn.execute("DELETE FROM case_embedding_store")
            for case in cases:
                case_row = {
                    key: case[key]
                    for key in (
                        "case_id",
                        "query_source",
                        "case_type",
                        "anchor_type",
                        "code",
                        "anchor_date",
                        "asof_start_date",
                        "asof_end_date",
                        "outcome_class",
                        "success_flag",
                        "failure_reason",
                        "trade_side",
                        "setup_family",
                        "break_direction",
                        "future_path_signature",
                        "embedding_version",
                        "source_snapshot_id",
                    )
                }
                case_row["generation_run_id"] = run_id
                columns = list(case_row.keys())
                conn.execute(
                    f"INSERT INTO case_library ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                    [case_row[column] for column in columns],
                )
                conn.execute(
                    """
                    INSERT INTO case_embedding_store (case_id, embedding_version, embedding_role, vector_json, generated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [case["case_id"], EMBEDDING_VERSION, "champion", json.dumps(case["vector"], ensure_ascii=False), _utcnow()],
                )
                if case["path_rows"]:
                    conn.executemany(
                        """
                        INSERT INTO case_window_bars (
                            case_id, rel_day, trade_date, close_norm, volume_norm, ma20_gap, generation_run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            [
                                case["case_id"],
                                int(path_row["rel_day"]),
                                int(path_row["trade_date"]),
                                float(path_row["close_norm"]),
                                float(path_row["volume_norm"]),
                                float(path_row["ma20_gap"]),
                                run_id,
                            ]
                            for path_row in case["path_rows"]
                        ],
                    )
            summary = {
                "case_count": len(cases),
                "success_count": len([case for case in cases if case["success_flag"]]),
                "failure_count": len([case for case in cases if not case["success_flag"]]),
                "target_as_of_date": target_as_of,
            }
            conn.execute(
                """
                INSERT OR REPLACE INTO case_generation_runs (
                    run_id, kind, status, as_of_date, publish_id, started_at, finished_at, summary_json
                ) VALUES (?, ?, ?, CAST(? AS DATE), ?, ?, ?, ?)
                """,
                [run_id, "case_library_build", "success", _as_of_date_text(target_as_of), None, _utcnow(), _utcnow(), json.dumps(summary, ensure_ascii=False, sort_keys=True)],
            )
            total_case_count = int(conn.execute("SELECT COUNT(*) FROM case_library").fetchone()[0])
            upsert_manifest(
                conn=conn,
                table_name="similarity_generation_manifest",
                generation_key="case_library",
                source_signature=str(probe.get("source_signature") or ""),
                dependency_version=EMBEDDING_VERSION,
                cache_state="partial_stale" if probe["action"] == "partial" else "fresh",
                row_count=total_case_count,
                dirty_ranges=probe["dirty_ranges"],
                run_id=run_id,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return {
        "ok": True,
        "run_id": run_id,
        "case_count": len(cases),
        "embedding_version": EMBEDDING_VERSION,
        "skipped": False,
        "cache_state": "partial_stale" if probe["action"] == "partial" else "fresh",
        "reason": probe["reason"],
        "dirty_ranges": probe["dirty_ranges"],
        "source_signature": probe.get("source_signature"),
    }


def _load_case_vectors(
    similarity_db_path: str | None = None,
    *,
    embedding_version: str = EMBEDDING_VERSION,
) -> tuple[dict[str, list[float]], dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    conn = connect_similarity_db(similarity_db_path)
    try:
        embedding_rows = conn.execute("SELECT case_id, vector_json FROM case_embedding_store WHERE embedding_version = ?", [embedding_version]).fetchall()
        library_rows = conn.execute(
            """
            SELECT
                case_id, query_source, case_type, anchor_type, code, anchor_date, outcome_class, success_flag, failure_reason,
                trade_side, setup_family, break_direction
            FROM case_library
            """
        ).fetchall()
        path_rows = conn.execute(
            """
            SELECT case_id, rel_day, close_norm, volume_norm
            FROM case_window_bars
            WHERE rel_day BETWEEN 1 AND 20
            ORDER BY case_id, rel_day
            """
        ).fetchall()
    finally:
        conn.close()
    vectors = {str(row[0]): [float(value) for value in json.loads(row[1])] for row in embedding_rows}
    library = {
        str(row[0]): {
            "case_id": str(row[0]),
            "query_source": str(row[1]),
            "case_type": str(row[2]),
            "anchor_type": None if row[3] is None else str(row[3]),
            "code": str(row[4]),
            "anchor_date": int(row[5]),
            "outcome_class": str(row[6]),
            "success_flag": bool(row[7]),
            "failure_reason": None if row[8] is None else str(row[8]),
            "trade_side": None if row[9] is None else str(row[9]),
            "setup_family": None if row[10] is None else str(row[10]),
            "break_direction": None if row[11] is None else str(row[11]),
        }
        for row in library_rows
    }
    paths: dict[str, list[dict[str, Any]]] = {}
    for case_id, rel_day, close_norm, volume_norm in path_rows:
        paths.setdefault(str(case_id), []).append(
            {
                "rel_day": int(rel_day),
                "path_return_norm": float(close_norm or 0.0),
                "path_volume_norm": float(volume_norm or 0.0),
            }
        )
    return vectors, library, paths


def _load_embedding_vectors(
    similarity_db_path: str | None = None,
    *,
    embedding_version: str,
    case_ids: list[str] | None = None,
) -> dict[str, list[float]]:
    conn = connect_similarity_db(similarity_db_path)
    try:
        if case_ids:
            rows = conn.execute(
                f"SELECT case_id, vector_json FROM case_embedding_store WHERE embedding_version = ? AND case_id IN ({', '.join(['?'] * len(case_ids))})",
                [embedding_version, *case_ids],
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT case_id, vector_json FROM case_embedding_store WHERE embedding_version = ?",
                [embedding_version],
            ).fetchall()
    finally:
        conn.close()
    return {str(row[0]): [float(value) for value in json.loads(row[1])] for row in rows}


def _load_query_vectors(export_db_path: str | None, as_of_date: int, *, codes: list[str] | None = None) -> list[dict[str, Any]]:
    export_conn = connect_export_db(export_db_path)
    try:
        code_filter_sql = ""
        params: list[Any] = [as_of_date]
        if codes:
            code_filter_sql = f" AND code IN ({', '.join(['?'] * len(codes))})"
            params.extend([str(code) for code in codes])
        rows = export_conn.execute(
            f"""
            WITH enriched AS (
                SELECT
                    b.code,
                    b.trade_date AS as_of_date,
                    b.c,
                    b.v,
                    i.ma20,
                    AVG(b.v) OVER (
                        PARTITION BY b.code
                        ORDER BY b.trade_date
                        ROWS BETWEEN {LOOKBACK_DAYS} PRECEDING AND 1 PRECEDING
                    ) AS avg_volume_prev,
                    LAG(b.c, {LOOKBACK_DAYS}) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS close_lookback
                FROM bars_daily_export b
                LEFT JOIN indicator_daily_export i
                  ON i.code = b.code AND i.trade_date = b.trade_date
            )
            SELECT code, as_of_date, c, v, ma20, avg_volume_prev, close_lookback
            FROM enriched
            WHERE as_of_date = ?{code_filter_sql}
            ORDER BY code
            """,
            params,
        ).fetchall()
    finally:
        export_conn.close()
    query_rows: list[dict[str, Any]] = []
    for code, row_as_of, close_price, volume_value, ma20, avg_volume_prev, close_lookback in rows:
        if close_price in (None, 0) or close_lookback in (None, 0):
            continue
        close_price_f = float(close_price)
        ma20_gap = 0.0 if ma20 in (None, 0) else (close_price_f / float(ma20)) - 1.0
        volume_norm = 1.0 if not avg_volume_prev or float(avg_volume_prev) <= 0 else float(volume_value or 0.0) / float(avg_volume_prev)
        momentum = (close_price_f / float(close_lookback)) - 1.0
        query_rows.append(
            {
                "code": str(code),
                "as_of_date": int(row_as_of),
                "momentum": round(momentum, 6),
                "ma20_gap": round(ma20_gap, 6),
                "volume_norm": round(volume_norm, 6),
                "vector": [
                    round(momentum, 6),
                    round(ma20_gap, 6),
                    round(volume_norm - 1.0, 6),
                    round(momentum, 6),
                ],
            }
        )
    return query_rows


def _classify_query_setup(
    *,
    momentum: float,
    ma20_gap: float,
    volume_norm: float,
) -> tuple[str, str, str]:
    if abs(momentum) <= 0.05 and abs(ma20_gap) <= 0.04 and volume_norm >= 1.3:
        return ("long" if momentum >= 0 else "short", "range_break_pre_move", "up" if momentum >= 0 else "down")
    if momentum >= 0.10 or ma20_gap >= 0.05:
        return ("long", "big_win_pre_move", "up")
    if momentum <= -0.10 or ma20_gap <= -0.05:
        return ("short", "big_win_pre_move", "down")
    if momentum >= 0:
        return ("short", "big_loss_pre_move", "up")
    return ("long", "big_loss_pre_move", "down")


def _derive_focus_setups(query_rows: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
    setups: set[tuple[str, str, str]] = set()
    for row in query_rows:
        setups.add(
            _classify_query_setup(
                momentum=float(row.get("momentum") or 0.0),
                ma20_gap=float(row.get("ma20_gap") or 0.0),
                volume_norm=float(row.get("volume_norm") or 1.0),
            )
        )
    return setups


def _build_challenger_embedding(
    *,
    base_vector: list[float],
    path_rows: list[dict[str, Any]],
) -> list[float]:
    future_rows = [row for row in path_rows if int(row["rel_day"]) > 0]
    if not future_rows:
        return list(base_vector)
    returns = [float(row["path_return_norm"]) for row in future_rows]
    volume_values = [float(row["path_volume_norm"]) for row in future_rows]
    future_finish = returns[-1]
    future_peak = max(returns)
    future_trough = min(returns)
    future_mean = sum(returns) / len(returns)
    future_volume_mean = sum(volume_values) / len(volume_values) if volume_values else 1.0
    return [
        round((float(base_vector[0]) * 0.55) + (future_finish * 0.45), 6),
        round((float(base_vector[1]) * 0.45) + (future_peak * 0.55), 6),
        round((float(base_vector[2]) * 0.45) + (future_trough * 0.55), 6),
        round((float(base_vector[3]) * 0.35) + (future_mean * 0.45) + ((future_volume_mean - 1.0) * 0.20), 6),
    ]


def _persist_challenger_embeddings(
    *,
    similarity_db_path: str | None,
    challenger_vectors: dict[str, list[float]],
) -> None:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            if challenger_vectors:
                for case_id, vector in challenger_vectors.items():
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO case_embedding_store (
                            case_id, embedding_version, embedding_role, vector_json, generated_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        [case_id, CHALLENGER_EMBEDDING_VERSION, "challenger", json.dumps(vector, ensure_ascii=False), _utcnow()],
                    )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def _load_generation_manifest_entry(
    *,
    similarity_db_path: str | None,
    generation_key: str,
) -> dict[str, Any] | None:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        row = conn.execute(
            """
            SELECT generation_key, source_signature, dependency_version, cache_state, dirty_ranges_json, row_count, generation_run_id
            FROM similarity_generation_manifest
            WHERE generation_key = ?
            """,
            [generation_key],
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return {
        "generation_key": str(row[0]),
        "source_signature": str(row[1]),
        "dependency_version": str(row[2]),
        "cache_state": str(row[3]),
        "dirty_ranges": json.loads(row[4]) if isinstance(row[4], str) else (row[4] or []),
        "row_count": int(row[5]),
        "generation_run_id": str(row[6]),
    }


def _persist_shadow_template_rows(
    *,
    similarity_db_path: str | None,
    template_key: str,
    template_rows: list[dict[str, Any]],
) -> str:
    run_id = str(template_rows[0]["run_id"]) if template_rows else _run_id("challenger_shadow_template")
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DELETE FROM similarity_shadow_template_rows WHERE template_key = ?", [template_key])
            if template_rows:
                columns = list(template_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO similarity_shadow_template_rows ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                    [[row[column] for column in columns] for row in template_rows],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return run_id


def _load_shadow_template_rows(
    *,
    similarity_db_path: str | None,
    template_key: str,
) -> list[dict[str, Any]]:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        rows = conn.execute(
            """
            SELECT template_key, run_id, query_case_id, query_code, embedding_version, neighbor_rank, case_id,
                   neighbor_code, outcome_class, success_flag, similarity_score, created_at
            FROM similarity_shadow_template_rows
            WHERE template_key = ?
            ORDER BY query_case_id, neighbor_rank
            """,
            [template_key],
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "template_key": str(row[0]),
            "run_id": str(row[1]),
            "query_case_id": str(row[2]),
            "query_code": str(row[3]),
            "embedding_version": str(row[4]),
            "neighbor_rank": int(row[5]),
            "case_id": str(row[6]),
            "neighbor_code": str(row[7]),
            "outcome_class": str(row[8]),
            "success_flag": bool(row[9]),
            "similarity_score": None if row[10] is None else float(row[10]),
            "created_at": row[11],
        }
        for row in rows
    ]


def _select_query_case_ids(
    *,
    challenger_vectors: dict[str, list[float]],
    library: dict[str, dict[str, Any]],
    query_case_limit: int | None,
    focus_codes: set[str] | None = None,
) -> list[str]:
    ordered = sorted(
        challenger_vectors.keys(),
        key=lambda case_id: (
            int((library.get(case_id) or {}).get("anchor_date") or 0),
            str((library.get(case_id) or {}).get("code") or ""),
            str(case_id),
        ),
    )
    if focus_codes:
        focused = [case_id for case_id in ordered if str((library.get(case_id) or {}).get("code") or "") in focus_codes]
        if focused:
            ordered = focused
    if query_case_limit is None or int(query_case_limit) <= 0 or len(ordered) <= int(query_case_limit):
        return ordered
    limit = int(query_case_limit)
    if limit == 1:
        return [ordered[-1]]
    last_index = len(ordered) - 1
    selected_indexes = {
        min(last_index, int(round(position * last_index / (limit - 1))))
        for position in range(limit)
    }
    return [ordered[index] for index in sorted(selected_indexes)]


def _query_scope_signature(
    *,
    focus_codes: set[str] | None,
    focus_setups: set[tuple[str, str, str]] | None = None,
) -> str | None:
    tokens: list[str] = []
    if focus_codes:
        tokens.extend(sorted(focus_codes))
    if focus_setups:
        tokens.extend(["|".join(item) for item in sorted(focus_setups)])
    if not tokens:
        return None
    digest = hashlib.sha1(",".join(tokens).encode("utf-8")).hexdigest()
    return digest[:12]


def _select_candidate_pool_ids(
    *,
    ordered_case_ids: list[str],
    ordered_anchor_dates: list[int],
    query_case_id: str,
    library: dict[str, dict[str, Any]],
    candidate_pool_limit: int | None,
) -> list[str]:
    if candidate_pool_limit is None or int(candidate_pool_limit) <= 0 or len(ordered_case_ids) <= int(candidate_pool_limit):
        return ordered_case_ids
    query_meta = library.get(query_case_id) or {}
    query_anchor_date = int(query_meta.get("anchor_date") or 0)
    limit = max(1, int(candidate_pool_limit))
    pivot = bisect_left(ordered_anchor_dates, query_anchor_date)
    left = max(0, pivot - (limit // 2))
    right = min(len(ordered_case_ids), left + limit)
    left = max(0, right - limit)
    return ordered_case_ids[left:right]


def _plan_challenger_working_set(
    *,
    champion_vectors: dict[str, list[float]],
    library: dict[str, dict[str, Any]],
    query_case_limit: int | None,
    candidate_pool_limit: int | None,
    focus_codes: set[str] | None,
    focus_setups: set[tuple[str, str, str]] | None,
) -> dict[str, Any]:
    filtered_case_ids = [
        case_id
        for case_id in champion_vectors.keys()
        if not focus_setups
        or (
            (
                str((library.get(case_id) or {}).get("trade_side") or ""),
                str((library.get(case_id) or {}).get("setup_family") or ""),
                str((library.get(case_id) or {}).get("break_direction") or ""),
            )
            in focus_setups
        )
    ]
    if not filtered_case_ids:
        filtered_case_ids = list(champion_vectors.keys())
    ordered_case_ids = sorted(
        filtered_case_ids,
        key=lambda case_id: (
            int((library.get(case_id) or {}).get("anchor_date") or 0),
            str((library.get(case_id) or {}).get("code") or ""),
            str(case_id),
        ),
    )
    ordered_anchor_dates = [int((library.get(case_id) or {}).get("anchor_date") or 0) for case_id in ordered_case_ids]
    selected_query_ids = _select_query_case_ids(
        challenger_vectors=champion_vectors,
        library=library,
        query_case_limit=query_case_limit,
        focus_codes=focus_codes,
    )
    working_set_ids: set[str] = set(selected_query_ids)
    for query_case_id in selected_query_ids:
        working_set_ids.update(
            _select_candidate_pool_ids(
                ordered_case_ids=ordered_case_ids,
                ordered_anchor_dates=ordered_anchor_dates,
                query_case_id=query_case_id,
                library=library,
                candidate_pool_limit=candidate_pool_limit,
            )
        )
    return {
        "ordered_case_ids": ordered_case_ids,
        "ordered_anchor_dates": ordered_anchor_dates,
        "selected_query_ids": selected_query_ids,
        "working_set_ids": sorted(
            working_set_ids,
            key=lambda case_id: (
                int((library.get(case_id) or {}).get("anchor_date") or 0),
                str((library.get(case_id) or {}).get("code") or ""),
                str(case_id),
            ),
        ),
    }


def _build_shadow_template(
    *,
    template_key: str,
    challenger_vectors: dict[str, list[float]],
    champion_vectors: dict[str, list[float]],
    library: dict[str, dict[str, Any]],
    top_k: int,
    query_case_limit: int | None = None,
    candidate_pool_limit: int | None = None,
    focus_codes: set[str] | None = None,
    selected_query_ids: list[str] | None = None,
    ordered_case_ids: list[str] | None = None,
    ordered_anchor_dates: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    shadow_run_id = _run_id("challenger_shadow")
    template_rows: list[dict[str, Any]] = []
    overlap_scores: list[float] = []
    success_hits = 0
    failure_hits = 0
    big_drop_hits = 0
    returned = 0
    ordered_case_ids = ordered_case_ids or sorted(
        challenger_vectors.keys(),
        key=lambda case_id: (
            int((library.get(case_id) or {}).get("anchor_date") or 0),
            str((library.get(case_id) or {}).get("code") or ""),
            str(case_id),
        ),
    )
    ordered_anchor_dates = ordered_anchor_dates or [int((library.get(case_id) or {}).get("anchor_date") or 0) for case_id in ordered_case_ids]
    resolved_query_ids = selected_query_ids or _select_query_case_ids(
        challenger_vectors=challenger_vectors,
        library=library,
        query_case_limit=query_case_limit,
        focus_codes=focus_codes,
    )
    for query_case_id in resolved_query_ids:
        query_vector = challenger_vectors[query_case_id]
        query_meta = library.get(query_case_id)
        if not query_meta:
            continue
        candidate_pool_ids = _select_candidate_pool_ids(
            ordered_case_ids=ordered_case_ids,
            ordered_anchor_dates=ordered_anchor_dates,
            query_case_id=query_case_id,
            library=library,
            candidate_pool_limit=candidate_pool_limit,
        )
        challenger_scored = []
        champion_scored = []
        for case_id in candidate_pool_ids:
            vector = challenger_vectors[case_id]
            case_meta = library.get(case_id)
            if not case_meta or case_id == query_case_id or case_meta["code"] == query_meta["code"]:
                continue
            challenger_scored.append((case_id, _vector_distance(query_vector, vector)))
        for case_id in candidate_pool_ids:
            vector = champion_vectors[case_id]
            case_meta = library.get(case_id)
            if not case_meta or case_id == query_case_id or case_meta["code"] == query_meta["code"]:
                continue
            champion_scored.append((case_id, _vector_distance(champion_vectors[query_case_id], vector)))
        challenger_scored.sort(key=lambda item: (float(item[1]), str(item[0])))
        champion_scored.sort(key=lambda item: (float(item[1]), str(item[0])))
        challenger_top = challenger_scored[:top_k]
        champion_top_ids = {case_id for case_id, _ in champion_scored[:top_k]}
        if challenger_top:
            overlap_scores.append(len([case_id for case_id, _ in challenger_top if case_id in champion_top_ids]) / len(challenger_top))
        for neighbor_rank, (case_id, distance) in enumerate(challenger_top, start=1):
            case_meta = library[case_id]
            returned += 1
            if bool(case_meta["success_flag"]):
                success_hits += 1
            else:
                failure_hits += 1
            if str(case_meta["outcome_class"]) == "big_drop":
                big_drop_hits += 1
            template_rows.append(
                {
                    "template_key": template_key,
                    "run_id": shadow_run_id,
                    "query_case_id": query_case_id,
                    "query_code": query_meta["code"],
                    "embedding_version": CHALLENGER_EMBEDDING_VERSION,
                    "neighbor_rank": neighbor_rank,
                    "case_id": case_id,
                    "neighbor_code": case_meta["code"],
                    "outcome_class": case_meta["outcome_class"],
                    "success_flag": bool(case_meta["success_flag"]),
                    "similarity_score": round(1.0 / (1.0 + float(distance)), 6),
                    "created_at": _utcnow(),
                }
            )
    metrics = {
        "engine_role": "challenger",
        "baseline_version": EMBEDDING_VERSION,
        "embedding_version": CHALLENGER_EMBEDDING_VERSION,
        "comparison_target_version": EMBEDDING_VERSION,
        "top_k": int(top_k),
        "case_count": len(challenger_vectors),
        "success_count": sum(1 for item in library.values() if bool(item.get("success_flag"))),
        "failure_count": sum(1 for item in library.values() if not bool(item.get("success_flag"))),
        "big_drop_count": sum(1 for item in library.values() if str(item.get("outcome_class")) == "big_drop"),
        "query_count": len(resolved_query_ids),
        "returned_case_count": returned,
        "returned_path_count": 0,
        "avg_similarity_score": (sum(float(row["similarity_score"]) for row in template_rows) / len(template_rows)) if template_rows else None,
        "overlap_at_k": (sum(overlap_scores) / len(overlap_scores)) if overlap_scores else None,
        "success_hit_rate_at_k": (success_hits / returned) if returned else None,
        "failure_hit_rate_at_k": (failure_hits / returned) if returned else None,
        "big_drop_hit_rate_at_k": (big_drop_hits / returned) if returned else None,
    }
    return template_rows, metrics


def _materialize_shadow_rows(
    *,
    publish_id: str,
    as_of_date: int,
    template_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in template_rows:
        payload = dict(row)
        payload.pop("template_key", None)
        payload["publish_id"] = publish_id
        payload["as_of_date"] = _as_of_date_text(as_of_date)
        rows.append(payload)
    return rows


def _persist_shadow_rows(
    *,
    similarity_db_path: str | None,
    publish_id: str,
    shadow_rows: list[dict[str, Any]],
) -> str:
    run_id = str(shadow_rows[0]["run_id"]) if shadow_rows else _run_id("challenger_shadow")
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                "DELETE FROM similarity_shadow_cases WHERE publish_id = ? AND embedding_version = ?",
                [publish_id, CHALLENGER_EMBEDDING_VERSION],
            )
            if shadow_rows:
                columns = list(shadow_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO similarity_shadow_cases ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                    [[row[column] for column in columns] for row in shadow_rows],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return run_id


def _build_similarity_metrics(
    *,
    publish_id: str,
    as_of_date: int,
    case_vectors: dict[str, list[float]],
    library: dict[str, dict[str, Any]],
    query_rows: list[dict[str, Any]],
    similar_rows: list[dict[str, Any]],
    path_rows: list[dict[str, Any]],
    top_k: int,
) -> dict[str, Any]:
    success_count = sum(1 for item in library.values() if bool(item.get("success_flag")))
    big_drop_count = sum(1 for item in library.values() if str(item.get("outcome_class")) == "big_drop")
    scores = [float(row["similarity_score"]) for row in similar_rows if row.get("similarity_score") is not None]
    returned_case_count = len(similar_rows)
    success_hits = sum(1 for row in similar_rows if bool(row.get("success_flag")))
    big_drop_hits = sum(1 for row in similar_rows if str(row.get("outcome_class")) == "big_drop")
    return {
        "run_id": _run_id("similarity_metrics"),
        "publish_id": publish_id,
        "as_of_date": _as_of_date_text(as_of_date),
        "engine_role": "champion",
        "baseline_version": EMBEDDING_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "comparison_target_version": EMBEDDING_VERSION,
        "top_k": int(top_k),
        "case_count": len(case_vectors),
        "success_count": success_count,
        "failure_count": max(0, len(library) - success_count),
        "big_drop_count": big_drop_count,
        "query_count": len(query_rows),
        "returned_case_count": returned_case_count,
        "returned_path_count": len(path_rows),
        "avg_similarity_score": (sum(scores) / len(scores)) if scores else None,
        "overlap_at_k": None,
        "success_hit_rate_at_k": (success_hits / returned_case_count) if returned_case_count else None,
        "failure_hit_rate_at_k": ((returned_case_count - success_hits) / returned_case_count) if returned_case_count else None,
        "big_drop_hit_rate_at_k": (big_drop_hits / returned_case_count) if returned_case_count else None,
        "created_at": _utcnow(),
    }


def _materialize_similarity_quality_metrics(
    *,
    metrics_template: dict[str, Any],
    publish_id: str,
    as_of_date: int,
    scope_type: str | None = None,
    scope_id: str | None = None,
    producer_work_id: str | None = None,
) -> dict[str, Any]:
    resolved_scope_type, resolved_scope_id = _scope_fields(
        publish_id,
        scope_type=scope_type,
        scope_id=scope_id,
    )
    payload = dict(metrics_template)
    payload["run_id"] = _run_id("similarity_metrics")
    payload["publish_id"] = publish_id
    payload["as_of_date"] = _as_of_date_text(as_of_date)
    payload["scope_type"] = resolved_scope_type
    payload["scope_id"] = resolved_scope_id
    payload["producer_work_id"] = producer_work_id
    payload["created_at"] = _utcnow()
    return payload


def _persist_similarity_quality_metrics(
    *,
    similarity_db_path: str | None,
    metrics: dict[str, Any],
) -> str:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                DELETE FROM similarity_quality_metrics
                WHERE publish_id = ? AND engine_role = ? AND embedding_version = ?
                """,
                [metrics["publish_id"], metrics["engine_role"], metrics["embedding_version"]],
            )
            columns = list(metrics.keys())
            conn.execute(
                f"INSERT INTO similarity_quality_metrics ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [metrics[column] for column in columns],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return str(metrics["run_id"])


def persist_similarity_quality_metrics_with_retry(
    *,
    similarity_db_path: str | None,
    metrics: dict[str, Any],
    max_attempts: int = METRICS_MAX_ATTEMPTS,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, int(max_attempts) + 1):
        try:
            run_id = _persist_similarity_quality_metrics(similarity_db_path=similarity_db_path, metrics=metrics)
            return {"saved": True, "attempts": attempt, "run_id": run_id, "error_class": None}
        except Exception as exc:
            last_error = exc
    return {
        "saved": False,
        "attempts": int(max_attempts),
        "run_id": None,
        "error_class": None if last_error is None else last_error.__class__.__name__,
    }


def _load_metrics_row(
    *,
    similarity_db_path: str | None,
    publish_id: str,
    engine_role: str,
    embedding_version: str,
) -> dict[str, Any] | None:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        row = conn.execute(
            """
            SELECT publish_id, as_of_date, engine_role, baseline_version, embedding_version, comparison_target_version,
                   top_k, case_count, query_count, returned_case_count, avg_similarity_score, overlap_at_k,
                   success_hit_rate_at_k, failure_hit_rate_at_k, big_drop_hit_rate_at_k, scope_type, scope_id, producer_work_id
            FROM similarity_quality_metrics
            WHERE publish_id = ? AND engine_role = ? AND embedding_version = ?
            """,
            [publish_id, engine_role, embedding_version],
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    columns = (
        "publish_id",
        "as_of_date",
        "engine_role",
        "baseline_version",
        "embedding_version",
        "comparison_target_version",
        "top_k",
        "case_count",
        "query_count",
        "returned_case_count",
        "avg_similarity_score",
        "overlap_at_k",
        "success_hit_rate_at_k",
        "failure_hit_rate_at_k",
        "big_drop_hit_rate_at_k",
        "scope_type",
        "scope_id",
        "producer_work_id",
    )
    return dict(zip(columns, row, strict=True))


def _load_recent_challenger_pass_streak(
    *,
    similarity_db_path: str | None,
    challenger_version: str,
    limit_runs: int = PROMOTION_REQUIRED_STREAK,
) -> int:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        rows = conn.execute(
            """
            SELECT pass_gate
            FROM similarity_promotion_reviews
            WHERE challenger_version = ?
            ORDER BY as_of_date DESC, created_at DESC
            LIMIT ?
            """,
            [challenger_version, int(limit_runs)],
        ).fetchall()
    finally:
        conn.close()
    streak = 0
    for row in rows:
        if bool(row[0]):
            streak += 1
        else:
            break
    return streak


def evaluate_similarity_promotion_gate(
    *,
    similarity_db_path: str | None,
    publish_id: str,
    as_of_date: str | int,
    champion_version: str = EMBEDDING_VERSION,
    challenger_version: str = CHALLENGER_EMBEDDING_VERSION,
    required_streak: int = PROMOTION_REQUIRED_STREAK,
) -> dict[str, Any]:
    champion_metrics = _load_metrics_row(
        similarity_db_path=similarity_db_path,
        publish_id=publish_id,
        engine_role="champion",
        embedding_version=champion_version,
    )
    challenger_metrics = _load_metrics_row(
        similarity_db_path=similarity_db_path,
        publish_id=publish_id,
        engine_role="challenger",
        embedding_version=challenger_version,
    )
    if not champion_metrics or not challenger_metrics:
        raise RuntimeError("promotion gate requires champion and challenger metrics")
    reasons: list[str] = []
    pass_gate = True
    overlap_at_k = float(challenger_metrics.get("overlap_at_k") or 0.0)
    challenger_success = float(challenger_metrics.get("success_hit_rate_at_k") or 0.0)
    champion_success = float(champion_metrics.get("success_hit_rate_at_k") or 0.0)
    challenger_big_drop = float(challenger_metrics.get("big_drop_hit_rate_at_k") or 0.0)
    champion_big_drop = float(champion_metrics.get("big_drop_hit_rate_at_k") or 0.0)
    if overlap_at_k < 0.40:
        pass_gate = False
        reasons.append("OVERLAP_LT_0_40")
    if challenger_success < champion_success:
        pass_gate = False
        reasons.append("SUCCESS_HIT_NOT_ABOVE_CHAMPION")
    if challenger_big_drop > (champion_big_drop + 0.05):
        pass_gate = False
        reasons.append("BIG_DROP_WORSE_THAN_THRESHOLD")
    prior_streak = _load_recent_challenger_pass_streak(
        similarity_db_path=similarity_db_path,
        challenger_version=challenger_version,
        limit_runs=required_streak - 1,
    )
    observed_streak = prior_streak + (1 if pass_gate else 0)
    if observed_streak < int(required_streak):
        pass_gate = False
        reasons.append("STREAK_NOT_REACHED")
    if not reasons and pass_gate:
        reasons.append("PROMOTION_GATE_PASS")
    return {
        "review_id": _run_id("promotion_review"),
        "publish_id": publish_id,
        "as_of_date": _as_of_date_text(_normalize_as_of_date(as_of_date)),
        "champion_version": champion_version,
        "challenger_version": challenger_version,
        "required_streak": int(required_streak),
        "observed_streak": int(observed_streak),
        "overlap_at_k": overlap_at_k,
        "success_hit_rate_at_k": challenger_success,
        "champion_success_hit_rate_at_k": champion_success,
        "big_drop_hit_rate_at_k": challenger_big_drop,
        "champion_big_drop_hit_rate_at_k": champion_big_drop,
        "pass_gate": bool(pass_gate),
        "reason_codes": json.dumps(reasons, ensure_ascii=False),
        "created_at": _utcnow(),
    }


def persist_similarity_promotion_review(
    *,
    similarity_db_path: str | None,
    review: dict[str, Any],
) -> str:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                DELETE FROM similarity_promotion_reviews
                WHERE publish_id = ? AND champion_version = ? AND challenger_version = ?
                """,
                [review["publish_id"], review["champion_version"], review["challenger_version"]],
            )
            columns = list(review.keys())
            conn.execute(
                f"INSERT INTO similarity_promotion_reviews ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [review[column] for column in columns],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return str(review["review_id"])


def build_similarity_nightly_summary(
    *,
    champion_metrics: dict[str, Any],
    challenger_metrics: dict[str, Any],
    promotion_review: dict[str, Any],
) -> dict[str, Any]:
    return {
        "summary_id": _run_id("similarity_summary"),
        "publish_id": challenger_metrics["publish_id"],
        "as_of_date": challenger_metrics["as_of_date"],
        "champion_version": champion_metrics["embedding_version"],
        "challenger_version": challenger_metrics["embedding_version"],
        "summary_json": json.dumps(
            {
                "champion": _json_ready_dict(champion_metrics),
                "challenger": _json_ready_dict(challenger_metrics),
                "promotion_review": {
                    key: value for key, value in _json_ready_dict(promotion_review).items() if key not in {"review_id", "created_at"}
                },
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "created_at": _utcnow(),
    }


def persist_similarity_nightly_summary(
    *,
    similarity_db_path: str | None,
    summary: dict[str, Any],
) -> str:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                DELETE FROM similarity_nightly_summaries
                WHERE publish_id = ? AND champion_version = ? AND challenger_version = ?
                """,
                [summary["publish_id"], summary["champion_version"], summary["challenger_version"]],
            )
            columns = list(summary.keys())
            conn.execute(
                f"INSERT INTO similarity_nightly_summaries ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [summary[column] for column in columns],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return str(summary["summary_id"])


def prepare_challenger_template(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    similarity_db_path: str | None = None,
    as_of_date: str | int | None = None,
    top_k: int = TOP_K,
    codes: list[str] | None = None,
    query_case_limit: int | None = None,
    candidate_pool_limit: int | None = None,
    cached_metrics_template: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if as_of_date is None:
        raise ValueError("as_of_date is required")
    as_of_date_int = _normalize_as_of_date(as_of_date)
    case_library_payload = build_case_library(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        similarity_db_path=similarity_db_path,
        as_of_date=as_of_date_int,
        codes=codes,
    )
    effective_top_k = _clamp_top_k(top_k)
    effective_query_case_limit = None if query_case_limit is None else max(1, int(query_case_limit))
    source_signature = str(case_library_payload.get("source_signature") or "")
    effective_candidate_pool_limit = None if candidate_pool_limit is None else max(1, int(candidate_pool_limit))
    focus_codes = None
    if case_library_payload.get("cache_state") == "partial_stale" and case_library_payload.get("dirty_ranges"):
        focus_codes = {str(item["code"]) for item in case_library_payload["dirty_ranges"] if item.get("code")}
    query_rows = _load_query_vectors(export_db_path, as_of_date_int, codes=codes)
    focus_setups = _derive_focus_setups(query_rows)
    query_scope_signature = _query_scope_signature(focus_codes=focus_codes, focus_setups=focus_setups)
    champion_vectors, library, case_paths = _load_case_vectors(
        similarity_db_path,
        embedding_version=EMBEDDING_VERSION,
    )
    working_set_plan = _plan_challenger_working_set(
        champion_vectors=champion_vectors,
        library=library,
        query_case_limit=effective_query_case_limit,
        candidate_pool_limit=effective_candidate_pool_limit,
        focus_codes=focus_codes,
        focus_setups=focus_setups,
    )
    working_set_case_ids = list(working_set_plan["working_set_ids"])
    use_partial_embedding_set = bool(working_set_case_ids) and len(working_set_case_ids) < len(champion_vectors)
    challenger_manifest = _load_generation_manifest_entry(
        similarity_db_path=similarity_db_path,
        generation_key=_challenger_embedding_generation_key(source_signature=source_signature),
    )
    if use_partial_embedding_set:
        challenger_vectors = _load_embedding_vectors(
            similarity_db_path,
            embedding_version=CHALLENGER_EMBEDDING_VERSION,
            case_ids=working_set_case_ids,
        )
        missing_case_ids = [case_id for case_id in working_set_case_ids if case_id not in challenger_vectors]
        if missing_case_ids:
            new_vectors = {
                case_id: _build_challenger_embedding(base_vector=champion_vectors[case_id], path_rows=case_paths.get(case_id, []))
                for case_id in missing_case_ids
            }
            _persist_challenger_embeddings(
                similarity_db_path=similarity_db_path,
                challenger_vectors=new_vectors,
            )
            challenger_vectors.update(new_vectors)
    elif (
        challenger_manifest
        and challenger_manifest.get("source_signature") == source_signature
        and challenger_manifest.get("row_count") == len(champion_vectors)
    ):
        challenger_vectors, _loaded_library, _loaded_case_paths = _load_case_vectors(
            similarity_db_path,
            embedding_version=CHALLENGER_EMBEDDING_VERSION,
        )
        if len(challenger_vectors) != len(champion_vectors):
            challenger_vectors = {
                case_id: _build_challenger_embedding(base_vector=vector, path_rows=case_paths.get(case_id, []))
                for case_id, vector in champion_vectors.items()
            }
            _persist_challenger_embeddings(
                similarity_db_path=similarity_db_path,
                challenger_vectors=challenger_vectors,
            )
        else:
            library = _loaded_library or library
    else:
        challenger_vectors = {
            case_id: _build_challenger_embedding(base_vector=vector, path_rows=case_paths.get(case_id, []))
            for case_id, vector in champion_vectors.items()
        }
        _persist_challenger_embeddings(
            similarity_db_path=similarity_db_path,
            challenger_vectors=challenger_vectors,
        )
        conn = connect_similarity_db(similarity_db_path)
        try:
            ensure_similarity_schema(conn)
            upsert_manifest(
                conn=conn,
                table_name="similarity_generation_manifest",
                generation_key=_challenger_embedding_generation_key(source_signature=source_signature),
                source_signature=source_signature,
                dependency_version=CHALLENGER_EMBEDDING_VERSION,
                cache_state="fresh",
                row_count=len(challenger_vectors),
                dirty_ranges=[],
                run_id=_run_id("challenger_embeddings"),
            )
            conn.execute("CHECKPOINT")
        finally:
            conn.close()
    template_key = _shadow_template_key(
        source_signature=source_signature,
        top_k=effective_top_k,
        query_case_limit=effective_query_case_limit,
        candidate_pool_limit=effective_candidate_pool_limit,
        query_scope_signature=query_scope_signature,
    )
    template_manifest = _load_generation_manifest_entry(
        similarity_db_path=similarity_db_path,
        generation_key=_shadow_template_generation_key(template_key=template_key),
    )
    template_rows: list[dict[str, Any]]
    metrics_template: dict[str, Any]
    reused_template = False
    if (
        template_manifest
        and template_manifest.get("source_signature") == source_signature
        and cached_metrics_template is not None
    ):
        template_rows = _load_shadow_template_rows(
            similarity_db_path=similarity_db_path,
            template_key=template_key,
        )
        if template_rows:
            metrics_template = dict(cached_metrics_template)
            reused_template = True
        else:
            template_rows, metrics_template = _build_shadow_template(
                template_key=template_key,
                challenger_vectors=challenger_vectors,
                champion_vectors=champion_vectors,
                library=library,
                top_k=effective_top_k,
                query_case_limit=effective_query_case_limit,
                candidate_pool_limit=effective_candidate_pool_limit,
                focus_codes=focus_codes,
                selected_query_ids=list(working_set_plan["selected_query_ids"]),
                ordered_case_ids=list(working_set_plan["ordered_case_ids"]),
                ordered_anchor_dates=list(working_set_plan["ordered_anchor_dates"]),
            )
    else:
        template_rows, metrics_template = _build_shadow_template(
            template_key=template_key,
            challenger_vectors=challenger_vectors,
            champion_vectors=champion_vectors,
            library=library,
            top_k=effective_top_k,
            query_case_limit=effective_query_case_limit,
            candidate_pool_limit=effective_candidate_pool_limit,
            focus_codes=focus_codes,
            selected_query_ids=list(working_set_plan["selected_query_ids"]),
            ordered_case_ids=list(working_set_plan["ordered_case_ids"]),
            ordered_anchor_dates=list(working_set_plan["ordered_anchor_dates"]),
        )
    if not reused_template:
        _persist_shadow_template_rows(
            similarity_db_path=similarity_db_path,
            template_key=template_key,
            template_rows=template_rows,
        )
        conn = connect_similarity_db(similarity_db_path)
        try:
            ensure_similarity_schema(conn)
            upsert_manifest(
                conn=conn,
                table_name="similarity_generation_manifest",
                generation_key=_shadow_template_generation_key(template_key=template_key),
                source_signature=source_signature,
                dependency_version=f"{CHALLENGER_EMBEDDING_VERSION}:k{effective_top_k}:q{effective_query_case_limit or 'all'}:p{effective_candidate_pool_limit or 'all'}",
                cache_state="fresh",
                row_count=len(template_rows),
                dirty_ranges=[],
                run_id=str(template_rows[0]["run_id"]) if template_rows else _run_id("challenger_template"),
            )
            if not use_partial_embedding_set:
                upsert_manifest(
                    conn=conn,
                    table_name="similarity_generation_manifest",
                    generation_key=_challenger_embedding_generation_key(source_signature=source_signature),
                    source_signature=source_signature,
                    dependency_version=CHALLENGER_EMBEDDING_VERSION,
                    cache_state="fresh",
                    row_count=len(challenger_vectors),
                    dirty_ranges=[],
                    run_id=_run_id("challenger_embeddings"),
                )
            conn.execute("CHECKPOINT")
        finally:
            conn.close()
    return {
        "ok": True,
        "as_of_date": _as_of_date_text(as_of_date_int),
        "top_k": effective_top_k,
        "template_key": template_key,
        "source_signature": source_signature,
        "case_library": case_library_payload,
        "template_rows": template_rows,
        "metrics_template": metrics_template,
        "shadow_case_count": len(template_rows),
        "query_case_limit": effective_query_case_limit,
        "query_case_count": int(metrics_template.get("query_count") or 0),
        "candidate_pool_limit": effective_candidate_pool_limit,
        "focus_code_count": 0 if not focus_codes else len(focus_codes),
        "focus_setup_count": len(focus_setups),
        "reused_template": reused_template,
    }


def materialize_challenger_metrics(
    *,
    similarity_db_path: str | None,
    publish_id: str,
    as_of_date: str | int,
    metrics_template: dict[str, Any],
    scope_type: str | None = None,
    scope_id: str | None = None,
    producer_work_id: str | None = None,
) -> dict[str, Any]:
    metrics_row = _materialize_similarity_quality_metrics(
        metrics_template=metrics_template,
        publish_id=publish_id,
        as_of_date=_normalize_as_of_date(as_of_date),
        scope_type=scope_type,
        scope_id=scope_id,
        producer_work_id=producer_work_id,
    )
    result = persist_similarity_quality_metrics_with_retry(
        similarity_db_path=similarity_db_path,
        metrics=metrics_row,
    )
    return {
        "metrics": metrics_row,
        "metrics_saved": bool(result["saved"]),
        "metrics_attempts": int(result["attempts"]),
        "metrics_error_class": result.get("error_class"),
        "similarity_metrics_run_id": result.get("run_id"),
    }


def run_similarity_baseline(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    as_of_date: str | int | None = None,
    publish_id: str | None = None,
    freshness_state: str = "fresh",
    top_k: int = TOP_K,
    publish_public: bool = True,
    codes: list[str] | None = None,
) -> dict[str, Any]:
    if as_of_date is None:
        raise ValueError("as_of_date is required")
    as_of_date_int = _normalize_as_of_date(as_of_date)
    actual_publish_id = str(publish_id) if publish_id else None
    if actual_publish_id is None:
        result_conn = connect_result_db(result_db_path, read_only=True)
        try:
            row = result_conn.execute(
                """
                SELECT publish_id
                FROM publish_pointer
                WHERE pointer_name = 'latest_successful'
                """
            ).fetchone()
        finally:
            result_conn.close()
        if not row:
            raise RuntimeError("latest successful publish is required for similarity publish")
        actual_publish_id = str(row[0])
    else:
        actual_publish_id = str(actual_publish_id)
    effective_top_k = _clamp_top_k(top_k)
    case_library_payload = build_case_library(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        similarity_db_path=similarity_db_path,
        as_of_date=as_of_date_int,
        codes=codes,
    )
    case_vectors, library, case_paths = _load_case_vectors(similarity_db_path)
    query_rows = _load_query_vectors(export_db_path, as_of_date_int, codes=codes)
    similar_rows: list[dict[str, Any]] = []
    path_rows: list[dict[str, Any]] = []
    for query in query_rows:
        scored = []
        for case_id, vector in case_vectors.items():
            case_meta = library.get(case_id)
            if not case_meta or case_meta["code"] == query["code"]:
                continue
            scored.append((case_id, _vector_distance(query["vector"], vector)))
        scored.sort(key=lambda item: (float(item[1]), str(item[0])))
        for neighbor_rank, (case_id, distance) in enumerate(scored[:effective_top_k], start=1):
            case_meta = library[case_id]
            similar_rows.append(
                {
                    "publish_id": actual_publish_id,
                    "as_of_date": _as_of_date_text(as_of_date_int),
                    "code": query["code"],
                    "query_type": case_meta["query_source"],
                    "query_anchor_type": case_meta["anchor_type"],
                    "neighbor_rank": neighbor_rank,
                    "case_id": case_id,
                    "neighbor_code": case_meta["code"],
                    "neighbor_anchor_date": _as_of_date_text(int(case_meta["anchor_date"])),
                    "case_type": case_meta["case_type"],
                    "outcome_class": case_meta["outcome_class"],
                    "success_flag": bool(case_meta["success_flag"]),
                    "similarity_score": round(1.0 / (1.0 + float(distance)), 6),
                    "reason_codes": json.dumps(
                        ["SIMILARITY_BASELINE", str(case_meta["case_type"]).upper(), str(case_meta["outcome_class"]).upper()],
                        ensure_ascii=False,
                    ),
                }
            )
            for path_row in case_paths.get(case_id, [])[:MAX_PUBLIC_PATH_ROWS]:
                path_rows.append(
                    {
                        "publish_id": actual_publish_id,
                        "as_of_date": _as_of_date_text(as_of_date_int),
                        "code": query["code"],
                        "case_id": case_id,
                        "rel_day": int(path_row["rel_day"]),
                        "path_return_norm": float(path_row["path_return_norm"]),
                        "path_volume_norm": float(path_row["path_volume_norm"]),
                    }
                )
    conn = connect_result_db(result_db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DELETE FROM similar_cases_daily WHERE publish_id = ?", [actual_publish_id])
            conn.execute("DELETE FROM similar_case_paths WHERE publish_id = ?", [actual_publish_id])
            if similar_rows:
                similar_columns = list(similar_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO similar_cases_daily ({', '.join(similar_columns)}) VALUES ({', '.join(['?'] * len(similar_columns))})",
                    [[row[column] for column in similar_columns] for row in similar_rows],
                )
            if path_rows:
                path_columns = list(path_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO similar_case_paths ({', '.join(path_columns)}) VALUES ({', '.join(['?'] * len(path_columns))})",
                    [[row[column] for column in path_columns] for row in path_rows],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        candidate_count_row = conn.execute("SELECT COUNT(*) FROM candidate_daily WHERE publish_id = ?", [actual_publish_id]).fetchone()
        regime_count_row = conn.execute("SELECT COUNT(*) FROM regime_daily WHERE publish_id = ?", [actual_publish_id]).fetchone()
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    publish_payload = None
    if publish_public:
        publish_payload = publish_result(
            db_path=result_db_path,
            publish_id=actual_publish_id,
            as_of_date=_as_of_date_text(as_of_date_int),
            freshness_state=freshness_state,
            table_row_counts={
                "candidate_daily": int(candidate_count_row[0]) if candidate_count_row else 0,
                "regime_daily": int(regime_count_row[0]) if regime_count_row else 0,
                "state_eval_daily": 0,
                "similar_cases_daily": len(similar_rows),
                "similar_case_paths": len(path_rows),
            },
            degrade_ready=True,
        )
    metrics_payload = _materialize_similarity_quality_metrics(
        metrics_template=_build_similarity_metrics(
            publish_id=actual_publish_id,
            as_of_date=as_of_date_int,
            case_vectors=case_vectors,
            library=library,
            query_rows=query_rows,
            similar_rows=similar_rows,
            path_rows=path_rows,
            top_k=effective_top_k,
        ),
        publish_id=actual_publish_id,
        as_of_date=as_of_date_int,
    )
    metrics_result = persist_similarity_quality_metrics_with_retry(
        similarity_db_path=similarity_db_path,
        metrics=metrics_payload,
    )
    return {
        "ok": True,
        "publish": publish_payload,
        "publish_id": actual_publish_id,
        "as_of_date": _as_of_date_text(as_of_date_int),
        "embedding_version": EMBEDDING_VERSION,
        "top_k": effective_top_k,
        "similar_case_count": len(similar_rows),
        "similar_path_count": len(path_rows),
        "case_library": case_library_payload,
        "metrics_saved": bool(metrics_result["saved"]),
        "metrics_attempts": int(metrics_result["attempts"]),
        "metrics_error_class": metrics_result.get("error_class"),
        "similarity_metrics_run_id": metrics_result.get("run_id"),
    }


def run_similarity_challenger_shadow(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    as_of_date: str | int | None = None,
    publish_id: str | None = None,
    top_k: int = TOP_K,
    codes: list[str] | None = None,
    query_case_limit: int | None = None,
    candidate_pool_limit: int | None = None,
    scope_type: str | None = None,
    scope_id: str | None = None,
    producer_work_id: str | None = None,
    persist_shadow_rows: bool = True,
    cached_metrics_template: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if as_of_date is None:
        raise ValueError("as_of_date is required")
    as_of_date_int = _normalize_as_of_date(as_of_date)
    actual_publish_id = str(publish_id) if publish_id else None
    if actual_publish_id is None:
        result_conn = connect_result_db(result_db_path, read_only=True)
        try:
            row = result_conn.execute(
                """
                SELECT publish_id
                FROM publish_pointer
                WHERE pointer_name = 'latest_successful'
                """
            ).fetchone()
        finally:
            result_conn.close()
        if not row:
            actual_publish_id = _default_publish_id(as_of_date_int)
        else:
            actual_publish_id = str(row[0])
    template_payload = prepare_challenger_template(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        similarity_db_path=similarity_db_path,
        as_of_date=as_of_date_int,
        top_k=top_k,
        codes=codes,
        query_case_limit=query_case_limit,
        candidate_pool_limit=candidate_pool_limit,
        cached_metrics_template=cached_metrics_template,
    )
    shadow_rows = _materialize_shadow_rows(
        publish_id=actual_publish_id,
        as_of_date=as_of_date_int,
        template_rows=template_payload["template_rows"],
    )
    shadow_run_id = None
    if persist_shadow_rows:
        shadow_run_id = _persist_shadow_rows(
            similarity_db_path=similarity_db_path,
            publish_id=actual_publish_id,
            shadow_rows=shadow_rows,
        )
    metrics_payload = _materialize_similarity_quality_metrics(
        metrics_template=template_payload["metrics_template"],
        publish_id=actual_publish_id,
        as_of_date=as_of_date_int,
        scope_type=scope_type,
        scope_id=scope_id,
        producer_work_id=producer_work_id,
    )
    metrics_result = persist_similarity_quality_metrics_with_retry(
        similarity_db_path=similarity_db_path,
        metrics=metrics_payload,
    )
    champion_metrics = _load_metrics_row(
        similarity_db_path=similarity_db_path,
        publish_id=actual_publish_id,
        engine_role="champion",
        embedding_version=EMBEDDING_VERSION,
    )
    promotion_review = None
    summary_id = None
    if champion_metrics and metrics_result["saved"]:
        promotion_review = evaluate_similarity_promotion_gate(
            similarity_db_path=similarity_db_path,
            publish_id=actual_publish_id,
            as_of_date=as_of_date_int,
        )
        persist_similarity_promotion_review(
            similarity_db_path=similarity_db_path,
            review=promotion_review,
        )
        summary_id = persist_similarity_nightly_summary(
            similarity_db_path=similarity_db_path,
            summary=build_similarity_nightly_summary(
                champion_metrics=champion_metrics,
                challenger_metrics=_load_metrics_row(
                    similarity_db_path=similarity_db_path,
                    publish_id=actual_publish_id,
                    engine_role="challenger",
                    embedding_version=CHALLENGER_EMBEDDING_VERSION,
                )
                or metrics_payload,
                promotion_review=promotion_review,
            ),
        )
    return {
        "ok": True,
        "publish_id": actual_publish_id,
        "as_of_date": _as_of_date_text(as_of_date_int),
        "embedding_version": CHALLENGER_EMBEDDING_VERSION,
        "engine_role": "challenger",
        "comparison_target_version": EMBEDDING_VERSION,
        "case_library": template_payload["case_library"],
        "template_key": template_payload["template_key"],
        "source_signature": template_payload["source_signature"],
        "top_k": int(top_k),
        "query_case_limit": template_payload.get("query_case_limit"),
        "query_case_count": template_payload.get("query_case_count"),
        "candidate_pool_limit": template_payload.get("candidate_pool_limit"),
        "focus_setup_count": template_payload.get("focus_setup_count"),
        "shadow_run_id": shadow_run_id,
        "shadow_case_count": len(shadow_rows),
        "reused_template": bool(template_payload["reused_template"]),
        "metrics_saved": bool(metrics_result["saved"]),
        "metrics_attempts": int(metrics_result["attempts"]),
        "metrics_error_class": metrics_result.get("error_class"),
        "similarity_metrics_run_id": metrics_result.get("run_id"),
        "metrics_template": template_payload["metrics_template"],
        "promotion_gate_passed": None if promotion_review is None else bool(promotion_review["pass_gate"]),
        "promotion_review_id": None if promotion_review is None else promotion_review["review_id"],
        "nightly_summary_id": summary_id,
    }
