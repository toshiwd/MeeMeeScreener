from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import duckdb

from external_analysis.ops.store import persist_review_artifact


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _parse_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _latest_scope_payload(conn: duckdb.DuckDBPyConnection, scope_type: str, scope_id: str) -> dict[str, Any]:
    rollup_rows = conn.execute(
        """
        SELECT window_size, CAST(end_as_of_date AS VARCHAR), run_count,
               overlap_at_k_mean, success_hit_rate_at_k_mean, failure_hit_rate_at_k_mean,
               big_drop_hit_rate_at_k_mean, avg_similarity_score_mean,
               recall_at_20_mean, recall_at_10_mean, monthly_top5_capture_mean, avg_ret_20_top20_mean,
               success_delta_vs_champion, failure_delta_vs_champion, big_drop_delta_vs_champion
        FROM external_comparison_rollups
        WHERE scope_type = ? AND scope_id = ?
        ORDER BY window_size
        """,
        [scope_type, scope_id],
    ).fetchall()
    readiness_rows = conn.execute(
        """
        SELECT window_size, readiness_pass, reason_codes, summary_json
        FROM external_promotion_readiness
        WHERE scope_type = ? AND scope_id = ?
        ORDER BY window_size
        """,
        [scope_type, scope_id],
    ).fetchall()
    readiness_by_window = {
        int(row[0]): {
            "readiness_pass": bool(row[1]),
            "reason_codes": _parse_json(row[2]) or [],
            "summary": _parse_json(row[3]) or {},
        }
        for row in readiness_rows
    }
    windows: dict[str, Any] = {}
    latest_end = None
    for row in rollup_rows:
        window_size = int(row[0])
        latest_end = str(row[1])
        windows[str(window_size)] = {
            "end_as_of_date": str(row[1]),
            "run_count": int(row[2]),
            "overlap_at_k_mean": None if row[3] is None else float(row[3]),
            "success_hit_rate_at_k_mean": None if row[4] is None else float(row[4]),
            "failure_hit_rate_at_k_mean": None if row[5] is None else float(row[5]),
            "big_drop_hit_rate_at_k_mean": None if row[6] is None else float(row[6]),
            "avg_similarity_score_mean": None if row[7] is None else float(row[7]),
            "recall_at_20_mean": None if row[8] is None else float(row[8]),
            "recall_at_10_mean": None if row[9] is None else float(row[9]),
            "monthly_top5_capture_mean": None if row[10] is None else float(row[10]),
            "avg_ret_20_top20_mean": None if row[11] is None else float(row[11]),
            "success_delta_vs_champion": None if row[12] is None else float(row[12]),
            "failure_delta_vs_champion": None if row[13] is None else float(row[13]),
            "big_drop_delta_vs_champion": None if row[14] is None else float(row[14]),
            "readiness": readiness_by_window.get(window_size, {}),
        }
    return {
        "scope_type": scope_type,
        "scope_id": scope_id,
        "latest_end_as_of_date": latest_end,
        "windows": windows,
    }


def _recent_run_stats(conn: duckdb.DuckDBPyConnection, recent_run_limit: int) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT status
        FROM external_job_runs
        WHERE job_type = 'nightly_similarity_challenger_pipeline'
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [int(recent_run_limit)],
    ).fetchall()
    statuses = [str(row[0]) for row in rows]
    failed = [status for status in statuses if status not in {"success", "shadow_with_metrics_failure"}]
    failure_rate = 0.0 if not statuses else len(failed) / len(statuses)
    quarantine_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM external_job_quarantine
        WHERE job_type = 'nightly_similarity_challenger_pipeline'
        """
    ).fetchone()
    return {
        "recent_run_limit": int(recent_run_limit),
        "recent_failure_rate": float(failure_rate),
        "recent_quarantine_count": int(quarantine_count[0]) if quarantine_count else 0,
    }


def _top_reason_codes(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT reason_codes FROM external_promotion_readiness").fetchall()
    counter: Counter[str] = Counter()
    for row in rows:
        for code in _parse_json(row[0]) or []:
            counter[str(code)] += 1
    return [{"reason_code": reason_code, "count": count} for reason_code, count in counter.most_common(5)]


def build_review_summary(*, ops_db_path: str, review_id: str = "weekly_review_latest", recent_run_limit: int = 20) -> dict[str, Any]:
    conn = duckdb.connect(ops_db_path, read_only=True)
    try:
        replay_scope_row = conn.execute(
            """
            SELECT scope_id
            FROM external_comparison_rollups
            WHERE scope_type = 'replay'
            ORDER BY end_as_of_date DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        nightly_scope_row = conn.execute(
            """
            SELECT scope_id
            FROM external_comparison_rollups
            WHERE scope_type = 'nightly'
            ORDER BY end_as_of_date DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        combined_scope_row = conn.execute(
            """
            SELECT scope_id
            FROM external_comparison_rollups
            WHERE scope_type = 'combined'
            ORDER BY end_as_of_date DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        replay_scope_id = None if replay_scope_row is None else str(replay_scope_row[0])
        nightly_scope_id = None if nightly_scope_row is None else str(nightly_scope_row[0])
        combined_scope_id = None if combined_scope_row is None else str(combined_scope_row[0])
        replay_summary = {} if replay_scope_id is None else _latest_scope_payload(conn, "replay", replay_scope_id)
        nightly_summary = {} if nightly_scope_id is None else _latest_scope_payload(conn, "nightly", nightly_scope_id)
        combined_summary = {} if combined_scope_id is None else _latest_scope_payload(conn, "combined", combined_scope_id)
        run_stats = _recent_run_stats(conn, recent_run_limit)
        top_reason_codes = _top_reason_codes(conn)
    finally:
        conn.close()
    combined_windows = combined_summary.get("windows", {})
    latest_end_as_of_date = (
        combined_summary.get("latest_end_as_of_date")
        or nightly_summary.get("latest_end_as_of_date")
        or replay_summary.get("latest_end_as_of_date")
    )
    summary_payload = {
        "review_id": review_id,
        "latest_20_40_60_readiness": {
            "20": combined_windows.get("20", {}).get("readiness", {}),
            "40": combined_windows.get("40", {}).get("readiness", {}),
            "60": combined_windows.get("60", {}).get("readiness", {}),
        },
        "scope_comparison": {
            "replay": replay_summary,
            "nightly": nightly_summary,
            "combined": combined_summary,
        },
        "recent_run_stats": run_stats,
        "top_reason_codes": top_reason_codes,
    }
    review_row = {
        "review_id": review_id,
        "review_kind": "weekly_similarity_candidate_review",
        "latest_end_as_of_date": latest_end_as_of_date,
        "replay_scope_id": replay_scope_id,
        "nightly_scope_id": nightly_scope_id,
        "combined_scope_id": combined_scope_id,
        "combined_readiness_20": combined_windows.get("20", {}).get("readiness", {}).get("readiness_pass"),
        "combined_readiness_40": combined_windows.get("40", {}).get("readiness", {}).get("readiness_pass"),
        "combined_readiness_60": combined_windows.get("60", {}).get("readiness", {}).get("readiness_pass"),
        "recent_run_limit": int(run_stats["recent_run_limit"]),
        "recent_failure_rate": float(run_stats["recent_failure_rate"]),
        "recent_quarantine_count": int(run_stats["recent_quarantine_count"]),
        "top_reason_codes_json": _json(top_reason_codes),
        "replay_summary_json": _json(replay_summary),
        "nightly_summary_json": _json(nightly_summary),
        "combined_summary_json": _json(combined_summary),
        "summary_json": _json(summary_payload),
        "created_at": _utcnow(),
    }
    persist_review_artifact(review_row=review_row, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "review_id": review_id,
        "latest_end_as_of_date": latest_end_as_of_date,
        "recent_failure_rate": run_stats["recent_failure_rate"],
        "recent_quarantine_count": run_stats["recent_quarantine_count"],
    }
