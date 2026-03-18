from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import duckdb

from external_analysis.ops.store import (
    persist_comparison_rollups,
    persist_metric_daily_summaries,
    persist_promotion_readiness_rows,
)
from external_analysis.results.result_schema import connect_result_db
from external_analysis.similarity.store import connect_similarity_db, ensure_similarity_schema

ROLLING_WINDOWS = (20, 40, 60)
CHAMPION_VERSION = "deterministic_similarity_v1"
CHALLENGER_VERSION = "future_path_challenger_v1"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _mean(values: list[float | None]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    return None if not filtered else sum(filtered) / len(filtered)


def _scope_where(scope_type: str, scope_id: str) -> tuple[str, list[str]]:
    if scope_type == "replay":
        return "publish_id LIKE ?", [f"replay_{scope_id}_%"]
    if scope_type == "nightly":
        return "publish_id NOT LIKE 'replay_%'", []
    return "1 = 1", []


def _similarity_scope_where(scope_type: str, scope_id: str) -> tuple[str, list[str]]:
    if scope_type == "replay":
        return (
            "((scope_type = 'replay' AND scope_id = ?) OR (scope_type IS NULL AND publish_id LIKE ?))",
            [scope_id, f"replay_{scope_id}_%"],
        )
    if scope_type == "nightly":
        return (
            "((scope_type = 'nightly' AND scope_id = 'nightly') OR (scope_type IS NULL AND publish_id NOT LIKE 'replay_%'))",
            [],
        )
    return "1 = 1", []


def _load_candidate_rows(*, result_db_path: str, scope_type: str, scope_id: str) -> dict[str, dict[str, Any]]:
    where_clause, params = _scope_where(scope_type, scope_id)
    conn = connect_result_db(result_db_path, read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT publish_id, CAST(as_of_date AS VARCHAR), baseline_version, recall_at_20, recall_at_10,
                   monthly_top5_capture, avg_ret_20_top20
            FROM nightly_candidate_metrics
            WHERE {where_clause}
            ORDER BY as_of_date, publish_id
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return {
        str(row[0]): {
            "publish_id": str(row[0]),
            "as_of_date": str(row[1]),
            "baseline_version": str(row[2]),
            "recall_at_20": None if row[3] is None else float(row[3]),
            "recall_at_10": None if row[4] is None else float(row[4]),
            "monthly_top5_capture": None if row[5] is None else float(row[5]),
            "avg_ret_20_top20": None if row[6] is None else float(row[6]),
        }
        for row in rows
    }


def _load_similarity_rows(*, similarity_db_path: str, scope_type: str, scope_id: str) -> dict[tuple[str, str], dict[str, Any]]:
    where_clause, params = _similarity_scope_where(scope_type, scope_id)
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        rows = conn.execute(
            f"""
            SELECT publish_id, CAST(as_of_date AS VARCHAR), engine_role, baseline_version, embedding_version,
                   comparison_target_version, overlap_at_k, success_hit_rate_at_k, failure_hit_rate_at_k,
                   big_drop_hit_rate_at_k, avg_similarity_score, case_count, query_count, returned_case_count,
                   scope_type, scope_id, producer_work_id
            FROM similarity_quality_metrics
            WHERE {where_clause}
            ORDER BY as_of_date, publish_id, engine_role
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return {
        (str(row[0]), str(row[2])): {
            "publish_id": str(row[0]),
            "as_of_date": str(row[1]),
            "engine_role": str(row[2]),
            "baseline_version": str(row[3]),
            "embedding_version": str(row[4]),
            "comparison_target_version": str(row[5]),
            "overlap_at_k": None if row[6] is None else float(row[6]),
            "success_hit_rate_at_k": None if row[7] is None else float(row[7]),
            "failure_hit_rate_at_k": None if row[8] is None else float(row[8]),
            "big_drop_hit_rate_at_k": None if row[9] is None else float(row[9]),
            "avg_similarity_score": None if row[10] is None else float(row[10]),
            "case_count": int(row[11]),
            "query_count": int(row[12]),
            "returned_case_count": int(row[13]),
            "scope_type": None if row[14] is None else str(row[14]),
            "scope_id": None if row[15] is None else str(row[15]),
            "producer_work_id": None if row[16] is None else str(row[16]),
        }
        for row in rows
    }


def _merge_daily_rows(
    *,
    candidate_by_publish: dict[str, dict[str, Any]],
    similarity_by_publish: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    publish_ids = sorted(
        {
            *candidate_by_publish.keys(),
            *[publish_id for publish_id, _engine_role in similarity_by_publish.keys()],
        },
        key=lambda publish_id: (
            candidate_by_publish.get(publish_id, {}).get("as_of_date")
            or similarity_by_publish.get((publish_id, "champion"), {}).get("as_of_date")
            or similarity_by_publish.get((publish_id, "challenger"), {}).get("as_of_date")
            or "",
            publish_id,
        ),
    )
    rows: list[dict[str, Any]] = []
    for publish_id in publish_ids:
        candidate = candidate_by_publish.get(publish_id, {})
        champion = similarity_by_publish.get((publish_id, "champion"), {})
        challenger = similarity_by_publish.get((publish_id, "challenger"), {})
        as_of_date = candidate.get("as_of_date") or champion.get("as_of_date") or challenger.get("as_of_date")
        if not as_of_date:
            continue
        rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": as_of_date,
                "candidate": candidate,
                "champion": champion,
                "challenger": challenger,
            }
        )
    return rows


def _build_daily_summary_rows(*, scope_type: str, scope_id: str, merged_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in merged_rows:
        rows.append(
            {
                "summary_id": f"{scope_type}_{scope_id}_{item['publish_id']}",
                "scope_type": scope_type,
                "scope_id": scope_id,
                "as_of_date": item["as_of_date"],
                "publish_id": item["publish_id"],
                "source_kind": "replay" if str(item["publish_id"]).startswith("replay_") else "nightly",
                "summary_json": _json(
                    {
                        "candidate": item["candidate"],
                        "champion": item["champion"],
                        "challenger": item["challenger"],
                    }
                ),
                "created_at": _utcnow(),
            }
        )
    return rows


def _build_rollup_rows(*, scope_type: str, scope_id: str, merged_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rollups: list[dict[str, Any]] = []
    readiness: list[dict[str, Any]] = []
    for window_size in ROLLING_WINDOWS:
        window = merged_rows[-window_size:]
        if not window:
            continue
        challenger_rows = [row["challenger"] for row in window if row["challenger"]]
        champion_rows = [row["champion"] for row in window if row["champion"]]
        candidate_rows = [row["candidate"] for row in window if row["candidate"]]
        overlap_mean = _mean([row.get("overlap_at_k") for row in challenger_rows])
        success_mean = _mean([row.get("success_hit_rate_at_k") for row in challenger_rows])
        failure_mean = _mean([row.get("failure_hit_rate_at_k") for row in challenger_rows])
        big_drop_mean = _mean([row.get("big_drop_hit_rate_at_k") for row in challenger_rows])
        avg_similarity_mean = _mean([row.get("avg_similarity_score") for row in challenger_rows])
        champion_success_mean = _mean([row.get("success_hit_rate_at_k") for row in champion_rows])
        champion_failure_mean = _mean([row.get("failure_hit_rate_at_k") for row in champion_rows])
        champion_big_drop_mean = _mean([row.get("big_drop_hit_rate_at_k") for row in champion_rows])
        success_delta = None if success_mean is None or champion_success_mean is None else success_mean - champion_success_mean
        failure_delta = None if failure_mean is None or champion_failure_mean is None else failure_mean - champion_failure_mean
        big_drop_delta = None if big_drop_mean is None or champion_big_drop_mean is None else big_drop_mean - champion_big_drop_mean
        reason_codes: list[str] = []
        if len(window) < window_size:
            reason_codes.append("insufficient_runs")
        if overlap_mean is None or overlap_mean < 0.40:
            reason_codes.append("overlap_below_threshold")
        if success_delta is None or success_delta < 0.0:
            reason_codes.append("success_not_beating_champion")
        if big_drop_delta is None or big_drop_delta > 0.05:
            reason_codes.append("big_drop_regression")
        readiness_pass = not reason_codes
        summary_payload = {
            "window_size": window_size,
            "run_count": len(window),
            "success_delta_vs_champion": success_delta,
            "failure_delta_vs_champion": failure_delta,
            "big_drop_delta_vs_champion": big_drop_delta,
            "candidate_metric_coverage": len(candidate_rows),
            "champion_metric_coverage": len(champion_rows),
            "challenger_metric_coverage": len(challenger_rows),
        }
        rollups.append(
            {
                "rollup_id": f"{scope_type}_{scope_id}_w{window_size}",
                "scope_type": scope_type,
                "scope_id": scope_id,
                "window_size": int(window_size),
                "start_as_of_date": window[0]["as_of_date"],
                "end_as_of_date": window[-1]["as_of_date"],
                "run_count": len(window),
                "champion_version": CHAMPION_VERSION,
                "challenger_version": CHALLENGER_VERSION,
                "overlap_at_k_mean": overlap_mean,
                "success_hit_rate_at_k_mean": success_mean,
                "failure_hit_rate_at_k_mean": failure_mean,
                "big_drop_hit_rate_at_k_mean": big_drop_mean,
                "avg_similarity_score_mean": avg_similarity_mean,
                "recall_at_20_mean": _mean([row.get("recall_at_20") for row in candidate_rows]),
                "recall_at_10_mean": _mean([row.get("recall_at_10") for row in candidate_rows]),
                "monthly_top5_capture_mean": _mean([row.get("monthly_top5_capture") for row in candidate_rows]),
                "avg_ret_20_top20_mean": _mean([row.get("avg_ret_20_top20") for row in candidate_rows]),
                "success_delta_vs_champion": success_delta,
                "failure_delta_vs_champion": failure_delta,
                "big_drop_delta_vs_champion": big_drop_delta,
                "summary_json": _json(summary_payload),
                "created_at": _utcnow(),
            }
        )
        readiness.append(
            {
                "readiness_id": f"{scope_type}_{scope_id}_w{window_size}",
                "scope_type": scope_type,
                "scope_id": scope_id,
                "window_size": int(window_size),
                "end_as_of_date": window[-1]["as_of_date"],
                "run_count": len(window),
                "champion_version": CHAMPION_VERSION,
                "challenger_version": CHALLENGER_VERSION,
                "readiness_pass": bool(readiness_pass),
                "reason_codes": _json(reason_codes),
                "summary_json": _json(summary_payload),
                "created_at": _utcnow(),
            }
        )
    return rollups, readiness


def aggregate_comparison_windows(
    *,
    result_db_path: str,
    similarity_db_path: str,
    ops_db_path: str,
    scope_type: str,
    scope_id: str,
) -> dict[str, Any]:
    candidate_by_publish = _load_candidate_rows(result_db_path=result_db_path, scope_type=scope_type, scope_id=scope_id)
    similarity_by_publish = _load_similarity_rows(similarity_db_path=similarity_db_path, scope_type=scope_type, scope_id=scope_id)
    merged_rows = _merge_daily_rows(candidate_by_publish=candidate_by_publish, similarity_by_publish=similarity_by_publish)
    daily_rows = _build_daily_summary_rows(scope_type=scope_type, scope_id=scope_id, merged_rows=merged_rows)
    rollup_rows, readiness_rows = _build_rollup_rows(scope_type=scope_type, scope_id=scope_id, merged_rows=merged_rows)
    persist_metric_daily_summaries(scope_type=scope_type, scope_id=scope_id, daily_rows=daily_rows, ops_db_path=ops_db_path)
    persist_comparison_rollups(scope_type=scope_type, scope_id=scope_id, rollup_rows=rollup_rows, ops_db_path=ops_db_path)
    persist_promotion_readiness_rows(scope_type=scope_type, scope_id=scope_id, readiness_rows=readiness_rows, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "scope_type": scope_type,
        "scope_id": scope_id,
        "daily_summary_count": len(daily_rows),
        "rollup_count": len(rollup_rows),
        "readiness_count": len(readiness_rows),
    }
