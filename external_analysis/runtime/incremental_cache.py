from __future__ import annotations

import json
from typing import Any

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db, ensure_label_schema
from external_analysis.similarity.store import connect_similarity_db, ensure_similarity_schema

LABEL_RELEVANT_EXPORT_TABLES = frozenset({"bars_daily_export", "indicator_daily_export"})
ANCHOR_RELEVANT_EXPORT_TABLES = frozenset({"bars_daily_export", "indicator_daily_export"})
SIMILARITY_RELEVANT_EXPORT_TABLES = frozenset({"bars_daily_export", "indicator_daily_export"})


def load_latest_export_run(export_db_path: str | None) -> dict[str, Any] | None:
    conn = connect_export_db(export_db_path)
    try:
        row = conn.execute(
            """
            SELECT run_id, source_signature, changed_table_names, diff_reason
            FROM meta_export_runs
            ORDER BY started_at DESC, run_id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    changed = json.loads(row[2]) if row[2] else []
    diff_reason = json.loads(row[3]) if row[3] else {}
    return {
        "run_id": str(row[0]),
        "source_signature": str(row[1]),
        "changed_table_names": [str(value) for value in changed],
        "diff_reason": diff_reason,
    }


def _build_dirty_ranges_from_export_run(
    *,
    export_db_path: str | None,
    export_run_id: str,
    relevant_tables: frozenset[str],
) -> list[dict[str, Any]]:
    conn = connect_export_db(export_db_path)
    try:
        ranges: dict[str, dict[str, Any]] = {}
        if "bars_daily_export" in relevant_tables:
            for code, min_trade_date, max_trade_date in conn.execute(
                """
                SELECT code, MIN(trade_date), MAX(trade_date)
                FROM bars_daily_export
                WHERE export_run_id = ?
                GROUP BY code
                """,
                [export_run_id],
            ).fetchall():
                ranges[str(code)] = {
                    "code": str(code),
                    "date_from": int(min_trade_date),
                    "date_to": int(max_trade_date),
                    "reason": "bars_daily_export_changed",
                }
        if "indicator_daily_export" in relevant_tables:
            for code, min_trade_date, max_trade_date in conn.execute(
                """
                SELECT code, MIN(trade_date), MAX(trade_date)
                FROM indicator_daily_export
                WHERE export_run_id = ?
                GROUP BY code
                """,
                [export_run_id],
            ).fetchall():
                item = ranges.get(str(code))
                if item is None:
                    ranges[str(code)] = {
                        "code": str(code),
                        "date_from": int(min_trade_date),
                        "date_to": int(max_trade_date),
                        "reason": "indicator_daily_export_changed",
                    }
                    continue
                item["date_from"] = min(int(item["date_from"]), int(min_trade_date))
                item["date_to"] = max(int(item["date_to"]), int(max_trade_date))
                item["reason"] = "bars_indicator_changed"
    finally:
        conn.close()
    return sorted(ranges.values(), key=lambda item: (str(item["code"]), int(item["date_from"])))


def _load_manifest(conn, table_name: str, generation_key: str) -> dict[str, Any] | None:
    row = conn.execute(
        f"""
        SELECT source_signature, dependency_version, cache_state, dirty_ranges_json, row_count
        FROM {table_name}
        WHERE generation_key = ?
        """,
        [generation_key],
    ).fetchone()
    if not row:
        return None
    return {
        "source_signature": str(row[0]),
        "dependency_version": str(row[1]),
        "cache_state": str(row[2]),
        "dirty_ranges": json.loads(row[3]) if row[3] else [],
        "row_count": int(row[4] or 0),
    }


def upsert_manifest(
    *,
    conn,
    table_name: str,
    generation_key: str,
    source_signature: str,
    dependency_version: str,
    cache_state: str,
    row_count: int,
    dirty_ranges: list[dict[str, Any]],
    run_id: str,
) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {table_name} (
            generation_key, source_signature, dependency_version, cache_state,
            dirty_ranges_json, row_count, generation_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
        """,
        [
            generation_key,
            source_signature,
            dependency_version,
            cache_state,
            json.dumps(dirty_ranges, ensure_ascii=False, sort_keys=True),
            int(row_count),
            run_id,
        ],
    )


def probe_label_cache(
    *,
    export_db_path: str | None,
    label_db_path: str | None,
    generation_key: str,
    dependency_version: str,
    relevant_tables: frozenset[str],
) -> dict[str, Any]:
    export_run = load_latest_export_run(export_db_path)
    if export_run is None:
        return {"action": "full", "cache_state": "rebuild_required", "reason": "missing_export_run", "dirty_ranges": []}
    conn = connect_label_db(label_db_path)
    try:
        ensure_label_schema(conn)
        manifest = _load_manifest(conn, "label_generation_manifest", generation_key)
    finally:
        conn.close()
    changed_tables = set(export_run["changed_table_names"])
    relevant_changed = changed_tables.intersection(relevant_tables)
    if manifest and manifest["dependency_version"] == dependency_version and manifest["source_signature"] == export_run["source_signature"]:
        return {
            "action": "skip",
            "cache_state": "fresh",
            "reason": "source_signature_unchanged",
            "dirty_ranges": [],
            "source_signature": export_run["source_signature"],
        }
    if not relevant_changed:
        if not manifest:
            return {
                "action": "full",
                "cache_state": "rebuild_required",
                "reason": "manifest_missing",
                "dirty_ranges": [],
                "source_signature": export_run["source_signature"],
            }
        return {
            "action": "skip",
            "cache_state": "fresh" if manifest else "rebuild_required",
            "reason": "irrelevant_export_change",
            "dirty_ranges": [],
            "source_signature": export_run["source_signature"],
        }
    if any(int((export_run["diff_reason"].get(name) or {}).get("deleted", 0)) > 0 for name in relevant_changed):
        return {
            "action": "full",
            "cache_state": "rebuild_required",
            "reason": "relevant_delete_detected",
            "dirty_ranges": [],
            "source_signature": export_run["source_signature"],
        }
    dirty_ranges = _build_dirty_ranges_from_export_run(
        export_db_path=export_db_path,
        export_run_id=str(export_run["run_id"]),
        relevant_tables=relevant_tables,
    )
    return {
        "action": "partial" if dirty_ranges and manifest else "full",
        "cache_state": "partial_stale" if dirty_ranges else "rebuild_required",
        "reason": "relevant_export_change",
        "dirty_ranges": dirty_ranges,
        "source_signature": export_run["source_signature"],
    }


def probe_similarity_cache(
    *,
    export_db_path: str | None,
    similarity_db_path: str | None,
    generation_key: str,
    dependency_version: str,
) -> dict[str, Any]:
    export_run = load_latest_export_run(export_db_path)
    if export_run is None:
        return {"action": "full", "cache_state": "rebuild_required", "reason": "missing_export_run", "dirty_ranges": []}
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        manifest = _load_manifest(conn, "similarity_generation_manifest", generation_key)
    finally:
        conn.close()
    changed_tables = set(export_run["changed_table_names"])
    relevant_changed = changed_tables.intersection(SIMILARITY_RELEVANT_EXPORT_TABLES)
    if manifest and manifest["dependency_version"] == dependency_version and manifest["source_signature"] == export_run["source_signature"]:
        return {
            "action": "skip",
            "cache_state": "fresh",
            "reason": "source_signature_unchanged",
            "dirty_ranges": [],
            "source_signature": export_run["source_signature"],
        }
    if not relevant_changed:
        if not manifest:
            return {
                "action": "full",
                "cache_state": "rebuild_required",
                "reason": "manifest_missing",
                "dirty_ranges": [],
                "source_signature": export_run["source_signature"],
            }
        return {
            "action": "skip",
            "cache_state": "fresh" if manifest else "rebuild_required",
            "reason": "irrelevant_export_change",
            "dirty_ranges": [],
            "source_signature": export_run["source_signature"],
        }
    if any(int((export_run["diff_reason"].get(name) or {}).get("deleted", 0)) > 0 for name in relevant_changed):
        return {
            "action": "full",
            "cache_state": "rebuild_required",
            "reason": "relevant_delete_detected",
            "dirty_ranges": [],
            "source_signature": export_run["source_signature"],
        }
    dirty_ranges = _build_dirty_ranges_from_export_run(
        export_db_path=export_db_path,
        export_run_id=str(export_run["run_id"]),
        relevant_tables=SIMILARITY_RELEVANT_EXPORT_TABLES,
    )
    return {
        "action": "partial" if dirty_ranges and manifest else "full",
        "cache_state": "partial_stale" if dirty_ranges else "rebuild_required",
        "reason": "relevant_export_change",
        "dirty_ranges": dirty_ranges,
        "source_signature": export_run["source_signature"],
    }
