from __future__ import annotations

import json
from typing import Any

from external_analysis.results.manifest import now_utc
from external_analysis.results.result_schema import (
    ALLOWED_FRESHNESS_STATES,
    CONTRACT_VERSION,
    POINTER_NAME_LATEST_SUCCESSFUL,
    SCHEMA_VERSION,
    connect_result_db,
    ensure_result_schema,
)


def publish_result(
    *,
    publish_id: str,
    as_of_date: str,
    freshness_state: str = "fresh",
    pointer_name: str = POINTER_NAME_LATEST_SUCCESSFUL,
    table_row_counts: dict[str, int] | None = None,
    degrade_ready: bool = True,
    db_path: str | None = None,
) -> dict[str, Any]:
    if freshness_state not in ALLOWED_FRESHNESS_STATES:
        raise ValueError(f"unsupported freshness_state: {freshness_state}")
    row_counts = table_row_counts or {}
    ts = now_utc()
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_runs (
                    publish_id, as_of_date, contract_version, schema_version, status, created_at, published_at,
                    validation_summary, row_counts
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    publish_id,
                    as_of_date,
                    CONTRACT_VERSION,
                    SCHEMA_VERSION,
                    "published",
                    ts,
                    ts,
                    json.dumps({"valid": True}, ensure_ascii=False),
                    json.dumps(row_counts, ensure_ascii=False),
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_manifest (
                    publish_id, as_of_date, schema_version, contract_version, status, published_at,
                    freshness_state, degrade_ready, table_row_counts
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    publish_id,
                    as_of_date,
                    SCHEMA_VERSION,
                    CONTRACT_VERSION,
                    "published",
                    ts,
                    freshness_state,
                    bool(degrade_ready),
                    json.dumps(row_counts, ensure_ascii=False),
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_pointer (
                    pointer_name, publish_id, as_of_date, published_at, schema_version, contract_version, freshness_state
                ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?)
                """,
                [
                    pointer_name,
                    publish_id,
                    as_of_date,
                    ts,
                    SCHEMA_VERSION,
                    CONTRACT_VERSION,
                    freshness_state,
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "publish_id": publish_id,
            "pointer_name": pointer_name,
            "as_of_date": as_of_date,
            "freshness_state": freshness_state,
        }
    finally:
        conn.close()
