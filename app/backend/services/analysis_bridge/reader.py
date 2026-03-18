from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb

from app.backend.services.analysis_bridge.contracts import (
    ALLOWED_PUBLIC_TABLES,
    DEGRADE_REASON_HARD_STALE,
    DEGRADE_REASON_MANIFEST_MISMATCH,
    DEGRADE_REASON_NO_PUBLISH,
    DEGRADE_REASON_POINTER_CORRUPTION,
    DEGRADE_REASON_RESULT_DB_MISSING,
    DEGRADE_REASON_REGIME_ROW_CORRUPTION,
    DEGRADE_REASON_SCHEMA_MISMATCH,
    DEGRADE_REASON_WARNING_STALE,
    LATEST_POINTER_NAME,
    MAX_PUBLIC_SIMILAR_CASE_ROWS,
    MAX_PUBLIC_SIMILAR_PATH_ROWS,
    allowed_public_columns,
    is_allowed_public_table,
)
from app.backend.services.analysis_bridge.degrade import build_degrade_payload
from external_analysis.exporter.source_reader import normalize_market_date
from external_analysis.contracts.paths import resolve_ops_db_path, resolve_result_db_path, resolve_source_db_path
from external_analysis.ops.ops_schema import connect_ops_db, ensure_ops_schema
from external_analysis.ops.store import persist_promotion_decision
from external_analysis.results.result_schema import (
    CONTRACT_VERSION,
    SCHEMA_VERSION,
)

CANDLE_RESEARCH_TAGS: set[str] = {
    "bullish_engulfing",
    "hammer_reversal",
    "inside_break_bull",
    "bullish_follow_through",
    "bearish_engulfing",
    "shooting_star_reversal",
    "inside_break_bear",
    "bearish_follow_through",
}

CANDLE_COMBO_RESEARCH_TAGS: set[str] = {
    "bullish_engulfing_after_inside",
    "hammer_after_bear",
    "three_bar_bull_reversal",
    "bearish_engulfing_after_inside",
    "shooting_star_after_bull",
    "three_bar_bear_reversal",
}


def _connect_read_only() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(resolve_result_db_path()), read_only=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _public_table_counts(conn: duckdb.DuckDBPyConnection, publish_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in ALLOWED_PUBLIC_TABLES:
        if table_name in {"publish_pointer", "publish_manifest"}:
            continue
        if not is_allowed_public_table(table_name):
            continue
        if not _table_exists(conn, table_name):
            counts[table_name] = -1
            continue
        row = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
        counts[table_name] = int(row[0]) if row else 0
    return counts


def _load_public_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    publish_id: str,
    order_by: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if not is_allowed_public_table(table_name):
        return []
    columns = allowed_public_columns(table_name)
    if not columns:
        return []
    if not _table_exists(conn, table_name):
        return []
    query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE publish_id = ? ORDER BY {order_by}"
    params: list[Any] = [publish_id]
    if limit is not None:
        query += " LIMIT ?"
        params.append(int(limit))
    rows = conn.execute(query, params).fetchall()
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _public_payload_metadata(snapshot: dict[str, Any]) -> dict[str, Any]:
    publish = snapshot.get("publish") or {}
    return {
        "publish_id": publish.get("publish_id"),
        "as_of_date": publish.get("as_of_date"),
        "freshness_state": publish.get("freshness_state"),
    }


def get_analysis_bridge_snapshot(pointer_name: str = LATEST_POINTER_NAME) -> dict[str, Any]:
    db_path = resolve_result_db_path()
    if not db_path.exists():
        payload = build_degrade_payload(DEGRADE_REASON_RESULT_DB_MISSING)
        payload.update({"publish": None, "public_table_counts": {}})
        return payload
    try:
        conn = _connect_read_only()
    except Exception:
        payload = build_degrade_payload(DEGRADE_REASON_RESULT_DB_MISSING)
        payload.update({"publish": None, "public_table_counts": {}})
        return payload
    try:
        if not _table_exists(conn, "publish_pointer"):
            payload = build_degrade_payload(DEGRADE_REASON_POINTER_CORRUPTION)
            payload.update({"publish": None, "public_table_counts": {}})
            return payload
        try:
            pointer_rows = conn.execute("SELECT COUNT(*) FROM publish_pointer").fetchone()
        except Exception:
            payload = build_degrade_payload(DEGRADE_REASON_POINTER_CORRUPTION)
            payload.update({"publish": None, "public_table_counts": {}})
            return payload
        pointer_row_count = int(pointer_rows[0]) if pointer_rows else 0
        if pointer_row_count > 1:
            payload = build_degrade_payload(DEGRADE_REASON_POINTER_CORRUPTION)
            payload.update({"publish": None, "public_table_counts": {}})
            return payload
        try:
            pointer_row = conn.execute(
                """
                SELECT pointer_name, publish_id, CAST(as_of_date AS VARCHAR), published_at, schema_version, contract_version, freshness_state
                FROM publish_pointer
                WHERE pointer_name = ?
                """,
                [pointer_name],
            ).fetchone()
        except Exception:
            payload = build_degrade_payload(DEGRADE_REASON_POINTER_CORRUPTION)
            payload.update({"publish": None, "public_table_counts": {}})
            return payload
        if not pointer_row:
            payload = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
            payload.update({"publish": None, "public_table_counts": {}})
            return payload
        pointer = {
            "pointer_name": str(pointer_row[0]),
            "publish_id": str(pointer_row[1]),
            "as_of_date": str(pointer_row[2]),
            "published_at": str(pointer_row[3]),
            "schema_version": str(pointer_row[4]),
            "contract_version": str(pointer_row[5]),
            "freshness_state": str(pointer_row[6]),
        }
        if pointer["schema_version"] != SCHEMA_VERSION or pointer["contract_version"] != CONTRACT_VERSION:
            payload = build_degrade_payload(DEGRADE_REASON_SCHEMA_MISMATCH)
            payload.update({"publish": pointer, "public_table_counts": {}})
            return payload
        if not _table_exists(conn, "publish_manifest"):
            payload = build_degrade_payload(DEGRADE_REASON_MANIFEST_MISMATCH)
            payload.update({"publish": pointer, "public_table_counts": {}})
            return payload
        try:
            manifest_row = conn.execute(
                """
                SELECT publish_id, CAST(as_of_date AS VARCHAR), schema_version, contract_version, status, published_at, freshness_state, degrade_ready, table_row_counts
                FROM publish_manifest
                WHERE publish_id = ?
                """,
                [pointer["publish_id"]],
            ).fetchone()
        except Exception:
            payload = build_degrade_payload(DEGRADE_REASON_MANIFEST_MISMATCH)
            payload.update({"publish": pointer, "public_table_counts": {}})
            return payload
        if not manifest_row:
            payload = build_degrade_payload(DEGRADE_REASON_MANIFEST_MISMATCH)
            payload.update({"publish": pointer, "public_table_counts": {}})
            return payload
        manifest = {
            "publish_id": str(manifest_row[0]),
            "as_of_date": str(manifest_row[1]),
            "schema_version": str(manifest_row[2]),
            "contract_version": str(manifest_row[3]),
            "status": str(manifest_row[4]),
            "published_at": str(manifest_row[5]),
            "freshness_state": str(manifest_row[6]),
            "degrade_ready": bool(manifest_row[7]),
            "table_row_counts": manifest_row[8],
        }
        if manifest["publish_id"] != pointer["publish_id"] or manifest["schema_version"] != pointer["schema_version"] or manifest["contract_version"] != pointer["contract_version"]:
            payload = build_degrade_payload(DEGRADE_REASON_MANIFEST_MISMATCH)
            payload.update({"publish": pointer, "public_table_counts": {}})
            return payload
        freshness_state = pointer["freshness_state"]
        if freshness_state == "warning":
            payload = build_degrade_payload(DEGRADE_REASON_WARNING_STALE)
            payload.update({"publish": pointer, "manifest": manifest, "public_table_counts": _public_table_counts(conn, pointer["publish_id"])})
            return payload
        if freshness_state == "hard":
            payload = build_degrade_payload(DEGRADE_REASON_HARD_STALE)
            payload.update({"publish": pointer, "manifest": manifest, "public_table_counts": _public_table_counts(conn, pointer["publish_id"])})
            return payload
        return {
            "degraded": False,
            "degrade_reason": None,
            "stale_message": None,
            "cta_suppressed": False,
            "show_candidates": True,
            "show_similar_cases": True,
            "show_state_evaluation": True,
            "app_continues": True,
            "publish": pointer,
            "manifest": manifest,
            "public_table_counts": _public_table_counts(conn, pointer["publish_id"]),
        }
    finally:
        conn.close()


def get_candidate_daily_rows(pointer_name: str = LATEST_POINTER_NAME, *, limit_per_side: int = 20) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "rows": [], "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    conn = _connect_read_only()
    try:
        rows = _load_public_rows(
            conn,
            table_name="candidate_daily",
            publish_id=publish_id,
            order_by="rank_position ASC, code ASC",
        )
    finally:
        conn.close()
    by_side: dict[str, list[dict[str, Any]]] = {"long": [], "short": []}
    for row in rows:
        side = str(row.get("side") or "")
        if side in by_side and len(by_side[side]) < int(limit_per_side):
            by_side[side].append(row)
    ordered_rows = by_side["long"] + by_side["short"]
    snapshot.update({"rows": ordered_rows, **_public_payload_metadata(snapshot)})
    return snapshot


def get_regime_daily_rows(pointer_name: str = LATEST_POINTER_NAME) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "rows": [], "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    conn = _connect_read_only()
    try:
        rows = _load_public_rows(
            conn,
            table_name="regime_daily",
            publish_id=publish_id,
            order_by="as_of_date DESC, regime_tag ASC",
            limit=10,
        )
    finally:
        conn.close()
    if len(rows) > 1:
        degraded = build_degrade_payload(DEGRADE_REASON_REGIME_ROW_CORRUPTION)
        degraded.update(
            {
                "publish": snapshot.get("publish"),
                "manifest": snapshot.get("manifest"),
                "public_table_counts": snapshot.get("public_table_counts", {}),
                "rows": [],
                **_public_payload_metadata(snapshot),
            }
        )
        return degraded
    snapshot.update({"rows": rows, **_public_payload_metadata(snapshot)})
    return snapshot


def get_state_eval_rows(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    code: str | None = None,
    limit: int = 40,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "rows": [], "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    effective_limit = max(1, min(int(limit), 200))
    conn = _connect_read_only()
    try:
        if not _table_exists(conn, "state_eval_daily"):
            snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
            return snapshot
        where_sql = "WHERE publish_id = ?"
        params: list[Any] = [publish_id]
        if side:
            where_sql += " AND side = ?"
            params.append(str(side))
        if code:
            where_sql += " AND code = ?"
            params.append(str(code))
        rows = conn.execute(
            f"""
            SELECT publish_id, as_of_date, code, side, holding_band, strategy_tags, state_action, decision_3way, confidence, reason_codes, reason_text_top3, freshness_state
            FROM state_eval_daily
            {where_sql}
            ORDER BY side ASC, confidence DESC, code ASC
            LIMIT ?
            """,
            [*params, effective_limit],
        ).fetchall()
    finally:
        conn.close()
    columns = allowed_public_columns("state_eval_daily")
    snapshot.update({"rows": [dict(zip(columns, row, strict=True)) for row in rows], **_public_payload_metadata(snapshot)})
    return snapshot


def get_internal_state_eval_tag_rows(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    strategy_tag: str | None = None,
    limit: int = 40,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "rows": [], "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    effective_limit = max(1, min(int(limit), 200))
    conn = connect_ops_db()
    try:
        ensure_ops_schema(conn)
        where_sql = "WHERE publish_id = ?"
        params: list[Any] = [publish_id]
        if side:
            where_sql += " AND side = ?"
            params.append(str(side))
        if strategy_tag:
            where_sql += " AND strategy_tag = ?"
            params.append(str(strategy_tag))
        rows = conn.execute(
            f"""
            SELECT
                publish_id, CAST(as_of_date AS VARCHAR), side, holding_band, strategy_tag,
                observation_count, labeled_count, enter_count, wait_count, skip_count,
                expectancy_mean, adverse_mean, large_loss_rate, win_rate, teacher_alignment_mean,
                failure_count, readiness_hint, latest_failure_examples, worst_failure_examples, summary_json
            FROM external_state_eval_tag_rollups
            {where_sql}
            ORDER BY side ASC, labeled_count DESC, expectancy_mean DESC NULLS LAST, strategy_tag ASC
            LIMIT ?
            """,
            [*params, effective_limit],
        ).fetchall()
    finally:
        conn.close()
    columns = (
        "publish_id",
        "as_of_date",
        "side",
        "holding_band",
        "strategy_tag",
        "observation_count",
        "labeled_count",
        "enter_count",
        "wait_count",
        "skip_count",
        "expectancy_mean",
        "adverse_mean",
        "large_loss_rate",
        "win_rate",
        "teacher_alignment_mean",
        "failure_count",
        "readiness_hint",
        "latest_failure_examples",
        "worst_failure_examples",
        "summary_json",
    )
    snapshot.update({"rows": [dict(zip(columns, row, strict=True)) for row in rows], **_public_payload_metadata(snapshot)})
    return snapshot


def get_internal_state_eval_tag_summary(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    payload = get_internal_state_eval_tag_rows(pointer_name=pointer_name, side=side, limit=500)
    if payload.get("degraded"):
        payload.update({"summary": {"top_expectancy": [], "risk_heavy": [], "needs_samples": []}})
        return payload
    rows = list(payload.get("rows") or [])
    effective_limit = max(1, min(int(limit), 20))

    def _to_float(row: dict[str, Any], key: str, default: float) -> float:
        value = row.get(key)
        try:
            return float(value) if value is not None else float(default)
        except (TypeError, ValueError):
            return float(default)

    top_expectancy = sorted(
        [
            row
            for row in rows
            if row.get("expectancy_mean") is not None and str(row.get("readiness_hint") or "") != "needs_samples"
        ],
        key=lambda row: (_to_float(row, "expectancy_mean", -999.0), _to_float(row, "labeled_count", 0.0)),
        reverse=True,
    )[:effective_limit]
    risk_heavy = sorted(
        [
            row
            for row in rows
            if str(row.get("readiness_hint") or "") in {"risk_heavy", "negative_expectancy"}
            or _to_float(row, "large_loss_rate", 0.0) >= 0.35
        ],
        key=lambda row: (_to_float(row, "large_loss_rate", 0.0), -_to_float(row, "expectancy_mean", 0.0)),
        reverse=True,
    )[:effective_limit]
    needs_samples = sorted(
        [row for row in rows if str(row.get("readiness_hint") or "") == "needs_samples"],
        key=lambda row: (_to_float(row, "labeled_count", 0.0), _to_float(row, "observation_count", 0.0)),
    )[:effective_limit]
    payload.update(
        {
            "summary": {
                "top_expectancy": top_expectancy,
                "risk_heavy": risk_heavy,
                "needs_samples": needs_samples,
            },
            **_public_payload_metadata(payload),
        }
    )
    return payload


def get_internal_state_eval_candle_summary(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    payload = get_internal_state_eval_tag_rows(pointer_name=pointer_name, side=side, limit=500)
    if payload.get("degraded"):
        payload.update({"summary": {"top_expectancy": [], "risk_heavy": [], "needs_samples": []}})
        return payload
    rows = [
        row
        for row in list(payload.get("rows") or [])
        if str(row.get("strategy_tag") or "") in CANDLE_RESEARCH_TAGS
    ]
    effective_limit = max(1, min(int(limit), 20))

    def _to_float(row: dict[str, Any], key: str, default: float) -> float:
        value = row.get(key)
        try:
            return float(value) if value is not None else float(default)
        except (TypeError, ValueError):
            return float(default)

    top_expectancy = sorted(
        [
            row
            for row in rows
            if row.get("expectancy_mean") is not None and str(row.get("readiness_hint") or "") != "needs_samples"
        ],
        key=lambda row: (_to_float(row, "expectancy_mean", -999.0), _to_float(row, "labeled_count", 0.0)),
        reverse=True,
    )[:effective_limit]
    risk_heavy = sorted(
        [
            row
            for row in rows
            if str(row.get("readiness_hint") or "") in {"risk_heavy", "negative_expectancy"}
            or _to_float(row, "large_loss_rate", 0.0) >= 0.35
        ],
        key=lambda row: (_to_float(row, "large_loss_rate", 0.0), -_to_float(row, "expectancy_mean", 0.0)),
        reverse=True,
    )[:effective_limit]
    needs_samples = sorted(
        [row for row in rows if str(row.get("readiness_hint") or "") == "needs_samples"],
        key=lambda row: (_to_float(row, "labeled_count", 0.0), _to_float(row, "observation_count", 0.0)),
    )[:effective_limit]
    payload.update(
        {
            "rows": rows,
            "summary": {
                "top_expectancy": top_expectancy,
                "risk_heavy": risk_heavy,
                "needs_samples": needs_samples,
            },
            **_public_payload_metadata(payload),
        }
    )
    return payload


def get_internal_state_eval_candle_combo_summary(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    payload = get_internal_state_eval_tag_rows(pointer_name=pointer_name, side=side, limit=500)
    if payload.get("degraded"):
        payload.update({"summary": {"top_expectancy": [], "risk_heavy": [], "needs_samples": []}})
        return payload
    rows = [
        row
        for row in list(payload.get("rows") or [])
        if str(row.get("strategy_tag") or "") in CANDLE_COMBO_RESEARCH_TAGS
    ]
    effective_limit = max(1, min(int(limit), 20))

    def _to_float(row: dict[str, Any], key: str, default: float) -> float:
        value = row.get(key)
        try:
            return float(value) if value is not None else float(default)
        except (TypeError, ValueError):
            return float(default)

    top_expectancy = sorted(
        [
            row
            for row in rows
            if row.get("expectancy_mean") is not None and str(row.get("readiness_hint") or "") != "needs_samples"
        ],
        key=lambda row: (_to_float(row, "expectancy_mean", -999.0), _to_float(row, "labeled_count", 0.0)),
        reverse=True,
    )[:effective_limit]
    risk_heavy = sorted(
        [
            row
            for row in rows
            if str(row.get("readiness_hint") or "") in {"risk_heavy", "negative_expectancy"}
            or _to_float(row, "large_loss_rate", 0.0) >= 0.35
        ],
        key=lambda row: (_to_float(row, "large_loss_rate", 0.0), -_to_float(row, "expectancy_mean", 0.0)),
        reverse=True,
    )[:effective_limit]
    needs_samples = sorted(
        [row for row in rows if str(row.get("readiness_hint") or "") == "needs_samples"],
        key=lambda row: (_to_float(row, "labeled_count", 0.0), _to_float(row, "observation_count", 0.0)),
    )[:effective_limit]
    payload.update(
        {
            "rows": rows,
            "summary": {
                "top_expectancy": top_expectancy,
                "risk_heavy": risk_heavy,
                "needs_samples": needs_samples,
            },
            **_public_payload_metadata(payload),
        }
    )
    return payload


def get_internal_state_eval_daily_summary(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update(
            {
                "daily_summary": {
                    "promotion": None,
                    "top_strategy": None,
                    "top_candle": None,
                    "risk_watch": None,
                    "sample_watch": None,
                },
                **_public_payload_metadata(snapshot),
            }
        )
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if publish_id:
        conn = connect_ops_db()
        try:
            ensure_ops_schema(conn)
            scope = str(side or "all")
            row = conn.execute(
                """
                SELECT summary_json
                FROM external_state_eval_daily_summaries
                WHERE publish_id = ? AND side_scope = ?
                """,
                [publish_id, scope],
            ).fetchone()
        finally:
            conn.close()
        if row and row[0] is not None:
            try:
                daily_summary = json.loads(str(row[0]))
            except (TypeError, ValueError, json.JSONDecodeError):
                daily_summary = None
            if isinstance(daily_summary, dict):
                snapshot.update({"daily_summary": daily_summary, **_public_payload_metadata(snapshot)})
                return snapshot

    tag_payload = get_internal_state_eval_tag_summary(pointer_name=pointer_name, side=side, limit=3)
    candle_payload = get_internal_state_eval_candle_summary(pointer_name=pointer_name, side=side, limit=3)
    review_payload = get_internal_state_eval_promotion_review(pointer_name=pointer_name)
    tag_summary = tag_payload.get("summary") or {}
    candle_summary = candle_payload.get("summary") or {}
    review = review_payload.get("review")
    daily_summary = {
        "promotion": review,
        "top_strategy": (tag_summary.get("top_expectancy") or [None])[0],
        "top_candle": (candle_summary.get("top_expectancy") or [None])[0],
        "risk_watch": (tag_summary.get("risk_heavy") or [None])[0],
        "sample_watch": (tag_summary.get("needs_samples") or [None])[0],
    }
    tag_payload.update({"daily_summary": daily_summary, **_public_payload_metadata(tag_payload)})
    return tag_payload


def get_internal_state_eval_daily_summary_history(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    limit: int = 30,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    effective_limit = max(1, min(int(limit), 120))
    conn = connect_ops_db()
    try:
        ensure_ops_schema(conn)
        where_sql = "WHERE side_scope = ?"
        params: list[Any] = [str(side or "all")]
        raw_rows = conn.execute(
            f"""
            SELECT
                publish_id, CAST(as_of_date AS VARCHAR), side_scope,
                top_strategy_tag, top_strategy_expectancy,
                top_candle_tag, top_candle_expectancy,
                risk_watch_tag, risk_watch_loss_rate,
                sample_watch_tag, sample_watch_labeled_count,
                promotion_ready, promotion_sample_count,
                summary_json
            FROM external_state_eval_daily_summaries
            {where_sql}
            ORDER BY as_of_date DESC, created_at DESC
            LIMIT ?
            """,
            [*params, effective_limit],
        ).fetchall()
        publish_ids = [str(row[0]) for row in raw_rows if row and row[0] is not None]
        decision_map: dict[str, dict[str, Any]] = {}
        if publish_ids:
            placeholders = ", ".join(["?"] * len(publish_ids))
            decision_rows = conn.execute(
                f"""
                SELECT publish_id, decision, note, actor, CAST(created_at AS VARCHAR)
                FROM (
                    SELECT
                        publish_id,
                        decision,
                        note,
                        actor,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY publish_id
                            ORDER BY created_at DESC, decision_id DESC
                        ) AS row_num
                    FROM external_promotion_decisions
                    WHERE publish_id IN ({placeholders})
                )
                WHERE row_num = 1
                """,
                publish_ids,
            ).fetchall()
            decision_map = {
                str(row[0]): {
                    "decision": row[1],
                    "note": row[2],
                    "actor": row[3],
                    "created_at": row[4],
                }
                for row in decision_rows
            }
    finally:
        conn.close()
    columns = (
        "publish_id",
        "as_of_date",
        "side_scope",
        "top_strategy_tag",
        "top_strategy_expectancy",
        "top_candle_tag",
        "top_candle_expectancy",
        "risk_watch_tag",
        "risk_watch_loss_rate",
        "sample_watch_tag",
        "sample_watch_labeled_count",
        "promotion_ready",
        "promotion_sample_count",
        "summary_json",
    )
    rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        row_dict = dict(zip(columns, raw_row, strict=True))
        latest_decision = decision_map.get(str(row_dict.get("publish_id") or ""))
        promotion_ready = bool(row_dict.get("promotion_ready"))
        if latest_decision:
            row_dict["approval_decision"] = latest_decision
            row_dict["decision_status"] = "recorded"
            row_dict["codex_command"] = None
        else:
            row_dict["approval_decision"] = None
            row_dict["decision_status"] = "pending" if promotion_ready else "not_ready"
            row_dict["codex_command"] = (
                'python -m external_analysis promotion-decision-run --decision hold --note "needs_manual_review"'
                if promotion_ready
                else None
            )
        rows.append(row_dict)
    snapshot.update({"rows": rows, **_public_payload_metadata(snapshot)})
    return snapshot


def get_internal_state_eval_action_queue(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"actions": [], **_public_payload_metadata(snapshot)})
        return snapshot
    daily_payload = get_internal_state_eval_daily_summary(pointer_name=pointer_name, side=side)
    trend_payload = get_internal_state_eval_trend_summary(pointer_name=pointer_name, side=side, lookback=14, limit=3)
    combo_payload = get_internal_state_eval_candle_combo_trend_summary(pointer_name=pointer_name, side=side, lookback=14, limit=3)
    review_payload = get_internal_state_eval_promotion_review(pointer_name=pointer_name)

    actions: list[dict[str, Any]] = []
    daily_summary = daily_payload.get("daily_summary") or {}
    review = review_payload.get("review") or {}
    if review:
        approval_decision = review.get("approval_decision") or {}
        if not approval_decision:
            actions.append(
                {
                    "kind": "promotion_decision_pending",
                    "priority": 1,
                    "title": "Record promotion decision",
                    "label": "Review",
                    "side": str(side or "all"),
                    "strategy_tag": None,
                    "holding_band": None,
                    "metric_label": "Expectancy delta",
                    "metric_value": review.get("expectancy_delta"),
                    "note": "run promotion-decision-run from Codex",
                }
            )
        else:
            actions.append(
                {
                    "kind": "promotion_review",
                    "priority": 2,
                    "title": "Promotion decision recorded",
                    "label": "Review",
                    "side": str(side or "all"),
                    "strategy_tag": None,
                    "holding_band": None,
                    "metric_label": "Expectancy delta",
                    "metric_value": review.get("expectancy_delta"),
                    "note": f"latest decision: {approval_decision.get('decision')}",
                }
            )
    top_strategy = daily_summary.get("top_strategy") or {}
    if isinstance(top_strategy, dict) and top_strategy.get("strategy_tag"):
        actions.append(
            {
                "kind": "top_strategy",
                "priority": 2,
                "title": "Monitor top strategy",
                "label": "Watch",
                "side": top_strategy.get("side"),
                "strategy_tag": top_strategy.get("strategy_tag"),
                "holding_band": top_strategy.get("holding_band"),
                "metric_label": "Expectancy",
                "metric_value": top_strategy.get("expectancy_mean"),
                "note": daily_summary.get("top_strategy_reason"),
            }
        )
    risk_watch = daily_summary.get("risk_watch") or {}
    if isinstance(risk_watch, dict) and risk_watch.get("strategy_tag"):
        actions.append(
            {
                "kind": "risk_watch",
                "priority": 3,
                "title": "Review risk-heavy tag",
                "label": "Risk",
                "side": risk_watch.get("side"),
                "strategy_tag": risk_watch.get("strategy_tag"),
                "holding_band": risk_watch.get("holding_band"),
                "metric_label": "Loss rate",
                "metric_value": risk_watch.get("large_loss_rate"),
                "note": daily_summary.get("risk_watch_reason"),
            }
        )
    sample_watch = daily_summary.get("sample_watch") or {}
    if isinstance(sample_watch, dict) and sample_watch.get("strategy_tag"):
        actions.append(
            {
                "kind": "sample_watch",
                "priority": 5,
                "title": "Collect more samples",
                "label": "Study",
                "side": sample_watch.get("side"),
                "strategy_tag": sample_watch.get("strategy_tag"),
                "holding_band": sample_watch.get("holding_band"),
                "metric_label": "Samples",
                "metric_value": sample_watch.get("labeled_count"),
                "note": daily_summary.get("sample_watch_reason"),
            }
        )
    trends = trend_payload.get("trends") or {}
    improving = list(trends.get("improving") or [])
    if improving:
        top = improving[0]
        actions.append(
            {
                "kind": "improving_tag",
                "priority": 4,
                "title": "Track improving tag",
                "label": "Trend",
                "side": top.get("side"),
                "strategy_tag": top.get("strategy_tag"),
                "holding_band": top.get("holding_band"),
                "metric_label": "Exp delta",
                "metric_value": top.get("expectancy_delta"),
                "note": "improving over recent windows",
            }
        )
    combo_trends = combo_payload.get("trends") or {}
    combo_improving = list(combo_trends.get("improving") or [])
    if combo_improving:
        top = combo_improving[0]
        actions.append(
            {
                "kind": "improving_combo",
                "priority": 4,
                "title": "Track improving combo",
                "label": "Combo",
                "side": top.get("side"),
                "strategy_tag": top.get("strategy_tag"),
                "holding_band": top.get("holding_band"),
                "metric_label": "Exp delta",
                "metric_value": top.get("expectancy_delta"),
                "note": "combo pattern gaining strength",
            }
        )
    actions = sorted(actions, key=lambda item: (int(item.get("priority") or 99), str(item.get("title") or "")))[:6]
    snapshot.update({"actions": actions, **_public_payload_metadata(snapshot)})
    return snapshot


def get_internal_replay_progress(*, replay_id: str | None = None, recent_limit: int = 5) -> dict[str, Any]:
    conn = duckdb.connect(str(resolve_ops_db_path()), read_only=True)
    try:
        effective_limit = max(1, min(int(recent_limit), 10))
        where_sql = ""
        params: list[Any] = []
        if replay_id and str(replay_id).strip():
            where_sql = "WHERE replay_id = ?"
            params.append(str(replay_id).strip())
        run_rows = conn.execute(
            f"""
            SELECT
                replay_id,
                status,
                CAST(start_as_of_date AS VARCHAR),
                CAST(end_as_of_date AS VARCHAR),
                max_days,
                universe_limit,
                CAST(created_at AS VARCHAR),
                CAST(started_at AS VARCHAR),
                CAST(finished_at AS VARCHAR),
                CAST(last_completed_as_of_date AS VARCHAR),
                error_class,
                details_json
            FROM external_replay_runs
            {where_sql}
            ORDER BY
                CASE WHEN status = 'running' THEN 0 ELSE 1 END,
                COALESCE(started_at, created_at) DESC,
                replay_id DESC
            LIMIT ?
            """,
            [*params, effective_limit],
        ).fetchall()
        if not run_rows:
            return {
                "running": False,
                "current_run": None,
                "recent_runs": [],
            }
        run_columns = (
            "replay_id",
            "status",
            "start_as_of_date",
            "end_as_of_date",
            "max_days",
            "universe_limit",
            "created_at",
            "started_at",
            "finished_at",
            "last_completed_as_of_date",
            "error_class",
            "details_json",
        )
        runs = [dict(zip(run_columns, row, strict=True)) for row in run_rows]
        replay_ids = [str(row["replay_id"]) for row in runs]
        placeholders = ", ".join(["?"] * len(replay_ids))
        day_rows = conn.execute(
            f"""
            SELECT replay_id, status, COUNT(*)
            FROM external_replay_days
            WHERE replay_id IN ({placeholders})
            GROUP BY replay_id, status
            """,
            replay_ids,
        ).fetchall()
        current_day_rows = conn.execute(
            f"""
            SELECT replay_id, CAST(as_of_date AS VARCHAR), publish_id, CAST(started_at AS VARCHAR)
            FROM (
                SELECT
                    replay_id,
                    as_of_date,
                    publish_id,
                    started_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY replay_id
                        ORDER BY started_at DESC NULLS LAST, as_of_date DESC
                    ) AS row_num
                FROM external_replay_days
                WHERE replay_id IN ({placeholders}) AND status = 'running'
            )
            WHERE row_num = 1
            """,
            replay_ids,
        ).fetchall()
    finally:
        conn.close()

    day_counts: dict[str, dict[str, int]] = {}
    for replay_key, status, count in day_rows:
        replay_dict = day_counts.setdefault(str(replay_key), {})
        replay_dict[str(status)] = int(count)
    current_days = {
        str(row[0]): {
            "as_of_date": row[1],
            "publish_id": row[2],
            "started_at": row[3],
        }
        for row in current_day_rows
    }

    source_conn = duckdb.connect(str(resolve_source_db_path()), read_only=True)
    try:
        total_days_by_replay: dict[str, int] = {}
        for row in runs:
            replay_key = str(row["replay_id"])
            start_value = int(str(row["start_as_of_date"]).replace("-", ""))
            end_value = int(str(row["end_as_of_date"]).replace("-", ""))
            raw_dates = source_conn.execute("SELECT DISTINCT date FROM daily_bars ORDER BY date").fetchall()
            normalized_dates = [
                int(normalized)
                for normalized in (normalize_market_date(raw[0]) for raw in raw_dates)
                if normalized is not None and start_value <= int(normalized) <= end_value
            ]
            total_days = len(normalized_dates)
            if row.get("max_days") is not None:
                total_days = min(total_days, int(row["max_days"]))
            total_days_by_replay[replay_key] = total_days
    finally:
        source_conn.close()

    hydrated_runs: list[dict[str, Any]] = []
    now = _utcnow()
    for row in runs:
        replay_key = str(row["replay_id"])
        details = row.get("details_json")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                details = {}
        elif not isinstance(details, dict):
            details = {}
        counts = day_counts.get(replay_key, {})
        success_days = int(counts.get("success", 0))
        failed_days = int(counts.get("failed", 0))
        skipped_days = int(counts.get("skipped", 0))
        running_days = int(counts.get("running", 0))
        processed_days = success_days + failed_days + skipped_days + running_days
        total_days = int(total_days_by_replay.get(replay_key, 0))
        completed_days = success_days + failed_days + skipped_days
        remaining_days = max(total_days - completed_days - running_days, 0)
        progress_pct = round((completed_days / total_days) * 100.0, 1) if total_days > 0 else 0.0
        started_at = _parse_timestamp(row.get("started_at") or row.get("created_at"))
        eta_seconds: int | None = None
        eta_at: str | None = None
        if started_at is not None and completed_days > 0 and remaining_days > 0:
            elapsed_seconds = max((now - started_at).total_seconds(), 1.0)
            days_per_second = completed_days / elapsed_seconds
            if days_per_second > 0:
                eta_seconds = int(round(remaining_days / days_per_second))
                eta_at = (now + timedelta(seconds=eta_seconds)).isoformat(timespec="seconds")
        hydrated_runs.append(
            {
                **row,
                "total_days": total_days,
                "completed_days": completed_days,
                "processed_days": processed_days,
                "remaining_days": remaining_days,
                "success_days": success_days,
                "failed_days": failed_days,
                "skipped_days": skipped_days,
                "running_days": running_days,
                "progress_pct": progress_pct,
                "current_day": current_days.get(replay_key),
                "current_phase": details.get("current_phase"),
                "last_heartbeat_at": details.get("heartbeat_at"),
                "current_publish_id": details.get("current_publish_id"),
                "eta_seconds": eta_seconds,
                "eta_at": eta_at,
            }
        )
    current_run = hydrated_runs[0] if hydrated_runs else None
    return {
        "running": bool(current_run and current_run.get("status") == "running"),
        "current_run": current_run,
        "recent_runs": hydrated_runs,
    }


def get_internal_state_eval_trend_summary(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    lookback: int = 14,
    limit: int = 5,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"trends": {"improving": [], "weakening": [], "persistent_risk": []}, **_public_payload_metadata(snapshot)})
        return snapshot
    effective_lookback = max(4, min(int(lookback), 60))
    effective_limit = max(1, min(int(limit), 20))
    conn = connect_ops_db()
    try:
        ensure_ops_schema(conn)
        where_sql = ""
        params: list[Any] = []
        if side:
            where_sql = "WHERE side = ?"
            params.append(str(side))
        rows = conn.execute(
            f"""
            SELECT
                CAST(as_of_date AS VARCHAR),
                side,
                holding_band,
                strategy_tag,
                labeled_count,
                expectancy_mean,
                large_loss_rate,
                teacher_alignment_mean,
                summary_json
            FROM external_state_eval_tag_rollups
            {where_sql}
            ORDER BY as_of_date DESC, strategy_tag ASC
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    unique_dates: list[str] = []
    for row in rows:
        date_text = str(row[0])
        if date_text not in unique_dates:
            unique_dates.append(date_text)
        if len(unique_dates) >= effective_lookback:
            break
    allowed_dates = set(unique_dates)
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        date_text = str(row[0])
        if date_text not in allowed_dates:
            continue
        key = (str(row[1]), str(row[2]), str(row[3]))
        try:
            summary_json = json.loads(str(row[8] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary_json = {}
        grouped.setdefault(key, []).append(
            {
                "as_of_date": date_text,
                "side": str(row[1]),
                "holding_band": str(row[2]),
                "strategy_tag": str(row[3]),
                "labeled_count": int(row[4] or 0),
                "expectancy_mean": row[5],
                "large_loss_rate": row[6],
                "teacher_alignment_mean": row[7],
                "summary_json": summary_json if isinstance(summary_json, dict) else {},
            }
        )

    improving: list[dict[str, Any]] = []
    weakening: list[dict[str, Any]] = []
    persistent_risk: list[dict[str, Any]] = []
    for (_side, _band, _tag), series in grouped.items():
        ordered = sorted(series, key=lambda item: item["as_of_date"], reverse=True)
        if len(ordered) < 2:
            continue
        split = max(1, len(ordered) // 2)
        recent = ordered[:split]
        prior = ordered[split:]
        if not prior:
            continue

        def _avg(items: list[dict[str, Any]], key: str, default: float = 0.0) -> float:
            values = []
            for item in items:
                value = item.get(key)
                try:
                    if value is not None:
                        values.append(float(value))
                except (TypeError, ValueError):
                    continue
            if not values:
                return float(default)
            return float(sum(values) / len(values))

        recent_expectancy = _avg(recent, "expectancy_mean")
        prior_expectancy = _avg(prior, "expectancy_mean")
        recent_risk = _avg(recent, "large_loss_rate")
        prior_risk = _avg(prior, "large_loss_rate")
        recent_samples = _avg(recent, "labeled_count")
        latest = recent[0]
        trend_row = {
            "side": latest["side"],
            "holding_band": latest["holding_band"],
            "strategy_tag": latest["strategy_tag"],
            "recent_expectancy": recent_expectancy,
            "prior_expectancy": prior_expectancy,
            "expectancy_delta": recent_expectancy - prior_expectancy,
            "recent_risk": recent_risk,
            "prior_risk": prior_risk,
            "risk_delta": recent_risk - prior_risk,
            "recent_labeled_count": int(round(recent_samples)),
            "teacher_signal_mean": latest["summary_json"].get("teacher_signal_mean"),
            "similarity_signal_mean": latest["summary_json"].get("similarity_signal_mean"),
            "last_as_of_date": latest["as_of_date"],
        }
        if trend_row["expectancy_delta"] >= 0.02 and trend_row["risk_delta"] <= 0.05:
            improving.append(trend_row)
        if trend_row["expectancy_delta"] <= -0.02 or trend_row["risk_delta"] >= 0.05:
            weakening.append(trend_row)
        if recent_risk >= 0.35 and prior_risk >= 0.35:
            persistent_risk.append(trend_row)

    improving.sort(key=lambda row: (float(row["expectancy_delta"]), -float(row["risk_delta"])), reverse=True)
    weakening.sort(key=lambda row: (float(row["risk_delta"]), -float(row["expectancy_delta"])), reverse=True)
    persistent_risk.sort(key=lambda row: (float(row["recent_risk"]), -float(row["recent_expectancy"])), reverse=True)
    snapshot.update(
        {
            "trends": {
                "improving": improving[:effective_limit],
                "weakening": weakening[:effective_limit],
                "persistent_risk": persistent_risk[:effective_limit],
            },
            **_public_payload_metadata(snapshot),
        }
    )
    return snapshot


def get_internal_state_eval_candle_combo_trend_summary(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    side: str | None = None,
    lookback: int = 14,
    limit: int = 5,
) -> dict[str, Any]:
    payload = get_internal_state_eval_trend_summary(
        pointer_name=pointer_name,
        side=side,
        lookback=lookback,
        limit=max(limit, 20),
    )
    if payload.get("degraded"):
        payload.update({"trends": {"improving": [], "weakening": [], "persistent_risk": []}})
        return payload
    trends = payload.get("trends") or {}

    def _filtered(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if str(row.get("strategy_tag") or "") in CANDLE_COMBO_RESEARCH_TAGS
        ][: max(1, min(int(limit), 20))]

    payload.update(
        {
            "trends": {
                "improving": _filtered(list(trends.get("improving") or [])),
                "weakening": _filtered(list(trends.get("weakening") or [])),
                "persistent_risk": _filtered(list(trends.get("persistent_risk") or [])),
            },
            **_public_payload_metadata(payload),
        }
    )
    return payload


def get_internal_state_eval_promotion_review(pointer_name: str = LATEST_POINTER_NAME) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"review": None, **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "review": None, "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    conn = connect_ops_db()
    try:
        ensure_ops_schema(conn)
        readiness_row = conn.execute(
            """
            SELECT
                CAST(as_of_date AS VARCHAR), champion_version, challenger_version, sample_count, expectancy_delta,
                improved_expectancy, mae_non_worse, adverse_move_non_worse, stable_window, alignment_ok,
                readiness_pass, reason_codes, summary_json
            FROM external_state_eval_readiness
            WHERE publish_id = ?
            """,
            [publish_id],
        ).fetchone()
        side_rows = conn.execute(
            """
            SELECT
                side,
                COUNT(*) AS compared_count,
                SUM(CASE WHEN champion_decision = 'enter' THEN 1 ELSE 0 END) AS champion_enter_count,
                SUM(CASE WHEN challenger_decision = 'enter' THEN 1 ELSE 0 END) AS challenger_enter_count,
                AVG(expected_return) FILTER (WHERE label_available) AS expected_return_mean,
                AVG(adverse_move) FILTER (WHERE label_available) AS adverse_move_mean,
                AVG(teacher_alignment) FILTER (WHERE label_available) AS teacher_alignment_mean
            FROM external_state_eval_shadow_runs
            WHERE publish_id = ?
            GROUP BY side
            ORDER BY side ASC
            """,
            [publish_id],
        ).fetchall()
        decision_row = conn.execute(
            """
            SELECT decision_id, decision, note, actor, CAST(created_at AS VARCHAR), summary_json
            FROM external_promotion_decisions
            WHERE publish_id = ?
            ORDER BY created_at DESC, decision_id DESC
            LIMIT 1
            """,
            [publish_id],
        ).fetchone()
    finally:
        conn.close()
    if not readiness_row:
        snapshot.update({"review": None, **_public_payload_metadata(snapshot)})
        return snapshot
    summary_json = readiness_row[12]
    try:
        summary = json.loads(str(summary_json or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        summary = {}
    try:
        reason_codes = json.loads(str(readiness_row[11] or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        reason_codes = []
    review = {
        "as_of_date": str(readiness_row[0]),
        "champion_version": str(readiness_row[1]),
        "challenger_version": str(readiness_row[2]),
        "sample_count": int(readiness_row[3]),
        "expectancy_delta": readiness_row[4],
        "improved_expectancy": bool(readiness_row[5]),
        "mae_non_worse": bool(readiness_row[6]),
        "adverse_move_non_worse": bool(readiness_row[7]),
        "stable_window": bool(readiness_row[8]),
        "alignment_ok": bool(readiness_row[9]),
        "readiness_pass": bool(readiness_row[10]),
        "reason_codes": reason_codes if isinstance(reason_codes, list) else [],
        "summary": summary,
        "approval_decision": None
        if not decision_row
        else {
            "decision_id": str(decision_row[0]),
            "decision": str(decision_row[1]),
            "note": None if decision_row[2] is None else str(decision_row[2]),
            "actor": None if decision_row[3] is None else str(decision_row[3]),
            "created_at": str(decision_row[4]),
            "summary": json.loads(str(decision_row[5] or "{}")),
        },
        "by_side": [
            {
                "side": str(row[0]),
                "compared_count": int(row[1]),
                "champion_enter_count": int(row[2]),
                "challenger_enter_count": int(row[3]),
                "expected_return_mean": row[4],
                "adverse_move_mean": row[5],
                "teacher_alignment_mean": row[6],
            }
            for row in side_rows
        ],
    }
    snapshot.update({"review": review, **_public_payload_metadata(snapshot)})
    return snapshot


def save_internal_state_eval_promotion_decision(
    *,
    decision: str,
    note: str | None = None,
    actor: str | None = None,
    pointer_name: str = LATEST_POINTER_NAME,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    normalized_decision = str(decision or "").strip().lower()
    if normalized_decision not in {"approved", "hold", "rejected"}:
        raise ValueError("invalid_promotion_decision")
    payload = get_internal_state_eval_promotion_review(pointer_name=pointer_name)
    if payload.get("degraded"):
        return payload
    review = payload.get("review")
    publish = payload.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not review or not publish_id:
        raise RuntimeError("promotion_review_not_ready")
    decision_row = {
        "decision_id": f"{publish_id}:{normalized_decision}:{_utcnow().strftime('%Y%m%dT%H%M%S%fZ')}",
        "publish_id": publish_id,
        "as_of_date": str(review.get("as_of_date") or payload.get("as_of_date") or ""),
        "champion_version": review.get("champion_version"),
        "challenger_version": review.get("challenger_version"),
        "decision": normalized_decision,
        "note": None if note is None or not str(note).strip() else str(note).strip(),
        "actor": None if actor is None or not str(actor).strip() else str(actor).strip(),
        "summary_json": json.dumps(
            {
                "readiness_pass": bool(review.get("readiness_pass")),
                "sample_count": int(review.get("sample_count") or 0),
                "expectancy_delta": review.get("expectancy_delta"),
                "reason_codes": list(review.get("reason_codes") or []),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "created_at": _utcnow(),
    }
    persist_promotion_decision(decision_row=decision_row, ops_db_path=ops_db_path)
    if ops_db_path is None:
        return get_internal_state_eval_promotion_review(pointer_name=pointer_name)
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        decision_readback = conn.execute(
            """
            SELECT decision_id, decision, note, actor, CAST(created_at AS VARCHAR), summary_json
            FROM external_promotion_decisions
            WHERE publish_id = ?
            ORDER BY created_at DESC, decision_id DESC
            LIMIT 1
            """,
            [publish_id],
        ).fetchone()
    finally:
        conn.close()
    review["approval_decision"] = None if not decision_readback else {
        "decision_id": str(decision_readback[0]),
        "decision": str(decision_readback[1]),
        "note": None if decision_readback[2] is None else str(decision_readback[2]),
        "actor": None if decision_readback[3] is None else str(decision_readback[3]),
        "created_at": str(decision_readback[4]),
        "summary": json.loads(str(decision_readback[5] or "{}")),
    }
    payload.update({"review": review})
    return payload


def get_similar_cases_rows(pointer_name: str = LATEST_POINTER_NAME, *, code: str, limit: int = 10) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "rows": [], "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    effective_limit = max(1, min(int(limit), MAX_PUBLIC_SIMILAR_CASE_ROWS))
    conn = _connect_read_only()
    try:
        if not _table_exists(conn, "similar_cases_daily"):
            snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
            return snapshot
        rows = conn.execute(
            """
            SELECT publish_id, as_of_date, code, query_type, query_anchor_type, neighbor_rank, case_id,
                   neighbor_code, neighbor_anchor_date, case_type, outcome_class, success_flag, similarity_score, reason_codes
            FROM similar_cases_daily
            WHERE publish_id = ? AND code = ?
            ORDER BY neighbor_rank ASC, case_id ASC
            LIMIT ?
            """,
            [publish_id, str(code), effective_limit],
        ).fetchall()
    finally:
        conn.close()
    columns = allowed_public_columns("similar_cases_daily")
    snapshot.update({"rows": [dict(zip(columns, row, strict=True)) for row in rows], **_public_payload_metadata(snapshot)})
    return snapshot


def get_similar_case_paths_rows(pointer_name: str = LATEST_POINTER_NAME, *, code: str, case_id: str) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "rows": [], "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    conn = _connect_read_only()
    try:
        if not _table_exists(conn, "similar_case_paths"):
            snapshot.update({"rows": [], **_public_payload_metadata(snapshot)})
            return snapshot
        rows = conn.execute(
            """
            SELECT publish_id, as_of_date, code, case_id, rel_day, path_return_norm, path_volume_norm
            FROM similar_case_paths
            WHERE publish_id = ? AND code = ? AND case_id = ?
            ORDER BY rel_day ASC
            LIMIT ?
            """,
            [publish_id, str(code), str(case_id), MAX_PUBLIC_SIMILAR_PATH_ROWS],
        ).fetchall()
    finally:
        conn.close()
    columns = allowed_public_columns("similar_case_paths")
    snapshot.update({"rows": [dict(zip(columns, row, strict=True)) for row in rows], **_public_payload_metadata(snapshot)})
    return snapshot
