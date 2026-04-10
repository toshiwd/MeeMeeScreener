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
from external_analysis.contracts.paths import resolve_result_db_path
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


def _research_side_from_public_side(side: Any) -> str | None:
    side_key = str(side or "").strip().lower()
    if side_key in {"long", "up"}:
        return "up"
    if side_key in {"short", "down"}:
        return "down"
    return None


def _enrich_rows_with_research_prior(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    try:
        from app.backend.services.ml import rankings_cache
    except Exception:
        return rows

    snapshot = rankings_cache._load_research_prior_snapshot()
    enriched: list[dict[str, Any]] = []
    for base in rows:
        row = dict(base)
        side = _research_side_from_public_side(row.get("side"))
        code = str(row.get("code") or "").strip()
        if not side or not code:
            enriched.append(row)
            continue
        probe: dict[str, Any] = {}
        rankings_cache._calc_research_prior_bonus(
            item=probe,
            direction=side,  # type: ignore[arg-type]
            code=code,
            prior_snapshot=snapshot,
        )
        row["signalStrength"] = probe.get("researchSignalStrength")
        row["promotionStage"] = probe.get("researchPromotionStage")
        row["decisionReasons"] = probe.get("researchDecisionReasons")
        row["riskWatch"] = probe.get("researchRiskWatch")
        row["provisional"] = bool(probe.get("researchProvisional")) if probe.get("researchPriorAligned") else False
        row["hypothesisFamily"] = probe.get("researchHypothesisFamily")
        row["researchPriorBonus"] = probe.get("researchPriorBonus")
        enriched.append(row)
    return enriched


def _research_prior_summary_payload() -> dict[str, Any] | None:
    try:
        from app.backend.services.ml import rankings_cache
    except Exception:
        return None
    snapshot = rankings_cache._load_research_prior_snapshot()
    if not isinstance(snapshot, dict):
        return None
    summary = snapshot.get("summary")
    if not isinstance(summary, dict):
        return None
    return {
        "family_leaderboard": summary.get("family_leaderboard") if isinstance(summary.get("family_leaderboard"), list) else [],
        "worst_failure_patterns": summary.get("worst_failure_patterns") if isinstance(summary.get("worst_failure_patterns"), list) else [],
        "next_promotion_candidates": summary.get("next_promotion_candidates") if isinstance(summary.get("next_promotion_candidates"), list) else [],
        "provisional_deterioration": summary.get("provisional_deterioration") if isinstance(summary.get("provisional_deterioration"), list) else [],
        "action_queue": summary.get("action_queue") if isinstance(summary.get("action_queue"), list) else [],
    }


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
    ordered_rows = _enrich_rows_with_research_prior(by_side["long"] + by_side["short"])
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
            SELECT publish_id, as_of_date, code, side, holding_band, strategy_tags, state_action, decision_3way, confidence,
                   machine_action_state, human_readable_judgement, buy_score, environment_score, trend_score, trigger_score, risk_score,
                   invalidation_price, invalidation_reason_code, reason_codes, reason_text_top3, freshness_state
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
    out_rows = [dict(zip(columns, row, strict=True)) for row in rows]
    snapshot.update({"rows": _enrich_rows_with_research_prior(out_rows), **_public_payload_metadata(snapshot)})
    return snapshot


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

