from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb

from app.backend.services.analysis_bridge.contracts import (
    DEGRADE_REASON_NO_PUBLISH,
    LATEST_POINTER_NAME,
)
from app.backend.services.analysis_bridge.degrade import build_degrade_payload
from app.backend.services.analysis_bridge.reader import (
    CANDLE_COMBO_RESEARCH_TAGS,
    CANDLE_RESEARCH_TAGS,
    _parse_timestamp,
    _public_payload_metadata,
    _research_prior_summary_payload,
    _utcnow,
    get_analysis_bridge_snapshot,
)
from app.core.config import config as core_config
from external_analysis.contracts.paths import (
    resolve_result_db_path as external_resolve_result_db_path,
    resolve_ops_db_path as external_resolve_ops_db_path,
    resolve_source_db_path as external_resolve_source_db_path,
)
from external_analysis.exporter.source_reader import normalize_market_date
from external_analysis.ops.ops_schema import connect_ops_db as external_connect_ops_db, ensure_ops_schema
from external_analysis.ops.store import persist_promotion_decision
from external_analysis.results.result_schema import connect_result_db


def _legacy_ops_db_path() -> Path:
    return Path(str(core_config.DATA_DIR)).expanduser().resolve() / "external_analysis" / "ops.duckdb"


def _legacy_source_db_path() -> Path:
    return Path(str(core_config.DATA_DIR)).expanduser().resolve() / "stocks.duckdb"


def resolve_ops_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return external_resolve_ops_db_path(db_path)
    legacy_path = _legacy_ops_db_path()
    if legacy_path.exists():
        return legacy_path
    return external_resolve_ops_db_path()


def resolve_source_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return external_resolve_source_db_path(db_path)
    legacy_path = _legacy_source_db_path()
    if legacy_path.exists():
        return legacy_path
    return external_resolve_source_db_path()


def resolve_result_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return external_resolve_result_db_path(db_path)
    return external_resolve_result_db_path()


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = 'main' AND lower(table_name) = lower(?)
        LIMIT 1
        """,
        [str(table_name)],
    ).fetchone()
    return bool(row)


def _json_load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed if parsed is not None else default


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def connect_ops_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved = resolve_ops_db_path(db_path)
    return external_connect_ops_db(str(resolved))


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
                research_summary = _research_prior_summary_payload()
                if isinstance(research_summary, dict):
                    daily_summary["decision_signal"] = research_summary
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
    research_summary = _research_prior_summary_payload()
    if isinstance(research_summary, dict):
        daily_summary["decision_signal"] = research_summary
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
    resolved_ops_db_path = str(resolve_ops_db_path(ops_db_path))
    persist_promotion_decision(decision_row=decision_row, ops_db_path=resolved_ops_db_path)
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


def get_internal_forecast_surface_review(pointer_name: str = LATEST_POINTER_NAME) -> dict[str, Any]:
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
    conn = connect_result_db(str(resolve_result_db_path()))
    try:
        if not _table_exists(conn, "forecast_surface_evaluation_runs"):
            snapshot.update({"review": None, **_public_payload_metadata(snapshot)})
            return snapshot
        recent_rows = conn.execute(
            """
            SELECT CAST(as_of_date AS VARCHAR), readiness_pass, gate_reason
            FROM forecast_surface_evaluation_runs
            WHERE scope_type = 'publish'
            ORDER BY as_of_date DESC NULLS LAST, created_at DESC, run_id DESC
            LIMIT 20
            """
        ).fetchall()
        run_row = conn.execute(
            """
            SELECT
                run_id, scope_type, publish_id, CAST(as_of_date AS VARCHAR), model_version, top_k, fold_count, daily_count,
                horizon_count, top_long_mean_ret20_net, top_short_mean_ret20_net, top_combined_mean_ret20_net,
                candidate_long_mean_ret20_net, candidate_short_mean_ret20_net, candidate_combined_mean_ret20_net,
                signal_long_mean_ret20_net, signal_short_mean_ret20_net, direction_brier_long, direction_brier_short,
                calibration_gap_long, calibration_gap_short, top_k_uplift, worst_regime_combined_mean_ret20_net,
                primary_gate_reason, gate_failures_json, calibration_method_long, calibration_method_short,
                ready_streak, recent_ready_count_20, regime_breakdown_json, fold_metrics_json,
                readiness_pass, gate_reason, CAST(created_at AS VARCHAR)
            FROM forecast_surface_evaluation_runs
            WHERE publish_id = ? AND scope_type = 'publish'
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """,
            [publish_id],
        ).fetchone()
        if not run_row:
            fallback_row = conn.execute(
                """
                SELECT
                    run_id, scope_type, publish_id, CAST(as_of_date AS VARCHAR), model_version, top_k, fold_count, daily_count,
                    horizon_count, top_long_mean_ret20_net, top_short_mean_ret20_net, top_combined_mean_ret20_net,
                    candidate_long_mean_ret20_net, candidate_short_mean_ret20_net, candidate_combined_mean_ret20_net,
                    signal_long_mean_ret20_net, signal_short_mean_ret20_net, direction_brier_long, direction_brier_short,
                    calibration_gap_long, calibration_gap_short, top_k_uplift, worst_regime_combined_mean_ret20_net,
                    primary_gate_reason, gate_failures_json, calibration_method_long, calibration_method_short,
                    ready_streak, recent_ready_count_20, regime_breakdown_json, fold_metrics_json,
                    readiness_pass, gate_reason, CAST(created_at AS VARCHAR)
                FROM forecast_surface_evaluation_runs
                WHERE scope_type = 'walk_forward'
                ORDER BY created_at DESC, run_id DESC
                LIMIT 1
                """,
            ).fetchone()
            run_row = fallback_row
        if not run_row:
            snapshot.update({"review": None, **_public_payload_metadata(snapshot)})
            return snapshot
        fold_rows = conn.execute(
            """
            SELECT
                run_id, scope_type, publish_id, CAST(as_of_date AS VARCHAR), regime_tag, side, horizon_days, top_k,
                sample_count, top_mean_ret_net, top_mean_mfe_net, top_mean_mae_net, top_win_rate, top_brier,
                top_calibration_gap, candidate_mean_ret_net, candidate_win_rate, signal_mean_ret_net, signal_sample_count,
                CAST(created_at AS VARCHAR)
            FROM forecast_surface_evaluation_folds
            WHERE run_id = ?
            ORDER BY CAST(as_of_date AS VARCHAR) ASC, side ASC, horizon_days ASC
            """,
            [run_row[0]],
        ).fetchall()
    finally:
        conn.close()
    regime_breakdown = _json_load(run_row[29], {})
    fold_summary = _json_load(run_row[30], [])
    ready_streak = int(run_row[27] or 0)
    fail_streak = 0
    recent_ready_count = int(run_row[28] or 0)
    on_fail_prefix = True
    for row in recent_rows:
        is_ready = bool(row[1])
        if on_fail_prefix and not is_ready:
            fail_streak += 1
        else:
            on_fail_prefix = False
    alerts: list[str] = []
    gate_failures = _json_load(run_row[24], [])
    if not bool(run_row[31]):
        alerts.append(f"gate_fail:{str(run_row[23] or run_row[32])}")
    if fail_streak >= 3:
        alerts.append("gate_fail_streak>=3")
    if _safe_float(run_row[17]) is not None and float(run_row[17]) > 0.25:
        alerts.append("long_calibration_degraded")
    if _safe_float(run_row[18]) is not None and float(run_row[18]) > 0.25:
        alerts.append("short_calibration_degraded")
    if _safe_float(run_row[19]) is not None and float(run_row[19]) > 0.10:
        alerts.append("long_calibration_gap_degraded")
    if _safe_float(run_row[20]) is not None and float(run_row[20]) > 0.10:
        alerts.append("short_calibration_gap_degraded")
    review = {
        "run_id": str(run_row[0]),
        "scope_type": str(run_row[1]),
        "publish_id": publish_id if run_row[2] is None else str(run_row[2]),
        "as_of_date": str(run_row[3]) if run_row[3] is not None else None,
        "model_version": str(run_row[4]),
        "top_k": int(run_row[5]),
        "fold_count": int(run_row[6]),
        "daily_count": int(run_row[7]),
        "horizon_count": int(run_row[8]),
        "top_long_mean_ret20_net": run_row[9],
        "top_short_mean_ret20_net": run_row[10],
        "top_combined_mean_ret20_net": run_row[11],
        "candidate_long_mean_ret20_net": run_row[12],
        "candidate_short_mean_ret20_net": run_row[13],
        "candidate_combined_mean_ret20_net": run_row[14],
        "signal_long_mean_ret20_net": run_row[15],
        "signal_short_mean_ret20_net": run_row[16],
        "direction_brier_long": run_row[17],
        "direction_brier_short": run_row[18],
        "calibration_gap_long": run_row[19],
        "calibration_gap_short": run_row[20],
        "top_k_uplift": run_row[21],
        "worst_regime_combined_mean_ret20_net": run_row[22],
        "primary_gate_reason": str(run_row[23] or run_row[32]),
        "gate_failures": gate_failures if isinstance(gate_failures, list) else [],
        "calibration_method_long": None if run_row[25] is None else str(run_row[25]),
        "calibration_method_short": None if run_row[26] is None else str(run_row[26]),
        "regime_breakdown": regime_breakdown if isinstance(regime_breakdown, dict) else {},
        "fold_metrics": fold_summary,
        "readiness_pass": bool(run_row[31]),
        "reason_codes": gate_failures if isinstance(gate_failures, list) else ([] if not str(run_row[32] or "").strip() else str(run_row[32]).split(",")),
        "gate_reason": str(run_row[23] or run_row[32]),
        "summary": {
            "top_k_uplift": run_row[21],
            "ready_streak": ready_streak,
            "fail_streak": fail_streak,
            "recent_ready_count_20": recent_ready_count,
        },
        "ready_streak": ready_streak,
        "fail_streak": fail_streak,
        "recent_ready_count_20": recent_ready_count,
        "alerts": alerts,
        "created_at": str(run_row[33]),
    }
    snapshot.update({"review": review, **_public_payload_metadata(snapshot)})
    return snapshot


def get_internal_forecast_surface_projection(
    pointer_name: str = LATEST_POINTER_NAME,
    *,
    limit_per_side: int = 20,
) -> dict[str, Any]:
    snapshot = get_analysis_bridge_snapshot(pointer_name=pointer_name)
    if snapshot.get("degraded"):
        snapshot.update({"projection": None, **_public_payload_metadata(snapshot)})
        return snapshot
    publish = snapshot.get("publish") or {}
    publish_id = str(publish.get("publish_id") or "")
    if not publish_id:
        degraded = build_degrade_payload(DEGRADE_REASON_NO_PUBLISH)
        degraded.update({"publish": None, "projection": None, "publish_id": None, "as_of_date": None, "freshness_state": None})
        return degraded
    conn = connect_result_db(str(resolve_result_db_path()))
    try:
        if not _table_exists(conn, "forecast_surface_daily"):
            snapshot.update({"projection": None, **_public_payload_metadata(snapshot)})
            return snapshot
        run_row = None
        if _table_exists(conn, "forecast_surface_runs"):
            run_row = conn.execute(
                """
                SELECT
                    CAST(as_of_date AS VARCHAR),
                    model_version,
                    universe_code_count,
                    expected_row_count,
                    actual_row_count,
                    missing_row_count,
                    coverage_ratio,
                    feature_frame_version,
                    market_opportunity_score_enabled,
                    personal_fit_score_enabled,
                    side_counts_json,
                    action_counts_json,
                    source_context_presence_json,
                    alerts_json,
                    CAST(created_at AS VARCHAR)
                FROM forecast_surface_runs
                WHERE publish_id = ?
                """,
                [publish_id],
            ).fetchone()
        rows = conn.execute(
            """
            SELECT
                CAST(as_of_date AS VARCHAR),
                code,
                side,
                action_state,
                direction_prob,
                expected_ret_20,
                expected_mfe_20,
                expected_mae_20,
                invalidation_price,
                setup_tags,
                reason_codes,
                opportunity_score,
                freshness_state
            FROM forecast_surface_daily
            WHERE publish_id = ?
            ORDER BY opportunity_score DESC, direction_prob DESC, code ASC, side ASC
            """,
            [publish_id],
        ).fetchall()
    finally:
        conn.close()
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        side = str(row[2] or "")
        expected_mfe = _safe_float(row[6], 0.0) or 0.0
        expected_mae = _safe_float(row[7], 0.0) or 0.0
        if side == "long":
            expected_upside = max(expected_mfe, 0.0)
            expected_downside = max(-expected_mae, 0.0)
        else:
            expected_upside = max(expected_mae, 0.0)
            expected_downside = max(-expected_mfe, 0.0)
        normalized_rows.append(
            {
                "as_of_date": str(row[0]) if row[0] is not None else None,
                "code": str(row[1]),
                "side": side,
                "action_state": str(row[3]),
                "direction_prob": _safe_float(row[4], 0.0),
                "expected_ret_20": _safe_float(row[5], 0.0),
                "expected_upside": float(expected_upside),
                "expected_downside": float(expected_downside),
                "invalidation_price": _safe_float(row[8]),
                "setup_tags": _json_load(row[9], []),
                "reason_codes": _json_load(row[10], []),
                "opportunity_score": _safe_float(row[11], 0.0),
                "freshness_state": str(row[12] or ""),
            }
        )
    effective_limit = max(1, min(int(limit_per_side), 50))
    long_rank = [row for row in normalized_rows if row["side"] == "long" and row["action_state"] in {"enter", "wait"}][:effective_limit]
    short_rank = [row for row in normalized_rows if row["side"] == "short" and row["action_state"] in {"enter", "wait"}][:effective_limit]
    high_risk_avoid = sorted(
        normalized_rows,
        key=lambda row: (
            float(row.get("expected_downside") or 0.0),
            -float(row.get("direction_prob") or 0.0),
            -float(row.get("opportunity_score") or 0.0),
        ),
        reverse=True,
    )[:effective_limit]
    watchlist_promotions = [row for row in normalized_rows if row["action_state"] == "enter"][:effective_limit]
    if run_row:
        summary = {
            "as_of_date": str(run_row[0]) if run_row[0] is not None else None,
            "model_version": str(run_row[1]),
            "universe_code_count": int(run_row[2]),
            "expected_row_count": int(run_row[3]),
            "actual_row_count": int(run_row[4]),
            "missing_row_count": int(run_row[5]),
            "coverage_ratio": float(run_row[6]),
            "feature_frame_version": None if run_row[7] is None else str(run_row[7]),
            "market_opportunity_score_enabled": bool(run_row[8]),
            "personal_fit_score_enabled": bool(run_row[9]),
            "side_counts": _json_load(run_row[10], {}),
            "action_counts": _json_load(run_row[11], {}),
            "source_context_presence": _json_load(run_row[12], {}),
            "alerts": _json_load(run_row[13], []),
            "created_at": str(run_row[14]) if run_row[14] is not None else None,
        }
    else:
        code_count = len({str(row["code"]) for row in normalized_rows})
        expected_row_count = int(code_count * 2)
        actual_row_count = int(len(normalized_rows))
        summary = {
            "as_of_date": str(normalized_rows[0]["as_of_date"]) if normalized_rows else None,
            "model_version": None,
            "universe_code_count": code_count,
            "expected_row_count": expected_row_count,
            "actual_row_count": actual_row_count,
            "missing_row_count": max(expected_row_count - actual_row_count, 0),
            "coverage_ratio": float(actual_row_count / max(expected_row_count, 1)),
            "side_counts": {
                "long": sum(1 for row in normalized_rows if row["side"] == "long"),
                "short": sum(1 for row in normalized_rows if row["side"] == "short"),
            },
            "action_counts": {
                "enter": sum(1 for row in normalized_rows if row["action_state"] == "enter"),
                "wait": sum(1 for row in normalized_rows if row["action_state"] == "wait"),
                "skip": sum(1 for row in normalized_rows if row["action_state"] == "skip"),
            },
            "source_context_presence": {},
            "alerts": [],
            "created_at": None,
        }
    projection = {
        "summary": summary,
        "long_rank": long_rank,
        "short_rank": short_rank,
        "high_risk_avoid": high_risk_avoid,
        "watchlist_promotions": watchlist_promotions,
    }
    snapshot.update({"projection": projection, **_public_payload_metadata(snapshot)})
    return snapshot



