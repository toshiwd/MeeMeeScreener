from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb

from external_analysis.exporter.export_schema import connect_export_db, ensure_export_schema
from external_analysis.exporter.source_reader import normalize_market_date
from external_analysis.labels.rolling_labels import build_rolling_labels
from external_analysis.models.candidate_baseline import run_candidate_baseline
from external_analysis.ops.store import (
    insert_quarantine_record,
    persist_replay_summary,
    upsert_replay_day,
    upsert_replay_run,
    upsert_work_item,
)
from external_analysis.runtime.source_snapshot import create_source_snapshot
from external_analysis.similarity.baseline import (
    CHALLENGER_EMBEDDING_VERSION,
    EMBEDDING_VERSION,
    build_case_library,
    run_similarity_baseline,
)
from external_analysis.similarity.store import connect_similarity_db, ensure_similarity_schema

JOB_TYPE = "historical_replay_runner"
MAX_ATTEMPTS = 3
ROLLING_WINDOWS = (20, 40, 60)
logger = logging.getLogger(__name__)


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


def _replay_publish_id(replay_id: str, as_of_date: int) -> str:
    return f"replay_{replay_id}_{_as_of_date_text(as_of_date)}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _merge_details(*payloads: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for payload in payloads:
        if payload:
            merged.update(payload)
    return merged


def _heartbeat_details(*, current_phase: str, as_of_date: int, publish_id: str, attempt: int) -> dict[str, Any]:
    return {
        "heartbeat_at": _utcnow().isoformat(),
        "current_phase": current_phase,
        "current_as_of_date": _as_of_date_text(as_of_date),
        "current_publish_id": publish_id,
        "current_attempt": int(attempt),
    }


def _bootstrap_market_date_sql(column_name: str) -> str:
    return (
        f"CASE WHEN {column_name} >= 100000000 "
        f"THEN CAST(strftime(to_timestamp(CAST({column_name} AS BIGINT)), '%Y%m%d') AS INTEGER) "
        f"ELSE CAST({column_name} AS INTEGER) END"
    )


def _attached_table_exists(conn: duckdb.DuckDBPyConnection, *, database: str, table_name: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {database}.{table_name} LIMIT 1").fetchone()
        return True
    except duckdb.Error:
        return False


def _run_replay_bootstrap_export(
    *,
    source_db_path: str,
    export_db_path: str,
    on_phase: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    run_id = _utcnow().strftime("replay_export_%Y%m%dT%H%M%S%fZ")
    started_at = _utcnow()
    schema_conn = connect_export_db(export_db_path)
    try:
        ensure_export_schema(schema_conn)
    finally:
        schema_conn.close()

    work_conn = duckdb.connect()
    source_alias = "replay_src"
    export_alias = "replay_export"
    source_path_sql = str(source_db_path).replace("'", "''")
    export_path_sql = str(export_db_path).replace("'", "''")
    work_conn.execute(f"ATTACH '{source_path_sql}' AS {source_alias} (READ_ONLY)")
    work_conn.execute(f"ATTACH '{export_path_sql}' AS {export_alias}")
    try:
        work_conn.execute("BEGIN TRANSACTION")
        try:
            if on_phase is not None:
                on_phase("bootstrap_export_prepare")
            work_conn.execute(f"DELETE FROM {export_alias}.bars_daily_export")
            work_conn.execute(f"DELETE FROM {export_alias}.indicator_daily_export")
            work_conn.execute(f"DELETE FROM {export_alias}.pattern_state_export")
            work_conn.execute(f"DELETE FROM {export_alias}.trade_event_export")
            work_conn.execute(f"DELETE FROM {export_alias}.position_snapshot_export")
            work_conn.execute(f"DELETE FROM {export_alias}.bars_monthly_export")

            if on_phase is not None:
                on_phase("bootstrap_export_bars")
            work_conn.execute(
                f"""
                INSERT INTO {export_alias}.bars_daily_export (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                SELECT
                    code,
                    {_bootstrap_market_date_sql('date')} AS trade_date,
                    o, h, l, c, v,
                    COALESCE(source, 'unknown'),
                    code || ':' || CAST({_bootstrap_market_date_sql('date')} AS VARCHAR),
                    ?
                FROM {source_alias}.daily_bars
                """,
                [run_id],
            )

            has_daily_ma = _attached_table_exists(work_conn, database=source_alias, table_name="daily_ma")
            has_feature_snapshot = _attached_table_exists(work_conn, database=source_alias, table_name="feature_snapshot_daily")
            if has_daily_ma:
                if on_phase is not None:
                    on_phase("bootstrap_export_indicators")
                work_conn.execute(
                    f"""
                    INSERT INTO {export_alias}.indicator_daily_export (
                        code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr,
                        cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id
                    )
                    WITH ma_rows AS (
                        SELECT
                            code,
                            {_bootstrap_market_date_sql('date')} AS trade_date,
                            ma7,
                            ma20,
                            ma60
                        FROM {source_alias}.daily_ma
                    ),
                    feature_rows AS (
                        SELECT
                            code,
                            {_bootstrap_market_date_sql('dt')} AS trade_date,
                            atr14,
                            diff20_pct,
                            diff20_atr,
                            cnt_20_above,
                            cnt_7_above,
                            day_count,
                            candle_flags
                        FROM {source_alias}.feature_snapshot_daily
                    )
                    SELECT
                        COALESCE(ma_rows.code, feature_rows.code) AS code,
                        COALESCE(ma_rows.trade_date, feature_rows.trade_date) AS trade_date,
                        ma_rows.ma7,
                        ma_rows.ma20,
                        ma_rows.ma60,
                        NULL AS ma100,
                        NULL AS ma200,
                        feature_rows.atr14,
                        feature_rows.diff20_pct,
                        feature_rows.diff20_atr,
                        feature_rows.cnt_20_above,
                        feature_rows.cnt_7_above,
                        feature_rows.day_count,
                        feature_rows.candle_flags,
                        COALESCE(ma_rows.code, feature_rows.code) || ':' || CAST(COALESCE(ma_rows.trade_date, feature_rows.trade_date) AS VARCHAR),
                        ?
                    FROM ma_rows
                    FULL OUTER JOIN feature_rows
                      ON ma_rows.code = feature_rows.code AND ma_rows.trade_date = feature_rows.trade_date
                    """,
                    [run_id],
                )
            elif has_feature_snapshot:
                if on_phase is not None:
                    on_phase("bootstrap_export_indicators")
                work_conn.execute(
                    f"""
                    INSERT INTO {export_alias}.indicator_daily_export (
                        code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr,
                        cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id
                    )
                    SELECT
                        code,
                        {_bootstrap_market_date_sql('dt')} AS trade_date,
                        NULL, NULL, NULL, NULL, NULL,
                        atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags,
                        code || ':' || CAST({_bootstrap_market_date_sql('dt')} AS VARCHAR),
                        ?
                    FROM {source_alias}.feature_snapshot_daily
                    """,
                    [run_id],
                )

            if has_feature_snapshot:
                if on_phase is not None:
                    on_phase("bootstrap_export_patterns")
                work_conn.execute(
                    f"""
                    INSERT INTO {export_alias}.pattern_state_export (
                        code, trade_date, ppp_state, abc_state, box_state, box_upper, box_lower,
                        ranking_state, event_flags, row_hash, export_run_id
                    )
                    SELECT
                        code,
                        {_bootstrap_market_date_sql('dt')} AS trade_date,
                        NULL, NULL, NULL, NULL, NULL, NULL,
                        candle_flags,
                        code || ':' || CAST({_bootstrap_market_date_sql('dt')} AS VARCHAR),
                        ?
                    FROM {source_alias}.feature_snapshot_daily
                    """,
                    [run_id],
                )

            if on_phase is not None:
                on_phase("bootstrap_export_commit")
            work_conn.execute("COMMIT")
        except Exception:
            work_conn.execute("ROLLBACK")
            raise
    finally:
        work_conn.execute(f"DETACH {export_alias}")
        work_conn.execute(f"DETACH {source_alias}")
        work_conn.close()

    export_conn = connect_export_db(export_db_path)
    try:
        source_row_counts = {
            "bars_daily_export": int(export_conn.execute("SELECT COUNT(*) FROM bars_daily_export").fetchone()[0]),
            "indicator_daily_export": int(export_conn.execute("SELECT COUNT(*) FROM indicator_daily_export").fetchone()[0]),
            "pattern_state_export": int(export_conn.execute("SELECT COUNT(*) FROM pattern_state_export").fetchone()[0]),
        }
        max_trade_row = export_conn.execute("SELECT MAX(trade_date) FROM bars_daily_export").fetchone()
        source_max_trade_date = int(max_trade_row[0]) if max_trade_row and max_trade_row[0] is not None else None
        signature = json.dumps(source_row_counts, ensure_ascii=False, sort_keys=True)
        export_conn.execute(
            """
            INSERT OR REPLACE INTO meta_export_runs (
                run_id, started_at, finished_at, status, source_db_path, source_signature,
                source_max_trade_date, source_row_counts, changed_table_names, diff_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                started_at,
                _utcnow(),
                "success",
                str(source_db_path),
                signature,
                source_max_trade_date,
                json.dumps(source_row_counts, ensure_ascii=False, sort_keys=True),
                json.dumps(["bars_daily_export", "indicator_daily_export", "pattern_state_export"], ensure_ascii=False),
                json.dumps({"mode": "replay_bootstrap_fast"}, ensure_ascii=False, sort_keys=True),
            ],
        )
        export_conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "run_id": run_id,
            "source_db_path": str(source_db_path),
            "source_signature": signature,
            "source_max_trade_date": source_max_trade_date,
            "changed_table_names": ["bars_daily_export", "indicator_daily_export", "pattern_state_export"],
            "diff_reason": {"mode": "replay_bootstrap_fast"},
        }
    finally:
        export_conn.close()


def _run_replay_bootstrap(
    *,
    source_db_path: str,
    export_db_path: str,
    label_db_path: str,
    on_phase: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if on_phase is not None:
        on_phase("bootstrap_export")
    export_payload = _run_replay_bootstrap_export(
        source_db_path=source_db_path,
        export_db_path=export_db_path,
        on_phase=on_phase,
    )
    if on_phase is not None:
        on_phase("bootstrap_labels_h20")
    label_payload = build_rolling_labels(export_db_path=export_db_path, label_db_path=label_db_path, horizons=(20,))
    if on_phase is not None:
        on_phase("bootstrap_labels_completed")
    return {
        "export": export_payload,
        "labels": label_payload,
        "anchors": {
            "ok": True,
            "run_id": None,
            "summary": {},
            "policy_version": "deferred_for_replay_bootstrap_v1",
            "skipped": True,
            "reason": "deferred_for_replay_bootstrap",
        },
    }


def _select_replay_dates(
    *,
    source_db_path: str,
    start_as_of_date: str,
    end_as_of_date: str,
    max_days: int | None = None,
) -> list[int]:
    conn = duckdb.connect(source_db_path, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM daily_bars
            ORDER BY date
            """,
        ).fetchall()
    finally:
        conn.close()
    start_value = _normalize_as_of_date(start_as_of_date)
    end_value = _normalize_as_of_date(end_as_of_date)
    dates = sorted(
        {
            int(normalized)
            for normalized in (normalize_market_date(row[0]) for row in rows)
            if normalized is not None and start_value <= int(normalized) <= end_value
        }
    )
    if max_days is not None:
        dates = dates[: int(max_days)]
    return dates


def _select_codes(
    *,
    source_db_path: str,
    codes: list[str] | None,
    max_codes: int | None,
) -> list[str] | None:
    if codes:
        selected = [str(code) for code in codes]
    elif max_codes is not None:
        conn = duckdb.connect(source_db_path, read_only=True)
        try:
            rows = conn.execute(
                "SELECT DISTINCT code FROM daily_bars ORDER BY code LIMIT ?",
                [int(max_codes)],
            ).fetchall()
        finally:
            conn.close()
        selected = [str(row[0]) for row in rows]
    else:
        selected = []
    return selected or None


def _get_day_status(*, replay_id: str, as_of_date: int, ops_db_path: str | None) -> str | None:
    conn = duckdb.connect(str(ops_db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT status
            FROM external_replay_days
            WHERE replay_id = ? AND as_of_date = CAST(? AS DATE)
            """,
            [replay_id, _as_of_date_text(as_of_date)],
        ).fetchone()
    finally:
        conn.close()
    return None if not row else str(row[0])


def _load_replay_days(*, replay_id: str, ops_db_path: str) -> list[dict[str, Any]]:
    conn = duckdb.connect(ops_db_path, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT CAST(as_of_date AS VARCHAR), status, publish_id, details_json
            FROM external_replay_days
            WHERE replay_id = ?
            ORDER BY as_of_date
            """,
            [replay_id],
        ).fetchall()
    finally:
        conn.close()
    values: list[dict[str, Any]] = []
    for as_of_date, status, publish_id, details_json in rows:
        payload = {}
        if details_json:
            if isinstance(details_json, str):
                payload = json.loads(details_json)
            else:
                payload = details_json
        values.append(
            {
                "as_of_date": _normalize_as_of_date(str(as_of_date)),
                "status": str(status),
                "publish_id": None if publish_id is None else str(publish_id),
                "details": payload,
            }
        )
    return values


def _candidate_metric_rows(*, result_db_path: str, publish_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not publish_ids:
        return {}
    conn = duckdb.connect(result_db_path, read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT publish_id, recall_at_20, recall_at_10, monthly_top5_capture, avg_ret_20_top20
            FROM nightly_candidate_metrics
            WHERE publish_id IN ({', '.join(['?'] * len(publish_ids))})
            """,
            publish_ids,
        ).fetchall()
    finally:
        conn.close()
    return {
        str(row[0]): {
            "recall_at_20": None if row[1] is None else float(row[1]),
            "recall_at_10": None if row[2] is None else float(row[2]),
            "monthly_top5_capture": None if row[3] is None else float(row[3]),
            "avg_ret_20_top20": None if row[4] is None else float(row[4]),
        }
        for row in rows
    }


def _similarity_metric_rows(*, similarity_db_path: str, publish_ids: list[str]) -> dict[tuple[str, str], dict[str, Any]]:
    if not publish_ids:
        return {}
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        rows = conn.execute(
            f"""
            SELECT publish_id, engine_role, baseline_version, embedding_version, comparison_target_version,
                   overlap_at_k, success_hit_rate_at_k, failure_hit_rate_at_k, big_drop_hit_rate_at_k, avg_similarity_score,
                   case_count, query_count, returned_case_count
            FROM similarity_quality_metrics
            WHERE publish_id IN ({', '.join(['?'] * len(publish_ids))})
            """,
            publish_ids,
        ).fetchall()
    finally:
        conn.close()
    return {
        (str(row[0]), str(row[1])): {
            "baseline_version": str(row[2]),
            "embedding_version": str(row[3]),
            "comparison_target_version": str(row[4]),
            "overlap_at_k": None if row[5] is None else float(row[5]),
            "success_hit_rate_at_k": None if row[6] is None else float(row[6]),
            "failure_hit_rate_at_k": None if row[7] is None else float(row[7]),
            "big_drop_hit_rate_at_k": None if row[8] is None else float(row[8]),
            "avg_similarity_score": None if row[9] is None else float(row[9]),
            "case_count": int(row[10]),
            "query_count": int(row[11]),
            "returned_case_count": int(row[12]),
        }
        for row in rows
    }


def _current_case_library_source_signature(*, similarity_db_path: str) -> str | None:
    conn = connect_similarity_db(similarity_db_path)
    try:
        ensure_similarity_schema(conn)
        row = conn.execute(
            """
            SELECT source_signature
            FROM similarity_generation_manifest
            WHERE generation_key = 'case_library'
            """
        ).fetchone()
    finally:
        conn.close()
    return None if row is None or row[0] is None else str(row[0])


def _mean(values: list[float | None]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    return None if not filtered else sum(filtered) / len(filtered)


def _build_replay_readiness_rows(
    *,
    replay_id: str,
    successful_days: list[dict[str, Any]],
    result_db_path: str,
    similarity_db_path: str,
) -> list[dict[str, Any]]:
    if not successful_days:
        return []
    publish_ids = [str(day["publish_id"]) for day in successful_days]
    candidate_by_publish = _candidate_metric_rows(result_db_path=result_db_path, publish_ids=publish_ids)
    similarity_by_publish = _similarity_metric_rows(similarity_db_path=similarity_db_path, publish_ids=publish_ids)
    rows: list[dict[str, Any]] = []
    for window_size in ROLLING_WINDOWS:
        window_days = successful_days[-window_size:]
        if not window_days:
            continue
        candidate_rows = [candidate_by_publish.get(str(day["publish_id"]), {}) for day in window_days]
        champion_rows = [similarity_by_publish.get((str(day["publish_id"]), "champion"), {}) for day in window_days]
        challenger_rows = [similarity_by_publish.get((str(day["publish_id"]), "challenger"), {}) for day in window_days]
        overlap_mean = _mean([row.get("overlap_at_k") for row in challenger_rows])
        challenger_success_mean = _mean([row.get("success_hit_rate_at_k") for row in challenger_rows])
        champion_success_mean = _mean([row.get("success_hit_rate_at_k") for row in champion_rows])
        challenger_big_drop_mean = _mean([row.get("big_drop_hit_rate_at_k") for row in challenger_rows])
        champion_big_drop_mean = _mean([row.get("big_drop_hit_rate_at_k") for row in champion_rows])
        readiness_pass = (
            len(window_days) >= window_size
            and overlap_mean is not None
            and overlap_mean >= 0.40
            and challenger_success_mean is not None
            and champion_success_mean is not None
            and challenger_success_mean >= champion_success_mean
            and challenger_big_drop_mean is not None
            and champion_big_drop_mean is not None
            and challenger_big_drop_mean <= (champion_big_drop_mean + 0.05)
        )
        rows.append(
            {
                "readiness_id": f"{replay_id}_w{window_size}",
                "replay_id": replay_id,
                "window_size": int(window_size),
                "start_as_of_date": _as_of_date_text(int(window_days[0]["as_of_date"])),
                "end_as_of_date": _as_of_date_text(int(window_days[-1]["as_of_date"])),
                "run_count": len(window_days),
                "champion_version": EMBEDDING_VERSION,
                "challenger_version": CHALLENGER_EMBEDDING_VERSION,
                "overlap_at_k_mean": overlap_mean,
                "success_hit_rate_at_k_mean": challenger_success_mean,
                "failure_hit_rate_at_k_mean": _mean([row.get("failure_hit_rate_at_k") for row in challenger_rows]),
                "big_drop_hit_rate_at_k_mean": challenger_big_drop_mean,
                "avg_similarity_score_mean": _mean([row.get("avg_similarity_score") for row in challenger_rows]),
                "recall_at_20_mean": _mean([row.get("recall_at_20") for row in candidate_rows]),
                "recall_at_10_mean": _mean([row.get("recall_at_10") for row in candidate_rows]),
                "monthly_top5_capture_mean": _mean([row.get("monthly_top5_capture") for row in candidate_rows]),
                "avg_ret_20_top20_mean": _mean([row.get("avg_ret_20_top20") for row in candidate_rows]),
                "readiness_pass": bool(readiness_pass),
                "summary_json": json.dumps(
                    _json_ready(
                        {
                            "window_size": window_size,
                            "run_count": len(window_days),
                            "champion_success_hit_rate_at_k_mean": champion_success_mean,
                            "champion_big_drop_hit_rate_at_k_mean": champion_big_drop_mean,
                        }
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "created_at": _utcnow(),
            }
        )
    return rows


def run_replay_core(
    *,
    source_db_path: str,
    export_db_path: str,
    label_db_path: str,
    result_db_path: str,
    similarity_db_path: str,
    ops_db_path: str,
    start_as_of_date: str,
    end_as_of_date: str,
    replay_id: str,
    codes: list[str] | None = None,
    max_days: int | None = None,
    max_codes: int | None = None,
    resume: bool = True,
    max_attempts: int = MAX_ATTEMPTS,
    snapshot_source: bool = True,
    snapshot_root: str | None = None,
) -> dict[str, Any]:
    started_at = _utcnow()
    snapshot_payload = (
        create_source_snapshot(
            source_db_path=source_db_path,
            snapshot_root=snapshot_root or (str(Path(str(export_db_path)).expanduser().resolve().parent / "source_snapshots") if export_db_path else None),
            label=f"historical_replay_{replay_id}",
        )
        if snapshot_source
        else None
    )
    effective_source_db_path = str((snapshot_payload or {}).get("snapshot_db_path") or source_db_path or "")
    replay_dates = _select_replay_dates(
        source_db_path=effective_source_db_path,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        max_days=max_days,
    )
    selected_codes = _select_codes(source_db_path=effective_source_db_path, codes=codes, max_codes=max_codes)
    upsert_replay_run(
        replay_id=replay_id,
        job_type=JOB_TYPE,
        status="running",
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        max_days=max_days,
        universe_filter=",".join(selected_codes) if selected_codes else None,
        universe_limit=max_codes,
        started_at=started_at,
        details={"resume": bool(resume), "source_snapshot": snapshot_payload},
        ops_db_path=ops_db_path,
    )
    bootstrap_phase = {
        "heartbeat_at": _utcnow().isoformat(),
        "current_phase": "bootstrap",
        "success_days": 0,
        "failed_days": 0,
        "resume": bool(resume),
        "source_snapshot": snapshot_payload,
    }
    upsert_replay_run(
        replay_id=replay_id,
        job_type=JOB_TYPE,
        status="running",
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        max_days=max_days,
        universe_filter=",".join(selected_codes) if selected_codes else None,
        universe_limit=max_codes,
        started_at=started_at,
        details=bootstrap_phase,
        ops_db_path=ops_db_path,
    )
    def _update_bootstrap_phase(phase: str) -> None:
        upsert_replay_run(
            replay_id=replay_id,
            job_type=JOB_TYPE,
            status="running",
            start_as_of_date=start_as_of_date,
            end_as_of_date=end_as_of_date,
            max_days=max_days,
            universe_filter=",".join(selected_codes) if selected_codes else None,
            universe_limit=max_codes,
            started_at=started_at,
            details={
                **bootstrap_phase,
                "heartbeat_at": _utcnow().isoformat(),
                "current_phase": phase,
            },
            ops_db_path=ops_db_path,
        )

    bootstrap_payload = _run_replay_bootstrap(
        source_db_path=effective_source_db_path,
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        on_phase=_update_bootstrap_phase,
    )
    similarity_bootstrap_payload: dict[str, Any] | None = None
    if replay_dates:
        _update_bootstrap_phase("bootstrap_similarity_case_library")
        similarity_bootstrap_payload = build_case_library(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            similarity_db_path=similarity_db_path,
            as_of_date=replay_dates[-1],
            codes=selected_codes,
        )
    bootstrap_details = {
        **bootstrap_payload,
        "similarity_case_library": similarity_bootstrap_payload,
    }
    bootstrap_phase = {
        **bootstrap_phase,
        "heartbeat_at": _utcnow().isoformat(),
        "current_phase": "bootstrap_completed",
        "bootstrap": _json_ready(bootstrap_details),
    }
    upsert_replay_run(
        replay_id=replay_id,
        job_type=JOB_TYPE,
        status="running",
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        max_days=max_days,
        universe_filter=",".join(selected_codes) if selected_codes else None,
        universe_limit=max_codes,
        started_at=started_at,
        details=bootstrap_phase,
        ops_db_path=ops_db_path,
    )
    successful_days: list[dict[str, Any]] = []
    failed_days: list[dict[str, Any]] = []
    skipped_days: list[dict[str, Any]] = []
    for as_of_date in replay_dates:
        if resume and _get_day_status(replay_id=replay_id, as_of_date=as_of_date, ops_db_path=ops_db_path) == "success":
            skipped_days.append({"as_of_date": as_of_date})
            continue
        publish_id = _replay_publish_id(replay_id, as_of_date)
        day_started_at = _utcnow()
        upsert_replay_day(
            replay_id=replay_id,
            as_of_date=_as_of_date_text(as_of_date),
            status="running",
            attempt=1,
            publish_id=publish_id,
            started_at=day_started_at,
            details={"publish_id": publish_id},
            ops_db_path=ops_db_path,
        )
        upsert_replay_run(
            replay_id=replay_id,
            job_type=JOB_TYPE,
            status="running",
            start_as_of_date=start_as_of_date,
            end_as_of_date=end_as_of_date,
            max_days=max_days,
            universe_filter=",".join(selected_codes) if selected_codes else None,
            universe_limit=max_codes,
            started_at=started_at,
            details=_merge_details(
                bootstrap_phase,
                _heartbeat_details(
                    current_phase="day_started",
                    as_of_date=as_of_date,
                    publish_id=publish_id,
                    attempt=1,
                ),
                {
                    "success_days": len(successful_days),
                    "failed_days": len(failed_days),
                },
            ),
            ops_db_path=ops_db_path,
        )
        last_error: Exception | None = None
        for attempt in range(1, int(max_attempts) + 1):
            try:
                phase_details = _heartbeat_details(
                    current_phase="candidate",
                    as_of_date=as_of_date,
                    publish_id=publish_id,
                    attempt=attempt,
                )
                upsert_replay_day(
                    replay_id=replay_id,
                    as_of_date=_as_of_date_text(as_of_date),
                    status="running",
                    attempt=attempt,
                    publish_id=publish_id,
                    started_at=day_started_at,
                    details=_merge_details({"publish_id": publish_id}, phase_details),
                    ops_db_path=ops_db_path,
                )
                upsert_replay_run(
                    replay_id=replay_id,
                    job_type=JOB_TYPE,
                    status="running",
                    start_as_of_date=start_as_of_date,
                    end_as_of_date=end_as_of_date,
                    max_days=max_days,
                    universe_filter=",".join(selected_codes) if selected_codes else None,
                    universe_limit=max_codes,
                    started_at=started_at,
                    details=_merge_details(
                        bootstrap_phase,
                        phase_details,
                        {
                            "success_days": len(successful_days),
                            "failed_days": len(failed_days),
                        },
                    ),
                    ops_db_path=ops_db_path,
                )
                candidate_payload = run_candidate_baseline(
                    export_db_path=export_db_path,
                    label_db_path=label_db_path,
                    result_db_path=result_db_path,
                    similarity_db_path=similarity_db_path,
                    as_of_date=as_of_date,
                    publish_id=publish_id,
                    freshness_state="replay",
                    publish_public=False,
                    codes=selected_codes,
                )
                upsert_replay_run(
                    replay_id=replay_id,
                    job_type=JOB_TYPE,
                    status="running",
                    start_as_of_date=start_as_of_date,
                    end_as_of_date=end_as_of_date,
                    max_days=max_days,
                    universe_filter=",".join(selected_codes) if selected_codes else None,
                    universe_limit=max_codes,
                    started_at=started_at,
                    details=_merge_details(
                        bootstrap_phase,
                        phase_details,
                        {
                            "success_days": len(successful_days),
                            "failed_days": len(failed_days),
                        },
                    ),
                    ops_db_path=ops_db_path,
                )
                phase_details = _heartbeat_details(
                    current_phase="similarity",
                    as_of_date=as_of_date,
                    publish_id=publish_id,
                    attempt=attempt,
                )
                upsert_replay_day(
                    replay_id=replay_id,
                    as_of_date=_as_of_date_text(as_of_date),
                    status="running",
                    attempt=attempt,
                    publish_id=publish_id,
                    started_at=day_started_at,
                    details=_merge_details({"publish_id": publish_id}, phase_details),
                    ops_db_path=ops_db_path,
                )
                champion_payload = run_similarity_baseline(
                    export_db_path=export_db_path,
                    label_db_path=label_db_path,
                    result_db_path=result_db_path,
                    similarity_db_path=similarity_db_path,
                    as_of_date=as_of_date,
                    publish_id=publish_id,
                    freshness_state="replay",
                    publish_public=False,
                    codes=selected_codes,
                )
                if not candidate_payload.get("metrics_saved", False):
                    raise RuntimeError("candidate_metrics_missing")
                if not champion_payload.get("metrics_saved", False):
                    raise RuntimeError("similarity_champion_metrics_missing")
                day_payload = {
                    "as_of_date": as_of_date,
                    "publish_id": publish_id,
                    "export_run_id": bootstrap_payload["export"].get("run_id"),
                    "label_run_id": bootstrap_payload["labels"].get("run_id"),
                    "anchor_run_id": bootstrap_payload["anchors"].get("run_id"),
                    "candidate_metrics_saved": candidate_payload.get("metrics_saved"),
                    "similarity_metrics_saved": champion_payload.get("metrics_saved"),
                }
                successful_days.append(day_payload)
                upsert_replay_day(
                    replay_id=replay_id,
                    as_of_date=_as_of_date_text(as_of_date),
                    status="success",
                    attempt=attempt,
                    publish_id=publish_id,
                    started_at=day_started_at,
                    finished_at=_utcnow(),
                    details=_json_ready(day_payload),
                    ops_db_path=ops_db_path,
                )
                upsert_replay_run(
                    replay_id=replay_id,
                    job_type=JOB_TYPE,
                    status="running",
                    start_as_of_date=start_as_of_date,
                    end_as_of_date=end_as_of_date,
                    max_days=max_days,
                    universe_filter=",".join(selected_codes) if selected_codes else None,
                    universe_limit=max_codes,
                    started_at=started_at,
                    last_completed_as_of_date=_as_of_date_text(as_of_date),
                    details={
                        "success_days": len(successful_days),
                        "failed_days": len(failed_days),
                        "source_snapshot": snapshot_payload,
                        "bootstrap": _json_ready(bootstrap_payload),
                        "heartbeat_at": _utcnow().isoformat(),
                        "current_phase": "day_completed",
                        "current_as_of_date": _as_of_date_text(as_of_date),
                        "current_publish_id": publish_id,
                    },
                    ops_db_path=ops_db_path,
                )
                break
            except Exception as exc:
                last_error = exc
                if attempt == int(max_attempts):
                    failed_days.append({"as_of_date": as_of_date, "error_class": exc.__class__.__name__})
                    upsert_replay_day(
                        replay_id=replay_id,
                        as_of_date=_as_of_date_text(as_of_date),
                        status="failed",
                        attempt=attempt,
                        publish_id=publish_id,
                        started_at=day_started_at,
                        finished_at=_utcnow(),
                        error_class=exc.__class__.__name__,
                        details={"message": str(exc)},
                        ops_db_path=ops_db_path,
                    )
                    insert_quarantine_record(
                        quarantine_id=f"{replay_id}_{_as_of_date_text(as_of_date)}",
                        job_type=JOB_TYPE,
                        as_of_date=_as_of_date_text(as_of_date),
                        publish_id=publish_id,
                        attempt_count=attempt,
                        reason="historical_replay_day_failed",
                        payload={"error_class": exc.__class__.__name__, "message": str(exc), "replay_id": replay_id},
                        ops_db_path=ops_db_path,
                    )
                logger.warning("historical_replay day failed replay_id=%s as_of_date=%s attempt=%s", replay_id, as_of_date, attempt)
        if last_error and len(failed_days) and failed_days[-1]["as_of_date"] == as_of_date:
            continue
    persisted_days = _load_replay_days(replay_id=replay_id, ops_db_path=ops_db_path)
    persisted_successful_days = [day for day in persisted_days if day["status"] == "success" and day.get("publish_id")]
    persisted_failed_days = [day for day in persisted_days if day["status"] == "failed"]
    summary = {
        "replay_id": replay_id,
        "start_as_of_date": _as_of_date_text(replay_dates[0]) if replay_dates else _as_of_date_text(_normalize_as_of_date(start_as_of_date)),
        "end_as_of_date": _as_of_date_text(replay_dates[-1]) if replay_dates else _as_of_date_text(_normalize_as_of_date(end_as_of_date)),
        "total_days": len(replay_dates),
        "success_days": len(persisted_successful_days),
        "failed_days": len(persisted_failed_days),
        "skipped_days": len(skipped_days),
        "daily_results": _json_ready(persisted_days),
        "readiness_windows": [],
    }
    persist_replay_summary(
        summary_id=f"{replay_id}_summary",
        replay_id=replay_id,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        total_days=len(replay_dates),
        success_days=len(persisted_successful_days),
        failed_days=len(persisted_failed_days),
        skipped_days=len(skipped_days),
        summary=summary,
        ops_db_path=ops_db_path,
    )
    queued_work_ids: list[str] = []
    if persisted_successful_days:
        challenger_work_id = f"challenger_eval_replay_{replay_id}"
        upsert_work_item(
            work_id=challenger_work_id,
            work_type="challenger_eval",
            scope_type="replay",
            scope_id=replay_id,
            status="pending",
            payload={
                "replay_id": replay_id,
                "days": [
                    {
                        "publish_id": str(day["publish_id"]),
                        "as_of_date": _as_of_date_text(int(day["as_of_date"])),
                    }
                    for day in persisted_successful_days
                ],
                "top_k": 5,
                "source_signature": _current_case_library_source_signature(similarity_db_path=similarity_db_path),
                "challenger_version": CHALLENGER_EMBEDDING_VERSION,
                "champion_version": EMBEDDING_VERSION,
            },
            ops_db_path=ops_db_path,
        )
        queued_work_ids.append(challenger_work_id)
    final_status = "success" if not failed_days else "partial_failure"
    upsert_replay_run(
        replay_id=replay_id,
        job_type=JOB_TYPE,
        status=final_status,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        max_days=max_days,
        universe_filter=",".join(selected_codes) if selected_codes else None,
        universe_limit=max_codes,
        started_at=started_at,
        finished_at=_utcnow(),
        last_completed_as_of_date=None if not persisted_successful_days else _as_of_date_text(int(persisted_successful_days[-1]["as_of_date"])),
        error_class=None if not persisted_failed_days else "ReplayDayFailed",
        details={
            "summary_id": f"{replay_id}_summary",
            "readiness_count": 0,
            "queued_work_ids": queued_work_ids,
            "source_snapshot": snapshot_payload,
            "bootstrap": _json_ready(bootstrap_details),
        },
        ops_db_path=ops_db_path,
    )
    return {
        "ok": True,
        "replay_id": replay_id,
        "job_type": JOB_TYPE,
        "status": final_status,
        "total_days": len(replay_dates),
        "success_days": len(persisted_successful_days),
        "failed_days": len(persisted_failed_days),
        "skipped_days": len(skipped_days),
        "summary_id": f"{replay_id}_summary",
        "readiness_count": 0,
        "queued_work_ids": queued_work_ids,
        "source_snapshot": snapshot_payload,
        "bootstrap": _json_ready(bootstrap_details),
    }


def run_historical_replay(**kwargs: Any) -> dict[str, Any]:
    return run_replay_core(**kwargs)
