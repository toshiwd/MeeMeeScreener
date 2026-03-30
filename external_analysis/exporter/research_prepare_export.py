from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from external_analysis.exporter.diff_export import (
    PROGRESS_SCHEMA_VERSION,
    PROGRESS_STATUS_COMPLETE,
    PROGRESS_STATUS_FAILED,
    PROGRESS_STATUS_PENDING,
    PROGRESS_STATUS_RUNNING,
    PROGRESS_STATUS_SKIPPED,
)
from external_analysis.exporter.export_schema import connect_export_db, ensure_export_schema
from external_analysis.exporter.snapshot_status import build_source_signature_payload, resolve_snapshot_progress_path

_STEP_NAMES: tuple[str, ...] = (
    "bars_daily_export",
    "bars_monthly_export",
    "indicator_daily_export",
    "pattern_state_export",
    "trade_event_export",
    "position_snapshot_export",
    "meta_export_runs",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def _date_sql(column_name: str) -> str:
    return (
        f"CASE "
        f"WHEN {column_name} BETWEEN 19000101 AND 20991231 THEN CAST({column_name} AS INTEGER) "
        f"WHEN {column_name} >= 1000000000000 THEN CAST(strftime(to_timestamp({column_name} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {column_name} >= 1000000000 THEN CAST(strftime(to_timestamp({column_name}), '%Y%m%d') AS INTEGER) "
        f"ELSE NULL END"
    )


def _month_sql(column_name: str) -> str:
    return f"CAST(SUBSTR(CAST({_date_sql(column_name)} AS VARCHAR), 1, 6) AS INTEGER)"


def _exists(conn: duckdb.DuckDBPyConnection, *, db: str, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {db}.{table} LIMIT 1").fetchone()
        return True
    except duckdb.Error:
        return False


def _build_progress_payload(*, run_id: str, source_db_path: str, export_db_path: str, source_signature: str) -> dict[str, Any]:
    started_at = _utcnow_iso()
    return {
        "schema_version": PROGRESS_SCHEMA_VERSION,
        "status": PROGRESS_STATUS_RUNNING,
        "reason_code": "prepare_running",
        "run_id": run_id,
        "source_db_path": source_db_path,
        "export_db_path": export_db_path,
        "source_signature": source_signature,
        "started_at": started_at,
        "updated_at": started_at,
        "current_step": None,
        "completed_steps": [],
        "steps": [
            {
                "step_name": step_name,
                "step_kind": "export",
                "status": PROGRESS_STATUS_PENDING,
                "started_at": None,
                "finished_at": None,
                "row_count": None,
                "max_trade_date": None,
                "details": {},
            }
            for step_name in _STEP_NAMES
        ],
    }


def _record_progress_row(
    conn: duckdb.DuckDBPyConnection,
    *,
    database: str,
    run_id: str,
    source_signature: str,
    step_name: str,
    status: str,
    started_at: datetime | None,
    finished_at: datetime | None,
    row_count: int | None,
    max_trade_date: int | None,
    details: dict[str, Any] | None,
) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {database}.meta_export_table_progress (
            run_id, source_signature, step_name, step_kind, status, started_at, finished_at,
            row_count, max_trade_date, details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            source_signature,
            step_name,
            "export",
            status,
            started_at,
            finished_at,
            row_count,
            max_trade_date,
            json.dumps(_json_ready(details or {}), ensure_ascii=False, sort_keys=True),
        ],
    )


def _update_progress(
    progress: dict[str, Any],
    path: Path,
    *,
    step_name: str,
    status: str,
    started_at: datetime | None,
    finished_at: datetime | None,
    row_count: int | None,
    max_trade_date: int | None,
    details: dict[str, Any] | None,
) -> None:
    for step in progress["steps"]:
        if str(step.get("step_name")) != step_name:
            continue
        step.update(
            {
                "status": status,
                "started_at": started_at.isoformat() if started_at is not None else None,
                "finished_at": finished_at.isoformat() if finished_at is not None else None,
                "row_count": row_count,
                "max_trade_date": max_trade_date,
                "details": _json_ready(details or {}),
            }
        )
        break
    progress["updated_at"] = _utcnow_iso()
    progress["current_step"] = step_name if status == PROGRESS_STATUS_RUNNING else None
    progress["completed_steps"] = [
        str(step.get("step_name") or "")
        for step in progress["steps"]
        if str(step.get("status") or "") in {PROGRESS_STATUS_COMPLETE, PROGRESS_STATUS_SKIPPED}
    ]
    _write_json(path, progress)


def _month_chunks(conn: duckdb.DuckDBPyConnection, *, db: str, table: str, column_name: str) -> list[int]:
    if not _exists(conn, db=db, table=table):
        return []
    rows = conn.execute(
        f"SELECT DISTINCT {_month_sql(column_name)} AS month_key FROM {db}.{table} WHERE {_date_sql(column_name)} IS NOT NULL ORDER BY month_key"
    ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _update_chunk_running(
    progress: dict[str, Any],
    progress_path: Path,
    *,
    step_name: str,
    table_name: str,
    started_at: datetime,
    completed_chunks: int,
    total_chunks: int,
    current_chunk: int | None,
    eta_seconds: int | None,
) -> None:
    details: dict[str, Any] = {
        "table_name": table_name,
        "completed_chunks": completed_chunks,
        "total_chunks": total_chunks,
        "eta_seconds": eta_seconds,
    }
    if current_chunk is not None:
        details["current_chunk"] = current_chunk
    _update_progress(
        progress,
        progress_path,
        step_name=step_name,
        status=PROGRESS_STATUS_RUNNING,
        started_at=started_at,
        finished_at=None,
        row_count=None,
        max_trade_date=None,
        details=details,
    )


def run_research_prepare_export(source_db_path: str, export_db_path: str) -> dict[str, Any]:
    source_payload = build_source_signature_payload(source_db_path)
    export_path = Path(str(export_db_path)).expanduser().resolve()
    progress_path = resolve_snapshot_progress_path(export_path)
    run_id = _utcnow().strftime("prep_%Y%m%dT%H%M%S%fZ")
    source_signature = str(source_payload["source_signature"])
    progress = _build_progress_payload(
        run_id=run_id,
        source_db_path=str(source_payload["source_db_path"]),
        export_db_path=str(export_path),
        source_signature=source_signature,
    )
    _write_json(progress_path, progress)

    schema_conn = connect_export_db(str(export_path))
    try:
        ensure_export_schema(schema_conn)
        schema_conn.execute("CHECKPOINT")
    finally:
        schema_conn.close()

    work = duckdb.connect()
    src = "prep_src"
    dst = "prep_export"
    attached_src = False
    attached_dst = False
    try:
        source_path_sql = str(source_payload["source_db_path"]).replace("'", "''")
        export_path_sql = str(export_path).replace("'", "''")
        work.execute(f"ATTACH '{source_path_sql}' AS {src} (READ_ONLY)")
        attached_src = True
        work.execute(f"ATTACH '{export_path_sql}' AS {dst}")
        attached_dst = True
        for table_name in (
            "bars_daily_export",
            "bars_monthly_export",
            "indicator_daily_export",
            "pattern_state_export",
            "trade_event_export",
            "position_snapshot_export",
        ):
            work.execute(f"DELETE FROM {dst}.{table_name}")

        has_daily_bars = _exists(work, db=src, table="daily_bars")
        has_daily_ma = _exists(work, db=src, table="daily_ma")
        has_feature = _exists(work, db=src, table="feature_snapshot_daily")
        has_monthly = _exists(work, db=src, table="monthly_bars")
        has_trade_events = _exists(work, db=src, table="trade_events")
        has_rounds = _exists(work, db=src, table="position_rounds")
        has_positions = _exists(work, db=src, table="positions_live")

        def _chunked(
            *,
            step_name: str,
            table_name: str,
            months: list[int],
            sql: str,
            params_builder,
            max_field: str = "trade_date",
        ) -> tuple[int, int | None]:
            started_at = _utcnow()
            total_chunks = len(months)
            if total_chunks == 0:
                details = {"table_name": table_name, "completed_chunks": 0, "total_chunks": 0, "eta_seconds": 0}
                _record_progress_row(
                    work,
                    database=dst,
                    run_id=run_id,
                    source_signature=source_signature,
                    step_name=step_name,
                    status=PROGRESS_STATUS_SKIPPED,
                    started_at=started_at,
                    finished_at=_utcnow(),
                    row_count=0,
                    max_trade_date=None,
                    details=details,
                )
                _update_progress(
                    progress,
                    progress_path,
                    step_name=step_name,
                    status=PROGRESS_STATUS_SKIPPED,
                    started_at=started_at,
                    finished_at=_utcnow(),
                    row_count=0,
                    max_trade_date=None,
                    details=details,
                )
                return 0, None

            _record_progress_row(
                work,
                database=dst,
                run_id=run_id,
                source_signature=source_signature,
                step_name=step_name,
                status=PROGRESS_STATUS_RUNNING,
                started_at=started_at,
                finished_at=None,
                row_count=None,
                max_trade_date=None,
                details={"table_name": table_name, "completed_chunks": 0, "total_chunks": total_chunks, "eta_seconds": None},
            )
            _update_chunk_running(
                progress,
                progress_path,
                step_name=step_name,
                table_name=table_name,
                started_at=started_at,
                completed_chunks=0,
                total_chunks=total_chunks,
                current_chunk=None,
                eta_seconds=None,
            )

            chunk_seconds: list[float] = []
            for index, month in enumerate(months, start=1):
                chunk_started = _utcnow()
                work.execute(sql, params_builder(month))
                chunk_finished = _utcnow()
                chunk_seconds.append(max((chunk_finished - chunk_started).total_seconds(), 0.0))
                avg = sum(chunk_seconds) / float(len(chunk_seconds))
                eta_seconds = max(int(round(avg * max(total_chunks - index, 0))), 0)
                _record_progress_row(
                    work,
                    database=dst,
                    run_id=run_id,
                    source_signature=source_signature,
                    step_name=step_name,
                    status=PROGRESS_STATUS_RUNNING,
                    started_at=started_at,
                    finished_at=None,
                    row_count=None,
                    max_trade_date=None,
                    details={
                        "table_name": table_name,
                        "completed_chunks": index,
                        "total_chunks": total_chunks,
                        "current_chunk": month,
                        "eta_seconds": eta_seconds,
                    },
                )
                _update_chunk_running(
                    progress,
                    progress_path,
                    step_name=step_name,
                    table_name=table_name,
                    started_at=started_at,
                    completed_chunks=index,
                    total_chunks=total_chunks,
                    current_chunk=month,
                    eta_seconds=eta_seconds,
                )

            row_count = int(work.execute(f"SELECT COUNT(*) FROM {dst}.{table_name}").fetchone()[0] or 0)
            max_row = work.execute(f"SELECT MAX({max_field}) FROM {dst}.{table_name}").fetchone()
            max_value = int(max_row[0]) if max_row and max_row[0] is not None else None
            finished_at = _utcnow()
            details = {"table_name": table_name, "completed_chunks": total_chunks, "total_chunks": total_chunks, "eta_seconds": 0}
            _record_progress_row(
                work,
                database=dst,
                run_id=run_id,
                source_signature=source_signature,
                step_name=step_name,
                status=PROGRESS_STATUS_COMPLETE,
                started_at=started_at,
                finished_at=finished_at,
                row_count=row_count,
                max_trade_date=max_value,
                details=details,
            )
            _update_progress(
                progress,
                progress_path,
                step_name=step_name,
                status=PROGRESS_STATUS_COMPLETE,
                started_at=started_at,
                finished_at=finished_at,
                row_count=row_count,
                max_trade_date=max_value,
                details=details,
            )
            return row_count, max_value

        bars_months = _month_chunks(work, db=src, table="daily_bars", column_name="date") if has_daily_bars else []
        bars_count, bars_max = _chunked(
            step_name="bars_daily_export",
            table_name="bars_daily_export",
            months=bars_months,
            sql=(
                f"INSERT INTO {dst}.bars_daily_export (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id) "
                f"SELECT code, {_date_sql('date')}, o, h, l, c, v, COALESCE(source, 'unknown'), "
                f"code || ':' || CAST({_date_sql('date')} AS VARCHAR), ? "
                f"FROM {src}.daily_bars WHERE {_month_sql('date')} = ?"
            ),
            params_builder=lambda month: [run_id, int(month)],
        )
        _chunked(
            step_name="bars_monthly_export",
            table_name="bars_monthly_export",
            months=[1] if has_monthly else [],
            sql=(
                f"INSERT INTO {dst}.bars_monthly_export (code, month_key, o, h, l, c, v, row_hash, export_run_id) "
                f"SELECT code, month, o, h, l, c, v, code || ':' || CAST(month AS VARCHAR), ? FROM {src}.monthly_bars"
            ),
            params_builder=lambda _month: [run_id],
            max_field="month_key",
        )
        indicator_months = sorted(
            set(_month_chunks(work, db=src, table="daily_ma", column_name="date") if has_daily_ma else [])
            | set(_month_chunks(work, db=src, table="feature_snapshot_daily", column_name="dt") if has_feature else [])
        )
        indicator_count, _ = _chunked(
            step_name="indicator_daily_export",
            table_name="indicator_daily_export",
            months=indicator_months,
            sql=f"""
            INSERT INTO {dst}.indicator_daily_export (
                code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr,
                cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id
            )
            WITH ma_rows AS (
                SELECT code, {_date_sql('date')} AS trade_date, ma7, ma20, ma60
                FROM {src}.daily_ma
                WHERE {_month_sql('date')} = ?
            ),
            feature_rows AS (
                SELECT code, {_date_sql('dt')} AS trade_date, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags
                FROM {src}.feature_snapshot_daily
                WHERE {_month_sql('dt')} = ?
            )
            SELECT
                COALESCE(ma_rows.code, feature_rows.code),
                COALESCE(ma_rows.trade_date, feature_rows.trade_date),
                ma_rows.ma7, ma_rows.ma20, ma_rows.ma60, NULL, NULL,
                feature_rows.atr14, feature_rows.diff20_pct, feature_rows.diff20_atr, feature_rows.cnt_20_above, feature_rows.cnt_7_above, feature_rows.day_count, feature_rows.candle_flags,
                COALESCE(ma_rows.code, feature_rows.code) || ':' || CAST(COALESCE(ma_rows.trade_date, feature_rows.trade_date) AS VARCHAR),
                ?
            FROM ma_rows
            FULL OUTER JOIN feature_rows
              ON ma_rows.code = feature_rows.code AND ma_rows.trade_date = feature_rows.trade_date
            """,
            params_builder=lambda month: [int(month), int(month), run_id],
        )
        pattern_months = _month_chunks(work, db=src, table="feature_snapshot_daily", column_name="dt") if has_feature else []
        pattern_count, _ = _chunked(
            step_name="pattern_state_export",
            table_name="pattern_state_export",
            months=pattern_months,
            sql=(
                f"INSERT INTO {dst}.pattern_state_export (code, trade_date, ppp_state, abc_state, box_state, box_upper, box_lower, ranking_state, event_flags, row_hash, export_run_id) "
                f"SELECT code, {_date_sql('dt')}, NULL, NULL, NULL, NULL, NULL, NULL, candle_flags, code || ':' || CAST({_date_sql('dt')} AS VARCHAR), ? "
                f"FROM {src}.feature_snapshot_daily WHERE {_month_sql('dt')} = ?"
            ),
            params_builder=lambda month: [run_id, int(month)],
        )
        _chunked(
            step_name="trade_event_export",
            table_name="trade_event_export",
            months=[1] if (has_trade_events or has_rounds) else [],
            sql=(
                f"INSERT INTO {dst}.trade_event_export (code, event_ts, event_seq, event_type, broker_label, qty, price, row_hash, export_run_id) "
                f"WITH ordered AS ("
                f"SELECT symbol AS code, exec_dt AS event_ts, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY exec_dt, source_row_hash) AS event_seq, action AS event_type, broker AS broker_label, qty, price, COALESCE(source_row_hash, symbol || ':' || CAST(exec_dt AS VARCHAR)) AS row_hash "
                f"FROM {src}.trade_events WHERE exec_dt IS NOT NULL"
                f") SELECT code, event_ts, event_seq, event_type, broker_label, qty, price, row_hash, ? FROM ordered"
                if has_trade_events
                else (
                    f"INSERT INTO {dst}.trade_event_export (code, event_ts, event_seq, event_type, broker_label, qty, price, row_hash, export_run_id) "
                    f"WITH raw_events AS ("
                    f"SELECT symbol AS code, opened_at AS event_ts, 'round_open' AS event_type, round_id || ':open' AS row_hash FROM {src}.position_rounds WHERE opened_at IS NOT NULL "
                    f"UNION ALL "
                    f"SELECT symbol AS code, closed_at AS event_ts, 'round_close' AS event_type, round_id || ':close' AS row_hash FROM {src}.position_rounds WHERE closed_at IS NOT NULL"
                    f"), ordered AS ("
                    f"SELECT code, event_ts, ROW_NUMBER() OVER (PARTITION BY code ORDER BY event_ts, row_hash) AS event_seq, event_type, NULL AS broker_label, NULL AS qty, NULL AS price, row_hash FROM raw_events"
                    f") SELECT code, event_ts, event_seq, event_type, broker_label, qty, price, row_hash, ? FROM ordered"
                )
            ),
            params_builder=lambda _month: [run_id],
            max_field="event_ts",
        )
        _chunked(
            step_name="position_snapshot_export",
            table_name="position_snapshot_export",
            months=[1] if has_positions else [],
            sql=(
                f"INSERT INTO {dst}.position_snapshot_export (code, snapshot_at, spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note, row_hash, export_run_id) "
                f"SELECT symbol, COALESCE(updated_at, opened_at), spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note, symbol || ':' || CAST(COALESCE(updated_at, opened_at) AS VARCHAR), ? "
                f"FROM {src}.positions_live WHERE COALESCE(updated_at, opened_at) IS NOT NULL"
            ),
            params_builder=lambda _month: [run_id],
            max_field="snapshot_at",
        )

        meta_started = _utcnow()
        _record_progress_row(
            work,
            database=dst,
            run_id=run_id,
            source_signature=source_signature,
            step_name="meta_export_runs",
            status=PROGRESS_STATUS_RUNNING,
            started_at=meta_started,
            finished_at=None,
            row_count=None,
            max_trade_date=bars_max,
            details={"table_name": "meta_export_runs", "completed_chunks": 0, "total_chunks": 1, "eta_seconds": None},
        )
        _update_progress(
            progress,
            progress_path,
            step_name="meta_export_runs",
            status=PROGRESS_STATUS_RUNNING,
            started_at=meta_started,
            finished_at=None,
            row_count=None,
            max_trade_date=bars_max,
            details={"table_name": "meta_export_runs", "completed_chunks": 0, "total_chunks": 1, "eta_seconds": None},
        )
        changed_table_names = [
            "bars_daily_export",
            "bars_monthly_export",
            "indicator_daily_export",
            "pattern_state_export",
            "trade_event_export",
            "position_snapshot_export",
        ]
        work.execute(
            f"""
            INSERT OR REPLACE INTO {dst}.meta_export_runs (
                run_id, started_at, finished_at, status, source_db_path, source_signature,
                source_max_trade_date, source_row_counts, changed_table_names, diff_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                _utcnow(),
                _utcnow(),
                "success",
                str(source_payload["source_db_path"]),
                source_signature,
                int(source_payload.get("source_max_trade_date") or 0) or None,
                json.dumps(source_payload.get("source_counts") or {}, ensure_ascii=False, sort_keys=True),
                json.dumps(changed_table_names, ensure_ascii=False),
                json.dumps({"mode": "daily_research_prepare_bulk"}, ensure_ascii=False, sort_keys=True),
            ],
        )
        work.execute("CHECKPOINT")
        meta_finished = _utcnow()
        _record_progress_row(
            work,
            database=dst,
            run_id=run_id,
            source_signature=source_signature,
            step_name="meta_export_runs",
            status=PROGRESS_STATUS_COMPLETE,
            started_at=meta_started,
            finished_at=meta_finished,
            row_count=1,
            max_trade_date=bars_max,
            details={"table_name": "meta_export_runs", "completed_chunks": 1, "total_chunks": 1, "eta_seconds": 0},
        )
        _update_progress(
            progress,
            progress_path,
            step_name="meta_export_runs",
            status=PROGRESS_STATUS_COMPLETE,
            started_at=meta_started,
            finished_at=meta_finished,
            row_count=1,
            max_trade_date=bars_max,
            details={"table_name": "meta_export_runs", "completed_chunks": 1, "total_chunks": 1, "eta_seconds": 0},
        )
        progress["status"] = PROGRESS_STATUS_COMPLETE
        progress["reason_code"] = "prepare_complete"
        progress["current_step"] = None
        progress["updated_at"] = _utcnow_iso()
        _write_json(progress_path, progress)
        return {
            "ok": True,
            "run_id": run_id,
            "source_db_path": str(source_payload["source_db_path"]),
            "source_signature": source_signature,
            "source_max_trade_date": int(source_payload.get("source_max_trade_date") or 0) or None,
            "changed_table_names": changed_table_names,
            "diff_reason": {"mode": "daily_research_prepare_bulk"},
            "progress_path": str(progress_path),
            "export_counts": {
                "bars_count": bars_count,
                "indicator_count": indicator_count,
                "pattern_count": pattern_count,
                "max_trade_date": int(bars_max or 0),
            },
        }
    except Exception:
        progress["status"] = PROGRESS_STATUS_FAILED
        progress["reason_code"] = "prepare_failed"
        progress["current_step"] = None
        progress["updated_at"] = _utcnow_iso()
        _write_json(progress_path, progress)
        raise
    finally:
        if attached_dst:
            work.execute(f"DETACH {dst}")
        if attached_src:
            work.execute(f"DETACH {src}")
        work.close()
