from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from external_analysis.contracts.paths import resolve_export_db_path, resolve_source_db_path
from external_analysis.exporter.export_schema import connect_export_db, ensure_export_schema
from external_analysis.exporter.source_reader import (
    connect_source_db,
    fetch_rows,
    normalize_market_date,
    source_column_exists,
    source_table_exists,
)

SOURCE_PROGRESS_STEPS: tuple[tuple[str, str], ...] = (
    ("daily_bars", "source"),
    ("monthly_bars", "source"),
    ("daily_ma", "source"),
    ("feature_snapshot_daily", "source"),
    ("positions_live", "source"),
    ("position_rounds", "source"),
)

EXPORT_PROGRESS_STEPS: tuple[tuple[str, str], ...] = (
    ("bars_daily_export", "export"),
    ("bars_monthly_export", "export"),
    ("indicator_daily_export", "export"),
    ("pattern_state_export", "export"),
    ("trade_event_export", "export"),
    ("position_snapshot_export", "export"),
    ("meta_export_runs", "export"),
)

PROGRESS_SCHEMA_VERSION = "tradex_export_snapshot_progress_v1"
PROGRESS_STATUS_PENDING = "pending"
PROGRESS_STATUS_RUNNING = "running"
PROGRESS_STATUS_COMPLETE = "complete"
PROGRESS_STATUS_FAILED = "failed"
PROGRESS_STATUS_SKIPPED = "skipped"
PROGRESS_STATUS_RESUMED = "resumed"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_run_id() -> str:
    return _utcnow().strftime("exp_%Y%m%dT%H%M%S%fZ")


def _row_hash(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    payload = {key: row.get(key) for key in keys}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _source_signature(source_row_counts: dict[str, int], source_max_trade_date: int | None) -> str:
    raw = json.dumps(
        {"row_counts": source_row_counts, "source_max_trade_date": source_max_trade_date},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)
    return path


def _resolve_snapshot_progress_path(export_db_path: str | Path) -> Path:
    export_path = Path(str(export_db_path)).expanduser().resolve()
    return export_path.with_name(f"{export_path.name}.snapshot_progress.json")


def _normalize_progress_trade_date(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.strftime("%Y%m%d"))
    try:
        return normalize_market_date(value)
    except Exception:
        return None


def _existing_hashes(conn, table_name: str, key_columns: tuple[str, ...]) -> dict[tuple[Any, ...], str]:
    selected = ", ".join([*key_columns, "row_hash"])
    rows = conn.execute(f"SELECT {selected} FROM {table_name}").fetchall()
    return {tuple(row[: len(key_columns)]): str(row[-1]) for row in rows}


def _upsert_rows(conn, table_name: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    conn.executemany(
        f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
        [[row.get(column) for column in columns] for row in rows],
    )


def _delete_missing_rows(conn, table_name: str, key_columns: tuple[str, ...], source_keys: set[tuple[Any, ...]]) -> int:
    rows = conn.execute(f"SELECT {', '.join(key_columns)} FROM {table_name}").fetchall()
    stale_keys = [tuple(row) for row in rows if tuple(row) not in source_keys]
    for stale_key in stale_keys:
        where_sql = " AND ".join(f"{column} = ?" for column in key_columns)
        conn.execute(f"DELETE FROM {table_name} WHERE {where_sql}", list(stale_key))
    return len(stale_keys)


def _build_daily_export_rows(source_conn, export_run_id: str) -> list[dict[str, Any]]:
    if not source_table_exists(source_conn, "daily_bars"):
        return []
    has_source = source_column_exists(source_conn, "daily_bars", "source")
    rows = fetch_rows(
        source_conn,
        "daily_bars",
        ("code", "date", "o", "h", "l", "c", "v", *(("source",) if has_source else tuple())),
        order_by="code, date",
    )
    return [
        {
            "code": row["code"],
            "trade_date": normalize_market_date(row["date"]),
            "o": row["o"],
            "h": row["h"],
            "l": row["l"],
            "c": row["c"],
            "v": row["v"],
            "source": row.get("source") or "unknown",
            "row_hash": _row_hash(
                {**row, "date": normalize_market_date(row["date"]), "source": row.get("source") or "unknown"},
                ("code", "date", "o", "h", "l", "c", "v", "source"),
            ),
            "export_run_id": export_run_id,
        }
        for row in rows
    ]


def _build_monthly_export_rows(source_conn, export_run_id: str) -> list[dict[str, Any]]:
    if not source_table_exists(source_conn, "monthly_bars"):
        return []
    rows = fetch_rows(
        source_conn,
        "monthly_bars",
        ("code", "month", "o", "h", "l", "c", "v"),
        order_by="code, month",
    )
    return [
        {
            "code": row["code"],
            "month_key": normalize_market_date(row["month"]),
            "o": row["o"],
            "h": row["h"],
            "l": row["l"],
            "c": row["c"],
            "v": row["v"],
            "row_hash": _row_hash({**row, "month": normalize_market_date(row["month"])}, ("code", "month", "o", "h", "l", "c", "v")),
            "export_run_id": export_run_id,
        }
        for row in rows
    ]


def _build_indicator_export_rows(source_conn, export_run_id: str) -> list[dict[str, Any]]:
    date_keys: dict[tuple[str, int], dict[str, Any]] = {}
    if source_table_exists(source_conn, "daily_ma"):
        for row in fetch_rows(
            source_conn,
            "daily_ma",
            ("code", "date", "ma7", "ma20", "ma60"),
            order_by="code, date",
        ):
            trade_date = normalize_market_date(row["date"])
            date_keys[(row["code"], trade_date)] = {
                "code": row["code"],
                "trade_date": trade_date,
                "ma7": row["ma7"],
                "ma20": row["ma20"],
                "ma60": row["ma60"],
                "ma100": None,
                "ma200": None,
                "atr14": None,
                "diff20_pct": None,
                "diff20_atr": None,
                "cnt_20_above": None,
                "cnt_7_above": None,
                "day_count": None,
                "candle_flags": None,
            }
    if source_table_exists(source_conn, "feature_snapshot_daily"):
        for row in fetch_rows(
            source_conn,
            "feature_snapshot_daily",
            ("code", "dt", "atr14", "diff20_pct", "diff20_atr", "cnt_20_above", "cnt_7_above", "day_count", "candle_flags"),
            order_by="code, dt",
        ):
            trade_date = normalize_market_date(row["dt"])
            merged = date_keys.setdefault(
                (row["code"], trade_date),
                {
                    "code": row["code"],
                    "trade_date": trade_date,
                    "ma7": None,
                    "ma20": None,
                    "ma60": None,
                    "ma100": None,
                    "ma200": None,
                    "atr14": None,
                    "diff20_pct": None,
                    "diff20_atr": None,
                    "cnt_20_above": None,
                    "cnt_7_above": None,
                    "day_count": None,
                    "candle_flags": None,
                },
            )
            merged.update(
                {
                    "atr14": row["atr14"],
                    "diff20_pct": row["diff20_pct"],
                    "diff20_atr": row["diff20_atr"],
                    "cnt_20_above": row["cnt_20_above"],
                    "cnt_7_above": row["cnt_7_above"],
                    "day_count": row["day_count"],
                    "candle_flags": row["candle_flags"],
                }
            )
    export_rows: list[dict[str, Any]] = []
    for row in sorted(date_keys.values(), key=lambda item: (str(item["code"]), int(item["trade_date"]))):
        export_rows.append(
            {
                **row,
                "row_hash": _row_hash(
                    row,
                    ("code", "trade_date", "ma7", "ma20", "ma60", "ma100", "ma200", "atr14", "diff20_pct", "diff20_atr", "cnt_20_above", "cnt_7_above", "day_count", "candle_flags"),
                ),
                "export_run_id": export_run_id,
            }
        )
    return export_rows


def _build_pattern_export_rows(source_conn, export_run_id: str) -> list[dict[str, Any]]:
    if not source_table_exists(source_conn, "feature_snapshot_daily"):
        return []
    rows = fetch_rows(source_conn, "feature_snapshot_daily", ("code", "dt", "candle_flags"), order_by="code, dt")
    return [
        {
            "code": row["code"],
            "trade_date": normalize_market_date(row["dt"]),
            "ppp_state": None,
            "abc_state": None,
            "box_state": None,
            "box_upper": None,
            "box_lower": None,
            "ranking_state": None,
            "event_flags": row["candle_flags"],
            "row_hash": _row_hash({**row, "dt": normalize_market_date(row["dt"])}, ("code", "dt", "candle_flags")),
            "export_run_id": export_run_id,
        }
        for row in rows
    ]


def _build_position_export_rows(source_conn, export_run_id: str) -> list[dict[str, Any]]:
    if not source_table_exists(source_conn, "positions_live"):
        return []
    rows = fetch_rows(
        source_conn,
        "positions_live",
        ("symbol", "spot_qty", "margin_long_qty", "margin_short_qty", "buy_qty", "sell_qty", "opened_at", "updated_at", "has_issue", "issue_note"),
        order_by="symbol",
    )
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        snapshot_at = row["updated_at"] or row["opened_at"]
        if snapshot_at is None:
            continue
        export_rows.append(
            {
                "code": row["symbol"],
                "snapshot_at": snapshot_at,
                "spot_qty": row["spot_qty"],
                "margin_long_qty": row["margin_long_qty"],
                "margin_short_qty": row["margin_short_qty"],
                "buy_qty": row["buy_qty"],
                "sell_qty": row["sell_qty"],
                "has_issue": row["has_issue"],
                "issue_note": row["issue_note"],
                "row_hash": _row_hash(
                    row,
                    ("symbol", "spot_qty", "margin_long_qty", "margin_short_qty", "buy_qty", "sell_qty", "opened_at", "updated_at", "has_issue", "issue_note"),
                ),
                "export_run_id": export_run_id,
            }
        )
    return export_rows


def _build_trade_event_export_rows(source_conn, export_run_id: str) -> list[dict[str, Any]]:
    if source_table_exists(source_conn, "trade_events"):
        rows = fetch_rows(
            source_conn,
            "trade_events",
            ("broker", "exec_dt", "symbol", "action", "qty", "price", "source_row_hash"),
            order_by="symbol, exec_dt, source_row_hash",
        )
        export_rows: list[dict[str, Any]] = []
        event_seq = 1
        for row in rows:
            if row["exec_dt"] is None:
                continue
            export_rows.append(
                {
                    "code": row["symbol"],
                    "event_ts": row["exec_dt"],
                    "event_seq": event_seq,
                    "event_type": row["action"] or "trade_event",
                    "broker_label": row.get("broker"),
                    "qty": row.get("qty"),
                    "price": row.get("price"),
                    "row_hash": row.get("source_row_hash") or _row_hash(row, ("broker", "exec_dt", "symbol", "action", "qty", "price")),
                    "export_run_id": export_run_id,
                }
            )
            event_seq += 1
        return export_rows
    if not source_table_exists(source_conn, "position_rounds"):
        return []
    rows = fetch_rows(
        source_conn,
        "position_rounds",
        ("round_id", "symbol", "opened_at", "closed_at", "closed_reason"),
        order_by="symbol, opened_at, round_id",
    )
    export_rows: list[dict[str, Any]] = []
    event_seq = 1
    for row in rows:
        if row["opened_at"] is not None:
            export_rows.append(
                {
                    "code": row["symbol"],
                    "event_ts": row["opened_at"],
                    "event_seq": event_seq,
                    "event_type": "round_open",
                    "broker_label": None,
                    "qty": None,
                    "price": None,
                    "row_hash": _row_hash(row, ("round_id", "symbol", "opened_at", "closed_reason")),
                    "export_run_id": export_run_id,
                }
            )
            event_seq += 1
        if row["closed_at"] is not None:
            export_rows.append(
                {
                    "code": row["symbol"],
                    "event_ts": row["closed_at"],
                    "event_seq": event_seq,
                    "event_type": row["closed_reason"] or "round_close",
                    "broker_label": None,
                    "qty": None,
                    "price": None,
                    "row_hash": _row_hash(row, ("round_id", "symbol", "closed_at", "closed_reason")),
                    "export_run_id": export_run_id,
                }
            )
            event_seq += 1
    return export_rows


EXPORT_STEP_SPECS: tuple[dict[str, Any], ...] = (
    {"step_name": "bars_daily_export", "table_name": "bars_daily_export", "key_columns": ("code", "trade_date"), "builder": _build_daily_export_rows, "max_field": "trade_date"},
    {"step_name": "bars_monthly_export", "table_name": "bars_monthly_export", "key_columns": ("code", "month_key"), "builder": _build_monthly_export_rows, "max_field": "month_key"},
    {"step_name": "indicator_daily_export", "table_name": "indicator_daily_export", "key_columns": ("code", "trade_date"), "builder": _build_indicator_export_rows, "max_field": "trade_date"},
    {"step_name": "pattern_state_export", "table_name": "pattern_state_export", "key_columns": ("code", "trade_date"), "builder": _build_pattern_export_rows, "max_field": "trade_date"},
    {"step_name": "trade_event_export", "table_name": "trade_event_export", "key_columns": ("code", "event_ts", "event_seq"), "builder": _build_trade_event_export_rows, "max_field": "event_ts"},
    {"step_name": "position_snapshot_export", "table_name": "position_snapshot_export", "key_columns": ("code", "snapshot_at"), "builder": _build_position_export_rows, "max_field": "snapshot_at"},
)


def _source_step_max_date(source_conn, table_name: str) -> int | None:
    if not source_table_exists(source_conn, table_name):
        return None
    date_column_map = {"daily_bars": "date", "monthly_bars": "month", "daily_ma": "date", "feature_snapshot_daily": "dt"}
    date_column = date_column_map.get(table_name)
    if not date_column:
        return None
    row = source_conn.execute(f"SELECT MAX({date_column}) FROM {table_name}").fetchone()
    return _normalize_progress_trade_date(row[0] if row else None)


def _collect_source_row_counts(source_conn) -> dict[str, int]:
    return {
        table_name: (
            int(source_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
            if source_table_exists(source_conn, table_name)
            else 0
        )
        for table_name, _ in SOURCE_PROGRESS_STEPS
    }


def _collect_source_progress_rows(source_conn) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table_name, step_kind in SOURCE_PROGRESS_STEPS:
        table_exists = bool(source_table_exists(source_conn, table_name))
        row_count = int(source_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]) if table_exists else 0
        rows.append(
            {
                "step_name": table_name,
                "step_kind": step_kind,
                "row_count": row_count,
                "max_trade_date": _source_step_max_date(source_conn, table_name),
                "details": {"source_table": table_name, "table_exists": table_exists},
            }
        )
    return rows


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _table_state(conn, table_name: str, max_column: str | None) -> dict[str, Any]:
    if not _table_exists(conn, table_name):
        return {"row_count": 0, "max_trade_date": None}
    if max_column:
        row = conn.execute(f"SELECT COUNT(*), MAX({max_column}) FROM {table_name}").fetchone()
        return {"row_count": int(row[0] or 0), "max_trade_date": _normalize_progress_trade_date(row[1] if row else None)}
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return {"row_count": int(row[0] or 0), "max_trade_date": None}


def _step_row_summary(rows: list[dict[str, Any]], max_field: str | None) -> dict[str, Any]:
    max_trade_date = None
    if max_field:
        values = [_normalize_progress_trade_date(row.get(max_field)) for row in rows]
        normalized = [value for value in values if value is not None]
        max_trade_date = max(normalized) if normalized else None
    return {"row_count": len(rows), "max_trade_date": max_trade_date}


def _serialize_details(details: dict[str, Any] | None) -> str:
    return json.dumps(_json_ready(details or {}), ensure_ascii=False, sort_keys=True)


def _record_progress_row(
    conn,
    *,
    run_id: str,
    source_signature: str,
    step_name: str,
    step_kind: str,
    status: str,
    started_at: datetime | None,
    finished_at: datetime | None,
    row_count: int | None,
    max_trade_date: int | None,
    details: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO meta_export_table_progress (
            run_id, source_signature, step_name, step_kind, status, started_at, finished_at,
            row_count, max_trade_date, details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            source_signature,
            step_name,
            step_kind,
            status,
            started_at,
            finished_at,
            row_count,
            max_trade_date,
            _serialize_details(details),
        ],
    )


def _load_latest_step_progress(conn, source_signature: str) -> dict[str, dict[str, Any]]:
    if not _table_exists(conn, "meta_export_table_progress"):
        return {}
    rows = conn.execute(
        """
        SELECT
            run_id,
            step_name,
            step_kind,
            status,
            started_at,
            finished_at,
            row_count,
            max_trade_date,
            details_json
        FROM meta_export_table_progress
        WHERE source_signature = ?
        ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST, run_id DESC
        """,
        [source_signature],
    ).fetchall()
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        step_name = str(row[1])
        if step_name in latest:
            continue
        latest[step_name] = {
            "run_id": str(row[0]),
            "step_name": step_name,
            "step_kind": str(row[2]),
            "status": str(row[3]),
            "started_at": row[4],
            "finished_at": row[5],
            "row_count": int(row[6] or 0) if row[6] is not None else 0,
            "max_trade_date": int(row[7]) if row[7] is not None else None,
            "details": json.loads(str(row[8])) if row[8] is not None else {},
        }
    return latest


def _build_progress_payload(*, source_db_path: str, export_db_path: str, run_id: str, source_signature: str, started_at: str) -> dict[str, Any]:
    return {
        "schema_version": PROGRESS_SCHEMA_VERSION,
        "status": "running",
        "source_db_path": source_db_path,
        "export_db_path": export_db_path,
        "run_id": run_id,
        "source_signature": source_signature,
        "started_at": started_at,
        "updated_at": started_at,
        "completed_steps": [],
        "current_step": None,
        "steps": [
            {
                "step_name": step_name,
                "step_kind": step_kind,
                "status": PROGRESS_STATUS_PENDING,
                "started_at": None,
                "finished_at": None,
                "row_count": None,
                "max_trade_date": None,
                "details": {},
            }
            for step_name, step_kind in (*SOURCE_PROGRESS_STEPS, *EXPORT_PROGRESS_STEPS)
        ],
        "reason_code": "export_running",
    }


def _update_progress_payload(
    payload: dict[str, Any],
    *,
    step_name: str,
    step_kind: str,
    status: str,
    started_at: datetime | None,
    finished_at: datetime | None,
    row_count: int | None,
    max_trade_date: int | None,
    details: dict[str, Any] | None,
) -> None:
    for step in payload["steps"]:
        if step["step_name"] == step_name and step["step_kind"] == step_kind:
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
    payload["updated_at"] = _utcnow_iso()
    payload["completed_steps"] = [
        step["step_name"]
        for step in payload["steps"]
        if str(step.get("status") or "") in {PROGRESS_STATUS_COMPLETE, PROGRESS_STATUS_RESUMED, PROGRESS_STATUS_SKIPPED}
    ]
    payload["current_step"] = step_name if status == PROGRESS_STATUS_RUNNING else None


def _write_progress_payload(progress_path: Path, payload: dict[str, Any]) -> None:
    _write_json(progress_path, payload)


def _set_progress_failure(progress_path: Path, payload: dict[str, Any], *, reason_code: str, current_step: str | None) -> None:
    payload["status"] = PROGRESS_STATUS_FAILED
    payload["reason_code"] = reason_code
    payload["current_step"] = current_step
    payload["updated_at"] = _utcnow_iso()
    _write_progress_payload(progress_path, payload)


def _set_progress_complete(progress_path: Path, payload: dict[str, Any]) -> None:
    payload["status"] = PROGRESS_STATUS_COMPLETE
    payload["reason_code"] = "export_complete"
    payload["current_step"] = None
    payload["updated_at"] = _utcnow_iso()
    _write_progress_payload(progress_path, payload)


def _run_source_progress_steps(
    *,
    export_conn,
    progress_payload: dict[str, Any],
    progress_path: Path,
    run_id: str,
    source_signature: str,
    source_progress_rows: list[dict[str, Any]],
) -> None:
    for row in source_progress_rows:
        started_at = _utcnow()
        finished_at = _utcnow()
        _record_progress_row(
            export_conn,
            run_id=run_id,
            source_signature=source_signature,
            step_name=str(row["step_name"]),
            step_kind=str(row["step_kind"]),
            status=PROGRESS_STATUS_COMPLETE,
            started_at=started_at,
            finished_at=finished_at,
            row_count=int(row["row_count"]),
            max_trade_date=row["max_trade_date"],
            details=dict(row["details"]),
        )
        _update_progress_payload(
            progress_payload,
            step_name=str(row["step_name"]),
            step_kind=str(row["step_kind"]),
            status=PROGRESS_STATUS_COMPLETE,
            started_at=started_at,
            finished_at=finished_at,
            row_count=int(row["row_count"]),
            max_trade_date=row["max_trade_date"],
            details=dict(row["details"]),
        )
        _write_progress_payload(progress_path, progress_payload)


def _can_resume_step(*, export_conn, latest_progress: dict[str, dict[str, Any]], step_spec: dict[str, Any]) -> dict[str, Any] | None:
    latest = latest_progress.get(str(step_spec["step_name"]))
    if not latest:
        return None
    if str(latest.get("status") or "") not in {PROGRESS_STATUS_COMPLETE, PROGRESS_STATUS_RESUMED, PROGRESS_STATUS_SKIPPED}:
        return None
    current_state = _table_state(export_conn, str(step_spec["table_name"]), str(step_spec.get("max_field") or ""))
    if int(current_state["row_count"] or 0) != int(latest.get("row_count") or 0):
        return None
    if _normalize_progress_trade_date(current_state.get("max_trade_date")) != _normalize_progress_trade_date(latest.get("max_trade_date")):
        return None
    return latest


def _run_export_step(
    *,
    source_conn,
    export_conn,
    progress_payload: dict[str, Any],
    progress_path: Path,
    run_id: str,
    source_signature: str,
    latest_progress: dict[str, dict[str, Any]],
    step_spec: dict[str, Any],
) -> dict[str, Any]:
    step_name = str(step_spec["step_name"])
    table_name = str(step_spec["table_name"])
    key_columns = tuple(step_spec["key_columns"])
    builder: Callable[[Any, str], list[dict[str, Any]]] = step_spec["builder"]
    max_field = str(step_spec.get("max_field") or "")

    resumable = _can_resume_step(export_conn=export_conn, latest_progress=latest_progress, step_spec=step_spec)
    if resumable is not None:
        started_at = _utcnow()
        finished_at = _utcnow()
        details = {"resume_from_run_id": resumable.get("run_id"), "resume_from_status": resumable.get("status"), "table_name": table_name}
        _record_progress_row(
            export_conn,
            run_id=run_id,
            source_signature=source_signature,
            step_name=step_name,
            step_kind="export",
            status=PROGRESS_STATUS_RESUMED,
            started_at=started_at,
            finished_at=finished_at,
            row_count=int(resumable.get("row_count") or 0),
            max_trade_date=_normalize_progress_trade_date(resumable.get("max_trade_date")),
            details=details,
        )
        _update_progress_payload(
            progress_payload,
            step_name=step_name,
            step_kind="export",
            status=PROGRESS_STATUS_RESUMED,
            started_at=started_at,
            finished_at=finished_at,
            row_count=int(resumable.get("row_count") or 0),
            max_trade_date=_normalize_progress_trade_date(resumable.get("max_trade_date")),
            details=details,
        )
        _write_progress_payload(progress_path, progress_payload)
        return {"step_name": step_name, "table_name": table_name, "inserted": 0, "updated": 0, "deleted": 0, "resumed": True, "row_count": int(resumable.get("row_count") or 0), "max_trade_date": _normalize_progress_trade_date(resumable.get("max_trade_date"))}

    started_at = _utcnow()
    _record_progress_row(
        export_conn,
        run_id=run_id,
        source_signature=source_signature,
        step_name=step_name,
        step_kind="export",
        status=PROGRESS_STATUS_RUNNING,
        started_at=started_at,
        finished_at=None,
        row_count=None,
        max_trade_date=None,
        details={"table_name": table_name},
    )
    _update_progress_payload(
        progress_payload,
        step_name=step_name,
        step_kind="export",
        status=PROGRESS_STATUS_RUNNING,
        started_at=started_at,
        finished_at=None,
        row_count=None,
        max_trade_date=None,
        details={"table_name": table_name},
    )
    _write_progress_payload(progress_path, progress_payload)

    rows = builder(source_conn, run_id)
    existing_hashes = _existing_hashes(export_conn, table_name, key_columns)
    source_keys = {tuple(row[key] for key in key_columns) for row in rows}
    inserted = 0
    updated = 0
    changed_rows: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(row[key_column] for key_column in key_columns)
        existing_hash = existing_hashes.get(key)
        if existing_hash is None:
            inserted += 1
            changed_rows.append(row)
        elif existing_hash != row["row_hash"]:
            updated += 1
            changed_rows.append(row)
    deleted = _delete_missing_rows(export_conn, table_name, key_columns, source_keys)
    _upsert_rows(export_conn, table_name, changed_rows)
    row_summary = _step_row_summary(rows, max_field if max_field else None)
    finished_at = _utcnow()
    details = {"table_name": table_name, "inserted": inserted, "updated": updated, "deleted": deleted, "changed_rows": len(changed_rows)}
    _record_progress_row(
        export_conn,
        run_id=run_id,
        source_signature=source_signature,
        step_name=step_name,
        step_kind="export",
        status=PROGRESS_STATUS_COMPLETE,
        started_at=started_at,
        finished_at=finished_at,
        row_count=int(row_summary["row_count"]),
        max_trade_date=_normalize_progress_trade_date(row_summary.get("max_trade_date")),
        details=details,
    )
    _update_progress_payload(
        progress_payload,
        step_name=step_name,
        step_kind="export",
        status=PROGRESS_STATUS_COMPLETE,
        started_at=started_at,
        finished_at=finished_at,
        row_count=int(row_summary["row_count"]),
        max_trade_date=_normalize_progress_trade_date(row_summary.get("max_trade_date")),
        details=details,
    )
    _write_progress_payload(progress_path, progress_payload)
    return {"step_name": step_name, "table_name": table_name, "inserted": inserted, "updated": updated, "deleted": deleted, "resumed": False, "row_count": int(row_summary["row_count"]), "max_trade_date": _normalize_progress_trade_date(row_summary.get("max_trade_date"))}


def _write_meta_export_run(
    export_conn,
    *,
    export_run_id: str,
    started_at: datetime,
    source_path: Path,
    signature: str,
    source_max_trade_date: int | None,
    source_row_counts: dict[str, int],
    changed_table_names: list[str],
    diff_reason: dict[str, dict[str, Any]],
) -> None:
    finished_at = _utcnow()
    export_conn.execute(
        """
        INSERT OR REPLACE INTO meta_export_runs (
            run_id, started_at, finished_at, status, source_db_path, source_signature,
            source_max_trade_date, source_row_counts, changed_table_names, diff_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            export_run_id,
            started_at,
            finished_at,
            "success",
            str(source_path),
            signature,
            source_max_trade_date,
            json.dumps(source_row_counts, ensure_ascii=False, sort_keys=True),
            json.dumps(changed_table_names, ensure_ascii=False),
            json.dumps(diff_reason, ensure_ascii=False, sort_keys=True),
        ],
    )


def run_diff_export(source_db_path: str | None = None, export_db_path: str | None = None) -> dict[str, Any]:
    source_path = resolve_source_db_path(source_db_path)
    export_path = resolve_export_db_path(export_db_path)
    export_run_id = _make_run_id()
    started_at = _utcnow()
    progress_path = _resolve_snapshot_progress_path(export_path)
    source_conn = connect_source_db(str(source_path))
    export_conn = connect_export_db(str(export_path))
    progress_payload: dict[str, Any] | None = None
    current_step: str | None = None
    try:
        ensure_export_schema(export_conn)
        source_row_counts = _collect_source_row_counts(source_conn)
        max_trade_row = source_conn.execute("SELECT MAX(date) FROM daily_bars").fetchone() if source_table_exists(source_conn, "daily_bars") else None
        source_max_trade_date = normalize_market_date(max_trade_row[0]) if max_trade_row and max_trade_row[0] is not None else None
        source_signature = _source_signature(source_row_counts, source_max_trade_date)
        source_progress_rows = _collect_source_progress_rows(source_conn)
        latest_progress = _load_latest_step_progress(export_conn, source_signature)

        progress_payload = _build_progress_payload(
            source_db_path=str(source_path),
            export_db_path=str(export_path),
            run_id=export_run_id,
            source_signature=source_signature,
            started_at=started_at.isoformat(),
        )
        _write_progress_payload(progress_path, progress_payload)
        _run_source_progress_steps(
            export_conn=export_conn,
            progress_payload=progress_payload,
            progress_path=progress_path,
            run_id=export_run_id,
            source_signature=source_signature,
            source_progress_rows=source_progress_rows,
        )

        changed_table_names: list[str] = []
        diff_reason: dict[str, dict[str, Any]] = {}
        for step_spec in EXPORT_STEP_SPECS:
            current_step = str(step_spec["step_name"])
            step_result = _run_export_step(
                source_conn=source_conn,
                export_conn=export_conn,
                progress_payload=progress_payload,
                progress_path=progress_path,
                run_id=export_run_id,
                source_signature=source_signature,
                latest_progress=latest_progress,
                step_spec=step_spec,
            )
            if int(step_result["inserted"]) > 0 or int(step_result["updated"]) > 0 or int(step_result["deleted"]) > 0:
                changed_table_names.append(str(step_result["table_name"]))
            diff_reason[str(step_result["table_name"])] = {
                "inserted": int(step_result["inserted"]),
                "updated": int(step_result["updated"]),
                "deleted": int(step_result["deleted"]),
                "resumed": bool(step_result["resumed"]),
                "row_count": int(step_result["row_count"]),
                "max_trade_date": _normalize_progress_trade_date(step_result.get("max_trade_date")),
            }

        current_step = "meta_export_runs"
        meta_started_at = _utcnow()
        _record_progress_row(
            export_conn,
            run_id=export_run_id,
            source_signature=source_signature,
            step_name="meta_export_runs",
            step_kind="export",
            status=PROGRESS_STATUS_RUNNING,
            started_at=meta_started_at,
            finished_at=None,
            row_count=None,
            max_trade_date=source_max_trade_date,
            details={"table_name": "meta_export_runs"},
        )
        _update_progress_payload(progress_payload, step_name="meta_export_runs", step_kind="export", status=PROGRESS_STATUS_RUNNING, started_at=meta_started_at, finished_at=None, row_count=None, max_trade_date=source_max_trade_date, details={"table_name": "meta_export_runs"})
        _write_progress_payload(progress_path, progress_payload)
        _write_meta_export_run(
            export_conn,
            export_run_id=export_run_id,
            started_at=started_at,
            source_path=source_path,
            signature=source_signature,
            source_max_trade_date=source_max_trade_date,
            source_row_counts=source_row_counts,
            changed_table_names=changed_table_names,
            diff_reason=diff_reason,
        )
        meta_finished_at = _utcnow()
        _record_progress_row(
            export_conn,
            run_id=export_run_id,
            source_signature=source_signature,
            step_name="meta_export_runs",
            step_kind="export",
            status=PROGRESS_STATUS_COMPLETE,
            started_at=meta_started_at,
            finished_at=meta_finished_at,
            row_count=1,
            max_trade_date=source_max_trade_date,
            details={"table_name": "meta_export_runs", "status": "success"},
        )
        _update_progress_payload(progress_payload, step_name="meta_export_runs", step_kind="export", status=PROGRESS_STATUS_COMPLETE, started_at=meta_started_at, finished_at=meta_finished_at, row_count=1, max_trade_date=source_max_trade_date, details={"table_name": "meta_export_runs", "status": "success"})
        export_conn.execute("CHECKPOINT")
        _set_progress_complete(progress_path, progress_payload)
        return {
            "ok": True,
            "run_id": export_run_id,
            "source_db_path": str(source_path),
            "source_signature": source_signature,
            "source_max_trade_date": source_max_trade_date,
            "changed_table_names": changed_table_names,
            "diff_reason": diff_reason,
            "progress_path": str(progress_path),
        }
    except Exception:
        if progress_payload is not None:
            _set_progress_failure(progress_path, progress_payload, reason_code="export_failed", current_step=current_step)
        raise
    finally:
        source_conn.close()
        export_conn.close()
