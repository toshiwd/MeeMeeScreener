from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.contracts.paths import resolve_source_db_path
from external_analysis.exporter.export_schema import connect_export_db, ensure_export_schema
from external_analysis.exporter.source_reader import (
    connect_source_db,
    fetch_rows,
    normalize_market_date,
    source_column_exists,
    source_table_exists,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
            (
                "code",
                "dt",
                "atr14",
                "diff20_pct",
                "diff20_atr",
                "cnt_20_above",
                "cnt_7_above",
                "day_count",
                "candle_flags",
            ),
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
                    (
                        "code",
                        "trade_date",
                        "ma7",
                        "ma20",
                        "ma60",
                        "ma100",
                        "ma200",
                        "atr14",
                        "diff20_pct",
                        "diff20_atr",
                        "cnt_20_above",
                        "cnt_7_above",
                        "day_count",
                        "candle_flags",
                    ),
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
        (
            "symbol",
            "spot_qty",
            "margin_long_qty",
            "margin_short_qty",
            "buy_qty",
            "sell_qty",
            "opened_at",
            "updated_at",
            "has_issue",
            "issue_note",
        ),
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
                    (
                        "symbol",
                        "spot_qty",
                        "margin_long_qty",
                        "margin_short_qty",
                        "buy_qty",
                        "sell_qty",
                        "opened_at",
                        "updated_at",
                        "has_issue",
                        "issue_note",
                    ),
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


def run_diff_export(source_db_path: str | None = None, export_db_path: str | None = None) -> dict[str, Any]:
    source_path = resolve_source_db_path(source_db_path)
    export_run_id = _make_run_id()
    started_at = _utcnow()
    source_conn = connect_source_db(str(source_path))
    export_conn = connect_export_db(export_db_path)
    try:
        ensure_export_schema(export_conn)
        source_row_counts = {
            table_name: (
                int(source_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
                if source_table_exists(source_conn, table_name)
                else 0
            )
            for table_name in ("daily_bars", "monthly_bars", "daily_ma", "feature_snapshot_daily", "positions_live", "position_rounds")
        }
        max_trade_row = (
            source_conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
            if source_table_exists(source_conn, "daily_bars")
            else None
        )
        source_max_trade_date = normalize_market_date(max_trade_row[0]) if max_trade_row and max_trade_row[0] is not None else None
        export_payloads = {
            "bars_daily_export": (_build_daily_export_rows(source_conn, export_run_id), ("code", "trade_date")),
            "bars_monthly_export": (_build_monthly_export_rows(source_conn, export_run_id), ("code", "month_key")),
            "indicator_daily_export": (_build_indicator_export_rows(source_conn, export_run_id), ("code", "trade_date")),
            "pattern_state_export": (_build_pattern_export_rows(source_conn, export_run_id), ("code", "trade_date")),
            "ranking_snapshot_export": ([], ("trade_date", "code", "ranking_family")),
            "trade_event_export": (_build_trade_event_export_rows(source_conn, export_run_id), ("code", "event_ts", "event_seq")),
            "position_snapshot_export": (_build_position_export_rows(source_conn, export_run_id), ("code", "snapshot_at")),
        }
        changed_table_names: list[str] = []
        diff_reason: dict[str, dict[str, int]] = {}
        for table_name, (rows, key_columns) in export_payloads.items():
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
            if inserted or updated or deleted:
                changed_table_names.append(table_name)
            diff_reason[table_name] = {"inserted": inserted, "updated": updated, "deleted": deleted}
        finished_at = _utcnow()
        signature = _source_signature(source_row_counts, source_max_trade_date)
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
        export_conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "run_id": export_run_id,
            "source_db_path": str(source_path),
            "source_signature": signature,
            "source_max_trade_date": source_max_trade_date,
            "changed_table_names": changed_table_names,
            "diff_reason": diff_reason,
        }
    finally:
        source_conn.close()
        export_conn.close()
