from db import get_conn
from positions import parse_rakuten_csv, parse_sbi_csv, rebuild_positions, TradeEvent


def _insert_events(conn, events: list[TradeEvent]) -> int:
    if not events:
        return 0
    hashes = [event.source_row_hash for event in events]
    placeholders = ",".join(["?"] * len(hashes))
    existing = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM trade_events
        WHERE source_row_hash IN ({placeholders})
        """,
        hashes
    ).fetchone()[0]

    rows = [
        (
            event.broker,
            event.exec_dt,
            event.symbol,
            event.action,
            event.qty,
            event.price,
            event.source_row_hash,
            event.transaction_type,
            event.side_type,
            event.margin_type
        )
        for event in events
    ]
    conn.executemany(
        """
        INSERT INTO trade_events (
            broker,
            exec_dt,
            symbol,
            action,
            qty,
            price,
            source_row_hash,
            transaction_type,
            side_type,
            margin_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_row_hash) DO NOTHING
        """,
        rows
    )
    total = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM trade_events
        WHERE source_row_hash IN ({placeholders})
        """,
        hashes
    ).fetchone()[0]
    return max(0, int(total or 0) - int(existing or 0))


def _process_import(
    broker: str,
    events: list[TradeEvent],
    warnings: list[str],
    replace_existing: bool
) -> dict:
    with get_conn() as conn:
        if replace_existing:
            conn.execute("DELETE FROM trade_events")
        inserted = _insert_events(conn, events)
        rebuild_summary = rebuild_positions(conn)

    affected_symbols = sorted({event.symbol for event in events})
    return {
        "ok": True,
        "received": len(events),
        "inserted": inserted,
        "warnings": warnings,
        "affected": affected_symbols,
        "rebuild": rebuild_summary
    }


def process_import_rakuten(file_bytes: bytes, replace_existing: bool = True) -> dict:
    events, warnings = parse_rakuten_csv(file_bytes)
    return _process_import("rakuten", events, warnings, replace_existing)


def process_import_sbi(file_bytes: bytes, replace_existing: bool = True) -> dict:
    events, warnings = parse_sbi_csv(file_bytes)
    return _process_import("sbi", events, warnings, replace_existing)
