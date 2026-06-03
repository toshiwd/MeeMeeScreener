try:
    from app.db.session import try_get_conn
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from db import try_get_conn  # type: ignore

try:
    from app.backend.positions import TradeEvent, parse_rakuten_csv, parse_sbi_csv, rebuild_positions
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from positions import TradeEvent, parse_rakuten_csv, parse_sbi_csv, rebuild_positions  # type: ignore

try:
    from app.backend.services.operator_mutation_lock import OperatorMutationBusyError, operator_mutation_scope
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    OperatorMutationBusyError = RuntimeError  # type: ignore
    operator_mutation_scope = None  # type: ignore


class TradeImportBusyError(RuntimeError):
    def __init__(self, message: str, *, retry_after_ms: int = 1000) -> None:
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


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
    if not events:
        return {
            "ok": False,
            "received": 0,
            "inserted": 0,
            "warnings": warnings,
            "affected": [],
            "rebuild": None,
        }

    broker_key = str(broker or "").strip().lower()
    try:
        scope = (
            operator_mutation_scope("trade_history_import", timeout_sec=0.0)
            if operator_mutation_scope is not None
            else None
        )
        if scope is None:
            conn_scope = try_get_conn(timeout_sec=2.5)
            with conn_scope as conn:
                if conn is None:
                    raise TradeImportBusyError("database is temporarily busy")
                if replace_existing:
                    conn.execute("DELETE FROM trade_events WHERE lower(broker) = ?", [broker_key])
                inserted = _insert_events(conn, events)
                rebuild_summary = rebuild_positions(conn)
        else:
            with scope:
                conn_scope = try_get_conn(timeout_sec=2.5)
                with conn_scope as conn:
                    if conn is None:
                        raise TradeImportBusyError("database is temporarily busy")
                    if replace_existing:
                        conn.execute("DELETE FROM trade_events WHERE lower(broker) = ?", [broker_key])
                    inserted = _insert_events(conn, events)
                    rebuild_summary = rebuild_positions(conn)
    except OperatorMutationBusyError as exc:
        raise TradeImportBusyError("another import or update is already running") from exc

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
