from __future__ import annotations

def get_events(conn, symbols: list[str] | None = None):
    """
    Return trade_events rows in a stable tuple shape.
    Migrated from app/services/trade_events.py
    """
    try:
        info = conn.execute("PRAGMA table_info('trade_events')").fetchall()
        columns = {row[1] for row in info}
    except Exception:
        columns = set()

    symbol_col = "symbol" if "symbol" in columns else "code" if "code" in columns else None

    select_cols = [
        "id",
        "broker",
        "exec_dt",
        symbol_col or "symbol",
        "action",
        "qty",
        "price",
        "source_row_hash",
        "created_at",
        "transaction_type",
        "side_type",
        "margin_type",
    ]
    select_exprs = []
    for col in select_cols:
        if col and col in columns:
            select_exprs.append(col)
        elif col == symbol_col or (symbol_col is None and col == "symbol"):
            # Always expose a 'symbol' slot
            select_exprs.append("NULL AS symbol")
        else:
            select_exprs.append(f"NULL AS {col}")

    query = f"SELECT {', '.join(select_exprs)} FROM trade_events"
    params: list = []
    if symbols and symbol_col:
        placeholders = ",".join(["?"] * len(symbols))
        query += f" WHERE {symbol_col} IN ({placeholders})"
        params.extend(symbols)

    order_cols: list[str] = []
    if "exec_dt" in columns:
        order_cols.append("exec_dt ASC")
    if "created_at" in columns:
        order_cols.append("created_at ASC")
    if order_cols:
        query += " ORDER BY " + ", ".join(order_cols)
    return conn.execute(query, params).fetchall()
