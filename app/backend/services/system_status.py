from __future__ import annotations

import json
import os
import logging
from datetime import datetime

from app.core.config import UPDATE_STATE_PATH
from app.db.session import get_conn

logger = logging.getLogger(__name__)
_REQUIRED_TABLES = [
    "tickers",
    "daily_bars",
    "monthly_bars",
    "daily_ma",
    "monthly_ma",
    "trade_events",
    "positions_live",
    "position_rounds",
    "initial_positions_seed",
]


def _list_tables(conn) -> set[str]:
    rows = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    return {row[0] for row in rows}


def _collect_db_stats() -> dict:
    stats = {
        "tickers": None,
        "daily_rows": None,
        "monthly_rows": None,
        "trade_events": None,
        "positions_live": None,
        "position_rounds": None,
        "missing_tables": [],
        "errors": [],
    }
    try:
        with get_conn() as conn:
            tables = _list_tables(conn)
            stats["missing_tables"] = [name for name in _REQUIRED_TABLES if name not in tables]
            if "tickers" in tables:
                stats["tickers"] = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
            if "daily_bars" in tables:
                stats["daily_rows"] = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
            if "monthly_bars" in tables:
                stats["monthly_rows"] = conn.execute("SELECT COUNT(*) FROM monthly_bars").fetchone()[0]
            if "trade_events" in tables:
                stats["trade_events"] = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]
            if "positions_live" in tables:
                stats["positions_live"] = conn.execute("SELECT COUNT(*) FROM positions_live").fetchone()[0]
            if "position_rounds" in tables:
                stats["position_rounds"] = conn.execute("SELECT COUNT(*) FROM position_rounds").fetchone()[0]
    except Exception as exc:
        stats["errors"].append(str(exc))
    return stats


def _collect_db_readiness() -> dict:
    state = {
        "missing_tables": [],
        "errors": [],
    }
    try:
        with get_conn() as conn:
            tables = _list_tables(conn)
            state["missing_tables"] = [name for name in _REQUIRED_TABLES if name not in tables]
    except Exception as exc:
        state["errors"].append(str(exc))
    return state


def _get_last_updated_timestamp() -> str:
    try:
        if os.path.isfile(UPDATE_STATE_PATH):
            with open(UPDATE_STATE_PATH, "r", encoding="utf-8") as handle:
                state = json.load(handle)
                return state.get("last_txt_update_at")
    except Exception as exc:
        logger.warning("Failed to read update state (%s): %s", UPDATE_STATE_PATH, exc)
    return datetime.now().isoformat()
