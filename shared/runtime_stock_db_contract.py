from __future__ import annotations

from datetime import date
from functools import lru_cache
import os
from pathlib import Path
from typing import Any, Final, TypedDict

import duckdb

from shared.tradex_storage import tradex_db_path


APP_NAME: Final[str] = "MeeMeeScreener"
DEV_APP_SUFFIX: Final[str] = "MeeMeeScreener-dev"
RUNTIME_STOCK_DB_FILENAME: Final[str] = "stocks.duckdb"

RUNTIME_STOCK_DB_RESOLUTION_ORDER: Final[tuple[str, ...]] = (
    "STOCKS_DB_PATH",
    "TRADEX_LIVE_STOCKS_DB_PATH",
    "MEEMEE_DATA_DIR",
    "LOCALAPPDATA_DEV",
    "LOCALAPPDATA_LEGACY",
    "TRADEX_DEFAULT",
)


class RuntimeStockDBSelection(TypedDict):
    runtime_db_path: str
    resolution_source: str
    resolution_reason: str
    validated: bool
    db_exists: bool
    daily_bars_rows: int | None
    market_regime_daily_rows: int | None


class RuntimeStockDBFreshness(TypedDict):
    runtime_db_path: str
    resolution_source: str
    resolution_reason: str
    db_exists: bool
    daily_bars_rows: int
    market_regime_daily_rows: int
    latest_available_global_date: int | None
    latest_available_global_date_iso: str | None
    requested_symbol: str | None
    requested_symbol_latest_date: int | None
    requested_symbol_latest_date_iso: str | None
    requested_chart_date: int | None
    requested_chart_date_iso: str | None
    date_gap_days: int | None
    date_match_status: str
    source_freshness_status: str
    freshness_blocked: bool
    required_tables_present: bool
    required_tables: dict[str, bool]
    same_reference_db_path: bool | None
    reference_db_path: str | None


def _normalize_path(value: str | Path | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return str(Path(text).expanduser().resolve(strict=False))
    except Exception:
        return text


def _default_app_storage_name() -> str:
    if os.getenv("MEEMEE_SELFTEST", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"{APP_NAME}-selftest"
    if os.getenv("MEEMEE_DEV", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"{APP_NAME}-dev"
    if os.getenv("MEEMEE_DEV_MODE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"{APP_NAME}-dev"
    app_env = (os.getenv("APP_ENV") or os.getenv("ENV") or "dev").strip().lower()
    if app_env in {"dev", "development", "test"}:
        return f"{APP_NAME}-dev"
    return APP_NAME


def _local_appdata_root(app_name: str) -> Path:
    base = os.getenv("LOCALAPPDATA") or str(Path.home())
    return Path(base) / app_name / "data"


def _candidate_paths() -> list[tuple[str, Path, bool]]:
    candidates: list[tuple[str, Path, bool]] = []

    explicit = _normalize_path(os.getenv("STOCKS_DB_PATH"))
    if explicit:
        candidates.append(("STOCKS_DB_PATH", Path(explicit), True))
        return candidates

    live_override = _normalize_path(os.getenv("TRADEX_LIVE_STOCKS_DB_PATH"))
    if live_override:
        candidates.append(("TRADEX_LIVE_STOCKS_DB_PATH", Path(live_override), True))
        return candidates

    meemee_data_dir_env = _normalize_path(os.getenv("MEEMEE_DATA_DIR"))
    if meemee_data_dir_env:
        candidates.append(("MEEMEE_DATA_DIR", Path(meemee_data_dir_env) / RUNTIME_STOCK_DB_FILENAME, False))
    else:
        candidates.append(("LOCALAPPDATA_DEV", _local_appdata_root(DEV_APP_SUFFIX) / RUNTIME_STOCK_DB_FILENAME, False))
    candidates.extend(
        [
            ("LOCALAPPDATA_LEGACY", _local_appdata_root(APP_NAME) / RUNTIME_STOCK_DB_FILENAME, False),
            ("TRADEX_DEFAULT", tradex_db_path(RUNTIME_STOCK_DB_FILENAME), False),
        ]
    )
    return candidates


def _table_names(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    return {str(row[0]).strip() for row in rows if str(row[0]).strip()}


def _table_count(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _latest_ymd_from_daily_bars(conn: duckdb.DuckDBPyConnection, *, code: str | None = None) -> int | None:
    where_clause = "WHERE code = ?" if code else ""
    params = [str(code)] if code else []
    row = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd
            FROM daily_bars
            {where_clause}
        )
        SELECT MAX(ymd) FROM normalized
        """,
        params,
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _ymd_to_date(value: int | None) -> date | None:
    if value is None:
        return None
    text = str(int(value)).strip()
    if len(text) != 8:
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except Exception:
        return None


def _ymd_to_iso(value: int | None) -> str | None:
    resolved = _ymd_to_date(value)
    return None if resolved is None else resolved.isoformat()


def _match_status(*, requested_chart_date: int | None, latest_available_date: int | None, symbol_latest_date: int | None) -> tuple[str, str, bool, int | None]:
    if requested_chart_date is None:
        return "exact", "exact", False, None
    if latest_available_date is None or symbol_latest_date is None:
        return "blocked", "stale_blocking", True, None

    requested = _ymd_to_date(requested_chart_date)
    source = _ymd_to_date(latest_available_date)
    symbol = _ymd_to_date(symbol_latest_date)
    if requested is None or source is None or symbol is None:
        return "blocked", "stale_blocking", True, None

    if symbol < requested:
        gap_days = (requested - symbol).days
        return "lagged_provisional", "lagged", True, gap_days

    gap_days = (requested - source).days
    return "exact", "exact", False, gap_days


def _inspect_contract_tables(db_path: Path) -> tuple[bool, int, int, bool]:
    if not db_path.exists():
        return False, 0, 0, False
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return False, 0, 0, False
    try:
        table_names = _table_names(conn)
        daily_exists = "daily_bars" in table_names
        regime_exists = "market_regime_daily" in table_names
        daily_rows = _table_count(conn, "daily_bars") if daily_exists else 0
        regime_rows = _table_count(conn, "market_regime_daily") if regime_exists else 0
        return daily_exists and regime_exists and daily_rows > 0 and regime_rows > 0, daily_rows, regime_rows, True
    finally:
        conn.close()


@lru_cache(maxsize=1)
def resolve_runtime_stock_db_selection() -> RuntimeStockDBSelection:
    candidates = _candidate_paths()
    for source, candidate_path, explicit in candidates:
        normalized = _normalize_path(candidate_path)
        if normalized is None:
            continue
        path = Path(normalized)
        if explicit:
            valid, daily_rows, regime_rows, exists = _inspect_contract_tables(path)
            return RuntimeStockDBSelection(
                runtime_db_path=str(path),
                resolution_source=source,
                resolution_reason="explicit_runtime_override" if source == "STOCKS_DB_PATH" else "explicit_legacy_live_override",
                validated=valid,
                db_exists=exists,
                daily_bars_rows=daily_rows if valid else None,
                market_regime_daily_rows=regime_rows if valid else None,
            )
        valid, daily_rows, regime_rows, db_exists = _inspect_contract_tables(path)
        if valid:
            return RuntimeStockDBSelection(
                runtime_db_path=str(path),
                resolution_source=source,
                resolution_reason="validated_runtime_fallback",
                validated=True,
                db_exists=db_exists,
                daily_bars_rows=daily_rows,
                market_regime_daily_rows=regime_rows,
            )

    fallback = Path(_normalize_path(tradex_db_path(RUNTIME_STOCK_DB_FILENAME)) or tradex_db_path(RUNTIME_STOCK_DB_FILENAME))
    return RuntimeStockDBSelection(
        runtime_db_path=str(fallback),
        resolution_source="TRADEX_DEFAULT",
        resolution_reason="unresolved_default_fallback",
        validated=False,
        db_exists=fallback.exists(),
        daily_bars_rows=None,
        market_regime_daily_rows=None,
    )


def resolve_runtime_stock_db_path() -> Path:
    return Path(resolve_runtime_stock_db_selection()["runtime_db_path"]).expanduser().resolve(strict=False)


def inspect_runtime_stock_db(
    *,
    runtime_db_path: str | Path | None = None,
    requested_symbol: str | None = None,
    requested_chart_date: int | str | None = None,
    reference_db_path: str | Path | None = None,
) -> RuntimeStockDBFreshness:
    selection = resolve_runtime_stock_db_selection() if runtime_db_path is None else RuntimeStockDBSelection(
        runtime_db_path=str(Path(str(runtime_db_path)).expanduser().resolve(strict=False)),
        resolution_source="explicit_argument",
        resolution_reason="explicit_runtime_argument",
        validated=Path(str(runtime_db_path)).expanduser().resolve(strict=False).exists(),
        db_exists=Path(str(runtime_db_path)).expanduser().resolve(strict=False).exists(),
        daily_bars_rows=None,
        market_regime_daily_rows=None,
    )
    path = Path(selection["runtime_db_path"])
    requested_symbol_text = str(requested_symbol or "").strip() or None
    requested_chart_date_int = None
    if requested_chart_date is not None:
        try:
            requested_chart_date_int = int(str(requested_chart_date).replace("-", ""))
        except Exception:
            requested_chart_date_int = None

    freshness: RuntimeStockDBFreshness = RuntimeStockDBFreshness(
        runtime_db_path=str(path),
        resolution_source=str(selection["resolution_source"]),
        resolution_reason=str(selection["resolution_reason"]),
        db_exists=bool(selection["db_exists"]),
        daily_bars_rows=int(selection["daily_bars_rows"]) if isinstance(selection.get("daily_bars_rows"), int) else 0,
        market_regime_daily_rows=int(selection["market_regime_daily_rows"]) if isinstance(selection.get("market_regime_daily_rows"), int) else 0,
        latest_available_global_date=None,
        latest_available_global_date_iso=None,
        requested_symbol=requested_symbol_text,
        requested_symbol_latest_date=None,
        requested_symbol_latest_date_iso=None,
        requested_chart_date=requested_chart_date_int,
        requested_chart_date_iso=_ymd_to_iso(requested_chart_date_int),
        date_gap_days=None,
        date_match_status="blocked",
        source_freshness_status="stale_blocking",
        freshness_blocked=True,
        required_tables_present=False,
        required_tables={"daily_bars": False, "market_regime_daily": False},
        same_reference_db_path=None,
        reference_db_path=str(Path(str(reference_db_path)).expanduser().resolve(strict=False)) if reference_db_path else None,
    )

    if not path.exists():
        return freshness

    try:
        conn = duckdb.connect(str(path), read_only=True)
    except Exception:
        return freshness

    try:
        table_names = _table_names(conn)
        daily_exists = "daily_bars" in table_names
        regime_exists = "market_regime_daily" in table_names
        freshness["required_tables"] = {"daily_bars": daily_exists, "market_regime_daily": regime_exists}
        freshness["required_tables_present"] = daily_exists and regime_exists
        daily_rows = _table_count(conn, "daily_bars") if daily_exists else 0
        regime_rows = _table_count(conn, "market_regime_daily") if regime_exists else 0
        freshness["daily_bars_rows"] = daily_rows
        freshness["market_regime_daily_rows"] = regime_rows
        latest_global = _latest_ymd_from_daily_bars(conn)
        latest_symbol = _latest_ymd_from_daily_bars(conn, code=requested_symbol_text) if requested_symbol_text else latest_global
        freshness["latest_available_global_date"] = latest_global
        freshness["latest_available_global_date_iso"] = _ymd_to_iso(latest_global)
        freshness["requested_symbol_latest_date"] = latest_symbol
        freshness["requested_symbol_latest_date_iso"] = _ymd_to_iso(latest_symbol)
        date_match_status, source_status, blocked, gap_days = _match_status(
            requested_chart_date=requested_chart_date_int,
            latest_available_date=latest_global,
            symbol_latest_date=latest_symbol,
        )
        freshness["date_match_status"] = date_match_status
        freshness["source_freshness_status"] = source_status
        freshness["freshness_blocked"] = blocked or not freshness["required_tables_present"] or daily_rows <= 0 or regime_rows <= 0
        freshness["date_gap_days"] = gap_days
        if freshness["reference_db_path"] is not None:
            freshness["same_reference_db_path"] = str(path) == str(freshness["reference_db_path"])
        else:
            freshness["same_reference_db_path"] = None
    finally:
        conn.close()

    if freshness["freshness_blocked"]:
        return freshness

    freshness["source_freshness_status"] = "exact" if freshness["date_match_status"] == "exact" else "lagged"
    return freshness
