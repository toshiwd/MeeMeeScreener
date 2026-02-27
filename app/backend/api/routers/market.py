from __future__ import annotations

from typing import Any
from datetime import datetime, timezone
import logging
import os
from urllib.parse import quote

import duckdb
from fastapi import APIRouter, Query

from app.backend.core.config import config

router = APIRouter(prefix="/api/market", tags=["market"])
logger = logging.getLogger(__name__)

_SECTOR_FALLBACK = [
    {"sector33_code": "01", "name": "水産・農林業"},
    {"sector33_code": "02", "name": "鉱業"},
    {"sector33_code": "03", "name": "建設業"},
    {"sector33_code": "04", "name": "食料品"},
    {"sector33_code": "05", "name": "繊維製品"},
    {"sector33_code": "06", "name": "パルプ・紙"},
    {"sector33_code": "07", "name": "化学"},
    {"sector33_code": "08", "name": "医薬品"},
    {"sector33_code": "09", "name": "石油・石炭製品"},
    {"sector33_code": "10", "name": "ゴム製品"},
    {"sector33_code": "11", "name": "ガラス・土石製品"},
    {"sector33_code": "12", "name": "鉄鋼"},
    {"sector33_code": "13", "name": "非鉄金属"},
    {"sector33_code": "14", "name": "金属製品"},
    {"sector33_code": "15", "name": "機械"},
    {"sector33_code": "16", "name": "電気機器"},
    {"sector33_code": "17", "name": "輸送用機器"},
    {"sector33_code": "18", "name": "精密機器"},
    {"sector33_code": "19", "name": "その他製品"},
    {"sector33_code": "20", "name": "電気・ガス業"},
    {"sector33_code": "21", "name": "陸運業"},
    {"sector33_code": "22", "name": "海運業"},
    {"sector33_code": "23", "name": "空運業"},
    {"sector33_code": "24", "name": "倉庫・運輸関連業"},
    {"sector33_code": "25", "name": "情報・通信業"},
    {"sector33_code": "26", "name": "卸売業"},
    {"sector33_code": "27", "name": "小売業"},
    {"sector33_code": "28", "name": "銀行業"},
    {"sector33_code": "29", "name": "証券・商品先物取引業"},
    {"sector33_code": "30", "name": "保険業"},
    {"sector33_code": "31", "name": "その他金融業"},
    {"sector33_code": "32", "name": "不動産業"},
    {"sector33_code": "33", "name": "サービス業"},
]

_OFFSET_MAP = {"1d": 2, "1w": 6, "1m": 21}


def _build_heatmap_item(
    sector33_code: str,
    name: str,
    weight: float,
    value: float,
    ticker_count: int,
    period: str,
) -> dict[str, Any]:
    sector_param = quote(sector33_code) if sector33_code else ""
    period_param = quote(period)
    detail_route = f"/?sector={sector_param}&period={period_param}" if sector_param else f"/?period={period_param}"
    return {
        "sector33_code": sector33_code,
        "name": name,
        "weight": weight,
        "value": value,
        # Backward-compatible alias used by older clients/selftest.
        "color": value,
        "tickerCount": ticker_count,
        # Backward-compatible alias used by older clients.
        "count": ticker_count,
        "detailRoute": detail_route,
    }


def _build_default_payload(period: str) -> list[dict[str, Any]]:
    return [
        _build_heatmap_item(item["sector33_code"], item["name"], 0, 0, 0, period)
        for item in _SECTOR_FALLBACK
    ]


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def _resolve_price_table(conn: duckdb.DuckDBPyConnection) -> tuple[str, str, str] | None:
    if _table_exists(conn, "stock_prices"):
        return ("stock_prices", "close", "volume")
    if _table_exists(conn, "daily_bars"):
        return ("daily_bars", "c", "v")
    return None


def _has_industry_master(conn: duckdb.DuckDBPyConnection) -> bool:
    if not _table_exists(conn, "industry_master"):
        return False
    row = conn.execute("SELECT COUNT(*) FROM industry_master").fetchone()
    return bool(row and row[0] > 0)


def _get_table_count(conn: duckdb.DuckDBPyConnection, name: str) -> int:
    if not _table_exists(conn, name):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
    return int(row[0] or 0) if row else 0


def _build_heatmap_diagnostics(period: str) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {
        "industry_master_present": False,
        "industry_master_rows": 0,
        "tickers_rows": 0,
        "computed_from": "fallback",
        "period": period,
    }
    db_path = str(config.DB_PATH)
    offset = _OFFSET_MAP.get(period)
    if not offset:
        return diagnostics
    try:
        with duckdb.connect(db_path) as conn:
            diagnostics["industry_master_present"] = _has_industry_master(conn)
            diagnostics["industry_master_rows"] = _get_table_count(conn, "industry_master")
            table_info = _resolve_price_table(conn)
            if table_info:
                table_name, _, _ = table_info
                diagnostics["tickers_rows"] = _get_table_count(conn, table_name)
    except Exception:
        logger.exception("heatmap diagnostics failed")
    return diagnostics


def _fetch_heatmap(period: str) -> list[dict[str, Any]] | None:
    db_path = str(config.DB_PATH)
    offset = _OFFSET_MAP.get(period)
    if not offset:
        return None
    try:
        with duckdb.connect(db_path) as conn:
            if not _has_industry_master(conn):
                logger.warning("industry_master missing or empty; returning fallback heatmap")
                return None
            table_info = _resolve_price_table(conn)
            if not table_info:
                logger.warning("heatmap price table missing; returning fallback heatmap")
                return None
            table_name, close_col, volume_col = table_info
            rows = conn.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        code,
                        date,
                        {close_col} AS close,
                        {volume_col} AS volume,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
                    FROM {table_name}
                ),
                latest AS (
                    SELECT code, close AS latest_close, volume AS latest_volume
                    FROM ranked
                    WHERE rn = 1
                ),
                past AS (
                    SELECT code, close AS past_close
                    FROM ranked
                    WHERE rn = ?
                )
                SELECT
                    im.sector33_code AS sector33_code,
                    im.sector33_name AS name,
                    SUM(COALESCE(latest.latest_close, 0) * COALESCE(latest.latest_volume, 0)) AS weight,
                    AVG(((latest.latest_close - past.past_close) / NULLIF(past.past_close, 0)) * 100) AS value,
                    COUNT(*) AS ticker_count
                FROM latest
                JOIN past ON past.code = latest.code
                JOIN industry_master im ON im.code = latest.code
                GROUP BY im.sector33_code, im.sector33_name
                ORDER BY weight DESC
                """,
                [offset],
            ).fetchall()
            return [
                _build_heatmap_item(
                    row[0],
                    row[1],
                    float(row[2] or 0),
                    float(row[3] or 0),
                    int(row[4] or 0),
                    period,
                )
                for row in rows
            ]
    except Exception as exc:
        logger.exception("heatmap fetch failed: %s", exc)
        return None


def _format_timeline_label(period: str, asof_ts: int) -> str:
    dt = datetime.fromtimestamp(asof_ts, tz=timezone.utc)
    if period == "1m":
        return dt.strftime("%Y-%m")
    return dt.strftime("%Y-%m-%d")


def _fetch_heatmap_timeline(period: str, limit: int) -> list[dict[str, Any]] | None:
    db_path = str(config.DB_PATH)
    if period not in ("1d", "1w", "1m"):
        return None
    safe_limit = max(1, min(int(limit or 0), 400))
    try:
        with duckdb.connect(db_path) as conn:
            if not _has_industry_master(conn):
                logger.warning("industry_master missing or empty; returning fallback heatmap timeline")
                return None

            if period == "1m":
                has_monthly = _table_exists(conn, "monthly_bars")
                if has_monthly:
                    source_sql = """
                        SELECT
                            code,
                            CASE
                                WHEN month BETWEEN 100000 AND 999912 THEN strptime(CAST(month AS VARCHAR), '%Y%m')
                                ELSE to_timestamp(month)
                            END AS asof_dt,
                            c AS close,
                            v AS volume,
                            c * v AS weight
                        FROM monthly_bars
                    """
                else:
                    source_sql = """
                        WITH base AS (
                            SELECT
                                code,
                                CASE
                                    WHEN date BETWEEN 10000000 AND 99991231 THEN strptime(CAST(date AS VARCHAR), '%Y%m%d')
                                    ELSE to_timestamp(date)
                                END AS dt,
                                c,
                                v
                            FROM daily_bars
                        )
                        SELECT
                            code,
                            date_trunc('month', dt) AS asof_dt,
                            arg_max(c, dt) AS close,
                            SUM(v) AS volume,
                            SUM(c * v) AS weight
                        FROM base
                        GROUP BY code, asof_dt
                    """
            elif period == "1w":
                source_sql = """
                    WITH base AS (
                        SELECT
                            code,
                            CASE
                                WHEN date BETWEEN 10000000 AND 99991231 THEN strptime(CAST(date AS VARCHAR), '%Y%m%d')
                                ELSE to_timestamp(date)
                            END AS dt,
                            c,
                            v
                        FROM daily_bars
                    )
                    SELECT
                        code,
                        date_trunc('week', dt) AS week_start,
                        max(dt) AS asof_dt,
                        arg_max(c, dt) AS close,
                        SUM(v) AS volume,
                        SUM(c * v) AS weight
                    FROM base
                    GROUP BY code, week_start
                """
            else:
                source_sql = """
                    SELECT
                        code,
                        CASE
                            WHEN date BETWEEN 10000000 AND 99991231 THEN strptime(CAST(date AS VARCHAR), '%Y%m%d')
                            ELSE to_timestamp(date)
                        END AS asof_dt,
                        c AS close,
                        v AS volume,
                        c * v AS weight
                    FROM daily_bars
                """

            if period == "1w":
                base_sql = f"""
                    WITH source AS ({source_sql}),
                    ordered AS (
                        SELECT
                            code,
                            asof_dt,
                            close,
                            volume,
                            weight,
                            LAG(close) OVER (PARTITION BY code ORDER BY asof_dt) AS prev_close
                        FROM source
                    ),
                    joined AS (
                        SELECT
                            o.asof_dt,
                            im.sector33_code AS sector33_code,
                            im.sector33_name AS name,
                            o.weight,
                            CASE
                                WHEN o.prev_close IS NULL OR o.prev_close = 0 THEN NULL
                                ELSE (o.close - o.prev_close) / o.prev_close * 100
                            END AS change_pct
                        FROM ordered o
                        JOIN industry_master im ON im.code = o.code
                    ),
                    agg AS (
                        SELECT
                            asof_dt,
                            sector33_code,
                            name,
                            SUM(weight) AS weight,
                            AVG(change_pct) AS value,
                            COUNT(*) AS ticker_count
                        FROM joined
                        GROUP BY 1,2,3
                    ),
                    frames AS (
                        SELECT DISTINCT asof_dt
                        FROM agg
                        ORDER BY asof_dt DESC
                        LIMIT ?
                    ),
                    sectors AS (
                        SELECT sector33_code, sector33_name AS name
                        FROM industry_master
                        GROUP BY 1,2
                    ),
                    full_grid AS (
                        SELECT
                            f.asof_dt,
                            epoch(f.asof_dt)::BIGINT AS asof_ts,
                            s.sector33_code,
                            s.name,
                            COALESCE(a.weight, 0) AS weight,
                            COALESCE(a.value, 0) AS value,
                            COALESCE(a.ticker_count, 0) AS ticker_count
                        FROM frames f
                        CROSS JOIN sectors s
                        LEFT JOIN agg a
                            ON a.asof_dt = f.asof_dt
                            AND a.sector33_code = s.sector33_code
                    )
                    SELECT
                        asof_ts,
                        sector33_code,
                        name,
                        weight,
                        value,
                        ticker_count,
                        weight - COALESCE(LAG(weight) OVER (PARTITION BY sector33_code ORDER BY asof_ts), 0) AS flow
                    FROM full_grid
                    ORDER BY asof_ts ASC, sector33_code ASC
                """
            else:
                base_sql = f"""
                    WITH source AS ({source_sql}),
                    ordered AS (
                        SELECT
                            code,
                            asof_dt,
                            close,
                            volume,
                            weight,
                            LAG(close) OVER (PARTITION BY code ORDER BY asof_dt) AS prev_close
                        FROM source
                    ),
                    joined AS (
                        SELECT
                            o.asof_dt,
                            im.sector33_code AS sector33_code,
                            im.sector33_name AS name,
                            o.weight,
                            CASE
                                WHEN o.prev_close IS NULL OR o.prev_close = 0 THEN NULL
                                ELSE (o.close - o.prev_close) / o.prev_close * 100
                            END AS change_pct
                        FROM ordered o
                        JOIN industry_master im ON im.code = o.code
                    ),
                    agg AS (
                        SELECT
                            asof_dt,
                            sector33_code,
                            name,
                            SUM(weight) AS weight,
                            AVG(change_pct) AS value,
                            COUNT(*) AS ticker_count
                        FROM joined
                        GROUP BY 1,2,3
                    ),
                    frames AS (
                        SELECT DISTINCT asof_dt
                        FROM agg
                        ORDER BY asof_dt DESC
                        LIMIT ?
                    ),
                    sectors AS (
                        SELECT sector33_code, sector33_name AS name
                        FROM industry_master
                        GROUP BY 1,2
                    ),
                    full_grid AS (
                        SELECT
                            f.asof_dt,
                            epoch(f.asof_dt)::BIGINT AS asof_ts,
                            s.sector33_code,
                            s.name,
                            COALESCE(a.weight, 0) AS weight,
                            COALESCE(a.value, 0) AS value,
                            COALESCE(a.ticker_count, 0) AS ticker_count
                        FROM frames f
                        CROSS JOIN sectors s
                        LEFT JOIN agg a
                            ON a.asof_dt = f.asof_dt
                            AND a.sector33_code = s.sector33_code
                    )
                    SELECT
                        asof_ts,
                        sector33_code,
                        name,
                        weight,
                        value,
                        ticker_count,
                        weight - COALESCE(LAG(weight) OVER (PARTITION BY sector33_code ORDER BY asof_ts), 0) AS flow
                    FROM full_grid
                    ORDER BY asof_ts ASC, sector33_code ASC
                """

            rows = conn.execute(base_sql, [safe_limit]).fetchall()
            if not rows:
                return []

            frames: dict[int, dict[str, Any]] = {}
            for row in rows:
                asof_ts = int(row[0] or 0)
                sector_code = row[1]
                name = row[2]
                weight = float(row[3] or 0)
                value = float(row[4] or 0)
                ticker_count = int(row[5] or 0)
                flow = float(row[6] or 0)

                frame = frames.get(asof_ts)
                if frame is None:
                    frame = {
                        "asof": asof_ts,
                        "label": _format_timeline_label(period, asof_ts),
                        "items": [],
                    }
                    frames[asof_ts] = frame
                frame["items"].append(
                    {
                        "sector33_code": sector_code,
                        "name": name,
                        "weight": weight,
                        "value": value,
                        "tickerCount": ticker_count,
                        "flow": flow,
                    }
                )

            ordered_frames = [frames[key] for key in sorted(frames.keys())]
            return ordered_frames
    except Exception as exc:
        logger.exception("heatmap timeline fetch failed: %s", exc)
        return None


@router.get("/heatmap/timeline")
def get_market_heatmap_timeline(
    period: str = Query("1d", pattern="^(1d|1w|1m)$"),
    limit: int = Query(180, ge=1, le=400),
):
    """
    Returns sector heatmap timeline data.
    Response format:
      {
        "frames": [
          {
            "asof": number,          # unix seconds
            "label": str,            # YYYY-MM-DD or YYYY-MM
            "items": [
              {
                "sector33_code": str,
                "name": str,
                "weight": number,
                "value": number,
                "tickerCount": number,
                "flow": number
              }
            ]
          }
        ],
        "period": str,
        "diagnostics": { ... } | null
      }
    """
    payload = _fetch_heatmap_timeline(period, limit)
    computed_from = "industry_master" if payload is not None else "fallback"
    if payload is None:
        payload = []
    diagnostics = (
        _build_heatmap_diagnostics(period)
        if os.getenv("MEEMEE_DEV", "").lower() in ("1", "true", "yes", "on")
        or os.getenv("MEEMEE_SELFTEST", "").lower() in ("1", "true", "yes", "on")
        else None
    )
    if diagnostics is not None:
        diagnostics["computed_from"] = computed_from
    return {"frames": payload, "period": period, "diagnostics": diagnostics}


@router.get("/heatmap")
def get_market_heatmap(period: str = Query("1d", pattern="^(1d|1w|1m)$")):
    """
    Returns sector heatmap data.
    Response format:
      {
        "items": [
          {
            "sector33_code": str,
            "name": str,
            "weight": number,        # proxy for total market value in the sector
            "value": number,         # average price change (%) over the selected period
            "tickerCount": number,   # number of tickers contributing to the sector
            "detailRoute": str       # front-end route to drill down into the sector
          },
          ...
        ],
        "period": str,
        "diagnostics": { ... } | null
      }
    """
    payload = _fetch_heatmap(period)
    computed_from = "industry_master" if payload else "fallback"
    if not payload:
        payload = _build_default_payload(period)
    diagnostics = (
        _build_heatmap_diagnostics(period)
        if os.getenv("MEEMEE_DEV", "").lower() in ("1", "true", "yes", "on")
        or os.getenv("MEEMEE_SELFTEST", "").lower() in ("1", "true", "yes", "on")
        else None
    )
    if diagnostics is not None:
        diagnostics["computed_from"] = computed_from
    return {"items": payload, "period": period, "diagnostics": diagnostics}
