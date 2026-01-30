from __future__ import annotations

from typing import Any

import duckdb
from fastapi import APIRouter, Query

from app.backend.core.config import config

router = APIRouter(prefix="/api/market", tags=["market"])

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


def _build_default_payload() -> list[dict[str, Any]]:
    return [
        {
            "sector33_code": item["sector33_code"],
            "name": item["name"],
            "size": 0,
            "color": 0,
            "count": 0,
        }
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


def _fetch_heatmap(period: str) -> list[dict[str, Any]] | None:
    db_path = str(config.DB_PATH)
    offset = _OFFSET_MAP.get(period)
    if not offset:
        return None
    try:
        with duckdb.connect(db_path) as conn:
            if not _table_exists(conn, "industry_master"):
                return None
            table_info = _resolve_price_table(conn)
            if not table_info:
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
                    SUM(COALESCE(latest.latest_close, 0) * COALESCE(latest.latest_volume, 0)) AS size,
                    AVG(((latest.latest_close - past.past_close) / NULLIF(past.past_close, 0)) * 100) AS color,
                    COUNT(*) AS count
                FROM latest
                JOIN past ON past.code = latest.code
                JOIN industry_master im ON im.code = latest.code
                GROUP BY im.sector33_code, im.sector33_name
                ORDER BY size DESC
                """,
                [offset],
            ).fetchall()
            return [
                {
                    "sector33_code": row[0],
                    "name": row[1],
                    "size": float(row[2] or 0),
                    "color": float(row[3] or 0),
                    "count": int(row[4] or 0),
                }
                for row in rows
            ]
    except Exception:
        return None


@router.get("/heatmap")
def get_market_heatmap(period: str = Query("1d", pattern="^(1d|1w|1m)$")):
    """
    Returns sector heatmap data.
    Response format:
      [{ name: str, size: number, color: number, sector33_code: str, count: number }, ...]
    """
    payload = _fetch_heatmap(period)
    if not payload:
        payload = _build_default_payload()
    return {"items": payload, "period": period}
