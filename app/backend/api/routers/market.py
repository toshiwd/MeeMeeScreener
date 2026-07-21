from __future__ import annotations

from typing import Any
from copy import deepcopy
from datetime import datetime, timezone
import logging
import math
import os
import time
from urllib.parse import quote

import duckdb
from fastapi import APIRouter, HTTPException, Query, Response

from app.backend.core.config import config
from app.backend.services.market_baskets import get_market_basket_defs, market_basket_catalog
from app.backend.services.watchlist import load_watchlist_codes, resolve_watchlist_path
from app.db.session import try_get_conn_for_path

router = APIRouter(prefix="/api/market", tags=["market"])
logger = logging.getLogger(__name__)
_THEME_API_CACHE_TTL_SEC = 20.0
_THEME_API_CACHE_MAX_ENTRIES = 24
_THEME_API_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}

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

_LEGACY_THEME_DEFS: list[dict[str, Any]] = [
    {
        "theme_id": "semiconductor",
        "name": "半導体",
        "sector_codes": {"3650", "3600", "3750"},
        "codes": {
            "3436", "4063", "6146", "6266", "6323", "6525", "6613", "6723",
            "6728", "6857", "6890", "6920", "6963", "6976", "6981", "7735",
            "8035",
        },
        "keywords": ("半導体", "マイクロ", "エレクトロン", "レーザ", "シリコン"),
    },
    {
        "theme_id": "ai_datacenter",
        "name": "AI/データセンター",
        "sector_codes": {"3650", "5250"},
        "codes": {"3778", "3856", "4816", "5574", "5801", "5802", "5803", "6701", "6702", "6758", "9432", "9433"},
        "keywords": ("AI", "データ", "クラウド", "ネット", "通信", "電線", "フジクラ", "古河電"),
    },
    {
        "theme_id": "power_grid",
        "name": "電力/送電",
        "sector_codes": {"4050"},
        "codes": {"9501", "9502", "9503", "9504", "9505", "9506", "9507", "9508", "9509", "9511", "9513"},
        "keywords": ("電力", "電源", "ガス", "送電"),
    },
    {
        "theme_id": "financials",
        "name": "金融",
        "sector_codes": {"7050", "7100", "7150", "7200"},
        "codes": set(),
        "keywords": ("銀行", "証券", "保険", "リース", "フィナンシャル"),
    },
    {
        "theme_id": "trading_house",
        "name": "商社/資源",
        "sector_codes": {"6050", "3300", "3450"},
        "codes": {"8001", "8002", "8015", "8031", "8053", "8058"},
        "keywords": ("商事", "物産", "商社", "金属", "鉱業", "資源"),
    },
    {
        "theme_id": "defense",
        "name": "防衛/重工",
        "sector_codes": {"3600", "3700"},
        "codes": {"6208", "7011", "7012", "7013", "7224", "7721"},
        "keywords": ("重工", "防衛", "造船", "航空", "IHI"),
    },
    {
        "theme_id": "domestic_defensive",
        "name": "内需/食品",
        "sector_codes": {"3050", "3100", "3200", "6100"},
        "codes": set(),
        "keywords": ("食品", "小売", "薬局", "ドラッグ", "飲料"),
    },
    {
        "theme_id": "healthcare",
        "name": "医療/ヘルスケア",
        "sector_codes": {"3250", "3750"},
        "codes": {"2413", "4502", "4503", "4506", "4507", "4519", "4523", "4543", "4568", "6869", "7741"},
        "keywords": ("医薬", "テルモ", "ヘルス", "メディカル", "バイオ"),
    },
    {
        "theme_id": "inbound_transport",
        "name": "インバウンド/交通",
        "sector_codes": {"5050", "5100", "5150", "5200", "6100"},
        "codes": {"3099", "4661", "8233", "9001", "9005", "9020", "9021", "9022", "9041", "9201", "9202", "9603", "9706"},
        "keywords": ("鉄道", "空港", "航空", "ホテル", "百貨店", "観光"),
    },
    {
        "theme_id": "construction_infra",
        "name": "建設/インフラ",
        "sector_codes": {"2050", "3400", "3550"},
        "codes": {"1721", "1801", "1802", "1803", "1812", "1925", "1928", "6367"},
        "keywords": ("建設", "工業", "設備", "プラント", "インフラ"),
    },
    {
        "theme_id": "etf_index_commodity",
        "name": "ETF/指数・商品",
        "sector_codes": set(),
        "codes": {"1306", "1308", "1357", "1478", "1489", "1540", "1545", "1655", "1671", "2510"},
        "keywords": ("ETF", "上場投信", "ＮＥＸＴ　ＦＵＮＤＳ", "ｉシェアーズ", "連動型上場投信", "純金", "原油"),
    },
    {
        "theme_id": "reit_yield",
        "name": "REIT/利回り",
        "sector_codes": set(),
        "codes": {"3234", "3292", "3295", "3296", "3309", "3481", "8954", "8957", "8984"},
        "keywords": ("リート", "投資法人"),
    },
    {
        "theme_id": "real_estate_asset",
        "name": "不動産/資産",
        "sector_codes": set(),
        "codes": {
            "1878", "3003", "3252", "3288", "3293", "3452", "3479", "3482",
            "4666", "8801", "8802", "8804", "8830", "8848", "8876",
        },
        "keywords": ("不動産", "地所", "建託", "リート", "ヒューリック", "ハウス"),
    },
    {
        "theme_id": "restaurant_service",
        "name": "外食/店舗サービス",
        "sector_codes": set(),
        "codes": {"3474", "4665", "6036"},
        "keywords": ("外食", "店舗", "ダスキン", "ＫｅｅＰｅｒ"),
    },
    {
        "theme_id": "digital_consumer",
        "name": "ネット/消費サービス",
        "sector_codes": set(),
        "codes": {"2120", "2371", "2492", "4751", "4755", "6027", "6037", "7082", "9211", "9338"},
        "keywords": ("ネット", "ドットコム", "インフォ", "楽天", "サイバー", "ジモティー"),
    },
    {
        "theme_id": "advertising_media",
        "name": "広告/メディア",
        "sector_codes": set(),
        "codes": {"4324"},
        "keywords": ("広告", "電通"),
    },
    {
        "theme_id": "hr_bpo",
        "name": "人材/BPO",
        "sector_codes": set(),
        "codes": {"2146", "2168", "2170", "2471", "6070"},
        "keywords": ("人材", "パソナ", "キャリア", "エスプール", "モチベーション"),
    },
    {
        "theme_id": "security_facility",
        "name": "警備/施設管理",
        "sector_codes": set(),
        "codes": {"1717", "2331", "9735"},
        "keywords": ("警備", "セコム", "ＡＬＳＯＫ", "ファシリティ"),
    },
    {
        "theme_id": "logistics_postal",
        "name": "物流/郵政",
        "sector_codes": set(),
        "codes": {"6178"},
        "keywords": ("郵政", "物流"),
    },
    {
        "theme_id": "used_car_auction",
        "name": "中古車/オークション",
        "sector_codes": set(),
        "codes": {"4732"},
        "keywords": ("中古車", "オークション", "ユー・エス・エス"),
    },
    {
        "theme_id": "leisure_entertainment",
        "name": "レジャー/エンタメ",
        "sector_codes": set(),
        "codes": {"4301", "4680", "4681", "6191", "7803", "7867", "7974", "9616"},
        "keywords": ("レジャー", "エンタメ", "ゲーム", "任天堂", "ラウンドワン", "リゾート", "旅行"),
    },
    {
        "theme_id": "consulting_care_services",
        "name": "コンサル/介護サービス",
        "sector_codes": set(),
        "codes": {"6062", "6532"},
        "keywords": ("コンサル", "ケア", "介護"),
    },
    {
        "theme_id": "mobility_tires",
        "name": "自動車部材/タイヤ",
        "sector_codes": {"3350"},
        "codes": {"5101", "5105", "5108"},
        "keywords": ("タイヤ", "ゴム"),
    },
    {
        "theme_id": "paper_packaging",
        "name": "紙/包装",
        "sector_codes": {"3150"},
        "codes": {"3864", "3941"},
        "keywords": ("製紙", "紙", "レンゴー", "包装"),
    },
    {
        "theme_id": "fisheries_agri_food",
        "name": "水産/農業食品",
        "sector_codes": {"50"},
        "codes": {"1332", "1375"},
        "keywords": ("水産", "農林", "食品"),
    },
    {
        "theme_id": "sports_lifestyle",
        "name": "スポーツ/ライフスタイル",
        "sector_codes": set(),
        "codes": {"7906", "7936", "7951", "7956", "7806"},
        "keywords": ("スポーツ", "ヨネックス", "アシックス", "ヤマハ", "ピジョン", "ＭＴＧ"),
    },
    {
        "theme_id": "printing_materials",
        "name": "印刷/素材",
        "sector_codes": set(),
        "codes": {"7794", "7912", "7984", "7992"},
        "keywords": ("印刷", "セーラー", "イーディーピー"),
    },
    {
        "theme_id": "growth_small",
        "name": "グロース/小型材料",
        "sector_codes": set(),
        "codes": set(),
        "keywords": ("HD", "ホールディングス", "テック", "システム", "ソリューション"),
    },
]

_THEME_DEFS: list[dict[str, Any]] = get_market_basket_defs() + _LEGACY_THEME_DEFS


def _normalize_ymd_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        numeric = int(value)
    except Exception:
        return None
    if 10_000_000 <= numeric <= 99_991_231:
        return numeric
    if numeric > 1_000_000_000:
        return int(datetime.fromtimestamp(numeric, tz=timezone.utc).strftime("%Y%m%d"))
    return None


def _ymd_from_asof_ts(asof_ts: int) -> int:
    return int(datetime.fromtimestamp(int(asof_ts), tz=timezone.utc).strftime("%Y%m%d"))


def _is_near_earnings(code: str, asof_ymd: int, earnings_by_code: dict[str, set[int]]) -> bool:
    dates = earnings_by_code.get(code)
    if not dates:
        return False
    try:
        asof_dt = datetime.strptime(str(asof_ymd), "%Y%m%d").date()
    except Exception:
        return False
    for ymd in dates:
        try:
            event_dt = datetime.strptime(str(ymd), "%Y%m%d").date()
        except Exception:
            continue
        if abs((event_dt - asof_dt).days) <= 1:
            return True
    return False


def _load_earnings_dates(conn: duckdb.DuckDBPyConnection) -> dict[str, set[int]]:
    if not _table_exists(conn, "earnings_planned"):
        return {}
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info('earnings_planned')").fetchall()
        if row and row[1] is not None
    }
    date_col = next((col for col in ("planned_date", "earnings_date", "event_date", "date", "scheduled_date") if col in columns), None)
    if not date_col or "code" not in columns:
        return {}
    rows = conn.execute(f"SELECT code, {date_col} FROM earnings_planned").fetchall()
    by_code: dict[str, set[int]] = {}
    for code, raw_date in rows:
        code_text = str(code or "").strip()
        ymd = _normalize_ymd_int(raw_date)
        if not code_text or ymd is None:
            continue
        by_code.setdefault(code_text, set()).add(ymd)
    return by_code


def _theme_for_row(code: str, name: str, sector_code: str, sector_name: str) -> tuple[str, str]:
    name_text = f"{name} {sector_name}"
    for theme in _THEME_DEFS:
        if code in theme["codes"]:
            return str(theme["theme_id"]), str(theme["name"])
        if sector_code in theme["sector_codes"]:
            return str(theme["theme_id"]), str(theme["name"])
        if any(keyword and keyword in name_text for keyword in theme["keywords"]):
            return str(theme["theme_id"]), str(theme["name"])
    fallback_id = f"sector:{sector_code or 'unknown'}"
    return fallback_id, sector_name or "その他"


def _theme_status(strength: float, baseline: float, previous: float | None) -> str:
    delta = strength - baseline
    prev_delta = strength - previous if previous is not None else delta
    if strength >= 0.75 and baseline <= 0.2:
        return "NEW"
    if delta >= 0.45 or prev_delta >= 0.35:
        return "ACCEL"
    if strength >= 0.55:
        return "HOT"
    if delta <= -0.45 or prev_delta <= -0.35:
        return "FADE"
    if strength <= -0.25:
        return "COLD"
    return "WATCH"


def _load_market_watchlist_codes() -> list[str]:
    try:
        return load_watchlist_codes(resolve_watchlist_path())
    except Exception as exc:
        logger.warning("theme rotation watchlist load failed: %s", exc)
        return []


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        numeric = float(value)
    except Exception:
        return fallback
    return numeric if math.isfinite(numeric) else fallback


def _candidate_candlestick_state(row: dict[str, Any]) -> dict[str, Any]:
    open_price = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    close = _safe_float(row.get("close"))
    day_range = max(high - low, 0.0)
    body = close - open_price
    body_pct = body / open_price if open_price > 0 else 0.0
    close_position = (close - low) / day_range if day_range > 0 else 0.5
    upper_wick_ratio = (high - max(open_price, close)) / day_range if day_range > 0 else 0.0
    lower_wick_ratio = (min(open_price, close) - low) / day_range if day_range > 0 else 0.0

    states: list[str] = []
    if close < open_price and abs(body_pct) >= 0.025:
        states.append("bearish_large_body")
    if upper_wick_ratio >= 0.42 and close_position <= 0.62:
        states.append("upper_wick_rejection")
    if close_position <= 0.33:
        states.append("weak_close")
    if close >= open_price and close_position >= 0.72:
        states.append("bullish_close")
    if not states:
        states.append("neutral")

    return {
        "states": states,
        "closePosition": close_position,
        "bodyPct": body_pct * 100,
        "upperWickRatio": upper_wick_ratio,
        "lowerWickRatio": lower_wick_ratio,
    }


def _candidate_visual_check(row: dict[str, Any], candle: dict[str, Any]) -> dict[str, Any]:
    close = _safe_float(row.get("close"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    high60 = _safe_float(row.get("high60"))
    low60 = _safe_float(row.get("low60"))
    close_position = _safe_float(candle.get("closePosition"), 0.5)
    states = set(candle.get("states") or [])

    supports: list[str] = []
    resistances: list[str] = []
    warnings: list[str] = []
    setup = "needs_detail_chart_check"

    if ma20 > 0 and close >= ma20:
        supports.append("above_ma20")
    if ma60 > 0 and close >= ma60:
        supports.append("above_ma60")
    if close_position >= 0.72:
        supports.append("high_close")
    if high60 > 0 and close >= high60 * 0.995:
        supports.append("near_60d_high")

    if high60 > 0 and close < high60 * 0.95:
        resistances.append("below_recent_high")
    if ma60 > 0 and close < ma60:
        resistances.append("below_ma60")
    if ma20 > 0 and close > ma20 * 1.12:
        warnings.append("extended_from_ma20")
    if "upper_wick_rejection" in states:
        warnings.append("upper_wick")
    if "weak_close" in states:
        warnings.append("weak_close")
    if "bearish_large_body" in states:
        warnings.append("bearish_body")

    if "weak_close" in warnings or "bearish_body" in warnings or "below_ma60" in resistances:
        setup = "avoid_until_repair"
    elif "extended_from_ma20" in warnings:
        setup = "momentum_only"
    elif "near_60d_high" in supports and "high_close" in supports:
        setup = "breakout_watch"
    elif "above_ma20" in supports and "above_ma60" in supports and "high_close" in supports:
        setup = "constructive"

    support_price = ma20 if ma20 > 0 else low60 if low60 > 0 else None
    resistance_price = high60 if high60 > 0 else None
    return {
        "required": True,
        "setup": setup,
        "supports": supports,
        "resistances": resistances,
        "warnings": warnings,
        "supportPrice": round(support_price, 2) if support_price else None,
        "resistancePrice": round(resistance_price, 2) if resistance_price else None,
        "detailRouteRequired": True,
    }


def _score_theme_candidate(row: dict[str, Any], theme_stats: dict[str, Any], mode: str) -> dict[str, Any]:
    close = _safe_float(row.get("close"))
    prev_close = _safe_float(row.get("prevClose"))
    high60 = _safe_float(row.get("high60"))
    low60 = _safe_float(row.get("low60"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    avg_volume20 = _safe_float(row.get("avgVolume20"))
    volume = _safe_float(row.get("volume"))
    change_pct = ((close - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
    volume_ratio = (volume / avg_volume20) if avg_volume20 > 0 else 0.0
    extended20 = ((close - ma20) / ma20 * 100) if ma20 > 0 else 0.0
    range60 = ((high60 - low60) / low60 * 100) if low60 > 0 else 0.0
    high_retention = (close / high60) if high60 > 0 else 0.0
    breakout60 = bool(high60 > 0 and close >= high60 * 0.995)
    above20 = bool(ma20 > 0 and close >= ma20)
    above60 = bool(ma60 > 0 and close >= ma60)

    candle = _candidate_candlestick_state(row)
    visual_check = _candidate_visual_check(row, candle)
    states = set(candle["states"])
    theme_count = int(theme_stats.get("count") or 0)
    advancer_ratio = _safe_float(theme_stats.get("advancerRatio"))
    avg_theme_change = _safe_float(theme_stats.get("avgChangePct"))
    avg_theme_volume = _safe_float(theme_stats.get("avgVolumeRatio"))

    reason_codes: list[str] = []
    penalty_codes: list[str] = []
    if volume_ratio >= 1.8:
        reason_codes.append("volume_expansion")
    if breakout60:
        reason_codes.append("breakout_60d")
    if theme_count >= 5 and advancer_ratio >= 0.6:
        reason_codes.append("theme_breadth")
    if above20 and above60:
        reason_codes.append("ma_alignment")
    if "bullish_close" in states:
        reason_codes.append("bullish_close")
    if range60 >= 25:
        reason_codes.append("wide_range")

    for code in ("bearish_large_body", "upper_wick_rejection", "weak_close"):
        if code in states:
            penalty_codes.append(code)
    if extended20 >= 13:
        penalty_codes.append("extended")
    if theme_count < 4:
        penalty_codes.append("thin_theme")
    if ma60 > 0 and close < ma60:
        penalty_codes.append("below_ma60")

    theme_score = (
        _clamp((avg_theme_change + 1.0) / 5.0, 0.0, 1.0) * 35
        + _clamp(advancer_ratio, 0.0, 1.0) * 35
        + _clamp(avg_theme_volume / 2.5, 0.0, 1.0) * 20
        + _clamp(theme_count / 10.0, 0.0, 1.0) * 10
    )
    chart_score = (
        _clamp(candle["closePosition"], 0.0, 1.0) * 32
        + (18 if above20 else 0)
        + (14 if above60 else 0)
        + _clamp(high_retention, 0.0, 1.0) * 18
        + _clamp((volume_ratio - 0.8) / 2.2, 0.0, 1.0) * 18
    )
    range_score = (
        _clamp(range60 / 45.0, 0.0, 1.0) * 38
        + _clamp(abs(change_pct) / 8.0, 0.0, 1.0) * 18
        + _clamp(volume_ratio / 3.0, 0.0, 1.0) * 24
        + (20 if breakout60 else _clamp(high_retention, 0.0, 1.0) * 12)
    )
    continuation_score = (
        (22 if above20 else 0)
        + (20 if above60 else 0)
        + _clamp(high_retention, 0.0, 1.0) * 20
        + _clamp((change_pct + 1.0) / 6.0, 0.0, 1.0) * 12
        + _clamp(volume_ratio / 2.5, 0.0, 1.0) * 10
        + _clamp(theme_score / 100.0, 0.0, 1.0) * 16
    )
    ease_score = (
        _clamp(chart_score / 100.0, 0.0, 1.0) * 48
        + _clamp(theme_score / 100.0, 0.0, 1.0) * 24
        + _clamp((10.0 - max(extended20, 0.0)) / 10.0, 0.0, 1.0) * 18
        + _clamp((change_pct + 2.0) / 5.0, 0.0, 1.0) * 10
    )

    if mode == "value_range":
        score = range_score * 0.5 + theme_score * 0.25 + continuation_score * 0.25
    elif mode == "continuation":
        score = continuation_score * 0.55 + theme_score * 0.25 + chart_score * 0.20
    else:
        score = ease_score

    penalty_weight = {
        "bearish_large_body": 13,
        "upper_wick_rejection": 10,
        "weak_close": 14,
        "extended": 9 if mode != "value_range" else 5,
        "thin_theme": 8,
        "below_ma60": 10,
    }
    score -= sum(penalty_weight.get(code, 0) for code in penalty_codes)
    score = _clamp(score, 0.0, 100.0)
    return {
        "score": round(score, 1),
        "changePct": round(change_pct, 2),
        "volumeRatio": round(volume_ratio, 2),
        "extended20": round(extended20, 2),
        "range60Pct": round(range60, 2),
        "highRetention": round(high_retention, 3),
        "breakout60": breakout60,
        "reasonCodes": reason_codes,
        "penaltyCodes": penalty_codes,
        "candlestickState": candle,
        "visualCheck": visual_check,
    }


def _build_visual_frame(rows: list[tuple[Any, Any, Any, Any, Any, Any]], timeframe: str) -> dict[str, Any]:
    ordered = [
        {
            "date": row[0],
            "open": _safe_float(row[1]),
            "high": _safe_float(row[2]),
            "low": _safe_float(row[3]),
            "close": _safe_float(row[4]),
            "volume": _safe_float(row[5]),
        }
        for row in rows
        if row is not None
    ]
    ordered = [row for row in ordered if row["close"] > 0]
    if not ordered:
        return {"timeframe": timeframe, "status": "empty"}
    latest = ordered[-1]
    prev = ordered[-2] if len(ordered) >= 2 else None
    closes = [row["close"] for row in ordered]
    highs = [row["high"] for row in ordered]
    lows = [row["low"] for row in ordered]
    volumes = [row["volume"] for row in ordered]

    def avg(values: list[float], length: int) -> float | None:
        if len(values) < length:
            return None
        return sum(values[-length:]) / length

    ma7 = avg(closes, 7)
    ma20 = avg(closes, 20)
    ma60 = avg(closes, 60)
    high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    low20 = min(lows[-20:]) if len(lows) >= 20 else min(lows)
    high60 = max(highs[-60:]) if len(highs) >= 60 else max(highs)
    low60 = min(lows[-60:]) if len(lows) >= 60 else min(lows)
    avg_volume20 = avg(volumes[:-1], 20) if len(volumes) >= 21 else avg(volumes, min(len(volumes), 20))
    row = {
        "open": latest["open"],
        "high": latest["high"],
        "low": latest["low"],
        "close": latest["close"],
        "prevClose": prev["close"] if prev else 0,
        "volume": latest["volume"],
        "avgVolume20": avg_volume20 or 0,
        "ma20": ma20 or 0,
        "ma60": ma60 or 0,
        "high60": high60,
        "low60": low60,
    }
    candle = _candidate_candlestick_state(row)
    visual = _candidate_visual_check(row, candle)
    change_pct = ((latest["close"] - prev["close"]) / prev["close"] * 100) if prev and prev["close"] > 0 else None
    extension20 = ((latest["close"] - ma20) / ma20 * 100) if ma20 and ma20 > 0 else None
    volume_ratio = (latest["volume"] / avg_volume20) if avg_volume20 and avg_volume20 > 0 else None
    trend = "neutral"
    if ma20 and ma60 and latest["close"] >= ma20 >= ma60:
        trend = "uptrend"
    elif ma20 and ma60 and latest["close"] < ma20 < ma60:
        trend = "downtrend"
    elif ma20 and latest["close"] >= ma20:
        trend = "repairing"
    elif ma20 and latest["close"] < ma20:
        trend = "below_ma20"
    return {
        "timeframe": timeframe,
        "status": "ok",
        "asOf": latest["date"],
        "barCount": len(ordered),
        "latest": {
            "open": latest["open"],
            "high": latest["high"],
            "low": latest["low"],
            "close": latest["close"],
            "volume": latest["volume"],
            "changePct": round(change_pct, 2) if change_pct is not None else None,
        },
        "movingAverages": {
            "ma7": round(ma7, 2) if ma7 else None,
            "ma20": round(ma20, 2) if ma20 else None,
            "ma60": round(ma60, 2) if ma60 else None,
        },
        "levels": {
            "high20": round(high20, 2),
            "low20": round(low20, 2),
            "high60": round(high60, 2),
            "low60": round(low60, 2),
            "support": visual.get("supportPrice"),
            "resistance": visual.get("resistancePrice"),
        },
        "metrics": {
            "extension20": round(extension20, 2) if extension20 is not None else None,
            "volumeRatio": round(volume_ratio, 2) if volume_ratio is not None else None,
            "closePosition": round(_safe_float(candle.get("closePosition")), 3),
        },
        "candlestickState": candle,
        "visualCheck": visual,
        "trend": trend,
    }


def _build_weekly_rows_from_daily(rows: list[tuple[Any, Any, Any, Any, Any, Any]]) -> list[tuple[Any, float, float, float, float, float]]:
    buckets: dict[str, dict[str, Any]] = {}
    for raw_date, open_price, high, low, close, volume in rows:
        ymd = _normalize_ymd_int(raw_date)
        if ymd is None:
            continue
        dt = datetime.strptime(str(ymd), "%Y%m%d").date()
        key = f"{dt.isocalendar().year:04d}{dt.isocalendar().week:02d}"
        bucket = buckets.setdefault(
            key,
            {
                "date": ymd,
                "open": _safe_float(open_price),
                "high": _safe_float(high),
                "low": _safe_float(low),
                "close": _safe_float(close),
                "volume": 0.0,
            },
        )
        if ymd < int(bucket["date"]):
            bucket["open"] = _safe_float(open_price)
        bucket["date"] = max(int(bucket["date"]), ymd)
        bucket["high"] = max(float(bucket["high"]), _safe_float(high))
        bucket["low"] = min(float(bucket["low"]), _safe_float(low))
        bucket["close"] = _safe_float(close)
        bucket["volume"] += _safe_float(volume)
    return [
        (bucket["date"], bucket["open"], bucket["high"], bucket["low"], bucket["close"], bucket["volume"])
        for _, bucket in sorted(buckets.items())
    ]


def _detail_visual_decision(frames: dict[str, dict[str, Any]]) -> dict[str, Any]:
    daily = frames.get("daily", {})
    weekly = frames.get("weekly", {})
    monthly = frames.get("monthly", {})
    warnings = []
    supports = []
    score = 0
    for key, weight in (("daily", 45), ("weekly", 35), ("monthly", 20)):
        frame = frames.get(key, {})
        if frame.get("status") != "ok":
            warnings.append(f"{key}_missing")
            continue
        setup = str((frame.get("visualCheck") or {}).get("setup") or "")
        trend = str(frame.get("trend") or "")
        if setup in ("constructive", "breakout_watch"):
            score += weight
            supports.append(f"{key}_{setup}")
        elif setup == "momentum_only":
            score += weight * 0.6
            warnings.append(f"{key}_extended")
        elif setup == "avoid_until_repair":
            warnings.append(f"{key}_avoid")
        if trend == "uptrend":
            score += 5
        elif trend == "downtrend":
            warnings.append(f"{key}_downtrend")
    if score >= 75 and not any(item.endswith("_avoid") for item in warnings):
        decision = "entry_candidate"
    elif score >= 55 and not any(item == "daily_avoid" for item in warnings):
        decision = "watch_candidate"
    else:
        decision = "avoid_or_wait"
    return {
        "decision": decision,
        "score": round(_clamp(float(score), 0.0, 100.0), 1),
        "supports": supports,
        "warnings": warnings,
        "requiresScreenshot": False,
        "requiresHumanChartOpen": decision != "entry_candidate",
    }


def _fetch_confirmed_chart_rows(code: str) -> dict[str, list[tuple[Any, Any, Any, Any, Any, Any]]] | None:
    code_text = str(code or "").strip()
    if not code_text:
        return {"daily": [], "weekly": [], "monthly": []}
    db_path = str(config.DB_PATH)
    try:
        with try_get_conn_for_path(db_path, timeout_sec=2.5, read_only=True) as conn:
            if conn is None:
                return None
            if not _table_exists(conn, "daily_bars"):
                return {"daily": [], "weekly": [], "monthly": []}
            daily_rows = conn.execute(
                """
                SELECT date, o, h, l, c, v
                FROM (
                    SELECT date, o, h, l, c, v
                    FROM daily_bars
                    WHERE code = ?
                    ORDER BY date DESC
                    LIMIT 260
                )
                ORDER BY date ASC
                """,
                [code_text],
            ).fetchall()
            monthly_rows: list[tuple[Any, Any, Any, Any, Any, Any]] = []
            if _table_exists(conn, "monthly_bars"):
                monthly_rows = conn.execute(
                    """
                    SELECT month, o, h, l, c, v
                    FROM (
                        SELECT month, o, h, l, c, v
                        FROM monthly_bars
                        WHERE code = ?
                        ORDER BY month DESC
                        LIMIT 120
                    )
                    ORDER BY month ASC
                    """,
                    [code_text],
                ).fetchall()
            return {
                "daily": daily_rows,
                "weekly": _build_weekly_rows_from_daily(daily_rows),
                "monthly": monthly_rows,
            }
    except Exception as exc:
        logger.exception("confirmed chart row fetch failed: %s", exc)
        return None


def _render_svg_candles(
    *,
    code: str,
    title: str,
    row_sets: dict[str, list[tuple[Any, Any, Any, Any, Any, Any]]],
    max_bars: int = 90,
) -> str:
    width = 1200
    panel_h = 260
    header_h = 42
    gap = 18
    left = 48
    right = 26
    top_pad = 24
    bottom_pad = 34
    chart_w = width - left - right
    labels = {"daily": "Daily", "weekly": "Weekly", "monthly": "Monthly"}
    frames = ["daily", "weekly", "monthly"]
    height = header_h + len(frames) * panel_h + (len(frames) - 1) * gap

    def esc(text: Any) -> str:
        return (
            str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f7f4ec"/>',
        f'<text x="22" y="27" font-size="18" font-weight="700" fill="#0b3551">{esc(code)} {esc(title)}</text>',
        '<text x="1120" y="27" font-size="11" text-anchor="end" fill="#6b7f90">confirmed bars</text>',
    ]
    for frame_index, frame in enumerate(frames):
        rows = row_sets.get(frame, [])[-max_bars:]
        y0 = header_h + frame_index * (panel_h + gap)
        parts.append(f'<rect x="16" y="{y0}" width="{width - 32}" height="{panel_h}" rx="10" fill="#ffffff" stroke="#d7e1ea"/>')
        parts.append(f'<text x="28" y="{y0 + 22}" font-size="13" font-weight="700" fill="#0b3551">{labels[frame]}</text>')
        if not rows:
            parts.append(f'<text x="{width / 2}" y="{y0 + panel_h / 2}" font-size="13" text-anchor="middle" fill="#8aa0b3">no data</text>')
            continue
        highs = [_safe_float(row[2]) for row in rows]
        lows = [_safe_float(row[3]) for row in rows]
        closes = [_safe_float(row[4]) for row in rows]
        price_high = max(highs)
        price_low = min(lows)
        if price_high <= price_low:
            price_high = price_low + 1
        plot_top = y0 + top_pad
        plot_bottom = y0 + panel_h - bottom_pad
        plot_h = plot_bottom - plot_top

        def y_for(price: float) -> float:
            return plot_bottom - (price - price_low) / (price_high - price_low) * plot_h

        for tick in range(5):
            ratio = tick / 4
            y = plot_top + ratio * plot_h
            price = price_high - ratio * (price_high - price_low)
            parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#e8eef3" stroke-width="1"/>')
            parts.append(f'<text x="{width - right + 4}" y="{y + 4:.1f}" font-size="10" fill="#6b7f90">{price:.0f}</text>')

        bar_gap = chart_w / max(1, len(rows))
        candle_w = max(2.0, min(9.0, bar_gap * 0.62))
        for idx, row in enumerate(rows):
            open_price = _safe_float(row[1])
            high = _safe_float(row[2])
            low = _safe_float(row[3])
            close = _safe_float(row[4])
            x = left + idx * bar_gap + bar_gap / 2
            color = "#ef4444" if close >= open_price else "#22c55e"
            y_high = y_for(high)
            y_low = y_for(low)
            y_open = y_for(open_price)
            y_close = y_for(close)
            body_top = min(y_open, y_close)
            body_h = max(1.0, abs(y_close - y_open))
            parts.append(f'<line x1="{x:.1f}" y1="{y_high:.1f}" x2="{x:.1f}" y2="{y_low:.1f}" stroke="{color}" stroke-width="1.3"/>')
            parts.append(
                f'<rect x="{x - candle_w / 2:.1f}" y="{body_top:.1f}" width="{candle_w:.1f}" height="{body_h:.1f}" '
                f'fill="{color}" opacity="0.88"/>'
            )

        ma_lengths = [(7, "#ef4444"), (20, "#16a34a"), (60, "#2563eb")]
        for length, color in ma_lengths:
            if len(closes) < length:
                continue
            points = []
            for idx in range(length - 1, len(closes)):
                ma = sum(closes[idx - length + 1:idx + 1]) / length
                x = left + idx * bar_gap + bar_gap / 2
                points.append(f"{x:.1f},{y_for(ma):.1f}")
            if points:
                parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="1.5" opacity="0.9"/>')
        latest = rows[-1]
        parts.append(
            f'<text x="28" y="{y0 + panel_h - 10}" font-size="11" fill="#6b7f90">'
            f'last {esc(latest[0])} O {_safe_float(latest[1]):.0f} H {_safe_float(latest[2]):.0f} '
            f'L {_safe_float(latest[3]):.0f} C {_safe_float(latest[4]):.0f}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def _fetch_detail_visual_check(code: str) -> dict[str, Any] | None:
    code_text = str(code or "").strip()
    if not code_text:
        return {"status": "invalid_code", "code": code_text}
    db_path = str(config.DB_PATH)
    try:
        with try_get_conn_for_path(db_path, timeout_sec=2.5, read_only=True) as conn:
            if conn is None:
                return {"status": "db_unavailable", "code": code_text}
            if not _table_exists(conn, "daily_bars"):
                return None
            name_row = conn.execute(
                "SELECT COALESCE(name, code), COALESCE(sector33_code, ''), COALESCE(sector33_name, '') FROM industry_master WHERE code = ? LIMIT 1",
                [code_text],
            ).fetchone() if _has_industry_master(conn) else None
            chart_rows = _fetch_confirmed_chart_rows(code_text)
            if chart_rows is None:
                return {"status": "db_unavailable", "code": code_text}
            frames = {
                "daily": _build_visual_frame(chart_rows["daily"], "daily"),
                "weekly": _build_visual_frame(chart_rows["weekly"], "weekly"),
                "monthly": _build_visual_frame(chart_rows["monthly"], "monthly"),
            }
            name = str(name_row[0]) if name_row else code_text
            sector_code = str(name_row[1]) if name_row else ""
            sector_name = str(name_row[2]) if name_row else ""
            theme_id, theme_name = _theme_for_row(code_text, name, sector_code, sector_name)
            return {
                "status": "ok" if frames["daily"].get("status") == "ok" else "empty",
                "code": code_text,
                "name": name,
                "themeId": theme_id,
                "theme": theme_name,
                "detailRoute": f"/detail/{code_text}",
                "frames": frames,
                "visualSelection": _detail_visual_decision(frames),
                "basis": "confirmed_bars",
            }
    except Exception as exc:
        logger.exception("detail visual check failed: %s", exc)
        return None


def _theme_api_cache_marker(db_path: str) -> tuple[str, int | None]:
    try:
        return (str(db_path), int(os.path.getmtime(db_path) * 1000))
    except Exception:
        return (str(db_path), None)


def _theme_api_cache_get(key: tuple[Any, ...]) -> dict[str, Any] | None:
    entry = _THEME_API_CACHE.get(key)
    if entry is None:
        return None
    expires_at, payload = entry
    if expires_at < time.monotonic():
        _THEME_API_CACHE.pop(key, None)
        return None
    return deepcopy(payload)


def _theme_api_cache_put(key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    if len(_THEME_API_CACHE) >= _THEME_API_CACHE_MAX_ENTRIES:
        oldest_key = min(_THEME_API_CACHE.items(), key=lambda item: item[1][0])[0]
        _THEME_API_CACHE.pop(oldest_key, None)
    _THEME_API_CACHE[key] = (time.monotonic() + _THEME_API_CACHE_TTL_SEC, deepcopy(payload))


def _fetch_theme_candidates(*, mode: str, limit: int, exclude_earnings: bool) -> dict[str, Any] | None:
    db_path = str(config.DB_PATH)
    safe_mode = mode if mode in ("entry_ease", "value_range", "continuation") else "entry_ease"
    safe_limit = max(1, min(int(limit or 12), 40))
    watchlist_codes = _load_market_watchlist_codes()
    if not watchlist_codes:
        return {
            "items": [],
            "mode": safe_mode,
            "excludeEarnings": exclude_earnings,
            "scope": "watchlist",
            "watchlistCount": 0,
            "status": "watchlist_empty",
        }
    cache_key = (
        "theme_candidates",
        safe_mode,
        safe_limit,
        bool(exclude_earnings),
        tuple(watchlist_codes),
        _theme_api_cache_marker(db_path),
    )
    cached = _theme_api_cache_get(cache_key)
    if cached is not None:
        cached["cache"] = "memory"
        return cached
    try:
        with try_get_conn_for_path(db_path, timeout_sec=2.5, read_only=True) as conn:
            if conn is None:
                logger.warning("theme candidates skipped: db unavailable")
                return {
                    "items": [],
                    "mode": safe_mode,
                    "excludeEarnings": exclude_earnings,
                    "scope": "watchlist",
                    "watchlistCount": len(watchlist_codes),
                    "status": "db_unavailable",
                    "degraded": True,
                    "retryable": True,
                }
            if not _has_industry_master(conn) or not _table_exists(conn, "daily_bars"):
                return None
            watchlist_values = ",".join(["?"] * len(watchlist_codes))
            earnings_by_code = _load_earnings_dates(conn)
            rows = conn.execute(
                f"""
                WITH candidate_threshold AS (
                    SELECT MIN(date) AS min_date
                    FROM (
                        SELECT DISTINCT b.date
                        FROM daily_bars b
                        WHERE b.code IN ({watchlist_values})
                        ORDER BY b.date DESC
                        LIMIT 61
                    )
                ),
                raw AS (
                    SELECT
                        b.code,
                        COALESCE(im.name, b.code) AS name,
                        COALESCE(im.sector33_code, '') AS sector33_code,
                        COALESCE(im.sector33_name, '') AS sector33_name,
                        b.date,
                        CASE
                            WHEN b.date BETWEEN 10000000 AND 99991231 THEN strptime(CAST(b.date AS VARCHAR), '%Y%m%d')
                            ELSE to_timestamp(b.date)
                        END AS asof_dt,
                        b.o,
                        b.h,
                        b.l,
                        b.c,
                        b.v,
                        LAG(b.c) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_close,
                        AVG(b.c) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                        AVG(b.c) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                        MAX(b.h) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
                        MIN(b.l) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
                        AVG(b.v) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg_volume20,
                        ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.date DESC) AS rn
                    FROM daily_bars b
                    CROSS JOIN candidate_threshold ct
                    LEFT JOIN industry_master im ON im.code = b.code
                    WHERE b.code IN ({watchlist_values}) AND b.date >= ct.min_date
                )
                SELECT
                    epoch(asof_dt)::BIGINT AS asof_ts,
                    code,
                    name,
                    sector33_code,
                    sector33_name,
                    o,
                    h,
                    l,
                    c,
                    v,
                    prev_close,
                    ma20,
                    ma60,
                    high60,
                    low60,
                    avg_volume20
                FROM raw
                WHERE rn = 1 AND prev_close IS NOT NULL
                """,
                [*watchlist_codes, *watchlist_codes],
            ).fetchall()
            if not rows:
                return {
                    "items": [],
                    "mode": safe_mode,
                    "excludeEarnings": exclude_earnings,
                    "scope": "watchlist",
                    "watchlistCount": len(watchlist_codes),
                    "status": "empty",
                }

            candidates: list[dict[str, Any]] = []
            theme_acc: dict[str, dict[str, Any]] = {}
            asof_ts = int(rows[0][0] or 0)
            for row in rows:
                code = str(row[1] or "").strip()
                asof_ymd = _ymd_from_asof_ts(int(row[0] or 0))
                is_earnings = _is_near_earnings(code, asof_ymd, earnings_by_code)
                if exclude_earnings and is_earnings:
                    continue
                name = str(row[2] or code)
                sector_code = str(row[3] or "").strip()
                sector_name = str(row[4] or "").strip()
                theme_id, theme_name = _theme_for_row(code, name, sector_code, sector_name)
                item = {
                    "asof": int(row[0] or 0),
                    "code": code,
                    "name": name,
                    "sector33Code": sector_code,
                    "sector33Name": sector_name,
                    "themeId": theme_id,
                    "theme": theme_name,
                    "open": _safe_float(row[5]),
                    "high": _safe_float(row[6]),
                    "low": _safe_float(row[7]),
                    "close": _safe_float(row[8]),
                    "volume": _safe_float(row[9]),
                    "prevClose": _safe_float(row[10]),
                    "ma20": _safe_float(row[11]),
                    "ma60": _safe_float(row[12]),
                    "high60": _safe_float(row[13]),
                    "low60": _safe_float(row[14]),
                    "avgVolume20": _safe_float(row[15]),
                    "isEarnings": is_earnings,
                }
                change_pct = ((item["close"] - item["prevClose"]) / item["prevClose"] * 100) if item["prevClose"] > 0 else 0.0
                volume_ratio = (item["volume"] / item["avgVolume20"]) if item["avgVolume20"] > 0 else 0.0
                acc = theme_acc.setdefault(
                    theme_id,
                    {
                        "themeId": theme_id,
                        "name": theme_name,
                        "count": 0,
                        "advancers": 0,
                        "changeSum": 0.0,
                        "volumeRatioSum": 0.0,
                    },
                )
                acc["count"] += 1
                acc["advancers"] += 1 if change_pct > 0 else 0
                acc["changeSum"] += change_pct
                acc["volumeRatioSum"] += volume_ratio
                candidates.append(item)

            theme_stats: dict[str, dict[str, Any]] = {}
            for theme_id, acc in theme_acc.items():
                count = max(1, int(acc["count"]))
                theme_stats[theme_id] = {
                    "themeId": theme_id,
                    "name": acc["name"],
                    "count": count,
                    "advancerRatio": float(acc["advancers"]) / count,
                    "avgChangePct": float(acc["changeSum"]) / count,
                    "avgVolumeRatio": float(acc["volumeRatioSum"]) / count,
                }

            scored: list[dict[str, Any]] = []
            for item in candidates:
                stats = theme_stats.get(str(item["themeId"]), {})
                score_data = _score_theme_candidate(item, stats, safe_mode)
                scored.append(
                    {
                        "code": item["code"],
                        "name": item["name"],
                        "themeId": item["themeId"],
                        "theme": item["theme"],
                        "score": score_data["score"],
                        "mode": safe_mode,
                        "asOf": item["asof"],
                        "label": _format_timeline_label("1d", int(item["asof"] or asof_ts)),
                        "close": item["close"],
                        "changePct": score_data["changePct"],
                        "volumeRatio": score_data["volumeRatio"],
                        "ma20": item["ma20"],
                        "ma60": item["ma60"],
                        "high60": item["high60"],
                        "extended20": score_data["extended20"],
                        "range60Pct": score_data["range60Pct"],
                        "highRetention": score_data["highRetention"],
                        "breakout60": score_data["breakout60"],
                        "themeCount": int(stats.get("count") or 0),
                        "themeAdvancerRatio": round(_safe_float(stats.get("advancerRatio")), 3),
                        "themeAvgChangePct": round(_safe_float(stats.get("avgChangePct")), 2),
                        "themeAvgVolumeRatio": round(_safe_float(stats.get("avgVolumeRatio")), 2),
                        "candlestickState": score_data["candlestickState"],
                        "visualCheck": score_data["visualCheck"],
                        "reasonCodes": score_data["reasonCodes"],
                        "penaltyCodes": score_data["penaltyCodes"],
                        "isEarnings": item["isEarnings"],
                        "detailRoute": f"/detail/{item['code']}",
                    }
                )
            scored.sort(key=lambda item: (float(item["score"]), float(item["changePct"])), reverse=True)
            for index, item in enumerate(scored, start=1):
                item["rank"] = index
            payload = {
                "asof": asof_ts,
                "label": _format_timeline_label("1d", asof_ts),
                "items": scored[:safe_limit],
                "mode": safe_mode,
                "excludeEarnings": exclude_earnings,
                "scope": "watchlist",
                "watchlistCount": len(watchlist_codes),
                "status": "ok",
            }
            _theme_api_cache_put(cache_key, payload)
            return payload
    except Exception as exc:
        logger.exception("theme candidates fetch failed: %s", exc)
        return None


def _fetch_theme_rotation(*, limit: int, exclude_earnings: bool) -> dict[str, Any] | None:
    db_path = str(config.DB_PATH)
    safe_limit = max(1, min(int(limit or 12), 40))
    watchlist_codes = _load_market_watchlist_codes()
    if not watchlist_codes:
        return {
            "themes": [],
            "frames": [],
            "excludeEarnings": exclude_earnings,
            "scope": "watchlist",
            "watchlistCount": 0,
            "status": "watchlist_empty",
        }
    cache_key = (
        "theme_rotation",
        safe_limit,
        bool(exclude_earnings),
        tuple(watchlist_codes),
        _theme_api_cache_marker(db_path),
    )
    cached = _theme_api_cache_get(cache_key)
    if cached is not None:
        cached["cache"] = "memory"
        return cached
    try:
        with try_get_conn_for_path(db_path, timeout_sec=2.5, read_only=True) as conn:
            if conn is None:
                logger.warning("theme rotation skipped: db unavailable")
                return {
                    "themes": [],
                    "frames": [],
                    "excludeEarnings": exclude_earnings,
                    "scope": "watchlist",
                    "watchlistCount": len(watchlist_codes),
                    "status": "db_unavailable",
                    "degraded": True,
                    "retryable": True,
                }
            if not _has_industry_master(conn) or not _table_exists(conn, "daily_bars"):
                return None
            watchlist_values = ",".join(["?"] * len(watchlist_codes))
            earnings_by_code = _load_earnings_dates(conn)
            rows = conn.execute(
                f"""
                WITH frame_threshold AS (
                    SELECT MIN(date) AS min_date
                    FROM (
                        SELECT DISTINCT b.date
                        FROM daily_bars b
                        WHERE b.code IN ({watchlist_values})
                        ORDER BY b.date DESC
                        LIMIT (? + 21)
                    )
                ),
                base AS (
                    SELECT
                        b.code,
                        COALESCE(im.name, b.code) AS name,
                        COALESCE(im.sector33_code, '') AS sector33_code,
                        COALESCE(im.sector33_name, '') AS sector33_name,
                        CASE
                            WHEN b.date BETWEEN 10000000 AND 99991231 THEN strptime(CAST(b.date AS VARCHAR), '%Y%m%d')
                            ELSE to_timestamp(b.date)
                        END AS asof_dt,
                        b.c AS close,
                        b.v AS volume,
                        LAG(b.c) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_close,
                        AVG(b.v) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg_volume20
                    FROM daily_bars b
                    CROSS JOIN frame_threshold ft
                    LEFT JOIN industry_master im ON im.code = b.code
                    WHERE b.code IN ({watchlist_values}) AND b.date >= ft.min_date
                ),
                frames AS (
                    SELECT DISTINCT asof_dt
                    FROM base
                    ORDER BY asof_dt DESC
                    LIMIT ?
                )
                SELECT
                    epoch(b.asof_dt)::BIGINT AS asof_ts,
                    b.code,
                    b.name,
                    b.sector33_code,
                    b.sector33_name,
                    CASE
                        WHEN b.prev_close IS NULL OR b.prev_close = 0 THEN NULL
                        ELSE (b.close - b.prev_close) / b.prev_close * 100
                    END AS change_pct,
                    b.close,
                    b.volume,
                    b.close * b.volume AS turnover,
                    CASE
                        WHEN b.avg_volume20 IS NULL OR b.avg_volume20 = 0 THEN NULL
                        ELSE b.volume / b.avg_volume20
                    END AS volume_ratio
                FROM base b
                JOIN frames f ON f.asof_dt = b.asof_dt
                WHERE b.prev_close IS NOT NULL
                ORDER BY b.asof_dt ASC, b.code ASC
                """,
                [*watchlist_codes, safe_limit, *watchlist_codes, safe_limit],
            ).fetchall()
            if not rows:
                return {
                    "themes": [],
                    "frames": [],
                    "excludeEarnings": exclude_earnings,
                    "scope": "watchlist",
                    "watchlistCount": len(watchlist_codes),
                    "status": "empty",
                }

            frames: dict[int, dict[str, Any]] = {}
            for row in rows:
                asof_ts = int(row[0] or 0)
                asof_ymd = _ymd_from_asof_ts(asof_ts)
                code = str(row[1] or "").strip()
                name = str(row[2] or code)
                sector_code = str(row[3] or "").strip()
                sector_name = str(row[4] or "").strip()
                change_pct = float(row[5] or 0.0)
                close = float(row[6] or 0.0)
                volume = float(row[7] or 0.0)
                turnover = float(row[8] or 0.0)
                volume_ratio = float(row[9] or 0.0)
                is_earnings = _is_near_earnings(code, asof_ymd, earnings_by_code)
                if exclude_earnings and is_earnings:
                    continue
                theme_id, theme_name = _theme_for_row(code, name, sector_code, sector_name)
                frame = frames.setdefault(
                    asof_ts,
                    {
                        "asof": asof_ts,
                        "label": _format_timeline_label("1d", asof_ts),
                        "themes": {},
                    },
                )
                theme = frame["themes"].setdefault(
                    theme_id,
                    {
                        "themeId": theme_id,
                        "name": theme_name,
                        "count": 0,
                        "advancers": 0,
                        "decliners": 0,
                        "turnover": 0.0,
                        "changeSum": 0.0,
                        "volumeRatioSum": 0.0,
                        "earningsCount": 0,
                        "leaders": [],
                    },
                )
                theme["count"] += 1
                theme["advancers"] += 1 if change_pct > 0 else 0
                theme["decliners"] += 1 if change_pct < 0 else 0
                theme["turnover"] += turnover
                theme["changeSum"] += change_pct
                theme["volumeRatioSum"] += volume_ratio
                theme["earningsCount"] += 1 if is_earnings else 0
                theme["leaders"].append(
                    {
                        "code": code,
                        "name": name,
                        "changePct": change_pct,
                        "close": close,
                        "volume": volume,
                        "volumeRatio": volume_ratio,
                        "isEarnings": is_earnings,
                    }
                )

            ordered_frames: list[dict[str, Any]] = []
            for asof_ts in sorted(frames):
                frame = frames[asof_ts]
                themes = []
                max_turnover = max((theme["turnover"] for theme in frame["themes"].values()), default=1.0) or 1.0
                for theme in frame["themes"].values():
                    count = max(1, int(theme["count"]))
                    avg_change = float(theme["changeSum"]) / count
                    advancer_ratio = float(theme["advancers"]) / count
                    avg_volume_ratio = float(theme["volumeRatioSum"]) / count
                    turnover_score = min(1.0, math.log1p(float(theme["turnover"])) / max(1.0, math.log1p(max_turnover)))
                    strength = (
                        avg_change * 0.16
                        + (advancer_ratio - 0.5) * 1.2
                        + min(avg_volume_ratio, 3.0) * 0.18
                        + turnover_score * 0.22
                    )
                    leaders = sorted(theme["leaders"], key=lambda item: float(item["changePct"] or 0), reverse=True)[:5]
                    themes.append(
                        {
                            "themeId": theme["themeId"],
                            "name": theme["name"],
                            "avgChangePct": avg_change,
                            "advancerRatio": advancer_ratio,
                            "avgVolumeRatio": avg_volume_ratio,
                            "turnover": float(theme["turnover"]),
                            "strength": strength,
                            "count": int(theme["count"]),
                            "earningsCount": int(theme["earningsCount"]),
                            "leaders": leaders,
                        }
                    )
                themes.sort(key=lambda item: float(item["strength"]), reverse=True)
                ordered_frames.append({"asof": asof_ts, "label": frame["label"], "themes": themes})

            if not ordered_frames:
                return {"themes": [], "frames": [], "excludeEarnings": exclude_earnings}

            latest = ordered_frames[-1]
            previous_by_theme = {
                theme["themeId"]: theme
                for theme in (ordered_frames[-2]["themes"] if len(ordered_frames) >= 2 else [])
            }
            baseline_by_theme: dict[str, float] = {}
            for frame in ordered_frames[:-1]:
                for theme in frame["themes"]:
                    baseline_by_theme.setdefault(str(theme["themeId"]), 0.0)
            for theme_id in list(baseline_by_theme):
                values = [
                    float(theme["strength"])
                    for frame in ordered_frames[:-1]
                    for theme in frame["themes"]
                    if str(theme["themeId"]) == theme_id
                ]
                baseline_by_theme[theme_id] = sum(values) / len(values) if values else 0.0
            latest_themes = []
            for theme in latest["themes"]:
                theme_id = str(theme["themeId"])
                previous = previous_by_theme.get(theme_id)
                baseline = baseline_by_theme.get(theme_id, 0.0)
                strength = float(theme["strength"])
                previous_strength = float(previous["strength"]) if previous else None
                clone = dict(theme)
                clone["previousStrength"] = previous_strength
                clone["baselineStrength"] = baseline
                clone["strengthDelta"] = strength - (previous_strength if previous_strength is not None else baseline)
                clone["status"] = _theme_status(strength, baseline, previous_strength)
                latest_themes.append(clone)
            latest_themes.sort(key=lambda item: float(item["strength"]), reverse=True)
            payload = {
                "asof": latest["asof"],
                "label": latest["label"],
                "themes": latest_themes[:16],
                "frames": ordered_frames,
                "excludeEarnings": exclude_earnings,
                "scope": "watchlist",
                "watchlistCount": len(watchlist_codes),
                "status": "ok",
            }
            _theme_api_cache_put(cache_key, payload)
            return payload
    except Exception as exc:
        logger.exception("theme rotation fetch failed: %s", exc)
        return None


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
        with try_get_conn_for_path(db_path, timeout_sec=0.8, read_only=True) as conn:
            if conn is None:
                logger.warning("heatmap diagnostics skipped: db unavailable")
                return diagnostics
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
        with try_get_conn_for_path(db_path, timeout_sec=0.8, read_only=True) as conn:
            if conn is None:
                logger.warning("heatmap fetch skipped: db unavailable")
                return None
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
        with try_get_conn_for_path(db_path, timeout_sec=0.8, read_only=True) as conn:
            if conn is None:
                logger.warning("heatmap timeline skipped: db unavailable")
                return None
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


@router.get("/theme-rotation")
def get_market_theme_rotation(
    limit: int = Query(12, ge=3, le=40),
    exclude_earnings: bool = Query(False),
):
    """
    Returns a display-only theme rotation summary for the market screen.

    Theme names are assigned from a small MeeMee display dictionary first and
    fall back to the JPX 33-sector name when no theme rule matches.
    """
    payload = _fetch_theme_rotation(limit=limit, exclude_earnings=exclude_earnings)
    computed_from = "theme_dictionary" if payload is not None else "fallback"
    if payload is None:
        payload = {"themes": [], "frames": [], "excludeEarnings": exclude_earnings}
    if payload.get("status") == "db_unavailable":
        computed_from = "db_unavailable_fallback"
        payload = {
            **payload,
            "degraded": True,
            "retryable": True,
        }
    diagnostics = (
        _build_heatmap_diagnostics("1d")
        if os.getenv("MEEMEE_DEV", "").lower() in ("1", "true", "yes", "on")
        or os.getenv("MEEMEE_SELFTEST", "").lower() in ("1", "true", "yes", "on")
        else None
    )
    if diagnostics is not None:
        diagnostics["computed_from"] = computed_from
    return {**payload, "diagnostics": diagnostics}


@router.get("/theme-candidates")
def get_market_theme_candidates(
    mode: str = Query("entry_ease", pattern="^(entry_ease|value_range|continuation)$"),
    limit: int = Query(12, ge=3, le=40),
    exclude_earnings: bool = Query(False),
):
    """
    Returns a fixed-score candidate list for MeeMee display/confirmation.

    This is not a TRADEX challenger or production ranking source. It exposes
    deterministic chart/theme reason codes so a human or Codex can compare the
    same candidate with the same criteria across questions.
    """
    payload = _fetch_theme_candidates(mode=mode, limit=limit, exclude_earnings=exclude_earnings)
    computed_from = "fixed_candidate_score" if payload is not None else "fallback"
    if payload is None:
        payload = {"items": [], "mode": mode, "excludeEarnings": exclude_earnings}
    if payload.get("status") == "db_unavailable":
        computed_from = "db_unavailable_fallback"
        payload = {
            **payload,
            "degraded": True,
            "retryable": True,
        }
    diagnostics = (
        _build_heatmap_diagnostics("1d")
        if os.getenv("MEEMEE_DEV", "").lower() in ("1", "true", "yes", "on")
        or os.getenv("MEEMEE_SELFTEST", "").lower() in ("1", "true", "yes", "on")
        else None
    )
    if diagnostics is not None:
        diagnostics["computed_from"] = computed_from
    return {**payload, "diagnostics": diagnostics}


@router.get("/baskets")
def get_market_baskets() -> dict[str, Any]:
    baskets = market_basket_catalog()
    return {"baskets": baskets, "count": len(baskets), "status": "ok"}


@router.get("/detail-check/{code}")
def get_market_detail_check(code: str):
    """
    Returns a screenshot-free visual selection summary for one ticker.

    The endpoint converts daily/weekly/monthly confirmed bars into the same
    checks a human would inspect on the detail chart: candle shape, MA position,
    support/resistance, extension, and multi-timeframe setup.
    """
    payload = _fetch_detail_visual_check(code)
    if payload is None:
        raise HTTPException(status_code=503, detail="market_detail_check_unavailable")
    if payload.get("status") == "db_unavailable":
        raise HTTPException(status_code=503, detail="market_detail_check_db_unavailable")
    return payload


@router.get("/detail-chart-image/{code}")
def get_market_detail_chart_image(
    code: str,
    max_bars: int = Query(90, ge=30, le=180),
):
    """
    Returns a lightweight SVG chart image for screenshot-free visual review.

    The image is generated from confirmed daily/monthly bars and an aggregated
    weekly view. It is meant for quick Codex/human inspection, not for ranking.
    """
    code_text = str(code or "").strip()
    rows = _fetch_confirmed_chart_rows(code_text)
    if rows is None:
        raise HTTPException(status_code=503, detail="market_detail_chart_image_db_unavailable")
    meta = _fetch_detail_visual_check(code_text)
    title = str(meta.get("name") or code_text) if meta else code_text
    svg = _render_svg_candles(code=code_text, title=title, row_sets=rows, max_bars=max_bars)
    return Response(content=svg, media_type="image/svg+xml")
