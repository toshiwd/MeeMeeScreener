from __future__ import annotations

from datetime import datetime
import json
import math
import re
from typing import Any
from zoneinfo import ZoneInfo

import duckdb


JST = ZoneInfo("Asia/Tokyo")
_ALIAS_SPLIT_RE = re.compile(r"[\s_\-./()%\[\]{}:%％]+")

_ALIAS_EBITDA = (
    "ebitda",
    "償却前営業利益",
    "ebitdamargin",
    "ebitdaratio",
    "ebitda率",
)
_ALIAS_ROE = (
    "roe",
    "自己資本利益率",
    "自己資本当期純利益率",
    "returnonequity",
)
_ALIAS_EQUITY_RATIO = (
    "equityratio",
    "自己資本比率",
    "株主資本比率",
    "equitycapitalratio",
)
_ALIAS_DEBT_RATIO = (
    "debtratio",
    "debttoequity",
    "deratio",
    "d/e",
    "有利子負債比率",
    "負債比率",
)
_ALIAS_OPERATING_CF_MARGIN = (
    "operatingcfmargin",
    "operatingcashflowmargin",
    "営業cfマージン",
    "営業キャッシュフローマージン",
    "営業cf比率",
    "営業キャッシュフロー比率",
)
_ALIAS_REVENUE_GROWTH_YOY = (
    "revenuegrowthyoy",
    "salesgrowthyoy",
    "salesgrowthrate",
    "revenuegrowthrate",
    "売上成長率",
    "売上高成長率",
    "増収率",
    "売上高前年比",
)


def _normalize_key(text: object) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    merged = _ALIAS_SPLIT_RE.sub("", raw).lower()
    return merged


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        if math.isfinite(f):
            return f
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "")
    cleaned = cleaned.replace("%", "").replace("％", "")
    try:
        f = float(cleaned)
    except ValueError:
        return None
    if not math.isfinite(f):
        return None
    return f


def _collect_numeric_pairs(payload: Any) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    stack: list[tuple[str, Any]] = [("", payload)]
    while stack:
        prefix, node = stack.pop()
        if isinstance(node, dict):
            for key, value in reversed(list(node.items())):
                path = f"{prefix}.{key}" if prefix else str(key)
                stack.append((path, value))
            continue
        if isinstance(node, list):
            for idx, value in reversed(list(enumerate(node))):
                path = f"{prefix}[{idx}]"
                stack.append((path, value))
            continue
        value_f = _parse_float(node)
        if value_f is None:
            continue
        if not prefix:
            continue
        out.append((prefix, value_f))
    return out


def _find_first_metric(
    pairs_primary: list[tuple[str, float]],
    pairs_secondary: list[tuple[str, float]],
    aliases: tuple[str, ...],
) -> float | None:
    alias_norm = [_normalize_key(alias) for alias in aliases if _normalize_key(alias)]
    if not alias_norm:
        return None
    for pairs in (pairs_primary, pairs_secondary):
        for path, value in pairs:
            key_norm = _normalize_key(path)
            if not key_norm:
                continue
            for alias in alias_norm:
                if alias in key_norm:
                    return float(value)
    return None


def _json_load(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _to_ymd(value: int | str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "-" in text and len(text) >= 10:
        try:
            dt = datetime.fromisoformat(text[:10])
            return int(dt.strftime("%Y%m%d"))
        except ValueError:
            return None
    try:
        ymd = int(text)
    except ValueError:
        return None
    if 19000101 <= ymd <= 20991231:
        return ymd
    return None


def _freshness_days(*, as_of_ymd: int | None, fetched_at: datetime | None) -> int | None:
    if as_of_ymd is None or fetched_at is None:
        return None
    try:
        asof_dt = datetime.strptime(str(as_of_ymd), "%Y%m%d").replace(tzinfo=JST)
    except ValueError:
        return None
    fetched_jst = fetched_at.replace(tzinfo=JST) if fetched_at.tzinfo is None else fetched_at.astimezone(JST)
    delta = asof_dt.date() - fetched_jst.date()
    return max(0, int(delta.days))


def _freshness_score(days: int | None) -> float:
    if days is None:
        return 0.0
    if days <= 45:
        return 1.0
    if days <= 120:
        return 0.75
    if days <= 365:
        return 0.45
    return 0.20


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def load_edinet_rank_features(
    conn: duckdb.DuckDBPyConnection,
    codes: list[str],
    as_of_ymd: int | str | None,
) -> dict[str, dict[str, Any]]:
    requested = [str(code).strip() for code in codes if str(code).strip()]
    unique_codes = sorted(set(requested))
    out: dict[str, dict[str, Any]] = {}
    asof_ymd_int = _to_ymd(as_of_ymd)
    defaults = {
        "edinetStatus": "unmapped",
        "edinetMapped": False,
        "edinetFreshnessDays": None,
        "edinetMetricCount": 0,
        "edinetQualityScore": 0.0,
        "edinetDataScore": 0.0,
        "edinetEbitdaMetric": None,
        "edinetRoe": None,
        "edinetEquityRatio": None,
        "edinetDebtRatio": None,
        "edinetOperatingCfMargin": None,
        "edinetRevenueGrowthYoy": None,
    }
    for code in unique_codes:
        out[code] = dict(defaults)
    if not unique_codes:
        return out

    required_tables = (
        "edinetdb_company_map",
        "edinetdb_ratios",
        "edinetdb_financials",
    )
    if any(not _table_exists(conn, name) for name in required_tables):
        for code in unique_codes:
            out[code]["edinetStatus"] = "missing_tables"
        return out

    placeholders = ",".join(["?"] * len(unique_codes))
    rows = conn.execute(
        f"""
        WITH ratio_latest AS (
            SELECT
                edinet_code,
                payload_json,
                fetched_at,
                ROW_NUMBER() OVER (
                    PARTITION BY edinet_code
                    ORDER BY fetched_at DESC NULLS LAST, fiscal_year DESC NULLS LAST
                ) AS rn
            FROM edinetdb_ratios
        ),
        fin_latest AS (
            SELECT
                edinet_code,
                payload_json,
                fetched_at,
                ROW_NUMBER() OVER (
                    PARTITION BY edinet_code
                    ORDER BY fetched_at DESC NULLS LAST, fiscal_year DESC NULLS LAST
                ) AS rn
            FROM edinetdb_financials
        )
        SELECT
            cm.sec_code,
            cm.edinet_code,
            r.payload_json AS ratio_payload_json,
            r.fetched_at AS ratio_fetched_at,
            f.payload_json AS fin_payload_json,
            f.fetched_at AS fin_fetched_at
        FROM edinetdb_company_map cm
        LEFT JOIN ratio_latest r ON r.edinet_code = cm.edinet_code AND r.rn = 1
        LEFT JOIN fin_latest f ON f.edinet_code = cm.edinet_code AND f.rn = 1
        WHERE cm.sec_code IN ({placeholders})
        """,
        unique_codes,
    ).fetchall()

    for row in rows:
        sec_code = str(row[0] or "").strip()
        edinet_code = str(row[1] or "").strip()
        if not sec_code or not edinet_code or sec_code not in out:
            continue
        ratio_payload = _json_load(row[2])
        ratio_fetched = row[3] if isinstance(row[3], datetime) else None
        fin_payload = _json_load(row[4])
        fin_fetched = row[5] if isinstance(row[5], datetime) else None
        if ratio_payload is None and fin_payload is None:
            out[sec_code]["edinetStatus"] = "no_payload"
            out[sec_code]["edinetMapped"] = True
            continue

        ratio_pairs = _collect_numeric_pairs(ratio_payload)
        fin_pairs = _collect_numeric_pairs(fin_payload)
        ebitda = _find_first_metric(fin_pairs, ratio_pairs, _ALIAS_EBITDA)
        roe = _find_first_metric(ratio_pairs, fin_pairs, _ALIAS_ROE)
        equity_ratio = _find_first_metric(ratio_pairs, fin_pairs, _ALIAS_EQUITY_RATIO)
        debt_ratio = _find_first_metric(ratio_pairs, fin_pairs, _ALIAS_DEBT_RATIO)
        operating_cf_margin = _find_first_metric(ratio_pairs, fin_pairs, _ALIAS_OPERATING_CF_MARGIN)
        revenue_growth = _find_first_metric(fin_pairs, ratio_pairs, _ALIAS_REVENUE_GROWTH_YOY)
        metrics = [
            ebitda,
            roe,
            equity_ratio,
            debt_ratio,
            operating_cf_margin,
            revenue_growth,
        ]
        metric_count = sum(1 for v in metrics if isinstance(v, (int, float)) and math.isfinite(float(v)))
        quality_score = float(max(0.0, min(1.0, metric_count / 6.0)))
        latest_fetched = max(
            [d for d in (ratio_fetched, fin_fetched) if isinstance(d, datetime)],
            default=None,
        )
        freshness_days = _freshness_days(as_of_ymd=asof_ymd_int, fetched_at=latest_fetched)
        freshness_score = _freshness_score(freshness_days)
        data_score = float(max(0.0, min(1.0, (0.72 * quality_score) + (0.28 * freshness_score))))
        out[sec_code] = {
            "edinetStatus": "ok",
            "edinetMapped": True,
            "edinetFreshnessDays": freshness_days,
            "edinetMetricCount": int(metric_count),
            "edinetQualityScore": float(quality_score),
            "edinetDataScore": float(data_score),
            "edinetEbitdaMetric": float(ebitda) if ebitda is not None else None,
            "edinetRoe": float(roe) if roe is not None else None,
            "edinetEquityRatio": float(equity_ratio) if equity_ratio is not None else None,
            "edinetDebtRatio": float(debt_ratio) if debt_ratio is not None else None,
            "edinetOperatingCfMargin": float(operating_cf_margin) if operating_cf_margin is not None else None,
            "edinetRevenueGrowthYoy": float(revenue_growth) if revenue_growth is not None else None,
        }

    return out
