from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from app.backend.tdnetdb.schema import ensure_tdnetdb_schema, utcnow_naive
from app.db.session import get_conn_for_path


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is None else value.astimezone().replace(tzinfo=None)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo is None else parsed.astimezone().replace(tzinfo=None)


_WS_RE = re.compile(r"\s+")


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return _WS_RE.sub(" ", text)


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _build_feature_row(
    *,
    disclosure_id: str,
    sec_code: str | None,
    title: str | None,
    category: str | None,
    summary_text: str | None,
    published_at: datetime | None,
    fetched_at: datetime,
) -> list[Any]:
    title_norm = _normalize_text(title)
    category_norm = _normalize_text(category)
    summary_norm = _normalize_text(summary_text)
    combined = " ".join(part for part in (title_norm, category_norm, summary_norm) if part).strip()

    forecast_revision = _contains_any(combined, ("業績予想", "forecast", "修正", "上方修正", "下方修正"))
    dividend_revision = _contains_any(combined, ("配当", "dividend"))
    share_buyback = _contains_any(combined, ("自己株式", "自社株買", "buyback", "repurchase"))
    share_split = _contains_any(combined, ("株式分割", "split", "併合"))
    earnings = _contains_any(combined, ("決算短信", "earnings", "financial results"))
    governance = _contains_any(combined, ("ガバナンス", "取締役", "監査", "役員", "governance"))
    distress = _contains_any(combined, ("特別損失", "下方修正", "継続企業", "債務超過", "赤字", "loss"))

    event_type = "other"
    if forecast_revision:
        event_type = "forecast_revision"
    elif dividend_revision:
        event_type = "dividend_revision"
    elif share_buyback:
        event_type = "share_buyback"
    elif share_split:
        event_type = "share_split"
    elif earnings:
        event_type = "earnings"
    elif governance:
        event_type = "governance"

    sentiment = "neutral"
    if _contains_any(combined, ("上方修正", "増配", "自己株式取得", "share buyback", "buyback")):
        sentiment = "positive"
    elif _contains_any(combined, ("下方修正", "減配", "特別損失", "債務超過", "赤字")):
        sentiment = "negative"

    importance_score = 0.2
    if forecast_revision or dividend_revision or share_buyback or share_split or earnings:
        importance_score = 0.75
    if distress:
        importance_score = max(importance_score, 0.9)

    tags = [
        key
        for key, enabled in (
            ("forecast_revision", forecast_revision),
            ("dividend_revision", dividend_revision),
            ("share_buyback", share_buyback),
            ("share_split", share_split),
            ("earnings", earnings),
            ("governance", governance),
            ("distress", distress),
        )
        if enabled
    ]
    return [
        disclosure_id,
        sec_code,
        published_at,
        event_type,
        sentiment,
        float(importance_score),
        bool(forecast_revision),
        bool(dividend_revision),
        bool(share_buyback),
        bool(share_split),
        bool(earnings),
        bool(governance),
        bool(distress),
        title_norm,
        _json_dumps(tags),
        combined,
        fetched_at,
    ]


class TdnetdbRepository:
    def __init__(self, db_path: str | Path):
        self._db_path = str(Path(db_path).expanduser().resolve())

    def _connect_read(self):
        return get_conn_for_path(self._db_path, timeout_sec=2.5, read_only=True)

    def _connect_write(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self._db_path)
        ensure_tdnetdb_schema(conn)
        return conn

    def upsert_disclosures(self, items: list[dict[str, Any]], *, fetched_at: datetime | None = None) -> int:
        fetched_at = fetched_at or utcnow_naive()
        rows: list[list[Any]] = []
        feature_rows: list[list[Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            disclosure_id = _text(item.get("disclosure_id") or item.get("disclosureId") or item.get("id"))
            if not disclosure_id:
                continue
            sec_code = _text(item.get("sec_code") or item.get("secCode") or item.get("code"))
            title = _text(item.get("title"))
            category = _text(item.get("category"))
            published_at = _parse_datetime(item.get("published_at") or item.get("publishedAt") or item.get("disclosedAt"))
            summary_text = _text(item.get("summary_text") or item.get("summaryText") or item.get("summary"))
            rows.append(
                [
                    disclosure_id,
                    sec_code,
                    _text(item.get("company_name") or item.get("companyName") or item.get("name")),
                    title,
                    category,
                    published_at,
                    _text(item.get("tdnet_url") or item.get("tdnetUrl") or item.get("url")),
                    _text(item.get("pdf_url") or item.get("pdfUrl")),
                    _text(item.get("xbrl_url") or item.get("xbrlUrl")),
                    summary_text,
                    _json_dumps(item),
                    fetched_at,
                ]
            )
            feature_rows.append(
                _build_feature_row(
                    disclosure_id=disclosure_id,
                    sec_code=sec_code,
                    title=title,
                    category=category,
                    summary_text=summary_text,
                    published_at=published_at,
                    fetched_at=fetched_at,
                )
            )
        if not rows:
            return 0
        with self._connect_write() as conn:
            conn.executemany(
                """
                INSERT INTO tdnet_disclosures (
                    disclosure_id, sec_code, company_name, title, category,
                    published_at, tdnet_url, pdf_url, xbrl_url, summary_text, raw_json, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(disclosure_id) DO UPDATE SET
                    sec_code = excluded.sec_code,
                    company_name = excluded.company_name,
                    title = excluded.title,
                    category = excluded.category,
                    published_at = excluded.published_at,
                    tdnet_url = excluded.tdnet_url,
                    pdf_url = excluded.pdf_url,
                    xbrl_url = excluded.xbrl_url,
                    summary_text = excluded.summary_text,
                    raw_json = excluded.raw_json,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
            conn.executemany(
                """
                INSERT INTO tdnet_disclosure_features (
                    disclosure_id, sec_code, published_at, event_type, sentiment, importance_score,
                    forecast_revision, dividend_revision, share_buyback, share_split,
                    earnings, governance, distress, title_normalized, tags_json, raw_text, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(disclosure_id) DO UPDATE SET
                    sec_code = excluded.sec_code,
                    published_at = excluded.published_at,
                    event_type = excluded.event_type,
                    sentiment = excluded.sentiment,
                    importance_score = excluded.importance_score,
                    forecast_revision = excluded.forecast_revision,
                    dividend_revision = excluded.dividend_revision,
                    share_buyback = excluded.share_buyback,
                    share_split = excluded.share_split,
                    earnings = excluded.earnings,
                    governance = excluded.governance,
                    distress = excluded.distress,
                    title_normalized = excluded.title_normalized,
                    tags_json = excluded.tags_json,
                    raw_text = excluded.raw_text,
                    fetched_at = excluded.fetched_at
                """,
                feature_rows,
            )
        return len(rows)

    def list_disclosures_by_code(self, sec_code: str, *, limit: int = 20) -> list[dict[str, Any]]:
        code = str(sec_code or "").strip()
        if not code:
            return []
        with self._connect_read() as conn:
            rows = conn.execute(
                """
                SELECT d.disclosure_id, d.sec_code, d.company_name, d.title, d.category,
                       d.published_at, tdnet_url, pdf_url, xbrl_url, summary_text, d.fetched_at,
                       f.event_type, f.sentiment, f.importance_score, f.tags_json
                FROM tdnet_disclosures d
                LEFT JOIN tdnet_disclosure_features f ON f.disclosure_id = d.disclosure_id
                WHERE d.sec_code = ?
                ORDER BY d.published_at DESC NULLS LAST, d.fetched_at DESC NULLS LAST, d.disclosure_id DESC
                LIMIT ?
                """,
                [code, int(limit)],
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "disclosureId": row[0],
                    "secCode": row[1],
                    "companyName": row[2],
                    "title": row[3],
                    "category": row[4],
                    "publishedAt": row[5].isoformat() if isinstance(row[5], datetime) else None,
                    "tdnetUrl": row[6],
                    "pdfUrl": row[7],
                    "xbrlUrl": row[8],
                    "summaryText": row[9],
                    "fetchedAt": row[10].isoformat() if isinstance(row[10], datetime) else None,
                    "eventType": row[11],
                    "sentiment": row[12],
                    "importanceScore": float(row[13]) if row[13] is not None else None,
                    "tags": json.loads(row[14]) if row[14] else [],
                }
            )
        return items
