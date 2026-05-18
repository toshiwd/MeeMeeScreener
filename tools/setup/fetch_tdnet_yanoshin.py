from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://webapi.yanoshin.jp/webapi/tdnet/list"
REPORT_LINK_FIELDS: tuple[tuple[str, str], ...] = (
    ("本文PDF", "document_url"),
    ("短信サマリー", "url_report_type_summary"),
    ("連結FS", "url_report_type_fs_consolidated"),
    ("非連結FS", "url_report_type_fs_non_consolidated"),
    ("業績予想", "url_report_type_earnings_forecast"),
    ("配当予想", "url_report_type_expected_dividends"),
    ("XBRL", "url_xbrl"),
)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sec_code(value: Any, fallback: str | None) -> str | None:
    raw = _text(value)
    if raw:
        stripped = raw.strip()
        if len(stripped) >= 4:
            return stripped[:4]
        return stripped
    return _text(fallback)


def _iso_datetime(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).isoformat()
        except ValueError:
            pass
    return text.replace(" ", "T")


def _report_links(source: dict[str, Any]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for label, key in REPORT_LINK_FIELDS:
        url = _text(source.get(key))
        if not url or url in seen:
            continue
        seen.add(url)
        links.append({"label": label, "url": url})
    return links


def normalize_tdnet_item(item: dict[str, Any], *, fallback_code: str | None = None) -> dict[str, Any] | None:
    source = item.get("Tdnet") if isinstance(item.get("Tdnet"), dict) else item
    if not isinstance(source, dict):
        return None
    disclosure_id = _text(source.get("id") or source.get("disclosure_id") or source.get("disclosureId"))
    if not disclosure_id:
        return None
    document_url = _text(source.get("document_url") or source.get("documentUrl") or source.get("tdnet_url"))
    xbrl_url = _text(source.get("url_xbrl") or source.get("xbrl_url") or source.get("xbrlUrl"))
    sec_code = _sec_code(source.get("company_code") or source.get("sec_code"), fallback_code)
    markets = _text(source.get("markets_string") or source.get("markets"))
    return {
        "disclosure_id": f"yanoshin:{disclosure_id}",
        "sec_code": sec_code,
        "company_name": _text(source.get("company_name") or source.get("companyName")),
        "title": _text(source.get("title")),
        "category": markets or _text(source.get("category")),
        "published_at": _iso_datetime(source.get("pubdate") or source.get("published_at") or source.get("publishedAt")),
        "tdnet_url": document_url,
        "pdf_url": document_url if document_url and document_url.lower().endswith(".pdf") else None,
        "xbrl_url": xbrl_url,
        "summary_text": _text(source.get("summary_text") or source.get("summaryText") or source.get("update_history")),
        "provider": "yanoshin",
        "markets": markets,
        "report_links": _report_links(source),
        "raw": source,
    }


def fetch_tdnet_items(
    *,
    code: str | None,
    limit: int,
    base_url: str = DEFAULT_BASE_URL,
    timeout_sec: float = 20.0,
) -> list[dict[str, Any]]:
    condition = _text(code) or "recent"
    safe_limit = max(1, min(500, int(limit)))
    url = f"{base_url.rstrip('/')}/{condition}.json?{urlencode({'limit': safe_limit})}"
    request = Request(url, headers={"User-Agent": "MeeMeeScreener/tdnet-yanoshin-fetcher"})
    with urlopen(request, timeout=max(1.0, float(timeout_sec))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    raw_items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(raw_items, list):
        raise RuntimeError("Yanoshin TDNET response must be a list or {items: [...]}")
    normalized = [normalize_tdnet_item(item, fallback_code=code) for item in raw_items if isinstance(item, dict)]
    return [item for item in normalized if item is not None]


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Fetch TDNET disclosures from Yanoshin WEB-API as MeeMee JSON.")
    parser.add_argument("--code", default="")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout-sec", type=float, default=20.0)
    args = parser.parse_args(argv)
    items = fetch_tdnet_items(
        code=args.code,
        limit=args.limit,
        base_url=args.base_url,
        timeout_sec=args.timeout_sec,
    )
    json.dump({"items": items, "provider": "yanoshin"}, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
