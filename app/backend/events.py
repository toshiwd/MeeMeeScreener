from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable
import os
import re
import unicodedata
import urllib.parse
import urllib.request

try:
    import jpholiday
except ImportError:  # Optional at runtime; refresh will error if missing.
    jpholiday = None

try:
    import pandas as pd
except ImportError:  # Optional at runtime; refresh will error if missing.
    pd = None
from zoneinfo import ZoneInfo

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_RAW_DIR = os.path.join(REPO_ROOT, "data_store", "raw")
EVENTS_RAW_DIR = os.path.abspath(os.getenv("EVENTS_RAW_DIR") or DEFAULT_RAW_DIR)

EARNINGS_PAGE_URL = os.getenv(
    "JPX_EARNINGS_PAGE_URL",
    "https://www.jpx.co.jp/listing/event-schedules/financial-announcement/"
)
RIGHTS_PAGE_URL = os.getenv(
    "JPX_RIGHTS_PAGE_URL",
    "https://www.jpx.co.jp/listing/others/ex-rights/"
)
EARNINGS_URLS_ENV = os.getenv("JPX_EARNINGS_XLSX_URLS")
RIGHTS_URLS_ENV = os.getenv("JPX_RIGHTS_XLSX_URLS")

JST = ZoneInfo("Asia/Tokyo")


def jst_now() -> datetime:
    return datetime.now(JST)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", "", text)
    return text.lower()




def _normalize_code(value: object) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    match = re.search(r"\d{4}", text)
    if match:
        return match.group(0)
    return None


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        if value > 10_000_000:
            yyyymmdd = str(int(value))
            if len(yyyymmdd) == 8:
                try:
                    return datetime.strptime(yyyymmdd, "%Y%m%d").date()
                except ValueError:
                    return None
        try:
            base = datetime(1899, 12, 30)
            return (base + timedelta(days=float(value))).date()
        except (ValueError, OverflowError):
            return None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(trimmed, fmt).date()
            except ValueError:
                continue
        if re.match(r"^\d{8}$", trimmed):
            try:
                return datetime.strptime(trimmed, "%Y%m%d").date()
            except ValueError:
                return None
    return None


def _safe_text(value: object | None) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text if text else None


def _find_column(columns: Iterable[object], keywords: list[str]) -> object | None:
    normalized_keywords = [_normalize_text(item) for item in keywords if item]
    for col in columns:
        normalized = _normalize_text(col)
        if not normalized:
            continue
        if any(keyword in normalized for keyword in normalized_keywords):
            return col
    return None


def _locate_header(df: pd.DataFrame, keywords: list[str]) -> pd.DataFrame | None:
    # Check if current columns match
    if _find_column(df.columns, keywords):
        return df
    
    # Search first 20 rows
    for i in range(min(20, len(df))):
        row_values = df.iloc[i].astype(str).tolist()
        if _find_column(row_values, keywords):
            # Found header at row i
            # Make sure we don't have duplicate columns
            new_header = df.iloc[i]
            new_df = df.iloc[i+1:].copy()
            new_df.columns = new_header
            return new_df
    return None


def _load_excel_sheets(path: str) -> list[pd.DataFrame]:
    if pd is None:
        raise RuntimeError("pandas_not_installed")
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsx":
        workbook = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    else:
        workbook = pd.read_excel(path, sheet_name=None)
    return list(workbook.values())


def _download_file(url: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    parsed = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed.path) or "data.xlsx"
    dest_path = os.path.join(dest_dir, filename)
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request) as response, open(dest_path, "wb") as handle:
        handle.write(response.read())
    return dest_path


def _discover_excel_urls(page_url: str) -> list[str]:
    try:
        request = urllib.request.Request(page_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(request) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return []
    urls = re.findall(r'href=["\']([^"\']+\.(?:xls|xlsx))["\']', html, flags=re.IGNORECASE)
    resolved: list[str] = []
    for href in urls:
        resolved.append(urllib.parse.urljoin(page_url, href))
    return list(dict.fromkeys(resolved))


def _resolve_urls(env_value: str | None, fallback_page: str) -> list[str]:
    if env_value:
        return [item.strip() for item in env_value.split(",") if item.strip()]
    return _discover_excel_urls(fallback_page)


def _is_business_day(target: date) -> bool:
    if target.weekday() >= 5:
        return False
    if jpholiday is None:
        return True
    return not jpholiday.is_holiday(target)




def _previous_business_day(target: date) -> date:
    cursor = target - timedelta(days=1)
    while not _is_business_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def fetch_earnings_snapshot() -> list[dict]:
    urls = _resolve_urls(EARNINGS_URLS_ENV, EARNINGS_PAGE_URL)
    if not urls:
        raise RuntimeError("earnings_excel_urls_not_found")
    fetched_at = jst_now().replace(tzinfo=None)
    token = fetched_at.strftime("%Y%m%d")
    dest_dir = os.path.join(EVENTS_RAW_DIR, "jpx_financial_announcement", token)
    rows: list[dict] = []
    for url in urls:
        path = _download_file(url, dest_dir)
        for sheet in _load_excel_sheets(path):
            if sheet.empty:
                continue
            
            sheet = _locate_header(sheet, ["\u30b3\u30fc\u30c9", "\u9298\u67c4\u30b3\u30fc\u30c9", "\u4f1a\u793e\u30b3\u30fc\u30c9", "companycode"])
            if sheet is None:
                continue

            code_col = _find_column(sheet.columns, ["\u30b3\u30fc\u30c9", "\u9298\u67c4\u30b3\u30fc\u30c9", "\u4f1a\u793e\u30b3\u30fc\u30c9", "companycode"])
            date_col = _find_column(sheet.columns, ["\u6c7a\u7b97\u767a\u8868\u65e5", "\u767a\u8868\u65e5", "\u767a\u8868\u4e88\u5b9a\u65e5", "\u4e88\u5b9a\u65e5", "date"])
            kind_col = _find_column(sheet.columns, ["\u7a2e\u5225", "\u533a\u5206", "\u7a2e\u985e", "\u6c7a\u7b97\u7a2e\u5225", "type"])
            name_col = _find_column(sheet.columns, ["\u4f1a\u793e\u540d", "\u9298\u67c4\u540d", "\u540d\u79f0", "name"])
            if not code_col or not date_col:
                continue
            for _, record in sheet.iterrows():
                code = _normalize_code(record.get(code_col))
                planned_date = _parse_date(record.get(date_col))
                if not code or not planned_date:
                    continue
                rows.append(
                    {
                        "code": code,
                        "planned_date": planned_date,
                        "kind": _safe_text(record.get(kind_col)) if kind_col else None,
                        "company_name": _safe_text(record.get(name_col)) if name_col else None,
                        "source": "JPX",
                        "fetched_at": fetched_at
                    }
                )
    deduped: list[dict] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (row.get("code"), row.get("planned_date"), row.get("kind"), row.get("company_name"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def fetch_rights_snapshot() -> list[dict]:
    urls = _resolve_urls(RIGHTS_URLS_ENV, RIGHTS_PAGE_URL)
    if not urls:
        raise RuntimeError("rights_excel_urls_not_found")
    fetched_at = jst_now().replace(tzinfo=None)
    token = fetched_at.strftime("%Y%m%d")
    dest_dir = os.path.join(EVENTS_RAW_DIR, "jpx_ex_rights", token)
    rows: list[dict] = []
    for url in urls:
        path = _download_file(url, dest_dir)
        for sheet in _load_excel_sheets(path):
            if sheet.empty:
                continue
            
            sheet = _locate_header(sheet, ["\u30b3\u30fc\u30c9", "\u9298\u67c4\u30b3\u30fc\u30c9", "companycode"])
            if sheet is None:
                continue

            code_col = _find_column(sheet.columns, ["\u30b3\u30fc\u30c9", "\u9298\u67c4\u30b3\u30fc\u30c9", "companycode"])
            ex_date_col = _find_column(sheet.columns, ["\u6a29\u5229\u843d\u65e5", "\u914d\u5f53\u843d\u65e5", "\u6a29\u5229\u843d", "\u914d\u5f53\u843d", "ex-date", "exdate"])
            record_col = _find_column(sheet.columns, ["\u6a29\u5229\u78ba\u5b9a\u65e5", "\u78ba\u5b9a\u65e5", "recorddate", "record date"])
            category_col = _find_column(sheet.columns, ["\u533a\u5206", "\u7a2e\u5225", "\u5185\u5bb9", "category"])
            last_rights_col = _find_column(sheet.columns, ["\u6a29\u5229\u4ed8\u304d\u6700\u7d42\u65e5", "\u6a29\u5229\u4ed8\u6700\u7d42\u65e5", "\u6700\u7d42\u65e5"])
            if not code_col or not ex_date_col:
                continue
            for _, record in sheet.iterrows():
                code = _normalize_code(record.get(code_col))
                ex_date = _parse_date(record.get(ex_date_col))
                if not code or not ex_date:
                    continue
                record_date = _parse_date(record.get(record_col)) if record_col else None
                last_rights_date = _parse_date(record.get(last_rights_col)) if last_rights_col else None
                if last_rights_date is None:
                    last_rights_date = _previous_business_day(ex_date)
                rows.append(
                    {
                        "code": code,
                        "ex_date": ex_date,
                        "record_date": record_date,
                        "category": _safe_text(record.get(category_col)) if category_col else None,
                        "last_rights_date": last_rights_date,
                        "source": "JPX",
                        "fetched_at": fetched_at
                    }
                )
    deduped: list[dict] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (
            row.get("code"),
            row.get("ex_date"),
            row.get("record_date"),
            row.get("category"),
            row.get("last_rights_date")
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


DEFAULT_STATS_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
INDUSTRY_STATS_URL = os.getenv("JPX_INDUSTRY_PAGE_URL", DEFAULT_STATS_URL)
COLUMN_ALIASES = {
    "code": ["コード", "銘柄コード", "証券コード", "コード(4桁)", "Code"],
    "name": ["銘柄名", "会社名", "銘柄名称", "名称", "Company Name"],
    "sector33_code": ["33業種コード", "３３業種コード", "33業種ｺｰﾄﾞ", "33業種コード(新)", "33業種"],
    "sector33_name": ["33業種区分", "３３業種区分", "33業種区分(新)", "33業種区分", "33業種名"],
    "market_code": ["市場・商品区分", "市場区分", "市場", "市場・商品区分／コード", "市場・商品区分コード"],
}
REQUIRED_FIELDS = ["code", "name", "sector33_code", "sector33_name", "market_code"]


def _fetch_industry_excel_url(page_url: str) -> str | None:
    try:
        request = urllib.request.Request(page_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(request) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return None

    links = re.findall(r"href=[\"']([^\"']+)[\"']", html, flags=re.IGNORECASE)
    excel_links = [link for link in links if re.search(r"\.xls[x]?$", link, flags=re.IGNORECASE)]
    if not excel_links:
        return None

    prioritized = []
    for link in excel_links:
        lower = link.lower()
        if any(token in lower for token in ["listed", "meigara", "tse", "stock", "securities", "toukyou"]):
            prioritized.append(link)
    candidates = prioritized or excel_links
    return urllib.parse.urljoin(page_url, candidates[0])


def _load_industry_dataframe(data: bytes, excel_url: str) -> pd.DataFrame | None:
    import io
    ext = os.path.splitext(excel_url)[1].lower()
    engine = "openpyxl" if ext == ".xlsx" else None
    try:
        book = pd.read_excel(io.BytesIO(data), sheet_name=None, engine=engine)
    except Exception:
        return None

    frame = None
    mapping = None
    
    # helper to find mapping
    def build_column_map(columns):
        normalized = {_normalize_text(col): col for col in columns}
        current_map = {}
        for key, aliases in COLUMN_ALIASES.items():
            target = None
            for alias in aliases:
                alias_norm = _normalize_text(alias)
                for col_norm, col_original in normalized.items():
                    if alias_norm and alias_norm in col_norm:
                        target = col_original
                        break
                if target is not None:
                    break
            if target is None:
                return None
            current_map[target] = key
        return current_map

    for _, sheet_frame in book.items():
        mapping = build_column_map(sheet_frame.columns)
        if mapping is not None:
            frame = sheet_frame
            break
            
    if frame is None or mapping is None:
        return None

    frame = frame.rename(columns=mapping)
    frame = frame[REQUIRED_FIELDS].copy()
    frame = frame.fillna("")
    
    # Normalize
    def norm_code(val):
        t = _normalize_code(val)
        return t if t else None

    frame["code"] = frame["code"].map(norm_code)
    frame = frame[frame["code"].notna()]
    return frame


def fetch_industry_snapshot() -> list[dict]:
    url = _fetch_industry_excel_url(INDUSTRY_STATS_URL)
    if not url:
        raise RuntimeError("industry_excel_url_not_found")
    
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        content = response.read()

    df = _load_industry_dataframe(content, url)
    if df is None:
        raise RuntimeError("industry_dataframe_load_failed")

    rows = []
    fetched_at = jst_now().replace(tzinfo=None)
    
    for _, record in df.iterrows():
        rows.append({
            "code": str(record["code"]),
            "name": str(record["name"]).strip(),
            "sector33_code": str(record["sector33_code"]).strip(),
            "sector33_name": str(record["sector33_name"]).strip(),
            "market_code": str(record["market_code"]).strip(),
            "fetched_at": fetched_at
        })
    return rows

