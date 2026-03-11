"""
Download JPX listed company Excel and populate DuckDB industry_master.

Usage:
  python tools/setup/import_industry_master.py
  python tools/setup/import_industry_master.py --excel-url <url>
  python tools/setup/import_industry_master.py --excel-path <path>
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from typing import Iterable, Optional
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import duckdb
import pandas as pd

# Add project root to path for app.* imports
sys.path.append(os.getcwd())

from app.backend.core.config import config

DEFAULT_STATS_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
USER_AGENT = "Mozilla/5.0 (MeeMeeScreener)"

REQUIRED_FIELDS = ["code", "name", "sector33_code", "sector33_name", "market_code"]

COLUMN_ALIASES = {
    "code": ["コード", "銘柄コード", "証券コード", "コード(4桁)", "Code"],
    "name": ["銘柄名", "会社名", "銘柄名称", "名称", "Company Name"],
    "sector33_code": ["33業種コード", "３３業種コード", "33業種ｺｰﾄﾞ", "33業種コード(新)", "33業種"],
    "sector33_name": ["33業種区分", "３３業種区分", "33業種区分(新)", "33業種区分", "33業種名"],
    "market_code": ["市場・商品区分", "市場区分", "市場", "市場・商品区分／コード", "市場・商品区分コード"],
}


def normalize_label(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("　", " ").strip()
    text = re.sub(r"\s+", "", text)
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("／", "/")
    text = text.replace("・", "")
    return text


def build_column_map(columns: Iterable[object]) -> Optional[dict[str, str]]:
    normalized = {normalize_label(col): col for col in columns}
    mapping: dict[str, str] = {}
    for key, aliases in COLUMN_ALIASES.items():
        target = None
        for alias in aliases:
            alias_norm = normalize_label(alias)
            for col_norm, col_original in normalized.items():
                if alias_norm and alias_norm in col_norm:
                    target = col_original
                    break
            if target is not None:
                break
        if target is None:
            return None
        mapping[target] = key
    return mapping


def pick_sheet(book: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, dict[str, str]]:
    for _, frame in book.items():
        mapping = build_column_map(frame.columns)
        if mapping is not None:
            return frame, mapping
    names = list(book.keys())
    raise RuntimeError(f"Required columns not found in sheets: {names}")


def normalize_code(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    text = re.sub(r"\D", "", text)
    if not text:
        return None
    return text.zfill(4)


def fetch_html(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req) as response:
        return response.read()


def find_excel_url(html: bytes, base_url: str) -> Optional[str]:
    text = html.decode("utf-8", "ignore")
    links = re.findall(r"href=[\"']([^\"']+)[\"']", text, flags=re.IGNORECASE)
    excel_links = [link for link in links if re.search(r"\.xls[x]?$", link, flags=re.IGNORECASE)]
    if not excel_links:
        return None

    prioritized = []
    for link in excel_links:
        lower = link.lower()
        if any(token in lower for token in ["listed", "meigara", "tse", "stock", "securities", "toukyou"]):
            prioritized.append(link)
    candidates = prioritized or excel_links
    return urljoin(base_url, candidates[0])


def load_excel_bytes(excel_url: str) -> bytes:
    req = Request(excel_url, headers={"User-Agent": USER_AGENT})
    with urlopen(req) as response:
        return response.read()


def load_dataframe(data: bytes, excel_url: str) -> pd.DataFrame:
    ext = os.path.splitext(excel_url)[1].lower()
    engine = "openpyxl" if ext == ".xlsx" else None
    book = pd.read_excel(io.BytesIO(data), sheet_name=None, engine=engine)
    frame, mapping = pick_sheet(book)
    frame = frame.rename(columns=mapping)
    frame = frame[REQUIRED_FIELDS].copy()
    frame = frame.fillna("")

    frame["code"] = frame["code"].map(normalize_code)
    frame["name"] = frame["name"].astype(str).str.strip()
    frame["sector33_code"] = frame["sector33_code"].astype(str).str.strip()
    frame["sector33_name"] = frame["sector33_name"].astype(str).str.strip()
    frame["market_code"] = frame["market_code"].astype(str).str.strip()

    frame = frame[frame["code"].notna() & (frame["code"] != "")]
    return frame


def write_to_duckdb(frame: pd.DataFrame, db_path: str, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] rows: {len(frame)}")
        return
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS industry_master (
                code VARCHAR PRIMARY KEY,
                name VARCHAR,
                sector33_code VARCHAR,
                sector33_name VARCHAR,
                market_code VARCHAR
            )
            """
        )
        conn.execute("DELETE FROM industry_master")
        conn.register("industry_df", frame)
        conn.execute(
            """
            INSERT INTO industry_master (code, name, sector33_code, sector33_name, market_code)
            SELECT code, name, sector33_code, sector33_name, market_code FROM industry_df
            """
        )
    print(f"industry_master updated: {len(frame)} rows")


def main() -> int:
    parser = argparse.ArgumentParser(description="Import JPX industry master into DuckDB")
    parser.add_argument("--page-url", default=DEFAULT_STATS_URL, help="JPX stats page URL")
    parser.add_argument("--excel-url", default=None, help="Direct Excel URL override")
    parser.add_argument("--excel-path", default=None, help="Local Excel file path override")
    parser.add_argument("--db", default=str(config.DB_PATH), help="DuckDB path")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, do not write")
    args = parser.parse_args()

    excel_url = args.excel_url
    excel_bytes: Optional[bytes] = None

    if args.excel_path:
        with open(args.excel_path, "rb") as f:
            excel_bytes = f.read()
        excel_url = args.excel_path
    else:
        if not excel_url:
            html = fetch_html(args.page_url)
            excel_url = find_excel_url(html, args.page_url)
        if not excel_url:
            print("Failed to find Excel URL. Use --excel-url or --excel-path.")
            return 1
        excel_bytes = load_excel_bytes(excel_url)

    frame = load_dataframe(excel_bytes, excel_url)
    if frame.empty:
        print("No rows found in industry master source.")
        return 1

    write_to_duckdb(frame, args.db, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
