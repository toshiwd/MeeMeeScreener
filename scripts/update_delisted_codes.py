from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = REPO_ROOT / "config" / "delisted_codes.json"
DEFAULT_CODE_LIST_PATH = REPO_ROOT / "tools" / "code.txt"
DEFAULT_JPX_URL = "https://www.jpx.co.jp/listing/stocks/delisted/"
DEFAULT_DB_TABLES = (
    "daily_bars",
    "daily_ma",
    "monthly_bars",
    "monthly_ma",
    "stock_meta",
    "tickers",
    "feature_snapshot_daily",
    "ml_feature_daily",
    "ml_label_20d",
    "ml_pred_20d",
    "label_20d",
    "phase_pred_daily",
    "sell_analysis_daily",
    "signal_basis_daily",
    "signal_decision_daily",
    "signal_occurrence",
    "ranking_appearance_daily",
    "ranking_candidate_decisions",
)


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._in_cell = False
        self._in_row = False
        self._current_row: list[str] = []
        self._current_cell: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._in_row = True
            self._current_row = []
        elif tag in {"td", "th"} and self._in_row:
            self._in_cell = True
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._in_cell:
            value = re.sub(r"\s+", " ", "".join(self._current_cell)).strip()
            self._current_row.append(value)
            self._in_cell = False
        elif tag == "tr" and self._in_row:
            if self._current_row:
                self.rows.append(self._current_row)
            self._in_row = False


def _today_jst() -> date:
    return (datetime.now(timezone.utc).astimezone().date())


def _parse_date(value: str) -> str | None:
    text = str(value or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def fetch_jpx_delisted_rows(url: str = DEFAULT_JPX_URL) -> list[dict[str, str]]:
    request = Request(url, headers={"User-Agent": "MeeMeeScreener/1.0"})
    with urlopen(request, timeout=30) as response:
        raw = response.read()
    html = raw.decode("utf-8", errors="replace")
    return parse_jpx_delisted_html(html)


def parse_jpx_delisted_html(html: str) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(html)
    records: list[dict[str, str]] = []
    for row in parser.rows:
        if len(row) < 3:
            continue
        delisted_on = _parse_date(row[0])
        code = str(row[2]).strip()
        if delisted_on is None or not re.fullmatch(r"\d{4}", code):
            continue
        records.append(
            {
                "code": code,
                "name": str(row[1]).strip(),
                "delisted_on": delisted_on,
                "market": str(row[3]).strip() if len(row) > 3 else "",
                "reason": str(row[4]).strip() if len(row) > 4 else "",
                "source": "JPX",
            }
        )
    if not records:
        raise RuntimeError("JPX delisted table parse returned no records")
    return records


def load_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": "meemee_delisted_codes_v1", "codes": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def merge_registry(existing: dict[str, Any], rows: list[dict[str, str]]) -> dict[str, Any]:
    codes = dict(existing.get("codes") or {})
    for row in rows:
        code = row["code"]
        previous = codes.get(code) if isinstance(codes.get(code), dict) else {}
        codes[code] = {
            **previous,
            "delisted_on": row["delisted_on"],
            "name": row["name"],
            "market": row["market"],
            "reason": row["reason"],
            "source": row["source"],
        }
    return {
        "schema_version": str(existing.get("schema_version") or "meemee_delisted_codes_v1"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source_url": DEFAULT_JPX_URL,
        "codes": dict(sorted(codes.items())),
    }


def effective_codes(registry: dict[str, Any], *, as_of: date) -> set[str]:
    out: set[str] = set()
    for code, meta in (registry.get("codes") or {}).items():
        if not isinstance(meta, dict):
            continue
        delisted_on = _parse_date(str(meta.get("delisted_on") or ""))
        if delisted_on and datetime.strptime(delisted_on, "%Y-%m-%d").date() <= as_of:
            out.add(str(code))
    return out


def delete_codes_from_db(db_path: Path, codes: set[str], *, dry_run: bool) -> dict[str, Any]:
    if not codes:
        return {"db_path": str(db_path), "dry_run": dry_run, "codes": [], "tables": {}}
    table_results: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as conn:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        for table in DEFAULT_DB_TABLES:
            if table not in tables:
                continue
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE code IN (SELECT UNNEST(?))",
                [sorted(codes)],
            ).fetchone()[0]
            table_results[table] = int(count or 0)
            if not dry_run and count:
                conn.execute(
                    f"DELETE FROM {table} WHERE code IN (SELECT UNNEST(?))",
                    [sorted(codes)],
                )
    return {"db_path": str(db_path), "dry_run": dry_run, "codes": sorted(codes), "tables": table_results}


def remove_codes_from_code_list(path: Path, codes: set[str], *, dry_run: bool) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "dry_run": dry_run, "removed_codes": []}
    before = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    remove = set(str(code) for code in codes)
    after = [code for code in before if code not in remove]
    removed = sorted(set(before) - set(after), key=lambda value: (0, int(value)) if value.isdigit() else (1, value))
    if not dry_run and removed:
        path.write_text("\n".join(after) + "\n", encoding="utf-8")
    return {
        "path": str(path),
        "exists": True,
        "dry_run": dry_run,
        "before_count": len(before),
        "after_count": len(after),
        "removed_codes": removed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry-path", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--code-list-path", type=Path, default=DEFAULT_CODE_LIST_PATH)
    parser.add_argument("--skip-code-list", action="store_true")
    parser.add_argument("--db-path", type=Path, action="append", default=[])
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-fetch", action="store_true")
    args = parser.parse_args()

    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date() if args.as_of else _today_jst()
    existing = load_registry(args.registry_path)
    rows = [] if args.skip_fetch else fetch_jpx_delisted_rows()
    updated = merge_registry(existing, rows) if rows else existing
    if not args.dry_run and rows:
        args.registry_path.parent.mkdir(parents=True, exist_ok=True)
        args.registry_path.write_text(json.dumps(updated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    codes = effective_codes(updated, as_of=as_of)
    db_results = [delete_codes_from_db(path, codes, dry_run=args.dry_run) for path in args.db_path]
    code_list_result = (
        None
        if args.skip_code_list
        else remove_codes_from_code_list(args.code_list_path, codes, dry_run=args.dry_run)
    )
    print(
        json.dumps(
            {
                "registry_path": str(args.registry_path),
                "fetched_count": len(rows),
                "registry_count": len(updated.get("codes") or {}),
                "effective_as_of": as_of.isoformat(),
                "effective_count": len(codes),
                "dry_run": bool(args.dry_run),
                "code_list_result": code_list_result,
                "db_results": db_results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
