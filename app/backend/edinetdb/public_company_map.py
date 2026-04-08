from __future__ import annotations

import csv
import io
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.backend.edinetdb.targets import normalize_sec_code

_CODELIST_HEADER = "ＥＤＩＮＥＴコード"
_USER_AGENT = "MeeMee-EDINET/1.0"


def _to_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _find_header_index(rows: list[list[str]]) -> int | None:
    for index, row in enumerate(rows):
        if row and _to_text(row[0]) == _CODELIST_HEADER:
            return index
    return None


def _extract_metadata(rows: list[list[str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    if rows and rows[0]:
        first = rows[0]
        if len(first) >= 2 and _to_text(first[0]) == "ダウンロード実行日":
            out["downloaded_label"] = _to_text(first[1])
        if len(first) >= 4 and _to_text(first[2]) == "件数":
            out["row_count_label"] = _to_text(first[3])
    return out


@dataclass(frozen=True)
class PublicCompanyMapSyncResult:
    source_url: str
    downloaded_label: str | None
    row_count_label: str | None
    total_rows: int
    mapped_rows: int
    rows: list[dict[str, Any]]


def download_public_company_map(*, source_url: str, timeout_sec: int) -> PublicCompanyMapSyncResult:
    request = urllib.request.Request(
        source_url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "application/zip,application/octet-stream,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        raw = response.read()
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        csv_name = next((entry.filename for entry in archive.infolist() if entry.filename.lower().endswith(".csv")), None)
        if not csv_name:
            raise RuntimeError("public_company_map_csv_missing")
        with archive.open(csv_name, "r") as fp:
            text = io.TextIOWrapper(fp, encoding="cp932", newline="")
            reader = csv.reader(text)
            csv_rows = [list(row) for row in reader]
    header_index = _find_header_index(csv_rows)
    if header_index is None:
        raise RuntimeError("public_company_map_header_missing")
    metadata = _extract_metadata(csv_rows)
    header = csv_rows[header_index]
    data_rows = csv_rows[header_index + 1 :]
    mapped_rows: list[dict[str, Any]] = []
    for raw_row in data_rows:
        if not raw_row:
            continue
        record = {header[idx]: raw_row[idx] if idx < len(raw_row) else "" for idx in range(len(header))}
        edinet_code = _to_text(record.get("ＥＤＩＮＥＴコード"))
        sec_code = normalize_sec_code(record.get("証券コード"))
        if not edinet_code or not sec_code:
            continue
        mapped_rows.append(
            {
                "sec_code": sec_code,
                "edinet_code": edinet_code,
                "name": _to_text(record.get("提出者名")) or None,
                "industry": _to_text(record.get("提出者業種")) or None,
                "updated_at": datetime.now(UTC).replace(tzinfo=None, microsecond=0),
            }
        )
    return PublicCompanyMapSyncResult(
        source_url=source_url,
        downloaded_label=metadata.get("downloaded_label"),
        row_count_label=metadata.get("row_count_label"),
        total_rows=len(data_rows),
        mapped_rows=len(mapped_rows),
        rows=mapped_rows,
    )
