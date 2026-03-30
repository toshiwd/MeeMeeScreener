from __future__ import annotations

import io
import zipfile
from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.config import JST, load_config
from app.backend.edinetdb.jobs import run_backfill_700
from app.backend.edinetdb.official_api import (
    OfficialApiResponse,
    sync_official_documents_for_codes,
    sync_recent_official_documents,
)
from app.backend.edinetdb.public_company_map import download_public_company_map
from app.backend.edinetdb.repository import EdinetdbRepository


def _build_public_company_map_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        csv_text = "\n".join(
            [
                "ダウンロード実行日,2026年03月30日現在,件数,2件",
                "ＥＤＩＮＥＴコード,提出者種別,上場区分,連結の有無,資本金,決算日,提出者名,提出者名（英字）,提出者名（ヨミ）,所在地,提出者業種,証券コード,提出者法人番号",
                '"E10001","内国法人・組合","上場","有","100","3月31日","テスト株式会社","","","","情報・通信業","13010","123"',
                '"E10002","内国法人・組合","上場","有","100","3月31日","サンプル株式会社","","","","食料品","13770","456"',
            ]
        )
        archive.writestr("EdinetcodeDlInfo.csv", csv_text.encode("cp932"))
    return buffer.getvalue()


class _BinaryResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_download_public_company_map_parses_zip(monkeypatch):
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout=0: _BinaryResponse(_build_public_company_map_zip()),
    )
    result = download_public_company_map(
        source_url="https://example.test/Edinetcode.zip",
        timeout_sec=5,
    )
    assert result.downloaded_label == "2026年03月30日現在"
    assert result.mapped_rows == 2
    assert result.rows[0]["sec_code"] == "1301"
    assert result.rows[1]["industry"] == "食料品"


def test_sync_recent_official_documents_upserts_documents_and_company_map(monkeypatch, tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)
    repo.ensure_schema()
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("EDINET_OFFICIAL_API_KEY", "official-key")
    cfg = load_config(datetime(2026, 3, 30, 9, 0, 0, tzinfo=JST))

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_json(self, path, params=None):
            return OfficialApiResponse(
                status=200,
                url="https://api.example.test/documents",
                payload={
                    "results": [
                        {
                            "docID": "S100TEST1",
                            "edinetCode": "E1301",
                            "secCode": "13010",
                            "filerName": "Target",
                            "formCode": "030000",
                            "docTypeCode": "120",
                            "periodStart": "2025-04-01",
                            "periodEnd": "2026-03-31",
                            "submitDateTime": "2026-03-30 15:00",
                            "docDescription": "有価証券報告書",
                            "csvFlag": "1",
                            "pdfFlag": "1",
                            "xbrlFlag": "1",
                            "legalStatus": "1",
                        }
                    ]
                },
            )

    monkeypatch.setattr("app.backend.edinetdb.official_api.OfficialEdinetApiClient", FakeClient)
    summary = sync_recent_official_documents(repo=repo, cfg=cfg, job_name="daily_watch", days=1)
    assert summary["documents"] == 1
    assert summary["mapped_rows"] == 1
    assert repo.lookup_edinet_codes(["1301"]) == {"1301": "E1301"}
    docs = repo.list_official_documents(sec_code="1301")
    assert len(docs) == 1
    assert docs[0]["doc_description"] == "有価証券報告書"


def test_run_backfill_700_populates_public_map_even_without_paid_api_keys(monkeypatch, tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.delenv("EDINETDB_API_KEY", raising=False)
    monkeypatch.delenv("EDINETDB_API_KEYS", raising=False)
    monkeypatch.delenv("EDINET_OFFICIAL_API_KEY", raising=False)

    monkeypatch.setattr(
        "app.backend.edinetdb.jobs.download_public_company_map",
        lambda **kwargs: type(
            "PublicMapResult",
            (),
            {
                "source_url": "https://example.test/Edinetcode.zip",
                "downloaded_label": "2026年03月30日現在",
                "row_count_label": "1件",
                "total_rows": 1,
                "mapped_rows": 1,
                "rows": [
                    {
                        "sec_code": "1301",
                        "edinet_code": "E1301",
                        "name": "Target",
                        "industry": "Food",
                    }
                ],
            },
        )(),
    )

    summary = run_backfill_700()
    assert summary["skipped"] is True
    assert summary["public_company_map"]["mapped_rows"] == 1
    repo = EdinetdbRepository(db_path)
    assert repo.lookup_edinet_codes(["1301"]) == {"1301": "E1301"}


def test_sync_official_documents_for_codes_filters_and_updates_last_checked(monkeypatch, tmp_path):
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)
    repo.ensure_schema()
    repo.save_company_map(
        [
            {
                "sec_code": "1301",
                "edinet_code": "E1301",
                "name": "Target",
                "industry": "Food",
            }
        ]
    )
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("EDINET_OFFICIAL_API_KEY", "official-key")
    cfg = load_config(datetime(2026, 3, 30, 9, 0, 0, tzinfo=JST))

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_json(self, path, params=None):
            return OfficialApiResponse(
                status=200,
                url="https://api.example.test/documents",
                payload={
                    "results": [
                        {
                            "docID": "S100TARGET1",
                            "edinetCode": "E1301",
                            "secCode": "13010",
                            "filerName": "Target",
                            "formCode": "030000",
                            "docTypeCode": "120",
                            "periodStart": "2025-04-01",
                            "periodEnd": "2026-03-31",
                            "submitDateTime": "2026-03-30 15:00",
                            "docDescription": "Target filing",
                            "csvFlag": "1",
                            "pdfFlag": "1",
                            "xbrlFlag": "1",
                            "legalStatus": "1",
                        },
                        {
                            "docID": "S100OTHER1",
                            "edinetCode": "E9999",
                            "secCode": "99990",
                            "filerName": "Other",
                            "formCode": "030000",
                            "docTypeCode": "120",
                            "periodStart": "2025-04-01",
                            "periodEnd": "2026-03-31",
                            "submitDateTime": "2026-03-30 15:10",
                            "docDescription": "Other filing",
                            "csvFlag": "1",
                            "pdfFlag": "1",
                            "xbrlFlag": "1",
                            "legalStatus": "1",
                        },
                    ]
                },
            )

    monkeypatch.setattr("app.backend.edinetdb.official_api.OfficialEdinetApiClient", FakeClient)
    summary = sync_official_documents_for_codes(
        repo=repo,
        cfg=cfg,
        job_name="official_backfill:1301",
        sec_codes=["1301"],
        days=1,
    )

    assert summary["documents"] == 1
    assert summary["sec_codes"] == ["1301"]
    assert summary["matched_dates"] == ["2026-03-30"]
    docs = repo.list_official_documents(sec_code="1301")
    assert len(docs) == 1
    assert docs[0]["doc_id"] == "S100TARGET1"
    latest = repo.get_company_latest("E1301")
    assert latest is not None
    assert latest["last_checked_at"] is not None
