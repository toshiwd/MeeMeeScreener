from __future__ import annotations

from app.backend.tdnetdb.repository import TdnetdbRepository
from tools.setup.fetch_tdnet_yanoshin import normalize_tdnet_item


def _yanoshin_sample() -> dict:
    return {
        "Tdnet": {
            "id": "1244728",
            "pubdate": "2026-05-08 13:55:00",
            "company_code": "72030",
            "company_name": "Toyota",
            "title": "FY2026 financial results",
            "document_url": "https://example.test/report.pdf",
            "url_report_type_summary": "https://example.test/summary.pdf",
            "url_report_type_earnings_forecast": "https://example.test/forecast.pdf",
            "url_xbrl": "https://example.test/report.zip",
            "markets_string": "TSE/NSE",
        }
    }


def test_normalize_tdnet_item_maps_yanoshin_shape() -> None:
    item = normalize_tdnet_item(_yanoshin_sample(), fallback_code=None)

    assert item == {
        "disclosure_id": "yanoshin:1244728",
        "sec_code": "7203",
        "company_name": "Toyota",
        "title": "FY2026 financial results",
        "category": "TSE/NSE",
        "published_at": "2026-05-08T13:55:00",
        "tdnet_url": "https://example.test/report.pdf",
        "pdf_url": "https://example.test/report.pdf",
        "xbrl_url": "https://example.test/report.zip",
        "summary_text": None,
        "provider": "yanoshin",
        "markets": "TSE/NSE",
        "report_links": [
            {"label": "本文PDF", "url": "https://example.test/report.pdf"},
            {"label": "短信サマリー", "url": "https://example.test/summary.pdf"},
            {"label": "業績予想", "url": "https://example.test/forecast.pdf"},
            {"label": "XBRL", "url": "https://example.test/report.zip"},
        ],
        "raw": _yanoshin_sample()["Tdnet"],
    }


def test_tdnet_repository_exposes_free_provider_report_links(tmp_path) -> None:
    repo = TdnetdbRepository(tmp_path / "stocks.duckdb")
    item = normalize_tdnet_item(_yanoshin_sample(), fallback_code=None)
    assert item is not None

    assert repo.upsert_disclosures([item]) == 1

    [row] = repo.list_disclosures_by_code("7203", limit=5)
    assert row["sourceProvider"] == "yanoshin"
    assert row["markets"] == "TSE/NSE"
    assert {"label": "短信サマリー", "url": "https://example.test/summary.pdf"} in row["reportLinks"]
    assert {"label": "業績予想", "url": "https://example.test/forecast.pdf"} in row["reportLinks"]
    assert {"label": "XBRL", "url": "https://example.test/report.zip"} in row["reportLinks"]


def test_tdnet_repository_is_compatible_with_legacy_tables_without_primary_keys(tmp_path) -> None:
    import duckdb

    db_path = tmp_path / "tdnet-legacy.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE tdnet_disclosures AS SELECT 'yanoshin:1244728' disclosure_id, '7203' sec_code, 'old' company_name, 'old' title, NULL::VARCHAR category, NULL::TIMESTAMP published_at, NULL::VARCHAR tdnet_url, NULL::VARCHAR pdf_url, NULL::VARCHAR xbrl_url, NULL::VARCHAR summary_text, '{}' raw_json, CURRENT_TIMESTAMP fetched_at")
        conn.execute("CREATE TABLE tdnet_disclosure_features AS SELECT 'yanoshin:1244728' disclosure_id, '7203' sec_code, NULL::TIMESTAMP published_at, 'other' event_type, 'neutral' sentiment, 0.2::DOUBLE importance_score, FALSE forecast_revision, FALSE dividend_revision, FALSE share_buyback, FALSE share_split, FALSE earnings, FALSE governance, FALSE distress, '' title_normalized, '[]' tags_json, '' raw_text, CURRENT_TIMESTAMP fetched_at")
    repo = TdnetdbRepository(db_path)
    item = normalize_tdnet_item(_yanoshin_sample(), fallback_code=None)
    assert item is not None

    assert repo.upsert_disclosures([item]) == 1

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute("SELECT company_name FROM tdnet_disclosures WHERE disclosure_id = 'yanoshin:1244728'").fetchall()
    assert rows == [("Toyota",)]
