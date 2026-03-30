from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.api.routers import ticker
from app.backend.edinetdb.repository import EdinetdbRepository


def _clear_ticker_edinet_caches() -> None:
    ticker._EDINET_FINANCIALS_CACHE.clear()
    ticker._EDINET_SUMMARY_CACHE.clear()


def _seed_reference_company(repo: EdinetdbRepository) -> None:
    repo.save_company_map(
        [
            {
                "sec_code": "9999",
                "edinet_code": "E9999",
                "name": "Reference",
                "industry": "Ref",
            }
        ]
    )
    repo.upsert_financials(
        "E9999",
        {"items": [{"fiscal_year": "2024", "accounting_standard": "JP GAAP", "revenue": 10}]},
    )
    repo.upsert_ratios(
        "E9999",
        {"items": [{"fiscal_year": "2024", "accounting_standard": "JP GAAP", "roe": 0.05}]},
    )


def _seed_target_company(repo: EdinetdbRepository) -> None:
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
    repo.upsert_financials(
        "E1301",
        {
            "items": [
                {
                    "fiscal_year": "2024",
                    "accounting_standard": "JP GAAP",
                    "revenue": 900,
                    "operatingIncome": 90,
                    "netIncome": 50,
                    "eps": 55,
                    "bps": 320,
                    "dividendPerShare": 18,
                },
                {
                    "fiscal_year": "2025",
                    "accounting_standard": "JP GAAP",
                    "revenue": 1000,
                    "grossProfit": 250,
                    "operatingIncome": 120,
                    "netIncome": 70,
                    "eps": 70,
                    "bps": 360,
                    "dividendPerShare": 20,
                    "netInterestBearingDebt": -50,
                },
            ]
        },
    )
    repo.upsert_ratios(
        "E1301",
        {
            "items": [
                {"fiscal_year": "2024", "accounting_standard": "JP GAAP", "roe": 0.11, "equityRatio": 0.42},
                {
                    "fiscal_year": "2025",
                    "accounting_standard": "JP GAAP",
                    "roe": 0.15,
                    "roa": 0.06,
                    "equityRatio": 0.48,
                    "grossMargin": 0.25,
                    "operatingMargin": 0.12,
                    "netMargin": 0.07,
                },
            ]
        },
    )
    repo.upsert_analysis(
        "E1301",
        {
            "asof_date": "2026-03-30",
            "summary": "国内需要が底堅く、利益率の改善が続いている。",
            "strengths": "主力商品の価格改定が浸透し、収益性が安定している。",
            "risks": "原材料価格の再上昇が利益率を圧迫する可能性がある。",
            "outlook": "来期は海外販路の拡大が寄与する見通し。",
            "valuation": "同業比でEV/EBITDAは依然割安圏にある。",
        },
    )
    repo.upsert_text_blocks(
        "E1301",
        {
            "items": [
                {
                    "fiscal_year": "2025",
                    "block_name": "business_overview",
                    "text": "加工食品事業を中心に国内外で販売網を展開している。",
                },
                {
                    "fiscal_year": "2025",
                    "block_name": "strategy",
                    "text": "高付加価値商品の比率を引き上げ、営業利益率の改善を進める。",
                },
                {
                    "fiscal_year": "2025",
                    "block_name": "risk_factors",
                    "text": "穀物価格や為替変動により原価率が変動するリスクがある。",
                },
            ]
        },
    )
    repo.save_company_latest(
        "E1301",
        latest_fiscal_year="2025",
        latest_hash="hash-1301",
        fetched_at=datetime(2026, 3, 30, 3, 0, 0),
        last_checked_at=datetime(2026, 3, 30, 4, 0, 0),
    )
    repo.upsert_official_documents(
        [
            {
                "doc_id": "S100TARGET1",
                "sec_code": "1301",
                "edinet_code": "E1301",
                "filer_name": "Target",
                "form_code": "030000",
                "doc_type_code": "120",
                "period_start": "2025-04-01",
                "period_end": "2026-03-31",
                "submit_datetime": "2026-03-28 15:00",
                "doc_description": "有価証券報告書",
                "csv_flag": 1,
                "pdf_flag": 1,
                "xbrl_flag": 1,
                "legal_status": "1",
                "payload": {"docID": "S100TARGET1"},
            }
        ]
    )


def test_edinet_financial_payload_distinguishes_statuses_and_enriches_content(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    repo = EdinetdbRepository(db_path)
    repo.ensure_schema()
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    _clear_ticker_edinet_caches()
    monkeypatch.setattr(ticker, "get_active_edinet_bootstrap_state", lambda: {"active": False, "mode": None, "jobId": None, "message": None})
    empty_payload = ticker._build_edinet_financials_payload("1301")
    assert empty_payload is not None
    assert empty_payload["status"] == "empty_tables"
    assert empty_payload["officialFilings"] == []

    _clear_ticker_edinet_caches()
    monkeypatch.setattr(
        ticker,
        "get_active_edinet_bootstrap_state",
        lambda: {"active": True, "mode": "backfill_700", "jobId": "job-1", "message": "bootstrapping"},
    )
    loading_payload = ticker._build_edinet_financials_payload("1301")
    assert loading_payload is not None
    assert loading_payload["status"] == "loading"
    assert loading_payload["bootstrapState"]["jobId"] == "job-1"

    _seed_reference_company(repo)
    _clear_ticker_edinet_caches()
    monkeypatch.setattr(ticker, "get_active_edinet_bootstrap_state", lambda: {"active": False, "mode": None, "jobId": None, "message": None})
    unmapped_payload = ticker._build_edinet_financials_payload("1301")
    assert unmapped_payload is not None
    assert unmapped_payload["status"] == "unmapped"
    assert unmapped_payload["mapped"] is False

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
    _clear_ticker_edinet_caches()
    no_payload = ticker._build_edinet_financials_payload("1301")
    assert no_payload is not None
    assert no_payload["status"] == "no_payload"
    assert no_payload["mapped"] is True

    _seed_target_company(repo)
    _clear_ticker_edinet_caches()
    ok_payload = ticker._build_edinet_financials_payload("1301")
    assert ok_payload is not None
    assert ok_payload["status"] == "ok"
    assert ok_payload["summary"]["latestFiscalYear"] == 2025
    assert ok_payload["lastCheckedAt"] == "2026-03-30T04:00:00"
    assert ok_payload["officialFilings"][0]["docId"] == "S100TARGET1"
    assert ok_payload["officialFilings"][0]["docDescription"] == "有価証券報告書"
    assert ok_payload["analysisSummary"]["items"][0]["label"] == "要約"
    assert ok_payload["analysisSummary"]["items"][1]["label"] == "強み"
    assert ok_payload["textHighlights"][0]["blockName"] == "business_overview"
    assert ok_payload["textHighlights"][1]["blockName"] == "strategy"

    _clear_ticker_edinet_caches()
    monkeypatch.setattr(ticker, "_build_edinet_financials_base_payload", lambda _code: None)
    error_payload = ticker._build_edinet_financials_payload("1301")
    assert error_payload is not None
    assert error_payload["status"] == "error"
