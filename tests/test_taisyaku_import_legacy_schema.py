from __future__ import annotations

from pathlib import Path

import duckdb

from app.backend.services.data import taisyaku_import


def test_import_taisyaku_csvs_is_compatible_with_legacy_tables_without_primary_keys(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "taisyaku-legacy.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE taisyaku_issue_master AS SELECT 20260312 application_date, '1306' code, 'old' issue_name, 0 tse_flag, 0 jnx_flag, 0 odx_flag, 0 jax_flag, 0 nse_flag, 0 fse_flag, 0 sse_flag, CURRENT_TIMESTAMP fetched_at")
        conn.execute("CREATE TABLE taisyaku_balance_daily AS SELECT 20260311 application_date, 20260313 settlement_date, '1306' code, 'old' issue_name, 'market' market_name, 'report' report_type, 0::BIGINT finance_new_shares, 0::BIGINT finance_repay_shares, 0::BIGINT finance_balance_shares, 0::BIGINT stock_new_shares, 0::BIGINT stock_repay_shares, 0::BIGINT stock_balance_shares, 0::BIGINT net_balance_shares, NULL::DOUBLE loan_ratio, CURRENT_TIMESTAMP fetched_at WHERE FALSE")
        conn.execute("CREATE TABLE taisyaku_fee_daily AS SELECT 20260311 application_date, 20260313 settlement_date, '1306' code, 'old' issue_name, 'market' market_name, 'reason' reason_type, 'value' reason_value, 0::DOUBLE price_yen, 0::BIGINT stock_excess_shares, 0::DOUBLE max_fee_yen, 0::DOUBLE current_fee_yen, 0 fee_days, 0::DOUBLE prior_fee_yen, CURRENT_TIMESTAMP fetched_at WHERE FALSE")
        conn.execute("CREATE TABLE taisyaku_restriction_notices AS SELECT '1306' code, 'old' issue_name, 'kind' announcement_kind, 'type' measure_type, 'detail' measure_detail, 20260312 notice_date, '' afternoon_stop, CURRENT_TIMESTAMP fetched_at WHERE FALSE")

    monkeypatch.setattr(taisyaku_import, "_download_csv_rows", lambda url: {
        taisyaku_import.TAISYAKU_MASTER_URL: [["header"], ["header"], ["20260312", "1306", "NEXT TOPIX ETF", "1", "0", "1", "1", "1", "0", "0", "0"]],
        taisyaku_import.TAISYAKU_BALANCE_URL: [["header"] * 14],
        taisyaku_import.TAISYAKU_FEE_URL: [["header"]] * 4,
        taisyaku_import.TAISYAKU_RESTRICTION_URL: [["header"]] * 5,
    }[url])

    taisyaku_import.import_taisyaku_csvs(db_path=db_path)

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute("SELECT issue_name FROM taisyaku_issue_master WHERE code = '1306'").fetchall()
    assert rows == [("NEXT TOPIX ETF",)]
