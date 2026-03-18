from __future__ import annotations

import csv
import io
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.config import config
from app.db.schema import ensure_schema
from app.db.session import get_conn, get_conn_for_path

TAISYAKU_MASTER_URL = "https://www.taisyaku.jp/data/meigara.csv"
TAISYAKU_BALANCE_URL = "https://www.taisyaku.jp/data/zandaka.csv"
TAISYAKU_FEE_URL = "https://www.taisyaku.jp/data/shina.csv"
TAISYAKU_RESTRICTION_URL = "https://www.taisyaku.jp/data/seigenichiran.csv"


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date_key(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 8:
        return None
    try:
        return int(digits)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text == "*****":
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text == "*****":
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _download_csv_rows(url: str) -> list[list[str]]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "MeeMeeScreener/1.0 (+https://www.taisyaku.jp/download/)",
            "Accept": "text/csv,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read()
    text = raw.decode("cp932", errors="ignore")
    return [list(row) for row in csv.reader(io.StringIO(text))]


def _normalize_code(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _compute_loan_ratio(finance_balance: int | None, stock_balance: int | None) -> float | None:
    if finance_balance is None or stock_balance is None or stock_balance == 0:
        return None
    try:
        return float(finance_balance) / float(stock_balance)
    except Exception:
        return None


def parse_issue_master_rows(rows: list[list[str]]) -> list[list[Any]]:
    parsed: list[list[Any]] = []
    for row in rows[2:]:
        if len(row) < 11:
            continue
        application_date = _parse_date_key(row[0])
        code = _normalize_code(row[1])
        if application_date is None or not code:
            continue
        parsed.append(
            [
                application_date,
                code,
                _text(row[2]),
                _parse_int(row[3]),
                _parse_int(row[5]),
                _parse_int(row[6]),
                _parse_int(row[7]),
                _parse_int(row[8]),
                _parse_int(row[9]),
                _parse_int(row[10]),
            ]
        )
    return parsed


def parse_balance_rows(rows: list[list[str]]) -> list[list[Any]]:
    parsed: list[list[Any]] = []
    for row in rows[1:]:
        if len(row) < 14:
            continue
        application_date = _parse_date_key(row[0])
        settlement_date = _parse_date_key(row[1])
        code = _normalize_code(row[2])
        if application_date is None or not code:
            continue
        finance_balance = _parse_int(row[9])
        stock_balance = _parse_int(row[12])
        parsed.append(
            [
                application_date,
                settlement_date,
                code,
                _text(row[3]),
                _text(row[4]),
                _text(row[6]),
                _parse_int(row[7]),
                _parse_int(row[8]),
                finance_balance,
                _parse_int(row[10]),
                _parse_int(row[11]),
                stock_balance,
                _parse_int(row[13]),
                _compute_loan_ratio(finance_balance, stock_balance),
            ]
        )
    return parsed


def parse_fee_rows(rows: list[list[str]]) -> list[list[Any]]:
    parsed: list[list[Any]] = []
    for row in rows[4:]:
        if len(row) < 12:
            continue
        application_date = _parse_date_key(row[0])
        settlement_date = _parse_date_key(row[1])
        code = _normalize_code(row[2])
        if application_date is None or not code:
            continue
        parsed.append(
            [
                application_date,
                settlement_date,
                code,
                _text(row[3]),
                _text(row[4]),
                _text(row[5]),
                _text(row[6]),
                _parse_float(row[7]),
                _parse_int(row[8]),
                _parse_float(row[9]),
                _parse_float(row[10]),
                _parse_int(row[11]),
                _parse_float(row[12] if len(row) > 12 else None),
            ]
        )
    return parsed


def parse_restriction_rows(rows: list[list[str]]) -> list[list[Any]]:
    parsed: list[list[Any]] = []
    for row in rows[5:]:
        if len(row) < 6:
            continue
        code = _normalize_code(row[1] if len(row) > 1 else None)
        notice_date = _parse_date_key(row[5] if len(row) > 5 else None)
        if not code or notice_date is None:
            continue
        parsed.append(
            [
                code,
                _text(row[2]),
                _text(row[0]),
                _text(row[3]) or "",
                _text(row[4]) or "",
                notice_date,
                _text(row[6] if len(row) > 6 else None),
            ]
        )
    return parsed


def _write_import_rows(
    *,
    db_path: str | Path | None,
    master_rows: list[list[Any]],
    balance_rows: list[list[Any]],
    fee_rows: list[list[Any]],
    restriction_rows: list[list[Any]],
) -> None:
    if db_path is None:
        context = get_conn()
    else:
        context = get_conn_for_path(str(Path(db_path).expanduser().resolve()), timeout_sec=2.5, read_only=False)
    fetched_at = datetime.now(UTC).replace(tzinfo=None)
    with context as conn:
        ensure_schema(conn)
        if master_rows:
            conn.executemany(
                """
                INSERT INTO taisyaku_issue_master (
                    application_date, code, issue_name,
                    tse_flag, jnx_flag, odx_flag, jax_flag, nse_flag, fse_flag, sse_flag, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, application_date) DO UPDATE SET
                    issue_name = excluded.issue_name,
                    tse_flag = excluded.tse_flag,
                    jnx_flag = excluded.jnx_flag,
                    odx_flag = excluded.odx_flag,
                    jax_flag = excluded.jax_flag,
                    nse_flag = excluded.nse_flag,
                    fse_flag = excluded.fse_flag,
                    sse_flag = excluded.sse_flag,
                    fetched_at = excluded.fetched_at
                """,
                [row + [fetched_at] for row in master_rows],
            )
        if balance_rows:
            conn.executemany(
                """
                INSERT INTO taisyaku_balance_daily (
                    application_date, settlement_date, code, issue_name, market_name, report_type,
                    finance_new_shares, finance_repay_shares, finance_balance_shares,
                    stock_new_shares, stock_repay_shares, stock_balance_shares,
                    net_balance_shares, loan_ratio, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, application_date, market_name, report_type) DO UPDATE SET
                    settlement_date = excluded.settlement_date,
                    issue_name = excluded.issue_name,
                    finance_new_shares = excluded.finance_new_shares,
                    finance_repay_shares = excluded.finance_repay_shares,
                    finance_balance_shares = excluded.finance_balance_shares,
                    stock_new_shares = excluded.stock_new_shares,
                    stock_repay_shares = excluded.stock_repay_shares,
                    stock_balance_shares = excluded.stock_balance_shares,
                    net_balance_shares = excluded.net_balance_shares,
                    loan_ratio = excluded.loan_ratio,
                    fetched_at = excluded.fetched_at
                """,
                [row + [fetched_at] for row in balance_rows],
            )
        if fee_rows:
            conn.executemany(
                """
                INSERT INTO taisyaku_fee_daily (
                    application_date, settlement_date, code, issue_name, market_name,
                    reason_type, reason_value, price_yen, stock_excess_shares,
                    max_fee_yen, current_fee_yen, fee_days, prior_fee_yen, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, application_date, market_name) DO UPDATE SET
                    settlement_date = excluded.settlement_date,
                    issue_name = excluded.issue_name,
                    reason_type = excluded.reason_type,
                    reason_value = excluded.reason_value,
                    price_yen = excluded.price_yen,
                    stock_excess_shares = excluded.stock_excess_shares,
                    max_fee_yen = excluded.max_fee_yen,
                    current_fee_yen = excluded.current_fee_yen,
                    fee_days = excluded.fee_days,
                    prior_fee_yen = excluded.prior_fee_yen,
                    fetched_at = excluded.fetched_at
                """,
                [row + [fetched_at] for row in fee_rows],
            )
        if restriction_rows:
            conn.executemany(
                """
                INSERT INTO taisyaku_restriction_notices (
                    code, issue_name, announcement_kind, measure_type, measure_detail,
                    notice_date, afternoon_stop, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, measure_type, measure_detail, notice_date) DO UPDATE SET
                    issue_name = excluded.issue_name,
                    announcement_kind = excluded.announcement_kind,
                    afternoon_stop = excluded.afternoon_stop,
                    fetched_at = excluded.fetched_at
                """,
                [row + [fetched_at] for row in restriction_rows],
            )


def import_taisyaku_csvs(*, db_path: str | Path | None = None) -> dict[str, Any]:
    master_rows = parse_issue_master_rows(_download_csv_rows(TAISYAKU_MASTER_URL))
    balance_rows = parse_balance_rows(_download_csv_rows(TAISYAKU_BALANCE_URL))
    fee_rows = parse_fee_rows(_download_csv_rows(TAISYAKU_FEE_URL))
    restriction_rows = parse_restriction_rows(_download_csv_rows(TAISYAKU_RESTRICTION_URL))
    _write_import_rows(
        db_path=db_path,
        master_rows=master_rows,
        balance_rows=balance_rows,
        fee_rows=fee_rows,
        restriction_rows=restriction_rows,
    )
    latest_balance_date = max((row[0] for row in balance_rows), default=None)
    latest_fee_date = max((row[0] for row in fee_rows), default=None)
    latest_restriction_date = max((row[5] for row in restriction_rows), default=None)
    return {
        "masterSaved": len(master_rows),
        "balanceSaved": len(balance_rows),
        "feeSaved": len(fee_rows),
        "restrictionSaved": len(restriction_rows),
        "latestBalanceDate": latest_balance_date,
        "latestFeeDate": latest_fee_date,
        "latestRestrictionDate": latest_restriction_date,
    }


def load_taisyaku_snapshot(
    code: str,
    *,
    db_path: str | Path | None = None,
    history_limit: int = 10,
) -> dict[str, Any] | None:
    normalized_code = _normalize_code(code)
    if not normalized_code:
        return None
    if db_path is None:
        context = get_conn()
    else:
        context = get_conn_for_path(str(Path(db_path).expanduser().resolve()), timeout_sec=2.5, read_only=True)
    with context as conn:
        balance_rows = conn.execute(
            """
            SELECT application_date, settlement_date, issue_name, market_name, report_type,
                   finance_balance_shares, stock_balance_shares, net_balance_shares, loan_ratio, fetched_at
            FROM taisyaku_balance_daily
            WHERE code = ?
            ORDER BY application_date DESC, fetched_at DESC
            LIMIT ?
            """,
            [normalized_code, max(1, int(history_limit))],
        ).fetchall()
        fee_row = conn.execute(
            """
            SELECT application_date, settlement_date, issue_name, market_name, reason_type, reason_value,
                   price_yen, stock_excess_shares, max_fee_yen, current_fee_yen, fee_days, prior_fee_yen, fetched_at
            FROM taisyaku_fee_daily
            WHERE code = ?
            ORDER BY application_date DESC, fetched_at DESC
            LIMIT 1
            """,
            [normalized_code],
        ).fetchone()
        restriction_rows = conn.execute(
            """
            SELECT issue_name, announcement_kind, measure_type, measure_detail, notice_date, afternoon_stop, fetched_at
            FROM taisyaku_restriction_notices
            WHERE code = ?
            ORDER BY notice_date DESC, fetched_at DESC
            LIMIT 8
            """,
            [normalized_code],
        ).fetchall()
        issue_row = conn.execute(
            """
            SELECT application_date, issue_name, tse_flag, jnx_flag, odx_flag, jax_flag, nse_flag, fse_flag, sse_flag, fetched_at
            FROM taisyaku_issue_master
            WHERE code = ?
            ORDER BY application_date DESC, fetched_at DESC
            LIMIT 1
            """,
            [normalized_code],
        ).fetchone()
    if not balance_rows and fee_row is None and not restriction_rows and issue_row is None:
        return None

    history_items = [
        {
            "applicationDate": row[0],
            "settlementDate": row[1],
            "issueName": row[2],
            "marketName": row[3],
            "reportType": row[4],
            "financeBalanceShares": row[5],
            "stockBalanceShares": row[6],
            "netBalanceShares": row[7],
            "loanRatio": float(row[8]) if row[8] is not None else None,
            "fetchedAt": row[9].isoformat() if row[9] is not None else None,
        }
        for row in balance_rows
    ]
    latest_balance = history_items[0] if history_items else None
    latest_fee = (
        {
            "applicationDate": fee_row[0],
            "settlementDate": fee_row[1],
            "issueName": fee_row[2],
            "marketName": fee_row[3],
            "reasonType": fee_row[4],
            "reasonValue": fee_row[5],
            "priceYen": float(fee_row[6]) if fee_row[6] is not None else None,
            "stockExcessShares": fee_row[7],
            "maxFeeYen": float(fee_row[8]) if fee_row[8] is not None else None,
            "currentFeeYen": float(fee_row[9]) if fee_row[9] is not None else None,
            "feeDays": fee_row[10],
            "priorFeeYen": float(fee_row[11]) if fee_row[11] is not None else None,
            "fetchedAt": fee_row[12].isoformat() if fee_row[12] is not None else None,
        }
        if fee_row is not None
        else None
    )
    restrictions = [
        {
            "issueName": row[0],
            "announcementKind": row[1],
            "measureType": row[2],
            "measureDetail": row[3],
            "noticeDate": row[4],
            "afternoonStop": row[5],
            "fetchedAt": row[6].isoformat() if row[6] is not None else None,
        }
        for row in restriction_rows
    ]
    issue = (
        {
            "applicationDate": issue_row[0],
            "issueName": issue_row[1],
            "tseFlag": issue_row[2],
            "jnxFlag": issue_row[3],
            "odxFlag": issue_row[4],
            "jaxFlag": issue_row[5],
            "nseFlag": issue_row[6],
            "fseFlag": issue_row[7],
            "sseFlag": issue_row[8],
            "fetchedAt": issue_row[9].isoformat() if issue_row[9] is not None else None,
        }
        if issue_row is not None
        else None
    )
    fetched_values = [
        item.get("fetchedAt")
        for item in ([latest_balance] if latest_balance else [])
        + ([latest_fee] if latest_fee else [])
        + restrictions
        + ([issue] if issue else [])
    ]
    fetched_latest = max((value for value in fetched_values if value), default=None)
    return {
        "code": normalized_code,
        "issue": issue,
        "latestBalance": latest_balance,
        "balanceHistory": history_items,
        "latestFee": latest_fee,
        "restrictions": restrictions,
        "fetchedAt": fetched_latest,
    }
