from __future__ import annotations

import csv
import os
import re

from app.core.config import resolve_trade_csv_paths
from app.utils.text_utils import _normalize_code

_trade_cache = {"key": None, "rows": [], "warnings": []}

def _detect_trade_broker(raw_data: bytes, filename: str | None = None) -> tuple[str, str]:
    """
    Returns (broker, reason). broker is "rakuten" or "sbi".
    """
    name = (filename or "").lower()
    if "sbi" in name:
        return "sbi", "filename"
    if "rakuten" in name or "楽天" in (filename or ""):
        return "rakuten", "filename"

    # Header heuristics (both cp932 and utf-8 just in case)
    for enc in ("cp932", "utf-8"):
        try:
            head = raw_data[:8192].decode(enc, errors="ignore")
        except Exception:
            continue
        if "受渡金額/決済損益" in head or "信用新規買" in head:
            return "sbi", f"header:{enc}"
        if "口座" in head and "手数料" in head:
            return "rakuten", f"header:{enc}"

    return "rakuten", "default"

def _parse_trade_csv() -> dict:
    warnings: list[dict] = []
    paths = resolve_trade_csv_paths()
    existing_paths = [path for path in paths if os.path.isfile(path)]
    if not existing_paths:
        missing = ", ".join(paths)
        warnings.append({"type": "trade_csv_missing", "message": f"trade_csv_missing:{missing}"})
        return {"rows": [], "warnings": warnings}

    key = tuple((path, os.path.getmtime(path)) for path in existing_paths)
    if _trade_cache["key"] == key:
        return {"rows": _trade_cache["rows"], "warnings": _trade_cache["warnings"]}

    rows: list[dict] = []

    def normalize_text(value: str | None) -> str:
        if value is None:
            return ""
        text = str(value).replace("\ufeff", "")
        if text.strip().lower() in ("nan", "none", "--"):
            return ""
        text = text.replace("\u3000", " ")
        return text.strip()

    def normalize_label(value: str | None) -> str:
        text = normalize_text(value)
        if not text:
            return ""
        return re.sub(r"\s+", "", text)

    def read_csv_rows(path: str, encoding: str) -> list[list[str]]:
        with open(path, "r", encoding=encoding, newline="") as handle:
            reader = csv.reader(handle)
            return list(reader)

    def make_dedup_key(
        code: str,
        date_value: str | None,
        trade_label: str,
        qty_raw: str,
        price_raw: str,
        amount_raw: str,
        fee_raw: str = "",
        tax_raw: str = "",
        account: str = ""
    ) -> str:
        parts = [
            normalize_text(code),
            normalize_text(date_value or ""),
            normalize_label(trade_label),
            normalize_text(qty_raw),
            normalize_text(price_raw),
            normalize_text(amount_raw),
            normalize_text(fee_raw),
            normalize_text(tax_raw),
            normalize_text(account)
        ]
        return "|".join(parts)

    def log_dedup_summary(duplicate_counts: dict[str, int]) -> None:
        if not duplicate_counts:
            return
        print(
            "trade_dedup_key=code|date|trade|qty|price|amount|fee|tax|account "
            f"duplicates={duplicate_counts}"
        )

    def to_float(value: str) -> float:
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return 0.0

    def to_optional_float(value: str) -> float | None:
        text = normalize_text(value)
        if not text:
            return None
        try:
            return float(text.replace(",", ""))
        except ValueError:
            return None

    def parse_date(value: str) -> str | None:
        raw = normalize_text(value)
        if not raw:
            return None
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def find_sbi_header_index(rows_all: list[list[str]]) -> int | None:
        start = min(6, max(0, len(rows_all)))
        for idx in range(start, min(len(rows_all), start + 6)):
            row = rows_all[idx]
            if not row or not any(cell.strip() for cell in row):
                continue
            if "約定日" in row or "銘柄コード" in row:
                return idx
        return None

    def looks_like_sbi(rows_all: list[list[str]]) -> bool:
        for row in rows_all[:10]:
            if any("CSV作成日" in cell for cell in row):
                return True
        header_index = find_sbi_header_index(rows_all)
        if header_index is None:
            return False
        header_row = [cell.strip() for cell in rows_all[header_index]]
        if any(
            name in header_row
            for name in ("受渡金額/決済損益", "決済損益", "受渡金額", "手数料/諸経費等")
        ):
            return True
        if "取引" in header_row:
            trade_idx = header_row.index("取引")
            for row in rows_all[header_index + 1 : header_index + 50]:
                if trade_idx < len(row) and any(
                    key in row[trade_idx] for key in ("信用新規買", "信用返済売", "信用新規売", "信用返済買")
                ):
                    return True
        return False

    def parse_sbi_rows(rows_all: list[list[str]], encoding_used: str) -> dict:
        header_index = find_sbi_header_index(rows_all)
        if header_index is None:
            warnings.append({"type": "sbi_header_missing", "message": "sbi_header_missing"})
            return {"rows": [], "warnings": warnings}

        header = [cell.strip() for cell in rows_all[header_index]]

        def find_col(*names: str) -> int | None:
            for name in names:
                if name in header:
                    return header.index(name)
            return None

        col_trade_date = find_col("約定日")
        col_settle_date = find_col("受渡日")
        col_code = find_col("銘柄コード")
        col_name = find_col("銘柄")
        col_market = find_col("市場")
        col_trade = find_col("取引")
        col_account = find_col("預り")
        col_qty = find_col("約定数量", "数量")
        col_price = find_col("約定単価", "単価")
        col_fee = find_col("手数料/諸経費等", "手数料等")
        col_tax = find_col("税額")
        col_amount = find_col("受渡金額/決済損益", "決済損益", "受渡金額")

        dedup_keys: set[str] = set()
        duplicate_counts: dict[str, int] = {}
        for row_index, line in enumerate(rows_all[header_index + 1 :], start=1):
            if not line or col_trade_date is None or col_code is None:
                continue
            if not any(cell.strip() for cell in line):
                continue
            date_value = parse_date(line[col_trade_date]) if col_trade_date < len(line) else None
            code_raw = normalize_text(line[col_code]) if col_code < len(line) else ""
            if not date_value or not code_raw:
                continue
            code = _normalize_code(code_raw)
            if not code:
                continue

            name = normalize_text(line[col_name]) if col_name is not None and col_name < len(line) else ""
            market = normalize_text(line[col_market]) if col_market is not None and col_market < len(line) else ""
            account = normalize_text(line[col_account]) if col_account is not None and col_account < len(line) else ""
            trade_raw = normalize_text(line[col_trade]) if col_trade is not None and col_trade < len(line) else ""
            qty_raw = normalize_text(line[col_qty]) if col_qty is not None and col_qty < len(line) else ""
            price_raw = normalize_text(line[col_price]) if col_price is not None and col_price < len(line) else ""
            fee_raw = normalize_text(line[col_fee]) if col_fee is not None and col_fee < len(line) else ""
            tax_raw = normalize_text(line[col_tax]) if col_tax is not None and col_tax < len(line) else ""
            amount_raw = normalize_text(line[col_amount]) if col_amount is not None and col_amount < len(line) else ""
            settle_date = (
                parse_date(line[col_settle_date]) if col_settle_date is not None and col_settle_date < len(line) else None
            )

            qty_shares = to_float(qty_raw)
            if qty_shares <= 0:
                continue
            if qty_shares % 100 != 0:
                warnings.append(
                    {
                        "type": "non_100_shares",
                        "message": f"non_100_shares:{code}:{date_value}:{qty_shares}",
                        "code": code
                    }
                )
            price = to_optional_float(price_raw)
            fee = to_optional_float(fee_raw)
            tax = to_optional_float(tax_raw)
            amount = to_optional_float(amount_raw)
            realized_net = None
            if amount is not None:
                realized_net = amount
                if fee is not None:
                    realized_net -= fee
                if tax is not None:
                    realized_net -= tax

            trade_label = normalize_label(trade_raw)
            txn_type = ""
            event_kind = None
            if "信用新規買" in trade_label:
                txn_type = "OPEN_LONG"
                event_kind = "BUY_OPEN"
            elif "信用返済売" in trade_label:
                txn_type = "CLOSE_LONG"
                event_kind = "SELL_CLOSE"
            elif "信用新規売" in trade_label:
                txn_type = "OPEN_SHORT"
                event_kind = "SELL_OPEN"
            elif "信用返済買" in trade_label:
                txn_type = "CLOSE_SHORT"
                event_kind = "BUY_CLOSE"
            elif "現物買" in trade_label or "買付" in trade_label:
                txn_type = "OPEN_LONG"
                event_kind = "BUY_OPEN"
            elif "現物売" in trade_label or "売付" in trade_label:
                txn_type = "CLOSE_LONG"
                event_kind = "SELL_CLOSE"
            elif "入庫" in trade_label:
                txn_type = "CORPORATE_ACTION"
                event_kind = "INBOUND"
            elif "出庫" in trade_label:
                txn_type = "CORPORATE_ACTION"
                event_kind = "OUTBOUND"

            if event_kind is None:
                sample = f"取引={trade_raw or '(blank)'}"
                unknown_labels_by_code.setdefault(code, set()).add(sample)
                continue

            dedup_key = make_dedup_key(
                code,
                date_value,
                trade_label,
                qty_raw,
                price_raw,
                amount_raw,
                fee_raw,
                tax_raw,
                account
            )
            if dedup_key in dedup_keys:
                duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                continue
            dedup_keys.add(dedup_key)

            if event_kind == "BUY_OPEN":
                side = "buy"
                action = "open"
            elif event_kind == "BUY_CLOSE":
                side = "buy"
                action = "close"
            elif event_kind == "SELL_OPEN":
                side = "sell"
                action = "open"
            elif event_kind == "SELL_CLOSE":
                side = "sell"
                action = "close"
            else:
                side = "buy"
                action = "open"

            if event_kind in ("BUY_OPEN", "SELL_OPEN"):
                event_order = 0
            elif event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                event_order = 1
            else:
                event_order = 2

            rows.append(
                {
                    "broker": "SBI",
                    "tradeDate": date_value,
                    "trade_date": date_value,
                    "settleDate": settle_date,
                    "settle_date": settle_date,
                    "code": code,
                    "name": name,
                    "market": market,
                    "account": account,
                    "txnType": txn_type,
                    "txn_type": txn_type,
                    "qty": qty_shares,
                    "qtyShares": qty_shares,
                    "units": int(qty_shares // 100),
                    "price": price if price is not None and price > 0 else None,
                    "fee": fee,
                    "tax": tax,
                    "realizedPnlGross": amount,
                    "realizedPnlNet": realized_net,
                    "memo": trade_raw,
                    "date": date_value,
                    "side": side,
                    "action": action,
                    "kind": event_kind,
                    "_row_index": row_index,
                    "_event_order": event_order,
                    "raw": {
                        "date": line[col_trade_date] if col_trade_date is not None and col_trade_date < len(line) else "",
                        "code": code_raw,
                        "name": name,
                        "trade": trade_raw,
                        "market": market,
                        "account": account,
                        "qty": qty_raw,
                        "price": price_raw,
                        "fee": fee_raw,
                        "tax": tax_raw,
                        "amount": amount_raw,
                        "encoding": encoding_used
                    }
                }
            )

        for code, count in duplicate_counts.items():
            warnings.append(
                {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
            )

        log_dedup_summary(duplicate_counts)

        for code, samples_set in unknown_labels_by_code.items():
            samples = sorted(list(samples_set))[:5]
            warnings.append(
                {
                    "type": "unrecognized_labels",
                    "count": len(samples_set),
                    "samples": samples,
                    "code": code
                }
            )

        rows.sort(
            key=lambda item: (item.get("date", ""), item.get("_event_order", 2), item.get("_row_index", 0))
        )
        return {"rows": rows, "warnings": warnings}

    def parse_single(path: str) -> tuple[list[dict], list[dict]]:
        file_rows: list[dict] = []
        file_warnings: list[dict] = []
        unknown_labels_by_code: dict[str, set[str]] = {}

        try:
            rows_all = read_csv_rows(path, "cp932")
            encoding_used = "cp932"
        except UnicodeDecodeError:
            rows_all = read_csv_rows(path, "utf-8-sig")
            encoding_used = "utf-8-sig"

        if rows_all:
            header = [normalize_text(cell) for cell in rows_all[0]] if rows_all else []
            if not looks_like_sbi(rows_all) and ("約定日" not in header and "約定日付" not in header):
                try:
                    rows_all = read_csv_rows(path, "utf-8-sig")
                    encoding_used = "utf-8-sig"
                except UnicodeDecodeError:
                    pass

        if looks_like_sbi(rows_all):
            header_index = find_sbi_header_index(rows_all)
            if header_index is None:
                file_warnings.append(
                    {"type": "sbi_header_missing", "message": f"sbi_header_missing:{path}"}
                )
                return file_rows, file_warnings
            raw_header = [normalize_text(cell) for cell in rows_all[header_index]]
            data_rows = rows_all[header_index + 1 :]
            header = raw_header
            col_map = {name: index for index, name in enumerate(header) if name}
            get_cell = lambda row, key: normalize_text(row[col_map.get(key, -1)]) if key in col_map else ""

            dedup_keys: set[str] = set()
            duplicate_counts: dict[str, int] = {}

            for row_index, row in enumerate(data_rows, start=1):
                if not row or not any(cell.strip() for cell in row):
                    continue
                trade_date = parse_date(get_cell(row, "約定日"))
                if not trade_date:
                    continue
                code = _normalize_code(get_cell(row, "銘柄コード"))
                name = get_cell(row, "銘柄")
                market = get_cell(row, "市場")
                account = get_cell(row, "預り")
                trade_kind = get_cell(row, "取引") or get_cell(row, "取引区分")
                qty_raw = get_cell(row, "約定数量") or get_cell(row, "数量")
                qty_shares = to_float(qty_raw)
                price_raw = get_cell(row, "約定単価") or get_cell(row, "単価")
                price = to_optional_float(price_raw)
                fee_raw = get_cell(row, "手数料/諸経費等")
                tax_raw = get_cell(row, "税金") or get_cell(row, "税額")
                pnl_raw = get_cell(row, "受渡金額/決済損益") or get_cell(row, "決済損益")
                realized_pnl = to_optional_float(pnl_raw)
                if qty_shares <= 0:
                    continue

                event_kind = None
                if "信用新規買" in trade_kind:
                    event_kind = "BUY_OPEN"
                elif "信用返済売" in trade_kind:
                    event_kind = "SELL_CLOSE"
                elif "信用新規売" in trade_kind:
                    event_kind = "SELL_OPEN"
                elif "信用返済買" in trade_kind:
                    event_kind = "BUY_CLOSE"
                elif "現物買" in trade_kind or "買付" in trade_kind:
                    event_kind = "BUY_OPEN"
                elif "現物売" in trade_kind or "売付" in trade_kind:
                    event_kind = "SELL_CLOSE"

                if event_kind is None:
                    sample = f"取引区分={trade_kind or '(blank)'}, 売買区分=(blank)"
                    unknown_labels_by_code.setdefault(code, set()).add(sample)
                    continue

                dedup_key = make_dedup_key(
                    code,
                    trade_date,
                    trade_kind,
                    qty_raw,
                    price_raw,
                    pnl_raw,
                    fee_raw,
                    tax_raw,
                    account
                )
                if dedup_key in dedup_keys:
                    duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                    continue
                dedup_keys.add(dedup_key)

                if event_kind == "BUY_OPEN":
                    side = "buy"
                    action = "open"
                elif event_kind == "BUY_CLOSE":
                    side = "buy"
                    action = "close"
                elif event_kind == "SELL_OPEN":
                    side = "sell"
                    action = "open"
                else:
                    side = "sell"
                    action = "close"

                if event_kind in ("BUY_OPEN", "SELL_OPEN"):
                    event_order = 0
                elif event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                    event_order = 1
                else:
                    event_order = 2

                txn_type = "CORPORATE_ACTION"
                if event_kind == "BUY_OPEN":
                    txn_type = "OPEN_LONG"
                elif event_kind == "SELL_CLOSE":
                    txn_type = "CLOSE_LONG"
                elif event_kind == "SELL_OPEN":
                    txn_type = "OPEN_SHORT"
                elif event_kind == "BUY_CLOSE":
                    txn_type = "CLOSE_SHORT"

                file_rows.append(
                    {
                        "broker": "SBI",
                        "tradeDate": trade_date,
                        "trade_date": trade_date,
                        "settleDate": parse_date(get_cell(row, "受渡日")),
                        "settle_date": parse_date(get_cell(row, "受渡日")),
                        "date": trade_date,
                        "code": code,
                        "name": name,
                        "market": market,
                        "account": account,
                        "txnType": txn_type,
                        "txn_type": txn_type,
                        "qty": qty_shares,
                        "side": side,
                        "action": action,
                        "kind": event_kind,
                        "qtyShares": qty_shares,
                        "units": int(qty_shares // 100),
                        "price": price if price is not None and price > 0 else None,
                        "fee": to_optional_float(fee_raw),
                        "tax": to_optional_float(tax_raw),
                        "realizedPnlGross": realized_pnl,
                        "realizedPnlNet": realized_pnl,
                        "memo": trade_kind,
                        "_row_index": row_index,
                        "_event_order": event_order,
                        "raw": {
                            "date": trade_date,
                            "code": code,
                            "name": name,
                            "trade": trade_kind,
                            "qty": qty_raw,
                            "price": price_raw,
                            "amount": pnl_raw,
                            "encoding": encoding_used
                        }
                    }
                )

            for code, count in duplicate_counts.items():
                file_warnings.append(
                    {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
                )

            log_dedup_summary(duplicate_counts)

        else:
            rows_all = rows_all
            header = [normalize_text(cell) for cell in rows_all[0]] if rows_all else []
            data_rows = rows_all[1:] if rows_all else []
            col_map = {name: index for index, name in enumerate(header) if name}
            get_cell = lambda row, key: normalize_text(row[col_map.get(key, -1)]) if key in col_map else ""

            dedup_keys: set[str] = set()
            duplicate_counts: dict[str, int] = {}

            for row_index, row in enumerate(data_rows, start=1):
                if not row or not any(cell.strip() for cell in row):
                    continue
                date_raw = get_cell(row, "約定日") or get_cell(row, "日付")
                date_value = parse_date(date_raw)
                if not date_value:
                    continue
                settle_date = parse_date(get_cell(row, "受渡日"))
                code_raw = get_cell(row, "銘柄コード") or get_cell(row, "銘柄ｺｰﾄﾞ") or get_cell(row, "銘柄")
                code = _normalize_code(code_raw)
                name = get_cell(row, "銘柄名") or get_cell(row, "銘柄")
                market = get_cell(row, "市場")
                account = get_cell(row, "口座区分") or get_cell(row, "預り区分")
                type_raw = get_cell(row, "取引区分")
                kind_raw = get_cell(row, "売買区分")
                trade_type = normalize_label(type_raw)
                trade_kind = normalize_label(kind_raw)
                qty_raw = (
                    get_cell(row, "数量［株］")
                    or get_cell(row, "数量[株]")
                    or get_cell(row, "数量")
                    or get_cell(row, "数量(株)")
                )
                qty_shares = to_float(qty_raw)
                price_raw = (
                    get_cell(row, "単価［円］")
                    or get_cell(row, "単価[円]")
                    or get_cell(row, "単価")
                    or get_cell(row, "約定単価")
                )
                price = to_optional_float(price_raw)
                amount_raw = (
                    get_cell(row, "受渡金額［円］")
                    or get_cell(row, "受渡金額[円]")
                    or get_cell(row, "受渡金額")
                )
                fee_raw = (
                    get_cell(row, "手数料［円］")
                    or get_cell(row, "手数料[円]")
                    or get_cell(row, "手数料")
                )
                tax_raw = (
                    get_cell(row, "税金等［円］")
                    or get_cell(row, "税金等[円]")
                    or get_cell(row, "税金")
                )
                if qty_shares <= 0:
                    continue

                event_kind = None
                if trade_kind == "現渡" or trade_type == "現渡":
                    event_kind = "DELIVERY"
                elif trade_kind == "現引" or trade_type == "現引":
                    event_kind = "TAKE_DELIVERY"
                elif trade_type == "入庫" or trade_kind == "入庫":
                    event_kind = "INBOUND"
                elif trade_type == "出庫" or trade_kind == "出庫":
                    event_kind = "OUTBOUND"

                if event_kind is None:
                    if "買建" in trade_kind:
                        event_kind = "BUY_OPEN"
                    elif "売建" in trade_kind:
                        event_kind = "SELL_OPEN"
                    elif "買埋" in trade_kind:
                        event_kind = "BUY_CLOSE"
                    elif "売埋" in trade_kind:
                        event_kind = "SELL_CLOSE"
                    elif "現物買" in trade_kind or "買付" in trade_kind:
                        event_kind = "BUY_OPEN"
                    elif "現物売" in trade_kind or "売付" in trade_kind:
                        event_kind = "SELL_CLOSE"
                    elif trade_type == "入庫" or trade_kind == "入庫":
                        event_kind = "INBOUND"
                    elif trade_type == "出庫" or trade_kind == "出庫":
                        event_kind = "OUTBOUND"
                    elif trade_type == "現渡" or trade_kind == "現渡":
                        event_kind = "DELIVERY"
                    elif trade_type == "現引" or trade_kind == "現引":
                        event_kind = "TAKE_DELIVERY"

                if event_kind is None:
                    sample = f"取引区分={trade_type or '(blank)'}, 売買区分={trade_kind or '(blank)'}"
                    unknown_labels_by_code.setdefault(code, set()).add(sample)
                    continue

                dedup_key = make_dedup_key(
                    code,
                    date_value,
                    trade_type + "|" + trade_kind,
                    qty_raw,
                    price_raw,
                    amount_raw,
                    fee_raw,
                    tax_raw,
                    account
                )
                if dedup_key in dedup_keys:
                    duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                    continue
                dedup_keys.add(dedup_key)

                if event_kind == "BUY_OPEN":
                    side = "buy"
                    action = "open"
                elif event_kind == "BUY_CLOSE":
                    side = "buy"
                    action = "close"
                elif event_kind == "SELL_OPEN":
                    side = "sell"
                    action = "open"
                elif event_kind == "SELL_CLOSE":
                    side = "sell"
                    action = "close"
                else:
                    side = "buy"
                    action = "open"

                if event_kind in ("BUY_OPEN", "SELL_OPEN"):
                    event_order = 0
                elif event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                    event_order = 1
                else:
                    event_order = 2

                txn_type = "CORPORATE_ACTION"
                if event_kind == "BUY_OPEN":
                    txn_type = "OPEN_LONG"
                elif event_kind == "SELL_CLOSE":
                    txn_type = "CLOSE_LONG"
                elif event_kind == "SELL_OPEN":
                    txn_type = "OPEN_SHORT"
                elif event_kind == "BUY_CLOSE":
                    txn_type = "CLOSE_SHORT"

                file_rows.append(
                    {
                        "broker": "RAKUTEN",
                        "tradeDate": date_value,
                        "trade_date": date_value,
                        "settleDate": settle_date,
                        "settle_date": settle_date,
                        "date": date_value,
                        "code": code,
                        "name": name,
                        "market": market,
                        "account": account,
                        "txnType": txn_type,
                        "txn_type": txn_type,
                        "qty": qty_shares,
                        "side": side,
                        "action": action,
                        "kind": event_kind,
                        "qtyShares": qty_shares,
                        "units": int(qty_shares // 100),
                        "price": price if price is not None and price > 0 else None,
                        "fee": to_optional_float(fee_raw),
                        "tax": to_optional_float(tax_raw),
                        "realizedPnlGross": None,
                        "realizedPnlNet": None,
                        "memo": kind_raw or type_raw,
                        "_row_index": row_index,
                        "_event_order": event_order,
                        "raw": {
                            "date": date_raw,
                            "code": code_raw,
                            "name": name,
                            "trade": kind_raw,
                            "type": type_raw,
                            "qty": qty_raw,
                            "price": price_raw,
                            "amount": amount_raw,
                            "encoding": encoding_used
                        }
                    }
                )

            for code, count in duplicate_counts.items():
                file_warnings.append(
                    {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
                )

            log_dedup_summary(duplicate_counts)

        for code, samples_set in unknown_labels_by_code.items():
            samples = sorted(list(samples_set))[:5]
            file_warnings.append(
                {
                    "type": "unrecognized_labels",
                    "count": len(samples_set),
                    "samples": samples,
                    "code": code
                }
            )

        file_rows.sort(
            key=lambda item: (
                item.get("date", ""),
                item.get("_event_order", 2),
                item.get("_row_index", 0)
            )
        )
        return file_rows, file_warnings

    for path in existing_paths:
        file_rows, file_warnings = parse_single(path)
        if not file_rows and not file_warnings:
            continue
        rows.extend(file_rows)
        warnings.extend(file_warnings)

    global_dedup_keys: set[str] = set()
    global_duplicate_counts: dict[str, int] = {}
    deduped_rows: list[dict] = []
    for row in rows:
        raw = row.get("raw") or {}
        code = row.get("code") or ""
        date_value = row.get("date") or row.get("tradeDate") or ""
        trade_label = raw.get("trade") or raw.get("type") or row.get("memo") or row.get("kind") or ""
        qty_raw = raw.get("qty") or row.get("qtyShares") or ""
        price_raw = raw.get("price") or row.get("price") or ""
        amount_raw = raw.get("amount") or row.get("realizedPnlGross") or row.get("realizedPnlNet") or ""
        fee_raw = raw.get("fee") or row.get("fee") or ""
        tax_raw = raw.get("tax") or row.get("tax") or ""
        account = raw.get("account") or row.get("account") or ""
        dedup_key = make_dedup_key(
            str(code),
            str(date_value),
            str(trade_label),
            str(qty_raw),
            str(price_raw),
            str(amount_raw),
            str(fee_raw),
            str(tax_raw),
            str(account)
        )
        if dedup_key in global_dedup_keys:
            code_key = str(code) if code is not None else "unknown"
            global_duplicate_counts[code_key] = global_duplicate_counts.get(code_key, 0) + 1
            continue
        global_dedup_keys.add(dedup_key)
        deduped_rows.append(row)

    if global_duplicate_counts:
        for code, count in global_duplicate_counts.items():
            warnings.append(
                {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
            )
        log_dedup_summary(global_duplicate_counts)

    rows = deduped_rows
    rows.sort(
        key=lambda item: (item.get("date", ""), item.get("_event_order", 2), item.get("_row_index", 0))
    )

    _trade_cache["key"] = key
    _trade_cache["rows"] = rows
    _trade_cache["warnings"] = warnings
    return {"rows": rows, "warnings": warnings}

