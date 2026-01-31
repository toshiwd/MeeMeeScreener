from __future__ import annotations

import re
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any, Set

class TradeParser:
    @staticmethod
    def normalize_text(value: Optional[Any]) -> str:
        if value is None:
            return ""
        text = str(value).replace("\ufeff", "")
        if text.strip().lower() in ("nan", "none", "--"):
            return ""
        text = text.replace("\u3000", " ")
        return text.strip()

    @staticmethod
    def normalize_label(value: Optional[str]) -> str:
        text = TradeParser.normalize_text(value)
        if not text:
            return ""
        return re.sub(r"\s+", "", text)

    @staticmethod
    def normalize_code(code: str) -> Optional[str]:
        if not code:
            return None
        c = str(code).strip()
        if "." in c:
            c = c.split(".")[0]
        # Remove odd characters if any
        c = re.sub(r"\D", "", c)
        if len(c) == 4:
            return c
        return None

    @staticmethod
    def to_float(value: Any) -> float:
        try:
            return float(str(value).replace(",", ""))
        except ValueError:
            return 0.0

    @staticmethod
    def to_optional_float(value: Any) -> Optional[float]:
        text = TradeParser.normalize_text(value)
        if not text:
            return None
        try:
            return float(text.replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def parse_date(value: Any) -> Optional[str]:
        raw = TradeParser.normalize_text(value)
        if not raw:
            return None
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    @staticmethod
    def make_dedup_key(
        code: str,
        date_value: Optional[str],
        trade_label: str,
        qty_raw: str,
        price_raw: str,
        amount_raw: str,
        fee_raw: str = "",
        tax_raw: str = "",
        account: str = ""
    ) -> str:
        parts = [
            TradeParser.normalize_text(code),
            TradeParser.normalize_text(date_value or ""),
            TradeParser.normalize_label(trade_label),
            TradeParser.normalize_text(qty_raw),
            TradeParser.normalize_text(price_raw),
            TradeParser.normalize_text(amount_raw),
            TradeParser.normalize_text(fee_raw),
            TradeParser.normalize_text(tax_raw),
            TradeParser.normalize_text(account)
        ]
        return "|".join(parts)

    @staticmethod
    def find_sbi_header_index(rows_all: List[List[str]]) -> Optional[int]:
        start = min(6, max(0, len(rows_all)))
        for idx in range(start, min(len(rows_all), start + 6)):
            row = rows_all[idx]
            if not row or not any(cell.strip() for cell in row):
                continue
            if "約定日" in row or "銘柄コード" in row:
                return idx
        return None

    @staticmethod
    def looks_like_sbi(rows_all: List[List[str]]) -> bool:
        # Check first few rows for markers
        for row in rows_all[:10]:
            if any("CSV作成日" in cell for cell in row):
                return True
        header_index = TradeParser.find_sbi_header_index(rows_all)
        if header_index is None:
            return False
            
        header_row = [cell.strip() for cell in rows_all[header_index]]
        if any(name in header_row for name in ("受渡金額/決済損益", "決済損益", "受渡金額", "手数料/諸経費等")):
            return True
            
        if "取引" in header_row:
            trade_idx = header_row.index("取引")
            # Peek ahead to see if transaction types match SBI
            for row in rows_all[header_index + 1 : header_index + 50]:
                if trade_idx < len(row) and any(
                    key in row[trade_idx] for key in ("信用新規買", "信用返済売", "信用新規売", "信用返済買")
                ):
                    return True
        return False

    @staticmethod
    def determine_event_kind(label: str, type_label: str = "") -> Tuple[Optional[str], str, str]:
        """
        Returns (event_kind, side, action)
        """
        # Combined normalization
        full_label = label + type_label
        
        event_kind = None
        
        # Explicit Matches
        if "現渡" in full_label: # Usually Delivery
            event_kind = "DELIVERY"
        elif "現引" in full_label: # Usually Take Delivery
            event_kind = "TAKE_DELIVERY"
        elif "入庫" in full_label:
            event_kind = "INBOUND"
        elif "出庫" in full_label:
            event_kind = "OUTBOUND"
            
        if event_kind is None:
            actual_label = TradeParser.normalize_text(full_label)
            fallback_terms = {
                "BUY_OPEN": ("信用新規買", "買建", "現物買", "買付", "新規買", "信用買"),
                "SELL_CLOSE": ("信用返済売", "売埋", "現物売", "売付", "決済売"),
                "SELL_OPEN": ("信用新規売", "売建", "空売"),
                "BUY_CLOSE": ("信用返済買", "買埋", "返済買", "買戻")
            }
            for kind, markers in fallback_terms.items():
                if any(marker and (marker in full_label or marker in actual_label) for marker in markers):
                    event_kind = kind
                    break
            if event_kind is None:
                if "現渡" in actual_label or "現渡" in full_label:
                    event_kind = "DELIVERY"
                elif "現引" in actual_label or "現引" in full_label:
                    event_kind = "TAKE_DELIVERY"
        
        # Derive Side/Action
        side = "buy"
        action = "open"
        
        if event_kind == "BUY_OPEN":
            side, action = ("buy", "open")
        elif event_kind == "BUY_CLOSE":
            side, action = ("buy", "close")
        elif event_kind == "SELL_OPEN":
            side, action = ("sell", "open")
        elif event_kind == "SELL_CLOSE":
            side, action = ("sell", "close")
            
        return event_kind, side, action

    @staticmethod
    def parse_sbi_rows(rows_all: List[List[str]], encoding_used: str = "") -> Dict[str, Any]:
        warnings: List[Dict] = []
        rows: List[Dict] = []
        
        header_index = TradeParser.find_sbi_header_index(rows_all)
        if header_index is None:
            warnings.append({"type": "sbi_header_missing", "message": "sbi_header_missing"})
            return {"rows": [], "warnings": warnings}

        header = [cell.strip() for cell in rows_all[header_index]]

        def find_col(*names: str) -> Optional[int]:
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

        dedup_keys: Set[str] = set()
        duplicate_counts: Dict[str, int] = {}
        unknown_labels_by_code: Dict[str, Set[str]] = {}

        for row_index, line in enumerate(rows_all[header_index + 1 :], start=1):
            if not line or col_trade_date is None or col_code is None:
                continue
            if not any(cell.strip() for cell in line):
                continue
                
            get_val = lambda idx: line[idx] if idx is not None and idx < len(line) else ""
            
            date_value = TradeParser.parse_date(get_val(col_trade_date))
            code_raw = TradeParser.normalize_text(get_val(col_code))
            if not date_value or not code_raw:
                continue
                
            code = TradeParser.normalize_code(code_raw)
            if not code:
                continue

            name = TradeParser.normalize_text(get_val(col_name))
            market = TradeParser.normalize_text(get_val(col_market))
            account = TradeParser.normalize_text(get_val(col_account))
            trade_raw = TradeParser.normalize_text(get_val(col_trade))
            qty_raw = TradeParser.normalize_text(get_val(col_qty))
            price_raw = TradeParser.normalize_text(get_val(col_price))
            fee_raw = TradeParser.normalize_text(get_val(col_fee))
            tax_raw = TradeParser.normalize_text(get_val(col_tax))
            amount_raw = TradeParser.normalize_text(get_val(col_amount))
            settle_date = TradeParser.parse_date(get_val(col_settle_date))

            qty_shares = TradeParser.to_float(qty_raw)
            if qty_shares <= 0:
                continue
                
            if qty_shares % 100 != 0:
                warnings.append({
                    "type": "non_100_shares",
                    "message": f"non_100_shares:{code}:{date_value}:{qty_shares}",
                    "code": code
                })

            price = TradeParser.to_optional_float(price_raw)
            fee = TradeParser.to_optional_float(fee_raw)
            tax = TradeParser.to_optional_float(tax_raw)
            amount = TradeParser.to_optional_float(amount_raw)
            
            realized_net = None
            if amount is not None:
                realized_net = amount
                if fee is not None: realized_net -= fee
                if tax is not None: realized_net -= tax

            trade_label = TradeParser.normalize_label(trade_raw)
            event_kind, side, action = TradeParser.determine_event_kind(trade_label)

            if event_kind is None:
                sample = f"取引={trade_raw or '(blank)'}"
                unknown_labels_by_code.setdefault(code, set()).add(sample)
                continue

            dedup_key = TradeParser.make_dedup_key(
                code, date_value, trade_label, qty_raw, price_raw, 
                amount_raw, fee_raw, tax_raw, account
            )
            
            if dedup_key in dedup_keys:
                duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                continue
            dedup_keys.add(dedup_key)

            txn_type = "CORPORATE_ACTION"
            if event_kind == "BUY_OPEN": txn_type = "OPEN_LONG"
            elif event_kind == "SELL_CLOSE": txn_type = "CLOSE_LONG"
            elif event_kind == "SELL_OPEN": txn_type = "OPEN_SHORT"
            elif event_kind == "BUY_CLOSE": txn_type = "CLOSE_SHORT"

            event_order = 0
            if event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                event_order = 1
            elif event_kind not in ("BUY_OPEN", "SELL_OPEN"):
                event_order = 2

            rows.append({
                "broker": "SBI",
                "tradeDate": date_value,
                "settleDate": settle_date,
                "code": code,
                "name": name,
                "market": market,
                "account": account,
                "txnType": txn_type,
                "qty": qty_shares,
                "price": price if price is not None and price > 0 else None,
                "fee": fee,
                "tax": tax,
                "realizedPnlGross": amount,
                "realizedPnlNet": realized_net,
                "memo": trade_raw,
                "side": side,
                "action": action,
                "kind": event_kind,
                "_row_index": row_index,
                "_event_order": event_order,
                "raw": {
                    "date": date_value,
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
        })

        # Process Warnings
        for code, count in duplicate_counts.items():
            warnings.append({"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code})

        for code, samples_set in unknown_labels_by_code.items():
            samples = sorted(list(samples_set))[:5]
            warnings.append({"type": "unrecognized_labels", "count": len(samples_set), "samples": samples, "code": code})
        
        rows.sort(key=lambda item: (item.get("tradeDate", ""), item.get("_event_order", 2), item.get("_row_index", 0)))
        return {"rows": rows, "warnings": warnings}

    @staticmethod
    def parse_rakuten_rows(rows_all: List[List[str]], encoding_used: str = "") -> Dict[str, Any]:
        warnings: List[Dict] = []
        rows: List[Dict] = []
        
        if not rows_all:
            return {"rows": [], "warnings": []}

        header = [TradeParser.normalize_text(cell) for cell in rows_all[0]]
        data_rows = rows_all[1:]
        col_map = {name: index for index, name in enumerate(header) if name}

        def get_cell(row: List[str], *keys: str) -> str:
            for key in keys:
                if not key:
                    continue
                idx = col_map.get(key)
                if idx is None or idx >= len(row):
                    continue
                return TradeParser.normalize_text(row[idx])
            return ""

        dedup_keys: Set[str] = set()
        duplicate_counts: Dict[str, int] = {}
        unknown_labels_by_code: Dict[str, Set[str]] = {}

        for row_index, row in enumerate(data_rows, start=1):
            if not row or not any(cell.strip() for cell in row):
                continue
            
            date_raw = get_cell(row, "約定日", "取引年月日", "日付")
            date_value = TradeParser.parse_date(date_raw)
            if not date_value:
                continue
                
            code_raw = (
                get_cell(row, "銘柄コード")
                or get_cell(row, "銘柄コード（4桁）")
                or get_cell(row, "銘柄ｺｰﾄﾞ")
                or get_cell(row, "銘柄")
            )
            code = TradeParser.normalize_code(code_raw)
            if not code:
                continue

            name = get_cell(row, "銘柄名") or get_cell(row, "銘柄")
            market = get_cell(row, "市場")
            account = get_cell(row, "口座区分") or get_cell(row, "預り区分") or get_cell(row, "口座")
            settle_date = TradeParser.parse_date(get_cell(row, "受渡日"))
            
            type_raw = get_cell(row, "取引区分")
            kind_raw = get_cell(row, "売買区分")
            
            qty_raw = get_cell(row, "数量［株］") or get_cell(row, "数量[株]") or get_cell(row, "数量")
            qty_shares = TradeParser.to_float(qty_raw)
            if qty_shares <= 0:
                continue

            price_raw = get_cell(row, "単価［円］") or get_cell(row, "単価[円]") or get_cell(row, "単価") or get_cell(row, "約定単価")
            price = TradeParser.to_optional_float(price_raw)
            
            amount_raw = get_cell(row, "受渡金額［円］") or get_cell(row, "受渡金額[円]") or get_cell(row, "受渡金額")
            fee_raw = get_cell(row, "手数料［円］") or get_cell(row, "手数料[円]") or get_cell(row, "手数料")
            tax_raw = get_cell(row, "税金等［円］") or get_cell(row, "税金等[円]") or get_cell(row, "税金")
            
            amount = TradeParser.to_optional_float(amount_raw)
            fee = TradeParser.to_optional_float(fee_raw)
            tax = TradeParser.to_optional_float(tax_raw)

            event_kind, side, action = TradeParser.determine_event_kind(kind_raw, type_raw)

            if event_kind is None:
                sample = f"取引区分={type_raw or '(blank)'}, 売買区分={kind_raw or '(blank)'}"
                unknown_labels_by_code.setdefault(code, set()).add(sample)
                continue

            dedup_key = TradeParser.make_dedup_key(
                code, date_value, type_raw + "|" + kind_raw,
                qty_raw, price_raw, amount_raw, fee_raw, tax_raw, account
            )
            
            if dedup_key in dedup_keys:
                duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                continue
            dedup_keys.add(dedup_key)
            
            txn_type = "CORPORATE_ACTION"
            if event_kind == "BUY_OPEN": txn_type = "OPEN_LONG"
            elif event_kind == "SELL_CLOSE": txn_type = "CLOSE_LONG"
            elif event_kind == "SELL_OPEN": txn_type = "OPEN_SHORT"
            elif event_kind == "BUY_CLOSE": txn_type = "CLOSE_SHORT"

            event_order = 0
            if event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                event_order = 1
            elif event_kind not in ("BUY_OPEN", "SELL_OPEN"):
                event_order = 2

            rows.append({
                "broker": "RAKUTEN",
                "tradeDate": date_value,
                "settleDate": settle_date,
                "code": code,
                "name": name,
                "market": market,
                "account": account,
                "txnType": txn_type,
                "qty": qty_shares,
                "price": price if price is not None and price > 0 else None,
                "fee": fee,
                "tax": tax,
                "realizedPnlGross": None, # Rakuten CSV usually doesn't have per-trade realization in this format
                "realizedPnlNet": None,
                "memo": kind_raw or type_raw,
                "side": side,
                "action": action,
                "kind": event_kind,
                "_row_index": row_index,
                "_event_order": event_order,
                "raw": {
                    "date": date_value,
                    "code": code,
                    "name": name,
                    "trade": kind_raw,
                    "type": type_raw,
                    "qty": qty_raw,
                    "price": price_raw,
                    "amount": amount_raw,
                    "encoding": encoding_used
                }
            })

        for code, count in duplicate_counts.items():
            warnings.append({"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code})

        for code, samples_set in unknown_labels_by_code.items():
            samples = sorted(list(samples_set))[:5]
            warnings.append({"type": "unrecognized_labels", "count": len(samples_set), "samples": samples, "code": code})

        rows.sort(key=lambda item: (item.get("tradeDate", ""), item.get("_event_order", 2), item.get("_row_index", 0)))
        return {"rows": rows, "warnings": warnings}
