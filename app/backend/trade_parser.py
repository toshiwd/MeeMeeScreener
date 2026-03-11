from __future__ import annotations

import re
import hashlib
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any, Set

class TradeParser:
    @staticmethod
    def normalize_text(value: Optional[Any]) -> str:
        if value is None:
            return ""
        text = str(value).replace("\ufeff", "")
        if text.strip().lower() in ("nan", "none", "--", "-", "－"):
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
        account: str = "",
        market: str = "",
        row_id: str = "" 
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
            TradeParser.normalize_text(account),
            TradeParser.normalize_text(market),
            str(row_id)
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
                amount_raw, fee_raw, tax_raw, account, market, row_id=str(row_index)
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
        col_map = {TradeParser.normalize_label(name): index for index, name in enumerate(header) if name}

        def get_cell(row: List[str], *keys: str) -> str:
            for key in keys:
                if not key:
                    continue
                idx = col_map.get(TradeParser.normalize_label(key))
                if idx is None or idx >= len(row):
                    continue
                return TradeParser.normalize_text(row[idx])
            return ""

        def normalize_number(value: Any) -> str:
            return TradeParser.normalize_text(value).replace(",", "")

        def parse_int_strict(value: Any) -> Optional[int]:
            text = normalize_number(value)
            if not text:
                return None
            try:
                return int(text)
            except ValueError:
                try:
                    parsed = float(text)
                except ValueError:
                    return None
                if parsed.is_integer():
                    return int(parsed)
            return None

        def resolve_rakuten_action(trade_type: str, side_label: str) -> Tuple[Optional[str], Optional[str]]:
            t = TradeParser.normalize_text(trade_type)
            s = TradeParser.normalize_text(side_label)
            if s == "入庫":
                return "SPOT_IN", "INBOUND"
            if s == "出庫":
                return "SPOT_OUT", "OUTBOUND"
            if t == "現物":
                if s == "買付":
                    return "SPOT_BUY", "BUY_OPEN"
                if s == "売付":
                    return "SPOT_SELL", "SELL_CLOSE"
            elif t == "信用新規":
                if s == "買建":
                    return "MARGIN_OPEN_LONG", "BUY_OPEN"
                if s == "売建":
                    return "MARGIN_OPEN_SHORT", "SELL_OPEN"
            elif t == "信用返済":
                if s == "売埋":
                    return "MARGIN_CLOSE_LONG", "SELL_CLOSE"
                if s == "買埋":
                    return "MARGIN_CLOSE_SHORT", "BUY_CLOSE"
            elif t == "現渡":
                return "DELIVERY_SHORT", "DELIVERY"
            elif t == "現引":
                return "MARGIN_SWAP_TO_SPOT", "TAKE_DELIVERY"
            return None, None

        def resolve_side_action(event_kind: Optional[str]) -> Tuple[str, str]:
            if event_kind == "BUY_OPEN":
                return "buy", "open"
            if event_kind == "BUY_CLOSE":
                return "buy", "close"
            if event_kind == "SELL_OPEN":
                return "sell", "open"
            if event_kind == "SELL_CLOSE":
                return "sell", "close"
            if event_kind == "DELIVERY":
                return "buy", "close"
            if event_kind == "TAKE_DELIVERY":
                return "buy", "open"
            if event_kind == "INBOUND":
                return "buy", "open"
            if event_kind == "OUTBOUND":
                return "sell", "close"
            return "", ""

        dedup_keys: Set[str] = set()
        duplicate_counts: Dict[str, int] = {}
        unknown_labels_by_code: Dict[str, Set[str]] = {}

        for row_index, row in enumerate(data_rows, start=1):
            if not row or not any(cell.strip() for cell in row):
                continue

            date_raw = get_cell(row, "約定日")
            date_value = TradeParser.parse_date(date_raw)
            if not date_value:
                continue

            code_raw = get_cell(row, "銘柄コード")
            code = TradeParser.normalize_code(code_raw)
            if not code:
                continue

            name = get_cell(row, "銘柄名")
            market = get_cell(row, "市場名称")
            account = get_cell(row, "口座区分")
            settle_date = TradeParser.parse_date(get_cell(row, "受渡日"))

            trade_type = get_cell(row, "取引区分")
            side_label = get_cell(row, "売買区分")
            credit_type = get_cell(row, "信用区分")
            expiry = get_cell(row, "弁済期限")

            qty_raw = get_cell(row, "数量［株］", "数量[株]")
            qty_shares = parse_int_strict(qty_raw)
            if qty_shares is None:
                warnings.append({"type": "invalid_number", "message": f"invalid_qty:{code}:{row_index}:{qty_raw}", "code": code})
                continue
            if qty_shares == 0:
                warnings.append({"type": "invalid_number", "message": f"zero_qty:{code}:{row_index}", "code": code})
                continue

            price_raw = get_cell(row, "単価［円］", "単価[円]")
            price = TradeParser.to_optional_float(price_raw)
            if price_raw and price is None:
                warnings.append({"type": "invalid_number", "message": f"invalid_price:{code}:{row_index}:{price_raw}", "code": code})

            amount_raw = get_cell(row, "受渡金額［円］", "受渡金額[円]")
            amount = TradeParser.to_optional_float(amount_raw)
            if amount_raw and amount is None:
                warnings.append({"type": "invalid_number", "message": f"invalid_amount:{code}:{row_index}:{amount_raw}", "code": code})

            fee_raw = get_cell(row, "手数料［円］", "手数料[円]")
            tax_raw = get_cell(row, "税金等［円］", "税金等[円]")
            fee = TradeParser.to_optional_float(fee_raw)
            tax = TradeParser.to_optional_float(tax_raw)

            open_date_raw = get_cell(row, "建約定日")
            open_date = TradeParser.parse_date(open_date_raw)
            open_price_raw = get_cell(row, "建単価［円］", "建単価[円]")
            open_price = TradeParser.to_optional_float(open_price_raw)

            position_action, event_kind = resolve_rakuten_action(trade_type, side_label)
            if event_kind is None:
                sample = f"取引区分:{trade_type or '(blank)'} 売買区分:{side_label or '(blank)'}"
                unknown_labels_by_code.setdefault(code, set()).add(sample)
                position_action = "UNKNOWN"
                event_kind = "UNKNOWN"

            side, action = resolve_side_action(event_kind)

            hash_parts = [
                date_value or "",
                settle_date or "",
                code,
                trade_type,
                side_label,
                credit_type,
                expiry,
                normalize_number(qty_raw),
                normalize_number(price_raw),
                normalize_number(amount_raw),
                open_date or "",
                normalize_number(open_price_raw),
            ]
            dedup_key = "|".join(hash_parts)
            if dedup_key in dedup_keys:
                duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
            else:
                dedup_keys.add(dedup_key)
            row_hash = hashlib.sha256("|".join([*hash_parts, str(row_index)]).encode("utf-8")).hexdigest()

            event_order = 0
            if event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                event_order = 1
            elif event_kind not in ("BUY_OPEN", "SELL_OPEN"):
                event_order = 2

            memo = " ".join([part for part in (trade_type, side_label) if part]).strip()

            rows.append({
                "broker": "RAKUTEN",
                "tradeDate": date_value,
                "settleDate": settle_date,
                "code": code,
                "name": name,
                "market": market,
                "account": account,
                "tradeType": trade_type,
                "buySell": side_label,
                "creditType": credit_type,
                "expiry": expiry,
                "qty": qty_shares,
                "price": price if price is not None and price > 0 else None,
                "fee": fee,
                "tax": tax,
                "realizedPnlGross": None, # Rakuten CSV usually doesn't have per-trade realization in this format
                "realizedPnlNet": None,
                "memo": memo or trade_type or side_label,
                "side": side,
                "action": action,
                "kind": event_kind,
                "position_action": position_action,
                "row_hash": row_hash,
                "openDate": open_date,
                "openPrice": open_price,
                "amount": amount,
                "_row_index": row_index,
                "_event_order": event_order,
                "raw": {
                    "date": date_value,
                    "code": code,
                    "name": name,
                    "trade": side_label,
                    "type": trade_type,
                    "qty": qty_raw,
                    "price": price_raw,
                    "amount": amount_raw,
                    "settle": settle_date,
                    "creditType": credit_type,
                    "expiry": expiry,
                    "openDate": open_date_raw,
                    "openPrice": open_price_raw,
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
