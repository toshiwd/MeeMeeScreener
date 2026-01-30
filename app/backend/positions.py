from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import csv
import hashlib
import io
import re
import uuid


@dataclass
class TradeEvent:
    broker: str
    exec_dt: datetime
    symbol: str
    action: str
    qty: float
    price: float | None
    source_row_hash: str
    transaction_type: str | None = None
    side_type: str | None = None
    margin_type: str | None = None


ACTION_SPOT_BUY = "SPOT_BUY"
ACTION_SPOT_SELL = "SPOT_SELL"
ACTION_SPOT_IN = "SPOT_IN"
ACTION_SPOT_OUT = "SPOT_OUT"
ACTION_MARGIN_OPEN_LONG = "MARGIN_OPEN_LONG"
ACTION_MARGIN_OPEN_SHORT = "MARGIN_OPEN_SHORT"
ACTION_MARGIN_CLOSE_LONG = "MARGIN_CLOSE_LONG"
ACTION_MARGIN_CLOSE_SHORT = "MARGIN_CLOSE_SHORT"
ACTION_DELIVERY_SHORT = "DELIVERY_SHORT"
ACTION_MARGIN_SWAP_TO_SPOT = "MARGIN_SWAP_TO_SPOT"
ACTION_UNKNOWN = "UNKNOWN"


RAKUTEN_HASH_KEYS = [
    "約定日",
    "受渡日",
    "銘柄コード",
    "取引区分",
    "売買区分",
    "信用区分",
    "弁済期限",
    "数量［株］",
    "単価［円］",
    "受渡金額［円］",
    "建約定日",
    "建単価［円］"
]


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "")
    if text.strip().lower() in ("nan", "none", "--"):
        return ""
    text = text.replace("\u3000", " ").strip()
    return text


def _normalize_label(value: str | None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    return re.sub(r"\s+", "", text)


def _normalize_number_text(value: str | None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    return text.replace(",", "")


def _parse_float(value: str | None) -> float | None:
    text = _normalize_number_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(value: str | None) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _read_csv_bytes(data: bytes) -> tuple[list[list[str]], list[str]]:
    warnings: list[str] = []
    for encoding in ("cp932", "utf-8-sig", "utf-8"):
        try:
            text = data.decode(encoding)
            reader = csv.reader(io.StringIO(text))
            return [row for row in reader], warnings
        except UnicodeDecodeError:
            continue
    warnings.append("decode_failed:cp932")
    text = data.decode("utf-8", errors="replace")
    return [row for row in csv.reader(io.StringIO(text))], warnings


def _build_header_map(headers: list[str], expected: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    normalized_headers = { _normalize_label(h): h for h in headers }
    for key in expected:
        normalized = _normalize_label(key)
        if normalized in normalized_headers:
            mapping[key] = normalized_headers[normalized]
    return mapping


def _resolve_header(headers: list[str], candidates: list[str]) -> str | None:
    normalized_headers = { _normalize_label(h): h for h in headers }
    for candidate in candidates:
        normalized = _normalize_label(candidate)
        if normalized in normalized_headers:
            return normalized_headers[normalized]
    return None


def _build_rakuten_row_hash(row: dict, headers: list[str], row_index: int) -> str:
    parts = ["rakuten"]
    parts.append(f"row_index:{row_index}")
    for header in headers:
        raw = row.get(header, "")
        norm = _normalize_text(raw)
        parts.append(f"{header}:{norm}")
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_symbol(raw: str) -> str:
    text = _normalize_text(raw)
    if text.endswith(".T"):
        text = text[:-2]
    match = re.match(r"^(\d{4})", text)
    if match:
        return match.group(1)
    return text



def _map_parser_row_to_event(row: dict) -> TradeEvent:
    kind = row.get("kind")
    memo = row.get("memo", "")
    
    action = ACTION_UNKNOWN
    
    is_spot = "現物" in memo or "買付" in memo or "売付" in memo or "現渡" in memo or "現引" in memo or "入庫" in memo or "出庫" in memo
    
    if kind == "BUY_OPEN":
        action = ACTION_SPOT_BUY if is_spot else ACTION_MARGIN_OPEN_LONG
    elif kind == "SELL_CLOSE":
        action = ACTION_SPOT_SELL if is_spot else ACTION_MARGIN_CLOSE_LONG
    elif kind == "SELL_OPEN":
        action = ACTION_MARGIN_OPEN_SHORT
    elif kind == "BUY_CLOSE":
        action = ACTION_MARGIN_CLOSE_SHORT
    elif kind == "DELIVERY":
        action = ACTION_DELIVERY_SHORT
    elif kind == "TAKE_DELIVERY":
        action = ACTION_MARGIN_SWAP_TO_SPOT
    elif kind == "INBOUND":
        action = ACTION_SPOT_IN
    elif kind == "OUTBOUND":
        action = ACTION_SPOT_OUT
        
    if action == ACTION_UNKNOWN:
        if "現渡" in memo: action = ACTION_DELIVERY_SHORT
        elif "現引" in memo: action = ACTION_MARGIN_SWAP_TO_SPOT
        elif "入庫" in memo: action = ACTION_SPOT_IN
        elif "出庫" in memo: action = ACTION_SPOT_OUT

    unique_str = f"{row.get('broker')}|{row.get('tradeDate')}|{row.get('code')}|{row.get('qty')}|{row.get('price')}|{row.get('memo')}|{row.get('_row_index')}"
    source_hash = hashlib.sha256(unique_str.encode("utf-8")).hexdigest()

    return TradeEvent(
        broker=str(row.get("broker", "")).lower(),
        exec_dt=datetime.strptime(row["tradeDate"], "%Y-%m-%d"),
        symbol=str(row["code"]),
        action=action,
        qty=float(row["qty"]),
        price=float(row["price"]) if row["price"] is not None else None,
        source_row_hash=source_hash,
        transaction_type=row.get("memo"),
        side_type=row.get("side", ""),
        margin_type="" 
    )



def parse_rakuten_csv(data: bytes) -> tuple[list[TradeEvent], list[str]]:
    # Decode
    text = ""
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
             text = data.decode(enc)
             break
        except: continue
    if not text:
        return [], ["decode_failed"]
        
    rows_all = list(csv.reader(text.splitlines()))
    result = TradeParser.parse_rakuten_rows(rows_all)
    
    events = [_map_parser_row_to_event(r) for r in result["rows"]]
    warnings = [w["message"] for w in result["warnings"]]
    return events, warnings


def _find_header_row(rows: list[list[str]], header_keys: list[str]) -> int | None:
    for idx, row in enumerate(rows):
        normalized = [_normalize_label(cell) for cell in row]
        if all(any(_normalize_label(key) == cell for cell in normalized) for key in header_keys):
            return idx
    return None



def parse_sbi_csv(data: bytes) -> tuple[list[TradeEvent], list[str]]:
    text = ""
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
             text = data.decode(enc)
             break
        except: continue
    if not text:
        return [], ["decode_failed"]
        
    rows_all = list(csv.reader(text.splitlines()))
    result = TradeParser.parse_sbi_rows(rows_all)
    
    events = [_map_parser_row_to_event(r) for r in result["rows"]]
    warnings = [w["message"] for w in result["warnings"]]
    return events, warnings


def rebuild_positions(conn) -> dict:
    rows = conn.execute(
        """
        SELECT broker, exec_dt, symbol, action, qty, price, source_row_hash,
               transaction_type, side_type, margin_type
        FROM trade_events
        ORDER BY exec_dt, source_row_hash
        """
    ).fetchall()
    seeds = conn.execute(
        "SELECT symbol, buy_qty, sell_qty, asof_dt, memo FROM initial_positions_seed"
    ).fetchall()
    seed_map = {
        row[0]: {
            "buy_qty": float(row[1] or 0),
            "sell_qty": float(row[2] or 0),
            "asof_dt": row[3],
            "memo": row[4]
        }
        for row in seeds
    }

    grouped: dict[str, list[TradeEvent]] = {}
    for (
        broker, exec_dt, symbol, action, qty, price,
        source_hash, transaction_type, side_type, margin_type
    ) in rows:
        event = TradeEvent(
            broker=broker,
            exec_dt=exec_dt,
            symbol=symbol,
            action=action,
            qty=float(qty or 0),
            price=float(price) if price is not None else None,
            source_row_hash=source_hash,
            transaction_type=transaction_type,
            side_type=side_type,
            margin_type=margin_type
        )
        grouped.setdefault(symbol, []).append(event)

    conn.execute("DELETE FROM positions_live")
    conn.execute("DELETE FROM position_rounds")

    round_rows: list[tuple] = []
    live_rows: list[tuple] = []
    issue_count = 0

    symbols = sorted(set(grouped.keys()) | set(seed_map.keys()))
    for symbol in symbols:
        events = grouped.get(symbol, [])
        seed = seed_map.get(symbol)
        seed_buy = seed["buy_qty"] if seed else 0.0
        seed_sell = seed["sell_qty"] if seed else 0.0
        opened_at = seed["asof_dt"] if seed and (seed_buy != 0 or seed_sell != 0) else None
        active_round_id = None
        round_issue = False
        issue_notes: list[str] = []
        symbol_has_issue = False
        seed_asof = seed["asof_dt"] if seed else None

        # Seed is stored as aggregated buy/sell. Treat buy as spot and sell as short.
        spot_qty = float(seed_buy or 0)
        margin_long_qty = 0.0
        margin_short_qty = float(seed_sell or 0)

        for event in events:
            if seed_asof and event.exec_dt and event.exec_dt < seed_asof:
                continue

            buy_total = spot_qty + margin_long_qty
            sell_total = margin_short_qty
            was_flat = buy_total == 0 and sell_total == 0

            if event.action == ACTION_SPOT_BUY or event.action == ACTION_SPOT_IN:
                spot_qty += event.qty
            elif event.action == ACTION_SPOT_SELL:
                if event.qty > spot_qty:
                    round_issue = True
                    symbol_has_issue = True
                    issue_notes.append(f"Spot sell exceeds holdings at {event.exec_dt}")
                spot_qty = max(0.0, spot_qty - event.qty)
            elif event.action == ACTION_SPOT_OUT:
                if event.qty > spot_qty:
                    round_issue = True
                    symbol_has_issue = True
                    issue_notes.append(f"Spot outbound exceeds holdings at {event.exec_dt}")
                spot_qty = max(0.0, spot_qty - event.qty)
            elif event.action == ACTION_MARGIN_OPEN_LONG:
                margin_long_qty += event.qty
            elif event.action == ACTION_MARGIN_CLOSE_LONG:
                if event.qty > margin_long_qty:
                    round_issue = True
                    symbol_has_issue = True
                    issue_notes.append(f"Margin long close exceeds holdings at {event.exec_dt}")
                margin_long_qty = max(0.0, margin_long_qty - event.qty)
            elif event.action == ACTION_MARGIN_OPEN_SHORT:
                margin_short_qty += event.qty
            elif event.action == ACTION_MARGIN_CLOSE_SHORT:
                if event.qty > margin_short_qty:
                    round_issue = True
                    symbol_has_issue = True
                    issue_notes.append(f"Margin short close exceeds holdings at {event.exec_dt}")
                margin_short_qty = max(0.0, margin_short_qty - event.qty)
            elif event.action == ACTION_DELIVERY_SHORT:
                if event.qty > spot_qty or event.qty > margin_short_qty:
                    round_issue = True
                    symbol_has_issue = True
                    issue_notes.append(f"Delivery exceeds holdings at {event.exec_dt}")
                spot_qty = max(0.0, spot_qty - event.qty)
                margin_short_qty = max(0.0, margin_short_qty - event.qty)
            elif event.action == ACTION_MARGIN_SWAP_TO_SPOT:
                if event.qty > margin_long_qty:
                    round_issue = True
                    symbol_has_issue = True
                    issue_notes.append(f"Genbiki exceeds holdings at {event.exec_dt}")
                move_qty = min(event.qty, margin_long_qty)
                margin_long_qty -= move_qty
                spot_qty += move_qty
            elif event.action == ACTION_UNKNOWN:
                round_issue = True
                symbol_has_issue = True
                issue_notes.append(f"Unknown action {event.transaction_type}/{event.side_type}")

            if spot_qty < 0 or margin_long_qty < 0 or margin_short_qty < 0:
                round_issue = True
                symbol_has_issue = True
                issue_notes.append(f"Negative qty at {event.exec_dt}")

            buy_total = spot_qty + margin_long_qty
            sell_total = margin_short_qty
            is_flat = buy_total == 0 and sell_total == 0

            if was_flat and not is_flat:
                opened_at = event.exec_dt
                active_round_id = uuid.uuid4().hex
            if not was_flat and is_flat and active_round_id:
                round_rows.append(
                    (
                        active_round_id,
                        symbol,
                        opened_at,
                        event.exec_dt,
                        "FLAT",
                        "0-0",
                        round_issue,
                        "; ".join(issue_notes) if issue_notes else None
                    )
                )
                active_round_id = None
                opened_at = None
                round_issue = False
                issue_notes = []

        spot_qty = round(spot_qty)
        margin_long_qty = round(margin_long_qty)
        margin_short_qty = round(margin_short_qty)
        buy_total = spot_qty + margin_long_qty
        sell_total = margin_short_qty

        if symbol_has_issue:
            issue_count += 1

        if buy_total != 0 or sell_total != 0:
            live_rows.append(
                (
                    symbol,
                    spot_qty,
                    margin_long_qty,
                    margin_short_qty,
                    buy_total,
                    sell_total,
                    opened_at,
                    datetime.utcnow(),
                    round_issue,
                    "; ".join(issue_notes) if issue_notes else None
                )
            )

    if live_rows:
        conn.executemany(
            """
            INSERT INTO positions_live (
                symbol,
                spot_qty,
                margin_long_qty,
                margin_short_qty,
                buy_qty,
                sell_qty,
                opened_at,
                updated_at,
                has_issue,
                issue_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            live_rows
        )
    if round_rows:
        conn.executemany(
            """
            INSERT INTO position_rounds (
                round_id,
                symbol,
                opened_at,
                closed_at,
                closed_reason,
                last_state_sell_buy,
                has_issue,
                issue_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            round_rows
        )
    return {"positions": len(live_rows), "rounds": len(round_rows), "issues": issue_count}
