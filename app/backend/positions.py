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


ACTION_LONG_OPEN = "LONG_OPEN"
ACTION_LONG_CLOSE = "LONG_CLOSE"
ACTION_SHORT_OPEN = "SHORT_OPEN"
ACTION_SHORT_CLOSE = "SHORT_CLOSE"

RAKUTEN_HEADERS = {
    "約定日": "trade_date",
    "銘柄コード": "symbol",
    "取引区分": "trade_type",
    "売買区分": "trade_side",
    "数量": "qty",
    "単価": "price"
}

SBI_HEADERS = {
    "約定日": "trade_date",
    "銘柄コード": "symbol",
    "取引": "trade_kind",
    "約定数量": "qty",
    "約定単価": "price"
}


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", "", text)
    return text


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _make_row_hash(broker: str, row: list[str]) -> str:
    payload = f"{broker}|{'|'.join(row)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _find_header_row(rows: list[list[str]], header_keys: list[str]) -> int | None:
    for idx, row in enumerate(rows):
        normalized = [_normalize_text(cell) for cell in row]
        if all(any(key in cell for cell in normalized) for key in header_keys):
            return idx
    return None


def _extract_column_map(headers: list[str], expected: dict[str, str]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for idx, header in enumerate(headers):
        normalized = _normalize_text(header)
        for key, name in expected.items():
            if key in normalized and name not in mapping:
                mapping[name] = idx
    return mapping


def _read_csv_bytes(data: bytes) -> list[list[str]]:
    for encoding in ("utf-8-sig", "cp932", "utf-8"):
        try:
            text = data.decode(encoding)
            reader = csv.reader(io.StringIO(text))
            return [row for row in reader]
        except UnicodeDecodeError:
            continue
    text = data.decode("utf-8", errors="replace")
    return [row for row in csv.reader(io.StringIO(text))]


def parse_rakuten_csv(data: bytes) -> tuple[list[TradeEvent], list[str]]:
    rows = _read_csv_bytes(data)
    if not rows:
        return [], ["rakuten:empty"]
    header = rows[0]
    mapping = _extract_column_map(header, RAKUTEN_HEADERS)
    required = {"trade_date", "symbol", "trade_type", "trade_side", "qty"}
    missing = sorted(required - set(mapping.keys()))
    if missing:
        return [], [f"rakuten:missing_columns:{','.join(missing)}"]
    events: list[TradeEvent] = []
    warnings: list[str] = []
    for row in rows[1:]:
        if not row or len(row) < len(header):
            continue
        trade_type = _normalize_text(row[mapping["trade_type"]])
        trade_side = _normalize_text(row[mapping["trade_side"]])
        symbol = _normalize_text(row[mapping["symbol"]])
        exec_dt = _parse_date(row[mapping["trade_date"]])
        qty = _parse_float(row[mapping["qty"]]) or 0.0
        price = _parse_float(row[mapping.get("price")]) if "price" in mapping else None

        action = None
        if "現物" in trade_type:
            if "買付" in trade_side:
                action = ACTION_LONG_OPEN
            elif "売却" in trade_side or "売付" in trade_side:
                action = ACTION_LONG_CLOSE
        elif "信用新規" in trade_type:
            if "買建" in trade_side:
                action = ACTION_LONG_OPEN
            elif "売建" in trade_side:
                action = ACTION_SHORT_OPEN
        elif "信用返済" in trade_type:
            if "売返済" in trade_side:
                action = ACTION_LONG_CLOSE
            elif "買返済" in trade_side:
                action = ACTION_SHORT_CLOSE


        if not symbol or exec_dt is None or action is None:
            warnings.append(f"rakuten:unmapped:{trade_type}:{trade_side}:{symbol}")
            continue

        source_row_hash = _make_row_hash("rakuten", row)
        events.append(
            TradeEvent(
                broker="rakuten",
                exec_dt=exec_dt,
                symbol=symbol,
                action=action,
                qty=qty,
                price=price,
                source_row_hash=source_row_hash
            )
        )
    return events, warnings


def parse_sbi_csv(data: bytes) -> tuple[list[TradeEvent], list[str]]:
    rows = _read_csv_bytes(data)
    if not rows:
        return [], ["sbi:empty"]
    header_idx = _find_header_row(rows, ["約定日", "銘柄コード", "取引"])
    if header_idx is None:
        return [], ["sbi:header_not_found"]
    header = rows[header_idx]
    mapping = _extract_column_map(header, SBI_HEADERS)
    required = {"trade_date", "symbol", "trade_kind", "qty"}
    missing = sorted(required - set(mapping.keys()))
    if missing:
        return [], [f"sbi:missing_columns:{','.join(missing)}"]
    events: list[TradeEvent] = []
    warnings: list[str] = []
    for row in rows[header_idx + 1:]:
        if not row or len(row) < len(header):
            continue
        trade_kind = _normalize_text(row[mapping["trade_kind"]])
        symbol = _normalize_text(row[mapping["symbol"]])
        exec_dt = _parse_date(row[mapping["trade_date"]])
        qty = _parse_float(row[mapping["qty"]]) or 0.0
        price = _parse_float(row[mapping.get("price")]) if "price" in mapping else None

        action = None
        if "信用新規買" in trade_kind:
            action = ACTION_LONG_OPEN
        elif "信用返済売" in trade_kind:
            action = ACTION_LONG_CLOSE
        elif "信用新規売" in trade_kind:
            action = ACTION_SHORT_OPEN
        elif "信用返済買" in trade_kind:
            action = ACTION_SHORT_CLOSE
        elif "現物買" in trade_kind or "買付" in trade_kind:
            action = ACTION_LONG_OPEN
        elif "現物売" in trade_kind or "売付" in trade_kind:
            action = ACTION_LONG_CLOSE
        elif "現渡" in trade_kind or "現引" in trade_kind:
            action = ACTION_SHORT_CLOSE

        if not symbol or exec_dt is None or action is None:
            warnings.append(f"sbi:unmapped:{trade_kind}:{symbol}")
            continue

        source_row_hash = _make_row_hash("sbi", row)
        events.append(
            TradeEvent(
                broker="sbi",
                exec_dt=exec_dt,
                symbol=symbol,
                action=action,
                qty=qty,
                price=price,
                source_row_hash=source_row_hash
            )
        )
    return events, warnings


def rebuild_positions(conn) -> dict:
    rows = conn.execute(
        """
        SELECT broker, exec_dt, symbol, action, qty, price, source_row_hash
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
    for broker, exec_dt, symbol, action, qty, price, source_hash in rows:
        event = TradeEvent(
            broker=broker,
            exec_dt=exec_dt,
            symbol=symbol,
            action=action,
            qty=float(qty or 0),
            price=float(price) if price is not None else None,
            source_row_hash=source_hash
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
        buy_qty = seed["buy_qty"] if seed else 0.0
        sell_qty = seed["sell_qty"] if seed else 0.0
        opened_at = seed["asof_dt"] if seed and (buy_qty > 0 or sell_qty > 0) else None
        active_round_id = None
        round_issue = False
        issue_note = None
        seed_asof = seed["asof_dt"] if seed else None

        for event in events:
            if seed_asof and event.exec_dt and event.exec_dt < seed_asof:
                continue
            was_flat = buy_qty <= 0 and sell_qty <= 0
            if event.action == ACTION_LONG_OPEN:
                buy_qty += event.qty
            elif event.action == ACTION_LONG_CLOSE:
                buy_qty -= event.qty
            elif event.action == ACTION_SHORT_OPEN:
                sell_qty += event.qty
            elif event.action == ACTION_SHORT_CLOSE:
                sell_qty -= event.qty

            if buy_qty < 0 or sell_qty < 0:
                issue_count += 1
                round_issue = True
                issue_note = issue_note or f"negative_qty:{event.source_row_hash}"
                buy_qty = max(0.0, buy_qty)
                sell_qty = max(0.0, sell_qty)

            is_flat = buy_qty <= 0 and sell_qty <= 0
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
                        issue_note
                    )
                )
                active_round_id = None
                opened_at = None
                round_issue = False
                issue_note = None

        if buy_qty > 0 or sell_qty > 0:
            live_rows.append(
                (
                    symbol,
                    buy_qty,
                    sell_qty,
                    opened_at,
                    datetime.utcnow(),
                    round_issue,
                    issue_note
                )
            )

    if live_rows:
        conn.executemany(
            """
            INSERT INTO positions_live (
                symbol,
                buy_qty,
                sell_qty,
                opened_at,
                updated_at,
                has_issue,
                issue_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
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
