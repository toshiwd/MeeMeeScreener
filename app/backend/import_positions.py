import csv
import io
import hashlib
from datetime import datetime
from db import get_conn
from position_engine import recalculate_positions

def calculate_row_hash(row_data: dict) -> str:
    raw = "".join([str(k) + str(v) for k, v in sorted(row_data.items())])
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def parse_csv_rakuten(file_stream) -> list[dict]:
    # Rakuten CSV encoding is usually Shift-JIS
    wrapper = io.TextIOWrapper(file_stream, encoding='shift_jis')
    reader = csv.DictReader(wrapper)
    
    events = []
    
    for row in reader:
        exec_date_str = row.get("約定日", "")
        if not exec_date_str: continue

        try:
            exec_dt = datetime.strptime(exec_date_str, "%Y/%m/%d")
        except:
            continue
            
        code = row.get("銘柄コード", "").strip()
        # Normalize code
        if code.endswith('.T'):
            code = code[:-2]
        # Remove any non-digit chars if it looks like a standard JP ticker?
        # Keeping it simple for now, trusting strict equality match later
        code = code.split()[0] # In case of "7203 トヨタ" etc

        try:
            qty_str = row.get("約定数量", "0").replace(",", "")
            qty = float(qty_str)
            price_str = row.get("約定単価", "0").replace(",", "")
            price = float(price_str)
        except:
            continue
            
        kubun1 = row.get("取引区分", "")
        kubun2 = row.get("売買区分", "")
        
        action = None
        if "現物" in kubun1:
            if "買" in kubun2: action = "LONG_OPEN"
            elif "売" in kubun2: action = "LONG_CLOSE"
        elif "信用新規" in kubun1:
            if "買" in kubun2: action = "LONG_OPEN"
            elif "売" in kubun2: action = "SHORT_OPEN"
        elif "信用返済" in kubun1:
            if "売" in kubun2: action = "LONG_CLOSE" # 売埋
            elif "買" in kubun2: action = "SHORT_CLOSE" # 買埋
        
        if "現渡" in kubun1 or "現渡" in kubun2:
            action = "SHORT_CLOSE"

        if action and qty > 0:
            events.append({
                "broker": "rakuten",
                "exec_dt": exec_dt,
                "symbol": code,
                "action": action,
                "qty": qty,
                "price": price,
                "source_row_hash": calculate_row_hash(row)
            })
            
    return events

def save_events(events: list[dict]):
    conn = get_conn()
    try:
        inserted_count = 0
        skipped_count = 0
        
        for ev in events:
            # Check by hash
            existing = conn.execute("SELECT 1 FROM trade_events WHERE source_row_hash = ?", [ev["source_row_hash"]]).fetchone()
            if existing:
                skipped_count += 1
                continue
                
            conn.execute("""
                INSERT INTO trade_events (broker, exec_dt, symbol, action, qty, price, source_row_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                ev["broker"], ev["exec_dt"], ev["symbol"], ev["action"], ev["qty"], ev["price"], ev["source_row_hash"]
            ])
            inserted_count += 1
            
        return inserted_count, skipped_count
    except Exception as e:
        raise e
    finally:
        conn.close()
        
def process_import(file_bytes, broker="rakuten"):
    # file_bytes should be bytes
    file_stream = io.BytesIO(file_bytes)
    
    if broker == "rakuten":
        events = parse_csv_rakuten(file_stream)
    else:
        raise ValueError("Unsupported broker")
        
    inserted, skipped = save_events(events)
    
    # Recalc
    affected_symbols = set(e["symbol"] for e in events)
    for sym in affected_symbols:
        recalculate_positions(sym)
        
    return {"inserted": inserted, "skipped": skipped, "affected_symbols": list(affected_symbols)}
