
import duckdb
import os
import pandas as pd
from datetime import datetime, timezone

DB_PATH = os.path.join("app", "backend", "stocks.duckdb")

def verify_timestamps():
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    code = "2413"
    print(f"Checking timestamps for {code}...")
    
    # Check Daily Bars (t)
    # t in daily_bars is integer
    bars = conn.execute("SELECT t, date FROM daily_bars WHERE code = ? ORDER BY t DESC LIMIT 5", [code]).fetchall()
    print("Daily Bars (latest 5):")
    for b in bars:
        t = b[0] # date column in DB is often YYYYMMDD integer? No, ingest puts 'date' as Unix Secs in 'daily' DF, but DB uses `t`?
        # Let's check DB schema for daily_bars
        # In `db.py`: CREATE TABLE daily_bars (code, t, ...)
        # So b[0] is t.
        # But wait, ingest_txt.py saves to `daily_bars`.
        # ingest_txt line 1025: `daily.rename(columns={"date": "t"}, inplace=True)`
        # And line 724: `daily["date"] = (daily["date"]... // 1e9)`
        # So `t` is Unix Seconds.
        
        # Let's interpret t
        dt_utc = datetime.fromtimestamp(t, timezone.utc)
        print(f"  t={t} -> {dt_utc}")
        
    print("\nCalculated Timestamps logic:")
    # Simulate my logic
    date_str = "2025/11/04"
    dt = datetime.strptime(date_str, "%Y/%m/%d") # Naive
    
    # Old logic (User environment likely JST/offset?)
    ts_local = int(dt.replace(tzinfo=None).timestamp())
    print(f"  Naive({date_str}).timestamp() [Local] = {ts_local}")
    
    # New logic (Force UTC)
    ts_utc = int(dt.replace(tzinfo=timezone.utc).timestamp())
    print(f"  Naive({date_str}).replace(tzinfo=utc).timestamp() = {ts_utc}")
    
    # Difference
    diff = ts_local - ts_utc
    print(f"  Diff (Local - UTC) = {diff/3600} hours")

if __name__ == "__main__":
    verify_timestamps()
