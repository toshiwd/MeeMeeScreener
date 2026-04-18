import duckdb
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

# Check result.duckdb for 2531
con = duckdb.connect('G:/Tradex/db/result.duckdb', read_only=True)

# Check candidate_daily for 2531 - latest entries
print("=== candidate_daily for 2531 ===")
df = con.execute("SELECT * FROM candidate_daily WHERE code='2531' ORDER BY as_of_date DESC LIMIT 10").df()
print(df)

# Check forecast_surface_daily  
print("\n=== forecast_surface_daily ===")
try:
    cols = con.execute("DESCRIBE forecast_surface_daily").fetchall()
    col_names = [c[0] for c in cols]
    print(f"  columns: {col_names}")
    if 'code' in col_names:
        df = con.execute("SELECT * FROM forecast_surface_daily WHERE code='2531' ORDER BY 1 DESC LIMIT 5").df()
        print(df)
except Exception as e:
    print(f"  ERROR: {e}")

# Check regime_daily
print("\n=== regime_daily ===")
try:
    cols = con.execute("DESCRIBE regime_daily").fetchall()
    col_names = [c[0] for c in cols]
    print(f"  columns: {col_names}")
    df2 = con.execute("SELECT * FROM regime_daily ORDER BY 1 DESC LIMIT 5").df()
    print(df2)
except Exception as e:
    print(f"  ERROR: {e}")

# Check signal_decision_daily from stocks.duckdb
print("\n=== signal_decision_daily from stocks.duckdb ===")
con2 = duckdb.connect('G:/Tradex/db/stocks.duckdb', read_only=True)
try:
    cols = con2.execute("DESCRIBE signal_decision_daily").fetchall()
    col_names = [c[0] for c in cols]
    print(f"  columns: {col_names}")
    cnt = con2.execute("SELECT COUNT(*) FROM signal_decision_daily").fetchone()[0]
    print(f"  total rows: {cnt}")
    if cnt > 0:
        df3 = con2.execute("SELECT * FROM signal_decision_daily WHERE code='2531' ORDER BY 1 DESC LIMIT 5").df()
        print(df3)
except Exception as e:
    print(f"  ERROR: {e}")

# Check ranking_appearance_daily 
print("\n=== ranking_appearance_daily ===")
try:
    cols = con2.execute("DESCRIBE ranking_appearance_daily").fetchall()
    col_names = [c[0] for c in cols]
    print(f"  columns: {col_names}")
    cnt = con2.execute("SELECT COUNT(*) FROM ranking_appearance_daily").fetchone()[0]
    print(f"  total rows: {cnt}")
except Exception as e:
    print(f"  ERROR: {e}")

# Check feature_snapshot_daily
print("\n=== feature_snapshot_daily ===")
try:
    cols = con2.execute("DESCRIBE feature_snapshot_daily").fetchall()
    col_names = [c[0] for c in cols]
    print(f"  columns: {col_names}")
    cnt = con2.execute("SELECT COUNT(*) FROM feature_snapshot_daily").fetchone()[0]
    print(f"  total rows: {cnt}")
except Exception as e:
    print(f"  ERROR: {e}")

# Check market_regime_daily
print("\n=== market_regime_daily ===")
try:
    cols = con2.execute("DESCRIBE market_regime_daily").fetchall()
    col_names = [c[0] for c in cols]
    print(f"  columns: {col_names}")
    cnt = con2.execute("SELECT COUNT(*) FROM market_regime_daily").fetchone()[0]
    print(f"  total rows: {cnt}")
    if cnt > 0:
        df4 = con2.execute("SELECT * FROM market_regime_daily ORDER BY 1 DESC LIMIT 5").df()
        print(df4)
except Exception as e:
    print(f"  ERROR: {e}")

con.close()
con2.close()
