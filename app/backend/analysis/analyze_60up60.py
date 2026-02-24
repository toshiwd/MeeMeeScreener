
import os
import sys
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add project root to path
try:
    from app.core.config import config
    DB_PATH = str(config.DB_PATH)
except ImportError:
    # Fallback or try alternate import
    try:
        from app.backend.config import config
        DB_PATH = str(config.DB_PATH)
    except Exception as e:
        print(f"Config import failed: {e}")
        # Default fallback
        DB_PATH = "c:/work/meemee-screener/db/screener.duckdb"

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def run_analysis():
    print(f"Connecting to DB at {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    
    # Get all codes
    codes = [r[0] for r in con.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
    print(f"Found {len(codes)} codes.")

    results = []

    for code in codes:
        # Fetch data
        df = con.execute(f"SELECT date, c, o, h, l, v FROM daily_bars WHERE code = '{code}' ORDER BY date").df()
        if len(df) < 100:
            continue
            
        # Handle date format (timestamp or YYYYMMDD)
        is_ts = df['date'] > 1000000000
        df['date_dt'] = pd.NaT
        if is_ts.any():
            df.loc[is_ts, 'date_dt'] = pd.to_datetime(df.loc[is_ts, 'date'], unit='s')
        if (~is_ts).any():
            df.loc[~is_ts, 'date_dt'] = pd.to_datetime(df.loc[~is_ts, 'date'].astype(str), format='%Y%m%d', errors='coerce')
        
        df = df.dropna(subset=['date_dt'])
        df['date'] = df['date_dt']
        df.drop(columns=['date_dt'], inplace=True)
        df.set_index('date', inplace=True)
        
        # Calculate MA60
        df['ma60'] = df['c'].rolling(window=60).mean()
        df['ma20'] = df['c'].rolling(window=20).mean()
        
        # Identify "Above 60MA"
        df['above60'] = df['c'] > df['ma60']
        
        # Calculate streak
        # Group by consecutive True values
        # We want to count how many consecutive days it has been above
        df['streak_group'] = (df['above60'] != df['above60'].shift()).cumsum()
        df['streak_count'] = df.groupby('streak_group').cumcount() + 1
        df.loc[~df['above60'], 'streak_count'] = 0
        
        # Filter for days where streak >= 60
        # We are looking for "Any day where streak >= 60"
        # And we want to see if a drop happens AFTER this day.
        
        # Let's calculate forward returns
        # 5 day return, 10 day return, 20 day max drawdown
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=20)
        df['fwd_min_low_20'] = df['l'].rolling(window=indexer).min()
        df['fwd_max_drop_20'] = (df['fwd_min_low_20'] - df['c']) / df['c']
        
        indexer_5 = pd.api.indexers.FixedForwardWindowIndexer(window_size=5)
        df['fwd_ret_5'] = df['c'].rolling(window=indexer_5).apply(lambda x: x[-1]/x[0]-1 if len(x)>0 else np.nan, raw=True)

        # Metrics for correlation
        df['rsi14'] = compute_rsi(df['c'])
        df['dist_ma60'] = (df['c'] / df['ma60']) - 1
        df['dist_ma20'] = (df['c'] / df['ma20']) - 1
        df['vol_ratio'] = df['v'] / df['v'].rolling(20).mean()
        
        # Filter target population
        target = df[df['streak_count'] >= 60].copy()
        
        if target.empty:
            continue
            
        # We want to identify "High probability of drop"
        # "Drop" defined as fwd_max_drop_20 < -0.10 (10% drop in next 20 days)
        # Or just use the raw drop value
        
        subset = target[['streak_count', 'rsi14', 'dist_ma60', 'dist_ma20', 'vol_ratio', 'fwd_max_drop_20', 'fwd_ret_5']].dropna()
        if not subset.empty:
            subset['code'] = code
            results.append(subset)

    if not results:
        print("No data found matching criteria.")
        return

    all_data = pd.concat(results)
    
    print(f"Total data points (days with streak >= 60): {len(all_data)}")
    
    with open('report.txt', 'w', encoding='utf-8') as f:
        f.write(f"Total data points (days with streak >= 60): {len(all_data)}\n\n")
        
        correlations = all_data.corr(numeric_only=True)['fwd_max_drop_20'].sort_values()
        f.write("Correlations with Max Drop (20 days) (Negative correlation = Higher metric -> Larger drop):\n")
        f.write(correlations.to_string())
        f.write("\n\n")
        
        f.write("Avg Max Drop by RSI 14 Buckets:\n")
        all_data['rsi_bucket'] = pd.qcut(all_data['rsi14'], 5, duplicates='drop')
        f.write(all_data.groupby('rsi_bucket', observed=True)['fwd_max_drop_20'].mean().to_string())
        f.write("\n\n")

        f.write("Avg Max Drop by Dist MA60 Buckets:\n")
        all_data['dist_bucket'] = pd.qcut(all_data['dist_ma60'], 5, duplicates='drop')
        f.write(all_data.groupby('dist_bucket', observed=True)['fwd_max_drop_20'].mean().to_string())
        f.write("\n\n")

        f.write("Avg Max Drop by Streak Count Buckets:\n")
        all_data['streak_bucket'] = pd.qcut(all_data['streak_count'], 5, duplicates='drop')
        f.write(all_data.groupby('streak_bucket', observed=True)['fwd_max_drop_20'].mean().to_string())
        f.write("\n\n")
        
        f.write("Avg Max Drop by Volume Ratio Buckets:\n")
        all_data['vol_bucket'] = pd.qcut(all_data['vol_ratio'], 5, duplicates='drop')
        f.write(all_data.groupby('vol_bucket', observed=True)['fwd_max_drop_20'].mean().to_string())
        f.write("\n")
    print("Report written to report.txt")

if __name__ == "__main__":
    run_analysis()
