
import os
import sys
import duckdb
import pandas as pd
import numpy as np

try:
    from app.core.config import config
    DB_PATH = str(config.DB_PATH)
except ImportError:
    try:
        from app.backend.config import config
        DB_PATH = str(config.DB_PATH)
    except Exception:
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
    codes = [r[0] for r in con.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
    
    signals_stats = {
        "MA20_Breakdown": [],
        "MA60_Breakdown": [],
        "Overheated_Reversal": [],
        "RSI_Div_Breakdown": [],
        "Death_Cross": [],
        "High_Vol_Reversal": []
    }

    print(f"Analyzing {len(codes)} codes...")
    
    for code in codes:
        df = con.execute(f"SELECT date, c, o, h, l, v FROM daily_bars WHERE code = '{code}' ORDER BY date").df()
        if len(df) < 100:
            continue

        # Date handling
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

        # Indicators
        df['ma5'] = df['c'].rolling(5).mean()
        df['ma20'] = df['c'].rolling(20).mean()
        df['ma60'] = df['c'].rolling(60).mean()
        df['rsi'] = compute_rsi(df['c'])
        df['vol_avg'] = df['v'].rolling(20).mean()
        df['dist_ma60'] = (df['c'] / df['ma60']) - 1
        
        # Forward Returns (20 days)
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=20)
        df['fwd_min'] = df['l'].rolling(window=indexer).min()
        df['max_drop'] = (df['fwd_min'] - df['c']) / df['c'] # Negative value is drop
        
        # Signals
        # 1. MA20 Breakdown: Yesterday Close > MA20, Today Close < MA20
        # Filter: Must be in uptrend (MA20 > MA60) or overheated? Let's just catch all.
        sig_ma20 = (df['c'].shift(1) > df['ma20'].shift(1)) & (df['c'] < df['ma20'])
        
        # 2. MA60 Breakdown
        sig_ma60 = (df['c'].shift(1) > df['ma60'].shift(1)) & (df['c'] < df['ma60'])
        
        # 3. Overheated Reversal: DistMA60 > 15%, Close < Open, Drop > 2%
        sig_hot = (df['dist_ma60'] > 0.15) & (df['c'] < df['o']) & (df['c'].pct_change() < -0.02)
        
        # 4. Death Cross: MA20 crosses below MA60
        sig_dc = (df['ma20'].shift(1) > df['ma60'].shift(1)) & (df['ma20'] < df['ma60'])
        
        # 5. High Vol Reversal: New 60d High, Close < Open, Vol > 2x Avg
        high60 = df['h'].rolling(60).max()
        sig_vol = (df['h'] >= high60) & (df['c'] < df['o']) & (df['v'] > 2 * df['vol_avg'])

        # Collect
        def collect(mask, name):
            drops = df.loc[mask, 'max_drop'].dropna()
            if not drops.empty:
                signals_stats[name].extend(drops.tolist())

        collect(sig_ma20, "MA20_Breakdown")
        collect(sig_ma60, "MA60_Breakdown")
        collect(sig_hot, "Overheated_Reversal")
        collect(sig_dc, "Death_Cross")
        collect(sig_vol, "High_Vol_Reversal")

    print("\nAnalysis Complete. Generating Report...")
    
    with open('short_pattern_report.txt', 'w', encoding='utf-8') as f:
        f.write(f"Short Pattern Analysis (Target: >10% Drop in 20 days)\n")
        f.write(f"====================================================\n\n")
        
        # DataFrame for ranking
        rows = []
        for name, drops in signals_stats.items():
            if not drops:
                continue
            arr = np.array(drops)
            count = len(arr)
            avg_drop = np.mean(arr)
            
            # Win Rate: Drop < -0.10
            wins = np.sum(arr < -0.10)
            win_rate = wins / count
            
            # Median Drop
            median_drop = np.median(arr)
            
            rows.append({
                "Pattern": name,
                "Count": count,
                "Avg Drop": avg_drop,
                "Median Drop": median_drop,
                "Win Rate (>10% Drop)": win_rate
            })
            
        res_df = pd.DataFrame(rows).sort_values("Win Rate (>10% Drop)", ascending=False)
        
        f.write(res_df.to_string(formatters={
            'Avg Drop': '{:.2%}'.format,
            'Median Drop': '{:.2%}'.format,
            'Win Rate (>10% Drop)': '{:.2%}'.format
        }))
        f.write("\n\n")
        
        f.write("Details:\n")
        f.write("- MA20_Breakdown: Close crosses below 20-day MA.\n")
        f.write("- MA60_Breakdown: Close crosses below 60-day MA.\n")
        f.write("- Overheated_Reversal: Price >15% above 60MA + Daily Drop > 2%.\n")
        f.write("- Death_Cross: 20MA crosses below 60MA.\n")
        f.write("- High_Vol_Reversal: New 60-day High + Bearish Candle + Vol > 2x Avg.\n")

    print(res_df)
    print("Saved to short_pattern_report.txt")

if __name__ == "__main__":
    run_analysis()
