
import os
import sys
import duckdb
import pandas as pd
import numpy as np
import itertools

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

def run_search():
    print(f"Connecting to DB at {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    codes = [r[0] for r in con.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]

    # We will collect 'events' - days where basics are met, with minimal filtering
    # Then we will process them in memory to check combinations.
    
    # Base criteria: Streak >= 60 (already established as the regime)
    
    all_rows = []
    
    print(f"Scanning {len(codes)} codes for base regime...")
    
    for code in codes:
        df = con.execute(f"SELECT date, c, o, h, l, v FROM daily_bars WHERE code = '{code}' ORDER BY date").df()
        if len(df) < 100:
            continue
            
        # Date fix
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

        # Features
        df['ma60'] = df['c'].rolling(60).mean()
        df['above60'] = df['c'] > df['ma60']
        
        # Streak
        streak_group = (df['above60'] != df['above60'].shift()).cumsum()
        streak_count = df.groupby(streak_group).cumcount() + 1
        df['streak'] = np.where(df['above60'], streak_count, 0)
        
        # Filter: Regime only
        regime_mask = df['streak'] >= 60
        if not regime_mask.any():
            continue
            
        target = df[regime_mask].copy()
        
        # Add rich features for target rows
        # We need to look back from these rows.
        # Actually easier to compute features for whole DF then filter.
        
        df['ma5'] = df['c'].rolling(5).mean()
        df['ma20'] = df['c'].rolling(20).mean()
        df['vol_avg20'] = df['v'].rolling(20).mean()
        df['rsi14'] = compute_rsi(df['c'])
        df['dist_ma60'] = (df['c'] / df['ma60']) - 1
        df['dist_ma20'] = (df['c'] / df['ma20']) - 1
        df['return_1d'] = df['c'].pct_change()
        
        # Forward targets
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=10) # 10 day horizon
        df['fwd_close_10'] = df['c'].rolling(window=indexer).apply(lambda x: x[-1] if len(x)>0 else np.nan, raw=True)
        df['fwd_ret_10'] = (df['fwd_close_10'] - df['c']) / df['c']
        df['fwd_min_10'] = df['l'].rolling(window=indexer).min()
        df['fwd_max_drop_10'] = (df['fwd_min_10'] - df['c']) / df['c']

        # Slice to regime
        subset = df.loc[regime_mask].copy()
        
        # Boolean Signals (The building blocks)
        subset['Sig_Overheated15'] = subset['dist_ma60'] > 0.15
        subset['Sig_Overheated25'] = subset['dist_ma60'] > 0.25
        subset['Sig_RSI_High70'] = subset['rsi14'] > 70
        subset['Sig_RSI_Extreme80'] = subset['rsi14'] > 80
        subset['Sig_Vol_High2'] = subset['v'] > 2 * subset['vol_avg20']
        subset['Sig_Vol_Low05'] = subset['v'] < 0.5 * subset['vol_avg20']
        subset['Sig_BearishCandle'] = subset['c'] < subset['o']
        subset['Sig_BigDrop2'] = subset['return_1d'] < -0.02
        subset['Sig_MA5_CrossDown'] = (subset['c'] < subset['ma5']) & (subset['c'].shift(1) > subset['ma5'].shift(1))
        
        # Keep minimal data for search
        cols = [c for c in subset.columns if c.startswith('Sig_')] + ['fwd_ret_10', 'fwd_max_drop_10']
        all_rows.append(subset[cols].dropna())

    if not all_rows:
        print("No data.")
        return

    full_df = pd.concat(all_rows)
    print(f"Total Regime Days: {len(full_df)}")
    
    # Combinatorial Search
    # We want to find a combination of signal columns (AND logic) that yields high win rate.
    # Win definitions:
    # A: Drops > 0% at any point in 10 days (fwd_max_drop_10 < 0) -> "Not getting stuck"
    # B: Drops > 3% at any point in 10 days (fwd_max_drop_10 < -0.03) -> "Scalp Profit"
    # C: Drops > 5% at any point (fwd_max_drop_10 < -0.05) -> "Decent Profit"
    
    signal_cols = [c for c in full_df.columns if c.startswith('Sig_')]
    
    results = []
    
    # 1. Single Signals
    # 2. Pairs
    # 3. Triplets
    
    combinations = []
    for r in range(1, 4):
        combinations.extend(itertools.combinations(signal_cols, r))
        
    print(f"Evaluating {len(combinations)} patterns...")
    
    for combo in combinations:
        # Create mask
        mask = np.ones(len(full_df), dtype=bool)
        for col in combo:
            mask = mask & full_df[col].values
            
        count = mask.sum()
        if count < 50: # Min sample size
            continue
            
        subset_targets = full_df.loc[mask]
        
        # Check Win Rates
        win_any = (subset_targets['fwd_max_drop_10'] < 0).mean()
        win_3pct = (subset_targets['fwd_max_drop_10'] < -0.03).mean()
        win_5pct = (subset_targets['fwd_max_drop_10'] < -0.05).mean()
        avg_drop = subset_targets['fwd_max_drop_10'].mean()
        
        if win_3pct > 0.60 or win_any > 0.85:
           results.append({
               "Pattern": " + ".join([c.replace('Sig_', '') for c in combo]),
               "N": count,
               "WinRate_AnyDrop": win_any,
               "WinRate_3pct": win_3pct,
               "WinRate_5pct": win_5pct,
               "AvgMaxDrop": avg_drop
           })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        print("No high probability patterns found.")
    else:
        results_df = results_df.sort_values("WinRate_5pct", ascending=False)
        print(f"Found {len(results_df)} promising patterns.")
        
        with open('high_win_patterns.txt', 'w', encoding='utf-8') as f:
            f.write("High Win Rate Patterns (10 Day Horizon)\n")
            f.write("WinRate_AnyDrop: Probability price drops below entry at least once.\n")
            f.write("WinRate_3pct: Probability price drops >3% at least once.\n\n")
            f.write(results_df.head(50).to_string(float_format="{:.1%}".format))
        
        print(results_df.head(10))

if __name__ == "__main__":
    run_search()
