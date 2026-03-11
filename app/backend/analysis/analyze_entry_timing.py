
import duckdb
import pandas as pd
import numpy as np


DB_PATH = "C:/Users/enish/AppData/Local/MeeMeeScreener/data/stocks.duckdb"

def run_analysis():
    print(f"Connecting to DB at {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=True)
    codes = [r[0] for r in con.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
    
    results = []

    print(f"Analyzing {len(codes)} codes for Next Day Open Entry...")
    
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

        # Features
        df['ma60'] = df['c'].rolling(60).mean()
        df['vol_avg20'] = df['v'].rolling(20).mean()
        df['dist_ma60'] = (df['c'] / df['ma60']) - 1
        
        # Regime: Price > MA60 for 60 days
        df['above60'] = df['c'] > df['ma60']
        streak_group = (df['above60'] != df['above60'].shift()).cumsum()
        streak_count = df.groupby(streak_group).cumcount() + 1
        df['streak'] = np.where(df['above60'], streak_count, 0)
        
        # Patterns
        df['Sig_SuperClimax'] = (df['streak'] >= 60) & (df['dist_ma60'] > 0.25) & (df['v'] > 2 * df['vol_avg20'])
        df['Sig_Climax'] = (df['streak'] >= 60) & (df['dist_ma60'] > 0.15) & (df['v'] > 2 * df['vol_avg20'])

        # Next Day Entry Logic
        # Entry Price = Next Day Open (shift -1)
        df['next_open'] = df['o'].shift(-1)
        
        # Forward Min Low (Next 20 days starting from Next Day)
        # We want the min low of [T+1, T+20]
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=20)
        df['fwd_min_20'] = df['l'].shift(-1).rolling(window=indexer).min()
        
        # Max Drop from Entry
        df['drop_from_open'] = (df['fwd_min_20'] - df['next_open']) / df['next_open']
        
        # Gap Risk: (Next Open - Today Close) / Today Close
        df['gap_pct'] = (df['next_open'] - df['c']) / df['c']
        
        # Collect
        if df['Sig_SuperClimax'].any():
            subset = df[df['Sig_SuperClimax']].copy()
            subset['Pattern'] = 'Super Climax (>25%)'
            results.append(subset[['drop_from_open', 'gap_pct', 'Pattern']])
            
        if df['Sig_Climax'].any():
            # Exclude Super Climax from Climax to see "Just Overheated" vs "Super"? 
            # Or just analyze as defined. Let's stick to definition.
            subset = df[df['Sig_Climax']].copy()
            subset['Pattern'] = 'Climax (>15%)'
            results.append(subset[['drop_from_open', 'gap_pct', 'Pattern']])

    if not results:
        print("No signals found.")
        return

    all_res = pd.concat(results)
    
    print("\nNext Day Open Entry Analysis")
    print("============================")
    
    for pat in ['Super Climax (>25%)', 'Climax (>15%)']:
        sub = all_res[all_res['Pattern'] == pat]
        n = len(sub)
        avg_drop = sub['drop_from_open'].mean()
        win_3 = (sub['drop_from_open'] < -0.03).mean()
        win_5 = (sub['drop_from_open'] < -0.05).mean()
        win_10 = (sub['drop_from_open'] < -0.10).mean()
        avg_gap = sub['gap_pct'].mean()
        
        print(f"\nPattern: {pat}")
        print(f"Sample Size: {n}")
        print(f"Avg Gap (Overnight): {avg_gap:.2%}")
        print(f"Avg Max Drop (from Open): {avg_drop:.2%}")
        print(f"Win Rate (>3% profit): {win_3:.1%}")
        print(f"Win Rate (>5% profit): {win_5:.1%}")
        print(f"Win Rate (>10% profit): {win_10:.1%}")

    with open('entry_timing_report.txt', 'w', encoding='utf-8') as f:
         f.write("Entry Timing Analysis (Next Day Open)\n")
         f.write("=====================================\n\n")
         for pat in ['Super Climax (>25%)', 'Climax (>15%)']:
            sub = all_res[all_res['Pattern'] == pat]
            n = len(sub)
            avg_drop = sub['drop_from_open'].mean()
            win_3 = (sub['drop_from_open'] < -0.03).mean()
            win_5 = (sub['drop_from_open'] < -0.05).mean()
            
            f.write(f"Pattern: {pat}\n")
            f.write(f"Avg Max Drop (from Open): {avg_drop:.2%}\n")
            f.write(f"Win Rate (>3%): {win_3:.1%}\n")
            f.write(f"Win Rate (>5%): {win_5:.1%}\n\n")

if __name__ == "__main__":
    run_analysis()
