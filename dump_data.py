import duckdb, os, pandas as pd
from pathlib import Path
import datetime

db = r'C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb'
con = duckdb.connect(db)

# We want from 2012+ or 2015+
min_ts = pd.Timestamp("2010-01-01").timestamp()

df_daily = con.execute(f"SELECT * FROM daily_bars WHERE date >= {min_ts}").df()
df_daily['date'] = pd.to_datetime(df_daily['date'], unit='s').dt.strftime('%Y-%m-%d')
df_daily.to_csv('dummy_daily.csv', index=False)
print('Exported dummy_daily.csv', len(df_daily))

# Export universe
dates = pd.to_datetime(df_daily['date'])
month_ends = dates.groupby(dates.dt.to_period('M')).max().dt.normalize().unique()

uni_dir = Path('dummy_universe')
uni_dir.mkdir(exist_ok=True)

codes = df_daily['code'].unique()
for me in month_ends:
    pd.DataFrame({'code': codes, 'asof_date': str(pd.Timestamp(me).date())}).to_csv(uni_dir / f"{pd.Timestamp(me).strftime('%Y%m')}.csv", index=False)
print('Exported universe CSVs')
