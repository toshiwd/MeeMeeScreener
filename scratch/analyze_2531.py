import duckdb
import pandas as pd
import json
import urllib.request
from datetime import datetime, timezone, timedelta

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_rows', 100)
pd.set_option('display.float_format', '{:.4f}'.format)

JST = timezone(timedelta(hours=9))

def fetch_yahoo(symbol, period='6mo'):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range={period}&interval=1d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    result = payload["chart"]["result"][0]
    timestamps = result["timestamp"]
    quote = result["indicators"]["quote"][0]
    rows = []
    for ts, o, h, l, c, v in zip(timestamps, quote["open"], quote["high"], quote["low"], quote["close"], quote["volume"]):
        if c is None:
            continue
        d = datetime.fromtimestamp(int(ts), tz=JST).date()
        rows.append({"date": d, "open": o, "high": h, "low": l, "close": c, "volume": v})
    return pd.DataFrame(rows)

print("=== Fetching 2531.T from Yahoo ===")
df = fetch_yahoo("2531.T", "1y")
df = df.sort_values("date").reset_index(drop=True)
print(f"Records: {len(df)}, Date range: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")

# Compute technical indicators
df['ma7'] = df['close'].rolling(7).mean()
df['ma20'] = df['close'].rolling(20).mean()
df['ma60'] = df['close'].rolling(60).mean()
df['ma100'] = df['close'].rolling(100).mean()
df['ma200'] = df['close'].rolling(200).mean()
df['vol_ma20'] = df['volume'].rolling(20).mean()
df['vol_ratio'] = df['volume'] / df['vol_ma20']

# RSI
delta = df['close'].diff()
gain = delta.where(delta > 0, 0).rolling(14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
df['rsi14'] = 100 - (100 / (1 + gain / loss))

# Daily change
df['day_change'] = df['close'].pct_change()

# Distance from MAs
for ma in ['ma7', 'ma20', 'ma60', 'ma100', 'ma200']:
    df[f'dist_{ma}'] = (df['close'] / df[ma] - 1.0)

# ATR
tr1 = df['high'] - df['low']
tr2 = (df['high'] - df['close'].shift(1)).abs()
tr3 = (df['low'] - df['close'].shift(1)).abs()
df['atr14'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(14).mean()
df['atr_pct'] = df['atr14'] / df['close']

# Latest N days
print("\n=== Last 10 trading days ===")
last10 = df.tail(10)[['date', 'open', 'high', 'low', 'close', 'volume', 'day_change', 'ma7', 'ma20', 'ma60', 'rsi14']].copy()
print(last10.to_string(index=False))

latest = df.iloc[-1]
print(f"\n=== Latest: {latest['date']} ===")
print(f"Close: {latest['close']:.0f}")
print(f"MA7: {latest['ma7']:.1f} (dist: {latest['dist_ma7']:.2%})")
print(f"MA20: {latest['ma20']:.1f} (dist: {latest['dist_ma20']:.2%})")
print(f"MA60: {latest['ma60']:.1f} (dist: {latest['dist_ma60']:.2%})")
print(f"MA100: {latest['ma100']:.1f} (dist: {latest['dist_ma100']:.2%})")
if not pd.isna(latest['ma200']):
    print(f"MA200: {latest['ma200']:.1f} (dist: {latest['dist_ma200']:.2%})")
print(f"RSI14: {latest['rsi14']:.1f}")
print(f"ATR14: {latest['atr14']:.1f} ({latest['atr_pct']:.2%})")
print(f"Vol ratio: {latest['vol_ratio']:.2f}")

# Trend check
print(f"\n=== Trend Summary ===")
ma_order = []
for ma_name in ['ma7', 'ma20', 'ma60', 'ma100']:
    if not pd.isna(latest[ma_name]):
        ma_order.append((ma_name, latest[ma_name]))
print("MA order (short→long):", " > ".join([f"{n}={v:.0f}" for n, v in ma_order]))
sorted_by_val = sorted(ma_order, key=lambda x: -x[1])
is_perfect_order = [x[0] for x in sorted_by_val] == [x[0] for x in ma_order]
print(f"Perfect bullish order (ma7 > ma20 > ma60 > ma100): {is_perfect_order}")

# Check if above all MAs
above_all = all(latest['close'] > latest[ma] for ma in ['ma7', 'ma20', 'ma60', 'ma100'] if not pd.isna(latest[ma]))
print(f"Price above all MAs: {above_all}")

# Recent performance
print(f"\n=== Recent Returns ===")
for period, label in [(5, '1w'), (10, '2w'), (20, '1m'), (60, '3m')]:
    if len(df) > period:
        ret = df['close'].iloc[-1] / df['close'].iloc[-period-1] - 1
        print(f"  {label}: {ret:.2%}")

# High/low
hi52 = df['high'].tail(240).max() if len(df) >= 240 else df['high'].max()
lo52 = df['low'].tail(240).min() if len(df) >= 240 else df['low'].min()
print(f"\n52w High: {hi52:.0f}")
print(f"52w Low: {lo52:.0f}")
print(f"Position in range: {(latest['close'] - lo52) / (hi52 - lo52):.1%}")

# Check forecast data from result.duckdb
print("\n=== TRADEX Forecast Surface for 2531 (latest) ===")
con = duckdb.connect('G:/Tradex/db/result.duckdb', read_only=True)
forecast = con.execute("""
    SELECT as_of_date, side, action_state, direction_prob, 
           expected_ret_5, expected_ret_10, expected_ret_20,
           expected_mfe_20, expected_mae_20,
           market_opportunity_score, opportunity_score,
           setup_tags, reason_codes
    FROM forecast_surface_daily 
    WHERE code='2531' 
    ORDER BY as_of_date DESC 
    LIMIT 10
""").df()
print(forecast.to_string())

# Regime data
print("\n=== Market Regime (latest) ===")
regime = con.execute("SELECT * FROM regime_daily ORDER BY as_of_date DESC LIMIT 5").df()
print(regime.to_string())

# Recent label data from label.duckdb - how 2531 has performed at similar setups
print("\n=== Historical Label Data (h20) for 2531 - Recent ===")
con_label = duckdb.connect('G:/Tradex/db/label.duckdb', read_only=True)
labels = con_label.execute("""
    SELECT as_of_date, ret_h, mfe_h, mae_h, days_to_mfe_h, rank_ret_h, 
           top_1pct_h, top_3pct_h, top_5pct_h, cross_section_count
    FROM label_daily_h20 
    WHERE code='2531' AND as_of_date >= 20250101
    ORDER BY as_of_date DESC
    LIMIT 20
""").df()
print(labels.to_string())

# Stats summary
print("\n=== Historical Label Stats (h20, last 2 years) ===")
stats = con_label.execute("""
    SELECT 
        COUNT(*) as n,
        AVG(ret_h) as avg_ret,
        MEDIAN(ret_h) as med_ret,
        STDDEV(ret_h) as std_ret,
        AVG(mfe_h) as avg_mfe,
        AVG(mae_h) as avg_mae,
        SUM(CASE WHEN top_5pct_h THEN 1 ELSE 0 END) as top5_count,
        SUM(CASE WHEN ret_h > 0 THEN 1 ELSE 0 END) as pos_count
    FROM label_daily_h20 
    WHERE code='2531' AND as_of_date >= 20240101
""").df()
print(stats.to_string())

con.close()
con_label.close()
