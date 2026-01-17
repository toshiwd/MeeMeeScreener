"""
診断スクリプト - 保有銘柄と決算・権利落ちデータの問題を調査

このスクリプトは以下を確認します:
1. 取引履歴データの有無
2. 保有銘柄データの有無  
3. イベントデータ(決算・権利落ち)の状態
4. 必要なライブラリのインストール状況
"""

import sys
import os
from datetime import datetime

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

print("=" * 80)
print("MeeMee Screener 診断レポート")
print("=" * 80)
print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Check required libraries
print("【1】必要なライブラリのチェック")
print("-" * 80)
libraries = {
    "pandas": "Excel解析に必要",
    "openpyxl": "Excel(.xlsx)ファイルの読み込みに必要",
    "jpholiday": "営業日判定に必要",
    "pyarrow": "類似検索に必要",
    "duckdb": "データベースアクセスに必要"
}

missing_libs = []
for lib, purpose in libraries.items():
    try:
        __import__(lib)
        print(f"✓ {lib:15s} - インストール済み ({purpose})")
    except ImportError:
        print(f"✗ {lib:15s} - 未インストール ({purpose})")
        missing_libs.append(lib)

if missing_libs:
    print(f"\n⚠ 不足しているライブラリ: {', '.join(missing_libs)}")
    print(f"インストールコマンド: pip install {' '.join(missing_libs)}")
else:
    print("\n✓ すべての必要なライブラリがインストールされています")

print()

# Check database
print("【2】データベースのチェック")
print("-" * 80)

# Try to find database
db_path = os.path.join(os.getcwd(), "app", "backend", "stocks.duckdb")
if not os.path.exists(db_path):
    # Try alternative location
    db_path = os.path.join(os.getcwd(), "data_store", "stocks.duckdb")
    
if not os.path.exists(db_path):
    print(f"✗ データベースが見つかりません")
    print(f"  探した場所:")
    print(f"    - {os.path.join(os.getcwd(), 'app', 'backend', 'stocks.duckdb')}")
    print(f"    - {os.path.join(os.getcwd(), 'data_store', 'stocks.duckdb')}")
    print(f"\n  データベースが初期化されていない可能性があります")
    print(f"  バックエンドサーバーを一度起動してください")
    sys.exit(1)

print(f"✓ データベース: {db_path}")
print(f"  サイズ: {os.path.getsize(db_path) / (1024*1024):.2f} MB")

try:
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
except Exception as e:
    print(f"✗ データベース接続エラー: {e}")
    sys.exit(1)

# Check trade events
print()
print("【3】取引履歴データのチェック")
print("-" * 80)

trade_count_result = conn.execute("SELECT COUNT(*) as count FROM trade_events").fetchone()
trade_count = trade_count_result[0] if trade_count_result else 0
print(f"取引イベント数: {trade_count}")

if trade_count > 0:
    # Show sample
    sample = conn.execute("""
        SELECT broker, exec_dt, symbol, action, qty 
        FROM trade_events 
        ORDER BY exec_dt DESC 
        LIMIT 5
    """).fetchall()
    print("\n最新の取引イベント (最大5件):")
    for row in sample:
        print(f"  {row[1]} | {row[2]} | {row[3]} | {row[4]} 株 | {row[0]}")
    
    # Check unique symbols
    symbols = conn.execute("SELECT DISTINCT symbol FROM trade_events ORDER BY symbol").fetchall()
    symbol_list = [row[0] for row in symbols]
    print(f"\n取引のある銘柄数: {len(symbol_list)}")
    print(f"銘柄: {', '.join(symbol_list)}")
else:
    print("⚠ 取引履歴データがありません")
    print("  → 取引履歴CSVをインポートしてください")

# Check positions_live
print()
print("【4】保有銘柄データのチェック")
print("-" * 80)

positions = conn.execute("""
    SELECT p.symbol, p.buy_qty, p.sell_qty, p.opened_at, p.has_issue, p.issue_note, t.name
    FROM positions_live p
    LEFT JOIN tickers t ON p.symbol = t.code
    ORDER BY p.symbol
""").fetchall()

print(f"保有銘柄数: {len(positions)}")

if len(positions) > 0:
    print("\n保有銘柄一覧:")
    for row in positions:
        symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note, name = row
        sell = float(sell_qty or 0)
        buy = float(buy_qty or 0)
        name = name or symbol
        opened = opened_at or '不明'
        issue = " ⚠要確認" if has_issue else ""
        print(f"  {symbol} {name:20s} | {sell:g}-{buy:g} | 開始: {opened}{issue}")
        if issue_note:
            print(f"    問題: {issue_note}")
else:
    print("⚠ 保有銘柄データがありません")
    if trade_count > 0:
        print("  → rebuild_positions が実行されていない可能性があります")
    else:
        print("  → 取引履歴をインポートしてください")

# Check position rounds (history)
print()
print("【5】取引履歴(ラウンド)のチェック")
print("-" * 80)

rounds = conn.execute("""
    SELECT COUNT(*) as count, 
           MIN(opened_at) as first_open,
           MAX(closed_at) as last_close
    FROM position_rounds
""").fetchone()

if rounds:
    count, first_open, last_close = rounds
    print(f"完了したラウンド数: {count}")
    if count > 0:
        print(f"最初の取引開始: {first_open}")
        print(f"最後の取引終了: {last_close}")
else:
    print(f"完了したラウンド数: 0")

# Check events meta
print()
print("【6】イベントデータ(決算・権利落ち)のチェック")
print("-" * 80)

meta = conn.execute("""
    SELECT 
        earnings_last_success_at,
        rights_last_success_at,
        last_error,
        last_attempt_at,
        is_refreshing
    FROM events_meta
    LIMIT 1
""").fetchone()

if meta:
    earnings_last, rights_last, last_error, last_attempt, is_refreshing = meta
    print(f"決算データ最終更新: {earnings_last or '未更新'}")
    print(f"権利落ちデータ最終更新: {rights_last or '未更新'}")
    print(f"更新中: {'はい' if is_refreshing else 'いいえ'}")
    print(f"最終試行: {last_attempt or '未実行'}")
    
    if last_error:
        print(f"\n⚠ 最後のエラー:")
        print(f"  {last_error}")
        
        # Parse error to give helpful advice
        error = last_error
        if 'pandas_not_installed' in error or 'ModuleNotFoundError' in error:
            print("\n💡 解決策: 必要なライブラリをインストールしてください")
            print("  pip install pandas openpyxl jpholiday")
        elif 'earnings_excel_urls_not_found' in error or 'rights_excel_urls_not_found' in error:
            print("\n💡 解決策: JPXのウェブサイトからExcelファイルのURLを取得できませんでした")
            print("  ネットワーク接続を確認するか、手動でURLを環境変数に設定してください")
        elif 'URLError' in error or 'timeout' in error:
            print("\n💡 解決策: ネットワーク接続を確認してください")
else:
    print("⚠ イベントメタデータが見つかりません")

# Check earnings data
earnings_count_result = conn.execute("SELECT COUNT(*) FROM earnings_planned").fetchone()
earnings_count = earnings_count_result[0] if earnings_count_result else 0
print(f"\n決算予定データ件数: {earnings_count}")

if earnings_count > 0:
    # Show upcoming earnings
    upcoming = conn.execute("""
        SELECT e.code, e.planned_date, e.company_name, t.name
        FROM earnings_planned e
        LEFT JOIN tickers t ON e.code = t.code
        WHERE e.planned_date >= current_date
        ORDER BY e.planned_date
        LIMIT 5
    """).fetchall()
    
    if upcoming:
        print("\n直近の決算予定 (最大5件):")
        for row in upcoming:
            code, planned_date, company_name, ticker_name = row
            name = ticker_name or company_name or code
            print(f"  {planned_date} | {code} {name}")

# Check rights data
rights_count_result = conn.execute("SELECT COUNT(*) FROM ex_rights").fetchone()
rights_count = rights_count_result[0] if rights_count_result else 0
print(f"\n権利落ちデータ件数: {rights_count}")

if rights_count > 0:
    # Show upcoming ex-rights
    upcoming = conn.execute("""
        SELECT e.code, e.ex_date, e.last_rights_date, e.category, t.name
        FROM ex_rights e
        LEFT JOIN tickers t ON e.code = t.code
        WHERE e.ex_date >= current_date
        ORDER BY e.ex_date
        LIMIT 5
    """).fetchall()
    
    if upcoming:
        print("\n直近の権利落ち予定 (最大5件):")
        for row in upcoming:
            code, ex_date, last_rights_date, category, ticker_name = row
            name = ticker_name or code
            category = category or ''
            print(f"  {ex_date} | {code} {name} | {category}")

conn.close()

print()
print("=" * 80)
print("診断完了")
print("=" * 80)

# Summary and recommendations
print()
print("【推奨アクション】")
print("-" * 80)

if missing_libs:
    print(f"1. 不足しているライブラリをインストール:")
    print(f"   pip install {' '.join(missing_libs)}")

if trade_count == 0:
    print("2. 取引履歴CSVをインポート:")
    print("   - フロントエンドの「保有/履歴」画面から楽天またはSBIのCSVをアップロード")

if len(positions) == 0 and trade_count > 0:
    print("3. ポジション再構築が必要な可能性があります")
    print("   - 取引履歴を再インポートしてみてください")

if earnings_count == 0 or rights_count == 0:
    print("4. イベントデータを更新:")
    print("   - フロントエンドから「イベント更新」を実行")
    print("   - または API: POST /api/events/refresh")

if not missing_libs and trade_count > 0 and len(positions) > 0 and earnings_count > 0:
    print("✓ すべて正常に動作しています!")

print()
