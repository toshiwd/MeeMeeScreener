"""
既存のCSVファイルから取引履歴をデータベースにインポートするスクリプト

data/楽天証券取引履歴.csv と data/SBI証券取引履歴.csv を読み込み、
trade_events テーブルに保存し、positions_live を再構築します。
"""

import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

from positions import parse_rakuten_csv, parse_sbi_csv, rebuild_positions
from db import get_conn

print("=" * 80)
print("取引履歴インポートスクリプト")
print("=" * 80)
print()

# File paths
rakuten_path = os.path.join(os.getcwd(), "data", "楽天証券取引履歴.csv")
sbi_path = os.path.join(os.getcwd(), "data", "SBI証券取引履歴.csv")

total_events = []
all_warnings = []

# Import Rakuten CSV
if os.path.exists(rakuten_path):
    print(f"【楽天証券】{rakuten_path}")
    print(f"  ファイルサイズ: {os.path.getsize(rakuten_path) / 1024:.1f} KB")
    
    try:
        with open(rakuten_path, "rb") as f:
            data = f.read()
        
        events, warnings = parse_rakuten_csv(data)
        total_events.extend(events)
        all_warnings.extend(warnings)
        
        print(f"  ✓ パース成功: {len(events)} 件の取引イベント")
        if warnings:
            print(f"  ⚠ 警告: {len(warnings)} 件")
            for w in warnings[:3]:
                print(f"    - {w}")
            if len(warnings) > 3:
                print(f"    ... 他 {len(warnings) - 3} 件")
    except Exception as e:
        print(f"  ✗ エラー: {e}")
else:
    print(f"⚠ 楽天証券CSVが見つかりません: {rakuten_path}")

print()

# Import SBI CSV
if os.path.exists(sbi_path):
    print(f"【SBI証券】{sbi_path}")
    print(f"  ファイルサイズ: {os.path.getsize(sbi_path) / 1024:.1f} KB")
    
    try:
        with open(sbi_path, "rb") as f:
            data = f.read()
        
        events, warnings = parse_sbi_csv(data)
        total_events.extend(events)
        all_warnings.extend(warnings)
        
        print(f"  ✓ パース成功: {len(events)} 件の取引イベント")
        if warnings:
            print(f"  ⚠ 警告: {len(warnings)} 件")
            for w in warnings[:3]:
                print(f"    - {w}")
            if len(warnings) > 3:
                print(f"    ... 他 {len(warnings) - 3} 件")
    except Exception as e:
        print(f"  ✗ エラー: {e}")
else:
    print(f"⚠ SBI証券CSVが見つかりません: {sbi_path}")

print()
print("=" * 80)
print(f"合計: {len(total_events)} 件の取引イベント")
print("=" * 80)
print()

if not total_events:
    print("✗ インポートする取引イベントがありません")
    sys.exit(1)

# Import to database
print("データベースにインポート中...")

try:
    with get_conn() as conn:
        # Check existing events
        existing_hashes = set()
        existing_rows = conn.execute(
            "SELECT source_row_hash FROM trade_events"
        ).fetchall()
        existing_hashes = {row[0] for row in existing_rows}
        
        print(f"  既存の取引イベント: {len(existing_hashes)} 件")
        
        # Prepare rows to insert
        rows_to_insert = []
        skipped = 0
        
        for event in total_events:
            if event.source_row_hash in existing_hashes:
                skipped += 1
                continue
            
            rows_to_insert.append((
                event.broker,
                event.exec_dt,
                event.symbol,
                event.action,
                event.qty,
                event.price,
                event.source_row_hash
            ))
        
        print(f"  新規インポート: {len(rows_to_insert)} 件")
        print(f"  スキップ (重複): {skipped} 件")
        
        if rows_to_insert:
            # Insert new events (ignore duplicates)
            conn.executemany(
                """
                INSERT INTO trade_events (
                    broker,
                    exec_dt,
                    symbol,
                    action,
                    qty,
                    price,
                    source_row_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (source_row_hash) DO NOTHING
                """,
                rows_to_insert
            )
            
            # Check how many were actually inserted
            final_count_result = conn.execute(
                "SELECT COUNT(*) FROM trade_events"
            ).fetchone()
            final_count = final_count_result[0] if final_count_result else 0
            actually_inserted = final_count - len(existing_hashes)
            
            print(f"  ✓ データベースに保存しました ({actually_inserted} 件)")
        
        # Rebuild positions
        print()
        print("保有銘柄を再構築中...")
        rebuild_summary = rebuild_positions(conn)
        
        print(f"  ✓ 再構築完了")
        print(f"    - 保有銘柄: {rebuild_summary['positions']} 件")
        print(f"    - 完了ラウンド: {rebuild_summary['rounds']} 件")
        if rebuild_summary['issues'] > 0:
            print(f"    - ⚠ 問題あり: {rebuild_summary['issues']} 件")
        
        print()
        print("=" * 80)
        print("✓ インポート完了!")
        print("=" * 80)
        print()
        print("「保有/履歴」画面をリロードして確認してください。")
        
except Exception as e:
    print(f"✗ エラー: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
