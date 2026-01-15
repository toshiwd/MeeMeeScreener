"""
イベントデータ更新のテストスクリプト
"""

import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

from events import fetch_earnings_snapshot, fetch_rights_snapshot

print("=" * 80)
print("イベントデータ更新テスト")
print("=" * 80)
print()

# Test earnings fetch
print("【決算予定データの取得】")
try:
    earnings_rows = fetch_earnings_snapshot()
    print(f"✓ 成功: {len(earnings_rows)} 件の決算予定を取得")
    
    if earnings_rows:
        # Show sample
        print("\nサンプル (最大5件):")
        for row in earnings_rows[:5]:
            code = row.get('code')
            date = row.get('planned_date')
            kind = row.get('kind', '')
            company = row.get('company_name', '')
            print(f"  {code} | {date} | {kind} | {company}")
except Exception as e:
    print(f"✗ 失敗: {e}")
    import traceback
    traceback.print_exc()

print()

# Test rights fetch
print("【権利落ちデータの取得】")
try:
    rights_rows = fetch_rights_snapshot()
    print(f"✓ 成功: {len(rights_rows)} 件の権利落ちデータを取得")
    
    if rights_rows:
        # Show sample
        print("\nサンプル (最大5件):")
        for row in rights_rows[:5]:
            code = row.get('code')
            ex_date = row.get('ex_date')
            category = row.get('category', '')
            print(f"  {code} | {ex_date} | {category}")
except Exception as e:
    print(f"✗ 失敗: {e}")
    import traceback
    traceback.print_exc()

print()
print("=" * 80)
print("テスト完了")
print("=" * 80)
