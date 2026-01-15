"""
データベーススキーマ初期化スクリプト

不足しているテーブルを作成します。
"""

import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

from db import init_schema

print("データベーススキーマを初期化しています...")
try:
    init_schema()
    print("✓ スキーマ初期化完了")
    print()
    print("以下のテーブルが作成されました:")
    print("  - trade_events (取引イベント)")
    print("  - positions_live (保有銘柄)")
    print("  - position_rounds (取引履歴)")
    print("  - initial_positions_seed (初期ポジション)")
    print("  - earnings_planned (決算予定)")
    print("  - ex_rights (権利落ち)")
    print("  - events_meta (イベントメタデータ)")
    print("  - events_refresh_jobs (イベント更新ジョブ)")
except Exception as e:
    print(f"✗ エラー: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
