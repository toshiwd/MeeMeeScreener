"""
イベントメタデータを初期化するスクリプト
"""

import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

from db import get_conn

print("イベントメタデータを初期化中...")

try:
    with get_conn() as conn:
        # Check if row exists
        existing = conn.execute("SELECT COUNT(*) FROM events_meta WHERE id = 1").fetchone()
        count = existing[0] if existing else 0
        
        if count == 0:
            # Insert initial row
            conn.execute(
                """
                INSERT INTO events_meta (
                    id,
                    earnings_last_success_at,
                    rights_last_success_at,
                    last_error,
                    last_attempt_at,
                    is_refreshing,
                    refresh_lock_job_id,
                    refresh_lock_started_at
                ) VALUES (1, NULL, NULL, NULL, NULL, FALSE, NULL, NULL)
                """
            )
            print("✓ イベントメタデータを初期化しました")
        else:
            print("✓ イベントメタデータは既に存在します")
            
except Exception as e:
    print(f"✗ エラー: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
