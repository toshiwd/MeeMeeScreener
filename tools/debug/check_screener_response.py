"""
スクリーナーAPIのレスポンスを確認するスクリプト
"""
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'backend'))

from main import _build_screener_rows

def main():
    print("=== Checking Screener API Response ===\n")
    
    rows = _build_screener_rows()
    
    # Find 1928
    sekisui = None
    for row in rows:
        if row.get('code') == '1928':
            sekisui = row
            break
    
    if sekisui:
        print("Found 1928 (Sekisui House):")
        print(f"  Code: {sekisui.get('code')}")
        print(f"  Name: {sekisui.get('name')}")
        print(f"  eventEarningsDate: {sekisui.get('eventEarningsDate')}")
        print(f"  eventRightsDate: {sekisui.get('eventRightsDate')}")
        print(f"  event_earnings_date: {sekisui.get('event_earnings_date')}")
        print(f"  event_rights_date: {sekisui.get('event_rights_date')}")
        print("\nFull data:")
        print(json.dumps(sekisui, indent=2, ensure_ascii=False, default=str))
    else:
        print("1928 not found in screener results")
        print(f"\nTotal rows: {len(rows)}")
        if rows:
            print("\nSample row (first):")
            print(json.dumps(rows[0], indent=2, ensure_ascii=False, default=str))

if __name__ == "__main__":
    main()
