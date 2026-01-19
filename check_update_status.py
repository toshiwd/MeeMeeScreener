#!/usr/bin/env python3
"""Check the TXT update status to see if there were any errors"""

import requests
import json

try:
    response = requests.get("http://127.0.0.1:28888/api/txt_update/status")
    if response.status_code == 200:
        data = response.json()
        print("=" * 80)
        print("TXT Update Status")
        print("=" * 80)
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("=" * 80)
        
        if data.get("error"):
            print(f"\n❌ Error detected: {data['error']}")
        elif data.get("phase") == "done":
            print("\n✅ Last update completed successfully")
        else:
            print(f"\n⏳ Current phase: {data.get('phase')}")
            
        if data.get("stdout_tail"):
            print("\nLast output lines:")
            print("-" * 80)
            for line in data.get("stdout_tail", [])[-20:]:
                print(line)
    else:
        print(f"Failed to get status: HTTP {response.status_code}")
except Exception as e:
    print(f"Error: {e}")
    print("\nNote: Make sure the MeeMee Screener app is running!")
