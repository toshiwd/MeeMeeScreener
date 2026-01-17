"""
Direct API test by calling the endpoint function
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app', 'backend'))

from main import positions_current
from db import get_conn

# Simulate the API call
print("=== Simulating /api/positions/current ===\n")

response = positions_current()
content = response.body.decode('utf-8')

import json
data = json.loads(content)

print(f"holding_codes: {len(data.get('holding_codes', []))}")
print(f"Sample holding_codes: {data.get('holding_codes', [])[:10]}")
print(f"\nall_traded_codes: {len(data.get('all_traded_codes', []))}")
print(f"\ncurrent_positions_by_code: {len(data.get('current_positions_by_code', {}))}")

# Show a sample position
positions = data.get('current_positions_by_code', {})
if positions:
    sample_code = list(positions.keys())[0]
    print(f"\nSample position for {sample_code}:")
    print(json.dumps(positions[sample_code], indent=2))
