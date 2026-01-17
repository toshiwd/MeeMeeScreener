import requests
import json

def test_api():
    base_url = "http://localhost:8765"
    
    print("=== Testing Backend API ===\n")
    
    # Test 1: /api/positions/current
    print("1. Testing /api/positions/current")
    try:
        response = requests.get(f"{base_url}/api/positions/current", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"   Status: OK")
            print(f"   holding_codes: {len(data.get('holding_codes', []))} symbols")
            print(f"   Sample: {data.get('holding_codes', [])[:5]}")
            print(f"   all_traded_codes: {len(data.get('all_traded_codes', []))} symbols")
        else:
            print(f"   Error: Status {response.status_code}")
            print(f"   Response: {response.text[:200]}")
    except requests.exceptions.ConnectionError:
        print("   Error: Cannot connect to backend (not running?)")
    except Exception as e:
        print(f"   Error: {e}")
    
    print()
    
    # Test 2: /api/positions/held
    print("2. Testing /api/positions/held")
    try:
        response = requests.get(f"{base_url}/api/positions/held", timeout=5)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            print(f"   Status: OK")
            print(f"   Items: {len(items)}")
            if items:
                print(f"   Sample: {items[0]}")
        else:
            print(f"   Error: Status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("   Error: Cannot connect to backend (not running?)")
    except Exception as e:
        print(f"   Error: {e}")
    
    print()
    
    # Test 3: Check if backend is running
    print("3. Testing backend health")
    try:
        response = requests.get(f"{base_url}/api/list", timeout=5)
        if response.status_code == 200:
            print("   Backend is running and responding")
        else:
            print(f"   Backend responded with status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("   ERROR: Backend is NOT running!")
        print("   Please start the application first.")
    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    test_api()
