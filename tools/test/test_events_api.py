import requests
import json

def main():
    base_url = "http://localhost:8000"
    
    print("=== Checking Events API ===\n")
    
    # Check events meta
    print("1. Checking events metadata...")
    try:
        response = requests.get(f"{base_url}/api/events/meta")
        if response.status_code == 200:
            meta = response.json()
            print(f"   Status: {response.status_code}")
            print(f"   Data: {json.dumps(meta, indent=2, ensure_ascii=False)}")
        else:
            print(f"   Error: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n2. Triggering events refresh...")
    try:
        response = requests.post(f"{base_url}/api/events/refresh", params={"reason": "manual_test"})
        if response.status_code == 200:
            result = response.json()
            print(f"   Status: {response.status_code}")
            print(f"   Result: {json.dumps(result, indent=2, ensure_ascii=False)}")
        else:
            print(f"   Error: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    main()
