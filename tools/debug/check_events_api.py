import requests

try:
    response = requests.get('http://127.0.0.1:8000/api/events/meta', timeout=2)
    print(f"Status: {response.status_code}")
    print(f"Content: {response.text}")
except Exception as e:
    print(f"Error: {e}")
