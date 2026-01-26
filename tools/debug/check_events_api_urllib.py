import urllib.request
import urllib.error

try:
    with urllib.request.urlopen('http://127.0.0.1:8000/api/events/meta', timeout=2) as response:
        print(f"Status: {response.getcode()}")
        print(f"Content: {response.read().decode('utf-8')}")
except urllib.error.HTTPError as e:
    print(f"HTTPError: {e.code}")
except Exception as e:
    print(f"Error: {e}")
