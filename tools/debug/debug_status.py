
import sys
import os
import requests

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

try:
    from similarity import SimilarityService
    svc = SimilarityService()
    svc.load_artifacts()
    
    print("\n--- Index Stats ---")
    if svc.df_env is not None:
        print(f"Total rows: {len(svc.df_env)}")
        print(f"Unique tickers: {svc.df_env['code'].nunique()}")
        print(f"Sample tickers: {svc.df_env['code'].unique()[:10]}")
        
        # Check specific
        target = "6005"
        mask = svc.df_env["code"] == target
        print(f"Check {target}: Found {mask.sum()} rows")
        
    else:
        print("df_env is None")

    print("\n--- API Test ---")
    try:
        # Test specific date mentioned by user
        t_date = "2024-12-05"
        print(f"Testing ticker=6005 asof={t_date}")
        resp = requests.get("http://127.0.0.1:8000/api/search/similar", params={"ticker": "6005", "asof": t_date, "k": 5})
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"Response: {resp.text[:500]}")
        else:
            print(f"Error: {resp.text}")
    except Exception as e:
        print(f"API Request failed: {e}")

except Exception as e:
    print(e)
