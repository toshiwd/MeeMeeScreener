
import sys
import os
import traceback
import json
from pydantic import BaseModel

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

try:
    from similarity import SimilarityService, SearchResult
    
    svc = SimilarityService()
    svc.load_artifacts()
    
    results = svc.search("1911", k=5)
    print(f"Found {len(results)} results")
    
    # Simulate FastAPI serialization
    # FastAPI triggers .model_dump() (v2) or .dict() (v1) and then json.dumps
    
    data = [r.model_dump() for r in results]
    
    # Check for NaNs
    import math
    has_nan = False
    for item in data:
        for k, v in item.items():
            if isinstance(v, float) and math.isnan(v):
                print(f"NaN found in {k}")
                has_nan = True
            if isinstance(v, list):
                for i, val in enumerate(v):
                    if isinstance(val, float) and math.isnan(val):
                        print(f"NaN found in {k}[{i}]")
                        has_nan = True
    
    if has_nan:
        print("DETECTED NANs! JSON serialization will produce invalid JSON or error.")

    # Try json dump
    json_str = json.dumps(data)
    print("JSON dump success (standard json module)")
    # Note: standard json dumps NaN as NaN. 
    # But usually web clients hate it. 
    # And some json encoders (simplejson default) might error? strict=True?
    
    print("First result JSON:", json_str[:200])

except Exception:
    traceback.print_exc()
