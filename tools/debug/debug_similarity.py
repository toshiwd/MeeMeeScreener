
import sys
import os
import traceback

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

try:
    from similarity import SimilarityService
    print("Import successful")
    
    svc = SimilarityService()
    print("Service initialized")
    
    svc.load_artifacts()
    print("Artifacts loaded")
    
    results = svc.search("1911", k=5)
    print(f"Search successful. Found {len(results)} results.")
    for r in results:
        print(r)

except Exception:
    traceback.print_exc()
