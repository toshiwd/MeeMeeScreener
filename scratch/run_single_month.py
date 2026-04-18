import sys
import pandas as pd
from research.config import load_config
from research.storage import ResearchPaths
from research.features import build_features_for_asof

def run():
    paths = ResearchPaths.build()
    config = load_config("canonical_config.json")
    snapshot_id = "202604_production"
    asof_date = "2020-03-31"
    
    print("Building modified features...")
    res = build_features_for_asof(paths, config, snapshot_id, asof_date, force=True, workers=2)
    print("Build complete:", res)

if __name__ == "__main__":
    run()
