import sys
import pandas as pd
from pathlib import Path

# Files to compare
cache_dir_old = Path(r"G:\Tradex\scratch\research\workspace\cache\202604_production__v3_deep__v3_quality_sl8__ed477133316a")
cache_dir_new = Path(r"G:\Tradex\scratch\research\workspace\cache\202604_production__v3_deep__v3_quality_sl8__0b909e57ae9c")
file_old = cache_dir_old / "features_2020-03-31.csv.bak"
file_new = cache_dir_new / "features_2020-03-31.csv"


def run_test():
    old_df = pd.read_csv(file_old)
    new_df = pd.read_csv(file_new)
    
    print(f"Old shape: {old_df.shape}")
    print(f"New shape: {new_df.shape}")
    
    if old_df.shape != new_df.shape:
        print("ERROR: Row or Column counts differ!")
        return
        
    old_cols = set(old_df.columns)
    new_cols = set(new_df.columns)
    
    if old_cols != new_cols:
        print("ERROR: Column sets differ")
        return
        
    # Sort just in case
    old_df = old_df.sort_values("code").reset_index(drop=True)
    new_df = new_df.sort_values("code").reset_index(drop=True)
    
    # Exclude non-deterministic or metadata columns
    exclude_cols = {'created_at', 'snapshot_id', 'feature_version', 'asof_date', 'code'}
    compare_cols = [c for c in old_df.columns if c not in exclude_cols]
    
    diff_cols = []
    import numpy as np
    for c in compare_cols:
        arr_old = pd.to_numeric(old_df[c], errors='coerce').fillna(0).to_numpy(dtype=float)
        arr_new = pd.to_numeric(new_df[c], errors='coerce').fillna(0).to_numpy(dtype=float)
        
        if not np.allclose(arr_old, arr_new, rtol=1e-5, atol=1e-5):
            diff_cols.append(c)
            
    if diff_cols:
        print(f"ERROR: Differences found in columns: {diff_cols}")
        for c in diff_cols[:3]:
            arr_old = pd.to_numeric(old_df[c], errors='coerce').fillna(0).to_numpy(dtype=float)
            arr_new = pd.to_numeric(new_df[c], errors='coerce').fillna(0).to_numpy(dtype=float)
            diffs = ~np.isclose(arr_old, arr_new, rtol=1e-5, atol=1e-5)
            idx = np.where(diffs)[0][0]
            print(f"  Col {c} diff at idx {idx}: Old={arr_old[idx]}, New={arr_new[idx]}")
    else:
        print("SUCCESS: DataFrames are numerically identical!")

if __name__ == "__main__":
    run_test()
