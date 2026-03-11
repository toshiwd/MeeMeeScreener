
import shutil
import time
import os

SRC = "C:/Users/enish/AppData/Local/MeeMeeScreener/data/stocks.duckdb"
DST = "c:/work/meemee-screener/temp_stocks.duckdb"

def copy_db():
    print(f"Attempting to copy {SRC} to {DST}")
    for i in range(10):
        try:
            shutil.copy2(SRC, DST)
            print("Copy successful!")
            return
        except OSError as e:
            print(f"Attempt {i+1} failed: {e}")
            time.sleep(1)
            
    print("Failed to copy after 10 attempts.")

if __name__ == "__main__":
    copy_db()
