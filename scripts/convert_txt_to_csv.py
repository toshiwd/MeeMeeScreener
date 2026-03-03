import pandas as pd
from pathlib import Path
import os
import glob

def main():
    txt_dir = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt")
    output_dir = Path(r"c:\work\meemee-screener\production_data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    daily_csv = output_dir / "production_daily.csv"
    universe_dir = output_dir / "universe"
    universe_dir.mkdir(parents=True, exist_ok=True)
    
    txt_files = list(txt_dir.glob("*.txt"))
    total_files = len(txt_files)
    print(f"Found {total_files} txt files.")
    
    all_chunks = []
    universe_rows = []
    
    for i, path in enumerate(txt_files):
        if i % 100 == 0:
            print(f"Processing file {i}/{total_files}...")
        try:
            # 形式: 1332,1994/06/14,511,520,511,518,863
            # ヘッダーなし
            df = pd.read_csv(path, header=None, names=["code", "date", "open", "high", "low", "close", "volume"])
            
            # 日付形式の変換 (1994/06/14 -> 1994-06-14)
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.dropna(subset=["date"])
            
            # codeを文字列4桁に統一 (念のため)
            if not df.empty:
                code_str = str(df["code"].iloc[0]).zfill(4)
                df["code"] = code_str
                
                all_chunks.append(df)
                
                # ユニバース用: 月末日を特定して抽出
                df_dt = pd.to_datetime(df["date"])
                month_ends = df_dt.groupby(df_dt.dt.to_period("M")).max()
                for me in month_ends:
                    universe_rows.append({"asof_date": me.strftime("%Y-%m-%d"), "code": code_str})
                    
        except Exception as e:
            print(f"Error processing {path.name}: {e}")

    if not all_chunks:
        print("No data processed.")
        return

    print("Concatenating all data...")
    full_daily = pd.concat(all_chunks, ignore_index=True)
    full_daily.to_csv(daily_csv, index=False)
    print(f"Saved {len(full_daily)} rows to {daily_csv}")
    
    print("Saving universe files...")
    univ_df = pd.DataFrame(universe_rows)
    # 月ごとにCSVを分割して保存 (ingest.pyがglob/*.csv形式を期待するため)
    for ym, group in univ_df.groupby(pd.to_datetime(univ_df["asof_date"]).dt.strftime("%Y-%m")):
        group.to_csv(universe_dir / f"{ym}.csv", index=False)
    
    print("Done.")

if __name__ == "__main__":
    main()
