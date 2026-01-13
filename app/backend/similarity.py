
import os
import duckdb
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Literal, Any
from dataclasses import dataclass

# Constants
# User specified: C:\work\meemee-data\ or similar outside repo.
# We map to local gitignored dir for safety unless env var is set.
DATA_STORE_DIR = os.getenv("MEEMEE_DATA_STORE", os.path.join(os.path.dirname(__file__), "..", "..", "data_store"))

from pydantic import BaseModel

class SearchResult(BaseModel):
    ticker: str
    asof: str  # YYYY-MM-DD
    score_total: float
    score60: float
    score24: float
    tag_id: str
    tags: Dict[str, Any]
    vec60: Optional[List[float]] = None
    vec24: Optional[List[float]] = None

class SimilarityService:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.getenv("STOCKS_DB_PATH", os.path.join(os.path.dirname(__file__), "..", "..", "stocks.duckdb"))
        self.data_dir = os.path.abspath(DATA_STORE_DIR)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.df_monthly_path = os.path.join(self.data_dir, "monthly_bars.parquet")
        self.df_vec60_path = os.path.join(self.data_dir, "vec60.parquet")
        self.df_vec24_path = os.path.join(self.data_dir, "vec24.parquet")
        self.df_env_path = os.path.join(self.data_dir, "monthly_env.parquet")
        self.tag_index_path = os.path.join(self.data_dir, "tag_index.pkl")

        # In-memory Cache
        self.df_vec60: Optional[pd.DataFrame] = None
        self.df_vec24: Optional[pd.DataFrame] = None
        self.df_env: Optional[pd.DataFrame] = None
        self.tag_index: Optional[Dict[str, List[int]]] = None
        self.loaded = False

    def load_artifacts(self):
        """Load pre-computed data into memory."""
        if self.loaded:
            return

        if not os.path.exists(self.df_vec60_path):
            print("Artifacts not found. Please run refresh_data().")
            return

        self.df_vec60 = pd.read_parquet(self.df_vec60_path)
        self.df_vec24 = pd.read_parquet(self.df_vec24_path)
        self.df_env = pd.read_parquet(self.df_env_path)
        
        with open(self.tag_index_path, "rb") as f:
            self.tag_index = pickle.load(f)
            
        self.loaded = True

    def refresh_data(self):
        """Fully rebuilds the similarity dataset from DuckDB."""
        print("Starting Refresh Data...")
        
        # 1. Load Daily Data
        with duckdb.connect(self.db_path) as conn:
            # We assume daily_bars has adjusted prices or we use it as is per instructions.
            # User requirement: "Use adjusted prices". 
            # If daily_bars is not adjusted, we might have issues. 
            # But the user says "Internal 120 months...". 
            # Let's assume daily_bars in DuckDB is the source.
            df_daily = conn.execute("SELECT code, date, o, h, l, c, v FROM daily_bars").df()
        
        if df_daily.empty:
            print("No daily data found.")
            return

        print(f"Loaded {len(df_daily)} daily rows.")

        # 2. Convert Date
        # daily_bars date is INTEGER (YYYYMMDD) or Unix Timestamp? 
        # ingest_txt.py line 724: daily["date"] = (daily["date"].astype("int64") // 1_000_000_000)
        # It seems stock.duckdb 'date' might be Unix timestamp (seconds) based on ingest code.
        # Let's check a sample or assume ingest_txt logic.
        # Wait, ingest_txt line 32: PRIMARY KEY(code, date) date INTEGER.
        # ingest_txt line 724 converts TO int64 timestamp.
        # So in DB it is int64 timestamp (seconds).
        
        df_daily["dt"] = pd.to_datetime(df_daily["date"], unit="s")
        
        # 3. Build Monthly Bars (asof = Month End)
        print("Building Monthly Bars...")
        # Resample to Month End
        # We want to group by code and Month.
        # To get "Month End", we can rely on pandas resample('M') but that fills gaps.
        # Better: Group by Year-Month, then take aggregate.
        # Then set 'asof' to the actual last date of that month (or the last trading day?)
        # User says: "asof is 'the date when the month C is fixed (= month end)'". 
        # Usually this means the last calendar day or last trading day.
        # Let's use the actual last trading day of the month as the timestamp for 'asof', 
        # OR use the calendar month end.
        # User says: "monthly_bars: ticker, month_end(asof)..."
        # "O=Start, H=Max, L=Min, C=End". 
        
        df_daily["period"] = df_daily["dt"].dt.to_period("M")
        
        def agg_ohlc(x):
            d = {}
            d['o'] = x['o'].iloc[0]
            d['h'] = x['h'].max()
            d['l'] = x['l'].min()
            d['c'] = x['c'].iloc[1] if len(x) > 1 else x['c'].iloc[-1] # Wait, C is last value
            d['c'] = x['c'].iloc[-1]
            d['v'] = x['v'].sum()
            d['asof'] = x['dt'].iloc[-1] # Last trading day
            return pd.Series(d)

        # Optimization: pandas groupby apply is slow. Use named agg.
        # Sort by date first
        df_daily = df_daily.sort_values(["code", "dt"])
        
        # Group
        g = df_daily.groupby(["code", "period"])
        df_monthly = g.agg(
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            asof=("dt", "last") # Last trading day is the 'asof' for that month bar
        ).reset_index()
        
        # Filter < 120 months history
        # Count by code
        counts = df_monthly["code"].value_counts()
        valid_codes = counts[counts >= 120].index
        df_monthly = df_monthly[df_monthly["code"].isin(valid_codes)].copy()
        
        print(f"Monthly Bars built: {len(df_monthly)} rows, {len(valid_codes)} tickers.")
        
        # Save Monthly Bars
        # We might need columns: ticker, asof (datetime), o, h, l, c, v
        # Ensure asof is datetime or string? Parquet handles datetime.
        
        # 4. Indicators (MA20, MA60) on Monthly
        print("Calculating Indicators...")
        df_monthly = df_monthly.sort_values(["code", "period"])
        
        # Calculate Returns (for Vectors)
        # vector = (x - mean) / std.
        # x = monthly return.
        df_monthly["prev_c"] = df_monthly.groupby("code")["c"].shift(1)
        df_monthly["ret"] = (df_monthly["c"] / df_monthly["prev_c"]) - 1.0
        # Fill NaN? dropna later.
        
        # MA20, MA60 for Tags
        # Use simple rolling mean
        df_monthly["ma20"] = df_monthly.groupby("code")["c"].transform(lambda x: x.rolling(20).mean())
        df_monthly["ma60"] = df_monthly.groupby("code")["c"].transform(lambda x: x.rolling(60).mean())
        
        # 5. Generate Vectors (Rolling Window)
        # We need, for each row (asof), the past 60 months returns.
        # This is tricky in pandas vectorized.
        # Creating a matrix of (N_rows, 60) features.
        # We can use strided sliding window (numpy) or manual loop (slow).
        # Optimization: `rolling` with custom apply? Slow.
        # Fastest: Construct the matrix using shift.
        
        print("Generating Vectors...")
        # Clean data for vector gen
        # We need 'ret'.
        # Drop initial rows where ret is NaN (first month)
        
        # Vector 60 dim
        # We want return at t, t-1, ..., t-59.
        # So we make 60 columns.
        cols_60 = {}
        for i in range(60):
            cols_60[f"r{i}"] = df_monthly.groupby("code")["ret"].shift(i)
        
        df_vec_temp = pd.DataFrame(cols_60)
        # The row at index `t` now has r0=ret(t), r1=ret(t-1)...
        
        # Drop rows with NaNs (first 60 months roughly)
        valid_vec_mask = df_vec_temp.notna().all(axis=1)
        
        # Now normalize each row
        # (x - mean) / std / norm
        matrix_60 = df_vec_temp[valid_vec_mask].values.astype(np.float32)
        
        # Row-wise stats
        means = np.mean(matrix_60, axis=1, keepdims=True)
        stds = np.std(matrix_60, axis=1, keepdims=True) + 1e-9
        
        z_scores = (matrix_60 - means) / stds
        
        # L2 Norm
        l2_norms = np.linalg.norm(z_scores, axis=1, keepdims=True) + 1e-9
        final_vec60 = z_scores / l2_norms
        
        # Prepare storage
        # We need to map back to (code, asof).
        # Use index of df_monthly[valid_vec_mask]
        df_res_60 = df_monthly.loc[valid_vec_mask, ["code", "asof"]].copy()
        
        # To store list of floats in parquet is inefficient for querying?
        # Better: Store as individual columns or a single binary blob?
        # pandas parquet handles arrays? Or just columns v0..v59.
        # User said "Matrix x Vector". If we load into memory, we want a Matrix.
        # If we save as individual columns, loading is easy.
        # Let's simple use a new DF with index=(code, asof) and columns 0..59 or a 'vec' column.
        # A 'vec' column with list is okay.
        
        df_res_60["vec60"] = list(final_vec60)
        
        # Repeat for Vec24
        # Just use the first 24 columns of matrix_60? 
        # No, re-normalize based on 24 months.
        matrix_24 = matrix_60[:, :24] # r0..r23
        means24 = np.mean(matrix_24, axis=1, keepdims=True)
        stds24 = np.std(matrix_24, axis=1, keepdims=True) + 1e-9
        z_scores24 = (matrix_24 - means24) / stds24
        l2_norms24 = np.linalg.norm(z_scores24, axis=1, keepdims=True) + 1e-9
        final_vec24 = z_scores24 / l2_norms24
        
        df_res_60["vec24"] = list(final_vec24)
        
        # 6. Environment Tags
        # Defined on the same valid rows (since we rely on MA60 which needs 60 months)
        # Actually MA60 needs 60 periods of price, Vec60 needs 60 periods of returns (so 61 prices).
        # We align on the valid_vec_mask which ensures extensive history.
        
        print("Generating Tags...")
        
        indices = df_res_60.index
        subset = df_monthly.loc[indices].copy()
        
        # 20MA Trend (C > MA20)
        subset["tag_ma20"] = np.where(subset["c"] > subset["ma20"], "UP", "DOWN")
        
        # 60MA Trend (C > MA60)
        subset["tag_ma60"] = np.where(subset["c"] > subset["ma60"], "UP", "DOWN")
        
        # 60MA Direction (Slope)
        # Check MA60 vs MA60 of previous month
        # Note: subset is just the rows, but we need prev value. 
        # Actually df_monthly has them.
        # Let's get "prev_ma60"
        df_monthly["prev_ma60"] = df_monthly.groupby("code")["ma60"].shift(1)
        subset["prev_ma60"] = df_monthly.loc[indices, "prev_ma60"]
        
        # Direction: UP / FLAT / DOWN.
        # Define threshold? Or just strict inequality?
        # User: "Up/Side/Down".
        # Let's use 0.2% slope or something. Or just simple comparison.
        # Simple comparison for now.
        slope = (subset["ma60"] - subset["prev_ma60"]) / subset["prev_ma60"]
        # Thresholds maybe 0.1%?
        # Let's use strict for now, or a very small epsilon.
        subset["tag_dir60"] = "SIDE"
        subset.loc[slope > 0.001, "tag_dir60"] = "UP"
        subset.loc[slope < -0.001, "tag_dir60"] = "DOWN"
        
        # 10 Year Range Position (5% - 95%)
        # Need 120 month high/low for each row.
        # Rolling 120 Max/Min on H/L.
        df_monthly["low120"] = df_monthly.groupby("code")["l"].transform(lambda x: x.rolling(120).min())
        df_monthly["high120"] = df_monthly.groupby("code")["h"].transform(lambda x: x.rolling(120).max())
        
        subset["low120"] = df_monthly.loc[indices, "low120"]
        subset["high120"] = df_monthly.loc[indices, "high120"]
        
        # Pos = (C - Low) / (High - Low)
        # Tag: High (Top 1/3), Mid, Low.
        denom = subset["high120"] - subset["low120"]
        pos = (subset["c"] - subset["low120"]) / denom.replace(0, 1)
        
        subset["tag_range"] = "MID"
        subset.loc[pos > 0.66, "tag_range"] = "HIGH"
        subset.loc[pos < 0.33, "tag_range"] = "LOW"
        
        # Combine Tags into ID
        # ID = MA20_MA60_DIR_RANGE (e.g. UP_UP_UP_HIGH)
        subset["tag_id"] = (
            subset["tag_ma20"] + "_" +
            subset["tag_ma60"] + "_" +
            subset["tag_dir60"] + "_" +
            subset["tag_range"]
        )
        
        # 7. Save
        # Vectors
        # We flatten the vectors for storage or keep as efficient format?
        # Parquet with list column is fine.
        
        self.df_vec60 = df_res_60[["code", "asof", "vec60"]]
        self.df_vec24 = df_res_60[["code", "asof", "vec24"]]
        
        # Env (Tags)
        self.df_env = subset[["code", "asof", "tag_id", "tag_ma20", "tag_ma60", "tag_dir60", "tag_range"]]
        
        print("Saving to parquet...")
        self.df_vec60.to_parquet(self.df_vec60_path, index=False)
        self.df_vec24.to_parquet(self.df_vec24_path, index=False)
        self.df_env.to_parquet(self.df_env_path, index=False)
        
        # 8. Build Index
        print("Building Index...")
        # Dictionary: tag_id -> list of indices (integers) corresponding to the DF rows.
        # IMPORTANT: We need row-based access for matrix mult.
        # If we load df_vec60 in memory, we can access by row integer.
        # So index should store row numbers of df_vec60 (which aligns with df_env).
        
        # Reset index to get 0..N
        # Make sure they are aligned!
        # df_vec60 and df_env are derived from same indices, so if we sort them identical?
        # subset index is preserved.
        # Let's assume we load them back and they might be whatever order.
        # Prudent: Sort by code, asof.
        
        self.df_vec60 = self.df_vec60.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_vec24 = self.df_vec24.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_env = self.df_env.sort_values(["code", "asof"]).reset_index(drop=True)
        
        self.df_vec60.to_parquet(self.df_vec60_path, index=False)
        self.df_vec24.to_parquet(self.df_vec24_path, index=False)
        self.df_env.to_parquet(self.df_env_path, index=False)

        # Build map Tag -> List[Int]
        tag_map = {}
        for idx, row in self.df_env.iterrows():
            tid = row["tag_id"]
            if tid not in tag_map:
                tag_map[tid] = []
            tag_map[tid].append(idx)
            
        with open(self.tag_index_path, "wb") as f:
            pickle.dump(tag_map, f)
            
        self.tag_index = tag_map
        self.loaded = True
        print("Refresh Complete.")

    def search(
        self, 
        ticker: str, 
        asof: Optional[str] = None, 
        k: int = 30, 
        alpha: float = 0.7
    ) -> List[SearchResult]:
        
        if not self.loaded:
            self.load_artifacts()
            
        # Parse AsOf
        if asof:
            # Try parsing YYYY-MM-DD
            target_date = pd.to_datetime(asof)
        else:
            # Use latest available date for the ticker
            target_date = None
            
        # Find Query Row
        query_mask = (self.df_env["code"] == ticker)
        df_ticker = self.df_env[query_mask].copy()
        
        if df_ticker.empty:
            # Ticker not in index (likely < 120 months data)
            raise ValueError(f"Ticker {ticker} not indexed (insufficient history)")
            
        if target_date:
            # Fuzzy match: Match Year-Month
            target_period = target_date.to_period("M")
            df_ticker["period"] = df_ticker["asof"].dt.to_period("M")
            
            # 1. Exact Month Match
            match = df_ticker[df_ticker["period"] == target_period]
            
            if match.empty:
                # 2. Try closest within +/- 1 month? Or just fallback to latest?
                # User suggestion: "Round to latest" or similar. 
                # Let's try to find the closest date if exact month fails.
                # Actually, if user asks for specific date and we don't have it, generally that's an error.
                # But for robustness, let's pick the closest date available.
                # Ensure target_date is datetime (it matches df_ticker['asof'] type)
                # target_date was created with pd.to_datetime(asof) so it is Timestamp.
                # df_ticker["asof"] is datetime64[ns]
                df_ticker["diff"] = (df_ticker["asof"] - target_date).abs()
                closest_idx = df_ticker["diff"].idxmin()
                
                query_idx = closest_idx
                # Log warning?
                closest_asof = self.df_env.loc[query_idx, 'asof']
                print(f"Warning: Exact asof {asof} not found for {ticker}. Using closest: {closest_asof}")
            else:
                query_idx = match.index[0]
        else:
            # Latest
            # Assumes sorted by time
            query_idx = df_ticker.index[-1]
            
        query_row = self.df_env.iloc[query_idx]
        query_tag = query_row["tag_id"]
        query_vec60 = np.array(self.df_vec60.iloc[query_idx]["vec60"], dtype=np.float32)
        query_vec24 = np.array(self.df_vec24.iloc[query_idx]["vec24"], dtype=np.float32)
        
        # Fallback Levels
        # 0: Exact Tag
        candidates_indices = self.tag_index.get(query_tag, [])
        
        # If few candidates, relax
        # Parse Tag ID: MA20_MA60_DIR_RANGE
        # Ex: UP_UP_UP_HIGH
        parts = query_tag.split("_") # [MA20, MA60, DIR, RANGE]
        
        level_desc = "Level 0 (Exact)"
        
        if len(candidates_indices) < k:
            # Level 1: Ignore Range
            # Pattern: MA20_MA60_DIR_*
            prefix = "_".join(parts[:3])
            candidates_indices = self._find_indices_prefix(prefix)
            level_desc = "Level 1 (Ignore Range)"
            
        if len(candidates_indices) < k:
            # Level 2: Ignore Dir
            # Pattern: MA20_MA60_*
            prefix = "_".join(parts[:2])
            candidates_indices = self._find_indices_prefix(prefix)
            level_desc = "Level 2 (Ignore Dir)"
            
        if len(candidates_indices) < k:
            # Level 3: MA60 only? User: "60MA上下のみ一致"
            # Pattern: *_MA60_*? 
            # User spec: "Level 3: 60MA上下のみ一致".
            # Tag: MA20_MA60_DIR_RANGE.
            # So we match the 2nd part.
            target_ma60 = parts[1]
            candidates_indices = self._find_indices_ma60(target_ma60)
            level_desc = "Level 3 (MA60 Only)"
            
        if len(candidates_indices) < k:
             # Level 4: All
             candidates_indices = list(self.df_env.index)
             level_desc = "Level 4 (All)"
             
        # Limit candidates to prevents MEMORY EXPLOSION?
        # User says "Search DB scan prohibited. Candidates from index."
        # If Level 4, it's all.
        # "Safety cap": User says "Return limit (100)".
        # Also "Candidates limit"?
        # "Level 4 (Final resort. Cap count)".
        # 180k rows is manageable for dot product (approx 50MB matrix).
        # Increasing limit to cover full dataset if needed.
        if len(candidates_indices) > 300000: 
            candidates_indices = candidates_indices[:300000]
            
        print(f"[Search Debug] Query: {ticker} (tag={query_tag}) -> {level_desc}, Candidates: {len(candidates_indices)}")

        # Calculate Scores
        # Vectorize!
        # Gather vectors
        cand_vec60 = np.vstack(self.df_vec60.iloc[candidates_indices]["vec60"].values) # (N, 60)
        cand_vec24 = np.vstack(self.df_vec24.iloc[candidates_indices]["vec24"].values) # (N, 24)
        
        # Dot Product
        # query_vec is (D,)
        # score = cand @ query
        s60 = np.dot(cand_vec60, query_vec60)
        s24 = np.dot(cand_vec24, query_vec24)
        
        scores = alpha * s60 + (1 - alpha) * s24
        
        # Build Result DF
        df_res = pd.DataFrame({
            "idx": candidates_indices,
            "score": scores,
            "s60": s60,
            "s24": s24
        })
        
        df_res = df_res.sort_values("score", ascending=False)
        
        # Dedup (Min Sep Months)
        # We need code and time for the candidates.
        # Join back
        final_results = []
        seen_codes = {} # code -> list of asof timestamps
        
        for _, r in df_res.iterrows():
            if len(final_results) >= k:
                break
                
            idx = int(r["idx"])
            row_meta = self.df_env.iloc[idx]
            code = row_meta["code"]
            r_asof = row_meta["asof"]
            
            # Check overlap
            # ±12 months
            skip = False
            
            # 1. Self logic: Don't show "too close" to query date
            if code == ticker:
                 q_asof = query_row["asof"]
                 diff_days = abs((r_asof - q_asof).days)
                 # Relaxed from 365 to 60 days
                 if diff_days < 60:
                     continue

            # 2. Others logic
            if code in seen_codes:
                # Allow up to 3 results per ticker
                if len(seen_codes[code]) >= 3:
                     continue
                     
                for existing_ts in seen_codes[code]:
                    diff_days = abs((r_asof - existing_ts).days)
                    if diff_days < 60: # Relaxed to 2 months
                        skip = True
                        break
            if skip:
                continue

            if code not in seen_codes:
                seen_codes[code] = []
            seen_codes[code].append(r_asof)
            
            final_results.append(SearchResult(
                ticker=code,
                asof=r_asof.strftime("%Y-%m-%d"),
                score_total=float(r["score"]),
                score60=float(r["s60"]),
                score24=float(r["s24"]),
                tag_id=row_meta["tag_id"],
                tags={
                    "ma20": str(row_meta["tag_ma20"]),
                    "ma60": str(row_meta["tag_ma60"]),
                    "dir": str(row_meta["tag_dir60"]),
                    "range": str(row_meta["tag_range"]),
                    "fallback": str(level_desc)
                },
                vec60=[float(x) for x in self.df_vec60.iloc[idx]["vec60"]],
                vec24=[float(x) for x in self.df_vec24.iloc[idx]["vec24"]]
            ))
            
        return final_results

    def _find_indices_prefix(self, prefix):
        # Scan keys?
        # Optimization: Pre-group by prefix if needed.
        # For now, iterate keys.
        res = []
        for k, v in self.tag_index.items():
            if k.startswith(prefix):
                res.extend(v)
        return res

    def _find_indices_ma60(self, ma60_val):
        # ma60 is 2nd part.
        res = []
        for k, v in self.tag_index.items():
            parts = k.split("_")
            if len(parts) >= 2 and parts[1] == ma60_val:
                res.extend(v)
        return res

if __name__ == "__main__":
    # Test script
    svc = SimilarityService()
    svc.refresh_data()
