from __future__ import annotations

import os
import duckdb
import numpy as np
import pickle
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Literal, Any, TYPE_CHECKING
from dataclasses import dataclass

try:
    import pandas as pd
except Exception:  # pragma: no cover - runtime fallback for missing pandas in packaged app
    pd = None  # type: ignore[assignment]

if TYPE_CHECKING:
    import pandas as pd

# Constants
# User specified: C:\work\meemee-data\ or similar outside repo.
# We map to local gitignored dir for safety unless env var is set.
DATA_STORE_DIR = os.getenv("MEEMEE_DATA_STORE", os.path.join(os.path.dirname(__file__), "..", "..", "data_store"))

from pydantic import BaseModel

LONG_TERM_WINDOW = 60
SHORT_TERM_WINDOW = 12

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
        self.db_path = db_path or os.getenv("STOCKS_DB_PATH", os.path.join(os.path.dirname(__file__), "stocks.duckdb"))
        self.data_dir = os.path.abspath(DATA_STORE_DIR)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.df_monthly_path = os.path.join(self.data_dir, "monthly_bars.parquet")
        self.df_vec60_path = os.path.join(self.data_dir, "vec60.parquet")
        self.df_vec24_path = os.path.join(self.data_dir, "vec24.parquet")
        self.df_env_path = os.path.join(self.data_dir, "monthly_env.parquet")
        self.tag_index_path = os.path.join(self.data_dir, "tag_index.pkl")

        # In-memory Cache
        self.df_vec60: Optional["pd.DataFrame"] = None
        self.df_vec24: Optional["pd.DataFrame"] = None
        self.df_env: Optional["pd.DataFrame"] = None
        self.tag_index: Optional[Dict[str, List[int]]] = None
        self.loaded = False

    def _load_monthly_bars(
        self,
        conn: duckdb.DuckDBPyConnection,
        codes: Optional[list[str]] = None
    ) -> "pd.DataFrame":
        if pd is None:
            raise RuntimeError("pandas_missing")
        if codes:
            placeholders = ",".join(["?"] * len(codes))
            query = f"""
                SELECT code, month, o, h, l, c
                FROM monthly_bars
                WHERE code IN ({placeholders})
                ORDER BY code, month
            """
            df_monthly = conn.execute(query, codes).df()
        else:
            df_monthly = conn.execute(
                """
                SELECT code, month, o, h, l, c
                FROM monthly_bars
                ORDER BY code, month
                """
            ).df()

        if df_monthly.empty:
            return df_monthly

        if df_monthly["month"].max() >= 1_000_000_000:
            dt = pd.to_datetime(df_monthly["month"], unit="s")
        else:
            dt = pd.to_datetime(df_monthly["month"].astype(str) + "01", format="%Y%m%d")
        df_monthly["period"] = dt.dt.to_period("M")
        df_monthly["asof"] = df_monthly["period"].dt.to_timestamp("M")
        return df_monthly

    def _build_vectors_and_tags(
        self, df_monthly: "pd.DataFrame"
    ) -> tuple["pd.DataFrame", "pd.DataFrame", "pd.DataFrame"]:
        if pd is None:
            raise RuntimeError("pandas_missing")
        df_monthly = df_monthly.sort_values(["code", "period"]).copy()

        df_monthly["prev_c"] = df_monthly.groupby("code")["c"].shift(1)
        df_monthly["ret"] = (df_monthly["c"] / df_monthly["prev_c"]) - 1.0

        df_monthly["ma7"] = df_monthly.groupby("code")["c"].transform(lambda x: x.rolling(7).mean())
        df_monthly["ma20"] = df_monthly.groupby("code")["c"].transform(lambda x: x.rolling(20).mean())
        df_monthly["ma60"] = df_monthly.groupby("code")["c"].transform(lambda x: x.rolling(60).mean())
        df_monthly["ma100"] = df_monthly.groupby("code")["c"].transform(lambda x: x.rolling(100).mean())

        cols_60 = {}
        for i in range(LONG_TERM_WINDOW):
            cols_60[f"r{i}"] = df_monthly.groupby("code")["ret"].shift(i)

        df_vec_temp = pd.DataFrame(cols_60)
        valid_vec_mask = df_vec_temp.notna().all(axis=1)

        matrix_60 = df_vec_temp[valid_vec_mask].values.astype(np.float32)
        means = np.mean(matrix_60, axis=1, keepdims=True)
        stds = np.std(matrix_60, axis=1, keepdims=True) + 1e-9
        z_scores = (matrix_60 - means) / stds
        l2_norms = np.linalg.norm(z_scores, axis=1, keepdims=True) + 1e-9
        final_vec60 = z_scores / l2_norms

        # subset will be created later after tags, but we already have vec60

        matrix_short = matrix_60[:, :SHORT_TERM_WINDOW]
        means_short = np.mean(matrix_short, axis=1, keepdims=True)
        stds_short = np.std(matrix_short, axis=1, keepdims=True) + 1e-9
        z_scores_short = (matrix_short - means_short) / stds_short
        l2_norms_short = np.linalg.norm(z_scores_short, axis=1, keepdims=True) + 1e-9
        final_vec24 = z_scores_short / l2_norms_short
        df_monthly["prev_ma60"] = df_monthly.groupby("code")["ma60"].shift(1)
        df_monthly["low120"] = df_monthly.groupby("code")["l"].transform(lambda x: x.rolling(120).min())
        df_monthly["high120"] = df_monthly.groupby("code")["h"].transform(lambda x: x.rolling(120).max())

        for ma_col in ["ma7", "ma20", "ma60", "ma100"]:
            df_monthly[f"{ma_col}_slope"] = df_monthly.groupby("code")[ma_col].diff()

        subset = df_monthly.loc[valid_vec_mask].copy()
        subset["tag_ma20"] = np.where(subset["c"] > subset["ma20"], "UP", "DOWN")
        subset["tag_ma60"] = np.where(subset["c"] > subset["ma60"], "UP", "DOWN")

        slope = (subset["ma60"] - subset["prev_ma60"]) / subset["prev_ma60"]
        subset["tag_dir60"] = "SIDE"
        subset.loc[slope > 0.001, "tag_dir60"] = "UP"
        subset.loc[slope < -0.001, "tag_dir60"] = "DOWN"

        denom = subset["high120"] - subset["low120"]
        pos = (subset["c"] - subset["low120"]) / denom.replace(0, 1)
        subset["tag_range"] = "MID"
        subset.loc[pos > 0.66, "tag_range"] = "HIGH"
        subset.loc[pos < 0.33, "tag_range"] = "LOW"

        subset["tag_id"] = (
            subset["tag_ma20"] + "_" +
            subset["tag_ma60"] + "_" +
            subset["tag_dir60"] + "_" +
            subset["tag_range"]
        )

        df_res_60 = subset[["code", "asof"]].copy()
        df_res_60["tag_id"] = subset["tag_id"].values
        df_res_60["vec60"] = list(final_vec60)
        df_res_60["vec24"] = list(final_vec24)
        df_vec60 = df_res_60[["code", "asof", "vec60"]]
        df_vec24 = df_res_60[["code", "asof", "vec24"]]
        df_env = subset[
            [
                "code",
                "asof",
                "tag_id",
                "tag_ma20",
                "tag_ma60",
                "tag_dir60",
                "tag_range",
                "c",
                "ma7",
                "ma20",
                "ma60",
                "ma100",
                "ma7_slope",
                "ma20_slope",
                "ma60_slope",
                "ma100_slope",
            ]
        ]

        return df_vec60, df_vec24, df_env

    def _persist_artifacts(self) -> None:
        if pd is None:
            raise RuntimeError("pandas_missing")
        self.df_vec60 = self.df_vec60.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_vec24 = self.df_vec24.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_env = self.df_env.sort_values(["code", "asof"]).reset_index(drop=True)

        self.df_vec60.to_parquet(self.df_vec60_path, index=False)
        self.df_vec24.to_parquet(self.df_vec24_path, index=False)
        self.df_env.to_parquet(self.df_env_path, index=False)

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

    def load_artifacts(self):
        """Load pre-computed data into memory."""
        if self.loaded:
            return
        if pd is None:
            raise RuntimeError("pandas_missing")

        required_paths = [
            self.df_vec60_path,
            self.df_vec24_path,
            self.df_env_path,
            self.tag_index_path
        ]
        missing = [path for path in required_paths if not os.path.exists(path)]
        if missing:
            try:
                # Attempt to rebuild artifacts on demand if missing
                self.refresh_data(incremental=False)
            except Exception as exc:
                missing_list = ", ".join(missing)
                raise FileNotFoundError(
                    f"Similarity artifacts not found: {missing_list}"
                ) from exc
            missing = [path for path in required_paths if not os.path.exists(path)]
            if missing:
                missing_list = ", ".join(missing)
                raise FileNotFoundError(f"Similarity artifacts not found: {missing_list}")

        try:
            self.df_vec60 = pd.read_parquet(self.df_vec60_path)
            self.df_vec24 = pd.read_parquet(self.df_vec24_path)
            self.df_env = pd.read_parquet(self.df_env_path)
        except ImportError as exc:
            raise RuntimeError("Parquet engine missing. Install pyarrow or fastparquet.") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to load similarity artifacts: {exc}") from exc

        try:
            with open(self.tag_index_path, "rb") as f:
                self.tag_index = pickle.load(f)
        except Exception as exc:
            raise RuntimeError(f"Failed to load tag index: {exc}") from exc
            
        self.loaded = True

    def refresh_data(self, incremental: bool = False):
        """Rebuilds the similarity dataset from DuckDB. Incremental updates are supported."""
        print("Starting Refresh Data...")
        if pd is None:
            raise RuntimeError("pandas_missing")

        if incremental and not os.path.exists(self.df_vec60_path):
            incremental = False

        if incremental:
            try:
                self.load_artifacts()
            except Exception as exc:
                print(f"Incremental refresh fallback to full: {exc}")
                incremental = False

        with duckdb.connect(self.db_path) as conn:
            if incremental:
                db_max = conn.execute(
                    "SELECT code, MAX(month) AS max_month FROM monthly_bars GROUP BY code"
                ).df()
                if db_max.empty:
                    print("No monthly data found.")
                    return

                use_epoch = db_max["max_month"].max() >= 1_000_000_000
                existing = self.df_env.copy()
                if use_epoch:
                    month_start = existing["asof"].dt.to_period("M").dt.to_timestamp()
                    existing["month"] = (month_start.astype("int64") // 1_000_000_000).astype("int64")
                else:
                    existing["month"] = existing["asof"].dt.year * 100 + existing["asof"].dt.month
                existing_max = existing.groupby("code", as_index=False)["month"].max()

                merged = db_max.merge(existing_max, on="code", how="left", suffixes=("_db", "_existing"))
                needs_update = merged[
                    merged["month"].isna() | (merged["max_month"] > merged["month"])
                ]
                codes_to_update = needs_update["code"].tolist()

                if not codes_to_update:
                    print("No incremental updates needed.")
                    return

                df_monthly = self._load_monthly_bars(conn, codes_to_update)
                if df_monthly.empty:
                    print("No monthly data found for updates.")
                    return

                counts = df_monthly["code"].value_counts()
                valid_codes = set(counts[counts >= 120].index)
                insufficient_codes = [code for code in codes_to_update if code not in valid_codes]
                df_monthly = df_monthly[df_monthly["code"].isin(valid_codes)].copy()

                if insufficient_codes:
                    self.df_vec60 = self.df_vec60[~self.df_vec60["code"].isin(insufficient_codes)]
                    self.df_vec24 = self.df_vec24[~self.df_vec24["code"].isin(insufficient_codes)]
                    self.df_env = self.df_env[~self.df_env["code"].isin(insufficient_codes)]

                if df_monthly.empty:
                    self._persist_artifacts()
                    print("Incremental Refresh Complete (removals only).")
                    return

                print(f"Incremental rebuild for {len(valid_codes)} tickers.")
                df_vec60_new, df_vec24_new, df_env_new = self._build_vectors_and_tags(df_monthly)

                drop_codes = set(codes_to_update)
                self.df_vec60 = self.df_vec60[~self.df_vec60["code"].isin(drop_codes)]
                self.df_vec24 = self.df_vec24[~self.df_vec24["code"].isin(drop_codes)]
                self.df_env = self.df_env[~self.df_env["code"].isin(drop_codes)]

                self.df_vec60 = pd.concat([self.df_vec60, df_vec60_new], ignore_index=True)
                self.df_vec24 = pd.concat([self.df_vec24, df_vec24_new], ignore_index=True)
                self.df_env = pd.concat([self.df_env, df_env_new], ignore_index=True)

                self._persist_artifacts()
                print("Incremental Refresh Complete.")
                return

            df_monthly = self._load_monthly_bars(conn)

        if df_monthly.empty:
            print("No monthly data found.")
            return

        counts = df_monthly["code"].value_counts()
        valid_codes = counts[counts >= 120].index
        df_monthly = df_monthly[df_monthly["code"].isin(valid_codes)].copy()

        print(f"Monthly Bars loaded: {len(df_monthly)} rows, {len(valid_codes)} tickers.")
        print("Generating vectors and tags...")

        self.df_vec60, self.df_vec24, self.df_env = self._build_vectors_and_tags(df_monthly)
        self._persist_artifacts()
        print("Refresh Complete.")

    def search(
        self,
        ticker: str,
        asof: Optional[str] = None,
        k: int = 30,
        alpha: float = 0.7,
        match_tag: bool = False,
    ) -> List[SearchResult]:
        if pd is None:
            raise RuntimeError("pandas_missing")
        
        if not self.loaded:
            self.load_artifacts()
        if not self.loaded or self.df_env is None or self.df_vec60 is None or self.df_vec24 is None or self.tag_index is None:
            raise RuntimeError("Similarity artifacts are not loaded. Run refresh_data() first.")
            
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
        tag_focus_info = ""
        if match_tag:
            tag_focus_info = " (Tag Focus)" if candidates_indices else " (Tag Focus: none)"
        
        # If few candidates, relax
        # Parse Tag ID: MA20_MA60_DIR_RANGE
        # Ex: UP_UP_UP_HIGH
        parts = query_tag.split("_") # [MA20, MA60, DIR, RANGE]
        
        level_desc = f"Level 0 (Exact{tag_focus_info})"
        
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

        if not candidates_indices:
            return []
            
        print(f"[Search Debug] Query: {ticker} (tag={query_tag}) -> {level_desc}, Candidates: {len(candidates_indices)}")

        # Calculate Scores
        # Vectorize!
        cand_env = self.df_env.iloc[candidates_indices]
        cand_vec60 = np.vstack(self.df_vec60.iloc[candidates_indices]["vec60"].values) # (N, 60)
        cand_vec24 = np.vstack(self.df_vec24.iloc[candidates_indices]["vec24"].values) # (N, 24)

        # Dot Product base
        s60 = np.dot(cand_vec60, query_vec60)
        s24 = np.dot(cand_vec24, query_vec24)
        scores = alpha * s60 + (1 - alpha) * s24

        # Monthly MA similarity boost
        ma_cols = ["ma7", "ma20", "ma60", "ma100"]
        slope_cols = [f"{col}_slope" for col in ma_cols]
        query_ma = np.nan_to_num(query_row[ma_cols].to_numpy(dtype=np.float32), nan=0.0)
        query_slopes = np.nan_to_num(query_row[slope_cols].to_numpy(dtype=np.float32), nan=0.0)
        cand_ma = np.nan_to_num(cand_env[ma_cols].to_numpy(dtype=np.float32), nan=0.0)
        cand_slopes = np.nan_to_num(cand_env[slope_cols].to_numpy(dtype=np.float32), nan=0.0)
        ma_diff = np.abs(cand_ma - query_ma) / (np.abs(query_ma) + 1e-3)
        slope_diff = np.abs(cand_slopes - query_slopes) / (np.abs(query_slopes) + 1e-3)
        ma_similarity = np.exp(-np.sum(ma_diff, axis=1))
        slope_similarity = np.exp(-np.sum(slope_diff, axis=1))
        scores = scores + 0.25 * ma_similarity + 0.15 * slope_similarity
        
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
