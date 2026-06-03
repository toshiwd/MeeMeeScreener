from __future__ import annotations

import os
import time
import duckdb
from threading import RLock
try:
    import numpy as np
except Exception:  # pragma: no cover - runtime fallback for missing numpy in packaged app
    np = None  # type: ignore[assignment]
import pickle
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Literal, Any, TYPE_CHECKING
from dataclasses import dataclass
from app.core.config import config
from app.db.session import get_conn_for_path

try:
    import pandas as pd
except Exception:  # pragma: no cover - runtime fallback for missing pandas in packaged app
    pd = None  # type: ignore[assignment]

if TYPE_CHECKING:
    import pandas as pd

# Constants
# Prefer MEEMEE_DATA_STORE if set, else MEEMEE_DATA_DIR/data_store, else fallback to repo-local data_store.
_data_store_env = os.getenv("MEEMEE_DATA_STORE")
_data_dir_env = os.getenv("MEEMEE_DATA_DIR")
if _data_store_env:
    DATA_STORE_DIR = _data_store_env
elif _data_dir_env:
    DATA_STORE_DIR = os.path.join(_data_dir_env, "data_store")
else:
    DATA_STORE_DIR = str(config.DATA_DIR / "data_store")

from pydantic import BaseModel

LONG_TERM_WINDOW = 60
SHORT_TERM_WINDOW = 24
RECENT_DAILY_SHAPE_WINDOW = 126
RECENT_DAILY_FEATURE_COUNT = 6
RECENT_DAILY_SHAPE_FEATURES = "close_return,ma7_gap,ma20_gap,ma60_gap,ma100_gap,ma200_gap"
RECENT_DAILY_VECTOR_WIDTH = RECENT_DAILY_SHAPE_WINDOW * RECENT_DAILY_FEATURE_COUNT
RECENT_DAILY_WEIGHTING = "linear_recent_1.0_to_2.5"
RECENT_DAILY_WEIGHT_MIN = 1.0
RECENT_DAILY_WEIGHT_MAX = 2.5
SEARCH_CACHE_TTL_SEC = max(1.0, float(os.getenv("MEEMEE_SIMILAR_SEARCH_CACHE_TTL_SEC", "15")))
SEARCH_CACHE_MAX_ENTRIES = max(16, int(os.getenv("MEEMEE_SIMILAR_SEARCH_CACHE_MAX_ENTRIES", "128")))
SHAPE_SCORE_WEIGHT = 0.85
MA_PROFILE_SCORE_WEIGHT = 0.10
SLOPE_PROFILE_SCORE_WEIGHT = 0.05
MONTHLY_TOTAL_SCORE_WEIGHT = 0.55
DAILY_SHAPE_TOTAL_SCORE_WEIGHT = 0.45

class SearchResult(BaseModel):
    ticker: str
    asof: str  # YYYY-MM-DD
    score_total: float
    score_monthly: Optional[float] = None
    score_daily: Optional[float] = None
    score60: float
    score24: float
    tag_id: str
    tags: Dict[str, Any]
    vec60: Optional[List[float]] = None
    vec24: Optional[List[float]] = None

class SimilarityService:
    def __init__(self, db_path: str = None):
        self.db_path = os.path.abspath(str(db_path or os.getenv("STOCKS_DB_PATH") or config.DB_PATH))
        self.data_dir = os.path.abspath(DATA_STORE_DIR)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.df_monthly_path = os.path.join(self.data_dir, "monthly_bars.parquet")
        self.df_vec60_path = os.path.join(self.data_dir, "vec60.parquet")
        self.df_vec24_path = os.path.join(self.data_dir, "vec24.parquet")
        self.df_daily_vec_path = os.path.join(self.data_dir, "daily_vec126_ohlc_ma_recent_weighted_v2.parquet")
        self.df_env_path = os.path.join(self.data_dir, "monthly_env.parquet")
        self.tag_index_path = os.path.join(self.data_dir, "tag_index.pkl")

        # In-memory Cache
        self.df_vec60: Optional["pd.DataFrame"] = None
        self.df_vec24: Optional["pd.DataFrame"] = None
        self.df_daily_vec: Optional["pd.DataFrame"] = None
        self.df_env: Optional["pd.DataFrame"] = None
        self.tag_index: Optional[Dict[str, List[int]]] = None
        self.tag_prefix_index: Dict[str, List[int]] = {}
        self.tag_ma60_index: Dict[str, List[int]] = {}
        self.loaded = False
        self._state_lock = RLock()
        self._search_cache: Dict[tuple[str, Optional[str], int, float, bool], tuple[float, List[dict[str, Any]]]] = {}
        self._daily_shape_live_cache: Dict[str, tuple[float, list[dict[str, Any]], "np.ndarray"]] = {}

    def _ensure_daily_long_ma_columns(self, df_daily: "pd.DataFrame") -> "pd.DataFrame":
        missing = {"ma100", "ma200"} - set(df_daily.columns)
        if not missing:
            return df_daily
        required = {"code", "asof", "c"}
        if not required.issubset(df_daily.columns):
            return df_daily
        enriched = df_daily.sort_values(["code", "asof"]).copy()
        grouped_close = enriched.groupby("code", sort=False)["c"]
        if "ma100" in missing:
            enriched["ma100"] = grouped_close.transform(lambda values: values.rolling(100).mean())
        if "ma200" in missing:
            enriched["ma200"] = grouped_close.transform(lambda values: values.rolling(200).mean())
        return enriched

    def _daily_artifact_matches_current_shape(self, df_daily_vec: "pd.DataFrame") -> bool:
        if df_daily_vec.empty or "vec_daily" not in df_daily_vec.columns:
            return True
        sample = df_daily_vec["vec_daily"].dropna()
        if sample.empty:
            return True
        return len(sample.iloc[0]) == RECENT_DAILY_VECTOR_WIDTH

    def _clear_search_cache(self) -> None:
        self._search_cache.clear()
        self._daily_shape_live_cache.clear()

    def _rebuild_tag_lookup_indexes(self) -> None:
        prefix_index: Dict[str, List[int]] = {}
        ma60_index: Dict[str, List[int]] = {}
        source = self.tag_index or {}
        for tag_id, indices in source.items():
            parts = str(tag_id).split("_")
            if len(parts) >= 3:
                prefix_level1 = "_".join(parts[:3])
                prefix_index.setdefault(prefix_level1, []).extend(indices)
            if len(parts) >= 2:
                prefix_level2 = "_".join(parts[:2])
                prefix_index.setdefault(prefix_level2, []).extend(indices)
                ma60_index.setdefault(parts[1], []).extend(indices)
        self.tag_prefix_index = prefix_index
        self.tag_ma60_index = ma60_index

    def _get_cached_search(
        self, key: tuple[str, Optional[str], int, float, bool]
    ) -> Optional[List[SearchResult]]:
        cached = self._search_cache.get(key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= time.monotonic():
            self._search_cache.pop(key, None)
            return None
        return [SearchResult(**item) for item in payload]

    def _store_cached_search(
        self,
        key: tuple[str, Optional[str], int, float, bool],
        results: List[SearchResult],
    ) -> None:
        if len(self._search_cache) >= SEARCH_CACHE_MAX_ENTRIES:
            expired_keys = [
                cache_key
                for cache_key, (expires_at, _) in self._search_cache.items()
                if expires_at <= time.monotonic()
            ]
            for expired_key in expired_keys:
                self._search_cache.pop(expired_key, None)
            if len(self._search_cache) >= SEARCH_CACHE_MAX_ENTRIES:
                oldest_key = next(iter(self._search_cache), None)
                if oldest_key is not None:
                    self._search_cache.pop(oldest_key, None)
        self._search_cache[key] = (
            time.monotonic() + SEARCH_CACHE_TTL_SEC,
            [item.model_dump() for item in results],
        )

    @staticmethod
    def _relative_profile(values: "np.ndarray", denominators: "np.ndarray") -> "np.ndarray":
        if np is None:
            raise RuntimeError("numpy_missing")
        denom = np.where(np.abs(denominators) > 1e-6, np.abs(denominators), np.nan)
        return np.nan_to_num(values / denom, nan=0.0, posinf=0.0, neginf=0.0)

    @staticmethod
    def _normalize_shape_vector(values: "np.ndarray") -> "np.ndarray":
        if np is None:
            raise RuntimeError("numpy_missing")
        matrix = np.asarray(values, dtype=np.float32)
        means = np.mean(matrix, axis=1, keepdims=True)
        stds = np.std(matrix, axis=1, keepdims=True) + 1e-9
        z_scores = (matrix - means) / stds
        l2_norms = np.linalg.norm(z_scores, axis=1, keepdims=True) + 1e-9
        return z_scores / l2_norms

    @staticmethod
    def _recent_daily_shape_weights() -> "np.ndarray":
        if np is None:
            raise RuntimeError("numpy_missing")
        # Daily slices are reversed before weighting: offset 0 is the latest bar.
        return np.linspace(
            RECENT_DAILY_WEIGHT_MAX,
            RECENT_DAILY_WEIGHT_MIN,
            RECENT_DAILY_SHAPE_WINDOW,
            dtype=np.float32,
        )

    def _month_values_to_compare_keys(self, values: "pd.Series") -> "pd.Series":
        if pd is None:
            raise RuntimeError("pandas_missing")
        month_values = values.astype("int64").copy()
        epoch_mask = month_values >= 1_000_000_000
        ymd_mask = (~epoch_mask) & (month_values >= 10_000_000)

        if epoch_mask.any():
            epoch_dt = pd.to_datetime(month_values.loc[epoch_mask], unit="s", utc=True)
            month_values.loc[epoch_mask] = (
                epoch_dt.dt.year * 100 + epoch_dt.dt.month
            ).astype("int64")
        if ymd_mask.any():
            month_values.loc[ymd_mask] = (month_values.loc[ymd_mask] // 100).astype("int64")
        return month_values

    def _date_values_to_compare_keys(self, values: "pd.Series") -> "pd.Series":
        if pd is None:
            raise RuntimeError("pandas_missing")
        date_values = values.astype("int64").copy()
        epoch_mask = date_values >= 1_000_000_000

        if epoch_mask.any():
            epoch_dt = pd.to_datetime(date_values.loc[epoch_mask], unit="s", utc=True)
            date_values.loc[epoch_mask] = (
                epoch_dt.dt.year * 10000 + epoch_dt.dt.month * 100 + epoch_dt.dt.day
            ).astype("int64")
        return date_values

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

    def _load_daily_bars(
        self,
        conn: duckdb.DuckDBPyConnection,
        codes: Optional[list[str]] = None
    ) -> "pd.DataFrame":
        if pd is None:
            raise RuntimeError("pandas_missing")
        if codes:
            placeholders = ",".join(["?"] * len(codes))
            query = f"""
                SELECT b.code, b.date, b.o, b.h, b.l, b.c, m.ma7, m.ma20, m.ma60
                FROM daily_bars b
                LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
                WHERE b.code IN ({placeholders})
                ORDER BY b.code, b.date
            """
            df_daily = conn.execute(query, codes).df()
        else:
            df_daily = conn.execute(
                """
                SELECT b.code, b.date, b.o, b.h, b.l, b.c, m.ma7, m.ma20, m.ma60
                FROM daily_bars b
                LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
                ORDER BY b.code, b.date
                """
            ).df()

        if df_daily.empty:
            return df_daily

        if df_daily["date"].max() >= 1_000_000_000:
            dt = pd.to_datetime(df_daily["date"], unit="s")
        else:
            dt = pd.to_datetime(df_daily["date"].astype(str), format="%Y%m%d")
        df_daily["asof"] = dt.dt.normalize()
        return self._ensure_daily_long_ma_columns(df_daily)

    def _build_vectors_and_tags(
        self, df_monthly: "pd.DataFrame"
    ) -> tuple["pd.DataFrame", "pd.DataFrame", "pd.DataFrame"]:
        if pd is None:
            raise RuntimeError("pandas_missing")
        if np is None:
            raise RuntimeError("numpy_missing")
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
        final_vec60 = self._normalize_shape_vector(matrix_60)

        # subset will be created later after tags, but we already have vec60

        matrix_short = matrix_60[:, :SHORT_TERM_WINDOW]
        final_vec24 = self._normalize_shape_vector(matrix_short)
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

    def _build_daily_vectors_for_env(
        self,
        df_daily: "pd.DataFrame",
        df_env: "pd.DataFrame",
    ) -> "pd.DataFrame":
        if pd is None:
            raise RuntimeError("pandas_missing")
        if np is None:
            raise RuntimeError("numpy_missing")

        env = df_env[["code", "asof"]].reset_index(drop=True).copy()
        zero_vec = np.zeros(RECENT_DAILY_VECTOR_WIDTH, dtype=np.float32)
        vectors: list[np.ndarray] = [zero_vec.copy() for _ in range(len(env))]
        daily_asofs: list[pd.Timestamp | Any] = [pd.NaT for _ in range(len(env))]
        available = [False for _ in range(len(env))]

        if df_daily.empty or env.empty:
            result = env.copy()
            result["daily_asof"] = daily_asofs
            result["daily_shape_available"] = available
            result["vec_daily"] = vectors
            return result

        df_daily = self._ensure_daily_long_ma_columns(df_daily)
        daily = df_daily[["code", "asof", "o", "h", "l", "c", "ma7", "ma20", "ma60", "ma100", "ma200"]].dropna(subset=["o", "h", "l", "c"]).sort_values(["code", "asof"])
        daily_by_code = {str(code): group for code, group in daily.groupby("code", sort=False)}

        for code, env_group in env.groupby("code", sort=False):
            daily_group = daily_by_code.get(str(code))
            if daily_group is None or daily_group.empty:
                continue

            env_dates = env_group["asof"].to_numpy(dtype="datetime64[ns]")
            daily_dates = daily_group["asof"].to_numpy(dtype="datetime64[ns]")
            positions = np.searchsorted(daily_dates, env_dates, side="right") - 1

            for output_idx, daily_pos in zip(env_group.index.to_numpy(), positions):
                built = self._build_recent_daily_shape_vector(daily_group, daily_pos=int(daily_pos))
                if built is None:
                    continue
                vector, daily_asof = built
                vectors[int(output_idx)] = vector
                daily_asofs[int(output_idx)] = daily_asof
                available[int(output_idx)] = True

        result = env.copy()
        result["daily_asof"] = daily_asofs
        result["daily_shape_available"] = available
        result["vec_daily"] = vectors
        return result

    def _build_recent_daily_shape_vector(
        self,
        daily_group: "pd.DataFrame",
        *,
        daily_pos: int,
    ) -> tuple["np.ndarray", "pd.Timestamp"] | None:
        if np is None:
            raise RuntimeError("numpy_missing")
        if daily_pos < RECENT_DAILY_SHAPE_WINDOW:
            return None
        ordered = daily_group.sort_values("asof")
        daily_dates = ordered["asof"].to_numpy(dtype="datetime64[ns]")
        opens = ordered["o"].to_numpy(dtype=np.float32)
        highs = ordered["h"].to_numpy(dtype=np.float32)
        lows = ordered["l"].to_numpy(dtype=np.float32)
        closes = ordered["c"].to_numpy(dtype=np.float32)
        ma7 = ordered["ma7"].to_numpy(dtype=np.float32)
        ma20 = ordered["ma20"].to_numpy(dtype=np.float32)
        ma60 = ordered["ma60"].to_numpy(dtype=np.float32)
        if "ma100" in ordered:
            ma100 = ordered["ma100"].to_numpy(dtype=np.float32)
        else:
            ma100 = ordered["c"].rolling(100).mean().to_numpy(dtype=np.float32)
        if "ma200" in ordered:
            ma200 = ordered["ma200"].to_numpy(dtype=np.float32)
        else:
            ma200 = ordered["c"].rolling(200).mean().to_numpy(dtype=np.float32)
        if closes.shape[0] <= RECENT_DAILY_SHAPE_WINDOW:
            return None

        returns = np.empty_like(closes, dtype=np.float32)
        returns[0] = np.nan
        returns[1:] = (closes[1:] / (closes[:-1] + 1e-9)) - 1.0
        close_base = np.where(np.abs(closes) > 1e-9, np.abs(closes), np.nan)
        ma7_gap = np.where(np.isfinite(ma7), (ma7 / close_base) - 1.0, 0.0).astype(np.float32)
        ma20_gap = np.where(np.isfinite(ma20), (ma20 / close_base) - 1.0, 0.0).astype(np.float32)
        ma60_gap = np.where(np.isfinite(ma60), (ma60 / close_base) - 1.0, 0.0).astype(np.float32)
        ma100_gap = np.where(np.isfinite(ma100), (ma100 / close_base) - 1.0, 0.0).astype(np.float32)
        ma200_gap = np.where(np.isfinite(ma200), (ma200 / close_base) - 1.0, 0.0).astype(np.float32)

        start = int(daily_pos) - RECENT_DAILY_SHAPE_WINDOW + 1
        slices = [
            returns[start:int(daily_pos) + 1],
            ma7_gap[start:int(daily_pos) + 1],
            ma20_gap[start:int(daily_pos) + 1],
            ma60_gap[start:int(daily_pos) + 1],
            ma100_gap[start:int(daily_pos) + 1],
            ma200_gap[start:int(daily_pos) + 1],
        ]
        if any(item.shape[0] != RECENT_DAILY_SHAPE_WINDOW for item in slices):
            return None
        feature_matrix = np.vstack([item[::-1] for item in slices]).astype(np.float32)
        if not np.isfinite(feature_matrix).all():
            return None
        feature_matrix *= self._recent_daily_shape_weights()
        vector = self._normalize_shape_vector(feature_matrix.reshape(1, -1))[0].astype(np.float32)
        return vector, pd.Timestamp(daily_dates[int(daily_pos)]).normalize()

    def _persist_artifacts(self) -> None:
        if pd is None:
            raise RuntimeError("pandas_missing")
        self.df_vec60 = self.df_vec60.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_vec24 = self.df_vec24.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_daily_vec = self.df_daily_vec.sort_values(["code", "asof"]).reset_index(drop=True)
        self.df_env = self.df_env.sort_values(["code", "asof"]).reset_index(drop=True)

        self.df_vec60.to_parquet(self.df_vec60_path, index=False)
        self.df_vec24.to_parquet(self.df_vec24_path, index=False)
        self.df_daily_vec.to_parquet(self.df_daily_vec_path, index=False)
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
        self._rebuild_tag_lookup_indexes()
        self.loaded = True

    def load_artifacts(self):
        """Load pre-computed data into memory."""
        with self._state_lock:
            if self.loaded:
                return
            if pd is None:
                raise RuntimeError("pandas_missing")

            required_paths = [
                self.df_vec60_path,
                self.df_vec24_path,
                self.df_daily_vec_path,
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
                        f"Similarity artifacts not found: {missing_list}. refresh_data failed: {exc}"
                    ) from exc
                missing = [path for path in required_paths if not os.path.exists(path)]
                if missing:
                    missing_list = ", ".join(missing)
                    raise FileNotFoundError(
                        f"Similarity artifacts not found after refresh_data(): {missing_list}"
                    )

            try:
                self.df_vec60 = pd.read_parquet(self.df_vec60_path)
                self.df_vec24 = pd.read_parquet(self.df_vec24_path)
                self.df_daily_vec = pd.read_parquet(self.df_daily_vec_path)
                self.df_env = pd.read_parquet(self.df_env_path)
                if not self._daily_artifact_matches_current_shape(self.df_daily_vec):
                    print("Daily similarity artifact shape is stale; rebuilding artifacts.")
                    self.refresh_data(incremental=False)
                    self.df_vec60 = pd.read_parquet(self.df_vec60_path)
                    self.df_vec24 = pd.read_parquet(self.df_vec24_path)
                    self.df_daily_vec = pd.read_parquet(self.df_daily_vec_path)
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

            self._rebuild_tag_lookup_indexes()
            self.loaded = True
            self._clear_search_cache()

    def refresh_data(self, incremental: bool = False):
        """Rebuilds the similarity dataset from DuckDB. Incremental updates are supported."""
        with self._state_lock:
            print("Starting Refresh Data...")
            self._clear_search_cache()
            if pd is None:
                raise RuntimeError("pandas_missing")

            incremental_required_paths = [
                self.df_vec60_path,
                self.df_vec24_path,
                self.df_daily_vec_path,
                self.df_env_path,
                self.tag_index_path,
            ]
            if incremental and not all(os.path.exists(path) for path in incremental_required_paths):
                incremental = False

            if incremental:
                try:
                    self.load_artifacts()
                except Exception as exc:
                    print(f"Incremental refresh fallback to full: {exc}")
                    incremental = False

            with get_conn_for_path(self.db_path, timeout_sec=2.5, read_only=True) as conn:
                if incremental:
                    db_max = conn.execute(
                        """
                        SELECT code, MAX(month) AS max_month
                        FROM monthly_bars
                        GROUP BY code
                        HAVING COUNT(*) >= 120
                        """
                    ).df()
                    db_daily_max = conn.execute(
                        "SELECT code, MAX(date) AS max_date FROM daily_bars GROUP BY code"
                    ).df()
                    if db_max.empty:
                        raise RuntimeError(
                            "monthly_bars table is empty. Import monthly data before building similarity artifacts."
                        )
                    if db_daily_max.empty:
                        raise RuntimeError(
                            "daily_bars table is empty. Import daily data before building similarity artifacts."
                        )

                    db_max["month_key_db"] = self._month_values_to_compare_keys(db_max["max_month"])
                    db_daily_max["daily_key_db"] = self._date_values_to_compare_keys(db_daily_max["max_date"])
                    existing = self.df_env[["code", "asof"]].copy()
                    existing["month_key_existing"] = (
                        existing["asof"].dt.year * 100 + existing["asof"].dt.month
                    )
                    existing["month_asof_key_existing"] = (
                        existing["asof"].dt.year * 10000
                        + existing["asof"].dt.month * 100
                        + existing["asof"].dt.day
                    )
                    existing_max = existing.groupby("code", as_index=False).agg(
                        month_key_existing=("month_key_existing", "max"),
                        month_asof_key_existing=("month_asof_key_existing", "max"),
                    )
                    existing_daily = self.df_daily_vec[["code", "daily_asof"]].copy()
                    existing_daily["daily_key_existing"] = (
                        existing_daily["daily_asof"].dt.year * 10000
                        + existing_daily["daily_asof"].dt.month * 100
                        + existing_daily["daily_asof"].dt.day
                    )
                    existing_daily_max = existing_daily.groupby("code", as_index=False)["daily_key_existing"].max()

                    merged = db_max.merge(existing_max, on="code", how="left")
                    merged = merged.merge(db_daily_max[["code", "daily_key_db"]], on="code", how="left")
                    merged = merged.merge(existing_daily_max, on="code", how="left")
                    merged["daily_key_needed"] = merged[["daily_key_db", "month_asof_key_existing"]].min(axis=1)
                    current_month_key = int(pd.Timestamp.now(tz="Asia/Tokyo").strftime("%Y%m"))
                    daily_stale = (
                        merged["month_key_existing"].fillna(0).astype("int64") >= current_month_key
                    ) & (
                        merged["daily_key_existing"].isna()
                        | (merged["daily_key_needed"] > merged["daily_key_existing"])
                    )
                    needs_update = merged[
                        merged["month_key_existing"].isna() |
                        (merged["month_key_db"] > merged["month_key_existing"]) |
                        daily_stale
                    ]
                    codes_to_update = needs_update["code"].tolist()

                    if not codes_to_update:
                        print("No incremental updates needed.")
                        return

                    df_monthly = self._load_monthly_bars(conn, codes_to_update)
                    if df_monthly.empty:
                        raise RuntimeError(
                            "monthly_bars returned no rows for incremental similarity refresh."
                        )

                    counts = df_monthly["code"].value_counts()
                    valid_codes = set(counts[counts >= 120].index)
                    insufficient_codes = [code for code in codes_to_update if code not in valid_codes]
                    df_monthly = df_monthly[df_monthly["code"].isin(valid_codes)].copy()

                    if insufficient_codes:
                        self.df_vec60 = self.df_vec60[~self.df_vec60["code"].isin(insufficient_codes)]
                        self.df_vec24 = self.df_vec24[~self.df_vec24["code"].isin(insufficient_codes)]
                        self.df_daily_vec = self.df_daily_vec[~self.df_daily_vec["code"].isin(insufficient_codes)]
                        self.df_env = self.df_env[~self.df_env["code"].isin(insufficient_codes)]

                    if df_monthly.empty:
                        self._persist_artifacts()
                        print("Incremental Refresh Complete (removals only).")
                        return

                    print(f"Incremental rebuild for {len(valid_codes)} tickers.")
                    df_vec60_new, df_vec24_new, df_env_new = self._build_vectors_and_tags(df_monthly)
                    df_daily = self._load_daily_bars(conn, sorted(valid_codes))
                    if df_daily.empty:
                        raise RuntimeError(
                            "daily_bars returned no rows for incremental similarity refresh."
                        )
                    df_daily_vec_new = self._build_daily_vectors_for_env(df_daily, df_env_new)

                    drop_codes = set(codes_to_update)
                    self.df_vec60 = self.df_vec60[~self.df_vec60["code"].isin(drop_codes)]
                    self.df_vec24 = self.df_vec24[~self.df_vec24["code"].isin(drop_codes)]
                    self.df_daily_vec = self.df_daily_vec[~self.df_daily_vec["code"].isin(drop_codes)]
                    self.df_env = self.df_env[~self.df_env["code"].isin(drop_codes)]

                    self.df_vec60 = pd.concat([self.df_vec60, df_vec60_new], ignore_index=True)
                    self.df_vec24 = pd.concat([self.df_vec24, df_vec24_new], ignore_index=True)
                    self.df_daily_vec = pd.concat([self.df_daily_vec, df_daily_vec_new], ignore_index=True)
                    self.df_env = pd.concat([self.df_env, df_env_new], ignore_index=True)

                    self._persist_artifacts()
                    print("Incremental Refresh Complete.")
                    return

                df_monthly = self._load_monthly_bars(conn)
                if df_monthly.empty:
                    raise RuntimeError(
                        "monthly_bars table is empty. Import monthly data before building similarity artifacts."
                    )

                counts = df_monthly["code"].value_counts()
                valid_codes = counts[counts >= 120].index
                df_monthly = df_monthly[df_monthly["code"].isin(valid_codes)].copy()
                if df_monthly.empty:
                    raise RuntimeError(
                        "No tickers have the 120 months of history required for similarity search."
                    )
                df_daily = self._load_daily_bars(conn, list(valid_codes))
                if df_daily.empty:
                    raise RuntimeError(
                        "daily_bars table is empty. Import daily data before building similarity artifacts."
                    )

            print(f"Monthly Bars loaded: {len(df_monthly)} rows, {len(valid_codes)} tickers.")
            print("Generating monthly and daily vectors and tags...")

            self.df_vec60, self.df_vec24, self.df_env = self._build_vectors_and_tags(df_monthly)
            self.df_daily_vec = self._build_daily_vectors_for_env(df_daily, self.df_env)
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
        if np is None:
            raise RuntimeError("numpy_missing")
        cache_key = (ticker, asof, int(k), round(float(alpha), 4), bool(match_tag))

        with self._state_lock:
            cached = self._get_cached_search(cache_key)
            if cached is not None:
                return cached
            if not self.loaded:
                self.load_artifacts()
            if (
                not self.loaded
                or self.df_env is None
                or self.df_vec60 is None
                or self.df_vec24 is None
                or self.df_daily_vec is None
                or self.tag_index is None
            ):
                raise RuntimeError("Similarity artifacts are not loaded. Run refresh_data() first.")
            df_env = self.df_env
            df_vec60 = self.df_vec60
            df_vec24 = self.df_vec24
            df_daily_vec = self.df_daily_vec
            tag_index = self.tag_index
             
        # Parse AsOf
        if asof:
            # Try parsing YYYY-MM-DD
            target_date = pd.to_datetime(asof)
        else:
            # Use latest available date for the ticker
            target_date = None
            
        # Find Query Row
        query_mask = (df_env["code"] == ticker)
        df_ticker = df_env[query_mask].copy()

        if alpha <= 0.05:
            results = self._search_recent_daily_shape_only(
                ticker=ticker,
                target_date=target_date,
                k=k,
                df_env=df_env,
                df_daily_vec=df_daily_vec,
            )
            with self._state_lock:
                self._store_cached_search(cache_key, results)
            return results
        
        if df_ticker.empty:
            results = self._search_recent_daily_shape_only(
                ticker=ticker,
                target_date=target_date,
                k=k,
                df_env=df_env,
                df_daily_vec=df_daily_vec,
            )
            with self._state_lock:
                self._store_cached_search(cache_key, results)
            return results
            
        if target_date:
            historical_match = df_ticker[df_ticker["asof"] <= target_date]
            if historical_match.empty:
                raise ValueError(f"Ticker {ticker} has no indexed history on or before {asof}")
            query_idx = historical_match.index[-1]
        else:
            # Latest
            # Assumes sorted by time
            query_idx = df_ticker.index[-1]
            
        query_row = df_env.loc[query_idx]
        query_tag = query_row["tag_id"]
        query_vec60 = np.array(df_vec60.loc[query_idx]["vec60"], dtype=np.float32)
        query_vec24 = np.array(df_vec24.loc[query_idx]["vec24"], dtype=np.float32)
        query_daily_row = df_daily_vec.loc[query_idx]
        if not bool(query_daily_row.get("daily_shape_available", False)):
            raise RuntimeError(
                f"Daily similarity vector missing for {ticker} asof {query_row['asof']}. "
                "Run refresh_data() after daily_bars import."
            )
        query_vec_daily = np.array(query_daily_row["vec_daily"], dtype=np.float32)
        query_asof = target_date if target_date is not None else query_row["asof"]

        def filter_to_query_asof(indices: List[int]) -> List[int]:
            if not indices:
                return []
            candidate_asofs = df_env.loc[indices]["asof"]
            allowed = candidate_asofs <= query_asof
            return [int(idx) for idx, keep in zip(indices, allowed.to_list()) if bool(keep)]
        
        # Fallback Levels
        # 0: Exact Tag
        candidates_indices = filter_to_query_asof(tag_index.get(query_tag, []))
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
            candidates_indices = filter_to_query_asof(self._find_indices_prefix(prefix))
            level_desc = "Level 1 (Ignore Range)"
            
        if len(candidates_indices) < k:
            # Level 2: Ignore Dir
            # Pattern: MA20_MA60_*
            prefix = "_".join(parts[:2])
            candidates_indices = filter_to_query_asof(self._find_indices_prefix(prefix))
            level_desc = "Level 2 (Ignore Dir)"
            
        if len(candidates_indices) < k:
            # Level 3: MA60 only? User: "60MA上下のみ一致"
            # Pattern: *_MA60_*? 
            # User spec: "Level 3: 60MA上下のみ一致".
            # Tag: MA20_MA60_DIR_RANGE.
            # So we match the 2nd part.
            target_ma60 = parts[1]
            candidates_indices = filter_to_query_asof(self._find_indices_ma60(target_ma60))
            level_desc = "Level 3 (MA60 Only)"
            
        if len(candidates_indices) < k:
             # Level 4: All
             candidates_indices = filter_to_query_asof(list(df_env.index))
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
        cand_env = df_env.loc[candidates_indices]
        cand_vec60 = np.vstack(df_vec60.loc[candidates_indices]["vec60"].values) # (N, 60)
        cand_vec24 = np.vstack(df_vec24.loc[candidates_indices]["vec24"].values) # (N, 24)
        cand_daily_rows = df_daily_vec.loc[candidates_indices]
        cand_vec_daily = np.vstack(cand_daily_rows["vec_daily"].values)

        # Dot Product base
        s60 = np.dot(cand_vec60, query_vec60)
        s24 = np.dot(cand_vec24, query_vec24)
        monthly_shape_scores = alpha * s60 + (1 - alpha) * s24
        s_daily = np.dot(cand_vec_daily, query_vec_daily)
        daily_available = cand_daily_rows["daily_shape_available"].fillna(False).to_numpy(dtype=bool)
        s_daily = np.where(daily_available, s_daily, 0.0)

        # Monthly MA similarity boost. Compare MA profiles as ratios so search is
        # shape-scale invariant across different nominal share prices.
        ma_cols = ["ma7", "ma20", "ma60", "ma100"]
        slope_cols = [f"{col}_slope" for col in ma_cols]
        query_ma = np.nan_to_num(query_row[ma_cols].to_numpy(dtype=np.float32), nan=0.0)
        query_slopes = np.nan_to_num(query_row[slope_cols].to_numpy(dtype=np.float32), nan=0.0)
        cand_ma = np.nan_to_num(cand_env[ma_cols].to_numpy(dtype=np.float32), nan=0.0)
        cand_slopes = np.nan_to_num(cand_env[slope_cols].to_numpy(dtype=np.float32), nan=0.0)
        query_close = np.array([[float(query_row["c"])]], dtype=np.float32)
        cand_close = cand_env["c"].to_numpy(dtype=np.float32).reshape(-1, 1)
        query_ma_profile = self._relative_profile(query_ma.reshape(1, -1), query_close)[0]
        cand_ma_profile = self._relative_profile(cand_ma, cand_close)
        query_slope_profile = self._relative_profile(query_slopes.reshape(1, -1), query_ma.reshape(1, -1))[0]
        cand_slope_profile = self._relative_profile(cand_slopes, cand_ma)
        ma_diff = np.abs(cand_ma_profile - query_ma_profile)
        slope_diff = np.abs(cand_slope_profile - query_slope_profile)
        ma_similarity = np.exp(-np.sum(ma_diff, axis=1))
        slope_similarity = np.exp(-np.sum(slope_diff, axis=1))
        monthly_scores = (
            SHAPE_SCORE_WEIGHT * monthly_shape_scores
            + MA_PROFILE_SCORE_WEIGHT * ma_similarity
            + SLOPE_PROFILE_SCORE_WEIGHT * slope_similarity
        )
        scores = (
            MONTHLY_TOTAL_SCORE_WEIGHT * monthly_scores
            + DAILY_SHAPE_TOTAL_SCORE_WEIGHT * s_daily
        )
        scores = np.minimum(scores, 1.0)
        
        # Build Result DF
        df_res = pd.DataFrame({
            "idx": candidates_indices,
            "score": scores,
            "score_monthly": monthly_scores,
            "score_daily": s_daily,
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
            row_meta = df_env.iloc[idx]
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
                score_monthly=float(r["score_monthly"]),
                score_daily=float(r["score_daily"]),
                score60=float(r["s60"]),
                score24=float(r["s24"]),
                tag_id=row_meta["tag_id"],
                tags={
                    "ma20": str(row_meta["tag_ma20"]),
                    "ma60": str(row_meta["tag_ma60"]),
                    "dir": str(row_meta["tag_dir60"]),
                    "range": str(row_meta["tag_range"]),
                    "fallback": str(level_desc),
                    "daily_shape": "available" if bool(df_daily_vec.iloc[idx]["daily_shape_available"]) else "missing",
                    "daily_shape_window_days": RECENT_DAILY_SHAPE_WINDOW,
                    "daily_shape_features": RECENT_DAILY_SHAPE_FEATURES,
                    "daily_shape_weighting": RECENT_DAILY_WEIGHTING,
                    "daily_asof": (
                        df_daily_vec.iloc[idx]["daily_asof"].strftime("%Y-%m-%d")
                        if pd.notna(df_daily_vec.iloc[idx]["daily_asof"])
                        else None
                    )
                },
                vec60=[float(x) for x in df_vec60.iloc[idx]["vec60"]],
                vec24=[float(x) for x in df_vec24.iloc[idx]["vec24"]]
            ))

        with self._state_lock:
            self._store_cached_search(cache_key, final_results)
        return final_results

    def _search_recent_daily_shape_only(
        self,
        *,
        ticker: str,
        target_date: Optional["pd.Timestamp"],
        k: int,
        df_env: "pd.DataFrame",
        df_daily_vec: "pd.DataFrame",
    ) -> List[SearchResult]:
        if pd is None:
            raise RuntimeError("pandas_missing")
        if np is None:
            raise RuntimeError("numpy_missing")
        artifact_results = self._search_recent_daily_shape_from_artifacts(
            ticker=ticker,
            target_date=target_date,
            k=k,
            df_env=df_env,
            df_daily_vec=df_daily_vec,
        )
        if artifact_results is not None:
            return artifact_results
        with get_conn_for_path(self.db_path, timeout_sec=2.5, read_only=True) as conn:
            df_daily_query = self._load_daily_bars(conn, [ticker])
            df_daily_all = self._load_daily_bars(conn, None)
        if df_daily_query.empty:
            raise ValueError(f"Ticker {ticker} has no daily history for recent shape search")
        df_daily_query = self._ensure_daily_long_ma_columns(df_daily_query)
        df_daily_all = self._ensure_daily_long_ma_columns(df_daily_all)
        daily_query = df_daily_query[["code", "asof", "o", "h", "l", "c", "ma7", "ma20", "ma60", "ma100", "ma200"]].dropna(subset=["o", "h", "l", "c"]).sort_values(["code", "asof"])
        if daily_query.empty:
            raise ValueError(f"Ticker {ticker} has no valid daily OHLC history for recent shape search")
        query_date = target_date if target_date is not None else daily_query["asof"].max()
        allowed_daily = daily_query[daily_query["asof"] <= query_date]
        if allowed_daily.empty:
            asof_text = query_date.strftime("%Y-%m-%d") if hasattr(query_date, "strftime") else str(query_date)
            raise ValueError(f"Ticker {ticker} has no daily history on or before {asof_text}")
        daily_pos = int(len(allowed_daily) - 1)
        built = self._build_recent_daily_shape_vector(allowed_daily, daily_pos=daily_pos)
        if built is None:
            raise ValueError(
                f"Ticker {ticker} needs at least {RECENT_DAILY_SHAPE_WINDOW + 1} daily bars for recent shape search"
            )
        query_vec_daily, query_daily_asof = built

        candidate_rows, cand_vec_daily = self._get_live_daily_shape_candidates(
            query_daily_asof=query_daily_asof,
            df_env=df_env,
            df_daily_all=df_daily_all,
        )
        if not candidate_rows or cand_vec_daily.size == 0:
            return []

        s_daily = np.dot(cand_vec_daily, query_vec_daily)
        scores = np.minimum(s_daily, 1.0)
        df_res = pd.DataFrame({
            "idx": list(range(len(candidate_rows))),
            "score": scores,
            "score_daily": s_daily,
        }).sort_values("score", ascending=False)

        final_results: List[SearchResult] = []
        seen_codes: Dict[str, list[Any]] = {}
        for _, r in df_res.iterrows():
            if len(final_results) >= k:
                break
            idx = int(r["idx"])
            candidate = candidate_rows[idx]
            row_meta = candidate["row_meta"]
            code = str(candidate["code"])
            if code == ticker:
                continue
            daily_asof = candidate["daily_asof"]
            r_asof = daily_asof.normalize() if hasattr(daily_asof, "normalize") else pd.Timestamp(daily_asof).normalize()
            if code in seen_codes:
                if len(seen_codes[code]) >= 3:
                    continue
                if any(abs((r_asof - existing_ts).days) < 60 for existing_ts in seen_codes[code]):
                    continue
            seen_codes.setdefault(code, []).append(r_asof)
            final_results.append(SearchResult(
                ticker=code,
                asof=r_asof.strftime("%Y-%m-%d"),
                score_total=float(r["score"]),
                score_monthly=None,
                score_daily=float(r["score_daily"]),
                score60=0.0,
                score24=0.0,
                tag_id=str(row_meta["tag_id"]),
                tags={
                    "ma20": str(row_meta["tag_ma20"]),
                    "ma60": str(row_meta["tag_ma60"]),
                    "dir": str(row_meta["tag_dir60"]),
                    "range": str(row_meta["tag_range"]),
                    "fallback": "recent_daily_shape_only",
                    "query_daily_asof": query_daily_asof.strftime("%Y-%m-%d"),
                    "daily_shape": "available",
                    "daily_shape_window_days": RECENT_DAILY_SHAPE_WINDOW,
                    "daily_shape_features": RECENT_DAILY_SHAPE_FEATURES,
                    "daily_shape_weighting": RECENT_DAILY_WEIGHTING,
                    "daily_asof": r_asof.strftime("%Y-%m-%d"),
                },
                vec60=None,
                vec24=None,
            ))
        return final_results

    def _search_recent_daily_shape_from_artifacts(
        self,
        *,
        ticker: str,
        target_date: Optional["pd.Timestamp"] = None,
        k: int,
        df_env: "pd.DataFrame",
        df_daily_vec: "pd.DataFrame",
    ) -> Optional[List[SearchResult]]:
        query_rows = df_env.index[df_env["code"] == ticker].tolist()
        if not query_rows:
            return None
        query_candidates = df_daily_vec.loc[query_rows]
        query_candidates = query_candidates[query_candidates["daily_shape_available"].astype(bool)]
        if target_date is not None:
            query_candidates = query_candidates[query_candidates["daily_asof"] <= target_date]
        if query_candidates.empty:
            return None
        query_idx = int(query_candidates.sort_values("daily_asof").index[-1])
        query_vec_daily = np.array(df_daily_vec.loc[query_idx]["vec_daily"], dtype=np.float32)
        query_daily_asof = pd.Timestamp(df_daily_vec.loc[query_idx]["daily_asof"]).normalize()

        candidate_mask = (
            df_daily_vec["daily_shape_available"].astype(bool)
            & (df_daily_vec["daily_asof"] <= query_daily_asof)
        )
        candidate_frame = df_daily_vec.loc[candidate_mask, ["code", "daily_asof"]].copy()
        if candidate_frame.empty:
            return []
        candidate_indices = candidate_frame.index.astype(int).tolist()
        if not candidate_indices:
            return []
        cand_vec_daily = np.vstack(df_daily_vec.loc[candidate_indices]["vec_daily"].values)
        s_daily = np.dot(cand_vec_daily, query_vec_daily)
        scores = np.minimum(s_daily, 1.0)
        df_res = pd.DataFrame({
            "idx": candidate_indices,
            "score": scores,
            "score_daily": s_daily,
        }).sort_values("score", ascending=False)

        final_results: List[SearchResult] = []
        seen_codes: Dict[str, list[Any]] = {}
        for _, r in df_res.iterrows():
            if len(final_results) >= k:
                break
            idx = int(r["idx"])
            row_meta = df_env.loc[idx]
            code = str(row_meta["code"])
            if code == ticker:
                continue
            daily_asof = pd.Timestamp(df_daily_vec.loc[idx]["daily_asof"]).normalize()
            if code in seen_codes:
                if len(seen_codes[code]) >= 3:
                    continue
                if any(abs((daily_asof - existing_ts).days) < 60 for existing_ts in seen_codes[code]):
                    continue
            seen_codes.setdefault(code, []).append(daily_asof)
            final_results.append(SearchResult(
                ticker=code,
                asof=daily_asof.strftime("%Y-%m-%d"),
                score_total=float(r["score"]),
                score_monthly=None,
                score_daily=float(r["score_daily"]),
                score60=0.0,
                score24=0.0,
                tag_id=str(row_meta["tag_id"]),
                tags={
                    "ma20": str(row_meta["tag_ma20"]),
                    "ma60": str(row_meta["tag_ma60"]),
                    "dir": str(row_meta["tag_dir60"]),
                    "range": str(row_meta["tag_range"]),
                    "fallback": "recent_daily_shape_artifact",
                    "query_daily_asof": query_daily_asof.strftime("%Y-%m-%d"),
                    "daily_shape": "available",
                    "daily_shape_window_days": RECENT_DAILY_SHAPE_WINDOW,
                    "daily_shape_features": RECENT_DAILY_SHAPE_FEATURES,
                    "daily_shape_weighting": RECENT_DAILY_WEIGHTING,
                    "daily_asof": daily_asof.strftime("%Y-%m-%d"),
                },
                vec60=None,
                vec24=None,
            ))
        return final_results

    def prewarm_recent_daily_shape_cache(self, asof: Optional[str] = None) -> dict[str, Any]:
        if pd is None:
            raise RuntimeError("pandas_missing")
        if np is None:
            raise RuntimeError("numpy_missing")
        with self._state_lock:
            if not self.loaded:
                self.load_artifacts()
            if not self.loaded or self.df_env is None:
                raise RuntimeError("Similarity artifacts are not loaded. Run refresh_data() first.")
            df_env = self.df_env
        target_date = pd.to_datetime(asof) if asof else None
        with get_conn_for_path(self.db_path, timeout_sec=2.5, read_only=True) as conn:
            df_daily_all = self._load_daily_bars(conn, None)
        df_daily_all = self._ensure_daily_long_ma_columns(df_daily_all)
        daily_all = df_daily_all[["code", "asof", "o", "h", "l", "c", "ma7", "ma20", "ma60", "ma100", "ma200"]].dropna(subset=["o", "h", "l", "c"]).sort_values(["code", "asof"])
        if daily_all.empty:
            raise ValueError("No valid daily OHLC history for recent shape cache prewarm")
        query_daily_asof = target_date if target_date is not None else daily_all["asof"].max()
        available = daily_all[daily_all["asof"] <= query_daily_asof]
        if available.empty:
            asof_text = query_daily_asof.strftime("%Y-%m-%d") if hasattr(query_daily_asof, "strftime") else str(query_daily_asof)
            raise ValueError(f"No daily history on or before {asof_text}")
        query_daily_asof = available["asof"].max().normalize()
        candidate_rows, cand_vec_daily = self._get_live_daily_shape_candidates(
            query_daily_asof=query_daily_asof,
            df_env=df_env,
            df_daily_all=df_daily_all,
        )
        return {
            "ok": True,
            "asof": query_daily_asof.strftime("%Y-%m-%d"),
            "candidate_count": len(candidate_rows),
            "vector_shape": list(cand_vec_daily.shape),
            "cache_ttl_sec": SEARCH_CACHE_TTL_SEC,
        }

    def _get_live_daily_shape_candidates(
        self,
        *,
        query_daily_asof: "pd.Timestamp",
        df_env: "pd.DataFrame",
        df_daily_all: "pd.DataFrame",
    ) -> tuple[list[dict[str, Any]], "np.ndarray"]:
        if pd is None:
            raise RuntimeError("pandas_missing")
        if np is None:
            raise RuntimeError("numpy_missing")
        cache_key = query_daily_asof.strftime("%Y-%m-%d")
        cached = self._daily_shape_live_cache.get(cache_key)
        if cached is not None:
            expires_at, candidate_rows, cand_vec_daily = cached
            if expires_at > time.monotonic():
                return candidate_rows, cand_vec_daily
            self._daily_shape_live_cache.pop(cache_key, None)

        df_daily_all = self._ensure_daily_long_ma_columns(df_daily_all)
        daily_all = df_daily_all[["code", "asof", "o", "h", "l", "c", "ma7", "ma20", "ma60", "ma100", "ma200"]].dropna(subset=["o", "h", "l", "c"]).sort_values(["code", "asof"])
        if daily_all.empty:
            empty = np.empty((0, RECENT_DAILY_VECTOR_WIDTH), dtype=np.float32)
            return [], empty
        env_by_code: Dict[str, "pd.DataFrame"] = {
            str(code): group.sort_values("asof")
            for code, group in df_env.groupby("code", sort=False)
        }
        candidate_rows: list[dict[str, Any]] = []
        candidate_vectors: list["np.ndarray"] = []
        for code_value, daily_group in daily_all.groupby("code", sort=False):
            code = str(code_value)
            env_group = env_by_code.get(code)
            if env_group is None or env_group.empty:
                continue
            candidate_env = env_group[env_group["asof"] <= query_daily_asof]
            daily_dates = daily_group["asof"].to_numpy(dtype="datetime64[ns]")
            if candidate_env.empty:
                allowed_candidate = daily_group[daily_group["asof"] <= query_daily_asof]
                if allowed_candidate.empty:
                    continue
                built_candidate = self._build_recent_daily_shape_vector(
                    allowed_candidate,
                    daily_pos=int(len(allowed_candidate) - 1),
                )
                if built_candidate is None:
                    continue
                vector, daily_asof = built_candidate
                candidate_rows.append({"code": code, "daily_asof": daily_asof, "row_meta": env_group.iloc[0]})
                candidate_vectors.append(vector)
                continue
            env_dates = candidate_env["asof"].to_numpy(dtype="datetime64[ns]")
            positions = np.searchsorted(daily_dates, env_dates, side="right") - 1
            for (_, row_meta), daily_pos in zip(candidate_env.iterrows(), positions):
                if int(daily_pos) < 0:
                    continue
                built_candidate = self._build_recent_daily_shape_vector(
                    daily_group,
                    daily_pos=int(daily_pos),
                )
                if built_candidate is None:
                    continue
                vector, daily_asof = built_candidate
                candidate_rows.append({"code": code, "daily_asof": daily_asof, "row_meta": row_meta})
                candidate_vectors.append(vector)
        if not candidate_vectors:
            empty = np.empty((0, RECENT_DAILY_VECTOR_WIDTH), dtype=np.float32)
            return [], empty
        cand_vec_daily = np.vstack(candidate_vectors)
        if len(self._daily_shape_live_cache) >= SEARCH_CACHE_MAX_ENTRIES:
            self._daily_shape_live_cache.clear()
        self._daily_shape_live_cache[cache_key] = (
            time.monotonic() + SEARCH_CACHE_TTL_SEC,
            candidate_rows,
            cand_vec_daily,
        )
        return candidate_rows, cand_vec_daily

    def _find_indices_prefix(self, prefix, tag_index: Optional[Dict[str, List[int]]] = None):
        if tag_index is None:
            return list(self.tag_prefix_index.get(str(prefix), []))
        res = []
        for k, v in tag_index.items():
            if k.startswith(prefix):
                res.extend(v)
        return res

    def _find_indices_ma60(self, ma60_val, tag_index: Optional[Dict[str, List[int]]] = None):
        if tag_index is None:
            return list(self.tag_ma60_index.get(str(ma60_val), []))
        res = []
        for k, v in tag_index.items():
            parts = k.split("_")
            if len(parts) >= 2 and parts[1] == ma60_val:
                res.extend(v)
        return res

if __name__ == "__main__":
    # Test script
    svc = SimilarityService()
    svc.refresh_data()
