from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


def write_parquet_frame(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    try:
        conn.register("frame_df", frame)
        conn.execute("COPY frame_df TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        conn.close()
    return path


def read_parquet_frame(path: Path) -> pd.DataFrame:
    conn = duckdb.connect()
    try:
        return conn.execute("SELECT * FROM read_parquet(?)", [str(path)]).df()
    finally:
        conn.close()


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return float(number)
