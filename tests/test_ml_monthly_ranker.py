from __future__ import annotations

import os
import sys
from typing import Any

import duckdb
import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import ml_service


def _seed_monthly_label_inputs(conn: duckdb.DuckDBPyConnection) -> None:
    ml_service._ensure_ml_schema(conn)  # type: ignore[attr-defined]
    conn.execute(
        """
        CREATE TABLE monthly_bars (
            code TEXT,
            month INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT
        )
        """
    )
    codes = [f"{2000 + i}" for i in range(20)]
    returns = np.linspace(-0.12, 0.12, num=20, dtype=float)
    feature_rows: list[tuple[Any, ...]] = []
    monthly_rows: list[tuple[Any, ...]] = []
    for idx, code in enumerate(codes):
        turnover = float(idx + 1)
        feature_rows.append((20240131, code, turnover, 3))
        close_1 = 100.0
        close_2 = close_1 * (1.0 + float(returns[idx]))
        monthly_rows.append((code, 202401, close_1, close_1, close_1, close_1, 1000))
        monthly_rows.append((code, 202402, close_2, close_2, close_2, close_2, 1000))
    conn.executemany(
        """
        INSERT INTO ml_feature_daily (dt, code, turnover20, feature_version, computed_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        feature_rows,
    )
    conn.executemany(
        """
        INSERT INTO monthly_bars (code, month, o, h, l, c, v)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        monthly_rows,
    )


def test_refresh_ml_monthly_label_table_quantile_labels() -> None:
    previous = os.environ.get("MEEMEE_DISABLE_LEGACY_ANALYSIS")
    os.environ["MEEMEE_DISABLE_LEGACY_ANALYSIS"] = "0"
    with duckdb.connect(":memory:") as conn:
        _seed_monthly_label_inputs(conn)
        inserted = ml_service.refresh_ml_monthly_label_table(conn)
        assert inserted == 20
        labels = conn.execute(
            """
            SELECT code, ret1m, up_big, down_big, liquidity_pass
            FROM ml_monthly_label
            WHERE dt = 20240101
            ORDER BY code
            """
        ).fetchdf()
    passed = labels[labels["liquidity_pass"] == 1]
    pass_rets = passed["ret1m"].to_numpy(dtype=float)
    up_threshold = float(np.nanquantile(pass_rets, 1.0 - ml_service.MONTHLY_LABEL_QUANTILE))
    down_threshold = float(np.nanquantile(pass_rets, ml_service.MONTHLY_LABEL_QUANTILE))
    up_rows = passed[passed["up_big"] == 1]
    down_rows = passed[passed["down_big"] == 1]
    assert len(up_rows) > 0
    assert len(down_rows) > 0
    assert bool((up_rows["ret1m"] >= up_threshold - 1e-12).all())
    assert bool((down_rows["ret1m"] <= down_threshold + 1e-12).all())
    excluded = labels[labels["liquidity_pass"] == 0]
    assert int(excluded["up_big"].sum()) == 0
    assert int(excluded["down_big"].sum()) == 0
    if previous is None:
        os.environ.pop("MEEMEE_DISABLE_LEGACY_ANALYSIS", None)
    else:
        os.environ["MEEMEE_DISABLE_LEGACY_ANALYSIS"] = previous


def test_refresh_ml_monthly_label_table_liquidity_filter_bottom30() -> None:
    previous = os.environ.get("MEEMEE_DISABLE_LEGACY_ANALYSIS")
    os.environ["MEEMEE_DISABLE_LEGACY_ANALYSIS"] = "0"
    with duckdb.connect(":memory:") as conn:
        _seed_monthly_label_inputs(conn)
        ml_service.refresh_ml_monthly_label_table(conn)
        row = conn.execute(
            "SELECT COUNT(*) FROM ml_monthly_label WHERE dt = 20240101 AND liquidity_pass = 0"
        ).fetchone()
    assert int(row[0]) == 6
    if previous is None:
        os.environ.pop("MEEMEE_DISABLE_LEGACY_ANALYSIS", None)
    else:
        os.environ["MEEMEE_DISABLE_LEGACY_ANALYSIS"] = previous


class _FakeBooster:
    def __init__(self, values: list[float]) -> None:
        self._values = np.asarray(values, dtype=float)

    def predict(self, data: np.ndarray) -> np.ndarray:
        n = int(len(data))
        if self._values.size == 1:
            return np.full(n, float(self._values[0]), dtype=float)
        if self._values.size >= n:
            return self._values[:n]
        return np.resize(self._values, n)


def test_predict_monthly_frame_probability_identity() -> None:
    frame = pd.DataFrame(
        {
            "dt": [20240101, 20240101, 20240101],
            "code": ["1111", "2222", "3333"],
            "close": [100.0, 110.0, 120.0],
            "ma7": [99.0, 109.0, 119.0],
            "ma20": [98.0, 108.0, 118.0],
            "ma60": [97.0, 107.0, 117.0],
            "close_prev1": [99.0, 109.0, 119.0],
            "close_prev5": [95.0, 105.0, 115.0],
            "close_prev10": [90.0, 100.0, 110.0],
            "ma7_prev1": [98.0, 108.0, 118.0],
            "ma20_prev1": [97.0, 107.0, 117.0],
            "ma60_prev1": [96.0, 106.0, 116.0],
        }
    )
    models = ml_service.MonthlyTrainedModels(
        abs_cls=_FakeBooster([0.80, 0.60, 0.50]),
        dir_cls=_FakeBooster([0.20, 0.70, 0.40]),
        feature_columns=list(ml_service.FEATURE_COLUMNS),
        medians={col: 0.0 for col in ml_service.FEATURE_COLUMNS},
        abs_temperature=1.0,
        dir_temperature=1.0,
        n_train_abs=300,
        n_train_dir=120,
    )
    pred = ml_service._predict_monthly_frame(frame, models)  # type: ignore[attr-defined]
    lhs = pred["p_up_big"].to_numpy(dtype=float) + pred["p_down_big"].to_numpy(dtype=float)
    rhs = pred["p_abs_big"].to_numpy(dtype=float)
    assert np.allclose(lhs, rhs, atol=1e-9)


def test_search_monthly_target20_gate_for_direction_backtest_lift() -> None:
    rows: list[dict[str, float | int]] = []
    for month in range(1, 13):
        dt = int(20230000 + month * 100 + 1)
        for idx in range(20):
            p_up_big = 0.05 + 0.90 * (idx / 19.0)
            if idx >= 18:
                ret1m = 0.26
            elif idx >= 14:
                ret1m = 0.08
            else:
                ret1m = -0.04
            rows.append(
                {
                    "dt": dt,
                    "ret1m": float(ret1m),
                    "p_up_big": float(p_up_big),
                    "p_down_big": float(max(0.0, 1.0 - p_up_big)),
                }
            )
    pred_df = pd.DataFrame(rows)
    lookup = ml_service._build_monthly_ret20_lookup_for_direction(  # type: ignore[attr-defined]
        pred_df,
        direction="up",
    )
    result = ml_service._search_monthly_target20_gate_for_direction(  # type: ignore[attr-defined]
        pred_df,
        direction="up",
        lookup_dir=lookup,
    )
    assert result
    assert result.get("source") == "backtest_search"
    assert float(result.get("target20_gate") or 0.0) >= 0.10
    baseline = float(result.get("baseline_rate") or 0.0)
    event_rate = float(result.get("event_rate") or 0.0)
    assert event_rate > baseline
    assert float(result.get("lift") or 0.0) >= ml_service.MONTHLY_TARGET20_MIN_LIFT
