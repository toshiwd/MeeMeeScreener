from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import swing_expectancy_service


def _prepare_db(path: Path, *, include_p_down: bool = True) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT
            )
            """
        )
        if include_p_down:
            conn.execute(
                """
                CREATE TABLE ml_pred_20d (
                    code TEXT,
                    dt INTEGER,
                    p_up DOUBLE,
                    p_down DOUBLE,
                    p_turn_up DOUBLE,
                    p_turn_down DOUBLE,
                    ev20_net DOUBLE
                )
                """
            )
        else:
            conn.execute(
                """
                CREATE TABLE ml_pred_20d (
                    code TEXT,
                    dt INTEGER,
                    p_up DOUBLE,
                    p_turn_up DOUBLE,
                    p_turn_down DOUBLE,
                    ev20_net DOUBLE
                )
                """
            )

        bars_rows: list[tuple] = []
        pred_rows_with_down: list[tuple] = []
        pred_rows_without_down: list[tuple] = []
        start = date(2024, 1, 1)
        close = 100.0
        for idx in range(90):
            current = start + timedelta(days=idx)
            ymd = int(current.strftime("%Y%m%d"))
            o = close
            c = close + 0.4
            h = max(o, c) + 1.0
            l = min(o, c) - 1.0
            bars_rows.append(("1301", ymd, o, h, l, c, 150_000))
            pred_rows_with_down.append(("1301", ymd, 0.66, 0.34, 0.60, 0.40, 0.012))
            pred_rows_without_down.append(("1301", ymd, 0.66, 0.60, 0.40, 0.012))
            close = c

        conn.executemany(
            "INSERT INTO daily_bars (code, date, o, h, l, c, v) VALUES (?, ?, ?, ?, ?, ?, ?)",
            bars_rows,
        )
        if include_p_down:
            conn.executemany(
                "INSERT INTO ml_pred_20d (code, dt, p_up, p_down, p_turn_up, p_turn_down, ev20_net) VALUES (?, ?, ?, ?, ?, ?, ?)",
                pred_rows_with_down,
            )
        else:
            conn.executemany(
                "INSERT INTO ml_pred_20d (code, dt, p_up, p_turn_up, p_turn_down, ev20_net) VALUES (?, ?, ?, ?, ?, ?)",
                pred_rows_without_down,
            )


def test_refresh_and_resolve_expectancy(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "swing_expectancy.duckdb"
    _prepare_db(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    swing_expectancy_service._load_snapshot_cached.cache_clear()  # type: ignore[attr-defined]

    refreshed = swing_expectancy_service.refresh_swing_setup_stats(
        as_of_ymd=20240330,
        horizons=(20,),
    )
    assert refreshed["ok"] is True
    assert int(refreshed["rows"]) > 0

    expectancy = swing_expectancy_service.resolve_setup_expectancy(
        side="long",
        setup_type="breakout",
        horizon_days=20,
        as_of_ymd=20240330,
    )
    assert expectancy["side"] == "long"
    assert expectancy["setupType"] == "breakout"
    assert int(expectancy["samples"]) > 0
    assert isinstance(expectancy["shrunkMeanRet"], float)

    n = float(expectancy["samples"])
    alpha = n / (n + 120.0)
    expected_shrunk = alpha * float(expectancy["meanRet"]) + (1.0 - alpha) * float(expectancy["sideMeanRet"])
    assert abs(float(expectancy["shrunkMeanRet"]) - expected_shrunk) < 1e-12


def test_refresh_works_without_p_down_column(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "swing_expectancy_without_p_down.duckdb"
    _prepare_db(db_path, include_p_down=False)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    swing_expectancy_service._load_snapshot_cached.cache_clear()  # type: ignore[attr-defined]

    refreshed = swing_expectancy_service.refresh_swing_setup_stats(
        as_of_ymd=20240330,
        horizons=(20,),
    )
    assert refreshed["ok"] is True
    assert int(refreshed["rows"]) > 0
