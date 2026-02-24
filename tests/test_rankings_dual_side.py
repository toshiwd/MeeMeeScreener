from __future__ import annotations

import os
from pathlib import Path
import sys

import duckdb

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import rankings_cache
from app.backend.services.ml_config import MLConfig


def _prepare_ranking_db(path: Path) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE ml_pred_20d (
                dt INTEGER,
                code TEXT,
                p_up DOUBLE,
                p_up_5 DOUBLE,
                p_up_10 DOUBLE,
                p_turn_up DOUBLE,
                p_turn_down DOUBLE,
                p_turn_down_5 DOUBLE,
                p_turn_down_10 DOUBLE,
                p_turn_down_20 DOUBLE,
                p_down DOUBLE,
                rank_up_20 DOUBLE,
                rank_down_20 DOUBLE,
                ret_pred20 DOUBLE,
                ev20 DOUBLE,
                ev20_net DOUBLE,
                ev5_net DOUBLE,
                ev10_net DOUBLE,
                model_version TEXT
            )
            """
        )
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
        conn.execute(
            """
            CREATE TABLE daily_ma (
                code TEXT,
                date INTEGER,
                ma20 DOUBLE,
                ma60 DOUBLE
            )
            """
        )

        pred_rows = [
            (20240202, "A", 0.80, 0.81, 0.79, 0.62, 0.21, 0.20, 0.21, 0.22, 0.20, 0.95, 0.10, 0.02, 0.02, 0.012, 0.011, 0.010, "vtest"),
            (20240202, "B", 0.45, 0.46, 0.44, 0.30, 0.69, 0.68, 0.67, 0.66, 0.55, 0.20, 0.98, -0.03, -0.03, -0.028, -0.025, -0.024, "vtest"),
            (20240202, "C", 0.70, 0.71, 0.69, 0.54, 0.35, 0.36, 0.35, 0.34, 0.30, 0.60, 0.45, 0.01, 0.01, 0.009, 0.008, 0.007, "vtest"),
        ]
        conn.executemany(
            """
            INSERT INTO ml_pred_20d (
                dt, code, p_up, p_up_5, p_up_10, p_turn_up, p_turn_down,
                p_turn_down_5, p_turn_down_10, p_turn_down_20,
                p_down, rank_up_20, rank_down_20, ret_pred20, ev20, ev20_net,
                ev5_net, ev10_net, model_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            pred_rows,
        )

        bar_rows = [
            ("A", 20240201, 100.0, 102.0, 99.0, 101.0, 100000),
            ("A", 20240202, 101.0, 103.0, 100.0, 102.0, 101000),
            ("B", 20240201, 200.0, 201.0, 198.0, 199.0, 120000),
            ("B", 20240202, 199.0, 200.0, 197.0, 198.0, 121000),
            ("C", 20240201, 300.0, 303.0, 299.0, 302.0, 130000),
            ("C", 20240202, 302.0, 305.0, 301.0, 304.0, 131000),
        ]
        ma_rows = [
            ("A", 20240201, 100.0, 99.0),
            ("A", 20240202, 100.5, 99.5),
            ("B", 20240201, 199.5, 200.0),
            ("B", 20240202, 199.0, 199.8),
            ("C", 20240201, 301.0, 300.0),
            ("C", 20240202, 301.5, 300.5),
        ]
        conn.executemany(
            "INSERT INTO daily_bars (code, date, o, h, l, c, v) VALUES (?, ?, ?, ?, ?, ?, ?)",
            bar_rows,
        )
        conn.executemany(
            "INSERT INTO daily_ma (code, date, ma20, ma60) VALUES (?, ?, ?, ?)",
            ma_rows,
        )


def test_apply_ml_mode_uses_dual_rank_by_direction(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "ranking_dual_side.duckdb"
    _prepare_ranking_db(db_path)

    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setattr(
        rankings_cache,
        "load_ml_config",
        lambda: MLConfig(
            top_n=3,
            p_up_threshold=0.55,
            rank_weight=0.5,
        ),
    )

    items = [
        {"code": "A", "name": "A", "asOf": "2024-02-02", "changePct": 0.01},
        {"code": "B", "name": "B", "asOf": "2024-02-02", "changePct": -0.02},
        {"code": "C", "name": "C", "asOf": "2024-02-02", "changePct": 0.03},
    ]

    up_items, up_pred_dt, _ = rankings_cache._apply_ml_mode(  # type: ignore[attr-defined]
        items,
        direction="up",
        mode="ml",
        limit=2,
    )
    down_items, down_pred_dt, _ = rankings_cache._apply_ml_mode(  # type: ignore[attr-defined]
        items,
        direction="down",
        mode="ml",
        limit=2,
    )

    assert up_pred_dt == 20240202
    assert down_pred_dt == 20240202
    assert up_items[0]["code"] == "A"
    assert down_items[0]["code"] == "B"
    assert "mlRankUp" in up_items[0]
    assert "mlRankDown" in down_items[0]
    assert "mlPDown" in down_items[0]
