from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sys

import duckdb

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import rankings_cache


def _prepare_monthly_pred_db(path: Path, *, with_rows: bool) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE ml_monthly_pred (
                dt INTEGER,
                code TEXT,
                p_abs_big DOUBLE,
                p_up_given_big DOUBLE,
                p_up_big DOUBLE,
                p_down_big DOUBLE,
                score_up DOUBLE,
                score_down DOUBLE,
                model_version TEXT,
                n_train_abs INTEGER,
                n_train_dir INTEGER,
                computed_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ml_monthly_model_registry (
                model_version TEXT,
                model_key TEXT,
                label_version INTEGER,
                metrics_json TEXT,
                artifact_path TEXT,
                n_train_abs INTEGER,
                n_train_dir INTEGER,
                created_at TIMESTAMP,
                is_active BOOLEAN
            )
            """
        )
        if with_rows:
            conn.executemany(
                """
                INSERT INTO ml_monthly_pred (
                    dt, code, p_abs_big, p_up_given_big, p_up_big, p_down_big,
                    score_up, score_down, model_version, n_train_abs, n_train_dir, computed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (20240201, "A", 0.80, 0.50, 0.40, 0.40, 0.52, 0.52, "mtest", 900, 320),
                    (20240201, "B", 0.72, 0.85, 0.61, 0.11, 0.66, 0.28, "mtest", 900, 320),
                    (20240201, "C", 0.65, 0.86, 0.56, 0.09, 0.61, 0.23, "mtest", 900, 320),
                ],
            )


def _monthly_cache_items() -> list[dict]:
    return [
        {
            "code": "A",
            "name": "A",
            "asOf": "2024-02-29",
            "changePct": 0.11,
            "liquidity20d": 12_000_000,
            "monthlyBreakoutUpProb": 0.40,
            "monthlyBreakoutDownProb": 0.20,
            "monthlyRangeProb": 0.40,
        },
        {
            "code": "B",
            "name": "B",
            "asOf": "2024-02-29",
            "changePct": 0.07,
            "liquidity20d": 11_000_000,
            "monthlyBreakoutUpProb": 0.62,
            "monthlyBreakoutDownProb": 0.10,
            "monthlyRangeProb": 0.42,
        },
        {
            "code": "C",
            "name": "C",
            "asOf": "2024-02-29",
            "changePct": 0.03,
            "liquidity20d": 10_000_000,
            "monthlyBreakoutUpProb": 0.58,
            "monthlyBreakoutDownProb": 0.12,
            "monthlyRangeProb": 0.45,
        },
    ]


def test_monthly_hybrid_uses_monthly_pred_scores(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "monthly_rankings.duckdb"
    _prepare_monthly_pred_db(db_path, with_rows=True)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO ml_monthly_model_registry (
                model_version, model_key, label_version, metrics_json, artifact_path,
                n_train_abs, n_train_dir, created_at, is_active
            )
            VALUES (?, ?, 1, ?, '{}', 10, 10, CURRENT_TIMESTAMP, TRUE)
            """,
            [
                "mtest",
                "ml_monthly_abs_dir_1m_v1",
                '{"ret20_lookup":{"target_abs_ret":0.2,"up":{"baseline_rate":0.03,"bins":[{"min_prob":0.0,"max_prob":1.0,"event_rate":0.2,"samples":1000}]},"down":{"baseline_rate":0.02,"bins":[]}}}',
            ],
        )
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): _monthly_cache_items(),
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    codes = [item.get("code") for item in result["items"]]
    assert codes == ["B", "C", "A"]
    assert result["pred_dt"] == 20240201
    assert result["model_version"] == "mtest"
    top = result["items"][0]
    assert top["mlPAbsBig"] is not None
    assert top["mlScoreUp1M"] is not None
    assert top["mlP20Side1M"] is not None
    assert top["target20Qualified"] in (True, False)
    assert top["entryQualified"] is True


def test_monthly_hybrid_relaxes_strict_recommended_gate(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "monthly_rankings_relax.duckdb"
    _prepare_monthly_pred_db(db_path, with_rows=True)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO ml_monthly_model_registry (
                model_version, model_key, label_version, metrics_json, artifact_path,
                n_train_abs, n_train_dir, created_at, is_active
            )
            VALUES (?, ?, 1, ?, '{}', 10, 10, CURRENT_TIMESTAMP, TRUE)
            """,
            [
                "mtest",
                "ml_monthly_abs_dir_1m_v1",
                '{"gate_recommendation":{"up":{"abs_gate":0.70,"side_gate":0.70},"down":{"abs_gate":0.70,"side_gate":0.70}}}',
            ],
        )
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): _monthly_cache_items(),
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    codes = [item.get("code") for item in result["items"]]
    assert codes == ["B", "C", "A"]
    assert all(item.get("entryQualified") is True for item in result["items"])
    assert float(result["items"][0]["entryGateSide"]) < 0.70


def test_monthly_hybrid_prefers_backtested_target20_gate(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "monthly_rankings_target20_gate.duckdb"
    _prepare_monthly_pred_db(db_path, with_rows=True)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO ml_monthly_model_registry (
                model_version, model_key, label_version, metrics_json, artifact_path,
                n_train_abs, n_train_dir, created_at, is_active
            )
            VALUES (?, ?, 1, ?, '{}', 10, 10, CURRENT_TIMESTAMP, TRUE)
            """,
            [
                "mtest",
                "ml_monthly_abs_dir_1m_v1",
                '{"gate_recommendation":{"up":{"abs_gate":0.20,"side_gate":0.20,"target20_gate":0.27},"down":{"abs_gate":0.20,"side_gate":0.20,"target20_gate":0.19}}}',
            ],
        )
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): _monthly_cache_items(),
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    top = result["items"][0]
    assert float(top["target20Gate"]) >= 0.27
    assert top["target20GateSource"] == "model_backtest"


def test_monthly_hybrid_falls_back_to_rule_order_when_pred_missing(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "monthly_rankings_empty.duckdb"
    _prepare_monthly_pred_db(db_path, with_rows=False)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    now = datetime.now(timezone.utc)
    base_items = _monthly_cache_items()
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): base_items,
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    codes = [item.get("code") for item in result["items"]]
    assert codes == [item["code"] for item in base_items]
    assert result["pred_dt"] is None
    assert result["model_version"] is None


def test_non_monthly_hybrid_uses_existing_ml_path(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("D", "latest", "up"): [{"code": "D0", "asOf": "2024-02-29", "changePct": 0.01}],
        ("M", "latest", "up"): [{"code": "M0", "asOf": "2024-02-29", "changePct": 0.02}],
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]
    calls = {"default": 0, "monthly": 0}

    def _fake_default(items: list[dict], *, direction: str, mode: str, limit: int):
        _ = (items, direction, mode, limit)
        calls["default"] += 1
        return ([{"code": "D1"}], 20240229, "daily_model")

    def _fake_monthly(items: list[dict], *, direction: str, limit: int):
        _ = (items, direction, limit)
        calls["monthly"] += 1
        return ([{"code": "M1"}], 20240201, "monthly_model")

    monkeypatch.setattr(rankings_cache, "_apply_ml_mode", _fake_default)
    monkeypatch.setattr(rankings_cache, "_apply_monthly_ml_mode", _fake_monthly)

    day = rankings_cache.get_rankings("D", "latest", "up", 5, mode="hybrid")
    month = rankings_cache.get_rankings("M", "latest", "up", 5, mode="hybrid")

    assert day["items"][0]["code"] == "D1"
    assert month["items"][0]["code"] == "M1"
    assert calls["default"] == 1
    assert calls["monthly"] == 1
