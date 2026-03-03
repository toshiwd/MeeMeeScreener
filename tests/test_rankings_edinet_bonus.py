from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.api.routers.rankings import router as rankings_router
from app.backend.services import rankings_cache


def _prepare_monthly_db(path: Path, *, with_edinet: bool) -> None:
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
        conn.executemany(
            """
            INSERT INTO ml_monthly_pred (
                dt, code, p_abs_big, p_up_given_big, p_up_big, p_down_big,
                score_up, score_down, model_version, n_train_abs, n_train_dir, computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (20240105, "A", 0.80, 0.68, 0.54, 0.26, 0.62, 0.34, "m_edinet", 800, 320),
                (20240105, "B", 0.75, 0.61, 0.46, 0.29, 0.56, 0.40, "m_edinet", 800, 320),
                (20240105, "C", 0.71, 0.59, 0.42, 0.30, 0.52, 0.42, "m_edinet", 800, 320),
            ],
        )
        conn.execute(
            """
            INSERT INTO ml_monthly_model_registry (
                model_version, model_key, label_version, metrics_json, artifact_path,
                n_train_abs, n_train_dir, created_at, is_active
            )
            VALUES (?, ?, 1, ?, '{}', 10, 10, CURRENT_TIMESTAMP, TRUE)
            """,
            [
                "m_edinet",
                "ml_monthly_abs_dir_1m_v1",
                json.dumps(
                    {
                        "ret20_lookup": {
                            "target_abs_ret": 0.2,
                            "up": {"baseline_rate": 0.03, "bins": [{"min_prob": 0.0, "max_prob": 1.0, "event_rate": 0.2, "samples": 100}]},
                            "down": {"baseline_rate": 0.02, "bins": [{"min_prob": 0.0, "max_prob": 1.0, "event_rate": 0.2, "samples": 100}]},
                        }
                    }
                ),
            ],
        )
        start = date(2024, 1, 1)
        for code, base_price in (("A", 100.0), ("B", 120.0), ("C", 80.0)):
            rows: list[tuple] = []
            for i in range(35):
                d = start + timedelta(days=i)
                ymd = int(d.strftime("%Y%m%d"))
                c = base_price + (i * (0.8 if code == "A" else (0.2 if code == "B" else -0.1)))
                rows.append((code, ymd, c - 1.0, c + 1.0, c - 2.0, c, 100000 + i))
            conn.executemany(
                "INSERT INTO daily_bars (code, date, o, h, l, c, v) VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

        if with_edinet:
            conn.execute(
                """
                CREATE TABLE edinetdb_company_map (
                    sec_code TEXT PRIMARY KEY,
                    edinet_code TEXT,
                    name TEXT,
                    industry TEXT,
                    updated_at TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE edinetdb_financials (
                    edinet_code TEXT,
                    fiscal_year TEXT,
                    accounting_standard TEXT,
                    payload_json TEXT,
                    fetched_at TIMESTAMP,
                    PRIMARY KEY(edinet_code, fiscal_year, accounting_standard)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE edinetdb_ratios (
                    edinet_code TEXT,
                    fiscal_year TEXT,
                    accounting_standard TEXT,
                    payload_json TEXT,
                    fetched_at TIMESTAMP,
                    PRIMARY KEY(edinet_code, fiscal_year, accounting_standard)
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO edinetdb_company_map(sec_code, edinet_code, name, industry, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    ("A", "E00001", "A Co", "Tech"),
                    ("B", "E00002", "B Co", "Finance"),
                ],
            )
            ratio_payload = json.dumps(
                {
                    "roe": 0.14,
                    "equity_ratio": 0.48,
                    "debt_to_equity": 0.62,
                    "operating_cf_margin": 0.12,
                }
            )
            fin_payload = json.dumps(
                {
                    "ebitda": 120000000.0,
                    "sales_growth_rate": 0.09,
                }
            )
            conn.executemany(
                """
                INSERT INTO edinetdb_ratios(edinet_code, fiscal_year, accounting_standard, payload_json, fetched_at)
                VALUES (?, '2023', 'jgaap', ?, CURRENT_TIMESTAMP)
                """,
                [
                    ("E00001", ratio_payload),
                    ("E00002", ratio_payload),
                ],
            )
            conn.executemany(
                """
                INSERT INTO edinetdb_financials(edinet_code, fiscal_year, accounting_standard, payload_json, fetched_at)
                VALUES (?, '2023', 'jgaap', ?, CURRENT_TIMESTAMP)
                """,
                [
                    ("E00001", fin_payload),
                    ("E00002", fin_payload),
                ],
            )


def _monthly_cache_items() -> list[dict]:
    return [
        {
            "code": "A",
            "name": "A",
            "asOf": "2024-01-05",
            "changePct": 0.07,
            "liquidity20d": 10_000_000,
            "monthlyBreakoutUpProb": 0.63,
            "monthlyBreakoutDownProb": 0.12,
            "monthlyRangeProb": 0.40,
        },
        {
            "code": "B",
            "name": "B",
            "asOf": "2024-01-05",
            "changePct": 0.03,
            "liquidity20d": 11_000_000,
            "monthlyBreakoutUpProb": 0.59,
            "monthlyBreakoutDownProb": 0.18,
            "monthlyRangeProb": 0.44,
        },
        {
            "code": "C",
            "name": "C",
            "asOf": "2024-01-05",
            "changePct": -0.01,
            "liquidity20d": 9_000_000,
            "monthlyBreakoutUpProb": 0.52,
            "monthlyBreakoutDownProb": 0.24,
            "monthlyRangeProb": 0.47,
        },
    ]


def test_monthly_edinet_fields_and_flag_application(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "rankings_edinet.duckdb"
    _prepare_monthly_db(db_path, with_edinet=True)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    now = datetime.now(timezone.utc)
    cache_items = _monthly_cache_items()
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): cache_items,
        ("M", "latest", "down"): cache_items,
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    monkeypatch.setenv("MEEMEE_RANK_EDINET_BONUS_ENABLED", "0")
    off_result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    off_map = {str(item["code"]): item for item in off_result["items"]}
    assert off_map["A"]["edinetStatus"] == "ok"
    assert off_map["C"]["edinetStatus"] == "unmapped"
    assert off_map["A"]["edinetFeatureFlagApplied"] is False

    monkeypatch.setenv("MEEMEE_RANK_EDINET_BONUS_ENABLED", "1")
    on_result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    on_map = {str(item["code"]): item for item in on_result["items"]}
    assert on_map["A"]["edinetFeatureFlagApplied"] is True
    bonus_a = float(on_map["A"]["edinetScoreBonus"] or 0.0)
    expected = max(0.0, min(1.0, float(off_map["A"]["entryScore"]) + bonus_a))
    assert abs(float(on_map["A"]["entryScore"]) - expected) < 1e-9

    down_result = rankings_cache.get_rankings("M", "latest", "down", 3, mode="hybrid")
    down_map = {str(item["code"]): item for item in down_result["items"]}
    up_bonus = float(on_map["A"]["edinetScoreBonus"] or 0.0)
    down_bonus = float(down_map["A"]["edinetScoreBonus"] or 0.0)
    assert up_bonus * down_bonus <= 0.0
    assert abs(abs(up_bonus) - abs(down_bonus)) < 1e-9


def test_monthly_edinet_missing_table_fallback(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "rankings_edinet_missing.duckdb"
    _prepare_monthly_db(db_path, with_edinet=False)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("MEEMEE_RANK_EDINET_BONUS_ENABLED", "1")

    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): _monthly_cache_items(),
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    assert result["items"]
    assert all(item.get("edinetStatus") == "missing_tables" for item in result["items"])
    assert all(item.get("edinetFeatureFlagApplied") is True for item in result["items"])


def test_edinet_audit_and_monitor_api(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "rankings_edinet_audit.duckdb"
    _prepare_monthly_db(db_path, with_edinet=True)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv("MEEMEE_RANK_EDINET_BONUS_ENABLED", "1")

    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): _monthly_cache_items(),
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    _ = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    _ = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")

    with duckdb.connect(str(db_path)) as conn:
        count_rows = conn.execute("SELECT COUNT(*) FROM ranking_edinet_audit_daily").fetchone()[0]
        realized_rows = conn.execute(
            "SELECT COUNT(*) FROM ranking_edinet_audit_daily WHERE realized_ret_20 IS NOT NULL"
        ).fetchone()[0]
    assert int(count_rows) == 3
    assert int(realized_rows) >= 1

    app = FastAPI()
    app.include_router(rankings_router)
    client = TestClient(app)

    ranking_res = client.get(
        "/api/rankings",
        params={"tf": "M", "which": "latest", "dir": "up", "mode": "hybrid", "risk_mode": "balanced", "limit": 3},
    )
    assert ranking_res.status_code == 200
    payload = ranking_res.json()
    assert payload["items"]
    assert "edinetStatus" in payload["items"][0]
    assert "edinetScoreBonus" in payload["items"][0]

    monitor_res = client.get(
        "/api/rankings/edinet/monitor",
        params={"lookback_days": 365, "dir": "up", "risk_mode": "balanced", "which": "latest"},
    )
    assert monitor_res.status_code == 200
    monitor = monitor_res.json()
    assert "groups" in monitor
    assert "positive" in monitor["groups"]
    assert "negative" in monitor["groups"]
