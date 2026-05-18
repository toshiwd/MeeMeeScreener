from __future__ import annotations

import numpy as np
import pandas as pd

from app.backend.similarity import SimilarityService


def _unit_vector(length: int) -> np.ndarray:
    values = np.zeros(length, dtype=np.float32)
    values[0] = 1.0
    return values


def _negative_unit_vector(length: int) -> np.ndarray:
    values = np.zeros(length, dtype=np.float32)
    values[0] = -1.0
    return values


def _env_row(
    *,
    code: str,
    close: float,
    scale: float,
) -> dict:
    return {
        "code": code,
        "asof": pd.Timestamp("2026-03-31"),
        "tag_id": "UP_UP_UP_HIGH",
        "tag_ma20": "UP",
        "tag_ma60": "UP",
        "tag_dir60": "UP",
        "tag_range": "HIGH",
        "c": close,
        "ma7": 95.0 * scale,
        "ma20": 90.0 * scale,
        "ma60": 80.0 * scale,
        "ma100": 70.0 * scale,
        "ma7_slope": 0.95 * scale,
        "ma20_slope": 0.90 * scale,
        "ma60_slope": 0.80 * scale,
        "ma100_slope": 0.70 * scale,
    }


def test_similarity_search_scores_same_shape_equally_across_price_scales() -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0),
        _env_row(code="SAME_PRICE", close=100.0, scale=1.0),
        _env_row(code="DOUBLE_PRICE", close=200.0, scale=2.0),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec60": [_unit_vector(60), _unit_vector(60), _unit_vector(60)],
        }
    )
    service.df_vec24 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec24": [_unit_vector(24), _unit_vector(24), _unit_vector(24)],
        }
    )
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [row["asof"] for row in rows],
            "daily_shape_available": [True, True, True],
            "vec_daily": [_unit_vector(60), _unit_vector(60), _unit_vector(60)],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    results = service.search("QUERY", asof="2026-03-31", k=2, alpha=0.7)
    scores = {item.ticker: item.score_total for item in results}

    assert set(scores) == {"SAME_PRICE", "DOUBLE_PRICE"}
    assert scores["SAME_PRICE"] <= 1.0
    assert scores["DOUBLE_PRICE"] <= 1.0
    assert abs(scores["SAME_PRICE"] - scores["DOUBLE_PRICE"]) < 1e-6
    assert scores["SAME_PRICE"] > 0.99


def test_similarity_search_uses_daily_shape_after_monthly_shape() -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0),
        _env_row(code="DAILY_MATCH", close=100.0, scale=1.0),
        _env_row(code="DAILY_MISMATCH", close=100.0, scale=1.0),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec60": [_unit_vector(60), _unit_vector(60), _unit_vector(60)],
        }
    )
    service.df_vec24 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec24": [_unit_vector(24), _unit_vector(24), _unit_vector(24)],
        }
    )
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [row["asof"] for row in rows],
            "daily_shape_available": [True, True, True],
            "vec_daily": [_unit_vector(60), _unit_vector(60), _negative_unit_vector(60)],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    results = service.search("QUERY", asof="2026-03-31", k=2, alpha=0.7)

    assert [item.ticker for item in results] == ["DAILY_MATCH", "DAILY_MISMATCH"]
    assert results[0].score_monthly == results[1].score_monthly
    assert results[0].score_daily > results[1].score_daily
