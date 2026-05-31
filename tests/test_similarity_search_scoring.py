from __future__ import annotations

import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.routers import similar
from app.backend.similarity import (
    RECENT_DAILY_FEATURE_COUNT,
    RECENT_DAILY_SHAPE_FEATURES,
    RECENT_DAILY_SHAPE_WINDOW,
    SimilarityService,
)


def _unit_vector(length: int) -> np.ndarray:
    values = np.zeros(length, dtype=np.float32)
    values[0] = 1.0
    return values


def _negative_unit_vector(length: int) -> np.ndarray:
    values = np.zeros(length, dtype=np.float32)
    values[0] = -1.0
    return values


def _recent_daily_unit_vector() -> np.ndarray:
    return _unit_vector(RECENT_DAILY_SHAPE_WINDOW * RECENT_DAILY_FEATURE_COUNT)


def _negative_recent_daily_unit_vector() -> np.ndarray:
    return _negative_unit_vector(RECENT_DAILY_SHAPE_WINDOW * RECENT_DAILY_FEATURE_COUNT)


def _env_row(
    *,
    code: str,
    close: float,
    scale: float,
    asof: str = "2026-03-31",
) -> dict:
    return {
        "code": code,
        "asof": pd.Timestamp(asof),
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


def test_similar_api_hides_vectors_unless_requested(monkeypatch) -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0),
        _env_row(code="MATCH", close=100.0, scale=1.0),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec60": [_unit_vector(60), _unit_vector(60)],
        }
    )
    service.df_vec24 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec24": [_unit_vector(24), _unit_vector(24)],
        }
    )
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [row["asof"] for row in rows],
            "daily_shape_available": [True, True],
            "vec_daily": [_recent_daily_unit_vector(), _recent_daily_unit_vector()],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True
    monkeypatch.setattr(similar, "_service", service)
    app = FastAPI()
    app.include_router(similar.router)
    client = TestClient(app)

    plain = client.get("/api/search/similar", params={"ticker": "QUERY", "asof": "2026-03-31", "k": 1})
    with_vectors = client.get(
        "/api/search/similar",
        params={"ticker": "QUERY", "asof": "2026-03-31", "k": 1, "include_vectors": "true"},
    )

    assert plain.status_code == 200
    assert with_vectors.status_code == 200
    assert "vec60" not in plain.json()[0]
    assert "vec24" not in plain.json()[0]
    assert "vec60" in with_vectors.json()[0]
    assert "vec24" in with_vectors.json()[0]


def test_recent_daily_shape_vector_uses_half_year_close_and_ma_features_without_volume() -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    dates = pd.bdate_range("2025-10-01", periods=140)
    daily_rows = []
    for idx, date_value in enumerate(dates):
        close = 100.0 + idx * 0.4
        daily_rows.append(
            {
                "code": "QUERY",
                "asof": date_value.normalize(),
                "o": close - 0.6,
                "h": close + 1.1,
                "l": close - 1.0,
                "c": close,
                "ma7": close - 1.0,
                "ma20": close - 3.0,
                "ma60": close - 6.0,
            }
        )
    env = pd.DataFrame(
        [
            {
                "code": "QUERY",
                "asof": pd.Timestamp(dates[-1]).normalize(),
            }
        ]
    )

    result = service._build_daily_vectors_for_env(pd.DataFrame(daily_rows), env)

    assert bool(result.loc[0, "daily_shape_available"])
    assert result.loc[0, "daily_asof"] == pd.Timestamp(dates[-1]).normalize()
    assert len(result.loc[0, "vec_daily"]) == RECENT_DAILY_SHAPE_WINDOW * RECENT_DAILY_FEATURE_COUNT


def test_similarity_search_falls_back_to_recent_daily_shape_for_unindexed_ticker(monkeypatch) -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="DAILY_MATCH", close=100.0, scale=1.0, asof="2026-04-30"),
        _env_row(code="DAILY_MISMATCH", close=100.0, scale=1.0, asof="2026-04-30"),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec60": [_unit_vector(60), _unit_vector(60)],
        }
    )
    service.df_vec24 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec24": [_unit_vector(24), _unit_vector(24)],
        }
    )
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [pd.Timestamp("2026-03-31"), pd.Timestamp("2026-03-31")],
            "daily_shape_available": [True, True],
            "vec_daily": [_recent_daily_unit_vector(), _negative_recent_daily_unit_vector()],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True
    dates = pd.bdate_range(end="2026-03-31", periods=150)
    daily_rows = pd.DataFrame(
        [
            {
                "code": "NEW",
                "date": int(date_value.strftime("%Y%m%d")),
                "asof": date_value.normalize(),
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.5,
                "ma7": 99.0,
                "ma20": 98.0,
                "ma60": 97.0,
            }
            for date_value in dates
        ]
    )
    candidate_daily_rows = pd.concat(
        [
            daily_rows.assign(code="DAILY_MATCH"),
            daily_rows.assign(code="DAILY_MISMATCH"),
        ],
        ignore_index=True,
    )

    class DummyConn:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr("app.backend.similarity.get_conn_for_path", lambda *_args, **_kwargs: DummyConn())
    monkeypatch.setattr(
        service,
        "_load_daily_bars",
        lambda _conn, codes=None: daily_rows if codes else pd.concat([daily_rows, candidate_daily_rows], ignore_index=True),
    )
    def build_recent_daily_shape_vector(frame, *_args, **_kwargs):
        code_value = str(frame["code"].iloc[0])
        vector = _negative_recent_daily_unit_vector() if code_value == "DAILY_MISMATCH" else _recent_daily_unit_vector()
        return vector, pd.Timestamp("2026-03-31")

    monkeypatch.setattr(service, "_build_recent_daily_shape_vector", build_recent_daily_shape_vector)

    results = service.search("NEW", asof="2026-03-31", k=2, alpha=0.7)

    assert [item.ticker for item in results] == ["DAILY_MATCH", "DAILY_MISMATCH"]
    assert results[0].tags["fallback"] == "recent_daily_shape_only"
    assert results[0].asof == "2026-03-31"
    assert results[0].tags["daily_asof"] == "2026-03-31"
    assert results[0].score_monthly is None
    assert results[0].score_daily > results[1].score_daily


def test_recent_daily_shape_search_reuses_live_candidate_cache(monkeypatch) -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-04-30"),
        _env_row(code="DAILY_MATCH", close=100.0, scale=1.0, asof="2026-04-30"),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame({"code": [row["code"] for row in rows], "asof": [row["asof"] for row in rows], "vec60": [_unit_vector(60), _unit_vector(60)]})
    service.df_vec24 = pd.DataFrame({"code": [row["code"] for row in rows], "asof": [row["asof"] for row in rows], "vec24": [_unit_vector(24), _unit_vector(24)]})
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [pd.Timestamp("2026-03-31"), pd.Timestamp("2026-03-31")],
            "daily_shape_available": [False, True],
            "vec_daily": [_recent_daily_unit_vector(), _recent_daily_unit_vector()],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True
    dates = pd.bdate_range(end="2026-03-31", periods=150)
    daily_rows = pd.DataFrame(
        [
            {
                "code": code,
                "date": int(date_value.strftime("%Y%m%d")),
                "asof": date_value.normalize(),
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.5,
                "ma7": 99.0,
                "ma20": 98.0,
                "ma60": 97.0,
            }
            for code in ["QUERY", "DAILY_MATCH"]
            for date_value in dates
        ]
    )
    build_calls = {"count": 0}

    class DummyConn:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return False

    def load_daily(_conn, codes=None):
        if codes:
            wanted = {str(code) for code in codes}
            return daily_rows[daily_rows["code"].isin(wanted)].copy()
        return daily_rows.copy()

    def build_recent_daily_shape_vector(frame, *_args, **_kwargs):
        if str(frame["code"].iloc[0]) == "DAILY_MATCH":
            build_calls["count"] += 1
        return _recent_daily_unit_vector(), pd.Timestamp("2026-03-31")

    monkeypatch.setattr("app.backend.similarity.get_conn_for_path", lambda *_args, **_kwargs: DummyConn())
    monkeypatch.setattr(service, "_load_daily_bars", load_daily)
    monkeypatch.setattr(service, "_build_recent_daily_shape_vector", build_recent_daily_shape_vector)

    first = service.search("QUERY", asof="2026-03-31", k=1, alpha=0.0)
    service._search_cache.clear()
    second = service.search("QUERY", asof="2026-03-31", k=1, alpha=0.0)

    assert first[0].ticker == "DAILY_MATCH"
    assert second[0].ticker == "DAILY_MATCH"
    assert build_calls["count"] == 1
    assert all(item.ticker != "QUERY" for item in first + second)


def test_recent_daily_shape_artifact_search_can_return_historical_match() -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-03-31"),
        _env_row(code="MATCH", close=100.0, scale=1.0, asof="2025-12-31"),
        _env_row(code="MATCH", close=100.0, scale=1.0, asof="2026-03-31"),
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
            "vec_daily": [
                _recent_daily_unit_vector(),
                _recent_daily_unit_vector(),
                _negative_recent_daily_unit_vector(),
            ],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    results = service.search("QUERY", k=1, alpha=0.0)

    assert [(item.ticker, item.asof) for item in results] == [("MATCH", "2025-12-31")]
    assert results[0].tags["fallback"] == "recent_daily_shape_artifact"


def test_recent_daily_shape_asof_search_uses_precomputed_artifacts(monkeypatch) -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2025-12-31"),
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-03-31"),
        _env_row(code="MATCH", close=100.0, scale=1.0, asof="2025-12-31"),
        _env_row(code="MATCH", close=100.0, scale=1.0, asof="2026-03-31"),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec60": [_unit_vector(60), _unit_vector(60), _unit_vector(60), _unit_vector(60)],
        }
    )
    service.df_vec24 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec24": [_unit_vector(24), _unit_vector(24), _unit_vector(24), _unit_vector(24)],
        }
    )
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [row["asof"] for row in rows],
            "daily_shape_available": [True, True, True, True],
            "vec_daily": [
                _recent_daily_unit_vector(),
                _negative_recent_daily_unit_vector(),
                _recent_daily_unit_vector(),
                _negative_recent_daily_unit_vector(),
            ],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2, 3]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    def fail_db_access(*_args, **_kwargs):
        raise AssertionError("DB should not be used for indexed daily shape asof search")

    monkeypatch.setattr("app.backend.similarity.get_conn_for_path", fail_db_access)

    results = service.search("QUERY", asof="2026-01-15", k=1, alpha=0.0)

    assert [(item.ticker, item.asof) for item in results] == [("MATCH", "2025-12-31")]
    assert results[0].tags["query_daily_asof"] == "2025-12-31"


def test_recent_daily_shape_cache_can_be_prewarmed(monkeypatch) -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-04-30"),
        _env_row(code="DAILY_MATCH", close=100.0, scale=1.0, asof="2026-04-30"),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame({"code": [row["code"] for row in rows], "asof": [row["asof"] for row in rows], "vec60": [_unit_vector(60), _unit_vector(60)]})
    service.df_vec24 = pd.DataFrame({"code": [row["code"] for row in rows], "asof": [row["asof"] for row in rows], "vec24": [_unit_vector(24), _unit_vector(24)]})
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [pd.Timestamp("2026-03-31"), pd.Timestamp("2026-03-31")],
            "daily_shape_available": [True, True],
            "vec_daily": [_recent_daily_unit_vector(), _recent_daily_unit_vector()],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True
    dates = pd.bdate_range(end="2026-03-31", periods=150)
    daily_rows = pd.DataFrame(
        [
            {
                "code": code,
                "date": int(date_value.strftime("%Y%m%d")),
                "asof": date_value.normalize(),
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.5,
                "ma7": 99.0,
                "ma20": 98.0,
                "ma60": 97.0,
            }
            for code in ["QUERY", "DAILY_MATCH"]
            for date_value in dates
        ]
    )
    build_calls = {"count": 0}

    class DummyConn:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return False

    def build_recent_daily_shape_vector(frame, *_args, **_kwargs):
        if str(frame["code"].iloc[0]) == "DAILY_MATCH":
            build_calls["count"] += 1
        return _recent_daily_unit_vector(), pd.Timestamp("2026-03-31")

    monkeypatch.setattr("app.backend.similarity.get_conn_for_path", lambda *_args, **_kwargs: DummyConn())
    def load_daily(_conn, codes=None):
        if codes:
            wanted = {str(code) for code in codes}
            return daily_rows[daily_rows["code"].isin(wanted)].copy()
        return daily_rows.copy()

    monkeypatch.setattr(service, "_load_daily_bars", load_daily)
    monkeypatch.setattr(service, "_build_recent_daily_shape_vector", build_recent_daily_shape_vector)

    prewarmed = service.prewarm_recent_daily_shape_cache(asof="2026-03-31")
    calls_after_prewarm = build_calls["count"]
    result = service.search("QUERY", asof="2026-03-31", k=1, alpha=0.0)

    assert prewarmed["asof"] == "2026-03-31"
    assert prewarmed["candidate_count"] == 2
    assert result[0].ticker == "DAILY_MATCH"
    assert build_calls["count"] == calls_after_prewarm


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
            "vec_daily": [_recent_daily_unit_vector(), _recent_daily_unit_vector(), _recent_daily_unit_vector()],
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
    assert results[0].tags["daily_shape_window_days"] == RECENT_DAILY_SHAPE_WINDOW
    assert results[0].tags["daily_shape_features"] == RECENT_DAILY_SHAPE_FEATURES


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
            "vec_daily": [_recent_daily_unit_vector(), _recent_daily_unit_vector(), _negative_recent_daily_unit_vector()],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    results = service.search("QUERY", asof="2026-03-31", k=2, alpha=0.7)

    assert [item.ticker for item in results] == ["DAILY_MATCH", "DAILY_MISMATCH"]
    assert results[0].score_monthly == results[1].score_monthly
    assert results[0].score_daily > results[1].score_daily


def test_similarity_search_excludes_future_candidate_asofs() -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-05-20"),
        _env_row(code="PAST_MATCH", close=100.0, scale=1.0, asof="2026-04-30"),
        _env_row(code="FUTURE_MATCH", close=100.0, scale=1.0, asof="2026-05-31"),
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
            "vec_daily": [_recent_daily_unit_vector(), _recent_daily_unit_vector(), _recent_daily_unit_vector()],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    results = service.search("QUERY", asof="2026-05-20", k=3, alpha=0.7)

    assert [item.ticker for item in results] == ["PAST_MATCH"]
    assert all(item.asof <= "2026-05-20" for item in results)


def test_similarity_search_uses_latest_indexed_row_on_or_before_requested_asof() -> None:
    service = SimilarityService(db_path="C:/tmp/unused.duckdb")
    rows = [
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-04-30"),
        _env_row(code="QUERY", close=100.0, scale=1.0, asof="2026-05-31"),
        _env_row(code="PAST_MATCH", close=100.0, scale=1.0, asof="2026-04-30"),
        _env_row(code="FUTURE_MATCH", close=100.0, scale=1.0, asof="2026-05-31"),
    ]
    service.df_env = pd.DataFrame(rows)
    service.df_vec60 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec60": [_unit_vector(60), _negative_unit_vector(60), _unit_vector(60), _negative_unit_vector(60)],
        }
    )
    service.df_vec24 = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "vec24": [_unit_vector(24), _negative_unit_vector(24), _unit_vector(24), _negative_unit_vector(24)],
        }
    )
    service.df_daily_vec = pd.DataFrame(
        {
            "code": [row["code"] for row in rows],
            "asof": [row["asof"] for row in rows],
            "daily_asof": [row["asof"] for row in rows],
            "daily_shape_available": [True, True, True, True],
            "vec_daily": [
                _recent_daily_unit_vector(),
                _negative_recent_daily_unit_vector(),
                _recent_daily_unit_vector(),
                _negative_recent_daily_unit_vector(),
            ],
        }
    )
    service.tag_index = {"UP_UP_UP_HIGH": [0, 1, 2, 3]}
    service._rebuild_tag_lookup_indexes()
    service.loaded = True

    results = service.search("QUERY", asof="2026-05-20", k=3, alpha=0.7)

    assert [item.ticker for item in results] == ["PAST_MATCH"]
    assert all(item.asof <= "2026-05-20" for item in results)
