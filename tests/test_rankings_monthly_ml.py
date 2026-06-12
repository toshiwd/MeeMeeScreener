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


def _prepare_hybrid_ml_cache(monkeypatch) -> None:
    rankings_cache._RESULT_CACHE = {}  # type: ignore[attr-defined]
    rankings_cache._RESULT_CACHE_GENERATION = 0  # type: ignore[attr-defined]
    rankings_cache._RESULT_REFRESH_IN_PROGRESS = {}  # type: ignore[attr-defined]
    rankings_cache._RESULT_REFRESH_LAST_ERROR = {}  # type: ignore[attr-defined]
    monkeypatch.setattr(rankings_cache, "_ensure_cache_fresh_stale_ok", lambda **kwargs: None)
    monkeypatch.setattr(rankings_cache, "is_legacy_analysis_disabled", lambda: False)


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
    _prepare_hybrid_ml_cache(monkeypatch)

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
    _prepare_hybrid_ml_cache(monkeypatch)

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
    _prepare_hybrid_ml_cache(monkeypatch)

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
    _prepare_hybrid_ml_cache(monkeypatch)

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
    _prepare_hybrid_ml_cache(monkeypatch)
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


def test_monthly_short_support_requires_bearish_backdrop() -> None:
    assert not rankings_cache._has_short_monthly_support(  # type: ignore[attr-defined]
        trend_down_strict=False,
        monthly_breakout_down_prob=0.54,
        monthly_range_prob=0.55,
        monthly_range_pos=0.50,
        monthly_box_state="box_mid",
    )
    assert rankings_cache._has_short_monthly_support(  # type: ignore[attr-defined]
        trend_down_strict=True,
        monthly_breakout_down_prob=0.54,
        monthly_range_prob=0.55,
        monthly_range_pos=0.50,
        monthly_box_state="box_mid",
    )
    assert rankings_cache._has_short_monthly_support(  # type: ignore[attr-defined]
        trend_down_strict=False,
        monthly_breakout_down_prob=0.60,
        monthly_range_prob=0.55,
        monthly_range_pos=0.50,
        monthly_box_state="box_mid",
    )


def test_monthly_short_playbook_prioritizes_stronger_patterns() -> None:
    strong_double_top = rankings_cache._calc_playbook_entry_bonus(  # type: ignore[attr-defined]
        direction="down",
        shape_patterns={"d4ShortDoubleTop": True},
    )
    strong_head_shoulders = rankings_cache._calc_playbook_entry_bonus(  # type: ignore[attr-defined]
        direction="down",
        shape_patterns={"d5ShortHeadShoulders": True},
    )
    weak_mixed = rankings_cache._calc_playbook_entry_bonus(  # type: ignore[attr-defined]
        direction="down",
        shape_patterns={"d2ShortMixedFar": True},
    )
    weak_na_below = rankings_cache._calc_playbook_entry_bonus(  # type: ignore[attr-defined]
        direction="down",
        shape_patterns={"d3ShortNaBelow": True},
    )

    assert strong_double_top > weak_mixed
    assert strong_head_shoulders > weak_na_below


def test_combo_entry_bonus_prefers_stronger_bull_bundles() -> None:
    core_item = {
        "weeklyBreakoutUpProb": 0.58,
        "trendUpStrict": True,
        "diff20_pct": 0.04,
    }
    stage_item = {
        **core_item,
        "breakout20Up": 0.02,
        "cnt_20_above": 12,
    }
    mtf_item = {
        **core_item,
        "monthlyBreakoutUpProb": 0.63,
    }

    core_bonus = rankings_cache._calc_combo_entry_bonus(  # type: ignore[attr-defined]
        direction="up",
        item=core_item,
    )
    stage_bonus = rankings_cache._calc_combo_entry_bonus(  # type: ignore[attr-defined]
        direction="up",
        item=stage_item,
    )
    mtf_bonus = rankings_cache._calc_combo_entry_bonus(  # type: ignore[attr-defined]
        direction="up",
        item=mtf_item,
    )

    assert core_bonus > 0.0
    assert stage_bonus > core_bonus
    assert mtf_bonus >= stage_bonus


def test_combo_entry_bonus_prefers_stronger_bear_bundles() -> None:
    core_item = {
        "breakout20Down": 0.02,
        "market_ret20": -0.03,
        "diff20_pct": -0.04,
    }
    confirmed_item = {
        **core_item,
        "trendDownStrict": True,
    }

    core_bonus = rankings_cache._calc_combo_entry_bonus(  # type: ignore[attr-defined]
        direction="down",
        item=core_item,
    )
    confirmed_bonus = rankings_cache._calc_combo_entry_bonus(  # type: ignore[attr-defined]
        direction="down",
        item=confirmed_item,
    )

    assert core_bonus > 0.0
    assert confirmed_bonus > core_bonus


def test_monthly_entry_sort_key_prioritizes_down_combo_bonus() -> None:
    base_item = {
        "code": "A",
        "entryScore": 0.80,
        "comboScoreBonus": 0.0,
        "probSide": 0.60,
    }
    combo_item = {
        "code": "B",
        "entryScore": 0.80,
        "comboScoreBonus": 0.01,
        "probSide": 0.10,
    }

    down_order = sorted(
        [base_item, combo_item],
        key=lambda item: rankings_cache._monthly_entry_sort_key(item, direction="down"),  # type: ignore[attr-defined]
    )
    up_order = sorted(
        [base_item, combo_item],
        key=lambda item: rankings_cache._monthly_entry_sort_key(item, direction="up"),  # type: ignore[attr-defined]
    )

    assert down_order[0]["code"] == "B"
    assert up_order[0]["code"] == "A"


def test_monthly_hybrid_populates_combo_score_bonus(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "monthly_rankings_combo_bonus.duckdb"
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
    _prepare_hybrid_ml_cache(monkeypatch)
    monkeypatch.setattr(rankings_cache, "_calc_combo_entry_bonus", lambda **kwargs: 0.017)  # type: ignore[attr-defined]

    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("M", "latest", "up"): _monthly_cache_items(),
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    result = rankings_cache.get_rankings("M", "latest", "up", 3, mode="hybrid")
    assert all(item.get("comboScoreBonus") == 0.017 for item in result["items"])
    assert all(item.get("entryScore") is not None for item in result["items"])


def test_rule_entry_gate_uses_snapshot_features_for_combo_bonus(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_resolve_rule_snapshot_map",
        lambda items: {
            "A": {
                "trend_up_strict": True,
                "diff20_pct": 0.05,
                "breakout20_up": 0.02,
                "cnt20_above": 12.0,
                "market_ret20": -0.03,
            }
        },
    )

    result = rankings_cache._decorate_rule_items_with_entry_gate(  # type: ignore[attr-defined]
        [
            {
                "code": "A",
                "asOf": "2026-03-12",
                "changePct": 0.04,
                "weeklyBreakoutUpProb": 0.58,
                "monthlyBreakoutUpProb": 0.63,
                "liquidity20d": 12_000_000,
                "monthlyBoxState": "box_mid",
                "monthlyBoxMonths": 5,
                "monthlyRangeProb": 0.40,
            }
        ],
        direction="up",
    )

    assert result[0]["comboScoreBonus"] > 0.0
    assert result[0]["entryScore"] >= result[0]["comboScoreBonus"]
