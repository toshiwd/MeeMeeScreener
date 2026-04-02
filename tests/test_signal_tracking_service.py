from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta

import duckdb

from app.backend.services import signal_tracking_service as service
from app.db.schema import ensure_schema


def _build_business_days(start: date, count: int) -> list[int]:
    days: list[int] = []
    cursor = start
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(int(cursor.strftime("%Y%m%d")))
        cursor += timedelta(days=1)
    return days


def _make_temp_db() -> str:
    tmp_dir = tempfile.mkdtemp(prefix="meemee_signal_tracking_")
    return os.path.join(tmp_dir, "stocks.duckdb")


def _seed_market_data(db_path: str) -> list[int]:
    market_days = _build_business_days(date(2026, 1, 5), 50)
    conn = duckdb.connect(db_path)
    try:
        ensure_schema(conn)
        conn.executemany(
            "INSERT OR REPLACE INTO stock_meta(code, name) VALUES (?, ?)",
            [
                ("1111", "Alpha"),
                ("2222", "Beta"),
            ],
        )
        rows: list[tuple[str, int, float, float, float, float, int, str]] = []
        for index, dt in enumerate(market_days):
            alpha_close = 100.0 + index
            beta_close = 200.0 - index
            rows.append(("1111", dt, alpha_close - 0.5, alpha_close + 1.0, alpha_close - 1.0, alpha_close, 100000, "test"))
            rows.append(("2222", dt, beta_close + 0.5, beta_close + 1.0, beta_close - 1.0, beta_close, 100000, "test"))
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_bars(code, date, o, h, l, c, v, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    finally:
        conn.close()
    return market_days


def _basis_row(dt: int, code: str, name: str, buy_rank: int | None, sell_rank: int | None) -> dict[str, object]:
    return {
        "dt": dt,
        "code": code,
        "name": name,
        "source_rank_buy": buy_rank,
        "source_rank_sell": sell_rank,
        "basis_payload_json": service._json_dump(  # type: ignore[attr-defined]
            {
                "code": code,
                "name": name,
                "asOf": service._ymd_to_iso(dt),  # type: ignore[attr-defined]
            }
        ),
    }


def _build_fake_items(as_of_int: int, side: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    buy_dates = {20260105, 20260106}
    sell_dates = {20260106, 20260107}
    if side == "buy":
        all_items = [
            {
                "code": "1111",
                "name": "Alpha",
                "asOf": service._ymd_to_iso(as_of_int),  # type: ignore[attr-defined]
                "entryQualified": as_of_int in buy_dates,
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
                "tradeDecisionReasons": ["上放れ初動"],
                "tradeRiskWatch": ["出来高継続"],
                "tradePriorityScore": 0.9,
                "entryScore": 0.8,
                "probSide": 0.7,
            },
            {
                "code": "2222",
                "name": "Beta",
                "asOf": service._ymd_to_iso(as_of_int),  # type: ignore[attr-defined]
                "entryQualified": False,
                "setupType": "watch",
                "monthlyBoxState": "box_lower",
                "tradeDecisionReasons": ["見送り"],
                "tradeRiskWatch": [],
                "tradePriorityScore": 0.2,
                "entryScore": 0.1,
                "probSide": 0.3,
            },
        ]
    else:
        all_items = [
            {
                "code": "1111",
                "name": "Alpha",
                "asOf": service._ymd_to_iso(as_of_int),  # type: ignore[attr-defined]
                "entryQualified": False,
                "setupType": "watch",
                "monthlyBoxState": "box_upper",
                "tradeDecisionReasons": ["見送り"],
                "tradeRiskWatch": [],
                "tradePriorityScore": 0.1,
                "entryScore": 0.1,
                "probSide": 0.2,
            },
            {
                "code": "2222",
                "name": "Beta",
                "asOf": service._ymd_to_iso(as_of_int),  # type: ignore[attr-defined]
                "entryQualified": as_of_int in sell_dates,
                "setupType": "short_breakdown",
                "monthlyBoxState": "box_lower",
                "tradeDecisionReasons": ["下放れ継続"],
                "tradeRiskWatch": ["踏み上げ注意"],
                "tradePriorityScore": 0.95,
                "entryScore": 0.82,
                "probSide": 0.74,
            },
        ]
    ranked_items = [dict(item) for item in all_items if item.get("entryQualified") is True]
    return all_items, ranked_items


def _install_fake_pipeline(monkeypatch, target_dates: list[int]) -> None:
    def _fake_build_basis_rows_for_date(as_of_int: int) -> list[dict[str, object]]:
        if as_of_int not in target_dates:
            return []
        return [
            _basis_row(as_of_int, "1111", "Alpha", 1, 2),
            _basis_row(as_of_int, "2222", "Beta", 2, 1),
        ]

    def _fake_evaluate_trade_items_from_basis(*, items, as_of_int: int, side: str):
        _ = items
        all_items, ranked_items = _build_fake_items(as_of_int, side)
        return all_items, ranked_items, as_of_int, "model:test:v1"

    monkeypatch.setattr(service, "_build_basis_rows_for_date", _fake_build_basis_rows_for_date)
    monkeypatch.setattr(service, "_evaluate_trade_items_from_basis", _fake_evaluate_trade_items_from_basis)


def _no_refresh(*args, **kwargs):
    _ = (args, kwargs)
    return {"ok": True, "skipped": True}


def test_signal_tracking_pipeline_supports_buy_and_sell_with_unqualified_rows(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    basis_result = service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    decision_result = service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    campaign_result = service.rebuild_signal_campaigns(
        logic_version="logic:test:v1",
        side="all",
        db_path=db_path,
    )

    assert basis_result["dates_processed"] == 3
    assert decision_result["decision_upserted"] == 12
    assert campaign_result["campaign_count"] == 2

    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)
    buy_campaigns = service.list_signal_campaigns(
        status="completed",
        side="buy",
        logic_version="logic:test:v1",
        db_path=db_path,
    )
    sell_campaigns = service.list_signal_campaigns(
        status="completed",
        side="sell",
        logic_version="logic:test:v1",
        db_path=db_path,
    )

    assert buy_campaigns["count"] == 1
    assert buy_campaigns["items"][0]["signal_count"] == 2
    assert sell_campaigns["count"] == 1
    assert sell_campaigns["items"][0]["code"] == "2222"

    with duckdb.connect(db_path) as conn:
        total_buy_decisions = conn.execute(
            "SELECT COUNT(*) FROM signal_decision_daily WHERE logic_version = 'logic:test:v1' AND side = 'buy'"
        ).fetchone()[0]
        unqualified_buy_decisions = conn.execute(
            """
            SELECT COUNT(*)
            FROM signal_decision_daily
            WHERE logic_version = 'logic:test:v1' AND side = 'buy' AND entry_qualified = FALSE
            """
        ).fetchone()[0]

    assert total_buy_decisions == 6
    assert unqualified_buy_decisions == 4


def test_signal_tracking_summary_is_scoped_by_logic_version(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v1", side="all", db_path=db_path)

    def _fake_eval_v2(*, items, as_of_int: int, side: str):
        _ = items
        all_items, ranked_items = _build_fake_items(as_of_int, side)
        if side == "buy":
            for item in all_items:
                if item["code"] == "1111":
                    item["entryQualified"] = as_of_int == 20260105
            ranked_items = [dict(item) for item in all_items if item.get("entryQualified") is True]
        else:
            for item in all_items:
                item["entryQualified"] = False
            ranked_items = []
        return all_items, ranked_items, as_of_int, "model:test:v2"

    monkeypatch.setattr(service, "_evaluate_trade_items_from_basis", _fake_eval_v2)
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v2",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v2", side="all", db_path=db_path)

    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)
    logic_versions = service.list_logic_versions(db_path=db_path)
    summary_v1 = service.get_signal_tracking_summary(side="buy", logic_version="logic:test:v1", db_path=db_path)
    summary_v2 = service.get_signal_tracking_summary(side="buy", logic_version="logic:test:v2", db_path=db_path)
    activation = service.activate_logic_version("logic:test:v2", db_path=db_path)
    summary_latest = service.get_signal_tracking_summary(side="buy", logic_version="latest", db_path=db_path)

    assert {item["logic_version"] for item in logic_versions["items"]} >= {"logic:test:v1", "logic:test:v2"}
    assert summary_v1["active_count"] == 0
    assert summary_v1["completed_count"] == 1
    assert summary_v2["active_count"] == 0
    assert summary_v2["completed_count"] == 1
    assert summary_v1["duplicate_signal_rate"] == 1.0
    assert summary_v2["duplicate_signal_rate"] == 0.0
    assert activation["logic_version"] == "logic:test:v2"
    assert summary_latest["logic_version"] == "logic:test:v2"
    assert summary_latest["duplicate_signal_rate"] == 0.0


def test_signal_tracking_validation_returns_decision_and_campaign_levels(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v1", side="all", db_path=db_path)

    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)
    buy_validation = service.get_signal_tracking_validation(
        side="buy",
        logic_version="logic:test:v1",
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        db_path=db_path,
    )
    sell_validation = service.get_signal_tracking_validation(
        side="sell",
        logic_version="logic:test:v1",
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        db_path=db_path,
    )

    assert buy_validation["decision_level"]["total_decisions"] == 6
    assert buy_validation["decision_level"]["qualified_decisions"] == 2
    assert buy_validation["decision_level"]["by_setup_type"][0]["setup_type"] == "breakout"
    assert buy_validation["decision_level"]["average_directional_return_30"] is not None
    assert buy_validation["decision_level"]["qualified_directional_hit_rate_30"] is not None
    assert buy_validation["decision_level"]["median_days_to_max_favorable_30"] is not None
    assert buy_validation["decision_level"]["peak_day_buckets"]
    assert buy_validation["decision_level"]["profit_timing_patterns"]
    assert buy_validation["decision_level"]["by_regime"]
    assert isinstance(buy_validation["decision_level"]["failure_examples"], list)
    assert buy_validation["campaign_level"]["total_campaigns"] == 1
    assert buy_validation["campaign_level"]["duplicate_signal_rate"] == 1.0
    assert buy_validation["campaign_level"]["median_days_to_max_favorable_30"] is not None
    assert buy_validation["audit"]["basis_provenance"]["future_source_as_of_count"] == 0
    assert sell_validation["decision_level"]["qualified_decisions"] == 2
    assert sell_validation["decision_level"]["qualified_directional_hit_rate_30"] is not None
    assert sell_validation["decision_level"]["median_days_to_max_favorable_30"] is not None
    assert sell_validation["campaign_level"]["total_campaigns"] == 1
    assert sell_validation["campaign_level"]["by_setup_type"][0]["setup_type"] == "short_breakdown"


def test_signal_tracking_analysis_and_leakage_audit(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v1", side="all", db_path=db_path)
    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)

    with duckdb.connect(db_path) as conn:
        conn.execute(
            "UPDATE signal_basis_daily SET source_as_of = dt + 1 WHERE code = '1111' AND dt = ?",
            [target_dates[0]],
        )

    analysis = service.get_signal_tracking_analysis(
        side="buy",
        logic_version="logic:test:v1",
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        db_path=db_path,
    )
    leakage = service.get_signal_tracking_leakage_audit(
        side="buy",
        logic_version="logic:test:v1",
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        db_path=db_path,
    )

    assert analysis["summary"]["directional_hit_rate_30"] is not None
    assert analysis["summary"]["median_days_to_max_favorable_30"] is not None
    assert analysis["peak_day_buckets"]
    assert analysis["rolling_6m"]
    assert isinstance(analysis["failure_examples"], list)
    assert leakage["basis_provenance"]["future_source_as_of_count"] == 1
    assert leakage["latest_signal_parity"]["available"] is True


def test_signal_tracking_comparison_uses_sell_primary_horizon(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version=service.DEFAULT_LOGIC_VERSION,
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version=service.DEFAULT_LOGIC_VERSION, side="all", db_path=db_path)
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version=service.SELL_TIGHTENED_LOGIC_VERSION,
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version=service.SELL_TIGHTENED_LOGIC_VERSION, side="all", db_path=db_path)

    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)
    comparison = service.get_signal_tracking_comparison(
        side="sell",
        base_logic_version=service.DEFAULT_LOGIC_VERSION,
        target_logic_version=service.SELL_TIGHTENED_LOGIC_VERSION,
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        db_path=db_path,
    )

    assert comparison["primary_horizon"] == 10
    assert comparison["decision"]["qualified_decisions"]["base"] == 2
    assert comparison["decision"]["qualified_decisions"]["target"] <= 2
    assert "delta" in comparison["decision"]["directional_hit_rate"]


def test_signal_basis_provenance_backfill_fills_missing_fields(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:2]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_pred_20d (
              dt INTEGER,
              code VARCHAR,
              p_up DOUBLE,
              ret_pred20 DOUBLE,
              ev20 DOUBLE,
              ev20_net DOUBLE,
              model_version VARCHAR,
              n_train INTEGER,
              computed_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            UPDATE signal_basis_daily
            SET source_as_of = NULL,
                basis_source = NULL,
                source_hash = NULL,
                payload_schema_version = NULL
            WHERE basis_version = 'basis:test:v1'
            """
        )

    result = service.backfill_signal_basis_provenance(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        db_path=db_path,
    )

    assert result["candidate_rows"] > 0
    with duckdb.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM signal_basis_daily
            WHERE basis_version = 'basis:test:v1'
              AND source_as_of IS NULL
            """
        ).fetchone()
    assert int(row[0]) == 0


def test_signal_basis_provenance_backfill_joins_model_version_from_ml_pred(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:1]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[0],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_pred_20d (
              dt INTEGER,
              code VARCHAR,
              p_up DOUBLE,
              ret_pred20 DOUBLE,
              ev20 DOUBLE,
              ev20_net DOUBLE,
              model_version VARCHAR,
              n_train INTEGER,
              computed_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            UPDATE signal_basis_daily
            SET pred_dt = ?, model_version = NULL
            WHERE basis_version = 'basis:test:v1'
            """,
            [target_dates[0]],
        )
        conn.execute(
            """
            INSERT INTO ml_pred_20d(dt, code, p_up, ret_pred20, ev20, ev20_net, model_version, n_train, computed_at)
            VALUES (?, ?, 0.5, 0.1, 0.1, 0.1, ?, 10, NOW())
            """,
            [target_dates[0], "1111", "model:test:v1"],
        )

    result = service.backfill_signal_basis_provenance(
        from_ymd=target_dates[0],
        to_ymd=target_dates[0],
        basis_version="basis:test:v1",
        db_path=db_path,
    )

    assert result["model_version_join_rows"] > 0
    with duckdb.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM signal_basis_daily
            WHERE basis_version = 'basis:test:v1'
              AND model_version = 'model:test:v1'
            """
        ).fetchone()
    assert int(row[0]) > 0


def test_signal_validation_returns_sell_subset_comparison(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v1", side="all", db_path=db_path)

    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)
    validation = service.get_signal_tracking_validation(
        logic_version="logic:test:v1",
        side="sell",
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        db_path=db_path,
    )

    subset_payload = validation["sell_subset_comparison"]
    assert subset_payload is not None
    assert subset_payload["primary_horizon"] == 10
    assert len(subset_payload["subsets"]) == 4
    assert subset_payload["subsets"][0]["subset_key"] == "breakdown_only"


def test_signal_events_and_ranking_appearances_are_materialized(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v1", side="all", db_path=db_path)
    ranking_result = service.rebuild_ranking_appearances(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        ranking_logic_version="ranking:test:v1",
        signal_logic_version="logic:test:v1",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )

    monkeypatch.setattr(service, "ensure_signal_tracking_current", _no_refresh)
    signal_events = service.list_signal_events(
        status="completed",
        side="buy",
        logic_version="logic:test:v1",
        db_path=db_path,
    )
    ranking_history = service.list_ranking_appearances(
        status="completed",
        direction="up",
        ranking_logic_version="ranking:test:v1",
        db_path=db_path,
    )
    ranking_summary = service.get_ranking_history_summary(
        direction="up",
        ranking_logic_version="ranking:test:v1",
        db_path=db_path,
    )
    ranking_analysis = service.get_ranking_history_analysis(
        ranking_logic_version="ranking:test:v1",
        db_path=db_path,
    )

    assert ranking_result["appearance_upserted"] == 12
    assert signal_events["count"] == 1
    assert signal_events["items"][0]["return_30d"] is not None
    assert signal_events["items"][0]["days_to_max_favorable_30"] is not None
    assert ranking_history["count"] == 6
    assert ranking_history["items"][0]["signal_state_at_appearance"] in {"buy", "wait", "sell", "both"}
    assert ranking_history["items"][0]["days_to_max_favorable_30"] is not None
    assert ranking_summary["completed_count"] == 6
    assert ranking_analysis["by_dir"]
    assert ranking_analysis["by_dir"][0]["median_days_to_max_favorable_30"] is not None


def test_tracking_runtime_status_reports_generated_history(monkeypatch) -> None:
    db_path = _make_temp_db()
    market_days = _seed_market_data(db_path)
    target_dates = market_days[:3]
    _install_fake_pipeline(monkeypatch, target_dates)

    service.backfill_signal_basis(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_decisions(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        logic_version="logic:test:v1",
        side="all",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )
    service.rebuild_signal_campaigns(logic_version="logic:test:v1", side="all", db_path=db_path)
    service.rebuild_ranking_appearances(
        from_ymd=target_dates[0],
        to_ymd=target_dates[-1],
        ranking_logic_version="ranking:test:v1",
        signal_logic_version="logic:test:v1",
        basis_version="basis:test:v1",
        reset_scope=True,
        db_path=db_path,
    )

    status = service.get_tracking_runtime_status(db_path=db_path)

    assert status["resolved_stocks_db_path"].endswith("stocks.duckdb")
    assert status["signal_history_generated"] is True
    assert status["ranking_history_generated"] is True
    assert status["signal_occurrence_count"] == 4
    assert status["ranking_appearance_count"] == 12
    assert status["signal_latest_date_iso"] == "2026-01-07"
    assert status["ranking_latest_date_iso"] == "2026-01-07"
