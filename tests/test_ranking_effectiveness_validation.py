from __future__ import annotations

from pathlib import Path

import duckdb

from app.backend.tools import ranking_effectiveness_validation as validation


def _create_validation_db(path: Path) -> None:
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
                v BIGINT,
                source TEXT
            )
            """
        )
        conn.execute("CREATE TABLE tickers(code TEXT, name TEXT)")
        conn.executemany(
            "INSERT INTO tickers VALUES (?, ?)",
            [(f"{idx:04d}", f"Name {idx:04d}") for idx in range(1, 9)],
        )
        rows = []
        for day_index in range(80):
            ymd = 20260101 + day_index
            for code_index in range(1, 9):
                code = f"{code_index:04d}"
                base = 100.0 + day_index
                drift = code_index * 0.2
                if code in {"0001", "0002"} and day_index >= 40:
                    drift += (day_index - 39) * 1.5
                if code in {"0007", "0008"} and day_index >= 40:
                    drift -= (day_index - 39) * 1.5
                close = base + drift
                rows.append((code, ymd, close - 0.2, close + 0.5, close - 0.5, close, 1000, "pan"))
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)


def test_run_validation_writes_authoritative_json(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _create_validation_db(db_path)

    from app.backend.services import codex_bridge_service
    from app.backend.services.ml import rankings_cache

    monkeypatch.setattr(
        codex_bridge_service,
        "get_runtime_stock_db_status",
        lambda: {
            "freshness_state": "fresh",
            "latest_available_global_date": 20260180,
            "latest_available_global_date_iso": "2026-01-80",
            "selected_runtime_db_path": str(db_path),
        },
    )
    monkeypatch.setattr(
        codex_bridge_service,
        "get_rankings_freshness",
        lambda **kwargs: {
            "freshness_state": "fresh",
            "stale": False,
            "snapshot_as_of": "2026-01-80",
            "tf": kwargs.get("tf"),
            "direction": kwargs.get("direction"),
        },
    )

    def fake_rankings_asof(tf, which, direction, limit, *, as_of, mode="trade", risk_mode="balanced"):
        codes = ["0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008"]
        if direction == "down":
            codes = list(reversed(codes))
        return {
            "tf": tf,
            "which": which,
            "dir": direction,
            "mode": mode,
            "risk_mode": risk_mode,
            "requested_as_of": str(as_of),
            "items": [
                {
                    "code": code,
                    "name": code,
                    "asOf": str(as_of),
                    "changePct": 0.01,
                    "tradePriorityScore": 1.0 - index * 0.01,
                    "entryQualified": True,
                }
                for index, code in enumerate(codes[:limit])
            ],
        }

    monkeypatch.setattr(rankings_cache, "get_rankings_asof", fake_rankings_asof)

    result = validation.run_validation(
        db_path=db_path,
        output_root=tmp_path / "out",
        required_latest_ymd=20260170,
        lookback_dates=3,
        eval_step=3,
        min_eval_dates=1,
        horizons=(5, 10, 20),
        top_k_values=(5, 10, 20),
    )

    assert result["ok"] is True
    decision_path = Path(result["artifact_refs"]["authoritative_decision"])
    assert decision_path.exists()
    payload = validation.json.loads(decision_path.read_text(encoding="utf-8"))
    assert payload["fixed_evaluation_conditions"]["source_db"] == "snapshot_only"
    assert payload["fixed_evaluation_conditions"]["live_db_queries"] is False
    assert payload["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
    assert "momentum_follow_through_v1" in payload["candidate_local_decision"]
    assert Path(result["artifact_refs"]["ranking_surface_inventory"]).exists()


def test_run_validation_stops_decision_when_snapshot_is_stale(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _create_validation_db(db_path)

    from app.backend.services import codex_bridge_service
    from app.backend.services.ml import rankings_cache

    monkeypatch.setattr(codex_bridge_service, "get_runtime_stock_db_status", lambda: {"freshness_state": "stale"})
    monkeypatch.setattr(codex_bridge_service, "get_rankings_freshness", lambda **_kwargs: {"freshness_state": "stale", "stale": True})
    monkeypatch.setattr(
        rankings_cache,
        "get_rankings_asof",
        lambda *args, **kwargs: {"items": [{"code": "0001", "asOf": str(kwargs.get("as_of"))}]},
    )

    result = validation.run_validation(
        db_path=db_path,
        output_root=tmp_path / "out",
        required_latest_ymd=20260507,
        lookback_dates=1,
        eval_step=1,
    )

    decision_path = Path(result["artifact_refs"]["authoritative_decision"])
    payload = validation.json.loads(decision_path.read_text(encoding="utf-8"))
    assert payload["authoritative_rollup_decision"] == "not-yet-reportable"
    assert payload["not_reportable_reasons"]
