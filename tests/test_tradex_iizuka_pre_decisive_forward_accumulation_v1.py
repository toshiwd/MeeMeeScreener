from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

import scripts.tradex_iizuka_pre_decisive_forward_accumulation_v1 as module


def test_json_ready_converts_numpy_scalars() -> None:
    assert module._json_ready(np.int64(7)) == 7


def test_forward_availability_stops_on_sparse_same_universe_overlap(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("create table feature_frame_daily(dt bigint, code varchar)")
        conn.execute(
            "insert into feature_frame_daily values (?, ?), (?, ?)",
            [
                int(pd.Timestamp("2026-02-02", tz="UTC").timestamp()),
                "1001",
                int(pd.Timestamp("2026-02-03", tz="UTC").timestamp()),
                "1002",
            ],
        )
    finally:
        conn.close()

    previous_db = module.DEFAULT_RUNTIME_DB
    module.DEFAULT_RUNTIME_DB = db_path
    try:
        source = pd.DataFrame(
            {
                "anchor_date": ["2026-01-19", "2026-01-19"],
                "symbol": ["1001", "1002"],
                "side": ["long", "long"],
            }
        )
        trading_dates = pd.date_range("2026-01-01", periods=60, freq="B").strftime("%Y-%m-%d").tolist()
        runtime_dates = {
            "daily_bars_max_date": "2026-04-30",
            "feature_frame_daily_max_date": "2026-04-30",
            "ml_feature_daily_max_date": "2026-04-30",
            "label_20d_max_date": "2026-02-12",
            "ml_label_20d_max_date": "2026-02-12",
            "ml_pred_20d_max_date": "2026-03-13",
            "feature_snapshot_daily_max_date": "2026-04-30",
        }
        audit = module._build_forward_availability_audit(
            source=source,
            trading_dates=trading_dates,
            runtime_dates=runtime_dates,
            runtime_status={"status": "ok"},
            ranking_status={"status": "ok"},
        )
        decision = module._build_decision(audit)
    finally:
        module.DEFAULT_RUNTIME_DB = previous_db

    assert audit["decision_hint"]["decision"] == "insufficient_forward_data"
    assert audit["contract_completion"]["same_universe_overlap_too_sparse"] is True
    assert audit["forward_window"]["expected_candidate_rows"] == 2
    assert decision["decision"] == "insufficient_forward_data"
