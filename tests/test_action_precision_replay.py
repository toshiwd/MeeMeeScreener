from __future__ import annotations

import duckdb
import pandas as pd

from scripts.action_precision_replay import (
    ActionPrecisionThresholds,
    _axis_from_failure,
    _apply_long_too_late_variant,
    _apply_long_cluster_variant,
    _label_directional,
    _label_timing_long,
    _label_timing_short,
    _long_too_late_candidate_table,
    _long_weak_direction_candidate_table,
    _snapshot_horizon_contract,
    _split_months,
    _state_combo,
)


def test_split_months_uses_trailing_12_6_6_blocks() -> None:
    months = [
        *[2023_00 + month for month in range(1, 13)],
        *[2024_00 + month for month in range(1, 13)],
        *[2025_00 + month for month in range(1, 13)],
    ]
    split = _split_months(months, train_months=12, tune_months=6, validation_months=6)
    assert len(split.train_months) == 12
    assert len(split.tune_months) == 6
    assert len(split.validation_months) == 6
    assert split.train_months[0] == 202401
    assert split.validation_months[-1] == 202512


def test_snapshot_horizon_contract_derives_latest_trade_date_and_last_full_month(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER)")
        for ymd in [*range(20260101, 20260132), *range(20260201, 20260229)]:
            conn.execute("INSERT INTO daily_bars VALUES ('1301', ?)", [ymd])
    finally:
        conn.close()

    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        horizon = _snapshot_horizon_contract(conn)
    finally:
        conn.close()

    assert horizon["snapshot_max_trade_date"] == 20260228
    assert horizon["replay_lookback_start_date"] == 20160228
    assert horizon["last_fully_confirmable_month"] == 202601
    assert horizon["analysis_cutoff_ymd"] == 20260131


def test_long_directional_and_timing_labels() -> None:
    thresholds = ActionPrecisionThresholds()
    directional, meta = _label_directional(
        side="buy",
        mfe_20=0.12,
        mae_20=0.04,
        days_to_mfe=7,
        thresholds=thresholds,
    )
    timing, score = _label_timing_long(
        entry_price=100.0,
        best_refined_entry_open=97.0,
        long_mfe_20=0.12,
        pre_signal_runup_long=0.02,
        thresholds=thresholds,
    )
    assert directional == "BUY_STRONG"
    assert meta["strong"] is True
    assert timing == "BUY_TOO_EARLY"
    assert 0.0 <= score <= 100.0


def test_short_directional_and_timing_labels() -> None:
    thresholds = ActionPrecisionThresholds()
    directional, meta = _label_directional(
        side="sell",
        mfe_20=0.11,
        mae_20=0.03,
        days_to_mfe=6,
        thresholds=thresholds,
    )
    timing, score = _label_timing_short(
        entry_price=100.0,
        best_refined_entry_open=103.0,
        short_mfe_20=0.11,
        pre_signal_drop_short=0.02,
        thresholds=thresholds,
    )
    assert directional == "SELL_STRONG"
    assert meta["strong"] is True
    assert timing == "SELL_TOO_EARLY"
    assert 0.0 <= score <= 100.0


def test_state_combo_reflects_payload_features() -> None:
    payload = {
        "marketRegime": "risk_on",
        "monthlyBoxState": "box_upper",
        "weeklyBreakoutUpProb": 0.72,
        "weeklyBreakoutDownProb": 0.21,
        "monthlyBreakoutUpProb": 0.63,
        "monthlyBreakoutDownProb": 0.33,
        "changePct": 0.04,
        "candleBodyRatio": 0.75,
        "candleUpperWickRatio": 0.10,
        "candleLowerWickRatio": 0.15,
        "bullMarubozu": 1.0,
        "bearMarubozu": 0.0,
        "shootingStarLike": 0.0,
        "morningStar": 1.0,
        "reclaim60": 1.0,
        "v60Core": 1.0,
        "v60Strong": 1.0,
    }
    combo = _state_combo("buy", payload)
    assert "regime=risk_on" in combo
    assert "monthly=box_upper" in combo
    assert "daily=daily_bull" in combo
    assert "change=up" in combo


def test_axis_from_failure_maps_to_side_specific_axis() -> None:
    assert _axis_from_failure("buy", "weak_direction") == "weak_direction_buy"
    assert _axis_from_failure("sell", "too_late") == "too_late_sell"


def test_long_too_late_candidate_selection_blocks_top_clusters() -> None:
    rows = []
    for state, count, too_late_count, strong_count in [
        ("cluster_a", 4, 4, 3),
        ("cluster_b", 3, 2, 2),
        ("cluster_c", 2, 1, 1),
    ]:
        for idx in range(count):
            rows.append(
                {
                    "side": "buy",
                    "directional_label": "BUY_STRONG" if idx < strong_count else "BUY_WEAK",
                    "state_combination": state,
                    "long_mfe_20": 0.12 + (0.01 * idx),
                    "long_mae_20": 0.03 + (0.005 * idx),
                    "long_timing_score": 80.0 + idx,
                    "remaining_upside_ratio_long": 0.2 + (0.01 * idx),
                    "pre_signal_runup_long": 0.08 + (0.005 * idx),
                    "long_timing_label": "BUY_TOO_LATE" if idx < too_late_count else "BUY_ON_TIME",
                }
            )
    diagnosis_frame = pd.DataFrame(rows)

    table, selected = _long_too_late_candidate_table(diagnosis_frame, min_sample_count=1, block_cluster_count=2)

    assert selected == ["cluster_a", "cluster_b"]
    assert table.loc[table["state_combination"] == "cluster_a", "selected_for_block"].item() is True
    assert table.loc[table["state_combination"] == "cluster_c", "recommended_action"].item() == "downgrade_to_buy_weak"


def test_long_too_late_variant_block_and_downgrade_behave_differently() -> None:
    frame = pd.DataFrame(
        [
            {
                "side": "buy",
                "directional_label": "BUY_STRONG",
                "directional_strong": True,
                "directional_weak": False,
                "state_combination": "cluster_a",
                "long_mfe_20": 0.12,
                "long_mae_20": 0.03,
                "long_timing_score": 80.0,
                "remaining_upside_ratio_long": 0.2,
                "pre_signal_runup_long": 0.08,
                "long_timing_label": "BUY_TOO_LATE",
            },
            {
                "side": "buy",
                "directional_label": "BUY_STRONG",
                "directional_strong": True,
                "directional_weak": False,
                "state_combination": "cluster_b",
                "long_mfe_20": 0.13,
                "long_mae_20": 0.02,
                "long_timing_score": 81.0,
                "remaining_upside_ratio_long": 0.3,
                "pre_signal_runup_long": 0.07,
                "long_timing_label": "BUY_ON_TIME",
            },
        ]
    )

    blocked = _apply_long_too_late_variant(frame, selected_clusters={"cluster_a"}, mode="block")
    downgraded = _apply_long_too_late_variant(frame, selected_clusters={"cluster_a"}, mode="downgrade")

    assert len(blocked) == 1
    assert blocked.iloc[0]["state_combination"] == "cluster_b"
    assert len(downgraded) == 2
    assert downgraded.loc[downgraded["state_combination"] == "cluster_a", "directional_label"].item() == "BUY_WEAK"


def test_long_weak_direction_candidate_selection_uses_coverage_band() -> None:
    rows = []
    for state, count, strong_count, weak_count, mfe, mae, timing in [
        ("cluster_a", 3, 0, 3, 0.05, 0.03, 79.0),
        ("cluster_b", 3, 1, 2, 0.07, 0.04, 81.0),
        ("cluster_frozen", 3, 0, 3, 0.02, 0.01, 78.0),
    ]:
        for idx in range(count):
            rows.append(
                {
                    "side": "buy",
                    "directional_label": "BUY_STRONG" if idx < strong_count else "BUY_WEAK",
                    "state_combination": state,
                    "long_mfe_20": mfe + (0.001 * idx),
                    "long_mae_20": mae + (0.001 * idx),
                    "long_timing_score": timing + idx,
                    "tradeability_score": 60.0 + idx,
                    "failure_kind": "weak_direction",
                }
            )
    diagnosis_frame = pd.DataFrame(rows)

    table, selected = _long_weak_direction_candidate_table(
        diagnosis_frame,
        min_sample_count=3,
        max_sample_count=3,
        block_cluster_count=1,
        excluded_clusters={"cluster_frozen"},
    )

    assert selected == ["cluster_a"]
    assert table.loc[table["state_combination"] == "cluster_a", "selected_for_block"].item() is True
    assert table.loc[table["state_combination"] == "cluster_b", "recommended_action"].item() == "downgrade_to_buy_weak"
    assert table.loc[table["state_combination"] == "cluster_frozen", "selected_for_block"].item() is False


def test_long_cluster_variant_block_and_downgrade_behave_differently() -> None:
    frame = pd.DataFrame(
        [
            {
                "side": "buy",
                "directional_label": "BUY_STRONG",
                "directional_strong": True,
                "directional_weak": False,
                "state_combination": "cluster_a",
                "long_mfe_20": 0.12,
                "long_mae_20": 0.03,
                "long_timing_score": 80.0,
                "tradeability_score": 60.0,
                "failure_kind": "weak_direction",
            },
            {
                "side": "buy",
                "directional_label": "BUY_STRONG",
                "directional_strong": True,
                "directional_weak": False,
                "state_combination": "cluster_b",
                "long_mfe_20": 0.13,
                "long_mae_20": 0.02,
                "long_timing_score": 81.0,
                "tradeability_score": 61.0,
                "failure_kind": "weak_direction",
            },
        ]
    )

    blocked = _apply_long_cluster_variant(frame, selected_clusters={"cluster_a"}, mode="block")
    downgraded = _apply_long_cluster_variant(frame, selected_clusters={"cluster_a"}, mode="downgrade")

    assert len(blocked) == 1
    assert blocked.iloc[0]["state_combination"] == "cluster_b"
    assert len(downgraded) == 2
    assert downgraded.loc[downgraded["state_combination"] == "cluster_a", "directional_label"].item() == "BUY_WEAK"
