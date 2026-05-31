from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

from scripts import tradex_10pct_move_capture_research_v1 as mod


def _make_db(path: Path) -> None:
    rows = []
    for code, direction in [("1001", "long"), ("2001", "short")]:
        price = 100.0
        for i in range(95):
            ymd = 20250101 + i
            if direction == "long":
                close = price * (1.001 if i < 70 else 1.02)
                open_ = price * (0.995 if i < 70 else 0.99)
                high = max(open_, close) * (1.01 if i < 72 else 1.07)
                low = min(open_, close) * 0.995
            else:
                close = price * (0.999 if i < 70 else 0.98)
                open_ = price * (1.005 if i < 70 else 1.01)
                high = max(open_, close) * (1.005 if i < 70 else 1.02)
                low = min(open_, close) * (0.995 if i < 72 else 0.93)
            rows.append((code, ymd, open_, high, low, close, 1000 + i, "pan"))
            price = close
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)


def test_research_writes_required_artifacts_and_contract_flags(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _make_db(db_path)

    result = mod.run_research(db_path=db_path, output_root=tmp_path / "out", start_ymd=20250101, end_ymd=20250250)
    out = Path(result["output_dir"])

    for name in mod.REQUIRED_ARTIFACTS:
        assert (out / name).exists(), name

    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((out / "research_decision.json").read_text(encoding="utf-8"))
    coverage = json.loads((out / "source_coverage.json").read_text(encoding="utf-8"))
    no_lookahead = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    direction_contract = json.loads((out / "long_short_direction_contract.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["runtime_db_write"] is False
    assert complete["meemee_reflectable_candidate"] is False
    assert complete["production_ranking_changed"] is False
    assert complete["production_candidate_generator_changed"] is False
    assert complete["validated_buy_count"] == 0
    assert no_lookahead["pass"] is True
    assert no_lookahead["future_bars_used_for_selection"] == []
    assert no_lookahead["ret20_derived_feature_tags_used"] is False
    assert coverage["provisional_yahoo_bars_used"] is False
    assert coverage["short_borrow_contract_missing"] is True
    assert direction_contract["short_theoretical_only_when_borrow_missing"] is True
    assert decision["research_decision"] in {
        "ten_pct_capture_keep_for_policy_replay",
        "ten_pct_capture_promising_but_underpowered",
        "long_only_edge",
        "short_only_edge_theoretical",
        "no_ten_pct_capture_edge",
        "blocked_missing_point_in_time_features",
        "blocked_short_borrow_contract_missing_for_operational_short",
    }


def test_path_logic_detects_long_and_short_target_before_stop(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _make_db(db_path)

    result = mod.run_research(db_path=db_path, output_root=tmp_path / "out2", start_ymd=20250101, end_ymd=20250250)
    out = Path(result["output_dir"])
    with (out / "ten_pct_capture_rows.csv").open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows
    assert {"long", "short"} & {row["direction"] for row in rows}
    assert {"target_before_stop", "neither_hit", "stop_before_target", "same_bar_both"} >= {row["event"] for row in rows}
    assert any(row["target_before_stop"] == "True" for row in rows)


def test_metrics_keep_direction_and_setup_separate(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _make_db(db_path)

    result = mod.run_research(db_path=db_path, output_root=tmp_path / "out3", start_ymd=20250101, end_ymd=20250250)
    out = Path(result["output_dir"])
    setup_metrics = json.loads((out / "setup_metrics.json").read_text(encoding="utf-8"))
    direction_metrics = json.loads((out / "direction_metrics.json").read_text(encoding="utf-8"))
    target_contract = json.loads((out / "target_stop_contract.json").read_text(encoding="utf-8"))

    assert target_contract["primary_objective"] == "hit_10pct_target_before_stop_or_invalidation"
    assert target_contract["no_ret20_derived_feature_tags"] is True
    assert set(direction_metrics).issubset({"long", "short"})
    assert all("|" in key for key in setup_metrics)
    assert all("target_before_stop_rate" in value for value in setup_metrics.values())
