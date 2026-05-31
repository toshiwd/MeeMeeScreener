from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

from scripts import tradex_market_regime_trade_permission_gate_v1 as mod


def _write_input(root: Path, db_path: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "dt": 20250180,
            "code": "1001",
            "direction": "long",
            "setup_family": "long_c",
            "horizon": 20,
            "stop_rule": "stop_a",
            "target_hit": True,
            "target_before_stop": True,
            "stop_before_target": False,
            "neither_hit": False,
            "severe_loss": False,
            "adverse_excursion": -0.02,
            "return_at_exit": 0.10,
            "days_to_event": 8,
        },
        {
            "dt": 20250181,
            "code": "1002",
            "direction": "long",
            "setup_family": "long_a",
            "horizon": 20,
            "stop_rule": "stop_a",
            "target_hit": False,
            "target_before_stop": False,
            "stop_before_target": True,
            "neither_hit": False,
            "severe_loss": False,
            "adverse_excursion": -0.04,
            "return_at_exit": -0.05,
            "days_to_event": 5,
        },
        {
            "dt": 20250180,
            "code": "2001",
            "direction": "short",
            "setup_family": "short_b",
            "horizon": 20,
            "stop_rule": "stop_a",
            "target_hit": True,
            "target_before_stop": True,
            "stop_before_target": False,
            "neither_hit": False,
            "severe_loss": False,
            "adverse_excursion": 0.02,
            "return_at_exit": 0.10,
            "days_to_event": 7,
        },
    ]
    with (root / "ten_pct_capture_rows.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    (root / "source_coverage.json").write_text(
        json.dumps(
            {
                "runtime_db_path": str(db_path),
                "short_borrow_contract_missing": True,
                "provisional_yahoo_bars_used": False,
            }
        ),
        encoding="utf-8",
    )
    (root / "research_decision.json").write_text(
        json.dumps({"authoritative_rollup_decision": "no_ten_pct_capture_edge"}),
        encoding="utf-8",
    )


def _make_db(path: Path) -> None:
    rows = []
    for code in ["1001", "1002", "2001", "3001", "3002"]:
        price = 100.0
        for i in range(120):
            ymd = 20250080 + i
            close = price * (1.006 if i < 100 else 1.001)
            open_ = price * 0.998
            high = max(open_, close) * 1.01
            low = min(open_, close) * 0.99
            rows.append((code, ymd, open_, high, low, close, 1000 + i, "pan"))
            price = close
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)


def test_market_gate_audit_writes_required_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    input_root = tmp_path / "input"
    _make_db(db_path)
    _write_input(input_root, db_path)

    result = mod.run_audit(input_root=input_root, output_root=tmp_path / "out", db_path=db_path)
    out = Path(result["output_dir"])

    for name in mod.REQUIRED_ARTIFACTS:
        assert (out / name).exists(), name
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    coverage = json.loads((out / "source_coverage.json").read_text(encoding="utf-8"))
    decision = json.loads((out / "research_decision.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["runtime_db_write"] is False
    assert complete["meemee_reflectable_candidate"] is False
    assert complete["production_ranking_changed"] is False
    assert complete["production_candidate_generator_changed"] is False
    assert complete["validated_buy_count"] == 0
    assert audit["pass"] is True
    assert audit["future_bars_used_for_permission"] == []
    assert coverage["provisional_yahoo_bars_used"] is False
    assert coverage["short_borrow_contract_missing"] is True
    assert decision["research_decision"] in {
        "market_gate_keep_for_setup_replay",
        "market_gate_promising_but_underpowered",
        "long_permission_edge_found",
        "short_permission_edge_theoretical",
        "no_market_regime_gate_edge",
        "blocked_missing_market_features",
        "blocked_no_lookahead_violation",
    }


def test_permission_metrics_keep_long_short_separate(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    input_root = tmp_path / "input"
    _make_db(db_path)
    _write_input(input_root, db_path)

    result = mod.run_audit(input_root=input_root, output_root=tmp_path / "out2", db_path=db_path)
    out = Path(result["output_dir"])
    long_metrics = json.loads((out / "long_permission_metrics.json").read_text(encoding="utf-8"))
    short_metrics = json.loads((out / "short_permission_metrics.json").read_text(encoding="utf-8"))
    contract = json.loads((out / "regime_definition_contract.json").read_text(encoding="utf-8"))

    assert contract["target_stop_or_setup_changed"] is False
    assert any(key.startswith("long_") for key in long_metrics)
    assert all("|long" in key for key in long_metrics)
    assert all("|short" in key for key in short_metrics)
    assert all("comparison_vs_all_conditions" in value for value in long_metrics.values())


def test_market_rows_include_point_in_time_features(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    input_root = tmp_path / "input"
    _make_db(db_path)
    _write_input(input_root, db_path)

    result = mod.run_audit(input_root=input_root, output_root=tmp_path / "out3", db_path=db_path)
    out = Path(result["output_dir"])
    with (out / "market_regime_gate_rows.csv").open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows
    assert "universe_pct_above_ma20" in rows[0]
    assert "permission_gate" in rows[0]
    assert "regime_bucket" in rows[0]
