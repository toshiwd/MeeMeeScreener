from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import duckdb

from scripts import tradex_monthly_drawdown_guarded_momentum_entry_timing_confirmation_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _row(day: str, symbol: str, ret: float, score: float, *, momentum: bool = False, monthly: str = "monthly_prior_uptrend") -> dict:
    return {
        "event_date": day,
        "symbol": symbol,
        "baseline_candidate_flag": True,
        "baseline_score": score,
        "combined_candidate_flag": True,
        "momentum_candidate_flag": momentum,
        "ma5_h12_candidate_flag": False,
        "momentum_low_risk_context_flag": False,
        "momentum_high_risk_context_flag": False,
        "monthly_prior_state": monthly,
        "weekly_prior_state": "weekly_prior_uptrend",
        "pre_ret20_state": "pre20_down" if monthly == "monthly_prior_down_or_drawdown" else "pre20_up",
        "pre_ret5_state": "pre5_up",
        "pre_ma20_path_state": "pre_ma20_reclaim_base",
        "ret20_fwd": ret,
        "mfe20": max(ret, 0.01),
        "mae20": min(ret, -0.01),
        "severe_loss20": ret <= -0.10,
        "win20": ret > 0.0,
        "is_future_top10_by_ret20": ret >= 0.10,
    }


def _make_duckdb(path: Path, symbols: list[str], event_day: date) -> None:
    rows = []
    start = event_day - timedelta(days=120)
    days = [start + timedelta(days=i) for i in range(121)]
    days = [d for d in days if d.weekday() < 5]
    for symbol in symbols:
        for idx, current in enumerate(days):
            if symbol in {"7327", "1002"}:
                close = 100 + idx * 0.8
                volume = 2000 if current == event_day else 1000
            else:
                close = 150 - idx * 0.4
                volume = 1000
            open_ = close - 0.5
            rows.append((symbol, current.isoformat(), open_, close + 1.0, close - 1.0, close, volume, "pan"))
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)


def _make_sources(tmp_path: Path) -> Path:
    event_day = date(2021, 4, 30)
    day = event_day.isoformat()
    rows = [
        _row(day, "1001", -0.12, 0.95),
        _row(day, "7327", 0.20, 0.94, momentum=True, monthly="monthly_prior_down_or_drawdown"),
        _row(day, "1002", 0.15, 0.93, momentum=True),
        _row(day, "1003", -0.05, 0.92),
        _row(day, "1004", -0.04, 0.91),
    ]
    pretest = tmp_path / "pretest"
    top5 = tmp_path / "top5"
    repair = tmp_path / "repair"
    db = tmp_path / "stocks.duckdb"
    _make_duckdb(db, [row["symbol"] for row in rows], event_day)
    _write_json(pretest / "research_decision.json", {"authoritative_research_decision": "starter_entry_pretest_keep"})
    _write_json(pretest / "run_manifest.json", {"source_top5_gate_root": str(top5), "source_field_repair_root": str(repair)})
    _write_json(top5 / "strict_gate_leaderboard.json", {"best_variant": {"spec": {"momentum_weight": 0.02, "momentum_low_risk_weight": -0.02, "momentum_high_risk_penalty": -0.02, "monthly_down_or_drawdown_penalty": -0.005}}})
    _write_json(repair / "run_manifest.json", {"source_duckdb": str(db)})
    _write_jsonl(repair / "repaired_common_top5_candidate_ledger.jsonl", rows)
    return pretest


def _run(tmp_path: Path) -> Path:
    args = argparse.Namespace(source_pretest_root=_make_sources(tmp_path), output_parent=tmp_path / "out", run_id="entry-run")
    return mod.run(args)


def test_entry_timing_confirmation_outputs_7327_case_and_contract(tmp_path: Path) -> None:
    output = _run(tmp_path)

    decision = _read_json(output / "research_decision.json")
    contract = _read_json(output / "entry_timing_confirmation_contract.json")
    case = _read_json(output / "symbol_7327_case_report.json")
    complete = _read_json(output / "_ARTIFACT_COMPLETE.json")

    assert decision["entry_timing_confirmation_created"] is True
    assert decision["future_labels_used_in_candidate_construction"] is False
    assert contract["confirmation_uses_future_labels"] is False
    assert case["case_count"] == 1
    assert case["confirmed_case_count"] == 1
    assert complete["complete"] is True


def test_entry_timing_confirmation_rows_and_no_mutation(tmp_path: Path) -> None:
    output = _run(tmp_path)

    rows = (output / "candidate_timing_confirmation_rows.jsonl").read_text(encoding="utf-8").strip().splitlines()
    mutation = _read_json(output / "no_mutation_audit.json")
    metrics = _read_json(output / "confirmed_candidate_metrics.json")

    assert len(rows) == 5
    assert mutation["no_mutation_pass"] is True
    assert mutation["runtime_duckdb_written"] is False
    assert metrics["confirmed_candidate_count"] >= 1
