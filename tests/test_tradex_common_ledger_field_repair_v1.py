from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import duckdb

from scripts import tradex_common_ledger_field_repair_v1 as repair


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _make_daily_db(path: Path, symbol: str = "1001", days: int = 30) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
    start = date(2020, 1, 1)
    rows = []
    for i in range(days):
        day = start + timedelta(days=i)
        ymd = int(day.strftime("%Y%m%d"))
        close = 100.0 + i
        rows.append((symbol, ymd, 100.0 + i * 0.5, close + 2.0, close - 2.0, close, 1000, "pan"))
    con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()


def _make_source_roots(tmp_path: Path, rows: list[dict]) -> tuple[Path, Path]:
    common_parent = tmp_path / "common"
    risk_parent = tmp_path / "risk"
    common_root = common_parent / "common-run"
    risk_root = risk_parent / "risk-run"
    _write_json(common_root / "research_decision.json", {"decision": "hold"})
    _write_json(common_root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    _write_jsonl(common_root / "common_top5_candidate_ledger.jsonl", rows)
    _write_json(risk_root / "research_decision.json", {"decision": "ready_for_common_top5_candidate_ledger_build"})
    return common_parent, risk_parent


def _base_ma5_row(**overrides: object) -> dict:
    row = {
        "event_date": "2020-01-01",
        "symbol": "1001",
        "baseline_candidate_flag": False,
        "momentum_candidate_flag": False,
        "ma5_h12_candidate_flag": True,
        "combined_candidate_flag": True,
        "source_family_flags": {"ma5_h12_near_bull_ma60_rising": True},
        "baseline_score": None,
        "baseline_rank": None,
        "momentum_score": None,
        "momentum_rank": None,
        "ma5_h12_context_score": None,
        "ma5_h12_rank": None,
        "combined_score": None,
        "shadow_candidate_rank": None,
        "ret20_fwd": None,
        "mfe20": None,
        "mae20": None,
        "severe_loss20": None,
        "win20": None,
        "is_big_winner_ret20_ge_10pct": None,
        "is_future_top10_by_ret20": None,
        "ma5_exit_ret": 0.99,
        "ma5_exit_mfe": 0.99,
        "ma5_exit_mae": 0.99,
        "ma5_exit_severe_loss": False,
    }
    row.update(overrides)
    return row


def _run(tmp_path: Path, rows: list[dict], *, daily_days: int = 30) -> Path:
    db_path = tmp_path / "stocks.duckdb"
    _make_daily_db(db_path, days=daily_days)
    common_parent, risk_parent = _make_source_roots(tmp_path, rows)
    args = argparse.Namespace(
        source_common_ledger_run_id="common-run",
        source_risk_decomposition_run_id="risk-run",
        run_id="repair-run",
        source_common_ledger_parent=common_parent,
        source_risk_parent=risk_parent,
        source_duckdb=db_path,
        output_parent=tmp_path / "out",
    )
    return repair.run(args)


def test_happy_path_repairs_ma5_forward_labels_without_substituting_exit_labels(tmp_path: Path) -> None:
    output_root = _run(tmp_path, [_base_ma5_row()])

    rows = _read_jsonl(output_root / "repaired_common_top5_candidate_ledger.jsonl")
    decision = _read_json(output_root / "research_decision.json")
    report = _read_json(output_root / "ma5_h12_label_repair_report.json")

    assert decision["authoritative_research_decision"] == "common_ledger_fields_repaired_ready_for_top5_validation"
    assert report["ma5_h12_rows_repaired_count"] == 1
    assert rows[0]["ret20_label_available"] is True
    assert rows[0]["ret20_fwd"] != rows[0]["ma5_exit_ret"]
    assert rows[0]["ma5_exit_labels_used_as_ret20_labels"] is False
    assert rows[0]["membership_flags_changed"] is False
    assert rows[0]["candidate_construction_changed"] is False


def test_incomplete_forward_window_holds_and_marks_unavailable_reason(tmp_path: Path) -> None:
    output_root = _run(tmp_path, [_base_ma5_row()], daily_days=10)

    rows = _read_jsonl(output_root / "repaired_common_top5_candidate_ledger.jsonl")
    decision = _read_json(output_root / "research_decision.json")
    report = _read_json(output_root / "ma5_h12_label_repair_report.json")

    assert decision["authoritative_research_decision"] == "common_ledger_field_repair_failed"
    assert report["ma5_h12_rows_repaired_count"] == 0
    assert rows[0]["ret20_label_available"] is False
    assert rows[0]["label_unavailable_reason"].startswith("incomplete_forward_window:")


def test_artifact_complete_includes_itself(tmp_path: Path) -> None:
    output_root = _run(tmp_path, [_base_ma5_row()])

    complete = _read_json(output_root / "_ARTIFACT_COMPLETE.json")

    assert complete["complete"] is True
    assert complete["artifacts"]["_ARTIFACT_COMPLETE.json"]["exists"] is True
    for name in repair.REQUIRED_ARTIFACTS:
        assert complete["artifacts"][name]["exists"] is True
        assert complete["artifacts"][name]["bytes"] > 0
