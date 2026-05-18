import json
from pathlib import Path

from scripts import tradex_sell_monthly_breakout_hard_filter_compare_v1 as hard_filter_compare
from scripts import tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 as replay


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _row(as_of_date: int, rank: int, code: str, monthly_up: float, entry: float, exit_: float) -> dict:
    return {
        "as_of_date": as_of_date,
        "rank": rank,
        "code": code,
        "side": "sell",
        "execution_available": True,
        "monthly_breakout_up_prob": monthly_up,
        "entry_date": as_of_date + 1,
        "exit_date": as_of_date + 21,
        "entry_price": entry,
        "exit_close": exit_,
        "short_ret20_next_open_to_20d_close": (entry - exit_) / entry,
        "bad_pick": exit_ >= entry,
        "severe_loser": ((entry - exit_) / entry) <= -0.05,
        "year": as_of_date // 10000,
        "month": str(as_of_date)[:6],
    }


def _source_compare_run(tmp_path: Path, rows: list[dict]) -> Path:
    source = tmp_path / "source"
    compare_root = tmp_path / "compare_root"
    compare_run_root = tmp_path / "compare_run"
    _write_json(compare_root / "challenger_definition.json", {"thresholds": {"monthly_breakout_up_prob_low_q25": 0.10}})
    _write_jsonl(source / "candidate_outcome_table_top50.jsonl", rows)
    result = hard_filter_compare.run(source_root=source, compare_root=compare_root, output_root=compare_run_root)
    return Path(result["output_dir"])


def test_portfolio_replay_keeps_when_hard_filter_reaches_buy_level(tmp_path: Path) -> None:
    rows = [
        _row(20250110, 1, "A", 0.05, 100.0, 120.0),
        _row(20250110, 2, "B", 0.20, 100.0, 90.0),
        _row(20250110, 3, "C", 0.30, 100.0, 90.0),
        _row(20250110, 4, "D", 0.40, 100.0, 90.0),
        _row(20250110, 5, "E", 0.50, 100.0, 90.0),
        _row(20250110, 6, "F", 0.60, 100.0, 90.0),
        _row(20260110, 1, "G", 0.05, 100.0, 120.0),
        _row(20260110, 2, "H", 0.20, 100.0, 90.0),
        _row(20260110, 3, "I", 0.30, 100.0, 90.0),
        _row(20260110, 4, "J", 0.40, 100.0, 90.0),
        _row(20260110, 5, "K", 0.50, 100.0, 90.0),
        _row(20260110, 6, "L", 0.60, 100.0, 90.0),
    ]
    compare_run = _source_compare_run(tmp_path, rows)

    result = replay.run(compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = replay._read_json(Path(result["artifact_refs"]["final_portfolio_replay_decision"]))
    audit = replay._read_json(Path(result["artifact_refs"]["no_lookahead_replay_audit"]))
    assert decision["authoritative_rollup_decision"] == "keep_as_buy_level_equivalent_research_candidate"
    assert decision["buy_level_equivalence_reached"] is True
    assert audit["future_outcome_fields_used_in_selection"] == []


def test_portfolio_replay_holds_when_improved_but_negative_year_remains(tmp_path: Path) -> None:
    rows = [
        _row(20250110, 1, "A", 0.05, 100.0, 120.0),
        _row(20250110, 2, "B", 0.20, 100.0, 99.0),
        _row(20250110, 3, "C", 0.30, 100.0, 99.0),
        _row(20250110, 4, "D", 0.40, 100.0, 99.0),
        _row(20250110, 5, "E", 0.50, 100.0, 99.0),
        _row(20250110, 6, "F", 0.60, 100.0, 99.0),
        _row(20260110, 1, "G", 0.05, 100.0, 120.0),
        _row(20260110, 2, "H", 0.20, 100.0, 101.0),
        _row(20260110, 3, "I", 0.30, 100.0, 101.0),
        _row(20260110, 4, "J", 0.40, 100.0, 101.0),
        _row(20260110, 5, "K", 0.50, 100.0, 101.0),
        _row(20260110, 6, "L", 0.60, 100.0, 101.0),
    ]
    compare_run = _source_compare_run(tmp_path, rows)

    result = replay.run(compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = replay._read_json(Path(result["artifact_refs"]["final_portfolio_replay_decision"]))
    assert decision["authoritative_rollup_decision"] in {"hold_for_portfolio_risk_repair", "drop_after_portfolio_replay"}
    assert decision["buy_level_equivalence_reached"] is False
