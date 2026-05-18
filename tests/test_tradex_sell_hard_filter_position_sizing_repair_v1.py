from pathlib import Path

from scripts import tradex_sell_hard_filter_position_sizing_repair_v1 as repair
from scripts import tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 as replay
from tests.test_tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 import _row


def _write_json(path: Path, payload: dict) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _ready_compare_root(tmp_path: Path, rows: list[dict]) -> Path:
    source = tmp_path / "source"
    compare_run = tmp_path / "compare_run"
    contract = compare_run / "contract.json"
    decision = compare_run / "decision.json"
    _write_jsonl(source / "candidate_outcome_table_top50.jsonl", rows)
    _write_json(contract, {"source_root": str(source), "source_compare_root": str(tmp_path / "unused"), "threshold": 0.10})
    _write_json(decision, {"decision": "keep_for_portfolio_replay", "portfolio_replay_allowed_next": True})
    _write_json(compare_run / "_ARTIFACT_COMPLETE.json", {"artifact_refs": {"contract": str(contract), "decision": str(decision)}})
    return compare_run


def test_rank3_half_sizing_can_reach_buy_level_on_stable_sample(tmp_path: Path) -> None:
    rows = [
        _row(20250110, 1, "A0", 0.05, 100.0, 130.0),
        _row(20250110, 2, "A", 0.20, 100.0, 90.0),
        _row(20250110, 3, "B", 0.20, 100.0, 90.0),
        _row(20250110, 4, "C", 0.20, 100.0, 120.0),
        _row(20250110, 5, "D", 0.20, 100.0, 90.0),
        _row(20250110, 6, "E", 0.20, 100.0, 90.0),
        _row(20250110, 7, "F", 0.20, 100.0, 90.0),
        _row(20260110, 1, "G0", 0.05, 100.0, 130.0),
        _row(20260110, 2, "G", 0.20, 100.0, 90.0),
        _row(20260110, 3, "H", 0.20, 100.0, 90.0),
        _row(20260110, 4, "I", 0.20, 100.0, 120.0),
        _row(20260110, 5, "J", 0.20, 100.0, 90.0),
        _row(20260110, 6, "K", 0.20, 100.0, 90.0),
        _row(20260110, 7, "L", 0.20, 100.0, 90.0),
    ]
    compare_run = _ready_compare_root(tmp_path, rows)
    source_replay = Path(replay.run(compare_run_root=compare_run, output_root=tmp_path / "source_replay")["output_dir"])

    result = repair.run(source_replay_root=source_replay, compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = repair._read_json(Path(result["artifact_refs"]["final_position_sizing_decision"]))
    audit = repair._read_json(Path(result["artifact_refs"]["no_lookahead_audit"]))
    assert decision["authoritative_rollup_decision"] == "keep_as_buy_level_equivalent_research_candidate"
    assert audit["future_outcome_fields_used_in_selection_or_sizing"] == []
    assert audit["sizing_fields"] == ["rank"]


def test_rank3_half_sizing_holds_when_negative_year_remains(tmp_path: Path) -> None:
    rows = [
        _row(20250110, 1, "A0", 0.05, 100.0, 130.0),
        _row(20250110, 2, "A", 0.20, 100.0, 120.0),
        _row(20250110, 3, "B", 0.20, 100.0, 120.0),
        _row(20250110, 4, "C", 0.20, 100.0, 120.0),
        _row(20250110, 5, "D", 0.20, 100.0, 90.0),
        _row(20250110, 6, "E", 0.20, 100.0, 90.0),
        _row(20250110, 7, "F", 0.20, 100.0, 90.0),
        _row(20260110, 1, "G0", 0.05, 100.0, 130.0),
        _row(20260110, 2, "G", 0.20, 100.0, 90.0),
        _row(20260110, 3, "H", 0.20, 100.0, 90.0),
        _row(20260110, 4, "I", 0.20, 100.0, 90.0),
        _row(20260110, 5, "J", 0.20, 100.0, 120.0),
        _row(20260110, 6, "K", 0.20, 100.0, 120.0),
        _row(20260110, 7, "L", 0.20, 100.0, 120.0),
    ]
    compare_run = _ready_compare_root(tmp_path, rows)
    source_replay = Path(replay.run(compare_run_root=compare_run, output_root=tmp_path / "source_replay")["output_dir"])

    result = repair.run(source_replay_root=source_replay, compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = repair._read_json(Path(result["artifact_refs"]["final_position_sizing_decision"]))
    assert decision["buy_level_equivalence_reached"] is False
    assert decision["authoritative_rollup_decision"] in {
        "hold_for_remaining_year_stability_repair",
        "drop_position_sizing_repair",
    }
