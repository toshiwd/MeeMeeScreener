import json
from pathlib import Path

from scripts import tradex_sell_monthly_breakout_hard_filter_compare_v1 as hard_filter


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _row(as_of_date: int, rank: int, code: str, monthly_up: float, ret20: float, bad: bool, severe: bool = False) -> dict:
    return {
        "as_of_date": as_of_date,
        "rank": rank,
        "code": code,
        "side": "sell",
        "execution_available": True,
        "monthly_breakout_up_prob": monthly_up,
        "short_ret20_next_open_to_20d_close": ret20,
        "bad_pick": bad,
        "severe_loser": severe,
        "year": as_of_date // 10000,
        "month": str(as_of_date)[:6],
    }


def test_hard_filter_writes_keep_when_fixed_bucket_improves_top5(tmp_path: Path) -> None:
    source = tmp_path / "source"
    compare = tmp_path / "compare"
    _write_json(compare / "challenger_definition.json", {"thresholds": {"monthly_breakout_up_prob_low_q25": 0.10}})
    rows = [
        _row(20250131, 1, "A", 0.05, -0.08, True, True),
        _row(20250131, 2, "B", 0.20, 0.03, False),
        _row(20250131, 3, "C", 0.30, 0.02, False),
        _row(20250131, 4, "D", 0.40, 0.01, False),
        _row(20250131, 5, "E", 0.50, 0.01, False),
        _row(20250131, 6, "F", 0.60, 0.04, False),
        _row(20260131, 1, "G", 0.05, -0.07, True, True),
        _row(20260131, 2, "H", 0.20, 0.03, False),
        _row(20260131, 3, "I", 0.30, 0.02, False),
        _row(20260131, 4, "J", 0.40, 0.01, False),
        _row(20260131, 5, "K", 0.50, 0.01, False),
        _row(20260131, 6, "L", 0.60, 0.04, False),
    ]
    _write_jsonl(source / "candidate_outcome_table_top50.jsonl", rows)

    result = hard_filter.run(source_root=source, compare_root=compare, output_root=tmp_path / "out")

    decision = hard_filter._read_json(Path(result["artifact_refs"]["decision"]))
    compare_payload = hard_filter._read_json(Path(result["artifact_refs"]["compare"]))
    audit = hard_filter._read_json(Path(result["artifact_refs"]["no_lookahead_audit"]))
    assert decision["authoritative_rollup_decision"] == "keep_for_portfolio_replay"
    assert decision["production_ranking_changed"] is False
    assert audit["future_outcome_fields_used_in_selection"] == []
    assert compare_payload["delta"]["bad_pick_delta"] == -2
    assert compare_payload["delta"]["changed_top5_members_count"] == 4


def test_hard_filter_drops_when_top5_underfilled(tmp_path: Path) -> None:
    source = tmp_path / "source"
    compare = tmp_path / "compare"
    _write_json(compare / "challenger_definition.json", {"thresholds": {"monthly_breakout_up_prob_low_q25": 0.10}})
    rows = [
        _row(20250131, 1, "A", 0.05, -0.08, True),
        _row(20250131, 2, "B", 0.04, -0.01, True),
        _row(20250131, 3, "C", 0.03, -0.02, True),
        _row(20250131, 4, "D", 0.02, -0.03, True),
        _row(20250131, 5, "E", 0.01, -0.04, True),
    ]
    _write_jsonl(source / "candidate_outcome_table_top50.jsonl", rows)

    result = hard_filter.run(source_root=source, compare_root=compare, output_root=tmp_path / "out")

    decision = hard_filter._read_json(Path(result["artifact_refs"]["decision"]))
    assert decision["authoritative_rollup_decision"] == "drop_hard_filter"
    assert "hard_filter_created_underfilled_top5_dates" in decision["blockers"]
