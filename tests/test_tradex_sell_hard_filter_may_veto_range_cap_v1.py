from pathlib import Path

from scripts import tradex_sell_hard_filter_may_veto_range_cap_v1 as range_cap
from scripts import tradex_sell_hard_filter_may_veto_repair_v1 as may_veto
from scripts import tradex_sell_hard_filter_position_sizing_repair_v1 as sizing
from scripts.tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 import run as replay_run
from tests.test_tradex_sell_hard_filter_position_sizing_repair_v1 import _ready_compare_root
from tests.test_tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 import _row


def _build_chain(tmp_path: Path, rows: list[dict]) -> tuple[Path, Path]:
    compare_run = _ready_compare_root(tmp_path, rows)
    source_replay = Path(replay_run(compare_run_root=compare_run, output_root=tmp_path / "source_replay")["output_dir"])
    source_sizing = Path(sizing.run(source_replay_root=source_replay, compare_run_root=compare_run, output_root=tmp_path / "sizing")["output_dir"])
    source_may = Path(may_veto.run(source_sizing_root=source_sizing, compare_run_root=compare_run, output_root=tmp_path / "may")["output_dir"])
    return compare_run, source_may


def test_range_cap_keeps_when_it_removes_range_bad_pick_without_future_fields(tmp_path: Path) -> None:
    rows = [
        {**_row(20240505, 1, "A0", 0.05, 100.0, 130.0), "monthly_range_prob": 0.0},
        {**_row(20240605, 1, "B0", 0.05, 100.0, 130.0), "monthly_range_prob": 0.0},
        {**_row(20240605, 2, "B", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 3, "C", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 4, "D", 0.20, 100.0, 130.0), "monthly_range_prob": 0.8},
        {**_row(20240605, 5, "E", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 6, "F", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 1, "G0", 0.05, 100.0, 130.0), "monthly_range_prob": 0.0},
        {**_row(20250605, 2, "G", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 3, "H", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 4, "I", 0.20, 100.0, 130.0), "monthly_range_prob": 0.8},
        {**_row(20250605, 5, "J", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 6, "K", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
    ]
    compare_run, source_may = _build_chain(tmp_path, rows)

    result = range_cap.run(source_may_root=source_may, compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = range_cap._read_json(Path(result["artifact_refs"]["final_range_cap_decision"]))
    audit = range_cap._read_json(Path(result["artifact_refs"]["no_lookahead_audit"]))
    assert decision["authoritative_rollup_decision"] == "keep_as_buy_level_equivalent_research_candidate"
    assert audit["future_outcome_fields_used_in_selection_sizing_or_veto"] == []
    assert audit["veto_fields"] == ["as_of_date_month", "monthly_range_prob"]


def test_range_cap_holds_when_range_cap_does_not_fix_negative_year(tmp_path: Path) -> None:
    rows = [
        {**_row(20240605, 1, "A0", 0.05, 100.0, 130.0), "monthly_range_prob": 0.0},
        {**_row(20240605, 2, "A", 0.20, 100.0, 130.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 3, "B", 0.20, 100.0, 130.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 4, "C", 0.20, 100.0, 130.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 5, "D", 0.20, 100.0, 130.0), "monthly_range_prob": 0.1},
        {**_row(20240605, 6, "E", 0.20, 100.0, 130.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 1, "F0", 0.05, 100.0, 130.0), "monthly_range_prob": 0.0},
        {**_row(20250605, 2, "F", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 3, "G", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 4, "H", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 5, "I", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
        {**_row(20250605, 6, "J", 0.20, 100.0, 90.0), "monthly_range_prob": 0.1},
    ]
    compare_run, source_may = _build_chain(tmp_path, rows)

    result = range_cap.run(source_may_root=source_may, compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = range_cap._read_json(Path(result["artifact_refs"]["final_range_cap_decision"]))
    assert decision["buy_level_equivalence_reached"] is False
    assert decision["authoritative_rollup_decision"] == "hold_for_breadth_and_forward_shadow_review"
