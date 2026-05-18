from pathlib import Path

from scripts import tradex_sell_hard_filter_may_veto_repair_v1 as may_veto
from scripts import tradex_sell_hard_filter_position_sizing_repair_v1 as sizing
from scripts.tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 import run as replay_run
from tests.test_tradex_sell_hard_filter_position_sizing_repair_v1 import _ready_compare_root
from tests.test_tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 import _row


def test_may_veto_can_reach_buy_level_without_future_fields(tmp_path: Path) -> None:
    rows = [
        _row(20240505, 1, "A0", 0.05, 100.0, 130.0),
        _row(20240505, 2, "A", 0.20, 100.0, 130.0),
        _row(20240505, 3, "B", 0.20, 100.0, 130.0),
        _row(20240505, 4, "C", 0.20, 100.0, 130.0),
        _row(20240505, 5, "D", 0.20, 100.0, 130.0),
        _row(20240505, 6, "E", 0.20, 100.0, 130.0),
        _row(20240605, 1, "F0", 0.05, 100.0, 130.0),
        _row(20240605, 2, "F", 0.20, 100.0, 90.0),
        _row(20240605, 3, "G", 0.20, 100.0, 90.0),
        _row(20240605, 4, "H", 0.20, 100.0, 90.0),
        _row(20240605, 5, "I", 0.20, 100.0, 90.0),
        _row(20240605, 6, "J", 0.20, 100.0, 90.0),
        _row(20250605, 1, "K0", 0.05, 100.0, 130.0),
        _row(20250605, 2, "K", 0.20, 100.0, 90.0),
        _row(20250605, 3, "L", 0.20, 100.0, 90.0),
        _row(20250605, 4, "M", 0.20, 100.0, 90.0),
        _row(20250605, 5, "N", 0.20, 100.0, 90.0),
        _row(20250605, 6, "O", 0.20, 100.0, 90.0),
    ]
    compare_run = _ready_compare_root(tmp_path, rows)
    source_replay = Path(replay_run(compare_run_root=compare_run, output_root=tmp_path / "source_replay")["output_dir"])
    source_sizing = Path(sizing.run(source_replay_root=source_replay, compare_run_root=compare_run, output_root=tmp_path / "sizing")["output_dir"])

    result = may_veto.run(source_sizing_root=source_sizing, compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = may_veto._read_json(Path(result["artifact_refs"]["final_may_veto_decision"]))
    audit = may_veto._read_json(Path(result["artifact_refs"]["no_lookahead_audit"]))
    assert decision["authoritative_rollup_decision"] == "keep_as_buy_level_equivalent_research_candidate"
    assert audit["future_outcome_fields_used_in_selection_sizing_or_veto"] == []
    assert audit["veto_fields"] == ["as_of_date_month"]


def test_may_veto_holds_or_drops_when_non_may_year_still_negative(tmp_path: Path) -> None:
    rows = [
        _row(20240605, 1, "F0", 0.05, 100.0, 130.0),
        _row(20240605, 2, "F", 0.20, 100.0, 130.0),
        _row(20240605, 3, "G", 0.20, 100.0, 130.0),
        _row(20240605, 4, "H", 0.20, 100.0, 130.0),
        _row(20240605, 5, "I", 0.20, 100.0, 130.0),
        _row(20240605, 6, "J", 0.20, 100.0, 130.0),
        _row(20250605, 1, "K0", 0.05, 100.0, 130.0),
        _row(20250605, 2, "K", 0.20, 100.0, 90.0),
        _row(20250605, 3, "L", 0.20, 100.0, 90.0),
        _row(20250605, 4, "M", 0.20, 100.0, 90.0),
        _row(20250605, 5, "N", 0.20, 100.0, 90.0),
        _row(20250605, 6, "O", 0.20, 100.0, 90.0),
    ]
    compare_run = _ready_compare_root(tmp_path, rows)
    source_replay = Path(replay_run(compare_run_root=compare_run, output_root=tmp_path / "source_replay")["output_dir"])
    source_sizing = Path(sizing.run(source_replay_root=source_replay, compare_run_root=compare_run, output_root=tmp_path / "sizing")["output_dir"])

    result = may_veto.run(source_sizing_root=source_sizing, compare_run_root=compare_run, output_root=tmp_path / "out")

    decision = may_veto._read_json(Path(result["artifact_refs"]["final_may_veto_decision"]))
    assert decision["buy_level_equivalence_reached"] is False
    assert decision["authoritative_rollup_decision"] in {
        "hold_for_breadth_and_forward_shadow_review",
        "drop_may_veto_repair",
    }
