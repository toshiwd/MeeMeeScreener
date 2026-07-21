import json

from scripts.tradex_short_pattern_router_v1 import _extreme_timing_metrics, _kept, _watchlist_codes, build, classify, write_artifact_set


KEEP = {"decision": {"candidate_local_decision": "keep", "overall_readiness_pass": True}, "overall": {"sample_count": 103}}
HIGH_KEEP = {"decision": {"candidate_local_decision": "keep"}, "best": {"full_metrics": {"row_count": 185}}}
EXTREME_KEEP = {"decision": {"candidate_local_decision": "keep"}, "chosen_candidate": {"metrics": {"row_count": 362, "ret20_positive_rate": 0.6436}}}
EXTREME_CORRECTED_KEEP = {"judgment": {"candidate_local_decision": "keep"}, "authoritative_result": {"chosen_candidate": "second_down_w5", "candidates": {"second_down_w5": {"metrics": {"row_count": 484, "win_rate": 0.5455, "mean_return": 0.0143}}}}}
BLOCK = {"authoritative_rollup_decision": {"candidate_local_decision": "hold"}}
TRANSITION_BOTH = {"decision": {"selected_policies": {"low20_break_relative_weakness": "family_wait", "high_zone_climax": "next_open"}}}
TRANSITION_LOW_ONLY = {"decision": {"selected_policies": {"low20_break_relative_weakness": "family_wait", "high_zone_climax": None}}}


def row(**overrides):
    value = {"code": "1111", "as_of": 20260716, "low20_dist": 0.01, "breakout20_down": -0.04, "rel_ret20": -0.06, "close": 90, "ma7": 100}
    value.update(overrides)
    return value


def test_routes_kept_event_to_wait_without_chasing_low():
    assert classify(row(), low20_enabled=True, high_zone_enabled=True, extreme_roll_enabled=True, block_far_from_low=True)["action"] == "戻り待ち"


def test_routes_next_open_and_ma7_pullback_to_entry():
    assert classify(row(next_open_available=True), low20_enabled=True, high_zone_enabled=True, extreme_roll_enabled=True, block_far_from_low=True)["action"] == "戻り待ち"
    assert classify(row(close=99.5), low20_enabled=True, high_zone_enabled=True, extreme_roll_enabled=True, block_far_from_low=True)["action"] == "今日売れる"


def test_rejects_unmatched_and_bullish_denial():
    avoided = classify(row(rel_ret20=-0.01), low20_enabled=True, high_zone_enabled=True, extreme_roll_enabled=True, block_far_from_low=True)
    assert avoided["action"] == "売り回避"
    assert avoided["add_condition"] and avoided["invalidation"] and len(avoided["historical_reference"]) == 3
    assert classify(row(bullish_denial=True), low20_enabled=True, high_zone_enabled=True, extreme_roll_enabled=True, block_far_from_low=True)["action"] == "反転否定"


def test_routes_high_zone_family_independently():
    high = row(low20_dist=0.5, breakout20_down=0, rel_ret20=0.4, ret20=0.9, dist_ma20=0.15, close_range_pos=0.95, close_pos60=0.99, next_open_available=True)
    result = classify(high, low20_enabled=True, high_zone_enabled=True, extreme_roll_enabled=True, block_far_from_low=True)
    assert result["rule_id"] == "high_zone_climax"
    assert result["action"] == "今日売れる"


def test_board_requires_both_authoritative_keep_gates():
    board = build([row()], KEEP, KEEP, BLOCK, HIGH_KEEP, HIGH_KEEP, TRANSITION_BOTH, EXTREME_KEEP, EXTREME_KEEP)
    assert board["decision"]["candidate_local_decision"] == "keep"
    assert board["summary"]["action_counts"] == {"戻り待ち": 1}
    failed = build([row()], KEEP, {"decision": {"candidate_local_decision": "drop", "overall_readiness_pass": False}}, BLOCK, HIGH_KEEP, HIGH_KEEP, TRANSITION_BOTH, EXTREME_KEEP, EXTREME_KEEP)
    assert failed["items"][0]["action"] == "売り回避"


def test_watchlist_parser_deduplicates_codes(tmp_path):
    path = tmp_path / "code.txt"
    path.write_text("1111 first\n2222\n1111 duplicate\n", encoding="utf-8")
    assert _watchlist_codes(path) == ["1111", "2222"]


def test_writes_authoritative_artifact_set(tmp_path):
    board = build([row()], KEEP, KEEP, BLOCK, HIGH_KEEP, HIGH_KEEP, TRANSITION_BOTH, EXTREME_KEEP, EXTREME_KEEP, {"watchlist_count": 1, "confirmed_as_of": 20260716, "provisional_as_of": 20260717})
    output = tmp_path / "board.json"
    write_artifact_set(output, board, KEEP, KEEP, HIGH_KEEP, HIGH_KEEP, TRANSITION_BOTH, EXTREME_KEEP, EXTREME_KEEP)
    assert json.loads((tmp_path / "compare.json").read_text(encoding="utf-8"))["verify"]["active_family_keep_at_least_2"] is True
    assert json.loads((tmp_path / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))["status"] == "complete"


def test_transition_drop_disables_high_zone_family():
    high = row(low20_dist=0.5, breakout20_down=0, rel_ret20=0.4, ret20=0.9, dist_ma20=0.15, close_range_pos=0.95, close_pos60=0.99, next_open_available=True)
    board = build([high], KEEP, KEEP, BLOCK, HIGH_KEEP, HIGH_KEEP, TRANSITION_LOW_ONLY, EXTREME_KEEP, EXTREME_KEEP)
    assert board["items"][0]["action"] == "売り回避"
    assert board["decision"]["candidate_local_decision"] == "keep"


def test_accepts_corrected_extreme_timing_artifact_shape():
    assert _kept(EXTREME_CORRECTED_KEEP) is True
    assert _extreme_timing_metrics(EXTREME_CORRECTED_KEEP)["row_count"] == 484
    board = build([row()], KEEP, KEEP, BLOCK, HIGH_KEEP, HIGH_KEEP, TRANSITION_LOW_ONLY, EXTREME_KEEP, EXTREME_CORRECTED_KEEP)
    assert board["fixed_evaluation_conditions"]["entry_modes"]["extreme_high_roll"] == ["confirmation_close_within_5_sessions"]


def test_routes_extreme_roll_wait_confirm_and_denial():
    wait = row(low20_dist=0.5, rel_ret20=0.2, extreme_roll_setup_age=0)
    assert classify(wait, low20_enabled=True, high_zone_enabled=False, extreme_roll_enabled=True, block_far_from_low=True)["action"] == "戻り待ち"
    confirmed = row(low20_dist=0.5, rel_ret20=0.2, extreme_roll_setup_age=1, extreme_roll_timing_confirmed=True)
    result = classify(confirmed, low20_enabled=True, high_zone_enabled=False, extreme_roll_enabled=True, block_far_from_low=True)
    assert result["rule_id"] == "extreme_high_roll" and result["action"] == "今日売れる"
    denied = row(low20_dist=0.5, rel_ret20=0.2, extreme_roll_setup_age=1, extreme_roll_bullish_denial=True)
    assert classify(denied, low20_enabled=True, high_zone_enabled=False, extreme_roll_enabled=True, block_far_from_low=True)["action"] == "反転否定"
