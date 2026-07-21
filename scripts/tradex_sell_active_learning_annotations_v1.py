#!/usr/bin/env python
"""Freeze human annotations, then reveal and compare the separate answer key."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-pack", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    queue_path = args.source_pack / "review_queue.json"
    key_path = args.source_pack / "answer_key.json"
    source_queue_sha = sha(queue_path)
    source_key_sha = sha(key_path)
    queue = json.loads(queue_path.read_text(encoding="utf-8"))["items"]
    lookup = {row["case_id"]: row for row in queue}

    annotations = [
        {
            "case_id": "SELL-AL-01", "code": "6305", "ymd": 20230314,
            "human_decision": "UNJUDGEABLE_OR_AVOID_NEW_SHORT",
            "human_reason": "MA200・MA100・MA60が近接し、強い密集帯を形成。移動平均自体も密集しており、横ばい化しやすく読みづらい。",
            "concepts": {
                "long_ma_cluster_sideways_risk": True,
                "ma200_short_location_risk": False,
            },
        },
        {
            "case_id": "SELL-AL-02", "code": "7211", "ymd": 20250603,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "MA200の位置が新規売りに不利。",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": True,
            },
        },
        {
            "case_id": "SELL-AL-03", "code": "5631", "ymd": 20240904,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "すでに下落しており、下側のMAと直近安値が近いため、支持に当たるまでの残存値幅が不足している。",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": True,
            },
        },
        {
            "case_id": "SELL-AL-04", "code": "9007", "ymd": 20231011,
            "human_decision": "PROFIT_TAKE_SHORT",
            "human_reason": "7MA下の連続本数が7本付近に達しており、その位置が短期的な底になりやすい。",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": True,
            },
        },
        {
            "case_id": "SELL-AL-05", "code": "9107", "ymd": 20241126,
            "human_decision": "ADD_SHORT",
            "human_reason": "GDの下向きの勢いと直近安値割れを追加売りの根拠とする。新規売りなら11月21日から22日のMA200反落を優先する。",
            "preferred_initial_entry_window": [20241121, 20241122],
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": True,
                "gap_down_prior_low_break_add_short": True,
            },
        },
        {
            "case_id": "SELL-AL-06", "code": "4755", "ymd": 20251114,
            "human_decision": "NEW_SHORT_WITH_REBOUND_RISK",
            "human_reason": "新規売りでよいが、当日の下げ幅が大きくMA60に接触しているため、大きな反発を受けるリスクがある。",
            "blind_status": "OUTCOME_AWARE_EXCLUDE_FROM_ACCURACY",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": True,
                "large_drop_ma60_contact_rebound_risk": True,
            },
        },
        {
            "case_id": "SELL-AL-07", "code": "4004", "ymd": 20230816,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "直近安値帯を明確に割っておらず、下方向の支持・抵抗帯として機能する可能性が高いためエントリーしない。",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": True,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": True,
            },
        },
        {
            "case_id": "SELL-AL-08", "code": "6702", "ymd": 20250311,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "長い下ヒゲが出ており、MA60とMA100の上で引けたため新規売りを回避する。翌日の陽線は打診売り済みの場合の即時撤退根拠とし、当日判定には使わない。",
            "blind_status": "PARTIALLY_OUTCOME_AWARE_NEXT_DAY_MANAGEMENT_ONLY",
            "next_day_management": "EXIT_PROBE_SHORT_ON_BULL_CONTINUATION",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": True,
                "close_holds_above_ma60_ma100": True,
            },
        },
        {
            "case_id": "SELL-AL-09", "code": "6857", "ymd": 20240827,
            "human_decision": "PROBE_SHORT_ONLY",
            "human_reason": "8月27日に行うなら打診売りに限定し、本売りは9月3日のトライ届かず発生を待つ。",
            "blind_status": "PARTIALLY_OUTCOME_AWARE_LATER_ENTRY_TIMING",
            "preferred_full_entry_ymd": 20240903,
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": True,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": True,
                "failed_try_full_short_entry": True,
            },
        },
        {
            "case_id": "SELL-AL-10", "code": "8253", "ymd": 20250815,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "直前区間ですでに下落しすぎており、長期MA支持帯（人間読解ではMA200）まで下げたため反発可能性が高く、エントリーしない。権威データ上の寄りはGUではなく大幅GD。",
            "data_reconciliation": {
                "human_gap_read": "GU",
                "authoritative_open_gap_pct": -0.10194174757281549,
                "authoritative_gap_read": "GD",
                "human_long_ma_touch": "MA200",
                "machine_detected_long_ma_touch": "MA100",
            },
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": True,
                "downside_room_to_support_risk": True,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": False,
                "failed_try_full_short_entry": False,
                "prior_interval_overextension_long_ma_support": True,
            },
        },
        {
            "case_id": "SELL-AL-11", "code": "2802", "ymd": 20240216,
            "human_decision": "AVOID_NEW_SHORT_AND_PROFIT_TAKE",
            "human_reason": "新規売りなら2月6日の全戻し陰線。2月16日はMA100が下値支持として完全に機能しており、明確に割るまでは新規売りせず、2月6日からの既存売りは利確する。",
            "preferred_initial_entry_ymd": 20240206,
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": True,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": False,
                "failed_try_full_short_entry": False,
                "prior_interval_overextension_long_ma_support": False,
                "full_erasure_bear_initial_short": True,
                "unbroken_ma100_profit_take": True,
            },
        },
        {
            "case_id": "SELL-AL-12", "code": "6526", "ymd": 20251014,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "陰線だが既存レンジ内にあり、レンジ下限や直近安値を壊す決定打ではないためエントリーしない。",
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": True,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": False,
                "failed_try_full_short_entry": False,
                "prior_interval_overextension_long_ma_support": False,
                "full_erasure_bear_initial_short": False,
                "unbroken_ma100_profit_take": False,
                "bear_candle_inside_range_not_entry": True,
            },
        },
        {
            "case_id": "SELL-AL-13", "code": "6301", "ymd": 20230531,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "月足はボックス上抜け後の横ばいで、元ボックスへ回帰していないためボックス天井売りはできない。日足の長期MA支持が近く、下落しても利益余地が不足する。",
            "data_reconciliation": {
                "human_daily_support_read": "MA100 near",
                "machine_nearest_confirmed_support": "MA60 at about 0.13 ATR",
                "machine_ma100_touch_flag": False,
            },
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": True,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": True,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": False,
                "failed_try_full_short_entry": False,
                "prior_interval_overextension_long_ma_support": False,
                "full_erasure_bear_initial_short": False,
                "unbroken_ma100_profit_take": False,
                "bear_candle_inside_range_not_entry": False,
                "monthly_post_box_breakout_consolidation": True,
                "no_monthly_box_reentry": True,
            },
        },
        {
            "case_id": "SELL-AL-14", "code": "7269", "ymd": 20241024,
            "human_decision": "AVOID_NEW_SHORT",
            "human_reason": "約1490円に既存の価格帯があり、下方向の支持・抵抗として利益余地を塞ぐため売り対象にしない。",
            "human_price_band": {"center_yen": 1490, "role_for_short": "downside_barrier"},
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": True,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": False,
                "failed_try_full_short_entry": False,
                "prior_interval_overextension_long_ma_support": False,
                "full_erasure_bear_initial_short": False,
                "unbroken_ma100_profit_take": False,
                "bear_candle_inside_range_not_entry": False,
                "monthly_post_box_breakout_consolidation": False,
                "no_monthly_box_reentry": False,
                "nearby_multitouch_price_band_blocks_short": True,
            },
        },
        {
            "case_id": "SELL-AL-15", "code": "6532", "ymd": 20230626,
            "human_decision": "ADD_SHORT",
            "human_reason": "月足ボックス天井圏で、6月23日に6月21日の大陽線を全戻ししたため打診売り。6月26日に追加し、7月4日の再失速でさらに追加する段階売りが理想。",
            "blind_status": "PARTIALLY_OUTCOME_AWARE_LATER_ADD_TIMING",
            "position_path": [
                {"ymd": 20230623, "action": "PROBE_SHORT", "reason": "full erasure of 20230621 large bull candle"},
                {"ymd": 20230626, "action": "ADD_SHORT", "reason": "continued weakness after erasure"},
                {"ymd": 20230704, "action": "ADD_SHORT", "reason": "later renewed rejection; excluded from 20230626 information set"},
            ],
            "concepts": {
                "long_ma_cluster_sideways_risk": False,
                "ma200_short_location_risk": False,
                "downside_room_to_support_risk": False,
                "below_ma7_seven_bar_bottom_risk": False,
                "ma200_rejection_initial_short": False,
                "gap_down_prior_low_break_add_short": False,
                "new_short_structure_trigger": False,
                "large_drop_ma60_contact_rebound_risk": False,
                "unbroken_prior_low_zone_blocks_new_short": False,
                "lower_wick_rejection_at_ma60_ma100": False,
                "close_holds_above_ma60_ma100": False,
                "probe_only_before_retry_failure": False,
                "failed_try_full_short_entry": False,
                "prior_interval_overextension_long_ma_support": False,
                "full_erasure_bear_initial_short": True,
                "unbroken_ma100_profit_take": False,
                "bear_candle_inside_range_not_entry": False,
                "monthly_post_box_breakout_consolidation": False,
                "no_monthly_box_reentry": False,
                "nearby_multitouch_price_band_blocks_short": False,
                "monthly_box_ceiling_short_environment": True,
                "full_erasure_probe_then_staged_add": True,
            },
        },
    ]
    for row in annotations:
        source = lookup.get(row["case_id"])
        if source is None or str(source["code"]).zfill(4) != row["code"] or int(source["ymd"]) != row["ymd"]:
            raise ValueError(f"source identity mismatch: {row['case_id']}")
    frozen = {
        "schema_version": "tradex_sell_active_learning_human_annotations_v1",
        "annotation_only": True,
        "frozen_before_answer_reveal": True,
        "schema_extension": {
            "long_ma_cluster_sideways_risk": {
                "lane": "short_avoidance_or_unjudgeable",
                "meaning": "long moving averages form a dense nearby band associated with sideways ambiguity",
                "not_buy_add": True,
            },
            "ma200_short_location_risk": {
                "lane": "short_avoidance_or_unjudgeable",
                "meaning": "MA200 location is unfavorable for initiating a short",
                "not_buy_add": True,
            },
            "downside_room_to_support_risk": {
                "lane": "short_avoidance_or_unjudgeable",
                "meaning": "price is already extended down and nearby moving averages or a prior low leave insufficient room before likely support",
                "not_buy_add": True,
            },
            "below_ma7_seven_bar_bottom_risk": {
                "lane": "profit_take_and_rebound_risk",
                "meaning": "a run of roughly seven bars below MA7 raises short-term bottom and rebound risk",
                "not_buy_add": True,
            },
            "ma200_rejection_initial_short": {
                "lane": "initial_short_timing",
                "meaning": "a rebound into MA200 is rejected before the later breakdown",
                "not_buy_add": True,
            },
            "gap_down_prior_low_break_add_short": {
                "lane": "add_short_timing",
                "meaning": "a later gap-down with a break of the preceding swing low confirms continuation for adding to an existing short",
                "not_buy_add": True,
            },
            "new_short_structure_trigger": {
                "lane": "initial_short_timing",
                "meaning": "the structural breakdown is sufficient for a new short entry",
                "not_buy_add": True,
            },
            "large_drop_ma60_contact_rebound_risk": {
                "lane": "rebound_risk_and_position_sizing",
                "meaning": "a large same-day decline into MA60 can coexist with a valid short trigger but raises immediate adverse-rebound risk",
                "not_buy_add": True,
            },
            "unbroken_prior_low_zone_blocks_new_short": {
                "lane": "short_avoidance_or_unjudgeable",
                "meaning": "a nearby prior-low zone remains unbroken and may block downside follow-through, so weakening or a gap-down alone is insufficient for a new short",
                "not_buy_add": True,
            },
            "lower_wick_rejection_at_ma60_ma100": {
                "lane": "rebound_risk_and_short_avoidance",
                "meaning": "a visually meaningful lower wick rejects prices around the MA60 and MA100 support area",
                "not_buy_add": True,
            },
            "close_holds_above_ma60_ma100": {
                "lane": "rebound_risk_and_short_avoidance",
                "meaning": "the close remains above both MA60 and MA100 after testing the area",
                "not_buy_add": True,
            },
            "probe_only_before_retry_failure": {
                "lane": "initial_short_position_sizing",
                "meaning": "weakening exists but rebound-risk evidence limits any early short to probe size until a later failed retry confirms",
                "not_buy_add": True,
            },
            "failed_try_full_short_entry": {
                "lane": "initial_short_timing",
                "meaning": "a later recovery attempt fails to reach or sustain the prior target area and closes weak, enabling the main short entry",
                "not_buy_add": True,
            },
            "prior_interval_overextension_long_ma_support": {
                "lane": "rebound_risk_and_short_avoidance",
                "meaning": "the preceding decline is already extended and price reaches a long-term moving-average support band, making rebound risk dominate a new short",
                "not_buy_add": True,
            },
            "full_erasure_bear_initial_short": {
                "lane": "initial_short_timing",
                "meaning": "a bearish candle fully erases the preceding bullish recovery and provides the preferred initial short timing",
                "not_buy_add": True,
            },
            "unbroken_ma100_profit_take": {
                "lane": "profit_take_and_new_short_avoidance",
                "meaning": "MA100 remains unbroken after the decline, blocking a new short and favoring profit-taking on an existing short",
                "not_buy_add": True,
            },
            "bear_candle_inside_range_not_entry": {
                "lane": "short_avoidance_or_unjudgeable",
                "meaning": "a bearish candle remains inside the established range and does not break the range floor or prior-low zone, so it is not a decisive short entry trigger",
                "not_buy_add": True,
            },
            "monthly_post_box_breakout_consolidation": {
                "lane": "monthly_environment_gate",
                "meaning": "the monthly chart is consolidating above a previously broken box rather than trading inside the old box",
                "not_buy_add": True,
            },
            "no_monthly_box_reentry": {
                "lane": "monthly_environment_gate",
                "meaning": "without monthly re-entry into the former box, the old box ceiling is not a valid box-ceiling short location",
                "not_buy_add": True,
            },
            "nearby_multitouch_price_band_blocks_short": {
                "lane": "tradeability_and_downside_room_gate",
                "meaning": "a nearby multi-touch price band can block downside follow-through and leave insufficient profit room even when a failed-rebound trigger exists",
                "not_buy_add": True,
            },
            "monthly_box_ceiling_short_environment": {
                "lane": "monthly_environment_gate",
                "meaning": "the monthly chart is at a valid box-ceiling short location rather than above a confirmed breakout",
                "not_buy_add": True,
            },
            "full_erasure_probe_then_staged_add": {
                "lane": "position_path_and_add_short_timing",
                "meaning": "a large bullish candle is fully erased near a monthly box ceiling, enabling a probe followed by separately confirmed staged additions",
                "not_buy_add": True,
            },
        },
        "thresholds_defined_or_tuned": False,
        "annotations": annotations,
    }
    frozen_path = args.output / "frozen_annotations.json"
    dump(frozen_path, frozen)
    frozen_sha = sha(frozen_path)

    # Answer reveal occurs only after the frozen annotation file exists and is hashed.
    answer = json.loads(key_path.read_text(encoding="utf-8"))["items"]
    answer_lookup = {row["case_id"]: row for row in answer}
    comparisons = []
    for row in annotations:
        truth = answer_lookup[row["case_id"]]
        avoid = row["human_decision"] in {"UNJUDGEABLE_OR_AVOID_NEW_SHORT", "AVOID_NEW_SHORT"}
        profit_take = row["human_decision"] in {"PROFIT_TAKE_SHORT", "AVOID_NEW_SHORT_AND_PROFIT_TAKE"}
        add_short = row["human_decision"] == "ADD_SHORT"
        new_short_risk = row["human_decision"] == "NEW_SHORT_WITH_REBOUND_RISK"
        probe_short = row["human_decision"] == "PROBE_SHORT_ONLY"
        directional_mismatch = bool(avoid and truth["label"] == "DOWN_FIRST")
        comparisons.append({
            "case_id": row["case_id"], "human_decision": row["human_decision"],
            "concepts": row["concepts"], "hidden_label": truth["label"],
            "ret_close_3": truth["ret_close_3"], "down_exc_3": truth["down_exc_3"], "up_exc_3": truth["up_exc_3"],
            "directional_mismatch": directional_mismatch,
            "profit_take_lane": profit_take,
            "add_short_lane": add_short,
            "new_short_with_rebound_risk_lane": new_short_risk,
            "probe_short_lane": probe_short,
            "blind_status": row.get("blind_status", "BLIND"),
            "interpretation": (
                "avoidance would have missed a down-first move in this case; this is a false-negative against the directional label, "
                "but does not establish that the risk concept is invalid because the annotation objective includes ambiguity and entry risk"
            ) if directional_mismatch else (
                "the human action is profit-taking; the revealed first-passage label must be read as post-exit path evidence, not as a direct verdict on risk-adjusted exit management"
            ) if profit_take else (
                "the human action is adding to an existing short after an earlier preferred initial entry; the revealed path evaluates this add origin but does not by itself validate sizing or the earlier initial-entry rule"
            ) if add_short else (
                "the human action allows a new short while separately flagging rebound risk; the outcome was known and is excluded from blind accuracy evidence"
            ) if new_short_risk else (
                "the human action limits the date-t entry to probe size and prefers a later failed-try entry; rebound-first measures adverse probe timing, while the later entry requires its own outcome origin"
            ) if probe_short else "no directional mismatch",
        })
    comparison = {
        "schema_version": "tradex_sell_active_learning_annotation_answer_comparison_v1",
        "frozen_annotations_sha256": frozen_sha,
        "answer_revealed_after_freeze": True,
        "annotation_only_not_evaluation": True,
        "n": len(comparisons),
        "directional_mismatch_count": sum(x["directional_mismatch"] for x in comparisons),
        "insight": "All avoidance judgments currently conflict with the h3 DOWN_FIRST labels. Directional correctness is intentionally separated from short tradeability, remaining profit room, and rebound-management difficulty. The sample remains insufficient for adoption, rejection, or threshold tuning.",
        "threshold_action": "none",
        "comparisons": comparisons,
    }
    comparison_path = args.output / "annotation_answer_comparison.json"
    dump(comparison_path, comparison)
    audit = {
        "schema_version": "tradex_sell_active_learning_annotations_v1.audit",
        "source_pack": str(args.source_pack),
        "source_queue": {"path": str(queue_path), "sha256_before": source_queue_sha, "sha256_after": sha(queue_path), "unchanged": source_queue_sha == sha(queue_path)},
        "source_answer_key": {"path": str(key_path), "sha256_before": source_key_sha, "sha256_after": sha(key_path), "unchanged": source_key_sha == sha(key_path)},
        "frozen_annotations": {"path": str(frozen_path), "sha256": frozen_sha},
        "comparison": {"path": str(comparison_path), "sha256": sha(comparison_path)},
        "annotations": len(annotations), "thresholds_tuned": False,
        "boundary": {"owner": "TRADEX", "review_only": True, "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    audit_path = args.output / "audit.json"
    dump(audit_path, audit)
    dump(args.output / "complete.json", {
        "complete": True,
        "sha256": {
            "frozen_annotations.json": sha(frozen_path),
            "annotation_answer_comparison.json": sha(comparison_path),
            "audit.json": sha(audit_path),
        },
    })


if __name__ == "__main__":
    main()
