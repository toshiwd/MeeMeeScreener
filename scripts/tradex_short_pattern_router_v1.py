from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_pattern_router_v1"
LOW20_RULE_ID = "low20_break_relative_weakness"
HIGH_ZONE_RULE_ID = "high_zone_climax"
EXTREME_ROLL_RULE_ID = "extreme_high_roll"
EXTREME_ROLL_CONFIRM_WINDOW = 5


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _ymd(epoch: int) -> int:
    return int(datetime.fromtimestamp(int(epoch), timezone.utc).strftime("%Y%m%d"))


def _watchlist_codes(path: Path) -> list[str]:
    codes: list[str] = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        match = re.search(r"(?<!\d)(\d{4})(?!\d)", line)
        if match and match.group(1) not in codes:
            codes.append(match.group(1))
    if not codes:
        raise ValueError(f"watchlist has no four-digit codes: {path}")
    return codes


def load_current_features(db_path: Path, watchlist_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    codes = _watchlist_codes(watchlist_path)
    placeholders = ",".join("?" for _ in codes)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        confirmed_dt = int(conn.execute("select max(date) from daily_bars where source='pan'").fetchone()[0])
        provisional_dt = conn.execute("select max(date) from daily_bars where source='yahoo' and date>?", [confirmed_dt]).fetchone()[0]
        feature_dt = int(conn.execute("select max(dt) from feature_frame_daily where dt<=?", [confirmed_dt]).fetchone()[0])
        feature_rows = conn.execute(
            f"select code,close,ma7,ma20,ma60,low20_dist,breakout20_down,rel_ret20 "
            f"from feature_frame_daily where dt=? and code in ({placeholders})",
            [feature_dt, *codes],
        ).fetchall()
        high_zone_rows = conn.execute(
            f"""
            with bars as (
              select code,date,o,h,l,c,
                avg(c) over(partition by code order by date rows between 19 preceding and current row) ma20,
                lag(c,20) over(partition by code order by date) c20,
                max(h) over(partition by code order by date rows between 59 preceding and current row) high60,
                min(l) over(partition by code order by date rows between 59 preceding and current row) low60
              from daily_bars where source='pan' and code in ({placeholders})
            )
            select code,o,h,l,c,ma20,c20,high60,low60 from bars where date=?
            """,
            [*codes, confirmed_dt],
        ).fetchall()
        extreme_rows = conn.execute(
            f"""
            with bars as (
              select code,date,o,h,l,c,
                avg(c) over(partition by code order by date rows between 19 preceding and current row) ma20,
                lag(c,1) over(partition by code order by date) c1,
                lag(c,5) over(partition by code order by date) c5,
                lag(c,20) over(partition by code order by date) c20,
                max(h) over(partition by code order by date rows between 59 preceding and current row) high60,
                min(l) over(partition by code order by date rows between 59 preceding and current row) low60
              from daily_bars where source='pan' and code in ({placeholders})
            ), ranked as (
              select *,row_number() over(partition by code order by date desc) recent_rank
              from bars where date<=?
            )
            select code,date,o,h,l,c,ma20,c1,c5,c20,high60,low60
            from ranked where recent_rank<=? order by code,date
            """,
            [*codes, confirmed_dt, EXTREME_ROLL_CONFIRM_WINDOW + 1],
        ).fetchall()
        provisional_rows = [] if provisional_dt is None else conn.execute(
            f"select code,o,h,l,c from daily_bars where source='yahoo' and date=? and code in ({placeholders})",
            [int(provisional_dt), *codes],
        ).fetchall()

    feature_map = {str(row[0]): row for row in feature_rows}
    high_zone_map = {str(row[0]): row for row in high_zone_rows}
    extreme_map: dict[str, list[tuple[Any, ...]]] = {}
    for row in extreme_rows:
        extreme_map.setdefault(str(row[0]), []).append(row)
    provisional_map = {str(row[0]): row for row in provisional_rows}
    items: list[dict[str, Any]] = []
    for code in codes:
        feature = feature_map.get(code)
        high_zone = high_zone_map.get(code)
        if feature is None or high_zone is None:
            items.append({"code": code, "as_of": _ymd(confirmed_dt), "data_status": "confirmed_feature_missing"})
            continue
        _, close, ma7, ma20, ma60, low20_dist, breakout20_down, rel_ret20 = feature
        _, open_, high, low, confirmed_close, high_ma20, close20, high60, low60 = high_zone
        provisional = provisional_map.get(code)
        current_close = float(provisional[4]) if provisional and provisional[4] is not None else float(close)
        current_ma7 = (float(ma7) * 7.0 - float(close) + current_close) / 7.0 if provisional and ma7 is not None else float(ma7)
        bullish_denial = False
        if provisional and all(value is not None for value in provisional[1:]):
            _, p_open, p_high, p_low, p_close = provisional
            span = max(float(p_high) - float(p_low), 1e-9)
            bullish_denial = float(p_close) > float(p_open) and (float(p_close) - float(p_low)) / span >= 0.70 and abs(float(p_close) - float(p_open)) / span >= 0.50 and float(p_close) > current_ma7
        span = max(float(high) - float(low), 1e-9)
        range60 = max(float(high60) - float(low60), 1e-9)
        extreme_history = extreme_map.get(code, [])
        extreme_setup_age = None
        extreme_setup_high = None
        extreme_confirmed = False
        extreme_denied = False
        if extreme_history:
            current_extreme = extreme_history[-1]
            _, _, current_o, current_h, current_l, current_c, _, current_c1, current_c5, current_c20, _, _ = current_extreme
            if all(value not in (None, 0) for value in (current_c, current_c1, current_c5, current_c20)):
                current_ret1 = float(current_c) / float(current_c1) - 1.0
                current_ret5 = float(current_c) / float(current_c5) - 1.0
                current_ret20 = float(current_c) / float(current_c20) - 1.0
                extreme_confirmed = current_ret1 < -0.005 and current_ret5 <= current_ret20
            for age, setup in enumerate(reversed(extreme_history)):
                _, _, setup_o, setup_h, setup_l, setup_c, setup_ma20, setup_c1, _, setup_c20, setup_high60, setup_low60 = setup
                if any(value in (None, 0) for value in (setup_h, setup_l, setup_c, setup_ma20, setup_c1, setup_c20, setup_high60, setup_low60)):
                    continue
                setup_span = max(float(setup_h) - float(setup_l), 1e-9)
                setup_range60 = max(float(setup_high60) - float(setup_low60), 1e-9)
                setup_matches = (
                    (float(setup_c) - float(setup_low60)) / setup_range60 >= 0.85
                    and (float(setup_c) - float(setup_l)) / setup_span <= 0.60
                    and 0.20 <= float(setup_c) / float(setup_ma20) - 1.0 <= 0.50
                    and float(setup_c) / float(setup_c1) - 1.0 <= 0.005
                    and float(setup_c) / float(setup_c20) - 1.0 >= 0.32
                )
                if setup_matches:
                    extreme_setup_age = age
                    extreme_setup_high = float(setup_h)
                    break
            if extreme_setup_age is not None and extreme_setup_age > 0 and all(value is not None for value in (current_o, current_h, current_l, current_c)):
                current_span = max(float(current_h) - float(current_l), 1e-9)
                extreme_denied = float(current_c) > float(current_o) and (float(current_c) - float(current_l)) / current_span >= 0.70 and float(current_c) > float(extreme_setup_high)
        items.append({
            "code": code,
            "as_of": _ymd(int(provisional_dt)) if provisional else _ymd(confirmed_dt),
            "confirmed_as_of": _ymd(confirmed_dt),
            "bar_status": "provisional" if provisional else "confirmed",
            "data_status": "ready",
            "low20_dist": None if low20_dist is None else float(low20_dist),
            "breakout20_down": None if breakout20_down is None else float(breakout20_down),
            "rel_ret20": None if rel_ret20 is None else float(rel_ret20),
            "ret20": None if close20 in (None, 0) else float(confirmed_close) / float(close20) - 1.0,
            "dist_ma20": None if high_ma20 in (None, 0) else float(confirmed_close) / float(high_ma20) - 1.0,
            "close_range_pos": (float(confirmed_close) - float(low)) / span,
            "close_pos60": (float(confirmed_close) - float(low60)) / range60,
            "close": current_close,
            "ma7": current_ma7,
            "ma20": None if ma20 is None else float(ma20),
            "ma60": None if ma60 is None else float(ma60),
            "next_open_available": provisional is not None,
            "bullish_denial": bullish_denial,
            "extreme_roll_setup_age": extreme_setup_age,
            "extreme_roll_timing_confirmed": extreme_confirmed and extreme_setup_age in range(1, EXTREME_ROLL_CONFIRM_WINDOW + 1),
            "extreme_roll_bullish_denial": extreme_denied,
        })
    return items, {
        "db_path": str(db_path),
        "watchlist_path": str(watchlist_path),
        "watchlist_count": len(codes),
        "confirmed_as_of": _ymd(confirmed_dt),
        "provisional_as_of": None if provisional_dt is None else _ymd(int(provisional_dt)),
        "feature_as_of": _ymd(feature_dt),
    }


def _kept(compare: dict[str, Any]) -> bool:
    decision = compare.get("decision", compare.get("judgment", compare.get("authoritative_rollup_decision", {})))
    return decision.get("candidate_local_decision") == "keep" and decision.get("overall_readiness_pass", True) is True


def _extreme_timing_metrics(compare: dict[str, Any]) -> dict[str, Any]:
    chosen = compare.get("chosen_candidate")
    if isinstance(chosen, dict):
        return chosen.get("metrics", {})
    result = compare.get("authoritative_result", {})
    chosen_key = result.get("chosen_candidate")
    return result.get("candidates", {}).get(chosen_key, {}).get("metrics", {}) if chosen_key else {}


def _selected_transition_policy(compare: dict[str, Any], family: str) -> str | None:
    decision = compare.get("decision", {})
    return decision.get("selected_policies", {}).get(family)


def _low20_matched(row: dict[str, Any]) -> bool:
    values = (row.get("low20_dist"), row.get("breakout20_down"), row.get("rel_ret20"))
    return all(value is not None for value in values) and values[0] <= 0.02 and values[1] <= -0.03 and values[2] <= -0.05


def _high_zone_matched(row: dict[str, Any]) -> bool:
    values = (row.get("ret20"), row.get("dist_ma20"), row.get("close_range_pos"), row.get("close_pos60"))
    return all(value is not None for value in values) and values[0] >= 0.80 and values[1] >= 0.08 and values[2] >= 0.90 and values[3] >= 0.98


def classify(
    row: dict[str, Any], *, low20_enabled: bool, high_zone_enabled: bool, extreme_roll_enabled: bool, block_far_from_low: bool
) -> dict[str, Any]:
    base = {
        "code": str(row["code"]),
        "as_of": row.get("as_of"),
        "confirmed_as_of": row.get("confirmed_as_of"),
        "bar_status": row.get("bar_status"),
        "data_status": row.get("data_status", "ready"),
    }
    low20_match = low20_enabled and _low20_matched(row)
    high_zone_match = high_zone_enabled and _high_zone_matched(row)
    extreme_roll_match = extreme_roll_enabled and row.get("extreme_roll_setup_age") in (0, 1, 2)
    if not low20_match and not high_zone_match and not extreme_roll_match:
        return {
            **base,
            "rule_id": None,
            "action": "売り回避",
            "confidence": "medium",
            "reason": "採用済み複合条件に未一致" if base["data_status"] == "ready" else "確定特徴データ不足",
            "add_condition": "安値割れ複合条件または高値圏クライマックス条件が新たに成立するまで追加なし",
            "invalidation": "現在は売り仮説自体が未成立",
            "holding_horizon": None,
            "historical_reference": {
                LOW20_RULE_ID: row.get("historical_reference"),
                HIGH_ZONE_RULE_ID: row.get("high_zone_historical_reference"),
                EXTREME_ROLL_RULE_ID: row.get("extreme_roll_historical_reference"),
            },
        }
    if extreme_roll_match:
        if row.get("extreme_roll_bullish_denial", False):
            action, reason = "反転否定", "セットアップ高値を強い陽線終値で更新"
        elif row.get("extreme_roll_timing_confirmed", False):
            action, reason = "今日売れる", "極端高値巻き戻し後、2営業日以内の弱化確認"
        else:
            action, reason = "戻り待ち", "極端高値巻き戻しセットアップ後の弱化確認待ち"
        return {
            **base,
            "rule_id": EXTREME_ROLL_RULE_ID,
            "action": action,
            "confidence": "medium",
            "reason": reason,
            "add_condition": "確認後さらに陰線が続き、セットアップ高値を回復しない場合のみ追加検討",
            "invalidation": "セットアップ高値を強い陽線の終値で更新",
            "holding_horizon": "10%利確または5営業日",
            "historical_reference": row.get("extreme_roll_historical_reference"),
        }
    if high_zone_match:
        bullish_denial = bool(row.get("bullish_denial", False))
        if bullish_denial:
            action, reason = "反転否定", "強い陽線で高値圏失速仮説を否定"
        elif row.get("next_open_available", False):
            action, reason = "今日売れる", "高値圏クライマックス成立後の翌日寄り"
        else:
            action, reason = "戻り待ち", "翌日寄りまたは2本目の陰線を待つ"
        return {
            **base,
            "rule_id": HIGH_ZONE_RULE_ID,
            "action": action,
            "confidence": "medium",
            "reason": reason,
            "add_condition": "2営業日以内に2本目の陰線を確認した場合のみ追加検討",
            "invalidation": "セットアップ高値を強い陽線の終値で更新",
            "holding_horizon": "10%利確または5営業日",
            "historical_reference": row.get("high_zone_historical_reference"),
        }
    if block_far_from_low and float(row["low20_dist"]) > 0.075:
        return {**base, "rule_id": LOW20_RULE_ID, "action": "売り回避", "confidence": "medium", "reason": "20日安値から遠く被弾リスク"}

    close = row.get("close")
    ma7 = row.get("ma7")
    next_open_available = row.get("next_open_available", False)
    bullish_denial = bool(row.get("bullish_denial", False))
    if bullish_denial:
        action, reason = "反転否定", "大陽線または上方終値で下落仮説を否定"
    elif close is not None and ma7 is not None and float(close) >= float(ma7) * 0.99:
        action, reason = "今日売れる", "採用複合条件成立後の7日線戻り"
    else:
        action, reason = "戻り待ち", "安値追いを避け7日線への戻りを待つ"
    return {
        **base,
        "rule_id": LOW20_RULE_ID,
        "action": action,
        "confidence": "medium",
        "reason": reason,
        "add_condition": "7日線付近まで戻って再び上値を否定した場合のみ追加検討",
        "invalidation": "シグナル高値または7日線を強い陽線の終値で明確に回復",
        "holding_horizon": "5～20営業日。10日を中心に再判定",
        "historical_reference": row.get("historical_reference"),
    }


def build(
    features: list[dict[str, Any]],
    quality: dict[str, Any],
    timing: dict[str, Any],
    block: dict[str, Any],
    high_zone: dict[str, Any],
    high_zone_timing: dict[str, Any],
    transition_compare: dict[str, Any],
    extreme_roll_setup: dict[str, Any],
    extreme_roll_timing: dict[str, Any],
    source_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    low20_transition = _selected_transition_policy(transition_compare, LOW20_RULE_ID)
    high_zone_transition = _selected_transition_policy(transition_compare, HIGH_ZONE_RULE_ID)
    low20_enabled = _kept(quality) and _kept(timing) and low20_transition == "family_wait"
    high_zone_enabled = _kept(high_zone) and _kept(high_zone_timing) and high_zone_transition is not None
    extreme_roll_enabled = _kept(extreme_roll_setup) and _kept(extreme_roll_timing)
    block_decision = block.get("authoritative_rollup_decision", block.get("decision", {}))
    block_hold = block_decision.get("candidate_local_decision") == "hold"
    historical = quality.get("overall", {})
    high_zone_historical = high_zone.get("best", {}).get("full_metrics", {})
    extreme_roll_historical = _extreme_timing_metrics(extreme_roll_timing)
    rows = []
    for source in features:
        row = dict(source)
        row["historical_reference"] = historical
        row["high_zone_historical_reference"] = high_zone_historical
        row["extreme_roll_historical_reference"] = extreme_roll_historical
        rows.append(
            classify(
                row,
                low20_enabled=low20_enabled,
                high_zone_enabled=high_zone_enabled,
                extreme_roll_enabled=extreme_roll_enabled,
                block_far_from_low=block_hold,
            )
        )
    counts = Counter(row["action"] for row in rows)
    return {
        "schema_version": f"{AXIS_ID}.board.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_meta": source_meta or {},
        "fixed_evaluation_conditions": {
            "rules": {
                LOW20_RULE_ID: "low20_dist<=0.02 and breakout20_down<=-0.03 and rel_ret20<=-0.05",
                HIGH_ZONE_RULE_ID: "ret20>=0.80 and dist_ma20>=0.08 and close_range_pos>=0.90 and close_pos60>=0.98",
                EXTREME_ROLL_RULE_ID: "ret20>=0.32 and 0.20<=dist_ma20<=0.50 and close_pos60>=0.85 and close_range_pos<=0.60; confirm ret1<-0.005 and ret5<=ret20 within 5 sessions",
            },
            "entry_modes": {
                LOW20_RULE_ID: ["pullback_to_ma7"],
                HIGH_ZONE_RULE_ID: ["next_open", "second_down_within_2_sessions"],
                EXTREME_ROLL_RULE_ID: ["confirmation_close_within_5_sessions"],
            },
            "actions": ["今日売れる", "戻り待ち", "売り回避", "反転否定"],
            "costs": "ignored_by_user_request",
        },
        "source_decisions": {
            "low20_quality_keep": _kept(quality),
            "low20_timing_keep": _kept(timing),
            "high_zone_keep": _kept(high_zone),
            "high_zone_timing_keep": _kept(high_zone_timing),
            "low20_transition_policy": low20_transition,
            "high_zone_transition_policy": high_zone_transition,
            "extreme_roll_keep": _kept(extreme_roll_setup),
            "extreme_roll_timing_keep": _kept(extreme_roll_timing),
            "blockshort_hold": block_hold,
        },
        "summary": {"input_count": len(features), "action_counts": dict(counts)},
        "items": rows,
        "decision": {
            "candidate_local_decision": "keep" if low20_enabled and extreme_roll_enabled else ("hold" if low20_enabled or high_zone_enabled or extreme_roll_enabled else "drop"),
            "authoritative_rollup_decision": "review_only",
            "reason_type": "low20_and_extreme_roll_active_high_zone_dropped" if low20_enabled and extreme_roll_enabled and not high_zone_enabled else "source_keep_gate_incomplete",
        },
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }


def write_artifact_set(
    output: Path,
    board: dict[str, Any],
    quality: dict[str, Any],
    timing: dict[str, Any],
    high_zone: dict[str, Any],
    high_zone_timing: dict[str, Any],
    transition_compare: dict[str, Any],
    extreme_roll_setup: dict[str, Any],
    extreme_roll_timing: dict[str, Any],
    source_paths: dict[str, str] | None = None,
) -> None:
    families = [
        {
            "family_id": LOW20_RULE_ID,
            "decision": "keep" if _selected_transition_policy(transition_compare, LOW20_RULE_ID) == "family_wait" else "hold",
            "fixed_rule": board["fixed_evaluation_conditions"]["rules"][LOW20_RULE_ID],
            "entry_modes": board["fixed_evaluation_conditions"]["entry_modes"][LOW20_RULE_ID],
            "historical_metrics": quality.get("overall", {}),
            "entry_timing_decision": timing.get("decision", {}),
        },
        {
            "family_id": HIGH_ZONE_RULE_ID,
            "decision": "keep" if _selected_transition_policy(transition_compare, HIGH_ZONE_RULE_ID) else "drop",
            "fixed_rule": board["fixed_evaluation_conditions"]["rules"][HIGH_ZONE_RULE_ID],
            "entry_modes": board["fixed_evaluation_conditions"]["entry_modes"][HIGH_ZONE_RULE_ID],
            "historical_metrics": high_zone.get("best", {}).get("full_metrics", {}),
            "recent_metrics": high_zone.get("best", {}).get("recent_metrics", {}),
            "entry_timing_baseline": high_zone_timing.get("baseline", {}).get("full_metrics", {}),
            "entry_timing_challenger": high_zone_timing.get("best_challenger", {}).get("full_metrics", {}),
        },
        {
            "family_id": EXTREME_ROLL_RULE_ID,
            "decision": "keep" if _kept(extreme_roll_setup) and _kept(extreme_roll_timing) else "drop",
            "fixed_rule": board["fixed_evaluation_conditions"]["rules"][EXTREME_ROLL_RULE_ID],
            "entry_modes": board["fixed_evaluation_conditions"]["entry_modes"][EXTREME_ROLL_RULE_ID],
            "setup_metrics": extreme_roll_setup.get("chosen_candidate", {}).get("metrics", {}),
            "timing_metrics": _extreme_timing_metrics(extreme_roll_timing),
            "timing_decision": extreme_roll_timing.get("decision", extreme_roll_timing.get("judgment", {})),
        },
    ]
    rule_counts = Counter(item.get("rule_id") for item in board["items"] if item.get("rule_id"))
    compare = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": board["fixed_evaluation_conditions"],
        "source_meta": board["source_meta"],
        "authoritative_sources": source_paths or {},
        "authoritative_result": board["decision"],
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": sum(rule_counts.values()),
            "selection_divergence_reason": "independent kept family matches; router is not a ranking reranker",
            "family_match_counts": dict(rule_counts),
        },
        "current_board_summary": board["summary"],
        "verify": {
            "active_family_keep_count": sum(family["decision"] == "keep" for family in families),
            "active_family_keep_at_least_2": sum(family["decision"] == "keep" for family in families) >= 2,
            "watchlist_coverage_complete": board["summary"]["input_count"] == board["source_meta"].get("watchlist_count"),
            "confirmed_provisional_separated": board["source_meta"].get("confirmed_as_of") != board["source_meta"].get("provisional_as_of"),
        },
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    leaderboard = {
        "schema_version": f"{AXIS_ID}.family_leaderboard.v1",
        "artifact_role": "authoritative",
        "families": families,
    }
    _write(output, board)
    _write(output.parent / "compare.json", compare)
    _write(output.parent / "family_leaderboard.json", leaderboard)
    _write(
        output.parent / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "required_files": [output.name, "compare.json", "family_leaderboard.json", "_ARTIFACT_COMPLETE.json"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--features-json", type=Path)
    source.add_argument("--db-path", type=Path)
    parser.add_argument("--watchlist-path", type=Path, default=Path("tools/code.txt"))
    parser.add_argument("--quality-compare", type=Path, required=True)
    parser.add_argument("--timing-compare", type=Path, required=True)
    parser.add_argument("--blockshort-compare", type=Path, required=True)
    parser.add_argument("--high-zone-compare", type=Path, required=True)
    parser.add_argument("--high-zone-timing-compare", type=Path, required=True)
    parser.add_argument("--transition-compare", type=Path, required=True)
    parser.add_argument("--extreme-roll-setup-compare", type=Path, required=True)
    parser.add_argument("--extreme-roll-timing-compare", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.db_path is not None:
        features, source_meta = load_current_features(args.db_path, args.watchlist_path)
    else:
        features_payload = _read(args.features_json)
        features = features_payload.get("items", features_payload)
        source_meta = {"features_json": str(args.features_json)}
    quality = _read(args.quality_compare)
    timing = _read(args.timing_compare)
    block = _read(args.blockshort_compare)
    high_zone = _read(args.high_zone_compare)
    high_zone_timing = _read(args.high_zone_timing_compare)
    transition_compare = _read(args.transition_compare)
    extreme_roll_setup = _read(args.extreme_roll_setup_compare)
    extreme_roll_timing = _read(args.extreme_roll_timing_compare)
    result = build(
        features,
        quality,
        timing,
        block,
        high_zone,
        high_zone_timing,
        transition_compare,
        extreme_roll_setup,
        extreme_roll_timing,
        source_meta,
    )
    write_artifact_set(
        args.output,
        result,
        quality,
        timing,
        high_zone,
        high_zone_timing,
        transition_compare,
        extreme_roll_setup,
        extreme_roll_timing,
        {
            "low20_quality": str(args.quality_compare),
            "low20_timing": str(args.timing_compare),
            "blockshort": str(args.blockshort_compare),
            "high_zone_quality": str(args.high_zone_compare),
            "high_zone_timing": str(args.high_zone_timing_compare),
            "state_transition": str(args.transition_compare),
            "extreme_roll_setup": str(args.extreme_roll_setup_compare),
            "extreme_roll_timing": str(args.extreme_roll_timing_compare),
        },
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
