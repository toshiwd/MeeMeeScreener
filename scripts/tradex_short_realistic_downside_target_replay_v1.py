from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_downside_target_overlay_replay_v1 import (
    DIST_PRIOR_80_HIGH_MIN,
    ENTRY_READY_LAST_VOL_RATIO_MAX,
    ENTRY_READY_RANGE_40_20_MIN,
    MAX_HOLD_DAYS,
    STOP_LOSS,
    _build_events,
    _json_ready,
    _summarize,
    _write_json,
    _write_jsonl,
)
from scripts.tradex_short_downside_target_overlay_v1 import (
    _add_context_features,
    _last_swing_low,
    _momentum_score,
    _risk_reward,
    _target_candidates,
)
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features, _load_daily
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _feature_payload, _is_gated_event
from scripts.tradex_short_downside_target_overlay_replay_v1 import _entry_ready, _replay_overlay
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "short_realistic_downside_target_replay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_realistic_downside_target_replay_v1")
MIN_REALISTIC_DOWNSIDE = 0.04
MAX_REALISTIC_DOWNSIDE = 0.15


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _choose_realistic_target(levels: list[dict[str, Any]], momentum_score: float) -> tuple[dict[str, Any] | None, str]:
    realistic = [
        item
        for item in levels
        if MIN_REALISTIC_DOWNSIDE
        <= float(item.get("downside_pct_from_signal_close") or 0.0)
        <= MAX_REALISTIC_DOWNSIDE
    ]
    if not realistic:
        return None, "no_reference_level_in_realistic_4_15pct_band"
    realistic.sort(key=lambda item: float(item["downside_pct_from_signal_close"]))
    if momentum_score >= 3.0:
        return realistic[-1], "strong_momentum_deepest_realistic_level"
    if momentum_score >= 1.5:
        return realistic[min(len(realistic) - 1, len(realistic) // 2)], "moderate_momentum_mid_realistic_level"
    return realistic[0], "weak_momentum_nearest_realistic_level"


def _overlay_at_signal_realistic(g: pd.DataFrame, idx: int) -> dict[str, Any]:
    row = g.iloc[idx]
    signal_close = float(row["c"])
    swing_low = _last_swing_low(g, idx)
    levels = _target_candidates(row, swing_low)
    momentum_score, momentum_reasons = _momentum_score(row)
    target, reason = _choose_realistic_target(levels, momentum_score)
    stop_price = signal_close * (1.0 + STOP_LOSS)
    rr = _risk_reward(signal_close, target, stop_price)
    expected_downside = None if target is None else signal_close / float(target["price"]) - 1.0
    if target is None:
        actionability = "AvoidNoTarget"
        quality = "NoTarget"
    elif rr is not None and rr >= 1.2 and expected_downside is not None and expected_downside >= 0.08:
        actionability = "DownsideReviewCandidate"
        quality = "RealisticDeepTarget"
    elif rr is not None and rr >= 0.75 and expected_downside is not None and expected_downside >= 0.04:
        actionability = "ScalpOnlyReview"
        quality = "RealisticShallowTarget"
    else:
        actionability = "AvoidPoorReward"
        quality = "PoorReward"
    return {
        "expected_target_price": None if target is None else float(target["price"]),
        "expected_downside_pct": expected_downside,
        "risk_reward_to_sl8": rr,
        "review_actionability": actionability,
        "target_quality": quality,
        "target_reason": reason,
        "momentum_score": momentum_score,
        "momentum_reasons": momentum_reasons,
        "target_level_id": None if target is None else target.get("level_id"),
        "target_level_type": None if target is None else target.get("level_type"),
    }


def _build_realistic_events(daily: pd.DataFrame) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        shaped = _add_shape_features(group)
        enriched = _add_context_features(shaped)
        for idx in range(200, len(enriched) - MAX_HOLD_DAYS - 1):
            current = enriched.iloc[idx]
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if not _is_gated_event(features, pattern):
                continue
            if not _entry_ready(features):
                continue
            overlay = _overlay_at_signal_realistic(enriched, idx)
            replay = _replay_overlay(enriched, idx, overlay["expected_target_price"])
            if not replay.get("valid"):
                continue
            events.append(
                {
                    "code": str(code),
                    "signal_ymd": int(current["ymd"]),
                    "month": int(current["ymd"]) // 100,
                    "pattern": pattern,
                    **features,
                    **overlay,
                    **replay,
                }
            )
    return events


def _decision(summary: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    down = next((row for row in summary if row["review_actionability"] == "DownsideReviewCandidate"), None)
    scalp = next((row for row in summary if row["review_actionability"] == "ScalpOnlyReview"), None)
    avoid = next((row for row in summary if row["review_actionability"] == "AvoidPoorReward"), None)
    candidate = down or scalp
    if not candidate:
        return {
            "authoritative_decision": "drop_realistic_target_no_candidate_bucket",
            "reason": "No actionable bucket survived the realistic target band.",
        }
    if candidate["n"] < 30:
        return {
            "authoritative_decision": "hold_insufficient_realistic_target_sample",
            "candidate_local_decision": candidate,
            "reason": "Best actionable bucket has too few samples.",
        }
    improves_return = candidate["mean_short_ret"] > baseline.get("mean_short_ret", -999)
    improves_target = candidate["target_hit_rate"] > baseline.get("target_hit_rate", -999)
    controls_stop = candidate["stop_hit_rate"] <= baseline.get("stop_hit_rate", 1)
    separates_avoid = avoid is None or candidate["mean_short_ret"] > avoid["mean_short_ret"]
    if improves_return and improves_target and controls_stop and separates_avoid:
        return {
            "authoritative_decision": "keep_realistic_downside_target_overlay",
            "candidate_local_decision": candidate,
            "baseline": baseline,
            "avoid_comparison": avoid,
            "reason": "Realistic target bucket improves return and target hit rate without worsening stop hit rate.",
        }
    return {
        "authoritative_decision": "hold_realistic_downside_target_overlay_needs_refinement",
        "candidate_local_decision": candidate,
        "baseline": baseline,
        "avoid_comparison": avoid,
        "reason": "Realistic target challenger did not satisfy all keep gates.",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Realistic Downside Target Replay v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- event_count: {payload['baseline'].get('n', 0)}",
        f"- realistic_target_band: {MIN_REALISTIC_DOWNSIDE:.0%} to {MAX_REALISTIC_DOWNSIDE:.0%}",
        "",
        "| actionability | n | mean_ret | win_rate | target_hit | stop_hit | denial_exit | avg_downside | avg_rr |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["actionability_leaderboard"]:
        lines.append(
            f"| {row['review_actionability']} | {row['n']} | {row['mean_short_ret']:.4f} | "
            f"{row['win_rate']:.3f} | {row['target_hit_rate']:.3f} | {row['stop_hit_rate']:.3f} | "
            f"{row['denial_exit_rate']:.3f} | {row['avg_expected_downside_pct']:.4f} | {row['avg_risk_reward_to_sl8']:.2f} |"
        )
    lines.extend(
        [
            "",
            f"- reason: {payload['research_decision']['reason']}",
            "- Review-only. No ranking, entry geometry, exit policy, runtime DB, MeeMee, or production behavior changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    events = _build_realistic_events(daily)
    summary, baseline = _summarize(events)
    decision = _decision(summary, baseline)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_status": runtime_status,
        "changed_axis": "target_selection_only_realistic_4_15pct_band",
        "fixed_evaluation_conditions": {
            "entry_ready_range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
            "entry_ready_last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
            "dist_prior_80_high_min": DIST_PRIOR_80_HIGH_MIN,
            "stop_loss": "sl8 from signal close",
            "max_hold_days": MAX_HOLD_DAYS,
            "exit_invalidation": "any bullish denial after entry",
        },
        "realistic_target_band": {
            "min_expected_downside": MIN_REALISTIC_DOWNSIDE,
            "max_expected_downside": MAX_REALISTIC_DOWNSIDE,
        },
        "baseline": baseline,
        "actionability_leaderboard": summary,
        "research_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "realistic_downside_target_replay.json", payload)
    _write_jsonl(run_dir / "realistic_downside_target_replay_events.jsonl", events)
    (run_dir / "realistic_downside_target_replay_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "realistic_downside_target_replay.json",
                "realistic_downside_target_replay_events.jsonl",
                "realistic_downside_target_replay_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--code-limit", type=int, default=None)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.code_limit))


if __name__ == "__main__":
    main()
