from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features, _load_daily
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _feature_payload, _is_gated_event
from scripts.tradex_short_downside_target_overlay_replay_v1 import (
    DIST_PRIOR_80_HIGH_MIN,
    ENTRY_READY_LAST_VOL_RATIO_MAX,
    ENTRY_READY_RANGE_40_20_MIN,
    MAX_HOLD_DAYS,
    STOP_LOSS,
    _entry_ready,
    _json_ready,
    _replay_overlay,
    _write_json,
    _write_jsonl,
)
from scripts.tradex_short_downside_target_overlay_v1 import _add_context_features, _safe_float
from scripts.tradex_short_early_continuation_filter_replay_v1 import _early_continuation
from scripts.tradex_short_realistic_downside_target_replay_v1 import _overlay_at_signal_realistic
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "short_visual_micro_path_replay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_visual_micro_path_replay_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _ratio(value: float | None, ref: float | None) -> float | None:
    if value is None or ref is None or ref <= 0:
        return None
    return value / ref - 1.0


def _micro_path_label(g: pd.DataFrame, idx: int) -> dict[str, Any]:
    row = g.iloc[idx]
    close = float(row["c"])
    high = float(row["h"])
    low = float(row["l"])
    ma7 = _safe_float(row.get("ma7"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    ma7_slope = _safe_float(row.get("ma7_slope5")) or 0.0
    ma20_slope = _safe_float(row.get("ma20_slope5")) or 0.0
    ma60_slope = _safe_float(row.get("ma60_slope5")) or 0.0
    close_pos = _safe_float(row.get("close_pos")) or 0.5
    body_ratio = _safe_float(row.get("body_ratio")) or 0.0
    upper_wick = _safe_float(row.get("upper_wick_ratio")) or 0.0
    lower_wick = _safe_float(row.get("lower_wick_ratio")) or 0.0
    ret3 = _safe_float(row.get("ret_3")) or 0.0
    ret5 = _safe_float(row.get("ret_5")) or 0.0
    prior_low20 = _safe_float(row.get("prior_low_20"))
    dist_ma7 = _ratio(close, ma7)
    dist_ma20 = _ratio(close, ma20)
    dist_ma60 = _ratio(close, ma60)

    recent = g.iloc[max(0, idx - 9) : idx + 1].copy()
    prev = g.iloc[max(0, idx - 19) : max(0, idx - 9)].copy()
    recent_high = float(recent["h"].astype(float).max()) if not recent.empty else high
    prev_high = float(prev["h"].astype(float).max()) if not prev.empty else recent_high
    recent_low = float(recent["l"].astype(float).min()) if not recent.empty else low
    prev_low = float(prev["l"].astype(float).min()) if not prev.empty else recent_low
    lower_high = recent_high < prev_high * 0.985
    lower_low_pressure = recent_low < prev_low * 0.995

    red_cluster5 = int((_safe_float(row.get("red_streak_5")) or 0.0))
    weak_close5 = int((_safe_float(row.get("weak_close_streak_5")) or 0.0))
    below_ma7 = ma7 is not None and close < ma7
    below_ma20 = ma20 is not None and close < ma20
    ma_overhead = (below_ma7 and ma7_slope <= 0.01) or (below_ma20 and ma20_slope <= 0.005)
    support_touch = prior_low20 is not None and low <= prior_low20 * 1.015
    support_not_broken = prior_low20 is not None and close >= prior_low20 * 1.02
    too_extended = ret5 <= -0.10 or (dist_ma7 is not None and dist_ma7 <= -0.08) or (dist_ma20 is not None and dist_ma20 <= -0.15)
    lower_wick_reclaim = lower_wick >= 0.42 and close_pos >= 0.50
    bullish_denial_shape = close > float(row["o"]) and body_ratio >= 0.35 and close_pos >= 0.65
    upper_rejection = upper_wick >= 0.35 and close_pos <= 0.45
    clean_pressure = weak_close5 >= 2 and red_cluster5 >= 2 and close_pos <= 0.45

    reasons: list[str] = []
    if too_extended and (lower_wick_reclaim or support_touch):
        label = "BounceRiskHigh"
        reasons.append("extended_down_near_support_or_lower_wick_reclaim")
    elif lower_wick_reclaim or bullish_denial_shape:
        label = "BounceRiskHigh"
        reasons.append("bullish_reclaim_or_lower_wick_support")
    elif too_extended:
        label = "TooExtendedDown"
        reasons.append("already_far_below_recent_ma_or_5d_drop")
    elif lower_high and ma_overhead and clean_pressure and not support_touch:
        label = "SellableRollover"
        reasons.append("lower_high_ma_overhead_weak_closes")
    elif lower_low_pressure and below_ma7 and clean_pressure and upper_rejection:
        label = "CleanContinuationDown"
        reasons.append("lower_low_below_ma7_upper_rejection_weak_closes")
    elif support_not_broken and ret3 < 0 and ma20_slope >= -0.01:
        label = "PullbackBeforeBreak"
        reasons.append("pullback_above_prior_low_before_break")
    else:
        label = "NoCleanPath"
        reasons.append("mixed_or_choppy_path_without_clean_pressure")

    return {
        "visual_micro_label": label,
        "visual_micro_reasons": reasons,
        "visual_lower_high_10v10": bool(lower_high),
        "visual_lower_low_pressure": bool(lower_low_pressure),
        "visual_ma_overhead": bool(ma_overhead),
        "visual_support_touch_20": bool(support_touch),
        "visual_too_extended": bool(too_extended),
        "visual_lower_wick_reclaim": bool(lower_wick_reclaim),
        "visual_upper_rejection": bool(upper_rejection),
        "visual_clean_pressure": bool(clean_pressure),
        "visual_close_pos": close_pos,
        "visual_body_ratio": body_ratio,
        "visual_upper_wick_ratio": upper_wick,
        "visual_lower_wick_ratio": lower_wick,
        "visual_ret3": ret3,
        "visual_ret5": ret5,
        "visual_dist_ma7": dist_ma7,
        "visual_dist_ma20": dist_ma20,
        "visual_dist_ma60": dist_ma60,
        "visual_ma7_slope5": ma7_slope,
        "visual_ma20_slope5": ma20_slope,
        "visual_ma60_slope5": ma60_slope,
    }


def _micro_action(label: str, early_bucket: str) -> str:
    if label in {"BounceRiskHigh", "TooExtendedDown"}:
        return "VisualBlock"
    if label in {"SellableRollover", "CleanContinuationDown"} and early_bucket in {
        "EarlyImpulse6NoDenial",
        "EarlyImpulse4NoDenial",
    }:
        return "VisualContinuationPermit"
    if label in {"SellableRollover", "CleanContinuationDown"}:
        return "VisualSetupWatch"
    if label == "PullbackBeforeBreak":
        return "VisualWaitBreak"
    return "VisualNoEdge"


def _build_events(daily: pd.DataFrame) -> list[dict[str, Any]]:
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
            micro = _micro_path_label(enriched, idx)
            early = _early_continuation(enriched, idx)
            events.append(
                {
                    "code": str(code),
                    "signal_ymd": int(current["ymd"]),
                    "month": int(current["ymd"]) // 100,
                    "pattern": pattern,
                    **features,
                    **overlay,
                    **micro,
                    **early,
                    "visual_micro_action": _micro_action(str(micro["visual_micro_label"]), str(early["early_bucket"])),
                    **replay,
                }
            )
    return events


def _summarize_by(events: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    if not events:
        return []
    df = pd.DataFrame(events)
    rows: list[dict[str, Any]] = []
    for key, group in df.groupby(field, dropna=False):
        ret = pd.to_numeric(group["short_ret"], errors="coerce")
        rows.append(
            {
                field: str(key),
                "n": int(len(group)),
                "symbols": int(group["code"].nunique()),
                "months": int(group["month"].nunique()),
                "mean_short_ret": float(ret.mean()),
                "median_short_ret": float(ret.median()),
                "win_rate": float((ret > 0).mean()),
                "target_hit_rate": float(group["target_hit"].astype(bool).mean()),
                "stop_hit_rate": float(group["stop_hit"].astype(bool).mean()),
                "denial_exit_rate": float(group["denial_exit"].astype(bool).mean()),
                "mean_mfe_20": float(pd.to_numeric(group["mfe_20"], errors="coerce").mean()),
                "mean_mae_20": float(pd.to_numeric(group["mae_20"], errors="coerce").mean()),
                "avg_expected_downside_pct": float(pd.to_numeric(group["expected_downside_pct"], errors="coerce").mean()),
                "avg_risk_reward_to_sl8": float(pd.to_numeric(group["risk_reward_to_sl8"], errors="coerce").mean()),
                "exit_reason_counts": group["exit_reason"].value_counts().to_dict(),
            }
        )
    rows.sort(key=lambda row: (row["mean_short_ret"], row["target_hit_rate"], -row["stop_hit_rate"]), reverse=True)
    return rows


def _baseline(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {}
    df = pd.DataFrame(events)
    ret = pd.to_numeric(df["short_ret"], errors="coerce")
    return {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_hit_rate": float(df["target_hit"].astype(bool).mean()),
        "stop_hit_rate": float(df["stop_hit"].astype(bool).mean()),
        "visual_micro_label_counts": dict(Counter(str(item["visual_micro_label"]) for item in events)),
        "visual_micro_action_counts": dict(Counter(str(item["visual_micro_action"]) for item in events)),
    }


def _decision(label_summary: list[dict[str, Any]], action_summary: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    permit = next((row for row in action_summary if row["visual_micro_action"] == "VisualContinuationPermit"), None)
    setup_watch = next((row for row in action_summary if row["visual_micro_action"] == "VisualSetupWatch"), None)
    block = next((row for row in action_summary if row["visual_micro_action"] == "VisualBlock"), None)
    clean_labels = [
        row for row in label_summary if row["visual_micro_label"] in {"SellableRollover", "CleanContinuationDown"}
    ]
    if not clean_labels:
        return {
            "authoritative_decision": "drop_visual_micro_path_no_clean_short_label",
            "reason": "No clean short-side visual micro label was produced.",
        }
    candidate = permit or setup_watch or max(clean_labels, key=lambda row: row["mean_short_ret"])
    if int(candidate["n"]) < 30:
        return {
            "authoritative_decision": "hold_visual_micro_path_insufficient_sample",
            "candidate_local_decision": candidate,
            "block_comparison": block,
            "reason": "Best visual micro short bucket is below the minimum review sample size.",
        }
    improves_return = candidate["mean_short_ret"] > baseline.get("mean_short_ret", -999)
    improves_target = candidate["target_hit_rate"] >= baseline.get("target_hit_rate", -999)
    controls_stop = candidate["stop_hit_rate"] <= baseline.get("stop_hit_rate", 1)
    separates_block = block is None or candidate["mean_short_ret"] > block["mean_short_ret"]
    if improves_return and improves_target and controls_stop and separates_block:
        return {
            "authoritative_decision": "keep_visual_micro_path_overlay_for_review_board",
            "candidate_local_decision": candidate,
            "baseline": baseline,
            "block_comparison": block,
            "reason": "Visual micro path bucket improves return/target hit, controls stop hit, and separates blocked paths.",
        }
    return {
        "authoritative_decision": "hold_visual_micro_path_overlay_needs_refinement",
        "candidate_local_decision": candidate,
        "baseline": baseline,
        "block_comparison": block,
        "reason": "Visual micro path labels separated chart texture but did not satisfy all keep gates.",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Visual Micro Path Replay v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- event_count: {payload['baseline'].get('n', 0)}",
        "",
        "## By Visual Micro Label",
        "",
        "| label | n | mean_ret | win_rate | target_hit | stop_hit | denial_exit | avg_mfe20 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["visual_micro_label_leaderboard"]:
        lines.append(
            f"| {row['visual_micro_label']} | {row['n']} | {row['mean_short_ret']:.4f} | "
            f"{row['win_rate']:.3f} | {row['target_hit_rate']:.3f} | {row['stop_hit_rate']:.3f} | "
            f"{row['denial_exit_rate']:.3f} | {row['mean_mfe_20']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## By Visual Micro Action",
            "",
            "| action | n | mean_ret | win_rate | target_hit | stop_hit | denial_exit | avg_mfe20 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in payload["visual_micro_action_leaderboard"]:
        lines.append(
            f"| {row['visual_micro_action']} | {row['n']} | {row['mean_short_ret']:.4f} | "
            f"{row['win_rate']:.3f} | {row['target_hit_rate']:.3f} | {row['stop_hit_rate']:.3f} | "
            f"{row['denial_exit_rate']:.3f} | {row['mean_mfe_20']:.4f} |"
        )
    lines.extend(
        [
            "",
            f"- reason: {payload['research_decision']['reason']}",
            "- Review-only. No ranking, EntryReady geometry, exit policy, runtime DB, MeeMee, or production behavior changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    events = _build_events(daily)
    baseline = _baseline(events)
    label_summary = _summarize_by(events, "visual_micro_label")
    action_summary = _summarize_by(events, "visual_micro_action")
    decision = _decision(label_summary, action_summary, baseline)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_status": runtime_status,
        "changed_axis": "visual_micro_path_label_only",
        "fixed_evaluation_conditions": {
            "entry_population": "typical pre-crash pattern plus fixed EntryReady geometry and oversold guard",
            "entry_ready_range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
            "entry_ready_last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
            "dist_prior_80_high_min": DIST_PRIOR_80_HIGH_MIN,
            "target_model": "realistic_downside_target_4_15pct_band",
            "stop_loss": f"sl{int(STOP_LOSS * 100)} from signal close",
            "max_hold_days": MAX_HOLD_DAYS,
            "exit_invalidation": "any bullish denial after entry",
            "entry_convention": "next session open after signal day",
        },
        "visual_micro_label_definition": {
            "SellableRollover": "lower highs, MA overhead, weak closes, and no immediate support touch",
            "CleanContinuationDown": "lower-low pressure below MA7 with upper rejection and weak closes",
            "PullbackBeforeBreak": "minor pullback still above prior low; wait for break",
            "TooExtendedDown": "already far below short MAs or severe 5-day drop",
            "BounceRiskHigh": "lower-wick/bullish reclaim or extended support touch",
            "NoCleanPath": "mixed/choppy path without clean short pressure",
        },
        "baseline": baseline,
        "visual_micro_label_leaderboard": label_summary,
        "visual_micro_action_leaderboard": action_summary,
        "research_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "short_visual_micro_path_replay.json", payload)
    _write_jsonl(run_dir / "short_visual_micro_path_replay_events.jsonl", events)
    (run_dir / "short_visual_micro_path_replay_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "short_visual_micro_path_replay.json",
                "short_visual_micro_path_replay_events.jsonl",
                "short_visual_micro_path_replay_summary.md",
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
