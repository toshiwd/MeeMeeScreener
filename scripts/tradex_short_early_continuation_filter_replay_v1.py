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
    _escape_flags,
    _json_ready,
    _replay_overlay,
    _summarize,
    _write_json,
    _write_jsonl,
)
from scripts.tradex_short_downside_target_overlay_v1 import _add_context_features, _safe_float
from scripts.tradex_short_realistic_downside_target_replay_v1 import _overlay_at_signal_realistic
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "short_early_continuation_filter_replay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_early_continuation_filter_replay_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _early_continuation(g: pd.DataFrame, idx: int) -> dict[str, Any]:
    entry_idx = idx + 1
    end_idx = min(idx + 3, len(g) - 1)
    signal = g.iloc[idx]
    if entry_idx > end_idx:
        return {
            "early_window_valid": False,
            "early_no_bullish_denial_3d": False,
            "early_close_below_entry_3d": False,
            "early_mfe_ge_4pct_3d": False,
            "early_mfe_ge_6pct_3d": False,
            "early_bucket": "EarlyWindowMissing",
        }
    entry = g.iloc[entry_idx]
    entry_price = float(entry["o"])
    signal_high = float(signal["h"])
    ma5 = _safe_float(signal.get("ma5"))
    window = g.iloc[entry_idx : end_idx + 1]
    denial = False
    for _, row in window.iterrows():
        flags = _escape_flags(row, signal_high, ma5)
        if flags["any_bullish_denial"]:
            denial = True
            break
    min_low = float(window["l"].astype(float).min())
    last_close = float(window.iloc[-1]["c"])
    mfe = entry_price / min_low - 1.0 if min_low > 0 else None
    close_below_entry = last_close < entry_price
    mfe_ge_4 = mfe is not None and mfe >= 0.04
    mfe_ge_6 = mfe is not None and mfe >= 0.06
    no_denial = not denial
    if no_denial and mfe_ge_6:
        bucket = "EarlyImpulse6NoDenial"
    elif no_denial and mfe_ge_4:
        bucket = "EarlyImpulse4NoDenial"
    elif no_denial and close_below_entry:
        bucket = "EarlyDriftDownNoDenial"
    elif no_denial:
        bucket = "EarlyNoDenialNoProgress"
    else:
        bucket = "EarlyDenied"
    return {
        "early_window_valid": True,
        "early_no_bullish_denial_3d": no_denial,
        "early_close_below_entry_3d": close_below_entry,
        "early_mfe_3d": mfe,
        "early_mfe_ge_4pct_3d": mfe_ge_4,
        "early_mfe_ge_6pct_3d": mfe_ge_6,
        "early_bucket": bucket,
    }


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
            early = _early_continuation(enriched, idx)
            action = overlay["review_actionability"]
            if early["early_bucket"] in {"EarlyImpulse6NoDenial", "EarlyImpulse4NoDenial"}:
                action = "ContinuationPermit"
            elif early["early_bucket"] in {"EarlyDenied", "EarlyNoDenialNoProgress"}:
                action = "ContinuationBlock"
            elif early["early_bucket"] == "EarlyDriftDownNoDenial" and action in {"DownsideReviewCandidate", "ScalpOnlyReview"}:
                action = "ContinuationWatch"
            events.append(
                {
                    "code": str(code),
                    "signal_ymd": int(current["ymd"]),
                    "month": int(current["ymd"]) // 100,
                    "pattern": pattern,
                    **features,
                    **overlay,
                    **early,
                    **replay,
                    "review_actionability": action,
                    "base_target_actionability": overlay["review_actionability"],
                }
            )
    return events


def _decision(summary: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    permit = next((row for row in summary if row["review_actionability"] == "ContinuationPermit"), None)
    block = next((row for row in summary if row["review_actionability"] == "ContinuationBlock"), None)
    if not permit:
        return {
            "authoritative_decision": "drop_early_continuation_no_permit_bucket",
            "reason": "No continuation permit bucket produced.",
        }
    if permit["n"] < 30:
        return {
            "authoritative_decision": "hold_insufficient_early_continuation_sample",
            "candidate_local_decision": permit,
            "block_comparison": block,
            "reason": "ContinuationPermit sample below minimum review threshold.",
        }
    improves_return = permit["mean_short_ret"] > baseline.get("mean_short_ret", -999)
    improves_target = permit["target_hit_rate"] > baseline.get("target_hit_rate", -999)
    controls_stop = permit["stop_hit_rate"] <= baseline.get("stop_hit_rate", 1)
    separates_block = block is None or permit["mean_short_ret"] > block["mean_short_ret"]
    if improves_return and improves_target and controls_stop and separates_block:
        return {
            "authoritative_decision": "keep_early_continuation_filter_for_review_board",
            "candidate_local_decision": permit,
            "baseline": baseline,
            "block_comparison": block,
            "reason": "Early continuation improves return/target hit, controls stop hit, and separates blocked names.",
        }
    return {
        "authoritative_decision": "hold_early_continuation_filter_needs_refinement",
        "candidate_local_decision": permit,
        "baseline": baseline,
        "block_comparison": block,
        "reason": "Early continuation did not satisfy all keep gates.",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Early Continuation Filter Replay v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- event_count: {payload['baseline'].get('n', 0)}",
        "",
        "| actionability | n | mean_ret | win_rate | target_hit | stop_hit | denial_exit | avg_mfe20 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["actionability_leaderboard"]:
        lines.append(
            f"| {row['review_actionability']} | {row['n']} | {row['mean_short_ret']:.4f} | "
            f"{row['win_rate']:.3f} | {row['target_hit_rate']:.3f} | {row['stop_hit_rate']:.3f} | "
            f"{row['denial_exit_rate']:.3f} | {row['mean_mfe_20']:.4f} |"
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
    events = _build_events(daily)
    summary, baseline = _summarize(events)
    decision = _decision(summary, baseline)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_status": runtime_status,
        "changed_axis": "early_continuation_filter_only",
        "fixed_evaluation_conditions": {
            "entry_ready_range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
            "entry_ready_last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
            "dist_prior_80_high_min": DIST_PRIOR_80_HIGH_MIN,
            "target_model": "realistic_downside_target_4_15pct_band",
            "stop_loss": f"sl{int(STOP_LOSS * 100)} from signal close",
            "max_hold_days": MAX_HOLD_DAYS,
            "exit_invalidation": "any bullish denial after entry",
        },
        "early_filter_definition": {
            "permit": "first 3 sessions have no bullish denial and MFE >= 4% or 6%",
            "watch": "first 3 sessions no bullish denial and close below entry, only if target bucket was actionable",
            "block": "bullish denial or no early progress",
        },
        "baseline": baseline,
        "actionability_leaderboard": summary,
        "research_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "early_continuation_filter_replay.json", payload)
    _write_jsonl(run_dir / "early_continuation_filter_replay_events.jsonl", events)
    (run_dir / "early_continuation_filter_replay_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "early_continuation_filter_replay.json",
                "early_continuation_filter_replay_events.jsonl",
                "early_continuation_filter_replay_summary.md",
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
