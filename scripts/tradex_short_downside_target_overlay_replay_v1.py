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
from scripts.tradex_short_downside_target_overlay_v1 import (
    _add_context_features,
    _choose_target,
    _last_swing_low,
    _momentum_score,
    _risk_reward,
    _safe_float,
    _target_candidates,
)
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "short_downside_target_overlay_replay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_downside_target_overlay_replay_v1")
ENTRY_READY_RANGE_40_20_MIN = 0.465006
ENTRY_READY_LAST_VOL_RATIO_MAX = 0.901902
DIST_PRIOR_80_HIGH_MIN = -0.484599
STOP_LOSS = 0.08
MAX_HOLD_DAYS = 20


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_ready(row), ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _entry_ready(features: dict[str, float | None]) -> bool:
    range40 = features.get("range_40_20")
    vol = features.get("last_vol_ratio")
    dist_high = features.get("dist_prior_80_high")
    return bool(
        range40 is not None
        and vol is not None
        and dist_high is not None
        and float(range40) >= ENTRY_READY_RANGE_40_20_MIN
        and float(vol) <= ENTRY_READY_LAST_VOL_RATIO_MAX
        and float(dist_high) >= DIST_PRIOR_80_HIGH_MIN
    )


def _overlay_at_signal(g: pd.DataFrame, idx: int) -> dict[str, Any]:
    row = g.iloc[idx]
    signal_close = float(row["c"])
    swing_low = _last_swing_low(g, idx)
    levels = _target_candidates(row, swing_low)
    momentum_score, momentum_reasons = _momentum_score(row)
    target, reason = _choose_target(levels, momentum_score)
    stop_price = signal_close * (1.0 + STOP_LOSS)
    rr = _risk_reward(signal_close, target, stop_price)
    expected_downside = None if target is None else signal_close / float(target["price"]) - 1.0
    if target is None:
        actionability = "AvoidNoTarget"
        quality = "NoTarget"
    elif rr is not None and rr >= 1.5 and expected_downside is not None and expected_downside >= 0.08:
        actionability = "DownsideReviewCandidate"
        quality = "DeepTarget"
    elif rr is not None and rr >= 0.8 and expected_downside is not None and expected_downside >= 0.04:
        actionability = "ScalpOnlyReview"
        quality = "ShallowTarget"
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


def _escape_flags(row: pd.Series, signal_high: float, ma5: float | None) -> dict[str, bool]:
    open_ = float(row["o"])
    high = float(row["h"])
    low = float(row["l"])
    close = float(row["c"])
    span = high - low
    if span <= 0 or open_ <= 0:
        return {"any_bullish_denial": False, "signal_high_reclaim": False, "ma5_reclaim": False, "large_bullish_denial": False}
    close_pos = (close - low) / span
    bullish = close > open_
    large = bullish and abs(close - open_) / open_ >= 0.025 and close_pos >= 0.65
    signal_high_reclaim = bullish and close_pos >= 0.65 and close > signal_high
    ma5_reclaim = bullish and close_pos >= 0.65 and ma5 is not None and close > ma5
    return {
        "any_bullish_denial": bool(signal_high_reclaim or ma5_reclaim or large),
        "signal_high_reclaim": bool(signal_high_reclaim),
        "ma5_reclaim": bool(ma5_reclaim),
        "large_bullish_denial": bool(large),
    }


def _replay_overlay(g: pd.DataFrame, idx: int, target_price: float | None) -> dict[str, Any]:
    entry_idx = idx + 1
    exit_limit = min(idx + MAX_HOLD_DAYS, len(g) - 1)
    if entry_idx > exit_limit:
        return {"valid": False}
    signal = g.iloc[idx]
    entry = g.iloc[entry_idx]
    entry_price = float(entry["o"])
    signal_close = float(signal["c"])
    signal_high = float(signal["h"])
    ma5 = _safe_float(signal.get("ma5"))
    if entry_price <= 0:
        return {"valid": False}
    stop_price = signal_close * (1.0 + STOP_LOSS)
    exit_price = float(g.iloc[exit_limit]["c"])
    exit_idx = exit_limit
    exit_reason = "max_hold_close"
    target_hit = False
    stop_hit = False
    denial_exit = False
    mfe = 0.0
    mae = 0.0
    for day_idx in range(entry_idx, exit_limit + 1):
        row = g.iloc[day_idx]
        high = float(row["h"])
        low = float(row["l"])
        mfe = max(mfe, entry_price / low - 1.0 if low > 0 else 0.0)
        mae = min(mae, entry_price / high - 1.0 if high > 0 else 0.0)
        if high >= stop_price:
            exit_price = stop_price
            exit_idx = day_idx
            exit_reason = "sl8_from_signal_close"
            stop_hit = True
            break
        if target_price is not None and low <= float(target_price):
            exit_price = float(target_price)
            exit_idx = day_idx
            exit_reason = "overlay_target_hit"
            target_hit = True
            break
        flags = _escape_flags(row, signal_high, ma5)
        if flags["any_bullish_denial"]:
            exit_price = float(row["c"])
            exit_idx = day_idx
            exit_reason = "any_bullish_denial"
            denial_exit = True
            break
    short_ret = entry_price / exit_price - 1.0 if exit_price > 0 else None
    return {
        "valid": short_ret is not None,
        "entry_ymd": int(entry["ymd"]),
        "entry_price": entry_price,
        "exit_ymd": int(g.iloc[exit_idx]["ymd"]),
        "exit_price": exit_price,
        "hold_days": int(exit_idx - entry_idx + 1),
        "exit_reason": exit_reason,
        "short_ret": short_ret,
        "target_hit": target_hit,
        "stop_hit": stop_hit,
        "denial_exit": denial_exit,
        "mfe_20": mfe,
        "mae_20": mae,
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
            overlay = _overlay_at_signal(enriched, idx)
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


def _summarize(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not events:
        return [], {}
    df = pd.DataFrame(events)
    rows: list[dict[str, Any]] = []
    for action, group in df.groupby("review_actionability", dropna=False):
        ret = group["short_ret"].astype(float)
        rows.append(
            {
                "review_actionability": str(action),
                "n": int(len(group)),
                "symbols": int(group["code"].nunique()),
                "months": int(group["month"].nunique()),
                "mean_short_ret": float(ret.mean()),
                "median_short_ret": float(ret.median()),
                "win_rate": float((ret > 0).mean()),
                "loss_rate": float((ret < 0).mean()),
                "target_hit_rate": float(group["target_hit"].astype(bool).mean()),
                "stop_hit_rate": float(group["stop_hit"].astype(bool).mean()),
                "denial_exit_rate": float(group["denial_exit"].astype(bool).mean()),
                "mean_mfe_20": float(group["mfe_20"].astype(float).mean()),
                "mean_mae_20": float(group["mae_20"].astype(float).mean()),
                "avg_expected_downside_pct": float(pd.to_numeric(group["expected_downside_pct"], errors="coerce").mean()),
                "avg_risk_reward_to_sl8": float(pd.to_numeric(group["risk_reward_to_sl8"], errors="coerce").mean()),
                "exit_reason_counts": group["exit_reason"].value_counts().to_dict(),
            }
        )
    rows.sort(key=lambda row: (row["mean_short_ret"], row["target_hit_rate"], -row["stop_hit_rate"]), reverse=True)
    action_counts = Counter(str(item["review_actionability"]) for item in events)
    baseline = {
        "n": len(events),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "mean_short_ret": float(df["short_ret"].astype(float).mean()),
        "median_short_ret": float(df["short_ret"].astype(float).median()),
        "win_rate": float((df["short_ret"].astype(float) > 0).mean()),
        "target_hit_rate": float(df["target_hit"].astype(bool).mean()),
        "stop_hit_rate": float(df["stop_hit"].astype(bool).mean()),
        "action_counts": dict(action_counts),
    }
    return rows, baseline


def _decision(summary: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    down = next((row for row in summary if row["review_actionability"] == "DownsideReviewCandidate"), None)
    avoid = next((row for row in summary if row["review_actionability"] == "AvoidPoorReward"), None)
    if not down:
        return {
            "authoritative_decision": "hold_insufficient_downside_review_sample",
            "reason": "no DownsideReviewCandidate sample in historical replay",
        }
    if down["n"] < 30:
        return {
            "authoritative_decision": "hold_insufficient_downside_review_sample",
            "candidate_local_decision": down,
            "reason": "DownsideReviewCandidate sample below minimum review threshold",
        }
    improves_baseline = down["mean_short_ret"] > baseline.get("mean_short_ret", -999)
    controls_stop = down["stop_hit_rate"] <= baseline.get("stop_hit_rate", 1)
    separates_avoid = avoid is None or down["mean_short_ret"] > avoid["mean_short_ret"]
    if improves_baseline and controls_stop and separates_avoid:
        return {
            "authoritative_decision": "keep_downside_target_overlay_for_review_board",
            "candidate_local_decision": down,
            "baseline": baseline,
            "avoid_comparison": avoid,
            "reason": "DownsideReviewCandidate improves mean return, does not worsen stop hit rate, and separates from AvoidPoorReward",
        }
    return {
        "authoritative_decision": "hold_downside_target_overlay_needs_refinement",
        "candidate_local_decision": down,
        "baseline": baseline,
        "avoid_comparison": avoid,
        "reason": "Overlay did not satisfy all keep gates under fixed historical replay",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Downside Target Overlay Replay v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- db_path: `{payload['db_path']}`",
        f"- event_count: {payload['baseline'].get('n', 0)}",
        "",
        "## By Actionability",
        "",
        "| actionability | n | mean_ret | win_rate | target_hit | stop_hit | denial_exit | avg_expected_downside | avg_rr |",
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
            "## Interpretation",
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
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": "typical pre-crash pattern plus fixed EntryReady geometry and oversold guard",
            "entry_ready_range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
            "entry_ready_last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
            "dist_prior_80_high_min": DIST_PRIOR_80_HIGH_MIN,
            "entry_convention": "next session open after signal day",
            "stop_loss": "sl8 from signal close",
            "max_hold_days": MAX_HOLD_DAYS,
            "exit_invalidation": "any bullish denial after entry",
        },
        "baseline": baseline,
        "actionability_leaderboard": summary,
        "research_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "downside_target_overlay_replay.json", payload)
    _write_jsonl(run_dir / "downside_target_overlay_replay_events.jsonl", events)
    (run_dir / "downside_target_overlay_replay_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "downside_target_overlay_replay.json",
                "downside_target_overlay_replay_events.jsonl",
                "downside_target_overlay_replay_summary.md",
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
