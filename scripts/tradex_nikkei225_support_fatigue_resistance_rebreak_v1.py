from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import HORIZONS, _labels


AXIS_ID = "tradex_nikkei225_support_fatigue_resistance_rebreak_v1"
COMPARE_STATES = (
    "SUPPORT_CANDIDATE",
    "FIRST_BREAK",
    "WAITING_RETEST",
    "RESISTANCE_REJECTED",
    "REBREAK_CONFIRMED",
)


def _touch_episodes(rows: list[dict[str, Any]], start: int, end: int, level: float) -> list[int]:
    episodes: list[int] = []
    armed = True
    last_touch = -10_000
    for index in range(start, end):
        row = rows[index]
        atr = float(row["atr14"] or 0.0)
        if atr <= 0:
            continue
        if episodes and float(row["h"]) >= level + atr:
            armed = True
        near = abs(float(row["l"]) - level) <= 0.35 * atr
        if near and armed and index - last_touch >= 3:
            episodes.append(index)
            last_touch = index
            armed = False
    return episodes


def _candidate(rows: list[dict[str, Any]], index: int, seed: float | None) -> tuple[float, list[int]] | None:
    if index < 60 or seed is None or not np.isfinite(seed):
        return None
    start = index - 60
    near_lows = []
    for j in range(start, index):
        atr = float(rows[j]["atr14"] or 0.0)
        if atr > 0 and abs(float(rows[j]["l"]) - seed) <= 0.35 * atr:
            near_lows.append(float(rows[j]["l"]))
    if len(near_lows) < 2:
        return None
    level = float(np.median(near_lows))
    episodes = _touch_episodes(rows, start, index, level)
    if len(episodes) < 2:
        return None
    return level, episodes


def _episode_diagnostics(
    rows: list[dict[str, Any]], episodes: list[int], level: float, end: int
) -> tuple[list[int], list[float], int | None, int | None, float | None, float | None, int]:
    dates = [int(rows[index]["ymd"]) for index in episodes]
    intervals = [episodes[i] - episodes[i - 1] for i in range(1, len(episodes))]
    recoveries: list[float] = []
    for number, touch_index in enumerate(episodes):
        stop = episodes[number + 1] if number + 1 < len(episodes) else end
        if stop <= touch_index + 1:
            recoveries.append(0.0)
            continue
        peak = max(float(rows[j]["h"]) for j in range(touch_index + 1, stop))
        atr = float(rows[touch_index]["atr14"] or 0.0)
        recoveries.append((peak - level) / atr if atr > 0 else 0.0)
    previous_interval = intervals[-2] if len(intervals) >= 2 else None
    latest_interval = intervals[-1] if intervals else None
    previous_recovery = recoveries[-2] if len(recoveries) >= 2 else None
    latest_recovery = recoveries[-1] if recoveries else None
    fatigue_score = int(
        previous_interval is not None and latest_interval is not None and latest_interval <= previous_interval
    ) + int(
        previous_recovery is not None and latest_recovery is not None and latest_recovery <= previous_recovery
    )
    return dates, recoveries, previous_interval, latest_interval, previous_recovery, latest_recovery, fatigue_score


def _state_rows(part: pd.DataFrame) -> list[dict[str, Any]]:
    part = part.sort_values("ymd").copy()
    part["_support_seed"] = part["l"].shift(1).rolling(60, min_periods=60).quantile(0.10)
    rows = part.to_dict("records")
    output: list[dict[str, Any]] = []
    state = "NONE"
    level: float | None = None
    touch_indices: list[int] = []
    frozen_atr: float | None = None
    first_break_index: int | None = None
    first_break_ymd: int | None = None
    first_break_low: float | None = None
    retest_index: int | None = None
    retest_ymd: int | None = None
    rebreak_ymd: int | None = None
    reclaim_count = 0
    retest_upper_wick: float | None = None
    retest_close_pos: float | None = None

    def clear() -> None:
        nonlocal level, touch_indices, frozen_atr, first_break_index, first_break_ymd
        nonlocal first_break_low, retest_index, retest_ymd, rebreak_ymd, reclaim_count
        nonlocal retest_upper_wick, retest_close_pos
        level = None
        touch_indices = []
        frozen_atr = None
        first_break_index = None
        first_break_ymd = None
        first_break_low = None
        retest_index = None
        retest_ymd = None
        rebreak_ymd = None
        reclaim_count = 0
        retest_upper_wick = None
        retest_close_pos = None

    for index, row in enumerate(rows):
        ymd = int(row["ymd"])
        atr = float(row["atr14"] or 0.0)
        close = float(row["c"])
        high = float(row["h"])
        low = float(row["l"])
        ma20 = float(row["ma20"])
        close_pos = float(row["close_pos"] or 0.0)
        reason = "state_hold"

        if state in {"REBREAK_CONFIRMED", "RECLAIMED", "EXPIRED"}:
            state = "NONE"
            clear()
            reason = "terminal_reset"

        if state in {"NONE", "SUPPORT_CANDIDATE"}:
            candidate = _candidate(rows, index, row.get("_support_seed"))
            if candidate is None:
                if state == "SUPPORT_CANDIDATE":
                    state = "EXPIRED"
                    reason = "rolling_support_cluster_lost"
            else:
                candidate_level, episodes = candidate
                previous = rows[index - 1] if index > 0 else None
                previous_atr = float(previous["atr14"] or 0.0) if previous else 0.0
                previous_close = float(previous["c"]) if previous else close
                break_now = bool(
                    atr > 0
                    and previous_atr > 0
                    and close < candidate_level - 0.35 * atr
                    and previous_close >= candidate_level - 0.35 * previous_atr
                    and close_pos <= 0.40
                )
                level = candidate_level
                touch_indices = episodes
                if break_now:
                    state = "FIRST_BREAK"
                    frozen_atr = atr
                    first_break_index = index
                    first_break_ymd = ymd
                    first_break_low = low
                    reason = "close_break_below_frozen_support"
                else:
                    state = "SUPPORT_CANDIDATE"
                    reason = "rolling_low_cluster_established"
        elif level is not None and first_break_index is not None:
            break_age = index - first_break_index
            if close > level + 0.35 * atr:
                reclaim_count += 1
            else:
                reclaim_count = 0
            if reclaim_count >= 1:
                state = "RECLAIMED"
                reason = "strong_close_reclaim_above_support"
            elif state in {"FIRST_BREAK", "WAITING_RETEST"}:
                if break_age > 7:
                    state = "EXPIRED"
                    reason = "retest_window_gt_7"
                elif break_age >= 1 and high >= level - 0.35 * atr and close < level:
                    state = "RESISTANCE_REJECTED"
                    retest_index = index
                    retest_ymd = ymd
                    retest_upper_wick = float(row["upper_wick_ratio"] or 0.0)
                    retest_close_pos = close_pos
                    reason = "broken_support_retested_but_close_below"
                elif break_age >= 1:
                    state = "WAITING_RETEST"
                    reason = "waiting_retest_within_7"
            elif state == "RESISTANCE_REJECTED" and retest_index is not None:
                retest_age = index - retest_index
                if retest_age > 5:
                    state = "EXPIRED"
                    reason = "rebreak_window_gt_5"
                else:
                    prior_low = min(float(rows[j]["l"]) for j in range(first_break_index, index))
                    if close < prior_low - 0.10 * atr and close < ma20:
                        state = "REBREAK_CONFIRMED"
                        rebreak_ymd = ymd
                        reason = "post_retest_new_low_and_below_ma20"
                    else:
                        reason = "resistance_rejection_hold"

        if level is not None and touch_indices:
            diagnostics = _episode_diagnostics(rows, touch_indices, level, first_break_index or index)
            touch_dates, recoveries, prev_interval, last_interval, prev_recovery, last_recovery, fatigue = diagnostics
        else:
            touch_dates, recoveries = [], []
            prev_interval = last_interval = prev_recovery = last_recovery = None
            fatigue = 0
        if first_break_index is not None and state in {"FIRST_BREAK", "WAITING_RETEST"}:
            state_age = index - first_break_index
        elif retest_index is not None and state in {"RESISTANCE_REJECTED", "REBREAK_CONFIRMED"}:
            state_age = index - retest_index
        else:
            state_age = 0 if state != "NONE" else None
        enriched = {key: value for key, value in row.items() if key != "_support_seed"}
        enriched.update({
            "support_rebreak_state": state,
            "support_rebreak_reason_code": reason,
            "support_state_asof_ymd": ymd,
            "support_state_age": state_age,
            "support_frozen_level": level if first_break_index is not None else None,
            "support_candidate_level": level,
            "support_frozen_atr": frozen_atr,
            "support_touch_dates": json.dumps(touch_dates),
            "support_touch_count": len(touch_dates),
            "support_previous_touch_interval": prev_interval,
            "support_latest_touch_interval": last_interval,
            "support_touch_recoveries_atr": json.dumps(recoveries),
            "support_previous_recovery_atr": prev_recovery,
            "support_latest_recovery_atr": last_recovery,
            "support_fatigue_score": fatigue,
            "support_first_break_ymd": first_break_ymd,
            "support_first_break_low": first_break_low,
            "support_retest_ymd": retest_ymd,
            "support_retest_upper_wick": retest_upper_wick,
            "support_retest_close_pos": retest_close_pos,
            "support_rebreak_ymd": rebreak_ymd,
        })
        output.append(enriched)
    return output


def _metrics(frame: pd.DataFrame, labels: np.ndarray) -> dict[str, Any]:
    if len(frame) == 0:
        return {"n": 0, "codes": 0, "months": 0}
    return {
        "n": int(len(frame)),
        "codes": int(frame["code"].nunique()),
        "months": int(frame["ymd"].astype(str).str[:6].nunique()),
        "downside_rate": float((labels == 0).mean()),
        "rebound_rate": float((labels == 1).mean()),
        "neutral_rate": float((labels == 2).mean()),
    }


def run(input_parquet: Path, output_root: Path) -> Path:
    frame = pd.read_parquet(input_parquet).sort_values(["code", "ymd"]).reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for _, part in frame.groupby("code", sort=False):
        rows.extend(_state_rows(part))
    ledger = pd.DataFrame(rows).sort_values(["code", "ymd"]).reset_index(drop=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    ledger_path = output / "support_fatigue_resistance_rebreak_state_ledger.parquet"
    ledger.to_parquet(ledger_path, index=False, compression="zstd")

    periods = {
        "development_2019_2024": (20190101, 20241231),
        "locked_validation_2025": (20250101, 20251231),
        "exploratory_2026": (20260101, 20260713),
    }
    comparisons: dict[str, Any] = {}
    for horizon in HORIZONS:
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}"]
        part = ledger.dropna(subset=required).copy()
        labels = _labels(part, horizon)
        result: dict[str, Any] = {}
        for period, (start, end) in periods.items():
            period_mask = part["ymd"].between(start, end).to_numpy()
            metrics = {"universe_baseline": _metrics(part.loc[period_mask], labels[period_mask])}
            for state in COMPARE_STATES:
                mask = period_mask & (part["support_rebreak_state"].to_numpy() == state)
                metrics[state] = _metrics(part.loc[mask], labels[mask])
            result[period] = metrics
        comparisons[str(horizon)] = result

    gates: dict[str, Any] = {}
    for horizon in HORIZONS:
        result = comparisons[str(horizon)]
        dev = result["development_2019_2024"]
        val = result["locked_validation_2025"]
        dev_base, dev_event = dev["universe_baseline"], dev["REBREAK_CONFIRMED"]
        val_base, val_event = val["universe_baseline"], val["REBREAK_CONFIRMED"]
        checks = {
            "development_n_ge_100": dev_event.get("n", 0) >= 100,
            "validation_n_ge_80": val_event.get("n", 0) >= 80,
            "development_codes_ge_50": dev_event.get("codes", 0) >= 50,
            "validation_codes_ge_40": val_event.get("codes", 0) >= 40,
            "development_months_ge_24": dev_event.get("months", 0) >= 24,
            "validation_months_ge_9": val_event.get("months", 0) >= 9,
            "development_downside_uplift_ge_5pp": dev_event.get("downside_rate", 0) >= dev_base.get("downside_rate", 1) + 0.05,
            "validation_downside_uplift_ge_5pp": val_event.get("downside_rate", 0) >= val_base.get("downside_rate", 1) + 0.05,
            "development_rebound_reduction_ge_3pp": dev_event.get("rebound_rate", 1) <= dev_base.get("rebound_rate", 0) - 0.03,
            "validation_rebound_reduction_ge_3pp": val_event.get("rebound_rate", 1) <= val_base.get("rebound_rate", 0) - 0.03,
        }
        gates[str(horizon)] = {"checks": checks, "all_pass": all(checks.values())}
    keep = any(item["all_pass"] for item in gates.values())
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "source_parquet": str(input_parquet),
        "source_sha256": hashlib.sha256(input_parquet.read_bytes()).hexdigest(),
        "state_ledger": str(ledger_path),
        "fixed_conditions": {
            "single_axis": "PIT support cluster fatigue then close break, failed reclaim and post-retest rebreak",
            "support_formation": "prior 60 bars only; low cluster within 0.35 ATR; at least two independent touches",
            "independent_touch": "at least 3 sessions apart and rearmed only after price moves at least 1 ATR above level",
            "first_break": "close below level minus 0.35 ATR, prior close not below threshold, close_pos <= 0.40",
            "retest": "within 7 sessions; high reaches level minus 0.35 ATR and close remains below level",
            "rebreak": "within 5 sessions after retest; close below prior sequence low minus 0.10 ATR and below MA20",
            "strong_reclaim": "close above level plus 0.35 ATR invalidates",
            "feature_time": "current close or earlier; no state backfill",
            "development": "2019-2024 fixed rule, no outcome fitting",
            "locked_validation": 2025,
            "exploratory_only": "2026 through 2026-07-13",
            "costs": "ignored by user rule",
        },
        "comparisons": comparisons,
        "gate_audit": gates,
        "observed_branching": {
            "state_counts": {str(key): int(value) for key, value in ledger["support_rebreak_state"].value_counts().items()},
            "rebreak_rows": int((ledger["support_rebreak_state"] == "REBREAK_CONFIRMED").sum()),
            "rebreak_codes": int(ledger.loc[ledger["support_rebreak_state"] == "REBREAK_CONFIRMED", "code"].nunique()),
        },
        "decision": {
            "candidate_local_decision": "hold_for_clean_shadow" if keep else "drop_state_axis",
            "authoritative_rollup_decision": "review_only",
            "reason_type": "at_least_one_horizon_all_locked_gates_pass" if keep else "all_horizons_fail_development_or_locked_validation_gate",
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "remaining_risks": [
            "current Nikkei225 registry creates survivorship bias",
            "support seed uses a fixed prior-60-bar 10th percentile before cluster refinement",
            "2026 remains exploratory because representative cases were discovered there",
        ],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "compare": str(compare), "state_ledger": str(ledger_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-parquet", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_support_fatigue_resistance_rebreak_v1"))
    args = parser.parse_args()
    print(run(args.input_parquet, args.output_root))


if __name__ == "__main__":
    main()
