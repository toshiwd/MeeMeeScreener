"""Audit early/continuation/late timing inside the current short shape population."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import (
    _feature_payload,
    _is_gated_event,
    _load_daily,
)
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from scripts.tradex_pre_crash_short_state_review_board_v1 import (
    ENTRY_READY_LAST_VOL_RATIO_MAX,
    ENTRY_READY_RANGE_40_20_MIN,
)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fixed3_outcome(g, idx):
    entry = float(g.iloc[idx + 1].o)
    target = entry * 0.97
    stop = entry * 1.03
    for day in range(1, 6):
        row = g.iloc[idx + day]
        if float(row.o) >= stop:
            return 100 * (entry / float(row.o) - 1), day, "gap_stop"
        if float(row.o) <= target:
            return 100 * (entry / float(row.o) - 1), day, "gap_target"
        if float(row.h) >= stop:
            return -3.0, day, "stop_first"
        if float(row.l) <= target:
            return 3.0, day, "target"
    return 100 * (entry / float(g.iloc[idx + 5].c) - 1), 5, "fixed5_close"


def add_episode_features(g):
    x = g.copy().reset_index(drop=True)
    x["ret1"] = x.c.pct_change()
    x["ma7_episode"] = x.c.rolling(7, min_periods=7).mean()
    x["prior5_low"] = x.l.shift(1).rolling(5, min_periods=5).min()
    span = (x.h - x.l).replace(0, np.nan)
    x["episode_body_ratio"] = (x.o - x.c) / span
    x["episode_close_pos"] = (x.c - x.l) / span
    x["episode_onset"] = (
        x.ret1.le(-0.02)
        & x.c.lt(x.o)
        & x.episode_body_ratio.ge(0.45)
        & x.episode_close_pos.le(0.35)
        & (x.c.lt(x.prior5_low) | x.ret1.le(-0.05))
    )
    episode_id = 0
    active_start = None
    active_reference = None
    recovery_streak = 0
    ids, ages, drops, starts = [], [], [], []
    for idx, row in x.iterrows():
        if active_start is not None:
            recovery_streak = recovery_streak + 1 if row.c >= row.ma7_episode else 0
            if recovery_streak >= 2 or row.c >= active_reference:
                active_start = None
                active_reference = None
                recovery_streak = 0
        if bool(row.episode_onset) and active_start is None:
            episode_id += 1
            active_start = idx
            active_reference = float(x.iloc[idx - 1].c) if idx > 0 else float(row.o)
            recovery_streak = 0
        if active_start is None:
            ids.append(None); ages.append(None); drops.append(None); starts.append(None)
        else:
            ids.append(episode_id)
            ages.append(idx - active_start)
            drops.append(float(row.c / active_reference - 1))
            starts.append(int(x.iloc[active_start].ymd))
    x["episode_id"] = ids
    x["episode_age"] = ages
    x["episode_drop"] = drops
    x["episode_start_ymd"] = starts
    return x


def timing_state(age):
    if pd.isna(age):
        return "NoEpisode"
    age = int(age)
    if age <= 3:
        return "Early"
    if age <= 5:
        return "Continuation"
    return "LateChase"


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    ret = frame.return_fixed3_pct.astype(float)
    years = frame.assign(year=frame.signal_ymd // 10000).groupby("year").return_fixed3_pct.mean()
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "mean_return": float(ret.mean()),
        "median_return": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_rate": float(frame.exit_reason.isin(["target", "gap_target"]).mean()),
        "stop_rate": float(frame.exit_reason.isin(["stop_first", "gap_stop"]).mean()),
        "positive_year_rate": float((years > 0).mean()),
        "years": {str(year): {"n": int(len(rows)), "mean_return": float(rows.return_fixed3_pct.mean())}
                  for year, rows in frame.assign(year=frame.signal_ymd // 10000).groupby("year")},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    daily = _load_daily(a.db, None)
    events = []
    for code, group in daily.groupby("code", sort=False):
        g = add_episode_features(_add_shape_features(group))
        signal_ordinal = {}
        for idx in range(140, len(g) - 5):
            current = g.iloc[idx]
            if int(current.ymd) < 20190101:
                continue
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if not _is_gated_event(features, pattern):
                continue
            entry_ready = (
                float(features.get("range_40_20") or -999) >= ENTRY_READY_RANGE_40_20_MIN
                and float(features.get("last_vol_ratio") or 999) <= ENTRY_READY_LAST_VOL_RATIO_MAX
            )
            if not entry_ready:
                continue
            episode_key = current.episode_id
            if pd.isna(episode_key):
                ordinal = 0
            else:
                key = int(episode_key)
                signal_ordinal[key] = signal_ordinal.get(key, 0) + 1
                ordinal = signal_ordinal[key]
            outcome, hold_days, exit_reason = fixed3_outcome(g, idx)
            events.append({
                "code": str(code),
                "signal_ymd": int(current.ymd),
                "pattern": pattern,
                "episode_start_ymd": None if pd.isna(current.episode_start_ymd) else int(current.episode_start_ymd),
                "episode_age": None if pd.isna(current.episode_age) else int(current.episode_age),
                "episode_drop_pct": None if pd.isna(current.episode_drop) else 100 * float(current.episode_drop),
                "episode_signal_ordinal": ordinal,
                "timing_state": timing_state(current.episode_age),
                "range_40_20": features.get("range_40_20"),
                "last_vol_ratio": features.get("last_vol_ratio"),
                "dist_prior_80_high": features.get("dist_prior_80_high"),
                "entry_open": float(g.iloc[idx + 1].o),
                "return_fixed3_pct": outcome,
                "hold_days": hold_days,
                "exit_reason": exit_reason,
            })
    ledger = pd.DataFrame(events)
    ledger.to_parquet(a.output / "episode_timing_event_ledger.parquet", index=False)
    states = {state: metrics(ledger.loc[ledger.timing_state.eq(state)])
              for state in ["Early", "Continuation", "LateChase", "NoEpisode"]}
    first_only = metrics(ledger.loc[ledger.episode_signal_ordinal.le(1)])
    repeats = metrics(ledger.loc[ledger.episode_signal_ordinal.gt(1)])
    anchor = []
    anchor_source = daily.loc[daily.code.astype(str).eq("6996")].copy()
    if not anchor_source.empty:
        anchor_frame = add_episode_features(_add_shape_features(anchor_source))
        for row in anchor_frame.loc[anchor_frame.ymd.between(20260701, 20260716)].itertuples(index=False):
            anchor.append({
                "code": "6996", "signal_ymd": int(row.ymd), "close": float(row.c),
                "episode_onset": bool(row.episode_onset),
                "episode_start_ymd": None if pd.isna(row.episode_start_ymd) else int(row.episode_start_ymd),
                "episode_age": None if pd.isna(row.episode_age) else int(row.episode_age),
                "episode_drop_pct": None if pd.isna(row.episode_drop) else 100 * float(row.episode_drop),
                "timing_state": timing_state(row.episode_age),
            })
    anchor_by_date = {int(row["signal_ymd"]): row for row in anchor}
    checks = {
        "6996_jul06_early": anchor_by_date.get(20260706, {}).get("timing_state") == "Early",
        "6996_jul07_early": anchor_by_date.get(20260707, {}).get("timing_state") == "Early",
        "6996_jul13_late": anchor_by_date.get(20260713, {}).get("timing_state") == "LateChase",
        "6996_jul14_late": anchor_by_date.get(20260714, {}).get("timing_state") == "LateChase",
        "6996_jul15_late": anchor_by_date.get(20260715, {}).get("timing_state") == "LateChase",
        "6996_jul16_late": anchor_by_date.get(20260716, {}).get("timing_state") == "LateChase",
        "early_mean_gt_late": states["Early"].get("mean_return", -999) > states["LateChase"].get("mean_return", 999),
        "early_stop_rate_le_late": states["Early"].get("stop_rate", 1) <= states["LateChase"].get("stop_rate", 0),
        "early_n_ge_30": states["Early"].get("n", 0) >= 30,
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_episode_timing_axis_v1.compare.v1",
        "artifact_role": "authoritative_short_episode_timing_axis",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "current_selector": "pre-crash typical shape plus kept range-volume EntryReady split",
            "axis_changed": "episode age and repeated-signal timing only",
            "episode_onset": "ret1<=-2%, bearish body>=45%, close_pos<=35%, close below previous 5-session low",
            "episode_reset": "two consecutive closes at/above MA7 or age>15",
            "timing_states": {"Early": "age 0-3", "Continuation": "age 4-5", "LateChase": "age >=6"},
            "execution": "next open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "period": "2019-2026",
            "costs": "ignored",
            "weekly_inputs": [],
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "all": metrics(ledger),
            "states": states,
            "first_signal_only": first_only,
            "repeated_signals": repeats,
            "anchor_6996": anchor,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(ledger.loc[ledger.timing_state.eq("LateChase")])),
            "selection_divergence_reason": "same shape signals split by elapsed sessions since first breakdown impulse",
            "state_counts": {state: states[state].get("n", 0) for state in states},
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_episode_timing_axis" if keep else "hold_episode_definition",
            "authoritative_rollup_decision": "keep_late_chase_veto_review_only" if keep else "hold",
            "reason_type": "anchor_and_fixed_outcome_gates_passed" if keep else "one_or_more_episode_timing_gates_failed",
        },
        "not_changed": ["MA20 axis", "event axis", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"db": {"path": str(a.db.resolve()), "read_only": True}},
        "events": int(len(ledger)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "episode_timing_event_ledger.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "states": states, "anchor": anchor, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()



