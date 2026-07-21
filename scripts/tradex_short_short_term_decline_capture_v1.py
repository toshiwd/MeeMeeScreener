"""Compare short selectors by near-term decline capture, ignoring prior rebounds."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _load_daily


HORIZON = 5
PRIMARY_TARGET_PCT = 3.0
TARGETS_PCT = (1.0, 2.0, 3.0, 5.0)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def enrich(events: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    histories = {
        str(code): group.sort_values("ymd").reset_index(drop=True)
        for code, group in daily.groupby("code", sort=False)
    }
    rows = []
    for event in events.itertuples(index=False):
        code = str(event.code)
        history = histories.get(code)
        if history is None:
            continue
        matches = history.index[history.ymd.eq(int(event.signal_ymd))]
        if len(matches) == 0:
            continue
        signal_idx = int(matches[-1])
        future = history.iloc[signal_idx + 1:signal_idx + 1 + HORIZON]
        if len(future) < HORIZON:
            continue
        entry = float(future.iloc[0].o)
        if entry <= 0:
            continue
        lows_pct = 100.0 * (future.l.astype(float) / entry - 1.0)
        highs_pct = 100.0 * (future.h.astype(float) / entry - 1.0)
        closes_pct = 100.0 * (future.c.astype(float) / entry - 1.0)
        target_days = {}
        for target in TARGETS_PCT:
            hits = [day for day, value in enumerate(lows_pct.tolist(), start=1) if value <= -target]
            target_days[str(int(target))] = hits[0] if hits else None
        primary_day = target_days[str(int(PRIMARY_TARGET_PCT))]
        if primary_day is None:
            adverse_before_hit = float(highs_pct.max())
        else:
            adverse_before_hit = float(highs_pct.iloc[:primary_day].max())
        row = event._asdict()
        row.update({
            "entry_open_recomputed": entry,
            "min_low_5d_pct": float(lows_pct.min()),
            "max_high_5d_pct": float(highs_pct.max()),
            "close_1d_pct": float(closes_pct.iloc[0]),
            "close_2d_pct": float(closes_pct.iloc[1]),
            "close_3d_pct": float(closes_pct.iloc[2]),
            "close_5d_pct": float(closes_pct.iloc[4]),
            "target_1pct_hit": target_days["1"] is not None,
            "target_2pct_hit": target_days["2"] is not None,
            "target_3pct_hit": primary_day is not None,
            "target_5pct_hit": target_days["5"] is not None,
            "target_3pct_day": primary_day,
            "max_adverse_before_3pct_hit_pct": adverse_before_hit,
            "rebounded_3pct_before_target": bool(
                primary_day is not None and adverse_before_hit >= PRIMARY_TARGET_PCT
            ),
            "clean_target_3pct_hit": bool(
                primary_day is not None and adverse_before_hit < PRIMARY_TARGET_PCT
            ),
            "fast_target_3pct_hit": bool(primary_day is not None and primary_day <= 3),
            "fast_clean_target_3pct_hit": bool(
                primary_day is not None
                and primary_day <= 3
                and adverse_before_hit < PRIMARY_TARGET_PCT
            ),
        })
        rows.append(row)
    return pd.DataFrame(rows)


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0}
    years = {}
    for year, rows in frame.assign(year=frame.signal_ymd // 10000).groupby("year"):
        years[str(int(year))] = {
            "n": int(len(rows)),
            "target_3pct_hit_rate": float(rows.target_3pct_hit.mean()),
            "median_min_low_5d_pct": float(rows.min_low_5d_pct.median()),
        }
    wins = frame.loc[frame.target_3pct_hit]
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.astype(str).nunique()),
        "target_1pct_hit_rate": float(frame.target_1pct_hit.mean()),
        "target_2pct_hit_rate": float(frame.target_2pct_hit.mean()),
        "target_3pct_hit_rate": float(frame.target_3pct_hit.mean()),
        "target_5pct_hit_rate": float(frame.target_5pct_hit.mean()),
        "median_min_low_5d_pct": float(frame.min_low_5d_pct.median()),
        "mean_min_low_5d_pct": float(frame.min_low_5d_pct.mean()),
        "median_close_5d_pct": float(frame.close_5d_pct.median()),
        "mean_close_5d_pct": float(frame.close_5d_pct.mean()),
        "median_max_high_5d_pct": float(frame.max_high_5d_pct.median()),
        "target_3pct_median_day": (
            None if wins.empty else float(wins.target_3pct_day.median())
        ),
        "target_3pct_after_3pct_rebound_rate_all": float(frame.rebounded_3pct_before_target.mean()),
        "target_3pct_after_3pct_rebound_rate_winners": (
            None if wins.empty else float(wins.rebounded_3pct_before_target.mean())
        ),
        "winner_median_adverse_before_hit_pct": (
            None if wins.empty else float(wins.max_adverse_before_3pct_hit_pct.median())
        ),
        "clean_target_3pct_hit_rate": float(frame.clean_target_3pct_hit.mean()),
        "fast_target_3pct_hit_rate": float(frame.fast_target_3pct_hit.mean()),
        "fast_clean_target_3pct_hit_rate": float(frame.fast_clean_target_3pct_hit.mean()),
        "positive_years_by_hit_rate_gt_50": int(
            sum(row["target_3pct_hit_rate"] > 0.5 for row in years.values())
        ),
        "year_count": int(len(years)),
        "years": years,
    }


def anchor_rows(frame: pd.DataFrame, code: str, dates: set[int]) -> list[dict]:
    selected = frame.loc[
        frame.code.astype(str).eq(code) & frame.signal_ymd.astype(int).isin(dates)
    ]
    fields = [
        "code", "signal_ymd", "timing_state", "entry_open_recomputed",
        "min_low_5d_pct", "max_high_5d_pct", "target_3pct_hit",
        "target_3pct_day", "max_adverse_before_3pct_hit_pct",
        "rebounded_3pct_before_target",
        "clean_target_3pct_hit", "fast_target_3pct_hit",
        "fast_clean_target_3pct_hit",
    ]
    return selected[[field for field in fields if field in selected.columns]].to_dict("records")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--current-events", type=Path, required=True)
    ap.add_argument("--challenger-events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    daily = _load_daily(args.db, None)
    current = enrich(pd.read_parquet(args.current_events), daily)
    challenger = enrich(pd.read_parquet(args.challenger_events), daily)
    current.to_parquet(args.output / "current_selector_decline_capture.parquet", index=False)
    challenger.to_parquet(args.output / "challenger_selector_decline_capture.parquet", index=False)

    current_metrics = metrics(current)
    challenger_metrics = metrics(challenger)
    current_members = set(zip(current.code.astype(str), current.signal_ymd.astype(int)))
    challenger_members = set(zip(challenger.code.astype(str), challenger.signal_ymd.astype(int)))
    anchors = {
        "6996_current": anchor_rows(current, "6996", {20260706, 20260707, 20260713}),
        "6996_challenger": anchor_rows(challenger, "6996", {20260706, 20260707, 20260713}),
    }
    checks = {
        "challenger_n_ge_500": challenger_metrics["n"] >= 500,
        "challenger_3pct_hit_rate_gt_current": (
            challenger_metrics["target_3pct_hit_rate"] > current_metrics["target_3pct_hit_rate"]
        ),
        "challenger_median_low_better_than_current": (
            challenger_metrics["median_min_low_5d_pct"] < current_metrics["median_min_low_5d_pct"]
        ),
        "challenger_3pct_hit_rate_gt_50": challenger_metrics["target_3pct_hit_rate"] > 0.50,
        "rebound_winners_are_counted": (
            challenger_metrics["target_3pct_after_3pct_rebound_rate_all"] > 0
        ),
        "challenger_fast_clean_hit_rate_gt_40": (
            challenger_metrics["fast_clean_target_3pct_hit_rate"] > 0.40
        ),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_short_term_decline_capture_v1.compare.v1",
        "artifact_role": "authoritative_short_term_decline_capture_comparison",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "period": "2019-2026",
            "entry": "next session open",
            "horizon_sessions": HORIZON,
            "primary_win_label": "intraday low reaches -3% from entry within 5 sessions",
            "path_policy": "a prior rebound does not invalidate a later decline hit",
            "diagnostic_targets_pct": list(TARGETS_PCT),
            "costs": "ignored",
            "selector_axis_changed": False,
            "evaluation_axis_changed": "stop-first outcome replaced by short-term decline capture",
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "current_selector": current_metrics,
            "challenger_selector": challenger_metrics,
            "metric_lift": {
                "target_3pct_hit_rate": (
                    challenger_metrics["target_3pct_hit_rate"]
                    - current_metrics["target_3pct_hit_rate"]
                ),
                "median_min_low_5d_pct": (
                    challenger_metrics["median_min_low_5d_pct"]
                    - current_metrics["median_min_low_5d_pct"]
                ),
                "fast_clean_target_3pct_hit_rate": (
                    challenger_metrics["fast_clean_target_3pct_hit_rate"]
                    - current_metrics["fast_clean_target_3pct_hit_rate"]
                ),
            },
            "anchors": anchors,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(current_members.symmetric_difference(challenger_members))),
            "selection_divergence_reason": (
                "selectors unchanged; outcome now measures whether short-term decline occurs "
                "even after a temporary rebound"
            ),
            "current_members": int(len(current_members)),
            "challenger_members": int(len(challenger_members)),
            "overlap_members": int(len(current_members & challenger_members)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": (
                "keep_decline_capture_definition" if keep else "hold_decline_capture_definition"
            ),
            "authoritative_rollup_decision": (
                "keep_short_term_decline_capture_v1_review_only"
                if keep else "hold_continue_timing_research"
            ),
            "reason_type": (
                "challenger_improves_decline_capture_and_counts_rebound_then_drop"
                if keep else "one_or_more_decline_capture_gates_failed"
            ),
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production logic", "selector conditions"],
        "remaining_risks": [
            "decline capture is not realized PnL",
            "discretionary chart exits are not simulated",
            "large adverse moves before decline remain operationally difficult",
            "event history remains insufficient",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "db": {"path": str(args.db.resolve()), "read_only": True},
            "current_events": {"path": str(args.current_events.resolve()), "sha256": sha(args.current_events)},
            "challenger_events": {
                "path": str(args.challenger_events.resolve()),
                "sha256": sha(args.challenger_events),
            },
        },
        "compare_sha256": sha(compare),
        "current_ledger_sha256": sha(args.output / "current_selector_decline_capture.parquet"),
        "challenger_ledger_sha256": sha(args.output / "challenger_selector_decline_capture.parquet"),
    }
    (args.output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps(
            {"complete": True, "authoritative": "compare.json", "sha256": sha(compare)},
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
