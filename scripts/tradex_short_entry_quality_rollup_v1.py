"""Authoritative same-condition comparison of the current and early-entry short selectors."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    ret = frame.return_fixed3_pct.astype(float)
    x = frame.assign(year=frame.signal_ymd // 10000)
    years = {str(year): {"n": int(len(rows)), "mean_return": float(rows.return_fixed3_pct.mean())}
             for year, rows in x.groupby("year")}
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "mean_return": float(ret.mean()),
        "median_return": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_rate": float(frame.exit_reason.isin(["target", "gap_target"]).mean()),
        "stop_rate": float(frame.exit_reason.isin(["stop_first", "gap_stop"]).mean()),
        "positive_years": int(sum(row["mean_return"] > 0 for row in years.values())),
        "year_count": int(len(years)),
        "years": years,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--current-events", type=Path, required=True)
    ap.add_argument("--challenger-events", type=Path, required=True)
    ap.add_argument("--episode-compare", type=Path, required=True)
    ap.add_argument("--ma20-compare", type=Path, required=True)
    ap.add_argument("--price-compare", type=Path, required=True)
    ap.add_argument("--event-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    current = pd.read_parquet(a.current_events)
    challenger = pd.read_parquet(a.challenger_events)
    current_metrics = metrics(current)
    challenger_metrics = metrics(challenger)
    current_members = set(zip(current.code.astype(str), current.signal_ymd.astype(int)))
    challenger_members = set(zip(challenger.code.astype(str), challenger.signal_ymd.astype(int)))
    episode = json.loads(a.episode_compare.read_text(encoding="utf-8"))
    ma20 = json.loads(a.ma20_compare.read_text(encoding="utf-8"))
    price = json.loads(a.price_compare.read_text(encoding="utf-8"))
    event = json.loads(a.event_compare.read_text(encoding="utf-8"))
    anchors_6996 = price["authoritative_result"]["anchor_6996"]
    anchor_9381 = ma20["authoritative_result"]["anchor_9381"]
    a6996 = {int(row["signal_ymd"]): row for row in anchors_6996}
    checks = {
        "challenger_n_ge_500": challenger_metrics["n"] >= 500,
        "challenger_mean_gt_0_30": challenger_metrics["mean_return"] > 0.30,
        "challenger_win_rate_gt_0_53": challenger_metrics["win_rate"] > 0.53,
        "challenger_stop_rate_lt_0_40": challenger_metrics["stop_rate"] < 0.40,
        "challenger_mean_gt_current": challenger_metrics["mean_return"] > current_metrics["mean_return"],
        "challenger_stop_rate_lt_current": challenger_metrics["stop_rate"] < current_metrics["stop_rate"],
        "positive_years_ge_5": challenger_metrics["positive_years"] >= 5,
        "all_2024plus_positive": all(
            row["mean_return"] > 0 for year, row in challenger_metrics["years"].items() if int(year) >= 2024
        ),
        "6996_jul06_review": a6996.get(20260706, {}).get("review_state") == "EarlyCrashReview",
        "6996_jul07_review": a6996.get(20260707, {}).get("review_state") == "EarlyCrashReview",
        "6996_jul13_late": a6996.get(20260713, {}).get("review_state") == "LateChase",
        "9381_trend_pullback_block": anchor_9381.get("decision") == "TrendPullbackBlock",
        "event_axis_not_promoted": event["judgment"]["authoritative_rollup_decision"] == "hold_event_veto_annotation_only",
    }
    keep = all(checks.values())
    current_late = episode["authoritative_result"]["states"]["LateChase"]["n"]
    result = {
        "schema_version": "tradex_short_entry_quality_rollup_v1.compare.v1",
        "artifact_role": "authoritative_short_entry_quality_rollup",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "period": "2019-2026",
            "execution": "next open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "current_selector": "pre-crash typical shape plus kept range-volume EntryReady split",
            "challenger_selector": "episode Early age0-3; close>=MA20; MA20 slope5<=0; 900<=close<5000",
            "event_policy": "annotation only because historical calendar coverage is insufficient",
            "costs": "ignored",
            "weekly_inputs": [],
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "current_selector": current_metrics,
            "challenger_selector": challenger_metrics,
            "metric_lift": {
                "mean_return": challenger_metrics["mean_return"] - current_metrics["mean_return"],
                "win_rate": challenger_metrics["win_rate"] - current_metrics["win_rate"],
                "stop_rate": challenger_metrics["stop_rate"] - current_metrics["stop_rate"],
            },
            "entry_quality": {
                "current_late_chase_count": int(current_late),
                "current_late_chase_rate": float(current_late / len(current)),
                "challenger_late_chase_count": 0,
                "challenger_late_chase_rate": 0.0,
                "trend_pullback_block_count": int(ma20["observed_branching"]["trend_pullback_block_count"]),
            },
            "anchors": {"6996": anchors_6996, "9381": anchor_9381},
            "event_axis": event["judgment"],
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(current_members.symmetric_difference(challenger_members))),
            "selection_divergence_reason": "replaces late distance-from-high detection with early episode, MA20-turn, and price-band entry timing",
            "current_members": len(current_members),
            "challenger_members": len(challenger_members),
            "overlap_members": len(current_members & challenger_members),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_early_entry_quality_challenger" if keep else "hold",
            "authoritative_rollup_decision": "keep_short_entry_quality_v1_review_only" if keep else "hold_continue_research",
            "reason_type": "all_entry_quality_and_anchor_gates_passed" if keep else "one_or_more_rollup_gates_failed",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production logic"],
        "remaining_risks": [
            "2022 and 2023 remain negative",
            "event axis is annotation-only",
            "current-day Yahoo-only bars remain provisional",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "current_events": {"path": str(a.current_events.resolve()), "sha256": sha(a.current_events)},
            "challenger_events": {"path": str(a.challenger_events.resolve()), "sha256": sha(a.challenger_events)},
            "episode_compare": {"path": str(a.episode_compare.resolve()), "sha256": sha(a.episode_compare)},
            "ma20_compare": {"path": str(a.ma20_compare.resolve()), "sha256": sha(a.ma20_compare)},
            "price_compare": {"path": str(a.price_compare.resolve()), "sha256": sha(a.price_compare)},
            "event_compare": {"path": str(a.event_compare.resolve()), "sha256": sha(a.event_compare)},
        },
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "current": current_metrics, "challenger": challenger_metrics, "checks": checks, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
