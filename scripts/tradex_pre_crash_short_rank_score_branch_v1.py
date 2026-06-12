from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import (
    DIST_HIGH_MAX,
    RANGE_20_MIN,
    RANGE_40_MIN,
    _feature_payload,
    _is_gated_event,
    _json_ready,
    _load_daily,
    _replay_trade,
    _write_json,
    _write_jsonl,
)
from scripts.tradex_pre_crash_short_portfolio_replay_v1 import _apply_topk_cooldown, _score
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_short_rank_score_branch_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_rank_score_branch_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
POLICIES = (
    {"policy_id": "pt20_sl8", "profit_target": 0.20, "stop_loss": 0.08},
    {"policy_id": "pt20_sl10", "profit_target": 0.20, "stop_loss": 0.10},
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _f(features: dict[str, Any], key: str) -> float:
    value = features.get(key)
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except Exception:
        pass
    return float(value)


def _score_distance_only(features: dict[str, Any]) -> float:
    return abs(_f(features, "dist_prior_80_high"))


def _score_range_distance(features: dict[str, Any]) -> float:
    return _f(features, "range_20_0") * 0.45 + _f(features, "range_40_20") * 0.25 + abs(_f(features, "dist_prior_80_high")) * 0.30


def _score_weak_close(features: dict[str, Any]) -> float:
    return (
        _f(features, "range_20_0") * 0.25
        + _f(features, "range_40_20") * 0.15
        + abs(_f(features, "dist_prior_80_high")) * 0.25
        + _f(features, "red_cluster_10") / 10.0 * 0.175
        + _f(features, "weak_close_cluster_10") / 10.0 * 0.175
    )


def _score_volume_break(features: dict[str, Any]) -> float:
    return (
        _f(features, "range_20_0") * 0.30
        + _f(features, "range_40_20") * 0.20
        + abs(_f(features, "dist_prior_80_high")) * 0.25
        + min(_f(features, "last_vol_ratio"), 4.0) / 4.0 * 0.15
        + _f(features, "weak_close_cluster_10") / 10.0 * 0.10
    )


def _score_recent_breakdown(features: dict[str, Any]) -> float:
    return (
        max(-_f(features, "ret_20_0"), 0.0) * 0.35
        + _f(features, "range_20_0") * 0.25
        + abs(_f(features, "dist_prior_80_high")) * 0.25
        + _f(features, "weak_close_cluster_10") / 10.0 * 0.15
    )


SCORE_FUNCS: dict[str, Callable[[dict[str, Any]], float]] = {
    "baseline_weighted": _score,
    "distance_only": _score_distance_only,
    "range_distance": _score_range_distance,
    "weak_close_cluster": _score_weak_close,
    "volume_break": _score_volume_break,
    "recent_breakdown": _score_recent_breakdown,
}


def _build_rows(daily: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = _add_shape_features(group)
        for idx in range(140, len(g) - 21):
            current = g.iloc[idx]
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if not _is_gated_event(features, pattern):
                continue
            score_values = {f"score_{name}": float(func(features)) for name, func in SCORE_FUNCS.items()}
            base = {
                "code": str(code),
                "signal_ymd": int(current["ymd"]),
                "month": int(current["ymd"]) // 100,
                "pattern": pattern,
                **features,
                **score_values,
            }
            for policy in POLICIES:
                replay = _replay_trade(
                    g,
                    idx,
                    profit_target=float(policy["profit_target"]),
                    stop_loss=float(policy["stop_loss"]),
                    exit_mode="none",
                )
                if replay.get("valid"):
                    rows.append({**base, **policy, **replay})
    return rows


def _summarize(selected: pd.DataFrame) -> dict[str, Any]:
    ret = selected["short_ret"].astype(float)
    by_month = selected.assign(short_ret_for_month=ret).groupby("month")["short_ret_for_month"].mean()
    return {
        "n": int(len(selected)),
        "symbols": int(selected["code"].nunique()),
        "months": int(selected["month"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "loss_rate": float((ret < 0).mean()),
        "avg_win": float(ret[ret > 0].mean()) if (ret > 0).any() else 0.0,
        "avg_loss": float(ret[ret < 0].mean()) if (ret < 0).any() else 0.0,
        "payoff_ratio_abs": float(abs(ret[ret > 0].mean() / ret[ret < 0].mean())) if (ret > 0).any() and (ret < 0).any() else None,
        "target_hit_rate": float(selected["exit_reason"].astype(str).str.startswith("profit_target").mean()),
        "stop_hit_rate": float(selected["exit_reason"].astype(str).str.startswith("stop_loss").mean()),
        "max_hold_rate": float((selected["exit_reason"] == "max_hold_close").mean()),
        "severe_loss_rate_8pct": float((ret <= -0.08).mean()),
        "avg_hold_days": float(selected["hold_days"].mean()),
        "positive_month_rate": float((by_month > 0).mean()),
        "mean_monthly_avg_ret": float(by_month.mean()),
        "exit_reason_counts": selected["exit_reason"].value_counts().to_dict(),
    }


def _evaluate(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    df = pd.DataFrame(rows)
    if df.empty:
        return [], [], {}
    leaderboard: list[dict[str, Any]] = []
    selected_by_key: dict[tuple[str, str], pd.DataFrame] = {}
    baseline_members: dict[str, set[tuple[str, int]]] = {}
    for policy_id, group in df.groupby("policy_id"):
        baseline = group.rename(columns={"score_baseline_weighted": "rank_score"}).copy()
        baseline_selected = _apply_topk_cooldown(baseline, TOP_K, COOLDOWN_DAYS)
        baseline_members[str(policy_id)] = set(zip(baseline_selected["code"].astype(str), baseline_selected["signal_ymd"].astype(int)))
        baseline_summary = _summarize(baseline_selected)
        for score_id in SCORE_FUNCS:
            candidate = group.rename(columns={f"score_{score_id}": "rank_score"}).copy()
            selected = _apply_topk_cooldown(candidate, TOP_K, COOLDOWN_DAYS)
            if selected.empty:
                continue
            selected_by_key[(str(policy_id), score_id)] = selected
            summary = _summarize(selected)
            members = set(zip(selected["code"].astype(str), selected["signal_ymd"].astype(int)))
            base_members = baseline_members[str(policy_id)]
            row = {
                "policy_id": str(policy_id),
                "score_id": score_id,
                "top_k": TOP_K,
                "cooldown_days": COOLDOWN_DAYS,
                "changed_selected_members_count": int(len(members.symmetric_difference(base_members))),
                "mean_short_ret_lift_vs_policy_baseline": float(summary["mean_short_ret"] - baseline_summary["mean_short_ret"]),
                "positive_month_rate_lift_vs_policy_baseline": float(summary["positive_month_rate"] - baseline_summary["positive_month_rate"]),
                "stop_hit_rate_delta_vs_policy_baseline": float(summary["stop_hit_rate"] - baseline_summary["stop_hit_rate"]),
                **summary,
            }
            row["decision"] = (
                "keep"
                if row["changed_selected_members_count"] >= 100
                and row["n"] >= 1000
                and row["months"] >= 48
                and row["mean_short_ret_lift_vs_policy_baseline"] >= 0.002
                and row["positive_month_rate_lift_vs_policy_baseline"] >= 0
                else "drop_or_hold_diagnostic"
            )
            leaderboard.append(row)
    leaderboard.sort(
        key=lambda row: (
            row["decision"] == "keep",
            float(row["mean_short_ret"]),
            float(row["positive_month_rate"]),
            -float(row["stop_hit_rate"]),
        ),
        reverse=True,
    )
    examples: list[dict[str, Any]] = []
    if leaderboard:
        best = leaderboard[0]
        selected = selected_by_key.get((best["policy_id"], best["score_id"]))
        if selected is not None:
            examples = selected.sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    branch_meta = {
        "baseline_policy_members": {policy_id: len(members) for policy_id, members in baseline_members.items()},
    }
    return leaderboard, examples, branch_meta


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    rows = _build_rows(daily)
    leaderboard, examples, branch_meta = _evaluate(rows)
    keep = [row for row in leaderboard if row.get("decision") == "keep"]
    best = keep[0] if keep else (leaderboard[0] if leaderboard else None)
    decision = {
        "authoritative_decision": "keep_rank_score_branch" if keep else "drop_no_rank_score_branch_edge",
        "candidate_local_decision": best,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "single-axis top-k rank score branch; costs and borrow ignored by user request",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "portfolio": {"top_k": TOP_K, "cooldown_days": COOLDOWN_DAYS},
            "entry_convention": "next session open after signal day",
            "exit_policies": POLICIES,
            "changed_axis": "top-k rank score only",
            "score_ids": list(SCORE_FUNCS.keys()),
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
            "no_lookahead": "rank score features use signal-day and prior rolling features only",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no pattern classifier change",
            "no range/high-distance gate change",
            "no exit policy grid expansion",
        ],
    }
    _write_json(run_dir / "evaluation_contract.json", contract)
    _write_json(
        run_dir / "run_manifest.json",
        {
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "db_path": str(db_path),
            "output_dir": str(run_dir),
            "code_limit": code_limit,
            "runtime_status": runtime_status,
            "raw_rows": int(len(daily)),
            "trade_rows": int(len(rows)),
            "branch_meta": branch_meta,
        },
    )
    _write_json(run_dir / "rank_score_leaderboard.json", {"rows": leaderboard})
    _write_jsonl(run_dir / "best_rank_score_examples.jsonl", examples)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "evaluation_contract.json",
                "run_manifest.json",
                "rank_score_leaderboard.json",
                "best_rank_score_examples.jsonl",
                "research_decision.json",
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
