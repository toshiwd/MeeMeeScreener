from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


AXIS_ID = "pre_crash_short_entry_denial_veto_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_entry_denial_veto_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
POLICIES = (
    {"policy_id": "pt20_sl8", "profit_target": 0.20, "stop_loss": 0.08},
    {"policy_id": "pt20_sl10", "profit_target": 0.20, "stop_loss": 0.10},
)
VETOES = (
    {"veto_id": "baseline_no_veto"},
    {"veto_id": "veto_signal_upper_close", "min_close_pos": 0.65},
    {"veto_id": "veto_signal_green_upper_close", "min_close_pos": 0.65, "require_green": True},
    {"veto_id": "veto_signal_large_green_upper_close", "min_close_pos": 0.65, "require_green": True, "min_body_pct": 0.025},
    {"veto_id": "veto_signal_close_reclaim_3pct", "min_signal_ret": 0.03, "require_green": True},
    {"veto_id": "veto_signal_volume_green_upper_close", "min_close_pos": 0.65, "require_green": True, "min_vol_ratio": 1.5},
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _signal_candle_features(row: pd.Series) -> dict[str, Any]:
    open_ = float(row["o"])
    high = float(row["h"])
    low = float(row["l"])
    close = float(row["c"])
    span = high - low
    body_pct = abs(close - open_) / open_ if open_ > 0 else None
    close_pos = (close - low) / span if span > 0 else None
    signal_ret = close / open_ - 1.0 if open_ > 0 else None
    return {
        "signal_green": bool(close > open_),
        "signal_close_pos": close_pos,
        "signal_body_pct": body_pct,
        "signal_ret": signal_ret,
    }


def _veto_hit(veto: dict[str, Any], row: dict[str, Any]) -> bool:
    if veto["veto_id"] == "baseline_no_veto":
        return False
    close_pos = row.get("signal_close_pos")
    body_pct = row.get("signal_body_pct")
    signal_ret = row.get("signal_ret")
    vol_ratio = row.get("last_vol_ratio")
    if veto.get("require_green") and not row.get("signal_green"):
        return False
    if "min_close_pos" in veto and (close_pos is None or close_pos < float(veto["min_close_pos"])):
        return False
    if "min_body_pct" in veto and (body_pct is None or body_pct < float(veto["min_body_pct"])):
        return False
    if "min_signal_ret" in veto and (signal_ret is None or signal_ret < float(veto["min_signal_ret"])):
        return False
    if "min_vol_ratio" in veto and (vol_ratio is None or vol_ratio < float(veto["min_vol_ratio"])):
        return False
    return True


def _build_base_trades(daily: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = _add_shape_features(group)
        for idx in range(140, len(g) - 21):
            current = g.iloc[idx]
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if not _is_gated_event(features, pattern):
                continue
            base = {
                "code": str(code),
                "signal_ymd": int(current["ymd"]),
                "month": int(current["ymd"]) // 100,
                "pattern": pattern,
                "rank_score": _score(features),
                **features,
                **_signal_candle_features(current),
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


def _evaluate(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    df = pd.DataFrame(rows)
    if df.empty:
        return [], []
    leaderboard: list[dict[str, Any]] = []
    for policy_id, policy_group in df.groupby("policy_id"):
        baseline_selected = _apply_topk_cooldown(policy_group, TOP_K, COOLDOWN_DAYS)
        if baseline_selected.empty:
            continue
        baseline_summary = _summarize(baseline_selected)
        for veto in VETOES:
            candidate = policy_group.copy()
            if veto["veto_id"] != "baseline_no_veto":
                mask = candidate.apply(lambda row: not _veto_hit(veto, row.to_dict()), axis=1)
                candidate = candidate[mask]
            selected = _apply_topk_cooldown(candidate, TOP_K, COOLDOWN_DAYS)
            if selected.empty:
                continue
            summary = _summarize(selected)
            row = {
                **veto,
                "policy_id": str(policy_id),
                "profit_target": float(selected["profit_target"].iloc[0]),
                "stop_loss": float(selected["stop_loss"].iloc[0]),
                "top_k": TOP_K,
                "cooldown_days": COOLDOWN_DAYS,
                "removed_after_topk_replay_delta": int(baseline_summary["n"] - summary["n"]),
                "mean_short_ret_lift_vs_policy_baseline": float(summary["mean_short_ret"] - baseline_summary["mean_short_ret"]),
                "stop_hit_rate_delta_vs_policy_baseline": float(summary["stop_hit_rate"] - baseline_summary["stop_hit_rate"]),
                **summary,
            }
            row["decision"] = (
                "hold_for_review"
                if row["n"] >= 1000
                and row["months"] >= 48
                and row["mean_short_ret"] > baseline_summary["mean_short_ret"]
                and row["positive_month_rate"] >= baseline_summary["positive_month_rate"]
                else "drop_or_diagnostic_only"
            )
            leaderboard.append(row)
    leaderboard.sort(
        key=lambda row: (
            row["decision"] == "hold_for_review",
            float(row["mean_short_ret"]),
            float(row["positive_month_rate"]),
            -float(row["stop_hit_rate"]),
        ),
        reverse=True,
    )
    examples: list[dict[str, Any]] = []
    if leaderboard:
        best = leaderboard[0]
        best_group = df[df["policy_id"] == best["policy_id"]].copy()
        if best["veto_id"] != "baseline_no_veto":
            best_group = best_group[best_group.apply(lambda row: not _veto_hit(best, row.to_dict()), axis=1)]
        selected = _apply_topk_cooldown(best_group, TOP_K, COOLDOWN_DAYS)
        examples = selected.sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    return leaderboard, examples


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    rows = _build_base_trades(daily)
    leaderboard, examples = _evaluate(rows)
    best = leaderboard[0] if leaderboard else None
    decision = {
        "authoritative_decision": "hold_review_only" if best and best.get("decision") == "hold_for_review" else "drop_no_entry_denial_veto_edge",
        "candidate_local_decision": best,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "single-axis signal-day bullish denial veto; costs and borrow ignored by user request",
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
            "changed_axis": "signal-day bullish denial veto only",
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
            "no_lookahead": "veto features use signal-day candle and prior rolling features only",
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
        },
    )
    _write_json(run_dir / "entry_denial_veto_leaderboard.json", {"rows": leaderboard})
    _write_jsonl(run_dir / "best_entry_denial_veto_examples.jsonl", examples)
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
                "entry_denial_veto_leaderboard.json",
                "best_entry_denial_veto_examples.jsonl",
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
