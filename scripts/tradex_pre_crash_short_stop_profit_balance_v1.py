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


AXIS_ID = "pre_crash_short_stop_profit_balance_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_stop_profit_balance_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
PROFIT_TARGETS = (0.08, 0.10, 0.12, 0.15, 0.18, 0.20)
STOP_LOSSES = (0.03, 0.04, 0.05, 0.06, 0.08, 0.10)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _build_policy_trades(daily: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    policies = [
        {
            "policy_id": f"pt{int(pt * 100)}_sl{int(sl * 100)}",
            "profit_target": pt,
            "stop_loss": sl,
        }
        for pt in PROFIT_TARGETS
        for sl in STOP_LOSSES
    ]
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
            }
            for policy in policies:
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


def _summarize_selected(selected: pd.DataFrame) -> dict[str, Any]:
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
        "severe_loss_rate_8pct": float((ret <= -0.08).mean()),
        "target_hit_rate": float(selected["exit_reason"].astype(str).str.startswith("profit_target").mean()),
        "stop_hit_rate": float(selected["exit_reason"].astype(str).str.startswith("stop_loss").mean()),
        "max_hold_rate": float((selected["exit_reason"] == "max_hold_close").mean()),
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
    examples: list[dict[str, Any]] = []
    for policy_id, group in df.groupby("policy_id"):
        selected = _apply_topk_cooldown(group, TOP_K, COOLDOWN_DAYS)
        if selected.empty:
            continue
        summary = _summarize_selected(selected)
        pt = float(selected["profit_target"].iloc[0])
        sl = float(selected["stop_loss"].iloc[0])
        row = {
            "policy_id": str(policy_id),
            "profit_target": pt,
            "stop_loss": sl,
            "reward_risk_ratio": pt / sl if sl else None,
            "top_k": TOP_K,
            "cooldown_days": COOLDOWN_DAYS,
            **summary,
        }
        row["decision"] = (
            "hold_for_review"
            if row["mean_short_ret"] > 0 and row["positive_month_rate"] >= 0.45 and row["target_hit_rate"] >= 0.12
            else "drop_or_diagnostic_only"
        )
        leaderboard.append(row)
    leaderboard.sort(
        key=lambda row: (
            row["decision"] == "hold_for_review",
            float(row["mean_short_ret"]),
            float(row["positive_month_rate"]),
            float(row["target_hit_rate"]),
        ),
        reverse=True,
    )
    if leaderboard:
        best_id = leaderboard[0]["policy_id"]
        sample = df[df["policy_id"] == best_id]
        selected = _apply_topk_cooldown(sample, TOP_K, COOLDOWN_DAYS)
        examples = selected.sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    return leaderboard, examples


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    rows = _build_policy_trades(daily)
    leaderboard, examples = _evaluate(rows)
    best = leaderboard[0] if leaderboard else None
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "portfolio": {"top_k": TOP_K, "cooldown_days": COOLDOWN_DAYS},
            "entry_convention": "next session open after signal day",
            "profit_targets": list(PROFIT_TARGETS),
            "stop_losses": list(STOP_LOSSES),
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
            "no_lookahead": "selection score uses signal-day and prior features only",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no entry gate tuning",
            "no live short claim",
        ],
    }
    decision = {
        "authoritative_decision": "hold_review_only" if best and best.get("decision") == "hold_for_review" else "drop_no_stop_profit_balance_edge",
        "candidate_local_decision": best,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "profit target and stop loss balance only; costs and borrow ignored by user request",
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
            "trade_policy_rows": int(len(rows)),
        },
    )
    _write_json(run_dir / "stop_profit_leaderboard.json", {"policies": leaderboard})
    _write_jsonl(run_dir / "best_stop_profit_examples.jsonl", examples)
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
                "stop_profit_leaderboard.json",
                "best_stop_profit_examples.jsonl",
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
