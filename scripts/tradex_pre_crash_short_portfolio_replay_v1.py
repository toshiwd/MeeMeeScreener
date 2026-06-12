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
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_short_portfolio_replay_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_portfolio_replay_v1")
TOP_K_VALUES = (3, 5, 10)
COOLDOWN_DAYS_VALUES = (5, 10, 20)
COSTS = (0.0, 0.003, 0.006)
POLICY_ID = "pt15_sl5"
PROFIT_TARGET = 0.15
STOP_LOSS = 0.05


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _score(features: dict[str, float | None]) -> float:
    range20 = float(features.get("range_20_0") or 0.0)
    range40 = float(features.get("range_40_20") or 0.0)
    dist_high = abs(float(features.get("dist_prior_80_high") or 0.0))
    red = float(features.get("red_cluster_10") or 0.0) / 10.0
    weak = float(features.get("weak_close_cluster_10") or 0.0) / 10.0
    return range20 * 0.35 + range40 * 0.25 + dist_high * 0.25 + red * 0.075 + weak * 0.075


def _build_candidate_trades(daily: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = _add_shape_features(group)
        for idx in range(140, len(g) - 21):
            current = g.iloc[idx]
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if not _is_gated_event(features, pattern):
                continue
            replay = _replay_trade(
                g,
                idx,
                profit_target=PROFIT_TARGET,
                stop_loss=STOP_LOSS,
                exit_mode="none",
            )
            if not replay.get("valid"):
                continue
            rows.append(
                {
                    "code": str(code),
                    "signal_ymd": int(current["ymd"]),
                    "month": int(current["ymd"]) // 100,
                    "pattern": pattern,
                    "policy_id": POLICY_ID,
                    "rank_score": _score(features),
                    **features,
                    **replay,
                }
            )
    return rows


def _apply_topk_cooldown(df: pd.DataFrame, top_k: int, cooldown_days: int) -> pd.DataFrame:
    selected: list[pd.Series] = []
    last_selected_index_by_code: dict[str, int] = {}
    dates = sorted(df["signal_ymd"].unique().tolist())
    ordinal_by_date = {date: idx for idx, date in enumerate(dates)}
    for date in dates:
        day = df[df["signal_ymd"] == date].sort_values("rank_score", ascending=False)
        picked = 0
        for _, row in day.iterrows():
            code = str(row["code"])
            last_idx = last_selected_index_by_code.get(code)
            current_idx = ordinal_by_date[date]
            if last_idx is not None and current_idx - last_idx <= cooldown_days:
                continue
            selected.append(row)
            last_selected_index_by_code[code] = current_idx
            picked += 1
            if picked >= top_k:
                break
    if not selected:
        return df.iloc[0:0].copy()
    return pd.DataFrame(selected).reset_index(drop=True)


def _summarize(selected: pd.DataFrame, *, top_k: int, cooldown_days: int, cost: float) -> dict[str, Any]:
    if selected.empty:
        return {
            "top_k": top_k,
            "cooldown_days": cooldown_days,
            "cost": cost,
            "n": 0,
            "decision": "drop_empty",
        }
    ret = selected["short_ret"].astype(float) - cost
    by_month = selected.assign(net_short_ret=ret).groupby("month")["net_short_ret"].mean()
    return {
        "top_k": top_k,
        "cooldown_days": cooldown_days,
        "cost": cost,
        "n": int(len(selected)),
        "symbols": int(selected["code"].nunique()),
        "months": int(selected["month"].nunique()),
        "mean_net_short_ret": float(ret.mean()),
        "median_net_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "loss_rate": float((ret < 0).mean()),
        "severe_loss_rate_8pct": float((ret <= -0.08).mean()),
        "target_hit_rate": float(selected["exit_reason"].astype(str).str.startswith("profit_target").mean()),
        "avg_hold_days": float(selected["hold_days"].mean()),
        "positive_month_rate": float((by_month > 0).mean()),
        "mean_monthly_avg_ret": float(by_month.mean()),
        "exit_reason_counts": selected["exit_reason"].value_counts().to_dict(),
        "decision": "hold_for_review" if float(ret.mean()) > 0 and float((by_month > 0).mean()) >= 0.45 else "drop_or_diagnostic_only",
    }


def _portfolio_grid(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    df = pd.DataFrame(rows)
    if df.empty:
        return [], []
    leaderboard: list[dict[str, Any]] = []
    selections: dict[tuple[int, int], pd.DataFrame] = {}
    for top_k in TOP_K_VALUES:
        for cooldown in COOLDOWN_DAYS_VALUES:
            selected = _apply_topk_cooldown(df, top_k, cooldown)
            selections[(top_k, cooldown)] = selected
            for cost in COSTS:
                leaderboard.append(_summarize(selected, top_k=top_k, cooldown_days=cooldown, cost=cost))
    leaderboard.sort(
        key=lambda row: (
            row.get("decision") == "hold_for_review",
            float(row.get("mean_net_short_ret") or -999),
            float(row.get("positive_month_rate") or 0),
        ),
        reverse=True,
    )
    best = leaderboard[0] if leaderboard else None
    examples: list[dict[str, Any]] = []
    if best:
        selected = selections.get((int(best["top_k"]), int(best["cooldown_days"])))
        if selected is not None and not selected.empty:
            sample = selected.sort_values("short_ret", ascending=False).head(100).copy()
            sample["cost"] = float(best["cost"])
            sample["net_short_ret"] = sample["short_ret"].astype(float) - float(best["cost"])
            examples = sample.to_dict(orient="records")
    return leaderboard, examples


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    candidate_rows = _build_candidate_trades(daily)
    leaderboard, examples = _portfolio_grid(candidate_rows)
    best = leaderboard[0] if leaderboard else None
    cost06 = [row for row in leaderboard if abs(float(row.get("cost") or 0.0) - 0.006) < 1e-9]
    best_cost06 = cost06[0] if cost06 else None
    decision = {
        "authoritative_decision": "hold_review_only" if best and best.get("decision") == "hold_for_review" else "drop_no_portfolio_edge",
        "candidate_local_decision": best,
        "cost_006_decision": best_cost06,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "deduped top-k portfolio replay with fixed entry and pt15_sl5 exit",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "entry_convention": "next session open after signal day",
            "exit_policy": "pt15_sl5",
            "top_k_values": list(TOP_K_VALUES),
            "cooldown_days_values": list(COOLDOWN_DAYS_VALUES),
            "costs": list(COSTS),
            "ranking_score": "range20/range40/high-distance/red/weak weighted diagnostic score",
            "borrow_lending": "not_available_short_side_theoretical_only",
            "no_lookahead": "selection score uses signal-day and prior features only",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no entry gate tuning",
            "no exit policy tuning",
            "no validated live short claim",
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
            "candidate_trade_rows": int(len(candidate_rows)),
        },
    )
    _write_json(run_dir / "portfolio_leaderboard.json", {"policies": leaderboard})
    _write_jsonl(run_dir / "best_portfolio_examples.jsonl", examples)
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
                "portfolio_leaderboard.json",
                "best_portfolio_examples.jsonl",
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
