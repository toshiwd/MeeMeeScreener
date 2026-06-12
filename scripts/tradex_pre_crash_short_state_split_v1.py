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
from scripts.tradex_pre_crash_short_portfolio_replay_v1 import _apply_topk_cooldown
from scripts.tradex_pre_crash_short_rank_score_branch_v1 import _score_volume_break
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_short_state_split_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_state_split_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
PROFIT_TARGET = 0.20
STOP_LOSS = 0.08
MIN_STATE_N = 250
MIN_MONTHS = 36


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _f(row: pd.Series | dict[str, Any], key: str) -> float:
    value = row.get(key)
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except Exception:
        pass
    return float(value)


def _build_trades(daily: pd.DataFrame) -> list[dict[str, Any]]:
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
                    "rank_score": float(_score_volume_break(features)),
                    **features,
                    **replay,
                }
            )
    return rows


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "n": 0,
            "symbols": 0,
            "months": 0,
            "mean_short_ret": None,
            "target_hit_rate": None,
            "stop_hit_rate": None,
            "positive_month_rate": None,
        }
    ret = df["short_ret"].astype(float)
    by_month = df.assign(short_ret_for_month=ret).groupby("month")["short_ret_for_month"].mean()
    return {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "loss_rate": float((ret < 0).mean()),
        "target_hit_rate": float(df["exit_reason"].astype(str).str.startswith("profit_target").mean()),
        "stop_hit_rate": float(df["exit_reason"].astype(str).str.startswith("stop_loss").mean()),
        "max_hold_rate": float((df["exit_reason"] == "max_hold_close").mean()),
        "avg_hold_days": float(df["hold_days"].mean()),
        "positive_month_rate": float((by_month > 0).mean()),
        "mean_monthly_avg_ret": float(by_month.mean()),
        "exit_reason_counts": df["exit_reason"].value_counts().to_dict(),
    }


def _quantile_thresholds(df: pd.DataFrame, feature: str) -> list[float]:
    values = df[feature].dropna().astype(float)
    if values.empty:
        return []
    return sorted({float(values.quantile(q)) for q in (0.25, 0.33, 0.50, 0.67, 0.75)})


def _apply_rule(df: pd.DataFrame, feature: str, op: str, threshold: float) -> pd.Series:
    if op == "ge":
        return df[feature].astype(float) >= threshold
    if op == "le":
        return df[feature].astype(float) <= threshold
    raise ValueError(f"unknown op: {op}")


def _evaluate_one_axis(df: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    baseline = _summarize(df)
    candidates: list[dict[str, Any]] = []
    features = [
        "last_vol_ratio",
        "weak_close_cluster_10",
        "red_cluster_10",
        "dist_prior_80_high",
        "dist_prior_80_low",
        "ret_20_0",
        "ret_40_20",
        "range_20_0",
        "range_40_20",
        "late_high_break",
    ]
    for feature in features:
        for threshold in _quantile_thresholds(df, feature):
            for op in ("ge", "le"):
                mask = _apply_rule(df, feature, op, threshold)
                entry_ready = df[mask].copy()
                avoid = df[~mask].copy()
                er = _summarize(entry_ready)
                av = _summarize(avoid)
                if er["n"] < MIN_STATE_N or er["months"] < MIN_MONTHS or av["n"] < MIN_STATE_N:
                    continue
                mean_lift = float(er["mean_short_ret"] - baseline["mean_short_ret"])
                target_lift = float(er["target_hit_rate"] - baseline["target_hit_rate"])
                stop_delta = float(er["stop_hit_rate"] - baseline["stop_hit_rate"])
                avoid_mean_delta = float(av["mean_short_ret"] - baseline["mean_short_ret"])
                row = {
                    "axis_id": f"{feature}_{op}_{threshold:.6f}",
                    "feature": feature,
                    "op": op,
                    "threshold": threshold,
                    "baseline": baseline,
                    "states": {
                        "EntryReady": er,
                        "Avoid": av,
                        "Watch": baseline,
                    },
                    "entry_ready_mean_lift": mean_lift,
                    "entry_ready_target_hit_lift": target_lift,
                    "entry_ready_stop_hit_delta": stop_delta,
                    "avoid_mean_delta": avoid_mean_delta,
                    "decision": (
                        "keep"
                        if mean_lift >= 0.004
                        and target_lift >= 0.02
                        and stop_delta <= 0.0
                        and avoid_mean_delta <= 0.0
                        else "drop_or_diagnostic"
                    ),
                }
                candidates.append(row)
    candidates.sort(
        key=lambda row: (
            row["decision"] == "keep",
            float(row["entry_ready_mean_lift"]),
            float(row["entry_ready_target_hit_lift"]),
            -float(row["entry_ready_stop_hit_delta"]),
        ),
        reverse=True,
    )
    best = candidates[0] if candidates else None
    return candidates, best


def _state_examples(df: pd.DataFrame, best: dict[str, Any] | None) -> list[dict[str, Any]]:
    if best is None:
        return df.sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    mask = _apply_rule(df, str(best["feature"]), str(best["op"]), float(best["threshold"]))
    entry_ready = df[mask].copy()
    entry_ready["review_state"] = "EntryReady"
    avoid = df[~mask].copy()
    avoid["review_state"] = "Avoid"
    examples = pd.concat(
        [
            entry_ready.sort_values("short_ret", ascending=False).head(50),
            avoid.sort_values("short_ret", ascending=True).head(50),
        ],
        ignore_index=True,
    )
    return examples.to_dict(orient="records")


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    rows = _build_trades(daily)
    df = pd.DataFrame(rows)
    selected = _apply_topk_cooldown(df, TOP_K, COOLDOWN_DAYS) if not df.empty else df
    leaderboard, best = _evaluate_one_axis(selected) if not selected.empty else ([], None)
    examples = _state_examples(selected, best) if not selected.empty else []
    keep = best is not None and best.get("decision") == "keep"
    decision = {
        "authoritative_decision": "keep_state_split" if keep else "drop_no_state_split_edge",
        "candidate_local_decision": best,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "single-axis EntryReady/Avoid split on pt20_sl8 selected candidates; costs and borrow ignored by user request",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "portfolio": {"top_k": TOP_K, "cooldown_days": COOLDOWN_DAYS, "rank_score": "volume_break"},
            "entry_convention": "next session open after signal day",
            "exit_policy": {"profit_target": PROFIT_TARGET, "stop_loss": STOP_LOSS},
            "changed_axis": "single feature threshold state split only",
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
            "no_lookahead": "state features use signal-day and prior rolling features only",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no multi-feature model",
            "no pattern classifier change",
            "no exit policy change",
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
            "selected_rows": int(len(selected)),
        },
    )
    _write_json(run_dir / "state_split_leaderboard.json", {"rows": leaderboard})
    _write_jsonl(run_dir / "state_split_examples.jsonl", examples)
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
                "state_split_leaderboard.json",
                "state_split_examples.jsonl",
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
