from __future__ import annotations

import argparse
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


AXIS_ID = "pre_crash_short_refined_entry_stability_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_refined_entry_stability_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
PROFIT_TARGET = 0.20
STOP_LOSS = 0.08
ENTRY_READY_RANGE_40_20_MIN = 0.46500567966679285
ENTRY_READY_LAST_VOL_RATIO_MAX = 0.9019019159535286
ENTRY_READY_DIST_HIGH_MIN = -0.4845991561181434


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


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
            if replay.get("valid"):
                rows.append(
                    {
                        "code": str(code),
                        "signal_ymd": int(current["ymd"]),
                        "month": int(current["ymd"]) // 100,
                        "year": int(current["ymd"]) // 10000,
                        "pattern": pattern,
                        "rank_score": float(_score_volume_break(features)),
                        **features,
                        **replay,
                    }
                )
    return rows


def _summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n": 0}
    ret = df["short_ret"].astype(float)
    return {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_hit_rate": float(df["exit_reason"].astype(str).str.startswith("profit_target").mean()),
        "stop_hit_rate": float(df["exit_reason"].astype(str).str.startswith("stop_loss").mean()),
        "max_hold_rate": float((df["exit_reason"] == "max_hold_close").mean()),
        "exit_reason_counts": df["exit_reason"].value_counts().to_dict(),
    }


def _period_rows(df: pd.DataFrame, key: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value, group in df.groupby(key):
        row = {key: int(value), **_summary(group)}
        row["period_decision"] = "positive" if float(row["mean_short_ret"]) > 0 else "negative"
        rows.append(row)
    rows.sort(key=lambda row: int(row[key]))
    return rows


def _stability(df: pd.DataFrame) -> dict[str, Any]:
    overall = _summary(df)
    monthly = _period_rows(df, "month")
    yearly = _period_rows(df, "year")
    active_months = [row for row in monthly if int(row.get("n", 0)) > 0]
    positive_months = [row for row in active_months if float(row["mean_short_ret"]) > 0]
    negative_months = [row for row in active_months if float(row["mean_short_ret"]) <= 0]
    active_years = [row for row in yearly if int(row.get("n", 0)) > 0]
    positive_years = [row for row in active_years if float(row["mean_short_ret"]) > 0]
    return {
        "overall": overall,
        "active_months": int(len(active_months)),
        "positive_months": int(len(positive_months)),
        "negative_months": int(len(negative_months)),
        "positive_month_rate": float(len(positive_months) / len(active_months)) if active_months else None,
        "active_years": int(len(active_years)),
        "positive_years": int(len(positive_years)),
        "positive_year_rate": float(len(positive_years) / len(active_years)) if active_years else None,
        "worst_months": sorted(active_months, key=lambda row: float(row["mean_short_ret"]))[:10],
        "best_months": sorted(active_months, key=lambda row: float(row["mean_short_ret"]), reverse=True)[:10],
        "monthly": monthly,
        "yearly": yearly,
    }


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    trades = _build_trades(daily)
    df = pd.DataFrame(trades)
    selected = _apply_topk_cooldown(df, TOP_K, COOLDOWN_DAYS) if not df.empty else df
    refined = selected[
        (selected["range_40_20"].astype(float) >= ENTRY_READY_RANGE_40_20_MIN)
        & (selected["last_vol_ratio"].astype(float) <= ENTRY_READY_LAST_VOL_RATIO_MAX)
        & (selected["dist_prior_80_high"].astype(float) >= ENTRY_READY_DIST_HIGH_MIN)
    ].copy() if not selected.empty else selected
    stability = _stability(refined)
    overall = stability["overall"]
    positive_month_rate = stability["positive_month_rate"] or 0.0
    positive_year_rate = stability["positive_year_rate"] or 0.0
    keep = (
        int(overall.get("n", 0)) >= 120
        and float(overall.get("mean_short_ret", 0.0)) > 0.02
        and positive_month_rate >= 0.58
        and positive_year_rate >= 0.70
    )
    decision = {
        "authoritative_decision": "hold_refined_entry_stability" if keep else "drop_or_hold_unstable_refined_entry",
        "candidate_local_decision": {
            "overall": overall,
            "positive_month_rate": positive_month_rate,
            "positive_year_rate": positive_year_rate,
            "active_months": stability["active_months"],
            "active_years": stability["active_years"],
        },
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "stability check only for refined EntryReady; costs and borrow ignored by user request",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "refined_entry_ready": {
                "range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
                "last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
                "dist_prior_80_high_min": ENTRY_READY_DIST_HIGH_MIN,
            },
            "portfolio": {"top_k": TOP_K, "cooldown_days": COOLDOWN_DAYS, "rank_score": "volume_break"},
            "exit_policy": {"profit_target": PROFIT_TARGET, "stop_loss": STOP_LOSS},
            "changed_axis": "none; stability check only",
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no new entry feature",
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
            "trade_rows": int(len(trades)),
            "selected_rows": int(len(selected)),
            "refined_entry_rows": int(len(refined)),
        },
    )
    _write_json(run_dir / "refined_entry_stability.json", stability)
    _write_jsonl(run_dir / "refined_entry_trades.jsonl", refined.to_dict(orient="records"))
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
                "refined_entry_stability.json",
                "refined_entry_trades.jsonl",
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
