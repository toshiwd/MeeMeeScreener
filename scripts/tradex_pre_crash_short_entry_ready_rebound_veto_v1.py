from __future__ import annotations

import argparse
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


AXIS_ID = "pre_crash_short_entry_ready_rebound_veto_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_entry_ready_rebound_veto_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
PROFIT_TARGET = 0.20
STOP_LOSS = 0.08
ENTRY_READY_RANGE_40_20_MIN = 0.46500567966679285
ENTRY_READY_LAST_VOL_RATIO_MAX = 0.9019019159535286
MIN_ENTRY_N = 120
MIN_MONTHS = 30


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
                        "pattern": pattern,
                        "rank_score": float(_score_volume_break(features)),
                        **features,
                        **replay,
                    }
                )
    return rows


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n": 0, "symbols": 0, "months": 0}
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
        "positive_month_rate": float((by_month > 0).mean()),
        "mean_monthly_avg_ret": float(by_month.mean()),
        "exit_reason_counts": df["exit_reason"].value_counts().to_dict(),
    }


def _thresholds(df: pd.DataFrame, feature: str) -> list[float]:
    values = df[feature].dropna().astype(float)
    if values.empty:
        return []
    return sorted({float(values.quantile(q)) for q in (0.20, 0.33, 0.50, 0.67, 0.80)})


def _mask(df: pd.DataFrame, feature: str, op: str, threshold: float) -> pd.Series:
    values = df[feature].astype(float)
    if op == "ge":
        return values >= threshold
    if op == "le":
        return values <= threshold
    raise ValueError(op)


def _evaluate(entry_ready: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    baseline = _summarize(entry_ready)
    rows: list[dict[str, Any]] = []
    veto_features = [
        "ret_20_0",
        "ret_60_0",
        "dist_prior_80_low",
        "dist_prior_80_high",
        "range_20_0",
        "late_high_break",
    ]
    for feature in veto_features:
        for threshold in _thresholds(entry_ready, feature):
            for op in ("ge", "le"):
                keep_mask = _mask(entry_ready, feature, op, threshold)
                kept = entry_ready[keep_mask].copy()
                vetoed = entry_ready[~keep_mask].copy()
                kept_summary = _summarize(kept)
                veto_summary = _summarize(vetoed)
                if kept_summary["n"] < MIN_ENTRY_N or kept_summary["months"] < MIN_MONTHS or veto_summary["n"] < 40:
                    continue
                mean_lift = float(kept_summary["mean_short_ret"] - baseline["mean_short_ret"])
                stop_delta = float(kept_summary["stop_hit_rate"] - baseline["stop_hit_rate"])
                target_delta = float(kept_summary["target_hit_rate"] - baseline["target_hit_rate"])
                positive_month_delta = float(kept_summary["positive_month_rate"] - baseline["positive_month_rate"])
                veto_mean_delta = float(veto_summary["mean_short_ret"] - baseline["mean_short_ret"])
                row = {
                    "axis_id": f"keep_{feature}_{op}_{threshold:.6f}",
                    "feature": feature,
                    "op": op,
                    "threshold": threshold,
                    "baseline_entry_ready": baseline,
                    "states": {
                        "EntryReady": kept_summary,
                        "Watch": baseline,
                        "Avoid": veto_summary,
                    },
                    "entry_ready_mean_lift": mean_lift,
                    "entry_ready_stop_hit_delta": stop_delta,
                    "entry_ready_target_hit_delta": target_delta,
                    "entry_ready_positive_month_delta": positive_month_delta,
                    "vetoed_mean_delta": veto_mean_delta,
                    "decision": (
                        "keep"
                        if stop_delta <= -0.04
                        and mean_lift >= 0.0
                        and target_delta >= -0.01
                        and positive_month_delta >= -0.05
                        and veto_mean_delta <= 0.0
                        else "drop_or_diagnostic"
                    ),
                }
                rows.append(row)
    rows.sort(
        key=lambda row: (
            row["decision"] == "keep",
            -float(row["entry_ready_stop_hit_delta"]),
            float(row["entry_ready_mean_lift"]),
            float(row["entry_ready_target_hit_delta"]),
        ),
        reverse=True,
    )
    return rows, rows[0] if rows else None


def _examples(entry_ready: pd.DataFrame, best: dict[str, Any] | None) -> list[dict[str, Any]]:
    if best is None:
        return entry_ready.sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    keep_mask = _mask(entry_ready, str(best["feature"]), str(best["op"]), float(best["threshold"]))
    kept = entry_ready[keep_mask].copy()
    kept["review_state"] = "EntryReady"
    vetoed = entry_ready[~keep_mask].copy()
    vetoed["review_state"] = "Avoid"
    out = pd.concat(
        [kept.sort_values("short_ret", ascending=False).head(50), vetoed.sort_values("short_ret", ascending=True).head(50)],
        ignore_index=True,
    )
    return out.to_dict(orient="records")


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    trades = _build_trades(daily)
    df = pd.DataFrame(trades)
    selected = _apply_topk_cooldown(df, TOP_K, COOLDOWN_DAYS) if not df.empty else df
    entry_ready = selected[
        (selected["range_40_20"].astype(float) >= ENTRY_READY_RANGE_40_20_MIN)
        & (selected["last_vol_ratio"].astype(float) <= ENTRY_READY_LAST_VOL_RATIO_MAX)
    ].copy() if not selected.empty else selected
    leaderboard, best = _evaluate(entry_ready) if not entry_ready.empty else ([], None)
    keep = best is not None and best.get("decision") == "keep"
    decision = {
        "authoritative_decision": "keep_entry_ready_rebound_veto" if keep else "drop_no_entry_ready_rebound_veto_edge",
        "candidate_local_decision": best,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "single-axis rebound-risk veto inside kept EntryReady population; costs and borrow ignored by user request",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "entry_ready_parent": {
                "range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
                "last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
            },
            "portfolio": {"top_k": TOP_K, "cooldown_days": COOLDOWN_DAYS, "rank_score": "volume_break"},
            "entry_convention": "next session open after signal day",
            "exit_policy": {"profit_target": PROFIT_TARGET, "stop_loss": STOP_LOSS},
            "changed_axis": "single rebound-risk veto inside EntryReady only",
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
            "no_lookahead": "veto features use signal-day and prior rolling features only",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no exit policy change",
            "no parent EntryReady rule change",
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
            "entry_ready_rows": int(len(entry_ready)),
        },
    )
    _write_json(run_dir / "entry_ready_rebound_veto_leaderboard.json", {"rows": leaderboard})
    _write_jsonl(run_dir / "entry_ready_rebound_veto_examples.jsonl", _examples(entry_ready, best) if not entry_ready.empty else [])
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
                "entry_ready_rebound_veto_leaderboard.json",
                "entry_ready_rebound_veto_examples.jsonl",
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
