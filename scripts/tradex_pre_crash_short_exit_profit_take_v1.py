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

from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features, _load_daily
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import TYPICAL_PATTERNS
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_short_exit_profit_take_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_exit_profit_take_v1")
RANGE_20_MIN = 0.28
RANGE_40_MIN = 0.20
DIST_HIGH_MAX = -0.24
PROFIT_TARGETS = (0.08, 0.12, 0.15)
STOP_LOSSES = (0.05, 0.08)
MAX_HOLD_DAYS = 20


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _feature_payload(row: pd.Series) -> dict[str, float | None]:
    keys = [
        "ret_80_40",
        "ret_40_20",
        "ret_20_0",
        "ret_60_0",
        "range_40_20",
        "range_20_0",
        "dist_prior_80_high",
        "dist_prior_80_low",
        "late_high_break",
        "last_vol_ratio",
        "red_cluster_10",
        "weak_close_cluster_10",
    ]
    return {key: None if pd.isna(row.get(key)) else float(row.get(key)) for key in keys}


def _is_gated_event(features: dict[str, float | None], pattern: str) -> bool:
    if pattern not in TYPICAL_PATTERNS:
        return False
    r20 = features.get("range_20_0")
    r40 = features.get("range_40_20")
    dh = features.get("dist_prior_80_high")
    if r20 is None or r40 is None or dh is None:
        return False
    return r20 >= RANGE_20_MIN and r40 >= RANGE_40_MIN and dh <= DIST_HIGH_MAX


def _escape_flags(row: pd.Series, signal_high: float, ma5: float | None) -> dict[str, bool]:
    open_ = float(row["o"])
    high = float(row["h"])
    low = float(row["l"])
    close = float(row["c"])
    span = high - low
    if span <= 0 or open_ <= 0:
        return {
            "bullish_denial": False,
            "signal_high_reclaim": False,
            "ma5_reclaim": False,
            "large_bullish_denial": False,
        }
    close_pos = (close - low) / span
    bullish = close > open_
    large = bullish and abs(close - open_) / open_ >= 0.025 and close_pos >= 0.65
    signal_high_reclaim = bullish and close_pos >= 0.65 and close > signal_high
    ma5_reclaim = bullish and close_pos >= 0.65 and ma5 is not None and close > ma5
    return {
        "bullish_denial": bool(signal_high_reclaim or ma5_reclaim or large),
        "signal_high_reclaim": bool(signal_high_reclaim),
        "ma5_reclaim": bool(ma5_reclaim),
        "large_bullish_denial": bool(large),
    }


def _replay_trade(
    g: pd.DataFrame,
    idx: int,
    *,
    profit_target: float | None,
    stop_loss: float | None,
    exit_mode: str,
) -> dict[str, Any]:
    signal = g.iloc[idx]
    entry_idx = idx + 1
    exit_limit = min(idx + MAX_HOLD_DAYS, len(g) - 1)
    if entry_idx > exit_limit:
        return {"valid": False}
    entry = g.iloc[entry_idx]
    entry_price = float(entry["o"])
    signal_high = float(signal["h"])
    ma5 = None if pd.isna(signal.get("ma5")) else float(signal["ma5"])
    if entry_price <= 0:
        return {"valid": False}
    exit_price = float(g.iloc[exit_limit]["c"])
    exit_idx = exit_limit
    exit_reason = "max_hold_close"
    for day_idx in range(entry_idx, exit_limit + 1):
        row = g.iloc[day_idx]
        high = float(row["h"])
        low = float(row["l"])
        if stop_loss is not None and high >= entry_price * (1.0 + stop_loss):
            exit_price = entry_price * (1.0 + stop_loss)
            exit_idx = day_idx
            exit_reason = f"stop_loss_{stop_loss:.2f}"
            break
        if profit_target is not None and low <= entry_price * (1.0 - profit_target):
            exit_price = entry_price * (1.0 - profit_target)
            exit_idx = day_idx
            exit_reason = f"profit_target_{profit_target:.2f}"
            break
        flags = _escape_flags(row, signal_high, ma5)
        if exit_mode == "signal_high_reclaim" and flags["signal_high_reclaim"]:
            exit_price = float(row["c"])
            exit_idx = day_idx
            exit_reason = "signal_high_reclaim"
            break
        if exit_mode == "ma5_reclaim" and flags["ma5_reclaim"]:
            exit_price = float(row["c"])
            exit_idx = day_idx
            exit_reason = "ma5_reclaim"
            break
        if exit_mode == "any_bullish_denial" and flags["bullish_denial"]:
            exit_price = float(row["c"])
            exit_idx = day_idx
            exit_reason = "any_bullish_denial"
            break
    short_ret = entry_price / exit_price - 1.0 if exit_price > 0 else None
    return {
        "valid": short_ret is not None,
        "entry_ymd": int(entry["ymd"]),
        "entry_price": entry_price,
        "exit_ymd": int(g.iloc[exit_idx]["ymd"]),
        "exit_price": exit_price,
        "hold_days": int(exit_idx - entry_idx + 1),
        "exit_reason": exit_reason,
        "short_ret": short_ret,
    }


def _build_trades(daily: pd.DataFrame) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    policies: list[dict[str, Any]] = [{"policy_id": "hold20", "profit_target": None, "stop_loss": None, "exit_mode": "none"}]
    for target in PROFIT_TARGETS:
        policies.append(
            {
                "policy_id": f"pt{int(target * 100)}",
                "profit_target": target,
                "stop_loss": None,
                "exit_mode": "none",
            }
        )
        for stop in STOP_LOSSES:
            policies.append(
                {
                    "policy_id": f"pt{int(target * 100)}_sl{int(stop * 100)}",
                    "profit_target": target,
                    "stop_loss": stop,
                    "exit_mode": "none",
                }
            )
    for exit_mode in ("signal_high_reclaim", "ma5_reclaim", "any_bullish_denial"):
        policies.append(
            {
                "policy_id": f"pt12_sl8_exit_{exit_mode}",
                "profit_target": 0.12,
                "stop_loss": 0.08,
                "exit_mode": exit_mode,
            }
        )
    for code, group in daily.groupby("code", sort=False):
        g = _add_shape_features(group)
        for idx in range(140, len(g) - MAX_HOLD_DAYS - 1):
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
                **features,
            }
            for policy in policies:
                replay = _replay_trade(
                    g,
                    idx,
                    profit_target=policy["profit_target"],
                    stop_loss=policy["stop_loss"],
                    exit_mode=policy["exit_mode"],
                )
                if not replay.get("valid"):
                    continue
                trades.append({**base, **policy, **replay})
    return trades


def _summary(trades: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    df = pd.DataFrame(trades)
    if df.empty:
        return [], []
    rows: list[dict[str, Any]] = []
    for policy_id, group in df.groupby("policy_id"):
        ret = group["short_ret"].astype(float)
        rows.append(
            {
                "policy_id": policy_id,
                "n": int(len(group)),
                "symbols": int(group["code"].nunique()),
                "months": int(group["month"].nunique()),
                "mean_short_ret": float(ret.mean()),
                "median_short_ret": float(ret.median()),
                "win_rate": float((ret > 0).mean()),
                "loss_rate": float((ret < 0).mean()),
                "severe_loss_rate_8pct": float((ret <= -0.08).mean()),
                "target_hit_rate": float(group["exit_reason"].astype(str).str.startswith("profit_target").mean()),
                "avg_hold_days": float(group["hold_days"].mean()),
                "exit_reason_counts": group["exit_reason"].value_counts().to_dict(),
            }
        )
    rows.sort(
        key=lambda row: (
            float(row["mean_short_ret"]),
            float(row["win_rate"]),
            -float(row["severe_loss_rate_8pct"]),
        ),
        reverse=True,
    )
    examples = df[df["policy_id"] == rows[0]["policy_id"]].sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    return rows, examples


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    trades = _build_trades(daily)
    leaderboard, examples = _summary(trades)
    champion = leaderboard[0] if leaderboard else None
    hold20 = next((row for row in leaderboard if row["policy_id"] == "hold20"), None)
    decision = {
        "authoritative_decision": "hold_for_exit_pretest" if champion and hold20 and champion["mean_short_ret"] > hold20["mean_short_ret"] else "drop_exit_policy_no_improvement",
        "candidate_local_decision": champion,
        "baseline_hold20": hold20,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "fixed entry gate with profit-taking and bullish denial exits compared against hold20",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": "typical pattern plus range20>=0.28 range40>=0.20 dist_prior_80_high<=-0.24",
            "entry_convention": "next session open after signal day",
            "max_hold_days": MAX_HOLD_DAYS,
            "profit_targets": list(PROFIT_TARGETS),
            "stop_losses": list(STOP_LOSSES),
            "bullish_denial_exits": ["signal_high_reclaim", "ma5_reclaim", "any_bullish_denial"],
            "cost_slippage": "not_applied_diagnostic_only",
            "borrow_lending": "not_available_short_side_theoretical_only",
            "no_lookahead": "entry features use rows at or before signal date; exits use subsequent execution path",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no entry gate tuning",
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
            "trade_rows": int(len(trades)),
        },
    )
    _write_json(run_dir / "exit_policy_leaderboard.json", {"policies": leaderboard})
    _write_jsonl(run_dir / "best_policy_examples.jsonl", examples)
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
                "exit_policy_leaderboard.json",
                "best_policy_examples.jsonl",
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
