from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
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


AXIS_ID = "pre_crash_short_regime_permission_gate_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_regime_permission_gate_v1")
TOP_K = 3
COOLDOWN_DAYS = 5
PROFIT_TARGET = 0.20
STOP_LOSS = 0.08
ENTRY_READY_RANGE_40_20_MIN = 0.46500567966679285
ENTRY_READY_LAST_VOL_RATIO_MAX = 0.9019019159535286
ENTRY_READY_DIST_HIGH_MIN = -0.4845991561181434
MIN_KEPT_N = 20
MIN_YEARS = 2


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _load_regime(db_path: Path) -> pd.DataFrame:
    sql = """
        SELECT
          CAST(dt AS INTEGER) AS signal_ymd,
          regime_id,
          CAST(breadth_above_ma20 AS DOUBLE) AS breadth_above_ma20,
          CAST(breadth_above_ma60 AS DOUBLE) AS breadth_above_ma60,
          CAST(advancers_ratio AS DOUBLE) AS advancers_ratio,
          CAST(index_close_vs_ma20 AS DOUBLE) AS index_close_vs_ma20,
          CAST(index_close_vs_ma60 AS DOUBLE) AS index_close_vs_ma60,
          CAST(market_atr_pct AS DOUBLE) AS market_atr_pct,
          CAST(regime_score AS DOUBLE) AS regime_score
        FROM market_regime_daily
    """
    return duckdb.connect(str(db_path), read_only=True).execute(sql).df()


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
    by_month = df.assign(short_ret_for_month=ret).groupby("month")["short_ret_for_month"].mean()
    by_year = df.assign(short_ret_for_year=ret).groupby("year")["short_ret_for_year"].mean()
    return {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "years": int(df["year"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_hit_rate": float(df["exit_reason"].astype(str).str.startswith("profit_target").mean()),
        "stop_hit_rate": float(df["exit_reason"].astype(str).str.startswith("stop_loss").mean()),
        "positive_month_rate": float((by_month > 0).mean()),
        "positive_year_rate": float((by_year > 0).mean()),
        "exit_reason_counts": df["exit_reason"].value_counts().to_dict(),
    }


def _thresholds(df: pd.DataFrame, feature: str) -> list[float]:
    values = df[feature].dropna().astype(float)
    if values.empty:
        return []
    return sorted({float(values.quantile(q)) for q in (0.25, 0.33, 0.50, 0.67, 0.75)})


def _gate_mask(df: pd.DataFrame, feature: str, op: str, threshold: float) -> pd.Series:
    values = df[feature].astype(float)
    if op == "le":
        return values <= threshold
    if op == "ge":
        return values >= threshold
    raise ValueError(op)


def _evaluate(refined: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    refined = refined.dropna(
        subset=[
            "breadth_above_ma20",
            "breadth_above_ma60",
            "advancers_ratio",
            "index_close_vs_ma20",
            "index_close_vs_ma60",
            "market_atr_pct",
            "regime_score",
        ]
    ).copy()
    if refined.empty:
        return [], None
    baseline = _summary(refined)
    rows: list[dict[str, Any]] = []
    features = [
        "breadth_above_ma20",
        "breadth_above_ma60",
        "advancers_ratio",
        "index_close_vs_ma20",
        "index_close_vs_ma60",
        "market_atr_pct",
        "regime_score",
    ]
    for feature in features:
        for threshold in _thresholds(refined, feature):
            for op in ("le", "ge"):
                mask = _gate_mask(refined, feature, op, threshold)
                permitted = refined[mask].copy()
                blocked = refined[~mask].copy()
                ps = _summary(permitted)
                bs = _summary(blocked)
                if ps.get("n", 0) < MIN_KEPT_N or ps.get("years", 0) < MIN_YEARS or bs.get("n", 0) < 10:
                    continue
                row = {
                    "axis_id": f"{feature}_{op}_{threshold:.6f}",
                    "feature": feature,
                    "op": op,
                    "threshold": threshold,
                    "baseline": baseline,
                    "states": {"PermitShort": ps, "BlockShort": bs, "Watch": baseline},
                    "permit_mean_lift": float(ps["mean_short_ret"] - baseline["mean_short_ret"]),
                    "permit_stop_hit_delta": float(ps["stop_hit_rate"] - baseline["stop_hit_rate"]),
                    "permit_target_hit_delta": float(ps["target_hit_rate"] - baseline["target_hit_rate"]),
                    "permit_positive_month_delta": float(ps["positive_month_rate"] - baseline["positive_month_rate"]),
                    "permit_positive_year_delta": float(ps["positive_year_rate"] - baseline["positive_year_rate"]),
                    "blocked_mean_delta": float(bs["mean_short_ret"] - baseline["mean_short_ret"]),
                    "decision": "drop_or_diagnostic",
                }
                row["decision"] = (
                    "keep"
                    if row["permit_mean_lift"] >= 0.004
                    and row["permit_stop_hit_delta"] <= -0.03
                    and row["permit_positive_month_delta"] >= 0
                    and row["blocked_mean_delta"] <= 0
                    else "drop_or_diagnostic"
                )
                rows.append(row)
    rows.sort(
        key=lambda row: (
            row["decision"] == "keep",
            float(row["permit_mean_lift"]),
            -float(row["permit_stop_hit_delta"]),
            float(row["permit_positive_month_delta"]),
        ),
        reverse=True,
    )
    return rows, rows[0] if rows else None


def _examples(refined: pd.DataFrame, best: dict[str, Any] | None) -> list[dict[str, Any]]:
    if best is None:
        return refined.sort_values("short_ret", ascending=False).head(100).to_dict(orient="records")
    mask = _gate_mask(refined, str(best["feature"]), str(best["op"]), float(best["threshold"]))
    permitted = refined[mask].copy()
    permitted["review_state"] = "PermitShort"
    blocked = refined[~mask].copy()
    blocked["review_state"] = "BlockShort"
    out = pd.concat(
        [permitted.sort_values("short_ret", ascending=False).head(50), blocked.sort_values("short_ret", ascending=True).head(50)],
        ignore_index=True,
    )
    return out.to_dict(orient="records")


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    regime = _load_regime(db_path)
    trades = _build_trades(daily)
    df = pd.DataFrame(trades)
    selected = _apply_topk_cooldown(df, TOP_K, COOLDOWN_DAYS) if not df.empty else df
    refined = selected[
        (selected["range_40_20"].astype(float) >= ENTRY_READY_RANGE_40_20_MIN)
        & (selected["last_vol_ratio"].astype(float) <= ENTRY_READY_LAST_VOL_RATIO_MAX)
        & (selected["dist_prior_80_high"].astype(float) >= ENTRY_READY_DIST_HIGH_MIN)
    ].copy() if not selected.empty else selected
    joined = refined.merge(regime, on="signal_ymd", how="left") if not refined.empty else refined
    joined_with_regime = joined.dropna(subset=["regime_score"]).copy() if not joined.empty else joined
    leaderboard, best = _evaluate(joined) if not joined.empty else ([], None)
    keep = best is not None and best.get("decision") == "keep"
    decision = {
        "authoritative_decision": "keep_regime_permission_gate" if keep else "drop_no_regime_permission_gate_edge",
        "candidate_local_decision": best,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "single-axis market_regime_daily permission gate on refined EntryReady; costs and borrow ignored by user request",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "refined_entry_ready": {
                "range_40_20_min": ENTRY_READY_RANGE_40_20_MIN,
                "last_vol_ratio_max": ENTRY_READY_LAST_VOL_RATIO_MAX,
                "dist_prior_80_high_min": ENTRY_READY_DIST_HIGH_MIN,
            },
            "regime_source": "market_regime_daily joined on signal_ymd=dt",
            "portfolio": {"top_k": TOP_K, "cooldown_days": COOLDOWN_DAYS, "rank_score": "volume_break"},
            "exit_policy": {"profit_target": PROFIT_TARGET, "stop_loss": STOP_LOSS},
            "changed_axis": "single market regime permission gate only",
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no entry geometry change",
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
            "regime_rows": int(len(regime)),
            "trade_rows": int(len(trades)),
            "selected_rows": int(len(selected)),
            "refined_rows": int(len(refined)),
            "joined_rows": int(len(joined)),
            "joined_with_regime_rows": int(len(joined_with_regime)),
        },
    )
    _write_json(run_dir / "regime_permission_leaderboard.json", {"rows": leaderboard})
    _write_jsonl(run_dir / "regime_permission_examples.jsonl", _examples(joined, best) if not joined.empty else [])
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
                "regime_permission_leaderboard.json",
                "regime_permission_examples.jsonl",
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
