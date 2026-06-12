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

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from scripts.tradex_pre_crash_short_exit_profit_take_v1 import (
    DIST_HIGH_MAX,
    RANGE_20_MIN,
    RANGE_40_MIN,
    _feature_payload,
    _is_gated_event,
    _json_ready,
    _load_daily,
    _write_json,
    _write_jsonl,
)
from scripts.tradex_pre_crash_short_rank_score_branch_v1 import _score_volume_break
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape
from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_short_review_board_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_review_board_v1")
DEFAULT_RECENT_SESSIONS = 20
DEFAULT_LIMIT = 20
PROFIT_TARGET = 0.20
STOP_LOSS = 0.08
RANK_SCORE_ID = "volume_break"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _latest_dates(daily: pd.DataFrame, recent_sessions: int) -> set[int]:
    dates = sorted(int(value) for value in daily["ymd"].dropna().unique().tolist())
    return set(dates[-recent_sessions:])


def _float_or_none(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _candidate_row(code: str, current: pd.Series, features: dict[str, float | None], pattern: str) -> dict[str, Any]:
    close = float(current["c"])
    high = float(current["h"])
    ma5 = _float_or_none(current.get("ma5"))
    entry_reference = close
    target_price = entry_reference * (1.0 - PROFIT_TARGET)
    stop_price = entry_reference * (1.0 + STOP_LOSS)
    return {
        "code": str(code),
        "signal_ymd": int(current["ymd"]),
        "pattern": pattern,
        "review_state": "WatchShortReview",
        "rank_score_id": RANK_SCORE_ID,
        "rank_score": float(_score_volume_break(features)),
        "entry_reference": "signal_close_reference_next_open_required",
        "signal_close": close,
        "profit_take_pct": PROFIT_TARGET,
        "stop_loss_pct": STOP_LOSS,
        "target_price_from_signal_close": target_price,
        "stop_price_from_signal_close": stop_price,
        "invalidation": {
            "signal_high_reclaim_close_above": high,
            "ma5_reclaim_close_above": ma5,
            "large_bullish_denial": "green candle with upper-range close and large body after entry",
        },
        "why_short_candidate": [
            "typical_pre_crash_shape_pattern",
            f"range20_ge_{RANGE_20_MIN}",
            f"range40_ge_{RANGE_40_MIN}",
            f"dist_prior_80_high_le_{DIST_HIGH_MAX}",
            "volume_break_rank_score_review_only",
        ],
        "cost_slippage": "ignored_by_user_request",
        "borrow_lending": "ignored_by_user_request",
        **features,
    }


def _build_board(daily: pd.DataFrame, recent_sessions: int, limit: int) -> list[dict[str, Any]]:
    recent_dates = _latest_dates(daily, recent_sessions)
    rows: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = _add_shape_features(group)
        for idx in range(140, len(g) - 1):
            current = g.iloc[idx]
            if int(current["ymd"]) not in recent_dates:
                continue
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if not _is_gated_event(features, pattern):
                continue
            rows.append(_candidate_row(str(code), current, features, pattern))
    rows.sort(key=lambda row: (int(row["signal_ymd"]), float(row["rank_score"])), reverse=True)
    return rows[:limit]


def run(db_path: Path, output_root: Path, code_limit: int | None, recent_sessions: int, limit: int) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness_down = get_rankings_freshness(
        tf="D",
        which="latest",
        direction="down",
        mode="trade",
        risk_mode="balanced",
        limit=max(limit, 20),
    )
    daily = _load_daily(db_path, code_limit)
    board = _build_board(daily, recent_sessions, limit)
    decision = {
        "authoritative_decision": "ready_review_only_board" if board else "ready_empty_review_only_board",
        "candidate_local_decision": {
            "board_candidate_count": len(board),
            "review_state": "WatchShortReview",
            "profit_take_pct": PROFIT_TARGET,
            "stop_loss_pct": STOP_LOSS,
            "rank_score_id": RANK_SCORE_ID,
        },
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "review-only short board generated from fixed research conditions; no validated live short claim",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "review_board_materialization",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "entry_population": f"typical pattern plus range20>={RANGE_20_MIN} range40>={RANGE_40_MIN} dist_prior_80_high<={DIST_HIGH_MAX}",
            "recent_sessions": recent_sessions,
            "limit": limit,
            "rank_score_id": RANK_SCORE_ID,
            "profit_take_pct": PROFIT_TARGET,
            "stop_loss_pct": STOP_LOSS,
            "cost_slippage": "ignored_by_user_request",
            "borrow_lending": "ignored_by_user_request",
            "no_lookahead": "board features use signal-day and prior rolling features only",
        },
        "source_freshness": {
            "runtime_status": runtime_status,
            "rankings_freshness_down": rankings_freshness_down,
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no cost/slippage/borrow/lending evaluation",
            "no validated live short claim",
            "no order sizing",
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
            "raw_rows": int(len(daily)),
            "recent_sessions": recent_sessions,
            "limit": limit,
        },
    )
    _write_json(run_dir / "short_review_board.json", {"candidates": board})
    _write_jsonl(run_dir / "short_review_board.jsonl", board)
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
                "short_review_board.json",
                "short_review_board.jsonl",
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
    parser.add_argument("--recent-sessions", type=int, default=DEFAULT_RECENT_SESSIONS)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.code_limit, args.recent_sessions, args.limit))


if __name__ == "__main__":
    main()
