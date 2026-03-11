from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from app.backend.services import strategy_backtest_service


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build lookup-based daily router candidates from strategy conditional stats.")
    parser.add_argument("--start-dt", type=int, default=None)
    parser.add_argument("--end-dt", type=int, default=None)
    parser.add_argument("--max-codes", type=int, default=500)
    parser.add_argument("--horizon", type=int, default=20)
    parser.add_argument("--label-version", default="v1")
    parser.add_argument("--scope-key", default=None)
    parser.add_argument("--top-n-per-day", type=int, default=25)
    parser.add_argument("--min-pattern-support", type=int, default=40)
    parser.add_argument("--min-router-score", type=float, default=-0.25)
    parser.add_argument("--candidate-long-score-min", type=float, default=2.0)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = strategy_backtest_service.build_router_daily_candidates(
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        max_codes=args.max_codes,
        horizon=args.horizon,
        label_version=str(args.label_version),
        scope_key=args.scope_key,
        top_n_per_day=args.top_n_per_day,
        min_pattern_support=args.min_pattern_support,
        min_router_score=args.min_router_score,
        candidate_long_score_min=args.candidate_long_score_min,
    )
    payload = {
        "built_at": datetime.now(tz=timezone.utc).isoformat(),
        "args": {
            "start_dt": args.start_dt,
            "end_dt": args.end_dt,
            "max_codes": args.max_codes,
            "horizon": args.horizon,
            "label_version": str(args.label_version),
            "scope_key": args.scope_key,
            "top_n_per_day": args.top_n_per_day,
            "min_pattern_support": args.min_pattern_support,
            "min_router_score": args.min_router_score,
            "candidate_long_score_min": args.candidate_long_score_min,
        },
        "result": result,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
