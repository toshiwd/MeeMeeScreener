from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from app.backend.services import strategy_backtest_service


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build strategy conditional stats from regime / future pattern labels.")
    parser.add_argument("--start-dt", type=int, default=None)
    parser.add_argument("--end-dt", type=int, default=None)
    parser.add_argument("--max-codes", type=int, default=500)
    parser.add_argument("--horizon", type=int, default=20)
    parser.add_argument("--label-version", default="v1")
    parser.add_argument("--scope-key", default=None)
    parser.add_argument("--strategy-id", action="append", dest="strategy_ids", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = strategy_backtest_service.build_strategy_conditional_stats(
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        max_codes=args.max_codes,
        horizon=args.horizon,
        label_version=str(args.label_version),
        scope_key=args.scope_key,
        strategy_ids=list(args.strategy_ids or []),
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
            "strategy_ids": list(args.strategy_ids or []),
        },
        "result": result,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
