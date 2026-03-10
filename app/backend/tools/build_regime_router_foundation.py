from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from app.backend.services import strategy_backtest_service


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build market regime / future pattern foundation tables.")
    parser.add_argument("--start-dt", type=int, default=None)
    parser.add_argument("--end-dt", type=int, default=None)
    parser.add_argument("--label-version", default="v1")
    parser.add_argument("--horizon", type=int, default=20)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    regime_result = strategy_backtest_service.build_market_regime_daily(
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        label_version=str(args.label_version),
    )
    future_result = strategy_backtest_service.build_future_pattern_daily(
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        horizon=int(args.horizon),
        label_version=str(args.label_version),
    )
    summary = strategy_backtest_service.get_regime_router_foundation_summary(
        label_version=str(args.label_version),
        horizon=int(args.horizon),
    )
    payload = {
        "built_at": datetime.now(tz=timezone.utc).isoformat(),
        "args": {
            "start_dt": args.start_dt,
            "end_dt": args.end_dt,
            "label_version": str(args.label_version),
            "horizon": int(args.horizon),
        },
        "market_regime_daily": regime_result,
        "future_pattern_daily": future_result,
        "summary": summary,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
