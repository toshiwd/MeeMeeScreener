from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from app.backend.services.analysis import ranking_backtest_service


def _parse_date(value: str | None) -> datetime.date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run MeeMee ranking backtests with baseline vs tradex-experiment comparison and ToreDex policy."
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = ranking_backtest_service.run_ranking_backtest(
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
    )
    payload = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "args": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "output_dir": args.output_dir,
        },
        "result": result,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
