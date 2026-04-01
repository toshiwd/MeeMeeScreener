from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from app.backend.services.analysis.toredex_policy_diagnosis_service import run_toredex_policy_diagnosis


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ToreDex policy diagnosis variants for the current daily ranking contract.")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_toredex_policy_diagnosis(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
    )
    print(
        json.dumps(
            {
                "generated_at": datetime.now(tz=timezone.utc).isoformat(),
                "args": {
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                    "output_dir": args.output_dir,
                },
                "result": result,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
