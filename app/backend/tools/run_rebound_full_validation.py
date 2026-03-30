from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from app.backend.services.analysis.rebound_full_validation_service import run_rebound_full_validation


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run serialized full validation for rebound_onset policy adoption.")
    parser.add_argument("--dataset-id", default="monthly-event-meemee-registered-sample100-v12")
    parser.add_argument("--monitor-days", type=int, default=60)
    parser.add_argument("--diagnosis-start-date", default=None)
    parser.add_argument("--diagnosis-end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_rebound_full_validation(
        dataset_id=str(args.dataset_id),
        monitor_days=int(args.monitor_days),
        diagnosis_start_date=args.diagnosis_start_date,
        diagnosis_end_date=args.diagnosis_end_date,
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
    )
    print(
        json.dumps(
            {
                "generated_at": datetime.now(tz=timezone.utc).isoformat(),
                "args": {
                    "dataset_id": args.dataset_id,
                    "monitor_days": int(args.monitor_days),
                    "diagnosis_start_date": args.diagnosis_start_date,
                    "diagnosis_end_date": args.diagnosis_end_date,
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
