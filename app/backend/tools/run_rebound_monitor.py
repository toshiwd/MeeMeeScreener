from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from external_analysis.event_image_dataset.analysis import build_event_image_rebound_live_monitor


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TRADEX rebound_onset live ranking delta monitor.")
    parser.add_argument("--dataset-id", default="monthly-event-meemee-registered-sample100-v12")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--output-dir", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = build_event_image_rebound_live_monitor(
        dataset_id=str(args.dataset_id),
        days=int(args.days),
        output_root=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
    )
    print(
        json.dumps(
            {
                "generated_at": datetime.now(tz=timezone.utc).isoformat(),
                "args": {
                    "dataset_id": args.dataset_id,
                    "days": int(args.days),
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
