from __future__ import annotations

import argparse
import json
import sys

from app.backend.services.toredex_replay import replay_decision
from app.backend.services.toredex_runner import run_live


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="toredex", description="TOREDEX CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    live = sub.add_parser("run-live", help="Run TOREDEX live paper for one asOf")
    live.add_argument("--season-id", required=True)
    live.add_argument("--asof", default=None)
    live.add_argument("--dry-run", action="store_true")

    replay = sub.add_parser("replay", help="Replay decision hash from saved snapshot")
    replay.add_argument("--season-id", required=True)
    replay.add_argument("--asof", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "run-live":
            result = run_live(
                season_id=str(args.season_id),
                as_of=str(args.asof) if args.asof is not None else None,
                dry_run=bool(args.dry_run),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "replay":
            result = replay_decision(
                season_id=str(args.season_id),
                as_of=str(args.asof),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        parser.print_help()
        return 2
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
