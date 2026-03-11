from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from app.backend.services.toredex_replay import replay_decision
from app.backend.services.toredex_runner import run_backtest, run_live
from app.backend.services.toredex_self_improve import run_self_improve, run_self_improve_loop


def _read_override(path: str | None, operating_mode: str | None) -> dict[str, object]:
    out: dict[str, object] = {}
    if path:
        p = Path(path)
        with open(p, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
            if isinstance(payload, dict):
                out.update(payload)
    if operating_mode:
        mode = str(operating_mode).strip().lower()
        if mode in {"champion", "challenger"}:
            out["operatingMode"] = mode
    return out


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="toredex", description="TOREDEX CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    live = sub.add_parser("run-live", help="Run TOREDEX live paper for one asOf")
    live.add_argument("--season-id", required=True)
    live.add_argument("--asof", default=None)
    live.add_argument("--dry-run", action="store_true")
    live.add_argument("--operating-mode", default=None, choices=["champion", "challenger"])
    live.add_argument("--config-override-json", default=None)

    backtest = sub.add_parser("run-backtest", help="Run TOREDEX backtest day-by-day")
    backtest.add_argument("--season-id", required=True)
    backtest.add_argument("--start-date", required=True)
    backtest.add_argument("--end-date", required=True)
    backtest.add_argument("--dry-run", action="store_true")
    backtest.add_argument("--operating-mode", default=None, choices=["champion", "challenger"])
    backtest.add_argument("--config-override-json", default=None)

    replay = sub.add_parser("replay", help="Replay decision hash from saved snapshot")
    replay.add_argument("--season-id", required=True)
    replay.add_argument("--asof", required=True)

    improve = sub.add_parser("self-improve", help="Run TOREDEX self-improvement multi-stage loop")
    improve.add_argument("--mode", default="challenger", choices=["champion", "challenger"])
    improve.add_argument("--iterations", type=int, default=None)
    improve.add_argument("--stage2-topk", type=int, default=None)
    improve.add_argument("--seed", type=int, default=None)
    improve.add_argument("--stage0-months", type=int, default=None)
    improve.add_argument("--stage1-months", type=int, default=None)
    improve.add_argument("--stage2-months", type=int, default=None)
    improve.add_argument("--parallel-workers", type=int, default=None)
    improve.add_argument("--parallel-db-path", action="append", default=None)

    improve_loop = sub.add_parser(
        "self-improve-loop",
        help="Run TOREDEX self-improvement repeatedly until target is reached or max cycles",
    )
    improve_loop.add_argument("--mode", default="challenger", choices=["champion", "challenger"])
    improve_loop.add_argument("--iterations", type=int, default=None)
    improve_loop.add_argument("--stage2-topk", type=int, default=None)
    improve_loop.add_argument("--seed", type=int, default=None)
    improve_loop.add_argument("--stage0-months", type=int, default=None)
    improve_loop.add_argument("--stage1-months", type=int, default=None)
    improve_loop.add_argument("--stage2-months", type=int, default=None)
    improve_loop.add_argument("--parallel-workers", type=int, default=None)
    improve_loop.add_argument("--parallel-db-path", action="append", default=None)
    improve_loop.add_argument("--max-cycles", type=int, default=None)
    improve_loop.add_argument("--target-net-return-pct", type=float, default=None)
    improve_loop.add_argument("--target-score-objective", type=float, default=None)
    improve_loop.add_argument("--require-stage2-pass", action=argparse.BooleanOptionalAction, default=None)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "run-live":
            config_override = _read_override(args.config_override_json, args.operating_mode)
            result = run_live(
                season_id=str(args.season_id),
                as_of=str(args.asof) if args.asof is not None else None,
                dry_run=bool(args.dry_run),
                config_override=config_override if config_override else None,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "run-backtest":
            config_override = _read_override(args.config_override_json, args.operating_mode)
            result = run_backtest(
                season_id=str(args.season_id),
                start_date=str(args.start_date),
                end_date=str(args.end_date),
                dry_run=bool(args.dry_run),
                config_override=config_override if config_override else None,
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

        if args.command == "self-improve":
            result = run_self_improve(
                mode=str(args.mode),
                iterations=int(args.iterations) if args.iterations is not None else None,
                stage2_topk=int(args.stage2_topk) if args.stage2_topk is not None else None,
                seed=int(args.seed) if args.seed is not None else None,
                stage0_months=int(args.stage0_months) if args.stage0_months is not None else None,
                stage1_months=int(args.stage1_months) if args.stage1_months is not None else None,
                stage2_months=int(args.stage2_months) if args.stage2_months is not None else None,
                parallel_workers=int(args.parallel_workers) if args.parallel_workers is not None else None,
                parallel_db_paths=list(args.parallel_db_path) if isinstance(args.parallel_db_path, list) else None,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "self-improve-loop":
            result = run_self_improve_loop(
                mode=str(args.mode),
                iterations=int(args.iterations) if args.iterations is not None else None,
                stage2_topk=int(args.stage2_topk) if args.stage2_topk is not None else None,
                seed=int(args.seed) if args.seed is not None else None,
                stage0_months=int(args.stage0_months) if args.stage0_months is not None else None,
                stage1_months=int(args.stage1_months) if args.stage1_months is not None else None,
                stage2_months=int(args.stage2_months) if args.stage2_months is not None else None,
                parallel_workers=int(args.parallel_workers) if args.parallel_workers is not None else None,
                parallel_db_paths=list(args.parallel_db_path) if isinstance(args.parallel_db_path, list) else None,
                max_cycles=int(args.max_cycles) if args.max_cycles is not None else None,
                target_net_return_pct=(
                    float(args.target_net_return_pct) if args.target_net_return_pct is not None else None
                ),
                target_score_objective=(
                    float(args.target_score_objective) if args.target_score_objective is not None else None
                ),
                require_stage2_pass=(
                    bool(args.require_stage2_pass) if args.require_stage2_pass is not None else None
                ),
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
