from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from research.config import load_config
from research.evaluate import run_evaluate
from research.features import build_features_for_asof
from research.ingest import run_ingest
from research.labels import build_labels_for_asof
from research.loop import run_loop, run_loop_all
from research.publish import run_publish
from research.storage import ResearchPaths
from research.train import run_train


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="research",
        description="Isolated monthly Top20 research pipeline",
    )
    parser.add_argument("--workspace-root", default="research_workspace", help="Internal research workspace root")
    parser.add_argument("--published-root", default="published", help="Published snapshot root")
    parser.add_argument("--config", default=None, help="Config JSON path (default: research/default_config.json)")

    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest daily data and monthly universe into a snapshot")
    ingest.add_argument("--daily-csv", required=True)
    ingest.add_argument("--universe-dir", required=True)
    ingest.add_argument("--calendar-csv", default=None)
    ingest.add_argument("--snapshot-id", default=None)

    feat = sub.add_parser("build_features", help="Build monthly features for one asof date")
    feat.add_argument("--asof", required=True)
    feat.add_argument("--snapshot-id", default=None)
    feat.add_argument("--force", action="store_true")
    feat.add_argument("--workers", type=int, default=1)
    feat.add_argument("--chunk-size", type=int, default=120)

    lbl = sub.add_parser("build_labels", help="Build TP-based labels for one asof date")
    lbl.add_argument("--asof", required=True)
    lbl.add_argument("--snapshot-id", default=None)
    lbl.add_argument("--force", action="store_true")
    lbl.add_argument("--workers", type=int, default=1)
    lbl.add_argument("--chunk-size", type=int, default=120)

    train = sub.add_parser("train", help="Train candidate+rank model and produce Top20 inference")
    train.add_argument("--asof", required=True)
    train.add_argument("--run_id", required=True)
    train.add_argument("--snapshot-id", default=None)
    train.add_argument("--workers", type=int, default=1)
    train.add_argument("--chunk-size", type=int, default=120)

    evaluate = sub.add_parser("evaluate", help="Evaluate one run and compute Pareto position")
    evaluate.add_argument("--run_id", required=True)

    publish = sub.add_parser("publish", help="Publish run result into published_vNNN and latest")
    publish.add_argument("--run_id", required=True)
    publish.add_argument("--allow-non-pareto", action="store_true")
    publish.add_argument("--allow-quality-gate-fail", action="store_true")
    publish.add_argument("--publish-phases", default="test,inference")

    loop = sub.add_parser("loop", help="Run challenger loop (train+evaluate repeated)")
    loop.add_argument("--asof", required=True)
    loop.add_argument("--snapshot-id", default=None)
    loop.add_argument("--cycles", type=int, default=1)
    loop.add_argument("--workers", type=int, default=1)
    loop.add_argument("--chunk-size", type=int, default=120)

    loop_all = sub.add_parser("loop_all", help="Batch build features and labels for all months in a snapshot")
    loop_all.add_argument("--snapshot-id", default=None)
    loop_all.add_argument("--workers", type=int, default=1)
    loop_all.add_argument("--chunk-size", type=int, default=120)

    return parser


def _resolve_snapshot_id(paths: ResearchPaths, snapshot_id: str | None) -> str:
    if snapshot_id and snapshot_id.strip():
        return snapshot_id.strip()
    return paths.get_latest_snapshot_id()


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    paths = ResearchPaths.build(
        repo_root=repo_root,
        workspace_root=repo_root / str(args.workspace_root),
        published_root=repo_root / str(args.published_root),
    )

    try:
        if args.command == "ingest":
            result = run_ingest(
                paths=paths,
                daily_csv=str(args.daily_csv),
                universe_dir=str(args.universe_dir),
                calendar_csv=str(args.calendar_csv) if args.calendar_csv else None,
                snapshot_id=str(args.snapshot_id) if args.snapshot_id else None,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        config = load_config(args.config)

        if args.command == "build_features":
            snapshot_id = _resolve_snapshot_id(paths, args.snapshot_id)
            result = build_features_for_asof(
                paths=paths,
                config=config,
                snapshot_id=snapshot_id,
                asof_date=str(args.asof),
                force=bool(args.force),
                workers=int(args.workers),
                chunk_size=int(args.chunk_size),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "build_labels":
            snapshot_id = _resolve_snapshot_id(paths, args.snapshot_id)
            result = build_labels_for_asof(
                paths=paths,
                config=config,
                snapshot_id=snapshot_id,
                asof_date=str(args.asof),
                force=bool(args.force),
                workers=int(args.workers),
                chunk_size=int(args.chunk_size),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "train":
            snapshot_id = _resolve_snapshot_id(paths, args.snapshot_id)
            result = run_train(
                paths=paths,
                config=config,
                snapshot_id=snapshot_id,
                asof_date=str(args.asof),
                run_id=str(args.run_id),
                workers=int(args.workers),
                chunk_size=int(args.chunk_size),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "evaluate":
            result = run_evaluate(paths=paths, run_id=str(args.run_id))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "publish":
            raw_phases = str(args.publish_phases or "").strip()
            phases = tuple([p.strip() for p in raw_phases.split(",") if p.strip()])
            result = run_publish(
                paths=paths,
                run_id=str(args.run_id),
                allow_non_pareto=bool(args.allow_non_pareto),
                allow_quality_gate_fail=bool(args.allow_quality_gate_fail),
                publish_phases=phases if phases else ("test", "inference"),
                config=config,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "loop":
            snapshot_id = _resolve_snapshot_id(paths, args.snapshot_id)
            result = run_loop(
                paths=paths,
                config=config,
                snapshot_id=snapshot_id,
                asof_date=str(args.asof),
                cycles=int(args.cycles),
                workers=int(args.workers),
                chunk_size=int(args.chunk_size),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "loop_all":
            snapshot_id = _resolve_snapshot_id(paths, args.snapshot_id)
            result = run_loop_all(
                paths=paths,
                config=config,
                snapshot_id=snapshot_id,
                workers=int(args.workers),
                chunk_size=int(args.chunk_size),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        parser.print_help()
        return 2
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
