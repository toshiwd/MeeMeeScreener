from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path

from app.backend.edinetdb.config import load_config, mask_api_key
from app.backend.edinetdb.jobs import run_backfill_700, run_daily_watch
from app.backend.edinetdb.merge import merge_edinetdb_tables, merge_raw_dirs
from app.backend.edinetdb.schema import ensure_edinetdb_schema_at_path


def _print_summary(summary: dict) -> None:
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edinetdb")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("backfill_700")
    subparsers.add_parser("daily_watch")
    merge = subparsers.add_parser("merge_from")
    merge.add_argument("--src-db", required=True)
    merge.add_argument("--src-raw", default=None)
    merge.add_argument("--merge-raw", action="store_true")
    args = parser.parse_args(argv)

    cfg = load_config()
    ensure_edinetdb_schema_at_path(cfg.db_path)
    key_preview = ",".join(mask_api_key(key) for key in cfg.api_keys[:2])
    if len(cfg.api_keys) > 2:
        key_preview = f"{key_preview},..."
    print(
        f"[edinetdb] command={args.command} "
        f"db={cfg.db_path} raw={cfg.raw_dir} budget={cfg.daily_budget} "
        f"api_keys={len(cfg.api_keys)}[{key_preview}]"
    )

    if args.command in {"backfill_700", "daily_watch"} and not cfg.api_keys:
        print("[edinetdb] skip: EDINETDB_API_KEY(S) is not set")
        return 0

    try:
        if args.command == "backfill_700":
            summary = run_backfill_700(cfg)
        elif args.command == "daily_watch":
            summary = run_daily_watch(cfg)
        else:
            src_db = Path(args.src_db).expanduser().resolve()
            merged = merge_edinetdb_tables(dst_db_path=cfg.db_path, src_db_path=src_db)
            summary = {
                "job": "merge_from",
                "dst_db": str(cfg.db_path),
                "src_db": str(src_db),
                "merged": merged,
            }
            if args.merge_raw:
                src_raw = Path(args.src_raw).expanduser().resolve() if args.src_raw else cfg.raw_dir
                raw = merge_raw_dirs(dst_raw_dir=cfg.raw_dir, src_raw_dir=src_raw)
                summary["raw"] = raw
        _print_summary(summary)
        return 0
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
