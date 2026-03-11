from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.backend.tdnetdb.repository import TdnetdbRepository
from app.core.config import config


def main() -> int:
    parser = argparse.ArgumentParser(description="Import TDNET disclosure JSON into DuckDB.")
    parser.add_argument("json_path", help="Path to a JSON file containing an array or {items:[...]} payload.")
    parser.add_argument("--db", dest="db_path", default=str(config.DB_PATH), help="DuckDB path")
    args = parser.parse_args()

    json_path = Path(args.json_path).expanduser().resolve()
    with open(json_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise SystemExit("items must be a list or wrapped in {\"items\": [...]}")

    repo = TdnetdbRepository(args.db_path)
    saved = repo.upsert_disclosures([item for item in items if isinstance(item, dict)])
    print(json.dumps({"ok": True, "saved": saved, "db": str(Path(args.db_path).resolve())}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
