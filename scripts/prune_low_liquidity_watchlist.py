from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from update_delisted_codes import delete_codes_from_db, remove_codes_from_code_list


def _epoch(ymd: str) -> int:
    return int(datetime.strptime(ymd, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def find_low_liquidity_codes(
    db_path: Path,
    active_codes: list[str],
    *,
    as_of: str,
    sessions: int,
    minimum_average_shares: int,
) -> list[dict]:
    if not active_codes:
        return []
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
WITH ranked AS (
  SELECT code, date, v,
         ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
  FROM daily_bars
  WHERE source = 'pan'
    AND date <= ?
    AND code IN (SELECT UNNEST(?))
), averaged AS (
  SELECT code, COUNT(*) AS session_count, AVG(v) * 100.0 AS average_shares,
         MIN(date) AS first_date, MAX(date) AS last_date
  FROM ranked
  WHERE rn <= ?
  GROUP BY code
)
SELECT code, session_count, average_shares, first_date, last_date
FROM averaged
WHERE session_count = ? AND average_shares < ?
ORDER BY average_shares, code
""",
            [_epoch(as_of), active_codes, sessions, sessions, minimum_average_shares],
        ).fetchall()
    return [
        {
            "code": str(code),
            "session_count": int(count),
            "average_shares": int(round(float(average))),
            "first_date_epoch": int(first_date),
            "last_date_epoch": int(last_date),
        }
        for code, count, average, first_date, last_date in rows
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--code-list-path", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--sessions", type=int, default=5)
    parser.add_argument("--minimum-average-shares", type=int, default=100_000)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    active_codes = [line.strip() for line in args.code_list_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows = find_low_liquidity_codes(
        args.db_path,
        active_codes,
        as_of=args.as_of,
        sessions=max(1, args.sessions),
        minimum_average_shares=max(0, args.minimum_average_shares),
    )
    codes = {row["code"] for row in rows}
    code_list_result = remove_codes_from_code_list(args.code_list_path, codes, dry_run=args.dry_run)
    db_result = delete_codes_from_db(args.db_path, codes, dry_run=args.dry_run)
    report = {
        "schema_version": "meemee_low_liquidity_prune_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "boundary_owner": "MeeMee",
        "as_of": args.as_of,
        "sessions": args.sessions,
        "minimum_average_shares": args.minimum_average_shares,
        "volume_storage_unit_shares": 100,
        "dry_run": args.dry_run,
        "selected_count": len(rows),
        "selected": rows,
        "code_list_result": code_list_result,
        "db_result": db_result,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "selected_count": len(rows), "dry_run": args.dry_run}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
