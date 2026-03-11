from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config
from app.backend.services.rankings_cache import get_rankings_asof


def _collect_trading_dates(start_ymd: int, end_ymd: int) -> list[int]:
    with duckdb.connect(str(config.DB_PATH), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT
              CASE
                WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt/1000), '%Y%m%d') AS INTEGER)
                WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                ELSE NULL
              END AS ymd
            FROM ml_pred_20d
            WHERE ymd IS NOT NULL
              AND ymd BETWEEN ? AND ?
            ORDER BY ymd
            """,
            [int(start_ymd), int(end_ymd)],
        ).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def run(args: argparse.Namespace) -> dict:
    tf = args.tf.upper()
    which = args.which
    direction = args.dir
    mode = args.mode
    limit = int(args.limit)

    dates = _collect_trading_dates(args.start_ymd, args.end_ymd)
    traces: list[dict] = []
    zero_streak = 0
    max_zero_streak = 0
    last_non_zero_date = None

    for ymd in dates:
        out = get_rankings_asof(tf, which, direction, limit, as_of=ymd, mode=mode)
        items = out.get("items", [])
        qualified = [x for x in items if x.get("entryQualified") is True]
        q_count = len(qualified)
        if q_count == 0:
            zero_streak += 1
            max_zero_streak = max(max_zero_streak, zero_streak)
            continue
        zero_streak = 0
        last_non_zero_date = ymd
        traces.append(
            {
                "date": ymd,
                "qualified_count": q_count,
                "codes": [str(x.get("code")) for x in qualified],
                "top_details": [
                    {
                        "code": str(x.get("code")),
                        "entryScore": x.get("entryScore"),
                        "probSide": x.get("probSide"),
                        "ev20Net": x.get("mlEv20Net"),
                        "setupType": x.get("setupType"),
                    }
                    for x in qualified[:10]
                ],
            }
        )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "query": {
            "tf": tf,
            "which": which,
            "dir": direction,
            "mode": mode,
            "limit": limit,
            "start_ymd": args.start_ymd,
            "end_ymd": args.end_ymd,
        },
        "summary": {
            "days": len(dates),
            "qualified_days": len(traces),
            "zero_qualified_days": len(dates) - len(traces),
            "qualified_day_rate": (len(traces) / len(dates)) if dates else 0.0,
            "last_non_zero_date": last_non_zero_date,
            "max_zero_streak_days": max_zero_streak,
        },
        "recent_hits": traces[-args.recent_count :],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Trace last dates where ranking had entryQualified=true")
    parser.add_argument("--tf", default="D", help="D/W/M")
    parser.add_argument("--which", default="latest", choices=["latest", "prev"])
    parser.add_argument("--dir", default="up", choices=["up", "down"])
    parser.add_argument("--mode", default="hybrid", choices=["rule", "ml", "hybrid", "turn"])
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--start-ymd", type=int, default=20250101)
    parser.add_argument("--end-ymd", type=int, default=20260226)
    parser.add_argument("--recent-count", type=int, default=10)
    parser.add_argument("--output", default="tmp/ranking_trace_last_qualified.json")
    args = parser.parse_args()

    result = run(args)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "output": str(out_path),
                "summary": result["summary"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
