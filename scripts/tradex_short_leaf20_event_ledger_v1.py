from __future__ import annotations

import hashlib
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_short_breadth_loss_cap_v1 import SPLITS
from scripts.tradex_short_support_break_breadth_gate_v1 import SQL
from scripts.tradex_short_support_break_exit_grid_v1 import DB_PATH, clean, simulate

AXIS_ID = "tradex_short_leaf20_event_ledger_v1"
OUT = Path(r"G:\Tradex\short_leaf20_event_ledger_v1")
SOURCE_FILES = (
    ROOT / "scripts" / "tradex_short_breadth_loss_cap_v1.py",
    ROOT / "scripts" / "tradex_short_leaf20_final_rollup_v1.py",
    ROOT / "scripts" / "tradex_short_support_break_breadth_gate_v1.py",
    ROOT / "scripts" / "tradex_short_support_break_exit_grid_v1.py",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def split_for_ymd(ymd: int) -> str | None:
    year = int(str(ymd)[:4])
    return next((name for name, span in SPLITS.items() if span[0] <= year <= span[1]), None)


def build_rows(raw: list[dict]) -> pd.DataFrame:
    rows = []
    for row in raw:
        if float(row["breadth_below_ma20"]) < 0.40:
            continue
        outcome = simulate(row, 0.10, 0.05, 10)
        rows.append(
            {
                "code": str(row["code"]),
                "signal_ymd": int(row["ymd"]),
                "entry_ymd": int(row["e_ymd"]),
                "split": split_for_ymd(int(row["ymd"])),
                "signal_low": float(row["l"]),
                "breadth_below_ma20": float(row["breadth_below_ma20"]),
                "ret": float(outcome["ret"]),
                "exit_reason": outcome["reason"],
                "holding_days": int(outcome["days"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["signal_ymd", "code"]).reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DB_PATH, help="Explicit read-only source DB or locked-DB snapshot")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    run = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    run.mkdir(parents=True)
    with duckdb.connect(str(args.db), read_only=True) as db:
        raw = [{k: clean(v) for k, v in row.items()} for row in db.execute(SQL).fetchdf().to_dict("records")]
    ledger = build_rows(raw)
    ledger_path = run / "event_ledger.parquet"
    ledger.to_parquet(ledger_path, index=False)
    manifest = {
        "schema_version": f"{AXIS_ID}.v1",
        "generated_at": now.isoformat(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "rule_contract": {
            "shape_and_gate": "support break capitulation; same-day breadth below MA20 >=40%",
            "entry": "next-day signal-low stop entry",
            "exit": "TP10%, SL5%, maximum 10 trading days, same-bar stop before target",
            "splits": SPLITS,
        },
        "future_leakage_control": "selection uses signal-day fields only; forward fields are used solely by the frozen exit simulator after entry trigger",
        "row_count": len(ledger),
        "rows_by_split": ledger["split"].value_counts(dropna=False).to_dict(),
        "event_ledger": str(ledger_path),
        "source_db": {"path": str(args.db), "sha256": sha256(args.db)},
        "source_files": [{"path": str(path), "sha256": sha256(path)} for path in SOURCE_FILES],
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    manifest_path = run / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
