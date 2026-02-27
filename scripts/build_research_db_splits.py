from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb


DATE_TABLE_COLUMNS: dict[str, str] = {
    "daily_bars": "date",
    "daily_ma": "date",
    "daily_memo": "date",
    "daily_memos": "date",
    "earnings_planned": "planned_date",
    "ex_rights": "ex_date",
    "feature_snapshot_daily": "dt",
    "label_20d": "dt",
    "ml_feature_daily": "dt",
    "ml_label_20d": "dt",
    "ml_monthly_label": "dt",
    "ml_monthly_pred": "dt",
    "ml_pred_20d": "dt",
    "phase_pred_daily": "dt",
    "sell_analysis_daily": "dt",
}

MONTH_TABLE_COLUMNS: dict[str, str] = {
    "monthly_bars": "month",
    "monthly_ma": "month",
}

STATIC_TABLES: tuple[str, ...] = (
    "industry_master",
    "stock_meta",
    "stock_scores",
    "tickers",
    "events_meta",
)


@dataclass(frozen=True)
class SplitRange:
    start_ymd: int
    end_ymd: int

    @property
    def label(self) -> str:
        return f"{self.start_ymd}_{self.end_ymd}"


def _parse_splits(raw: str) -> list[SplitRange]:
    parsed: list[SplitRange] = []
    for token in (part.strip() for part in raw.split(",")):
        if not token:
            continue
        if ":" not in token:
            raise ValueError(f"Invalid split token '{token}'. Expected START:END")
        start_text, end_text = (part.strip() for part in token.split(":", 1))
        if not (start_text.isdigit() and end_text.isdigit() and len(start_text) == 8 and len(end_text) == 8):
            raise ValueError(f"Invalid split token '{token}'. START/END must be YYYYMMDD")
        start = int(start_text)
        end = int(end_text)
        if start > end:
            raise ValueError(f"Invalid split token '{token}'. START must be <= END")
        parsed.append(SplitRange(start_ymd=start, end_ymd=end))
    if not parsed:
        raise ValueError("No split ranges specified")
    return parsed


def _resolve_source_db(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"source DB not found: {path}")
        return path
    env = os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path
    default = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    if default.exists():
        return default
    raise FileNotFoundError("Could not resolve source DB. Pass --source-db explicitly.")


def _normalize_ymd_expr(column: str) -> str:
    # Supports DATE, YYYYMMDD int, epoch seconds, epoch milliseconds.
    num = f"TRY_CAST({column} AS BIGINT)"
    dte = f"TRY_CAST({column} AS DATE)"
    return (
        "CASE "
        f"WHEN {dte} IS NOT NULL THEN CAST(strftime({dte}, '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 19000101 AND 20991231 THEN CAST({num} AS INTEGER) "
        f"WHEN {num} >= 1000000000000 THEN CAST(strftime(to_timestamp({num} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp({num}), '%Y%m%d') AS INTEGER) "
        "ELSE NULL END"
    )


def _normalize_month_to_ymd_expr(column: str) -> str:
    # Supports YYYYMM int and date-like values.
    num = f"TRY_CAST({column} AS BIGINT)"
    dte = f"TRY_CAST({column} AS DATE)"
    return (
        "CASE "
        f"WHEN {dte} IS NOT NULL THEN CAST(strftime({dte}, '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 190001 AND 209912 THEN CAST({num} * 100 + 1 AS INTEGER) "
        f"WHEN {num} >= 1000000000000 THEN CAST(strftime(to_timestamp({num} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp({num}), '%Y%m%d') AS INTEGER) "
        "ELSE NULL END"
    )


def _list_source_tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    return {str(row[0]) for row in rows}


def _build_target_db(
    *,
    source_db: Path,
    target_db: Path,
    split: SplitRange,
    include_static: bool,
    drop_existing: bool,
) -> dict[str, object]:
    if target_db.exists():
        if not drop_existing:
            raise FileExistsError(f"target DB already exists: {target_db}")
        target_db.unlink()

    target_db.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(target_db))
    try:
        conn.execute(f"ATTACH '{source_db.as_posix()}' AS src (READ_ONLY)")
        src_tables = _list_source_tables(conn)

        copied_counts: dict[str, int] = {}

        for table, column in DATE_TABLE_COLUMNS.items():
            if table not in src_tables:
                continue
            ymd_expr = _normalize_ymd_expr(column)
            conn.execute(
                f"""
                CREATE TABLE "{table}" AS
                SELECT *
                FROM src."{table}"
                WHERE {ymd_expr} BETWEEN ? AND ?
                """,
                [split.start_ymd, split.end_ymd],
            )
            count = int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            copied_counts[table] = count

        for table, column in MONTH_TABLE_COLUMNS.items():
            if table not in src_tables:
                continue
            ymd_expr = _normalize_month_to_ymd_expr(column)
            conn.execute(
                f"""
                CREATE TABLE "{table}" AS
                SELECT *
                FROM src."{table}"
                WHERE {ymd_expr} BETWEEN ? AND ?
                """,
                [split.start_ymd, split.end_ymd],
            )
            count = int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            copied_counts[table] = count

        if include_static:
            for table in STATIC_TABLES:
                if table not in src_tables:
                    continue
                conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM src."{table}"')
                count = int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
                copied_counts[table] = count

        meta = {
            "source_db": str(source_db),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "start_ymd": split.start_ymd,
            "end_ymd": split.end_ymd,
            "tables": copied_counts,
        }
        conn.execute(
            """
            CREATE TABLE research_split_meta (
                source_db VARCHAR,
                created_at_utc VARCHAR,
                start_ymd INTEGER,
                end_ymd INTEGER,
                tables_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO research_split_meta VALUES (?, ?, ?, ?, ?)
            """,
            [
                meta["source_db"],
                meta["created_at_utc"],
                meta["start_ymd"],
                meta["end_ymd"],
                json.dumps(meta["tables"], ensure_ascii=False),
            ],
        )
        conn.execute("CHECKPOINT")
        return meta
    finally:
        conn.close()


def _build_manifest(path: Path, entries: Iterable[dict[str, object]]) -> None:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "splits": list(entries),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build research-only split DuckDB files for long-horizon validation."
    )
    parser.add_argument(
        "--source-db",
        default="",
        help="Source stocks.duckdb path. Default: STOCKS_DB_PATH or %LOCALAPPDATA%/MeeMeeScreener/data/stocks.duckdb",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(".local") / "meemee" / "research_db"),
        help="Output directory for split DB files",
    )
    parser.add_argument(
        "--splits",
        default="20160226:20191231,20200101:20221231,20230101:20260226",
        help="Comma-separated START:END ranges in YYYYMMDD",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Overwrite existing split DB files",
    )
    parser.add_argument(
        "--no-static",
        action="store_true",
        help="Do not copy static metadata tables",
    )
    args = parser.parse_args()

    source_db = _resolve_source_db(args.source_db or None)
    output_dir = Path(args.output_dir).expanduser().resolve()
    splits = _parse_splits(args.splits)

    entries: list[dict[str, object]] = []
    for split in splits:
        target = output_dir / f"stocks_research_{split.label}.duckdb"
        meta = _build_target_db(
            source_db=source_db,
            target_db=target,
            split=split,
            include_static=not args.no_static,
            drop_existing=bool(args.drop_existing),
        )
        entry = {
            "file": str(target),
            "start_ymd": split.start_ymd,
            "end_ymd": split.end_ymd,
            "tables": meta["tables"],
        }
        entries.append(entry)
        print(f"[OK] {target.name} tables={len(meta['tables'])}")

    manifest = output_dir / "manifest.json"
    _build_manifest(manifest, entries)
    print(f"[DONE] manifest: {manifest}")


if __name__ == "__main__":
    main()
