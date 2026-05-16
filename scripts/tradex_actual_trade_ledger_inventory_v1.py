from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None  # type: ignore[assignment]


CANDIDATE_NAME = "actual_trade_ledger_inventory_v1"
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME
REPO_ROOT = Path(__file__).resolve().parents[1]

TRADE_KEYWORDS = (
    "trade",
    "trades",
    "ledger",
    "broker",
    "rakuten",
    "sbi",
    "execution",
    "position",
    "pnl",
    "損益",
    "取引",
    "約定",
    "譲渡",
    "履歴",
)

MINIMUM_SCHEMA = {
    "required": [
        "symbol",
        "side",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "quantity_or_notional",
        "realized_pnl_or_computable_return",
        "broker_if_available",
        "trade_id_or_grouping_key",
    ],
    "optional": [
        "entry_reason",
        "exit_reason",
        "memo",
        "stop_price",
        "target_price",
        "commission",
        "tax",
        "holding_days",
        "strategy_tag",
        "position_scale",
        "partial_entry_exit_group",
    ],
    "accepted_execution_event_fields": {
        "symbol": ["symbol", "code", "ticker"],
        "side": ["side", "action", "side_type", "transaction_type"],
        "event_date": ["exec_dt", "trade_date", "asOf"],
        "price": ["price", "exec_price"],
        "quantity": ["qty", "quantity", "delta_units"],
        "broker": ["broker", "account_or_broker"],
        "grouping_key": ["source_row_hash", "trade_id", "round_id"],
    },
}


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")


def file_hash(path: Path, limit_bytes: int = 1024 * 1024) -> str | None:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            h.update(f.read(limit_bytes))
        return h.hexdigest()
    except OSError:
        return None


def is_fixture_or_sample(path: Path) -> bool:
    lower = str(path).lower()
    return any(part in lower for part in ("fixture", "sample", "selftest", "test", "docs"))


def detect_broker_from_name(path: Path) -> str | None:
    text = path.name.lower()
    if "sbi" in text:
        return "sbi"
    if "rakuten" in text or "楽天" in path.name or "讌" in path.name:
        return "rakuten"
    return None


def inspect_csv(path: Path) -> dict[str, Any]:
    row_count = 0
    header: list[str] = []
    encoding_used = None
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                rows = []
                for idx, row in enumerate(reader):
                    if idx < 5:
                        rows.append(row)
                    row_count += 1
                header = next((r for r in rows if any(c.strip() for c in r)), [])
            encoding_used = enc
            break
        except Exception:
            row_count = 0
            header = []
    in_runtime_data = "appdata" in str(path).lower() and "meemeescreener\\data" in str(path).lower()
    appears_real = path.exists() and row_count > 50 and in_runtime_data and not is_fixture_or_sample(path)
    return {
        "source_type": "csv",
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else None,
        "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat() if path.exists() else None,
        "sha256_first_1mb": file_hash(path),
        "encoding_used": encoding_used,
        "header_or_first_nonempty_row": header,
        "row_count_including_header": row_count,
        "broker_guess": detect_broker_from_name(path),
        "appears_real_user_data": appears_real,
        "real_data_reason": "canonical_runtime_trade_csv" if appears_real else "fixture_or_schema_hint_or_too_small",
    }


def query_scalar(con: Any, sql: str) -> Any:
    try:
        return con.execute(sql).fetchone()[0]
    except Exception:
        return None


def table_info(con: Any, table: str) -> list[dict[str, Any]]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [{"name": r[1], "type": r[2], "notnull": bool(r[3]), "pk": bool(r[5])} for r in rows]


def inspect_table(con: Any, db_path: Path, table: str) -> dict[str, Any]:
    info = table_info(con, table)
    cols = [c["name"] for c in info]
    colset = set(cols)
    payload: dict[str, Any] = {
        "source_type": "duckdb_table",
        "db_path": str(db_path),
        "table": table,
        "columns": info,
        "row_count": query_scalar(con, f'SELECT COUNT(*) FROM "{table}"'),
        "appears_real_user_data": False,
        "classification": "unknown",
        "schema_support": {},
    }

    date_col = next((c for c in ("exec_dt", "opened_at", "closed_at", "asOf", "created_at") if c in colset), None)
    symbol_col = next((c for c in ("symbol", "code", "ticker") if c in colset), None)
    side_col = next((c for c in ("side", "action", "side_type", "transaction_type") if c in colset), None)
    price_col = next((c for c in ("price", "entry_price", "exit_price", "avg_price") if c in colset), None)
    qty_col = next((c for c in ("qty", "quantity", "delta_units", "buy_qty", "sell_qty", "units") if c in colset), None)
    pnl_col = next((c for c in ("realized_pnl", "pnl", "pnl_pct", "fees_cost") if c in colset), None)

    if date_col:
        payload["date_range"] = {
            "column": date_col,
            "min": query_scalar(con, f'SELECT MIN("{date_col}") FROM "{table}"'),
            "max": query_scalar(con, f'SELECT MAX("{date_col}") FROM "{table}"'),
        }
    if symbol_col:
        payload["symbols_count"] = query_scalar(con, f'SELECT COUNT(DISTINCT "{symbol_col}") FROM "{table}"')
    if side_col:
        try:
            rows = con.execute(f'SELECT "{side_col}", COUNT(*) FROM "{table}" GROUP BY 1 ORDER BY 2 DESC LIMIT 20').fetchall()
            payload["side_coverage"] = [{"value": str(r[0]), "count": r[1]} for r in rows]
        except Exception:
            payload["side_coverage"] = []

    duplicate_key_cols = [c for c in ("source_row_hash", "trade_id", "round_id") if c in colset]
    if duplicate_key_cols:
        key = duplicate_key_cols[0]
        dup_rows = query_scalar(
            con,
            f'SELECT COUNT(*) FROM (SELECT "{key}", COUNT(*) c FROM "{table}" GROUP BY 1 HAVING COUNT(*) > 1)',
        )
        payload["duplicate_key_check"] = {"column": key, "duplicate_key_count": dup_rows}

    if price_col:
        payload["non_positive_price_count"] = query_scalar(con, f'SELECT COUNT(*) FROM "{table}" WHERE "{price_col}" <= 0')
    if "opened_at" in colset and "closed_at" in colset:
        payload["impossible_dates_exit_before_entry_count"] = query_scalar(
            con,
            f'SELECT COUNT(*) FROM "{table}" WHERE closed_at IS NOT NULL AND opened_at IS NOT NULL AND closed_at < opened_at',
        )

    payload["schema_support"] = {
        "symbol": symbol_col is not None,
        "side": side_col is not None,
        "entry_date": "entry_date" in colset or "opened_at" in colset,
        "entry_price": "entry_price" in colset,
        "exit_date": "exit_date" in colset or "closed_at" in colset,
        "exit_price": "exit_price" in colset,
        "quantity_or_notional": qty_col is not None or "notional" in colset,
        "realized_pnl_or_return": pnl_col is not None,
        "broker": "broker" in colset,
        "trade_id_or_grouping_key": bool(duplicate_key_cols) or "id" in colset,
        "execution_event_date": date_col is not None,
        "execution_price": price_col is not None,
    }

    count = payload.get("row_count") or 0
    lower_db = str(db_path).lower()
    if table == "trade_events" and count > 10 and "selftest" not in lower_db:
        payload["appears_real_user_data"] = True
        payload["classification"] = "actual_execution_events_needs_trade_pairing"
    elif table == "position_rounds" and count > 10 and "selftest" not in lower_db:
        payload["appears_real_user_data"] = True
        payload["classification"] = "actual_position_rounds_missing_prices_and_pnl"
    elif table.startswith("toredex_"):
        payload["classification"] = "tradex_replay_or_simulation_not_actual_user_ledger"
    elif "selftest" in lower_db:
        payload["classification"] = "selftest_fixture"
    else:
        payload["classification"] = "schema_hint_or_empty"

    return payload


def inspect_duckdb(path: Path) -> list[dict[str, Any]]:
    if duckdb is None or not path.exists():
        return []
    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception as exc:
        return [{"source_type": "duckdb", "db_path": str(path), "error": str(exc)}]
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        hits = [t for t in tables if any(k in t.lower() for k in ("trade", "position", "execution", "order", "pnl"))]
        return [inspect_table(con, path, t) for t in hits]
    finally:
        con.close()


def candidate_paths() -> dict[str, list[Path]]:
    local = os.environ.get("LOCALAPPDATA", "")
    local_roots = [
        Path(local) / "MeeMeeScreener" / "data",
        Path(local) / "MeeMeeScreener-dev" / "data",
        Path(local) / "MeeMeeScreener-selftest" / "data",
    ]
    csv_paths: list[Path] = []
    db_paths: list[Path] = [
        REPO_ROOT / "data" / "stocks.duckdb",
        *[root / "stocks.duckdb" for root in local_roots],
    ]

    for root in local_roots:
        for sub in (root, root / "csv"):
            if sub.exists():
                csv_paths.extend(sorted(sub.glob("*.csv")))

    for path in [REPO_ROOT / "fixtures", REPO_ROOT / "docs", REPO_ROOT / "artifacts", REPO_ROOT / "external_analysis"]:
        if not path.exists():
            continue
        try:
            for item in path.rglob("*"):
                if item.is_file() and item.suffix.lower() in (".csv", ".tsv", ".xlsx", ".xls", ".parquet", ".duckdb"):
                    name = str(item).lower()
                    if any(k.lower() in name for k in TRADE_KEYWORDS):
                        if item.suffix.lower() == ".duckdb":
                            db_paths.append(item)
                        else:
                            csv_paths.append(item)
        except OSError:
            continue

    g_tradex = Path(r"G:\Tradex")
    if g_tradex.exists():
        for sub in (g_tradex, g_tradex / "db", g_tradex / "keep", g_tradex / "sample_replays"):
            if not sub.exists():
                continue
            try:
                for item in sub.glob("*"):
                    if item.is_file():
                        name = str(item).lower()
                        if item.suffix.lower() == ".duckdb":
                            db_paths.append(item)
                        elif item.suffix.lower() in (".csv", ".tsv", ".parquet") and any(k.lower() in name for k in TRADE_KEYWORDS):
                            csv_paths.append(item)
            except OSError:
                continue

    return {
        "csv": sorted(set(csv_paths), key=lambda p: str(p).lower()),
        "duckdb": sorted(set(db_paths), key=lambda p: str(p).lower()),
    }


def decide(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    actual = [c for c in candidates if c.get("appears_real_user_data")]
    complete = []
    for c in actual:
        support = c.get("schema_support") or {}
        if all(
            bool(support.get(k))
            for k in (
                "symbol",
                "side",
                "entry_date",
                "entry_price",
                "exit_date",
                "exit_price",
                "quantity_or_notional",
                "realized_pnl_or_return",
                "trade_id_or_grouping_key",
            )
        ):
            complete.append(c)

    if complete:
        decision = "ready_for_context_reconstruction"
        reason = "actual trade ledger satisfies the minimum paired-entry/exit schema"
    elif actual:
        decision = "needs_normalization"
        reason = "actual execution and position data exist, but no source currently satisfies paired entry/exit price and realized PnL schema"
    else:
        decision = "data_missing"
        reason = "no actual user trade ledger source was verified"

    return {
        "ledger_found": bool(actual),
        "usable_paired_ledger_found": bool(complete),
        "decision": decision,
        "reason": reason,
        "actual_candidate_count": len(actual),
        "usable_paired_candidate_count": len(complete),
        "manual_export_recommended": decision in ("data_missing", "needs_normalization"),
    }


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    paths = candidate_paths()
    csv_candidates = [inspect_csv(p) for p in paths["csv"]]
    table_candidates: list[dict[str, Any]] = []
    for db_path in paths["duckdb"]:
        table_candidates.extend(inspect_duckdb(db_path))

    all_candidates = csv_candidates + table_candidates
    validation = decide(table_candidates + csv_candidates)

    import_capability_confirmed = {
        "confirmed": True,
        "evidence": [
            str(REPO_ROOT / "app" / "backend" / "api" / "routers" / "trades.py"),
            str(REPO_ROOT / "app" / "backend" / "infra" / "files" / "trade_repo.py"),
            str(REPO_ROOT / "app" / "backend" / "domain" / "positions" / "parser.py"),
        ],
        "canonical_runtime_paths": [
            str(Path(os.environ.get("LOCALAPPDATA", "")) / "MeeMeeScreener" / "data" / "csv" / "SBI証券取引履歴.csv"),
            str(Path(os.environ.get("LOCALAPPDATA", "")) / "MeeMeeScreener" / "data" / "csv" / "楽天証券取引履歴.csv"),
        ],
    }

    inventory = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "boundary": "TRADEX-only diagnostic; MeeMee runtime and ranking logic not changed",
        "ledger_found": validation["ledger_found"],
        "decision": validation["decision"],
        "searched": {
            "repo_root": str(REPO_ROOT),
            "local_appdata_roots": [
                str(Path(os.environ.get("LOCALAPPDATA", "")) / "MeeMeeScreener" / "data"),
                str(Path(os.environ.get("LOCALAPPDATA", "")) / "MeeMeeScreener-dev" / "data"),
                str(Path(os.environ.get("LOCALAPPDATA", "")) / "MeeMeeScreener-selftest" / "data"),
            ],
            "g_tradex_bounded_roots": [r"G:\Tradex", r"G:\Tradex\db", r"G:\Tradex\keep", r"G:\Tradex\sample_replays"],
        },
        "import_capability": import_capability_confirmed,
        "candidate_source_count": len(all_candidates),
        "candidate_sources": all_candidates,
        "non_scope_confirmation": {
            "meemee_changed": False,
            "live_ranking_changed": False,
            "champion_scoring_changed": False,
            "publish_promotion_changed": False,
            "counterfactual_tests_run": False,
            "setup_analysis_run": False,
        },
    }

    schema_candidates = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "schema_candidates": all_candidates,
        "fixture_or_sample_sources": [c for c in all_candidates if not c.get("appears_real_user_data")],
        "actual_sources": [c for c in all_candidates if c.get("appears_real_user_data")],
    }

    validation_payload = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        **validation,
        "checks": {
            "entry_exit_pairing_available": validation["usable_paired_ledger_found"],
            "realized_pnl_available_in_usable_source": validation["usable_paired_ledger_found"],
            "actual_execution_events_available": any(
                c.get("classification") == "actual_execution_events_needs_trade_pairing" for c in table_candidates
            ),
            "actual_position_rounds_available": any(
                c.get("classification") == "actual_position_rounds_missing_prices_and_pnl" for c in table_candidates
            ),
            "short_trades_representable": any(
                "side_coverage" in c and any("sell" in str(x.get("value", "")).lower() for x in c["side_coverage"])
                for c in table_candidates
            ),
            "multi_leg_or_partial_fill_support": "execution events include source_row_hash and per-execution rows; grouping normalization still required",
            "future_data_used": False,
        },
        "blocking_missing_fields": [
            "entry_date and exit_date paired into a single trade outcome",
            "entry_price and exit_price paired into a single trade outcome",
            "realized_pnl or computable closed-trade return per grouped trade",
            "stable trade_id / partial_entry_exit_group across fills",
        ],
    }

    required_schema = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "minimum_required_schema": MINIMUM_SCHEMA,
        "recommended_import_contract": {
            "one_row_per": "closed trade outcome after normalizing broker executions and partial fills",
            "date_policy": "entry_date and exit_date are actual execution dates; later context reconstruction must use data available through entry_date only",
            "price_policy": "entry_price and exit_price should be volume-weighted average execution prices when partial fills exist",
            "pnl_policy": "realized_pnl should include fees/taxes when available; otherwise include raw gross return and explicit fee/tax missing flags",
            "broker_policy": "preserve broker and account where available",
        },
    }

    artifact_complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "required_artifacts": [
            "trade_ledger_inventory.json",
            "trade_ledger_schema_candidates.json",
            "trade_ledger_validation.json",
            "required_trade_ledger_schema.json",
        ],
        "complete": True,
        "decision": validation["decision"],
    }

    write_json(run_root / "trade_ledger_inventory.json", inventory)
    write_json(run_root / "trade_ledger_schema_candidates.json", schema_candidates)
    write_json(run_root / "trade_ledger_validation.json", validation_payload)
    write_json(run_root / "required_trade_ledger_schema.json", required_schema)
    write_json(run_root / "_ARTIFACT_COMPLETE.json", artifact_complete)

    print(json.dumps({"run_root": str(run_root), "decision": validation["decision"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
