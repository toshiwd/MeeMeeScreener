from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from app.backend.infra.duckdb.stock_repo import StockRepository
from app.core.config import config as app_config
from external_analysis.contracts.paths import resolve_result_db_path


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _probe_duckdb(path: Path, table_name: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "table": table_name,
        "table_exists": False,
        "row_count": None,
    }
    if not path.exists():
        return result
    with duckdb.connect(str(path), read_only=True) as conn:
        tables = [row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name").fetchall()]
        result["table_exists"] = table_name in tables
        result["table_count"] = len(tables)
        if table_name in tables:
            result["row_count"] = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    return result


def collect_tradex_data_smoke() -> dict[str, Any]:
    db_path = Path(str(app_config.DB_PATH)).expanduser().resolve()
    repo = StockRepository(str(db_path))
    codes = [code for code in repo.get_all_codes() if str(code).strip()]
    source_probe = _probe_duckdb(db_path, "daily_bars")
    source_probe["distinct_code_count"] = len(codes)
    analysis_probe = _probe_duckdb(db_path, "market_regime_daily")

    result_db_path = resolve_result_db_path()
    regime_probe = _probe_duckdb(result_db_path, "regime_daily")

    return {
        "data_dir": str(app_config.DATA_DIR),
        "source_db": source_probe,
        "analysis_db": analysis_probe,
        "result_db": regime_probe,
        "stock_repo_initialized": True,
        "confirmed_universe_count": len(codes),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke check TRADEX confirmed data availability.")
    parser.add_argument("--json", action="store_true", help="Print JSON only.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = collect_tradex_data_smoke()
    text = json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
    print(text)
    return 0 if int(payload.get("confirmed_universe_count") or 0) > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
