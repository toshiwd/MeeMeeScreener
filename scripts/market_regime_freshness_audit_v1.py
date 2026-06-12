from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.current_short_regime_permission_board_v1 import run as run_permission_board
from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path, resolve_runtime_stock_db_selection
from shared.tradex_storage import tradex_db_path


ARTIFACT_ROOT = Path(r"G:\Tradex\market_regime_freshness_audit_v1")
PRIOR_CURRENT_BOARD_ARTIFACT = Path(
    r"G:\Tradex\current_short_regime_permission_board_v1"
    r"\20260604T130213Z-current-short-regime-permission-board-v1"
    r"\current_short_regime_permission_board.json"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _table_names(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    return {str(row[0]) for row in rows}


def _dt_expr(conn: duckdb.DuckDBPyConnection, table_name: str) -> str | None:
    cols = {str(row[0]) for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
    if "dt" in cols:
        return "dt"
    if "date" in cols:
        return """
            CASE
                WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                ELSE NULL
            END
        """
    return None


def _source_candidates(extra_paths: list[Path]) -> list[Path]:
    local = Path.home() / "AppData" / "Local"
    paths: list[Path] = []
    try:
        paths.append(resolve_runtime_stock_db_path())
    except Exception:
        pass
    paths.extend(
        [
            local / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
            local / "MeeMeeScreener" / "data" / "stocks.duckdb",
            Path("data") / "stocks.duckdb",
            Path("app") / "backend" / "stocks.duckdb",
            tradex_db_path("stocks.duckdb"),
        ]
    )
    paths.extend(extra_paths)
    for root in [
        Path(r"G:\Tradex\db"),
        Path(r"G:\Tradex\scratch\source_snapshots"),
    ]:
        if root.exists():
            try:
                paths.extend(sorted(root.glob("*.duckdb"), key=lambda path: path.stat().st_mtime, reverse=True))
            except OSError:
                pass
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        try:
            normalized = str(path.expanduser().resolve(strict=False))
        except Exception:
            normalized = str(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(Path(normalized))
    return out


def inspect_source(path: Path, signal_ymds: set[int]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "source_path": str(path),
        "exists": path.exists(),
        "market_regime_daily_exists": False,
        "row_count": None,
        "min_dt": None,
        "max_dt": None,
        "daily_bars_exists": False,
        "daily_bars_row_count": None,
        "daily_bars_min_dt": None,
        "daily_bars_max_dt": None,
        "current_board_min_signal_ymd": min(signal_ymds) if signal_ymds else None,
        "current_board_max_signal_ymd": max(signal_ymds) if signal_ymds else None,
        "matched_signal_ymd_count": 0,
        "missing_signal_ymd_count": len(signal_ymds),
        "usable_for_current_board": False,
        "error": None,
    }
    if not path.exists():
        return result
    try:
        with duckdb.connect(str(path), read_only=True) as conn:
            tables = _table_names(conn)
            result["daily_bars_exists"] = "daily_bars" in tables
            if "daily_bars" in tables:
                expr = _dt_expr(conn, "daily_bars")
                if expr is not None:
                    row = conn.execute(f"SELECT count(*), min(x.dt), max(x.dt) FROM (SELECT {expr} AS dt FROM daily_bars) x").fetchone()
                    result["daily_bars_row_count"] = int(row[0] or 0) if row else 0
                    result["daily_bars_min_dt"] = int(row[1]) if row and row[1] is not None else None
                    result["daily_bars_max_dt"] = int(row[2]) if row and row[2] is not None else None
            if "market_regime_daily" not in tables:
                return result
            result["market_regime_daily_exists"] = True
            cols = {str(row[0]) for row in conn.execute("DESCRIBE market_regime_daily").fetchall()}
            if "dt" not in cols:
                result["error"] = "market_regime_daily_missing_dt_column"
                return result
            row = conn.execute("SELECT count(*), min(dt), max(dt) FROM market_regime_daily").fetchone()
            result["row_count"] = int(row[0] or 0) if row else 0
            result["min_dt"] = int(row[1]) if row and row[1] is not None else None
            result["max_dt"] = int(row[2]) if row and row[2] is not None else None
            if result["row_count"]:
                matched = conn.execute(
                    f"""
                    SELECT count(DISTINCT dt)
                    FROM market_regime_daily
                    WHERE dt IN ({", ".join("?" for _ in signal_ymds)})
                    """,
                    list(sorted(signal_ymds)),
                ).fetchone()[0]
                result["matched_signal_ymd_count"] = int(matched or 0)
                result["missing_signal_ymd_count"] = len(signal_ymds) - int(matched or 0)
                result["usable_for_current_board"] = int(matched or 0) == len(signal_ymds) and len(signal_ymds) > 0
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"
    return result


def producer_candidates() -> list[dict[str, Any]]:
    return [
        {
            "file_path": str(REPO_ROOT / "app/backend/services/analysis/strategy_backtest_service.py"),
            "function_or_script_name": "build_market_regime_daily",
            "confirmed_behavior": (
                "Creates market_regime_daily schema; computes breadth/advancers/index/volatility metrics "
                "from daily_bars; deletes requested dt range; inserts rows for computed dt range."
            ),
            "unresolved_questions": [
                "Which operational job is expected to call it for 2026-05/2026-06 current runtime data.",
                "Whether latest daily_bars in the selected runtime DB are available past 20260402.",
            ],
        },
        {
            "file_path": str(REPO_ROOT / "app/backend/tools/build_regime_router_foundation.py"),
            "function_or_script_name": "build_regime_router_foundation.py",
            "confirmed_behavior": "CLI wrapper calls build_market_regime_daily(start_dt, end_dt, label_version) and build_future_pattern_daily.",
            "unresolved_questions": ["No evidence in this audit that this CLI was run after 20260402."],
        },
        {
            "file_path": str(REPO_ROOT / "app/backend/services/signal_tracking_service.py"),
            "function_or_script_name": "_load_market_regime_lookup",
            "confirmed_behavior": "If queried regime rows are missing, attempts to materialize build_market_regime_daily for the requested range.",
            "unresolved_questions": ["This path is tied to signal tracking validation, not confirmed as the current short board regime source."],
        },
    ]


def build_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Market Regime Freshness Audit v1",
        "",
        "## Blocker",
        "",
        "- Current short board classification was blocked because geometry-passing candidates had no matching market_regime_daily rows.",
        f"- prior_current_board_artifact: `{payload['prior_current_board_artifact_path']}`",
        "",
        "## Source Inspection",
        "",
        "| source | exists | regime rows | min dt | max dt | matched | missing | usable |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in payload["inspected_sources"]:
        lines.append(
            f"| `{item['source_path']}` | {item['exists']} | {item['row_count']} | "
            f"{item['min_dt']} | {item['max_dt']} | {item['matched_signal_ymd_count']} | "
            f"{item['missing_signal_ymd_count']} | {item['usable_for_current_board']} |"
        )
    lines.extend(
        [
            "",
            "## Selected Source",
            "",
            f"- selected_regime_source: `{payload['selected_regime_source']}`",
            f"- fresher_valid_source_exists: {payload['selected_regime_source'] is not None}",
            f"- current_board_classification_rerun: {payload['rerun_classification_artifact_path'] is not None}",
        ]
    )
    if payload.get("rerun_counts"):
        counts = payload["rerun_counts"]
        lines.extend(
            [
                f"- PermitShort: {counts['permit_short_count']}",
                f"- BlockShort: {counts['block_short_count']}",
                f"- Avoid: {counts['avoid_count']}",
                f"- RegimeMissing: {counts['regime_missing_count']}",
            ]
        )
    else:
        lines.extend(
            [
                "- exact_blocker: No inspected source had market_regime_daily rows for every current board signal_ymd.",
                "- next_required_action: rebuild/update market_regime_daily as a separate task against the intended authoritative runtime source.",
            ]
        )
    lines.extend(
        [
            "",
            "## Producer Candidates",
            "",
        ]
    )
    for item in payload["producer_candidates"]:
        lines.append(f"- `{item['function_or_script_name']}` in `{item['file_path']}`: {item['confirmed_behavior']}")
    lines.extend(
        [
            "",
            "## Decision",
            "",
            f"- authoritative_decision: {payload['authoritative_decision']}",
            "- no_runtime_db_write: true",
            "- no_meemee_change: true",
            "- no_production_ranking_change: true",
        ]
    )
    return "\n".join(lines) + "\n"


def run(prior_current_board_artifact_path: Path, extra_paths: list[Path]) -> Path:
    created = _utc_now()
    run_id = f"{created.strftime('%Y%m%dT%H%M%SZ')}-market-regime-freshness-audit-v1"
    out_dir = ARTIFACT_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    prior = _read_json(prior_current_board_artifact_path)
    signal_ymds = {int(candidate["signal_ymd"]) for candidate in prior.get("candidates", []) if candidate.get("signal_ymd") is not None}
    inspected = [inspect_source(path, signal_ymds) for path in _source_candidates(extra_paths)]
    usable = [item for item in inspected if item.get("usable_for_current_board")]
    selected = usable[0]["source_path"] if usable else None
    rerun_path: str | None = None
    rerun_counts: dict[str, Any] | None = None
    if selected is not None:
        rerun_dir = run_permission_board(
            Path(prior["source_board_path"]),
            Path(prior["prior_gate_artifact_path"]),
            Path(selected),
        )
        rerun_json = _read_json(rerun_dir / "current_short_regime_permission_board.json")
        rerun_path = str(rerun_dir / "current_short_regime_permission_board.json")
        rerun_counts = rerun_json.get("counts")

    selection = {}
    try:
        selection = dict(resolve_runtime_stock_db_selection())
    except Exception as exc:
        selection = {"error": f"{type(exc).__name__}:{exc}"}

    payload = {
        "run_id": run_id,
        "created_at": created.isoformat(),
        "prior_current_board_artifact_path": str(prior_current_board_artifact_path),
        "current_board_signal_ymds": sorted(signal_ymds),
        "runtime_db_selection": selection,
        "inspected_sources": inspected,
        "producer_candidates": producer_candidates(),
        "selected_regime_source": selected,
        "rerun_classification_artifact_path": rerun_path,
        "rerun_counts": rerun_counts,
        "authoritative_decision": (
            "rerun_current_board_classification_with_fresher_regime_source"
            if selected is not None
            else "regime_source_stale_blocks_current_board_classification"
        ),
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    json_path = out_dir / "market_regime_freshness_audit.json"
    md_path = out_dir / "market_regime_freshness_audit_summary.md"
    marker_path = out_dir / "_ARTIFACT_COMPLETE.json"
    _write_json(json_path, payload)
    md_path.write_text(build_markdown(payload), encoding="utf-8")
    _write_json(
        marker_path,
        {
            "run_id": run_id,
            "complete": True,
            "json_path": str(json_path),
            "markdown_path": str(md_path),
            "created_at": created.isoformat(),
        },
    )
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prior-current-board-artifact-path", type=Path, default=PRIOR_CURRENT_BOARD_ARTIFACT)
    parser.add_argument("--extra-db-path", type=Path, action="append", default=[])
    args = parser.parse_args()
    print(run(args.prior_current_board_artifact_path, list(args.extra_db_path)))


if __name__ == "__main__":
    main()
