from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.current_short_regime_permission_board_v1 import find_latest_source_board
from scripts.tradex_current_short_decision_support_board_v1 import run as run_current_board
from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "market_regime_artifact_rebuild_and_rerun_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\market_regime_artifact_rebuild_and_rerun_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _stat(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False}
    s = path.stat()
    return {"exists": True, "size": int(s.st_size), "mtime_ns": int(s.st_mtime_ns)}


def _signal_range(source_board_path: Path) -> tuple[int, int, list[int]]:
    payload = _read_json(source_board_path)
    dates = sorted({int(row["signal_ymd"]) for row in payload.get("candidates", []) if row.get("signal_ymd") is not None})
    if not dates:
        raise RuntimeError("source board contains no signal_ymd values")
    return dates[0], dates[-1], dates


def _regime_stats(db_path: Path, signal_dates: list[int]) -> dict[str, Any]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
        out: dict[str, Any] = {"market_regime_daily_exists": "market_regime_daily" in tables}
        if "market_regime_daily" not in tables:
            return out | {"row_count": 0, "min_dt": None, "max_dt": None, "matched_signal_ymd_count": 0}
        row = conn.execute("SELECT count(*), min(dt), max(dt) FROM market_regime_daily").fetchone()
        matched = conn.execute(
            f"""
            SELECT count(DISTINCT dt)
            FROM market_regime_daily
            WHERE dt IN ({", ".join("?" for _ in signal_dates)})
            """,
            signal_dates,
        ).fetchone()[0]
        sample = conn.execute(
            f"""
            SELECT dt, advancers_ratio, breadth_above_ma20, regime_score
            FROM market_regime_daily
            WHERE dt IN ({", ".join("?" for _ in signal_dates)})
            ORDER BY dt
            """,
            signal_dates,
        ).fetchall()
        return out | {
            "row_count": int(row[0] or 0) if row else 0,
            "min_dt": int(row[1]) if row and row[1] is not None else None,
            "max_dt": int(row[2]) if row and row[2] is not None else None,
            "matched_signal_ymd_count": int(matched or 0),
            "matched_rows": [
                {
                    "signal_ymd": int(item[0]),
                    "advancers_ratio": None if item[1] is None else float(item[1]),
                    "breadth_above_ma20": None if item[2] is None else float(item[2]),
                    "regime_score": None if item[3] is None else float(item[3]),
                }
                for item in sample
            ],
        }


def _build_regime_on_copy(copy_db_path: Path, start_dt: int, end_dt: int) -> dict[str, Any]:
    code = """
import json
from app.backend.services.analysis import strategy_backtest_service
result = strategy_backtest_service.build_market_regime_daily(
    start_dt=int(__import__('os').environ['REGIME_START_DT']),
    end_dt=int(__import__('os').environ['REGIME_END_DT']),
    label_version='v1',
)
print(json.dumps(result, ensure_ascii=False))
"""
    env = dict(os.environ)
    env["STOCKS_DB_PATH"] = str(copy_db_path)
    env["REGIME_START_DT"] = str(int(start_dt))
    env["REGIME_END_DT"] = str(int(end_dt))
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=180,
    )
    if proc.returncode != 0:
        return {
            "ok": False,
            "returncode": int(proc.returncode),
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    try:
        result = json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception:
        result = {"raw_stdout": proc.stdout}
    return {"ok": True, "returncode": 0, "result": result, "stderr": proc.stderr}


def _copy_runtime_db(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _markdown(payload: dict[str, Any]) -> str:
    rerun_counts = payload.get("rerun_board_counts") or {}
    lines = [
        "# Market Regime Artifact Rebuild And Rerun v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- source_runtime_db_path: `{payload['source_runtime_db_path']}`",
        f"- artifact_copy_db_path: `{payload['artifact_copy_db_path']}`",
        f"- source_board_path: `{payload['source_board_path']}`",
        f"- copied_db_runtime_unchanged: {payload['runtime_db_unchanged']}",
        "",
        "## Regime Rebuild",
        "",
        f"- requested_start_dt: {payload['requested_start_dt']}",
        f"- requested_end_dt: {payload['requested_end_dt']}",
        f"- before_max_dt: {payload['copy_regime_before'].get('max_dt')}",
        f"- after_max_dt: {payload['copy_regime_after'].get('max_dt')}",
        f"- matched_signal_ymd_count: {payload['copy_regime_after'].get('matched_signal_ymd_count')}",
        "",
        "## Rerun Board Counts",
        "",
        f"- rerun_board_path: `{payload.get('rerun_board_artifact_path')}`",
        f"- final_status_counts: `{(rerun_counts.get('final_status_counts') if rerun_counts else None)}`",
        f"- regime_permission_counts: `{(rerun_counts.get('regime_permission_counts') if rerun_counts else None)}`",
        "",
        "## Boundary",
        "",
        "- Runtime DB was not written.",
        "- MeeMee and production ranking were not modified.",
        "- Rebuild writes only the copied DuckDB inside this TRADEX artifact.",
    ]
    return "\n".join(lines) + "\n"


def run(runtime_db_path: Path, output_root: Path, source_board_path: Path) -> Path:
    run_dir = output_root / _run_id()
    run_dir.mkdir(parents=True, exist_ok=False)
    source_board_path = source_board_path.expanduser().resolve(strict=False)
    runtime_db_path = runtime_db_path.expanduser().resolve(strict=False)
    start_dt, end_dt, signal_dates = _signal_range(source_board_path)
    before_runtime = _stat(runtime_db_path)
    artifact_copy = run_dir / "stocks_regime_rebuild_copy.duckdb"
    _copy_runtime_db(runtime_db_path, artifact_copy)
    before_copy = _regime_stats(artifact_copy, signal_dates)
    build_result = _build_regime_on_copy(artifact_copy, start_dt, end_dt)
    after_copy = _regime_stats(artifact_copy, signal_dates)
    rerun_dir = run_current_board(
        runtime_db_path,
        Path(r"G:\Tradex\current_short_decision_support_board_v1"),
        source_board_path,
        artifact_copy,
    )
    rerun_json_path = rerun_dir / "current_short_decision_support_board.json"
    rerun_json = _read_json(rerun_json_path)
    after_runtime = _stat(runtime_db_path)
    runtime_unchanged = before_runtime == after_runtime
    decision = (
        "ready_current_board_rerun_with_artifact_regime_copy"
        if build_result.get("ok") and after_copy.get("matched_signal_ymd_count") == len(signal_dates)
        else "hold_regime_artifact_rebuild_incomplete"
    )
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_runtime_db_path": str(runtime_db_path),
        "artifact_copy_db_path": str(artifact_copy),
        "source_board_path": str(source_board_path),
        "requested_start_dt": int(start_dt),
        "requested_end_dt": int(end_dt),
        "signal_ymds": signal_dates,
        "runtime_db_stat_before": before_runtime,
        "runtime_db_stat_after": after_runtime,
        "runtime_db_unchanged": runtime_unchanged,
        "copy_regime_before": before_copy,
        "build_result": build_result,
        "copy_regime_after": after_copy,
        "rerun_board_artifact_path": str(rerun_json_path),
        "rerun_board_counts": rerun_json.get("counts"),
        "authoritative_decision": decision,
        "runtime_db_write": False,
        "artifact_copy_db_write": True,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "market_regime_artifact_rebuild_and_rerun.json", payload)
    (run_dir / "market_regime_artifact_rebuild_and_rerun_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "market_regime_artifact_rebuild_and_rerun.json",
                "market_regime_artifact_rebuild_and_rerun_summary.md",
                "stocks_regime_rebuild_copy.duckdb",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-board-path", type=Path, default=find_latest_source_board())
    args = parser.parse_args()
    print(run(args.runtime_db_path, args.output_root, args.source_board_path))


if __name__ == "__main__":
    main()
