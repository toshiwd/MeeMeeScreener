from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


AXIS_ID = "tradex_adaptive_rule_refresh_v1"
ROOT = Path(__file__).resolve().parents[1]
OUT = Path(r"G:\Tradex\adaptive_rule_refresh_v1")


def run_command(args: list[str]) -> dict:
    completed = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, encoding="utf-8", errors="replace")
    if completed.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(args)}\n{completed.stdout}\n{completed.stderr}")
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    return {"command": args, "returncode": completed.returncode, "last_output": lines[-1] if lines else None, "stdout": completed.stdout}


def run() -> Path:
    sys.path[:0] = [str(ROOT), str(ROOT / "app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = str(runtime["selected_runtime_db_path"])
    python = sys.executable
    steps = []

    steps.append(run_command([python, "scripts/tradex_shallow_high_zone_next_open_execution_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_long_short_weekly_coverage_v1.py"]))
    source = run_command([python, "scripts/tradex_pattern_family_source_rows_v1.py", "--db-path", db_path])
    steps.append(source)
    source_payload = json.loads(source["stdout"][source["stdout"].find("{"):])
    source_path = str(Path(source_payload["output_dir"]) / "pattern_family_source_rows.parquet")
    regime = run_command([python, "scripts/tradex_position_lifecycle_multiyear_momentum_regime_audit_v1.py", "--source-path", source_path])
    steps.append(regime)
    steps.append(run_command([python, "scripts/tradex_momentum_reentry_h10_union_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_adaptive_dormant_family_events_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_long_ma_weekly_reversal_axis_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_riskoff_capitulation_reversal_long_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_ma20_reclaim_family_events_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_shape_entry_current_board_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_2026_momentum_leader_reentry_current_board_v1.py", "--db-path", db_path]))
    steps.append(run_command([python, "scripts/tradex_unified_current_opportunity_board_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_adaptive_current_family_scan_v1.py"]))
    router = run_command([python, "scripts/tradex_adaptive_rule_router_v1.py"])
    steps.append(router)
    intraday = run_command([python, "scripts/tradex_intraday_short_preview_v1.py"])
    steps.append(intraday)
    steps.append(run_command([python, "scripts/tradex_short_climax_failure_events_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_short_climax_entry_timing_v1.py"]))
    steps.append(run_command([python, "scripts/tradex_short_failed_high_current_scan_v1.py"]))
    short_router = run_command([python, "scripts/tradex_adaptive_short_rule_router_v1.py"])
    steps.append(short_router)
    integrated = run_command([python, "scripts/tradex_integrated_entry_board_v1.py"])
    steps.append(integrated)
    stress = run_command([python, "scripts/tradex_integrated_router_no_momentum_audit_v1.py"])
    steps.append(stress)

    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    payload = {
        "schema_version": f"{AXIS_ID}.audit.v1", "artifact_role": "authoritative",
        "confirmed_date": runtime["latest_confirmed_daily_bars_date_iso"], "runtime_db": db_path,
        "step_count": len(steps), "steps": [{key: value for key, value in step.items() if key != "stdout"} for step in steps], "router_artifact": router["last_output"], "adaptive_short_router_artifact": short_router["last_output"], "intraday_short_artifact": intraday["last_output"], "integrated_entry_board_artifact": integrated["last_output"], "no_momentum_stress_artifact": stress["last_output"],
        "status": "pass", "purpose": "research and display refresh only",
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path = output / "refresh_audit.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
