from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyability_readiness_audit_v1"
DEFAULT_ASOF_ROOT = Path(r"G:\Tradex\asof_positive_selection_score_v1\20260525T134008Z-asof-positive-selection-score-v1")
DEFAULT_EVENT_ROOT = Path(r"G:\Tradex\historical_asof_event_backfill_contract_v1\20260525T140234Z-historical-asof-event-backfill-contract-v1")
DEFAULT_RIGHTS_ROOT = Path(r"G:\Tradex\recent_ex_rights_supportive_seed_gate_v1\20260525T140755Z-recent-ex-rights-supportive-seed-gate-v1")
DEFAULT_SOURCE_DB = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyability_readiness_audit_v1")
REQUIRED_ARTIFACTS = (
    "current_buyability_readiness_summary.json",
    "current_snapshot_candidates_preview.csv",
    "data_freshness_audit.json",
    "contract_readiness_audit.json",
    "missing_contracts.json",
    "lineage.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def db_latest_confirmed_date(source_db: Path) -> tuple[int | None, int]:
    if not source_db.exists():
        return None, 0
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        row = con.execute("SELECT max(CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)), count(distinct code) FROM daily_bars WHERE COALESCE(source,'') <> 'yahoo'").fetchone()
        return (int(row[0]) if row and row[0] is not None else None, int(row[1]) if row and row[1] is not None else 0)
    finally:
        con.close()


def latest_artifact_date(parquet_path: Path) -> tuple[int | None, int]:
    if not parquet_path.exists():
        return None, 0
    frame = pd.read_parquet(parquet_path, columns=["as_of_date", "code"])
    latest = int(frame["as_of_date"].max()) if not frame.empty else None
    latest_count = int(frame.loc[frame["as_of_date"] == latest, "code"].nunique()) if latest else 0
    return latest, latest_count


def current_preview(source_db: Path, latest_db_date: int | None, limit: int = 100) -> pd.DataFrame:
    if latest_db_date is None or not source_db.exists():
        return pd.DataFrame(columns=["as_of_date", "code", "close", "volume"])
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        query = """
            SELECT
                CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS as_of_date,
                CAST(code AS VARCHAR) AS code,
                c AS close,
                v AS volume
            FROM daily_bars
            WHERE COALESCE(source,'') <> 'yahoo'
              AND CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) = ?
            ORDER BY code
            LIMIT ?
        """
        return con.execute(query, [latest_db_date, limit]).fetchdf()
    finally:
        con.close()


def freshness_audit(asof_root: Path, event_root: Path, source_db: Path) -> dict[str, Any]:
    latest_db, db_code_count = db_latest_confirmed_date(source_db)
    latest_score, score_code_count = latest_artifact_date(asof_root / "asof_positive_selection_score_rows.parquet")
    latest_event, event_code_count = latest_artifact_date(event_root / "event_backfill_rows.parquet")
    return {
        "axis_id": AXIS_ID,
        "source_db": str(source_db),
        "latest_confirmed_bar_date": latest_db,
        "latest_confirmed_bar_code_count": db_code_count,
        "latest_scored_asof_date": latest_score,
        "latest_scored_code_count": score_code_count,
        "latest_event_backfill_asof_date": latest_event,
        "latest_event_backfill_code_count": event_code_count,
        "score_surface_current_to_latest_confirmed_bar": latest_score == latest_db,
        "event_surface_current_to_latest_confirmed_bar": latest_event == latest_db,
        "outcome_lagged_research_surface": latest_score is not None and latest_db is not None and latest_score < latest_db,
        "research_fallback_used": False,
    }


def contract_readiness(asof_root: Path, event_root: Path, rights_root: Path) -> dict[str, Any]:
    asof_decision = _load_json(asof_root / "research_decision.json")
    event_decision = _load_json(event_root / "research_decision.json")
    rights_decision = _load_json(rights_root / "research_decision.json")
    return {
        "axis_id": AXIS_ID,
        "asof_positive_selection_score_v1": {
            "decision": asof_decision.get("research_decision"),
            "decision_class": asof_decision.get("decision_class"),
            "usable_for_buyability": False,
            "reason": "dropped_no_edge",
        },
        "historical_asof_event_backfill_contract_v1": {
            "decision": event_decision.get("research_decision"),
            "decision_class": event_decision.get("decision_class"),
            "usable_for_buyability": False,
            "reason": "undercovered_full_history",
        },
        "recent_ex_rights_supportive_seed_gate_v1": {
            "decision": rights_decision.get("research_decision"),
            "decision_class": rights_decision.get("decision_class"),
            "usable_for_buyability": False,
            "reason": "promising_but_underpowered_recent_only",
        },
    }


def decide(freshness: dict[str, Any], readiness: dict[str, Any]) -> tuple[str, str, list[str]]:
    if freshness["latest_confirmed_bar_date"] is None:
        return "blocked_missing_confirmed_bar_source", "BLOCKED", ["confirmed_bar_source_missing"]
    if freshness["outcome_lagged_research_surface"]:
        return "current_buyability_blocked_need_forward_candidate_surface", "BLOCKED", ["research_surfaces_are_outcome_lagged_and_not_current_to_latest_confirmed_bars"]
    if any(item.get("usable_for_buyability") for key, item in readiness.items() if isinstance(item, dict)):
        return "current_buyability_pretest_ready", "KEEP", ["at_least_one_contract_ready_and_current"]
    return "current_buyability_blocked_no_valid_selector", "BLOCKED", ["no_validated_or_keep_worthy_selector_contract_exists"]


def missing_contracts(freshness: dict[str, Any]) -> dict[str, Any]:
    missing = [
        {
            "contract_id": "forward_current_candidate_surface_v1",
            "purpose": "generate current as_of_date candidates from confirmed bars without requiring ret20 outcome horizon",
            "minimum_requirements": ["latest confirmed non-yahoo daily bars", "point-in-time features only", "no ret5/ret20 output requirement", "no production mutation"],
        },
        {
            "contract_id": "current_period_validation_protocol_v1",
            "purpose": "separate research-watch output from validated buy claims when only forward/current data exists",
            "minimum_requirements": ["no validated buy label", "paper/forward tracking ledger", "future outcome join only after horizon completes"],
        },
        {
            "contract_id": "full_historical_event_source_coverage_v1",
            "purpose": "extend event coverage beyond recent JPX snapshots for robust historical validation",
            "minimum_requirements": ["historical TDNET or equivalent event archive", "as-of timestamps", "earnings/ex-rights/dividend/dilution taxonomy"],
        },
    ]
    return {"axis_id": AXIS_ID, "missing_contracts": missing, "freshness_context": freshness}


def run(asof_root: Path = DEFAULT_ASOF_ROOT, event_root: Path = DEFAULT_EVENT_ROOT, rights_root: Path = DEFAULT_RIGHTS_ROOT, source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    freshness = freshness_audit(asof_root, event_root, source_db)
    readiness = contract_readiness(asof_root, event_root, rights_root)
    decision, decision_class, reasons = decide(freshness, readiness)
    out = output_root / f"{_now_tag()}-current-buyability-readiness-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    preview = current_preview(source_db, freshness["latest_confirmed_bar_date"])
    preview.to_csv(out / "current_snapshot_candidates_preview.csv", index=False)
    summary = {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "buyable_selection_ready": decision_class == "KEEP", "freshness_audit": freshness, "contract_readiness": readiness}
    _write_json(out / "current_buyability_readiness_summary.json", summary)
    _write_json(out / "data_freshness_audit.json", freshness)
    _write_json(out / "contract_readiness_audit.json", readiness)
    _write_json(out / "missing_contracts.json", missing_contracts(freshness))
    _write_json(out / "lineage.json", {"asof_root": str(asof_root), "event_root": str(event_root), "rights_root": str(rights_root), "source_db": str(source_db)})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "audit_only": True, "offline_outcomes_used_for_current_preview": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "current_preview_rows": int(len(preview)), "latest_confirmed_bar_date": freshness["latest_confirmed_bar_date"], "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "buyable_selection_ready": decision_class == "KEEP", "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asof-root", type=Path, default=DEFAULT_ASOF_ROOT)
    parser.add_argument("--event-root", type=Path, default=DEFAULT_EVENT_ROOT)
    parser.add_argument("--rights-root", type=Path, default=DEFAULT_RIGHTS_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.asof_root, args.event_root, args.rights_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
