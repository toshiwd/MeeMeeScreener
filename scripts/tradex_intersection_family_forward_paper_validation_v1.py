from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_buyable_intersection_family_audit_v1 as family_audit


AXIS_ID = "intersection_family_forward_paper_validation_v1"
DEFAULT_SOURCE_DB = family_audit.DEFAULT_SOURCE_DB
DEFAULT_SUPPORT_ROOT = Path(r"G:\Tradex\buyable_intersection_family_support_gate_v1\20260526T004451Z-buyable-intersection-family-support-gate-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\intersection_family_forward_paper_validation_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "forward_paper_validation_summary.json",
    "forward_paper_candidate_rows.csv",
    "forward_paper_contract.json",
    "validation_schedule.json",
    "future_outcome_join_contract.json",
    "source_coverage.json",
    "no_lookahead_audit.json",
    "lineage.json",
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


def build_current_candidates(source_db: Path, date_count: int = DEFAULT_DATE_COUNT) -> pd.DataFrame:
    frame = family_audit.build_intersection_frame(source_db, date_count)
    candidates = frame[frame["variant_b_entry_qualified_top50"]].copy()
    if candidates.empty:
        return candidates
    latest = pd.to_numeric(candidates["as_of_date"], errors="coerce").max()
    current = candidates[pd.to_numeric(candidates["as_of_date"], errors="coerce") == latest].copy()
    current = current.sort_values(["fresh_runtime_research_watch_rank", "code"])
    current["forward_paper_rank"] = range(1, len(current) + 1)
    current["forward_paper_status"] = "research_paper_pending_future_outcome"
    current["buy_recommendation"] = False
    current["validated_buy"] = False
    current["active_gate_created"] = False
    return current


def validation_schedule(as_of_date: int | None) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "as_of_date": as_of_date,
        "validation_windows": [
            {"window_id": "ret5_forward_paper_check", "outcome_column": "ret5", "required_future_confirmed_sessions": 5},
            {"window_id": "ret20_forward_paper_check", "outcome_column": "ret20", "required_future_confirmed_sessions": 20},
        ],
        "no_current_buy_claim": True,
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def future_outcome_join_contract() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "join_keys": ["as_of_date", "code"],
        "allowed_future_outcome_columns": ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"],
        "outcome_usage": "future_paper_evaluation_only_after_candidates_are_frozen",
        "live_feature_construction_after_join_allowed": False,
        "research_watch_only": True,
    }


def no_lookahead_audit(rows: pd.DataFrame, support_decision: dict[str, Any]) -> dict[str, Any]:
    forbidden = {"ret5", "ret10", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"}
    present = sorted(forbidden & set(rows.columns))
    support_ok = support_decision.get("research_decision") == "intersection_family_ready_for_forward_paper_validation"
    passed = bool(support_ok and not present)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "support_gate_ready": support_ok,
        "offline_outcome_columns_present_in_candidate_rows": present,
        "future_outcomes_used_for_selection": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(rows: pd.DataFrame, audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["candidate_freeze_failed_no_lookahead_or_support_contract"]
    if rows.empty:
        return "blocked_no_current_intersection_candidates", "BLOCKED", ["no_current_candidates_for_passing_intersection_family"]
    return "intersection_family_forward_paper_candidates_frozen", "KEEP", ["current_candidates_frozen_for_forward_paper_validation"]


def run(source_db: Path = DEFAULT_SOURCE_DB, support_root: Path = DEFAULT_SUPPORT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    support_decision = _load_json(support_root / "research_decision.json")
    support_summary = _load_json(support_root / "support_gate_summary.json")
    rows = build_current_candidates(source_db, date_count)
    keep_cols = [
        "as_of_date",
        "code",
        "fresh_runtime_research_watch_rank",
        "fresh_runtime_research_watch_score",
        "buy_entry_qualified",
        "buy_breakout_surface",
        "variant_b_entry_qualified_top50",
        "forward_paper_rank",
        "forward_paper_status",
        "buy_recommendation",
        "validated_buy",
        "active_gate_created",
    ]
    candidate_rows = rows[keep_cols].copy() if not rows.empty else rows.reindex(columns=keep_cols)
    audit = no_lookahead_audit(candidate_rows, support_decision)
    decision, decision_class, reasons = decide(candidate_rows, audit)
    out = output_root / f"{_now_tag()}-intersection-family-forward-paper-validation-v1"
    out.mkdir(parents=True, exist_ok=True)

    candidate_rows.to_csv(out / "forward_paper_candidate_rows.csv", index=False)
    as_of_date = int(pd.to_numeric(rows["as_of_date"], errors="coerce").max()) if not rows.empty else None
    _write_json(out / "forward_paper_validation_summary.json", {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "support_root": str(support_root),
        "source_db": str(source_db),
        "as_of_date": as_of_date,
        "candidate_count": int(len(candidate_rows)),
        "candidate_codes": candidate_rows["code"].astype(str).tolist() if not candidate_rows.empty else [],
        "historical_support_metrics": support_summary.get("overall_metrics"),
        "buyable_selection_ready": decision_class == "KEEP",
        "validated_buy_count": 0,
        "research_watch_only": True,
    })
    _write_json(out / "forward_paper_contract.json", {
        "axis_id": AXIS_ID,
        "candidate_family": "variant_b_entry_qualified_top50",
        "candidate_rule": "buy_entry_qualified_true_and_fresh_runtime_research_watch_rank_lte_50",
        "buy_signal": False,
        "paper_validation_only": True,
        "validated_buy_claim": False,
        "production_candidate_generator_changed": False,
        "production_ranking_changed": False,
    })
    _write_json(out / "validation_schedule.json", validation_schedule(as_of_date))
    _write_json(out / "future_outcome_join_contract.json", future_outcome_join_contract())
    _write_json(out / "source_coverage.json", {
        "axis_id": AXIS_ID,
        "source_db": str(source_db),
        "support_root": str(support_root),
        "candidate_count": int(len(rows)),
        "candidate_date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0,
        "research_fallback_used": False,
    })
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "support_root": str(support_root), "support_decision": support_decision, "support_summary": support_summary})
    _write_json(out / "research_decision.json", {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "research_watch_only": True,
        "buyable_selection_ready": decision_class == "KEEP",
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
        "research_fallback_used": False,
    })
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--support-root", type=Path, default=DEFAULT_SUPPORT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--date-count", type=int, default=DEFAULT_DATE_COUNT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.support_root, args.output_root, args.date_count)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
