from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "fresh_runtime_watch_validation_protocol_v1"
DEFAULT_SURFACE_ROOT = Path(r"G:\Tradex\fresh_runtime_candidate_surface_v1\20260525T143559Z-fresh-runtime-candidate-surface-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_runtime_watch_validation_protocol_v1")
WATCH_TOP_N = 20
REQUIRED_ARTIFACTS = (
    "fresh_watch_protocol_summary.json",
    "fresh_watch_rows.csv",
    "fresh_watch_contract.json",
    "validation_schedule.json",
    "future_outcome_join_contract.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
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


def load_surface(surface_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows_path = surface_root / "fresh_runtime_candidate_surface_rows.parquet"
    decision_path = surface_root / "research_decision.json"
    coverage_path = surface_root / "source_coverage.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    if not coverage_path.exists():
        raise FileNotFoundError(coverage_path)
    return pd.read_parquet(rows_path), _load_json(decision_path), _load_json(coverage_path)


def build_watch_rows(surface: pd.DataFrame, top_n: int = WATCH_TOP_N) -> pd.DataFrame:
    required = {
        "as_of_date",
        "code",
        "fresh_runtime_research_watch_score",
        "fresh_runtime_research_watch_rank",
        "fresh_runtime_research_watch_bucket",
        "fresh_runtime_live_feature_available_flag",
    }
    missing = sorted(required - set(surface.columns))
    if missing:
        raise ValueError(f"missing required fresh runtime surface columns: {missing}")
    watch = surface.sort_values(["fresh_runtime_research_watch_rank", "code"]).head(top_n).copy()
    watch["fresh_watch_protocol_rank"] = range(1, len(watch) + 1)
    watch["fresh_watch_status"] = "research_watch_pending_future_outcome"
    watch["buy_recommendation"] = False
    watch["validated_buy"] = False
    watch["active_gate_created"] = False
    watch["future_ret5_required_for_evaluation"] = True
    watch["future_ret20_required_for_evaluation"] = True
    return watch


def build_validation_schedule(watch_rows: pd.DataFrame) -> dict[str, Any]:
    dates = sorted(pd.to_numeric(watch_rows["as_of_date"], errors="coerce").dropna().astype(int).unique().tolist())
    as_of_date = dates[-1] if dates else None
    return {
        "axis_id": AXIS_ID,
        "as_of_date": as_of_date,
        "validation_windows": [
            {"window_id": "ret5_forward_evaluation", "outcome_column": "ret5", "required_future_confirmed_sessions": 5},
            {"window_id": "ret20_forward_evaluation", "outcome_column": "ret20", "required_future_confirmed_sessions": 20},
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
        "outcome_usage": "future_evaluation_only_after_fresh_watch_rows_are_frozen",
        "forbidden_live_feature_terms": ["ret5", "ret10", "ret20", "winner", "bad", "severe", "future_max", "future_min", "future_trigger"],
        "live_feature_construction_after_join_allowed": False,
        "research_watch_only": True,
    }


def no_lookahead_audit(watch_rows: pd.DataFrame, source_decision: dict[str, Any]) -> dict[str, Any]:
    forbidden = {"ret5", "ret10", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"}
    present = sorted(forbidden & set(watch_rows.columns))
    source_safe = bool(source_decision.get("research_watch_only") is True and source_decision.get("buyable_selection_ready") is False)
    passed = not present and source_safe
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "source_surface_research_watch_only": source_decision.get("research_watch_only"),
        "source_buyable_selection_ready": source_decision.get("buyable_selection_ready"),
        "offline_outcome_columns_present_in_watch_rows": present,
        "future_outcomes_used_for_selection": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(watch_rows: pd.DataFrame, audit: dict[str, Any], source_decision: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["fresh_watch_rows_or_source_contract_failed_no_lookahead"]
    if source_decision.get("research_decision") != "fresh_runtime_surface_ready_for_research_watch_pretest":
        return "blocked_missing_fresh_runtime_surface_contract", "BLOCKED", ["source_surface_not_ready_for_fresh_research_watch_pretest"]
    if watch_rows.empty:
        return "blocked_missing_fresh_runtime_watch_candidates", "BLOCKED", ["no_fresh_watch_rows_selected"]
    return "fresh_runtime_watch_protocol_ready_for_future_outcome_validation", "HOLD_UNDERPOWERED", [
        "fresh_runtime_watch_rows_frozen_for_future_ret5_ret20_evaluation",
        "not_buyable_until_future_outcomes_validate_selector",
    ]


def run(surface_root: Path = DEFAULT_SURFACE_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT, top_n: int = WATCH_TOP_N) -> Path:
    surface, source_decision, source_cov = load_surface(surface_root)
    watch_rows = build_watch_rows(surface, top_n=top_n)
    audit = no_lookahead_audit(watch_rows, source_decision)
    decision, decision_class, reasons = decide(watch_rows, audit, source_decision)

    out = output_root / f"{_now_tag()}-fresh-runtime-watch-validation-protocol-v1"
    out.mkdir(parents=True, exist_ok=True)
    keep_cols = [
        "as_of_date",
        "code",
        "fresh_runtime_research_watch_score",
        "fresh_runtime_research_watch_rank",
        "fresh_runtime_research_watch_bucket",
        "fresh_watch_protocol_rank",
        "fresh_watch_status",
        "buy_recommendation",
        "validated_buy",
        "active_gate_created",
        "future_ret5_required_for_evaluation",
        "future_ret20_required_for_evaluation",
    ]
    watch_rows[keep_cols].to_csv(out / "fresh_watch_rows.csv", index=False)
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "source_surface_root": str(surface_root),
        "watch_top_n": top_n,
        "watch_row_count": int(len(watch_rows)),
        "watch_date_count": int(watch_rows["as_of_date"].nunique()) if not watch_rows.empty else 0,
        "watch_code_count": int(watch_rows["code"].nunique()) if not watch_rows.empty else 0,
        "top_watch_codes": watch_rows.sort_values("fresh_watch_protocol_rank")["code"].astype(str).head(10).tolist(),
        "research_watch_only": True,
        "buyable_selection_ready": False,
        "validated_buy_count": 0,
    }
    _write_json(out / "fresh_watch_protocol_summary.json", summary)
    _write_json(out / "fresh_watch_contract.json", {"axis_id": AXIS_ID, "watch_selection_rule": "freeze_top_n_by_fresh_runtime_research_watch_rank_from_authoritative_fresh_runtime_surface", "watch_top_n": top_n, "research_watch_only": True, "buy_signal": False, "validated_buy_claim": False, "active_gate_created": False, "production_candidate_generator_changed": False, "production_ranking_changed": False})
    _write_json(out / "validation_schedule.json", build_validation_schedule(watch_rows))
    _write_json(out / "future_outcome_join_contract.json", future_outcome_join_contract())
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_surface_root": str(surface_root), "source_row_count": int(len(surface)), "source_date_count": int(surface["as_of_date"].nunique()), "source_code_count": int(surface["code"].nunique()), "watch_row_count": int(len(watch_rows)), "watch_date_count": int(watch_rows["as_of_date"].nunique()), "watch_code_count": int(watch_rows["code"].nunique()), "source_latest_feature_snapshot_date": source_cov.get("latest_feature_snapshot_date"), "source_latest_daily_bar_date": source_cov.get("latest_daily_bar_date"), "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "source_surface_root": str(surface_root), "source_research_decision": source_decision, "source_coverage": source_cov})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "research_watch_only": True, "buyable_selection_ready": False, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--surface-root", type=Path, default=DEFAULT_SURFACE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--top-n", type=int, default=WATCH_TOP_N)
    args = parser.parse_args(argv)
    out = run(args.surface_root, args.output_root, args.top_n)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
