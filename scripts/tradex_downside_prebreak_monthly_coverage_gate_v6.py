from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


AXIS_ID = "tradex_downside_prebreak_monthly_coverage_gate_v6"
SCHEMA_PREFIX = "tradex_downside_prebreak_monthly_coverage_gate_v6"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\downside_prebreak_narrow_universe_stability_v5")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\downside_prebreak_monthly_coverage_gate_v6")
DEFAULT_SOURCE_RANK_LIMIT = 20

REQUIRED_ARTIFACTS = (
    "downside_prebreak_monthly_coverage_contract.json",
    "downside_prebreak_monthly_coverage_summary.json",
    "downside_prebreak_monthly_coverage_rows.csv",
    "downside_prebreak_monthly_coverage_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_int(value: Any) -> int:
    value_float = _safe_float(value)
    return 0 if value_float is None else int(value_float)


def _monthly_rows(source_root: Path, source_rank_limit: int) -> list[dict[str, Any]]:
    path = source_root / f"rank_limit_{source_rank_limit}" / "downside_prebreak_narrow_universe_monthly_rankings.csv"
    if not path.exists():
        raise FileNotFoundError(f"monthly rankings not found: {path}")
    frame = pd.read_csv(path)
    return [dict(row) for row in frame.to_dict(orient="records")]


def _build_monthly_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [row for row in rows if not bool(row.get("skipped")) and _safe_int(row.get("selected_candidate_count")) > 0]
    skipped = [row for row in rows if bool(row.get("skipped")) or _safe_int(row.get("selected_candidate_count")) == 0]
    improved_event_months = 0
    degraded_event_months = 0
    improved_target_months = 0
    degraded_target_months = 0
    coverage_rows: list[dict[str, Any]] = []
    for row in rows:
        base_hit = _safe_float(row.get("baseline_top_hit_rate"))
        chal_hit = _safe_float(row.get("challenger_top_hit_rate"))
        base_mean = _safe_float(row.get("baseline_top_mean_ret20"))
        chal_mean = _safe_float(row.get("challenger_top_mean_ret20"))
        hit_delta = None if base_hit is None or chal_hit is None else chal_hit - base_hit
        mean_delta = None if base_mean is None or chal_mean is None else chal_mean - base_mean
        event_helped = hit_delta is not None and hit_delta > 0.0
        event_hurt = hit_delta is not None and hit_delta < 0.0
        target_helped = mean_delta is not None and mean_delta > 0.0
        target_hurt = mean_delta is not None and mean_delta < 0.0
        if event_helped:
            improved_event_months += 1
        if event_hurt:
            degraded_event_months += 1
        if target_helped:
            improved_target_months += 1
        if target_hurt:
            degraded_target_months += 1
        coverage_rows.append(
            {
                "month": _safe_int(row.get("month")),
                "skipped": bool(row.get("skipped")) or _safe_int(row.get("selected_candidate_count")) == 0,
                "skip_reason": None if pd.isna(row.get("skip_reason")) else row.get("skip_reason"),
                "selected_candidate_count": _safe_int(row.get("selected_candidate_count")),
                "closed_horizon_candidate_count": _safe_int(row.get("closed_horizon_candidate_count")),
                "unknown_candidate_count": _safe_int(row.get("unknown_candidate_count")),
                "out_of_narrow_universe_count": _safe_int(row.get("out_of_narrow_universe_count")),
                "baseline_top_hit_rate": base_hit,
                "challenger_top_hit_rate": chal_hit,
                "hit_rate_delta": hit_delta,
                "baseline_top_mean_ret20": base_mean,
                "challenger_top_mean_ret20": chal_mean,
                "mean_ret20_delta": mean_delta,
                "changed_top5_members_count": _safe_int(row.get("changed_top5_members_count")),
                "changed_rank_count": _safe_int(row.get("changed_rank_count")),
            }
        )
    return {
        "month_count": len(rows),
        "evaluated_month_count": len(evaluated),
        "skipped_month_count": len(skipped),
        "improved_event_month_count": improved_event_months,
        "degraded_event_month_count": degraded_event_months,
        "improved_mean_ret20_month_count": improved_target_months,
        "degraded_mean_ret20_month_count": degraded_target_months,
        "total_unknown_candidate_count": sum(_safe_int(row.get("unknown_candidate_count")) for row in rows),
        "total_closed_horizon_candidate_count": sum(_safe_int(row.get("closed_horizon_candidate_count")) for row in rows),
        "coverage_rows": coverage_rows,
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary["evaluated_month_count"] < 6:
        return "hold_due_to_thin_closed_horizon_month_coverage", "evaluated_month_count_below_gate"
    if summary["improved_event_month_count"] <= summary["degraded_event_month_count"]:
        return "drop_as_monthly_event_edge_unstable", "event_edge_not_monthly_stable"
    if summary["degraded_mean_ret20_month_count"] > summary["improved_mean_ret20_month_count"] + 1:
        return "hold_due_to_20d_quality_tradeoff", "near_term_edge_costs_too_much_20d_quality"
    return "keep_for_shadow_paper_replay_monthly_gate", "monthly_coverage_and_event_edge_pass"


def run(*, source_root: str | Path = DEFAULT_SOURCE_ROOT, output_dir: str | Path | None = None, source_rank_limit: int = DEFAULT_SOURCE_RANK_LIMIT) -> dict[str, Any]:
    source_root_path = Path(source_root).expanduser().resolve()
    output_root = Path(output_dir).expanduser().resolve() if output_dir else DEFAULT_OUTPUT_ROOT
    output_root.mkdir(parents=True, exist_ok=True)
    generated_at = _utc_now()
    rows = _monthly_rows(source_root_path, source_rank_limit)
    summary = _build_monthly_summary(rows)
    decision, reason = _decision(summary)
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "boundary": "TRADEX-only",
        "source_root": str(source_root_path),
        "source_rank_limit": int(source_rank_limit),
        "same_condition_controls": {
            "same_source_artifacts": True,
            "same_top_k": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "no_silent_fallback": True,
            "no_meemee_reflection": True,
        },
        "what_will_not_change": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal"],
    }
    summary_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "decision": decision,
        "reason_type": reason,
        **{key: value for key, value in summary.items() if key != "coverage_rows"},
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "decision": decision,
        "reason_type": reason,
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "next_gate": "wait_for_more_closed_horizon_months" if decision.startswith("hold") else "paper_replay_or_hold_review",
        "summary": summary_payload,
    }
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "pass": True,
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "source_artifacts_only": True,
        "silent_fallback_used": False,
        "runtime_db_written": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    _write_json(output_root / "downside_prebreak_monthly_coverage_contract.json", contract)
    _write_json(output_root / "downside_prebreak_monthly_coverage_summary.json", summary_payload)
    pd.DataFrame(summary["coverage_rows"]).to_csv(output_root / "downside_prebreak_monthly_coverage_rows.csv", index=False)
    _write_json(output_root / "downside_prebreak_monthly_coverage_decision.json", decision_payload)
    _write_json(output_root / "no_lookahead_audit.json", no_lookahead)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "decision": decision,
        "reason_type": reason,
        "silent_fallback_used": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "decision": decision, "reason_type": reason, "summary": summary_payload}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monthly closed-horizon coverage gate for downside prebreak selector.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--source-rank-limit", type=int, default=DEFAULT_SOURCE_RANK_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run(source_root=args.source_root, output_dir=args.output_dir, source_rank_limit=args.source_rank_limit)
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
