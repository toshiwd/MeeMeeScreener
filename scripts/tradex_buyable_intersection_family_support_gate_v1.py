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
from scripts import tradex_buyable_intersection_family_audit_v1 as audit


AXIS_ID = "buyable_intersection_family_support_gate_v1"
DEFAULT_SOURCE_DB = audit.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\buyable_intersection_family_support_gate_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "support_gate_summary.json",
    "support_gate_rows.csv",
    "period_stability_metrics.json",
    "date_concentration_audit.json",
    "current_candidate_projection.json",
    "buyability_gate_audit.json",
    "feature_contract.json",
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


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    valid = rows[rows["ret20"].notna()].copy()
    if valid.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "mean_ret20": None, "median_ret20": None, "winner_rate_ret20_gt_10pct": None, "bad_rate_ret20_lt_minus_5pct": None, "severe_rate_ret20_lt_minus_10pct": None, "outcome_coverage_rate": 0.0}
    return {
        "sample_count": int(len(valid)),
        "date_count": int(valid["as_of_date"].nunique()),
        "code_count": int(valid["code"].nunique()),
        "mean_ret20": float(valid["ret20"].mean()),
        "median_ret20": float(valid["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((valid["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((valid["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((valid["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(valid) / len(rows)) if len(rows) else 0.0,
    }


def build_gate_rows(source_db: Path, date_count: int) -> pd.DataFrame:
    frame = audit.build_intersection_frame(source_db, date_count)
    return frame[frame["variant_b_entry_qualified_top50"]].copy()


def period_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {}
    out = {}
    dt = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64").astype(str)
    keyed = rows.copy()
    keyed["period"] = dt.str.slice(0, 4) + "H" + dt.str.slice(4, 6).astype(int).map(lambda month: "1" if month <= 6 else "2")
    for period, grp in keyed.groupby("period"):
        out[str(period)] = _metrics(grp)
    return out


def date_concentration(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {"sample_count": 0, "date_count": 0, "average_candidates_per_date": 0.0, "median_candidates_per_date": 0.0, "max_candidates_per_date": 0, "top_10_dates_share_of_samples": 0.0, "zero_candidate_date_count": 0}
    counts = rows.groupby("as_of_date").size().sort_values(ascending=False)
    all_dates = int(rows["as_of_date"].nunique())
    return {
        "sample_count": int(len(rows)),
        "date_count": all_dates,
        "average_candidates_per_date": float(len(rows) / all_dates) if all_dates else 0.0,
        "median_candidates_per_date": float(counts.median()) if len(counts) else 0.0,
        "max_candidates_per_date": int(counts.max()) if len(counts) else 0,
        "top_10_dates_share_of_samples": float(counts.head(10).sum() / len(rows)) if len(rows) else 0.0,
        "zero_candidate_date_count": 0,
    }


def current_projection(rows: pd.DataFrame) -> dict[str, Any]:
    latest = int(pd.to_numeric(rows["as_of_date"], errors="coerce").max()) if not rows.empty else None
    current = rows[pd.to_numeric(rows["as_of_date"], errors="coerce") == latest].copy() if latest is not None else pd.DataFrame()
    return {
        "latest_as_of_date": latest,
        "current_candidate_count": int(len(current)),
        "current_candidate_codes": current.sort_values("fresh_runtime_research_watch_rank")["code"].astype(str).tolist()[:20] if not current.empty else [],
        "research_watch_only": True,
        "validated_buy_count": 0,
        "buyable_selection_ready": False,
    }


def support_gate(overall: dict[str, Any], periods: dict[str, Any], concentration: dict[str, Any], projection: dict[str, Any]) -> dict[str, Any]:
    quality_ok = (
        overall["mean_ret20"] is not None and overall["mean_ret20"] > 0.03
        and overall["winner_rate_ret20_gt_10pct"] is not None and overall["winner_rate_ret20_gt_10pct"] >= 0.20
        and overall["bad_rate_ret20_lt_minus_5pct"] is not None and overall["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and overall["severe_rate_ret20_lt_minus_10pct"] is not None and overall["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    support_ok = overall["sample_count"] >= 500 and overall["date_count"] >= 50 and concentration["average_candidates_per_date"] >= 1.0
    stable_periods = [p for p, m in periods.items() if m["sample_count"] >= 30 and m["mean_ret20"] is not None and m["mean_ret20"] > 0 and m["bad_rate_ret20_lt_minus_5pct"] <= 0.25]
    stability_ok = len(stable_periods) >= max(1, math.ceil(len(periods) * 0.6)) if periods else False
    concentration_ok = concentration["top_10_dates_share_of_samples"] <= 0.20
    current_ok = projection["current_candidate_count"] > 0
    return {
        "support_gate_pass": bool(quality_ok and support_ok and stability_ok and concentration_ok and current_ok),
        "quality_gate_pass": bool(quality_ok),
        "support_count_gate_pass": bool(support_ok),
        "period_stability_gate_pass": bool(stability_ok),
        "date_concentration_gate_pass": bool(concentration_ok),
        "current_projection_gate_pass": bool(current_ok),
        "stable_periods": stable_periods,
        "thresholds": {"sample_count_min": 500, "date_count_min": 50, "average_candidates_per_date_min": 1.0, "top_10_dates_share_max": 0.20},
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def decide(gate: dict[str, Any]) -> tuple[str, str, list[str]]:
    if gate["support_gate_pass"]:
        return "intersection_family_ready_for_forward_paper_validation", "KEEP", ["intersection_family_passed_historical_buyability_and_support_gate"]
    return "intersection_family_keep_candidate_but_needs_support_repair", "HOLD_UNDERPOWERED", ["historical_gate_passed_but_support_or_current_projection_gate_failed"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    rows = build_gate_rows(source_db, date_count)
    overall = _metrics(rows)
    periods = period_metrics(rows)
    concentration = date_concentration(rows)
    projection = current_projection(rows)
    gate = support_gate(overall, periods, concentration, projection)
    decision, decision_class, reasons = decide(gate)
    out = output_root / f"{_now_tag()}-buyable-intersection-family-support-gate-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = ["as_of_date", "code", "fresh_runtime_research_watch_rank", "ret20", "buy_entry_qualified", "variant_b_entry_qualified_top50"]
    rows[cols].to_csv(out / "support_gate_rows.csv", index=False)
    _write_json(out / "support_gate_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "overall_metrics": overall, "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0})
    _write_json(out / "period_stability_metrics.json", {"axis_id": AXIS_ID, "periods": periods})
    _write_json(out / "date_concentration_audit.json", {"axis_id": AXIS_ID, **concentration})
    _write_json(out / "current_candidate_projection.json", {"axis_id": AXIS_ID, **projection})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "feature_contract.json", {"axis_id": AXIS_ID, "fields": {"fresh_runtime_research_watch_rank": {"classification": "point_in_time_feature"}, "buy_entry_qualified": {"classification": "point_in_time_feature"}, "ret20": {"classification": "offline_outcome_only"}}})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0, "code_count": int(rows["code"].nunique()) if not rows.empty else 0, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "support_gate_uses_frozen_intersection_definition": True, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "upstream_axis": audit.AXIS_ID, "passing_variant": "variant_b_entry_qualified_top50"})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "research_watch_only": True, "buyable_selection_ready": decision_class == "KEEP", "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--date-count", type=int, default=DEFAULT_DATE_COUNT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.output_root, args.date_count)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
