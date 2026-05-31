from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "recent_event_risk_buyability_pretest_v1"
DEFAULT_EVENT_ROOT = Path(r"G:\Tradex\historical_asof_event_backfill_contract_v1\20260525T140234Z-historical-asof-event-backfill-contract-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\recent_event_risk_buyability_pretest_v1")
OFFLINE_OUTCOME_COLUMNS = ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]
REQUIRED_ARTIFACTS = (
    "recent_event_risk_summary.json",
    "recent_event_risk_rows.csv",
    "recent_event_risk_metrics.json",
    "selected_vs_excluded_quality.json",
    "period_support_audit.json",
    "feature_contract.json",
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


def load_rows(event_root: Path) -> pd.DataFrame:
    path = event_root / "event_backfill_rows.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


def add_recent_event_selection(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["recent_event_covered_flag"] = out["selected_event_snapshot_date"].notna()
    earnings_near = out["earnings_nearby_flag"].astype("boolean").fillna(False).astype(bool)
    rights_near = out["ex_rights_nearby_flag"].astype("boolean").fillna(False).astype(bool)
    out["event_risk_exclusion_flag"] = earnings_near
    out["event_supportive_flag"] = rights_near & ~earnings_near
    out["recent_event_buyability_candidate_flag"] = out["recent_event_covered_flag"] & ~out["event_risk_exclusion_flag"]
    out["recent_event_decision_bucket"] = "not_event_covered"
    out.loc[out["recent_event_covered_flag"], "recent_event_decision_bucket"] = "covered_event_neutral"
    out.loc[out["event_risk_exclusion_flag"], "recent_event_decision_bucket"] = "exclude_earnings_nearby"
    out.loc[out["event_supportive_flag"], "recent_event_decision_bucket"] = "supportive_ex_rights_nearby"
    return out


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def metric(frame: pd.DataFrame) -> dict[str, Any]:
    per_date = frame.groupby("as_of_date").size() if not frame.empty else pd.Series(dtype=float)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "winner_rate_ret20_gt_10pct": _rate(frame["winner_ret20_gt_10pct"]) if "winner_ret20_gt_10pct" in frame else None,
        "bad_rate_ret20_lt_minus_5pct": _rate(frame["bad_ret20_lt_minus_5pct"]) if "bad_ret20_lt_minus_5pct" in frame else None,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["severe_ret20_lt_minus_10pct"]) if "severe_ret20_lt_minus_10pct" in frame else None,
        "outcome_coverage_rate": float(frame["ret20"].notna().mean()) if "ret20" in frame and not frame.empty else None,
    }


def build_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "recent_event_covered": metric(rows[rows["recent_event_covered_flag"]]),
        "selected_after_event_risk": metric(rows[rows["recent_event_buyability_candidate_flag"]]),
        "excluded_earnings_nearby": metric(rows[rows["event_risk_exclusion_flag"]]),
        "supportive_ex_rights_nearby": metric(rows[rows["event_supportive_flag"]]),
        "not_event_covered": metric(rows[~rows["recent_event_covered_flag"]]),
    }


def selected_vs_excluded(metrics: dict[str, Any]) -> dict[str, Any]:
    selected = metrics["selected_after_event_risk"]
    excluded = metrics["excluded_earnings_nearby"]
    return {
        "selected_minus_excluded_mean_ret20": (selected.get("mean_ret20") or 0.0) - (excluded.get("mean_ret20") or 0.0),
        "selected_minus_excluded_bad_rate": (selected.get("bad_rate_ret20_lt_minus_5pct") or 0.0) - (excluded.get("bad_rate_ret20_lt_minus_5pct") or 0.0),
        "selected_minus_excluded_severe_rate": (selected.get("severe_rate_ret20_lt_minus_10pct") or 0.0) - (excluded.get("severe_rate_ret20_lt_minus_10pct") or 0.0),
        "selected_minus_excluded_winner_rate": (selected.get("winner_rate_ret20_gt_10pct") or 0.0) - (excluded.get("winner_rate_ret20_gt_10pct") or 0.0),
    }


def period_support(rows: pd.DataFrame) -> dict[str, Any]:
    covered = rows[rows["recent_event_covered_flag"]].copy()
    covered["period_month"] = covered["as_of_date"].astype(str).str.slice(0, 6)
    return {period: metric(group) for period, group in covered.groupby("period_month")}


def decide(metrics: dict[str, Any], diff: dict[str, Any]) -> tuple[str, str, list[str]]:
    selected = metrics["selected_after_event_risk"]
    covered = metrics["recent_event_covered"]
    enough_support = (selected.get("sample_count") or 0) >= 10000 and (selected.get("date_count") or 0) >= 25
    risk_improves = diff["selected_minus_excluded_bad_rate"] < -0.05 and diff["selected_minus_excluded_severe_rate"] < -0.04
    return_ok = (selected.get("mean_ret20") or 0.0) > (covered.get("mean_ret20") or -999.0)
    if enough_support and risk_improves and return_ok:
        return "recent_event_risk_ready_for_current_period_buyability_pretest", "KEEP", ["recent_event_filter_removes_high_downside_earnings_nearby_rows_with_enough_recent_support"]
    if risk_improves and return_ok:
        return "recent_event_risk_promising_but_undercovered", "HOLD_UNDERPOWERED", ["event_filter_direction_positive_but_recent_support_is_thin"]
    return "recent_event_risk_no_buyability_edge", "DROP", ["recent_event_filter_does_not_improve_return_risk_enough"]


def feature_contract() -> dict[str, Any]:
    fields = {
        "as_of_date": {"classification": "identifier"},
        "code": {"classification": "identifier"},
        "selected_event_snapshot_date": {"classification": "point_in_time_feature"},
        "earnings_nearby_flag": {"classification": "point_in_time_feature"},
        "ex_rights_nearby_flag": {"classification": "point_in_time_feature"},
        "recent_event_covered_flag": {"classification": "point_in_time_feature"},
        "event_risk_exclusion_flag": {"classification": "point_in_time_feature"},
        "event_supportive_flag": {"classification": "point_in_time_feature"},
        "recent_event_buyability_candidate_flag": {"classification": "point_in_time_feature"},
        "recent_event_decision_bucket": {"classification": "point_in_time_feature"},
        "ret20_derived_tags": {"classification": "forbidden_future_leak"},
    }
    for col in OFFLINE_OUTCOME_COLUMNS:
        fields[col] = {"classification": "offline_outcome_only"}
    return {"axis_id": AXIS_ID, "fields": fields}


def run(event_root: Path = DEFAULT_EVENT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    source_decision = _load_json(event_root / "research_decision.json")
    source_no_lookahead = _load_json(event_root / "no_lookahead_audit.json")
    rows = add_recent_event_selection(load_rows(event_root))
    metrics = build_metrics(rows)
    diff = selected_vs_excluded(metrics)
    decision, decision_class, reasons = decide(metrics, diff)
    out = output_root / f"{_now_tag()}-recent-event-risk-buyability-pretest-v1"
    out.mkdir(parents=True, exist_ok=True)
    out_cols = [
        "as_of_date",
        "code",
        "selected_event_snapshot_date",
        "earnings_nearby_flag",
        "ex_rights_nearby_flag",
        "recent_event_covered_flag",
        "event_risk_exclusion_flag",
        "event_supportive_flag",
        "recent_event_buyability_candidate_flag",
        "recent_event_decision_bucket",
        *[c for c in OFFLINE_OUTCOME_COLUMNS if c in rows.columns],
    ]
    rows[rows["recent_event_covered_flag"]][out_cols].to_csv(out / "recent_event_risk_rows.csv", index=False)
    _write_json(out / "recent_event_risk_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "recent_support_only": True, "metrics": metrics, "selected_vs_excluded_quality": diff})
    _write_json(out / "recent_event_risk_metrics.json", metrics)
    _write_json(out / "selected_vs_excluded_quality.json", diff)
    _write_json(out / "period_support_audit.json", period_support(rows))
    _write_json(out / "feature_contract.json", feature_contract())
    _write_json(out / "lineage.json", {"source_event_root": str(event_root), "source_decision": source_decision})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass" if source_no_lookahead.get("no_lookahead_pass") else "blocked", "no_lookahead_pass": bool(source_no_lookahead.get("no_lookahead_pass")), "source_no_lookahead": source_no_lookahead, "offline_outcomes_used_in_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_rows": int(len(rows)), "recent_event_covered_rows": int(rows["recent_event_covered_flag"].sum()), "selected_rows": int(rows["recent_event_buyability_candidate_flag"].sum()), "date_count": int(rows["as_of_date"].nunique()), "covered_date_count": int(rows.loc[rows["recent_event_covered_flag"], "as_of_date"].nunique()), "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "recent_support_only": True, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-root", type=Path, default=DEFAULT_EVENT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.event_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
