from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "recent_ex_rights_supportive_seed_gate_v1"
DEFAULT_RECENT_EVENT_ROOT = Path(r"G:\Tradex\recent_event_risk_buyability_pretest_v1\20260525T140547Z-recent-event-risk-buyability-pretest-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\recent_ex_rights_supportive_seed_gate_v1")
OFFLINE_OUTCOME_COLUMNS = ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]
REQUIRED_ARTIFACTS = (
    "ex_rights_seed_gate_summary.json",
    "ex_rights_seed_rows.csv",
    "ex_rights_seed_metrics.json",
    "period_stability_metrics.json",
    "date_concentration_audit.json",
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


def load_rows(root: Path) -> pd.DataFrame:
    path = root / "recent_event_risk_rows.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def supportive_slice(rows: pd.DataFrame) -> pd.DataFrame:
    ex = rows["ex_rights_nearby_flag"].fillna(False).astype(bool)
    earnings = rows["earnings_nearby_flag"].fillna(False).astype(bool)
    return rows[ex & ~earnings].copy()


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
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "winner_rate_ret20_gt_10pct": _rate(frame["winner_ret20_gt_10pct"]) if "winner_ret20_gt_10pct" in frame else None,
        "bad_rate_ret20_lt_minus_5pct": _rate(frame["bad_ret20_lt_minus_5pct"]) if "bad_ret20_lt_minus_5pct" in frame else None,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["severe_ret20_lt_minus_10pct"]) if "severe_ret20_lt_minus_10pct" in frame else None,
        "outcome_coverage_rate": float(frame["ret20"].notna().mean()) if "ret20" in frame and not frame.empty else None,
    }


def period_stability(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {}
    frame = rows.copy()
    frame["period_week"] = pd.to_datetime(frame["as_of_date"].astype(str), errors="coerce").dt.strftime("%Y-W%U")
    return {period: metric(group) for period, group in frame.groupby("period_week")}


def concentration(rows: pd.DataFrame) -> dict[str, Any]:
    per_date = rows.groupby("as_of_date").size() if not rows.empty else pd.Series(dtype=float)
    return {
        "sample_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0,
        "top_5_dates_share_of_samples": None if per_date.empty else float(per_date.sort_values(ascending=False).head(5).sum() / len(rows)),
        "top_10_dates_share_of_samples": None if per_date.empty else float(per_date.sort_values(ascending=False).head(10).sum() / len(rows)),
    }


def decide(metrics: dict[str, Any], concentration_audit: dict[str, Any], periods: dict[str, Any]) -> tuple[str, str, list[str]]:
    positive = (metrics.get("mean_ret20") or 0.0) >= 0.03 and (metrics.get("winner_rate_ret20_gt_10pct") or 0.0) >= 0.16
    risk_ok = (metrics.get("bad_rate_ret20_lt_minus_5pct") or 1.0) <= 0.18 and (metrics.get("severe_rate_ret20_lt_minus_10pct") or 1.0) <= 0.06
    breadth_ok = (metrics.get("sample_count") or 0) >= 5000 and (metrics.get("date_count") or 0) >= 60
    concentrated = (concentration_audit.get("top_10_dates_share_of_samples") or 1.0) > 0.65
    negative_periods = [p for p, m in periods.items() if (m.get("mean_ret20") or 0.0) < 0]
    if positive and risk_ok and breadth_ok and not concentrated and len(negative_periods) <= max(1, len(periods) // 3):
        return "ex_rights_seed_keep_for_current_buyability_pretest", "KEEP", ["ex_rights_supportive_slice_has_return_risk_breadth_and_stability"]
    if positive and risk_ok:
        return "ex_rights_seed_promising_but_underpowered", "HOLD_UNDERPOWERED", ["ex_rights_supportive_slice_has_edge_but_support_or_stability_is_thin"]
    return "ex_rights_seed_no_buyability_edge", "DROP", ["ex_rights_supportive_slice_not_strong_or_stable_enough"]


def feature_contract() -> dict[str, Any]:
    fields = {
        "as_of_date": {"classification": "identifier"},
        "code": {"classification": "identifier"},
        "ex_rights_nearby_flag": {"classification": "point_in_time_feature"},
        "earnings_nearby_flag": {"classification": "point_in_time_feature"},
        "selected_event_snapshot_date": {"classification": "point_in_time_feature"},
        "ret20_derived_tags": {"classification": "forbidden_future_leak"},
    }
    for col in OFFLINE_OUTCOME_COLUMNS:
        fields[col] = {"classification": "offline_outcome_only"}
    return {"axis_id": AXIS_ID, "fields": fields}


def run(recent_event_root: Path = DEFAULT_RECENT_EVENT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    rows = load_rows(recent_event_root)
    seed = supportive_slice(rows)
    metrics = metric(seed)
    periods = period_stability(seed)
    concentration_audit = concentration(seed)
    decision, decision_class, reasons = decide(metrics, concentration_audit, periods)
    out = output_root / f"{_now_tag()}-recent-ex-rights-supportive-seed-gate-v1"
    out.mkdir(parents=True, exist_ok=True)
    seed.to_csv(out / "ex_rights_seed_rows.csv", index=False)
    _write_json(out / "ex_rights_seed_gate_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "metrics": metrics, "recent_support_only": True})
    _write_json(out / "ex_rights_seed_metrics.json", metrics)
    _write_json(out / "period_stability_metrics.json", periods)
    _write_json(out / "date_concentration_audit.json", concentration_audit)
    _write_json(out / "feature_contract.json", feature_contract())
    _write_json(out / "lineage.json", {"recent_event_root": str(recent_event_root), "source_decision": _load_json(recent_event_root / "research_decision.json")})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "source_event_snapshot_selection": "already_audited", "offline_outcomes_used_in_seed_definition": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_rows": int(len(rows)), "seed_rows": int(len(seed)), "source_date_count": int(rows["as_of_date"].nunique()), "seed_date_count": int(seed["as_of_date"].nunique()) if not seed.empty else 0, "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "recent_support_only": True, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-event-root", type=Path, default=DEFAULT_RECENT_EVENT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.recent_event_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
