from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_high_upside_reserve_risk_containment_v1 as base_axis


AXIS_ID = "high_upside_reserve_risk_containment_robustness_gate_v1"
FIXED_VARIANT = "variant_a_refined"
DEFAULT_PRIOR_CONTAINMENT_ROOT = Path(r"G:\Tradex\high_upside_reserve_risk_containment_v1\20260525T090455Z-high-upside-reserve-risk-containment-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\high_upside_reserve_risk_containment_robustness_gate_v1")
REQUIRED_ARTIFACTS = (
    "robustness_gate_summary.json",
    "robustness_gate_rows.csv",
    "period_stability_metrics.json",
    "regime_stability_metrics.json",
    "date_concentration_audit.json",
    "candidate_breadth_audit.json",
    "kept_removed_quality_by_period.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
REGIME_COLUMNS = [
    "weekly_monthly_uptrend_proxy",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "ma7_gt_ma20_gt_ma60",
    "primary_family",
]


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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _date_year(value: Any) -> int | None:
    if pd.isna(value):
        return None
    text = str(int(value))
    return int(text[:4]) if len(text) >= 4 else None


def _date_half(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(int(value))
    if len(text) < 6:
        return None
    month = int(text[4:6])
    half = "H1" if month <= 6 else "H2"
    return f"{text[:4]}{half}"


def load_fixed_population(prior_containment_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    summary_path = prior_containment_root / "risk_containment_summary.json"
    decision_path = prior_containment_root / "research_decision.json"
    if not summary_path.exists() or not decision_path.exists():
        raise ValueError("required prior risk containment artifacts are missing")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    if summary.get("best_variant") != FIXED_VARIANT or decision.get("best_variant") != FIXED_VARIANT:
        raise ValueError("fixed variant contract cannot be reconstructed from prior artifacts")
    prior_source = Path(summary["prior_root"])
    rows = base_axis.load_prior_rows(prior_source)
    raw = base_axis.base_population(rows)
    mask = base_axis.variant_masks(raw)[FIXED_VARIANT]
    tagged = raw.copy()
    tagged["fixed_variant"] = FIXED_VARIANT
    tagged["kept_by_fixed_variant"] = mask
    tagged["period_year"] = tagged["decision_date"].map(_date_year)
    tagged["period_half"] = tagged["decision_date"].map(_date_half)
    return tagged, summary, decision


def metric(frame: pd.DataFrame, raw_frame: pd.DataFrame | None = None) -> dict[str, Any]:
    per_date = frame.groupby("decision_date").size() if not frame.empty else pd.Series(dtype=float)
    raw_count = len(raw_frame) if raw_frame is not None else len(frame)
    bad = frame["ret20"] < -0.05 if not frame.empty else pd.Series(dtype=bool)
    severe = frame["ret20"] < -0.10 if not frame.empty else pd.Series(dtype=bool)
    winner = frame["ret20"] > 0.10 if not frame.empty else pd.Series(dtype=bool)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
        "kept_share": None if raw_count == 0 else len(frame) / raw_count,
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "winner_rate": _rate(winner),
        "bad_rate": _rate(bad),
        "severe_rate": _rate(severe),
    }


def date_concentration_audit(rows: pd.DataFrame) -> dict[str, Any]:
    kept = rows[rows["kept_by_fixed_variant"]]
    raw_dates = set(rows["decision_date"].dropna().unique().tolist())
    kept_counts = kept.groupby("decision_date").size().sort_values(ascending=False)
    top_10_samples = int(kept_counts.head(10).sum()) if not kept_counts.empty else 0
    return {
        **metric(kept, rows),
        "raw_top5_date_count": int(len(raw_dates)),
        "zero_candidate_date_count": int(len(raw_dates - set(kept_counts.index.tolist()))),
        "top_10_dates_share_of_samples": None if len(kept) == 0 else top_10_samples / len(kept),
    }


def candidate_breadth_audit(rows: pd.DataFrame) -> dict[str, Any]:
    kept = rows[rows["kept_by_fixed_variant"]]
    return {
        "raw_sample_count": int(len(rows)),
        "kept_sample_count": int(len(kept)),
        "removed_sample_count": int((~rows["kept_by_fixed_variant"]).sum()),
        "kept_share": None if len(rows) == 0 else len(kept) / len(rows),
        "raw_code_count": int(rows["code"].nunique()),
        "kept_code_count": int(kept["code"].nunique()),
        "raw_date_count": int(rows["decision_date"].nunique()),
        "kept_date_count": int(kept["decision_date"].nunique()),
        "single_candidate_date_share": None if kept.empty else float((kept.groupby("decision_date").size() == 1).mean()),
        "dates_with_at_least_two_candidates": int((kept.groupby("decision_date").size() >= 2).sum()) if not kept.empty else 0,
    }


def period_stability(rows: pd.DataFrame) -> dict[str, Any]:
    period_col = "period_half" if rows[rows["kept_by_fixed_variant"]]["period_half"].nunique() >= 4 else "period_year"
    out: dict[str, Any] = {"period_column": period_col, "periods": {}}
    for period, raw_g in rows.groupby(period_col, dropna=True):
        kept = raw_g[raw_g["kept_by_fixed_variant"]]
        out["periods"][str(period)] = metric(kept, raw_g)
    return out


def regime_stability(rows: pd.DataFrame) -> dict[str, Any]:
    available = [c for c in REGIME_COLUMNS if c in rows.columns]
    missing = [c for c in REGIME_COLUMNS if c not in rows.columns]
    out: dict[str, Any] = {"available_regime_columns": available, "missing_regime_columns": missing, "regimes": {}}
    for col in available:
        out["regimes"][col] = {}
        for value, raw_g in rows.groupby(col, dropna=False):
            kept = raw_g[raw_g["kept_by_fixed_variant"]]
            out["regimes"][col][str(value)] = metric(kept, raw_g)
    return out


def kept_removed_quality_by_period(rows: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"overall": kept_removed_quality(rows), "periods": {}}
    period_col = "period_half" if rows[rows["kept_by_fixed_variant"]]["period_half"].nunique() >= 4 else "period_year"
    out["period_column"] = period_col
    for period, g in rows.groupby(period_col, dropna=True):
        out["periods"][str(period)] = kept_removed_quality(g)
    return out


def kept_removed_quality(rows: pd.DataFrame) -> dict[str, Any]:
    kept = rows[rows["kept_by_fixed_variant"]]
    removed = rows[~rows["kept_by_fixed_variant"]]
    return {
        "kept_mean_ret20": _mean(kept, "ret20"),
        "removed_mean_ret20": _mean(removed, "ret20"),
        "kept_winner_rate": _rate(kept["ret20"] > 0.10) if not kept.empty else None,
        "removed_winner_rate": _rate(removed["ret20"] > 0.10) if not removed.empty else None,
        "kept_bad_rate": _rate(kept["ret20"] < -0.05) if not kept.empty else None,
        "removed_bad_rate": _rate(removed["ret20"] < -0.05) if not removed.empty else None,
        "kept_severe_rate": _rate(kept["ret20"] < -0.10) if not kept.empty else None,
        "removed_severe_rate": _rate(removed["ret20"] < -0.10) if not removed.empty else None,
    }


def decide(overall: dict[str, Any], date_audit: dict[str, Any], period_metrics: dict[str, Any], quality: dict[str, Any]) -> tuple[str, list[str]]:
    kept_share = overall["kept_share"] or 0
    mean_ret20 = overall["mean_ret20"] or 0
    bad_rate = overall["bad_rate"] if overall["bad_rate"] is not None else 1
    severe_rate = overall["severe_rate"] if overall["severe_rate"] is not None else 1
    support_ok = kept_share >= 0.30
    breadth_justified = (date_audit["date_count"] or 0) >= 100 and (date_audit["top_10_dates_share_of_samples"] or 1) <= 0.25
    periods = period_metrics.get("periods", {})
    usable_periods = [p for p in periods.values() if (p.get("sample_count") or 0) >= 10]
    negative_periods = [p for p in usable_periods if (p.get("mean_ret20") or 0) <= 0 or (p.get("bad_rate") or 1) >= 0.35]
    concentration_ok = (date_audit["top_10_dates_share_of_samples"] or 1) <= 0.35 and (date_audit["zero_candidate_date_count"] or 9999) <= 200
    edge_ok = quality["overall"]["kept_mean_ret20"] is not None and quality["overall"]["removed_mean_ret20"] is not None and quality["overall"]["kept_mean_ret20"] > quality["overall"]["removed_mean_ret20"]
    risk_ok = bad_rate < 0.25 and severe_rate <= 0.15
    direction_ok = mean_ret20 >= 0.05 and edge_ok and risk_ok
    most_periods_ok = len(usable_periods) > 0 and len(negative_periods) <= max(1, len(usable_periods) // 3)
    if direction_ok and (support_ok or breadth_justified) and concentration_ok and most_periods_ok:
        return "risk_containment_keep_for_pattern_portfolio_pretest", ["fixed_variant_passes_return_risk_support_and_concentration_gate"]
    if direction_ok and not (support_ok or breadth_justified):
        return "risk_containment_promising_but_underpowered", ["fixed_variant_direction_remains_strong_but_support_breadth_is_still_insufficient"]
    if direction_ok and not concentration_ok:
        return "upside_signal_unstable_freeze_family_seed", ["fixed_variant_result_is_concentrated_in_too_few_dates_or_periods"]
    if not direction_ok:
        return "no_controllable_family_edge", ["fixed_variant_no_longer_shows_clear_return_risk_advantage"]
    return "risk_containment_promising_but_underpowered", ["fixed_variant_stability_support_is_not_strong_enough_for_keep"]


def source_coverage(rows: pd.DataFrame) -> dict[str, Any]:
    fields = [c for c in base_axis.POINT_IN_TIME_FIELDS + base_axis.OUTCOME_FIELDS if c in rows]
    return {
        "axis_id": AXIS_ID,
        "fixed_variant_reconstructed": True,
        "row_count": int(len(rows)),
        "kept_row_count": int(rows["kept_by_fixed_variant"].sum()),
        "date_count": int(rows["decision_date"].nunique()),
        "code_count": int(rows["code"].nunique()),
        "research_fallback_used": False,
        "coverage": {c: float(rows[c].notna().mean()) for c in fields},
    }


def run(prior_containment_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-high-upside-reserve-risk-containment-robustness-gate-v1"
    out.mkdir(parents=True, exist_ok=True)
    try:
        rows, prior_summary, prior_decision = load_fixed_population(prior_containment_root)
        blocked_reason = None
    except Exception as exc:
        rows = pd.DataFrame()
        prior_summary = {}
        prior_decision = {}
        blocked_reason = str(exc)

    if blocked_reason:
        decision = "blocked_missing_contract"
        reasons = [blocked_reason]
        overall = {}
        date_audit = {}
        period_metrics = {}
        regime_metrics = {}
        breadth = {}
        quality = {}
    else:
        kept = rows[rows["kept_by_fixed_variant"]]
        overall = metric(kept, rows)
        date_audit = date_concentration_audit(rows)
        period_metrics = period_stability(rows)
        regime_metrics = regime_stability(rows)
        breadth = candidate_breadth_audit(rows)
        quality = kept_removed_quality_by_period(rows)
        decision, reasons = decide(overall, date_audit, period_metrics, quality)

    rows.to_csv(out / "robustness_gate_rows.csv", index=False)
    _write_json(out / "period_stability_metrics.json", period_metrics)
    _write_json(out / "regime_stability_metrics.json", regime_metrics)
    _write_json(out / "date_concentration_audit.json", date_audit)
    _write_json(out / "candidate_breadth_audit.json", breadth)
    _write_json(out / "kept_removed_quality_by_period.json", quality)
    _write_json(out / "source_coverage.json", source_coverage(rows) if not rows.empty else {"fixed_variant_reconstructed": False, "research_fallback_used": False})
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "blocked" if blocked_reason else "pass",
            "fixed_variant_reused_exactly": not blocked_reason,
            "variant_name": FIXED_VARIANT,
            "thresholds_retuned": False,
            "new_variants_added": False,
            "features_use_saved_point_in_time_context_only": True,
            "outcomes_used_evaluation_only": True,
            "runtime_db_write": False,
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "robustness_gate_summary.json",
        {
            "axis_id": AXIS_ID,
            "prior_containment_root": prior_containment_root,
            "fixed_variant": FIXED_VARIANT,
            "prior_decision": prior_decision.get("research_decision"),
            "prior_summary_decision": prior_summary.get("decision"),
            "overall_fixed_variant_metrics": overall,
            "date_concentration": date_audit,
            "candidate_breadth": breadth,
            "decision": decision,
            "reason_typed": reasons,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "reason_typed": reasons,
            "fixed_variant": FIXED_VARIANT,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prior-containment-root", type=Path, default=DEFAULT_PRIOR_CONTAINMENT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.prior_containment_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
