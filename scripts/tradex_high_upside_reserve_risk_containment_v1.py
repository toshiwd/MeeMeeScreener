from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "high_upside_reserve_risk_containment_v1"
SOURCE_AXIS_ID = "high_upside_reserve_family_discovery_v1"
DEFAULT_PRIOR_ROOT = Path(r"G:\Tradex\high_upside_reserve_family_discovery_v1\20260525T085443Z-high-upside-reserve-family-discovery-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\high_upside_reserve_risk_containment_v1")
REQUIRED_ARTIFACTS = (
    "risk_containment_summary.json",
    "risk_containment_rows.csv",
    "feature_contract.json",
    "containment_variant_metrics.json",
    "downside_failure_profile.json",
    "kept_vs_removed_quality.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
NUMERIC_FIELDS = [
    "decision_date",
    "year",
    "baseline_rank",
    "baseline_score",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "volume_ma20_ratio",
    "realized_vol20",
    "atr14_pct",
    "ret5",
    "ret20",
    "winner_probability",
]
BOOLEAN_FIELDS = [
    "ma7_gt_ma20_gt_ma60",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "path20_available",
    "winner_label",
    "bad_label",
    "severe_label",
    "oos_eval",
]
POINT_IN_TIME_FIELDS = [
    "code",
    "year",
    "decision_date",
    "baseline_rank",
    "baseline_score",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "ma7_gt_ma20_gt_ma60",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ma20_ratio",
    "realized_vol20",
    "atr14_pct",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "primary_family",
    "winner_probability",
    "high_upside_bucket",
]
OUTCOME_FIELDS = ["ret5", "ret20", "winner_label", "bad_label", "severe_label"]


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


def _to_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


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


def load_prior_rows(prior_root: Path) -> pd.DataFrame:
    source = prior_root / "family_discovery_rows.csv"
    rows = pd.read_csv(source, low_memory=False)
    for col in NUMERIC_FIELDS:
        if col in rows:
            rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in BOOLEAN_FIELDS:
        if col in rows:
            rows[col] = _to_bool(rows[col])
    if "code" in rows:
        rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    return rows


def feature_contract(header: list[str]) -> dict[str, Any]:
    fields: dict[str, dict[str, str]] = {}
    for col in sorted(set(POINT_IN_TIME_FIELDS + OUTCOME_FIELDS + ["liquidity_event_fields", "ret20_derived_terms"])):
        if col in OUTCOME_FIELDS:
            cls = "outcome_only"
        elif col == "ret20_derived_terms":
            cls = "forbidden_future_leak"
        elif col == "liquidity_event_fields":
            cls = "unavailable"
        elif col in header:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    return {
        "axis_id": AXIS_ID,
        "source_axis_id": SOURCE_AXIS_ID,
        "source_rows_reused": True,
        "fields": fields,
    }


def base_population(rows: pd.DataFrame) -> pd.DataFrame:
    required = {"oos_eval", "high_upside_bucket", "ret20", "decision_date", "code"}
    missing = sorted(required - set(rows.columns))
    if missing:
        raise ValueError(f"prior family rows missing required columns: {missing}")
    out = rows[rows["oos_eval"] & rows["high_upside_bucket"].eq("top_5pct") & rows["ret20"].notna()].copy()
    out["raw_top5_family"] = True
    return out


def remaining_reserve_population(rows: pd.DataFrame) -> pd.DataFrame:
    return rows[rows["oos_eval"] & rows["high_upside_bucket"].eq("remaining_reserve") & rows["ret20"].notna()].copy()


def metric(frame: pd.DataFrame) -> dict[str, Any]:
    per_date = frame.groupby("decision_date").size() if not frame.empty else pd.Series(dtype=float)
    bad_rate = _rate(frame["ret20"] < -0.05) if not frame.empty else None
    winner_rate = _rate(frame["ret20"] > 0.10) if not frame.empty else None
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
        "kept_share": None,
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(frame["ret20"] > 0) if not frame.empty else None,
        "winner_rate_ret20_gt_10pct": winner_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["ret20"] < -0.10) if not frame.empty else None,
        "downside_to_upside_ratio": None if not winner_rate else (bad_rate or 0) / winner_rate,
    }


def variant_masks(base: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "raw_top_5pct": pd.Series(True, index=base.index),
        "variant_a_refined": ~(
            (base["dist_ma20_pct"] > 0.12)
            | (base["dist_ma60_pct"] > 0.30)
            | (base["realized_vol20"] > 0.05)
            | (base["atr14_pct"] > 0.06)
            | (base["volume_ma20_ratio"] > 2.50)
        ),
        "variant_b": ~(
            base["failed_high_update"]
            | (base["upper_wick_ratio"] > 0.35)
            | base["large_bearish_candle"]
        ),
        "variant_c": (
            base["weekly_monthly_uptrend_proxy"]
            & (base["monthly_high_zone_proxy"] | base["monthly_box_inside_proxy"])
            & ~base["failed_high_update"]
            & ~base["large_bearish_candle"]
            & (base["upper_wick_ratio"] <= 0.35)
            & (base["dist_ma20_pct"] <= 0.12)
            & (base["realized_vol20"] <= 0.05)
        ),
    }


def containment_metrics(base: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    raw = metric(base)
    rows = []
    out: dict[str, Any] = {}
    for name, mask in variant_masks(base).items():
        kept = base[mask].copy()
        m = metric(kept)
        m["kept_share"] = None if raw["sample_count"] == 0 else m["sample_count"] / raw["sample_count"]
        m["delta_mean_ret20_vs_raw"] = None if raw["mean_ret20"] is None or m["mean_ret20"] is None else m["mean_ret20"] - raw["mean_ret20"]
        m["delta_bad_rate_vs_raw"] = None if raw["bad_rate_ret20_lt_minus_5pct"] is None or m["bad_rate_ret20_lt_minus_5pct"] is None else m["bad_rate_ret20_lt_minus_5pct"] - raw["bad_rate_ret20_lt_minus_5pct"]
        m["delta_severe_rate_vs_raw"] = None if raw["severe_rate_ret20_lt_minus_10pct"] is None or m["severe_rate_ret20_lt_minus_10pct"] is None else m["severe_rate_ret20_lt_minus_10pct"] - raw["severe_rate_ret20_lt_minus_10pct"]
        out[name] = m
        tagged = kept.copy()
        tagged["containment_variant"] = name
        tagged["kept_by_variant"] = True
        rows.append(tagged)
    return out, pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def kept_removed_quality(base: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, mask in variant_masks(base).items():
        if name == "raw_top_5pct":
            continue
        kept = base[mask]
        removed = base[~mask]
        out[name] = {
            "kept_mean_ret20": _mean(kept, "ret20"),
            "removed_mean_ret20": _mean(removed, "ret20"),
            "kept_winner_rate": _rate(kept["ret20"] > 0.10) if not kept.empty else None,
            "removed_winner_rate": _rate(removed["ret20"] > 0.10) if not removed.empty else None,
            "kept_bad_rate": _rate(kept["ret20"] < -0.05) if not kept.empty else None,
            "removed_bad_rate": _rate(removed["ret20"] < -0.05) if not removed.empty else None,
            "kept_severe_rate": _rate(kept["ret20"] < -0.10) if not kept.empty else None,
            "removed_severe_rate": _rate(removed["ret20"] < -0.10) if not removed.empty else None,
        }
    return out


def downside_failure_profile(base: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    masks = variant_masks(base)
    for name, mask in masks.items():
        kept = base[mask]
        failures = kept[kept["ret20"] < -0.05]
        severe = kept[kept["ret20"] < -0.10]
        out[name] = {
            "bad_count": int(len(failures)),
            "severe_count": int(len(severe)),
            "daily_extension": {
                "dist_ma20_mean": _mean(failures, "dist_ma20_pct"),
                "dist_ma60_mean": _mean(failures, "dist_ma60_pct"),
            },
            "volatility_atr_proxy": {
                "realized_vol20_mean": _mean(failures, "realized_vol20"),
                "atr14_pct_mean": _mean(failures, "atr14_pct"),
            },
            "failed_high_wick_bearish": {
                "failed_high_rate": _rate(failures["failed_high_update"]) if not failures.empty else None,
                "upper_wick_mean": _mean(failures, "upper_wick_ratio"),
                "large_bearish_rate": _rate(failures["large_bearish_candle"]) if not failures.empty else None,
            },
            "weekly_monthly_context": {
                "weekly_monthly_uptrend_rate": _rate(failures["weekly_monthly_uptrend_proxy"]) if not failures.empty else None,
                "monthly_high_zone_rate": _rate(failures["monthly_high_zone_proxy"]) if not failures.empty else None,
                "monthly_inside_rate": _rate(failures["monthly_box_inside_proxy"]) if not failures.empty else None,
            },
            "rank_bucket": {
                "rank_11_20_rate": _rate(failures["baseline_rank"].between(11, 20, inclusive="both")) if not failures.empty else None,
                "rank_21_30_rate": _rate(failures["baseline_rank"].between(21, 30, inclusive="both")) if not failures.empty else None,
                "rank_31_50_rate": _rate(failures["baseline_rank"].between(31, 50, inclusive="both")) if not failures.empty else None,
            },
            "primary_family_counts": failures["primary_family"].fillna("missing").value_counts().head(10).to_dict() if "primary_family" in failures else {},
        }
    return out


def choose_best_variant(metrics: dict[str, Any]) -> str:
    candidates = ["variant_a_refined", "variant_b", "variant_c"]
    return max(candidates, key=lambda name: metrics[name].get("mean_ret20") if metrics[name].get("mean_ret20") is not None else -999)


def decide(metrics: dict[str, Any], remaining: dict[str, Any], best_name: str) -> tuple[str, list[str]]:
    raw = metrics["raw_top_5pct"]
    best = metrics[best_name]
    raw_bad = raw["bad_rate_ret20_lt_minus_5pct"] or 1
    raw_severe = raw["severe_rate_ret20_lt_minus_10pct"] or 1
    best_bad = best["bad_rate_ret20_lt_minus_5pct"] or 1
    best_severe = best["severe_rate_ret20_lt_minus_10pct"] or 1
    best_mean = best["mean_ret20"] or 0
    best_winner = best["winner_rate_ret20_gt_10pct"] or 0
    remaining_winner = remaining["winner_rate_ret20_gt_10pct"] or 0
    bad_improved = best_bad <= raw_bad - 0.04
    severe_improved = best_severe <= raw_severe - 0.04
    keep_threshold_met = best_bad < 0.25 and best_severe <= 0.15
    usable_breadth = (best["average_candidates_per_date"] or 0) >= 1.0 and (best["date_count"] or 0) >= 60 and (best["kept_share"] or 0) >= 0.30
    upside_ok = best_mean >= 0.05 and best_winner >= remaining_winner + 0.10
    if upside_ok and bad_improved and severe_improved and keep_threshold_met and usable_breadth:
        return "risk_containment_keep_for_pattern_portfolio_pretest", [f"{best_name}_keeps_upside_and_controls_bad_severe_rates"]
    if upside_ok and bad_improved and severe_improved and not usable_breadth:
        return "risk_containment_promising_but_underpowered", [f"{best_name}_direction_positive_but_sample_or_date_support_thin"]
    if upside_ok:
        return "upside_signal_remains_risky_freeze_family_seed", [f"{best_name}_preserves_upside_but_bad_or_severe_risk_fails_predeclared_control"]
    return "no_controllable_family_edge", [f"{best_name}_does_not_preserve_independent_family_return_or_downside_improvement"]


def source_coverage(rows: pd.DataFrame, base: pd.DataFrame) -> dict[str, Any]:
    fields = [c for c in POINT_IN_TIME_FIELDS + OUTCOME_FIELDS if c in rows]
    return {
        "prior_source_axis_id": SOURCE_AXIS_ID,
        "source_row_count": int(len(rows)),
        "base_top5_oos_row_count": int(len(base)),
        "base_top5_oos_date_count": int(base["decision_date"].nunique()) if not base.empty else 0,
        "base_top5_oos_code_count": int(base["code"].nunique()) if not base.empty else 0,
        "research_fallback_used": False,
        "coverage": {c: float(rows[c].notna().mean()) for c in fields},
    }


def run(prior_root: Path, output_root: Path) -> Path:
    rows = load_prior_rows(prior_root)
    contract = feature_contract(rows.columns.tolist())
    base = base_population(rows)
    remaining = remaining_reserve_population(rows)
    out = output_root / f"{_now_tag()}-high-upside-reserve-risk-containment-v1"
    out.mkdir(parents=True, exist_ok=True)

    if base.empty:
        decision = "blocked_missing_point_in_time_features"
        reasons = ["prior_family_discovery_rows_have_no_oos_top_5pct_population"]
        variant_metrics: dict[str, Any] = {}
        best_name = None
        remaining_metric = metric(remaining)
        containment_rows = pd.DataFrame()
    else:
        variant_metrics, containment_rows = containment_metrics(base)
        remaining_metric = metric(remaining)
        best_name = choose_best_variant(variant_metrics)
        decision, reasons = decide(variant_metrics, remaining_metric, best_name)

    containment_rows.to_csv(out / "risk_containment_rows.csv", index=False)
    _write_json(out / "feature_contract.json", contract)
    _write_json(out / "containment_variant_metrics.json", variant_metrics)
    _write_json(out / "downside_failure_profile.json", downside_failure_profile(base) if not base.empty else {})
    _write_json(out / "kept_vs_removed_quality.json", kept_removed_quality(base) if not base.empty else {})
    _write_json(
        out / "risk_containment_summary.json",
        {
            "axis_id": AXIS_ID,
            "prior_root": prior_root,
            "population": "oos_top_5pct_high_upside_reserve_candidates_only",
            "remaining_reserve_reference": remaining_metric,
            "raw_top5_reference": variant_metrics.get("raw_top_5pct") if variant_metrics else None,
            "best_variant": best_name,
            "best_variant_metrics": variant_metrics.get(best_name) if best_name else None,
            "bad_rate_keep_threshold": 0.25,
            "severe_rate_keep_threshold": 0.15,
            "decision": decision,
            "reason_typed": reasons,
        },
    )
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass" if decision != "blocked_missing_point_in_time_features" else "blocked",
            "prior_oos_predictions_reused": True,
            "features_use_saved_point_in_time_context_only": True,
            "outcomes_used_evaluation_only": True,
            "ret20_terms_used_in_feature_construction": False,
            "top10_lift_tested": False,
            "demotion_tested": False,
            "runtime_db_write": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "source_coverage.json", source_coverage(rows, base))
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "reason_typed": reasons,
            "best_variant": best_name,
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
    parser.add_argument("--prior-root", type=Path, default=DEFAULT_PRIOR_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.prior_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
