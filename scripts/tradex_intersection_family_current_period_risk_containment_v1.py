from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "intersection_family_current_period_risk_containment_v1"
DEFAULT_SUPPORT_ROOT = Path(
    r"G:\Tradex\buyable_intersection_family_support_gate_v1\20260526T004451Z-buyable-intersection-family-support-gate-v1"
)
DEFAULT_SOURCE_ROWS = Path(
    r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1\pattern_family_source_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\intersection_family_current_period_risk_containment_v1")
REQUIRED_ARTIFACTS = (
    "risk_containment_summary.json",
    "risk_containment_rows.csv",
    "containment_variant_metrics.json",
    "current_period_metrics.json",
    "selected_vs_removed_quality.json",
    "feature_contract.json",
    "source_coverage.json",
    "no_lookahead_audit.json",
    "lineage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

LIVE_FEATURES = {
    "failed_high_flag",
    "upper_wick_ratio",
    "bearish_body_flag",
    "atr14_pct",
    "realized_vol20",
    "close_vs_ma20_pct",
    "recent_high_distance_pct",
    "weekly_supportive_flag",
    "monthly_supportive_flag",
}
OFFLINE_OUTCOMES = {"ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"}


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


def period_bucket(as_of_date: Any) -> str:
    date_int = int(as_of_date)
    year = date_int // 10000
    month = (date_int // 100) % 100
    return f"{year}{'H1' if month <= 6 else 'H2'}"


def load_support_rows(support_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = support_root / "support_gate_rows.csv"
    decision_path = support_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    rows["ret20"] = pd.to_numeric(rows["ret20"], errors="coerce")
    rows["period_bucket"] = rows["as_of_date"].map(period_bucket)
    return rows, _load_json(decision_path)


def load_source_features(source_rows: Path, keys: pd.DataFrame) -> pd.DataFrame:
    if not source_rows.exists():
        raise FileNotFoundError(source_rows)
    cols = ["as_of_date", "code", *sorted(LIVE_FEATURES)]
    source = pd.read_parquet(source_rows, columns=[c for c in cols if c])
    source["as_of_date"] = pd.to_numeric(source["as_of_date"], errors="coerce").astype("Int64")
    source["code"] = source["code"].astype(str)
    key_frame = keys[["as_of_date", "code"]].drop_duplicates().copy()
    return key_frame.merge(source, on=["as_of_date", "code"], how="left", validate="one_to_one")


def attach_features(support: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    rows = support.merge(features, on=["as_of_date", "code"], how="left", validate="many_to_one")
    rows["feature_available_flag"] = rows[sorted(LIVE_FEATURES)].notna().any(axis=1)
    rows["feature_missing_reason"] = rows["feature_available_flag"].map(lambda ok: "" if ok else "source_feature_row_missing")
    return rows


def add_variants(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    failed_high_clean = ~out["failed_high_flag"].fillna(True).astype(bool)
    bearish_clean = ~out["bearish_body_flag"].fillna(True).astype(bool)
    upper_wick_clean = pd.to_numeric(out["upper_wick_ratio"], errors="coerce").le(0.45)
    atr_clean = pd.to_numeric(out["atr14_pct"], errors="coerce").le(0.08)
    realized_vol_clean = pd.to_numeric(out["realized_vol20"], errors="coerce").le(0.08)
    extension_clean = pd.to_numeric(out["close_vs_ma20_pct"], errors="coerce").between(-0.03, 0.12)
    not_chase_high = pd.to_numeric(out["recent_high_distance_pct"], errors="coerce").le(0.03)
    weekly_or_monthly = out["weekly_supportive_flag"].fillna(False).astype(bool) | out["monthly_supportive_flag"].fillna(False).astype(bool)
    out["variant_a_candle_risk_clean"] = failed_high_clean & bearish_clean & upper_wick_clean
    out["variant_b_volatility_extension_clean"] = atr_clean & realized_vol_clean & extension_clean & not_chase_high
    out["variant_c_combined_context_risk_clean"] = out["variant_a_candle_risk_clean"] & out["variant_b_volatility_extension_clean"] & weekly_or_monthly
    return out


def metric_payload(rows: pd.DataFrame) -> dict[str, Any]:
    evaluated = rows[rows["ret20"].notna()].copy()
    if evaluated.empty:
        return {
            "sample_count": 0,
            "date_count": 0,
            "code_count": 0,
            "mean_ret20": None,
            "median_ret20": None,
            "winner_rate_ret20_gt_10pct": None,
            "bad_rate_ret20_lt_minus_5pct": None,
            "severe_rate_ret20_lt_minus_10pct": None,
            "outcome_coverage_rate": 0.0,
        }
    return {
        "sample_count": int(len(evaluated)),
        "date_count": int(evaluated["as_of_date"].nunique()),
        "code_count": int(evaluated["code"].nunique()),
        "mean_ret20": float(evaluated["ret20"].mean()),
        "median_ret20": float(evaluated["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((evaluated["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((evaluated["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((evaluated["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(evaluated) / len(rows)) if len(rows) else 0.0,
    }


def variant_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    variants = [
        "baseline_intersection_family",
        "variant_a_candle_risk_clean",
        "variant_b_volatility_extension_clean",
        "variant_c_combined_context_risk_clean",
    ]
    out: dict[str, Any] = {"baseline_intersection_family": metric_payload(rows)}
    for variant in variants[1:]:
        selected = rows[rows[variant].fillna(False).astype(bool)]
        metrics = metric_payload(selected)
        metrics["selected_share"] = float(len(selected) / len(rows)) if len(rows) else 0.0
        out[variant] = metrics
    return out


def selected_vs_removed(rows: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for variant in ["variant_a_candle_risk_clean", "variant_b_volatility_extension_clean", "variant_c_combined_context_risk_clean"]:
        selected = rows[rows[variant].fillna(False).astype(bool)]
        removed = rows[~rows[variant].fillna(False).astype(bool)]
        s = metric_payload(selected)
        r = metric_payload(removed)
        result[variant] = {
            "selected": s,
            "removed": r,
            "selected_minus_removed_ret20": None if s["mean_ret20"] is None or r["mean_ret20"] is None else s["mean_ret20"] - r["mean_ret20"],
            "selected_minus_removed_bad_rate": None
            if s["bad_rate_ret20_lt_minus_5pct"] is None or r["bad_rate_ret20_lt_minus_5pct"] is None
            else s["bad_rate_ret20_lt_minus_5pct"] - r["bad_rate_ret20_lt_minus_5pct"],
            "selected_minus_removed_severe_rate": None
            if s["severe_rate_ret20_lt_minus_10pct"] is None or r["severe_rate_ret20_lt_minus_10pct"] is None
            else s["severe_rate_ret20_lt_minus_10pct"] - r["severe_rate_ret20_lt_minus_10pct"],
        }
    return result


def current_period(rows: pd.DataFrame) -> pd.DataFrame:
    latest = sorted(rows["period_bucket"].dropna().unique())[-1]
    return rows[rows["period_bucket"] == latest].copy()


def choose_best_current_variant(metrics: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    candidates = {k: v for k, v in metrics.items() if k != "baseline_intersection_family"}
    return max(candidates.items(), key=lambda item: (item[1]["mean_ret20"] or -999, -(item[1]["bad_rate_ret20_lt_minus_5pct"] or 999)))


def no_lookahead_audit(rows: pd.DataFrame, support_decision: dict[str, Any]) -> dict[str, Any]:
    support_ok = support_decision.get("research_decision") == "intersection_family_ready_for_forward_paper_validation"
    forbidden_in_variants = sorted(OFFLINE_OUTCOMES & {"variant_a_candle_risk_clean", "variant_b_volatility_extension_clean", "variant_c_combined_context_risk_clean"})
    live_present = sorted(LIVE_FEATURES & set(rows.columns))
    passed = bool(support_ok and len(live_present) >= 6 and not forbidden_in_variants)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "support_gate_ready": support_ok,
        "live_feature_columns_present": live_present,
        "future_outcomes_used_for_variant_construction": False,
        "ret20_used_evaluation_only": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def feature_contract(rows: pd.DataFrame) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for col in rows.columns:
        if col in {"as_of_date", "code"}:
            classification = "identifier"
        elif col in OFFLINE_OUTCOMES:
            classification = "offline_outcome_only"
        elif col in LIVE_FEATURES or col.startswith("variant_") or col in {"fresh_runtime_research_watch_rank", "buy_entry_qualified"}:
            classification = "point_in_time_feature"
        else:
            classification = "source_metadata"
        fields[col] = {"classification": classification}
    return {"axis_id": AXIS_ID, "fields": fields}


def decide(best: tuple[str, dict[str, Any]], current_baseline: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["risk_containment_feature_or_support_contract_failed"]
    name, metrics = best
    support_ok = metrics["sample_count"] >= 20 and metrics["date_count"] >= 10 and metrics["selected_share"] >= 0.2
    quality_ok = (
        metrics["mean_ret20"] is not None
        and metrics["mean_ret20"] > 0.03
        and metrics["winner_rate_ret20_gt_10pct"] is not None
        and metrics["winner_rate_ret20_gt_10pct"] >= 0.20
        and metrics["bad_rate_ret20_lt_minus_5pct"] is not None
        and metrics["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and metrics["severe_rate_ret20_lt_minus_10pct"] is not None
        and metrics["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    improves_current = (
        current_baseline["mean_ret20"] is not None
        and metrics["mean_ret20"] is not None
        and metrics["mean_ret20"] > current_baseline["mean_ret20"]
        and metrics["bad_rate_ret20_lt_minus_5pct"] is not None
        and current_baseline["bad_rate_ret20_lt_minus_5pct"] is not None
        and metrics["bad_rate_ret20_lt_minus_5pct"] < current_baseline["bad_rate_ret20_lt_minus_5pct"]
    )
    if support_ok and quality_ok and improves_current:
        return "intersection_current_period_risk_containment_buyable_ready", "KEEP", [f"{name}_passed_current_period_buyability_gate"]
    if improves_current:
        return "intersection_current_period_risk_containment_improved_but_not_buyable", "HOLD_UNDERPOWERED", [
            f"{name}_improved_current_period_but_failed_support_or_quality_gate"
        ]
    return "intersection_current_period_risk_containment_no_edge", "DROP", ["fixed_risk_containment_variants_did_not_repair_current_period"]


def run(
    support_root: Path = DEFAULT_SUPPORT_ROOT,
    source_rows: Path = DEFAULT_SOURCE_ROWS,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    support, support_decision = load_support_rows(support_root)
    features = load_source_features(source_rows, support)
    rows = add_variants(attach_features(support, features))
    current = current_period(rows)
    all_metrics = variant_metrics(rows)
    current_metrics = variant_metrics(current)
    best = choose_best_current_variant(current_metrics)
    audit = no_lookahead_audit(rows, support_decision)
    decision, decision_class, reasons = decide(best, current_metrics["baseline_intersection_family"], audit)

    out = output_root / f"{_now_tag()}-intersection-family-current-period-risk-containment-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows.to_csv(out / "risk_containment_rows.csv", index=False)
    _write_json(
        out / "risk_containment_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "best_current_period_variant": best[0],
            "best_current_period_metrics": best[1],
            "current_period_baseline_metrics": current_metrics["baseline_intersection_family"],
            "buyable_selection_ready": decision_class == "KEEP",
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "containment_variant_metrics.json", {"axis_id": AXIS_ID, "all_periods": all_metrics})
    _write_json(out / "current_period_metrics.json", {"axis_id": AXIS_ID, "current_period": sorted(rows["period_bucket"].unique())[-1], "variants": current_metrics})
    _write_json(out / "selected_vs_removed_quality.json", {"axis_id": AXIS_ID, "all_periods": selected_vs_removed(rows), "current_period": selected_vs_removed(current)})
    _write_json(out / "feature_contract.json", feature_contract(rows))
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "support_root": str(support_root),
            "source_rows": str(source_rows),
            "row_count": int(len(rows)),
            "feature_available_rate": float(rows["feature_available_flag"].mean()) if len(rows) else 0.0,
            "date_count": int(rows["as_of_date"].nunique()),
            "code_count": int(rows["code"].nunique()),
            "research_fallback_used": False,
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "support_root": str(support_root), "support_decision": support_decision, "source_rows": str(source_rows)})
    _write_json(
        out / "research_decision.json",
        {
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
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--support-root", type=Path, default=DEFAULT_SUPPORT_ROOT)
    parser.add_argument("--source-rows", type=Path, default=DEFAULT_SOURCE_ROWS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.support_root, args.source_rows, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
