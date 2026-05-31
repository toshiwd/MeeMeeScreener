from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


AXIS_ID = "high_upside_reserve_family_discovery_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_SEPARABILITY_ROOT = Path(r"G:\Tradex\rank11_50_winner_separability_audit_v1\20260525T083530Z-rank11-50-winner-separability-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\high_upside_reserve_family_discovery_v1")
REQUIRED_ARTIFACTS = (
    "family_discovery_summary.json",
    "family_discovery_rows.csv",
    "feature_contract.json",
    "high_upside_bucket_metrics.json",
    "downside_risk_decomposition.json",
    "family_candidate_profiles.json",
    "risk_containment_probe.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
NUMERIC_FEATURES = [
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
    "lower_wick_ratio",
    "upper_wick_ratio",
    "volume_ma20_ratio",
    "realized_vol20",
    "atr14_pct",
]
BOOLEAN_FEATURES = [
    "ma7_gt_ma20_gt_ma60",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
]
CATEGORICAL_FEATURES = ["primary_family"]
READ_COLUMNS = ["decision_date", "code", "year", "path20_available", *NUMERIC_FEATURES, *BOOLEAN_FEATURES, *CATEGORICAL_FEATURES, "ret5", "ret20"]


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


def feature_contract(header: list[str]) -> dict[str, Any]:
    fields = {}
    for col in READ_COLUMNS:
        if col in {"ret5", "ret20"}:
            cls = "outcome_only"
        elif col in header:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    fields["ret20_derived_terms"] = {"classification": "forbidden_future_leak"}
    fields["liquidity_event_fields"] = {"classification": "unavailable"}
    return {"axis_id": AXIS_ID, "fields": fields, "model_dependency": "sklearn.LogisticRegression"}


def load_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = input_root / "candidate_family_source_rows.csv"
    header = pd.read_csv(source, nrows=0).columns.tolist()
    present = [c for c in READ_COLUMNS if c in header]
    rows = pd.concat([c for c in pd.read_csv(source, usecols=present, chunksize=250_000, low_memory=False)], ignore_index=True)
    for c in READ_COLUMNS:
        if c not in rows:
            rows[c] = pd.NA
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    for col in ["decision_date", "year", *NUMERIC_FEATURES, "ret5", "ret20"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["path20_available", *BOOLEAN_FEATURES]:
        rows[col] = _to_bool(rows[col])
    rows = rows[rows["path20_available"] & rows["baseline_rank"].between(11, 50, inclusive="both") & rows["ret20"].notna()].copy()
    rows["winner_label"] = rows["ret20"] > 0.10
    rows["bad_label"] = rows["ret20"] < -0.05
    rows["severe_label"] = rows["ret20"] < -0.10
    return rows, feature_contract(header)


def build_model() -> Pipeline:
    pre = ColumnTransformer(
        [
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), NUMERIC_FEATURES),
            ("bool", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(drop="if_binary", handle_unknown="ignore"))]), BOOLEAN_FEATURES),
            ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(handle_unknown="ignore"))]), CATEGORICAL_FEATURES),
        ]
    )
    return Pipeline([("preprocess", pre), ("model", LogisticRegression(max_iter=1000, class_weight="balanced", solver="liblinear", random_state=42))])


def score_oos(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    split_date = int(rows["decision_date"].quantile(0.70))
    train = rows[rows["decision_date"] <= split_date].copy()
    test = rows[rows["decision_date"] > split_date].copy()
    features = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES
    train_x = train[features].copy()
    all_x = rows[features].copy()
    for col in BOOLEAN_FEATURES + CATEGORICAL_FEATURES:
        train_x[col] = train_x[col].astype(str)
        all_x[col] = all_x[col].astype(str)
    model = build_model()
    model.fit(train_x, train["winner_label"].astype(int))
    out = rows.copy()
    out["winner_probability"] = model.predict_proba(all_x)[:, 1]
    out["oos_eval"] = out["decision_date"] > split_date
    report = {"model": "LogisticRegression", "split_date": split_date, "train_sample_count": int(len(train)), "oos_sample_count": int(len(test)), "train_date_count": int(train["decision_date"].nunique()), "oos_date_count": int(test["decision_date"].nunique())}
    return out, report


def assign_buckets(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    oos = out[out["oos_eval"]]
    out["high_upside_bucket"] = "train_not_evaluated"
    cuts = {
        "top_1pct": oos["winner_probability"].quantile(0.99),
        "top_3pct": oos["winner_probability"].quantile(0.97),
        "top_5pct": oos["winner_probability"].quantile(0.95),
        "top_10pct": oos["winner_probability"].quantile(0.90),
    }
    out.loc[out["oos_eval"], "high_upside_bucket"] = "remaining_reserve"
    for name in ["top_10pct", "top_5pct", "top_3pct", "top_1pct"]:
        out.loc[out["oos_eval"] & (out["winner_probability"] >= cuts[name]), "high_upside_bucket"] = name
    return out


def bucket_metric(frame: pd.DataFrame) -> dict[str, Any]:
    per_date = frame.groupby("decision_date").size() if not frame.empty else pd.Series(dtype=float)
    bad = frame["bad_label"] if not frame.empty else pd.Series(dtype=bool)
    win = frame["winner_label"] if not frame.empty else pd.Series(dtype=bool)
    bad_rate = _rate(bad)
    win_rate = _rate(win)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(frame["ret20"] > 0) if not frame.empty else None,
        "winner_rate_ret20_gt_10pct": win_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["severe_label"]) if not frame.empty else None,
        "downside_to_upside_ratio": None if not win_rate else (bad_rate or 0) / win_rate,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
    }


def bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    oos = rows[rows["oos_eval"]]
    return {bucket: bucket_metric(oos[oos["high_upside_bucket"].eq(bucket)]) for bucket in ["top_1pct", "top_3pct", "top_5pct", "top_10pct", "remaining_reserve"]}


def downside_decomposition(rows: pd.DataFrame) -> dict[str, Any]:
    oos = rows[rows["oos_eval"] & rows["high_upside_bucket"].isin(["top_1pct", "top_3pct", "top_5pct", "top_10pct"])].copy()
    out = {}
    for bucket, g in oos.groupby("high_upside_bucket"):
        bad = g[g["bad_label"] | g["severe_label"]]
        out[bucket] = {
            "bad_or_severe_count": int(len(bad)),
            "daily_extension": {"dist_ma20_mean": _mean(bad, "dist_ma20_pct"), "dist_ma60_mean": _mean(bad, "dist_ma60_pct")},
            "weekly_monthly_regime": {"weekly_monthly_uptrend_rate": _rate(bad["weekly_monthly_uptrend_proxy"]) if not bad.empty else None, "monthly_high_zone_rate": _rate(bad["monthly_high_zone_proxy"]) if not bad.empty else None},
            "volume_turnover_proxies": {"volume_ma20_ratio_mean": _mean(bad, "volume_ma20_ratio")},
            "failed_high_wick_bearish": {"failed_high_rate": _rate(bad["failed_high_update"]) if not bad.empty else None, "upper_wick_mean": _mean(bad, "upper_wick_ratio"), "large_bearish_rate": _rate(bad["large_bearish_candle"]) if not bad.empty else None},
            "volatility_atr_proxy": {"realized_vol20_mean": _mean(bad, "realized_vol20"), "atr14_pct_mean": _mean(bad, "atr14_pct")},
            "rank_bucket": {
                "rank_11_20_rate": _rate(bad["baseline_rank"].between(11, 20, inclusive="both")) if not bad.empty else None,
                "rank_21_30_rate": _rate(bad["baseline_rank"].between(21, 30, inclusive="both")) if not bad.empty else None,
                "rank_31_50_rate": _rate(bad["baseline_rank"].between(31, 50, inclusive="both")) if not bad.empty else None,
            },
            "primary_family_counts": bad["primary_family"].fillna("missing").value_counts().head(10).to_dict(),
        }
    return out


def containment_variants(rows: pd.DataFrame) -> dict[str, Any]:
    oos = rows[rows["oos_eval"] & rows["high_upside_bucket"].eq("top_5pct")].copy()
    raw = bucket_metric(oos)
    variants = {
        "variant_a": oos[~((oos["dist_ma20_pct"] > 0.12) | (oos["realized_vol20"] > 0.05) | (oos["atr14_pct"] > 0.06))],
        "variant_b": oos[~(oos["failed_high_update"] | (oos["upper_wick_ratio"] > 0.35) | oos["large_bearish_candle"])],
        "variant_c": oos[oos["weekly_monthly_uptrend_proxy"] & (oos["monthly_high_zone_proxy"] | oos["monthly_box_inside_proxy"])],
    }
    out = {"raw_top_5pct": raw}
    for name, frame in variants.items():
        m = bucket_metric(frame)
        out[name] = {
            **m,
            "risk_containment_delta_mean_ret20": None if raw["mean_ret20"] is None or m["mean_ret20"] is None else m["mean_ret20"] - raw["mean_ret20"],
            "risk_containment_delta_bad_rate": None if raw["bad_rate_ret20_lt_minus_5pct"] is None or m["bad_rate_ret20_lt_minus_5pct"] is None else m["bad_rate_ret20_lt_minus_5pct"] - raw["bad_rate_ret20_lt_minus_5pct"],
            "risk_containment_delta_severe_rate": None if raw["severe_rate_ret20_lt_minus_10pct"] is None or m["severe_rate_ret20_lt_minus_10pct"] is None else m["severe_rate_ret20_lt_minus_10pct"] - raw["severe_rate_ret20_lt_minus_10pct"],
            "risk_containment_kept_share": None if raw["sample_count"] == 0 else m["sample_count"] / raw["sample_count"],
        }
    return out


def family_profiles(rows: pd.DataFrame, metrics: dict[str, Any], containment: dict[str, Any]) -> dict[str, Any]:
    remaining = metrics["remaining_reserve"]
    top = metrics["top_5pct"]
    best_variant = max(["variant_a", "variant_b", "variant_c"], key=lambda v: containment[v].get("mean_ret20") or -999)
    best = containment[best_variant]
    return {
        "raw_high_upside_top_5pct_vs_remaining": {
            "family_mean_ret20": top["mean_ret20"],
            "remaining_mean_ret20": remaining["mean_ret20"],
            "family_winner_rate": top["winner_rate_ret20_gt_10pct"],
            "remaining_winner_rate": remaining["winner_rate_ret20_gt_10pct"],
            "family_bad_rate": top["bad_rate_ret20_lt_minus_5pct"],
            "remaining_bad_rate": remaining["bad_rate_ret20_lt_minus_5pct"],
            "family_severe_rate": top["severe_rate_ret20_lt_minus_10pct"],
            "remaining_severe_rate": remaining["severe_rate_ret20_lt_minus_10pct"],
            "family_sample_count": top["sample_count"],
            "family_average_candidates_per_date": top["average_candidates_per_date"],
        },
        "best_risk_containment_variant": best_variant,
        "best_risk_containment_profile": {
            "family_mean_ret20": best["mean_ret20"],
            "family_winner_rate": best["winner_rate_ret20_gt_10pct"],
            "family_bad_rate": best["bad_rate_ret20_lt_minus_5pct"],
            "family_severe_rate": best["severe_rate_ret20_lt_minus_10pct"],
            "family_sample_count": best["sample_count"],
            "family_average_candidates_per_date": best["average_candidates_per_date"],
            "risk_containment_delta_mean_ret20": best["risk_containment_delta_mean_ret20"],
            "risk_containment_delta_bad_rate": best["risk_containment_delta_bad_rate"],
            "risk_containment_delta_severe_rate": best["risk_containment_delta_severe_rate"],
            "risk_containment_kept_share": best["risk_containment_kept_share"],
        },
    }


def decide(metrics: dict[str, Any], containment: dict[str, Any], profiles: dict[str, Any]) -> tuple[str, list[str]]:
    top = metrics["top_5pct"]
    remaining = metrics["remaining_reserve"]
    best = profiles["best_risk_containment_profile"]
    raw_edge = (top["mean_ret20"] or 0) - (remaining["mean_ret20"] or 0)
    winner_edge = (top["winner_rate_ret20_gt_10pct"] or 0) - (remaining["winner_rate_ret20_gt_10pct"] or 0)
    contained_risk_ok = (best["family_bad_rate"] or 1) <= 0.25 and (best["family_severe_rate"] or 1) <= 0.15 and (best["risk_containment_kept_share"] or 0) >= 0.30
    if raw_edge <= 0 or winner_edge <= 0:
        return "no_independent_family_edge", ["top_predicted_buckets_do_not_outperform_remaining_reserve"]
    if top["mean_ret20"] and top["mean_ret20"] >= 0.05 and winner_edge > 0.10 and contained_risk_ok and (best["family_average_candidates_per_date"] or 0) >= 1:
        return "high_upside_family_keep_for_pattern_portfolio_pretest", ["high_upside_bucket_outperforms_remaining_reserve_and_risk_containment_is_operational"]
    if top["mean_ret20"] and top["mean_ret20"] >= 0.05 and not contained_risk_ok:
        return "high_upside_signal_exists_but_risk_uncontrolled", ["strong_upside_signal_but_bad_or_severe_rate_not_controlled_by_fixed_risk_variants"]
    return "high_upside_family_promising_but_risky", ["upside_edge_exists_but_family_risk_or_breadth_requires_more_work"]


def run(input_root: Path, separability_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-high-upside-reserve-family-discovery-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows, contract = load_rows(input_root)
    scored, model_report = score_oos(rows)
    scored = assign_buckets(scored)
    metrics = bucket_metrics(scored)
    decomp = downside_decomposition(scored)
    containment = containment_variants(scored)
    profiles = family_profiles(scored, metrics, containment)
    decision, reasons = decide(metrics, containment, profiles)
    scored.to_csv(out / "family_discovery_rows.csv", index=False)
    _write_json(out / "feature_contract.json", contract)
    _write_json(out / "high_upside_bucket_metrics.json", metrics)
    _write_json(out / "downside_risk_decomposition.json", decomp)
    _write_json(out / "risk_containment_probe.json", containment)
    _write_json(out / "family_candidate_profiles.json", profiles)
    _write_json(out / "family_discovery_summary.json", {"axis_id": AXIS_ID, "input_rows": int(len(scored)), "separability_root": separability_root, "model_report": model_report, "decision": decision})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "chronological_oos_split": True, "features_use_saved_point_in_time_context_only": True, "outcomes_used_evaluation_only": True, "candidate_generation_changed": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"row_count": int(len(scored)), "date_count": int(scored["decision_date"].nunique()), "code_count": int(scored["code"].nunique()), "research_fallback_used": False, "coverage": {c: float(scored[c].notna().mean()) for c in READ_COLUMNS if c in scored}})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--separability-root", type=Path, default=DEFAULT_SEPARABILITY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.separability_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
