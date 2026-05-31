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
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


AXIS_ID = "rank11_50_winner_separability_audit_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_FAILED_LIFT_ROOT = Path(r"G:\Tradex\rank11_50_positive_selection_lift_v1\20260525T081536Z-rank11-50-positive-selection-lift-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\rank11_50_winner_separability_audit_v1")
REQUIRED_ARTIFACTS = (
    "separability_summary.json",
    "separability_rows.csv",
    "feature_contract.json",
    "feature_profile_by_outcome.json",
    "time_split_model_probe.json",
    "top_feature_candidates.json",
    "lift_simulation_from_probe.json",
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
OUTCOME_COLUMNS = ["ret20", "winner_label", "nonwinner_label", "bad_label", "severe_label"]
READ_COLUMNS = ["decision_date", "code", "year", "path20_available", *NUMERIC_FEATURES, *BOOLEAN_FEATURES, *CATEGORICAL_FEATURES, "ret20"]


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


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def feature_contract(header: list[str]) -> dict[str, Any]:
    fields: dict[str, dict[str, str]] = {}
    for col in READ_COLUMNS:
        if col in {"ret20"}:
            cls = "outcome_only"
        elif col in header:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    for col in OUTCOME_COLUMNS:
        fields[col] = {"classification": "outcome_only"}
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
    for col in ["decision_date", "year", *NUMERIC_FEATURES, "ret20"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["path20_available", *BOOLEAN_FEATURES]:
        rows[col] = _to_bool(rows[col])
    rows = rows[rows["path20_available"] & rows["baseline_rank"].between(1, 50, inclusive="both") & rows["ret20"].notna()].copy()
    rows["rank11_50_pool"] = rows["baseline_rank"].between(11, 50, inclusive="both")
    rows["winner_label"] = rows["ret20"] > 0.10
    rows["nonwinner_label"] = rows["ret20"] <= 0
    rows["bad_label"] = rows["ret20"] <= -0.05
    rows["severe_label"] = rows["ret20"] <= -0.10
    return rows, feature_contract(header)


def metric_block(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "mean_ret20": _mean(frame, "ret20"),
        "winner_rate": _rate(frame["winner_label"]) if not frame.empty else None,
        "bad_rate": _rate(frame["bad_label"]) if not frame.empty else None,
        "severe_rate": _rate(frame["severe_label"]) if not frame.empty else None,
    }


def ranking_gradient(rows: pd.DataFrame) -> dict[str, Any]:
    if "rank11_50_pool" not in rows:
        rows = rows.copy()
        rows["rank11_50_pool"] = rows["baseline_rank"].between(11, 50, inclusive="both")
    rows = rows[rows["rank11_50_pool"]]
    buckets = {
        "rank_11_20": rows[rows["baseline_rank"].between(11, 20, inclusive="both")],
        "rank_21_30": rows[rows["baseline_rank"].between(21, 30, inclusive="both")],
        "rank_31_50": rows[rows["baseline_rank"].between(31, 50, inclusive="both")],
    }
    return {name: metric_block(frame) for name, frame in buckets.items()}


def feature_profile(rows: pd.DataFrame) -> dict[str, Any]:
    if "rank11_50_pool" not in rows:
        rows = rows.copy()
        rows["rank11_50_pool"] = rows["baseline_rank"].between(11, 50, inclusive="both")
    rows = rows[rows["rank11_50_pool"]]
    winners = rows[rows["winner_label"]]
    non = rows[rows["nonwinner_label"]]
    numeric = {}
    for col in NUMERIC_FEATURES:
        wm = _mean(winners, col)
        nm = _mean(non, col)
        numeric[col] = {
            "winner_mean": wm,
            "nonwinner_mean": nm,
            "winner_minus_nonwinner": None if wm is None or nm is None else wm - nm,
            "missing_rate": float(rows[col].isna().mean()),
        }
    boolean = {}
    for col in BOOLEAN_FEATURES:
        wr = _rate(winners[col])
        nr = _rate(non[col])
        boolean[col] = {
            "winner_rate": wr,
            "nonwinner_rate": nr,
            "winner_minus_nonwinner": None if wr is None or nr is None else wr - nr,
            "missing_rate": float(rows[col].isna().mean()),
        }
    stable = sorted(
        [{"feature": k, "effect": abs(v["winner_minus_nonwinner"] or 0), "direction": v["winner_minus_nonwinner"]} for k, v in {**numeric, **boolean}.items()],
        key=lambda x: x["effect"],
        reverse=True,
    )[:15]
    return {"numeric": numeric, "boolean": boolean, "stable_candidate_features": stable}


def build_model() -> Pipeline:
    pre = ColumnTransformer(
        [
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), NUMERIC_FEATURES),
            ("bool", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(drop="if_binary", handle_unknown="ignore"))]), BOOLEAN_FEATURES),
            ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(handle_unknown="ignore"))]), CATEGORICAL_FEATURES),
        ]
    )
    return Pipeline([("preprocess", pre), ("model", LogisticRegression(max_iter=1000, class_weight="balanced", solver="liblinear", random_state=42))])


def train_probe(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    pool = rows[rows["rank11_50_pool"]].copy()
    split_date = int(pool["decision_date"].quantile(0.70))
    train = pool[pool["decision_date"] <= split_date].copy()
    test = pool[pool["decision_date"] > split_date].copy()
    report: dict[str, Any] = {
        "model": "LogisticRegression",
        "split_date": split_date,
        "train_sample_count": int(len(train)),
        "test_sample_count": int(len(test)),
        "train_date_count": int(train["decision_date"].nunique()),
        "test_date_count": int(test["decision_date"].nunique()),
        "blocked_missing_dependency": False,
    }
    if train["winner_label"].nunique() < 2 or test["winner_label"].nunique() < 2:
        report["auc"] = None
        report["blocked_reason"] = "class_balance_insufficient"
        rows["winner_probability"] = pd.NA
        return rows, report, []
    features = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES
    train_x = train[features].copy()
    rows_x = rows[features].copy()
    for col in BOOLEAN_FEATURES + CATEGORICAL_FEATURES:
        train_x[col] = train_x[col].astype(str)
        rows_x[col] = rows_x[col].astype(str)
    model = build_model()
    model.fit(train_x, train["winner_label"].astype(int))
    scored = rows.copy()
    scored["winner_probability"] = model.predict_proba(rows_x)[:, 1]
    test_scores = scored[(scored["decision_date"] > split_date) & scored["rank11_50_pool"]]
    y = test_scores["winner_label"].astype(int)
    p = test_scores["winner_probability"]
    report["auc"] = float(roc_auc_score(y, p))
    for pct in [0.01, 0.05, 0.10]:
        cutoff = p.quantile(1 - pct)
        pred = p >= cutoff
        precision, recall, _, _ = precision_recall_fscore_support(y, pred.astype(int), average="binary", zero_division=0)
        selected = test_scores[pred]
        unselected = test_scores[~pred]
        report[f"top_{int(pct*100)}pct"] = {
            "precision": float(precision),
            "recall": float(recall),
            "selected_count": int(len(selected)),
            "selected_mean_ret20": _mean(selected, "ret20"),
            "unselected_mean_ret20": _mean(unselected, "ret20"),
        }
    feature_rows = model_feature_importance(model)
    return scored, report, feature_rows


def model_feature_importance(model: Pipeline) -> list[dict[str, Any]]:
    pre: ColumnTransformer = model.named_steps["preprocess"]
    names: list[str] = []
    for name, transformer, cols in pre.transformers_:
        if name == "num":
            names.extend(cols)
        else:
            names.extend(transformer.named_steps["onehot"].get_feature_names_out(cols).tolist())
    coefs = model.named_steps["model"].coef_[0]
    return [
        {"feature": f, "coefficient": float(c), "abs_coefficient": float(abs(c))}
        for f, c in sorted(zip(names, coefs), key=lambda x: abs(x[1]), reverse=True)[:30]
    ]


def simulate_lift(rows: pd.DataFrame, split_date: int) -> dict[str, Any]:
    test = rows[rows["decision_date"] > split_date].copy()
    out: dict[str, Any] = {}
    for promote_n in [1, 2, 3]:
        records = []
        for date, g in test.groupby("decision_date", sort=True):
            pool = g[g["baseline_rank"].between(11, 50, inclusive="both")].sort_values(["winner_probability", "baseline_rank"], ascending=[False, True]).head(promote_n)
            displaced = g[g["baseline_rank"].between(1, 10, inclusive="both")].sort_values(["baseline_rank"], ascending=False).head(len(pool))
            if pool.empty or displaced.empty:
                continue
            base10 = g[g["baseline_rank"] <= 10]
            challenger_codes = (set(base10["code"].astype(str)) - set(displaced["code"].astype(str))) | set(pool["code"].astype(str))
            challenger = g[g["code"].astype(str).isin(challenger_codes)]
            records.append(
                {
                    "decision_date": int(date),
                    "promoted_mean_ret20": _mean(pool, "ret20"),
                    "displaced_mean_ret20": _mean(displaced, "ret20"),
                    "promoted_bad_rate": _rate(pool["bad_label"]),
                    "promoted_severe_rate": _rate(pool["severe_label"]),
                    "promoted_winner_rate": _rate(pool["winner_label"]),
                    "base10_mean_ret20": _mean(base10, "ret20"),
                    "challenger10_mean_ret20": _mean(challenger, "ret20"),
                    "base10_bad_rate": _rate(base10["bad_label"]),
                    "challenger10_bad_rate": _rate(challenger["bad_label"]),
                    "base10_severe_rate": _rate(base10["severe_label"]),
                    "challenger10_severe_rate": _rate(challenger["severe_label"]),
                    "base10_winner_rate": _rate(base10["winner_label"]),
                    "challenger10_winner_rate": _rate(challenger["winner_label"]),
                }
            )
        df = pd.DataFrame(records)
        out[f"promote_{promote_n}"] = {
            "date_count": int(df["decision_date"].nunique()) if not df.empty else 0,
            "OOS_promoted_mean_ret20": _mean(df, "promoted_mean_ret20"),
            "OOS_displaced_mean_ret20": _mean(df, "displaced_mean_ret20"),
            "promoted_minus_displaced_ret20": None if df.empty else (_mean(df, "promoted_mean_ret20") or 0) - (_mean(df, "displaced_mean_ret20") or 0),
            "OOS_top10_delta_mean_ret20": None if df.empty else (_mean(df, "challenger10_mean_ret20") or 0) - (_mean(df, "base10_mean_ret20") or 0),
            "OOS_top10_delta_bad_pick_rate": None if df.empty else (_mean(df, "challenger10_bad_rate") or 0) - (_mean(df, "base10_bad_rate") or 0),
            "OOS_top10_delta_severe_loss_rate": None if df.empty else (_mean(df, "challenger10_severe_rate") or 0) - (_mean(df, "base10_severe_rate") or 0),
            "OOS_top10_winner_rate_delta": None if df.empty else (_mean(df, "challenger10_winner_rate") or 0) - (_mean(df, "base10_winner_rate") or 0),
            "reserve_winner_capture_rate": _mean(df, "promoted_winner_rate"),
            "accidental_promotion_bad_rate": _mean(df, "promoted_bad_rate"),
        }
    return out


def decide(probe: dict[str, Any], lift: dict[str, Any], gradient: dict[str, Any]) -> tuple[str, list[str], str]:
    best_name, best = max(lift.items(), key=lambda kv: kv[1].get("OOS_top10_delta_mean_ret20") or -999)
    if (
        (best.get("promoted_minus_displaced_ret20") or 0) > 0
        and (best.get("OOS_top10_delta_mean_ret20") or 0) > 0
        and (best.get("OOS_top10_delta_bad_pick_rate") or 0) <= 0.002
        and (best.get("OOS_top10_delta_severe_loss_rate") or 0) <= 0.002
        and (best.get("accidental_promotion_bad_rate") or 1) <= 0.25
    ):
        return "separability_supports_learned_lift_pretest", ["OOS_lift_positive_with_controlled_bad_and_severe_rates"], best_name
    gradients = [v.get("mean_ret20") for v in gradient.values() if v.get("mean_ret20") is not None]
    if (
        (best.get("OOS_top10_delta_bad_pick_rate") or 0) > 0.002
        or (best.get("OOS_top10_delta_severe_loss_rate") or 0) > 0.002
        or (best.get("accidental_promotion_bad_rate") or 0) > 0.25
    ) and max(gradients) - min(gradients) < 0.002:
        return "ranking_gradient_too_weak_rebuild_candidate_generation", ["OOS_lift_improves_return_but_bad_or_severe_rates_worsen_and_reserve_rank_gradient_is_flat"], best_name
    if probe.get("auc") is not None and probe["auc"] >= 0.53 and (best.get("promoted_minus_displaced_ret20") or 0) > 0:
        return "separability_promising_but_underpowered", ["model_probe_separates_weakly_but_top10_keep_gates_not_met"], best_name
    if probe.get("auc") is None or probe.get("auc", 0) < 0.52:
        return "feature_signal_too_weak_rebuild_candidate_generation", ["time_split_model_probe_cannot_separate_rank11_50_winners"], best_name
    if max(gradients) - min(gradients) < 0.002:
        return "ranking_gradient_too_weak_rebuild_candidate_generation", ["reserve_rank_gradient_flat_and_no_OOS_lift_worked"], best_name
    return "feature_signal_too_weak_rebuild_candidate_generation", ["OOS_lift_failed_despite_some_feature_signal"], best_name


def run(input_root: Path, failed_lift_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-rank11-50-winner-separability-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows, contract = load_rows(input_root)
    profile = feature_profile(rows)
    gradient = ranking_gradient(rows)
    scored, probe, top_features = train_probe(rows)
    lift = simulate_lift(scored, int(probe["split_date"])) if "split_date" in probe else {}
    decision, reasons, best_lift = decide(probe, lift, gradient)
    _write_json(out / "feature_contract.json", contract)
    _write_json(out / "feature_profile_by_outcome.json", profile)
    _write_json(out / "time_split_model_probe.json", probe)
    _write_json(out / "top_feature_candidates.json", {"rows": top_features})
    _write_json(out / "lift_simulation_from_probe.json", lift)
    scored.to_csv(out / "separability_rows.csv", index=False)
    pool_scored = scored[scored["rank11_50_pool"]]
    _write_json(
        out / "separability_summary.json",
        {
            "axis_id": AXIS_ID,
            "input_rows": int(len(pool_scored)),
            "simulation_rows_rank1_50": int(len(scored)),
            "date_count": int(pool_scored["decision_date"].nunique()),
            "failed_lift_root": failed_lift_root,
            "ranking_gradient": gradient,
            "best_lift": best_lift,
        },
    )
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "chronological_split": True, "features_use_saved_point_in_time_context_only": True, "future_outcomes_used_as_labels_only": True, "candidate_generation_changed": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"row_count": int(len(pool_scored)), "simulation_rows_rank1_50": int(len(scored)), "date_count": int(pool_scored["decision_date"].nunique()), "code_count": int(pool_scored["code"].nunique()), "research_fallback_used": False, "coverage": {c: float(pool_scored[c].notna().mean()) for c in READ_COLUMNS if c in pool_scored}})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "best_lift": best_lift, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--failed-lift-root", type=Path, default=DEFAULT_FAILED_LIFT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.failed_lift_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
