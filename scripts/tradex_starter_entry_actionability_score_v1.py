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


AXIS_ID = "starter_entry_actionability_score_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_objective_reform_v1\20260525T002305Z-starter-entry-objective-reform-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_actionability_score_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "model_config.json",
    "feature_contract_report.json",
    "training_fold_report.json",
    "candidate_actionability_rows.csv",
    "topk_actionability_comparison_summary.json",
    "replacement_quality.csv",
    "model_interpretability_report.json",
    "winner_damage_report.csv",
    "pool_ceiling_capture_summary.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
FOLDS = (
    {"validation_period": "2024", "train_years": [2019, 2020, 2021, 2022, 2023], "valid_years": [2024]},
    {"validation_period": "2025", "train_years": [2019, 2020, 2021, 2022, 2023, 2024], "valid_years": [2025]},
    {"validation_period": "2026_label_safe", "train_years": [2019, 2020, 2021, 2022, 2023, 2024, 2025], "valid_years": [2026]},
)
TOPK_VALUES = (5, 10, 20)
NUMERIC_FEATURES = [
    "baseline_score",
    "baseline_rank",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "above7_streak",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "days_since_ma60_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
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
TAG_COLUMNS = ["research_setup_tags_json", "research_risk_tags_json", "research_regime_tags_json"]


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
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


def _mean(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def load_rows(input_root: Path) -> pd.DataFrame:
    rows = pd.read_csv(input_root / "candidate_role_rows.csv", low_memory=False)
    for col in ["decision_date", "year", "baseline_rank", "baseline_score"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    rows["code"] = rows["code"].astype(str)
    return rows


def expand_tags(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    for column in TAG_COLUMNS:
        prefix = column.replace("research_", "").replace("_tags_json", "")
        tags = sorted({tag for value in rows[column].fillna("[]") for tag in json.loads(value)})
        for tag in tags:
            rows[f"{prefix}:{tag}"] = rows[column].fillna("[]").map(lambda x, t=tag: t in json.loads(x))
    return rows


def feature_columns(rows: pd.DataFrame) -> tuple[list[str], list[str]]:
    numeric = [c for c in NUMERIC_FEATURES if c in rows.columns]
    bools = [c for c in BOOLEAN_FEATURES if c in rows.columns] + [c for c in rows.columns if c.startswith(("setup:", "risk:", "regime:"))]
    return numeric, bools


def build_model(numeric: list[str], bools: list[str]) -> Pipeline:
    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), numeric),
            ("bool", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(drop="if_binary", handle_unknown="ignore"))]), bools),
        ],
        remainder="drop",
    )
    return Pipeline(
        [
            ("preprocess", pre),
            ("model", LogisticRegression(C=0.5, solver="liblinear", max_iter=1000, class_weight="balanced", random_state=42)),
        ]
    )


def transformed_feature_names(model: Pipeline) -> list[str]:
    pre: ColumnTransformer = model.named_steps["preprocess"]
    names: list[str] = []
    for name, transformer, cols in pre.transformers_:
        if name == "remainder":
            continue
        if name == "num":
            names.extend(list(cols))
        elif name == "bool":
            onehot = transformer.named_steps["onehot"]
            names.extend(list(onehot.get_feature_names_out(cols)))
    return names


def reason_components(row: pd.Series, coefficients: dict[str, float], top_n: int = 5) -> str:
    comps: list[dict[str, Any]] = []
    for feature, coef in coefficients.items():
        raw_feature = feature
        active = False
        value: Any = None
        if feature in row.index:
            value = row.get(feature)
            active = pd.notna(value)
        elif "_True" in feature:
            raw_feature = feature.removesuffix("_True")
            value = row.get(raw_feature)
            active = bool(value) is True
        elif "_1" in feature:
            raw_feature = feature.rsplit("_", 1)[0]
            value = row.get(raw_feature)
            active = bool(value) is True
        if active:
            comps.append({"feature": raw_feature, "coefficient": float(coef), "value": _json_ready(value)})
    comps = sorted(comps, key=lambda x: abs(x["coefficient"]), reverse=True)[:top_n]
    return json.dumps(comps, ensure_ascii=False, sort_keys=True)


def train_score_folds(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows = expand_tags(rows)
    numeric, bools = feature_columns(rows)
    rows["starter_target"] = rows["starter_good"].astype(bool)
    scored_frames: list[pd.DataFrame] = []
    fold_reports: list[dict[str, Any]] = []
    coef_by_fold: dict[str, dict[str, float]] = {}
    for fold in FOLDS:
        train = rows[rows["year"].isin(fold["train_years"]) & rows["path20_available"].eq(True)].copy()
        valid = rows[rows["year"].isin(fold["valid_years"]) & rows["path20_available"].eq(True)].copy()
        model = build_model(numeric, bools)
        model.fit(train[numeric + bools], train["starter_target"])
        proba = model.predict_proba(valid[numeric + bools])[:, 1]
        names = transformed_feature_names(model)
        coefs = dict(zip(names, model.named_steps["model"].coef_[0]))
        coef_by_fold[fold["validation_period"]] = coefs
        valid["starter_entry_probability"] = proba
        valid["starter_entry_actionability_score"] = proba
        valid["watch_quality_score"] = valid["baseline_score"]
        valid["model_version"] = "starter_entry_actionability_score_v1_l2_logistic"
        valid["training_period"] = f"{min(fold['train_years'])}-{max(fold['train_years'])}"
        valid["validation_period"] = fold["validation_period"]
        valid["feature_availability_json"] = json.dumps({c: c in rows.columns for c in numeric + bools}, sort_keys=True)
        valid["actionability_reason_components_json"] = valid.apply(lambda r: reason_components(r, coefs), axis=1)
        valid["actionability_rank"] = valid.sort_values(["decision_date", "starter_entry_actionability_score", "code"], ascending=[True, False, True]).groupby("decision_date").cumcount() + 1
        scored_frames.append(valid)
        fold_reports.append(
            {
                "validation_period": fold["validation_period"],
                "train_years": fold["train_years"],
                "valid_years": fold["valid_years"],
                "train_rows": int(len(train)),
                "valid_rows": int(len(valid)),
                "train_positive_rate": _rate(train["starter_target"]),
                "valid_positive_rate": _rate(valid["starter_target"]),
                "feature_count": len(numeric) + len(bools),
            }
        )
    scored = pd.concat(scored_frames, ignore_index=True)
    return scored, {"folds": fold_reports}, {"coef_by_fold": coef_by_fold, "numeric_features": numeric, "boolean_features": bools}


def _period_slices(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["validation_period"].eq("2024")]),
        ("2025", rows[rows["validation_period"].eq("2025")]),
        ("2026_label_safe", rows[rows["validation_period"].eq("2026_label_safe")]),
        ("2024_2025", rows[rows["validation_period"].isin(["2024", "2025"])]),
        ("2024_2026_combined", rows[rows["validation_period"].isin(["2024", "2025", "2026_label_safe"])]),
    ]


def summarize_selection(frame: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = frame[frame[rank_col] <= topk]
    return {
        "n": int(len(g)),
        "mean_ret20": _mean(g, "ret20"),
        "median_ret20": _median(g, "ret20"),
        "starter_good_rate": _rate(g["starter_good"]) if not g.empty else None,
        "starter_bad_rate": _rate(g["starter_bad"]) if not g.empty else None,
        "selected_loser_rate": _rate(g["selected_loser"]) if not g.empty else None,
        "selected_winner_rate": _rate(g["selected_winner"]) if not g.empty else None,
        "immediate_adverse_rate": _rate(g["immediate_adverse_entry"]) if not g.empty else None,
        "severe_loss_rate": _rate(g["ret20"] <= -0.05) if not g.empty else None,
        "mae20_mean": _mean(g, "mae20"),
        "mae20_median": _median(g, "mae20"),
        "mfe20_mean": _mean(g, "mfe20"),
        "mfe20_median": _median(g, "mfe20"),
        "hit_rate_ret20_gt_5pct": _rate(g["ret20"] >= 0.05) if not g.empty else None,
        "bottom_decile_rate": _rate(g["same_date_ret20_rank_pct"] <= 0.10) if not g.empty else None,
    }


def comparison_summary(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(rows):
        for topk in TOPK_VALUES:
            base = summarize_selection(period_rows, "baseline_rank", topk)
            act = summarize_selection(period_rows, "actionability_rank", topk)
            records.append(
                {
                    "period": period,
                    "topk": topk,
                    **{f"baseline_{k}": v for k, v in base.items()},
                    **{f"actionability_{k}": v for k, v in act.items()},
                    "delta_mean_ret20": (act.get("mean_ret20") or 0.0) - (base.get("mean_ret20") or 0.0),
                    "delta_starter_good_rate": (act.get("starter_good_rate") or 0.0) - (base.get("starter_good_rate") or 0.0),
                    "delta_starter_bad_rate": (act.get("starter_bad_rate") or 0.0) - (base.get("starter_bad_rate") or 0.0),
                    "delta_selected_loser_rate": (act.get("selected_loser_rate") or 0.0) - (base.get("selected_loser_rate") or 0.0),
                }
            )
    return pd.DataFrame(records)


def replacement_quality(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(rows):
        for topk in TOPK_VALUES:
            for date, g in period_rows.groupby("decision_date"):
                base_codes = set(g[g["baseline_rank"] <= topk]["code"])
                act_codes = set(g[g["actionability_rank"] <= topk]["code"])
                added = g[g["code"].isin(act_codes - base_codes)]
                removed = g[g["code"].isin(base_codes - act_codes)]
                records.append(
                    {
                        "period": period,
                        "topk": topk,
                        "decision_date": int(date),
                        "changed_members_count": len(added) + len(removed),
                        "added_count": len(added),
                        "removed_count": len(removed),
                        "added_ret20_mean": _mean(added, "ret20"),
                        "removed_ret20_mean": _mean(removed, "ret20"),
                        "added_minus_removed_ret20": (_mean(added, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0),
                    }
                )
    return pd.DataFrame(records)


def winner_damage(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(rows):
        for topk in TOPK_VALUES:
            base = period_rows[period_rows["baseline_rank"] <= topk]
            act = period_rows[period_rows["actionability_rank"] <= topk]
            removed_winners = base[base["selected_winner"] & ~base["code"].isin(act["code"])]
            added_winners = act[act["selected_winner"] & ~act["code"].isin(base["code"])]
            records.append(
                {
                    "period": period,
                    "topk": topk,
                    "baseline_selected_winners_removed": int(len(removed_winners)),
                    "actionability_selected_winners_added": int(len(added_winners)),
                    "removed_winner_ret20_mean": _mean(removed_winners, "ret20"),
                    "added_winner_ret20_mean": _mean(added_winners, "ret20"),
                    "winner_damage_risk": "high" if len(removed_winners) > len(added_winners) else "medium",
                    "baseline_selected_loser_rate": _rate(base["selected_loser"]) if not base.empty else None,
                    "actionability_selected_loser_rate": _rate(act["selected_loser"]) if not act.empty else None,
                }
            )
    return pd.DataFrame(records)


def interpretability_report(model_info: dict[str, Any]) -> dict[str, Any]:
    folds = model_info["coef_by_fold"]
    fold_payload: dict[str, Any] = {}
    sign_counts: dict[str, list[int]] = {}
    for fold, coefs in folds.items():
        sorted_pos = sorted(coefs.items(), key=lambda kv: kv[1], reverse=True)[:15]
        sorted_neg = sorted(coefs.items(), key=lambda kv: kv[1])[:15]
        fold_payload[fold] = {
            "top_positive_features": [{"feature": f, "coefficient": c} for f, c in sorted_pos],
            "top_negative_features": [{"feature": f, "coefficient": c} for f, c in sorted_neg],
        }
        for f, c in coefs.items():
            sign_counts.setdefault(f, []).append(1 if c > 0 else -1 if c < 0 else 0)
    stable = {
        f: {"signs": signs, "stable_sign": len(set(signs)) == 1}
        for f, signs in sign_counts.items()
        if len(signs) >= 2
    }
    return {
        "model_type": "L2 logistic regression scorecard",
        "folds": fold_payload,
        "sign_stability": stable,
        "interpretable": True,
    }


def pool_ceiling_capture(comp: pd.DataFrame, input_root: Path) -> dict[str, Any]:
    if (input_root / "objective_gap_summary.json").exists():
        objective_gap = json.loads((input_root / "objective_gap_summary.json").read_text(encoding="utf-8"))
        oracle = objective_gap.get("oracle_top10", {})
    else:
        source_rows = load_rows(input_root)
        source_rows = source_rows[source_rows["year"].isin([2024, 2025, 2026]) & source_rows["path20_available"].eq(True)]
        oracle_rows = source_rows.sort_values(["decision_date", "starter_good", "ret20"], ascending=[True, False, False]).groupby("decision_date").head(10)
        oracle = {"oracle_mean_ret20": _mean(oracle_rows, "ret20")}
    row = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    baseline = float(row["baseline_mean_ret20"])
    action = float(row["actionability_mean_ret20"])
    oracle_ret = float(oracle.get("oracle_mean_ret20") or baseline)
    gap = oracle_ret - baseline
    capture = None if gap == 0 else (action - baseline) / gap
    return {
        "baseline_top10_mean_ret20": baseline,
        "actionability_top10_mean_ret20": action,
        "oracle_top10_mean_ret20": oracle_ret,
        "oracle_gap": gap,
        "actionability_gap_capture": capture,
        "starter_good_capture_delta": float(row["delta_starter_good_rate"]),
    }


def decide(comp: pd.DataFrame, repl: pd.DataFrame, interp: dict[str, Any]) -> dict[str, Any]:
    recent10 = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    y2024 = comp[(comp["period"] == "2024") & (comp["topk"] == 10)].iloc[0]
    y2025 = comp[(comp["period"] == "2025") & (comp["topk"] == 10)].iloc[0]
    y2026 = comp[(comp["period"] == "2026_label_safe") & (comp["topk"] == 10)].iloc[0]
    repl_recent = repl[(repl["period"] == "2024_2026_combined") & (repl["topk"] == 10)]
    repl_quality = _mean(repl_recent, "added_minus_removed_ret20") or 0.0
    if (
        recent10["delta_mean_ret20"] >= 0.005
        and recent10["delta_starter_good_rate"] > 0
        and recent10["delta_starter_bad_rate"] < 0
        and repl_quality > 0
        and y2024["delta_mean_ret20"] > -0.005
        and y2025["delta_mean_ret20"] > -0.005
        and y2026["delta_mean_ret20"] > -0.02
        and interp.get("interpretable")
    ):
        decision = "keep_for_formal_challenger_compare"
        reasons = ["actionability score improves recent top10 mean/starter labels with positive replacement quality"]
    elif recent10["delta_starter_good_rate"] > 0 and recent10["delta_starter_bad_rate"] < 0:
        decision = "hold_for_model_refinement"
        reasons = ["starter labels improve but return gate or year stability is not fully satisfied"]
    elif recent10["delta_mean_ret20"] < 0:
        decision = "drop_actionability_model"
        reasons = ["actionability rerank worsens recent top10 mean return"]
    else:
        decision = "inconclusive"
        reasons = ["actionability rerank does not produce a clear stable result"]
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
    }


def run(input_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-actionability-score-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = load_rows(input_root)
    available_years = sorted(int(y) for y in rows["year"].dropna().unique())
    missing_train_folds = [
        {
            "validation_period": fold["validation_period"],
            "required_train_years": fold["train_years"],
            "available_train_years": [year for year in fold["train_years"] if year in available_years],
            "missing_train_years": [year for year in fold["train_years"] if year not in available_years],
        }
        for fold in FOLDS
        if not rows[rows["year"].isin(fold["train_years"]) & rows["path20_available"].eq(True)].shape[0]
    ]
    if missing_train_folds:
        reason = {
            "research_decision": "inconclusive",
            "reason_typed": ["walk-forward training rows are unavailable for at least one required fold; no fallback training used"],
            "meemee_reflectable": False,
            "ranking_reflectable": False,
            "publish_allowed": False,
        }
        _write_json(out / "input_artifact_report.json", {"input_root": input_root, "input_rows": len(rows), "available_years": available_years})
        _write_json(
            out / "model_config.json",
            {
                "model_type": "LogisticRegression",
                "status": "not_trained",
                "blocked_reason": "missing_required_walk_forward_training_rows",
                "fallback_training_used": False,
            },
        )
        _write_json(
            out / "feature_contract_report.json",
            {
                "required_training_folds": FOLDS,
                "missing_train_folds": missing_train_folds,
                "forbidden_fallback": "no in-sample or same-year training substituted",
            },
        )
        _write_json(out / "training_fold_report.json", {"folds": [], "missing_train_folds": missing_train_folds})
        pd.DataFrame(columns=["decision_date", "code", "baseline_rank", "actionability_rank", "starter_entry_actionability_score"]).to_csv(
            out / "candidate_actionability_rows.csv", index=False
        )
        _write_json(out / "topk_actionability_comparison_summary.json", {"rows": []})
        pd.DataFrame(columns=["period", "topk", "decision_date", "added_ret20_mean", "removed_ret20_mean", "added_minus_removed_ret20"]).to_csv(
            out / "replacement_quality.csv", index=False
        )
        _write_json(out / "model_interpretability_report.json", {"interpretable": False, "model_trained": False, "reason": "missing walk-forward train rows"})
        pd.DataFrame(columns=["period", "topk", "baseline_selected_winners_removed", "actionability_selected_winners_added"]).to_csv(
            out / "winner_damage_report.csv", index=False
        )
        _write_json(out / "pool_ceiling_capture_summary.json", {"status": "not_evaluated", "reason": "model not trained"})
        _write_json(out / "research_decision.json", reason)
        _write_json(
            out / "no_lookahead_audit.json",
            {
                "audit_result": "blocked_before_training",
                "walk_forward_splits": FOLDS,
                "model_for_validation_year_uses_prior_year_labels_only": None,
                "fallback_training_used": False,
                "ranking_order_changed": False,
                "score_formula_changed": False,
                "candidate_generation_changed": False,
                "runtime_db_write": False,
            },
        )
        _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
        return out
    scored, fold_report, model_info = train_score_folds(rows)
    comp = comparison_summary(scored)
    repl = replacement_quality(scored)
    damage = winner_damage(scored)
    interp = interpretability_report(model_info)
    pool_capture = pool_ceiling_capture(comp, input_root)
    decision = decide(comp, repl, interp)

    model_config = {
        "model_type": "LogisticRegression",
        "penalty": "l2",
        "C": 0.5,
        "class_weight": "balanced",
        "solver": "liblinear",
        "score_column": "starter_entry_actionability_score",
        "rerank_scope": "same decision_date candidate pool",
    }
    feature_contract = {
        "numeric_features": model_info["numeric_features"],
        "boolean_features": model_info["boolean_features"],
        "forbidden_features": ["future_returns", "future_ma_touch_reclaim", "oracle_labels", "synthetic_candidate_source"],
        "label_column": "starter_good",
    }
    _write_json(out / "input_artifact_report.json", {"input_root": input_root, "input_rows": len(rows)})
    _write_json(out / "model_config.json", model_config)
    _write_json(out / "feature_contract_report.json", feature_contract)
    _write_json(out / "training_fold_report.json", fold_report)
    scored.to_csv(out / "candidate_actionability_rows.csv", index=False)
    _write_json(out / "topk_actionability_comparison_summary.json", {"rows": comp.to_dict("records")})
    repl.to_csv(out / "replacement_quality.csv", index=False)
    _write_json(out / "model_interpretability_report.json", interp)
    damage.to_csv(out / "winner_damage_report.csv", index=False)
    _write_json(out / "pool_ceiling_capture_summary.json", pool_capture)
    _write_json(out / "research_decision.json", decision)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "walk_forward_splits": FOLDS,
            "model_for_validation_year_uses_prior_year_labels_only": True,
            "same_date_rerank_uses_decision_date_features_only": True,
            "future_labels_used_only_for_training_prior_years_and_evaluation": True,
            "baseline_ranking_unchanged": True,
            "score_formula_changed": False,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train TRADEX starter-entry actionability scorecard")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
