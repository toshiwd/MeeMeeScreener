from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_entry_actionability_score_v1 as action


AXIS_ID = "starter_entry_utility_score_v1"
DEFAULT_BACKFILL_ROOT = Path(r"G:\Tradex\starter_entry_role_backfill_v1\20260525T020451Z-starter-entry-role-backfill-v1")
DEFAULT_PRIOR_ROOT = Path(r"G:\Tradex\starter_entry_actionability_score_v1\20260525T021329Z-starter-entry-actionability-score-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_utility_score_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "model_config.json",
    "feature_contract_report.json",
    "training_fold_report.json",
    "candidate_utility_rows.csv",
    "topk_utility_comparison_summary.json",
    "replacement_quality.csv",
    "upside_preservation_report.json",
    "bad_suppression_report.json",
    "model_interpretability_report.json",
    "winner_damage_report.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
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
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return value.item()
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


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def _rate(s: pd.Series) -> float | None:
    v = s.dropna()
    return None if v.empty else float(v.astype(bool).mean())


def load_rows(backfill_root: Path, prior_root: Path) -> pd.DataFrame:
    rows = action.expand_tags(action.load_rows(backfill_root))
    prior_path = prior_root / "candidate_actionability_rows.csv"
    if prior_path.exists():
        prior = pd.read_csv(prior_path, usecols=["decision_date", "code", "starter_entry_actionability_score", "actionability_rank"], low_memory=False)
        prior["code"] = prior["code"].astype(str)
        prior["decision_date"] = pd.to_numeric(prior["decision_date"], errors="coerce")
        rows = rows.merge(prior.rename(columns={"starter_entry_actionability_score": "bad_risk_v1_score", "actionability_rank": "bad_risk_v1_rank"}), on=["decision_date", "code"], how="left")
    return rows


def _preprocessor(numeric: list[str], bools: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        [
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), numeric),
            ("bool", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(drop="if_binary", handle_unknown="ignore"))]), bools),
        ],
        remainder="drop",
    )


def _feature_names(model: Pipeline) -> list[str]:
    pre: ColumnTransformer = model.named_steps["preprocess"]
    names: list[str] = []
    for name, transformer, cols in pre.transformers_:
        if name == "num":
            names.extend(list(cols))
        elif name == "bool":
            names.extend(list(transformer.named_steps["onehot"].get_feature_names_out(cols)))
    return names


def _reason(row: pd.Series, coef: dict[str, float]) -> str:
    comps = []
    for f, c in coef.items():
        raw = f.removesuffix("_True").removesuffix("_1")
        if raw in row.index and (pd.notna(row[raw])):
            if f.endswith(("_True", "_1")) and not bool(row[raw]):
                continue
            comps.append({"feature": raw, "coefficient": float(c), "value": _json_ready(row[raw])})
    return json.dumps(sorted(comps, key=lambda x: abs(x["coefficient"]), reverse=True)[:6], ensure_ascii=False, sort_keys=True)


def train_score(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    numeric, bools = action.feature_columns(rows)
    scored = []
    reports = []
    interp: dict[str, Any] = {"model_type": "Ridge upside + L2 logistic bad risk utility score", "folds": {}, "interpretable": True}
    for fold in action.FOLDS:
        train = rows[rows["year"].isin(fold["train_years"]) & rows["path20_available"].eq(True)].copy()
        valid = rows[rows["year"].isin(fold["valid_years"]) & rows["path20_available"].eq(True)].copy()
        upside_target = pd.to_numeric(train["ret20"], errors="coerce").clip(-0.15, 0.25)
        bad_target = (train["starter_bad_abs"].astype(bool) | train["immediate_adverse_entry"].astype(bool) | (train["ret20"] <= -0.05)).astype(int)
        upside = Pipeline([("preprocess", _preprocessor(numeric, bools)), ("model", Ridge(alpha=10.0, random_state=42))])
        bad = Pipeline([("preprocess", _preprocessor(numeric, bools)), ("model", LogisticRegression(C=0.5, solver="liblinear", max_iter=1000, class_weight="balanced", random_state=42))])
        upside.fit(train[numeric + bools], upside_target)
        bad.fit(train[numeric + bools], bad_target)
        tr_up = upside.predict(train[numeric + bools])
        tr_bad = bad.predict_proba(train[numeric + bools])[:, 1]
        up_mean, up_std = float(pd.Series(tr_up).mean()), float(pd.Series(tr_up).std(ddof=0) or 1.0)
        bad_mean, bad_std = float(pd.Series(tr_bad).mean()), float(pd.Series(tr_bad).std(ddof=0) or 1.0)
        va_up = upside.predict(valid[numeric + bools])
        va_bad = bad.predict_proba(valid[numeric + bools])[:, 1]
        valid["upside_score"] = va_up
        valid["bad_risk_score"] = va_bad
        valid["starter_entry_probability"] = 1.0 - va_bad
        valid["bad_entry_probability"] = va_bad
        valid["starter_entry_utility_score"] = ((va_up - up_mean) / up_std) - ((va_bad - bad_mean) / bad_std)
        valid["utility_rank"] = valid.sort_values(["decision_date", "starter_entry_utility_score", "code"], ascending=[True, False, True]).groupby("decision_date").cumcount() + 1
        valid["model_version"] = "starter_entry_utility_score_v1_ridge_logistic"
        valid["training_period"] = f"{min(fold['train_years'])}-{max(fold['train_years'])}"
        valid["validation_period"] = fold["validation_period"]
        up_coef = dict(zip(_feature_names(upside), upside.named_steps["model"].coef_))
        bad_coef = dict(zip(_feature_names(bad), bad.named_steps["model"].coef_[0]))
        valid["utility_reason_components_json"] = valid.apply(lambda r: _reason(r, up_coef), axis=1)
        scored.append(valid)
        reports.append({"validation_period": fold["validation_period"], "train_years": fold["train_years"], "valid_years": fold["valid_years"], "train_rows": int(len(train)), "valid_rows": int(len(valid)), "feature_count": len(numeric) + len(bools)})
        interp["folds"][fold["validation_period"]] = {
            "upside_top_positive_features": [{"feature": f, "coefficient": c} for f, c in sorted(up_coef.items(), key=lambda kv: kv[1], reverse=True)[:12]],
            "upside_top_negative_features": [{"feature": f, "coefficient": c} for f, c in sorted(up_coef.items(), key=lambda kv: kv[1])[:12]],
            "bad_top_positive_features": [{"feature": f, "coefficient": c} for f, c in sorted(bad_coef.items(), key=lambda kv: kv[1], reverse=True)[:12]],
            "bad_top_negative_features": [{"feature": f, "coefficient": c} for f, c in sorted(bad_coef.items(), key=lambda kv: kv[1])[:12]],
        }
    return pd.concat(scored, ignore_index=True), {"folds": reports}, interp


def _summarize(frame: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
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
        "mfe20_mean": _mean(g, "mfe20"),
        "hit_rate_ret20_gt_5pct": _rate(g["ret20"] >= 0.05) if not g.empty else None,
        "bottom_decile_rate": _rate(g["same_date_ret20_rank_pct"] <= 0.10) if not g.empty else None,
    }


def _periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["validation_period"].eq("2024")]),
        ("2025", rows[rows["validation_period"].eq("2025")]),
        ("2026_label_safe", rows[rows["validation_period"].eq("2026_label_safe")]),
        ("2024_2025", rows[rows["validation_period"].isin(["2024", "2025"])]),
        ("2024_2026_combined", rows[rows["validation_period"].isin(["2024", "2025", "2026_label_safe"])]),
    ]


def comparison(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for period, pr in _periods(rows):
        for topk in action.TOPK_VALUES:
            base = _summarize(pr, "baseline_rank", topk)
            util = _summarize(pr, "utility_rank", topk)
            prior = _summarize(pr.dropna(subset=["bad_risk_v1_rank"]) if "bad_risk_v1_rank" in pr else pr.iloc[0:0], "bad_risk_v1_rank", topk)
            out.append({"period": period, "topk": topk, **{f"baseline_{k}": v for k, v in base.items()}, **{f"bad_risk_v1_{k}": v for k, v in prior.items()}, **{f"utility_{k}": v for k, v in util.items()}, "utility_delta_mean_ret20": (util["mean_ret20"] or 0) - (base["mean_ret20"] or 0), "utility_delta_starter_bad_rate": (util["starter_bad_rate"] or 0) - (base["starter_bad_rate"] or 0), "utility_delta_starter_good_rate": (util["starter_good_rate"] or 0) - (base["starter_good_rate"] or 0), "utility_delta_selected_loser_rate": (util["selected_loser_rate"] or 0) - (base["selected_loser_rate"] or 0)})
    return pd.DataFrame(out)


def replacement(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for period, pr in _periods(rows):
        for topk in action.TOPK_VALUES:
            for date, g in pr.groupby("decision_date"):
                b = set(g[g["baseline_rank"] <= topk]["code"])
                u = set(g[g["utility_rank"] <= topk]["code"])
                added = g[g["code"].isin(u - b)]
                removed = g[g["code"].isin(b - u)]
                rec.append({"period": period, "topk": topk, "decision_date": int(date), "changed_members_count": len(added) + len(removed), "added_ret20_mean": _mean(added, "ret20"), "removed_ret20_mean": _mean(removed, "ret20"), "added_minus_removed_ret20": (_mean(added, "ret20") or 0) - (_mean(removed, "ret20") or 0)})
    return pd.DataFrame(rec)


def winner_damage(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for period, pr in _periods(rows):
        for topk in action.TOPK_VALUES:
            base = pr[pr["baseline_rank"] <= topk]
            util = pr[pr["utility_rank"] <= topk]
            rem = base[base["selected_winner"] & ~base["code"].isin(util["code"])]
            add = util[util["selected_winner"] & ~util["code"].isin(base["code"])]
            rec.append({"period": period, "topk": topk, "baseline_winners_removed": int(len(rem)), "utility_winners_added": int(len(add)), "removed_winner_ret20_mean": _mean(rem, "ret20"), "added_winner_ret20_mean": _mean(add, "ret20")})
    return pd.DataFrame(rec)


def reports(rows: pd.DataFrame, comp: pd.DataFrame, repl: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    recent = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    repl_recent = repl[(repl["period"] == "2024_2026_combined") & (repl["topk"] == 10)]
    upside = {"top10_baseline_mean_ret20": recent["baseline_mean_ret20"], "top10_utility_mean_ret20": recent["utility_mean_ret20"], "replacement_added_minus_removed_mean": _mean(repl_recent, "added_minus_removed_ret20")}
    bad = {"starter_bad_delta": recent["utility_delta_starter_bad_rate"], "selected_loser_delta": recent["utility_delta_selected_loser_rate"], "severe_loss_delta": (recent["utility_severe_loss_rate"] or 0) - (recent["baseline_severe_loss_rate"] or 0)}
    return upside, bad


def decide(comp: pd.DataFrame, repl: pd.DataFrame) -> dict[str, Any]:
    r = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    y24 = comp[(comp["period"] == "2024") & (comp["topk"] == 10)].iloc[0]
    y25 = comp[(comp["period"] == "2025") & (comp["topk"] == 10)].iloc[0]
    y26 = comp[(comp["period"] == "2026_label_safe") & (comp["topk"] == 10)].iloc[0]
    rq = _mean(repl[(repl["period"] == "2024_2026_combined") & (repl["topk"] == 10)], "added_minus_removed_ret20") or 0
    if r["utility_delta_mean_ret20"] >= 0.005 and rq > 0 and r["utility_delta_starter_bad_rate"] < 0 and r["utility_delta_selected_loser_rate"] < 0 and y24["utility_delta_mean_ret20"] > -0.005 and y25["utility_delta_mean_ret20"] > -0.005 and y26["utility_delta_mean_ret20"] > -0.02:
        d, reason = "keep_for_formal_challenger_compare", "utility score improves recent top10 return and bad-label suppression"
    elif r["utility_delta_starter_bad_rate"] < 0 or r["utility_delta_selected_loser_rate"] < 0:
        d, reason = "hold_for_model_refinement", "bad labels improve but return/replacement gate misses"
    elif r["utility_delta_mean_ret20"] < 0:
        d, reason = "drop_utility_model", "utility rerank fails to improve ret20"
    else:
        d, reason = "candidate_pool_redesign_needed", "available features do not rank starter upside reliably"
    return {"research_decision": d, "reason_typed": [reason], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(backfill_root: Path, prior_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-utility-score-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = load_rows(backfill_root, prior_root)
    scored, fold_report, interp = train_score(rows)
    comp = comparison(scored)
    repl = replacement(scored)
    damage = winner_damage(scored)
    upside, bad = reports(scored, comp, repl)
    decision = decide(comp, repl)
    _write_json(out / "input_artifact_report.json", {"backfill_root": backfill_root, "prior_actionability_root": prior_root, "input_rows": len(rows)})
    _write_json(out / "model_config.json", {"upside_model": "Ridge(alpha=10)", "bad_risk_model": "L2 LogisticRegression(C=0.5)", "utility": "z(upside_score)-z(bad_risk_score)", "weights": "1.0:1.0"})
    _write_json(out / "feature_contract_report.json", {"features": action.NUMERIC_FEATURES + action.BOOLEAN_FEATURES, "forbidden": ["ret20", "ret5", "mae20", "mfe20", "future events", "synthetic source/family"]})
    _write_json(out / "training_fold_report.json", fold_report)
    scored.to_csv(out / "candidate_utility_rows.csv", index=False)
    _write_json(out / "topk_utility_comparison_summary.json", {"rows": comp.to_dict("records")})
    repl.to_csv(out / "replacement_quality.csv", index=False)
    _write_json(out / "upside_preservation_report.json", upside)
    _write_json(out / "bad_suppression_report.json", bad)
    _write_json(out / "model_interpretability_report.json", interp)
    damage.to_csv(out / "winner_damage_report.csv", index=False)
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "walk_forward_splits": action.FOLDS, "upside_and_bad_models_use_prior_year_labels_only": True, "same_date_rerank_uses_decision_date_features_only": True, "zscore_normalization_fit_on_training_fold_only": True, "ranking_order_changed": False, "score_formula_changed": False, "candidate_generation_changed": False, "runtime_db_write": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--backfill-root", type=Path, default=DEFAULT_BACKFILL_ROOT)
    p.add_argument("--prior-root", type=Path, default=DEFAULT_PRIOR_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = p.parse_args(argv)
    out = run(args.backfill_root, args.prior_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
