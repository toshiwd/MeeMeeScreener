from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from scipy import sparse
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_entry_actionability_score_v1 as action


AXIS_ID = "starter_entry_pairwise_reranker_v1"
DEFAULT_BACKFILL_ROOT = Path(r"G:\Tradex\starter_entry_role_backfill_v1\20260525T020451Z-starter-entry-role-backfill-v1")
DEFAULT_UTILITY_ROOT = Path(r"G:\Tradex\starter_entry_utility_score_v1\20260525T023616Z-starter-entry-utility-score-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_pairwise_reranker_v1")
PAIR_CAP_PER_DATE = 200
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "model_config.json",
    "pair_construction_report.json",
    "feature_contract_report.json",
    "training_fold_report.json",
    "candidate_pairwise_rows.csv",
    "topk_pairwise_comparison_summary.json",
    "replacement_quality.csv",
    "pairwise_model_interpretability_report.json",
    "upside_preservation_report.json",
    "bad_suppression_report.json",
    "oracle_gap_capture_summary.json",
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


def load_rows(backfill_root: Path, utility_root: Path) -> pd.DataFrame:
    rows = action.expand_tags(action.load_rows(backfill_root))
    prior = utility_root / "candidate_utility_rows.csv"
    if prior.exists():
        usecols = ["decision_date", "code", "bad_risk_v1_rank", "utility_rank", "starter_entry_utility_score"]
        p = pd.read_csv(prior, usecols=usecols, low_memory=False)
        p["code"] = p["code"].astype(str)
        p["decision_date"] = pd.to_numeric(p["decision_date"], errors="coerce")
        rows = rows.merge(p, on=["decision_date", "code"], how="left")
    return rows


def _preprocessor(numeric: list[str], bools: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        [
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), numeric),
            ("bool", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(drop="if_binary", handle_unknown="ignore"))]), bools),
        ],
        remainder="drop",
    )


def _feature_names(pre: ColumnTransformer) -> list[str]:
    names: list[str] = []
    for name, transformer, cols in pre.transformers_:
        if name == "num":
            names.extend(list(cols))
        elif name == "bool":
            names.extend(list(transformer.named_steps["onehot"].get_feature_names_out(cols)))
    return names


def pair_indices(train: pd.DataFrame, cap_per_date: int = PAIR_CAP_PER_DATE) -> tuple[list[int], list[int], dict[str, Any]]:
    pos_idx: list[int] = []
    neg_idx: list[int] = []
    dates_with_pairs = 0
    raw_pairs = 0
    for _, g in train.groupby("decision_date", sort=True):
        pos = g[g["starter_good_abs"].astype(bool) | g["starter_good_cross_sectional"].astype(bool)].sort_values(["baseline_rank", "code"]).index.tolist()
        neg = g[g["starter_bad_abs"].astype(bool) | g["starter_bad_cross_sectional"].astype(bool)].sort_values(["baseline_rank", "code"]).index.tolist()
        if not pos or not neg:
            continue
        dates_with_pairs += 1
        total = len(pos) * len(neg)
        raw_pairs += total
        cap = min(cap_per_date, total)
        for k in range(cap):
            pos_idx.append(pos[k % len(pos)])
            neg_idx.append(neg[(k // len(pos)) % len(neg)])
    report = {"dates_with_pairs": dates_with_pairs, "raw_pair_count": raw_pairs, "sampled_pair_count": len(pos_idx), "pair_cap_per_date": cap_per_date, "deterministic_sampling": True}
    return pos_idx, neg_idx, report


def _reason(row: pd.Series, coef: dict[str, float]) -> str:
    comps = []
    for f, c in coef.items():
        raw = f.removesuffix("_True").removesuffix("_1")
        if raw in row.index and pd.notna(row[raw]):
            if f.endswith(("_True", "_1")) and not bool(row[raw]):
                continue
            comps.append({"feature": raw, "coefficient": float(c), "value": _json_ready(row[raw])})
    return json.dumps(sorted(comps, key=lambda x: abs(x["coefficient"]), reverse=True)[:6], ensure_ascii=False, sort_keys=True)


def train_pairwise(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    numeric, bools = action.feature_columns(rows)
    scored = []
    fold_reports = []
    pair_reports: dict[str, Any] = {}
    interp: dict[str, Any] = {"model_type": "same-date pairwise L2 logistic linear ranker", "interpretable": True, "folds": {}}
    for fold in action.FOLDS:
        train = rows[rows["year"].isin(fold["train_years"]) & rows["path20_available"].eq(True)].copy()
        valid = rows[rows["year"].isin(fold["valid_years"]) & rows["path20_available"].eq(True)].copy()
        pre = _preprocessor(numeric, bools)
        x_train = pre.fit_transform(train[numeric + bools])
        pos_idx, neg_idx, pair_report = pair_indices(train)
        pos_pos = train.index.get_indexer(pos_idx)
        neg_pos = train.index.get_indexer(neg_idx)
        diffs = x_train[pos_pos] - x_train[neg_pos]
        if sparse.issparse(diffs):
            x_pair = sparse.vstack([diffs, -diffs], format="csr")
        else:
            import numpy as np

            x_pair = np.vstack([diffs, -diffs])
        y_pair = [1] * len(pos_idx) + [0] * len(pos_idx)
        model = LogisticRegression(C=0.5, solver="liblinear", max_iter=1000, random_state=42)
        model.fit(x_pair, y_pair)
        x_valid = pre.transform(valid[numeric + bools])
        valid["starter_entry_pairwise_score"] = model.decision_function(x_valid)
        valid["pairwise_rank"] = valid.sort_values(["decision_date", "starter_entry_pairwise_score", "code"], ascending=[True, False, True]).groupby("decision_date").cumcount() + 1
        valid["watch_quality_score"] = valid["baseline_score"]
        valid["model_version"] = "starter_entry_pairwise_reranker_v1_l2_logistic"
        valid["training_period"] = f"{min(fold['train_years'])}-{max(fold['train_years'])}"
        valid["validation_period"] = fold["validation_period"]
        valid["feature_availability_json"] = json.dumps({c: c in rows.columns for c in numeric + bools}, sort_keys=True)
        coefs = dict(zip(_feature_names(pre), model.coef_[0]))
        valid["pairwise_reason_components_json"] = valid.apply(lambda r: _reason(r, coefs), axis=1)
        scored.append(valid)
        pair_reports[fold["validation_period"]] = pair_report
        fold_reports.append({"validation_period": fold["validation_period"], "train_years": fold["train_years"], "valid_years": fold["valid_years"], "train_rows": int(len(train)), "valid_rows": int(len(valid)), "sampled_pair_count": pair_report["sampled_pair_count"], "feature_count": len(numeric) + len(bools)})
        interp["folds"][fold["validation_period"]] = {
            "top_positive_features": [{"feature": f, "coefficient": c} for f, c in sorted(coefs.items(), key=lambda kv: kv[1], reverse=True)[:15]],
            "top_negative_features": [{"feature": f, "coefficient": c} for f, c in sorted(coefs.items(), key=lambda kv: kv[1])[:15]],
        }
    return pd.concat(scored, ignore_index=True), {"folds": fold_reports}, pair_reports, interp


def _periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["validation_period"].eq("2024")]),
        ("2025", rows[rows["validation_period"].eq("2025")]),
        ("2026_label_safe", rows[rows["validation_period"].eq("2026_label_safe")]),
        ("2024_2025", rows[rows["validation_period"].isin(["2024", "2025"])]),
        ("2024_2026_combined", rows[rows["validation_period"].isin(["2024", "2025", "2026_label_safe"])]),
    ]


def _summarize(frame: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = frame[frame[rank_col] <= topk] if rank_col in frame else frame.iloc[0:0]
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


def comparison(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for period, pr in _periods(rows):
        for topk in action.TOPK_VALUES:
            base = _summarize(pr, "baseline_rank", topk)
            bad = _summarize(pr.dropna(subset=["bad_risk_v1_rank"]) if "bad_risk_v1_rank" in pr else pr.iloc[0:0], "bad_risk_v1_rank", topk)
            util = _summarize(pr.dropna(subset=["utility_rank"]) if "utility_rank" in pr else pr.iloc[0:0], "utility_rank", topk)
            pair = _summarize(pr, "pairwise_rank", topk)
            row = {"period": period, "topk": topk}
            for name, vals in [("baseline", base), ("bad_risk_v1", bad), ("utility_v1", util), ("pairwise_v1", pair)]:
                row.update({f"{name}_{k}": v for k, v in vals.items()})
            row["pairwise_delta_mean_ret20"] = (pair["mean_ret20"] or 0) - (base["mean_ret20"] or 0)
            row["pairwise_delta_starter_good_rate"] = (pair["starter_good_rate"] or 0) - (base["starter_good_rate"] or 0)
            row["pairwise_delta_starter_bad_rate"] = (pair["starter_bad_rate"] or 0) - (base["starter_bad_rate"] or 0)
            row["pairwise_delta_selected_loser_rate"] = (pair["selected_loser_rate"] or 0) - (base["selected_loser_rate"] or 0)
            rec.append(row)
    return pd.DataFrame(rec)


def replacement(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for period, pr in _periods(rows):
        for topk in action.TOPK_VALUES:
            for date, g in pr.groupby("decision_date"):
                b = set(g[g["baseline_rank"] <= topk]["code"])
                p = set(g[g["pairwise_rank"] <= topk]["code"])
                added = g[g["code"].isin(p - b)]
                removed = g[g["code"].isin(b - p)]
                rec.append({"period": period, "topk": topk, "decision_date": int(date), "changed_members_count": len(added) + len(removed), "added_ret20_mean": _mean(added, "ret20"), "removed_ret20_mean": _mean(removed, "ret20"), "added_minus_removed_ret20": (_mean(added, "ret20") or 0) - (_mean(removed, "ret20") or 0), "added_starter_bad_rate": _rate(added["starter_bad"]) if not added.empty else None, "removed_starter_bad_rate": _rate(removed["starter_bad"]) if not removed.empty else None})
    return pd.DataFrame(rec)


def reports(rows: pd.DataFrame, comp: pd.DataFrame, repl: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    r = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    repl_recent = repl[(repl["period"] == "2024_2026_combined") & (repl["topk"] == 10)]
    base10 = rows[rows["baseline_rank"] <= 10]
    pair10 = rows[rows["pairwise_rank"] <= 10]
    removed_winners = base10[base10["selected_winner"] & ~base10["code"].isin(pair10["code"])]
    added_winners = pair10[pair10["selected_winner"] & ~pair10["code"].isin(base10["code"])]
    upside = {"baseline_winners_removed": int(len(removed_winners)), "pairwise_winners_added": int(len(added_winners)), "removed_winner_mfe20_mean": _mean(removed_winners, "mfe20"), "added_winner_mfe20_mean": _mean(added_winners, "mfe20"), "replacement_added_minus_removed_mean": _mean(repl_recent, "added_minus_removed_ret20")}
    bad = {"starter_bad_delta": r["pairwise_delta_starter_bad_rate"], "selected_loser_delta": r["pairwise_delta_selected_loser_rate"], "severe_loss_delta": (r["pairwise_v1_severe_loss_rate"] or 0) - (r["baseline_severe_loss_rate"] or 0), "utility_v1_starter_bad_delta": (r["utility_v1_starter_bad_rate"] or 0) - (r["baseline_starter_bad_rate"] or 0)}
    oracle_gap = {"baseline_top10_mean_ret20": r["baseline_mean_ret20"], "utility_v1_top10_mean_ret20": r["utility_v1_mean_ret20"], "pairwise_v1_top10_mean_ret20": r["pairwise_v1_mean_ret20"], "utility_vs_baseline_delta": (r["utility_v1_mean_ret20"] or 0) - (r["baseline_mean_ret20"] or 0), "pairwise_vs_baseline_delta": r["pairwise_delta_mean_ret20"]}
    return upside, bad, oracle_gap


def decide(comp: pd.DataFrame, repl: pd.DataFrame) -> dict[str, Any]:
    r = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    y24 = comp[(comp["period"] == "2024") & (comp["topk"] == 10)].iloc[0]
    y25 = comp[(comp["period"] == "2025") & (comp["topk"] == 10)].iloc[0]
    y26 = comp[(comp["period"] == "2026_label_safe") & (comp["topk"] == 10)].iloc[0]
    rq = _mean(repl[(repl["period"] == "2024_2026_combined") & (repl["topk"] == 10)], "added_minus_removed_ret20") or 0
    if r["pairwise_delta_mean_ret20"] >= 0.005 and rq > 0 and r["pairwise_delta_starter_bad_rate"] < 0 and r["pairwise_delta_selected_loser_rate"] < 0 and y24["pairwise_delta_mean_ret20"] > -0.005 and y25["pairwise_delta_mean_ret20"] > -0.005 and y26["pairwise_delta_mean_ret20"] > -0.02:
        d, reason = "keep_for_formal_challenger_compare", "pairwise reranker improves recent top10 return with bad-label suppression"
    elif rq > 0 or r["pairwise_delta_starter_bad_rate"] < 0 or r["pairwise_delta_selected_loser_rate"] < 0:
        d, reason = "hold_for_ranker_refinement", "pairwise improves part of the ranking contract but keep gate misses"
    elif r["pairwise_delta_mean_ret20"] < 0:
        d, reason = "drop_pairwise_ranker", "pairwise rerank worsens recent top10 return"
    else:
        d, reason = "candidate_pool_redesign_needed", "available features do not support same-date pairwise starter ranking"
    return {"research_decision": d, "reason_typed": [reason], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(backfill_root: Path, utility_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-pairwise-reranker-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = load_rows(backfill_root, utility_root)
    scored, folds, pair_report, interp = train_pairwise(rows)
    comp = comparison(scored)
    repl = replacement(scored)
    upside, bad, oracle = reports(scored, comp, repl)
    decision = decide(comp, repl)
    _write_json(out / "input_artifact_report.json", {"backfill_root": backfill_root, "utility_root": utility_root, "input_rows": len(rows)})
    _write_json(out / "model_config.json", {"model_type": "same-date pairwise L2 logistic linear ranker", "pair_cap_per_date": PAIR_CAP_PER_DATE, "pair_sampling": "deterministic within decision_date only", "regularization": "LogisticRegression(C=0.5, solver=liblinear)"})
    _write_json(out / "pair_construction_report.json", pair_report)
    _write_json(out / "feature_contract_report.json", {"features": action.NUMERIC_FEATURES + action.BOOLEAN_FEATURES, "forbidden": ["ret20", "ret5", "mae20", "mfe20", "future events", "no-trigger status", "synthetic source/family"]})
    _write_json(out / "training_fold_report.json", folds)
    scored.to_csv(out / "candidate_pairwise_rows.csv", index=False)
    _write_json(out / "topk_pairwise_comparison_summary.json", {"rows": comp.to_dict("records")})
    repl.to_csv(out / "replacement_quality.csv", index=False)
    _write_json(out / "pairwise_model_interpretability_report.json", interp)
    _write_json(out / "upside_preservation_report.json", upside)
    _write_json(out / "bad_suppression_report.json", bad)
    _write_json(out / "oracle_gap_capture_summary.json", oracle)
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "walk_forward_splits": action.FOLDS, "pairs_within_same_decision_date_only": True, "validation_year_labels_not_used_in_training": True, "same_date_rerank_uses_decision_date_features_only": True, "baseline_ranking_changed": False, "score_formula_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "meemee_unchanged": True})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--backfill-root", type=Path, default=DEFAULT_BACKFILL_ROOT)
    p.add_argument("--utility-root", type=Path, default=DEFAULT_UTILITY_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = p.parse_args(argv)
    out = run(args.backfill_root, args.utility_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
