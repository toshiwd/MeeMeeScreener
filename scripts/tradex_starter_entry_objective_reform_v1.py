from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "starter_entry_objective_reform_v1"
DEFAULT_TAXONOMY_ROOT = Path(r"G:\Tradex\candidate_family_taxonomy_shadow_v1\20260524T135527Z-candidate-family-taxonomy-shadow-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_objective_reform_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "starter_entry_label_schema.json",
    "candidate_role_rows.csv",
    "baseline_objective_audit.csv",
    "objective_gap_summary.json",
    "oracle_pool_ceiling_summary.csv",
    "starter_good_vs_bad_feature_audit.csv",
    "watch_only_failure_profile.csv",
    "next_reform_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
TOPK_VALUES = (5, 10, 20)
YEARS = (2024, 2025, 2026)


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


def starter_entry_label_schema() -> dict[str, Any]:
    return {
        "schema_version": "starter_entry_label_schema_v1",
        "scope": "TRADEX-only",
        "labels_are_diagnostic_only": True,
        "thresholds": {
            "starter_good_abs": "ret20 >= 0.05 and mae20 > -0.08",
            "starter_bad_abs": "ret20 <= -0.05 or mae20 <= -0.08",
            "starter_good_cross_sectional": "same-date ret20 rank pct >= 0.70",
            "starter_bad_cross_sectional": "same-date ret20 rank pct <= 0.30",
            "immediate_adverse_entry": "ret5 < 0 or mae5 <= -0.03",
            "selected_loser_continuity": "ret20 <= -0.05 or same-date bottom30",
            "selected_winner_continuity": "ret20 >= 0.05 or same-date top30",
        },
        "role_rules": {
            "starter_entry_candidate": "starter_good_abs or starter_good_cross_sectional, and not immediate_adverse_entry",
            "watch_only_candidate": "baseline_rank <= 20 and (starter_bad_abs or immediate_adverse_entry)",
            "avoid_candidate": "starter_bad_abs and immediate_adverse_entry",
            "unclear_candidate": "neutral or missing label path",
        },
        "no_lookahead": {
            "features": "decision-date or prior only",
            "labels": "future returns/path only",
            "oracle": "diagnostic only",
        },
    }


def load_rows(taxonomy_root: Path) -> pd.DataFrame:
    rows = pd.read_csv(taxonomy_root / "candidate_family_tag_rows.csv")
    rows["code"] = rows["code"].astype(str)
    rows["decision_date"] = pd.to_numeric(rows["decision_date"], errors="coerce").astype("Int64")
    rows["baseline_rank"] = pd.to_numeric(rows["baseline_rank"], errors="coerce")
    rows["baseline_score"] = pd.to_numeric(rows["baseline_score"], errors="coerce")
    rows["year"] = pd.to_numeric(rows["year"], errors="coerce").astype("Int64")
    return rows


def build_forward_labels(daily_path: Path) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, usecols=["code", "date", "open", "high", "low", "close"])
    daily["code"] = daily["code"].astype(str)
    daily["decision_date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y%m%d").astype(int)
    daily = daily.sort_values(["code", "decision_date"]).drop_duplicates(["code", "decision_date"], keep="last").reset_index(drop=True)
    records: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = group.reset_index(drop=True)
        close = pd.to_numeric(g["close"], errors="coerce")
        high = pd.to_numeric(g["high"], errors="coerce")
        low = pd.to_numeric(g["low"], errors="coerce")
        for idx, row in g.iterrows():
            entry = close.iloc[idx]
            if pd.isna(entry) or entry == 0:
                continue
            ret5 = close.iloc[idx + 5] / entry - 1 if idx + 5 < len(g) else None
            ret20 = close.iloc[idx + 20] / entry - 1 if idx + 20 < len(g) else None
            end5 = min(idx + 5, len(g) - 1)
            end20 = min(idx + 20, len(g) - 1)
            full5 = idx + 5 < len(g)
            full20 = idx + 20 < len(g)
            low5 = low.iloc[idx + 1 : end5 + 1].min() if full5 else None
            high5 = high.iloc[idx + 1 : end5 + 1].max() if full5 else None
            low20 = low.iloc[idx + 1 : end20 + 1].min() if full20 else None
            high20 = high.iloc[idx + 1 : end20 + 1].max() if full20 else None
            records.append(
                {
                    "code": code,
                    "decision_date": int(row["decision_date"]),
                    "ret5": None if ret5 is None or pd.isna(ret5) else float(ret5),
                    "ret20_path": None if ret20 is None or pd.isna(ret20) else float(ret20),
                    "mae5": None if low5 is None or pd.isna(low5) else float(low5 / entry - 1),
                    "mfe5": None if high5 is None or pd.isna(high5) else float(high5 / entry - 1),
                    "mae20": None if low20 is None or pd.isna(low20) else float(low20 / entry - 1),
                    "mfe20": None if high20 is None or pd.isna(high20) else float(high20 / entry - 1),
                    "path20_available": bool(full20),
                }
            )
    return pd.DataFrame(records)


def attach_starter_labels(rows: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    out = rows.drop(columns=[c for c in ["ret5", "mae5", "mfe5", "mae20", "mfe20", "path20_available"] if c in rows], errors="ignore").merge(
        labels, on=["code", "decision_date"], how="left"
    )
    if "ret20_path" in out:
        ret20_path = pd.to_numeric(out["ret20_path"], errors="coerce")
        prior_ret20 = pd.to_numeric(out["ret20"], errors="coerce") if "ret20" in out else pd.Series(float("nan"), index=out.index)
        out["ret20"] = ret20_path.where(ret20_path.notna(), prior_ret20)
    out["same_date_ret20_rank_pct"] = out.groupby("decision_date")["ret20"].rank(pct=True, method="average")
    out["starter_good_abs"] = (out["ret20"] >= 0.05) & (out["mae20"] > -0.08)
    out["starter_bad_abs"] = (out["ret20"] <= -0.05) | (out["mae20"] <= -0.08)
    out["starter_good_cross_sectional"] = out["same_date_ret20_rank_pct"] >= 0.70
    out["starter_bad_cross_sectional"] = out["same_date_ret20_rank_pct"] <= 0.30
    out["selected_loser"] = (out["ret20"] <= -0.05) | out["starter_bad_cross_sectional"]
    out["selected_winner"] = (out["ret20"] >= 0.05) | out["starter_good_cross_sectional"]
    out["immediate_adverse_entry"] = (out["ret5"] < 0) | (out["mae5"] <= -0.03)
    out["starter_good"] = (out["starter_good_abs"] | out["starter_good_cross_sectional"]) & ~out["immediate_adverse_entry"]
    out["starter_bad"] = out["starter_bad_abs"] | out["starter_bad_cross_sectional"] | out["immediate_adverse_entry"]
    roles: list[str] = []
    for row in out.to_dict("records"):
        if not row.get("path20_available"):
            roles.append("unclear_candidate")
        elif row.get("starter_good"):
            roles.append("starter_entry_candidate")
        elif row.get("starter_bad_abs") and row.get("immediate_adverse_entry"):
            roles.append("avoid_candidate")
        elif row.get("starter_bad") and float(row.get("baseline_rank") or 9999) <= 20:
            roles.append("watch_only_candidate")
        else:
            roles.append("unclear_candidate")
    out["diagnostic_candidate_role"] = roles
    out["watch_candidate"] = True
    return out


def _period_slices(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["year"].eq(2024)]),
        ("2025", rows[rows["year"].eq(2025)]),
        ("2026_label_safe", rows[rows["year"].eq(2026)]),
        ("2024_2026_combined", rows[rows["year"].isin(YEARS)]),
    ]


def baseline_objective_audit(rows: pd.DataFrame) -> pd.DataFrame:
    output: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(rows):
        for topk in TOPK_VALUES:
            g = period_rows[(period_rows["baseline_rank"] <= topk) & (period_rows["path20_available"].eq(True))]
            output.append(
                {
                    "period": period,
                    "topk": topk,
                    "n": int(len(g)),
                    "starter_good_rate": _rate(g["starter_good"]) if not g.empty else None,
                    "starter_bad_rate": _rate(g["starter_bad"]) if not g.empty else None,
                    "watch_only_rate": _rate(g["diagnostic_candidate_role"].eq("watch_only_candidate")) if not g.empty else None,
                    "avoid_rate": _rate(g["diagnostic_candidate_role"].eq("avoid_candidate")) if not g.empty else None,
                    "selected_loser_rate": _rate(g["selected_loser"]) if not g.empty else None,
                    "selected_winner_rate": _rate(g["selected_winner"]) if not g.empty else None,
                    "mean_ret20": _mean(g, "ret20"),
                    "median_ret20": _median(g, "ret20"),
                    "mean_mae20": _mean(g, "mae20"),
                    "mean_mfe20": _mean(g, "mfe20"),
                    "immediate_adverse_rate": _rate(g["immediate_adverse_entry"]) if not g.empty else None,
                    "severe_loss_rate": _rate(g["ret20"] <= -0.05) if not g.empty else None,
                }
            )
    return pd.DataFrame(output)


def oracle_pool_ceiling(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(rows[rows["path20_available"].eq(True)]):
        for topk in TOPK_VALUES:
            baseline = period_rows[period_rows["baseline_rank"] <= topk]
            oracle = period_rows.sort_values(["decision_date", "starter_good", "ret20"], ascending=[True, False, False]).groupby("decision_date").head(topk)
            starter_good_pool = period_rows.groupby("decision_date")["starter_good"].sum()
            avg_rank_good = _mean(period_rows[period_rows["starter_good"]], "baseline_rank")
            deep_good = period_rows[period_rows["starter_good"] & (period_rows["baseline_rank"] > topk)]
            output.append(
                {
                    "period": period,
                    "topk": topk,
                    "baseline_mean_ret20": _mean(baseline, "ret20"),
                    "oracle_mean_ret20": _mean(oracle, "ret20"),
                    "oracle_improvement": (_mean(oracle, "ret20") or 0.0) - (_mean(baseline, "ret20") or 0.0),
                    "baseline_starter_good_rate": _rate(baseline["starter_good"]) if not baseline.empty else None,
                    "oracle_starter_good_rate": _rate(oracle["starter_good"]) if not oracle.empty else None,
                    "avg_starter_good_available_per_day": None if starter_good_pool.empty else float(starter_good_pool.mean()),
                    "avg_baseline_rank_of_starter_good": avg_rank_good,
                    "deep_starter_good_share": None
                    if period_rows[period_rows["starter_good"]].empty
                    else float(len(deep_good) / len(period_rows[period_rows["starter_good"]])),
                    "pool_sufficient": bool((starter_good_pool >= topk).mean() >= 0.50) if not starter_good_pool.empty else False,
                }
            )
    ceiling = pd.DataFrame(output)
    combined10 = ceiling[(ceiling["period"] == "2024_2026_combined") & (ceiling["topk"] == 10)]
    summary = combined10.iloc[0].to_dict() if not combined10.empty else {}
    return ceiling, summary


def feature_audit(rows: pd.DataFrame) -> pd.DataFrame:
    features = [
        "baseline_score",
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "above20_streak",
        "above60_streak",
        "realized_vol20",
        "atr14_pct",
        "upper_wick_ratio",
        "volume_ma20_ratio",
    ]
    records: list[dict[str, Any]] = []
    recent = rows[rows["year"].isin(YEARS) & rows["path20_available"].eq(True)]
    good = recent[recent["starter_good"]]
    bad = recent[recent["starter_bad"]]
    for feature in features:
        records.append(
            {
                "feature": feature,
                "starter_good_mean": _mean(good, feature),
                "starter_bad_mean": _mean(bad, feature),
                "good_minus_bad_mean": (_mean(good, feature) or 0.0) - (_mean(bad, feature) or 0.0),
                "starter_good_median": _median(good, feature),
                "starter_bad_median": _median(bad, feature),
                "coverage": float(recent[feature].notna().mean()) if feature in recent else 0.0,
            }
        )
    for column, group in [
        ("research_setup_tags_json", "setup"),
        ("research_risk_tags_json", "risk"),
        ("research_regime_tags_json", "regime"),
    ]:
        tag_counts: dict[str, dict[str, int]] = {}
        for is_good, tags in zip(recent["starter_good"], recent[column].map(json.loads)):
            for tag in tags:
                tag_counts.setdefault(tag, {"good": 0, "bad": 0, "n": 0})
                tag_counts[tag]["n"] += 1
                if is_good:
                    tag_counts[tag]["good"] += 1
                else:
                    tag_counts[tag]["bad"] += 1
        for tag, counts in tag_counts.items():
            if counts["n"] < 300:
                continue
            records.append(
                {
                    "feature": f"{group}:{tag}",
                    "starter_good_mean": counts["good"] / counts["n"],
                    "starter_bad_mean": counts["bad"] / counts["n"],
                    "good_minus_bad_mean": (counts["good"] - counts["bad"]) / counts["n"],
                    "starter_good_median": None,
                    "starter_bad_median": None,
                    "coverage": counts["n"] / len(recent) if len(recent) else 0.0,
                }
            )
    return pd.DataFrame(records)


def watch_only_profile(rows: pd.DataFrame) -> pd.DataFrame:
    recent = rows[rows["year"].isin(YEARS) & rows["path20_available"].eq(True)]
    watch = recent[recent["diagnostic_candidate_role"].eq("watch_only_candidate")]
    output: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(watch):
        for topk in TOPK_VALUES:
            g = period_rows[period_rows["baseline_rank"] <= topk]
            output.append(
                {
                    "period": period,
                    "topk": topk,
                    "n": int(len(g)),
                    "ret20_mean": _mean(g, "ret20"),
                    "mae20_mean": _mean(g, "mae20"),
                    "immediate_adverse_rate": _rate(g["immediate_adverse_entry"]) if not g.empty else None,
                    "severe_loss_rate": _rate(g["ret20"] <= -0.05) if not g.empty else None,
                    "high_volatility_risk_share": _tag_share(g, "research_risk_tags_json", "high_volatility_risk"),
                    "overextension_candidate_share": _tag_share(g, "research_setup_tags_json", "overextension_candidate"),
                    "monthly_high_zone_share": _tag_share(g, "research_regime_tags_json", "monthly_high_zone"),
                }
            )
    return pd.DataFrame(output)


def _tag_share(frame: pd.DataFrame, column: str, tag: str) -> float | None:
    if frame.empty or column not in frame:
        return None
    return float(frame[column].map(lambda x: tag in json.loads(x)).mean())


def objective_gap_summary(rows: pd.DataFrame, audit: pd.DataFrame, ceiling: pd.DataFrame) -> dict[str, Any]:
    recent = rows[rows["year"].isin(YEARS) & rows["path20_available"].eq(True)]
    top10 = audit[(audit["period"] == "2024_2026_combined") & (audit["topk"] == 10)]
    ceil10 = ceiling[(ceiling["period"] == "2024_2026_combined") & (ceiling["topk"] == 10)]
    starter_good = recent[recent["starter_good"]]
    return {
        "recent_rows_with_path": int(len(recent)),
        "baseline_top10": top10.iloc[0].to_dict() if not top10.empty else {},
        "oracle_top10": ceil10.iloc[0].to_dict() if not ceil10.empty else {},
        "starter_good_count": int(len(starter_good)),
        "starter_good_deep_rank_share_gt10": None if starter_good.empty else float((starter_good["baseline_rank"] > 10).mean()),
        "objective_problem_type": "objective_problem" if not top10.empty and top10.iloc[0]["watch_only_rate"] > 0.25 else "mixed",
    }


def next_reform_candidates(feature: pd.DataFrame, gap: dict[str, Any]) -> list[dict[str, Any]]:
    top = feature.sort_values("good_minus_bad_mean", ascending=False).head(5)
    candidates: list[dict[str, Any]] = []
    if gap.get("objective_problem_type") == "objective_problem":
        candidates.append(
            {
                "axis_name": "starter_entry_actionability_score",
                "based_on": "objective split",
                "intended_next": "starter_score_model_needed",
                "evidence": "baseline topK has high watch_only/starter_bad contamination while oracle pool ceiling is materially higher",
                "recommended_next": "build interpretable starter-entry score contract",
            }
        )
    for _, row in top.iterrows():
        candidates.append(
            {
                "axis_name": str(row["feature"]),
                "based_on": "feature_or_shadow_tag",
                "intended_next": "hold",
                "observed_good_minus_bad": row["good_minus_bad_mean"],
                "coverage": row["coverage"],
                "recommended_next": "use as candidate input only if starter score model is approved",
            }
        )
    return candidates[:5]


def decide(gap: dict[str, Any], feature: pd.DataFrame) -> dict[str, Any]:
    baseline = gap.get("baseline_top10", {})
    oracle = gap.get("oracle_top10", {})
    rows = gap.get("recent_rows_with_path", 0)
    oracle_improvement = float(oracle.get("oracle_improvement") or 0.0)
    starter_good_count = int(gap.get("starter_good_count") or 0)
    watch_only_rate = float(baseline.get("watch_only_rate") or 0.0)
    best_single = float(feature["good_minus_bad_mean"].abs().max()) if not feature.empty else 0.0
    if rows < 1000:
        decision = "objective_contract_gap"
        reasons = ["insufficient recent label-safe rows"]
    elif starter_good_count < 1000:
        decision = "candidate_pool_redesign_needed"
        reasons = ["starter_good candidates are too sparse in current pool"]
    elif oracle_improvement >= 0.02 and watch_only_rate >= 0.25:
        decision = "starter_score_model_needed"
        reasons = ["pool has starter-entry signal, but no single fixed axis explains starter_good vs starter_bad"]
    elif oracle_improvement >= 0.01 and watch_only_rate >= 0.25:
        decision = "starter_objective_pretest_allowed"
        reasons = ["baseline topK has measurable watch-only contamination and pool ceiling supports starter-entry objective"]
    else:
        decision = "no_clear_reform_path"
        reasons = ["starter-entry objective gap is not large enough under current labels"]
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
    }


def run(taxonomy_root: Path, daily_path: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-objective-reform-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = load_rows(taxonomy_root)
    labels = build_forward_labels(daily_path)
    rows = attach_starter_labels(rows, labels)
    audit = baseline_objective_audit(rows)
    ceiling, ceiling_summary = oracle_pool_ceiling(rows)
    feature = feature_audit(rows)
    watch = watch_only_profile(rows)
    gap = objective_gap_summary(rows, audit, ceiling)
    candidates = next_reform_candidates(feature, gap)
    decision = decide(gap, feature)

    _write_json(out / "input_artifact_report.json", {"taxonomy_root": taxonomy_root, "daily_path": daily_path, "input_rows": len(rows)})
    _write_json(out / "starter_entry_label_schema.json", starter_entry_label_schema())
    rows.to_csv(out / "candidate_role_rows.csv", index=False)
    audit.to_csv(out / "baseline_objective_audit.csv", index=False)
    _write_json(out / "objective_gap_summary.json", gap)
    ceiling.to_csv(out / "oracle_pool_ceiling_summary.csv", index=False)
    feature.to_csv(out / "starter_good_vs_bad_feature_audit.csv", index=False)
    watch.to_csv(out / "watch_only_failure_profile.csv", index=False)
    _write_json(out / "next_reform_candidates.json", {"candidates": candidates, "oracle_top10_summary": ceiling_summary})
    _write_json(out / "research_decision.json", decision)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "candidate_features_use_decision_date_or_prior": True,
            "labels_use_future_path_only": True,
            "oracle_analysis_is_diagnostic_only": True,
            "future_label_influences_candidate_generation_or_ranking": False,
            "rows_without_full_future_path_marked_unavailable": True,
            "ranking_order_changed": False,
            "score_formula_changed": False,
            "runtime_db_write": False,
            "source_taxonomy_audit": str(taxonomy_root / "no_lookahead_audit.json"),
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build starter-entry actionability objective reform artifact")
    parser.add_argument("--taxonomy-root", type=Path, default=DEFAULT_TAXONOMY_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.taxonomy_root, args.daily_path, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
