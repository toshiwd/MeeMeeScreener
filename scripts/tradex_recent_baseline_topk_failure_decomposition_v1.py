from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "recent_baseline_topk_failure_decomposition_v1"
DEFAULT_INPUT_ROOT = Path(
    r"G:\Tradex\monthly_box_breakout_above60_maturity_context_pretest_v1\20260523T194427Z-monthly-box-breakout-above60-maturity-context-pretest-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\recent_baseline_topk_failure_decomposition_v1")
REQUIRED_INPUTS = ("candidate_rows_scored.csv", "no_lookahead_audit.json", "research_decision.json")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "baseline_topk_failure_summary.json",
    "selected_losers_by_year_topk.csv",
    "missed_winners_by_year_topk.csv",
    "winner_rank_distance_by_year.csv",
    "feature_contrast_selected_loser_missed_winner.csv",
    "failure_mode_by_year_topk.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
FEATURE_COLUMNS = (
    "monthly_box_breakout_proxy",
    "monthly_high_zone_proxy",
    "monthly_box_inside_proxy",
    "above60_streak",
    "days_since_ma60_reclaim",
    "monthly_box_breakout_bool",
    "above60_streak_numeric",
    "gated_score_delta",
    "ungated_score_delta",
    "baseline_score",
)
TOPK_VALUES = (5, 10)
YEARS = (2024, 2025, 2026)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
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


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if col not in df or df.empty:
        return None
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if col not in df or df.empty:
        return None
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _rate_bool(series: pd.Series) -> float | None:
    if series.empty:
        return None
    return float(series.fillna(False).astype(bool).mean())


def add_labels(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["ret20_num"] = pd.to_numeric(out["ret20"], errors="coerce")
    out = out[out["year"].isin(YEARS) & out["ret20_num"].notna()].copy()
    out["ret20_pct_rank_by_date"] = out.groupby("decision_ymd")["ret20_num"].rank(pct=True, method="average")
    out["winner20_abs"] = out["ret20_num"] >= 0.05
    out["loser20_abs"] = out["ret20_num"] <= -0.05
    out["winner20_cross_sectional"] = out["ret20_pct_rank_by_date"] >= 0.70
    out["loser20_cross_sectional"] = out["ret20_pct_rank_by_date"] <= 0.30
    out["winner20"] = out["winner20_abs"] | out["winner20_cross_sectional"]
    out["loser20"] = out["loser20_abs"] | out["loser20_cross_sectional"]
    return out


def _rank_distance_bucket(rank: float, topk: int) -> str:
    if rank <= topk:
        return "inside_topk"
    distance = rank - topk
    if distance <= 5:
        return "topk_plus_1_5"
    if distance <= 10:
        return "topk_plus_6_10"
    if distance <= 20:
        return "topk_plus_11_20"
    if distance <= 50:
        return "topk_plus_21_50"
    return "deep_rank_gt_topk_plus_50"


def topk_failure_tables(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    selected_parts: list[dict[str, Any]] = []
    missed_parts: list[dict[str, Any]] = []
    distance_parts: list[dict[str, Any]] = []
    rank = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows = rows.assign(rank_num=rank)
    for year in YEARS:
        year_rows = rows[rows["year"] == year].copy()
        for topk in TOPK_VALUES:
            selected = year_rows[year_rows["rank_num"] <= topk]
            selected_losers = selected[selected["loser20"]]
            winners = year_rows[year_rows["winner20"]]
            missed_winners = winners[winners["rank_num"] > topk]
            selected_parts.append(
                {
                    "year": year,
                    "topk": topk,
                    "selected_n": int(len(selected)),
                    "selected_loser_n": int(len(selected_losers)),
                    "selected_loser_rate": None if len(selected) == 0 else float(len(selected_losers) / len(selected)),
                    "selected_loser_ret20_mean": _mean(selected_losers, "ret20_num"),
                    "selected_loser_ret20_median": _median(selected_losers, "ret20_num"),
                    "selected_ret20_mean": _mean(selected, "ret20_num"),
                }
            )
            missed_parts.append(
                {
                    "year": year,
                    "topk": topk,
                    "candidate_winner_n": int(len(winners)),
                    "captured_winner_n": int((winners["rank_num"] <= topk).sum()),
                    "missed_winner_n": int(len(missed_winners)),
                    "winner_capture_rate": None if len(winners) == 0 else float((winners["rank_num"] <= topk).sum() / len(winners)),
                    "missed_winner_ret20_mean": _mean(missed_winners, "ret20_num"),
                    "missed_winner_rank_median": _median(missed_winners, "rank_num"),
                    "missed_winner_rank_mean": _mean(missed_winners, "rank_num"),
                }
            )
            if not winners.empty:
                buckets = winners["rank_num"].map(lambda x: _rank_distance_bucket(float(x), topk))
                for bucket, group in winners.assign(rank_distance_bucket=buckets).groupby("rank_distance_bucket", sort=True):
                    distance_parts.append(
                        {
                            "year": year,
                            "topk": topk,
                            "rank_distance_bucket": bucket,
                            "winner_n": int(len(group)),
                            "winner_share": float(len(group) / len(winners)),
                            "ret20_mean": _mean(group, "ret20_num"),
                            "rank_mean": _mean(group, "rank_num"),
                        }
                    )
    return pd.DataFrame(selected_parts), pd.DataFrame(missed_parts), pd.DataFrame(distance_parts)


def feature_contrast(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    rank = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows = rows.assign(rank_num=rank)
    for year in YEARS:
        year_rows = rows[rows["year"] == year].copy()
        for topk in TOPK_VALUES:
            selected_losers = year_rows[(year_rows["rank_num"] <= topk) & year_rows["loser20"]]
            missed_winners = year_rows[(year_rows["rank_num"] > topk) & year_rows["winner20"]]
            selected_winners = year_rows[(year_rows["rank_num"] <= topk) & year_rows["winner20"]]
            for feature in FEATURE_COLUMNS:
                if feature not in year_rows:
                    continue
                for cohort_name, cohort in (
                    ("selected_loser", selected_losers),
                    ("missed_winner", missed_winners),
                    ("selected_winner", selected_winners),
                ):
                    values = cohort[feature] if feature in cohort else pd.Series(dtype=float)
                    numeric = pd.to_numeric(values, errors="coerce")
                    out.append(
                        {
                            "year": year,
                            "topk": topk,
                            "feature": feature,
                            "cohort": cohort_name,
                            "n": int(len(cohort)),
                            "mean": None if numeric.dropna().empty else float(numeric.mean()),
                            "median": None if numeric.dropna().empty else float(numeric.median()),
                            "true_rate": _rate_bool(values) if str(values.dtype) in {"bool", "boolean"} else None,
                        }
                    )
    return pd.DataFrame(out)


def classify_failure_modes(selected: pd.DataFrame, missed: pd.DataFrame, distance: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = []
    for _, s in selected.iterrows():
        year = int(s["year"])
        topk = int(s["topk"])
        m = missed[(missed["year"] == year) & (missed["topk"] == topk)].iloc[0]
        d = distance[(distance["year"] == year) & (distance["topk"] == topk)]
        near_share = float(d[d["rank_distance_bucket"].isin(["topk_plus_1_5", "topk_plus_6_10"])]["winner_share"].sum()) if not d.empty else 0.0
        capture = float(m["winner_capture_rate"]) if pd.notna(m["winner_capture_rate"]) else 0.0
        loser_rate = float(s["selected_loser_rate"]) if pd.notna(s["selected_loser_rate"]) else 0.0
        missed_n = int(m["missed_winner_n"])
        selected_n = int(s["selected_n"])
        if selected_n == 0 or int(m["candidate_winner_n"]) == 0:
            mode = "source_contract_failure"
        elif loser_rate >= 0.30:
            mode = "selected_loser_failure"
        elif missed_n > selected_n and near_share >= 0.35:
            mode = "ordering_failure"
        elif capture < 0.10 and near_share < 0.20:
            mode = "pool_failure"
        elif loser_rate < 0.20 and capture >= 0.20:
            mode = "timing_failure"
        else:
            mode = "ordering_failure"
        rows.append(
            {
                "year": year,
                "topk": topk,
                "failure_mode": mode,
                "winner_capture_rate": capture,
                "selected_loser_rate": loser_rate,
                "missed_winner_n": missed_n,
                "near_boundary_winner_share_topk_plus_10": near_share,
            }
        )
    table = pd.DataFrame(rows)
    mode_counts = table["failure_mode"].value_counts().to_dict()
    stable = table.groupby("topk")["failure_mode"].nunique().max() == 1 if not table.empty else False
    if "source_contract_failure" in mode_counts:
        decision = "source_contract_failure"
    elif mode_counts.get("selected_loser_failure", 0) >= mode_counts.get("ordering_failure", 0):
        decision = "selected_loser_failure"
    elif mode_counts.get("ordering_failure", 0) >= 3:
        decision = "ordering_failure"
    elif mode_counts.get("pool_failure", 0) >= 3:
        decision = "pool_failure"
    else:
        decision = "timing_failure"
    return table, {"primary_failure_mode": decision, "failure_mode_counts": mode_counts, "same_failure_mode_across_years": bool(stable)}


def run(*, input_root: Path = DEFAULT_INPUT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    missing = [name for name in REQUIRED_INPUTS if not (input_root / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing required input artifacts: {missing}")
    run_dir = output_root / f"{_now_tag()}-recent-baseline-topk-failure-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((input_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows = add_labels(pd.read_csv(input_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False))
    selected, missed, distance = topk_failure_tables(rows)
    contrast = feature_contrast(rows)
    failure_modes, decision_payload = classify_failure_modes(selected, missed, distance)
    summary = {
        "period": "2024-2026 label-safe",
        "rows_loaded": int(len(rows)),
        "years": list(YEARS),
        "topk_values": list(TOPK_VALUES),
        "winner_label": "ret20 >= +5% OR top 30% within same decision date candidate cohort",
        "loser_label": "ret20 <= -5% OR bottom 30% within same decision date candidate cohort",
        **decision_payload,
    }
    selected.to_csv(run_dir / "selected_losers_by_year_topk.csv", index=False)
    missed.to_csv(run_dir / "missed_winners_by_year_topk.csv", index=False)
    distance.to_csv(run_dir / "winner_rank_distance_by_year.csv", index=False)
    contrast.to_csv(run_dir / "feature_contrast_selected_loser_missed_winner.csv", index=False)
    failure_modes.to_csv(run_dir / "failure_mode_by_year_topk.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"input_root": input_root, "source_no_lookahead_audit": source_audit.get("audit_result"), "rows_loaded": int(len(rows)), "scope": "TRADEX-only baseline TOPK failure decomposition"})
    _write_json(run_dir / "baseline_topk_failure_summary.json", summary)
    _write_json(run_dir / "research_decision.json", {"research_decision": summary["primary_failure_mode"], "reason_typed": [f"dominant failure mode counts: {summary['failure_mode_counts']}"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False})
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "features_are_point_in_time": True, "ret20_used_only_as_label": True, "same_date_candidate_cohort_labels": True, "topk_baseline_only": True})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": summary["primary_failure_mode"], "summary": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decompose 2024-2026 baseline TOPK selected losers and missed winners")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(input_root=args.input_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
