from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "pattern_family_candidate_evaluation_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pattern_family_candidate_evaluation_v1")
REQUIRED_ARTIFACTS = (
    "family_candidate_evaluation_summary.json",
    "family_candidate_rows.csv",
    "family_metrics.json",
    "family_overlap_matrix.json",
    "family_breadth_metrics.json",
    "family_risk_metrics.json",
    "family_period_stability.json",
    "family_candidate_decisions.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
BROAD_REFERENCE_FLAG = "high_upside_reserve_reference_match"
BROAD_REFERENCE_WINNER_RATE = 0.1275583470978234
FAMILY_DECISIONS = {
    "keep_for_next_family_pretest",
    "promising_but_underpowered",
    "broad_low_quality_screen",
    "no_edge",
    "reference_proxy_only",
    "blocked_missing_outcomes",
}


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


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


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


def load_contracts(source_root: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    return (
        json.loads((source_root / "feature_contract.json").read_text(encoding="utf-8")),
        json.loads((source_root / "family_definition_contract.json").read_text(encoding="utf-8")),
        json.loads((source_root / "source_coverage.json").read_text(encoding="utf-8")),
        json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8")),
    )


def family_flags(family_contract: dict[str, Any]) -> list[str]:
    return sorted(family_contract.get("families", {}).keys())


def period_half(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(int(value))
    if len(text) < 6:
        return None
    return f"{text[:4]}{'H1' if int(text[4:6]) <= 6 else 'H2'}"


def metric(frame: pd.DataFrame, all_dates: set[Any]) -> dict[str, Any]:
    per_date = frame.groupby("as_of_date").size() if not frame.empty else pd.Series(dtype=float)
    present_dates = set(frame["as_of_date"].dropna().unique().tolist()) if not frame.empty else set()
    outcome = pd.to_numeric(frame["ret20"], errors="coerce") if "ret20" in frame else pd.Series(dtype=float)
    outcome_coverage = None if frame.empty else float(outcome.notna().mean())
    bad = frame["bad_ret20_lt_minus_5pct"] if "bad_ret20_lt_minus_5pct" in frame else outcome < -0.05
    severe = frame["severe_ret20_lt_minus_10pct"] if "severe_ret20_lt_minus_10pct" in frame else outcome < -0.10
    winner = frame["winner_ret20_gt_10pct"] if "winner_ret20_gt_10pct" in frame else outcome > 0.10
    bad_rate = _rate(bad)
    winner_rate = _rate(winner)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
        "zero_candidate_date_count": int(len(all_dates - present_dates)),
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(outcome > 0) if not outcome.empty else None,
        "winner_rate_ret20_gt_10pct": winner_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(severe),
        "downside_to_upside_ratio": None if not winner_rate else (bad_rate or 0) / winner_rate,
        "outcome_coverage_rate": outcome_coverage,
    }


def family_metrics(rows: pd.DataFrame, flags: list[str]) -> dict[str, Any]:
    all_dates = set(rows["as_of_date"].dropna().unique().tolist())
    return {flag: metric(rows[rows[flag].astype(bool)], all_dates) for flag in flags}


def overlap_matrix(rows: pd.DataFrame, flags: list[str]) -> dict[str, Any]:
    key_sets = {flag: set(zip(rows.loc[rows[flag].astype(bool), "as_of_date"], rows.loc[rows[flag].astype(bool), "code"])) for flag in flags}
    out: dict[str, Any] = {}
    for left in flags:
        out[left] = {}
        for right in flags:
            inter = key_sets[left] & key_sets[right]
            denom = len(key_sets[left])
            out[left][right] = {
                "overlap_sample_count": int(len(inter)),
                "left_sample_count": int(len(key_sets[left])),
                "right_sample_count": int(len(key_sets[right])),
                "left_overlap_rate": None if denom == 0 else len(inter) / denom,
            }
    return out


def breadth_metrics(rows: pd.DataFrame, flags: list[str], metrics: dict[str, Any]) -> dict[str, Any]:
    all_non_broad = [flag for flag in flags if flag != BROAD_REFERENCE_FLAG]
    key_sets = {flag: set(zip(rows.loc[rows[flag].astype(bool), "as_of_date"], rows.loc[rows[flag].astype(bool), "code"])) for flag in flags}
    union_non_broad = set().union(*(key_sets[flag] for flag in all_non_broad)) if all_non_broad else set()
    out = {
        "combined_non_broad_candidate_families": {
            "combined_unique_sample_count": int(len(union_non_broad)),
            "combined_unique_date_count": int(len({d for d, _ in union_non_broad})),
        },
        "families": {},
    }
    for flag in flags:
        others = set().union(*(key_sets[o] for o in flags if o != flag)) if len(flags) > 1 else set()
        unique = key_sets[flag] - others
        m = metrics[flag]
        out["families"][flag] = {
            "unique_sample_count": int(len(unique)),
            "unique_date_count": int(len({d for d, _ in unique})),
            "sample_count": m["sample_count"],
            "date_count": m["date_count"],
            "average_candidates_per_date": m["average_candidates_per_date"],
            "is_broad_screen": bool((m["sample_count"] or 0) > 100_000 or (m["average_candidates_per_date"] or 0) > 50),
        }
    return out


def risk_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        flag: {
            "mean_ret20": m["mean_ret20"],
            "median_ret20": m["median_ret20"],
            "winner_rate_ret20_gt_10pct": m["winner_rate_ret20_gt_10pct"],
            "bad_rate_ret20_lt_minus_5pct": m["bad_rate_ret20_lt_minus_5pct"],
            "severe_rate_ret20_lt_minus_10pct": m["severe_rate_ret20_lt_minus_10pct"],
            "downside_to_upside_ratio": m["downside_to_upside_ratio"],
            "outcome_coverage_rate": m["outcome_coverage_rate"],
        }
        for flag, m in metrics.items()
    }


def period_stability(rows: pd.DataFrame, flags: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    rows = rows.copy()
    rows["period_half"] = rows["as_of_date"].map(period_half)
    for flag in flags:
        out[flag] = {}
        for period, group in rows[rows[flag].astype(bool)].groupby("period_half", dropna=True):
            out[flag][str(period)] = metric(group, set(group["as_of_date"].dropna().unique().tolist()))
    return out


def family_decisions(metrics: dict[str, Any], breadth: dict[str, Any]) -> dict[str, Any]:
    decisions: dict[str, Any] = {}
    for flag, m in metrics.items():
        if m["outcome_coverage_rate"] is None or m["outcome_coverage_rate"] < 0.95:
            decision = "blocked_missing_outcomes"
            reason = "offline_outcome_coverage_below_required_level"
        elif flag == BROAD_REFERENCE_FLAG:
            decision = "reference_proxy_only"
            reason = "known_broad_reference_proxy_not_usable_family"
        else:
            b = breadth["families"][flag]
            broad = b["is_broad_screen"]
            mean_ret20 = m["mean_ret20"] or 0
            winner = m["winner_rate_ret20_gt_10pct"] or 0
            bad = m["bad_rate_ret20_lt_minus_5pct"] if m["bad_rate_ret20_lt_minus_5pct"] is not None else 1
            severe = m["severe_rate_ret20_lt_minus_10pct"] if m["severe_rate_ret20_lt_minus_10pct"] is not None else 1
            enough_breadth = (m["sample_count"] or 0) >= 100 and (m["date_count"] or 0) >= 60
            risk_ok = bad < 0.20 and severe <= 0.08
            strong_direction = mean_ret20 >= 0.03 and winner > BROAD_REFERENCE_WINNER_RATE
            weak_positive = mean_ret20 > 0 and winner >= 0.10
            if broad and not strong_direction:
                decision = "broad_low_quality_screen"
                reason = "broad_screen_without_return_or_winner_edge"
            elif strong_direction and risk_ok and enough_breadth and not broad:
                decision = "keep_for_next_family_pretest"
                reason = "return_winner_risk_and_breadth_gates_pass"
            elif weak_positive and not broad:
                decision = "promising_but_underpowered"
                reason = "positive_but_return_winner_or_risk_gate_not_keep_worthy"
            elif broad:
                decision = "broad_low_quality_screen"
                reason = "too_broad_for_candidate_family_use"
            else:
                decision = "no_edge"
                reason = "does_not_beat_broad_reference_on_return_winner_risk"
        decisions[flag] = {"decision": decision, "reason_typed": reason}
    return decisions


def overall_decision(decisions: dict[str, Any]) -> tuple[str, list[str]]:
    vals = [d["decision"] for d in decisions.values()]
    if "blocked_missing_outcomes" in vals and len(set(vals)) == 1:
        return "blocked_missing_outcome_or_family_contract", ["all_families_missing_required_outcome_or_family_contract"]
    if "keep_for_next_family_pretest" in vals:
        return "family_candidate_keep_found", ["at_least_one_family_passed_candidate_quality_gate"]
    if "promising_but_underpowered" in vals:
        return "family_candidate_promising_underpowered_found", ["at_least_one_family_is_directionally_positive_but_not_keep_worthy"]
    if all(v in {"broad_low_quality_screen", "reference_proxy_only"} for v in vals):
        return "only_broad_low_quality_or_reference_proxies", ["families_are_only_broad_low_quality_screens_or_reference_proxies"]
    return "no_family_edge", ["no_family_beats_reference_on_return_winner_risk"]


def selected_rows_sample(rows: pd.DataFrame, flags: list[str]) -> pd.DataFrame:
    selected = rows[rows[flags].any(axis=1)].copy()
    cols = ["as_of_date", "code", *flags, "ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]
    return selected[cols].head(25_000)


def run(source_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-pattern-family-candidate-evaluation-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows_path = source_root / "pattern_family_source_rows.parquet"
    feature_contract, family_contract, source_cov, source_no_lookahead = load_contracts(source_root)
    flags = family_flags(family_contract)
    if not rows_path.exists() or not flags:
        decision = "blocked_missing_outcome_or_family_contract"
        reasons = ["source_rows_or_family_flags_missing"]
        rows = pd.DataFrame()
        metrics: dict[str, Any] = {}
        overlap: dict[str, Any] = {}
        breadth: dict[str, Any] = {}
        risk: dict[str, Any] = {}
        stability: dict[str, Any] = {}
        decisions: dict[str, Any] = {}
    else:
        rows = pd.read_parquet(rows_path)
        metrics = family_metrics(rows, flags)
        overlap = overlap_matrix(rows, flags)
        breadth = breadth_metrics(rows, flags, metrics)
        risk = risk_metrics(metrics)
        stability = period_stability(rows, flags)
        decisions = family_decisions(metrics, breadth)
        decision, reasons = overall_decision(decisions)
    selected_rows_sample(rows, flags).to_csv(out / "family_candidate_rows.csv", index=False)
    _write_json(out / "family_metrics.json", metrics)
    _write_json(out / "family_overlap_matrix.json", overlap)
    _write_json(out / "family_breadth_metrics.json", breadth)
    _write_json(out / "family_risk_metrics.json", risk)
    _write_json(out / "family_period_stability.json", stability)
    _write_json(out / "family_candidate_decisions.json", decisions)
    _write_json(
        out / "family_candidate_evaluation_summary.json",
        {
            "axis_id": AXIS_ID,
            "source_root": source_root,
            "source_rows_authoritative": True,
            "family_flags_evaluated": flags,
            "decision": decision,
            "reason_typed": reasons,
        },
    )
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass" if source_no_lookahead.get("audit_result") == "pass" else "blocked",
            "source_no_lookahead_audit": source_no_lookahead.get("audit_result"),
            "source_rows_authoritative": True,
            "family_flags_use_point_in_time_features_only": True,
            "offline_outcomes_used_for_evaluation_only": True,
            "runtime_db_write": False,
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "source_root": source_root,
            "source_rows": int(len(rows)),
            "source_date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0,
            "source_code_count": int(rows["code"].nunique()) if not rows.empty else 0,
            "source_coverage": source_cov,
            "feature_contract_axis": feature_contract.get("axis_id"),
            "family_contract_axis": family_contract.get("axis_id"),
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "reason_typed": reasons,
            "source_rows_authoritative": True,
            "production_candidate_generator_changed": False,
            "runtime_db_write": False,
            "meemee_reflectable_candidate": False,
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
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.source_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
