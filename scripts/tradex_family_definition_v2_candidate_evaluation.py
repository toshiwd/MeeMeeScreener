from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "family_definition_v2_candidate_evaluation"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\family_definition_v2_source_rows\20260525T132408Z-family-definition-v2-source-rows")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\family_definition_v2_candidate_evaluation")
V2_FLAGS = [
    "high_upside_contained_reserve_family_v2",
    "constructive_pullback_confirmation_family_v2",
    "volatility_compression_pre_breakout_family_v2",
]
REQUIRED_ARTIFACTS = (
    "family_v2_candidate_evaluation_summary.json",
    "family_v2_candidate_rows.csv",
    "feature_contract.json",
    "lineage.json",
    "family_v2_metrics.json",
    "family_v2_overlap_matrix.json",
    "family_v2_period_stability.json",
    "family_v2_candidate_decisions.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
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
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def period_half(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(int(value))
    return f"{text[:4]}{'H1' if int(text[4:6]) <= 6 else 'H2'}" if len(text) >= 6 else None


def load_rows(source_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    return (
        pd.read_parquet(source_root / "family_v2_source_rows.parquet"),
        json.loads((source_root / "feature_contract.json").read_text(encoding="utf-8")),
        json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8")),
        json.loads((source_root / "research_decision.json").read_text(encoding="utf-8")),
    )


def metric(frame: pd.DataFrame, all_dates: set[Any]) -> dict[str, Any]:
    per_date = frame.groupby("as_of_date").size() if not frame.empty else pd.Series(dtype=float)
    present = set(frame["as_of_date"].dropna().unique().tolist()) if not frame.empty else set()
    bad = frame["bad_ret20_lt_minus_5pct"] if "bad_ret20_lt_minus_5pct" in frame else pd.Series(dtype=bool)
    severe = frame["severe_ret20_lt_minus_10pct"] if "severe_ret20_lt_minus_10pct" in frame else pd.Series(dtype=bool)
    winner = frame["winner_ret20_gt_10pct"] if "winner_ret20_gt_10pct" in frame else pd.Series(dtype=bool)
    bad_rate = _rate(bad)
    winner_rate = _rate(winner)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
        "zero_candidate_date_count": int(len(all_dates - present)),
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(frame["ret20"] > 0) if not frame.empty else None,
        "winner_rate_ret20_gt_10pct": winner_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(severe),
        "downside_to_upside_ratio": None if not winner_rate else (bad_rate or 0) / winner_rate,
        "outcome_coverage_rate": None if frame.empty else float(frame["ret20"].notna().mean()),
    }


def family_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    dates = set(rows["as_of_date"].dropna().unique().tolist())
    return {flag: metric(rows[rows[flag]], dates) for flag in V2_FLAGS}


def overlap_matrix(rows: pd.DataFrame) -> dict[str, Any]:
    keys = {flag: set(zip(rows.loc[rows[flag], "as_of_date"], rows.loc[rows[flag], "code"])) for flag in V2_FLAGS}
    out: dict[str, Any] = {}
    for left in V2_FLAGS:
        out[left] = {}
        for right in V2_FLAGS:
            inter = keys[left] & keys[right]
            out[left][right] = {
                "overlap_count": int(len(inter)),
                "left_count": int(len(keys[left])),
                "right_count": int(len(keys[right])),
                "left_overlap_rate": None if not keys[left] else len(inter) / len(keys[left]),
            }
    return out


def period_stability(rows: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    rows = rows.copy()
    rows["period_half"] = rows["as_of_date"].map(period_half)
    for flag in V2_FLAGS:
        out[flag] = {}
        for period, g in rows[rows[flag]].groupby("period_half", dropna=True):
            out[flag][str(period)] = metric(g, set(g["as_of_date"].dropna().unique().tolist()))
    return out


def family_decisions(metrics: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for flag, m in metrics.items():
        mean_ret = m["mean_ret20"] or 0
        winner = m["winner_rate_ret20_gt_10pct"] or 0
        bad = m["bad_rate_ret20_lt_minus_5pct"] if m["bad_rate_ret20_lt_minus_5pct"] is not None else 1
        severe = m["severe_rate_ret20_lt_minus_10pct"] if m["severe_rate_ret20_lt_minus_10pct"] is not None else 1
        breadth = (m["sample_count"] or 0) >= 1000 and (m["date_count"] or 0) >= 250
        risk_ok = bad <= 0.18 and severe <= 0.07
        edge = mean_ret >= 0.025 and winner >= 0.15
        if edge and risk_ok and breadth:
            decision = "keep_for_next_stage"
            reason = "return_winner_risk_breadth_gate_passed"
        elif mean_ret > 0.012 and winner >= 0.12:
            decision = "promising_but_underpowered"
            reason = "positive_direction_but_not_keep_worthy"
        elif mean_ret > 0:
            decision = "drop"
            reason = "weak_positive_return_without_winner_or_risk_edge"
        else:
            decision = "close_branch_no_reusable_signal"
            reason = "no_positive_family_edge"
        out[flag] = {"decision": decision, "decision_class": map_decision(decision), "reason_typed": reason}
    return out


def map_decision(decision: str) -> str:
    return {
        "keep_for_next_stage": "KEEP",
        "promising_but_underpowered": "HOLD_UNDERPOWERED",
        "drop": "DROP",
        "close_branch_no_reusable_signal": "CLOSE",
    }[decision]


def overall_decision(decisions: dict[str, Any]) -> tuple[str, list[str], str]:
    classes = [d["decision_class"] for d in decisions.values()]
    if "KEEP" in classes:
        return "keep_for_next_stage", ["at_least_one_v2_family_keep_worthy"], "KEEP"
    if "HOLD_UNDERPOWERED" in classes:
        return "promising_but_underpowered", ["at_least_one_v2_family_positive_but_underpowered"], "HOLD_UNDERPOWERED"
    if "DROP" in classes:
        return "drop", ["v2_families_weak_positive_but_not_keep_worthy"], "DROP"
    return "close_branch_no_reusable_signal", ["no_v2_family_reusable_signal"], "CLOSE"


def feature_contract_passthrough(source_contract: dict[str, Any]) -> dict[str, Any]:
    return {"axis_id": AXIS_ID, "source_feature_contract_axis": source_contract.get("axis_id"), "fields": source_contract.get("fields", {})}


def run(source_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-family-definition-v2-candidate-evaluation"
    out.mkdir(parents=True, exist_ok=True)
    rows, source_contract, source_no_lookahead, source_decision = load_rows(source_root)
    metrics = family_metrics(rows)
    overlap = overlap_matrix(rows)
    stability = period_stability(rows)
    decisions = family_decisions(metrics)
    decision, reasons, decision_class = overall_decision(decisions)
    selected = rows[rows[V2_FLAGS].any(axis=1)]
    selected[["as_of_date", "code", *V2_FLAGS, "ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]].head(25000).to_csv(out / "family_v2_candidate_rows.csv", index=False)
    _write_json(out / "feature_contract.json", feature_contract_passthrough(source_contract))
    _write_json(out / "lineage.json", {"source_root": source_root, "source_decision": source_decision, "source_rows_authoritative": True})
    _write_json(out / "family_v2_metrics.json", metrics)
    _write_json(out / "family_v2_overlap_matrix.json", overlap)
    _write_json(out / "family_v2_period_stability.json", stability)
    _write_json(out / "family_v2_candidate_decisions.json", decisions)
    _write_json(out / "family_v2_candidate_evaluation_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "selected_row_count": int(len(selected)), "source_root": source_root})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass" if source_no_lookahead.get("audit_result") == "pass" else "blocked", "source_no_lookahead_audit": source_no_lookahead.get("audit_result"), "family_flags_from_source_only": True, "outcomes_used_evaluation_only": True, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_rows": int(len(rows)), "selected_family_rows": int(len(selected)), "date_count": int(rows["as_of_date"].nunique()), "code_count": int(rows["code"].nunique()), "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
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
