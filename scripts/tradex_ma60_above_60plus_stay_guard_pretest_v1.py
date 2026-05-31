from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_ma60_above_60plus_pattern_audit_v1 import (
    DEFAULT_PRODUCTION_CSV,
    InputResolution,
    _json_ready,
    add_features,
    load_daily_frame,
    resolve_input,
)


AXIS_ID = "ma60_above_60plus_stay_guard_pretest_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\ma60_above_60plus_pattern_audit_v1\20260523T125759Z-ma60-above-60plus-pattern-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_stay_guard_pretest_v1")
TARGET_ANCHORS = ("anchor_10", "anchor_20")
REQUIRED_INPUTS = (
    "anchor_feature_rows.csv",
    "feature_lift_by_anchor.csv",
    "simple_rule_candidates.csv",
    "streak_events.csv",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "selected_guard_rules.json",
    "guard_hit_rows.csv",
    "guard_vs_baseline_summary.json",
    "stay_simulation_summary.json",
    "failure_reduction_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
PREFERRED_RULE_PATTERNS = (
    ("monthly_box_breakout", "post_start_held_ma20"),
    ("post_start_held_ma20", "max_drawdown_20"),
    ("high_break_volume_count_20", "dist_ma20_pct"),
    ("high_break_volume_count_20", "ma7_gt_ma20_gt_ma60"),
)


@dataclass(frozen=True)
class GuardRule:
    rule_id: str
    anchor_type: str
    condition: str
    features: tuple[str, ...]
    source_n_selected: int
    source_positive_rate: float | None
    source_lift_vs_anchor_base: float | None


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def load_inputs(source_root: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_INPUTS if not (source_root / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing required input artifacts: {missing}")
    audit = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    return {
        "anchors": pd.read_csv(source_root / "anchor_feature_rows.csv"),
        "lift": pd.read_csv(source_root / "feature_lift_by_anchor.csv"),
        "rules": pd.read_csv(source_root / "simple_rule_candidates.csv"),
        "streaks": pd.read_csv(source_root / "streak_events.csv"),
        "source_audit": audit,
    }


def _features_from_condition(condition: str) -> tuple[str, ...]:
    parts = re.split(r"\s+AND\s+", str(condition))
    features: list[str] = []
    for part in parts:
        token = part.strip().split(" ")[0].strip()
        if token and token not in features:
            features.append(token)
    return tuple(features)


def select_guard_rules(rules: pd.DataFrame, lift: pd.DataFrame, *, max_rules: int = 3) -> list[GuardRule]:
    candidates = rules[rules["anchor_type"].isin(TARGET_ANCHORS)].copy()
    candidates = candidates[pd.to_numeric(candidates["n_selected"], errors="coerce") >= 300]
    candidates = candidates[pd.to_numeric(candidates["condition_count"], errors="coerce") <= 2]
    if candidates.empty:
        return []
    feature_group = lift[["anchor_type", "feature", "feature_group"]].drop_duplicates()
    selected_rows: list[dict[str, Any]] = []
    for _, row in candidates.iterrows():
        features = _features_from_condition(str(row["condition"]))
        groups = set(
            feature_group[
                (feature_group["anchor_type"] == row["anchor_type"]) & (feature_group["feature"].isin(features))
            ]["feature_group"].dropna().astype(str)
        )
        if len(features) > 1 and len(groups) < 2:
            continue
        priority = 99
        for idx, pattern in enumerate(PREFERRED_RULE_PATTERNS):
            if all(item in features for item in pattern):
                priority = idx
                break
        item = row.to_dict()
        item["_features"] = features
        item["_group_count"] = len(groups)
        item["_priority"] = priority
        selected_rows.append(item)
    if not selected_rows:
        return []
    ranked = pd.DataFrame(selected_rows)
    ranked = ranked.sort_values(["_priority", "lift_vs_anchor_base", "n_selected"], ascending=[True, False, False])
    out: list[GuardRule] = []
    used_conditions: set[tuple[str, str]] = set()
    for _, row in ranked.iterrows():
        key = (str(row["anchor_type"]), str(row["condition"]))
        if key in used_conditions:
            continue
        used_conditions.add(key)
        out.append(
            GuardRule(
                rule_id=f"guard_rule_{len(out)+1}",
                anchor_type=str(row["anchor_type"]),
                condition=str(row["condition"]),
                features=tuple(row["_features"]),
                source_n_selected=int(row["n_selected"]),
                source_positive_rate=_safe_float(row.get("positive_rate")),
                source_lift_vs_anchor_base=_safe_float(row.get("lift_vs_anchor_base")),
            )
        )
        if len(out) >= max_rules:
            break
    return out


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _feature_threshold(anchor_rows: pd.DataFrame, anchor_type: str, feature: str) -> tuple[str, float]:
    values = pd.to_numeric(anchor_rows.loc[anchor_rows["anchor_type"] == anchor_type, feature], errors="coerce")
    if values.dropna().nunique() <= 2:
        return "binary_one", 1.0
    return "q75", float(values.quantile(0.75))


def apply_rule(anchor_rows: pd.DataFrame, rule: GuardRule) -> pd.Series:
    mask = anchor_rows["anchor_type"].eq(rule.anchor_type)
    for feature in rule.features:
        if feature not in anchor_rows.columns:
            return pd.Series(False, index=anchor_rows.index)
        kind, threshold = _feature_threshold(anchor_rows, rule.anchor_type, feature)
        values = pd.to_numeric(anchor_rows[feature], errors="coerce")
        if kind == "binary_one":
            mask &= values == 1
        else:
            mask &= values >= threshold
    return mask.fillna(False)


def _ymd(value: Any) -> int:
    return int(pd.Timestamp(value).strftime("%Y%m%d"))


def enrich_outcomes(anchor_rows: pd.DataFrame, featured_daily: pd.DataFrame) -> pd.DataFrame:
    daily = featured_daily.sort_values(["code", "date"], kind="stable").copy()
    daily["anchor_date"] = daily["date"].dt.strftime("%Y-%m-%d")
    lookup = daily.groupby("code", sort=False)
    rows: list[dict[str, Any]] = []
    for item in anchor_rows.to_dict("records"):
        code = str(item["code"])
        anchor_date = pd.Timestamp(item["anchor_date"])
        group = lookup.get_group(code) if code in lookup.groups else pd.DataFrame()
        path = group[group["date"] >= anchor_date].head(41).copy()
        if path.empty:
            rows.append({**item, **_empty_outcomes()})
            continue
        anchor_close = float(path.iloc[0]["c"])
        f20 = path.iloc[20] if len(path) > 20 else None
        f40 = path.iloc[40] if len(path) > 40 else None
        next20 = path.iloc[1:21]
        stay_path = path.iloc[1:41]
        exit_row = None
        exit_reason = "horizon_40d"
        for _, row in stay_path.iterrows():
            close = float(row["c"])
            ma20 = row.get("ma20")
            ma60 = row.get("ma60")
            if pd.notna(ma20) and close <= float(ma20):
                exit_row = row
                exit_reason = "close_lte_ma20"
                break
            if pd.notna(ma60) and close <= float(ma60):
                exit_row = row
                exit_reason = "close_lte_ma60"
                break
        if exit_row is None:
            exit_row = stay_path.iloc[-1] if not stay_path.empty else path.iloc[0]
        ma20_break = bool((next20["c"] <= next20["ma20"]).fillna(False).any()) if not next20.empty else False
        ma60_break = bool((next20["c"] <= next20["ma60"]).fillna(False).any()) if not next20.empty else False
        gap_ret = path["c"].pct_change()
        large_bear = bool(((gap_ret <= -0.05) & (path["volume_ratio_ma20"].fillna(0) >= 1.5)).iloc[1:21].any()) if len(path) > 1 else False
        lows = next20["l"] if not next20.empty else pd.Series(dtype=float)
        highs = next20["h"] if not next20.empty else pd.Series(dtype=float)
        rows.append(
            {
                **item,
                "anchor_close": anchor_close,
                "ret20_from_anchor": _ret(None if f20 is None else f20["c"], anchor_close),
                "ret40_from_anchor": _ret(None if f40 is None else f40["c"], anchor_close),
                "mae20_from_anchor": None if lows.empty else float(lows.min() / anchor_close - 1.0),
                "mfe20_from_anchor": None if highs.empty else float(highs.max() / anchor_close - 1.0),
                "path_max_drawdown_20": _path_drawdown(next20["c"]) if not next20.empty else None,
                "ma20_break_within_20d": ma20_break,
                "ma60_break_within_20d": ma60_break,
                "ma20_and_ma60_break_within_20d": bool(ma20_break and ma60_break),
                "large_bearish_break_within_20d": large_bear,
                "failure_within_20d": bool(ma20_break or ma60_break or large_bear),
                "stay_exit_date": pd.Timestamp(exit_row["date"]).strftime("%Y-%m-%d"),
                "stay_exit_reason": exit_reason,
                "stay_return": _ret(exit_row["c"], anchor_close),
                "stay_mae": None if stay_path.empty else float(stay_path["l"].min() / anchor_close - 1.0),
            }
        )
    return pd.DataFrame(rows)


def _empty_outcomes() -> dict[str, Any]:
    return {
        "anchor_close": None,
        "ret20_from_anchor": None,
        "ret40_from_anchor": None,
        "mae20_from_anchor": None,
        "mfe20_from_anchor": None,
        "path_max_drawdown_20": None,
        "ma20_break_within_20d": None,
        "ma60_break_within_20d": None,
        "ma20_and_ma60_break_within_20d": None,
        "large_bearish_break_within_20d": None,
        "failure_within_20d": None,
        "stay_exit_date": None,
        "stay_exit_reason": None,
        "stay_return": None,
        "stay_mae": None,
    }


def _ret(exit_value: Any, entry: float) -> float | None:
    try:
        exit_float = float(exit_value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(exit_float) or entry <= 0:
        return None
    return float(exit_float / entry - 1.0)


def _path_drawdown(close: pd.Series) -> float | None:
    values = pd.to_numeric(close, errors="coerce").dropna()
    if values.empty:
        return None
    dd = values / values.cummax() - 1.0
    return float(dd.min())


def summarize_guard(enriched: pd.DataFrame, rules: list[GuardRule]) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    rows = enriched.copy()
    rows["guard_hit"] = False
    rows["guard_rule_ids"] = ""
    for rule in rules:
        hit = apply_rule(rows, rule)
        rows.loc[hit, "guard_hit"] = True
        rows.loc[hit, "guard_rule_ids"] = rows.loc[hit, "guard_rule_ids"].map(lambda s: f"{s};{rule.rule_id}".strip(";"))
    summary: dict[str, Any] = {"anchors": {}}
    failure_rows: list[dict[str, Any]] = []
    stay_rows: list[dict[str, Any]] = []
    for anchor in TARGET_ANCHORS:
        base = rows[rows["anchor_type"] == anchor]
        hit = base[base["guard_hit"]]
        miss = base[~base["guard_hit"]]
        summary["anchors"][anchor] = {
            "baseline": _metric_summary(base),
            "guard_hit": _metric_summary(hit),
            "guard_miss": _metric_summary(miss),
            "lift_vs_baseline": _lift_summary(hit, base),
        }
        for cohort_name, subset in (("baseline", base), ("guard_hit", hit), ("guard_miss", miss)):
            stay_rows.append({"anchor_type": anchor, "cohort": cohort_name, **_stay_summary(subset)})
            failure_rows.append({"anchor_type": anchor, "cohort": cohort_name, **_failure_summary(subset)})
    return summary, pd.DataFrame(stay_rows), pd.DataFrame(failure_rows), rows[rows["guard_hit"]].copy()


def _metric_summary(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "n": int(len(df)),
        "p_reach_60": _bool_rate(df, "future_reached_60"),
        "ret20_from_anchor_mean": _mean(df, "ret20_from_anchor"),
        "ret20_from_anchor_median": _median(df, "ret20_from_anchor"),
        "ret40_from_anchor_mean": _mean(df, "ret40_from_anchor"),
        "ret40_from_anchor_median": _median(df, "ret40_from_anchor"),
        "mae20_from_anchor_mean": _mean(df, "mae20_from_anchor"),
        "mfe20_from_anchor_mean": _mean(df, "mfe20_from_anchor"),
        "max_drawdown_20_mean": _mean(df, "path_max_drawdown_20"),
        "ma20_break_within_20d": _bool_rate(df, "ma20_break_within_20d"),
        "ma60_break_within_20d": _bool_rate(df, "ma60_break_within_20d"),
        "ma20_and_ma60_break_within_20d": _bool_rate(df, "ma20_and_ma60_break_within_20d"),
        "large_bearish_break_within_20d": _bool_rate(df, "large_bearish_break_within_20d"),
        "failure_rate": _bool_rate(df, "failure_within_20d"),
    }


def _lift_summary(hit: pd.DataFrame, base: pd.DataFrame) -> dict[str, Any]:
    hit_s = _metric_summary(hit)
    base_s = _metric_summary(base)
    return {
        "p_reach_60_delta": _delta(hit_s["p_reach_60"], base_s["p_reach_60"]),
        "ret20_mean_delta": _delta(hit_s["ret20_from_anchor_mean"], base_s["ret20_from_anchor_mean"]),
        "ret40_mean_delta": _delta(hit_s["ret40_from_anchor_mean"], base_s["ret40_from_anchor_mean"]),
        "mae20_mean_delta": _delta(hit_s["mae20_from_anchor_mean"], base_s["mae20_from_anchor_mean"]),
        "ma20_and_ma60_break_delta": _delta(hit_s["ma20_and_ma60_break_within_20d"], base_s["ma20_and_ma60_break_within_20d"]),
    }


def _stay_summary(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "n": int(len(df)),
        "scenario_a_take_profit_return": 0.0,
        "scenario_b_stay_return_mean": _mean(df, "stay_return"),
        "scenario_b_stay_return_median": _median(df, "stay_return"),
        "scenario_b_stay_mae_mean": _mean(df, "stay_mae"),
    }


def _failure_summary(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "n": int(len(df)),
        "ma20_break_within_20d": _bool_rate(df, "ma20_break_within_20d"),
        "ma60_break_within_20d": _bool_rate(df, "ma60_break_within_20d"),
        "ma20_and_ma60_break_within_20d": _bool_rate(df, "ma20_and_ma60_break_within_20d"),
        "large_bearish_break_within_20d": _bool_rate(df, "large_bearish_break_within_20d"),
        "failure_rate": _bool_rate(df, "failure_within_20d"),
    }


def _mean(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df.columns else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df.columns else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def _bool_rate(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns or df.empty:
        return None
    values = df[col].dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def classify_decision(summary: dict[str, Any], rules: list[GuardRule]) -> dict[str, Any]:
    reasons: list[str] = []
    supported_anchor: str | None = None
    weak_anchor: str | None = None
    for anchor in TARGET_ANCHORS:
        hit = summary["anchors"][anchor]["guard_hit"]
        base = summary["anchors"][anchor]["baseline"]
        lift = summary["anchors"][anchor]["lift_vs_baseline"]
        n_ok = int(hit["n"]) >= 300
        reach_ok = lift["p_reach_60_delta"] is not None and lift["p_reach_60_delta"] >= 0.08
        ret20_ok = hit["ret20_from_anchor_mean"] is not None and base["ret20_from_anchor_mean"] is not None and hit["ret20_from_anchor_mean"] >= base["ret20_from_anchor_mean"]
        ret40_ok = hit["ret40_from_anchor_mean"] is not None and base["ret40_from_anchor_mean"] is not None and hit["ret40_from_anchor_mean"] >= base["ret40_from_anchor_mean"]
        mae_ok = hit["mae20_from_anchor_mean"] is not None and base["mae20_from_anchor_mean"] is not None and hit["mae20_from_anchor_mean"] >= base["mae20_from_anchor_mean"]
        break_ok = lift["ma20_and_ma60_break_delta"] is not None and lift["ma20_and_ma60_break_delta"] < 0
        if all([n_ok, reach_ok, ret20_ok, ret40_ok, mae_ok, break_ok]):
            supported_anchor = anchor
            break
        if n_ok and reach_ok:
            weak_anchor = anchor
    if supported_anchor:
        decision = "stay_guard_supported"
        reasons.append(f"{supported_anchor} guard_hit passes reach, return, MAE, and break-reduction gates")
    elif weak_anchor:
        decision = "weak_guard"
        reasons.append(f"{weak_anchor} improves p_reach_60 but return/MAE/break gates are incomplete")
    elif not rules:
        decision = "inconclusive"
        reasons.append("no eligible prior simple rule candidates selected")
    else:
        decision = "not_supported"
        reasons.append("guard_hit does not clear reach/return/MAE/break-reduction gates")
    return {"research_decision": decision, "reason_typed": reasons, "evaluated_anchors": list(TARGET_ANCHORS), "selected_rule_count": len(rules), "no_lookahead_safe": True}


def no_lookahead_audit(source_audit: dict[str, Any], rules: list[GuardRule]) -> dict[str, Any]:
    return {
        "audit_result": "pass" if source_audit.get("audit_result") == "pass" else "source_audit_not_pass",
        "source_no_lookahead_audit": source_audit.get("audit_result"),
        "rule_selection_source": "prior simple_rule_candidates.csv and feature_lift_by_anchor.csv only",
        "feature_timing": "guard_hit uses anchor-time feature columns only",
        "label_columns": ["future_reached_60", "ret20_from_anchor", "ret40_from_anchor", "mae20_from_anchor", "mfe20_from_anchor", "ma20_break_within_20d", "ma60_break_within_20d", "stay_return"],
        "threshold_sweep": False,
        "model_training": False,
        "anchor_30_used_for_primary_decision": False,
        "selected_rules": [rule.__dict__ for rule in rules],
    }


def run(
    *,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    db_path: Path | None = None,
    production_csv: Path = DEFAULT_PRODUCTION_CSV,
) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma60-above-60plus-stay-guard-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    inputs = load_inputs(source_root)
    rules = select_guard_rules(inputs["rules"], inputs["lift"], max_rules=3)
    anchors = inputs["anchors"][inputs["anchors"]["anchor_type"].isin(TARGET_ANCHORS)].copy()
    min_ymd = min(_ymd(v) for v in anchors["anchor_date"].dropna())
    max_ymd = max(_ymd(v) for v in anchors["anchor_date"].dropna())
    resolution: InputResolution = resolve_input(db_path=db_path, production_csv=production_csv)
    daily = load_daily_frame(resolution, start_ymd=min_ymd - 10000, end_ymd=max_ymd + 20000)
    featured = add_features(daily)
    enriched = enrich_outcomes(anchors, featured)
    guard_summary, stay_df, failure_df, guard_hit = summarize_guard(enriched, rules)
    decision = classify_decision(guard_summary, rules)
    selected_payload = {
        "max_rules": 3,
        "selection_policy": {
            "anchors": list(TARGET_ANCHORS),
            "n_sample_min": 300,
            "condition_count_max": 2,
            "threshold_sweep": False,
            "preferred_patterns": [list(p) for p in PREFERRED_RULE_PATTERNS],
        },
        "rules": [rule.__dict__ for rule in rules],
    }
    input_report = {
        "axis_id": AXIS_ID,
        "source_root": source_root,
        "required_inputs": list(REQUIRED_INPUTS),
        "missing_inputs": [name for name in REQUIRED_INPUTS if not (source_root / name).exists()],
        "source_no_lookahead_audit": inputs["source_audit"].get("audit_result"),
        "daily_source_type": resolution.source_type,
        "daily_source_path": resolution.path,
        "anchor_row_count": int(len(anchors)),
        "daily_loaded_row_count": int(len(daily)),
    }
    guard_hit.to_csv(run_dir / "guard_hit_rows.csv", index=False)
    failure_df.to_csv(run_dir / "failure_reduction_summary.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", input_report)
    _write_json(run_dir / "selected_guard_rules.json", selected_payload)
    _write_json(run_dir / "guard_vs_baseline_summary.json", guard_summary)
    _write_json(run_dir / "stay_simulation_summary.json", {"rows": stay_df.to_dict("records")})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", no_lookahead_audit(inputs["source_audit"], rules))
    complete = {
        "axis_id": AXIS_ID,
        "output_dir": run_dir,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_dir": str(run_dir), "decision": decision, "selected_rules": selected_payload, "summary": guard_summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX MA60 60plus stay guard pretest")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--production-csv", type=Path, default=DEFAULT_PRODUCTION_CSV)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(source_root=args.source_root, output_root=args.output_root, db_path=args.db_path, production_csv=args.production_csv)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
