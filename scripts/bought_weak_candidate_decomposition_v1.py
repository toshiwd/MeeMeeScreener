from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "bought_weak_candidate_decomposition_v1"
SCHEMA_PREFIX = "tradex_bought_weak_candidate_decomposition_v1"
DEFAULT_DIAG_DIR_NAME = "diagnosis_v1"
DEFAULT_OUTPUT_DIR_NAME = "bought_weak_candidate_decomposition_v1"

SOURCE_ARTIFACTS = (
    "daily_candidate_snapshot.csv",
    "daily_action_ledger.jsonl",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "rejected_candidates.csv",
    "post_run_outcome_labels.csv",
    "failure_diagnosis_summary.json",
    "selection_feature_manifest.json",
    "no_lookahead_audit.json",
)
DIAG_ARTIFACTS = ("bought_weak_candidate_cases.csv", "trade_contribution.csv")
OUTPUT_ARTIFACTS = (
    "bought_weak_candidate_decomposition_summary.json",
    "weak_buy_cases_enriched.csv",
    "weak_buy_feature_distribution.csv",
    "weak_buy_reason_code_distribution.csv",
    "weak_buy_rank_bucket_summary.csv",
    "weak_buy_regime_summary.csv",
    "weak_buy_mae_mfe_summary.csv",
    "weak_buy_same_day_alternatives.csv",
    "weak_buy_veto_candidate_rules.json",
    "next_veto_pretest_plan.json",
    "_ARTIFACT_COMPLETE.json",
)
FEATURE_COLUMNS = (
    "daily_ma_stack",
    "daily_ma60_slope_state",
    "daily_ret20_state",
    "daily_candle_state",
    "daily_volume_state",
    "daily_sequence_state",
    "weekly_trend_state",
    "weekly_ret4_state",
    "monthly_trend_state",
    "monthly_ret6_state",
)
RANK_BUCKETS = (
    (1, 5, "rank_1_5"),
    (6, 10, "rank_6_10"),
    (11, 20, "rank_11_20"),
    (21, 50, "rank_21_50"),
    (51, 100, "rank_51_100"),
)
NEXT_AXIS_CANDIDATES = ("bought_weak_candidate",)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
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
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    frame.to_csv(path, index=False)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _rank_bucket(rank: Any) -> str:
    value = _safe_float(rank)
    if value is None:
        return "rank_unknown"
    ivalue = int(value)
    for lo, hi, label in RANK_BUCKETS:
        if lo <= ivalue <= hi:
            return label
    return "rank_over_100"


def _parse_components(raw: Any) -> dict[str, Any]:
    if raw is None or pd.isna(raw):
        return {}
    try:
        items = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    out: dict[str, Any] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        feature = str(item.get("feature") or "").strip()
        if not feature:
            continue
        out[feature] = item.get("value")
        out[f"{feature}_points"] = item.get("points")
    return out


def _add_feature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    parsed = [_parse_components(value) for value in work.get("score_components_json", pd.Series(dtype=object))]
    for column in FEATURE_COLUMNS:
        work[column] = [item.get(column) for item in parsed]
    work["reason_codes"] = [
        "|".join(f"{col}={item.get(col)}" for col in FEATURE_COLUMNS if item.get(col) is not None)
        for item in parsed
    ]
    return work


def _source_status(run_root: Path, diag_root: Path) -> dict[str, bool]:
    status = {name: (run_root / name).exists() for name in SOURCE_ARTIFACTS}
    status.update({f"{DEFAULT_DIAG_DIR_NAME}/{name}": (diag_root / name).exists() for name in DIAG_ARTIFACTS})
    return status


def _selected_good_labels(labels: pd.DataFrame, weak_keys: set[tuple[int, str]]) -> pd.DataFrame:
    selected = labels[labels["was_selected"] == True].copy()
    selected["_key"] = list(zip(selected["decision_ymd"].astype(int), selected["code"].astype(str)))
    good = selected[~selected["_key"].isin(weak_keys)].copy()
    return good.drop(columns=["_key"])


def enrich_weak_cases(
    weak: pd.DataFrame,
    candidates: pd.DataFrame,
    orders: pd.DataFrame,
    positions: pd.DataFrame,
) -> pd.DataFrame:
    enriched = _add_feature_columns(weak)
    enriched["rank_bucket"] = enriched["candidate_rank"].apply(_rank_bucket)
    buy_orders = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    buy_orders = buy_orders.rename(columns={"decision_ymd": "entry_decision_ymd", "execution_ymd": "entry_execution_ymd"})
    enriched = enriched.merge(
        buy_orders[["entry_decision_ymd", "code", "order_id", "position_id", "entry_execution_ymd", "execution_price", "shares", "notional", "cost_amount"]],
        left_on=["decision_ymd", "code"],
        right_on=["entry_decision_ymd", "code"],
        how="left",
    )
    timing_rows: list[dict[str, Any]] = []
    for _idx, row in enriched.iterrows():
        position_id = row.get("position_id")
        history = positions[positions["position_id"].astype(str) == str(position_id)].copy() if position_id is not None and not pd.isna(position_id) else pd.DataFrame()
        if history.empty:
            timing_rows.append({"position_id": position_id, "mae_first_5_holding_days": None, "mae_observed_holding": None, "mfe_observed_holding": None, "entry_timing_bucket": "position_history_unavailable"})
            continue
        history["return_from_cost_basis"] = pd.to_numeric(history["unrealized_pnl"], errors="coerce") / pd.to_numeric(history["cost_basis"], errors="coerce")
        first5 = history[pd.to_numeric(history["holding_days"], errors="coerce") <= 5]
        mae5 = None if first5.empty else float(first5["return_from_cost_basis"].min())
        mae_all = float(history["return_from_cost_basis"].min())
        mfe_all = float(history["return_from_cost_basis"].max())
        post5 = _safe_float(row.get("post_ret_5"))
        post20 = _safe_float(row.get("post_ret_20"))
        if (mae5 is not None and mae5 <= -0.03) or (post5 is not None and post5 < 0.0):
            bucket = "immediate_adverse_within_5d"
        elif (mae_all <= -0.06) or (post20 is not None and post20 < 0.0):
            bucket = "delayed_breakdown_after_initial_hold"
        else:
            bucket = "weak_vs_alternative_not_absolute_loss"
        timing_rows.append(
            {
                "position_id": position_id,
                "mae_first_5_holding_days": mae5,
                "mae_observed_holding": mae_all,
                "mfe_observed_holding": mfe_all,
                "entry_timing_bucket": bucket,
            }
        )
    timing = pd.DataFrame(timing_rows).drop(columns=["position_id"], errors="ignore")
    if not timing.empty:
        enriched = pd.concat([enriched.reset_index(drop=True), timing.reset_index(drop=True)], axis=1)
    candidate_cols = ["decision_ymd", "code", "entry_allowed_by_score", "downside_guard_blocked", "next_open_available"]
    existing_cols = [col for col in candidate_cols if col in candidates.columns]
    if existing_cols:
        enriched = enriched.merge(candidates[existing_cols].drop_duplicates(["decision_ymd", "code"]), on=["decision_ymd", "code"], how="left")
    return enriched


def build_feature_distribution(weak_enriched: pd.DataFrame, good_enriched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weak_n = len(weak_enriched)
    good_n = len(good_enriched)
    for feature in FEATURE_COLUMNS:
        values = sorted(set(weak_enriched.get(feature, pd.Series(dtype=object)).dropna().astype(str)) | set(good_enriched.get(feature, pd.Series(dtype=object)).dropna().astype(str)))
        for value in values:
            weak_count = int((weak_enriched.get(feature, pd.Series(dtype=object)).astype(str) == value).sum())
            good_count = int((good_enriched.get(feature, pd.Series(dtype=object)).astype(str) == value).sum())
            weak_rate = weak_count / weak_n if weak_n else 0.0
            good_rate = good_count / good_n if good_n else 0.0
            rows.append(
                {
                    "feature": feature,
                    "value": value,
                    "weak_buy_count": weak_count,
                    "good_buy_count": good_count,
                    "weak_buy_rate": weak_rate,
                    "good_buy_rate": good_rate,
                    "weak_minus_good_rate": weak_rate - good_rate,
                    "overrepresentation_ratio": (weak_rate / good_rate) if good_rate else None,
                }
            )
    return pd.DataFrame(rows).sort_values(["weak_minus_good_rate", "weak_buy_count"], ascending=[False, False], kind="stable")


def build_reason_code_distribution(weak_enriched: pd.DataFrame, good_enriched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weak_counter = Counter(weak_enriched["reason_codes"].fillna("").astype(str))
    good_counter = Counter(good_enriched["reason_codes"].fillna("").astype(str))
    weak_n = len(weak_enriched)
    good_n = len(good_enriched)
    for reason in sorted(set(weak_counter) | set(good_counter)):
        rows.append(
            {
                "reason_code_signature": reason,
                "weak_buy_count": int(weak_counter.get(reason, 0)),
                "good_buy_count": int(good_counter.get(reason, 0)),
                "weak_buy_rate": weak_counter.get(reason, 0) / weak_n if weak_n else 0.0,
                "good_buy_rate": good_counter.get(reason, 0) / good_n if good_n else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values(["weak_buy_count", "good_buy_count"], ascending=[False, True], kind="stable")


def build_rank_bucket_summary(weak_enriched: pd.DataFrame, good_enriched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weak_enriched = weak_enriched.copy()
    good_enriched = good_enriched.copy()
    weak_enriched["rank_bucket"] = weak_enriched["candidate_rank"].apply(_rank_bucket)
    good_enriched["rank_bucket"] = good_enriched["candidate_rank"].apply(_rank_bucket)
    for _lo, _hi, label in RANK_BUCKETS:
        weak_subset = weak_enriched[weak_enriched["rank_bucket"] == label]
        good_subset = good_enriched[good_enriched["rank_bucket"] == label]
        rows.append(
            {
                "rank_bucket": label,
                "weak_buy_count": int(len(weak_subset)),
                "good_buy_count": int(len(good_subset)),
                "weak_buy_rate": len(weak_subset) / len(weak_enriched) if len(weak_enriched) else 0.0,
                "good_buy_rate": len(good_subset) / len(good_enriched) if len(good_enriched) else 0.0,
                "mean_weak_post_ret20": float(pd.to_numeric(weak_subset.get("post_ret_20", pd.Series(dtype=float)), errors="coerce").mean()) if not weak_subset.empty else None,
                "mean_good_post_ret20": float(pd.to_numeric(good_subset.get("post_ret_20", pd.Series(dtype=float)), errors="coerce").mean()) if not good_subset.empty else None,
            }
        )
    return pd.DataFrame(rows)


def build_regime_summary(feature_distribution: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_regime_summary_v1",
        "monthly": feature_distribution[feature_distribution["feature"].str.startswith("monthly_")].to_dict("records"),
        "weekly": feature_distribution[feature_distribution["feature"].str.startswith("weekly_")].to_dict("records"),
        "daily": feature_distribution[feature_distribution["feature"].str.startswith("daily_")].to_dict("records"),
        "availability": {
            "ma7_ma20_ma60_state": "represented_by_daily_ma_stack",
            "volume_state": "available_as_daily_weekly_monthly_volume_state_when_present_in_score_components",
            "event_risk_flag": "unavailable_in_requested_artifacts",
            "liquidity_flag": "unavailable_in_requested_artifacts",
        },
    }


def build_mae_mfe_summary(weak_enriched: pd.DataFrame, good_enriched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for label, frame in (("weak_buy", weak_enriched), ("good_buy", good_enriched)):
        rows.append(
            {
                "cohort": label,
                "count": int(len(frame)),
                "mean_post_ret5": float(pd.to_numeric(frame.get("post_ret_5", pd.Series(dtype=float)), errors="coerce").mean()) if len(frame) else None,
                "mean_post_ret20": float(pd.to_numeric(frame.get("post_ret_20", pd.Series(dtype=float)), errors="coerce").mean()) if len(frame) else None,
                "mean_mae20": float(pd.to_numeric(frame.get("mae_20", pd.Series(dtype=float)), errors="coerce").mean()) if len(frame) else None,
                "mean_mfe20": float(pd.to_numeric(frame.get("mfe_20", pd.Series(dtype=float)), errors="coerce").mean()) if len(frame) else None,
                "immediate_adverse_count": int((frame.get("entry_timing_bucket", pd.Series(dtype=str)) == "immediate_adverse_within_5d").sum()) if "entry_timing_bucket" in frame else None,
                "delayed_breakdown_count": int((frame.get("entry_timing_bucket", pd.Series(dtype=str)) == "delayed_breakdown_after_initial_hold").sum()) if "entry_timing_bucket" in frame else None,
            }
        )
    return pd.DataFrame(rows)


def build_same_day_alternatives(weak_enriched: pd.DataFrame, labels: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    rejected_labels = labels[labels["was_selected"] == False].copy()
    candidate_features = _add_feature_columns(candidates)
    for _idx, weak in weak_enriched.iterrows():
        decision_ymd = int(weak["decision_ymd"])
        weak_ret = _safe_float(weak.get("post_ret_20"))
        alternatives = rejected_labels[
            (rejected_labels["decision_ymd"].astype(int) == decision_ymd)
            & (pd.to_numeric(rejected_labels["post_ret_20"], errors="coerce") > (weak_ret if weak_ret is not None else -999.0))
        ].copy()
        alternatives = alternatives.sort_values("post_ret_20", ascending=False, kind="stable").head(5)
        for _aidx, alt in alternatives.iterrows():
            alt_features = candidate_features[(candidate_features["decision_ymd"].astype(int) == decision_ymd) & (candidate_features["code"].astype(str) == str(alt["code"]))]
            alt_row = alt_features.iloc[0].to_dict() if not alt_features.empty else {}
            diff_features = []
            for feature in FEATURE_COLUMNS:
                if weak.get(feature) != alt_row.get(feature):
                    diff_features.append(f"{feature}:{weak.get(feature)}->{alt_row.get(feature)}")
            rows.append(
                {
                    "decision_ymd": decision_ymd,
                    "weak_code": str(weak["code"]),
                    "weak_candidate_rank": weak.get("candidate_rank"),
                    "weak_selection_score": weak.get("selection_score"),
                    "weak_post_ret20": weak_ret,
                    "alternative_code": str(alt["code"]),
                    "alternative_post_ret20": _safe_float(alt.get("post_ret_20")),
                    "alternative_candidate_rank": alt_row.get("candidate_rank"),
                    "alternative_selection_score": alt_row.get("selection_score"),
                    "post_ret20_advantage": (_safe_float(alt.get("post_ret_20")) or 0.0) - (weak_ret or 0.0),
                    "same_day_feature_differences": "|".join(diff_features),
                    "distinguishable_by_same_day_features": bool(diff_features),
                }
            )
    return pd.DataFrame(rows).sort_values(["post_ret20_advantage", "decision_ymd"], ascending=[False, True], kind="stable") if rows else pd.DataFrame()


def _rule_mask(frame: pd.DataFrame, rule: dict[str, Any]) -> pd.Series:
    feature = str(rule["feature"])
    op = rule.get("op")
    value = rule.get("value")
    if feature not in frame.columns:
        return pd.Series(False, index=frame.index)
    if op == "eq":
        return frame[feature].astype(str) == str(value)
    if op == "rank_gte":
        return pd.to_numeric(frame[feature], errors="coerce") >= float(value)
    if op == "score_lte":
        return pd.to_numeric(frame[feature], errors="coerce") <= float(value)
    return pd.Series(False, index=frame.index)


def build_veto_candidate_rules(weak_enriched: pd.DataFrame, good_enriched: pd.DataFrame, feature_distribution: pd.DataFrame) -> dict[str, Any]:
    rules: list[dict[str, Any]] = []
    good_n = max(1, len(good_enriched))
    for row in feature_distribution.head(30).to_dict("records"):
        if int(row.get("weak_buy_count") or 0) < 3:
            continue
        if float(row.get("weak_minus_good_rate") or 0.0) <= 0.05:
            continue
        rule = {"feature": row["feature"], "op": "eq", "value": row["value"]}
        weak_hits = int(_rule_mask(weak_enriched, rule).sum())
        good_hits = int(_rule_mask(good_enriched, rule).sum())
        false_good_rate = good_hits / good_n
        rules.append(
            {
                "rule_id": f"feature_{len(rules)+1}",
                "description": f"{row['feature']} == {row['value']}",
                "uses_only_same_day_features": True,
                "rule": rule,
                "captured_weak_buy_count": weak_hits,
                "false_veto_good_buy_count": good_hits,
                "false_veto_good_buy_rate": false_good_rate,
                "expected_false_veto_risk": false_good_rate,
                "diagnostic_only_outcome_used_for_evaluation": True,
                "implementation_status": "candidate_only_not_implemented",
            }
        )
    supplemental = [
        {"feature": "candidate_rank", "op": "rank_gte", "value": 4, "description": "candidate_rank >= 4"},
        {"feature": "selection_score", "op": "score_lte", "value": 10, "description": "selection_score <= 10"},
    ]
    for rule_info in supplemental:
        rule = {k: rule_info[k] for k in ("feature", "op", "value")}
        weak_hits = int(_rule_mask(weak_enriched, rule).sum())
        good_hits = int(_rule_mask(good_enriched, rule).sum())
        if weak_hits == 0:
            continue
        rules.append(
            {
                "rule_id": f"structural_{len(rules)+1}",
                "description": rule_info["description"],
                "uses_only_same_day_features": True,
                "rule": rule,
                "captured_weak_buy_count": weak_hits,
                "false_veto_good_buy_count": good_hits,
                "false_veto_good_buy_rate": good_hits / good_n,
                "expected_false_veto_risk": good_hits / good_n,
                "diagnostic_only_outcome_used_for_evaluation": True,
                "implementation_status": "candidate_only_not_implemented",
            }
        )
    for item in rules:
        item["pretest_priority"] = "eligible" if float(item["false_veto_good_buy_rate"]) <= 0.30 else "hold_false_veto_risk_high"
    rules = sorted(
        rules,
        key=lambda item: (
            item["pretest_priority"] != "eligible",
            -item["captured_weak_buy_count"],
            item["false_veto_good_buy_rate"],
            item["rule_id"],
        ),
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_veto_candidate_rules_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_rules": rules,
        "rule_construction_policy": {
            "selection_features_only": True,
            "post_ret_mae_mfe_used_only_for_diagnostic_scoring": True,
            "rules_implemented": False,
            "replay_rerun": False,
        },
    }


def build_next_veto_pretest_plan(rules_payload: dict[str, Any]) -> dict[str, Any]:
    candidates = list(rules_payload.get("candidate_rules") or [])
    selected = candidates[:3]
    active_axis = selected[0]["rule_id"] if selected else None
    return {
        "schema_version": f"{SCHEMA_PREFIX}_next_veto_pretest_plan_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "selected_veto_candidates": selected,
        "selected_count": len(selected),
        "next_single_axis_rule_id": active_axis,
        "next_single_axis": "bought_weak_candidate",
        "pretest_policy": {
            "max_candidates": 3,
            "actual_next_axis_count": 1,
            "no_replay_condition_change": True,
            "no_rule_change_in_this_decomposition": True,
        },
    }


def run_bought_weak_candidate_decomposition_v1(
    *,
    run_root: str | Path,
    diagnosis_root: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_root).expanduser().resolve()
    diag_root = Path(diagnosis_root).expanduser().resolve() if diagnosis_root else root / DEFAULT_DIAG_DIR_NAME
    out = Path(output_dir).expanduser().resolve() if output_dir else root / DEFAULT_OUTPUT_DIR_NAME
    out.mkdir(parents=True, exist_ok=True)
    source_status = _source_status(root, diag_root)
    missing = [name for name, exists in source_status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing source artifacts: {missing}")

    candidates = pd.read_csv(root / "daily_candidate_snapshot.csv")
    orders = pd.read_csv(root / "orders_ledger.csv")
    positions = pd.read_csv(root / "positions_ledger.csv")
    rejected = pd.read_csv(root / "rejected_candidates.csv")
    labels = pd.read_csv(root / "post_run_outcome_labels.csv")
    weak = pd.read_csv(diag_root / "bought_weak_candidate_cases.csv")
    trade_contribution = pd.read_csv(diag_root / "trade_contribution.csv")
    failure = _read_json(root / "failure_diagnosis_summary.json")
    selection_manifest = _read_json(root / "selection_feature_manifest.json")
    no_lookahead = _read_json(root / "no_lookahead_audit.json")
    _actions = _read_jsonl(root / "daily_action_ledger.jsonl")

    weak_enriched = enrich_weak_cases(weak, candidates, orders, positions)
    weak_keys = set(zip(weak_enriched["decision_ymd"].astype(int), weak_enriched["code"].astype(str)))
    good_labels = _selected_good_labels(labels, weak_keys)
    good_enriched = _add_feature_columns(good_labels.merge(candidates, on=["decision_ymd", "code"], how="left", suffixes=("", "_candidate")))
    good_enriched["rank_bucket"] = good_enriched["candidate_rank"].apply(_rank_bucket)

    feature_distribution = build_feature_distribution(weak_enriched, good_enriched)
    reason_distribution = build_reason_code_distribution(weak_enriched, good_enriched)
    rank_summary = build_rank_bucket_summary(weak_enriched, good_enriched)
    regime_summary = build_regime_summary(feature_distribution)
    mae_mfe_summary = build_mae_mfe_summary(weak_enriched, good_enriched)
    alternatives = build_same_day_alternatives(weak_enriched, labels, candidates)
    veto_rules = build_veto_candidate_rules(weak_enriched, good_enriched, feature_distribution)
    pretest_plan = build_next_veto_pretest_plan(veto_rules)

    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_run_root": str(root),
        "diagnosis_root": str(diag_root),
        "source_gate_status": {
            "no_lookahead_audit": no_lookahead.get("audit_result"),
            "selection_feature_manifest": selection_manifest.get("audit_result"),
            "replay_rerun": False,
            "rule_changed": False,
            "silent_fallback_used": False,
        },
        "counts": {
            "weak_buy_cases": int(len(weak_enriched)),
            "good_buy_cases": int(len(good_enriched)),
            "same_day_alternative_rows": int(len(alternatives)),
            "veto_candidate_rules": int(len(veto_rules["candidate_rules"])),
        },
        "headline": {
            "weak_buy_rank_bucket_top": rank_summary.sort_values("weak_buy_count", ascending=False).head(1).to_dict("records")[0] if not rank_summary.empty else None,
            "top_overrepresented_feature": feature_distribution.head(1).to_dict("records")[0] if not feature_distribution.empty else None,
            "entry_timing_bucket_counts": weak_enriched["entry_timing_bucket"].value_counts().to_dict() if "entry_timing_bucket" in weak_enriched else {},
            "next_single_axis_rule_id": pretest_plan.get("next_single_axis_rule_id"),
        },
        "diagnostic_limitations": {
            "event_risk_flag_available": False,
            "liquidity_flag_available": False,
            "mae_5_exact_available": False,
            "mae_5_proxy": "observed holding ledger first 5 holding days plus post_ret_5",
        },
        "judgment": {
            "decomposition_decision": "ready_for_veto_pretest" if pretest_plan.get("selected_count", 0) > 0 else "hold_no_clean_veto_candidate",
            "next_axis": "bought_weak_candidate",
        },
    }

    paths: dict[str, str] = {}
    paths["bought_weak_candidate_decomposition_summary.json"] = str(_write_json(out / "bought_weak_candidate_decomposition_summary.json", summary))
    paths["weak_buy_cases_enriched.csv"] = str(_write_csv(out / "weak_buy_cases_enriched.csv", weak_enriched))
    paths["weak_buy_feature_distribution.csv"] = str(_write_csv(out / "weak_buy_feature_distribution.csv", feature_distribution))
    paths["weak_buy_reason_code_distribution.csv"] = str(_write_csv(out / "weak_buy_reason_code_distribution.csv", reason_distribution))
    paths["weak_buy_rank_bucket_summary.csv"] = str(_write_csv(out / "weak_buy_rank_bucket_summary.csv", rank_summary))
    paths["weak_buy_regime_summary.csv"] = str(_write_csv(out / "weak_buy_regime_summary.csv", pd.DataFrame(
        feature_distribution[feature_distribution["feature"].isin(FEATURE_COLUMNS)].to_dict("records")
    )))
    _write_json(out / "weak_buy_regime_summary.json", regime_summary)
    paths["weak_buy_mae_mfe_summary.csv"] = str(_write_csv(out / "weak_buy_mae_mfe_summary.csv", mae_mfe_summary))
    paths["weak_buy_same_day_alternatives.csv"] = str(_write_csv(out / "weak_buy_same_day_alternatives.csv", alternatives))
    paths["weak_buy_veto_candidate_rules.json"] = str(_write_json(out / "weak_buy_veto_candidate_rules.json", veto_rules))
    paths["next_veto_pretest_plan.json"] = str(_write_json(out / "next_veto_pretest_plan.json", pretest_plan))
    output_status = {name: (out / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(out),
        "source_artifacts_present": source_status,
        "output_artifacts_present": output_status,
        "complete": all(source_status.values()) and all(output_status.values()),
        "replay_rerun": False,
        "rule_changed": False,
        "selected_veto_candidate_count": int(pretest_plan.get("selected_count", 0)),
        "next_single_axis": pretest_plan.get("next_single_axis"),
        "next_single_axis_rule_id": pretest_plan.get("next_single_axis_rule_id"),
        "silent_fallback_used": False,
    }
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(out / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(out),
        "paths": paths,
        "complete": complete["complete"],
        "weak_buy_cases": int(len(weak_enriched)),
        "good_buy_cases": int(len(good_enriched)),
        "same_day_alternative_rows": int(len(alternatives)),
        "selected_veto_candidate_count": int(pretest_plan.get("selected_count", 0)),
        "next_single_axis_rule_id": pretest_plan.get("next_single_axis_rule_id"),
        "decomposition_decision": summary["judgment"]["decomposition_decision"],
        "silent_fallback_used": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--diagnosis-root", default="")
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args(argv)
    result = run_bought_weak_candidate_decomposition_v1(
        run_root=args.run_root,
        diagnosis_root=args.diagnosis_root.strip() or None,
        output_dir=args.output_dir.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
