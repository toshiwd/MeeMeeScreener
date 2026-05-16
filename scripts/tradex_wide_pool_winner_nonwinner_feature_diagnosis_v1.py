from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_threshold_no_trade_control_for_wide_pool_v1 as threshold_mod


AXIS_ID = "wide_pool_winner_nonwinner_feature_diagnosis_v1"
SCHEMA_PREFIX = "tradex_wide_pool_winner_nonwinner_feature_diagnosis_v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_RISK_RUN_ID = "20260513T040000Z-selection-risk-control-for-wide-pool-v1"
DEFAULT_THRESHOLD_RUN_ID = "20260513T050000Z-threshold-no-trade-control-for-wide-pool-v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_RISK_ROOT = Path(r"G:\Tradex\selection_risk_control_for_wide_pool_v1")
DEFAULT_THRESHOLD_ROOT = Path(r"G:\Tradex\threshold_no_trade_control_for_wide_pool_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\wide_pool_winner_nonwinner_feature_diagnosis_v1")

TOP_K = 3
RANDOM_SEED = 20260513
BASE_SCORE_COLUMN = threshold_mod.BASE_SCORE_COLUMN

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "group_definition_contract.json",
    "feature_availability_audit.json",
    "leakage_audit.json",
    "same_date_pair_ledger.jsonl",
    "score_bucket_pair_ledger.jsonl",
    "feature_contrast_report.json",
    "same_date_contrast_report.json",
    "score_bucket_contrast_report.json",
    "negative_guard_decomposition_report.json",
    "time_block_stability.json",
    "candidate_feature_shortlist.json",
    "rejected_feature_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

NUMERIC_FEATURE_COLUMNS = {
    "event_strength_score",
    "score_momentum_continuation_soft_boost_v1",
    "score_hybrid_reclaim_momentum_soft_risk_v1",
    "score_baseline_momentum_continuation_soft_boost_v1",
    "score_severe_loss_soft_penalty_v1",
    BASE_SCORE_COLUMN,
    "continuation_signal_score",
    "blowoff_signal_score",
    "past_only_severe_loss_risk_estimate",
    "past_only_bad_pick_risk_estimate",
    "past_only_mae_abs_risk_estimate",
    "threshold_risk_value",
    "same_date_score_rank",
    "score_margin_to_next_candidate",
}
CATEGORICAL_FEATURE_COLUMNS = {
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "pre_candle_energy_state",
    "pre_wick_warning_state",
    "pre_volume_state",
    "pre_compression_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "event_daily_ret20_state",
    "event_daily_candle_state",
    "risk_estimate_source",
}
TAG_FEATURE_COLUMNS = {
    "guard_safe_full",
    "negative_guard_match",
}
FUTURE_LABEL_COLUMNS = set(threshold_mod.FUTURE_LABEL_COLUMNS) | {
    "ret20_fwd",
    "mfe20",
    "mae20",
    "severe_loss20",
    "win20",
    "is_future_top10_by_ret20",
    "is_future_top5_by_ret20",
    "is_big_winner_ret20_ge_10pct",
    "is_big_winner_MFE20_ge_15pct",
    "future_winner",
    "selected_nonwinner",
    "selected_severe_loser",
    "negative_guard_continuation_winner",
    "negative_guard_blowoff_loser",
    "same_day_oracle_miss",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
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


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return _safe_path(root, default_root) / run_id


def _safe_rate(count: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return float(count) / float(total)


def _available_feature_columns(events: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    numeric = sorted(column for column in NUMERIC_FEATURE_COLUMNS if column in events.columns)
    categorical = sorted(column for column in CATEGORICAL_FEATURE_COLUMNS if column in events.columns)
    tags = sorted(column for column in TAG_FEATURE_COLUMNS if column in events.columns)
    return numeric, categorical, tags


def _present_mask(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
        return series.notna()
    text = series.astype("string")
    return series.notna() & text.str.strip().ne("")


def _ks_statistic(left: pd.Series, right: pd.Series) -> float | None:
    a = pd.to_numeric(left, errors="coerce").dropna().sort_values()
    b = pd.to_numeric(right, errors="coerce").dropna().sort_values()
    if a.empty or b.empty:
        return None
    values = sorted(set(a.tolist() + b.tolist()))
    if not values:
        return None
    max_delta = 0.0
    for value in values:
        max_delta = max(max_delta, abs(float((a <= value).mean()) - float((b <= value).mean())))
    return float(max_delta)


def _numeric_contrast(left: pd.DataFrame, right: pd.DataFrame, feature: str) -> dict[str, Any]:
    lval = pd.to_numeric(left[feature], errors="coerce").dropna()
    rval = pd.to_numeric(right[feature], errors="coerce").dropna()
    lmean = float(lval.mean()) if not lval.empty else None
    rmean = float(rval.mean()) if not rval.empty else None
    lmedian = float(lval.median()) if not lval.empty else None
    rmedian = float(rval.median()) if not rval.empty else None
    pooled = None
    smd = None
    if len(lval) >= 2 and len(rval) >= 2:
        pooled = math.sqrt((float(lval.var(ddof=1)) + float(rval.var(ddof=1))) / 2.0)
    if pooled and lmean is not None and rmean is not None:
        smd = float((lmean - rmean) / pooled)
    elif lmean is not None and rmean is not None:
        smd = 0.0 if lmean == rmean else float(math.copysign(9.99, lmean - rmean))
    return {
        "feature_id": feature,
        "feature_type": "numeric",
        "left_count": int(len(lval)),
        "right_count": int(len(rval)),
        "left_mean": lmean,
        "right_mean": rmean,
        "standardized_mean_difference": smd,
        "median_delta": None if lmedian is None or rmedian is None else float(lmedian - rmedian),
        "KS_statistic_if_numeric": _ks_statistic(lval, rval),
        "categorical_lift_if_categorical": None,
    }


def _categorical_contrast(left: pd.DataFrame, right: pd.DataFrame, feature: str, feature_type: str) -> dict[str, Any]:
    lval = left[feature].astype(str).fillna("")
    rval = right[feature].astype(str).fillna("")
    categories = sorted(set(lval.tolist() + rval.tolist()))
    rows = []
    max_lift = 0.0
    for category in categories:
        lp = float((lval == category).mean()) if len(lval) else 0.0
        rp = float((rval == category).mean()) if len(rval) else 0.0
        lift = lp - rp
        max_lift = lift if abs(lift) > abs(max_lift) else max_lift
        rows.append({"category": category, "left_rate": lp, "right_rate": rp, "rate_delta": lift})
    return {
        "feature_id": feature,
        "feature_type": feature_type,
        "left_count": int(len(lval)),
        "right_count": int(len(rval)),
        "left_mean": None,
        "right_mean": None,
        "standardized_mean_difference": None,
        "median_delta": None,
        "KS_statistic_if_numeric": None,
        "categorical_lift_if_categorical": float(max_lift),
        "category_rows": rows[:25],
    }


def _contrast(
    events: pd.DataFrame,
    left_mask: pd.Series,
    right_mask: pd.Series,
    *,
    left_group: str,
    right_group: str,
    numeric_features: list[str],
    categorical_features: list[str],
    tag_features: list[str],
) -> list[dict[str, Any]]:
    left = events[left_mask].copy()
    right = events[right_mask].copy()
    rows: list[dict[str, Any]] = []
    for feature in numeric_features:
        row = _numeric_contrast(left, right, feature)
        row.update({"left_group": left_group, "right_group": right_group})
        rows.append(row)
    for feature in categorical_features:
        row = _categorical_contrast(left, right, feature, "categorical")
        row.update({"left_group": left_group, "right_group": right_group})
        rows.append(row)
    for feature in tag_features:
        row = _numeric_contrast(left.assign(**{feature: left[feature].astype(float)}), right.assign(**{feature: right[feature].astype(float)}), feature)
        row.update({"left_group": left_group, "right_group": right_group, "feature_type": "tag"})
        rows.append(row)
    return rows


def _feature_effect(row: dict[str, Any]) -> float | None:
    if row.get("standardized_mean_difference") is not None:
        return float(row["standardized_mean_difference"])
    if row.get("categorical_lift_if_categorical") is not None:
        return float(row["categorical_lift_if_categorical"])
    return None


def load_source_artifacts(
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
) -> dict[str, Any]:
    required = ["_ARTIFACT_COMPLETE.json", "research_decision.json", "threshold_leaderboard.json"]
    missing = [name for name in required if not (threshold_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"threshold source missing required artifacts: {missing} at {threshold_dir}")
    threshold_json = {name: _load_json(threshold_dir / name) for name in required}
    if threshold_json["_ARTIFACT_COMPLETE.json"].get("complete") is not True:
        raise RuntimeError("threshold source artifact is not complete")
    if threshold_json["_ARTIFACT_COMPLETE.json"].get("silent_fallback_used") is not False or threshold_json["research_decision.json"].get("silent_fallback_used") is not False:
        raise RuntimeError("threshold source artifact used silent fallback")
    if threshold_json["_ARTIFACT_COMPLETE.json"].get("research_fallback_used") is not False or threshold_json["research_decision.json"].get("research_fallback_used") is not False:
        raise RuntimeError("threshold source artifact used research fallback")
    if threshold_json["research_decision.json"].get("authoritative_research_decision") != "threshold_no_trade_control_drop":
        raise RuntimeError("threshold source decision is not threshold_no_trade_control_drop")
    loaded = threshold_mod.load_source_artifacts(pattern_dir, guard_dir, wide_dir, risk_dir)
    events = threshold_mod.add_threshold_inputs(loaded["events"])
    selected, scored, calibration_rows = threshold_mod.build_threshold_selection(events)
    return {
        "events": scored,
        "selected": selected,
        "calibration_rows": calibration_rows,
        "risk_json": loaded["risk_json"],
        "threshold_json": threshold_json,
    }


def add_diagnostic_labels(events: pd.DataFrame, selected: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    frame["event_date"] = frame["event_date"].astype(str).str.slice(0, 10)
    if "event_ymd" not in frame.columns:
        frame["event_ymd"] = frame["event_date"].astype(str).str.replace("-", "", regex=False).astype(int)
    selected_keys = selected[["event_date", "code", "threshold_family_id", "selection_rank"]].copy()
    selected_keys["event_date"] = selected_keys["event_date"].astype(str).str.slice(0, 10)
    selected_keys["selected_rank_min"] = pd.to_numeric(selected_keys["selection_rank"], errors="coerce")
    grouped = selected_keys.groupby(["event_date", "code"], dropna=False)
    selected_summary = grouped.agg(
        selected_policy_ids=("threshold_family_id", lambda values: sorted(set(str(value) for value in values))),
        selected_rank_min=("selected_rank_min", "min"),
    ).reset_index()
    frame = frame.merge(selected_summary, on=["event_date", "code"], how="left")
    frame["selected_policy_ids"] = frame["selected_policy_ids"].apply(lambda value: value if isinstance(value, list) else [])
    frame["selected_by_prior_policy"] = frame["selected_policy_ids"].map(bool)
    frame["future_winner"] = (
        frame["is_future_top10_by_ret20"].astype(bool)
        | pd.to_numeric(frame["ret20_fwd"], errors="coerce").ge(0.10)
        | pd.to_numeric(frame["mfe20"], errors="coerce").ge(0.15)
    )
    frame["weak_relative_return"] = pd.to_numeric(frame["ret20_rank_pct_by_date"], errors="coerce").gt(0.50)
    frame["selected_nonwinner"] = frame["selected_by_prior_policy"] & ~frame["future_winner"] & (
        pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0) | frame["weak_relative_return"]
    )
    frame["selected_severe_loser"] = frame["selected_by_prior_policy"] & frame["severe_loss20"].astype(bool)
    frame["negative_guard_continuation_winner"] = frame["negative_guard_match"].astype(bool) & frame["future_winner"]
    frame["negative_guard_blowoff_loser"] = frame["negative_guard_match"].astype(bool) & (
        frame["severe_loss20"].astype(bool) | pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0)
    )
    future_winner_by_date = frame.groupby("event_date")["future_winner"].transform("any")
    frame["same_day_oracle_miss"] = frame["selected_nonwinner"] & future_winner_by_date
    rank = pd.to_numeric(frame[BASE_SCORE_COLUMN], errors="coerce").rank(method="first", pct=True)
    frame["research_score_bucket"] = (rank.mul(10).apply(math.ceil).clip(lower=1, upper=10)).astype(int)
    frame["time_block"] = frame["event_date"].astype(str).str.slice(0, 4)
    return frame


def build_group_definition_contract(events: pd.DataFrame) -> dict[str, Any]:
    groups = {
        "future_winner": "is_future_top10_by_ret20 or ret20 >= +10% or MFE20 >= +15%",
        "selected_nonwinner": "selected by prior wide/risk/threshold policy, not future_winner, and ret20 <= 0 or weak same-date relative return",
        "selected_severe_loser": "selected by prior policy and severe_loss20 = true",
        "negative_guard_continuation_winner": "negative_guard_match = true and future_winner = true",
        "negative_guard_blowoff_loser": "negative_guard_match = true and severe_loss20 = true or ret20 <= 0",
        "same_day_oracle_miss": "same-date future winner existed while selected candidate was nonwinner",
    }
    counts = {name: int(events[name].sum()) for name in groups}
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_group_definition_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "groups": groups,
        "group_counts": counts,
        "future_labels_used_for_group_definition_only": True,
        "future_labels_used_in_feature_inputs": False,
        "future_labels_used_in_score_inputs": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_feature_availability_audit(events: pd.DataFrame, numeric_features: list[str], categorical_features: list[str], tag_features: list[str]) -> dict[str, Any]:
    group_masks = {
        "future_winner": events["future_winner"],
        "selected_nonwinner": events["selected_nonwinner"],
        "selected_severe_loser": events["selected_severe_loser"],
        "negative_guard_continuation_winner": events["negative_guard_continuation_winner"],
        "negative_guard_blowoff_loser": events["negative_guard_blowoff_loser"],
    }
    rows = []
    for feature in [*numeric_features, *categorical_features, *tag_features]:
        present = _present_mask(events[feature])
        group_rates = {}
        for group_name, mask in group_masks.items():
            group_rates[group_name] = {
                "feature_present_rate": float(present[mask].mean()) if int(mask.sum()) else None,
                "feature_missing_rate": float((~present[mask]).mean()) if int(mask.sum()) else None,
                "group_count": int(mask.sum()),
            }
        rows.append(
            {
                "feature_id": feature,
                "feature_type": "numeric" if feature in numeric_features else ("tag" if feature in tag_features else "categorical"),
                "feature_present_rate": float(present.mean()) if len(present) else 0.0,
                "feature_missing_rate_by_group": group_rates,
                "source": "existing_artifact_field",
            }
        )
    usable = [row for row in rows if row["feature_present_rate"] >= 0.65]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_rows": rows,
        "feature_present_rate": float(sum(row["feature_present_rate"] for row in rows) / len(rows)) if rows else 0.0,
        "usable_feature_count": len(usable),
        "dropped_feature_count": len(rows) - len(usable),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_leakage_audit(feature_columns: list[str]) -> dict[str, Any]:
    leaked = sorted(set(feature_columns).intersection(FUTURE_LABEL_COLUMNS))
    return {
        "schema_version": f"{SCHEMA_PREFIX}_leakage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_input_columns": sorted(feature_columns),
        "future_label_columns": sorted(FUTURE_LABEL_COLUMNS),
        "leaked_feature_columns": leaked,
        "future_label_used_in_feature": bool(leaked),
        "future_label_used_in_score_input": False,
        "same_period_label_tuning": False,
        "future_labels_used_for_group_definition_only": True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_feature_contrast_report(events: pd.DataFrame, numeric_features: list[str], categorical_features: list[str], tag_features: list[str]) -> dict[str, Any]:
    comparisons = [
        ("future_winner", "selected_nonwinner", events["future_winner"], events["selected_nonwinner"]),
        ("future_winner", "selected_severe_loser", events["future_winner"], events["selected_severe_loser"]),
    ]
    rows = []
    for left_name, right_name, left_mask, right_mask in comparisons:
        rows.extend(
            _contrast(
                events,
                left_mask,
                right_mask,
                left_group=left_name,
                right_group=right_name,
                numeric_features=numeric_features,
                categorical_features=categorical_features,
                tag_features=tag_features,
            )
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_contrast_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "comparisons": ["future_winner vs selected_nonwinner", "future_winner vs selected_severe_loser"],
        "rows": rows,
    }


def build_same_date_contrast(events: pd.DataFrame, numeric_features: list[str], categorical_features: list[str], tag_features: list[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    feature_columns = [*numeric_features, *categorical_features, *tag_features]
    category_winner_rates: dict[str, dict[str, float]] = {}
    for feature in [*categorical_features, *tag_features]:
        rates = events.groupby(feature, dropna=False)["future_winner"].mean()
        category_winner_rates[feature] = {str(key): float(value) for key, value in rates.items()}
    ledger = []
    for event_date, group in events.groupby("event_date", sort=True):
        winners = group[group["future_winner"]].sort_values("ret20_fwd", ascending=False)
        misses = group[group["selected_nonwinner"]].sort_values("selected_rank_min", ascending=True)
        if winners.empty or misses.empty:
            continue
        winner = winners.iloc[0]
        for _, miss in misses.iterrows():
            numeric_deltas = {
                feature: (
                    None
                    if pd.isna(winner.get(feature)) or pd.isna(miss.get(feature))
                    else float(pd.to_numeric(pd.Series([winner.get(feature)]), errors="coerce").iloc[0] - pd.to_numeric(pd.Series([miss.get(feature)]), errors="coerce").iloc[0])
                )
                for feature in numeric_features
            }
            categorical_deltas = {
                feature: {
                    "winner_value": str(winner.get(feature)),
                    "nonwinner_value": str(miss.get(feature)),
                    "winner_category_rate_delta": category_winner_rates.get(feature, {}).get(str(winner.get(feature)), 0.0)
                    - category_winner_rates.get(feature, {}).get(str(miss.get(feature)), 0.0),
                }
                for feature in [*categorical_features, *tag_features]
            }
            ledger.append(
                {
                    "event_date": event_date,
                    "winner_code": str(winner["code"]),
                    "nonwinner_code": str(miss["code"]),
                    "winner_ret20": float(winner["ret20_fwd"]),
                    "nonwinner_ret20": float(miss["ret20_fwd"]),
                    "winner_feature_numeric_delta": numeric_deltas,
                    "winner_feature_categorical_delta": categorical_deltas,
                }
            )
    rows = []
    for feature in feature_columns:
        if feature in numeric_features:
            deltas = [row["winner_feature_numeric_delta"][feature] for row in ledger if row["winner_feature_numeric_delta"].get(feature) is not None]
        else:
            deltas = [
                row["winner_feature_categorical_delta"][feature]["winner_category_rate_delta"]
                for row in ledger
                if feature in row["winner_feature_categorical_delta"]
            ]
        positive = [value for value in deltas if value > 0]
        negative = [value for value in deltas if value < 0]
        rows.append(
            {
                "feature_id": feature,
                "feature_type": "numeric" if feature in numeric_features else ("tag" if feature in tag_features else "categorical"),
                "same_date_pair_count": int(len(deltas)),
                "winner_minus_nonwinner_feature_delta_avg": float(sum(deltas) / len(deltas)) if deltas else None,
                "winner_feature_dominance_rate": _safe_rate(len(positive), len(deltas)),
                "same_date_sign_stability": max(_safe_rate(len(positive), len(deltas)), _safe_rate(len(negative), len(deltas))) if deltas else 0.0,
            }
        )
    return (
        {
            "schema_version": f"{SCHEMA_PREFIX}_same_date_contrast_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "same_date_pair_count": len(ledger),
            "rows": rows,
        },
        ledger,
    )


def build_score_bucket_contrast(events: pd.DataFrame, numeric_features: list[str], categorical_features: list[str], tag_features: list[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    feature_columns = [*numeric_features, *categorical_features, *tag_features]
    ledger = []
    rows = []
    for feature in feature_columns:
        effects = []
        for bucket, group in events.groupby("research_score_bucket", sort=True):
            left = group[group["future_winner"]]
            right = group[group["selected_nonwinner"]]
            if len(left) < 2 or len(right) < 2:
                continue
            if feature in numeric_features or feature in tag_features:
                contrast = _numeric_contrast(left, right, feature)
            else:
                contrast = _categorical_contrast(left, right, feature, "categorical")
            effect = _feature_effect(contrast)
            if effect is None:
                continue
            effects.append(effect)
            ledger.append(
                {
                    "feature_id": feature,
                    "score_bucket": int(bucket),
                    "score_bucket_pair_count": int(min(len(left), len(right))),
                    "feature_effect_within_score_bucket": effect,
                    "left_count": int(len(left)),
                    "right_count": int(len(right)),
                }
            )
        positives = len([value for value in effects if value > 0])
        negatives = len([value for value in effects if value < 0])
        rows.append(
            {
                "feature_id": feature,
                "feature_type": "numeric" if feature in numeric_features else ("tag" if feature in tag_features else "categorical"),
                "score_bucket_pair_count": int(sum(row["score_bucket_pair_count"] for row in ledger if row["feature_id"] == feature)),
                "feature_effect_within_score_bucket": float(sum(effects) / len(effects)) if effects else None,
                "feature_effect_by_score_decile": [
                    row for row in ledger if row["feature_id"] == feature
                ],
                "score_bucket_sign_stability": max(_safe_rate(positives, len(effects)), _safe_rate(negatives, len(effects))) if effects else 0.0,
            }
        )
    return (
        {
            "schema_version": f"{SCHEMA_PREFIX}_score_bucket_contrast_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": rows,
        },
        ledger,
    )


def build_negative_guard_decomposition_report(events: pd.DataFrame, numeric_features: list[str], categorical_features: list[str], tag_features: list[str]) -> dict[str, Any]:
    rows = _contrast(
        events,
        events["negative_guard_continuation_winner"],
        events["negative_guard_blowoff_loser"],
        left_group="negative_guard_continuation_winner",
        right_group="negative_guard_blowoff_loser",
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        tag_features=[feature for feature in tag_features if feature != "negative_guard_match"],
    )
    candidates = []
    for row in rows:
        effect = _feature_effect(row)
        if effect is not None and abs(effect) >= 0.15:
            candidates.append(
                {
                    "feature_id": row["feature_id"],
                    "feature_type": row["feature_type"],
                    "effect_size": effect,
                    "intended_use": "decomposition_feature" if effect > 0 else "soft_penalty",
                }
            )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_decomposition_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "negative_guard_winner_count": int(events["negative_guard_continuation_winner"].sum()),
        "negative_guard_severe_loser_count": int(events["negative_guard_blowoff_loser"].sum()),
        "feature_delta_negative_guard_winner_vs_loser": rows,
        "continuation_winner_indicator_candidates": [row for row in candidates if row["effect_size"] > 0],
        "blowoff_loser_indicator_candidates": [row for row in candidates if row["effect_size"] < 0],
        "decomposition_available": bool(candidates),
    }


def build_time_block_stability(events: pd.DataFrame, numeric_features: list[str], categorical_features: list[str], tag_features: list[str]) -> dict[str, Any]:
    rows = []
    for feature in [*numeric_features, *categorical_features, *tag_features]:
        effects = []
        for block, group in events.groupby("time_block", sort=True):
            if len(group[group["future_winner"]]) < 2 or len(group[group["selected_nonwinner"]]) < 2:
                continue
            if feature in numeric_features or feature in tag_features:
                contrast = _numeric_contrast(group[group["future_winner"]], group[group["selected_nonwinner"]], feature)
            else:
                contrast = _categorical_contrast(group[group["future_winner"]], group[group["selected_nonwinner"]], feature, "categorical")
            effect = _feature_effect(contrast)
            if effect is None:
                continue
            effects.append({"time_block": str(block), "effect_size": effect})
        values = [row["effect_size"] for row in effects]
        positives = len([value for value in values if value > 0])
        negatives = len([value for value in values if value < 0])
        stability = max(_safe_rate(positives, len(values)), _safe_rate(negatives, len(values))) if values else 0.0
        rows.append(
            {
                "feature_id": feature,
                "feature_type": "numeric" if feature in numeric_features else ("tag" if feature in tag_features else "categorical"),
                "time_block_effects": effects,
                "time_block_sign_stability": stability,
                "time_block_effect_size_min": min(values) if values else None,
                "time_block_effect_size_max": max(values) if values else None,
                "feature_passes_stability_gate": bool(stability >= 0.60 and len(values) >= 2),
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }


def build_candidate_feature_shortlist(
    *,
    availability: dict[str, Any],
    feature_report: dict[str, Any],
    same_date_report: dict[str, Any],
    score_bucket_report: dict[str, Any],
    negative_guard_report: dict[str, Any],
    time_stability: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    availability_by_feature = {row["feature_id"]: row for row in availability["feature_rows"]}
    global_rows = {
        row["feature_id"]: row
        for row in feature_report["rows"]
        if row["left_group"] == "future_winner" and row["right_group"] == "selected_nonwinner"
    }
    same_date_by_feature = {row["feature_id"]: row for row in same_date_report["rows"]}
    score_bucket_by_feature = {row["feature_id"]: row for row in score_bucket_report["rows"]}
    time_by_feature = {row["feature_id"]: row for row in time_stability["rows"]}
    negative_features = {row["feature_id"] for row in negative_guard_report["continuation_winner_indicator_candidates"] + negative_guard_report["blowoff_loser_indicator_candidates"]}
    shortlist = []
    rejected = []
    for feature, global_row in sorted(global_rows.items()):
        effect = _feature_effect(global_row) or 0.0
        availability_rate = float(availability_by_feature.get(feature, {}).get("feature_present_rate", 0.0))
        same_row = same_date_by_feature.get(feature, {})
        bucket_row = score_bucket_by_feature.get(feature, {})
        time_row = time_by_feature.get(feature, {})
        same_stability = float(same_row.get("same_date_sign_stability") or 0.0)
        bucket_stability = float(bucket_row.get("score_bucket_sign_stability") or 0.0)
        time_sign_stability = float(time_row.get("time_block_sign_stability") or 0.0)
        passes = {
            "coverage": availability_rate >= 0.65,
            "global_effect": abs(effect) >= 0.15,
            "same_date_stability": same_stability >= 0.55,
            "score_bucket_added_info": bucket_stability >= 0.50 and bucket_row.get("feature_effect_within_score_bucket") is not None,
            "time_block_stability": bool(time_row.get("feature_passes_stability_gate")),
            "leakage_safe": feature not in FUTURE_LABEL_COLUMNS,
        }
        payload = {
            "feature_id": feature,
            "source": "existing_artifact_field",
            "feature_type": global_row["feature_type"],
            "intended_use": "decomposition_feature" if feature in negative_features else ("soft_boost" if effect > 0 else "soft_penalty"),
            "target_problem": "negative_guard_decomposition" if feature in negative_features else ("winner_capture" if effect > 0 else "nonwinner_reduction"),
            "coverage_rate": availability_rate,
            "effect_size": effect,
            "same_date_stability": same_stability,
            "score_bucket_sign_stability": bucket_stability,
            "time_block_sign_stability": time_sign_stability,
            "leakage_safe": passes["leakage_safe"],
            "recommended_for_next_scorer": all(passes.values()),
            "gate_status": passes,
        }
        if payload["recommended_for_next_scorer"]:
            shortlist.append(payload)
        else:
            rejected.append({**payload, "reject_reasons": [key for key, value in passes.items() if not value]})
    return (
        {
            "schema_version": f"{SCHEMA_PREFIX}_candidate_feature_shortlist_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "candidate_scoring_created": False,
            "candidate_feature_shortlist_created": True,
            "rows": shortlist,
        },
        {
            "schema_version": f"{SCHEMA_PREFIX}_rejected_feature_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": rejected,
        },
    )


def build_research_decision(
    *,
    shortlist: dict[str, Any],
    rejected: dict[str, Any],
    negative_guard_report: dict[str, Any],
    leakage_audit: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    recommended = shortlist["rows"]
    negative_candidates = [row for row in recommended if row["target_problem"] == "negative_guard_decomposition"]
    no_leakage = leakage_audit["future_label_used_in_feature"] is False and leakage_audit["future_label_used_in_score_input"] is False
    keep_pass = (
        artifact_complete
        and no_leakage
        and len(recommended) >= 3
        and len(negative_candidates) >= 1
        and negative_guard_report["decomposition_available"] is True
    )
    hold_pass = artifact_complete and no_leakage and (len(recommended) > 0 or negative_guard_report["decomposition_available"])
    if keep_pass:
        decision = "keep_candidate"
        authoritative = "winner_nonwinner_feature_diagnosis_promising"
    elif hold_pass:
        decision = "hold"
        authoritative = "winner_nonwinner_feature_diagnosis_hold"
    else:
        decision = "drop"
        authoritative = "winner_nonwinner_feature_diagnosis_failed"
    typed_reasons = [
        "feature_diagnosis_created",
        "candidate_feature_shortlist_created",
        "candidate_scoring_not_created",
        "threshold_policy_not_created",
        "no_future_label_leakage" if no_leakage else "future_label_leakage_detected",
        "negative_guard_decomposition_available" if negative_guard_report["decomposition_available"] else "negative_guard_decomposition_not_available",
        f"recommended_feature_count_{len(recommended)}",
        f"rejected_feature_count_{len(rejected['rows'])}",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "wide_pool_winner_nonwinner_feature_diagnosis",
        "boundary": "TRADEX-only",
        "axis_moved": "wide_pool_winner_nonwinner_feature_diagnosis",
        "source_threshold_decision": "threshold_no_trade_control_drop",
        "feature_diagnosis_created": True,
        "candidate_feature_shortlist_created": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "future_labels_used_for_group_definition_only": True,
        "future_labels_used_in_feature_inputs": False,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": typed_reasons,
        "recommended_feature_count": len(recommended),
        "negative_guard_recommended_feature_count": len(negative_candidates),
        "artifact_complete": artifact_complete,
    }


def build_next_axis_recommendation(decision: dict[str, Any]) -> dict[str, Any]:
    if decision["authoritative_research_decision"] == "winner_nonwinner_feature_diagnosis_promising":
        next_axis = "feature_based_wide_pool_rerank_v2"
    elif decision["authoritative_research_decision"] == "winner_nonwinner_feature_diagnosis_hold":
        next_axis = "missing_feature_extraction_runner_or_additional_audit"
    else:
        next_axis = "image_assisted_rerank_phase0_1"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "recommended_next_axis": next_axis,
        "one_recommended_next_axis_only": True,
    }


def build_source_artifact_refs(pattern_dir: Path, guard_dir: Path, upside_dir: Path, wide_dir: Path, risk_dir: Path, threshold_dir: Path) -> dict[str, Any]:
    refs = []
    for source, root, names in [
        ("pattern", pattern_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "pre_strength_event_ledger.jsonl", "research_decision.json"]),
        ("guard", guard_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "research_decision.json"]),
        ("upside", upside_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json"]),
        ("wide", wide_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "score_leaderboard.json"]),
        ("risk", risk_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "risk_leaderboard.json"]),
        ("threshold", threshold_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "threshold_leaderboard.json"]),
    ]:
        for name in names:
            path = root / name
            item: dict[str, Any] = {"source": source, "name": name, "path": str(path), "exists": path.exists()}
            if path.exists() and path.suffix == ".json":
                item["content_hash"] = _stable_hash(_load_json(path))
            refs.append(item)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "pattern_artifact_root": str(pattern_dir),
        "guard_artifact_root": str(guard_dir),
        "upside_artifact_root": str(upside_dir),
        "wide_artifact_root": str(wide_dir),
        "risk_artifact_root": str(risk_dir),
        "threshold_artifact_root": str(threshold_dir),
        "refs": refs,
    }


def build_evaluation_contract(events: pd.DataFrame, feature_columns: list[str], dirs: dict[str, Path]) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "wide_pool_winner_nonwinner_feature_diagnosis",
        "boundary": "TRADEX-only",
        "axis_moved": "wide_pool_winner_nonwinner_feature_diagnosis",
        "source_threshold_decision": "threshold_no_trade_control_drop",
        "artifact_roots": {key: str(value) for key, value in dirs.items()},
        "period": {"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max()))},
        "event_count": int(len(events)),
        "top_k": TOP_K,
        "feature_input_columns": sorted(feature_columns),
        "future_label_policy": {
            "future_labels_used_for_group_definition_only": True,
            "future_labels_used_in_feature_inputs": False,
            "future_labels_used_in_score_inputs": False,
            "candidate_scoring_created": False,
            "threshold_policy_created": False,
        },
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": "wide_pool_pre_strength_event_universe",
            "same_cost_slippage": "ignored_by_user_intent",
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
        excluded.add("next_axis_recommendation.json")
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required.values()),
        "required_artifacts": required,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "feature_diagnosis_created": True,
        "candidate_feature_shortlist_created": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def run_wide_pool_winner_nonwinner_feature_diagnosis_v1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_risk_run_id: str = DEFAULT_RISK_RUN_ID,
    source_threshold_run_id: str = DEFAULT_THRESHOLD_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    risk_root: str | Path = DEFAULT_RISK_ROOT,
    threshold_root: str | Path = DEFAULT_THRESHOLD_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    guard_dir = _run_dir(guard_root, source_guard_run_id, DEFAULT_GUARD_ROOT)
    upside_dir = _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    risk_dir = _run_dir(risk_root, source_risk_run_id, DEFAULT_RISK_ROOT)
    threshold_dir = _run_dir(threshold_root, source_threshold_run_id, DEFAULT_THRESHOLD_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    loaded = load_source_artifacts(pattern_dir, guard_dir, upside_dir, wide_dir, risk_dir, threshold_dir)
    events = add_diagnostic_labels(loaded["events"], loaded["selected"])
    numeric_features, categorical_features, tag_features = _available_feature_columns(events)
    feature_columns = [*numeric_features, *categorical_features, *tag_features]
    dirs = {
        "pattern": pattern_dir,
        "guard": guard_dir,
        "upside": upside_dir,
        "wide": wide_dir,
        "risk": risk_dir,
        "threshold": threshold_dir,
    }
    evaluation_contract = build_evaluation_contract(events, feature_columns, dirs)
    group_contract = build_group_definition_contract(events)
    source_refs = build_source_artifact_refs(pattern_dir, guard_dir, upside_dir, wide_dir, risk_dir, threshold_dir)
    availability = build_feature_availability_audit(events, numeric_features, categorical_features, tag_features)
    leakage = build_leakage_audit(feature_columns)
    feature_report = build_feature_contrast_report(events, numeric_features, categorical_features, tag_features)
    same_date_report, same_date_ledger = build_same_date_contrast(events, numeric_features, categorical_features, tag_features)
    score_bucket_report, score_bucket_ledger = build_score_bucket_contrast(events, numeric_features, categorical_features, tag_features)
    negative_guard_report = build_negative_guard_decomposition_report(events, numeric_features, categorical_features, tag_features)
    time_stability = build_time_block_stability(events, numeric_features, categorical_features, tag_features)
    shortlist, rejected = build_candidate_feature_shortlist(
        availability=availability,
        feature_report=feature_report,
        same_date_report=same_date_report,
        score_bucket_report=score_bucket_report,
        negative_guard_report=negative_guard_report,
        time_stability=time_stability,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=[
            {"name": "source_pattern_artifact_root", "path": str(pattern_dir)},
            {"name": "source_guard_artifact_root", "path": str(guard_dir)},
            {"name": "source_upside_artifact_root", "path": str(upside_dir)},
            {"name": "source_wide_artifact_root", "path": str(wide_dir)},
            {"name": "source_risk_artifact_root", "path": str(risk_dir)},
            {"name": "source_threshold_artifact_root", "path": str(threshold_dir)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={"axis_id": AXIS_ID, "top_k": TOP_K, "candidate_scoring_created": False, "threshold_policy_created": False},
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "wide_pool_winner_nonwinner_feature_diagnosis"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "group_definition_contract.json": group_contract,
        "feature_availability_audit.json": availability,
        "leakage_audit.json": leakage,
        "feature_contrast_report.json": feature_report,
        "same_date_contrast_report.json": same_date_report,
        "score_bucket_contrast_report.json": score_bucket_report,
        "negative_guard_decomposition_report.json": negative_guard_report,
        "time_block_stability.json": time_stability,
        "candidate_feature_shortlist.json": shortlist,
        "rejected_feature_report.json": rejected,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["same_date_pair_ledger.jsonl"] = str(_write_jsonl(output_dir / "same_date_pair_ledger.jsonl", same_date_ledger))
    paths["score_bucket_pair_ledger.jsonl"] = str(_write_jsonl(output_dir / "score_bucket_pair_ledger.jsonl", score_bucket_ledger))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        shortlist=shortlist,
        rejected=rejected,
        negative_guard_report=negative_guard_report,
        leakage_audit=leakage,
        artifact_complete=bool(pre_complete["complete"]),
    )
    next_axis = build_next_axis_recommendation(decision)
    paths["next_axis_recommendation.json"] = str(_write_json(output_dir / "next_axis_recommendation.json", next_axis))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "recommended_feature_count": decision["recommended_feature_count"],
        "negative_guard_recommended_feature_count": decision["negative_guard_recommended_feature_count"],
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-risk-run-id", default=DEFAULT_RISK_RUN_ID)
    parser.add_argument("--source-threshold-run-id", default=DEFAULT_THRESHOLD_RUN_ID)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--risk-root", default=str(DEFAULT_RISK_ROOT))
    parser.add_argument("--threshold-root", default=str(DEFAULT_THRESHOLD_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_wide_pool_winner_nonwinner_feature_diagnosis_v1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        source_upside_run_id=args.source_upside_run_id,
        source_wide_run_id=args.source_wide_run_id,
        source_risk_run_id=args.source_risk_run_id,
        source_threshold_run_id=args.source_threshold_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        upside_root=args.upside_root,
        wide_root=args.wide_root,
        risk_root=args.risk_root,
        threshold_root=args.threshold_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
