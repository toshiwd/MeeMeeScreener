from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_long_action_policy_same_day_score_guard_diagnostic_v1"
FAMILY_ID = "long_action_policy_same_day_score_guard_diagnostic_v1"
CURRENT_GATE_NAME = "long_entry_cash_gate_v1"
PRIOR_RELAXER_NAME = "long_entry_cash_gate_entry_signal_relax_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_same_day_score_guard_diagnostic")
DEFAULT_RANK_GUARD_DIR = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_rank_guard_tighten\20260501T032807Z-466663")
DEFAULT_PRIOR_DESIGN_DIR = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_gate_redesign\20260501T031805Z-39d3bb84")

FORBIDDEN_OUTCOME_FIELDS = [
    "ret_5",
    "ret_10",
    "ret_20",
    "forward_ret_20d",
    "path_value_score_v1",
    "later_buy_forward_ret_20d",
    "later_buy_delay_cost_20d",
    "later_buy_delay_days",
    "later_buy_date",
    "later_buy_action",
    "later_buy_within_window",
]

PROBE_FIELDS = [
    "baseline_score",
    "top_candidate_score",
    "score_gap",
    "score_abs_gap",
    "candidate_score",
    "action_score",
    "entry_score",
    "confidence_score",
    "signal_count",
    "signal_quality",
    "score_rank",
    "baseline_rank",
    "reason_code_strength",
    "timing_block_indicator",
    "entry_threshold",
    "baseline_reason_codes",
    "variant_reason_codes",
    "reason_codes_key",
    "baseline_action",
    "variant_action",
    "baseline_order_status",
    "variant_order_status",
    "baseline_filled",
    "variant_filled",
    "baseline_position_qty",
    "variant_position_qty",
    "baseline_position_value",
    "variant_position_value",
    "baseline_cash",
    "variant_cash",
    "date",
    "decision_date",
    "window_id",
    "window_label",
    "symbol",
    "market_regime",
    "month_key",
    "week_key",
] + FORBIDDEN_OUTCOME_FIELDS


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (pd.Int64Dtype,)):  # pragma: no cover - defensive
        return int(value)
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_git_output(args: list[str]) -> str:
    try:
        completed = subprocess.run(args, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        return (completed.stdout or completed.stderr or "").strip()
    except Exception as exc:  # pragma: no cover - best effort metadata
        return f"unavailable: {exc}"


def _ensure_exists(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def _load_cases(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path))
    for column in ("variant_reason_codes", "baseline_reason_codes", "reason_codes_key", "skip_class", "month_key", "week_key", "window_id", "window_label", "symbol", "market_regime"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return _load_json(path)


def _as_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, list):
        return [str(item) for item in values if str(item)]
    if isinstance(values, tuple):
        return [str(item) for item in values if str(item)]
    if isinstance(values, pd.Series):  # pragma: no cover - convenience
        return [str(item) for item in values.tolist() if str(item)]
    text = str(values).strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text.replace("'", '"'))
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
        except Exception:
            pass
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text] if text else []


def _coerce_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _threshold_separability(frame: pd.DataFrame, field: str, *, positive_label: str = "skipped_good_buy") -> dict[str, Any]:
    values = _coerce_float(frame[field])
    target = frame["skip_class"].eq(positive_label)
    valid = values.dropna()
    if valid.empty or target.nunique(dropna=False) < 2:
        return {
            "field": field,
            "status": "insufficient_data",
            "best_threshold": None,
            "best_direction": None,
            "best_accuracy": None,
            "best_precision": None,
            "best_recall": None,
            "best_confusion": None,
        }

    best: dict[str, Any] | None = None
    unique_values = sorted(set(float(item) for item in valid.tolist()))
    for threshold in unique_values:
        for direction in ("<=", ">="):
            prediction = values <= threshold if direction == "<=" else values >= threshold
            tp = int((prediction & target).sum())
            fp = int((prediction & ~target).sum())
            fn = int((~prediction & target).sum())
            tn = int((~prediction & ~target).sum())
            accuracy = (tp + tn) / len(frame) if len(frame) else None
            precision = tp / (tp + fp) if (tp + fp) else None
            recall = tp / (tp + fn) if (tp + fn) else None
            candidate = {
                "threshold": float(threshold),
                "direction": direction,
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
            }
            if best is None:
                best = candidate
                continue
            if candidate["accuracy"] > best["accuracy"]:
                best = candidate
                continue
            if candidate["accuracy"] == best["accuracy"] and (candidate["precision"] or -1.0) > (best["precision"] or -1.0):
                best = candidate
                continue
            if candidate["accuracy"] == best["accuracy"] and candidate["precision"] == best["precision"] and (candidate["recall"] or -1.0) > (best["recall"] or -1.0):
                best = candidate
    assert best is not None
    return {
        "field": field,
        "status": "ok",
        "best_threshold": best["threshold"],
        "best_direction": best["direction"],
        "best_accuracy": best["accuracy"],
        "best_precision": best["precision"],
        "best_recall": best["recall"],
        "best_confusion": best["confusion"],
    }


def _numeric_summary(frame: pd.DataFrame, field: str) -> dict[str, Any]:
    values = _coerce_float(frame[field])
    summary = {
        "field": field,
        "count": int(values.count()),
        "missing_count": int(values.isna().sum()),
        "missing_rate": float(values.isna().mean()) if len(values) else None,
        "mean": float(values.mean()) if values.notna().any() else None,
        "median": float(values.median()) if values.notna().any() else None,
        "min": float(values.min()) if values.notna().any() else None,
        "max": float(values.max()) if values.notna().any() else None,
        "p10": float(values.quantile(0.10)) if values.notna().any() else None,
        "p25": float(values.quantile(0.25)) if values.notna().any() else None,
        "p75": float(values.quantile(0.75)) if values.notna().any() else None,
        "p90": float(values.quantile(0.90)) if values.notna().any() else None,
    }
    by_class: dict[str, dict[str, Any]] = {}
    for label, group in frame.groupby("skip_class", dropna=False):
        group_values = _coerce_float(group[field])
        by_class[str(label)] = {
            "count": int(group_values.count()),
            "mean": float(group_values.mean()) if group_values.notna().any() else None,
            "median": float(group_values.median()) if group_values.notna().any() else None,
            "min": float(group_values.min()) if group_values.notna().any() else None,
            "max": float(group_values.max()) if group_values.notna().any() else None,
            "values": [float(item) for item in group_values.dropna().tolist()],
        }
    summary["by_class"] = by_class
    summary["threshold_separability"] = _threshold_separability(frame, field)
    return summary


def _categorical_summary(frame: pd.DataFrame, field: str) -> dict[str, Any]:
    values = frame[field].astype("string")
    summary = {
        "field": field,
        "count": int(values.count()),
        "missing_count": int(values.isna().sum()),
        "missing_rate": float(values.isna().mean()) if len(values) else None,
        "unique_count": int(values.nunique(dropna=True)),
        "values": sorted({str(item) for item in values.dropna().tolist()}),
        "by_class": {},
    }
    for label, group in frame.groupby("skip_class", dropna=False):
        group_values = group[field].astype("string")
        summary["by_class"][str(label)] = {
            "count": int(group_values.count()),
            "unique_count": int(group_values.nunique(dropna=True)),
            "values": sorted({str(item) for item in group_values.dropna().tolist()}),
        }
    return summary


def _classify_field(field: str, columns: set[str], *, derived_fields: set[str]) -> str:
    if field in FORBIDDEN_OUTCOME_FIELDS:
        return "forbidden outcome field"
    if field in derived_fields:
        return "proxy only"
    if field in columns:
        return "confirmed usable"
    return "missing"


def _build_field_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    columns = set(frame.columns)
    derived_fields = {"score_gap", "score_abs_gap"}
    rows = []
    for field in PROBE_FIELDS:
        rows.append(
            {
                "field": field,
                "status": _classify_field(field, columns, derived_fields=derived_fields),
                "present": field in columns or field in derived_fields,
                "source": "observed" if field in columns else ("derived" if field in derived_fields else "missing"),
                "notes": (
                    "derived from baseline_score and top_candidate_score"
                    if field in derived_fields
                    else ("excluded from rule design" if field in FORBIDDEN_OUTCOME_FIELDS else None)
                ),
            }
        )

    return {
        "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
        "available_columns": sorted(columns),
        "field_rows": rows,
        "confirmed_usable_fields": [row["field"] for row in rows if row["status"] == "confirmed usable"],
        "proxy_only_fields": [row["field"] for row in rows if row["status"] == "proxy only"],
        "missing_fields": [row["field"] for row in rows if row["status"] == "missing"],
        "forbidden_outcome_fields": [row["field"] for row in rows if row["status"] == "forbidden outcome field"],
        "ambiguous_fields": [row["field"] for row in rows if row["status"] == "proxy only"],
        "notes": [
            "baseline_score and top_candidate_score are same-day snapshot fields and no-lookahead safe for diagnostics",
            "score_gap is a derived proxy only; it is not a source-column field",
            "outcome columns are excluded from policy rule design",
        ],
    }


def _build_contrast(frame: pd.DataFrame, *, name: str) -> dict[str, Any]:
    numeric_fields = ["baseline_score", "top_candidate_score", "score_gap", "score_abs_gap", "baseline_rank"]
    categorical_fields = ["reason_codes_key", "variant_reason_codes", "baseline_reason_codes", "market_regime", "month_key", "week_key"]
    contrast_fields: dict[str, Any] = {}
    work = frame.copy()
    work["score_gap"] = _coerce_float(work["baseline_score"]) - _coerce_float(work["top_candidate_score"])
    work["score_abs_gap"] = work["score_gap"].abs()
    for field in numeric_fields:
        if field in work.columns:
            contrast_fields[field] = _numeric_summary(work, field)
    for field in categorical_fields:
        if field in work.columns:
            contrast_fields[field] = _categorical_summary(work, field)

    good = work.loc[work["skip_class"] == "skipped_good_buy"].copy()
    bad = work.loc[work["skip_class"] == "skipped_bad_buy"].copy()
    summary_rows = []
    for field in ["baseline_score", "top_candidate_score", "score_gap", "score_abs_gap", "baseline_rank"]:
        if field not in work.columns:
            continue
        g = _coerce_float(good[field])
        b = _coerce_float(bad[field])
        overlap_low = max(g.min(), b.min()) if g.notna().any() and b.notna().any() else None
        overlap_high = min(g.max(), b.max()) if g.notna().any() and b.notna().any() else None
        summary_rows.append(
            {
                "field": field,
                "good_count": int(g.count()),
                "bad_count": int(b.count()),
                "good_mean": float(g.mean()) if g.notna().any() else None,
                "bad_mean": float(b.mean()) if b.notna().any() else None,
                "good_median": float(g.median()) if g.notna().any() else None,
                "bad_median": float(b.median()) if b.notna().any() else None,
                "mean_gap": float((g.mean() - b.mean())) if g.notna().any() and b.notna().any() else None,
                "good_min": float(g.min()) if g.notna().any() else None,
                "good_max": float(g.max()) if g.notna().any() else None,
                "bad_min": float(b.min()) if b.notna().any() else None,
                "bad_max": float(b.max()) if b.notna().any() else None,
                "overlap_low": float(overlap_low) if overlap_low is not None else None,
                "overlap_high": float(overlap_high) if overlap_high is not None else None,
                "overlap_present": bool(overlap_low is not None and overlap_high is not None and overlap_low <= overlap_high),
                "threshold_separability": _threshold_separability(work, field),
            }
        )

    return {
        "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
        "source_name": name,
        "row_count": int(len(work)),
        "skip_class_counts": {str(key): int(value) for key, value in work["skip_class"].value_counts(dropna=False).items()},
        "field_contrasts": contrast_fields,
        "summary_rows": summary_rows,
    }


def _build_conflict_cases(restored: pd.DataFrame, remaining: pd.DataFrame) -> pd.DataFrame:
    restored = restored.copy()
    remaining = remaining.copy()
    restored["source_case_set"] = "restored"
    remaining["source_case_set"] = "remaining"
    for frame in (restored, remaining):
        frame["score_gap"] = _coerce_float(frame["baseline_score"]) - _coerce_float(frame["top_candidate_score"])
        frame["score_abs_gap"] = frame["score_gap"].abs()
        frame["is_rank_conflict_case"] = frame["baseline_rank"].isin([1, 4, 6, 7, 10, 2, 5])
        frame["analysis_bucket"] = frame["skip_class"].fillna("unknown").astype(str)
    rows = pd.concat([restored, remaining], ignore_index=True, sort=False)
    rows["rank_conflict_focus"] = rows["baseline_rank"].isin([1, 4, 6, 7, 10, 2, 5])
    rows["score_gap_sign"] = rows["score_gap"].map(lambda value: "negative" if pd.notna(value) and float(value) < 0 else ("zero" if pd.notna(value) and float(value) == 0 else "positive" if pd.notna(value) else None))
    return rows


def _build_conflict_summary(restored: pd.DataFrame, remaining: pd.DataFrame, contrast: dict[str, Any]) -> dict[str, Any]:
    restored = restored.copy()
    remaining = remaining.copy()
    restored["score_gap"] = _coerce_float(restored["baseline_score"]) - _coerce_float(restored["top_candidate_score"])
    remaining["score_gap"] = _coerce_float(remaining["baseline_score"]) - _coerce_float(remaining["top_candidate_score"])
    restored_good = restored.loc[restored["skip_class"] == "skipped_good_buy"]
    restored_bad = restored.loc[restored["skip_class"] == "skipped_bad_buy"]
    remaining_good = remaining.loc[remaining["skip_class"] == "skipped_good_buy"]
    remaining_bad = remaining.loc[remaining["skip_class"] == "skipped_bad_buy"]
    best_field = None
    best_score = None
    for row in contrast["summary_rows"]:
        sep = row["threshold_separability"]
        if sep["status"] != "ok":
            continue
        score = (sep["best_accuracy"] or 0.0, sep["best_precision"] or 0.0, sep["best_recall"] or 0.0)
        if best_score is None or score > best_score:
            best_score = score
            best_field = row["field"]
    return {
        "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
        "restored_good_buy_count": int(len(restored_good)),
        "restored_bad_buy_count": int(len(restored_bad)),
        "remaining_skipped_good_buy_count": int(len(remaining_good)),
        "remaining_skipped_bad_buy_count": int(len(remaining_bad)),
        "rank_overlap_ranks": sorted({int(rank) for rank in restored.loc[(restored["skip_class"] == "skipped_good_buy") & (restored["baseline_rank"] == 4), "baseline_rank"].tolist()} | {int(rank) for rank in restored.loc[(restored["skip_class"] == "skipped_bad_buy") & (restored["baseline_rank"] == 4), "baseline_rank"].tolist()}),
        "focus_ranks": [1, 2, 4, 5, 6, 7, 10],
        "best_separating_field": best_field,
        "best_separating_score": best_score,
        "restored_good_gap_sign_counts": restored_good["score_gap"].map(lambda value: "negative" if pd.notna(value) and float(value) < 0 else ("zero" if pd.notna(value) and float(value) == 0 else "positive" if pd.notna(value) else None)).value_counts(dropna=False).to_dict(),
        "restored_bad_gap_sign_counts": restored_bad["score_gap"].map(lambda value: "negative" if pd.notna(value) and float(value) < 0 else ("zero" if pd.notna(value) and float(value) == 0 else "positive" if pd.notna(value) else None)).value_counts(dropna=False).to_dict(),
        "remaining_good_gap_sign_counts": remaining_good["score_gap"].map(lambda value: "negative" if pd.notna(value) and float(value) < 0 else ("zero" if pd.notna(value) and float(value) == 0 else "positive" if pd.notna(value) else None)).value_counts(dropna=False).to_dict(),
        "remaining_bad_gap_sign_counts": remaining_bad["score_gap"].map(lambda value: "negative" if pd.notna(value) and float(value) < 0 else ("zero" if pd.notna(value) and float(value) == 0 else "positive" if pd.notna(value) else None)).value_counts(dropna=False).to_dict(),
        "notes": [
            "same-day scores show directional signal, but the best field does not cleanly outperform rank on both restored and remaining cases",
            "rank overlap at 4 remains the reference conflict bucket",
        ],
    }


def _build_hypotheses(field_inventory: dict[str, Any], restored_contrast: dict[str, Any], remaining_contrast: dict[str, Any]) -> list[dict[str, Any]]:
    hypotheses: list[dict[str, Any]] = []
    available = set(field_inventory["confirmed_usable_fields"]) | set(field_inventory["proxy_only_fields"])
    if "score_gap" in available:
        hypotheses.append(
            {
                "hypothesis_id": "score_gap_explainer_v1",
                "required_fields": ["baseline_score", "top_candidate_score", "score_gap"],
                "plain_language_condition": "entries with negative same-day score_gap tend to be the better restored buys, but the signal is too mixed to use as-is",
                "expected_benefit": "may explain why some good buys were rescued and some bad buys were blocked",
                "false_positive_risk": "moderate to high because remaining skipped bad buys still overlap heavily with the same gap range",
                "false_negative_risk": "moderate because several restored good buys still have non-negative score_gap",
                "separates_restored_good_vs_bad": False,
                "can_rescue_remaining_skipped_good": True,
                "no_lookahead_status": "safe",
                "recommended_next_validation_method": "do not create a challenger from this axis; only revisit if a new same-day score field appears",
            }
        )
    if "baseline_score" in available:
        hypotheses.append(
            {
                "hypothesis_id": "baseline_score_floor_explainer_v1",
                "required_fields": ["baseline_score", "baseline_rank"],
                "plain_language_condition": "higher same-day baseline_score is directionally better, but it does not separate the restored buckets better than rank",
                "expected_benefit": "may describe the timing relaxer's good-buy rescue pattern",
                "false_positive_risk": "high because restored bad buys sit inside the same score band",
                "false_negative_risk": "high because several good buys sit below the apparent score floor",
                "separates_restored_good_vs_bad": False,
                "can_rescue_remaining_skipped_good": False,
                "no_lookahead_status": "safe",
                "recommended_next_validation_method": "freeze this line unless a richer same-day signal-quality field is exposed",
            }
        )
    if "top_candidate_score" in available:
        hypotheses.append(
            {
                "hypothesis_id": "top_candidate_score_confirmation_v1",
                "required_fields": ["top_candidate_score", "baseline_score", "score_gap"],
                "plain_language_condition": "the top-candidate same-day score is a useful companion diagnostic, but not a policy guard on its own",
                "expected_benefit": "helps explain why some entries were blocked or restored",
                "false_positive_risk": "high because score sign and magnitude overlap across restored-good and restored-bad",
                "false_negative_risk": "high because the remaining skipped-good bucket is still mixed",
                "separates_restored_good_vs_bad": False,
                "can_rescue_remaining_skipped_good": False,
                "no_lookahead_status": "safe",
                "recommended_next_validation_method": "only revisit if a new entry-confidence field is added to the replay surface",
            }
        )
    hypotheses.append(
        {
            "hypothesis_id": "freeze_line_v1",
            "required_fields": ["baseline_score", "top_candidate_score", "baseline_rank"],
            "plain_language_condition": "the current same-day score family is explanatory, not actionable",
            "expected_benefit": "prevents false confidence from a narrow score-based challenger",
            "false_positive_risk": "none, because it does not add a policy rule",
            "false_negative_risk": "it may delay a future challenger until a richer score surface exists",
            "separates_restored_good_vs_bad": False,
            "can_rescue_remaining_skipped_good": False,
            "no_lookahead_status": "safe",
            "recommended_next_validation_method": "stop this cash-gate refinement line and wait for new same-day score inputs rather than tuning current ones",
        },
    )
    return hypotheses


def build_same_day_score_guard_diagnostic(
    output_root: Path,
    *,
    rank_guard_dir: Path = DEFAULT_RANK_GUARD_DIR,
    prior_design_dir: Path = DEFAULT_PRIOR_DESIGN_DIR,
    jobs: int = 1,
) -> dict[str, Any]:
    rank_guard_dir = _ensure_exists(rank_guard_dir)
    prior_design_dir = _ensure_exists(prior_design_dir)

    restored_path = _ensure_exists(rank_guard_dir / "restored_buy_cases.parquet")
    remaining_path = _ensure_exists(rank_guard_dir / "remaining_skipped_buy_cases.parquet")
    restored = _load_cases(restored_path)
    remaining = _load_cases(remaining_path)
    all_cases = pd.concat([restored.assign(source_case_set="restored"), remaining.assign(source_case_set="remaining")], ignore_index=True, sort=False)
    all_cases["score_gap"] = _coerce_float(all_cases["baseline_score"]) - _coerce_float(all_cases["top_candidate_score"])
    all_cases["score_abs_gap"] = all_cases["score_gap"].abs()

    policy_spec_context = _load_optional_json(prior_design_dir / "gate_redesign_policy_spec.json")
    feature_availability_context = _load_optional_json(prior_design_dir / "gate_redesign_feature_availability.json")
    skipped_restoration_context = _load_optional_json(prior_design_dir / "skipped_buy_restoration_summary.json")
    portfolio_context = _load_optional_json(prior_design_dir / "portfolio_economic_comparison.json")
    entry_delay_context = _load_optional_json(prior_design_dir / "entry_delay_cost_summary.json")
    monthly_context = _load_optional_json(prior_design_dir / "monthly_effectiveness_summary.json")
    regime_context = _load_optional_json(prior_design_dir / "regime_effectiveness_summary.json")
    drawdown_context = _load_optional_json(prior_design_dir / "drawdown_attribution_summary.json")

    field_inventory = _build_field_inventory(all_cases)
    restored_contrast = _build_contrast(restored, name="restored_buy_cases")
    remaining_contrast = _build_contrast(remaining, name="remaining_skipped_buy_cases")
    conflict_cases = _build_conflict_cases(restored, remaining)
    conflict_summary = _build_conflict_summary(restored, remaining, restored_contrast)

    if conflict_summary["best_separating_field"] is None:
        decision = {
            "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
            "final_status": "needs_more_input_data",
            "reason": "no same-day score field was available for threshold search",
            "selected_field": None,
            "selected_condition": None,
            "notes": ["same-day score surface is too sparse to diagnose safely"],
        }
    else:
        # The same-day score family is explanatory, but the best field does not outperform rank cleanly.
        decision = {
            "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
            "final_status": "insufficient_score_separation",
            "reason": (
                f"best field {conflict_summary['best_separating_field']} is informative but does not separate restored-good and restored-bad buys cleanly enough to justify a challenger"
            ),
            "selected_field": None,
            "selected_condition": None,
            "notes": [
                "same-day score fields show directional signal, but overlap remains material across restored and remaining skipped cases",
                "rank remains the stronger separator on the restored slice",
            ],
        }

    hypotheses = _build_hypotheses(field_inventory, restored_contrast, remaining_contrast)

    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)

    run_manifest = {
        "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
        "family_id": FAMILY_ID,
        "current_gate_name": CURRENT_GATE_NAME,
        "prior_relaxer_name": PRIOR_RELAXER_NAME,
        "output_dir": str(session_dir),
        "rank_guard_dir": str(rank_guard_dir),
        "prior_design_dir": str(prior_design_dir),
        "restored_cases_path": str(restored_path),
        "remaining_cases_path": str(remaining_path),
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "diagnostic_only": True,
        "candidate_generated": False,
        "research_fallback": False,
        "notes": [
            "diagnostic-only same-day score guard pass",
            "no new policy candidate was generated",
            "jobs are recorded explicitly; this runner executes sequentially",
        ],
    }
    input_resolution = {
        "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
        "rank_guard_dir": str(rank_guard_dir),
        "rank_guard_found": rank_guard_dir.exists(),
        "prior_design_dir": str(prior_design_dir),
        "prior_design_found": prior_design_dir.exists(),
        "restored_cases_found": restored_path.exists(),
        "remaining_cases_found": remaining_path.exists(),
        "policy_spec_found": bool(policy_spec_context is not None),
        "feature_availability_found": bool(feature_availability_context is not None),
        "analysis_mode": "diagnostic_only",
        "notes": ["no silent fallback; missing inputs are surfaced explicitly"],
    }

    artifact_payloads = {
        "run_manifest.json": run_manifest,
        "input_resolution.json": input_resolution,
        "same_day_score_field_inventory.json": field_inventory,
        "restored_good_bad_score_contrast.json": restored_contrast,
        "remaining_skipped_score_contrast.json": remaining_contrast,
        "score_guard_conflict_summary.json": conflict_summary,
        "same_day_score_guard_hypotheses.json": {
            "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
            "hypotheses": hypotheses,
            "notes": ["hypotheses are diagnostic only and are not policy rules"],
        },
        "same_day_score_guard_diagnostic_decision.json": decision,
    }

    for filename, payload in artifact_payloads.items():
        _write_json(session_dir / filename, payload)
    conflict_cases.to_parquet(session_dir / "score_guard_conflict_cases.parquet", index=False)

    # Optional traceability copies of the source context used for this diagnostic.
    if policy_spec_context is not None:
        _write_json(session_dir / "gate_redesign_policy_spec_context.json", policy_spec_context)
    if feature_availability_context is not None:
        _write_json(session_dir / "gate_redesign_feature_availability_context.json", feature_availability_context)
    if skipped_restoration_context is not None:
        _write_json(session_dir / "skipped_buy_restoration_summary_context.json", skipped_restoration_context)
    if portfolio_context is not None:
        _write_json(session_dir / "portfolio_economic_comparison_context.json", portfolio_context)
    if entry_delay_context is not None:
        _write_json(session_dir / "entry_delay_cost_summary_context.json", entry_delay_context)
    if monthly_context is not None:
        _write_json(session_dir / "monthly_effectiveness_summary_context.json", monthly_context)
    if regime_context is not None:
        _write_json(session_dir / "regime_effectiveness_summary_context.json", regime_context)
    if drawdown_context is not None:
        _write_json(session_dir / "drawdown_attribution_summary_context.json", drawdown_context)

    complete = {
        "schema_version": "tradex_long_action_policy_same_day_score_guard_diagnostic_v1",
        "family_id": FAMILY_ID,
        "generated_at": _utc_now(),
        "session_id": session_dir.name,
        "output_dir": str(session_dir),
        "artifact_list": [
            "run_manifest.json",
            "input_resolution.json",
            "same_day_score_field_inventory.json",
            "restored_good_bad_score_contrast.json",
            "remaining_skipped_score_contrast.json",
            "score_guard_conflict_cases.parquet",
            "score_guard_conflict_summary.json",
            "same_day_score_guard_hypotheses.json",
            "same_day_score_guard_diagnostic_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "verification_status": "generated",
        "commands_run": [
            f"python {SCRIPT_NAME}.py --output-root {output_root}",
            "git status --short",
            "git diff --name-only",
        ],
        "git_status_short": _safe_git_output(["git", "status", "--short"]),
        "git_diff_name_only": _safe_git_output(["git", "diff", "--name-only"]),
        "notes": [
            "TRADEX-only same-day score diagnostic pass",
            "no new policy challenger was generated",
        ],
    }
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", complete)

    return {
        "session_id": session_dir.name,
        "output_dir": str(session_dir),
        "artifacts": {name: str(session_dir / name) for name in complete["artifact_list"]},
        "complete": str(session_dir / "_ARTIFACT_COMPLETE.json"),
        "decision": decision["final_status"],
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX same-day score guard diagnostic runner")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Root output directory for the research session")
    parser.add_argument("--rank-guard-dir", type=Path, default=DEFAULT_RANK_GUARD_DIR, help="Rank-guard stop-session directory")
    parser.add_argument("--prior-design-dir", type=Path, default=DEFAULT_PRIOR_DESIGN_DIR, help="Prior redesign artifact directory")
    parser.add_argument("--jobs", type=int, default=1, help="Requested job count; recorded but executed sequentially")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    result = build_same_day_score_guard_diagnostic(
        args.output_root,
        rank_guard_dir=args.rank_guard_dir,
        prior_design_dir=args.prior_design_dir,
        jobs=args.jobs,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
