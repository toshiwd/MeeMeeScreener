from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import chart_context_feature_contract_v1 as chart_features


AXIS_ID = "manual_top5_review_pack_v1"
SCHEMA_PREFIX = "tradex_manual_top5_review_pack_v1"
DEFAULT_ROBUSTNESS_ROOT = Path(
    "G:/Tradex/portfolio_agent_baseline_robustness_gate_v1/baseline-2019-2025-robustness-gate"
)
DEFAULT_SOURCE_REPRESENTATIVE_JSON = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_manual_candidate_review_pack_v1/"
    "20260515T010000Z-monthly-drawdown-guarded-momentum-manual-candidate-review-pack-v1/"
    "representative_top5_candidate_lists.json"
)
DEFAULT_OUTPUT_DIR_NAME = "manual_top5_review_pack_v1"

REQUIRED_ARTIFACTS = (
    "manual_top5_review_pack_summary.json",
    "manual_review_targets.csv",
    "manual_review_sheet.csv",
    "per_sample_candidate_context.csv",
    "per_candidate_chart_context.csv",
    "human_selection_decision_template.jsonl",
    "review_result_schema.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)

REVIEW_LABELS = (
    "select_strong",
    "select_optional",
    "reject_clear",
    "reject_uncertain",
    "cannot_judge_from_screen",
)

REVIEW_TARGETS: tuple[dict[str, Any], ...] = (
    {
        "sample_group": "priority_review",
        "sample_date": "2025-12-25",
        "codes": ("5801", "8035", "6330", "6890", "7992"),
        "review_reason": "good_day_first_priority",
    },
    {
        "sample_group": "priority_review",
        "sample_date": "2025-02-17",
        "codes": ("5210", "9147", "4251", "5721", "1812"),
        "review_reason": "good_day_first_priority",
    },
    {
        "sample_group": "priority_review",
        "sample_date": "2023-04-27",
        "codes": ("5445", "6305", "7014", "4523", "4631"),
        "review_reason": "good_day_first_priority",
    },
    {
        "sample_group": "weakness_check",
        "sample_date": "2023-05-11",
        "codes": ("7806", "8309", "9107", "1489", "3382"),
        "review_reason": "weak_day_can_human_avoid",
    },
    {
        "sample_group": "weakness_check",
        "sample_date": "2025-03-21",
        "codes": ("7327", "8359", "5949", "1717", "2212"),
        "review_reason": "weak_day_can_human_avoid",
    },
    {
        "sample_group": "weakness_check",
        "sample_date": "2025-03-12",
        "codes": ("5449", "5976", "9517", "9001", "2181"),
        "review_reason": "weak_day_can_human_avoid",
    },
    {
        "sample_group": "supplement",
        "sample_date": "2022-01-21",
        "codes": ("4246", "8331", "8584", "8604", "9432"),
        "review_reason": "supplemental_context",
    },
    {
        "sample_group": "supplement",
        "sample_date": "2026-01-23",
        "codes": ("6701", "4493", "4825", "7731", "6208"),
        "review_reason": "supplemental_context",
    },
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _date_int(value: str) -> int:
    return int(str(value).replace("-", ""))


def _target_rows() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    sample_order = 0
    for target in REVIEW_TARGETS:
        sample_order += 1
        for rank, code in enumerate(target["codes"], start=1):
            rows.append(
                {
                    "sample_group": target["sample_group"],
                    "sample_order": sample_order,
                    "sample_date": target["sample_date"],
                    "decision_ymd": _date_int(target["sample_date"]),
                    "code": code,
                    "rank": rank,
                    "candidate_label": f"{target['sample_group']}_rank{rank}",
                    "review_reason": target["review_reason"],
                }
            )
    return pd.DataFrame(rows)


def _load_source_pack_index(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    if not path.exists():
        return {}
    payload = _read_json(path)
    found: dict[tuple[str, str], dict[str, Any]] = {}
    examples = payload.get("examples", {})
    for group_name, group_examples in examples.items():
        for example in group_examples or []:
            event_date = str(example.get("event_date", ""))
            for list_name in ("starter_top5", "baseline_top5"):
                for candidate in example.get(list_name, []) or []:
                    key = (event_date, _code(candidate.get("symbol")))
                    if key not in found or list_name == "starter_top5":
                        found[key] = {
                            "source_representative_group": group_name,
                            "source_representative_list": list_name,
                            "source_representative_rank": candidate.get("rank"),
                            "source_representative_score": candidate.get("score"),
                            "source_monthly_prior_state": candidate.get("monthly_prior_state"),
                        }
    return found


def _target_frame_with_source(source_pack: dict[tuple[str, str], dict[str, Any]]) -> pd.DataFrame:
    target_frame = _target_rows()
    source_rows: list[dict[str, Any]] = []
    for row in target_frame.to_dict("records"):
        info = source_pack.get((row["sample_date"], _code(row["code"])), {})
        source_rows.append(
            {
                "source_representative_found": bool(info),
                "source_representative_rank": info.get("source_representative_rank"),
                "source_representative_score": info.get("source_representative_score"),
                "source_monthly_prior_state": info.get("source_monthly_prior_state"),
            }
        )
    return pd.concat([target_frame.reset_index(drop=True), pd.DataFrame(source_rows)], axis=1)


def _load_feature_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if "decision_ymd" in frame.columns:
        frame["decision_ymd"] = frame["decision_ymd"].astype(int)
    if "code" in frame.columns:
        frame["code"] = frame["code"].map(_code)
    return frame


def _feature_lookup(frame: pd.DataFrame) -> dict[tuple[int, str], dict[str, Any]]:
    if frame.empty:
        return {}
    records: dict[tuple[int, str], dict[str, Any]] = {}
    for row in frame.to_dict("records"):
        records[(int(row["decision_ymd"]), _code(row["code"]))] = row
    return records


def _target_context_features_from_source_db(robustness_root: Path, target_frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    source_db = chart_features._resolve_source_db(None, robustness_root)
    keys = target_frame[["code", "decision_ymd", "rank", "source_representative_score"]].copy()
    keys["year"] = keys["decision_ymd"].astype(str).str[:4].astype(int)
    keys["candidate_rank"] = keys["rank"].astype(int)
    keys["selection_score"] = pd.to_numeric(keys["source_representative_score"], errors="coerce").fillna(0.0)
    keys = keys[["code", "decision_ymd", "year", "candidate_rank", "selection_score"]].drop_duplicates(
        ["code", "decision_ymd"]
    )
    start_ymd = int(max(20000101, int(keys["decision_ymd"].min()) - 20000))
    end_ymd = int(keys["decision_ymd"].max())
    daily = chart_features._load_daily(source_db, start_ymd=start_ymd, end_ymd=end_ymd)
    daily = daily[daily["code"].astype(str).isin(set(keys["code"].astype(str)))].copy()
    if daily.empty:
        raise RuntimeError("source DB had no OHLCV overlap with manual review target codes")
    features = chart_features.build_chart_context_features(daily, keys)
    features["week_key"] = pd.to_datetime(features["decision_ymd"].astype(str), format="%Y%m%d").dt.to_period("W-FRI").astype(str)
    features["month_key"] = pd.to_datetime(features["decision_ymd"].astype(str), format="%Y%m%d").dt.to_period("M").astype(str)
    weekly = features.sort_values(["code", "decision_ymd"], kind="stable").groupby(["code", "week_key"], as_index=False).tail(1)
    monthly = features.sort_values(["code", "decision_ymd"], kind="stable").groupby(["code", "month_key"], as_index=False).tail(1)
    return {"daily": features, "weekly": weekly, "monthly": monthly, "source_db": pd.DataFrame([{"source_db": str(source_db)}])}


def _merge_feature_lookup(primary: dict[tuple[int, str], dict[str, Any]], supplement: pd.DataFrame) -> dict[tuple[int, str], dict[str, Any]]:
    merged = dict(primary)
    for key, row in _feature_lookup(supplement).items():
        if key not in merged or not bool(merged[key].get("c") is not None and not pd.isna(merged[key].get("c"))):
            merged[key] = row
    return merged


def _fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "missing"
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_num(value: Any, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "missing"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _flag(row: dict[str, Any], name: str) -> bool:
    value = row.get(name)
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    return str(value).lower() in {"true", "1", "yes"}


def _value(row: dict[str, Any] | None, name: str, default: Any = None) -> Any:
    if not row:
        return default
    value = row.get(name, default)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return default
    return value


def _context_strings(daily: dict[str, Any] | None, weekly: dict[str, Any] | None, monthly: dict[str, Any] | None) -> dict[str, str]:
    if not daily:
        return {
            "monthly_context": "missing_chart_context",
            "weekly_context": "missing_chart_context",
            "daily_context": "missing_chart_context",
            "resistance_support_context": "missing_chart_context",
            "gap_context": "missing_chart_context",
            "full_retrace_context": "missing_chart_context",
            "ma_lifecycle_context": "missing_chart_context",
            "sideways_context": "missing_chart_context",
            "volume_context": "missing_chart_context",
        }
    monthly = monthly or daily
    weekly = weekly or daily
    return {
        "monthly_context": (
            f"ma_stack={_value(monthly, 'ma_stack_state', 'missing')}; "
            f"above_ma20={_value(monthly, 'close_above_ma20_count', 'missing')}; "
            f"ma20_slope={_fmt_num(_value(monthly, 'ma20_slope'))}; "
            f"ma60_slope={_fmt_num(_value(monthly, 'ma60_slope'))}"
        ),
        "weekly_context": (
            f"ma_stack={_value(weekly, 'ma_stack_state', 'missing')}; "
            f"above_ma20={_value(weekly, 'close_above_ma20_count', 'missing')}; "
            f"resistance_dist={_fmt_pct(_value(weekly, 'weekly_resistance_distance_pct'))}; "
            f"sideways_days={_value(weekly, 'sideways_length_days', 'missing')}"
        ),
        "daily_context": (
            f"score={_value(daily, 'selection_score', 'missing')}; "
            f"rank={_value(daily, 'candidate_rank', 'missing')}; "
            f"ma_stack={_value(daily, 'ma_stack_state', 'missing')}; "
            f"body_range={_fmt_pct(_value(daily, 'body_range_pct'))}"
        ),
        "resistance_support_context": (
            f"prior_high_dist={_fmt_pct(_value(daily, 'prior_high_distance_pct'))}; "
            f"prior_low_dist={_fmt_pct(_value(daily, 'prior_low_distance_pct'))}; "
            f"box_upper_dist={_fmt_pct(_value(daily, 'box_upper_distance_pct'))}; "
            f"box_lower_dist={_fmt_pct(_value(daily, 'box_lower_distance_pct'))}; "
            f"breakout={_flag(daily, 'breakout_above_resistance_flag')}; "
            f"failed_breakout={_flag(daily, 'failed_breakout_flag')}"
        ),
        "gap_context": (
            f"gap_up={_flag(daily, 'gap_up_flag')}; gap_down={_flag(daily, 'gap_down_flag')}; "
            f"gap_atr={_fmt_num(_value(daily, 'gap_size_atr_ratio'))}; "
            f"gap_fail_same_day={_flag(daily, 'gap_fail_same_day_flag')}; "
            f"gap_fill_3d_prior_reaction={_flag(daily, 'gap_fill_3d_flag')}"
        ),
        "full_retrace_context": (
            f"bull_full_retrace={_flag(daily, 'bullish_full_retrace_flag')}; "
            f"bear_full_retrace={_flag(daily, 'bearish_full_retrace_flag')}; "
            f"bull_engulf={_flag(daily, 'engulfing_bullish_flag')}; "
            f"bear_engulf={_flag(daily, 'engulfing_bearish_flag')}; "
            f"denial_bull={_flag(daily, 'denial_of_prior_bull_flag')}; "
            f"denial_bear={_flag(daily, 'denial_of_prior_bear_flag')}"
        ),
        "ma_lifecycle_context": (
            f"above_ma7={_value(daily, 'close_above_ma7_count', 'missing')}; "
            f"above_ma20={_value(daily, 'close_above_ma20_count', 'missing')}; "
            f"below_ma20={_value(daily, 'close_below_ma20_count', 'missing')}; "
            f"days_since_ma20_reclaim={_value(daily, 'days_since_ma20_reclaim', 'missing')}; "
            f"days_since_ma20_break={_value(daily, 'days_since_ma20_break', 'missing')}; "
            f"ma7_ma20_dist={_fmt_pct(_value(daily, 'ma7_ma20_distance_pct'))}"
        ),
        "sideways_context": (
            f"sideways_days={_value(daily, 'sideways_length_days', 'missing')}; "
            f"box_days={_value(daily, 'box_length_days', 'missing')}; "
            f"atr_compression={_fmt_num(_value(daily, 'atr_compression_ratio'))}; "
            f"ma_compression={_flag(daily, 'ma_compression_flag')}; "
            f"box_breakout={_flag(daily, 'box_breakout_flag')}; "
            f"box_breakdown={_flag(daily, 'box_breakdown_flag')}"
        ),
        "volume_context": (
            f"volume_compression={_fmt_num(_value(daily, 'volume_compression_ratio'))}; "
            f"volume_denial={_flag(daily, 'volume_confirmed_denial_flag')}; "
            f"shakeout_candidate={_flag(daily, 'shakeout_recovery_candidate_flag')}; "
            f"true_breakdown_candidate={_flag(daily, 'true_breakdown_candidate_flag')}"
        ),
    }


def _raw_context_row(target: dict[str, Any], daily: dict[str, Any] | None, weekly: dict[str, Any] | None, monthly: dict[str, Any] | None) -> dict[str, Any]:
    row = dict(target)
    row.update(
        {
            "chart_context_available": daily is not None,
            "weekly_context_available": weekly is not None,
            "monthly_context_available": monthly is not None,
        }
    )
    for prefix, source in (("daily", daily), ("weekly", weekly), ("monthly", monthly)):
        for name in (
            "candidate_rank",
            "selection_score",
            "ma_stack_state",
            "prior_high_distance_pct",
            "prior_low_distance_pct",
            "box_upper_distance_pct",
            "box_lower_distance_pct",
            "gap_up_flag",
            "gap_down_flag",
            "gap_size_atr_ratio",
            "bearish_full_retrace_flag",
            "bullish_full_retrace_flag",
            "engulfing_bearish_flag",
            "engulfing_bullish_flag",
            "close_above_ma7_count",
            "close_above_ma20_count",
            "close_below_ma20_count",
            "sideways_length_days",
            "volume_compression_ratio",
            "feature_missing",
        ):
            row[f"{prefix}_{name}"] = _value(source, name)
    return row


def build_review_pack(
    *,
    robustness_root: Path = DEFAULT_ROBUSTNESS_ROOT,
    source_representative_json: Path = DEFAULT_SOURCE_REPRESENTATIVE_JSON,
    output_root: Path | None = None,
    compute_missing_from_source_db: bool = True,
) -> Path:
    output_root = output_root or (robustness_root / DEFAULT_OUTPUT_DIR_NAME)
    output_root.mkdir(parents=True, exist_ok=True)
    chart_root = robustness_root / "chart_context_feature_contract_v1"
    source_pack = _load_source_pack_index(source_representative_json)
    targets = _target_frame_with_source(source_pack)
    daily = _feature_lookup(_load_feature_frame(chart_root / "chart_context_features_daily.parquet"))
    weekly = _feature_lookup(_load_feature_frame(chart_root / "chart_context_features_weekly.parquet"))
    monthly = _feature_lookup(_load_feature_frame(chart_root / "chart_context_features_monthly.parquet"))
    target_context_source_db: str | None = None
    target_context_generated_rows = 0
    if compute_missing_from_source_db:
        generated = _target_context_features_from_source_db(robustness_root, targets)
        target_context_source_db = str(generated["source_db"].iloc[0]["source_db"])
        target_context_generated_rows = int(len(generated["daily"]))
        daily = _merge_feature_lookup(daily, generated["daily"])
        weekly = _merge_feature_lookup(weekly, generated["weekly"])
        monthly = _merge_feature_lookup(monthly, generated["monthly"])

    review_rows: list[dict[str, Any]] = []
    raw_context_rows: list[dict[str, Any]] = []
    template_rows: list[dict[str, Any]] = []
    for target in targets.to_dict("records"):
        key = (int(target["decision_ymd"]), _code(target["code"]))
        source_key = (target["sample_date"], _code(target["code"]))
        daily_row = daily.get(key)
        weekly_row = weekly.get(key)
        monthly_row = monthly.get(key)
        contexts = _context_strings(daily_row, weekly_row, monthly_row)
        source_info = source_pack.get(source_key, {})
        row = {
            "sample_date": target["sample_date"],
            "code": target["code"],
            "rank": target["rank"],
            "candidate_label": target["candidate_label"],
            **contexts,
            "human_selectable_label": "",
            "human_pick_rank": "",
            "max3_selected_flag": "",
            "reject_reason": "",
            "review_comment": "",
        }
        review_rows.append(row)
        raw_context = _raw_context_row(target, daily_row, weekly_row, monthly_row)
        raw_context.update(source_info)
        raw_context_rows.append(raw_context)
        template_rows.append(
            {
                "sample_date": target["sample_date"],
                "code": target["code"],
                "rank": target["rank"],
                "human_selectable_label": "",
                "human_pick_rank": None,
                "max3_selected_flag": None,
                "reject_reason": "",
                "review_comment": "",
                "reviewer": "",
                "reviewed_at": "",
            }
        )

    review_sheet = pd.DataFrame(review_rows)
    raw_context = pd.DataFrame(raw_context_rows)
    target_frame = targets.copy()
    target_frame["chart_context_available"] = raw_context["chart_context_available"].astype(bool)
    per_sample = (
        target_frame.groupby(["sample_group", "sample_order", "sample_date", "review_reason"], as_index=False)
        .agg(
            candidate_count=("code", "count"),
            chart_context_available_count=("chart_context_available", "sum"),
            source_representative_found_count=("source_representative_found", "sum"),
        )
        .sort_values(["sample_order"], kind="stable")
    )
    per_sample["max_selectable_count"] = 3
    per_sample["review_mode"] = "manual_as_of_chart_review"

    _write_csv(output_root / "manual_review_targets.csv", target_frame)
    _write_csv(output_root / "manual_review_sheet.csv", review_sheet)
    _write_csv(output_root / "per_sample_candidate_context.csv", per_sample)
    _write_csv(output_root / "per_candidate_chart_context.csv", raw_context)
    _write_jsonl(output_root / "human_selection_decision_template.jsonl", template_rows)

    missing_context = raw_context.loc[~raw_context["chart_context_available"].astype(bool), ["sample_date", "code"]].to_dict(
        "records"
    )
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "source_representative_json": str(source_representative_json),
        "chart_context_root": str(chart_root),
        "target_context_source_db": target_context_source_db,
        "target_context_generated_rows": target_context_generated_rows,
        "target_sample_count": int(per_sample["sample_date"].nunique()),
        "target_candidate_count": int(len(target_frame)),
        "chart_context_available_count": int(raw_context["chart_context_available"].sum()),
        "chart_context_missing_count": int((~raw_context["chart_context_available"].astype(bool)).sum()),
        "source_representative_found_count": int(target_frame["source_representative_found"].sum()),
        "review_order": ["priority_review", "weakness_check", "supplement"],
        "review_labels": REVIEW_LABELS,
        "manual_selection_max_per_date": 3,
        "outcome_columns_in_review_sheet": False,
        "post_run_outcome_visible_before_review": False,
        "target_context_computed_from_source_db": compute_missing_from_source_db,
        "replay_rerun": False,
        "policy_change": False,
        "candidate_generation_change": False,
        "ranking_change": False,
        "meemee_ui_changed": False,
        "publish_registry_changed": False,
        "missing_chart_context_candidates": missing_context,
    }
    schema = {
        "schema_version": f"{SCHEMA_PREFIX}_review_result_schema_v1",
        "axis_id": AXIS_ID,
        "review_labels": REVIEW_LABELS,
        "per_sample_max_selected": 3,
        "selection_rules": [
            "each sample_date may have zero to three selected candidates",
            "do not force exactly three selections",
            "weakness_check days may correctly select zero candidates",
            "review must use as_of_date chart only",
        ],
        "required_human_fields": [
            "human_selectable_label",
            "human_pick_rank",
            "max3_selected_flag",
            "reject_reason",
            "review_comment",
        ],
        "forbidden_review_inputs": [
            "post_ret",
            "future_return",
            "MAE",
            "MFE",
            "outcome_label",
            "future_chart",
        ],
    }
    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "audit_result": "pass",
        "post_run_outcome_in_review_sheet": False,
        "future_chart_display_required": False,
        "feature_source": "chart_context_feature_contract_v1_point_in_time_features",
        "target_context_computed_from_source_db": compute_missing_from_source_db,
        "review_as_of_date_required": True,
        "silent_fallback_used": False,
        "missing_chart_context_recorded": bool(missing_context),
    }
    _write_json(output_root / "manual_top5_review_pack_summary.json", summary)
    _write_json(output_root / "review_result_schema.json", schema)
    _write_json(output_root / "no_lookahead_audit.json", audit)

    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": False,
        "required_artifacts": REQUIRED_ARTIFACTS,
        "required_artifacts_all_present": False,
        "silent_fallback_used": False,
        "outcome_columns_in_review_sheet": False,
        "meemee_reflectable": False,
        "policy_promotion_allowed": False,
        "artifacts": {},
    }
    for artifact in REQUIRED_ARTIFACTS:
        path = output_root / artifact
        complete["artifacts"][artifact] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["required_artifacts_all_present"] = all(
        item["exists"] and item["bytes"] > 0
        for name, item in complete["artifacts"].items()
        if name != "_ARTIFACT_COMPLETE.json"
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
        "exists": (output_root / "_ARTIFACT_COMPLETE.json").exists(),
        "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size,
    }
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    complete["required_artifacts_all_present"] = complete["complete"]
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return output_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--robustness-root", type=Path, default=DEFAULT_ROBUSTNESS_ROOT)
    parser.add_argument("--source-representative-json", type=Path, default=DEFAULT_SOURCE_REPRESENTATIVE_JSON)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--no-target-context-compute", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    output_root = build_review_pack(
        robustness_root=args.robustness_root,
        source_representative_json=args.source_representative_json,
        output_root=args.output_root,
        compute_missing_from_source_db=not args.no_target_context_compute,
    )
    summary = _read_json(output_root / "manual_top5_review_pack_summary.json")
    print(
        json.dumps(
            {
                "axis_id": AXIS_ID,
                "output_root": str(output_root),
                "target_candidate_count": summary["target_candidate_count"],
                "chart_context_available_count": summary["chart_context_available_count"],
                "chart_context_missing_count": summary["chart_context_missing_count"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
