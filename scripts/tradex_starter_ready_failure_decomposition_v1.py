from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "starter_ready_failure_decomposition_v1"
DEFAULT_REPLAY_ROOT = Path(
    r"G:\Tradex\starter_candidate_chart_review_historical_replay_v1\20260525T065259Z-starter-candidate-chart-review-historical-replay-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_ready_failure_decomposition_v1")

REQUIRED_ARTIFACTS = (
    "failure_decomposition_summary.json",
    "starter_ready_failure_rows.csv",
    "failure_mode_metrics.json",
    "subtype_breakdown.json",
    "trigger_vs_invalidation_failure_audit.json",
    "reusable_negative_tags.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

OPTIONAL_ANALYSIS_COLUMNS = {
    "daily_ma_state": ["close_above_ma7", "close_above_ma20", "close_above_ma60", "ma7_slope", "ma20_slope", "ma60_slope"],
    "weekly_supportiveness": ["weekly_trend_direction", "weekly_close_above_ma10", "weekly_close_above_ma30"],
    "monthly_supportiveness": ["monthly_trend_direction", "monthly_high_zone_context", "monthly_overextension_risk"],
    "extension_flags": ["dist_ma20_pct", "dist_ma60_pct"],
    "failed_high_bearish_flags": ["failed_high", "large_bearish_candle", "upper_wick_ratio"],
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


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def pattern_type(family: Any) -> str:
    text = str(family or "")
    if "pullback" in text:
        return "pullback"
    if "breakout" in text:
        return "breakout"
    if "early" in text:
        return "early_trend"
    if "mature" in text:
        return "mature_trend"
    if "range" in text:
        return "range"
    if "overextension" in text:
        return "overextension"
    return "unknown"


def ret20_bucket(value: Any) -> str:
    ret = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(ret):
        return "unavailable"
    if ret <= -0.10:
        return "severe"
    if ret <= -0.05:
        return "bad"
    if ret > 0:
        return "good"
    return "flat_or_negative"


def failure_signature(row: pd.Series) -> str:
    parts = [str(row.get("pattern_type", "unknown"))]
    parts.append("trigger_hit" if bool(row.get("trigger_hit")) else "no_trigger_hit")
    parts.append("invalidation_hit" if bool(row.get("invalidation_hit")) else "no_invalidation_hit")
    parts.append(str(row.get("ret20_bucket", "unavailable")))
    reason = str(row.get("reason_summary") or "").lower()
    if "monthly context is supportive" in reason:
        parts.append("monthly_supportive_text")
    if "monthly context is not clearly supportive" in reason:
        parts.append("monthly_not_supportive_text")
    if "failed" in reason or "bearish" in reason:
        parts.append("failed_or_bearish_text")
    return "|".join(parts)


def missing_column_report(rows: pd.DataFrame) -> dict[str, Any]:
    return {axis: [col for col in cols if col not in rows.columns] for axis, cols in OPTIONAL_ANALYSIS_COLUMNS.items()}


def metric_block(frame: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce").dropna()
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if "decision_date" in frame else 0,
        "code_count": int(frame["code"].astype(str).nunique()) if "code" in frame else 0,
        "mean_ret20": float(ret20.mean()) if not ret20.empty else None,
        "median_ret20": float(ret20.median()) if not ret20.empty else None,
        "hit_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
        "trigger_hit_rate": float(frame["trigger_hit"].dropna().astype(bool).mean()) if "trigger_hit" in frame and not frame["trigger_hit"].dropna().empty else None,
        "invalidation_hit_rate": float(frame["invalidation_hit"].dropna().astype(bool).mean()) if "invalidation_hit" in frame and not frame["invalidation_hit"].dropna().empty else None,
    }


def grouped_metrics(rows: pd.DataFrame, group_cols: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if rows.empty:
        return out
    for keys, group in rows.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        record = {col: key for col, key in zip(group_cols, keys)}
        record.update(metric_block(group))
        out.append(record)
    return sorted(out, key=lambda r: (r.get("sample_count") or 0, r.get("mean_ret20") or 0), reverse=True)


def build_negative_tags(starter_failures: pd.DataFrame, all_rows: pd.DataFrame) -> list[dict[str, Any]]:
    if starter_failures.empty:
        return []
    tags: list[dict[str, Any]] = []
    for signature, group in starter_failures.groupby("failure_signature", dropna=False):
        all_same = all_rows[all_rows["failure_signature"].eq(signature)] if "failure_signature" in all_rows else pd.DataFrame()
        metrics = metric_block(group)
        if len(group) >= 2 or (metrics["mean_ret20"] is not None and metrics["mean_ret20"] <= 0):
            tags.append(
                {
                    "tag": f"diagnostic_negative::{signature}",
                    "failure_signature": signature,
                    "starter_ready_failure_count": int(len(group)),
                    "all_label_same_signature_count": int(len(all_same)),
                    "metrics": metrics,
                    "status": "diagnostic_candidate_only",
                    "active_gate": False,
                }
            )
    return sorted(tags, key=lambda r: (r["starter_ready_failure_count"], -(r["metrics"]["mean_ret20"] or 0)), reverse=True)


def decide(starter_ready: pd.DataFrame, starter_failures: pd.DataFrame, missing: dict[str, Any], negative_tags: list[dict[str, Any]]) -> str:
    critical_missing = all(len(cols) == len(OPTIONAL_ANALYSIS_COLUMNS[axis]) for axis, cols in missing.items())
    if starter_ready.empty or len(starter_ready) < 10:
        return "sample_too_thin_for_decomposition"
    if critical_missing and starter_failures.empty:
        return "blocked_missing_columns"
    if negative_tags:
        return "negative_tag_candidate_found"
    if len(starter_failures) >= 1:
        return "manual_card_only_keep"
    return "close_as_failed_selection_signal"


def run(replay_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-ready-failure-decomposition-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = pd.read_csv(replay_root / "historical_replay_rows.csv", low_memory=False)
    replay_summary = json.loads((replay_root / "historical_replay_summary.json").read_text(encoding="utf-8"))
    label_metrics = json.loads((replay_root / "label_bucket_metrics.json").read_text(encoding="utf-8"))
    trigger_audit = json.loads((replay_root / "trigger_invalidation_audit.json").read_text(encoding="utf-8"))

    rows["pattern_type"] = rows["research_candidate_source_family"].map(pattern_type)
    rows["ret20_bucket"] = rows["ret20"].map(ret20_bucket)
    rows["reconstructed_snapshot"] = not bool(replay_summary.get("historical_snapshots_found"))
    rows["failure_signature"] = rows.apply(failure_signature, axis=1)
    starter_ready = rows[rows["manual_judgment"].eq("starter_ready")].copy()
    starter_failures = starter_ready[starter_ready["ret20_bucket"].isin(["flat_or_negative", "bad", "severe"])].copy()
    watch_successes = rows[(rows["manual_judgment"].eq("watch_continue")) & (pd.to_numeric(rows["ret20"], errors="coerce") > 0)].copy()
    starter_failures.to_csv(out / "starter_ready_failure_rows.csv", index=False)

    missing = missing_column_report(rows)
    failure_mode_metrics = {
        "starter_ready_all": metric_block(starter_ready),
        "starter_ready_failures": metric_block(starter_failures),
        "watch_continue_successes": metric_block(watch_successes),
        "by_failure_signature": grouped_metrics(starter_failures, ["failure_signature"]),
        "by_pattern_type": grouped_metrics(starter_failures, ["pattern_type"]),
        "by_trigger_invalidation": grouped_metrics(starter_failures, ["trigger_hit", "invalidation_hit"]),
    }
    subtype_breakdown = {
        "starter_ready_by_pattern": grouped_metrics(starter_ready, ["pattern_type"]),
        "starter_ready_by_ret20_bucket": grouped_metrics(starter_ready, ["ret20_bucket"]),
        "all_labels_by_pattern_and_label": grouped_metrics(rows, ["pattern_type", "manual_judgment"]),
        "missing_columns": missing,
    }
    trigger_vs_invalidation = {
        "starter_ready_failures": grouped_metrics(starter_failures, ["trigger_hit", "invalidation_hit", "ret20_bucket"]),
        "starter_ready_all": grouped_metrics(starter_ready, ["trigger_hit", "invalidation_hit"]),
        "input_trigger_invalidation_audit": trigger_audit,
    }
    negative_tags = build_negative_tags(starter_failures, rows)
    decision = decide(starter_ready, starter_failures, missing, negative_tags)

    top_loss_subtype = None
    by_pattern = failure_mode_metrics["by_pattern_type"]
    if by_pattern:
        top_loss_subtype = sorted(by_pattern, key=lambda r: (r["sample_count"], -(r["mean_ret20"] or 0)), reverse=True)[0]

    _write_json(out / "failure_mode_metrics.json", failure_mode_metrics)
    _write_json(out / "subtype_breakdown.json", subtype_breakdown)
    _write_json(out / "trigger_vs_invalidation_failure_audit.json", trigger_vs_invalidation)
    _write_json(
        out / "reusable_negative_tags.json",
        {
            "tags": negative_tags,
            "diagnostic_only": True,
            "active_gate": False,
            "validated_buy_claim": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "starter_ready_promotable": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "label_rescue_attempted": False,
            "threshold_retune_attempted": False,
            "negative_tags_diagnostic_only": True,
        },
    )
    _write_json(
        out / "failure_decomposition_summary.json",
        {
            "axis_id": AXIS_ID,
            "input_replay_root": replay_root,
            "input_replay_decision": replay_summary.get("decision"),
            "sample_count": int(len(rows)),
            "starter_ready_count": int(len(starter_ready)),
            "starter_ready_failure_count": int(len(starter_failures)),
            "watch_continue_success_count": int(len(watch_successes)),
            "top_loss_subtype": top_loss_subtype,
            "missing_columns": missing,
            "label_bucket_metrics_input": label_metrics,
            "starter_ready_promotable": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "confirmed_source_only": bool(replay_summary.get("confirmed_source_only")),
            "reconstructed_snapshot": not bool(replay_summary.get("historical_snapshots_found")),
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.replay_root, args.output_root))


if __name__ == "__main__":
    main()
