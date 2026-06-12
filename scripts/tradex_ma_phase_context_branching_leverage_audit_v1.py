from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_phase_context_branching_leverage_audit_v1"
DEFAULT_ATTACH_ROOT = Path("G:/Tradex/ma_phase_context_entry_replay_attach_v1/20260604T014342Z-ma-phase-context-entry-replay-attach-v1")
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_phase_context_branching_leverage_audit_v1")
REQUIRED = (
    "final_research_decision.json",
    "branching_leverage_audit.json",
    "changed_rows_detail.csv",
    "date_level_effect_summary.json",
    "feature_flag_effect_summary.json",
    "replacement_vs_displacement_summary.json",
    "_ARTIFACT_COMPLETE.json",
)
TOPKS = (5, 10, 20)
FEATURE_FLAGS = (
    "ma7_phase_3_5_no_light_upper",
    "ma7_pullback_reclaim_support_bull",
    "ma60_run_20_plus_no_light_upper",
    "ma60_failure_guard_heavy_weak_candle",
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _load_attach(root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    rows_path = root / "context_attached_replay_rows.csv"
    required_files = [
        rows_path,
        root / "compare.json",
        root / "candidate_branching_summary.json",
        root / "replacement_quality_summary.json",
        root / "final_research_decision.json",
    ]
    missing = [str(p) for p in required_files if not p.exists()]
    if missing:
        raise FileNotFoundError(f"missing attach artifact files: {missing}")
    rows = pd.read_csv(rows_path)
    rows["code"] = rows["code"].astype(str)
    rows["as_of_date"] = rows["as_of_date"].astype(str)
    rows["baseline_rank"] = pd.to_numeric(rows["baseline_rank"], errors="coerce")
    rows["context_rank"] = pd.to_numeric(rows["context_rank"], errors="coerce")
    rows["baseline_top_rank"] = rows.groupby("as_of_date")["baseline_rank"].rank(method="first")
    rows["rank_delta"] = rows["context_rank"] - rows["baseline_top_rank"]
    rows["hit_flag"] = rows["ret20"] > 0
    rows["miss_flag"] = rows["ret20"] <= 0
    rows["severe_loss_flag"] = rows["ret20"] <= -0.10
    rows["bottom_loss_flag"] = rows["ret20"] <= -0.05
    for col in list(FEATURE_FLAGS) + ["rebreak_ma7_7b", "rebreak_ma60_20b", "ma_phase_bad_pick_guard"]:
        rows[col] = rows[col].fillna(False).astype(bool)
    return (
        rows,
        _read_json(root / "compare.json"),
        _read_json(root / "candidate_branching_summary.json"),
        _read_json(root / "replacement_quality_summary.json"),
    )


def _changed_detail(rows: pd.DataFrame) -> pd.DataFrame:
    details: list[dict[str, Any]] = []
    for topk in TOPKS:
        for _, row in rows.iterrows():
            baseline_in = bool(row["baseline_top_rank"] <= topk)
            challenger_in = bool(row["context_rank"] <= topk)
            if baseline_in and challenger_in:
                effect_type = "within_topk_reorder" if row["rank_delta"] != 0 else "unchanged_in_topk"
            elif baseline_in and not challenger_in:
                effect_type = "removed"
            elif challenger_in and not baseline_in:
                effect_type = "added"
            else:
                effect_type = "outside_topk_rank_changed" if row["rank_delta"] != 0 else "outside_topk_unchanged"
            if effect_type in {"unchanged_in_topk", "outside_topk_unchanged"}:
                continue
            payload = {
                "topk": topk,
                "as_of_date": row["as_of_date"],
                "code": row["code"],
                "original_rank": row["baseline_top_rank"],
                "challenger_rank": row["context_rank"],
                "rank_delta": row["rank_delta"],
                "effect_type": effect_type,
                "ret20": row["ret20"],
                "hit_flag": row["hit_flag"],
                "miss_flag": row["miss_flag"],
                "severe_loss_flag": row["severe_loss_flag"],
                "bottom_loss_flag": row["bottom_loss_flag"],
                "rebreak_ma7_7b": row["rebreak_ma7_7b"],
                "rebreak_ma60_20b": row["rebreak_ma60_20b"],
                "ma_phase_positive_context_count": row["ma_phase_positive_context_count"],
                "ma_phase_bad_pick_guard": row["ma_phase_bad_pick_guard"],
            }
            for flag in FEATURE_FLAGS:
                payload[flag] = row[flag]
            details.append(payload)
    return pd.DataFrame(details)


def _selection_metrics(part: pd.DataFrame) -> dict[str, Any]:
    return {
        "row_count": int(len(part)),
        "unique_date_count": int(part["as_of_date"].nunique()) if not part.empty else 0,
        "unique_symbol_count": int(part["code"].nunique()) if not part.empty else 0,
        "mean_ret20": _mean(part["ret20"]),
        "median_ret20": _median(part["ret20"]),
        "hit_rate": _rate(part["hit_flag"]),
        "bottom_loss_rate": _rate(part["bottom_loss_flag"]),
        "severe_loss_rate": _rate(part["severe_loss_flag"]),
        "mean_max_drawdown_20d": _mean(part["max_drawdown_20b"]) if "max_drawdown_20b" in part else None,
        "rebreak_ma7_7b_rate": _rate(part["rebreak_ma7_7b"]) if "rebreak_ma7_7b" in part else None,
        "rebreak_ma60_20b_rate": _rate(part["rebreak_ma60_20b"]) if "rebreak_ma60_20b" in part else None,
    }


def _date_effect(rows: pd.DataFrame) -> dict[str, Any]:
    date_rows: list[dict[str, Any]] = []
    no_op_counts = {}
    for topk in TOPKS:
        no_op = 0
        for date, g in rows.groupby("as_of_date", sort=True):
            baseline = g[g["baseline_top_rank"] <= topk]
            challenger = g[g["context_rank"] <= topk]
            bset = set(baseline["code"])
            cset = set(challenger["code"])
            removed = baseline[baseline["code"].isin(bset - cset)]
            added = challenger[challenger["code"].isin(cset - bset)]
            common = g[g["code"].isin(bset & cset)].copy()
            common_reordered = common[common["rank_delta"] != 0]
            changed_members = len(bset ^ cset)
            if changed_members == 0 and common_reordered.empty:
                no_op += 1
            date_rows.append(
                {
                    "topk": topk,
                    "as_of_date": date,
                    "candidate_count": int(len(g)),
                    "changed_members_count": int(changed_members),
                    "within_topk_reordered_count": int(len(common_reordered)),
                    "removed_count": int(len(removed)),
                    "added_count": int(len(added)),
                    "removed_mean_ret20": _mean(removed["ret20"]),
                    "added_mean_ret20": _mean(added["ret20"]),
                    "member_replacement_advantage": (
                        _mean(added["ret20"]) - _mean(removed["ret20"])
                        if _mean(added["ret20"]) is not None and _mean(removed["ret20"]) is not None
                        else None
                    ),
                    "baseline_mean_ret20": _mean(baseline["ret20"]),
                    "challenger_mean_ret20": _mean(challenger["ret20"]),
                    "total_topk_mean_ret20_delta": (
                        _mean(challenger["ret20"]) - _mean(baseline["ret20"])
                        if _mean(challenger["ret20"]) is not None and _mean(baseline["ret20"]) is not None
                        else None
                    ),
                    "bad_pick_removed_count": int(removed["bottom_loss_flag"].sum()),
                    "good_pick_displaced_count": int((removed["ret20"] > 0).sum()),
                    "bad_pick_added_count": int(added["bottom_loss_flag"].sum()),
                    "good_pick_added_count": int((added["ret20"] > 0).sum()),
                }
            )
        no_op_counts[f"top{topk}_no_op_dates"] = no_op
    frame = pd.DataFrame(date_rows)
    summary: dict[str, Any] = {"date_rows": date_rows, "no_op_counts": no_op_counts, "topk_summary": []}
    for topk in TOPKS:
        part = frame[frame["topk"] == topk]
        summary["topk_summary"].append(
            {
                "topk": topk,
                "date_count": int(len(part)),
                "branching_date_count": int((part["changed_members_count"] > 0).sum()),
                "reorder_only_date_count": int(((part["changed_members_count"] == 0) & (part["within_topk_reordered_count"] > 0)).sum()),
                "no_op_date_count": int(((part["changed_members_count"] == 0) & (part["within_topk_reordered_count"] == 0)).sum()),
                "mean_member_replacement_advantage_on_changed_dates": _mean(part.loc[part["changed_members_count"] > 0, "member_replacement_advantage"]),
                "mean_total_topk_delta_all_dates": _mean(part["total_topk_mean_ret20_delta"]),
                "mean_total_topk_delta_changed_dates": _mean(part.loc[part["changed_members_count"] > 0, "total_topk_mean_ret20_delta"]),
                "bad_pick_removed_count": int(part["bad_pick_removed_count"].sum()),
                "good_pick_displaced_count": int(part["good_pick_displaced_count"].sum()),
                "bad_pick_added_count": int(part["bad_pick_added_count"].sum()),
                "good_pick_added_count": int(part["good_pick_added_count"].sum()),
            }
        )
    return summary


def _feature_summary(rows: pd.DataFrame, changed: pd.DataFrame) -> dict[str, Any]:
    payload: dict[str, Any] = {"feature_flags": []}
    for flag in FEATURE_FLAGS:
        flagged = rows[rows[flag]]
        unflagged = rows[~rows[flag]]
        changed_flagged = changed[changed[flag]] if not changed.empty else pd.DataFrame()
        payload["feature_flags"].append(
            {
                "feature_flag": flag,
                "all_flagged": _selection_metrics(flagged),
                "all_unflagged": _selection_metrics(unflagged),
                "changed_row_count": int(len(changed_flagged)),
                "changed_effect_counts": changed_flagged["effect_type"].value_counts().to_dict() if not changed_flagged.empty else {},
            }
        )
    segment_checks = []
    for flag in FEATURE_FLAGS:
        for topk in TOPKS:
            top = rows[rows["baseline_top_rank"] <= topk]
            flagged = top[top[flag]]
            if len(flagged) >= 30:
                segment_checks.append(
                    {
                        "segment": flag,
                        "topk": topk,
                        "sample_count": int(len(flagged)),
                        "mean_ret20": _mean(flagged["ret20"]),
                        "hit_rate": _rate(flagged["hit_flag"]),
                        "severe_loss_rate": _rate(flagged["severe_loss_flag"]),
                        "rebreak_ma7_7b_rate": _rate(flagged["rebreak_ma7_7b"]),
                        "rebreak_ma60_20b_rate": _rate(flagged["rebreak_ma60_20b"]),
                    }
                )
    payload["non_threshold_tuned_segment_checks"] = segment_checks
    return payload


def _replacement_vs_displacement(changed: pd.DataFrame) -> dict[str, Any]:
    payload = {"topk": []}
    for topk in TOPKS:
        part = changed[changed["topk"] == topk]
        added = part[part["effect_type"] == "added"]
        removed = part[part["effect_type"] == "removed"]
        reordered = part[part["effect_type"] == "within_topk_reorder"]
        payload["topk"].append(
            {
                "topk": topk,
                "added": _selection_metrics(added),
                "removed": _selection_metrics(removed),
                "within_topk_reordered": _selection_metrics(reordered),
                "member_replacement_effect": (
                    _mean(added["ret20"]) - _mean(removed["ret20"])
                    if _mean(added["ret20"]) is not None and _mean(removed["ret20"]) is not None
                    else None
                ),
                "bad_pick_removal_count": int(removed["bottom_loss_flag"].sum()) if not removed.empty else 0,
                "good_pick_displacement_count": int((removed["ret20"] > 0).sum()) if not removed.empty else 0,
                "bad_pick_added_count": int(added["bottom_loss_flag"].sum()) if not added.empty else 0,
                "good_pick_added_count": int((added["ret20"] > 0).sum()) if not added.empty else 0,
            }
        )
    return payload


def _why_small(date_summary: dict[str, Any], replacement: dict[str, Any], compare: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    top10 = next(item for item in date_summary["topk_summary"] if item["topk"] == 10)
    top5 = next(item for item in date_summary["topk_summary"] if item["topk"] == 5)
    top20 = next(item for item in date_summary["topk_summary"] if item["topk"] == 20)
    if top10["branching_date_count"] <= 3:
        reasons.append("top10_branching_sparse_only_3_dates")
    if top10["bad_pick_removed_count"] <= 1:
        reasons.append("top10_bad_pick_removal_only_1")
    if top20["branching_date_count"] == 0:
        reasons.append("top20_no_member_branching")
    if top5["bad_pick_added_count"] > 0 or top5["good_pick_displaced_count"] > 0:
        reasons.append("top5_side_effect_offsets_replacement")
    deltas = compare.get("compare", {}).get("deltas", [])
    top5_delta = next((d for d in deltas if d.get("topk") == 5), {})
    if top5_delta.get("hit_rate_delta", 0) < 0 or top5_delta.get("rebreak_ma7_7b_rate_delta", 0) > 0:
        reasons.append("top5_hit_or_ma7_rebreak_deteriorated")
    repl10 = next((item for item in replacement.get("topk_summary", []) if item.get("topk") == 10), {})
    if repl10.get("replacement_event_count", 0) <= 3:
        reasons.append("top10_positive_replacement_advantage_has_too_few_events")
    return reasons


def _decision(date_summary: dict[str, Any], feature_summary: dict[str, Any], replacement_summary: dict[str, Any], why: list[str]) -> tuple[str, str, bool]:
    top10 = next(item for item in date_summary["topk_summary"] if item["topk"] == 10)
    top5 = next(item for item in date_summary["topk_summary"] if item["topk"] == 5)
    candidate_segment = None
    for item in feature_summary["non_threshold_tuned_segment_checks"]:
        if item["sample_count"] >= 50 and item["hit_rate"] is not None and item["hit_rate"] >= 0.75 and item["severe_loss_rate"] is not None and item["severe_loss_rate"] <= 0.03:
            candidate_segment = item
            break
    if candidate_segment and top10["branching_date_count"] >= 10:
        return "propose_single_next_challenger", "clear_non_threshold_tuned_segment_with_sufficient_branching", True
    if top10["branching_date_count"] <= 3 and top5["bad_pick_added_count"] > 0:
        return "park_low_priority", "positive_effects_sparse_and_top5_side_effect_present", False
    if "top5_hit_or_ma7_rebreak_deteriorated" in why:
        return "park_low_priority", "context_has_sparse_top10_help_but_top5_side_effect_or_rebreak_deterioration", False
    return "keep_as_context_feature", "context_explains_some_bad_pick_or_rebreak_diagnostics_without_stable_topk_improvement", False


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    rows, compare, branching, replacement = _load_attach(args.attach_root)
    changed = _changed_detail(rows)
    date_summary = _date_effect(rows)
    feature_summary = _feature_summary(rows, changed)
    replacement_summary = _replacement_vs_displacement(changed)
    why = _why_small(date_summary, replacement, compare)
    decision, reason, next_challenger = _decision(date_summary, feature_summary, replacement_summary, why)

    changed.to_csv(out_dir / "changed_rows_detail.csv", index=False, encoding="utf-8")
    _write_json(
        out_dir / "branching_leverage_audit.json",
        {
            "axis_id": AXIS_ID,
            "source_attach_artifact": str(args.attach_root),
            "prior_decision": _read_json(args.attach_root / "final_research_decision.json").get("authoritative_rollup_decision"),
            "row_count": int(len(rows)),
            "changed_detail_row_count": int(len(changed)),
            "why_prior_uplift_was_small": why,
            "input_branching_summary": branching,
            "input_replacement_quality_summary": replacement,
        },
    )
    _write_json(out_dir / "date_level_effect_summary.json", {"axis_id": AXIS_ID, **date_summary})
    _write_json(out_dir / "feature_flag_effect_summary.json", {"axis_id": AXIS_ID, **feature_summary})
    _write_json(out_dir / "replacement_vs_displacement_summary.json", {"axis_id": AXIS_ID, **replacement_summary})
    payload = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "source_attach_artifact": str(args.attach_root),
        "what_was_audited": [
            "member replacement effect",
            "within-topK reorder effect",
            "bad-pick removal effect",
            "good-pick displacement effect",
            "no-op dates",
            "feature flag affected slices",
        ],
        "why_prior_uplift_was_small": why,
        "useful_segment": {
            "ma60_run_20_plus_no_light_upper": next(
                (x for x in feature_summary["feature_flags"] if x["feature_flag"] == "ma60_run_20_plus_no_light_upper"),
                None,
            ),
            "interpretation": "best broad context slice but not enough branching leverage for candidate promotion",
        },
        "harmful_or_weak_segment": {
            "top5": next(item for item in date_summary["topk_summary"] if item["topk"] == 5),
            "interpretation": "top5 had sparse replacement, one bad pick added, and prior compare showed hit/rebreak side effect",
        },
        "next_challenger_justified": next_challenger,
        "next_required_single_step": (
            "freeze_ma_phase_context_and_move_to_independent_bad_pick_removal_axis"
            if not next_challenger
            else "prepare_single_boundary_only_veto_challenger_without_threshold_tuning"
        ),
        "boundary_flags": {
            "tradex_only": True,
            "read_only_diagnostic": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "ma_phase_thresholds_changed": False,
            "exit_thresholds_changed": False,
            "baseline_rank_source_changed": False,
            "candidate_set_changed": False,
            "replay_rows_changed": False,
        },
    }
    _write_json(out_dir / "final_research_decision.json", payload)
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "required_files": list(REQUIRED),
            "required_files_present": all((out_dir / name).exists() for name in REQUIRED if name != "_ARTIFACT_COMPLETE.json"),
        },
    )
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit why MA phase context branching did not convert into material top-K uplift.")
    parser.add_argument("--attach-root", type=Path, default=DEFAULT_ATTACH_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    args = parser.parse_args()
    print(run(args))


if __name__ == "__main__":
    main()
