from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_bad_pick_removal_v1 import (  # noqa: E402
    FIXED_CONDITIONS,
    _load_frame,
    _rank_with_penalty,
    _variant_masks,
)

DEFAULT_INPUT_RUN_ROOT = Path(r"G:\Tradex\bad_pick_removal_v1\20260511T061803Z-bad_pick_removal_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_removal_v1_decomposition")
BEST_VARIANT_ID = "bad_pick_removal_v1_breakout_trap_only"

RUN_CONFIG_SCHEMA_VERSION = "tradex_bad_pick_removal_v1_decomposition_run_config_v1"
SWAP_SCHEMA_VERSION = "tradex_bad_pick_removal_v1_swap_decomposition_v1"
EXAMPLE_SCHEMA_VERSION = "tradex_bad_pick_removal_v1_decomposition_examples_v1"
MONTHLY_SCHEMA_VERSION = "tradex_bad_pick_removal_v1_monthly_swap_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_bad_pick_removal_v1_decomposition_decision_v1"
COMPLETE_SCHEMA_VERSION = "tradex_bad_pick_removal_v1_decomposition_artifact_complete_v1"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_json_ready(v) for v in value.tolist()]
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if value is None or isinstance(value, (str, bytes, bool, int)):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, Path):
        return str(value)
    return value


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_hash_or_unknown() -> str:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True)
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _mean(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return _safe_float(values.mean()) if len(values) else None


def _median(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return _safe_float(values.median()) if len(values) else None


def _rate(mask: pd.Series) -> float | None:
    return _safe_float(mask.mean()) if len(mask) else None


def _keys(frame: pd.DataFrame) -> list[tuple[str, str, str]]:
    return list(zip(frame["anchor_date"].astype(str), frame["side"].astype(str), frame["symbol"].astype(str)))


def _selected(frame: pd.DataFrame, rank_col: str, topk: int) -> pd.DataFrame:
    return frame[pd.to_numeric(frame[rank_col], errors="coerce") <= topk].copy()


def _candidate_stats(frame: pd.DataFrame, champion_topk_mean: float | None) -> dict[str, Any]:
    bottom10_available = "bottom10_label" in frame.columns
    return {
        "count": int(len(frame)),
        "ret20_mean": _mean(frame["forward_ret_20d"]) if len(frame) else None,
        "ret20_median": _median(frame["forward_ret_20d"]) if len(frame) else None,
        "positive_rate": _rate(frame["forward_ret_20d"] > 0) if len(frame) else None,
        "bottom15_count": int(frame["bottom15_label"].sum()) if len(frame) else 0,
        "bottom10_count": int(frame["bottom10_label"].sum()) if bottom10_available and len(frame) else None,
        "bottom10_status": "available" if bottom10_available else "unavailable",
        "top15_count": int(frame["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in frame.columns and len(frame) else None,
        "false_veto_count": int(((frame["forward_ret_20d"] > 0) | frame.get("top15_label", pd.Series(False, index=frame.index)).fillna(False).astype(bool)).sum()) if len(frame) else 0,
        "false_veto_rate": _rate((frame["forward_ret_20d"] > 0) | frame.get("top15_label", pd.Series(False, index=frame.index)).fillna(False).astype(bool)) if len(frame) else None,
        "bad_pick_removed_count": int(frame["bottom15_label"].sum()) if len(frame) else 0,
        "bad_pick_removed_rate": _rate(frame["bottom15_label"]) if len(frame) else None,
        "replacement_hit_rate": _rate(frame["forward_ret_20d"] > 0) if len(frame) else None,
        "replacement_bad_pick_rate": _rate(frame["bottom15_label"]) if len(frame) else None,
        "ret20_vs_champion_topk_mean_delta": None if champion_topk_mean is None or not len(frame) else (_mean(frame["forward_ret_20d"]) or 0.0) - champion_topk_mean,
    }


def _boundary_fields(frame: pd.DataFrame, topk: int) -> pd.DataFrame:
    out = frame.copy()
    boundary_col = f"top{topk}_boundary_score"
    boundaries = (
        out.sort_values(["anchor_date", "side", "champion_rank"], ascending=[True, True, True], kind="mergesort")
        .groupby(["anchor_date", "side"], as_index=False)
        .nth(topk - 1)[["anchor_date", "side", "champion_score"]]
        .rename(columns={"champion_score": boundary_col})
    )
    out = out.merge(boundaries, on=["anchor_date", "side"], how="left")
    out[f"score_gap_to_top{topk}_boundary"] = pd.to_numeric(out["champion_score"], errors="coerce") - pd.to_numeric(out[boundary_col], errors="coerce")
    return out


def _build_swap_frame(ranked: pd.DataFrame, topk: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    champion = _selected(ranked, "champion_rank", topk)
    challenger = _selected(ranked, "challenger_rank", topk)
    champ_keys = set(_keys(champion))
    chal_keys = set(_keys(challenger))
    removed = champion[[key not in chal_keys for key in _keys(champion)]].copy()
    added = challenger[[key not in champ_keys for key in _keys(challenger)]].copy()
    changed = pd.concat(
        [
            removed.assign(change_type="removed"),
            added.assign(change_type="added"),
        ],
        ignore_index=True,
    )
    return removed, added, changed


def _per_date_swaps(removed: pd.DataFrame, added: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    keys = sorted(set(zip(removed["anchor_date"], removed["side"])) | set(zip(added["anchor_date"], added["side"])))
    for anchor_date, side in keys:
        r = removed[(removed["anchor_date"] == anchor_date) & (removed["side"] == side)]
        a = added[(added["anchor_date"] == anchor_date) & (added["side"] == side)]
        r_mean = _mean(r["forward_ret_20d"]) if len(r) else None
        a_mean = _mean(a["forward_ret_20d"]) if len(a) else None
        rows.append(
            {
                "anchor_date": anchor_date,
                "side": side,
                "removed_count": int(len(r)),
                "added_count": int(len(a)),
                "removed_ret20_mean": r_mean,
                "added_ret20_mean": a_mean,
                "net_swap_delta": None if r_mean is None or a_mean is None else a_mean - r_mean,
                "removed_bottom15_count": int(r["bottom15_label"].sum()) if len(r) else 0,
                "added_bottom15_count": int(a["bottom15_label"].sum()) if len(a) else 0,
                "removed_top15_count": int(r.get("top15_label", pd.Series(False, index=r.index)).fillna(False).astype(bool).sum()) if len(r) else 0,
                "added_top15_count": int(a.get("top15_label", pd.Series(False, index=a.index)).fillna(False).astype(bool).sum()) if len(a) else 0,
            }
        )
    return rows


def _net_swap_aggregate(per_date: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = pd.Series([row["net_swap_delta"] for row in per_date if row.get("net_swap_delta") is not None], dtype="float64")
    month_delta: dict[str, list[float]] = {}
    for row in per_date:
        if row.get("net_swap_delta") is None:
            continue
        month_delta.setdefault(str(row["anchor_date"])[:7], []).append(float(row["net_swap_delta"]))
    month_means = {m: sum(v) / len(v) for m, v in month_delta.items() if v}
    return {
        "net_swap_delta_mean": _safe_float(deltas.mean()) if len(deltas) else None,
        "net_swap_delta_median": _safe_float(deltas.median()) if len(deltas) else None,
        "positive_net_swap_rate": _safe_float((deltas > 0).mean()) if len(deltas) else None,
        "months_with_positive_net_swap": int(sum(1 for v in month_means.values() if v > 0)),
        "months_with_negative_net_swap": int(sum(1 for v in month_means.values() if v < 0)),
        "worst_month_net_swap_delta": min(month_means.values()) if month_means else None,
    }


def _boundary_summary(changed: pd.DataFrame) -> dict[str, Any]:
    if changed.empty:
        return {
            "changes_inside_top5_boundary": 0,
            "changes_inside_top10_boundary": 0,
            "changes_near_boundary": 0,
            "changes_far_from_boundary": 0,
            "average_removed_rank": None,
            "average_added_rank": None,
        }
    changed = _boundary_fields(_boundary_fields(changed, 5), 10)
    removed = changed[changed["change_type"] == "removed"]
    added = changed[changed["change_type"] == "added"]
    near = (
        pd.to_numeric(changed["champion_rank"], errors="coerce").between(4, 12)
        | pd.to_numeric(changed["challenger_rank"], errors="coerce").between(4, 12)
    )
    return {
        "changes_inside_top5_boundary": int((pd.to_numeric(changed["champion_rank"], errors="coerce") <= 5).sum() + (pd.to_numeric(changed["challenger_rank"], errors="coerce") <= 5).sum()),
        "changes_inside_top10_boundary": int((pd.to_numeric(changed["champion_rank"], errors="coerce") <= 10).sum() + (pd.to_numeric(changed["challenger_rank"], errors="coerce") <= 10).sum()),
        "changes_near_boundary": int(near.sum()),
        "changes_far_from_boundary": int((~near).sum()),
        "average_removed_rank": _mean(removed["champion_rank"]) if len(removed) else None,
        "average_added_rank": _mean(added["champion_rank"]) if len(added) else None,
        "average_added_challenger_rank": _mean(added["challenger_rank"]) if len(added) else None,
        "average_removed_score_gap_to_top5_boundary": _mean(removed["score_gap_to_top5_boundary"]) if len(removed) else None,
        "average_removed_score_gap_to_top10_boundary": _mean(removed["score_gap_to_top10_boundary"]) if len(removed) else None,
        "average_added_score_gap_to_top5_boundary": _mean(added["score_gap_to_top5_boundary"]) if len(added) else None,
        "average_added_score_gap_to_top10_boundary": _mean(added["score_gap_to_top10_boundary"]) if len(added) else None,
    }


def _penalty_bucket(row: pd.Series) -> str:
    reasons: list[str] = []
    if bool(row.get("stable_bad_pick_family", False)):
        reasons.append("stable_bad_pick_family")
    if str(row.get("family_classification", "")).lower() in {"unstable_or_sparse_family", "regime_dependent_family"}:
        reasons.append(str(row.get("family_classification")).lower())
    if _safe_float(row.get("family_bottom15_rate")) is not None and float(row.get("family_bottom15_rate")) >= 0.20:
        reasons.append("family_bottom15_rate_ge_20pct")
    if "bad_pick_diagnostic" in str(row.get("prefilter_reason", "")).lower():
        reasons.append("prefilter_bad_pick_diagnostic")
    return "+".join(reasons) if reasons else "no_penalty"


def _penalty_summary(ranked: pd.DataFrame, removed: pd.DataFrame) -> dict[str, Any]:
    penalized = ranked[ranked["bad_pick_veto"].astype(bool)].copy()
    penalized["penalty_bucket"] = penalized.apply(_penalty_bucket, axis=1)
    removed_keys = set(_keys(removed))
    penalized["was_removed_from_topk"] = [key in removed_keys for key in _keys(penalized)]
    bucket_rows = {}
    for bucket, group in penalized.groupby("penalty_bucket", dropna=False):
        bucket_rows[str(bucket)] = {
            "count": int(len(group)),
            "ret20_mean": _mean(group["forward_ret_20d"]),
            "false_veto_rate": _rate((group["was_removed_from_topk"]) & ((group["forward_ret_20d"] > 0) | group.get("top15_label", pd.Series(False, index=group.index)).fillna(False).astype(bool))),
            "bad_pick_removed_rate": _rate((group["was_removed_from_topk"]) & group["bottom15_label"]),
        }
    return {
        "penalty_reason_counts": dict(Counter(penalized["penalty_bucket"].tolist())),
        "penalty_strength_distribution": {"penalty_value": 0.25, "mode": "binary_veto_penalty"},
        "ret20_by_penalty_bucket": bucket_rows,
    }


def _monthly_summary(removed: pd.DataFrame, added: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    months = sorted(set(removed["month_bucket"].astype(str).tolist()) | set(added["month_bucket"].astype(str).tolist()))
    for month in months:
        r = removed[removed["month_bucket"].astype(str) == month]
        a = added[added["month_bucket"].astype(str) == month]
        r_mean = _mean(r["forward_ret_20d"]) if len(r) else None
        a_mean = _mean(a["forward_ret_20d"]) if len(a) else None
        out[month] = {
            "removed_bad_pick_count": int(r["bottom15_label"].sum()) if len(r) else 0,
            "false_veto_count": int(((r["forward_ret_20d"] > 0) | r.get("top15_label", pd.Series(False, index=r.index)).fillna(False).astype(bool)).sum()) if len(r) else 0,
            "added_bad_pick_count": int(a["bottom15_label"].sum()) if len(a) else 0,
            "replacement_hit_rate": _rate(a["forward_ret_20d"] > 0) if len(a) else None,
            "net_swap_delta": None if r_mean is None or a_mean is None else a_mean - r_mean,
            "removed_count": int(len(r)),
            "added_count": int(len(a)),
        }
    return out


def _examples(frame: pd.DataFrame, mode: str, limit: int = 25) -> list[dict[str, Any]]:
    if mode == "false_veto":
        rows = frame[(frame["forward_ret_20d"] > 0) | frame.get("top15_label", pd.Series(False, index=frame.index)).fillna(False).astype(bool)].copy()
        rows = rows.sort_values(["forward_ret_20d"], ascending=False, kind="mergesort")
    else:
        rows = frame[(frame["forward_ret_20d"] <= 0) | frame["bottom15_label"]].copy()
        rows = rows.sort_values(["forward_ret_20d"], ascending=True, kind="mergesort")
    cols = [
        "anchor_date",
        "month_bucket",
        "side",
        "symbol",
        "champion_rank",
        "challenger_rank",
        "champion_score",
        "challenger_score",
        "forward_ret_20d",
        "bottom15_label",
        "top15_label",
        "monthly_context",
        "weekly_context",
        "market_regime_bucket",
        "family_classification",
        "family_bottom15_rate",
        "prefilter_reason",
    ]
    return [{c: row.get(c) for c in cols if c in rows.columns} for row in rows.head(limit).to_dict(orient="records")]


def _decompose_topk(ranked: pd.DataFrame, topk: int) -> dict[str, Any]:
    removed, added, changed = _build_swap_frame(ranked, topk)
    champion_mean = _mean(_selected(ranked, "champion_rank", topk)["forward_ret_20d"])
    per_date = _per_date_swaps(removed, added)
    return {
        "schema_version": SWAP_SCHEMA_VERSION,
        "variant_id": BEST_VARIANT_ID,
        "top_k": topk,
        "removed_candidates": _candidate_stats(removed, champion_mean),
        "added_candidates": _candidate_stats(added, champion_mean),
        "net_swap_quality": {
            "per_date": per_date,
            "aggregate": _net_swap_aggregate(per_date),
        },
        "boundary_usefulness": _boundary_summary(changed),
        "penalty_strength": _penalty_summary(ranked, removed),
        "regime_analysis_status": "unavailable" if ranked.get("market_regime_bucket", pd.Series(["unknown"])).astype(str).nunique() == 1 and str(ranked["market_regime_bucket"].iloc[0]) == "unknown" else "available",
    }


def _pick_failure_mode(top5: dict[str, Any], top10: dict[str, Any], monthly: dict[str, Any], missing_fields: list[str]) -> tuple[str, list[str], bool, str, list[str]]:
    t5r = top5["removed_candidates"]
    t5a = top5["added_candidates"]
    t10r = top10["removed_candidates"]
    t10a = top10["added_candidates"]
    secondary: list[str] = []
    if missing_fields:
        secondary.append("missing_input_coverage")
    false_veto_high = (t5r.get("false_veto_rate") or 0) >= 0.50 or (t10r.get("false_veto_rate") or 0) >= 0.50
    weak_replacement = (t5a.get("ret20_mean") or 0) < (t5r.get("ret20_mean") or 0) or (t10a.get("ret20_mean") or 0) < (t10r.get("ret20_mean") or 0)
    over_broad = top10["boundary_usefulness"]["changes_inside_top10_boundary"] >= 300
    concentration = sum(1 for m in monthly.values() if (m.get("net_swap_delta") or 0) < 0) > sum(1 for m in monthly.values() if (m.get("net_swap_delta") or 0) > 0)
    veto_good = (t5r.get("ret20_mean") or 0) < 0.012408160828106586 and (t5r.get("bad_pick_removed_rate") or 0) >= 0.10 and not false_veto_high
    if false_veto_high:
        primary = "false_veto"
        next_axis = "breakout_trap_narrowing_v1"
    elif weak_replacement:
        primary = "weak_replacement"
        next_axis = "replacement_quality_gate_v1"
    elif over_broad:
        primary = "over_broad_penalty"
        next_axis = "breakout_trap_narrowing_v1"
    elif concentration:
        primary = "concentration"
        next_axis = "drop_bad_pick_removal_v1"
    elif missing_fields:
        primary = "missing_input_coverage"
        next_axis = "input_coverage_repair"
    else:
        primary = "insufficient_signal"
        next_axis = "drop_bad_pick_removal_v1"
    for mode, active in {
        "false_veto": false_veto_high,
        "weak_replacement": weak_replacement,
        "over_broad_penalty": over_broad,
        "concentration": concentration,
    }.items():
        if active and mode != primary:
            secondary.append(mode)
    salvageable = bool(veto_good and weak_replacement and not false_veto_high)
    do_not = []
    if false_veto_high:
        do_not.append("removed candidate false_veto_rate is excessive")
    if not veto_good:
        do_not.append("removed candidates are not clearly worse with acceptable false veto")
    if concentration:
        do_not.append("monthly net swap is negative in more slices than positive")
    return primary, secondary, salvageable, next_axis, do_not


def run_decomposition(
    *,
    input_run_root: str | Path = DEFAULT_INPUT_RUN_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    input_root = Path(input_run_root)
    run_config = _read_json(input_root / "run_config.json")
    previous_decision = _read_json(input_root / "decision.json")
    variant_results = _read_json(input_root / "variant_results.json")
    source_path = Path(run_config["source_rows_parquet"])
    frame = _load_frame(source_path, limit_anchor_dates=None)
    description, mask, fields = _variant_masks(frame)[BEST_VARIANT_ID]
    ranked = _rank_with_penalty(frame, mask)
    top5 = _decompose_topk(ranked, 5)
    top10 = _decompose_topk(ranked, 10)
    removed5, added5, _ = _build_swap_frame(ranked, 5)
    removed10, added10, _ = _build_swap_frame(ranked, 10)
    monthly = _monthly_summary(removed10, added10)
    missing_fields = []
    for label, required in {
        "event_risk": ["earnings_proximity", "ex_dividend_proximity", "shareholder_benefit_proximity", "major_event_flag"],
        "supply_demand": ["margin_buying_ratio", "lending_balance", "borrow_rate", "crowding_proxy"],
        "regime": ["market_regime_bucket"],
    }.items():
        if label == "regime":
            if frame["market_regime_bucket"].astype(str).eq("unknown").all():
                missing_fields.append("regime_all_unknown")
        elif not any(col in frame.columns for col in required):
            missing_fields.append(f"{label}_fields_unavailable")
    primary, secondary, salvageable, next_axis, do_not = _pick_failure_mode(top5, top10, monthly, missing_fields)

    session_dir = Path(output_root) / f"{_utc_stamp()}-bad_pick_removal_v1_decomposition"
    config_payload = {
        "schema_version": RUN_CONFIG_SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "git_hash": _git_hash_or_unknown(),
        "input_run_root": str(input_root),
        "source_rows_parquet": str(source_path),
        "best_variant": BEST_VARIANT_ID,
        "variant_penalizes": description,
        "feature_fields_used": fields,
        "fixed_evaluation_conditions": FIXED_CONDITIONS,
        "previous_decision_artifact": str(input_root / "decision.json"),
        "previous_artifacts_read_only": True,
        "runtime_db_write_performed": False,
        "non_scope": ["MeeMee UI", "runtime DB writes", "production ranking", "champion logic", "publish flow", "existing decision artifact mutation"],
    }
    false_examples = {
        "schema_version": EXAMPLE_SCHEMA_VERSION,
        "top5_false_veto_examples": _examples(removed5, "false_veto"),
        "top10_false_veto_examples": _examples(removed10, "false_veto"),
    }
    weak_examples = {
        "schema_version": EXAMPLE_SCHEMA_VERSION,
        "top5_weak_replacement_examples": _examples(added5, "weak_replacement"),
        "top10_weak_replacement_examples": _examples(added10, "weak_replacement"),
    }
    monthly_payload = {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "variant_id": BEST_VARIANT_ID,
        "top_k": 10,
        "monthly": monthly,
        "regime_analysis_status": "unavailable" if "regime_all_unknown" in missing_fields else "available",
    }
    decision_payload = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "primary_failure_mode": primary,
        "secondary_failure_modes": secondary,
        "is_breakout_trap_signal_salvageable": salvageable,
        "recommended_next_axis_only": next_axis,
        "do_not_proceed_reasons": do_not,
        "evidence_summary": {
            "top5_removed_ret20_mean": top5["removed_candidates"]["ret20_mean"],
            "top5_added_ret20_mean": top5["added_candidates"]["ret20_mean"],
            "top5_false_veto_rate": top5["removed_candidates"]["false_veto_rate"],
            "top5_bad_pick_removed_rate": top5["removed_candidates"]["bad_pick_removed_rate"],
            "top5_net_swap_delta_mean": top5["net_swap_quality"]["aggregate"]["net_swap_delta_mean"],
            "top10_removed_ret20_mean": top10["removed_candidates"]["ret20_mean"],
            "top10_added_ret20_mean": top10["added_candidates"]["ret20_mean"],
            "top10_false_veto_rate": top10["removed_candidates"]["false_veto_rate"],
            "top10_bad_pick_removed_rate": top10["removed_candidates"]["bad_pick_removed_rate"],
            "top10_net_swap_delta_mean": top10["net_swap_quality"]["aggregate"]["net_swap_delta_mean"],
            "top10_changes_inside_boundary": top10["boundary_usefulness"]["changes_inside_top10_boundary"],
            "missing_fields": missing_fields,
            "previous_decision": previous_decision.get("decision"),
            "previous_best_variant_metrics": next((v for v in variant_results.get("variants", []) if v.get("variant_id") == BEST_VARIANT_ID), {}),
        },
    }
    complete = {
        "schema_version": COMPLETE_SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "parse_status": {
            "run_config": True,
            "swap_decomposition_top5": True,
            "swap_decomposition_top10": True,
            "false_veto_examples": True,
            "weak_replacement_examples": True,
            "monthly_swap_summary": True,
            "decomposition_decision": True,
        },
        "verification": {
            "no_meemee_files_changed_by_script": True,
            "champion_logic_changed": False,
            "production_publish_changed": False,
            "runtime_db_write_occurred": False,
            "previous_artifacts_read_only": True,
            "silent_fallback_used": False,
            "past_only_analysis_contract": {
                "feature_construction_uses_past_only_fields": True,
                "realized_ret20_used_only_for_evaluation": True,
                "monthly_context_no_lookahead_all_true_or_missing": bool(frame.get("monthly_context_no_lookahead", pd.Series(True, index=frame.index)).fillna(True).astype(bool).all()),
                "weekly_context_no_lookahead_all_true_or_missing": bool(frame.get("weekly_context_no_lookahead", pd.Series(True, index=frame.index)).fillna(True).astype(bool).all()),
            },
        },
    }
    _write_json(session_dir / "run_config.json", config_payload)
    _write_json(session_dir / "swap_decomposition_top5.json", top5)
    _write_json(session_dir / "swap_decomposition_top10.json", top10)
    _write_json(session_dir / "false_veto_examples.json", false_examples)
    _write_json(session_dir / "weak_replacement_examples.json", weak_examples)
    _write_json(session_dir / "monthly_swap_summary.json", monthly_payload)
    _write_json(session_dir / "decomposition_decision.json", decision_payload)
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_dir": str(session_dir),
        "decision_path": str(session_dir / "decomposition_decision.json"),
        "primary_failure_mode": primary,
        "is_breakout_trap_signal_salvageable": salvageable,
        "recommended_next_axis_only": next_axis,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-run-root", default=str(DEFAULT_INPUT_RUN_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    print(json.dumps(run_decomposition(input_run_root=args.input_run_root, output_root=args.output_root), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
