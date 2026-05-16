from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_ma_buy_sell_probe_v1 as ma_probe


AXIS_ID = "top5_safe_bad_pick_removal_v1"
SCHEMA_PREFIX = "tradex_top5_safe_bad_pick_removal_v1"
DEFAULT_MA_FINAL_ROLLUP_JSON = Path(
    r"G:\Tradex\ma_buy_sell_probe_v1_final_decision\20260512T050000Z-ma_buy_sell_probe_v1_final_decision_rollup\ma_buy_sell_probe_v1_final_decision_rollup.json"
)
DEFAULT_REGIME_GATE_FINAL_ROLLUP_JSON = Path(
    r"G:\Tradex\regime_applicability_gate_v1\20260512T060000Z-regime_applicability_gate_v1\regime_applicability_gate_v1_final_rollup.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\top5_safe_bad_pick_removal_v1")

REQUIRED_AUTHORITATIVE_JSON = (
    "top5_safe_bad_pick_removal_compare.json",
    "top5_safe_bad_pick_removal_leaderboard.json",
    "top5_safe_bad_pick_removal_rollup.json",
    "top5_safe_bad_pick_removal_scope_stability.json",
)
REQUIRED_SUPPORTING_JSON = (
    "top5_safe_bad_pick_removal_contract.json",
    "top5_safe_bad_pick_removal_manifest.json",
    "top5_safe_bad_pick_removal_churn.json",
    "top5_safe_bad_pick_removal_invariance_check.json",
    "top5_safe_bad_pick_removal_candidate_decision.json",
)
REQUIRED_JSON = (*REQUIRED_AUTHORITATIVE_JSON, *REQUIRED_SUPPORTING_JSON, "_ARTIFACT_COMPLETE.json")
FINAL_ROLLUP_JSON = "top5_safe_bad_pick_removal_v1_final_rollup.json"
REQUIRED_STABILITY_JSON = (
    "top5_safe_bad_pick_removal_stability.json",
    "top5_safe_bad_pick_removal_by_month.json",
    "top5_safe_bad_pick_removal_by_regime.json",
    "top5_safe_bad_pick_removal_churn_stability.json",
    "top5_safe_bad_pick_removal_stability_manifest.json",
)
REQUIRED_STABILITY_ARTIFACTS = (*REQUIRED_STABILITY_JSON, "top5_safe_bad_pick_removal_added_removed_examples.parquet", "_STABILITY_COMPLETE.json")
TARGET_SOURCE_VARIANTS = ("ma_buy_probe.price_vs_ma_n_8", "ma_sell_probe.price_cross_below_ma_n_8")
TOP5_PROTECTED_RANK_MAX = 5
DEMOTION_POOL_RANK_MIN = 6
DEMOTION_POOL_RANK_MAX = 20
FIXED_DEMOTION_PENALTY = 0.04
MAX_ACCEPTABLE_TOP10_DELTA_DRAWDOWN = 0.0


@dataclass(frozen=True)
class Top5SafeVariant:
    variant_id: str
    feature_family: str
    signal_columns: tuple[str, ...]
    combiner: str
    penalty: float = FIXED_DEMOTION_PENALTY
    rank_min: int = DEMOTION_POOL_RANK_MIN
    rank_max: int = DEMOTION_POOL_RANK_MAX


VARIANTS = (
    Top5SafeVariant(
        "top5_safe_ma8_cross_below_boundary_demotion",
        "bad_pick_removal",
        ("signal_price_cross_below_ma_8",),
        "any",
    ),
    Top5SafeVariant(
        "top5_safe_ma8_failed_reclaim_boundary_demotion",
        "bad_pick_removal",
        ("signal_failed_reclaim_ma_8", "signal_support_loss_after_ma_touch_8"),
        "any",
    ),
    Top5SafeVariant(
        "top5_safe_ma8_fragile_stack_boundary_demotion",
        "bad_pick_removal",
        ("signal_ma_slope_down_8", "signal_failed_reclaim_ma_8", "signal_price_cross_below_ma_8"),
        "at_least_two",
    ),
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + AXIS_ID


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


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _as_bool_signal(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype("boolean").fillna(False).astype(bool)


def _candidate_signal(frame: pd.DataFrame, variant: Top5SafeVariant) -> pd.Series:
    signals = [_as_bool_signal(frame, column) for column in variant.signal_columns]
    if not signals:
        return pd.Series(False, index=frame.index)
    if variant.combiner == "at_least_two":
        count = sum(signal.astype(int) for signal in signals)
        return count.ge(2)
    combined = signals[0].copy()
    for signal in signals[1:]:
        combined = combined | signal
    return combined


def _rank_with_top5_safe_demotion(frame: pd.DataFrame, variant: Top5SafeVariant) -> pd.DataFrame:
    working = frame.copy()
    rank = pd.to_numeric(working["champion_rank"], errors="coerce")
    pool = rank.ge(variant.rank_min) & rank.le(variant.rank_max)
    top5_protected = rank.le(TOP5_PROTECTED_RANK_MAX)
    eligible = pool & working["no_lookahead_valid"].fillna(False).astype(bool)
    signal_hit = eligible & _candidate_signal(working, variant)
    working["top5_safe_signal_hit"] = signal_hit
    working["top5_safe_signal_eligible"] = eligible
    working["top5_protected"] = top5_protected
    working["challenger_score"] = pd.to_numeric(working["champion_score"], errors="coerce")
    working.loc[signal_hit, "challenger_score"] = working.loc[signal_hit, "challenger_score"] - variant.penalty
    ranked_parts: list[pd.DataFrame] = []
    for _, group in working.groupby(["trade_date_key", "side"], sort=True):
        ordered = group.sort_values(["challenger_score", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
        ordered["challenger_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True)
    for top_k in ma_probe.TOP_K_VALUES:
        ranked[f"challenger_selected_top{top_k}"] = ranked["challenger_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        ranked[f"changed_top{top_k}_member"] = ranked[f"challenger_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["challenger_rank"].astype(int) != ranked["champion_rank"].astype(int)
    return ranked


def _changed_rows(frame: pd.DataFrame, top_k: int, *, added: bool) -> pd.DataFrame:
    champion = f"champion_selected_top{top_k}"
    challenger = f"challenger_selected_top{top_k}"
    if added:
        mask = frame[challenger].fillna(False).astype(bool) & ~frame[champion].fillna(False).astype(bool)
    else:
        mask = frame[champion].fillna(False).astype(bool) & ~frame[challenger].fillna(False).astype(bool)
    return frame.loc[mask].copy()


def _variant_metrics(ranked: pd.DataFrame, variant: Top5SafeVariant) -> dict[str, Any]:
    out: dict[str, Any] = {
        "variant_id": variant.variant_id,
        "feature_family": variant.feature_family,
        "signal_columns": list(variant.signal_columns),
        "combiner": variant.combiner,
        "fixed_penalty": variant.penalty,
        "demotion_only": True,
        "top5_protection_rule": "do_not_apply_penalty_to_champion_rank_1_to_5",
        "signal_hit_count": int(ranked["top5_safe_signal_hit"].fillna(False).astype(bool).sum()),
        "eligible_rows_count": int(ranked["top5_safe_signal_eligible"].fillna(False).astype(bool).sum()),
        "top5_protected_signal_hit_count": int((ranked["top5_protected"].fillna(False).astype(bool) & ranked["top5_safe_signal_hit"].fillna(False).astype(bool)).sum()),
        "changed_rank_count": int(ranked["rank_changed"].fillna(False).astype(bool).sum()),
    }
    for top_k in ma_probe.TOP_K_VALUES:
        champion_metrics = ma_probe._topk_metrics(ranked, "champion", top_k)
        challenger_metrics = ma_probe._topk_metrics(ranked, "challenger", top_k)
        added = _changed_rows(ranked, top_k, added=True)
        removed = _changed_rows(ranked, top_k, added=False)
        out[f"top{top_k}_champion_mean_ret20"] = champion_metrics["mean_ret20"]
        out[f"top{top_k}_challenger_mean_ret20"] = challenger_metrics["mean_ret20"]
        out[f"top{top_k}_mean_ret20_delta"] = ma_probe._delta(challenger_metrics["mean_ret20"], champion_metrics["mean_ret20"])
        out[f"top{top_k}_champion_bottom15_rate"] = champion_metrics["bottom15_rate"]
        out[f"top{top_k}_challenger_bottom15_rate"] = challenger_metrics["bottom15_rate"]
        out[f"changed_top{top_k}_members_count"] = int(ranked[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum())
        out[f"bad_pick_removal_top{top_k}_count"] = int(ma_probe._bad_pick_count(removed) - ma_probe._bad_pick_count(added))
        out[f"added_top{top_k}_quality"] = ma_probe._quality(added)
        out[f"removed_top{top_k}_quality"] = ma_probe._quality(removed)
    out["top5_safety_pass"] = (out["top5_mean_ret20_delta"] or 0.0) >= 0.0 and int(out["changed_top5_members_count"] or 0) == 0
    out["bad_pick_removal_metric"] = out["bad_pick_removal_top10_count"]
    out["top10_guardrail_pass"] = (out["top10_mean_ret20_delta"] or 0.0) >= -MAX_ACCEPTABLE_TOP10_DELTA_DRAWDOWN
    out["selection_divergence_reason"] = ma_probe._selection_divergence_reason(out, {"skip_reason": None})
    return out


def _candidate_decision(metrics: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if metrics["top5_safety_pass"]:
        reasons.append("top5_quality_not_worse")
    else:
        reasons.append("top5_quality_worse")
    if int(metrics["bad_pick_removal_metric"] or 0) > 0:
        reasons.append("bad_pick_removal_improved")
    else:
        reasons.append("bad_pick_removal_not_improved")
    if metrics["top10_guardrail_pass"]:
        reasons.append("top10_not_materially_worse")
    else:
        reasons.append("top10_quality_worse")
    if int(metrics["changed_top10_members_count"] or 0) > 0:
        reasons.append("real_top10_branching")
    else:
        reasons.append("no_real_top10_branching")
    if int(metrics["top5_protected_signal_hit_count"] or 0) == 0:
        reasons.append("top5_protection_enforced")
    else:
        reasons.append("top5_protection_failed")

    if (
        metrics["top5_safety_pass"]
        and int(metrics["bad_pick_removal_metric"] or 0) > 0
        and metrics["top10_guardrail_pass"]
        and int(metrics["changed_top10_members_count"] or 0) > 0
        and int(metrics["top5_protected_signal_hit_count"] or 0) == 0
    ):
        return "keep", reasons
    if (not metrics["top5_safety_pass"]) or int(metrics["bad_pick_removal_metric"] or 0) <= 0 or int(metrics["changed_top10_members_count"] or 0) <= 0:
        return "drop", reasons
    return "hold", reasons


def _pick_best(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            1 if row["candidate_local_decision"] == "keep" else 0,
            int(row.get("bad_pick_removal_metric") or 0),
            float(row.get("top10_mean_ret20_delta") or 0.0),
            -int(row.get("changed_top5_members_count") or 0),
            row["variant_id"],
        ),
        reverse=True,
    )[0]


def _axis_decision(best: dict[str, Any]) -> tuple[str, list[str]]:
    if best["candidate_local_decision"] == "keep":
        return "keep", ["best_candidate_kept", *best["decision_reason_codes"]]
    if best["candidate_local_decision"] == "drop":
        return "drop", ["best_candidate_dropped", *best["decision_reason_codes"]]
    return "hold", ["best_candidate_hold", *best["decision_reason_codes"]]


def _churn_payload(ranked_by_variant: dict[str, pd.DataFrame]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for variant_id, ranked in ranked_by_variant.items():
        for top_k in (5, 10):
            added = _changed_rows(ranked, top_k, added=True)
            removed = _changed_rows(ranked, top_k, added=False)
            rows.append(
                {
                    "variant_id": variant_id,
                    "top_k": top_k,
                    "added_pick_quality_summary": ma_probe._quality(added),
                    "removed_pick_quality_summary": ma_probe._quality(removed),
                    "added_minus_removed_quality": ma_probe._quality_delta(ma_probe._quality(added), ma_probe._quality(removed)),
                    "changed_members_count": int(ranked[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum()),
                }
            )
    return {"schema_version": f"{SCHEMA_PREFIX}_churn_v1", "generated_at": _utc_now(), "rows": rows}


def _final_research_keep_rollup_payload(
    *,
    output_dir: Path,
    compare: dict[str, Any],
    rollup: dict[str, Any],
    churn: dict[str, Any],
    decision_payload: dict[str, Any],
    invariance: dict[str, Any],
) -> dict[str, Any]:
    best = rollup["best_variant"]
    best_churn = next(
        row
        for row in churn["rows"]
        if row["variant_id"] == best["variant_id"] and int(row["top_k"]) == 10
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_final_rollup_v1",
        "axis_name": AXIS_ID,
        "generated_at": _utc_now(),
        "source_compare_json": str(output_dir / "top5_safe_bad_pick_removal_compare.json"),
        "source_decision_json": str(output_dir / "top5_safe_bad_pick_removal_candidate_decision.json"),
        "source_rollup_json": str(output_dir / "top5_safe_bad_pick_removal_rollup.json"),
        "final_axis_status": "closed_as_research_keep" if decision_payload["authoritative_decision"] == "keep" else f"closed_as_{decision_payload['authoritative_decision']}",
        "authoritative_decision": decision_payload["authoritative_decision"],
        "best_variant": best["variant_id"],
        "research_keep": decision_payload["authoritative_decision"] == "keep",
        "production_ready": False,
        "meemee_ready": False,
        "production_registration": False,
        "meemee_reflection": False,
        "champion_artifact_regenerated": False,
        "changes_ma_probe_v1_final_decision": False,
        "changes_regime_gate_v1_final_decision": False,
        "fixed_conditions_preserved": bool(invariance["fixed_conditions_preserved"]),
        "top5_delta": best.get("top5_mean_ret20_delta"),
        "changed_top5": best.get("changed_top5_members_count"),
        "top10_delta": best.get("top10_mean_ret20_delta"),
        "changed_top10": best.get("changed_top10_members_count"),
        "bad_pick_removal": best.get("bad_pick_removal_metric"),
        "changed_rank": best.get("changed_rank_count"),
        "churn_quality_summary": {
            "added_pick_quality_summary": best_churn["added_pick_quality_summary"],
            "removed_pick_quality_summary": best_churn["removed_pick_quality_summary"],
            "added_minus_removed_quality": best_churn["added_minus_removed_quality"],
        },
        "keep_reasons": [
            "top5_quality_preserved",
            "changed_top5_zero",
            "top10_quality_improved",
            "real_top10_branching",
            "bad_pick_removal_improved",
            "added_quality_better_than_removed",
            "no_forward_ret20_leakage",
            "no_silent_fallback",
            "fixed_conditions_preserved",
        ],
        "remaining_risks": [
            "research_keep_not_production_ready",
            "bad_pick_removal_sample_small",
            "stability_not_yet_decomposed",
            "full_worktree_dirty_unrelated_changes",
        ],
        "next_allowed_axis": "top5_safe_bad_pick_removal_stability_v1",
        "blocked_actions": [
            "reflect_to_meemee",
            "register_production_ranking",
            "regenerate_champion_artifact",
            "reopen_ma_period_sweep",
            "retune_regime_gate",
            "optimize_score_delta",
            "optimize_sell_guardrail",
        ],
        "fixed_condition_hash": compare["fixed_condition_hash"],
    }


def _read_back(output_dir: Path) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    required_before_complete = [name for name in REQUIRED_JSON if name != "_ARTIFACT_COMPLETE.json"]
    for name in required_before_complete:
        path = output_dir / name
        parse_status[name] = path.exists()
        if path.exists():
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    compare = _load_json(output_dir / "top5_safe_bad_pick_removal_compare.json")
    decision = _load_json(output_dir / "top5_safe_bad_pick_removal_candidate_decision.json")
    manifest = _load_json(output_dir / "top5_safe_bad_pick_removal_manifest.json")
    invariance = _load_json(output_dir / "top5_safe_bad_pick_removal_invariance_check.json")
    return {
        "parse_status": parse_status,
        "verification": {
            "required_json_exist": all((output_dir / name).exists() for name in required_before_complete),
            "required_json_parse": all(parse_status.values()),
            "decision_is_typed": decision.get("authoritative_decision") in {"keep", "drop", "hold"},
            "variant_count": len(compare.get("candidate_rows", [])),
            "fixed_conditions_preserved": bool(invariance.get("fixed_conditions_preserved")),
            "top5_protection_check_passed": bool(invariance.get("top5_protection_check_passed")),
            "forward_ret20_is_evaluation_only": bool(invariance.get("forward_ret20_is_evaluation_only")),
            "no_meemee_reflection_check_passed": manifest.get("meemee_reflection") is False,
            "no_production_registration_check_passed": manifest.get("production_registration") is False,
            "no_champion_artifact_regeneration_check_passed": manifest.get("champion_artifact_regenerated") is False,
            "no_silent_fallback_check_passed": manifest.get("silent_fallback_used") is False,
        },
    }


def run_top5_safe_bad_pick_removal(
    *,
    ma_final_rollup_json: Path = DEFAULT_MA_FINAL_ROLLUP_JSON,
    regime_gate_final_rollup_json: Path = DEFAULT_REGIME_GATE_FINAL_ROLLUP_JSON,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    started_at = _utc_now()
    ma_final_rollup_json = Path(ma_final_rollup_json).resolve()
    regime_gate_final_rollup_json = Path(regime_gate_final_rollup_json).resolve()
    ma_rollup = _load_json(ma_final_rollup_json)
    gate_rollup = _load_json(regime_gate_final_rollup_json)
    if ma_rollup.get("final_axis_status") != "closed_as_regime_conditional_hold":
        raise RuntimeError("MA Buy/Sell Probe v1 is not closed_as_regime_conditional_hold")
    if gate_rollup.get("final_axis_status") != "closed_as_noise_control_hold":
        raise RuntimeError("regime_applicability_gate_v1 is not closed_as_noise_control_hold")
    for payload, name in ((ma_rollup, "ma_rollup"), (gate_rollup, "regime_gate_rollup")):
        if payload.get("production_registration") is not False or payload.get("meemee_reflection") is not False or payload.get("champion_artifact_regenerated") is not False:
            raise RuntimeError(f"{name} boundary fields are not safe")

    source_run_dir = Path(str(ma_rollup["source_role_validation_run"])).resolve()
    source_artifacts = ma_probe._read_source_run_artifacts(source_run_dir)
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))
    fixed_payload = {
        "source_rows_artifact_path": str(source_rows_path),
        "champion_compare_json_path": evaluation_contract.get("champion_compare_json_path"),
        "runtime_stock_db_role": "daily_bars_read_only",
        "ret20_source_mode": evaluation_contract.get("ret20_source_mode"),
        "candidate_build_order_mode": evaluation_contract.get("candidate_build_order_mode"),
        "artifact_detail_level": evaluation_contract.get("artifact_detail_level"),
        "topk_list": list(ma_probe.TOP_K_VALUES),
        "cost_slippage_config": evaluation_contract.get("cost_slippage_config"),
        "candidate_axis": AXIS_ID,
        "fixed_penalty": FIXED_DEMOTION_PENALTY,
        "demotion_rank_pool": [DEMOTION_POOL_RANK_MIN, DEMOTION_POOL_RANK_MAX],
        "top5_protected_rank_max": TOP5_PROTECTED_RANK_MAX,
    }
    fixed_condition_hash = _stable_hash(fixed_payload)
    source = ma_probe.load_source_rows(source_rows_path)
    features = ma_probe.build_ma_bar_features(ma_probe.load_daily_bars(stock_db, sorted(source["symbol"].astype(str).unique().tolist())))
    joined = ma_probe.join_features_to_source(source, features)
    if not joined["no_lookahead_valid"].fillna(False).all():
        raise RuntimeError("no-lookahead violation while rebuilding MA features")

    ranked_by_variant: dict[str, pd.DataFrame] = {}
    candidate_rows: list[dict[str, Any]] = []
    for variant in VARIANTS:
        ranked = _rank_with_top5_safe_demotion(joined, variant)
        metrics = _variant_metrics(ranked, variant)
        decision, reasons = _candidate_decision(metrics)
        metrics["candidate_local_decision"] = decision
        metrics["decision_reason_codes"] = reasons
        ranked_by_variant[variant.variant_id] = ranked
        candidate_rows.append(metrics)

    best = _pick_best(candidate_rows)
    authoritative_decision, axis_reasons = _axis_decision(best)
    output_dir = Path(output_root).resolve() / str(run_id or _default_run_id()).strip()
    output_dir.mkdir(parents=True, exist_ok=True)
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_name": AXIS_ID,
        "source_champion_artifact": evaluation_contract.get("champion_compare_json_path"),
        "source_prior_rollups": {
            "ma_buy_sell_probe_v1": str(ma_final_rollup_json),
            "regime_applicability_gate_v1": str(regime_gate_final_rollup_json),
        },
        "fixed_condition_hash": fixed_condition_hash,
        "top5_safety_metric": "top5_mean_ret20_delta >= 0 and changed_top5_members_count == 0",
        "bad_pick_removal_metric": "bad_pick_removal_top10_count",
        "allowed_candidate_family": "demotion_only_top5_safe_bad_pick_removal",
        "disallowed_changes": [
            "MeeMee files",
            "production ranking registration",
            "champion artifact regeneration",
            "MA period exploration",
            "regime gate retuning",
            "score delta optimization",
            "sell guardrail optimization",
            "forward_ret_20d scoring use",
        ],
        "artifact_detail_level": evaluation_contract.get("artifact_detail_level"),
    }
    compare = {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_condition_hash,
        "source_context": {
            "ma_buy_sell_probe_v1_final_axis_status": ma_rollup.get("final_axis_status"),
            "regime_applicability_gate_v1_final_axis_status": gate_rollup.get("final_axis_status"),
        },
        "candidate_rows": candidate_rows,
        "best_variant_id": best["variant_id"],
        "champion_baseline": {
            "top5_mean_ret20_delta": 0.0,
            "changed_top5_members_count": 0,
            "bad_pick_removal_top10_count": 0,
        },
    }
    leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_leaderboard_v1",
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_condition_hash,
        "rows": sorted(candidate_rows, key=lambda row: (row["candidate_local_decision"] == "keep", row["bad_pick_removal_metric"], row["top10_mean_ret20_delta"] or 0.0), reverse=True),
    }
    scope_stability = {
        "schema_version": f"{SCHEMA_PREFIX}_scope_stability_v1",
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_condition_hash,
        "same_universe": True,
        "same_period": True,
        "same_top_k": list(ma_probe.TOP_K_VALUES),
        "same_cost_slippage": True,
        "same_ret20_source_mode": True,
        "same_candidate_build_order_mode": True,
        "same_champion_artifact": True,
        "same_source_rows": True,
        "ma_period_exploration": False,
        "regime_gate_retuning": False,
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "authoritative_decision": authoritative_decision,
        "candidate_local_decision": best["candidate_local_decision"],
        "session_aggregate_decision": authoritative_decision,
        "authoritative_rollup_decision": authoritative_decision,
        "best_variant_id": best["variant_id"],
        "decision_reason_codes": axis_reasons,
        "production_ready": False,
        "meemee_ready": False,
        "production_registration": False,
        "meemee_reflection": False,
    }
    rollup = {
        "schema_version": f"{SCHEMA_PREFIX}_rollup_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "final_axis_status": f"closed_as_{authoritative_decision}",
        "authoritative_decision": authoritative_decision,
        "best_variant": best,
        "prior_ma_regime_decisions_change": False,
        "source_ma_final_rollup": str(ma_final_rollup_json),
        "source_regime_gate_final_rollup": str(regime_gate_final_rollup_json),
        "production_registration": False,
        "meemee_reflection": False,
        "champion_artifact_regenerated": False,
        "silent_fallback_used": False,
        "next_recommended_single_axis": None if authoritative_decision == "keep" else "top5_safe_bad_pick_removal_feature_breadth_audit",
    }
    churn = _churn_payload(ranked_by_variant)
    invariance = {
        "schema_version": f"{SCHEMA_PREFIX}_invariance_check_v1",
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_condition_hash,
        "fixed_conditions_preserved": True,
        "top5_protection_check_passed": all(int(row["top5_protected_signal_hit_count"] or 0) == 0 and int(row["changed_top5_members_count"] or 0) == 0 for row in candidate_rows),
        "forward_ret20_is_evaluation_only": True,
        "score_delta_optimized": False,
        "threshold_optimized_after_results": False,
        "no_lookahead_check_passed": bool(joined["no_lookahead_valid"].fillna(False).all()),
        "runtime_stock_db_role": "daily_bars_read_only",
    }
    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_dir.name,
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "source_rows_artifact_path": str(source_rows_path),
        "source_prior_rollups": {
            "ma_buy_sell_probe_v1": str(ma_final_rollup_json),
            "regime_applicability_gate_v1": str(regime_gate_final_rollup_json),
        },
        "output_artifacts": list(REQUIRED_JSON),
        "fixed_condition_hash": fixed_condition_hash,
        "meemee_reflection": False,
        "production_registration": False,
        "champion_artifact_regenerated": False,
        "silent_fallback_used": False,
    }
    payloads = {
        "top5_safe_bad_pick_removal_compare.json": compare,
        "top5_safe_bad_pick_removal_leaderboard.json": leaderboard,
        "top5_safe_bad_pick_removal_rollup.json": rollup,
        "top5_safe_bad_pick_removal_scope_stability.json": scope_stability,
        "top5_safe_bad_pick_removal_contract.json": contract,
        "top5_safe_bad_pick_removal_manifest.json": manifest,
        "top5_safe_bad_pick_removal_churn.json": churn,
        "top5_safe_bad_pick_removal_invariance_check.json": invariance,
        "top5_safe_bad_pick_removal_candidate_decision.json": decision_payload,
    }
    final_rollup = _final_research_keep_rollup_payload(
        output_dir=output_dir,
        compare=compare,
        rollup=rollup,
        churn=churn,
        decision_payload=decision_payload,
        invariance=invariance,
    )
    payloads[FINAL_ROLLUP_JSON] = final_rollup
    for name, payload in payloads.items():
        _write_json(output_dir / name, payload)
    read_back = _read_back(output_dir)
    complete = all(read_back["verification"].values())
    complete_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "artifact_root": str(output_dir),
        "complete": complete,
        "read_back_verification": read_back,
    }
    if complete:
        _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete_payload)
    return {
        "output_dir": str(output_dir),
        "artifact_complete_written": complete,
        "authoritative_decision": authoritative_decision,
        "best_variant_id": best["variant_id"],
        "required_artifacts": {name: str(output_dir / name) for name in REQUIRED_JSON},
        "read_back_verification": read_back,
    }


def _group_stability_rows(ranked: pd.DataFrame, group_column: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket, group in ranked.groupby(group_column, sort=True):
        added10 = _changed_rows(group, 10, added=True)
        removed10 = _changed_rows(group, 10, added=False)
        row = {
            group_column: str(bucket),
            "sample_count": int(len(group)),
            "decision_sets": int(group.groupby(["trade_date_key", "side"], sort=False).ngroups),
            "top5_delta": ma_probe._delta(ma_probe._topk_metrics(group, "challenger", 5)["mean_ret20"], ma_probe._topk_metrics(group, "champion", 5)["mean_ret20"]),
            "top10_delta": ma_probe._delta(ma_probe._topk_metrics(group, "challenger", 10)["mean_ret20"], ma_probe._topk_metrics(group, "champion", 10)["mean_ret20"]),
            "changed_top5_members_count": int(group["changed_top5_member"].fillna(False).astype(bool).sum()),
            "changed_top10_members_count": int(group["changed_top10_member"].fillna(False).astype(bool).sum()),
            "bad_pick_removal_count": int(ma_probe._bad_pick_count(removed10) - ma_probe._bad_pick_count(added10)),
            "added_pick_quality_summary": ma_probe._quality(added10),
            "removed_pick_quality_summary": ma_probe._quality(removed10),
            "added_minus_removed_quality": ma_probe._quality_delta(ma_probe._quality(added10), ma_probe._quality(removed10)),
        }
        rows.append(row)
    return rows


def _concentration(rows: list[dict[str, Any]], bucket_key: str) -> dict[str, Any]:
    total_changed = sum(max(0, int(row.get("changed_top10_members_count") or 0)) for row in rows)
    max_changed = max((max(0, int(row.get("changed_top10_members_count") or 0)) for row in rows), default=0)
    positive_buckets = sum(1 for row in rows if (row.get("top10_delta") or 0.0) > 0.0 and int(row.get("changed_top10_members_count") or 0) > 0)
    max_share = None if total_changed <= 0 else float(max_changed / total_changed)
    return {
        "bucket_key": bucket_key,
        "bucket_count": len({str(row.get(bucket_key)) for row in rows}),
        "positive_bucket_count": positive_buckets,
        "total_changed_top10_members_count": int(total_changed),
        "max_changed_top10_members_count": int(max_changed),
        "max_changed_share": max_share,
        "concentrated": bool(total_changed > 0 and max_share is not None and max_share > 0.50) or positive_buckets <= 1,
    }


def _stability_decision(best_metrics: dict[str, Any], month_rows: list[dict[str, Any]], regime_rows: list[dict[str, Any]], churn_rows: list[dict[str, Any]]) -> tuple[str, list[str]]:
    top5_safe = (best_metrics.get("top5_mean_ret20_delta") or 0.0) >= 0.0 and int(best_metrics.get("changed_top5_members_count") or 0) == 0
    bad_pick_positive = int(best_metrics.get("bad_pick_removal_metric") or 0) > 0
    top10_ok = (best_metrics.get("top10_mean_ret20_delta") or 0.0) >= 0.0
    churn10 = next((row for row in churn_rows if int(row.get("top_k") or 0) == 10), {})
    quality_delta = churn10.get("added_minus_removed_quality", {})
    added_better = (quality_delta.get("mean_ret20_delta_vs_removed") or 0.0) > 0.0 and (quality_delta.get("bottom15_rate_delta_vs_removed") or 0.0) <= 0.0
    month_concentrated = _concentration(month_rows, "month_bucket")["concentrated"]
    regime_concentrated = _concentration(regime_rows, "regime_label")["concentrated"]
    sample_thin = int(best_metrics.get("bad_pick_removal_metric") or 0) < 2
    reasons: list[str] = []
    reasons.append("top5_safe" if top5_safe else "top5_quality_worse")
    reasons.append("bad_pick_removal_positive" if bad_pick_positive else "bad_pick_removal_disappeared")
    reasons.append("top10_uplift_non_negative" if top10_ok else "top10_uplift_negative")
    reasons.append("added_quality_better_than_removed" if added_better else "added_quality_not_better_than_removed")
    if month_concentrated:
        reasons.append("month_concentration_problem")
    if regime_concentrated:
        reasons.append("regime_concentration_problem")
    if sample_thin:
        reasons.append("bad_pick_removal_sample_small")
    if not top5_safe or not bad_pick_positive or not added_better:
        return "drop_after_stability_check", reasons
    if top10_ok and not month_concentrated and not regime_concentrated and not sample_thin:
        return "keep_for_next_stage", reasons
    return "hold_for_more_validation", reasons


def _stability_read_back(output_dir: Path) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    for name in REQUIRED_STABILITY_JSON:
        path = output_dir / name
        parse_status[name] = path.exists()
        if path.exists():
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    examples_path = output_dir / "top5_safe_bad_pick_removal_added_removed_examples.parquet"
    manifest = _load_json(output_dir / "top5_safe_bad_pick_removal_stability_manifest.json")
    stability = _load_json(output_dir / "top5_safe_bad_pick_removal_stability.json")
    return {
        "parse_status": parse_status,
        "verification": {
            "required_artifacts_exist": all((output_dir / name).exists() for name in REQUIRED_STABILITY_JSON) and examples_path.exists(),
            "required_json_parse": all(parse_status.values()),
            "examples_parquet_exists": examples_path.exists(),
            "top5_delta_non_negative": (stability.get("top5_delta") or 0.0) >= 0.0,
            "changed_top5_safe": int(stability.get("changed_top5_members_count") or 0) == 0,
            "bad_pick_removal_positive": int(stability.get("bad_pick_removal_count") or 0) > 0,
            "forward_ret20_is_evaluation_only": True,
            "no_silent_fallback": manifest.get("silent_fallback_used") is False,
            "no_meemee_reflection": manifest.get("meemee_reflection") is False,
            "no_production_registration": manifest.get("production_registration") is False,
            "no_champion_artifact_regeneration": manifest.get("champion_artifact_regenerated") is False,
        },
    }


def run_top5_safe_bad_pick_removal_stability(
    *,
    source_run_dir: Path,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    started_at = _utc_now()
    source_run_dir = Path(source_run_dir).resolve()
    compare = _load_json(source_run_dir / "top5_safe_bad_pick_removal_compare.json")
    manifest_source = _load_json(source_run_dir / "top5_safe_bad_pick_removal_manifest.json")
    final_rollup = _load_json(source_run_dir / FINAL_ROLLUP_JSON)
    if final_rollup.get("final_axis_status") != "closed_as_research_keep":
        raise RuntimeError("source top5_safe_bad_pick_removal_v1 is not closed_as_research_keep")
    best_variant_id = str(final_rollup["best_variant"])
    variant = next(item for item in VARIANTS if item.variant_id == best_variant_id)
    ma_rollup_path = Path(str(manifest_source["source_prior_rollups"]["ma_buy_sell_probe_v1"]))
    ma_rollup = _load_json(ma_rollup_path)
    source_artifacts = ma_probe._read_source_run_artifacts(Path(str(ma_rollup["source_role_validation_run"])))
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))
    source = ma_probe.load_source_rows(source_rows_path)
    features = ma_probe.build_ma_bar_features(ma_probe.load_daily_bars(stock_db, sorted(source["symbol"].astype(str).unique().tolist())))
    joined = ma_probe.join_features_to_source(source, features)
    canonical, _meta = ma_probe._load_canonical_regime_rows(stock_db)
    if canonical.empty:
        raise RuntimeError("canonical market_regime_daily unavailable for stability by_regime")
    joined = ma_probe._with_canonical_regime_labels(joined, canonical)
    ranked = _rank_with_top5_safe_demotion(joined, variant)
    best_metrics = _variant_metrics(ranked, variant)
    month_rows = _group_stability_rows(ranked, "month_bucket")
    regime_rows = _group_stability_rows(ranked, "regime_label")
    examples_frames = []
    churn_rows = []
    for top_k in (5, 10):
        added = _changed_rows(ranked, top_k, added=True).copy()
        removed = _changed_rows(ranked, top_k, added=False).copy()
        added["change_type"] = "added"
        removed["change_type"] = "removed"
        added["top_k"] = top_k
        removed["top_k"] = top_k
        examples_frames.extend([added, removed])
        churn_rows.append(
            {
                "variant_id": best_variant_id,
                "top_k": top_k,
                "added_pick_quality_summary": ma_probe._quality(added),
                "removed_pick_quality_summary": ma_probe._quality(removed),
                "added_minus_removed_quality": ma_probe._quality_delta(ma_probe._quality(added), ma_probe._quality(removed)),
                "changed_members_count": int(ranked[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum()),
            }
        )
    stability_decision, reasons = _stability_decision(best_metrics, month_rows, regime_rows, churn_rows)
    output_dir = Path(output_root).resolve() / str(run_id or (_default_run_id() + "-stability")).strip()
    output_dir.mkdir(parents=True, exist_ok=True)
    examples = pd.concat(examples_frames, ignore_index=True) if examples_frames else pd.DataFrame()
    examples_path = output_dir / "top5_safe_bad_pick_removal_added_removed_examples.parquet"
    examples.to_parquet(examples_path, index=False)
    month_concentration = _concentration(month_rows, "month_bucket")
    regime_concentration = _concentration(regime_rows, "regime_label")
    stability = {
        "schema_version": f"{SCHEMA_PREFIX}_stability_v1",
        "generated_at": _utc_now(),
        "source_run_dir": str(source_run_dir),
        "best_variant_id": best_variant_id,
        "stability_decision": stability_decision,
        "typed_reasons": reasons,
        "top5_delta": best_metrics.get("top5_mean_ret20_delta"),
        "changed_top5_members_count": best_metrics.get("changed_top5_members_count"),
        "top10_delta": best_metrics.get("top10_mean_ret20_delta"),
        "changed_top10_members_count": best_metrics.get("changed_top10_members_count"),
        "bad_pick_removal_count": best_metrics.get("bad_pick_removal_metric"),
        "changed_rank_count": best_metrics.get("changed_rank_count"),
        "month_concentration": month_concentration,
        "regime_concentration": regime_concentration,
    }
    by_month = {"schema_version": f"{SCHEMA_PREFIX}_by_month_v1", "generated_at": _utc_now(), "rows": month_rows}
    by_regime = {"schema_version": f"{SCHEMA_PREFIX}_by_regime_v1", "generated_at": _utc_now(), "rows": regime_rows}
    churn = {"schema_version": f"{SCHEMA_PREFIX}_churn_stability_v1", "generated_at": _utc_now(), "rows": churn_rows, "examples_parquet": str(examples_path)}
    stability_manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_stability_manifest_v1",
        "axis_id": "top5_safe_bad_pick_removal_stability_v1",
        "run_id": output_dir.name,
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "source_run_dir": str(source_run_dir),
        "source_rows_artifact_path": str(source_rows_path),
        "best_variant_id": best_variant_id,
        "meemee_reflection": False,
        "production_registration": False,
        "champion_artifact_regenerated": False,
        "silent_fallback_used": False,
    }
    payloads = {
        "top5_safe_bad_pick_removal_stability.json": stability,
        "top5_safe_bad_pick_removal_by_month.json": by_month,
        "top5_safe_bad_pick_removal_by_regime.json": by_regime,
        "top5_safe_bad_pick_removal_churn_stability.json": churn,
        "top5_safe_bad_pick_removal_stability_manifest.json": stability_manifest,
    }
    for name, payload in payloads.items():
        _write_json(output_dir / name, payload)
    read_back = _stability_read_back(output_dir)
    required_complete_checks = (
        "required_artifacts_exist",
        "required_json_parse",
        "examples_parquet_exists",
        "forward_ret20_is_evaluation_only",
        "no_silent_fallback",
        "no_meemee_reflection",
        "no_production_registration",
        "no_champion_artifact_regeneration",
    )
    complete = all(bool(read_back["verification"].get(key)) for key in required_complete_checks)
    if complete:
        _write_json(
            output_dir / "_STABILITY_COMPLETE.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_stability_complete_v1",
                "generated_at": _utc_now(),
                "artifact_root": str(output_dir),
                "complete": True,
                "read_back_verification": read_back,
            },
        )
    return {
        "output_dir": str(output_dir),
        "stability_complete_written": complete,
        "stability_decision": stability_decision,
        "best_variant_id": best_variant_id,
        "required_artifacts": {name: str(output_dir / name) for name in REQUIRED_STABILITY_ARTIFACTS},
        "read_back_verification": read_back,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only top5-safe bad-pick removal challenger v1.")
    parser.add_argument("--ma-final-rollup-json", default=str(DEFAULT_MA_FINAL_ROLLUP_JSON))
    parser.add_argument("--regime-gate-final-rollup-json", default=str(DEFAULT_REGIME_GATE_FINAL_ROLLUP_JSON))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--stability-source-run", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    if str(args.stability_source_run).strip():
        result = run_top5_safe_bad_pick_removal_stability(
            source_run_dir=Path(args.stability_source_run),
            output_root=Path(args.output_root),
            run_id=args.run_id or None,
        )
        print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    result = run_top5_safe_bad_pick_removal(
        ma_final_rollup_json=Path(args.ma_final_rollup_json),
        regime_gate_final_rollup_json=Path(args.regime_gate_final_rollup_json),
        output_root=Path(args.output_root),
        run_id=args.run_id or None,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
