from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_ma_buy_sell_probe_v1 as ma_probe


AXIS_ID = "regime_applicability_gate_v1"
SCHEMA_PREFIX = "tradex_regime_applicability_gate_v1"
DEFAULT_SOURCE_FINAL_ROLLUP_JSON = Path(
    r"G:\Tradex\ma_buy_sell_probe_v1_final_decision\20260512T050000Z-ma_buy_sell_probe_v1_final_decision_rollup\ma_buy_sell_probe_v1_final_decision_rollup.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\regime_applicability_gate_v1")

TARGET_VARIANTS = (
    "ma_buy_probe.price_vs_ma_n_8",
    "ma_sell_probe.price_cross_below_ma_n_8",
)
ALLOW_REGIMES = ("neutral_range", "risk_off_trend", "risk_on_trend")
CAUTION_REGIMES = ("risk_on_range",)
BLOCK_REGIMES = ("capitulation_rebound", "high_vol_chaos")
REQUIRED_JSON = (
    "regime_applicability_gate_v1_compare.json",
    "regime_applicability_gate_v1_decision.json",
    "regime_applicability_gate_v1_final_rollup.json",
    "regime_applicability_gate_v1_manifest.json",
    "_ARTIFACT_COMPLETE.json",
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


def normalize_regime_label(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.startswith("C:"):
        text = text[2:]
    return text or "unknown"


def _delta(candidate: float | None, baseline: float | None) -> float | None:
    if candidate is None or baseline is None:
        return None
    return float(candidate - baseline)


def _rank_with_regime_gate(
    frame: pd.DataFrame,
    spec: ma_probe.VariantSpec,
    *,
    allow_regimes: tuple[str, ...] = ALLOW_REGIMES,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = frame.copy()
    working["canonical_regime_label_normalized"] = working["regime_label"].map(normalize_regime_label)
    working["regime_gate_allowed"] = working["canonical_regime_label_normalized"].isin(set(allow_regimes))
    side_mask = ma_probe._side_scope_mask(working, spec)
    signal_source = working.get(spec.signal_column)
    if signal_source is None:
        signal_source = pd.Series(pd.NA, index=working.index)
    feature_eligible = side_mask & signal_source.notna() & working["no_lookahead_valid"].fillna(False).astype(bool)
    eligible = feature_eligible & working["regime_gate_allowed"].fillna(False).astype(bool)
    signal_bool = signal_source.astype("boolean").fillna(False).astype(bool)
    pre_gate_signal_hit = feature_eligible & signal_bool
    signal_hit = eligible & signal_bool
    working["ma_probe_signal_hit"] = signal_hit
    working["ma_probe_signal_eligible"] = eligible
    working["ma_probe_pre_gate_signal_hit"] = pre_gate_signal_hit
    working["challenger_score"] = pd.to_numeric(working["champion_score"], errors="coerce")
    working.loc[signal_hit, "challenger_score"] = working.loc[signal_hit, "challenger_score"] + spec.score_delta

    ranked_parts: list[pd.DataFrame] = []
    for _, group in working.groupby(["trade_date_key", "side"], sort=True):
        ordered = group.sort_values(
            ["challenger_score", "champion_rank", "symbol"],
            ascending=[False, True, True],
            kind="stable",
        ).copy()
        ordered["challenger_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True)
    for top_k in ma_probe.TOP_K_VALUES:
        ranked[f"challenger_selected_top{top_k}"] = ranked["challenger_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        ranked[f"changed_top{top_k}_member"] = ranked[f"challenger_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["challenger_rank"].astype(int) != ranked["champion_rank"].astype(int)

    total_rows = int(side_mask.sum())
    eligible_rows = int(eligible.sum())
    feature_eligible_rows = int(feature_eligible.sum())
    pre_gate_hit_count = int(pre_gate_signal_hit.sum())
    signal_hit_count = int(signal_hit.sum())
    coverage = {
        "variant_id": spec.variant_id,
        "probe_family": spec.probe_family,
        "feature_name": spec.feature_name,
        "periods": list(spec.periods),
        "required_lookback_days": spec.required_lookback_days,
        "eligible_rows_count": eligible_rows,
        "feature_eligible_rows_count": feature_eligible_rows,
        "total_rows_count": total_rows,
        "coverage_rate": None if total_rows == 0 else float(eligible_rows / total_rows),
        "feature_coverage_rate": None if total_rows == 0 else float(feature_eligible_rows / total_rows),
        "skipped_rows_count": int(max(0, total_rows - eligible_rows)),
        "skip_reason": None if total_rows and feature_eligible_rows == total_rows else "feature_coverage_incomplete",
        "regime_gate_allowed_rows_count": int((side_mask & working["regime_gate_allowed"]).sum()),
        "regime_gate_blocked_rows_count": int((side_mask & ~working["regime_gate_allowed"]).sum()),
        "pre_gate_signal_hit_count": pre_gate_hit_count,
        "post_gate_signal_hit_count": signal_hit_count,
        "regime_gate_blocked_signal_hit_count": int(pre_gate_hit_count - signal_hit_count),
        "allow_regimes": list(allow_regimes),
        "caution_regimes": list(CAUTION_REGIMES),
        "block_regimes": list(BLOCK_REGIMES),
    }
    return ranked, coverage


def _by_regime_rows(ranked: pd.DataFrame, variant_id: str, mode: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    working = ranked.copy()
    working["canonical_regime_label_normalized"] = working["regime_label"].map(normalize_regime_label)
    for regime, group in working.groupby("canonical_regime_label_normalized", sort=True):
        row: dict[str, Any] = {
            "variant_id": variant_id,
            "mode": mode,
            "regime_label": str(regime),
            "sample_count": int(len(group)),
        }
        for top_k in ma_probe.TOP_K_VALUES:
            champion = ma_probe._topk_metrics(group, "champion", top_k)
            challenger = ma_probe._topk_metrics(group, "challenger", top_k)
            row[f"top{top_k}_delta"] = _delta(challenger.get("mean_ret20"), champion.get("mean_ret20"))
            row[f"changed_top{top_k}_members_count"] = int(group[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum())
        row["changed_rank_count"] = int(group["rank_changed"].fillna(False).astype(bool).sum())
        rows.append(row)
    return rows


def _variant_summary(
    *,
    variant_id: str,
    ungated_metrics: dict[str, Any],
    gated_metrics: dict[str, Any],
    ungated_rows: list[dict[str, Any]],
    gated_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    caution_or_block = set(CAUTION_REGIMES + BLOCK_REGIMES)
    allow = set(ALLOW_REGIMES)

    def changed(rows: list[dict[str, Any]], regimes: set[str], key: str) -> int:
        return int(sum(int(row.get(key) or 0) for row in rows if row["regime_label"] in regimes))

    ungated_noisy_top10 = changed(ungated_rows, caution_or_block, "changed_top10_members_count")
    gated_noisy_top10 = changed(gated_rows, caution_or_block, "changed_top10_members_count")
    ungated_allow_top10 = changed(ungated_rows, allow, "changed_top10_members_count")
    gated_allow_top10 = changed(gated_rows, allow, "changed_top10_members_count")
    return {
        "variant_id": variant_id,
        "ungated": {
            "candidate_local_decision": ungated_metrics.get("candidate_local_decision"),
            "top5_mean_ret20_delta": ungated_metrics.get("top5_mean_ret20_delta"),
            "top10_mean_ret20_delta": ungated_metrics.get("top10_mean_ret20_delta"),
            "top20_mean_ret20_delta": ungated_metrics.get("top20_mean_ret20_delta"),
            "changed_top5_members_count": ungated_metrics.get("changed_top5_members_count"),
            "changed_top10_members_count": ungated_metrics.get("changed_top10_members_count"),
            "changed_rank_count": ungated_metrics.get("changed_rank_count"),
            "bad_pick_removal_count": ungated_metrics.get("bad_pick_removal_count"),
            "signal_hit_count": ungated_metrics.get("signal_hit_count"),
            "selection_divergence_reason": ungated_metrics.get("selection_divergence_reason"),
        },
        "gated": {
            "candidate_local_decision": gated_metrics.get("candidate_local_decision"),
            "top5_mean_ret20_delta": gated_metrics.get("top5_mean_ret20_delta"),
            "top10_mean_ret20_delta": gated_metrics.get("top10_mean_ret20_delta"),
            "top20_mean_ret20_delta": gated_metrics.get("top20_mean_ret20_delta"),
            "changed_top5_members_count": gated_metrics.get("changed_top5_members_count"),
            "changed_top10_members_count": gated_metrics.get("changed_top10_members_count"),
            "changed_rank_count": gated_metrics.get("changed_rank_count"),
            "bad_pick_removal_count": gated_metrics.get("bad_pick_removal_count"),
            "signal_hit_count": gated_metrics.get("signal_hit_count"),
            "selection_divergence_reason": gated_metrics.get("selection_divergence_reason"),
            "coverage": gated_metrics.get("coverage"),
        },
        "delta_gated_minus_ungated": {
            "top5_mean_ret20_delta": _delta(gated_metrics.get("top5_mean_ret20_delta"), ungated_metrics.get("top5_mean_ret20_delta")),
            "top10_mean_ret20_delta": _delta(gated_metrics.get("top10_mean_ret20_delta"), ungated_metrics.get("top10_mean_ret20_delta")),
            "top20_mean_ret20_delta": _delta(gated_metrics.get("top20_mean_ret20_delta"), ungated_metrics.get("top20_mean_ret20_delta")),
            "changed_top5_members_count": int(gated_metrics.get("changed_top5_members_count") or 0) - int(ungated_metrics.get("changed_top5_members_count") or 0),
            "changed_top10_members_count": int(gated_metrics.get("changed_top10_members_count") or 0) - int(ungated_metrics.get("changed_top10_members_count") or 0),
            "changed_rank_count": int(gated_metrics.get("changed_rank_count") or 0) - int(ungated_metrics.get("changed_rank_count") or 0),
            "bad_pick_removal_count": int(gated_metrics.get("bad_pick_removal_count") or 0) - int(ungated_metrics.get("bad_pick_removal_count") or 0),
            "signal_hit_count": int(gated_metrics.get("signal_hit_count") or 0) - int(ungated_metrics.get("signal_hit_count") or 0),
        },
        "regime_branching": {
            "ungated_caution_or_block_changed_top10_members_count": ungated_noisy_top10,
            "gated_caution_or_block_changed_top10_members_count": gated_noisy_top10,
            "caution_or_block_changed_top10_reduction": int(ungated_noisy_top10 - gated_noisy_top10),
            "ungated_allow_changed_top10_members_count": ungated_allow_top10,
            "gated_allow_changed_top10_members_count": gated_allow_top10,
        },
    }


def _axis_decision(variant_summaries: list[dict[str, Any]]) -> tuple[str, list[str]]:
    total_noisy_reduction = sum(
        int(row["regime_branching"]["caution_or_block_changed_top10_reduction"])
        for row in variant_summaries
    )
    total_gated_allow_branching = sum(
        int(row["regime_branching"]["gated_allow_changed_top10_members_count"])
        for row in variant_summaries
    )
    top10_worse = [
        row["variant_id"]
        for row in variant_summaries
        if (row["delta_gated_minus_ungated"]["top10_mean_ret20_delta"] or 0.0) < 0.0
    ]
    top5_worse = [
        row["variant_id"]
        for row in variant_summaries
        if (row["delta_gated_minus_ungated"]["top5_mean_ret20_delta"] or 0.0) < 0.0
    ]
    gated_positive_top10 = [
        row["variant_id"]
        for row in variant_summaries
        if (row["gated"]["top10_mean_ret20_delta"] or 0.0) > 0.0
    ]
    reasons: list[str] = []
    if total_noisy_reduction > 0:
        reasons.append("caution_or_block_branching_reduced")
    else:
        reasons.append("no_caution_or_block_branching_reduction")
    if total_gated_allow_branching > 0:
        reasons.append("allowed_regime_branching_remains")
    else:
        reasons.append("allowed_regime_branching_absent")
    if top10_worse:
        reasons.append("top10_quality_worse_vs_ungated")
    else:
        reasons.append("top10_quality_not_worse_vs_ungated")
    if top5_worse:
        reasons.append("top5_quality_worse_vs_ungated")
    else:
        reasons.append("top5_quality_not_worse_vs_ungated")
    if len(gated_positive_top10) == len(variant_summaries):
        reasons.append("gated_top10_positive_for_all_candidates")
    else:
        reasons.append("gated_top10_not_positive_for_all_candidates")

    if total_noisy_reduction > 0 and total_gated_allow_branching > 0 and not top10_worse and not top5_worse and len(gated_positive_top10) == len(variant_summaries):
        return "keep", reasons
    if total_noisy_reduction <= 0 and (top10_worse or total_gated_allow_branching <= 0):
        return "drop", reasons
    return "hold", reasons


def _variant_by_prefix(variant_summaries: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    for row in variant_summaries:
        if str(row.get("variant_id", "")).startswith(prefix):
            return row
    raise RuntimeError(f"variant summary missing for prefix: {prefix}")


def _final_rollup_payload(
    *,
    output_dir: Path,
    compare_payload: dict[str, Any],
    decision_payload: dict[str, Any],
    manifest_payload: dict[str, Any],
) -> dict[str, Any]:
    variant_summaries = compare_payload["variant_summaries"]
    buy = _variant_by_prefix(variant_summaries, "ma_buy_probe.")
    sell = _variant_by_prefix(variant_summaries, "ma_sell_probe.")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_final_rollup_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "source_decision_json": str(output_dir / "regime_applicability_gate_v1_decision.json"),
        "source_compare_json": str(output_dir / "regime_applicability_gate_v1_compare.json"),
        "final_axis_status": "closed_as_noise_control_hold",
        "authoritative_decision": decision_payload["authoritative_decision"],
        "best_gate_variant": {
            "variant_id": "canonical_allow_only_8ma_gate",
            "allow_regimes": list(ALLOW_REGIMES),
            "caution_regimes": list(CAUTION_REGIMES),
            "block_regimes": list(BLOCK_REGIMES),
            "status": "noise_control_hold",
        },
        "buy_gate_decision": {
            "variant_id": buy["variant_id"],
            "ungated_local_decision": buy["ungated"]["candidate_local_decision"],
            "gated_local_decision": buy["gated"]["candidate_local_decision"],
            "authoritative_axis_decision": decision_payload["authoritative_decision"],
        },
        "sell_gate_decision": {
            "variant_id": sell["variant_id"],
            "ungated_local_decision": sell["ungated"]["candidate_local_decision"],
            "gated_local_decision": sell["gated"]["candidate_local_decision"],
            "authoritative_axis_decision": decision_payload["authoritative_decision"],
        },
        "not_keep_reasons": [
            "top5_quality_worse_vs_ungated",
            "gated_sell_local_decision_drop",
            "bad_pick_removal_worse_for_sell",
            "not_production_ready",
        ],
        "not_drop_reasons": [
            "caution_or_block_branching_reduced",
            "allowed_regime_branching_remains",
            "top10_quality_not_worse_vs_ungated",
            "gated_top10_positive_for_all_candidates",
        ],
        "noisy_branching_reduction_summary": {
            "buy_caution_or_block_top10_branching_before": buy["regime_branching"]["ungated_caution_or_block_changed_top10_members_count"],
            "buy_caution_or_block_top10_branching_after": buy["regime_branching"]["gated_caution_or_block_changed_top10_members_count"],
            "sell_caution_or_block_top10_branching_before": sell["regime_branching"]["ungated_caution_or_block_changed_top10_members_count"],
            "sell_caution_or_block_top10_branching_after": sell["regime_branching"]["gated_caution_or_block_changed_top10_members_count"],
        },
        "top5_degradation_summary": {
            "buy_gated_minus_ungated_top5_mean_ret20_delta": buy["delta_gated_minus_ungated"]["top5_mean_ret20_delta"],
            "sell_gated_minus_ungated_top5_mean_ret20_delta": sell["delta_gated_minus_ungated"]["top5_mean_ret20_delta"],
        },
        "top10_preservation_summary": {
            "buy_gated_minus_ungated_top10_mean_ret20_delta": buy["delta_gated_minus_ungated"]["top10_mean_ret20_delta"],
            "sell_gated_minus_ungated_top10_mean_ret20_delta": sell["delta_gated_minus_ungated"]["top10_mean_ret20_delta"],
            "buy_top10_changed_before": buy["ungated"]["changed_top10_members_count"],
            "buy_top10_changed_after": buy["gated"]["changed_top10_members_count"],
            "sell_top10_changed_before": sell["ungated"]["changed_top10_members_count"],
            "sell_top10_changed_after": sell["gated"]["changed_top10_members_count"],
        },
        "bad_pick_removal_change_summary": {
            "buy_gated_minus_ungated_bad_pick_removal_count": buy["delta_gated_minus_ungated"]["bad_pick_removal_count"],
            "sell_gated_minus_ungated_bad_pick_removal_count": sell["delta_gated_minus_ungated"]["bad_pick_removal_count"],
        },
        "production_registration": False,
        "meemee_reflection": False,
        "champion_artifact_regenerated": False,
        "silent_fallback_used": False,
        "changes_ma_probe_v1_final_decision": False,
        "next_allowed_axis": {
            "axis_id": "top5_safe_bad_pick_removal_v1",
            "status": "allowed_next",
            "purpose": "validate top5-safe bad-pick removal without continuing regime gate tuning",
        },
        "blocked_actions": [
            "continue_tuning_regime_gate_variants",
            "add_more_gate_variants",
            "change_score_delta",
            "change_sell_guardrail",
            "add_ma_periods",
            "run_2_to_200_ma_sweep",
            "reflect_to_meemee",
            "register_production_ranking",
            "regenerate_champion_artifact",
        ],
        "fixed_condition_hash": compare_payload["fixed_condition_hash"],
        "condition_contract_hash": manifest_payload["condition_contract_hash"],
        "join_quality": compare_payload["join_quality"],
    }


def _read_back(output_dir: Path) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    for name in REQUIRED_JSON:
        path = output_dir / name
        parse_status[name] = path.exists()
        if path.exists():
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    compare = _load_json(output_dir / "regime_applicability_gate_v1_compare.json")
    decision = _load_json(output_dir / "regime_applicability_gate_v1_decision.json")
    manifest = _load_json(output_dir / "regime_applicability_gate_v1_manifest.json")
    complete = _load_json(output_dir / "_ARTIFACT_COMPLETE.json")
    return {
        "parse_status": parse_status,
        "verification": {
            "required_json_exist": all((output_dir / name).exists() for name in REQUIRED_JSON),
            "required_json_parse": all(parse_status.values()),
            "decision_is_typed": decision.get("authoritative_decision") in {"keep", "drop", "hold"},
            "variant_count": len(compare.get("variant_summaries", [])),
            "target_variants_match": sorted(row.get("variant_id") for row in compare.get("variant_summaries", [])) == sorted(TARGET_VARIANTS),
            "same_fixed_condition_hash": bool(compare.get("fixed_condition_hash")) and compare.get("fixed_condition_hash") == manifest.get("fixed_condition_hash"),
            "no_meemee_reflection_check_passed": manifest.get("meemee_reflection") is False,
            "no_production_registration_check_passed": manifest.get("production_registration") is False,
            "no_champion_artifact_regeneration_check_passed": manifest.get("champion_artifact_regenerated") is False,
            "no_silent_fallback_check_passed": manifest.get("silent_fallback_used") is False,
            "artifact_complete": complete.get("complete") is True,
        },
    }


def run_regime_applicability_gate(
    *,
    source_final_rollup_json: Path = DEFAULT_SOURCE_FINAL_ROLLUP_JSON,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    started_at = _utc_now()
    source_final_rollup_json = Path(source_final_rollup_json).resolve()
    if not source_final_rollup_json.exists():
        raise FileNotFoundError(f"source final rollup artifact missing: {source_final_rollup_json}")
    source_rollup = _load_json(source_final_rollup_json)
    if source_rollup.get("final_axis_status") != "closed_as_regime_conditional_hold":
        raise RuntimeError(f"source final rollup not closed_as_regime_conditional_hold: {source_final_rollup_json}")

    source_run_dir = Path(str(source_rollup["source_role_validation_run"])).resolve()
    if not source_run_dir.exists():
        raise FileNotFoundError(f"source role validation run missing: {source_run_dir}")
    source_artifacts = ma_probe._read_source_run_artifacts(source_run_dir)
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    fixed_hash = str(evaluation_contract["fixed_condition_hash"])
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))
    canonical_artifact = str(source_rollup.get("regime_source", {}).get("canonical_regime_artifact_path") or "")
    canonical_db = Path(canonical_artifact.split("#", 1)[0]).resolve() if canonical_artifact else stock_db

    source = ma_probe.load_source_rows(source_rows_path)
    canonical, canonical_meta = ma_probe._load_canonical_regime_rows(canonical_db)
    if canonical.empty:
        raise RuntimeError(f"canonical regime source is missing or cannot be joined: {canonical_db} meta={canonical_meta}")
    features = ma_probe.build_ma_bar_features(ma_probe.load_daily_bars(stock_db, sorted(source["symbol"].astype(str).unique().tolist())))
    joined = ma_probe.join_features_to_source(source, features)
    if not joined["no_lookahead_valid"].fillna(False).all():
        raise RuntimeError("no-lookahead violation while rebuilding MA features")
    joined = ma_probe._with_canonical_regime_labels(joined, canonical)
    join_quality = ma_probe._canonical_regime_join_quality(source, canonical, {}, [])
    if int(join_quality.get("rows_joined_count") or 0) <= 0:
        raise RuntimeError(f"canonical regime source cannot be joined: {canonical_db}")

    run_id_text = str(run_id or _default_run_id()).strip()
    output_dir = Path(output_root).resolve() / run_id_text
    output_dir.mkdir(parents=True, exist_ok=True)

    spec_map = ma_probe._variant_spec_map()
    variant_summaries: list[dict[str, Any]] = []
    by_regime_rows: list[dict[str, Any]] = []
    for variant_id in TARGET_VARIANTS:
        spec = spec_map[variant_id]
        if tuple(spec.periods) != (8,):
            raise RuntimeError(f"MA period expansion detected for {variant_id}: {spec.periods}")
        ungated_ranked, ungated_coverage = ma_probe._rank_with_variant(joined, spec)
        gated_ranked, gated_coverage = _rank_with_regime_gate(joined, spec)
        ungated_metrics = ma_probe.decide_variant(ma_probe._variant_metrics(ungated_ranked, spec, ungated_coverage), spec)
        gated_metrics = ma_probe.decide_variant(ma_probe._variant_metrics(gated_ranked, spec, gated_coverage), spec)
        gated_metrics["coverage"] = gated_coverage
        ungated_rows = _by_regime_rows(ungated_ranked, variant_id, "ungated_8ma")
        gated_rows = _by_regime_rows(gated_ranked, variant_id, "regime_gated_8ma")
        by_regime_rows.extend(ungated_rows)
        by_regime_rows.extend(gated_rows)
        variant_summaries.append(
            _variant_summary(
                variant_id=variant_id,
                ungated_metrics=ungated_metrics,
                gated_metrics=gated_metrics,
                ungated_rows=ungated_rows,
                gated_rows=gated_rows,
            )
        )

    decision, typed_reasons = _axis_decision(variant_summaries)
    condition_contract = {
        "same_universe": True,
        "same_period": True,
        "same_top_k": list(ma_probe.TOP_K_VALUES),
        "same_cost_slippage": evaluation_contract.get("cost_slippage_config"),
        "same_ret20_source_mode": evaluation_contract.get("ret20_source_mode"),
        "same_candidate_build_order_mode": evaluation_contract.get("candidate_build_order_mode"),
        "same_score_delta": ma_probe.SCORE_DELTA_CONFIG,
        "same_sell_guardrail": ma_probe.SELL_GUARDRAIL,
        "same_ma_period": 8,
        "same_champion_artifact": evaluation_contract.get("champion_compare_json_path"),
        "same_source_rows": str(source_rows_path),
        "same_canonical_market_regime_daily_source": str(canonical_db) + "#market_regime_daily",
        "ma_period_expansion": False,
        "pair_stack_expansion": False,
        "new_feature_family": False,
        "regime_correction": False,
    }
    compare_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "source_final_rollup_json": str(source_final_rollup_json),
        "source_final_axis_status": source_rollup.get("final_axis_status"),
        "fixed_condition_hash": fixed_hash,
        "validation_grouping_hash": source_rollup.get("validation_grouping_hash"),
        "allow_regimes": list(ALLOW_REGIMES),
        "caution_regimes": list(CAUTION_REGIMES),
        "block_regimes": list(BLOCK_REGIMES),
        "condition_contract": condition_contract,
        "join_quality": join_quality,
        "variant_summaries": variant_summaries,
        "by_regime_rows": by_regime_rows,
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason_codes": typed_reasons,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_policy": {
            "keep": "gated 8MA improves or preserves top5/top10 quality for both candidates while reducing caution/block branching and retaining allowed-regime branching",
            "drop": "gate fails to reduce noisy branching and worsens quality or eliminates useful branching",
            "hold": "gate changes branching but quality evidence is mixed, weak, or not ready for production",
        },
        "production_ready": False,
        "meemee_ready": False,
        "meemee_reflection": False,
        "production_registration": False,
        "champion_artifact_regenerated": False,
    }
    manifest_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": run_id_text,
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "output_dir": str(output_dir),
        "source_final_rollup_json": str(source_final_rollup_json),
        "source_role_validation_run": str(source_run_dir),
        "source_rows_artifact_path": str(source_rows_path),
        "runtime_stock_db_path": str(stock_db),
        "canonical_regime_db_path": str(canonical_db),
        "fixed_condition_hash": fixed_hash,
        "condition_contract_hash": _stable_hash(condition_contract),
        "output_artifacts": list(REQUIRED_JSON),
        "meemee_reflection": False,
        "production_registration": False,
        "champion_artifact_regenerated": False,
        "silent_fallback_used": False,
        "non_scope": [
            "MeeMee files",
            "MeeMee display MA",
            "MeeMee ranking UI",
            "MeeMee publish flow",
            "production ranking registration",
            "champion artifact regeneration",
            "2MA-200MA sweep",
            "new MA periods",
            "score delta optimization",
            "sell guardrail optimization",
            "regime correction",
            "ticker-specific correction",
            "ranking loss change",
        ],
    }
    _write_json(output_dir / "regime_applicability_gate_v1_compare.json", compare_payload)
    _write_json(output_dir / "regime_applicability_gate_v1_decision.json", decision_payload)
    final_rollup_payload = _final_rollup_payload(
        output_dir=output_dir,
        compare_payload=compare_payload,
        decision_payload=decision_payload,
        manifest_payload=manifest_payload,
    )
    _write_json(output_dir / "regime_applicability_gate_v1_final_rollup.json", final_rollup_payload)
    _write_json(output_dir / "regime_applicability_gate_v1_manifest.json", manifest_payload)
    complete_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": True,
        "required_json": list(REQUIRED_JSON),
        "read_back_verification": {},
    }
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete_payload)
    read_back = _read_back(output_dir)
    complete_payload["read_back_verification"] = read_back
    complete_payload["complete"] = all(bool(value) for value in read_back["verification"].values())
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete_payload)
    return {
        "output_dir": str(output_dir),
        "decision": decision,
        "required_artifacts": {name: str(output_dir / name) for name in REQUIRED_JSON},
        "read_back_verification": read_back,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only regime applicability gate v1 for 8MA buy/sell candidates.")
    parser.add_argument("--source-final-rollup-json", default=str(DEFAULT_SOURCE_FINAL_ROLLUP_JSON))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_regime_applicability_gate(
        source_final_rollup_json=Path(args.source_final_rollup_json),
        output_root=Path(args.output_root),
        run_id=args.run_id or None,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
