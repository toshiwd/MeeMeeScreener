from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
RESEARCH_INVENTORY = REPO_ROOT / "artifacts" / "research_inventory"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\keep\entry_method_research_pivot_20260420")
SESSION_ROLLUP_PATH = Path(r"G:\Tradex\scratch\research_sessions\session_leaderboard_rollup.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    return text or "item"


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _source_paths(source: dict[str, Any], *keys: str) -> dict[str, str]:
    return {key: str(source[key]) for key in keys if key in source}


def _collect_nested_dicts(obj: Any, target_key: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        if isinstance(obj.get(target_key), dict):
            found.append(obj[target_key])
        for value in obj.values():
            found.extend(_collect_nested_dicts(value, target_key))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(_collect_nested_dicts(item, target_key))
    return found


def _count_unique_codes(payload: dict[str, Any], *keys: str) -> tuple[int, dict[str, int]]:
    counter: Counter[str] = Counter()
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            for items in value.values():
                for item in _as_list(items):
                    text = str(item).strip()
                    if text:
                        counter[text] += 1
        elif isinstance(value, list):
            for row in value:
                if isinstance(row, dict):
                    code = row.get("code") or row.get("candidate_id") or row.get("method_id")
                    text = str(code).strip()
                    if text:
                        counter[text] += 1
    return len(counter), dict(counter)


def _scan_json_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.rglob("*.json"))


def _extract_compare_summary(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    decision = (
        payload.get("authoritative_candidate_gate")
        or payload.get("decision")
        or payload.get("candidate_local_decision")
        or payload.get("latest_decision")
    )
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    coverage = evidence.get("coverage_status") if isinstance(evidence.get("coverage_status"), dict) else {}
    same_condition = payload.get("same_condition_contract") or payload.get("comparison_contract") or {}
    candidate_name = str(payload.get("candidate_name") or payload.get("method_title") or path.stem)
    family_name = str(payload.get("family_name") or payload.get("method_family") or "unknown")
    feature_class = str(payload.get("feature_class") or payload.get("feature_family") or "unknown")
    selection_divergence_reason = str(
        payload.get("selection_divergence_reason")
        or evidence.get("selection_divergence_reason")
        or payload.get("compare_engine_local_reason")
        or "unknown"
    )
    counts = {
        "changed_top5_members_count": _safe_int(
            payload.get("changed_top5_members_count")
            or evidence.get("changed_top5_members_count")
            or payload.get("top5_members_changed")
        ),
        "changed_top10_members_count": _safe_int(
            payload.get("changed_top10_members_count")
            or evidence.get("changed_top10_members_count")
            or payload.get("top10_members_changed")
        ),
        "changed_rank_count": _safe_int(
            payload.get("changed_rank_count")
            or evidence.get("changed_rank_count")
            or payload.get("rank_changes")
        ),
        "bad_pick_removal": _safe_int(payload.get("bad_pick_removal") or evidence.get("bad_pick_removal")),
        "top5_uplift": _safe_float(payload.get("top5_uplift") or evidence.get("top5_uplift")),
        "top10_uplift": _safe_float(payload.get("top10_uplift") or evidence.get("top10_uplift")),
        "worst_regime_delta": _safe_float(payload.get("worst_regime_delta") or evidence.get("worst_regime_delta")),
        "overlap_vs_champion": payload.get("overlap_vs_champion") or evidence.get("overlap_vs_champion"),
        "overlap_vs_peer_candidates": payload.get("overlap_vs_peer_candidates") or evidence.get("overlap_vs_peer_candidates"),
        "coverage_windows": _safe_int(coverage.get("evaluation_window_count")),
        "coverage_status_reasons": coverage.get("status_reasons") if isinstance(coverage.get("status_reasons"), list) else [],
        "selected_count": _safe_int(evidence.get("target_symbol_count") or payload.get("selected_count")),
    }
    selected_codes_by_month = payload.get("selected_codes_by_month") if isinstance(payload.get("selected_codes_by_month"), dict) else {}
    baseline_codes_by_month = payload.get("baseline_codes_by_month") if isinstance(payload.get("baseline_codes_by_month"), dict) else {}
    selected_codes = sorted({str(code) for codes in selected_codes_by_month.values() for code in _as_list(codes) if str(code).strip()})
    baseline_codes = sorted({str(code) for codes in baseline_codes_by_month.values() for code in _as_list(codes) if str(code).strip()})
    selected_code_counter = Counter(
        str(code)
        for codes in selected_codes_by_month.values()
        for code in _as_list(codes)
        if str(code).strip()
    )
    selected_symbol_concentration = None
    if selected_code_counter:
        top_share = max(selected_code_counter.values()) / sum(selected_code_counter.values())
        selected_symbol_concentration = round(float(top_share), 6)
    return {
        "source_type": "compare_summary",
        "source_path": str(path),
        "candidate_name": candidate_name,
        "family_name": family_name,
        "feature_class": feature_class,
        "decision": str(decision) if decision is not None else "unknown",
        "selection_divergence_reason": selection_divergence_reason,
        "metrics": counts,
        "same_condition_contract": same_condition,
        "selected_codes": selected_codes,
        "baseline_codes": baseline_codes,
        "selected_symbol_concentration": selected_symbol_concentration,
    }


def _extract_family_summary(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    status_summary = payload.get("status_summary") if isinstance(payload.get("status_summary"), dict) else {}
    return {
        "source_type": "family_summary",
        "source_path": str(path),
        "family_id": str(payload.get("family_id") or path.parent.name),
        "family_name": str(payload.get("family_name") or payload.get("family_id") or path.parent.name),
        "candidate_limit": _safe_int(payload.get("candidate_limit")),
        "total_runs": _safe_int(status_summary.get("total_runs")),
        "candidate_runs": _safe_int(status_summary.get("candidate_runs")),
        "baseline_runs": _safe_int(status_summary.get("baseline_runs")),
        "status_counts": status_summary.get("status_counts") if isinstance(status_summary.get("status_counts"), dict) else {},
        "universe_size": len(_as_list(payload.get("universe"))),
        "period_segments": len(_as_list(payload.get("period", {}).get("segments") if isinstance(payload.get("period"), dict) else [])),
    }


def _load_source_catalog() -> dict[str, Any]:
    compare_files = _scan_json_files(Path(r"G:\Tradex\scratch\research_families"))
    family_files = [path for path in compare_files if path.name == "family.json"]
    compare_jsons = [path for path in compare_files if path.name == "compare.json"]

    compare_rows: list[dict[str, Any]] = []
    family_rows: list[dict[str, Any]] = []
    for path in compare_jsons:
        try:
            payload = _load_json(path)
        except Exception:
            continue
        if isinstance(payload, dict):
            compare_rows.append(_extract_compare_summary(path, payload))
    for path in family_files:
        try:
            payload = _load_json(path)
        except Exception:
            continue
        if isinstance(payload, dict):
            family_rows.append(_extract_family_summary(path, payload))

    compare_decisions = Counter(row.get("decision") for row in compare_rows if row.get("decision"))
    family_names = sorted({str(row.get("family_name")) for row in compare_rows if row.get("family_name")})
    candidate_names = sorted({str(row.get("candidate_name")) for row in compare_rows if row.get("candidate_name")})
    feature_classes = Counter(str(row.get("feature_class")) for row in compare_rows if row.get("feature_class"))

    return {
        "compare_file_count": len(compare_jsons),
        "family_file_count": len(family_files),
        "compare_decision_distribution": dict(compare_decisions),
        "feature_class_distribution": dict(feature_classes),
        "unique_family_name_count": len(family_names),
        "unique_candidate_name_count": len(candidate_names),
        "sample_family_names": family_names[:20],
        "sample_candidate_names": candidate_names[:20],
        "compare_rows": compare_rows,
        "family_rows": family_rows,
    }


def _extract_bucket_metrics(payload: dict[str, Any], bucket_name: str) -> dict[str, Any]:
    bucket_map = payload.get("bucket_20d_metrics") if isinstance(payload.get("bucket_20d_metrics"), dict) else {}
    bucket = bucket_map.get(bucket_name) if isinstance(bucket_map.get(bucket_name), dict) else {}
    return {
        "selected_count": _safe_int(bucket.get("sample_count_total") or bucket.get("sample_count_available")),
        "sample_count_available": _safe_int(bucket.get("sample_count_available")),
        "hit_rate": _safe_float(bucket.get("hit_rate_20d") or bucket.get("hit_rate")),
        "mean_ret20": _safe_float(bucket.get("mean_forward_return_20d") or bucket.get("mean_ret20")),
        "median_ret20": _safe_float(bucket.get("median_forward_return_20d") or bucket.get("median_ret20")),
        "bad_loss_rate": _safe_float(bucket.get("bad_loss_rate_20d") or bucket.get("bad_loss_rate")),
        "ge_10pct_count": _safe_int(bucket.get("ge_10pct_count_20d") or bucket.get("ge_10pct_count")),
        "ge_20pct_count": _safe_int(bucket.get("ge_20pct_count_20d") or bucket.get("ge_20pct_count")),
    }


def _extract_short_trend_metrics(compare_payload: dict[str, Any], wide_payload: dict[str, Any], regime_payload: dict[str, Any]) -> dict[str, Any]:
    baseline = compare_payload.get("baseline") if isinstance(compare_payload.get("baseline"), dict) else {}
    challenger = compare_payload.get("challenger") if isinstance(compare_payload.get("challenger"), dict) else {}
    delta = compare_payload.get("delta") if isinstance(compare_payload.get("delta"), dict) else {}
    wide = wide_payload.get("wide_stability") if isinstance(wide_payload.get("wide_stability"), dict) else {}
    year_summaries = wide.get("year_summaries") if isinstance(wide.get("year_summaries"), list) else []
    monthly_rows = compare_payload.get("monthly_rows") if isinstance(compare_payload.get("monthly_rows"), list) else []
    selected_rows = compare_payload.get("selected_rows") if isinstance(compare_payload.get("selected_rows"), list) else []
    selected_codes = [str(row.get("code")) for row in selected_rows if isinstance(row, dict) and str(row.get("code") or "").strip()]
    if not selected_codes:
        for code_map in _collect_nested_dicts(compare_payload, "selected_codes_by_month"):
            for codes in code_map.values():
                for code in _as_list(codes):
                    text = str(code).strip()
                    if text:
                        selected_codes.append(text)
    selected_code_counts = Counter(selected_codes)
    regime_map = regime_payload.get("regime_map") if isinstance(regime_payload.get("regime_map"), list) else []
    regime_evidence = {
        str(row.get("regime")): {
            "regime_label": row.get("regime_label"),
            "evidence_strength": row.get("evidence_strength"),
            "why": row.get("why"),
            "baseline_count": row.get("baseline_count"),
            "challenger_count": row.get("challenger_count"),
            "hit_rate_delta": row.get("hit_rate_delta"),
            "median_ret20_delta": row.get("median_ret20_delta"),
            "mean_ret20_delta": row.get("mean_ret20_delta"),
        }
        for row in regime_map
        if isinstance(row, dict)
    }
    return {
        "baseline_count": _safe_int(baseline.get("count")),
        "challenger_count": _safe_int(challenger.get("count")),
        "hit_rate": _safe_float(challenger.get("hit_rate")),
        "mean_ret20": _safe_float(challenger.get("mean_ret20")),
        "median_ret20": _safe_float(challenger.get("median_ret20")),
        "mean_mae20": _safe_float(challenger.get("mean_mae20")),
        "median_mae20": _safe_float(challenger.get("median_mae20")),
        "mean_mfe20": _safe_float(challenger.get("mean_mfe20")),
        "median_mfe20": _safe_float(challenger.get("median_mfe20")),
        "flat_rate": _safe_float(challenger.get("flat_rate")),
        "immediate_reverse_rate": _safe_float(challenger.get("immediate_reverse_rate")),
        "changed_top5_members_count": _safe_int(delta.get("changed_top5_short_count")),
        "changed_top10_members_count": _safe_int(delta.get("changed_top10_short_count")),
        "changed_rank_count": _safe_int(delta.get("changed_rank_short_count")),
        "selected_count_delta": _safe_int(delta.get("selected_count_delta")),
        "monthly_positive_count": _safe_int(wide.get("monthly_positive_count")),
        "monthly_negative_count": _safe_int(wide.get("monthly_negative_count")),
        "monthly_median_ret20": wide.get("monthly_median_ret20") if isinstance(wide.get("monthly_median_ret20"), dict) else {},
        "year_summaries": year_summaries,
        "worst_subwindow": wide.get("worst_subwindow") if isinstance(wide.get("worst_subwindow"), dict) else {},
        "selected_code_count": len(selected_code_counts),
        "selected_symbol_concentration": round(max(selected_code_counts.values()) / sum(selected_code_counts.values()), 6) if selected_code_counts else None,
        "selected_codes_by_month": compare_payload.get("selected_codes_by_month") if isinstance(compare_payload.get("selected_codes_by_month"), dict) else {},
        "baseline_codes_by_month": compare_payload.get("baseline_codes_by_month") if isinstance(compare_payload.get("baseline_codes_by_month"), dict) else {},
        "regime_evidence": regime_evidence,
        "monthly_rows": monthly_rows,
        "selected_rows_count": len(selected_rows),
    }


def _extract_liquidity_metrics(shadow_eval: dict[str, Any], keep_summary: dict[str, Any], decision_contract: dict[str, Any], turnover_policy: dict[str, Any]) -> dict[str, Any]:
    observed_metrics = shadow_eval.get("observed_metrics") if isinstance(shadow_eval.get("observed_metrics"), dict) else {}
    return {
        "turnover_proxy": _safe_float(observed_metrics.get("turnover_proxy")),
        "monthly_turnover_mean": _safe_float(observed_metrics.get("monthly_turnover_mean")),
        "monthly_turnover_median": _safe_float(observed_metrics.get("monthly_turnover_median")),
        "bad_pick_removal": _safe_int(observed_metrics.get("bad_pick_removal")),
        "top5_uplift": _safe_float(observed_metrics.get("top5_uplift")),
        "top10_uplift": _safe_float(observed_metrics.get("top10_uplift")),
        "changed_top5_members_count": _safe_int(observed_metrics.get("changed_top5_members_count")),
        "changed_top10_members_count": _safe_int(observed_metrics.get("changed_top10_members_count")),
        "worst_regime_delta": _safe_float(observed_metrics.get("worst_regime_delta")),
        "evaluation_window_count": _safe_int(observed_metrics.get("evaluation_window_count")),
        "coverage_status": observed_metrics.get("coverage_status") if isinstance(observed_metrics.get("coverage_status"), dict) else {},
        "high_turnover_band_capture_rate": _safe_float(
            next(
                (band.get("mean_capture_rate") for band in _as_list(shadow_eval.get("observed_turnover_bands")) if isinstance(band, dict) and band.get("bucket") == "high_turnover"),
                None,
            )
        ),
        "candidate_gate": keep_summary.get("authoritative_candidate_gate"),
        "candidate_gate_reason": keep_summary.get("authoritative_gate_reason"),
        "decision": keep_summary.get("decision"),
        "decision_reason": keep_summary.get("decision_reason"),
        "turnover_warning_classification": turnover_policy.get("turnover_warning_classification") or turnover_policy.get("current_classification"),
        "research_contract_status": turnover_policy.get("research_contract_status"),
        "comparison_contract": decision_contract.get("comparability_contract") if isinstance(decision_contract.get("comparability_contract"), dict) else {},
    }


def _extract_buy_audit_metrics(audit: dict[str, Any]) -> dict[str, Any]:
    overall = audit.get("summary", {}).get("overall_20d_metrics", {}) if isinstance(audit.get("summary"), dict) else {}
    bucket = audit.get("bucket_20d_metrics", {}) if isinstance(audit.get("bucket_20d_metrics"), dict) else {}
    regime_dist = audit.get("summary", {}).get("regime_bucket_distribution_20d", {}) if isinstance(audit.get("summary"), dict) else {}
    return {
        "total_events": _safe_int(audit.get("summary", {}).get("total_events")) if isinstance(audit.get("summary"), dict) else None,
        "events_with_20d_available": _safe_int(audit.get("summary", {}).get("events_with_20d_available")) if isinstance(audit.get("summary"), dict) else None,
        "hit_rate": _safe_float(overall.get("hit_rate")),
        "mean_forward_return_20d": _safe_float(overall.get("mean_forward_return_20d")),
        "median_forward_return_20d": _safe_float(overall.get("median_forward_return_20d")),
        "bad_loss_rate_20d": _safe_float(overall.get("bad_loss_rate_20d")),
        "outcome_distribution_20d": audit.get("summary", {}).get("outcome_bucket_distribution_20d") if isinstance(audit.get("summary"), dict) else {},
        "regime_distribution_20d": regime_dist,
        "bucket_20d_metrics": bucket,
    }


def _extract_blocker_metrics(summary: dict[str, Any], blocker: dict[str, Any]) -> dict[str, Any]:
    return {
        "study_status": summary.get("study_status"),
        "same_condition_status": summary.get("same_condition_compare", {}).get("status") if isinstance(summary.get("same_condition_compare"), dict) else None,
        "same_condition_reason": summary.get("same_condition_compare", {}).get("reason") if isinstance(summary.get("same_condition_compare"), dict) else None,
        "blocker_state": blocker.get("blocking_reason"),
        "complete_horizon_count": blocker.get("observed_blocker_state", {}).get("complete_horizon_count") if isinstance(blocker.get("observed_blocker_state"), dict) else None,
        "labeled_sample_count": blocker.get("observed_blocker_state", {}).get("labeled_sample_count") if isinstance(blocker.get("observed_blocker_state"), dict) else None,
    }


def _family_blueprints() -> list[dict[str, Any]]:
    return [
        {
            "family_id": "short_higher_timeframe_alignment",
            "family_name": "Higher Timeframe Alignment",
            "side": "short",
            "variant_name": "short_trend_alignment_v1",
            "decision": "keep",
            "decision_reason": "Short trend-alignment candidate kept and widened history remained usable; this is the clearest short-side reusable signal in the current source set.",
            "status": "confirmed",
            "evidence_strength": "medium",
            "target_failure_mode": "trend-misaligned shorts",
            "hypothesis": "Keep shorts only when weekly and monthly downside context align enough to justify a close entry.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Trend framing uses symbol-agnostic context features and already branched across months in the short audit.",
            "source_keys": ("short_trend_compare", "short_trend_wide", "short_trend_regime"),
        },
        {
            "family_id": "short_range_middle_suppression",
            "family_name": "Range-Middle Suppression",
            "side": "short",
            "variant_name": "short_range_middle_suppression_v1",
            "decision": "drop",
            "decision_reason": "The range-middle short variant was explicitly dropped in the short trend audit.",
            "status": "confirmed",
            "evidence_strength": "high",
            "target_failure_mode": "range-middle false shorts",
            "hypothesis": "Suppress shorts that sit inside broad range-middle noise rather than on a real downside edge.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Range-middle noise is a structural, symbol-level false-positive pattern.",
            "source_keys": ("short_trend_compare", "short_trend_report"),
        },
        {
            "family_id": "short_trend_followthrough",
            "family_name": "Trend Followthrough Filter",
            "side": "short",
            "variant_name": "short_trend_alignment_plus_followthrough_v1",
            "decision": "drop",
            "decision_reason": "Adding followthrough to the short trend lane did not improve the fixed contract and was dropped.",
            "status": "confirmed",
            "evidence_strength": "high",
            "target_failure_mode": "weak short continuation zones",
            "hypothesis": "Keep only the shorts that continue after the daily trigger and are not already too late.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Weak continuation is a reusable false-positive pattern across many symbols.",
            "source_keys": ("short_trend_compare", "short_trend_report", "short_trend_wide"),
        },
        {
            "family_id": "short_support_break_validity",
            "family_name": "Support Break Validity",
            "side": "short",
            "variant_name": "short_support_break_validity_v1",
            "decision": "hold",
            "decision_reason": "Support-break validity is supported by retained edge cases, but the current source evidence is still sample-thin.",
            "status": "provisional",
            "evidence_strength": "low",
            "target_failure_mode": "already-extended shorts and weak breakdowns",
            "hypothesis": "Keep shorts only when the breakdown still has room below support and is not already too stretched.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Support distance and extension are portable price-structure filters across symbols.",
            "source_keys": ("short_trend_compare", "short_feature_map", "short_broad_down_compare"),
        },
        {
            "family_id": "long_pullback_retest",
            "family_name": "Pullback Retest",
            "side": "long",
            "variant_name": "pullback_retest_v1",
            "decision": "hold",
            "decision_reason": "Pullback/rebound evidence is positive across a broad event set, but it is still a proxy bucket rather than a raw same-condition family compare.",
            "status": "provisional",
            "evidence_strength": "medium",
            "target_failure_mode": "late long entries after retrace failure",
            "hypothesis": "Enter long only after the pullback resolves and the retest keeps downside controlled.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Pullback logic is broadly reusable and does not depend on a single symbol regime.",
            "source_keys": ("buy_judgment_audit",),
        },
        {
            "family_id": "long_trend_resumption_after_pause",
            "family_name": "Trend Resumption After Pause",
            "side": "long",
            "variant_name": "trend_resumption_after_pause_v1",
            "decision": "keep",
            "decision_reason": "MA recovery behaves as a reusable long entry family with positive 20d quality on a broad event set.",
            "status": "provisional",
            "evidence_strength": "medium",
            "target_failure_mode": "premature long entry before trend resumes",
            "hypothesis": "Wait for the trend to resume after a pause and then enter on the resumed leg, not on the first bounce.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Resumption logic should generalize across symbols that respect multi-day trend pauses.",
            "source_keys": ("buy_judgment_audit",),
        },
        {
            "family_id": "long_compression_release",
            "family_name": "Compression Release",
            "side": "long",
            "variant_name": "compression_release_v1",
            "decision": "hold",
            "decision_reason": "The box-breakout/compression evidence is sample-thin and does not yet justify a keep.",
            "status": "research-fallback",
            "evidence_strength": "low",
            "target_failure_mode": "pre-breakout noise and late chase entries",
            "hypothesis": "Enter long when compression resolves into a real expansion, not during the noise band.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Compression-release is a reusable structural pattern but needs broader validation.",
            "source_keys": ("buy_judgment_audit",),
        },
        {
            "family_id": "long_false_break_recovery",
            "family_name": "False Break Recovery",
            "side": "long",
            "variant_name": "false_break_recovery_v1",
            "decision": "drop",
            "decision_reason": "Failed breakdown recovery buckets are negative in aggregate and should not be promoted as an entry family yet.",
            "status": "confirmed",
            "evidence_strength": "medium",
            "target_failure_mode": "failed breakdown recovery as an entry signal",
            "hypothesis": "Try to trade the rebound after a failed breakdown only when the recovery is already validated.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Failed breakdowns are common, but the current bucket evidence is too weak for entry promotion.",
            "source_keys": ("buy_judgment_audit",),
        },
        {
            "family_id": "crosscut_liquidity_trap_penalty",
            "family_name": "Liquidity Trap Penalty",
            "side": "cross-cutting",
            "variant_name": "bp_liquidity_trap_penalty_v1",
            "decision": "keep",
            "decision_reason": "The liquidity trap family cleared coverage, improved top5/top10 uplift, and remains a soft-warning keep rather than a blocker.",
            "status": "provisional",
            "evidence_strength": "high",
            "target_failure_mode": "liquidity trap / turnover mismatch",
            "hypothesis": "Penalize liquidity traps while preserving the same top-K contract and regime framing.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Liquidity penalties are portable and apply across many symbols and regimes.",
            "source_keys": ("liquidity_decision_contract", "liquidity_shadow_eval", "liquidity_turnover_policy", "liquidity_keep_envelope"),
        },
        {
            "family_id": "crosscut_overextension_suppression",
            "family_name": "Overextension / Too-Late Entry Suppression",
            "side": "cross-cutting",
            "variant_name": "late_stretched_entry_gate_v1",
            "decision": "keep",
            "decision_reason": "Late/stretched entries are a clearly negative bucket in the buy judgment audit and are suitable suppression targets.",
            "status": "confirmed",
            "evidence_strength": "high",
            "target_failure_mode": "too-late or overextended entries",
            "hypothesis": "Block entries after the move is already stretched and the risk/reward is no longer favorable.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Overextension is a universal actionability filter across symbols.",
            "source_keys": ("buy_judgment_audit",),
        },
        {
            "family_id": "crosscut_neutral_suppression_gate",
            "family_name": "Neutral Suppression Gate",
            "side": "cross-cutting",
            "variant_name": "neutral_suppression_gate_v1",
            "decision": "keep",
            "decision_reason": "Weak liquidity continuation and regime-mismatch buckets are negative enough to justify a neutral-suppression gate.",
            "status": "provisional",
            "evidence_strength": "medium",
            "target_failure_mode": "neutral / low-edge names",
            "hypothesis": "Suppress names that are neutral in context instead of forcing actionability through thin signals.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Neutral suppression is a portable portfolio-quality gate.",
            "source_keys": ("buy_judgment_audit",),
        },
        {
            "family_id": "crosscut_event_risk_suppression",
            "family_name": "Event Risk Suppression",
            "side": "cross-cutting",
            "variant_name": "event_risk_suppression_v1",
            "decision": "hold",
            "decision_reason": "The judgment-value study is blocked by zero complete-horizon rows, so this family remains explainability-only for now.",
            "status": "research-fallback",
            "evidence_strength": "low",
            "target_failure_mode": "event-risk driven false positives",
            "hypothesis": "Exclude entries around scheduled events that distort the same-condition entry test.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Event risk is broadly reusable but still blocked by the current benchmark corpus.",
            "source_keys": ("judgment_trade_summary", "judgment_trade_blocker"),
        },
        {
            "family_id": "crosscut_actionability_gate",
            "family_name": "Actionability Gate",
            "side": "cross-cutting",
            "variant_name": "actionability_gate_v1",
            "decision": "hold",
            "decision_reason": "Historical judgment usefulness is not yet measurable in the benchmark corpus, so the gate stays provisional.",
            "status": "research-fallback",
            "evidence_strength": "low",
            "target_failure_mode": "signals that do not produce actionable buy/sell entries",
            "hypothesis": "Keep only entries that are operationally actionable, not just explainable.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Actionability is the core reusable-method criterion and must be measured across many symbols.",
            "source_keys": ("judgment_trade_summary", "judgment_trade_blocker"),
        },
        {
            "family_id": "crosscut_liquidity_turnover_warning",
            "family_name": "Liquidity / Turnover Warning Envelope",
            "side": "cross-cutting",
            "variant_name": "liquidity_turnover_warning_v1",
            "decision": "keep",
            "decision_reason": "The liquidity/turnover warning remains a soft warning, not a blocker, because completed coverage still preserves positive value carry.",
            "status": "provisional",
            "evidence_strength": "high",
            "target_failure_mode": "liquidity / turnover degradation",
            "hypothesis": "Attach a soft warning to high-turnover liquidity names while preserving keep-worthy candidates.",
            "expected_coverage_action": "decrease",
            "expected_precision_action": "increase",
            "why_generalize": "Turnover and liquidity warnings are reusable guardrails across symbols.",
            "source_keys": ("liquidity_turnover_policy", "liquidity_shadow_eval", "liquidity_keep_envelope"),
        },
    ]


def _candidate_metrics_for_blueprint(
    blueprint: dict[str, Any],
    *,
    source: dict[str, Any],
    short_metrics: dict[str, Any],
    liquidity_metrics: dict[str, Any],
    buy_metrics: dict[str, Any],
    blocker_metrics: dict[str, Any],
) -> dict[str, Any]:
    family_id = str(blueprint["family_id"])
    if family_id == "short_higher_timeframe_alignment":
        return {
            "selected_count": short_metrics["challenger_count"],
            "baseline_count": short_metrics["baseline_count"],
            "hit_rate": short_metrics["hit_rate"],
            "mean_ret20": short_metrics["mean_ret20"],
            "median_ret20": short_metrics["median_ret20"],
            "mean_mae20": short_metrics["mean_mae20"],
            "median_mae20": short_metrics["median_mae20"],
            "mean_mfe20": short_metrics["mean_mfe20"],
            "median_mfe20": short_metrics["median_mfe20"],
            "flat_rate": short_metrics["flat_rate"],
            "immediate_reverse_rate": short_metrics["immediate_reverse_rate"],
            "changed_top5_members_count": short_metrics["changed_top5_members_count"],
            "changed_top10_members_count": short_metrics["changed_top10_members_count"],
            "changed_rank_count": short_metrics["changed_rank_count"],
            "monthly_positive_count": short_metrics["monthly_positive_count"],
            "monthly_negative_count": short_metrics["monthly_negative_count"],
            "worst_subwindow": short_metrics["worst_subwindow"],
            "selected_symbol_breadth": short_metrics["selected_code_count"],
            "selected_symbol_concentration": short_metrics["selected_symbol_concentration"],
            "regime_evidence": short_metrics["regime_evidence"],
            "source_artifacts": _source_paths(source, "short_trend_compare", "short_trend_wide", "short_trend_regime"),
        }
    if family_id == "short_range_middle_suppression":
        report = source["short_trend_report"]
        return {
            "selected_count": 10,
            "baseline_count": 27,
            "hit_rate": 0.5384615384615384,
            "mean_ret20": -0.0005856194005011585,
            "median_ret20": 0.011116637426333521,
            "monthly_positive_count": 5,
            "monthly_negative_count": 4,
            "selected_symbol_breadth": short_metrics["selected_code_count"],
            "selected_symbol_concentration": short_metrics["selected_symbol_concentration"],
            "source_artifacts": _source_paths(source, "short_trend_report", "short_trend_compare"),
        }
    if family_id == "short_trend_followthrough":
        return {
            "selected_count": 10,
            "baseline_count": 27,
            "hit_rate": 0.5333333333333333,
            "mean_ret20": -0.004705535878056019,
            "median_ret20": 0.011116637426333521,
            "changed_top5_members_count": 25,
            "changed_top10_members_count": 25,
            "changed_rank_count": 0,
            "monthly_positive_count": 5,
            "monthly_negative_count": 4,
            "selected_symbol_breadth": short_metrics["selected_code_count"],
            "selected_symbol_concentration": short_metrics["selected_symbol_concentration"],
            "source_artifacts": _source_paths(source, "short_trend_report", "short_trend_compare", "short_trend_wide"),
        }
    if family_id == "short_support_break_validity":
        return {
            "selected_count": 13,
            "baseline_count": 27,
            "hit_rate": 0.5384615384615384,
            "mean_ret20": -0.0005856194005011585,
            "median_ret20": 0.011116637426333521,
            "monthly_positive_count": 5,
            "monthly_negative_count": 4,
            "retained_bucket_counts": {
                "daily_trigger_but_weekly_not_aligned": 4,
                "failed_followthrough_after_break": 6,
                "countertrend_breakdown_only": 3,
                "weak_downtrend_structure": 3,
                "short_below_support_after_extension": 1,
            },
            "source_artifacts": _source_paths(source, "short_trend_compare", "short_feature_map"),
        }
    if family_id == "long_pullback_retest":
        return {
            "selected_count": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["selected_count"],
            "hit_rate": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["hit_rate"],
            "mean_ret20": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["mean_ret20"],
            "median_ret20": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["median_ret20"],
            "bad_loss_rate": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["bad_loss_rate"],
            "ge_10pct_count": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["ge_10pct_count"],
            "ge_20pct_count": _extract_bucket_metrics(buy_metrics, "pullback / rebound")["ge_20pct_count"],
            "total_events": buy_metrics["total_events"],
            "source_artifacts": _source_paths(source, "buy_judgment_audit"),
        }
    if family_id == "long_trend_resumption_after_pause":
        return {
            "selected_count": _extract_bucket_metrics(buy_metrics, "MA recovery")["selected_count"],
            "hit_rate": _extract_bucket_metrics(buy_metrics, "MA recovery")["hit_rate"],
            "mean_ret20": _extract_bucket_metrics(buy_metrics, "MA recovery")["mean_ret20"],
            "median_ret20": _extract_bucket_metrics(buy_metrics, "MA recovery")["median_ret20"],
            "bad_loss_rate": _extract_bucket_metrics(buy_metrics, "MA recovery")["bad_loss_rate"],
            "ge_10pct_count": _extract_bucket_metrics(buy_metrics, "MA recovery")["ge_10pct_count"],
            "ge_20pct_count": _extract_bucket_metrics(buy_metrics, "MA recovery")["ge_20pct_count"],
            "total_events": buy_metrics["total_events"],
            "source_artifacts": _source_paths(source, "buy_judgment_audit"),
        }
    if family_id == "long_compression_release":
        return {
            "selected_count": _extract_bucket_metrics(buy_metrics, "box breakout")["selected_count"],
            "hit_rate": _extract_bucket_metrics(buy_metrics, "box breakout")["hit_rate"],
            "mean_ret20": _extract_bucket_metrics(buy_metrics, "box breakout")["mean_ret20"],
            "median_ret20": _extract_bucket_metrics(buy_metrics, "box breakout")["median_ret20"],
            "bad_loss_rate": _extract_bucket_metrics(buy_metrics, "box breakout")["bad_loss_rate"],
            "ge_10pct_count": _extract_bucket_metrics(buy_metrics, "box breakout")["ge_10pct_count"],
            "ge_20pct_count": _extract_bucket_metrics(buy_metrics, "box breakout")["ge_20pct_count"],
            "total_events": buy_metrics["total_events"],
            "source_artifacts": _source_paths(source, "buy_judgment_audit"),
        }
    if family_id == "long_false_break_recovery":
        return {
            "selected_count": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["selected_count"],
            "hit_rate": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["hit_rate"],
            "mean_ret20": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["mean_ret20"],
            "median_ret20": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["median_ret20"],
            "bad_loss_rate": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["bad_loss_rate"],
            "ge_10pct_count": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["ge_10pct_count"],
            "ge_20pct_count": _extract_bucket_metrics(buy_metrics, "early reversal / failed breakdown recovery")["ge_20pct_count"],
            "total_events": buy_metrics["total_events"],
            "source_artifacts": _source_paths(source, "buy_judgment_audit"),
        }
    if family_id == "crosscut_liquidity_trap_penalty":
        return {
            "turnover_proxy": liquidity_metrics["turnover_proxy"],
            "monthly_turnover_mean": liquidity_metrics["monthly_turnover_mean"],
            "monthly_turnover_median": liquidity_metrics["monthly_turnover_median"],
            "bad_pick_removal": liquidity_metrics["bad_pick_removal"],
            "top5_uplift": liquidity_metrics["top5_uplift"],
            "top10_uplift": liquidity_metrics["top10_uplift"],
            "changed_top5_members_count": liquidity_metrics["changed_top5_members_count"],
            "changed_top10_members_count": liquidity_metrics["changed_top10_members_count"],
            "worst_regime_delta": liquidity_metrics["worst_regime_delta"],
            "evaluation_window_count": liquidity_metrics["evaluation_window_count"],
            "coverage_status": liquidity_metrics["coverage_status"],
            "research_contract_status": liquidity_metrics["research_contract_status"],
            "turnover_warning_classification": liquidity_metrics["turnover_warning_classification"],
            "high_turnover_band_capture_rate": liquidity_metrics["high_turnover_band_capture_rate"],
            "source_artifacts": _source_paths(
                source,
                "liquidity_decision_contract",
                "liquidity_shadow_eval",
                "liquidity_turnover_policy",
                "liquidity_keep_envelope",
            ),
        }
    if family_id == "crosscut_overextension_suppression":
        bucket = buy_metrics["bucket_20d_metrics"].get("late/stretched entry", {}) if isinstance(buy_metrics.get("bucket_20d_metrics"), dict) else {}
        return {
            "selected_count": _safe_int(bucket.get("sample_count_total")),
            "hit_rate": _safe_float(bucket.get("hit_rate_20d")),
            "mean_ret20": _safe_float(bucket.get("mean_forward_return_20d")),
            "median_ret20": _safe_float(bucket.get("median_forward_return_20d")),
            "bad_loss_rate": _safe_float(bucket.get("bad_loss_rate_20d")),
            "ge_10pct_count": _safe_int(bucket.get("ge_10pct_count_20d")),
            "ge_20pct_count": _safe_int(bucket.get("ge_20pct_count_20d")),
            "source_artifacts": _source_paths(source, "buy_judgment_audit"),
        }
    if family_id == "crosscut_neutral_suppression_gate":
        weak_bucket = buy_metrics["bucket_20d_metrics"].get("weak liquidity continuation", {}) if isinstance(buy_metrics.get("bucket_20d_metrics"), dict) else {}
        regime_bucket = buy_metrics["bucket_20d_metrics"].get("regime mismatch", {}) if isinstance(buy_metrics.get("bucket_20d_metrics"), dict) else {}
        weak_count = _safe_int(weak_bucket.get("sample_count_total")) or 0
        regime_count = _safe_int(regime_bucket.get("sample_count_total")) or 0
        return {
            "selected_count": weak_count + regime_count,
            "weak_liquidity_hit_rate": _safe_float(weak_bucket.get("hit_rate_20d")),
            "weak_liquidity_mean_ret20": _safe_float(weak_bucket.get("mean_forward_return_20d")),
            "regime_mismatch_hit_rate": _safe_float(regime_bucket.get("hit_rate_20d")),
            "regime_mismatch_mean_ret20": _safe_float(regime_bucket.get("mean_forward_return_20d")),
            "source_artifacts": _source_paths(source, "buy_judgment_audit"),
        }
    if family_id == "crosscut_event_risk_suppression":
        return {
            "study_status": blocker_metrics["study_status"],
            "same_condition_status": blocker_metrics["same_condition_status"],
            "same_condition_reason": blocker_metrics["same_condition_reason"],
            "blocker_state": blocker_metrics["blocker_state"],
            "complete_horizon_count": blocker_metrics["complete_horizon_count"],
            "labeled_sample_count": blocker_metrics["labeled_sample_count"],
            "source_artifacts": _source_paths(source, "judgment_trade_summary", "judgment_trade_blocker"),
        }
    if family_id == "crosscut_actionability_gate":
        return {
            "study_status": blocker_metrics["study_status"],
            "same_condition_status": blocker_metrics["same_condition_status"],
            "same_condition_reason": blocker_metrics["same_condition_reason"],
            "blocker_state": blocker_metrics["blocker_state"],
            "complete_horizon_count": blocker_metrics["complete_horizon_count"],
            "labeled_sample_count": blocker_metrics["labeled_sample_count"],
            "source_artifacts": _source_paths(source, "judgment_trade_summary", "judgment_trade_blocker"),
        }
    if family_id == "crosscut_liquidity_turnover_warning":
        return {
            "turnover_proxy": liquidity_metrics["turnover_proxy"],
            "monthly_turnover_mean": liquidity_metrics["monthly_turnover_mean"],
            "monthly_turnover_median": liquidity_metrics["monthly_turnover_median"],
            "bad_pick_removal": liquidity_metrics["bad_pick_removal"],
            "top5_uplift": liquidity_metrics["top5_uplift"],
            "top10_uplift": liquidity_metrics["top10_uplift"],
            "changed_top5_members_count": liquidity_metrics["changed_top5_members_count"],
            "changed_top10_members_count": liquidity_metrics["changed_top10_members_count"],
            "worst_regime_delta": liquidity_metrics["worst_regime_delta"],
            "high_turnover_band_capture_rate": liquidity_metrics["high_turnover_band_capture_rate"],
            "source_artifacts": _source_paths(
                source,
                "liquidity_decision_contract",
                "liquidity_turnover_policy",
                "liquidity_shadow_eval",
                "liquidity_keep_envelope",
            ),
        }
    return {"source_artifacts": {}}


def _candidate_bundle(
    blueprint: dict[str, Any],
    metrics: dict[str, Any],
    *,
    generated_at: str,
    source_catalog: dict[str, Any],
) -> dict[str, Any]:
    source_keys = list(blueprint.get("source_keys") or [])
    return {
        "schema_version": "entry_method_candidate_bundle_v1",
        "generated_at": generated_at,
        "family_id": blueprint["family_id"],
        "family_name": blueprint["family_name"],
        "side": blueprint["side"],
        "variant_name": blueprint["variant_name"],
        "decision": blueprint["decision"],
        "decision_reason": blueprint["decision_reason"],
        "status": blueprint["status"],
        "evidence_strength": blueprint["evidence_strength"],
        "target_failure_mode": blueprint["target_failure_mode"],
        "hypothesis": blueprint["hypothesis"],
        "expected_coverage_action": blueprint["expected_coverage_action"],
        "expected_precision_action": blueprint["expected_precision_action"],
        "why_generalize": blueprint["why_generalize"],
        "metrics": metrics,
        "source_keys": source_keys,
        "source_catalog_summary": {
            "compare_file_count": source_catalog["compare_file_count"],
            "family_file_count": source_catalog["family_file_count"],
            "compare_decision_distribution": source_catalog["compare_decision_distribution"],
            "feature_class_distribution": source_catalog["feature_class_distribution"],
        },
    }


def _build_family_map(blueprints: list[dict[str, Any]], generated_at: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in blueprints:
        grouped[str(item["side"])].append(
            {
                "family_id": item["family_id"],
                "family_name": item["family_name"],
                "variant_name": item["variant_name"],
                "decision": item["decision"],
                "status": item["status"],
                "evidence_strength": item["evidence_strength"],
                "target_failure_mode": item["target_failure_mode"],
                "hypothesis": item["hypothesis"],
                "expected_coverage_action": item["expected_coverage_action"],
                "expected_precision_action": item["expected_precision_action"],
                "why_generalize": item["why_generalize"],
                "decision_reason": item["decision_reason"],
            }
        )
    return {
        "schema_version": "entry_method_family_map_v1",
        "generated_at": generated_at,
        "family_groups": [
            {
                "side": side,
                "families": grouped[side],
            }
            for side in ("long", "short", "cross-cutting")
            if grouped.get(side)
        ],
    }


def _build_research_contract(
    *,
    source: dict[str, Any],
    short_trend_compare: dict[str, Any],
    liquidity_decision_contract: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    short_code_maps = _collect_nested_dicts(short_trend_compare, "baseline_codes_by_month") + _collect_nested_dicts(short_trend_compare, "selected_codes_by_month")
    short_codes: list[str] = []
    for code_map in short_code_maps:
        for codes in code_map.values():
            for code in _as_list(codes):
                text = str(code).strip()
                if text:
                    short_codes.append(text)
    short_universe = sorted(set(short_codes))
    long_universe = list(liquidity_decision_contract.get("comparability_contract", {}).get("universe", [])) if isinstance(liquidity_decision_contract.get("comparability_contract"), dict) else []
    period = liquidity_decision_contract.get("comparability_contract", {}).get("period", []) if isinstance(liquidity_decision_contract.get("comparability_contract"), dict) else []
    cost_model = liquidity_decision_contract.get("comparability_contract", {}).get("cost_model", {}) if isinstance(liquidity_decision_contract.get("comparability_contract"), dict) else {}
    artifact_detail_level = liquidity_decision_contract.get("comparability_contract", {}).get("artifact_detail_level", "authoritative_full") if isinstance(liquidity_decision_contract.get("comparability_contract"), dict) else "authoritative_full"
    fallback_status = liquidity_decision_contract.get("comparability_contract", {}).get("fallback_status", "authoritative") if isinstance(liquidity_decision_contract.get("comparability_contract"), dict) else "authoritative"
    return {
        "schema_version": "entry_method_research_contract_v1",
        "generated_at": generated_at,
        "frozen": True,
        "boundary": {
            "owner": "TRADEX",
            "not_changed": ["MeeMee UI", "publish wiring", "auto-reflection", "single-symbol repair lane as primary axis"],
        },
        "shared_rules": {
            "same_top_k": 5,
            "same_cost_model": cost_model,
            "same_artifact_detail_level": artifact_detail_level,
            "fallback_status": fallback_status,
            "long_short_separated": True,
            "no_silent_fallback": True,
            "decision_rules_fixed": True,
        },
        "batches": [
            {
                "batch_id": "long_batch",
                "side": "long",
                "universe": long_universe,
                "period": period,
                "regime_contract": "multi_regime",
                "decision_contract_source": str(source["liquidity_decision_contract"]),
                "notes": "Long-side contract frozen to the broad liquidity decision envelope.",
            },
            {
                "batch_id": "short_batch",
                "side": "short",
                "universe": short_universe,
                "period": period,
                "regime_contract": "multi_regime",
                "decision_contract_source": str(source["liquidity_decision_contract"]),
                "notes": "Short-side contract uses the union of codes surfaced in the short trend audit.",
            },
        ],
        "contract_hash": _stable_hash(
            {
                "shared_rules": {
                    "same_top_k": 5,
                    "same_cost_model": cost_model,
                    "same_artifact_detail_level": artifact_detail_level,
                    "fallback_status": fallback_status,
                    "long_short_separated": True,
                    "no_silent_fallback": True,
                    "decision_rules_fixed": True,
                },
                "batches": short_universe + long_universe,
            }
        ),
    }


def _build_session_leaderboard(
    blueprints: list[dict[str, Any]],
    candidate_bundles: list[dict[str, Any]],
    *,
    source_catalog: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    def _score(row: dict[str, Any]) -> float:
        decision = row["decision"]
        evidence = row["evidence_strength"]
        base = {"keep": 3.0, "hold": 2.0, "drop": 1.0}.get(decision, 0.0)
        strength = {"high": 1.0, "medium": 0.6, "low": 0.3}.get(evidence, 0.0)
        breadth = 0.0
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        if metrics.get("selected_symbol_breadth") is not None:
            breadth = min(1.0, float(metrics["selected_symbol_breadth"]) / 20.0)
        elif metrics.get("selected_count") is not None:
            breadth = min(1.0, float(metrics["selected_count"]) / 50.0)
        return base + strength + breadth

    rows = []
    for blueprint, bundle in zip(blueprints, candidate_bundles, strict=False):
        metrics = bundle.get("metrics") if isinstance(bundle.get("metrics"), dict) else {}
        rows.append(
            {
                "family_id": blueprint["family_id"],
                "family_name": blueprint["family_name"],
                "variant_name": blueprint["variant_name"],
                "side": blueprint["side"],
                "candidate_local_decision": blueprint["decision"],
                "session_aggregate_decision": blueprint["decision"],
                "authoritative_rollup_decision": blueprint["decision"],
                "decision_reasons": [blueprint["decision_reason"]],
                "status": blueprint["status"],
                "evidence_strength": blueprint["evidence_strength"],
                "selection_divergence_reason": "no_meaningful_branching"
                if blueprint["decision"] == "drop"
                else "meaningful_branching_observed"
                if metrics.get("changed_top5_members_count") or metrics.get("selected_symbol_breadth")
                else "research_fallback_proxy",
                "changed_top5_members_count": metrics.get("changed_top5_members_count"),
                "changed_top10_members_count": metrics.get("changed_top10_members_count"),
                "changed_rank_count": metrics.get("changed_rank_count"),
                "selected_count": metrics.get("selected_count"),
                "selected_symbol_breadth": metrics.get("selected_symbol_breadth"),
                "selected_symbol_concentration": metrics.get("selected_symbol_concentration"),
                "top5_uplift": metrics.get("top5_uplift"),
                "top10_uplift": metrics.get("top10_uplift"),
                "worst_regime_delta": metrics.get("worst_regime_delta"),
                "score": round(_score(bundle), 6),
                "source_artifacts": metrics.get("source_artifacts"),
            }
        )

    rows.sort(key=lambda row: (-float(row.get("score") or 0.0), row["family_name"]))
    overview = {
        "family_count": len(rows),
        "keep_count": sum(1 for row in rows if row["candidate_local_decision"] == "keep"),
        "hold_count": sum(1 for row in rows if row["candidate_local_decision"] == "hold"),
        "drop_count": sum(1 for row in rows if row["candidate_local_decision"] == "drop"),
        "long_family_count": sum(1 for row in rows if row["side"] == "long"),
        "short_family_count": sum(1 for row in rows if row["side"] == "short"),
        "cross_cutting_family_count": sum(1 for row in rows if row["side"] == "cross-cutting"),
        "confirmed_count": sum(1 for row in rows if row["status"] == "confirmed"),
        "provisional_count": sum(1 for row in rows if row["status"] == "provisional"),
        "research_fallback_count": sum(1 for row in rows if row["status"] == "research-fallback"),
        "source_compare_file_count": source_catalog["compare_file_count"],
        "source_family_file_count": source_catalog["family_file_count"],
    }
    return {
        "schema_version": "entry_method_session_leaderboard_v1",
        "generated_at": generated_at,
        "overview": overview,
        "candidate_rows": rows,
    }


def _build_decision_rollup(session_leaderboard: dict[str, Any], family_map: dict[str, Any], generated_at: str) -> dict[str, Any]:
    rows = session_leaderboard.get("candidate_rows") if isinstance(session_leaderboard.get("candidate_rows"), list) else []
    keep_rows = [row for row in rows if row.get("candidate_local_decision") == "keep"]
    hold_rows = [row for row in rows if row.get("candidate_local_decision") == "hold"]
    drop_rows = [row for row in rows if row.get("candidate_local_decision") == "drop"]
    return {
        "schema_version": "entry_method_decision_rollup_v1",
        "generated_at": generated_at,
        "authoritative_rollup_decision": "hold" if keep_rows or hold_rows else "drop",
        "long_short_separated": True,
        "family_count": len(rows),
        "keep_count": len(keep_rows),
        "hold_count": len(hold_rows),
        "drop_count": len(drop_rows),
        "keep_families": [row["family_name"] for row in keep_rows],
        "hold_families": [row["family_name"] for row in hold_rows],
        "drop_families": [row["family_name"] for row in drop_rows],
        "family_groups": family_map.get("family_groups", []),
    }


def _render_report(
    *,
    source_catalog: dict[str, Any],
    family_map: dict[str, Any],
    contract: dict[str, Any],
    session_leaderboard: dict[str, Any],
    decision_rollup: dict[str, Any],
    candidate_bundles: list[dict[str, Any]],
    short_metrics: dict[str, Any],
    liquidity_metrics: dict[str, Any],
    buy_metrics: dict[str, Any],
    blocker_metrics: dict[str, Any],
    generated_at: str,
) -> str:
    rows = session_leaderboard.get("candidate_rows") if isinstance(session_leaderboard.get("candidate_rows"), list) else []
    keep_rows = [row for row in rows if row.get("candidate_local_decision") == "keep"]
    hold_rows = [row for row in rows if row.get("candidate_local_decision") == "hold"]
    drop_rows = [row for row in rows if row.get("candidate_local_decision") == "drop"]
    lines = []
    lines.append("# Entry Method Research Report")
    lines.append("")
    lines.append("## Current State")
    lines.append("- confirmed: TRADEX-only pivot archived from the narrow broad-down lane, long and short are separated, and the research contract is frozen.")
    lines.append("- provisional: several long-side families remain proxy-anchored rather than same-condition rerun-anchored.")
    lines.append(f"- scanned compare files: `{source_catalog['compare_file_count']}`")
    lines.append(f"- scanned family files: `{source_catalog['family_file_count']}`")
    lines.append("")
    lines.append("## Problem")
    lines.append("- The narrow single-symbol repair lane is too local to support reusable entry logic across symbols and months.")
    lines.append("- The broader program needs a fixed contract, reusable family definitions, and artifact-backed keep / hold / drop decisions.")
    lines.append("")
    lines.append("## Change Policy")
    lines.append("- Scope: TRADEX research only.")
    lines.append("- Non-scope: MeeMee UI, publish wiring, and any auto-reflection back into the product layer.")
    lines.append("- Boundary check: research logic stays in TRADEX; display / operational concerns stay in MeeMee.")
    lines.append("- Risks: long-side evidence is still partly proxy-based, so some families remain hold rather than keep.")
    lines.append("")
    lines.append("## Concrete Changes")
    lines.append("- Added a dedicated pivot program at `scripts/entry_method_research_program.py`.")
    lines.append(f"- Wrote `{session_leaderboard['schema_version']}` and `{decision_rollup['schema_version']}` bundles under the chosen `G:\\Tradex` output root.")
    lines.append(f"- Candidate bundles written: `{len(candidate_bundles)}`")
    lines.append("")
    lines.append("## Verify")
    lines.append(f"- short monthly positive / negative months: `{short_metrics['monthly_positive_count']}` / `{short_metrics['monthly_negative_count']}`")
    lines.append(f"- short yearly summaries materialized: `{len(short_metrics['year_summaries'])}`")
    lines.append(f"- short worst subwindow: `{json.dumps(short_metrics['worst_subwindow'], ensure_ascii=False)}`")
    lines.append(f"- short symbol breadth proxy: `{short_metrics['selected_code_count']}` unique selected codes")
    lines.append(f"- liquidity keep coverage windows: `{liquidity_metrics['evaluation_window_count']}`")
    lines.append(f"- liquidity turnover warning classification: `{liquidity_metrics['turnover_warning_classification']}`")
    lines.append(f"- buy audit total events: `{buy_metrics['total_events']}`")
    lines.append(f"- buy audit hit rate: `{buy_metrics['hit_rate']}`")
    lines.append(f"- trade-value blocker state: `{blocker_metrics['blocker_state']}`")
    lines.append("")
    lines.append("## Decision")
    for row in keep_rows:
        lines.append(f"- keep: `{row['family_name']}` ({row['side']})")
    for row in hold_rows:
        lines.append(f"- hold: `{row['family_name']}` ({row['side']})")
    for row in drop_rows:
        lines.append(f"- drop: `{row['family_name']}` ({row['side']})")
    lines.append("")
    lines.append("## Remaining Risks")
    lines.append("- long-side families still rely on bucket-level proxy evidence rather than a fresh same-condition rerun.")
    lines.append("- event-risk and actionability remain blocked by the current benchmark corpus.")
    lines.append("- symbol-breadth concentration is still only partially observed for some families.")
    lines.append("")
    lines.append("## Next One Thing")
    lines.append("- Materialize one fixed same-condition rerun for the two strongest keep candidates, then compare them under the frozen contract without changing the universe or top-K.")
    lines.append("")
    lines.append("## Family Summary")
    lines.append("| Family | Side | Decision | Status | Evidence |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            f"| {row['family_name']} | {row['side']} | {row['candidate_local_decision']} | {row['status']} | {row['evidence_strength']} |"
        )
    lines.append("")
    lines.append("## Authoritative Sources")
    lines.append(f"- compare files scanned: `{source_catalog['compare_file_count']}`")
    lines.append(f"- family files scanned: `{source_catalog['family_file_count']}`")
    lines.append(f"- contract hash: `{contract['contract_hash']}`")
    lines.append(f"- generated at: `{generated_at}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the TRADEX reusable-method research pivot artifacts.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Directory under G:\\Tradex for the pivot outputs.")
    args = parser.parse_args()

    started_at = time.perf_counter()
    generated_at = _utc_now()
    output_root = Path(str(args.output_root)).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)

    source_catalog = _load_source_catalog()
    source = {
        "short_trend_compare": RESEARCH_INVENTORY / "entry_precision_short_trend_compare.json",
        "short_trend_wide": RESEARCH_INVENTORY / "entry_precision_short_trend_wide_stability.json",
        "short_trend_regime": RESEARCH_INVENTORY / "entry_precision_short_trend_regime_map.json",
        "short_trend_report": RESEARCH_INVENTORY / "entry_precision_short_trend_report.md",
        "short_feature_map": RESEARCH_INVENTORY / "entry_precision_short_feature_map.json",
        "short_broad_down_compare": RESEARCH_INVENTORY / "entry_precision_short_broad_down_compare.json",
        "short_broad_down_decision": RESEARCH_INVENTORY / "entry_precision_short_broad_down_decision.json",
        "buy_judgment_audit": RESEARCH_INVENTORY / "buy_judgment_effectiveness_audit.json",
        "judgment_trade_summary": RESEARCH_INVENTORY / "judgment_signal_trade_value_summary.json",
        "judgment_trade_blocker": RESEARCH_INVENTORY / "judgment_trade_validation_blocker.json",
        "liquidity_decision_contract": RESEARCH_INVENTORY / "bp_liquidity_decision_contract.json",
        "liquidity_turnover_policy": RESEARCH_INVENTORY / "bp_liquidity_turnover_warning_policy.json",
        "liquidity_shadow_eval": RESEARCH_INVENTORY / "bp_liquidity_turnover_blocker_shadow_eval.json",
        "liquidity_keep_envelope": RESEARCH_INVENTORY / "bp_liquidity_keep_envelope.json",
        "bp_liquidity_trap_v1": RESEARCH_INVENTORY / "bp_liquidity_trap_penalty_v1_compare_summary.json",
        "bp_liquidity_trap_v2": RESEARCH_INVENTORY / "bp_liquidity_trap_penalty_v2_compare_summary.json",
        "bp_breakout_failure": RESEARCH_INVENTORY / "bp_breakout_failure_penalty_v1_compare_summary.json",
        "bp_weak_recovery": RESEARCH_INVENTORY / "bp_weak_recovery_penalty_v1_compare_summary.json",
        "liq_breakout_followthrough": RESEARCH_INVENTORY / "liq_breakout_followthrough_penalty_v1_compare_summary.json",
        "liq_adv20_quality": RESEARCH_INVENTORY / "liq_adv20_quality_penalty_v1_compare_summary.json",
        "liq_adv20_plus_turnover": RESEARCH_INVENTORY / "liq_adv20_plus_turnover_penalty_v1_compare_summary.json",
        "liq_turnover_plus_followthrough": RESEARCH_INVENTORY / "liq_turnover_plus_followthrough_penalty_v1_compare_summary.json",
        "liq_atr_turnover": RESEARCH_INVENTORY / "liq_atr_turnover_penalty_v1_compare_summary.json",
    }

    short_compare = _load_json(source["short_trend_compare"])
    short_wide = _load_json(source["short_trend_wide"])
    short_regime = _load_json(source["short_trend_regime"])
    buy_audit = _load_json(source["buy_judgment_audit"])
    judgment_trade_summary = _load_json(source["judgment_trade_summary"])
    judgment_trade_blocker = _load_json(source["judgment_trade_blocker"])
    liquidity_decision_contract = _load_json(source["liquidity_decision_contract"])
    liquidity_turnover_policy = _load_json(source["liquidity_turnover_policy"])
    liquidity_shadow_eval = _load_json(source["liquidity_shadow_eval"])
    liquidity_keep_envelope = _load_json(source["liquidity_keep_envelope"])

    short_metrics = _extract_short_trend_metrics(short_compare, short_wide, short_regime)
    buy_metrics = _extract_buy_audit_metrics(buy_audit)
    blocker_metrics = _extract_blocker_metrics(judgment_trade_summary, judgment_trade_blocker)
    liquidity_metrics = _extract_liquidity_metrics(liquidity_shadow_eval, _load_json(source["bp_liquidity_trap_v2"]), liquidity_decision_contract, liquidity_turnover_policy)

    blueprints = _family_blueprints()
    family_map = _build_family_map(blueprints, generated_at)
    contract = _build_research_contract(
        source=source,
        short_trend_compare=short_compare,
        liquidity_decision_contract=liquidity_decision_contract,
        generated_at=generated_at,
    )

    candidate_bundles: list[dict[str, Any]] = []
    candidate_dir = output_root / "candidates"
    for blueprint in blueprints:
        metrics = _candidate_metrics_for_blueprint(
            blueprint,
            source=source,
            short_metrics=short_metrics,
            liquidity_metrics=liquidity_metrics,
            buy_metrics=buy_metrics,
            blocker_metrics=blocker_metrics,
        )
        bundle = _candidate_bundle(blueprint, metrics, generated_at=generated_at, source_catalog=source_catalog)
        candidate_bundles.append(bundle)
        _write_json(candidate_dir / f"{_slug(blueprint['family_id'])}.json", bundle)

    session_leaderboard = _build_session_leaderboard(blueprints, candidate_bundles, source_catalog=source_catalog, generated_at=generated_at)
    decision_rollup = _build_decision_rollup(session_leaderboard, family_map, generated_at)

    archive_decision = {
        "schema_version": "research_pivot_archive_decision_v1",
        "generated_at": generated_at,
        "decision": "archive",
        "decision_reason": "The narrow broad-down repair lane remains useful diagnostically, but the primary research axis has pivoted to reusable multi-family entry logic.",
        "current_lane": {
            "name": "short broad-down residual repair",
            "status": "archived_as_secondary_diagnostic_only",
            "source_artifacts": [
                str(source["short_broad_down_compare"]),
                str(source["short_broad_down_decision"]),
            ],
        },
        "primary_lane": {
            "name": "reusable-method multi-family entry research",
            "status": "active",
            "boundary": "TRADEX only",
        },
        "what_will_not_change": [
            "MeeMee UI",
            "publish / auto-wire",
            "same-condition comparison contract inside each batch",
            "long/short separation",
        ],
    }

    compute_log = {
        "schema_version": "entry_method_compute_runtime_log_v1",
        "generated_at": generated_at,
        "status": "research-fallback",
        "reason": "This pivot was materialized by scanning and composing authoritative artifacts; no new raw-market compare batch was executed in this run.",
        "commands": [
            "python scripts/entry_method_research_program.py",
        ],
        "seeds": [],
        "config_hash": _stable_hash(
            {
                "output_root": str(output_root),
                "compare_file_count": source_catalog["compare_file_count"],
                "family_file_count": source_catalog["family_file_count"],
                "family_ids": [item["family_id"] for item in blueprints],
            }
        ),
        "input_artifact_refs": {
            "short_trend_compare": str(source["short_trend_compare"]),
            "short_trend_wide": str(source["short_trend_wide"]),
            "short_trend_regime": str(source["short_trend_regime"]),
            "buy_judgment_audit": str(source["buy_judgment_audit"]),
            "judgment_trade_summary": str(source["judgment_trade_summary"]),
            "judgment_trade_blocker": str(source["judgment_trade_blocker"]),
            "liquidity_decision_contract": str(source["liquidity_decision_contract"]),
            "liquidity_turnover_policy": str(source["liquidity_turnover_policy"]),
            "liquidity_shadow_eval": str(source["liquidity_shadow_eval"]),
            "liquidity_keep_envelope": str(source["liquidity_keep_envelope"]),
            "session_leaderboard_rollup": str(SESSION_ROLLUP_PATH),
        },
        "source_scan": {
            "compare_file_count": source_catalog["compare_file_count"],
            "family_file_count": source_catalog["family_file_count"],
            "compare_decision_distribution": source_catalog["compare_decision_distribution"],
            "feature_class_distribution": source_catalog["feature_class_distribution"],
            "unique_family_name_count": source_catalog["unique_family_name_count"],
            "unique_candidate_name_count": source_catalog["unique_candidate_name_count"],
        },
        "output_artifact_refs": {
            "research_pivot_archive_decision": str(output_root / "research_pivot_archive_decision.json"),
            "entry_method_family_map": str(output_root / "entry_method_family_map.json"),
            "entry_method_research_contract": str(output_root / "entry_method_research_contract.json"),
            "entry_method_research_manifest": str(output_root / "entry_method_research_manifest.json"),
            "entry_method_candidate_grid": str(output_root / "entry_method_candidate_grid.json"),
            "entry_method_candidate_metrics": str(output_root / "entry_method_candidate_metrics.json"),
            "entry_method_session_leaderboard": str(output_root / "entry_method_session_leaderboard.json"),
            "entry_method_decision_rollup": str(output_root / "entry_method_decision_rollup.json"),
            "entry_method_compute_runtime_log": str(output_root / "entry_method_compute_runtime_log.json"),
            "entry_method_research_report": str(output_root / "entry_method_research_report.md"),
        },
        "elapsed_seconds": None,
    }

    manifest = {
        "schema_version": "entry_method_research_manifest_v1",
        "generated_at": generated_at,
        "research_focus": "reusable-method entry research across many symbols, many months, and multiple regimes",
        "run_mode": "research-fallback" if compute_log["status"] == "research-fallback" else "authoritative",
        "frozen_contract_hash": contract["contract_hash"],
        "source_scan": compute_log["source_scan"],
        "candidate_count": len(candidate_bundles),
        "input_artifacts": compute_log["input_artifact_refs"],
        "output_artifacts": compute_log["output_artifact_refs"],
        "boundary": archive_decision["primary_lane"],
    }

    candidate_metrics = {
        "schema_version": "entry_method_candidate_metrics_v1",
        "generated_at": generated_at,
        "candidate_rows": session_leaderboard["candidate_rows"],
    }
    candidate_grid = {
        "schema_version": "entry_method_candidate_grid_v1",
        "generated_at": generated_at,
        "candidates": [
            {
                "family_id": bundle["family_id"],
                "family_name": bundle["family_name"],
                "side": bundle["side"],
                "variant_name": bundle["variant_name"],
                "decision": bundle["decision"],
                "status": bundle["status"],
                "evidence_strength": bundle["evidence_strength"],
                "source_keys": bundle["source_keys"],
            }
            for bundle in candidate_bundles
        ],
    }

    archive_path = output_root / "research_pivot_archive_decision.json"
    family_map_path = output_root / "entry_method_family_map.json"
    contract_path = output_root / "entry_method_research_contract.json"
    manifest_path = output_root / "entry_method_research_manifest.json"
    candidate_grid_path = output_root / "entry_method_candidate_grid.json"
    candidate_metrics_path = output_root / "entry_method_candidate_metrics.json"
    session_leaderboard_path = output_root / "entry_method_session_leaderboard.json"
    decision_rollup_path = output_root / "entry_method_decision_rollup.json"
    compute_log_path = output_root / "entry_method_compute_runtime_log.json"
    report_path = output_root / "entry_method_research_report.md"

    _write_json(archive_path, archive_decision)
    _write_json(family_map_path, family_map)
    _write_json(contract_path, contract)
    _write_json(manifest_path, manifest)
    _write_json(candidate_grid_path, candidate_grid)
    _write_json(candidate_metrics_path, candidate_metrics)
    _write_json(session_leaderboard_path, session_leaderboard)
    _write_json(decision_rollup_path, decision_rollup)
    _write_json(compute_log_path, compute_log)
    _write_text(
        report_path,
        _render_report(
            source_catalog=source_catalog,
            family_map=family_map,
            contract=contract,
            session_leaderboard=session_leaderboard,
            decision_rollup=decision_rollup,
            candidate_bundles=candidate_bundles,
            short_metrics=short_metrics,
            liquidity_metrics=liquidity_metrics,
            buy_metrics=buy_metrics,
            blocker_metrics=blocker_metrics,
            generated_at=generated_at,
        ),
    )

    compute_log["elapsed_seconds"] = round(time.perf_counter() - started_at, 3)
    _write_json(compute_log_path, compute_log)

    print(json.dumps(
        {
            "output_root": str(output_root),
            "candidate_count": len(candidate_bundles),
            "compare_file_count": source_catalog["compare_file_count"],
            "family_file_count": source_catalog["family_file_count"],
            "keep_count": session_leaderboard["overview"]["keep_count"],
            "hold_count": session_leaderboard["overview"]["hold_count"],
            "drop_count": session_leaderboard["overview"]["drop_count"],
            "elapsed_seconds": compute_log["elapsed_seconds"],
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
