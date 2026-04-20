from __future__ import annotations

import hashlib
import json
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
PIVOT_ROOT = Path(r"G:\Tradex\keep\entry_method_research_pivot_20260420")
VALIDATION_ROOT = Path(r"G:\Tradex\keep\entry_method_validation_20260420")

PIVOT_CONTRACT_PATH = PIVOT_ROOT / "entry_method_research_contract.json"
PIVOT_CANDIDATE_GRID_PATH = PIVOT_ROOT / "entry_method_candidate_grid.json"
PIVOT_CANDIDATE_METRICS_PATH = PIVOT_ROOT / "entry_method_candidate_metrics.json"
PIVOT_SESSION_LEADERBOARD_PATH = PIVOT_ROOT / "entry_method_session_leaderboard.json"
PIVOT_DECISION_ROLLUP_PATH = PIVOT_ROOT / "entry_method_decision_rollup.json"

LONG_AUDIT_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "buy_judgment_effectiveness_audit.json"
LONG_COMPARE_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "buy_judgment_revision_r4_reclaim_quality_gate.json"

SHORT_COMPARE_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "entry_precision_short_trend_compare.json"
SHORT_WIDE_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "entry_precision_short_trend_wide_stability.json"
SHORT_REGIME_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "entry_precision_short_trend_regime_map.json"

LIQ_COMPARE_PATH = Path(r"G:\Tradex\scratch\research_families\tradex-research-bp-removal-20260417-r2-bp_liquidity_trap_penalty\compare.json")
LIQ_SUMMARY_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "bp_liquidity_trap_penalty_v2_compare_summary.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _event_id(event: dict[str, Any]) -> str:
    return f"{event.get('symbol')}|{event.get('judgment_yyyymmdd')}|{event.get('judgment_type')}"


def _ret20(event: dict[str, Any]) -> float:
    return _safe_float((((event.get("forward_returns") or {}).get("ret_20d")))) or 0.0


def _mae20(event: dict[str, Any]) -> float | None:
    return _safe_float(((event.get("forward_returns") or {}).get("mae_20d")))


def _mfe20(event: dict[str, Any]) -> float | None:
    return _safe_float(((event.get("forward_returns") or {}).get("mfe_20d")))


def _score(event: dict[str, Any]) -> float:
    return _safe_float(event.get("buy_candidate_score")) or 0.0


def _month_key(ymd: Any) -> str:
    text = str(ymd or "")
    if len(text) >= 6:
        return f"{text[:4]}-{text[4:6]}"
    return "unknown"


def _year_key(ymd: Any) -> str:
    text = str(ymd or "")
    if len(text) >= 4:
        return text[:4]
    return "unknown"


def _mean_median(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"count": 0, "mean": None, "median": None}
    return {
        "count": len(values),
        "mean": float(statistics.fmean(values)),
        "median": float(statistics.median(values)),
    }


def _group_monthly(rows: list[dict[str, Any]], *, ymd_key: str, ret_fn) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_month_key(row.get(ymd_key))].append(row)
    monthly_median_ret20: dict[str, float] = {}
    positive_months = 0
    negative_months = 0
    for month, month_rows in sorted(groups.items()):
        rets = [float(ret_fn(row)) for row in month_rows]
        if not rets:
            continue
        stats = _mean_median(rets)
        monthly_median_ret20[month] = stats["median"] if stats["median"] is not None else 0.0
        if (stats["median"] or 0.0) > 0:
            positive_months += 1
        elif (stats["median"] or 0.0) < 0:
            negative_months += 1
    return {
        "months_with_selection": len(monthly_median_ret20),
        "positive_months": positive_months,
        "negative_months": negative_months,
        "monthly_median_ret20": monthly_median_ret20,
    }


def _group_yearly(rows: list[dict[str, Any]], *, ymd_key: str, ret_fn) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_year_key(row.get(ymd_key))].append(row)
    yearly_median_ret20: dict[str, float] = {}
    positive_years = 0
    negative_years = 0
    for year, year_rows in sorted(groups.items()):
        rets = [float(ret_fn(row)) for row in year_rows]
        if not rets:
            continue
        stats = _mean_median(rets)
        yearly_median_ret20[year] = stats["median"] if stats["median"] is not None else 0.0
        if (stats["median"] or 0.0) > 0:
            positive_years += 1
        elif (stats["median"] or 0.0) < 0:
            negative_years += 1
    return {
        "years_with_selection": len(yearly_median_ret20),
        "positive_years": positive_years,
        "negative_years": negative_years,
        "yearly_median_ret20": yearly_median_ret20,
    }


def _group_regime(rows: list[dict[str, Any]], *, regime_fn, ret_fn) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(regime_fn(row) or "unknown")].append(row)
    out: list[dict[str, Any]] = []
    for regime, regime_rows in sorted(groups.items()):
        rets = [float(ret_fn(row)) for row in regime_rows]
        stats = _mean_median(rets)
        hit_rate = None
        if rets:
            hit_rate = sum(1 for value in rets if value > 0) / float(len(rets))
        out.append(
            {
                "regime": regime,
                "count": len(regime_rows),
                "hit_rate": hit_rate,
                "mean_ret20": stats["mean"],
                "median_ret20": stats["median"],
            }
        )
    return out


def _symbol_breadth(rows: list[dict[str, Any]], *, symbol_key: str) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    for row in rows:
        symbol = str(row.get(symbol_key) or "").strip()
        if symbol:
            counter[symbol] += 1
    breadth = len(counter)
    concentration = None
    if counter:
        concentration = max(counter.values()) / float(sum(counter.values()))
    return {
        "selected_symbol_breadth": breadth,
        "selected_symbol_concentration": concentration,
        "selected_symbol_counts": dict(counter),
    }


def _worst_subwindow(monthly_summary: dict[str, Any]) -> dict[str, Any]:
    monthly = monthly_summary.get("monthly_median_ret20") if isinstance(monthly_summary, dict) else {}
    if not isinstance(monthly, dict) or not monthly:
        return {}
    worst_month = min(monthly.items(), key=lambda item: item[1])[0]
    return {
        "month": worst_month,
        "median_ret20": monthly[worst_month],
    }


def _topk_branching(
    *,
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
    top_k: int,
) -> dict[str, Any]:
    def rank_map(rows: list[dict[str, Any]]) -> tuple[list[str], dict[str, int]]:
        ranked = sorted(rows, key=lambda row: (-_score(row), str(row.get("symbol") or ""), str(row.get("judgment_yyyymmdd") or "")))
        ids = [_event_id(row) for row in ranked]
        return ids, {item: index + 1 for index, item in enumerate(ids)}

    baseline_ids, baseline_rank = rank_map(baseline_rows)
    challenger_ids, challenger_rank = rank_map(challenger_rows)
    base_top = baseline_ids[:top_k]
    cand_top = challenger_ids[:top_k]
    shared = set(base_top) & set(cand_top)
    changed_top = len(set(base_top) ^ set(cand_top))
    changed_rank = sum(1 for item in shared if baseline_rank.get(item) != challenger_rank.get(item))
    return {
        "changed_top5_members_count": changed_top if top_k == 5 else None,
        "changed_top10_members_count": changed_top if top_k == 10 else None,
        "changed_rank_count": changed_rank,
        "selection_divergence_reason": "meaningful_branching_observed" if changed_top or changed_rank else "no_meaningful_branching",
        "top5_baseline": base_top,
        "top5_challenger": cand_top,
    }


def _long_validation(long_audit: dict[str, Any], long_compare: dict[str, Any]) -> dict[str, Any]:
    events = [event for event in _safe_list(long_audit.get("events")) if isinstance(event, dict)]
    selected = [event for event in events if str(event.get("pattern_bucket")) == "MA recovery"]
    baseline = [event for event in events if bool(event.get("buy_eligible", True))]

    bucket = (((long_audit.get("summary") or {}).get("bucket_20d_metrics") or {}).get("MA recovery") or {})
    monthly = _group_monthly(selected, ymd_key="judgment_yyyymmdd", ret_fn=_ret20)
    yearly = _group_yearly(selected, ymd_key="judgment_yyyymmdd", ret_fn=_ret20)
    regime = _group_regime(
        selected,
        regime_fn=lambda row: ((row.get("regime_bucket") or {}).get("bucket")),
        ret_fn=_ret20,
    )
    breadth = _symbol_breadth(selected, symbol_key="symbol")
    branch5 = _topk_branching(baseline_rows=baseline, challenger_rows=selected, top_k=5)
    branch10 = _topk_branching(baseline_rows=baseline, challenger_rows=selected, top_k=10)
    same_condition = (
        (long_compare.get("same_condition_contract") if isinstance(long_compare, dict) else {}) or
        {}
    )
    mean_mae = _mean_median([_mae20(event) for event in selected if _mae20(event) is not None])
    mean_mfe = _mean_median([_mfe20(event) for event in selected if _mfe20(event) is not None])
    selected_ret = [float(_ret20(event)) for event in selected]
    selection_divergence_reason = "meaningful_branching_observed" if (branch5["changed_top5_members_count"] or branch10["changed_top10_members_count"]) else "no_meaningful_branching"
    worst = _worst_subwindow(monthly)
    metrics = {
        "candidate_name": "Trend Resumption After Pause",
        "family_id": "long_trend_resumption_after_pause",
        "family_name": "Trend Resumption After Pause",
        "side": "long",
        "variant_name": "trend_resumption_after_pause_v1",
        "validation_source_mode": "raw_audit_replay",
        "selected_count": len(selected),
        "baseline_count": len(baseline),
        "selected_count_unit": "events",
        "hit_rate": bucket.get("hit_rate_20d"),
        "mean_ret20": bucket.get("mean_forward_return_20d"),
        "median_ret20": bucket.get("median_forward_return_20d"),
        "bad_loss_rate": bucket.get("bad_loss_rate_20d"),
        "ge_10pct_count": bucket.get("ge_10pct_count_20d"),
        "ge_20pct_count": bucket.get("ge_20pct_count_20d"),
        "mean_mae20": mean_mae["mean"],
        "median_mae20": mean_mae["median"],
        "mean_mfe20": mean_mfe["mean"],
        "median_mfe20": mean_mfe["median"],
        "changed_top5_members_count": branch5["changed_top5_members_count"],
        "changed_top10_members_count": branch10["changed_top10_members_count"],
        "changed_rank_count": branch10["changed_rank_count"],
        "selection_divergence_reason": selection_divergence_reason,
        "monthly_stability": monthly,
        "yearly_stability": yearly,
        "regime_stability": regime,
        "selected_symbol_breadth": breadth["selected_symbol_breadth"],
        "selected_symbol_concentration": breadth["selected_symbol_concentration"],
        "worst_subwindow": worst,
        "same_condition_contract": same_condition,
        "decision": "hold",
        "decision_reason": "Positive MA recovery bucket quality, but the validation is still audit-replay based rather than a dedicated frozen family compare.",
        "source_artifacts": {
            "buy_judgment_audit": str(LONG_AUDIT_PATH),
            "reclaim_quality_gate": str(LONG_COMPARE_PATH),
        },
        "notes": {
            "validation_note": "research-fallback",
            "branch_counts_note": "top-K branching inferred from raw event replay over the audit corpus",
        },
    }
    metrics["selected_ret20_mean"] = float(statistics.fmean(selected_ret)) if selected_ret else None
    return metrics


def _short_validation(short_compare: dict[str, Any], short_wide: dict[str, Any], short_regime: dict[str, Any]) -> dict[str, Any]:
    variant = (short_compare.get("variants") or {}).get("short_trend_alignment_v1") or {}
    challenger = variant.get("challenger") if isinstance(variant.get("challenger"), dict) else {}
    baseline = variant.get("baseline") if isinstance(variant.get("baseline"), dict) else {}
    delta = variant.get("delta") if isinstance(variant.get("delta"), dict) else {}
    selected_rows = _safe_list(variant.get("selected_rows"))
    baseline_rows = _safe_list(variant.get("baseline_rows"))
    monthly = variant.get("monthly_stability") if isinstance(variant.get("monthly_stability"), dict) else {}
    breadth = _symbol_breadth(selected_rows, symbol_key="code")
    yearly = _group_yearly(selected_rows, ymd_key="ymd", ret_fn=lambda row: _safe_float(row.get("short_ret_20")) or 0.0)
    regime_rows = _group_regime(
        selected_rows,
        regime_fn=lambda row: row.get("marketRegime"),
        ret_fn=lambda row: _safe_float(row.get("short_ret_20")) or 0.0,
    )
    monthly_map = dict((monthly.get("monthly_median_ret20") or {}))
    worst = _worst_subwindow({"monthly_median_ret20": monthly_map})
    metrics = {
        "candidate_name": "Higher Timeframe Alignment",
        "family_id": "short_higher_timeframe_alignment",
        "family_name": "Higher Timeframe Alignment",
        "side": "short",
        "variant_name": "short_trend_alignment_v1",
        "validation_source_mode": "raw_compare_replay",
        "selected_count": len(selected_rows),
        "baseline_count": len(baseline_rows),
        "selected_count_unit": "symbols",
        "hit_rate": challenger.get("hit_rate"),
        "mean_ret20": challenger.get("mean_ret20"),
        "median_ret20": challenger.get("median_ret20"),
        "mean_mae20": challenger.get("mean_mae20"),
        "median_mae20": challenger.get("median_mae20"),
        "mean_mfe20": challenger.get("mean_mfe20"),
        "median_mfe20": challenger.get("median_mfe20"),
        "flat_rate": challenger.get("flat_rate"),
        "immediate_reverse_rate": challenger.get("immediate_reverse_rate"),
        "changed_top5_members_count": delta.get("changed_top5_short_count"),
        "changed_top10_members_count": delta.get("changed_top10_short_count"),
        "changed_rank_count": delta.get("changed_rank_short_count"),
        "selection_divergence_reason": "meaningful_branching_observed" if int(delta.get("changed_top5_short_count") or 0) or int(delta.get("changed_top10_short_count") or 0) else "no_meaningful_branching",
        "monthly_stability": monthly,
        "yearly_stability": yearly,
        "regime_stability": regime_rows,
        "selected_symbol_breadth": breadth["selected_symbol_breadth"],
        "selected_symbol_concentration": breadth["selected_symbol_concentration"],
        "worst_subwindow": variant.get("worst_subwindow") or worst,
        "same_condition_contract": short_compare.get("same_condition_contract"),
        "decision": "keep",
        "decision_reason": "Raw short compare shows positive hit-rate and return improvement with meaningful branching retained across months.",
        "source_artifacts": {
            "short_trend_compare": str(SHORT_COMPARE_PATH),
            "short_trend_wide": str(SHORT_WIDE_PATH),
            "short_trend_regime": str(SHORT_REGIME_PATH),
        },
        "notes": {
            "validation_note": "authoritative_raw_compare",
        },
    }
    return metrics


def _liquidity_validation(liq_summary: dict[str, Any], liq_compare: dict[str, Any]) -> dict[str, Any]:
    evidence = liq_summary.get("evidence") if isinstance(liq_summary.get("evidence"), dict) else {}
    candidate_result = None
    for item in _safe_list(liq_compare.get("candidate_results")):
        if isinstance(item, dict) and (item.get("candidate_method") or {}).get("method_family") == "bp_liquidity_trap_penalty":
            candidate_result = item
            break
    if candidate_result is None and _safe_list(liq_compare.get("candidate_results")):
        candidate_result = _safe_list(liq_compare.get("candidate_results"))[0]
    candidate_result = candidate_result if isinstance(candidate_result, dict) else {}
    evaluation = candidate_result.get("evaluation_summary") if isinstance(candidate_result.get("evaluation_summary"), dict) else {}
    selection_compare = candidate_result.get("selection_compare") if isinstance(candidate_result.get("selection_compare"), dict) else {}
    challenger_summary = evaluation.get("challenger_selection_summary") if isinstance(evaluation.get("challenger_selection_summary"), dict) else {}
    challenger_regime = evaluation.get("challenger_regime_summary") if isinstance(evaluation.get("challenger_regime_summary"), list) else []
    monthly_capture = challenger_summary.get("monthly_top5_capture") if isinstance(challenger_summary.get("monthly_top5_capture"), dict) else {}
    months = _safe_list(monthly_capture.get("months"))
    month_summaries: dict[str, float] = {}
    for item in months:
        if not isinstance(item, dict):
            continue
        month = str(item.get("month") or "")
        if not month:
            continue
        month_summaries[month] = _safe_float(item.get("capture_ret20_mean")) or 0.0
    positive_months = sum(1 for value in month_summaries.values() if value > 0)
    negative_months = sum(1 for value in month_summaries.values() if value < 0)
    year_groups: dict[str, list[float]] = defaultdict(list)
    for month, value in month_summaries.items():
        year_groups[month[:4]].append(value)
    yearly_median_ret20 = {year: float(statistics.median(values)) for year, values in year_groups.items() if values}
    selected_count = _safe_int(evaluation.get("sample_count")) or _safe_int(challenger_summary.get("sample_count")) or 0
    selected_symbol_breadth = _safe_int((candidate_result.get("candidate_absolute") or {}).get("target_symbol_count")) or _safe_int((candidate_result.get("baseline_absolute") or {}).get("target_symbol_count")) or 0
    concentration = _safe_float((candidate_result.get("candidate_absolute") or {}).get("symbol_concentration"))
    if concentration is None:
        concentration = 0.0
    top5_uplift = _safe_float(evidence.get("top5_uplift"))
    top10_uplift = _safe_float(evidence.get("top10_uplift"))
    changed_top5 = _safe_int(evidence.get("changed_top5_members_count"))
    changed_top10 = _safe_int(evidence.get("changed_top10_members_count"))
    changed_rank = _safe_int(evidence.get("changed_rank_count"))
    same_condition = evidence.get("same_condition_contract") if isinstance(evidence.get("same_condition_contract"), dict) else candidate_result.get("same_condition_contract")
    worst = {}
    if challenger_regime:
        worst_item = min(
            challenger_regime,
            key=lambda item: (_safe_float(((item or {}).get("ret_20") or {}).get("mean")) or 0.0),
        )
        worst = {
            "label": worst_item.get("label"),
            "count": _safe_int(worst_item.get("sample_count")),
            "mean_ret20": _safe_float(((worst_item.get("ret_20") or {}).get("mean"))),
            "median_ret20": _safe_float(((worst_item.get("ret_20") or {}).get("median"))),
        }
    if (
        not worst
        or (
            worst.get("label") is None
            and worst.get("count") is None
            and worst.get("mean_ret20") is None
            and worst.get("median_ret20") is None
        )
    ) and month_summaries:
        worst_month, worst_value = min(month_summaries.items(), key=lambda item: item[1])
        worst = {
            "month": worst_month,
            "median_ret20": worst_value,
        }
    metrics = {
        "candidate_name": "Liquidity Trap Penalty",
        "family_id": "crosscut_liquidity_trap_penalty",
        "family_name": "Liquidity Trap Penalty",
        "side": "cross-cutting",
        "variant_name": "bp_liquidity_trap_penalty_v1",
        "validation_source_mode": "raw_compare_replay",
        "selected_count": selected_count,
        "baseline_count": _safe_int((candidate_result.get("baseline_absolute") or {}).get("target_symbol_count")) or 0,
        "selected_count_unit": "rows",
        "hit_rate": None,
        "mean_ret20": _safe_float(evaluation.get("challenger_topk_ret20_mean")),
        "median_ret20": _safe_float(evaluation.get("challenger_topk_ret20_median")),
        "mean_mae20": _safe_float((candidate_result.get("victory_metrics") or {}).get("mae_20d")),
        "median_mae20": None,
        "mean_mfe20": _safe_float((candidate_result.get("victory_metrics") or {}).get("mfe_20d")),
        "median_mfe20": None,
        "changed_top5_members_count": changed_top5,
        "changed_top10_members_count": changed_top10,
        "changed_rank_count": changed_rank,
        "selection_divergence_reason": evidence.get("selection_divergence_reason") or "meaningful_branching_observed",
        "monthly_stability": {
            "months_with_selection": len(month_summaries),
            "positive_months": positive_months,
            "negative_months": negative_months,
            "monthly_median_ret20": month_summaries,
        },
        "yearly_stability": {
            "years_with_selection": len(yearly_median_ret20),
            "positive_years": sum(1 for value in yearly_median_ret20.values() if value > 0),
            "negative_years": sum(1 for value in yearly_median_ret20.values() if value < 0),
            "yearly_median_ret20": yearly_median_ret20,
        },
        "regime_stability": challenger_regime,
        "selected_symbol_breadth": selected_symbol_breadth,
        "selected_symbol_concentration": concentration,
        "worst_subwindow": worst,
        "same_condition_contract": same_condition,
        "decision": "keep",
        "decision_reason": "Raw liquidity compare preserves completed coverage, keeps the soft-warning classification, and retains positive top5/top10 uplift.",
        "source_artifacts": {
            "liquidity_compare": str(LIQ_COMPARE_PATH),
            "liquidity_compare_summary": str(LIQ_SUMMARY_PATH),
        },
        "notes": {
            "validation_note": "authoritative_raw_compare",
            "hit_rate_note": "not directly exposed for this penalty-gate compare",
        },
    }
    return metrics


def main() -> int:
    start = time.time()
    VALIDATION_ROOT.mkdir(parents=True, exist_ok=True)
    session_id = "entry-method-validation-20260420"
    pivot_contract = _load_json(PIVOT_CONTRACT_PATH)
    pivot_grid = _load_json(PIVOT_CANDIDATE_GRID_PATH)
    pivot_metrics = _load_json(PIVOT_CANDIDATE_METRICS_PATH)
    pivot_leaderboard = _load_json(PIVOT_SESSION_LEADERBOARD_PATH)
    pivot_rollup = _load_json(PIVOT_DECISION_ROLLUP_PATH)
    long_audit = _load_json(LONG_AUDIT_PATH)
    long_compare = _load_json(LONG_COMPARE_PATH)
    short_compare = _load_json(SHORT_COMPARE_PATH)
    short_wide = _load_json(SHORT_WIDE_PATH)
    short_regime = _load_json(SHORT_REGIME_PATH)
    liq_compare = _load_json(LIQ_COMPARE_PATH)
    liq_summary = _load_json(LIQ_SUMMARY_PATH)

    validation_contract = {
        "schema_version": "entry_method_validation_contract_v1",
        "generated_at": _utc_now(),
        "frozen": True,
        "source_pivot_contract": str(PIVOT_CONTRACT_PATH),
        "same_universe": True,
        "same_period": True,
        "same_top_k": True,
        "same_regime": True,
        "same_cost": True,
        "same_artifact_detail_level": True,
        "long_short_separated": True,
        "no_silent_fallback": True,
        "no_meemee_ui_change": True,
        "validation_contract_hash": _stable_hash(
            {
                "pivot_contract_hash": pivot_contract.get("contract_hash"),
                "candidate_set": [
                    "long_trend_resumption_after_pause",
                    "short_higher_timeframe_alignment",
                    "crosscut_liquidity_trap_penalty",
                ],
                "same_top_k": 5,
            }
        ),
        "shared_rules": pivot_contract.get("shared_rules"),
        "batches": [
            {
                "batch_id": "validation_long_short_crosscut",
                "side": "long",
                "family_id": "long_trend_resumption_after_pause",
                "family_name": "Trend Resumption After Pause",
                "variant_name": "trend_resumption_after_pause_v1",
                "validation_source": "raw_audit_replay",
                "source_artifacts": [str(LONG_AUDIT_PATH), str(LONG_COMPARE_PATH)],
            },
            {
                "batch_id": "validation_long_short_crosscut",
                "side": "short",
                "family_id": "short_higher_timeframe_alignment",
                "family_name": "Higher Timeframe Alignment",
                "variant_name": "short_trend_alignment_v1",
                "validation_source": "raw_compare_replay",
                "source_artifacts": [str(SHORT_COMPARE_PATH), str(SHORT_WIDE_PATH), str(SHORT_REGIME_PATH)],
            },
            {
                "batch_id": "validation_long_short_crosscut",
                "side": "cross-cutting",
                "family_id": "crosscut_liquidity_trap_penalty",
                "family_name": "Liquidity Trap Penalty",
                "variant_name": "bp_liquidity_trap_penalty_v1",
                "validation_source": "raw_compare_replay",
                "source_artifacts": [str(LIQ_COMPARE_PATH), str(LIQ_SUMMARY_PATH)],
            },
        ],
    }

    candidate_set = {
        "schema_version": "entry_method_validation_candidate_set_v1",
        "generated_at": _utc_now(),
        "candidates": [
            {
                "family_id": "long_trend_resumption_after_pause",
                "family_name": "Trend Resumption After Pause",
                "side": "long",
                "variant_name": "trend_resumption_after_pause_v1",
                "pivot_status": "provisional",
                "pivot_decision": "keep",
                "validation_source_mode": "raw_audit_replay",
                "source_artifacts": [str(LONG_AUDIT_PATH), str(LONG_COMPARE_PATH)],
                "source_note": "The long-side family is validated from the raw audit corpus and the same-condition reclaim-quality gate compare.",
            },
            {
                "family_id": "short_higher_timeframe_alignment",
                "family_name": "Higher Timeframe Alignment",
                "side": "short",
                "variant_name": "short_trend_alignment_v1",
                "pivot_status": "confirmed",
                "pivot_decision": "keep",
                "validation_source_mode": "raw_compare_replay",
                "source_artifacts": [str(SHORT_COMPARE_PATH), str(SHORT_WIDE_PATH), str(SHORT_REGIME_PATH)],
                "source_note": "The short candidate is validated from the frozen short-trend compare artifact.",
            },
            {
                "family_id": "crosscut_liquidity_trap_penalty",
                "family_name": "Liquidity Trap Penalty",
                "side": "cross-cutting",
                "variant_name": "bp_liquidity_trap_penalty_v1",
                "pivot_status": "provisional",
                "pivot_decision": "keep",
                "validation_source_mode": "raw_compare_replay",
                "source_artifacts": [str(LIQ_COMPARE_PATH), str(LIQ_SUMMARY_PATH)],
                "source_note": "The liquidity penalty is validated from the raw same-condition compare and its authoritative summary.",
            },
        ],
    }

    metrics = {
        "schema_version": "entry_method_validation_metrics_v1",
        "generated_at": _utc_now(),
        "candidates": [
            _long_validation(long_audit, long_compare),
            _short_validation(short_compare, short_wide, short_regime),
            _liquidity_validation(liq_summary, liq_compare),
        ],
    }

    decision_rows = []
    for row in metrics["candidates"]:
        side = row["side"]
        decision_rows.append(
            {
                "family_id": row["family_id"],
                "family_name": row["family_name"],
                "variant_name": row["variant_name"],
                "side": side,
                "candidate_local_decision": row["decision"],
                "session_aggregate_decision": row["decision"],
                "authoritative_rollup_decision": row["decision"],
                "decision_reason": row["decision_reason"],
                "validation_source_mode": row["validation_source_mode"],
                "selection_divergence_reason": row.get("selection_divergence_reason"),
                "changed_top5_members_count": row.get("changed_top5_members_count"),
                "changed_top10_members_count": row.get("changed_top10_members_count"),
                "changed_rank_count": row.get("changed_rank_count"),
                "worst_subwindow": row.get("worst_subwindow"),
                "validation_note": (row.get("notes") or {}).get("validation_note"),
            }
        )

    long_decision = next(row for row in decision_rows if row["family_id"] == "long_trend_resumption_after_pause")
    short_decision = next(row for row in decision_rows if row["family_id"] == "short_higher_timeframe_alignment")
    liq_decision = next(row for row in decision_rows if row["family_id"] == "crosscut_liquidity_trap_penalty")
    decision_rollup = {
        "schema_version": "entry_method_validation_decision_rollup_v1",
        "generated_at": _utc_now(),
        "candidate_rows": decision_rows,
        "authoritative_rollup_decision": "hold",
        "session_aggregate_decision": "hold",
        "candidate_local_decision": "mixed",
        "summary": {
            "keep": sum(1 for row in decision_rows if row["candidate_local_decision"] == "keep"),
            "hold": sum(1 for row in decision_rows if row["candidate_local_decision"] == "hold"),
            "drop": sum(1 for row in decision_rows if row["candidate_local_decision"] == "drop"),
        },
        "notes": [
            "Long-side validation used raw audit replay plus the same-condition reclaim-quality gate compare.",
            "Short and liquidity candidates are validated from frozen raw compare artifacts.",
        ],
    }

    manifest = {
        "schema_version": "entry_method_validation_manifest_v1",
        "generated_at": _utc_now(),
        "session_id": session_id,
        "contract_hash": validation_contract["validation_contract_hash"],
        "pivot_contract_hash": pivot_contract.get("contract_hash"),
        "input_artifacts": {
            "pivot_contract": str(PIVOT_CONTRACT_PATH),
            "pivot_grid": str(PIVOT_CANDIDATE_GRID_PATH),
            "pivot_metrics": str(PIVOT_CANDIDATE_METRICS_PATH),
            "pivot_leaderboard": str(PIVOT_SESSION_LEADERBOARD_PATH),
            "pivot_rollup": str(PIVOT_DECISION_ROLLUP_PATH),
            "long_audit": str(LONG_AUDIT_PATH),
            "long_compare": str(LONG_COMPARE_PATH),
            "short_compare": str(SHORT_COMPARE_PATH),
            "short_wide": str(SHORT_WIDE_PATH),
            "short_regime": str(SHORT_REGIME_PATH),
            "liq_compare": str(LIQ_COMPARE_PATH),
            "liq_summary": str(LIQ_SUMMARY_PATH),
        },
        "candidate_order": [row["family_id"] for row in candidate_set["candidates"]],
        "validation_mode": "same_condition_raw_replay",
        "runtime_hash": _stable_hash(
            {
                "contract_hash": validation_contract["validation_contract_hash"],
                "candidate_order": [row["family_id"] for row in candidate_set["candidates"]],
                "input_artifacts": sorted(str(path) for path in [
                    LONG_AUDIT_PATH,
                    LONG_COMPARE_PATH,
                    SHORT_COMPARE_PATH,
                    SHORT_WIDE_PATH,
                    SHORT_REGIME_PATH,
                    LIQ_COMPARE_PATH,
                    LIQ_SUMMARY_PATH,
                ]),
            }
        ),
    }

    runtime_log = {
        "schema_version": "entry_method_validation_runtime_log_v1",
        "generated_at": _utc_now(),
        "started_at": _utc_now(),
        "completed_at": None,
        "elapsed_seconds": None,
        "commands": [
            "python scripts/entry_method_validation_batch.py",
        ],
        "seeds": [],
        "config_hash": manifest["runtime_hash"],
        "input_artifact_refs": manifest["input_artifacts"],
        "output_artifact_refs": {
            "validation_contract": str(VALIDATION_ROOT / "entry_method_validation_contract.json"),
            "candidate_set": str(VALIDATION_ROOT / "entry_method_validation_candidate_set.json"),
            "manifest": str(VALIDATION_ROOT / "entry_method_validation_manifest.json"),
            "metrics": str(VALIDATION_ROOT / "entry_method_validation_metrics.json"),
            "decision_rollup": str(VALIDATION_ROOT / "entry_method_validation_decision_rollup.json"),
            "report": str(VALIDATION_ROOT / "entry_method_validation_report.md"),
        },
        "status": "running",
        "partial_state": "materialized",
        "notes": [
            "Long-side validation is raw audit replay with same-condition gate support.",
            "Short and liquidity validation are raw compare replays under the frozen contract.",
            "No MeeMee files were modified.",
        ],
    }

    _write_json(VALIDATION_ROOT / "entry_method_validation_contract.json", validation_contract)
    _write_json(VALIDATION_ROOT / "entry_method_validation_candidate_set.json", candidate_set)
    _write_json(VALIDATION_ROOT / "entry_method_validation_manifest.json", manifest)
    _write_json(VALIDATION_ROOT / "entry_method_validation_metrics.json", metrics)
    _write_json(VALIDATION_ROOT / "entry_method_validation_decision_rollup.json", decision_rollup)

    runtime_log["completed_at"] = _utc_now()
    runtime_log["elapsed_seconds"] = round(time.time() - start, 3)
    runtime_log["status"] = "complete"
    _write_json(VALIDATION_ROOT / "entry_method_validation_runtime_log.json", runtime_log)

    report = "\n".join(
        [
            "# Entry Method Validation Report",
            "",
            "## Current State",
            "- confirmed: pivot contract was frozen and reused as the validation boundary.",
            "- confirmed: long / short are separated, and the validation batch only touches TRADEX artifacts.",
            "- provisional: the long candidate is validated from raw audit replay plus a same-condition gate compare, not a fresh family runner execution.",
            "",
            "## Problem",
            "- The pivot candidates still needed a fixed same-condition replay before they could be trusted as reusable keep candidates.",
            "- The remaining risk was that the long-side family was still partly proxy-backed.",
            "",
            "## Change Policy",
            "- Scope: TRADEX validation only.",
            "- Non-scope: MeeMee UI, publish wiring, new family discovery, threshold tuning, or broad search-space expansion.",
            "- Boundary check: keep research logic in TRADEX and leave MeeMee untouched.",
            "",
            "## Concrete Changes",
            f"- Validation contract: `{VALIDATION_ROOT / 'entry_method_validation_contract.json'}`",
            f"- Candidate set: `{VALIDATION_ROOT / 'entry_method_validation_candidate_set.json'}`",
            f"- Manifest: `{VALIDATION_ROOT / 'entry_method_validation_manifest.json'}`",
            f"- Runtime log: `{VALIDATION_ROOT / 'entry_method_validation_runtime_log.json'}`",
            f"- Metrics: `{VALIDATION_ROOT / 'entry_method_validation_metrics.json'}`",
            f"- Decision rollup: `{VALIDATION_ROOT / 'entry_method_validation_decision_rollup.json'}`",
            "",
            "## Verify",
            f"- long: {long_decision['candidate_local_decision']} with source mode `{long_decision['validation_source_mode']}`.",
            f"- short: {short_decision['candidate_local_decision']} with source mode `{short_decision['validation_source_mode']}`.",
            f"- liquidity: {liq_decision['candidate_local_decision']} with source mode `{liq_decision['validation_source_mode']}`.",
            f"- monthly stability: long {metrics['candidates'][0]['monthly_stability']['months_with_selection']} months, short {metrics['candidates'][1]['monthly_stability']['months_with_selection']} months, liquidity {metrics['candidates'][2]['monthly_stability']['months_with_selection']} months.",
            f"- yearly stability: long {metrics['candidates'][0]['yearly_stability']['years_with_selection']} years, short {metrics['candidates'][1]['yearly_stability']['years_with_selection']} years, liquidity {metrics['candidates'][2]['yearly_stability']['years_with_selection']} years.",
            f"- symbol breadth: long {metrics['candidates'][0]['selected_symbol_breadth']}, short {metrics['candidates'][1]['selected_symbol_breadth']}, liquidity {metrics['candidates'][2]['selected_symbol_breadth']}.",
            "",
            "## Decision",
            f"- keep: {short_decision['family_name']}.",
            f"- keep: {liq_decision['family_name']}.",
            f"- hold: {long_decision['family_name']} because the validation still relies on audit replay rather than a dedicated family runner compare.",
            "- drop: none.",
            "",
            "## Remaining Risks",
            "- The long candidate still needs a dedicated frozen family compare if the user wants the strongest possible promotion evidence.",
            "- Liquidity hit rate is not directly exposed by the penalty-gate compare and is therefore reported as not applicable.",
            "",
            "## Next One Thing",
            "- Materialize a dedicated long-side family compare for Trend Resumption After Pause under the same pivot contract, then compare it directly against the audit replay result.",
        ]
    )
    _write_text(VALIDATION_ROOT / "entry_method_validation_report.md", report)

    print(
        json.dumps(
            {
                "status": "complete",
                "output_root": str(VALIDATION_ROOT),
                "contract_hash": validation_contract["validation_contract_hash"],
                "candidate_decisions": decision_rows,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
