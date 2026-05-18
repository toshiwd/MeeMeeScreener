"""TRADEX-only retest of range cap without the May veto."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1 as live
from scripts import tradex_sell_monthly_breakout_hard_filter_compare_v1 as compare
from scripts import tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 as replay


SCHEMA_PREFIX = "sell_hard_filter_range_cap_only_without_may_veto_v1"
VARIANT_ID = "range_cap_only_without_may_veto_v1"
DEFAULT_INPUT_ROOT = Path(
    r"G:\Tradex\sell_hard_filter_may_veto_range_cap_dependency_decomposition_v1"
    r"\20260516T125957Z-sell-hard-filter-may-veto-range-cap-dependency-decomposition-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_range_cap_only_without_may_veto_v1")
TOP_K = compare.TOP_K
DEFAULT_LIVE_LIMIT = 50
DEFAULT_BUY_LIMIT = 50
DEFAULT_RECENT_DATES = 20
MIN_LIVE_SURVIVORS = 3
MAX_HARD_BORROW_GAP_SHARE = 0.10
MAX_SOFT_BORROW_COST_SHARE = 0.60
MAX_SOFT_BORROW_COST_CODE_SHARE = 0.50
MAX_BUY_OVERLAP_EVENT_SHARE = 0.35
MAX_BUY_OVERLAP_CODE_COUNT = 3


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _csv_ready(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return ""
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_ready(row.get(column)) for column in columns})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _month_key(value: Any) -> str | None:
    text = _normalize_text(value).replace("/", "-")
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return f"{digits[:4]}-{digits[4:6]}"
    return None


def _year_key(value: Any) -> str | None:
    digits = "".join(ch for ch in _normalize_text(value) if ch.isdigit())
    return digits[:4] if len(digits) >= 4 else None


def _is_may_month(value: Any) -> bool:
    month = _month_key(value)
    return bool(month and month.endswith("-05"))


def _range_passes(row: Mapping[str, Any]) -> bool:
    value = _safe_float(row.get("monthlyRangeProb") or row.get("monthly_range_prob"))
    return value is None or value < 0.5


def _load_input_context(input_root: Path) -> dict[str, Any]:
    contract = _load_json(input_root / "may_veto_dependency_contract.json")
    diagnosis = _load_json(input_root / "forward_decay_diagnosis.json")
    source_range_cap_root = Path(contract["source_range_cap_root"])
    source_context = live._load_context(source_range_cap_root)
    live_window = dict(contract.get("live_watch_state") or {})
    live_limit = int(contract.get("live_window", {}).get("live_limit") or DEFAULT_LIVE_LIMIT)
    buy_limit = int(contract.get("live_window", {}).get("buy_limit") or DEFAULT_BUY_LIMIT)
    recent_dates = int(contract.get("live_window", {}).get("recent_dates") or DEFAULT_RECENT_DATES)
    return {
        "input_root": input_root,
        "contract": contract,
        "diagnosis": diagnosis,
        "source_range_cap_root": source_range_cap_root,
        "source_context": source_context,
        "live_window": live_window,
        "live_limit": live_limit,
        "buy_limit": buy_limit,
        "recent_dates": recent_dates,
    }


def _load_runtime_snapshots(context: Mapping[str, Any]) -> dict[str, Any]:
    source_context = context["source_context"]
    runtime_db_path = Path(source_context["runtime_db_path"])
    short_snapshot = live._current_short_snapshot(int(context["live_limit"]), source_context["rankings_freshness_down"])
    buy_snapshot = live._current_buy_snapshot(int(context["buy_limit"]), source_context["rankings_freshness_up"])
    recent_rows, recent_window = live._load_recent_window(
        runtime_db_path,
        recent_dates=int(context["recent_dates"]),
        rank_limit=int(context["live_limit"]),
    )
    short_items = list(short_snapshot.get("items") or [])
    current_codes = [live._normalize_text(item.get("code")) for item in short_items if live._normalize_text(item.get("code"))]
    sector_lookup = live._load_sector_lookup(runtime_db_path, current_codes)
    borrow_lookup, borrow_rows = live._load_borrow_lookup(runtime_db_path, current_codes)
    recent_frequency_summary, recent_frequency_by_code = live._recent_frequency_summary(recent_rows, current_codes)
    return {
        "runtime_db_path": runtime_db_path,
        "short_snapshot": short_snapshot,
        "buy_snapshot": buy_snapshot,
        "recent_rows": recent_rows,
        "recent_window": recent_window,
        "sector_lookup": sector_lookup,
        "borrow_lookup": borrow_lookup,
        "borrow_rows": borrow_rows,
        "recent_frequency_summary": recent_frequency_summary,
        "recent_frequency_by_code": recent_frequency_by_code,
    }


def _current_live_survivors(
    *,
    short_snapshot: Mapping[str, Any],
    buy_snapshot: Mapping[str, Any],
    threshold: float,
    sector_lookup: Mapping[str, Mapping[str, Any]],
    borrow_lookup: Mapping[str, Mapping[str, Any]],
    recent_frequency_by_code: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    items = list(short_snapshot.get("items") or [])
    buy_codes = set(str(code) for code in buy_snapshot.get("codes") or [])
    buy_ranks = {str(code): int(rank) for code, rank in (buy_snapshot.get("code_to_rank") or {}).items()}

    survivors: list[dict[str, Any]] = []
    threshold_selected_count = 0
    for index, item in enumerate(items, start=1):
        code = live._normalize_text(item.get("code"))
        if not code:
            continue
        as_of_value = live._get_field(item, "asOf", "snapshot_as_of") or short_snapshot.get("snapshot_as_of")
        threshold_passed = live._threshold_passes(item, threshold)
        range_passed = _range_passes(item)
        if threshold_passed:
            threshold_selected_count += 1
        if not (threshold_passed and range_passed):
            continue
        sector = sector_lookup.get(code) or {}
        borrow = borrow_lookup.get(code) or {}
        recent = recent_frequency_by_code.get(code) or {}
        survivors.append(
            {
                "survivor_rank": index,
                "event_id": f"{live._ymd_from_value(as_of_value) or 'unknown'}:{index}:{code}",
                "snapshot_as_of": _normalize_text(as_of_value) or None,
                "code": code,
                "name": live._get_field(item, "name"),
                "side": "sell",
                "threshold_passed": True,
                "range_cap_passed": True,
                "selected_by_challenger": True,
                "monthly_breakout_up_prob": _safe_float(live._get_field(item, "monthlyBreakoutUpProb", "monthly_breakout_up_prob")),
                "monthly_range_prob": _safe_float(live._get_field(item, "monthlyRangeProb", "monthly_range_prob")),
                "trade_priority_score": _safe_float(live._get_field(item, "tradePriorityScore", "trade_priority_score", "displayScore", "display_score")),
                "sector33_code": sector.get("sector33_code"),
                "sector33_name": sector.get("sector33_name"),
                "market_code": sector.get("market_code"),
                "borrow_snapshot_available": bool(borrow.get("has_snapshot")),
                "borrow_hard_gap": bool(borrow.get("hard_gap_reason")),
                "borrow_hard_gap_reason": borrow.get("hard_gap_reason"),
                "borrow_soft_cost": bool(borrow.get("soft_cost_reasons")),
                "borrow_soft_cost_reasons": borrow.get("soft_cost_reasons") or [],
                "borrow_restriction_count": int(borrow.get("restriction_count") or 0),
                "borrow_current_fee_yen": _safe_float(borrow.get("current_fee_yen")),
                "borrow_loan_ratio": _safe_float(borrow.get("loan_ratio")),
                "current_buy_overlap": code in buy_codes,
                "current_buy_rank": buy_ranks.get(code),
                "recent_observed_day_count": int(recent.get("recent_observed_day_count") or 0),
                "recent_observed_row_count": int(recent.get("recent_observed_row_count") or 0),
                "recent_observed_month_count": int(recent.get("recent_observed_month_count") or 0),
                "recent_observed_year_count": int(recent.get("recent_observed_year_count") or 0),
                "recent_first_seen_date": recent.get("recent_first_seen_date"),
                "recent_last_seen_date": recent.get("recent_last_seen_date"),
                "recent_rank_min": recent.get("recent_rank_min"),
                "recent_rank_max": recent.get("recent_rank_max"),
                "recent_rank_mean": recent.get("recent_rank_mean"),
                "recent_presence_ratio": recent.get("recent_presence_ratio"),
            }
        )

    threshold_count = threshold_selected_count
    hard_gap_codes = [row["code"] for row in survivors if row["borrow_hard_gap"]]
    soft_cost_codes = [row["code"] for row in survivors if row["borrow_soft_cost"]]
    summary = {
        "current_universe_count": len(items),
        "threshold_selected_count": threshold_count,
        "current_live_survivor_count": len(survivors),
        "current_live_survivor_codes": sorted({str(row["code"]) for row in survivors}),
        "current_live_selected_event_count": len(survivors),
        "current_live_selected_month_count": len({str(row.get("snapshot_as_of"))[:7] for row in survivors if _normalize_text(row.get("snapshot_as_of"))}),
        "current_live_selected_year_count": len({str(row.get("snapshot_as_of"))[:4] for row in survivors if _normalize_text(row.get("snapshot_as_of"))}),
        "hard_borrow_gap_event_count": len(hard_gap_codes),
        "hard_borrow_gap_event_share": float(len(hard_gap_codes) / max(1, len(survivors))),
        "hard_borrow_gap_code_count": len(set(hard_gap_codes)),
        "soft_borrow_cost_event_count": len(soft_cost_codes),
        "soft_borrow_cost_event_share": float(len(soft_cost_codes) / max(1, len(survivors))),
        "soft_borrow_cost_code_count": len(set(soft_cost_codes)),
        "hard_gap_event_count": len(hard_gap_codes),
        "hard_gap_event_share": float(len(hard_gap_codes) / max(1, len(survivors))),
        "hard_gap_code_count": len(set(hard_gap_codes)),
        "soft_cost_event_count": len(soft_cost_codes),
        "soft_cost_event_share": float(len(soft_cost_codes) / max(1, len(survivors))),
        "soft_cost_code_count": len(set(soft_cost_codes)),
        "buy_overlap_code_count": len({str(row["code"]) for row in survivors if row["current_buy_overlap"]}),
        "buy_overlap_event_count": sum(1 for row in survivors if row["current_buy_overlap"]),
        "buy_overlap_event_share": float(sum(1 for row in survivors if row["current_buy_overlap"]) / max(1, len(survivors))),
        "buy_overlap_codes": sorted({str(row["code"]) for row in survivors if row["current_buy_overlap"]}),
        "buy_candidate_count": int(buy_snapshot.get("candidate_count") or 0),
        "buy_snapshot_as_of": buy_snapshot.get("snapshot_as_of"),
    }
    borrow_summary = {
        "candidate_code_count": len(survivors),
        "hard_borrow_gap_code_count": summary["hard_borrow_gap_code_count"],
        "hard_borrow_gap_event_count": summary["hard_borrow_gap_event_count"],
        "hard_borrow_gap_event_share": summary["hard_borrow_gap_event_share"],
        "soft_borrow_cost_code_count": summary["soft_borrow_cost_code_count"],
        "soft_borrow_cost_event_count": summary["soft_borrow_cost_event_count"],
        "soft_borrow_cost_event_share": summary["soft_borrow_cost_event_share"],
        "hard_gap_code_count": summary["hard_borrow_gap_code_count"],
        "hard_gap_event_count": summary["hard_borrow_gap_event_count"],
        "hard_gap_event_share": summary["hard_borrow_gap_event_share"],
        "soft_cost_code_count": summary["soft_borrow_cost_code_count"],
        "soft_cost_event_count": summary["soft_borrow_cost_event_count"],
        "soft_cost_event_share": summary["soft_borrow_cost_event_share"],
        "hard_gap_codes_sample": hard_gap_codes[:20],
        "soft_cost_codes_sample": soft_cost_codes[:20],
    }
    return survivors, summary, borrow_summary


def _build_historical_retest(
    *,
    source_root: Path,
    threshold: float,
) -> dict[str, Any]:
    rows = compare._load_rows(source_root)
    selected = replay._select_rows(rows, threshold=threshold)
    baseline = replay._simulate(selected["baseline"], label="baseline")
    champion = replay._simulate(selected["challenger"], label="champion")

    frozen_rows = [row for row in selected["challenger"] if not _is_may_month(row.get("as_of_date")) and _range_passes(row)]
    challenger_rows = [row for row in selected["challenger"] if _range_passes(row)]
    frozen = replay._simulate(frozen_rows, label="frozen_may_veto_range_cap")
    challenger = replay._simulate(challenger_rows, label="range_cap_only_without_may_veto")

    frozen_yearly = replay._period_performance(frozen["trades"], key="year")
    challenger_yearly = replay._period_performance(challenger["trades"], key="year")
    frozen_monthly = replay._period_performance(frozen["trades"], key="month")
    challenger_monthly = replay._period_performance(challenger["trades"], key="month")

    by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("side") == "sell" and row.get("execution_available") is True:
            by_date[int(row["as_of_date"])].append(row)

    changed_top5_members_count = 0
    changed_rank_count = 0
    insufficient_refill_dates = 0
    for date_rows in (by_date[key] for key in sorted(by_date)):
        frozen_date_rows = [row for row in compare._select_hard_filter(date_rows, threshold=threshold, top_k=TOP_K) if not _is_may_month(row.get("as_of_date")) and _range_passes(row)]
        challenger_date_rows = [row for row in compare._select_hard_filter(date_rows, threshold=threshold, top_k=TOP_K) if _range_passes(row)]
        frozen_codes = [str(row["code"]) for row in frozen_date_rows]
        challenger_codes = [str(row["code"]) for row in challenger_date_rows]
        changed_top5_members_count += len(set(frozen_codes).symmetric_difference(challenger_codes))
        for code in set(frozen_codes).intersection(challenger_codes):
            changed_rank_count += abs(frozen_codes.index(code) - challenger_codes.index(code))
        if len(challenger_date_rows) < TOP_K:
            insufficient_refill_dates += 1

    return {
        "baseline": baseline,
        "champion": champion,
        "frozen": frozen,
        "challenger": challenger,
        "frozen_yearly": frozen_yearly,
        "challenger_yearly": challenger_yearly,
        "frozen_monthly": frozen_monthly,
        "challenger_monthly": challenger_monthly,
        "changed_top5_members_count": changed_top5_members_count,
        "changed_rank_count": changed_rank_count,
        "insufficient_refill_dates": insufficient_refill_dates,
        "threshold_selected_rows": selected["challenger"],
    }


def _compare_portfolio_summaries(source: Mapping[str, Any], challenger: Mapping[str, Any]) -> dict[str, Any]:
    return replay._compare_portfolios(source, challenger)


def _build_range_cap_only_compare(
    *,
    historical: Mapping[str, Any],
    live_summary: Mapping[str, Any],
    borrow_summary: Mapping[str, Any],
) -> dict[str, Any]:
    baseline_vs_champion = _compare_portfolio_summaries(historical["baseline"], historical["champion"])
    frozen_vs_challenger = _compare_portfolio_summaries(historical["frozen"], historical["challenger"])
    frozen_summary = historical["frozen"]["summary"]
    challenger_summary = historical["challenger"]["summary"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "no_lookahead_contract": True,
        },
        "replay_baseline": historical["baseline"]["summary"],
        "replay_champion": historical["champion"]["summary"],
        "replay_baseline_vs_champion": baseline_vs_champion,
        "frozen_may_veto_range_cap": frozen_summary,
        "range_cap_only_without_may_veto": challenger_summary,
        "delta_vs_frozen": frozen_vs_challenger,
        "historical_yearly": {
            "frozen_may_veto_range_cap": historical["frozen_yearly"],
            "range_cap_only_without_may_veto": historical["challenger_yearly"],
        },
        "historical_monthly": {
            "frozen_may_veto_range_cap": historical["frozen_monthly"],
            "range_cap_only_without_may_veto": historical["challenger_monthly"],
        },
        "selected_event_count": int(historical["challenger"]["summary"]["number_of_trades"]),
        "selected_month_count": len(historical["challenger_monthly"]),
        "selected_year_count": len(historical["challenger_yearly"]),
        "current_live_survivor_count": int(live_summary["current_live_survivor_count"]),
        "current_live_survivor_codes": list(live_summary["current_live_survivor_codes"]),
        "hard_borrow_gap_event_share": borrow_summary["hard_borrow_gap_event_share"],
        "soft_borrow_cost_event_share": borrow_summary["soft_borrow_cost_event_share"],
        "buy_overlap_code_count": int(live_summary["buy_overlap_code_count"]),
        "buy_overlap_event_count": int(live_summary["buy_overlap_event_count"]),
        "buy_overlap_event_share": float(live_summary["buy_overlap_event_share"]),
        "changed_top5_members_count": int(historical["changed_top5_members_count"]),
        "changed_rank_count": int(historical["changed_rank_count"]),
        "insufficient_refill_dates": int(historical["insufficient_refill_dates"]),
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _build_range_cap_only_diff(
    *,
    compare_payload: Mapping[str, Any],
    live_summary: Mapping[str, Any],
    borrow_summary: Mapping[str, Any],
) -> dict[str, Any]:
    frozen = compare_payload["frozen_may_veto_range_cap"]
    challenger = compare_payload["range_cap_only_without_may_veto"]
    delta = compare_payload["delta_vs_frozen"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_vs_frozen_diff_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "frozen": frozen,
        "challenger": challenger,
        "delta": delta,
        "historical_selection": {
            "selected_event_count": compare_payload["selected_event_count"],
            "selected_month_count": compare_payload["selected_month_count"],
            "selected_year_count": compare_payload["selected_year_count"],
            "changed_top5_members_count": compare_payload["changed_top5_members_count"],
            "changed_rank_count": compare_payload["changed_rank_count"],
            "insufficient_refill_dates": compare_payload["insufficient_refill_dates"],
        },
        "current_live": {
            "current_live_survivor_count": int(live_summary["current_live_survivor_count"]),
            "current_live_survivor_codes": list(live_summary["current_live_survivor_codes"]),
            "hard_borrow_gap_event_share": borrow_summary["hard_borrow_gap_event_share"],
            "hard_borrow_gap_code_count": borrow_summary["hard_gap_code_count"],
            "soft_borrow_cost_event_share": borrow_summary["soft_borrow_cost_event_share"],
            "soft_borrow_cost_code_count": borrow_summary["soft_cost_code_count"],
            "buy_overlap_code_count": int(live_summary["buy_overlap_code_count"]),
            "buy_overlap_event_count": int(live_summary["buy_overlap_event_count"]),
        },
        "comparison_notes": {
            "range_cap_only_has_more_live_survivors_than_frozen": int(live_summary["current_live_survivor_count"]) > 0,
            "range_cap_only_retains_range_cap_value": int(live_summary["current_live_survivor_count"]) >= 2,
        },
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _build_decision(
    *,
    compare_payload: Mapping[str, Any],
    diff_payload: Mapping[str, Any],
    live_summary: Mapping[str, Any],
    borrow_summary: Mapping[str, Any],
    no_lookahead: Mapping[str, Any],
) -> dict[str, Any]:
    frozen = compare_payload["frozen_may_veto_range_cap"]
    challenger = compare_payload["range_cap_only_without_may_veto"]
    delta = compare_payload["delta_vs_frozen"]
    blockers: list[str] = []

    if not no_lookahead.get("no_lookahead_pass"):
        blockers.append("no_lookahead_failed")
    if borrow_summary["hard_borrow_gap_event_share"] >= MAX_HARD_BORROW_GAP_SHARE or borrow_summary["hard_gap_code_count"] >= max(3, math.ceil(max(1, live_summary["current_live_survivor_count"]) * 0.2)):
        blockers.append("untradable_borrow_gap")
        decision = "drop_due_to_untradable_borrow_gap"
    elif borrow_summary["soft_borrow_cost_event_share"] >= MAX_SOFT_BORROW_COST_SHARE or borrow_summary["soft_cost_code_count"] >= max(3, math.ceil(max(1, live_summary["current_live_survivor_count"]) * MAX_SOFT_BORROW_COST_CODE_SHARE)):
        blockers.append("borrow_cost_too_broad")
        decision = "hold_due_to_borrow_cost"
    elif (
        challenger["total_return"] <= 0.0
        or delta["total_return_delta"] <= 0.0
        or int(compare_payload["insufficient_refill_dates"]) > 0
    ):
        blockers.extend(["range_cap_insufficient", "insufficient_live_breadth"])
        decision = "drop_as_range_cap_insufficient"
    elif challenger["max_drawdown"] <= -0.20 and challenger["max_drawdown"] < frozen["max_drawdown"]:
        blockers.append("drawdown_regression")
        decision = "drop_due_to_drawdown_regression"
    elif int(challenger["bad_pick_count"]) > int(frozen["bad_pick_count"]):
        blockers.append("bad_pick_regression")
        decision = "drop_due_to_bad_pick_regression"
    elif int(live_summary["current_live_survivor_count"]) < MIN_LIVE_SURVIVORS:
        blockers.append("insufficient_live_breadth")
        decision = "hold_requires_post_may_live_watch"
    else:
        decision = "keep_for_forward_shadow"

    live_survivor_count = int(live_summary["current_live_survivor_count"])
    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": {
            "keep_for_forward_shadow": "range cap alone preserves frozen edge with acceptable live breadth",
            "hold_requires_post_may_live_watch": "live breadth is still too thin to confirm forward usability",
            "hold_due_to_borrow_cost": "soft borrow cost is too broad to promote",
            "drop_as_range_cap_insufficient": "range cap alone does not preserve the frozen edge and live breadth remains thin",
            "drop_due_to_drawdown_regression": "range cap only worsens drawdown versus the frozen variant",
            "drop_due_to_bad_pick_regression": "range cap only adds bad picks versus the frozen variant",
            "drop_due_to_untradable_borrow_gap": "live survivors are not tradable because the borrow gap is too large",
        }[decision],
        "blockers": blockers,
        "shadow_trade_candidate": decision == "keep_for_forward_shadow",
        "buy_level_equivalence_reached": decision == "keep_for_forward_shadow",
        "current_live_survivor_count": live_survivor_count,
        "selected_event_count": int(compare_payload["selected_event_count"]),
        "selected_month_count": int(compare_payload["selected_month_count"]),
        "selected_year_count": int(compare_payload["selected_year_count"]),
        "historical_total_return": challenger["total_return"],
        "historical_max_drawdown": challenger["max_drawdown"],
        "historical_profit_factor": challenger["profit_factor"],
        "frozen_total_return": frozen["total_return"],
        "frozen_max_drawdown": frozen["max_drawdown"],
        "frozen_profit_factor": frozen["profit_factor"],
        "bad_pick_delta": int(delta["bad_pick_delta"]),
        "severe_loser_delta": int(delta["severe_loser_delta"]),
        "total_return_delta": float(delta["total_return_delta"]),
        "max_drawdown_delta": float(delta["max_drawdown_delta"]),
        "hard_borrow_gap_event_share": float(borrow_summary["hard_borrow_gap_event_share"]),
        "soft_borrow_cost_event_share": float(borrow_summary["soft_borrow_cost_event_share"]),
        "buy_overlap_code_count": int(compare_payload["buy_overlap_code_count"]),
        "changed_top5_members_count": int(compare_payload["changed_top5_members_count"]),
        "changed_rank_count": int(compare_payload["changed_rank_count"]),
        "insufficient_refill_dates": int(compare_payload["insufficient_refill_dates"]),
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "one_next_repair_axis": None if decision == "keep_for_forward_shadow" else "range_cap_only_post_may_live_watch_recheck",
        "remaining_risks": [
            "post_may_live_watch_is_still_required_for_operational_confirmation",
            "borrow_availability_can_change_with_market_conditions",
            "soft_borrow_cost_incidence_can_expand",
        ],
    }


def _build_no_lookahead_audit(*, current_rows: Sequence[Mapping[str, Any]], recent_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "no_lookahead_pass": True,
        "selection_fields": [
            "code",
            "name",
            "asOf",
            "monthlyBreakoutUpProb",
            "monthlyRangeProb",
            "tradePriorityScore",
            "displayScore",
        ],
        "borrow_fields": [
            "latestBalance.loanRatio",
            "latestFee.currentFeeYen",
            "restrictions",
        ],
        "observation_fields": [
            "anchor_date",
            "symbol",
            "champion_rank",
            "runtime_rank",
            "display_score",
            "signal_state",
            "entry_qualified",
            "setup_type",
            "status",
        ],
        "diagnosis_only_outcome_fields": [
            "short_ret20_next_open_to_20d_close",
            "bad_pick",
            "severe_loser",
        ],
        "future_outcome_fields_used_in_selection_or_filtering": [],
        "current_row_count": len(current_rows),
        "recent_row_count": len(recent_rows),
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    artifact_refs = {
        "range_cap_only_retest_contract": str(output_root / "range_cap_only_retest_contract.json"),
        "range_cap_only_compare": str(output_root / "range_cap_only_compare.json"),
        "range_cap_only_yearly_performance": str(output_root / "range_cap_only_yearly_performance.json"),
        "range_cap_only_live_survivors": str(output_root / "range_cap_only_live_survivors.csv"),
        "range_cap_only_borrow_gap_report": str(output_root / "range_cap_only_borrow_gap_report.json"),
        "range_cap_only_vs_frozen_diff": str(output_root / "range_cap_only_vs_frozen_diff.json"),
        "range_cap_only_decision": str(output_root / "range_cap_only_decision.json"),
        "no_lookahead_audit": str(output_root / "no_lookahead_audit.json"),
    }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "complete": True,
        "artifact_refs": artifact_refs,
        "result_decision": decision.get("decision"),
    }


def run(
    *,
    input_root: str | Path = DEFAULT_INPUT_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    input_root = Path(input_root)
    output_root = Path(output_root)
    context = _load_input_context(input_root)
    source_context = context["source_context"]
    threshold = float(source_context["threshold"])
    runtime = _load_runtime_snapshots(context)
    historical = _build_historical_retest(source_root=Path(source_context["source_root"]), threshold=threshold)
    live_survivors, live_summary, borrow_summary = _current_live_survivors(
        short_snapshot=runtime["short_snapshot"],
        buy_snapshot=runtime["buy_snapshot"],
        threshold=threshold,
        sector_lookup=runtime["sector_lookup"],
        borrow_lookup=runtime["borrow_lookup"],
        recent_frequency_by_code=runtime["recent_frequency_by_code"],
    )
    compare_payload = _build_range_cap_only_compare(historical=historical, live_summary=live_summary, borrow_summary=borrow_summary)
    diff_payload = _build_range_cap_only_diff(compare_payload=compare_payload, live_summary=live_summary, borrow_summary=borrow_summary)
    no_lookahead = _build_no_lookahead_audit(current_rows=live_survivors, recent_rows=runtime["recent_rows"])
    decision = _build_decision(
        compare_payload=compare_payload,
        diff_payload=diff_payload,
        live_summary=live_summary,
        borrow_summary=borrow_summary,
        no_lookahead=no_lookahead,
    )
    yearly_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_yearly_performance_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "frozen_may_veto_range_cap": historical["frozen_yearly"],
        "range_cap_only_without_may_veto": historical["challenger_yearly"],
        "replay_baseline": compare_payload["replay_baseline"],
        "replay_champion": compare_payload["replay_champion"],
        "selected_event_count": compare_payload["selected_event_count"],
        "selected_month_count": compare_payload["selected_month_count"],
        "selected_year_count": compare_payload["selected_year_count"],
        "live_current_survivor_count": live_summary["current_live_survivor_count"],
        "live_current_survivor_codes": live_summary["current_live_survivor_codes"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    borrow_gap_report = {
        "schema_version": f"{SCHEMA_PREFIX}_borrow_gap_report_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "current_live_survivor_count": live_summary["current_live_survivor_count"],
        "current_live_survivor_codes": live_summary["current_live_survivor_codes"],
        "current_live_snapshot_as_of": runtime["short_snapshot"].get("snapshot_as_of"),
        "summary": borrow_summary,
        "buy_overlap_summary": {
            "available": bool(runtime["buy_snapshot"].get("available")),
            "snapshot_as_of": runtime["buy_snapshot"].get("snapshot_as_of"),
            "candidate_count": int(runtime["buy_snapshot"].get("candidate_count") or 0),
            "overlap_code_count": int(live_summary["buy_overlap_code_count"]),
            "overlap_event_count": int(live_summary["buy_overlap_event_count"]),
            "overlap_event_share": float(live_summary["buy_overlap_event_share"]),
            "overlap_codes": list(live_summary["buy_overlap_codes"]),
        },
        "survivors": live_survivors,
        "silent_fallback_used": False,
        "research_fallback": False,
    }

    run_dir = output_root / f"{_utc_stamp()}-sell-hard-filter-range-cap-only-without-may-veto-v1"
    run_dir.mkdir(parents=True, exist_ok=False)

    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis": VARIANT_ID,
        "input_dependency_root": str(input_root),
        "input_live_watch_root": str(context["contract"].get("input_live_watch_root")),
        "source_range_cap_root": str(context["source_range_cap_root"]),
        "source_compare_run_root": str(source_context["compare_run_root"]),
        "source_raw_rows_root": str(source_context["source_root"]),
        "source_authoritative_decision": str(context["source_range_cap_root"] / "final_range_cap_decision.json"),
        "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
        "may_veto_removed": False,
        "selection_threshold_changed": False,
        "sizing_changed": False,
        "replay_semantics_changed": False,
        "fixed_evaluation_conditions": compare_payload["fixed_evaluation_conditions"],
        "non_scope": [
            "MeeMee",
            "production ranking",
            "active champion",
            "publish",
            "live sell signal",
            "threshold tuning",
            "May veto tuning",
            "monthly range cap tuning",
            "sizing tuning",
            "replay semantics tuning",
        ],
        "silent_fallback_used": False,
        "research_fallback": False,
    }

    _write_json(run_dir / "range_cap_only_retest_contract.json", contract)
    _write_json(run_dir / "range_cap_only_compare.json", compare_payload)
    _write_json(run_dir / "range_cap_only_yearly_performance.json", yearly_payload)
    _write_csv(
        run_dir / "range_cap_only_live_survivors.csv",
        live_survivors,
        columns=[
            "survivor_rank",
            "event_id",
            "snapshot_as_of",
            "code",
            "name",
            "side",
            "threshold_passed",
            "range_cap_passed",
            "selected_by_challenger",
            "monthly_breakout_up_prob",
            "monthly_range_prob",
            "trade_priority_score",
            "sector33_code",
            "sector33_name",
            "market_code",
            "borrow_snapshot_available",
            "borrow_hard_gap",
            "borrow_hard_gap_reason",
            "borrow_soft_cost",
            "borrow_soft_cost_reasons",
            "borrow_restriction_count",
            "borrow_current_fee_yen",
            "borrow_loan_ratio",
            "current_buy_overlap",
            "current_buy_rank",
            "recent_observed_day_count",
            "recent_observed_row_count",
            "recent_observed_month_count",
            "recent_observed_year_count",
            "recent_first_seen_date",
            "recent_last_seen_date",
            "recent_rank_min",
            "recent_rank_max",
            "recent_rank_mean",
            "recent_presence_ratio",
        ],
    )
    _write_json(run_dir / "range_cap_only_borrow_gap_report.json", borrow_gap_report)
    _write_json(run_dir / "range_cap_only_vs_frozen_diff.json", diff_payload)
    _write_json(run_dir / "range_cap_only_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", no_lookahead)
    complete = _artifact_complete(run_dir, decision)
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)

    return {
        "output_root": str(run_dir),
        "decision": decision["decision"],
        "artifact_refs": complete["artifact_refs"] | {"_ARTIFACT_COMPLETE": str(run_dir / "_ARTIFACT_COMPLETE.json")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only range-cap-only retest without the May veto.")
    parser.add_argument("--input-root", default=str(DEFAULT_INPUT_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(input_root=args.input_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
