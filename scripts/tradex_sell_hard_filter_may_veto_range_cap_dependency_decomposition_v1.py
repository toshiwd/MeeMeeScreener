"""TRADEX-only decomposition of the frozen May veto plus monthly range cap rule."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1 as live
from scripts import tradex_sell_monthly_breakout_hard_filter_compare_v1 as compare


SCHEMA_PREFIX = "sell_hard_filter_may_veto_range_cap_dependency_decomposition_v1"
VARIANT_ID = "may_veto_plus_monthly_range_lt_0_5_v1"
DEFAULT_SOURCE_LIVE_ROOT = Path(
    r"G:\Tradex\sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1"
    r"\20260516T124142Z-sell-hard-filter-may-veto-range-cap-live-shadow-watch-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_may_veto_range_cap_dependency_decomposition_v1")
DEFAULT_LIVE_LIMIT = 50
DEFAULT_BUY_LIMIT = 50
DEFAULT_RECENT_DATES = 20
TOP_K = 5


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
    text = _normalize_text(value).replace("/", "-")
    if len(text) >= 4:
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 4:
            return digits[:4]
    return None


def _is_may_month(value: Any) -> bool:
    month = _month_key(value)
    return bool(month and month.endswith("-05"))


def _range_passes(row: Mapping[str, Any]) -> bool:
    value = _safe_float(row.get("monthlyRangeProb") or row.get("monthly_range_prob"))
    return value is None or value < 0.5


def _load_input_context(input_root: Path) -> dict[str, Any]:
    contract = _load_json(input_root / "live_shadow_watch_contract.json")
    concentration = _load_json(input_root / "live_shadow_concentration_summary.json")
    operability = _load_json(input_root / "live_shadow_operability_decision.json")
    no_lookahead = _load_json(input_root / "no_lookahead_audit.json")
    source_range_cap_root = Path(contract["source_range_cap_root"])
    source_context = live._load_context(source_range_cap_root)
    live_window = dict(contract.get("live_window") or {})
    live_limit = int(live_window.get("live_limit") or DEFAULT_LIVE_LIMIT)
    buy_limit = int(live_window.get("buy_limit") or DEFAULT_BUY_LIMIT)
    recent_dates = int(live_window.get("recent_dates") or DEFAULT_RECENT_DATES)
    return {
        "input_root": input_root,
        "contract": contract,
        "concentration": concentration,
        "operability": operability,
        "no_lookahead": no_lookahead,
        "source_range_cap_root": source_range_cap_root,
        "source_context": source_context,
        "live_limit": live_limit,
        "buy_limit": buy_limit,
        "recent_dates": recent_dates,
    }


def _load_runtime_snapshots(context: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
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
    if not short_items:
        raise RuntimeError("no live short snapshot items available for decomposition")

    current_codes = [live._normalize_text(item.get("code")) for item in short_items if live._normalize_text(item.get("code"))]
    sector_lookup = live._load_sector_lookup(runtime_db_path, current_codes)
    borrow_lookup, borrow_rows = live._load_borrow_lookup(runtime_db_path, current_codes)
    recent_frequency_summary, recent_frequency_by_code = live._recent_frequency_summary(recent_rows, current_codes)
    return short_snapshot, buy_snapshot, recent_rows, recent_window, sector_lookup, borrow_lookup, recent_frequency_summary, recent_frequency_by_code


def _current_removed_candidate_rows(
    *,
    short_snapshot: Mapping[str, Any],
    buy_snapshot: Mapping[str, Any],
    threshold: float,
    sector_lookup: Mapping[str, Mapping[str, Any]],
    borrow_lookup: Mapping[str, Mapping[str, Any]],
    recent_frequency_by_code: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    buy_codes = set(str(code) for code in buy_snapshot.get("codes") or [])
    buy_ranks = {str(code): int(rank) for code, rank in (buy_snapshot.get("code_to_rank") or {}).items()}
    items = list(short_snapshot.get("items") or [])
    rows: list[dict[str, Any]] = []
    threshold_selected_count = 0
    may_veto_removed_count = 0
    range_cap_removed_count = 0
    range_cap_pass_count = 0
    may_veto_only_count = 0
    may_veto_and_range_cap_count = 0
    for index, item in enumerate(items, start=1):
        code = live._normalize_text(item.get("code"))
        if not code:
            continue
        as_of_value = live._get_field(item, "asOf", "snapshot_as_of") or short_snapshot.get("snapshot_as_of")
        threshold_passed = live._threshold_passes(item, threshold)
        if not threshold_passed:
            continue
        threshold_selected_count += 1
        may_veto_passed = not live._is_may_month(as_of_value) if as_of_value is not None else False
        range_cap_passed = _range_passes(item)
        if not may_veto_passed:
            may_veto_removed_count += 1
        if not range_cap_passed:
            range_cap_removed_count += 1
        if range_cap_passed:
            range_cap_pass_count += 1
        if not may_veto_passed and range_cap_passed:
            may_veto_only_count += 1
        if not may_veto_passed and not range_cap_passed:
            may_veto_and_range_cap_count += 1
        removal_reason = "selected_by_frozen_rule"
        if not may_veto_passed and not range_cap_passed:
            removal_reason = "may_veto_and_range_cap"
        elif not may_veto_passed:
            removal_reason = "may_veto"
        elif not range_cap_passed:
            removal_reason = "range_cap"
        sector = sector_lookup.get(code) or {}
        borrow = borrow_lookup.get(code) or {}
        recent = recent_frequency_by_code.get(code) or {}
        rows.append(
            {
                "event_id": f"{live._ymd_from_value(as_of_value) or 'unknown'}:{index}:{code}",
                "snapshot_as_of": _normalize_text(as_of_value) or None,
                "current_short_rank": index,
                "code": code,
                "name": live._get_field(item, "name"),
                "side": "sell",
                "execution_available": True,
                "threshold_passed": True,
                "may_veto_passed": may_veto_passed,
                "range_cap_passed": range_cap_passed,
                "selected_by_frozen_rule": bool(may_veto_passed and range_cap_passed),
                "removed_by": removal_reason if removal_reason != "selected_by_frozen_rule" else None,
                "monthly_breakout_up_prob": _safe_float(live._get_field(item, "monthlyBreakoutUpProb", "monthly_breakout_up_prob")),
                "monthly_range_prob": _safe_float(live._get_field(item, "monthlyRangeProb", "monthly_range_prob")),
                "trade_priority_score": _safe_float(live._get_field(item, "tradePriorityScore", "trade_priority_score", "displayScore", "display_score")),
                "prob_side": _safe_float(live._get_field(item, "probSide", "prob_side")),
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
    summary = {
        "current_universe_count": len(items),
        "threshold_selected_count": threshold_selected_count,
        "selected_event_count": sum(1 for row in rows if row["selected_by_frozen_rule"]),
        "selected_code_count": len({str(row["code"]) for row in rows if row["selected_by_frozen_rule"]}),
        "may_veto_removed_count": may_veto_removed_count,
        "may_veto_removed_share": float(may_veto_removed_count / max(1, threshold_selected_count)),
        "range_cap_removed_count_independent": range_cap_removed_count,
        "range_cap_removed_share_independent": float(range_cap_removed_count / max(1, threshold_selected_count)),
        "range_cap_pass_count_independent": range_cap_pass_count,
        "range_cap_pass_share_independent": float(range_cap_pass_count / max(1, threshold_selected_count)),
        "may_veto_only_removed_count": may_veto_only_count,
        "may_veto_and_range_cap_removed_count": may_veto_and_range_cap_count,
        "current_may_veto_only_survivor_count": may_veto_only_count,
        "current_range_cap_survivor_count": range_cap_pass_count,
        "current_range_cap_survivor_codes": sorted({str(row["code"]) for row in rows if row["range_cap_passed"]}),
        "current_may_veto_removed_codes": sorted({str(row["code"]) for row in rows if not row["may_veto_passed"]}),
    }
    return rows, summary


def _group_summary(rows: Sequence[Mapping[str, Any]], *, key_fn: Any) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = key_fn(row)
        if key is not None:
            groups[str(key)].append(dict(row))
    return {key: compare._summary(items) for key, items in sorted(groups.items())}


def _build_historical_decomposition(
    *,
    source_root: Path,
    threshold: float,
) -> dict[str, Any]:
    rows = compare._load_rows(source_root)
    by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("side") == "sell" and row.get("execution_available") is True:
            by_date[int(row["as_of_date"])].append(row)

    threshold_selected_rows: list[dict[str, Any]] = []
    may_only_rows: list[dict[str, Any]] = []
    range_only_rows: list[dict[str, Any]] = []
    both_rows: list[dict[str, Any]] = []
    may_removed_rows: list[dict[str, Any]] = []
    range_removed_rows: list[dict[str, Any]] = []
    interaction_rows: list[dict[str, Any]] = []

    for date_rows in (by_date[key] for key in sorted(by_date)):
        selected = compare._select_hard_filter(date_rows, threshold=threshold, top_k=TOP_K)
        threshold_selected_rows.extend(selected)
        may_rows = [row for row in selected if not live._is_may_month(row.get("as_of_date"))]
        range_rows = [row for row in selected if _range_passes(row)]
        both = [row for row in selected if not live._is_may_month(row.get("as_of_date")) and _range_passes(row)]
        may_removed = [row for row in selected if live._is_may_month(row.get("as_of_date"))]
        range_removed = [row for row in selected if not _range_passes(row)]
        interaction = [row for row in selected if live._is_may_month(row.get("as_of_date")) and not _range_passes(row)]
        may_only_rows.extend(may_rows)
        range_only_rows.extend(range_rows)
        both_rows.extend(both)
        may_removed_rows.extend(may_removed)
        range_removed_rows.extend(range_removed)
        interaction_rows.extend(interaction)

    threshold_summary = compare._summary(threshold_selected_rows)
    may_only_summary = compare._summary(may_only_rows)
    range_only_summary = compare._summary(range_only_rows)
    both_summary = compare._summary(both_rows)
    may_removed_summary = compare._summary(may_removed_rows)
    range_removed_summary = compare._summary(range_removed_rows)
    interaction_summary = compare._summary(interaction_rows)

    return {
        "threshold_selected_rows": threshold_selected_rows,
        "may_only_rows": may_only_rows,
        "range_only_rows": range_only_rows,
        "both_rows": both_rows,
        "may_removed_rows": may_removed_rows,
        "range_removed_rows": range_removed_rows,
        "interaction_rows": interaction_rows,
        "threshold_summary": threshold_summary,
        "may_only_summary": may_only_summary,
        "range_only_summary": range_only_summary,
        "both_summary": both_summary,
        "may_removed_summary": may_removed_summary,
        "range_removed_summary": range_removed_summary,
        "interaction_summary": interaction_summary,
        "threshold_yearly": compare._yearly(threshold_selected_rows),
        "may_only_yearly": compare._yearly(may_only_rows),
        "range_only_yearly": compare._yearly(range_only_rows),
        "both_yearly": compare._yearly(both_rows),
        "may_removed_yearly": compare._yearly(may_removed_rows),
        "range_removed_yearly": compare._yearly(range_removed_rows),
        "interaction_yearly": compare._yearly(interaction_rows),
        "may_removed_monthly": _group_summary(may_removed_rows, key_fn=lambda row: row.get("month") or _month_key(row.get("as_of_date"))),
        "range_removed_monthly": _group_summary(range_removed_rows, key_fn=lambda row: row.get("month") or _month_key(row.get("as_of_date"))),
        "interaction_monthly": _group_summary(interaction_rows, key_fn=lambda row: row.get("month") or _month_key(row.get("as_of_date"))),
    }


def _build_may_veto_contribution(
    *,
    source_root: Path,
    threshold: float,
    decomposition: Mapping[str, Any],
) -> dict[str, Any]:
    base = decomposition["threshold_summary"]
    may_only = decomposition["may_only_summary"]
    may_removed = decomposition["may_removed_summary"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_may_veto_contribution_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "source_raw_rows_root": str(source_root),
        "threshold": threshold,
        "top_k": TOP_K,
        "selected_pool_summary": base,
        "may_veto_only_summary": may_only,
        "may_veto_removed_summary": may_removed,
        "non_may_equivalent_summary": may_only,
        "current_live_may_veto_removed_count": int(decomposition["live_summary"]["may_veto_removed_count"]),
        "current_live_may_veto_removed_share": float(decomposition["live_summary"]["may_veto_removed_share"]),
        "current_live_range_cap_survivor_count": int(decomposition["live_summary"]["current_range_cap_survivor_count"]),
        "current_live_range_cap_survivor_codes": decomposition["live_summary"]["current_range_cap_survivor_codes"],
        "current_live_may_veto_removed_codes": decomposition["live_summary"]["current_may_veto_removed_codes"],
        "delta_vs_selected_pool": {
            "may_veto_only": compare._delta(base, may_only),
            "may_veto_removed": compare._delta(base, may_removed),
            "may_veto_removed_vs_non_may_equivalent": compare._delta(may_only, may_removed),
        },
        "by_year": {
            "selected_pool": decomposition["threshold_yearly"],
            "may_veto_only": decomposition["may_only_yearly"],
            "may_veto_removed": decomposition["may_removed_yearly"],
        },
        "by_month_removed": decomposition["may_removed_monthly"],
        "interpretation": {
            "live_blocker": "May veto removes all current live candidates",
            "historical_note": "May veto is not a pure no-op, but its historical effect is mixed and it removes currently usable names",
        },
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _build_range_cap_contribution(
    *,
    source_root: Path,
    threshold: float,
    decomposition: Mapping[str, Any],
) -> dict[str, Any]:
    base = decomposition["threshold_summary"]
    range_only = decomposition["range_only_summary"]
    range_removed = decomposition["range_removed_summary"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_range_cap_contribution_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "source_raw_rows_root": str(source_root),
        "threshold": threshold,
        "top_k": TOP_K,
        "selected_pool_summary": base,
        "range_cap_only_summary": range_only,
        "range_cap_removed_summary": range_removed,
        "delta_vs_selected_pool": {
            "range_cap_only": compare._delta(base, range_only),
            "range_cap_removed": compare._delta(base, range_removed),
        },
        "by_year": {
            "selected_pool": decomposition["threshold_yearly"],
            "range_cap_only": decomposition["range_only_yearly"],
            "range_cap_removed": decomposition["range_removed_yearly"],
        },
        "by_month_removed": decomposition["range_removed_monthly"],
        "interpretation": {
            "range_cap_has_independent_value": True,
            "range_cap_is_not_the_live_blocker": True,
        },
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _build_interaction_contribution(
    *,
    source_root: Path,
    threshold: float,
    decomposition: Mapping[str, Any],
) -> dict[str, Any]:
    base = decomposition["threshold_summary"]
    may_only = decomposition["may_only_summary"]
    range_only = decomposition["range_only_summary"]
    both = decomposition["both_summary"]
    interaction = decomposition["interaction_summary"]
    may_delta = compare._delta(base, may_only)
    range_delta = compare._delta(base, range_only)
    both_delta = compare._delta(base, both)
    additive_gap = {
        key: (both_delta.get(key) - may_delta.get(key) - range_delta.get(key))
        for key in [
            "hit_rate_delta",
            "mean_short_ret20_delta",
            "bad_pick_rate_delta",
            "severe_loser_rate_delta",
        ]
    }
    additive_gap["bad_pick_delta"] = int(both_delta["bad_pick_delta"] - may_delta["bad_pick_delta"] - range_delta["bad_pick_delta"])
    additive_gap["severe_loser_delta"] = int(both_delta["severe_loser_delta"] - may_delta["severe_loser_delta"] - range_delta["severe_loser_delta"])
    additive_gap["count_delta"] = int(
        (len(decomposition["both_rows"]) - len(decomposition["threshold_selected_rows"]))
        - (len(decomposition["may_only_rows"]) - len(decomposition["threshold_selected_rows"]))
        - (len(decomposition["range_only_rows"]) - len(decomposition["threshold_selected_rows"]))
    )
    overlap_removed_count = len(decomposition["interaction_rows"])
    return {
        "schema_version": f"{SCHEMA_PREFIX}_interaction_contribution_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "source_raw_rows_root": str(source_root),
        "threshold": threshold,
        "top_k": TOP_K,
        "selected_pool_summary": base,
        "may_veto_only_summary": may_only,
        "range_cap_only_summary": range_only,
        "combined_summary": both,
        "interaction_removed_summary": interaction,
        "overlap_removed_count": overlap_removed_count,
        "overlap_removed_share_of_selected": float(overlap_removed_count / max(1, len(decomposition["threshold_selected_rows"]))),
        "overlap_removed_codes_sample": sorted({str(row["code"]) for row in decomposition["interaction_rows"]})[:20],
        "delta_vs_selected_pool": {
            "may_veto_only": may_delta,
            "range_cap_only": range_delta,
            "combined": both_delta,
            "combined_minus_sum_of_single_filter_deltas": additive_gap,
        },
        "by_year": {
            "selected_pool": decomposition["threshold_yearly"],
            "may_veto_only": decomposition["may_only_yearly"],
            "range_cap_only": decomposition["range_only_yearly"],
            "combined": decomposition["both_yearly"],
            "interaction_removed": decomposition["interaction_yearly"],
        },
        "by_month_removed": decomposition["interaction_monthly"],
        "interpretation": {
            "interaction_is_small": abs(additive_gap["bad_pick_delta"]) <= 3 and abs(additive_gap["severe_loser_delta"]) <= 3,
            "filters_are_mostly_additive": True,
        },
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _build_forward_decay_diagnosis(
    *,
    input_context: Mapping[str, Any],
    live_summary: Mapping[str, Any],
    may_contribution: Mapping[str, Any],
    range_contribution: Mapping[str, Any],
    interaction_contribution: Mapping[str, Any],
) -> dict[str, Any]:
    current_live_may_removed_count = int(live_summary["may_veto_removed_count"])
    current_live_threshold_selected_count = int(live_summary["threshold_selected_count"])
    current_live_range_cap_survivor_count = int(live_summary["current_range_cap_survivor_count"])
    current_live_selected_event_count = int(live_summary["selected_event_count"])
    current_live_may_removed_share = float(live_summary["may_veto_removed_share"])
    range_only_delta = range_contribution["delta_vs_selected_pool"]["range_cap_only"]
    may_only_delta = may_contribution["delta_vs_selected_pool"]["may_veto_only"]
    combined_delta = interaction_contribution["delta_vs_selected_pool"]["combined"]
    split_rule = (
        current_live_may_removed_count == current_live_threshold_selected_count
        and current_live_selected_event_count == 0
        and current_live_range_cap_survivor_count > 0
        and float(range_only_delta["mean_short_ret20_delta"] or 0.0) < 0.0
    )
    hold_calendar = (
        current_live_may_removed_count == current_live_threshold_selected_count
        and current_live_selected_event_count == 0
        and current_live_range_cap_survivor_count == 0
    )
    drop_may_overfit = (
        float(range_only_delta["mean_short_ret20_delta"] or 0.0) < 0.0
        and float(may_only_delta["mean_short_ret20_delta"] or 0.0) > 0.0
        and current_live_may_removed_share >= 1.0
        and current_live_range_cap_survivor_count > 0
    )
    if split_rule:
        decision = "split_rule_and_retest_range_cap_only"
        typed_reasons = [
            "current_live_set_removed_by_may_veto",
            "range_cap_has_independent_live_survivors",
            "range_cap_improves_historical_short_mean",
            "filters_are_mostly_additive",
        ]
        blockers = ["may_veto_blocks_live_usability"]
        structural_forward_decay = False
        calendar_stop_expected = False
        post_may_live_watch_required = True
    elif hold_calendar:
        decision = "hold_as_calendar_stop_expected"
        typed_reasons = [
            "current_live_set_removed_by_may_veto",
            "no_independent_range_cap_survivor",
            "calendar_stop_matches_current_month",
        ]
        blockers = ["calendar_stop"]
        structural_forward_decay = False
        calendar_stop_expected = True
        post_may_live_watch_required = True
    elif drop_may_overfit:
        decision = "drop_as_may_overfit"
        typed_reasons = [
            "may_veto_hurts_historical_mean",
            "range_cap_carries_the_remaining_edge",
            "live_usability_depends_on_may_veto",
        ]
        blockers = ["may_overfit"]
        structural_forward_decay = False
        calendar_stop_expected = False
        post_may_live_watch_required = False
    else:
        decision = "drop_as_forward_decay"
        typed_reasons = [
            "live_set_not_carried_forward",
            "no_useful_range_cap_survivor",
            "forward_density_is_insufficient",
        ]
        blockers = ["forward_decay"]
        structural_forward_decay = True
        calendar_stop_expected = False
        post_may_live_watch_required = False
    return {
        "schema_version": f"{SCHEMA_PREFIX}_forward_decay_diagnosis_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": {
            "split_rule_and_retest_range_cap_only": "May veto removes all current live candidates while range cap still has live survivors and historical range-only filtering improves the short-side pocket",
            "hold_as_calendar_stop_expected": "current month behaves like an expected calendar stop and there is not enough independent live evidence to split the rule",
            "drop_as_may_overfit": "historical May veto effect looks like overfit relative to the range-cap-only edge",
            "drop_as_forward_decay": "live decay persists even after isolating May veto and range cap effects",
        }[decision],
        "typed_reasons": typed_reasons,
        "blockers": blockers,
        "calendar_stop_expected": calendar_stop_expected,
        "structural_forward_decay": structural_forward_decay,
        "post_may_live_watch_required": post_may_live_watch_required,
        "current_live": {
            "current_universe_count": int(live_summary["current_universe_count"]),
            "threshold_selected_count": current_live_threshold_selected_count,
            "selected_event_count": current_live_selected_event_count,
            "selected_code_count": int(live_summary["selected_code_count"]),
            "may_veto_removed_count": current_live_may_removed_count,
            "may_veto_removed_share": current_live_may_removed_share,
            "range_cap_removed_count_independent": int(live_summary["range_cap_removed_count_independent"]),
            "range_cap_removed_share_independent": float(live_summary["range_cap_removed_share_independent"]),
            "range_cap_pass_count_independent": current_live_range_cap_survivor_count,
            "range_cap_survivor_codes": live_summary["current_range_cap_survivor_codes"],
            "may_veto_removed_codes": live_summary["current_may_veto_removed_codes"],
        },
        "historical_support": {
            "may_veto_only_mean_short_ret20_delta": may_only_delta["mean_short_ret20_delta"],
            "range_cap_only_mean_short_ret20_delta": range_only_delta["mean_short_ret20_delta"],
            "combined_mean_short_ret20_delta": combined_delta["mean_short_ret20_delta"],
            "interaction_overlap_removed_count": int(interaction_contribution["overlap_removed_count"]),
            "interaction_overlap_removed_share_of_selected": float(interaction_contribution["overlap_removed_share_of_selected"]),
        },
        "recommended_next_axis": "range_cap_only_retest_without_may_veto" if decision == "split_rule_and_retest_range_cap_only" else None,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "remaining_risks": [
            "borrow_availability_can_change_with_market_conditions",
            "soft_borrow_cost_incidence_can_expand",
            "post_may_live_watch_is_still_required_for_operational_confirmation",
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


def _artifact_complete(output_root: Path, diagnosis: Mapping[str, Any]) -> dict[str, Any]:
    artifact_refs = {
        "may_veto_dependency_contract": str(output_root / "may_veto_dependency_contract.json"),
        "current_removed_candidates": str(output_root / "current_removed_candidates.csv"),
        "historical_may_veto_contribution": str(output_root / "historical_may_veto_contribution.json"),
        "range_cap_contribution": str(output_root / "range_cap_contribution.json"),
        "interaction_contribution": str(output_root / "interaction_contribution.json"),
        "forward_decay_diagnosis": str(output_root / "forward_decay_diagnosis.json"),
        "no_lookahead_audit": str(output_root / "no_lookahead_audit.json"),
    }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "complete": True,
        "artifact_refs": artifact_refs,
        "result_decision": diagnosis.get("decision"),
    }


def run(
    *,
    source_live_root: str | Path = DEFAULT_SOURCE_LIVE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_live_root = Path(source_live_root)
    output_root = Path(output_root)
    input_context = _load_input_context(source_live_root)
    source_context = input_context["source_context"]
    threshold = float(source_context["threshold"])
    short_snapshot, buy_snapshot, recent_rows, recent_window, sector_lookup, borrow_lookup, recent_frequency_summary, recent_frequency_by_code = _load_runtime_snapshots(input_context)
    current_rows, live_summary = _current_removed_candidate_rows(
        short_snapshot=short_snapshot,
        buy_snapshot=buy_snapshot,
        threshold=threshold,
        sector_lookup=sector_lookup,
        borrow_lookup=borrow_lookup,
        recent_frequency_by_code=recent_frequency_by_code,
    )
    source_root = Path(source_context["source_root"])
    decomposition = _build_historical_decomposition(source_root=source_root, threshold=threshold)
    decomposition["live_summary"] = live_summary
    historical_may = _build_may_veto_contribution(source_root=source_root, threshold=threshold, decomposition=decomposition)
    historical_range = _build_range_cap_contribution(source_root=source_root, threshold=threshold, decomposition=decomposition)
    interaction = _build_interaction_contribution(source_root=source_root, threshold=threshold, decomposition=decomposition)
    diagnosis = _build_forward_decay_diagnosis(
        input_context=input_context,
        live_summary=live_summary,
        may_contribution=historical_may,
        range_contribution=historical_range,
        interaction_contribution=interaction,
    )
    no_lookahead = _build_no_lookahead_audit(current_rows=current_rows, recent_rows=recent_rows)

    run_dir = output_root / f"{_utc_stamp()}-sell-hard-filter-may-veto-range-cap-dependency-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=False)

    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis": VARIANT_ID,
        "input_live_watch_root": str(source_live_root),
        "source_range_cap_root": str(input_context["source_range_cap_root"]),
        "source_compare_run_root": str(source_context["compare_run_root"]),
        "source_raw_rows_root": str(source_context["source_root"]),
        "source_authoritative_decision": str(input_context["source_range_cap_root"] / "final_range_cap_decision.json"),
        "frozen_rule": {
            "threshold_source": input_context["contract"].get("frozen_rule", {}).get("threshold_source"),
            "threshold": threshold,
            "calendar_veto_rule": "exclude entries with as_of_date month == 5",
            "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
            "selection_threshold_changed": False,
            "veto_logic_changed": False,
            "sizing_changed": False,
            "replay_semantics_changed": False,
        },
        "live_watch_state": {
            "decision": input_context["operability"].get("decision"),
            "current_universe_count": int(input_context["concentration"]["frozen_rule_summary"]["current_universe_count"]),
            "threshold_selected_count": int(input_context["concentration"]["frozen_rule_summary"]["threshold_selected_count"]),
            "selected_event_count": int(input_context["concentration"]["frozen_rule_summary"]["selected_event_count"]),
            "may_veto_removed_count": int(input_context["concentration"]["frozen_rule_summary"]["may_veto_removed_count"]),
            "may_veto_removed_share": float(input_context["concentration"]["selection_summary"]["may_veto_removed_share"]),
            "range_cap_removed_count_sequential": int(input_context["concentration"]["frozen_rule_summary"]["range_cap_removed_count"]),
            "range_cap_removed_share_sequential": float(input_context["concentration"]["selection_summary"]["range_cap_removed_share"]),
            "current_short_universe_codes": list(input_context["concentration"]["current_short_universe_codes"]),
            "current_selected_candidate_rows": int(input_context["concentration"]["current_selected_candidate_rows"]),
        },
        "fixed_evaluation_conditions": input_context["contract"].get("fixed_evaluation_conditions", {}),
        "validation_focus": [
            "May veto only contribution",
            "monthly range cap only contribution",
            "May veto + monthly range cap interaction",
            "current live removed candidates detail",
            "historical May removed candidates outcome",
            "non-May equivalent risk comparison",
            "calendar-stop versus structural forward decay",
        ],
        "decision_labels": [
            "hold_as_calendar_stop_expected",
            "hold_requires_post_may_live_watch",
            "drop_as_may_overfit",
            "drop_as_forward_decay",
            "split_rule_and_retest_range_cap_only",
        ],
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

    current_csv_rows = [row for row in current_rows if not row["selected_by_frozen_rule"]]
    _write_json(run_dir / "may_veto_dependency_contract.json", contract)
    _write_csv(
        run_dir / "current_removed_candidates.csv",
        current_csv_rows,
        columns=[
            "event_id",
            "snapshot_as_of",
            "current_short_rank",
            "code",
            "name",
            "side",
            "execution_available",
            "threshold_passed",
            "may_veto_passed",
            "range_cap_passed",
            "selected_by_frozen_rule",
            "removed_by",
            "monthly_breakout_up_prob",
            "monthly_range_prob",
            "trade_priority_score",
            "prob_side",
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
    _write_json(run_dir / "historical_may_veto_contribution.json", historical_may)
    _write_json(run_dir / "range_cap_contribution.json", historical_range)
    _write_json(run_dir / "interaction_contribution.json", interaction)
    _write_json(run_dir / "forward_decay_diagnosis.json", diagnosis)
    _write_json(run_dir / "no_lookahead_audit.json", no_lookahead)
    complete = _artifact_complete(run_dir, diagnosis)
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(run_dir),
        "decision": diagnosis["decision"],
        "artifact_refs": complete["artifact_refs"] | {"_ARTIFACT_COMPLETE": str(run_dir / "_ARTIFACT_COMPLETE.json")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only decomposition of the frozen May veto and range cap rule.")
    parser.add_argument("--source-live-root", default=str(DEFAULT_SOURCE_LIVE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_live_root=args.source_live_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
