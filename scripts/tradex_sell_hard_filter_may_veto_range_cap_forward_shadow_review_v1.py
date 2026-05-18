from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status  # noqa: E402
from app.backend.services.data.taisyaku_import import load_taisyaku_snapshot  # noqa: E402
from app.backend.services.ml import rankings_cache  # noqa: E402
from scripts import tradex_sell_monthly_breakout_hard_filter_compare_v1 as compare  # noqa: E402


SCHEMA_PREFIX = "sell_hard_filter_may_veto_range_cap_forward_shadow_review_v1"
VARIANT_ID = "may_veto_plus_monthly_range_lt_0_5_v1"
DEFAULT_SOURCE_RANGE_CAP_ROOT = Path(
    r"G:\Tradex\sell_hard_filter_may_veto_range_cap_v1"
    r"\20260516T115208Z-sell-hard-filter-may-veto-range-cap-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_may_veto_range_cap_forward_shadow_review_v1")
DEFAULT_TF = "D"
DEFAULT_WHICH = "latest"
DEFAULT_DIRECTION = "up"
DEFAULT_MODE = "trade"
DEFAULT_RISK_MODE = "balanced"
DEFAULT_BUY_LIMIT = 50
MIN_SELECTED_EVENT_COUNT = 50
MIN_SELECTED_MONTH_COUNT = 12
MIN_SELECTED_YEAR_COUNT = 3
MAX_CODE_TOP1_SHARE = 0.10
MAX_SECTOR_TOP1_SHARE = 0.25
MAX_HARD_BORROW_GAP_SHARE = 0.10
MAX_MAY_VETO_REMOVED_SHARE = 0.25
MAX_RANGE_CAP_REMOVED_SHARE = 0.15

EVENT_COLUMNS = [
    "event_id",
    "as_of_date",
    "entry_date",
    "exit_date",
    "year",
    "month",
    "rank",
    "code",
    "name",
    "side",
    "execution_available",
    "threshold_passed",
    "may_veto_passed",
    "range_cap_passed",
    "selected_by_frozen_rule",
    "monthly_breakout_up_prob",
    "monthly_range_prob",
    "short_ret20_next_open_to_20d_close",
    "bad_pick",
    "severe_loser",
    "good_candidate",
    "top5_member",
    "sector33_code",
    "sector33_name",
    "market_code",
    "borrow_snapshot_available",
    "borrow_hard_gap",
    "borrow_soft_cost",
    "borrow_restriction_count",
    "borrow_current_fee_yen",
    "borrow_loan_ratio",
    "current_buy_overlap",
    "current_buy_rank",
]


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
    if hasattr(value, "__class__") and value.__class__.__name__ == "NAType":
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
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
            writer.writerow({key: _json_ready(row.get(key)) for key in columns})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_rows(source_root: Path) -> list[dict[str, Any]]:
    path = source_root / "candidate_outcome_table_top50.jsonl"
    if not path.exists():
        raise FileNotFoundError(f"missing required source rows: {path}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _is_may_month(row: dict[str, Any]) -> bool:
    return int(str(int(row["as_of_date"]))[4:6]) == 5


def _range_allowed(row: dict[str, Any]) -> bool:
    value = _safe_float(row.get("monthly_range_prob"))
    if value is None:
        return True
    return value < 0.5


def _group_rows(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("side") == "sell" and row.get("execution_available") is True:
            grouped[int(row["as_of_date"])].append(dict(row))
    return grouped


def _select_threshold_rows(rows: list[dict[str, Any]], *, threshold: float) -> list[dict[str, Any]]:
    grouped = _group_rows(rows)
    selected: list[dict[str, Any]] = []
    for _, date_rows in sorted(grouped.items()):
        selected.extend(compare._select_hard_filter(date_rows, threshold=threshold, top_k=compare.TOP_K))
    return selected


def _select_baseline_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = _group_rows(rows)
    selected: list[dict[str, Any]] = []
    for _, date_rows in sorted(grouped.items()):
        selected.extend(compare._top_rows(date_rows, top_k=compare.TOP_K))
    return selected


def _select_frozen_rows(rows: list[dict[str, Any]], *, threshold: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped = _group_rows(rows)
    selected: list[dict[str, Any]] = []
    threshold_selected_count = 0
    may_removed_count = 0
    range_removed_count = 0
    for as_of_date, date_rows in sorted(grouped.items()):
        threshold_rows = compare._select_hard_filter(date_rows, threshold=threshold, top_k=compare.TOP_K)
        threshold_selected_count += len(threshold_rows)
        may_rows = [row for row in threshold_rows if not _is_may_month(row)]
        may_removed_count += len(threshold_rows) - len(may_rows)
        final_rows = [row for row in may_rows if _range_allowed(row)]
        range_removed_count += len(may_rows) - len(final_rows)
        for row in final_rows:
            row["selected_by_frozen_rule"] = True
            row["threshold_passed"] = not (row.get("monthly_breakout_up_prob") is not None and _safe_float(row.get("monthly_breakout_up_prob")) <= threshold)
            row["may_veto_passed"] = not _is_may_month(row)
            row["range_cap_passed"] = _range_allowed(row)
            row["frozen_rule"] = VARIANT_ID
            row["frozen_rule_passed"] = True
        selected.extend(final_rows)
    summary = {
        "threshold_selected_count": threshold_selected_count,
        "may_veto_removed_count": may_removed_count,
        "range_cap_removed_count": range_removed_count,
    }
    return selected, summary


def _load_sector_lookup(runtime_db_path: Path, codes: list[str]) -> dict[str, dict[str, Any]]:
    if not codes:
        return {}
    conn = duckdb.connect(str(runtime_db_path), read_only=True)
    try:
        placeholders = ",".join(["?"] * len(codes))
        rows = conn.execute(
            f"""
            SELECT code, name, sector33_code, sector33_name, market_code
            FROM industry_master
            WHERE code IN ({placeholders})
            """,
            codes,
        ).fetchall()
    finally:
        conn.close()
    lookup: dict[str, dict[str, Any]] = {}
    for code, name, sector33_code, sector33_name, market_code in rows:
        code_key = str(code).strip()
        if not code_key:
            continue
        lookup[code_key] = {
            "code": code_key,
            "name": str(name) if name is not None else None,
            "sector33_code": str(sector33_code) if sector33_code is not None else None,
            "sector33_name": str(sector33_name) if sector33_name is not None else None,
            "market_code": str(market_code) if market_code is not None else None,
        }
    return lookup


def _load_borrow_lookup(runtime_db_path: Path, codes: list[str]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    borrow_lookup: dict[str, dict[str, Any]] = {}
    report_rows: list[dict[str, Any]] = []
    for code in codes:
        snapshot = load_taisyaku_snapshot(code, db_path=runtime_db_path, history_limit=3)
        latest_balance = snapshot.get("latestBalance") if isinstance(snapshot, dict) else None
        latest_fee = snapshot.get("latestFee") if isinstance(snapshot, dict) else None
        restrictions = list(snapshot.get("restrictions") or []) if isinstance(snapshot, dict) else []
        has_snapshot = snapshot is not None
        current_fee = _safe_float(latest_fee.get("currentFeeYen")) if isinstance(latest_fee, dict) else None
        loan_ratio = _safe_float(latest_balance.get("loanRatio")) if isinstance(latest_balance, dict) else None
        hard_gap_reason = None
        if not has_snapshot:
            hard_gap_reason = "missing_snapshot"
        elif restrictions:
            hard_gap_reason = "restriction_notice"
        soft_cost_reasons: list[str] = []
        if current_fee is not None and current_fee > 0:
            soft_cost_reasons.append("current_fee_positive")
        if loan_ratio is not None and loan_ratio >= 1.0:
            soft_cost_reasons.append("loan_ratio_high")
        borrow_lookup[code] = {
            "code": code,
            "has_snapshot": has_snapshot,
            "hard_gap_reason": hard_gap_reason,
            "soft_cost_reasons": soft_cost_reasons,
            "restriction_count": len(restrictions),
            "current_fee_yen": current_fee,
            "loan_ratio": loan_ratio,
            "latest_issue_name": (
                str((latest_balance or {}).get("issueName") or (latest_fee or {}).get("issueName") or "")
                or None
            ),
            "latest_market_name": (
                str((latest_balance or {}).get("marketName") or (latest_fee or {}).get("marketName") or "")
                or None
            ),
        }
        report_rows.append(
            {
                "code": code,
                "has_snapshot": has_snapshot,
                "hard_gap_reason": hard_gap_reason,
                "soft_cost_reasons": soft_cost_reasons,
                "restriction_count": len(restrictions),
                "current_fee_yen": current_fee,
                "loan_ratio": loan_ratio,
            }
        )
    return borrow_lookup, report_rows


def _current_buy_snapshot() -> dict[str, Any]:
    freshness = get_rankings_freshness(tf=DEFAULT_TF, which=DEFAULT_WHICH, direction=DEFAULT_DIRECTION, mode=DEFAULT_MODE, risk_mode=DEFAULT_RISK_MODE, limit=DEFAULT_BUY_LIMIT)
    snapshot: dict[str, Any] = {
        "freshness": freshness,
        "available": bool(freshness.get("current_candidate_available")),
        "snapshot_as_of": freshness.get("snapshot_as_of"),
        "candidate_count": 0,
        "codes": [],
        "code_to_rank": {},
        "items": [],
    }
    if not snapshot["available"]:
        return snapshot
    payload = rankings_cache.get_rankings(DEFAULT_TF, DEFAULT_WHICH, DEFAULT_DIRECTION, DEFAULT_BUY_LIMIT, mode=DEFAULT_MODE, risk_mode=DEFAULT_RISK_MODE)
    candidates = list(payload.get("confirmed_actionable_buy_candidates") or [])
    snapshot["items"] = candidates
    snapshot["candidate_count"] = len(candidates)
    codes: list[str] = []
    code_to_rank: dict[str, int] = {}
    for index, item in enumerate(candidates, start=1):
        code = str(item.get("code") or "").strip()
        if not code:
            continue
        codes.append(code)
        code_to_rank[code] = index
    snapshot["codes"] = codes
    snapshot["code_to_rank"] = code_to_rank
    return snapshot


def _bucket_frequency(rows: list[dict[str, Any]], *, bucket_field: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        bucket = str(row.get(bucket_field) or "").strip()
        if bucket:
            buckets[bucket].append(row)
    result: list[dict[str, Any]] = []
    for bucket, items in sorted(buckets.items()):
        result.append(
            {
                bucket_field: bucket,
                "event_count": len(items),
                "day_count": len({int(item["as_of_date"]) for item in items}),
                "unique_code_count": len({str(item["code"]) for item in items}),
                "bad_pick_count": sum(1 for item in items if bool(item.get("bad_pick"))),
                "severe_loser_count": sum(1 for item in items if bool(item.get("severe_loser"))),
                "row_short_ret20_sum_proxy": float(sum(_safe_float(item.get("short_ret20_next_open_to_20d_close")) or 0.0 for item in items)),
                "row_short_ret20_mean_proxy": float(
                    sum(_safe_float(item.get("short_ret20_next_open_to_20d_close")) or 0.0 for item in items) / len(items)
                ),
            }
        )
    return result


def _concentration(rows: list[dict[str, Any]], *, sector_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    code_counts = Counter(str(row["code"]) for row in rows)
    sector_counts = Counter(
        str((sector_lookup.get(str(row["code"])) or {}).get("sector33_name") or (sector_lookup.get(str(row["code"])) or {}).get("market_code") or "<unknown>")
        for row in rows
    )
    total = len(rows) or 1
    top1_code = code_counts.most_common(1)[0][1] if code_counts else 0
    top3_code = sum(count for _, count in code_counts.most_common(3))
    top1_sector = sector_counts.most_common(1)[0][1] if sector_counts else 0
    top3_sector = sum(count for _, count in sector_counts.most_common(3))
    return {
        "code": {
            "unique_count": len(code_counts),
            "top1_code": code_counts.most_common(1)[0][0] if code_counts else None,
            "top1_count": top1_code,
            "top1_share": float(top1_code / total),
            "top3_count": top3_code,
            "top3_share": float(top3_code / total),
        },
        "sector": {
            "unique_count": len(sector_counts),
            "top1_sector": sector_counts.most_common(1)[0][0] if sector_counts else None,
            "top1_count": top1_sector,
            "top1_share": float(top1_sector / total),
            "top3_count": top3_sector,
            "top3_share": float(top3_sector / total),
        },
    }


def _augment_events(
    rows: list[dict[str, Any]],
    *,
    sector_lookup: dict[str, dict[str, Any]],
    borrow_lookup: dict[str, dict[str, Any]],
    current_buy_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    augmented: list[dict[str, Any]] = []
    current_buy_codes = set(str(code) for code in current_buy_snapshot.get("codes") or [])
    code_to_rank = {str(code): int(rank) for code, rank in (current_buy_snapshot.get("code_to_rank") or {}).items()}
    for row in rows:
        code = str(row.get("code") or "").strip()
        sector = sector_lookup.get(code) or {}
        borrow = borrow_lookup.get(code) or {}
        hard_gap = bool(borrow.get("hard_gap_reason"))
        soft_cost = bool(borrow.get("soft_cost_reasons"))
        selected = True
        augmented.append(
            {
                "event_id": f"{int(row['as_of_date'])}:{int(row['rank'])}:{code}",
                "as_of_date": int(row["as_of_date"]),
                "entry_date": int(row.get("entry_date")) if row.get("entry_date") is not None else None,
                "exit_date": int(row.get("exit_date")) if row.get("exit_date") is not None else None,
                "year": str(row.get("year") or "") or None,
                "month": str(row.get("month") or "") or None,
                "rank": int(row["rank"]),
                "code": code,
                "name": row.get("name"),
                "side": row.get("side"),
                "execution_available": bool(row.get("execution_available")),
                "threshold_passed": bool(row.get("threshold_passed")),
                "may_veto_passed": bool(row.get("may_veto_passed")),
                "range_cap_passed": bool(row.get("range_cap_passed")),
                "selected_by_frozen_rule": selected,
                "monthly_breakout_up_prob": _safe_float(row.get("monthly_breakout_up_prob")),
                "monthly_range_prob": _safe_float(row.get("monthly_range_prob")),
                "short_ret20_next_open_to_20d_close": _safe_float(row.get("short_ret20_next_open_to_20d_close")),
                "bad_pick": bool(row.get("bad_pick")),
                "severe_loser": bool(row.get("severe_loser")),
                "good_candidate": bool(row.get("good_candidate")) if "good_candidate" in row else None,
                "top5_member": bool(row.get("top5_member")) if "top5_member" in row else None,
                "sector33_code": sector.get("sector33_code"),
                "sector33_name": sector.get("sector33_name"),
                "market_code": sector.get("market_code"),
                "borrow_snapshot_available": bool(borrow.get("has_snapshot")),
                "borrow_hard_gap": hard_gap,
                "borrow_soft_cost": soft_cost,
                "borrow_restriction_count": int(borrow.get("restriction_count") or 0),
                "borrow_current_fee_yen": _safe_float(borrow.get("current_fee_yen")),
                "borrow_loan_ratio": _safe_float(borrow.get("loan_ratio")),
                "current_buy_overlap": code in current_buy_codes,
                "current_buy_rank": code_to_rank.get(code),
            }
        )
    return augmented


def _borrow_gap_report(
    rows: list[dict[str, Any]],
    *,
    borrow_lookup: dict[str, dict[str, Any]],
    sector_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    codes = sorted({str(row["code"]) for row in rows})
    code_counts = Counter(str(row["code"]) for row in rows)
    total_events = len(rows) or 1
    report_rows: list[dict[str, Any]] = []
    hard_gap_codes: list[str] = []
    soft_cost_codes: list[str] = []
    for code in codes:
        borrow = borrow_lookup.get(code) or {}
        sector = sector_lookup.get(code) or {}
        hard_gap_reason = borrow.get("hard_gap_reason")
        soft_cost_reasons = list(borrow.get("soft_cost_reasons") or [])
        selected_event_count = int(code_counts.get(code, 0))
        if hard_gap_reason:
            hard_gap_codes.append(code)
        if soft_cost_reasons:
            soft_cost_codes.append(code)
        report_rows.append(
            {
                "code": code,
                "name": sector.get("name"),
                "sector33_code": sector.get("sector33_code"),
                "sector33_name": sector.get("sector33_name"),
                "market_code": sector.get("market_code"),
                "selected_event_count": selected_event_count,
                "has_snapshot": bool(borrow.get("has_snapshot")),
                "borrowable": not hard_gap_reason,
                "hard_gap_reason": hard_gap_reason,
                "soft_cost_reasons": soft_cost_reasons,
                "restriction_count": int(borrow.get("restriction_count") or 0),
                "current_fee_yen": _safe_float(borrow.get("current_fee_yen")),
                "loan_ratio": _safe_float(borrow.get("loan_ratio")),
            }
        )
    hard_gap_event_count = sum(code_counts.get(code, 0) for code in hard_gap_codes)
    soft_cost_event_count = sum(code_counts.get(code, 0) for code in soft_cost_codes)
    hard_gap_code_count = len(hard_gap_codes)
    soft_cost_code_count = len(soft_cost_codes)
    summary = {
        "candidate_code_count": len(codes),
        "hard_gap_code_count": hard_gap_code_count,
        "hard_gap_event_count": hard_gap_event_count,
        "hard_gap_event_share": float(hard_gap_event_count / total_events),
        "soft_cost_code_count": soft_cost_code_count,
        "soft_cost_event_count": soft_cost_event_count,
        "soft_cost_event_share": float(soft_cost_event_count / total_events),
        "hard_gap_codes_sample": hard_gap_codes[:20],
        "soft_cost_codes_sample": soft_cost_codes[:20],
    }
    return {"summary": summary, "codes": report_rows}


def _build_decision(
    *,
    selection_summary: dict[str, Any],
    borrow_summary: dict[str, Any],
    buy_snapshot: dict[str, Any],
) -> dict[str, Any]:
    event_count = int(selection_summary["selected_event_count"])
    month_count = int(selection_summary["selected_month_count"])
    year_count = int(selection_summary["selected_year_count"])
    code_top1_share = float(selection_summary["code_top1_share"])
    sector_top1_share = float(selection_summary["sector_top1_share"])
    hard_gap_event_share = float(borrow_summary["hard_gap_event_share"])
    hard_gap_code_count = int(borrow_summary["hard_gap_code_count"])
    hard_gap_code_share = float(hard_gap_code_count / max(1, selection_summary["selected_code_count"]))
    may_veto_removed_share = float(selection_summary["may_veto_removed_share"])
    range_cap_removed_share = float(selection_summary["range_cap_removed_share"])
    buy_available = bool(buy_snapshot.get("available"))
    buy_candidate_count = int(buy_snapshot.get("candidate_count") or 0)
    buy_overlap_ready = buy_available and buy_candidate_count > 0

    blockers: list[str] = []
    if hard_gap_event_share >= MAX_HARD_BORROW_GAP_SHARE or hard_gap_code_share >= MAX_HARD_BORROW_GAP_SHARE:
        blockers.append("borrow_gap_too_large")
        decision = "drop_as_untradable_due_to_borrow_gap"
        reason = "borrow gap too large for forward shadow"
    elif may_veto_removed_share >= MAX_MAY_VETO_REMOVED_SHARE or range_cap_removed_share >= MAX_RANGE_CAP_REMOVED_SHARE or int(selection_summary["negative_year_count"]) > 0:
        blockers.append("calendar_rule_fragility")
        decision = "drop_as_calendar_overfit"
        reason = "calendar or range rules appear too fragile"
    elif (
        event_count < MIN_SELECTED_EVENT_COUNT
        or month_count < MIN_SELECTED_MONTH_COUNT
        or year_count < MIN_SELECTED_YEAR_COUNT
        or not buy_overlap_ready
        or code_top1_share > MAX_CODE_TOP1_SHARE
        or sector_top1_share > MAX_SECTOR_TOP1_SHARE
    ):
        if event_count < MIN_SELECTED_EVENT_COUNT:
            blockers.append("too_few_events")
        if month_count < MIN_SELECTED_MONTH_COUNT:
            blockers.append("too_few_months")
        if year_count < MIN_SELECTED_YEAR_COUNT:
            blockers.append("too_few_years")
        if not buy_overlap_ready:
            blockers.append("buy_overlap_unavailable")
        if code_top1_share > MAX_CODE_TOP1_SHARE:
            blockers.append("code_concentration_too_high")
        if sector_top1_share > MAX_SECTOR_TOP1_SHARE:
            blockers.append("sector_concentration_too_high")
        decision = "hold_requires_more_forward_data"
        reason = "forward data breadth or overlap is not yet sufficient"
    else:
        decision = "keep_for_forward_shadow"
        reason = "borrow gap is small and the shadow candidate looks operationally natural"

    typed_reasons = {
        "keep_for_forward_shadow": [
            "borrow_gap_small",
            "frequency_natural",
            "sector_and_code_concentration_not_excessive",
            "current_buy_snapshot_available",
            "calendar_rules_not_overfit",
            "range_cap_rules_not_overfit",
        ],
        "hold_requires_more_forward_data": blockers or ["forward_data_insufficient"],
        "drop_as_calendar_overfit": blockers or ["calendar_rule_fragility"],
        "drop_as_untradable_due_to_borrow_gap": blockers or ["borrow_gap_too_large"],
    }[decision]

    return {
        "schema_version": f"{SCHEMA_PREFIX}_forward_shadow_review_decision_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "typed_reasons": typed_reasons,
        "blockers": blockers,
        "source_authoritative_rollup_decision": None,
        "shadow_trade_candidate": decision == "keep_for_forward_shadow",
        "buy_level_equivalence_reached": True,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "forward_shadow_ready": decision == "keep_for_forward_shadow",
        "next_gate": "forward_shadow_monitoring_and_borrow_watch" if decision == "keep_for_forward_shadow" else None,
    }


def _load_context(source_range_cap_root: Path) -> dict[str, Any]:
    range_cap_decision = _load_json(source_range_cap_root / "final_range_cap_decision.json")
    range_cap_contract = _load_json(source_range_cap_root / "range_cap_contract.json")
    range_cap_compare = _load_json(source_range_cap_root / "range_cap_compare.json")
    no_lookahead = _load_json(source_range_cap_root / "no_lookahead_audit.json")
    yearly = _load_json(source_range_cap_root / "yearly_performance.json")
    monthly = _load_json(source_range_cap_root / "monthly_performance.json")
    compare_run_root = Path(range_cap_contract["source_compare_run_root"])
    compare_contract = _load_json(compare_run_root / "hard_filter_contract.json")
    compare_compare = _load_json(compare_run_root / "hard_filter_compare.json")
    compare_decision = _load_json(compare_run_root / "hard_filter_decision.json")
    compare_no_lookahead = _load_json(compare_run_root / "hard_filter_no_lookahead_audit.json")
    source_root = Path(compare_contract["source_root"])
    threshold = float(compare_contract["threshold"])
    source_runtime = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(tf=DEFAULT_TF, which=DEFAULT_WHICH, direction=DEFAULT_DIRECTION, mode=DEFAULT_MODE, risk_mode=DEFAULT_RISK_MODE, limit=DEFAULT_BUY_LIMIT)
    runtime_db_path = Path(source_runtime["selected_runtime_db_path"])
    return {
        "range_cap_decision": range_cap_decision,
        "range_cap_contract": range_cap_contract,
        "range_cap_compare": range_cap_compare,
        "range_cap_no_lookahead": no_lookahead,
        "range_cap_yearly": yearly,
        "range_cap_monthly": monthly,
        "compare_run_root": compare_run_root,
        "compare_contract": compare_contract,
        "compare_compare": compare_compare,
        "compare_decision": compare_decision,
        "compare_no_lookahead": compare_no_lookahead,
        "source_root": source_root,
        "threshold": threshold,
        "runtime_status": source_runtime,
        "rankings_freshness": rankings_freshness,
        "runtime_db_path": runtime_db_path,
    }


def run(
    *,
    source_range_cap_root: str | Path = DEFAULT_SOURCE_RANGE_CAP_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_range_cap_root = Path(source_range_cap_root)
    output_root = Path(output_root)
    context = _load_context(source_range_cap_root)
    source_rows = _load_rows(context["source_root"])
    baseline_rows = _select_baseline_rows(source_rows)
    threshold_rows = _select_threshold_rows(source_rows, threshold=context["threshold"])
    final_rows, selection_stats = _select_frozen_rows(source_rows, threshold=context["threshold"])
    buy_snapshot = _current_buy_snapshot()
    sector_lookup = _load_sector_lookup(context["runtime_db_path"], sorted({str(row["code"]) for row in final_rows}))
    borrow_lookup, borrow_report_rows = _load_borrow_lookup(context["runtime_db_path"], sorted({str(row["code"]) for row in final_rows}))
    enriched_rows = _augment_events(final_rows, sector_lookup=sector_lookup, borrow_lookup=borrow_lookup, current_buy_snapshot=buy_snapshot)
    borrow_report = _borrow_gap_report(final_rows, borrow_lookup=borrow_lookup, sector_lookup=sector_lookup)
    row_frequency_by_month = _bucket_frequency(final_rows, bucket_field="month")
    row_frequency_by_year = _bucket_frequency(final_rows, bucket_field="year")
    concentration = _concentration(final_rows, sector_lookup=sector_lookup)
    selected_codes = sorted({str(row["code"]) for row in final_rows})
    selected_days = sorted({int(row["as_of_date"]) for row in final_rows})
    selected_months = sorted({str(row["month"]) for row in final_rows})
    selected_years = sorted({str(row["year"]) for row in final_rows})
    selected_buy_overlap_codes = sorted({str(row["code"]) for row in final_rows if str(row["code"]) in set(buy_snapshot.get("codes") or [])})
    buy_overlap_event_count = sum(1 for row in final_rows if str(row["code"]) in set(buy_snapshot.get("codes") or []))
    buy_overlap_event_share = float(buy_overlap_event_count / max(1, len(final_rows)))
    hard_filter_branching = context["compare_compare"].get("delta") or {}
    range_cap_delta = context["range_cap_compare"].get("delta") or {}
    range_cap_challenger = context["range_cap_compare"].get("challenger") or {}
    source_may_veto = context["range_cap_compare"].get("source_may_veto") or {}
    yearly_performance = list((context["range_cap_yearly"].get("challenger") or []))
    monthly_performance = list((context["range_cap_monthly"].get("challenger") or []))
    negative_year_count = sum(1 for row in yearly_performance if str(row.get("classification") or "") == "negative")
    negative_month_count = sum(1 for row in monthly_performance if str(row.get("classification") or "") == "negative")
    worst_months = sorted(
        (
            {
                "month": row.get("month"),
                "return_on_base_capital": _safe_float(row.get("return_on_base_capital")),
                "trade_count": row.get("trade_count"),
                "classification": row.get("classification"),
            }
            for row in monthly_performance
        ),
        key=lambda item: (item["return_on_base_capital"] if item["return_on_base_capital"] is not None else math.inf),
    )[:5]
    best_months = sorted(
        (
            {
                "month": row.get("month"),
                "return_on_base_capital": _safe_float(row.get("return_on_base_capital")),
                "trade_count": row.get("trade_count"),
                "classification": row.get("classification"),
            }
            for row in monthly_performance
        ),
        key=lambda item: (item["return_on_base_capital"] if item["return_on_base_capital"] is not None else -math.inf),
        reverse=True,
    )[:5]
    selection_summary = {
        "selected_event_count": len(final_rows),
        "selected_day_count": len(selected_days),
        "selected_month_count": len(selected_months),
        "selected_year_count": len(selected_years),
        "selected_code_count": len(selected_codes),
        "selected_sector_count": len({(sector_lookup.get(code) or {}).get("sector33_name") or (sector_lookup.get(code) or {}).get("market_code") or "<unknown>" for code in selected_codes}),
        "code_top1_share": concentration["code"]["top1_share"],
        "code_top3_share": concentration["code"]["top3_share"],
        "sector_top1_share": concentration["sector"]["top1_share"],
        "sector_top3_share": concentration["sector"]["top3_share"],
        "may_veto_removed_share": float(selection_stats["may_veto_removed_count"] / max(1, selection_stats["threshold_selected_count"])),
        "range_cap_removed_share": float(
            selection_stats["range_cap_removed_count"]
            / max(1, selection_stats["threshold_selected_count"] - selection_stats["may_veto_removed_count"])
        ),
        "negative_year_count": negative_year_count,
    }
    decision = _build_decision(selection_summary=selection_summary, borrow_summary=borrow_report["summary"], buy_snapshot=buy_snapshot)
    decision["source_authoritative_rollup_decision"] = context["range_cap_decision"].get("decision")
    decision["source_buy_level_equivalence_reached"] = bool(context["range_cap_decision"].get("buy_level_equivalence_reached"))
    decision["shadow_trade_candidate"] = decision["decision"] == "keep_for_forward_shadow"
    decision["buy_level_equivalence_reached"] = bool(context["range_cap_decision"].get("buy_level_equivalence_reached"))

    selection_branching = {
        "changed_top5_members_count": hard_filter_branching.get("changed_top5_members_count"),
        "changed_rank_count": hard_filter_branching.get("changed_rank_count"),
        "filtered_baseline_top5_candidate_count": hard_filter_branching.get("filtered_baseline_top5_candidate_count"),
        "insufficient_refill_dates": hard_filter_branching.get("insufficient_refill_dates"),
    }
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_shadow_candidate_summary_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "source_artifacts": {
            "source_range_cap_root": str(source_range_cap_root),
            "source_compare_run_root": str(context["compare_run_root"]),
            "source_raw_rows_root": str(context["source_root"]),
            "source_range_cap_decision": str(source_range_cap_root / "final_range_cap_decision.json"),
            "source_range_cap_compare": str(source_range_cap_root / "range_cap_compare.json"),
            "source_hard_filter_compare": str(context["compare_run_root"] / "hard_filter_compare.json"),
        },
        "runtime_crosscheck": {
            "runtime_db_status": context["runtime_status"],
            "rankings_freshness": context["rankings_freshness"],
            "used_buy_overlap_gate": bool(buy_snapshot.get("available")),
        },
        "source_authoritative_result": {
            "decision": context["range_cap_decision"].get("decision"),
            "buy_level_equivalence_reached": bool(context["range_cap_decision"].get("buy_level_equivalence_reached")),
            "total_return": _safe_float(range_cap_challenger.get("total_return")),
            "max_drawdown": _safe_float(range_cap_challenger.get("max_drawdown")),
            "profit_factor": _safe_float(range_cap_challenger.get("profit_factor")),
            "bad_pick_delta": range_cap_delta.get("bad_pick_delta"),
            "severe_loser_delta": range_cap_delta.get("severe_loser_delta"),
        },
        "historical_branching": {
            "hard_filter": selection_branching,
            "source_may_veto_total_return": _safe_float(source_may_veto.get("total_return")),
            "source_may_veto_max_drawdown": _safe_float(source_may_veto.get("max_drawdown")),
            "source_may_veto_profit_factor": _safe_float(source_may_veto.get("profit_factor")),
        },
        "selection_summary": selection_summary,
        "frequency_summary": {
            "row_frequency_by_year": row_frequency_by_year,
            "row_frequency_by_month": row_frequency_by_month,
            "authoritative_yearly_performance": yearly_performance,
            "authoritative_monthly_performance": monthly_performance,
            "negative_year_count": negative_year_count,
            "negative_month_count": negative_month_count,
            "worst_months": worst_months,
            "best_months": best_months,
        },
        "concentration_summary": concentration,
        "borrow_summary": borrow_report["summary"],
        "buy_overlap_summary": {
            "available": bool(buy_snapshot.get("available")),
            "snapshot_as_of": buy_snapshot.get("snapshot_as_of"),
            "candidate_count": int(buy_snapshot.get("candidate_count") or 0),
            "overlap_code_count": len(selected_buy_overlap_codes),
            "overlap_event_count": buy_overlap_event_count,
            "overlap_event_share": buy_overlap_event_share,
            "overlap_codes": selected_buy_overlap_codes,
        },
        "fragility_summary": {
            "may_veto_removed_count": selection_stats["may_veto_removed_count"],
            "may_veto_removed_share": selection_summary["may_veto_removed_share"],
            "range_cap_removed_count": selection_stats["range_cap_removed_count"],
            "range_cap_removed_share": selection_summary["range_cap_removed_share"],
            "calendar_overfit_flag": selection_summary["may_veto_removed_share"] >= MAX_MAY_VETO_REMOVED_SHARE
            or selection_summary["range_cap_removed_share"] >= MAX_RANGE_CAP_REMOVED_SHARE
            or negative_year_count > 0,
        },
        "decision_summary": decision,
        "remaining_risks": [
            "forward_shadow_snapshot_is_current_only",
            "borrow_availability_can_change_with_market_conditions",
            "calendar_and_range_rules_should_be_live-monitored",
        ],
    }

    run_dir = output_root / f"{_utc_stamp()}-sell-hard-filter-may-veto-range-cap-forward-shadow-review-v1"
    run_dir.mkdir(parents=True, exist_ok=False)
    artifacts = {
        "shadow_review_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_shadow_review_contract_v1",
            "axis": VARIANT_ID,
            "source_range_cap_root": str(source_range_cap_root),
            "source_compare_run_root": str(context["compare_run_root"]),
            "source_raw_rows_root": str(context["source_root"]),
            "source_authoritative_decision": str(source_range_cap_root / "final_range_cap_decision.json"),
            "fixed_evaluation_conditions": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": True,
                "same_regime_condition": True,
                "same_cost_slippage": True,
                "same_artifact_detail_level": True,
                "no_lookahead_contract": True,
            },
            "frozen_rule": {
                "threshold_source": context["compare_contract"].get("threshold_source"),
                "threshold": context["threshold"],
                "calendar_veto_rule": "exclude entries with as_of_date month == 5",
                "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
                "selection_threshold_changed": False,
                "veto_logic_changed": False,
                "sizing_changed": False,
                "replay_semantics_changed": False,
            },
            "validation_focus": [
                "candidate_frequency_by_month_and_year",
                "concentration_by_sector_and_code",
                "overlap_with_current_buy_candidates_if_available",
                "bad_pick_and_severe_loser_prevention_stability",
                "drawdown_contribution",
                "borrow_availability_and_sellability_gap",
                "calendar_rule_fragility",
                "range_cap_fragility",
            ],
            "decision_labels": [
                "keep_for_forward_shadow",
                "hold_requires_more_forward_data",
                "drop_as_calendar_overfit",
                "drop_as_untradable_due_to_borrow_gap",
            ],
            "non_scope": [
                "MeeMee",
                "production ranking",
                "active champion",
                "publish",
                "live sell signal",
                "threshold tuning",
                "veto tuning",
                "sizing tuning",
                "replay semantics tuning",
            ],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
        "shadow_candidate_summary.json": summary,
        "borrow_availability_gap_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_borrow_availability_gap_report_v1",
            "generated_at_utc": _utc_now(),
            "axis": VARIANT_ID,
            "summary": borrow_report["summary"],
            "codes": borrow_report["codes"],
        },
        "forward_shadow_review_decision.json": decision,
        "no_lookahead_audit.json": {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "no_lookahead_pass": True,
            "selection_fields": ["as_of_date", "rank", "side", "execution_available", "monthly_breakout_up_prob"],
            "veto_fields": ["as_of_date_month", "monthly_range_prob"],
            "future_outcome_fields_used_in_selection_sizing_or_veto": [],
            "future_outcome_fields_used_in_review_only": [
                "short_ret20_next_open_to_20d_close",
                "bad_pick",
                "severe_loser",
                "name",
                "sector33_name",
                "market_code",
                "borrow_snapshot_available",
                "borrow_current_fee_yen",
                "borrow_loan_ratio",
                "current_buy_overlap",
            ],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    }
    for name, payload in artifacts.items():
        if name.endswith(".csv"):
            continue
        _write_json(run_dir / name, payload)
    _write_csv(run_dir / "shadow_candidate_daily_events.csv", enriched_rows, columns=EVENT_COLUMNS)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "status": "complete",
        "complete": True,
        "artifact_refs": {
            "shadow_review_contract": str(run_dir / "shadow_review_contract.json"),
            "shadow_candidate_daily_events": str(run_dir / "shadow_candidate_daily_events.csv"),
            "shadow_candidate_summary": str(run_dir / "shadow_candidate_summary.json"),
            "borrow_availability_gap_report": str(run_dir / "borrow_availability_gap_report.json"),
            "forward_shadow_review_decision": str(run_dir / "forward_shadow_review_decision.json"),
            "no_lookahead_audit": str(run_dir / "no_lookahead_audit.json"),
        },
        "authoritative_decision": str(run_dir / "forward_shadow_review_decision.json"),
        "decision": decision["decision"],
        "shadow_trade_candidate": decision["shadow_trade_candidate"],
        "buy_level_equivalence_reached": decision["buy_level_equivalence_reached"],
        "silent_fallback_used": False,
        "research_fallback": False,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
    }
    complete["artifact_refs"]["_ARTIFACT_COMPLETE"] = str(run_dir / "_ARTIFACT_COMPLETE.json")
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": decision["decision"],
        "artifact_refs": complete["artifact_refs"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only forward shadow review gate for the frozen sell hard-filter.")
    parser.add_argument("--source-range-cap-root", default=str(DEFAULT_SOURCE_RANGE_CAP_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_range_cap_root=args.source_range_cap_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
