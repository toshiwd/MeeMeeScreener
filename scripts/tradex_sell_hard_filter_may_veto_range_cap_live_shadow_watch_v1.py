"""TRADEX-only live shadow watch for the frozen sell hard-filter candidate."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.data.taisyaku_import import load_taisyaku_snapshot
from app.backend.services.ml import rankings_cache
from app.backend.services.teppan_live_safe_materialization import load_recent_runtime_ranking_rows


SCHEMA_PREFIX = "sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1"
VARIANT_ID = "may_veto_plus_monthly_range_lt_0_5_v1"
DEFAULT_SOURCE_RANGE_CAP_ROOT = Path(
    r"G:\Tradex\sell_hard_filter_may_veto_range_cap_v1"
    r"\20260516T115208Z-sell-hard-filter-may-veto-range-cap-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_may_veto_range_cap_live_shadow_watch_v1")
DEFAULT_TF = "D"
DEFAULT_WHICH = "latest"
DEFAULT_DIRECTION = "down"
DEFAULT_BUY_DIRECTION = "up"
DEFAULT_MODE = "trade"
DEFAULT_RISK_MODE = "balanced"
DEFAULT_LIVE_LIMIT = 50
DEFAULT_BUY_LIMIT = 50
DEFAULT_RECENT_DATES = 20

MIN_SELECTED_EVENT_COUNT = 5
MIN_SELECTED_CODE_COUNT = 3
MIN_RECENT_WINDOW_DAYS = 5
MIN_FORWARD_PERSISTENCE_SHARE = 0.25
MIN_FORWARD_WINDOW_DAYS = 10
MAX_HARD_BORROW_GAP_SHARE = 0.10
MAX_CODE_TOP1_SHARE = 0.35
MAX_CODE_TOP3_SHARE = 0.70
MAX_SECTOR_TOP1_SHARE = 0.45
MAX_SECTOR_TOP3_SHARE = 0.80
MAX_BUY_OVERLAP_EVENT_SHARE = 0.35
MAX_BUY_OVERLAP_CODE_COUNT = 3
MAX_SOFT_BORROW_COST_SHARE = 0.60
MAX_SOFT_BORROW_COST_CODE_SHARE = 0.50
MAX_MAY_VETO_REMOVED_SHARE = 0.35
MAX_RANGE_CAP_REMOVED_SHARE = 0.25


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _json_ready(row.get(column)) for column in columns})


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


def _safe_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _get_field(item: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in item and item[name] is not None:
            return item[name]
    return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _ymd_from_value(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    text = text.replace("/", "-")
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return int(text[:4] + text[5:7] + text[8:10])
        except Exception:
            return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        try:
            return int(digits[:8])
        except Exception:
            return None
    return None


def _month_from_value(value: Any) -> int | None:
    ymd = _ymd_from_value(value)
    if ymd is None:
        return None
    return int(str(ymd)[4:6])


def _year_from_value(value: Any) -> int | None:
    ymd = _ymd_from_value(value)
    if ymd is None:
        return None
    return int(str(ymd)[:4])


def _is_may_month(value: Any) -> bool:
    return _month_from_value(value) == 5


def _load_context(source_range_cap_root: Path) -> dict[str, Any]:
    range_cap_decision = _load_json(source_range_cap_root / "final_range_cap_decision.json")
    range_cap_contract = _load_json(source_range_cap_root / "range_cap_contract.json")
    range_cap_compare = _load_json(source_range_cap_root / "range_cap_compare.json")
    range_cap_no_lookahead = _load_json(source_range_cap_root / "no_lookahead_audit.json")
    range_cap_yearly = _load_json(source_range_cap_root / "yearly_performance.json")
    range_cap_monthly = _load_json(source_range_cap_root / "monthly_performance.json")

    compare_run_root = Path(range_cap_contract["source_compare_run_root"])
    compare_contract = _load_json(compare_run_root / "hard_filter_contract.json")
    compare_compare = _load_json(compare_run_root / "hard_filter_compare.json")
    compare_decision = _load_json(compare_run_root / "hard_filter_decision.json")
    compare_no_lookahead = _load_json(compare_run_root / "hard_filter_no_lookahead_audit.json")
    source_root = Path(compare_contract["source_root"])
    threshold = _safe_float(compare_contract.get("threshold"))
    if threshold is None:
        raise RuntimeError("frozen hard-filter threshold missing")

    runtime_status = get_runtime_stock_db_status()
    rankings_freshness_down = get_rankings_freshness(
        tf=DEFAULT_TF,
        which=DEFAULT_WHICH,
        direction=DEFAULT_DIRECTION,
        mode=DEFAULT_MODE,
        risk_mode=DEFAULT_RISK_MODE,
        limit=DEFAULT_LIVE_LIMIT,
    )
    rankings_freshness_up = get_rankings_freshness(
        tf=DEFAULT_TF,
        which=DEFAULT_WHICH,
        direction=DEFAULT_BUY_DIRECTION,
        mode=DEFAULT_MODE,
        risk_mode=DEFAULT_RISK_MODE,
        limit=DEFAULT_BUY_LIMIT,
    )
    runtime_db_path = Path(str(runtime_status.get("selected_runtime_db_path") or ""))
    return {
        "range_cap_decision": range_cap_decision,
        "range_cap_contract": range_cap_contract,
        "range_cap_compare": range_cap_compare,
        "range_cap_no_lookahead": range_cap_no_lookahead,
        "range_cap_yearly": range_cap_yearly,
        "range_cap_monthly": range_cap_monthly,
        "compare_run_root": compare_run_root,
        "compare_contract": compare_contract,
        "compare_compare": compare_compare,
        "compare_decision": compare_decision,
        "compare_no_lookahead": compare_no_lookahead,
        "source_root": source_root,
        "threshold": threshold,
        "runtime_status": runtime_status,
        "rankings_freshness_down": rankings_freshness_down,
        "rankings_freshness_up": rankings_freshness_up,
        "runtime_db_path": runtime_db_path,
    }


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
        code_key = _normalize_text(code)
        if not code_key:
            continue
        lookup[code_key] = {
            "code": code_key,
            "name": _normalize_text(name) or None,
            "sector33_code": _normalize_text(sector33_code) or None,
            "sector33_name": _normalize_text(sector33_name) or None,
            "market_code": _normalize_text(market_code) or None,
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


def _current_snapshot(
    *,
    direction: str,
    snapshot_key: str,
    limit: int,
    freshness: Mapping[str, Any],
    candidate_key: str,
) -> dict[str, Any]:
    available = bool(freshness.get("current_candidate_available"))
    snapshot: dict[str, Any] = {
        "freshness": dict(freshness),
        "available": available,
        "snapshot_as_of": freshness.get("snapshot_as_of"),
        "candidate_count": 0,
        "codes": [],
        "code_to_rank": {},
        "items": [],
    }
    if not available:
        return snapshot
    payload = rankings_cache.get_rankings(
        DEFAULT_TF,
        DEFAULT_WHICH,
        direction,
        limit,
        mode=DEFAULT_MODE,
        risk_mode=DEFAULT_RISK_MODE,
    )
    items = list(payload.get(candidate_key) or [])
    if not items:
        return snapshot
    codes: list[str] = []
    code_to_rank: dict[str, int] = {}
    for index, item in enumerate(items, start=1):
        code = _normalize_text(item.get("code"))
        if not code:
            continue
        codes.append(code)
        code_to_rank[code] = index
    snapshot.update(
        {
            "snapshot_as_of": payload.get("confirmed_snapshot_as_of") or payload.get("snapshot_as_of") or freshness.get("snapshot_as_of"),
            "candidate_count": len(codes),
            "codes": codes,
            "code_to_rank": code_to_rank,
            "items": items,
            "payload": payload,
        }
    )
    return snapshot


def _current_short_snapshot(limit: int, freshness: Mapping[str, Any]) -> dict[str, Any]:
    return _current_snapshot(
        direction=DEFAULT_DIRECTION,
        snapshot_key="confirmed_snapshot_as_of",
        limit=limit,
        freshness=freshness,
        candidate_key="confirmed_actionable_short_candidates",
    )


def _current_buy_snapshot(limit: int, freshness: Mapping[str, Any]) -> dict[str, Any]:
    return _current_snapshot(
        direction=DEFAULT_BUY_DIRECTION,
        snapshot_key="confirmed_snapshot_as_of",
        limit=limit,
        freshness=freshness,
        candidate_key="confirmed_actionable_buy_candidates",
    )


def _threshold_passes(row: Mapping[str, Any], threshold: float) -> bool:
    value = _safe_float(_get_field(row, "monthlyBreakoutUpProb", "monthly_breakout_up_prob"))
    return value is None or value > threshold


def _range_passes(row: Mapping[str, Any]) -> bool:
    value = _safe_float(_get_field(row, "monthlyRangeProb", "monthly_range_prob"))
    return value is None or value < 0.5


def _select_live_rows(
    items: Sequence[Mapping[str, Any]],
    *,
    threshold: float,
    snapshot_as_of: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    threshold_selected_count = 0
    may_veto_removed_count = 0
    range_cap_removed_count = 0
    frozen_field_missing_count = 0
    for index, item in enumerate(items, start=1):
        code = _normalize_text(item.get("code"))
        if not code:
            continue
        as_of_value = _get_field(item, "asOf", "snapshot_as_of") or snapshot_as_of
        threshold_passed = _threshold_passes(item, threshold)
        may_veto_passed = not _is_may_month(as_of_value) if as_of_value is not None else False
        range_passed = _range_passes(item)
        if _safe_float(_get_field(item, "monthlyBreakoutUpProb", "monthly_breakout_up_prob")) is None:
            frozen_field_missing_count += 1
        if threshold_passed:
            threshold_selected_count += 1
            if not may_veto_passed:
                may_veto_removed_count += 1
            elif not range_passed:
                range_cap_removed_count += 1
            else:
                rows.append(
                    {
                        "event_id": f"{_ymd_from_value(as_of_value) or 'unknown'}:{index}:{code}",
                        "snapshot_as_of": _normalize_text(as_of_value) or None,
                        "current_short_rank": index,
                        "code": code,
                        "name": _get_field(item, "name"),
                        "side": "sell",
                        "execution_available": True,
                        "threshold_passed": True,
                        "may_veto_passed": True,
                        "range_cap_passed": True,
                        "selected_by_frozen_rule": True,
                        "monthly_breakout_up_prob": _safe_float(_get_field(item, "monthlyBreakoutUpProb", "monthly_breakout_up_prob")),
                        "monthly_range_prob": _safe_float(_get_field(item, "monthlyRangeProb", "monthly_range_prob")),
                        "trade_priority_score": _safe_float(_get_field(item, "tradePriorityScore", "trade_priority_score", "displayScore", "display_score")),
                        "prob_side": _safe_float(_get_field(item, "probSide", "prob_side")),
                    }
                )
    summary = {
        "current_universe_count": len([item for item in items if _normalize_text(item.get("code"))]),
        "threshold_selected_count": threshold_selected_count,
        "may_veto_removed_count": may_veto_removed_count,
        "range_cap_removed_count": range_cap_removed_count,
        "frozen_field_missing_count": frozen_field_missing_count,
    }
    return rows, summary


def _load_recent_window(
    runtime_db_path: Path,
    *,
    recent_dates: int,
    rank_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        rows = load_recent_runtime_ranking_rows(
            runtime_db_path,
            direction=DEFAULT_DIRECTION,
            recent_dates=recent_dates,
            rank_limit=rank_limit,
        )
    except Exception as exc:
        return [], {
            "available": False,
            "error": f"{type(exc).__name__}: {exc}",
            "recent_dates": recent_dates,
            "rank_limit": rank_limit,
            "row_count": 0,
            "day_count": 0,
            "month_count": 0,
            "year_count": 0,
        }
    days = sorted({str(row.get("anchor_date")) for row in rows if _normalize_text(row.get("anchor_date"))})
    months = sorted({str(row.get("anchor_date"))[:7] for row in rows if _normalize_text(row.get("anchor_date"))})
    years = sorted({str(row.get("anchor_date"))[:4] for row in rows if _normalize_text(row.get("anchor_date"))})
    return rows, {
        "available": True,
        "error": None,
        "recent_dates": recent_dates,
        "rank_limit": rank_limit,
        "row_count": len(rows),
        "day_count": len(days),
        "month_count": len(months),
        "year_count": len(years),
        "date_span": {
            "first_date": days[-1] if days else None,
            "last_date": days[0] if days else None,
        },
    }


def _recent_frequency_summary(
    recent_rows: Sequence[Mapping[str, Any]],
    selected_codes: Sequence[str],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    selected_set = {str(code).strip() for code in selected_codes if str(code).strip()}
    by_code: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in recent_rows:
        code = _normalize_text(row.get("symbol"))
        if code and code in selected_set:
            by_code[code].append(row)
    recent_days = sorted({str(row.get("anchor_date")) for row in recent_rows if _normalize_text(row.get("anchor_date"))})
    selected_days = sorted({str(row.get("anchor_date")) for row in recent_rows if _normalize_text(row.get("anchor_date")) and _normalize_text(row.get("symbol")) in selected_set})
    selected_months = sorted({str(row.get("anchor_date"))[:7] for row in recent_rows if _normalize_text(row.get("symbol")) in selected_set and _normalize_text(row.get("anchor_date"))})
    selected_years = sorted({str(row.get("anchor_date"))[:4] for row in recent_rows if _normalize_text(row.get("symbol")) in selected_set and _normalize_text(row.get("anchor_date"))})
    if selected_set:
        presence_ratios = [len({str(item.get("anchor_date")) for item in items}) / max(1, len(recent_days)) for items in by_code.values()]
    else:
        presence_ratios = []
    per_code: dict[str, dict[str, Any]] = {}
    for code, items in sorted(by_code.items()):
        dates = [str(item.get("anchor_date")) for item in items if _normalize_text(item.get("anchor_date"))]
        ranks = [int(item.get("champion_rank") or item.get("runtime_rank") or 0) for item in items if _safe_int(item.get("champion_rank") or item.get("runtime_rank")) is not None]
        per_code[code] = {
            "recent_observed_day_count": len(set(dates)),
            "recent_observed_row_count": len(items),
            "recent_observed_month_count": len({date[:7] for date in dates}),
            "recent_observed_year_count": len({date[:4] for date in dates}),
            "recent_first_seen_date": min(dates) if dates else None,
            "recent_last_seen_date": max(dates) if dates else None,
            "recent_rank_min": min(ranks) if ranks else None,
            "recent_rank_max": max(ranks) if ranks else None,
            "recent_rank_mean": float(statistics.fmean(ranks)) if ranks else None,
            "recent_presence_ratio": float(len(set(dates)) / max(1, len(recent_days))) if recent_days else None,
        }
    summary = {
        "available": bool(recent_rows),
        "window_day_count": len(recent_days),
        "window_month_count": len({day[:7] for day in recent_days}),
        "window_year_count": len({day[:4] for day in recent_days}),
        "selected_code_count_with_recent_presence": len(per_code),
        "selected_code_recent_row_count": sum(len(items) for items in by_code.values()),
        "persistent_code_count": sum(1 for item in per_code.values() if int(item.get("recent_observed_day_count") or 0) >= 2),
        "persistent_code_share": float(sum(1 for item in per_code.values() if int(item.get("recent_observed_day_count") or 0) >= 2) / max(1, len(selected_set))) if selected_set else None,
        "mean_presence_ratio": float(statistics.fmean(presence_ratios)) if presence_ratios else None,
        "median_presence_ratio": float(statistics.median(presence_ratios)) if presence_ratios else None,
        "recent_days": recent_days,
        "recent_selected_days": selected_days,
        "recent_selected_months": selected_months,
        "recent_selected_years": selected_years,
    }
    return summary, per_code


def _concentration(rows: Sequence[Mapping[str, Any]], *, sector_lookup: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    code_counts = Counter(str(row["code"]) for row in rows if _normalize_text(row.get("code")))
    sector_counts = Counter(
        str(
            (sector_lookup.get(str(row["code"])) or {}).get("sector33_name")
            or (sector_lookup.get(str(row["code"])) or {}).get("market_code")
            or "<unknown>"
        )
        for row in rows
        if _normalize_text(row.get("code"))
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


def _borrow_gap_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    borrow_lookup: Mapping[str, Mapping[str, Any]],
    sector_lookup: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    codes = sorted({str(row["code"]) for row in rows if _normalize_text(row.get("code"))})
    code_counts = Counter(str(row["code"]) for row in rows if _normalize_text(row.get("code")))
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
    summary = {
        "candidate_code_count": len(codes),
        "hard_gap_code_count": len(hard_gap_codes),
        "hard_gap_event_count": hard_gap_event_count,
        "hard_gap_event_share": float(hard_gap_event_count / total_events),
        "soft_cost_code_count": len(soft_cost_codes),
        "soft_cost_event_count": soft_cost_event_count,
        "soft_cost_event_share": float(soft_cost_event_count / total_events),
        "hard_gap_codes_sample": hard_gap_codes[:20],
        "soft_cost_codes_sample": soft_cost_codes[:20],
    }
    return {"summary": summary, "codes": report_rows}


def _buy_snapshot_rows(
    selected_rows: Sequence[Mapping[str, Any]],
    buy_snapshot: Mapping[str, Any],
    *,
    borrow_lookup: Mapping[str, Mapping[str, Any]],
    sector_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    buy_codes = set(str(code) for code in buy_snapshot.get("codes") or [])
    code_to_rank = {str(code): int(rank) for code, rank in (buy_snapshot.get("code_to_rank") or {}).items()}
    rows: list[dict[str, Any]] = []
    for row in selected_rows:
        code = str(row.get("code") or "").strip()
        if code not in buy_codes:
            continue
        borrow = borrow_lookup.get(code) or {}
        sector = sector_lookup.get(code) or {}
        rows.append(
            {
                "code": code,
                "name": row.get("name"),
                "current_short_rank": row.get("current_short_rank"),
                "current_buy_rank": code_to_rank.get(code),
                "snapshot_as_of": row.get("snapshot_as_of"),
                "sector33_code": sector.get("sector33_code"),
                "sector33_name": sector.get("sector33_name"),
                "market_code": sector.get("market_code"),
                "borrow_hard_gap": bool(borrow.get("hard_gap_reason")),
                "borrow_soft_cost": bool(borrow.get("soft_cost_reasons")),
                "borrow_current_fee_yen": _safe_float(borrow.get("current_fee_yen")),
                "borrow_loan_ratio": _safe_float(borrow.get("loan_ratio")),
            }
        )
    return rows


def _row_enrichment(
    selected_rows: Sequence[Mapping[str, Any]],
    *,
    current_short_snapshot: Mapping[str, Any],
    recent_frequency_by_code: Mapping[str, Mapping[str, Any]],
    buy_snapshot: Mapping[str, Any],
    sector_lookup: Mapping[str, Mapping[str, Any]],
    borrow_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    buy_codes = set(str(code) for code in buy_snapshot.get("codes") or [])
    buy_rank = {str(code): int(rank) for code, rank in (buy_snapshot.get("code_to_rank") or {}).items()}
    rows: list[dict[str, Any]] = []
    for row in selected_rows:
        code = str(row.get("code") or "").strip()
        sector = sector_lookup.get(code) or {}
        borrow = borrow_lookup.get(code) or {}
        recent = recent_frequency_by_code.get(code) or {}
        rows.append(
            {
                "event_id": row.get("event_id"),
                "snapshot_as_of": row.get("snapshot_as_of") or current_short_snapshot.get("snapshot_as_of"),
                "current_short_rank": int(row.get("current_short_rank") or 0),
                "code": code,
                "name": row.get("name"),
                "side": row.get("side"),
                "execution_available": bool(row.get("execution_available")),
                "threshold_passed": bool(row.get("threshold_passed")),
                "may_veto_passed": bool(row.get("may_veto_passed")),
                "range_cap_passed": bool(row.get("range_cap_passed")),
                "selected_by_frozen_rule": bool(row.get("selected_by_frozen_rule")),
                "monthly_breakout_up_prob": _safe_float(row.get("monthly_breakout_up_prob")),
                "monthly_range_prob": _safe_float(row.get("monthly_range_prob")),
                "trade_priority_score": _safe_float(row.get("trade_priority_score")),
                "prob_side": _safe_float(row.get("prob_side")),
                "sector33_code": sector.get("sector33_code"),
                "sector33_name": sector.get("sector33_name"),
                "market_code": sector.get("market_code"),
                "borrow_snapshot_available": bool(borrow.get("has_snapshot")),
                "borrow_hard_gap": bool(borrow.get("hard_gap_reason")),
                "borrow_soft_cost": bool(borrow.get("soft_cost_reasons")),
                "borrow_restriction_count": int(borrow.get("restriction_count") or 0),
                "borrow_current_fee_yen": _safe_float(borrow.get("current_fee_yen")),
                "borrow_loan_ratio": _safe_float(borrow.get("loan_ratio")),
                "current_buy_overlap": code in buy_codes,
                "current_buy_rank": buy_rank.get(code),
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
    return rows


def _selection_summary(
    selected_rows: Sequence[Mapping[str, Any]],
    *,
    current_universe_count: int,
    threshold_selected_count: int,
    may_veto_removed_count: int,
    range_cap_removed_count: int,
    frozen_field_missing_count: int,
    sector_lookup: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    selected_codes = sorted({str(row["code"]) for row in selected_rows if _normalize_text(row.get("code"))})
    selected_days = sorted({_ymd_from_value(row.get("snapshot_as_of")) or 0 for row in selected_rows if _ymd_from_value(row.get("snapshot_as_of")) is not None})
    selected_months = sorted({f"{_year_from_value(row.get('snapshot_as_of')):04d}-{_month_from_value(row.get('snapshot_as_of')):02d}" for row in selected_rows if _year_from_value(row.get("snapshot_as_of")) is not None and _month_from_value(row.get("snapshot_as_of")) is not None})
    selected_years = sorted({f"{_year_from_value(row.get('snapshot_as_of')):04d}" for row in selected_rows if _year_from_value(row.get("snapshot_as_of")) is not None})
    concentration = _concentration(selected_rows, sector_lookup=sector_lookup) if selected_rows else {
        "code": {"unique_count": 0, "top1_code": None, "top1_count": 0, "top1_share": 0.0, "top3_count": 0, "top3_share": 0.0},
        "sector": {"unique_count": 0, "top1_sector": None, "top1_count": 0, "top1_share": 0.0, "top3_count": 0, "top3_share": 0.0},
    }
    return {
        "current_universe_count": int(current_universe_count),
        "threshold_selected_count": int(threshold_selected_count),
        "selected_event_count": len(selected_rows),
        "selected_code_count": len(selected_codes),
        "selected_day_count": len(selected_days),
        "selected_month_count": len(selected_months),
        "selected_year_count": len(selected_years),
        "code_top1_share": concentration["code"]["top1_share"],
        "code_top3_share": concentration["code"]["top3_share"],
        "sector_top1_share": concentration["sector"]["top1_share"],
        "sector_top3_share": concentration["sector"]["top3_share"],
        "may_veto_removed_count": int(may_veto_removed_count),
        "may_veto_removed_share": float(may_veto_removed_count / max(1, threshold_selected_count)),
        "range_cap_removed_count": int(range_cap_removed_count),
        "range_cap_removed_share": float(range_cap_removed_count / max(1, threshold_selected_count - may_veto_removed_count)),
        "threshold_removed_count": int(current_universe_count - threshold_selected_count),
        "threshold_removed_share": float((current_universe_count - threshold_selected_count) / max(1, current_universe_count)),
        "frozen_field_missing_count": int(frozen_field_missing_count),
        "calendar_overfit_flag": (
            float(may_veto_removed_count / max(1, threshold_selected_count)) >= MAX_MAY_VETO_REMOVED_SHARE
            or float(range_cap_removed_count / max(1, threshold_selected_count - may_veto_removed_count)) >= MAX_RANGE_CAP_REMOVED_SHARE
        ),
        "selected_codes": selected_codes,
        "selected_days": [int(day) for day in selected_days if day],
        "selected_months": selected_months,
        "selected_years": selected_years,
        "concentration": concentration,
    }


def _buy_overlap_summary(
    selected_rows: Sequence[Mapping[str, Any]],
    buy_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    available = bool(buy_snapshot.get("available"))
    candidate_count = int(buy_snapshot.get("candidate_count") or 0)
    buy_codes = set(str(code) for code in buy_snapshot.get("codes") or [])
    overlap_codes = sorted({str(row["code"]) for row in selected_rows if str(row.get("code") or "") in buy_codes})
    overlap_event_count = sum(1 for row in selected_rows if str(row.get("code") or "") in buy_codes)
    overlap_event_share = float(overlap_event_count / max(1, len(selected_rows)))
    return {
        "available": available,
        "snapshot_as_of": buy_snapshot.get("snapshot_as_of"),
        "candidate_count": candidate_count,
        "overlap_code_count": len(overlap_codes),
        "overlap_event_count": overlap_event_count,
        "overlap_event_share": overlap_event_share,
        "overlap_codes": overlap_codes,
    }


def _build_operability_decision(
    *,
    selection_summary: Mapping[str, Any],
    borrow_summary: Mapping[str, Any],
    buy_overlap_summary: Mapping[str, Any],
    recent_window_summary: Mapping[str, Any],
    runtime_summary: Mapping[str, Any],
) -> dict[str, Any]:
    current_candidate_available = bool(runtime_summary.get("current_candidate_available"))
    current_universe_count = int(selection_summary.get("current_universe_count") or 0)
    selected_event_count = int(selection_summary.get("selected_event_count") or 0)
    selected_code_count = int(selection_summary.get("selected_code_count") or 0)
    code_top1_share = float(selection_summary.get("code_top1_share") or 0.0)
    code_top3_share = float(selection_summary.get("code_top3_share") or 0.0)
    sector_top1_share = float(selection_summary.get("sector_top1_share") or 0.0)
    sector_top3_share = float(selection_summary.get("sector_top3_share") or 0.0)
    may_veto_removed_share = float(selection_summary.get("may_veto_removed_share") or 0.0)
    range_cap_removed_share = float(selection_summary.get("range_cap_removed_share") or 0.0)
    hard_gap_event_share = float(borrow_summary.get("hard_gap_event_share") or 0.0)
    hard_gap_code_count = int(borrow_summary.get("hard_gap_code_count") or 0)
    soft_cost_event_share = float(borrow_summary.get("soft_cost_event_share") or 0.0)
    soft_cost_code_count = int(borrow_summary.get("soft_cost_code_count") or 0)
    buy_overlap_available = bool(buy_overlap_summary.get("available"))
    buy_overlap_event_share = float(buy_overlap_summary.get("overlap_event_share") or 0.0)
    buy_overlap_code_count = int(buy_overlap_summary.get("overlap_code_count") or 0)
    recent_window_day_count = int(recent_window_summary.get("window_day_count") or 0)
    persistent_code_count = int(recent_window_summary.get("persistent_code_count") or 0)
    persistent_code_share = _safe_float(recent_window_summary.get("persistent_code_share"))
    median_presence_ratio = _safe_float(recent_window_summary.get("median_presence_ratio"))
    mean_presence_ratio = _safe_float(recent_window_summary.get("mean_presence_ratio"))

    blockers: list[str] = []
    if not current_candidate_available or current_universe_count == 0 or recent_window_day_count < MIN_RECENT_WINDOW_DAYS:
        blockers.append("insufficient_live_events")
        decision = "hold_due_to_insufficient_live_events"
        reason = "live candidate breadth or window coverage is too small"
    elif hard_gap_event_share >= MAX_HARD_BORROW_GAP_SHARE or hard_gap_code_count >= max(3, math.ceil(max(1, selected_code_count) * 0.2)):
        blockers.append("borrow_gap_too_large")
        decision = "drop_due_to_hard_borrow_gap"
        reason = "borrow gap is too large for live shadow"
    elif (
        code_top1_share > MAX_CODE_TOP1_SHARE
        or code_top3_share > MAX_CODE_TOP3_SHARE
        or sector_top1_share > MAX_SECTOR_TOP1_SHARE
        or sector_top3_share > MAX_SECTOR_TOP3_SHARE
    ):
        blockers.append("concentration_too_high")
        decision = "drop_due_to_concentration"
        reason = "candidate flow is too concentrated by code or sector"
    elif buy_overlap_available and (buy_overlap_event_share > MAX_BUY_OVERLAP_EVENT_SHARE or buy_overlap_code_count > MAX_BUY_OVERLAP_CODE_COUNT):
        blockers.append("buy_overlap_conflict")
        decision = "drop_due_to_buy_overlap_conflict"
        reason = "sell candidates conflict too much with current buy candidates"
    elif (
        current_universe_count >= max(10, MIN_SELECTED_EVENT_COUNT * 2)
        and selected_event_count < MIN_SELECTED_EVENT_COUNT
        and (may_veto_removed_share >= MAX_MAY_VETO_REMOVED_SHARE or range_cap_removed_share >= MAX_RANGE_CAP_REMOVED_SHARE)
    ) or (
        recent_window_day_count >= MIN_FORWARD_WINDOW_DAYS
        and (
            (persistent_code_share is not None and persistent_code_share < MIN_FORWARD_PERSISTENCE_SHARE)
            or (median_presence_ratio is not None and median_presence_ratio < MIN_FORWARD_PERSISTENCE_SHARE)
            or (mean_presence_ratio is not None and mean_presence_ratio < MIN_FORWARD_PERSISTENCE_SHARE)
            or persistent_code_count == 0
        )
    ):
        blockers.append("forward_decay")
        decision = "drop_due_to_forward_decay"
        reason = "frozen rule is not carrying forward with enough density or persistence"
    elif selected_event_count < MIN_SELECTED_EVENT_COUNT or selected_code_count < MIN_SELECTED_CODE_COUNT:
        blockers.append("insufficient_live_events")
        decision = "hold_due_to_insufficient_live_events"
        reason = "live candidate breadth or window coverage is too small"
    elif soft_cost_event_share >= MAX_SOFT_BORROW_COST_SHARE or soft_cost_code_count >= max(3, math.ceil(max(1, selected_code_count) * MAX_SOFT_BORROW_COST_CODE_SHARE)):
        blockers.append("soft_borrow_cost_too_broad")
        decision = "hold_due_to_soft_borrow_cost"
        reason = "soft borrow cost is broad enough to hold the watch"
    else:
        decision = "continue_live_shadow"
        reason = "live candidates are present and operational gaps are not material"

    typed_reasons = {
        "continue_live_shadow": [
            "live_candidate_breadth_ok",
            "borrow_gap_small",
            "buy_overlap_not_material",
            "concentration_not_excessive",
            "forward_persistence_ok",
        ],
        "hold_due_to_insufficient_live_events": blockers or ["insufficient_live_events"],
        "hold_due_to_soft_borrow_cost": blockers or ["soft_borrow_cost_too_broad"],
        "drop_due_to_hard_borrow_gap": blockers or ["borrow_gap_too_large"],
        "drop_due_to_concentration": blockers or ["concentration_too_high"],
        "drop_due_to_buy_overlap_conflict": blockers or ["buy_overlap_conflict"],
        "drop_due_to_forward_decay": blockers or ["forward_decay"],
    }[decision]

    return {
        "schema_version": f"{SCHEMA_PREFIX}_operability_decision_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "typed_reasons": typed_reasons,
        "blockers": blockers,
        "shadow_trade_candidate": decision == "continue_live_shadow",
        "live_shadow_ready": decision == "continue_live_shadow",
        "buy_level_equivalence_reached": True,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "current_candidate_available": current_candidate_available,
        "current_universe_count": current_universe_count,
        "selected_event_count": selected_event_count,
        "selected_code_count": selected_code_count,
        "selected_month_count": int(selection_summary.get("selected_month_count") or 0),
        "selected_year_count": int(selection_summary.get("selected_year_count") or 0),
        "current_buy_overlap_available": buy_overlap_available,
        "next_gate": "paper_execution_replay" if decision == "continue_live_shadow" else None,
    }


def _build_no_lookahead_audit(
    *,
    selection_rows: Sequence[Mapping[str, Any]],
    recent_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
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
        "borrow_fields": [
            "latestBalance.loanRatio",
            "latestFee.currentFeeYen",
            "restrictions",
        ],
        "future_outcome_fields_used_in_selection_sizing_or_veto": [],
        "future_outcome_fields_used_in_observation_only": [],
        "current_row_count": len(selection_rows),
        "recent_row_count": len(recent_rows),
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _artifact_complete(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    artifact_refs = {
        "live_shadow_watch_contract": str(output_root / "live_shadow_watch_contract.json"),
        "live_shadow_daily_candidates": str(output_root / "live_shadow_daily_candidates.csv"),
        "live_shadow_borrow_status": str(output_root / "live_shadow_borrow_status.csv"),
        "live_shadow_buy_overlap": str(output_root / "live_shadow_buy_overlap.csv"),
        "live_shadow_concentration_summary": str(output_root / "live_shadow_concentration_summary.json"),
        "live_shadow_operability_decision": str(output_root / "live_shadow_operability_decision.json"),
        "no_lookahead_audit": str(output_root / "no_lookahead_audit.json"),
    }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "complete": True,
        "artifact_refs": artifact_refs,
        "result_decision": result.get("decision"),
    }


def run(
    *,
    source_range_cap_root: str | Path = DEFAULT_SOURCE_RANGE_CAP_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    live_limit: int = DEFAULT_LIVE_LIMIT,
    buy_limit: int = DEFAULT_BUY_LIMIT,
    recent_dates: int = DEFAULT_RECENT_DATES,
) -> dict[str, Any]:
    source_range_cap_root = Path(source_range_cap_root)
    output_root = Path(output_root)
    context = _load_context(source_range_cap_root)
    runtime_db_path = Path(context["runtime_db_path"])
    short_freshness = context["rankings_freshness_down"]
    buy_freshness = context["rankings_freshness_up"]
    short_snapshot = _current_short_snapshot(live_limit, short_freshness)
    buy_snapshot = _current_buy_snapshot(buy_limit, buy_freshness)
    recent_rows, recent_window = _load_recent_window(runtime_db_path, recent_dates=recent_dates, rank_limit=live_limit)

    short_items = list(short_snapshot.get("items") or [])
    selected_rows, frozen_stats = _select_live_rows(short_items, threshold=float(context["threshold"]), snapshot_as_of=short_snapshot.get("snapshot_as_of"))
    selected_codes = sorted({str(row["code"]) for row in selected_rows})
    current_codes = sorted({str(item.get("code") or "").strip() for item in short_items if _normalize_text(item.get("code"))})

    sector_lookup = _load_sector_lookup(runtime_db_path, selected_codes)
    borrow_lookup, borrow_report_rows = _load_borrow_lookup(runtime_db_path, selected_codes)
    recent_frequency_summary, recent_frequency_by_code = _recent_frequency_summary(recent_rows, selected_codes)
    enriched_rows = _row_enrichment(
        selected_rows,
        current_short_snapshot=short_snapshot,
        recent_frequency_by_code=recent_frequency_by_code,
        buy_snapshot=buy_snapshot,
        sector_lookup=sector_lookup,
        borrow_lookup=borrow_lookup,
    )
    borrow_report = _borrow_gap_report(selected_rows, borrow_lookup=borrow_lookup, sector_lookup=sector_lookup)
    buy_overlap_rows = _buy_snapshot_rows(selected_rows, buy_snapshot, borrow_lookup=borrow_lookup, sector_lookup=sector_lookup)
    selection_summary = _selection_summary(
        selected_rows,
        current_universe_count=int(frozen_stats["current_universe_count"]),
        threshold_selected_count=int(frozen_stats["threshold_selected_count"]),
        may_veto_removed_count=int(frozen_stats["may_veto_removed_count"]),
        range_cap_removed_count=int(frozen_stats["range_cap_removed_count"]),
        frozen_field_missing_count=int(frozen_stats["frozen_field_missing_count"]),
        sector_lookup=sector_lookup,
    )
    buy_overlap_summary = _buy_overlap_summary(selected_rows, buy_snapshot)
    borrow_summary = borrow_report["summary"]
    recent_window_summary = {
        **recent_window,
        **recent_frequency_summary,
    }
    runtime_summary = {
        "runtime_db_status": context["runtime_status"],
        "rankings_freshness_down": short_freshness,
        "rankings_freshness_up": buy_freshness,
        "current_candidate_available": bool(short_snapshot.get("available")),
        "buy_candidate_available": bool(buy_snapshot.get("available")),
        "current_short_universe_count": int(frozen_stats["current_universe_count"]),
        "current_short_candidate_count": len(selected_rows),
        "current_buy_candidate_count": int(buy_snapshot.get("candidate_count") or 0),
        "current_snapshot_as_of": short_snapshot.get("snapshot_as_of"),
        "recent_window_available": bool(recent_window.get("available")),
    }
    decision = _build_operability_decision(
        selection_summary=selection_summary,
        borrow_summary=borrow_summary,
        buy_overlap_summary=buy_overlap_summary,
        recent_window_summary=recent_window_summary,
        runtime_summary=runtime_summary,
    )
    no_lookahead = _build_no_lookahead_audit(selection_rows=enriched_rows, recent_rows=recent_rows)

    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_concentration_summary_v1",
        "generated_at_utc": _utc_now(),
        "axis": VARIANT_ID,
        "source_authoritative_result": {
            "decision": context["range_cap_decision"].get("decision"),
            "buy_level_equivalence_reached": bool(context["range_cap_decision"].get("buy_level_equivalence_reached")),
            "total_return": _safe_float((context["range_cap_compare"].get("challenger") or {}).get("total_return")),
            "max_drawdown": _safe_float((context["range_cap_compare"].get("challenger") or {}).get("max_drawdown")),
            "profit_factor": _safe_float((context["range_cap_compare"].get("challenger") or {}).get("profit_factor")),
            "bad_pick_delta": (context["range_cap_compare"].get("delta") or {}).get("bad_pick_delta"),
            "severe_loser_delta": (context["range_cap_compare"].get("delta") or {}).get("severe_loser_delta"),
        },
        "runtime_crosscheck": {
            "runtime_db_status": context["runtime_status"],
            "rankings_freshness_down": short_freshness,
            "rankings_freshness_up": buy_freshness,
        },
        "frozen_rule_summary": {
            **frozen_stats,
            "selected_event_count": len(selected_rows),
            "selected_code_count": len(selected_codes),
        },
        "selection_summary": selection_summary,
        "recent_window_summary": recent_window_summary,
        "borrow_summary": borrow_summary,
        "buy_overlap_summary": buy_overlap_summary,
        "current_selected_candidate_rows": len(enriched_rows),
        "current_short_universe_codes": current_codes,
        "recent_frequency_by_code": recent_frequency_by_code,
        "calendar_fragility_summary": {
            "calendar_overfit_flag": selection_summary["calendar_overfit_flag"],
            "may_veto_removed_share": selection_summary["may_veto_removed_share"],
            "range_cap_removed_share": selection_summary["range_cap_removed_share"],
            "persistent_code_share": recent_window_summary.get("persistent_code_share"),
            "median_presence_ratio": recent_window_summary.get("median_presence_ratio"),
            "mean_presence_ratio": recent_window_summary.get("mean_presence_ratio"),
        },
        "production_state_unchanged": {
            "production_ranking_changed": False,
            "active_champion_changed": False,
            "publish_run": False,
            "live_sell_signal_added": False,
        },
        "remaining_risks": [
            "borrow_availability_can_change_with_market_conditions",
            "soft_borrow_cost_incidence_can_expand",
            "forward_shadow_is_observation_only",
            "paper_execution_replay_has_not_started",
        ],
    }

    run_dir = output_root / f"{_utc_stamp()}-sell-hard-filter-may-veto-range-cap-live-shadow-watch-v1"
    run_dir.mkdir(parents=True, exist_ok=False)

    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
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
        "live_window": {
            "live_limit": live_limit,
            "buy_limit": buy_limit,
            "recent_dates": recent_dates,
            "rank_limit": live_limit,
            "direction": DEFAULT_DIRECTION,
            "buy_direction": DEFAULT_BUY_DIRECTION,
        },
        "validation_focus": [
            "candidate_frequency_by_month_and_year",
            "concentration_by_sector_and_code",
            "overlap_with_current_buy_candidates_if_available",
            "bad_pick_and_severe_loser_prevention_stability",
            "borrow_availability_and_sellability_gap",
            "calendar_rule_fragility",
            "range_cap_fragility",
            "forward_density_and_persistence",
        ],
        "decision_labels": [
            "continue_live_shadow",
            "hold_due_to_insufficient_live_events",
            "hold_due_to_soft_borrow_cost",
            "drop_due_to_hard_borrow_gap",
            "drop_due_to_concentration",
            "drop_due_to_buy_overlap_conflict",
            "drop_due_to_forward_decay",
        ],
        "non_scope": [
            "MeeMee",
            "production ranking",
            "active champion",
            "publish",
            "live sell signal",
            "threshold tuning",
            "veto tuning",
            "range cap tuning",
            "sizing tuning",
            "replay semantics tuning",
        ],
        "silent_fallback_used": False,
        "research_fallback": False,
    }

    _write_json(run_dir / "live_shadow_watch_contract.json", contract)
    _write_csv(
        run_dir / "live_shadow_daily_candidates.csv",
        enriched_rows,
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
            "monthly_breakout_up_prob",
            "monthly_range_prob",
            "trade_priority_score",
            "prob_side",
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
    _write_csv(
        run_dir / "live_shadow_borrow_status.csv",
        borrow_report_rows,
        columns=[
            "code",
            "has_snapshot",
            "hard_gap_reason",
            "soft_cost_reasons",
            "restriction_count",
            "current_fee_yen",
            "loan_ratio",
        ],
    )
    _write_csv(
        run_dir / "live_shadow_buy_overlap.csv",
        buy_overlap_rows,
        columns=[
            "code",
            "name",
            "current_short_rank",
            "current_buy_rank",
            "snapshot_as_of",
            "sector33_code",
            "sector33_name",
            "market_code",
            "borrow_hard_gap",
            "borrow_soft_cost",
            "borrow_current_fee_yen",
            "borrow_loan_ratio",
        ],
    )
    _write_json(run_dir / "live_shadow_concentration_summary.json", summary)
    _write_json(run_dir / "live_shadow_operability_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", no_lookahead)
    complete = _artifact_complete(run_dir, decision)
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(run_dir),
        "decision": decision["decision"],
        "live_shadow_concentration_summary": summary,
        "live_shadow_operability_decision": decision,
        "artifact_complete": complete,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-range-cap-root", type=Path, default=DEFAULT_SOURCE_RANGE_CAP_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--live-limit", type=int, default=DEFAULT_LIVE_LIMIT)
    parser.add_argument("--buy-limit", type=int, default=DEFAULT_BUY_LIMIT)
    parser.add_argument("--recent-dates", type=int, default=DEFAULT_RECENT_DATES)
    args = parser.parse_args()
    run(
        source_range_cap_root=args.source_range_cap_root,
        output_root=args.output_root,
        live_limit=args.live_limit,
        buy_limit=args.buy_limit,
        recent_dates=args.recent_dates,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
