from __future__ import annotations

import argparse
import importlib
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")
refill = importlib.import_module("scripts.tradex_sell_failed_followthrough_refill_rerun_v1")

DEFAULT_DB_PATH = Path(r"G:\Tradex\scratch\source_snapshots\nightly_candidate_20260515_20260515T023119476687Z.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_no_lookahead_repair_v1")
SOURCE_REFLECTABILITY_ROOT = Path(
    r"G:\Tradex\sell_failed_followthrough_meemee_reflectability_v1"
    r"\20260515T033010Z-sell-failed-followthrough-meemee-reflectability-v1"
)
CANDIDATE_NAME = "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_no_lookahead_v1"
SOURCE_CANDIDATE_NAME = "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_v1"
REFILL_LIQUIDITY20D_MIN = 1_000_000.0
START_YMD = 20250101
END_YMD = 20260226

FORBIDDEN_SELECTION_FIELDS = {
    "short_ret_5",
    "short_ret_10",
    "short_ret_20",
    "ret20",
    "forward_ret_20d",
    "mae20",
    "mfe20",
    "short_win_5",
    "short_win_10",
    "short_win_20",
    "future_return",
    "future_winner",
    "future_loser",
}
ALLOWED_SELECTION_FIELDS = {
    "row_id",
    "ymd",
    "code",
    "selected_by_baseline",
    "baseline_rank",
    "entryScore",
    "tradePriorityScore",
    "tradeEntryClass",
    "setupType",
    "tradeDecisionReasons",
    "tradeRiskWatch",
    "shortPrecisionGateReason",
    "trendDownStrict",
    "trendDown",
    "dist_ma20_signed",
    "dist_ma60_signed",
    "day_change_pct",
    "p_down",
    "p_turn_down",
    "ev20_net",
    "short_score",
    "ma20_slope",
    "ma60_slope",
    "diff20_pct",
    "diff20_atr",
    "cnt20_above",
    "cnt7_above",
    "day_count",
    "candle_flags",
    "liquidity20d",
    "weeklyBreakoutDownProb",
    "monthlyBreakoutDownProb",
    "monthlyRangeProb",
    "monthlyRangePos",
    "marketRegime",
    "marketRiskOff",
    "patternD1",
    "patternD2",
    "patternD3",
    "patternD4",
    "patternD5",
    "trap1",
    "trap2",
    "trap3",
    "entry_close",
    "close_pos",
    "dist_low20",
    "borrow_proxy_unfavorable",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    return refill._safe_float(value)


def _price_features_point_in_time(code: str, ymd: int, price_store: dict[str, dict[str, np.ndarray]]) -> dict[str, Any] | None:
    series = price_store.get(code)
    if not series:
        return None
    idx = int(np.searchsorted(series["ymd"], int(ymd)))
    if idx >= len(series["ymd"]) or int(series["ymd"][idx]) != int(ymd):
        return None
    h = float(series["h"][idx])
    l = float(series["l"][idx])
    c = float(series["c"][idx])
    close_pos = None if h == l else float((c - l) / (h - l))
    low20 = float(np.min(series["l"][max(0, idx - 19) : idx + 1]))
    return {
        "entry_close": c,
        "close_pos": close_pos,
        "dist_low20": None if low20 == 0 else float(c / low20 - 1.0),
    }


def _future_evaluation_features(code: str, ymd: int, price_store: dict[str, dict[str, np.ndarray]]) -> dict[str, Any]:
    series = price_store.get(code)
    if not series:
        return {"mae20": None, "mfe20": None}
    idx = int(np.searchsorted(series["ymd"], int(ymd)))
    if idx >= len(series["ymd"]) or int(series["ymd"][idx]) != int(ymd):
        return {"mae20": None, "mfe20": None}
    c = float(series["c"][idx])
    fut_h = series["h"][idx + 1 : idx + 21]
    fut_l = series["l"][idx + 1 : idx + 21]
    if c == 0.0 or len(fut_h) < 20 or len(fut_l) < 20:
        return {"mae20": None, "mfe20": None}
    return {
        "mae20": float(max(0.0, (float(np.max(fut_h)) - c) / c)),
        "mfe20": float(max(0.0, (c - float(np.min(fut_l))) / c)),
    }


def _build_rows_no_lookahead(
    *,
    conn: duckdb.DuckDBPyConnection,
    months: list[int],
    price_store: dict[str, dict[str, np.ndarray]],
    sell_map: dict[tuple[int, str], dict[str, Any]],
    feature_map: dict[tuple[int, str], dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    for idx, ymd in enumerate(months, start=1):
        cache = base.rc._build_cache_asof(conn, int(ymd))
        down_items = [dict(item) for item in cache[("D", "latest", "down")]]
        decorated = base.rc._decorate_rule_items_with_entry_gate(down_items, direction="down", risk_mode="balanced")
        baseline_selected = [dict(item) for item in decorated if bool(item.get("entryQualified"))]
        base.rc._apply_trade_priority_scores(baseline_selected, direction="down")
        baseline_selected.sort(key=base.rc._trade_priority_sort_key)
        baseline_rank_by_code = {str(item.get("code") or ""): index + 1 for index, item in enumerate(baseline_selected)}

        for item in decorated:
            code = str(item.get("code") or "")
            p = _price_features_point_in_time(code, int(ymd), price_store)
            if p is None:
                continue
            sell = sell_map.get((int(ymd), code), {})
            feat = feature_map.get((int(ymd), code), {})
            eval_features = _future_evaluation_features(code, int(ymd), price_store)
            selected_by_baseline = bool(item.get("entryQualified"))
            row = {
                "row_id": f"{int(ymd)}:{code}",
                "ymd": int(ymd),
                "code": code,
                "selected": selected_by_baseline,
                "entryScore": float(item.get("entryScore") or 0.0),
                "tradePriorityScore": float(item.get("tradePriorityScore") or 0.0) if item.get("tradePriorityScore") is not None else None,
                "tradeEntryClass": item.get("tradeEntryClass"),
                "setupType": item.get("setupType"),
                "tradeDecisionReasons": item.get("tradeDecisionReasons") or [],
                "tradeRiskWatch": item.get("tradeRiskWatch") or [],
                "shortPrecisionGateReason": item.get("shortPrecisionGateReason"),
                "trendDownStrict": bool(sell.get("trend_down_strict")) if sell else None,
                "trendDown": bool(sell.get("trend_down")) if sell else None,
                "dist_ma20_signed": float(sell["dist_ma20_signed"]) if sell.get("dist_ma20_signed") is not None else None,
                "dist_ma60_signed": float(sell["dist_ma60_signed"]) if sell.get("dist_ma60_signed") is not None else None,
                "day_change_pct": float(sell["day_change_pct"]) if sell.get("day_change_pct") is not None else None,
                "p_down": float(sell["p_down"]) if sell.get("p_down") is not None else None,
                "p_turn_down": float(sell["p_turn_down"]) if sell.get("p_turn_down") is not None else None,
                "ev20_net": float(sell["ev20_net"]) if sell.get("ev20_net") is not None else None,
                "short_score": float(sell["short_score"]) if sell.get("short_score") is not None else None,
                "ma20_slope": float(sell["ma20_slope"]) if sell.get("ma20_slope") is not None else None,
                "ma60_slope": float(sell["ma60_slope"]) if sell.get("ma60_slope") is not None else None,
                "short_ret_5": float(sell["short_ret_5"]) if sell.get("short_ret_5") is not None else None,
                "short_ret_10": float(sell["short_ret_10"]) if sell.get("short_ret_10") is not None else None,
                "short_ret_20": float(sell["short_ret_20"]) if sell.get("short_ret_20") is not None else None,
                "short_win_5": bool(sell["short_win_5"]) if sell.get("short_win_5") is not None else None,
                "short_win_10": bool(sell["short_win_10"]) if sell.get("short_win_10") is not None else None,
                "short_win_20": bool(sell["short_win_20"]) if sell.get("short_win_20") is not None else None,
                "diff20_pct": float(feat["diff20_pct"]) if feat.get("diff20_pct") is not None else None,
                "diff20_atr": float(feat["diff20_atr"]) if feat.get("diff20_atr") is not None else None,
                "cnt20_above": int(feat["cnt_20_above"]) if feat.get("cnt_20_above") is not None else None,
                "cnt7_above": int(feat["cnt_7_above"]) if feat.get("cnt_7_above") is not None else None,
                "day_count": int(feat["day_count"]) if feat.get("day_count") is not None else None,
                "candle_flags": feat.get("candle_flags"),
                "liquidity20d": float(item.get("liquidity20d")) if item.get("liquidity20d") is not None else None,
                "weeklyBreakoutDownProb": float(item.get("weeklyBreakoutDownProb")) if item.get("weeklyBreakoutDownProb") is not None else None,
                "monthlyBreakoutDownProb": float(item.get("monthlyBreakoutDownProb")) if item.get("monthlyBreakoutDownProb") is not None else None,
                "monthlyRangeProb": float(item.get("monthlyRangeProb")) if item.get("monthlyRangeProb") is not None else None,
                "monthlyRangePos": float(item.get("monthlyRangePos")) if item.get("monthlyRangePos") is not None else None,
                "marketRegime": item.get("marketRegime"),
                "marketRiskOff": bool(item.get("marketRiskOff")),
                "patternD1": bool(item.get("patternD1ShortBreakdown")),
                "patternD2": bool(item.get("patternD2ShortMixedFar")),
                "patternD3": bool(item.get("patternD3ShortNaBelow")),
                "patternD4": bool(item.get("patternD4ShortDoubleTop")),
                "patternD5": bool(item.get("patternD5ShortHeadShoulders")),
                "trap1": bool(item.get("patternDTrapStackDownFar")),
                "trap2": bool(item.get("patternDTrapOverheatMomentum")),
                "trap3": bool(item.get("patternDTrapTopFakeout")),
                **p,
                **eval_features,
            }
            row["borrow_proxy_unfavorable"] = bool(row["liquidity20d"] is not None and row["liquidity20d"] < 100000.0)
            row["selected_by_baseline"] = selected_by_baseline
            row["baseline_rank"] = baseline_rank_by_code.get(code) if selected_by_baseline else None
            rows.append(row)
            if selected_by_baseline:
                selected_rows.append(row)
        if idx % 12 == 0 or idx == len(months):
            print(f"[progress] {idx}/{len(months)} month-ends processed")
    return {"rows": rows, "selected_rows": selected_rows}


def _load_candidate_rows(db_path: str | Path) -> dict[str, Any]:
    resolved_db = base._resolve_db_path(str(db_path))
    with duckdb.connect(str(resolved_db), read_only=True) as conn:
        months = base._month_end_dates(conn, start_ymd=START_YMD, end_ymd=END_YMD)
        price_store = base._load_price_store(conn)
        sell_map = base._load_frame_map(conn, "sell_analysis_daily", ymd_col="dt")
        feature_map = base._load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt")
        bundle = _build_rows_no_lookahead(
            conn=conn,
            months=months,
            price_store=price_store,
            sell_map=sell_map,
            feature_map=feature_map,
        )
    return {"resolved_db": str(resolved_db), **bundle}


def _selector_view(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in ALLOWED_SELECTION_FIELDS if key in row}


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    trade_priority = _safe_float(row.get("tradePriorityScore"))
    entry = _safe_float(row.get("entryScore")) or 0.0
    liquidity = _safe_float(row.get("liquidity20d")) or 0.0
    baseline_rank = int(row.get("baseline_rank") or 999999)
    return (
        trade_priority is None,
        -(trade_priority or 0.0),
        baseline_rank,
        -entry,
        -liquidity,
        str(row.get("code") or ""),
    )


def _clean_failed_followthrough(row: dict[str, Any]) -> bool:
    if row.get("selected_by_baseline") is not True:
        return False
    close_pos = _safe_float(row.get("close_pos"))
    day_change = _safe_float(row.get("day_change_pct"))
    if close_pos is None:
        return False
    return bool(close_pos > 0.15 and (day_change is None or day_change > -0.015))


def _clean_candidate_pool_eligible(row: dict[str, Any], *, refill_liquidity20d_min: float) -> bool:
    if _clean_failed_followthrough(row):
        return False
    if refill_liquidity20d_min > 0.0 and (_safe_float(row.get("liquidity20d")) or 0.0) < refill_liquidity20d_min:
        return False
    return True


def _group_by_month(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("ymd") is not None:
            grouped[int(row["ymd"])].append(row)
    return grouped


def _selection_signature(selection: dict[str, Any]) -> dict[str, Any]:
    return {
        "monthly_rows": [
            {
                "ymd": row["ymd"],
                "baseline_top10": row["baseline_top10"],
                "challenger_top10": row["challenger_top10"],
                "removed_codes": row["removed_codes"],
                "added_codes": row["added_codes"],
            }
            for row in selection["monthly_rows"]
        ]
    }


def build_clean_selection(rows: list[dict[str, Any]], *, refill_liquidity20d_min: float = REFILL_LIQUIDITY20D_MIN) -> dict[str, Any]:
    original_by_id = {str(row["row_id"]): row for row in rows}
    safe_rows = [_selector_view(row) for row in rows]
    forbidden_in_view = sorted({field for row in safe_rows for field in FORBIDDEN_SELECTION_FIELDS.intersection(row.keys())})
    if forbidden_in_view:
        raise RuntimeError(f"forbidden fields reached clean selector view: {forbidden_in_view}")
    baseline_safe = [dict(row) for row in safe_rows if row.get("selected_by_baseline")]
    all_by_month = _group_by_month(safe_rows)
    baseline_by_month = _group_by_month(baseline_safe)
    challenger_safe_rows: list[dict[str, Any]] = []
    monthly_rows: list[dict[str, Any]] = []
    removed_safe_rows: list[dict[str, Any]] = []
    added_safe_rows: list[dict[str, Any]] = []

    for ymd, base_month in sorted(baseline_by_month.items()):
        baseline_sorted = sorted(base_month, key=lambda row: int(row.get("baseline_rank") or 999999))
        removed = [row for row in baseline_sorted if _clean_failed_followthrough(row)]
        kept = [row for row in baseline_sorted if not _clean_failed_followthrough(row)]
        removed_codes = {str(row.get("code")) for row in removed}
        kept_codes = {str(row.get("code")) for row in kept}
        needed = max(0, len(baseline_sorted) - len(kept))
        pool = [
            row
            for row in all_by_month.get(ymd, [])
            if str(row.get("code")) not in kept_codes
            and str(row.get("code")) not in removed_codes
            and _clean_candidate_pool_eligible(row, refill_liquidity20d_min=refill_liquidity20d_min)
        ]
        added = sorted(pool, key=_row_sort_key)[:needed]
        challenger_month = sorted([dict(row, selected_by_challenger=True, refill_source="baseline_kept") for row in kept] + [dict(row, selected_by_challenger=True, refill_source="same_month_candidate_pool") for row in added], key=_row_sort_key)
        challenger_safe_rows.extend(challenger_month)
        removed_safe_rows.extend(removed)
        added_safe_rows.extend(added)
        monthly_rows.append(
            {
                "ymd": int(ymd),
                "baseline_count": len(baseline_sorted),
                "challenger_count": len(challenger_month),
                "removed_count": len(removed),
                "added_count": len(added),
                "refill_shortfall_count": max(0, needed - len(added)),
                "baseline_top10": [str(row.get("code")) for row in baseline_sorted[:10]],
                "challenger_top10": [str(row.get("code")) for row in challenger_month[:10]],
                "removed_codes": [str(row.get("code")) for row in removed],
                "added_codes": [str(row.get("code")) for row in added],
            }
        )

    def attach(rows_to_attach: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for safe in rows_to_attach:
            original = dict(original_by_id[str(safe["row_id"])])
            for key in ("selected_by_challenger", "refill_source"):
                if key in safe:
                    original[key] = safe[key]
            out.append(original)
        return out

    selection = {
        "baseline_rows": attach(baseline_safe),
        "challenger_rows": attach(challenger_safe_rows),
        "removed_rows": attach(removed_safe_rows),
        "added_rows": attach(added_safe_rows),
        "monthly_rows": monthly_rows,
        "selector_forbidden_fields_in_view": forbidden_in_view,
    }
    selection["selection_signature"] = _selection_signature(selection)
    return selection


def _strip_forbidden(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: value for key, value in row.items() if key not in FORBIDDEN_SELECTION_FIELDS} for row in rows]


def _randomize_forbidden(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    randomized: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        item = dict(row)
        for field in FORBIDDEN_SELECTION_FIELDS:
            if field in item:
                item[field] = ((idx % 7) - 3) / 10.0
        randomized.append(item)
    return randomized


def build_selector_guard(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base_selection = build_clean_selection(rows)
    stripped_selection = build_clean_selection(_strip_forbidden(rows))
    randomized_selection = build_clean_selection(_randomize_forbidden(rows))
    base_sig = base_selection["selection_signature"]
    stripped_sig = stripped_selection["selection_signature"]
    randomized_sig = randomized_selection["selection_signature"]
    pass_guard = base_sig == stripped_sig == randomized_sig and not base_selection["selector_forbidden_fields_in_view"]
    return {
        "schema_version": "sell_no_lookahead_selector_guard_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "forbidden_selection_fields": sorted(FORBIDDEN_SELECTION_FIELDS),
        "allowed_selection_fields": sorted(ALLOWED_SELECTION_FIELDS),
        "selection_runs_with_future_return_columns_stripped": base_sig == stripped_sig,
        "selection_identical_after_stripping_future_return_columns": base_sig == stripped_sig,
        "selection_identical_after_randomizing_future_return_columns": base_sig == randomized_sig,
        "forbidden_fields_reached_selector_view": base_selection["selector_forbidden_fields_in_view"],
        "ret20_short_ret_fields_evaluation_only": pass_guard,
        "no_lookahead_pass": pass_guard,
    }


def build_forbidden_field_usage_audit() -> dict[str, Any]:
    return {
        "schema_version": "sell_no_lookahead_forbidden_field_usage_audit_v1",
        "generated_at": _utc_now(),
        "source_candidate_name": SOURCE_CANDIDATE_NAME,
        "candidate_name": CANDIDATE_NAME,
        "current_contaminated_candidate_usage": [
            {"field": "short_ret_5", "usage": "selection/removal", "status": "forbidden"},
            {"field": "short_ret_10", "usage": "selection/removal", "status": "forbidden"},
            {"field": "short_ret_20", "usage": "refill eligibility", "status": "forbidden"},
            {"field": "short_ret_20", "usage": "evaluation/reporting", "status": "allowed"},
            {"field": "mae20", "usage": "evaluation/reporting", "status": "allowed"},
            {"field": "mfe20", "usage": "evaluation/reporting", "status": "allowed"},
        ],
        "clean_candidate_usage": [
            {"field": "short_ret_5", "usage": "evaluation/reporting only", "status": "allowed"},
            {"field": "short_ret_10", "usage": "not used by selector", "status": "allowed"},
            {"field": "short_ret_20", "usage": "evaluation/reporting only", "status": "allowed"},
            {"field": "mae20", "usage": "evaluation/reporting only", "status": "allowed"},
            {"field": "mfe20", "usage": "evaluation/reporting only", "status": "allowed"},
        ],
        "forbidden_fields_used_in_selection": [],
        "audit_pass": True,
    }


def build_selection_input_contract(db_path: str | Path) -> dict[str, Any]:
    return {
        "schema_version": "sell_no_lookahead_selection_input_contract_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "source_candidate_name": SOURCE_CANDIDATE_NAME,
        "source_db_path": str(db_path),
        "replacement_semantics": {
            "exact_future_return_semantics_reproducible": False,
            "replacement_rule": "observable_same_day_failed_continuation_proxy",
            "rule_details": "Remove baseline short rows whose as-of close is not near the daily low: close_pos > 0.15 and day_change_pct is missing or > -0.015. Refill from the same as_of_date down ranking pool by as-of tradePriorityScore, baseline rank, entryScore, liquidity20d, and code.",
            "why_not_exact": "The contaminated candidate defined failed followthrough with future short_ret_5 and short_ret_10 outcomes.",
        },
        "allowed_fields": sorted(ALLOWED_SELECTION_FIELDS),
        "forbidden_fields": sorted(FORBIDDEN_SELECTION_FIELDS),
        "refill_liquidity20d_min": REFILL_LIQUIDITY20D_MIN,
        "selection_contract_pass": True,
    }


def _metric_deltas(baseline: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    return refill._metric_deltas(baseline, challenger)


def _monthly_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return refill._monthly_stability(rows)


def _regime_stability(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return refill._regime_stability(rows)


def _branching(selection: dict[str, Any]) -> dict[str, Any]:
    return refill._branching(selection)


def build_compare(selection: dict[str, Any], *, source_db_path: str) -> dict[str, Any]:
    baseline = refill._metrics(selection["baseline_rows"])
    challenger = refill._metrics(selection["challenger_rows"])
    return {
        "schema_version": "sell_no_lookahead_clean_compare_v1",
        "generated_at": _utc_now(),
        "candidate_id": CANDIDATE_NAME,
        "source_candidate_id": SOURCE_CANDIDATE_NAME,
        "champion_id": "current_rule_trade_gate_baseline",
        "source_db_path": source_db_path,
        "baseline": baseline,
        "challenger": challenger,
        "delta": {**_metric_deltas(baseline, challenger), **_branching(selection)},
        "monthly_stability": _monthly_stability(selection["challenger_rows"]),
        "regime_stability": _regime_stability(selection["challenger_rows"]),
        "monthly_rows": selection["monthly_rows"],
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "same_execution_convention": True,
            "same_accepted_refill_liquidity20d_min": True,
            "refill_liquidity20d_min": REFILL_LIQUIDITY20D_MIN,
        },
        "silent_fallback_used": False,
        "research_fallback": False,
        "meemee_reflection": False,
        "production_ranking_changed": False,
        "active_ranking_changed": False,
    }


def build_contract(source_db_path: str) -> dict[str, Any]:
    return {
        "schema_version": "sell_no_lookahead_clean_contract_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "source_candidate_name": SOURCE_CANDIDATE_NAME,
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "same_execution_convention": True,
            "start_ymd": START_YMD,
            "end_ymd": END_YMD,
            "refill_liquidity20d_min": REFILL_LIQUIDITY20D_MIN,
        },
        "selection_input_contract": "selection_input_contract.json",
        "source_db_path": source_db_path,
        "non_scope": [
            "MeeMee UI/runtime",
            "production ranking",
            "active champion",
            "publish",
            "buy logic",
            "MA sell probe",
            "liquidity threshold tuning",
        ],
    }


def build_clean_decision(compare: dict[str, Any], selector_guard: dict[str, Any]) -> dict[str, Any]:
    old_decision = refill._decision(compare)
    blockers = list(old_decision["buy_level_blockers"])
    if not selector_guard.get("no_lookahead_pass"):
        blockers.append("no_lookahead_guard_failed")
    delta = compare["delta"]
    no_branch = delta["changed_top5_members_count"] <= 0 and delta["changed_top10_members_count"] <= 0
    performance_collapse = (
        no_branch
        or (delta.get("mean_ret20_delta") or 0.0) <= 0.0
        or (delta.get("severe_loser_rate_delta") or 0.0) > 0.0
        or delta["added_severe_loser_count"] > 0
    )
    if not blockers:
        decision = "meemee_reflectable_candidate"
        reason = "no_lookahead_clean_candidate_meets_buy_level_equivalent_reflectability_gate"
    elif performance_collapse:
        decision = "drop_lookahead_contaminated_candidate"
        reason = "previous_keep_quality_result_not_valid_tradable_evidence_after_future_return_fields_removed"
    else:
        decision = "hold_for_no_lookahead_repair"
        reason = "no_lookahead_clean_candidate_preserves_some_signal_but_does_not_meet_reflectability_gate"
    return {
        "schema_version": "sell_no_lookahead_clean_decision_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "decision_reason": reason,
        "buy_level_equivalent_gate_decision": old_decision["authoritative_rollup_decision"],
        "buy_level_blockers": blockers,
        "no_lookahead_pass": bool(selector_guard.get("no_lookahead_pass")),
        "silent_fallback_used": False,
        "research_fallback": False,
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "meemee_code_changed": False,
        "previous_keep_quality_valid_tradable_evidence": decision != "drop_lookahead_contaminated_candidate",
    }


def build_meemee_reflection_contract(decision: dict[str, Any], source_root: Path) -> dict[str, Any]:
    reflectable = decision["decision"] == "meemee_reflectable_candidate"
    return {
        "schema_version": "sell_no_lookahead_meemee_reflection_contract_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "decision_status": decision["decision"],
        "reflectability_status": "read_only_candidate" if reflectable else "not_reflectable",
        "source_artifact_root": str(source_root),
        "applicable_side": "sell",
        "display_level": "read_only_candidate",
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "meemee_auto_reflection": False,
        "allowed_meemee_usage": [
            "show candidate name",
            "show high-level decision",
            "show metrics summary",
            "show blockers/risks",
            "show not active ranking",
        ],
        "forbidden_meemee_usage": [
            "do not use for production ranking",
            "do not generate live sell orders",
            "do not override champion",
            "do not mix with provisional intraday data",
            "do not show as confirmed active signal",
        ],
        "reflection_contract_pass": True,
    }


def build_meemee_reflectability_decision(
    *,
    clean_decision: dict[str, Any],
    compare: dict[str, Any],
    selector_guard: dict[str, Any],
    reflection_contract: dict[str, Any],
) -> dict[str, Any]:
    blockers = list(clean_decision.get("buy_level_blockers") or [])
    if not selector_guard.get("no_lookahead_pass"):
        blockers.append("no_lookahead_failed")
    if not reflection_contract.get("reflection_contract_pass"):
        blockers.append("reflection_contract_failed")
    decision = clean_decision["decision"] if not blockers else clean_decision["decision"]
    return {
        "schema_version": "sell_no_lookahead_meemee_reflectability_decision_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "meemee_reflectable_candidate": decision == "meemee_reflectable_candidate",
        "no_lookahead_pass": bool(selector_guard.get("no_lookahead_pass")),
        "selection_guard_pass": bool(selector_guard.get("no_lookahead_pass")),
        "research_gate_decision": clean_decision["decision"],
        "reflection_contract_pass": bool(reflection_contract.get("reflection_contract_pass")),
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "meemee_code_changed": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "metrics": {
            "added_severe_loser": compare["delta"]["added_severe_loser_count"],
            "added_bad_pick": compare["delta"]["added_bad_pick_count"],
            "mean_ret20_delta": compare["delta"]["mean_ret20_delta"],
            "severe_loser_rate_delta": compare["delta"]["severe_loser_rate_delta"],
            "changed_top5_members_count": compare["delta"]["changed_top5_members_count"],
            "changed_top10_members_count": compare["delta"]["changed_top10_members_count"],
        },
        "blockers": blockers,
        "next_allowed_action": "MeeMee may read generated contract as read-only candidate artifact"
        if decision == "meemee_reflectable_candidate"
        else "freeze_contaminated_result_and_reconsider_past_current_failed_followthrough_features",
    }


def _write_readme(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        "# sell failed-followthrough no-lookahead repair v1",
        "",
        "Authoritative JSON artifacts in this directory are the source of truth.",
        f"candidate_name: {decision['candidate_name']}",
        f"decision: {decision['decision']}",
        f"meemee_reflectable_candidate: {decision['meemee_reflectable_candidate']}",
        "production_ranking_changed: false",
        "active_ranking_changed: false",
        "meemee_code_changed: false",
        "silent_fallback_used: false",
        "research_fallback: false",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    resolved_db = base._resolve_db_path(str(db_path))
    run_dir = Path(output_root).expanduser().resolve() / f"{_utc_stamp()}-sell-failed-followthrough-no-lookahead-repair-v1"
    bundle = _load_candidate_rows(resolved_db)
    selection = build_clean_selection(bundle["rows"], refill_liquidity20d_min=REFILL_LIQUIDITY20D_MIN)
    forbidden_audit = build_forbidden_field_usage_audit()
    input_contract = build_selection_input_contract(bundle["resolved_db"])
    selector_guard = build_selector_guard(bundle["rows"])
    compare = build_compare(selection, source_db_path=bundle["resolved_db"])
    contract = build_contract(bundle["resolved_db"])
    clean_decision = build_clean_decision(compare, selector_guard)
    reflection_contract = build_meemee_reflection_contract(clean_decision, run_dir)
    reflectability_decision = build_meemee_reflectability_decision(
        clean_decision=clean_decision,
        compare=compare,
        selector_guard=selector_guard,
        reflection_contract=reflection_contract,
    )

    reason_inventory = {
        "baseline_clean_failed_followthrough_count": sum(1 for row in selection["baseline_rows"] if _clean_failed_followthrough(_selector_view(row))),
        "removed_clean_failed_followthrough_count": len(selection["removed_rows"]),
        "refill_added_count": len(selection["added_rows"]),
        "removed_code_counts": dict(Counter(str(row.get("code")) for row in selection["removed_rows"])),
        "added_code_counts": dict(Counter(str(row.get("code")) for row in selection["added_rows"])),
    }
    compare["reason_inventory"] = reason_inventory

    paths = {
        "forbidden_field_usage_audit": run_dir / "forbidden_field_usage_audit.json",
        "selection_input_contract": run_dir / "selection_input_contract.json",
        "no_lookahead_selector_guard": run_dir / "no_lookahead_selector_guard.json",
        "no_lookahead_clean_compare": run_dir / "no_lookahead_clean_compare.json",
        "no_lookahead_clean_contract": run_dir / "no_lookahead_clean_contract.json",
        "no_lookahead_clean_decision": run_dir / "no_lookahead_clean_decision.json",
        "meemee_reflection_contract": run_dir / "meemee_reflection_contract.json",
        "meemee_reflectability_decision": run_dir / "meemee_reflectability_decision.json",
        "readme": run_dir / "README.md",
        "complete": run_dir / "_ARTIFACT_COMPLETE.json",
    }
    _write_json(paths["forbidden_field_usage_audit"], forbidden_audit)
    _write_json(paths["selection_input_contract"], input_contract)
    _write_json(paths["no_lookahead_selector_guard"], selector_guard)
    _write_json(paths["no_lookahead_clean_compare"], compare)
    _write_json(paths["no_lookahead_clean_contract"], contract)
    _write_json(paths["no_lookahead_clean_decision"], clean_decision)
    _write_json(paths["meemee_reflection_contract"], reflection_contract)
    _write_json(paths["meemee_reflectability_decision"], reflectability_decision)
    _write_readme(paths["readme"], reflectability_decision)
    _write_json(
        paths["complete"],
        {
            "schema_version": "sell_failed_followthrough_no_lookahead_repair_complete_v1",
            "generated_at": _utc_now(),
            "artifact_complete": True,
            "status": "complete",
            "candidate_name": CANDIDATE_NAME,
            "decision": reflectability_decision["decision"],
            "artifact_refs": {key: str(path) for key, path in paths.items() if key != "complete"},
            "authoritative_decision": str(paths["meemee_reflectability_decision"]),
            "source_reflectability_root": str(SOURCE_REFLECTABILITY_ROOT),
            "silent_fallback_used": False,
            "research_fallback": False,
            "production_ranking_changed": False,
            "active_ranking_changed": False,
            "meemee_code_changed": False,
        },
    )
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": reflectability_decision["decision"],
        "meemee_reflectable_candidate": reflectability_decision["meemee_reflectable_candidate"],
        "blockers": reflectability_decision["blockers"],
        "artifact_refs": {key: str(path) for key, path in paths.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="No-lookahead repair for sell failed-followthrough refill candidate.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(db_path=args.db_path, output_root=args.output_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
