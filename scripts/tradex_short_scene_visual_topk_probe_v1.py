from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_visual_ai_entry_benchmark_v1 import _bars_asof, _load_bars_by_code, _parse_json, _safe_float, _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_topk_probe_v1")
TOP_K_VALUES = (5, 10)
BAD_LOSER_THRESHOLD = -0.05
SEVERE_LOSER_THRESHOLD = -0.10
KEEP_KEYS = {
    "downtrend_a_phase|sell_rebound_rejection_or_lower_low|short|pullback_probe_candidate",
    "distribution_or_failed_retest|sell_failed_high_retest_after_7ma_break|short|keep_probe_candidate",
}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_signal_rows(con: duckdb.DuckDBPyConnection, *, start_dt: int, end_dt: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT dt, code, name, side, entry_qualified, setup_type,
               score_snapshot_json, rank_snapshot_json,
               forward_return_5, forward_return_20, forward_return_30,
               max_favorable_30, max_adverse_30
        FROM signal_decision_daily
        WHERE dt BETWEEN ? AND ?
          AND side = 'sell'
          AND entry_qualified = true
          AND forward_return_20 IS NOT NULL
        ORDER BY dt, code
        """,
        [start_dt, end_dt],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        score_payload = _parse_json(row[6])
        rank_payload = _parse_json(row[7])
        trade_priority = _safe_float(score_payload.get("tradePriorityScore"))
        if trade_priority is None:
            trade_priority = _safe_float(rank_payload.get("tradePriorityScore"))
        if trade_priority is None:
            continue
        final_rank = _safe_float(rank_payload.get("finalRank"))
        out.append(
            {
                "dt": int(row[0]),
                "code": str(row[1]),
                "name": row[2],
                "side": str(row[3]),
                "entry_qualified": bool(row[4]),
                "setup_type": row[5],
                "tradePriorityScore": trade_priority,
                "finalRank": final_rank,
                "forward_return_5": _safe_float(row[8]),
                "forward_return_20": _safe_float(row[9]),
                "forward_return_30": _safe_float(row[10]),
                "max_favorable_30": _safe_float(row[11]),
                "max_adverse_30": _safe_float(row[12]),
            }
        )
    return out


def _row_key(row: dict[str, Any]) -> str:
    return f"{row.get('market_scene')}|{row.get('action_bias')}|{row.get('trade_side')}|{row.get('visual_decision')}"


def _selection_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret20 = [float(row["forward_return_20"]) for row in rows if row.get("forward_return_20") is not None]
    return {
        "count": len(rows),
        "unique_codes": len({str(row["code"]) for row in rows}),
        "forward_return_20_mean": _round(mean(ret20) if ret20 else None),
        "forward_return_20_median": _round(median(ret20) if ret20 else None),
        "hit_rate_20": _round(sum(1 for value in ret20 if value > 0) / len(ret20) if ret20 else None),
        "bad_loser_rate_20": _round(sum(1 for value in ret20 if value <= BAD_LOSER_THRESHOLD) / len(ret20) if ret20 else None),
        "severe_loser_rate_20": _round(sum(1 for value in ret20 if value <= SEVERE_LOSER_THRESHOLD) / len(ret20) if ret20 else None),
    }


def _compare_topk(groups: dict[int, list[dict[str, Any]]], *, topk: int) -> dict[str, Any]:
    champion_rows: list[dict[str, Any]] = []
    rerank_rows: list[dict[str, Any]] = []
    filter_rows: list[dict[str, Any]] = []
    changed_rerank: list[int] = []
    changed_filter: list[int] = []
    dates = 0
    for dt, rows in sorted(groups.items()):
        if len(rows) < topk:
            continue
        dates += 1
        champion = sorted(rows, key=lambda row: (-float(row["tradePriorityScore"]), float(row.get("finalRank") or 1e9), row["code"]))[:topk]
        rerank = sorted(rows, key=lambda row: (-float(row["scene_visual_score"]), float(row.get("finalRank") or 1e9), row["code"]))[:topk]
        eligible = [row for row in rows if row.get("scene_visual_keep_signal") is True]
        filtered = sorted(eligible, key=lambda row: (-float(row["tradePriorityScore"]), float(row.get("finalRank") or 1e9), row["code"]))[:topk]
        champion_set = {row["code"] for row in champion}
        rerank_set = {row["code"] for row in rerank}
        filter_set = {row["code"] for row in filtered}
        changed_rerank.append(len(champion_set.symmetric_difference(rerank_set)))
        changed_filter.append(len(champion_set.symmetric_difference(filter_set)))
        champion_rows.extend(champion)
        rerank_rows.extend(rerank)
        filter_rows.extend(filtered)
    champion = _selection_metrics(champion_rows)
    rerank = _selection_metrics(rerank_rows)
    filtered = _selection_metrics(filter_rows)

    def delta(target: dict[str, Any]) -> dict[str, Any]:
        return {
            "forward_return_20_mean": _round((target["forward_return_20_mean"] or 0.0) - (champion["forward_return_20_mean"] or 0.0)),
            "forward_return_20_median": _round((target["forward_return_20_median"] or 0.0) - (champion["forward_return_20_median"] or 0.0)),
            "hit_rate_20": _round((target["hit_rate_20"] or 0.0) - (champion["hit_rate_20"] or 0.0)),
            "bad_loser_rate_20": _round((target["bad_loser_rate_20"] or 0.0) - (champion["bad_loser_rate_20"] or 0.0)),
            "severe_loser_rate_20": _round((target["severe_loser_rate_20"] or 0.0) - (champion["severe_loser_rate_20"] or 0.0)),
        }

    return {
        "topk": topk,
        "evaluated_dates": dates,
        "champion": champion,
        "rerank": rerank,
        "filter": filtered,
        "rerank_delta": delta(rerank),
        "filter_delta": delta(filtered),
        "changed_rerank_member_count_total": int(sum(changed_rerank)),
        "changed_filter_member_count_total": int(sum(changed_filter)),
        "changed_rerank_member_count_mean": _round(mean([float(v) for v in changed_rerank]) if changed_rerank else None),
        "changed_filter_member_count_mean": _round(mean([float(v) for v in changed_filter]) if changed_filter else None),
    }


def _decision(compare: dict[str, Any]) -> dict[str, Any]:
    top5 = compare["top5"]
    top10 = compare["top10"]
    if (top5.get("changed_rerank_member_count_total") or 0) == 0 and (top5.get("changed_filter_member_count_total") or 0) == 0:
        return {"judgment": "hold", "reason_type": "no_material_branching"}
    rerank_delta = top5["rerank_delta"]
    filter_delta = top5["filter_delta"]
    filter_count = ((top5.get("filter") or {}).get("count") or 0)
    top10_rerank_delta = top10["rerank_delta"]
    if (
        (rerank_delta.get("forward_return_20_mean") or 0.0) > 0
        and (rerank_delta.get("bad_loser_rate_20") or 0.0) <= 0
        and (top10_rerank_delta.get("forward_return_20_mean") or 0.0) >= -0.002
    ):
        return {"judgment": "keep", "reason_type": "rerank_improves_top5_without_bad_loser_damage"}
    if filter_count > 0 and (filter_delta.get("forward_return_20_mean") or 0.0) > 0 and (filter_delta.get("bad_loser_rate_20") or 0.0) <= 0:
        return {"judgment": "hold", "reason_type": "filter_positive_but_pool_may_shrink"}
    if filter_count == 0:
        return {"judgment": "drop", "reason_type": "signal_absent_from_sell_candidate_pool"}
    return {"judgment": "drop", "reason_type": "no_topk_improvement"}


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_topk_probe_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = _load_signal_rows(con, start_dt=start_dt, end_dt=end_dt)
        bars_by_code = _load_bars_by_code(con, codes={row["code"] for row in rows}, max_dt=end_dt)
    finally:
        con.close()

    enriched: list[dict[str, Any]] = []
    key_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        bars = _bars_asof(bars_by_code.get(row["code"], []), dt=int(row["dt"]), window=160)
        shape = classify_shape_from_bars([[bar["ymd"], bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]] for bar in bars])
        visual = _visual_review(_visual_features_from_ohlc(bars[-60:]))
        enriched_row = {
            **row,
            "market_scene": shape.get("market_scene"),
            "trade_side": shape.get("trade_side"),
            "action_bias": shape.get("action_bias"),
            "shape_intent": shape.get("shape_intent"),
            "entry_timing": shape.get("entry_timing"),
            "visual_decision": visual.get("decision"),
            "visual_entry_method": visual.get("entry_method"),
        }
        key = _row_key(enriched_row)
        keep_signal = key in KEEP_KEYS
        key_counts[key] += 1
        enriched_row["scene_visual_key"] = key
        enriched_row["scene_visual_keep_signal"] = keep_signal
        enriched_row["scene_visual_score"] = float(row["tradePriorityScore"]) + (0.12 if keep_signal else -0.03)
        enriched.append(enriched_row)

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in enriched:
        groups[int(row["dt"])].append(row)
    compare = {f"top{topk}": _compare_topk(groups, topk=topk) for topk in TOP_K_VALUES}
    decision = _decision(compare)
    result = {
        "schema_version": "tradex_short_scene_visual_topk_probe_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily",
            "price_source": "daily_bars",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "side": "sell",
            "entry_qualified_only": True,
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "signal_keys": sorted(KEEP_KEYS),
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "coverage": {
            "input_rows": len(rows),
            "enriched_rows": len(enriched),
            "date_count": len(groups),
            "keep_signal_rows": sum(1 for row in enriched if row["scene_visual_keep_signal"]),
            "scene_visual_key_counts": dict(sorted(key_counts.items())),
        },
        "compare": compare,
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "meemee_reflectable": False,
        "remaining_risks": [
            "OHLC visual proxy is not pixel screenshot analysis",
            "single-period in-sample probe",
            "signal_decision_daily sell pool is small in this period",
        ],
    }
    compare_path = output_dir / "short_scene_visual_topk_probe_compare.json"
    decision_path = output_dir / "short_scene_visual_topk_probe_decision.json"
    ledger_path = output_dir / "short_scene_visual_topk_probe_ledger.jsonl"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "compare_json": str(compare_path),
        "decision_json": str(decision_path),
        "ledger_jsonl": str(ledger_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(compare_path, result)
    _write_json(decision_path, {k: result[k] for k in ("schema_version", "authoritative_rollup_decision", "reason_type", "meemee_reflectable", "remaining_risks", "artifacts")})
    with ledger_path.open("w", encoding="utf-8") as fh:
        for row in enriched:
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-dt", type=int, default=20260301)
    parser.add_argument("--end-dt", type=int, default=20260430)
    args = parser.parse_args()
    result = run_probe(db_path=args.db_path, output_root=args.output_root, start_dt=args.start_dt, end_dt=args.end_dt)
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "top5": result["compare"]["top5"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
