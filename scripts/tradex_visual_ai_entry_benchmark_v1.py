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

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/visual_ai_entry_benchmark_v1")
SEVERE_LOSER_THRESHOLD = -0.10
BAD_LOSER_THRESHOLD = -0.05
TOP_K_VALUES = (5, 10)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _parse_json(text: Any) -> dict[str, Any]:
    if not text:
        return {}
    try:
        payload = json.loads(str(text))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _rate(values: list[bool]) -> float | None:
    return sum(1 for value in values if value) / len(values) if values else None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_signal_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    start_dt: int,
    end_dt: int,
    side: str,
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT dt, code, name, side, entry_qualified, setup_type,
               score_snapshot_json, rank_snapshot_json,
               forward_return_5, forward_return_20, forward_return_30,
               max_favorable_30, max_adverse_30
        FROM signal_decision_daily
        WHERE dt BETWEEN ? AND ?
          AND side = ?
          AND entry_qualified = true
          AND forward_return_20 IS NOT NULL
        ORDER BY dt, code
        """,
        [start_dt, end_dt, side],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        score_payload = _parse_json(row[6])
        rank_payload = _parse_json(row[7])
        trade_priority = _safe_float(score_payload.get("tradePriorityScore"))
        if trade_priority is None:
            trade_priority = _safe_float(rank_payload.get("tradePriorityScore"))
        final_rank = _safe_float(rank_payload.get("finalRank"))
        if trade_priority is None:
            continue
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
                "entryScore": _safe_float(score_payload.get("entryScore") or rank_payload.get("entryScore")),
                "tradePriorityProfitScore": _safe_float(score_payload.get("tradePriorityProfitScore")),
                "tradePriorityHitScore": _safe_float(score_payload.get("tradePriorityHitScore")),
                "forward_return_5": _safe_float(row[8]),
                "forward_return_20": _safe_float(row[9]),
                "forward_return_30": _safe_float(row[10]),
                "max_favorable_30": _safe_float(row[11]),
                "max_adverse_30": _safe_float(row[12]),
            }
        )
    return out


def _load_bars_by_code(con: duckdb.DuckDBPyConnection, *, codes: set[str], max_dt: int) -> dict[str, list[dict[str, Any]]]:
    if not codes:
        return {}
    rows = con.execute(
        """
        WITH normalized AS (
            SELECT
                code,
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8
                        THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                o, h, l, c, v
            FROM daily_bars
            WHERE code IN (SELECT * FROM UNNEST(?))
        )
        SELECT code, ymd, o, h, l, c, v
        FROM normalized
        WHERE ymd <= ?
        ORDER BY code, ymd
        """,
        [list(codes), max_dt],
    ).fetchall()
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for code, ymd, open_, high, low, close, volume in rows:
        by_code[str(code)].append(
            {
                "ymd": int(ymd),
                "open": _safe_float(open_),
                "high": _safe_float(high),
                "low": _safe_float(low),
                "close": _safe_float(close),
                "volume": _safe_float(volume),
            }
        )
    return by_code


def _bars_asof(bars: list[dict[str, Any]], *, dt: int, window: int = 60) -> list[dict[str, Any]]:
    eligible = [row for row in bars if int(row["ymd"]) <= dt and row.get("close") is not None]
    return eligible[-window:]


def _visual_features_from_ohlc(bars: list[dict[str, Any]]) -> dict[str, Any]:
    if len(bars) < 20:
        return {
            "confirmed": False,
            "reason": "insufficient_history",
        }
    closes = [float(row["close"]) for row in bars if row.get("close") is not None]
    highs = [float(row["high"]) for row in bars if row.get("high") is not None]
    lows = [float(row["low"]) for row in bars if row.get("low") is not None]
    if len(closes) < 20 or not highs or not lows:
        return {
            "confirmed": False,
            "reason": "insufficient_valid_ohlc",
        }
    last = bars[-1]
    close = float(last["close"])
    high_60 = max(highs)
    low_60 = min(lows)
    close_position = (close - low_60) / (high_60 - low_60) if high_60 > low_60 else 0.5
    return_5 = close / closes[-6] - 1.0 if len(closes) >= 6 and closes[-6] > 0 else None
    return_20 = close / closes[-21] - 1.0 if len(closes) >= 21 and closes[-21] > 0 else None
    ma7 = mean(closes[-7:]) if len(closes) >= 7 else None
    ma20 = mean(closes[-20:]) if len(closes) >= 20 else None
    ma60 = mean(closes[-60:]) if len(closes) >= 60 else mean(closes)
    latest_upper_wick = 0.0
    if last.get("high") is not None and last.get("open") is not None and last.get("low") is not None:
        span = max(float(last["high"]) - float(last["low"]), 0.0)
        if span > 0:
            latest_upper_wick = (float(last["high"]) - max(float(last["open"]), close)) / span
    recent_high = max(highs[-10:])
    recent_high_drawdown = close / recent_high - 1.0 if recent_high > 0 else 0.0
    high_rejection_risk = latest_upper_wick >= 0.45 and recent_high_drawdown <= -0.015
    chase_risk = close_position >= 0.78 and (return_20 or 0.0) >= 0.12 and close > (ma7 or close)
    tight_high_hold = close_position >= 0.66 and abs(recent_high_drawdown) <= 0.035 and not high_rejection_risk
    pullback_to_ma7 = ma7 is not None and abs(close / ma7 - 1.0) <= 0.025 and close >= (ma20 or close)
    trend_slope = (ma20 / ma60 - 1.0) if ma20 and ma60 and ma60 > 0 else None
    return {
        "confirmed": True,
        "latest_price_position_pct": _round(close_position),
        "return_5": _round(return_5),
        "return_20": _round(return_20),
        "ma7_distance": _round(close / ma7 - 1.0 if ma7 and ma7 > 0 else None),
        "ma20_distance": _round(close / ma20 - 1.0 if ma20 and ma20 > 0 else None),
        "trend_slope_pct": _round(trend_slope),
        "latest_upper_wick_ratio": _round(latest_upper_wick),
        "recent_high_drawdown": _round(recent_high_drawdown),
        "high_rejection_risk": high_rejection_risk,
        "chase_risk": chase_risk,
        "tight_high_hold": tight_high_hold,
        "pullback_to_ma7": pullback_to_ma7,
    }


def _visual_review(features: dict[str, Any]) -> dict[str, Any]:
    if not features.get("confirmed"):
        return {
            "decision": "unsafe_to_classify",
            "score_adjustment": 0.0,
            "entry_method": "no_visual_adjustment",
            "reasons": [str(features.get("reason") or "visual_unconfirmed")],
        }
    reasons: list[str] = []
    if features.get("high_rejection_risk"):
        reasons.append("visual_high_rejection_risk")
        return {
            "decision": "drop_or_wait",
            "score_adjustment": -0.08,
            "entry_method": "wait_for_reclaim_after_upper_rejection",
            "reasons": reasons,
        }
    if features.get("chase_risk"):
        reasons.append("visual_chase_risk")
        return {
            "decision": "probe_only_or_wait",
            "score_adjustment": -0.04,
            "entry_method": "small_probe_only_or_wait_for_7ma_pullback",
            "reasons": reasons,
        }
    if features.get("tight_high_hold"):
        reasons.append("visual_tight_high_hold")
        return {
            "decision": "keep_probe_candidate",
            "score_adjustment": 0.02,
            "entry_method": "small_probe_then_add_if_high_hold_continues",
            "reasons": reasons,
        }
    if features.get("pullback_to_ma7"):
        reasons.append("visual_pullback_to_ma7")
        return {
            "decision": "pullback_probe_candidate",
            "score_adjustment": 0.015,
            "entry_method": "probe_if_reclaims_intraday_or_holds_7ma",
            "reasons": reasons,
        }
    return {
        "decision": "watch",
        "score_adjustment": 0.0,
        "entry_method": "wait_for_clearer_visual_entry",
        "reasons": ["visual_neutral"],
    }


def _selection_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret5 = [float(row["forward_return_5"]) for row in rows if row.get("forward_return_5") is not None]
    ret20 = [float(row["forward_return_20"]) for row in rows if row.get("forward_return_20") is not None]
    return {
        "count": len(rows),
        "unique_codes": len({str(row["code"]) for row in rows}),
        "forward_return_5_mean": _round(_mean(ret5)),
        "forward_return_20_mean": _round(_mean(ret20)),
        "forward_return_20_median": _round(median(ret20) if ret20 else None),
        "hit_rate_20": _round(_rate([value > 0 for value in ret20])),
        "bad_loser_rate_20": _round(_rate([value <= BAD_LOSER_THRESHOLD for value in ret20])),
        "severe_loser_rate_20": _round(_rate([value <= SEVERE_LOSER_THRESHOLD for value in ret20])),
    }


def _compare_topk(groups: dict[int, list[dict[str, Any]]], *, topk: int) -> dict[str, Any]:
    champion_rows: list[dict[str, Any]] = []
    challenger_rows: list[dict[str, Any]] = []
    changed_counts: list[int] = []
    dates = 0
    zero_branch_dates = 0
    dropped_winners = 0
    avoided_bad_losers = 0
    avoided_severe_losers = 0
    for dt, rows in sorted(groups.items()):
        if len(rows) < topk:
            continue
        dates += 1
        champion = sorted(rows, key=lambda row: (-float(row["tradePriorityScore"]), float(row.get("finalRank") or 1e9), row["code"]))[:topk]
        challenger = sorted(rows, key=lambda row: (-float(row["visual_challenger_score"]), float(row.get("finalRank") or 1e9), row["code"]))[:topk]
        champion_set = {row["code"] for row in champion}
        challenger_set = {row["code"] for row in challenger}
        dropped = [row for row in champion if row["code"] not in challenger_set]
        added = [row for row in challenger if row["code"] not in champion_set]
        changed = len(champion_set.symmetric_difference(challenger_set))
        if changed == 0:
            zero_branch_dates += 1
        changed_counts.append(changed)
        dropped_winners += sum(1 for row in dropped if (row.get("forward_return_20") or 0.0) > 0)
        avoided_bad_losers += sum(1 for row in dropped if (row.get("forward_return_20") or 0.0) <= BAD_LOSER_THRESHOLD)
        avoided_severe_losers += sum(1 for row in dropped if (row.get("forward_return_20") or 0.0) <= SEVERE_LOSER_THRESHOLD)
        champion_rows.extend(champion)
        challenger_rows.extend(challenger)
    champion_metrics = _selection_metrics(champion_rows)
    challenger_metrics = _selection_metrics(challenger_rows)
    return {
        "topk": topk,
        "evaluated_dates": dates,
        "changed_member_count_mean": _round(_mean([float(value) for value in changed_counts])),
        "changed_member_count_total": int(sum(changed_counts)),
        "zero_branch_dates": zero_branch_dates,
        "dropped_winners": dropped_winners,
        "avoided_bad_losers": avoided_bad_losers,
        "avoided_severe_losers": avoided_severe_losers,
        "champion": champion_metrics,
        "challenger": challenger_metrics,
        "delta": {
            "forward_return_5_mean": _round(
                (challenger_metrics["forward_return_5_mean"] or 0.0) - (champion_metrics["forward_return_5_mean"] or 0.0)
            ),
            "forward_return_20_mean": _round(
                (challenger_metrics["forward_return_20_mean"] or 0.0) - (champion_metrics["forward_return_20_mean"] or 0.0)
            ),
            "forward_return_20_median": _round(
                (challenger_metrics["forward_return_20_median"] or 0.0) - (champion_metrics["forward_return_20_median"] or 0.0)
            ),
            "hit_rate_20": _round((challenger_metrics["hit_rate_20"] or 0.0) - (champion_metrics["hit_rate_20"] or 0.0)),
            "bad_loser_rate_20": _round(
                (challenger_metrics["bad_loser_rate_20"] or 0.0) - (champion_metrics["bad_loser_rate_20"] or 0.0)
            ),
            "severe_loser_rate_20": _round(
                (challenger_metrics["severe_loser_rate_20"] or 0.0) - (champion_metrics["severe_loser_rate_20"] or 0.0)
            ),
        },
    }


def _decision(compare: dict[str, Any]) -> dict[str, Any]:
    top5 = compare["top5"]
    top10 = compare["top10"]
    top5_delta = top5["delta"]
    top10_delta = top10["delta"]
    branching = (top5.get("changed_member_count_total") or 0) > 0
    if not branching:
        return {"judgment": "hold", "reason_type": "no_material_branching"}
    if (
        (top5_delta.get("bad_loser_rate_20") or 0.0) < 0
        and (top5_delta.get("forward_return_20_mean") or 0.0) >= 0
        and (top10_delta.get("forward_return_20_mean") or 0.0) >= -0.002
    ):
        return {"judgment": "keep", "reason_type": "bad_loser_reduction_without_mean_return_damage"}
    if (
        (top5_delta.get("bad_loser_rate_20") or 0.0) < 0
        and (top5_delta.get("forward_return_20_mean") or 0.0) < 0
    ):
        return {"judgment": "hold", "reason_type": "risk_reduction_but_return_drag"}
    return {"judgment": "drop", "reason_type": "no_same_condition_improvement"}


def run_benchmark(
    *,
    db_path: Path,
    output_root: Path,
    start_dt: int,
    end_dt: int,
    side: str,
) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-{side}-visual_ai_entry_benchmark_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = _load_signal_rows(con, start_dt=start_dt, end_dt=end_dt, side=side)
        bars_by_code = _load_bars_by_code(con, codes={row["code"] for row in rows}, max_dt=end_dt)
    finally:
        con.close()

    enriched: list[dict[str, Any]] = []
    visual_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        bars = _bars_asof(bars_by_code.get(row["code"], []), dt=int(row["dt"]), window=60)
        visual_features = _visual_features_from_ohlc(bars)
        visual_review = _visual_review(visual_features)
        visual_counts[str(visual_review["decision"])] += 1
        enriched.append(
            {
                **row,
                "visual_features": visual_features,
                "visual_review": visual_review,
                "visual_challenger_score": float(row["tradePriorityScore"]) + float(visual_review["score_adjustment"]),
            }
        )

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in enriched:
        groups[int(row["dt"])].append(row)
    compare = {f"top{topk}": _compare_topk(groups, topk=topk) for topk in TOP_K_VALUES}
    decision = _decision(compare)
    result = {
        "schema_version": "tradex_visual_ai_entry_benchmark_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily",
            "price_source": "daily_bars",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "side": side,
            "entry_qualified_only": True,
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "same_artifact_detail_level": "full_json_with_ledger",
            "primary_metric": "forward_return_20",
            "bad_loser_threshold": BAD_LOSER_THRESHOLD,
            "severe_loser_threshold": SEVERE_LOSER_THRESHOLD,
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "coverage": {
            "input_rows": len(rows),
            "enriched_rows": len(enriched),
            "date_count": len(groups),
            "visual_decision_counts": dict(sorted(visual_counts.items())),
        },
        "compare": compare,
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "meemee_reflectable": False,
        "remaining_risks": [
            "OHLC visual proxy is not pixel screenshot analysis",
            "in-sample benchmark only",
            "visual thresholds are first-pass and not tuned by month",
        ],
    }
    compare_path = output_dir / "visual_ai_entry_benchmark_compare.json"
    decision_path = output_dir / "visual_ai_entry_benchmark_decision.json"
    ledger_path = output_dir / "visual_ai_entry_benchmark_ledger.jsonl"
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
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare_json": str(compare_path), "decision_json": str(decision_path), "ledger_jsonl": str(ledger_path)})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-dt", type=int, default=20260401)
    parser.add_argument("--end-dt", type=int, default=20260501)
    parser.add_argument("--side", choices=["buy"], default="buy")
    args = parser.parse_args()
    result = run_benchmark(
        db_path=args.db_path,
        output_root=args.output_root,
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        side=args.side,
    )
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "top5": result["compare"]["top5"]["delta"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
