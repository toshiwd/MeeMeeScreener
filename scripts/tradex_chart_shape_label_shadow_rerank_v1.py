from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/chart_shape_label_shadow_rerank_v1")
DEFAULT_BUY_ALL_SUMMARY = Path(
    "G:/Tradex/chart_shape_label_validation_v1/20260519T064427Z-chart_shape_label_validation_v1/chart_shape_label_validation_summary.json"
)
DEFAULT_BUY_ENTRYQ_SUMMARY = Path(
    "G:/Tradex/chart_shape_label_validation_v1/20260519T064916Z-buy-entryq-chart_shape_label_validation_v1/chart_shape_label_validation_summary.json"
)
DEFAULT_BUY_ENTRYQ_LEDGER = Path(
    "G:/Tradex/chart_shape_label_validation_v1/20260519T064916Z-buy-entryq-chart_shape_label_validation_v1/chart_shape_label_validation_ledger.jsonl"
)

SEVERE_LOSER_THRESHOLD = -0.10
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


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _parse_json(text: Any) -> dict[str, Any]:
    if not text:
        return {}
    try:
        payload = json.loads(str(text))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_ledger(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_score_lookup(
    con: duckdb.DuckDBPyConnection,
    *,
    start_dt: int,
    end_dt: int,
    side: str,
) -> dict[tuple[int, str], dict[str, Any]]:
    rows = con.execute(
        """
        SELECT dt, code, score_snapshot_json, rank_snapshot_json
        FROM signal_decision_daily
        WHERE dt BETWEEN ? AND ?
          AND side = ?
          AND entry_qualified = true
          AND forward_return_20 IS NOT NULL
        """,
        [start_dt, end_dt, side],
    ).fetchall()
    lookup: dict[tuple[int, str], dict[str, Any]] = {}
    for dt, code, score_text, rank_text in rows:
        score = _parse_json(score_text)
        rank = _parse_json(rank_text)
        trade_priority = _safe_float(score.get("tradePriorityScore"))
        if trade_priority is None:
            trade_priority = _safe_float(rank.get("tradePriorityScore"))
        final_rank = _safe_float(rank.get("finalRank"))
        lookup[(int(dt), str(code))] = {
            "tradePriorityScore": trade_priority,
            "finalRank": final_rank,
        }
    return lookup


def _build_label_policy(
    *,
    buy_all_summary: dict[str, Any],
    buy_entryq_summary: dict[str, Any],
    min_entryq_count: int,
    min_all_count: int,
    boost_scale: float,
    max_abs_boost: float,
) -> dict[str, dict[str, Any]]:
    all_shapes = buy_all_summary.get("shape_summary") or {}
    entryq_shapes = buy_entryq_summary.get("shape_summary") or {}
    all_tendency = buy_all_summary.get("shape_tendency") or {}
    entryq_tendency = buy_entryq_summary.get("shape_tendency") or {}
    policy: dict[str, dict[str, Any]] = {}
    for label in sorted(set(all_shapes) | set(entryq_shapes)):
        if label in {"insufficient_data"}:
            continue
        all_stats = all_shapes.get(label) or {}
        entryq_stats = entryq_shapes.get(label) or {}
        entryq = entryq_tendency.get(label) or {}
        all_ = all_tendency.get(label) or {}
        entryq_count = int(entryq_stats.get("sample_count") or 0)
        all_count = int(all_stats.get("sample_count") or 0)
        if entryq_count >= min_entryq_count:
            raw_delta = _safe_float(entryq.get("forward_return_20_mean_delta_vs_baseline")) or 0.0
            evidence_source = "entryq"
            shrinkage = 1.0
        elif all_count >= min_all_count and entryq_count > 0:
            raw_delta = _safe_float(all_.get("forward_return_20_mean_delta_vs_baseline")) or 0.0
            evidence_source = "all_sample_shrunk_by_entryq_count"
            shrinkage = max(0.15, min(1.0, entryq_count / max(float(min_entryq_count), 1.0)))
        elif all_count >= min_all_count:
            raw_delta = _safe_float(all_.get("forward_return_20_mean_delta_vs_baseline")) or 0.0
            evidence_source = "all_sample_no_entryq_support"
            shrinkage = 0.15
        else:
            continue
        boost = max(-max_abs_boost, min(max_abs_boost, raw_delta * boost_scale * shrinkage))
        policy[label] = {
            "boost": round(boost, 6),
            "raw_mean_delta": _round(raw_delta),
            "evidence_source": evidence_source,
            "shrinkage": _round(shrinkage),
            "entryq_sample_count": entryq_count,
            "all_sample_count": all_count,
            "entryq_tendency": entryq.get("tendency"),
            "all_tendency": all_.get("tendency"),
        }
    return policy


def _selection_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_safe_float(row.get("forward_return_20")) for row in rows]
    returns = [value for value in returns if value is not None]
    codes = {str(row.get("code")) for row in rows}
    return {
        "count": len(rows),
        "unique_codes": len(codes),
        "mean_forward_return_20": _round(_mean(returns)),
        "positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns) if returns else None),
        "severe_loser_rate": _round(sum(1 for value in returns if value <= SEVERE_LOSER_THRESHOLD) / len(returns) if returns else None),
    }


def _compare_topk(groups: dict[int, list[dict[str, Any]]], *, topk: int) -> dict[str, Any]:
    champion_rows: list[dict[str, Any]] = []
    challenger_rows: list[dict[str, Any]] = []
    changed_counts: list[int] = []
    evaluated_dates = 0
    zero_branch_dates = 0
    for dt, rows in sorted(groups.items()):
        if len(rows) < topk:
            continue
        evaluated_dates += 1
        champion = sorted(
            rows,
            key=lambda row: (
                -float(row.get("champion_score") or -1e9),
                float(row.get("final_rank") or 1e9),
                str(row.get("code")),
            ),
        )[:topk]
        challenger = sorted(
            rows,
            key=lambda row: (
                -float(row.get("challenger_score") or -1e9),
                float(row.get("final_rank") or 1e9),
                str(row.get("code")),
            ),
        )[:topk]
        champion_set = {str(row.get("code")) for row in champion}
        challenger_set = {str(row.get("code")) for row in challenger}
        changed = len(champion_set.symmetric_difference(challenger_set))
        if changed == 0:
            zero_branch_dates += 1
        changed_counts.append(changed)
        champion_rows.extend(champion)
        challenger_rows.extend(challenger)
    champion_metrics = _selection_metrics(champion_rows)
    challenger_metrics = _selection_metrics(challenger_rows)
    return {
        "topk": topk,
        "evaluated_dates": evaluated_dates,
        "champion": champion_metrics,
        "challenger": challenger_metrics,
        "delta": {
            "mean_forward_return_20": _round(
                (challenger_metrics["mean_forward_return_20"] or 0.0) - (champion_metrics["mean_forward_return_20"] or 0.0)
            ),
            "positive_rate": _round((challenger_metrics["positive_rate"] or 0.0) - (champion_metrics["positive_rate"] or 0.0)),
            "severe_loser_rate": _round(
                (challenger_metrics["severe_loser_rate"] or 0.0) - (champion_metrics["severe_loser_rate"] or 0.0)
            ),
        },
        "changed_member_count_mean": _round(_mean([float(value) for value in changed_counts])),
        "zero_branch_dates": zero_branch_dates,
    }


def run_shadow_rerank(
    *,
    db_path: Path,
    buy_all_summary_path: Path,
    buy_entryq_summary_path: Path,
    buy_entryq_ledger_path: Path,
    output_root: Path,
    start_dt: int,
    end_dt: int,
    side: str,
    min_entryq_count: int,
    min_all_count: int,
    boost_scale: float,
    max_abs_boost: float,
) -> dict[str, Any]:
    variant_tag = f"scale{boost_scale:g}_cap{max_abs_boost:g}".replace(".", "p")
    output_dir = output_root / f"{_now_tag()}-{side}-{variant_tag}-chart_shape_label_shadow_rerank_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    buy_all_summary = _load_json(buy_all_summary_path)
    buy_entryq_summary = _load_json(buy_entryq_summary_path)
    label_policy = _build_label_policy(
        buy_all_summary=buy_all_summary,
        buy_entryq_summary=buy_entryq_summary,
        min_entryq_count=min_entryq_count,
        min_all_count=min_all_count,
        boost_scale=boost_scale,
        max_abs_boost=max_abs_boost,
    )
    ledger_rows = [
        row
        for row in _load_ledger(buy_entryq_ledger_path)
        if int(row.get("dt") or 0) >= start_dt and int(row.get("dt") or 0) <= end_dt and str(row.get("side")) == side
    ]
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        score_lookup = _load_score_lookup(con, start_dt=start_dt, end_dt=end_dt, side=side)
    finally:
        con.close()

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    missing_score_rows = 0
    excluded_unusable_label_rows = 0
    for row in ledger_rows:
        dt = int(row.get("dt") or 0)
        code = str(row.get("code") or "")
        score_info = score_lookup.get((dt, code), {})
        champion_score = _safe_float(score_info.get("tradePriorityScore"))
        if champion_score is None:
            missing_score_rows += 1
            continue
        label = str(row.get("shape_label") or "unknown")
        label_info = label_policy.get(label)
        if label_info is None:
            excluded_unusable_label_rows += 1
            boost = 0.0
            usable = False
        else:
            boost = float(label_info.get("boost") or 0.0)
            usable = True
        enriched = {
            "dt": dt,
            "code": code,
            "shape_label": label,
            "shape_label_usable": usable,
            "champion_score": champion_score,
            "shape_boost": boost,
            "challenger_score": champion_score + boost,
            "final_rank": score_info.get("finalRank"),
            "forward_return_20": _safe_float(row.get("forward_return_20")),
        }
        groups[dt].append(enriched)

    topk_compare = {f"top{topk}": _compare_topk(groups, topk=topk) for topk in TOP_K_VALUES}
    top5 = topk_compare["top5"]
    top10 = topk_compare["top10"]
    top5_delta = top5["delta"]["mean_forward_return_20"]
    top10_delta = top10["delta"]["mean_forward_return_20"]
    severe_delta = top5["delta"]["severe_loser_rate"]
    if top5_delta is not None and top10_delta is not None and top5_delta > 0 and top10_delta > 0 and (severe_delta or 0.0) <= 0:
        decision = "ranking_improved_hold_for_ma_context"
        reason = "top5_and_top10_forward_return_improved_without_top5_severe_loser_increase"
    elif top5_delta is not None and top5_delta > 0:
        decision = "partial_improvement_hold"
        reason = "top5_improved_but_secondary_gate_not_clean"
    else:
        decision = "drop_no_ranking_improvement"
        reason = "top5_forward_return_not_improved"

    result = {
        "schema_version": "tradex_chart_shape_label_shadow_rerank_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "scope": {
            "tradex_only": True,
            "meemee_ui_changed": False,
            "meemee_ranking_changed": False,
            "candidate_generation_changed": False,
            "runtime_db_written": False,
            "publish_registry_changed": False,
        },
        "silent_fallback_used": False,
        "source_artifacts": {
            "buy_all_summary_json": str(buy_all_summary_path),
            "buy_entryq_summary_json": str(buy_entryq_summary_path),
            "buy_entryq_ledger_jsonl": str(buy_entryq_ledger_path),
        },
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily",
            "side": side,
            "entry_qualified_only": True,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "champion_score": "score_snapshot_json.tradePriorityScore",
            "challenger_score": "champion_score + chart_shape_label_boost",
            "primary_metric": "forward_return_20",
            "severe_loser_threshold": SEVERE_LOSER_THRESHOLD,
            "topk_values": list(TOP_K_VALUES),
            "min_entryq_count": min_entryq_count,
            "min_all_count": min_all_count,
            "boost_scale": boost_scale,
            "max_abs_boost": max_abs_boost,
            "ma_context_included": False,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "label_policy": label_policy,
        "coverage": {
            "ledger_rows": len(ledger_rows),
            "rerank_rows": sum(len(rows) for rows in groups.values()),
            "date_count": len(groups),
            "missing_score_rows": missing_score_rows,
            "excluded_unusable_label_rows": excluded_unusable_label_rows,
            "usable_label_count": len(label_policy),
        },
        "topk_compare": topk_compare,
        "authoritative_rollup_decision": decision,
        "reason_type": reason,
        "meemee_reflectable": False,
        "remaining_risks": [
            "MA context not included",
            "in-sample label policy and ranking comparison share the same period",
            "shadow rerank only; production ranking unchanged",
        ],
    }
    summary_path = output_dir / "chart_shape_label_shadow_rerank_compare.json"
    _write_json(summary_path, result)
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, "summary_json": str(summary_path)})
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "compare_json": str(summary_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--buy-all-summary", type=Path, default=DEFAULT_BUY_ALL_SUMMARY)
    parser.add_argument("--buy-entryq-summary", type=Path, default=DEFAULT_BUY_ENTRYQ_SUMMARY)
    parser.add_argument("--buy-entryq-ledger", type=Path, default=DEFAULT_BUY_ENTRYQ_LEDGER)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-dt", type=int, default=20250101)
    parser.add_argument("--end-dt", type=int, default=20260515)
    parser.add_argument("--side", choices=["buy"], default="buy")
    parser.add_argument("--min-entryq-count", type=int, default=30)
    parser.add_argument("--min-all-count", type=int, default=300)
    parser.add_argument("--boost-scale", type=float, default=0.75)
    parser.add_argument("--max-abs-boost", type=float, default=0.04)
    args = parser.parse_args()
    result = run_shadow_rerank(
        db_path=args.db_path,
        buy_all_summary_path=args.buy_all_summary,
        buy_entryq_summary_path=args.buy_entryq_summary,
        buy_entryq_ledger_path=args.buy_entryq_ledger,
        output_root=args.output_root,
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        side=args.side,
        min_entryq_count=args.min_entryq_count,
        min_all_count=args.min_all_count,
        boost_scale=args.boost_scale,
        max_abs_boost=args.max_abs_boost,
    )
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
