from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\noncandle_combined_rerank_gate_v1")
SCHEMA_VERSION = "noncandle_combined_rerank_gate_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _load_rows(source_db: Path, *, start_dt: int | None, end_dt: int | None, direction: str, max_rank: int) -> list[dict[str, Any]]:
    where = ["r.dir = ?", "r.rank <= ?", "s.forward_return_20 IS NOT NULL"]
    params: list[Any] = [direction, max_rank]
    if start_dt is not None:
        where.append("r.dt >= ?")
        params.append(start_dt)
    if end_dt is not None:
        where.append("r.dt <= ?")
        params.append(end_dt)
    side = "buy" if direction == "up" else "sell"
    query = f"""
        SELECT
            r.dt,
            r.dir,
            r.rank,
            r.code,
            r.name,
            r.display_score,
            r.payload_json,
            s.forward_return_20
        FROM ranking_appearance_daily r
        JOIN signal_decision_daily s
          ON s.dt = r.dt
         AND s.code = r.code
         AND s.side = ?
        WHERE {" AND ".join(where)}
        ORDER BY r.dt, r.dir, r.rank, r.code
    """
    with duckdb.connect(str(source_db), read_only=True) as conn:
        records = conn.execute(query, [side, *params]).fetchall()

    rows: list[dict[str, Any]] = []
    for dt, row_dir, rank, code, name, display_score, payload_json, forward_return_20 in records:
        payload = json.loads(payload_json) if payload_json else {}
        ranking_item = payload.get("ranking_item") if isinstance(payload, dict) else {}
        if not isinstance(ranking_item, dict):
            ranking_item = {}
        rows.append(
            {
                "dt": int(dt),
                "dir": str(row_dir),
                "rank": int(rank),
                "code": str(code),
                "name": name,
                "display_score": _safe_float(display_score),
                "forward_return_20": _safe_float(forward_return_20),
                "cnt60Up": _safe_float(ranking_item.get("cnt60Up")),
                "monthlyRangeProb": _safe_float(ranking_item.get("monthlyRangeProb")),
            }
        )
    return rows


def _apply_cnt60(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _rerank(
        rows,
        lambda row: _section_cnt60(row),
        rank_field="cnt60_rank",
    )


def _apply_monthly_range(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _rerank(
        rows,
        lambda row: _section_monthly_range(row),
        rank_field="combined_rank",
        source_rank_field="cnt60_rank",
    )


def _section_cnt60(row: dict[str, Any]) -> tuple[int, str]:
    rank = int(row["rank"])
    cnt60_up = row.get("cnt60Up")
    if rank < 1:
        return 0, "outside_rank_window_before_no_change"
    if rank > 20:
        return 3, "outside_rank_window_after_no_change"
    if cnt60_up is None:
        return 2, "missing_cnt60up_no_silent_fallback"
    if float(cnt60_up) <= 20.0:
        return 1, "cnt60up_rank_window_pass"
    return 2, "cnt60up_rank_window_demoted"


def _section_monthly_range(row: dict[str, Any]) -> tuple[int, str]:
    rank = int(row["cnt60_rank"])
    monthly_range_prob = row.get("monthlyRangeProb")
    if rank < 1:
        return 0, "outside_rank_window_before_no_change"
    if rank > 10:
        return 3, "outside_rank_window_after_no_change"
    if monthly_range_prob is None:
        return 2, "missing_monthly_range_prob_no_silent_fallback"
    if float(monthly_range_prob) <= 0.05:
        return 1, "monthly_range_prob_rank_window_pass"
    return 2, "monthly_range_prob_rank_window_demoted"


def _rerank(
    rows: list[dict[str, Any]],
    section_fn: Any,
    *,
    rank_field: str,
    source_rank_field: str = "rank",
) -> list[dict[str, Any]]:
    out = [dict(row) for row in rows]
    groups: dict[tuple[int, str], list[int]] = defaultdict(list)
    for idx, row in enumerate(out):
        section, reason = section_fn(row)
        row[f"{rank_field}_reason"] = reason
        row["_section"] = section
        groups[(int(row["dt"]), str(row["dir"]))].append(idx)
    for indexes in groups.values():
        ordered = sorted(
            indexes,
            key=lambda idx: (
                int(out[idx]["_section"]),
                int(out[idx][source_rank_field]),
                str(out[idx]["code"]),
            ),
        )
        for new_rank, idx in enumerate(ordered, start=1):
            out[idx][rank_field] = new_rank
    for row in out:
        row.pop("_section", None)
    return out


def _topk_metrics(rows: list[dict[str, Any]], *, rank_field: str, top_k: int) -> dict[str, Any]:
    by_group: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_group[(int(row["dt"]), str(row["dir"]))].append(row)

    selected: list[dict[str, Any]] = []
    for group_rows in by_group.values():
        selected.extend(sorted(group_rows, key=lambda row: (int(row[rank_field]), str(row["code"])))[:top_k])

    returns = [float(row["forward_return_20"]) for row in selected if row.get("forward_return_20") is not None]
    severe_threshold = -0.10
    bad_threshold = -0.05
    return {
        "selection_count": len(selected),
        "mean_ret20": sum(returns) / len(returns) if returns else None,
        "bad_rate20": sum(1 for value in returns if value <= bad_threshold) / len(returns) if returns else None,
        "severe_rate20": sum(1 for value in returns if value <= severe_threshold) / len(returns) if returns else None,
    }


def _changed_members(rows: list[dict[str, Any]], *, left_rank: str, right_rank: str, top_k: int) -> int:
    changed = 0
    groups: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(int(row["dt"]), str(row["dir"]))].append(row)
    for group_rows in groups.values():
        left = {row["code"] for row in group_rows if int(row[left_rank]) <= top_k}
        right = {row["code"] for row in group_rows if int(row[right_rank]) <= top_k}
        changed += len(left.symmetric_difference(right))
    return changed


def _delta(challenger: dict[str, Any], champion: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in ("mean_ret20", "bad_rate20", "severe_rate20"):
        left = challenger.get(key)
        right = champion.get(key)
        out[f"delta_{key}"] = (left - right) if left is not None and right is not None else None
    return out


def run_gate(source_db: Path, output_dir: Path, *, start_dt: int | None, end_dt: int | None, direction: str, max_rank: int) -> dict[str, Any]:
    rows = _load_rows(source_db, start_dt=start_dt, end_dt=end_dt, direction=direction, max_rank=max_rank)
    cnt60_rows = _apply_cnt60(rows)
    combined_rows = _apply_monthly_range(cnt60_rows)

    rank_changed = sum(1 for row in combined_rows if int(row["combined_rank"]) != int(row["cnt60_rank"]))
    top5_changed = _changed_members(combined_rows, left_rank="cnt60_rank", right_rank="combined_rank", top_k=5)
    top10_changed = _changed_members(combined_rows, left_rank="cnt60_rank", right_rank="combined_rank", top_k=10)
    metrics = {}
    for top_k in (5, 10):
        champion = _topk_metrics(combined_rows, rank_field="cnt60_rank", top_k=top_k)
        challenger = _topk_metrics(combined_rows, rank_field="combined_rank", top_k=top_k)
        metrics[f"top{top_k}"] = {
            "champion_cnt60up": champion,
            "challenger_cnt60up_plus_monthly_range": challenger,
            "delta": _delta(challenger, champion),
        }

    top5_delta = metrics["top5"]["delta"]
    top10_delta = metrics["top10"]["delta"]
    helped = (
        rank_changed > 0
        and (top5_delta["delta_mean_ret20"] or 0.0) >= 0.0
        and (top10_delta["delta_mean_ret20"] or 0.0) >= 0.0
        and (top5_delta["delta_bad_rate20"] or 0.0) <= 0.0
        and (top10_delta["delta_bad_rate20"] or 0.0) <= 0.0
        and (top5_delta["delta_severe_rate20"] or 0.0) <= 0.0
        and (top10_delta["delta_severe_rate20"] or 0.0) <= 0.0
    )
    decision = "keep_combined_default_candidate" if helped else "drop_combined_default_candidate"
    reason = "same_condition_non_worse_risk_return_with_branching" if helped else "same_condition_combined_axis_did_not_clear_non_worse_gate"

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source": {
            "source_db": source_db,
            "tables": ["ranking_appearance_daily", "signal_decision_daily"],
            "join": "dt/code plus dir up=buy down=sell",
        },
        "fixed_evaluation_conditions": {
            "direction": direction,
            "max_rank_loaded": max_rank,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "champion": "cnt60Up_lte_20_window_1_20",
            "challenger": "cnt60Up_lte_20_window_1_20_then_monthlyRangeProb_lte_0.05_window_1_10",
            "cost_slippage": "unchanged_from_forward_return_20_source",
            "runtime_write": False,
            "meemee_reflection": False,
        },
        "observed_branching": {
            "row_count": len(combined_rows),
            "changed_rank_count": rank_changed,
            "changed_top5_members_count": top5_changed,
            "changed_top10_members_count": top10_changed,
            "selection_divergence_reason": "monthlyRangeProb reranked the cnt60Up-adjusted top10 window",
        },
        "metrics": metrics,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "may_enable_monthly_range_default_with_cnt60up": helped,
        "sample_changed_rows": [
            {
                "dt": row["dt"],
                "dir": row["dir"],
                "code": row["code"],
                "name": row["name"],
                "rank": row["rank"],
                "cnt60_rank": row["cnt60_rank"],
                "combined_rank": row["combined_rank"],
                "cnt60Up": row["cnt60Up"],
                "monthlyRangeProb": row["monthlyRangeProb"],
                "forward_return_20": row["forward_return_20"],
                "combined_rank_reason": row["combined_rank_reason"],
            }
            for row in combined_rows
            if int(row["combined_rank"]) != int(row["cnt60_rank"])
        ][:50],
    }
    out_path = output_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-noncandle-combined-rerank-gate-v1") / "combined_rerank_gate_summary.json"
    payload["artifact_path"] = str(out_path)
    _write_json(out_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-dt", type=int, default=None)
    parser.add_argument("--end-dt", type=int, default=None)
    parser.add_argument("--direction", choices=["up", "down"], default="up")
    parser.add_argument("--max-rank", type=int, default=50)
    args = parser.parse_args()
    payload = run_gate(args.source_db, args.output_dir, start_dt=args.start_dt, end_dt=args.end_dt, direction=args.direction, max_rank=args.max_rank)
    print(json.dumps({"artifact_path": payload["artifact_path"], "authoritative_rollup_decision": payload["authoritative_rollup_decision"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
