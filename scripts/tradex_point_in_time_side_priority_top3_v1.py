from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.tradex_point_in_time_side_permission_router_v1 import (
    DEFAULT_DB,
    _cvar10,
    _pf,
    build_corrected_baseline,
    metrics,
    permission_table,
)


AXIS_ID = "tradex_point_in_time_side_priority_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_side_priority_top3_v1")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _interleave(day: pd.DataFrame, first_side: str) -> pd.DataFrame:
    other = "sell" if first_side == "buy" else "buy"
    buckets = {
        side: day[day.side == side].sort_values(["rank", "code"]).to_dict("records")
        for side in ("buy", "sell")
    }
    ordered: list[dict] = []
    for idx in range(3):
        for side in (first_side, other):
            if idx < len(buckets[side]):
                ordered.append(buckets[side][idx])
    result = pd.DataFrame(ordered[:3])
    if len(result):
        result["global_rank"] = range(1, len(result) + 1)
    return result


def select_top3(events: pd.DataFrame, permissions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    baseline_parts, challenger_parts, states = [], [], []
    for signal_date, day in events.groupby("signal_ymd", sort=True):
        base = _interleave(day, "buy")
        baseline_parts.append(base)
        state = permissions[permissions.signal_ymd == signal_date].set_index("side")
        scores = {}
        for side in ("buy", "sell"):
            if side in state.index:
                row = state.loc[side]
                pf = float(row.permission_pf) if pd.notna(row.permission_pf) else float("-inf")
                exp = float(row.permission_expectancy) if pd.notna(row.permission_expectancy) else float("-inf")
                cvar = float(row.permission_cvar10) if pd.notna(row.permission_cvar10) else float("-inf")
                qualified = int(pf >= 1.30 and exp > 0 and cvar >= -0.08)
                scores[side] = (qualified, pf, exp, 1 if side == "buy" else 0)
            else:
                scores[side] = (0, float("-inf"), float("-inf"), 1 if side == "buy" else 0)
        first = max(("buy", "sell"), key=lambda side: scores[side])
        challenger = _interleave(day, first)
        challenger_parts.append(challenger)
        states.append({"signal_ymd": int(signal_date), "priority_first_side": first, "buy_health_tuple": repr(scores["buy"]), "sell_health_tuple": repr(scores["sell"])})
    baseline = pd.concat(baseline_parts, ignore_index=True) if baseline_parts else pd.DataFrame()
    challenger = pd.concat(challenger_parts, ignore_index=True) if challenger_parts else pd.DataFrame()
    return baseline, challenger, pd.DataFrame(states)


def branching(baseline: pd.DataFrame, challenger: pd.DataFrame) -> dict:
    rows = []
    for ymd in sorted(set(baseline.signal_ymd) | set(challenger.signal_ymd)):
        left = baseline[baseline.signal_ymd == ymd].sort_values("global_rank")
        right = challenger[challenger.signal_ymd == ymd].sort_values("global_rank")
        lkeys = (left.side + ":" + left.code.astype(str)).tolist()
        rkeys = (right.side + ":" + right.code.astype(str)).tolist()
        rows.append({"signal_ymd": int(ymd), "split": left.split.iloc[0] if len(left) else right.split.iloc[0], "changed_members_count": len(set(lkeys) ^ set(rkeys)), "changed_rank_count": sum(a != b for a, b in zip(lkeys, rkeys)) + abs(len(lkeys) - len(rkeys)), "jaccard": len(set(lkeys) & set(rkeys)) / len(set(lkeys) | set(rkeys)) if set(lkeys) | set(rkeys) else 1.0})
    detail = pd.DataFrame(rows)
    summary = {}
    for split in ("train", "validation", "shadow"):
        part = detail[detail.split == split]
        changed = part.changed_rank_count > 0
        summary[split] = {"date_count": int(len(part)), "changed_top3_members_count": int(part.changed_members_count.sum()), "changed_rank_count": int(part.changed_rank_count.sum()), "changed_days": int(changed.sum()), "changed_day_rate": float(changed.mean()) if len(part) else None, "mean_jaccard": float(part.jaccard.mean()) if len(part) else None}
    return {"summary": summary, "days": rows}


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, coverage = build_corrected_baseline(db_path, calendar)
    permissions = permission_table(events)
    baseline, challenger, priority = select_top3(events, permissions)
    ranking_end = int(coverage["ranking_history_end"])
    calendar_counts = {"train": sum(20240101 <= d <= 20241231 for d in calendar), "validation": sum(20250101 <= d <= 20251231 for d in calendar), "shadow": sum(20260101 <= d <= ranking_end for d in calendar)}
    bm = {s: metrics(baseline, s, calendar_counts[s]) for s in calendar_counts}
    cm = {s: metrics(challenger, s, calendar_counts[s]) for s in calendar_counts}
    branch = branching(baseline, challenger)
    v, bv = cm["validation"], bm["validation"]
    gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": v["daily_profit_factor"] is not None and bv["daily_profit_factor"] is not None and v["daily_profit_factor"] - bv["daily_profit_factor"] >= 0.10, "calendar_expectancy_improves": v["calendar_expectancy"] is not None and v["calendar_expectancy"] > bv["calendar_expectancy"], "frequency_ge_one_day_week": v["signals_per_week"] >= 1.0, "cvar_non_degrade": v["cvar10"] is not None and bv["cvar10"] is not None and v["cvar10"] >= bv["cvar10"] - 1e-12, "drawdown_non_degrade": v["max_drawdown_equal_weight"] is not None and bv["max_drawdown_equal_weight"] is not None and v["max_drawdown_equal_weight"] >= bv["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch["summary"]["validation"]["changed_day_rate"] or 0) >= 0.20}
    if all(gates.values()):
        decision = "keep_shadow_2026"
    elif (branch["summary"]["validation"]["changed_day_rate"] or 0) < 0.20:
        decision = "drop_no_meaningful_branching"
    elif v["daily_profit_factor"] is not None and v["daily_profit_factor"] < 1.0:
        decision = "drop_effectiveness"
    else:
        decision = "hold"
    now = datetime.now(timezone.utc)
    root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False)
    challenger.to_csv(root / "challenger_health_priority_top3.csv", index=False)
    priority.to_csv(root / "point_in_time_side_priority.csv", index=False)
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"candidate_pool": "corrected MeeMee ranking appearance BUY and SELL each execution-eligible rank-asc top3; maximum six", "baseline": "deterministic interleave buy1,sell1,buy2,sell2,buy3,sell3 then first3", "challenger": "same interleave with first side selected by lagged matured health tuple qualified(PF>=1.3,exp>0,CVaR10>=-8%), PF, expectancy, deterministic buy tie", "candidate_suppression": False, "daily_global_cap": 3, "execution": "unchanged TP8/SL5/H10/10bp", "splits": {"train": "2024 reused", "validation": "2025", "shadow": "2026 through ranking history end"}, "shadow_tuning": False}, "coverage": coverage, "source_artifacts": [{"path": str(db_path), "sha256": sha256(db_path)}], "baseline_fixed_interleave": bm, "challenger_health_priority": cm, "branching": branch, "validation_keep_gates": gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_point_in_time_side_priority_top3_validation"}, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False, "silent_fallback_used": False}
    path = root / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
