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

from scripts.tradex_failed_high_retest_short_backtest_v1 import _load_bars, _signal_for_bars
from scripts.tradex_point_in_time_side_permission_router_v1 import DEFAULT_DB, build_corrected_baseline, metrics
from scripts.tradex_point_in_time_side_priority_top3_v1 import _interleave, branching


AXIS_ID = "tradex_failed_high_priority_promotion_v1"
DEFAULT_AUDIT = Path(r"G:\Tradex\failed_high_retest_short_backtest_v1\20260701T024617Z-failed_high_retest_short_backtest_v1\failed_high_retest_backtest_audit.json")
DEFAULT_OUT = Path(r"G:\Tradex\failed_high_priority_promotion_v1")
EXPECTED_ATOMS = ["peak_age>=120", "peak_prominence>=0.03", "pullback_depth>=0.2", "stage=forming"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def is_failed_high_keep(signal: dict | None) -> bool:
    return bool(
        signal
        and int(signal["peak_age"]) >= 120
        and float(signal["peak_prominence"]) >= 0.03
        and float(signal["pullback_depth"]) >= 0.20
        and signal["stage"] == "forming"
    )


def annotate_sell_candidates(events: pd.DataFrame, db_path: Path) -> tuple[pd.DataFrame, dict]:
    result = events.copy()
    result["failed_high_promotion"] = False
    result["failed_high_stage"] = None
    sell = result[result.side == "sell"]
    missing_dates: list[dict] = []
    recomputed = matched = 0
    with duckdb.connect(str(db_path), read_only=True) as con:
        for code, code_rows in sell.groupby("code", sort=True):
            bars = _load_bars(con, str(code), 19000101, int(code_rows.signal_ymd.max()))
            by_date = {int(bar["date"]): idx for idx, bar in enumerate(bars)}
            for row_idx, row in code_rows.iterrows():
                signal_ymd = int(row.signal_ymd)
                bar_idx = by_date.get(signal_ymd)
                if bar_idx is None:
                    missing_dates.append({"code": str(code), "signal_ymd": signal_ymd})
                    continue
                signal = _signal_for_bars(bars, bar_idx)
                keep = is_failed_high_keep(signal)
                result.at[row_idx, "failed_high_promotion"] = keep
                result.at[row_idx, "failed_high_stage"] = signal.get("stage") if signal else None
                recomputed += 1
                matched += int(keep)
    if missing_dates:
        sample = missing_dates[:10]
        raise ValueError(f"PAN_SIGNAL_BAR_MISSING: count={len(missing_dates)} sample={sample}")
    return result, {
        "sell_candidate_count": int(len(sell)),
        "deterministically_recomputed_count": recomputed,
        "failed_high_match_count": matched,
        "missing_signal_bar_count": 0,
    }


def _interleave_promoted(day: pd.DataFrame) -> pd.DataFrame:
    buy = day[day.side == "buy"].sort_values(["rank", "code"])
    sell = day[day.side == "sell"].sort_values(
        ["failed_high_promotion", "rank", "code"], ascending=[False, True, True]
    )
    buckets = {"buy": buy.to_dict("records"), "sell": sell.to_dict("records")}
    ordered: list[dict] = []
    for idx in range(3):
        for side in ("buy", "sell"):
            if idx < len(buckets[side]):
                ordered.append(buckets[side][idx])
    selected = pd.DataFrame(ordered[:3])
    if len(selected):
        selected["global_rank"] = range(1, len(selected) + 1)
    return selected


def select(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    baseline_parts, challenger_parts = [], []
    for _, day in events.groupby("signal_ymd", sort=True):
        baseline_parts.append(_interleave(day, "buy"))
        challenger_parts.append(_interleave_promoted(day))
    return pd.concat(baseline_parts, ignore_index=True), pd.concat(challenger_parts, ignore_index=True)


def generate(db_path: Path, audit_path: Path, out_root: Path) -> Path:
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    atoms = audit.get("top_candidate", {}).get("rule_atoms")
    if atoms != EXPECTED_ATOMS or not audit.get("top_candidate", {}).get("passes_target_all_splits"):
        raise ValueError(f"AUTHORITATIVE_RULE_CONTRACT_MISMATCH: atoms={atoms}")

    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute(
            "select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1"
        ).fetchall()]
    events, coverage = build_corrected_baseline(db_path, calendar)
    annotated, recomputation = annotate_sell_candidates(events, db_path)
    baseline, challenger = select(annotated)
    ranking_end = int(coverage["ranking_history_end"])
    calendar_counts = {
        "train": sum(20240101 <= d <= 20241231 for d in calendar),
        "validation": sum(20250101 <= d <= 20251231 for d in calendar),
        "shadow": sum(20260101 <= d <= ranking_end for d in calendar),
    }
    baseline_metrics = {split: metrics(baseline, split, calendar_counts[split]) for split in calendar_counts}
    challenger_metrics = {split: metrics(challenger, split, calendar_counts[split]) for split in calendar_counts}
    branch = branching(baseline, challenger)
    base_v, chal_v = baseline_metrics["validation"], challenger_metrics["validation"]
    gates = {
        "daily_pf_ge_1_30": chal_v["daily_profit_factor"] is not None and chal_v["daily_profit_factor"] >= 1.30,
        "daily_pf_delta_ge_0_10": chal_v["daily_profit_factor"] is not None and base_v["daily_profit_factor"] is not None and chal_v["daily_profit_factor"] - base_v["daily_profit_factor"] >= 0.10,
        "calendar_expectancy_improves": chal_v["calendar_expectancy"] is not None and chal_v["calendar_expectancy"] > base_v["calendar_expectancy"],
        "frequency_ge_one_day_week": chal_v["signals_per_week"] >= 1.0,
        "cvar_non_degrade": chal_v["cvar10"] is not None and base_v["cvar10"] is not None and chal_v["cvar10"] >= base_v["cvar10"] - 1e-12,
        "drawdown_non_degrade": chal_v["max_drawdown_equal_weight"] is not None and base_v["max_drawdown_equal_weight"] is not None and chal_v["max_drawdown_equal_weight"] >= base_v["max_drawdown_equal_weight"] - 1e-12,
        "branch_ge_20pct": (branch["summary"]["validation"]["changed_day_rate"] or 0) >= 0.20,
    }
    if all(gates.values()):
        decision = "keep_shadow_2026"
    elif (branch["summary"]["validation"]["changed_day_rate"] or 0) < 0.20:
        decision = "drop_no_meaningful_branching"
    elif chal_v["daily_profit_factor"] is not None and chal_v["daily_profit_factor"] < 1.0:
        decision = "drop_effectiveness"
    else:
        decision = "hold"

    now = datetime.now(timezone.utc)
    root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False)
    challenger.to_csv(root / "challenger_failed_high_sell_promotion_top3.csv", index=False)
    annotated.to_csv(root / "candidate_recomputation_audit.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "candidate_pool": "corrected MeeMee ranking appearance BUY and SELL each execution-eligible rank-asc top3; maximum six",
            "only_axis": "SELL candidates matching failed-high keep atoms are priority-promoted within SELL",
            "candidate_suppression": False,
            "buy_order_changed": False,
            "baseline": "deterministic interleave buy1,sell1,buy2,sell2,buy3,sell3 then first3",
            "execution": "unchanged TP8/SL5/H10/10bp",
            "splits": {"train": "2024", "validation": "2025", "shadow": "2026 through ranking history end"},
            "shadow_tuning": False,
        },
        "deterministic_recomputation": {**recomputation, "uses_pan_bars_through_signal_date_only": True, "fallback_used": False},
        "coverage": coverage,
        "source_artifacts": [
            {"path": str(db_path), "sha256": sha256(db_path)},
            {"path": str(audit_path), "sha256": sha256(audit_path)},
            {"path": str(Path(__file__).resolve()), "sha256": sha256(Path(__file__).resolve())},
        ],
        "baseline_fixed_interleave": baseline_metrics,
        "challenger_failed_high_sell_promotion": challenger_metrics,
        "branching": branch,
        "validation_keep_gates": gates,
        "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_failed_high_sell_priority_validation"},
        "shadow_tuning_used": False,
        "silent_fallback_used": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    path = root / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--audit", type=Path, default=DEFAULT_AUDIT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.audit, args.out))


if __name__ == "__main__":
    main()
