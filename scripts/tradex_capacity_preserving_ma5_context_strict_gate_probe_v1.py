from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_common_ledger_field_repair_v1 import _build_forward_labels, _load_daily_rows


AXIS_ID = "capacity_preserving_ma5_context_strict_gate_probe_v1"
DEFAULT_RUN_ID = "20260514T235500Z-capacity-preserving-ma5-context-strict-gate-probe-v1"
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/capacity_preserving_ma5_context_strict_gate_probe_v1")
DEFAULT_REPAIRED_LEDGER = Path(
    "G:/Tradex/common_ledger_field_repair_v1/20260514T230000Z-common-ledger-field-repair-v1/"
    "repaired_common_top5_candidate_ledger.jsonl"
)
DEFAULT_MA5_TRADE_LEDGER = Path(
    "G:/Tradex/ma5_reclaim_ma20_exit_probe_v1/"
    "20260512T000000Z-ma5-reclaim-ma20-exit-probe-v1-ma5_reclaim_ma20_exit_probe_v1/trade_ledger.jsonl"
)
DEFAULT_SOURCE_DUCKDB = Path(
    "G:/Tradex/db/meemee_snapshots/"
    "20260512T130453Z_winner_lookalike_candle_decomposition_v1/stocks.duckdb"
)
DEFAULT_SOURCE_DAILY_MAX_YMD = 20260508

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "capacity_preserving_probe_report.json",
    "strict_gate_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

MANDATORY_GATES = [
    "top5_avg_ret20_improved",
    "top5_big_winner_capture_improved",
    "top5_future_top10_capture_improved",
    "top5_severe_loss_rate_not_worse",
    "top5_bad_pick_count_not_increased",
    "human_selectable_day_rate_not_worse",
    "family_concentration_not_excessive",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _complete(row: Mapping[str, Any]) -> bool:
    return all(row.get(field) is not None for field in ("ret20_fwd", "mfe20", "mae20", "severe_loss20"))


def _prepare_base(path: Path) -> pd.DataFrame:
    rows = [row for row in _read_jsonl(path) if _complete(row)]
    frame = pd.DataFrame(rows)
    for col in ["baseline_candidate_flag", "momentum_candidate_flag", "combined_candidate_flag", "severe_loss20", "win20"]:
        frame[col] = frame[col].fillna(False).astype(bool)
    dates = set(frame.loc[frame["baseline_candidate_flag"], "event_date"]) & set(frame.loc[frame["combined_candidate_flag"], "event_date"])
    frame = frame[frame["event_date"].isin(dates)].copy()
    frame["ret20_fwd"] = pd.to_numeric(frame["ret20_fwd"])
    frame["baseline_score"] = pd.to_numeric(frame["baseline_score"], errors="coerce")
    frame["ma5_candidate_flag"] = False
    for col in ["ma_stack", "ma60_slope_state", "ma20_vs_ma60", "price_vs_ma60"]:
        if col not in frame:
            frame[col] = None
    return frame


def _prepare_ma5_additive(trade_ledger: Path, source_duckdb: Path, dates: set[str], source_daily_max_ymd: int) -> pd.DataFrame:
    trades = [row for row in _read_jsonl(trade_ledger) if row.get("signal_date") in dates]
    if not trades:
        return pd.DataFrame()
    trade_frame = pd.DataFrame(trades)
    symbols = sorted(trade_frame["symbol"].astype(str).unique())
    ymds = [int(str(value).replace("-", "")) for value in trade_frame["signal_date"]]
    daily = _load_daily_rows(source_duckdb, symbols, min(ymds), int(source_daily_max_ymd))
    labels = _build_forward_labels(daily)
    label_lookup = {(str(row.symbol), int(row.event_ymd)): row for row in labels.itertuples(index=False)}
    out = []
    for row in trades:
        ymd = int(str(row["signal_date"]).replace("-", ""))
        label = label_lookup.get((str(row["symbol"]), ymd))
        if label is None or any(pd.isna(getattr(label, field)) for field in ("ret20_fwd", "mfe20", "mae20")):
            continue
        out.append(
            {
                "event_date": row["signal_date"],
                "symbol": str(row["symbol"]),
                "baseline_candidate_flag": False,
                "momentum_candidate_flag": False,
                "ma5_candidate_flag": True,
                "baseline_score": None,
                "ret20_fwd": float(label.ret20_fwd),
                "mfe20": float(label.mfe20),
                "mae20": float(label.mae20),
                "severe_loss20": bool(label.severe_loss20),
                "win20": bool(label.win20),
                "ma_stack": row.get("ma_stack"),
                "ma60_slope_state": row.get("ma60_slope_state"),
                "ma20_vs_ma60": row.get("ma20_vs_ma60"),
                "price_vs_ma60": row.get("price_vs_ma60"),
            }
        )
    return pd.DataFrame(out)


def _build_universe(base: pd.DataFrame, ma5: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "event_date",
        "symbol",
        "baseline_candidate_flag",
        "momentum_candidate_flag",
        "ma5_candidate_flag",
        "baseline_score",
        "ret20_fwd",
        "mfe20",
        "mae20",
        "severe_loss20",
        "win20",
        "ma_stack",
        "ma60_slope_state",
        "ma20_vs_ma60",
        "price_vs_ma60",
    ]
    frame = pd.concat([base[cols], ma5[cols]], ignore_index=True)
    frame = frame.drop_duplicates(["event_date", "symbol", "baseline_candidate_flag", "momentum_candidate_flag", "ma5_candidate_flag"], keep="first")
    for col in ["baseline_candidate_flag", "momentum_candidate_flag", "ma5_candidate_flag", "severe_loss20", "win20"]:
        frame[col] = frame[col].fillna(False).astype(bool)
    frame["ret20_fwd"] = pd.to_numeric(frame["ret20_fwd"])
    frame["baseline_score"] = pd.to_numeric(frame["baseline_score"], errors="coerce")
    frame["is_big"] = frame["ret20_fwd"].ge(0.10)
    frame["is_bad"] = frame["ret20_fwd"].le(0.0) | frame["severe_loss20"]
    frame["human_selectable"] = frame["ret20_fwd"].gt(0.0) & ~frame["severe_loss20"]
    frame["future_top10"] = False
    for _date, group in frame.groupby("event_date"):
        frame.loc[group.sort_values(["ret20_fwd", "symbol"], ascending=[False, True]).head(10).index, "future_top10"] = True
    return frame


def _select(frame: pd.DataFrame, score: pd.Series, base_counts: Mapping[str, int]) -> pd.DataFrame:
    work = frame.assign(_score=score)
    work = work[work["_score"].notna()].sort_values(["event_date", "_score", "symbol"], ascending=[True, False, True], kind="stable")
    parts = []
    for event_date, group in work.groupby("event_date", sort=False):
        count = int(base_counts.get(str(event_date), 0))
        if count:
            parts.append(group.head(count))
    return pd.concat(parts, ignore_index=True) if parts else work.head(0)


def _metrics(selected: pd.DataFrame, universe: pd.DataFrame, date_count: int) -> dict[str, Any]:
    family_counts = {
        "baseline_only": int((selected["baseline_candidate_flag"] & ~selected["momentum_candidate_flag"] & ~selected["ma5_candidate_flag"]).sum()),
        "momentum": int((selected["momentum_candidate_flag"] & ~selected["ma5_candidate_flag"]).sum()),
        "ma5": int(selected["ma5_candidate_flag"].sum()),
    }
    return {
        "candidate_count": int(len(selected)),
        "top5_avg_ret20": float(selected["ret20_fwd"].mean()),
        "top5_big_winner_capture_rate": _rate(int(selected["is_big"].sum()), int(universe["is_big"].sum())),
        "top5_future_top10_capture_rate": _rate(int(selected["future_top10"].sum()), int(universe["future_top10"].sum())),
        "top5_severe_loss_rate20": _rate(int(selected["severe_loss20"].sum()), len(selected)),
        "top5_bad_pick_count": int(selected["is_bad"].sum()),
        "human_selectable_day_rate": _rate(int((selected.groupby("event_date")["human_selectable"].sum() >= 3).sum()), date_count),
        "family_counts": family_counts,
        "max_family_share": max(_rate(value, len(selected)) for value in family_counts.values()) if len(selected) else 0.0,
        "member_keys": set(zip(selected["event_date"].astype(str), selected["symbol"].astype(str))),
    }


def _rate(numer: int, denom: int) -> float:
    return float(numer / denom) if denom else 0.0


def _delta(value: float, base: float) -> float:
    return float(value - base)


def _condition_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "h14_bull_or_near_bull_ma60_not_falling": frame["ma60_slope_state"].astype(str).isin(["ma60_flat", "ma60_rising"])
        & frame["ma_stack"].astype(str).isin(["bull_stack_5_20_60", "ma5_above_20_below_60"]),
        "h06_ma60_not_falling": frame["ma60_slope_state"].astype(str).isin(["ma60_flat", "ma60_rising"]),
        "h17_bear_stack_ma60_rising": frame["ma_stack"].astype(str).eq("bear_stack_5_20_60")
        & frame["ma60_slope_state"].astype(str).eq("ma60_rising"),
        "h03_ma20_above_ma60": frame["ma20_vs_ma60"].astype(str).eq("ma20_above_ma60"),
    }


def _run_probe(frame: pd.DataFrame, base_counts: Mapping[str, int]) -> dict[str, Any]:
    date_count = len(base_counts)
    baseline_score = frame["baseline_score"].where(frame["baseline_candidate_flag"])
    baseline = _select(frame, baseline_score, base_counts)
    base_metrics = _metrics(baseline, frame, date_count)
    masks = _condition_masks(frame)
    rows = []
    for condition_id, mask in masks.items():
        for ma5_score in [-0.5, 0.0, 0.5, 1.0, 2.0]:
            for momentum_weight in [0.0, 0.01, 0.02, 0.03]:
                score = baseline_score + frame["momentum_candidate_flag"].astype(float) * momentum_weight
                score = score.where(~(frame["ma5_candidate_flag"] & mask), ma5_score)
                selected = _select(frame, score, base_counts)
                metrics = _metrics(selected, frame, date_count)
                deltas = {
                    "top5_avg_ret20_delta_vs_baseline": _delta(metrics["top5_avg_ret20"], base_metrics["top5_avg_ret20"]),
                    "top5_big_winner_capture_delta_vs_baseline": _delta(metrics["top5_big_winner_capture_rate"], base_metrics["top5_big_winner_capture_rate"]),
                    "top5_future_top10_capture_delta_vs_baseline": _delta(metrics["top5_future_top10_capture_rate"], base_metrics["top5_future_top10_capture_rate"]),
                    "top5_severe_loss_rate_delta_vs_baseline": _delta(metrics["top5_severe_loss_rate20"], base_metrics["top5_severe_loss_rate20"]),
                    "top5_bad_pick_count_delta_vs_baseline": int(metrics["top5_bad_pick_count"]) - int(base_metrics["top5_bad_pick_count"]),
                    "human_selectable_day_rate_delta_vs_baseline": _delta(metrics["human_selectable_day_rate"], base_metrics["human_selectable_day_rate"]),
                    "top5_changed_members_count_vs_baseline": len(metrics["member_keys"].symmetric_difference(base_metrics["member_keys"])),
                }
                gates = {
                    "top5_avg_ret20_improved": deltas["top5_avg_ret20_delta_vs_baseline"] > 0.0,
                    "top5_big_winner_capture_improved": deltas["top5_big_winner_capture_delta_vs_baseline"] > 0.0,
                    "top5_future_top10_capture_improved": deltas["top5_future_top10_capture_delta_vs_baseline"] > 0.0,
                    "top5_severe_loss_rate_not_worse": deltas["top5_severe_loss_rate_delta_vs_baseline"] <= 0.0,
                    "top5_bad_pick_count_not_increased": deltas["top5_bad_pick_count_delta_vs_baseline"] <= 0,
                    "human_selectable_day_rate_not_worse": deltas["human_selectable_day_rate_delta_vs_baseline"] >= 0.0,
                    "family_concentration_not_excessive": metrics["max_family_share"] <= 0.90,
                }
                rows.append(
                    {
                        "variant_id": f"{condition_id}_ma5{ma5_score}_mom{momentum_weight}",
                        "condition_id": condition_id,
                        "ma5_score": ma5_score,
                        "momentum_weight": momentum_weight,
                        "metrics": {key: value for key, value in metrics.items() if key != "member_keys"},
                        "deltas_vs_baseline": deltas,
                        "gate_results": gates,
                        "gate_pass_count": sum(bool(value) for value in gates.values()),
                        "all_gates_pass": all(bool(value) for value in gates.values()) and deltas["top5_changed_members_count_vs_baseline"] > 0,
                    }
                )
    pass_rows = [row for row in rows if row["all_gates_pass"]]
    ranked = sorted(rows, key=lambda row: (row["all_gates_pass"], row["gate_pass_count"], row["deltas_vs_baseline"]["top5_avg_ret20_delta_vs_baseline"]), reverse=True)
    return {"baseline": {key: value for key, value in base_metrics.items() if key != "member_keys"}, "rows": rows, "pass_rows": pass_rows, "best": ranked[0] if ranked else None}


def run(args: argparse.Namespace) -> Path:
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    base = _prepare_base(args.repaired_ledger)
    dates = set(base["event_date"].astype(str))
    ma5 = _prepare_ma5_additive(args.ma5_trade_ledger, args.source_duckdb, dates, args.source_daily_max_ymd)
    universe = _build_universe(base, ma5)
    base_counts = base[base["baseline_candidate_flag"]].groupby("event_date").size().to_dict()
    result = _run_probe(universe, base_counts)
    pass_count = len(result["pass_rows"])
    best = result["best"]
    decision = "keep_candidate" if pass_count else "drop"
    authoritative = "capacity_preserving_ma5_context_strict_gate_keep_candidate" if pass_count else "capacity_preserving_ma5_context_strict_gate_failed"
    generated_at = _utc_now()
    payloads = {
        "evaluation_contract.json": {
            "schema_version": "tradex_capacity_preserving_ma5_context_evaluation_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "candidate_count_policy": "preserve_baseline_per_date_candidate_count",
            "ma5_exit_labels_used_as_ret20_labels": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
            "production_ranking_changed": False,
        },
        "run_manifest.json": {
            "schema_version": "tradex_capacity_preserving_ma5_context_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "label_complete_base_rows": int(len(base)),
            "ma5_additive_labelled_rows": int(len(ma5)),
            "variant_count": len(result["rows"]),
        },
        "source_artifact_refs.json": {
            "schema_version": "tradex_capacity_preserving_ma5_context_source_refs_v1",
            "repaired_ledger": str(args.repaired_ledger),
            "ma5_trade_ledger": str(args.ma5_trade_ledger),
            "source_duckdb": str(args.source_duckdb),
        },
        "capacity_preserving_probe_report.json": {
            "schema_version": "tradex_capacity_preserving_ma5_context_probe_report_v1",
            "axis_id": AXIS_ID,
            "baseline_metrics": result["baseline"],
            "pass_count": pass_count,
            "best_variant": best,
            "top_rows": sorted(result["rows"], key=lambda row: (row["all_gates_pass"], row["gate_pass_count"], row["deltas_vs_baseline"]["top5_avg_ret20_delta_vs_baseline"]), reverse=True)[:20],
        },
        "strict_gate_report.json": {
            "schema_version": "tradex_capacity_preserving_ma5_context_strict_gate_report_v1",
            "axis_id": AXIS_ID,
            "pass_count": pass_count,
            "best_variant_failed_gates": [gate for gate, ok in (best or {}).get("gate_results", {}).items() if not ok],
            "best_variant_gate_results": (best or {}).get("gate_results", {}),
        },
        "research_decision.json": {
            "schema_version": "tradex_capacity_preserving_ma5_context_research_decision_v1",
            "generated_at_utc": generated_at,
            "research_phase": "capacity_preserving_ma5_context_strict_gate_probe",
            "boundary": "TRADEX-only",
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "strict_pass_variant_count": pass_count,
            "best_variant_id": (best or {}).get("variant_id"),
            "candidate_count_policy": "preserve_baseline_per_date_candidate_count",
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
            "ma5_exit_labels_used_as_ret20_labels": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "typed_reasons": ["all_strict_gates_passed"] if pass_count else ["no_capacity_preserving_ma5_context_variant_satisfied_all_gates"],
        },
    }
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    complete = {
        "schema_version": "tradex_capacity_preserving_ma5_context_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "run_id": args.run_id,
        "artifact_root": str(output_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "artifacts": {},
        "complete": False,
    }
    for name in REQUIRED_ARTIFACTS:
        path = output_root / name
        complete["artifacts"][name] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for name, item in complete["artifacts"].items() if name != "_ARTIFACT_COMPLETE.json")
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {"exists": True, "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size}
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return output_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--repaired-ledger", type=Path, default=DEFAULT_REPAIRED_LEDGER)
    parser.add_argument("--ma5-trade-ledger", type=Path, default=DEFAULT_MA5_TRADE_LEDGER)
    parser.add_argument("--source-duckdb", type=Path, default=DEFAULT_SOURCE_DUCKDB)
    parser.add_argument("--source-daily-max-ymd", type=int, default=DEFAULT_SOURCE_DAILY_MAX_YMD)
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
