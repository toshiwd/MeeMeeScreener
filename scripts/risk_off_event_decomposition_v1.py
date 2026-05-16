from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "risk_off_event_decomposition_v1"
SCHEMA_PREFIX = "tradex_risk_off_event_decomposition_v1"
DEFAULT_OUTPUT_DIR_NAME = "risk_off_event_decomposition_v1"

REQUIRED_ARTIFACTS = (
    "risk_off_event_decomposition_summary.json",
    "risk_off_event_cases.csv",
    "risk_off_good_events.csv",
    "risk_off_bad_events.csv",
    "risk_off_2024_damage_cases.csv",
    "risk_off_saved_loss_cases.csv",
    "risk_off_missed_profit_cases.csv",
    "risk_off_market_context.csv",
    "risk_off_candidate_context.csv",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "market_regime_gated_risk_off_pretest",
    "faster_risk_on_reentry_pretest",
    "softer_trim_ratio_pretest",
    "new_buy_stop_only_pretest",
    "position_sizing_haircut_pretest",
    "abandon_risk_off_cash_control",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _ratio(end: float | None, start: float | None) -> float | None:
    if end is None or start is None or start == 0:
        return None
    return float(end) / float(start) - 1.0


def _event_class(year: int, net_effect: float, benchmark_return: float | None, risk_ret: float | None, base_ret: float | None) -> str:
    if net_effect > 100_000:
        return "saved_loss"
    if net_effect < -100_000:
        if year in {2024, 2025}:
            return "recovery_year_damage"
        if benchmark_return is not None and benchmark_return > 0:
            return "false_risk_off"
        if risk_ret is not None and base_ret is not None and risk_ret < base_ret:
            return "missed_profit"
    if net_effect < -30_000 and benchmark_return is not None and benchmark_return > 0:
        return "delayed_reentry_damage"
    return "neutral"


def _market_regime(bench_ret20: float | None, bench_ret60: float | None, bench_dd: float | None) -> str:
    if bench_ret20 is None:
        return "unknown"
    if bench_ret20 > 0.03 and (bench_ret60 is None or bench_ret60 > 0):
        return "benchmark_up"
    if bench_dd is not None and bench_dd <= -0.08:
        return "benchmark_down"
    if bench_ret20 > 0 and bench_ret60 is not None and bench_ret60 < 0:
        return "recovery"
    if abs(bench_ret20) <= 0.02:
        return "sideways"
    return "benchmark_down" if bench_ret20 < 0 else "benchmark_up"


def _event_pairs(events: pd.DataFrame, equity: pd.DataFrame) -> list[dict[str, Any]]:
    risk_off = events[events["event_type"].astype(str) == "risk_off"].copy()
    risk_on = events[events["event_type"].astype(str) == "risk_on"].copy()
    last_by_year = equity.groupby("year", sort=False)["ymd"].max().to_dict()
    out: list[dict[str, Any]] = []
    for _idx, row in risk_off.iterrows():
        event_id = str(row["event_id"])
        year = int(row["year"])
        on = risk_on[risk_on["event_id"].astype(str) == event_id]
        end_ymd = _safe_int(on.iloc[0].get("decision_ymd")) if not on.empty else _safe_int(last_by_year.get(year))
        out.append(
            {
                "event_id": event_id,
                "year": year,
                "event_start_date": _safe_int(row.get("decision_ymd")),
                "event_execution_date": _safe_int(row.get("execution_ymd")),
                "event_end_date": end_ymd,
                "trigger_trailing_dd": _safe_float(row.get("trailing_dd")),
                "open_position_count": _safe_float(row.get("open_position_count")),
                "trim_ratio": _safe_float(row.get("trim_ratio")),
                "release_reason": None if on.empty else on.iloc[0].get("release_reason"),
            }
        )
    return out


def _load_candidate_context(robustness_root: Path, event: dict[str, Any]) -> dict[str, Any]:
    year = int(event["year"])
    run_dir = robustness_root / "subruns" / f"{year}-baseline-portfolio_agent_replay_v1"
    start = event["event_start_date"]
    end = event["event_end_date"]
    if start is None or end is None or not run_dir.exists():
        return {"event_id": event["event_id"], "year": year, "candidate_context_status": "unavailable"}
    candidates_path = run_dir / "daily_candidate_snapshot.csv"
    outcomes_path = run_dir / "post_run_outcome_labels.csv"
    if not candidates_path.exists() or not outcomes_path.exists():
        return {"event_id": event["event_id"], "year": year, "candidate_context_status": "unavailable_missing_artifact"}
    candidates = pd.read_csv(candidates_path)
    outcomes = pd.read_csv(outcomes_path)
    candidates["decision_ymd"] = candidates["decision_ymd"].astype(int)
    candidates["candidate_rank"] = pd.to_numeric(candidates["candidate_rank"], errors="coerce")
    slice_candidates = candidates[(candidates["decision_ymd"] >= int(start)) & (candidates["decision_ymd"] <= int(end))].copy()
    if slice_candidates.empty:
        return {"event_id": event["event_id"], "year": year, "candidate_context_status": "empty"}
    outcomes["decision_ymd"] = outcomes["decision_ymd"].astype(int)
    outcomes["code"] = outcomes["code"].astype(str)
    slice_candidates["code"] = slice_candidates["code"].astype(str)
    joined = slice_candidates.merge(outcomes[["decision_ymd", "code", "post_ret_20", "mfe_20"]], on=["decision_ymd", "code"], how="left")
    top10 = joined[joined["candidate_rank"] <= 10].copy()
    selected = joined[joined.get("selected_for_buy", False).astype(str).str.lower().isin(["true", "1"])].copy() if "selected_for_buy" in joined.columns else pd.DataFrame()
    missed_big = top10[(pd.to_numeric(top10["post_ret_20"], errors="coerce") >= 0.08) | (pd.to_numeric(top10["mfe_20"], errors="coerce") >= 0.12)].copy()
    return {
        "event_id": event["event_id"],
        "year": year,
        "candidate_context_status": "available",
        "candidate_days": int(slice_candidates["decision_ymd"].nunique()),
        "top10_candidate_count": int(len(top10)),
        "top10_post_ret20_mean": None if top10.empty else float(pd.to_numeric(top10["post_ret_20"], errors="coerce").mean()),
        "selected_post_ret20_mean": None if selected.empty else float(pd.to_numeric(selected["post_ret_20"], errors="coerce").mean()),
        "missed_big_winner_top10_count": int(len(missed_big)),
        "missed_big_winner_top10_post_ret20_sum": float(pd.to_numeric(missed_big["post_ret_20"], errors="coerce").fillna(0).sum()) if not missed_big.empty else 0.0,
    }


def _event_metrics(event: dict[str, Any], equity: pd.DataFrame, orders: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    year = int(event["year"])
    start = event["event_execution_date"] or event["event_start_date"]
    end = event["event_end_date"]
    year_eq = equity[equity["year"].astype(int) == year].sort_values("ymd", kind="stable").copy()
    slice_eq = year_eq[(year_eq["ymd"].astype(int) >= int(start)) & (year_eq["ymd"].astype(int) <= int(end))].copy() if start and end else pd.DataFrame()
    if slice_eq.empty:
        case = dict(event)
        case.update(
            {
                "duration_days": 0,
                "portfolio_return_during_event": None,
                "benchmark_return_during_event": None,
                "baseline_return_during_same_period": None,
                "risk_off_return_during_same_period": None,
                "saved_loss_estimate": 0.0,
                "missed_profit_estimate": 0.0,
                "net_effect": 0.0,
                "event_class": "neutral",
                "risk_off_trim_count": 0,
                "risk_off_trim_notional": 0.0,
                "blocked_buy_count": 0,
            }
        )
        return case, {"event_id": event["event_id"], "year": year, "market_context_status": "empty_event_slice"}

    first = slice_eq.iloc[0]
    last = slice_eq.iloc[-1]
    risk_start = _safe_float(first.get("equity"))
    risk_end = _safe_float(last.get("equity"))
    base_start = _safe_float(first.get("baseline_equity"))
    base_end = _safe_float(last.get("baseline_equity"))
    bench_start = _safe_float(first.get("market_benchmark_equity"))
    bench_end = _safe_float(last.get("market_benchmark_equity"))
    risk_ret = _ratio(risk_end, risk_start)
    base_ret = _ratio(base_end, base_start)
    bench_ret = _ratio(bench_end, bench_start)
    risk_delta = 0.0 if risk_start is None or risk_end is None else risk_end - risk_start
    base_delta = 0.0 if base_start is None or base_end is None else base_end - base_start
    net_effect = risk_delta - base_delta
    event_orders = orders[(orders.get("event_id", pd.Series(dtype=str)).astype(str) == str(event["event_id"]))].copy()
    trims = event_orders[(event_orders["action"].astype(str) == "risk_off_trim") & (event_orders["order_status"].astype(str) == "filled")]
    blocked = orders[
        (orders["execution_ymd"].fillna(0).astype(float).astype(int) >= int(start))
        & (orders["execution_ymd"].fillna(0).astype(float).astype(int) <= int(end))
        & (orders["action"].astype(str) == "buy")
        & (orders["order_status"].astype(str) == "unfilled")
        & (orders.get("unfilled_reason", pd.Series(dtype=str)).astype(str) == "risk_off_or_position_limit")
    ]
    case = dict(event)
    case.update(
        {
            "duration_days": int(len(slice_eq)),
            "portfolio_return_during_event": risk_ret,
            "benchmark_return_during_event": bench_ret,
            "baseline_return_during_same_period": base_ret,
            "risk_off_return_during_same_period": risk_ret,
            "saved_loss_estimate": max(net_effect, 0.0),
            "missed_profit_estimate": max(-net_effect, 0.0),
            "net_effect": net_effect,
            "event_class": _event_class(year, net_effect, bench_ret, risk_ret, base_ret),
            "risk_off_trim_count": int(len(trims)),
            "risk_off_trim_notional": float(pd.to_numeric(trims.get("notional", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()),
            "risk_off_trim_realized_pnl": float(pd.to_numeric(trims.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()),
            "blocked_buy_count": int(len(blocked)),
        }
    )

    trigger_idx = year_eq.index[year_eq["ymd"].astype(int) <= int(event["event_start_date"])].tolist()
    hist = year_eq.loc[trigger_idx].tail(61).copy() if trigger_idx else pd.DataFrame()
    current_bench = _safe_float(hist.iloc[-1].get("market_benchmark_equity")) if not hist.empty else None
    bench_20 = _safe_float(hist.iloc[-21].get("market_benchmark_equity")) if len(hist) >= 21 else None
    bench_60 = _safe_float(hist.iloc[-61].get("market_benchmark_equity")) if len(hist) >= 61 else None
    hist_bench = pd.to_numeric(hist.get("market_benchmark_equity", pd.Series(dtype=float)), errors="coerce")
    bench_peak = float(hist_bench.max()) if not hist_bench.empty else None
    bench_dd = None if current_bench is None or not bench_peak else current_bench / bench_peak - 1.0
    market = {
        "event_id": event["event_id"],
        "year": year,
        "event_start_date": event["event_start_date"],
        "benchmark_20d_return": _ratio(current_bench, bench_20),
        "benchmark_60d_return": _ratio(current_bench, bench_60),
        "benchmark_drawdown": bench_dd,
        "benchmark_return_during_event": bench_ret,
        "market_regime_diagnostic": _market_regime(_ratio(current_bench, bench_20), _ratio(current_bench, bench_60), bench_dd),
        "market_context_status": "available",
    }
    return case, market


def _choose_next_axis(cases: pd.DataFrame, candidates: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    bad = cases[cases["net_effect"] < 0].copy()
    damage_2024 = bad[bad["year"].astype(int) == 2024].copy()
    total_missed = float(pd.to_numeric(cases["missed_profit_estimate"], errors="coerce").fillna(0).sum())
    total_saved = float(pd.to_numeric(cases["saved_loss_estimate"], errors="coerce").fillna(0).sum())
    market_up_bad = bad[pd.to_numeric(bad["benchmark_return_during_event"], errors="coerce") > 0]
    blocked_damage = float(pd.to_numeric(bad["blocked_buy_count"], errors="coerce").fillna(0).sum())
    damage_2024_total = float(pd.to_numeric(damage_2024["missed_profit_estimate"], errors="coerce").fillna(0).sum())
    top10_positive_bad = candidates[
        (candidates["event_id"].isin(bad["event_id"]))
        & (pd.to_numeric(candidates.get("top10_post_ret20_mean", pd.Series(dtype=float)), errors="coerce") > 0)
    ]
    evidence = {
        "event_count": int(len(cases)),
        "saved_loss_event_count": int((cases["event_class"].astype(str) == "saved_loss").sum()),
        "bad_event_count": int(len(bad)),
        "market_up_bad_event_count": int(len(market_up_bad)),
        "damage_2024_event_count": int(len(damage_2024)),
        "total_saved_loss_estimate": total_saved,
        "total_missed_profit_estimate": total_missed,
        "net_effect_total": total_saved - total_missed,
        "damage_2024_missed_profit_estimate": damage_2024_total,
        "bad_event_blocked_buy_count": blocked_damage,
        "bad_event_positive_top10_context_count": int(len(top10_positive_bad)),
    }
    if total_missed > total_saved * 2 and len(bad) >= len(cases) * 0.65:
        return "abandon_risk_off_cash_control", "risk_off_events_are_net_profit_damaging", evidence
    if len(market_up_bad) >= max(2, len(bad) * 0.45):
        return "market_regime_gated_risk_off_pretest", "bad_events_often_occur_while_benchmark_rises", evidence
    if blocked_damage >= max(3, len(bad) * 0.5):
        return "new_buy_stop_only_pretest", "new_buy_blocking_is_material_in_bad_events", evidence
    if damage_2024_total > total_saved:
        return "faster_risk_on_reentry_pretest", "recovery_year_damage_dominates_saved_loss", evidence
    return "softer_trim_ratio_pretest", "trim_exposure_cut_damages_profit_but_dd_helped", evidence


def run_event_decomposition(risk_off_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    risk_off_root = Path(risk_off_root)
    output_root = Path(output_root) if output_root else risk_off_root / DEFAULT_OUTPUT_DIR_NAME
    summary = _read_json(risk_off_root / "risk_off_cash_control_summary.json")
    robustness_root = Path(summary["robustness_root"])
    events = pd.read_csv(risk_off_root / "risk_off_events.csv")
    orders = pd.read_csv(risk_off_root / "risk_off_orders_ledger.csv")
    equity = pd.read_csv(risk_off_root / "risk_off_equity_curve_by_year.csv")
    pairs = _event_pairs(events, equity)
    cases: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for event in pairs:
        case, market = _event_metrics(event, equity, orders)
        cases.append(case)
        markets.append(market)
        candidates.append(_load_candidate_context(robustness_root, event))
    cases_df = pd.DataFrame(cases)
    market_df = pd.DataFrame(markets)
    candidate_df = pd.DataFrame(candidates)
    good_df = cases_df[cases_df["net_effect"] > 0].copy()
    bad_df = cases_df[cases_df["net_effect"] < 0].copy()
    damage_2024_df = bad_df[bad_df["year"].astype(int) == 2024].copy()
    saved_df = cases_df[cases_df["saved_loss_estimate"] > 0].copy()
    missed_df = cases_df[cases_df["missed_profit_estimate"] > 0].copy()
    decision, reason, evidence = _choose_next_axis(cases_df, candidate_df)

    _write_csv(output_root / "risk_off_event_cases.csv", cases_df)
    _write_csv(output_root / "risk_off_good_events.csv", good_df)
    _write_csv(output_root / "risk_off_bad_events.csv", bad_df)
    _write_csv(output_root / "risk_off_2024_damage_cases.csv", damage_2024_df)
    _write_csv(output_root / "risk_off_saved_loss_cases.csv", saved_df)
    _write_csv(output_root / "risk_off_missed_profit_cases.csv", missed_df)
    _write_csv(output_root / "risk_off_market_context.csv", market_df)
    _write_csv(output_root / "risk_off_candidate_context.csv", candidate_df)
    decomposition_summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "source_risk_off_root": str(risk_off_root),
        "source_decision": summary.get("decision"),
        "source_reason_type": summary.get("reason_type"),
        "source_event_row_count": int(len(events)),
        "source_risk_off_episode_count": int((events["event_type"].astype(str) == "risk_off").sum()),
        "source_risk_on_row_count": int((events["event_type"].astype(str) == "risk_on").sum()),
        "event_count": int(len(cases_df)),
        "event_class_counts": cases_df["event_class"].astype(str).value_counts().to_dict() if not cases_df.empty else {},
        "total_saved_loss_estimate": float(pd.to_numeric(cases_df["saved_loss_estimate"], errors="coerce").fillna(0).sum()) if not cases_df.empty else 0.0,
        "total_missed_profit_estimate": float(pd.to_numeric(cases_df["missed_profit_estimate"], errors="coerce").fillna(0).sum()) if not cases_df.empty else 0.0,
        "net_effect_total": float(pd.to_numeric(cases_df["net_effect"], errors="coerce").fillna(0).sum()) if not cases_df.empty else 0.0,
        "damage_2024_event_count": int(len(damage_2024_df)),
        "damage_2024_missed_profit_estimate": float(pd.to_numeric(damage_2024_df["missed_profit_estimate"], errors="coerce").fillna(0).sum()) if not damage_2024_df.empty else 0.0,
        "decision": decision,
        "reason_type": reason,
        "metrics": evidence,
        "scope": {"tradex_only": True, "rule_changed": False, "threshold_changed": False, "sweep_used": False, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False},
    }
    _write_json(output_root / "risk_off_event_decomposition_summary.json", decomposition_summary)
    _write_json(
        output_root / "next_axis_decision.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "decision_candidates": list(DECISIONS),
            "decision": decision,
            "decision_count": 1,
            "reason_type": reason,
            "metrics": evidence,
            "policy_promotion_allowed": False,
            "meemee_reflectable": False,
        },
    )
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": True,
        "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
        "decision": decision,
        "decision_count": 1,
        "rule_changed": False,
        "threshold_changed": False,
        "sweep_used": False,
        "silent_fallback_used": False,
        "meemee_reflectable": False,
        "policy_promotion_allowed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompose risk-off cash-control events.")
    parser.add_argument("--risk-off-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_event_decomposition(args.risk_off_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
