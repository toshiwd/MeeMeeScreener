from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "baseline_regime_failure_decomposition_v1"
SCHEMA_PREFIX = "tradex_baseline_regime_failure_decomposition_v1"
DEFAULT_OUTPUT_DIR_NAME = "baseline_regime_failure_decomposition_v1"
REQUIRED_ARTIFACTS = (
    "baseline_regime_failure_summary.json",
    "yearly_failure_decomposition.csv",
    "monthly_failure_decomposition.csv",
    "regime_bucket_summary.csv",
    "candidate_quality_by_year.csv",
    "bought_vs_rejected_by_year.csv",
    "entry_exit_failure_by_year.csv",
    "exposure_cash_by_year.csv",
    "drawdown_event_cases.csv",
    "benchmark_positive_portfolio_negative_cases.csv",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
NEXT_AXIS_CANDIDATES = (
    "regime_filter_pretest",
    "risk_off_cash_control_pretest",
    "position_sizing_haircut_pretest",
    "entry_confirmation_pretest",
    "exit_risk_control_pretest",
    "candidate_generation_redesign",
    "abandon_current_baseline_policy",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    data.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _month_key(ymd: pd.Series) -> pd.Series:
    return (pd.to_numeric(ymd, errors="coerce").astype("Int64") // 100).astype("Int64")


def _mean_numeric(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    return None if series.empty else float(series.mean())


def _regime_label(port_ret: float, bench_ret: float, max_dd: float) -> str:
    if max_dd <= -0.15:
        return "high_drawdown"
    if bench_ret >= 0.08 and port_ret >= 0.08:
        return "strong_trend_up"
    if bench_ret >= 0.03:
        return "benchmark_up"
    if bench_ret <= -0.03:
        return "benchmark_down"
    if abs(bench_ret) < 0.03 and abs(port_ret) < 0.03:
        return "sideways"
    return "volatile"


def _candidate_quality(run_dir: Path, year: int) -> dict[str, Any]:
    candidates = pd.read_csv(run_dir / "daily_candidate_snapshot.csv")
    outcomes = pd.read_csv(run_dir / "post_run_outcome_labels.csv")
    merged = candidates.merge(outcomes, on=["decision_ymd", "code"], how="left", suffixes=("", "_outcome"))
    merged["decision_month"] = _month_key(merged["decision_ymd"])
    top10 = merged[pd.to_numeric(merged["candidate_rank"], errors="coerce") <= 10].copy()
    bought = merged[merged.get("selected_for_buy", pd.Series(dtype=bool)).fillna(False).astype(bool)].copy()
    rejected_top10 = top10[~top10.get("selected_for_buy", pd.Series(dtype=bool)).fillna(False).astype(bool)].copy()
    return {
        "year": year,
        "candidate_top10_post_ret20_mean": _mean_numeric(top10, "post_ret_20"),
        "bought_post_ret20_mean": _mean_numeric(bought, "post_ret_20"),
        "rejected_top10_post_ret20_mean": _mean_numeric(rejected_top10, "post_ret_20"),
        "bought_vs_rejected_gap": None
        if _mean_numeric(bought, "post_ret_20") is None or _mean_numeric(rejected_top10, "post_ret_20") is None
        else float(_mean_numeric(bought, "post_ret_20") - _mean_numeric(rejected_top10, "post_ret_20")),
        "candidate_top10_count": int(len(top10)),
        "bought_count": int(len(bought)),
        "rejected_top10_count": int(len(rejected_top10)),
    }


def _monthly_quality(run_dir: Path) -> pd.DataFrame:
    candidates = pd.read_csv(run_dir / "daily_candidate_snapshot.csv")
    outcomes = pd.read_csv(run_dir / "post_run_outcome_labels.csv")
    merged = candidates.merge(outcomes, on=["decision_ymd", "code"], how="left", suffixes=("", "_outcome"))
    merged["month"] = _month_key(merged["decision_ymd"])
    merged["candidate_rank_num"] = pd.to_numeric(merged["candidate_rank"], errors="coerce")
    merged["selected_bool"] = merged.get("selected_for_buy", pd.Series(dtype=bool)).fillna(False).astype(bool)
    rows: list[dict[str, Any]] = []
    for month, group in merged.groupby("month", sort=True):
        top10 = group[group["candidate_rank_num"] <= 10]
        bought = group[group["selected_bool"]]
        rejected_top10 = top10[~top10["selected_bool"]]
        rows.append(
            {
                "month": int(month),
                "candidate_top10_post_ret20_mean": _mean_numeric(top10, "post_ret_20"),
                "bought_post_ret20_mean": _mean_numeric(bought, "post_ret_20"),
                "rejected_top10_post_ret20_mean": _mean_numeric(rejected_top10, "post_ret_20"),
                "bought_vs_rejected_gap": None
                if _mean_numeric(bought, "post_ret_20") is None or _mean_numeric(rejected_top10, "post_ret_20") is None
                else float(_mean_numeric(bought, "post_ret_20") - _mean_numeric(rejected_top10, "post_ret_20")),
                "bought_count": int(len(bought)),
                "top10_count": int(len(top10)),
            }
        )
    return pd.DataFrame(rows)


def _run_monthly(run_dir: Path, year: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    equity = pd.read_csv(run_dir / "equity_curve.csv")
    equity["month"] = _month_key(equity["ymd"])
    rows: list[dict[str, Any]] = []
    drawdown_events: list[dict[str, Any]] = []
    benchmark_negative: list[dict[str, Any]] = []
    for month, group in equity.groupby("month", sort=True):
        group = group.sort_values("ymd", kind="stable")
        start_equity = float(group.iloc[0]["equity"])
        end_equity = float(group.iloc[-1]["equity"])
        port_ret = end_equity / start_equity - 1.0 if start_equity else 0.0
        bench_start = float(group.iloc[0]["market_benchmark_equity"]) if pd.notna(group.iloc[0].get("market_benchmark_equity")) else None
        bench_end = float(group.iloc[-1]["market_benchmark_equity"]) if pd.notna(group.iloc[-1].get("market_benchmark_equity")) else None
        bench_ret = 0.0 if not bench_start or bench_end is None else bench_end / bench_start - 1.0
        peak = pd.to_numeric(group["equity"], errors="coerce").cummax()
        dd = pd.to_numeric(group["equity"], errors="coerce") / peak - 1.0
        max_dd = float(dd.min())
        cash_idle = float((pd.to_numeric(group["cash"], errors="coerce") / pd.to_numeric(group["equity"], errors="coerce")).mean())
        exposure = float((pd.to_numeric(group["positions_market_value"], errors="coerce") / pd.to_numeric(group["equity"], errors="coerce")).mean())
        regime = _regime_label(port_ret, bench_ret, max_dd)
        row = {
            "year": year,
            "month": int(month),
            "portfolio_return": port_ret,
            "benchmark_return": bench_ret,
            "excess_return": port_ret - bench_ret,
            "month_max_drawdown": max_dd,
            "cash_idle_ratio": cash_idle,
            "gross_exposure_mean": exposure,
            "open_position_count_mean": _mean_numeric(group, "open_position_count"),
            "regime_bucket": regime,
        }
        rows.append(row)
        if max_dd <= -0.10:
            trough_idx = dd.idxmin()
            drawdown_events.append({**row, "event_type": "monthly_drawdown_lte_minus10", "trough_ymd": int(equity.loc[trough_idx, "ymd"])})
        if bench_ret > 0 and port_ret < 0:
            benchmark_negative.append({**row, "case_type": "benchmark_positive_portfolio_negative"})
    return pd.DataFrame(rows), pd.DataFrame(drawdown_events), pd.DataFrame(benchmark_negative)


def _entry_exit(run_dir: Path, year: int) -> dict[str, Any]:
    orders = pd.read_csv(run_dir / "orders_ledger.csv")
    filled = orders[orders.get("order_status", pd.Series(dtype=str)).astype(str) == "filled"].copy()
    exits = filled[filled["action"].astype(str).isin(["exit", "stop"])].copy()
    stops = filled[filled["action"].astype(str) == "stop"].copy()
    losses = exits[pd.to_numeric(exits.get("realized_pnl", pd.Series(dtype=float)), errors="coerce") < 0]
    return {
        "year": year,
        "order_count": int(len(orders)),
        "filled_order_count": int(len(filled)),
        "buy_count": int((filled["action"].astype(str) == "buy").sum()) if not filled.empty else 0,
        "stop_count": int(len(stops)),
        "exit_count": int((filled["action"].astype(str) == "exit").sum()) if not filled.empty else 0,
        "loss_exit_count": int(len(losses)),
        "realized_pnl_total": _mean_numeric(pd.DataFrame([{"x": pd.to_numeric(exits.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").sum()}]), "x"),
        "avg_realized_return": _mean_numeric(exits, "realized_return"),
    }


def _exposure_cash(run_dir: Path, year: int) -> dict[str, Any]:
    equity = pd.read_csv(run_dir / "equity_curve.csv")
    cash_ratio = pd.to_numeric(equity["cash"], errors="coerce") / pd.to_numeric(equity["equity"], errors="coerce")
    exposure = pd.to_numeric(equity["positions_market_value"], errors="coerce") / pd.to_numeric(equity["equity"], errors="coerce")
    return {
        "year": year,
        "cash_idle_ratio": float(cash_ratio.mean()),
        "cash_idle_ratio_min": float(cash_ratio.min()),
        "gross_exposure_mean": float(exposure.mean()),
        "gross_exposure_max": float(exposure.max()),
        "open_position_count_mean": _mean_numeric(equity, "open_position_count"),
    }


def _decide(yearly: pd.DataFrame, monthly: pd.DataFrame, quality: pd.DataFrame, exposure: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    severe_years = yearly[pd.to_numeric(yearly["max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist()
    bppn_years = yearly[(pd.to_numeric(yearly["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(yearly["portfolio_return"], errors="coerce") < 0)]["year"].astype(int).tolist()
    weak_candidate_years = quality[pd.to_numeric(quality["candidate_top10_post_ret20_mean"], errors="coerce") < 0]["year"].astype(int).tolist()
    selection_gap_years = quality[pd.to_numeric(quality["bought_vs_rejected_gap"], errors="coerce") < 0]["year"].astype(int).tolist()
    high_exposure_bad_months = monthly[(pd.to_numeric(monthly["portfolio_return"], errors="coerce") < -0.05) & (pd.to_numeric(monthly["gross_exposure_mean"], errors="coerce") > 0.75)]
    evidence = {
        "severe_drawdown_years": severe_years,
        "benchmark_positive_portfolio_negative_years": bppn_years,
        "candidate_top10_negative_years": weak_candidate_years,
        "bought_underperformed_rejected_years": selection_gap_years,
        "high_exposure_bad_month_count": int(len(high_exposure_bad_months)),
        "high_exposure_bad_months": high_exposure_bad_months["month"].astype(int).tolist() if not high_exposure_bad_months.empty else [],
    }
    if bppn_years and high_exposure_bad_months.empty and selection_gap_years:
        return "entry_confirmation_pretest", "benchmark_up_failure_with_selection_gap", evidence
    if high_exposure_bad_months.empty and weak_candidate_years:
        return "candidate_generation_redesign", "candidate_pool_negative_in_failure_years", evidence
    if high_exposure_bad_months.empty and bppn_years:
        return "regime_filter_pretest", "benchmark_up_failure_without_cash_control_signal", evidence
    if high_exposure_bad_months.empty:
        return "abandon_current_baseline_policy", "no_single_axis_failure_signal", evidence
    if len(severe_years) >= 1 and len(high_exposure_bad_months) >= 2:
        return "risk_off_cash_control_pretest", "severe_drawdown_with_high_exposure_bad_months", evidence
    return "position_sizing_haircut_pretest", "losses_present_but_cash_control_signal_weak", evidence


def run_decomposition(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    yearly_gate = pd.read_csv(robustness_root / "yearly_results.csv")
    yearly_rows: list[dict[str, Any]] = []
    quality_rows: list[dict[str, Any]] = []
    entry_exit_rows: list[dict[str, Any]] = []
    exposure_rows: list[dict[str, Any]] = []
    monthly_frames: list[pd.DataFrame] = []
    drawdown_frames: list[pd.DataFrame] = []
    benchmark_negative_frames: list[pd.DataFrame] = []

    for _idx, row in yearly_gate.iterrows():
        year = int(row["year"])
        run_dir = Path(str(row["run_dir"]))
        quality = _candidate_quality(run_dir, year)
        quality_rows.append(quality)
        entry_exit_rows.append(_entry_exit(run_dir, year))
        exposure_rows.append(_exposure_cash(run_dir, year))
        monthly, drawdowns, bppn = _run_monthly(run_dir, year)
        monthly_quality = _monthly_quality(run_dir)
        monthly = monthly.merge(monthly_quality, on="month", how="left")
        monthly_frames.append(monthly)
        if not drawdowns.empty:
            drawdown_frames.append(drawdowns)
        if not bppn.empty:
            benchmark_negative_frames.append(bppn)
        yearly_rows.append(
            {
                "year": year,
                "portfolio_return": row["total_return"],
                "benchmark_return": row["benchmark_return"],
                "excess_return": row["excess_return"],
                "max_drawdown": row["max_drawdown"],
                "primary_failure_mode": row["primary_failure_mode"],
                "candidate_top10_post_ret20_mean": quality["candidate_top10_post_ret20_mean"],
                "bought_post_ret20_mean": quality["bought_post_ret20_mean"],
                "rejected_top10_post_ret20_mean": quality["rejected_top10_post_ret20_mean"],
                "bought_vs_rejected_gap": quality["bought_vs_rejected_gap"],
                "cash_idle_ratio": exposure_rows[-1]["cash_idle_ratio"],
                "gross_exposure_mean": exposure_rows[-1]["gross_exposure_mean"],
                "stop_count": row["stop_count"],
                "exit_count": row["exit_count"],
                "order_count": row["order_count"],
            }
        )

    yearly_df = pd.DataFrame(yearly_rows)
    quality_df = pd.DataFrame(quality_rows)
    entry_exit_df = pd.DataFrame(entry_exit_rows)
    exposure_df = pd.DataFrame(exposure_rows)
    monthly_df = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    drawdown_df = pd.concat(drawdown_frames, ignore_index=True) if drawdown_frames else pd.DataFrame()
    bppn_df = pd.concat(benchmark_negative_frames, ignore_index=True) if benchmark_negative_frames else pd.DataFrame()
    regime_df = (
        monthly_df.groupby("regime_bucket", dropna=False)
        .agg(
            month_count=("month", "count"),
            portfolio_return_mean=("portfolio_return", "mean"),
            benchmark_return_mean=("benchmark_return", "mean"),
            excess_return_mean=("excess_return", "mean"),
            max_drawdown_min=("month_max_drawdown", "min"),
            cash_idle_ratio_mean=("cash_idle_ratio", "mean"),
            gross_exposure_mean=("gross_exposure_mean", "mean"),
            bought_vs_rejected_gap_mean=("bought_vs_rejected_gap", "mean"),
        )
        .reset_index()
    )
    bought_vs_rejected = quality_df[["year", "bought_post_ret20_mean", "rejected_top10_post_ret20_mean", "bought_vs_rejected_gap", "bought_count", "rejected_top10_count"]].copy()
    decision, reason, evidence = _decide(yearly_df, monthly_df, quality_df, exposure_df)

    _write_csv(output_root / "yearly_failure_decomposition.csv", yearly_df)
    _write_csv(output_root / "monthly_failure_decomposition.csv", monthly_df)
    _write_csv(output_root / "regime_bucket_summary.csv", regime_df)
    _write_csv(output_root / "candidate_quality_by_year.csv", quality_df)
    _write_csv(output_root / "bought_vs_rejected_by_year.csv", bought_vs_rejected)
    _write_csv(output_root / "entry_exit_failure_by_year.csv", entry_exit_df)
    _write_csv(output_root / "exposure_cash_by_year.csv", exposure_df)
    _write_csv(output_root / "drawdown_event_cases.csv", drawdown_df)
    _write_csv(output_root / "benchmark_positive_portfolio_negative_cases.csv", bppn_df)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "decision": decision,
        "reason_type": reason,
        "evidence": evidence,
        "scope": {
            "tradex_only": True,
            "policy_changed": False,
            "optimization": False,
            "threshold_sweep": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "ranking_changed": False,
            "publish_registry_changed": False,
        },
    }
    _write_json(output_root / "baseline_regime_failure_summary.json", summary)
    _write_json(
        output_root / "next_axis_decision.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "decision_candidates": list(NEXT_AXIS_CANDIDATES),
            "selected_next_axis": decision,
            "decision_count": 1,
            "reason_type": reason,
            "evidence": evidence,
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
        "policy_changed": False,
        "optimization": False,
        "threshold_sweep": False,
        "silent_fallback_used": False,
        "meemee_reflectable": False,
        "policy_promotion_allowed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "selected_next_axis": decision, "reason_type": reason, "evidence": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompose baseline multi-year regime failures.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_decomposition(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
