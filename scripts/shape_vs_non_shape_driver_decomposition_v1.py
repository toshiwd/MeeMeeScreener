from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import partial_stop_at_minus8_pretest_v1 as base
from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery


AXIS_ID = "shape_vs_non_shape_driver_decomposition_v1"
SCHEMA_PREFIX = "tradex_shape_vs_non_shape_driver_decomposition_v1"
DEFAULT_OUTPUT_DIR_NAME = "shape_vs_non_shape_driver_decomposition_v1"
BENCHMARK_CODE = "1306"

REQUIRED_ARTIFACTS = (
    "shape_vs_non_shape_driver_summary.json",
    "driver_yearly_performance.csv",
    "driver_regime_performance.csv",
    "chart_family_vs_driver_matrix.csv",
    "benchmark_positive_negative_driver_cases.csv",
    "shape_works_cases.csv",
    "shape_fails_cases.csv",
    "non_shape_driver_candidates.csv",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "relative_strength_filter_pretest",
    "sector_flow_filter_pretest",
    "breadth_regime_filter_pretest",
    "volatility_whipsaw_guard_pretest",
    "volume_quality_filter_pretest",
    "chart_context_feature_contract_v1_1",
    "candidate_generation_redesign_v1",
)

FORBIDDEN_DRIVER_COLUMNS = (
    "post_ret_5",
    "post_ret_10",
    "post_ret_20",
    "post_ret_40",
    "mae_20",
    "mfe_20",
    "outcome_bucket",
    "future_return",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    return base._json_ready(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    data.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_source_db(robustness_root: Path) -> Path:
    yearly = pd.read_csv(robustness_root / "yearly_results.csv")
    run_config = Path(str(yearly.iloc[0]["run_dir"])) / "run_config.json"
    if run_config.exists():
        path = Path(str(_read_json(run_config)["source_db"])).expanduser().resolve()
    else:
        path = discovery.DEFAULT_SOURCE_DB.resolve()
    if not path.exists():
        raise FileNotFoundError(f"source DB not found: {path}")
    return path


def _load_family_map(chart_family_root: Path) -> pd.DataFrame:
    frame = pd.read_csv(chart_family_root / "chart_context_candidate_family_map.csv")
    frame["code"] = frame["code"].astype(str)
    frame["decision_ymd"] = frame["decision_ymd"].astype(int)
    frame["year"] = frame["year"].astype(int)
    if "selected_for_buy_bool" not in frame.columns:
        frame["selected_for_buy_bool"] = False
    frame["selected_for_buy_bool"] = frame["selected_for_buy_bool"].astype(str).str.lower().isin(["true", "1", "yes"])
    frame["month"] = frame["decision_ymd"] // 100
    return frame


def _load_sector_map(source_db: Path) -> pd.DataFrame:
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("show tables").fetchall()}
        if "industry_master" not in tables:
            return pd.DataFrame(columns=["code", "sector33_name", "sector_source"])
        sector = conn.execute("SELECT code, sector33_name FROM industry_master").fetchdf()
    finally:
        conn.close()
    sector["code"] = sector["code"].astype(str)
    sector["sector33_name"] = sector["sector33_name"].fillna("unknown_sector").astype(str)
    sector["sector_source"] = "industry_master"
    return sector


def _load_daily_returns(source_db: Path, codes: set[str], *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    load_codes = set(codes) | {BENCHMARK_CODE}
    frame = base._load_daily_ohlc(source_db, codes=load_codes, start_ymd=start_ymd, end_ymd=end_ymd)
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = frame["ymd"].astype(int)
    grouped = frame.sort_values(["code", "ymd"], kind="stable").groupby("code", sort=False)
    frame["ret20_signal"] = grouped["c"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ret60_signal"] = grouped["c"].transform(lambda s: s / s.shift(60) - 1.0)
    frame["up_day_flag"] = frame["c"] > frame["o"]
    return frame


def _bucket(value: Any, *, low: float = -0.03, high: float = 0.03, prefix: str) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return f"{prefix}_unknown"
    if not math.isfinite(parsed):
        return f"{prefix}_unknown"
    if parsed <= low:
        return f"{prefix}_weak"
    if parsed >= high:
        return f"{prefix}_strong"
    return f"{prefix}_neutral"


def _driver_rows(mapped: pd.DataFrame, source_db: Path) -> pd.DataFrame:
    start_ymd = int(max(20000101, mapped["decision_ymd"].min() - 20000))
    end_ymd = int(mapped["decision_ymd"].max())
    daily = _load_daily_returns(source_db, set(mapped["code"].astype(str)), start_ymd=start_ymd, end_ymd=end_ymd)
    benchmark = daily[daily["code"] == BENCHMARK_CODE][["ymd", "ret20_signal", "ret60_signal"]].rename(columns={"ret20_signal": "benchmark_20d_return", "ret60_signal": "benchmark_60d_return"})
    stock_ret = daily[daily["code"] != BENCHMARK_CODE][["code", "ymd", "ret20_signal", "ret60_signal"]].rename(columns={"ymd": "decision_ymd", "ret20_signal": "stock_20d_return", "ret60_signal": "stock_60d_return"})
    frame = mapped.merge(stock_ret, on=["code", "decision_ymd"], how="left")
    frame = frame.merge(benchmark.rename(columns={"ymd": "decision_ymd"}), on="decision_ymd", how="left")
    frame["relative_strength_20d"] = pd.to_numeric(frame["stock_20d_return"], errors="coerce") - pd.to_numeric(frame["benchmark_20d_return"], errors="coerce")
    frame["relative_strength_60d"] = pd.to_numeric(frame["stock_60d_return"], errors="coerce") - pd.to_numeric(frame["benchmark_60d_return"], errors="coerce")
    frame["relative_strength_driver"] = frame["relative_strength_20d"].map(lambda v: _bucket(v, low=-0.03, high=0.03, prefix="rs20"))

    sector = _load_sector_map(source_db)
    frame = frame.merge(sector, on="code", how="left")
    frame["sector33_name"] = frame["sector33_name"].fillna("unknown_sector")
    sector_flow = frame.groupby(["decision_ymd", "sector33_name"], dropna=False, as_index=False)["stock_20d_return"].mean().rename(columns={"stock_20d_return": "sector_20d_return_candidate_universe"})
    frame = frame.merge(sector_flow, on=["decision_ymd", "sector33_name"], how="left")
    frame["sector_relative_rank_pct"] = frame.groupby(["decision_ymd", "sector33_name"])["stock_20d_return"].rank(pct=True, ascending=True)
    frame["sector_flow_driver"] = frame["sector_20d_return_candidate_universe"].map(lambda v: _bucket(v, low=-0.02, high=0.02, prefix="sector20"))

    breadth = frame.groupby("decision_ymd", as_index=False).agg(
        breadth_up_ratio=("stock_20d_return", lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean())),
        candidate_count=("code", "count"),
        score_std=("selection_score", "std"),
    )
    frame = frame.merge(breadth, on="decision_ymd", how="left")
    frame["breadth_driver"] = frame["breadth_up_ratio"].map(lambda v: "breadth_broad_up" if pd.notna(v) and v >= 0.55 else "breadth_narrow_or_down" if pd.notna(v) and v <= 0.45 else "breadth_mixed")

    frame["whipsaw_rate_proxy"] = frame.groupby("decision_ymd")["failed_breakout_flag"].transform(lambda s: s.astype(str).str.lower().isin(["true", "1"]).mean()) if "failed_breakout_flag" in frame.columns else 0.0
    frame["gap_rate_proxy"] = frame.groupby("decision_ymd")["gap_up_flag"].transform(lambda s: s.astype(str).str.lower().isin(["true", "1"]).mean()) if "gap_up_flag" in frame.columns else 0.0
    frame["volatility_whipsaw_driver"] = frame["whipsaw_rate_proxy"].map(lambda v: "whipsaw_high" if pd.notna(v) and v >= 0.10 else "whipsaw_normal")
    vol = pd.to_numeric(frame.get("volume_compression_ratio", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    frame["volume_quality_driver"] = "volume_quality_normal"
    frame.loc[vol <= 0.75, "volume_quality_driver"] = "volume_dry"
    frame.loc[vol >= 1.25, "volume_quality_driver"] = "volume_expansion"
    frame["chart_shape_driver"] = frame["chart_context_family"].fillna("unknown_or_mixed")
    return frame


def _metric_rows(frame: pd.DataFrame, group_cols: list[str], driver_name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in frame.groupby(group_cols, dropna=False, sort=True):
        keys_tuple = keys if isinstance(keys, tuple) else (keys,)
        post = pd.to_numeric(group.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        mae = pd.to_numeric(group.get("mae_20", pd.Series(dtype=float)), errors="coerce")
        mfe = pd.to_numeric(group.get("mfe_20", pd.Series(dtype=float)), errors="coerce")
        bought = group[group["selected_for_buy_bool"].fillna(False).astype(bool)]
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        row = {name: value for name, value in zip(group_cols, keys_tuple, strict=False)}
        row.update(
            {
                "driver_name": driver_name,
                "candidate_count": int(len(group)),
                "bought_count": int(len(bought)),
                "post_ret20_mean": float(post.mean()) if not post.dropna().empty else None,
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "severe_loser_rate": float(((post <= -0.10) | (mae <= -0.10)).mean()) if len(group) else None,
                "big_winner_rate": float(((post >= 0.10) | (mfe >= 0.15)).mean()) if len(group) else None,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _driver_yearly(frame: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for driver in ["chart_shape_driver", "relative_strength_driver", "sector_flow_driver", "breadth_driver", "volatility_whipsaw_driver", "volume_quality_driver"]:
        parts.append(_metric_rows(frame, ["year", driver], driver))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _driver_regime(frame: pd.DataFrame, robustness_root: Path) -> pd.DataFrame:
    path = robustness_root / "baseline_regime_failure_decomposition_v1" / "monthly_failure_decomposition.csv"
    work = frame.copy()
    if path.exists():
        regimes = pd.read_csv(path)
        regimes["year"] = regimes["year"].astype(int)
        regimes["month"] = regimes["month"].astype(int)
        work = work.merge(regimes[["year", "month", "regime_bucket"]], on=["year", "month"], how="left")
    work["regime_bucket"] = work.get("regime_bucket", pd.Series(index=work.index, dtype="object")).fillna("unknown")
    parts = []
    for driver in ["chart_shape_driver", "relative_strength_driver", "sector_flow_driver", "breadth_driver", "volatility_whipsaw_driver", "volume_quality_driver"]:
        parts.append(_metric_rows(work, ["regime_bucket", driver], driver))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _family_driver_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for family, group in frame.groupby("chart_context_family", sort=True):
        for driver in ["relative_strength_driver", "sector_flow_driver", "breadth_driver", "volatility_whipsaw_driver", "volume_quality_driver"]:
            for bucket, subset in group.groupby(driver, dropna=False, sort=True):
                post = pd.to_numeric(subset.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
                bought = subset[subset["selected_for_buy_bool"].fillna(False).astype(bool)]
                bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
                rows.append({"chart_context_family": family, "driver_name": driver, "driver_bucket": bucket, "candidate_count": int(len(subset)), "bought_count": int(len(bought)), "post_ret20_mean": float(post.mean()) if not post.dropna().empty else None, "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None})
    return pd.DataFrame(rows)


def _shape_cases(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    bought = frame[frame["selected_for_buy_bool"].fillna(False).astype(bool)].copy()
    post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
    mae = pd.to_numeric(bought.get("mae_20", pd.Series(dtype=float)), errors="coerce")
    mfe = pd.to_numeric(bought.get("mfe_20", pd.Series(dtype=float)), errors="coerce")
    keep_cols = ["year", "decision_ymd", "code", "chart_context_family", "relative_strength_driver", "sector_flow_driver", "breadth_driver", "volatility_whipsaw_driver", "volume_quality_driver", "post_ret_20", "mae_20", "mfe_20", "stock_20d_return", "benchmark_20d_return", "relative_strength_20d", "sector33_name", "sector_20d_return_candidate_universe", "breadth_up_ratio"]
    works = bought[(post >= 0.08) | (mfe >= 0.12)].copy()
    fails = bought[(post <= -0.08) | (mae <= -0.10)].copy()
    return works[[c for c in keep_cols if c in works.columns]], fails[[c for c in keep_cols if c in fails.columns]]


def _benchmark_positive_negative_cases(frame: pd.DataFrame, robustness_root: Path) -> pd.DataFrame:
    yearly = pd.read_csv(robustness_root / "yearly_results.csv")
    bad_years = yearly[(pd.to_numeric(yearly["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(yearly["total_return"], errors="coerce") < 0)]["year"].astype(int).tolist()
    cases = frame[(frame["year"].isin(bad_years)) & (frame["selected_for_buy_bool"].fillna(False).astype(bool))].copy()
    return cases[["year", "decision_ymd", "code", "chart_context_family", "relative_strength_driver", "sector_flow_driver", "breadth_driver", "volatility_whipsaw_driver", "volume_quality_driver", "post_ret_20", "mae_20", "mfe_20", "relative_strength_20d", "sector33_name", "breadth_up_ratio"]]


def _driver_candidates(driver_yearly: pd.DataFrame, matrix: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for driver in ["relative_strength_driver", "sector_flow_driver", "breadth_driver", "volatility_whipsaw_driver", "volume_quality_driver"]:
        subset = driver_yearly[driver_yearly["driver_name"] == driver].copy()
        if subset.empty or driver not in subset.columns:
            continue
        bucket_stats = subset.groupby(driver).agg(
            bought_count=("bought_count", "sum"),
            avg_bought_post_ret20=("bought_post_ret20_mean", "mean"),
            avg_severe_loser_rate=("severe_loser_rate", "mean"),
            year_count=("year", "nunique"),
        ).reset_index().rename(columns={driver: "driver_bucket"})
        if bucket_stats.empty:
            continue
        spread = pd.to_numeric(bucket_stats["avg_bought_post_ret20"], errors="coerce").max() - pd.to_numeric(bucket_stats["avg_bought_post_ret20"], errors="coerce").min()
        best = bucket_stats.sort_values("avg_bought_post_ret20", ascending=False).head(1).iloc[0].to_dict()
        worst = bucket_stats.sort_values("avg_bought_post_ret20", ascending=True).head(1).iloc[0].to_dict()
        rows.append({"driver_name": driver, "driver_spread_bought_post_ret20": float(spread) if pd.notna(spread) else None, "best_bucket": best.get("driver_bucket"), "worst_bucket": worst.get("driver_bucket"), "best_bucket_bought_count": int(best.get("bought_count") or 0), "worst_bucket_bought_count": int(worst.get("bought_count") or 0), "axis_candidate": f"{driver}_pretest"})
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["sample_ok"] = (out["best_bucket_bought_count"] >= 30) & (out["worst_bucket_bought_count"] >= 30)
    return out.sort_values(["sample_ok", "driver_spread_bought_post_ret20"], ascending=[False, False], kind="stable")


def _choose_next_axis(candidates: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    if candidates.empty:
        return "candidate_generation_redesign_v1", "no_non_shape_driver_candidate_detected", {"driver_candidates": []}
    usable = candidates[candidates["sample_ok"].fillna(False).astype(bool)].copy()
    chosen = (usable if not usable.empty else candidates).iloc[0].to_dict()
    driver = str(chosen["driver_name"])
    mapping = {
        "relative_strength_driver": "relative_strength_filter_pretest",
        "sector_flow_driver": "sector_flow_filter_pretest",
        "breadth_driver": "breadth_regime_filter_pretest",
        "volatility_whipsaw_driver": "volatility_whipsaw_guard_pretest",
        "volume_quality_driver": "volume_quality_filter_pretest",
    }
    decision = mapping.get(driver, "candidate_generation_redesign_v1")
    return decision, f"{driver}_has_largest_observed_signal_date_driver_separation", {"selected_driver": chosen, "driver_candidates": candidates.to_dict(orient="records")}


def run_decomposition(
    robustness_root: str | Path,
    chart_feature_root: str | Path | None = None,
    chart_family_root: str | Path | None = None,
    regime_filter_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    chart_feature_root = Path(chart_feature_root) if chart_feature_root else robustness_root / "chart_context_feature_contract_v1"
    chart_family_root = Path(chart_family_root) if chart_family_root else robustness_root / "chart_context_candidate_family_map_v1"
    regime_filter_root = Path(regime_filter_root) if regime_filter_root else robustness_root / "regime_aware_family_filter_pretest_v1"
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    source_db = _load_source_db(robustness_root)
    mapped = _load_family_map(chart_family_root)
    driver_frame = _driver_rows(mapped, source_db)
    yearly = _driver_yearly(driver_frame)
    regime = _driver_regime(driver_frame, robustness_root)
    matrix = _family_driver_matrix(driver_frame)
    works, fails = _shape_cases(driver_frame)
    bpneg = _benchmark_positive_negative_cases(driver_frame, robustness_root)
    driver_candidates = _driver_candidates(yearly, matrix)
    decision, reason, evidence = _choose_next_axis(driver_candidates)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "driver_yearly_performance.csv", yearly)
    _write_csv(output_root / "driver_regime_performance.csv", regime)
    _write_csv(output_root / "chart_family_vs_driver_matrix.csv", matrix)
    _write_csv(output_root / "benchmark_positive_negative_driver_cases.csv", bpneg)
    _write_csv(output_root / "shape_works_cases.csv", works)
    _write_csv(output_root / "shape_fails_cases.csv", fails)
    _write_csv(output_root / "non_shape_driver_candidates.csv", driver_candidates)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "chart_feature_root": str(chart_feature_root),
        "chart_family_root": str(chart_family_root),
        "regime_filter_root": str(regime_filter_root),
        "source_db": str(source_db),
        "decision": decision,
        "reason_type": reason,
        "metrics": evidence,
        "driver_scope": {
            "sector_flow_driver_scope": "candidate_codes_industry_master_sector33_mean_return_proxy",
            "breadth_driver_scope": "candidate_snapshot_universe_breadth_proxy",
            "relative_strength_driver_scope": "stock_vs_1306_signal_date_20d_60d_returns",
            "volatility_whipsaw_driver_scope": "candidate_snapshot_failed_breakout_gap_proxy",
            "volume_quality_driver_scope": "chart_context_volume_compression_proxy",
        },
        "scope": {"tradex_only": True, "replay_rerun": False, "policy_change": False, "candidate_generation_change": False, "ranking_change": False, "optimization": False, "threshold_sweep": False, "meemee_ui_changed": False, "runtime_db_written": False, "publish_registry_changed": False},
        "no_lookahead": {"post_run_outcomes_used_for_driver_construction": False, "post_run_outcomes_used_for_diagnostic_labels": True, "future_benchmark_return_used": False},
    }
    _write_json(output_root / "shape_vs_non_shape_driver_summary.json", summary)
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "replay_rerun": False, "policy_change": False, "candidate_generation_change": False, "ranking_change": False, "optimization": False, "threshold_sweep": False, "silent_fallback_used": False, "policy_promotion_allowed": False, "meemee_reflectable": False})
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompose chart shape vs non-shape signal-date drivers.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--chart-feature-root", type=Path, default=None)
    parser.add_argument("--chart-family-root", type=Path, default=None)
    parser.add_argument("--regime-filter-root", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_decomposition(args.robustness_root, args.chart_feature_root, args.chart_family_root, args.regime_filter_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
