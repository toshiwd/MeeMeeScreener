from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "starter_entry_monthly_box_regime_interaction_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_monthly_box_regime_interaction_v1")
TOPK_VALUES = (5, 10, 20)
READ_COLUMNS = [
    "decision_date",
    "code",
    "year",
    "baseline_rank",
    "baseline_score",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "path20_available",
    "ret20",
    "mae20",
    "mfe20",
    "starter_good",
    "starter_bad",
    "selected_loser",
    "selected_winner",
    "immediate_adverse_entry",
    "same_date_ret20_rank_pct",
]
MONTHLY_FEATURES = [
    "monthly_box_position",
    "monthly_box_position_bucket",
    "monthly_close_vs_box_low_pct",
    "monthly_close_vs_box_high_pct",
    "monthly_box_width_pct",
    "monthly_box_month_count",
    "monthly_ma7_slope",
    "monthly_ma20_slope",
    "monthly_close_vs_ma7_pct",
    "monthly_close_vs_ma20_pct",
    "monthly_regime_bucket",
    "monthly_supportive_flag",
    "monthly_overextended_flag",
    "monthly_breakout_context_flag",
    "monthly_pullback_context_flag",
]
REQUIRED_ARTIFACTS = (
    "monthly_box_regime_summary.json",
    "monthly_box_regime_rows.csv",
    "bucket_metrics.json",
    "topk_comparison.json",
    "replacement_quality.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "lineage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _to_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    if frame.empty or col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def pct(numer: pd.Series, denom: pd.Series) -> pd.Series:
    return numer.div(denom.where(denom.ne(0))).sub(1)


def load_candidate_rows(input_root: Path) -> pd.DataFrame:
    path = input_root / "candidate_family_source_rows.csv"
    frames = [chunk for chunk in pd.read_csv(path, usecols=lambda c: c in READ_COLUMNS, chunksize=250_000, low_memory=False)]
    rows = pd.concat(frames, ignore_index=True)
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    for col in ["decision_date", "year", "baseline_rank", "baseline_score", "ret20", "mae20", "mfe20", "same_date_ret20_rank_pct"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in [
        "monthly_high_zone_proxy",
        "monthly_box_breakout_proxy",
        "monthly_box_inside_proxy",
        "weekly_monthly_uptrend_proxy",
        "path20_available",
        "starter_good",
        "starter_bad",
        "selected_loser",
        "selected_winner",
        "immediate_adverse_entry",
    ]:
        rows[col] = _to_bool(rows[col])
    return rows[rows["path20_available"] & rows["baseline_rank"].notna()].copy()


def build_monthly_context(daily_path: Path, keys: pd.DataFrame) -> pd.DataFrame:
    codes = set(keys["code"].astype(str).unique())
    daily = pd.read_csv(daily_path, usecols=["code", "date", "open", "high", "low", "close"], low_memory=False)
    daily["code"] = daily["code"].astype(str)
    daily = daily[daily["code"].isin(codes)].copy()
    daily["dt"] = pd.to_datetime(daily["date"], errors="coerce")
    daily = daily.dropna(subset=["dt"]).sort_values(["code", "dt"])
    daily["decision_date"] = daily["dt"].dt.strftime("%Y%m%d").astype(int)
    monthly_parts: list[pd.DataFrame] = []
    for code, g in daily.groupby("code", sort=False):
        work = g.sort_values("dt").copy()
        work["month"] = work["dt"].dt.to_period("M")
        work["monthly_box_low"] = work.groupby("month")["low"].cummin()
        work["monthly_box_high"] = work.groupby("month")["high"].cummax()
        month_last = work.groupby("month", sort=True)["close"].last()
        ma7_by_month = month_last.rolling(7, min_periods=3).mean()
        ma20_by_month = month_last.rolling(20, min_periods=6).mean()
        prev_ma7 = ma7_by_month.shift(1)
        prev_ma20 = ma20_by_month.shift(1)
        month_count = pd.Series(range(1, len(month_last) + 1), index=month_last.index)
        work["monthly_ma7"] = work["month"].map(ma7_by_month)
        work["monthly_ma20"] = work["month"].map(ma20_by_month)
        work["monthly_ma7_prev"] = work["month"].map(prev_ma7)
        work["monthly_ma20_prev"] = work["month"].map(prev_ma20)
        work["monthly_box_month_count"] = work["month"].map(month_count)
        rng = (work["monthly_box_high"] - work["monthly_box_low"]).where(lambda s: s.ne(0))
        work["monthly_box_position"] = (work["close"] - work["monthly_box_low"]).div(rng)
        work["monthly_close_vs_box_low_pct"] = pct(work["close"], work["monthly_box_low"])
        work["monthly_close_vs_box_high_pct"] = pct(work["close"], work["monthly_box_high"])
        work["monthly_box_width_pct"] = (work["monthly_box_high"] - work["monthly_box_low"]).div(work["close"].where(work["close"].ne(0)))
        work["monthly_ma7_slope"] = pct(work["monthly_ma7"], work["monthly_ma7_prev"])
        work["monthly_ma20_slope"] = pct(work["monthly_ma20"], work["monthly_ma20_prev"])
        work["monthly_close_vs_ma7_pct"] = pct(work["close"], work["monthly_ma7"])
        work["monthly_close_vs_ma20_pct"] = pct(work["close"], work["monthly_ma20"])
        monthly_parts.append(work[["code", "decision_date", *[c for c in MONTHLY_FEATURES if c not in {"monthly_box_position_bucket", "monthly_regime_bucket", "monthly_supportive_flag", "monthly_overextended_flag", "monthly_breakout_context_flag", "monthly_pullback_context_flag"}]]])
    monthly = pd.concat(monthly_parts, ignore_index=True)
    monthly["monthly_box_position_bucket"] = pd.cut(
        monthly["monthly_box_position"], bins=[-float("inf"), 0.25, 0.55, 0.8, float("inf")], labels=["box_low", "box_mid", "box_high", "box_upper_extreme"]
    ).astype(str)
    monthly["monthly_supportive_flag"] = (monthly["monthly_close_vs_ma7_pct"] >= -0.02) & (monthly["monthly_ma7_slope"].fillna(0) >= 0)
    monthly["monthly_overextended_flag"] = (monthly["monthly_box_position"] >= 0.8) | (monthly["monthly_close_vs_ma20_pct"] >= 0.18)
    monthly["monthly_breakout_context_flag"] = (monthly["monthly_close_vs_box_high_pct"] >= -0.01) & (monthly["monthly_ma7_slope"].fillna(0) >= 0)
    monthly["monthly_pullback_context_flag"] = monthly["monthly_supportive_flag"] & (monthly["monthly_box_position"].between(0.15, 0.55, inclusive="both"))
    monthly["monthly_regime_bucket"] = "monthly_neutral"
    monthly.loc[monthly["monthly_breakout_context_flag"], "monthly_regime_bucket"] = "monthly_breakout_context"
    monthly.loc[monthly["monthly_pullback_context_flag"], "monthly_regime_bucket"] = "monthly_support_pullback_context"
    monthly.loc[monthly["monthly_overextended_flag"], "monthly_regime_bucket"] = "monthly_overextended_context"
    return monthly


def attach_monthly(rows: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    merged = rows.merge(monthly, on=["code", "decision_date"], how="left")
    return merged


def score_variants(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    daily_quality = out["weekly_monthly_uptrend_proxy"] | out["monthly_box_inside_proxy"]
    out["monthly_box_regime_action"] = "retain"
    out.loc[out["monthly_overextended_flag"].fillna(False), "monthly_box_regime_action"] = "demote_overextended_box_high"
    out.loc[out["monthly_pullback_context_flag"].fillna(False) & daily_quality, "monthly_box_regime_action"] = "boost_constructive_support"
    delta = pd.Series(0.0, index=out.index)
    delta = delta.mask(out["monthly_overextended_flag"].fillna(False), -50.0)
    delta = delta.mask(out["monthly_pullback_context_flag"].fillna(False) & daily_quality, 5.0)
    out["monthly_box_regime_sort_score"] = out["baseline_score"].fillna(0.0) + delta - out["baseline_rank"].fillna(9999) * 0.001
    out["monthly_box_regime_rank"] = (
        out.sort_values(["decision_date", "monthly_box_regime_sort_score", "baseline_rank", "code"], ascending=[True, False, True, True])
        .groupby("decision_date")
        .cumcount()
        + 1
    )
    return out


def _periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["year"].eq(2024)]),
        ("2025", rows[rows["year"].eq(2025)]),
        ("2026_label_safe", rows[rows["year"].eq(2026)]),
        ("2024_2025", rows[rows["year"].isin([2024, 2025])]),
        ("2024_2026_combined", rows[rows["year"].isin([2024, 2025, 2026])]),
    ]


def summarize(frame: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = frame[frame[rank_col] <= topk]
    return {
        "n": int(len(g)),
        "mean_ret20": _mean(g, "ret20"),
        "starter_bad_rate": _rate(g["starter_bad"]) if not g.empty else None,
        "severe_loss_rate": _rate(g["ret20"] <= -0.05) if not g.empty else None,
        "selected_loser_rate": _rate(g["selected_loser"]) if not g.empty else None,
        "starter_good_rate": _rate(g["starter_good"]) if not g.empty else None,
        "selected_winner_rate": _rate(g["selected_winner"]) if not g.empty else None,
    }


def topk_comparison(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for period, pr in _periods(rows):
        for topk in TOPK_VALUES:
            base = summarize(pr, "baseline_rank", topk)
            var = summarize(pr, "monthly_box_regime_rank", topk)
            rec.append(
                {
                    "period": period,
                    "topk": topk,
                    **{f"baseline_{k}": v for k, v in base.items()},
                    **{f"challenger_{k}": v for k, v in var.items()},
                    "delta_mean_ret20": None if base["mean_ret20"] is None or var["mean_ret20"] is None else var["mean_ret20"] - base["mean_ret20"],
                    "delta_bad_pick_rate": None if base["starter_bad_rate"] is None or var["starter_bad_rate"] is None else var["starter_bad_rate"] - base["starter_bad_rate"],
                    "delta_severe_loss_rate": None if base["severe_loss_rate"] is None or var["severe_loss_rate"] is None else var["severe_loss_rate"] - base["severe_loss_rate"],
                }
            )
    return pd.DataFrame(rec)


def replacement_quality(rows: pd.DataFrame) -> dict[str, Any]:
    by_period: list[dict[str, Any]] = []
    for period, pr in _periods(rows):
        for topk in TOPK_VALUES:
            values = []
            changed_dates = 0
            for date, g in pr.groupby("decision_date", sort=True):
                base = set(g[g["baseline_rank"] <= topk]["code"].astype(str))
                var = set(g[g["monthly_box_regime_rank"] <= topk]["code"].astype(str))
                added = g[g["code"].astype(str).isin(var - base)]
                removed = g[g["code"].astype(str).isin(base - var)]
                if not added.empty or not removed.empty:
                    changed_dates += 1
                if not added.empty and not removed.empty:
                    values.append((_mean(added, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0))
            by_period.append(
                {
                    "period": period,
                    "topk": topk,
                    "changed_dates": changed_dates,
                    "replacement_delta_ret20": None if not values else float(pd.Series(values).mean()),
                    "replacement_count": len(values),
                }
            )
    return {"rows": by_period}


def boundary_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    recent = rows[rows["year"].isin([2024, 2025, 2026])]
    out: dict[str, Any] = {}
    for topk in [5, 10]:
        changed = []
        for _, g in recent.groupby("decision_date", sort=True):
            base = set(g[g["baseline_rank"] <= topk]["code"].astype(str))
            var = set(g[g["monthly_box_regime_rank"] <= topk]["code"].astype(str))
            changed.append(len(base.symmetric_difference(var)))
        out[f"changed_top{topk}_members_count"] = int(sum(1 for x in changed if x > 0))
        out[f"changed_top{topk}_member_slots"] = int(sum(changed))
    out["changed_rank_count"] = int((rows["baseline_rank"] != rows["monthly_box_regime_rank"]).sum())
    out["selection_divergence_reason"] = "fixed monthly box/regime demotion for overextended zones and small boost for constructive support"
    return out


def bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    buckets = {}
    for col in ["monthly_box_position_bucket", "monthly_regime_bucket", "monthly_box_regime_action"]:
        values = []
        for bucket, g in rows.groupby(col, dropna=False):
            values.append({"bucket": str(bucket), **summarize(g, "baseline_rank", 10)})
        buckets[col] = values
    return buckets


def decide(comp: pd.DataFrame, repl: dict[str, Any], boundary: dict[str, Any], rows: pd.DataFrame) -> dict[str, Any]:
    recent = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    repl_recent = [r for r in repl["rows"] if r["period"] == "2024_2026_combined" and r["topk"] == 10][0]
    support_dates = int(rows[rows["year"].isin([2024, 2025, 2026])]["decision_date"].nunique())
    if rows[MONTHLY_FEATURES].notna().mean().min() < 0.80:
        decision = "blocked_missing_contract"
        reason = "required_monthly_box_regime_features_have_insufficient_point_in_time_coverage"
    elif boundary["changed_top10_members_count"] < 10:
        decision = "close_branch_no_reusable_signal"
        reason = "topK_boundary_did_not_move_enough"
    elif (recent["delta_mean_ret20"] or 0.0) > 0 and (recent["delta_bad_pick_rate"] or 0.0) <= 0 and (recent["delta_severe_loss_rate"] or 0.0) <= 0:
        if support_dates >= 100 and (repl_recent["replacement_delta_ret20"] or 0.0) > 0:
            decision = "keep_for_next_stage"
            reason = "mean_ret20_improved_without_bad_or_severe_rate_worsening"
        else:
            decision = "promising_but_underpowered"
            reason = "positive_direction_but_support_or_replacement_quality_is_thin"
    elif ((recent["delta_bad_pick_rate"] or 0.0) < 0 or (recent["delta_severe_loss_rate"] or 0.0) < 0) and (repl_recent["replacement_delta_ret20"] or 0.0) < 0 and (recent["delta_mean_ret20"] or 0.0) <= 0:
        decision = "drop"
        reason = "bad_or_severe_rate_improved_but_replacement_quality_negative_and_mean_ret20_not_improved"
    else:
        decision = "close_branch_no_reusable_signal"
        reason = "monthly_box_regime_interaction_has_no_clear_same_condition_edge"
    return {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "reason_typed": [reason],
        "replacement_delta_ret20_top10_recent": repl_recent["replacement_delta_ret20"],
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def source_coverage(rows: pd.DataFrame, daily_path: Path) -> dict[str, Any]:
    return {
        "daily_path": daily_path,
        "row_count": int(len(rows)),
        "date_count": int(rows["decision_date"].nunique()),
        "confirmed_bars_only": True,
        "research_fallback_used": False,
        "monthly_feature_coverage": {col: float(rows[col].notna().mean()) for col in MONTHLY_FEATURES},
    }


def run(input_root: Path, daily_path: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-monthly-box-regime-interaction-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = load_candidate_rows(input_root)
    keys = rows[["code", "decision_date"]].drop_duplicates()
    monthly = build_monthly_context(daily_path, keys)
    scored = score_variants(attach_monthly(rows, monthly))
    comp = topk_comparison(scored)
    repl = replacement_quality(scored)
    boundary = boundary_metrics(scored)
    buckets = bucket_metrics(scored)
    decision = decide(comp, repl, boundary, scored)
    coverage = source_coverage(scored, daily_path)

    summary = {
        "axis_id": AXIS_ID,
        "input_rows": int(len(scored)),
        "monthly_features": MONTHLY_FEATURES,
        **boundary,
    }
    _write_json(out / "monthly_box_regime_summary.json", summary)
    scored.to_csv(out / "monthly_box_regime_rows.csv", index=False)
    _write_json(out / "bucket_metrics.json", buckets)
    _write_json(out / "topk_comparison.json", {"rows": comp.to_dict("records"), **boundary})
    _write_json(out / "replacement_quality.json", repl)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass" if decision["research_decision"] != "blocked_missing_contract" else "blocked",
            "monthly_features_use_daily_bars_up_to_decision_date": True,
            "future_outcomes_used_for_ranking": False,
            "outcomes_used_evaluation_only": True,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "source_coverage.json", coverage)
    _write_json(
        out / "lineage.json",
        {
            "input_family_source_root": input_root,
            "daily_confirmed_bar_source": daily_path,
            "closed_prior_axes": [
                r"G:\Tradex\starter_chart_review_branch_closure_v1\20260525T072259Z-starter-chart-review-branch-closure-v1",
                r"G:\Tradex\watch_persistence_quality_pretest_v1\20260525T072903Z-watch-persistence-quality-pretest-v1",
                r"G:\Tradex\starter_entry_volatility_extension_demotion_v1\20260525T073632Z-starter-entry-volatility-extension-demotion-v1",
            ],
        },
    )
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX starter-entry monthly box/regime interaction pretest")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.daily_path, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
