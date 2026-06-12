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

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "ma_transition_bad_pick_pretest_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_transition_bad_pick_pretest_v1")
DEFAULT_TRANSITION_DEFINITION = Path(
    "G:/Tradex/ma_break_regime_transition_probability_v1/20260603T093838Z-ma-break-regime-transition-probability-v1/transition_definition.json"
)
TOPK_VALUES = (5, 10, 20)
VARIANTS = (
    "baseline",
    "hard_exclude_ma100_trend_down",
    "hard_exclude_ma60_or_ma100_trend_down",
    "soft_demotion_ma100_trend_down",
    "soft_demotion_ma60_or_ma100_trend_down",
    "ma200_bearish_stack_guard",
)
MIN_EXPOSURE_SAMPLE = 50
MIN_YEAR_SAMPLE = 20
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "signal_definition.json",
    "base_topk_signal_exposure.csv",
    "variant_topk_comparison.csv",
    "bad_pick_removal_summary.json",
    "replacement_quality_summary.csv",
    "yearly_stability_summary.csv",
    "candidate_diff_examples.csv",
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
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _date_expr(column: str) -> str:
    return f"""
    CASE
      WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER)
      WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000), '%Y%m%d') AS INTEGER)
      WHEN {column} >= 100000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)
      ELSE CAST(regexp_replace(CAST({column} AS VARCHAR), '[^0-9]', '', 'g') AS INTEGER)
    END
    """


def _load_daily(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int | None) -> pd.DataFrame:
    end_clause = "" if end_ymd is None else "AND ymd <= ?"
    params: list[Any] = [int(start_ymd)]
    if end_ymd is not None:
        params.append(int(end_ymd))
    query = f"""
    WITH normalized AS (
      SELECT CAST(code AS VARCHAR) AS code, {_date_expr("date")} AS ymd,
             CAST(o AS DOUBLE) AS o, CAST(h AS DOUBLE) AS h, CAST(l AS DOUBLE) AS l, CAST(c AS DOUBLE) AS c,
             lower(coalesce(source, '')) AS source
      FROM daily_bars
      WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
        AND lower(coalesce(source, '')) IN ('pan', 'txt', 'confirmed')
    )
    SELECT * FROM normalized WHERE ymd >= ? {end_clause} ORDER BY code, ymd
    """
    df = conn.execute(query, params).fetchdf()
    if df.empty:
        raise RuntimeError("daily_bars query returned no rows")
    df["code"] = df["code"].astype(str)
    df["ymd"] = pd.to_numeric(df["ymd"], errors="coerce").astype(int)
    return df.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _load_monthly(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
    SELECT CAST(mb.code AS VARCHAR) AS code, {_date_expr("mb.month")} AS month_ymd,
           CAST(mb.c AS DOUBLE) AS monthly_close, CAST(mm.ma20 AS DOUBLE) AS monthly_ma20
    FROM monthly_bars mb
    LEFT JOIN monthly_ma mm ON mb.code = mm.code AND mb.month = mm.month
    WHERE mb.c > 0
    ORDER BY mb.code, mb.month
    """
    df = conn.execute(query).fetchdf()
    if df.empty:
        return df
    df["month_dt"] = pd.to_datetime(df["month_ymd"].astype(str), format="%Y%m%d", errors="coerce")
    df["monthly_above_ma20"] = df["monthly_close"] >= df["monthly_ma20"]
    df["monthly_ma20_slope_3m_pct"] = df.groupby("code")["monthly_ma20"].transform(lambda s: (s / s.shift(3) - 1.0) * 100.0)
    df["monthly_ma20_slope_state"] = pd.cut(df["monthly_ma20_slope_3m_pct"], [-float("inf"), -1.0, 1.0, float("inf")], labels=["down", "flat", "up"]).astype("object")
    return df


def _weekly_context(daily: pd.DataFrame) -> pd.DataFrame:
    weekly = daily[["code", "ymd", "c"]].copy()
    weekly["date"] = pd.to_datetime(weekly["ymd"].astype(str), format="%Y%m%d")
    weekly = (
        weekly.set_index("date")
        .groupby("code", group_keys=False)
        .resample("W-FRI")
        .agg({"code": "last", "ymd": "max", "c": "last"})
        .dropna(subset=["code", "ymd", "c"])
        .reset_index(drop=True)
    )
    g = weekly.groupby("code", group_keys=False)
    weekly["weekly_ma20"] = g["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    weekly["weekly_ma20_slope_4w_pct"] = g["weekly_ma20"].transform(lambda s: (s / s.shift(4) - 1.0) * 100.0)
    weekly["weekly_above_ma20"] = weekly["c"] >= weekly["weekly_ma20"]
    weekly["weekly_ma20_slope_state"] = pd.cut(weekly["weekly_ma20_slope_4w_pct"], [-float("inf"), -0.5, 0.5, float("inf")], labels=["down", "flat", "up"]).astype("object")
    return weekly.rename(columns={"ymd": "week_ymd"})


def _add_signal_features(daily: pd.DataFrame, monthly: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    g = out.groupby("code", group_keys=False)
    for w in (20, 60, 100, 200):
        out[f"ma{w}"] = g["c"].transform(lambda s, w=w: s.rolling(w, min_periods=w).mean())
        out[f"prev_c_ma{w}"] = g["c"].shift(1)
        out[f"prev_ma{w}"] = g[f"ma{w}"].shift(1)
        out[f"break_ma{w}"] = (out[f"prev_c_ma{w}"] >= out[f"prev_ma{w}"]) & (out["c"] < out[f"ma{w}"])
    for w in (60, 100):
        out[f"ma{w}_slope_20d_pct"] = g[f"ma{w}"].transform(lambda s: (s / s.shift(20) - 1.0) * 100.0)
        out[f"ma{w}_slope_state"] = pd.cut(out[f"ma{w}_slope_20d_pct"], [-float("inf"), -0.5, 0.5, float("inf")], labels=["down", "flat", "up"]).astype("object")
    out["ma_alignment"] = "mixed_stack"
    out.loc[(out["ma20"] > out["ma60"]) & (out["ma60"] > out["ma100"]) & (out["ma100"] > out["ma200"]), "ma_alignment"] = "bullish_stack"
    out.loc[(out["ma20"] < out["ma60"]) & (out["ma60"] < out["ma100"]) & (out["ma100"] < out["ma200"]), "ma_alignment"] = "bearish_stack"
    out["event_dt"] = pd.to_datetime(out["ymd"].astype(str), format="%Y%m%d")

    if not monthly.empty:
        parts = []
        mcols = ["code", "month_dt", "monthly_above_ma20", "monthly_ma20_slope_state"]
        for code, rows in out.groupby("code", sort=False):
            m = monthly[monthly["code"] == code][mcols].dropna(subset=["month_dt"]).sort_values("month_dt")
            parts.append(pd.merge_asof(rows.sort_values("event_dt"), m, left_on="event_dt", right_on="month_dt", by="code", direction="backward") if not m.empty else rows)
        out = pd.concat(parts, ignore_index=True)
    else:
        out["monthly_above_ma20"] = None
        out["monthly_ma20_slope_state"] = None
    parts = []
    wcols = ["code", "week_ymd", "weekly_above_ma20", "weekly_ma20_slope_state"]
    for code, rows in out.groupby("code", sort=False):
        w = weekly[weekly["code"] == code][wcols].copy().sort_values("week_ymd")
        w["week_dt"] = pd.to_datetime(w["week_ymd"].astype(int).astype(str), format="%Y%m%d")
        parts.append(pd.merge_asof(rows.sort_values("event_dt"), w.drop(columns=["code"]), left_on="event_dt", right_on="week_dt", direction="backward"))
    out = pd.concat(parts, ignore_index=True)

    monthly_up = (out["monthly_above_ma20"] == True) & (out["monthly_ma20_slope_state"].isin(["up", "flat"]))
    weekly_up = (out["weekly_above_ma20"] == True) & (out["weekly_ma20_slope_state"].isin(["up", "flat"]))
    trend_down = ((out["c"] < out["ma60"]) & (out["ma60_slope_state"] == "down")) | ((out["c"] < out["ma100"]) & (out["ma100_slope_state"] == "down")) | ((out["ma20"] < out["ma60"]) & (out["ma60_slope_state"] == "down"))
    uptrend_pullback = (monthly_up | weekly_up) & ((out["c"] < out["ma20"]) | (out["c"] < out["ma60"])) & (out["ma60_slope_state"].isin(["up", "flat"]) | out["ma100_slope_state"].isin(["up", "flat"]))
    out["range_vs_trend"] = "other"
    out.loc[uptrend_pullback, "range_vs_trend"] = "uptrend_pullback_candidate"
    out.loc[trend_down, "range_vs_trend"] = "trend_down_candidate"
    out["ma100_trend_down_signal"] = out["break_ma100"].fillna(False) & out["range_vs_trend"].eq("trend_down_candidate")
    out["ma60_trend_down_signal"] = out["break_ma60"].fillna(False) & out["range_vs_trend"].eq("trend_down_candidate")
    out["ma200_bearish_stack_signal"] = out["break_ma200"].fillna(False) & out["ma_alignment"].eq("bearish_stack")
    return out[
        [
            "code",
            "ymd",
            "ma60_trend_down_signal",
            "ma100_trend_down_signal",
            "ma200_bearish_stack_signal",
            "range_vs_trend",
            "ma_alignment",
        ]
    ]


def _load_rankings(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int | None) -> pd.DataFrame:
    end_clause = "" if end_ymd is None else "AND dt <= ?"
    params: list[Any] = [int(start_ymd)]
    if end_ymd is not None:
        params.append(int(end_ymd))
    rows = conn.execute(
        f"""
        SELECT dt, CAST(code AS VARCHAR) AS code, name, rank, display_score, ranking_logic_version,
               anchor_price_close
        FROM ranking_appearance_daily
        WHERE dir = 'up' AND rank <= 50 AND dt >= ? {end_clause}
          AND ranking_logic_version = (SELECT ranking_logic_version FROM ranking_logic_registry WHERE is_active = true LIMIT 1)
        ORDER BY dt, rank, code
        """,
        params,
    ).fetchdf()
    if rows.empty:
        raise RuntimeError("ranking_appearance_daily returned no up top50 rows")
    rows["code"] = rows["code"].astype(str)
    return rows


def _attach_outcomes(rankings: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    daily_by_code = {code: rows.reset_index(drop=True) for code, rows in daily.groupby("code", sort=False)}
    out_rows: list[dict[str, Any]] = []
    for row in rankings.to_dict("records"):
        d = daily_by_code.get(str(row["code"]))
        if d is None:
            continue
        matches = d.index[d["ymd"] == int(row["dt"])].tolist()
        if not matches:
            continue
        idx = matches[0]
        if idx + 20 >= len(d):
            continue
        close = float(d.iloc[idx]["c"])
        win = d.iloc[idx + 1 : idx + 21]
        rec = dict(row)
        rec["event_year"] = int(str(int(row["dt"]))[:4])
        rec["ret20"] = float(d.iloc[idx + 20]["c"] / close - 1.0)
        rec["hit_rate_flag"] = rec["ret20"] > 0
        rec["severe_loss"] = rec["ret20"] <= -0.10
        rec["max_drawdown"] = float(win["l"].min() / close - 1.0)
        out_rows.append(rec)
    return pd.DataFrame(out_rows)


def _apply_variants(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["signal_ma100"] = out["ma100_trend_down_signal"].fillna(False).astype(bool)
    out["signal_ma60_or_ma100"] = out["ma60_trend_down_signal"].fillna(False).astype(bool) | out["signal_ma100"]
    out["signal_ma200"] = out["ma200_bearish_stack_signal"].fillna(False).astype(bool)
    sort_score = out["display_score"].fillna(0.0) - out["rank"].fillna(999) * 0.0001
    out["baseline_variant_rank"] = out["rank"]
    configs = {
        "hard_exclude_ma100_trend_down": ("signal_ma100", 1_000_000.0),
        "hard_exclude_ma60_or_ma100_trend_down": ("signal_ma60_or_ma100", 1_000_000.0),
        "soft_demotion_ma100_trend_down": ("signal_ma100", 0.10),
        "soft_demotion_ma60_or_ma100_trend_down": ("signal_ma60_or_ma100", 0.10),
        "ma200_bearish_stack_guard": ("signal_ma200", 1_000_000.0),
    }
    for variant, (flag, penalty) in configs.items():
        score_col = f"{variant}_sort_score"
        rank_col = f"{variant}_rank"
        out[score_col] = sort_score - out[flag].astype(int) * penalty
        out[rank_col] = out.sort_values(["dt", score_col, "rank", "code"], ascending=[True, False, True, True]).groupby("dt").cumcount() + 1
    return out


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    vals = pd.to_numeric(frame.get(col), errors="coerce").dropna()
    return None if vals.empty else float(vals.mean())


def _rate(series: pd.Series) -> float | None:
    vals = series.dropna()
    return None if vals.empty else float(vals.astype(bool).mean())


def _membership_counts(rows: pd.DataFrame, variant: str, topk: int) -> tuple[int, int, int, pd.DataFrame, pd.DataFrame]:
    removed_parts = []
    repl_parts = []
    changed_sessions = 0
    for _, g in rows.groupby("dt", sort=False):
        base = set(g[g["rank"] <= topk]["code"].astype(str))
        chal = set(g[g[f"{variant}_rank"] <= topk]["code"].astype(str)) if variant != "baseline" else base
        if base != chal:
            changed_sessions += 1
        removed_parts.append(g[g["code"].astype(str).isin(base - chal)])
        repl_parts.append(g[g["code"].astype(str).isin(chal - base)])
    removed = pd.concat(removed_parts, ignore_index=True) if removed_parts else pd.DataFrame()
    repl = pd.concat(repl_parts, ignore_index=True) if repl_parts else pd.DataFrame()
    return changed_sessions, int(len(removed)), int(len(repl)), removed, repl


def _variant_summary(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, Any]] = []
    repl_rows: list[dict[str, Any]] = []
    diff_rows: list[dict[str, Any]] = []
    for variant in VARIANTS:
        rank_col = "rank" if variant == "baseline" else f"{variant}_rank"
        for topk in TOPK_VALUES:
            base = rows[rows["rank"] <= topk]
            chal = rows[rows[rank_col] <= topk]
            changed5, _, _, _, _ = _membership_counts(rows, variant, 5)
            changed10, _, _, _, _ = _membership_counts(rows, variant, 10)
            changed20, removed_count, repl_count, removed, repl = _membership_counts(rows, variant, 20)
            signal_count = int(removed[["signal_ma100", "signal_ma60_or_ma100", "signal_ma200"]].any(axis=1).sum()) if not removed.empty else 0
            summary_rows.append(
                {
                    "variant": variant,
                    "topk": topk,
                    "sample_sessions": int(rows["dt"].nunique()),
                    "changed_top5_members_count": changed5,
                    "changed_top10_members_count": changed10,
                    "changed_top20_members_count": changed20,
                    "removed_signal_candidate_count": signal_count,
                    "replacement_candidate_count": repl_count,
                    "base_topK_mean_ret20": _mean(base, "ret20"),
                    "variant_topK_mean_ret20": _mean(chal, "ret20"),
                    "uplift_ret20": (_mean(chal, "ret20") or 0.0) - (_mean(base, "ret20") or 0.0),
                    "base_hit_rate": _rate(base["hit_rate_flag"]),
                    "variant_hit_rate": _rate(chal["hit_rate_flag"]),
                    "base_severe_loss_rate": _rate(base["severe_loss"]),
                    "variant_severe_loss_rate": _rate(chal["severe_loss"]),
                    "severe_loss_reduction": (_rate(base["severe_loss"]) or 0.0) - (_rate(chal["severe_loss"]) or 0.0),
                    "base_mean_max_drawdown": _mean(base, "max_drawdown"),
                    "variant_mean_max_drawdown": _mean(chal, "max_drawdown"),
                    "drawdown_improvement": (_mean(chal, "max_drawdown") or 0.0) - (_mean(base, "max_drawdown") or 0.0),
                    "replacement_mean_ret20": _mean(repl, "ret20"),
                    "replacement_severe_loss_rate": _rate(repl["severe_loss"]) if not repl.empty else None,
                    "selection_divergence_reason": "no_change" if variant == "baseline" or removed_count == 0 else "signal_candidate_removed_and_next_rank_replacement",
                }
            )
            repl_rows.append(
                {
                    "variant": variant,
                    "topk": topk,
                    "removed_count": removed_count,
                    "replacement_count": repl_count,
                    "removed_mean_ret20": _mean(removed, "ret20"),
                    "replacement_mean_ret20": _mean(repl, "ret20"),
                    "replacement_minus_removed_ret20": (_mean(repl, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0),
                    "removed_severe_loss_rate": _rate(removed["severe_loss"]) if not removed.empty else None,
                    "replacement_severe_loss_rate": _rate(repl["severe_loss"]) if not repl.empty else None,
                }
            )
            if not removed.empty:
                sample = removed.head(50).copy()
                sample["variant"] = variant
                sample["topk"] = topk
                diff_rows.extend(sample[["variant", "topk", "dt", "code", "name", "rank", "ret20", "max_drawdown", "signal_ma100", "signal_ma60_or_ma100", "signal_ma200"]].to_dict("records"))
    return pd.DataFrame(summary_rows), pd.DataFrame(repl_rows), pd.DataFrame(diff_rows)


def _exposure(rows: pd.DataFrame) -> pd.DataFrame:
    signal_cols = ["signal_ma100", "signal_ma60_or_ma100", "signal_ma200"]
    out = []
    for topk in TOPK_VALUES:
        subset = rows[rows["rank"] <= topk]
        for sig in signal_cols:
            exposed = subset[subset[sig]]
            by_year = exposed.groupby("event_year").size().to_dict()
            symbol_counts = exposed["code"].value_counts()
            out.append(
                {
                    "topk": topk,
                    "signal": sig,
                    "exposure_count": int(len(exposed)),
                    "exposure_rate": float(len(exposed) / len(subset)) if len(subset) else None,
                    "unique_symbol_count": int(exposed["code"].nunique()),
                    "top_symbol_share": None if exposed.empty else float(symbol_counts.iloc[0] / len(exposed)),
                    "year_distribution_json": json.dumps({str(k): int(v) for k, v in by_year.items()}, ensure_ascii=False),
                    "sample_status": "sufficient" if len(exposed) >= MIN_EXPOSURE_SAMPLE else "insufficient_sample",
                }
            )
    return pd.DataFrame(out)


def _yearly(summary_input: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year, yr in summary_input.groupby("event_year"):
        yearly_summary, _, _ = _variant_summary(yr)
        yearly_summary["event_year"] = int(year)
        yearly_summary["sample_status"] = yearly_summary["removed_signal_candidate_count"].apply(lambda n: "sufficient" if int(n) >= MIN_YEAR_SAMPLE else "insufficient_sample")
        rows.append(yearly_summary)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _decision(exposure: pd.DataFrame, comparison: pd.DataFrame, replacement: pd.DataFrame, yearly: pd.DataFrame) -> dict[str, Any]:
    reasons = []
    for variant in VARIANTS:
        if variant == "baseline":
            continue
        rows = comparison[(comparison["variant"] == variant) & (comparison["topk"] == 10)]
        repl = replacement[(replacement["variant"] == variant) & (replacement["topk"] == 10)]
        if rows.empty or repl.empty:
            continue
        row = rows.iloc[0]
        rep = repl.iloc[0]
        yearly_rows = yearly[(yearly["variant"] == variant) & (yearly["topk"] == 10) & (yearly["sample_status"] == "sufficient")]
        stable_years = int(((yearly_rows["severe_loss_reduction"] > 0) | (yearly_rows["drawdown_improvement"] > 0)).sum()) if not yearly_rows.empty else 0
        sufficient = int(row["removed_signal_candidate_count"]) >= MIN_EXPOSURE_SAMPLE
        risk_improved = float(row["severe_loss_reduction"]) > 0 and float(row["drawdown_improvement"]) > 0
        replacement_ok = pd.notna(rep["replacement_minus_removed_ret20"]) and float(rep["replacement_minus_removed_ret20"]) >= 0
        ret_ok = float(row["uplift_ret20"]) >= 0
        if sufficient and risk_improved and replacement_ok and ret_ok and stable_years >= 2:
            reasons.append({"variant": variant, "decision_type": "keep_for_challenger_next", "topk": 10, "stable_years": stable_years, "metrics": row.to_dict(), "replacement": rep.to_dict()})
        elif sufficient and risk_improved and stable_years >= 2:
            reasons.append({"variant": variant, "decision_type": "keep_as_veto_only", "topk": 10, "stable_years": stable_years, "metrics": row.to_dict(), "replacement": rep.to_dict()})
    if any(r["decision_type"] == "keep_for_challenger_next" for r in reasons):
        decision = "keep_for_challenger_next"
        reason = "signal_exposure_risk_reduction_replacement_quality_and_ret20_pass"
    elif any(r["decision_type"] == "keep_as_veto_only" for r in reasons):
        decision = "keep_as_veto_only"
        reason = "risk_improves_but_return_or_replacement_quality_is_weak"
    elif exposure["sample_status"].eq("sufficient").sum() == 0:
        decision = "drop"
        reason = "exposure_inside_base_topk_too_small"
    else:
        decision = "hold"
        reason = "direction_exists_but_sample_replacement_or_yearly_stability_is_insufficient"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "supporting_reasons": reasons,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DuckDB writes",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no live buy/sell rule",
            "no volume condition",
            "no stock-specific correction",
            "no MA20 uptrend pullback feature",
            "no threshold sweep beyond specified variants",
        ],
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    db_path = Path(args.db_path) if args.db_path else resolve_runtime_stock_db_path()
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness()
    db_contract = inspect_runtime_stock_db(runtime_db_path=db_path)
    transition_definition = json.loads(Path(args.transition_definition).read_text(encoding="utf-8"))

    with duckdb.connect(str(db_path), read_only=True) as conn:
        daily = _load_daily(conn, start_ymd=args.start_ymd, end_ymd=args.end_ymd)
        monthly = _load_monthly(conn)
        rankings = _load_rankings(conn, start_ymd=args.start_ymd, end_ymd=args.end_ymd)
    weekly = _weekly_context(daily)
    signals = _add_signal_features(daily, monthly, weekly)
    ranked = _attach_outcomes(rankings, daily)
    ranked = ranked.merge(signals, left_on=["code", "dt"], right_on=["code", "ymd"], how="left")
    ranked[["ma60_trend_down_signal", "ma100_trend_down_signal", "ma200_bearish_stack_signal"]] = ranked[
        ["ma60_trend_down_signal", "ma100_trend_down_signal", "ma200_bearish_stack_signal"]
    ].fillna(False)
    ranked = _apply_variants(ranked)

    exposure = _exposure(ranked)
    comparison, replacement, examples = _variant_summary(ranked)
    yearly = _yearly(ranked)
    decision = _decision(exposure, comparison, replacement, yearly)

    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "db_contract": db_contract,
        "runtime_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "transition_definition_path": str(args.transition_definition),
        "transition_definition_axis_id": transition_definition.get("axis_id"),
        "start_ymd": args.start_ymd,
        "end_ymd": args.end_ymd,
        "confirmed_bars_only": True,
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "same_condition_comparison_preserved": True,
        "ranking_source": "runtime DuckDB ranking_appearance_daily active ranking_logic_registry dir=up rank<=50",
        "ranking_rows_loaded": int(len(rankings)),
        "ranked_rows_with_ret20": int(len(ranked)),
        "sample_sessions": int(ranked["dt"].nunique()),
        "code_count": int(ranked["code"].nunique()),
        "min_dt": int(ranked["dt"].min()),
        "max_dt": int(ranked["dt"].max()),
    }

    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "signal_definition.json", {"axis_id": AXIS_ID, "transition_definition": transition_definition, "variants": list(VARIANTS)})
    exposure.to_csv(out_dir / "base_topk_signal_exposure.csv", index=False, encoding="utf-8")
    comparison.to_csv(out_dir / "variant_topk_comparison.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "bad_pick_removal_summary.json", {"axis_id": AXIS_ID, "rows": comparison.to_dict(orient="records")})
    replacement.to_csv(out_dir / "replacement_quality_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    examples.to_csv(out_dir / "candidate_diff_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete" if not missing else "incomplete",
            "missing_artifacts": missing,
            "authoritative_result": str(out_dir / "research_decision.json"),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only MA transition bad-pick topK pretest.")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--transition-definition", type=Path, default=DEFAULT_TRANSITION_DEFINITION)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
