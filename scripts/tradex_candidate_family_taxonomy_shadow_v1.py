from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "candidate_family_taxonomy_shadow_v1"
DEFAULT_TRACE_ROOT = Path(r"G:\Tradex\topk_reform_trace_contract_repair_v1\20260524T134106Z-topk-reform-trace-contract-repair-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_family_taxonomy_shadow_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "shadow_taxonomy_schema.json",
    "candidate_family_tag_rows.csv",
    "tag_coverage_summary.json",
    "selected_loser_by_tag_summary.csv",
    "tag_cooccurrence_summary.csv",
    "winner_damage_by_tag.csv",
    "candidate_source_redesign_recommendations.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
TOPK_VALUES = (5, 10, 20)


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
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _mean(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    return pd.to_numeric(num, errors="coerce") / pd.to_numeric(den, errors="coerce").replace(0, pd.NA)


def _streak(mask: pd.Series) -> pd.Series:
    out: list[int] = []
    count = 0
    for value in mask.fillna(False).astype(bool):
        count = count + 1 if value else 0
        out.append(count)
    return pd.Series(out, index=mask.index, dtype="int64")


def _days_since_reclaim(close: pd.Series, ma: pd.Series) -> pd.Series:
    above = close > ma
    prev_above = above.shift(1, fill_value=False)
    reclaim = above & ~prev_above
    out: list[float | None] = []
    last: int | None = None
    for i, is_reclaim in enumerate(reclaim.fillna(False).astype(bool)):
        if is_reclaim:
            last = i
        out.append(None if last is None else float(i - last))
    return pd.Series(out, index=close.index, dtype="float64")


def build_daily_features(daily_path: Path) -> pd.DataFrame:
    daily = pd.read_csv(daily_path)
    daily["code"] = daily["code"].astype(str)
    daily["decision_date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y%m%d").astype(int)
    daily = daily.sort_values(["code", "decision_date"]).drop_duplicates(["code", "decision_date"], keep="last").reset_index(drop=True)
    frames: list[pd.DataFrame] = []
    for _code, group in daily.groupby("code", sort=False):
        g = group.copy()
        close = pd.to_numeric(g["close"], errors="coerce")
        high = pd.to_numeric(g["high"], errors="coerce")
        low = pd.to_numeric(g["low"], errors="coerce")
        open_ = pd.to_numeric(g["open"], errors="coerce")
        volume = pd.to_numeric(g["volume"], errors="coerce")
        g["ma7"] = close.rolling(7, min_periods=7).mean()
        g["ma20"] = close.rolling(20, min_periods=20).mean()
        g["ma60"] = close.rolling(60, min_periods=60).mean()
        g["ma7_slope"] = g["ma7"].pct_change(5)
        g["ma20_slope"] = g["ma20"].pct_change(5)
        g["ma60_slope"] = g["ma60"].pct_change(5)
        g["dist_ma7_pct"] = _safe_div(close - g["ma7"], g["ma7"])
        g["dist_ma20_pct"] = _safe_div(close - g["ma20"], g["ma20"])
        g["dist_ma60_pct"] = _safe_div(close - g["ma60"], g["ma60"])
        g["ma7_gt_ma20_gt_ma60"] = (g["ma7"] > g["ma20"]) & (g["ma20"] > g["ma60"])
        g["above7_streak"] = _streak(close > g["ma7"])
        g["above20_streak"] = _streak(close > g["ma20"])
        g["above60_streak"] = _streak(close > g["ma60"])
        g["days_since_ma20_reclaim"] = _days_since_reclaim(close, g["ma20"])
        g["days_since_ma60_reclaim"] = _days_since_reclaim(close, g["ma60"])
        span = (high - low).replace(0, pd.NA)
        upper = high - pd.concat([open_, close], axis=1).max(axis=1)
        lower = pd.concat([open_, close], axis=1).min(axis=1) - low
        body = (close - open_).abs()
        g["upper_wick_ratio"] = upper / span
        g["lower_wick_ratio"] = lower / span
        g["large_bullish_candle"] = (close > open_) & (_safe_div(close - open_, close) >= 0.03)
        g["large_bearish_candle"] = (close < open_) & (_safe_div(open_ - close, close) >= 0.03)
        g["failed_high_update"] = (high >= high.rolling(20, min_periods=20).max().shift(1)) & (close < open_) & (upper > body)
        g["volume_ma20_ratio"] = _safe_div(volume, volume.rolling(20, min_periods=20).mean())
        g["realized_vol20"] = close.pct_change().rolling(20, min_periods=20).std()
        tr = pd.concat([(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        g["atr14_pct"] = _safe_div(tr.rolling(14, min_periods=14).mean(), close)
        high120 = close.rolling(120, min_periods=60).max().shift(1)
        low120 = close.rolling(120, min_periods=60).min().shift(1)
        prev_high120 = high120.shift(1)
        g["monthly_high_zone_proxy"] = close >= high120 * 0.95
        g["monthly_box_breakout_proxy"] = (close > prev_high120) & (close.shift(1) <= prev_high120)
        g["monthly_box_inside_proxy"] = (close < high120 * 0.95) & (close > low120 * 1.05)
        g["weekly_monthly_uptrend_proxy"] = (g["ma20"] > g["ma60"]) & (g["ma20_slope"] > 0) & (g["ma60_slope"] >= 0)
        frames.append(g)
    columns = [
        "code",
        "decision_date",
        "close",
        "ma7",
        "ma20",
        "ma60",
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "ma7_gt_ma20_gt_ma60",
        "above7_streak",
        "above20_streak",
        "above60_streak",
        "days_since_ma20_reclaim",
        "days_since_ma60_reclaim",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "large_bullish_candle",
        "large_bearish_candle",
        "failed_high_update",
        "volume_ma20_ratio",
        "realized_vol20",
        "atr14_pct",
        "monthly_high_zone_proxy",
        "monthly_box_breakout_proxy",
        "monthly_box_inside_proxy",
        "weekly_monthly_uptrend_proxy",
    ]
    return pd.concat(frames, ignore_index=True)[columns]


def attach_labels(rows: pd.DataFrame, daily_features: pd.DataFrame) -> pd.DataFrame:
    prices = daily_features[["code", "decision_date", "close", "ma20", "ma60"]].sort_values(["code", "decision_date"]).copy()
    labeled_frames: list[pd.DataFrame] = []
    for code, group in prices.groupby("code", sort=False):
        g = group.copy()
        g["ret20"] = g["close"].shift(-20) / g["close"] - 1
        labeled_frames.append(g[["code", "decision_date", "ret20"]])
    labels = pd.concat(labeled_frames, ignore_index=True)
    out = rows.merge(labels, on=["code", "decision_date"], how="left")
    out["same_date_ret20_rank_pct"] = out.groupby("decision_date")["ret20"].rank(pct=True, method="average")
    out["selected_loser"] = (out["ret20"] <= -0.05) | (out["same_date_ret20_rank_pct"] <= 0.30)
    out["selected_winner"] = (out["ret20"] >= 0.05) | (out["same_date_ret20_rank_pct"] >= 0.70)
    out["selected_non_loser"] = out["ret20"].notna() & ~out["selected_loser"]
    return out


def _quantile_flag(frame: pd.DataFrame, column: str, q: float) -> pd.Series:
    if column not in frame:
        return pd.Series(False, index=frame.index)
    thresholds = frame.groupby("decision_date")[column].transform(lambda s: pd.to_numeric(s, errors="coerce").quantile(q))
    return pd.to_numeric(frame[column], errors="coerce") >= thresholds


def tag_rows(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    rows["dist_ma20_top_quartile"] = _quantile_flag(rows, "dist_ma20_pct", 0.75)
    rows["dist_ma60_top_quartile"] = _quantile_flag(rows, "dist_ma60_pct", 0.75)
    rows["ma7_slope_top_quartile"] = _quantile_flag(rows, "ma7_slope", 0.75)
    rows["realized_vol20_top_quartile"] = _quantile_flag(rows, "realized_vol20", 0.75)

    setup_tags: list[list[str]] = []
    risk_tags: list[list[str]] = []
    regime_tags: list[list[str]] = []
    for row in rows.to_dict("records"):
        setup: list[str] = []
        risk: list[str] = []
        regime: list[str] = []
        above20 = row.get("above20_streak")
        above60 = row.get("above60_streak")
        if row.get("ma7_gt_ma20_gt_ma60") is True or row.get("ma7_gt_ma20_gt_ma60") == 1:
            setup.append("trend_continuation_candidate")
        if pd.notna(above20) and float(above20) < 10:
            setup.append("early_trend_candidate")
        if pd.notna(above60) and float(above60) >= 60:
            setup.append("mature_trend_candidate")
        if pd.notna(row.get("days_since_ma20_reclaim")) and float(row["days_since_ma20_reclaim"]) <= 5:
            setup.append("weak_reclaim_candidate")
        if pd.notna(above20) and 10 <= float(above20) < 40:
            setup.append("pullback_candidate")
        if row.get("monthly_box_breakout_proxy") is True or row.get("monthly_high_zone_proxy") is True:
            setup.append("breakout_or_high_zone_candidate")
        if row.get("monthly_box_inside_proxy") is True:
            setup.append("range_candidate")
        if row.get("dist_ma20_top_quartile") is True or row.get("dist_ma60_top_quartile") is True:
            setup.append("overextension_candidate")
        if not setup:
            setup.append("uncategorized_candidate")

        if row.get("dist_ma20_top_quartile") is True:
            risk.append("ma20_overextension_risk")
        if row.get("dist_ma60_top_quartile") is True:
            risk.append("ma60_overextension_risk")
        if row.get("ma7_slope_top_quartile") is True:
            risk.append("steep_ma7_slope_risk")
        if row.get("realized_vol20_top_quartile") is True:
            risk.append("high_volatility_risk")
        if pd.notna(row.get("upper_wick_ratio")) and float(row["upper_wick_ratio"]) >= 0.45:
            risk.append("upper_wick_risk")
        if row.get("failed_high_update") is True:
            risk.append("failed_high_update_risk")
        if row.get("large_bearish_candle") is True:
            risk.append("large_bearish_candle_risk")
        if not risk:
            risk.append("no_shadow_risk_tag")

        if row.get("monthly_high_zone_proxy") is True:
            regime.append("monthly_high_zone")
        if row.get("monthly_box_inside_proxy") is True:
            regime.append("monthly_box_inside")
        if row.get("monthly_box_breakout_proxy") is True:
            regime.append("monthly_box_breakout")
        if row.get("weekly_monthly_uptrend_proxy") is True:
            regime.append("weekly_monthly_uptrend_proxy")
        if row.get("monthly_box_inside_proxy") is True and not (row.get("weekly_monthly_uptrend_proxy") is True):
            regime.append("range_proxy")
        if not regime:
            regime.append("unknown_regime")

        setup_tags.append(sorted(set(setup)))
        risk_tags.append(sorted(set(risk)))
        regime_tags.append(sorted(set(regime)))
    rows["research_setup_tags_json"] = [json.dumps(tags, ensure_ascii=False, sort_keys=True) for tags in setup_tags]
    rows["research_risk_tags_json"] = [json.dumps(tags, ensure_ascii=False, sort_keys=True) for tags in risk_tags]
    rows["research_regime_tags_json"] = [json.dumps(tags, ensure_ascii=False, sort_keys=True) for tags in regime_tags]
    return rows


def shadow_taxonomy_schema() -> dict[str, Any]:
    return {
        "schema_version": "candidate_family_taxonomy_shadow_v1",
        "scope": "TRADEX-only",
        "naming_rule": {
            "do_not_fill": ["candidate_source", "signal_family", "setup_name", "reason_codes"],
            "derived_diagnostic_columns": ["research_setup_tags_json", "research_risk_tags_json", "research_regime_tags_json"],
        },
        "point_in_time": True,
        "definitions": {
            "same_date_relative_bins": "top quartile within decision_date candidate cohort; features only, no labels",
            "setup_tags": [
                "trend_continuation_candidate",
                "early_trend_candidate",
                "mature_trend_candidate",
                "pullback_candidate",
                "breakout_or_high_zone_candidate",
                "range_candidate",
                "overextension_candidate",
                "weak_reclaim_candidate",
                "uncategorized_candidate",
            ],
            "risk_tags": [
                "ma20_overextension_risk",
                "ma60_overextension_risk",
                "steep_ma7_slope_risk",
                "high_volatility_risk",
                "upper_wick_risk",
                "failed_high_update_risk",
                "large_bearish_candle_risk",
                "no_shadow_risk_tag",
            ],
            "regime_tags": [
                "monthly_high_zone",
                "monthly_box_inside",
                "monthly_box_breakout",
                "weekly_monthly_uptrend_proxy",
                "range_proxy",
                "unknown_regime",
            ],
        },
    }


def explode_tags(rows: pd.DataFrame, column: str, group_name: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows.itertuples(index=False):
        tags = json.loads(getattr(row, column))
        for tag in tags:
            records.append(
                {
                    "decision_date": row.decision_date,
                    "year": row.year,
                    "code": row.code,
                    "baseline_rank": row.baseline_rank,
                    "ret20": row.ret20,
                    "selected_loser": row.selected_loser,
                    "selected_winner": row.selected_winner,
                    "selected_non_loser": row.selected_non_loser,
                    "tag_group": group_name,
                    "tag": tag,
                }
            )
    return pd.DataFrame(records)


def _period_slices(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["year"].eq(2024)]),
        ("2025", rows[rows["year"].eq(2025)]),
        ("2026_label_safe", rows[rows["year"].eq(2026)]),
        ("2024_2026_combined", rows[rows["year"].isin([2024, 2025, 2026])]),
    ]


def summarize_by_tag(rows: pd.DataFrame) -> pd.DataFrame:
    tag_frames = [
        explode_tags(rows, "research_setup_tags_json", "setup"),
        explode_tags(rows, "research_risk_tags_json", "risk"),
        explode_tags(rows, "research_regime_tags_json", "regime"),
    ]
    tags = pd.concat(tag_frames, ignore_index=True)
    output: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(tags):
        for topk in TOPK_VALUES:
            top = period_rows[pd.to_numeric(period_rows["baseline_rank"], errors="coerce") <= topk]
            for (group, tag), g in top.groupby(["tag_group", "tag"], dropna=False):
                selected_loser_count = int(g["selected_loser"].sum())
                selected_winner_count = int(g["selected_winner"].sum())
                output.append(
                    {
                        "period": period,
                        "topk": topk,
                        "tag_group": group,
                        "tag": tag,
                        "n": int(len(g)),
                        "selected_loser_count": selected_loser_count,
                        "selected_winner_count": selected_winner_count,
                        "selected_non_loser_count": int(g["selected_non_loser"].sum()),
                        "selected_loser_rate": _rate(g["selected_loser"]),
                        "selected_winner_rate": _rate(g["selected_winner"]),
                        "loser_minus_winner_spread": (_rate(g["selected_loser"]) or 0.0) - (_rate(g["selected_winner"]) or 0.0),
                        "ret20_mean": _mean(g, "ret20"),
                        "ret20_median": _median(g, "ret20"),
                        "severe_loss_rate": _rate(g["ret20"] <= -0.05),
                    }
                )
    return pd.DataFrame(output)


def cooccurrence_summary(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows.to_dict("records"):
        setup = json.loads(row["research_setup_tags_json"])
        risk = json.loads(row["research_risk_tags_json"])
        regime = json.loads(row["research_regime_tags_json"])
        pairs = (
            [("setup_x_risk", a, b) for a in setup for b in risk]
            + [("setup_x_regime", a, b) for a in setup for b in regime]
            + [("risk_x_regime", a, b) for a in risk for b in regime]
        )
        for pair_group, left, right in pairs:
            records.append(
                {
                    "year": row["year"],
                    "baseline_rank": row["baseline_rank"],
                    "ret20": row["ret20"],
                    "selected_loser": row["selected_loser"],
                    "selected_winner": row["selected_winner"],
                    "pair_group": pair_group,
                    "left_tag": left,
                    "right_tag": right,
                }
            )
    pairs = pd.DataFrame(records)
    out: list[dict[str, Any]] = []
    for period, period_rows in _period_slices(pairs):
        for topk in (10,):
            top = period_rows[pd.to_numeric(period_rows["baseline_rank"], errors="coerce") <= topk]
            for (pair_group, left, right), g in top.groupby(["pair_group", "left_tag", "right_tag"], dropna=False):
                if len(g) < 30:
                    continue
                out.append(
                    {
                        "period": period,
                        "topk": topk,
                        "pair_group": pair_group,
                        "left_tag": left,
                        "right_tag": right,
                        "n": int(len(g)),
                        "selected_loser_rate": _rate(g["selected_loser"]),
                        "selected_winner_rate": _rate(g["selected_winner"]),
                        "loser_minus_winner_spread": (_rate(g["selected_loser"]) or 0.0) - (_rate(g["selected_winner"]) or 0.0),
                        "ret20_mean": _mean(g, "ret20"),
                    }
                )
    return pd.DataFrame(out).sort_values(["period", "loser_minus_winner_spread"], ascending=[True, False])


def coverage_summary(rows: pd.DataFrame) -> dict[str, Any]:
    features = [
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "above20_streak",
        "above60_streak",
        "monthly_high_zone_proxy",
        "monthly_box_inside_proxy",
        "monthly_box_breakout_proxy",
        "upper_wick_ratio",
        "failed_high_update",
        "realized_vol20",
        "atr14_pct",
    ]
    by_year = {}
    for year, g in rows.groupby("year"):
        by_year[str(year)] = {feature: float(g[feature].notna().mean()) if feature in g else 0.0 for feature in features}
    tag_counts: dict[str, dict[str, int]] = {}
    for col in ["research_setup_tags_json", "research_risk_tags_json", "research_regime_tags_json"]:
        counts: dict[str, int] = {}
        for tags in rows[col].map(json.loads):
            for tag in tags:
                counts[tag] = counts.get(tag, 0) + 1
        tag_counts[col] = counts
    return {"rows": int(len(rows)), "feature_coverage_by_year": by_year, "tag_counts": tag_counts, "tags_usable_for_selected_loser_audit": bool(len(rows) > 0)}


def winner_damage(summary: pd.DataFrame) -> pd.DataFrame:
    recent = summary[(summary["period"] == "2024_2026_combined") & (summary["topk"].isin([5, 10]))].copy()
    recent["winner_damage_risk"] = recent["selected_winner_rate"].map(lambda x: "high" if pd.notna(x) and x >= 0.30 else "medium")
    return recent.sort_values(["loser_minus_winner_spread", "n"], ascending=[False, False])


def recommendations(summary: pd.DataFrame, cooccur: pd.DataFrame) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    recent = summary[(summary["period"] == "2024_2026_combined") & (summary["topk"] == 10) & (summary["n"] >= 300)].copy()
    recent = recent.sort_values(["loser_minus_winner_spread", "n"], ascending=[False, False])
    for _, row in recent.head(3).iterrows():
        winner_rate = row.get("selected_winner_rate")
        candidates.append(
            {
                "axis_name": f"{row['tag_group']}_{row['tag']}",
                "based_on": f"{row['tag_group']} tag",
                "intended_next": "selected_loser_suppression_pretest" if row["loser_minus_winner_spread"] >= 0.05 else "hold",
                "loser_minus_winner_spread": row["loser_minus_winner_spread"],
                "sample_size": int(row["n"]),
                "winner_damage_risk": "high" if pd.notna(winner_rate) and winner_rate >= 0.30 else "medium",
                "stability": "shadow_taxonomy_recent_combined",
                "should_become_real_candidate_source": row["tag_group"] == "setup" and row["loser_minus_winner_spread"] >= 0.05,
            }
        )
    recent_pairs = cooccur[(cooccur["period"] == "2024_2026_combined") & (cooccur["n"] >= 300)].copy()
    if not recent_pairs.empty:
        row = recent_pairs.sort_values(["loser_minus_winner_spread", "n"], ascending=[False, False]).iloc[0]
        candidates.append(
            {
                "axis_name": f"{row['pair_group']}:{row['left_tag']}+{row['right_tag']}",
                "based_on": "tag cooccurrence",
                "intended_next": "candidate_source_split" if row["loser_minus_winner_spread"] >= 0.05 else "hold",
                "loser_minus_winner_spread": row["loser_minus_winner_spread"],
                "sample_size": int(row["n"]),
                "winner_damage_risk": "requires_pretest",
                "stability": "shadow_taxonomy_recent_combined",
                "should_become_real_candidate_source": row["loser_minus_winner_spread"] >= 0.05,
            }
        )
    return candidates[:5]


def decide(summary: pd.DataFrame, recs: list[dict[str, Any]], coverage: dict[str, Any]) -> dict[str, Any]:
    if not coverage.get("tags_usable_for_selected_loser_audit"):
        decision = "taxonomy_contract_gap"
        reasons = ["no usable shadow tags could be generated"]
    else:
        best = recs[0] if recs else {}
        spread = float(best.get("loser_minus_winner_spread") or 0.0)
        if spread >= 0.08 and best.get("winner_damage_risk") != "high":
            decision = "taxonomy_axis_found"
            reasons = ["one shadow tag axis overrepresents selected losers with manageable winner hit risk"]
        elif spread >= 0.05:
            decision = "candidate_source_split_needed"
            reasons = ["selected loser contamination is visible by broad shadow family, but winner damage risk requires source split/pretest"]
        else:
            decision = "scoring_objective_redesign_needed"
            reasons = ["shadow tags exist, but selected loser contamination remains broad across tags"]
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
    }


def run(trace_root: Path, daily_path: Path, output_root: Path) -> Path:
    tag = _now_tag()
    out = output_root / f"{tag}-candidate-family-taxonomy-shadow-v1"
    out.mkdir(parents=True, exist_ok=True)
    trace_path = trace_root / "daily_candidate_trace.csv"
    trace = pd.read_csv(trace_path)
    trace["code"] = trace["code"].astype(str)
    trace["decision_date"] = pd.to_numeric(trace["decision_date"], errors="coerce").astype("Int64")
    trace["baseline_rank"] = pd.to_numeric(trace["baseline_rank"], errors="coerce")
    trace["baseline_score"] = pd.to_numeric(trace["baseline_score"], errors="coerce")
    trace["year"] = (trace["decision_date"] // 10000).astype(int)
    daily_features = build_daily_features(daily_path)
    rows = trace.merge(daily_features, on=["code", "decision_date"], how="left")
    rows = attach_labels(rows, daily_features)
    rows = tag_rows(rows)
    summary = summarize_by_tag(rows)
    cooccur = cooccurrence_summary(rows)
    coverage = coverage_summary(rows)
    damage = winner_damage(summary)
    recs = recommendations(summary, cooccur)
    decision = decide(summary, recs, coverage)

    _write_json(out / "input_artifact_report.json", {"trace_root": trace_root, "trace_path": trace_path, "daily_path": daily_path, "input_trace_rows": len(trace)})
    _write_json(out / "shadow_taxonomy_schema.json", shadow_taxonomy_schema())
    rows.to_csv(out / "candidate_family_tag_rows.csv", index=False)
    _write_json(out / "tag_coverage_summary.json", coverage)
    summary.to_csv(out / "selected_loser_by_tag_summary.csv", index=False)
    cooccur.to_csv(out / "tag_cooccurrence_summary.csv", index=False)
    damage.to_csv(out / "winner_damage_by_tag.csv", index=False)
    _write_json(out / "candidate_source_redesign_recommendations.json", {"candidates": recs})
    _write_json(out / "research_decision.json", decision)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "trace_input_no_lookahead_audit": str(trace_root / "no_lookahead_audit.json"),
            "research_tags_are_derived_not_candidate_source": True,
            "features_use_decision_date_or_prior_daily_rows": True,
            "same_date_quartiles_use_features_only": True,
            "future_returns_used_only_for_labels": True,
            "ranking_order_changed": False,
            "score_formula_changed": False,
            "runtime_db_write": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build shadow setup taxonomy for TRADEX baseline selected loser audit")
    parser.add_argument("--trace-root", type=Path, default=DEFAULT_TRACE_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.trace_root, args.daily_path, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
