from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from research.bridge import export_bridge_decision_signal_prior
from research.storage import ResearchPaths, write_json
from scripts.monthly_box_breakout_research import (
    _build_event_frame as _build_monthly_box_event_frame,
    _build_failed_breakout_events,
    _build_phase_masks as _build_monthly_box_phase_masks,
    _prepare_frame as _prepare_monthly_box_frame,
)
from scripts.note_trade_repro_backtest import ROUND_TRIP_COST, _classify_path_quality


SCHEMA_VERSION = "meemee_decision_signal_prior_v1"
STRATEGY_ID = "meemee_decision_signal_prior_v1"
PATH_HORIZONS = (10, 20)
SIDE_UP = "up"
SIDE_DOWN = "down"
CURRENT_SIGNAL_LIMIT = 40
STAGE_CAPS: dict[str, float] = {
    "observe": 0.0,
    "assist": 0.01,
    "weighted": 0.02,
    "core": 0.03,
}


@dataclass(frozen=True)
class FamilySpec:
    family: str
    side: str
    polarity: int
    build_mask: Callable[[pd.DataFrame, dict[str, pd.Series]], pd.Series]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return float(numeric)


def _profit_factor(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna().to_numpy(dtype=np.float64, copy=False)
    if values.size == 0:
        return None
    gains = values[values > 0.0].sum()
    losses = -values[values < 0.0].sum()
    if losses > 0.0:
        return float(gains / losses)
    if gains > 0.0:
        return float("inf")
    return None


def _normalize_date(value: Any) -> str | None:
    if value is None or value == "":
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%d")


def _dedupe_texts(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _build_short_forward_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    grouped = daily.groupby("code", sort=False)
    entry_next_open = daily.get("entry_next_open")
    if entry_next_open is None:
        entry_next_open = grouped["o"].shift(-1)
        daily["entry_next_open"] = entry_next_open
    for horizon in PATH_HORIZONS:
        exit_close = grouped["c"].shift(-(horizon + 1))
        high_shifts = [grouped["h"].shift(-step) for step in range(1, horizon + 1)]
        low_shifts = [grouped["l"].shift(-step) for step in range(1, horizon + 1)]
        future_high = pd.concat(high_shifts, axis=1).max(axis=1)
        future_low = pd.concat(low_shifts, axis=1).min(axis=1)
        daily[f"ret_short_{horizon}d"] = -((exit_close / entry_next_open) - 1.0) - ROUND_TRIP_COST
        daily[f"mfe_short_{horizon}d"] = -((future_low / entry_next_open) - 1.0)
        daily[f"mae_short_{horizon}d"] = -((future_high / entry_next_open) - 1.0)
    return daily


def _build_short_path_quality(daily: pd.DataFrame) -> pd.Series:
    clean_mask = daily["hit_dn5_before_up5_20d"] & (daily["mae_short_20d"] > -0.05) & (daily["ret_short_10d"] > 0.0)
    volatile_mask = daily["ret_short_10d"] > 0.0
    failed_mask = daily["hit_up5_before_dn5_20d"] | (daily["mae_short_20d"] <= -0.08)
    return pd.Series(
        np.select(
            [clean_mask, volatile_mask, failed_mask],
            ["clean_break", "volatile_win", "failed_fast"],
            default="stalled",
        ),
        index=daily.index,
        dtype="object",
    )


def _prepare_decision_frame(db_paths: list[Path]) -> tuple[pd.DataFrame, dict[str, pd.Series], pd.DataFrame]:
    daily = _prepare_monthly_box_frame(db_paths)
    daily = _build_short_forward_metrics(daily)
    grouped = daily.groupby("code", sort=False)
    daily["prev_ma20"] = grouped["ma20"].shift(1)
    daily["prev_ma60"] = grouped["ma60"].shift(1)
    daily["ret_prev20"] = (daily["c"] / grouped["c"].shift(20)) - 1.0
    daily["ret_prev40"] = (daily["c"] / grouped["c"].shift(40)) - 1.0
    daily["ret_prev60"] = (daily["c"] / grouped["c"].shift(60)) - 1.0
    daily["recent_high_20"] = grouped["h"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).max())
    daily["recent_low_20"] = grouped["l"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).min())
    daily["dist_recent_high20"] = np.where(
        daily["recent_high_20"].notna() & (daily["recent_high_20"] > 0.0),
        (daily["c"] / daily["recent_high_20"]) - 1.0,
        np.nan,
    )
    daily["trend_up_context"] = (
        daily["ret_prev40"].fillna(0.0).ge(0.12)
        & daily["prev_c"].notna()
        & daily["prev_ma20"].notna()
        & (daily["prev_c"] >= daily["prev_ma20"] * 0.995)
        & daily["day_pos_ma60"].eq("above60")
    )
    daily["distribution_context"] = (
        daily["ret_prev60"].fillna(0.0).ge(0.20)
        & (
            daily["recent_breakout_15d"]
            | daily["dist_recent_high20"].fillna(0.0).ge(-0.06)
            | daily["box_zone"].isin(["upper", "breakout"])
        )
    )
    daily["month_key"] = daily["dt"].dt.strftime("%Y-%m")
    daily["long_path_quality"] = _classify_path_quality(daily)
    daily["short_path_quality"] = _build_short_path_quality(daily)
    phase_masks = _build_monthly_box_phase_masks(daily)
    monthly_box_events = _build_monthly_box_event_frame(daily, phase_masks)
    return daily, phase_masks, monthly_box_events


def _phase_start_mask(raw: pd.Series, codes: pd.Series) -> pd.Series:
    prev = raw.groupby(codes).shift(1)
    prev = prev.astype("boolean").fillna(False).astype(bool)
    return raw.astype(bool) & (~prev)


def _sell_to_buy_transition_mask(daily: pd.DataFrame, phase_masks: dict[str, pd.Series]) -> pd.Series:
    base = phase_masks.get("bottom_entry", pd.Series(False, index=daily.index))
    return (
        base
        & daily["ret_prev20"].fillna(0.0).le(-0.08)
        & daily["daily_ma20_reclaim"]
        & daily["weekly_context"].isin(["up_support_intact", "flat_support_intact"])
    )


def _broken_bullish_scenario_mask(daily: pd.DataFrame, _phase_masks: dict[str, pd.Series]) -> pd.Series:
    raw = (
        daily["trend_up_context"]
        & daily["support_break_day"]
        & (daily["c"] < daily["ma20"] * 0.995)
        & (~daily["week_support_hold"] | daily["week_slope"].ne("up"))
    )
    return _phase_start_mask(raw, daily["code"])


def _top_distribution_break_mask(daily: pd.DataFrame, _phase_masks: dict[str, pd.Series]) -> pd.Series:
    raw = (
        daily["distribution_context"]
        & daily["support_break_day"]
        & daily["bar_tag"].fillna("").str.endswith("LB")
        & daily["vol_bucket"].isin(["mid", "surge"])
    )
    return _phase_start_mask(raw, daily["code"])


def _ma20_break_continuation_mask(daily: pd.DataFrame, _phase_masks: dict[str, pd.Series]) -> pd.Series:
    raw = (
        daily["support_break_day"]
        & daily["prev_ma20"].notna()
        & daily["prev_c"].notna()
        & (daily["prev_c"] >= daily["prev_ma20"] * 0.995)
        & daily["atr_bucket"].isin(["mid", "high"])
    )
    return _phase_start_mask(raw, daily["code"])


def _monthly_box_lower_break_mask(daily: pd.DataFrame, _phase_masks: dict[str, pd.Series]) -> pd.Series:
    raw = (
        daily["box_active"]
        & daily["box_upper"].notna()
        & daily["box_lower"].notna()
        & (daily["c"] < daily["box_lower"] * 0.99)
        & (~daily["week_support_hold"])
    )
    return _phase_start_mask(raw, daily["code"])


FAMILY_SPECS: tuple[FamilySpec, ...] = (
    FamilySpec("monthly_box_bottom", SIDE_UP, 1, lambda daily, masks: masks.get("bottom_entry", pd.Series(False, index=daily.index))),
    FamilySpec("monthly_box_breakout", SIDE_UP, 1, lambda daily, masks: masks.get("breakout_entry", pd.Series(False, index=daily.index))),
    FamilySpec("failed_breakout_avoid", SIDE_UP, -1, lambda daily, masks: pd.Series(False, index=daily.index)),
    FamilySpec("sell_to_buy_transition", SIDE_UP, 1, _sell_to_buy_transition_mask),
    FamilySpec("broken_bullish_scenario", SIDE_DOWN, 1, _broken_bullish_scenario_mask),
    FamilySpec("top_distribution_break", SIDE_DOWN, 1, _top_distribution_break_mask),
    FamilySpec("ma20_break_continuation", SIDE_DOWN, 1, _ma20_break_continuation_mask),
    FamilySpec("monthly_box_lower_break", SIDE_DOWN, 1, _monthly_box_lower_break_mask),
)


def _stage_from_stats(row: dict[str, Any]) -> str:
    n = int(row.get("sample_n") or 0)
    months = int(row.get("months_covered") or 0)
    expectancy = _safe_float(row.get("expectancy_20d")) or 0.0
    pf = _safe_float(row.get("profit_factor_20d")) or 0.0
    positive_ratio = _safe_float(row.get("positive_window_ratio")) or 0.0
    worst_mae = _safe_float(row.get("mae_worst_gate")) or -1.0
    stability = _safe_float(row.get("by_period_stability")) or 0.0
    concentration = _safe_float(row.get("top_symbol_concentration")) or 1.0

    if (
        n >= 50
        and months >= 12
        and expectancy >= 0.015
        and pf >= 1.25
        and positive_ratio >= 0.58
        and worst_mae >= -0.12
        and stability >= 0.67
        and concentration <= 0.25
    ):
        return "core"
    if (
        n >= 30
        and months >= 9
        and expectancy >= 0.01
        and pf >= 1.15
        and positive_ratio >= 0.55
        and worst_mae >= -0.15
        and stability >= 0.50
        and concentration <= 0.30
    ):
        return "weighted"
    if (
        n >= 15
        and months >= 6
        and expectancy > 0.0
        and pf >= 1.05
        and positive_ratio >= 0.52
        and worst_mae >= -0.18
        and concentration <= 0.40
    ):
        return "assist"
    return "observe"


def _family_reasons(row: pd.Series, family: str) -> list[str]:
    reasons: list[str] = []
    if family.startswith("monthly_box"):
        reasons.append("月足box文脈")
    if row.get("weekly_context") == "up_support_intact":
        reasons.append("週足support維持")
    if row.get("weekly_context") == "support_broken":
        reasons.append("週足support破綻")
    if bool(row.get("daily_ma20_reclaim")):
        reasons.append("日足20MA回復")
    pattern = str(row.get("daily_pattern_2") or row.get("pattern_2") or "").strip()
    if pattern:
        reasons.append(f"足型:{pattern}")
    if family == "failed_breakout_avoid":
        reasons.append("上抜け失敗回避")
    if family in {"broken_bullish_scenario", "top_distribution_break", "ma20_break_continuation"}:
        reasons.append("上昇シナリオ破綻")
    return _dedupe_texts(reasons)


def _family_risk_watch(row: pd.Series, family: str) -> list[str]:
    risks: list[str] = []
    if str(row.get("vol_bucket") or "") == "dry":
        risks.append("出来高弱い")
    if str(row.get("atr_bucket") or "") == "high":
        risks.append("値幅荒い")
    if bool(row.get("week_climactic")):
        risks.append("週足過熱")
    if family == "failed_breakout_avoid":
        risks.append("再度box内へ逆戻り")
    if family == "sell_to_buy_transition":
        risks.append("転換初動でダマシ余地")
    return _dedupe_texts(risks)


def _calc_row_signal_strength(row: pd.Series, family: str, side: str) -> float:
    score = 0.35
    if side == SIDE_UP:
        if row.get("box_zone") in {"lower", "mid"}:
            score += 0.12
        if row.get("box_zone") in {"upper", "breakout"}:
            score += 0.14
        if bool(row.get("daily_ma20_reclaim")):
            score += 0.14
        if bool(row.get("week_support_hold")):
            score += 0.10
        if str(row.get("vol_bucket") or "") == "surge":
            score += 0.08
        if bool(row.get("climactic_day")) and family != "monthly_box_breakout":
            score -= 0.10
    else:
        if bool(row.get("support_break_day")):
            score += 0.18
        if str(row.get("bar_tag") or "").endswith("LB"):
            score += 0.12
        if "GD" in str(row.get("bar_tag") or ""):
            score += 0.10
        if not bool(row.get("week_support_hold", True)):
            score += 0.12
        if str(row.get("vol_bucket") or "") == "surge":
            score += 0.08
    if family == "failed_breakout_avoid":
        score += 0.08
    return float(max(0.0, min(1.0, score)))


def _calc_row_fit_score(row: pd.Series, family: str) -> float:
    score = 0.45
    if family == "monthly_box_bottom":
        score += 0.20 if row.get("entry_style") == "bottom_ma20_reclaim" else 0.08
    elif family == "monthly_box_breakout":
        score += 0.18 if row.get("entry_style") == "breakout_gap_hb" else 0.10
    elif family == "failed_breakout_avoid":
        score += 0.20 if str(row.get("failure_reason") or "") in {"support_break_after_breakout", "reentry_into_box"} else 0.08
    elif family == "sell_to_buy_transition":
        score += 0.18 if bool(row.get("daily_ma20_reclaim")) else 0.08
    elif family == "broken_bullish_scenario":
        score += 0.15 if bool(row.get("trend_up_context")) else 0.05
    elif family == "top_distribution_break":
        score += 0.18 if bool(row.get("distribution_context")) else 0.06
    elif family == "ma20_break_continuation":
        score += 0.16 if bool(row.get("support_break_day")) else 0.05
    elif family == "monthly_box_lower_break":
        score += 0.18 if bool(row.get("box_active")) else 0.05
    if str(row.get("vol_bucket") or "") == "surge":
        score += 0.06
    if str(row.get("atr_bucket") or "") == "high":
        score -= 0.04
    return float(max(0.0, min(1.0, score)))


def _build_family_events(
    daily: pd.DataFrame,
    phase_masks: dict[str, pd.Series],
    monthly_box_events: pd.DataFrame,
) -> pd.DataFrame:
    base_columns = [col for col in daily.columns]
    events: list[pd.DataFrame] = []

    long_event_map = {
        "monthly_box_bottom": monthly_box_events.loc[monthly_box_events["phase"].eq("bottom_entry")].copy(),
        "monthly_box_breakout": monthly_box_events.loc[monthly_box_events["phase"].eq("breakout_entry")].copy(),
        "failed_breakout_avoid": _build_failed_breakout_events(daily, phase_masks["breakout_entry"]).copy(),
    }
    if not long_event_map["failed_breakout_avoid"].empty:
        long_event_map["failed_breakout_avoid"]["phase"] = "failed_breakout_exit"

    for spec in FAMILY_SPECS:
        if spec.family in long_event_map:
            frame = long_event_map[spec.family].copy()
        else:
            mask = spec.build_mask(daily, phase_masks)
            frame = daily.loc[mask].copy()
        if frame.empty:
            continue
        for col in base_columns:
            if col in frame.columns:
                continue
            if len(frame.index) > 0 and set(frame.index).issubset(set(daily.index)):
                frame[col] = daily.loc[frame.index, col]
            else:
                frame[col] = None
        frame["family"] = spec.family
        frame["side"] = spec.side
        frame["polarity"] = int(spec.polarity)
        if spec.side == SIDE_UP:
            frame["target_ret_10d"] = frame["ret_long_10d"] * int(spec.polarity)
            frame["target_ret_20d"] = frame["ret_long_20d"] * int(spec.polarity)
            frame["target_mfe_20d"] = frame["mfe_20d"] * int(spec.polarity) if int(spec.polarity) < 0 else frame["mfe_20d"]
            frame["target_mae_20d"] = frame["mae_20d"]
            frame["path_quality"] = (
                frame["long_path_quality"]
                if int(spec.polarity) > 0
                else np.where(frame["hit_dn5_before_up5_20d"], "good_avoid", "weak_avoid")
            )
        else:
            frame["target_ret_10d"] = frame["ret_short_10d"]
            frame["target_ret_20d"] = frame["ret_short_20d"]
            frame["target_mfe_20d"] = frame["mfe_short_20d"]
            frame["target_mae_20d"] = frame["mae_short_20d"]
            frame["path_quality"] = frame["short_path_quality"]
        frame["pattern_tag"] = frame.get("daily_pattern_2", frame.get("pattern_2"))
        frame["cluster_key"] = (
            frame["family"].astype(str)
            + "|"
            + frame["weekly_context"].astype(str)
            + "|"
            + frame["daily_pattern_2"].astype(str)
            + "|"
            + frame["dist_bucket"].astype(str)
        )
        frame["regime_key"] = (
            frame["period_bucket"].astype(str)
            + "|wk_"
            + frame["week_slope"].astype(str)
            + "|vol_"
            + frame["vol_bucket"].astype(str)
        )
        frame["signal_strength"] = [_calc_row_signal_strength(row, spec.family, spec.side) for _, row in frame.iterrows()]
        frame["fit_score"] = [_calc_row_fit_score(row, spec.family) for _, row in frame.iterrows()]
        frame["decision_reasons"] = [_family_reasons(row, spec.family) for _, row in frame.iterrows()]
        frame["risk_watch"] = [_family_risk_watch(row, spec.family) for _, row in frame.iterrows()]
        events.append(frame)

    if not events:
        return pd.DataFrame(columns=[*base_columns, "family", "side", "polarity"])
    out = pd.concat(events, ignore_index=True, sort=False)
    out["signal_date"] = pd.to_datetime(out["dt"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out


def _family_period_stability(frame: pd.DataFrame) -> float | None:
    work = frame.loc[frame["target_ret_20d"].notna()]
    if work.empty:
        return None
    grouped = work.groupby("period_bucket", dropna=False)["target_ret_20d"].mean()
    valid = grouped.dropna()
    if valid.empty:
        return None
    return float(np.mean(valid > 0.0))


def _family_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family, group in events.groupby("family", sort=False):
        work = group.loc[group["target_ret_20d"].notna()].copy()
        if work.empty:
            continue
        sample_n = int(len(work))
        months = int(work["month_key"].nunique())
        expectancy = float(work["target_ret_20d"].mean())
        pf = _profit_factor(work["target_ret_20d"])
        positive = float(np.mean(work["target_ret_20d"] > 0.0))
        mae_p10 = _safe_float(work["target_mae_20d"].quantile(0.10))
        stability = _family_period_stability(work)
        concentration = float(work["code"].value_counts(normalize=True).iloc[0]) if not work.empty else None
        row = {
            "family": str(family),
            "side": str(work["side"].iloc[0]),
            "polarity": int(work["polarity"].iloc[0]),
            "sample_n": sample_n,
            "months_covered": months,
            "expectancy_10d": float(work["target_ret_10d"].mean()),
            "expectancy_20d": expectancy,
            "profit_factor_20d": pf,
            "positive_window_ratio": positive,
            "mae_worst_gate": mae_p10,
            "by_period_stability": stability,
            "top_symbol_concentration": concentration,
            "cluster_key": str(work["cluster_key"].mode().iloc[0]) if not work["cluster_key"].mode().empty else None,
            "regime_key": str(work["regime_key"].mode().iloc[0]) if not work["regime_key"].mode().empty else None,
        }
        row["promotion_stage"] = _stage_from_stats(row)
        row["bonus_cap"] = STAGE_CAPS[row["promotion_stage"]]
        rows.append(row)
    rows.sort(
        key=lambda row: (
            STAGE_CAPS.get(str(row["promotion_stage"]), 0.0),
            row.get("expectancy_20d") or 0.0,
            row.get("sample_n") or 0,
        ),
        reverse=True,
    )
    return rows


def _rank_side_payload(current: pd.DataFrame, *, provisional: bool) -> dict[str, Any]:
    if current.empty:
        return {
            "asof": None,
            "codes": [],
            "rank_map": {},
            "fit_score_map": {},
            "signal_strength_map": {},
            "pattern_tag_map": {},
            "decision_reason_map": {},
            "adoption_reason_map": {},
            "risk_watch_map": {},
            "promotion_stage_map": {},
            "provisional_map": {},
            "hypothesis_family_map": {},
            "bonus_map": {},
            "bonus_cap": max(STAGE_CAPS.values()),
        }

    rows: list[dict[str, Any]] = []
    for code, group in current.groupby("code", sort=False):
        ordered = group.sort_values(["bonus_abs", "signal_strength", "fit_score"], ascending=[False, False, False]).reset_index(drop=True)
        leader = ordered.iloc[0]
        bonus_total = float(np.clip(ordered["signed_bonus"].sum(), -max(STAGE_CAPS.values()), max(STAGE_CAPS.values())))
        decision_reasons = _dedupe_texts([reason for reasons in ordered["decision_reasons"] for reason in reasons])
        risk_watch = _dedupe_texts([reason for reasons in ordered["risk_watch"] for reason in reasons])
        rows.append(
            {
                "code": str(code),
                "rank_score": float((leader["signal_strength"] * 0.55) + (leader["fit_score"] * 0.35) + (abs(bonus_total) * 5.0)),
                "fit_score": float(leader["fit_score"]),
                "signal_strength": float(leader["signal_strength"]),
                "pattern_tag": str(leader.get("pattern_tag") or "").strip() or None,
                "decision_reasons": decision_reasons,
                "risk_watch": risk_watch,
                "promotion_stage": str(leader["promotion_stage"]),
                "provisional": bool(provisional),
                "hypothesis_family": str(leader["family"]),
                "bonus": bonus_total,
            }
        )
    rows.sort(key=lambda row: (row["rank_score"], abs(row["bonus"]), row["signal_strength"]), reverse=True)
    rows = rows[:CURRENT_SIGNAL_LIMIT]
    codes = [row["code"] for row in rows]
    return {
        "asof": _normalize_date(current["signal_date"].iloc[0]),
        "codes": codes,
        "rank_map": {row["code"]: idx + 1 for idx, row in enumerate(rows)},
        "fit_score_map": {row["code"]: row["fit_score"] for row in rows},
        "signal_strength_map": {row["code"]: row["signal_strength"] for row in rows},
        "pattern_tag_map": {row["code"]: row["pattern_tag"] for row in rows if row["pattern_tag"]},
        "decision_reason_map": {row["code"]: row["decision_reasons"] for row in rows if row["decision_reasons"]},
        "adoption_reason_map": {row["code"]: row["decision_reasons"] for row in rows if row["decision_reasons"]},
        "risk_watch_map": {row["code"]: row["risk_watch"] for row in rows if row["risk_watch"]},
        "promotion_stage_map": {row["code"]: row["promotion_stage"] for row in rows},
        "provisional_map": {row["code"]: row["provisional"] for row in rows},
        "hypothesis_family_map": {row["code"]: row["hypothesis_family"] for row in rows},
        "bonus_map": {row["code"]: row["bonus"] for row in rows},
        "bonus_cap": max(STAGE_CAPS.values()),
    }


def _build_current_signals(
    events: pd.DataFrame,
    family_stats: list[dict[str, Any]],
    *,
    asof: str | None,
    provisional: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if events.empty:
        return events.copy(), events.copy()
    target_asof = str(asof).strip() if asof else str(events["signal_date"].max())
    current = events.loc[events["signal_date"].eq(target_asof)].copy()
    if current.empty:
        return current, current
    stats_map = {str(row["family"]): row for row in family_stats}
    keep_rows: list[pd.Series] = []
    for _, row in current.iterrows():
        family = str(row["family"])
        stat = stats_map.get(family)
        if not stat:
            continue
        stage = str(stat["promotion_stage"])
        cap = float(stat["bonus_cap"])
        sign = int(row["polarity"])
        signal_strength = float(row["signal_strength"])
        fit_score = float(row["fit_score"])
        signed_bonus = float(sign * min(cap, cap * max(0.30, signal_strength) * max(0.30, fit_score)))
        if provisional:
            signed_bonus *= 0.5
        enriched = row.copy()
        enriched["promotion_stage"] = stage
        enriched["bonus_cap"] = cap
        enriched["signed_bonus"] = signed_bonus
        enriched["bonus_abs"] = abs(signed_bonus)
        keep_rows.append(enriched)
    if not keep_rows:
        return current.iloc[0:0].copy(), current.iloc[0:0].copy()
    current_enriched = pd.DataFrame(keep_rows)
    up = current_enriched.loc[current_enriched["side"].eq(SIDE_UP)].copy()
    down = current_enriched.loc[current_enriched["side"].eq(SIDE_DOWN)].copy()
    return up, down


def _build_summary_payload(
    family_stats: list[dict[str, Any]],
    events: pd.DataFrame,
    current_signals: pd.DataFrame,
) -> dict[str, Any]:
    leaderboard = [
        {
            "family": row["family"],
            "side": row["side"],
            "promotion_stage": row["promotion_stage"],
            "sample_n": row["sample_n"],
            "expectancy_20d": row["expectancy_20d"],
            "profit_factor_20d": row["profit_factor_20d"],
        }
        for row in family_stats[:12]
    ]
    worst_failure_patterns: list[dict[str, Any]] = []
    if not events.empty:
        failure = (
            events.loc[events["target_ret_20d"].notna()]
            .groupby(["family", "pattern_tag"], dropna=False)["target_ret_20d"]
            .agg(["size", "mean"])
            .reset_index()
        )
        failure = failure.loc[failure["size"] >= 5].sort_values(["mean", "size"], ascending=[True, False]).head(8)
        worst_failure_patterns = [
            {
                "family": str(row["family"]),
                "pattern_tag": str(row["pattern_tag"]) if row["pattern_tag"] is not None else None,
                "n": int(row["size"]),
                "mean_ret20d": float(row["mean"]),
            }
            for _, row in failure.iterrows()
        ]
    action_queue = []
    if not current_signals.empty:
        ordered = current_signals.sort_values(["bonus_abs", "signal_strength"], ascending=[False, False]).head(12)
        action_queue = [
            {
                "code": str(row["code"]),
                "side": str(row["side"]),
                "family": str(row["family"]),
                "promotion_stage": str(row["promotion_stage"]),
                "signal_strength": float(row["signal_strength"]),
                "fit_score": float(row["fit_score"]),
                "signed_bonus": float(row["signed_bonus"]),
            }
            for _, row in ordered.iterrows()
        ]
    promotion_audit = [
        {
            "family": row["family"],
            "promotion_stage": row["promotion_stage"],
            "bonus_cap": row["bonus_cap"],
            "sample_n": row["sample_n"],
            "months_covered": row["months_covered"],
            "positive_window_ratio": row["positive_window_ratio"],
            "by_period_stability": row["by_period_stability"],
            "top_symbol_concentration": row["top_symbol_concentration"],
        }
        for row in family_stats
    ]
    return {
        "family_leaderboard": leaderboard,
        "regime_leaderboard": [
            {"family": row["family"], "regime_key": row["regime_key"], "cluster_key": row["cluster_key"]}
            for row in family_stats[:12]
        ],
        "worst_failure_patterns": worst_failure_patterns,
        "top_decisive_patterns": [
            {"family": row["family"], "pattern_tag": row["cluster_key"], "expectancy_20d": row["expectancy_20d"]}
            for row in family_stats[:12]
        ],
        "action_queue": action_queue,
        "promotion_audit": promotion_audit,
        "risk_heavy_families": [
            {"family": row["family"], "mae_worst_gate": row["mae_worst_gate"]}
            for row in sorted(family_stats, key=lambda item: item.get("mae_worst_gate") or 0.0)[:8]
        ],
        "next_promotion_candidates": [
            {"family": row["family"], "promotion_stage": row["promotion_stage"], "sample_n": row["sample_n"]}
            for row in family_stats
            if row["promotion_stage"] in {"assist", "weighted"}
        ][:8],
    }


def build_decision_signal_prior(
    *,
    asof: str | None = None,
    provisional: bool = False,
    db_paths: list[Path],
) -> dict[str, Any]:
    daily, phase_masks, monthly_box_events = _prepare_decision_frame(db_paths)
    events = _build_family_events(daily, phase_masks, monthly_box_events)
    family_stats = _family_summary(events)
    up_current, down_current = _build_current_signals(events, family_stats, asof=asof, provisional=provisional)
    up_payload = _rank_side_payload(up_current, provisional=provisional)
    down_payload = _rank_side_payload(down_current, provisional=provisional)
    if not up_current.empty or not down_current.empty:
        current_signals = pd.concat([up_current, down_current], ignore_index=True)
    else:
        current_signals = events.iloc[0:0].copy()
    asof_value = up_payload["asof"] or down_payload["asof"] or (_normalize_date(asof) if asof else None)
    run_id = f"decision_signal_prior_{(asof_value or 'unknown').replace('-', '')}_{'provisional' if provisional else 'close'}"
    return {
        "schema_version": SCHEMA_VERSION,
        "strategy_id": STRATEGY_ID,
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "asof": asof_value,
        "provisional": bool(provisional),
        "source_dataset_id": ",".join([str(path) for path in db_paths]),
        "source_artifacts": {
            "seed_generators": [
                "monthly_box_breakout_research",
                "entry_invalidation_doten_study",
                "verify_sell_signal_loop",
            ]
        },
        "summary": _build_summary_payload(family_stats, events, current_signals),
        "up": up_payload,
        "down": down_payload,
    }


def run_decision_signal_prior(
    *,
    paths: ResearchPaths,
    asof: str | None = None,
    provisional: bool = False,
    db_paths: list[Path],
    output_json: Path | None = None,
    export_bridge: bool = True,
) -> dict[str, Any]:
    payload = build_decision_signal_prior(asof=asof, provisional=provisional, db_paths=db_paths)
    if output_json is not None:
        write_json(output_json, payload)
    bridge_result = None
    if export_bridge:
        bridge_result = export_bridge_decision_signal_prior(paths, payload=payload)
    return {
        "ok": True,
        "run_id": payload["run_id"],
        "asof": payload.get("asof"),
        "provisional": bool(payload.get("provisional")),
        "up_codes": len(payload.get("up", {}).get("codes", [])),
        "down_codes": len(payload.get("down", {}).get("codes", [])),
        "bridge": bridge_result,
        "payload_path": str(output_json) if output_json is not None else None,
    }


def _cli() -> int:
    import argparse
    from scripts.note_trade_repro_backtest import _resolve_default_db_paths

    parser = argparse.ArgumentParser(description="Build MeeMee decision signal prior snapshot")
    parser.add_argument("--asof", default=None)
    parser.add_argument("--provisional", action="store_true")
    parser.add_argument("--output-json", default=None)
    args = parser.parse_args()

    paths = ResearchPaths.build()
    db_paths = _resolve_default_db_paths()
    output_json = Path(args.output_json).resolve() if args.output_json else None
    result = run_decision_signal_prior(
        paths=paths,
        asof=args.asof,
        provisional=bool(args.provisional),
        db_paths=db_paths,
        output_json=output_json,
        export_bridge=True,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
