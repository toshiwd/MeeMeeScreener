from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "starter_candidate_chart_review_pack_v1"
DEFAULT_V3_ROOT = Path(r"G:\Tradex\starter_candidate_review_pack_v3\20260525T060441Z-starter-candidate-review-pack-v3")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_chart_review_pack_v1")

REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "chart_review_summary.json",
    "candidate_chart_review_rows.csv",
    "candidate_chart_review_cards.json",
    "daily_chart_context.csv",
    "weekly_chart_context.csv",
    "monthly_chart_context.csv",
    "family_checklist_result.json",
    "starter_promotion_judgment.json",
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


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _runtime_db_candidates() -> list[Path]:
    local = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
    return [
        local / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
        local / "MeeMeeScreener" / "data" / "stocks.duckdb",
        Path("data/stocks.duckdb"),
        Path("app/backend/stocks.duckdb"),
    ]


def _ymd_to_epoch(ymd: int) -> int:
    return int(pd.Timestamp(str(int(ymd)), tz="UTC").timestamp())


def _epoch_to_ymd(epoch: int | float) -> int:
    return int(pd.to_datetime(int(epoch), unit="s", utc=True).strftime("%Y%m%d"))


def select_confirmed_db(review_date: int) -> Path:
    target = _ymd_to_epoch(review_date)
    for path in _runtime_db_candidates():
        if not path.exists():
            continue
        con = duckdb.connect(str(path), read_only=True)
        try:
            row = con.execute(
                "SELECT max(date) FROM daily_bars WHERE source IN ('pan', 'txt', 'confirmed')"
            ).fetchone()
            if row and row[0] is not None and int(row[0]) >= target:
                return path
        finally:
            con.close()
    raise RuntimeError(f"no confirmed runtime daily_bars source covers {review_date}")


def load_bars(db_path: Path, codes: list[str], review_date: int) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE code IN ? AND date <= ? AND source IN ('pan', 'txt', 'confirmed')
            ORDER BY code, date
            """,
            [codes, _ymd_to_epoch(review_date)],
        ).df()
    finally:
        con.close()
    if df.empty:
        return df
    df["code"] = df["code"].astype(str)
    df["ymd"] = df["date"].map(_epoch_to_ymd)
    df["dt"] = pd.to_datetime(df["ymd"].astype(str), format="%Y%m%d")
    return df


def _slope(series: pd.Series, periods: int = 1) -> float | None:
    if len(series.dropna()) <= periods:
        return None
    cur = series.iloc[-1]
    prev = series.iloc[-1 - periods]
    if pd.isna(cur) or pd.isna(prev) or not prev:
        return None
    return float(cur / prev - 1)


def _ret(close: pd.Series, periods: int) -> float | None:
    if len(close) <= periods:
        return None
    prev = close.iloc[-1 - periods]
    if not prev:
        return None
    return float(close.iloc[-1] / prev - 1)


def _days_since_reclaim(close: pd.Series, ma: pd.Series) -> int | None:
    above = close > ma
    crossed = above & (~above.shift(1).fillna(False).astype(bool))
    idx = [i for i, ok in enumerate(crossed.tolist()) if ok]
    if not idx:
        return None
    return int(len(close) - 1 - idx[-1])


def daily_context_for(code: str, bars: pd.DataFrame, review_date: int) -> dict[str, Any]:
    g = bars[bars["code"].eq(code)].sort_values("date").tail(120).copy()
    if g.empty or int(g["ymd"].max()) < review_date:
        return {"code": code, "decision_date": review_date, "data_freshness_status": "missing"}
    g["ma7"] = g["c"].rolling(7).mean()
    g["ma20"] = g["c"].rolling(20).mean()
    g["ma60"] = g["c"].rolling(60).mean()
    g["vol_ma20"] = g["v"].rolling(20).mean()
    last = g.iloc[-1]
    prev = g.iloc[-2] if len(g) >= 2 else last
    rng = max(float(last["h"] - last["l"]), 1e-9)
    body = abs(float(last["c"] - last["o"]))
    upper = float((last["h"] - max(last["o"], last["c"])) / rng)
    lower = float((min(last["o"], last["c"]) - last["l"]) / rng)
    prev_close = float(prev["c"]) if prev is not None else float(last["c"])
    gap = float(last["o"] / prev_close - 1) if prev_close else None
    failed_high = bool(last["h"] < g["h"].tail(20).max() and last["c"] < last["o"])
    return {
        "code": code,
        "decision_date": review_date,
        "close": float(last["c"]),
        "ma7": float(last["ma7"]) if pd.notna(last["ma7"]) else None,
        "ma20": float(last["ma20"]) if pd.notna(last["ma20"]) else None,
        "ma60": float(last["ma60"]) if pd.notna(last["ma60"]) else None,
        "close_above_ma7": bool(last["c"] > last["ma7"]) if pd.notna(last["ma7"]) else False,
        "close_above_ma20": bool(last["c"] > last["ma20"]) if pd.notna(last["ma20"]) else False,
        "close_above_ma60": bool(last["c"] > last["ma60"]) if pd.notna(last["ma60"]) else False,
        "ma7_slope": _slope(g["ma7"], 1),
        "ma20_slope": _slope(g["ma20"], 1),
        "ma60_slope": _slope(g["ma60"], 1),
        "dist_ma7_pct": float(last["c"] / last["ma7"] - 1) if pd.notna(last["ma7"]) and last["ma7"] else None,
        "dist_ma20_pct": float(last["c"] / last["ma20"] - 1) if pd.notna(last["ma20"]) and last["ma20"] else None,
        "dist_ma60_pct": float(last["c"] / last["ma60"] - 1) if pd.notna(last["ma60"]) and last["ma60"] else None,
        "ret5": _ret(g["c"], 5),
        "ret10": _ret(g["c"], 10),
        "ret20": _ret(g["c"], 20),
        "upper_wick_ratio": upper,
        "lower_wick_ratio": lower,
        "large_bearish_candle": bool(last["c"] < last["o"] and body / rng >= 0.6),
        "large_bullish_candle": bool(last["c"] > last["o"] and body / rng >= 0.6),
        "failed_high": failed_high,
        "gap_pct": gap,
        "gap_up": bool(gap is not None and gap >= 0.02),
        "gap_down": bool(gap is not None and gap <= -0.02),
        "volume_ma20_ratio": float(last["v"] / last["vol_ma20"]) if pd.notna(last["vol_ma20"]) and last["vol_ma20"] else None,
        "volume_expansion": bool(pd.notna(last["vol_ma20"]) and last["vol_ma20"] and last["v"] / last["vol_ma20"] >= 1.2),
        "days_since_ma20_reclaim": _days_since_reclaim(g["c"], g["ma20"]),
        "data_freshness_status": "fresh",
        "bar_source": "confirmed",
    }


def _ohlcv_resample(g: pd.DataFrame, rule: str) -> pd.DataFrame:
    work = g.set_index("dt").sort_index()
    out = work.resample(rule).agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"})
    return out.dropna(subset=["o", "h", "l", "c"]).reset_index()


def timeframe_context_for(code: str, bars: pd.DataFrame, review_date: int, timeframe: str) -> dict[str, Any]:
    g = bars[bars["code"].eq(code)].sort_values("date").copy()
    if g.empty:
        return {"code": code, "decision_date": review_date, "timeframe": timeframe, "data_freshness_status": "missing"}
    rule = "W-FRI" if timeframe == "weekly" else "ME"
    tf = _ohlcv_resample(g, rule)
    if timeframe == "weekly":
        short, mid, long = 10, 30, 60
    else:
        short, mid, long = 6, 12, 24
    tf[f"ma{short}"] = tf["c"].rolling(short).mean()
    tf[f"ma{mid}"] = tf["c"].rolling(mid).mean()
    tf[f"ma{long}"] = tf["c"].rolling(long).mean()
    last = tf.iloc[-1]
    trend_up = bool(last[f"ma{short}"] > last[f"ma{mid}"] and _slope(tf[f"ma{mid}"], 1) is not None and (_slope(tf[f"ma{mid}"], 1) or 0) >= 0)
    high_zone = bool(last["c"] >= tf["c"].tail(12).max() * 0.95)
    pullback = bool(pd.notna(last[f"ma{mid}"]) and abs(last["c"] / last[f"ma{mid}"] - 1) <= 0.06)
    return {
        "code": code,
        "decision_date": review_date,
        "timeframe": timeframe,
        "close": float(last["c"]),
        f"close_above_ma{short}": bool(last["c"] > last[f"ma{short}"]) if pd.notna(last[f"ma{short}"]) else False,
        f"close_above_ma{mid}": bool(last["c"] > last[f"ma{mid}"]) if pd.notna(last[f"ma{mid}"]) else False,
        f"close_above_ma{long}": bool(last["c"] > last[f"ma{long}"]) if pd.notna(last[f"ma{long}"]) else False,
        f"ma{short}_slope": _slope(tf[f"ma{short}"], 1),
        f"ma{mid}_slope": _slope(tf[f"ma{mid}"], 1),
        f"ma{long}_slope": _slope(tf[f"ma{long}"], 1),
        "trend_direction": "up" if trend_up else "mixed_or_down",
        "high_zone_context": high_zone,
        "pullback_context": pullback,
        "overextension_risk": bool(pd.notna(last[f"ma{mid}"]) and last["c"] / last[f"ma{mid}"] - 1 >= 0.15),
        "data_freshness_status": "fresh",
        "bar_source": "confirmed",
    }


def judge_candidate(row: pd.Series, daily: dict[str, Any], weekly: dict[str, Any], monthly: dict[str, Any]) -> dict[str, Any]:
    family = str(row["research_candidate_source_family"])
    close = float(daily.get("close") or 0)
    ma7 = daily.get("ma7")
    ma20 = daily.get("ma20")
    failed = bool(daily.get("failed_high")) or bool(daily.get("large_bearish_candle"))
    upper = float(daily.get("upper_wick_ratio") or 0)
    vol_ratio = float(daily.get("volume_ma20_ratio") or 0)
    dist20 = float(daily.get("dist_ma20_pct") or 0)
    dist60 = float(daily.get("dist_ma60_pct") or 0)
    ma20_slope = float(daily.get("ma20_slope") or 0)
    weekly_up = weekly.get("trend_direction") == "up"
    monthly_up = monthly.get("trend_direction") == "up"
    starter_trigger = None
    invalidation = None
    judgment = "watch_continue"
    confidence = "medium"
    reason = []

    if family == "pullback_reclaim_source":
        starter_trigger = f"close holds above MA7/MA20 ({ma7:.2f}/{ma20:.2f})" if ma7 and ma20 else "MA7/MA20 reclaim confirmation"
        invalidation = f"close below MA20 ({ma20:.2f})" if ma20 else "close below MA20"
        if close < (ma20 or close):
            judgment, confidence = "avoid", "high"
            reason.append("close is below MA20")
        elif close > (ma7 or close + 1) and close > (ma20 or close + 1) and ma20_slope >= -0.002 and not failed and dist20 <= 0.10:
            judgment, confidence = "starter_ready", "medium"
            reason.append("close is above MA7/MA20 with no failed-high or bearish candle")
        elif abs(dist20) <= 0.06:
            judgment = "wait_for_trigger"
            reason.append("close is near MA20 but reclaim confirmation is incomplete")
        else:
            reason.append("pullback/reclaim context remains constructive but timing needs confirmation")
    elif family == "breakout_retest_source":
        starter_trigger = "retest hold above breakout zone with no upper-wick failure"
        invalidation = "failed breakout, failed high, or upper wick failure"
        if failed or upper >= 0.45:
            judgment, confidence = "avoid", "high"
            reason.append("failed high or upper-wick failure is present")
        elif dist20 <= 0.12 and vol_ratio >= 0.8 and weekly_up:
            judgment, confidence = "starter_ready", "medium"
            reason.append("breakout context has acceptable extension, volume, and weekly trend")
        elif dist20 > 0.12:
            judgment = "wait_for_trigger"
            reason.append("breakout is valid but extended; wait for retest")
        else:
            reason.append("breakout context is valid but retest hold remains manual")
    elif family == "early_trend_source":
        starter_trigger = "MA7/MA20 constructive structure stays intact"
        invalidation = f"close below MA20 ({ma20:.2f})" if ma20 else "close below key moving average"
        if close < (ma20 or close):
            judgment, confidence = "avoid", "high"
            reason.append("close is below key moving average support")
        elif close > (ma7 or close + 1) and close > (ma20 or close + 1) and dist20 <= 0.12 and weekly_up:
            judgment, confidence = "starter_ready", "medium"
            reason.append("early trend has constructive daily/weekly MA structure")
        else:
            judgment = "wait_for_trigger"
            reason.append("early trend is emerging but confirmation is thin")
    elif family == "overextension_risk_source":
        starter_trigger = "only after pullback/consolidation reduces MA20/MA60 extension"
        invalidation = "failed high, upper wick, or extension acceleration"
        if failed or upper >= 0.45 or dist20 >= 0.25 or dist60 >= 0.40:
            judgment, confidence = "avoid", "medium"
            reason.append("overextension risk is too high for starter review")
        else:
            judgment = "wait_for_trigger"
            reason.append("overextension family defaults to wait until risk cools")
    else:
        starter_trigger = "family setup confirmation"
        invalidation = "loss of key MA support or failed setup"
        reason.append("family-specific rule unavailable")

    if monthly_up:
        reason.append("monthly context is supportive")
    else:
        reason.append("monthly context is not clearly supportive")
    return {
        "manual_judgment": judgment,
        "judgment_confidence": confidence,
        "reason_summary": "; ".join(reason),
        "next_manual_check": starter_trigger,
        "invalidation_level": invalidation,
        "starter_trigger_level": starter_trigger,
        "data_freshness_status": daily.get("data_freshness_status"),
    }


def build_pack(v3_root: Path, output_root: Path, db_path: Path | None = None) -> Path:
    out = output_root / f"{_now_tag()}-starter-candidate-chart-review-pack-v1"
    out.mkdir(parents=True, exist_ok=True)
    v3_summary = json.loads((v3_root / "review_pack_summary.json").read_text(encoding="utf-8"))
    v3_decision = json.loads((v3_root / "review_pack_decision.json").read_text(encoding="utf-8"))
    candidates = pd.read_csv(v3_root / "review_candidate_rows.csv", low_memory=False)
    watch = candidates[candidates["candidate_action_class"].eq("watch")].copy()
    review_date = int(v3_summary["review_date"])
    codes = watch["code"].astype(str).tolist()
    selected_db = db_path or select_confirmed_db(review_date)
    bars = load_bars(selected_db, codes, review_date)

    daily_rows = [daily_context_for(code, bars, review_date) for code in codes]
    weekly_rows = [timeframe_context_for(code, bars, review_date, "weekly") for code in codes]
    monthly_rows = [timeframe_context_for(code, bars, review_date, "monthly") for code in codes]
    daily_df = pd.DataFrame(daily_rows)
    weekly_df = pd.DataFrame(weekly_rows)
    monthly_df = pd.DataFrame(monthly_rows)
    daily_df.to_csv(out / "daily_chart_context.csv", index=False)
    weekly_df.to_csv(out / "weekly_chart_context.csv", index=False)
    monthly_df.to_csv(out / "monthly_chart_context.csv", index=False)

    judgments = []
    checklist_results: dict[str, Any] = {}
    for _, row in watch.iterrows():
        code = str(row["code"])
        daily = daily_df[daily_df["code"].astype(str).eq(code)].iloc[0].to_dict()
        weekly = weekly_df[weekly_df["code"].astype(str).eq(code)].iloc[0].to_dict()
        monthly = monthly_df[monthly_df["code"].astype(str).eq(code)].iloc[0].to_dict()
        judgment = judge_candidate(row, daily, weekly, monthly)
        checklist_status = "pass" if judgment["manual_judgment"] == "starter_ready" else ("fail" if judgment["manual_judgment"] == "avoid" else "partial")
        checklist_results[code] = {"family": row["research_candidate_source_family"], "checklist_result": checklist_status, **judgment}
        judgments.append({**row.to_dict(), **judgment, "family_checklist_result": checklist_status})
    review_rows = pd.DataFrame(judgments)
    review_rows.to_csv(out / "candidate_chart_review_rows.csv", index=False)

    cards = []
    for row in review_rows.to_dict("records"):
        cards.append(
            {
                "code": str(row["code"]),
                "family": row["research_candidate_source_family"],
                "candidate_action_class": row["candidate_action_class"],
                "manual_judgment": row["manual_judgment"],
                "judgment_confidence": row["judgment_confidence"],
                "reason_summary": row["reason_summary"],
                "starter_trigger_level": row["starter_trigger_level"],
                "invalidation_level": row["invalidation_level"],
                "next_manual_check": row["next_manual_check"],
                "data_freshness_status": row["data_freshness_status"],
            }
        )
    _write_json(out / "candidate_chart_review_cards.json", cards)
    _write_json(out / "family_checklist_result.json", checklist_results)
    counts = {k: int((review_rows["manual_judgment"] == k).sum()) for k in ["starter_ready", "watch_continue", "wait_for_trigger", "avoid"]}
    decision = {
        **{f"{k}_count": v for k, v in counts.items()},
        "validated_buy_count": 0,
        "manual_review_available": bool(v3_decision.get("manual_review_available")),
        "meemee_reflectable_candidate": False,
        "blocker": "manual review only; no keep-gated validated challenger",
        "runtime_db_write": False,
        "production_ranking_changed": False,
    }
    _write_json(out / "starter_promotion_judgment.json", decision)
    _write_json(
        out / "chart_review_summary.json",
        {
            "axis_id": AXIS_ID,
            "review_date": review_date,
            "candidate_count": int(len(review_rows)),
            "manual_judgment_counts": counts,
            "confirmed_source_only": True,
            "runtime_db_path": str(selected_db),
            "manual_review_available": bool(v3_decision.get("manual_review_available")),
            "validated_buy_count": 0,
        },
    )
    _write_json(
        out / "input_artifact_report.json",
        {
            "v3_root": v3_root,
            "review_date": review_date,
            "latest_global_date": v3_summary.get("review_date"),
            "runtime_db_path": selected_db,
            "candidate_count": int(len(watch)),
            "bar_source": "confirmed",
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v3-root", type=Path, default=DEFAULT_V3_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(build_pack(args.v3_root, args.output_root, args.db_path))


if __name__ == "__main__":
    main()
