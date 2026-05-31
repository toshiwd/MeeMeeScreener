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
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_candidate_chart_review_outcome_audit_v1 as outcome_audit
from scripts import tradex_starter_ready_failure_decomposition_v1 as decomp


AXIS_ID = "starter_chart_context_enriched_replay_v1"
DEFAULT_REPLAY_ROOT = Path(
    r"G:\Tradex\starter_candidate_chart_review_historical_replay_v1\20260525T065259Z-starter-candidate-chart-review-historical-replay-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_chart_context_enriched_replay_v1")

REQUIRED_ARTIFACTS = (
    "enriched_replay_summary.json",
    "enriched_replay_rows.csv",
    "chart_context_column_contract.json",
    "missing_column_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "feature_only_signature_candidates.json",
    "feature_only_signature_metrics.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DAILY_COLUMNS = [
    "close_vs_ma7_pct",
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "ma7_slope_5d",
    "ma20_slope_10d",
    "ma60_slope_20d",
    "close_above_ma7",
    "close_above_ma20",
    "close_above_ma60",
    "ma7_above_ma20",
    "ma20_above_ma60",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "body_ratio",
    "bearish_body_flag",
    "bullish_body_flag",
    "failed_high_flag",
    "recent_high_distance_pct",
    "recent_low_distance_pct",
    "volume_vs_20d_avg",
    "gap_up_flag",
    "gap_down_flag",
]
WEEKLY_COLUMNS = [
    "weekly_close_vs_ma7_pct",
    "weekly_close_vs_ma20_pct",
    "weekly_ma7_slope",
    "weekly_ma20_slope",
    "weekly_trend_supportive_flag",
    "weekly_failed_high_flag",
]
MONTHLY_COLUMNS = [
    "monthly_close_vs_ma7_pct",
    "monthly_close_vs_ma20_pct",
    "monthly_ma7_slope",
    "monthly_ma20_slope",
    "monthly_supportive_flag",
    "monthly_box_position",
]
POINT_IN_TIME_SIGNATURE_COLUMNS = set(DAILY_COLUMNS + WEEKLY_COLUMNS + MONTHLY_COLUMNS + ["pattern_type"])
FORBIDDEN_SIGNATURE_TERMS = {"ret5", "ret10", "ret20", "trigger_hit", "invalidation_hit", "flat_or_negative", "good", "bad", "severe"}


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


def _ymd_to_epoch(ymd: int) -> int:
    return int(pd.Timestamp(str(int(ymd)), tz="UTC").timestamp())


def _epoch_to_ymd(epoch: int | float) -> int:
    return int(pd.to_datetime(int(epoch), unit="s", utc=True).strftime("%Y%m%d"))


def load_confirmed_bars(db_path: Path, rows: pd.DataFrame) -> pd.DataFrame:
    codes = sorted(rows["code"].astype(str).unique().tolist())
    min_ymd = int(rows["decision_date"].min())
    max_ymd = int(rows["decision_date"].max())
    min_epoch = _ymd_to_epoch(int((pd.Timestamp(str(min_ymd)) - pd.Timedelta(days=500)).strftime("%Y%m%d")))
    max_epoch = _ymd_to_epoch(max_ymd)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        bars = con.execute(
            """
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE code IN ? AND date >= ? AND date <= ? AND source IN ('pan', 'txt', 'confirmed')
            ORDER BY code, date
            """,
            [codes, min_epoch, max_epoch],
        ).df()
    finally:
        con.close()
    if bars.empty:
        return bars
    bars["code"] = bars["code"].astype(str)
    bars["ymd"] = bars["date"].map(_epoch_to_ymd)
    bars["dt"] = pd.to_datetime(bars["ymd"].astype(str), format="%Y%m%d")
    return bars


def pct(close: Any, base: Any) -> float | None:
    if pd.isna(close) or pd.isna(base) or not base:
        return None
    return float(close / base - 1)


def slope(series: pd.Series, periods: int) -> float | None:
    clean = series.dropna()
    if len(clean) <= periods:
        return None
    prev = clean.iloc[-1 - periods]
    cur = clean.iloc[-1]
    if not prev:
        return None
    return float(cur / prev - 1)


def daily_context(code: str, bars: pd.DataFrame, decision_date: int) -> dict[str, Any]:
    g = bars[(bars["code"].astype(str).eq(str(code))) & (bars["ymd"].le(decision_date))].sort_values("date").tail(120).copy()
    if g.empty or int(g["ymd"].max()) < decision_date:
        return {"chart_context_available": False}
    g["ma7"] = g["c"].rolling(7).mean()
    g["ma20"] = g["c"].rolling(20).mean()
    g["ma60"] = g["c"].rolling(60).mean()
    g["vol_ma20"] = g["v"].rolling(20).mean()
    last = g.iloc[-1]
    prev = g.iloc[-2] if len(g) >= 2 else last
    rng = max(float(last["h"] - last["l"]), 1e-9)
    body = abs(float(last["c"] - last["o"]))
    recent_high = float(g["h"].tail(20).max())
    recent_low = float(g["l"].tail(20).min())
    prev_close = float(prev["c"])
    gap = pct(float(last["o"]), prev_close)
    return {
        "chart_context_available": True,
        "close_vs_ma7_pct": pct(last["c"], last["ma7"]),
        "close_vs_ma20_pct": pct(last["c"], last["ma20"]),
        "close_vs_ma60_pct": pct(last["c"], last["ma60"]),
        "ma7_slope_5d": slope(g["ma7"], 5),
        "ma20_slope_10d": slope(g["ma20"], 10),
        "ma60_slope_20d": slope(g["ma60"], 20),
        "close_above_ma7": bool(pd.notna(last["ma7"]) and last["c"] > last["ma7"]),
        "close_above_ma20": bool(pd.notna(last["ma20"]) and last["c"] > last["ma20"]),
        "close_above_ma60": bool(pd.notna(last["ma60"]) and last["c"] > last["ma60"]),
        "ma7_above_ma20": bool(pd.notna(last["ma7"]) and pd.notna(last["ma20"]) and last["ma7"] > last["ma20"]),
        "ma20_above_ma60": bool(pd.notna(last["ma20"]) and pd.notna(last["ma60"]) and last["ma20"] > last["ma60"]),
        "upper_wick_ratio": float((last["h"] - max(last["o"], last["c"])) / rng),
        "lower_wick_ratio": float((min(last["o"], last["c"]) - last["l"]) / rng),
        "body_ratio": float(body / rng),
        "bearish_body_flag": bool(last["c"] < last["o"] and body / rng >= 0.45),
        "bullish_body_flag": bool(last["c"] > last["o"] and body / rng >= 0.45),
        "failed_high_flag": bool(last["h"] < recent_high and last["c"] < last["o"]),
        "recent_high_distance_pct": pct(last["c"], recent_high),
        "recent_low_distance_pct": pct(last["c"], recent_low),
        "volume_vs_20d_avg": pct(last["v"], last["vol_ma20"]),
        "gap_up_flag": bool(gap is not None and gap >= 0.02),
        "gap_down_flag": bool(gap is not None and gap <= -0.02),
    }


def resample_ohlcv(g: pd.DataFrame, rule: str) -> pd.DataFrame:
    return (
        g.set_index("dt")
        .sort_index()
        .resample(rule)
        .agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"})
        .dropna(subset=["o", "h", "l", "c"])
        .reset_index()
    )


def timeframe_context(code: str, bars: pd.DataFrame, decision_date: int, timeframe: str) -> dict[str, Any]:
    g = bars[(bars["code"].astype(str).eq(str(code))) & (bars["ymd"].le(decision_date))].sort_values("date").copy()
    prefix = "weekly" if timeframe == "weekly" else "monthly"
    if g.empty:
        return {}
    tf = resample_ohlcv(g, "W-FRI" if timeframe == "weekly" else "ME")
    if len(tf) < 8:
        return {}
    tf["ma7"] = tf["c"].rolling(7).mean()
    tf["ma20"] = tf["c"].rolling(20).mean()
    last = tf.iloc[-1]
    recent_high = float(tf["h"].tail(8).max())
    failed_high = bool(last["h"] < recent_high and last["c"] < last["o"])
    supportive = bool(pd.notna(last["ma7"]) and pd.notna(last["ma20"]) and last["c"] > last["ma7"] and last["ma7"] >= last["ma20"])
    result = {
        f"{prefix}_close_vs_ma7_pct": pct(last["c"], last["ma7"]),
        f"{prefix}_close_vs_ma20_pct": pct(last["c"], last["ma20"]),
        f"{prefix}_ma7_slope": slope(tf["ma7"], 1),
        f"{prefix}_ma20_slope": slope(tf["ma20"], 1),
    }
    if timeframe == "weekly":
        result["weekly_trend_supportive_flag"] = supportive
        result["weekly_failed_high_flag"] = failed_high
    else:
        result["monthly_supportive_flag"] = supportive
        result["monthly_box_position"] = pct(last["c"], recent_high)
    return result


def enrich_rows(rows: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    enriched: list[dict[str, Any]] = []
    for row in rows.to_dict("records"):
        code = str(row["code"])
        decision_date = int(row["decision_date"])
        ctx = {
            **daily_context(code, bars, decision_date),
            **timeframe_context(code, bars, decision_date, "weekly"),
            **timeframe_context(code, bars, decision_date, "monthly"),
        }
        ctx["pattern_type"] = decomp.pattern_type(row.get("research_candidate_source_family"))
        ctx["chart_review_label"] = row.get("manual_judgment")
        ctx["reconstructed_snapshot"] = True
        ctx["original_reason_text"] = row.get("reason_summary")
        enriched.append({**row, **ctx})
    return pd.DataFrame(enriched)


def feature_signatures_for_row(row: pd.Series) -> list[str]:
    pattern = str(row.get("pattern_type") or "unknown")
    sigs: list[str] = []
    dist20 = row.get("close_vs_ma20_pct")
    upper = row.get("upper_wick_ratio")
    if pattern == "breakout" and pd.notna(dist20) and float(dist20) >= 0.08:
        sigs.append("breakout+close_extended_vs_ma20")
    if pattern == "breakout" and pd.notna(upper) and float(upper) >= 0.35:
        sigs.append("breakout+upper_wick_high")
    if pattern == "pullback" and row.get("close_above_ma7") is False:
        sigs.append("pullback+close_below_ma7")
    if pattern == "pullback" and row.get("bearish_body_flag") is True:
        sigs.append("pullback+bearish_body")
    if row.get("monthly_supportive_flag") is True and row.get("failed_high_flag") is True:
        sigs.append("monthly_supportive+daily_failed_high")
    if row.get("weekly_trend_supportive_flag") is True and row.get("bearish_body_flag") is True:
        sigs.append("weekly_supportive+daily_bearish_body")
    if pattern == "early_trend" and row.get("close_above_ma20") is False:
        sigs.append("early_trend+close_below_ma20")
    return sigs


def metric_block(frame: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce").dropna()
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if "decision_date" in frame else 0,
        "code_count": int(frame["code"].astype(str).nunique()) if "code" in frame else 0,
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret10": _mean(frame, "ret10"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": float(ret20.median()) if not ret20.empty else None,
        "hit_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
    }


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def compare(tagged: pd.DataFrame, untagged: pd.DataFrame) -> dict[str, Any]:
    left = metric_block(tagged)
    right = metric_block(untagged)
    return {
        "tagged": left,
        "untagged": right,
        "mean_ret20_delta_tagged_minus_untagged": None if left["mean_ret20"] is None or right["mean_ret20"] is None else left["mean_ret20"] - right["mean_ret20"],
        "sample_allows_comparison": len(tagged) >= 10 and len(untagged) >= 10,
    }


def signature_metrics(rows: pd.DataFrame, signatures: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for sig in signatures:
        tagged = rows[rows["feature_only_signatures"].str.split("|").apply(lambda xs: sig in xs)]
        untagged = rows[~rows.index.isin(tagged.index)]
        same_label_untagged = pd.DataFrame()
        same_pattern_untagged = pd.DataFrame()
        if not tagged.empty:
            labels = tagged["chart_review_label"].dropna().astype(str).unique().tolist()
            patterns = tagged["pattern_type"].dropna().astype(str).unique().tolist()
            same_label_untagged = untagged[untagged["chart_review_label"].astype(str).isin(labels)]
            same_pattern_untagged = untagged[untagged["pattern_type"].astype(str).isin(patterns)]
        metrics[sig] = {
            **metric_block(tagged),
            "comparison_vs_untagged_rows": compare(tagged, untagged),
            "comparison_vs_same_label_untagged_rows": compare(tagged, same_label_untagged),
            "comparison_vs_same_pattern_type_untagged_rows": compare(tagged, same_pattern_untagged),
        }
    return metrics


def decide(enriched: pd.DataFrame, signatures: list[str], metrics: dict[str, Any], missing: dict[str, Any]) -> str:
    if not bool(enriched.get("chart_context_available", pd.Series(dtype=bool)).all()):
        return "blocked_missing_chart_context"
    if not signatures:
        return "no_feature_only_negative_signature"
    if all(v["sample_count"] < 10 for v in metrics.values()):
        return "feature_context_created_but_underpowered"
    worse = [
        v
        for v in metrics.values()
        if v["comparison_vs_untagged_rows"]["mean_ret20_delta_tagged_minus_untagged"] is not None
        and v["comparison_vs_untagged_rows"]["mean_ret20_delta_tagged_minus_untagged"] < 0
    ]
    if worse:
        return "feature_context_ready_for_signature_pretest"
    return "close_branch_no_reusable_signal"


def missing_column_audit(enriched: pd.DataFrame) -> dict[str, Any]:
    required = DAILY_COLUMNS + WEEKLY_COLUMNS + MONTHLY_COLUMNS
    missing = [col for col in required if col not in enriched.columns or enriched[col].isna().all()]
    partial = [col for col in required if col in enriched.columns and enriched[col].isna().any() and not enriched[col].isna().all()]
    return {"missing_columns": missing, "partial_columns": partial, "required_columns": required}


def run(replay_root: Path, output_root: Path, db_path: Path | None = None) -> Path:
    out = output_root / f"{_now_tag()}-starter-chart-context-enriched-replay-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = pd.read_csv(replay_root / "historical_replay_rows.csv", low_memory=False)
    replay_summary = json.loads((replay_root / "historical_replay_summary.json").read_text(encoding="utf-8"))
    selected_db = db_path or outcome_audit.select_confirmed_db(int(rows["decision_date"].max()))
    bars = load_confirmed_bars(selected_db, rows)
    enriched = enrich_rows(rows, bars)
    enriched["feature_only_signature_list"] = enriched.apply(feature_signatures_for_row, axis=1)
    enriched["feature_only_signatures"] = enriched["feature_only_signature_list"].apply(lambda xs: "|".join(xs))
    enriched = enriched.drop(columns=["feature_only_signature_list"])
    enriched.to_csv(out / "enriched_replay_rows.csv", index=False)

    signatures = sorted({sig for cell in enriched["feature_only_signatures"] for sig in str(cell).split("|") if sig})
    metrics = signature_metrics(enriched, signatures)
    missing = missing_column_audit(enriched)
    decision = decide(enriched, signatures, metrics, missing)
    source_ok = bool(not bars.empty and set(bars["source"].dropna().unique()).issubset({"pan", "txt", "confirmed"}))
    forbidden_used = any(any(term in sig for term in FORBIDDEN_SIGNATURE_TERMS) for sig in signatures)

    _write_json(
        out / "chart_context_column_contract.json",
        {
            "daily_columns": DAILY_COLUMNS,
            "weekly_columns": WEEKLY_COLUMNS,
            "monthly_columns": MONTHLY_COLUMNS,
            "signature_allowed_columns": sorted(POINT_IN_TIME_SIGNATURE_COLUMNS),
            "signature_forbidden_terms": sorted(FORBIDDEN_SIGNATURE_TERMS),
        },
    )
    _write_json(out / "missing_column_audit.json", missing)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "passes": source_ok and not forbidden_used,
            "features_use_bars_through_as_of_date_only": True,
            "outcomes_used_only_after_signature_generation": True,
            "outcome_derived_terms_used_in_signatures": forbidden_used,
            "forbidden_terms": sorted(FORBIDDEN_SIGNATURE_TERMS),
            "bar_sources": sorted(bars["source"].dropna().unique().tolist()) if not bars.empty else [],
        },
    )
    _write_json(
        out / "source_coverage.json",
        {
            "runtime_db_path": selected_db,
            "feature_bar_sources": sorted(bars["source"].dropna().unique().tolist()) if not bars.empty else [],
            "outcome_bar_sources": sorted(enriched["outcome_bar_source"].dropna().unique().tolist()) if "outcome_bar_source" in enriched else [],
            "confirmed_source_only": source_ok and set(enriched.get("outcome_bar_source", pd.Series(dtype=str)).dropna().unique()).issubset({"confirmed"}),
            "sample_count": int(len(enriched)),
            "date_count": int(enriched["decision_date"].nunique()),
            "runtime_db_write": False,
            "meemee_changed": False,
            "production_ranking_changed": False,
        },
    )
    _write_json(
        out / "feature_only_signature_candidates.json",
        {
            "signatures": [{"signature": sig, "support_count": int(enriched["feature_only_signatures"].str.contains(sig, regex=False).sum())} for sig in signatures],
            "active_gate": False,
            "outcome_derived_terms_used": forbidden_used,
        },
    )
    _write_json(out / "feature_only_signature_metrics.json", metrics)
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "starter_ready_promotable": False,
            "negative_tags_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "outcome_derived_terms_used_in_signatures": forbidden_used,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "threshold_retune_attempted": False,
        },
    )
    _write_json(
        out / "enriched_replay_summary.json",
        {
            "axis_id": AXIS_ID,
            "input_replay_root": replay_root,
            "input_replay_decision": replay_summary.get("decision"),
            "sample_count": int(len(enriched)),
            "date_count": int(enriched["decision_date"].nunique()),
            "code_count": int(enriched["code"].astype(str).nunique()),
            "signature_count": len(signatures),
            "decision": decision,
            "starter_ready_promotable": False,
            "negative_tags_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "confirmed_source_only": source_ok,
            "outcome_derived_terms_used_in_signatures": forbidden_used,
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(args.replay_root, args.output_root, args.db_path))


if __name__ == "__main__":
    main()
