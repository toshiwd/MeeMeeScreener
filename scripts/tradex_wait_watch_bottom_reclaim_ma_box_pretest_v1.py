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

from scripts import tradex_pattern_family_source_rows_v1 as source_rows
from scripts import tradex_watch_to_entry_confirmation_pretest_v1 as watch_pretest


AXIS_ID = "wait_watch_bottom_reclaim_ma_box_pretest_v1"
DEFAULT_SOURCE = watch_pretest.DEFAULT_SOURCE
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\wait_watch_bottom_reclaim_ma_box_pretest_v1")
TOP_KS = (5, 10, 20)


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
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


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _streak(series: pd.Series) -> pd.Series:
    values: list[int] = []
    count = 0
    for value in series.fillna(False).astype(bool):
        count = count + 1 if value else 0
        values.append(count)
    return pd.Series(values, index=series.index, dtype="int64")


def _load_daily_features(db_path: Path, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    load_start = max(20000101, int(str(start_ymd)[:4]) * 10000 + 101 - 30000)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        daily = conn.execute(
            f"""
            SELECT
              CAST(code AS VARCHAR) AS code,
              {source_rows.ymd_expr("date")} AS as_of_date,
              CAST(o AS DOUBLE) AS open,
              CAST(h AS DOUBLE) AS high,
              CAST(l AS DOUBLE) AS low,
              CAST(c AS DOUBLE) AS close,
              CAST(v AS DOUBLE) AS volume
            FROM daily_bars
            WHERE {source_rows.ymd_expr("date")} BETWEEN ? AND ?
              AND COALESCE(CAST(source AS VARCHAR), '') <> 'yahoo'
            ORDER BY code, as_of_date
            """,
            [load_start, int(end_ymd)],
        ).fetchdf()
    daily = daily.dropna(subset=["code", "as_of_date", "open", "high", "low", "close"]).copy()
    daily["as_of_date"] = daily["as_of_date"].astype(int)
    daily["code"] = daily["code"].astype(str)
    daily = daily.sort_values(["code", "as_of_date"]).copy()
    g = daily.groupby("code", sort=False)
    daily["ma20_calc"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    daily["ma60_calc"] = g["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["ma100_calc"] = g["close"].transform(lambda s: s.rolling(100, min_periods=100).mean())
    daily["close_above_ma20_calc"] = daily["close"] > daily["ma20_calc"]
    daily["close_above_ma60_calc"] = daily["close"] > daily["ma60_calc"]
    daily["close_above_ma100_calc"] = daily["close"] > daily["ma100_calc"]
    daily["close_below_ma60_calc"] = daily["close"] < daily["ma60_calc"]
    daily["close_below_ma100_calc"] = daily["close"] < daily["ma100_calc"]
    for name in ("ma20", "ma60", "ma100"):
        daily[f"above_{name}_streak"] = g[f"close_above_{name}_calc"].transform(_streak)
    daily["below_ma60_streak"] = g["close_below_ma60_calc"].transform(_streak)
    daily["below_ma100_streak"] = g["close_below_ma100_calc"].transform(_streak)
    daily["prior_below_ma100_streak"] = g["below_ma100_streak"].shift(1)
    daily["prior_below_ma60_streak"] = g["below_ma60_streak"].shift(1)
    daily["prior_above_ma60_streak"] = g["above_ma60_streak"].shift(1)
    daily["ma20_slope_10d_calc"] = daily["ma20_calc"] / g["ma20_calc"].shift(10) - 1.0
    daily["ma60_slope_20d_calc"] = daily["ma60_calc"] / g["ma60_calc"].shift(20) - 1.0
    daily["ma100_slope_20d_calc"] = daily["ma100_calc"] / g["ma100_calc"].shift(20) - 1.0
    daily["ma60_vs_ma100_pct"] = daily["ma60_calc"] / daily["ma100_calc"] - 1.0
    prev_close = g["close"].shift(1)
    tr = pd.concat([(daily["high"] - daily["low"]), (daily["high"] - prev_close).abs(), (daily["low"] - prev_close).abs()], axis=1).max(axis=1)
    daily["atr14_pct_calc"] = tr.groupby(daily["code"]).transform(lambda s: s.rolling(14, min_periods=14).mean()) / daily["close"]
    daily["reclaim_ma60_today"] = daily["close_above_ma60_calc"] & (~g["close_above_ma60_calc"].shift(1).fillna(False).astype(bool))
    daily["reclaim_ma100_today"] = daily["close_above_ma100_calc"] & (~g["close_above_ma100_calc"].shift(1).fillna(False).astype(bool))
    return daily.loc[daily["as_of_date"].between(int(start_ymd), int(end_ymd))][
        [
            "code",
            "as_of_date",
            "ma20_calc",
            "ma60_calc",
            "ma100_calc",
            "close_above_ma20_calc",
            "close_above_ma60_calc",
            "close_above_ma100_calc",
            "above_ma20_streak",
            "above_ma60_streak",
            "above_ma100_streak",
            "below_ma60_streak",
            "below_ma100_streak",
            "prior_below_ma100_streak",
            "prior_below_ma60_streak",
            "prior_above_ma60_streak",
            "ma20_slope_10d_calc",
            "ma60_slope_20d_calc",
            "ma100_slope_20d_calc",
            "ma60_vs_ma100_pct",
            "atr14_pct_calc",
            "reclaim_ma60_today",
            "reclaim_ma100_today",
        ]
    ].copy()


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(rows.get("ret20"), errors="coerce").dropna()
    return {
        "row_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()) if "as_of_date" in rows else 0,
        "code_count": int(rows["code"].astype(str).nunique()) if "code" in rows else 0,
        "ret20_mean": float(ret20.mean()) if not ret20.empty else None,
        "ret20_median": float(ret20.median()) if not ret20.empty else None,
        "winner_rate_ret20_gt_10pct": float((ret20 > 0.10).mean()) if not ret20.empty else None,
        "positive_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
    }


def _delta(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    keys = ["ret20_mean", "winner_rate_ret20_gt_10pct", "positive_rate_ret20_gt_0", "bad_rate_ret20_lt_minus_5pct", "severe_rate_ret20_lt_minus_10pct"]
    return {key: None if left.get(key) is None or right.get(key) is None else left[key] - right[key] for key in keys}


def _topk(rows: pd.DataFrame, k: int) -> pd.DataFrame:
    return rows.sort_values(["as_of_date", "bottom_reclaim_score"], ascending=[True, False]).groupby("as_of_date", group_keys=False).head(k).copy()


def _topk_compare(base: pd.DataFrame, selected: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in TOP_KS:
        base_top = _topk(base, k)
        selected_top = _topk(selected, k)
        shared_dates = sorted(set(base_top["as_of_date"].astype(int)) & set(selected_top["as_of_date"].astype(int)))
        base_shared = base_top.loc[base_top["as_of_date"].astype(int).isin(shared_dates)]
        selected_shared = selected_top.loc[selected_top["as_of_date"].astype(int).isin(shared_dates)]
        bm = _metrics(base_shared)
        sm = _metrics(selected_shared)
        base_keys = set(zip(base_shared["as_of_date"].astype(int), base_shared["code"].astype(str)))
        selected_keys = set(zip(selected_shared["as_of_date"].astype(int), selected_shared["code"].astype(str)))
        out[str(k)] = {
            "shared_date_count": len(shared_dates),
            "base_wait_watch_topk": bm,
            "selected_topk": sm,
            "delta_selected_minus_base": _delta(sm, bm),
            "changed_members_count": len(base_keys.symmetric_difference(selected_keys)),
            "selection_divergence_reason": "bottom-reclaim MA streak and monthly box variant versus Wait+Watch scored baseline",
        }
    return out


def _add_bottom_score(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    box = pd.to_numeric(out["monthly_box_position"], errors="coerce")
    out["bottom_box_score"] = 0.0
    out.loc[box.between(0.0, 0.35, inclusive="both"), "bottom_box_score"] = 1.5
    out.loc[box.between(0.35, 0.70, inclusive="both"), "bottom_box_score"] = 1.0
    out["bottom_reclaim_score"] = (
        out["entry_actionability_score"].fillna(0.0)
        + out["bottom_box_score"]
        + out["close_above_ma20_calc"].astype(float).fillna(0.0) * 0.3
        + out["close_above_ma60_calc"].astype(float).fillna(0.0) * 0.4
        + (pd.to_numeric(out["prior_below_ma100_streak"], errors="coerce").fillna(0).clip(0, 120) / 120.0) * 0.7
        - out["failed_high_flag"].astype(float).fillna(0.0) * 0.5
    )
    return out


def _variant_masks(rows: pd.DataFrame) -> dict[str, pd.Series]:
    box = pd.to_numeric(rows["monthly_box_position"], errors="coerce")
    prior_below100 = pd.to_numeric(rows["prior_below_ma100_streak"], errors="coerce")
    below100 = pd.to_numeric(rows["below_ma100_streak"], errors="coerce")
    prior_below60 = pd.to_numeric(rows["prior_below_ma60_streak"], errors="coerce")
    above60 = pd.to_numeric(rows["above_ma60_streak"], errors="coerce")
    score = pd.to_numeric(rows["bottom_reclaim_score"], errors="coerce")
    clean = (~rows["failed_high_flag"].astype(bool)) & (~rows["bearish_body_flag"].astype(bool))
    bottom_mid = box.between(0.0, 0.70, inclusive="both")
    bottom_low = box.between(0.0, 0.35, inclusive="both")
    return {
        "box_bottom_mid_scored_top": bottom_mid & clean,
        "box_bottom_low_scored_top": bottom_low & clean,
        "below100_ge_100_any": below100 >= 100,
        "prior_below100_ge_100_reclaim60": (prior_below100 >= 100) & rows["close_above_ma60_calc"].fillna(False).astype(bool) & clean,
        "prior_below100_ge_100_reclaim20": (prior_below100 >= 100) & rows["close_above_ma20_calc"].fillna(False).astype(bool) & clean,
        "prior_below60_ge_60_reclaim60": (prior_below60 >= 60) & rows["close_above_ma60_calc"].fillna(False).astype(bool) & clean,
        "above60_ge_60_bottom_mid": (above60 >= 60) & bottom_mid & clean,
        "bottom_reclaim_score_ge_1_4": score >= 1.4,
        "bottom_reclaim_score_ge_1_8": score >= 1.8,
        "wait_only_bottom_score_ge_1_4": rows["review_bucket"].eq("Wait") & (score >= 1.4),
    }


def _decision(variants: dict[str, Any]) -> dict[str, Any]:
    keep: list[str] = []
    hold: list[str] = []
    drop: list[str] = []
    for name, item in variants.items():
        top10 = item["topk_comparison"]["10"]
        n = int(top10["selected_topk"]["row_count"] or 0)
        dates = int(top10["shared_date_count"] or 0)
        delta = top10["delta_selected_minus_base"]
        ret_delta = delta.get("ret20_mean")
        bad_delta = delta.get("bad_rate_ret20_lt_minus_5pct")
        severe_delta = delta.get("severe_rate_ret20_lt_minus_10pct")
        if n < 80 or dates < 40:
            hold.append(name)
        elif ret_delta is not None and ret_delta > 0.005 and bad_delta is not None and bad_delta <= 0.0 and severe_delta is not None and severe_delta <= 0.0:
            keep.append(name)
        elif ret_delta is not None and ret_delta > 0.01 and n >= 40:
            hold.append(name)
        else:
            drop.append(name)
    if keep:
        return {"candidate_local_decision": "keep", "kept_variants": keep, "held_variants": hold, "dropped_variants": drop, "reason": "bottom-reclaim variant improved top10 return without worsening downside under Wait+Watch conditions"}
    if hold:
        return {"candidate_local_decision": "hold", "kept_variants": [], "held_variants": hold, "dropped_variants": drop, "reason": "bottom-reclaim variants are promising but underpowered or incomplete"}
    return {"candidate_local_decision": "drop", "kept_variants": [], "held_variants": [], "dropped_variants": drop, "reason": "no bottom-reclaim variant improved enough under Wait+Watch conditions"}


def run(source: Path, db_path: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    rows["code"] = rows["code"].astype(str)
    base_source = rows.loc[rows["review_bucket"].isin(["Watch", "Wait"])].copy()
    min_dt = int(base_source["as_of_date"].min())
    max_dt = int(base_source["as_of_date"].max())
    daily_features = _load_daily_features(db_path, min_dt, max_dt)
    joined = base_source.merge(daily_features, on=["code", "as_of_date"], how="left")
    base = joined.loc[joined["ma60_calc"].notna() & joined["ma100_calc"].notna()].copy()
    base = _add_bottom_score(base)
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    variants: dict[str, Any] = {}
    for name, mask in _variant_masks(base).items():
        selected = base.loc[mask.fillna(False)].copy()
        selected["bottom_reclaim_variant"] = name
        variants[name] = {
            "metrics": _metrics(selected),
            "comparison_vs_wait_watch_base": {"base": _metrics(base), "selected": _metrics(selected), "delta_selected_minus_base": _delta(_metrics(selected), _metrics(base))},
            "topk_comparison": _topk_compare(base, selected),
            "review_bucket_distribution": selected["review_bucket"].value_counts().to_dict(),
        }
    payload = {
        "axis_id": AXIS_ID,
        "source": str(source),
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "source_surface": "practical_decision_support_bundle_v1 decision_support_surface.parquet",
            "source_bucket": "Wait + Watch",
            "single_axis_changed": "bottom_reclaim_ma_streak_box",
            "model_changed": False,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
            "meemee_unchanged": True,
            "offline_outcomes_used_for_selection": False,
        },
        "join_audit": {
            "source_rows": int(len(base_source)),
            "base_rows_with_ma60_ma100": int(len(base)),
            "missing_ma_rows": int(len(joined) - len(base)),
            "min_as_of_date": min_dt,
            "max_as_of_date": max_dt,
        },
        "base_wait_watch_metrics": _metrics(base),
        "base_review_bucket_distribution": base["review_bucket"].value_counts().to_dict(),
        "variants": variants,
    }
    payload["decision"] = _decision(variants)
    base.to_csv(output / "wait_watch_with_bottom_reclaim_features.csv", index=False)
    _write_json(output / "bottom_reclaim_compare.json", payload)
    _write_json(
        output / "research_decision.json",
        {
            "decision_class": "READY_REVIEW_ONLY",
            "candidate_local_decision": payload["decision"]["candidate_local_decision"],
            "research_decision": "wait_watch_bottom_reclaim_ma_box_pretest_ready_for_manual_review",
            "automatic_trade_action": False,
            "validated_buy_count": 0,
            "runtime_db_write": False,
            "meemee_unchanged": True,
            "production_ranking_changed": False,
            "candidate_generation_changed": False,
        },
    )
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.source, args.db_path, args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
