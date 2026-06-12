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
from scripts import tradex_watch_to_entry_confirmation_pretest_v1 as pretest


AXIS_ID = "watch_entry_ma_streak_box_pretest_v1"
DEFAULT_SOURCE = pretest.DEFAULT_SOURCE
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\watch_entry_ma_streak_box_pretest_v1")
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


def _load_daily_ma_streak_features(db_path: Path, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    load_start = max(20000101, int(str(start_ymd)[:4]) * 10000 + 101 - 20000)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        daily = conn.execute(
            f"""
            SELECT
              CAST(code AS VARCHAR) AS code,
              {source_rows.ymd_expr("date")} AS as_of_date,
              CAST(c AS DOUBLE) AS close
            FROM daily_bars
            WHERE {source_rows.ymd_expr("date")} BETWEEN ? AND ?
              AND COALESCE(CAST(source AS VARCHAR), '') <> 'yahoo'
            ORDER BY code, as_of_date
            """,
            [load_start, int(end_ymd)],
        ).fetchdf()
    daily = daily.dropna(subset=["code", "as_of_date", "close"]).copy()
    daily["as_of_date"] = daily["as_of_date"].astype(int)
    daily["code"] = daily["code"].astype(str)
    daily = daily.sort_values(["code", "as_of_date"]).copy()
    g = daily.groupby("code", sort=False)
    daily["ma60_calc"] = g["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["ma100_calc"] = g["close"].transform(lambda s: s.rolling(100, min_periods=100).mean())
    daily["close_above_ma60_calc"] = daily["close"] > daily["ma60_calc"]
    daily["close_below_ma60_calc"] = daily["close"] < daily["ma60_calc"]
    daily["close_above_ma100_calc"] = daily["close"] > daily["ma100_calc"]
    daily["close_below_ma100_calc"] = daily["close"] < daily["ma100_calc"]
    daily["above_ma60_streak"] = g["close_above_ma60_calc"].transform(_streak)
    daily["below_ma60_streak"] = g["close_below_ma60_calc"].transform(_streak)
    daily["above_ma100_streak"] = g["close_above_ma100_calc"].transform(_streak)
    daily["below_ma100_streak"] = g["close_below_ma100_calc"].transform(_streak)
    daily["prior_below_ma100_streak"] = g["below_ma100_streak"].shift(1)
    daily["prior_below_ma60_streak"] = g["below_ma60_streak"].shift(1)
    daily["prior_above_ma60_streak"] = g["above_ma60_streak"].shift(1)
    daily["days_since_ma60_reclaim"] = g["close_above_ma60_calc"].transform(lambda s: (~s.astype(bool)).groupby(s.astype(bool).cumsum()).cumcount())
    daily.loc[~daily["close_above_ma60_calc"], "days_since_ma60_reclaim"] = None
    daily["ma60_vs_ma100_pct"] = daily["ma60_calc"] / daily["ma100_calc"] - 1.0
    return daily.loc[daily["as_of_date"].between(int(start_ymd), int(end_ymd))][
        [
            "code",
            "as_of_date",
            "ma60_calc",
            "ma100_calc",
            "close_above_ma60_calc",
            "close_above_ma100_calc",
            "above_ma60_streak",
            "below_ma60_streak",
            "above_ma100_streak",
            "below_ma100_streak",
            "prior_below_ma100_streak",
            "prior_below_ma60_streak",
            "prior_above_ma60_streak",
            "days_since_ma60_reclaim",
            "ma60_vs_ma100_pct",
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
    return rows.sort_values(["as_of_date", "entry_actionability_score"], ascending=[True, False]).groupby("as_of_date", group_keys=False).head(k).copy()


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
            "base_promoted_topk": bm,
            "selected_topk": sm,
            "delta_selected_minus_base": _delta(sm, bm),
            "changed_members_count": len(base_keys.symmetric_difference(selected_keys)),
            "selection_divergence_reason": "same promoted Watch rule plus one MA-streak/box-context gate",
        }
    return out


def _variant_masks(rows: pd.DataFrame) -> dict[str, pd.Series]:
    box = pd.to_numeric(rows["monthly_box_position"], errors="coerce")
    below100_prior = pd.to_numeric(rows["prior_below_ma100_streak"], errors="coerce")
    below100 = pd.to_numeric(rows["below_ma100_streak"], errors="coerce")
    above60 = pd.to_numeric(rows["above_ma60_streak"], errors="coerce")
    below60_prior = pd.to_numeric(rows["prior_below_ma60_streak"], errors="coerce")
    reclaim60_days = pd.to_numeric(rows["days_since_ma60_reclaim"], errors="coerce")
    ma60_vs_100 = pd.to_numeric(rows["ma60_vs_ma100_pct"], errors="coerce")
    close_above60 = rows["close_above_ma60_calc"].fillna(False).astype(bool)
    close_above100 = rows["close_above_ma100_calc"].fillna(False).astype(bool)
    box_bottom_mid = box.between(0.0, 0.70, inclusive="both")
    box_mid_high = box.between(0.35, 0.85, inclusive="both")
    return {
        "above60_ge_60": above60 >= 60,
        "above60_ge_60_box_mid_high": (above60 >= 60) & box_mid_high,
        "below100_ge_100": below100 >= 100,
        "below100_ge_100_box_bottom_mid": (below100 >= 100) & box_bottom_mid,
        "ma60_reclaim_after_below100_ge_100": close_above60 & (below100_prior >= 100) & (reclaim60_days <= 10),
        "ma60_reclaim_after_below60_ge_60": close_above60 & (below60_prior >= 60) & (reclaim60_days <= 10),
        "below100_base_and_above60": (below100 >= 60) & close_above60,
        "above60_below100_compression": (above60 >= 20) & (~close_above100) & (ma60_vs_100 <= 0),
        "above60_ge_60_above100": (above60 >= 60) & close_above100,
        "box_bottom_mid_only": box_bottom_mid,
    }


def _bucket_rows(rows: pd.DataFrame) -> list[dict[str, Any]]:
    work = rows.copy()
    work["box_bucket"] = pd.cut(pd.to_numeric(work["monthly_box_position"], errors="coerce"), bins=[-math.inf, 0.35, 0.70, 0.85, math.inf], labels=["low", "mid", "high", "extreme_high"]).astype(str)
    work["above60_bucket"] = pd.cut(pd.to_numeric(work["above_ma60_streak"], errors="coerce"), bins=[-math.inf, 0, 20, 60, math.inf], labels=["not_above", "above_1_20", "above_21_60", "above_60plus"]).astype(str)
    work["below100_bucket"] = pd.cut(pd.to_numeric(work["below_ma100_streak"], errors="coerce"), bins=[-math.inf, 0, 20, 60, 100, math.inf], labels=["not_below", "below_1_20", "below_21_60", "below_61_100", "below_100plus"]).astype(str)
    out: list[dict[str, Any]] = []
    for (box, above60, below100), group in work.groupby(["box_bucket", "above60_bucket", "below100_bucket"], dropna=False, sort=True):
        if len(group) >= 5:
            out.append({"box_bucket": str(box), "above60_bucket": str(above60), "below100_bucket": str(below100), **_metrics(group)})
    return out


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
        if n < 40 or dates < 25:
            hold.append(name)
        elif ret_delta is not None and ret_delta > 0.005 and bad_delta is not None and bad_delta <= 0.0 and severe_delta is not None and severe_delta <= 0.0:
            keep.append(name)
        elif bad_delta is not None and bad_delta < -0.03 and severe_delta is not None and severe_delta < -0.015 and ret_delta is not None and ret_delta > -0.002:
            hold.append(name)
        else:
            drop.append(name)
    if keep:
        return {"candidate_local_decision": "keep", "kept_variants": keep, "held_variants": hold, "dropped_variants": drop, "reason": "MA streak or box condition improved top10 return without worsening downside"}
    if hold:
        return {"candidate_local_decision": "hold", "kept_variants": [], "held_variants": hold, "dropped_variants": drop, "reason": "some MA streak or box contexts look useful but are underpowered or improve downside only"}
    return {"candidate_local_decision": "drop", "kept_variants": [], "held_variants": [], "dropped_variants": drop, "reason": "no MA streak or box condition improved the promoted Watch rule under fixed conditions"}


def run(source: Path, db_path: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    rows["code"] = rows["code"].astype(str)
    watch = rows.loc[rows["review_bucket"].eq("Watch")].copy()
    promoted = watch.loc[pretest._variants(watch)["bullish_support_reclaim_volume_ok"]].copy()
    min_dt = int(promoted["as_of_date"].min())
    max_dt = int(promoted["as_of_date"].max())
    ma_features = _load_daily_ma_streak_features(db_path, min_dt, max_dt)
    joined = promoted.merge(ma_features, on=["code", "as_of_date"], how="left")
    base = joined.loc[joined["ma60_calc"].notna() & joined["ma100_calc"].notna()].copy()
    if base.empty:
        raise RuntimeError("No promoted rows joined to MA60/MA100 streak features")
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    variants: dict[str, Any] = {}
    for name, mask in _variant_masks(base).items():
        selected = base.loc[mask.fillna(False)].copy()
        selected["ma_streak_box_variant"] = name
        variants[name] = {
            "metrics": _metrics(selected),
            "comparison_vs_base_promoted": {"base": _metrics(base), "selected": _metrics(selected), "delta_selected_minus_base": _delta(_metrics(selected), _metrics(base))},
            "topk_comparison": _topk_compare(base, selected),
            "bucket_rows": _bucket_rows(selected),
        }
    payload = {
        "axis_id": AXIS_ID,
        "source": str(source),
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "source_surface": "practical_decision_support_bundle_v1 decision_support_surface.parquet",
            "source_bucket": "Watch only",
            "promotion_variant": "bullish_support_reclaim_volume_ok",
            "single_axis_changed": "MA60/MA100 streak plus monthly box context",
            "model_changed": False,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
            "meemee_unchanged": True,
            "offline_outcomes_used_for_selection": False,
        },
        "join_audit": {
            "promoted_rows": int(len(promoted)),
            "base_rows_with_ma60_ma100": int(len(base)),
            "missing_ma_rows": int(len(joined) - len(base)),
            "min_as_of_date": min_dt,
            "max_as_of_date": max_dt,
        },
        "base_promoted_metrics": _metrics(base),
        "base_bucket_rows": _bucket_rows(base),
        "variants": variants,
    }
    payload["decision"] = _decision(variants)
    base.to_csv(output / "promoted_with_ma_streak_box_rows.csv", index=False)
    _write_json(output / "ma_streak_box_compare.json", payload)
    _write_json(
        output / "research_decision.json",
        {
            "decision_class": "READY_REVIEW_ONLY",
            "candidate_local_decision": payload["decision"]["candidate_local_decision"],
            "research_decision": "watch_entry_ma_streak_box_pretest_ready_for_manual_review",
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
