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


AXIS_ID = "ma_phase_feature_base_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1")
MA_WINDOWS = (5, 7, 20, 60, 100, 200)
SLOPE_LOOKBACKS = (3, 5, 10, 20)
OUTCOME_HORIZONS = (3, 5, 7, 10, 20, 60)
RUN_BUCKET_BINS = (-1, 0, 2, 4, 6, 9, 14, 18, 20, 30, 40, 60, 80, 100, 10_000_000)
RUN_BUCKET_LABELS = ("0", "1-2", "3-4", "5-6", "7-9", "10-14", "15-18", "19-20", "21-30", "31-40", "41-60", "61-80", "81-100", "101+")
MILESTONES = (5, 7, 18, 20, 60, 100, 200)
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "feature_definition.json",
    "ma_phase_features.parquet",
    "ma_phase_feature_sample.csv",
    "feature_coverage_summary.json",
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
    frame = conn.execute(
        f"""
        WITH normalized AS (
          SELECT
            CAST(code AS VARCHAR) AS code,
            {_date_expr("date")} AS ymd,
            CAST(o AS DOUBLE) AS o,
            CAST(h AS DOUBLE) AS h,
            CAST(l AS DOUBLE) AS l,
            CAST(c AS DOUBLE) AS c,
            CAST(v AS DOUBLE) AS v,
            lower(coalesce(source, '')) AS source
          FROM daily_bars
          WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
            AND lower(coalesce(source, '')) IN ('pan', 'txt', 'confirmed')
        )
        SELECT code, ymd, o, h, l, c, v, source
        FROM normalized
        WHERE ymd >= ? {end_clause}
        ORDER BY code, ymd
        """,
        params,
    ).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype(int)
    return frame.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _streak_true(cond: pd.Series) -> pd.Series:
    values = cond.fillna(False).astype(bool)
    groups = values.ne(values.shift()).cumsum()
    return values.groupby(groups).cumcount().add(1).where(values, 0)


def _bars_since(cond: pd.Series) -> pd.Series:
    out: list[int | None] = []
    last_idx: int | None = None
    for idx, value in enumerate(cond.fillna(False).astype(bool).tolist()):
        if value:
            last_idx = idx
            out.append(0)
        elif last_idx is None:
            out.append(None)
        else:
            out.append(idx - last_idx)
    return pd.Series(out, index=cond.index)


def _bucket_run(series: pd.Series) -> pd.Series:
    return pd.cut(series.fillna(0).astype(int), bins=RUN_BUCKET_BINS, labels=RUN_BUCKET_LABELS).astype("object").fillna("unknown")


def _slope_bucket(series: pd.Series) -> pd.Series:
    return pd.cut(
        series,
        bins=[-float("inf"), -2.0, -0.35, 0.35, 2.0, float("inf")],
        labels=["strong_down", "weak_down", "flat", "weak_up", "strong_up"],
    ).astype("object").fillna("unknown")


def _add_ma_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    grouped = out.groupby("code", group_keys=False)
    prev_close = grouped["c"].shift(1)
    for ma in MA_WINDOWS:
        ma_col = f"ma{ma}"
        out[ma_col] = grouped["c"].transform(lambda s, ma=ma: s.rolling(ma, min_periods=ma).mean())
        prev_ma = grouped[ma_col].shift(1)
        above = out["c"] >= out[ma_col]
        below = out["c"] < out[ma_col]
        cross_above = (prev_close < prev_ma) & above
        cross_below = (prev_close >= prev_ma) & below
        out[f"close_above_ma{ma}"] = above
        out[f"close_below_ma{ma}"] = below
        out[f"cross_above_ma{ma}_today"] = cross_above
        out[f"cross_below_ma{ma}_today"] = cross_below
        out[f"bars_since_cross_above_ma{ma}"] = grouped[f"cross_above_ma{ma}_today"].transform(_bars_since)
        out[f"bars_since_cross_below_ma{ma}"] = grouped[f"cross_below_ma{ma}_today"].transform(_bars_since)
        out[f"consecutive_bars_above_ma{ma}"] = grouped[f"close_above_ma{ma}"].transform(_streak_true).astype(int)
        out[f"consecutive_bars_below_ma{ma}"] = grouped[f"close_below_ma{ma}"].transform(_streak_true).astype(int)
        out[f"above_ma{ma}_run_bucket"] = _bucket_run(out[f"consecutive_bars_above_ma{ma}"])
        out[f"below_ma{ma}_run_bucket"] = _bucket_run(out[f"consecutive_bars_below_ma{ma}"])
        for milestone in MILESTONES:
            out[f"ma{ma}_near_{milestone}th_bar"] = out[f"consecutive_bars_above_ma{ma}"].between(milestone - 1, milestone + 1, inclusive="both")
        for lookback in SLOPE_LOOKBACKS:
            slope = (out[ma_col] / grouped[ma_col].shift(lookback) - 1.0) * 100.0
            out[f"ma{ma}_slope_{lookback}d"] = slope
            out[f"ma{ma}_slope_{lookback}d_norm_close"] = (out[ma_col] - grouped[ma_col].shift(lookback)) / out["c"] * 100.0
            out[f"ma{ma}_slope_{lookback}d_bucket"] = _slope_bucket(slope)
    out["ma7_gt_ma20"] = out["ma7"] > out["ma20"]
    out["ma20_gt_ma60"] = out["ma20"] > out["ma60"]
    out["ma60_gt_ma100"] = out["ma60"] > out["ma100"]
    out["ma100_gt_ma200"] = out["ma100"] > out["ma200"]
    out["ma_stack_state"] = "mixed_stack"
    out.loc[(out["ma7"] > out["ma20"]) & (out["ma20"] > out["ma60"]) & (out["ma60"] > out["ma100"]) & (out["ma100"] > out["ma200"]), "ma_stack_state"] = "bullish_stack"
    out.loc[(out["ma7"] < out["ma20"]) & (out["ma20"] < out["ma60"]) & (out["ma60"] < out["ma100"]) & (out["ma100"] < out["ma200"]), "ma_stack_state"] = "bearish_stack"
    return out


def _add_support_resistance(out: pd.DataFrame) -> pd.DataFrame:
    ma_cols = [f"ma{ma}" for ma in MA_WINDOWS]
    ma_frame = out[ma_cols]
    above = ma_frame.where(ma_frame.gt(out["c"], axis=0))
    below = ma_frame.where(ma_frame.lt(out["c"], axis=0))
    out["nearest_upper_ma_distance_pct"] = ((above.min(axis=1) / out["c"]) - 1.0) * 100.0
    out["nearest_lower_ma_distance_pct"] = ((out["c"] / below.max(axis=1)) - 1.0) * 100.0
    out["nearest_upper_ma"] = above.idxmin(axis=1).str.replace("ma", "MA", regex=False)
    out["nearest_lower_ma"] = below.idxmax(axis=1).str.replace("ma", "MA", regex=False)
    for pct in (1, 3, 5):
        out[f"upper_ma_count_within_{pct}pct"] = ma_frame.gt(out["c"], axis=0).where(((ma_frame / out["c"].values.reshape(-1, 1)) - 1.0).le(pct / 100.0), False).sum(axis=1).astype(int)
        out[f"lower_ma_count_within_{pct}pct"] = ma_frame.lt(out["c"], axis=0).where(((out["c"].values.reshape(-1, 1) / ma_frame) - 1.0).le(pct / 100.0), False).sum(axis=1).astype(int)
    out["upper_resistance_bucket"] = pd.cut(
        out["upper_ma_count_within_5pct"],
        bins=[-1, 0, 1, 2, 99],
        labels=["none_near", "light_resistance", "medium_resistance", "heavy_resistance"],
    ).astype("object")
    out["lower_support_bucket"] = pd.cut(
        out["lower_ma_count_within_5pct"],
        bins=[-1, 0, 1, 2, 99],
        labels=["none_near", "light_support", "medium_support", "heavy_support"],
    ).astype("object")
    return out


def _add_candles(out: pd.DataFrame) -> pd.DataFrame:
    grouped = out.groupby("code", group_keys=False)
    prev_o = grouped["o"].shift(1)
    prev_c = grouped["c"].shift(1)
    prev_h = grouped["h"].shift(1)
    prev_l = grouped["l"].shift(1)
    rng = (out["h"] - out["l"]).where((out["h"] - out["l"]) > 0)
    body = (out["c"] - out["o"]).abs()
    upper = out["h"] - out[["o", "c"]].max(axis=1)
    lower = out[["o", "c"]].min(axis=1) - out["l"]
    out["body_pct_of_range"] = body / rng
    out["upper_wick_pct_of_range"] = upper / rng
    out["lower_wick_pct_of_range"] = lower / rng
    out["close_position_in_range"] = (out["c"] - out["l"]) / rng
    out["gap_up_pct"] = (out["o"] / prev_h - 1.0) * 100.0
    out["gap_down_pct"] = (out["o"] / prev_l - 1.0) * 100.0
    out["is_large_bull_body"] = (out["c"] > out["o"]) & (out["body_pct_of_range"] >= 0.65)
    out["is_large_bear_body"] = (out["c"] < out["o"]) & (out["body_pct_of_range"] >= 0.65)
    out["is_small_body"] = out["body_pct_of_range"] <= 0.25
    out["is_doji_like"] = out["body_pct_of_range"] <= 0.10
    out["is_upper_shadow_long"] = out["upper_wick_pct_of_range"] >= 0.45
    out["is_lower_shadow_long"] = out["lower_wick_pct_of_range"] >= 0.45
    out["is_hammer_like"] = out["is_small_body"] & out["is_lower_shadow_long"] & (out["upper_wick_pct_of_range"] <= 0.20)
    out["is_shooting_star_like"] = out["is_small_body"] & out["is_upper_shadow_long"] & (out["lower_wick_pct_of_range"] <= 0.20)
    out["is_engulfing_bull"] = (out["c"] > out["o"]) & (prev_c < prev_o) & (out["o"] <= prev_c) & (out["c"] >= prev_o)
    out["is_engulfing_bear"] = (out["c"] < out["o"]) & (prev_c > prev_o) & (out["o"] >= prev_c) & (out["c"] <= prev_o)
    out["is_inside_bar"] = (out["h"] <= prev_h) & (out["l"] >= prev_l)
    out["is_outside_bar"] = (out["h"] >= prev_h) & (out["l"] <= prev_l)
    return out


def _add_outcomes(out: pd.DataFrame) -> pd.DataFrame:
    grouped = out.groupby("code", group_keys=False)
    for horizon in OUTCOME_HORIZONS:
        future_close = grouped["c"].shift(-horizon)
        out[f"ret_{horizon}b"] = (future_close / out["c"] - 1.0) * 100.0
        out[f"max_up_{horizon}b"] = grouped["h"].transform(lambda s, h=horizon: s.shift(-1).rolling(h, min_periods=1).max().shift(-(h - 1))) / out["c"] * 100.0 - 100.0
        out[f"max_drawdown_{horizon}b"] = grouped["l"].transform(lambda s, h=horizon: s.shift(-1).rolling(h, min_periods=1).min().shift(-(h - 1))) / out["c"] * 100.0 - 100.0
        out[f"severe_loss_flag_{horizon}b"] = out[f"ret_{horizon}b"] <= -10.0
        out[f"higher_high_made_{horizon}b"] = grouped["h"].transform(lambda s, h=horizon: s.shift(-1).rolling(h, min_periods=1).max().shift(-(h - 1))) > out["h"]
        out[f"lower_low_made_{horizon}b"] = grouped["l"].transform(lambda s, h=horizon: s.shift(-1).rolling(h, min_periods=1).min().shift(-(h - 1))) < out["l"]
        out[f"pullback_occurred_{horizon}b"] = out[f"max_drawdown_{horizon}b"] <= -3.0
        out[f"recovered_after_pullback_{horizon}b"] = out[f"pullback_occurred_{horizon}b"] & (out[f"ret_{horizon}b"] > 0)
        upper_target = out["nearest_upper_ma_distance_pct"].notna() & (out["nearest_upper_ma_distance_pct"] <= out[f"max_up_{horizon}b"])
        out[f"reached_next_upper_ma_{horizon}b"] = upper_target
        for ma in (7, 20, 60, 100, 200):
            future_min_close = grouped["c"].transform(lambda s, h=horizon: s.shift(-1).rolling(h, min_periods=1).min().shift(-(h - 1)))
            future_max_close = grouped["c"].transform(lambda s, h=horizon: s.shift(-1).rolling(h, min_periods=1).max().shift(-(h - 1)))
            out[f"held_above_ma{ma}_{horizon}b"] = future_min_close >= out[f"ma{ma}"]
            out[f"rebreak_ma{ma}_{horizon}b"] = (out["c"] >= out[f"ma{ma}"]) & (future_min_close < out[f"ma{ma}"])
            out[f"recovered_ma{ma}_after_pullback_{horizon}b"] = out[f"rebreak_ma{ma}_{horizon}b"] & (future_max_close >= out[f"ma{ma}"])
    return out


def _coverage(out: pd.DataFrame) -> dict[str, Any]:
    key_cols = [
        "ma5",
        "ma7",
        "ma20",
        "ma60",
        "ma100",
        "ma200",
        "nearest_upper_ma_distance_pct",
        "nearest_lower_ma_distance_pct",
        "body_pct_of_range",
        "ret_20b",
        "max_drawdown_20b",
    ]
    return {
        "axis_id": AXIS_ID,
        "row_count": int(len(out)),
        "code_count": int(out["code"].nunique()),
        "min_ymd": int(out["ymd"].min()),
        "max_ymd": int(out["ymd"].max()),
        "column_count": int(len(out.columns)),
        "non_null_rates": {col: float(out[col].notna().mean()) for col in key_cols if col in out},
        "stack_distribution": out["ma_stack_state"].value_counts(dropna=False).to_dict(),
        "upper_resistance_bucket_distribution": out["upper_resistance_bucket"].value_counts(dropna=False).to_dict(),
        "lower_support_bucket_distribution": out["lower_support_bucket"].value_counts(dropna=False).to_dict(),
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "source": "runtime DuckDB daily_bars confirmed sources only",
        "ma_windows": list(MA_WINDOWS),
        "price_vs_ma_features": [
            "close_above_ma",
            "close_below_ma",
            "cross_above_ma_today",
            "cross_below_ma_today",
            "bars_since_cross_above_ma",
            "bars_since_cross_below_ma",
            "consecutive_bars_above_ma",
            "consecutive_bars_below_ma",
        ],
        "slope_lookbacks": list(SLOPE_LOOKBACKS),
        "slope_bucket": "strong_down < -2%, weak_down -2..-0.35%, flat -0.35..0.35%, weak_up 0.35..2%, strong_up > 2%",
        "ma_stack": {
            "bullish_stack": "MA7 > MA20 > MA60 > MA100 > MA200",
            "bearish_stack": "MA7 < MA20 < MA60 < MA100 < MA200",
            "mixed_stack": "otherwise",
        },
        "support_resistance": "nearest upper/lower MA and count of MA lines within 1/3/5 pct of close",
        "run_buckets": list(RUN_BUCKET_LABELS),
        "milestones": list(MILESTONES),
        "outcome_horizons": list(OUTCOME_HORIZONS),
        "outcome_labels_are_diagnostic_only": True,
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    db_path = Path(args.db_path) if args.db_path else resolve_runtime_stock_db_path()
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness()
    db_contract = inspect_runtime_stock_db(runtime_db_path=db_path)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        daily = _load_daily(conn, start_ymd=args.start_ymd, end_ymd=args.end_ymd)
    features = _add_ma_features(daily)
    features = _add_support_resistance(features)
    features = _add_candles(features)
    features = _add_outcomes(features)
    coverage = _coverage(features)
    decision = "feature_base_ready" if coverage["non_null_rates"].get("ma200", 0) > 0.7 and coverage["non_null_rates"].get("ret_20b", 0) > 0.9 else "feature_base_incomplete"
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "db_contract": db_contract,
        "runtime_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "start_ymd": args.start_ymd,
        "end_ymd": args.end_ymd,
        "confirmed_bars_only": True,
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "daily_rows": int(len(daily)),
        "feature_rows": int(len(features)),
        "code_count": int(features["code"].nunique()),
        "min_ymd": int(features["ymd"].min()),
        "max_ymd": int(features["ymd"].max()),
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "feature_definition.json", _definition())
    features.to_parquet(out_dir / "ma_phase_features.parquet", index=False, engine="pyarrow", compression="zstd")
    features.head(2000).to_csv(out_dir / "ma_phase_feature_sample.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "feature_coverage_summary.json", coverage)
    _write_json(
        out_dir / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "candidate_local_decision": decision,
            "session_aggregate_decision": decision,
            "authoritative_rollup_decision": decision,
            "reason": "shared_ma_phase_feature_base_ready_for_follow_on_diagnostics" if decision == "feature_base_ready" else "coverage_below_feature_base_threshold",
            "non_scope": [
                "no MeeMee reflection",
                "no runtime DuckDB writes",
                "no ranking change",
                "no publish",
                "no candidate generation change",
                "no buy/sell rule promotion",
                "no bad-pick removal implementation",
                "no score tuning",
                "no threshold optimization",
            ],
        },
    )
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
    parser = argparse.ArgumentParser(description="TRADEX read-only MA phase feature base builder.")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
