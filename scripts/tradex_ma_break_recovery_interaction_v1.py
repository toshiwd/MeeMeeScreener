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


AXIS_ID = "ma_break_recovery_interaction_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_break_recovery_interaction_v1")
MA_WINDOWS = (5, 7, 20, 60, 100, 200)
RECOVERY_HORIZONS = (3, 5, 10, 20, 40, 60)
FORWARD_HORIZONS = (5, 10, 20, 40, 60)
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "ma_break_events.csv",
    "ma_recovery_summary.json",
    "ma_recovery_by_interaction.csv",
    "ma_recovery_pair_matrix.csv",
    "downside_continuation_summary.csv",
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


def _load_daily_bars(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int | None, confirmed_only: bool) -> pd.DataFrame:
    source_filter = "AND lower(coalesce(source, '')) IN ('pan', 'txt', 'confirmed')" if confirmed_only else ""
    end_clause = "" if end_ymd is None else "AND ymd <= ?"
    params: list[Any] = [int(start_ymd)]
    if end_ymd is not None:
        params.append(int(end_ymd))
    query = f"""
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
        {source_filter}
    )
    SELECT code, ymd, o, h, l, c, v, source
    FROM normalized
    WHERE ymd >= ? {end_clause}
    ORDER BY code, ymd
    """
    frame = conn.execute(query, params).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["ymd", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    return frame.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _add_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    grouped = out.groupby("code", group_keys=False)
    for window in MA_WINDOWS:
        ma_col = f"ma{window}"
        below_col = f"below_ma{window}"
        break_col = f"break_ma{window}"
        out[ma_col] = grouped["c"].transform(lambda s, w=window: s.rolling(w, min_periods=w).mean())
        out[f"prev_c_ma{window}"] = grouped["c"].shift(1)
        out[f"prev_ma{window}"] = grouped[ma_col].shift(1)
        out[below_col] = out["c"] < out[ma_col]
        out[break_col] = (out[f"prev_c_ma{window}"] >= out[f"prev_ma{window}"]) & out[below_col]
    for horizon in FORWARD_HORIZONS:
        out[f"ret_{horizon}d_pct"] = (grouped["c"].shift(-horizon) / out["c"] - 1.0) * 100.0
    return out


def _build_events(frame: pd.DataFrame, *, max_horizon: int) -> pd.DataFrame:
    event_rows: list[dict[str, Any]] = []
    for code, rows in frame.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        closes = rows["c"].to_numpy()
        lows = rows["l"].to_numpy()
        ma_values = {window: rows[f"ma{window}"].to_numpy() for window in MA_WINDOWS}
        below_values = {window: rows[f"below_ma{window}"].fillna(False).astype(bool).to_numpy() for window in MA_WINDOWS}
        break_values = {window: rows[f"break_ma{window}"].fillna(False).astype(bool).to_numpy() for window in MA_WINDOWS}
        ymd_values = rows["ymd"].to_numpy()
        ret_values = {horizon: rows[f"ret_{horizon}d_pct"].to_numpy() for horizon in FORWARD_HORIZONS}
        for idx in range(len(rows)):
            for window in MA_WINDOWS:
                ma_col = f"ma{window}"
                ma_value = ma_values[window][idx]
                if not bool(break_values[window][idx]) or pd.isna(ma_value):
                    continue
                recovery_bars = None
                end = min(len(rows), idx + max_horizon + 1)
                if idx + 1 < end:
                    future_close = closes[idx + 1 : end]
                    future_ma = ma_values[window][idx + 1 : end]
                    recovered_mask = future_close >= future_ma
                    recovered_positions = recovered_mask.nonzero()[0]
                    if len(recovered_positions):
                        recovery_bars = int(recovered_positions[0] + 1)
                future_low_end = min(len(rows), idx + 21)
                future_min_ret_20d_pct = None
                if idx + 1 < future_low_end:
                    future_min_ret_20d_pct = float((lows[idx + 1 : future_low_end].min() / closes[idx] - 1.0) * 100.0)
                other_below = [f"ma{other}" for other in MA_WINDOWS if other != window and bool(below_values[other][idx])]
                other_same_day_breaks = [f"ma{other}" for other in MA_WINDOWS if other != window and bool(break_values[other][idx])]
                event: dict[str, Any] = {
                    "code": code,
                    "event_date": int(ymd_values[idx]),
                    "target_ma": f"ma{window}",
                    "target_window": window,
                    "close": float(closes[idx]),
                    "target_ma_value": float(ma_value),
                    "target_ma_distance_pct": float((closes[idx] / ma_value - 1.0) * 100.0),
                    "recovery_bars": recovery_bars,
                    "recovered_within_60": recovery_bars is not None and recovery_bars <= 60,
                    "other_below_count": len(other_below),
                    "other_same_day_break_count": len(other_same_day_breaks),
                    "other_below_mas": "|".join(other_below),
                    "other_same_day_break_mas": "|".join(other_same_day_breaks),
                    "future_min_ret_20d_pct": future_min_ret_20d_pct,
                }
                for other in MA_WINDOWS:
                    if other == window:
                        continue
                    event[f"below_ma{other}_at_event"] = bool(below_values[other][idx])
                    event[f"same_day_break_ma{other}"] = bool(break_values[other][idx])
                for horizon in RECOVERY_HORIZONS:
                    event[f"recovered_within_{horizon}d"] = recovery_bars is not None and recovery_bars <= horizon
                for horizon in FORWARD_HORIZONS:
                    value = ret_values[horizon][idx]
                    event[f"ret_{horizon}d_pct"] = None if pd.isna(value) else float(value)
                event_rows.append(event)
    return pd.DataFrame(event_rows)


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float(valid.mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.median())


def _event_summary(events: pd.DataFrame) -> dict[str, Any]:
    by_ma: list[dict[str, Any]] = []
    for target_ma, group in events.groupby("target_ma", sort=False):
        row: dict[str, Any] = {
            "target_ma": target_ma,
            "event_count": int(len(group)),
            "median_recovery_bars_recovered_only": _median(group["recovery_bars"]),
            "mean_ret_20d_pct": _mean(group["ret_20d_pct"]),
            "mean_future_min_ret_20d_pct": _mean(group["future_min_ret_20d_pct"]),
        }
        for horizon in RECOVERY_HORIZONS:
            row[f"recovery_rate_{horizon}d"] = _rate(group[f"recovered_within_{horizon}d"])
        by_ma.append(row)
    return {
        "axis_id": AXIS_ID,
        "fixed_evaluation_conditions": {
            "event_definition": "previous close >= target MA and current close < target MA",
            "recovery_definition": "future close >= same target MA",
            "ma_windows": list(MA_WINDOWS),
            "recovery_horizons_bars": list(RECOVERY_HORIZONS),
            "confirmed_bars_only": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
        },
        "by_target_ma": by_ma,
    }


def _interaction_summary(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in events.groupby(["target_ma", "other_below_count", "other_same_day_break_count"], dropna=False):
        target_ma, other_below_count, other_same_day_break_count = keys
        out: dict[str, Any] = {
            "target_ma": target_ma,
            "other_below_count": int(other_below_count),
            "other_same_day_break_count": int(other_same_day_break_count),
            "event_count": int(len(group)),
            "median_recovery_bars_recovered_only": _median(group["recovery_bars"]),
            "mean_ret_20d_pct": _mean(group["ret_20d_pct"]),
            "mean_future_min_ret_20d_pct": _mean(group["future_min_ret_20d_pct"]),
        }
        for horizon in RECOVERY_HORIZONS:
            out[f"recovery_rate_{horizon}d"] = _rate(group[f"recovered_within_{horizon}d"])
        rows.append(out)
    return pd.DataFrame(rows).sort_values(["target_ma", "other_below_count", "other_same_day_break_count"], kind="stable")


def _pair_matrix(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for target in MA_WINDOWS:
        target_events = events[events["target_window"] == target]
        for other in MA_WINDOWS:
            if other == target:
                continue
            flag = f"below_ma{other}_at_event"
            for flag_value, group in target_events.groupby(flag):
                rows.append(
                    {
                        "target_ma": f"ma{target}",
                        "condition": f"below_ma{other}_at_event={bool(flag_value)}",
                        "other_ma": f"ma{other}",
                        "event_count": int(len(group)),
                        "recovery_rate_20d": _rate(group["recovered_within_20d"]),
                        "recovery_rate_60d": _rate(group["recovered_within_60d"]),
                        "median_recovery_bars_recovered_only": _median(group["recovery_bars"]),
                        "mean_ret_20d_pct": _mean(group["ret_20d_pct"]),
                        "mean_future_min_ret_20d_pct": _mean(group["future_min_ret_20d_pct"]),
                    }
                )
    return pd.DataFrame(rows).sort_values(["target_ma", "other_ma", "condition"], kind="stable")


def _downside_summary(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in events.groupby(["target_ma", "other_below_count"], dropna=False):
        target_ma, other_below_count = keys
        rows.append(
            {
                "target_ma": target_ma,
                "other_below_count": int(other_below_count),
                "event_count": int(len(group)),
                "no_recovery_20d_rate": 1.0 - float(group["recovered_within_20d"].mean()),
                "no_recovery_60d_rate": 1.0 - float(group["recovered_within_60d"].mean()),
                "mean_ret_20d_pct": _mean(group["ret_20d_pct"]),
                "mean_ret_60d_pct": _mean(group["ret_60d_pct"]),
                "mean_future_min_ret_20d_pct": _mean(group["future_min_ret_20d_pct"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["target_ma", "other_below_count"], kind="stable")


def _research_decision(events: pd.DataFrame, pair: pd.DataFrame) -> dict[str, Any]:
    notable: list[dict[str, Any]] = []
    for _, row in pair.iterrows():
        if int(row["event_count"]) < 50 or "True" not in str(row["condition"]):
            continue
        baseline = pair[
            (pair["target_ma"] == row["target_ma"])
            & (pair["other_ma"] == row["other_ma"])
            & (pair["condition"] == f"below_{row['other_ma']}_at_event=False")
        ]
        if baseline.empty:
            continue
        base = baseline.iloc[0]
        if int(base["event_count"]) < 50:
            continue
        recovery_delta = None
        if pd.notna(row["recovery_rate_20d"]) and pd.notna(base["recovery_rate_20d"]):
            recovery_delta = float(row["recovery_rate_20d"] - base["recovery_rate_20d"])
        ret_delta = None
        if pd.notna(row["mean_ret_20d_pct"]) and pd.notna(base["mean_ret_20d_pct"]):
            ret_delta = float(row["mean_ret_20d_pct"] - base["mean_ret_20d_pct"])
        if recovery_delta is not None and ret_delta is not None and (recovery_delta <= -0.05 or ret_delta <= -1.0):
            notable.append(
                {
                    "target_ma": row["target_ma"],
                    "other_ma": row["other_ma"],
                    "event_count": int(row["event_count"]),
                    "baseline_event_count": int(base["event_count"]),
                    "recovery_rate_20d_delta": recovery_delta,
                    "mean_ret_20d_pct_delta": ret_delta,
                    "typed_reason": "other_ma_below_at_break_has_weaker_recovery_or_return",
                }
            )
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": "hold",
        "session_aggregate_decision": "hold",
        "authoritative_rollup_decision": "hold",
        "reason": "descriptive_event_study_only_no_candidate_generation_or_trade_rule_promoted",
        "event_count": int(len(events)),
        "notable_interactions": notable[:25],
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DuckDB writes",
            "no production ranking or candidate generator mutation",
            "no validated buy or sell claim",
        ],
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)

    db_path = Path(args.db_path) if args.db_path else resolve_runtime_stock_db_path()
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness()
    db_contract = inspect_runtime_stock_db(runtime_db_path=db_path)

    with duckdb.connect(str(db_path), read_only=True) as conn:
        daily = _load_daily_bars(conn, start_ymd=args.start_ymd, end_ymd=args.end_ymd, confirmed_only=not args.include_yahoo)
    featured = _add_features(daily)
    events = _build_events(featured, max_horizon=max(RECOVERY_HORIZONS))
    if events.empty:
        raise RuntimeError("no MA break events found")

    summary = _event_summary(events)
    interaction = _interaction_summary(events)
    pair = _pair_matrix(events)
    downside = _downside_summary(events)
    decision = _research_decision(events, pair)

    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "db_contract": db_contract,
        "runtime_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "start_ymd": args.start_ymd,
        "end_ymd": args.end_ymd,
        "confirmed_bars_only": not args.include_yahoo,
        "daily_rows": int(len(daily)),
        "code_count": int(daily["code"].nunique()),
        "min_ymd": int(daily["ymd"].min()),
        "max_ymd": int(daily["ymd"].max()),
        "event_count": int(len(events)),
        "artifact_contract": {
            "authoritative_json": ["ma_recovery_summary.json", "research_decision.json", "input_audit.json"],
            "derived_csv": [
                "ma_break_events.csv",
                "ma_recovery_by_interaction.csv",
                "ma_recovery_pair_matrix.csv",
                "downside_continuation_summary.csv",
            ],
        },
    }

    _write_json(out_dir / "input_audit.json", input_audit)
    events.to_csv(out_dir / "ma_break_events.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "ma_recovery_summary.json", summary)
    interaction.to_csv(out_dir / "ma_recovery_by_interaction.csv", index=False, encoding="utf-8")
    pair.to_csv(out_dir / "ma_recovery_pair_matrix.csv", index=False, encoding="utf-8")
    downside.to_csv(out_dir / "downside_continuation_summary.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)

    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    complete = {
        "axis_id": AXIS_ID,
        "status": "complete" if not missing else "incomplete",
        "missing_artifacts": missing,
        "authoritative_result": str(out_dir / "research_decision.json"),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", complete)
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only MA break recovery interaction event study.")
    parser.add_argument("--db-path", default="", help="Optional stocks.duckdb path. Defaults to runtime DB contract.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    parser.add_argument("--include-yahoo", action="store_true", help="Include yahoo/provisional rows. Default excludes them.")
    return parser.parse_args()


def main() -> None:
    out_dir = run(parse_args())
    print(out_dir)


if __name__ == "__main__":
    main()
