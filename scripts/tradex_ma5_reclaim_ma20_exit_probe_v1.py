from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts


AXIS_ID = "ma5_reclaim_ma20_exit_probe_v1"
SCHEMA_PREFIX = "tradex_ma5_reclaim_ma20_exit_probe_v1"
DEFAULT_SOURCE_DB = Path(
    r"G:\Tradex\db\meemee_snapshots\20260512T130453Z_winner_lookalike_candle_decomposition_v1\stocks.duckdb"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma5_reclaim_ma20_exit_probe_v1")

DEFAULT_YEARS = 10
ENTRY_MA = 5
EXIT_MA = 20
CONSECUTIVE_ABOVE_MA5_BARS = 4
MAX_HOLDING_DAYS = 40
MIN_HISTORY_DAYS = 80
SEVERE_LOSS_THRESHOLD = -0.10
MIN_GROUP_TRADES = 50

SIGNAL_FEATURE_COLUMNS = {
    "ma5",
    "ma20",
    "ma60",
    "ma_stack",
    "price_vs_ma20",
    "price_vs_ma60",
    "ma20_vs_ma60",
    "ma20_slope_state",
    "ma60_slope_state",
}
LABEL_COLUMNS = {
    "exit_close",
    "exit_date",
    "ret",
    "mfe",
    "mae",
    "win",
    "severe_loss",
}

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "trade_ledger.jsonl",
    "ma_condition_summary.json",
    "exit_reason_summary.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
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


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _date_norm_expr(column: str) -> str:
    num = f"TRY_CAST({column} AS BIGINT)"
    dte = f"TRY_CAST({column} AS DATE)"
    return (
        "CASE "
        f"WHEN {dte} IS NOT NULL THEN CAST(strftime({dte}, '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 19000101 AND 20991231 THEN CAST({num} AS INTEGER) "
        f"WHEN {num} >= 1000000000000 THEN CAST(strftime(to_timestamp({num} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp({num}), '%Y%m%d') AS INTEGER) "
        "ELSE NULL END"
    )


def _resolve_source_db(source_db: str | Path | None) -> Path:
    if source_db and str(source_db).strip():
        path = Path(str(source_db)).expanduser().resolve()
    elif os.getenv("STOCKS_DB_PATH"):
        path = Path(os.environ["STOCKS_DB_PATH"]).expanduser().resolve()
    else:
        path = DEFAULT_SOURCE_DB.resolve()
    if not path.exists():
        raise FileNotFoundError(f"source DB not found: {path}")
    return path


def _ymd_to_timestamp(value: int) -> pd.Timestamp:
    return pd.to_datetime(str(int(value)), format="%Y%m%d")


def _timestamp_to_ymd(value: pd.Timestamp) -> int:
    return int(value.strftime("%Y%m%d"))


def _load_max_daily_ymd(conn: duckdb.DuckDBPyConnection) -> int:
    expr = _date_norm_expr("date")
    row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars WHERE lower(coalesce(source, '')) = 'pan'").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("daily_bars has no PAN max date")
    return int(row[0])


def _load_daily_rows(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    b_expr = _date_norm_expr("b.date")
    m_expr = _date_norm_expr("m.date")
    frame = conn.execute(
        f"""
        WITH b AS (
            SELECT code, {b_expr} AS ymd, o, h, l, c, v, source
            FROM daily_bars AS b
        ),
        m AS (
            SELECT code, {m_expr} AS ymd, ma20, ma60
            FROM daily_ma AS m
        )
        SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma20, m.ma60
        FROM b
        LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
        WHERE b.ymd BETWEEN ? AND ?
          AND lower(coalesce(b.source, '')) = 'pan'
          AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
        ORDER BY b.code, b.ymd
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["code"] = frame["code"].astype(str)
    return frame


def build_ma_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.sort_values(["code", "date"], kind="stable").copy()
    grouped = work.groupby("code", sort=False)
    work["bar_index"] = grouped.cumcount()
    work["prev_c"] = grouped["c"].shift(1)
    work["ma5"] = grouped["c"].transform(lambda s: s.rolling(ENTRY_MA, min_periods=ENTRY_MA).mean())
    if "ma20" not in work.columns or work["ma20"].isna().all():
        work["ma20"] = grouped["c"].transform(lambda s: s.rolling(EXIT_MA, min_periods=EXIT_MA).mean())
    if "ma60" not in work.columns or work["ma60"].isna().all():
        work["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    work["prev_ma5"] = grouped["ma5"].shift(1)
    work["ma20_slope_20d"] = grouped["ma20"].transform(lambda s: s / s.shift(20) - 1.0)
    work["ma60_slope_20d"] = grouped["ma60"].transform(lambda s: s / s.shift(20) - 1.0)
    work["history_days"] = work["bar_index"] + 1

    work["cross_above_ma5"] = (work["prev_c"] <= work["prev_ma5"]) & (work["c"] > work["ma5"])
    work["above_ma5"] = work["c"] > work["ma5"]

    work["price_vs_ma20"] = "price_below_ma20"
    work.loc[work["c"] >= work["ma20"], "price_vs_ma20"] = "price_above_ma20"
    work["price_vs_ma60"] = "price_below_ma60"
    work.loc[work["c"] >= work["ma60"], "price_vs_ma60"] = "price_above_ma60"
    work["ma20_vs_ma60"] = "ma20_below_ma60"
    work.loc[work["ma20"] >= work["ma60"], "ma20_vs_ma60"] = "ma20_above_ma60"

    work["ma_stack"] = "ma_stack_mixed"
    work.loc[(work["ma5"] > work["ma20"]) & (work["ma20"] > work["ma60"]), "ma_stack"] = "bull_stack_5_20_60"
    work.loc[(work["ma5"] > work["ma20"]) & (work["ma20"] <= work["ma60"]), "ma_stack"] = "ma5_above_20_below_60"
    work.loc[(work["ma5"] <= work["ma20"]) & (work["ma20"] > work["ma60"]), "ma_stack"] = "pullback_in_ma20_above_60"
    work.loc[(work["ma5"] < work["ma20"]) & (work["ma20"] < work["ma60"]), "ma_stack"] = "bear_stack_5_20_60"

    work["ma20_slope_state"] = "ma20_flat"
    work.loc[work["ma20_slope_20d"] >= 0.02, "ma20_slope_state"] = "ma20_rising"
    work.loc[work["ma20_slope_20d"] <= -0.02, "ma20_slope_state"] = "ma20_falling"
    work["ma60_slope_state"] = "ma60_flat"
    work.loc[work["ma60_slope_20d"] >= 0.02, "ma60_slope_state"] = "ma60_rising"
    work.loc[work["ma60_slope_20d"] <= -0.02, "ma60_slope_state"] = "ma60_falling"
    return work


def _signal_confirmed(group: pd.DataFrame, pos: int) -> bool:
    confirm_end = pos + CONSECUTIVE_ABOVE_MA5_BARS - 1
    if confirm_end >= len(group):
        return False
    window = group.iloc[pos : confirm_end + 1]
    return bool(window["above_ma5"].fillna(False).all())


def simulate_trades(features: pd.DataFrame, *, anchor_start_ymd: int, max_holding_days: int = MAX_HOLDING_DAYS) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, group in features.groupby("code", sort=False):
        group = group.sort_values("date", kind="stable").reset_index(drop=True)
        open_until = -1
        for pos, bar in group.iterrows():
            if pos <= open_until:
                continue
            if int(bar["ymd"]) < int(anchor_start_ymd):
                continue
            if int(bar["history_days"]) < MIN_HISTORY_DAYS:
                continue
            if not bool(bar.get("cross_above_ma5")):
                continue
            if not _signal_confirmed(group, int(pos)):
                continue
            signal_pos = int(pos) + CONSECUTIVE_ABOVE_MA5_BARS - 1
            entry_pos = signal_pos + 1
            if entry_pos >= len(group):
                continue
            signal = group.iloc[signal_pos]
            entry = group.iloc[entry_pos]
            if any(pd.isna(signal.get(column)) for column in ("ma5", "ma20", "ma60")):
                continue
            entry_open = _safe_float(entry["o"])
            if entry_open is None or entry_open <= 0.0:
                continue

            exit_limit_pos = min(len(group) - 1, entry_pos + int(max_holding_days) - 1)
            exit_pos = exit_limit_pos
            exit_reason = "max_holding_days"
            for probe_pos in range(entry_pos, exit_limit_pos + 1):
                probe = group.iloc[probe_pos]
                ma20 = _safe_float(probe.get("ma20"))
                close = _safe_float(probe.get("c"))
                if ma20 is not None and close is not None and close < ma20:
                    exit_pos = probe_pos
                    exit_reason = "close_below_ma20"
                    break
            trade_window = group.iloc[entry_pos : exit_pos + 1]
            exit_bar = group.iloc[exit_pos]
            exit_close = float(exit_bar["c"])
            ret = float(exit_close / entry_open - 1.0)
            mfe = float(trade_window["h"].max() / entry_open - 1.0)
            mae = float(trade_window["l"].min() / entry_open - 1.0)
            rows.append(
                {
                    "symbol": str(code),
                    "cross_date": str(bar["date"].date()),
                    "signal_date": str(signal["date"].date()),
                    "entry_date": str(entry["date"].date()),
                    "exit_date": str(exit_bar["date"].date()),
                    "entry_open": entry_open,
                    "exit_close": exit_close,
                    "exit_reason": exit_reason,
                    "holding_days": int(exit_pos - entry_pos + 1),
                    "ret": ret,
                    "mfe": mfe,
                    "mae": mae,
                    "win": ret > 0.0,
                    "severe_loss": ret <= SEVERE_LOSS_THRESHOLD or mae <= SEVERE_LOSS_THRESHOLD,
                    "ma5": _safe_float(signal["ma5"]),
                    "ma20": _safe_float(signal["ma20"]),
                    "ma60": _safe_float(signal["ma60"]),
                    "ma_stack": str(signal["ma_stack"]),
                    "price_vs_ma20": str(signal["price_vs_ma20"]),
                    "price_vs_ma60": str(signal["price_vs_ma60"]),
                    "ma20_vs_ma60": str(signal["ma20_vs_ma60"]),
                    "ma20_slope_state": str(signal["ma20_slope_state"]),
                    "ma60_slope_state": str(signal["ma60_slope_state"]),
                    "ma20_slope_20d": _safe_float(signal["ma20_slope_20d"]),
                    "ma60_slope_20d": _safe_float(signal["ma60_slope_20d"]),
                    "no_lookahead_signal": True,
                }
            )
            open_until = exit_pos
    return pd.DataFrame(rows)


def _profit_factor(frame: pd.DataFrame) -> float | None:
    gains = pd.to_numeric(frame["ret"], errors="coerce")
    positive = float(gains[gains > 0.0].sum())
    negative = float(gains[gains < 0.0].sum())
    if negative == 0.0:
        return None if positive == 0.0 else 999.0
    return float(positive / abs(negative))


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "trade_count": 0,
            "avg_ret": None,
            "median_ret": None,
            "win_rate": None,
            "profit_factor": None,
            "avg_mfe": None,
            "avg_mae": None,
            "severe_loss_rate": None,
            "avg_holding_days": None,
        }
    return {
        "trade_count": int(len(frame)),
        "avg_ret": _safe_float(pd.to_numeric(frame["ret"], errors="coerce").mean()),
        "median_ret": _safe_float(pd.to_numeric(frame["ret"], errors="coerce").median()),
        "win_rate": _safe_float(frame["win"].astype(float).mean()),
        "profit_factor": _profit_factor(frame),
        "avg_mfe": _safe_float(pd.to_numeric(frame["mfe"], errors="coerce").mean()),
        "avg_mae": _safe_float(pd.to_numeric(frame["mae"], errors="coerce").mean()),
        "severe_loss_rate": _safe_float(frame["severe_loss"].astype(float).mean()),
        "avg_holding_days": _safe_float(pd.to_numeric(frame["holding_days"], errors="coerce").mean()),
    }


def _classify_condition(metrics: dict[str, Any]) -> str:
    count = int(metrics.get("trade_count") or 0)
    avg_ret = metrics.get("avg_ret")
    win_rate = metrics.get("win_rate")
    profit_factor = metrics.get("profit_factor")
    severe = metrics.get("severe_loss_rate")
    if count < MIN_GROUP_TRADES:
        return "insufficient_sample"
    if (
        avg_ret is not None
        and win_rate is not None
        and profit_factor is not None
        and severe is not None
        and avg_ret > 0.0
        and win_rate >= 0.52
        and profit_factor >= 1.15
        and severe <= 0.12
    ):
        return "works_high_win_rate"
    if (
        avg_ret is not None
        and profit_factor is not None
        and severe is not None
        and avg_ret > 0.0
        and profit_factor >= 1.10
        and severe <= 0.12
    ):
        return "positive_expectancy_low_win_rate"
    if avg_ret is not None and profit_factor is not None and (avg_ret < 0.0 or profit_factor < 1.0):
        return "fails"
    return "mixed"


def build_condition_summary(trades: pd.DataFrame) -> dict[str, Any]:
    group_specs = [
        ("overall", []),
        ("ma_stack", ["ma_stack"]),
        ("price_vs_ma60", ["price_vs_ma60"]),
        ("ma20_vs_ma60", ["ma20_vs_ma60"]),
        ("ma20_slope_state", ["ma20_slope_state"]),
        ("ma60_slope_state", ["ma60_slope_state"]),
        ("ma_stack_x_ma60_slope", ["ma_stack", "ma60_slope_state"]),
        ("ma_stack_x_price_vs_ma60", ["ma_stack", "price_vs_ma60"]),
    ]
    rows: list[dict[str, Any]] = []
    for family, columns in group_specs:
        if not columns:
            metrics = _metrics(trades)
            rows.append({"condition_family": family, "condition": "all", **metrics, "condition_decision": _classify_condition(metrics)})
            continue
        for keys, group in trades.groupby(columns, dropna=False, sort=True):
            if not isinstance(keys, tuple):
                keys = (keys,)
            condition = "|".join(f"{col}={value}" for col, value in zip(columns, keys))
            metrics = _metrics(group)
            rows.append({"condition_family": family, "condition": condition, **metrics, "condition_decision": _classify_condition(metrics)})
    rows = sorted(rows, key=lambda row: (row["condition_family"], -(row["trade_count"] or 0), row["condition"]))
    return {
        "schema_version": f"{SCHEMA_PREFIX}_ma_condition_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "entry_rule": "close crosses above MA5 from below; cross candle counts as bar 1; require 4 consecutive closes above MA5; buy next session open",
        "exit_rule": "sell at close when close < MA20, otherwise max_holding_days exit",
        "min_group_trades": MIN_GROUP_TRADES,
        "rows": rows,
    }


def build_exit_reason_summary(trades: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for reason, group in trades.groupby("exit_reason", dropna=False, sort=True):
        rows.append({"exit_reason": str(reason), **_metrics(group)})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_exit_reason_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }


def build_feature_availability_audit(features: pd.DataFrame, trades: pd.DataFrame) -> dict[str, Any]:
    feature_rows = []
    for column in sorted(SIGNAL_FEATURE_COLUMNS):
        present = column in features.columns or column in trades.columns
        source = features if column in features.columns else trades
        non_null = int(source[column].notna().sum()) if present and column in source.columns else 0
        total = int(len(source)) if present else 0
        feature_rows.append(
            {
                "column": column,
                "present": present,
                "non_null_count": non_null,
                "non_null_rate": None if total == 0 else float(non_null / total),
            }
        )
    overlap = sorted(SIGNAL_FEATURE_COLUMNS & LABEL_COLUMNS)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "bar_rows": int(len(features)),
        "trade_rows": int(len(trades)),
        "feature_rows": feature_rows,
        "signal_feature_columns": sorted(SIGNAL_FEATURE_COLUMNS),
        "label_columns_excluded_from_signal": sorted(LABEL_COLUMNS),
        "signal_label_overlap": overlap,
        "used_future_labels_in_signal": bool(overlap),
        "silent_fallback_used": False,
    }


def build_evaluation_contract(*, source_db: Path, anchor_start_ymd: int, max_daily_ymd: int, years: int) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "research_phase": "MA rule simulation / condition decomposition",
        "boundary": "TRADEX-only",
        "source_db": str(source_db),
        "anchor_start_ymd": int(anchor_start_ymd),
        "max_daily_ymd": int(max_daily_ymd),
        "requested_years": int(years),
        "entry_rule": {
            "ma": ENTRY_MA,
            "cross": "previous close <= previous MA5 and current close > current MA5",
            "confirmation": f"{CONSECUTIVE_ABOVE_MA5_BARS} consecutive closes above MA5; cross candle counts as bar 1",
            "fill": "next_session_open_after_confirmation",
        },
        "exit_rule": {
            "ma": EXIT_MA,
            "exit": "first close below MA20 after entry",
            "fallback_exit": f"close at max_holding_days={MAX_HOLDING_DAYS}",
        },
        "same_condition_controls": {
            "same_universe_source": "runtime snapshot daily_bars pan source",
            "same_period": True,
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "future_label_policy": {
            "future_prices_used_for_trade_outcome": True,
            "future_prices_used_for_signal": False,
        },
        "candidate_scoring_created": False,
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_research_decision(trades: pd.DataFrame, condition_summary: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    overall = next(row for row in condition_summary["rows"] if row["condition_family"] == "overall")
    positive_decisions = {"works_high_win_rate", "positive_expectancy_low_win_rate"}
    works = [row for row in condition_summary["rows"] if row["condition_decision"] in positive_decisions and row["condition_family"] != "overall"]
    fails = [row for row in condition_summary["rows"] if row["condition_decision"] == "fails" and row["condition_family"] != "overall"]
    if overall["condition_decision"] == "works_high_win_rate" and len(works) >= 2:
        decision = "rule_has_conditioned_high_win_rate_edge"
    elif overall["condition_decision"] == "positive_expectancy_low_win_rate" and len(works) >= 2:
        decision = "rule_has_positive_expectancy_but_low_win_rate"
    elif len(works) >= 1 and len(fails) >= 1:
        decision = "edge_is_context_dependent"
    elif overall["condition_decision"] == "fails":
        decision = "rule_not_useful"
    else:
        decision = "inconclusive"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": decision,
        "overall": overall,
        "working_conditions": sorted(works, key=lambda row: (row["avg_ret"] or -999.0), reverse=True)[:20],
        "failing_conditions": sorted(fails, key=lambda row: (row["avg_ret"] if row["avg_ret"] is not None else 999.0))[:20],
        "decision_reasons": [
            {"code": "overall_condition_decision", "value": overall["condition_decision"]},
            {"code": "working_condition_count", "value": len(works)},
            {"code": "failing_condition_count", "value": len(fails)},
            {"code": "trade_count", "value": int(len(trades))},
        ],
        "candidate_scoring_created": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    existing = {name: Path(path).exists() for name, path in paths.items()}
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": {**existing, **required_existing},
        "complete": all(existing.values()) and all(required_existing.values()),
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_ma5_reclaim_ma20_exit_probe_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    years: int = DEFAULT_YEARS,
) -> dict[str, Any]:
    source_path = _resolve_source_db(source_db)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        max_daily_ymd = _load_max_daily_ymd(conn)
        max_daily_ts = _ymd_to_timestamp(max_daily_ymd)
        anchor_start_ts = max_daily_ts - pd.DateOffset(years=int(years))
        data_start_ts = anchor_start_ts - pd.DateOffset(days=140)
        anchor_start_ymd = _timestamp_to_ymd(anchor_start_ts)
        data_start_ymd = _timestamp_to_ymd(data_start_ts)
        bars = _load_daily_rows(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
    finally:
        conn.close()

    features = build_ma_features(bars)
    trades = simulate_trades(features, anchor_start_ymd=anchor_start_ymd)
    if trades.empty:
        raise RuntimeError("strategy produced no trades")
    evaluation_contract = build_evaluation_contract(
        source_db=source_path,
        anchor_start_ymd=anchor_start_ymd,
        max_daily_ymd=max_daily_ymd,
        years=years,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_db", "path": str(source_path)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(max_daily_ymd),
        config={
            "axis_id": AXIS_ID,
            "entry_ma": ENTRY_MA,
            "exit_ma": EXIT_MA,
            "consecutive_above_ma5_bars": CONSECUTIVE_ABOVE_MA5_BARS,
            "max_holding_days": MAX_HOLDING_DAYS,
            "years": int(years),
            "candidate_scoring_created": False,
        },
        universe=sorted(trades["symbol"].astype(str).unique().tolist()),
        period={"start_date": str(anchor_start_ymd), "end_date": str(max_daily_ymd), "label": "daily_signal_backtest"},
        horizon=f"exit_below_ma{EXIT_MA}_or_{MAX_HOLDING_DAYS}d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    feature_audit = build_feature_availability_audit(features, trades)
    condition_summary = build_condition_summary(trades)
    exit_summary = build_exit_reason_summary(trades)
    research_decision = build_research_decision(trades, condition_summary, output_dir)

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "ma_condition_summary.json": condition_summary,
        "exit_reason_summary.json": exit_summary,
        "research_decision.json": research_decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["trade_ledger.jsonl"] = str(_write_jsonl(output_dir / "trade_ledger.jsonl", trades.to_dict(orient="records")))
    complete = _artifact_complete(output_dir, paths, research_decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "authoritative_research_decision": research_decision["authoritative_research_decision"],
        "overall": research_decision["overall"],
        "working_conditions": research_decision["working_conditions"][:10],
        "failing_conditions": research_decision["failing_conditions"][:10],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--years", type=int, default=DEFAULT_YEARS)
    args = parser.parse_args(argv)
    result = run_ma5_reclaim_ma20_exit_probe_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        years=args.years,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
