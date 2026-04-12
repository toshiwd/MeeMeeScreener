from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd

from external_analysis.exporter.source_reader import connect_source_db, source_table_exists
from external_analysis.labels.store import connect_label_db
from external_analysis.models.forecast_surface_learning import load_or_train_forecast_surface_bundle
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema

EVALUATION_VERSION = "forecast_surface_eval_v1"
DEFAULT_TOP_K = 20
DEFAULT_MIN_FOLDS = 12
DEFAULT_MIN_DAILY_COUNT = 250
SUPPORTED_HORIZONS = (5, 10, 20)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _as_of_date_text(value: int) -> str:
    text = f"{int(value):08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _json_dump(value: Any) -> str:
    return json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)


def _json_load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed if parsed is not None else default


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _read_latest_publish_id(conn: duckdb.DuckDBPyConnection) -> str | None:
    if not _table_exists(conn, "publish_pointer"):
        return None
    row = conn.execute(
        """
        SELECT publish_id
        FROM publish_pointer
        WHERE pointer_name = 'latest_successful'
        """,
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def _as_date_expr(column_name: str) -> str:
    return f"CAST(strftime(CAST({column_name} AS DATE), '%Y%m%d') AS INTEGER)"


def _load_surface_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    publish_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not _table_exists(conn, "forecast_surface_daily"):
        return pd.DataFrame()
    query = """
        SELECT
            publish_id,
            CAST(strftime(as_of_date, '%Y%m%d') AS INTEGER) AS as_of_date,
            code,
            side,
            action_state,
            direction_prob,
            expected_ret_5,
            expected_ret_10,
            expected_ret_20,
            expected_mfe_20,
            expected_mae_20,
            opportunity_score,
            freshness_state
        FROM forecast_surface_daily
    """
    params: list[Any] = []
    if publish_ids:
        placeholders = ", ".join(["?"] * len(publish_ids))
        query += f" WHERE publish_id IN ({placeholders})"
        params.extend([str(publish_id) for publish_id in publish_ids])
    return conn.execute(query, params).fetchdf()


def _load_candidate_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    publish_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not _table_exists(conn, "candidate_daily"):
        return pd.DataFrame()
    query = """
        SELECT
            publish_id,
            CAST(strftime(as_of_date, '%Y%m%d') AS INTEGER) AS as_of_date,
            code,
            side,
            rank_position,
            candidate_score,
            expected_horizon_days,
            regime_tag,
            freshness_state
        FROM candidate_daily
    """
    params: list[Any] = []
    if publish_ids:
        placeholders = ", ".join(["?"] * len(publish_ids))
        query += f" WHERE publish_id IN ({placeholders})"
        params.extend([str(publish_id) for publish_id in publish_ids])
    return conn.execute(query, params).fetchdf()


def _load_regime_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    publish_ids: list[str] | None = None,
) -> dict[str, str | None]:
    if not _table_exists(conn, "regime_daily"):
        return {}
    query = """
        SELECT publish_id, MAX(regime_tag) AS regime_tag
        FROM regime_daily
    """
    params: list[Any] = []
    if publish_ids:
        placeholders = ", ".join(["?"] * len(publish_ids))
        query += f" WHERE publish_id IN ({placeholders})"
        params.extend([str(publish_id) for publish_id in publish_ids])
    query += " GROUP BY publish_id"
    rows = conn.execute(query, params).fetchall()
    return {str(row[0]): (None if row[1] is None else str(row[1])) for row in rows}


def _load_label_rows(
    label_conn: duckdb.DuckDBPyConnection,
    *,
    horizon: int,
    dates: list[int],
) -> pd.DataFrame:
    table_name = f"label_daily_h{int(horizon)}"
    if not _table_exists(label_conn, table_name) or not dates:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(dates))
    query = f"""
        SELECT
            as_of_date,
            code,
            ret_h,
            mfe_h,
            mae_h,
            days_to_mfe_h,
            days_to_stop_h
        FROM {table_name}
        WHERE as_of_date IN ({placeholders})
    """
    return label_conn.execute(query, [int(date) for date in dates]).fetchdf()


def _load_signal_rows(
    source_db_path: str | None,
    *,
    dates: list[int],
) -> pd.DataFrame:
    if not source_db_path or not dates:
        return pd.DataFrame()
    source_conn = connect_source_db(source_db_path)
    try:
        if not source_table_exists(source_conn, "signal_decision_daily"):
            return pd.DataFrame()
        placeholders = ", ".join(["?"] * len(dates))
        query = f"""
            SELECT
                dt AS as_of_date,
                code,
                side,
                entry_qualified,
                forward_return_5,
                forward_return_20,
                forward_return_30,
                forward_return_60
            FROM signal_decision_daily
            WHERE dt IN ({placeholders})
              AND COALESCE(entry_qualified, FALSE) = TRUE
        """
        return source_conn.execute(query, [int(date) for date in dates]).fetchdf()
    finally:
        source_conn.close()


def _signed_values(side: str, ret_h: float | None, mfe_h: float | None, mae_h: float | None) -> tuple[float | None, float | None, float | None]:
    if ret_h is None:
        signed_ret = None
    else:
        signed_ret = float(ret_h) if side == "long" else float(-ret_h)
    if mfe_h is None:
        signed_mfe = None
    else:
        signed_mfe = float(mfe_h) if side == "long" else float(-mae_h if mae_h is not None else 0.0)
    if mae_h is None:
        signed_mae = None
    else:
        signed_mae = float(mae_h) if side == "long" else float(-mfe_h if mfe_h is not None else 0.0)
    return signed_ret, signed_mfe, signed_mae


def _metric_from_frame(
    *,
    selected: pd.DataFrame,
    labels: pd.DataFrame,
    side: str,
) -> dict[str, Any]:
    if selected.empty or labels.empty:
        return {
            "sample_count": 0,
            "top_mean_ret_net": None,
            "top_mean_mfe_net": None,
            "top_mean_mae_net": None,
            "top_win_rate": None,
            "top_mean_prob": None,
            "top_positive_rate": None,
            "top_brier": None,
            "top_calibration_gap": None,
        }
    merged = selected.merge(labels, on=["as_of_date", "code"], how="inner")
    if merged.empty:
        return {
            "sample_count": 0,
            "top_mean_ret_net": None,
            "top_mean_mfe_net": None,
            "top_mean_mae_net": None,
            "top_win_rate": None,
            "top_mean_prob": None,
            "top_positive_rate": None,
            "top_brier": None,
            "top_calibration_gap": None,
        }
    signed_ret: list[float] = []
    signed_mfe: list[float] = []
    signed_mae: list[float] = []
    wins: list[float] = []
    brier: list[float] = []
    probs: list[float] = []
    actuals: list[float] = []
    for _, row in merged.iterrows():
        ret_value = row.get("ret_h")
        mfe_value = row.get("mfe_h")
        mae_value = row.get("mae_h")
        ret = None if pd.isna(ret_value) else float(ret_value)
        mfe = None if pd.isna(mfe_value) else float(mfe_value)
        mae = None if pd.isna(mae_value) else float(mae_value)
        signed_value, signed_favorable, signed_adverse = _signed_values(side, ret, mfe, mae)
        if signed_value is None:
            continue
        signed_ret.append(float(signed_value))
        if signed_favorable is not None:
            signed_mfe.append(float(signed_favorable))
        if signed_adverse is not None:
            signed_mae.append(float(signed_adverse))
        actual = 1.0 if signed_value > 0 else 0.0
        prob_value = row.get("direction_prob")
        prob = float(prob_value) if prob_value is not None and not pd.isna(prob_value) else 0.5
        probs.append(prob)
        actuals.append(actual)
        wins.append(1.0 if signed_value > 0 else 0.0)
        brier.append((prob - actual) ** 2)
    if not signed_ret:
        return {
            "sample_count": 0,
            "top_mean_ret_net": None,
            "top_mean_mfe_net": None,
            "top_mean_mae_net": None,
            "top_win_rate": None,
            "top_mean_prob": None,
            "top_positive_rate": None,
            "top_brier": None,
            "top_calibration_gap": None,
        }
    mean_prob = sum(probs) / len(probs) if probs else None
    positive_rate = sum(actuals) / len(actuals) if actuals else None
    return {
        "sample_count": int(len(signed_ret)),
        "top_mean_ret_net": float(sum(signed_ret) / len(signed_ret)),
        "top_mean_mfe_net": float(sum(signed_mfe) / len(signed_mfe)) if signed_mfe else None,
        "top_mean_mae_net": float(sum(signed_mae) / len(signed_mae)) if signed_mae else None,
        "top_win_rate": float(sum(wins) / len(wins)) if wins else None,
        "top_mean_prob": None if mean_prob is None else float(mean_prob),
        "top_positive_rate": None if positive_rate is None else float(positive_rate),
        "top_brier": float(sum(brier) / len(brier)) if brier else None,
        "top_calibration_gap": None if mean_prob is None or positive_rate is None else float(abs(mean_prob - positive_rate)),
    }


def _signal_metric_from_frame(
    *,
    signal_rows: pd.DataFrame,
    as_of_date: int,
    side: str,
    horizon: int,
) -> dict[str, Any]:
    if signal_rows.empty:
        return {"signal_mean_ret_net": None, "signal_sample_count": 0}
    side_key = "buy" if side == "long" else "sell"
    filtered = signal_rows[(signal_rows["as_of_date"] == int(as_of_date)) & (signal_rows["side"].astype(str).str.lower() == side_key)]
    if filtered.empty:
        return {"signal_mean_ret_net": None, "signal_sample_count": 0}
    # The source only carries explicit 5/20/30/60 day signal returns.
    # Keep 10-day evaluation from inheriting a misleading proxy column.
    column = {5: "forward_return_5", 20: "forward_return_20"}.get(int(horizon))
    if column is None:
        return {"signal_mean_ret_net": None, "signal_sample_count": 0}
    values: list[float] = []
    for _, row in filtered.iterrows():
        value = row.get(column)
        if value is None or pd.isna(value):
            continue
        signed = float(value) if side == "long" else float(-value)
        values.append(signed)
    if not values:
        return {"signal_mean_ret_net": None, "signal_sample_count": 0}
    return {
        "signal_mean_ret_net": float(sum(values) / len(values)),
        "signal_sample_count": int(len(values)),
    }


def _candidate_metric_from_frame(
    *,
    candidate_rows: pd.DataFrame,
    labels: pd.DataFrame,
    as_of_date: int,
    side: str,
) -> dict[str, Any]:
    if candidate_rows.empty or labels.empty:
        return {"candidate_mean_ret_net": None, "candidate_win_rate": None}
    selected = candidate_rows[(candidate_rows["as_of_date"] == int(as_of_date)) & (candidate_rows["side"].astype(str) == side)]
    if selected.empty:
        return {"candidate_mean_ret_net": None, "candidate_win_rate": None}
    selected = selected.sort_values(["candidate_score", "code"], ascending=[False, True]).head(DEFAULT_TOP_K)
    merged = selected.merge(labels, on=["as_of_date", "code"], how="inner")
    if merged.empty:
        return {"candidate_mean_ret_net": None, "candidate_win_rate": None}
    signed_ret: list[float] = []
    wins: list[float] = []
    for _, row in merged.iterrows():
        ret_value = row.get("ret_h")
        if ret_value is None or pd.isna(ret_value):
            continue
        ret = float(ret_value)
        signed = ret if side == "long" else -ret
        signed_ret.append(signed)
        wins.append(1.0 if signed > 0 else 0.0)
    if not signed_ret:
        return {"candidate_mean_ret_net": None, "candidate_win_rate": None}
    return {
        "candidate_mean_ret_net": float(sum(signed_ret) / len(signed_ret)),
        "candidate_win_rate": float(sum(wins) / len(wins)) if wins else None,
    }


def _evaluate_fold_rows(
    *,
    publish_id: str,
    scope_type: str,
    surface_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    label_frames: dict[int, pd.DataFrame],
    signal_rows: pd.DataFrame,
    regime_tag: str | None,
    top_k: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if surface_df.empty:
        return rows
    dates = sorted({int(value) for value in surface_df["as_of_date"].tolist() if value is not None})
    for as_of_date in dates:
        day_surface = surface_df[surface_df["as_of_date"] == int(as_of_date)]
        day_candidates = candidate_df[candidate_df["as_of_date"] == int(as_of_date)] if not candidate_df.empty else pd.DataFrame()
        for side in ("long", "short"):
            selected = day_surface[day_surface["side"].astype(str) == side]
            if selected.empty:
                continue
            actionable = selected[selected["action_state"].astype(str).isin({"enter", "wait"})]
            if not actionable.empty:
                selected = actionable
            elif "opportunity_score" in selected.columns:
                positive = selected[pd.to_numeric(selected["opportunity_score"], errors="coerce").fillna(0.0) > 0.0]
                if not positive.empty:
                    selected = positive
            selected = selected.sort_values(["opportunity_score", "code"], ascending=[False, True]).head(top_k)
            candidate_selected = day_candidates[day_candidates["side"].astype(str) == side] if not day_candidates.empty else pd.DataFrame()
            if not candidate_selected.empty:
                candidate_selected = candidate_selected.sort_values(["candidate_score", "code"], ascending=[False, True]).head(top_k)
            for horizon, labels in label_frames.items():
                label_slice = labels[labels["as_of_date"] == int(as_of_date)] if not labels.empty else pd.DataFrame()
                metric = _metric_from_frame(selected=selected, labels=label_slice, side=side)
                candidate_metric = _candidate_metric_from_frame(
                    candidate_rows=day_candidates,
                    labels=label_slice,
                    as_of_date=as_of_date,
                    side=side,
                )
                signal_metric = _signal_metric_from_frame(
                    signal_rows=signal_rows,
                    as_of_date=as_of_date,
                    side=side,
                    horizon=horizon,
                )
                row = {
                    "run_id": None,
                    "scope_type": scope_type,
                    "publish_id": publish_id,
                    "as_of_date": _as_of_date_text(int(as_of_date)),
                    "regime_tag": regime_tag,
                    "side": side,
                    "horizon_days": int(horizon),
                    "top_k": int(top_k),
                    "sample_count": int(metric["sample_count"]),
                    "top_mean_ret_net": metric["top_mean_ret_net"],
                    "top_mean_mfe_net": metric["top_mean_mfe_net"],
                    "top_mean_mae_net": metric["top_mean_mae_net"],
                    "top_win_rate": metric["top_win_rate"],
                    "top_mean_prob": metric["top_mean_prob"],
                    "top_positive_rate": metric["top_positive_rate"],
                    "top_brier": metric["top_brier"],
                    "top_calibration_gap": metric["top_calibration_gap"],
                    "candidate_mean_ret_net": candidate_metric["candidate_mean_ret_net"],
                    "candidate_win_rate": candidate_metric["candidate_win_rate"],
                    "signal_mean_ret_net": signal_metric["signal_mean_ret_net"],
                    "signal_sample_count": signal_metric["signal_sample_count"],
                    "created_at": _utcnow(),
                }
                rows.append(row)
    return rows


def _aggregate_summary(
    *,
    run_id: str,
    scope_type: str,
    publish_id: str | None,
    surface_df: pd.DataFrame,
    fold_rows: list[dict[str, Any]],
    top_k: int,
    min_folds: int,
    min_daily_count: int,
) -> dict[str, Any]:
    fold_df = pd.DataFrame(fold_rows)
    all_dates = sorted({int(value) for value in surface_df["as_of_date"].tolist() if value is not None}) if not surface_df.empty else []
    horizon_df = fold_df[fold_df["horizon_days"] == 20] if not fold_df.empty else pd.DataFrame()
    fold_count = int(len({str(value) for value in surface_df["publish_id"].tolist() if value is not None})) if not surface_df.empty else 0
    daily_count = int(len(all_dates))

    def _mean(column: str) -> float | None:
        if horizon_df.empty or column not in horizon_df.columns:
            return None
        series = pd.to_numeric(horizon_df[column], errors="coerce").dropna()
        if series.empty:
            return None
        return float(series.mean())

    def _weighted_mean(frame: pd.DataFrame, column: str) -> float | None:
        if frame.empty or column not in frame.columns or "sample_count" not in frame.columns:
            return None
        values = pd.to_numeric(frame[column], errors="coerce")
        weights = pd.to_numeric(frame["sample_count"], errors="coerce")
        valid = values.notna() & weights.notna() & (weights > 0)
        if not bool(valid.any()):
            return None
        return float((values[valid] * weights[valid]).sum() / weights[valid].sum())

    def _weighted_calibration_gap(frame: pd.DataFrame) -> float | None:
        mean_prob = _weighted_mean(frame, "top_mean_prob")
        positive_rate = _weighted_mean(frame, "top_positive_rate")
        if mean_prob is None or positive_rate is None:
            return None
        return float(abs(mean_prob - positive_rate))

    top_long_mean_ret20_net = _mean("top_mean_ret_net") if not horizon_df.empty else None
    top_short_mean_ret20_net = None
    if not horizon_df.empty:
        short_series = pd.to_numeric(horizon_df[horizon_df["side"] == "short"]["top_mean_ret_net"], errors="coerce").dropna()
        top_short_mean_ret20_net = float(short_series.mean()) if not short_series.empty else None
        long_series = pd.to_numeric(horizon_df[horizon_df["side"] == "long"]["top_mean_ret_net"], errors="coerce").dropna()
        top_long_mean_ret20_net = float(long_series.mean()) if not long_series.empty else top_long_mean_ret20_net
    top_combined_mean_ret20_net = None
    if top_long_mean_ret20_net is not None and top_short_mean_ret20_net is not None:
        top_combined_mean_ret20_net = float((top_long_mean_ret20_net + top_short_mean_ret20_net) / 2.0)

    candidate_long_mean_ret20_net = None
    candidate_short_mean_ret20_net = None
    candidate_combined_mean_ret20_net = None
    if not horizon_df.empty:
        long_series = pd.to_numeric(horizon_df[horizon_df["side"] == "long"]["candidate_mean_ret_net"], errors="coerce").dropna()
        short_series = pd.to_numeric(horizon_df[horizon_df["side"] == "short"]["candidate_mean_ret_net"], errors="coerce").dropna()
        if not long_series.empty:
            candidate_long_mean_ret20_net = float(long_series.mean())
        if not short_series.empty:
            candidate_short_mean_ret20_net = float(short_series.mean())
        if candidate_long_mean_ret20_net is not None and candidate_short_mean_ret20_net is not None:
            candidate_combined_mean_ret20_net = float((candidate_long_mean_ret20_net + candidate_short_mean_ret20_net) / 2.0)

    signal_long_mean_ret20_net = None
    signal_short_mean_ret20_net = None
    if not horizon_df.empty:
        long_series = pd.to_numeric(horizon_df[horizon_df["side"] == "long"]["signal_mean_ret_net"], errors="coerce").dropna()
        short_series = pd.to_numeric(horizon_df[horizon_df["side"] == "short"]["signal_mean_ret_net"], errors="coerce").dropna()
        if not long_series.empty:
            signal_long_mean_ret20_net = float(long_series.mean())
        if not short_series.empty:
            signal_short_mean_ret20_net = float(short_series.mean())

    direction_brier_long = None
    direction_brier_short = None
    calibration_gap_long = None
    calibration_gap_short = None
    if not horizon_df.empty:
        long_frame = horizon_df[horizon_df["side"] == "long"]
        short_frame = horizon_df[horizon_df["side"] == "short"]
        direction_brier_long = _weighted_mean(long_frame, "top_brier")
        direction_brier_short = _weighted_mean(short_frame, "top_brier")
        calibration_gap_long = _weighted_calibration_gap(long_frame)
        calibration_gap_short = _weighted_calibration_gap(short_frame)

    top_k_uplift = None
    if top_combined_mean_ret20_net is not None and candidate_combined_mean_ret20_net is not None:
        top_k_uplift = float(top_combined_mean_ret20_net - candidate_combined_mean_ret20_net)

    regime_breakdown: dict[str, dict[str, Any]] = {}
    worst_regime_combined_mean_ret20_net = None
    if not horizon_df.empty and "regime_tag" in horizon_df.columns:
        for regime_tag, group in horizon_df.groupby(horizon_df["regime_tag"].fillna("unknown")):
            combined_series = pd.to_numeric(group["top_mean_ret_net"], errors="coerce").dropna()
            if combined_series.empty:
                continue
            sample_series = pd.to_numeric(group["sample_count"], errors="coerce").dropna()
            regime_breakdown[str(regime_tag)] = {
                "fold_count": int(len(group)),
                "sample_count": int(sample_series.sum()) if not sample_series.empty else 0,
                "combined_mean_ret20_net": float(combined_series.mean()),
                "best_fold_mean_ret20_net": float(combined_series.max()),
                "worst_fold_mean_ret20_net": float(combined_series.min()),
            }
        if regime_breakdown:
            worst_regime_combined_mean_ret20_net = min(
                float(item["combined_mean_ret20_net"]) for item in regime_breakdown.values()
            )

    gate_reasons: list[str] = []
    readiness_pass = True
    effective_min_folds = int(min_folds) if scope_type == "walk_forward" else 1
    effective_min_daily_count = int(min_daily_count) if scope_type == "walk_forward" else 1
    if fold_count < effective_min_folds:
        readiness_pass = False
        gate_reasons.append("insufficient_fold_count")
    if daily_count < effective_min_daily_count:
        readiness_pass = False
        gate_reasons.append("insufficient_daily_count")
    if top_combined_mean_ret20_net is None or top_combined_mean_ret20_net <= 0:
        readiness_pass = False
        gate_reasons.append("non_positive_combined_return")
    if top_k_uplift is None:
        readiness_pass = False
        gate_reasons.append("candidate_baseline_missing")
    elif top_k_uplift <= 0:
        readiness_pass = False
        gate_reasons.append("non_positive_top_k_uplift")
    if candidate_combined_mean_ret20_net is not None and top_combined_mean_ret20_net is not None and top_combined_mean_ret20_net <= candidate_combined_mean_ret20_net:
        readiness_pass = False
        gate_reasons.append("below_candidate_baseline")
    if top_long_mean_ret20_net is None or top_long_mean_ret20_net <= 0:
        readiness_pass = False
        gate_reasons.append("non_positive_long_return")
    if top_short_mean_ret20_net is None or top_short_mean_ret20_net <= 0:
        readiness_pass = False
        gate_reasons.append("non_positive_short_return")
    if direction_brier_long is not None and direction_brier_long > 0.25:
        readiness_pass = False
        gate_reasons.append("long_calibration_poor")
    if direction_brier_short is not None and direction_brier_short > 0.25:
        readiness_pass = False
        gate_reasons.append("short_calibration_poor")
    if calibration_gap_long is not None and calibration_gap_long > 0.10:
        readiness_pass = False
        gate_reasons.append("long_calibration_gap_poor")
    if calibration_gap_short is not None and calibration_gap_short > 0.10:
        readiness_pass = False
        gate_reasons.append("short_calibration_gap_poor")
    if worst_regime_combined_mean_ret20_net is not None and worst_regime_combined_mean_ret20_net < -0.02:
        readiness_pass = False
        gate_reasons.append("regime_instability")
    primary_gate_reason = "gate_passed" if readiness_pass else gate_reasons[0]

    return {
        "run_id": run_id,
        "scope_type": scope_type,
        "publish_id": publish_id,
        "as_of_date": None if surface_df.empty else _as_of_date_text(int(surface_df["as_of_date"].min())),
        "model_version": EVALUATION_VERSION,
        "top_k": int(top_k),
        "fold_count": int(fold_count),
        "daily_count": int(daily_count),
        "horizon_count": len(SUPPORTED_HORIZONS),
        "top_long_mean_ret20_net": top_long_mean_ret20_net,
        "top_short_mean_ret20_net": top_short_mean_ret20_net,
        "top_combined_mean_ret20_net": top_combined_mean_ret20_net,
        "candidate_long_mean_ret20_net": candidate_long_mean_ret20_net,
        "candidate_short_mean_ret20_net": candidate_short_mean_ret20_net,
        "candidate_combined_mean_ret20_net": candidate_combined_mean_ret20_net,
        "signal_long_mean_ret20_net": signal_long_mean_ret20_net,
        "signal_short_mean_ret20_net": signal_short_mean_ret20_net,
        "direction_brier_long": direction_brier_long,
        "direction_brier_short": direction_brier_short,
        "calibration_gap_long": calibration_gap_long,
        "calibration_gap_short": calibration_gap_short,
        "top_k_uplift": top_k_uplift,
        "worst_regime_combined_mean_ret20_net": worst_regime_combined_mean_ret20_net,
        "regime_breakdown_json": regime_breakdown,
        "fold_metrics_json": fold_rows,
        "primary_gate_reason": primary_gate_reason,
        "gate_failures_json": gate_reasons,
        "calibration_method_long": None,
        "calibration_method_short": None,
        "ready_streak": 1 if readiness_pass else 0,
        "recent_ready_count_20": 1 if readiness_pass else 0,
        "readiness_pass": readiness_pass,
        "gate_reason": primary_gate_reason,
        "created_at": _utcnow(),
    }


def _materialize_publish_streaks(
    conn: duckdb.DuckDBPyConnection,
    *,
    publish_id: str,
    current_as_of_date: str | None,
    current_readiness_pass: bool,
) -> tuple[int, int]:
    if not _table_exists(conn, "forecast_surface_evaluation_runs"):
        return (1 if current_readiness_pass else 0, 1 if current_readiness_pass else 0)
    rows = conn.execute(
        """
        SELECT CAST(as_of_date AS VARCHAR), readiness_pass
        FROM forecast_surface_evaluation_runs
        WHERE scope_type = 'publish'
          AND COALESCE(publish_id, '') <> COALESCE(?, '')
        ORDER BY as_of_date DESC NULLS LAST, created_at DESC, run_id DESC
        LIMIT 19
        """,
        [publish_id],
    ).fetchall()
    series: list[bool] = [bool(current_readiness_pass)] + [bool(row[1]) for row in rows]
    ready_streak = 0
    for item in series:
        if item:
            ready_streak += 1
        else:
            break
    recent_ready_count_20 = sum(1 for item in series[:20] if item)
    return ready_streak, recent_ready_count_20


def summarize_forecast_surface_shadow_run(
    *,
    result_db_path: str | None = None,
    publish_id_prefix: str = "shadow20_",
    min_days: int = 20,
    min_universe_code_count: int = 650,
) -> dict[str, Any]:
    """Summarize shadow-run readiness without trusting coverage alone."""
    prefix = str(publish_id_prefix or "").strip()
    required_days = max(1, int(min_days))
    required_universe = max(1, int(min_universe_code_count))
    conn = connect_result_db(result_db_path, read_only=True)
    try:
        if not _table_exists(conn, "forecast_surface_runs"):
            return {
                "ok": False,
                "acceptance_pass": False,
                "primary_reason": "forecast_surface_runs_missing",
                "publish_id_prefix": prefix,
                "observed_days": 0,
                "required_days": required_days,
                "min_universe_code_count": required_universe,
                "coverage_pass_count": 0,
                "universe_pass_count": 0,
                "gate_pass_count": 0,
                "failures": [{"reason": "forecast_surface_runs_missing"}],
                "rows": [],
            }
        if not _table_exists(conn, "forecast_surface_evaluation_runs"):
            return {
                "ok": False,
                "acceptance_pass": False,
                "primary_reason": "forecast_surface_evaluation_runs_missing",
                "publish_id_prefix": prefix,
                "observed_days": 0,
                "required_days": required_days,
                "min_universe_code_count": required_universe,
                "coverage_pass_count": 0,
                "universe_pass_count": 0,
                "gate_pass_count": 0,
                "failures": [{"reason": "forecast_surface_evaluation_runs_missing"}],
                "rows": [],
            }
        params: list[Any] = []
        prefix_predicate = ""
        if prefix:
            prefix_predicate = "WHERE substr(publish_id, 1, ?) = ?"
            params.extend([len(prefix), prefix])
        rows = conn.execute(
            f"""
            WITH latest_runs AS (
                SELECT
                    publish_id,
                    as_of_date,
                    universe_code_count,
                    expected_row_count,
                    actual_row_count,
                    missing_row_count,
                    coverage_ratio,
                    alerts_json,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY as_of_date
                        ORDER BY created_at DESC, publish_id DESC
                    ) AS run_rank
                FROM forecast_surface_runs
                {prefix_predicate}
            ),
            latest_evaluations AS (
                SELECT
                    publish_id,
                    readiness_pass,
                    primary_gate_reason,
                    gate_failures_json,
                    gate_reason,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY publish_id
                        ORDER BY created_at DESC, run_id DESC
                    ) AS evaluation_rank
                FROM forecast_surface_evaluation_runs
                WHERE scope_type = 'publish'
            )
            SELECT
                CAST(strftime(r.as_of_date, '%Y%m%d') AS INTEGER) AS as_of_date,
                r.publish_id,
                r.universe_code_count,
                r.expected_row_count,
                r.actual_row_count,
                r.missing_row_count,
                r.coverage_ratio,
                r.alerts_json,
                e.readiness_pass,
                e.primary_gate_reason,
                e.gate_failures_json,
                e.gate_reason,
                CAST(r.created_at AS VARCHAR) AS run_created_at,
                CAST(e.created_at AS VARCHAR) AS evaluation_created_at
            FROM latest_runs r
            LEFT JOIN latest_evaluations e
              ON e.publish_id = r.publish_id AND e.evaluation_rank = 1
            WHERE r.run_rank = 1
            ORDER BY r.as_of_date DESC
            LIMIT ?
            """,
            [*params, required_days],
        ).fetchall()
        walk_forward_filter = ""
        walk_forward_params: list[Any] = []
        if prefix:
            walk_forward_filter = " AND strpos(CAST(fold_metrics_json AS VARCHAR), ?) > 0"
            walk_forward_params.append(prefix)
        walk_forward_row = conn.execute(
            f"""
            SELECT
                run_id,
                readiness_pass,
                primary_gate_reason,
                gate_failures_json,
                top_k_uplift,
                top_long_mean_ret20_net,
                top_short_mean_ret20_net,
                calibration_gap_long,
                calibration_gap_short,
                direction_brier_long,
                direction_brier_short,
                fold_count,
                daily_count,
                CAST(created_at AS VARCHAR) AS created_at
            FROM forecast_surface_evaluation_runs
            WHERE scope_type = 'walk_forward'
              {walk_forward_filter}
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """,
            walk_forward_params,
        ).fetchone()
    finally:
        conn.close()

    normalized_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    publish_gate_failures: list[dict[str, Any]] = []
    for row in rows:
        as_of_date = int(row[0])
        publish_id = str(row[1])
        universe_code_count = int(row[2] or 0)
        expected_row_count = int(row[3] or 0)
        actual_row_count = int(row[4] or 0)
        missing_row_count = int(row[5] or 0)
        coverage_ratio = float(row[6] or 0.0)
        gate_failures = _json_load(row[10], [])
        if not isinstance(gate_failures, list):
            gate_failures = []
        gate_reason = str(row[9] or row[11] or "evaluation_missing")
        evaluation_ready = bool(row[8]) if row[8] is not None else False
        coverage_pass = bool(coverage_ratio >= 1.0 and missing_row_count == 0 and actual_row_count >= expected_row_count)
        universe_pass = bool(universe_code_count >= required_universe and expected_row_count >= required_universe * 2)
        gate_pass = bool(evaluation_ready and gate_reason == "gate_passed")
        alerts = _json_load(row[7], [])
        if not isinstance(alerts, list):
            alerts = []
        normalized = {
            "as_of_date": _as_of_date_text(as_of_date),
            "publish_id": publish_id,
            "universe_code_count": universe_code_count,
            "expected_row_count": expected_row_count,
            "actual_row_count": actual_row_count,
            "missing_row_count": missing_row_count,
            "coverage_ratio": coverage_ratio,
            "coverage_pass": coverage_pass,
            "universe_pass": universe_pass,
            "gate_pass": gate_pass,
            "readiness_pass": evaluation_ready,
            "gate_reason": gate_reason,
            "gate_failures": gate_failures,
            "alerts": alerts,
            "run_created_at": None if row[12] is None else str(row[12]),
            "evaluation_created_at": None if row[13] is None else str(row[13]),
        }
        normalized_rows.append(normalized)
        if not coverage_pass:
            failures.append({"as_of_date": normalized["as_of_date"], "publish_id": publish_id, "reason": "coverage_incomplete"})
        if not universe_pass:
            failures.append({"as_of_date": normalized["as_of_date"], "publish_id": publish_id, "reason": "universe_too_small"})
        if not gate_pass:
            publish_gate_failures.append({"as_of_date": normalized["as_of_date"], "publish_id": publish_id, "reason": f"gate_failed:{gate_reason}"})

    observed_days = len(normalized_rows)
    if observed_days < required_days:
        failures.insert(
            0,
            {
                "reason": "insufficient_days",
                "observed_days": observed_days,
                "required_days": required_days,
            },
        )

    coverage_pass_count = sum(1 for row in normalized_rows if bool(row["coverage_pass"]))
    universe_pass_count = sum(1 for row in normalized_rows if bool(row["universe_pass"]))
    gate_pass_count = sum(1 for row in normalized_rows if bool(row["gate_pass"]))
    walk_forward: dict[str, Any] | None = None
    walk_forward_gate_pass = False
    if walk_forward_row is not None:
        walk_forward_failures = _json_load(walk_forward_row[3], [])
        if not isinstance(walk_forward_failures, list):
            walk_forward_failures = []
        walk_forward_gate_pass = bool(walk_forward_row[1]) and str(walk_forward_row[2] or "") == "gate_passed"
        walk_forward = {
            "run_id": str(walk_forward_row[0]),
            "readiness_pass": bool(walk_forward_row[1]),
            "primary_gate_reason": str(walk_forward_row[2] or ""),
            "gate_failures": walk_forward_failures,
            "top_k_uplift": _safe_float(walk_forward_row[4]),
            "top_long_mean_ret20_net": _safe_float(walk_forward_row[5]),
            "top_short_mean_ret20_net": _safe_float(walk_forward_row[6]),
            "calibration_gap_long": _safe_float(walk_forward_row[7]),
            "calibration_gap_short": _safe_float(walk_forward_row[8]),
            "direction_brier_long": _safe_float(walk_forward_row[9]),
            "direction_brier_short": _safe_float(walk_forward_row[10]),
            "fold_count": int(walk_forward_row[11] or 0),
            "daily_count": int(walk_forward_row[12] or 0),
            "created_at": None if walk_forward_row[13] is None else str(walk_forward_row[13]),
        }
    if walk_forward is None:
        failures.append({"reason": "walk_forward_evaluation_missing"})
    elif not walk_forward_gate_pass:
        failures.append({"reason": f"walk_forward_gate_failed:{walk_forward.get('primary_gate_reason') or 'unknown'}"})
    acceptance_pass = bool(
        observed_days >= required_days
        and coverage_pass_count >= required_days
        and universe_pass_count >= required_days
        and walk_forward_gate_pass
        and not failures
    )
    primary_reason = "gate_passed" if acceptance_pass else str((failures or [{"reason": "no_shadow_runs"}])[0]["reason"])
    return {
        "ok": True,
        "acceptance_pass": acceptance_pass,
        "primary_reason": primary_reason,
        "publish_id_prefix": prefix,
        "observed_days": observed_days,
        "required_days": required_days,
        "min_universe_code_count": required_universe,
        "min_expected_row_count": required_universe * 2,
        "coverage_pass_count": coverage_pass_count,
        "universe_pass_count": universe_pass_count,
        "gate_pass_count": gate_pass_count,
        "publish_gate_pass_count": gate_pass_count,
        "publish_gate_failures": publish_gate_failures,
        "walk_forward_gate_pass": walk_forward_gate_pass,
        "walk_forward": walk_forward,
        "failures": failures,
        "rows": normalized_rows,
    }


def evaluate_forecast_surface(
    *,
    result_db_path: str | None = None,
    label_db_path: str | None = None,
    source_db_path: str | None = None,
    publish_id: str | None = None,
    publish_id_prefix: str | None = None,
    top_k: int = DEFAULT_TOP_K,
    min_folds: int = DEFAULT_MIN_FOLDS,
    min_daily_count: int = DEFAULT_MIN_DAILY_COUNT,
    persist: bool = True,
) -> dict[str, Any]:
    result_conn = connect_result_db(result_db_path, read_only=False)
    label_conn = connect_label_db(label_db_path, read_only=True)
    source_conn = connect_source_db(source_db_path) if source_db_path else None
    try:
        ensure_result_schema(result_conn)
        actual_publish_id = str(publish_id or "").strip() or None
        if actual_publish_id is None:
            actual_publish_id = _read_latest_publish_id(result_conn)
        if actual_publish_id is None:
            return {
                "ok": False,
                "readiness_pass": False,
                "reason": "publish_id_not_found",
                "summary": None,
                "folds": [],
            }
        scope_type = "publish"
        if not publish_id:
            scope_type = "walk_forward"
        surface_ids = [actual_publish_id] if publish_id else None
        surface_df = _load_surface_rows(result_conn, publish_ids=surface_ids)
        effective_publish_id_prefix = str(publish_id_prefix or "").strip()
        if not publish_id and effective_publish_id_prefix and not surface_df.empty:
            surface_df = surface_df[
                surface_df["publish_id"].astype(str).str.startswith(effective_publish_id_prefix)
            ]
        if surface_df.empty:
            return {
                "ok": False,
                "readiness_pass": False,
                "reason": "forecast_surface_missing",
                "summary": None,
                "folds": [],
            }
        if publish_id:
            surface_ids = [actual_publish_id]
        else:
            surface_ids = sorted({str(value) for value in surface_df["publish_id"].tolist() if value is not None})
        candidate_df = _load_candidate_rows(result_conn, publish_ids=surface_ids)
        regime_map = _load_regime_rows(result_conn, publish_ids=surface_ids)
        dates = sorted({int(value) for value in surface_df["as_of_date"].tolist() if value is not None})
        label_frames = {horizon: _load_label_rows(label_conn, horizon=horizon, dates=dates) for horizon in SUPPORTED_HORIZONS}
        signal_rows = _load_signal_rows(source_db_path, dates=dates) if source_conn is not None else pd.DataFrame()
        fold_rows: list[dict[str, Any]] = []
        for current_publish_id in surface_ids or []:
            publish_surface = surface_df[surface_df["publish_id"] == current_publish_id]
            publish_candidates = candidate_df[candidate_df["publish_id"] == current_publish_id] if not candidate_df.empty else pd.DataFrame()
            publish_dates = sorted({int(value) for value in publish_surface["as_of_date"].tolist() if value is not None})
            publish_label_frames = {
                horizon: frame[frame["as_of_date"].isin(publish_dates)] if not frame.empty else pd.DataFrame()
                for horizon, frame in label_frames.items()
            }
            publish_signal_rows = signal_rows[signal_rows["as_of_date"].isin(publish_dates)] if not signal_rows.empty else pd.DataFrame()
            fold_rows.extend(
                _evaluate_fold_rows(
                    publish_id=current_publish_id,
                    scope_type=scope_type,
                    surface_df=publish_surface,
                    candidate_df=publish_candidates,
                    label_frames=publish_label_frames,
                    signal_rows=publish_signal_rows,
                    regime_tag=regime_map.get(current_publish_id),
                    top_k=max(1, int(top_k)),
                )
            )
        summary = _aggregate_summary(
            run_id=f"forecast_surface_eval_{_utcnow().strftime('%Y%m%dT%H%M%S%fZ')}",
            scope_type=scope_type,
            publish_id=actual_publish_id if publish_id else None,
            surface_df=surface_df,
            fold_rows=fold_rows,
            top_k=max(1, int(top_k)),
            min_folds=int(min_folds),
            min_daily_count=int(min_daily_count),
        )
        bundle_meta = {}
        if source_db_path and label_db_path:
            try:
                latest_as_of_date = max(int(value) for value in surface_df["as_of_date"].tolist() if value is not None)
                bundle = load_or_train_forecast_surface_bundle(
                    source_db_path=source_db_path,
                    label_db_path=label_db_path,
                    as_of_date=latest_as_of_date,
                )
                if bundle:
                    bundle_meta = dict(bundle.get("meta") or {})
            except Exception:
                bundle_meta = {}
        run_id = str(summary["run_id"])
        for fold_row in fold_rows:
            fold_row["run_id"] = run_id
        summary["readiness_pass"] = bool(summary["readiness_pass"])
        summary["gate_reason"] = str(summary["gate_reason"])
        summary["primary_gate_reason"] = str(summary.get("primary_gate_reason") or summary["gate_reason"])
        summary["gate_failures_json"] = list(summary.get("gate_failures_json") or [])
        summary["min_folds"] = int(min_folds)
        summary["min_daily_count"] = int(min_daily_count)
        summary["calibration_method_long"] = str(
            ((bundle_meta.get("calibration_methods") or {}).get("long")) or "none"
        )
        summary["calibration_method_short"] = str(
            ((bundle_meta.get("calibration_methods") or {}).get("short")) or "none"
        )
        effective_min_folds = int(min_folds) if scope_type == "walk_forward" else 1
        effective_min_daily_count = int(min_daily_count) if scope_type == "walk_forward" else 1
        if int(summary["fold_count"]) < effective_min_folds:
            summary["readiness_pass"] = False
            if "insufficient_fold_count" not in summary["gate_failures_json"]:
                summary["gate_failures_json"] = ["insufficient_fold_count", *list(summary["gate_failures_json"])]
            summary["primary_gate_reason"] = str((summary["gate_failures_json"] or ["insufficient_fold_count"])[0])
            summary["gate_reason"] = summary["primary_gate_reason"]
        if int(summary["daily_count"]) < effective_min_daily_count:
            summary["readiness_pass"] = False
            if "insufficient_daily_count" not in summary["gate_failures_json"]:
                summary["gate_failures_json"] = ["insufficient_daily_count", *list(summary["gate_failures_json"])]
            summary["primary_gate_reason"] = str((summary["gate_failures_json"] or ["insufficient_daily_count"])[0])
            summary["gate_reason"] = summary["primary_gate_reason"]
        if scope_type == "publish" and actual_publish_id:
            ready_streak, recent_ready_count_20 = _materialize_publish_streaks(
                result_conn,
                publish_id=actual_publish_id,
                current_as_of_date=summary.get("as_of_date"),
                current_readiness_pass=bool(summary["readiness_pass"]),
            )
            summary["ready_streak"] = int(ready_streak)
            summary["recent_ready_count_20"] = int(recent_ready_count_20)
        if persist:
            result_conn.execute("BEGIN TRANSACTION")
            try:
                result_conn.execute(
                    "DELETE FROM forecast_surface_evaluation_runs WHERE scope_type = ? AND COALESCE(publish_id, '') = COALESCE(?, '')",
                    [scope_type, actual_publish_id if publish_id else None],
                )
                result_conn.execute(
                    "DELETE FROM forecast_surface_evaluation_folds WHERE scope_type = ? AND COALESCE(publish_id, '') = COALESCE(?, '')",
                    [scope_type, actual_publish_id if publish_id else None],
                )
                summary_columns = list(summary.keys())
                result_conn.execute(
                    f"INSERT INTO forecast_surface_evaluation_runs ({', '.join(summary_columns)}) VALUES ({', '.join(['?'] * len(summary_columns))})",
                    [
                        _json_dump(value) if isinstance(value, (dict, list)) else value
                        for value in (summary[column] for column in summary_columns)
                    ],
                )
                if fold_rows:
                    fold_columns = list(fold_rows[0].keys())
                    result_conn.executemany(
                        f"INSERT INTO forecast_surface_evaluation_folds ({', '.join(fold_columns)}) VALUES ({', '.join(['?'] * len(fold_columns))})",
                        [
                            [
                                _json_dump(row[column]) if isinstance(row[column], (dict, list)) else row[column]
                                for column in fold_columns
                            ]
                            for row in fold_rows
                        ],
                    )
                result_conn.execute("COMMIT")
            except Exception:
                result_conn.execute("ROLLBACK")
                raise
            result_conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "run_id": run_id,
            "publish_id": actual_publish_id if publish_id else None,
            "scope_type": scope_type,
            "summary": summary,
            "folds": fold_rows,
            "readiness_pass": bool(summary["readiness_pass"]),
            "gate_reason": summary["gate_reason"],
        }
    finally:
        result_conn.close()
        label_conn.close()
        if source_conn is not None:
            source_conn.close()
