from __future__ import annotations

import json
import math
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.metrics import balanced_accuracy_score, roc_auc_score

from app.backend.services.ml import rankings_cache
from app.backend.services.tradex_experiment_store import tradex_reports_root
from app.core.config import config as core_config
from app.db.session import get_conn
from external_analysis.event_image_dataset.paths import event_image_dataset_dir
from external_analysis.event_image_dataset.train import run_event_image_dataset_repro, train_event_image_dataset
from external_analysis.image_rerank.artifacts import read_json, verify_roundtrip, write_json


REGIME_GATE_SCHEMA_VERSION = "tradex_event_image_dataset_regime_gate_v1"
REGIME_COMPARE_SCHEMA_VERSION = "tradex_event_image_dataset_regime_compare_v1"
BATCH_SUMMARY_SCHEMA_VERSION = "tradex_event_image_dataset_analysis_batch_v1"
PATTERN_DECOMPOSITION_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_decomposition_v1"
PATTERN_LIBRARY_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_library_v1"
PATTERN_MICRO_FEATURES_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_micro_features_v1"
PATTERN_BOUNDARY_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_boundary_v1"
PATTERN_GATING_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_gating_v1"
PATTERN_SEQUENCE_COMBO_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_sequence_combo_v1"
PATTERN_ADOPTION_POLICY_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_adoption_policy_v1"
PATTERN_ADOPTION_COMPARE_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_adoption_compare_v1"
PATTERN_BREADTH_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_breadth_v1"
PATTERN_PLAYBOOK_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_playbook_v1"
PATTERN_PLAYBOOK_RELAX_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_playbook_relax_compare_v1"
PATTERN_PLAYBOOK_THRESHOLD_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_playbook_threshold_compare_v1"
PATTERN_VETO_COMPARE_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_veto_compare_v1"
PATTERN_VETO_ABLATION_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_veto_ablation_v1"
PATTERN_VETO_THIN_LIQUIDITY_COMPARE_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_veto_thin_liquidity_compare_v1"
PATTERN_SELECTION_CONTRACT_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_selection_contract_v1"
PATTERN_ADOPTION_SCHEMA_VERSION = "tradex_event_image_dataset_pattern_adoption_v1"
RESEARCH_PRIOR_BRIDGE_SCHEMA_VERSION = "tradex_rebound_onset_research_prior_v1"
ROBUSTNESS_COMPARE_SCHEMA_VERSION = "tradex_event_image_dataset_rebound_robustness_v1"
ROBUSTNESS_BATCH_SCHEMA_VERSION = "tradex_event_image_dataset_rebound_robustness_batch_v1"
REBOUND_LIVE_MONITOR_SCHEMA_VERSION = "tradex_rebound_live_monitor_v1"
REGIME_LABELS_JA = {
    "uptrend": "上昇トレンド",
    "sideways_compression": "横ばい・圧縮",
    "down_high_volatility": "下落・高ボラ",
    "rebound_onset": "反発初動",
}
REGIME_TAGS = {
    "uptrend": "上昇トレンド",
    "sideways_compression": "横ばい・圧縮",
    "down_high_volatility": "下落・高ボラ",
    "rebound_onset": "反発初動",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ymd_int_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    text = str(int(value)).zfill(8)
    if len(text) != 8 or not text.isdigit():
        return None
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _trade_date_expr(column: str) -> str:
    return (
        f"CASE "
        f"WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER) "
        f"WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000.0), '%Y%m%d') AS INTEGER) "
        f"WHEN {column} >= 1000000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER) "
        f"ELSE NULL END"
    )


def _write_json_artifact(path: Path, payload: dict[str, Any]) -> Path:
    write_json(path, payload)
    verify_roundtrip(path, payload)
    return path


def _write_markdown_report(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return path


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp_path, path)
    return path


def _safe_mean(values: pd.Series) -> float:
    cleaned = values.astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    return float(cleaned.mean()) if not cleaned.empty else 0.0


def _classify_regime(month_frame: pd.DataFrame) -> dict[str, Any]:
    breadth_score = float((month_frame["dist_ma60"].astype(float) > 0.0).mean())
    momentum_score = _safe_mean(month_frame["return_1m_pre"])
    volatility_score = _safe_mean(month_frame["realized_vol20"])
    compression_score = float(
        _safe_mean(month_frame["dist_ma20"].abs())
        + _safe_mean(month_frame["dist_ma60"].abs())
        + _safe_mean(month_frame["position_from_60d_high"].abs())
    )
    rebound_score = float(
        max(0.0, momentum_score)
        + max(0.0, -_safe_mean(month_frame["position_from_60d_high"]))
        + max(0.0, volatility_score - 0.02)
    )

    if breadth_score >= 0.60 and momentum_score >= 0.02 and volatility_score < 0.040:
        regime_tag = "uptrend"
    elif (
        momentum_score >= 0.01
        and _safe_mean(month_frame["position_from_60d_high"]) <= -0.06
        and 0.35 <= breadth_score <= 0.60
        and volatility_score >= 0.020
    ):
        regime_tag = "rebound_onset"
    elif breadth_score <= 0.40 and volatility_score >= 0.028:
        regime_tag = "down_high_volatility"
    elif compression_score <= 0.18 and abs(momentum_score) <= 0.03 and volatility_score < 0.030:
        regime_tag = "sideways_compression"
    elif breadth_score >= 0.55 and momentum_score >= 0.0:
        regime_tag = "uptrend"
    elif breadth_score <= 0.45:
        regime_tag = "down_high_volatility"
    else:
        regime_tag = "sideways_compression"

    return {
        "regime_tag": regime_tag,
        "regime_label": REGIME_LABELS_JA[regime_tag],
        "breadth_score": breadth_score,
        "momentum_score": momentum_score,
        "volatility_score": volatility_score,
        "compression_score": compression_score,
        "rebound_score": rebound_score,
    }


def _month_top_bottom_metrics(month_frame: pd.DataFrame, *, prob_column: str, pred_column: str) -> dict[str, float]:
    ordered = month_frame.sort_values(prob_column, ascending=False).reset_index(drop=True)
    top_slice = ordered.head(min(10, len(ordered)))
    bottom_slice = ordered.tail(min(10, len(ordered)))
    return {
        "accuracy": float((month_frame["label_id"].astype(int) == month_frame[pred_column].astype(int)).mean()),
        "monthly_top10_precision_up": float(top_slice["label_id"].mean()) if len(top_slice) > 0 else 0.0,
        "monthly_top10_mean_forward_return": float(top_slice["forward_return_1m"].mean()) if len(top_slice) > 0 else 0.0,
        "monthly_bottom10_mean_forward_return": float(bottom_slice["forward_return_1m"].mean()) if len(bottom_slice) > 0 else 0.0,
        "monthly_long_short_spread": (
            float(top_slice["forward_return_1m"].mean()) - float(bottom_slice["forward_return_1m"].mean())
            if len(top_slice) > 0 and len(bottom_slice) > 0
            else 0.0
        ),
    }


def _winner(left: float, right: float) -> str:
    if left > right:
        return "image"
    if left < right:
        return "numeric"
    return "tie"


def _top_candidates(frame: pd.DataFrame, *, prob_column: str, top_k: int = 10) -> pd.DataFrame:
    picked_parts: list[pd.DataFrame] = []
    for _, month_frame in frame.groupby("as_of_date", sort=True):
        picked_parts.append(month_frame.sort_values(prob_column, ascending=False).head(min(top_k, len(month_frame))).copy())
    if not picked_parts:
        return frame.head(0).copy()
    return pd.concat(picked_parts, ignore_index=True)


def _feature_stats(frame: pd.DataFrame) -> dict[str, dict[str, float | None]]:
    payload: dict[str, dict[str, float | None]] = {}
    for column in [str(value) for value in frame.columns]:
        values = frame[column].astype(float).replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=np.float64, copy=False)
        if values.size <= 0:
            payload[column] = {"count": 0, "mean": None, "median": None, "p25": None, "p75": None}
            continue
        payload[column] = {
            "count": int(values.size),
            "mean": float(np.mean(values)),
            "median": float(np.median(values)),
            "p25": float(np.quantile(values, 0.25)),
            "p75": float(np.quantile(values, 0.75)),
        }
    return payload


def _feature_stats_for_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> dict[str, dict[str, float | None]]:
    scoped = frame.loc[:, list(columns)].copy() if not frame.empty else pd.DataFrame(columns=list(columns))
    return _feature_stats(scoped)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0.0):
        return None
    return float(numerator) / float(denominator)


def _safe_pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0.0):
        return None
    return float(current) / float(previous) - 1.0


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(numeric) or np.isinf(numeric):
        return None
    return numeric


def _load_export_history(
    *,
    export_db_path: str,
    codes: list[str],
    max_trade_date: int,
) -> pd.DataFrame:
    normalized_codes = sorted({str(code) for code in codes if str(code).strip()})
    if not normalized_codes:
        raise RuntimeError("export history load requires at least one code")
    code_frame = pd.DataFrame({"code": normalized_codes})
    conn = duckdb.connect(str(export_db_path), read_only=True)
    try:
        conn.register("selected_codes_df", code_frame)
        frame = conn.execute(
            """
            SELECT
                b.code,
                b.trade_date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                i.ma7,
                i.ma20,
                i.ma60,
                i.ma100,
                i.ma200
            FROM bars_daily_export b
            INNER JOIN selected_codes_df s
              ON s.code = b.code
            LEFT JOIN indicator_daily_export i
              ON i.code = b.code AND i.trade_date = b.trade_date
            WHERE b.trade_date <= ?
            ORDER BY b.code, b.trade_date
            """,
            [int(max_trade_date)],
        ).df()
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError("export history query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="raise").astype(int)
    numeric_columns = ("o", "h", "l", "c", "v", "ma7", "ma20", "ma60", "ma100", "ma200")
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["daily_return"] = frame.groupby("code")["c"].pct_change()
    frame["ma120"] = (
        frame.groupby("code")["c"].rolling(window=120, min_periods=120).mean().reset_index(level=0, drop=True)
    )
    frame["rolling_high20"] = (
        frame.groupby("code")["h"].rolling(window=20, min_periods=20).max().reset_index(level=0, drop=True)
    )
    frame["rolling_high60"] = (
        frame.groupby("code")["h"].rolling(window=60, min_periods=60).max().reset_index(level=0, drop=True)
    )
    frame["volume_mean5"] = (
        frame.groupby("code")["v"].rolling(window=5, min_periods=5).mean().reset_index(level=0, drop=True)
    )
    frame["volume_mean20"] = (
        frame.groupby("code")["v"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    frame["turnover20"] = (
        frame.groupby("code")["c"].transform(lambda series: series)
        * frame.groupby("code")["v"].transform(lambda series: series)
    )
    frame["turnover20"] = (
        frame.groupby("code")["turnover20"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    frame["realized_vol20"] = (
        frame.groupby("code")["daily_return"].rolling(window=20, min_periods=20).std().reset_index(level=0, drop=True)
    )
    frame["range_pct"] = (frame["h"] - frame["l"]) / frame["c"].replace(0.0, np.nan)
    frame["range_median20"] = (
        frame.groupby("code")["range_pct"].rolling(window=20, min_periods=20).median().reset_index(level=0, drop=True)
    )
    return frame


def _consecutive_count(flags: list[bool]) -> int:
    count = 0
    for value in reversed(flags):
        if not value:
            break
        count += 1
    return count


def _compute_micro_features(history_frame: pd.DataFrame) -> dict[str, float | int | None]:
    ordered = history_frame.sort_values("trade_date").reset_index(drop=True)
    if ordered.empty:
        raise RuntimeError("micro-feature computation requires non-empty history")
    last = ordered.iloc[-1]
    prev = ordered.iloc[-2] if len(ordered) >= 2 else None
    prev2 = ordered.iloc[-3] if len(ordered) >= 3 else None
    range_last = _safe_float(last["h"] - last["l"])
    body_last = _safe_float(abs(float(last["c"]) - float(last["o"])))
    max_oc = max(float(last["o"]), float(last["c"]))
    min_oc = min(float(last["o"]), float(last["c"]))
    upper_wick = _safe_float(float(last["h"]) - max_oc)
    lower_wick = _safe_float(min_oc - float(last["l"]))
    prev_range = _safe_float(float(prev["h"]) - float(prev["l"])) if prev is not None else None
    prev2_range = _safe_float(float(prev2["h"]) - float(prev2["l"])) if prev2 is not None else None
    prev_upper_wick = (
        None
        if prev is None
        else _safe_float(float(prev["h"]) - max(float(prev["o"]), float(prev["c"])))
    )
    prev_lower_wick = (
        None
        if prev is None
        else _safe_float(min(float(prev["o"]), float(prev["c"])) - float(prev["l"]))
    )
    prev2_upper_wick = (
        None
        if prev2 is None
        else _safe_float(float(prev2["h"]) - max(float(prev2["o"]), float(prev2["c"])))
    )
    prev2_lower_wick = (
        None
        if prev2 is None
        else _safe_float(min(float(prev2["o"]), float(prev2["c"])) - float(prev2["l"]))
    )
    last3 = ordered.tail(3).copy()
    last5 = ordered.tail(5).copy()
    last10 = ordered.tail(10).copy()
    bull_flags = (last3["c"] > last3["o"]).tolist()
    bear_flags = (last3["c"] < last3["o"]).tolist()
    higher_close_count_5 = 0
    lower_close_count_5 = 0
    if len(last5) >= 2:
        close_diff = last5["c"].diff()
        higher_close_count_5 = int((close_diff > 0).sum())
        lower_close_count_5 = int((close_diff < 0).sum())
    narrow_range_count_5 = int(((last5["range_pct"] <= last5["range_median20"]).fillna(False)).sum()) if not last5.empty else 0
    ma20_slope_5 = _safe_pct_change(_safe_float(last.get("ma20")), _safe_float(ordered["ma20"].shift(5).iloc[-1])) if len(ordered) >= 6 else None
    ma60_slope_5 = _safe_pct_change(_safe_float(last.get("ma60")), _safe_float(ordered["ma60"].shift(5).iloc[-1])) if len(ordered) >= 6 else None
    ma120_slope_10 = _safe_pct_change(_safe_float(last.get("ma120")), _safe_float(ordered["ma120"].shift(10).iloc[-1])) if len(ordered) >= 11 else None
    rolling_high20 = _safe_float(last.get("rolling_high20"))
    rolling_high60 = _safe_float(last.get("rolling_high60"))
    ma20 = _safe_float(last.get("ma20"))
    ma60 = _safe_float(last.get("ma60"))
    ma120 = _safe_float(last.get("ma120"))
    close_last = _safe_float(last.get("c"))
    volume_mean5 = _safe_float(last.get("volume_mean5"))
    volume_mean20 = _safe_float(last.get("volume_mean20"))
    prev_close = _safe_float(prev.get("c")) if prev is not None else None
    prev_ma20 = _safe_float(prev.get("ma20")) if prev is not None else None
    prev_ma60 = _safe_float(prev.get("ma60")) if prev is not None else None
    prev_open = _safe_float(prev.get("o")) if prev is not None else None
    prev_high = _safe_float(prev.get("h")) if prev is not None else None
    prev_low = _safe_float(prev.get("l")) if prev is not None else None
    prev2_close = _safe_float(prev2.get("c")) if prev2 is not None else None
    prev2_open = _safe_float(prev2.get("o")) if prev2 is not None else None
    prev2_low = _safe_float(prev2.get("l")) if prev2 is not None else None
    close_above_prev1_body_mid_flag = 0
    engulfed_by_next_bear_flag = 0
    if prev is not None and close_last is not None and prev_open is not None and prev_close is not None:
        prev_body_mid = float(prev_open + prev_close) / 2.0
        close_above_prev1_body_mid_flag = int(close_last >= prev_body_mid)
        current_is_bear = float(last["c"]) < float(last["o"])
        if current_is_bear:
            prev_body_high = max(float(prev_open), float(prev_close))
            prev_body_low = min(float(prev_open), float(prev_close))
            current_body_high = max(float(last["o"]), float(last["c"]))
            current_body_low = min(float(last["o"]), float(last["c"]))
            engulfed_by_next_bear_flag = int(
                current_body_high >= prev_body_high and current_body_low <= prev_body_low
            )
    two_red_then_green_flag = 0
    if prev2 is not None and prev is not None:
        two_red_then_green_flag = int(
            float(prev2["c"]) < float(prev2["o"])
            and float(prev["c"]) < float(prev["o"])
            and float(last["c"]) > float(last["o"])
            and float(last["c"]) >= float(prev["c"])
        )
    narrow_range_then_bull_flag = 0
    if prev is not None:
        prev_range_pct = _safe_float(prev.get("range_pct"))
        prev_range_median20 = _safe_float(prev.get("range_median20"))
        narrow_range_then_bull_flag = int(
            prev_range_pct is not None
            and prev_range_median20 is not None
            and prev_range_pct <= prev_range_median20
            and float(last["c"]) > float(last["o"])
        )
    higher_low_3_flag = 0
    higher_close_3_flag = 0
    if prev2_low is not None and prev_low is not None:
        higher_low_3_flag = int(prev2_low <= prev_low <= float(last["l"]))
    if prev2_close is not None and prev_close is not None and close_last is not None:
        higher_close_3_flag = int(prev2_close <= prev_close <= close_last)
    features: dict[str, float | int | None] = {
        "body_pct_last": None if range_last in (None, 0.0) or body_last is None else float(body_last / range_last),
        "upper_wick_pct_last": None if range_last in (None, 0.0) or upper_wick is None else float(upper_wick / range_last),
        "lower_wick_pct_last": None if range_last in (None, 0.0) or lower_wick is None else float(lower_wick / range_last),
        "is_bull_last": int(float(last["c"]) > float(last["o"])),
        "is_bear_last": int(float(last["c"]) < float(last["o"])),
        "is_bull_prev1": 0 if prev is None else int(float(prev["c"]) > float(prev["o"])),
        "is_bull_prev2": 0 if prev2 is None else int(float(prev2["c"]) > float(prev2["o"])),
        "lower_wick_pct_prev1": None if prev_range in (None, 0.0) or prev_lower_wick is None else float(prev_lower_wick / prev_range),
        "lower_wick_pct_prev2": None if prev2_range in (None, 0.0) or prev2_lower_wick is None else float(prev2_lower_wick / prev2_range),
        "upper_wick_pct_prev1": None if prev_range in (None, 0.0) or prev_upper_wick is None else float(prev_upper_wick / prev_range),
        "upper_wick_pct_prev2": None if prev2_range in (None, 0.0) or prev2_upper_wick is None else float(prev2_upper_wick / prev2_range),
        "gap_from_prev_close_pct": _safe_pct_change(_safe_float(last.get("o")), prev_close),
        "inside_bar_last": 0 if prev is None else int(float(last["h"]) <= float(prev["h"]) and float(last["l"]) >= float(prev["l"])),
        "outside_bar_last": 0 if prev is None else int(float(last["h"]) >= float(prev["h"]) and float(last["l"]) <= float(prev["l"])),
        "close_above_prev1_body_mid_flag": int(close_above_prev1_body_mid_flag),
        "engulfed_by_next_bear_flag": int(engulfed_by_next_bear_flag),
        "two_red_then_green_flag": int(two_red_then_green_flag),
        "narrow_range_then_bull_flag": int(narrow_range_then_bull_flag),
        "higher_low_3_flag": int(higher_low_3_flag),
        "higher_close_3_flag": int(higher_close_3_flag),
        "bull_streak_3": int(_consecutive_count([bool(value) for value in bull_flags])),
        "bear_streak_3": int(_consecutive_count([bool(value) for value in bear_flags])),
        "narrow_range_count_5": int(narrow_range_count_5),
        "higher_close_count_5": int(higher_close_count_5),
        "lower_close_count_5": int(lower_close_count_5),
        "price_vs_ma20": _safe_pct_change(close_last, ma20),
        "price_vs_ma60": _safe_pct_change(close_last, ma60),
        "price_vs_ma120": _safe_pct_change(close_last, ma120),
        "ma20_vs_ma60": _safe_pct_change(ma20, ma60),
        "ma60_vs_ma120": _safe_pct_change(ma60, ma120),
        "ma20_slope_5": ma20_slope_5,
        "ma60_slope_5": ma60_slope_5,
        "ma120_slope_10": ma120_slope_10,
        "ma_squeeze_ratio_20_60": None if close_last in (None, 0.0) or ma20 is None or ma60 is None else float(abs(ma20 - ma60) / close_last),
        "distance_from_20d_high": _safe_pct_change(close_last, rolling_high20),
        "distance_from_60d_high": _safe_pct_change(close_last, rolling_high60),
        "pullback_from_recent_high_pct": None if close_last is None or rolling_high20 in (None, 0.0) else float((rolling_high20 - close_last) / rolling_high20),
        "reclaim_ma20_flag": 0 if prev is None or close_last is None or ma20 is None or prev_close is None or prev_ma20 is None else int(prev_close < prev_ma20 and close_last >= ma20),
        "reclaim_ma60_flag": 0 if prev is None or close_last is None or ma60 is None or prev_close is None or prev_ma60 is None else int(prev_close < prev_ma60 and close_last >= ma60),
        "volume_spike_ratio_5_20": _safe_ratio(volume_mean5, volume_mean20),
        "volume_last_vs_20": _safe_ratio(_safe_float(last.get("v")), volume_mean20),
        "turnover20": _safe_float(last.get("turnover20")),
        "realized_vol20": _safe_float(last.get("realized_vol20")),
    }
    return features


def _micro_feature_columns() -> tuple[str, ...]:
    return (
        "body_pct_last",
        "upper_wick_pct_last",
        "lower_wick_pct_last",
        "is_bull_last",
        "is_bear_last",
        "is_bull_prev1",
        "is_bull_prev2",
        "lower_wick_pct_prev1",
        "lower_wick_pct_prev2",
        "upper_wick_pct_prev1",
        "upper_wick_pct_prev2",
        "gap_from_prev_close_pct",
        "inside_bar_last",
        "outside_bar_last",
        "close_above_prev1_body_mid_flag",
        "engulfed_by_next_bear_flag",
        "two_red_then_green_flag",
        "narrow_range_then_bull_flag",
        "higher_low_3_flag",
        "higher_close_3_flag",
        "bull_streak_3",
        "bear_streak_3",
        "narrow_range_count_5",
        "higher_close_count_5",
        "lower_close_count_5",
        "price_vs_ma20",
        "price_vs_ma60",
        "price_vs_ma120",
        "ma20_vs_ma60",
        "ma60_vs_ma120",
        "ma20_slope_5",
        "ma60_slope_5",
        "ma120_slope_10",
        "ma_squeeze_ratio_20_60",
        "distance_from_20d_high",
        "distance_from_60d_high",
        "pullback_from_recent_high_pct",
        "reclaim_ma20_flag",
        "reclaim_ma60_flag",
        "volume_spike_ratio_5_20",
        "volume_last_vs_20",
        "turnover20",
        "realized_vol20",
    )


def _normalize_code_key(value: Any) -> str:
    return str(value or "").strip()


def _bool_passes(row: pd.Series, condition: bool) -> bool:
    return bool(condition)


def _finite_float_or_none(value: Any) -> float | None:
    return _safe_float(value)


def _score_component(condition: bool, weight: float) -> float:
    return float(weight) if condition else 0.0


def _suggest_pattern_rule(true_positive_frame: pd.DataFrame) -> dict[str, Any]:
    if true_positive_frame.empty:
        return {
            "trend_strength": {},
            "setup_quality": {},
            "tradability_risk": {},
        }
    stats = _feature_stats_for_columns(
        true_positive_frame,
        (
            "return_1m_pre",
            "dist_ma20",
            "dist_ma60",
            "dist_ma120",
            "volume_change20",
            "position_from_60d_high",
            "realized_vol20",
            "adv_proxy",
        ),
    )
    return {
        "trend_strength": {
            "return_1m_pre_min": stats["return_1m_pre"]["p25"],
            "dist_ma20_min": stats["dist_ma20"]["p25"],
            "dist_ma60_min": stats["dist_ma60"]["p25"],
            "dist_ma120_min": stats["dist_ma120"]["p25"],
        },
        "setup_quality": {
            "volume_change20_min": stats["volume_change20"]["p25"],
            "position_from_60d_high_range": [
                stats["position_from_60d_high"]["p25"],
                stats["position_from_60d_high"]["p75"],
            ],
        },
        "tradability_risk": {
            "realized_vol20_max": stats["realized_vol20"]["p75"],
            "adv_proxy_min": stats["adv_proxy"]["p25"],
        },
    }


def _winner_months_for_regime(regime_compare: dict[str, Any], *, regime_tag: str) -> list[int]:
    winner_months: list[int] = []
    for row in regime_compare["month_level_results"]:
        wins = sum(
            1
            for key in ("winner_accuracy", "winner_top10_precision", "winner_long_short_spread")
            if row.get(key) == "image"
        )
        if str(row.get("regime_tag")) == str(regime_tag) and wins >= 2:
            winner_months.append(int(row["as_of_date"]))
    return winner_months


def _attach_micro_feature_rows(candidate_frame: pd.DataFrame, history_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in candidate_frame.to_dict(orient="records"):
        code = str(row["code"])
        as_of_date = int(row["as_of_date"])
        code_history = history_frame.loc[
            (history_frame["code"] == code) & (history_frame["trade_date"] <= as_of_date)
        ].copy()
        if code_history.empty:
            continue
        rows.append({**row, **_compute_micro_features(code_history)})
    return pd.DataFrame(rows)


def build_event_image_pattern_micro_features(
    *,
    dataset_id: str,
    regime_tag: str,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    train_eval_manifest = read_json(dataset_dir / "train_eval_manifest.json")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    regime_compare = read_json(dataset_dir / "regime_compare.json")
    regime_index = pd.read_parquet(dataset_dir / "regime_month_index.parquet")
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    restricted_universe_manifest_path = dataset_dir / "restricted_universe_manifest.json"
    restricted_universe_manifest = read_json(restricted_universe_manifest_path) if restricted_universe_manifest_path.exists() else None

    winner_months = _winner_months_for_regime(regime_compare, regime_tag=regime_tag)
    if not winner_months:
        raise RuntimeError(f"no image-winning months found for regime_tag={regime_tag}")
    test_frame = predictions.loc[predictions["split"] == "test"].copy()
    winner_frame = test_frame.merge(regime_index[["as_of_date", "regime_tag", "regime_label"]], on="as_of_date", how="left")
    winner_frame = winner_frame.loc[winner_frame["as_of_date"].isin(winner_months)].copy()
    image_top = _top_candidates(winner_frame, prob_column="image_pred_prob_up", top_k=10)
    numeric_top = _top_candidates(winner_frame, prob_column="numeric_pred_prob_up", top_k=10)
    true_positive = image_top.loc[image_top["label_id"] == 1].copy()
    false_positive = image_top.loc[image_top["label_id"] == 0].copy()
    sample_codes = sorted({str(code) for code in pd.concat([image_top["code"], numeric_top["code"]], axis=0).tolist()})
    export_db_path = str(dataset_manifest["source_export_db_path"])
    history_frame = _load_export_history(export_db_path=export_db_path, codes=sample_codes, max_trade_date=max(winner_months))
    image_top_features = _attach_micro_feature_rows(image_top, history_frame)
    numeric_top_features = _attach_micro_feature_rows(numeric_top, history_frame)
    true_positive_features = image_top_features.loc[image_top_features["label_id"] == 1].copy()
    false_positive_features = image_top_features.loc[image_top_features["label_id"] == 0].copy()

    parquet_path = target_dir / f"pattern_micro_features_{regime_tag}.parquet"
    image_top_features.to_parquet(parquet_path, index=False)
    json_path = target_dir / f"pattern_micro_features_{regime_tag}.json"
    payload = {
        "schema_version": PATTERN_MICRO_FEATURES_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "regime_tag": str(regime_tag),
        "regime_label": str(REGIME_LABELS_JA.get(regime_tag, regime_tag)),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
            "train_eval_manifest": str(dataset_dir / "train_eval_manifest.json"),
            "fidelity_compare": str(dataset_dir / "fidelity_compare.json"),
            "regime_compare": str(dataset_dir / "regime_compare.json"),
            "regime_month_index": str(dataset_dir / "regime_month_index.parquet"),
            "predictions": str(dataset_dir / "predictions.parquet"),
            "restricted_universe_manifest": str(restricted_universe_manifest_path) if restricted_universe_manifest is not None else None,
            "export_db_path": export_db_path,
        },
        "compare_contract": {
            "same_dataset": True,
            "same_split": True,
            "same_labels": True,
            "same_horizon": True,
            "same_universe": True,
            "same_sample_keys": bool(fidelity_compare.get("same_sample_keys", False)),
            "sample_key_definition": str(fidelity_compare.get("sample_key_definition", "as_of_date + code + label")),
            "common_eligible_sample_count": int(fidelity_compare["common_eligible_sample_count"]),
            "dropped_by_warmup_v1_2_count": int(fidelity_compare["dropped_by_warmup_v1_2_count"]),
            "split_policy": str(train_eval_manifest["split_policy"]),
        },
        "sample_key_definition": str(fidelity_compare.get("sample_key_definition", "as_of_date + code + label")),
        "winner_months": winner_months,
        "candidate_counts": {
            "winner_sample_count": int(len(winner_frame)),
            "image_top_candidate_count": int(len(image_top_features)),
            "image_true_positive_count": int(len(true_positive_features)),
            "image_false_positive_count": int(len(false_positive_features)),
            "numeric_top_candidate_count": int(len(numeric_top_features)),
        },
        "feature_columns": list(_micro_feature_columns()),
        "true_positive_stats": _feature_stats_for_columns(true_positive_features, _micro_feature_columns()),
        "false_positive_stats": _feature_stats_for_columns(false_positive_features, _micro_feature_columns()),
        "numeric_top_stats": _feature_stats_for_columns(numeric_top_features, _micro_feature_columns()),
        "generated_at": _utc_now_iso(),
        "micro_feature_path": str(parquet_path),
    }
    _write_json_artifact(json_path, payload)
    return {
        "dataset_id": str(dataset_id),
        "regime_tag": str(regime_tag),
        "pattern_micro_features_path": str(parquet_path),
        "pattern_micro_features_summary_path": str(json_path),
    }


def decompose_event_image_pattern(
    *,
    dataset_id: str,
    regime_tag: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    micro_feature_result = build_event_image_pattern_micro_features(
        dataset_id=dataset_id,
        regime_tag=regime_tag,
        output_root=target_dir,
    )

    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    regime_compare = read_json(dataset_dir / "regime_compare.json")
    regime_index = pd.read_parquet(dataset_dir / "regime_month_index.parquet")
    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    train_eval_manifest = read_json(dataset_dir / "train_eval_manifest.json")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    restricted_universe_manifest_path = dataset_dir / "restricted_universe_manifest.json"
    restricted_universe_manifest = read_json(restricted_universe_manifest_path) if restricted_universe_manifest_path.exists() else None
    test_frame = predictions.loc[predictions["split"] == "test"].copy()
    merged = test_frame.merge(regime_index[["as_of_date", "regime_tag", "regime_label"]], on="as_of_date", how="left")

    winner_months: list[int] = []
    for row in regime_compare["month_level_results"]:
        wins = sum(
            1
            for key in ("winner_accuracy", "winner_top10_precision", "winner_long_short_spread")
            if row.get(key) == "image"
        )
        if str(row.get("regime_tag")) == str(regime_tag) and wins >= 2:
            winner_months.append(int(row["as_of_date"]))
    winner_frame = merged.loc[merged["as_of_date"].isin(winner_months)].copy()
    if winner_frame.empty:
        raise RuntimeError(f"no image-winning months found for regime_tag={regime_tag}")

    top_candidates = _top_candidates(winner_frame, prob_column="image_pred_prob_up", top_k=10)
    true_positive = top_candidates.loc[top_candidates["label_id"] == 1].copy()
    false_positive = top_candidates.loc[top_candidates["label_id"] == 0].copy()
    numeric_top = _top_candidates(winner_frame, prob_column="numeric_pred_prob_up", top_k=10)
    trend_columns = ("return_1m_pre", "dist_ma60", "dist_ma120")
    setup_columns = ("dist_ma20", "volume_change20", "position_from_60d_high")
    risk_columns = ("realized_vol20", "adv_proxy")

    decomposition = {
        "schema_version": PATTERN_DECOMPOSITION_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "regime_tag": str(regime_tag),
        "regime_label": str(REGIME_LABELS_JA.get(regime_tag, regime_tag)),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
            "train_eval_manifest": str(dataset_dir / "train_eval_manifest.json"),
            "fidelity_compare": str(dataset_dir / "fidelity_compare.json"),
            "regime_compare": str(dataset_dir / "regime_compare.json"),
            "regime_month_index": str(dataset_dir / "regime_month_index.parquet"),
            "predictions": str(dataset_dir / "predictions.parquet"),
            "restricted_universe_manifest": str(restricted_universe_manifest_path) if restricted_universe_manifest is not None else None,
        },
        "compare_contract": {
            "same_dataset": True,
            "same_split": True,
            "same_labels": True,
            "same_horizon": True,
            "same_universe": True,
            "same_sample_keys": bool(fidelity_compare.get("same_sample_keys", False)),
            "sample_key_definition": str(fidelity_compare.get("sample_key_definition", "as_of_date + code + label")),
            "common_eligible_sample_count": int(fidelity_compare["common_eligible_sample_count"]),
            "dropped_by_warmup_v1_2_count": int(fidelity_compare["dropped_by_warmup_v1_2_count"]),
            "split_policy": str(train_eval_manifest["split_policy"]),
        },
        "dataset_contract": {
            "evaluation_bundle_id": str(train_eval_manifest["evaluation_bundle_id"]),
            "renderer_spec_id": str(train_eval_manifest["renderer_spec_id"]),
            "featureizer_spec_id": str(train_eval_manifest["featureizer_spec_id"]),
            "numeric_feature_spec_id": str(train_eval_manifest["numeric_feature_spec_id"]),
            "image_feature_size": int(train_eval_manifest["image_feature_size"]),
            "control_image_feature_size": int(train_eval_manifest["control_image_feature_size"]),
            "restricted_universe_name": None if restricted_universe_manifest is None else str(restricted_universe_manifest.get("universe_name")),
        },
        "winner_months": winner_months,
        "winner_month_count": int(len(winner_months)),
        "winner_sample_count": int(len(winner_frame)),
        "image_top_candidate_count": int(len(top_candidates)),
        "image_true_positive_count": int(len(true_positive)),
        "image_false_positive_count": int(len(false_positive)),
        "numeric_top_candidate_count": int(len(numeric_top)),
        "trend_strength": {
            "feature_columns": list(trend_columns),
            "true_positive_stats": _feature_stats_for_columns(true_positive, trend_columns),
            "false_positive_stats": _feature_stats_for_columns(false_positive, trend_columns),
        },
        "setup_quality": {
            "feature_columns": list(setup_columns),
            "true_positive_stats": _feature_stats_for_columns(true_positive, setup_columns),
            "false_positive_stats": _feature_stats_for_columns(false_positive, setup_columns),
        },
        "tradability_risk": {
            "feature_columns": list(risk_columns),
            "true_positive_stats": _feature_stats_for_columns(true_positive, risk_columns),
            "false_positive_stats": _feature_stats_for_columns(false_positive, risk_columns),
        },
        "candidate_rule": _suggest_pattern_rule(true_positive),
        "exemplars": {
            "true_positive_images": true_positive[["sample_id", "as_of_date", "code", "fidelity_image_path", "forward_return_1m", "image_pred_prob_up"]]
            .sort_values("image_pred_prob_up", ascending=False)
            .head(8)
            .to_dict(orient="records"),
            "false_positive_images": false_positive[["sample_id", "as_of_date", "code", "fidelity_image_path", "forward_return_1m", "image_pred_prob_up"]]
            .sort_values("image_pred_prob_up", ascending=False)
            .head(8)
            .to_dict(orient="records"),
        },
        "generated_at": _utc_now_iso(),
        "micro_feature_artifact_path": str(micro_feature_result["pattern_micro_features_summary_path"]),
    }

    json_path = target_dir / f"pattern_decomposition_{regime_tag}.json"
    md_path = target_dir / f"pattern_decomposition_{regime_tag}.md"
    _write_json_artifact(json_path, decomposition)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Pattern Decomposition",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- regime: `{regime_tag}`",
            f"- winner_months: `{winner_months}`",
            "",
            "## Current State",
            f"- winner_sample_count: `{len(winner_frame)}`",
            f"- image_true_positive_count: `{len(true_positive)}`",
            f"- image_false_positive_count: `{len(false_positive)}`",
            "",
            "## What Changed",
            "- decomposed image-winning regime months into trend_strength / setup_quality / tradability_risk summaries",
            "",
            "## Evidence",
            f"- candidate_rule: `{decomposition['candidate_rule']}`",
            f"- top true-positive exemplars: `{decomposition['exemplars']['true_positive_images'][:3]}`",
            f"- top false-positive exemplars: `{decomposition['exemplars']['false_positive_images'][:3]}`",
            "",
            "## Decision",
            "- keep: rebound_onset pattern shows enough separation to preserve as a library candidate",
            "",
            "## Remaining Risks",
            "- current decomposition uses summary features, not explicit candlestick micro-features yet",
            "- winner month count may still be small",
            "",
            "## Next Single Step",
            "- build the first pattern library artifact for rebound_onset and compare it against uptrend under the same restricted-universe contract",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "regime_tag": str(regime_tag),
        "pattern_decomposition_path": str(json_path),
        "pattern_report_path": str(md_path),
        "pattern_micro_features_path": str(micro_feature_result["pattern_micro_features_path"]),
        "pattern_micro_features_summary_path": str(micro_feature_result["pattern_micro_features_summary_path"]),
    }


def _mean_stat(stats_payload: dict[str, Any], feature_name: str) -> float | None:
    feature = stats_payload.get(feature_name)
    if not isinstance(feature, dict):
        return None
    value = feature.get("mean")
    return None if value is None else float(value)


def _median_stat(stats_payload: dict[str, Any], feature_name: str) -> float | None:
    feature = stats_payload.get(feature_name)
    if not isinstance(feature, dict):
        return None
    value = feature.get("median")
    return None if value is None else float(value)


def _formal_compare_test_frame(predictions: pd.DataFrame, *, expected_count: int | None = None) -> pd.DataFrame:
    frame = predictions.loc[predictions["split"] == "test"].copy()
    if "fidelity_available" in frame.columns:
        frame = frame.loc[frame["fidelity_available"].fillna(False)].copy()
    if "control_available" in frame.columns:
        frame = frame.loc[frame["control_available"].fillna(False)].copy()
    for column in ("image_pred_prob_up", "numeric_pred_prob_up", "image_pred_label", "numeric_pred_label", "label_id"):
        frame = frame.loc[frame[column].notna()].copy()
    if expected_count is not None and len(frame) != int(expected_count):
        raise RuntimeError(
            f"formal compare contract mismatch: expected test common subset count={expected_count}, actual={len(frame)}"
        )
    return frame.reset_index(drop=True)


def _subset_model_metrics(frame: pd.DataFrame, *, prob_column: str, pred_column: str) -> dict[str, float | None]:
    if frame.empty:
        return {
            "balanced_accuracy": None,
            "roc_auc": None,
            "monthly_top10_precision_up": None,
            "monthly_long_short_spread": None,
        }
    label = frame["label_id"].astype(int)
    pred = frame[pred_column].astype(int)
    prob = frame[prob_column].astype(float)
    roc_auc = None
    if label.nunique(dropna=True) >= 2:
        roc_auc = float(roc_auc_score(label, prob))
    month_metrics = [_month_top_bottom_metrics(month_frame, prob_column=prob_column, pred_column=pred_column) for _, month_frame in frame.groupby("as_of_date", sort=True)]
    return {
        "balanced_accuracy": float(balanced_accuracy_score(label, pred)),
        "roc_auc": roc_auc,
        "monthly_top10_precision_up": float(np.mean([row["monthly_top10_precision_up"] for row in month_metrics])) if month_metrics else None,
        "monthly_long_short_spread": float(np.mean([row["monthly_long_short_spread"] for row in month_metrics])) if month_metrics else None,
    }


def _apply_gate_mask(
    frame: pd.DataFrame,
    *,
    price_vs_ma120_min: float | None,
    distance_from_60d_high_range: tuple[float, float] | None = None,
    realized_vol20_min: float | None = None,
    realized_vol20_max: float | None = None,
    volume_change20_min: float | None = None,
    volume_change20_max: float | None = None,
    price_vs_ma60_min: float | None = None,
) -> pd.Series:
    mask = pd.Series(True, index=frame.index, dtype=bool)
    if price_vs_ma120_min is not None:
        mask &= frame["dist_ma120"].astype(float) >= float(price_vs_ma120_min)
    if distance_from_60d_high_range is not None:
        lower, upper = distance_from_60d_high_range
        mask &= frame["position_from_60d_high"].astype(float).between(float(lower), float(upper), inclusive="both")
    if realized_vol20_min is not None:
        mask &= frame["realized_vol20"].astype(float) >= float(realized_vol20_min)
    if realized_vol20_max is not None:
        mask &= frame["realized_vol20"].astype(float) <= float(realized_vol20_max)
    if volume_change20_min is not None:
        mask &= frame["volume_change20"].astype(float) >= float(volume_change20_min)
    if volume_change20_max is not None:
        mask &= frame["volume_change20"].astype(float) <= float(volume_change20_max)
    if price_vs_ma60_min is not None:
        mask &= frame["dist_ma60"].astype(float) >= float(price_vs_ma60_min)
    return mask


def _metric_delta(left_metrics: dict[str, float | None], right_metrics: dict[str, float | None]) -> dict[str, float | None]:
    payload: dict[str, float | None] = {}
    for left_key, right_key, output_key in (
        ("balanced_accuracy", "balanced_accuracy", "balanced_accuracy_delta"),
        ("roc_auc", "roc_auc", "roc_auc_delta"),
        ("monthly_top10_precision_up", "monthly_top10_precision_up", "monthly_top10_precision_up_delta"),
        ("monthly_long_short_spread", "monthly_long_short_spread", "monthly_long_short_spread_delta"),
    ):
        left_value = left_metrics.get(left_key)
        right_value = right_metrics.get(right_key)
        payload[output_key] = None if left_value is None or right_value is None else float(left_value - right_value)
    return payload


def _apply_combo_rule(frame: pd.DataFrame, *, conditions: list[dict[str, Any]]) -> pd.Series:
    mask = pd.Series(True, index=frame.index, dtype=bool)
    for condition in conditions:
        if not bool(condition.get("enabled", True)):
            continue
        column = str(condition["column"])
        operator = str(condition["operator"])
        value = condition.get("value")
        series = frame[column]
        if operator == "eq":
            mask &= series.astype(float) == float(value)
        elif operator == "gte":
            mask &= series.astype(float) >= float(value)
        elif operator == "lte":
            mask &= series.astype(float) <= float(value)
        elif operator == "gt":
            mask &= series.astype(float) > float(value)
        elif operator == "between":
            lower, upper = value
            mask &= series.astype(float).between(float(lower), float(upper), inclusive="both")
        else:
            raise RuntimeError(f"unsupported combo operator: {operator}")
    return mask


def _score_combo_result(result: dict[str, Any]) -> tuple[float, float, float]:
    metrics = result["metric_deltas"]
    long_short = metrics.get("monthly_long_short_spread_delta_vs_numeric")
    balanced = metrics.get("balanced_accuracy_delta_vs_numeric")
    false_positive_hit_rate = result.get("image_false_positive_hit_rate")
    return (
        float(long_short) if long_short is not None else float("-inf"),
        float(balanced) if balanced is not None else float("-inf"),
        -float(false_positive_hit_rate) if false_positive_hit_rate is not None else 0.0,
    )


def _rebound_variant_name_to_threshold(variant_name: str) -> float:
    thresholds = {
        "v1_0_strict_bonus": 0.60,
        "v1_1_live_bonus": 0.50,
        "v1_2_soft_bonus": 0.45,
    }
    threshold = thresholds.get(str(variant_name))
    if threshold is None:
        raise RuntimeError(f"unsupported rebound adoption variant: {variant_name}")
    return float(threshold)


def _make_rebound_adoption_source_artifacts(
    *,
    dataset_dir: Path,
    micro_feature_summary_path: Path,
    export_db_path: str,
) -> dict[str, str]:
    return {
        "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
        "predictions": str(dataset_dir / "predictions.parquet"),
        "pattern_gating": str(dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json"),
        "pattern_combo": str(dataset_dir / "pattern_combo_rules_rebound_onset_vs_uptrend.json"),
        "pattern_library": str(dataset_dir / "pattern_library_candidates.json"),
        "micro_feature_summary": str(micro_feature_summary_path),
        "source_export_db_path": str(export_db_path),
    }


def _load_rebound_adoption_context(
    *,
    dataset_id: str,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    gating_payload = read_json(dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json")
    combo_payload = read_json(dataset_dir / "pattern_combo_rules_rebound_onset_vs_uptrend.json")
    library_payload = read_json(dataset_dir / "pattern_library_candidates.json")

    candidate_row = next(
        (row for row in library_payload["pattern_candidates"] if str(row.get("regime_tag")) == "rebound_onset"),
        None,
    )
    if candidate_row is None:
        raise RuntimeError("pattern library missing rebound_onset candidate")
    micro_feature_summary_path = Path(str(candidate_row.get("micro_feature_artifact_path") or ""))
    if not micro_feature_summary_path.exists():
        raise RuntimeError(f"missing rebound micro feature summary: {micro_feature_summary_path}")
    micro_feature_summary = read_json(micro_feature_summary_path)

    test_common_frame = _formal_compare_test_frame(
        predictions,
        expected_count=int(fidelity_compare["common_eligible_sample_count"]),
    )
    if test_common_frame.empty:
        raise RuntimeError("rebound adoption context requires non-empty test common subset")
    latest_asof = int(test_common_frame["as_of_date"].max())
    latest_frame = test_common_frame.loc[test_common_frame["as_of_date"] == latest_asof].copy().reset_index(drop=True)
    if latest_frame.empty:
        raise RuntimeError("rebound adoption context requires latest as_of candidates")

    export_db_path = str(dataset_manifest["source_export_db_path"])
    latest_codes = sorted({_normalize_code_key(code) for code in latest_frame["code"].tolist() if _normalize_code_key(code)})
    history_frame = _load_export_history(export_db_path=export_db_path, codes=latest_codes, max_trade_date=latest_asof)
    feature_frame = _attach_micro_feature_rows(latest_frame, history_frame)
    if feature_frame.empty:
        raise RuntimeError("rebound adoption context produced no feature rows")

    gate_rule = gating_payload["candidate_gate_rule"]["rebound_onset"]
    tp_stats = micro_feature_summary["true_positive_stats"]

    return {
        "dataset_dir": dataset_dir,
        "target_dir": target_dir,
        "dataset_manifest": dataset_manifest,
        "predictions": predictions,
        "fidelity_compare": fidelity_compare,
        "gating_payload": gating_payload,
        "combo_payload": combo_payload,
        "library_payload": library_payload,
        "candidate_row": candidate_row,
        "micro_feature_summary_path": micro_feature_summary_path,
        "micro_feature_summary": micro_feature_summary,
        "test_common_frame": test_common_frame,
        "latest_asof": latest_asof,
        "latest_frame": latest_frame,
        "feature_frame": feature_frame,
        "source_artifacts": _make_rebound_adoption_source_artifacts(
            dataset_dir=dataset_dir,
            micro_feature_summary_path=micro_feature_summary_path,
            export_db_path=export_db_path,
        ),
        "gate_rule": {
            "price_vs_ma120_min": float(gate_rule["price_vs_ma120_min"]),
            "distance_from_60d_high_range": [
                float(gate_rule["distance_from_60d_high_range"][0]),
                float(gate_rule["distance_from_60d_high_range"][1]),
            ],
            "realized_vol20_min": float(gate_rule["realized_vol20_min"]),
        },
        "tp_stats": tp_stats,
        "lower_wick_median": _median_stat(tp_stats, "lower_wick_pct_last"),
        "bull_streak_median": _median_stat(tp_stats, "bull_streak_3"),
        "narrow_range_median": _median_stat(tp_stats, "narrow_range_count_5"),
    }


def _load_rebound_latest_feature_frame(
    *,
    dataset_id: str,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    test_common_frame = _formal_compare_test_frame(predictions)
    if test_common_frame.empty:
        raise RuntimeError("latest feature frame requires non-empty test common subset")
    latest_asof = int(test_common_frame["as_of_date"].max())
    latest_frame = test_common_frame.loc[test_common_frame["as_of_date"] == latest_asof].copy().reset_index(drop=True)
    if latest_frame.empty:
        raise RuntimeError("latest feature frame requires latest as_of candidates")
    export_db_path = str(dataset_manifest["source_export_db_path"])
    latest_codes = sorted({_normalize_code_key(code) for code in latest_frame["code"].tolist() if _normalize_code_key(code)})
    history_frame = _load_export_history(export_db_path=export_db_path, codes=latest_codes, max_trade_date=latest_asof)
    feature_frame = _attach_micro_feature_rows(latest_frame, history_frame)
    if feature_frame.empty:
        raise RuntimeError("latest feature frame produced no feature rows")
    return {
        "dataset_dir": dataset_dir,
        "dataset_manifest": dataset_manifest,
        "predictions": predictions,
        "fidelity_compare": fidelity_compare,
        "test_common_frame": test_common_frame,
        "latest_asof": latest_asof,
        "latest_frame": latest_frame,
        "feature_frame": feature_frame,
    }


def _build_rebound_core_gate_candidate_frame(
    *,
    feature_frame: pd.DataFrame,
    gate_rule: dict[str, Any],
) -> pd.DataFrame:
    core_gate_mask = (
        feature_frame["price_vs_ma120"].astype(float) >= float(gate_rule["price_vs_ma120_min"])
    ) & (
        feature_frame["distance_from_60d_high"].astype(float).between(
            float(gate_rule["distance_from_60d_high_range"][0]),
            float(gate_rule["distance_from_60d_high_range"][1]),
            inclusive="both",
        )
    ) & (
        feature_frame["realized_vol20"].astype(float) >= float(gate_rule["realized_vol20_min"])
    )
    return feature_frame.loc[core_gate_mask].copy().reset_index(drop=True)


def _score_rebound_candidate_frame(
    *,
    candidate_frame: pd.DataFrame,
    lower_wick_median: float | None,
    bull_streak_median: float | None,
    narrow_range_median: float | None,
    threshold_for_bonus: float,
) -> pd.DataFrame:
    if candidate_frame.empty:
        return candidate_frame.copy()

    fit_scores: list[float] = []
    adoption_reasons: list[list[str]] = []
    bonus_eligible: list[bool] = []
    for _, row in candidate_frame.iterrows():
        fit_score = 0.0
        fit_score += _score_component(int(row.get("is_bull_last") or 0) == 1, 0.20)
        lower_wick = _finite_float_or_none(row.get("lower_wick_pct_last"))
        fit_score += _score_component(lower_wick_median is not None and lower_wick is not None and lower_wick >= lower_wick_median, 0.20)
        bull_streak = _finite_float_or_none(row.get("bull_streak_3"))
        fit_score += _score_component(
            bull_streak_median is not None and bull_streak is not None and bull_streak >= max(1.0, math.floor(bull_streak_median)),
            0.15,
        )
        narrow_range = _finite_float_or_none(row.get("narrow_range_count_5"))
        fit_score += _score_component(
            narrow_range_median is not None and narrow_range is not None and narrow_range >= math.floor(narrow_range_median),
            0.15,
        )
        fit_score += _score_component((_finite_float_or_none(row.get("ma20_slope_5")) or 0.0) > 0.0, 0.15)
        fit_score += _score_component((_finite_float_or_none(row.get("ma60_slope_5")) or 0.0) > 0.0, 0.15)
        fit_score = float(max(0.0, min(1.0, fit_score)))
        fit_scores.append(fit_score)
        bonus_eligible.append(fit_score >= float(threshold_for_bonus))
        adoption_reasons.append(
            _build_rebound_adoption_reasons(
                row=row,
                lower_wick_median=lower_wick_median,
                bull_streak_median=bull_streak_median,
                narrow_range_median=narrow_range_median,
            )
        )

    scored = candidate_frame.copy()
    scored["fit_score"] = fit_scores
    scored["bonus_eligible"] = bonus_eligible
    scored["pattern_tag"] = "rebound_onset"
    scored["adoption_reasons"] = adoption_reasons
    sort_prob_column = "image_pred_prob_up"
    if sort_prob_column not in scored.columns:
        sort_prob_column = "__sort_prob_fallback"
        scored[sort_prob_column] = scored["fit_score"].astype(float)
    scored = scored.sort_values(
        by=["fit_score", sort_prob_column, "code"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    if "__sort_prob_fallback" in scored.columns:
        scored = scored.drop(columns=["__sort_prob_fallback"])
    scored["rank"] = np.arange(1, len(scored) + 1, dtype=np.int64)
    return scored


def _build_rebound_bridge_snapshot(
    *,
    candidate_frame: pd.DataFrame,
    dataset_id: str,
    source_artifacts: dict[str, str],
    asof_iso: str,
    source_disposition: str,
    run_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": RESEARCH_PRIOR_BRIDGE_SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "strategy_id": "tradex_rebound_onset_aux_v1",
        "source_dataset_id": str(dataset_id),
        "source_artifacts": source_artifacts,
        "up": {
            "asof": asof_iso,
            "codes": candidate_frame["code"].astype(str).tolist(),
            "rank_map": {str(row["code"]): int(row["rank"]) for row in candidate_frame.to_dict(orient="records")},
            "fit_score_map": {
                str(row["code"]): float(row["fit_score"])
                for row in candidate_frame.to_dict(orient="records")
                if bool(row["bonus_eligible"])
            },
            "pattern_tag_map": {str(row["code"]): "rebound_onset" for row in candidate_frame.to_dict(orient="records")},
            "adoption_reason_map": {str(row["code"]): list(row["adoption_reasons"]) for row in candidate_frame.to_dict(orient="records")},
            "bonus_cap": 0.03,
            "source_pattern": "rebound_onset",
            "source_disposition": str(source_disposition),
        },
        "down": {
            "asof": asof_iso,
            "codes": [],
            "rank_map": {},
        },
    }


def _make_ranking_preview_items(frame: pd.DataFrame) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        image_prob = float(row.get("image_pred_prob_up") or 0.0)
        numeric_prob = float(row.get("numeric_pred_prob_up") or 0.0)
        items.append(
            {
                "code": str(row.get("code") or ""),
                "changePct": _finite_float_or_none(row.get("return_1m_pre")) or 0.0,
                "weeklyBreakoutUpProb": float(max(0.0, min(1.0, numeric_prob))),
                "monthlyBreakoutUpProb": float(max(0.0, min(1.0, image_prob))),
                "monthlyRangeProb": float(max(0.0, min(1.0, 1.0 - max(image_prob, numeric_prob)))),
                "candleTripletUp": float(max(0.0, min(1.0, image_prob))),
                "liquidity20d": float(max(0.0, _finite_float_or_none(row.get("adv_proxy")) or 0.0)),
                "hybridScore": float(max(0.0, min(1.0, max(image_prob, numeric_prob)))),
                "setupType": "watch",
            }
        )
    return items


def _preview_entry_decorated_items(
    *,
    items: list[dict[str, Any]],
    prior_snapshot: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    decorated: list[dict[str, Any]] = []
    for base in items:
        item = dict(base)
        code = str(item.get("code") or "")
        change = rankings_cache._first_finite(item.get("changePct"))
        weekly_breakout = rankings_cache._first_finite(item.get("weeklyBreakoutUpProb"))
        monthly_breakout = rankings_cache._first_finite(item.get("monthlyBreakoutUpProb"))
        monthly_range = rankings_cache._first_finite(item.get("monthlyRangeProb"))
        candle = rankings_cache._first_finite(item.get("candleTripletUp"))
        liquidity = rankings_cache._first_finite(item.get("liquidity20d"))
        prob_proxy = rankings_cache._first_finite(weekly_breakout, monthly_breakout, candle, abs(change) if change is not None else None)
        if prob_proxy is None:
            prob_proxy = 0.0
        rule_signal = float(change or 0.0)
        entry_score = float(
            0.34 * max(0.0, min(1.0, float(prob_proxy)))
            + 0.24 * max(0.0, min(1.0, (rule_signal + 0.06) / 0.14))
            + 0.22 * max(0.0, min(1.0, float(weekly_breakout or 0.0)))
            + 0.20 * max(0.0, min(1.0, float(monthly_breakout or 0.0)))
        )
        if monthly_range is not None and monthly_range >= 0.72 and (monthly_breakout is None or monthly_breakout < 0.55):
            entry_score -= 0.05
        research_bonus = rankings_cache._calc_research_prior_bonus(
            item=item,
            direction="up",
            code=code,
            prior_snapshot=prior_snapshot,
        )
        entry_score = float(max(0.0, min(1.0, entry_score + float(research_bonus))))
        gate_ok = bool(
            liquidity is not None
            and float(prob_proxy) >= rankings_cache._DAILY_RULE_GATE_MIN_PROB
            and (weekly_breakout is not None and float(weekly_breakout) >= rankings_cache._DAILY_RULE_GATE_MIN_BREAKOUT)
            and entry_score >= rankings_cache._DAILY_RULE_GATE_MIN_ENTRY_SCORE
        )
        if gate_ok and (monthly_breakout is not None and float(monthly_breakout) >= 0.60):
            setup_type = "breakout"
        elif gate_ok:
            setup_type = "watch"
        else:
            setup_type = "reject"
        item["entryScore"] = float(entry_score)
        item["entryQualified"] = bool(gate_ok)
        item["setupType"] = setup_type
        decorated.append(item)
    decorated.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("monthlyBreakoutUpProb") or 0.0),
            item.get("code", ""),
        )
    )
    return decorated


def _rank_position_map(items: list[dict[str, Any]]) -> dict[str, int]:
    return {str(item.get("code") or ""): idx + 1 for idx, item in enumerate(items) if str(item.get("code") or "")}


def _evaluate_rebound_ranking_impact(
    *,
    latest_frame: pd.DataFrame,
    candidate_frame: pd.DataFrame,
    dataset_id: str,
    source_artifacts: dict[str, str],
    asof_iso: str,
    source_disposition: str,
    variant_name: str,
) -> dict[str, Any]:
    preview_items = _make_ranking_preview_items(latest_frame)
    baseline_items = _preview_entry_decorated_items(items=preview_items, prior_snapshot=None)
    baseline_rank_map = _rank_position_map(baseline_items)
    baseline_setup_map = {str(item.get("code") or ""): str(item.get("setupType") or "") for item in baseline_items}
    baseline_hybrid_map = {str(item.get("code") or ""): item.get("hybridScore") for item in baseline_items}

    snapshot = _build_rebound_bridge_snapshot(
        candidate_frame=candidate_frame,
        dataset_id=dataset_id,
        source_artifacts=source_artifacts,
        asof_iso=asof_iso,
        source_disposition=source_disposition,
        run_id=f"{variant_name}_{asof_iso.replace('-', '')}",
    )
    variant_items = _preview_entry_decorated_items(items=preview_items, prior_snapshot=snapshot)
    variant_rank_map = _rank_position_map(variant_items)
    changed_codes = sorted(
        code
        for code, rank in variant_rank_map.items()
        if baseline_rank_map.get(code) is not None and int(rank) != int(baseline_rank_map[code])
    )
    positive_bonus_values = [
        float(item.get("researchPriorBonus") or 0.0)
        for item in variant_items
        if rankings_cache._first_finite(item.get("researchPriorBonus")) is not None and float(item.get("researchPriorBonus") or 0.0) > 0.0
    ]
    bonus_positive_count = int(len(positive_bonus_values))
    mean_bonus = float(
        np.mean(positive_bonus_values)
    ) if bonus_positive_count > 0 else 0.0
    max_bonus = float(
        max(
            [float(item.get("researchPriorBonus") or 0.0) for item in variant_items],
            default=0.0,
        )
    )
    hybrid_changed_count = sum(
        1
        for item in variant_items
        if baseline_hybrid_map.get(str(item.get("code") or "")) != item.get("hybridScore")
    )
    setup_type_changed_count = sum(
        1
        for item in variant_items
        if baseline_setup_map.get(str(item.get("code") or "")) != str(item.get("setupType") or "")
    )
    return {
        "research_prior_bonus_positive_count": int(bonus_positive_count),
        "entry_score_rank_changed_count": int(len(changed_codes)),
        "entry_score_rank_changed_codes": changed_codes,
        "mean_bonus": float(mean_bonus),
        "max_bonus": float(max_bonus),
        "hybrid_score_changed_count": int(hybrid_changed_count),
        "setup_type_overridden_count": int(setup_type_changed_count),
    }


def _winner_month_frame_for_regime(
    *,
    dataset_id: str,
    regime_tag: str,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    regime_compare = read_json(dataset_dir / "regime_compare.json")
    frame = _formal_compare_test_frame(
        predictions,
        expected_count=int(fidelity_compare["common_eligible_sample_count"]),
    )
    winner_months = _winner_months_for_regime(regime_compare, regime_tag=regime_tag)
    scoped = frame.loc[frame["as_of_date"].isin(winner_months)].copy().reset_index(drop=True)
    return {
        "dataset_dir": dataset_dir,
        "fidelity_compare": fidelity_compare,
        "regime_compare": regime_compare,
        "winner_months": winner_months,
        "frame": scoped,
    }


def _load_rebound_formal_feature_frame(
    *,
    dataset_id: str,
    max_trade_date: int | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    winner_payload = _winner_month_frame_for_regime(dataset_id=dataset_id, regime_tag="rebound_onset")
    winner_frame = winner_payload["frame"]
    if winner_frame.empty:
        raise RuntimeError("rebound formal feature frame requires non-empty winner-month subset")
    capped_trade_date = int(max_trade_date) if max_trade_date is not None else int(winner_frame["as_of_date"].max())
    codes = sorted({_normalize_code_key(code) for code in winner_frame["code"].tolist() if _normalize_code_key(code)})
    history_frame = _load_export_history(
        export_db_path=str(dataset_manifest["source_export_db_path"]),
        codes=codes,
        max_trade_date=capped_trade_date,
    )
    feature_frame = _attach_micro_feature_rows(winner_frame, history_frame)
    if feature_frame.empty:
        raise RuntimeError("rebound formal feature frame produced no feature rows")
    return {
        "dataset_dir": dataset_dir,
        "dataset_manifest": dataset_manifest,
        "winner_payload": winner_payload,
        "feature_frame": feature_frame,
    }


def _approx_p40(stat_payload: dict[str, Any], feature_name: str) -> float | None:
    feature = stat_payload.get(feature_name)
    if not isinstance(feature, dict):
        return None
    p25 = _safe_float(feature.get("p25"))
    median = _safe_float(feature.get("median"))
    if p25 is None and median is None:
        return None
    if p25 is None:
        return median
    if median is None:
        return p25
    return float(p25 + (median - p25) * 0.60)

def build_event_image_pattern_boundary(
    *,
    dataset_id: str,
    primary_regime: str,
    comparison_regime: str,
    output_root: str | Path | None = None,
    max_workers: int = 2,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    regimes = [str(primary_regime), str(comparison_regime)]
    worker_count = max(1, min(int(max_workers), len(regimes)))
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(build_event_image_pattern_micro_features, dataset_id=dataset_id, regime_tag=regime_tag, output_root=target_dir): regime_tag
            for regime_tag in regimes
        }
        micro_results = {future_map[future]: future.result() for future in as_completed(future_map)}

    primary_micro = read_json(Path(micro_results[primary_regime]["pattern_micro_features_summary_path"]))
    comparison_micro = read_json(Path(micro_results[comparison_regime]["pattern_micro_features_summary_path"]))
    primary_decomposition = read_json(target_dir / f"pattern_decomposition_{primary_regime}.json")
    comparison_decomposition = read_json(target_dir / f"pattern_decomposition_{comparison_regime}.json")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")

    feature_deltas: dict[str, dict[str, float | None]] = {}
    for feature_name in _micro_feature_columns():
        primary_tp = _mean_stat(primary_micro["true_positive_stats"], feature_name)
        comparison_tp = _mean_stat(comparison_micro["true_positive_stats"], feature_name)
        primary_fp = _mean_stat(primary_micro["false_positive_stats"], feature_name)
        comparison_fp = _mean_stat(comparison_micro["false_positive_stats"], feature_name)
        primary_numeric = _mean_stat(primary_micro["numeric_top_stats"], feature_name)
        comparison_numeric = _mean_stat(comparison_micro["numeric_top_stats"], feature_name)
        feature_deltas[feature_name] = {
            "primary_true_positive_mean": primary_tp,
            "comparison_true_positive_mean": comparison_tp,
            "delta_primary_vs_comparison": None if primary_tp is None or comparison_tp is None else float(primary_tp - comparison_tp),
            "primary_false_positive_mean": primary_fp,
            "comparison_false_positive_mean": comparison_fp,
            "primary_numeric_top_mean": primary_numeric,
            "comparison_numeric_top_mean": comparison_numeric,
        }

    primary_advantage_features = [
        "price_vs_ma120",
        "distance_from_60d_high",
        "pullback_from_recent_high_pct",
        "realized_vol20",
    ]
    comparison_advantage_features = [
        "volume_change20",
        "volume_spike_ratio_5_20",
        "price_vs_ma60",
        "price_vs_ma120",
        "realized_vol20",
    ]
    candidate_boundary_rule = {
        str(primary_regime): {
            "dist_ma120_min": primary_decomposition["candidate_rule"]["trend_strength"].get("dist_ma120_min"),
            "position_from_60d_high_range": primary_decomposition["candidate_rule"]["setup_quality"].get("position_from_60d_high_range"),
            "volume_change20_max": _mean_stat(primary_micro["true_positive_stats"], "volume_change20"),
            "realized_vol20_min": primary_decomposition["candidate_rule"]["tradability_risk"].get("realized_vol20_max"),
        },
        str(comparison_regime): {
            "volume_change20_min": comparison_decomposition["candidate_rule"]["setup_quality"].get("volume_change20_min"),
            "realized_vol20_max": comparison_decomposition["candidate_rule"]["tradability_risk"].get("realized_vol20_max"),
            "price_vs_ma60_min": _mean_stat(comparison_micro["true_positive_stats"], "price_vs_ma60"),
            "price_vs_ma120_min": _mean_stat(comparison_micro["true_positive_stats"], "price_vs_ma120"),
        },
    }
    uptrend_disposition = "hold" if (_mean_stat(primary_micro["true_positive_stats"], "realized_vol20") or 0.0) > (_mean_stat(comparison_micro["true_positive_stats"], "realized_vol20") or 0.0) and (_mean_stat(comparison_micro["true_positive_stats"], "volume_change20") or 0.0) > (_mean_stat(primary_micro["true_positive_stats"], "volume_change20") or 0.0) else "keep"
    disposition_recommendation = {
        str(primary_regime): "keep_strengthened",
        str(comparison_regime): uptrend_disposition,
    }
    payload = {
        "schema_version": PATTERN_BOUNDARY_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "fidelity_compare": str(dataset_dir / "fidelity_compare.json"),
            "primary_micro_features": str(micro_results[primary_regime]["pattern_micro_features_summary_path"]),
            "comparison_micro_features": str(micro_results[comparison_regime]["pattern_micro_features_summary_path"]),
            "primary_pattern": str(target_dir / f"pattern_decomposition_{primary_regime}.json"),
            "comparison_pattern": str(target_dir / f"pattern_decomposition_{comparison_regime}.json"),
        },
        "primary_regime": str(primary_regime),
        "comparison_regime": str(comparison_regime),
        "common_contract": {
            "same_dataset": True,
            "same_split": True,
            "same_labels": True,
            "same_horizon": True,
            "same_universe": True,
            "same_sample_keys": bool(fidelity_compare.get("same_sample_keys", False)),
            "sample_key_definition": str(fidelity_compare.get("sample_key_definition", "as_of_date + code + label")),
            "common_eligible_sample_count": int(fidelity_compare["common_eligible_sample_count"]),
            "dropped_by_warmup_v1_2_count": int(fidelity_compare["dropped_by_warmup_v1_2_count"]),
        },
        "feature_deltas": feature_deltas,
        "primary_advantage_features": primary_advantage_features,
        "comparison_advantage_features": comparison_advantage_features,
        "candidate_boundary_rule": candidate_boundary_rule,
        "disposition_recommendation": disposition_recommendation,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / f"pattern_boundary_compare_{primary_regime}_vs_{comparison_regime}.json"
    md_path = target_dir / f"pattern_boundary_compare_{primary_regime}_vs_{comparison_regime}.md"
    _write_json_artifact(json_path, payload)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Pattern Boundary Compare",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- primary: `{primary_regime}`",
            f"- comparison: `{comparison_regime}`",
            "",
            "## Current State",
            f"- common eligible sample count: `{int(fidelity_compare['common_eligible_sample_count'])}`",
            f"- dropped by warmup: `{int(fidelity_compare['dropped_by_warmup_v1_2_count'])}`",
            "",
            "## What Changed",
            "- compared regime-level micro-feature artifacts to isolate where image advantage weakens",
            "",
            "## Evidence",
            f"- disposition recommendation: `{disposition_recommendation}`",
            f"- primary advantage features: `{primary_advantage_features}`",
            f"- comparison advantage features: `{comparison_advantage_features}`",
            "",
            "## Decision",
            f"- `{primary_regime}` stays `keep_strengthened`; `{comparison_regime}` is `{uptrend_disposition}`",
            "",
            "## Remaining Risks",
            "- boundary compare is still based on small winner month counts",
            "- candlestick micro-features are directional evidence, not final production rules",
            "",
            "## Next Single Step",
            "- update the pattern library candidates with boundary-driven adoption and non-adoption conditions",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern_boundary_compare_path": str(json_path),
        "pattern_boundary_compare_report_path": str(md_path),
    }


def build_event_image_pattern_gating(
    *,
    dataset_id: str,
    primary_regime: str,
    comparison_regime: str,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    boundary_path = target_dir / f"pattern_boundary_compare_{primary_regime}_vs_{comparison_regime}.json"
    if not boundary_path.exists():
        raise RuntimeError(f"missing boundary compare artifact: {boundary_path}")
    boundary_payload = read_json(boundary_path)
    primary_micro_path = Path(boundary_payload["source_artifacts"]["primary_micro_features"])
    comparison_micro_path = Path(boundary_payload["source_artifacts"]["comparison_micro_features"])
    primary_micro = read_json(primary_micro_path)
    comparison_micro = read_json(comparison_micro_path)
    pattern_library_path = target_dir / "pattern_library_candidates.json"
    pattern_library = read_json(pattern_library_path) if pattern_library_path.exists() else None
    regime_compare_path = dataset_dir / "regime_compare.json"
    regime_compare = read_json(regime_compare_path)

    formal_frame = _formal_compare_test_frame(predictions, expected_count=int(fidelity_compare["common_eligible_sample_count"]))
    primary_rule_raw = dict(boundary_payload["candidate_boundary_rule"][str(primary_regime)])
    comparison_rule_raw = dict(boundary_payload["candidate_boundary_rule"][str(comparison_regime)])
    primary_gate_rule = {
        "price_vs_ma120_min": primary_rule_raw.get("dist_ma120_min"),
        "distance_from_60d_high_range": primary_rule_raw.get("position_from_60d_high_range"),
        "realized_vol20_min": primary_rule_raw.get("realized_vol20_min"),
        "volume_change20_max": primary_rule_raw.get("volume_change20_max"),
    }
    comparison_gate_rule = {
        "price_vs_ma120_min": comparison_rule_raw.get("price_vs_ma120_min"),
        "price_vs_ma60_min": comparison_rule_raw.get("price_vs_ma60_min"),
        "realized_vol20_max": comparison_rule_raw.get("realized_vol20_max"),
        "volume_change20_min": comparison_rule_raw.get("volume_change20_min"),
    }
    primary_gate_mask = _apply_gate_mask(
        formal_frame,
        price_vs_ma120_min=primary_gate_rule["price_vs_ma120_min"],
        distance_from_60d_high_range=None if primary_gate_rule["distance_from_60d_high_range"] is None else tuple(primary_gate_rule["distance_from_60d_high_range"]),
        realized_vol20_min=primary_gate_rule["realized_vol20_min"],
        volume_change20_max=primary_gate_rule["volume_change20_max"],
    )
    gate_true = formal_frame.loc[primary_gate_mask].copy()
    gate_false = formal_frame.loc[~primary_gate_mask].copy()
    image_top = _top_candidates(formal_frame, prob_column="image_pred_prob_up", top_k=10)
    numeric_top = _top_candidates(formal_frame, prob_column="numeric_pred_prob_up", top_k=10)
    image_true_positive = image_top.loc[image_top["label_id"] == 1].copy()
    image_false_positive = image_top.loc[image_top["label_id"] == 0].copy()

    image_true_metrics = _subset_model_metrics(gate_true, prob_column="image_pred_prob_up", pred_column="image_pred_label")
    numeric_true_metrics = _subset_model_metrics(gate_true, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label")
    image_false_metrics = _subset_model_metrics(gate_false, prob_column="image_pred_prob_up", pred_column="image_pred_label")
    numeric_false_metrics = _subset_model_metrics(gate_false, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label")

    image_advantage_when_gate_true = {
        "balanced_accuracy_delta_vs_numeric": _metric_delta(image_true_metrics, numeric_true_metrics)["balanced_accuracy_delta"],
        "roc_auc_delta_vs_numeric": _metric_delta(image_true_metrics, numeric_true_metrics)["roc_auc_delta"],
        "monthly_top10_precision_up_delta_vs_numeric": _metric_delta(image_true_metrics, numeric_true_metrics)["monthly_top10_precision_up_delta"],
        "monthly_long_short_spread_delta_vs_numeric": _metric_delta(image_true_metrics, numeric_true_metrics)["monthly_long_short_spread_delta"],
    }
    numeric_advantage_when_gate_false = {
        "balanced_accuracy_delta_vs_image": _metric_delta(numeric_false_metrics, image_false_metrics)["balanced_accuracy_delta"],
        "roc_auc_delta_vs_image": _metric_delta(numeric_false_metrics, image_false_metrics)["roc_auc_delta"],
        "monthly_top10_precision_up_delta_vs_image": _metric_delta(numeric_false_metrics, image_false_metrics)["monthly_top10_precision_up_delta"],
        "monthly_long_short_spread_delta_vs_image": _metric_delta(numeric_false_metrics, image_false_metrics)["monthly_long_short_spread_delta"],
    }

    gate_hit_counts = {
        str(primary_regime): {
            "gate_value": True,
            "sample_count": int(len(gate_true)),
            "label_counts": {str(key): int(value) for key, value in gate_true["label"].value_counts().to_dict().items()},
            "image_top_candidate_hit_rate": float((image_top["sample_key"].isin(gate_true["sample_key"])).mean()) if len(image_top) > 0 else 0.0,
            "numeric_top_candidate_hit_rate": float((numeric_top["sample_key"].isin(gate_true["sample_key"])).mean()) if len(numeric_top) > 0 else 0.0,
            "image_true_positive_hit_rate": float((image_true_positive["sample_key"].isin(gate_true["sample_key"])).mean()) if len(image_true_positive) > 0 else 0.0,
            "image_false_positive_hit_rate": float((image_false_positive["sample_key"].isin(gate_true["sample_key"])).mean()) if len(image_false_positive) > 0 else 0.0,
        },
        str(comparison_regime): {
            "gate_value": False,
            "sample_count": int(len(gate_false)),
            "label_counts": {str(key): int(value) for key, value in gate_false["label"].value_counts().to_dict().items()},
            "image_top_candidate_hit_rate": float((image_top["sample_key"].isin(gate_false["sample_key"])).mean()) if len(image_top) > 0 else 0.0,
            "numeric_top_candidate_hit_rate": float((numeric_top["sample_key"].isin(gate_false["sample_key"])).mean()) if len(numeric_top) > 0 else 0.0,
            "image_true_positive_hit_rate": float((image_true_positive["sample_key"].isin(gate_false["sample_key"])).mean()) if len(image_true_positive) > 0 else 0.0,
            "image_false_positive_hit_rate": float((image_false_positive["sample_key"].isin(gate_false["sample_key"])).mean()) if len(image_false_positive) > 0 else 0.0,
        },
    }

    true_positive_deltas = [value for value in image_advantage_when_gate_true.values() if value is not None and value > 0.0]
    false_positive_deltas = [value for value in numeric_advantage_when_gate_false.values() if value is not None and value > 0.0]
    rebound_disposition = "keep_strengthened" if len(true_positive_deltas) >= 2 else "keep"
    uptrend_disposition = "hold" if len(false_positive_deltas) >= 2 else "keep"

    gating_payload = {
        "schema_version": PATTERN_GATING_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
            "predictions": str(dataset_dir / "predictions.parquet"),
            "regime_compare": str(regime_compare_path),
            "boundary_compare": str(boundary_path),
            "primary_micro_features": str(primary_micro_path),
            "comparison_micro_features": str(comparison_micro_path),
            "pattern_library_candidates": None if pattern_library is None else str(pattern_library_path),
            "source_export_db_path": str(dataset_manifest["source_export_db_path"]),
        },
        "primary_regime": str(primary_regime),
        "comparison_regime": str(comparison_regime),
        "gating_dimensions": [
            "price_vs_ma120",
            "distance_from_60d_high",
            "realized_vol20",
            "volume_change20",
        ],
        "candidate_gate_rule": {
            str(primary_regime): primary_gate_rule,
            str(comparison_regime): comparison_gate_rule,
        },
        "gate_hit_counts": gate_hit_counts,
        "image_advantage_when_gate_true": image_advantage_when_gate_true,
        "numeric_advantage_when_gate_false": numeric_advantage_when_gate_false,
        "disposition_recommendation": {
            str(primary_regime): rebound_disposition,
            str(comparison_regime): uptrend_disposition,
        },
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / f"pattern_gating_rule_{primary_regime}_vs_{comparison_regime}.json"
    md_path = target_dir / f"pattern_gating_rule_{primary_regime}_vs_{comparison_regime}.md"
    _write_json_artifact(json_path, gating_payload)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Pattern Gating Rule",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- primary: `{primary_regime}`",
            f"- comparison: `{comparison_regime}`",
            "",
            "## Current State",
            f"- formal compare scope: `common eligible subset on test split only`",
            f"- gate true sample count: `{gate_hit_counts[str(primary_regime)]['sample_count']}`",
            f"- gate false sample count: `{gate_hit_counts[str(comparison_regime)]['sample_count']}`",
            "",
            "## What Changed",
            "- fixed a candidate gating rule on the test-split common subset and measured where image edge expands or weakens",
            "",
            "## Evidence",
            f"- image advantage when gate true: `{image_advantage_when_gate_true}`",
            f"- numeric advantage when gate false: `{numeric_advantage_when_gate_false}`",
            "",
            "## Decision",
            f"- `{primary_regime}`: `{rebound_disposition}`",
            f"- `{comparison_regime}`: `{uptrend_disposition}`",
            "",
            "## Remaining Risks",
            "- gating is still derived from small winner month counts",
            "- gate false is a complement bucket, not a pure uptrend classifier",
            "",
            "## Next Single Step",
            "- update the pattern library candidates with gate conditions and gate hit counts",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern_gating_rule_path": str(json_path),
        "pattern_gating_rule_report_path": str(md_path),
    }


def build_event_image_pattern_combo_rules(
    *,
    dataset_id: str,
    primary_regime: str,
    comparison_regime: str,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    regime_compare_path = dataset_dir / "regime_compare.json"
    regime_compare = read_json(regime_compare_path)
    boundary_path = target_dir / f"pattern_boundary_compare_{primary_regime}_vs_{comparison_regime}.json"
    gating_path = target_dir / f"pattern_gating_rule_{primary_regime}_vs_{comparison_regime}.json"
    if not boundary_path.exists():
        raise RuntimeError(f"missing boundary compare artifact: {boundary_path}")
    if not gating_path.exists():
        raise RuntimeError(f"missing gating artifact: {gating_path}")
    boundary_payload = read_json(boundary_path)
    gating_payload = read_json(gating_path)
    primary_micro_path = Path(boundary_payload["source_artifacts"]["primary_micro_features"])
    comparison_micro_path = Path(boundary_payload["source_artifacts"]["comparison_micro_features"])
    primary_micro = read_json(primary_micro_path)
    comparison_micro = read_json(comparison_micro_path)
    pattern_library_path = target_dir / "pattern_library_candidates.json"
    pattern_library = read_json(pattern_library_path) if pattern_library_path.exists() else None

    formal_frame = _formal_compare_test_frame(predictions, expected_count=int(fidelity_compare["common_eligible_sample_count"]))
    sample_codes = sorted({str(code) for code in formal_frame["code"].astype(str).tolist()})
    history_frame = _load_export_history(
        export_db_path=str(dataset_manifest["source_export_db_path"]),
        codes=sample_codes,
        max_trade_date=int(formal_frame["as_of_date"].max()),
    )
    formal_features = _attach_micro_feature_rows(formal_frame, history_frame)
    if len(formal_features) != len(formal_frame):
        raise RuntimeError(f"combo rule requires full feature coverage on test split: expected={len(formal_frame)}, actual={len(formal_features)}")
    image_top = _top_candidates(formal_features, prob_column="image_pred_prob_up", top_k=10)
    numeric_top = _top_candidates(formal_features, prob_column="numeric_pred_prob_up", top_k=10)
    image_true_positive = image_top.loc[image_top["label_id"] == 1].copy()
    image_false_positive = image_top.loc[image_top["label_id"] == 0].copy()

    primary_gate = gating_payload["candidate_gate_rule"][str(primary_regime)]
    comparison_gate = gating_payload["candidate_gate_rule"][str(comparison_regime)]
    primary_stats = primary_micro["true_positive_stats"]
    comparison_stats = comparison_micro["true_positive_stats"]

    combo_templates = {
        "rebound_tail_support": {
            "target_regime": str(primary_regime),
            "conditions": [
                {"column": "is_bull_last", "operator": "eq", "value": 1.0, "enabled": True},
                {"column": "lower_wick_pct_last", "operator": "gte", "value": _median_stat(primary_stats, "lower_wick_pct_last"), "enabled": _median_stat(primary_stats, "lower_wick_pct_last") is not None},
                {"column": "lower_close_count_5", "operator": "gte", "value": _median_stat(primary_stats, "lower_close_count_5"), "enabled": _median_stat(primary_stats, "lower_close_count_5") is not None},
                {"column": "price_vs_ma120", "operator": "gte", "value": primary_gate.get("price_vs_ma120_min"), "enabled": primary_gate.get("price_vs_ma120_min") is not None},
                {"column": "distance_from_60d_high", "operator": "between", "value": primary_gate.get("distance_from_60d_high_range"), "enabled": primary_gate.get("distance_from_60d_high_range") is not None},
                {"column": "realized_vol20", "operator": "gte", "value": primary_gate.get("realized_vol20_min"), "enabled": primary_gate.get("realized_vol20_min") is not None},
            ],
        },
        "rebound_coil_support": {
            "target_regime": str(primary_regime),
            "conditions": [
                {"column": "bull_streak_3", "operator": "gte", "value": max(1.0, float(np.floor(_median_stat(primary_stats, "bull_streak_3")))) if _median_stat(primary_stats, "bull_streak_3") is not None else None, "enabled": _median_stat(primary_stats, "bull_streak_3") is not None},
                {"column": "narrow_range_count_5", "operator": "gte", "value": float(np.floor(_median_stat(primary_stats, "narrow_range_count_5"))) if _median_stat(primary_stats, "narrow_range_count_5") is not None else None, "enabled": _median_stat(primary_stats, "narrow_range_count_5") is not None},
                {"column": "ma20_slope_5", "operator": "gt", "value": 0.0, "enabled": True},
                {"column": "ma60_slope_5", "operator": "gt", "value": 0.0, "enabled": True},
                {"column": "price_vs_ma120", "operator": "gte", "value": primary_gate.get("price_vs_ma120_min"), "enabled": primary_gate.get("price_vs_ma120_min") is not None},
                {"column": "distance_from_60d_high", "operator": "between", "value": primary_gate.get("distance_from_60d_high_range"), "enabled": primary_gate.get("distance_from_60d_high_range") is not None},
            ],
        },
        "uptrend_pullback_continue": {
            "target_regime": str(comparison_regime),
            "conditions": [
                {"column": "is_bear_last", "operator": "eq", "value": 1.0, "enabled": True},
                {"column": "volume_change20", "operator": "gte", "value": comparison_gate.get("volume_change20_min"), "enabled": comparison_gate.get("volume_change20_min") is not None},
                {"column": "price_vs_ma60", "operator": "gte", "value": comparison_gate.get("price_vs_ma60_min"), "enabled": comparison_gate.get("price_vs_ma60_min") is not None},
                {"column": "price_vs_ma120", "operator": "gte", "value": comparison_gate.get("price_vs_ma120_min"), "enabled": comparison_gate.get("price_vs_ma120_min") is not None},
                {"column": "realized_vol20", "operator": "lte", "value": comparison_gate.get("realized_vol20_max"), "enabled": comparison_gate.get("realized_vol20_max") is not None},
            ],
        },
        "uptrend_volume_continue": {
            "target_regime": str(comparison_regime),
            "conditions": [
                {"column": "bull_streak_3", "operator": "gte", "value": max(1.0, float(np.floor(_median_stat(comparison_stats, "bull_streak_3")))) if _median_stat(comparison_stats, "bull_streak_3") is not None else None, "enabled": _median_stat(comparison_stats, "bull_streak_3") is not None},
                {"column": "higher_close_count_5", "operator": "gte", "value": float(np.floor(_median_stat(comparison_stats, "higher_close_count_5"))) if _median_stat(comparison_stats, "higher_close_count_5") is not None else None, "enabled": _median_stat(comparison_stats, "higher_close_count_5") is not None},
                {"column": "volume_spike_ratio_5_20", "operator": "gte", "value": _median_stat(comparison_stats, "volume_spike_ratio_5_20"), "enabled": _median_stat(comparison_stats, "volume_spike_ratio_5_20") is not None},
                {"column": "volume_change20", "operator": "gte", "value": comparison_gate.get("volume_change20_min"), "enabled": comparison_gate.get("volume_change20_min") is not None},
                {"column": "price_vs_ma60", "operator": "gte", "value": comparison_gate.get("price_vs_ma60_min"), "enabled": comparison_gate.get("price_vs_ma60_min") is not None},
                {"column": "price_vs_ma120", "operator": "gte", "value": comparison_gate.get("price_vs_ma120_min"), "enabled": comparison_gate.get("price_vs_ma120_min") is not None},
            ],
        },
    }

    combo_results: dict[str, dict[str, Any]] = {}
    for combo_name, template in combo_templates.items():
        combo_mask = _apply_combo_rule(formal_features, conditions=list(template["conditions"]))
        combo_frame = formal_features.loc[combo_mask].copy()
        image_metrics = _subset_model_metrics(combo_frame, prob_column="image_pred_prob_up", pred_column="image_pred_label")
        numeric_metrics = _subset_model_metrics(combo_frame, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label")
        metric_deltas = {
            "balanced_accuracy_delta_vs_numeric": _metric_delta(image_metrics, numeric_metrics)["balanced_accuracy_delta"],
            "roc_auc_delta_vs_numeric": _metric_delta(image_metrics, numeric_metrics)["roc_auc_delta"],
            "monthly_top10_precision_up_delta_vs_numeric": _metric_delta(image_metrics, numeric_metrics)["monthly_top10_precision_up_delta"],
            "monthly_long_short_spread_delta_vs_numeric": _metric_delta(image_metrics, numeric_metrics)["monthly_long_short_spread_delta"],
        }
        combo_results[combo_name] = {
            "target_regime": str(template["target_regime"]),
            "conditions": list(template["conditions"]),
            "sample_count": int(len(combo_frame)),
            "label_counts": {str(key): int(value) for key, value in combo_frame["label"].value_counts().to_dict().items()},
            "image_top_candidate_hit_rate": float((image_top["sample_key"].isin(combo_frame["sample_key"])).mean()) if len(image_top) > 0 else 0.0,
            "numeric_top_candidate_hit_rate": float((numeric_top["sample_key"].isin(combo_frame["sample_key"])).mean()) if len(numeric_top) > 0 else 0.0,
            "image_true_positive_hit_rate": float((image_true_positive["sample_key"].isin(combo_frame["sample_key"])).mean()) if len(image_true_positive) > 0 else 0.0,
            "image_false_positive_hit_rate": float((image_false_positive["sample_key"].isin(combo_frame["sample_key"])).mean()) if len(image_false_positive) > 0 else 0.0,
            "metric_deltas": metric_deltas,
        }

    primary_combo_names = [name for name, template in combo_templates.items() if str(template["target_regime"]) == str(primary_regime)]
    comparison_combo_names = [name for name, template in combo_templates.items() if str(template["target_regime"]) == str(comparison_regime)]
    recommended_primary_combo = max(primary_combo_names, key=lambda name: _score_combo_result(combo_results[name]))
    recommended_comparison_combo = max(comparison_combo_names, key=lambda name: _score_combo_result(combo_results[name]))

    primary_metrics = combo_results[recommended_primary_combo]["metric_deltas"]
    comparison_metrics = combo_results[recommended_comparison_combo]["metric_deltas"]
    primary_disposition = (
        "keep_strengthened"
        if (primary_metrics["monthly_long_short_spread_delta_vs_numeric"] or 0.0) > 0.0
        and (primary_metrics["balanced_accuracy_delta_vs_numeric"] or 0.0) > 0.0
        else "keep"
    )
    comparison_disposition = (
        "hold"
        if (comparison_metrics["monthly_long_short_spread_delta_vs_numeric"] or 0.0) <= 0.0
        and (comparison_metrics["monthly_top10_precision_up_delta_vs_numeric"] or 0.0) <= 0.0
        else "keep"
    )

    payload = {
        "schema_version": "tradex_event_image_dataset_pattern_combo_v1",
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
            "predictions": str(dataset_dir / "predictions.parquet"),
            "regime_compare": str(regime_compare_path),
            "primary_micro_features": str(primary_micro_path),
            "comparison_micro_features": str(comparison_micro_path),
            "boundary_compare": str(boundary_path),
            "gating_compare": str(gating_path),
            "pattern_library_candidates": None if pattern_library is None else str(pattern_library_path),
            "source_export_db_path": str(dataset_manifest["source_export_db_path"]),
        },
        "primary_regime": str(primary_regime),
        "comparison_regime": str(comparison_regime),
        "combo_templates": combo_templates,
        "combo_results": combo_results,
        "recommended_primary_combo": {
            "combo_name": str(recommended_primary_combo),
            "result": combo_results[recommended_primary_combo],
        },
        "recommended_comparison_combo": {
            "combo_name": str(recommended_comparison_combo),
            "result": combo_results[recommended_comparison_combo],
        },
        "disposition_recommendation": {
            str(primary_regime): primary_disposition,
            str(comparison_regime): comparison_disposition,
        },
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / f"pattern_combo_rules_{primary_regime}_vs_{comparison_regime}.json"
    md_path = target_dir / f"pattern_combo_rules_{primary_regime}_vs_{comparison_regime}.md"
    _write_json_artifact(json_path, payload)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Pattern Combo Rules",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- primary: `{primary_regime}`",
            f"- comparison: `{comparison_regime}`",
            "",
            "## Current State",
            f"- formal compare scope: `common eligible subset on test split only`",
            f"- primary combo: `{recommended_primary_combo}`",
            f"- comparison combo: `{recommended_comparison_combo}`",
            "",
            "## What Changed",
            "- evaluated deterministic 2-3 candle + MA + volume combo templates on the test common subset",
            "",
            "## Evidence",
            f"- primary combo result: `{combo_results[recommended_primary_combo]}`",
            f"- comparison combo result: `{combo_results[recommended_comparison_combo]}`",
            "",
            "## Decision",
            f"- `{primary_regime}`: `{primary_disposition}`",
            f"- `{comparison_regime}`: `{comparison_disposition}`",
            "",
            "## Remaining Risks",
            "- combo rules are still derived from small winner month counts",
            "- test common subset remains small for rebound_onset-specific combos",
            "",
            "## Next Single Step",
            "- update pattern library candidates with recommended combo rules and combo metrics",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern_combo_rules_path": str(json_path),
        "pattern_combo_rules_report_path": str(md_path),
    }


def _build_rebound_adoption_reasons(
    *,
    row: pd.Series,
    lower_wick_median: float | None,
    bull_streak_median: float | None,
    narrow_range_median: float | None,
) -> list[str]:
    reasons = ["120MA上", "60日高値未回復帯", "20日ボラ高め"]
    if int(row.get("is_bull_last") or 0) == 1:
        reasons.append("陽線引け")
    lower_wick = _finite_float_or_none(row.get("lower_wick_pct_last"))
    if lower_wick_median is not None and lower_wick is not None and lower_wick >= lower_wick_median:
        reasons.append("下ヒゲ強い")
    bull_streak = _finite_float_or_none(row.get("bull_streak_3"))
    if bull_streak_median is not None and bull_streak is not None and bull_streak >= max(1.0, math.floor(bull_streak_median)):
        reasons.append("陽線継続")
    narrow_range = _finite_float_or_none(row.get("narrow_range_count_5"))
    if narrow_range_median is not None and narrow_range is not None and narrow_range >= math.floor(narrow_range_median):
        reasons.append("値幅圧縮")
    if (_finite_float_or_none(row.get("ma20_slope_5")) or 0.0) > 0.0:
        reasons.append("MA20上向き")
    if (_finite_float_or_none(row.get("ma60_slope_5")) or 0.0) > 0.0:
        reasons.append("MA60上向き")
    return reasons


def build_event_image_pattern_adoption(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("adoption v1 currently supports rebound_onset only")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    dataset_dir = context["dataset_dir"]
    target_dir = context["target_dir"]
    fidelity_compare = context["fidelity_compare"]
    candidate_row = context["candidate_row"]
    latest_asof = int(context["latest_asof"])
    latest_frame = context["latest_frame"]
    candidate_frame = _score_rebound_candidate_frame(
        candidate_frame=_build_rebound_core_gate_candidate_frame(
            feature_frame=context["feature_frame"],
            gate_rule=context["gate_rule"],
        ),
        lower_wick_median=context["lower_wick_median"],
        bull_streak_median=context["bull_streak_median"],
        narrow_range_median=context["narrow_range_median"],
        threshold_for_bonus=0.50,
    )

    if candidate_frame.empty:
        raise RuntimeError("rebound adoption produced no core-gate candidates")

    asof_iso = _ymd_int_to_iso(latest_asof)
    if asof_iso is None:
        raise RuntimeError(f"failed to convert latest as_of to iso date: {latest_asof}")

    candidate_export_columns = [
        "sample_id",
        "as_of_date",
        "code",
        "label_id",
        "image_pred_prob_up",
        "numeric_pred_prob_up",
        "price_vs_ma120",
        "distance_from_60d_high",
        "realized_vol20",
        "fit_score",
        "bonus_eligible",
        "pattern_tag",
        "adoption_reasons",
        "rank",
    ]
    if "label" in candidate_frame.columns:
        candidate_export_columns.insert(3, "label")

    adoption_artifact = {
        "schema_version": PATTERN_ADOPTION_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": context["source_artifacts"],
        "compare_contract": {
            "same_dataset": True,
            "same_split": True,
            "same_labels": True,
            "same_horizon": True,
            "same_universe": True,
            "same_sample_keys": bool(fidelity_compare.get("same_sample_keys", False)),
            "sample_key_definition": str(fidelity_compare.get("sample_key_definition", "as_of_date + code + label")),
            "common_eligible_sample_count": int(fidelity_compare["common_eligible_sample_count"]),
        },
        "latest_asof": asof_iso,
        "candidate_counts": {
            "test_common_subset_count": int(len(context["test_common_frame"])),
            "latest_asof_sample_count": int(len(latest_frame)),
            "core_gate_candidate_count": int(len(candidate_frame)),
            "tag_candidate_count": int(len(candidate_frame)),
            "bonus_candidate_count": int(candidate_frame["bonus_eligible"].sum()),
        },
        "tag_candidate_count": int(len(candidate_frame)),
        "bonus_candidate_count": int(candidate_frame["bonus_eligible"].sum()),
        "bonus_codes": [
            str(row["code"])
            for row in candidate_frame.to_dict(orient="records")
            if bool(row["bonus_eligible"])
        ],
        "bonus_score_map": {
            str(row["code"]): float(row["fit_score"])
            for row in candidate_frame.to_dict(orient="records")
            if bool(row["bonus_eligible"])
        },
        "core_gate": context["gate_rule"],
        "fit_score_policy": {
            "threshold_for_bonus": 0.50,
            "weights": {
                "is_bull_last": 0.20,
                "lower_wick_pct_last": 0.20,
                "bull_streak_3": 0.15,
                "narrow_range_count_5": 0.15,
                "ma20_slope_5": 0.15,
                "ma60_slope_5": 0.15,
            },
        },
        "candidate_rows": candidate_frame[candidate_export_columns].to_dict(orient="records"),
        "source_disposition": str(candidate_row.get("disposition") or "keep"),
        "generated_at": _utc_now_iso(),
    }

    run_id = f"tradex_rebound_onset_aux_v1_{latest_asof}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    bridge_snapshot = _build_rebound_bridge_snapshot(
        candidate_frame=candidate_frame,
        dataset_id=str(dataset_id),
        source_artifacts=adoption_artifact["source_artifacts"],
        asof_iso=asof_iso,
        source_disposition=str(candidate_row.get("disposition") or "keep"),
        run_id=run_id,
    )

    bridge_latest_dir = Path(core_config.RESEARCH_BRIDGE_DIR) / "latest"
    bridge_snapshot_path = bridge_latest_dir / "research_prior_snapshot.json"
    bridge_manifest_path = bridge_latest_dir / "bridge_manifest.json"
    bridge_manifest = {"generated_at": _utc_now_iso(), "artifacts": {}}
    if bridge_manifest_path.exists():
        try:
            bridge_manifest = read_json(bridge_manifest_path)
        except Exception:
            bridge_manifest = {"generated_at": _utc_now_iso(), "artifacts": {}}
    artifacts_slot = bridge_manifest.get("artifacts")
    if not isinstance(artifacts_slot, dict):
        artifacts_slot = {}
    artifacts_slot["research_prior_snapshot.json"] = {
        "source_type": "event_image_dataset_adoption",
        "source_id": run_id,
        "generated_at": _utc_now_iso(),
        "filename": "research_prior_snapshot.json",
    }
    bridge_manifest["generated_at"] = _utc_now_iso()
    bridge_manifest["artifacts"] = artifacts_slot

    adoption_json_path = target_dir / "pattern_adoption_rebound_onset.json"
    adoption_md_path = target_dir / "pattern_adoption_rebound_onset.md"
    _write_json_artifact(adoption_json_path, adoption_artifact)
    _write_markdown_report(
        adoption_md_path,
        [
            "# TRADEX Rebound Adoption Artifact",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- latest_asof: `{asof_iso}`",
            f"- pattern: `rebound_onset`",
            "",
            "## Current State",
            f"- latest_asof_sample_count: `{int(len(latest_frame))}`",
            f"- core_gate_candidate_count: `{int(len(candidate_frame))}`",
            f"- tag_candidate_count: `{int(len(candidate_frame))}`",
            f"- bonus_candidate_count: `{int(candidate_frame['bonus_eligible'].sum())}`",
            f"- bonus_codes: `{adoption_artifact['bonus_codes']}`",
            "",
            "## What Changed",
            "- built the first MeeMee rebound_onset adoption artifact and published a bridge snapshot",
            "",
            "## Evidence",
            f"- source_disposition: `{candidate_row.get('disposition')}`",
            f"- top candidates: `{adoption_artifact['candidate_rows'][:5]}`",
            "",
            "## Decision",
            "- publish as auxiliary tag + micro bonus only; do not replace the main ranking",
            "",
            "## Remaining Risks",
            "- adoption uses restricted-universe test/common subset artifacts, not live future months",
            "- bonus candidates may remain sparse when the rebound gate is narrow",
            "",
            "## Next Single Step",
            "- wire the rebound_onset bridge fields into MeeMee ranking items and detail metadata without overriding list setupType",
        ],
    )
    _write_json_atomic(bridge_snapshot_path, bridge_snapshot)
    _write_json_atomic(bridge_manifest_path, bridge_manifest)
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "adoption_artifact_path": str(adoption_json_path),
        "adoption_report_path": str(adoption_md_path),
        "bridge_snapshot_path": str(bridge_snapshot_path),
        "bridge_manifest_path": str(bridge_manifest_path),
        "run_id": run_id,
    }


def build_event_image_pattern_adoption_compare(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("adoption compare currently supports rebound_onset only")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    dataset_dir = context["dataset_dir"]
    target_dir = context["target_dir"]
    candidate_row = context["candidate_row"]
    latest_asof = int(context["latest_asof"])
    asof_iso = _ymd_int_to_iso(latest_asof)
    if asof_iso is None:
        raise RuntimeError(f"failed to convert latest as_of to iso date: {latest_asof}")

    core_gate_candidates = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if core_gate_candidates.empty:
        raise RuntimeError("rebound adoption compare produced no core-gate candidates")

    variant_names = (
        "v1_0_strict_bonus",
        "v1_1_live_bonus",
        "v1_2_soft_bonus",
    )
    variant_results: list[dict[str, Any]] = []
    for variant_name in variant_names:
        threshold = _rebound_variant_name_to_threshold(variant_name)
        scored = _score_rebound_candidate_frame(
            candidate_frame=core_gate_candidates,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold,
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=scored,
            dataset_id=str(dataset_id),
            source_artifacts=context["source_artifacts"],
            asof_iso=asof_iso,
            source_disposition=str(candidate_row.get("disposition") or "keep"),
            variant_name=variant_name,
        )
        bonus_rows = [row for row in scored.to_dict(orient="records") if bool(row["bonus_eligible"])]
        variant_results.append(
            {
                "variant_name": variant_name,
                "threshold_for_bonus": threshold,
                "tag_candidate_count": int(len(scored)),
                "bonus_candidate_count": int(scored["bonus_eligible"].sum()),
                "bonus_codes": [str(row["code"]) for row in bonus_rows],
                "bonus_score_map": {str(row["code"]): float(row["fit_score"]) for row in bonus_rows},
                **impact,
            }
        )

    def _variant_score(row: dict[str, Any]) -> tuple[int, int, int, int]:
        return (
            1 if int(row["bonus_candidate_count"]) >= 1 else 0,
            1 if 1 <= int(row["entry_score_rank_changed_count"]) <= 5 else 0,
            1 if int(row["hybrid_score_changed_count"]) == 0 else 0,
            1 if int(row["setup_type_overridden_count"]) == 0 else 0,
        )

    recommended_variant = max(
        variant_results,
        key=lambda row: (
            _variant_score(row),
            -abs(3 - int(row["entry_score_rank_changed_count"])),
            float(row["mean_bonus"]),
            -float(row["threshold_for_bonus"]),
        ),
    )

    changed_count = int(recommended_variant["entry_score_rank_changed_count"])
    disposition = "hold"
    if (
        int(recommended_variant["bonus_candidate_count"]) >= 1
        and 1 <= changed_count <= 5
        and int(recommended_variant["hybrid_score_changed_count"]) == 0
        and int(recommended_variant["setup_type_overridden_count"]) == 0
    ):
        disposition = "keep_strengthened"
    elif int(recommended_variant["bonus_candidate_count"]) >= 1:
        disposition = "keep"
    elif all(int(row["bonus_candidate_count"]) == 0 for row in variant_results):
        disposition = "drop_reconsider"

    artifact = {
        "schema_version": PATTERN_ADOPTION_COMPARE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": context["source_artifacts"],
        "core_gate_contract": context["gate_rule"],
        "fit_score_weights": {
            "is_bull_last": 0.20,
            "lower_wick_pct_last": 0.20,
            "bull_streak_3": 0.15,
            "narrow_range_count_5": 0.15,
            "ma20_slope_5": 0.15,
            "ma60_slope_5": 0.15,
        },
        "latest_asof": asof_iso,
        "variant_results": variant_results,
        "recommended_variant": {
            "variant_name": str(recommended_variant["variant_name"]),
            "threshold_for_bonus": float(recommended_variant["threshold_for_bonus"]),
        },
        "disposition_recommendation": str(disposition),
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_adoption_compare_rebound_onset.json"
    md_path = target_dir / "pattern_adoption_compare_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Adoption Compare",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- latest_asof: `{asof_iso}`",
            f"- recommended_variant: `{artifact['recommended_variant']['variant_name']}`",
            "",
            "## Current State",
            *[
                f"- `{row['variant_name']}`: tag=`{row['tag_candidate_count']}`, bonus=`{row['bonus_candidate_count']}`, changed=`{row['entry_score_rank_changed_count']}`"
                for row in variant_results
            ],
            "",
            "## Decision",
            f"- disposition: `{disposition}`",
            "",
            "## Next Single Step",
            "- compare rebound_onset breadth candidates under the same common-eligible test contract",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_adoption_compare_path": str(json_path),
        "pattern_adoption_compare_report_path": str(md_path),
        "recommended_variant": str(recommended_variant["variant_name"]),
        "disposition_recommendation": disposition,
    }


def build_event_image_pattern_breadth(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("pattern breadth currently supports rebound_onset only")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    target_dir = context["target_dir"]
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    feature_frame = formal_features["feature_frame"]
    tp_stats = context["tp_stats"]
    gate_rule = context["gate_rule"]

    core_gate_frame = _build_rebound_core_gate_candidate_frame(feature_frame=feature_frame, gate_rule=gate_rule)
    if core_gate_frame.empty:
        raise RuntimeError("rebound breadth produced no core-gate rows")

    p40_lower_wick = _approx_p40(tp_stats, "lower_wick_pct_last")
    narrow_range_median = context["narrow_range_median"]
    base_lower_wick = context["lower_wick_median"]
    base_bull_streak = context["bull_streak_median"]

    baseline_scored = _score_rebound_candidate_frame(
        candidate_frame=core_gate_frame,
        lower_wick_median=base_lower_wick,
        bull_streak_median=base_bull_streak,
        narrow_range_median=narrow_range_median,
        threshold_for_bonus=0.50,
    )
    baseline_candidate_count = int(len(baseline_scored))

    true_positive = feature_frame.loc[feature_frame["image_pred_label"].astype(int) == feature_frame["label_id"].astype(int)].copy()
    true_positive = true_positive.loc[true_positive["label_id"].astype(int) == 1].copy()
    false_positive = feature_frame.loc[(feature_frame["image_pred_label"].astype(int) == 1) & (feature_frame["label_id"].astype(int) == 0)].copy()

    candidates = {
        "wick_bias_relaxed": _score_rebound_candidate_frame(
            candidate_frame=core_gate_frame.loc[
                core_gate_frame["lower_wick_pct_last"].astype(float) >= float(p40_lower_wick if p40_lower_wick is not None else base_lower_wick or 0.0)
            ].copy(),
            lower_wick_median=base_lower_wick,
            bull_streak_median=base_bull_streak,
            narrow_range_median=narrow_range_median,
            threshold_for_bonus=0.50,
        ),
        "coil_bias_relaxed": _score_rebound_candidate_frame(
            candidate_frame=core_gate_frame.loc[
                core_gate_frame["narrow_range_count_5"].astype(float) >= float(
                    max(2.0, math.floor(narrow_range_median) - 1.0) if narrow_range_median is not None else 2.0
                )
            ].copy(),
            lower_wick_median=base_lower_wick,
            bull_streak_median=base_bull_streak,
            narrow_range_median=narrow_range_median,
            threshold_for_bonus=0.50,
        ),
        "slope_dual_keep": _score_rebound_candidate_frame(
            candidate_frame=core_gate_frame.loc[
                (core_gate_frame["ma20_slope_5"].astype(float) > 0.0)
                & (core_gate_frame["ma60_slope_5"].astype(float) > 0.0)
            ].copy(),
            lower_wick_median=None,
            bull_streak_median=base_bull_streak,
            narrow_range_median=narrow_range_median,
            threshold_for_bonus=0.50,
        ),
    }

    candidate_results: list[dict[str, Any]] = []
    for candidate_name, candidate_frame in candidates.items():
        image_metrics = _subset_model_metrics(
            candidate_frame,
            prob_column="image_pred_prob_up",
            pred_column="image_pred_label",
        )
        numeric_metrics = _subset_model_metrics(
            candidate_frame,
            prob_column="numeric_pred_prob_up",
            pred_column="numeric_pred_label",
        )
        metric_deltas = _metric_delta(image_metrics, numeric_metrics)
        tp_rate = None
        if len(true_positive) > 0:
            tp_keys = set(true_positive["sample_id"].astype(str))
            hit_keys = set(candidate_frame["sample_id"].astype(str))
            tp_rate = float(len(tp_keys & hit_keys) / len(tp_keys))
        fp_rate = None
        if len(false_positive) > 0:
            fp_keys = set(false_positive["sample_id"].astype(str))
            hit_keys = set(candidate_frame["sample_id"].astype(str))
            fp_rate = float(len(fp_keys & hit_keys) / len(fp_keys))
        spread_delta = metric_deltas.get("monthly_long_short_spread_delta")
        precision_delta = metric_deltas.get("monthly_top10_precision_up_delta")
        disposition = (
            "keep"
            if int(len(candidate_frame)) >= baseline_candidate_count
            and spread_delta is not None
            and float(spread_delta) >= 0.0
            and precision_delta is not None
            and float(precision_delta) >= 0.0
            else "drop_reconsider"
        )
        candidate_results.append(
            {
                "candidate_name": candidate_name,
                "candidate_count": int(len(candidate_frame)),
                "bonus_eligible_count": int(candidate_frame["bonus_eligible"].sum()) if "bonus_eligible" in candidate_frame.columns else 0,
                "image_true_positive_hit_rate": tp_rate,
                "image_false_positive_hit_rate": fp_rate,
                "monthly_top10_precision_up_delta_vs_numeric": precision_delta,
                "monthly_long_short_spread_delta_vs_numeric": spread_delta,
                "disposition": disposition,
            }
        )

    recommended_candidate = max(
        candidate_results,
        key=lambda row: (
            1 if row["disposition"] == "keep" else 0,
            float(row["monthly_long_short_spread_delta_vs_numeric"] or float("-inf")),
            int(row["candidate_count"]),
        ),
    )

    artifact = {
        "schema_version": PATTERN_BREADTH_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            **context["source_artifacts"],
            "regime_compare": str(event_image_dataset_dir(dataset_id) / "regime_compare.json"),
        },
        "core_gate_contract": gate_rule,
        "baseline_candidate_count": baseline_candidate_count,
        "candidate_results": candidate_results,
        "recommended_candidate": str(recommended_candidate["candidate_name"]),
        "disposition_recommendation": "keep" if any(row["disposition"] == "keep" for row in candidate_results) else "hold",
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_breadth_rebound_onset.json"
    md_path = target_dir / "pattern_breadth_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Pattern Breadth",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- baseline_candidate_count: `{baseline_candidate_count}`",
            f"- recommended_candidate: `{artifact['recommended_candidate']}`",
            "",
            "## Candidate Results",
            *[
                f"- `{row['candidate_name']}`: count=`{row['candidate_count']}`, bonus=`{row['bonus_eligible_count']}`, spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`"
                for row in candidate_results
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_breadth_path": str(json_path),
        "pattern_breadth_report_path": str(md_path),
        "recommended_candidate": str(recommended_candidate["candidate_name"]),
        "disposition_recommendation": str(artifact["disposition_recommendation"]),
    }


def build_event_image_pattern_sequence_combo(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("sequence combo currently supports rebound_onset only")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    feature_frame = formal_features["feature_frame"]
    gate_rule = context["gate_rule"]
    current_core_gate = _build_rebound_core_gate_candidate_frame(feature_frame=feature_frame, gate_rule=gate_rule)
    current_core_gate_count = int(len(current_core_gate))
    tp_stats = context["tp_stats"]
    lower_wick_median = _median_stat(tp_stats, "lower_wick_pct_last") or 0.0

    true_positive = feature_frame.loc[
        (feature_frame["image_pred_label"].astype(int) == 1) & (feature_frame["label_id"].astype(int) == 1)
    ].copy()
    false_positive = feature_frame.loc[
        (feature_frame["image_pred_label"].astype(int) == 1) & (feature_frame["label_id"].astype(int) == 0)
    ].copy()

    templates: dict[str, list[dict[str, Any]]] = {
        "tail_hold_2bar": [
            {"column": "is_bull_prev1", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "lower_wick_pct_prev1", "operator": "gte", "value": float(lower_wick_median), "enabled": True},
            {"column": "close_above_prev1_body_mid_flag", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "engulfed_by_next_bear_flag", "operator": "eq", "value": 0.0, "enabled": True},
            {"column": "price_vs_ma120", "operator": "gte", "value": float(gate_rule["price_vs_ma120_min"]), "enabled": True},
            {"column": "ma20_slope_5", "operator": "gt", "value": 0.0, "enabled": True},
            {"column": "ma60_slope_5", "operator": "gt", "value": 0.0, "enabled": True},
        ],
        "two_red_reversal_3bar": [
            {"column": "is_bull_prev2", "operator": "eq", "value": 0.0, "enabled": True},
            {"column": "is_bull_prev1", "operator": "eq", "value": 0.0, "enabled": True},
            {"column": "is_bull_last", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "two_red_then_green_flag", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "higher_low_3_flag", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "price_vs_ma120", "operator": "gte", "value": float(gate_rule["price_vs_ma120_min"]), "enabled": True},
        ],
        "nr_bull_break_2bar": [
            {"column": "narrow_range_then_bull_flag", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "is_bull_last", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "distance_from_60d_high", "operator": "between", "value": tuple(gate_rule["distance_from_60d_high_range"]), "enabled": True},
            {"column": "ma20_slope_5", "operator": "gt", "value": 0.0, "enabled": True},
        ],
        "higher_lows_support_3bar": [
            {"column": "higher_low_3_flag", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "higher_close_3_flag", "operator": "eq", "value": 1.0, "enabled": True},
            {"column": "price_vs_ma120", "operator": "gte", "value": float(gate_rule["price_vs_ma120_min"]), "enabled": True},
            {"column": "realized_vol20", "operator": "gte", "value": float(gate_rule["realized_vol20_min"]), "enabled": True},
        ],
    }

    combo_results: list[dict[str, Any]] = []
    for candidate_name, conditions in templates.items():
        candidate_frame = feature_frame.loc[_apply_combo_rule(feature_frame, conditions=conditions)].copy().reset_index(drop=True)
        scored = _score_rebound_candidate_frame(
            candidate_frame=candidate_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=0.45,
        )
        image_metrics = _subset_model_metrics(scored, prob_column="image_pred_prob_up", pred_column="image_pred_label")
        numeric_metrics = _subset_model_metrics(scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label")
        metric_deltas = _metric_delta(image_metrics, numeric_metrics)
        candidate_keys = set(scored["sample_id"].astype(str)) if "sample_id" in scored.columns else set()
        tp_keys = set(true_positive["sample_id"].astype(str)) if "sample_id" in true_positive.columns else set()
        fp_keys = set(false_positive["sample_id"].astype(str)) if "sample_id" in false_positive.columns else set()
        precision_delta = metric_deltas.get("monthly_top10_precision_up_delta")
        spread_delta = metric_deltas.get("monthly_long_short_spread_delta")
        disposition = "drop_reconsider"
        if int(len(scored)) > current_core_gate_count and (precision_delta or 0.0) >= 0.0 and (spread_delta or 0.0) >= 0.0:
            disposition = "keep_strengthened"
        elif int(len(scored)) >= current_core_gate_count and (spread_delta or 0.0) >= 0.0:
            disposition = "keep"
        elif int(len(scored)) >= current_core_gate_count:
            disposition = "hold"
        combo_results.append(
            {
                "candidate_name": candidate_name,
                "conditions": conditions,
                "candidate_count": int(len(scored)),
                "tag_candidate_count": int(len(scored)),
                "bonus_eligible_count": int(scored["bonus_eligible"].sum()) if "bonus_eligible" in scored.columns else 0,
                "image_true_positive_hit_rate": None if not tp_keys else float(len(candidate_keys & tp_keys) / len(tp_keys)),
                "image_false_positive_hit_rate": None if not fp_keys else float(len(candidate_keys & fp_keys) / len(fp_keys)),
                "monthly_top10_precision_up_delta_vs_numeric": precision_delta,
                "monthly_long_short_spread_delta_vs_numeric": spread_delta,
                "disposition": disposition,
            }
        )

    recommended_candidate = max(
        combo_results,
        key=lambda row: (
            1 if str(row["disposition"]).startswith("keep") else 0,
            float(row["monthly_long_short_spread_delta_vs_numeric"] or float("-inf")),
            float(row["monthly_top10_precision_up_delta_vs_numeric"] or float("-inf")),
            int(row["candidate_count"]),
        ),
    )
    disposition = "hold"
    if any(str(row.get("disposition")) == "keep_strengthened" for row in combo_results):
        disposition = "keep_strengthened"
    elif any(str(row.get("disposition")) == "keep" for row in combo_results):
        disposition = "keep"

    target_dir = context["target_dir"]
    artifact = {
        "schema_version": PATTERN_SEQUENCE_COMBO_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            **context["source_artifacts"],
            "pattern_library": str(context["dataset_dir"] / "pattern_library_candidates.json"),
        },
        "current_core_gate_count": current_core_gate_count,
        "combo_templates": list(templates.keys()),
        "combo_results": combo_results,
        "recommended_candidate": recommended_candidate,
        "disposition_recommendation": disposition,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_sequence_combo_rebound_onset.json"
    md_path = target_dir / "pattern_sequence_combo_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Sequence Combo",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- current_core_gate_count: `{current_core_gate_count}`",
            f"- recommended_candidate: `{recommended_candidate['candidate_name']}`",
            "",
            "## Candidate Results",
            *[
                f"- `{row['candidate_name']}`: count=`{row['candidate_count']}`, bonus=`{row['bonus_eligible_count']}`, spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`"
                for row in combo_results
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_sequence_combo_path": str(json_path),
        "pattern_sequence_combo_report_path": str(md_path),
        "recommended_candidate": str(recommended_candidate["candidate_name"]),
        "disposition_recommendation": disposition,
    }


def build_event_image_pattern_adoption_policy(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("adoption policy currently supports rebound_onset only")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    target_dir = context["target_dir"]
    sequence_path = target_dir / "pattern_sequence_combo_rebound_onset.json"
    if not sequence_path.exists():
        build_event_image_pattern_sequence_combo(dataset_id=dataset_id, pattern="rebound_onset", output_root=target_dir)
    sequence_payload = read_json(sequence_path)
    recommended_sequence = sequence_payload["recommended_candidate"]
    recommended_conditions = list(recommended_sequence.get("conditions") or [])

    core_gate_candidates = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    variants: list[dict[str, Any]] = []
    for variant_name, threshold, use_sequence in (
        ("baseline_live", 0.50, False),
        ("soft_bonus_only", 0.45, False),
        ("soft_bonus_plus_best_sequence", 0.45, True),
    ):
        candidate_frame = core_gate_candidates.copy()
        if use_sequence:
            candidate_frame = candidate_frame.loc[_apply_combo_rule(candidate_frame, conditions=recommended_conditions)].copy().reset_index(drop=True)
        scored = _score_rebound_candidate_frame(
            candidate_frame=candidate_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold,
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=scored,
            dataset_id=str(dataset_id),
            source_artifacts={
                **context["source_artifacts"],
                "pattern_sequence_combo": str(sequence_path),
            },
            asof_iso=_ymd_int_to_iso(int(context["latest_asof"])) or "",
            source_disposition=str(context["candidate_row"].get("disposition") or "keep"),
            variant_name=variant_name,
        )
        bonus_rows = [row for row in scored.to_dict(orient="records") if bool(row.get("bonus_eligible"))]
        variants.append(
            {
                "variant_name": variant_name,
                "threshold_for_bonus": threshold,
                "use_sequence_candidate": bool(use_sequence),
                "sequence_candidate_name": str(recommended_sequence["candidate_name"]) if use_sequence else None,
                "tag_candidate_count": int(len(scored)),
                "bonus_candidate_count": int(scored["bonus_eligible"].sum()) if "bonus_eligible" in scored.columns else 0,
                "bonus_codes": [str(row["code"]) for row in bonus_rows],
                "bonus_score_map": {str(row["code"]): float(row["fit_score"]) for row in bonus_rows},
                **impact,
            }
        )

    recommended_variant = max(
        variants,
        key=lambda row: (
            1 if int(row["bonus_candidate_count"]) >= 1 else 0,
            1 if 1 <= int(row["entry_score_rank_changed_count"]) <= 5 else 0,
            1 if int(row["hybrid_score_changed_count"]) == 0 else 0,
            1 if int(row["setup_type_overridden_count"]) == 0 else 0,
            int(row["bonus_candidate_count"]),
            -abs(3 - int(row["entry_score_rank_changed_count"])),
            -float(row["threshold_for_bonus"]),
        ),
    )
    disposition = "hold"
    if (
        int(recommended_variant["bonus_candidate_count"]) >= 1
        and 1 <= int(recommended_variant["entry_score_rank_changed_count"]) <= 5
        and int(recommended_variant["hybrid_score_changed_count"]) == 0
        and int(recommended_variant["setup_type_overridden_count"]) == 0
        and str(recommended_variant["variant_name"]) in {"soft_bonus_only", "soft_bonus_plus_best_sequence"}
    ):
        disposition = "keep_strengthened"
    elif int(recommended_variant["bonus_candidate_count"]) >= 1:
        disposition = "keep"

    artifact = {
        "schema_version": PATTERN_ADOPTION_POLICY_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            **context["source_artifacts"],
            "pattern_sequence_combo": str(sequence_path),
        },
        "core_gate_contract": context["gate_rule"],
        "variant_results": variants,
        "recommended_variant": {
            "variant_name": str(recommended_variant["variant_name"]),
            "threshold_for_bonus": float(recommended_variant["threshold_for_bonus"]),
            "sequence_candidate_name": recommended_variant.get("sequence_candidate_name"),
        },
        "disposition_recommendation": disposition,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_adoption_policy_rebound_onset.json"
    md_path = target_dir / "pattern_adoption_policy_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Adoption Policy",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- recommended_variant: `{artifact['recommended_variant']['variant_name']}`",
            "",
            "## Variant Results",
            *[
                f"- `{row['variant_name']}`: tag=`{row['tag_candidate_count']}`, bonus=`{row['bonus_candidate_count']}`, changed=`{row['entry_score_rank_changed_count']}`"
                for row in variants
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_adoption_policy_path": str(json_path),
        "pattern_adoption_policy_report_path": str(md_path),
        "recommended_variant": str(recommended_variant["variant_name"]),
        "disposition_recommendation": disposition,
    }


def _playbook_variant_specs() -> dict[str, dict[str, float]]:
    return {
        "environment_heavy": {"environment_weight": 0.70, "setup_weight": 0.30, "score_threshold": 0.58},
        "setup_heavy": {"environment_weight": 0.30, "setup_weight": 0.70, "score_threshold": 0.58},
        "balanced_playbook": {"environment_weight": 0.50, "setup_weight": 0.50, "score_threshold": 0.55},
    }


def _build_rebound_playbook_thresholds(context: dict[str, Any], adoption_artifact: dict[str, Any]) -> dict[str, float | int | None]:
    tp_stats = context["tp_stats"]
    fp_stats = context["micro_feature_summary"]["false_positive_stats"]
    gate_rule = context["gate_rule"]
    pullback_p25 = _safe_float(tp_stats.get("distance_from_60d_high", {}).get("p25"))
    pullback_p75 = _safe_float(tp_stats.get("distance_from_60d_high", {}).get("p75"))
    higher_close_median = _median_stat(tp_stats, "higher_close_count_5")
    turnover_p25 = _safe_float(tp_stats.get("turnover20", {}).get("p25"))
    fp_realized_vol_p75 = _safe_float(fp_stats.get("realized_vol20", {}).get("p75"))
    return {
        "price_vs_ma120_env_min": _median_stat(tp_stats, "price_vs_ma120") or float(gate_rule["price_vs_ma120_min"]),
        "distance_from_60d_high_low": max(float(gate_rule["distance_from_60d_high_range"][0]), float(pullback_p25 or gate_rule["distance_from_60d_high_range"][0])),
        "distance_from_60d_high_high": min(float(gate_rule["distance_from_60d_high_range"][1]), float(pullback_p75 or gate_rule["distance_from_60d_high_range"][1])),
        "bull_streak_floor": int(max(1, math.floor(context["bull_streak_median"] or 1.0))),
        "narrow_range_floor": int(max(1, math.floor(context["narrow_range_median"] or 1.0))),
        "higher_close_floor": int(max(1, math.floor(higher_close_median or 1.0))),
        "lower_wick_median": float(context["lower_wick_median"] or 0.0),
        "lower_wick_p40": _approx_p40(tp_stats, "lower_wick_pct_last"),
        "realized_vol20_veto_max": float(fp_realized_vol_p75 or 0.05),
        "realized_vol20_env_max": float(fp_realized_vol_p75 or 0.05),
        "turnover20_veto_min": turnover_p25,
        "distance_from_60d_high_veto_max": -0.02,
        "threshold_for_bonus": float(adoption_artifact.get("fit_score_policy", {}).get("threshold_for_bonus") or 0.50),
    }


def _playbook_feature_catalog(thresholds: dict[str, float | int | None]) -> dict[str, list[dict[str, Any]]]:
    return {
        "environment_features": [
            {"feature": "price_vs_ma120", "kind": "min", "value": thresholds["price_vs_ma120_env_min"]},
            {
                "feature": "distance_from_60d_high",
                "kind": "range",
                "value": [thresholds["distance_from_60d_high_low"], thresholds["distance_from_60d_high_high"]],
            },
            {"feature": "ma60_slope_5", "kind": "positive", "value": 0.0},
            {"feature": "ma20_slope_5", "kind": "positive", "value": 0.0},
            {"feature": "realized_vol20", "kind": "bounded_above", "value": thresholds["realized_vol20_veto_max"]},
        ],
        "setup_features": [
            {"feature": "is_bull_last", "kind": "eq", "value": 1},
            {"feature": "lower_wick_pct_last", "kind": "min", "value": thresholds["lower_wick_median"]},
            {"feature": "bull_streak_3", "kind": "min", "value": thresholds["bull_streak_floor"]},
            {"feature": "narrow_range_count_5", "kind": "min", "value": thresholds["narrow_range_floor"]},
            {"feature": "higher_close_count_5", "kind": "min", "value": thresholds["higher_close_floor"]},
            {"feature": "sequence_pattern", "kind": "any", "value": ["tail_hold", "two_red_then_green", "narrow_range_then_bull", "higher_low_3"]},
        ],
        "veto_features": [
            {"feature": "extreme_volatility", "kind": "gt", "value": thresholds["realized_vol20_veto_max"]},
            {"feature": "thin_liquidity", "kind": "lt", "value": thresholds["turnover20_veto_min"]},
            {"feature": "upside_too_thin", "kind": "gt", "value": thresholds["distance_from_60d_high_veto_max"]},
            {"feature": "trend_shape_conflict", "kind": "compound", "value": ["ma20_slope_5<=0", "ma60_slope_5<=0"]},
        ],
    }


def _decorate_rebound_playbook_frame(
    frame: pd.DataFrame,
    *,
    thresholds: dict[str, float | int | None],
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    environment_scores: list[float] = []
    setup_scores: list[float] = []
    veto_flags_column: list[list[str]] = []
    veto_blocked: list[bool] = []
    sequence_flags_column: list[list[str]] = []
    for _, row in frame.iterrows():
        sequence_flags: list[str] = []
        if int(row.get("close_above_prev1_body_mid_flag") or 0) == 1 and int(row.get("engulfed_by_next_bear_flag") or 0) == 0:
            sequence_flags.append("tail_hold")
        if int(row.get("two_red_then_green_flag") or 0) == 1:
            sequence_flags.append("two_red_then_green")
        if int(row.get("narrow_range_then_bull_flag") or 0) == 1:
            sequence_flags.append("narrow_range_then_bull")
        if int(row.get("higher_low_3_flag") or 0) == 1:
            sequence_flags.append("higher_low_3")
        sequence_flags_column.append(sequence_flags)

        env_score = 0.0
        env_score += _score_component((_finite_float_or_none(row.get("price_vs_ma120")) or float("-inf")) >= float(thresholds["price_vs_ma120_env_min"] or 0.0), 0.35)
        env_score += _score_component(
            (_finite_float_or_none(row.get("distance_from_60d_high")) is not None)
            and float(thresholds["distance_from_60d_high_low"] or -1.0) <= float(row["distance_from_60d_high"]) <= float(thresholds["distance_from_60d_high_high"] or 0.0),
            0.25,
        )
        env_score += _score_component((_finite_float_or_none(row.get("ma60_slope_5")) or 0.0) > 0.0, 0.20)
        env_score += _score_component((_finite_float_or_none(row.get("ma20_slope_5")) or 0.0) > 0.0, 0.10)
        env_score += _score_component(
            (_finite_float_or_none(row.get("realized_vol20")) or float("inf"))
            <= float(thresholds.get("realized_vol20_env_max") or thresholds["realized_vol20_veto_max"] or 0.05),
            0.10,
        )
        environment_scores.append(float(max(0.0, min(1.0, env_score))))

        setup_score = 0.0
        setup_score += _score_component(int(row.get("is_bull_last") or 0) == 1, 0.15)
        setup_score += _score_component((_finite_float_or_none(row.get("lower_wick_pct_last")) or float("-inf")) >= float(thresholds["lower_wick_median"] or 0.0), 0.20)
        setup_score += _score_component((_finite_float_or_none(row.get("bull_streak_3")) or float("-inf")) >= int(thresholds["bull_streak_floor"] or 1), 0.10)
        setup_score += _score_component((_finite_float_or_none(row.get("narrow_range_count_5")) or float("-inf")) >= int(thresholds["narrow_range_floor"] or 1), 0.15)
        setup_score += _score_component((_finite_float_or_none(row.get("higher_close_count_5")) or float("-inf")) >= int(thresholds["higher_close_floor"] or 1), 0.15)
        setup_score += _score_component(bool(sequence_flags), 0.25)
        setup_scores.append(float(max(0.0, min(1.0, setup_score))))

        veto_flags: list[str] = []
        realized_vol20 = _finite_float_or_none(row.get("realized_vol20"))
        if realized_vol20 is not None and realized_vol20 > float(thresholds["realized_vol20_veto_max"] or 0.05):
            veto_flags.append("extreme_volatility")
        turnover20 = _finite_float_or_none(row.get("turnover20"))
        turnover_floor = _finite_float_or_none(thresholds.get("turnover20_veto_min"))
        if turnover_floor is not None and turnover20 is not None and turnover20 < turnover_floor:
            veto_flags.append("thin_liquidity")
        distance_from_high = _finite_float_or_none(row.get("distance_from_60d_high"))
        if distance_from_high is not None and distance_from_high > float(thresholds["distance_from_60d_high_veto_max"] or -0.02):
            veto_flags.append("upside_too_thin")
        if (_finite_float_or_none(row.get("ma20_slope_5")) or 0.0) <= 0.0 and (_finite_float_or_none(row.get("ma60_slope_5")) or 0.0) <= 0.0:
            veto_flags.append("trend_shape_conflict")
        veto_flags_column.append(veto_flags)
        veto_blocked.append(bool(veto_flags))

    scored = frame.copy()
    scored["environment_score"] = environment_scores
    scored["setup_score"] = setup_scores
    scored["sequence_flags"] = sequence_flags_column
    scored["veto_flags"] = veto_flags_column
    scored["veto_blocked"] = veto_blocked
    scored["veto_flag_count"] = [len(flags) for flags in veto_flags_column]
    return scored


def _apply_rebound_playbook_variant(
    frame: pd.DataFrame,
    *,
    variant_name: str,
    thresholds: dict[str, float | int | None],
) -> pd.DataFrame:
    scored = _decorate_rebound_playbook_frame(frame, thresholds=thresholds)
    if scored.empty:
        return scored
    spec = _playbook_variant_specs()[variant_name]
    scored = scored.copy()
    scored["playbook_variant"] = str(variant_name)
    scored["playbook_score"] = (
        scored["environment_score"].astype(float) * float(spec["environment_weight"])
        + scored["setup_score"].astype(float) * float(spec["setup_weight"])
    )
    selected = scored.loc[
        (scored["playbook_score"].astype(float) >= float(spec["score_threshold"]))
        & (~scored["veto_blocked"].astype(bool))
    ].copy().reset_index(drop=True)
    return selected


def _select_rebound_playbook_candidates(
    scored: pd.DataFrame,
    *,
    environment_weight: float,
    setup_weight: float,
    score_threshold: float,
    variant_name: str,
) -> pd.DataFrame:
    if scored.empty:
        return scored.copy()
    selected = scored.copy()
    selected["playbook_variant"] = str(variant_name)
    selected["playbook_score"] = (
        selected["environment_score"].astype(float) * float(environment_weight)
        + selected["setup_score"].astype(float) * float(setup_weight)
    )
    return selected.loc[
        (selected["playbook_score"].astype(float) >= float(score_threshold))
        & (~selected["veto_blocked"].astype(bool))
    ].copy().reset_index(drop=True)


def _apply_rebound_veto_ablation(
    frame: pd.DataFrame,
    *,
    disabled_rules: Collection[str] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    disabled = {str(value) for value in (disabled_rules or ())}
    ablated = frame.copy()
    effective_flags: list[list[str]] = []
    effective_blocked: list[bool] = []
    for flags in ablated["veto_flags"].tolist():
        kept = [str(flag) for flag in list(flags) if str(flag) not in disabled]
        effective_flags.append(kept)
        effective_blocked.append(bool(kept))
    ablated["effective_veto_flags"] = effective_flags
    ablated["effective_veto_blocked"] = effective_blocked
    return ablated.loc[~ablated["effective_veto_blocked"].astype(bool)].copy().reset_index(drop=True)


def _apply_rebound_thin_liquidity_threshold(
    frame: pd.DataFrame,
    *,
    thin_liquidity_turnover20_min: float | None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    adjusted = frame.copy()
    effective_flags: list[list[str]] = []
    effective_blocked: list[bool] = []
    turnover_floor = _finite_float_or_none(thin_liquidity_turnover20_min)
    for _, row in adjusted.iterrows():
        flags = [str(flag) for flag in list(row.get("veto_flags") or []) if str(flag) != "thin_liquidity"]
        turnover20 = _finite_float_or_none(row.get("turnover20"))
        if turnover_floor is not None and turnover20 is not None and turnover20 < turnover_floor:
            flags.append("thin_liquidity")
        effective_flags.append(flags)
        effective_blocked.append(bool(flags))
    adjusted["effective_veto_flags"] = effective_flags
    adjusted["effective_veto_blocked"] = effective_blocked
    return adjusted.loc[~adjusted["effective_veto_blocked"].astype(bool)].copy().reset_index(drop=True)


def _resolve_latest_report_artifact(*, report_dir_name: str, filename: str) -> Path:
    root = tradex_reports_root() / str(report_dir_name)
    candidates = [path for path in root.glob(f"*/{filename}") if path.is_file()]
    if not candidates:
        raise RuntimeError(f"missing report artifact: {root}\\*\\{filename}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _filter_stage_frame_by_codes(
    frame: pd.DataFrame,
    *,
    codes: Collection[str],
) -> pd.DataFrame:
    selected = {str(code) for code in codes if str(code).strip()}
    if not selected:
        return frame.head(0).copy()
    code_series = frame["code"].astype(str)
    return frame.loc[code_series.isin(selected)].copy().reset_index(drop=True)


def _summarize_selection_contract_stage(
    *,
    stage_name: str,
    stage_frame: pd.DataFrame,
    previous_frame: pd.DataFrame | None,
    core_gate_frame: pd.DataFrame,
) -> dict[str, Any]:
    def _sample_keys(frame: pd.DataFrame, *, label_id: int) -> set[str]:
        if frame.empty or "label_id" not in frame.columns:
            return set()
        scoped = frame.loc[frame["label_id"].fillna(0).astype(int) == int(label_id)].copy()
        if scoped.empty:
            return set()
        if "sample_id" in scoped.columns:
            return {str(value) for value in scoped["sample_id"].astype(str).tolist()}
        return {
            f"{int(row['as_of_date'])}:{str(row['code'])}"
            for row in scoped.loc[:, ["as_of_date", "code"]].to_dict(orient="records")
        }

    def _codes_from_keys(frame: pd.DataFrame, keys: set[str], *, label_id: int) -> list[str]:
        if not keys or frame.empty or "label_id" not in frame.columns:
            return []
        scoped = frame.loc[frame["label_id"].fillna(0).astype(int) == int(label_id)].copy()
        if scoped.empty:
            return []
        if "sample_id" in scoped.columns:
            mask = scoped["sample_id"].astype(str).isin(keys)
        else:
            mask = scoped.apply(lambda row: f"{int(row['as_of_date'])}:{str(row['code'])}" in keys, axis=1)
        return sorted({str(code) for code in scoped.loc[mask, "code"].astype(str).tolist()})

    sample_count = int(len(stage_frame))
    positive_mask = stage_frame["label_id"].fillna(0).astype(int) == 1 if "label_id" in stage_frame.columns else pd.Series(False, index=stage_frame.index)
    false_positive_mask = stage_frame["label_id"].fillna(0).astype(int) == 0 if "label_id" in stage_frame.columns else pd.Series(False, index=stage_frame.index)
    previous_positive_keys = _sample_keys(previous_frame, label_id=1) if previous_frame is not None else set()
    current_positive_keys = _sample_keys(stage_frame, label_id=1)
    core_positive_keys = _sample_keys(core_gate_frame, label_id=1)
    core_false_positive_keys = _sample_keys(core_gate_frame, label_id=0)
    current_positive_codes = sorted({str(code) for code in stage_frame.loc[positive_mask, "code"].astype(str).tolist()})
    current_false_positive_codes = sorted({str(code) for code in stage_frame.loc[false_positive_mask, "code"].astype(str).tolist()})
    metrics = _metric_delta(
        _subset_model_metrics(stage_frame, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
        _subset_model_metrics(stage_frame, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
    )
    return {
        "stage_name": str(stage_name),
        "sample_count": sample_count,
        "positive_count": int(positive_mask.sum()),
        "false_positive_count": int(false_positive_mask.sum()),
        "sample_attrition_vs_previous": None if previous_frame is None else int(len(previous_frame) - sample_count),
        "sample_attrition_vs_core_gate": int(len(core_gate_frame) - sample_count),
        "positive_retention_vs_core_gate": (
            None
            if len(core_positive_keys) <= 0
            else float(len(current_positive_keys & core_positive_keys) / max(1, len(core_positive_keys)))
        ),
        "false_positive_reduction_vs_core_gate": int(len(core_false_positive_keys) - len(_sample_keys(stage_frame, label_id=0))),
        "balanced_accuracy_delta_vs_numeric": metrics.get("balanced_accuracy_delta"),
        "monthly_top10_precision_up_delta_vs_numeric": metrics.get("monthly_top10_precision_up_delta"),
        "monthly_long_short_spread_delta_vs_numeric": metrics.get("monthly_long_short_spread_delta"),
        "winner_codes": current_positive_codes,
        "lost_winner_codes": _codes_from_keys(previous_frame if previous_frame is not None else stage_frame.head(0).copy(), previous_positive_keys - current_positive_keys, label_id=1),
        "saved_false_positive_codes": _codes_from_keys(core_gate_frame, core_false_positive_keys - _sample_keys(stage_frame, label_id=0), label_id=0),
    }


def _determine_selection_contract_leak(
    *,
    current_veto_stage: dict[str, Any],
    weak_stage: dict[str, Any],
    ranking_top20_stage: dict[str, Any],
    entry_stage: dict[str, Any],
    full_validation_summary: dict[str, Any],
) -> tuple[str, str]:
    current_lost = len(current_veto_stage.get("lost_winner_codes") or [])
    weak_lost = len(weak_stage.get("lost_winner_codes") or [])
    ranking_lost = len(ranking_top20_stage.get("lost_winner_codes") or [])
    entry_lost = len(entry_stage.get("lost_winner_codes") or [])
    weak_fp = int(weak_stage.get("false_positive_count") or 0)
    current_fp = int(current_veto_stage.get("false_positive_count") or 0)
    weak_spread = _finite_float_or_none(weak_stage.get("monthly_long_short_spread_delta_vs_numeric"))
    weak_top10 = _finite_float_or_none(weak_stage.get("monthly_top10_precision_up_delta_vs_numeric"))
    validation_decision = str(full_validation_summary.get("decision") or "")

    if weak_lost < current_lost and weak_fp > current_fp:
        return "thin_liquidity_policy", (
            "weak_1 restores winner retention versus current veto, but false positives return with it"
        )
    if current_lost > 0:
        return "veto_current", "current veto removes winner candidates before ranking can act on them"
    if entry_lost > 0:
        return "entry_qualification", "winner candidates survive top20 but drop at entryQualified"
    if ranking_lost > 0:
        return "ranking_selection", "winner candidates survive veto but disappear by ranking top20 selection"
    if validation_decision == "fix_holdings_before_policy_change":
        return "ranking_selection", "selection funnel is relatively intact; remaining dominant loss sits in ToreDex holdings"
    if weak_stage.get("sample_count", 0) > 0 and (weak_spread or 0.0) >= 0.0 and (weak_top10 or 0.0) >= 0.0:
        return "thin_liquidity_policy", "thin_liquidity weakening is the main surviving selection lever"
    return "ranking_selection", "selection leak is not isolated cleanly earlier; ranking stage remains the safest focal point"


def _summarize_rebound_playbook_result(
    *,
    variant_name: str,
    formal_variant_scored: pd.DataFrame,
    latest_variant_scored: pd.DataFrame,
    metric_deltas: dict[str, float | None],
    impact: dict[str, Any],
    thin_threshold: int,
) -> dict[str, Any]:
    sample_count = int(len(formal_variant_scored))
    spread_delta = _finite_float_or_none(metric_deltas.get("monthly_long_short_spread_delta"))
    balanced_accuracy_delta = _finite_float_or_none(metric_deltas.get("balanced_accuracy_delta"))
    false_positive_count = int(
        (
            (formal_variant_scored.get("image_pred_label", pd.Series(dtype=int)).fillna(0).astype(int) == 1)
            & (formal_variant_scored.get("label_id", pd.Series(dtype=int)).fillna(0).astype(int) == 0)
        ).sum()
    )
    false_positive_rate = (false_positive_count / sample_count) if sample_count > 0 else None
    if (
        sample_count >= max(5, thin_threshold)
        and spread_delta is not None
        and spread_delta > 0.0
        and balanced_accuracy_delta is not None
        and balanced_accuracy_delta > 0.0
        and (false_positive_rate is None or false_positive_rate <= 0.50)
    ):
        disposition = "keep"
    elif sample_count > 0 and (
        (spread_delta is not None and spread_delta >= 0.0)
        or (balanced_accuracy_delta is not None and balanced_accuracy_delta > 0.0)
    ):
        disposition = "keep"
    elif sample_count > 0:
        disposition = "hold"
    else:
        disposition = "drop_reconsider"
    return {
        "variant_name": str(variant_name),
        "sample_count": sample_count,
        "latest_sample_count": int(len(latest_variant_scored)),
        "bonus_candidate_count": int(formal_variant_scored["bonus_eligible"].sum()) if "bonus_eligible" in formal_variant_scored.columns else 0,
        "rank_changed_count": int(impact["entry_score_rank_changed_count"]),
        "balanced_accuracy_delta_vs_numeric": metric_deltas.get("balanced_accuracy_delta"),
        "roc_auc_delta_vs_numeric": metric_deltas.get("roc_auc_delta"),
        "monthly_top10_precision_up_delta_vs_numeric": metric_deltas.get("monthly_top10_precision_up_delta"),
        "monthly_long_short_spread_delta_vs_numeric": metric_deltas.get("monthly_long_short_spread_delta"),
        "false_positive_count": false_positive_count,
        "false_positive_rate": false_positive_rate,
        "disposition": disposition,
    }


def _build_rebound_playbook_relax_specs(
    *,
    thresholds: dict[str, float | int | None],
    context: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    gate_rule = context["gate_rule"]
    mild_price_floor = min(
        float(thresholds["price_vs_ma120_env_min"] or 0.0),
        float(gate_rule["price_vs_ma120_min"]),
    )
    setup_lower_wick = _finite_float_or_none(thresholds.get("lower_wick_p40"))
    setup_lower_wick = float(setup_lower_wick if setup_lower_wick is not None else thresholds["lower_wick_median"] or 0.0)
    return {
        "environment_relax": {
            "axis": "environment",
            "description": "price_vs_ma120 / distance_from_60d_high / realized_vol20 を少し広げ、setup と veto は固定する。",
            "thresholds": {
                **thresholds,
                "price_vs_ma120_env_min": mild_price_floor,
                "distance_from_60d_high_low": float(gate_rule["distance_from_60d_high_range"][0]),
                "distance_from_60d_high_high": float(gate_rule["distance_from_60d_high_range"][1]),
                "realized_vol20_env_max": float(thresholds["realized_vol20_veto_max"] or 0.05) * 1.20,
            },
        },
        "setup_relax": {
            "axis": "setup",
            "description": "lower_wick / streak / narrow range / higher close の setup 側だけを緩め、environment と veto は固定する。",
            "thresholds": {
                **thresholds,
                "lower_wick_median": setup_lower_wick,
                "bull_streak_floor": int(max(0, int(thresholds["bull_streak_floor"] or 1) - 1)),
                "narrow_range_floor": int(max(0, int(thresholds["narrow_range_floor"] or 1) - 1)),
                "higher_close_floor": int(max(0, int(thresholds["higher_close_floor"] or 1) - 1)),
            },
        },
    }


def build_event_image_pattern_playbook(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("playbook analysis currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_library_candidates": dataset_dir / "pattern_library_candidates.json",
        "pattern_micro_features_rebound_onset": dataset_dir / "pattern_micro_features_rebound_onset.json",
        "pattern_gating_rule_rebound_onset_vs_uptrend": dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json",
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"playbook analysis requires existing artifacts: {missing}")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    candidate_row = context["candidate_row"]
    thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)
    feature_catalog = _playbook_feature_catalog(thresholds)

    formal_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_features["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if formal_core_gate.empty:
        raise RuntimeError("playbook analysis produced no formal core-gate candidates")
    latest_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if latest_core_gate.empty:
        raise RuntimeError("playbook analysis produced no latest core-gate candidates")

    baseline_metrics = _metric_delta(
        _subset_model_metrics(formal_core_gate, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
        _subset_model_metrics(formal_core_gate, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
    )
    decorated_formal = _decorate_rebound_playbook_frame(formal_core_gate, thresholds=thresholds)
    environment_only_frame = decorated_formal.loc[
        (decorated_formal["environment_score"].astype(float) >= 0.60) & (~decorated_formal["veto_blocked"].astype(bool))
    ].copy().reset_index(drop=True)
    setup_only_frame = decorated_formal.loc[
        (decorated_formal["setup_score"].astype(float) >= 0.60) & (~decorated_formal["veto_blocked"].astype(bool))
    ].copy().reset_index(drop=True)
    environment_only_metrics = _metric_delta(
        _subset_model_metrics(environment_only_frame, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
        _subset_model_metrics(environment_only_frame, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
    )
    setup_only_metrics = _metric_delta(
        _subset_model_metrics(setup_only_frame, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
        _subset_model_metrics(setup_only_frame, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
    )
    false_positive_mask = (
        (decorated_formal["image_pred_label"].astype(int) == 1)
        & (decorated_formal["label_id"].astype(int) == 0)
    )
    veto_saved_false_positive_count = int(
        decorated_formal.loc[false_positive_mask, "veto_blocked"].astype(bool).sum()
    )

    variant_results: list[dict[str, Any]] = []
    for variant_name, spec in _playbook_variant_specs().items():
        formal_variant = _apply_rebound_playbook_variant(
            formal_core_gate,
            variant_name=variant_name,
            thresholds=thresholds,
        )
        formal_variant_scored = _score_rebound_candidate_frame(
            candidate_frame=formal_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=float(thresholds["threshold_for_bonus"] or 0.50),
        )
        latest_variant = _apply_rebound_playbook_variant(
            latest_core_gate,
            variant_name=variant_name,
            thresholds=thresholds,
        )
        latest_variant_scored = _score_rebound_candidate_frame(
            candidate_frame=latest_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=float(thresholds["threshold_for_bonus"] or 0.50),
        )
        metric_deltas = _metric_delta(
            _subset_model_metrics(formal_variant_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(formal_variant_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=latest_variant_scored,
            dataset_id=str(dataset_id),
            source_artifacts={
                **context["source_artifacts"],
                "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"]),
            },
            asof_iso=_ymd_int_to_iso(int(context["latest_asof"])) or "",
            source_disposition=str(adoption_artifact.get("source_disposition") or candidate_row.get("disposition") or "keep"),
            variant_name=variant_name,
        )
        thin_threshold = max(3, math.ceil(len(formal_core_gate) * 0.5))
        sample_count = int(len(formal_variant_scored))
        spread_delta = _finite_float_or_none(metric_deltas.get("monthly_long_short_spread_delta"))
        balanced_accuracy_delta = _finite_float_or_none(metric_deltas.get("balanced_accuracy_delta"))
        if (
            spread_delta is not None
            and spread_delta > 0.0
            and balanced_accuracy_delta is not None
            and balanced_accuracy_delta > 0.0
            and sample_count >= thin_threshold
        ):
            disposition = "keep_strengthened"
        elif sample_count >= thin_threshold and spread_delta is not None and spread_delta >= 0.0:
            disposition = "keep"
        elif sample_count > 0 and any(
            (_finite_float_or_none(metric_deltas.get(key)) or 0.0) > 0.0
            for key in ("balanced_accuracy_delta", "roc_auc_delta", "monthly_top10_precision_up_delta", "monthly_long_short_spread_delta")
        ):
            disposition = "hold"
        else:
            disposition = "drop_reconsider"
        variant_results.append(
            {
                "variant_name": str(variant_name),
                "weights": {"environment": float(spec["environment_weight"]), "setup": float(spec["setup_weight"])},
                "score_threshold": float(spec["score_threshold"]),
                "sample_count": sample_count,
                "bonus_candidate_count": int(formal_variant_scored["bonus_eligible"].sum()) if "bonus_eligible" in formal_variant_scored.columns else 0,
                "rank_changed_count": int(impact["entry_score_rank_changed_count"]),
                "balanced_accuracy_delta_vs_numeric": metric_deltas.get("balanced_accuracy_delta"),
                "roc_auc_delta_vs_numeric": metric_deltas.get("roc_auc_delta"),
                "monthly_top10_precision_up_delta_vs_numeric": metric_deltas.get("monthly_top10_precision_up_delta"),
                "monthly_long_short_spread_delta_vs_numeric": metric_deltas.get("monthly_long_short_spread_delta"),
                "environment_only_gain": (
                    (_finite_float_or_none(environment_only_metrics.get("monthly_long_short_spread_delta")) or 0.0)
                    - (_finite_float_or_none(baseline_metrics.get("monthly_long_short_spread_delta")) or 0.0)
                ),
                "setup_only_gain": (
                    (_finite_float_or_none(setup_only_metrics.get("monthly_long_short_spread_delta")) or 0.0)
                    - (_finite_float_or_none(baseline_metrics.get("monthly_long_short_spread_delta")) or 0.0)
                ),
                "veto_saved_false_positive_count": veto_saved_false_positive_count,
                "disposition": disposition,
            }
        )

    recommended_variant = max(
        variant_results,
        key=lambda row: (
            1 if str(row["disposition"]) == "keep_strengthened" else 0,
            1 if str(row["disposition"]) == "keep" else 0,
            float(row["monthly_long_short_spread_delta_vs_numeric"] or float("-inf")),
            float(row["balanced_accuracy_delta_vs_numeric"] or float("-inf")),
            int(row["sample_count"]),
        ),
    )
    overall_disposition = "hold"
    if any(str(row["disposition"]) == "keep_strengthened" for row in variant_results):
        overall_disposition = "keep_strengthened"
    elif any(str(row["disposition"]) == "keep" for row in variant_results):
        overall_disposition = "keep"
    elif all(str(row["disposition"]) == "drop_reconsider" for row in variant_results):
        overall_disposition = "drop_reconsider"

    feature_effects = {
        "baseline_core_gate_metric_delta_vs_numeric": baseline_metrics,
        "environment_only_gain": {
            "sample_count": int(len(environment_only_frame)),
            "monthly_long_short_spread_delta_gain_vs_core_gate": (
                (_finite_float_or_none(environment_only_metrics.get("monthly_long_short_spread_delta")) or 0.0)
                - (_finite_float_or_none(baseline_metrics.get("monthly_long_short_spread_delta")) or 0.0)
            ),
        },
        "setup_only_gain": {
            "sample_count": int(len(setup_only_frame)),
            "monthly_long_short_spread_delta_gain_vs_core_gate": (
                (_finite_float_or_none(setup_only_metrics.get("monthly_long_short_spread_delta")) or 0.0)
                - (_finite_float_or_none(baseline_metrics.get("monthly_long_short_spread_delta")) or 0.0)
            ),
        },
        "veto_saved_false_positive_count": int(veto_saved_false_positive_count),
    }
    artifact = {
        "schema_version": PATTERN_PLAYBOOK_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {key: str(path) for key, path in required_paths.items()},
        "pattern_id": "rebound_onset_playbook_v1",
        **feature_catalog,
        "environment_score_definition": {
            "price_vs_ma120": 0.35,
            "distance_from_60d_high": 0.25,
            "ma60_slope_5": 0.20,
            "ma20_slope_5": 0.10,
            "realized_vol20_band": 0.10,
        },
        "setup_score_definition": {
            "is_bull_last": 0.15,
            "lower_wick_pct_last": 0.20,
            "bull_streak_3": 0.10,
            "narrow_range_count_5": 0.15,
            "higher_close_count_5": 0.15,
            "sequence_pattern": 0.25,
        },
        "veto_rule_definition": {
            "extreme_volatility": {"realized_vol20_gt": thresholds["realized_vol20_veto_max"]},
            "thin_liquidity": {"turnover20_lt": thresholds["turnover20_veto_min"]},
            "upside_too_thin": {"distance_from_60d_high_gt": thresholds["distance_from_60d_high_veto_max"]},
            "trend_shape_conflict": {"ma20_slope_5_lte": 0.0, "ma60_slope_5_lte": 0.0},
        },
        "feature_effects": feature_effects,
        "variant_results": variant_results,
        "recommended_playbook_variant": {
            "variant_name": str(recommended_variant["variant_name"]),
            "weights": dict(recommended_variant["weights"]),
            "score_threshold": float(recommended_variant["score_threshold"]),
        },
        "disposition_recommendation": str(overall_disposition),
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_playbook_rebound_onset.json"
    md_path = target_dir / "pattern_playbook_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Playbook",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- pattern_id: `rebound_onset_playbook_v1`",
            f"- recommended_playbook_variant: `{artifact['recommended_playbook_variant']['variant_name']}`",
            f"- disposition: `{artifact['disposition_recommendation']}`",
            "",
            "## Current State",
            f"- formal compare scope: `common eligible subset on test split only`",
            f"- current_core_gate_count: `{int(len(formal_core_gate))}`",
            "",
            "## What Changed",
            "- split the discretionary rebound_onset playbook into environment / setup / veto layers and evaluated three deterministic variants",
            "",
            "## Evidence",
            *[
                f"- `{row['variant_name']}`: sample=`{row['sample_count']}`, spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`, bonus=`{row['bonus_candidate_count']}`, rank_changed=`{row['rank_changed_count']}`"
                for row in variant_results
            ],
            f"- environment_only_gain: `{feature_effects['environment_only_gain']}`",
            f"- setup_only_gain: `{feature_effects['setup_only_gain']}`",
            f"- veto_saved_false_positive_count: `{feature_effects['veto_saved_false_positive_count']}`",
            "",
            "## Decision",
            f"- recommended_playbook_variant: `{artifact['recommended_playbook_variant']['variant_name']}`",
            f"- disposition: `{artifact['disposition_recommendation']}`",
            "",
            "## Remaining Risks",
            "- playbook scoring still runs on the restricted-universe test common subset and preview ranking impact only",
            "- veto rules are intentionally conservative and may still be incomplete for event-driven exclusions",
            "",
            "## Next Single Step",
            "- decide whether the playbook should remain analysis-only or be promoted into a bridge-facing candidate after another controlled validation round",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_playbook_path": str(json_path),
        "pattern_playbook_report_path": str(md_path),
        "recommended_playbook_variant": str(recommended_variant["variant_name"]),
        "disposition_recommendation": str(overall_disposition),
    }


def build_event_image_pattern_playbook_relax_compare(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("playbook relax compare currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_library_candidates": dataset_dir / "pattern_library_candidates.json",
        "pattern_micro_features_rebound_onset": dataset_dir / "pattern_micro_features_rebound_onset.json",
        "pattern_gating_rule_rebound_onset_vs_uptrend": dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json",
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "pattern_playbook_rebound_onset": dataset_dir / "pattern_playbook_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"playbook relax compare requires existing artifacts: {missing}")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    playbook_artifact = read_json(required_paths["pattern_playbook_rebound_onset"])

    base_thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)
    relax_specs = _build_rebound_playbook_relax_specs(
        thresholds=base_thresholds,
        context=context,
    )
    formal_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_features["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    latest_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if formal_core_gate.empty or latest_core_gate.empty:
        raise RuntimeError("playbook relax compare requires non-empty core-gate candidates")

    balanced_spec = _playbook_variant_specs()["balanced_playbook"]
    thin_threshold = 1
    candidate_row = context["candidate_row"]
    source_disposition = str(adoption_artifact.get("source_disposition") or candidate_row.get("disposition") or "keep")
    asof_iso = _ymd_int_to_iso(int(context["latest_asof"])) or ""

    baseline_formal = _apply_rebound_playbook_variant(
        formal_core_gate,
        variant_name="balanced_playbook",
        thresholds=base_thresholds,
    )
    baseline_formal_scored = _score_rebound_candidate_frame(
        candidate_frame=baseline_formal,
        lower_wick_median=context["lower_wick_median"],
        bull_streak_median=context["bull_streak_median"],
        narrow_range_median=context["narrow_range_median"],
        threshold_for_bonus=float(base_thresholds["threshold_for_bonus"] or 0.50),
    )
    baseline_latest = _apply_rebound_playbook_variant(
        latest_core_gate,
        variant_name="balanced_playbook",
        thresholds=base_thresholds,
    )
    baseline_latest_scored = _score_rebound_candidate_frame(
        candidate_frame=baseline_latest,
        lower_wick_median=context["lower_wick_median"],
        bull_streak_median=context["bull_streak_median"],
        narrow_range_median=context["narrow_range_median"],
        threshold_for_bonus=float(base_thresholds["threshold_for_bonus"] or 0.50),
    )
    baseline_metrics = _metric_delta(
        _subset_model_metrics(baseline_formal_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
        _subset_model_metrics(baseline_formal_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
    )
    baseline_impact = _evaluate_rebound_ranking_impact(
        latest_frame=context["latest_frame"],
        candidate_frame=baseline_latest_scored,
        dataset_id=str(dataset_id),
        source_artifacts={**context["source_artifacts"], "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"])},
        asof_iso=asof_iso,
        source_disposition=source_disposition,
        variant_name="balanced_playbook",
    )
    baseline_result = _summarize_rebound_playbook_result(
        variant_name="balanced_playbook",
        formal_variant_scored=baseline_formal_scored,
        latest_variant_scored=baseline_latest_scored,
        metric_deltas=baseline_metrics,
        impact=baseline_impact,
        thin_threshold=thin_threshold,
    )
    baseline_result["weights"] = {"environment": float(balanced_spec["environment_weight"]), "setup": float(balanced_spec["setup_weight"])}
    baseline_result["score_threshold"] = float(balanced_spec["score_threshold"])

    relax_results: list[dict[str, Any]] = []
    for variant_name, variant_spec in relax_specs.items():
        formal_variant = _apply_rebound_playbook_variant(
            formal_core_gate,
            variant_name="balanced_playbook",
            thresholds=dict(variant_spec["thresholds"]),
        )
        formal_variant_scored = _score_rebound_candidate_frame(
            candidate_frame=formal_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=float(base_thresholds["threshold_for_bonus"] or 0.50),
        )
        latest_variant = _apply_rebound_playbook_variant(
            latest_core_gate,
            variant_name="balanced_playbook",
            thresholds=dict(variant_spec["thresholds"]),
        )
        latest_variant_scored = _score_rebound_candidate_frame(
            candidate_frame=latest_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=float(base_thresholds["threshold_for_bonus"] or 0.50),
        )
        metric_deltas = _metric_delta(
            _subset_model_metrics(formal_variant_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(formal_variant_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=latest_variant_scored,
            dataset_id=str(dataset_id),
            source_artifacts={**context["source_artifacts"], "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"])},
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            variant_name=variant_name,
        )
        row = _summarize_rebound_playbook_result(
            variant_name=variant_name,
            formal_variant_scored=formal_variant_scored,
            latest_variant_scored=latest_variant_scored,
            metric_deltas=metric_deltas,
            impact=impact,
            thin_threshold=thin_threshold,
        )
        row["axis"] = str(variant_spec["axis"])
        row["description"] = str(variant_spec["description"])
        row["score_threshold"] = float(balanced_spec["score_threshold"])
        row["weights"] = {"environment": float(balanced_spec["environment_weight"]), "setup": float(balanced_spec["setup_weight"])}
        relax_results.append(row)

    recommended_result = max(
        relax_results,
        key=lambda row: (
            1 if int(row["sample_count"]) > 0 else 0,
            1 if str(row["disposition"]) == "keep_strengthened" else 0,
            1 if str(row["disposition"]) == "keep" else 0,
            float(row["monthly_long_short_spread_delta_vs_numeric"] or float("-inf")),
            float(row["balanced_accuracy_delta_vs_numeric"] or float("-inf")),
        ),
    )
    if any(str(row["disposition"]) == "keep_strengthened" for row in relax_results):
        overall_disposition = "keep_strengthened"
    elif any(str(row["disposition"]) == "keep" for row in relax_results):
        overall_disposition = "keep"
    elif any(int(row["sample_count"]) > 0 for row in relax_results):
        overall_disposition = "hold"
    else:
        overall_disposition = "drop_reconsider"

    artifact = {
        "schema_version": PATTERN_PLAYBOOK_RELAX_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {key: str(path) for key, path in required_paths.items()},
        "pattern_id": "rebound_onset_playbook_relax_compare_v1",
        "veto_features": list(playbook_artifact.get("veto_features", [])),
        "baseline_playbook_variant": {
            "variant_name": "balanced_playbook",
            "weights": {"environment": float(balanced_spec["environment_weight"]), "setup": float(balanced_spec["setup_weight"])},
            "score_threshold": float(balanced_spec["score_threshold"]),
        },
        "baseline_result": baseline_result,
        "relax_results": relax_results,
        "recommended_relax_axis": {
            "variant_name": str(recommended_result["variant_name"]),
            "axis": str(recommended_result["axis"]),
        },
        "disposition_recommendation": str(overall_disposition),
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_playbook_rebound_onset_relax_compare.json"
    md_path = target_dir / "pattern_playbook_rebound_onset_relax_compare.md"
    json_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_lines = [
        "# Rebound Onset Playbook Relax Compare",
        "",
        f"- dataset_id: `{dataset_id}`",
        f"- formal_compare_scope: `{artifact['formal_compare_scope']}`",
        f"- baseline_result: sample `{baseline_result['sample_count']}` / spread delta `{baseline_result['monthly_long_short_spread_delta_vs_numeric']}`",
        f"- recommended_relax_axis: `{artifact['recommended_relax_axis']['variant_name']}`",
        f"- disposition_recommendation: `{overall_disposition}`",
        "",
        "## Relax Results",
    ]
    for row in relax_results:
        summary_lines.extend(
            [
                f"- `{row['variant_name']}` ({row['axis']})",
                f"  - sample_count: `{row['sample_count']}`",
                f"  - bonus_candidate_count: `{row['bonus_candidate_count']}`",
                f"  - rank_changed_count: `{row['rank_changed_count']}`",
                f"  - balanced_accuracy_delta_vs_numeric: `{row['balanced_accuracy_delta_vs_numeric']}`",
                f"  - monthly_long_short_spread_delta_vs_numeric: `{row['monthly_long_short_spread_delta_vs_numeric']}`",
                f"  - disposition: `{row['disposition']}`",
            ]
        )
    md_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_playbook_relax_compare_path": str(json_path),
        "pattern_playbook_relax_compare_report_path": str(md_path),
        "recommended_relax_axis": str(artifact["recommended_relax_axis"]["variant_name"]),
        "disposition_recommendation": str(overall_disposition),
    }


def build_event_image_pattern_playbook_threshold_compare(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("playbook threshold compare currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_library_candidates": dataset_dir / "pattern_library_candidates.json",
        "pattern_micro_features_rebound_onset": dataset_dir / "pattern_micro_features_rebound_onset.json",
        "pattern_gating_rule_rebound_onset_vs_uptrend": dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json",
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "pattern_playbook_rebound_onset": dataset_dir / "pattern_playbook_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"playbook threshold compare requires existing artifacts: {missing}")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)
    formal_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_features["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    latest_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if formal_core_gate.empty or latest_core_gate.empty:
        raise RuntimeError("playbook threshold compare requires non-empty core-gate candidates")

    decorated_formal = _decorate_rebound_playbook_frame(formal_core_gate, thresholds=thresholds)
    decorated_latest = _decorate_rebound_playbook_frame(latest_core_gate, thresholds=thresholds)
    balanced_spec = _playbook_variant_specs()["balanced_playbook"]
    threshold_variants = [
        {"variant_name": "current_balanced", "score_threshold": float(balanced_spec["score_threshold"])},
        {"variant_name": "cutoff_055", "score_threshold": 0.55},
        {"variant_name": "cutoff_050", "score_threshold": 0.50},
        {"variant_name": "cutoff_045", "score_threshold": 0.45},
        {"variant_name": "cutoff_040", "score_threshold": 0.40},
    ]
    thin_threshold = 1
    candidate_row = context["candidate_row"]
    source_disposition = str(adoption_artifact.get("source_disposition") or candidate_row.get("disposition") or "keep")
    asof_iso = _ymd_int_to_iso(int(context["latest_asof"])) or ""

    threshold_results: list[dict[str, Any]] = []
    for spec in threshold_variants:
        formal_variant = _select_rebound_playbook_candidates(
            decorated_formal,
            environment_weight=float(balanced_spec["environment_weight"]),
            setup_weight=float(balanced_spec["setup_weight"]),
            score_threshold=float(spec["score_threshold"]),
            variant_name=str(spec["variant_name"]),
        )
        formal_variant_scored = _score_rebound_candidate_frame(
            candidate_frame=formal_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=float(thresholds["threshold_for_bonus"] or 0.50),
        )
        latest_variant = _select_rebound_playbook_candidates(
            decorated_latest,
            environment_weight=float(balanced_spec["environment_weight"]),
            setup_weight=float(balanced_spec["setup_weight"]),
            score_threshold=float(spec["score_threshold"]),
            variant_name=str(spec["variant_name"]),
        )
        latest_variant_scored = _score_rebound_candidate_frame(
            candidate_frame=latest_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=float(thresholds["threshold_for_bonus"] or 0.50),
        )
        metric_deltas = _metric_delta(
            _subset_model_metrics(formal_variant_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(formal_variant_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=latest_variant_scored,
            dataset_id=str(dataset_id),
            source_artifacts={**context["source_artifacts"], "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"])},
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            variant_name=str(spec["variant_name"]),
        )
        row = _summarize_rebound_playbook_result(
            variant_name=str(spec["variant_name"]),
            formal_variant_scored=formal_variant_scored,
            latest_variant_scored=latest_variant_scored,
            metric_deltas=metric_deltas,
            impact=impact,
            thin_threshold=thin_threshold,
        )
        row["score_threshold"] = float(spec["score_threshold"])
        threshold_results.append(row)

    recommended_result = max(
        threshold_results,
        key=lambda row: (
            1 if str(row["disposition"]) == "keep" else 0,
            1 if str(row["disposition"]) == "hold" else 0,
            1 if int(row["sample_count"]) > 0 else 0,
            float(row["monthly_long_short_spread_delta_vs_numeric"] or float("-inf")),
            float(row["balanced_accuracy_delta_vs_numeric"] or float("-inf")),
            -float(row["false_positive_rate"] if row["false_positive_rate"] is not None else 1.0),
        ),
    )
    if any(str(row["disposition"]) == "keep" for row in threshold_results):
        overall_disposition = "keep"
    elif any(str(row["disposition"]) == "hold" for row in threshold_results):
        overall_disposition = "hold"
    else:
        overall_disposition = "drop_reconsider"

    artifact = {
        "schema_version": PATTERN_PLAYBOOK_THRESHOLD_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {key: str(path) for key, path in required_paths.items()},
        "pattern_id": "rebound_onset_playbook_threshold_compare_v1",
        "fixed_playbook_variant": {
            "variant_name": "balanced_playbook",
            "weights": {"environment": float(balanced_spec["environment_weight"]), "setup": float(balanced_spec["setup_weight"])},
            "veto_fixed": True,
            "feature_definition_fixed": True,
        },
        "threshold_results": threshold_results,
        "recommended_threshold_variant": {
            "variant_name": str(recommended_result["variant_name"]),
            "score_threshold": float(recommended_result["score_threshold"]),
        },
        "disposition_recommendation": str(overall_disposition),
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_playbook_rebound_onset_threshold_compare.json"
    md_path = target_dir / "pattern_playbook_rebound_onset_threshold_compare.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# Rebound Onset Playbook Threshold Compare",
            "",
            f"- dataset: `{dataset_id}`",
            f"- formal compare scope: `{artifact['formal_compare_scope']}`",
            f"- recommended_threshold_variant: `{artifact['recommended_threshold_variant']['variant_name']}`",
            f"- disposition: `{artifact['disposition_recommendation']}`",
            "",
            "## Threshold Results",
            *[
                (
                    f"- `{row['variant_name']}`: cutoff=`{row['score_threshold']}`, sample=`{row['sample_count']}`, "
                    f"spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`, "
                    f"ba_delta=`{row['balanced_accuracy_delta_vs_numeric']}`, "
                    f"false_positive_count=`{row['false_positive_count']}`, "
                    f"bonus=`{row['bonus_candidate_count']}`, disposition=`{row['disposition']}`"
                )
                for row in threshold_results
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_playbook_threshold_compare_path": str(json_path),
        "pattern_playbook_threshold_compare_report_path": str(md_path),
        "recommended_threshold_variant": str(artifact["recommended_threshold_variant"]["variant_name"]),
        "disposition_recommendation": str(overall_disposition),
    }


def build_event_image_pattern_veto_compare(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("veto compare currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_library_candidates": dataset_dir / "pattern_library_candidates.json",
        "pattern_micro_features_rebound_onset": dataset_dir / "pattern_micro_features_rebound_onset.json",
        "pattern_gating_rule_rebound_onset_vs_uptrend": dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json",
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "pattern_playbook_rebound_onset": dataset_dir / "pattern_playbook_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"veto compare requires existing artifacts: {missing}")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    playbook_artifact = read_json(required_paths["pattern_playbook_rebound_onset"])
    thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)

    formal_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_features["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    latest_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if formal_core_gate.empty or latest_core_gate.empty:
        raise RuntimeError("veto compare requires non-empty core-gate candidates")

    decorated_formal = _decorate_rebound_playbook_frame(formal_core_gate, thresholds=thresholds)
    decorated_latest = _decorate_rebound_playbook_frame(latest_core_gate, thresholds=thresholds)
    formal_veto = decorated_formal.loc[~decorated_formal["veto_blocked"].astype(bool)].copy().reset_index(drop=True)
    latest_veto = decorated_latest.loc[~decorated_latest["veto_blocked"].astype(bool)].copy().reset_index(drop=True)

    threshold_for_bonus = float(adoption_artifact.get("fit_score_policy", {}).get("threshold_for_bonus") or thresholds["threshold_for_bonus"] or 0.50)
    source_disposition = str(adoption_artifact.get("source_disposition") or context["candidate_row"].get("disposition") or "keep")
    asof_iso = _ymd_int_to_iso(int(context["latest_asof"])) or ""

    comparisons: list[dict[str, Any]] = []
    for variant_name, formal_frame, latest_frame in (
        ("core_gate_only", formal_core_gate, latest_core_gate),
        ("core_gate_plus_veto", formal_veto, latest_veto),
    ):
        formal_scored = _score_rebound_candidate_frame(
            candidate_frame=formal_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        latest_scored = _score_rebound_candidate_frame(
            candidate_frame=latest_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        metric_deltas = _metric_delta(
            _subset_model_metrics(formal_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(formal_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=latest_scored,
            dataset_id=str(dataset_id),
            source_artifacts={**context["source_artifacts"], "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"])},
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            variant_name=variant_name,
        )
        false_positive_count = int(
            (
                (formal_scored["image_pred_label"].fillna(0).astype(int) == 1)
                & (formal_scored["label_id"].fillna(0).astype(int) == 0)
            ).sum()
        )
        comparisons.append(
            {
                "variant_name": variant_name,
                "sample_count": int(len(formal_scored)),
                "bonus_candidate_count": int(formal_scored["bonus_eligible"].sum()) if "bonus_eligible" in formal_scored.columns else 0,
                "rank_changed_count": int(impact["entry_score_rank_changed_count"]),
                "false_positive_count": false_positive_count,
                "balanced_accuracy_delta_vs_numeric": metric_deltas.get("balanced_accuracy_delta"),
                "roc_auc_delta_vs_numeric": metric_deltas.get("roc_auc_delta"),
                "monthly_top10_precision_up_delta_vs_numeric": metric_deltas.get("monthly_top10_precision_up_delta"),
                "monthly_long_short_spread_delta_vs_numeric": metric_deltas.get("monthly_long_short_spread_delta"),
            }
        )

    core_gate_row = next(row for row in comparisons if row["variant_name"] == "core_gate_only")
    veto_row = next(row for row in comparisons if row["variant_name"] == "core_gate_plus_veto")
    false_positive_reduced_count = int(core_gate_row["false_positive_count"]) - int(veto_row["false_positive_count"])
    sample_count_delta = int(veto_row["sample_count"]) - int(core_gate_row["sample_count"])
    spread_core = _finite_float_or_none(core_gate_row["monthly_long_short_spread_delta_vs_numeric"])
    spread_veto = _finite_float_or_none(veto_row["monthly_long_short_spread_delta_vs_numeric"])
    top10_core = _finite_float_or_none(core_gate_row["monthly_top10_precision_up_delta_vs_numeric"])
    top10_veto = _finite_float_or_none(veto_row["monthly_top10_precision_up_delta_vs_numeric"])
    veto_sample_count = int(veto_row["sample_count"])
    if (
        false_positive_reduced_count > 0
        and veto_sample_count > 0
        and (spread_veto is None or spread_core is None or spread_veto >= spread_core)
        and (top10_veto is None or top10_core is None or top10_veto >= top10_core)
    ):
        disposition = "keep"
        recommended_policy = "core_gate_plus_veto"
    elif false_positive_reduced_count > 0:
        disposition = "hold"
        recommended_policy = "analysis_only"
    else:
        disposition = "drop_reconsider"
        recommended_policy = "core_gate_only"

    artifact = {
        "schema_version": PATTERN_VETO_COMPARE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {key: str(path) for key, path in required_paths.items()},
        "pattern_id": "rebound_onset_veto_compare_v1",
        "veto_features": list(playbook_artifact.get("veto_features", [])),
        "veto_rule_definition": dict(playbook_artifact.get("veto_rule_definition", {})),
        "compare_results": comparisons,
        "false_positive_reduced_count": false_positive_reduced_count,
        "sample_count_delta_vs_core_gate": sample_count_delta,
        "recommended_policy": recommended_policy,
        "disposition_recommendation": disposition,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_veto_rebound_onset_compare.json"
    md_path = target_dir / "pattern_veto_rebound_onset_compare.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# Rebound Onset Veto Compare",
            "",
            f"- dataset: `{dataset_id}`",
            f"- formal compare scope: `{artifact['formal_compare_scope']}`",
            f"- false_positive_reduced_count: `{false_positive_reduced_count}`",
            f"- sample_count_delta_vs_core_gate: `{sample_count_delta}`",
            f"- recommended_policy: `{recommended_policy}`",
            f"- disposition: `{disposition}`",
            "",
            "## Compare Results",
            *[
                (
                    f"- `{row['variant_name']}`: sample=`{row['sample_count']}`, "
                    f"false_positive_count=`{row['false_positive_count']}`, "
                    f"top10_delta=`{row['monthly_top10_precision_up_delta_vs_numeric']}`, "
                    f"spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`, "
                    f"bonus=`{row['bonus_candidate_count']}`, rank_changed=`{row['rank_changed_count']}`"
                )
                for row in comparisons
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_veto_compare_path": str(json_path),
        "pattern_veto_compare_report_path": str(md_path),
        "recommended_policy": str(recommended_policy),
        "disposition_recommendation": str(disposition),
    }


def build_event_image_pattern_veto_ablation(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("veto ablation currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_library_candidates": dataset_dir / "pattern_library_candidates.json",
        "pattern_micro_features_rebound_onset": dataset_dir / "pattern_micro_features_rebound_onset.json",
        "pattern_gating_rule_rebound_onset_vs_uptrend": dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json",
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "pattern_playbook_rebound_onset": dataset_dir / "pattern_playbook_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"veto ablation requires existing artifacts: {missing}")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    playbook_artifact = read_json(required_paths["pattern_playbook_rebound_onset"])
    thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)

    formal_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_features["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    latest_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if formal_core_gate.empty or latest_core_gate.empty:
        raise RuntimeError("veto ablation requires non-empty core-gate candidates")

    decorated_formal = _decorate_rebound_playbook_frame(formal_core_gate, thresholds=thresholds)
    decorated_latest = _decorate_rebound_playbook_frame(latest_core_gate, thresholds=thresholds)

    threshold_for_bonus = float(adoption_artifact.get("fit_score_policy", {}).get("threshold_for_bonus") or thresholds["threshold_for_bonus"] or 0.50)
    source_disposition = str(adoption_artifact.get("source_disposition") or context["candidate_row"].get("disposition") or "keep")
    asof_iso = _ymd_int_to_iso(int(context["latest_asof"])) or ""

    ablation_specs = [
        {"variant_name": "core_gate_only", "disabled_veto_rule": None},
        {"variant_name": "core_gate_plus_all_veto", "disabled_veto_rule": None, "apply_all_veto": True},
        {"variant_name": "core_gate_plus_veto_without_extreme_volatility", "disabled_veto_rule": "extreme_volatility"},
        {"variant_name": "core_gate_plus_veto_without_thin_liquidity", "disabled_veto_rule": "thin_liquidity"},
        {"variant_name": "core_gate_plus_veto_without_upside_too_thin", "disabled_veto_rule": "upside_too_thin"},
        {"variant_name": "core_gate_plus_veto_without_trend_shape_conflict", "disabled_veto_rule": "trend_shape_conflict"},
    ]

    core_gate_scored = _score_rebound_candidate_frame(
        candidate_frame=formal_core_gate,
        lower_wick_median=context["lower_wick_median"],
        bull_streak_median=context["bull_streak_median"],
        narrow_range_median=context["narrow_range_median"],
        threshold_for_bonus=threshold_for_bonus,
    )
    core_gate_fp = int(
        (
            (core_gate_scored["image_pred_label"].fillna(0).astype(int) == 1)
            & (core_gate_scored["label_id"].fillna(0).astype(int) == 0)
        ).sum()
    )
    core_gate_top10 = _finite_float_or_none(
        _metric_delta(
            _subset_model_metrics(core_gate_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(core_gate_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        ).get("monthly_top10_precision_up_delta")
    )
    core_gate_spread = _finite_float_or_none(
        _metric_delta(
            _subset_model_metrics(core_gate_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(core_gate_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        ).get("monthly_long_short_spread_delta")
    )

    ablation_results: list[dict[str, Any]] = []
    for spec in ablation_specs:
        variant_name = str(spec["variant_name"])
        disabled_rule = spec.get("disabled_veto_rule")
        if variant_name == "core_gate_only":
            formal_frame = formal_core_gate.copy().reset_index(drop=True)
            latest_frame = latest_core_gate.copy().reset_index(drop=True)
        else:
            disabled_rules = () if spec.get("apply_all_veto") else ((str(disabled_rule),) if disabled_rule else ())
            formal_frame = _apply_rebound_veto_ablation(decorated_formal, disabled_rules=disabled_rules)
            latest_frame = _apply_rebound_veto_ablation(decorated_latest, disabled_rules=disabled_rules)

        formal_scored = _score_rebound_candidate_frame(
            candidate_frame=formal_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        latest_scored = _score_rebound_candidate_frame(
            candidate_frame=latest_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        metric_deltas = _metric_delta(
            _subset_model_metrics(formal_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(formal_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=latest_scored,
            dataset_id=str(dataset_id),
            source_artifacts={**context["source_artifacts"], "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"])},
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            variant_name=variant_name,
        )
        false_positive_count = int(
            (
                (formal_scored["image_pred_label"].fillna(0).astype(int) == 1)
                & (formal_scored["label_id"].fillna(0).astype(int) == 0)
            ).sum()
        )
        ablation_results.append(
            {
                "variant_name": variant_name,
                "disabled_veto_rule": disabled_rule,
                "sample_count": int(len(formal_scored)),
                "false_positive_count": false_positive_count,
                "sample_count_delta_vs_core_gate": int(len(formal_scored)) - int(len(core_gate_scored)),
                "false_positive_reduced_count": int(core_gate_fp) - false_positive_count,
                "balanced_accuracy_delta_vs_numeric": metric_deltas.get("balanced_accuracy_delta"),
                "monthly_top10_precision_up_delta_vs_numeric": metric_deltas.get("monthly_top10_precision_up_delta"),
                "monthly_long_short_spread_delta_vs_numeric": metric_deltas.get("monthly_long_short_spread_delta"),
                "bonus_candidate_count": int(formal_scored["bonus_eligible"].sum()) if "bonus_eligible" in formal_scored.columns else 0,
                "rank_changed_count": int(impact["entry_score_rank_changed_count"]),
            }
        )

    all_veto_row = next(row for row in ablation_results if row["variant_name"] == "core_gate_plus_all_veto")
    culprit_veto_rules: list[str] = []
    keep_veto_rules: list[str] = []
    all_veto_fp_reduced = int(all_veto_row["false_positive_reduced_count"])
    for row in ablation_results:
        disabled_rule = row["disabled_veto_rule"]
        if disabled_rule is None:
            continue
        sample_count = int(row["sample_count"])
        fp_reduced = int(row["false_positive_reduced_count"])
        top10_delta = _finite_float_or_none(row["monthly_top10_precision_up_delta_vs_numeric"])
        spread_delta = _finite_float_or_none(row["monthly_long_short_spread_delta_vs_numeric"])
        if (
            sample_count > 0
            and fp_reduced >= max(0, all_veto_fp_reduced - 1)
            and (top10_delta is None or core_gate_top10 is None or top10_delta >= core_gate_top10)
            and (spread_delta is None or core_gate_spread is None or spread_delta >= core_gate_spread)
        ):
            culprit_veto_rules.append(str(disabled_rule))
        if int(row["false_positive_count"]) > int(all_veto_row["false_positive_count"]):
            keep_veto_rules.append(str(disabled_rule))

    if culprit_veto_rules:
        recommended_veto_policy = "drop_one_veto_rule"
        disposition = "keep"
    elif any(int(row["sample_count"]) > 0 for row in ablation_results if row["disabled_veto_rule"] is not None):
        recommended_veto_policy = "analysis_only"
        disposition = "hold"
    elif keep_veto_rules and int(all_veto_row["sample_count"]) > 0:
        recommended_veto_policy = "all_veto_keep"
        disposition = "keep"
    else:
        recommended_veto_policy = "analysis_only"
        disposition = "drop_reconsider"

    artifact = {
        "schema_version": PATTERN_VETO_ABLATION_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {key: str(path) for key, path in required_paths.items()},
        "pattern_id": "rebound_onset_veto_ablation_v1",
        "veto_rule_definition": dict(playbook_artifact.get("veto_rule_definition", {})),
        "ablation_results": ablation_results,
        "recommended_veto_policy": recommended_veto_policy,
        "culprit_veto_rules": culprit_veto_rules,
        "disposition_recommendation": disposition,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_veto_ablation_rebound_onset.json"
    md_path = target_dir / "pattern_veto_ablation_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# Rebound Onset Veto Ablation",
            "",
            f"- dataset: `{dataset_id}`",
            f"- formal compare scope: `{artifact['formal_compare_scope']}`",
            f"- recommended_veto_policy: `{recommended_veto_policy}`",
            f"- culprit_veto_rules: `{culprit_veto_rules}`",
            f"- disposition: `{disposition}`",
            "",
            "## Ablation Results",
            *[
                (
                    f"- `{row['variant_name']}`: disabled=`{row['disabled_veto_rule']}`, "
                    f"sample=`{row['sample_count']}`, fp=`{row['false_positive_count']}`, "
                    f"sample_delta=`{row['sample_count_delta_vs_core_gate']}`, "
                    f"fp_reduced=`{row['false_positive_reduced_count']}`, "
                    f"top10_delta=`{row['monthly_top10_precision_up_delta_vs_numeric']}`, "
                    f"spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`"
                )
                for row in ablation_results
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_veto_ablation_path": str(json_path),
        "pattern_veto_ablation_report_path": str(md_path),
        "recommended_veto_policy": str(recommended_veto_policy),
        "disposition_recommendation": str(disposition),
    }


def build_event_image_pattern_veto_thin_liquidity_compare(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("thin-liquidity veto compare currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_library_candidates": dataset_dir / "pattern_library_candidates.json",
        "pattern_micro_features_rebound_onset": dataset_dir / "pattern_micro_features_rebound_onset.json",
        "pattern_gating_rule_rebound_onset_vs_uptrend": dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json",
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "pattern_playbook_rebound_onset": dataset_dir / "pattern_playbook_rebound_onset.json",
        "pattern_veto_ablation_rebound_onset": dataset_dir / "pattern_veto_ablation_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"thin-liquidity veto compare requires existing artifacts: {missing}")

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_features = _load_rebound_formal_feature_frame(dataset_id=dataset_id)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    playbook_artifact = read_json(required_paths["pattern_playbook_rebound_onset"])
    thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)

    formal_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_features["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    latest_core_gate = _build_rebound_core_gate_candidate_frame(
        feature_frame=context["feature_frame"],
        gate_rule=context["gate_rule"],
    )
    if formal_core_gate.empty or latest_core_gate.empty:
        raise RuntimeError("thin-liquidity veto compare requires non-empty core-gate candidates")

    decorated_formal = _decorate_rebound_playbook_frame(formal_core_gate, thresholds=thresholds)
    decorated_latest = _decorate_rebound_playbook_frame(latest_core_gate, thresholds=thresholds)

    threshold_for_bonus = float(
        adoption_artifact.get("fit_score_policy", {}).get("threshold_for_bonus")
        or thresholds["threshold_for_bonus"]
        or 0.50
    )
    source_disposition = str(
        adoption_artifact.get("source_disposition") or context["candidate_row"].get("disposition") or "keep"
    )
    asof_iso = _ymd_int_to_iso(int(context["latest_asof"])) or ""
    base_turnover20_veto_min = float(thresholds["turnover20_veto_min"] or 0.0)

    core_gate_scored = _score_rebound_candidate_frame(
        candidate_frame=formal_core_gate,
        lower_wick_median=context["lower_wick_median"],
        bull_streak_median=context["bull_streak_median"],
        narrow_range_median=context["narrow_range_median"],
        threshold_for_bonus=threshold_for_bonus,
    )
    core_gate_metrics = _metric_delta(
        _subset_model_metrics(core_gate_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
        _subset_model_metrics(core_gate_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
    )
    core_gate_false_positive_count = int(
        (
            (core_gate_scored["image_pred_label"].fillna(0).astype(int) == 1)
            & (core_gate_scored["label_id"].fillna(0).astype(int) == 0)
        ).sum()
    )
    core_gate_top10_delta = _finite_float_or_none(core_gate_metrics.get("monthly_top10_precision_up_delta"))
    core_gate_spread_delta = _finite_float_or_none(core_gate_metrics.get("monthly_long_short_spread_delta"))

    variant_specs = [
        {"variant_name": "core_gate_only", "thin_liquidity_turnover20_min": None, "reference_only": True},
        {"variant_name": "current", "thin_liquidity_turnover20_min": base_turnover20_veto_min},
        {"variant_name": "weak_1", "thin_liquidity_turnover20_min": base_turnover20_veto_min * 0.85},
        {"variant_name": "weak_2", "thin_liquidity_turnover20_min": base_turnover20_veto_min * 0.70},
        {"variant_name": "weak_3", "thin_liquidity_turnover20_min": base_turnover20_veto_min * 0.55},
        {"variant_name": "off", "thin_liquidity_turnover20_min": None},
    ]

    compare_results: list[dict[str, Any]] = []
    for spec in variant_specs:
        variant_name = str(spec["variant_name"])
        if variant_name == "core_gate_only":
            formal_variant = formal_core_gate.copy().reset_index(drop=True)
            latest_variant = latest_core_gate.copy().reset_index(drop=True)
        else:
            thin_floor = spec["thin_liquidity_turnover20_min"]
            formal_variant = _apply_rebound_thin_liquidity_threshold(
                decorated_formal,
                thin_liquidity_turnover20_min=None if variant_name == "off" else float(thin_floor),
            )
            latest_variant = _apply_rebound_thin_liquidity_threshold(
                decorated_latest,
                thin_liquidity_turnover20_min=None if variant_name == "off" else float(thin_floor),
            )

        formal_scored = _score_rebound_candidate_frame(
            candidate_frame=formal_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        latest_scored = _score_rebound_candidate_frame(
            candidate_frame=latest_variant,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        metric_deltas = _metric_delta(
            _subset_model_metrics(formal_scored, prob_column="image_pred_prob_up", pred_column="image_pred_label"),
            _subset_model_metrics(formal_scored, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label"),
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=context["latest_frame"],
            candidate_frame=latest_scored,
            dataset_id=str(dataset_id),
            source_artifacts={
                **context["source_artifacts"],
                "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"]),
            },
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            variant_name=f"thin_liquidity_{variant_name}",
        )
        false_positive_count = int(
            (
                (formal_scored["image_pred_label"].fillna(0).astype(int) == 1)
                & (formal_scored["label_id"].fillna(0).astype(int) == 0)
            ).sum()
        )
        compare_results.append(
            {
                "variant_name": variant_name,
                "thin_liquidity_turnover20_min": (
                    None if variant_name == "off" else _finite_float_or_none(spec["thin_liquidity_turnover20_min"])
                ),
                "sample_count": int(len(formal_scored)),
                "false_positive_count": false_positive_count,
                "sample_count_delta_vs_core_gate": int(len(formal_scored)) - int(len(core_gate_scored)),
                "false_positive_reduced_count": int(core_gate_false_positive_count) - false_positive_count,
                "balanced_accuracy_delta_vs_numeric": metric_deltas.get("balanced_accuracy_delta"),
                "monthly_top10_precision_up_delta_vs_numeric": metric_deltas.get("monthly_top10_precision_up_delta"),
                "monthly_long_short_spread_delta_vs_numeric": metric_deltas.get("monthly_long_short_spread_delta"),
                "bonus_candidate_count": int(formal_scored["bonus_eligible"].sum()) if "bonus_eligible" in formal_scored.columns else 0,
                "rank_changed_count": int(impact["entry_score_rank_changed_count"]),
            }
        )

    policy_rows = [row for row in compare_results if row["variant_name"] != "core_gate_only"]
    practical_rows: list[dict[str, Any]] = []
    fallback_rows: list[dict[str, Any]] = []
    order_map = {"current": 0, "weak_1": 1, "weak_2": 2, "weak_3": 3, "off": 4}
    for row in policy_rows:
        sample_count = int(row["sample_count"])
        false_positive_count = int(row["false_positive_count"])
        top10_delta = _finite_float_or_none(row["monthly_top10_precision_up_delta_vs_numeric"])
        spread_delta = _finite_float_or_none(row["monthly_long_short_spread_delta_vs_numeric"])
        if sample_count >= 1:
            fallback_rows.append(row)
        if (
            sample_count >= 1
            and false_positive_count <= core_gate_false_positive_count
            and (core_gate_top10_delta is None or top10_delta is None or top10_delta >= core_gate_top10_delta)
            and (core_gate_spread_delta is None or spread_delta is None or spread_delta >= core_gate_spread_delta)
        ):
            practical_rows.append(row)

    recommended_policy = "analysis_only"
    disposition = "drop_reconsider"
    if practical_rows:
        practical_rows.sort(
            key=lambda row: (
                order_map[str(row["variant_name"])],
                int(row["false_positive_count"]),
                -float(_finite_float_or_none(row["monthly_long_short_spread_delta_vs_numeric"]) or float("-inf")),
            )
        )
        chosen = practical_rows[0]
        recommended_policy = (
            "keep_current"
            if str(chosen["variant_name"]) == "current"
            else f"weaken_to_{chosen['variant_name']}"
        )
        disposition = "keep"
    elif fallback_rows:
        recommended_policy = "analysis_only"
        disposition = "hold"

    artifact = {
        "schema_version": PATTERN_VETO_THIN_LIQUIDITY_COMPARE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {key: str(path) for key, path in required_paths.items()},
        "pattern_id": "rebound_onset_veto_thin_liquidity_compare_v1",
        "base_turnover20_veto_min": base_turnover20_veto_min,
        "compare_results": compare_results,
        "recommended_thin_liquidity_policy": recommended_policy,
        "disposition_recommendation": disposition,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_veto_thin_liquidity_compare_rebound_onset.json"
    md_path = target_dir / "pattern_veto_thin_liquidity_compare_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# Rebound Onset Thin-Liquidity Veto Compare",
            "",
            f"- dataset: `{dataset_id}`",
            f"- formal compare scope: `{artifact['formal_compare_scope']}`",
            f"- base_turnover20_veto_min: `{base_turnover20_veto_min}`",
            f"- recommended_thin_liquidity_policy: `{recommended_policy}`",
            f"- disposition: `{disposition}`",
            "",
            "## Compare Results",
            *[
                (
                    f"- `{row['variant_name']}`: turnover20_min=`{row['thin_liquidity_turnover20_min']}`, "
                    f"sample=`{row['sample_count']}`, fp=`{row['false_positive_count']}`, "
                    f"sample_delta=`{row['sample_count_delta_vs_core_gate']}`, "
                    f"fp_reduced=`{row['false_positive_reduced_count']}`, "
                    f"top10_delta=`{row['monthly_top10_precision_up_delta_vs_numeric']}`, "
                    f"spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`"
                )
                for row in compare_results
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_veto_thin_liquidity_compare_path": str(json_path),
        "pattern_veto_thin_liquidity_compare_report_path": str(md_path),
        "recommended_thin_liquidity_policy": str(recommended_policy),
        "disposition_recommendation": str(disposition),
    }


def build_event_image_pattern_selection_contract(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("selection contract diagnostics currently supports rebound_onset only")

    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    required_paths = {
        "pattern_adoption_rebound_onset": dataset_dir / "pattern_adoption_rebound_onset.json",
        "pattern_veto_ablation_rebound_onset": dataset_dir / "pattern_veto_ablation_rebound_onset.json",
        "pattern_veto_thin_liquidity_compare_rebound_onset": dataset_dir / "pattern_veto_thin_liquidity_compare_rebound_onset.json",
        "predictions": dataset_dir / "predictions.parquet",
        "dataset_manifest": dataset_dir / "dataset_manifest.json",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise RuntimeError(f"selection contract diagnostics requires existing artifacts: {missing}")

    rebound_validation_summary_path = _resolve_latest_report_artifact(
        report_dir_name="rebound_full_validation",
        filename="rebound_full_validation_summary.json",
    )
    ranking_backtest_summary_path = _resolve_latest_report_artifact(
        report_dir_name="ranking_backtests",
        filename="ranking_backtest_summary.json",
    )
    full_validation_summary = read_json(rebound_validation_summary_path)
    ranking_backtest_summary = read_json(ranking_backtest_summary_path)
    adoption_artifact = read_json(required_paths["pattern_adoption_rebound_onset"])
    thin_liquidity_compare = read_json(required_paths["pattern_veto_thin_liquidity_compare_rebound_onset"])

    context = _load_rebound_adoption_context(dataset_id=dataset_id, output_root=output_root)
    formal_frame = context["test_common_frame"].copy().reset_index(drop=True)
    dataset_manifest = read_json(required_paths["dataset_manifest"])
    export_db_path = str(dataset_manifest["source_export_db_path"])
    history_frame = _load_export_history(
        export_db_path=export_db_path,
        codes=sorted({_normalize_code_key(code) for code in formal_frame["code"].tolist() if _normalize_code_key(code)}),
        max_trade_date=int(formal_frame["as_of_date"].max()),
    )
    formal_feature_frame = _attach_micro_feature_rows(formal_frame, history_frame)
    if formal_feature_frame.empty:
        raise RuntimeError("selection contract diagnostics produced no formal feature rows")

    core_gate_frame = _build_rebound_core_gate_candidate_frame(
        feature_frame=formal_feature_frame,
        gate_rule=context["gate_rule"],
    )
    thresholds = _build_rebound_playbook_thresholds(context, adoption_artifact)
    decorated_core_gate = _decorate_rebound_playbook_frame(core_gate_frame, thresholds=thresholds)
    thin_liquidity_floor_current = float(thin_liquidity_compare["base_turnover20_veto_min"])
    weak_1_row = next(
        (row for row in thin_liquidity_compare.get("compare_results", []) if str(row.get("variant_name")) == "weak_1"),
        None,
    )
    if not isinstance(weak_1_row, dict):
        raise RuntimeError("selection contract diagnostics requires weak_1 in thin-liquidity compare artifact")
    thin_liquidity_floor_weak_1 = _finite_float_or_none(weak_1_row.get("thin_liquidity_turnover20_min"))

    current_veto_frame = _apply_rebound_thin_liquidity_threshold(
        decorated_core_gate,
        thin_liquidity_turnover20_min=thin_liquidity_floor_current,
    )
    weak_1_veto_frame = _apply_rebound_thin_liquidity_threshold(
        decorated_core_gate,
        thin_liquidity_turnover20_min=thin_liquidity_floor_weak_1,
    )

    threshold_for_bonus = float(adoption_artifact.get("fit_score_policy", {}).get("threshold_for_bonus") or 0.50)
    source_disposition = str(adoption_artifact.get("source_disposition") or context["candidate_row"].get("disposition") or "keep")
    source_artifacts = {
        **context["source_artifacts"],
        "pattern_adoption_rebound_onset": str(required_paths["pattern_adoption_rebound_onset"]),
        "pattern_veto_ablation_rebound_onset": str(required_paths["pattern_veto_ablation_rebound_onset"]),
        "pattern_veto_thin_liquidity_compare_rebound_onset": str(required_paths["pattern_veto_thin_liquidity_compare_rebound_onset"]),
        "rebound_full_validation_summary": str(rebound_validation_summary_path),
        "ranking_backtest_summary": str(ranking_backtest_summary_path),
    }

    baseline_stage_parts: list[pd.DataFrame] = []
    candidate_stage_parts: list[pd.DataFrame] = []
    ranking_top20_parts: list[pd.DataFrame] = []
    ranking_entry_parts: list[pd.DataFrame] = []
    for as_of, month_frame in formal_feature_frame.groupby("as_of_date", sort=True):
        baseline_month = _score_rebound_candidate_frame(
            candidate_frame=core_gate_frame.loc[core_gate_frame["as_of_date"].astype(int) == int(as_of)].copy().reset_index(drop=True),
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        candidate_month = _score_rebound_candidate_frame(
            candidate_frame=weak_1_veto_frame.loc[weak_1_veto_frame["as_of_date"].astype(int) == int(as_of)].copy().reset_index(drop=True),
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=threshold_for_bonus,
        )
        asof_iso = _ymd_int_to_iso(int(as_of))
        if asof_iso is None:
            continue
        preview_items = _make_ranking_preview_items(month_frame)
        baseline_snapshot = _build_rebound_bridge_snapshot(
            candidate_frame=baseline_month,
            dataset_id=str(dataset_id),
            source_artifacts=source_artifacts,
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            run_id=f"selection_contract_baseline_{as_of}",
        )
        candidate_snapshot = _build_rebound_bridge_snapshot(
            candidate_frame=candidate_month,
            dataset_id=str(dataset_id),
            source_artifacts=source_artifacts,
            asof_iso=asof_iso,
            source_disposition=source_disposition,
            run_id=f"selection_contract_weak_1_{as_of}",
        )
        baseline_items = _preview_entry_decorated_items(items=preview_items, prior_snapshot=baseline_snapshot)
        candidate_items = _preview_entry_decorated_items(items=preview_items, prior_snapshot=candidate_snapshot)

        baseline_codes = {
            str(item.get("code") or "")
            for item in baseline_items
            if str(item.get("researchPatternTag") or "").strip() == "rebound_onset" or float(item.get("researchPriorBonus") or 0.0) > 0.0
        }
        candidate_codes = {
            str(item.get("code") or "")
            for item in candidate_items
            if str(item.get("researchPatternTag") or "").strip() == "rebound_onset" or float(item.get("researchPriorBonus") or 0.0) > 0.0
        }
        top20_codes = {str(item.get("code") or "") for item in candidate_items[:20]}
        top20_entry_codes = {
            str(item.get("code") or "")
            for item in candidate_items[:20]
            if bool(item.get("entryQualified"))
        }

        baseline_stage_parts.append(_filter_stage_frame_by_codes(month_frame, codes=baseline_codes))
        candidate_stage_parts.append(_filter_stage_frame_by_codes(month_frame, codes=candidate_codes))
        ranking_top20_parts.append(_filter_stage_frame_by_codes(month_frame, codes=top20_codes))
        ranking_entry_parts.append(_filter_stage_frame_by_codes(month_frame, codes=top20_entry_codes))

    baseline_tag_bonus_frame = (
        pd.concat(baseline_stage_parts, ignore_index=True) if baseline_stage_parts else formal_feature_frame.head(0).copy()
    )
    candidate_tag_bonus_frame = (
        pd.concat(candidate_stage_parts, ignore_index=True) if candidate_stage_parts else formal_feature_frame.head(0).copy()
    )
    ranking_top20_frame = (
        pd.concat(ranking_top20_parts, ignore_index=True) if ranking_top20_parts else formal_feature_frame.head(0).copy()
    )
    ranking_top20_entry_frame = (
        pd.concat(ranking_entry_parts, ignore_index=True) if ranking_entry_parts else formal_feature_frame.head(0).copy()
    )

    stage_frames = [
        ("formal_common_subset", formal_feature_frame),
        ("rebound_onset_core_gate", core_gate_frame),
        ("core_gate_plus_veto_current", current_veto_frame),
        ("core_gate_plus_veto_thin_liquidity_weak_1", weak_1_veto_frame),
        ("rebound_live_baseline_tag_bonus", baseline_tag_bonus_frame),
        ("rebound_candidate_policy_tag_bonus", candidate_tag_bonus_frame),
        ("ranking_top20", ranking_top20_frame),
        ("ranking_top20_entryQualified", ranking_top20_entry_frame),
    ]

    stage_results: list[dict[str, Any]] = []
    previous_frame: pd.DataFrame | None = None
    for stage_name, frame in stage_frames:
        stage_results.append(
            _summarize_selection_contract_stage(
                stage_name=stage_name,
                stage_frame=frame,
                previous_frame=previous_frame,
                core_gate_frame=core_gate_frame,
            )
        )
        previous_frame = frame

    stage_map = {str(row["stage_name"]): row for row in stage_results}
    primary_leak_stage, leak_reason = _determine_selection_contract_leak(
        current_veto_stage=stage_map["core_gate_plus_veto_current"],
        weak_stage=stage_map["core_gate_plus_veto_thin_liquidity_weak_1"],
        ranking_top20_stage=stage_map["ranking_top20"],
        entry_stage=stage_map["ranking_top20_entryQualified"],
        full_validation_summary=full_validation_summary,
    )

    weak_stage = stage_map["core_gate_plus_veto_thin_liquidity_weak_1"]
    ranking_stage = stage_map["ranking_top20"]
    entry_stage = stage_map["ranking_top20_entryQualified"]
    if (
        int(weak_stage["sample_count"]) > int(stage_map["core_gate_plus_veto_current"]["sample_count"])
        and (_finite_float_or_none(weak_stage["monthly_long_short_spread_delta_vs_numeric"]) or 0.0) >= 0.0
        and (_finite_float_or_none(weak_stage["monthly_top10_precision_up_delta_vs_numeric"]) or 0.0) >= 0.0
    ):
        recommended_next_action = "promote_weak_1_to_live_monitor"
    elif len(entry_stage.get("lost_winner_codes") or []) > 0:
        recommended_next_action = "keep_veto_analysis_only_and_revisit_entry_qualification"
    elif len(ranking_stage.get("lost_winner_codes") or []) > 0:
        recommended_next_action = "keep_current_veto_and_revisit_ranking_selection"
    elif str(full_validation_summary.get("decision") or "") == "fix_holdings_before_policy_change":
        recommended_next_action = "redirect_to_toredex_holdings_fix"
    else:
        recommended_next_action = "keep_current_veto_and_revisit_ranking_selection"

    artifact = {
        "schema_version": PATTERN_SELECTION_CONTRACT_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": source_artifacts,
        "pattern_id": "rebound_onset_selection_contract_v1",
        "latest_asof": _ymd_int_to_iso(int(context["latest_asof"])),
        "selection_stages": stage_results,
        "primary_leak_stage": primary_leak_stage,
        "leak_reason": leak_reason,
        "recommended_next_action": recommended_next_action,
        "full_validation_decision": full_validation_summary.get("decision"),
        "ranking_backtest_disposition": ranking_backtest_summary.get("disposition"),
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_selection_contract_rebound_onset.json"
    md_path = target_dir / "pattern_selection_contract_rebound_onset.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# Rebound Onset Selection Contract Diagnostics",
            "",
            f"- dataset: `{dataset_id}`",
            f"- formal compare scope: `{artifact['formal_compare_scope']}`",
            f"- primary_leak_stage: `{primary_leak_stage}`",
            f"- recommended_next_action: `{recommended_next_action}`",
            f"- full_validation_decision: `{artifact['full_validation_decision']}`",
            "",
            "## Stage Results",
            *[
                (
                    f"- `{row['stage_name']}`: sample=`{row['sample_count']}`, positive=`{row['positive_count']}`, "
                    f"false_positive=`{row['false_positive_count']}`, attrition_prev=`{row['sample_attrition_vs_previous']}`, "
                    f"retention_core=`{row['positive_retention_vs_core_gate']}`, spread_delta=`{row['monthly_long_short_spread_delta_vs_numeric']}`, "
                    f"lost_winners=`{row['lost_winner_codes']}`"
                )
                for row in stage_results
            ],
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "pattern_selection_contract_path": str(json_path),
        "pattern_selection_contract_report_path": str(md_path),
        "primary_leak_stage": str(primary_leak_stage),
        "recommended_next_action": str(recommended_next_action),
    }


def _load_source_history(
    *,
    codes: list[str],
    max_trade_date: int,
) -> pd.DataFrame:
    normalized_codes = sorted({str(code) for code in codes if str(code).strip()})
    if not normalized_codes:
        raise RuntimeError("source history load requires at least one code")
    placeholders = ",".join(["?"] * len(normalized_codes))
    bars_date_expr = _trade_date_expr("b.date")
    ma_date_expr = _trade_date_expr("m.date")
    with get_conn() as conn:
        frame = conn.execute(
            f"""
            SELECT
                b.code,
                {bars_date_expr} AS trade_date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                m.ma7,
                m.ma20,
                m.ma60,
                NULL::DOUBLE AS ma100,
                NULL::DOUBLE AS ma200
            FROM daily_bars b
            LEFT JOIN daily_ma m
              ON m.code = b.code AND {ma_date_expr} = {bars_date_expr}
            WHERE b.code IN ({placeholders})
              AND {bars_date_expr} <= ?
            ORDER BY b.code, trade_date
            """,
            [*normalized_codes, int(max_trade_date)],
        ).df()
    if frame.empty:
        raise RuntimeError("source history query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="raise").astype(int)
    numeric_columns = ("o", "h", "l", "c", "v", "ma7", "ma20", "ma60", "ma100", "ma200")
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["daily_return"] = frame.groupby("code")["c"].pct_change()
    frame["ma120"] = frame.groupby("code")["c"].rolling(window=120, min_periods=120).mean().reset_index(level=0, drop=True)
    frame["rolling_high20"] = frame.groupby("code")["h"].rolling(window=20, min_periods=20).max().reset_index(level=0, drop=True)
    frame["rolling_high60"] = frame.groupby("code")["h"].rolling(window=60, min_periods=60).max().reset_index(level=0, drop=True)
    frame["volume_mean5"] = frame.groupby("code")["v"].rolling(window=5, min_periods=5).mean().reset_index(level=0, drop=True)
    frame["volume_mean20"] = frame.groupby("code")["v"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    frame["turnover20"] = frame.groupby("code")["c"].transform(lambda series: series) * frame.groupby("code")["v"].transform(lambda series: series)
    frame["turnover20"] = frame.groupby("code")["turnover20"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    frame["realized_vol20"] = frame.groupby("code")["daily_return"].rolling(window=20, min_periods=20).std().reset_index(level=0, drop=True)
    frame["range_pct"] = (frame["h"] - frame["l"]) / frame["c"].replace(0.0, np.nan)
    frame["range_median20"] = frame.groupby("code")["range_pct"].rolling(window=20, min_periods=20).median().reset_index(level=0, drop=True)
    return frame


def _recent_trade_dates(*, days: int) -> list[int]:
    expr = _trade_date_expr("date")
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {expr} AS trade_date
            FROM daily_bars
            WHERE {expr} IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [int(days)],
        ).fetchall()
    values = sorted(int(row[0]) for row in rows if row and row[0] is not None)
    if not values:
        raise RuntimeError("failed to resolve recent trade dates")
    return values


def _make_monitor_preview_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    preview_items: list[dict[str, Any]] = []
    for item in items:
        preview_items.append(
            {
                "code": str(item.get("code") or ""),
                "changePct": rankings_cache._first_finite(item.get("changePct")) or 0.0,
                "weeklyBreakoutUpProb": rankings_cache._first_finite(item.get("weeklyBreakoutUpProb")),
                "monthlyBreakoutUpProb": rankings_cache._first_finite(item.get("monthlyBreakoutUpProb")),
                "monthlyRangeProb": rankings_cache._first_finite(item.get("monthlyRangeProb")),
                "candleTripletUp": rankings_cache._first_finite(item.get("candleTripletUp")),
                "liquidity20d": rankings_cache._first_finite(item.get("liquidity20d"), item.get("turnover20d")),
                "hybridScore": rankings_cache._first_finite(item.get("hybridScore")),
                "setupType": str(item.get("setupType") or "watch"),
            }
        )
    return preview_items


def build_event_image_rebound_live_monitor(
    *,
    dataset_id: str,
    days: int = 60,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    policy_path = dataset_dir / "pattern_adoption_policy_rebound_onset.json"
    if not policy_path.exists():
        build_event_image_pattern_adoption_policy(dataset_id=dataset_id, pattern="rebound_onset")
    policy_payload = read_json(policy_path)
    sequence_payload = read_json(dataset_dir / "pattern_sequence_combo_rebound_onset.json")
    context = _load_rebound_adoption_context(dataset_id=dataset_id)
    trade_dates = _recent_trade_dates(days=days)

    ranking_items_by_day: dict[int, list[dict[str, Any]]] = {}
    all_codes: set[str] = set()
    for as_of in trade_dates:
        payload = rankings_cache.get_rankings_asof("D", "latest", "up", 200, as_of=as_of, mode="hybrid", risk_mode="balanced")
        items = payload.get("items") if isinstance(payload, dict) else []
        normalized_items = [dict(item) for item in items if isinstance(item, dict) and str(item.get("code") or "").strip()]
        if not normalized_items:
            continue
        ranking_items_by_day[int(as_of)] = normalized_items
        all_codes.update(str(item["code"]) for item in normalized_items)
    if not ranking_items_by_day:
        raise RuntimeError("rebound live monitor produced no ranking days")

    history_frame = _load_source_history(codes=sorted(all_codes), max_trade_date=max(ranking_items_by_day))
    recommended_variant = str(policy_payload["recommended_variant"]["variant_name"])
    recommended_sequence = sequence_payload["recommended_candidate"]
    recommended_conditions = list(recommended_sequence.get("conditions") or [])

    panel_rows: list[dict[str, Any]] = []
    for as_of, items in sorted(ranking_items_by_day.items()):
        normalized_preview_items = _make_monitor_preview_items(items)
        feature_seed = pd.DataFrame([{"code": str(item["code"]), "as_of_date": int(as_of)} for item in items])
        feature_frame = _attach_micro_feature_rows(feature_seed, history_frame)
        core_gate_candidates = _build_rebound_core_gate_candidate_frame(feature_frame=feature_frame, gate_rule=context["gate_rule"])

        baseline_candidates = _score_rebound_candidate_frame(
            candidate_frame=core_gate_candidates,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=0.50,
        )

        candidate_frame = core_gate_candidates.copy()
        candidate_threshold = 0.45
        if recommended_variant == "soft_bonus_plus_best_sequence":
            candidate_frame = candidate_frame.loc[_apply_combo_rule(candidate_frame, conditions=recommended_conditions)].copy().reset_index(drop=True)
        elif recommended_variant == "baseline_live":
            candidate_threshold = 0.50
        candidate_candidates = _score_rebound_candidate_frame(
            candidate_frame=candidate_frame,
            lower_wick_median=context["lower_wick_median"],
            bull_streak_median=context["bull_streak_median"],
            narrow_range_median=context["narrow_range_median"],
            threshold_for_bonus=candidate_threshold,
        )

        asof_iso = _ymd_int_to_iso(int(as_of))
        if asof_iso is None:
            continue
        baseline_snapshot = _build_rebound_bridge_snapshot(
            candidate_frame=baseline_candidates,
            dataset_id=str(dataset_id),
            source_artifacts={**context["source_artifacts"], "pattern_adoption_policy": str(policy_path)},
            asof_iso=asof_iso,
            source_disposition=str(context["candidate_row"].get("disposition") or "keep"),
            run_id=f"baseline_live_{as_of}",
        )
        candidate_snapshot = _build_rebound_bridge_snapshot(
            candidate_frame=candidate_candidates,
            dataset_id=str(dataset_id),
            source_artifacts={**context["source_artifacts"], "pattern_adoption_policy": str(policy_path)},
            asof_iso=asof_iso,
            source_disposition=str(policy_payload.get("disposition_recommendation") or "keep"),
            run_id=f"{recommended_variant}_{as_of}",
        )
        baseline_items = _preview_entry_decorated_items(items=normalized_preview_items, prior_snapshot=baseline_snapshot)
        candidate_items = _preview_entry_decorated_items(items=normalized_preview_items, prior_snapshot=candidate_snapshot)
        baseline_rank_map = _rank_position_map(baseline_items)
        candidate_rank_map = _rank_position_map(candidate_items)
        changed_codes = sorted(
            code
            for code, rank in candidate_rank_map.items()
            if baseline_rank_map.get(code) is not None and int(rank) != int(baseline_rank_map[code])
        )
        baseline_top20 = {str(item.get("code") or "") for item in baseline_items[:20]}
        candidate_top20 = {str(item.get("code") or "") for item in candidate_items[:20]}
        denom = max(len(baseline_top20), len(candidate_top20), 1)
        top20_overlap = float(len(baseline_top20 & candidate_top20) / denom)
        baseline_bonus_rows = [row for row in baseline_candidates.to_dict(orient="records") if bool(row.get("bonus_eligible"))]
        bonus_rows = [row for row in candidate_candidates.to_dict(orient="records") if bool(row.get("bonus_eligible"))]
        panel_rows.append(
            {
                "as_of": int(as_of),
                "as_of_iso": asof_iso,
                "baseline_tag_candidate_count": int(len(baseline_candidates)),
                "baseline_bonus_candidate_count": int(baseline_candidates["bonus_eligible"].sum()) if "bonus_eligible" in baseline_candidates.columns else 0,
                "baseline_bonus_codes": [str(row["code"]) for row in baseline_bonus_rows],
                "tag_candidate_count": int(len(candidate_candidates)),
                "bonus_candidate_count": int(candidate_candidates["bonus_eligible"].sum()) if "bonus_eligible" in candidate_candidates.columns else 0,
                "bonus_codes": [str(row["code"]) for row in bonus_rows],
                "entry_score_rank_changed_count": int(len(changed_codes)),
                "entry_score_rank_changed_codes": changed_codes,
                "top20_overlap_with_baseline": top20_overlap,
            }
        )

    panel = pd.DataFrame(panel_rows).sort_values("as_of", kind="stable").reset_index(drop=True)
    baseline_positive = panel["baseline_bonus_candidate_count"].astype(int)
    candidate_positive = panel["bonus_candidate_count"].astype(int)
    artifact = {
        "schema_version": REBOUND_LIVE_MONITOR_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "days": int(days),
        "generated_at": _utc_now_iso(),
        "baseline_variant": "baseline_live",
        "candidate_variant": recommended_variant,
        "source_artifacts": {
            "pattern_adoption_policy": str(policy_path),
            "pattern_sequence_combo": str(dataset_dir / "pattern_sequence_combo_rebound_onset.json"),
        },
        "summary": {
            "baseline": {
                "days_with_tag": int((panel["baseline_tag_candidate_count"].astype(int) >= 1).sum()),
                "days_with_bonus": int((baseline_positive >= 1).sum()),
                "median_bonus_candidate_count": float(panel["baseline_bonus_candidate_count"].median()) if not panel.empty else 0.0,
            },
            "candidate": {
                "days_with_tag": int((panel["tag_candidate_count"].astype(int) >= 1).sum()),
                "days_with_bonus": int((candidate_positive >= 1).sum()),
                "median_bonus_candidate_count": float(panel["bonus_candidate_count"].median()) if not panel.empty else 0.0,
                "median_entry_rank_changed_count": float(panel["entry_score_rank_changed_count"].median()) if not panel.empty else 0.0,
                "max_entry_rank_changed_count": int(panel["entry_score_rank_changed_count"].max()) if not panel.empty else 0,
            },
            "comparison": {
                "days_with_bonus_delta_vs_baseline": int((candidate_positive >= 1).sum()) - int((baseline_positive >= 1).sum()),
                "median_bonus_candidate_count_delta_vs_baseline": (
                    float(panel["bonus_candidate_count"].median()) - float(panel["baseline_bonus_candidate_count"].median())
                    if not panel.empty
                    else 0.0
                ),
            },
        },
        "panel_path": None,
    }
    output_dir = Path(output_root).expanduser().resolve() if output_root is not None else (tradex_reports_root() / "rebound_live_monitor" / f"rebound_live_monitor_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}")
    output_dir.mkdir(parents=True, exist_ok=True)
    panel_path = output_dir / "rebound_live_monitor_panel.parquet"
    panel.to_parquet(panel_path, index=False)
    artifact["panel_path"] = str(panel_path)
    json_path = output_dir / "rebound_live_monitor.json"
    md_path = output_dir / "rebound_live_monitor.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Live Monitor",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- baseline_variant: `baseline_live`",
            f"- candidate_variant: `{recommended_variant}`",
            f"- baseline_days_with_bonus: `{artifact['summary']['baseline']['days_with_bonus']}`",
            f"- candidate_days_with_bonus: `{artifact['summary']['candidate']['days_with_bonus']}`",
            f"- max_entry_rank_changed_count: `{artifact['summary']['candidate']['max_entry_rank_changed_count']}`",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "rebound_live_monitor_path": str(json_path),
        "rebound_live_monitor_report_path": str(md_path),
        "rebound_live_monitor_panel_path": str(panel_path),
        "candidate_variant": recommended_variant,
    }


def run_event_image_dataset_rebound_v3_round(
    *,
    dataset_id: str,
    max_workers: int = 4,
    monitor_days: int = 60,
    diagnosis_start_date: str | None = None,
    diagnosis_end_date: str | None = None,
) -> dict[str, Any]:
    from app.backend.services.analysis.toredex_policy_diagnosis_service import run_toredex_policy_diagnosis

    dataset_dir = event_image_dataset_dir(dataset_id)
    if not (dataset_dir / "pattern_library_candidates.json").exists():
        build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=["rebound_onset", "uptrend"])

    workstream_results: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        future_map = {
            executor.submit(build_event_image_pattern_sequence_combo, dataset_id=dataset_id, pattern="rebound_onset"): "sequence_combo",
            executor.submit(
                run_toredex_policy_diagnosis,
                start_date=diagnosis_start_date,
                end_date=diagnosis_end_date,
                output_dir=None,
            ): "toredex_diagnosis",
        }
        for future in as_completed(future_map):
            label = future_map[future]
            try:
                workstream_results[label] = future.result()
            except Exception as exc:
                errors.append({"workstream": label, "error": str(exc)})
    if errors:
        raise RuntimeError(f"rebound v3 round failed: {errors}")

    try:
        workstream_results["adoption_policy"] = build_event_image_pattern_adoption_policy(dataset_id=dataset_id, pattern="rebound_onset")
        workstream_results["live_monitor"] = build_event_image_rebound_live_monitor(dataset_id=dataset_id, days=int(monitor_days))
    except Exception as exc:
        errors.append({"workstream": "dependent_phase", "error": str(exc)})
    if errors:
        raise RuntimeError(f"rebound v3 round failed: {errors}")

    sequence_payload = read_json(Path(workstream_results["sequence_combo"]["pattern_sequence_combo_path"]))
    adoption_policy_payload = read_json(Path(workstream_results["adoption_policy"]["pattern_adoption_policy_path"]))
    live_monitor_payload = read_json(Path(workstream_results["live_monitor"]["rebound_live_monitor_path"]))
    diagnosis_payload = read_json(Path(workstream_results["toredex_diagnosis"]["toredex_policy_diagnosis_path"]))

    sequence_has_keep = any(str(row.get("disposition")) in {"keep", "keep_strengthened"} for row in sequence_payload["combo_results"])
    recommended_variant = str(adoption_policy_payload["recommended_variant"]["variant_name"])
    adoption_ok = recommended_variant in {"soft_bonus_only", "soft_bonus_plus_best_sequence"}
    days_with_bonus_delta = int(live_monitor_payload["summary"]["comparison"]["days_with_bonus_delta_vs_baseline"])
    monitor_ok = days_with_bonus_delta > 0
    primary_failure_axis = str(diagnosis_payload.get("primary_failure_axis") or "").strip()
    diagnosis_ok = primary_failure_axis in {"holdings", "turnover", "risk_gate_only", "exit_policy"}

    disposition = "hold"
    if sequence_has_keep and adoption_ok and monitor_ok and diagnosis_ok:
        disposition = "keep_strengthened"
    elif sequence_has_keep and adoption_ok and diagnosis_ok:
        disposition = "keep"

    library_path = dataset_dir / "pattern_library_candidates.json"
    library_payload = read_json(library_path)
    updated_candidates: list[dict[str, Any]] = []
    for row in library_payload["pattern_candidates"]:
        updated = dict(row)
        if str(updated.get("regime_tag")) == "rebound_onset":
            updated["disposition"] = disposition
            updated["sequence_combo_artifact_path"] = str(Path(workstream_results["sequence_combo"]["pattern_sequence_combo_path"]))
            updated["adoption_policy_artifact_path"] = str(Path(workstream_results["adoption_policy"]["pattern_adoption_policy_path"]))
            updated["live_monitor_artifact_path"] = str(Path(workstream_results["live_monitor"]["rebound_live_monitor_path"]))
            updated["toredex_policy_diagnosis_artifact_path"] = str(Path(workstream_results["toredex_diagnosis"]["toredex_policy_diagnosis_path"]))
            updated["recommended_sequence_candidate"] = str(sequence_payload["recommended_candidate"]["candidate_name"])
            updated["recommended_adoption_policy_variant"] = recommended_variant
            updated["primary_failure_axis"] = primary_failure_axis
            updated["disposition_reason"] = {
                "sequence_has_keep": bool(sequence_has_keep),
                "adoption_ok": bool(adoption_ok),
                "monitor_days_with_bonus_delta": int(days_with_bonus_delta),
                "primary_failure_axis": primary_failure_axis,
            }
        updated_candidates.append(updated)
    library_payload["pattern_candidates"] = updated_candidates
    library_payload["generated_at"] = _utc_now_iso()
    _write_json_artifact(library_path, library_payload)

    checkpoint_dir = dataset_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = checkpoint_dir / f"checkpoint_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_rebound_v3_round.md"
    next_single_step = (
        "- build the change plan to fix MeeMee live policy on the candidate rebound_onset adoption policy"
        if disposition == "keep_strengthened"
        else "- continue with fit score weight reallocation research for rebound_onset"
        if disposition == "keep"
        else "- search for another feature that expands sample count without breaking rebound_onset identity"
    )
    _write_markdown_report(
        checkpoint_path,
        [
            "# TRADEX Rebound v3 Checkpoint",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- disposition: `{disposition}`",
            "",
            "## Current State",
            f"- recommended_sequence_candidate: `{sequence_payload['recommended_candidate']['candidate_name']}`",
            f"- recommended_adoption_policy_variant: `{recommended_variant}`",
            f"- primary_failure_axis: `{primary_failure_axis}`",
            "",
            "## What Changed",
            f"- sequence_combo: `{workstream_results['sequence_combo']['pattern_sequence_combo_path']}`",
            f"- adoption_policy: `{workstream_results['adoption_policy']['pattern_adoption_policy_path']}`",
            f"- live_monitor: `{workstream_results['live_monitor']['rebound_live_monitor_path']}`",
            f"- toredex_policy_diagnosis: `{workstream_results['toredex_diagnosis']['toredex_policy_diagnosis_path']}`",
            "",
            "## Evidence",
            f"- sequence_has_keep: `{sequence_has_keep}`",
            f"- monitor_days_with_bonus_delta: `{days_with_bonus_delta}`",
            f"- candidate_days_with_bonus: `{live_monitor_payload['summary']['candidate']['days_with_bonus']}`",
            f"- baseline_days_with_bonus: `{live_monitor_payload['summary']['baseline']['days_with_bonus']}`",
            "",
            "## Decision",
            f"- rebound_onset disposition updated to `{disposition}`",
            "",
            "## Remaining Risks",
            "- monitor still compares preview ranking deltas, not a full production E2E run",
            "- sequence widening still keeps the core gate fixed, so sample growth is intentionally bounded",
            "",
            "## Next Single Step",
            next_single_step,
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "disposition_recommendation": disposition,
        "pattern_library_path": str(library_path),
        "checkpoint_path": str(checkpoint_path),
        "pattern_sequence_combo_path": str(Path(workstream_results["sequence_combo"]["pattern_sequence_combo_path"])),
        "pattern_adoption_policy_path": str(Path(workstream_results["adoption_policy"]["pattern_adoption_policy_path"])),
        "rebound_live_monitor_path": str(Path(workstream_results["live_monitor"]["rebound_live_monitor_path"])),
        "toredex_policy_diagnosis_path": str(Path(workstream_results["toredex_diagnosis"]["toredex_policy_diagnosis_path"])),
    }


def _ensure_rebound_compare_artifacts(
    *,
    dataset_id: str,
) -> None:
    dataset_dir = event_image_dataset_dir(dataset_id)
    regime_compare_path = dataset_dir / "regime_compare.json"
    if not regime_compare_path.exists():
        analyze_event_image_dataset_regime(dataset_id=dataset_id)
    for regime_tag in ("rebound_onset", "uptrend"):
        decomposition_path = dataset_dir / f"pattern_decomposition_{regime_tag}.json"
        if not decomposition_path.exists():
            decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    boundary_path = dataset_dir / "pattern_boundary_compare_rebound_onset_vs_uptrend.json"
    if not boundary_path.exists():
        build_event_image_pattern_boundary(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend", max_workers=2)
    gating_path = dataset_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
    if not gating_path.exists():
        build_event_image_pattern_gating(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    combo_path = dataset_dir / "pattern_combo_rules_rebound_onset_vs_uptrend.json"
    if not combo_path.exists():
        build_event_image_pattern_combo_rules(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    library_path = dataset_dir / "pattern_library_candidates.json"
    if not library_path.exists():
        build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=["rebound_onset", "uptrend"])


def build_event_image_rebound_robustness_compare(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
    reference_dataset_id: str | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    if str(pattern) != "rebound_onset":
        raise RuntimeError("robustness compare currently supports rebound_onset only")

    reference_dataset = str(reference_dataset_id or dataset_id)
    _ensure_rebound_compare_artifacts(dataset_id=reference_dataset)
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    regime_compare = read_json(dataset_dir / "regime_compare.json")
    reference_context = _load_rebound_adoption_context(dataset_id=reference_dataset)
    latest_context = _load_rebound_latest_feature_frame(dataset_id=dataset_id)
    core_gate_candidates = _build_rebound_core_gate_candidate_frame(
        feature_frame=latest_context["feature_frame"],
        gate_rule=reference_context["gate_rule"],
    )
    if core_gate_candidates.empty:
        raise RuntimeError("rebound robustness compare produced no core-gate candidates")

    asof_iso = _ymd_int_to_iso(int(latest_context["latest_asof"]))
    if asof_iso is None:
        raise RuntimeError(f"failed to convert latest as_of to iso date: {latest_context['latest_asof']}")
    variant_results: list[dict[str, Any]] = []
    for variant_name in ("v1_0_strict_bonus", "v1_1_live_bonus", "v1_2_soft_bonus"):
        threshold = _rebound_variant_name_to_threshold(variant_name)
        scored = _score_rebound_candidate_frame(
            candidate_frame=core_gate_candidates,
            lower_wick_median=reference_context["lower_wick_median"],
            bull_streak_median=reference_context["bull_streak_median"],
            narrow_range_median=reference_context["narrow_range_median"],
            threshold_for_bonus=threshold,
        )
        impact = _evaluate_rebound_ranking_impact(
            latest_frame=latest_context["latest_frame"],
            candidate_frame=scored,
            dataset_id=str(dataset_id),
            source_artifacts={
                "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
                "predictions": str(dataset_dir / "predictions.parquet"),
                "reference_pattern_library": str(event_image_dataset_dir(reference_dataset) / "pattern_library_candidates.json"),
                "reference_pattern_gating": str(event_image_dataset_dir(reference_dataset) / "pattern_gating_rule_rebound_onset_vs_uptrend.json"),
            },
            asof_iso=asof_iso,
            source_disposition="keep",
            variant_name=f"{variant_name}_{dataset_id}",
        )
        variant_results.append(
            {
                "variant_name": variant_name,
                "threshold_for_bonus": threshold,
                "tag_candidate_count": int(len(scored)),
                "bonus_candidate_count": int(scored["bonus_eligible"].sum()),
                **impact,
            }
        )
    adoption_compare_payload = {
        "recommended_variant": {
            "variant_name": max(
                variant_results,
                key=lambda row: (
                    1 if int(row["bonus_candidate_count"]) >= 1 else 0,
                    1 if 1 <= int(row["entry_score_rank_changed_count"]) <= 5 else 0,
                    -float(row["threshold_for_bonus"]),
                ),
            )["variant_name"]
        },
        "variant_results": variant_results,
    }
    regime_row = next((row for row in regime_compare["regime_summary"] if str(row.get("regime_tag")) == "rebound_onset"), None)
    if regime_row is None:
        raise RuntimeError(f"missing rebound_onset regime summary for dataset={dataset_id}")
    recommended_variant_name = str(adoption_compare_payload["recommended_variant"]["variant_name"])
    recommended_variant_row = next(
        (row for row in adoption_compare_payload["variant_results"] if str(row.get("variant_name")) == recommended_variant_name),
        None,
    )
    if recommended_variant_row is None:
        raise RuntimeError("missing recommended rebound adoption variant row")

    artifact = {
        "schema_version": ROBUSTNESS_COMPARE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "regime_compare": str(dataset_dir / "regime_compare.json"),
            "reference_dataset_id": reference_dataset,
            "reference_pattern_library": str(event_image_dataset_dir(reference_dataset) / "pattern_library_candidates.json"),
            "reference_pattern_gating": str(event_image_dataset_dir(reference_dataset) / "pattern_gating_rule_rebound_onset_vs_uptrend.json"),
        },
        "rebound_onset_winner_month_count": int(regime_row["month_count"]),
        "image_vs_numeric_accuracy_delta": float(regime_row["image_accuracy_mean"]) - float(regime_row["numeric_accuracy_mean"]),
        "monthly_top10_precision_up_delta": float(regime_row["image_top10_precision_mean"]) - float(regime_row["numeric_top10_precision_mean"]),
        "monthly_long_short_spread_delta": float(regime_row["image_long_short_spread_mean"]) - float(regime_row["numeric_long_short_spread_mean"]),
        "tag_candidate_count": int(recommended_variant_row["tag_candidate_count"]),
        "bonus_candidate_count": int(recommended_variant_row["bonus_candidate_count"]),
        "recommended_variant": recommended_variant_name,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "rebound_robustness_compare.json"
    md_path = target_dir / "rebound_robustness_compare.md"
    _write_json_artifact(json_path, artifact)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Robustness Compare",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- recommended_variant: `{recommended_variant_name}`",
            f"- winner_month_count: `{artifact['rebound_onset_winner_month_count']}`",
            "",
            "## Evidence",
            f"- accuracy_delta: `{artifact['image_vs_numeric_accuracy_delta']}`",
            f"- top10_precision_delta: `{artifact['monthly_top10_precision_up_delta']}`",
            f"- long_short_spread_delta: `{artifact['monthly_long_short_spread_delta']}`",
            f"- tag_candidate_count: `{artifact['tag_candidate_count']}`",
            f"- bonus_candidate_count: `{artifact['bonus_candidate_count']}`",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern": "rebound_onset",
        "rebound_robustness_compare_path": str(json_path),
        "rebound_robustness_compare_report_path": str(md_path),
    }


def _run_rebound_robustness_job(
    dataset_id: str,
    *,
    pattern: str,
    reference_dataset_id: str | None,
) -> dict[str, Any]:
    return build_event_image_rebound_robustness_compare(
        dataset_id=dataset_id,
        pattern=pattern,
        reference_dataset_id=reference_dataset_id,
    )


def run_event_image_dataset_robustness_batch(
    *,
    dataset_ids: list[str] | tuple[str, ...],
    pattern: str = "rebound_onset",
    reference_dataset_id: str | None = None,
    max_workers: int | None = None,
) -> dict[str, Any]:
    normalized_dataset_ids = [str(dataset_id) for dataset_id in dataset_ids if str(dataset_id).strip()]
    if not normalized_dataset_ids:
        raise RuntimeError("dataset_ids is empty")
    worker_count = int(max_workers) if max_workers is not None else max(1, min(len(normalized_dataset_ids), max(1, (os.cpu_count() or 2) - 1)))
    batch_id = f"rebound-robustness-batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    batch_dir = (event_image_dataset_dir(normalized_dataset_ids[0]).parent.parent / "batches" / batch_id).resolve()
    batch_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _run_rebound_robustness_job,
                dataset_id,
                pattern=str(pattern),
                reference_dataset_id=str(reference_dataset_id) if reference_dataset_id is not None else None,
            ): dataset_id
            for dataset_id in normalized_dataset_ids
        }
        for future in as_completed(future_map):
            dataset_id = future_map[future]
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append({"dataset_id": str(dataset_id), "error": repr(exc)})

    summary_rows: list[dict[str, Any]] = []
    for result in sorted(results, key=lambda item: item["dataset_id"]):
        payload = read_json(Path(result["rebound_robustness_compare_path"]))
        summary_rows.append(
            {
                "dataset_id": payload["dataset_id"],
                "rebound_onset_winner_month_count": payload["rebound_onset_winner_month_count"],
                "image_vs_numeric_accuracy_delta": payload["image_vs_numeric_accuracy_delta"],
                "monthly_top10_precision_up_delta": payload["monthly_top10_precision_up_delta"],
                "monthly_long_short_spread_delta": payload["monthly_long_short_spread_delta"],
                "tag_candidate_count": payload["tag_candidate_count"],
                "bonus_candidate_count": payload["bonus_candidate_count"],
                "recommended_variant": payload["recommended_variant"],
            }
        )

    summary = {
        "schema_version": ROBUSTNESS_BATCH_SCHEMA_VERSION,
        "batch_id": batch_id,
        "pattern": str(pattern),
        "formal_compare_scope": "common eligible subset on test split only",
        "dataset_ids": normalized_dataset_ids,
        "worker_count": int(worker_count),
        "results": summary_rows,
        "errors": errors,
        "generated_at": _utc_now_iso(),
    }
    json_path = batch_dir / "robustness_batch_summary.json"
    md_path = batch_dir / "robustness_batch_report.md"
    _write_json_artifact(json_path, summary)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Rebound Robustness Batch",
            "",
            "## Summary",
            f"- pattern: `{pattern}`",
            f"- datasets: `{normalized_dataset_ids}`",
            f"- worker_count: `{worker_count}`",
            f"- errors: `{len(errors)}`",
            "",
            "## Results",
            *[
                f"- `{row['dataset_id']}`: spread_delta=`{row['monthly_long_short_spread_delta']}`, bonus=`{row['bonus_candidate_count']}`"
                for row in summary_rows
            ],
        ],
    )
    return {
        "batch_id": batch_id,
        "batch_dir": str(batch_dir),
        "robustness_batch_summary_path": str(json_path),
        "dataset_count": len(normalized_dataset_ids),
        "error_count": len(errors),
    }


def run_event_image_dataset_rebound_multi_research_round(
    *,
    dataset_id: str,
    robustness_dataset_ids: list[str] | tuple[str, ...],
    max_workers: int = 2,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    if not (dataset_dir / "pattern_library_candidates.json").exists():
        build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=["rebound_onset", "uptrend"])

    workstream_results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        future_map = {
            executor.submit(build_event_image_pattern_adoption_compare, dataset_id=dataset_id, pattern="rebound_onset"): "adoption_compare",
            executor.submit(build_event_image_pattern_breadth, dataset_id=dataset_id, pattern="rebound_onset"): "pattern_breadth",
            executor.submit(
                run_event_image_dataset_robustness_batch,
                dataset_ids=list(robustness_dataset_ids),
                pattern="rebound_onset",
                reference_dataset_id=str(dataset_id),
                max_workers=2,
            ): "robustness_batch",
        }
        for future in as_completed(future_map):
            workstream_results[future_map[future]] = future.result()

    adoption_compare = read_json(Path(workstream_results["adoption_compare"]["pattern_adoption_compare_path"]))
    pattern_breadth = read_json(Path(workstream_results["pattern_breadth"]["pattern_breadth_path"]))
    robustness_summary = read_json(Path(workstream_results["robustness_batch"]["robustness_batch_summary_path"]))

    recommended_variant = str(adoption_compare["recommended_variant"]["variant_name"])
    breadth_has_keep = any(str(row.get("disposition")) == "keep" for row in pattern_breadth["candidate_results"])
    restricted_row = next((row for row in robustness_summary["results"] if str(row.get("dataset_id")) == str(dataset_id)), None)
    if restricted_row is None:
        raise RuntimeError(f"robustness summary missing dataset row for {dataset_id}")
    robustness_ok = (
        float(restricted_row["image_vs_numeric_accuracy_delta"]) > 0.0
        and float(restricted_row["monthly_top10_precision_up_delta"]) >= 0.0
        and float(restricted_row["monthly_long_short_spread_delta"]) > 0.0
    )
    adoption_row = next(
        (row for row in adoption_compare["variant_results"] if str(row.get("variant_name")) == recommended_variant),
        None,
    )
    if adoption_row is None:
        raise RuntimeError("recommended adoption variant missing from compare artifact")
    disposition = "hold"
    if recommended_variant in {"v1_1_live_bonus", "v1_2_soft_bonus"} and breadth_has_keep and robustness_ok:
        disposition = "keep_strengthened"
    elif int(adoption_row["bonus_candidate_count"]) >= 1 and (breadth_has_keep or robustness_ok):
        disposition = "keep"

    library_path = dataset_dir / "pattern_library_candidates.json"
    library_payload = read_json(library_path)
    updated_candidates: list[dict[str, Any]] = []
    for row in library_payload["pattern_candidates"]:
        updated = dict(row)
        if str(updated.get("regime_tag")) == "rebound_onset":
            updated["disposition"] = disposition
            updated["adoption_compare_artifact_path"] = str(Path(workstream_results["adoption_compare"]["pattern_adoption_compare_path"]))
            updated["pattern_breadth_artifact_path"] = str(Path(workstream_results["pattern_breadth"]["pattern_breadth_path"]))
            updated["robustness_artifact_paths"] = {
                "batch_summary": str(Path(workstream_results["robustness_batch"]["robustness_batch_summary_path"])),
                "datasets": [str(event_image_dataset_dir(str(dataset)).resolve() / "rebound_robustness_compare.json") for dataset in robustness_dataset_ids],
            }
            updated["recommended_adoption_variant"] = recommended_variant
            updated["recommended_breadth_candidate"] = str(pattern_breadth["recommended_candidate"])
            updated["disposition_reason"] = {
                "adoption_compare": recommended_variant,
                "breadth_has_keep": bool(breadth_has_keep),
                "robustness_ok": bool(robustness_ok),
            }
        updated_candidates.append(updated)
    library_payload["pattern_candidates"] = updated_candidates
    library_payload["generated_at"] = _utc_now_iso()
    _write_json_artifact(library_path, library_payload)

    checkpoint_dir = dataset_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = checkpoint_dir / f"checkpoint_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_multi_research_round.md"
    _write_markdown_report(
        checkpoint_path,
        [
            "# TRADEX Multi Research Checkpoint",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- disposition: `{disposition}`",
            "",
            "## Current State",
            f"- recommended_variant: `{recommended_variant}`",
            f"- recommended_breadth_candidate: `{pattern_breadth['recommended_candidate']}`",
            f"- robustness_ok: `{robustness_ok}`",
            "",
            "## What Changed",
            f"- adoption_compare: `{workstream_results['adoption_compare']['pattern_adoption_compare_path']}`",
            f"- pattern_breadth: `{workstream_results['pattern_breadth']['pattern_breadth_path']}`",
            f"- robustness_batch: `{workstream_results['robustness_batch']['robustness_batch_summary_path']}`",
            "",
            "## Evidence",
            f"- adoption bonus candidates: `{adoption_row['bonus_candidate_count']}`",
            f"- adoption changed_count: `{adoption_row['entry_score_rank_changed_count']}`",
            f"- breadth keep count: `{sum(1 for row in pattern_breadth['candidate_results'] if str(row.get('disposition')) == 'keep')}`",
            f"- restricted accuracy delta: `{restricted_row['image_vs_numeric_accuracy_delta']}`",
            f"- restricted spread delta: `{restricted_row['monthly_long_short_spread_delta']}`",
            "",
            "## Decision",
            f"- rebound_onset disposition updated to `{disposition}`",
            "",
            "## Remaining Risks",
            "- bonus impact is still evaluated on preview ranking items, not the full live ranking stack",
            "- breadth candidates keep the core gate fixed, so sample growth remains intentionally constrained",
            "",
            "## Next Single Step",
            (
                "- fix the MeeMee auxiliary adoption policy and add a live ranking delta monitoring artifact"
                if disposition == "keep_strengthened"
                else "- continue with fit score weight reallocation research for rebound_onset"
                if disposition == "keep"
                else "- search for another feature that expands sample count without breaking rebound_onset identity"
            ),
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "disposition_recommendation": disposition,
        "pattern_library_path": str(library_path),
        "checkpoint_path": str(checkpoint_path),
        "adoption_compare_path": str(Path(workstream_results["adoption_compare"]["pattern_adoption_compare_path"])),
        "pattern_breadth_path": str(Path(workstream_results["pattern_breadth"]["pattern_breadth_path"])),
        "robustness_batch_summary_path": str(Path(workstream_results["robustness_batch"]["robustness_batch_summary_path"])),
    }


def build_event_image_pattern_library(
    *,
    dataset_id: str,
    regime_tags: list[str] | tuple[str, ...],
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    regime_compare = read_json(dataset_dir / "regime_compare.json")
    train_eval_manifest = read_json(dataset_dir / "train_eval_manifest.json")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    boundary_artifact_path = target_dir / "pattern_boundary_compare_rebound_onset_vs_uptrend.json"
    boundary_payload = read_json(boundary_artifact_path) if boundary_artifact_path.exists() else None
    gating_artifact_path = target_dir / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
    gating_payload = read_json(gating_artifact_path) if gating_artifact_path.exists() else None
    combo_artifact_path = target_dir / "pattern_combo_rules_rebound_onset_vs_uptrend.json"
    combo_payload = read_json(combo_artifact_path) if combo_artifact_path.exists() else None
    candidate_rows: list[dict[str, Any]] = []
    for regime_tag in [str(value) for value in regime_tags if str(value).strip()]:
        path = dataset_dir / f"pattern_decomposition_{regime_tag}.json"
        if not path.exists():
            raise RuntimeError(f"missing pattern decomposition artifact: {path}")
        artifact = read_json(path)
        regime_row = next((row for row in regime_compare["regime_summary"] if str(row["regime_tag"]) == regime_tag), None)
        if regime_row is None:
            raise RuntimeError(f"missing regime summary for regime_tag={regime_tag}")
        recommended_disposition = None
        if boundary_payload is not None:
            recommended_disposition = boundary_payload["disposition_recommendation"].get(regime_tag)
        if gating_payload is not None:
            recommended_disposition = gating_payload["disposition_recommendation"].get(regime_tag, recommended_disposition)
        if combo_payload is not None:
            recommended_disposition = combo_payload["disposition_recommendation"].get(regime_tag, recommended_disposition)
        disposition = str(recommended_disposition or ("keep" if int(artifact["winner_month_count"]) >= 1 and float(regime_row["image_long_short_spread_mean"]) > float(regime_row["numeric_long_short_spread_mean"]) else "hold"))
        strong_features = list((artifact["candidate_rule"]["trend_strength"].keys()))
        weak_features = ["false_positive_overlap", "winner_month_count_small"]
        if boundary_payload is not None:
            if regime_tag == str(boundary_payload["primary_regime"]):
                strong_features = list(boundary_payload["primary_advantage_features"])
                weak_features = list(boundary_payload["comparison_advantage_features"])
            elif regime_tag == str(boundary_payload["comparison_regime"]):
                strong_features = list(boundary_payload["comparison_advantage_features"])
                weak_features = list(boundary_payload["primary_advantage_features"])
        adoption_conditions = artifact["candidate_rule"]
        non_adoption_conditions = {"avoid_if": weak_features}
        gate_conditions = None if gating_payload is None else gating_payload["candidate_gate_rule"].get(regime_tag)
        gate_hit_counts = None if gating_payload is None else gating_payload["gate_hit_counts"].get(regime_tag)
        recommended_combo_rule = None
        combo_rule_metrics = None
        if combo_payload is not None:
            combo_key = "recommended_primary_combo" if regime_tag == str(combo_payload["primary_regime"]) else ("recommended_comparison_combo" if regime_tag == str(combo_payload["comparison_regime"]) else None)
            if combo_key is not None:
                combo_bundle = combo_payload.get(combo_key)
                if isinstance(combo_bundle, dict):
                    recommended_combo_rule = combo_bundle.get("result", {}).get("conditions")
                    combo_rule_metrics = combo_bundle.get("result", {}).get("metric_deltas")
        candidate_rows.append(
            {
                "regime_tag": regime_tag,
                "regime_label": str(artifact["regime_label"]),
                "disposition": disposition,
                "disposition_reason": "combo_compare" if combo_payload is not None else ("gating_compare" if gating_payload is not None else ("boundary_compare" if boundary_payload is not None else "regime_summary_only")),
                "winner_month_count": int(artifact["winner_month_count"]),
                "image_long_short_spread_mean": float(regime_row["image_long_short_spread_mean"]),
                "numeric_long_short_spread_mean": float(regime_row["numeric_long_short_spread_mean"]),
                "image_accuracy_mean": float(regime_row["image_accuracy_mean"]),
                "numeric_accuracy_mean": float(regime_row["numeric_accuracy_mean"]),
                "image_top10_precision_mean": float(regime_row["image_top10_precision_mean"]),
                "numeric_top10_precision_mean": float(regime_row["numeric_top10_precision_mean"]),
                "candidate_rule": artifact["candidate_rule"],
                "pattern_artifact_path": str(path),
                "micro_feature_artifact_path": str(artifact.get("micro_feature_artifact_path")),
                "boundary_artifact_path": str(boundary_artifact_path) if boundary_payload is not None else None,
                "gating_artifact_path": str(gating_artifact_path) if gating_payload is not None else None,
                "combo_rule_artifact_path": str(combo_artifact_path) if combo_payload is not None else None,
                "strong_features": strong_features,
                "weak_features": weak_features,
                "adoption_conditions": adoption_conditions,
                "non_adoption_conditions": non_adoption_conditions,
                "gate_conditions": gate_conditions,
                "gate_hit_counts": gate_hit_counts,
                "recommended_combo_rule": recommended_combo_rule,
                "combo_rule_metrics": combo_rule_metrics,
            }
        )
    library_payload = {
        "schema_version": PATTERN_LIBRARY_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "source_artifacts": {
            "regime_compare": str(dataset_dir / "regime_compare.json"),
            "train_eval_manifest": str(dataset_dir / "train_eval_manifest.json"),
            "fidelity_compare": str(dataset_dir / "fidelity_compare.json"),
            "pattern_decompositions": [str(dataset_dir / f"pattern_decomposition_{row['regime_tag']}.json") for row in candidate_rows],
            "boundary_compare": None if boundary_payload is None else str(boundary_artifact_path),
            "gating_compare": None if gating_payload is None else str(gating_artifact_path),
            "combo_compare": None if combo_payload is None else str(combo_artifact_path),
        },
        "compare_contract": {
            "same_dataset": True,
            "same_split": True,
            "same_labels": True,
            "same_horizon": True,
            "same_universe": True,
            "same_sample_keys": bool(fidelity_compare.get("same_sample_keys", False)),
            "common_eligible_sample_count": int(fidelity_compare["common_eligible_sample_count"]),
            "dropped_by_warmup_v1_2_count": int(fidelity_compare["dropped_by_warmup_v1_2_count"]),
        },
        "dataset_contract": {
            "evaluation_bundle_id": str(train_eval_manifest["evaluation_bundle_id"]),
            "renderer_spec_id": str(train_eval_manifest["renderer_spec_id"]),
            "featureizer_spec_id": str(train_eval_manifest["featureizer_spec_id"]),
        },
        "pattern_candidates": candidate_rows,
        "generated_at": _utc_now_iso(),
    }
    json_path = target_dir / "pattern_library_candidates.json"
    md_path = target_dir / "pattern_library_candidates.md"
    _write_json_artifact(json_path, library_payload)
    _write_markdown_report(
        md_path,
        [
            "# TRADEX Pattern Library Candidates",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            f"- regime_tags: `{[row['regime_tag'] for row in candidate_rows]}`",
            "",
            "## Current State",
            f"- common eligible sample count: `{int(fidelity_compare['common_eligible_sample_count'])}`",
            f"- dropped by warmup: `{int(fidelity_compare['dropped_by_warmup_v1_2_count'])}`",
            "",
            "## What Changed",
            "- promoted regime-level pattern decompositions into the first restricted-universe pattern library candidates",
            "",
            "## Evidence",
            f"- candidates: `{candidate_rows}`",
            "",
            "## Decision",
            f"- candidate dispositions: `{[(row['regime_tag'], row['disposition']) for row in candidate_rows]}`",
            "",
            "## Remaining Risks",
            "- current candidate library is based on small month counts per regime",
            "- candlestick micro-features are still not explicit features in the artifact",
            "",
            "## Next Single Step",
            "- promote rebound_onset gating conditions into the next regime-gate artifact if uptrend stays weaker",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "pattern_library_path": str(json_path),
        "pattern_library_report_path": str(md_path),
    }


def analyze_event_image_dataset_regime(
    *,
    dataset_id: str,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    predictions = pd.read_parquet(dataset_dir / "predictions.parquet")
    fidelity_compare = read_json(dataset_dir / "fidelity_compare.json")
    test_frame = predictions.loc[predictions["split"] == "test"].copy()
    if test_frame.empty:
        raise RuntimeError("regime analysis requires non-empty test split predictions")

    month_rows: list[dict[str, Any]] = []
    for as_of_date, month_frame in test_frame.groupby("as_of_date", sort=True):
        regime = _classify_regime(month_frame)
        image_metrics = _month_top_bottom_metrics(month_frame, prob_column="image_pred_prob_up", pred_column="image_pred_label")
        numeric_metrics = _month_top_bottom_metrics(month_frame, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label")
        row = {
            "as_of_date": int(as_of_date),
            "sample_count": int(len(month_frame)),
            "regime_tag": regime["regime_tag"],
            "regime_label": regime["regime_label"],
            "breadth_score": regime["breadth_score"],
            "momentum_score": regime["momentum_score"],
            "volatility_score": regime["volatility_score"],
            "compression_score": regime["compression_score"],
            "rebound_score": regime["rebound_score"],
            "image_accuracy": image_metrics["accuracy"],
            "numeric_accuracy": numeric_metrics["accuracy"],
            "image_monthly_top10_precision_up": image_metrics["monthly_top10_precision_up"],
            "numeric_monthly_top10_precision_up": numeric_metrics["monthly_top10_precision_up"],
            "image_monthly_top10_mean_forward_return": image_metrics["monthly_top10_mean_forward_return"],
            "numeric_monthly_top10_mean_forward_return": numeric_metrics["monthly_top10_mean_forward_return"],
            "image_monthly_bottom10_mean_forward_return": image_metrics["monthly_bottom10_mean_forward_return"],
            "numeric_monthly_bottom10_mean_forward_return": numeric_metrics["monthly_bottom10_mean_forward_return"],
            "image_monthly_long_short_spread": image_metrics["monthly_long_short_spread"],
            "numeric_monthly_long_short_spread": numeric_metrics["monthly_long_short_spread"],
            "winner_accuracy": _winner(image_metrics["accuracy"], numeric_metrics["accuracy"]),
            "winner_top10_precision": _winner(image_metrics["monthly_top10_precision_up"], numeric_metrics["monthly_top10_precision_up"]),
            "winner_long_short_spread": _winner(image_metrics["monthly_long_short_spread"], numeric_metrics["monthly_long_short_spread"]),
        }
        month_rows.append(row)

    month_index = pd.DataFrame(month_rows).sort_values("as_of_date").reset_index(drop=True)
    if month_index.empty:
        raise RuntimeError("regime analysis produced no month-level rows")

    regime_summary_rows: list[dict[str, Any]] = []
    for regime_tag, regime_frame in month_index.groupby("regime_tag", sort=True):
        regime_summary_rows.append(
            {
                "regime_tag": str(regime_tag),
                "regime_label": str(regime_frame["regime_label"].iloc[0]),
                "month_count": int(len(regime_frame)),
                "image_accuracy_mean": float(regime_frame["image_accuracy"].mean()),
                "numeric_accuracy_mean": float(regime_frame["numeric_accuracy"].mean()),
                "image_top10_precision_mean": float(regime_frame["image_monthly_top10_precision_up"].mean()),
                "numeric_top10_precision_mean": float(regime_frame["numeric_monthly_top10_precision_up"].mean()),
                "image_long_short_spread_mean": float(regime_frame["image_monthly_long_short_spread"].mean()),
                "numeric_long_short_spread_mean": float(regime_frame["numeric_monthly_long_short_spread"].mean()),
                "image_accuracy_win_months": int((regime_frame["winner_accuracy"] == "image").sum()),
                "numeric_accuracy_win_months": int((regime_frame["winner_accuracy"] == "numeric").sum()),
                "tie_accuracy_months": int((regime_frame["winner_accuracy"] == "tie").sum()),
            }
        )
    regime_summary = sorted(regime_summary_rows, key=lambda item: item["regime_tag"])

    regime_gate_manifest = {
        "schema_version": REGIME_GATE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "source_artifacts": {
            "dataset_manifest": str(dataset_dir / "dataset_manifest.json"),
            "predictions": str(dataset_dir / "predictions.parquet"),
            "fidelity_compare": str(dataset_dir / "fidelity_compare.json"),
        },
        "formal_compare_scope": "common eligible subset on test split only",
        "regime_tags": REGIME_LABELS_JA,
        "classification_policy": {
            "kind": "rule_based_v1",
            "breadth_signal": "share(dist_ma60 > 0)",
            "momentum_signal": "mean(return_1m_pre)",
            "volatility_signal": "mean(realized_vol20)",
            "compression_signal": "mean(|dist_ma20|)+mean(|dist_ma60|)+mean(|position_from_60d_high|)",
            "rebound_signal": "positive momentum + below-high distance + elevated volatility",
        },
        "generated_at": _utc_now_iso(),
    }
    regime_compare = {
        "schema_version": REGIME_COMPARE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "formal_compare_scope": "common eligible subset on test split only",
        "common_eligible_sample_count": int(fidelity_compare["common_eligible_sample_count"]),
        "test_month_count": int(month_index["as_of_date"].nunique()),
        "regime_month_counts": month_index["regime_tag"].value_counts().sort_index().to_dict(),
        "regime_summary": regime_summary,
        "month_level_results": month_rows,
    }

    month_index_path = target_dir / "regime_month_index.parquet"
    regime_gate_path = target_dir / "regime_gate_manifest.json"
    regime_compare_path = target_dir / "regime_compare.json"
    regime_report_path = target_dir / "regime_report.md"
    month_index.to_parquet(month_index_path, index=False)
    _write_json_artifact(regime_gate_path, regime_gate_manifest)
    _write_json_artifact(regime_compare_path, regime_compare)
    _write_markdown_report(
        regime_report_path,
        [
            "# TRADEX Regime Gate Report",
            "",
            "## Summary",
            f"- dataset: `{dataset_id}`",
            "- compare scope: `common eligible subset on test split only`",
            "",
            "## Current State",
            f"- test month count: `{int(month_index['as_of_date'].nunique())}`",
            f"- common eligible sample count: `{int(fidelity_compare['common_eligible_sample_count'])}`",
            "",
            "## What Changed",
            "- built rule-based regime gate artifact for test-split month decomposition",
            "",
            "## Evidence",
            f"- regime month counts: `{regime_compare['regime_month_counts']}`",
            f"- regime summary: `{regime_summary}`",
            "",
            "## Decision",
            "- regime artifact generated; use this as the next research gate, not as a production signal",
            "",
            "## Remaining Risks",
            "- regime classifier is rule-based and intentionally simple",
            "- current month count is small, so regime conclusions are provisional",
            "",
            "## Next Single Step",
            "- restricted-universe 上で勝ち月と負け月を `trend_strength / setup_quality / tradability_risk` に分解する",
        ],
    )
    return {
        "dataset_id": str(dataset_id),
        "regime_gate_manifest_path": str(regime_gate_path),
        "regime_compare_path": str(regime_compare_path),
        "regime_month_index_path": str(month_index_path),
        "regime_report_path": str(regime_report_path),
    }


def _run_analysis_batch_job(
    dataset_id: str,
    *,
    refresh_train: bool,
    refresh_repro: bool,
    feature_size: int,
    seeds: list[int] | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"dataset_id": str(dataset_id)}
    if refresh_train:
        result["train"] = train_event_image_dataset(dataset_id=dataset_id, seed=int((seeds or [7])[0]), feature_size=int(feature_size))
    if refresh_repro:
        result["repro"] = run_event_image_dataset_repro(dataset_id=dataset_id, seeds=seeds, feature_size=int(feature_size))
    result["regime"] = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    return result


def run_event_image_dataset_analysis_batch(
    *,
    dataset_ids: list[str] | tuple[str, ...],
    max_workers: int | None = None,
    refresh_train: bool = False,
    refresh_repro: bool = False,
    feature_size: int = 48,
    seeds: list[int] | tuple[int, ...] | None = None,
) -> dict[str, Any]:
    normalized_dataset_ids = [str(dataset_id) for dataset_id in dataset_ids if str(dataset_id).strip()]
    if not normalized_dataset_ids:
        raise RuntimeError("dataset_ids is empty")
    worker_count = int(max_workers) if max_workers is not None else max(1, min(len(normalized_dataset_ids), max(1, (os.cpu_count() or 2) - 1)))
    batch_id = f"analysis-batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    batch_dir = (event_image_dataset_dir(normalized_dataset_ids[0]).parent.parent / "batches" / batch_id).resolve()
    batch_dir.mkdir(parents=True, exist_ok=True)

    batch_results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _run_analysis_batch_job,
                dataset_id,
                refresh_train=bool(refresh_train),
                refresh_repro=bool(refresh_repro),
                feature_size=int(feature_size),
                seeds=None if seeds is None else [int(seed) for seed in seeds],
            ): dataset_id
            for dataset_id in normalized_dataset_ids
        }
        for future in as_completed(future_map):
            dataset_id = future_map[future]
            try:
                batch_results.append(future.result())
            except Exception as exc:
                errors.append({"dataset_id": str(dataset_id), "error": repr(exc)})

    summary = {
        "schema_version": BATCH_SUMMARY_SCHEMA_VERSION,
        "batch_id": batch_id,
        "dataset_ids": normalized_dataset_ids,
        "worker_count": worker_count,
        "refresh_train": bool(refresh_train),
        "refresh_repro": bool(refresh_repro),
        "feature_size": int(feature_size),
        "seed_list": None if seeds is None else [int(seed) for seed in seeds],
        "results": sorted(batch_results, key=lambda item: item["dataset_id"]),
        "errors": errors,
        "generated_at": _utc_now_iso(),
    }
    summary_path = batch_dir / "analysis_batch_summary.json"
    _write_json_artifact(summary_path, summary)
    _write_markdown_report(
        batch_dir / "analysis_batch_report.md",
        [
            "# TRADEX Event Image Dataset Analysis Batch",
            "",
            "## Summary",
            f"- datasets: `{normalized_dataset_ids}`",
            f"- worker_count: `{worker_count}`",
            f"- errors: `{len(errors)}`",
            "",
            "## Results",
            *[f"- `{item['dataset_id']}`" for item in sorted(batch_results, key=lambda item: item["dataset_id"])],
            "",
            "## Remaining Risks",
            "- batch runner parallelizes independent dataset jobs only; compare invariants still live inside each dataset artifact",
            "",
            "## Next Single Step",
            "- use this batch runner for the next restricted-universe/regime rounds instead of serial execution",
        ],
    )
    return {
        "batch_id": batch_id,
        "batch_dir": str(batch_dir),
        "summary_path": str(summary_path),
        "dataset_count": len(normalized_dataset_ids),
        "error_count": len(errors),
    }
