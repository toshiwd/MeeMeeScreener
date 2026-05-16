from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1")
DEFAULT_CONTRACT_PATH = REPO_ROOT / "docs" / "contracts" / "tradex_iizuka_signal_expectancy_contract_v1.json"

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_input_resolution_v1"
COMPARE_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_compare_v1"
BREAKDOWN_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_breakdowns_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_no_lookahead_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_decision_v1"
COMPLETE_SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_v1_artifact_complete_v1"

CANDIDATE_ID = "monthly_C_pullback_end_reclaim7_v1"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "iizuka_signal_contract_snapshot.json",
    "iizuka_signal_rows.parquet",
    "iizuka_signal_expectancy_compare.json",
    "iizuka_signal_breakdowns.json",
    "iizuka_no_lookahead_audit.json",
    "iizuka_signal_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
SIGNAL_ROW_REQUIRED_FIELDS = [
    "observation_date",
    "decision_date",
    "execution_date",
    "ret5_horizon_date",
    "ret10_horizon_date",
    "ret20_horizon_date",
    "feature_window_start",
    "feature_window_end",
    "no_lookahead_valid",
    "signal_subtype",
    "execution_price_source",
]
CONFIRMATION_ONLY_FIELDS = {
    "next_day_high_breakout",
    "post_signal_continuation",
    "post_signal_confirmation",
    "confirmation_high_update",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _normalize_source_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "code": "symbol",
        "dt": "date",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "atr": "atr14",
    }
    out = frame.rename(columns={k: v for k, v in rename.items() if k in frame.columns}).copy()
    missing = [column for column in ("symbol", "date", "open", "high", "low", "close", "ma7", "ma20", "atr14") if column not in out.columns]
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")
    out["symbol"] = out["symbol"].astype(str)
    raw_dates = out["date"]
    numeric_dates = pd.to_numeric(raw_dates, errors="coerce")
    ymd_mask = numeric_dates.between(19000101, 21001231).fillna(False)
    parsed_dates = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")
    if ymd_mask.any():
        parsed_dates.loc[ymd_mask] = pd.to_datetime(numeric_dates.loc[ymd_mask].astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
    numeric_epoch_mask = parsed_dates.isna() & numeric_dates.notna()
    if numeric_epoch_mask.any():
        parsed_dates.loc[numeric_epoch_mask] = pd.to_datetime(numeric_dates.loc[numeric_epoch_mask], unit="s", errors="coerce")
    text_mask = parsed_dates.isna() & numeric_dates.isna()
    if text_mask.any():
        parsed_dates.loc[text_mask] = pd.to_datetime(raw_dates.loc[text_mask], errors="coerce")
    out["date"] = parsed_dates
    if out["date"].isna().any():
        raise ValueError("source rows contain unparseable date values")
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    for column in ("open", "high", "low", "close", "ma7", "ma20", "atr14", "volume"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    if "monthly_C_regime" not in out.columns:
        for candidate in ("monthly_regime_c", "monthly_context_no_lookahead", "monthly_C"):
            if candidate in out.columns:
                out["monthly_C_regime"] = out[candidate]
                break
    if "monthly_C_regime" not in out.columns:
        raise ValueError("source rows missing monthly_C_regime or compatible proxy column")
    out["monthly_C_regime"] = out["monthly_C_regime"].fillna(False).astype(bool)
    if "sector" not in out.columns:
        out["sector"] = "unknown"
    if "liquidity_bucket" not in out.columns:
        if "volume_bucket" in out.columns:
            out["liquidity_bucket"] = out["volume_bucket"].astype(str)
        elif "volume" in out.columns:
            out["liquidity_bucket"] = pd.qcut(out["volume"].rank(method="first"), q=min(4, max(1, len(out))), labels=False, duplicates="drop").astype(str)
        else:
            out["liquidity_bucket"] = "unknown"
    out = out.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    return out


def _body(frame: pd.DataFrame) -> pd.Series:
    return (frame["close"] - frame["open"]).abs()


def _range(frame: pd.DataFrame) -> pd.Series:
    return (frame["high"] - frame["low"]).clip(lower=0.0)


def _lower_wick(frame: pd.DataFrame) -> pd.Series:
    return (pd.concat([frame["open"], frame["close"]], axis=1).min(axis=1) - frame["low"]).clip(lower=0.0)


def _close_position_in_range(frame: pd.DataFrame) -> pd.Series:
    rng = _range(frame)
    return ((frame["close"] - frame["low"]) / rng.replace(0.0, np.nan)).fillna(0.0)


def _flag_lower_wick(frame: pd.DataFrame) -> pd.Series:
    body = _body(frame)
    lower = _lower_wick(frame)
    return (lower >= 1.2 * body) & (lower >= 0.35 * frame["atr14"]) & (_close_position_in_range(frame) >= 0.60)


def _flag_koma(frame: pd.DataFrame) -> pd.Series:
    rng = _range(frame)
    body_ratio = (_body(frame) / rng.replace(0.0, np.nan)).fillna(1.0)
    return (body_ratio <= 0.25) & (rng <= 0.90 * frame["atr14"])


def _horizontal_for_group(group: pd.DataFrame) -> pd.Series:
    result = pd.Series(False, index=group.index)
    closes = group["close"].to_numpy(dtype=float)
    lows = group["low"].to_numpy(dtype=float)
    atr = group["atr14"].to_numpy(dtype=float)
    for pos in range(len(group)):
        ok = False
        for window in range(3, 7):
            start = pos - window + 1
            if start < 0:
                continue
            close_range = np.nanmax(closes[start : pos + 1]) - np.nanmin(closes[start : pos + 1])
            start_low = lows[start]
            min_low = np.nanmin(lows[start : pos + 1])
            atr_value = atr[pos]
            if math.isfinite(close_range) and math.isfinite(start_low) and math.isfinite(min_low) and math.isfinite(atr_value):
                if close_range <= 1.0 * atr_value and min_low >= start_low - 0.3 * atr_value:
                    ok = True
                    break
        result.iloc[pos] = ok
    return result


def _setup_for_group(group: pd.DataFrame) -> pd.DataFrame:
    out = group.copy()
    below = (out["close"] < out["ma7"]).fillna(False).to_numpy(dtype=bool)
    above20 = (out["close"] > out["ma20"]).fillna(False).to_numpy(dtype=bool)
    setup_count = np.zeros(len(out), dtype=int)
    setup_all_above20 = np.zeros(len(out), dtype=bool)
    for pos in range(len(out)):
        count = 0
        idx = pos - 1
        while idx >= 0 and below[idx]:
            count += 1
            idx -= 1
        setup_count[pos] = count
        if count:
            start = pos - count
            setup_all_above20[pos] = bool(above20[start:pos].all())
    out["ma7_below_setup_count"] = setup_count
    out["ma7_below_2_to_3_setup"] = (out["ma7_below_setup_count"].between(2, 3)) & setup_all_above20
    return out


def _attach_signal_flags(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["body"] = _body(out)
    out["range"] = _range(out)
    out["lower_wick"] = _lower_wick(out)
    out["close_position_in_range"] = _close_position_in_range(out)
    out["lower_wick_flag"] = _flag_lower_wick(out)
    out["koma_flag"] = _flag_koma(out)
    out["horizontal_flag"] = pd.concat([_horizontal_for_group(group) for _, group in out.groupby("symbol", sort=False)]).sort_index()
    out = pd.concat([_setup_for_group(group) for _, group in out.groupby("symbol", sort=False)], ignore_index=True)
    out["daily_above_ma20"] = (out["close"] > out["ma20"]) & (out["close"] >= out["ma20"] + 0.0 * out["atr14"])
    out["reclaim7"] = out["ma7_below_2_to_3_setup"] & (out["close"] > out["ma7"]) & (out["close"] >= out["ma7"] + 0.1 * out["atr14"])
    out["shape_or"] = out["lower_wick_flag"] | out["koma_flag"] | out["horizontal_flag"]
    out["main_signal"] = out["monthly_C_regime"] & out["daily_above_ma20"] & out["shape_or"] & out["reclaim7"]
    out["signal_subtype"] = out.apply(_subtype_for_row, axis=1)
    return out


def _subtype_for_row(row: pd.Series) -> str | None:
    flags = [
        ("lower_wick_reclaim7", bool(row.get("lower_wick_flag"))),
        ("koma_reclaim7", bool(row.get("koma_flag"))),
        ("horizontal_reclaim7", bool(row.get("horizontal_flag"))),
    ]
    active = [label for label, enabled in flags if enabled]
    if len(active) > 1:
        return "mixed_reclaim7"
    return active[0] if active else None


def _attach_forward_outcomes(frame: pd.DataFrame) -> pd.DataFrame:
    pieces = []
    for _, group in frame.groupby("symbol", sort=False):
        g = group.sort_values("date", kind="stable").copy().reset_index(drop=True)
        g["execution_date"] = g["date"].shift(-1)
        g["execution_price"] = g["open"].shift(-1)
        for horizon in (5, 10, 20):
            pos = horizon + 1
            g[f"ret{horizon}_horizon_date"] = g["date"].shift(-pos)
            g[f"ret{horizon}"] = (g["close"].shift(-pos) / g["execution_price"]) - 1.0
        lows = g["low"].to_numpy(dtype=float)
        execution = g["execution_price"].to_numpy(dtype=float)
        mae = []
        for pos in range(len(g)):
            low_window = lows[pos + 1 : pos + 21]
            if len(low_window) < 20 or not math.isfinite(execution[pos]) or execution[pos] == 0:
                mae.append(np.nan)
            else:
                mae.append(float(np.nanmin(low_window) / execution[pos] - 1.0))
        g["mae20"] = mae
        pieces.append(g)
    return pd.concat(pieces, ignore_index=True)


def _build_signal_rows(frame: pd.DataFrame) -> pd.DataFrame:
    out = _attach_forward_outcomes(_attach_signal_flags(frame))
    out["feature_window_start"] = out["date"]
    for _, group in out.groupby("symbol", sort=False):
        dates = group["date"].tolist()
        for pos, index in enumerate(group.index):
            setup_count = int(out.at[index, "ma7_below_setup_count"] or 0)
            start_pos = max(0, pos - setup_count)
            out.at[index, "feature_window_start"] = dates[start_pos]
    signal = out.loc[out["main_signal"]].copy()
    signal["observation_date"] = signal["date"]
    signal["decision_date"] = signal["date"]
    signal["feature_window_end"] = signal["date"]
    signal["execution_price_source"] = "next_session_open"
    signal["candidate_id"] = CANDIDATE_ID
    signal["no_lookahead_valid"] = (
        signal["execution_date"].notna()
        & signal["ret20_horizon_date"].notna()
        & signal["ma7_below_2_to_3_setup"].fillna(False).astype(bool)
        & signal["reclaim7"].fillna(False).astype(bool)
    )
    invalid_count = int((~signal["no_lookahead_valid"].fillna(False).astype(bool)).sum())
    valid = signal.loc[signal["no_lookahead_valid"].fillna(False).astype(bool)].copy().reset_index(drop=True)
    valid.attrs["raw_signal_row_count"] = int(len(signal))
    valid.attrs["excluded_no_lookahead_invalid_count"] = invalid_count
    return valid


def _eligible_rows(frame: pd.DataFrame, baseline_id: str) -> pd.DataFrame:
    if baseline_id == "baseline_1":
        return frame.copy()
    if baseline_id == "baseline_2":
        return frame.loc[frame["monthly_C_regime"] & frame["daily_above_ma20"]].copy()
    raise ValueError(f"unknown baseline_id: {baseline_id}")


def _sample_baseline_once(frame: pd.DataFrame, signal_rows: pd.DataFrame, baseline_id: str, seed: int, repetition: int) -> pd.DataFrame:
    eligible = _eligible_rows(frame, baseline_id)
    samples = []
    for decision_date, signal_group in signal_rows.groupby("decision_date", sort=True):
        pool = eligible.loc[eligible["date"].astype(str) == str(decision_date)].copy()
        if pool.empty:
            continue
        take = len(signal_group)
        replace = len(pool) < take
        random_state = int(seed + repetition * 1009 + sum(ord(ch) for ch in str(decision_date)))
        samples.append(pool.sample(n=take, replace=replace, random_state=random_state))
    if not samples:
        return eligible.head(0).copy()
    sampled = pd.concat(samples, ignore_index=True)
    sampled["baseline_id"] = baseline_id
    sampled["baseline_repetition"] = repetition
    return sampled


def _sample_baselines(frame: pd.DataFrame, signal_rows: pd.DataFrame, *, seed: int, repetitions: int) -> dict[str, pd.DataFrame]:
    return {
        baseline_id: pd.concat(
            [_sample_baseline_once(frame, signal_rows, baseline_id, seed, repetition) for repetition in range(repetitions)],
            ignore_index=True,
        )
        for baseline_id in ("baseline_1", "baseline_2")
    }


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"count": int(len(frame))}
    for horizon in (5, 10, 20):
        values = pd.to_numeric(frame.get(f"ret{horizon}", pd.Series(dtype=float)), errors="coerce").dropna()
        out[f"ret{horizon}_mean"] = float(values.mean()) if len(values) else None
        out[f"ret{horizon}_median"] = float(values.median()) if len(values) else None
        out[f"win_rate_{horizon}"] = float((values > 0).mean()) if len(values) else None
    mae = pd.to_numeric(frame.get("mae20", pd.Series(dtype=float)), errors="coerce").dropna()
    out["mae20_median"] = float(mae.median()) if len(mae) else None
    out["mae20_mean"] = float(mae.mean()) if len(mae) else None
    return out


def _repeated_baseline_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"repetitions": 0, "aggregate": _metrics(frame), "distribution": {}}
    per_rep = [_metrics(group) | {"baseline_repetition": int(rep)} for rep, group in frame.groupby("baseline_repetition", sort=True)]
    distribution: dict[str, Any] = {}
    metric_keys = [key for key in per_rep[0] if key != "baseline_repetition"]
    for key in metric_keys:
        series = pd.Series([item.get(key) for item in per_rep], dtype="float64").dropna()
        distribution[key] = {
            "mean": float(series.mean()) if len(series) else None,
            "median": float(series.median()) if len(series) else None,
            "p25": float(series.quantile(0.25)) if len(series) else None,
            "p75": float(series.quantile(0.75)) if len(series) else None,
        }
    return {"repetitions": int(frame["baseline_repetition"].nunique()), "aggregate": _metrics(frame), "distribution": distribution}


def _delta(signal_metrics: dict[str, Any], baseline_distribution: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in signal_metrics.items():
        if key == "count":
            continue
        base = baseline_distribution.get(key, {}).get("median")
        out[f"{key}_delta_vs_baseline_median"] = None if value is None or base is None else float(value - base)
    return out


def _largest_share(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    counts = frame[column].fillna("unknown").astype(str).value_counts()
    if counts.empty:
        return None
    return float(counts.iloc[0] / len(frame))


def _breakdown(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    if column not in frame.columns:
        return {"available": False, "groups": {}}
    groups = {}
    for key, group in frame.groupby(column, dropna=False, sort=True):
        groups[str(key)] = _metrics(group)
    return {"available": True, "groups": groups}


def _build_breakdowns(signal_rows: pd.DataFrame) -> dict[str, Any]:
    work = signal_rows.copy()
    work["year"] = work["decision_date"].astype(str).str.slice(0, 4)
    return {
        "schema_version": BREAKDOWN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "year": _breakdown(work, "year"),
        "monthly_regime": _breakdown(work, "monthly_C_regime"),
        "sector": _breakdown(work, "sector"),
        "liquidity_or_volume_bucket": _breakdown(work, "liquidity_bucket"),
        "subtype": _breakdown(work, "signal_subtype"),
        "concentration": {
            "largest_year_share": _largest_share(work, "year"),
            "largest_sector_share": _largest_share(work, "sector"),
        },
    }


def _build_compare(signal_rows: pd.DataFrame, baselines: dict[str, pd.DataFrame], *, research_fallback: bool) -> dict[str, Any]:
    signal_metrics = _metrics(signal_rows)
    baseline_payload = {baseline_id: _repeated_baseline_metrics(rows) for baseline_id, rows in baselines.items()}
    return {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "primary_baseline": "baseline_2",
        "research_fallback": research_fallback,
        "signal": signal_metrics,
        "baselines": baseline_payload,
        "deltas": {
            baseline_id: _delta(signal_metrics, payload["distribution"])
            for baseline_id, payload in baseline_payload.items()
        },
    }


def _build_no_lookahead_audit(signal_rows: pd.DataFrame, source_columns: list[str]) -> dict[str, Any]:
    missing_fields = [field for field in SIGNAL_ROW_REQUIRED_FIELDS if field not in signal_rows.columns]
    confirmation_overlap = sorted(CONFIRMATION_ONLY_FIELDS.intersection(source_columns))
    invalid_count = int((~signal_rows.get("no_lookahead_valid", pd.Series(False, index=signal_rows.index)).fillna(False).astype(bool)).sum())
    excluded_invalid_count = int(signal_rows.attrs.get("excluded_no_lookahead_invalid_count", 0))
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "pass": not missing_fields and invalid_count == 0,
        "signal_row_count": int(len(signal_rows)),
        "raw_signal_row_count": int(signal_rows.attrs.get("raw_signal_row_count", len(signal_rows))),
        "excluded_no_lookahead_invalid_count": excluded_invalid_count,
        "missing_required_signal_fields": missing_fields,
        "no_lookahead_invalid_count": invalid_count,
        "confirmation_only_columns_present_in_source": confirmation_overlap,
        "confirmation_only_columns_used_in_signal": [],
        "decision_trigger": "reclaim7",
        "pre_decisive_setup": "ma7_below_2_to_3_setup",
        "execution_price_source": "next_session_open",
    }


def _worse_mae_ratio(signal_mae: float | None, baseline_mae: float | None) -> float | None:
    if signal_mae is None or baseline_mae is None or baseline_mae == 0:
        return None
    return abs(signal_mae) / abs(baseline_mae)


def _build_decision(compare: dict[str, Any], breakdowns: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    signal = compare["signal"]
    b2 = compare["baselines"]["baseline_2"]["distribution"]
    gates = {
        "signal_count_ge_100": signal["count"] >= 100,
        "largest_year_share_le_0_40": (breakdowns["concentration"]["largest_year_share"] is not None and breakdowns["concentration"]["largest_year_share"] <= 0.40),
        "largest_sector_share_le_0_40": (breakdowns["concentration"]["largest_sector_share"] is not None and breakdowns["concentration"]["largest_sector_share"] <= 0.40),
        "no_lookahead_valid": bool(audit["pass"]),
    }
    mae_ratio = _worse_mae_ratio(signal.get("mae20_median"), b2.get("mae20_median", {}).get("median"))
    gates["mae_median_not_worse_than_1_2x_baseline_2"] = mae_ratio is not None and mae_ratio <= 1.2
    beats = {
        "ret20_mean_beats_baseline_2": signal.get("ret20_mean") is not None and b2.get("ret20_mean", {}).get("median") is not None and signal["ret20_mean"] > b2["ret20_mean"]["median"],
        "ret20_median_beats_baseline_2": signal.get("ret20_median") is not None and b2.get("ret20_median", {}).get("median") is not None and signal["ret20_median"] > b2["ret20_median"]["median"],
        "win_rate_20_beats_baseline_2": signal.get("win_rate_20") is not None and b2.get("win_rate_20", {}).get("median") is not None and signal["win_rate_20"] > b2["win_rate_20"]["median"],
    }
    if all(gates.values()) and all(beats.values()):
        decision = "keep"
        reason = "beats_primary_baseline_and_keep_gates_pass"
    elif signal["count"] == 0 or not audit["pass"]:
        decision = "drop"
        reason = "no_valid_signal_rows_or_no_lookahead_failed"
    elif any(beats.values()):
        decision = "hold"
        reason = "partial_primary_baseline_improvement_or_keep_gate_blocked"
    elif signal["count"] > 0:
        decision = "analysis_only"
        reason = "signal_rows_exist_but_primary_baseline_not_beaten"
    else:
        decision = "drop"
        reason = "no_signal_rows"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "primary_baseline": "baseline_2",
        "research_fallback": bool(compare["research_fallback"]),
        "keep_gates": gates,
        "primary_baseline_beats": beats,
        "mae_median_worse_ratio_vs_baseline_2": mae_ratio,
        "non_goals": [
            "No MeeMee UI changes",
            "No MeeMee ranking changes",
            "No production ranking changes",
            "No publish registry mutation",
            "No sell-side validation",
            "No unknown pattern discovery",
        ],
    }


def run_signal_expectancy(
    *,
    source_rows_parquet: Path,
    output_root: Path,
    contract_path: Path = DEFAULT_CONTRACT_PATH,
    random_seed: int = 20260509,
    baseline_repetitions: int = 100,
    max_rows: int | None = None,
) -> dict[str, Any]:
    session_root = output_root / _session_id()
    source = pd.read_parquet(source_rows_parquet)
    if max_rows is not None:
        source = source.head(max_rows).copy()
    frame = _attach_forward_outcomes(_attach_signal_flags(_normalize_source_frame(source)))
    signal_rows = _build_signal_rows(_normalize_source_frame(source))
    repeated = max(1, int(baseline_repetitions))
    research_fallback = repeated < 100
    baselines = _sample_baselines(frame, signal_rows, seed=random_seed, repetitions=repeated)
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    compare = _build_compare(signal_rows, baselines, research_fallback=research_fallback)
    breakdowns = _build_breakdowns(signal_rows)
    audit = _build_no_lookahead_audit(signal_rows, list(source.columns))
    decision = _build_decision(compare, breakdowns, audit)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_root": str(session_root),
        "candidate_id": CANDIDATE_ID,
        "boundary": "TRADEX-only",
        "research_only": True,
        "random_seed": int(random_seed),
        "baseline_repetitions_requested": int(baseline_repetitions),
        "baseline_repetitions_used": repeated,
        "research_fallback": research_fallback,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_rows_parquet": str(source_rows_parquet),
        "contract_path": str(contract_path),
        "source_row_count": int(len(source)),
        "normalized_row_count": int(len(frame)),
        "signal_row_count": int(len(signal_rows)),
        "raw_signal_row_count": int(signal_rows.attrs.get("raw_signal_row_count", len(signal_rows))),
        "excluded_no_lookahead_invalid_count": int(signal_rows.attrs.get("excluded_no_lookahead_invalid_count", 0)),
        "required_columns": ["symbol", "date", "open", "high", "low", "close", "ma7", "ma20", "atr14", "monthly_C_regime"],
    }
    _write_json(session_root / "run_manifest.json", manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "iizuka_signal_contract_snapshot.json", contract)
    _write_parquet(session_root / "iizuka_signal_rows.parquet", signal_rows)
    _write_json(session_root / "iizuka_signal_expectancy_compare.json", compare)
    _write_json(session_root / "iizuka_signal_breakdowns.json", breakdowns)
    _write_json(session_root / "iizuka_no_lookahead_audit.json", audit)
    _write_json(session_root / "iizuka_signal_decision.json", decision)
    complete = {
        "schema_version": COMPLETE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_root": str(session_root),
        "decision": decision["authoritative_rollup_decision"],
        "signal_count": int(len(signal_rows)),
        "artifact_refs": {name: str(session_root / name) for name in REQUIRED_ARTIFACTS},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka Phase 0-1 signal expectancy validation")
    parser.add_argument("--source-rows-parquet", required=True)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--contract-path", default=str(DEFAULT_CONTRACT_PATH))
    parser.add_argument("--random-seed", type=int, default=20260509)
    parser.add_argument("--baseline-repetitions", type=int, default=100)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()
    result = run_signal_expectancy(
        source_rows_parquet=_safe_path(args.source_rows_parquet, Path(args.source_rows_parquet)),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        contract_path=_safe_path(args.contract_path, DEFAULT_CONTRACT_PATH),
        random_seed=args.random_seed,
        baseline_repetitions=args.baseline_repetitions,
        max_rows=args.max_rows,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
