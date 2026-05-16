from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from PIL import Image, ImageDraw

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_negative_guard_missing_feature_extraction_audit_v1 as negative_mod


AXIS_ID = "image_assisted_rerank_phase0_1"
SCHEMA_PREFIX = "tradex_image_assisted_rerank_phase0_1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_RISK_RUN_ID = "20260513T040000Z-selection-risk-control-for-wide-pool-v1"
DEFAULT_THRESHOLD_RUN_ID = "20260513T050000Z-threshold-no-trade-control-for-wide-pool-v1"
DEFAULT_FEATURE_DIAGNOSIS_RUN_ID = "20260513T060000Z-wide-pool-winner-nonwinner-feature-diagnosis-v1"
DEFAULT_NEGATIVE_GUARD_FEATURE_RUN_ID = "20260513T070000Z-negative-guard-missing-feature-extraction-audit-v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_RISK_ROOT = Path(r"G:\Tradex\selection_risk_control_for_wide_pool_v1")
DEFAULT_THRESHOLD_ROOT = Path(r"G:\Tradex\threshold_no_trade_control_for_wide_pool_v1")
DEFAULT_FEATURE_DIAGNOSIS_ROOT = Path(r"G:\Tradex\wide_pool_winner_nonwinner_feature_diagnosis_v1")
DEFAULT_NEGATIVE_GUARD_FEATURE_ROOT = Path(r"G:\Tradex\negative_guard_missing_feature_extraction_audit_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\image_assisted_rerank_phase0_1")

RANDOM_SEED = 20260513
WINDOW_TRADING_DAYS = 80
LABEL_HORIZON_TRADING_DAYS = 20
EMBARGO_DAYS = 20
IMAGE_SIZE = 224
SOURCE_CANDIDATE_SET = "wide_strength_pool_events"
SOURCE_SCORE_FAMILY_ID = negative_mod.BASE_SCORE_COLUMN

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "image_rerank_phase_contract.json",
    "candidate_pool_contract.json",
    "baseline_score_interface_audit.json",
    "ohlcv_source_audit.json",
    "image_renderer_contract.json",
    "image_manifest.jsonl",
    "image_rendering_summary.json",
    "renderer_determinism_report.json",
    "label_contract.json",
    "label_ledger.jsonl",
    "split_contract.json",
    "split_assignment_ledger.jsonl",
    "split_leakage_audit.json",
    "class_balance_report.json",
    "negative_guard_image_sample_report.json",
    "safe_full_image_sample_report.json",
    "phase2_readiness_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

FUTURE_LABEL_COLUMNS = {
    "ret20",
    "MFE20",
    "MAE20",
    "severe_loss20",
    "future_top15_by_ret20",
    "future_bottom15_by_ret20",
    "neutral_middle70",
    "future_top10_by_ret20",
    "future_top5_by_ret20",
    "big_winner_ret20_ge_10pct",
    "big_winner_MFE20_ge_15pct",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    return negative_mod._json_ready(value)


def _json_text(payload: Any) -> str:
    return negative_mod._json_text(payload)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return negative_mod._write_json(path, payload)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    return negative_mod._write_jsonl(path, rows)


def _load_json(path: Path) -> dict[str, Any]:
    return negative_mod._load_json(path)


def _stable_hash(payload: Any) -> str:
    return negative_mod._stable_hash(payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return negative_mod._safe_path(value, default)


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return negative_mod._run_dir(root, run_id, default_root)


def _safe_rate(count: int | float, total: int | float) -> float:
    return negative_mod._safe_rate(count, total)


def _file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_ref(axis_id: str, run_id: str, path: Path, files: tuple[str, ...] = ("_ARTIFACT_COMPLETE.json", "research_decision.json")) -> dict[str, Any]:
    return {
        "axis_id": axis_id,
        "run_id": run_id,
        "path": str(path),
        "exists": path.exists(),
        "file_hashes": {name: _file_hash(path / name) for name in files if (path / name).exists()},
    }


def validate_negative_guard_source(negative_guard_feature_dir: Path) -> dict[str, Any]:
    required = (
        "_ARTIFACT_COMPLETE.json",
        "research_decision.json",
        "candidate_feature_shortlist_v2.json",
        "previous_shortlist_retest_report.json",
        "feature_availability_audit.json",
        "leakage_audit.json",
        "next_axis_recommendation.json",
    )
    missing = [name for name in required if not (negative_guard_feature_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"negative guard feature source missing required artifacts: {missing} at {negative_guard_feature_dir}")
    payloads = {name: _load_json(negative_guard_feature_dir / name) for name in required}
    complete = payloads["_ARTIFACT_COMPLETE.json"]
    decision = payloads["research_decision.json"]
    shortlist = payloads["candidate_feature_shortlist_v2.json"]
    previous = payloads["previous_shortlist_retest_report.json"]
    availability = payloads["feature_availability_audit.json"]
    leakage = payloads["leakage_audit.json"]
    recommendation = payloads["next_axis_recommendation.json"]
    if complete.get("complete") is not True:
        raise RuntimeError("negative guard feature source artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError("negative guard feature source artifact used silent fallback")
    if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is not False:
        raise RuntimeError("negative guard feature source artifact used research fallback")
    if decision.get("authoritative_research_decision") != "negative_guard_feature_extraction_failed":
        raise RuntimeError("negative guard feature source decision is not negative_guard_feature_extraction_failed")
    if decision.get("decision") != "drop":
        raise RuntimeError("negative guard feature source decision is not drop")
    if int(shortlist.get("recommended_feature_count") or 0) != 0:
        raise RuntimeError("negative guard feature source already has recommended v2 features")
    if int(previous.get("previous_shortlist_recommended_for_v2_count") or 0) != 0:
        raise RuntimeError("previous shortlist source already has recommended v2 features")
    if int(availability.get("silently_imputed_feature_count") or 0) != 0:
        raise RuntimeError("negative guard feature source silently imputed features")
    if leakage.get("future_label_used_in_feature_inputs") is not False or leakage.get("future_label_used_in_score_inputs") is not False:
        raise RuntimeError("negative guard feature source leaked future labels")
    next_axis = recommendation.get("next_axis") or recommendation.get("recommended_next_axis") or recommendation.get("axis_moved")
    return {
        "source_negative_guard_decision": decision.get("authoritative_research_decision"),
        "new_negative_guard_feature_count": int(shortlist.get("new_negative_guard_feature_count") or 0),
        "recommended_feature_count": int(shortlist.get("recommended_feature_count") or 0),
        "previous_shortlist_feature_count": int(previous.get("previous_shortlist_feature_count") or 0),
        "previous_shortlist_recommended_for_v2_count": int(previous.get("previous_shortlist_recommended_for_v2_count") or 0),
        "usable_feature_count": int(availability.get("usable_feature_count") or 0),
        "unavailable_feature_count": int(availability.get("unavailable_feature_count") or 0),
        "silently_imputed_feature_count": int(availability.get("silently_imputed_feature_count") or 0),
        "leakage_safe": True,
        "next_axis_recommendation": next_axis,
    }


def load_source_artifacts(
    *,
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
    feature_diagnosis_dir: Path,
    negative_guard_feature_dir: Path,
) -> dict[str, Any]:
    negative_guard_status = validate_negative_guard_source(negative_guard_feature_dir)
    loaded = negative_mod.load_source_artifacts(pattern_dir, guard_dir, upside_dir, wide_dir, risk_dir, threshold_dir, feature_diagnosis_dir)
    events = negative_mod.add_negative_guard_groups(loaded["events"])
    events = add_image_label_columns(events)
    return {
        "events": events,
        "selected": loaded["selected"],
        "feature_json": loaded["feature_json"],
        "negative_guard_status": negative_guard_status,
    }


def add_image_label_columns(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    frame["event_date"] = frame["event_date"].astype(str).str.slice(0, 10)
    if "event_ymd" not in frame.columns:
        frame["event_ymd"] = frame["event_date"].str.replace("-", "", regex=False).astype(int)
    ret20 = pd.to_numeric(frame.get("ret20_fwd"), errors="coerce")
    frame["ret20"] = ret20
    frame["MFE20"] = pd.to_numeric(frame.get("mfe20"), errors="coerce")
    frame["MAE20"] = pd.to_numeric(frame.get("mae20"), errors="coerce")
    frame["label_available"] = ret20.notna()
    count_by_date = frame.groupby("event_date")["code"].transform("count").clip(lower=1)
    top_cut = (count_by_date.astype(float).mul(0.15).apply(math.ceil)).clip(lower=1)
    bottom_cut = top_cut.copy()
    descending_rank = ret20.groupby(frame["event_date"]).rank(method="first", ascending=False)
    ascending_rank = ret20.groupby(frame["event_date"]).rank(method="first", ascending=True)
    if "is_future_top15_by_ret20" in frame.columns:
        frame["future_top15_by_ret20"] = frame["is_future_top15_by_ret20"].astype(bool)
    else:
        frame["future_top15_by_ret20"] = frame["label_available"] & descending_rank.le(top_cut)
    frame["future_bottom15_by_ret20"] = frame["label_available"] & ascending_rank.le(bottom_cut)
    frame["neutral_middle70"] = frame["label_available"] & ~frame["future_top15_by_ret20"] & ~frame["future_bottom15_by_ret20"]
    frame["future_top10_by_ret20"] = frame.get("is_future_top10_by_ret20", False)
    frame["future_top5_by_ret20"] = frame.get("is_future_top5_by_ret20", False)
    frame["big_winner_ret20_ge_10pct"] = frame.get("is_big_winner_ret20_ge_10pct", ret20.ge(0.10)).astype(bool)
    frame["big_winner_MFE20_ge_15pct"] = frame.get("is_big_winner_MFE20_ge_15pct", frame["MFE20"].ge(0.15)).astype(bool)
    frame["primary_label"] = "label_unavailable"
    frame.loc[frame["future_top15_by_ret20"], "primary_label"] = "future_top15_by_ret20"
    frame.loc[frame["future_bottom15_by_ret20"], "primary_label"] = "future_bottom15_by_ret20"
    frame.loc[frame["neutral_middle70"], "primary_label"] = "neutral_middle70"
    frame["source_candidate_set"] = SOURCE_CANDIDATE_SET
    frame["source_score_family_id"] = SOURCE_SCORE_FAMILY_ID
    frame["candidate_event_key"] = frame.apply(
        lambda row: hashlib.sha256(
            f"{row['code']}|{row['event_ymd']}|{SOURCE_CANDIDATE_SET}|{SOURCE_SCORE_FAMILY_ID}".encode("utf-8")
        ).hexdigest()[:24],
        axis=1,
    )
    return frame


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def render_candlestick_volume_png(window: pd.DataFrame, *, image_size: int = IMAGE_SIZE) -> bytes:
    frame = window.sort_values("ymd").tail(WINDOW_TRADING_DAYS).copy()
    if frame.empty:
        raise ValueError("cannot render empty OHLCV window")
    image = Image.new("RGB", (image_size, image_size), (255, 255, 255))
    draw = ImageDraw.Draw(image)
    padding = 6
    price_top = padding
    price_bottom = int(image_size * 0.74)
    volume_top = price_bottom + 8
    volume_bottom = image_size - padding
    price_height = max(1, price_bottom - price_top)
    volume_height = max(1, volume_bottom - volume_top)
    max_high = max(_as_float(value) for value in frame["h"].tolist())
    min_low = min(_as_float(value) for value in frame["l"].tolist())
    if max_high <= min_low:
        max_high = min_low + 1.0
    max_volume = max(1.0, max(_as_float(value) for value in frame["v"].tolist()))
    usable_width = image_size - padding * 2
    step = usable_width / max(1, len(frame))
    body_width = max(1, int(step * 0.68))

    def y_price(price: Any) -> int:
        value = _as_float(price, min_low)
        scaled = (max_high - value) / (max_high - min_low)
        return int(round(price_top + scaled * price_height))

    for idx, row in enumerate(frame.itertuples(index=False)):
        open_price = _as_float(getattr(row, "o"))
        high_price = _as_float(getattr(row, "h"))
        low_price = _as_float(getattr(row, "l"))
        close_price = _as_float(getattr(row, "c"))
        volume = _as_float(getattr(row, "v"))
        x_center = int(round(padding + idx * step + step / 2.0))
        x0 = max(padding, x_center - body_width // 2)
        x1 = min(image_size - padding, max(x0 + 1, x_center + body_width // 2))
        y_open = y_price(open_price)
        y_close = y_price(close_price)
        y_high = y_price(high_price)
        y_low = y_price(low_price)
        up_day = close_price >= open_price
        candle_color = (25, 128, 74) if up_day else (196, 64, 55)
        wick_color = (48, 48, 48)
        draw.line((x_center, y_high, x_center, y_low), fill=wick_color, width=1)
        body_top = min(y_open, y_close)
        body_bottom = max(y_open, y_close)
        if body_bottom == body_top:
            body_bottom += 1
        draw.rectangle((x0, body_top, x1, body_bottom), fill=candle_color)
        volume_bar_height = int(round((volume / max_volume) * volume_height))
        volume_color = (139, 190, 161) if up_day else (214, 151, 146)
        draw.rectangle((x0, volume_bottom - volume_bar_height, x1, volume_bottom), fill=volume_color)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=False, compress_level=9)
    return buffer.getvalue()


def _load_ohlcv_for_events(pattern_dir: Path, events: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_db = negative_mod._source_db_from_pattern(pattern_dir)
    if source_db is None or not source_db.exists():
        return pd.DataFrame(), {
            "source_db": str(source_db) if source_db else None,
            "source_db_available": False,
            "ohlcv_load_status": "source_db_unavailable",
        }
    min_ymd = int(events["event_ymd"].min())
    max_ymd = int(events["event_ymd"].max())
    load_start = int((pd.to_datetime(str(min_ymd), format="%Y%m%d") - pd.Timedelta(days=430)).strftime("%Y%m%d"))
    codes = sorted(events["code"].astype(str).unique().tolist())
    daily = negative_mod._load_daily_rows(source_db, codes=codes, start_ymd=load_start, end_ymd=max_ymd)
    return daily, {
        "source_db": str(source_db),
        "source_db_available": True,
        "ohlcv_load_status": "ohlcv_from_source_db" if not daily.empty else "ohlcv_source_returned_no_rows",
        "requested_code_count": len(codes),
        "loaded_code_count": int(daily["code"].nunique()) if not daily.empty else 0,
        "loaded_row_count": int(len(daily)),
        "load_start_ymd": load_start,
        "load_end_ymd": max_ymd,
    }


def _window_lookup(daily: pd.DataFrame) -> dict[str, tuple[list[int], pd.DataFrame]]:
    if daily.empty:
        return {}
    lookup: dict[str, tuple[list[int], pd.DataFrame]] = {}
    for code, group in daily.sort_values(["code", "ymd"]).groupby("code", sort=False):
        compact = group.drop_duplicates("ymd", keep="last").sort_values("ymd").reset_index(drop=True)
        lookup[str(code)] = (compact["ymd"].astype(int).tolist(), compact)
    return lookup


def _get_event_window(lookup: dict[str, tuple[list[int], pd.DataFrame]], code: str, event_ymd: int) -> tuple[pd.DataFrame | None, str]:
    item = lookup.get(str(code))
    if item is None:
        return None, "missing_code_ohlcv"
    ymds, frame = item
    try:
        idx = ymds.index(int(event_ymd))
    except ValueError:
        return None, "missing_event_date_ohlcv"
    start = idx - WINDOW_TRADING_DAYS + 1
    if start < 0:
        return None, "short_ohlcv_window"
    return frame.iloc[start : idx + 1].copy(), "renderable"


def build_image_dataset(
    *,
    events: pd.DataFrame,
    daily: pd.DataFrame,
    output_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    image_root = output_dir / "images" / str(IMAGE_SIZE)
    lookup = _window_lookup(daily)
    manifest_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    hash_counts: Counter[str] = Counter()
    deterministic_pass_count = 0
    deterministic_check_count = 0
    for row in events.sort_values(["event_ymd", "code"]).itertuples(index=False):
        code = str(getattr(row, "code"))
        event_ymd = int(getattr(row, "event_ymd"))
        window, status = _get_event_window(lookup, code, event_ymd)
        status_counts[status] += 1
        if window is None:
            continue
        png_bytes = render_candlestick_volume_png(window)
        repeat_bytes = render_candlestick_volume_png(window)
        deterministic_check_count += 1
        if png_bytes == repeat_bytes:
            deterministic_pass_count += 1
        image_hash = hashlib.sha256(png_bytes).hexdigest()
        hash_counts[image_hash] += 1
        window_start_ymd = int(window["ymd"].iloc[0])
        window_end_ymd = int(window["ymd"].iloc[-1])
        image_sample_key = hashlib.sha256(
            f"{code}|{event_ymd}|{window_start_ymd}|{window_end_ymd}|{SOURCE_CANDIDATE_SET}|{SOURCE_SCORE_FAMILY_ID}".encode("utf-8")
        ).hexdigest()[:24]
        year_dir = image_root / str(event_ymd)[:4]
        image_path = year_dir / f"{event_ymd}_{code}_{image_sample_key}.png"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(png_bytes)
        manifest_rows.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_image_manifest_row_v1",
                "image_sample_key": image_sample_key,
                "candidate_event_key": getattr(row, "candidate_event_key"),
                "symbol": code,
                "code": code,
                "event_date": str(getattr(row, "event_date")),
                "event_ymd": event_ymd,
                "window_start_date": str(pd.to_datetime(str(window_start_ymd), format="%Y%m%d").date()),
                "window_start_ymd": window_start_ymd,
                "window_end_date": str(pd.to_datetime(str(window_end_ymd), format="%Y%m%d").date()),
                "window_end_ymd": window_end_ymd,
                "window_trading_day_count": int(len(window)),
                "source_candidate_set": SOURCE_CANDIDATE_SET,
                "source_score_family_id": SOURCE_SCORE_FAMILY_ID,
                "image_size": IMAGE_SIZE,
                "image_format": "PNG",
                "image_path": str(image_path),
                "image_sha256": image_hash,
                "safe_full_tag": bool(getattr(row, "guard_safe_full", False)),
                "negative_guard_matched": bool(getattr(row, "negative_guard_match", False)),
                "prior_research_score_available": not pd.isna(getattr(row, SOURCE_SCORE_FAMILY_ID, None)),
                "prior_risk_score_available": not pd.isna(getattr(row, "threshold_risk_value", None)),
            }
        )
    duplicate_image_hash_count = int(sum(count - 1 for count in hash_counts.values() if count > 1))
    summary = {
        "candidate_event_count": int(len(events)),
        "image_renderable_event_count": int(len(manifest_rows)),
        "image_renderable_event_rate": _safe_rate(len(manifest_rows), len(events)),
        "missing_ohlcv_window_count": int(status_counts["missing_code_ohlcv"] + status_counts["missing_event_date_ohlcv"]),
        "short_window_count": int(status_counts["short_ohlcv_window"]),
        "render_status_counts": dict(sorted(status_counts.items())),
        "deterministic_check_count": deterministic_check_count,
        "deterministic_hash_pass_count": deterministic_pass_count,
        "deterministic_hash_pass_rate": _safe_rate(deterministic_pass_count, deterministic_check_count),
        "duplicate_image_hash_count": duplicate_image_hash_count,
    }
    return manifest_rows, summary


def build_label_ledger(events: pd.DataFrame, manifest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    key_map = {str(row["candidate_event_key"]): str(row["image_sample_key"]) for row in manifest_rows}
    rows = []
    rendered = events[events["candidate_event_key"].isin(key_map)].copy()
    for row in rendered.sort_values(["event_ymd", "code"]).itertuples(index=False):
        candidate_event_key = str(getattr(row, "candidate_event_key"))
        rows.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_label_ledger_row_v1",
                "image_sample_key": key_map[candidate_event_key],
                "candidate_event_key": candidate_event_key,
                "symbol": str(getattr(row, "code")),
                "event_date": str(getattr(row, "event_date")),
                "event_ymd": int(getattr(row, "event_ymd")),
                "label_horizon_trading_days": LABEL_HORIZON_TRADING_DAYS,
                "primary_label": str(getattr(row, "primary_label")),
                "future_top15_by_ret20": bool(getattr(row, "future_top15_by_ret20")),
                "future_bottom15_by_ret20": bool(getattr(row, "future_bottom15_by_ret20")),
                "neutral_middle70": bool(getattr(row, "neutral_middle70")),
                "ret20": _as_float(getattr(row, "ret20"), default=math.nan),
                "MFE20": _as_float(getattr(row, "MFE20"), default=math.nan),
                "MAE20": _as_float(getattr(row, "MAE20"), default=math.nan),
                "severe_loss20": bool(getattr(row, "severe_loss20", False)),
                "future_top10_by_ret20": bool(getattr(row, "future_top10_by_ret20")),
                "future_top5_by_ret20": bool(getattr(row, "future_top5_by_ret20")),
                "big_winner_ret20_ge_10pct": bool(getattr(row, "big_winner_ret20_ge_10pct")),
                "big_winner_MFE20_ge_15pct": bool(getattr(row, "big_winner_MFE20_ge_15pct")),
                "labels_used_in_image_rendering": False,
                "labels_used_in_candidate_key": False,
            }
        )
    return rows


def assign_time_block_split(manifest_rows: list[dict[str, Any]], *, embargo_days: int = EMBARGO_DAYS) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    if not manifest_rows:
        return [], {
            "split_created": False,
            "reason": "no_rendered_samples",
            "embargo_days": embargo_days,
        }, {
            "split_leakage_audit_passed": False,
            "reason": "no_rendered_samples",
        }
    sample_frame = pd.DataFrame(manifest_rows)
    unique_dates = sorted(int(value) for value in sample_frame["event_ymd"].unique().tolist())
    date_index = {ymd: idx for idx, ymd in enumerate(unique_dates)}
    date_count = len(unique_dates)
    validation_start_idx = int(date_count * 0.60)
    test_start_idx = int(date_count * 0.80)
    validation_start_idx = min(max(validation_start_idx, 1), max(1, date_count - 2))
    test_start_idx = min(max(test_start_idx, validation_start_idx + 1), max(validation_start_idx + 1, date_count - 1))

    def split_for_date(ymd: int) -> tuple[str, str | None]:
        idx = date_index[int(ymd)]
        if idx < max(0, validation_start_idx - embargo_days):
            return "train", None
        if idx < validation_start_idx:
            return "embargo", "pre_validation_embargo"
        if validation_start_idx <= idx < max(validation_start_idx, test_start_idx - embargo_days):
            return "validation", None
        if idx < test_start_idx:
            return "embargo", "pre_test_embargo"
        return "test", None

    assignment_rows = []
    for row in manifest_rows:
        split, embargo_reason = split_for_date(int(row["event_ymd"]))
        assignment_rows.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_split_assignment_row_v1",
                "image_sample_key": row["image_sample_key"],
                "symbol": row["symbol"],
                "event_date": row["event_date"],
                "event_ymd": int(row["event_ymd"]),
                "split": split,
                "embargo_reason": embargo_reason,
                "source_candidate_set": row["source_candidate_set"],
                "source_score_family_id": row["source_score_family_id"],
                "negative_guard_matched": bool(row["negative_guard_matched"]),
                "safe_full_tag": bool(row["safe_full_tag"]),
            }
        )
    assigned = pd.DataFrame(assignment_rows)
    non_embargo = assigned[assigned["split"].isin(["train", "validation", "test"])]
    split_counts = {name: int((assigned["split"] == name).sum()) for name in ["train", "validation", "test", "embargo"]}
    same_date_splits = non_embargo.groupby("event_ymd")["split"].nunique() if len(non_embargo) else pd.Series(dtype=int)
    same_date_cross_split = bool((same_date_splits > 1).any())
    split_created = all(split_counts[name] > 0 for name in ["train", "validation", "test"])
    train_date_indexes = [date_index[int(value)] for value in non_embargo.loc[non_embargo["split"] == "train", "event_ymd"].unique()]
    validation_date_indexes = [date_index[int(value)] for value in non_embargo.loc[non_embargo["split"] == "validation", "event_ymd"].unique()]
    future_train_val_overlap = bool(train_date_indexes and max(train_date_indexes) + embargo_days >= validation_start_idx)
    future_val_test_overlap = bool(validation_date_indexes and max(validation_date_indexes) + embargo_days >= test_start_idx)
    audit_passed = split_created and not same_date_cross_split and not future_train_val_overlap and not future_val_test_overlap
    split_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_split_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_policy": "time_block_split_with_embargo",
        "split_by": "event_date",
        "random_split_used": False,
        "labels_used_for_split_assignment": False,
        "embargo_days": embargo_days,
        "date_count": date_count,
        "validation_start_date": str(pd.to_datetime(str(unique_dates[validation_start_idx]), format="%Y%m%d").date()),
        "test_start_date": str(pd.to_datetime(str(unique_dates[test_start_idx]), format="%Y%m%d").date()),
        "split_counts": split_counts,
        "split_created": split_created,
    }
    split_contract["contract_hash"] = _stable_hash(split_contract)
    leakage_audit = {
        "schema_version": f"{SCHEMA_PREFIX}_split_leakage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_leakage_audit_passed": audit_passed,
        "same_date_cross_split": same_date_cross_split,
        "date_only_split": True,
        "labels_used_for_split_assignment": False,
        "future_labels_used_in_image_rendering": False,
        "future_labels_used_in_candidate_key": False,
        "future_label_window_overlap_train_validation": future_train_val_overlap,
        "future_label_window_overlap_validation_test": future_val_test_overlap,
        "feature_window_crosses_prior_split_boundary": True,
        "past_only_feature_window_overlap_allowed": True,
        "embargo_days_applied": embargo_days,
        "research_fallback_used": False,
        "silent_fallback_used": False,
    }
    return assignment_rows, split_contract, leakage_audit


def _counts_by_split(frame: pd.DataFrame, mask: pd.Series | None = None) -> dict[str, int]:
    scoped = frame if mask is None else frame[mask]
    return {name: int((scoped["split"] == name).sum()) for name in ["train", "validation", "test", "embargo"]}


def build_class_balance_report(label_rows: list[dict[str, Any]], assignment_rows: list[dict[str, Any]]) -> dict[str, Any]:
    labels = pd.DataFrame(label_rows)
    assignments = pd.DataFrame(assignment_rows)
    if labels.empty or assignments.empty:
        return {
            "schema_version": f"{SCHEMA_PREFIX}_class_balance_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "class_balance_available": False,
            "class_balance_by_split": {},
        }
    frame = assignments.merge(labels[["image_sample_key", "primary_label"]], on="image_sample_key", how="left")
    by_split: dict[str, Any] = {}
    for split in ["train", "validation", "test"]:
        group = frame[frame["split"] == split]
        counts = group["primary_label"].value_counts(dropna=False).to_dict()
        by_split[split] = {
            "sample_count": int(len(group)),
            "future_top15_by_ret20": int(counts.get("future_top15_by_ret20", 0)),
            "future_bottom15_by_ret20": int(counts.get("future_bottom15_by_ret20", 0)),
            "neutral_middle70": int(counts.get("neutral_middle70", 0)),
            "label_unavailable": int(counts.get("label_unavailable", 0)),
            "class_rates": {str(label): _safe_rate(count, len(group)) for label, count in counts.items()},
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_class_balance_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "class_balance_available": True,
        "class_balance_by_split": by_split,
    }


def build_tag_sample_report(
    *,
    report_name: str,
    assignment_rows: list[dict[str, Any]],
    label_rows: list[dict[str, Any]],
    tag_column: str,
) -> dict[str, Any]:
    assignments = pd.DataFrame(assignment_rows)
    labels = pd.DataFrame(label_rows)
    if assignments.empty:
        return {
            "schema_version": f"{SCHEMA_PREFIX}_{report_name}_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "sample_count_by_split": {},
            "sample_exists_in_all_required_splits": False,
        }
    frame = assignments.merge(
        labels[
            [
                "image_sample_key",
                "future_top10_by_ret20",
                "future_top15_by_ret20",
                "future_bottom15_by_ret20",
                "severe_loss20",
                "big_winner_ret20_ge_10pct",
            ]
        ],
        on="image_sample_key",
        how="left",
    )
    tag_mask = frame[tag_column].astype(bool)
    counts = _counts_by_split(frame, tag_mask)
    required_counts = {key: counts[key] for key in ["train", "validation", "test"]}
    by_split: dict[str, Any] = {}
    for split in ["train", "validation", "test", "embargo"]:
        group = frame[tag_mask & frame["split"].eq(split)]
        by_split[split] = {
            "sample_count": int(len(group)),
            "future_top10_count": int(group["future_top10_by_ret20"].sum()) if len(group) else 0,
            "future_top15_count": int(group["future_top15_by_ret20"].sum()) if len(group) else 0,
            "future_bottom15_count": int(group["future_bottom15_by_ret20"].sum()) if len(group) else 0,
            "severe_loss_count": int(group["severe_loss20"].sum()) if len(group) else 0,
            "big_winner_ret20_ge_10pct_count": int(group["big_winner_ret20_ge_10pct"].sum()) if len(group) else 0,
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_{report_name}_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "sample_count_by_split": counts,
        "sample_detail_by_split": by_split,
        "sample_exists_in_all_required_splits": all(value > 0 for value in required_counts.values()),
    }


def build_baseline_score_interface_audit(events: pd.DataFrame) -> dict[str, Any]:
    prior_score_present = SOURCE_SCORE_FAMILY_ID in events.columns
    prior_risk_present = "threshold_risk_value" in events.columns
    prior_score_available = events[SOURCE_SCORE_FAMILY_ID].notna() if prior_score_present else pd.Series(False, index=events.index)
    prior_risk_available = events["threshold_risk_value"].notna() if prior_risk_present else pd.Series(False, index=events.index)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_baseline_score_interface_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "baseline_score_family_id": SOURCE_SCORE_FAMILY_ID,
        "source_candidate_set": SOURCE_CANDIDATE_SET,
        "prior_research_score_field": SOURCE_SCORE_FAMILY_ID,
        "prior_research_score_available": bool(prior_score_present),
        "prior_research_score_available_rate": float(prior_score_available.mean()) if len(events) else 0.0,
        "prior_risk_score_field": "threshold_risk_value",
        "prior_risk_score_available": bool(prior_risk_present),
        "prior_risk_score_available_rate": float(prior_risk_available.mean()) if len(events) else 0.0,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(
    *,
    run_id: str,
    output_dir: Path,
    events: pd.DataFrame,
    manifest_rows: list[dict[str, Any]],
    image_summary: dict[str, Any],
    ohlcv_meta: dict[str, Any],
    source_refs: dict[str, Any],
    split_contract: dict[str, Any],
    split_leakage_audit: dict[str, Any],
    class_balance_report: dict[str, Any],
    negative_guard_report: dict[str, Any],
    safe_full_report: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    rendered_count = len(manifest_rows)
    rendered_event_keys = {row["candidate_event_key"] for row in manifest_rows}
    label_available_count = int(events.loc[events["candidate_event_key"].isin(rendered_event_keys), "label_available"].sum())
    baseline_score_audit = build_baseline_score_interface_audit(events)
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": AXIS_ID,
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
        },
        "candidate_pool_policy": "wide pool fixed from prior research artifacts; safe_full and negative_guard remain tags only",
        "image_model_trained": False,
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "cost_slippage_ignored_by_user_intent": True,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    image_phase_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_image_rerank_phase_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "phase": "phase0_1",
        "phase0_scope": [
            "validate_ohlcv_source_availability",
            "validate_candidate_pool_availability",
            "validate_prior_research_score_availability",
            "lock_image_sample_key",
            "lock_time_block_split_with_embargo",
        ],
        "phase1_scope": [
            "render_day80_candlestick_volume_images",
            "write_image_manifest",
            "write_renderer_determinism_report",
        ],
        "image_model_trained": False,
        "fusion_reranker_created": False,
        "ready_target": "image_only_classifier_baseline_phase2",
    }
    image_phase_contract["contract_hash"] = _stable_hash(image_phase_contract)
    candidate_pool_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_pool_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_candidate_set": SOURCE_CANDIDATE_SET,
        "source_score_family_id": SOURCE_SCORE_FAMILY_ID,
        "candidate_event_count": int(len(events)),
        "candidate_day_count": int(events["event_date"].nunique()),
        "candidate_symbol_count": int(events["code"].nunique()),
        "image_sample_key_fields": [
            "symbol",
            "event_date",
            "window_start_date",
            "window_end_date",
            "source_candidate_set",
            "source_score_family_id",
        ],
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "negative_guard_matched_event_count": int(events["negative_guard_match"].sum()) if "negative_guard_match" in events.columns else 0,
        "safe_full_tag_event_count": int(events["guard_safe_full"].sum()) if "guard_safe_full" in events.columns else 0,
    }
    candidate_pool_contract["contract_hash"] = _stable_hash(candidate_pool_contract)
    renderer_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_image_renderer_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "window_trading_days": WINDOW_TRADING_DAYS,
        "content": "daily_candlestick_plus_volume",
        "axes": False,
        "tick_labels": False,
        "title": False,
        "symbol_name": False,
        "watermark": False,
        "image_size": f"{IMAGE_SIZE}x{IMAGE_SIZE}",
        "optional_image_size": None,
        "colors": "fixed_green_red_muted_volume",
        "background": "fixed_white",
        "dpi": "not_applicable_pil_pixel_renderer",
        "padding": 6,
        "price_scaling": "per_window_high_low_normalization",
        "volume_scaling": "per_window_volume_normalization",
        "output_format": "PNG",
        "deterministic_hash_required": True,
        "future_labels_used_in_image_rendering": False,
    }
    renderer_contract["contract_hash"] = _stable_hash(renderer_contract)
    label_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_label_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "label_horizon_trading_days": LABEL_HORIZON_TRADING_DAYS,
        "primary_target": ["future_top15_by_ret20", "future_bottom15_by_ret20", "neutral_middle70"],
        "secondary_evaluation_labels": [
            "ret20",
            "MFE20",
            "MAE20",
            "severe_loss20",
            "future_top10_by_ret20",
            "future_top5_by_ret20",
            "big_winner_ret20_ge_10pct",
            "big_winner_MFE20_ge_15pct",
        ],
        "labels_for_train_eval_artifact_creation_only": True,
        "labels_used_in_image_rendering": False,
        "labels_used_in_candidate_key": False,
        "labels_used_in_split_assignment": False,
        "label_coverage_rate": _safe_rate(label_available_count, rendered_count),
    }
    label_contract["contract_hash"] = _stable_hash(label_contract)
    ohlcv_audit = {
        "schema_version": f"{SCHEMA_PREFIX}_ohlcv_source_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        **ohlcv_meta,
        "candidate_event_count": int(len(events)),
        "ohlcv_window_trading_days": WINDOW_TRADING_DAYS,
        "image_renderable_event_count": int(image_summary["image_renderable_event_count"]),
        "ohlcv_coverage_rate": float(image_summary["image_renderable_event_rate"]),
        "missing_ohlcv_window_count": int(image_summary["missing_ohlcv_window_count"]),
        "short_window_count": int(image_summary["short_window_count"]),
        "silent_fallback_used": False,
    }
    renderer_determinism = {
        "schema_version": f"{SCHEMA_PREFIX}_renderer_determinism_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "deterministic_hash_pass_count": int(image_summary["deterministic_hash_pass_count"]),
        "deterministic_check_count": int(image_summary["deterministic_check_count"]),
        "deterministic_hash_pass_rate": float(image_summary["deterministic_hash_pass_rate"]),
        "renderer_deterministic": float(image_summary["deterministic_hash_pass_rate"]) == 1.0 and int(image_summary["deterministic_check_count"]) > 0,
        "duplicate_image_hash_count": int(image_summary["duplicate_image_hash_count"]),
    }
    train_count = int(split_contract.get("split_counts", {}).get("train", 0))
    validation_count = int(split_contract.get("split_counts", {}).get("validation", 0))
    test_count = int(split_contract.get("split_counts", {}).get("test", 0))
    negative_guard_all_splits = bool(negative_guard_report.get("sample_exists_in_all_required_splits"))
    class_balance_ready = bool(class_balance_report.get("class_balance_available"))
    phase2_gate_checks = {
        "image_renderable_event_rate_ge_95pct": float(image_summary["image_renderable_event_rate"]) >= 0.95,
        "deterministic_hash_pass_rate_100pct": float(image_summary["deterministic_hash_pass_rate"]) == 1.0 and int(image_summary["deterministic_check_count"]) > 0,
        "split_leakage_audit_passed": split_leakage_audit.get("split_leakage_audit_passed") is True,
        "train_validation_test_non_empty": train_count > 0 and validation_count > 0 and test_count > 0,
        "class_balance_reported": class_balance_ready,
        "negative_guard_samples_exist_all_required_splits": negative_guard_all_splits,
        "prior_research_score_interface_available_or_marked": "prior_research_score_available" in baseline_score_audit,
        "future_labels_not_used_in_image_rendering": True,
        "future_labels_not_used_in_candidate_key": True,
        "silent_fallback_used_false": True,
        "artifact_complete": True,
    }
    ready_for_phase2 = all(phase2_gate_checks.values())
    phase2_readiness = {
        "schema_version": f"{SCHEMA_PREFIX}_phase2_readiness_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "ready_for_phase2": ready_for_phase2,
        "gate_checks": phase2_gate_checks,
        "ohlcv_coverage_rate": float(image_summary["image_renderable_event_rate"]),
        "image_renderable_event_count": int(image_summary["image_renderable_event_count"]),
        "image_renderable_event_rate": float(image_summary["image_renderable_event_rate"]),
        "missing_ohlcv_window_count": int(image_summary["missing_ohlcv_window_count"]),
        "short_window_count": int(image_summary["short_window_count"]),
        "deterministic_hash_pass_rate": float(image_summary["deterministic_hash_pass_rate"]),
        "duplicate_image_hash_count": int(image_summary["duplicate_image_hash_count"]),
        "label_coverage_rate": float(label_contract["label_coverage_rate"]),
        "split_created": split_contract.get("split_created") is True,
        "split_leakage_audit_passed": split_leakage_audit.get("split_leakage_audit_passed") is True,
        "embargo_days_applied": split_leakage_audit.get("embargo_days_applied"),
        "train_sample_count": train_count,
        "validation_sample_count": validation_count,
        "test_sample_count": test_count,
        "class_balance_by_split": class_balance_report.get("class_balance_by_split", {}),
        "negative_guard_sample_count_by_split": negative_guard_report.get("sample_count_by_split", {}),
        "safe_full_sample_count_by_split": safe_full_report.get("sample_count_by_split", {}),
        "prior_score_available_rate": baseline_score_audit.get("prior_research_score_available_rate"),
    }
    typed_reasons: list[str] = []
    fatal = False
    if renderer_determinism["renderer_deterministic"] is not True:
        typed_reasons.append("renderer_determinism_failed")
        fatal = True
    if split_leakage_audit.get("split_leakage_audit_passed") is not True:
        typed_reasons.append("split_leakage_audit_failed")
        fatal = True
    if float(image_summary["image_renderable_event_rate"]) < 0.50:
        typed_reasons.append("image_coverage_too_low")
        fatal = True
    if float(image_summary["image_renderable_event_rate"]) < 0.95:
        typed_reasons.append("image_renderable_event_rate_below_95pct")
    if not negative_guard_all_splits:
        typed_reasons.append("negative_guard_samples_sparse_by_split")
    if not class_balance_ready:
        typed_reasons.append("class_balance_not_reported")
    if ready_for_phase2:
        decision = "keep_candidate"
        authoritative = "image_assisted_phase0_1_ready_for_phase2"
        typed_reasons.append("image_dataset_contract_ready_for_phase2")
    elif fatal:
        decision = "drop"
        authoritative = "image_assisted_phase0_1_failed"
    else:
        decision = "hold"
        authoritative = "image_assisted_phase0_1_hold"
    research_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "image_assisted_rerank_phase0_1",
        "boundary": "TRADEX-only",
        "axis_moved": "image_assisted_rerank_phase0_1",
        "source_negative_guard_decision": "negative_guard_feature_extraction_failed",
        "image_renderer_created": True,
        "image_dataset_contract_created": True,
        "image_label_contract_created": True,
        "image_split_contract_created": True,
        "image_model_trained": False,
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "yolo_used": False,
        "llm_used": False,
        "future_labels_used_for_label_contract_only": True,
        "future_labels_used_in_image_rendering": False,
        "future_labels_used_in_candidate_key": False,
        "split_leakage_audit_passed": split_leakage_audit.get("split_leakage_audit_passed") is True,
        "renderer_deterministic": renderer_determinism["renderer_deterministic"],
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": sorted(set(typed_reasons)),
    }
    run_manifest = contracts.build_run_manifest(
        session_id=run_id,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=source_refs["refs"],
        asof=_utc_now(),
        config={
            "axis_id": AXIS_ID,
            "window_trading_days": WINDOW_TRADING_DAYS,
            "label_horizon_trading_days": LABEL_HORIZON_TRADING_DAYS,
            "embargo_days": EMBARGO_DAYS,
            "image_size": IMAGE_SIZE,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={
            "start_date": str(events["event_date"].min()),
            "end_date": str(events["event_date"].max()),
            "event_count": int(len(events)),
        },
        horizon=f"{LABEL_HORIZON_TRADING_DAYS} trading days",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "run_id": run_id,
        "complete": True,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "missing_artifacts": [],
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
    }
    return {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "image_rerank_phase_contract.json": image_phase_contract,
        "candidate_pool_contract.json": candidate_pool_contract,
        "baseline_score_interface_audit.json": baseline_score_audit,
        "ohlcv_source_audit.json": ohlcv_audit,
        "image_renderer_contract.json": renderer_contract,
        "image_rendering_summary.json": {
            "schema_version": f"{SCHEMA_PREFIX}_image_rendering_summary_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            **image_summary,
        },
        "renderer_determinism_report.json": renderer_determinism,
        "label_contract.json": label_contract,
        "split_contract.json": split_contract,
        "split_leakage_audit.json": split_leakage_audit,
        "class_balance_report.json": class_balance_report,
        "negative_guard_image_sample_report.json": negative_guard_report,
        "safe_full_image_sample_report.json": safe_full_report,
        "phase2_readiness_report.json": phase2_readiness,
        "research_decision.json": research_decision,
        "_ARTIFACT_COMPLETE.json": complete,
    }


def run_image_assisted_rerank_phase0_1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_risk_run_id: str = DEFAULT_RISK_RUN_ID,
    source_threshold_run_id: str = DEFAULT_THRESHOLD_RUN_ID,
    source_feature_diagnosis_run_id: str = DEFAULT_FEATURE_DIAGNOSIS_RUN_ID,
    source_negative_guard_feature_run_id: str = DEFAULT_NEGATIVE_GUARD_FEATURE_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    risk_root: str | Path = DEFAULT_RISK_ROOT,
    threshold_root: str | Path = DEFAULT_THRESHOLD_ROOT,
    feature_diagnosis_root: str | Path = DEFAULT_FEATURE_DIAGNOSIS_ROOT,
    negative_guard_feature_root: str | Path = DEFAULT_NEGATIVE_GUARD_FEATURE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or _default_run_id()
    pattern_dir = _safe_path(pattern_root, DEFAULT_PATTERN_ROOT) / source_pattern_run_id
    guard_dir = _safe_path(guard_root, DEFAULT_GUARD_ROOT) / source_guard_run_id
    upside_dir = _safe_path(upside_root, DEFAULT_UPSIDE_ROOT) / source_upside_run_id
    wide_dir = _safe_path(wide_root, DEFAULT_WIDE_ROOT) / source_wide_run_id
    risk_dir = _safe_path(risk_root, DEFAULT_RISK_ROOT) / source_risk_run_id
    threshold_dir = _safe_path(threshold_root, DEFAULT_THRESHOLD_ROOT) / source_threshold_run_id
    feature_diagnosis_dir = _safe_path(feature_diagnosis_root, DEFAULT_FEATURE_DIAGNOSIS_ROOT) / source_feature_diagnosis_run_id
    negative_guard_feature_dir = _safe_path(negative_guard_feature_root, DEFAULT_NEGATIVE_GUARD_FEATURE_ROOT) / source_negative_guard_feature_run_id
    output_dir = _run_dir(output_root, run_id, DEFAULT_OUTPUT_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    loaded = load_source_artifacts(
        pattern_dir=pattern_dir,
        guard_dir=guard_dir,
        upside_dir=upside_dir,
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        threshold_dir=threshold_dir,
        feature_diagnosis_dir=feature_diagnosis_dir,
        negative_guard_feature_dir=negative_guard_feature_dir,
    )
    events = loaded["events"].copy()
    daily, ohlcv_meta = _load_ohlcv_for_events(pattern_dir, events)
    manifest_rows, image_summary = build_image_dataset(events=events, daily=daily, output_dir=output_dir)
    label_rows = build_label_ledger(events, manifest_rows)
    assignment_rows, split_contract, split_leakage_audit = assign_time_block_split(manifest_rows)
    class_balance_report = build_class_balance_report(label_rows, assignment_rows)
    negative_guard_report = build_tag_sample_report(
        report_name="negative_guard_image_sample_report",
        assignment_rows=assignment_rows,
        label_rows=label_rows,
        tag_column="negative_guard_matched",
    )
    safe_full_report = build_tag_sample_report(
        report_name="safe_full_image_sample_report",
        assignment_rows=assignment_rows,
        label_rows=label_rows,
        tag_column="safe_full_tag",
    )
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "refs": [
            _source_ref("pre_strength_pattern_mining_v1", source_pattern_run_id, pattern_dir),
            _source_ref("pre_strength_guard_validation_v1", source_guard_run_id, guard_dir),
            _source_ref("upside_capture_missed_winner_diagnosis_v1", source_upside_run_id, upside_dir),
            _source_ref("wide_strength_pool_upside_rerank_v1", source_wide_run_id, wide_dir),
            _source_ref("selection_risk_control_for_wide_pool_v1", source_risk_run_id, risk_dir),
            _source_ref("threshold_no_trade_control_for_wide_pool_v1", source_threshold_run_id, threshold_dir),
            _source_ref("wide_pool_winner_nonwinner_feature_diagnosis_v1", source_feature_diagnosis_run_id, feature_diagnosis_dir),
            _source_ref("negative_guard_missing_feature_extraction_audit_v1", source_negative_guard_feature_run_id, negative_guard_feature_dir),
        ],
        "negative_guard_source_status": loaded["negative_guard_status"],
    }
    artifacts = build_contract_artifacts(
        run_id=run_id,
        output_dir=output_dir,
        events=events,
        manifest_rows=manifest_rows,
        image_summary=image_summary,
        ohlcv_meta=ohlcv_meta,
        source_refs=source_refs,
        split_contract=split_contract,
        split_leakage_audit=split_leakage_audit,
        class_balance_report=class_balance_report,
        negative_guard_report=negative_guard_report,
        safe_full_report=safe_full_report,
    )
    _write_jsonl(output_dir / "image_manifest.jsonl", manifest_rows)
    _write_jsonl(output_dir / "label_ledger.jsonl", label_rows)
    _write_jsonl(output_dir / "split_assignment_ledger.jsonl", assignment_rows)
    for filename, payload in artifacts.items():
        _write_json(output_dir / filename, payload)
    missing = [name for name in REQUIRED_ARTIFACTS if not (output_dir / name).exists()]
    if missing:
        raise RuntimeError(f"artifact write incomplete: {missing}")
    return {
        "output_dir": str(output_dir),
        "run_id": run_id,
        "decision": artifacts["research_decision.json"]["decision"],
        "authoritative_research_decision": artifacts["research_decision.json"]["authoritative_research_decision"],
        "ready_for_phase2": artifacts["phase2_readiness_report.json"]["ready_for_phase2"],
        "image_renderable_event_rate": artifacts["phase2_readiness_report.json"]["image_renderable_event_rate"],
        "image_renderable_event_count": artifacts["phase2_readiness_report.json"]["image_renderable_event_count"],
        "split_leakage_audit_passed": artifacts["phase2_readiness_report.json"]["split_leakage_audit_passed"],
        "renderer_deterministic": artifacts["research_decision.json"]["renderer_deterministic"],
        "image_model_trained": False,
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX image-assisted rerank Phase0/1 foundation")
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-risk-run-id", default=DEFAULT_RISK_RUN_ID)
    parser.add_argument("--source-threshold-run-id", default=DEFAULT_THRESHOLD_RUN_ID)
    parser.add_argument("--source-feature-diagnosis-run-id", default=DEFAULT_FEATURE_DIAGNOSIS_RUN_ID)
    parser.add_argument("--source-negative-guard-feature-run-id", default=DEFAULT_NEGATIVE_GUARD_FEATURE_RUN_ID)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--risk-root", default=str(DEFAULT_RISK_ROOT))
    parser.add_argument("--threshold-root", default=str(DEFAULT_THRESHOLD_ROOT))
    parser.add_argument("--feature-diagnosis-root", default=str(DEFAULT_FEATURE_DIAGNOSIS_ROOT))
    parser.add_argument("--negative-guard-feature-root", default=str(DEFAULT_NEGATIVE_GUARD_FEATURE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_image_assisted_rerank_phase0_1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        source_upside_run_id=args.source_upside_run_id,
        source_wide_run_id=args.source_wide_run_id,
        source_risk_run_id=args.source_risk_run_id,
        source_threshold_run_id=args.source_threshold_run_id,
        source_feature_diagnosis_run_id=args.source_feature_diagnosis_run_id,
        source_negative_guard_feature_run_id=args.source_negative_guard_feature_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        upside_root=args.upside_root,
        wide_root=args.wide_root,
        risk_root=args.risk_root,
        threshold_root=args.threshold_root,
        feature_diagnosis_root=args.feature_diagnosis_root,
        negative_guard_feature_root=args.negative_guard_feature_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
