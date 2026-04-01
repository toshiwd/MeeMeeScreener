from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from PIL import Image

from external_analysis.contracts.paths import resolve_source_db_path
from external_analysis.event_image_dataset.paths import event_image_dataset_dir
from external_analysis.event_image_dataset.renderer import (
    CONTROL_EVALUATION_BUNDLE_ID,
    CONTROL_FEATUREIZER_SPEC_ID,
    CONTROL_LOOKBACK_DAYS,
    CONTROL_RENDERER_SPEC_ID,
    FIDELITY_DAILY_BARS,
    FIDELITY_DAILY_PRICE_VOLUME_SPLIT,
    FIDELITY_EVALUATION_BUNDLE_ID,
    FIDELITY_FEATUREIZER_SPEC_ID,
    FIDELITY_MONTHLY_BARS,
    FIDELITY_PANE_LAYOUT,
    FIDELITY_RENDERER_SPEC_ID,
    FIDELITY_WARMUP_CONTRACT,
    FIDELITY_WEEKLY_BARS,
    render_event_chart,
    strict_agg_available,
)
from external_analysis.event_image_dataset.storage import read_parquet_frame, safe_float, write_parquet_frame
from external_analysis.exporter.snapshot_status import EXPORT_SNAPSHOT_STATUS_COMPLETE, probe_export_snapshot_readiness
from external_analysis.exporter.source_reader import connect_source_db, source_table_exists
from external_analysis.image_rerank.artifacts import verify_roundtrip, write_json
from external_analysis.image_rerank.renderer import DEFAULT_PALETTE


DATASET_SCHEMA_VERSION = "tradex_event_image_dataset_v1_2"
DATASET_DIAGNOSTIC_SCHEMA_VERSION = "tradex_event_image_dataset_diagnostic_v1_2"
PREVIEW_SCHEMA_VERSION = "tradex_event_image_dataset_preview_v1_2"
RESTRICTED_UNIVERSE_SCHEMA_VERSION = "tradex_event_image_dataset_restricted_universe_v1"
NUMERIC_FEATURE_SPEC_ID = "monthly_event_numeric_day120_summary_v1"
MIN_HISTORY_DAYS = 250
RECENT_COMPLETENESS_DAYS = 60
TOP_N = 20
BOTTOM_N = 20
LABEL_HORIZON_MONTHS = 1
LIQUIDITY_BOTTOM_RATIO = 0.30
CONTROL_IMAGE_FEATURE_SIZE = 12
FIDELITY_IMAGE_FEATURE_SIZE = 48
PREVIEW_MONTH_POSITIONS = ("earliest", "median", "latest")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_month_key(value: str | int | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        digits = digits[:6]
    if len(digits) != 6:
        raise ValueError(f"unsupported month value: {value}")
    return int(digits)


def _series_stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"count": 0, "mean": None, "median": None, "std": None, "p10": None, "p90": None}
    arr = np.asarray(values, dtype=np.float64)
    return {
        "count": int(arr.size),
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "std": float(np.std(arr)),
        "p10": float(np.quantile(arr, 0.10)),
        "p90": float(np.quantile(arr, 0.90)),
    }


def _distribution_by_label(frame: pd.DataFrame, column: str) -> dict[str, dict[str, float | None]]:
    payload: dict[str, dict[str, float | None]] = {}
    for label in ("top20_up", "bottom20_down"):
        values = [float(value) for value in frame.loc[frame["label"] == label, column].dropna().tolist()]
        payload[label] = _series_stats(values)
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_artifact(path: Path, payload: dict[str, Any]) -> Path:
    write_json(path, payload)
    verify_roundtrip(path, payload)
    return path


def _load_daily_frame(export_db_path: str) -> pd.DataFrame:
    conn = duckdb.connect(str(export_db_path), read_only=True)
    try:
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
                i.ma20,
                i.ma60
            FROM bars_daily_export b
            LEFT JOIN indicator_daily_export i
              ON i.code = b.code AND i.trade_date = b.trade_date
            ORDER BY b.code, b.trade_date
            """
        ).df()
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError("bars_daily_export is empty")
    frame["code"] = frame["code"].astype(str)
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="raise").astype(int)
    for column in ("o", "h", "l", "c", "v", "ma20", "ma60"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["month_key"] = (frame["trade_date"] // 100).astype(int)
    frame["turnover"] = frame["c"] * frame["v"]
    frame["daily_return"] = frame.groupby("code")["c"].pct_change()
    ordered_trade_dates = sorted(frame["trade_date"].unique().tolist())
    frame["global_index"] = frame["trade_date"].map({trade_date: idx for idx, trade_date in enumerate(ordered_trade_dates)})
    frame["row_number"] = frame.groupby("code").cumcount()
    for window in (7, 20, 60, 100, 120, 200):
        frame[f"ma{window}"] = (
            frame.groupby("code")["c"].rolling(window=window, min_periods=window).mean().reset_index(level=0, drop=True)
        )
    frame["turnover20"] = (
        frame.groupby("code")["turnover"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    frame["return_1m_pre"] = frame.groupby("code")["c"].pct_change(periods=20)
    frame["volume_mean20"] = (
        frame.groupby("code")["v"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    frame["volume_mean20_prev"] = frame.groupby("code")["volume_mean20"].shift(20)
    frame["volume_change20"] = (frame["volume_mean20"] / frame["volume_mean20_prev"]) - 1.0
    frame["rolling_high60"] = (
        frame.groupby("code")["h"].rolling(window=60, min_periods=60).max().reset_index(level=0, drop=True)
    )
    frame["position_from_60d_high"] = (frame["c"] / frame["rolling_high60"]) - 1.0
    frame["realized_vol20"] = (
        frame.groupby("code")["daily_return"].rolling(window=20, min_periods=20).std().reset_index(level=0, drop=True)
    )
    frame["global_index_shift_249"] = frame.groupby("code")["global_index"].shift(MIN_HISTORY_DAYS - 1)
    frame["global_index_shift_59"] = frame.groupby("code")["global_index"].shift(RECENT_COMPLETENESS_DAYS - 1)
    frame["history_250_complete"] = (frame["row_number"] >= (MIN_HISTORY_DAYS - 1)) & (
        frame["global_index"] - frame["global_index_shift_249"] == (MIN_HISTORY_DAYS - 1)
    )
    frame["recent_60_complete"] = (frame["row_number"] >= (RECENT_COMPLETENESS_DAYS - 1)) & (
        frame["global_index"] - frame["global_index_shift_59"] == (RECENT_COMPLETENESS_DAYS - 1)
    )
    frame["control_ma_ready"] = frame["ma20"].notna() & frame["ma60"].notna() & frame["ma120"].notna()
    frame["fidelity_daily_ready"] = (
        frame["ma7"].notna() & frame["ma20"].notna() & frame["ma60"].notna() & frame["ma100"].notna() & frame["ma200"].notna()
    )
    frame["turnover_ready"] = frame["turnover20"].notna()
    return frame


def _load_code_metadata(source_db_path: str | None) -> pd.DataFrame:
    resolved_source = resolve_source_db_path(source_db_path)
    conn = connect_source_db(str(resolved_source))
    try:
        if not source_table_exists(conn, "industry_master"):
            return pd.DataFrame(columns=["code", "sector", "market"])
        frame = conn.execute(
            """
            SELECT
                code,
                sector33_name AS sector,
                market_code AS market
            FROM industry_master
            """
        ).df()
    finally:
        conn.close()
    if frame.empty:
        return pd.DataFrame(columns=["code", "sector", "market"])
    frame["code"] = frame["code"].astype(str)
    return frame[["code", "sector", "market"]]


def _load_restricted_universe(
    restricted_universe_path: str | Path,
    *,
    expected_month_keys: list[int],
) -> tuple[dict[int, set[str]], dict[str, Any]]:
    path = Path(restricted_universe_path).expanduser().resolve()
    if not path.exists():
        raise RuntimeError(f"restricted universe artifact not found: {path}")
    frame = read_parquet_frame(path)
    required_columns = {
        "month_key",
        "universe_name",
        "code",
        "source_name",
        "source_version",
        "source_uri",
        "file_sha256",
        "sample_seed",
        "sampling_policy",
        "month_coverage_start",
        "month_coverage_end",
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise RuntimeError(f"restricted universe artifact is missing required columns: {missing_columns}")
    if frame.empty:
        raise RuntimeError("restricted universe artifact is empty")

    normalized = frame.copy()
    normalized["month_key"] = normalized["month_key"].map(_normalize_month_key)
    normalized["month_coverage_start"] = normalized["month_coverage_start"].map(_normalize_month_key)
    normalized["month_coverage_end"] = normalized["month_coverage_end"].map(_normalize_month_key)
    normalized["code"] = normalized["code"].astype(str).str.strip()
    normalized["universe_name"] = normalized["universe_name"].astype(str).str.strip()
    normalized["source_name"] = normalized["source_name"].astype(str).str.strip()
    normalized["source_version"] = normalized["source_version"].astype(str).str.strip()
    normalized["source_uri"] = normalized["source_uri"].astype(str).str.strip()
    normalized["file_sha256"] = normalized["file_sha256"].astype(str).str.strip()
    normalized["sample_seed"] = pd.to_numeric(normalized["sample_seed"], errors="raise").astype(int)
    normalized["sampling_policy"] = normalized["sampling_policy"].astype(str).str.strip()

    unique_universe_names = sorted({value for value in normalized["universe_name"].tolist() if value})
    if len(unique_universe_names) != 1:
        raise RuntimeError(f"restricted universe artifact must contain exactly one universe_name, got: {unique_universe_names}")
    metadata_fields = (
        "source_name",
        "source_version",
        "source_uri",
        "file_sha256",
        "sample_seed",
        "sampling_policy",
        "month_coverage_start",
        "month_coverage_end",
    )
    metadata: dict[str, Any] = {}
    for field in metadata_fields:
        unique_values = sorted({value for value in normalized[field].tolist() if value not in (None, "")})
        if len(unique_values) != 1:
            raise RuntimeError(f"restricted universe artifact field '{field}' must be constant, got: {unique_values}")
        metadata[field] = unique_values[0]

    available_months = sorted({int(value) for value in normalized["month_key"].dropna().tolist()})
    missing_months = [int(month_key) for month_key in expected_month_keys if int(month_key) not in available_months]
    if missing_months:
        raise RuntimeError(f"restricted universe artifact does not cover all dataset months: missing={missing_months}")

    membership: dict[int, set[str]] = {}
    for month_key, month_frame in normalized.groupby("month_key", sort=True):
        membership[int(month_key)] = {str(code) for code in month_frame["code"].tolist() if str(code).strip()}

    manifest = {
        "schema_version": RESTRICTED_UNIVERSE_SCHEMA_VERSION,
        "artifact_path": str(path),
        "universe_name": unique_universe_names[0],
        "source_name": str(metadata["source_name"]),
        "source_version": str(metadata["source_version"]),
        "source_uri": str(metadata["source_uri"]),
        "file_sha256": _sha256_file(path),
        "source_file_sha256": str(metadata["file_sha256"]),
        "sample_seed": int(metadata["sample_seed"]),
        "sampling_policy": str(metadata["sampling_policy"]),
        "month_coverage_start": int(metadata["month_coverage_start"]),
        "month_coverage_end": int(metadata["month_coverage_end"]),
        "row_count": int(len(normalized)),
        "month_count": int(len(available_months)),
    }
    return membership, manifest


def _month_end_anchors(daily_frame: pd.DataFrame, *, start_month: int | None, end_month: int | None) -> list[dict[str, int]]:
    trade_dates = pd.DataFrame({"trade_date": sorted(daily_frame["trade_date"].unique().tolist())})
    trade_dates["month_key"] = (trade_dates["trade_date"] // 100).astype(int)
    grouped = trade_dates.groupby("month_key", as_index=False)["trade_date"].max().sort_values("month_key")
    if start_month is not None:
        grouped = grouped[grouped["month_key"] >= int(start_month)]
    if end_month is not None:
        grouped = grouped[grouped["month_key"] <= int(end_month)]
    rows = grouped.to_dict(orient="records")
    anchors: list[dict[str, int]] = []
    for idx in range(len(rows) - 1):
        anchors.append(
            {"month_key": int(rows[idx]["month_key"]), "as_of_date": int(rows[idx]["trade_date"]), "forward_end_date": int(rows[idx + 1]["trade_date"])}
        )
    return anchors


def _assign_chronological_splits(as_of_dates: list[int]) -> dict[int, str]:
    ordered = sorted({int(value) for value in as_of_dates})
    if not ordered:
        return {}
    total = len(ordered)
    train_end = max(1, int(math.floor(total * 0.70)))
    validation_end = max(train_end + 1, int(math.floor(total * 0.85)))
    if validation_end >= total:
        validation_end = max(train_end, total - 1)
    split_map: dict[int, str] = {}
    for idx, as_of_date in enumerate(ordered):
        split_map[int(as_of_date)] = "train" if idx < train_end else ("validation" if idx < validation_end else "test")
    return split_map


def _aggregate_period_frame(code_frame: pd.DataFrame, *, as_of_date: int, period: str) -> pd.DataFrame:
    frame = code_frame.loc[code_frame["trade_date"] <= int(as_of_date)].copy()
    if frame.empty:
        return frame
    dates = pd.to_datetime(frame["trade_date"].astype(str), format="%Y%m%d")
    frame["_period_key"] = dates.dt.strftime("%G%V" if period == "weekly" else "%Y%m")
    grouped = (
        frame.groupby("_period_key", sort=True)
        .agg(trade_date=("trade_date", "max"), o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), v=("v", "sum"))
        .reset_index(drop=True)
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    return grouped


def _append_mas(frame: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    enriched = frame.copy()
    for window in windows:
        enriched[f"ma{window}"] = enriched["c"].rolling(window=window, min_periods=window).mean() if not enriched.empty else np.nan
    return enriched


def _build_preview_artifacts(dataset_dir: Path, fidelity_samples: pd.DataFrame) -> tuple[Path, Path]:
    unique_months = sorted({int(value) for value in fidelity_samples["as_of_date"].tolist()})
    if not unique_months:
        raise RuntimeError("no fidelity samples available for preview")
    selected_months = list(dict.fromkeys([unique_months[0], unique_months[len(unique_months) // 2], unique_months[-1]]))
    preview_rows: list[dict[str, Any]] = []
    for as_of_date in selected_months:
        month_frame = fidelity_samples.loc[fidelity_samples["as_of_date"] == int(as_of_date)].copy()
        top_rows = month_frame.loc[month_frame["label"] == "top20_up"].sort_values(["forward_return_1m", "code"], ascending=[False, True]).head(2)
        bottom_rows = month_frame.loc[month_frame["label"] == "bottom20_down"].sort_values(["forward_return_1m", "code"], ascending=[True, True]).head(2)
        preview_rows.extend(top_rows.to_dict(orient="records"))
        preview_rows.extend(bottom_rows.to_dict(orient="records"))
    images = [Image.open(str(row["fidelity_image_path"])).convert("RGB") for row in preview_rows]
    if not images:
        raise RuntimeError("preview selection produced no images")
    tile_width, tile_height = images[0].size
    columns = 4
    rows = int(math.ceil(len(images) / columns))
    sheet = Image.new("RGB", (tile_width * columns, tile_height * rows), DEFAULT_PALETTE["background"])
    for idx, image in enumerate(images):
        sheet.paste(image, ((idx % columns) * tile_width, (idx // columns) * tile_height))
    preview_contact_sheet_path = dataset_dir / "preview_contact_sheet.png"
    preview_contact_sheet_path.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(preview_contact_sheet_path)
    preview_manifest = {
        "schema_version": PREVIEW_SCHEMA_VERSION,
        "selection_rule": {
            "selected_month_positions": list(PREVIEW_MONTH_POSITIONS),
            "top_selection": "forward_return_1m desc, code asc, head(2)",
            "bottom_selection": "forward_return_1m asc, code asc, head(2)",
        },
        "selected_months": [int(value) for value in selected_months],
        "selected_sample_keys": [str(row["sample_key"]) for row in preview_rows],
        "preview_contact_sheet_path": str(preview_contact_sheet_path),
    }
    preview_manifest_path = dataset_dir / "preview_manifest.json"
    _write_json_artifact(preview_manifest_path, preview_manifest)
    return preview_manifest_path, preview_contact_sheet_path


def _render_sample_images(*, sample_row: dict[str, Any], code_frame: pd.DataFrame, dataset_dir: Path) -> dict[str, Any]:
    code = str(sample_row["code"])
    as_of_date = int(sample_row["trade_date"])
    current_global_index = int(sample_row["global_index"])
    control_start_global_index = current_global_index - (CONTROL_LOOKBACK_DAYS - 1)
    control_window = code_frame.loc[
        (code_frame["global_index"] >= control_start_global_index) & (code_frame["global_index"] <= current_global_index)
    ].copy()
    if len(control_window) != CONTROL_LOOKBACK_DAYS:
        raise RuntimeError(f"control lookback window is not complete for {code} at {as_of_date}")
    if int(control_window["trade_date"].max()) > as_of_date:
        raise RuntimeError(f"render leakage detected for {code} at {as_of_date}")

    control_image_path = dataset_dir / "images" / CONTROL_EVALUATION_BUNDLE_ID / str(as_of_date) / str(sample_row["label"]) / f"{code}.png"
    control_info = render_event_chart(
        evaluation_bundle_id=CONTROL_EVALUATION_BUNDLE_ID,
        path=control_image_path,
        daily_bars=control_window[["o", "h", "l", "c", "v", "ma20", "ma60", "ma120"]].to_dict(orient="records"),
    )

    weekly_frame = _append_mas(_aggregate_period_frame(code_frame, as_of_date=as_of_date, period="weekly"), [10, 30, 60])
    monthly_frame = _append_mas(_aggregate_period_frame(code_frame, as_of_date=as_of_date, period="monthly"), [6, 12, 24])
    fidelity_ready = bool(
        len(weekly_frame) >= int(FIDELITY_WARMUP_CONTRACT["indicator_warmup_weeks"])
        and len(monthly_frame) >= int(FIDELITY_WARMUP_CONTRACT["indicator_warmup_months"])
        and bool(sample_row["fidelity_daily_ready"])
    )
    fidelity_exclusion_reason = None
    fidelity_image_path = None
    fidelity_image_sha256 = None
    if not fidelity_ready:
        fidelity_exclusion_reason = "warmup_insufficient"
    else:
        daily_window = control_window.tail(FIDELITY_DAILY_BARS).copy()
        weekly_window = weekly_frame.tail(FIDELITY_WEEKLY_BARS).copy()
        monthly_window = monthly_frame.tail(FIDELITY_MONTHLY_BARS).copy()
        fidelity_image_path = dataset_dir / "images" / FIDELITY_EVALUATION_BUNDLE_ID / str(as_of_date) / str(sample_row["label"]) / f"{code}.png"
        fidelity_info = render_event_chart(
            evaluation_bundle_id=FIDELITY_EVALUATION_BUNDLE_ID,
            path=fidelity_image_path,
            daily_bars=daily_window[["o", "h", "l", "c", "v", "ma7", "ma20", "ma60", "ma100", "ma200"]].to_dict(orient="records"),
            weekly_bars=weekly_window[["o", "h", "l", "c", "v", "ma10", "ma30", "ma60"]].to_dict(orient="records"),
            monthly_bars=monthly_window[["o", "h", "l", "c", "v", "ma6", "ma12", "ma24"]].to_dict(orient="records"),
        )
        fidelity_image_sha256 = str(fidelity_info["image_sha256"])
    return {
        "control_image_path": str(control_image_path),
        "control_image_sha256": str(control_info["image_sha256"]),
        "control_actual_backend": str(control_info["actual_backend"]),
        "control_available": True,
        "fidelity_image_path": str(fidelity_image_path) if fidelity_image_path is not None else None,
        "fidelity_image_sha256": fidelity_image_sha256,
        "fidelity_actual_backend": "agg" if fidelity_image_path is not None else None,
        "fidelity_available": bool(fidelity_image_path is not None),
        "fidelity_exclusion_reason": fidelity_exclusion_reason,
    }


def _select_month_samples(
    *,
    month_frame: pd.DataFrame,
    anchor: dict[str, int],
    code_frames: dict[str, pd.DataFrame],
    dataset_dir: Path,
    restricted_codes: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if restricted_codes is not None:
        month_frame = month_frame.loc[month_frame["code"].astype(str).isin(restricted_codes)].copy()
    eligible_frame = month_frame[
        month_frame["history_250_complete"] & month_frame["recent_60_complete"] & month_frame["control_ma_ready"] & month_frame["turnover_ready"]
    ].copy()
    eligible_universe_size = int(len(eligible_frame))
    if eligible_universe_size <= 0:
        return [], {"as_of_date": int(anchor["as_of_date"]), "month_key": int(anchor["month_key"]), "forward_end_date": int(anchor["forward_end_date"]), "eligible_universe_size": 0, "eligible_after_liquidity_size": 0, "selected_top_count": 0, "selected_bottom_count": 0, "missing_forward_window_count": 0, "warmup_insufficient_count": 0}

    liq_threshold = float(eligible_frame["turnover20"].quantile(LIQUIDITY_BOTTOM_RATIO))
    eligible_liq = eligible_frame[eligible_frame["turnover20"] >= liq_threshold].copy()
    eligible_after_liquidity_size = int(len(eligible_liq))
    if eligible_after_liquidity_size <= 0:
        return [], {"as_of_date": int(anchor["as_of_date"]), "month_key": int(anchor["month_key"]), "forward_end_date": int(anchor["forward_end_date"]), "eligible_universe_size": eligible_universe_size, "eligible_after_liquidity_size": 0, "selected_top_count": 0, "selected_bottom_count": 0, "missing_forward_window_count": 0, "warmup_insufficient_count": 0}

    candidate_rows: list[dict[str, Any]] = []
    missing_forward_window_count = 0
    for row in eligible_liq.to_dict(orient="records"):
        code_frame = code_frames[str(row["code"])]
        forward_rows = code_frame.loc[code_frame["trade_date"] == int(anchor["forward_end_date"])]
        if forward_rows.empty:
            missing_forward_window_count += 1
            continue
        forward_close = safe_float(forward_rows.iloc[0]["c"])
        as_of_close = safe_float(row["c"])
        if forward_close is None or as_of_close in (None, 0.0):
            missing_forward_window_count += 1
            continue
        candidate_rows.append({**row, "forward_end_date": int(anchor["forward_end_date"]), "forward_return_1m": float((forward_close / as_of_close) - 1.0)})
    if not candidate_rows:
        return [], {"as_of_date": int(anchor["as_of_date"]), "month_key": int(anchor["month_key"]), "forward_end_date": int(anchor["forward_end_date"]), "eligible_universe_size": eligible_universe_size, "eligible_after_liquidity_size": eligible_after_liquidity_size, "selected_top_count": 0, "selected_bottom_count": 0, "missing_forward_window_count": missing_forward_window_count, "warmup_insufficient_count": 0}

    ranked = pd.DataFrame(candidate_rows).sort_values(["forward_return_1m", "code"], ascending=[False, True]).reset_index(drop=True)
    top_rows = ranked.head(TOP_N).copy()
    used_codes = {str(code) for code in top_rows["code"].tolist()}
    bottom_rows = ranked.loc[~ranked["code"].astype(str).isin(used_codes)].sort_values(["forward_return_1m", "code"], ascending=[True, True]).head(BOTTOM_N).copy()
    samples: list[dict[str, Any]] = []
    warmup_insufficient_count = 0

    def _sample_record(sample_row: dict[str, Any], *, label: str, rank_in_month: int) -> dict[str, Any]:
        nonlocal warmup_insufficient_count
        code = str(sample_row["code"])
        code_frame = code_frames[code]
        render_info = _render_sample_images(sample_row={**sample_row, "label": label}, code_frame=code_frame, dataset_dir=dataset_dir)
        if str(render_info["fidelity_exclusion_reason"] or "") == "warmup_insufficient":
            warmup_insufficient_count += 1
        return {
            "sample_id": f"{int(sample_row['trade_date'])}:{code}:{label}",
            "sample_key": f"{int(sample_row['trade_date'])}:{code}:{label}",
            "as_of_date": int(sample_row["trade_date"]),
            "forward_end_date": int(sample_row["forward_end_date"]),
            "code": code,
            "label": label,
            "label_id": 1 if label == "top20_up" else 0,
            "forward_return_1m": float(sample_row["forward_return_1m"]),
            "rank_in_month": int(rank_in_month),
            "eligible_universe_size": eligible_universe_size,
            "eligible_after_liquidity_size": eligible_after_liquidity_size,
            "lookback_start_date": int(code_frame.loc[code_frame["global_index"] >= int(sample_row["global_index"]) - (CONTROL_LOOKBACK_DAYS - 1), "trade_date"].min()),
            "lookback_end_date": int(sample_row["trade_date"]),
            "image_path": str(render_info["fidelity_image_path"] or render_info["control_image_path"]),
            "image_sha256": str(render_info["fidelity_image_sha256"] or render_info["control_image_sha256"]),
            "has_missing_data": False,
            "adv_proxy": float(sample_row["turnover20"]),
            "sector": sample_row.get("sector"),
            "market": sample_row.get("market"),
            "return_1m_pre": safe_float(sample_row.get("return_1m_pre")),
            "dist_ma20": safe_float((safe_float(sample_row.get("c")) / safe_float(sample_row.get("ma20")) - 1.0) if safe_float(sample_row.get("c")) not in (None, 0.0) and safe_float(sample_row.get("ma20")) not in (None, 0.0) else None),
            "dist_ma60": safe_float((safe_float(sample_row.get("c")) / safe_float(sample_row.get("ma60")) - 1.0) if safe_float(sample_row.get("c")) not in (None, 0.0) and safe_float(sample_row.get("ma60")) not in (None, 0.0) else None),
            "dist_ma120": safe_float((safe_float(sample_row.get("c")) / safe_float(sample_row.get("ma120")) - 1.0) if safe_float(sample_row.get("c")) not in (None, 0.0) and safe_float(sample_row.get("ma120")) not in (None, 0.0) else None),
            "volume_change20": safe_float(sample_row.get("volume_change20")),
            "position_from_60d_high": safe_float(sample_row.get("position_from_60d_high")),
            "realized_vol20": safe_float(sample_row.get("realized_vol20")),
            **render_info,
        }

    for rank, (_, row) in enumerate(top_rows.iterrows(), start=1):
        samples.append(_sample_record(row.to_dict(), label="top20_up", rank_in_month=rank))
    for rank, (_, row) in enumerate(bottom_rows.iterrows(), start=1):
        samples.append(_sample_record(row.to_dict(), label="bottom20_down", rank_in_month=rank))
    return samples, {"as_of_date": int(anchor["as_of_date"]), "month_key": int(anchor["month_key"]), "forward_end_date": int(anchor["forward_end_date"]), "eligible_universe_size": eligible_universe_size, "eligible_after_liquidity_size": eligible_after_liquidity_size, "selected_top_count": int(len(top_rows)), "selected_bottom_count": int(len(bottom_rows)), "missing_forward_window_count": int(missing_forward_window_count), "warmup_insufficient_count": int(warmup_insufficient_count)}


def build_event_image_dataset(
    *,
    export_db_path: str,
    dataset_id: str,
    source_db_path: str | None = None,
    start_month: str | int | None = None,
    end_month: str | int | None = None,
    renderer_backend: str = "agg",
    restricted_universe_path: str | None = None,
) -> dict[str, Any]:
    if str(renderer_backend).strip().lower() != "agg":
        raise RuntimeError("event image dataset v1.2 requires renderer_backend=agg")
    if not strict_agg_available():
        raise RuntimeError("strict agg backend is required for fidelity bundle rendering")

    probe = probe_export_snapshot_readiness(source_db_path, export_db_path)
    if str(probe.get("status") or "") != EXPORT_SNAPSHOT_STATUS_COMPLETE:
        raise RuntimeError(
            "export snapshot is not complete for event image dataset build: "
            + f"status={probe.get('status')} reason_code={probe.get('reason_code')}"
        )

    dataset_dir = event_image_dataset_dir(dataset_id)
    daily_frame = _load_daily_frame(str(export_db_path))
    metadata_frame = _load_code_metadata(source_db_path)
    if not metadata_frame.empty:
        daily_frame = daily_frame.merge(metadata_frame, on="code", how="left")
    else:
        daily_frame["sector"] = None
        daily_frame["market"] = None

    anchors = _month_end_anchors(daily_frame, start_month=_normalize_month_key(start_month), end_month=_normalize_month_key(end_month))
    if not anchors:
        raise RuntimeError("no month-end anchors available for dataset build")

    expected_month_keys = [int(anchor["month_key"]) for anchor in anchors]
    restricted_membership: dict[int, set[str]] | None = None
    restricted_universe_manifest: dict[str, Any] | None = None
    if restricted_universe_path is not None:
        restricted_membership, restricted_universe_manifest = _load_restricted_universe(
            restricted_universe_path,
            expected_month_keys=expected_month_keys,
        )

    code_frames = {str(code): frame.sort_values("trade_date").reset_index(drop=True) for code, frame in daily_frame.groupby("code", sort=False)}
    sample_rows: list[dict[str, Any]] = []
    month_summaries: list[dict[str, Any]] = []
    for anchor in anchors:
        month_frame = daily_frame.loc[daily_frame["trade_date"] == int(anchor["as_of_date"])].copy()
        restricted_codes = None if restricted_membership is None else restricted_membership.get(int(anchor["month_key"]), set())
        selected_rows, summary = _select_month_samples(
            month_frame=month_frame,
            anchor=anchor,
            code_frames=code_frames,
            dataset_dir=dataset_dir,
            restricted_codes=restricted_codes,
        )
        sample_rows.extend(selected_rows)
        month_summaries.append(summary)
    if not sample_rows:
        raise RuntimeError("event image dataset build produced no labeled samples")

    samples_frame = pd.DataFrame(sample_rows).sort_values(["as_of_date", "label_id", "rank_in_month", "code"]).reset_index(drop=True)
    split_map = _assign_chronological_splits(samples_frame["as_of_date"].astype(int).tolist())
    samples_frame["split"] = samples_frame["as_of_date"].map(split_map)
    samples_frame["evaluation_bundle_id"] = FIDELITY_EVALUATION_BUNDLE_ID
    samples_frame["renderer_spec_id"] = FIDELITY_RENDERER_SPEC_ID
    samples_frame["featureizer_spec_id"] = FIDELITY_FEATUREIZER_SPEC_ID

    monthly_event_index = samples_frame[
        [
            "sample_id",
            "sample_key",
            "as_of_date",
            "forward_end_date",
            "code",
            "label",
            "label_id",
            "forward_return_1m",
            "rank_in_month",
            "eligible_universe_size",
            "eligible_after_liquidity_size",
            "lookback_start_date",
            "lookback_end_date",
            "image_path",
            "image_sha256",
            "control_image_path",
            "fidelity_image_path",
            "control_available",
            "fidelity_available",
            "fidelity_exclusion_reason",
            "has_missing_data",
            "adv_proxy",
            "sector",
            "market",
        ]
    ].copy()
    monthly_sample_counts = [{**summary, "sample_count": int(summary["selected_top_count"] + summary["selected_bottom_count"])} for summary in month_summaries]
    class_balance = {"top20_up": int((samples_frame["label"] == "top20_up").sum()), "bottom20_down": int((samples_frame["label"] == "bottom20_down").sum())}
    fidelity_samples = samples_frame.loc[samples_frame["fidelity_available"]].copy()
    preview_manifest_path, preview_contact_sheet_path = _build_preview_artifacts(dataset_dir, fidelity_samples)

    diagnostic_payload = {
        "schema_version": DATASET_DIAGNOSTIC_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "monthly_sample_counts": monthly_sample_counts,
        "class_balance": class_balance,
        "sector_bias": samples_frame["sector"].fillna("unknown").value_counts().head(20).to_dict(),
        "market_bias": samples_frame["market"].fillna("unknown").value_counts().head(20).to_dict(),
        "adv_proxy_distribution": _series_stats([float(value) for value in samples_frame["adv_proxy"].dropna().tolist()]),
        "volatility_bias": _distribution_by_label(samples_frame, "realized_vol20"),
        "forward_return_dispersion": _distribution_by_label(samples_frame, "forward_return_1m"),
        "pre_event_volatility_distribution": _distribution_by_label(samples_frame, "realized_vol20"),
        "sector_concentration_top20": samples_frame.loc[samples_frame["label"] == "top20_up", "sector"].fillna("unknown").value_counts().head(10).to_dict(),
        "sector_concentration_bottom20": samples_frame.loc[samples_frame["label"] == "bottom20_down", "sector"].fillna("unknown").value_counts().head(10).to_dict(),
        "weak_periods": [],
        "hard_false_positives": [],
        "hard_false_negatives": [],
        "metadata_coverage": {"sector_non_null_ratio": float(samples_frame["sector"].notna().mean()), "market_non_null_ratio": float(samples_frame["market"].notna().mean())},
    }

    monthly_event_index_path = dataset_dir / "monthly_event_index.parquet"
    samples_path = dataset_dir / "samples.parquet"
    render_manifest_path = dataset_dir / "render_manifest.json"
    dataset_manifest_path = dataset_dir / "dataset_manifest.json"
    diagnostic_path = dataset_dir / "dataset_diagnostic.json"
    restricted_universe_manifest_path = dataset_dir / "restricted_universe_manifest.json"
    write_parquet_frame(monthly_event_index_path, monthly_event_index)
    write_parquet_frame(samples_path, samples_frame)

    render_manifest = {
        "schema_version": DATASET_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "image_spec_id": FIDELITY_RENDERER_SPEC_ID,
        "evaluation_bundle_id": FIDELITY_EVALUATION_BUNDLE_ID,
        "renderer_spec_id": FIDELITY_RENDERER_SPEC_ID,
        "featureizer_spec_id": FIDELITY_FEATUREIZER_SPEC_ID,
        "strict_backend": True,
        "actual_backend": "agg",
        "pane_layout": dict(FIDELITY_PANE_LAYOUT),
        "daily_spec": {"display_bars": FIDELITY_DAILY_BARS, "overlays": ["ma7", "ma20", "ma60", "ma100", "ma200"]},
        "price_volume_split": dict(FIDELITY_DAILY_PRICE_VOLUME_SPLIT),
        "weekly_spec": {"display_bars": FIDELITY_WEEKLY_BARS, "overlays": ["ma10", "ma30", "ma60"]},
        "monthly_spec": {"display_bars": FIDELITY_MONTHLY_BARS, "overlays": ["ma6", "ma12", "ma24"]},
        "warmup_contract": dict(FIDELITY_WARMUP_CONTRACT),
        "renderer_backend": "agg",
        "requested_renderer_backend": "agg",
        "backend_fallback_reason": None,
        "render_version": FIDELITY_RENDERER_SPEC_ID,
        "image_size": [512, 512],
        "normalization_rule": "pane_specific_ohlcv_with_fixed_layout",
        "palette": dict(DEFAULT_PALETTE),
        "sample_count": int(len(samples_frame)),
        "fidelity_sample_count": int(len(fidelity_samples)),
    }
    _write_json_artifact(render_manifest_path, render_manifest)
    _write_json_artifact(diagnostic_path, diagnostic_payload)
    if restricted_universe_manifest is not None:
        _write_json_artifact(restricted_universe_manifest_path, restricted_universe_manifest)

    dataset_manifest = {
        "schema_version": DATASET_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "evaluation_bundle_id": FIDELITY_EVALUATION_BUNDLE_ID,
        "renderer_spec_id": FIDELITY_RENDERER_SPEC_ID,
        "featureizer_spec_id": FIDELITY_FEATUREIZER_SPEC_ID,
        "control_bundle_id": CONTROL_EVALUATION_BUNDLE_ID,
        "control_renderer_spec_id": CONTROL_RENDERER_SPEC_ID,
        "control_featureizer_spec_id": CONTROL_FEATUREIZER_SPEC_ID,
        "image_spec_id": FIDELITY_RENDERER_SPEC_ID,
        "image_feature_size": FIDELITY_IMAGE_FEATURE_SIZE,
        "universe_definition": {
            "kind": "historical_restricted_membership" if restricted_universe_manifest is not None else "export_observed_monthly_eligible",
            "survivorship_note": "eligible universe uses only codes observable in export at each month-end; delisted names absent from export are not reconstructed in v1.2",
            "minimum_history_days": MIN_HISTORY_DAYS,
            "recent_completeness_days": RECENT_COMPLETENESS_DAYS,
            "liquidity_bottom_ratio": LIQUIDITY_BOTTOM_RATIO,
            "restricted_universe_name": None if restricted_universe_manifest is None else restricted_universe_manifest["universe_name"],
        },
        "start_month": int(min(item["month_key"] for item in month_summaries)),
        "end_month": int(max(item["month_key"] for item in month_summaries)),
        "label_horizon_months": LABEL_HORIZON_MONTHS,
        "top_n": TOP_N,
        "bottom_n": BOTTOM_N,
        "split_policy": {"kind": "chronological_month_group_v1", "train_ratio": 0.70, "validation_ratio": 0.15, "test_ratio": 0.15},
        "sample_count": int(len(samples_frame)),
        "class_balance": class_balance,
        "source_export_db_path": str(Path(str(export_db_path)).expanduser().resolve()),
        "source_signature": probe.get("source_signature"),
        "export_signature": probe.get("export_signature"),
        "cost_policy": "none",
        "artifact_paths": {
            "dataset_manifest": str(dataset_manifest_path),
            "monthly_event_index": str(monthly_event_index_path),
            "samples": str(samples_path),
            "render_manifest": str(render_manifest_path),
            "dataset_diagnostic": str(diagnostic_path),
            "preview_manifest": str(preview_manifest_path),
            "preview_contact_sheet": str(preview_contact_sheet_path),
            "images_root": str(dataset_dir / "images"),
            "restricted_universe_manifest": None if restricted_universe_manifest is None else str(restricted_universe_manifest_path),
        },
        "generated_at": _utc_now_iso(),
    }
    _write_json_artifact(dataset_manifest_path, dataset_manifest)
    return dataset_manifest
