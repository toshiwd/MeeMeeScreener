from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.models.candidate_baseline import _score_frame, load_candidate_input_frame


def normalize_as_of_date(value: Any) -> int:
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"unsupported as_of_date: {value}")
    return int(text)


def load_bars_frame(export_db_path: str | None) -> pd.DataFrame:
    conn = connect_export_db(export_db_path)
    try:
        frame = conn.execute(
            """
            SELECT code, trade_date, o, h, l, c, v
            FROM bars_daily_export
            ORDER BY code, trade_date
            """
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError("bars_daily_export is empty")
    frame["trade_date"] = frame["trade_date"].astype(int)
    for column in ("o", "h", "l", "c", "v"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def compute_candidate_universe_hash(rows: list[dict[str, Any]], *, as_of_snapshot_date: int) -> str:
    codes = sorted({str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()})
    payload = json.dumps({"as_of_snapshot_date": int(as_of_snapshot_date), "codes": codes}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_base_score_artifact(
    *,
    export_db_path: str | None,
    as_of_snapshot_date: int,
) -> dict[str, Any]:
    frame = load_candidate_input_frame(export_db_path, as_of_snapshot_date)
    if not frame:
        raise RuntimeError(f"no candidate rows found for as_of_snapshot_date={as_of_snapshot_date}")
    scored, regime = _score_frame(frame)
    ordered = sorted(
        scored,
        key=lambda row: (
            -float(row.get("ranking_score_long") or 0.0),
            -float(row.get("retrieval_score_long") or 0.0),
            str(row.get("code") or ""),
        ),
    )
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(ordered, start=1):
        rows.append(
            {
                "code": str(row.get("code") or ""),
                "as_of_snapshot_date": int(as_of_snapshot_date),
                "base_score": float(row.get("ranking_score_long") or 0.0),
                "base_retrieval_score": float(row.get("retrieval_score_long") or 0.0),
                "base_risk_penalty": float(row.get("risk_penalty") or 0.0),
                "base_rank": rank,
                "liquidity_proxy": float(row.get("volume_value") or 0.0),
                "close_price": float(row.get("close_price") or 0.0),
                "ma20": float(row.get("ma20") or 0.0),
                "ret_20_past": float(row.get("ret_20_past") or 0.0),
                "ret_5_past": float(row.get("ret_5_past") or 0.0),
                "trend_bias": float(row.get("close_vs_ma20") or 0.0),
            }
        )
    return {
        "schema_version": "tradex_image_rerank_base_score_v1",
        "as_of_snapshot_date": int(as_of_snapshot_date),
        "candidate_count": len(rows),
        "candidate_universe_hash": compute_candidate_universe_hash(rows, as_of_snapshot_date=as_of_snapshot_date),
        "regime": regime,
        "rows": rows,
    }


def build_historical_samples(
    *,
    bars_frame: pd.DataFrame,
    snapshot_date: int,
    feature_lookback_days: int,
    label_horizon_days: int,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for code, group in bars_frame.groupby("code", sort=True):
        ordered = group.sort_values("trade_date").reset_index(drop=True)
        if len(ordered) < feature_lookback_days + label_horizon_days:
            continue
        for idx in range(feature_lookback_days - 1, len(ordered) - label_horizon_days):
            as_of_date = int(ordered.iloc[idx]["trade_date"])
            if as_of_date > snapshot_date:
                break
            feature_rows = ordered.iloc[idx - feature_lookback_days + 1 : idx + 1]
            future_rows = ordered.iloc[idx + 1 : idx + 1 + label_horizon_days]
            if len(feature_rows) != feature_lookback_days or len(future_rows) != label_horizon_days:
                continue
            if feature_rows[["o", "h", "l", "c", "v"]].isna().any().any():
                continue
            if future_rows[["o", "h", "l", "c", "v"]].isna().any().any():
                continue
            feature_close = float(feature_rows.iloc[-1]["c"])
            future_close = float(future_rows.iloc[-1]["c"])
            if feature_close <= 0.0 or future_close <= 0.0:
                continue
            future_return = (future_close / feature_close) - 1.0
            feature_window = [
                {
                    "trade_date": int(row.trade_date),
                    "o": float(row.o),
                    "h": float(row.h),
                    "l": float(row.l),
                    "c": float(row.c),
                    "v": float(row.v),
                }
                for row in feature_rows.itertuples(index=False)
            ]
            samples.append(
                {
                    "code": str(code),
                    "as_of_date": as_of_date,
                    "feature_start_date": int(feature_rows.iloc[0]["trade_date"]),
                    "feature_end_date": as_of_date,
                    "future_end_date": int(future_rows.iloc[-1]["trade_date"]),
                    "feature_window": feature_window,
                    "future_return": float(future_return),
                    "future_close": float(future_close),
                    "feature_close": float(feature_close),
                    "liquidity_proxy": float(feature_rows["v"].mean()),
                    "feature_row_count": len(feature_rows),
                    "future_row_count": len(future_rows),
                }
            )
    return samples


def build_snapshot_rows(
    *,
    bars_frame: pd.DataFrame,
    snapshot_date: int,
    feature_lookback_days: int,
    label_horizon_days: int,
    base_score_artifact: dict[str, Any],
) -> list[dict[str, Any]]:
    historical_samples = build_historical_samples(
        bars_frame=bars_frame,
        snapshot_date=snapshot_date,
        feature_lookback_days=feature_lookback_days,
        label_horizon_days=label_horizon_days,
    )
    base_by_code = {str(row.get("code") or ""): row for row in base_score_artifact.get("rows") or []}
    rows: list[dict[str, Any]] = []
    for sample in historical_samples:
        if sample["as_of_date"] != snapshot_date:
            continue
        base_row = base_by_code.get(sample["code"], {})
        rows.append(
            {
                **sample,
                "base_score": float(base_row.get("base_score") or 0.0),
                "base_rank": int(base_row.get("base_rank") or 0),
                "base_retrieval_score": float(base_row.get("base_retrieval_score") or 0.0),
                "base_risk_penalty": float(base_row.get("base_risk_penalty") or 0.0),
            }
        )
    if not rows:
        raise RuntimeError("no snapshot rows produced")
    return rows
