from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.backend.services.watchlist import load_watchlist_codes, resolve_watchlist_path
from app.backend.services.tradex_experiment_store import resolve_tradex_root
from external_analysis.contracts.paths import resolve_source_db_path
from external_analysis.event_image_dataset.storage import write_parquet_frame
from external_analysis.image_rerank.artifacts import verify_roundtrip, write_json


RESTRICTED_UNIVERSE_BUILD_SCHEMA_VERSION = "tradex_event_image_dataset_restricted_universe_build_v1"
DEFAULT_SAMPLE_SIZE = 100
DEFAULT_SAMPLE_SEED = 7
DEFAULT_UNIVERSE_NAME = "MeeMeeRegisteredSample100"
DEFAULT_SAMPLING_POLICY = {
    "kind": "fixed_sample_universe",
    "source": "meemee_watchlist",
    "stratify_axes": ["sector", "liquidity"],
    "liquidity_floor_quantile": 0.10,
    "fill_remainder": "global_liquidity_desc_with_seeded_tiebreak",
    "rebalance_policy": "fixed_once_not_monthly",
}


def _write_json_artifact(path: Path, payload: dict[str, Any]) -> Path:
    write_json(path, payload)
    verify_roundtrip(path, payload)
    return path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_tiebreak(value: str, seed: int) -> str:
    return hashlib.sha256(f"{int(seed)}:{value}".encode("utf-8")).hexdigest()


def _month_range(start_month: int, end_month: int) -> list[int]:
    months: list[int] = []
    year = start_month // 100
    month = start_month % 100
    end_year = end_month // 100
    end_month_only = end_month % 100
    while (year, month) <= (end_year, end_month_only):
        months.append(year * 100 + month)
        month += 1
        if month > 12:
            year += 1
            month = 1
    return months


def _reference_universe_root() -> Path:
    root = resolve_tradex_root() / "reference_universes"
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def default_meemee_registered_sample_output_path() -> Path:
    return (_reference_universe_root() / "meemee_registered_sample100.parquet").resolve()


def _normalize_month_key(value: str | int) -> int:
    text = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(text) >= 8:
        text = text[:6]
    if len(text) != 6:
        raise ValueError(f"unsupported month value: {value}")
    return int(text)


def _load_watchlist_codes() -> tuple[Path, list[str]]:
    watchlist_path = Path(resolve_watchlist_path()).expanduser().resolve()
    if not watchlist_path.exists():
        raise RuntimeError(f"MeeMee watchlist source-of-truth not found: {watchlist_path}")
    codes = load_watchlist_codes(str(watchlist_path))
    if not codes:
        raise RuntimeError(f"MeeMee watchlist is empty: {watchlist_path}")
    return watchlist_path, [str(code) for code in codes]


def _load_candidate_frame(source_db_path: str | None, watchlist_codes: list[str]) -> pd.DataFrame:
    source_db = resolve_source_db_path(source_db_path)
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        code_values = ", ".join(f"'{code}'" for code in sorted(set(watchlist_codes)))
        frame = conn.execute(
            f"""
            WITH latest_trade_date AS (
                SELECT MAX(date) AS latest_date
                FROM daily_bars
            ),
            latest_rows AS (
                SELECT
                    b.code,
                    b.date,
                    b.c,
                    b.v,
                    AVG(b.c * b.v) OVER (
                        PARTITION BY b.code
                        ORDER BY b.date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS turnover20,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.code
                        ORDER BY b.date DESC
                    ) AS recency_rank
                FROM daily_bars b
                WHERE b.code IN ({code_values})
            )
            SELECT
                l.code,
                l.date AS latest_trade_date,
                l.turnover20,
                i.sector33_name AS sector,
                i.market_code AS market
            FROM latest_rows l
            LEFT JOIN industry_master i
              ON i.code = l.code
            WHERE l.recency_rank = 1
            ORDER BY l.code
            """
        ).df()
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError("failed to build candidate pool from MeeMee watchlist codes")
    frame["code"] = frame["code"].astype(str)
    frame["latest_trade_date"] = pd.to_numeric(frame["latest_trade_date"], errors="coerce").astype("Int64")
    frame["turnover20"] = pd.to_numeric(frame["turnover20"], errors="coerce")
    frame["sector"] = frame["sector"].fillna("unknown").astype(str)
    frame["market"] = frame["market"].fillna("unknown").astype(str)
    return frame


def _select_stratified_sample(
    candidate_frame: pd.DataFrame,
    *,
    sample_size: int,
    sample_seed: int,
    sampling_policy: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = candidate_frame.copy()
    frame = frame.loc[frame["turnover20"].notna() & (frame["turnover20"] > 0)].copy()
    if frame.empty:
        raise RuntimeError("candidate pool has no valid turnover20 values")

    liquidity_floor_quantile = float(sampling_policy["liquidity_floor_quantile"])
    liquidity_cutoff = float(frame["turnover20"].quantile(liquidity_floor_quantile))
    eligible = frame.loc[frame["turnover20"] >= liquidity_cutoff].copy()
    if eligible.empty:
        raise RuntimeError("liquidity floor removed all watchlist candidates")

    eligible["tiebreak"] = eligible["code"].map(lambda value: _stable_tiebreak(str(value), int(sample_seed)))
    sector_names = sorted({str(value) for value in eligible["sector"].tolist() if str(value).strip()})
    if not sector_names:
        raise RuntimeError("sector stratification failed: no sector values available")
    per_sector_target = max(1, int(sample_size) // len(sector_names))

    selected_parts: list[pd.DataFrame] = []
    selected_codes: set[str] = set()
    sector_allocation: dict[str, int] = {}
    for sector in sector_names:
        sector_frame = eligible.loc[eligible["sector"] == sector].sort_values(["turnover20", "tiebreak", "code"], ascending=[False, True, True])
        picked = sector_frame.head(per_sector_target).copy()
        if not picked.empty:
            selected_parts.append(picked)
            picked_codes = {str(code) for code in picked["code"].tolist()}
            selected_codes.update(picked_codes)
            sector_allocation[str(sector)] = int(len(picked))
        else:
            sector_allocation[str(sector)] = 0

    selected = pd.concat(selected_parts, ignore_index=True) if selected_parts else eligible.head(0).copy()
    remaining_slots = max(0, int(sample_size) - int(len(selected)))
    if remaining_slots > 0:
        remaining = eligible.loc[~eligible["code"].astype(str).isin(selected_codes)].sort_values(
            ["turnover20", "tiebreak", "code"],
            ascending=[False, True, True],
        )
        fill_rows = remaining.head(remaining_slots).copy()
        if not fill_rows.empty:
            selected = pd.concat([selected, fill_rows], ignore_index=True)
            for sector, count in fill_rows["sector"].value_counts().to_dict().items():
                sector_allocation[str(sector)] = int(sector_allocation.get(str(sector), 0) + int(count))

    selected = selected.sort_values(["sector", "turnover20", "tiebreak", "code"], ascending=[True, False, True, True]).reset_index(drop=True)
    if selected.empty:
        raise RuntimeError("stratified sample selection produced no codes")
    summary = {
        "watchlist_code_count": int(len(candidate_frame)),
        "eligible_after_liquidity_floor_count": int(len(eligible)),
        "sample_size_requested": int(sample_size),
        "sample_size_selected": int(len(selected)),
        "liquidity_floor_quantile": liquidity_floor_quantile,
        "liquidity_cutoff": liquidity_cutoff,
        "sector_allocation": sector_allocation,
    }
    return selected, summary


def build_meemee_registered_sample_universe(
    *,
    source_db_path: str | None = None,
    output_path: str | Path | None = None,
    start_month: str | int,
    end_month: str | int,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    sample_seed: int = DEFAULT_SAMPLE_SEED,
) -> dict[str, Any]:
    normalized_start_month = _normalize_month_key(start_month)
    normalized_end_month = _normalize_month_key(end_month)
    if normalized_start_month > normalized_end_month:
        raise RuntimeError("start_month must be <= end_month")

    watchlist_path, watchlist_codes = _load_watchlist_codes()
    candidate_frame = _load_candidate_frame(source_db_path, watchlist_codes)
    sampling_policy = {
        **DEFAULT_SAMPLING_POLICY,
        "sample_size": int(sample_size),
    }
    selected, selection_summary = _select_stratified_sample(
        candidate_frame,
        sample_size=int(sample_size),
        sample_seed=int(sample_seed),
        sampling_policy=sampling_policy,
    )

    target_path = Path(output_path).expanduser().resolve() if output_path is not None else default_meemee_registered_sample_output_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    months = _month_range(normalized_start_month, normalized_end_month)
    source_file_sha256 = _sha256_file(watchlist_path)
    sampling_policy_text = json.dumps(sampling_policy, ensure_ascii=False, sort_keys=True)

    membership_rows: list[dict[str, Any]] = []
    for month_key in months:
        for row in selected.to_dict(orient="records"):
            membership_rows.append(
                {
                    "month_key": int(month_key),
                    "universe_name": DEFAULT_UNIVERSE_NAME,
                    "code": str(row["code"]),
                    "source_name": "MeeMeeWatchlist",
                    "source_version": "watchlist_v1",
                    "source_uri": str(watchlist_path),
                    "file_sha256": source_file_sha256,
                    "sample_seed": int(sample_seed),
                    "sampling_policy": sampling_policy_text,
                    "month_coverage_start": int(normalized_start_month),
                    "month_coverage_end": int(normalized_end_month),
                    "sector": str(row["sector"]),
                    "market": str(row["market"]),
                    "turnover20": float(row["turnover20"]),
                }
            )

    membership_frame = pd.DataFrame(membership_rows)
    write_parquet_frame(target_path, membership_frame)
    artifact_sha256 = _sha256_file(target_path)
    manifest_path = target_path.with_suffix(".manifest.json")
    manifest = {
        "schema_version": RESTRICTED_UNIVERSE_BUILD_SCHEMA_VERSION,
        "artifact_path": str(target_path),
        "artifact_file_sha256": artifact_sha256,
        "universe_name": DEFAULT_UNIVERSE_NAME,
        "source_name": "MeeMeeWatchlist",
        "source_version": "watchlist_v1",
        "source_uri": str(watchlist_path),
        "source_file_sha256": source_file_sha256,
        "sample_seed": int(sample_seed),
        "sampling_policy": sampling_policy,
        "month_coverage_start": int(normalized_start_month),
        "month_coverage_end": int(normalized_end_month),
        "selection_summary": selection_summary,
        "selected_codes": [str(code) for code in selected["code"].tolist()],
    }
    _write_json_artifact(manifest_path, manifest)
    return {
        "artifact_path": str(target_path),
        "manifest_path": str(manifest_path),
        "sample_size": int(len(selected)),
        "sample_seed": int(sample_seed),
        "month_coverage_start": int(normalized_start_month),
        "month_coverage_end": int(normalized_end_month),
    }
