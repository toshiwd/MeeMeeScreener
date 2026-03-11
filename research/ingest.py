from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
import re
import shutil
from pathlib import Path
from typing import Iterable

import pandas as pd

from research.storage import ResearchPaths, git_commit, now_utc_iso, write_csv, write_json, ymd


def _pick_column(frame: pd.DataFrame, candidates: Iterable[str], required: bool = True) -> str | None:
    lowered = {str(col).strip().lower(): str(col) for col in frame.columns}
    for key in candidates:
        if key in lowered:
            return lowered[key]
    if required:
        raise ValueError(f"missing required column. candidates={list(candidates)} actual={list(frame.columns)}")
    return None


def _normalize_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
    c_date = _pick_column(frame, ("date", "dt", "asof_date", "trade_date"))
    c_code = _pick_column(frame, ("code", "ticker", "symbol"))
    c_open = _pick_column(frame, ("open", "o"))
    c_high = _pick_column(frame, ("high", "h"))
    c_low = _pick_column(frame, ("low", "l"))
    c_close = _pick_column(frame, ("close", "c"))
    c_volume = _pick_column(frame, ("volume", "v"), required=False)

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(frame[c_date], errors="coerce").dt.normalize(),
            "code": frame[c_code].astype(str).str.strip(),
            "open": pd.to_numeric(frame[c_open], errors="coerce"),
            "high": pd.to_numeric(frame[c_high], errors="coerce"),
            "low": pd.to_numeric(frame[c_low], errors="coerce"),
            "close": pd.to_numeric(frame[c_close], errors="coerce"),
            "volume": pd.to_numeric(frame[c_volume], errors="coerce") if c_volume else 0.0,
        }
    )
    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    out = out[out["code"] != ""]
    out["volume"] = out["volume"].fillna(0.0)
    out = out.sort_values(["date", "code"]).drop_duplicates(subset=["date", "code"], keep="last").reset_index(drop=True)
    return out


def _derive_month_ends(daily: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(daily["date"], errors="coerce").dropna().drop_duplicates().sort_values()
    if dates.empty:
        raise ValueError("daily is empty after normalization")
    month_ends = dates.groupby(dates.dt.to_period("M")).max().dt.normalize()
    return pd.DataFrame({"asof_date": month_ends.astype(str)})


def _normalize_calendar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    c_date = _pick_column(frame, ("asof_date", "date", "month_end", "dt", "calendar_date"))
    out = pd.DataFrame({"asof_date": pd.to_datetime(frame[c_date], errors="coerce").dt.normalize()})
    out = out.dropna(subset=["asof_date"]).drop_duplicates().sort_values("asof_date").reset_index(drop=True)
    out["asof_date"] = out["asof_date"].dt.strftime("%Y-%m-%d")
    return out


def _parse_asof_from_filename(path: Path) -> tuple[int, int] | None:
    stem = path.stem
    match = re.match(r"^\s*(\d{4})[-_]?(\d{2})\s*$", stem)
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        return None
    return year, month


def _resolve_month_end(year: int, month: int, trading_dates: pd.Series) -> pd.Timestamp:
    in_month = trading_dates[(trading_dates.dt.year == year) & (trading_dates.dt.month == month)]
    if not in_month.empty:
        return pd.Timestamp(in_month.max()).normalize()
    return pd.Timestamp(datetime(year=year, month=month, day=1)).normalize() + pd.offsets.MonthEnd(0)


def _normalize_universe_frame(frame: pd.DataFrame, fallback_asof: pd.Timestamp | None) -> pd.DataFrame:
    lowered = {str(col).strip().lower(): str(col) for col in frame.columns}
    code_col = None
    for name in ("code", "ticker", "symbol"):
        if name in lowered:
            code_col = lowered[name]
            break
    asof_col = lowered.get("asof_date") or lowered.get("date") or lowered.get("month_end")

    rows: list[dict[str, str]] = []
    if code_col:
        codes = frame[code_col].astype(str).str.strip()
        asof_values = (
            pd.to_datetime(frame[asof_col], errors="coerce").dt.normalize()
            if asof_col
            else pd.Series([fallback_asof] * len(frame))
        )
        for code, asof in zip(codes, asof_values):
            if not code or pd.isna(asof):
                continue
            rows.append({"asof_date": ymd(asof), "code": code})
    else:
        if fallback_asof is None:
            raise ValueError("universe csv has no code column and fallback asof is missing")
        for col in frame.columns:
            values = frame[col].astype(str).str.strip()
            for code in values:
                if code:
                    rows.append({"asof_date": ymd(fallback_asof), "code": code})

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["asof_date", "code"])
    out = out.drop_duplicates(subset=["asof_date", "code"]).sort_values(["asof_date", "code"]).reset_index(drop=True)
    return out


def _normalize_sector_frame(frame: pd.DataFrame, codes: pd.Series) -> pd.DataFrame:
    c_code = _pick_column(frame, ("code", "ticker", "symbol"))
    c_sector_code = _pick_column(
        frame,
        ("sector33_code", "sector_code", "industry_code", "sector"),
        required=False,
    )
    c_sector_name = _pick_column(
        frame,
        ("sector33_name", "sector_name", "industry_name", "name"),
        required=False,
    )

    out = pd.DataFrame(
        {
            "code": frame[c_code].astype(str).str.strip(),
            "sector33_code": (
                frame[c_sector_code].astype(str).str.strip()
                if c_sector_code
                else "__NA__"
            ),
            "sector33_name": (
                frame[c_sector_name].astype(str).str.strip()
                if c_sector_name
                else "UNCLASSIFIED"
            ),
        }
    )
    out = out[out["code"] != ""].copy()
    out["sector33_code"] = out["sector33_code"].replace("", "__NA__").fillna("__NA__")
    out["sector33_name"] = out["sector33_name"].replace("", "UNCLASSIFIED").fillna("UNCLASSIFIED")
    out = out.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)

    base = pd.DataFrame({"code": codes.astype(str).str.strip().drop_duplicates().tolist()})
    base = base[base["code"] != ""].copy()
    base["sector33_code"] = "__NA__"
    base["sector33_name"] = "UNCLASSIFIED"
    merged = base.merge(out, on="code", how="left", suffixes=("_base", ""))
    merged["sector33_code"] = merged["sector33_code"].fillna(merged["sector33_code_base"]).replace("", "__NA__")
    merged["sector33_name"] = merged["sector33_name"].fillna(merged["sector33_name_base"]).replace("", "UNCLASSIFIED")
    merged = merged[["code", "sector33_code", "sector33_name"]].copy()
    return merged.sort_values("code").reset_index(drop=True)


def _fallback_sector_frame(codes: pd.Series) -> pd.DataFrame:
    return (
        pd.DataFrame({"code": codes.astype(str).str.strip().drop_duplicates().tolist()})
        .query("code != ''")
        .assign(sector33_code="__NA__", sector33_name="UNCLASSIFIED")
        .sort_values("code")
        .reset_index(drop=True)
    )


def _load_universe(universe_dir: Path, trading_dates: pd.Series) -> pd.DataFrame:
    files = sorted(universe_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"no universe csv found: {universe_dir}")

    chunks: list[pd.DataFrame] = []
    for path in files:
        raw = pd.read_csv(path)
        ym = _parse_asof_from_filename(path)
        fallback = _resolve_month_end(ym[0], ym[1], trading_dates) if ym else None
        normalized = _normalize_universe_frame(raw, fallback)
        if not normalized.empty:
            chunks.append(normalized)
    if not chunks:
        raise ValueError("universe files loaded but no usable rows")
    out = pd.concat(chunks, ignore_index=True)
    out = out.drop_duplicates(subset=["asof_date", "code"]).sort_values(["asof_date", "code"]).reset_index(drop=True)
    return out


def _file_sha1(path: Path) -> str:
    h = sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def run_ingest(
    paths: ResearchPaths,
    daily_csv: str,
    universe_dir: str,
    calendar_csv: str | None = None,
    sector_csv: str | None = None,
    snapshot_id: str | None = None,
) -> dict[str, object]:
    daily_path = Path(daily_csv).resolve()
    universe_path = Path(universe_dir).resolve()
    calendar_path = Path(calendar_csv).resolve() if calendar_csv else None
    sector_path = Path(sector_csv).resolve() if sector_csv else None

    if not daily_path.exists():
        raise FileNotFoundError(f"daily csv not found: {daily_path}")
    if not universe_path.exists() or not universe_path.is_dir():
        raise FileNotFoundError(f"universe dir not found: {universe_path}")
    if calendar_path and not calendar_path.exists():
        raise FileNotFoundError(f"calendar csv not found: {calendar_path}")
    if sector_path and not sector_path.exists():
        raise FileNotFoundError(f"sector csv not found: {sector_path}")

    raw_daily = pd.read_csv(daily_path)
    daily = _normalize_daily_frame(raw_daily)
    trading_dates = pd.to_datetime(daily["date"]).drop_duplicates().sort_values()

    if calendar_path:
        raw_calendar = pd.read_csv(calendar_path)
        calendar = _normalize_calendar_frame(raw_calendar)
    else:
        calendar = _derive_month_ends(daily)

    universe = _load_universe(universe_path, trading_dates)
    if sector_path:
        raw_sector = pd.read_csv(sector_path)
        industry_master = _normalize_sector_frame(raw_sector, daily["code"])
    else:
        industry_master = _fallback_sector_frame(daily["code"])

    resolved_snapshot = (
        snapshot_id.strip()
        if snapshot_id and snapshot_id.strip()
        else datetime.now(timezone.utc).strftime("snapshot_%Y%m%d%H%M%S")
    )
    target_dir = paths.snapshot_dir(resolved_snapshot)
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    daily_out = daily.copy()
    daily_out["date"] = pd.to_datetime(daily_out["date"]).dt.strftime("%Y-%m-%d")
    write_csv(target_dir / "daily.csv", daily_out)
    write_csv(target_dir / "calendar_month_ends.csv", calendar)
    write_csv(target_dir / "universe_monthly.csv", universe)
    write_csv(target_dir / "industry_master.csv", industry_master)

    universe_files = sorted(universe_path.glob("*.csv"))
    manifest = {
        "snapshot_id": resolved_snapshot,
        "created_at": now_utc_iso(),
        "git_commit": git_commit(paths.repo_root),
        "inputs": {
            "daily_csv": str(daily_path),
            "universe_dir": str(universe_path),
            "calendar_csv": str(calendar_path) if calendar_path else None,
            "sector_csv": str(sector_path) if sector_path else None,
            "hashes": {
                "daily_csv_sha1": _file_sha1(daily_path),
                "calendar_csv_sha1": (_file_sha1(calendar_path) if calendar_path else None),
                "sector_csv_sha1": (_file_sha1(sector_path) if sector_path else None),
                "universe_csv_sha1": {str(p.name): _file_sha1(p) for p in universe_files},
            },
        },
        "daily_rows": int(len(daily_out)),
        "daily_codes": int(daily_out["code"].nunique()),
        "daily_start": str(daily_out["date"].min()),
        "daily_end": str(daily_out["date"].max()),
        "calendar_months": int(len(calendar)),
        "universe_rows": int(len(universe)),
        "universe_months": int(universe["asof_date"].nunique()),
        "industry_rows": int(len(industry_master)),
        "universe_files": [str(p.name) for p in universe_files],
    }
    write_json(target_dir / "manifest.json", manifest)
    paths.set_latest_snapshot_id(resolved_snapshot)

    return {
        "ok": True,
        "snapshot_id": resolved_snapshot,
        "snapshot_dir": str(target_dir),
        "daily_rows": int(len(daily_out)),
        "universe_rows": int(len(universe)),
        "calendar_rows": int(len(calendar)),
        "industry_rows": int(len(industry_master)),
    }
